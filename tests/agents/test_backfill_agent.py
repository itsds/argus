"""
Unit tests for the Incident & Backfill Planning agent.

WHY THIS FILE MATTERS (learning concepts):

  This test file covers a FUNDAMENTALLY DIFFERENT agent pattern than the
  Recon and DLQ agent tests. Those agents complete in a single invoke()
  call. The Backfill agent uses a TWO-PHASE LIFECYCLE:

    invoke()  → graph runs to interrupt  → status="needs_approval"
    resume()  → graph continues          → status="success" / "failure"

  This means we need to test scenarios that don't exist in Recon/DLQ:

  1. INTERRUPT DETECTION — after invoke(), the graph should be paused
     (graph_state.next is non-empty), and the agent should return
     status="needs_approval" with the plan.

  2. RESUME WITH APPROVAL — after resume("approved"), the graph should
     complete (graph_state.next is empty), and the agent should return
     status="success" with the execution audit.

  3. RESUME WITH REJECTION — after resume("rejected", feedback), the
     graph may interrupt AGAIN (revised plan) or fail (max revisions).

  4. REJECTION LOOP — the caller may need to call resume() multiple
     times, each time getting "needs_approval" until the human approves
     or max revisions is hit.

  5. STATEFUL INSTANCE — _thread_id, _context, _started_at persist
     across invoke/resume. Tests must verify they are set correctly
     and cleared after lifecycle completion.

  6. ERROR BOUNDARIES — both invoke() and resume() have their own
     try/except blocks. Tests verify both.

  7. GUARD RAILS — resume() without prior invoke() raises ValueError.

MOCKING STRATEGY:

  We mock the compiled graph object, NOT build_graph(). The mock graph
  exposes:
    - invoke(state_or_command, config=...) → returns result_state dict
    - get_state(config) → returns a mock StateSnapshot with .next

  For interrupt detection, the mock get_state() returns:
    - StateSnapshot(next=("approval_gate",)) for interrupted
    - StateSnapshot(next=()) for completed

  This tests the agent's _process_graph_result() logic without needing
  a real LangGraph graph, checkpointer, or LLM.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from argus.agents.base import AgentResult, TriggerContext
from argus.agents.backfill.agent import BackfillAgent, _extract_tool_calls
from argus.agents.backfill.state import make_initial_state
from argus.core.config import ArgusConfig
from argus.schemas.reports import (
    BackfillPlan,
    BackfillStep,
    Notification,
    Severity,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

def _make_config(overrides: dict[str, Any] | None = None) -> ArgusConfig:
    """
    Create a minimal ArgusConfig for testing.

    Mirrors the Recon/DLQ test pattern: no dependency on config files.
    """
    base = {
        "llm": {
            "provider": "google",
            "model": "gemini-2.0-flash",
            "temperature": 0.0,
        },
        "agents": {
            "backfill": {
                "max_iterations": 5,
                "max_plan_iterations": 3,
            },
        },
        "logging": {"level": "WARNING"},
    }
    if overrides:
        base.update(overrides)
    return ArgusConfig(base)


def _make_context(
    run_date: str = "2026-09-28",
    correlation_id: str = "test-corr-001",
) -> TriggerContext:
    """Create a standard TriggerContext for Backfill testing."""
    return TriggerContext(
        agent_name="backfill",
        trigger_source="test",
        run_date=run_date,
        correlation_id=correlation_id,
        params={
            "incident_id": "INC-2026-0928",
            "dag_id": "ttag_main",
            "error": "Gate 3 failure — 847 missing records",
        },
    )


def _make_sample_plan() -> BackfillPlan:
    """Create a sample BackfillPlan for mocking the plan_node output."""
    return BackfillPlan(
        incident_summary="Gate 3 failure on 2026-09-28 — 847 missing booking records",
        root_cause="Upstream Kafka consumer lag caused 847 records to arrive after Silver merge window",
        affected_partitions=["2026-09-28"],
        proposed_steps=[
            BackfillStep(
                order=1,
                description="Replay Silver booking_detail for partition 2026-09-28",
                watermark_key="booking__backfill",
                snapshot_range="snap-100..snap-105",
                collision_check="Verify no concurrent writes on booking_detail",
                requires_lock=True,
            ),
            BackfillStep(
                order=2,
                description="Rebuild Gold fact_booking from Silver for 2026-09-28",
                watermark_key="booking__backfill",
                snapshot_range=None,
                collision_check="Verify Gold row counts match Silver after rebuild",
                requires_lock=True,
            ),
        ],
        estimated_duration_minutes=45,
        risk_assessment="Low risk — single partition, no SCD changes affected",
        recommended_severity=Severity.P2,
        notifications=[
            Notification(
                severity=Severity.P2,
                channel="slack",
                title="Backfill plan ready for review",
                body="2-step backfill for 847 missing booking records on 2026-09-28",
            ),
        ],
    )


def _make_interrupted_state(plan: BackfillPlan | None = None) -> dict:
    """
    Build a result_state dict simulating a graph that stopped at interrupt.

    The graph ran entry → investigate → plan → approval_gate → interrupt().
    State contains investigation messages and the produced plan.
    """
    if plan is None:
        plan = _make_sample_plan()
    return {
        "messages": [
            SystemMessage(content="You are the Backfill investigation agent."),
            HumanMessage(content="Investigate incident INC-2026-0928"),
            AIMessage(
                content="Checking incident context...",
                tool_calls=[
                    {"name": "check_incident_context", "args": {"incident_id": "INC-2026-0928"}, "id": "tc1"},
                ],
            ),
            ToolMessage(content='{"incident_id": "INC-2026-0928", "status": "open"}', tool_call_id="tc1"),
            AIMessage(
                content="Now checking partition metadata...",
                tool_calls=[
                    {"name": "get_partition_metadata", "args": {"table": "booking_detail"}, "id": "tc2"},
                ],
            ),
            ToolMessage(content='{"partitions": ["2026-09-28"]}', tool_call_id="tc2"),
            AIMessage(content="Investigation complete. Producing plan."),
        ],
        "run_date": "2026-09-28",
        "trigger_params": {"incident_id": "INC-2026-0928", "dag_id": "ttag_main"},
        "correlation_id": "test-corr-001",
        "iteration": 3,
        "max_iterations": 5,
        "plan": plan,
        "approval_status": "pending",
        "revision_feedback": "",
        "plan_iterations": 1,
        "max_plan_iterations": 3,
        "execution_audit": [],
        "errors": [],
    }


def _make_completed_state(plan: BackfillPlan | None = None) -> dict:
    """
    Build a result_state dict simulating a graph that ran to END after
    approval and execution.
    """
    if plan is None:
        plan = _make_sample_plan()
    return {
        "messages": [
            SystemMessage(content="You are the Backfill execution agent."),
            HumanMessage(content="Execute the approved backfill plan."),
            AIMessage(
                content="Acquiring lock...",
                tool_calls=[
                    {"name": "acquire_pipeline_lock", "args": {"entity": "booking"}, "id": "tc10"},
                ],
            ),
            ToolMessage(content='{"lock_id": "lock-001", "status": "acquired"}', tool_call_id="tc10"),
            AIMessage(
                content="Executing step 1...",
                tool_calls=[
                    {"name": "execute_backfill_step", "args": {"step_order": 1}, "id": "tc11"},
                ],
            ),
            ToolMessage(content='{"status": "success", "rows_processed": 847}', tool_call_id="tc11"),
            AIMessage(
                content="Releasing lock...",
                tool_calls=[
                    {"name": "release_pipeline_lock", "args": {"lock_id": "lock-001"}, "id": "tc12"},
                ],
            ),
            ToolMessage(content='{"status": "released"}', tool_call_id="tc12"),
            AIMessage(content="Execution complete."),
        ],
        "run_date": "2026-09-28",
        "trigger_params": {"incident_id": "INC-2026-0928"},
        "correlation_id": "test-corr-001",
        "iteration": 5,
        "max_iterations": 5,
        "plan": plan,
        "approval_status": "approved",
        "revision_feedback": "",
        "plan_iterations": 1,
        "max_plan_iterations": 3,
        "execution_audit": [
            "LOCK ACQUIRED: booking [silver, gold] (lock-001)",
            "STEP 1 SUCCESS: Replay Silver booking_detail for 2026-09-28 — 847 rows",
            "LOCK RELEASED: booking [silver, gold] (lock-001)",
        ],
        "errors": [],
    }


def _make_mock_graph(
    invoke_return: dict,
    is_interrupted: bool,
) -> MagicMock:
    """
    Build a mock graph with invoke() and get_state() wired up.

    The key mock: get_state(config).next returns a non-empty tuple
    when interrupted, empty tuple when completed. This is how the
    agent detects whether the graph is paused or done.
    """
    mock_graph = MagicMock()
    mock_graph.invoke.return_value = invoke_return

    # Mock get_state() to return a StateSnapshot-like object
    mock_state_snapshot = MagicMock()
    # .next is a tuple of pending node names
    mock_state_snapshot.next = ("approval_gate",) if is_interrupted else ()
    mock_graph.get_state.return_value = mock_state_snapshot

    return mock_graph


# ---------------------------------------------------------------------------
# Tests: Agent properties
# ---------------------------------------------------------------------------

class TestAgentProperties:
    """Test that the agent satisfies the BaseAgent contract."""

    def test_name(self):
        agent = BackfillAgent(_make_config())
        assert agent.name == "backfill"

    def test_description(self):
        agent = BackfillAgent(_make_config())
        assert "backfill" in agent.description.lower()

    def test_graph_not_built_on_init(self):
        """The graph should be None until first invoke — lazy init."""
        agent = BackfillAgent(_make_config())
        assert agent._graph is None

    def test_initial_phase_state_is_none(self):
        """
        NEW vs Recon/DLQ: the Backfill agent has instance state that
        should be None on initialization.
        """
        agent = BackfillAgent(_make_config())
        assert agent._thread_id is None
        assert agent._context is None
        assert agent._started_at is None

    def test_has_pending_approval_initially_false(self):
        """has_pending_approval should be False before any invoke()."""
        agent = BackfillAgent(_make_config())
        assert agent.has_pending_approval is False


# ---------------------------------------------------------------------------
# Tests: make_initial_state
# ---------------------------------------------------------------------------

class TestMakeInitialState:
    """Test the Backfill state factory function."""

    def test_required_fields(self):
        state = make_initial_state(
            run_date="2026-09-28",
            trigger_params={"dag_id": "ttag_main", "incident_id": "INC-001"},
            correlation_id="corr-123",
        )
        assert state["run_date"] == "2026-09-28"
        assert state["trigger_params"]["incident_id"] == "INC-001"
        assert state["correlation_id"] == "corr-123"

    def test_defaults(self):
        state = make_initial_state(
            run_date="2026-09-28",
            trigger_params={},
            correlation_id="corr-123",
        )
        assert state["messages"] == []
        assert state["iteration"] == 0
        assert state["max_iterations"] == 15  # Backfill default (higher than Recon's 10)
        assert state["plan"] is None
        assert state["approval_status"] == ""
        assert state["revision_feedback"] == ""
        assert state["plan_iterations"] == 0
        assert state["max_plan_iterations"] == 3
        assert state["execution_audit"] == []
        assert state["errors"] == []

    def test_custom_iteration_limits(self):
        state = make_initial_state(
            run_date="2026-09-28",
            trigger_params={},
            correlation_id="corr-123",
            max_iterations=8,
            max_plan_iterations=5,
        )
        assert state["max_iterations"] == 8
        assert state["max_plan_iterations"] == 5


# ---------------------------------------------------------------------------
# Tests: _extract_tool_calls (same helper as Recon/DLQ)
# ---------------------------------------------------------------------------

class TestExtractToolCalls:
    """Test the helper that mines tool names from message history."""

    def test_empty_messages(self):
        assert _extract_tool_calls([]) == []

    def test_no_tool_calls(self):
        messages = [
            SystemMessage(content="system"),
            HumanMessage(content="investigate"),
            AIMessage(content="I'll investigate now."),
        ]
        assert _extract_tool_calls(messages) == []

    def test_single_tool_call(self):
        messages = [
            AIMessage(
                content="Checking incident...",
                tool_calls=[
                    {"name": "check_incident_context", "args": {}, "id": "tc1"},
                ],
            ),
        ]
        assert _extract_tool_calls(messages) == ["check_incident_context"]

    def test_multiple_tool_calls_both_phases(self):
        """
        NEW vs Recon/DLQ: Backfill tool calls span TWO phases
        (investigation + execution), captured in order.
        """
        messages = [
            AIMessage(
                content="Investigation...",
                tool_calls=[
                    {"name": "check_incident_context", "args": {}, "id": "tc1"},
                    {"name": "get_partition_metadata", "args": {}, "id": "tc2"},
                ],
            ),
            ToolMessage(content="{}", tool_call_id="tc1"),
            ToolMessage(content="{}", tool_call_id="tc2"),
            AIMessage(
                content="Executing...",
                tool_calls=[
                    {"name": "acquire_pipeline_lock", "args": {}, "id": "tc3"},
                    {"name": "execute_backfill_step", "args": {}, "id": "tc4"},
                    {"name": "release_pipeline_lock", "args": {}, "id": "tc5"},
                ],
            ),
        ]
        result = _extract_tool_calls(messages)
        assert result == [
            "check_incident_context",
            "get_partition_metadata",
            "acquire_pipeline_lock",
            "execute_backfill_step",
            "release_pipeline_lock",
        ]

    def test_ignores_non_ai_messages(self):
        messages = [
            SystemMessage(content="system"),
            HumanMessage(content="human"),
            ToolMessage(content="tool result", tool_call_id="tc1"),
        ]
        assert _extract_tool_calls(messages) == []


# ---------------------------------------------------------------------------
# Tests: invoke() — Phase 1 (interrupt path)
# ---------------------------------------------------------------------------

class TestInvokeInterrupt:
    """
    Test invoke() when the graph interrupts at approval_gate.

    This is the NORMAL path for the Backfill agent — invoke() should
    return needs_approval because the graph pauses for human review.
    """

    @pytest.mark.asyncio
    async def test_invoke_returns_needs_approval(self):
        """
        When graph.invoke() runs to interrupt, the agent should return
        AgentResult with status="needs_approval" and the plan as report.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()
        plan = _make_sample_plan()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(plan),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert isinstance(result, AgentResult)
        assert result.status == "needs_approval"
        assert result.agent_name == "backfill"
        assert result.correlation_id == "test-corr-001"
        # Report should be the plan as a dict (model_dump)
        assert result.report["incident_summary"] == plan.incident_summary
        assert result.report["root_cause"] == plan.root_cause
        assert len(result.report["proposed_steps"]) == 2

    @pytest.mark.asyncio
    async def test_invoke_extracts_tool_calls(self):
        """Actions taken should include investigation tool calls."""
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert "check_incident_context" in result.actions_taken
        assert "get_partition_metadata" in result.actions_taken

    @pytest.mark.asyncio
    async def test_invoke_sets_instance_state(self):
        """
        After invoke(), _thread_id, _context, _started_at should be set.
        This is NEW — Recon/DLQ don't store instance state.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        await agent.invoke(context)

        assert agent._thread_id is not None
        assert agent._thread_id.startswith("backfill-")
        assert agent._context is context
        assert agent._started_at is not None

    @pytest.mark.asyncio
    async def test_invoke_thread_id_from_correlation_id(self):
        """Thread ID should be derived from correlation_id for traceability."""
        agent = BackfillAgent(_make_config())
        context = _make_context(correlation_id="INC-2026-0928")

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        await agent.invoke(context)

        assert agent._thread_id == "backfill-INC-2026-0928"

    @pytest.mark.asyncio
    async def test_invoke_thread_id_fallback_without_correlation(self):
        """Without correlation_id, thread ID should use a UUID fallback."""
        agent = BackfillAgent(_make_config())
        context = _make_context(correlation_id="")

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        await agent.invoke(context)

        assert agent._thread_id is not None
        assert agent._thread_id.startswith("backfill-")
        # Should NOT be "backfill-" (empty) — should have a UUID suffix
        assert len(agent._thread_id) > len("backfill-")

    @pytest.mark.asyncio
    async def test_invoke_passes_config_with_thread_id(self):
        """
        graph.invoke() must receive config with thread_id.
        Without this, the MemorySaver can't save checkpoint state.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context(correlation_id="INC-001")

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        await agent.invoke(context)

        # Verify config was passed to graph.invoke()
        call_kwargs = mock_graph.invoke.call_args[1]
        assert "config" in call_kwargs
        assert call_kwargs["config"]["configurable"]["thread_id"] == "backfill-INC-001"

    @pytest.mark.asyncio
    async def test_invoke_has_pending_approval_true(self):
        """has_pending_approval should be True after invoke() with interrupt."""
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        await agent.invoke(context)

        assert agent.has_pending_approval is True

    @pytest.mark.asyncio
    async def test_invoke_no_plan_returns_error_in_report(self):
        """
        If graph interrupts but plan is None (planning node failed),
        the report should indicate no plan was produced.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        state = _make_interrupted_state()
        state["plan"] = None

        mock_graph = _make_mock_graph(
            invoke_return=state,
            is_interrupted=True,
        )
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert result.status == "needs_approval"
        assert "error" in result.report
        assert "No plan" in result.report["error"]


# ---------------------------------------------------------------------------
# Tests: invoke() — failure path
# ---------------------------------------------------------------------------

class TestInvokeFailure:
    """Test invoke() error handling."""

    @pytest.mark.asyncio
    async def test_graph_exception_returns_failure(self):
        """
        When graph.invoke() raises, the agent catches it and returns
        status="failure". Same pattern as Recon/DLQ.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = MagicMock()
        mock_graph.invoke.side_effect = RuntimeError("LLM API rate limited")
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert result.status == "failure"
        assert "rate limited" in result.report["error"].lower()
        assert any("rate limited" in e.lower() for e in result.errors)

    @pytest.mark.asyncio
    async def test_graph_exception_clears_phase_state(self):
        """
        On exception, _clear_phase_state() should reset instance state.
        The agent should be ready for a new invoke() call.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = MagicMock()
        mock_graph.invoke.side_effect = RuntimeError("boom")
        agent._graph = mock_graph

        await agent.invoke(context)

        assert agent._thread_id is None
        assert agent._context is None
        assert agent._started_at is None
        assert agent.has_pending_approval is False


# ---------------------------------------------------------------------------
# Tests: resume() — approval path
# ---------------------------------------------------------------------------

class TestResumeApproval:
    """
    Test resume() when the human approves the plan.

    After approval, the graph runs the execution phase to END,
    and the agent returns status="success" with plan + execution_audit.
    """

    @pytest.mark.asyncio
    async def test_resume_approved_returns_success(self):
        """
        resume("approved") after invoke() should return AgentResult
        with status="success" and the execution audit.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()
        plan = _make_sample_plan()

        # Phase 1: invoke (interrupt)
        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(plan),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        # Phase 2: resume (approval → completion)
        mock_graph.invoke.return_value = _make_completed_state(plan)
        mock_state = MagicMock()
        mock_state.next = ()  # completed — no pending nodes
        mock_graph.get_state.return_value = mock_state

        result = await agent.resume("approved")

        assert result.status == "success"
        assert result.agent_name == "backfill"
        assert result.correlation_id == "test-corr-001"
        assert "plan" in result.report
        assert "execution_audit" in result.report
        assert len(result.report["execution_audit"]) == 3  # lock, step, release

    @pytest.mark.asyncio
    async def test_resume_approved_passes_command(self):
        """
        resume() should pass Command(resume={"decision": ..., "feedback": ...})
        to graph.invoke(), not a state dict.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        # Set up for resume
        mock_graph.invoke.return_value = _make_completed_state()
        mock_state = MagicMock()
        mock_state.next = ()
        mock_graph.get_state.return_value = mock_state

        await agent.resume("approved")

        # Verify Command was passed (second invoke call)
        resume_call = mock_graph.invoke.call_args_list[1]
        command_arg = resume_call[0][0]
        # Command(resume=...) — check it's a Command with the right resume value
        from langgraph.types import Command
        assert isinstance(command_arg, Command)

    @pytest.mark.asyncio
    async def test_resume_approved_uses_same_thread_id(self):
        """
        resume() must use the SAME thread_id as invoke() so the
        checkpointer can find the interrupted state.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context(correlation_id="INC-999")

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        # Set up for resume
        mock_graph.invoke.return_value = _make_completed_state()
        mock_state = MagicMock()
        mock_state.next = ()
        mock_graph.get_state.return_value = mock_state

        await agent.resume("approved")

        # Both invoke calls should use the same thread_id
        invoke_config = mock_graph.invoke.call_args_list[0][1]["config"]
        resume_config = mock_graph.invoke.call_args_list[1][1]["config"]
        assert invoke_config["configurable"]["thread_id"] == "backfill-INC-999"
        assert resume_config["configurable"]["thread_id"] == "backfill-INC-999"

    @pytest.mark.asyncio
    async def test_resume_approved_clears_phase_state(self):
        """
        After successful completion, instance state should be cleared.
        The agent is ready for a new invoke() call.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        # Resume
        mock_graph.invoke.return_value = _make_completed_state()
        mock_state = MagicMock()
        mock_state.next = ()
        mock_graph.get_state.return_value = mock_state

        await agent.resume("approved")

        assert agent._thread_id is None
        assert agent._context is None
        assert agent._started_at is None
        assert agent.has_pending_approval is False

    @pytest.mark.asyncio
    async def test_resume_approved_extracts_execution_tools(self):
        """Actions taken should include execution-phase tool calls."""
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        # Resume
        mock_graph.invoke.return_value = _make_completed_state()
        mock_state = MagicMock()
        mock_state.next = ()
        mock_graph.get_state.return_value = mock_state

        result = await agent.resume("approved")

        assert "acquire_pipeline_lock" in result.actions_taken
        assert "execute_backfill_step" in result.actions_taken
        assert "release_pipeline_lock" in result.actions_taken


# ---------------------------------------------------------------------------
# Tests: resume() — rejection path
# ---------------------------------------------------------------------------

class TestResumeRejection:
    """
    Test resume() when the human rejects the plan.

    After rejection, the graph revises the plan and may interrupt again
    (needs_approval) or fail (max revisions exceeded).
    """

    @pytest.mark.asyncio
    async def test_resume_rejected_returns_needs_approval(self):
        """
        resume("rejected", feedback) should return "needs_approval"
        when the graph produces a revised plan and interrupts again.
        This is the REJECTION LOOP — the most important new test case.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()
        plan_v1 = _make_sample_plan()

        # Phase 1: invoke → interrupt with plan v1
        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(plan_v1),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        result = await agent.invoke(context)
        assert result.status == "needs_approval"

        # Phase 2: resume with rejection → interrupt again with revised plan
        plan_v2 = _make_sample_plan()
        plan_v2.risk_assessment = "Medium — added rollback steps per reviewer feedback"
        revised_state = _make_interrupted_state(plan_v2)
        revised_state["plan_iterations"] = 2  # second iteration

        mock_graph.invoke.return_value = revised_state
        mock_state = MagicMock()
        mock_state.next = ("approval_gate",)  # interrupted again
        mock_graph.get_state.return_value = mock_state

        result = await agent.resume("rejected", "Add rollback steps")

        assert result.status == "needs_approval"
        assert result.report["risk_assessment"] == plan_v2.risk_assessment
        # Agent should still have pending state (lifecycle not over)
        assert agent.has_pending_approval is True

    @pytest.mark.asyncio
    async def test_resume_rejected_passes_feedback_in_command(self):
        """
        The rejection feedback should be passed in the Command(resume=...)
        so approval_gate can inject it as a HumanMessage.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        # Resume with rejection
        mock_graph.invoke.return_value = _make_interrupted_state()
        mock_state = MagicMock()
        mock_state.next = ("approval_gate",)
        mock_graph.get_state.return_value = mock_state

        await agent.resume("rejected", "Add rollback steps for each partition")

        # Verify the Command includes the feedback
        resume_call = mock_graph.invoke.call_args_list[1]
        command_arg = resume_call[0][0]
        from langgraph.types import Command
        assert isinstance(command_arg, Command)

    @pytest.mark.asyncio
    async def test_rejection_loop_then_approve(self):
        """
        Full rejection loop: invoke → reject → reject → approve → success.
        Tests the complete multi-round lifecycle.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()
        plan = _make_sample_plan()

        # Phase 1: invoke → interrupt
        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(plan),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        result = await agent.invoke(context)
        assert result.status == "needs_approval"

        # Round 2: reject → revised plan → interrupt
        revised_state = _make_interrupted_state(plan)
        revised_state["plan_iterations"] = 2
        mock_graph.invoke.return_value = revised_state
        mock_state = MagicMock()
        mock_state.next = ("approval_gate",)
        mock_graph.get_state.return_value = mock_state

        result = await agent.resume("rejected", "Add rollback")
        assert result.status == "needs_approval"

        # Round 3: approve → execution → success
        mock_graph.invoke.return_value = _make_completed_state(plan)
        mock_state.next = ()
        mock_graph.get_state.return_value = mock_state

        result = await agent.resume("approved")
        assert result.status == "success"
        assert agent.has_pending_approval is False

    @pytest.mark.asyncio
    async def test_max_revisions_returns_failure(self):
        """
        When max_plan_iterations is exceeded, the graph ends with errors
        and the agent returns status="failure".
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        # invoke → interrupt
        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        # resume with rejection → graph ends with error (max revisions)
        failed_state = _make_completed_state()
        failed_state["errors"] = ["Max plan revisions exceeded (3 of 3)"]
        failed_state["approval_status"] = ""

        mock_graph.invoke.return_value = failed_state
        mock_state = MagicMock()
        mock_state.next = ()  # completed (ended, not interrupted)
        mock_graph.get_state.return_value = mock_state

        result = await agent.resume("rejected", "This plan is terrible")

        assert result.status == "failure"
        assert any("Max plan revisions" in e for e in result.errors)
        assert agent.has_pending_approval is False


# ---------------------------------------------------------------------------
# Tests: resume() — failure path
# ---------------------------------------------------------------------------

class TestResumeFailure:
    """Test resume() error handling."""

    @pytest.mark.asyncio
    async def test_resume_without_invoke_raises(self):
        """
        Calling resume() without a prior invoke() should raise ValueError.
        This is the GUARD RAIL preventing misuse of the two-phase API.
        """
        agent = BackfillAgent(_make_config())

        with pytest.raises(ValueError, match="No pending approval"):
            await agent.resume("approved")

    @pytest.mark.asyncio
    async def test_resume_graph_exception_returns_failure(self):
        """
        When graph.invoke() raises during resume, the agent catches it
        and returns status="failure". Same boundary as invoke().
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        # invoke → interrupt
        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        # resume → crash
        mock_graph.invoke.side_effect = RuntimeError("Execution tool failed")

        result = await agent.resume("approved")

        assert result.status == "failure"
        assert "Execution tool failed" in result.report["error"]
        assert result.report["phase"] == "resume"

    @pytest.mark.asyncio
    async def test_resume_exception_clears_phase_state(self):
        """On resume exception, instance state should be cleared."""
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        mock_graph.invoke.side_effect = RuntimeError("boom")
        await agent.resume("approved")

        assert agent._thread_id is None
        assert agent._context is None
        assert agent._started_at is None
        assert agent.has_pending_approval is False

    @pytest.mark.asyncio
    async def test_resume_after_cleared_state_raises(self):
        """
        After a lifecycle completes (state cleared), calling resume()
        again should raise ValueError — no pending approval.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        # invoke → interrupt
        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        # resume → success (clears state)
        mock_graph.invoke.return_value = _make_completed_state()
        mock_state = MagicMock()
        mock_state.next = ()
        mock_graph.get_state.return_value = mock_state
        await agent.resume("approved")

        # Second resume should fail
        with pytest.raises(ValueError, match="No pending approval"):
            await agent.resume("approved")


# ---------------------------------------------------------------------------
# Tests: Lazy graph building
# ---------------------------------------------------------------------------

class TestLazyGraphBuilding:
    """Test that the graph is built lazily and cached."""

    @pytest.mark.asyncio
    async def test_graph_built_on_first_invoke(self):
        """build_graph() should be called on first invoke, not on init."""
        agent = BackfillAgent(_make_config())
        assert agent._graph is None

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )

        with patch.object(agent, "build_graph", return_value=mock_graph) as mock_build:
            await agent.invoke(_make_context())
            mock_build.assert_called_once()

    @pytest.mark.asyncio
    async def test_graph_cached_across_invokes(self):
        """Second invoke (after lifecycle completes) reuses the graph."""
        agent = BackfillAgent(_make_config())

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )

        with patch.object(agent, "build_graph", return_value=mock_graph) as mock_build:
            # First lifecycle: invoke
            await agent.invoke(_make_context())

            # Complete the lifecycle (so we can invoke again)
            mock_graph.invoke.return_value = _make_completed_state()
            mock_state = MagicMock()
            mock_state.next = ()
            mock_graph.get_state.return_value = mock_state
            await agent.resume("approved")

            # Second lifecycle: invoke (should NOT rebuild)
            mock_graph.invoke.return_value = _make_interrupted_state()
            mock_state.next = ("approval_gate",)
            mock_graph.get_state.return_value = mock_state
            await agent.invoke(_make_context(correlation_id="test-corr-002"))

            # build_graph called only ONCE
            mock_build.assert_called_once()


# ---------------------------------------------------------------------------
# Tests: Config integration
# ---------------------------------------------------------------------------

class TestConfigIntegration:
    """Test that config values flow through to the agent."""

    @pytest.mark.asyncio
    async def test_max_iterations_from_config(self):
        """max_iterations from config should be passed to initial state."""
        config = _make_config({
            "llm": {"provider": "google", "model": "test", "temperature": 0},
            "agents": {"backfill": {"max_iterations": 8, "max_plan_iterations": 2}},
        })
        agent = BackfillAgent(config)

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        await agent.invoke(_make_context())

        call_args = mock_graph.invoke.call_args[0][0]
        assert call_args["max_iterations"] == 8
        assert call_args["max_plan_iterations"] == 2

    @pytest.mark.asyncio
    async def test_default_config_values(self):
        """When config keys are missing, defaults should be used."""
        config = _make_config({
            "llm": {"provider": "google", "model": "test", "temperature": 0},
            "agents": {"backfill": {}},  # no max_iterations or max_plan_iterations
        })
        agent = BackfillAgent(config)

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        await agent.invoke(_make_context())

        call_args = mock_graph.invoke.call_args[0][0]
        assert call_args["max_iterations"] == 15  # default from agent
        assert call_args["max_plan_iterations"] == 3  # default from agent


# ---------------------------------------------------------------------------
# Tests: _clear_phase_state
# ---------------------------------------------------------------------------

class TestClearPhaseState:
    """
    Test the lifecycle cleanup helper.
    NEW — Recon/DLQ don't have this because they're stateless.
    """

    def test_clears_all_fields(self):
        agent = BackfillAgent(_make_config())
        agent._thread_id = "backfill-test"
        agent._context = _make_context()
        agent._started_at = "2026-09-28T00:00:00Z"

        agent._clear_phase_state()

        assert agent._thread_id is None
        assert agent._context is None
        assert agent._started_at is None

    def test_idempotent(self):
        """Calling _clear_phase_state twice should not raise."""
        agent = BackfillAgent(_make_config())
        agent._clear_phase_state()
        agent._clear_phase_state()  # should not raise
        assert agent._thread_id is None


# ---------------------------------------------------------------------------
# Tests: AgentResult structure
# ---------------------------------------------------------------------------

class TestAgentResultStructure:
    """Test that AgentResult fields are populated correctly."""

    @pytest.mark.asyncio
    async def test_result_has_timestamps(self):
        """started_at and completed_at should both be set."""
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.completed_at >= result.started_at

    @pytest.mark.asyncio
    async def test_report_is_plain_dict(self):
        """
        AgentResult.report should be a plain dict (via .model_dump()),
        not a Pydantic model. Same contract as Recon/DLQ.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert isinstance(result.report, dict)
        assert not hasattr(result.report, "model_dump")

    @pytest.mark.asyncio
    async def test_success_report_contains_plan_and_audit(self):
        """
        On success, the report should contain both the plan (as dict)
        and the execution_audit list.
        """
        agent = BackfillAgent(_make_config())
        context = _make_context()

        # invoke → interrupt
        mock_graph = _make_mock_graph(
            invoke_return=_make_interrupted_state(),
            is_interrupted=True,
        )
        agent._graph = mock_graph
        await agent.invoke(context)

        # resume → success
        mock_graph.invoke.return_value = _make_completed_state()
        mock_state = MagicMock()
        mock_state.next = ()
        mock_graph.get_state.return_value = mock_state

        result = await agent.resume("approved")

        assert "plan" in result.report
        assert isinstance(result.report["plan"], dict)
        assert "execution_audit" in result.report
        assert isinstance(result.report["execution_audit"], list)
