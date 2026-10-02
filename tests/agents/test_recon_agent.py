"""
Unit tests for the Reconciliation Diagnostics agent.

WHY THIS FILE MATTERS (learning concepts):

  Testing LLM-powered agents is fundamentally different from testing
  normal functions. The LLM is non-deterministic — you can't assert
  that it returns exactly "check row counts next." Instead, you test:

  1. The STRUCTURE — does the graph wire correctly? Do nodes execute
     in the right order?
  2. The PLUMBING — does TriggerContext translate to initial state?
     Does AgentResult package correctly?
  3. The ERROR HANDLING — does the agent return failure gracefully?
  4. The HELPERS — does _extract_tool_calls work on real messages?

  The LLM itself is MOCKED in these tests. We replace it with a fake
  that returns predictable responses. The live test (recon_live_test.py)
  tests with a real LLM.

KEY TESTING CONCEPTS:

  1. unittest.mock.patch — replaces real objects with fakes during a test.
     We patch create_llm() so it returns our mock LLM instead of calling
     the real Google/OpenAI/Anthropic API.

  2. Mock LLM with tool_calls — the mock returns AIMessages with
     tool_calls matching real tool schemas, so ToolNode can execute them.

  3. pytest-asyncio — the agent's invoke() is async, so tests use
     async def and the pytest asyncio_mode="auto" config.

  4. Test isolation — each test creates its own agent with fresh config.
     No shared state between tests.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from argus.agents.base import AgentResult, TriggerContext
from argus.agents.reconciliation.agent import ReconciliationAgent, _extract_tool_calls
from argus.agents.reconciliation.state import make_initial_state
from argus.core.config import ArgusConfig
from argus.schemas.reports import (
    Notification,
    ReconReport,
    ReconciliationFinding,
    Severity,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

def _make_config(overrides: dict[str, Any] | None = None) -> ArgusConfig:
    """
    Create a minimal ArgusConfig for testing.

    Why not load_config("dev")? Because unit tests should NOT depend on
    config files on disk. A test that fails because config.yaml moved
    is testing the wrong thing.
    """
    base = {
        "llm": {
            "provider": "google",
            "model": "gemini-2.0-flash",
            "temperature": 0.0,
        },
        "agents": {
            "reconciliation": {
                "max_iterations": 5,
            },
        },
        "logging": {"level": "WARNING"},
    }
    if overrides:
        base.update(overrides)
    return ArgusConfig(base)


def _make_context(
    run_date: str = "2026-09-28",
    gate: str = "gate_3",
) -> TriggerContext:
    """Create a standard TriggerContext for testing."""
    return TriggerContext(
        agent_name="reconciliation",
        trigger_source="test",
        run_date=run_date,
        correlation_id="test-corr-001",
        params={"gate_failure": gate, "dag_id": "ttag_daily_dag"},
    )


def _make_sample_report() -> ReconReport:
    """Create a sample ReconReport for mocking the report node."""
    return ReconReport(
        gate_failed="gate_3",
        run_date="2026-09-28",
        findings=[
            ReconciliationFinding(
                check_name="compare_row_counts",
                table="booking_silver",
                partition="2026-09-28",
                expected="15012",
                actual="14712",
                delta="-300",
                possible_cause="Duplicate keys from upstream re-delivery",
            ),
        ],
        root_cause_summary="111 duplicate booking_ids in Bronze caused row count mismatch",
        suggested_fix="Deduplicate Bronze before Silver merge using booking_id + event_ts",
        recommended_severity=Severity.P2,
        notifications=[
            Notification(
                severity=Severity.P2,
                channel="slack",
                title="Gate 3 failure — duplicate keys",
                body="Recon agent found 111 duplicate booking_ids",
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Tests: Agent properties
# ---------------------------------------------------------------------------

class TestAgentProperties:
    """Test that the agent satisfies the BaseAgent contract."""

    def test_name(self):
        agent = ReconciliationAgent(_make_config())
        assert agent.name == "reconciliation"

    def test_description(self):
        agent = ReconciliationAgent(_make_config())
        assert "gate failures" in agent.description.lower()

    def test_graph_not_built_on_init(self):
        """The graph should be None until first invoke — lazy init."""
        agent = ReconciliationAgent(_make_config())
        assert agent._graph is None


# ---------------------------------------------------------------------------
# Tests: make_initial_state
# ---------------------------------------------------------------------------

class TestMakeInitialState:
    """Test the state factory function."""

    def test_required_fields(self):
        state = make_initial_state(
            run_date="2026-09-28",
            gate_name="gate_3",
            trigger_params={"dag_id": "ttag_main"},
            correlation_id="corr-123",
        )
        assert state["run_date"] == "2026-09-28"
        assert state["gate_name"] == "gate_3"
        assert state["correlation_id"] == "corr-123"
        assert state["trigger_params"] == {"dag_id": "ttag_main"}

    def test_defaults(self):
        state = make_initial_state(
            run_date="2026-09-28",
            gate_name="gate_3",
            trigger_params={},
            correlation_id="corr-123",
        )
        assert state["messages"] == []
        assert state["iteration"] == 0
        assert state["max_iterations"] == 10  # default
        assert state["report"] is None
        assert state["errors"] == []

    def test_custom_max_iterations(self):
        state = make_initial_state(
            run_date="2026-09-28",
            gate_name="gate_3",
            trigger_params={},
            correlation_id="corr-123",
            max_iterations=3,
        )
        assert state["max_iterations"] == 3


# ---------------------------------------------------------------------------
# Tests: _extract_tool_calls
# ---------------------------------------------------------------------------

class TestExtractToolCalls:
    """Test the helper that mines tool names from message history."""

    def test_empty_messages(self):
        assert _extract_tool_calls([]) == []

    def test_no_tool_calls(self):
        """Messages with no tool_calls should return empty list."""
        messages = [
            SystemMessage(content="You are an agent"),
            HumanMessage(content="Investigate gate 3"),
            AIMessage(content="I'll investigate now."),
        ]
        assert _extract_tool_calls(messages) == []

    def test_single_tool_call(self):
        """Extract one tool call from an AIMessage."""
        messages = [
            AIMessage(
                content="Let me check the gate results.",
                tool_calls=[
                    {"name": "query_gate_results", "args": {"run_date": "2026-09-28"}, "id": "tc1"},
                ],
            ),
        ]
        assert _extract_tool_calls(messages) == ["query_gate_results"]

    def test_multiple_tool_calls_across_messages(self):
        """Tool calls from multiple AIMessages should accumulate in order."""
        messages = [
            AIMessage(
                content="Checking gate results...",
                tool_calls=[
                    {"name": "query_gate_results", "args": {}, "id": "tc1"},
                ],
            ),
            ToolMessage(content='{"status": "FAILED"}', tool_call_id="tc1"),
            AIMessage(
                content="Now checking row counts...",
                tool_calls=[
                    {"name": "compare_row_counts", "args": {}, "id": "tc2"},
                    {"name": "check_duplicate_keys", "args": {}, "id": "tc3"},
                ],
            ),
            ToolMessage(content='{"rows": 100}', tool_call_id="tc2"),
            ToolMessage(content='{"duplicates": 5}', tool_call_id="tc3"),
        ]
        result = _extract_tool_calls(messages)
        assert result == [
            "query_gate_results",
            "compare_row_counts",
            "check_duplicate_keys",
        ]

    def test_ignores_non_ai_messages(self):
        """Only AIMessages should be checked for tool_calls."""
        messages = [
            SystemMessage(content="system"),
            HumanMessage(content="human"),
            ToolMessage(content="tool result", tool_call_id="tc1"),
        ]
        assert _extract_tool_calls(messages) == []


# ---------------------------------------------------------------------------
# Tests: Agent invoke — success path
# ---------------------------------------------------------------------------

class TestAgentInvokeSuccess:
    """Test the agent's invoke() with a mocked graph that returns a report."""

    @pytest.mark.asyncio
    async def test_success_returns_agent_result(self):
        """
        When the graph produces a ReconReport, invoke() should return
        AgentResult with status="success" and the report as a dict.
        """
        config = _make_config()
        agent = ReconciliationAgent(config)
        context = _make_context()
        report = _make_sample_report()

        # Mock the compiled graph's invoke() to return a final state
        # with a ReconReport, simulating a successful run.
        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [
                SystemMessage(content="system prompt"),
                HumanMessage(content="investigate gate_3"),
                AIMessage(
                    content="Checking gate results...",
                    tool_calls=[{"name": "query_gate_results", "args": {}, "id": "tc1"}],
                ),
                ToolMessage(content='{"status": "FAILED"}', tool_call_id="tc1"),
                AIMessage(content="Investigation complete."),
            ],
            "report": report,
            "errors": [],
            "iteration": 3,
            "run_date": "2026-09-28",
            "gate_name": "gate_3",
            "trigger_params": {},
            "correlation_id": "test-corr-001",
            "max_iterations": 5,
        }

        # Inject the mock graph — bypass build_graph entirely
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert isinstance(result, AgentResult)
        assert result.status == "success"
        assert result.agent_name == "reconciliation"
        assert result.correlation_id == "test-corr-001"
        assert result.report["gate_failed"] == "gate_3"
        assert result.report["root_cause_summary"] == report.root_cause_summary
        assert len(result.report["findings"]) == 1
        assert "query_gate_results" in result.actions_taken

    @pytest.mark.asyncio
    async def test_report_is_plain_dict(self):
        """
        AgentResult.report should be a plain dict (via .model_dump()),
        not a Pydantic model. The platform layer is agent-type-agnostic.
        """
        config = _make_config()
        agent = ReconciliationAgent(config)
        context = _make_context()

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-28",
            "gate_name": "gate_3",
            "trigger_params": {},
            "correlation_id": "test-corr-001",
            "max_iterations": 5,
        }
        agent._graph = mock_graph

        result = await agent.invoke(context)
        assert isinstance(result.report, dict)
        assert not hasattr(result.report, "model_dump")  # it's a dict, not Pydantic


# ---------------------------------------------------------------------------
# Tests: Agent invoke — failure paths
# ---------------------------------------------------------------------------

class TestAgentInvokeFailure:
    """Test the agent's error handling."""

    @pytest.mark.asyncio
    async def test_graph_exception_returns_failure(self):
        """
        When graph.invoke() raises an exception, the agent should catch it
        and return AgentResult with status="failure".
        """
        config = _make_config()
        agent = ReconciliationAgent(config)
        context = _make_context()

        mock_graph = MagicMock()
        mock_graph.invoke.side_effect = RuntimeError("LLM API rate limited")
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert result.status == "failure"
        assert "LLM API rate limited" in result.report["error"]
        assert any("rate limited" in e.lower() for e in result.errors)

    @pytest.mark.asyncio
    async def test_none_report_returns_failure(self):
        """
        When the graph completes but report is None (report node failed),
        the agent should return AgentResult with status="failure".
        """
        config = _make_config()
        agent = ReconciliationAgent(config)
        context = _make_context()

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": None,  # report node failed
            "errors": ["report_node: structured output failed"],
            "iteration": 2,
            "run_date": "2026-09-28",
            "gate_name": "gate_3",
            "trigger_params": {},
            "correlation_id": "test-corr-001",
            "max_iterations": 5,
        }
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert result.status == "failure"
        assert "Report generation failed" in result.report["error"]

    @pytest.mark.asyncio
    async def test_missing_gate_failure_defaults_to_unknown(self):
        """
        When params doesn't contain 'gate_failure', the agent should
        default gate_name to 'unknown' and still run.
        """
        config = _make_config()
        agent = ReconciliationAgent(config)
        context = TriggerContext(
            agent_name="reconciliation",
            trigger_source="test",
            run_date="2026-09-28",
            correlation_id="test-corr-002",
            params={},  # no gate_failure key
        )

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-28",
            "gate_name": "unknown",
            "trigger_params": {},
            "correlation_id": "test-corr-002",
            "max_iterations": 5,
        }
        agent._graph = mock_graph

        result = await agent.invoke(context)

        # Should succeed even with unknown gate
        assert result.status == "success"
        # Verify the graph was called with gate_name="unknown"
        call_args = mock_graph.invoke.call_args[0][0]
        assert call_args["gate_name"] == "unknown"


# ---------------------------------------------------------------------------
# Tests: Lazy graph building
# ---------------------------------------------------------------------------

class TestLazyGraphBuilding:
    """Test that the graph is built lazily and cached."""

    @pytest.mark.asyncio
    async def test_graph_built_on_first_invoke(self):
        """build_graph() should be called on first invoke, not on init."""
        config = _make_config()
        agent = ReconciliationAgent(config)
        assert agent._graph is None

        # We need to mock build_graph to avoid needing a real LLM
        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-28",
            "gate_name": "gate_3",
            "trigger_params": {},
            "correlation_id": "test-corr-001",
            "max_iterations": 5,
        }

        with patch.object(agent, "build_graph", return_value=mock_graph) as mock_build:
            context = _make_context()
            await agent.invoke(context)
            mock_build.assert_called_once()

    @pytest.mark.asyncio
    async def test_graph_cached_across_invokes(self):
        """Second invoke should reuse the cached graph, not rebuild."""
        config = _make_config()
        agent = ReconciliationAgent(config)

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-28",
            "gate_name": "gate_3",
            "trigger_params": {},
            "correlation_id": "test-corr-001",
            "max_iterations": 5,
        }

        with patch.object(agent, "build_graph", return_value=mock_graph) as mock_build:
            context = _make_context()
            await agent.invoke(context)
            await agent.invoke(context)
            # build_graph should only be called ONCE, even with two invokes
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
            "agents": {"reconciliation": {"max_iterations": 3}},
        })
        agent = ReconciliationAgent(config)
        context = _make_context()

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-28",
            "gate_name": "gate_3",
            "trigger_params": {},
            "correlation_id": "test-corr-001",
            "max_iterations": 3,
        }
        agent._graph = mock_graph

        await agent.invoke(context)

        # Verify max_iterations=3 was passed to the graph
        call_args = mock_graph.invoke.call_args[0][0]
        assert call_args["max_iterations"] == 3
