"""
Unit tests for the DLQ Triage & Auto-Remediation agent.

WHY THIS FILE MATTERS (learning concepts):

  This is the second agent test file in Argus. Comparing with
  test_recon_agent.py (Phase 2), the test STRUCTURE is nearly identical.
  This is intentional — it validates that the BaseAgent contract gives
  us a consistent, testable interface across all agents.

WHAT'S THE SAME:

  - Same 7 test classes covering the same concerns:
    1. Agent properties (name, description, lazy graph)
    2. State factory (make_initial_state)
    3. Helper function (_extract_tool_calls)
    4. Invoke success path (mocked graph → AgentResult)
    5. Invoke failure paths (exception, None report, missing params)
    6. Lazy graph building (build once, cache)
    7. Config integration (max_iterations flows through)

  - Same mock strategy: MagicMock for the compiled graph, injected via
    agent._graph = mock_graph. No real LLM calls.

WHAT'S DIFFERENT:

  - DLQ-specific fields: source_lane instead of gate_name,
    classifications and requeue_audit accumulators
  - DLQ-specific report: DLQTriageReport with per-record classifications,
    auto_requeued, quarantined, escalated counts
  - Default source_lane is "both" (not "unknown" like gate_name)

THE TESTING INSIGHT:

  When two agents share the same interface and follow the same patterns,
  their tests look almost identical. This is a GOOD sign — it means the
  abstraction (BaseAgent) is working. If the tests had wildly different
  structures, it would mean the agents are diverging from the contract,
  which makes the platform harder to maintain.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from argus.agents.base import AgentResult, TriggerContext
from argus.agents.dlq_triage.agent import DLQTriageAgent, _extract_tool_calls
from argus.agents.dlq_triage.state import make_initial_state
from argus.core.config import ArgusConfig
from argus.schemas.reports import (
    DLQClassification,
    DLQRecord,
    DLQTriageReport,
    Notification,
    Severity,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

def _make_config(overrides: dict[str, Any] | None = None) -> ArgusConfig:
    """
    Create a minimal ArgusConfig for testing.

    Same pattern as the Recon tests — unit tests should NOT depend on
    config files on disk.
    """
    base = {
        "llm": {
            "provider": "google",
            "model": "gemini-2.0-flash",
            "temperature": 0.0,
        },
        "agents": {
            "dlq_triage": {
                "max_iterations": 5,
            },
        },
        "logging": {"level": "WARNING"},
    }
    if overrides:
        base.update(overrides)
    return ArgusConfig(base)


def _make_context(
    run_date: str = "2026-09-30",
    source_lane: str = "kafka_dlq",
) -> TriggerContext:
    """Create a standard TriggerContext for testing."""
    return TriggerContext(
        agent_name="dlq_triage",
        trigger_source="test",
        run_date=run_date,
        correlation_id="test-corr-dlq-001",
        params={
            "dlq_threshold_breached": True,
            "source_lane": source_lane,
        },
    )


def _make_sample_report() -> DLQTriageReport:
    """Create a sample DLQTriageReport for mocking the report node."""
    return DLQTriageReport(
        total_records=3,
        records=[
            DLQRecord(
                record_id="dlq-kafka-001",
                source_lane="kafka_dlq",
                classification=DLQClassification.TRANSIENT,
                confidence=0.90,
                reason="TimeoutException — transient broker failure",
                action_taken="requeued",
            ),
            DLQRecord(
                record_id="dlq-kafka-003",
                source_lane="kafka_dlq",
                classification=DLQClassification.SCHEMA_MISMATCH,
                confidence=0.95,
                reason="SchemaRegistryException — consumer expects schema v3 but got v4",
                action_taken="quarantined",
            ),
            DLQRecord(
                record_id="dlq-kafka-005",
                source_lane="kafka_dlq",
                classification=DLQClassification.DATA_QUALITY,
                confidence=0.90,
                reason="NullPointerException — benefit_type is null (required field)",
                action_taken="quarantined",
            ),
        ],
        auto_requeued=1,
        quarantined=2,
        escalated=0,
        summary="3 DLQ records triaged: 1 transient (requeued), 1 schema mismatch, 1 data quality",
        recommended_severity=Severity.P2,
        notifications=[
            Notification(
                severity=Severity.P2,
                channel="slack",
                title="DLQ Triage — schema mismatch detected",
                body="Consumer schema update needed for benefit_value_v4",
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Tests: Agent properties
# ---------------------------------------------------------------------------

class TestAgentProperties:
    """Test that the agent satisfies the BaseAgent contract."""

    def test_name(self):
        agent = DLQTriageAgent(_make_config())
        assert agent.name == "dlq_triage"

    def test_description(self):
        agent = DLQTriageAgent(_make_config())
        assert "classif" in agent.description.lower()

    def test_graph_not_built_on_init(self):
        """The graph should be None until first invoke — lazy init."""
        agent = DLQTriageAgent(_make_config())
        assert agent._graph is None


# ---------------------------------------------------------------------------
# Tests: make_initial_state
# ---------------------------------------------------------------------------

class TestMakeInitialState:
    """Test the DLQ state factory function."""

    def test_required_fields(self):
        state = make_initial_state(
            run_date="2026-09-30",
            source_lane="kafka_dlq",
            trigger_params={"dlq_threshold_breached": True},
            correlation_id="corr-dlq-123",
        )
        assert state["run_date"] == "2026-09-30"
        assert state["source_lane"] == "kafka_dlq"
        assert state["correlation_id"] == "corr-dlq-123"
        assert state["trigger_params"] == {"dlq_threshold_breached": True}

    def test_defaults(self):
        state = make_initial_state(
            run_date="2026-09-30",
            source_lane="both",
            trigger_params={},
            correlation_id="corr-dlq-123",
        )
        assert state["messages"] == []
        assert state["iteration"] == 0
        assert state["max_iterations"] == 10
        assert state["classifications"] == []
        assert state["requeue_audit"] == []
        assert state["report"] is None
        assert state["errors"] == []

    def test_custom_max_iterations(self):
        state = make_initial_state(
            run_date="2026-09-30",
            source_lane="bad_files",
            trigger_params={},
            correlation_id="corr-dlq-123",
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
        messages = [
            SystemMessage(content="You are an agent"),
            HumanMessage(content="Triage the DLQ"),
            AIMessage(content="I'll investigate now."),
        ]
        assert _extract_tool_calls(messages) == []

    def test_single_tool_call(self):
        messages = [
            AIMessage(
                content="Let me read the DLQ records.",
                tool_calls=[
                    {"name": "read_dlq_records", "args": {"run_date": "2026-09-30", "source_lane": "kafka_dlq"}, "id": "tc1"},
                ],
            ),
        ]
        assert _extract_tool_calls(messages) == ["read_dlq_records"]

    def test_multiple_tool_calls_across_messages(self):
        """Tool calls from multiple AIMessages should accumulate in order."""
        messages = [
            AIMessage(
                content="Reading DLQ records...",
                tool_calls=[
                    {"name": "read_dlq_records", "args": {}, "id": "tc1"},
                ],
            ),
            ToolMessage(content='{"records": []}', tool_call_id="tc1"),
            AIMessage(
                content="Checking schema changelog and requeuing...",
                tool_calls=[
                    {"name": "query_schema_changelog", "args": {}, "id": "tc2"},
                    {"name": "requeue_message", "args": {}, "id": "tc3"},
                ],
            ),
            ToolMessage(content='{"entries": []}', tool_call_id="tc2"),
            ToolMessage(content='{"status": "requeued"}', tool_call_id="tc3"),
        ]
        result = _extract_tool_calls(messages)
        assert result == [
            "read_dlq_records",
            "query_schema_changelog",
            "requeue_message",
        ]

    def test_ignores_non_ai_messages(self):
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
        When the graph produces a DLQTriageReport, invoke() should return
        AgentResult with status="success" and the report as a dict.
        """
        config = _make_config()
        agent = DLQTriageAgent(config)
        context = _make_context()
        report = _make_sample_report()

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [
                SystemMessage(content="system prompt"),
                HumanMessage(content="triage kafka_dlq"),
                AIMessage(
                    content="Reading DLQ records...",
                    tool_calls=[{"name": "read_dlq_records", "args": {}, "id": "tc1"}],
                ),
                ToolMessage(content='{"records": []}', tool_call_id="tc1"),
                AIMessage(content="Triage complete."),
            ],
            "report": report,
            "errors": [],
            "iteration": 3,
            "run_date": "2026-09-30",
            "source_lane": "kafka_dlq",
            "trigger_params": {"dlq_threshold_breached": True},
            "correlation_id": "test-corr-dlq-001",
            "max_iterations": 5,
            "classifications": [],
            "requeue_audit": [],
        }

        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert isinstance(result, AgentResult)
        assert result.status == "success"
        assert result.agent_name == "dlq_triage"
        assert result.correlation_id == "test-corr-dlq-001"
        assert result.report["total_records"] == 3
        assert result.report["auto_requeued"] == 1
        assert result.report["quarantined"] == 2
        assert len(result.report["records"]) == 3
        assert "read_dlq_records" in result.actions_taken

    @pytest.mark.asyncio
    async def test_report_is_plain_dict(self):
        """AgentResult.report should be a plain dict, not a Pydantic model."""
        config = _make_config()
        agent = DLQTriageAgent(config)
        context = _make_context()

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-30",
            "source_lane": "kafka_dlq",
            "trigger_params": {},
            "correlation_id": "test-corr-dlq-001",
            "max_iterations": 5,
            "classifications": [],
            "requeue_audit": [],
        }
        agent._graph = mock_graph

        result = await agent.invoke(context)
        assert isinstance(result.report, dict)
        assert not hasattr(result.report, "model_dump")


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
        agent = DLQTriageAgent(config)
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
        agent = DLQTriageAgent(config)
        context = _make_context()

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": None,
            "errors": ["report_node: structured output failed"],
            "iteration": 2,
            "run_date": "2026-09-30",
            "source_lane": "kafka_dlq",
            "trigger_params": {},
            "correlation_id": "test-corr-dlq-001",
            "max_iterations": 5,
            "classifications": [],
            "requeue_audit": [],
        }
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert result.status == "failure"
        assert "Report generation failed" in result.report["error"]

    @pytest.mark.asyncio
    async def test_missing_source_lane_defaults_to_both(self):
        """
        When params doesn't contain 'source_lane', the agent should
        default to 'both' and still run.
        """
        config = _make_config()
        agent = DLQTriageAgent(config)
        context = TriggerContext(
            agent_name="dlq_triage",
            trigger_source="test",
            run_date="2026-09-30",
            correlation_id="test-corr-dlq-002",
            params={"dlq_threshold_breached": True},  # no source_lane
        )

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-30",
            "source_lane": "both",
            "trigger_params": {"dlq_threshold_breached": True},
            "correlation_id": "test-corr-dlq-002",
            "max_iterations": 5,
            "classifications": [],
            "requeue_audit": [],
        }
        agent._graph = mock_graph

        result = await agent.invoke(context)

        assert result.status == "success"
        # Verify the graph was called with source_lane="both"
        call_args = mock_graph.invoke.call_args[0][0]
        assert call_args["source_lane"] == "both"


# ---------------------------------------------------------------------------
# Tests: Lazy graph building
# ---------------------------------------------------------------------------

class TestLazyGraphBuilding:
    """Test that the graph is built lazily and cached."""

    @pytest.mark.asyncio
    async def test_graph_built_on_first_invoke(self):
        """build_graph() should be called on first invoke, not on init."""
        config = _make_config()
        agent = DLQTriageAgent(config)
        assert agent._graph is None

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-30",
            "source_lane": "kafka_dlq",
            "trigger_params": {},
            "correlation_id": "test-corr-dlq-001",
            "max_iterations": 5,
            "classifications": [],
            "requeue_audit": [],
        }

        with patch.object(agent, "build_graph", return_value=mock_graph) as mock_build:
            context = _make_context()
            await agent.invoke(context)
            mock_build.assert_called_once()

    @pytest.mark.asyncio
    async def test_graph_cached_across_invokes(self):
        """Second invoke should reuse the cached graph, not rebuild."""
        config = _make_config()
        agent = DLQTriageAgent(config)

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-30",
            "source_lane": "kafka_dlq",
            "trigger_params": {},
            "correlation_id": "test-corr-dlq-001",
            "max_iterations": 5,
            "classifications": [],
            "requeue_audit": [],
        }

        with patch.object(agent, "build_graph", return_value=mock_graph) as mock_build:
            context = _make_context()
            await agent.invoke(context)
            await agent.invoke(context)
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
            "agents": {"dlq_triage": {"max_iterations": 3}},
        })
        agent = DLQTriageAgent(config)
        context = _make_context()

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-30",
            "source_lane": "kafka_dlq",
            "trigger_params": {},
            "correlation_id": "test-corr-dlq-001",
            "max_iterations": 3,
            "classifications": [],
            "requeue_audit": [],
        }
        agent._graph = mock_graph

        await agent.invoke(context)

        call_args = mock_graph.invoke.call_args[0][0]
        assert call_args["max_iterations"] == 3

    @pytest.mark.asyncio
    async def test_fallback_to_global_max_iterations(self):
        """
        When agents.dlq_triage.max_iterations is not set, fall back to
        agents.max_iterations (the global default).
        """
        config = _make_config({
            "llm": {"provider": "google", "model": "test", "temperature": 0},
            "agents": {"max_iterations": 8},  # global, no dlq_triage-specific
        })
        agent = DLQTriageAgent(config)
        context = _make_context()

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "messages": [],
            "report": _make_sample_report(),
            "errors": [],
            "iteration": 1,
            "run_date": "2026-09-30",
            "source_lane": "kafka_dlq",
            "trigger_params": {},
            "correlation_id": "test-corr-dlq-001",
            "max_iterations": 8,
            "classifications": [],
            "requeue_audit": [],
        }
        agent._graph = mock_graph

        await agent.invoke(context)

        call_args = mock_graph.invoke.call_args[0][0]
        assert call_args["max_iterations"] == 8
