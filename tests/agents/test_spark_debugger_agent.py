"""
Unit tests for the AI Spark Debugger agent.

WHY THIS FILE MATTERS (learning concepts):

  These tests validate the Spark Debugger's ReAct + Reflection pattern
  WITHOUT calling a real LLM. Every test mocks the LLM response to control
  the exact path through the graph.

  This approach tests the WIRING, not the LLM's intelligence:
    - Do nodes pass state correctly?
    - Do routers make the right decision?
    - Does reflection loop back when needed?
    - Does the safety valve (max iterations/reflections) fire?
    - Does error handling prevent crashes?

TEST CLASSES (7):

  1. TestSparkTools — tool functions return expected simulated data
  2. TestState — make_initial_state produces valid seed state
  3. TestEntryNode — prompt template renders correctly
  4. TestShouldContinue — inner loop router logic
  5. TestShouldRevise — outer loop (reflection) router logic
  6. TestGraphIntegration — full graph runs with mocked LLM
  7. TestAgentInvoke — BaseAgent.invoke() packaging

MOCKING STRATEGY:

  We mock create_llm() to return a FakeLLM that returns predetermined
  responses. The FakeLLM cycles through a sequence of responses, allowing
  us to script the exact path:

    response 1: AIMessage with tool_calls → routes to tools
    response 2: AIMessage with tool_calls → routes to tools
    response 3: AIMessage text (hypothesis) → routes to reflect
    response 4: AIMessage "HYPOTHESIS_CONFIRMED" → routes to report
    response 5: structured SparkDiagnosis → report node output

  This gives us deterministic control over a non-deterministic system.
"""

import json
import operator
from datetime import datetime, timezone
from typing import Annotated, Any
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from pydantic import BaseModel

# --- Imports under test ---
from argus.agents.spark_debugger.state import (
    SparkDebuggerState,
    make_initial_state,
)
from argus.agents.spark_debugger.prompts import (
    SPARK_DEBUGGER_SYSTEM_PROMPT,
    SPARK_HUMAN_PROMPT,
    SPARK_REFLECTION_PROMPT,
    SPARK_PROMPT_TEMPLATE,
)
from argus.agents.spark_debugger.graph import (
    _make_entry_node,
    _make_llm_node,
    _make_reflect_node,
    _make_report_node,
    _should_continue,
    _should_revise,
    build_spark_debugger_graph,
)
from argus.agents.spark_debugger.agent import (
    SparkDebuggerAgent,
    _extract_tool_calls,
)
from argus.agents.base import TriggerContext
from argus.schemas.reports import SparkDiagnosis, SparkBottleneck, Severity
from argus.tools.compute.spark_tools import (
    SPARK_TOOLS,
    get_application_info,
    get_stage_metrics,
    get_task_distribution,
    get_executor_metrics,
    parse_physical_plan,
    read_event_log,
)


# ==========================================================================
# Test helpers
# ==========================================================================

def _make_test_state(**overrides) -> dict:
    """Build a minimal state dict for testing, with overrides."""
    base = make_initial_state(
        app_id="app-20260928-001",
        trigger_params={"threshold_minutes": 30},
        correlation_id="test-123",
    )
    base.update(overrides)
    return base


def _make_spark_diagnosis() -> SparkDiagnosis:
    """Build a valid SparkDiagnosis for testing."""
    return SparkDiagnosis(
        app_id="app-20260928-001",
        app_name="ttag_silver_booking_merge",
        total_duration_seconds=2820,
        bottlenecks=[
            SparkBottleneck(
                category="skew",
                stage_id=2,
                evidence="skew_ratio=329.4, task 142 processes 890MB vs median 6.8MB",
                impact="high",
                recommendation="Salt the booking_id join key to distribute hot key BK-PREMIUM-001",
            ),
        ],
        root_cause_summary=(
            "Data skew on booking_id='BK-PREMIUM-001' causes 398K records "
            "to hash to partition 142 (skew_ratio=329x), leading to spill "
            "and GC pressure on the SortMergeJoin in Stage 2."
        ),
        recommendations=[
            "Salt booking_id with 10 buckets to spread the hot key",
            "Enable AQE skew join handling: spark.sql.adaptive.skewJoin.enabled=true",
            "Increase executor memory to 4g for this job",
        ],
        recommended_severity=Severity.P2,
    )


class FakeLLM:
    """
    A mock LLM that returns predetermined responses in sequence.

    Each call to invoke() returns the next response from the list.
    If the list is exhausted, it returns a generic text response.

    This is the core testing trick: by controlling exactly what the
    LLM returns, we can test every path through the graph.
    """

    def __init__(self, responses: list):
        self.responses = list(responses)
        self.call_count = 0
        self.call_history = []

    def invoke(self, messages, **kwargs):
        self.call_history.append(messages)
        if self.call_count < len(self.responses):
            response = self.responses[self.call_count]
            self.call_count += 1
            return response
        # Fallback: generic text (no tool calls → will route to reflect)
        self.call_count += 1
        return AIMessage(content="Investigation complete.")

    def bind_tools(self, tools):
        """Return self — tools are irrelevant for fake responses."""
        return self

    def with_structured_output(self, schema):
        """Return self — structured output comes from the response list."""
        return self


# ==========================================================================
# Test 1: Spark tools (simulated data)
# ==========================================================================

class TestSparkTools:
    """Verify that the 6 Spark tools return expected simulated data."""

    def test_tool_registry_has_six_tools(self):
        """SPARK_TOOLS should register exactly 6 tools."""
        assert len(SPARK_TOOLS) == 6
        tool_names = {t.name for t in SPARK_TOOLS}
        expected = {
            "get_application_info",
            "get_stage_metrics",
            "get_task_distribution",
            "get_executor_metrics",
            "parse_physical_plan",
            "read_event_log",
        }
        assert tool_names == expected

    def test_get_application_info_known_app(self):
        """get_application_info returns data for a known simulated app."""
        result = get_application_info.invoke({"app_id": "app-20260928-001"})
        data = json.loads(result)
        assert data["app_id"] == "app-20260928-001"
        assert data["app_name"] == "ttag_silver_booking_merge"
        assert "duration_seconds" in data

    def test_get_application_info_unknown_app(self):
        """get_application_info returns an error for unknown apps."""
        result = get_application_info.invoke({"app_id": "app-unknown"})
        data = json.loads(result)
        assert "error" in data

    def test_get_stage_metrics_all_stages(self):
        """get_stage_metrics(stage_id=None) returns all stages."""
        result = get_stage_metrics.invoke({
            "app_id": "app-20260928-001",
        })
        data = json.loads(result)
        assert "stages" in data
        assert len(data["stages"]) > 0

    def test_get_task_distribution_skew_scenario(self):
        """get_task_distribution for the skew app shows high skew_ratio."""
        result = get_task_distribution.invoke({
            "app_id": "app-20260928-001",
            "stage_id": 2,
        })
        data = json.loads(result)
        assert data["skew_ratio"] > 100  # should be ~329

    def test_get_executor_metrics(self):
        """get_executor_metrics returns per-executor health data."""
        result = get_executor_metrics.invoke({
            "app_id": "app-20260928-001",
        })
        data = json.loads(result)
        assert "executors" in data
        assert len(data["executors"]) > 0

    def test_parse_physical_plan(self):
        """parse_physical_plan returns join strategies and partitions."""
        result = parse_physical_plan.invoke({
            "app_id": "app-20260928-001",
        })
        data = json.loads(result)
        assert "plan_text" in data
        assert "analysis" in data

    def test_read_event_log(self):
        """read_event_log returns Spark event data."""
        result = read_event_log.invoke({
            "app_id": "app-20260928-001",
        })
        data = json.loads(result)
        assert "events" in data

    def test_gc_pressure_scenario(self):
        """The GC pressure scenario (app-20260927-001) shows high gc_pct."""
        result = get_executor_metrics.invoke({
            "app_id": "app-20260927-001",
        })
        data = json.loads(result)
        # All executors should have high GC percentage
        for executor in data["executors"]:
            assert executor["gc_pct"] > 20


# ==========================================================================
# Test 2: State
# ==========================================================================

class TestState:
    """Verify SparkDebuggerState and make_initial_state."""

    def test_initial_state_defaults(self):
        """make_initial_state produces a valid seed state with defaults."""
        state = make_initial_state(
            app_id="app-test",
            trigger_params={"key": "value"},
            correlation_id="corr-1",
        )
        assert state["app_id"] == "app-test"
        assert state["trigger_params"] == {"key": "value"}
        assert state["correlation_id"] == "corr-1"
        assert state["iteration"] == 0
        assert state["max_iterations"] == 15
        assert state["hypothesis"] == ""
        assert state["reflection_count"] == 0
        assert state["max_reflections"] == 2
        assert state["report"] is None
        assert state["errors"] == []
        assert state["messages"] == []

    def test_initial_state_custom_limits(self):
        """make_initial_state accepts custom iteration limits."""
        state = make_initial_state(
            app_id="app-test",
            trigger_params={},
            correlation_id="corr-1",
            max_iterations=20,
            max_reflections=3,
        )
        assert state["max_iterations"] == 20
        assert state["max_reflections"] == 3

    def test_state_annotations_have_reducers(self):
        """messages and errors should have operator.add reducers."""
        annotations = SparkDebuggerState.__annotations__
        # Check that the Annotated types are present
        assert "messages" in annotations
        assert "errors" in annotations


# ==========================================================================
# Test 3: Entry node
# ==========================================================================

class TestEntryNode:
    """Verify the entry node seeds the conversation correctly."""

    def test_entry_node_produces_messages(self):
        """Entry node should produce system + human messages."""
        entry = _make_entry_node()
        state = _make_test_state()
        result = entry(state)

        messages = result["messages"]
        assert len(messages) == 2

        # First message is SystemMessage (Spark expertise prompt)
        assert isinstance(messages[0], SystemMessage)
        assert "Spark performance engineer" in messages[0].content

        # Second message is HumanMessage (investigation request)
        assert isinstance(messages[1], HumanMessage)
        assert "app-20260928-001" in messages[1].content

    def test_entry_node_injects_trigger_params(self):
        """Entry node should render trigger_params as JSON in the message."""
        entry = _make_entry_node()
        state = _make_test_state(
            trigger_params={"threshold_minutes": 45, "alert_source": "pagerduty"}
        )
        result = entry(state)

        human_msg = result["messages"][1]
        assert "threshold_minutes" in human_msg.content
        assert "45" in human_msg.content


# ==========================================================================
# Test 4: should_continue (inner loop router)
# ==========================================================================

class TestShouldContinue:
    """Verify the inner loop router logic."""

    def test_routes_to_tools_when_tool_calls_present(self):
        """When the LLM emits tool_calls, route to tools node."""
        state = _make_test_state(
            messages=[
                AIMessage(
                    content="",
                    tool_calls=[{"name": "get_application_info", "args": {"app_id": "app-1"}, "id": "tc1"}],
                ),
            ],
            iteration=1,
            max_iterations=15,
        )
        assert _should_continue(state) == "tools"

    def test_routes_to_reflect_when_no_tool_calls(self):
        """When the LLM emits text only, route to reflect node."""
        state = _make_test_state(
            messages=[AIMessage(content="My hypothesis: skew is the root cause.")],
            iteration=3,
            max_iterations=15,
        )
        assert _should_continue(state) == "reflect"

    def test_routes_to_reflect_on_max_iterations(self):
        """When max iterations reached, force reflection (safety valve)."""
        state = _make_test_state(
            messages=[
                AIMessage(
                    content="",
                    tool_calls=[{"name": "get_stage_metrics", "args": {}, "id": "tc2"}],
                ),
            ],
            iteration=15,
            max_iterations=15,
        )
        # Even though tool_calls are present, max iterations should
        # force a reflect to prevent runaway investigation
        assert _should_continue(state) == "reflect"


# ==========================================================================
# Test 5: should_revise (outer loop router)
# ==========================================================================

class TestShouldRevise:
    """Verify the outer loop (reflection) router logic."""

    def test_routes_to_llm_when_needs_more_investigation(self):
        """When reflection says NEEDS_MORE_INVESTIGATION, route back to LLM."""
        state = _make_test_state(
            messages=[
                AIMessage(
                    content="NEEDS_MORE_INVESTIGATION: haven't checked executor metrics yet"
                ),
            ],
            reflection_count=1,
            max_reflections=2,
        )
        assert _should_revise(state) == "llm"

    def test_routes_to_report_when_hypothesis_confirmed(self):
        """When reflection says HYPOTHESIS_CONFIRMED, route to report."""
        state = _make_test_state(
            messages=[
                AIMessage(
                    content="HYPOTHESIS_CONFIRMED: skew on booking_id causes cascading failures"
                ),
            ],
            reflection_count=1,
            max_reflections=2,
        )
        assert _should_revise(state) == "report"

    def test_routes_to_report_on_max_reflections(self):
        """When max reflections reached, force report (safety valve)."""
        state = _make_test_state(
            messages=[
                AIMessage(
                    content="NEEDS_MORE_INVESTIGATION: still unclear"
                ),
            ],
            reflection_count=2,
            max_reflections=2,
        )
        # Even though the reflection says more investigation is needed,
        # max reflections forces a report
        assert _should_revise(state) == "report"

    def test_defaults_to_report_on_unrecognized_format(self):
        """If the reflection format is unrecognized, default to report."""
        state = _make_test_state(
            messages=[
                AIMessage(content="I think the hypothesis is reasonable."),
            ],
            reflection_count=1,
            max_reflections=2,
        )
        assert _should_revise(state) == "report"

    def test_case_insensitive_matching(self):
        """NEEDS_MORE_INVESTIGATION matching should be case-insensitive."""
        state = _make_test_state(
            messages=[
                AIMessage(
                    content="needs_more_investigation: check GC metrics"
                ),
            ],
            reflection_count=0,
            max_reflections=2,
        )
        assert _should_revise(state) == "llm"


# ==========================================================================
# Test 6: Graph integration (mocked LLM)
# ==========================================================================

class TestGraphIntegration:
    """
    Full graph run with mocked LLM.

    These tests verify the WIRING — that state flows correctly through
    entry → llm → tools → llm → reflect → report → END.
    """

    @patch("argus.agents.spark_debugger.graph.create_llm")
    def test_full_graph_produces_diagnosis(self, mock_create_llm):
        """
        A full graph run should produce a SparkDiagnosis.

        Script:
          1. LLM calls get_application_info (tool_call)
          2. LLM states hypothesis (text → reflect)
          3. Reflection confirms (HYPOTHESIS_CONFIRMED → report)
          4. Report model produces SparkDiagnosis
        """
        diagnosis = _make_spark_diagnosis()

        fake_llm = FakeLLM([
            # Turn 1: LLM calls a tool
            AIMessage(
                content="",
                tool_calls=[{
                    "name": "get_application_info",
                    "args": {"app_id": "app-20260928-001"},
                    "id": "call_1",
                }],
            ),
            # Turn 2: LLM states hypothesis (no tool calls → reflect)
            AIMessage(content="Hypothesis: data skew on booking_id."),
            # Turn 3: Reflection confirms
            AIMessage(content="HYPOTHESIS_CONFIRMED: skew confirmed."),
            # Turn 4: Report model produces SparkDiagnosis
            diagnosis,
        ])

        mock_create_llm.return_value = fake_llm

        config = MagicMock()
        config.llm = {"provider": "google", "model": "gemini-2.0-flash"}

        graph = build_spark_debugger_graph(config)

        initial_state = make_initial_state(
            app_id="app-20260928-001",
            trigger_params={"threshold_minutes": 30},
            correlation_id="test-integration",
        )

        result = graph.invoke(initial_state)

        assert result["report"] is not None
        assert result["report"].app_id == "app-20260928-001"
        assert len(result["report"].bottlenecks) == 1
        assert result["report"].bottlenecks[0].category == "skew"
        assert result["iteration"] > 0
        assert result["reflection_count"] > 0

    @patch("argus.agents.spark_debugger.graph.create_llm")
    def test_graph_with_reflection_loop(self, mock_create_llm):
        """
        Test the reflection re-investigation loop.

        Script:
          1. LLM states hypothesis (no tools → reflect)
          2. Reflection says NEEDS_MORE_INVESTIGATION → back to llm
          3. LLM calls a tool
          4. LLM states refined hypothesis → reflect again
          5. Reflection confirms → report
          6. Report produces SparkDiagnosis
        """
        diagnosis = _make_spark_diagnosis()

        fake_llm = FakeLLM([
            # Turn 1: LLM states early hypothesis (→ reflect)
            AIMessage(content="Hypothesis: probably GC pressure."),
            # Turn 2: Reflection says needs more (→ back to llm)
            AIMessage(content="NEEDS_MORE_INVESTIGATION: check for skew first"),
            # Turn 3: LLM calls a tool (→ tools)
            AIMessage(
                content="",
                tool_calls=[{
                    "name": "get_task_distribution",
                    "args": {"app_id": "app-20260928-001", "stage_id": 2},
                    "id": "call_2",
                }],
            ),
            # Turn 4: LLM states refined hypothesis (→ reflect)
            AIMessage(content="Revised hypothesis: skew causes GC."),
            # Turn 5: Reflection confirms (→ report)
            AIMessage(content="HYPOTHESIS_CONFIRMED: skew is root cause."),
            # Turn 6: Report model
            diagnosis,
        ])

        mock_create_llm.return_value = fake_llm

        config = MagicMock()
        config.llm = {"provider": "google", "model": "gemini-2.0-flash"}

        graph = build_spark_debugger_graph(config)

        initial_state = make_initial_state(
            app_id="app-20260928-001",
            trigger_params={},
            correlation_id="test-reflect",
        )

        result = graph.invoke(initial_state)

        assert result["report"] is not None
        # Should have gone through 2 reflections
        assert result["reflection_count"] == 2
        assert result["hypothesis"] != ""

    @patch("argus.agents.spark_debugger.graph.create_llm")
    def test_max_iterations_forces_reflect(self, mock_create_llm):
        """When max_iterations is reached, the graph should force reflection."""
        diagnosis = _make_spark_diagnosis()

        # Create a sequence of tool calls that will hit the limit
        tool_call_responses = [
            AIMessage(
                content="",
                tool_calls=[{
                    "name": "get_application_info",
                    "args": {"app_id": "app-20260928-001"},
                    "id": f"call_{i}",
                }],
            )
            for i in range(5)
        ]

        fake_llm = FakeLLM(
            tool_call_responses + [
                # After forced reflect, will go to reflect
                AIMessage(content="Forced hypothesis after iteration limit."),
                # Reflection confirms (or would be forced after max_reflections)
                AIMessage(content="HYPOTHESIS_CONFIRMED: timeout."),
                # Report
                diagnosis,
            ]
        )

        mock_create_llm.return_value = fake_llm

        config = MagicMock()
        config.llm = {"provider": "google", "model": "gemini-2.0-flash"}

        graph = build_spark_debugger_graph(config)

        # Use a LOW max_iterations to trigger the safety valve
        initial_state = make_initial_state(
            app_id="app-20260928-001",
            trigger_params={},
            correlation_id="test-max-iter",
            max_iterations=3,
        )

        result = graph.invoke(initial_state)
        # Should have stopped and produced a report despite wanting more tools
        assert result["report"] is not None


# ==========================================================================
# Test 7: Agent invoke (BaseAgent wrapper)
# ==========================================================================

class TestAgentInvoke:
    """Verify SparkDebuggerAgent.invoke() packaging."""

    def test_agent_properties(self):
        """Agent name and description should be set."""
        config = MagicMock()
        agent = SparkDebuggerAgent(config)
        assert agent.name == "spark_debugger"
        assert "Spark" in agent.description
        assert "skew" in agent.description

    @patch("argus.agents.spark_debugger.graph.create_llm")
    @pytest.mark.asyncio
    async def test_invoke_success(self, mock_create_llm):
        """invoke() should return success AgentResult with diagnosis."""
        diagnosis = _make_spark_diagnosis()

        fake_llm = FakeLLM([
            AIMessage(content="Hypothesis: skew."),
            AIMessage(content="HYPOTHESIS_CONFIRMED: yes."),
            diagnosis,
        ])
        mock_create_llm.return_value = fake_llm

        config = MagicMock()
        config.llm = {"provider": "google", "model": "gemini-2.0-flash"}
        config.get = MagicMock(return_value=15)

        agent = SparkDebuggerAgent(config)

        context = TriggerContext(
            agent_name="spark_debugger",
            trigger_source="cli",
            run_date="2026-09-28",
            params={"app_id": "app-20260928-001", "threshold_minutes": 30},
        )

        result = await agent.invoke(context)

        assert result.status == "success"
        assert result.agent_name == "spark_debugger"
        assert "app_id" in result.report
        assert result.report["app_id"] == "app-20260928-001"
        assert "_meta" in result.report
        assert "reflections" in result.report["_meta"]

    @patch("argus.agents.spark_debugger.graph.create_llm")
    @pytest.mark.asyncio
    async def test_invoke_graph_failure(self, mock_create_llm):
        """invoke() should return failure AgentResult on graph crash."""
        mock_create_llm.side_effect = RuntimeError("LLM init failed")

        config = MagicMock()
        config.llm = {"provider": "google", "model": "gemini-2.0-flash"}
        config.get = MagicMock(return_value=15)

        agent = SparkDebuggerAgent(config)

        context = TriggerContext(
            agent_name="spark_debugger",
            trigger_source="cli",
            run_date="2026-09-28",
            params={"app_id": "app-20260928-001"},
        )

        result = await agent.invoke(context)

        assert result.status == "failure"
        assert len(result.errors) > 0

    def test_extract_tool_calls(self):
        """_extract_tool_calls should collect tool names from AIMessages."""
        messages = [
            HumanMessage(content="Start"),
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "get_application_info", "args": {}, "id": "1"},
                    {"name": "get_stage_metrics", "args": {}, "id": "2"},
                ],
            ),
            ToolMessage(content="{}", tool_call_id="1"),
            ToolMessage(content="{}", tool_call_id="2"),
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "get_task_distribution", "args": {}, "id": "3"},
                ],
            ),
            ToolMessage(content="{}", tool_call_id="3"),
            AIMessage(content="Hypothesis: skew."),
        ]

        result = _extract_tool_calls(messages)
        assert result == [
            "get_application_info",
            "get_stage_metrics",
            "get_task_distribution",
        ]


# ==========================================================================
# Prompt tests (bonus — verify template rendering)
# ==========================================================================

class TestPrompts:
    """Verify prompt templates render correctly."""

    def test_system_prompt_has_bottleneck_categories(self):
        """System prompt should mention all 5 bottleneck categories."""
        for category in ["skew", "spill", "small_files", "broadcast", "gc_pressure"]:
            assert category in SPARK_DEBUGGER_SYSTEM_PROMPT

    def test_reflection_prompt_has_placeholder(self):
        """Reflection prompt should have a {hypothesis} placeholder."""
        assert "{hypothesis}" in SPARK_REFLECTION_PROMPT

    def test_reflection_prompt_renders(self):
        """Reflection prompt should render without errors."""
        rendered = SPARK_REFLECTION_PROMPT.format(
            hypothesis="Data skew on booking_id is the root cause."
        )
        assert "Data skew on booking_id" in rendered
        assert "NEEDS_MORE_INVESTIGATION" in rendered
        assert "HYPOTHESIS_CONFIRMED" in rendered

    def test_prompt_template_renders(self):
        """SPARK_PROMPT_TEMPLATE should produce system + human messages."""
        result = SPARK_PROMPT_TEMPLATE.invoke({
            "app_id": "app-test-123",
            "trigger_params": '{"key": "value"}',
        })
        messages = result.to_messages()
        assert len(messages) == 2
        assert isinstance(messages[0], SystemMessage)
        assert isinstance(messages[1], HumanMessage)
        assert "app-test-123" in messages[1].content
