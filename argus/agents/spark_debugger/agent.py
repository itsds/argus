"""
Spark Debugger agent — the public interface.

WHY THIS FILE MATTERS (learning concepts):

  This is the adapter between the Spark Debugger's internals (LangGraph,
  tools, prompts, ReAct + Reflection loop) and the Argus platform (router,
  CLI, API, Airflow callbacks).

  Same pattern as ReconciliationAgent, but with two differences:

    1. TriggerContext translation extracts app_id instead of gate_name
    2. Result extraction handles SparkDiagnosis instead of ReconReport

  The structural similarity is intentional — all four Argus agents follow
  the same BaseAgent contract, making them interchangeable from the
  platform's perspective.

KEY CONCEPTS IN THIS FILE:

  1. BaseAgent subclass — same abstract contract as ReconciliationAgent.
     The platform routes triggers to agents by name; it doesn't know
     which pattern (ReAct, ReAct+Reflection, Plan-then-Execute) each
     agent uses internally.

  2. TriggerContext → initial state — the Spark Debugger needs an app_id
     (not a gate_name or run_date). The trigger context passes it in
     params, and this file extracts it into make_initial_state().

  3. config-driven iteration limits — max_iterations and max_reflections
     come from config, allowing different environments (dev/prod) to
     set different caps without code changes.

  4. Richer result metadata — besides tool_calls, the Spark Debugger's
     result includes reflection_count and hypothesis from the final state,
     giving the audit trail visibility into how many hypothesis refinements
     the agent went through.

COMPARISON WITH OTHER AGENTS:

  ReconciliationAgent:
    - TriggerContext key: gate_failure → gate_name
    - Report type: ReconReport
    - Pattern: ReAct

  SparkDebuggerAgent:
    - TriggerContext key: app_id → app_id
    - Report type: SparkDiagnosis
    - Pattern: ReAct + Reflection
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from langchain_core.messages import AIMessage

from argus.agents.base import AgentResult, BaseAgent, TriggerContext
from argus.agents.spark_debugger.graph import build_spark_debugger_graph
from argus.agents.spark_debugger.state import make_initial_state
from argus.core.config import ArgusConfig
from argus.core.logging import get_logger

logger = get_logger(__name__)


class SparkDebuggerAgent(BaseAgent):
    """
    AI Spark Debugger agent.

    Investigates slow or failing Spark jobs in the TTAG pipeline using
    a ReAct + Reflection pattern. Analyzes SparkUI metrics, execution
    plans, task distributions, and event logs to find root cause
    bottlenecks (skew, spill, small files, broadcast misses, GC pressure).
    Produces a structured SparkDiagnosis with causal chain analysis.

    Usage:
        config = load_config("dev")
        agent = SparkDebuggerAgent(config)
        context = TriggerContext(
            agent_name="spark_debugger",
            trigger_source="cli",
            run_date="2026-09-28",
            params={"app_id": "app-20260928-001", "threshold_minutes": 30},
        )
        result = await agent.invoke(context)
        print(result.report)  # structured SparkDiagnosis
    """

    # ── Abstract property implementations ──────────────────────────────

    @property
    def name(self) -> str:
        """Agent identifier used in routing, logging, and AgentResult."""
        return "spark_debugger"

    @property
    def description(self) -> str:
        """One-line description for logging and routing decisions."""
        return (
            "Investigates slow Spark jobs — data skew, spill, "
            "GC pressure, small files, broadcast threshold misses"
        )

    # ── Graph building ─────────────────────────────────────────────────

    def build_graph(self):
        """
        Construct the Spark Debugger LangGraph StateGraph.

        Delegates to build_spark_debugger_graph() which wires the full
        ReAct + Reflection topology: entry, llm, tools, reflect, report
        nodes with two conditional edges.

        Returns:
            A compiled LangGraph StateGraph (CompiledStateGraph).
        """
        return build_spark_debugger_graph(self.config)

    # ── Agent invocation ───────────────────────────────────────────────

    async def invoke(self, context: TriggerContext) -> AgentResult:
        """
        Execute the Spark Debugger for a given trigger context.

        Flow:
          1. TRANSLATE — TriggerContext → SparkDebuggerState initial dict
          2. BUILD — compile graph (lazy, cached)
          3. RUN — invoke the ReAct + Reflection graph
          4. EXTRACT — pull SparkDiagnosis from final state
          5. PACKAGE — wrap in AgentResult

        Args:
            context: TriggerContext from router (CLI, Airflow, API).
                     Must include params.app_id — the Spark application
                     to debug.

        Returns:
            AgentResult with status, report dict, actions taken, and errors.
        """
        started_at = datetime.now(timezone.utc)

        logger.info(
            "SparkDebuggerAgent.invoke: starting",
            extra={
                "run_date": context.run_date,
                "correlation_id": context.correlation_id,
                "trigger_source": context.trigger_source,
                "params": context.params,
            },
        )

        # ── Step 1: Lazy graph compilation ─────────────────────────
        if self._graph is None:
            logger.info(
                "SparkDebuggerAgent.invoke: building graph (first call)",
                extra={"correlation_id": context.correlation_id},
            )
            self._graph = self.build_graph()

        # ── Step 2: Translate TriggerContext → initial state ───────
        # The Spark Debugger needs app_id from the trigger params.
        # This comes from the Airflow callback or CLI invocation.
        app_id = context.params.get("app_id", "unknown")

        # Read iteration limits from config, with sensible defaults.
        # max_iterations controls the inner ReAct loop (tool calls).
        # max_reflections controls the outer reflection loop.
        max_iterations = self.config.get(
            "agents.spark_debugger.max_iterations", 15
        )
        max_reflections = self.config.get(
            "agents.spark_debugger.max_reflections", 2
        )

        initial_state = make_initial_state(
            app_id=app_id,
            trigger_params=context.params,
            correlation_id=context.correlation_id,
            max_iterations=max_iterations,
            max_reflections=max_reflections,
        )

        # ── Step 3: Run the graph ──────────────────────────────────
        try:
            final_state = self._graph.invoke(initial_state)
        except Exception as exc:
            logger.error(
                "SparkDebuggerAgent.invoke: graph execution failed",
                extra={
                    "error": str(exc),
                    "correlation_id": context.correlation_id,
                },
            )
            return self._make_result(
                context=context,
                status="failure",
                report={"error": str(exc)},
                started_at=started_at,
                errors=[f"Graph execution failed: {exc}"],
            )

        # ── Step 4: Extract results from final state ───────────────
        report = final_state.get("report")
        errors = final_state.get("errors", [])
        actions = _extract_tool_calls(final_state.get("messages", []))

        # ── Step 5: Package into AgentResult ───────────────────────
        if report is not None:
            # Include Spark Debugger-specific metadata in the report
            report_dict = report.model_dump()
            report_dict["_meta"] = {
                "iterations": final_state.get("iteration", 0),
                "reflections": final_state.get("reflection_count", 0),
                "hypothesis": final_state.get("hypothesis", ""),
            }

            logger.info(
                "SparkDebuggerAgent.invoke: completed successfully",
                extra={
                    "app_id": report.app_id,
                    "severity": report.recommended_severity.value,
                    "bottleneck_count": len(report.bottlenecks),
                    "iterations": final_state.get("iteration", 0),
                    "reflections": final_state.get("reflection_count", 0),
                    "tool_calls": actions,
                    "correlation_id": context.correlation_id,
                },
            )
            return self._make_result(
                context=context,
                status="success",
                report=report_dict,
                started_at=started_at,
                actions=actions,
                errors=errors,
            )
        else:
            logger.warning(
                "SparkDebuggerAgent.invoke: completed but no report produced",
                extra={
                    "errors": errors,
                    "iterations": final_state.get("iteration", 0),
                    "reflections": final_state.get("reflection_count", 0),
                    "correlation_id": context.correlation_id,
                },
            )
            return self._make_result(
                context=context,
                status="failure",
                report={"error": "Diagnosis generation failed", "errors": errors},
                started_at=started_at,
                actions=actions,
                errors=errors or ["No diagnosis produced"],
            )


# ---------------------------------------------------------------------------
# Helper — extract tool call names from the message history
# ---------------------------------------------------------------------------

def _extract_tool_calls(messages: list) -> list[str]:
    """
    Walk the message history and collect tool names the LLM called.

    Same helper as Recon — could be extracted to a shared utility,
    but keeping it local avoids coupling between agents. Each agent
    is a self-contained package.

    Returns:
        List of tool names in call order, e.g.:
        ["get_application_info", "get_stage_metrics", "get_task_distribution"]
    """
    tool_names = []
    for msg in messages:
        if isinstance(msg, AIMessage) and hasattr(msg, "tool_calls"):
            for tc in msg.tool_calls:
                tool_names.append(tc["name"])
    return tool_names
