"""
Reconciliation Diagnostics agent — the public interface.

WHY THIS FILE MATTERS (learning concepts):

  This is the LAST file in the Reconciliation agent's assembly chain.
  The previous files built the pieces:

    tools   → what the agent CAN DO
    state   → what data FLOWS between nodes
    prompts → what the agent KNOWS
    graph   → how the pieces CONNECT

  This file wraps all of that behind the BaseAgent interface so the
  platform (router, CLI, API) can invoke the agent without knowing
  anything about LangGraph, tools, or prompts. It's the adapter between
  "agent internals" and "platform contract".

KEY CONCEPTS IN THIS FILE:

  1. BaseAgent subclass — implements the abstract contract from base.py:
     name, description, build_graph(), invoke(). The platform only sees
     BaseAgent; it doesn't know this agent uses LangGraph inside.

  2. TriggerContext → initial state translation — the platform speaks in
     TriggerContext (agent_name, trigger_source, run_date, params). The
     graph speaks in ReconState (messages, gate_name, iteration, etc.).
     This file bridges the two.

  3. AgentResult packaging — the graph produces a ReconState dict with a
     ReconReport object. The platform expects an AgentResult with a plain
     dict report. This file extracts and converts.

  4. Error handling boundary — if the graph crashes, the LLM produces
     invalid output, or the report is None, this file catches it and
     returns a failure AgentResult instead of letting the exception
     propagate to the platform.

  5. Lazy graph building — the graph is compiled once on first invoke()
     and cached in self._graph. Building is expensive (creates the LLM
     client, binds tools, compiles the graph). Caching avoids repeating
     that on every invocation.

DESIGN DECISIONS:

  - Why async invoke()? Because the base class defines it as async.
    Currently graph.invoke() is synchronous (LangGraph's default), so
    we call it directly. When we add async tool execution (Phase 6),
    we'll switch to graph.ainvoke() — the async signature is future-proof.

  - Why extract tool names from messages? Because the platform's
    actions_taken field should show what the agent DID, not just that it
    succeeded. Extracting tool call names from the message history gives
    a concrete audit trail: ["query_gate_results", "compare_row_counts",
    "check_duplicate_keys"].
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from langchain_core.messages import AIMessage

from argus.agents.base import AgentResult, BaseAgent, TriggerContext
from argus.agents.reconciliation.graph import build_recon_graph
from argus.agents.reconciliation.state import make_initial_state
from argus.core.config import ArgusConfig
from argus.core.logging import get_logger

logger = get_logger(__name__)


class ReconciliationAgent(BaseAgent):
    """
    Reconciliation Diagnostics agent.

    Investigates pipeline gate failures (Gate 3: Bronze→Silver count mismatch,
    Gate 4: Gold→Snowflake count mismatch) using a ReAct loop with six
    investigation tools. Produces a structured ReconReport with findings,
    root cause, suggested fix, and severity.

    Usage:
        config = load_config("dev")
        agent = ReconciliationAgent(config)
        context = TriggerContext(
            agent_name="reconciliation",
            trigger_source="cli",
            run_date="2026-09-28",
            params={"gate_failure": "gate_3", "dag_id": "ttag_main"},
        )
        result = await agent.invoke(context)
        print(result.report)  # structured diagnosis
    """

    # ── Abstract property implementations ──────────────────────────────

    @property
    def name(self) -> str:
        """Agent identifier used in routing, logging, and AgentResult."""
        return "reconciliation"

    @property
    def description(self) -> str:
        """One-line description for logging and routing decisions."""
        return (
            "Investigates pipeline gate failures — row count mismatches, "
            "duplicate keys, FK integrity, watermark gaps"
        )

    # ── Graph building ─────────────────────────────────────────────────

    def build_graph(self):
        """
        Construct the Reconciliation LangGraph StateGraph.

        Delegates to build_recon_graph() which handles all the wiring:
        entry node, LLM node, tool node, report node, conditional edges.

        Returns:
            A compiled LangGraph StateGraph (CompiledStateGraph).
        """
        return build_recon_graph(self.config)

    # ── Agent invocation ───────────────────────────────────────────────

    async def invoke(self, context: TriggerContext) -> AgentResult:
        """
        Execute the Reconciliation agent for a given trigger context.

        This method is the bridge between the platform and the graph:

        1. TRANSLATE — convert TriggerContext to graph initial state
        2. BUILD — compile the graph (lazy, cached after first call)
        3. RUN — invoke the graph with the initial state
        4. EXTRACT — pull the ReconReport from the final state
        5. PACKAGE — wrap everything in an AgentResult for the platform

        Args:
            context: TriggerContext from the router (CLI, Airflow, API).

        Returns:
            AgentResult with status, report dict, actions taken, and errors.
        """
        started_at = datetime.now(timezone.utc)

        logger.info(
            "ReconciliationAgent.invoke: starting",
            extra={
                "run_date": context.run_date,
                "correlation_id": context.correlation_id,
                "trigger_source": context.trigger_source,
                "params": context.params,
            },
        )

        # ── Step 1: Lazy graph compilation ─────────────────────────
        # Build once, reuse on subsequent invocations. The compiled
        # graph is stateless — state lives in the invoke() call, not
        # in the graph object. Safe to reuse across invocations.
        if self._graph is None:
            logger.info(
                "ReconciliationAgent.invoke: building graph (first call)",
                extra={"correlation_id": context.correlation_id},
            )
            self._graph = self.build_graph()

        # ── Step 2: Translate TriggerContext → initial state ───────
        # The platform speaks TriggerContext; the graph speaks ReconState.
        # make_initial_state() is the translator.
        #
        # gate_name comes from params — the Airflow callback or CLI
        # passes it as "gate_failure". Default to "unknown" if missing,
        # so the graph still runs (the prompt will note it's unknown).
        gate_name = context.params.get("gate_failure", "unknown")
        max_iterations = self.config.get(
            "agents.reconciliation.max_iterations", 10
        )

        initial_state = make_initial_state(
            run_date=context.run_date,
            gate_name=gate_name,
            trigger_params=context.params,
            correlation_id=context.correlation_id,
            max_iterations=max_iterations,
        )

        # ── Step 3: Run the graph ──────────────────────────────────
        # graph.invoke() runs the full ReAct loop synchronously:
        # entry → llm → tools → llm → ... → report → END
        # It returns the final state dict with all accumulated data.
        #
        # We wrap this in try/except because any of these could fail:
        #   - LLM API call (network error, rate limit, bad response)
        #   - Tool execution (unexpected data, parsing error)
        #   - Structured output (LLM produces invalid JSON for ReconReport)
        try:
            final_state = self._graph.invoke(initial_state)
        except Exception as exc:
            logger.error(
                "ReconciliationAgent.invoke: graph execution failed",
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
            # Success — the report node produced a ReconReport.
            # .model_dump() converts the Pydantic model to a plain dict
            # because AgentResult.report is dict[str, Any], not ReconReport.
            # This keeps the platform layer Pydantic-model-agnostic.
            logger.info(
                "ReconciliationAgent.invoke: completed successfully",
                extra={
                    "severity": report.recommended_severity.value,
                    "finding_count": len(report.findings),
                    "iteration_count": final_state.get("iteration", 0),
                    "tool_calls": actions,
                    "correlation_id": context.correlation_id,
                },
            )
            return self._make_result(
                context=context,
                status="success",
                report=report.model_dump(),
                started_at=started_at,
                actions=actions,
                errors=errors,
            )
        else:
            # The report node failed or was skipped (shouldn't happen in
            # normal flow, but defensive coding). Return failure with
            # whatever errors accumulated during execution.
            logger.warning(
                "ReconciliationAgent.invoke: completed but no report produced",
                extra={
                    "errors": errors,
                    "iteration_count": final_state.get("iteration", 0),
                    "correlation_id": context.correlation_id,
                },
            )
            return self._make_result(
                context=context,
                status="failure",
                report={"error": "Report generation failed", "errors": errors},
                started_at=started_at,
                actions=actions,
                errors=errors or ["No report produced"],
            )


# ---------------------------------------------------------------------------
# Helper — extract tool call names from the message history
# ---------------------------------------------------------------------------

def _extract_tool_calls(messages: list) -> list[str]:
    """
    Walk the message history and collect tool names the LLM called.

    This gives the platform a concrete audit trail: which tools did the
    agent actually use during this investigation?

    Returns:
        List of tool names in call order, e.g.:
        ["query_gate_results", "compare_row_counts", "check_duplicate_keys"]
    """
    tool_names = []
    for msg in messages:
        if isinstance(msg, AIMessage) and hasattr(msg, "tool_calls"):
            for tc in msg.tool_calls:
                tool_names.append(tc["name"])
    return tool_names
