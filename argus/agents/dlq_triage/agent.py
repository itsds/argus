"""
DLQ Triage & Auto-Remediation agent — the public interface.

WHY THIS FILE MATTERS (learning concepts):

  This is the ADAPTER between the DLQ agent's internals (LangGraph,
  tools, prompts) and the Argus platform contract (BaseAgent interface).

  Comparing with ReconciliationAgent (Phase 2), the structure is nearly
  identical. This is by design — the BaseAgent interface provides a
  consistent contract that the platform (router, CLI, API) can depend on
  regardless of what the agent does internally.

WHAT'S DIFFERENT FROM THE RECON AGENT:

  1. DIFFERENT STATE TRANSLATION — TriggerContext is translated to
     DLQTriageState instead of ReconState. The key field is `source_lane`
     (which DLQ lane to check) instead of `gate_name` (which gate failed).

  2. DIFFERENT CONFIG PATH — reads `agents.dlq_triage.max_iterations`
     instead of `agents.reconciliation.max_iterations`.

  3. DIFFERENT REPORT FIELDS — the DLQ report has per-record
     classifications, requeue counts, and quarantine counts. The logging
     reflects these DLQ-specific metrics.

  4. SAME ERROR HANDLING — the try/except around graph.invoke() is
     identical. Both agents need the same error boundary because the
     failure modes are the same (LLM API errors, tool execution failures,
     structured output validation errors).

THE PATTERN:

  Every Argus agent follows this structure:
    1. Lazy graph build (compile once, cache)
    2. Translate TriggerContext → agent-specific initial state
    3. graph.invoke(initial_state) → final_state
    4. Extract report from final_state
    5. Package into AgentResult

  If you were building a third agent, you'd follow this same pattern.
  By Phase 6, we may extract a shared base that handles the common parts.
"""

from __future__ import annotations

from datetime import datetime, timezone

from langchain_core.messages import AIMessage

from argus.agents.base import AgentResult, BaseAgent, TriggerContext
from argus.agents.dlq_triage.graph import build_dlq_graph
from argus.agents.dlq_triage.state import make_initial_state
from argus.core.config import ArgusConfig
from argus.core.logging import get_logger

logger = get_logger(__name__)


class DLQTriageAgent(BaseAgent):
    """
    DLQ Triage & Auto-Remediation agent.

    Classifies dead-letter-queue records from the TTAG pipeline into four
    categories (transient, schema_mismatch, data_quality, unknown), auto-
    requeues transient failures, and produces a structured DLQTriageReport
    with per-record classifications and recommended actions.

    Usage:
        config = load_config("dev")
        agent = DLQTriageAgent(config)
        context = TriggerContext(
            agent_name="dlq_triage",
            trigger_source="cli",
            run_date="2026-09-30",
            params={
                "dlq_threshold_breached": True,
                "source_lane": "kafka_dlq",
            },
        )
        result = await agent.invoke(context)
        print(result.report)  # structured triage report
    """

    # ── Abstract property implementations ──────────────────────────────

    @property
    def name(self) -> str:
        """Agent identifier used in routing, logging, and AgentResult."""
        return "dlq_triage"

    @property
    def description(self) -> str:
        """One-line description for logging and routing decisions."""
        return (
            "Classifies DLQ records (transient/schema/data quality/unknown), "
            "auto-requeues transient failures, escalates unknowns"
        )

    # ── Graph building ─────────────────────────────────────────────────

    def build_graph(self):
        """
        Construct the DLQ Triage LangGraph StateGraph.

        Delegates to build_dlq_graph() which handles all the wiring.

        Returns:
            A compiled LangGraph StateGraph (CompiledStateGraph).
        """
        return build_dlq_graph(self.config)

    # ── Agent invocation ───────────────────────────────────────────────

    async def invoke(self, context: TriggerContext) -> AgentResult:
        """
        Execute the DLQ Triage agent for a given trigger context.

        Same five-step pattern as ReconciliationAgent:
        1. TRANSLATE — TriggerContext → DLQTriageState initial dict
        2. BUILD — compile graph (lazy, cached)
        3. RUN — graph.invoke(initial_state)
        4. EXTRACT — pull DLQTriageReport from final state
        5. PACKAGE — wrap in AgentResult

        Args:
            context: TriggerContext from the router (CLI, Airflow, API).

        Returns:
            AgentResult with status, report dict, actions taken, and errors.
        """
        started_at = datetime.now(timezone.utc)

        logger.info(
            "DLQTriageAgent.invoke: starting",
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
                "DLQTriageAgent.invoke: building graph (first call)",
                extra={"correlation_id": context.correlation_id},
            )
            self._graph = self.build_graph()

        # ── Step 2: Translate TriggerContext → initial state ───────
        # source_lane comes from params — the Airflow callback or CLI
        # passes it. Default to "both" if missing, so the agent checks
        # both DLQ lanes when the trigger doesn't specify.
        source_lane = context.params.get("source_lane", "both")
        max_iterations = self.config.get(
            "agents.dlq_triage.max_iterations",
            self.config.get("agents.max_iterations", 10),
        )

        initial_state = make_initial_state(
            run_date=context.run_date,
            source_lane=source_lane,
            trigger_params=context.params,
            correlation_id=context.correlation_id,
            max_iterations=max_iterations,
        )

        # ── Step 3: Run the graph ──────────────────────────────────
        try:
            final_state = self._graph.invoke(initial_state)
        except Exception as exc:
            logger.error(
                "DLQTriageAgent.invoke: graph execution failed",
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
            logger.info(
                "DLQTriageAgent.invoke: completed successfully",
                extra={
                    "severity": report.recommended_severity.value,
                    "total_records": report.total_records,
                    "auto_requeued": report.auto_requeued,
                    "quarantined": report.quarantined,
                    "escalated": report.escalated,
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
            logger.warning(
                "DLQTriageAgent.invoke: completed but no report produced",
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

    Same helper as the Recon agent — the logic is identical because
    it only depends on the LangChain message format, not the specific
    tools used.

    Returns:
        List of tool names in call order, e.g.:
        ["read_dlq_records", "query_schema_changelog", "requeue_message"]
    """
    tool_names = []
    for msg in messages:
        if isinstance(msg, AIMessage) and hasattr(msg, "tool_calls"):
            for tc in msg.tool_calls:
                tool_names.append(tc["name"])
    return tool_names
