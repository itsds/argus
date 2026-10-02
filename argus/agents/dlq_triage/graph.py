"""
LangGraph StateGraph for the DLQ Triage & Auto-Remediation agent.

WHY THIS FILE MATTERS (learning concepts):

  This is the second LangGraph StateGraph in Argus. Comparing it with
  the Recon graph (Phase 2) teaches a crucial insight about agentic
  architecture: the GRAPH TOPOLOGY is reusable even when the TASK is
  completely different.

WHAT'S THE SAME AS THE RECON GRAPH:

  The ReAct loop topology is identical:

    entry ──► llm ──► should_continue? ──► tools ──► (back to llm)
                           │
                           ▼
                        report ──► END

  Same node types:
    - entry node: seeds SystemMessage + HumanMessage from template
    - llm node: calls LLM with full message history (REASON step)
    - tool node: ToolNode executes tool calls (ACT step)
    - report node: .with_structured_output() produces typed report
    - should_continue: conditional router (OBSERVE → decide next step)

  Same patterns:
    - Closure factories for DI
    - Separate model configs for tools vs structured output
    - Iteration safety valve in the router

WHAT'S DIFFERENT (new in Phase 3):

  1. DIFFERENT TOOLS — DLQ_TOOLS instead of RECON_TOOLS. The tool node
     executes DLQ-specific tools (read_dlq_records, query_schema_changelog,
     requeue_message) instead of recon tools.

  2. DIFFERENT STATE — DLQTriageState instead of ReconState. The state
     has source_lane instead of gate_name, plus classification and
     requeue_audit accumulators.

  3. DIFFERENT PROMPTS — DLQ_PROMPT_TEMPLATE with classification rubric
     and requeue safety rules instead of investigation strategy.

  4. DIFFERENT REPORT MODEL — DLQTriageReport instead of ReconReport.
     The report includes per-record classifications and requeue counts.

  5. DIFFERENT REPORT INSTRUCTION — the final HumanMessage in the report
     node asks for classification summaries and requeue audit, not just
     root cause analysis.

THE KEY INSIGHT:

  The ReAct loop is a PATTERN, not a one-off implementation. You could
  extract this topology into a reusable function that takes:
    - tools list
    - state class
    - prompt template
    - report model
  ...and builds the graph. That's exactly what frameworks like LangGraph
  are designed for — reusable patterns with swappable components.

  We're NOT doing that abstraction yet (premature abstraction is worse
  than duplication for learning). But seeing two concrete graphs with the
  same topology teaches you the pattern intuitively before abstracting it.

DESIGN DECISIONS:

  - Same closure pattern as Recon — consistency across agents.
  - No new node types — the ReAct loop is sufficient for DLQ triage.
    A more complex agent (Phase 5 Spark Debugger) might need additional
    nodes for hypothesis tracking or reflection.
  - The report instruction explicitly asks for classification summaries
    because the DLQ report has per-record breakdowns that the Recon
    report didn't need.
"""

from __future__ import annotations

import json

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode

from argus.agents.dlq_triage.prompts import DLQ_PROMPT_TEMPLATE
from argus.agents.dlq_triage.state import DLQTriageState
from argus.core.config import ArgusConfig
from argus.core.llm import create_llm
from argus.core.logging import get_logger
from argus.schemas.reports import DLQTriageReport
from argus.tools.pipeline.dlq_tools import DLQ_TOOLS

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Node factories
# ---------------------------------------------------------------------------
# Same closure pattern as the Recon graph. See reconciliation/graph.py for
# a detailed explanation of why closures over classes or RunnableConfig.


def _make_entry_node():
    """
    Factory for the entry node.

    Seeds the conversation with SystemMessage + HumanMessage from
    DLQ_PROMPT_TEMPLATE. Same pattern as the Recon entry node, but
    uses source_lane instead of gate_name.
    """

    def entry_node(state: dict) -> dict:
        """Seed the conversation with system prompt and triage request."""
        params_str = json.dumps(state["trigger_params"], indent=2)

        prompt_value = DLQ_PROMPT_TEMPLATE.invoke({
            "run_date": state["run_date"],
            "source_lane": state["source_lane"],
            "trigger_params": params_str,
        })

        logger.info(
            "entry_node: seeded conversation",
            extra={
                "run_date": state["run_date"],
                "source_lane": state["source_lane"],
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        return {"messages": prompt_value.to_messages()}

    return entry_node


def _make_llm_node(model_with_tools):
    """
    Factory for the LLM node (REASON step).

    Identical in structure to the Recon LLM node — sends full message
    history to the LLM, increments iteration, returns AIMessage.

    The LLM's BEHAVIOR is different because it has different tools
    bound and a different system prompt. The node logic is the same.
    """

    def llm_node(state: dict) -> dict:
        """Call the LLM with the full conversation history."""
        logger.info(
            "llm_node: calling LLM",
            extra={
                "iteration": state["iteration"] + 1,
                "max_iterations": state["max_iterations"],
                "message_count": len(state["messages"]),
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        response = model_with_tools.invoke(state["messages"])

        if hasattr(response, "tool_calls") and response.tool_calls:
            tool_names = [tc["name"] for tc in response.tool_calls]
            logger.info(
                "llm_node: LLM requested tool calls",
                extra={
                    "tools": tool_names,
                    "correlation_id": state.get("correlation_id", ""),
                },
            )
        else:
            logger.info(
                "llm_node: LLM produced text response (no tool calls)",
                extra={"correlation_id": state.get("correlation_id", "")},
            )

        return {
            "messages": [response],
            "iteration": state["iteration"] + 1,
        }

    return llm_node


def _make_report_node(model):
    """
    Factory for the report node.

    Same pattern as the Recon report node: takes the full message
    history and uses .with_structured_output(DLQTriageReport) to
    produce a typed report.

    Key difference: the report instruction asks for classification
    breakdowns and requeue audit, which the Recon report didn't need.
    """

    report_model = model.with_structured_output(DLQTriageReport)

    def report_node(state: dict) -> dict:
        """Synthesize the triage into a structured DLQTriageReport."""
        logger.info(
            "report_node: generating structured report",
            extra={
                "message_count": len(state["messages"]),
                "iteration": state["iteration"],
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        # The report instruction tells the LLM what to produce.
        # It's specific to the DLQ agent's output requirements.
        report_instruction = HumanMessage(
            content=(
                "Based on your triage above, produce your structured DLQ "
                "triage report. Include:\n"
                f"- total_records: total number of DLQ records you examined\n"
                "- records: a list of EVERY record with its classification, "
                "confidence score, reason, and action taken\n"
                "- auto_requeued: count of records you requeued\n"
                "- quarantined: count of records to keep in quarantine "
                "(schema_mismatch + data_quality)\n"
                "- escalated: count of records classified as UNKNOWN\n"
                "- summary: one-paragraph summary of the DLQ state\n"
                "- recommended_severity:\n"
                "    P1 if any UNKNOWN records or high volume of failures\n"
                "    P2 if schema_mismatch (needs consumer/table update)\n"
                "    P3 if all records are transient and requeued\n"
                "- notifications: alerts to send based on severity\n\n"
                "For each record in the records list:\n"
                "  - record_id: the DLQ record ID\n"
                "  - source_lane: 'kafka_dlq' or 'bad_files'\n"
                "  - classification: TRANSIENT, SCHEMA_MISMATCH, "
                "DATA_QUALITY, or UNKNOWN\n"
                "  - confidence: your confidence score (0.0 to 1.0)\n"
                "  - reason: one-sentence explanation of WHY this "
                "classification\n"
                "  - action_taken: what you did (e.g., 'requeued', "
                "'quarantined', 'escalated')\n\n"
                "Be precise — cite specific error classes, schema versions, "
                "and record IDs from the tool results."
            )
        )

        messages_for_report = state["messages"] + [report_instruction]

        try:
            report = report_model.invoke(messages_for_report)
            logger.info(
                "report_node: report generated successfully",
                extra={
                    "total_records": report.total_records,
                    "auto_requeued": report.auto_requeued,
                    "quarantined": report.quarantined,
                    "escalated": report.escalated,
                    "severity": report.recommended_severity.value,
                    "correlation_id": state.get("correlation_id", ""),
                },
            )
            return {"report": report}

        except Exception as exc:
            error_msg = f"report_node: structured output failed: {exc}"
            logger.error(
                error_msg,
                extra={"correlation_id": state.get("correlation_id", "")},
            )
            return {"errors": [error_msg]}

    return report_node


# ---------------------------------------------------------------------------
# Router — the conditional edge that controls the ReAct loop
# ---------------------------------------------------------------------------

def _should_continue(state: dict) -> str:
    """
    Decide the next step after the LLM node runs.

    Same logic as the Recon router:
      1. Max iterations hit? → force report
      2. LLM emitted tool_calls? → go to tools
      3. No tool calls? → go to report

    No DLQ-specific routing logic is needed because the ReAct loop
    pattern is the same. The classification and requeue decisions
    happen INSIDE the LLM's reasoning (guided by the system prompt),
    not in the graph topology.
    """
    messages = state["messages"]
    iteration = state["iteration"]
    max_iterations = state["max_iterations"]

    if iteration >= max_iterations:
        logger.warning(
            "should_continue: max iterations reached, forcing report",
            extra={
                "iteration": iteration,
                "max_iterations": max_iterations,
                "correlation_id": state.get("correlation_id", ""),
            },
        )
        return "report"

    last_message = messages[-1]

    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        return "tools"

    return "report"


# ---------------------------------------------------------------------------
# Graph builder — the public API
# ---------------------------------------------------------------------------

def build_dlq_graph(config: ArgusConfig):
    """
    Build and compile the DLQ Triage & Auto-Remediation StateGraph.

    Same assembly pattern as build_recon_graph but with DLQ-specific
    components: DLQ_TOOLS, DLQTriageState, DLQ_PROMPT_TEMPLATE,
    DLQTriageReport.

    Architecture (same topology as Recon):

        entry ──► llm ──► should_continue? ──► tools ──► (back to llm)
                              │
                              ▼
                           report ──► END

    Args:
        config: ArgusConfig with LLM settings and agent parameters.

    Returns:
        A compiled LangGraph StateGraph (CompiledStateGraph).
    """
    # Step 1: Create the LLM
    llm = create_llm(config)

    # Step 2: Two model configurations
    model_with_tools = llm.bind_tools(DLQ_TOOLS)

    logger.info(
        "build_dlq_graph: building graph",
        extra={
            "provider": config.llm.get("provider", "google"),
            "model": config.llm.get("model", "unknown"),
            "tool_count": len(DLQ_TOOLS),
            "tool_names": [t.name for t in DLQ_TOOLS],
        },
    )

    # Step 3: Tool node (with DLQ tools)
    tool_node = ToolNode(DLQ_TOOLS)

    # Step 4: Build the StateGraph (with DLQ state)
    graph = StateGraph(DLQTriageState)

    # Step 5: Add nodes
    graph.add_node("entry", _make_entry_node())
    graph.add_node("llm", _make_llm_node(model_with_tools))
    graph.add_node("tools", tool_node)
    graph.add_node("report", _make_report_node(llm))

    # Step 6: Wire edges (same topology as Recon)
    graph.set_entry_point("entry")
    graph.add_edge("entry", "llm")
    graph.add_conditional_edges(
        "llm",
        _should_continue,
        {
            "tools": "tools",
            "report": "report",
        },
    )
    graph.add_edge("tools", "llm")
    graph.add_edge("report", END)

    # Step 7: Compile
    compiled = graph.compile()

    logger.info("build_dlq_graph: graph compiled successfully")

    return compiled
