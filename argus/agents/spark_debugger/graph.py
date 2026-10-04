"""
LangGraph StateGraph for the AI Spark Debugger agent.

WHY THIS FILE MATTERS (learning concepts):

  This is the most complex graph in Argus. While the Reconciliation agent
  uses a plain ReAct loop (llm → tools → llm → report), the Spark Debugger
  adds a REFLECTION step between investigation and reporting. This creates
  a two-level iteration structure that catches premature conclusions.

THE REACT + REFLECTION PATTERN:

  The key insight: LLMs often latch onto the FIRST plausible explanation.
  In Spark debugging, this is dangerous because symptoms often mask root
  causes:
    - "GC is high" ← but WHY? (skew → spill → GC)
    - "Stage 2 is slow" ← but WHY? (one partition has 130x more data)

  The reflection step forces the agent to challenge its own hypothesis
  before committing to a report. This catches the common failure mode
  of treating a symptom as the root cause.

GRAPH TOPOLOGY:

  ┌────────────────────────────────────────────────────────────────┐
  │                                                                │
  │    entry ──► llm ──► should_continue? ──► tools ──┐           │
  │                          │                         │           │
  │                          │ (no tools / max iter)   │           │
  │                          ▼                         │           │
  │                       reflect ──► should_revise?   │           │
  │                                      │      │      │           │
  │          (hypothesis confirmed)      │      │      │           │
  │                          ┌───────────┘      │      │           │
  │                          ▼                  │      │           │
  │                       report ──► END        │      │           │
  │                                             │      │           │
  │              ◄──────────────────────────────┘      │           │
  │              (needs more investigation)             │           │
  │              ◄─────────────────────────────────────┘           │
  │              (loop back to llm)                                │
  └────────────────────────────────────────────────────────────────┘

  Compared to the Recon graph:
    - SAME: entry → llm → tools loop (the ReAct backbone)
    - NEW: reflect node between investigation and report
    - NEW: should_revise router that can loop back to llm

KEY DIFFERENCES FROM RECON GRAPH:

  1. Two conditional edges (not one):
     - should_continue: after llm → tools or reflect?
     - should_revise: after reflect → report or back to llm?

  2. Reflect node — a separate LLM call (no tools bound) that evaluates
     the investigation's hypothesis. Uses SPARK_REFLECTION_PROMPT.

  3. Hypothesis extraction — after the LLM stops calling tools, the
     reflect node extracts the hypothesis from the last AI message and
     evaluates it against the evidence.

  4. Two-level iteration — inner loop (iteration/max_iterations) caps
     tool calls, outer loop (reflection_count/max_reflections) caps
     hypothesis refinements.

DESIGN DECISIONS:

  - The reflect node uses the SAME base LLM (no tools bound) but with
    the reflection prompt. This keeps the model consistent while changing
    the task framing.

  - should_revise parses the reflection response for "NEEDS_MORE_INVESTIGATION"
    vs "HYPOTHESIS_CONFIRMED". This is string-matching, not structured output,
    because the reflection is conversational — forcing JSON would reduce the
    quality of the self-critique.

  - The hypothesis is extracted from the last AIMessage's text content.
    This is the simplest approach — the investigation prompt instructs
    the LLM to state its hypothesis clearly at the end.
"""

from __future__ import annotations

import json

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode

from argus.agents.spark_debugger.prompts import (
    SPARK_PROMPT_TEMPLATE,
    SPARK_REFLECTION_PROMPT,
)
from argus.agents.spark_debugger.state import SparkDebuggerState
from argus.core.config import ArgusConfig
from argus.core.llm import create_llm
from argus.core.logging import get_logger
from argus.schemas.reports import SparkDiagnosis
from argus.tools.compute.spark_tools import SPARK_TOOLS

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Node factories
# ---------------------------------------------------------------------------
# Same closure pattern as the Recon agent — each factory captures the LLM
# in its scope and returns a function LangGraph can call as a node.
# See argus/agents/reconciliation/graph.py for detailed explanation of
# why closures over globals or RunnableConfig.


def _make_entry_node():
    """
    Factory for the entry node.

    Seeds the conversation with the Spark investigation prompt:
      1. SystemMessage — Spark expertise, investigation strategy, rules
      2. HumanMessage — the specific app_id and trigger params to debug

    Identical pattern to Recon's entry node, different prompt template.
    """

    def entry_node(state: dict) -> dict:
        """Seed the conversation with system prompt and investigation request."""
        params_str = json.dumps(state["trigger_params"], indent=2)

        prompt_value = SPARK_PROMPT_TEMPLATE.invoke({
            "app_id": state["app_id"],
            "trigger_params": params_str,
        })

        logger.info(
            "entry_node: seeded spark debugger conversation",
            extra={
                "app_id": state["app_id"],
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        return {"messages": prompt_value.to_messages()}

    return entry_node


def _make_llm_node(model_with_tools):
    """
    Factory for the LLM node (the REASON step in ReAct).

    Identical to Recon's llm_node — calls the LLM with the full message
    history and increments the iteration counter. The LLM either emits
    tool_calls (→ tools node) or a text response (→ reflect node).

    The only difference: after investigation ends (no tool calls), this
    graph routes to REFLECT instead of directly to REPORT.
    """

    def llm_node(state: dict) -> dict:
        """Call the LLM with the full conversation history."""
        logger.info(
            "llm_node: calling LLM",
            extra={
                "iteration": state["iteration"] + 1,
                "max_iterations": state["max_iterations"],
                "reflection_count": state["reflection_count"],
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
                "llm_node: LLM produced text response (investigation pause)",
                extra={"correlation_id": state.get("correlation_id", "")},
            )

        return {
            "messages": [response],
            "iteration": state["iteration"] + 1,
        }

    return llm_node


def _make_reflect_node(model):
    """
    Factory for the reflect node — NEW in this agent.

    This is what makes the Spark Debugger different from the other agents.
    After the ReAct investigation loop pauses (LLM stops calling tools),
    the reflect node:

      1. Extracts the hypothesis from the last AIMessage
      2. Renders the reflection prompt with the hypothesis injected
      3. Calls the LLM (WITHOUT tools) to evaluate the hypothesis
      4. Increments the reflection counter
      5. Returns the reflection response for the should_revise router

    WHY NO TOOLS in the reflect node?

      The reflection prompt tells the LLM to THINK, not ACT. If tools
      were bound, the LLM might call them instead of critically evaluating
      its work. By using a plain model (no tools), we force pure reasoning.

    WHY EXTRACT THE HYPOTHESIS from the AIMessage?

      The investigation prompt instructs the LLM to "state your hypothesis
      clearly" at the end. We extract this text and inject it into the
      reflection prompt so the reflection has a clear target to evaluate.

      In practice, the hypothesis is usually the full text of the last
      AIMessage (when the LLM stops calling tools, its response IS the
      hypothesis). We use the full content rather than trying to parse
      out a specific section — the reflection LLM can handle a multi-
      paragraph hypothesis.
    """

    def reflect_node(state: dict) -> dict:
        """Evaluate the current hypothesis against gathered evidence."""
        # Extract hypothesis from the last AI message
        last_ai_msg = None
        for msg in reversed(state["messages"]):
            if isinstance(msg, AIMessage):
                last_ai_msg = msg
                break

        hypothesis = ""
        if last_ai_msg:
            # The hypothesis is the text content of the last AIMessage
            # (when the LLM stops calling tools, its response IS the
            # hypothesis statement)
            hypothesis = (
                last_ai_msg.content
                if isinstance(last_ai_msg.content, str)
                else str(last_ai_msg.content)
            )

        logger.info(
            "reflect_node: evaluating hypothesis",
            extra={
                "hypothesis_length": len(hypothesis),
                "reflection_count": state["reflection_count"] + 1,
                "max_reflections": state["max_reflections"],
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        # Build the reflection message — inject the hypothesis into
        # the reflection prompt template
        reflection_msg = HumanMessage(
            content=SPARK_REFLECTION_PROMPT.format(hypothesis=hypothesis),
        )

        # Call the LLM WITHOUT tools — pure reasoning
        messages_for_reflection = state["messages"] + [reflection_msg]
        response = model.invoke(messages_for_reflection)

        logger.info(
            "reflect_node: reflection complete",
            extra={
                "reflection_count": state["reflection_count"] + 1,
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        return {
            "messages": [reflection_msg, response],
            "hypothesis": hypothesis,
            "reflection_count": state["reflection_count"] + 1,
        }

    return reflect_node


def _make_report_node(model):
    """
    Factory for the report node.

    Same pattern as Recon's report node — uses .with_structured_output()
    to produce a SparkDiagnosis Pydantic object from the full conversation
    history. The only difference is the schema (SparkDiagnosis instead of
    ReconReport) and the instruction message referencing Spark-specific
    fields.

    This node runs ONCE, after the reflect node confirms the hypothesis.
    """

    report_model = model.with_structured_output(SparkDiagnosis)

    def report_node(state: dict) -> dict:
        """Synthesize the investigation into a structured SparkDiagnosis."""
        logger.info(
            "report_node: generating structured diagnosis",
            extra={
                "message_count": len(state["messages"]),
                "iteration": state["iteration"],
                "reflection_count": state["reflection_count"],
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        report_instruction = HumanMessage(
            content=(
                "Based on your investigation and reflection above, produce "
                "your structured Spark diagnosis report. Include:\n"
                f"- app_id: {state['app_id']}\n"
                "- app_name (from the application info)\n"
                "- total_duration_seconds (from the application info)\n"
                "- All bottlenecks found, each with:\n"
                "  - category (skew/spill/small_files/broadcast/gc_pressure)\n"
                "  - stage_id (if applicable)\n"
                "  - evidence (specific numbers from your investigation)\n"
                "  - impact (high/medium/low)\n"
                "  - recommendation (specific Spark config or code change)\n"
                "- root_cause_summary: the CAUSAL CHAIN, not just the symptom\n"
                "- recommendations: ordered list of fixes\n"
                "- recommended_severity (P1/P2/P3)\n\n"
                "Be precise — cite specific stage IDs, task counts, memory "
                "figures, skew ratios, and config values from the tool results."
            )
        )

        messages_for_report = state["messages"] + [report_instruction]

        try:
            report = report_model.invoke(messages_for_report)
            logger.info(
                "report_node: diagnosis generated successfully",
                extra={
                    "app_id": report.app_id,
                    "bottleneck_count": len(report.bottlenecks),
                    "root_cause": report.root_cause_summary[:100],
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
# Routers — two conditional edges for the two-level loop
# ---------------------------------------------------------------------------


def _should_continue(state: dict) -> str:
    """
    Router after the LLM node — controls the inner (ReAct) loop.

    Three possible outcomes:

      1. Max iterations reached → force reflect (safety valve)
      2. LLM emitted tool_calls → go to tools (ACT step)
      3. LLM emitted text (no tools) → go to reflect (evaluate hypothesis)

    DIFFERENCE FROM RECON:
      Recon routes to "report" when the LLM stops calling tools.
      Spark Debugger routes to "reflect" — an intermediate evaluation
      step before committing to a report.
    """
    messages = state["messages"]
    iteration = state["iteration"]
    max_iterations = state["max_iterations"]

    if iteration >= max_iterations:
        logger.warning(
            "should_continue: max iterations reached, forcing reflection",
            extra={
                "iteration": iteration,
                "max_iterations": max_iterations,
                "correlation_id": state.get("correlation_id", ""),
            },
        )
        return "reflect"

    last_message = messages[-1]

    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        return "tools"

    # No tool calls — the LLM has formed a hypothesis, send to reflect
    return "reflect"


def _should_revise(state: dict) -> str:
    """
    Router after the reflect node — controls the outer (reflection) loop.

    Examines the reflection response to decide:

      1. Max reflections reached → force report (safety valve)
      2. Reflection says "NEEDS_MORE_INVESTIGATION" → back to llm
      3. Reflection says "HYPOTHESIS_CONFIRMED" or anything else → report

    WHY STRING MATCHING (not structured output)?

      The reflection is a self-critique exercise. Forcing JSON would
      constrain the LLM's reasoning and reduce reflection quality.
      Instead, the reflection prompt asks for one of two keywords at
      the end, and we check for them. This is intentionally loose —
      the LLM's detailed reasoning matters more than the format.

    WHY CHECK MAX_REFLECTIONS FIRST?

      Same reason as max_iterations in should_continue — safety valve.
      Without it, the agent could bounce between "needs more investigation"
      and "investigate" forever. Two reflections is enough for most cases;
      after that, whatever hypothesis we have is our best shot.
    """
    reflection_count = state["reflection_count"]
    max_reflections = state["max_reflections"]

    # Safety valve
    if reflection_count >= max_reflections:
        logger.warning(
            "should_revise: max reflections reached, forcing report",
            extra={
                "reflection_count": reflection_count,
                "max_reflections": max_reflections,
                "correlation_id": state.get("correlation_id", ""),
            },
        )
        return "report"

    # Check the last message (the reflection response) for the verdict
    messages = state["messages"]
    last_message = messages[-1]

    if isinstance(last_message, AIMessage):
        content = (
            last_message.content
            if isinstance(last_message.content, str)
            else str(last_message.content)
        )

        if "NEEDS_MORE_INVESTIGATION" in content.upper():
            logger.info(
                "should_revise: reflection found gaps, re-investigating",
                extra={
                    "reflection_count": reflection_count,
                    "correlation_id": state.get("correlation_id", ""),
                },
            )
            return "llm"

    # Default: hypothesis confirmed (or unrecognized format → proceed)
    logger.info(
        "should_revise: hypothesis confirmed, proceeding to report",
        extra={
            "reflection_count": reflection_count,
            "correlation_id": state.get("correlation_id", ""),
        },
    )
    return "report"


# ---------------------------------------------------------------------------
# Graph builder — the public API
# ---------------------------------------------------------------------------

def build_spark_debugger_graph(config: ArgusConfig):
    """
    Build and compile the Spark Debugger StateGraph.

    Architecture (ReAct + Reflection):

        entry ──► llm ──► should_continue? ──► tools ──► (back to llm)
                              │
                              ▼
                           reflect ──► should_revise? ──► (back to llm)
                                            │
                                            ▼
                                         report ──► END

    Two conditional edges create the two-level loop:
      - Inner: llm → tools → llm (ReAct tool-calling loop)
      - Outer: llm → reflect → llm (hypothesis refinement loop)

    Args:
        config: ArgusConfig with LLM settings and agent parameters.

    Returns:
        A compiled LangGraph StateGraph (CompiledStateGraph).

    Usage:
        config = load_config("dev")
        graph = build_spark_debugger_graph(config)
        result = graph.invoke(make_initial_state(
            app_id="app-20260928-001",
            trigger_params={"threshold_minutes": 30},
            correlation_id="abc123",
        ))
        diagnosis = result["report"]  # SparkDiagnosis or None
    """
    # --- Step 1: Create the LLM ---
    llm = create_llm(config)

    # --- Step 2: Create model configurations ---
    # model_with_tools: for the investigation loop (llm_node)
    # llm (plain): for reflection (reflect_node) and report (report_node)
    model_with_tools = llm.bind_tools(SPARK_TOOLS)

    logger.info(
        "build_spark_debugger_graph: building graph",
        extra={
            "provider": config.llm.get("provider", "google"),
            "model": config.llm.get("model", "unknown"),
            "tool_count": len(SPARK_TOOLS),
            "tool_names": [t.name for t in SPARK_TOOLS],
        },
    )

    # --- Step 3: Create the tool node ---
    tool_node = ToolNode(SPARK_TOOLS)

    # --- Step 4: Build the StateGraph ---
    graph = StateGraph(SparkDebuggerState)

    # --- Step 5: Add nodes ---
    graph.add_node("entry", _make_entry_node())
    graph.add_node("llm", _make_llm_node(model_with_tools))
    graph.add_node("tools", tool_node)
    graph.add_node("reflect", _make_reflect_node(llm))   # NEW
    graph.add_node("report", _make_report_node(llm))

    # --- Step 6: Wire edges ---
    graph.set_entry_point("entry")

    # entry → llm: always start with the first LLM call
    graph.add_edge("entry", "llm")

    # llm → conditional: tools (continue investigating) or reflect
    graph.add_conditional_edges(
        "llm",
        _should_continue,
        {
            "tools": "tools",       # ACT step — execute tool calls
            "reflect": "reflect",   # REFLECT step — evaluate hypothesis
        },
    )

    # tools → llm: after tool execution, return to reasoning
    graph.add_edge("tools", "llm")

    # reflect → conditional: report (hypothesis confirmed) or back to llm
    graph.add_conditional_edges(
        "reflect",
        _should_revise,
        {
            "report": "report",   # hypothesis solid → produce diagnosis
            "llm": "llm",         # gaps found → re-investigate
        },
    )

    # report → END: diagnosis produced, graph is done
    graph.add_edge("report", END)

    # --- Step 7: Compile ---
    compiled = graph.compile()

    logger.info("build_spark_debugger_graph: graph compiled successfully")

    return compiled
