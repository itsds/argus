"""
LangGraph state schema for the AI Spark Debugger agent.

WHY THIS FILE MATTERS (learning concepts):

  The Spark Debugger uses a ReAct + Reflection pattern, which needs
  MORE state than plain ReAct (used by Recon and DLQ agents). The
  extra state fields support the reflection loop:

    - hypothesis: the agent's current best guess about the bottleneck
    - reflection_count: how many times the agent has reflected
    - max_reflections: safety cap on reflection iterations

  The reflection pattern adds a self-critique step BETWEEN investigation
  and report generation. After the ReAct loop finishes (no more tool
  calls), the agent enters a "reflect" node that evaluates whether the
  evidence supports the hypothesis or if more investigation is needed.

KEY CONCEPTS IN THIS FILE:

  1. Hypothesis tracking — unlike the other agents that build up
     evidence and produce a report at the end, the Spark Debugger
     explicitly tracks its working hypothesis. This matters because:
       - Spark issues often have layered causes (skew → spill → GC)
       - The first hypothesis might be wrong (blaming GC when the
         real cause is skew that CAUSES the GC pressure)
       - The reflection node can compare the hypothesis against
         evidence and catch these mistakes

  2. Two-level iteration control (same pattern as Backfill):
       - Inner loop: iteration / max_iterations → ReAct tool calls
       - Outer loop: reflection_count / max_reflections → hypothesis
         refinement cycles

  3. No HITL fields — unlike the Backfill agent, the Spark Debugger
     is fully autonomous. It doesn't need approval gates because its
     output (a diagnosis report) is read-only — it doesn't trigger
     any side effects like requeuing or backfilling.

STATE FLOW THROUGH THE GRAPH:

  1. Entry node seeds: app_id, trigger_params, messages
  2. LLM investigates using compute tools (ReAct loop)
  3. When LLM stops calling tools → reflect node evaluates hypothesis
  4. If hypothesis needs work → back to LLM with reflection feedback
  5. If hypothesis is solid → report node produces SparkDiagnosis
"""

from __future__ import annotations

import operator
from typing import Annotated, Any

from langchain_core.messages import AnyMessage

from argus.schemas.reports import SparkDiagnosis


# ---------------------------------------------------------------------------
# Graph state
# ---------------------------------------------------------------------------

class SparkDebuggerState:
    """
    State schema for the Spark Debugger graph.

    Declared as annotations on a class (LangGraph's dict-state pattern).
    LangGraph reads __annotations__ to build the state channels.

    COMPARISON WITH ReconState:
      - Same: messages (ReAct backbone), iteration tracking, report, errors
      - New: hypothesis (working theory), reflection_count (outer loop),
             app_id (the Spark app under investigation)

    Flow:
      entry → [llm → tools]* → reflect → report (or back to llm)
    """

    # ── Conversation history (the ReAct backbone) ─────────────────────
    messages: Annotated[list[AnyMessage], operator.add]

    # ── Trigger metadata (set once by entry node) ─────────────────────
    app_id: str                         # Spark application ID to debug
    trigger_params: dict[str, Any]      # full params from trigger
    correlation_id: str                 # for log tracing

    # ── ReAct loop control (inner loop) ───────────────────────────────
    # Counts LLM calls. Max iterations caps the total number of tool-use
    # cycles to prevent runaway investigation.
    iteration: int
    max_iterations: int                 # from config

    # ── Reflection loop control (outer loop) ──────────────────────────
    # After the ReAct loop finishes (LLM stops calling tools), the
    # reflect node evaluates the hypothesis. If it's incomplete, the
    # agent goes back to investigation. reflection_count tracks how
    # many reflect→re-investigate cycles have occurred.
    #
    # WHY a separate counter? Because max_iterations caps tool calls
    # (inner loop), but a reflection might trigger MORE tool calls.
    # Without a separate cap, the agent could alternate between
    # "reflect → call one more tool → reflect → call one more tool"
    # indefinitely. max_reflections breaks this outer loop.
    hypothesis: str                     # current working hypothesis
    reflection_count: int               # how many reflections so far
    max_reflections: int                # safety cap (typically 2-3)

    # ── Final output ──────────────────────────────────────────────────
    report: SparkDiagnosis | None

    # ── Error accumulator ─────────────────────────────────────────────
    errors: Annotated[list[str], operator.add]


# ---------------------------------------------------------------------------
# Initial state factory
# ---------------------------------------------------------------------------

def make_initial_state(
    app_id: str,
    trigger_params: dict[str, Any],
    correlation_id: str,
    max_iterations: int = 15,
    max_reflections: int = 2,
) -> dict[str, Any]:
    """
    Build the seed state dict for graph.invoke().

    Why max_iterations=15 (vs 10 for Recon)?
      The Spark Debugger typically needs more tool calls because it
      investigates at multiple levels:
        1. get_application_info (1 call)
        2. get_stage_metrics for all stages (1 call)
        3. get_stage_metrics for specific stage (1 call)
        4. get_task_distribution for bottleneck stage (1 call)
        5. get_executor_metrics (1 call)
        6. parse_physical_plan (1 call)
        7. read_event_log for specifics (1-3 calls)
      That's 7-9 calls for a thorough investigation, plus possible
      re-investigation after reflection → 15 is a safe cap.

    Why max_reflections=2?
      Most diagnoses converge after 1 reflection. 2 reflections cover
      the case where the first reflection reveals a completely different
      root cause (e.g., "I thought it was GC, but actually it's skew
      causing the GC"). More than 2 reflections rarely add value and
      burn tokens.

    Args:
        app_id: Spark application ID to debug.
        trigger_params: Full params dict from the trigger.
        correlation_id: Tracing ID for structured logs.
        max_iterations: Safety cap on ReAct loop iterations.
        max_reflections: Safety cap on reflection cycles.

    Returns:
        Dict matching SparkDebuggerState's shape, ready for graph.invoke().
    """
    return {
        "messages": [],
        "app_id": app_id,
        "trigger_params": trigger_params,
        "correlation_id": correlation_id,
        "iteration": 0,
        "max_iterations": max_iterations,
        "hypothesis": "",           # empty until the agent forms one
        "reflection_count": 0,
        "max_reflections": max_reflections,
        "report": None,
        "errors": [],
    }
