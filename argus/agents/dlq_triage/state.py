"""
LangGraph state schema for the DLQ Triage & Auto-Remediation agent.

WHY THIS FILE MATTERS (learning concepts):

  This is the second LangGraph state schema in Argus. Comparing it with
  ReconState (Phase 2) teaches you how different agent TASKS require
  different state SHAPES — even though the underlying LangGraph mechanism
  (TypedDict with Annotated reducers) is the same.

WHAT'S NEW IN PHASE 3 (compared to ReconState):

  1. CLASSIFICATION ACCUMULATOR — the DLQ agent doesn't just investigate
     and report; it CLASSIFIES each record one by one. The `classifications`
     field uses Annotated[list[DLQRecord], operator.add] so the agent can
     build up a list of classified records across multiple ReAct iterations.

     ReconState didn't need this because the Recon agent produces a single
     monolithic report at the end. The DLQ agent's work is incremental —
     each record gets its own classification with a confidence score.

  2. REQUEUE AUDIT TRAIL — the DLQ agent can CHANGE PIPELINE STATE by
     requeuing transient failures. Every requeue action is tracked in
     `requeue_audit` so the final report has a full audit trail.

     This is a new pattern: state that tracks SIDE EFFECTS, not just
     observations. The Recon agent was read-only; the DLQ agent acts.

  3. SOURCE LANE instead of GATE NAME — the DLQ agent is triggered by
     a DLQ threshold breach (too many records in the dead-letter queue),
     not by a gate failure. The trigger context tells it which lane(s)
     to investigate: "kafka_dlq", "bad_files", or "both".

  4. DLQ-SPECIFIC REPORT TYPE — uses DLQTriageReport instead of
     ReconReport. The report includes per-record classifications,
     requeue counts, and quarantine counts.

KEY LANGGRAPH CONCEPT — SAME MECHANISM, DIFFERENT SHAPE:

  Both ReconState and DLQTriageState use the same LangGraph patterns:
    - Annotated[list[X], operator.add]  → accumulate items
    - Scalar fields (str, int)          → overwrite on update
    - TypedDict-style class             → defines the state channels

  But the FIELDS are different because the TASK is different:
    - ReconState: investigate what went wrong → single report
    - DLQTriageState: classify N records + act on some → incremental output

  This is a key insight: the LangGraph framework is generic, but the
  state schema encodes your agent's specific workflow requirements.
"""

from __future__ import annotations

import operator
from typing import Annotated, Any

from langchain_core.messages import AnyMessage

from argus.schemas.reports import DLQRecord, DLQTriageReport


# ---------------------------------------------------------------------------
# Graph state
# ---------------------------------------------------------------------------

class DLQTriageState:
    """
    State schema for the DLQ Triage & Auto-Remediation graph.

    Declared as annotations on a class (LangGraph's dict-state pattern).
    LangGraph reads __annotations__ to build the state channels. Each
    field becomes a "channel" in the graph with its own reducer.

    Flow through the graph:
      1. Entry node seeds: source_lane, run_date, trigger_params, messages
      2. LLM node reads messages → reasons about DLQ records
      3. Tool node executes tool calls → appends ToolMessages
         (read_dlq_records, query_schema_changelog, requeue_message)
      4. Router checks: more records to classify? → loop back to LLM
      5. Report node reads message history + classifications → DLQTriageReport

    The key difference from ReconState is that classification happens
    DURING the ReAct loop (not just at the end). Each time the LLM
    reasons about a record, the classification node can extract and
    accumulate the classification into `classifications`.
    """

    # ── Conversation history (the ReAct backbone) ─────────────────────
    # Same pattern as ReconState. Every LLM turn and tool result appends.
    messages: Annotated[list[AnyMessage], operator.add]

    # ── Trigger metadata (set once by entry node, read-only after) ────
    run_date: str                       # e.g. "2026-09-30"
    source_lane: str                    # "kafka_dlq", "bad_files", or "both"
    trigger_params: dict[str, Any]      # full params from trigger
    correlation_id: str                 # for log tracing

    # ── Loop control ──────────────────────────────────────────────────
    iteration: int
    max_iterations: int                 # from config: agents.dlq_triage.max_iterations

    # ── Classification accumulator (NEW in Phase 3) ───────────────────
    # As the agent classifies each DLQ record, it appends a DLQRecord
    # to this list. The operator.add reducer means classifications
    # accumulate across ReAct iterations — the agent can classify some
    # records in one iteration and more in the next.
    #
    # This is different from ReconState's approach: there, findings are
    # only extracted at the end by the report node. Here, classifications
    # build up incrementally because the agent may ACT on them (requeue)
    # before all records are classified.
    classifications: Annotated[list[DLQRecord], operator.add]

    # ── Requeue audit trail (NEW in Phase 3) ──────────────────────────
    # Every requeue_message tool call result is logged here for the
    # final report's audit trail. This tracks SIDE EFFECTS — the DLQ
    # agent changes pipeline state, unlike the read-only Recon agent.
    requeue_audit: Annotated[list[str], operator.add]

    # ── Final output (set by the report node) ─────────────────────────
    report: DLQTriageReport | None

    # ── Error accumulator ─────────────────────────────────────────────
    errors: Annotated[list[str], operator.add]


# ---------------------------------------------------------------------------
# Initial state factory
# ---------------------------------------------------------------------------

def make_initial_state(
    run_date: str,
    source_lane: str,
    trigger_params: dict[str, Any],
    correlation_id: str,
    max_iterations: int = 10,
) -> dict[str, Any]:
    """
    Build the seed state dict for graph.invoke().

    Same pattern as the Recon agent's make_initial_state, but with
    DLQ-specific fields (source_lane instead of gate_name, plus
    empty accumulators for classifications and requeue_audit).

    Why a factory function instead of constructing the dict inline?
      - Single place to set defaults (iteration=0, report=None, empty lists)
      - Type-checks the required fields at the call site
      - Easy to extend when we add fields later

    Usage in the agent:
        state = make_initial_state(
            run_date=context.run_date,
            source_lane=context.params.get("source_lane", "both"),
            trigger_params=context.params,
            correlation_id=context.correlation_id,
            max_iterations=config.get("agents.dlq_triage.max_iterations", 10),
        )
        result = graph.invoke(state)

    Args:
        run_date: ISO date string from the trigger.
        source_lane: Which DLQ lane to investigate:
                     "kafka_dlq", "bad_files", or "both".
        trigger_params: Full params dict from the trigger.
        correlation_id: Tracing ID for structured logs.
        max_iterations: Safety cap on ReAct loop iterations.

    Returns:
        Dict matching DLQTriageState's shape, ready for graph.invoke().
    """
    return {
        "messages": [],              # entry node seeds System + Human
        "run_date": run_date,
        "source_lane": source_lane,
        "trigger_params": trigger_params,
        "correlation_id": correlation_id,
        "iteration": 0,
        "max_iterations": max_iterations,
        "classifications": [],       # accumulates as agent classifies
        "requeue_audit": [],         # tracks requeue side effects
        "report": None,
        "errors": [],
    }
