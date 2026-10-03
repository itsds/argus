"""
LangGraph state schema for the Incident & Backfill Planning agent.

WHY THIS FILE MATTERS (learning concepts):

  This is the third LangGraph state schema in Argus. It introduces the
  STATE FIELDS NEEDED FOR HUMAN-IN-THE-LOOP (HITL) approval — something
  neither the Recon nor DLQ agents needed because they ran autonomously.

WHAT'S NEW IN PHASE 4 (compared to ReconState and DLQTriageState):

  1. PLAN AS INTERMEDIATE OUTPUT — the Recon and DLQ agents produce their
     report as the LAST step. The Backfill agent produces a BackfillPlan
     as an INTERMEDIATE step — it's the output of investigation, but the
     INPUT to the approval gate. The plan lives in state so the graph can:
       a) Show it to the human for approval
       b) Pass it to the execution phase if approved
       c) Hand it back to the LLM with feedback if rejected

  2. APPROVAL STATUS — a three-valued field tracking where we are in the
     HITL flow: "pending" (plan produced, waiting for human), "approved"
     (human said go), "rejected" (human said rework). The conditional
     edge after the interrupt() reads this to decide where to route.

  3. REVISION FEEDBACK — when the human rejects a plan, they provide
     feedback explaining why. This text is injected into the LLM's
     conversation as a HumanMessage so it can rework the plan. Without
     this field, the LLM would rework blindly — the feedback loop is
     what makes rejection useful.

  4. PLAN ITERATION TRACKING — the rejection loop needs a safety cap.
     Without it, a human could keep rejecting forever, and the LLM would
     keep reworking. plan_iterations counts how many plans have been
     produced; max_plan_iterations caps it (default 3). This is the
     PLAN-level safety valve, separate from the INVESTIGATION-level
     max_iterations that caps the ReAct tool-calling loop.

  5. EXECUTION AUDIT — after approval, the execution phase tracks every
     backfill step (lock acquired, step executed, lock released). This
     accumulates via operator.add just like DLQTriageState's requeue_audit,
     but tracks more structured multi-step execution.

KEY INSIGHT — TWO LEVELS OF ITERATION CONTROL:

  The Backfill agent has TWO loops, each with its own safety cap:

    Inner loop: ReAct investigation (iteration / max_iterations)
      → How many tool calls the LLM can make during investigation
      → Same pattern as Recon and DLQ agents
      → Prevents the LLM from calling tools forever

    Outer loop: Plan revision (plan_iterations / max_plan_iterations)
      → How many times the human can reject and request rework
      → NEW in Phase 4 — only needed because of HITL
      → Prevents infinite reject-rework cycles

  These are independent — the inner loop resets each time the outer loop
  cycles (the LLM gets fresh investigation iterations for each rework).

COMPARING STATE SCHEMAS ACROSS PHASES:

  ReconState (Phase 2):
    messages, run_date, gate_name, trigger_params, correlation_id,
    iteration, max_iterations, report, errors
    → Simple: investigate → report

  DLQTriageState (Phase 3):
    + source_lane, classifications, requeue_audit
    → Adds: incremental classification + side-effect tracking

  BackfillState (Phase 4):
    + plan, approval_status, revision_feedback,
      plan_iterations, max_plan_iterations, execution_audit
    → Adds: intermediate output + HITL flow control + execution tracking
"""

from __future__ import annotations

import operator
from typing import Annotated, Any

from langchain_core.messages import AnyMessage

from argus.schemas.reports import BackfillPlan


# ---------------------------------------------------------------------------
# Graph state
# ---------------------------------------------------------------------------

class BackfillState:
    """
    State schema for the Incident & Backfill Planning graph.

    Declared as annotations on a class (LangGraph's dict-state pattern).
    LangGraph reads __annotations__ to build the state channels. Each
    field becomes a "channel" in the graph with its own reducer.

    Flow through the graph (Plan-then-Execute with HITL):

      1. Entry node seeds: run_date, trigger_params, messages
      2. INVESTIGATION PHASE (ReAct loop):
         a. LLM node reads messages → calls investigation tools
         b. Tool node executes → appends ToolMessages
         c. Router: more investigation needed? → loop back to LLM
      3. PLANNING NODE:
         → LLM produces BackfillPlan (structured output)
         → Sets plan in state, approval_status = "pending"
      4. APPROVAL GATE (HITL interrupt):
         → Graph pauses via interrupt()
         → Human reviews plan, chooses: approve / reject with feedback
         → Graph resumes via Command(resume={"decision": ..., "feedback": ...})
      5. ROUTING:
         → If approved: proceed to execution phase
         → If rejected: inject feedback, loop back to investigation
         → If max plan iterations reached: stop with error
      6. EXECUTION PHASE (after approval):
         a. Execute backfill steps (Lock → Execute → Release)
         b. Each step appended to execution_audit
      7. REPORT NODE:
         → Packages plan + execution_audit into final output

    The key difference from Recon/DLQ is steps 3-5: the graph PAUSES
    for human input and can LOOP BACK based on the human's decision.
    This requires checkpointing (MemorySaver/SqliteSaver) to persist
    the state across the interrupt/resume boundary.
    """

    # ── Conversation history (the ReAct backbone) ─────────────────────
    # Same pattern as ReconState and DLQTriageState. Every LLM turn
    # (AIMessage) and tool result (ToolMessage) appends here.
    # operator.add means: accumulate, never overwrite.
    messages: Annotated[list[AnyMessage], operator.add]

    # ── Trigger metadata (set once by entry node, read-only after) ────
    # The Backfill agent is triggered by a backfill_requested event,
    # which could come from the Recon agent's recommendation, a manual
    # request, or the router detecting a recurring failure pattern.
    # No gate_name or source_lane — backfill is incident-driven.
    run_date: str                       # e.g. "2026-09-28"
    trigger_params: dict[str, Any]      # full params from trigger
    correlation_id: str                 # for log tracing

    # ── Investigation loop control ────────────────────────────────────
    # Same as Recon/DLQ. Caps the number of tool calls during
    # investigation. Resets when the plan is rejected and the LLM
    # re-investigates (the outer loop gives fresh inner iterations).
    iteration: int
    max_iterations: int                 # from config: agents.backfill.max_iterations

    # ── Plan output (NEW in Phase 4) ──────────────────────────────────
    # The BackfillPlan is produced by the planning node after the LLM
    # finishes investigation. It's a Pydantic model with:
    #   - incident_summary, root_cause
    #   - affected_partitions, proposed_steps (ordered BackfillStep list)
    #   - estimated_duration_minutes, risk_assessment
    #   - recommended_severity, notifications
    #
    # None until the planning node runs. Set to a new plan on each
    # plan iteration (overwrite semantics — no reducer needed because
    # we always want the latest plan, not a history of plans).
    plan: BackfillPlan | None

    # ── HITL approval fields (NEW in Phase 4) ─────────────────────────
    # These three fields drive the approval gate and rejection loop.
    #
    # approval_status: Where we are in the HITL flow.
    #   - "" (empty): investigation phase, no plan yet
    #   - "pending": plan produced, waiting for human decision
    #   - "approved": human approved, proceed to execution
    #   - "rejected": human rejected, rework with feedback
    #
    # revision_feedback: The human's rejection reason. Injected as a
    #   HumanMessage so the LLM understands what to change. Empty
    #   string when not rejected. This is the KEY to making the
    #   rejection loop useful — without feedback, the LLM would just
    #   regenerate the same plan.
    #
    # plan_iterations / max_plan_iterations: Outer loop safety cap.
    #   plan_iterations increments each time a plan is produced.
    #   When plan_iterations >= max_plan_iterations, the graph stops
    #   even if the human keeps rejecting — prevents infinite loops.
    approval_status: str
    revision_feedback: str
    plan_iterations: int
    max_plan_iterations: int            # from config: agents.backfill.max_plan_iterations

    # ── Execution audit trail (NEW in Phase 4) ────────────────────────
    # After approval, every execution action is logged here:
    #   - Lock acquisition: "LOCK ACQUIRED: booking [silver, gold]"
    #   - Step execution: "STEP 1 SUCCESS: Replay Silver booking_detail ..."
    #   - Lock release: "LOCK RELEASED: booking [silver, gold]"
    #
    # Uses operator.add reducer so execution nodes can append entries
    # across multiple tool calls. Similar to DLQTriageState's
    # requeue_audit but tracking multi-step execution, not single
    # requeue actions.
    execution_audit: Annotated[list[str], operator.add]

    # ── Error accumulator ─────────────────────────────────────────────
    # Same pattern as Recon/DLQ. Nodes append errors without halting.
    errors: Annotated[list[str], operator.add]


# ---------------------------------------------------------------------------
# Initial state factory
# ---------------------------------------------------------------------------

def make_initial_state(
    run_date: str,
    trigger_params: dict[str, Any],
    correlation_id: str,
    max_iterations: int = 15,
    max_plan_iterations: int = 3,
) -> dict[str, Any]:
    """
    Build the seed state dict for graph.invoke().

    Same factory pattern as Recon and DLQ agents, but with additional
    HITL-specific fields initialized to their "not started yet" values.

    Why max_iterations defaults to 15 (vs 10 for Recon/DLQ)?
      The Backfill agent has more investigation tools (5 vs Recon's 6 /
      DLQ's 3) and may need to call several of them with different
      parameters (e.g. assess_data_gaps for both "booking" and "benefit").
      15 iterations gives enough room for thorough investigation without
      being wastefully high.

    Why max_plan_iterations defaults to 3?
      Three attempts at producing an acceptable plan is generous.
      If the LLM can't produce a good plan in 3 tries, the incident
      likely needs manual handling anyway. This prevents:
        - Infinite reject-rework loops burning API credits
        - Human fatigue from reviewing too many plan versions
        - Diminishing returns — LLMs rarely improve after 3 iterations

    Usage in the agent:
        state = make_initial_state(
            run_date=context.run_date,
            trigger_params=context.params,
            correlation_id=context.correlation_id,
            max_iterations=config.get("agents.backfill.max_iterations", 15),
            max_plan_iterations=config.get("agents.backfill.max_plan_iterations", 3),
        )
        result = graph.invoke(state, config={"configurable": {"thread_id": ...}})

    Note the config with thread_id — this is REQUIRED for checkpointing.
    Without a thread_id, the MemorySaver/SqliteSaver can't persist the
    state across the interrupt/resume boundary, and the HITL flow breaks.

    Args:
        run_date: ISO date string from the trigger.
        trigger_params: Full params dict from the trigger.
        correlation_id: Tracing ID for structured logs.
        max_iterations: Safety cap on investigation ReAct loop iterations.
        max_plan_iterations: Safety cap on plan revision loops.

    Returns:
        Dict matching BackfillState's shape, ready for graph.invoke().
    """
    return {
        "messages": [],              # entry node seeds System + Human
        "run_date": run_date,
        "trigger_params": trigger_params,
        "correlation_id": correlation_id,
        "iteration": 0,
        "max_iterations": max_iterations,
        "plan": None,                # planning node will set this
        "approval_status": "",       # empty = not yet at approval stage
        "revision_feedback": "",     # empty = no feedback yet
        "plan_iterations": 0,        # increments each time a plan is produced
        "max_plan_iterations": max_plan_iterations,
        "execution_audit": [],       # tracks Lock → Execute → Release
        "errors": [],
    }
