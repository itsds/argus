"""
LangGraph StateGraph for the Incident & Backfill Planning agent.

WHY THIS FILE MATTERS (learning concepts):

  This is the most complex graph in Argus — and it introduces the most
  powerful LangGraph concepts. While the Recon and DLQ graphs used a
  simple ReAct loop (entry → llm → tools → report → END), the Backfill
  graph has a MULTI-PHASE topology with a HUMAN-IN-THE-LOOP (HITL)
  approval gate in the middle.

NEW LANGGRAPH CONCEPTS IN THIS FILE:

  1. interrupt() — PAUSES the graph execution and returns control to the
     caller. The caller (human) reviews the plan, then resumes with
     Command(resume=...). The graph state is persisted across the pause
     via checkpointing, meaning the process can stop for hours or days
     and resume exactly where it left off.

  2. Command(resume=...) — the caller's way to RESUME a paused graph.
     The resume value is passed directly into the node that called
     interrupt(). For our approval gate:
       Command(resume={"decision": "approved"})
       Command(resume={"decision": "rejected", "feedback": "..."})

  3. Checkpointing (MemorySaver) — LangGraph's persistence layer. Every
     node execution saves a "checkpoint" of the current state. When the
     graph is interrupted, the checkpoint holds the full state. When
     resumed, the graph loads the checkpoint and continues from the
     interrupt point. MemorySaver is the in-memory dev checkpointer;
     production would use SqliteSaver or PostgresSaver.

  4. thread_id — the key that identifies a specific graph execution for
     checkpointing. Each invocation needs a unique thread_id so the
     checkpointer can find the right checkpoint to resume:
       graph.invoke(state, config={"configurable": {"thread_id": "abc123"}})

  5. Multiple ToolNodes — the Recon/DLQ graphs had ONE ToolNode with one
     tool set. The Backfill graph has TWO:
       - investigation_tools: ToolNode(BACKFILL_INVESTIGATION_TOOLS)
       - execution_tools: ToolNode(BACKFILL_EXECUTION_TOOLS)
     Different phases of the graph route to different ToolNodes with
     different tool sets bound to the LLM.

  6. Multiple LLM configurations — three model configs in one graph:
       - investigation model: llm.bind_tools(INVESTIGATION_TOOLS)
       - planning model: llm.with_structured_output(BackfillPlan)
       - execution model: llm.bind_tools(EXECUTION_TOOLS)

  7. Multi-phase graph topology — the graph has FOUR distinct phases:
       Phase 1: Investigation (ReAct loop with investigation tools)
       Phase 2: Planning (structured output → BackfillPlan)
       Phase 3: Approval gate (interrupt + conditional routing)
       Phase 4: Execution (ReAct loop with execution tools)

  8. Rejection loop — when the human rejects, the graph loops BACK to
     planning (via the revise node) instead of ending. The revise node
     injects the human's feedback as a HumanMessage so the LLM knows
     WHAT to change.

THE GRAPH TOPOLOGY:

  ┌──────────────────────────────────────────────────────────────────┐
  │                                                                  │
  │  INVESTIGATION (ReAct loop):                                     │
  │    entry → investigate_llm → should_continue_investigating?      │
  │                                  │              │                │
  │                            (tools)│        (done)│               │
  │                                  ▼              ▼                │
  │                         investigation_tools   plan_node          │
  │                            │                    │                │
  │                            └──► (back to llm)   │                │
  │                                                 ▼                │
  │  APPROVAL LOOP:                                                  │
  │    approval_gate (interrupt) ◄─── revise_node                    │
  │         │              │               ▲                         │
  │    (approved)    (rejected+feedback)    │                        │
  │         │              └───────────────┘                         │
  │         │         (max revisions) ──► END (with error)           │
  │         ▼                                                        │
  │  EXECUTION (ReAct loop):                                         │
  │    execute_llm → should_continue_executing?                      │
  │                       │              │                           │
  │                 (tools)│        (done)│                           │
  │                       ▼              ▼                            │
  │                  execution_tools   END                            │
  │                       │                                          │
  │                       └──► (back to execute_llm)                 │
  │                                                                  │
  └──────────────────────────────────────────────────────────────────┘

COMPARING WITH PREVIOUS GRAPHS:

  Recon graph (Phase 2):
    - 4 nodes (entry, llm, tools, report)
    - 1 ToolNode, 1 LLM config with tools, 1 structured output config
    - Simple ReAct: entry → llm ↔ tools → report → END

  DLQ graph (Phase 3):
    - Same 4-node topology, different components
    - Same ReAct loop, same routing logic

  Backfill graph (Phase 4):
    - 8 nodes (entry, investigate_llm, investigation_tools, plan_node,
               approval_gate, revise_node, execute_llm, execution_tools)
    - 2 ToolNodes, 3 LLM configs (investigation, planning, execution)
    - Two ReAct loops with an approval gate between them
    - interrupt() / Command(resume=...) for HITL
    - Rejection loop for plan revision
    - Checkpointing required for state persistence across interrupt

DESIGN DECISIONS:

  1. WHY NOT reuse the Recon/DLQ ReAct loop helper?
     The Backfill graph has two SEPARATE ReAct loops (investigation and
     execution) with DIFFERENT tools, different prompts, and an approval
     gate between them. Extracting a generic ReAct builder would save
     ~30 lines but add a layer of abstraction that hides how the pieces
     connect. For learning, explicit is better than clever.

  2. WHY separate investigate_llm and execute_llm nodes?
     They bind DIFFERENT tools. The investigation LLM has 5 read-only
     tools; the execution LLM has 3 side-effect tools. Separate nodes
     make it impossible for the LLM to access execution tools during
     investigation — the safety guarantee comes from the graph topology.

  3. WHY does approval_gate handle the resume value directly?
     LangGraph's interrupt() returns the Command(resume=...) value
     directly into the node. The approval_gate reads the decision and
     updates state accordingly. This is simpler than having a separate
     "process_approval" node — one node, one responsibility.

  4. WHY does revise_node loop to plan_node, not back to investigate_llm?
     The investigation is already done — the evidence is in the message
     history. Revision means producing a BETTER plan from the same (or
     slightly expanded) evidence, not re-investigating from scratch.
     The revise node injects the feedback and routes to plan_node, which
     produces a new BackfillPlan using with_structured_output(). If the
     LLM needs more evidence during revision, the planning prompt tells
     it to call additional tools — but the default path is plan revision,
     not full re-investigation.

  5. WHY MemorySaver in build_backfill_graph()?
     The graph MUST have a checkpointer for interrupt() to work. Without
     it, the graph state is lost when interrupt() pauses execution, and
     there's nothing to resume from. MemorySaver is the simplest dev
     checkpointer — production would use SqliteSaver or PostgresSaver.
     The checkpointer is created inside the builder because it's a graph
     concern, not a caller concern.
"""

from __future__ import annotations

import json

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.types import Command, interrupt

from argus.agents.backfill.prompts import (
    BACKFILL_EXECUTION_PROMPT,
    BACKFILL_PLANNING_PROMPT,
    BACKFILL_PROMPT_TEMPLATE,
    BACKFILL_REVISION_TEMPLATE,
)
from argus.agents.backfill.state import BackfillState
from argus.core.config import ArgusConfig
from argus.core.llm import create_llm
from argus.core.logging import get_logger
from argus.schemas.reports import BackfillPlan
from argus.tools.pipeline.backfill_tools import (
    BACKFILL_EXECUTION_TOOLS,
    BACKFILL_INVESTIGATION_TOOLS,
)

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Node factories — Investigation phase
# ---------------------------------------------------------------------------
# The investigation phase is a standard ReAct loop, identical in structure
# to the Recon and DLQ graphs. The LLM calls read-only investigation tools
# to gather evidence about the pipeline incident.


def _make_entry_node():
    """
    Factory for the entry node.

    Seeds the conversation with SystemMessage (investigation strategy +
    pipeline architecture) and HumanMessage (incident details from the
    trigger). Same pattern as Recon/DLQ entry nodes.

    The Backfill entry node uses BACKFILL_PROMPT_TEMPLATE, which has
    BACKFILL_INVESTIGATION_PROMPT as the system message and
    BACKFILL_HUMAN_PROMPT as the human message.
    """

    def entry_node(state: dict) -> dict:
        """Seed the conversation with system prompt and investigation request."""
        params_str = json.dumps(state["trigger_params"], indent=2)

        prompt_value = BACKFILL_PROMPT_TEMPLATE.invoke({
            "run_date": state["run_date"],
            "trigger_params": params_str,
        })

        logger.info(
            "entry_node: seeded conversation",
            extra={
                "run_date": state["run_date"],
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        return {"messages": prompt_value.to_messages()}

    return entry_node


def _make_investigate_llm_node(model_with_investigation_tools):
    """
    Factory for the investigation LLM node (Phase 1 REASON step).

    Same structure as the Recon/DLQ LLM nodes — sends full message history
    to the LLM, increments iteration, returns AIMessage. The key difference
    is the model has INVESTIGATION tools bound (read-only), not execution
    tools.

    Why a separate factory from execute_llm?
      Because they bind DIFFERENT tools. The investigation LLM can call
      get_incident_context, assess_data_gaps, etc. but CANNOT call
      acquire_pipeline_lock or execute_backfill_step. This is the
      graph-level safety guarantee from the two-registry tool separation.

    Args:
        model_with_investigation_tools: LLM with BACKFILL_INVESTIGATION_TOOLS
            bound via .bind_tools(). Can only call read-only tools.
    """

    def investigate_llm_node(state: dict) -> dict:
        """Call the LLM with investigation tools to gather evidence."""
        logger.info(
            "investigate_llm_node: calling LLM (investigation phase)",
            extra={
                "iteration": state["iteration"] + 1,
                "max_iterations": state["max_iterations"],
                "message_count": len(state["messages"]),
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        response = model_with_investigation_tools.invoke(state["messages"])

        if hasattr(response, "tool_calls") and response.tool_calls:
            tool_names = [tc["name"] for tc in response.tool_calls]
            logger.info(
                "investigate_llm_node: LLM requested tool calls",
                extra={
                    "tools": tool_names,
                    "correlation_id": state.get("correlation_id", ""),
                },
            )
        else:
            logger.info(
                "investigate_llm_node: LLM done investigating (no tool calls)",
                extra={"correlation_id": state.get("correlation_id", "")},
            )

        return {
            "messages": [response],
            "iteration": state["iteration"] + 1,
        }

    return investigate_llm_node


# ---------------------------------------------------------------------------
# Router — Investigation phase
# ---------------------------------------------------------------------------

def _should_continue_investigating(state: dict) -> str:
    """
    Decide the next step after the investigation LLM node runs.

    This is the FIRST ReAct loop's control flow. Unlike the Recon/DLQ
    routers that go to "report" when done, this router goes to "plan_node"
    — investigation feeds into planning, not directly into a report.

    Routes:
      1. Max iterations hit? → "plan_node" (force plan with what we have)
      2. LLM emitted tool_calls? → "investigation_tools" (execute them)
      3. No tool calls? → "plan_node" (investigation complete, produce plan)

    Returns:
        "investigation_tools" or "plan_node"
    """
    messages = state["messages"]
    iteration = state["iteration"]
    max_iterations = state["max_iterations"]

    if iteration >= max_iterations:
        logger.warning(
            "should_continue_investigating: max iterations reached, "
            "forcing plan generation",
            extra={
                "iteration": iteration,
                "max_iterations": max_iterations,
                "correlation_id": state.get("correlation_id", ""),
            },
        )
        return "plan_node"

    last_message = messages[-1]

    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        return "investigation_tools"

    # No tool calls — the LLM is done investigating, move to planning
    return "plan_node"


# ---------------------------------------------------------------------------
# Node factories — Planning phase
# ---------------------------------------------------------------------------
# The planning phase produces a structured BackfillPlan from the
# investigation evidence. It uses .with_structured_output() — the same
# mechanism the Recon/DLQ report nodes use, but for an INTERMEDIATE
# output that goes to the human for approval, not directly to END.


def _make_plan_node(model):
    """
    Factory for the planning node.

    This node takes the FULL conversation history (all investigation
    evidence) and produces a structured BackfillPlan using
    .with_structured_output(BackfillPlan).

    How it works:
      1. Swap the system prompt to BACKFILL_PLANNING_PROMPT (the rubric
         for what makes a good plan)
      2. Add a human instruction asking for the plan
      3. Use .with_structured_output(BackfillPlan) to get a Pydantic object
      4. Store the plan in state and set approval_status = "pending"

    Why swap the system prompt?
      The investigation phase used BACKFILL_INVESTIGATION_PROMPT which
      tells the LLM HOW to investigate. The planning phase needs
      BACKFILL_PLANNING_PROMPT which tells the LLM WHAT MAKES A GOOD PLAN
      (field semantics, ordering rules, quality rubric). Different phase,
      different instructions.

    Why increment plan_iterations?
      Each time this node runs, it produces a new plan (either the first
      plan or a revision after rejection). The plan_iterations counter
      tracks how many plans have been produced so the approval_gate can
      enforce the max_plan_iterations cap.

    Args:
        model: The base LLM (without tools or structured output bound).
            The factory applies .with_structured_output(BackfillPlan).
    """
    plan_model = model.with_structured_output(BackfillPlan)

    def plan_node(state: dict) -> dict:
        """Produce a structured BackfillPlan from investigation evidence."""
        new_plan_iteration = state["plan_iterations"] + 1
        logger.info(
            "plan_node: generating backfill plan",
            extra={
                "plan_iteration": new_plan_iteration,
                "max_plan_iterations": state["max_plan_iterations"],
                "message_count": len(state["messages"]),
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        # Build the messages for the planning model:
        # 1. Replace the system prompt with the planning rubric
        # 2. Keep all non-system messages from the conversation (evidence)
        # 3. Add a human instruction to produce the plan
        planning_messages = [SystemMessage(content=BACKFILL_PLANNING_PROMPT)]

        # Copy all non-system messages from the conversation — these are
        # the investigation evidence (HumanMessage with trigger info,
        # AIMessages with reasoning, ToolMessages with results).
        for msg in state["messages"]:
            if not isinstance(msg, SystemMessage):
                planning_messages.append(msg)

        # Add the instruction to produce the plan
        planning_messages.append(HumanMessage(
            content=(
                "Based on your investigation findings above, produce a "
                "complete BackfillPlan. Follow the rubric in the system "
                "message for each field. Make sure proposed_steps are "
                "ordered upstream-first and reference real snapshot IDs "
                "from your investigation."
            )
        ))

        try:
            plan = plan_model.invoke(planning_messages)
            logger.info(
                "plan_node: plan generated successfully",
                extra={
                    "step_count": len(plan.proposed_steps),
                    "severity": plan.recommended_severity.value,
                    "estimated_minutes": plan.estimated_duration_minutes,
                    "plan_iteration": new_plan_iteration,
                    "correlation_id": state.get("correlation_id", ""),
                },
            )

            # Store the plan as an AIMessage so the LLM can see it in
            # the conversation history if the plan gets rejected and
            # needs revision.
            plan_summary = (
                f"BACKFILL PLAN (attempt {new_plan_iteration}):\n"
                f"Incident: {plan.incident_summary}\n"
                f"Root cause: {plan.root_cause}\n"
                f"Affected partitions: {plan.affected_partitions}\n"
                f"Steps ({len(plan.proposed_steps)}):\n"
            )
            for step in plan.proposed_steps:
                plan_summary += (
                    f"  {step.order}. {step.description} "
                    f"(lock={step.requires_lock})\n"
                )
            plan_summary += (
                f"Estimated duration: {plan.estimated_duration_minutes} min\n"
                f"Risk: {plan.risk_assessment}\n"
                f"Severity: {plan.recommended_severity.value}"
            )

            return {
                "messages": [AIMessage(content=plan_summary)],
                "plan": plan,
                "approval_status": "pending",
                "plan_iterations": new_plan_iteration,
            }

        except Exception as exc:
            error_msg = f"plan_node: structured output failed: {exc}"
            logger.error(
                error_msg,
                extra={"correlation_id": state.get("correlation_id", "")},
            )
            return {
                "errors": [error_msg],
                "plan_iterations": new_plan_iteration,
            }

    return plan_node


# ---------------------------------------------------------------------------
# Node factories — Approval gate (HITL interrupt/resume)
# ---------------------------------------------------------------------------
# This is the heart of the HITL pattern. The approval_gate node PAUSES
# the graph using interrupt(), waits for the human to review the plan,
# and resumes with their decision.


def _make_approval_gate():
    """
    Factory for the approval gate node.

    This node introduces the two KEY HITL concepts:

    1. interrupt(value) — pauses graph execution and returns `value`
       to the caller. The graph state is saved to the checkpointer.
       The caller sees the interrupt value (the plan for review) and
       can inspect it before deciding.

    2. Command(resume=value) — the caller resumes the graph by passing
       a value back into the interrupted node. The interrupt() call
       RETURNS this value, and the node continues executing with it.

    The flow:
      a) plan_node produces BackfillPlan → sets approval_status="pending"
      b) approval_gate runs:
         - Calls interrupt(plan_dict) → graph PAUSES
         - Human reviews the plan
         - Human calls: graph.invoke(
               Command(resume={"decision": "approved"}),
               config={"configurable": {"thread_id": "..."}}
           )
         - interrupt() RETURNS {"decision": "approved"}
         - approval_gate reads the decision, sets approval_status
      c) Conditional edge routes based on approval_status:
         - "approved" → execute_llm
         - "rejected" → revise_node
         - "max_revisions" → END

    Why interrupt(plan_dict) instead of interrupt()?
      The value passed to interrupt() is what the caller sees when the
      graph pauses. By passing the plan as a dict, the caller gets the
      full plan for review without having to inspect the graph state.
      This is a UX convenience — the caller can display the plan to the
      human directly from the interrupt return value.

    Why check plan_iterations in this node?
      The max_plan_iterations cap needs to be checked BEFORE showing the
      plan to the human. If we've hit the limit after rejection, there's
      no point asking for another review — the graph should stop.
      Checking here (rather than in the router) keeps the logic close
      to the interrupt.
    """

    def approval_gate(state: dict) -> dict:
        """Pause for human approval of the backfill plan."""
        plan = state.get("plan")
        plan_iterations = state["plan_iterations"]
        max_plan_iterations = state["max_plan_iterations"]

        # Safety check: if no plan was produced (e.g. structured output
        # failed), we can't ask for approval. End with error.
        if plan is None:
            logger.error(
                "approval_gate: no plan available for approval",
                extra={"correlation_id": state.get("correlation_id", "")},
            )
            return {
                "approval_status": "error",
                "errors": ["No plan was produced — cannot request approval"],
            }

        logger.info(
            "approval_gate: presenting plan for human approval",
            extra={
                "plan_iteration": plan_iterations,
                "max_plan_iterations": max_plan_iterations,
                "step_count": len(plan.proposed_steps),
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        # ── THE INTERRUPT ────────────────────────────────────────
        # This is where the magic happens. interrupt() does THREE things:
        #
        #   1. SAVES the current graph state to the checkpointer
        #   2. PAUSES execution and returns the value to the caller
        #   3. When resumed via Command(resume=...), RETURNS the
        #      resume value right here, as if interrupt() was a
        #      normal function that just took a while to return
        #
        # The plan is passed as a dict so the caller can display it.
        # model_dump() converts the Pydantic BackfillPlan to a dict.
        human_decision = interrupt(plan.model_dump())

        # ── AFTER RESUME ─────────────────────────────────────────
        # If we reach this line, the human has resumed the graph.
        # human_decision is whatever was passed in Command(resume=...).
        #
        # Expected format:
        #   {"decision": "approved"}
        #   {"decision": "rejected", "feedback": "Need to add..."}

        decision = human_decision.get("decision", "rejected")
        feedback = human_decision.get("feedback", "")

        logger.info(
            "approval_gate: human decision received",
            extra={
                "decision": decision,
                "has_feedback": bool(feedback),
                "plan_iteration": plan_iterations,
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        if decision == "approved":
            return {
                "approval_status": "approved",
                "messages": [HumanMessage(
                    content="Plan APPROVED. Proceed with execution."
                )],
            }
        else:
            # Rejected — check if we've hit the revision cap
            if plan_iterations >= max_plan_iterations:
                logger.warning(
                    "approval_gate: max plan iterations reached after "
                    "rejection, stopping",
                    extra={
                        "plan_iterations": plan_iterations,
                        "max_plan_iterations": max_plan_iterations,
                        "correlation_id": state.get("correlation_id", ""),
                    },
                )
                return {
                    "approval_status": "max_revisions",
                    "revision_feedback": feedback,
                    "errors": [
                        f"Plan rejected {plan_iterations} times "
                        f"(max {max_plan_iterations}). "
                        f"Last feedback: {feedback}"
                    ],
                }

            return {
                "approval_status": "rejected",
                "revision_feedback": feedback,
            }

    return approval_gate


# ---------------------------------------------------------------------------
# Router — Approval gate
# ---------------------------------------------------------------------------

def _route_after_approval(state: dict) -> str:
    """
    Route based on the human's approval decision.

    This conditional edge runs AFTER the approval_gate node. It reads
    approval_status (set by the approval_gate) and routes:

      "approved"      → "execute_llm"    (proceed to execution)
      "rejected"      → "revise_node"    (inject feedback, redo plan)
      "max_revisions" → END              (too many rejections, stop)
      "error"         → END              (no plan produced)

    Returns:
        Node name string for the next node.
    """
    status = state.get("approval_status", "error")

    if status == "approved":
        return "execute_llm"
    elif status == "rejected":
        return "revise_node"
    else:
        # "max_revisions" or "error" — end the graph
        return END


# ---------------------------------------------------------------------------
# Node factories — Revision (rejection loop)
# ---------------------------------------------------------------------------

def _make_revise_node():
    """
    Factory for the revision node.

    When the human rejects a plan, this node:
      1. Injects the human's feedback as a HumanMessage
      2. Resets the investigation iteration counter (gives the LLM
         fresh tool-call budget for additional investigation)
      3. Routes back to plan_node to produce a revised plan

    Why inject feedback as a HumanMessage?
      The LLM needs to SEE the feedback in its conversation history.
      A HumanMessage is the natural way — it appears as "the human said
      this" in the message sequence. The LLM reads its investigation
      evidence + the rejected plan + this feedback and produces a
      better plan.

    Why use BACKFILL_REVISION_TEMPLATE?
      The template adds framing: "Your plan was REJECTED. Attempt N of M."
      This gives the LLM context (rejection, not just new instructions)
      and urgency (iteration count). Without the template, the feedback
      would lack context.

    Why reset iteration counter?
      Each plan revision might need additional tool calls (e.g., the
      human says "check the backfill history for last month too"). The
      inner loop (investigation iterations) resets so the LLM has a
      fresh budget. The outer loop (plan_iterations) does NOT reset —
      it tracks total plans produced across all revisions.
    """

    def revise_node(state: dict) -> dict:
        """Inject rejection feedback and prepare for plan revision."""
        feedback = state.get("revision_feedback", "No specific feedback.")
        plan_iterations = state["plan_iterations"]
        max_plan_iterations = state["max_plan_iterations"]

        logger.info(
            "revise_node: injecting rejection feedback",
            extra={
                "plan_iteration": plan_iterations,
                "max_plan_iterations": max_plan_iterations,
                "feedback_length": len(feedback),
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        # Render the revision template with the feedback and iteration info
        revision_value = BACKFILL_REVISION_TEMPLATE.invoke({
            "plan_iterations": str(plan_iterations),
            "max_plan_iterations": str(max_plan_iterations),
            "revision_feedback": feedback,
        })

        return {
            "messages": revision_value.to_messages(),
            # Reset the investigation iteration counter so the LLM
            # has a fresh tool-call budget for any additional
            # investigation the revision might need.
            "iteration": 0,
        }

    return revise_node


# ---------------------------------------------------------------------------
# Node factories — Execution phase
# ---------------------------------------------------------------------------
# The execution phase is a SECOND ReAct loop, structurally similar to
# the investigation loop but with EXECUTION tools instead of investigation
# tools. It runs ONLY after human approval.


def _make_execute_llm_node(model_with_execution_tools):
    """
    Factory for the execution LLM node (Phase 2 REASON step).

    This node is the REASON step of the EXECUTION ReAct loop. It's
    structurally identical to investigate_llm_node but binds EXECUTION
    tools (acquire_pipeline_lock, execute_backfill_step,
    release_pipeline_lock) instead of investigation tools.

    The FIRST call to this node swaps the system prompt to
    BACKFILL_EXECUTION_PROMPT, which tells the LLM the safety rules
    for execution (Lock → Execute → Release pattern, stop on failure).

    Why a separate LLM node instead of reusing investigate_llm_node?
      1. DIFFERENT tools bound — execution tools have side effects
      2. DIFFERENT system prompt — execution safety rules
      3. DIFFERENT iteration counter semantics — execution iterations
         track execution steps, not investigation steps
      4. Clarity — when reading the graph, you see "execute_llm" and
         immediately know which phase you're in

    Args:
        model_with_execution_tools: LLM with BACKFILL_EXECUTION_TOOLS
            bound via .bind_tools(). Can call lock/execute/release tools.
    """

    def execute_llm_node(state: dict) -> dict:
        """Call the LLM with execution tools to carry out the plan."""
        # On the FIRST execution call, inject the execution system prompt
        # and a human message summarizing the approved plan.
        # We detect "first call" by checking if execution_audit is empty.
        messages = list(state["messages"])

        if not state.get("execution_audit"):
            # First execution call — inject execution context
            messages.append(SystemMessage(content=BACKFILL_EXECUTION_PROMPT))
            plan = state.get("plan")
            if plan:
                exec_instruction = (
                    "The plan has been APPROVED. Execute it now using "
                    "the Lock → Execute → Release pattern.\n\n"
                    "Approved plan steps:\n"
                )
                for step in plan.proposed_steps:
                    exec_instruction += (
                        f"  {step.order}. {step.description}\n"
                    )
                exec_instruction += (
                    f"\nEntity to lock: check the plan's affected partitions. "
                    f"Execute each step in order. Release the lock when done."
                )
                messages.append(HumanMessage(content=exec_instruction))

        logger.info(
            "execute_llm_node: calling LLM (execution phase)",
            extra={
                "iteration": state["iteration"] + 1,
                "message_count": len(messages),
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        response = model_with_execution_tools.invoke(messages)

        if hasattr(response, "tool_calls") and response.tool_calls:
            tool_names = [tc["name"] for tc in response.tool_calls]
            logger.info(
                "execute_llm_node: LLM requested execution tool calls",
                extra={
                    "tools": tool_names,
                    "correlation_id": state.get("correlation_id", ""),
                },
            )
        else:
            logger.info(
                "execute_llm_node: LLM done executing (no tool calls)",
                extra={"correlation_id": state.get("correlation_id", "")},
            )

        return {
            "messages": [response],
            "iteration": state["iteration"] + 1,
        }

    return execute_llm_node


# ---------------------------------------------------------------------------
# Router — Execution phase
# ---------------------------------------------------------------------------

def _should_continue_executing(state: dict) -> str:
    """
    Decide the next step after the execution LLM node runs.

    This is the SECOND ReAct loop's control flow. When the LLM is done
    executing (no more tool calls), the graph ends — there's no separate
    report node because the execution audit trail IS the report output
    (the agent.py layer packages it).

    Routes:
      1. Max iterations hit? → END (safety valve)
      2. LLM emitted tool_calls? → "execution_tools" (execute them)
      3. No tool calls? → END (execution complete)

    Returns:
        "execution_tools" or END
    """
    messages = state["messages"]
    iteration = state["iteration"]
    max_iterations = state["max_iterations"]

    if iteration >= max_iterations:
        logger.warning(
            "should_continue_executing: max iterations reached during "
            "execution, forcing end",
            extra={
                "iteration": iteration,
                "max_iterations": max_iterations,
                "correlation_id": state.get("correlation_id", ""),
            },
        )
        return END

    last_message = messages[-1]

    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        return "execution_tools"

    return END


# ---------------------------------------------------------------------------
# Graph builder — the public API
# ---------------------------------------------------------------------------

def build_backfill_graph(config: ArgusConfig):
    """
    Build and compile the Incident & Backfill Planning StateGraph.

    This is the assembly function — it takes config, creates the LLM,
    builds ALL nodes for ALL phases, wires the edges (including the
    HITL approval loop), adds a checkpointer, and returns a compiled
    graph ready for invoke with initial state.

    The compiled graph is a LangGraph Runnable. You call it with:

        # First invocation — runs investigation + planning, then pauses
        result = graph.invoke(
            initial_state,
            config={"configurable": {"thread_id": "incident-2026-09-28"}}
        )
        # result contains the interrupt value (the plan for review)

        # Resume after approval — runs execution phase
        result = graph.invoke(
            Command(resume={"decision": "approved"}),
            config={"configurable": {"thread_id": "incident-2026-09-28"}}
        )
        # result contains the final state with execution_audit

        # Or resume with rejection — loops back for revision
        result = graph.invoke(
            Command(resume={"decision": "rejected", "feedback": "..."}),
            config={"configurable": {"thread_id": "incident-2026-09-28"}}
        )
        # result contains the interrupt value (revised plan for review)

    IMPORTANT: The thread_id MUST be the same across invoke calls for
    the same incident. This is how the checkpointer finds the right
    checkpoint to resume from.

    Architecture:

      entry → investigate_llm ↔ investigation_tools → plan_node
              → approval_gate ↔ revise_node → plan_node
              → execute_llm ↔ execution_tools → END

    Args:
        config: ArgusConfig with LLM settings and agent parameters.

    Returns:
        A compiled LangGraph StateGraph (CompiledStateGraph) with
        MemorySaver checkpointer for HITL interrupt/resume support.

    Usage:
        config = load_config("dev")
        graph = build_backfill_graph(config)

        state = make_initial_state(
            run_date="2026-09-28",
            trigger_params={...},
            correlation_id="corr-123",
        )

        # Phase 1+2: investigate + plan → pauses at approval
        result = graph.invoke(
            state,
            config={"configurable": {"thread_id": "incident-2026-09-28"}}
        )

        # Phase 3: human approves
        result = graph.invoke(
            Command(resume={"decision": "approved"}),
            config={"configurable": {"thread_id": "incident-2026-09-28"}}
        )
    """
    # --- Step 1: Create the LLM from config ---
    llm = create_llm(config)

    # --- Step 2: Create THREE model configurations ---
    #
    # investigation_model: 5 read-only tools bound. Used during Phase 1
    #   (the investigation ReAct loop). The LLM can call
    #   get_incident_context, assess_data_gaps, etc.
    #
    # plan_model: .with_structured_output(BackfillPlan). Used in the
    #   plan_node to produce a validated Pydantic BackfillPlan object.
    #   Created inside _make_plan_node — listed here for documentation.
    #
    # execution_model: 3 side-effect tools bound. Used during Phase 4
    #   (the execution ReAct loop). The LLM can call
    #   acquire_pipeline_lock, execute_backfill_step, release_pipeline_lock.
    investigation_model = llm.bind_tools(BACKFILL_INVESTIGATION_TOOLS)
    execution_model = llm.bind_tools(BACKFILL_EXECUTION_TOOLS)

    logger.info(
        "build_backfill_graph: building graph",
        extra={
            "provider": config.llm.get("provider", "google"),
            "model": config.llm.get("model", "unknown"),
            "investigation_tool_count": len(BACKFILL_INVESTIGATION_TOOLS),
            "execution_tool_count": len(BACKFILL_EXECUTION_TOOLS),
            "investigation_tools": [
                t.name for t in BACKFILL_INVESTIGATION_TOOLS
            ],
            "execution_tools": [
                t.name for t in BACKFILL_EXECUTION_TOOLS
            ],
        },
    )

    # --- Step 3: Create TWO tool nodes ---
    # One for each phase. LangGraph's ToolNode automatically matches
    # tool_calls from the AIMessage to the tools in its list.
    investigation_tool_node = ToolNode(BACKFILL_INVESTIGATION_TOOLS)
    execution_tool_node = ToolNode(BACKFILL_EXECUTION_TOOLS)

    # --- Step 4: Build the StateGraph ---
    graph = StateGraph(BackfillState)

    # --- Step 5: Add ALL nodes ---
    #
    # Investigation phase (Phase 1):
    graph.add_node("entry", _make_entry_node())
    graph.add_node("investigate_llm", _make_investigate_llm_node(
        investigation_model
    ))
    graph.add_node("investigation_tools", investigation_tool_node)
    #
    # Planning phase (Phase 2):
    graph.add_node("plan_node", _make_plan_node(llm))
    #
    # Approval gate (Phase 3 — HITL):
    graph.add_node("approval_gate", _make_approval_gate())
    graph.add_node("revise_node", _make_revise_node())
    #
    # Execution phase (Phase 4):
    graph.add_node("execute_llm", _make_execute_llm_node(execution_model))
    graph.add_node("execution_tools", execution_tool_node)

    # --- Step 6: Wire edges ---
    #
    # INVESTIGATION PHASE edges:
    graph.set_entry_point("entry")
    graph.add_edge("entry", "investigate_llm")
    graph.add_conditional_edges(
        "investigate_llm",
        _should_continue_investigating,
        {
            "investigation_tools": "investigation_tools",
            "plan_node": "plan_node",
        },
    )
    graph.add_edge("investigation_tools", "investigate_llm")

    # PLANNING → APPROVAL edges:
    graph.add_edge("plan_node", "approval_gate")

    # APPROVAL GATE → conditional routing:
    graph.add_conditional_edges(
        "approval_gate",
        _route_after_approval,
        {
            "execute_llm": "execute_llm",
            "revise_node": "revise_node",
            END: END,
        },
    )

    # REJECTION LOOP edge:
    # revise_node → plan_node (not back to investigate_llm)
    graph.add_edge("revise_node", "plan_node")

    # EXECUTION PHASE edges:
    graph.add_conditional_edges(
        "execute_llm",
        _should_continue_executing,
        {
            "execution_tools": "execution_tools",
            END: END,
        },
    )
    graph.add_edge("execution_tools", "execute_llm")

    # --- Step 7: Compile with checkpointer ---
    #
    # The checkpointer is REQUIRED for interrupt() to work. Without it,
    # the graph state is lost when interrupt() pauses execution.
    #
    # MemorySaver stores checkpoints in memory — perfect for dev/testing.
    # Production would use SqliteSaver or PostgresSaver for durability.
    #
    # Import here to keep it close to where it's used and to make the
    # dependency on checkpointing explicit.
    from langgraph.checkpoint.memory import MemorySaver

    checkpointer = MemorySaver()
    compiled = graph.compile(checkpointer=checkpointer)

    logger.info(
        "build_backfill_graph: graph compiled with MemorySaver checkpointer"
    )

    return compiled
