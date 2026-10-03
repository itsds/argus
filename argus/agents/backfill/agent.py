"""
Incident & Backfill Planning agent — the public interface.

WHY THIS FILE MATTERS (learning concepts):

  This is the ADAPTER between the Backfill agent's internals (LangGraph,
  tools, prompts, HITL interrupt/resume) and the Argus platform contract
  (BaseAgent interface).

  Comparing with ReconciliationAgent (Phase 2) and DLQTriageAgent (Phase 3),
  this file introduces a FUNDAMENTALLY DIFFERENT invocation pattern:

WHAT'S NEW — TWO-PHASE INVOKE:

  Recon and DLQ agents have a SINGLE invoke():
    invoke() → graph runs to completion → AgentResult(status="success")

  The Backfill agent has a TWO-PHASE lifecycle:
    invoke()  → graph runs to interrupt  → AgentResult(status="needs_approval")
    resume()  → graph resumes from checkpoint → AgentResult(status="success")

  This happens because the Backfill graph uses interrupt() for human-in-the-loop
  approval. The graph PAUSES when it has a plan ready for review. The caller must:
    1. Call invoke()        — gets back a plan for review
    2. Present plan to human
    3. Call resume(decision, feedback) — either executes or revises

  The resume() might return "needs_approval" AGAIN if the human rejected and
  the agent produced a revised plan. This creates a loop:
    invoke() → needs_approval
           → resume(rejected, feedback) → needs_approval (revised plan)
           → resume(approved) → success

KEY CONCEPTS IN THIS FILE:

  1. Two-phase invoke pattern — invoke() for initial run, resume() for
     continuation. The platform (router, CLI, API) calls invoke() first,
     then resume() after the human reviews the plan. This is the agent-
     level expression of the HITL pattern that graph.py implements with
     interrupt() and Command(resume=...).

  2. Thread ID management — interrupt/resume requires a checkpoint
     thread_id that links the two graph.invoke() calls. This agent
     generates it from the correlation_id and stores it as an instance
     variable across the invoke/resume boundary. Without the same
     thread_id, the resumed graph can't find its checkpointed state.

  3. Interrupt detection — after graph.invoke() returns, we call
     graph.get_state(config) and check graph_state.next to determine
     if the graph is interrupted (waiting for approval) or completed.
     graph_state.next lists nodes pending execution — if non-empty,
     the graph is paused at an interrupt point.

  4. Instance state across phases — unlike Recon/DLQ which are stateless
     between invocations, the Backfill agent stores _thread_id, _context,
     and _started_at because resume() needs them to package the final
     AgentResult. This makes the agent STATEFUL between invoke/resume.

  5. Command(resume=...) — the LangGraph primitive that delivers the
     human's decision back into the paused approval_gate node. It's
     passed as the input to graph.invoke() on the resume call.

  6. Shared result processing — _process_graph_result() is used by both
     invoke() and resume() because both need the same detection logic:
     "is the graph interrupted or completed?"

DESIGN DECISIONS:

  - Why a separate resume() method instead of overloading invoke()?
    Because the semantics are different. invoke() starts a NEW
    investigation from a TriggerContext; resume() continues an EXISTING
    one. Overloading invoke() with an optional "decision" parameter
    would conflate "start new" with "continue existing" — separate
    methods make the lifecycle explicit in the API.

  - Why store context on self instead of requiring the caller to pass
    it again?
    The resume() call needs the original TriggerContext and started_at
    timestamp to build the final AgentResult. Requiring the caller to
    store and re-pass them would leak internal agent concerns to the
    platform. The agent manages its own lifecycle state.

  - Why check graph_state.next instead of approval_status?
    Because graph_state.next is the authoritative signal from LangGraph
    about whether the graph is paused or completed. Checking the
    approval_status field would couple this adapter to the exact state
    machine inside graph.py. graph_state.next is a graph-engine-level
    signal that works regardless of how the graph's internal routing
    changes.

  - Why can resume() also return "needs_approval"?
    Because the rejection loop means the graph can interrupt MULTIPLE
    times. Human rejects → agent revises → produces new plan →
    interrupt again. The caller must be prepared for this loop until
    either the human approves or max_plan_iterations is reached (which
    causes the graph to END with errors → status="failure").
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from langchain_core.messages import AIMessage
from langgraph.types import Command

from argus.agents.base import AgentResult, BaseAgent, TriggerContext
from argus.agents.backfill.graph import build_backfill_graph
from argus.agents.backfill.state import make_initial_state
from argus.core.config import ArgusConfig
from argus.core.logging import get_logger

logger = get_logger(__name__)


class BackfillAgent(BaseAgent):
    """
    Incident & Backfill Planning agent.

    Investigates pipeline incidents, produces a structured BackfillPlan
    with human-in-the-loop approval, and executes the approved plan using
    pipeline execution tools (lock, execute steps, release).

    Unlike Recon and DLQ agents which complete in a single invoke() call,
    this agent uses a TWO-PHASE lifecycle:

      Phase 1 — invoke(context):
        Investigation → Planning → interrupt (pauses for human approval)
        Returns AgentResult(status="needs_approval", report=plan_dict)

      Phase 2 — resume(decision, feedback):
        If approved:  Execution → END → AgentResult(status="success")
        If rejected:  Revision → Re-plan → interrupt again
                      → AgentResult(status="needs_approval")

    Usage:
        config = load_config("dev")
        agent = BackfillAgent(config)
        context = TriggerContext(
            agent_name="backfill",
            trigger_source="cli",
            run_date="2026-09-28",
            params={
                "incident_id": "INC-2026-0928",
                "dag_id": "ttag_main",
                "error": "Gate 3 failure — 847 missing records",
            },
        )

        # Phase 1: investigate and plan
        result = await agent.invoke(context)
        assert result.status == "needs_approval"
        plan = result.report  # the BackfillPlan as a dict

        # Present plan to human ... human approves
        result = await agent.resume(decision="approved")
        assert result.status == "success"
        print(result.report["execution_audit"])

        # OR: human rejects with feedback
        result = await agent.resume(
            decision="rejected",
            feedback="Add rollback steps for each partition",
        )
        # result.status is "needs_approval" again (revised plan)
        # Call resume() again with the new decision
    """

    def __init__(self, config: ArgusConfig):
        super().__init__(config)

        # ── Instance state for two-phase lifecycle ────────────────
        # These persist across the invoke() → resume() boundary.
        # Recon/DLQ agents don't need these because they complete
        # in a single invoke() call with no interrupts.
        self._thread_id: str | None = None
        self._context: TriggerContext | None = None
        self._started_at: datetime | None = None

    # ── Abstract property implementations ──────────────────────────────

    @property
    def name(self) -> str:
        """Agent identifier used in routing, logging, and AgentResult."""
        return "backfill"

    @property
    def description(self) -> str:
        """One-line description for logging and routing decisions."""
        return (
            "Investigates incidents, produces backfill plans with human "
            "approval, and executes approved plans safely"
        )

    # ── Graph building ─────────────────────────────────────────────────

    def build_graph(self):
        """
        Construct the Backfill LangGraph StateGraph.

        Delegates to build_backfill_graph() which handles all the wiring:
        8 nodes, 2 ToolNodes, 3 LLM configs, MemorySaver checkpointer.

        The returned graph includes a MemorySaver checkpointer — this is
        REQUIRED for interrupt/resume to work. Without a checkpointer,
        the state would be lost when the graph pauses at the approval gate.

        Returns:
            A compiled LangGraph StateGraph (CompiledStateGraph) with
            MemorySaver checkpointer already attached.
        """
        return build_backfill_graph(self.config)

    # ── Phase 1: Initial invocation ────────────────────────────────────

    async def invoke(self, context: TriggerContext) -> AgentResult:
        """
        Execute Phase 1: investigation and planning (up to approval gate).

        This runs the graph from entry through investigation, planning,
        and up to the approval gate where interrupt() pauses execution.

        The returned AgentResult will have:
          - status="needs_approval" — plan is ready for human review
          - report=plan dict — the BackfillPlan serialized as a dict
          - actions_taken — tools called during investigation

        After receiving this result, the caller should:
          1. Present result.report (the plan) to a human
          2. Call resume(decision, feedback) with the human's decision

        Args:
            context: TriggerContext from the router (CLI, Airflow, API).

        Returns:
            AgentResult with status "needs_approval" (normal path) or
            "failure" (if graph crashes before reaching the approval gate).
        """
        self._started_at = datetime.now(timezone.utc)
        self._context = context

        logger.info(
            "BackfillAgent.invoke: starting Phase 1 (investigate + plan)",
            extra={
                "run_date": context.run_date,
                "correlation_id": context.correlation_id,
                "trigger_source": context.trigger_source,
                "params": context.params,
            },
        )

        # ── Step 1: Lazy graph compilation ─────────────────────────
        # Same pattern as Recon/DLQ: build once, reuse. The compiled
        # graph is stateless — state lives in the checkpointer keyed
        # by thread_id, not in the graph object.
        if self._graph is None:
            logger.info(
                "BackfillAgent.invoke: building graph (first call)",
                extra={"correlation_id": context.correlation_id},
            )
            self._graph = self.build_graph()

        # ── Step 2: Generate thread_id for checkpointing ──────────
        # thread_id is the KEY concept linking invoke() and resume().
        # It tells the MemorySaver checkpointer which checkpoint to
        # load when the graph resumes after interrupt.
        #
        # We derive it from correlation_id when available so the same
        # incident maps to the same checkpoint thread. Falls back to
        # a random UUID for ad-hoc runs (CLI testing, etc.).
        #
        # Why not just use a UUID every time? Because correlation_id
        # provides traceability — you can look up the checkpoint by
        # incident ID, not just a random string.
        self._thread_id = (
            f"backfill-{context.correlation_id}"
            if context.correlation_id
            else f"backfill-{uuid4().hex[:12]}"
        )
        config = {"configurable": {"thread_id": self._thread_id}}

        # ── Step 3: Translate TriggerContext → initial state ───────
        # Unlike Recon (gate_name) and DLQ (source_lane), the Backfill
        # agent uses the full trigger_params dict because incidents can
        # come from multiple sources with varying metadata.
        max_iterations = self.config.get(
            "agents.backfill.max_iterations", 15
        )
        max_plan_iterations = self.config.get(
            "agents.backfill.max_plan_iterations", 3
        )

        initial_state = make_initial_state(
            run_date=context.run_date,
            trigger_params=context.params,
            correlation_id=context.correlation_id,
            max_iterations=max_iterations,
            max_plan_iterations=max_plan_iterations,
        )

        # ── Step 4: Run the graph (expect interrupt at approval) ───
        # This is where the Backfill agent diverges from Recon/DLQ:
        #
        # Recon/DLQ:  graph.invoke() → runs to END → returns final state
        # Backfill:   graph.invoke() → runs to interrupt → returns state
        #             at the point of interruption
        #
        # The graph should hit interrupt() at the approval_gate node
        # after the plan_node produces a BackfillPlan. The returned
        # state will contain the plan in state["plan"].
        #
        # We pass config with thread_id so the MemorySaver checkpointer
        # saves the interrupted state under that thread. The resume()
        # call will load it back using the same thread_id.
        try:
            result_state = self._graph.invoke(initial_state, config=config)
        except Exception as exc:
            logger.error(
                "BackfillAgent.invoke: graph execution failed",
                extra={
                    "error": str(exc),
                    "correlation_id": context.correlation_id,
                },
            )
            self._clear_phase_state()
            return self._make_result(
                context=context,
                status="failure",
                report={"error": str(exc)},
                started_at=self._started_at,
                errors=[f"Graph execution failed: {exc}"],
            )

        # ── Step 5: Detect interrupt vs unexpected completion ──────
        # _process_graph_result() checks graph.get_state() to determine
        # if the graph is paused (needs_approval) or completed.
        return self._process_graph_result(result_state, config)

    # ── Phase 2: Resume after human review ─────────────────────────────

    async def resume(
        self,
        decision: str,
        feedback: str = "",
    ) -> AgentResult:
        """
        Resume the Backfill agent after human plan review.

        Delivers the human's decision back into the paused approval_gate
        node via Command(resume=...). The graph then either:

          - decision="approved" → executes the plan → returns success
          - decision="rejected" + feedback → revises plan → might
            interrupt again (needs_approval) or fail (max revisions)

        IMPORTANT: resume() can return "needs_approval" AGAIN if the
        human rejected and the agent produced a revised plan. The caller
        must handle this loop:

            while result.status == "needs_approval":
                plan = result.report
                decision = present_to_human(plan)
                result = await agent.resume(decision, feedback)

        Args:
            decision: "approved" or "rejected".
            feedback: Human's rejection reason (used when rejected;
                      ignored when approved). The graph injects this as
                      a HumanMessage so the LLM knows what to change.

        Returns:
            AgentResult with status:
              - "needs_approval" — revised plan ready for review (loop)
              - "success" — execution completed after approval
              - "failure" — error during execution or max revisions hit

        Raises:
            ValueError: If no invoke() was called first (no pending
                        approval to resume).
        """
        if not self._thread_id or not self._context:
            raise ValueError(
                "No pending approval to resume. Call invoke() first."
            )

        logger.info(
            "BackfillAgent.resume: delivering human decision",
            extra={
                "decision": decision,
                "has_feedback": bool(feedback),
                "thread_id": self._thread_id,
                "correlation_id": self._context.correlation_id,
            },
        )

        config = {"configurable": {"thread_id": self._thread_id}}

        # ── Deliver the human's decision via Command(resume=...) ──
        # Command(resume=...) is the LangGraph primitive that passes
        # data back into the interrupted node (approval_gate).
        #
        # When approval_gate called interrupt(plan.model_dump()), it
        # paused and waited. Now Command(resume=...) delivers the
        # human's answer back as the RETURN VALUE of that interrupt()
        # call inside the approval_gate node.
        #
        # The resume value is a dict with "decision" and "feedback"
        # keys — matching what _make_approval_gate() in graph.py
        # expects from the interrupt() return value.
        try:
            result_state = self._graph.invoke(
                Command(resume={"decision": decision, "feedback": feedback}),
                config=config,
            )
        except Exception as exc:
            logger.error(
                "BackfillAgent.resume: graph execution failed after resume",
                extra={
                    "error": str(exc),
                    "decision": decision,
                    "correlation_id": self._context.correlation_id,
                },
            )
            context = self._context
            started_at = self._started_at
            self._clear_phase_state()
            return self._make_result(
                context=context,
                status="failure",
                report={"error": str(exc), "phase": "resume"},
                started_at=started_at,
                errors=[f"Graph execution failed after resume: {exc}"],
            )

        # ── Detect interrupt (revised plan) vs completion ──────────
        # Same detection as invoke() — the graph might hit interrupt
        # again if the human rejected and the agent revised the plan.
        return self._process_graph_result(result_state, config)

    # ── Shared result processing ───────────────────────────────────────

    def _process_graph_result(
        self,
        result_state: dict,
        config: dict,
    ) -> AgentResult:
        """
        Inspect graph state after invoke/resume and build AgentResult.

        This method is shared between invoke() and resume() because both
        need the same detection logic: "is the graph interrupted (plan
        needs approval) or did it complete (success/failure)?"

        The detection uses graph.get_state(config).next — the list of
        nodes pending execution. If non-empty, the graph is paused at
        an interrupt point. If empty, the graph ran to END.

        Args:
            result_state: The state dict returned by graph.invoke().
            config: The graph config with thread_id (for get_state).

        Returns:
            AgentResult with appropriate status:
              - "needs_approval" if graph is interrupted
              - "success" if graph completed without errors
              - "failure" if graph completed with errors
        """
        actions = _extract_tool_calls(result_state.get("messages", []))
        errors = result_state.get("errors", [])
        plan = result_state.get("plan")

        # ── Check if the graph is paused at an interrupt ───────────
        # graph.get_state() returns a StateSnapshot with a .next field
        # listing nodes that will execute when the graph resumes.
        # Non-empty .next means the graph is paused at interrupt().
        # Empty .next means the graph completed (reached END).
        #
        # This is the AUTHORITATIVE signal — it comes from LangGraph's
        # checkpoint system, not from our state fields. We don't need
        # to guess from approval_status or other state values.
        graph_state = self._graph.get_state(config)
        is_interrupted = bool(graph_state.next)

        if is_interrupted:
            # ── Graph is paused — plan needs human approval ────────
            # The plan_node has already run and produced a BackfillPlan
            # stored in state["plan"]. The approval_gate called
            # interrupt() and is waiting for Command(resume=...).
            #
            # Return "needs_approval" so the caller knows to present
            # the plan to a human and call resume() with their decision.
            logger.info(
                "BackfillAgent: graph interrupted — plan ready for review",
                extra={
                    "plan_iterations": result_state.get(
                        "plan_iterations", 0
                    ),
                    "investigation_tools_used": len(actions),
                    "correlation_id": self._context.correlation_id,
                },
            )
            return self._make_result(
                context=self._context,
                status="needs_approval",
                report=(
                    plan.model_dump()
                    if plan
                    else {"error": "No plan produced"}
                ),
                started_at=self._started_at,
                actions=actions,
            )
        else:
            # ── Graph completed (reached END) ──────────────────────
            # Two cases: success (plan approved + execution done) or
            # failure (errors like max revisions exceeded).
            context = self._context
            started_at = self._started_at

            # Clear phase state — lifecycle is complete. The agent is
            # ready for a new invoke() call.
            self._clear_phase_state()

            if errors:
                # ── Failure: graph ended with errors ───────────────
                # Most common cause: max_plan_iterations exceeded
                # (human kept rejecting, ran out of revision attempts).
                # Can also happen if execution tools fail.
                logger.warning(
                    "BackfillAgent: graph completed with errors",
                    extra={
                        "errors": errors,
                        "plan_iterations": result_state.get(
                            "plan_iterations", 0
                        ),
                        "correlation_id": context.correlation_id,
                    },
                )
                return self._make_result(
                    context=context,
                    status="failure",
                    report={
                        "plan": plan.model_dump() if plan else None,
                        "execution_audit": result_state.get(
                            "execution_audit", []
                        ),
                        "errors": errors,
                    },
                    started_at=started_at,
                    actions=actions,
                    errors=errors,
                )
            else:
                # ── Success: plan approved and executed ─────────────
                # The full lifecycle completed:
                #   investigate → plan → approve → execute → END
                # Report contains the approved plan AND the execution
                # audit trail (lock, steps, release).
                execution_audit = result_state.get("execution_audit", [])
                logger.info(
                    "BackfillAgent: completed successfully",
                    extra={
                        "execution_steps": len(execution_audit),
                        "plan_iterations": result_state.get(
                            "plan_iterations", 0
                        ),
                        "tool_calls": actions,
                        "correlation_id": context.correlation_id,
                    },
                )
                return self._make_result(
                    context=context,
                    status="success",
                    report={
                        "plan": plan.model_dump() if plan else None,
                        "execution_audit": execution_audit,
                    },
                    started_at=started_at,
                    actions=actions,
                )

    # ── Internal helpers ───────────────────────────────────────────────

    def _clear_phase_state(self) -> None:
        """
        Reset instance state after graph completion or unrecoverable failure.

        Called when the two-phase lifecycle ends — either the graph
        completed to END (success or failure) or crashed with an
        exception. Clears the stored thread_id, context, and started_at
        so the agent is ready for a new invoke() call.

        Without this cleanup, a subsequent invoke() could accidentally
        see stale state from a previous lifecycle.
        """
        self._thread_id = None
        self._context = None
        self._started_at = None

    @property
    def has_pending_approval(self) -> bool:
        """
        Check if this agent has a pending plan awaiting human approval.

        Useful for the platform layer (router, CLI) to know whether
        resume() can be called without hitting a ValueError.

        Returns:
            True if invoke() was called and the graph is paused at the
            approval gate. False if no lifecycle is active.
        """
        return self._thread_id is not None


# ---------------------------------------------------------------------------
# Helper — extract tool call names from the message history
# ---------------------------------------------------------------------------

def _extract_tool_calls(messages: list) -> list[str]:
    """
    Walk the message history and collect tool names the LLM called.

    Same helper as Recon and DLQ agents — the logic is identical because
    it only depends on the LangChain message format, not the specific
    tools used.

    For the Backfill agent, this captures tools from BOTH phases:
      - Investigation tools: check_incident_context, get_partition_metadata,
        get_pipeline_status, query_data_quality_metrics, get_backfill_history
      - Execution tools: acquire_pipeline_lock, execute_backfill_step,
        release_pipeline_lock

    Returns:
        List of tool names in call order, e.g.:
        ["check_incident_context", "get_partition_metadata",
         "acquire_pipeline_lock", "execute_backfill_step",
         "release_pipeline_lock"]
    """
    tool_names = []
    for msg in messages:
        if isinstance(msg, AIMessage) and hasattr(msg, "tool_calls"):
            for tc in msg.tool_calls:
                tool_names.append(tc["name"])
    return tool_names
