# Code Guide: `argus/agents/backfill/state.py`

> **Read this BEFORE opening `argus/agents/backfill/state.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **LangGraph state schema for the Backfill agent** — the typed state that flows through every node in the graph. It defines what information the graph carries, how fields accumulate across nodes, and what initial values look like.

This is the third state schema in Argus. Comparing it with `ReconState` (Phase 2) and `DLQTriageState` (Phase 3) reveals what HITL approval requires that autonomous agents don't need.

---

## What Feature Does It Bring to Argus?

1. **HITL approval state** — `approval_status`, `revision_feedback`, and `plan_iterations` fields that drive the interrupt/resume/rejection loop
2. **Plan as intermediate output** — `plan: BackfillPlan | None` holds the structured plan between investigation and execution phases
3. **Two-level iteration control** — independent safety caps for the inner ReAct loop and the outer plan-revision loop
4. **Execution audit accumulator** — `execution_audit` tracks Lock → Execute → Release actions after approval

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **LangGraph dict-state pattern** | Class annotations define state channels |
| **`Annotated[list, operator.add]`** | Reducer for accumulating messages, audit entries, errors |
| **Pydantic model reference** | `BackfillPlan` from `argus.schemas.reports` as intermediate output |
| **Factory function pattern** | `make_initial_state()` builds the seed dict for `graph.invoke()` |
| **Overwrite semantics** | `plan` field has no reducer — latest plan replaces previous |

---

## Field-by-Field Walkthrough

### Conversation History

```python
messages: Annotated[list[AnyMessage], operator.add]
```

Same pattern as ReconState and DLQTriageState. Every LLM response (AIMessage) and tool result (ToolMessage) appends here. The `operator.add` reducer means nodes return `{"messages": [new_msg]}` and it accumulates.

### Trigger Metadata (Set Once)

```python
run_date: str                   # e.g. "2026-09-28"
trigger_params: dict[str, Any]  # full params from trigger
correlation_id: str             # for log tracing
```

Set by the entry node, read-only after. Unlike Recon (which has `gate_name`) or DLQ (which has `source_lane`), the Backfill agent uses a generic `trigger_params` dict because backfill triggers can come from multiple sources (Recon recommendation, manual request, router pattern detection).

### Investigation Loop Control

```python
iteration: int
max_iterations: int  # default 15
```

Same as Recon/DLQ. Caps tool calls during the ReAct investigation phase. Defaults to 15 (higher than Recon/DLQ's 10) because the Backfill agent has 5 investigation tools and may need multiple calls with different parameters.

### Plan Output (NEW in Phase 4)

```python
plan: BackfillPlan | None
```

The key architectural difference from Phases 2 and 3. `BackfillPlan` is a Pydantic model with `incident_summary`, `root_cause`, `affected_partitions`, `proposed_steps`, `estimated_duration_minutes`, `risk_assessment`, `recommended_severity`, and `notifications`.

**No reducer** — uses overwrite semantics. When the human rejects and the LLM produces a revised plan, we want the latest plan, not a history.

### HITL Approval Fields (NEW in Phase 4)

```python
approval_status: str       # "" → "pending" → "approved" / "rejected"
revision_feedback: str     # human's rejection reason
plan_iterations: int       # outer loop counter
max_plan_iterations: int   # default 3
```

These four fields have no equivalent in autonomous agents:

- **`approval_status`** — the routing signal for the conditional edge after `interrupt()`. Empty means "still investigating." The planning node sets it to `"pending"`. The approval handler sets it to `"approved"` or `"rejected"` based on `Command(resume=...)`.
- **`revision_feedback`** — the human's rejection reason, injected as a HumanMessage so the LLM knows what to change. Empty string when not rejected.
- **`plan_iterations`** — increments each time a plan is produced. When `>= max_plan_iterations`, the graph stops even if the human keeps rejecting.
- **`max_plan_iterations`** — default 3. Prevents infinite reject-rework cycles.

### Execution Audit (NEW in Phase 4)

```python
execution_audit: Annotated[list[str], operator.add]
```

After approval, every execution action appends here: lock acquisition, step execution results, lock release. Same accumulator pattern as DLQ's `requeue_audit`, but tracking multi-step execution.

### Errors

```python
errors: Annotated[list[str], operator.add]
```

Same as all previous agents. Nodes append errors without halting the graph.

---

## The Two-Level Iteration Pattern

This is the most important concept in BackfillState:

```
Outer loop: plan revisions (plan_iterations / max_plan_iterations = 3)
  │
  ├── Inner loop: ReAct tool calls (iteration / max_iterations = 15)
  │     LLM calls tools → gets results → calls more tools → ...
  │     Capped at 15 iterations per investigation round
  │
  ├── Planning node → plan produced → interrupt → human reviews
  │
  ├── If rejected: iteration RESETS, plan_iterations increments, loop back
  ├── If approved: proceed to execution
  └── If plan_iterations >= 3: stop with error
```

The inner loop resets each time the outer loop cycles. This prevents a scenario where the LLM exhausts its tool-call budget during the first investigation and can't call any tools when reworking the rejected plan.

---

## Factory Function: `make_initial_state()`

```python
def make_initial_state(
    run_date: str,
    trigger_params: dict[str, Any],
    correlation_id: str,
    max_iterations: int = 15,
    max_plan_iterations: int = 3,
) -> dict[str, Any]:
```

Same factory pattern as Recon/DLQ, but initializes additional HITL fields to their "not started yet" values:

| Field | Initial Value | Meaning |
|---|---|---|
| `plan` | `None` | No plan yet |
| `approval_status` | `""` | Not at approval stage |
| `revision_feedback` | `""` | No feedback |
| `plan_iterations` | `0` | No plans produced |
| `execution_audit` | `[]` | Nothing executed |

---

## Comparing State Schemas Across Phases

| Category | ReconState | DLQTriageState | BackfillState |
|---|---|---|---|
| Messages | `operator.add` | same | same |
| Trigger context | `gate_name` | `source_lane` | `trigger_params` dict |
| Loop control | iteration/max | iteration/max | iteration/max + plan_iterations/max |
| Intermediate output | — | `classifications` | `plan` (BackfillPlan) |
| Side-effect tracking | — | `requeue_audit` | `execution_audit` |
| Final output | `report` | `report` | plan IS the output |
| HITL fields | — | — | approval_status, revision_feedback |

---

## Key Takeaways

1. **HITL requires explicit state fields** — approval status, feedback, and iteration caps can't be implicit
2. **Two-level loops need independent counters** — inner resets when outer cycles
3. **Intermediate outputs use overwrite, not accumulate** — only the latest plan matters
4. **Factory functions encapsulate defaults** — caller doesn't need to know about HITL internals
