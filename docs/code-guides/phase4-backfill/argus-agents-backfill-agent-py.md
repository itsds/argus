# Code Guide: `argus/agents/backfill/agent.py`

> **Read this BEFORE opening `argus/agents/backfill/agent.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **BaseAgent subclass for the Backfill agent** — the adapter between the Backfill graph internals (LangGraph, tools, prompts, HITL interrupt/resume) and the Argus platform contract (BaseAgent interface).

While the Recon and DLQ agent.py files taught the single-invoke adapter pattern, this file teaches a **fundamentally different lifecycle**: the **two-phase invoke** where `invoke()` pauses for human approval and `resume()` continues after the human decides.

---

## What Feature Does It Bring to Argus?

1. **Two-phase invoke pattern** — `invoke()` returns `needs_approval`, then `resume()` continues after human review
2. **Thread ID management** — generates and stores the checkpoint thread_id that links invoke and resume calls
3. **Interrupt detection** — uses `graph.get_state(config).next` to determine if the graph is paused or completed
4. **Rejection loop support** — `resume()` can return `needs_approval` again when the human rejects a revised plan
5. **Stateful agent lifecycle** — instance state persists across the invoke/resume boundary

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`BaseAgent` subclass** | Implements the platform contract (name, description, build_graph, invoke) |
| **`Command(resume=...)`** | LangGraph primitive to deliver human decision into interrupted node |
| **`graph.get_state(config)`** | Retrieve checkpoint state to detect interrupt vs completion |
| **`StateSnapshot.next`** | List of pending nodes — non-empty means graph is interrupted |
| **`thread_id` in config** | Checkpoint key linking invoke() and resume() calls |
| **`uuid4`** | Fallback thread_id generation for ad-hoc runs |
| **Instance variables** | `_thread_id`, `_context`, `_started_at` persist across phases |

---

## New Imports (vs Recon/DLQ Agent Files)

```python
from uuid import uuid4                  # NEW — thread_id fallback
from langgraph.types import Command     # NEW — resume primitive
```

The Recon and DLQ agent files only imported `AIMessage`, `BaseAgent`, `TriggerContext`, their graph builder, their state factory, and the logger. The Backfill agent adds `Command` (the resume primitive) and `uuid4` (for generating thread_ids when no correlation_id is provided).

---

## The Two-Phase Lifecycle

This is the central concept of the file. Here's how it works:

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  PHASE 1: invoke(context)                                   │
│    ├── Build graph (lazy, cached)                           │
│    ├── Generate thread_id from correlation_id               │
│    ├── Translate TriggerContext → initial state              │
│    ├── graph.invoke(initial_state, config={thread_id})      │
│    │     └── Graph runs: entry → investigate → plan →       │
│    │         approval_gate → interrupt() ← PAUSES HERE      │
│    ├── Detect interrupt via graph_state.next                │
│    └── Return AgentResult(status="needs_approval",          │
│                           report=plan_dict)                 │
│                                                             │
│  ── Human reviews plan ──                                   │
│                                                             │
│  PHASE 2: resume(decision, feedback)                        │
│    ├── Retrieve stored thread_id                            │
│    ├── graph.invoke(Command(resume={decision, feedback}),   │
│    │               config={thread_id})                      │
│    │     └── If approved: execute → END                     │
│    │     └── If rejected: revise → plan → interrupt again   │
│    ├── Detect interrupt vs completion                       │
│    └── Return AgentResult(status=...)                       │
│          "needs_approval" → revised plan (loop back)        │
│          "success"        → execution completed             │
│          "failure"        → errors (max revisions, etc.)    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Why Not Just One `invoke()` Call?

In Recon/DLQ agents, the graph runs to completion in one call:
```python
final_state = self._graph.invoke(initial_state)  # runs to END
```

The Backfill graph uses `interrupt()`, which pauses the graph mid-execution. The graph literally stops running and returns control to the caller. The caller must then present the plan to a human, collect their decision, and resume the graph. This can't be done in a single synchronous call — the human review step happens outside the graph.

---

## Function-by-Function Walkthrough

### `__init__(self, config)` — Extended Constructor

Adds three instance variables that Recon/DLQ agents don't need:

```python
self._thread_id: str | None = None    # Links invoke() to resume()
self._context: TriggerContext | None = None  # Original trigger
self._started_at: datetime | None = None     # Timing
```

**Why are these needed?** Because `resume()` is a separate method call that happens later. It needs:
- `_thread_id` to tell LangGraph which checkpoint to load
- `_context` to build the final AgentResult with the right agent_name, correlation_id
- `_started_at` to record the total elapsed time (from initial invoke to final completion)

### `invoke(context)` — Phase 1

Five steps, same structure as Recon/DLQ but with critical differences:

| Step | Recon/DLQ | Backfill |
|---|---|---|
| 1. Build graph | Same | Same (lazy, cached) |
| 2. Config | — | **Generate thread_id, store on self** |
| 3. Translate context | gate_name/source_lane | trigger_params dict |
| 4. Run graph | `graph.invoke(state)` | `graph.invoke(state, config=config)` — **config required** |
| 5. Process result | Extract report, package | **Detect interrupt vs completion** |

**Step 2 is entirely new.** Thread_id generation:
```python
self._thread_id = (
    f"backfill-{context.correlation_id}"
    if context.correlation_id
    else f"backfill-{uuid4().hex[:12]}"
)
```

**Step 4 passes config.** Recon/DLQ call `graph.invoke(state)` with no config because they have no checkpointer. Backfill MUST pass `config={"configurable": {"thread_id": ...}}` so the MemorySaver knows where to save the interrupted state.

### `resume(decision, feedback)` — Phase 2

This method has no equivalent in Recon/DLQ. It:

1. **Validates** — checks `_thread_id` exists (invoke was called first)
2. **Builds Command** — `Command(resume={"decision": ..., "feedback": ...})`
3. **Invokes with Command** — `graph.invoke(Command(...), config=config)`
4. **Processes result** — same detection as invoke() (shared method)

The Command object is passed as the INPUT to `graph.invoke()`, not as part of the state. LangGraph intercepts it and delivers the resume value to the paused `interrupt()` call inside approval_gate.

### `_process_graph_result(result_state, config)` — Shared Detection

This is the key method that both invoke() and resume() call. It uses LangGraph's checkpoint API:

```python
graph_state = self._graph.get_state(config)
is_interrupted = bool(graph_state.next)
```

`graph_state.next` is a tuple of node names that will execute when the graph resumes. If non-empty, the graph is paused (interrupted). If empty, the graph has completed (reached END).

**Three possible outcomes:**

| `is_interrupted` | `errors` | Status | Meaning |
|---|---|---|---|
| `True` | — | `needs_approval` | Plan ready for review |
| `False` | Empty | `success` | Plan approved and executed |
| `False` | Non-empty | `failure` | Max revisions or execution error |

### `_clear_phase_state()` — Lifecycle Cleanup

Resets `_thread_id`, `_context`, `_started_at` to `None`. Called when the lifecycle ends (graph completed or crashed). Without this, stale state from a previous lifecycle could contaminate a new invoke() call.

### `has_pending_approval` — Platform Query

Property that returns `True` if invoke() was called and the graph is paused. The platform (router, CLI) can check this before calling resume() to avoid the ValueError.

### `_extract_tool_calls()` — Same as Recon/DLQ

Module-level helper. Walks AIMessages to collect tool call names. Identical logic across all three agents because it depends only on LangChain's message format.

---

## Design Decisions

### Why Separate `invoke()` and `resume()` Instead of One Method?

Overloading `invoke()` with an optional decision parameter would conflate "start new investigation" with "continue existing investigation." The caller would need to know whether to pass a TriggerContext or a decision — the method signature would be ambiguous. Separate methods make the lifecycle explicit in the API:

```python
# Clear intent: starting a new investigation
result = await agent.invoke(context)

# Clear intent: continuing after human review
result = await agent.resume("approved")
```

### Why Store Context on Self Instead of Requiring Caller to Re-pass?

The platform shouldn't need to remember agent-internal details like the thread_id or original started_at timestamp. The agent manages its own lifecycle. This is the same encapsulation principle as `_make_result()` in BaseAgent — the helper exists so callers don't need to know about internal packaging.

### Why `graph_state.next` Instead of Checking `approval_status`?

`graph_state.next` is a **graph-engine-level signal** — it comes from LangGraph's checkpoint system and works regardless of how the graph's internal routing changes. Checking `approval_status` would couple the agent adapter to the exact state machine inside graph.py. If we ever renamed the field or changed the approval flow, we'd need to update agent.py too.

### Why Can `resume()` Return `needs_approval`?

The rejection loop means the graph can interrupt multiple times. Consider this flow:

```
invoke()  → plan v1 → needs_approval
resume(rejected, "add rollback") → plan v2 → needs_approval  
resume(rejected, "also add monitoring") → plan v3 → needs_approval
resume(approved) → execution → success
```

Each rejection cycle goes through: revise_node → plan_node → approval_gate → interrupt(). The agent returns `needs_approval` each time because the same detection logic applies. The caller must loop:

```python
result = await agent.invoke(context)
while result.status == "needs_approval":
    decision = present_plan_to_human(result.report)
    result = await agent.resume(**decision)
```

### Why Derive Thread ID from Correlation ID?

Traceability. If you have incident ID `INC-2026-0928`, the checkpoint thread is `backfill-INC-2026-0928`. You can look up the checkpoint by incident ID rather than a random UUID. The UUID fallback handles ad-hoc CLI runs that don't have a correlation_id.

---

## Comparing Agent Files Across Phases

| Aspect | Recon (Phase 2) | DLQ (Phase 3) | Backfill (Phase 4) |
|---|---|---|---|
| Lifecycle | Single invoke | Single invoke | **Two-phase: invoke + resume** |
| Instance state | Stateless | Stateless | **Stateful (_thread_id, _context, _started_at)** |
| Graph config | No config needed | No config needed | **config with thread_id required** |
| Status values | success / failure | success / failure | **success / failure / needs_approval** |
| Methods | invoke() | invoke() | **invoke() + resume() + has_pending_approval** |
| Command usage | — | — | **Command(resume=...) in resume()** |
| Interrupt detection | — | — | **graph.get_state().next** |
| Error boundary | try/except graph | Same | **try/except in both invoke and resume** |
| Result processing | Inline | Inline | **Shared _process_graph_result()** |
| Context translation | gate_name | source_lane | **trigger_params dict** |
| Report content | ReconReport.model_dump() | DLQTriageReport.model_dump() | **plan dict or plan + execution_audit** |

---

## How the Platform Uses This Agent

```python
from argus.agents.backfill.agent import BackfillAgent
from argus.agents.base import TriggerContext

# Create and invoke
agent = BackfillAgent(config)
context = TriggerContext(
    agent_name="backfill",
    trigger_source="cli",
    run_date="2026-09-28",
    params={"incident_id": "INC-2026-0928", "dag_id": "ttag_main"},
)

# Phase 1: investigate and plan
result = await agent.invoke(context)
# result.status == "needs_approval"
# result.report == plan dict (BackfillPlan fields)

# Check for pending approval
assert agent.has_pending_approval  # True

# Phase 2: human reviews and decides
while result.status == "needs_approval":
    print(f"Plan (iteration {result.report.get('plan_iterations', '?')}):")
    print(result.report)

    decision = input("Approve or reject? ")
    if decision == "approved":
        result = await agent.resume("approved")
    else:
        feedback = input("Feedback: ")
        result = await agent.resume("rejected", feedback)

# After loop: result.status is "success" or "failure"
assert not agent.has_pending_approval  # False — lifecycle complete
print(result.report)  # {"plan": {...}, "execution_audit": [...]}
```

---

## Key Takeaways

1. **HITL requires a two-phase invoke pattern** — `invoke()` starts, `resume()` continues. The graph pauses between them.
2. **Thread ID is the bridge between phases** — same thread_id tells the checkpointer to load the right interrupted state.
3. **`graph_state.next` is the interrupt signal** — non-empty means the graph is paused, empty means it completed.
4. **Agents can be stateful** — when the lifecycle spans multiple calls, instance state is needed. Cleanup (`_clear_phase_state`) prevents stale state.
5. **`Command(resume=...)` wraps the human's decision** — the agent translates a simple (decision, feedback) pair into LangGraph's resume primitive.
6. **`resume()` can loop** — rejection cycles mean the caller must handle multiple `needs_approval` responses.
7. **Shared result processing avoids duplication** — `_process_graph_result()` handles the same "interrupted or completed?" logic for both phases.
8. **The BaseAgent contract extends gracefully** — adding `resume()` and `has_pending_approval` doesn't break the existing `invoke()` contract that Recon/DLQ use.
