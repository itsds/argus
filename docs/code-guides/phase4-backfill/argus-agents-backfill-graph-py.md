# Code Guide: `argus/agents/backfill/graph.py`

> **Read this BEFORE opening `argus/agents/backfill/graph.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **LangGraph StateGraph for the Backfill agent** — the most complex graph in Argus. It connects 8 nodes into a multi-phase topology with two ReAct loops, a planning node, and a human-in-the-loop approval gate with a rejection loop.

While the Recon and DLQ graphs (Phases 2-3) taught the ReAct loop as a reusable pattern, this graph teaches how to **compose** multiple patterns — ReAct, structured output, and HITL interrupt/resume — into a single graph with conditional routing between phases.

---

## What Feature Does It Bring to Argus?

1. **Human-in-the-Loop (HITL)** — the graph pauses for human plan approval using `interrupt()` and resumes with `Command(resume=...)`
2. **Rejection loop** — rejected plans loop back through revision, not just binary approve/reject
3. **Two-phase execution** — investigation and execution run as separate ReAct loops with different tool sets
4. **Checkpointed state** — `MemorySaver` persists state across the interrupt/resume boundary

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`interrupt()`** | Pauses graph execution, returns value to caller |
| **`Command(resume=...)`** | Resumes paused graph with external data (human decision) |
| **`MemorySaver`** | In-memory checkpointer for interrupt/resume state persistence |
| **`thread_id`** | Checkpoint key linking initial invoke and resume invoke |
| **`StateGraph`** | LangGraph graph type with typed state flow |
| **`ToolNode`** | Prebuilt node for automatic tool dispatch (TWO instances) |
| **`.bind_tools()`** | Attaches tool schemas to LLM (TWO separate bindings) |
| **`.with_structured_output()`** | Forces LLM to produce Pydantic model (BackfillPlan) |
| **Closure pattern** | Factory functions capture model references for DI |
| **Conditional edges** | THREE routing functions (investigate, approve, execute) |

---

## New Imports (vs Recon/DLQ Graphs)

```python
from langgraph.types import Command, interrupt       # NEW — HITL primitives
from langgraph.checkpoint.memory import MemorySaver   # NEW — inside builder only
```

The Recon and DLQ graphs imported `StateGraph`, `END`, and `ToolNode`. The Backfill graph adds `Command` and `interrupt` from `langgraph.types` — these are the two HITL primitives. `MemorySaver` is imported inside `build_backfill_graph()` because checkpointing is a graph-assembly concern.

---

## Graph Topology

```
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  INVESTIGATION (ReAct loop):                                         │
│    entry → investigate_llm → should_continue_investigating?          │
│                                  │              │                    │
│                            (tools)│        (done)│                   │
│                                  ▼              ▼                    │
│                         investigation_tools   plan_node              │
│                            │                    │                    │
│                            └──► back to llm     │                    │
│                                                 ▼                    │
│  APPROVAL LOOP:                                                      │
│    approval_gate (interrupt) ◄─── revise_node                        │
│         │              │               ▲                             │
│    (approved)    (rejected+feedback)    │                             │
│         │              └───────────────┘                              │
│         │         (max revisions) ──► END (with error)               │
│         ▼                                                            │
│  EXECUTION (ReAct loop):                                             │
│    execute_llm → should_continue_executing?                          │
│                       │              │                               │
│                 (tools)│        (done)│                               │
│                       ▼              ▼                                │
│                  execution_tools   END                                │
│                       │                                              │
│                       └──► back to execute_llm                       │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Function-by-Function Walkthrough

### `_make_entry_node()` — Seed the Conversation

Same pattern as Recon/DLQ. Seeds `SystemMessage` (investigation strategy) + `HumanMessage` (incident details) using `BACKFILL_PROMPT_TEMPLATE`.

**Key difference from Recon/DLQ:** uses `trigger_params` (generic dict) instead of `gate_name` or `source_lane`.

### `_make_investigate_llm_node(model_with_investigation_tools)` — Phase 1 REASON Step

Identical structure to the Recon/DLQ LLM nodes: send full message history → get AIMessage → increment iteration. The difference is the model has `BACKFILL_INVESTIGATION_TOOLS` (5 read-only tools) bound.

**Why a separate factory from execute_llm?** Because they bind DIFFERENT tools. The investigation LLM literally cannot call `execute_backfill_step` — graph-level safety.

### `_should_continue_investigating(state) → str` — Investigation Router

Routes to `"investigation_tools"` or `"plan_node"`. This is the key difference from the Recon/DLQ router which routes to `"tools"` or `"report"`. Investigation feeds into planning, not directly into a report.

```python
if iteration >= max_iterations:
    return "plan_node"   # Force plan with what we have
if last_message has tool_calls:
    return "investigation_tools"
return "plan_node"       # Investigation complete
```

### `_make_plan_node(model)` — Structured Output

The planning node does four things:

1. **Swaps the system prompt** — replaces `BACKFILL_INVESTIGATION_PROMPT` with `BACKFILL_PLANNING_PROMPT` (the quality rubric)
2. **Preserves evidence** — copies all non-`SystemMessage`s from conversation
3. **Produces BackfillPlan** — via `model.with_structured_output(BackfillPlan)`
4. **Stores plan summary as AIMessage** — so the LLM can see it if the plan gets rejected

**Why swap the system prompt?** Different phase, different instructions. The planning rubric tells the LLM WHAT MAKES A GOOD PLAN — field semantics, ordering rules, quality examples. Mixing this with investigation instructions would confuse priorities.

**Why store the plan as AIMessage?** If the plan is rejected and the LLM needs to revise, it should be able to see what it previously proposed. The plan_summary string captures the key fields in a readable format that stays in the conversation history.

### `_make_approval_gate()` — The HITL Heart

This is the most important new concept in Phase 4. The node:

1. **Calls `interrupt(plan.model_dump())`** — serializes the plan and pauses the graph
2. **Receives the resume value** — the human's decision arrives as the return value of `interrupt()`
3. **Updates state** — sets `approval_status` and `revision_feedback`
4. **Checks max iterations** — if rejected too many times, sets an error and routes to END

```python
resume_value = interrupt(plan.model_dump())  # PAUSE HERE

decision = resume_value.get("decision", "")
feedback = resume_value.get("feedback", "")

if decision == "approved":
    return {"approval_status": "approved"}
elif decision == "rejected":
    if state["plan_iterations"] >= state["max_plan_iterations"]:
        return {"approval_status": "", "errors": ["Max plan revisions exceeded"]}
    return {"approval_status": "rejected", "revision_feedback": feedback}
```

### `_route_after_approval(state) → str` — Approval Router

Three-way conditional edge:

| `approval_status` | Route | What happens |
|---|---|---|
| `"approved"` | `"execute_llm"` | Proceed to execution phase |
| `"rejected"` | `"revise_node"` | Inject feedback, produce new plan |
| anything else | `END` | Error case (max revisions exceeded) |

### `_make_revise_node()` — Feedback Injection

Injects the human's rejection feedback as a `HumanMessage` using `BACKFILL_REVISION_TEMPLATE`. Two critical state updates:

- **`iteration: 0`** — resets the inner ReAct counter so the LLM gets fresh tool-call budget for revision
- Routes to `plan_node` (not back to `investigate_llm`) because the evidence is already in the history

### `_make_execute_llm_node(model_with_execution_tools)` — Phase 2 REASON Step

Second ReAct loop with execution tools. Key behavior: injects `BACKFILL_EXECUTION_PROMPT` as the system prompt ONLY on the first call (detected by empty `execution_audit`):

```python
if not state.get("execution_audit"):
    # First call — inject execution system prompt
    exec_system = SystemMessage(content=BACKFILL_EXECUTION_PROMPT)
    messages_for_llm = [exec_system] + non_system_messages
else:
    messages_for_llm = state["messages"]  # Subsequent calls — prompt already injected
```

This avoids wasting tokens by re-injecting the system prompt on every ReAct iteration.

### `_should_continue_executing(state) → str` — Execution Router

Routes to `"execution_tools"` or `END`. Same logic as the investigation router but the "done" destination is `END` instead of `"plan_node"`.

### `build_backfill_graph(config: ArgusConfig)` — The Public API

Assembles everything into a compiled graph. Key steps:

1. **Create base LLM** — `create_llm(config)` (same as Recon/DLQ)
2. **Bind tools (two configs)** — `llm.bind_tools(INVESTIGATION_TOOLS)` and `llm.bind_tools(EXECUTION_TOOLS)`
3. **Create two ToolNodes** — `ToolNode(INVESTIGATION_TOOLS)` and `ToolNode(EXECUTION_TOOLS)`
4. **Build StateGraph** — `StateGraph(BackfillState)` with 8 nodes
5. **Wire edges** — 3 fixed edges, 3 conditional edges
6. **Compile with checkpointer** — `graph.compile(checkpointer=MemorySaver())`

---

## Design Decisions

### Why Not Extract a Reusable ReAct Loop Builder?

The Backfill graph has two ReAct loops (investigation and execution) with different tools, different prompts, and an approval gate between them. A generic ReAct builder would save ~30 lines but add abstraction that hides how the pieces connect. For learning, explicit is better than clever.

### Why Does `revise_node` Route to `plan_node`, Not `investigate_llm`?

The investigation evidence is already in the message history. Revision means producing a BETTER plan from the same evidence, not re-investigating. The revise node injects feedback and routes to planning, which uses the full conversation (including investigation results) to produce a revised `BackfillPlan`.

### Why `MemorySaver` Inside `build_backfill_graph()`?

The graph MUST have a checkpointer for `interrupt()` to work. Without it, the state is lost when the graph pauses. Placing `MemorySaver()` inside the builder makes it a graph concern, not a caller concern — the caller doesn't need to know about checkpointing to use the graph.

### Why Store Plan Summary as AIMessage (Not Just in State)?

If the plan gets rejected, the LLM needs to see what it previously proposed to know what to change. State fields (`plan: BackfillPlan`) aren't visible in the conversation — only messages are. The plan summary as an `AIMessage` makes the previous plan visible to the LLM during revision.

---

## Comparing with Previous Graphs

| Aspect | Recon (Phase 2) | DLQ (Phase 3) | Backfill (Phase 4) |
|---|---|---|---|
| Nodes | 4 | 4 | 8 |
| ToolNodes | 1 | 1 | 2 |
| LLM configs | 2 | 2 | 3 |
| Conditional edges | 1 | 1 | 3 |
| HITL | — | — | `interrupt()` + `Command(resume=...)` |
| Checkpointer | — | — | `MemorySaver` |
| Rejection loop | — | — | revise → re-plan → re-approve |
| System prompt swapping | — | — | investigation → planning → execution |
| ReAct loops | 1 | 1 | 2 (investigation + execution) |
| Final output | `ReconReport` via structured output | `DLQTriageReport` | Plan = intermediate, execution = final |

---

## How the Caller Uses This Graph

```python
from langgraph.types import Command

# Build the graph (checkpointer is internal)
graph = build_backfill_graph(config)

# Phase 1: Investigation + Planning → interrupt
config = {"configurable": {"thread_id": "backfill-2026-09-28"}}
result = graph.invoke(initial_state, config=config)
# result contains the plan for review (from interrupt)

# Phase 2a: Approve → Execution → END
result = graph.invoke(
    Command(resume={"decision": "approved"}),
    config=config  # SAME thread_id
)

# OR Phase 2b: Reject → Revision → Re-plan → interrupt again
result = graph.invoke(
    Command(resume={"decision": "rejected",
                     "feedback": "Add rollback steps"}),
    config=config
)
# Then approve the revised plan with the same thread_id
```

---

## Key Takeaways

1. **`interrupt()` is the HITL primitive** — it pauses the graph and returns a value to the caller
2. **`Command(resume=...)` is how the human responds** — it delivers data back into the paused node
3. **Checkpointing is mandatory for HITL** — without it, interrupt loses state
4. **`thread_id` links invocations** — same thread_id = same graph execution
5. **Two ToolNodes provide structural safety** — the LLM can't call execution tools during investigation
6. **Rejection loops need iteration caps** — `max_plan_iterations` prevents infinite revision cycles
7. **System prompt swapping enables multi-phase behavior** — different instructions for different graph phases
8. **Plan summary as AIMessage preserves context** — the LLM can see rejected plans during revision
