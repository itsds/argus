# Code Guide: `argus/agents/reconciliation/state.py`

> **Read this BEFORE opening `argus/agents/reconciliation/state.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **state schema for the Reconciliation agent's LangGraph graph**. It defines every piece of data that flows between graph nodes: the conversation history, trigger metadata, loop control, and the final report. LangGraph reads this schema to know how to merge partial updates from each node.

---

## What Feature Does It Bring to Argus?

1. **Graph state definition** — defines the "shape" of data that flows through the Recon agent's execution graph
2. **Annotated reducers** — tells LangGraph HOW to merge node outputs (append messages vs overwrite values)
3. **ReAct loop backbone** — the `messages` field accumulates the full conversation history that drives the reason-act-observe loop
4. **Safety valve** — `iteration` / `max_iterations` prevent runaway loops
5. **Initial state factory** — `make_initial_state()` produces a clean starting state for each invocation

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **LangGraph state channels** | Each field in `ReconState` becomes a "channel" that LangGraph tracks and merges |
| **`Annotated[type, reducer]`** | Python `typing.Annotated` combined with a reducer function — the core LangGraph state pattern |
| **`operator.add`** | The reducer for lists — concatenates instead of replacing |
| **`AnyMessage` (LangChain)** | Union type covering all message types: SystemMessage, HumanMessage, AIMessage, ToolMessage |
| **`ReconReport` (Pydantic)** | The structured output schema — stored in state once the report node produces it |

---

## The Annotated Reducer Pattern — Deep Dive

This is the single most important concept in this file:

```python
messages: Annotated[list[AnyMessage], operator.add]
```

**Without a reducer** (plain `list[AnyMessage]`):
```python
# Node returns: {"messages": [new_msg]}
# LangGraph does: state["messages"] = [new_msg]  ← REPLACES entire list!
# Result: You lose all previous messages. The LLM sees nothing.
```

**With a reducer** (`Annotated[list[AnyMessage], operator.add]`):
```python
# Node returns: {"messages": [new_msg]}
# LangGraph does: state["messages"] = operator.add(state["messages"], [new_msg])
# Which is:       state["messages"] = state["messages"] + [new_msg]
# Result: New message is APPENDED. Full history preserved.
```

This is why the ReAct loop works — each LLM call and tool result accumulates in the `messages` list, and the LLM always sees the full conversation when deciding its next action.

---

## Field-by-Field Breakdown

### `messages: Annotated[list[AnyMessage], operator.add]`

The **conversation history** — the backbone of the ReAct loop:
1. Entry node seeds: `[SystemMessage, HumanMessage]`
2. LLM node adds: `AIMessage` (with `tool_calls` if it wants to use a tool)
3. Tool node adds: `ToolMessage` (with tool output)
4. Loop continues until LLM stops calling tools

### `run_date: str` / `gate_name: str` / `trigger_params: dict` / `correlation_id: str`

**Trigger metadata** — set once by the entry node, read-only after that. These come from the `TriggerContext` that started the agent. They have NO reducer (plain types), so any node that returns one of these replaces the value. Since only the entry node sets them, this is fine.

### `iteration: int` / `max_iterations: int`

**Loop control** — prevents the agent from calling tools forever:
```python
# Conditional edge in the graph:
if state["iteration"] >= state["max_iterations"]:
    return "report"   # force report generation
else:
    return "llm"      # continue investigating
```

### `report: ReconReport | None`

**Final output** — `None` until the report node produces the diagnosis. The agent's `invoke()` method reads this to build the `AgentResult`.

### `errors: Annotated[list[str], operator.add]`

**Error accumulator** — nodes can append errors without crashing the graph. Reducer ensures errors accumulate (not overwrite). Included in the final report.

---

## `make_initial_state()` — The Factory

```python
def make_initial_state(run_date, gate_name, trigger_params, correlation_id, max_iterations=10):
    return {
        "messages": [],          # entry node will add SystemMessage + HumanMessage
        "run_date": run_date,
        "gate_name": gate_name,
        "trigger_params": trigger_params,
        "correlation_id": correlation_id,
        "iteration": 0,
        "max_iterations": max_iterations,
        "report": None,
        "errors": [],
    }
```

**Why a factory function?**
- Single place to set defaults (`iteration=0`, `report=None`)
- Type-checks required fields at the call site
- Easy to extend when new fields are added
- Alternative: constructing the dict inline in `agent.py` — more error-prone, duplicated

---

## Code Flow Through the Graph

```
make_initial_state()
    │
    ▼
┌────────────────────────────────────────────────────────────────────┐
│ Entry Node                                                         │
│   Reads: run_date, gate_name, trigger_params                       │
│   Writes: messages += [SystemMessage, HumanMessage]                │
│   (seeds the conversation with the system prompt + investigation   │
│    request)                                                        │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────────┐
│ LLM Node                                                           │
│   Reads: messages (full conversation history)                      │
│   Writes: messages += [AIMessage]                                  │
│   Writes: iteration += 1                                           │
│   (LLM reasons about evidence and decides next action)             │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
                    ┌──────┴──────┐
                    │ Router      │
                    │ tool_calls? │
                    └──────┬──────┘
               ┌───────────┴───────────┐
          has tools              no tools (or max iterations)
               │                       │
               ▼                       ▼
┌──────────────────────┐  ┌──────────────────────┐
│ Tool Node            │  │ Report Node          │
│   Reads: AIMessage   │  │   Reads: messages    │
│   Writes: messages   │  │   Writes: report     │
│     += [ToolMessage] │  │   (structured output │
│   (executes tools)   │  │    from full history) │
└──────────┬───────────┘  └──────────────────────┘
           │
           └─── loops back to LLM Node
```

---

## Who Calls / Imports This File?

- **`agents/reconciliation/graph.py`** (upcoming) → uses `ReconState` as the StateGraph type parameter and `make_initial_state()` to seed the graph
- **`agents/reconciliation/agent.py`** (upcoming) → calls `make_initial_state()` with data from `TriggerContext`

---

## Where Does This File Fit?

```
argus/agents/reconciliation/
├── state.py             <── YOU ARE HERE (data schema for the graph)
├── prompts.py           <── system/human prompts (become messages in state)
├── graph.py             (upcoming — builds the StateGraph from this schema)
└── agent.py             (upcoming — invokes the graph, reads the report)
```

---

## Key Concepts to Understand

1. **LangGraph state is NOT Pydantic**: LangGraph uses plain `TypedDict` (or class annotations) for state, not Pydantic. Why? Because LangGraph needs shallow-merge semantics with reducers. Pydantic models do deep validation on every update, which would conflict with the reducer pattern.

2. **Reducers only apply when a node returns that key**: If a node returns `{"messages": [new_msg]}`, only the `messages` reducer runs. Other fields stay unchanged. If a node returns `{}`, nothing changes.

3. **Why `list[AnyMessage]` not `list[BaseMessage]`?**: `AnyMessage` is LangChain's union type that covers all message types. It's more precise than `BaseMessage` for type checking in tools and graph nodes.

4. **`from __future__ import annotations`**: This line at the top makes all type annotations strings (PEP 563), enabling forward references like `ReconReport | None` without import order issues. Essential for modern Python type hints.
