# Code Guide: `argus/agents/reconciliation/graph.py`

> **Read this AFTER reading the state, prompts, and tools guides.**
> This guide explains how the StateGraph wires those pieces into a running agent.

---

## What Is This File About?

This is the **LangGraph StateGraph** for the Reconciliation agent — the wiring diagram that connects tools, prompts, state, and LLM into an autonomous investigation loop. The previous files defined the pieces; this file assembles them into a graph the framework can execute.

---

## What Feature Does It Bring to Argus?

1. **ReAct execution loop** — the entry → LLM → tools → LLM → ... → report cycle that drives autonomous investigation
2. **Graph topology** — nodes, edges, and conditional routing that LangGraph compiles into a runnable
3. **Dependency injection via closures** — the LLM model is created once from config, then captured in node closures
4. **Iteration safety valve** — the router checks `iteration >= max_iterations` to prevent runaway loops
5. **Structured report generation** — a dedicated report node using `.with_structured_output(ReconReport)`

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`StateGraph`** (LangGraph) | The graph class — nodes are functions, edges connect them, state flows through |
| **`ToolNode`** (LangGraph prebuilt) | Auto-executes tool calls from AIMessages — no manual tool dispatch needed |
| **`.bind_tools()`** (LangChain) | Attaches tool schemas to the LLM so it can emit `tool_calls` |
| **`.with_structured_output()`** (LangChain) | Forces the LLM to produce JSON matching a Pydantic schema |
| **`END`** (LangGraph) | Sentinel marking the graph's terminal node |
| **Closures** (Python) | Factory functions that capture the LLM in scope for each node |

---

## The Four Nodes — Deep Dive

### 1. Entry Node (`_make_entry_node`)

**What it does:** Seeds the conversation with the system prompt and investigation request.

```python
prompt_value = RECON_PROMPT_TEMPLATE.invoke({
    "run_date": state["run_date"],
    "gate_name": state["gate_name"],
    "trigger_params": params_str,
})
return {"messages": prompt_value.to_messages()}
```

**Why it's separate from the LLM node:**
- The entry node does template rendering (no LLM call)
- The LLM node does model invocation (no template logic)
- Separation keeps each node doing exactly one thing

**Key detail:** `prompt_value.to_messages()` returns `[SystemMessage, HumanMessage]`. The `operator.add` reducer on `messages` appends these to the empty list from `make_initial_state()`.

### 2. LLM Node (`_make_llm_node`)

**What it does:** Sends the full conversation history to the LLM (with tools bound).

```python
response = model_with_tools.invoke(state["messages"])
return {
    "messages": [response],          # reducer appends the AIMessage
    "iteration": state["iteration"] + 1,  # no reducer → replaces
}
```

**This is the REASON step in ReAct.** The LLM sees everything — system prompt, human request, and every previous tool call + result. It then decides:
- Call one or more tools → emit `tool_calls` in the AIMessage
- Produce text only → the investigation is complete

**Why increment iteration here?** Each LLM call is one "thinking step." Tracking it in the LLM node gives an accurate count of how many times the agent reasoned. The safety valve in the router reads this count.

### 3. Tool Node (`ToolNode(RECON_TOOLS)`)

**What it does:** Automatically executes tool calls from the last AIMessage.

This is a **prebuilt LangGraph component** — you don't write tool execution logic:

```
AIMessage with tool_calls=[{"name": "compare_row_counts", "args": {...}}]
    │
    ▼ ToolNode reads the tool_calls
    │
    ▼ Finds compare_row_counts in RECON_TOOLS
    │
    ▼ Calls the function with the provided args
    │
    ▼ Returns: {"messages": [ToolMessage(content=<result>)]}
```

**Why ToolNode instead of manual dispatch?**
- No `if tool_name == "..." elif ...` boilerplate
- Handles multiple tool calls in one AIMessage
- Automatically creates `ToolMessage` with the correct `tool_call_id`
- Error handling if a tool raises an exception

### 4. Report Node (`_make_report_node`)

**What it does:** Synthesizes the full investigation into a structured `ReconReport`.

```python
report_model = model.with_structured_output(ReconReport)
# ...
report = report_model.invoke(messages_for_report)
return {"report": report}
```

**How `.with_structured_output(ReconReport)` works:**
1. LangChain converts the Pydantic model → JSON Schema
2. The schema is passed to the LLM as a function definition
3. The LLM produces JSON conforming to the schema
4. LangChain parses the JSON → `ReconReport` Pydantic object
5. Pydantic validates all fields (types, required, enums)

**Why a separate model configuration?**
- The LLM node uses `model.bind_tools(RECON_TOOLS)` → tool-calling config
- The report node uses `model.with_structured_output(ReconReport)` → structured output config
- You can't use both on the same model call — they're different invocation modes

**The report instruction trick:**
```python
report_instruction = HumanMessage(content="Based on your investigation above, produce your structured diagnosis report...")
messages_for_report = state["messages"] + [report_instruction]
```
This extra message is NOT added to graph state — it's appended to a local copy just for this one LLM call. It tells the LLM to shift from "investigator" mode to "report writer" mode.

---

## The Router — `_should_continue`

The conditional edge function that controls the ReAct loop:

```python
def _should_continue(state: dict) -> str:
    # 1. Safety valve first
    if iteration >= max_iterations:
        return "report"

    # 2. Did LLM request tools?
    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        return "tools"

    # 3. No tools → done investigating
    return "report"
```

**Why check max_iterations FIRST?**
Because a runaway agent could keep calling tools forever. The safety valve takes priority over everything. Even if the LLM wants to call more tools, we force report generation after N iterations.

**The return values ("tools" / "report") are edge names.** They map to the edge dict in `add_conditional_edges()`:
```python
graph.add_conditional_edges("llm", _should_continue, {
    "tools": "tools",
    "report": "report",
})
```

---

## The Closure Pattern — Why Factories?

The core design pattern in this file: each node function is created by a factory that captures dependencies:

```python
def _make_llm_node(model_with_tools):   # ← factory takes the dependency
    def llm_node(state: dict) -> dict:  # ← LangGraph calls this with (state)
        response = model_with_tools.invoke(...)  # ← captured in closure
        return {...}
    return llm_node
```

**Why not just use a global variable?**
- Globals are untestable — you can't run two graphs with different models
- Globals make dependencies invisible — you can't tell what a node needs

**Why not RunnableConfig?**
- LangGraph's `configurable` dict works but adds framework-specific complexity
- Closures are standard Python — nothing to learn beyond `def inside def`
- The dependency is explicit: `_make_llm_node(model)` — you see what's needed

**Why not a class with methods?**
- Would work fine, but adds ceremony (class, `self`, `__init__`) for no benefit
- Closures are lighter for stateless functions that just need a captured dependency

---

## Graph Assembly — `build_recon_graph`

The public API that wires everything together:

```python
def build_recon_graph(config: ArgusConfig):
    # 1. Create LLM from config
    llm = create_llm(config)

    # 2. Two model configurations
    model_with_tools = llm.bind_tools(RECON_TOOLS)  # for investigation
    # llm (plain) is used with .with_structured_output() for report

    # 3. Build nodes
    tool_node = ToolNode(RECON_TOOLS)

    # 4. Create graph
    graph = StateGraph(ReconState)
    graph.add_node("entry", _make_entry_node())
    graph.add_node("llm", _make_llm_node(model_with_tools))
    graph.add_node("tools", tool_node)
    graph.add_node("report", _make_report_node(llm))

    # 5. Wire edges
    graph.set_entry_point("entry")
    graph.add_edge("entry", "llm")
    graph.add_conditional_edges("llm", _should_continue, {...})
    graph.add_edge("tools", "llm")
    graph.add_edge("report", END)

    # 6. Compile and return
    return graph.compile()
```

**Why compile?** `.compile()` freezes the topology and returns a `CompiledStateGraph` — a runnable that validates the structure (no orphan nodes, no missing edges) and can be `.invoke()`'d with initial state.

---

## Full Execution Flow

Tracing one complete run through the graph:

```
build_recon_graph(config) → compiled graph

graph.invoke(make_initial_state(run_date="2026-09-28", gate_name="gate_3", ...))

1. ENTRY NODE
   ├── Renders RECON_PROMPT_TEMPLATE with run_date, gate_name, trigger_params
   ├── Returns: {"messages": [SystemMessage, HumanMessage]}
   └── State: messages=[sys, human], iteration=0

2. LLM NODE (iteration 1)
   ├── Sends [sys, human] to LLM
   ├── LLM responds: "I'll start by checking gate results" + tool_calls=[query_gate_results]
   ├── Returns: {"messages": [AIMessage], "iteration": 1}
   └── State: messages=[sys, human, ai], iteration=1

3. ROUTER: tool_calls exist → "tools"

4. TOOL NODE
   ├── Executes query_gate_results(run_date="2026-09-28")
   ├── Returns: {"messages": [ToolMessage(content="{...gate results...}")]}
   └── State: messages=[sys, human, ai, tool], iteration=1

5. LLM NODE (iteration 2)
   ├── Sends all 4 messages to LLM
   ├── LLM: "Gate 3 failed. Let me check row counts" + tool_calls=[compare_row_counts]
   ├── Returns: {"messages": [AIMessage], "iteration": 2}
   └── State: messages=[..., ai2], iteration=2

6. ROUTER: tool_calls exist → "tools"

7. TOOL NODE → executes compare_row_counts

... (more iterations as the agent investigates) ...

N. LLM NODE (iteration 5)
   ├── LLM: "I now have enough evidence" (no tool_calls)
   ├── Returns: {"messages": [AIMessage], "iteration": 5}

N+1. ROUTER: no tool_calls → "report"

N+2. REPORT NODE
   ├── Adds report instruction to message copy
   ├── Calls llm.with_structured_output(ReconReport).invoke(messages)
   ├── Returns: {"report": ReconReport(...)}
   └── State: report=ReconReport(gate_failed="gate_3", ...)

→ END
```

---

## Who Calls / Imports This File?

- **`agents/reconciliation/agent.py`** (upcoming) → calls `build_recon_graph(config)` to get the compiled graph, then `graph.invoke(make_initial_state(...))` to run it

---

## Where Does This File Fit?

```
argus/agents/reconciliation/
├── state.py             <── data schema for the graph
├── prompts.py           <── system/human prompts (become messages in state)
├── graph.py             <── YOU ARE HERE (builds and compiles the StateGraph)
└── agent.py             (upcoming — invokes the graph, reads the report)
```

---

## Key Concepts to Understand

1. **Two model configurations, one LLM** — `model.bind_tools()` and `model.with_structured_output()` are different invocation modes. You create both from the same base LLM, but they're used in different nodes for different purposes.

2. **ToolNode does the heavy lifting** — you never write `if tool_name == "query_gate_results": ...`. LangGraph's prebuilt `ToolNode` matches tool calls to functions, executes them, and wraps results in `ToolMessage`s.

3. **Closures are dependency injection** — node functions need the LLM but LangGraph calls them with only `(state)`. Closures capture the dependency in the function's scope. Factory → captures model → returns node function.

4. **The report instruction is transient** — it's appended to a copy of messages, not to graph state. The investigation history stays clean; only the report model sees the extra instruction.

5. **Compilation is validation** — `graph.compile()` checks the graph structure before you run it. If you forgot an edge or created an orphan node, compilation fails with a descriptive error.

6. **Graph ≠ Agent** — this file builds the graph (the execution engine). The agent class (upcoming `agent.py`) wraps the graph with platform concerns: creating `TriggerContext`, running the graph, extracting the report, building `AgentResult`.
