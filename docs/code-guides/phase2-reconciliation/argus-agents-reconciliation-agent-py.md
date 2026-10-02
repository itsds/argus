# Code Guide: `argus/agents/reconciliation/agent.py`

> **Read this AFTER reading the graph.py guide.**
> This guide explains how the agent class wraps the graph behind the platform interface.

---

## What Is This File About?

This is the **public interface** for the Reconciliation agent — a `BaseAgent` subclass that the platform (router, CLI, API) calls without knowing anything about LangGraph, tools, or prompts. It's the adapter between "agent internals" and "platform contract."

---

## What Feature Does It Bring to Argus?

1. **Platform integration** — implements the `BaseAgent` contract so the router can dispatch to it
2. **Context translation** — converts `TriggerContext` (platform language) to `make_initial_state()` (graph language)
3. **Result packaging** — extracts `ReconReport` from graph state and wraps it in `AgentResult`
4. **Error boundary** — catches graph failures and returns structured failure results instead of crashing
5. **Audit trail** — extracts tool call names from the message history for `actions_taken`

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`BaseAgent`** (ABC) | Abstract base class — agent contract the platform depends on |
| **`TriggerContext`** (Pydantic) | Inbound context from the trigger layer |
| **`AgentResult`** (Pydantic) | Outbound result envelope for the platform |
| **`build_recon_graph()`** | Graph builder from `graph.py` — returns compiled graph |
| **`make_initial_state()`** | State factory from `state.py` — translates context to graph seed |
| **`.model_dump()`** (Pydantic v2) | Converts `ReconReport` object to plain dict for `AgentResult.report` |

---

## The Five Steps of `invoke()` — Deep Dive

### Step 1: Lazy Graph Compilation

```python
if self._graph is None:
    self._graph = self.build_graph()
```

**Why lazy?** Building the graph is expensive — it creates the LLM client, binds tools, and compiles the graph topology. Doing it once on first `invoke()` and caching in `self._graph` avoids repeating that work.

**Why is reuse safe?** The compiled graph is stateless — all state lives in the `invoke()` call's input dict. Two concurrent invocations with different states don't interfere because they each get their own state copy flowing through the graph.

### Step 2: TriggerContext → Initial State

```python
gate_name = context.params.get("gate_failure", "unknown")
max_iterations = self.config.get("agents.reconciliation.max_iterations", 10)
initial_state = make_initial_state(
    run_date=context.run_date,
    gate_name=gate_name,
    trigger_params=context.params,
    correlation_id=context.correlation_id,
    max_iterations=max_iterations,
)
```

**Two different vocabularies:**
- The platform says: `TriggerContext(run_date="2026-09-28", params={"gate_failure": "gate_3"})`
- The graph says: `ReconState(run_date="2026-09-28", gate_name="gate_3", messages=[], iteration=0)`

This translation step bridges them. `gate_name` is extracted from `params["gate_failure"]`, and `max_iterations` comes from config (not hardcoded).

### Step 3: Run the Graph

```python
try:
    final_state = self._graph.invoke(initial_state)
except Exception as exc:
    return self._make_result(context=context, status="failure", ...)
```

**Why try/except?** The graph can fail for many reasons:
- LLM API errors (network, rate limits, invalid response)
- Tool execution errors (unexpected data formats)
- Structured output failures (LLM produces JSON that doesn't match ReconReport)

The agent catches ALL of these and returns a failure `AgentResult` rather than letting the exception propagate. The platform never has to handle raw exceptions from the graph.

### Step 4: Extract Results

```python
report = final_state.get("report")
errors = final_state.get("errors", [])
actions = _extract_tool_calls(final_state.get("messages", []))
```

The final state dict contains everything the graph accumulated:
- `report` — the `ReconReport` Pydantic object (or `None` if report node failed)
- `errors` — any errors nodes appended during execution
- `messages` — the full conversation history (mined for tool call names)

### Step 5: Package into AgentResult

```python
if report is not None:
    return self._make_result(
        status="success",
        report=report.model_dump(),  # Pydantic → dict
        actions=actions,
        errors=errors,
    )
```

**Why `.model_dump()`?** `AgentResult.report` is `dict[str, Any]`, not `ReconReport`. The platform layer is agent-type-agnostic — it doesn't import `ReconReport`. Converting to a plain dict keeps the boundary clean.

---

## The Audit Trail: `_extract_tool_calls()`

```python
def _extract_tool_calls(messages: list) -> list[str]:
    for msg in messages:
        if isinstance(msg, AIMessage) and hasattr(msg, "tool_calls"):
            for tc in msg.tool_calls:
                tool_names.append(tc["name"])
    return tool_names
```

This walks the full conversation history and collects every tool the LLM called, in order. The result looks like:
```python
["query_gate_results", "compare_row_counts", "check_duplicate_keys", "query_watermark_gaps"]
```

This is stored in `AgentResult.actions_taken` — useful for:
- **Audit logs** — which tools did the agent use for this investigation?
- **Debugging** — did the agent skip a critical tool?
- **Metrics** — average tool calls per investigation, most-used tools

---

## Error Handling Strategy

| Failure Mode | What Happens |
|---|---|
| Graph throws exception | Caught in try/except → failure AgentResult with error message |
| Report is `None` (report node failed) | Detected by `if report is not None` → failure AgentResult |
| Tools produce errors | Errors accumulate in `state["errors"]` via reducer → included in AgentResult |
| LLM hits max iterations | Router forces report generation → still a "success" (partial investigation) |

The key principle: **the agent ALWAYS returns an AgentResult**, never raises an exception. The platform can handle success/failure status uniformly.

---

## Who Calls / Imports This File?

- **`argus/core/router.py`** → imports `ReconciliationAgent` to register it in the agent registry
- **`argus/cli/main.py`** → creates `ReconciliationAgent(config)` when the CLI dispatches a recon command
- **`tests/agents/test_recon_agent.py`** (upcoming) → instantiates and invokes the agent with test contexts

---

## Where Does This File Fit?

```
argus/agents/reconciliation/
├── state.py             <── data schema for the graph
├── prompts.py           <── system/human prompts
├── graph.py             <── builds and compiles the StateGraph
└── agent.py             <── YOU ARE HERE (wraps graph behind BaseAgent)
```

This completes the Recon agent's assembly chain:
```
tools → state → prompts → graph → agent
(what)   (data)  (brain)  (wiring) (interface)
```

---

## Key Concepts to Understand

1. **Adapter pattern** — agent.py adapts the graph's internal API (ReconState, ReconReport) to the platform's external API (TriggerContext, AgentResult). Neither side knows about the other's types.

2. **Lazy initialization** — `build_graph()` is called once on first `invoke()`. The compiled graph is cached because it's stateless (state lives in the invoke call, not the graph object).

3. **Error boundary** — the agent never propagates raw exceptions. Every failure mode produces a structured `AgentResult(status="failure")` that the platform can handle uniformly.

4. **`.model_dump()` at the boundary** — Pydantic objects are converted to dicts at the agent/platform boundary. Inside the agent, we use rich Pydantic types (ReconReport). Outside, the platform sees plain dicts. This keeps the platform agent-type-agnostic.

5. **Actions as audit trail** — `_extract_tool_calls()` mines the message history for what the agent actually did. This is more reliable than having nodes log their actions, because the message history is the single source of truth for the ReAct conversation.
