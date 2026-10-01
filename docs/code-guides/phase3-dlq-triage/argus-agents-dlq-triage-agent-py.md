# Code Guide: `argus/agents/dlq_triage/agent.py`

> **Read this BEFORE opening `argus/agents/dlq_triage/agent.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **public interface for the DLQ Triage agent** — the adapter between the agent's internals (LangGraph, tools, prompts) and the Argus platform contract (`BaseAgent`). The platform (router, CLI, API) calls `agent.invoke(context)` without knowing anything about LangGraph, classification rubrics, or DLQ records.

---

## What Feature Does It Bring to Argus?

1. **Platform-compatible DLQ triage** — the router can dispatch to this agent the same way it dispatches to Recon
2. **Lazy graph compilation** — the graph is built on first `invoke()`, not at import time
3. **Context translation** — converts generic `TriggerContext` into DLQ-specific `make_initial_state()`
4. **Error boundary** — catches graph failures and returns structured `AgentResult` regardless

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`BaseAgent`** (abstract class) | Defines the contract: `name`, `description`, `build_graph()`, `invoke()` |
| **`TriggerContext`** | Platform-level input (agent_name, run_date, params, correlation_id) |
| **`AgentResult`** | Platform-level output (status, report dict, actions, errors, timestamps) |
| **`.model_dump()`** (Pydantic) | Converts `DLQTriageReport` to a plain dict for `AgentResult.report` |
| **Lazy initialization** | `self._graph is None` check → build on first use, cache |

---

## The Five-Step Pattern (Same as Recon)

Every Argus agent follows this sequence in `invoke()`:

```
Step 1: LAZY BUILD   — if self._graph is None: self._graph = self.build_graph()
Step 2: TRANSLATE     — TriggerContext → make_initial_state(...)
Step 3: RUN           — final_state = self._graph.invoke(initial_state)
Step 4: EXTRACT       — report = final_state.get("report")
Step 5: PACKAGE       — return AgentResult(status, report.model_dump(), ...)
```

If you were building a third agent, you'd follow this exact pattern. The only things that change per agent are the state factory, the graph builder, and the report model.

---

## What's Different from ReconciliationAgent

### Context Translation

```python
# Recon: gate_name from params
gate_name = context.params.get("gate_name", "unknown")

# DLQ: source_lane from params (default "both")
source_lane = context.params.get("source_lane", "both")
```

The default matters: `"both"` means the agent checks both DLQ lanes when the trigger doesn't specify. This is safer than defaulting to a single lane — you don't miss records.

### Config Path

```python
# Recon
config.get("agents.reconciliation.max_iterations", ...)

# DLQ
config.get("agents.dlq_triage.max_iterations", ...)
```

Both fall back to `agents.max_iterations` (the global default).

### Report Logging

```python
# Recon logs: root_cause, confidence, severity
# DLQ logs: total_records, auto_requeued, quarantined, escalated, severity
```

The logged fields reflect what each agent's report contains.

---

## The `_extract_tool_calls` Helper

Walks the message history and collects tool names from `AIMessage.tool_calls`:

```python
def _extract_tool_calls(messages: list) -> list[str]:
    tool_names = []
    for msg in messages:
        if isinstance(msg, AIMessage) and hasattr(msg, "tool_calls"):
            for tc in msg.tool_calls:
                tool_names.append(tc["name"])
    return tool_names
```

This is identical to the Recon version because it only depends on the LangChain message format, not the specific tools used. It populates `AgentResult.actions_taken` for the audit trail.

---

## Error Handling

The `try/except` around `graph.invoke()` catches:
- LLM API errors (rate limits, auth failures)
- Tool execution failures
- State validation errors
- Structured output parsing failures

On failure, the agent returns `AgentResult(status="failure", report={"error": ...})` — the platform always gets a structured response, never an unhandled exception.

---

## Key Insight: The Adapter Pattern

```
Platform                    Agent Internals
────────                    ───────────────
TriggerContext ──►          make_initial_state()
                            graph.invoke()
AgentResult    ◄──          final_state["report"].model_dump()
```

The platform sees `TriggerContext` in, `AgentResult` out. It doesn't know about:
- LangGraph or StateGraph
- ReAct loops or tool calls
- DLQ records, classifications, or requeue safety
- Pydantic report models

This separation means you can swap the agent's internals (different LLM, different graph topology, different tools) without changing the platform code. The contract is stable; the implementation evolves.

---

## How This Connects

- **`base.py`** defines `BaseAgent`, `TriggerContext`, `AgentResult` — the contract
- **`graph.py`** provides `build_dlq_graph()` — called by `build_graph()`
- **`state.py`** provides `make_initial_state()` — called in Step 2
- **`router.py`** dispatches `TriggerContext` to this agent by name
- **`cli/main.py`** creates a `TriggerContext` from CLI args and calls `invoke()`
