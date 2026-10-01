# Code Guide: `argus/agents/dlq_triage/graph.py`

> **Read this BEFORE opening `argus/agents/dlq_triage/graph.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **LangGraph StateGraph wiring for the DLQ Triage agent** — it connects the entry, LLM, tool, and report nodes into a ReAct loop. Comparing it with the Recon graph teaches a crucial insight: the **graph topology is reusable** even when the task is completely different.

---

## What Feature Does It Bring to Argus?

1. **ReAct loop for DLQ triage** — the same Reason→Act→Observe loop, but with DLQ-specific tools and prompts
2. **Pattern validation** — proves the ReAct topology is a reusable pattern, not a one-off implementation
3. **Structured triage report** — the report node uses `.with_structured_output(DLQTriageReport)` to produce typed output with per-record classifications

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`StateGraph`** (LangGraph) | Defines the graph structure with named nodes and edges |
| **`ToolNode`** (LangGraph prebuilt) | Executes tool calls from AIMessage.tool_calls |
| **`.bind_tools()`** | Gives the LLM access to DLQ_TOOLS |
| **`.with_structured_output()`** | Constrains report output to DLQTriageReport schema |
| **Closure factories** | `_make_entry_node()`, `_make_llm_node()`, `_make_report_node()` for DI |
| **Conditional edges** | `_should_continue` routes between tools and report |

---

## The ReAct Topology (Same as Recon)

```
entry ──► llm ──► should_continue? ──► tools ──► (back to llm)
                       │
                       ▼
                    report ──► END
```

This is identical to the Reconciliation graph. The TOPOLOGY is the same — only the COMPONENTS are swapped:

| Component | Recon Graph | DLQ Graph |
|---|---|---|
| State | `ReconState` | `DLQTriageState` |
| Tools | `RECON_TOOLS` (6 tools) | `DLQ_TOOLS` (3 tools) |
| Prompt | `RECON_PROMPT_TEMPLATE` | `DLQ_PROMPT_TEMPLATE` |
| Report model | `ReconReport` | `DLQTriageReport` |
| Report instruction | Root cause analysis | Classification summaries + requeue audit |

---

## The Four Nodes

### 1. Entry Node (`_make_entry_node`)

Seeds the conversation with SystemMessage + HumanMessage from `DLQ_PROMPT_TEMPLATE`. Same pattern as Recon, but uses `source_lane` instead of `gate_name`.

### 2. LLM Node (`_make_llm_node`)

The REASON step — sends full message history to the LLM. Identical structure to Recon. The LLM's *behavior* is different because it has different tools bound and a different system prompt, but the node *logic* is the same.

### 3. Tool Node (`ToolNode(DLQ_TOOLS)`)

The ACT step — executes tool calls. Uses LangGraph's prebuilt `ToolNode`, which reads `AIMessage.tool_calls` and runs the matching function. The tool node doesn't know or care about classification — that's the LLM's job.

### 4. Report Node (`_make_report_node`)

Synthesizes the conversation into a `DLQTriageReport`. The key difference from Recon is the report instruction, which asks for:
- Per-record classifications with confidence scores
- Requeue counts and audit trail
- Quarantine and escalation counts
- Severity based on classification distribution

---

## The Router: `_should_continue`

Same logic as Recon — three checks in order:

```python
1. iteration >= max_iterations? → "report" (safety valve)
2. last_message has tool_calls? → "tools" (continue ReAct)
3. no tool calls? → "report" (agent is done reasoning)
```

No DLQ-specific routing needed. Classification and requeue decisions happen INSIDE the LLM's reasoning (guided by the system prompt), not in the graph topology.

---

## Key Insight: Topology vs Content

This file proves that the ReAct loop is a **pattern**, not a one-off implementation. You could extract this into a reusable factory:

```python
def build_react_graph(state_class, tools, prompt_template, report_model, config):
    """Generic ReAct graph builder — same topology, swappable components."""
    ...
```

We're NOT doing this abstraction yet (premature abstraction is worse than duplication for learning). But seeing two concrete graphs with the same topology teaches you the pattern intuitively before abstracting it. By Phase 6, this refactoring becomes obvious.

---

## Closure Pattern for Dependency Injection

Each node is built by a factory function that captures its dependencies in a closure:

```python
def _make_llm_node(model_with_tools):
    def llm_node(state: dict) -> dict:
        response = model_with_tools.invoke(state["messages"])
        return {"messages": [response], "iteration": state["iteration"] + 1}
    return llm_node
```

Why closures over classes? LangGraph nodes must be callables that take state and return state updates. A closure captures the model reference without polluting the state dict. Same reason as Recon — consistency across agents.

---

## The Report Instruction

The report node appends a `HumanMessage` with detailed instructions for what the structured output should contain:

```
- total_records: total number of DLQ records examined
- records: list of EVERY record with classification, confidence, reason, action
- auto_requeued: count of requeued records
- quarantined: count of quarantined records
- escalated: count of escalated records
- summary: one-paragraph summary
- recommended_severity: P1/P2/P3 based on classification distribution
```

This instruction is more detailed than Recon's because the DLQ report has per-record breakdowns that require explicit guidance.

---

## How This Connects

- **`state.py`** defines `DLQTriageState` — the channels this graph tracks
- **`prompts.py`** provides the system prompt and human template
- **`dlq_tools.py`** provides `DLQ_TOOLS` bound to the LLM node
- **`agent.py`** calls `build_dlq_graph(config)` and invokes the compiled graph
- **`reports.py`** defines `DLQTriageReport` for structured output
