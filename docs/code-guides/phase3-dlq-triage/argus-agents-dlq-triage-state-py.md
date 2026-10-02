# Code Guide: `argus/agents/dlq_triage/state.py`

> **Read this BEFORE opening `argus/agents/dlq_triage/state.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **state schema for the DLQ Triage agent** — it defines every piece of data the LangGraph StateGraph tracks as the agent runs. Comparing it with ReconState (Phase 2) teaches you how different agent TASKS require different state SHAPES, even when the underlying LangGraph mechanism is the same.

---

## What Feature Does It Bring to Argus?

1. **Classification accumulator** — the `classifications` field lets the agent build up a list of classified records incrementally across ReAct iterations
2. **Side-effect tracking** — the `requeue_audit` field tracks every requeue action for the final report's audit trail
3. **Dual-lane trigger** — `source_lane` replaces ReconState's `gate_name`, reflecting that DLQ alerts come from lane breaches, not gate failures
4. **Initial state factory** — `make_initial_state()` provides a single place to set defaults and validate required fields

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`Annotated[list[X], operator.add]`** | LangGraph reducer — accumulates items across iterations instead of overwriting |
| **Class annotations pattern** | LangGraph reads `__annotations__` to build state channels (no metaclass magic) |
| **`AnyMessage`** (LangChain) | Union type covering SystemMessage, HumanMessage, AIMessage, ToolMessage |
| **Pydantic models** (`DLQRecord`, `DLQTriageReport`) | Typed report objects the state carries |
| **Factory function** | `make_initial_state()` returns a plain dict matching the state shape |

---

## The State Fields

### Conversation backbone (same as ReconState)

| Field | Type | Reducer | Purpose |
|---|---|---|---|
| `messages` | `list[AnyMessage]` | `operator.add` | Full ReAct conversation history |
| `iteration` | `int` | overwrite | Current loop count |
| `max_iterations` | `int` | overwrite | Safety cap |
| `errors` | `list[str]` | `operator.add` | Error accumulator |
| `report` | `DLQTriageReport \| None` | overwrite | Final structured output |

### Trigger metadata (DLQ-specific)

| Field | Type | Purpose |
|---|---|---|
| `run_date` | `str` | Which date's DLQ records to examine |
| `source_lane` | `str` | `"kafka_dlq"`, `"bad_files"`, or `"both"` |
| `trigger_params` | `dict` | Full trigger context for logging |
| `correlation_id` | `str` | Log tracing ID |

### NEW in Phase 3

| Field | Type | Reducer | Purpose |
|---|---|---|---|
| `classifications` | `list[DLQRecord]` | `operator.add` | Records classified so far — builds up incrementally |
| `requeue_audit` | `list[str]` | `operator.add` | Every requeue action, for the audit trail |

---

## Key Concept: Same Mechanism, Different Shape

Both state schemas use `Annotated[list[X], operator.add]` to accumulate items. But the FIELDS are different because the TASKS are different:

```
ReconState                          DLQTriageState
──────────                          ──────────────
gate_name: str                      source_lane: str
(no classification accumulator)     classifications: list[DLQRecord]
(no side-effect tracking)           requeue_audit: list[str]
report: ReconReport                 report: DLQTriageReport
```

The LangGraph framework is generic — the state schema encodes your agent's specific workflow requirements. This is a key insight for building more agents: start by asking "what does this agent NEED TO TRACK?" and the state schema writes itself.

---

## Key Concept: Incremental vs Monolithic

**ReconState** — monolithic: the agent investigates, then produces one report at the end. All findings live in the message history until the report node extracts them.

**DLQTriageState** — incremental: the agent classifies records one by one, potentially acting (requeuing) between classifications. The `classifications` accumulator lets each iteration add its classified records without losing previous ones.

This matters because the DLQ agent may need to ACT on a classification before it's done classifying everything. If transient records need requeuing, the agent requeues them as it goes — it doesn't wait until the end.

---

## The Factory Function

`make_initial_state()` returns a plain dict with all defaults set:

```python
{
    "messages": [],              # entry node seeds System + Human
    "run_date": "2026-09-30",
    "source_lane": "kafka_dlq",
    "trigger_params": {...},
    "correlation_id": "...",
    "iteration": 0,
    "max_iterations": 10,
    "classifications": [],       # starts empty, accumulates
    "requeue_audit": [],         # starts empty, accumulates
    "report": None,              # set by report node at the end
    "errors": [],
}
```

Why a factory instead of constructing inline? Single place for defaults, type-checks at the call site, easy to extend.

---

## How This Connects

- **`graph.py`** passes `DLQTriageState` to `StateGraph()` — defines the channels
- **`agent.py`** calls `make_initial_state()` to seed the graph
- **`prompts.py`** references `source_lane` and `run_date` in the human message template
- **Report model** (`schemas/reports.py`) defines `DLQRecord` and `DLQTriageReport`
