# Code Guide: `argus/tools/pipeline/recon_tools.py`

> **Read this BEFORE opening `argus/tools/pipeline/recon_tools.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **toolbox for the Reconciliation Diagnostics agent** — six LangChain tools that query TTAG pipeline infrastructure to investigate why a Gate 3 or Gate 4 reconciliation check failed. In dev mode, these tools return simulated data representing realistic failure scenarios. In production, they'd query real infrastructure (Iceberg, Snowflake, Airflow).

---

## What Feature Does It Bring to Argus?

1. **Agent investigation capabilities** — the Recon agent uses these tools in a ReAct loop (reason → act → observe → reason) to gather evidence
2. **Realistic failure simulation** — two fully modeled failure scenarios let you develop and test the agent without real infrastructure
3. **Prompt engineering through docstrings** — each tool's docstring tells the LLM WHEN to use it, WHAT to look for in results, and HOW to interpret the output

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`@tool` decorator** (LangChain) | Converts a Python function into a LangChain Tool object — the LLM can call it by name |
| **Tool docstrings** | Act as prompt engineering — the LLM reads docstrings to decide which tool to use and how |
| **Type hints** | Guide the LLM on what arguments to pass (`run_date: str`, `tables: list[str]`) |
| **`json.dumps()`** | All tools return JSON strings (LangChain convention for tool output) |
| **`RECON_TOOLS` list** | A registry of all tools — passed to `model.bind_tools()` to give the LLM access |

---

## The Six Tools

### 1. `query_gate_results(run_date)` — START HERE
**Use first.** Returns which gate failed, what counts it compared, and the error message. This orients the entire investigation.

### 2. `compare_row_counts(run_date, tables)` — TRACE THE DROP
**Use second.** Compare row counts across pipeline layers to find where rows were lost or gained.

### 3. `check_duplicate_keys(run_date, table)` — EXPLAIN BRONZE→SILVER DROP
**Use when** Bronze > Silver. Checks for duplicate natural keys in Silver — indicates upstream re-delivery.

### 4. `check_fk_integrity(run_date)` — EXPLAIN SILVER→GOLD DROP
**Use when** investigating Gate 4 failures. Checks for NULL surrogate keys in the Gold fact table — indicates missing dimension rows.

### 5. `query_watermark_gaps(run_date)` — ALWAYS CHECK BEFORE CONCLUDING
**Always use.** Checks whether all layers processed the same data range. A stale watermark explains count mismatches even when data is correct.

### 6. `query_iceberg_snapshots(run_date, table)` — DEEP DIVE
**Use last.** Examines Iceberg snapshot history — write operations, row counts, timestamps. Catches unexpected overwrites or partial writes.

---

## The Two Simulated Failure Scenarios

### Scenario 1: Gate 3 Failure (2026-09-28)

**Root cause**: Duplicate `booking_id`s in Silver + stale Silver watermark.

Evidence chain:
```
Gate 3 FAILED
  → Bronze booking: 15,012 rows, Silver booking: 14,712 rows (300 dropped!)
  → Wait — 111 booking_ids are duplicated in Silver (re-delivery from upstream)
  → Silver watermark is STALE (snapshot 5765432190 vs Bronze's 5765432198)
  → Silver job processed an older snapshot — it missed the latest Bronze data
```

### Scenario 2: Gate 4 Failure (2026-09-27)

**Root cause**: 15 missing DIM_CARD rows → NULL `card_sk` in FACT_TRAVEL_TAG.

Evidence chain:
```
Gate 4 FAILED: Snowflake has 15,087 rows, Gold Iceberg has 15,102 (15 missing)
  → FK check: 15 rows have NULL card_sk (cards CARD-88712, CARD-88713, CARD-91004)
  → These cards exist in Silver but missing from DIM_CARD
  → Dimension refresh task may have failed, or account-management pipeline delayed
  → Note: 2,341 NULL benefit_sk is EXPECTED (by design, not a defect)
```

---

## Code Flow

```
Agent (ReAct loop)
    │
    ├── "I should check the gate results first"
    │   └── calls query_gate_results("2026-09-28")
    │       └── returns JSON: gate_3 FAILED, booking Silver 14823
    │
    ├── "Let me compare row counts across layers"
    │   └── calls compare_row_counts("2026-09-28", ["bronze.booking_raw", "silver.booking_detail"])
    │       └── returns JSON: Bronze 15012, Silver 14712
    │
    ├── "Bronze > Silver — checking for duplicates"
    │   └── calls check_duplicate_keys("2026-09-28", "silver.booking_detail")
    │       └── returns JSON: 111 duplicate booking_ids
    │
    ├── "Let me also check watermarks"
    │   └── calls query_watermark_gaps("2026-09-28")
    │       └── returns JSON: Silver watermark STALE
    │
    └── "Root cause: stale watermark + duplicate re-delivery"
        └── produces ReconReport
```

---

## Key Design Pattern: Three Layers of Tool Prompting

This file demonstrates a critical agent engineering pattern — tools communicate with the LLM through three layers:

```
Layer 1: Function signature
    → Tool name + type hints tell the LLM WHAT it can call
    → query_gate_results(run_date: str) → str

Layer 2: Docstring
    → Tells the LLM WHEN to use the tool, HOW to interpret results
    → "Use this FIRST when investigating a reconciliation failure"

Layer 3: Return value
    → Structured data that feeds the LLM's next reasoning step
    → JSON with counts, messages, notes, explanations
```

The **docstrings in this file are not documentation for humans** — they are **prompt engineering for the LLM**. The LLM reads them to decide which tool to call and in what order. This is why phrases like "Use this FIRST", "Look for:", "This is your starting point" appear in the docstrings.

---

## Who Calls / Imports This File?

- **`agents/reconciliation/graph.py`** (upcoming) → imports `RECON_TOOLS` to bind to the LLM
- **`agents/reconciliation/agent.py`** (upcoming) → `model.bind_tools(RECON_TOOLS)` gives the LLM tool access
- **The LLM itself** → calls these functions by name during the ReAct loop (LangChain routes tool calls)

---

## Where Does This File Fit?

```
argus/tools/
└── pipeline/
    └── recon_tools.py   <── YOU ARE HERE (Recon agent's toolbox)

Consumed by:
argus/agents/reconciliation/
├── graph.py             <── binds RECON_TOOLS to the LLM node
├── state.py             <── tool results → ToolMessage → messages list
└── prompts.py           <── system prompt references these tools by name
```

---

## Key Concepts to Understand

1. **`@tool` decorator**: LangChain's `@tool` converts a plain function into a `Tool` object with:
   - A `name` (the function name)
   - A `description` (the docstring — THIS IS WHAT THE LLM READS)
   - An `args_schema` (auto-generated from type hints)
   - A `func` (the original function)

2. **All returns are `str`**: LangChain tools must return strings because the output goes into a `ToolMessage` in the conversation history. JSON-encoded strings are the standard — they're structured enough for the LLM to parse, but they're still just text in the message stream.

3. **Simulated data pattern**: The `_SIMULATED_*` dicts at the top act as an in-memory database. Each tool queries this "database" by `run_date`. In production, these would be replaced by actual SQL/API calls (Spark SQL for Iceberg, Snowflake JDBC, Airflow REST API).

4. **`RECON_TOOLS` list**: This is the tool registry — a list that gets passed to `model.bind_tools()`. The LLM can only call tools that are in this list. Adding a new tool means: (1) write the `@tool` function, (2) add it to `RECON_TOOLS`, (3) mention it in the system prompt.

5. **Tool docstring ↔ system prompt synergy**: The system prompt in `prompts.py` says "Follow this sequence: 1. START with query_gate_results..." and the tool docstring says "Use this FIRST...". They reinforce each other — the system prompt gives the strategy, the tool docstring gives the specifics.
