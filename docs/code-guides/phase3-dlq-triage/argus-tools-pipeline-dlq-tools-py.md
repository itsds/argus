# Code Guide: `argus/tools/pipeline/dlq_tools.py`

> **Read this BEFORE opening `argus/tools/pipeline/dlq_tools.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **toolbox for the DLQ Triage agent** — three LangChain tools that read from dead-letter queues, cross-reference schema changes, and requeue transient failures. In dev mode, these tools return simulated DLQ records representing realistic failure scenarios. In production, they'd query real Kafka DLQ topics and Iceberg quarantine tables.

---

## What Feature Does It Bring to Argus?

1. **Classification evidence** — `read_dlq_records` provides the error metadata the LLM needs to classify each record
2. **Cross-reference capability** — `query_schema_changelog` lets the LLM confirm schema mismatch hypotheses with evidence
3. **Guarded side effects** — `requeue_message` is the first Argus tool that CHANGES pipeline state, with three safety layers

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`@tool` decorator** (LangChain) | Converts Python functions into tools the LLM can call |
| **Idempotency set** (`_REQUEUED_RECORDS`) | Prevents double-requeue across ReAct iterations |
| **Safety limit** (`_MAX_REQUEUE_PER_INVOCATION`) | Caps total requeues at 10 per agent run |
| **Module-level state** | Simulated data stores + safety counters persist across tool calls within one run |
| **`json.dumps()`** | All tools return JSON strings (LangChain convention) |
| **`DLQ_TOOLS` list** | Registry passed to `model.bind_tools()` |

---

## The Three Tools

### 1. `read_dlq_records(run_date, source_lane)` — START HERE
**Always use first.** Returns DLQ records from either `kafka_dlq` (Benefit lane) or `bad_files` (Booking lane). Each record includes `error_class`, `error_message`, and `payload_summary` — the evidence the LLM needs for classification.

### 2. `query_schema_changelog(entity)` — CROSS-REFERENCE
**Use when you suspect SCHEMA_MISMATCH.** Returns recent schema evolution events for "benefit" or "booking" entities. A `pending_consumer_update` or `pending_table_migration` status confirms that DLQ records with schema-related errors are genuine mismatches.

### 3. `requeue_message(record_id, source_lane, reason)` — SIDE EFFECT
**Use ONLY for TRANSIENT records with confidence >= 0.80.** This tool changes pipeline state — it puts a message back into the processing queue. Three safety layers protect against misuse:

- **Idempotency guard**: `_REQUEUED_RECORDS` set prevents double-requeue
- **Safety limit**: `_MAX_REQUEUE_PER_INVOCATION = 10` caps total requeues
- **Audit trail**: Returns a confirmation string logged in `requeue_audit` state

---

## Simulated Data

### Kafka DLQ Records (2026-09-30)

6 records covering all 4 classification categories:

| Record ID | Error | Expected Classification |
|---|---|---|
| dlq-kafka-001 | TimeoutException | TRANSIENT (requeue) |
| dlq-kafka-002 | TimeoutException | TRANSIENT (requeue) |
| dlq-kafka-003 | SchemaRegistryException | SCHEMA_MISMATCH |
| dlq-kafka-004 | SchemaRegistryException | SCHEMA_MISMATCH |
| dlq-kafka-005 | NullPointerException | DATA_QUALITY |
| dlq-kafka-006 | UnknownProcessingException | UNKNOWN |

### Bad Files Records (2026-09-29)

3 records from the Booking lane:

| Record ID | Error | Expected Classification |
|---|---|---|
| dlq-bf-001 | CorruptFileException | TRANSIENT (re-fetch) |
| dlq-bf-002 | Column type mismatch | SCHEMA_MISMATCH |
| dlq-bf-003 | DQ check failures | DATA_QUALITY |

### Schema Changelog

Two entities with recent changes:
- **benefit**: v3→v4, `pending_consumer_update` (confirms Kafka schema errors)
- **booking**: v4→v5, `pending_table_migration` (confirms bad_files type errors)

---

## Key Concept: Defense in Depth for Side Effects

The `requeue_message` tool has safety at four layers:

```
Layer 1: PROMPT — "NEVER requeue SCHEMA_MISMATCH" (prevents the attempt)
Layer 2: IDEMPOTENCY — _REQUEUED_RECORDS set (prevents double-requeue)
Layer 3: RATE LIMIT — max 10 per invocation (prevents runaway requeuing)
Layer 4: AUDIT TRAIL — logged in requeue_audit state (makes everything visible)
```

No single layer is sufficient. The prompt might fail to prevent a bad requeue attempt. The idempotency guard only helps on retries. The rate limit only helps with volume. Together, they cover each other's gaps.

---

## How This Connects

- **System prompt** (`prompts.py`) tells the LLM WHEN to use each tool
- **Tool docstrings** tell the LLM WHAT the tool does and HOW to interpret results
- **State schema** (`state.py`) has `requeue_audit` to track side effects
- **Report model** (`schemas/reports.py`) has `auto_requeued` count from the audit trail
