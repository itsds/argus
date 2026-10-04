# Code Guide: `argus/tools/pipeline/backfill_tools.py`

> **Read this BEFORE opening `argus/tools/pipeline/backfill_tools.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **toolbox for the Incident & Backfill Planning agent** — eight LangChain tools split into two registries: five read-only **investigation tools** for understanding an incident, and three **execution tools** for carrying out an approved backfill plan. In dev mode, all tools return simulated data representing realistic pipeline incidents. In production, they'd query real Iceberg catalogs, Airflow DAGs, watermark tables, and pipeline lock services.

---

## What Feature Does It Bring to Argus?

1. **Incident investigation** — five tools give the LLM full context: what failed, what data is damaged, whether locks are held, how long past backfills took, and whether source data is ready
2. **Safe backfill execution** — three tools enforce Lock → Execute → Release ordering with exclusive pipeline locks, per-step audit trails, and safety limits
3. **Two-registry tool separation** — investigation tools and execution tools are exported as separate lists, so the graph can bind the right tools to the right phase (preventing the LLM from executing during investigation)

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`@tool` decorator** (LangChain) | Converts Python functions into tools the LLM can call |
| **Two tool registries** | `BACKFILL_INVESTIGATION_TOOLS` vs `BACKFILL_EXECUTION_TOOLS` — phase-gated access |
| **Module-level execution state** | `_ACTIVE_LOCKS`, `_EXECUTION_AUDIT`, `_MAX_STEPS_PER_INVOCATION` |
| **Exclusive pipeline locks** | Ownership-tracked, timeout-based auto-release, idempotent acquire |
| **Per-step audit trail** | Every `execute_backfill_step` call is logged with timestamp and details |
| **`json.dumps()`** | All tools return JSON strings (LangChain convention) |
| **`BACKFILL_TOOLS` combined list** | Full registry for contexts that need all tools |

---

## The Two Registries — Why Split?

This is the biggest design difference from Phases 2 and 3. The Recon and DLQ agents had a single `RECON_TOOLS` or `DLQ_TOOLS` list because their graphs had one phase (investigate → report). The Backfill agent has **two phases** with different risk profiles:

```
Phase 1: INVESTIGATION (read-only, safe to run freely)
   → bind BACKFILL_INVESTIGATION_TOOLS to the LLM
   → LLM can call these tools in a ReAct loop

Phase 2: EXECUTION (side effects, requires approved plan)
   → bind BACKFILL_EXECUTION_TOOLS to the LLM
   → LLM can only reach these AFTER human approval
```

If the LLM had access to execution tools during investigation, it might try to "fix things" before presenting a plan. The registry split enforces the plan-then-execute pattern at the tool-binding level — the LLM literally cannot call `execute_backfill_step` during investigation because it's not in the bound tool set.

---

## Investigation Tools (Read-Only)

### 1. `get_incident_context(run_date)` — START HERE
**Always use first.** Returns the full incident picture: which gate failed, which entity and layers are affected, the DAG run status, and a timeline of events. This orients the LLM before it dives into details.

### 2. `assess_data_gaps(run_date, entity)` — QUANTIFY THE DAMAGE
**Use after incident context.** Returns per-layer gap analysis with gap types (`partition_missing`, `row_count_mismatch`, `fk_lookup_failure`), expected vs actual counts, and affected partitions. This is what drives the backfill plan — each gap becomes one or more backfill steps.

### 3. `check_pipeline_locks(entity)` — SAFETY GATE
**Use before planning.** Checks whether any pipeline locks are currently held. If a lock is active, the agent should NOT propose a backfill plan until the lock is released — two concurrent backfills on the same entity would corrupt data.

### 4. `get_backfill_history(entity)` — DURATION ESTIMATES
**Use for planning accuracy.** Returns recent backfill history with actual durations and outcomes. The LLM uses this to estimate how long the proposed backfill will take — a Silver-only backfill that took 25 minutes last time gives a realistic baseline.

### 5. `validate_source_readiness(run_date, entity)` — PRE-FLIGHT CHECK
**Use before finalizing the plan.** Verifies that all source data is available: Iceberg snapshots exist, Kafka offsets are committed, dimension tables are up-to-date. A backfill plan is useless if the source data isn't there to backfill from.

---

## Execution Tools (Side Effects)

### 6. `acquire_pipeline_lock(entity, layers, reason)` — LOCK FIRST
**Always execute first in the execution phase.** Acquires an exclusive lock on the specified entity and layers. Safety features:

- **Idempotent**: Re-acquiring an existing lock by the same owner succeeds silently
- **Ownership tracking**: Locks record who acquired them and when
- **Timeout-based auto-release**: Locks expire after 60 minutes to prevent deadlocks
- **Conflict detection**: Returns error if another owner holds the lock

### 7. `execute_backfill_step(step_order, entity, layer, partition, ...)` — THEN EXECUTE
**Use only after lock is acquired.** Executes a single backfill step (one layer, one partition). Safety features:

- **Lock check**: Refuses to execute if no lock is held for the entity
- **Step limit**: `_MAX_STEPS_PER_INVOCATION = 20` prevents runaway execution
- **Per-step audit**: Every step is logged with timestamp, details, and result
- **Ordered execution**: `step_order` parameter ensures steps run in the planned sequence

### 8. `release_pipeline_lock(entity, layers)` — RELEASE LAST
**Always execute last.** Releases the pipeline lock and returns the full execution audit trail. The audit trail shows every step that was executed, when, and whether it succeeded — this goes into the final report.

---

## The Lock → Execute → Release Pattern

```
acquire_pipeline_lock("booking", ["silver", "gold"], "backfill for 2026-09-28")
    │
    ▼
execute_backfill_step(1, "booking", "silver", "2026-09-28", ...)
execute_backfill_step(2, "booking", "gold", "2026-09-28", ...)
    │
    ▼
release_pipeline_lock("booking", ["silver", "gold"])
    → Returns: full audit trail of all steps executed
```

This pattern is enforced by tool-level checks:
- `execute_backfill_step` checks `_ACTIVE_LOCKS` — no lock = refused
- `release_pipeline_lock` returns the audit trail from `_EXECUTION_AUDIT`
- The step limit prevents the LLM from running more than 20 steps even with a valid lock

---

## Simulated Data

### Scenario 1: 2026-09-28 (Gate 3 Failure — Silver + Gold Backfill)

| Aspect | Details |
|---|---|
| **Trigger** | Gate 3 count mismatch: Bronze 15,012 vs Silver 14,712 |
| **Root cause** | 300 duplicate booking_ids from upstream re-delivery |
| **Affected layers** | Silver (row_count_mismatch), Gold (partition_missing — never ran) |
| **Backfill strategy** | Deduplicate Silver from Bronze snapshot, then rebuild Gold |
| **Source readiness** | Bronze snapshot available, Kafka offsets committed |

### Scenario 2: 2026-09-27 (Gate 4 Failure — Gold-Only Backfill)

| Aspect | Details |
|---|---|
| **Trigger** | Gate 4 FK integrity failure: 15 NULL card_sk in FACT_TRAVEL_TAG |
| **Root cause** | DIM_CARD refresh failed, new cards missing from dimension |
| **Affected layers** | Gold only (fk_lookup_failure) — Silver is correct |
| **Backfill strategy** | Re-run Gold after DIM_CARD refresh completes |
| **Source readiness** | DIM_CARD has pending refresh (blocks backfill until ready) |

These two scenarios exercise different backfill strategies:
- **Scenario 1** requires multi-layer backfill (Silver then Gold, in order)
- **Scenario 2** requires single-layer backfill but has a dependency gate (DIM_CARD must refresh first)

---

## Key Concept: Two-Registry Tool Separation

Compare with Phase 3's approach:

```
Phase 3 (DLQ):     DLQ_TOOLS = [read_dlq, query_schema, requeue_message]
                    ↑ All tools in one registry, safety via prompt rules

Phase 4 (Backfill): BACKFILL_INVESTIGATION_TOOLS = [5 read-only tools]
                    BACKFILL_EXECUTION_TOOLS = [3 side-effect tools]
                    ↑ Separate registries, safety via tool-binding phase gates
```

Phase 3's `requeue_message` relied on prompt-level guardrails ("only requeue TRANSIENT with confidence >= 0.80") plus tool-level safety (idempotency, rate limit). Phase 4 adds a stronger layer: the execution tools literally aren't available to the LLM during investigation. The graph controls which registry is bound at each phase.

---

## Module-Level State

```python
_ACTIVE_LOCKS: dict[str, dict] = {}    # entity → lock info (owner, timestamp, layers)
_EXECUTION_AUDIT: list[dict] = []       # every step logged with timestamp and result
_MAX_STEPS_PER_INVOCATION = 20          # hard cap on steps per agent run
```

This mirrors the `_REQUEUED_RECORDS` pattern from Phase 3's `dlq_tools.py`, but scaled up for multi-step execution. The audit trail is especially important — when the lock is released, the full audit goes into the agent's report so humans can verify exactly what was executed.

---

## How This Connects

- **State schema** (`state.py`, next file) will have `plan`, `approval_status`, and `revision_feedback` fields for the HITL approval loop
- **System prompt** (`prompts.py`) will tell the LLM WHEN to use each tool and HOW to interpret results
- **Graph topology** (`graph.py`) will bind investigation tools to the investigation phase and execution tools to the execution phase
- **Report model** (`schemas/reports.py`) already has `BackfillPlan` and `BackfillStep` — the structured output the LLM produces after investigation
