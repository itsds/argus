# Code Guide: `experiments/dlq_live_test.py`

> **Read this BEFORE opening `experiments/dlq_live_test.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **live integration test for the DLQ Triage agent** — it runs the full agent pipeline against a real LLM (Gemini free tier) with simulated DLQ data. Unlike unit tests, this verifies that the LLM actually understands the classification rubric, respects requeue safety rules, and produces correct triage reports.

---

## What Feature Does It Bring to Argus?

1. **End-to-end validation** — proves the full chain works: TriggerContext → agent → ReAct loop → DLQTriageReport
2. **Classification accuracy check** — shows whether the LLM classifies records correctly with appropriate confidence
3. **Requeue safety validation** — post-hoc check that only TRANSIENT records with confidence ≥ 0.80 were requeued
4. **Tool schema inspection** — `--show-tools` displays what the LLM sees when tools are bound

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`argparse`** | CLI interface: `--scenario kafka|bad_files|both`, `--show-tools` |
| **`asyncio`** | Entry point wraps `main()` in `asyncio.run()` |
| **ANSI escape codes** | Color-coded terminal output for classification categories |
| **`time.monotonic()`** | Measures wall-clock execution time per scenario |
| **Real LLM** (Gemini) | No mocking — actual API calls to test the full pipeline |

---

## The Two Scenarios

### Scenario 1: `kafka` — Benefit Lane Kafka DLQ

**6 records** on 2026-09-30, covering all 4 classification categories:

| Record | Error | Expected | Action |
|---|---|---|---|
| dlq-kafka-001 | TimeoutException | TRANSIENT | Requeue |
| dlq-kafka-002 | TimeoutException | TRANSIENT | Requeue |
| dlq-kafka-003 | SchemaRegistryException | SCHEMA_MISMATCH | Quarantine |
| dlq-kafka-004 | SchemaRegistryException | SCHEMA_MISMATCH | Quarantine |
| dlq-kafka-005 | NullPointerException | DATA_QUALITY | Quarantine |
| dlq-kafka-006 | UnknownProcessingException | UNKNOWN | Escalate |

### Scenario 2: `bad_files` — Booking Lane Iceberg Quarantine

**3 records** on 2026-09-29, covering 3 categories:

| Record | Error | Expected | Action |
|---|---|---|---|
| dlq-bf-001 | CorruptFileException | TRANSIENT | Requeue |
| dlq-bf-002 | Column type mismatch | SCHEMA_MISMATCH | Quarantine |
| dlq-bf-003 | DQ check failures | DATA_QUALITY | Quarantine |

---

## Classification Color Coding

The output uses ANSI colors to make classifications scannable at a glance:

| Category | Color | Why |
|---|---|---|
| TRANSIENT | 🟢 Green | Safe — can be auto-resolved |
| SCHEMA_MISMATCH | 🟡 Yellow | Warning — needs human fix (schema update) |
| DATA_QUALITY | 🟣 Magenta | Bad data — quarantine for review |
| UNKNOWN | 🔴 Red | Danger — needs on-call engineer |

---

## Requeue Safety Validation (Key Feature)

After each scenario, `_validate_requeue_safety()` performs a post-hoc check:

```python
for rec in requeued_records:
    is_transient = rec["classification"] == "TRANSIENT"
    is_confident = rec["confidence"] >= 0.80
    
    if is_transient and is_confident:
        print("✅ Safe to requeue")
    else:
        print("❌ SAFETY VIOLATION!")
```

### Why this matters

The `requeue_message` tool doesn't check classification — it happily requeues any record the LLM tells it to. The PROMPT is the first line of defense. This validation catches cases where the prompt failed to prevent unsafe requeues, which would mean:
- The classification rubric needs tightening
- The safety rules need stronger wording
- Or the LLM needs more few-shot examples

### What it catches

- **Wrong classification requeued**: LLM classified SCHEMA_MISMATCH as TRANSIENT and requeued it
- **Low-confidence requeue**: LLM classified TRANSIENT at 0.65 and requeued anyway (below 0.80 threshold)
- Both indicate the prompt needs improvement

---

## Output Structure

```
══════════════════════════════════════════════════════
  🔍 ARGUS PHASE 3 — DLQ Triage Agent Live Test
══════════════════════════════════════════════════════

── SETUP ──
  Loading config, LLM provider, model, max_iterations

── SCENARIO: Kafka DLQ — Benefit Lane ──
  ▶ Invoking DLQTriageAgent...

── RESULTS ──
  ✅ Status: success
  ⏱️  Duration: 12.3s
  
  ── Triage Report ──
  Total records: 6 | Requeued: 2 | Quarantined: 3 | Escalated: 1

  Per-Record Classifications (6):
    1. dlq-kafka-001 → TRANSIENT @ 0.90 (requeued)
    2. dlq-kafka-003 → SCHEMA_MISMATCH @ 0.95 (quarantined)
    ...

── REQUEUE SAFETY VALIDATION ──
  ✅ All requeues passed safety validation.

── SUMMARY ──
  ✅ Kafka DLQ: Tools: 5 | Severity: P2 | Requeued: 2
```

---

## Differences from `recon_live_test.py`

| Aspect | Recon Live Test | DLQ Live Test |
|---|---|---|
| Scenarios | 2 gate failures (gate_3, gate_4) | 2 DLQ lanes (kafka, bad_files) |
| Report format | Root cause + confidence + evidence | Per-record classifications + counts |
| Safety validation | None (read-only agent) | `_validate_requeue_safety()` |
| Color coding | None | Per-classification category |
| Tool count | 6 tools | 3 tools |

---

## How to Run

```bash
# Both scenarios (default):
python experiments/dlq_live_test.py

# Single scenario:
python experiments/dlq_live_test.py --scenario kafka
python experiments/dlq_live_test.py --scenario bad_files

# Show tool schemas:
python experiments/dlq_live_test.py --show-tools
```

**Requires**: `GOOGLE_API_KEY` environment variable (Gemini free tier — no cost).

---

## How This Connects

- **`agent.py`** — the agent being tested end-to-end
- **`dlq_tools.py`** — provides simulated DLQ data the agent investigates
- **`config.yaml`** — `configs/dev/config.yaml` loaded for LLM settings
- **`test_dlq_agent.py`** — unit tests (mocked) complement this live test (real LLM)
- **`recon_live_test.py`** — parallel live test to compare patterns against
