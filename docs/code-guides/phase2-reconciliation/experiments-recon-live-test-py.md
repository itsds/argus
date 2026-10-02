# Code Guide: `experiments/recon_live_test.py`

> **Read this AFTER reading the test_recon_agent.py guide.**
> This guide explains how live integration tests complement unit tests for LLM agents.

---

## What Is This File About?

This is the **live integration test** for the Reconciliation agent. Unlike unit tests that mock the LLM, this runs the full agent pipeline with a real LLM (Gemini free tier) and simulated pipeline data. It verifies that the LLM actually understands the system prompt, uses the right tools, and produces a valid `ReconReport`.

---

## What Feature Does It Bring to Argus?

1. **End-to-end validation** — proves the entire stack works: config → agent → graph → LLM → tools → report
2. **Prompt quality feedback** — if the LLM picks wrong tools or produces a weak diagnosis, the prompt needs work
3. **Structured output verification** — confirms `.with_structured_output(ReconReport)` produces valid Pydantic objects
4. **Observable debugging** — prints every step with ANSI colors so you can watch the agent reason

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`asyncio.run()`** | Runs the async `main()` — the agent's `invoke()` is async |
| **`argparse`** | CLI argument parsing — `--scenario`, `--show-tools` |
| **`load_config("dev")`** | Loads `configs/dev/config.yaml` — real config, real LLM settings |
| **`ReconciliationAgent`** | The full agent class — no mocking |
| **`time.monotonic()`** | Precise timing for performance measurement |
| **ANSI escape codes** | Colored terminal output (same style as `calculator_agent.py`) |

---

## Unit Test vs Live Test — Why You Need Both

### What Unit Tests CAN'T Tell You

Unit tests mock the LLM, which means they can't answer:

| Question | Unit Test | Live Test |
|---|---|---|
| Does the LLM understand the system prompt? | ❌ | ✅ |
| Does it use tools in the right order? | ❌ | ✅ |
| Does the ReAct loop converge (not spin forever)? | ❌ | ✅ |
| Does `.with_structured_output()` actually work? | ❌ | ✅ |
| Does invoke() translate context correctly? | ✅ | ✅ |
| Does the error boundary catch exceptions? | ✅ | ❌ |
| Is the test deterministic and fast? | ✅ | ❌ |

**Key insight:** Unit tests verify the **plumbing** (does data flow correctly?). Live tests verify the **intelligence** (does the LLM reason correctly?).

---

## The Two Built-In Scenarios — Deep Dive

### Scenario: Gate 3 (2026-09-28)

```
Pipeline flow:  Bronze.booking_raw (15012) → Silver.booking_detail (14712)
                                                          ↑ 300 rows lost!
Root cause:     111 duplicate booking_ids from upstream re-delivery
                Silver MERGE INTO deduplicates on natural key, so dupes
                collapse rows, explaining the count drop.

Expected tool sequence:
  1. query_gate_results("2026-09-28") → sees Gate 3 FAILED
  2. compare_row_counts("2026-09-28", [...]) → sees Bronze > Silver
  3. check_duplicate_keys("2026-09-28", "silver.booking_detail") → finds 111 dupes
```

This scenario tests the agent's ability to follow the **diagnostic chain**: gate result → identify which tables mismatch → investigate WHY.

### Scenario: Gate 4 (2026-09-27)

```
Pipeline flow:  Gold.fact_travel_tag (15102) → Snowflake.FACT_TRAVEL_TAG (15087)
                                                                ↑ 15 rows short!
Root cause:     15 rows have NULL card_sk — missing dimension keys
                The dimension refresh task failed or account-management
                pipeline hasn't delivered those cards yet.

Expected tool sequence:
  1. query_gate_results("2026-09-27") → sees Gate 4 FAILED
  2. compare_row_counts("2026-09-27", [...]) → sees Gold > Snowflake
  3. check_fk_integrity("2026-09-27") → finds 15 NULL card_sk rows
```

This scenario tests a different investigation path — FK integrity instead of duplicate keys.

---

## Key Patterns — Deep Dive

### sys.path Insertion

```python
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
```

**Why?** When you run `python experiments/recon_live_test.py`, Python's working directory is wherever you ran the command, and only the script's directory gets added to `sys.path`. The `from argus.xxx` imports need the repo root on the path.

**Why not just `cd` to repo root?** You might run it from anywhere. This makes the script location-independent.

---

### Async Entry Point

```python
if __name__ == "__main__":
    asyncio.run(main())
```

**Why async?** The agent's `invoke()` is `async def`. Even though the current graph.invoke() is synchronous inside, the interface is async (future-proofing for async tool execution in Phase 6). `asyncio.run()` creates the event loop and runs `main()` inside it.

---

### Agent Reuse Across Scenarios

```python
agent = ReconciliationAgent(config)
for scenario_key in scenarios_to_run:
    result = await run_scenario(scenario_key, agent)
```

The same agent instance is reused across both scenarios. This is safe because:
1. The compiled graph is stateless (state flows through `invoke()` call, not stored in graph)
2. The lazy init builds the graph once on the first scenario, reuses for the second
3. Each scenario gets its own `TriggerContext` with different `run_date` and `gate_failure`

---

### Result Inspection

The test prints every field of the `AgentResult` and `ReconReport` with ANSI formatting. This is intentional — when you're tuning prompts, you need to see:

- **Actions taken** — did the LLM call the tools you expected, in the order you expected?
- **Root cause** — did the LLM correctly identify the problem?
- **Severity** — did it classify appropriately (P2 for duplicates, P3 for missing dims)?
- **Findings** — are the expected/actual values from the tool results, not hallucinated?

---

## Running the Live Test

```bash
# Set GOOGLE_API_KEY first (Gemini free tier):
# Windows PowerShell: $env:GOOGLE_API_KEY="your-key"
# WSL/Linux: export GOOGLE_API_KEY="your-key"

# Run both scenarios:
python experiments/recon_live_test.py

# Run a single scenario:
python experiments/recon_live_test.py --scenario gate3

# See what tools the LLM has:
python experiments/recon_live_test.py --show-tools
```

Each scenario takes ~10-30 seconds (LLM API latency + multiple ReAct loop iterations).

---

## How This Connects to the Bigger Picture

```
Phase 0:  experiments/calculator_agent.py    ← learned ReAct loop basics
Phase 2:  experiments/recon_live_test.py      ← THIS FILE — validates full Recon agent
Phase 3:  experiments/dlq_live_test.py        ← (future) DLQ Triage agent
Phase 4:  experiments/backfill_live_test.py   ← (future) Backfill Planning agent
Phase 5:  experiments/spark_live_test.py      ← (future) Spark Debugger agent
```

Each phase gets its own live test in `experiments/`. The unit tests live in `tests/` and are always run automatically. The live tests are run manually when you're tuning prompts or verifying a new model.
