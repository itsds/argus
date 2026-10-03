# Code Guide: `experiments/backfill_live_test.py`

## Purpose
Live integration test that runs the full Backfill agent against a real LLM (Gemini free tier). Unlike unit tests which mock the graph, this test makes **real API calls** to verify the complete two-phase lifecycle works end-to-end: investigation → planning → human review → execution.

## What You'll Learn
- How to test a **two-phase agent lifecycle** with a real LLM
- How to verify **plan quality** (structure validation, not just status codes)
- How to test the **rejection loop** with real LLM plan revision
- How to compare **original vs revised plans** to verify feedback incorporation
- How **preflight checks** prevent confusing errors when dependencies are missing

## Architecture

```
backfill_live_test.py
├── Display Helpers
│   ├── _separator()              — ANSI color section dividers
│   ├── _print_plan()             — Pretty-print BackfillPlan for human review
│   └── _print_execution_audit()  — Color-coded audit trail (lock/step/release)
│
├── Scenarios
│   ├── run_approve_first()       — Happy path: invoke → approve → success
│   └── run_reject_then_approve() — Rejection loop: invoke → reject → approve
│
├── Tool Display
│   └── show_tool_schemas()       — Shows investigation + execution tool schemas
│
└── Main
    ├── Preflight check           — Verifies graph.py exists before running
    ├── Config loading            — loads dev config, prints settings
    ├── Scenario runner           — Runs selected scenario(s)
    └── Summary                   — Results + agent state verification
```

## Key Concepts

### 1. Two-Phase Lifecycle Testing
Unlike the Recon live test (single invoke()), this test calls invoke() then resume():

```python
# Phase 1: invoke → needs_approval
result = await agent.invoke(context)
assert result.status == "needs_approval"
plan = result.report  # The BackfillPlan as dict

# (Human reviews the plan here — we just approve programmatically)

# Phase 2: resume → success
result = await agent.resume("approved")
assert result.status == "success"
```

### 2. Plan Quality Validation
Instead of just checking `status == "needs_approval"`, we validate the plan's structure:

```python
checks = {
    "Has incident summary": bool(plan.get("incident_summary")),
    "Has root cause": bool(plan.get("root_cause")),
    "Has proposed steps": len(plan.get("proposed_steps", [])) > 0,
    "Steps are ordered": all(s["order"] == i+1 for i, s in enumerate(steps)),
}
```

This catches cases where the LLM produces a technically valid Pydantic model but with empty/nonsensical content.

### 3. Rejection Loop Testing
The reject-then-approve scenario tests whether the LLM can incorporate feedback:

```python
# Invoke → get plan v1
result = await agent.invoke(context)
original_plan = result.report

# Reject with specific feedback
result = await agent.resume(
    "rejected",
    "Add rollback steps for each backfill step in case of failure."
)
revised_plan = result.report

# Compare: did the LLM actually change the plan?
orig_steps = len(original_plan["proposed_steps"])
rev_steps = len(revised_plan["proposed_steps"])
# Revised should have more steps (rollback steps added)
```

### 4. Preflight Check
Prevents confusing ImportError when graph.py doesn't exist yet:

```python
graph_path = REPO_ROOT / "argus" / "agents" / "backfill" / "graph.py"
if not graph_path.exists():
    print("Missing: graph.py — build Phase 4 files first")
    return
```

### 5. Tool Schema Display
The Backfill agent has TWO tool sets (investigation + execution), bound to different LLM nodes:

```python
# Investigation tools → investigation_llm node
from argus.tools.pipeline.backfill_investigation_tools import INVESTIGATION_TOOLS

# Execution tools → execution_llm node
from argus.tools.pipeline.backfill_execution_tools import EXECUTION_TOOLS
```

Both are wrapped in try/except ImportError since they may not be built yet.

## Comparison: Backfill vs Recon Live Test

| Aspect | Recon Live Test | Backfill Live Test |
|--------|----------------|-------------------|
| Agent calls | 1 invoke() per scenario | invoke() + resume() per scenario |
| Scenarios | 2 (gate3, gate4) | 2 (approve-first, reject-then-approve) |
| Plan review | N/A (no HITL) | Full plan pretty-printing + validation |
| Rejection test | N/A | Reject → compare original vs revised |
| Tool sets | 1 (RECON_TOOLS) | 2 (investigation + execution) |
| Execution audit | N/A | Lock → steps → release verification |
| Preflight check | None | Checks graph.py exists |
| Agent state check | None | Verifies has_pending_approval after all scenarios |

## Scenarios

### Approve First (Happy Path)
```
invoke() ──→ needs_approval ──→ review plan ──→ resume("approved") ──→ success
                                    │
                              validate structure:
                              • incident_summary
                              • root_cause  
                              • proposed_steps (ordered)
                              • risk_assessment
```

### Reject Then Approve (Rejection Loop)
```
invoke() ──→ needs_approval ──→ review plan v1 ──→ resume("rejected", feedback)
                                                          │
                                                    needs_approval
                                                          │
                                                    review plan v2
                                                          │
                                              compare v1 vs v2 (diff?)
                                                          │
                                                resume("approved")
                                                          │
                                                      success
```

## Dependencies
- `argus.agents.backfill.agent` — BackfillAgent
- `argus.agents.base` — TriggerContext
- `argus.core.config` — load_config
- GOOGLE_API_KEY environment variable (Gemini free tier)
- Phase 4 files: graph.py, investigation tools, execution tools

## Running
```bash
# Default: approve-first scenario
python experiments/backfill_live_test.py

# Rejection loop scenario
python experiments/backfill_live_test.py --scenario reject-then-approve

# Both scenarios
python experiments/backfill_live_test.py --scenario both

# Show tool schemas
python experiments/backfill_live_test.py --show-tools
```
