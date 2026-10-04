# Code Guide: `argus/agents/backfill/prompts.py`

> **Read this BEFORE opening `argus/agents/backfill/prompts.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **prompt engineering file for the Backfill agent** — three system prompts (one per graph phase), two human message templates, and two ChatPromptTemplates. It's the most complex prompt file in Argus because the Backfill agent has three distinct phases that each require different LLM behavior.

---

## What Feature Does It Bring to Argus?

1. **Phase-specific LLM behavior** — investigation, planning, and execution each get focused instructions
2. **Structured output quality control** — the planning rubric defines good vs bad examples for every BackfillPlan field
3. **Rejection loop support** — the revision template injects human feedback with urgency signals
4. **Safety-first execution** — the execution prompt enforces Lock → Execute → Release strictly

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`ChatPromptTemplate`** (LangChain) | Parameterized prompt assembly from system + human messages |
| **Multi-phase prompting** | Separate system prompts per graph phase |
| **Structured output rubric** | Good/bad examples for each plan field |
| **Feedback injection** | Revision template with `{revision_feedback}` variable |
| **f-string variables** | `{run_date}`, `{trigger_params}`, `{plan_iterations}`, `{max_plan_iterations}` |

---

## The Three System Prompts

### 1. `BACKFILL_INVESTIGATION_PROMPT` — Phase 1 (Read-Only)

**Purpose:** Guide the LLM through evidence gathering with the 5 investigation tools.

**Structure:** Role → Pipeline Architecture → Investigation Strategy → Rules

The investigation strategy maps directly to the 5 tools in `BACKFILL_INVESTIGATION_TOOLS`:

| Step | Tool | What It Answers |
|---|---|---|
| 1 | `get_incident_context` | WHAT happened |
| 2 | `assess_data_gaps` | How much DAMAGE |
| 3 | `check_pipeline_locks` | Is it SAFE to plan |
| 4 | `get_backfill_history` | How long will it TAKE |
| 5 | `validate_source_readiness` | Does SOURCE data exist |

**Key rule:** "Call at least 3 tools before concluding." Prevents the LLM from producing a plan after only one tool call.

**Pattern comparison:** Same structure as Recon's investigation prompt but with more tools and an explicit ordering strategy.

### 2. `BACKFILL_PLANNING_PROMPT` — Phase 2 (Structured Output)

**Purpose:** Define quality criteria for every field of the `BackfillPlan` Pydantic model.

This is the Phase 4 equivalent of DLQ's classification rubric. Instead of calibrating confidence scores, it calibrates plan quality.

**Per-field rubric:**

| Field | Good Example | Bad Example |
|---|---|---|
| `incident_summary` | "Gate 3 failed on 2026-09-28: Silver booking_detail is 111 rows short..." | "There was a pipeline failure" |
| `root_cause` | "Kafka consumer lag spike at 05:42 UTC caused 111 booking_ids to be re-delivered..." | "Row counts don't match" |
| `proposed_steps` | Ordered: Silver → Gold → Snowflake | Unordered, or Gold before Silver |
| `risk_assessment` | "Low risk — Bronze snapshot 5765432198 is available with 30-day retention..." | "There might be some risks" |

**Critical rules:**
- Steps must be ORDERED (upstream before downstream)
- Snapshot IDs must come from `validate_source_readiness` results (not hallucinated)
- Duration estimates must use `get_backfill_history` data as baseline
- Never propose backfill for layers that don't need it

### 3. `BACKFILL_EXECUTION_PROMPT` — Phase 3 (Side Effects)

**Purpose:** Enforce strict Lock → Execute → Release ordering after the plan is approved.

Short and mechanical — all rules, no reasoning guidance. Three rules:

1. **LOCK FIRST** — `acquire_pipeline_lock` before any step. If blocked, STOP and report.
2. **EXECUTE IN ORDER** — steps run sequentially. If one FAILS, stop. Never skip ahead.
3. **RELEASE LAST** — `release_pipeline_lock` ALWAYS runs, even on failure.

**The pattern as a diagram:**
```
acquire_pipeline_lock(entity, layers, reason)
  ↓
execute_backfill_step(1, ...)  → if FAIL → release → STOP
execute_backfill_step(2, ...)  → if FAIL → release → STOP
  ↓
release_pipeline_lock(entity, layers)
  → Returns: full audit trail
```

---

## The Two Human Message Templates

### `BACKFILL_HUMAN_PROMPT` — Initial Investigation

```
Investigate the following pipeline incident and produce a backfill plan:
- Run date: {run_date}
- Trigger params: {trigger_params}
```

Seeded by the entry node from `BackfillState.run_date` and `BackfillState.trigger_params`. Same pattern as Recon/DLQ human prompts.

### `BACKFILL_REVISION_PROMPT` — Rejection Feedback

```
Your backfill plan was REJECTED by the human reviewer.
This is attempt {plan_iterations} of {max_plan_iterations}.

Reviewer feedback:
{revision_feedback}

Revise your plan to address the feedback above...
```

**Three design choices:**
1. **"REJECTED" framing** — unambiguous context-setting
2. **"attempt 2 of 3"** — creates urgency via `{plan_iterations}` / `{max_plan_iterations}`
3. **"Focus on what changed"** — prevents the LLM from starting over (wasting tool calls)

This template is what makes the rejection loop useful. Without it, the LLM would either reproduce the same plan or re-investigate from scratch.

---

## The Two ChatPromptTemplates

### `BACKFILL_PROMPT_TEMPLATE` — Initial Entry

```python
ChatPromptTemplate.from_messages([
    ("system", BACKFILL_INVESTIGATION_PROMPT),
    ("human", BACKFILL_HUMAN_PROMPT),
])
```

Used by the entry node to seed the conversation. Note: this only covers the FIRST phase. Planning and execution prompts are injected by their respective graph nodes.

### `BACKFILL_REVISION_TEMPLATE` — Rejection Loop

```python
ChatPromptTemplate.from_messages([
    ("human", BACKFILL_REVISION_PROMPT),
])
```

Used by the rejection handler node. Only a human message — the system prompt is already in the conversation from initial seeding.

---

## Why Not One Big Prompt?

A single prompt with all three phases would work but has problems:

| Problem | Consequence |
|---|---|
| **Instruction leakage** | LLM sees execution rules during investigation, might try to execute early |
| **Token waste** | Longer prompt = more tokens per call = higher cost + latency |
| **Maintenance** | Changing one phase risks breaking another |
| **Redundancy** | The graph already controls which phase runs — the prompt shouldn't re-describe the flow |

Separate prompts per phase means the graph picks the right prompt for the right node, and each prompt is focused.

---

## Comparing Prompt Files Across Phases

| Aspect | Recon | DLQ | Backfill |
|---|---|---|---|
| System prompts | 1 | 1 | 3 |
| Human templates | 1 | 1 | 2 (initial + revision) |
| ChatPromptTemplates | 1 | 1 | 2 (initial + revision) |
| Rubric type | Investigation strategy | Classification + confidence | Structured output quality |
| Side-effect rules | — | Requeue safety | Lock → Execute → Release |
| Feedback handling | — | — | Revision prompt with iteration count |

---

## Key Takeaways

1. **Multi-phase agents need multi-phase prompts** — one prompt per phase prevents leakage and reduces tokens
2. **Structured output rubrics improve plan quality** — good/bad examples anchor the LLM's expectations
3. **Revision prompts make rejection loops useful** — without feedback injection, the LLM reworks blindly
4. **Execution prompts should be strict and short** — mechanical compliance, not creative reasoning
5. **ChatPromptTemplates stay simple** — complexity lives in the system prompt strings, not the template assembly
