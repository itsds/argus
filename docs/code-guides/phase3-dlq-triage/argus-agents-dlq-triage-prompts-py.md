# Code Guide: `argus/agents/dlq_triage/prompts.py`

> **Read this BEFORE opening `argus/agents/dlq_triage/prompts.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **system prompt for the DLQ Triage agent** — the instructions that tell the LLM how to classify dead-letter-queue records, when to requeue, and how to produce a triage report. It introduces two prompt engineering patterns that the Recon agent didn't need: **classification rubric with confidence calibration** and **side-effect guardrails**.

---

## What Feature Does It Bring to Argus?

1. **Classification rubric** — concrete rules mapping error types to categories (TRANSIENT, SCHEMA_MISMATCH, DATA_QUALITY, UNKNOWN)
2. **Confidence calibration** — explicit guidance on what makes confidence 0.9 vs 0.5, preventing the LLM from always outputting the same score
3. **Requeue safety rules** — strict ONLY/NEVER rules that form the first defense layer against unsafe side effects
4. **Few-shot examples** — embedded in the rubric to anchor the LLM's classification behavior

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`ChatPromptTemplate`** (LangChain) | Composes system + human messages with variable substitution |
| **System prompt** | Static instructions the LLM sees on every turn |
| **Human prompt template** | Per-run context injected at graph entry (`{run_date}`, `{source_lane}`) |
| **Classification rubric** | Structured decision tree embedded in natural language |
| **Few-shot prompting** | Examples like "TimeoutException → TRANSIENT at 0.90" anchor behavior |

---

## Prompt Structure: Recon vs DLQ

```
RECON PROMPT                         DLQ PROMPT
──────────                           ─────────
1. Role                              1. Role
2. Pipeline architecture             2. Pipeline DLQ architecture
3. Investigation strategy            3. Classification rubric (NEW)
4. Rules                             4. Confidence calibration (NEW)
                                     5. Requeue safety rules (NEW)
                                     6. Investigation strategy
                                     7. Rules
```

The DLQ prompt is longer because the task is more complex: classify AND act, not just investigate and report.

---

## The Classification Rubric (Deep Dive)

### Why a rubric instead of just "classify each record"?

Without structure, LLMs tend to:
- Classify everything as the first category they see
- Use vague justifications ("this seems like a transient error")
- Skip cross-referencing evidence

The rubric provides:
1. **Category definitions** — what each classification means
2. **Evidence examples** — specific error classes that map to each category
3. **Confidence ranges** — what evidence level maps to what score
4. **Few-shot examples** — concrete input → output pairs

### The Four Categories

| Category | When | Action | Confidence Range |
|---|---|---|---|
| TRANSIENT | Infrastructure failure, will succeed on retry | Requeue | 0.70–0.95 |
| SCHEMA_MISMATCH | Producer/consumer schema conflict | Quarantine | 0.60–0.95 |
| DATA_QUALITY | Invalid data, retry will fail identically | Quarantine | 0.70–0.95 |
| UNKNOWN | Doesn't fit above, or confidence < 0.60 | Escalate | 0.70+ |

---

## Confidence Calibration (Key Concept)

### The Problem

Without calibration, LLMs exhibit two failure modes:

**Over-confident**: Always outputs 0.90+ → dangerous because TRANSIENT at 0.90 triggers auto-requeue. If the LLM is over-confident on a SCHEMA_MISMATCH that it misclassified as TRANSIENT, it requeues a record that will fail again.

**Under-confident**: Always outputs 0.50–0.70 → nothing gets auto-requeued because the 0.80 threshold is never met. The agent becomes useless for automation.

### The Solution

The prompt includes explicit calibration guidance per category:

```
TRANSIENT:
  0.85-0.95: clear infrastructure error, no data/schema involvement
  0.70-0.85: likely transient but with some ambiguity
  Below 0.70: don't classify as transient — investigate more

SCHEMA_MISMATCH:
  0.90-0.95: SchemaRegistryException + changelog confirms
  0.75-0.90: error mentions schema + changelog has recent change
  0.60-0.75: might be schema-related but changelog doesn't confirm
```

These ranges anchor the LLM's confidence distribution to specific evidence levels.

---

## Side-Effect Guardrails (Key Concept)

### The Four Rules

```
✅ ONLY requeue TRANSIENT with confidence ≥ 0.80
❌ NEVER requeue SCHEMA_MISMATCH
❌ NEVER requeue DATA_QUALITY
❌ NEVER requeue UNKNOWN
```

### Why prompt-level guardrails when the tool has safety features?

Defense in depth:
- **Layer 1 (PROMPT)**: Prevents the LLM from attempting a bad requeue
- **Layer 2 (TOOL idempotency)**: Prevents double-requeue on retries
- **Layer 3 (TOOL rate limit)**: Prevents runaway volume
- **Layer 4 (AUDIT)**: Makes everything visible after the fact

The prompt is the FIRST line of defense — if it fails, the tool's safety features are backup, but they don't check classification correctness. A tool happily requeues a SCHEMA_MISMATCH record if the LLM tells it to.

---

## Few-Shot Examples in the Rubric

Each category includes concrete examples:

```
Example: TimeoutException from broker → TRANSIENT at 0.90
Example: SchemaRegistryException + changelog shows pending_consumer_update
         → SCHEMA_MISMATCH at 0.95
```

This is few-shot prompting embedded in the system message. It's more effective than abstract rules because:
- LLMs learn patterns from examples better than from descriptions
- Examples anchor the confidence scale to specific evidence levels
- They serve as implicit test cases during prompt development

---

## The Human Message Template

```
Triage the following DLQ alert:
- Run date: {run_date}
- Source lane: {source_lane}
- Trigger params: {trigger_params}
```

Variables are filled from `DLQTriageState` at graph entry time. The key difference from Recon is `source_lane` instead of `gate_name`.

---

## How This Connects

- **`graph.py`** entry node calls `DLQ_PROMPT_TEMPLATE.invoke()` to produce the initial messages
- **`state.py`** provides the variables (`run_date`, `source_lane`, `trigger_params`)
- **Tool docstrings** (`dlq_tools.py`) reinforce the prompt's investigation strategy
- **Report instruction** (in `graph.py` report node) asks for classifications that follow this rubric
