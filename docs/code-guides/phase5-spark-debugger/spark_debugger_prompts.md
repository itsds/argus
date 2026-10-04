# Code Guide: `argus/agents/spark_debugger/prompts.py`

## Purpose

Defines two system prompts (investigation + reflection) and a human prompt template for the Spark Debugger agent. This is the first agent with a **reflection prompt** — a separate prompt designed for self-critique rather than investigation.

## Reading Order Context

**Read AFTER**: `recon prompts.py` (base prompt pattern), `spark_tools.py` (the tools referenced in the prompt)
**Read BEFORE**: `graph.py` (which wires prompts to nodes)

## Key Concepts

### 1. Two Prompts, Not One

| Prompt | Used By | Tools Bound? | Purpose |
|--------|---------|--------------|---------|
| `SPARK_DEBUGGER_SYSTEM_PROMPT` | entry + llm nodes | Yes (investigation) | Define expertise, strategy, rules |
| `SPARK_REFLECTION_PROMPT` | reflect node | No (pure reasoning) | Self-critique, gap detection |

The investigation prompt tells the LLM **what to do**. The reflection prompt tells it **what to think about**. Separating them prevents the reflection from degenerating into more tool calls.

### 2. Causal Chain Reasoning

The prompt teaches a specific reasoning pattern:

```
❌ Symptom-level: "GC is high"
✅ Causal chain: "Skew → one partition gets 130x data → spill → GC pressure"
```

This is embedded through:
- Explicit ❌/✅ examples in the prompt
- A rule: "ALWAYS trace the causal chain"
- The reflection prompt's Question 3: "Have you traced the chain from root cause → intermediate effects → observed symptoms?"

### 3. Investigation Strategy as Soft Guidance

Unlike a rigid checklist, the prompt suggests an **order** but allows flexibility:
1. Application overview (always first)
2. Find bottleneck stage
3. Check for skew
4. Check executor health
5. Understand the plan
6. Deep dive events

"Adapt based on what you find" — the agent should follow the evidence, not a script.

### 4. Reflection Prompt Design

The reflection prompt asks three specific questions:
1. **Evidence sufficiency** — are claims backed by data?
2. **Alternative explanations** — could symptoms have a different cause?
3. **Causal chain completeness** — is the chain traced to the root?

It ends with a binary signal: `NEEDS_MORE_INVESTIGATION` or `HYPOTHESIS_CONFIRMED`. This keyword is used by the `should_revise` router to decide the next step.

### 5. Bottleneck Categories as Schema Guidance

The prompt defines 5 categories that map directly to `SparkBottleneck.category`:
- skew, spill, small_files, broadcast, gc_pressure

This alignment between prompt and schema ensures the LLM's free-form investigation produces output that fits the structured report.

## Learning Checkpoint

After reading this file, you should understand:
- [ ] Why investigation and reflection use separate prompts
- [ ] How causal chain reasoning differs from symptom listing
- [ ] Why the reflection uses keyword routing instead of structured output
- [ ] How prompt categories align with Pydantic schema fields
