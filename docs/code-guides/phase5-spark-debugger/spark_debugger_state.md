# Code Guide: `argus/agents/spark_debugger/state.py`

## Purpose

Defines `SparkDebuggerState` — the LangGraph state schema for the Spark Debugger agent. This state carries everything the graph needs: the conversation backbone (messages), investigation metadata (app_id, iteration counts), the working hypothesis, and the final diagnosis.

## Reading Order Context

**Read AFTER**: `recon state.py` (same annotation pattern), `spark_tools.py` (tools that populate this state)
**Read BEFORE**: `prompts.py`, `graph.py` (nodes read and write this state)

## Key Concepts

### 1. Hypothesis as First-Class State

Unlike the other agents where the LLM's "understanding" lives implicitly in the message history, the Spark Debugger makes the hypothesis **explicit**:

```python
hypothesis: str           # current working theory
reflection_count: int     # how many times we've reflected
max_reflections: int      # safety cap
```

Why? Because the reflection node needs to **evaluate** the hypothesis. Extracting it from the messages and storing it in state makes it available to the `should_revise` router and the report node without re-parsing messages.

### 2. Two-Level Iteration Control

Same pattern as the Backfill agent, different purpose:

| Level | Counter | Max | Purpose |
|-------|---------|-----|---------|
| Inner | `iteration` | `max_iterations` (15) | Caps ReAct tool calls |
| Outer | `reflection_count` | `max_reflections` (2) | Caps hypothesis refinements |

The inner loop prevents infinite tool calling. The outer loop prevents infinite "reflect → investigate → reflect" cycles.

### 3. No HITL Fields

Unlike BackfillState (which has `approval_status`, `revision_feedback`, `thread_id`), SparkDebuggerState has **no human-in-the-loop fields**. The Spark Debugger is fully autonomous because its output is read-only — a diagnosis report, not an action plan that modifies state.

### 4. make_initial_state() Factory

Same pattern as Recon — a plain function returning a dict (not a Pydantic instance). This is because LangGraph's `graph.invoke()` expects a dict, and using a factory keeps the construction logic testable and explicit.

## Learning Checkpoint

After reading this file, you should understand:
- [ ] Why the hypothesis needs to be in state (not just in messages)
- [ ] How two-level iteration control prevents runaway loops at both levels
- [ ] Why max_iterations=15 (vs 10 for Recon) — more tools, deeper investigation
- [ ] Why max_reflections=2 — diminishing returns after 2 rounds
