# Code Guide: `argus/agents/spark_debugger/agent.py`

## Purpose

Wraps the Spark Debugger's LangGraph graph behind the BaseAgent interface. Translates TriggerContext → SparkDebuggerState, runs the graph, and packages the SparkDiagnosis into an AgentResult.

## Reading Order Context

**Read AFTER**: `recon agent.py` (same adapter pattern), `spark_debugger graph.py` (the graph being wrapped)
**Read BEFORE**: Tests, live test

## Key Concepts

### 1. Same Pattern, Different Details

Structurally identical to ReconciliationAgent:
- `name` → `"spark_debugger"`
- `build_graph()` → delegates to `build_spark_debugger_graph()`
- `invoke()` → translate → build → run → extract → package

The differences are all in the **data**:
- TriggerContext extracts `app_id` (not `gate_failure`)
- Config reads `agents.spark_debugger.max_iterations` and `max_reflections`
- Report type is SparkDiagnosis (not ReconReport)

### 2. Reflection Metadata in Results

Unlike the other agents, SparkDebuggerAgent enriches the report dict with `_meta`:

```python
report_dict["_meta"] = {
    "iterations": final_state.get("iteration", 0),
    "reflections": final_state.get("reflection_count", 0),
    "hypothesis": final_state.get("hypothesis", ""),
}
```

This gives the audit trail visibility into:
- How many tool calls the investigation took
- How many hypothesis refinements occurred
- What the final hypothesis was (before report generation)

### 3. Config-Driven Limits

```python
max_iterations = self.config.get("agents.spark_debugger.max_iterations", 15)
max_reflections = self.config.get("agents.spark_debugger.max_reflections", 2)
```

This allows dev/prod to have different limits without code changes. Dev might use 15/2, prod might use 20/3 for harder cases.

## Learning Checkpoint

After reading this file, you should understand:
- [ ] How the BaseAgent adapter pattern stays consistent across all 4 agents
- [ ] What `_meta` adds to the audit trail
- [ ] Why config-driven limits matter for dev vs prod environments
