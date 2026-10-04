# Code Guide: `argus/agents/spark_debugger/graph.py`

## Purpose

Wires the Spark Debugger's ReAct + Reflection pattern into a LangGraph StateGraph. This is the most complex graph in Argus — 5 nodes and 2 conditional edges creating a two-level iteration structure.

## Reading Order Context

**Read AFTER**: `recon graph.py` (base ReAct topology), `spark_debugger state.py`, `prompts.py`, `spark_tools.py`
**Read BEFORE**: `agent.py` (which wraps this graph)

## Key Concepts

### 1. Graph Topology Comparison

**Recon (plain ReAct):**
```
entry → llm → should_continue? → tools → llm (loop)
                    ↓
                  report → END
```
4 nodes, 1 conditional edge

**Spark Debugger (ReAct + Reflection):**
```
entry → llm → should_continue? → tools → llm (inner loop)
                    ↓
                 reflect → should_revise? → llm (outer loop)
                                ↓
                              report → END
```
5 nodes, 2 conditional edges

### 2. The Reflect Node — What Makes This Agent Different

The reflect node is a **separate LLM call without tools bound**. It:
1. Extracts the hypothesis from the last AIMessage
2. Renders SPARK_REFLECTION_PROMPT with the hypothesis injected
3. Calls the plain model (no tools → can't call tools, forced to reason)
4. Returns the reflection response + increments reflection_count

Why no tools? If tools were bound, the LLM would try to investigate more instead of critically evaluating its work.

### 3. Two Conditional Edges

| Router | After | Decides | Options |
|--------|-------|---------|---------|
| `_should_continue` | llm node | Inner loop | `tools` (more investigation) or `reflect` (evaluate hypothesis) |
| `_should_revise` | reflect node | Outer loop | `llm` (gaps found, re-investigate) or `report` (hypothesis confirmed) |

Both routers have safety valves:
- `_should_continue`: max_iterations forces reflect
- `_should_revise`: max_reflections forces report

### 4. Closure Pattern (Same as Recon)

All node factories use closures to capture the LLM:
```python
def _make_reflect_node(model):     # model captured in closure
    def reflect_node(state: dict): # LangGraph calls this with (state)
        response = model.invoke(...)
        return {...}
    return reflect_node
```

Three model configurations exist:
- `model_with_tools` → llm_node (investigation)
- `llm` (plain) → reflect_node (self-critique)
- `llm.with_structured_output(SparkDiagnosis)` → report_node (final output)

### 5. String-Based Routing in should_revise

```python
if "NEEDS_MORE_INVESTIGATION" in content.upper():
    return "llm"
return "report"  # default: confirmed or unrecognized
```

This is intentionally loose — the reflection is conversational, and forcing JSON would reduce critique quality. The default-to-report behavior is also intentional: if the LLM's reflection format is unexpected, it's safer to proceed with whatever hypothesis exists than to loop forever.

## Learning Checkpoint

After reading this file, you should understand:
- [ ] How the 2-level loop works (inner: tools, outer: reflection)
- [ ] Why the reflect node uses a plain model (no tools)
- [ ] How safety valves prevent infinite loops at both levels
- [ ] Why string-matching is better than structured output for reflection routing
- [ ] The three different model configurations and their roles
