## CLAUDE.md — Phase 5 update instructions

Apply these changes to CLAUDE.md:

### 1. Repository Structure — add Spark Debugger files

Under `argus/agents/`, add:
```
│   │   └── spark_debugger/
│   │       ├── __init__.py    # Package marker (exists)
│   │       ├── state.py       # SparkDebuggerState with hypothesis + reflection tracking
│   │       ├── prompts.py     # Investigation + reflection prompts (2 system prompts)
│   │       ├── graph.py       # ReAct + Reflection topology (5 nodes, 2 conditional edges)
│   │       └── agent.py       # BaseAgent subclass (SparkDebuggerAgent)
```

Under `argus/tools/`, add:
```
│   │   └── compute/
│   │       ├── __init__.py    # Package marker (exists)
│   │       └── spark_tools.py # 6 @tool functions for Spark debugging
```

Under `tests/agents/`, add:
```
│       └── test_spark_debugger_agent.py  # Unit tests (8 test classes, mocked LLM)
```

Under `experiments/`, add:
```
│   └── spark_debugger_live_test.py # Live integration test (2 scenarios: skew, GC pressure)
```

### 2. Phase 5 status — replace the placeholder line

Replace:
```
- **Phase 5**: Spark Debugger agent (most complex)
```

With:
```
- **Phase 5** (COMPLETE): AI Spark Debugger agent (ReAct + Reflection)
  - ✅ `tools/compute/spark_tools.py` — 6 Spark investigation tools with simulated data (2 scenarios)
  - ✅ `agents/spark_debugger/state.py` — SparkDebuggerState with hypothesis + reflection tracking
  - ✅ `agents/spark_debugger/prompts.py` — Investigation prompt + reflection prompt (self-critique)
  - ✅ `agents/spark_debugger/graph.py` — ReAct + Reflection topology (5 nodes, 2 conditional edges, 2-level loop)
  - ✅ `agents/spark_debugger/agent.py` — BaseAgent subclass with reflection metadata in results
  - ✅ `tests/agents/test_spark_debugger_agent.py` — Unit tests (8 test classes, mocked LLM)
  - ✅ `experiments/spark_debugger_live_test.py` — Live integration test (2 scenarios: data skew, GC pressure)
```

### 3. Add Phase 5 New Concepts section — after Phase 4 New Concepts

```
## Phase 5 New Concepts (vs Phase 4)

- **ReAct + Reflection pattern** — after investigation loop ends (LLM stops calling tools), a separate LLM call evaluates the hypothesis for gaps, alternative explanations, and causal chain completeness before committing to a report
- **Two-level iteration control** — inner loop (`iteration`/`max_iterations`=15) caps ReAct tool calls, outer loop (`reflection_count`/`max_reflections`=2) caps hypothesis refinement cycles
- **Hypothesis tracking** — explicit `hypothesis` state field captures the agent's working theory, injected into the reflection prompt for targeted self-critique
- **Compute-aware tools** — completely different tool set from pipeline-aware agents (SparkUI REST API, execution plans, task distributions vs watermarks, gate results, DLQ records)
- **Dual conditional edges** — `should_continue` (inner: tools or reflect?) + `should_revise` (outer: report or back to llm?), creating a two-level routing structure
- **String-based reflection routing** — `should_revise` checks for "NEEDS_MORE_INVESTIGATION" vs "HYPOTHESIS_CONFIRMED" keywords instead of structured output, preserving reflection quality
- **Causal chain reasoning** — prompt engineering technique that teaches the agent to trace symptom → intermediate effect → root cause chains instead of blaming surface symptoms
- **Separate reflection prompt** — a dedicated prompt (without tool bindings) that forces the LLM to THINK rather than ACT, preventing premature tool calls during self-evaluation
- **Report metadata enrichment** — `_meta` dict in the report carries iteration count, reflection count, and final hypothesis for audit trail
- **Two simulated scenarios** — data skew (hot key on join) and systemic GC pressure (insufficient memory + disabled AQE), exercising different diagnostic paths
```

### 4. Environment — add Spark debugger live test command

Add under the environment section:
```
- Run Spark Debugger live test: `python experiments/spark_debugger_live_test.py`
```
