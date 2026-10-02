# Code Guide: `tests/agents/test_dlq_agent.py`

> **Read this BEFORE opening `tests/agents/test_dlq_agent.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **unit test suite for the DLQ Triage agent** — 7 test classes that verify the agent's properties, state factory, helper functions, invoke success/failure paths, lazy graph building, and config integration. All tests use mocked LLM calls — no real API calls are made.

---

## What Feature Does It Bring to Argus?

1. **Contract validation** — proves the DLQ agent satisfies the BaseAgent interface
2. **State factory correctness** — verifies `make_initial_state()` sets all defaults correctly
3. **Error boundary testing** — confirms the agent returns structured failures, never crashes
4. **Regression safety** — catches breaks when modifying the agent's internals

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **pytest** | Test runner and assertions |
| **`pytest.mark.asyncio`** | Marks async tests for pytest-asyncio |
| **`unittest.mock.MagicMock`** | Mocks the compiled graph to avoid real LLM calls |
| **`unittest.mock.patch`** | Patches `build_graph()` to verify lazy initialization |
| **Pydantic test models** | `_make_sample_report()` creates a typed `DLQTriageReport` |

---

## The 7 Test Classes

| # | Class | What It Tests |
|---|---|---|
| 1 | `TestAgentProperties` | `name == "dlq_triage"`, description, `_graph is None` on init |
| 2 | `TestMakeInitialState` | Required fields, defaults (empty lists, iteration=0), custom max_iterations |
| 3 | `TestExtractToolCalls` | Empty messages, no tool calls, single/multiple tool calls, ignores non-AI messages |
| 4 | `TestAgentInvokeSuccess` | Mocked graph → AgentResult with correct report fields and tool audit |
| 5 | `TestAgentInvokeFailure` | Graph exception → failure, None report → failure, missing source_lane → default "both" |
| 6 | `TestLazyGraphBuilding` | Graph built on first invoke, cached across subsequent invokes |
| 7 | `TestConfigIntegration` | `agents.dlq_triage.max_iterations` flows through; falls back to `agents.max_iterations` |

---

## Test Pattern: Same as Recon

The test structure mirrors `test_recon_agent.py` almost exactly. This is intentional — when two agents share the same interface, their tests SHOULD look almost identical. It means the BaseAgent abstraction is working.

### What's the same:
- 7 test classes covering the same concerns
- Mock strategy: `agent._graph = mock_graph` bypasses graph building
- Fixtures: `_make_config()`, `_make_context()`, `_make_sample_report()`
- Async tests with `@pytest.mark.asyncio`

### What's different:
- `source_lane` instead of `gate_name` in context and state
- `DLQTriageReport` fields: `auto_requeued`, `quarantined`, `escalated` (not `root_cause`, `confidence`)
- Default `source_lane` is `"both"` (not `"unknown"`)
- Report records have `classification`, `confidence`, `reason`, `action_taken`

---

## Key Test Fixtures

### `_make_config()`

Creates a minimal `ArgusConfig` without reading config files:

```python
{
    "llm": {"provider": "google", "model": "gemini-2.0-flash", "temperature": 0.0},
    "agents": {"dlq_triage": {"max_iterations": 5}},
    "logging": {"level": "WARNING"},
}
```

### `_make_context()`

Creates a standard `TriggerContext` for testing:

```python
TriggerContext(
    agent_name="dlq_triage",
    trigger_source="test",
    run_date="2026-09-30",
    params={"dlq_threshold_breached": True, "source_lane": "kafka_dlq"},
)
```

### `_make_sample_report()`

Creates a typed `DLQTriageReport` with 3 records covering 3 classifications. Used to mock the graph's return value.

---

## Mock Injection Pattern

```python
mock_graph = MagicMock()
mock_graph.invoke.return_value = {
    "messages": [...],
    "report": _make_sample_report(),
    "errors": [],
    ...
}
agent._graph = mock_graph  # bypass lazy graph building
```

This pattern:
1. Creates a mock that mimics the compiled graph's `.invoke()` return
2. Injects it directly via `agent._graph`, skipping `build_graph()`
3. The agent's `invoke()` calls `self._graph.invoke(initial_state)` and gets the mocked response

This is why `_graph` is a simple attribute, not a property with a getter — it allows direct test injection.

---

## Key Test: Missing source_lane Defaults to "both"

```python
async def test_missing_source_lane_defaults_to_both(self):
    context = TriggerContext(
        params={"dlq_threshold_breached": True},  # no source_lane!
    )
    ...
    call_args = mock_graph.invoke.call_args[0][0]
    assert call_args["source_lane"] == "both"
```

This verifies the agent's defensive default — when the trigger doesn't specify a lane, the agent checks both. Important because an Airflow callback might not always include `source_lane`.

---

## How This Connects

- **`agent.py`** — the code under test
- **`state.py`** — `make_initial_state()` tested in `TestMakeInitialState`
- **`base.py`** — `BaseAgent`, `TriggerContext`, `AgentResult` tested indirectly
- **`reports.py`** — `DLQTriageReport`, `DLQRecord`, `DLQClassification` used in fixtures
- **`test_recon_agent.py`** — parallel test file to compare patterns against
