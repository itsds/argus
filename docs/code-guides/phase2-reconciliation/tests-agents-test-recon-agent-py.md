# Code Guide: `tests/agents/test_recon_agent.py`

> **Read this AFTER reading the agent.py guide.**
> This guide explains how to test an LLM-powered agent without making real API calls.

---

## What Is This File About?

This is the **unit test suite** for the Reconciliation Diagnostics agent. It tests every aspect of the agent — properties, state factory, helper functions, success paths, failure paths, lazy initialization, and config integration — WITHOUT making real LLM API calls.

---

## What Feature Does It Bring to Argus?

1. **Confidence** — proves the agent contract works before you spend API credits on live tests
2. **Regression safety** — if you change `agent.py` or `graph.py`, the tests catch breakage immediately
3. **Documentation** — tests are executable examples of how the agent behaves in each scenario
4. **Fast feedback** — runs in < 1 second (no network calls), so you can run after every change

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **pytest** | Test runner — auto-discovers `test_*.py` files, runs classes and functions |
| **pytest-asyncio** | Enables `async def test_*()` — needed because `invoke()` is async |
| **`unittest.mock.MagicMock`** | Creates fake objects that record how they're called |
| **`unittest.mock.patch`** | Temporarily replaces a real object with a mock during one test |
| **`@pytest.mark.asyncio`** | Marks a test as async so pytest-asyncio knows to `await` it |

---

## Why Mock the LLM?

This is the most important concept in this file. Consider what happens if we use a real LLM:

```
Test run 1: LLM calls query_gate_results → compare_row_counts → PASS
Test run 2: LLM calls query_gate_results → check_duplicate_keys → compare_row_counts → PASS
Test run 3: LLM hallucinates a tool name → FAIL
```

The LLM is **non-deterministic**. Even with `temperature=0`, responses can vary. You can't write `assert result == exact_string` because the string changes every run.

**Solution: mock the graph entirely.** We inject a `MagicMock` as `agent._graph` that returns a predictable final state dict. Now we're testing:
- Does `invoke()` correctly translate `TriggerContext` to initial state?
- Does `invoke()` correctly extract the report from final state?
- Does `invoke()` handle exceptions gracefully?
- Does `_extract_tool_calls()` parse messages correctly?

The LLM reasoning itself is tested in `recon_live_test.py` (live integration test).

---

## Test Organization — Deep Dive

### Test Fixtures (Helper Functions)

```python
def _make_config(overrides=None) -> ArgusConfig:
def _make_context(run_date="2026-09-28", gate="gate_3") -> TriggerContext:
def _make_sample_report() -> ReconReport:
```

**Why helper functions, not `@pytest.fixture`?** Fixtures are great for shared setup that's the same across all tests. But here, several tests need slightly different configs or contexts. Helper functions with parameters are more flexible — each test calls `_make_config({...})` with its own overrides.

**Why not `load_config("dev")`?** Unit tests should be self-contained. If someone renames `configs/dev/config.yaml`, the tests shouldn't break — they're testing the agent, not the config file.

---

### `TestExtractToolCalls` — Testing a Pure Function

```python
def test_multiple_tool_calls_across_messages(self):
    messages = [
        AIMessage(content="...", tool_calls=[{"name": "query_gate_results", ...}]),
        ToolMessage(content='...', tool_call_id="tc1"),
        AIMessage(content="...", tool_calls=[
            {"name": "compare_row_counts", ...},
            {"name": "check_duplicate_keys", ...},
        ]),
    ]
    result = _extract_tool_calls(messages)
    assert result == ["query_gate_results", "compare_row_counts", "check_duplicate_keys"]
```

This is the simplest kind of test — a pure function (input → output, no side effects). We construct message lists with known tool_calls and verify the function extracts names correctly.

**Key edge cases tested:**
- Empty message list → `[]`
- Messages with no tool_calls → `[]`
- Non-AI messages (System, Human, Tool) → ignored
- Multiple tool_calls in a single AIMessage → all extracted
- Tool calls across multiple AIMessages → accumulated in order

---

### `TestAgentInvokeSuccess` — Testing the Happy Path

```python
async def test_success_returns_agent_result(self):
    agent._graph = mock_graph  # bypass build_graph()
    result = await agent.invoke(context)
    assert result.status == "success"
    assert result.report["gate_failed"] == "gate_3"
```

**The mock injection pattern:**

```python
mock_graph = MagicMock()
mock_graph.invoke.return_value = { ... final state dict ... }
agent._graph = mock_graph  # inject directly — skips build_graph()
```

By setting `agent._graph` directly, we skip the lazy `build_graph()` call entirely. The mock's `invoke()` returns a predictable state dict, so we can assert exact values in the AgentResult.

**Why check `isinstance(result.report, dict)`?** The platform layer is agent-type-agnostic. It knows `AgentResult.report` is a `dict[str, Any]`, not a `ReconReport`. The `.model_dump()` conversion in `agent.py` is what makes this work — the test verifies it happened.

---

### `TestAgentInvokeFailure` — Testing Error Boundaries

Three failure scenarios:

1. **Graph exception** — `mock_graph.invoke.side_effect = RuntimeError(...)` simulates an LLM API crash. The agent should catch it and return `status="failure"`.

2. **None report** — The graph completes but `report` is `None` (structured output failed). The agent should return a failure with an error message.

3. **Missing gate_failure** — `params={}` has no `gate_failure` key. The agent should default to `"unknown"` and still run successfully.

**Why test these?** In production, LLM APIs fail regularly (rate limits, network issues, malformed responses). An agent that crashes instead of returning a structured failure is unusable — the platform can't log, retry, or alert on an unhandled exception.

---

### `TestLazyGraphBuilding` — Testing Initialization

```python
with patch.object(agent, "build_graph", return_value=mock_graph) as mock_build:
    await agent.invoke(context)
    await agent.invoke(context)
    mock_build.assert_called_once()  # NOT twice
```

**`patch.object` vs direct injection:** Here we can't just set `agent._graph` because we're testing that `invoke()` calls `build_graph()` exactly once. `patch.object` replaces the method temporarily and tracks calls.

**`assert_called_once()`** — This is the key assertion. If the graph were rebuilt on every invoke, we'd waste resources creating LLM clients and compiling the graph topology. The lazy init pattern builds once and caches.

---

## Running the Tests

```bash
# From the repo root:
pytest tests/agents/test_recon_agent.py -v

# Run a specific test class:
pytest tests/agents/test_recon_agent.py::TestExtractToolCalls -v

# Run a single test:
pytest tests/agents/test_recon_agent.py::TestAgentInvokeSuccess::test_success_returns_agent_result -v
```

The `-v` flag shows each test name and PASS/FAIL status.

---

## How This Connects to the Bigger Picture

```
Unit tests (this file)          Live test (recon_live_test.py)
  ↓ Tests structure               ↓ Tests reasoning
  ↓ Mocked LLM                    ↓ Real LLM
  ↓ Fast (< 1s)                   ↓ Slow (10-30s per scenario)
  ↓ Deterministic                  ↓ Non-deterministic
  ↓ Run on every commit            ↓ Run manually / in staging
```

Both test types are necessary. Unit tests catch structural regressions instantly. Live tests catch prompt engineering issues that only surface with a real LLM.
