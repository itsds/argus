# Code Guide: `tests/agents/test_backfill_agent.py`

## Purpose
Unit tests for the Incident & Backfill Planning agent. This is the first test file in Argus that tests a **two-phase invoke lifecycle** — invoke() → needs_approval → resume() → success — which requires fundamentally different test patterns than the single-invoke Recon and DLQ agent tests.

## What You'll Learn
- How to mock a LangGraph compiled graph with **interrupt detection** (graph.get_state().next)
- How to test a **stateful agent** that persists instance variables across method calls
- How to verify a **rejection loop** (invoke → reject → reject → approve)
- How to test **guard rails** (ValueError when resume() called without invoke())
- How to test **lifecycle cleanup** (_clear_phase_state resets instance state)

## Architecture

```
test_backfill_agent.py
├── Fixtures (helpers)
│   ├── _make_config()          — Minimal ArgusConfig (no config files)
│   ├── _make_context()         — Standard TriggerContext for backfill
│   ├── _make_sample_plan()     — Sample BackfillPlan (Pydantic model)
│   ├── _make_interrupted_state() — Graph state at interrupt point
│   ├── _make_completed_state() — Graph state after execution completes
│   └── _make_mock_graph()      — Mock graph with invoke() + get_state()
│
├── TestAgentProperties         — BaseAgent contract (name, description, lazy init)
├── TestMakeInitialState        — State factory (required fields, defaults, HITL fields)
├── TestExtractToolCalls        — Tool name extraction from message history
├── TestInvokeInterrupt         — Phase 1: invoke → needs_approval (8 tests)
├── TestInvokeFailure           — Phase 1 error handling (2 tests)
├── TestResumeApproval          — Phase 2: resume(approved) → success (5 tests)
├── TestResumeRejection         — Rejection loop (4 tests)
├── TestResumeFailure           — Phase 2 error handling (4 tests)
├── TestLazyGraphBuilding       — Graph built once, cached (2 tests)
├── TestConfigIntegration       — Config → agent plumbing (2 tests)
├── TestClearPhaseState         — Lifecycle cleanup (2 tests)
└── TestAgentResultStructure    — Result field validation (3 tests)
```

## Key Concepts

### 1. Mocking Interrupt Detection
The most important mock in this file. The real graph uses `graph.get_state(config).next` to detect interrupts — a non-empty tuple means paused, empty means completed:

```python
def _make_mock_graph(invoke_return, is_interrupted):
    mock_graph = MagicMock()
    mock_graph.invoke.return_value = invoke_return
    
    mock_state_snapshot = MagicMock()
    # THIS IS THE KEY MOCK:
    # .next = ("approval_gate",)  → graph is paused at interrupt
    # .next = ()                  → graph completed (reached END)
    mock_state_snapshot.next = ("approval_gate",) if is_interrupted else ()
    mock_graph.get_state.return_value = mock_state_snapshot
    
    return mock_graph
```

**Why this matters**: Without mocking `get_state().next`, you can't test whether `_process_graph_result()` correctly detects interrupts vs completions. This is the agent-level expression of LangGraph's checkpoint system.

### 2. Two-Phase Test Pattern
Unlike Recon/DLQ tests (one invoke() call per test), Backfill tests set up the mock graph TWICE — once for invoke, once for resume:

```python
# Phase 1: invoke → interrupt
mock_graph = _make_mock_graph(invoke_return=interrupted_state, is_interrupted=True)
agent._graph = mock_graph
result = await agent.invoke(context)
assert result.status == "needs_approval"

# Phase 2: resume → completion (CHANGE the mock's behavior)
mock_graph.invoke.return_value = completed_state
mock_state = MagicMock()
mock_state.next = ()  # now it's completed
mock_graph.get_state.return_value = mock_state

result = await agent.resume("approved")
assert result.status == "success"
```

### 3. Rejection Loop Test
The most complex test — verifies the full cycle: invoke → reject → reject → approve:

```python
# invoke → needs_approval
# resume(rejected) → needs_approval (revised plan)  
# resume(approved) → success
```

Each resume() call changes the mock's return values to simulate the graph's different behaviors.

### 4. Guard Rail Test
Tests that resume() without invoke() raises ValueError:

```python
agent = BackfillAgent(config)
with pytest.raises(ValueError, match="No pending approval"):
    await agent.resume("approved")
```

### 5. Command(resume=...) Verification
Tests that resume() passes a LangGraph Command object (not a state dict) to graph.invoke():

```python
from langgraph.types import Command
resume_call = mock_graph.invoke.call_args_list[1]  # second invoke call
command_arg = resume_call[0][0]
assert isinstance(command_arg, Command)
```

## Comparison: Backfill Tests vs Recon/DLQ Tests

| Aspect | Recon/DLQ Tests | Backfill Tests |
|--------|----------------|----------------|
| invoke() calls per test | 1 | 1-4 (invoke + resume cycles) |
| Mock graph setup | invoke.return_value only | invoke.return_value + get_state().next |
| Instance state checks | None (stateless) | _thread_id, _context, _started_at |
| Lifecycle cleanup | Not needed | _clear_phase_state() verification |
| Guard rails | None | ValueError on premature resume() |
| Result statuses | "success" / "failure" | + "needs_approval" |

## Dependencies
- `pytest`, `pytest-asyncio` — async test framework
- `unittest.mock` — MagicMock, patch
- `langchain_core.messages` — AIMessage, HumanMessage, SystemMessage, ToolMessage
- `langgraph.types` — Command (verified in resume tests)
- `argus.agents.backfill.agent` — BackfillAgent, _extract_tool_calls
- `argus.agents.backfill.state` — make_initial_state
- `argus.agents.base` — AgentResult, TriggerContext
- `argus.schemas.reports` — BackfillPlan, BackfillStep, Notification, Severity

## Running
```bash
# Run all backfill agent tests
pytest tests/agents/test_backfill_agent.py -v

# Run a specific test class
pytest tests/agents/test_backfill_agent.py::TestResumeRejection -v

# Run with async output
pytest tests/agents/test_backfill_agent.py -v -s
```
