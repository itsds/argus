# Code Guide: `argus/agents/base.py`

> **Read this BEFORE opening `argus/agents/base.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **agent contract** — the abstract base class and data models that every Argus agent must implement. It defines three things: what an agent receives (TriggerContext), what it returns (AgentResult), and what interface it must expose (BaseAgent).

---

## What Feature Does It Bring to Argus?

1. **Uniform agent interface** — all four agents (Recon, Backfill, DLQ, Spark) share the same invoke/result pattern, making them interchangeable from the router's perspective
2. **Structured input** — `TriggerContext` standardizes how agents receive invocation data (run date, source, params)
3. **Structured output** — `AgentResult` provides a consistent envelope (status, timing, report, errors) that downstream systems (notifications, audit log) can process uniformly
4. **Extension point** — new agents inherit from `BaseAgent` and only implement the abstract methods

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **Pydantic `BaseModel`** | `TriggerContext` and `AgentResult` — data validation, serialization (`.model_dump_json()`), and schema generation |
| **`abc.ABC` + `@abstractmethod`** | Python's abstract base class mechanism — enforces that subclasses implement required methods |
| **`datetime` (UTC)** | Timestamps for `AgentResult.started_at` / `completed_at` — always UTC for consistency |
| **`Field(default_factory=dict/list)`** | Pydantic pattern for mutable default values (avoids the shared-mutable-default bug) |

---

## Class-by-Class Breakdown

### `TriggerContext(BaseModel)` — What the Agent Receives

```python
class TriggerContext(BaseModel):
    agent_name: str              # "reconciliation", "backfill", etc.
    trigger_source: str          # "airflow_callback" | "cli" | "api"
    run_date: str                # "2026-09-30" (ISO format)
    correlation_id: str = ""     # for log tracing
    params: dict[str, Any]       # trigger-specific data
```

**Why Pydantic?** Because trigger data comes from external sources (CLI arguments, API requests, Airflow callbacks), and Pydantic validates the data at construction time. If you pass `run_date=123`, Pydantic raises a clear error.

**`params` dict**: This is the extensible part — different triggers carry different data:
- Airflow gate failure: `{"gate_failure": "gate_3", "dag_id": "ttag_daily_dag"}`
- CLI: `{"dag_id": "ttag_main"}`
- DLQ alert: `{"dlq_threshold_breached": True, "queue": "booking_dlq"}`

### `AgentResult(BaseModel)` — What the Agent Returns

```python
class AgentResult(BaseModel):
    agent_name: str
    correlation_id: str
    status: str                  # "success" | "failure" | "needs_approval"
    started_at: datetime
    completed_at: datetime
    report: dict[str, Any]       # the agent-specific output
    actions_taken: list[str]
    errors: list[str]
```

**Why `report: dict[str, Any]` instead of a typed report?** Because each agent produces a different report type (ReconReport, BackfillPlan, etc.). The base result uses a generic dict, and the agent-specific code knows the actual type. This keeps the base class provider-agnostic.

**`status` values**:
- `"success"` — investigation complete, report produced
- `"failure"` — agent crashed or couldn't complete
- `"needs_approval"` — agent produced a plan that needs human approval (future: backfill plans)

### `BaseAgent(ABC)` — The Interface Contract

```python
class BaseAgent(ABC):
    def __init__(self, config: ArgusConfig): ...

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def description(self) -> str: ...

    @abstractmethod
    def build_graph(self): ...

    @abstractmethod
    async def invoke(self, context: TriggerContext) -> AgentResult: ...

    def _make_result(self, ...): ...
```

**Abstract methods** (must be implemented by every agent):
- `name` — identifier for routing and logging ("reconciliation")
- `description` — one-liner for display ("Reconciliation Diagnostics")
- `build_graph()` — constructs the LangGraph StateGraph for this agent
- `invoke(context)` — runs the agent and returns results (async because LLM calls are async)

**Concrete methods** (shared by all agents):
- `_make_result()` — helper to build an `AgentResult` with common fields filled in

---

## Code Flow

```
# Startup
agent = ReconAgent(config)       # calls BaseAgent.__init__(config)

# Registration
router.register(agent)           # uses agent.name to register

# Invocation
result = await agent.invoke(context)
    │
    ▼ (inside the agent's invoke())
    started_at = datetime.now(UTC)
    graph = self.build_graph()
    state = graph.invoke(initial_state)
    return self._make_result(context, "success", state["report"], started_at)
```

---

## Who Calls / Imports This File?

- **Every agent implementation** → `class ReconAgent(BaseAgent)` inherits from this
- **`core/router.py`** → imports `BaseAgent` for type hints and `TriggerContext` for routing
- **`cli/main.py`** → imports `TriggerContext` to construct the trigger from CLI arguments
- **Tests** → create `TriggerContext` instances to test agents

---

## Where Does This File Fit?

```
argus/agents/
├── base.py              <── YOU ARE HERE (contract for all agents)
├── reconciliation/      <── Recon agent (inherits from BaseAgent)
│   ├── state.py
│   ├── prompts.py
│   ├── graph.py         (upcoming)
│   └── agent.py         (upcoming — will class ReconAgent(BaseAgent))
├── backfill/            (future)
├── dlq_triage/          (future)
└── spark_debugger/      (future)
```

---

## Key Concepts to Understand

1. **Abstract Base Classes (ABC)**: `ABC` + `@abstractmethod` is Python's way of saying "you MUST implement these methods." If you write `class ReconAgent(BaseAgent)` without implementing `build_graph()`, Python raises `TypeError` when you try to instantiate it. This catches errors at construction time, not at runtime.

2. **`@property @abstractmethod` stacking**: Both decorators work together — `name` must be implemented as a property (not a regular method). This means subclasses use `@property` to define it, and callers access it as `agent.name` (not `agent.name()`).

3. **`async def invoke`**: The `async` keyword means this method returns a coroutine, and must be `await`ed. Agents are async because:
   - LLM API calls are I/O-bound (network requests)
   - Multiple agents might run concurrently in Phase 6
   - LangGraph's async execution enables non-blocking tool calls

4. **`_make_result()` helper**: The underscore prefix is a Python convention for "private" (internal use). It's a template method — it handles the boilerplate (filling in `agent_name`, `correlation_id`, `completed_at`) so each agent only supplies the unique parts (status, report, errors).

5. **`Field(default_factory=dict)`**: Why not `params: dict = {}`? Because Python shares mutable defaults across instances. If two `TriggerContext` objects modified `params`, they'd modify the same dict. `default_factory=dict` creates a fresh dict for each instance. This is a classic Python gotcha that Pydantic handles via the `Field` descriptor.
