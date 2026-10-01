# Code Guide: `argus/core/router.py`

> **Read this BEFORE opening `argus/core/router.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **agent router** — the traffic controller that decides which agent handles an incoming trigger. When Airflow reports a gate failure or a CLI user invokes an agent, the router maps that request to the right agent instance.

---

## What Feature Does It Bring to Argus?

1. **Central dispatch** — one place that knows about all registered agents, decoupling trigger sources from agent implementations
2. **Two routing modes** — direct dispatch (caller names the agent) and rules-based routing (params determine the agent)
3. **Extensibility** — Phase 6 will replace rules-based routing with an LLM-based supervisor agent; the router's interface stays the same
4. **Registration pattern** — agents register themselves at startup; the router doesn't hardcode which agents exist

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **Registry pattern** | `_agents` dict maps agent names to instances — the router doesn't know agent internals |
| **Strategy pattern** | The routing logic (rules now, LLM later) is encapsulated in `route()` and can be swapped |
| **Python `dict`** | The agent registry — simple and fast O(1) lookup by name |

---

## Code Flow

### Startup: Registration

```
router = AgentRouter()
router.register(recon_agent)     # recon_agent.name → "reconciliation"
router.register(backfill_agent)  # backfill_agent.name → "backfill"
    │
    ▼
_agents = {
    "reconciliation": <ReconAgent instance>,
    "backfill": <BackfillAgent instance>,
}
```

### Runtime: Routing

```
context = TriggerContext(agent_name="reconciliation", ...)
agent = router.route(context)
    │
    ├── agent_name is set and registered?
    │       → YES → return _agents["reconciliation"]  (direct dispatch)
    │
    └── NO → check params:
            ├── params.gate_failure?     → reconciliation
            ├── params.dlq_threshold?    → dlq_triage
            ├── params.backfill_req?     → backfill
            ├── params.spark_app_id?     → spark_debugger
            └── none matched             → raise ValueError
```

---

## Method-by-Method Breakdown

### `register(agent: BaseAgent) -> None`

Stores an agent instance by its `name` property. Logs the registration. Called at application startup before any routing happens.

**Why a method, not constructor injection?** Because agents are created one by one (each needs config, and some may depend on others). The register-after-creation pattern is more flexible.

### `route(context: TriggerContext) -> BaseAgent`

The main routing method. Two strategies in priority order:

1. **Direct dispatch**: If `context.agent_name` is set and matches a registered agent, return it immediately. This is used by the CLI (`argus invoke recon ...`) and the future API.

2. **Rules-based routing**: If no agent name is given, inspect `context.params` for known keys:
   - `gate_failure` → reconciliation agent
   - `dlq_threshold_breached` → DLQ triage agent
   - `backfill_requested` → backfill agent
   - `spark_app_id` → Spark debugger agent

   This is how **Airflow callbacks** will trigger agents — they don't name the agent, they describe the failure, and the router figures out who handles it.

### `_dispatch(agent_name, context) -> BaseAgent`

Private helper that does the actual lookup + logging. Raises `ValueError` if the agent isn't registered — this catches configuration errors early (e.g., forgot to register an agent at startup).

### `registered_agents` property

Returns a list of agent names. Used by `cli/main.py`'s `list-agents` command (future: will read from router instead of hardcoding).

---

## Who Calls This File?

- **`cli/main.py`** → will create a router, register agents, and call `route()` (currently a TODO)
- **Future: API server** → `POST /invoke` will create a `TriggerContext` and call `router.route()`
- **Future: Airflow callbacks** → will send trigger params to the API, which routes to the right agent

---

## Where Does This File Fit?

```
                 ┌──────────────┐
                 │ Trigger      │   CLI, API, Airflow callback
                 │ Sources      │
                 └──────┬───────┘
                        │
                        ▼
                 ┌──────────────┐
                 │ AgentRouter  │   <── YOU ARE HERE
                 │ route()      │
                 └──────┬───────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
    ┌──────────┐  ┌──────────┐  ┌──────────┐
    │ Recon    │  │ Backfill │  │ DLQ      │  ... agents
    │ Agent    │  │ Agent    │  │ Agent    │
    └──────────┘  └──────────┘  └──────────┘
```

The router sits between trigger sources and agents. It's the **single point of dispatch** — no trigger source talks to an agent directly.

---

## Key Concepts to Understand

1. **Registry pattern**: Instead of a switch statement that hardcodes agents, the router maintains a dynamic registry. New agents are added by calling `register()` — no router code changes needed. This is the Open/Closed Principle: open for extension (add agents), closed for modification (don't change router logic).

2. **Rules-based vs LLM-based routing**: Phase 1 uses deterministic rules (if key exists in params → specific agent). Phase 6 will use an LLM supervisor that can:
   - Chain multiple agents (investigate recon, then plan backfill)
   - Handle ambiguous triggers
   - Learn from past routing decisions

   The `route()` method signature stays the same — only the internals change.

3. **Why raise ValueError instead of returning None?**: A failed route is a bug, not a normal case. Every trigger should match an agent. Raising forces the caller to handle the error explicitly, rather than passing `None` downstream where it would cause a confusing `AttributeError`.

4. **Separation of concerns**: The router knows agent names and routing rules. It does NOT know how agents work internally. Agents know how to investigate. They do NOT know how they were selected. This separation makes both independently testable.
