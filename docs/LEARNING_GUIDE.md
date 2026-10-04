# Argus Learning Guide

> A living document that grows with the codebase. Every file in this repo
> teaches specific AI agent and data engineering concepts. This guide maps
> each file to what it teaches, explains the "why" behind every design
> decision, and connects the concepts so you can understand the full picture
> without reaching for ChatGPT or Claude.

**How to use this guide:**
Read it alongside the code. Each section corresponds to a development phase.
Within each phase, files are listed in the order they should be read — each
one builds on concepts from the previous. The code comments go deep on
implementation; this guide focuses on the *architecture-level why*.

---

## Table of Contents

- [The Big Picture](#the-big-picture)
- [Prerequisites](#prerequisites)
- [Phase 0: Agent Foundations](#phase-0-agent-foundations)
- [Phase 1: Platform Skeleton](#phase-1-platform-skeleton)
- [Phase 2: Reconciliation Diagnostics Agent](#phase-2-reconciliation-diagnostics-agent)
- [Phase 3: DLQ Triage & Auto-Remediation Agent](#phase-3-dlq-triage--auto-remediation-agent)
- [Phase 4: Incident & Backfill Planning Agent](#phase-4-incident--backfill-planning-agent)
- [Concept Index](#concept-index)
- [Glossary](#glossary)

---

## The Big Picture

Argus is a four-agent diagnostic platform that sits on top of the TTAG
(Transactions Authorization Tag) data pipeline. Each agent watches a
different failure surface:

| Agent | What It Watches | Workflow Pattern |
|-------|----------------|------------------|
| Reconciliation Diagnostics | Gate 3/4 count mismatches | ReAct (investigate loop) |
| DLQ Triage | Dead letter queue records | ReAct + Classification |
| Incident & Backfill Planning | Pipeline incidents needing recovery | Plan-then-Execute + Human-in-the-Loop |
| AI Spark Debugger | Spark job performance issues | ReAct + Reflection |

### Why Four Agents Instead of One?

Each agent has a **bounded responsibility** — it's an expert at one type of
failure. This matters for two reasons:

1. **Prompt engineering** — a focused system prompt outperforms a generic one.
   An agent told "you are a reconciliation specialist" reasons better about
   row count mismatches than one told "you handle all pipeline issues."

2. **Tool scoping** — each agent gets only the tools it needs. Fewer tools =
   fewer chances for the LLM to pick the wrong one. The Recon agent doesn't
   see Spark tools; the Spark Debugger doesn't see DLQ tools.

### The Six Layers

```
┌─────────────────────────────────────────────────────────┐
│  Layer 1: TRIGGERS                                      │
│  Airflow callback  │  CLI  │  REST API                  │
├─────────────────────────────────────────────────────────┤
│  Layer 2: ROUTER                                        │
│  Rules-based dispatcher → which agent handles this?     │
├─────────────────────────────────────────────────────────┤
│  Layer 3: AGENTS                                        │
│  LangGraph StateGraph per agent (nodes + edges + state) │
├─────────────────────────────────────────────────────────┤
│  Layer 4: TOOLS                                         │
│  @tool functions — the agent's hands and eyes           │
├─────────────────────────────────────────────────────────┤
│  Layer 5: INFRASTRUCTURE                                │
│  Iceberg, Kafka, Spark History, Airflow, Snowflake      │
├─────────────────────────────────────────────────────────┤
│  Layer 6: OUTPUT                                        │
│  Structured reports, notifications, audit logs          │
└─────────────────────────────────────────────────────────┘
```

---

## Prerequisites

Before diving in, you should be comfortable with:

- **Python 3.11+** — type hints, dataclasses, `async/await`, context managers
- **Pydantic v2** — `BaseModel`, `Field`, validators, `.model_dump_json()`
- **Basic LLM concepts** — what a prompt is, what tokens are, what "temperature" means

These concepts are *taught* by the codebase (you don't need to know them beforehand):

- LangGraph and LangChain (the agent framework)
- The ReAct pattern (how agents reason)
- Tool calling (how LLMs interact with external systems)
- Prompt engineering for agents (not the same as prompt engineering for chat)
- Data pipeline reconciliation, Iceberg internals, Spark debugging

---

## Phase 0: Agent Foundations

> **Goal:** Learn the ReAct loop with a throwaway calculator agent before
> building anything production-shaped.

### `experiments/calculator_agent.py`

**Concepts taught:** ReAct pattern, tool calling, LangGraph basics

This was the "hello world" of agent development. A simple agent with
arithmetic tools (`add`, `multiply`, `divide`) that demonstrates the core
loop every agent uses:

```
        ┌──────────┐
        │  REASON  │  LLM reads the conversation, decides what to do
        └────┬─────┘
             │
             ▼
        ┌──────────┐
        │   ACT    │  LLM calls a tool (e.g. multiply(6, 7))
        └────┬─────┘
             │
             ▼
        ┌──────────┐
        │ OBSERVE  │  Tool result (42) goes back to LLM as a message
        └────┬─────┘
             │
             ▼
        Should I call another tool?
           YES → loop back to REASON
           NO  → produce final answer
```

**Key insight:** The LLM doesn't execute code. It *asks* for a tool to be
called by emitting a structured `tool_calls` field in its response. The
framework catches that, runs the tool, and feeds the result back as a
`ToolMessage`. The LLM then decides what to do next.

**Why Gemini free tier?** During learning, you'll make hundreds of LLM calls
that produce garbage while you debug prompts and tool designs. Using
`gemini-2.0-flash` via Google's free tier means you can iterate without
worrying about cost.

---

## Phase 1: Platform Skeleton

> **Goal:** Build the shared infrastructure that all four agents will use —
> config, LLM client, logging, routing, schemas, CLI.

### Reading order and concept map:

```
pyproject.toml ─── Python packaging, dependency management
    │
    ▼
argus/core/config.py ─── YAML config with env-var interpolation
    │
    ▼
argus/core/llm.py ─── LLM client factory (provider abstraction)
    │
    ▼
argus/core/logging.py ─── Structured JSON logging, correlation IDs
    │
    ▼
argus/agents/base.py ─── Abstract agent interface (TriggerContext, AgentResult)
    │
    ▼
argus/schemas/reports.py ─── Pydantic report models (all four agents)
    │
    ▼
argus/core/router.py ─── Rules-based agent dispatcher
    │
    ▼
argus/cli/main.py ─── Click CLI entry point
    │
    ▼
configs/dev/config.yaml ─── Dev environment settings
configs/prod/config.yaml ─── Prod environment settings
```

### `pyproject.toml` — Python Packaging

**Concepts taught:** Modern Python project setup, dependency management

Why `pyproject.toml` instead of `setup.py` + `requirements.txt`? Since
PEP 621 (2021), `pyproject.toml` is the standard. It puts project metadata,
dependencies, tool configs (ruff, pytest), and entry points in one file.

Key patterns:
- **Optional dependency groups** (`[project.optional-dependencies]`) — keep
  the base install light. `langchain-openai` is only needed if you use OpenAI;
  don't force everyone to install it.
- **Entry point** (`argus = "argus.cli.main:cli"`) — after `pip install -e .`,
  you get an `argus` command that maps to the Click CLI.

### `argus/core/config.py` — Configuration Management

**Concepts taught:** YAML config loading, environment variable interpolation,
dot-notation access

Why not just use environment variables directly? Because agent config is
hierarchical — `llm.provider`, `agents.reconciliation.max_iterations`,
`pipeline.iceberg.catalog_url`. Flat env vars can't express this cleanly.

The pattern: YAML files per environment (`configs/dev/`, `configs/prod/`),
with `${ENV_VAR}` syntax for secrets that shouldn't be in source control.
`ArgusConfig` wraps the dict with dot-notation access (`config.get("llm.provider")`).

### `argus/core/llm.py` — LLM Client Factory

**Concepts taught:** Provider abstraction, factory pattern, lazy imports

Why a factory? Because the code that *uses* the LLM shouldn't know whether
it's talking to Gemini, GPT-4, or Claude. `create_llm(config)` reads the
provider from config and returns the right `BaseChatModel` subclass.

**Lazy imports** — `from langchain_openai import ChatOpenAI` happens inside
the `if provider == "openai"` branch, not at the top of the file. If you're
using Gemini, you don't need `langchain-openai` installed at all.

### `argus/core/logging.py` — Structured Logging

**Concepts taught:** JSON structured logging, correlation IDs via `contextvars`

Why JSON logging instead of plain text? Because in production, logs go to
centralized systems (ELK, Datadog, Splunk) that need to parse, filter, and
aggregate. `{"level": "INFO", "agent": "recon", "action": "tool_call",
"tool": "compare_row_counts", "duration_ms": 342}` is parseable; a plain
string isn't.

**Correlation IDs** — every agent invocation gets a 12-character UUID prefix.
Every log line from that invocation carries it. When you're debugging a
failure in production, you grep for the correlation ID and get the complete
execution trace.

`contextvars.ContextVar` is how you thread the correlation ID through async
code without passing it as a parameter everywhere. It's Python's equivalent
of thread-local storage, but async-safe.

### `argus/agents/base.py` — Abstract Agent Interface

**Concepts taught:** Abstract base classes, Pydantic models, interface design

This is the contract every agent signs:

- **`TriggerContext`** — what the agent receives (who triggered it, for what
  run date, with what parameters). This is the same regardless of whether
  the trigger came from Airflow, the CLI, or the API.

- **`AgentResult`** — what the agent returns (status, structured report,
  actions taken, errors). Every agent's output fits this envelope, so the
  platform can handle results uniformly.

- **`BaseAgent`** — the abstract class. Subclasses must implement
  `build_graph()` (construct the LangGraph StateGraph) and `invoke(context)`
  (run it and return AgentResult).

**Design principle:** Code to an interface. The router doesn't know whether
it's dispatching to a Recon agent or a Spark Debugger — it only knows
`BaseAgent`. This is how you add a fifth agent without changing the platform.

### `argus/schemas/reports.py` — Structured Output Schemas

**Concepts taught:** Pydantic for structured LLM output, domain modeling

Each agent produces a different report type, but they share common building
blocks (`Severity`, `Notification`, `AuditEntry`). This file defines all of
them upfront so the schemas are reviewable in one place.

**Why Pydantic for LLM output?** Because LLMs produce text, and text is
unreliable. By defining a Pydantic schema and telling the LLM to produce
output matching it (via LangChain's `with_structured_output()`), you get:
- **Type safety** — `findings: list[ReconciliationFinding]`, not a free-form string
- **Validation** — Pydantic rejects malformed output before it reaches your code
- **IDE support** — `report.root_cause_summary` auto-completes

### `argus/core/router.py` — Agent Dispatcher

**Concepts taught:** Rules-based routing, registry pattern

Phase 1 routing is deliberately simple — `if params.get("gate_failure")` →
send to Recon agent. No LLM call, no ambiguity. This is intentional:

1. **Testable** — you can unit-test routing logic with no LLM dependency
2. **Fast** — no API call, no latency
3. **Debuggable** — when the wrong agent runs, the fix is a rule, not a prompt

Phase 6 will evolve this into a supervisor agent (LLM-based router that can
chain multiple agents), but starting with rules means you have a working
baseline to compare against.

### `argus/cli/main.py` — Command-Line Interface

**Concepts taught:** Click framework, CLI design

The CLI is the first trigger path to work. `argus invoke recon --run-date 2026-09-28`
creates a `TriggerContext` and dispatches it through the router. Currently
a skeleton — it prints the context and says "not yet implemented." As agents
come online, the CLI will wire them up.

---

## Phase 2: Reconciliation Diagnostics Agent

> **Goal:** Build the first real agent end-to-end. The Recon agent is the
> ideal starting point because it's pure investigation — no side effects,
> no human-in-the-loop, just a ReAct loop that gathers evidence and produces
> a diagnosis.

### Reading order and concept map:

```
argus/tools/pipeline/recon_tools.py ─── Tool layer (the agent's hands)
    │
    │   "What can the agent DO?"
    ▼
argus/agents/reconciliation/state.py ─── State schema (data flowing between nodes)
    │
    │   "What data does the graph carry?"
    ▼
argus/agents/reconciliation/prompts.py ─── System prompt (the agent's brain)
    │
    │   "What does the agent KNOW and HOW should it think?"
    ▼
argus/agents/reconciliation/graph.py ─── Graph topology (the wiring)
    │
    │   "How do the pieces connect?"
    ▼
argus/agents/reconciliation/agent.py ─── Agent class (the public interface)
    │
    │   "How does the platform invoke this agent?"
    ▼
tests/agents/test_recon_agent.py ─── Tests
experiments/recon_live_test.py ─── Live test with real LLM
```

### `argus/tools/pipeline/recon_tools.py` — Tool Layer

**Concepts taught:** `@tool` decorator, docstrings as prompt engineering,
simulated data for development

This file defines six tools the Recon agent can call. Each one queries a
different aspect of the TTAG pipeline to investigate reconciliation failures.

#### The Three Layers of Tool Design

1. **Function signature** — type hints tell the LLM what arguments to pass.
   `def compare_row_counts(run_date: str, tables: list[str]) -> str` — the
   LLM knows it needs a date and a list of table names.

2. **Docstring** — this is the most critical layer. The LLM reads the
   docstring to decide *whether* to call the tool and *how to interpret*
   the result. A good docstring answers: What does this tool do? When should
   I use it? What should I look for in the output?

3. **Return value** — always `str` (LangChain convention). The tool output
   goes back to the LLM as a `ToolMessage` in the conversation. JSON strings
   work well because the LLM can reason about structured data.

#### Why Simulated Data?

In dev, these tools return hardcoded data that mimics real pipeline failures.
Two scenarios are built in:

- **2026-09-28 (Gate 3 failure):** Bronze has 15,012 rows, Silver has 14,712.
  The drop is caused by 111 duplicate `booking_id`s from upstream re-delivery,
  plus a stale Silver watermark that's behind Bronze.

- **2026-09-27 (Gate 4 failure):** Gold has 15,102 rows, Snowflake has 15,087.
  The 15-row delta comes from NULL `card_sk` in FACT_TRAVEL_TAG — those cards
  exist in Silver but are missing from DIM_CARD.

These scenarios let you test the full agent loop without real infrastructure.
In production, the tools would query Iceberg via Spark SQL, Snowflake via
JDBC, and the watermark control table directly.

#### `RECON_TOOLS` List

The bottom of the file exports `RECON_TOOLS = [query_gate_results, ...]`.
This is the single import point for the agent graph:
```python
llm_with_tools = llm.bind_tools(RECON_TOOLS)
```
`.bind_tools()` serializes each tool's name, description, and parameter
schema into the LLM's function-calling format. When the LLM wants to call
a tool, it emits a structured `tool_calls` response referencing one of
these names.

### `argus/agents/reconciliation/state.py` — LangGraph State Schema

**Concepts taught:** LangGraph state channels, `Annotated` reducers,
`operator.add` for message accumulation

This is the data contract between every node in the graph. When the graph
runs, it passes this state dict from node to node.

#### The Reducer Pattern

The most important concept in this file is the **reducer annotation**:

```python
messages: Annotated[list[AnyMessage], operator.add]
```

Without the annotation, when a node returns `{"messages": [new_msg]}`,
LangGraph would *replace* the entire messages list. With `operator.add` as
the reducer, it *appends*. This is what makes the ReAct conversation work —
every LLM response and tool result accumulates into a growing history.

Think of it as:
```python
# What LangGraph does internally:
state["messages"] = operator.add(state["messages"], node_return["messages"])
# Which means:
state["messages"] = state["messages"] + [new_msg]
```

#### Two Categories of State Fields

- **Accumulating fields** (`messages`, `errors`) — use
  `Annotated[list, operator.add]`, grow over the graph's lifetime. Multiple
  nodes can append to them.

- **Scalar fields** (`run_date`, `gate_name`, `iteration`, `report`) — no
  reducer, so they use last-write-wins semantics. Set once by the entry node,
  or overwritten by a specific node (like `iteration` being incremented).

#### The Factory Function

`make_initial_state()` builds the seed dict for `graph.invoke()`. It exists
as a factory (not inline dict construction) because:
- Single place to set defaults (`iteration=0`, `report=None`)
- Type-checks required fields at the call site
- Easy to extend when new fields are added

### `argus/agents/reconciliation/prompts.py` — System Prompt

**Concepts taught:** Role anchoring, domain context injection, investigation
strategy, guardrails, `ChatPromptTemplate`

The system prompt is where you program the agent's behavior with natural
language. It has five sections, each serving a specific purpose:

#### 1. Role Definition
"You are a senior data engineer investigating a reconciliation failure"

This anchors the LLM's expertise level. Without it, the LLM defaults to a
generalist tone and might over-explain basic concepts instead of diving into
the pipeline-specific reasoning.

#### 2. Pipeline Architecture
The entire TTAG architecture — Bronze/Silver/Gold/Snowflake, table names,
gate definitions, watermark schema — is embedded in the prompt. The LLM
can't query "what is Gate 3?" during execution. Every fact it needs to
reason with must be provided upfront.

This is a core tradeoff: more tokens in the system prompt means higher per-call
cost, but every reasoning step is grounded in real architecture rather than
hallucinated structure.

#### 3. Investigation Strategy
A numbered sequence telling the LLM which tools to call and when:
1. Start with `query_gate_results`
2. Then `compare_row_counts`
3. If Bronze > Silver, check `check_duplicate_keys`
4. If Silver > Gold, check `check_fk_integrity`
5. Always check `query_watermark_gaps`
6. Use `query_iceberg_snapshots` for deeper investigation

Without this, the LLM might call tools in random order or skip critical
checks. With it, the investigation follows a diagnostic flowchart.

#### 4. Guardrails
- "NEVER suggest a fix you haven't verified" — prevents hallucinated root causes
- "Investigate with at least 2-3 tools before forming a hypothesis" — forces
  evidence gathering before conclusion
- "When results are ambiguous, call another tool to cross-check" — prevents
  premature convergence

These rules address the LLM's biggest failure mode: confident wrong answers.

#### 5. Prompt ↔ Tool Docstring Synergy
The prompt says *when* to call each tool (investigation strategy). The tool
docstrings say *what the tool does* and *how to interpret the result*. These
two layers work together — neither is complete alone:

```
System Prompt:  "If you see a Bronze-to-Silver drop, CHECK check_duplicate_keys"
                    ↕ (when to call)
Tool Docstring: "Checks for duplicate natural keys in Silver. Duplicates from
                 upstream re-delivery are a common cause of row count drops."
                    ↕ (what it does, what it means)
```

#### ChatPromptTemplate
`ChatPromptTemplate.from_messages()` composes the system and human messages:
- System message is static (pipeline architecture doesn't change per run)
- Human message has `{variables}` (`{run_date}`, `{gate_name}`,
  `{trigger_params}`) filled from graph state at runtime

The entry node calls `.invoke({...})` to produce the seed messages that
start the ReAct conversation.

### `argus/agents/reconciliation/graph.py` — LangGraph StateGraph

**Concepts taught:** StateGraph assembly, node factories, conditional edges,
`ToolNode`, `.bind_tools()`, `.with_structured_output()`, closure pattern

This is where the Reconciliation agent comes alive. The previous files defined
the pieces — tools, state, prompts — and this file wires them into an executable
graph.

#### The Four Nodes

| Node | What It Does | LLM Call? |
|------|-------------|-----------|
| **entry** | Renders `RECON_PROMPT_TEMPLATE` → seeds `[SystemMessage, HumanMessage]` | No |
| **llm** | Sends full message history to `model.bind_tools(RECON_TOOLS)` | Yes |
| **tools** | `ToolNode(RECON_TOOLS)` — auto-executes tool calls from AIMessage | No |
| **report** | `model.with_structured_output(ReconReport)` — produces diagnosis | Yes |

#### The ReAct Loop as a Graph

```
entry ──► llm ──► should_continue? ──► tools ──► (back to llm)
                       │
                       ▼ (no tools / max iterations)
                    report ──► END
```

The conditional edge `_should_continue` is the loop's control flow:
1. **Safety valve** — if `iteration >= max_iterations`, force `"report"` (prevents
   runaway loops)
2. **Tool calls present** — if the AIMessage has `tool_calls`, route to `"tools"`
3. **No tool calls** — route to `"report"` (investigation complete)

#### Two Model Configurations from One LLM

A critical detail: `bind_tools()` and `with_structured_output()` are different
invocation modes. You can't use both on the same call:

```python
llm = create_llm(config)                   # base model
model_with_tools = llm.bind_tools(RECON_TOOLS)  # for investigation (llm node)
report_model = llm.with_structured_output(ReconReport)  # for diagnosis (report node)
```

#### The Closure Pattern

Nodes are functions that take `(state)` — there's no way to pass the LLM as an
argument. The solution: factory functions that capture the model in a closure:

```python
def _make_llm_node(model_with_tools):      # factory takes the dependency
    def llm_node(state: dict) -> dict:     # LangGraph calls this
        response = model_with_tools.invoke(state["messages"])  # captured
        return {"messages": [response], "iteration": state["iteration"] + 1}
    return llm_node
```

Why not globals? Untestable, can't run two graphs with different models.
Why not a class? Works, but adds ceremony for functions that just need one
captured dependency.

#### ToolNode — Zero Boilerplate Tool Execution

LangGraph's prebuilt `ToolNode` reads `tool_calls` from the last AIMessage,
matches each tool name against the provided list, executes the function, and
returns `ToolMessage`s. You never write `if tool_name == "..."` dispatch logic.

#### The Report Instruction Trick

The report node appends a final `HumanMessage` telling the LLM to produce its
diagnosis — but to a *copy* of the messages, not to graph state. The
investigation history stays clean; only the report model sees the extra instruction.

### `tests/agents/test_recon_agent.py` — Unit Tests

**Concepts taught:** Mocking LLM agents, `unittest.mock.patch`, pytest-asyncio,
test isolation, testing non-deterministic systems

Testing LLM-powered agents is fundamentally different from testing normal
functions. The LLM is non-deterministic — even with `temperature=0`, responses
can vary. You can't write `assert result == exact_string`. Instead, you test
the **structure** (does data flow correctly?) and **plumbing** (does the adapter
translate correctly?), not the LLM's reasoning.

#### What Gets Tested

| Test Area | What It Verifies |
|-----------|-----------------|
| Agent properties | `name`, `description`, lazy `_graph = None` |
| `make_initial_state()` | Required fields, defaults, custom max_iterations |
| `_extract_tool_calls()` | Empty list, no tool calls, single/multiple, non-AI messages |
| Success path | AgentResult status, report dict conversion, actions_taken audit trail |
| Failure paths | Graph exception → failure result, None report → failure, missing gate → "unknown" |
| Lazy init | Graph built once on first invoke, cached across subsequent invokes |
| Config integration | `max_iterations` flows from config to initial state |

#### The Mock Injection Pattern

```python
mock_graph = MagicMock()
mock_graph.invoke.return_value = { ... predictable final state ... }
agent._graph = mock_graph   # bypass build_graph() entirely
result = await agent.invoke(context)
assert result.status == "success"
```

By setting `agent._graph` directly, we replace the entire LangGraph execution
with a predictable response. The test verifies that `invoke()` correctly
translates `TriggerContext` → initial state, extracts the report, converts via
`.model_dump()`, and packages into `AgentResult`.

#### `patch.object` for Lazy Init Testing

```python
with patch.object(agent, "build_graph", return_value=mock_graph) as mock_build:
    await agent.invoke(context)
    await agent.invoke(context)
    mock_build.assert_called_once()   # NOT twice — cached after first call
```

Here we can't inject `_graph` directly because we're testing that `invoke()`
calls `build_graph()` exactly once. `patch.object` replaces the method
temporarily and tracks call count.

#### Why Helper Functions Instead of `@pytest.fixture`?

Fixtures are great for setup identical across all tests. But these tests need
*slightly different* configs and contexts. Helper functions with parameters are
more flexible — each test calls `_make_config({...})` with its own overrides.

### `experiments/recon_live_test.py` — Live Integration Test

**Concepts taught:** Integration testing vs unit testing, end-to-end agent
validation, observable debugging, CLI argument parsing

This is the counterpart to unit tests — it runs the full agent with a real LLM
(Gemini free tier) to verify that the LLM actually understands the prompts,
calls the right tools, and produces valid structured output.

#### Unit Test vs Live Test

| Dimension | Unit Test | Live Test |
|-----------|-----------|-----------|
| LLM | Mocked | Real (Gemini) |
| Speed | < 1 second | 10-30 seconds |
| Determinism | Yes | No |
| When to run | Every commit | Manual / staging |
| What it catches | Structural regressions | Prompt engineering issues |

Both are necessary. Unit tests catch plumbing breakage instantly. Live tests
catch reasoning failures that only surface with a real LLM.

#### The Two Scenarios

- **Gate 3 (2026-09-28):** Bronze→Silver row count mismatch from 111 duplicate
  booking_ids. Expected tools: `query_gate_results` → `compare_row_counts` →
  `check_duplicate_keys`.

- **Gate 4 (2026-09-27):** Gold→Snowflake count mismatch from 15 NULL card_sk
  rows. Expected tools: `query_gate_results` → `compare_row_counts` →
  `check_fk_integrity`.

Each exercises a different investigation path, validating the system prompt's
investigation strategy actually guides the LLM correctly.

---

## Phase 3: DLQ Triage & Auto-Remediation Agent

Phase 3 builds the second Argus agent. If Phase 2 taught you how to build a
ReAct agent from scratch, Phase 3 teaches you how to **reuse that pattern**
while adding two new prompt engineering techniques: **classification with
confidence scoring** and **guarded side effects**.

The big insight: the graph topology (ReAct loop) is identical to Phase 2.
What changes is the *content* — different tools, different prompts, different
state shape. This validates that the ReAct pattern is genuinely reusable.

### What's New in Phase 3

| Concept | Where It Appears | Why It Matters |
|---------|-----------------|----------------|
| Classification prompting | `prompts.py` | The LLM must categorize, not just investigate |
| Confidence calibration | `prompts.py` | Without it, LLMs default to 0.9 or 0.5 for everything |
| Guarded side effects | `dlq_tools.py` | First agent that CHANGES pipeline state |
| Idempotency guards | `dlq_tools.py` | Prevents double-requeue on retry |
| Safety limits | `dlq_tools.py` | Caps side effects per invocation |
| Incremental accumulators | `state.py` | State tracks classifications and requeue audit |
| Dual DLQ lanes | `dlq_tools.py`, `prompts.py` | Agent handles two different failure surfaces |

### Read Order

Read these files in this order — each builds on concepts from the previous:

1. `tools/pipeline/dlq_tools.py` — the tools the agent uses
2. `agents/dlq_triage/state.py` — the state it maintains
3. `agents/dlq_triage/prompts.py` — the instructions it follows
4. `agents/dlq_triage/graph.py` — the wiring that connects everything
5. `agents/dlq_triage/agent.py` — the adapter to the platform
6. `tests/agents/test_dlq_agent.py` — how to test it
7. `experiments/dlq_live_test.py` — how to run it for real

### `argus/tools/pipeline/dlq_tools.py` — DLQ Investigation Tools

**Concepts taught:** Side-effect tools, idempotency guards, safety limits,
dual data sources, audit trails

This file introduces the biggest difference from Phase 2: a tool that
**changes state** instead of just reading it. The Recon agent's tools were
all read-only (query this, compare that). The DLQ agent's `requeue_message`
tool puts messages back into the pipeline.

#### Side Effects Need Safety Layers

Side-effect tools need defensive programming that read-only tools don't:

```
Layer 1: PROMPT ENGINEERING — system prompt says "NEVER requeue SCHEMA_MISMATCH"
Layer 2: IDEMPOTENCY GUARD — _REQUEUED_RECORDS set prevents double-requeue
Layer 3: SAFETY LIMIT — _MAX_REQUEUE_PER_INVOCATION = 10 caps total requeues
Layer 4: AUDIT TRAIL — every requeue returns a logged confirmation string
```

Each layer catches failures the layer above might miss. The prompt might not
prevent the LLM from trying; the idempotency guard catches retries; the
safety limit prevents runaway requeuing; the audit trail makes everything
visible.

#### Dual DLQ Lanes

The TTAG pipeline has two DLQ mechanisms:

- **Kafka DLQ** (Benefit lane) — consumer processing failures land in a DLQ
  topic. Error info is in Kafka headers.
- **bad_files Iceberg quarantine** (Booking lane) — file ingestion failures
  move files to a quarantine table. Error info is in the record metadata.

The `read_dlq_records` tool reads from either lane based on the `source_lane`
parameter. This teaches an important design pattern: different infrastructure
surfaces can present the same abstraction to the agent.

#### Tool Design for Classification

The `read_dlq_records` tool returns rich error metadata (error_class,
error_message, payload_summary) specifically so the LLM can classify records.
The `query_schema_changelog` tool exists purely as a cross-reference source
for SCHEMA_MISMATCH classification — without it, the LLM would classify
based on error messages alone, which is less reliable.

### `argus/agents/dlq_triage/state.py` — DLQ State Schema

**Concepts taught:** Incremental accumulators, side-effect tracking in state,
task-specific state vs generic mechanism

#### Two New Accumulator Fields

DLQTriageState has everything ReconState had, plus two new fields that
illustrate how different tasks need different state shapes:

```python
# Builds up as the agent classifies each record
classifications: Annotated[list[DLQRecord], operator.add]

# Tracks every requeue action for the final report
requeue_audit: Annotated[list[str], operator.add]
```

The `classifications` accumulator exists because the DLQ agent's work is
*incremental* — it classifies records one at a time across multiple ReAct
iterations. The Recon agent's work was *holistic* — it investigated globally
and produced one report at the end.

The `requeue_audit` accumulator exists because the DLQ agent has *side
effects* that need tracking. The Recon agent was read-only, so it didn't
need to track what it changed.

#### The Same Mechanism, Different Shape

Both ReconState and DLQTriageState use `Annotated[list, operator.add]` for
accumulation and plain fields for scalars. The LangGraph mechanism is
generic; the state shape encodes your agent's specific workflow.

### `argus/agents/dlq_triage/prompts.py` — Classification Rubric

**Concepts taught:** Classification prompting, confidence calibration,
few-shot examples in system prompts, side-effect guardrails

This is the most instructive file in Phase 3. The Recon agent's prompt said
"investigate and report." The DLQ agent's prompt says "classify each record
with a confidence score AND take action based on that classification." This
requires three new prompt engineering techniques.

#### 1. Classification Rubric

A structured rubric maps evidence patterns to categories:

```
TRANSIENT → TimeoutException, ConnectionReset, BrokerNotAvailable
SCHEMA_MISMATCH → SchemaRegistryException + changelog confirms
DATA_QUALITY → NullPointerException on required fields
UNKNOWN → anything else, or confidence < 0.60
```

Without a rubric, the LLM improvises categories or uses inconsistent criteria
across records. With it, every classification has a clear evidence chain.

#### 2. Confidence Calibration

LLMs have no innate sense of confidence scales. Left uncalibrated, they
either report 0.90+ for everything (over-confident) or cluster around 0.60
(under-confident). The prompt fixes this with explicit calibration:

```
0.85-0.95: clear infrastructure error, no data/schema involvement
0.70-0.85: likely transient but with some ambiguity
Below 0.70: don't classify as transient
```

These ranges anchor the LLM's confidence output to specific evidence levels.
Combined with the "only requeue if confidence ≥ 0.80" rule, this creates
a reliable decision boundary for side effects.

#### 3. Few-Shot Examples in the Rubric

```
Example: TimeoutException from broker → TRANSIENT at 0.90
Example: SchemaRegistryException + changelog shows pending_consumer_update
         → SCHEMA_MISMATCH at 0.95
```

These examples serve double duty: they teach the classification *and* anchor
the confidence scale. The LLM sees "TimeoutException = 0.90" and calibrates
similar errors accordingly.

#### Side-Effect Guardrails

The prompt has explicit ✅/❌ rules for requeue safety:

```
✅ ONLY requeue records classified as TRANSIENT with confidence ≥ 0.80
❌ NEVER requeue SCHEMA_MISMATCH — they'll fail again the same way
❌ NEVER requeue DATA_QUALITY — bad data stays bad
❌ NEVER requeue UNKNOWN — don't retry what you don't understand
```

These rules are the first line of defense for side effects. The tool has its
own safety layers (idempotency, rate limit), but preventing the LLM from
*trying* is cheaper than catching bad attempts.

### `argus/agents/dlq_triage/graph.py` — LangGraph StateGraph

**Concepts taught:** Pattern reuse, topology vs content, swappable components

The most important thing about this file is how *similar* it is to the Recon
graph. Same topology:

```
entry ──► llm ──► should_continue? ──► tools ──► (back to llm)
                       │
                       ▼
                    report ──► END
```

Same node types, same conditional routing, same closure pattern. Only four
things change:

1. **DLQ_TOOLS** instead of RECON_TOOLS
2. **DLQTriageState** instead of ReconState
3. **DLQ_PROMPT_TEMPLATE** instead of RECON_PROMPT_TEMPLATE
4. **DLQTriageReport** instead of ReconReport

This validates the ReAct topology as a reusable pattern. In Phase 6, we may
extract a generic `build_react_graph()` that takes tools, state class, prompt
template, and report model as parameters. Two concrete implementations teach
the pattern intuitively before abstracting.

#### The Report Node's Different Instruction

The one meaningful difference is the report node's instruction message. The
Recon report asks for root cause analysis. The DLQ report asks for per-record
classification summaries, requeue counts, and severity based on the
classification mix. The instruction shapes what the LLM produces, even with
the same `.with_structured_output()` mechanism.

### `argus/agents/dlq_triage/agent.py` — DLQ Agent Adapter

**Concepts taught:** Adapter pattern consistency, source_lane vs gate_name,
config path namespacing

Same five-step pattern as ReconciliationAgent:

1. Lazy graph build (compile once, cache)
2. Translate TriggerContext → DLQTriageState initial state
3. `graph.invoke(initial_state)` → final state
4. Extract DLQTriageReport from final state
5. Package into AgentResult

The differences are in step 2 (source_lane instead of gate_name, different
config path for max_iterations) and step 4 (different report fields for
logging). If the agents' `invoke()` methods were any more similar, you'd
extract a shared implementation. Right now the duplication is small enough
that the clarity of seeing the full flow in each agent outweighs the DRY
benefit.

### `tests/agents/test_dlq_agent.py` — DLQ Unit Tests

**Concepts taught:** Test pattern reuse, testing side-effect agents,
DLQ-specific fixtures

Same 7 test classes as the Recon tests, adapted for DLQ:

| Test Class | What It Verifies |
|-----------|-----------------|
| TestAgentProperties | name="dlq_triage", description mentions classification, lazy init |
| TestMakeInitialState | source_lane, classifications=[], requeue_audit=[], defaults |
| TestExtractToolCalls | Same logic — only depends on AIMessage format |
| TestAgentInvokeSuccess | DLQTriageReport with per-record classifications, requeue counts |
| TestAgentInvokeFailure | Graph exception, None report, missing source_lane defaults to "both" |
| TestLazyGraphBuilding | Same pattern — build once, cache |
| TestConfigIntegration | agents.dlq_triage.max_iterations, fallback to agents.max_iterations |

The test structure being nearly identical validates the BaseAgent contract.
When two agents follow the same interface, their tests follow the same
pattern — a sign the abstraction is working.

### `experiments/dlq_live_test.py` — DLQ Live Integration Test

**Concepts taught:** Live testing classification accuracy, requeue safety
validation, side-effect observability

Same structure as `recon_live_test.py` with two additions:

#### Two DLQ Scenarios

- **Kafka DLQ (2026-09-30):** 6 Benefit lane records spanning all 4
  classification categories. Tests classification accuracy and requeue
  safety for transient records.

- **Bad Files (2026-09-29):** 3 Booking lane records spanning 3 categories.
  Tests the agent handles Iceberg quarantine records differently from Kafka
  DLQ records.

#### Requeue Safety Validation

The live test includes a post-hoc `_validate_requeue_safety()` function
that checks every requeued record:

- Was it classified as TRANSIENT? (If not → safety violation)
- Was its confidence ≥ 0.80? (If not → low-confidence requeue warning)

This validates the prompt engineering end-to-end: the classification rubric
should prevent the LLM from even *trying* to requeue non-transient records,
and the confidence calibration should produce scores above 0.80 for genuinely
transient errors. A safety violation means the rubric or calibration guidance
needs tightening.

---

## Phase 4: Incident & Backfill Planning Agent

> **Goal:** Build the third Argus agent, introducing two major new patterns:
> **Plan-then-Execute** (the agent produces a plan before taking action) and
> **Human-in-the-Loop** (the graph pauses for human approval before executing
> the plan). Phase 4 also introduces the **rejection loop** — the human can
> reject a plan with feedback, and the agent reworks it.

If Phase 2 taught the ReAct loop and Phase 3 taught classification with
guarded side effects, Phase 4 teaches you how to build an agent that **stops
and waits for a human** before doing anything dangerous.

### What's New in Phase 4

| Concept | Where It Appears | Why It Matters |
|---------|-----------------|----------------|
| Two-registry tool separation | `backfill_tools.py` | Prevents LLM from executing during investigation |
| Plan-then-Execute pattern | `agents/backfill/graph.py` | Agent plans first, executes only after approval |
| Human-in-the-Loop (HITL) | `agents/backfill/graph.py` | LangGraph `interrupt()` pauses for human decision |
| Rejection loop | `agents/backfill/graph.py` | Human rejects → agent reworks → re-presents |
| LangGraph checkpointing | `agents/backfill/graph.py` | State persists across interrupt/resume |
| Pipeline locking | `backfill_tools.py` | Exclusive locks prevent concurrent writes |
| Lock → Execute → Release | `backfill_tools.py` | Enforced ordering for safe backfill execution |
| Per-step execution audit | `backfill_tools.py` | Every step logged for the final report |
| Mocking interrupt detection | `test_backfill_agent.py` | Mock `graph.get_state().next` to test paused vs completed |
| Two-phase test pattern | `test_backfill_agent.py` | Set up mock graph twice (invoke + resume) in a single test |
| Rejection loop testing | `test_backfill_agent.py` | Full cycle: invoke → reject → reject → approve → success |
| Guard rail testing | `test_backfill_agent.py` | ValueError when `resume()` called without prior `invoke()` |
| Live two-phase lifecycle | `backfill_live_test.py` | Real LLM runs invoke → approve → success end-to-end |
| Plan quality validation | `backfill_live_test.py` | Validate plan structure beyond just status codes |
| Live rejection feedback | `backfill_live_test.py` | Test whether real LLM incorporates rejection feedback |

### Read Order

Read these files in this order — each builds on concepts from the previous:

```
argus/tools/pipeline/backfill_tools.py ─── Tool layer (two registries)
    │
    │   "What can the agent DO? And when is each tool available?"
    ▼
argus/agents/backfill/state.py ─── State schema (plan + approval fields)
    │
    │   "What data does the graph carry across the interrupt?"
    ▼
argus/agents/backfill/prompts.py ─── System prompt (investigation + planning rubric)
    │
    │   "How does the agent reason about incidents and build plans?"
    ▼
argus/agents/backfill/graph.py ─── Graph topology (Plan-then-Execute + HITL)
    │
    │   "How does the graph pause, resume, and handle rejection?"
    ▼
argus/agents/backfill/agent.py ─── Agent class (two-phase invoke)
    │
    │   "How does the platform invoke an agent that pauses?"
    ▼
tests/agents/test_backfill_agent.py ─── Tests (interrupt/resume/rejection)
experiments/backfill_live_test.py ─── Live test with real LLM
```

### `argus/tools/pipeline/backfill_tools.py` — Tool Layer

**Concepts taught:** Two-registry tool separation, pipeline locking,
Lock → Execute → Release pattern, per-step audit trails

This file introduces the biggest structural difference from Phases 2 and 3:
**two separate tool registries**. The Recon and DLQ agents had a single tool
list because their graphs had one phase (investigate → report). The Backfill
agent has two phases with different risk profiles, and the tool registries
enforce that boundary.

#### Two Registries, Two Phases

```python
BACKFILL_INVESTIGATION_TOOLS = [
    get_incident_context,      # What happened?
    assess_data_gaps,          # How bad is the damage?
    check_pipeline_locks,      # Is it safe to plan?
    get_backfill_history,      # How long will it take?
    validate_source_readiness  # Is the source data ready?
]

BACKFILL_EXECUTION_TOOLS = [
    acquire_pipeline_lock,     # Lock before executing
    execute_backfill_step,     # Execute one step
    release_pipeline_lock      # Release and get audit trail
]
```

The graph will bind `BACKFILL_INVESTIGATION_TOOLS` during the investigation
phase and `BACKFILL_EXECUTION_TOOLS` during the execution phase. The LLM
literally cannot call `execute_backfill_step` during investigation — it's
not in the bound tool set.

Compare with Phase 3's approach: `DLQ_TOOLS` was a single list with all three
tools. Safety relied on prompt rules ("only requeue TRANSIENT") plus tool-level
guards (idempotency, rate limit). Phase 4 adds a **structural** safety layer:
the tools aren't even available until the human approves the plan.

#### Lock → Execute → Release

The three execution tools enforce a specific ordering:

1. **`acquire_pipeline_lock`** — gets an exclusive lock (idempotent, timeout-based)
2. **`execute_backfill_step`** — runs one step (refuses without a lock)
3. **`release_pipeline_lock`** — releases and returns the full audit trail

This pattern prevents two critical failures:
- **Concurrent writes** — if two backfills run on the same entity simultaneously,
  they could corrupt data. The exclusive lock prevents this.
- **Orphaned locks** — if the agent crashes mid-execution, the 60-minute timeout
  ensures the lock is eventually released.

#### Per-Step Audit Trail

Every `execute_backfill_step` call is logged in `_EXECUTION_AUDIT` with:
- Timestamp
- Entity, layer, partition
- Source snapshot ID
- Step description
- Result (simulated success in dev)

When `release_pipeline_lock` is called, it returns the complete audit trail.
This goes into the agent's final report so humans can verify exactly what was
executed, in what order, and whether it succeeded.

#### Safety Limits

Like Phase 3's `_MAX_REQUEUE_PER_INVOCATION = 10`, Phase 4 has
`_MAX_STEPS_PER_INVOCATION = 20`. Even with a valid lock and an approved plan,
the agent can't execute more than 20 steps in one run. This prevents runaway
execution if the LLM hallucinates extra steps.

#### Two Simulated Scenarios

- **2026-09-28 (Gate 3 failure):** Silver row count mismatch from duplicate
  re-delivery → backfill Silver + Gold (multi-layer, ordered)
- **2026-09-27 (Gate 4 failure):** NULL FKs from failed DIM_CARD refresh →
  backfill Gold only (single-layer, with dependency gate)

These exercise different backfill strategies: Scenario 1 needs multi-layer
ordered execution, Scenario 2 needs single-layer execution but has an external
dependency (DIM_CARD must be refreshed before backfill can proceed).


### `argus/agents/backfill/state.py` — State Schema

**Concepts taught:** Plan as intermediate output, HITL approval state fields,
two-level iteration control, execution audit accumulator

This is the third state schema in Argus. Comparing it with ReconState (Phase 2)
and DLQTriageState (Phase 3) shows how the HITL pattern requires new state
fields that autonomous agents don't need.

#### Plan as Intermediate Output

In Recon and DLQ, the report is the LAST thing produced — the graph ends by
extracting a report from the conversation. The Backfill agent produces a
`BackfillPlan` as an INTERMEDIATE step. It sits in state between the
investigation phase and the execution phase, where the human reviews it.

```python
plan: BackfillPlan | None  # None until planning node produces it
```

This uses overwrite semantics (no reducer) because we always want the latest
plan. When the human rejects and the LLM produces a revised plan, the old plan
is replaced — we don't accumulate a history of rejected plans in state.

#### HITL Approval Fields

Three fields drive the approval gate:

```python
approval_status: str     # "" → "pending" → "approved" / "rejected"
revision_feedback: str   # human's rejection reason (empty if not rejected)
plan_iterations: int     # how many plans have been produced (safety cap)
max_plan_iterations: int # default 3 — prevents infinite reject-rework loops
```

`approval_status` is the routing signal. After the interrupt node resumes:
- `"approved"` → conditional edge routes to execution phase
- `"rejected"` → conditional edge routes back to investigation with feedback

`revision_feedback` is what makes the rejection loop useful. Without it, the
LLM would just regenerate the same plan. With feedback injected as a
HumanMessage, the LLM knows WHAT to change.

#### Two-Level Iteration Control

This is the key insight in BackfillState. The agent has TWO independent loops,
each needing its own safety cap:

```
┌──── Outer loop: plan revisions (plan_iterations / max_plan_iterations = 3) ────┐
│                                                                                 │
│  ┌──── Inner loop: ReAct tool calls (iteration / max_iterations = 15) ────┐    │
│  │  LLM calls tools → gets results → calls more tools → ...               │    │
│  │  Capped at 15 iterations per investigation round                        │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  Planning node → plan produced → interrupt → human reviews                      │
│  If rejected: iteration resets, plan_iterations increments, loop back            │
│  If approved: proceed to execution                                              │
│  If plan_iterations >= 3: stop with error                                       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

The inner loop resets each time the outer loop cycles — the LLM gets fresh
investigation iterations for each rework attempt. This prevents a scenario
where the LLM exhausts its tool-call budget during the first investigation
and can't call any tools when reworking the rejected plan.

#### Execution Audit

After approval, every execution action is logged:

```python
execution_audit: Annotated[list[str], operator.add]  # accumulates across tool calls
```

Same accumulator pattern as DLQ's `requeue_audit`, but tracking the full
Lock → Execute → Release sequence rather than individual requeue actions.

#### Comparing State Schemas

| Field Category | ReconState | DLQTriageState | BackfillState |
|---|---|---|---|
| Messages | ✅ `operator.add` | ✅ same | ✅ same |
| Trigger context | gate_name | source_lane | trigger_params dict |
| Loop control | iteration/max | iteration/max | iteration/max + plan_iterations/max |
| Intermediate output | — | classifications | plan (BackfillPlan) |
| Side-effect tracking | — | requeue_audit | execution_audit |
| Final output | report: ReconReport | report: DLQTriageReport | plan IS the output |
| HITL fields | — | — | approval_status, revision_feedback |
| Errors | ✅ `operator.add` | ✅ same | ✅ same |


### `argus/agents/backfill/prompts.py` — System Prompts

**Concepts taught:** Multi-phase prompting, structured output rubric,
revision prompt with feedback injection

This is the most complex prompt file in Argus. While the Recon and DLQ agents
each had ONE system prompt, the Backfill agent has THREE — one per graph phase.
Plus a revision template for the rejection loop.

#### Why Multiple System Prompts?

The Backfill agent's graph has three distinct phases, each requiring different
LLM behavior:

```
Phase 1: INVESTIGATION → "Use these 5 tools to understand the incident"
         System prompt: BACKFILL_INVESTIGATION_PROMPT
         Tools bound: BACKFILL_INVESTIGATION_TOOLS

Phase 2: PLANNING → "Produce a BackfillPlan with these exact fields"
         System prompt: BACKFILL_PLANNING_PROMPT
         Output: BackfillPlan (via .with_structured_output())

Phase 3: EXECUTION → "Follow Lock → Execute → Release strictly"
         System prompt: BACKFILL_EXECUTION_PROMPT
         Tools bound: BACKFILL_EXECUTION_TOOLS
```

Each prompt is focused on its phase. Putting all three in a single prompt would
mean the LLM sees execution instructions during investigation (risky — it might
try to execute early) and wastes tokens on irrelevant context.

#### Investigation Prompt — Tool Strategy

The investigation prompt follows the same pattern as Phase 2's Recon prompt:
role definition → pipeline architecture → investigation strategy → rules.
The strategy tells the LLM which tools to call and in what order:

1. `get_incident_context` → understand WHAT happened
2. `assess_data_gaps` → quantify the DAMAGE
3. `check_pipeline_locks` → confirm it's SAFE to plan
4. `get_backfill_history` → estimate DURATION
5. `validate_source_readiness` → verify SOURCE exists

The key rule: "call at least 3 tools before concluding." This prevents the
LLM from producing a plan after only calling `get_incident_context`.

#### Planning Prompt — Structured Output Rubric

This is the Phase 4 equivalent of the DLQ prompt's classification rubric.
Instead of calibrating confidence scores, it calibrates PLAN QUALITY:

For each BackfillPlan field, the rubric shows:
- **Good example**: "Gate 3 failed on 2026-09-28: Silver booking_detail is
  111 rows short due to upstream re-delivery"
- **Bad example**: "There was a pipeline failure" (too vague)

Critical rules for plan quality:
- Steps must be ORDERED (upstream before downstream — Silver before Gold)
- Snapshot IDs must come from `validate_source_readiness` results (not hallucinated)
- Duration estimates must use `get_backfill_history` data as baseline
- Risk assessment must cite evidence, not generalize

These rules are the planning equivalent of the DLQ prompt's "NEVER requeue
SCHEMA_MISMATCH" — they prevent common LLM failure modes when producing plans.

#### Execution Prompt — Lock → Execute → Release Safety

The execution prompt is short and strict — it's all rules, no reasoning
guidance. The LLM's job during execution is mechanical: follow the approved
plan, execute steps in order, handle failures safely. The three rules:

1. **LOCK FIRST** — acquire before any step, stop if blocked
2. **EXECUTE IN ORDER** — stop on failure, don't skip ahead
3. **RELEASE LAST** — always release, even on failure

#### Revision Prompt — Feedback Injection

When the human rejects a plan, this template injects their feedback:

```
Your backfill plan was REJECTED by the human reviewer.
This is attempt {plan_iterations} of {max_plan_iterations}.

Reviewer feedback:
{revision_feedback}

Revise your plan to address the feedback above...
```

Three design choices in this template:
1. **"REJECTED" framing** — sets the right context (not ambiguous)
2. **Iteration count** — "attempt 2 of 3" creates urgency (don't waste it)
3. **"Focus on what changed"** — prevents the LLM from starting over from
   scratch and re-calling all investigation tools (wasting iterations)

#### Comparing Prompt Structures

| Aspect | Recon | DLQ | Backfill |
|---|---|---|---|
| System prompts | 1 | 1 | 3 (investigation, planning, execution) |
| Human templates | 1 | 1 | 2 (initial + revision) |
| ChatPromptTemplates | 1 | 1 | 2 (initial + revision) |
| Rubric type | investigation strategy | classification + confidence | structured output quality |
| Side-effect rules | — | requeue safety | Lock → Execute → Release |
| Feedback handling | — | — | revision prompt with iteration count |


### `argus/agents/backfill/graph.py` — The Multi-Phase HITL Graph

**Concepts taught:** `interrupt()` / `Command(resume=...)`, LangGraph checkpointing
(`MemorySaver`), `thread_id`, multiple `ToolNode`s, multiple LLM configurations,
multi-phase graph topology, approval gate with rejection loop

This is the most complex graph in Argus — 8 nodes, 2 ToolNodes, 3 LLM
configurations, and the first use of `interrupt()` for human-in-the-loop. If the
Recon graph (Phase 2) taught you the ReAct loop and the DLQ graph (Phase 3)
proved it's reusable, the Backfill graph teaches you how to COMPOSE multiple
patterns into a single graph.

#### From 4 Nodes to 8: Why the Jump?

The Recon and DLQ graphs each had 4 nodes in one ReAct loop:

```
entry → llm ↔ tools → report → END
```

The Backfill graph needs 8 nodes because it has a fundamentally different
structure — two ReAct loops with an approval gate between them:

```
entry → investigate_llm ↔ investigation_tools → plan_node
        → approval_gate ↔ revise_node → plan_node
        → execute_llm ↔ execution_tools → END
```

Each section serves a different purpose:
- **Investigation loop** (entry, investigate_llm, investigation_tools): ReAct
  with read-only tools, same pattern as Recon/DLQ
- **Planning node** (plan_node): structured output via `.with_structured_output(BackfillPlan)`
- **Approval gate** (approval_gate, revise_node): HITL interrupt/resume + rejection loop
- **Execution loop** (execute_llm, execution_tools): ReAct with side-effect tools

#### `interrupt()` — Pausing the Graph

The approval gate node calls `interrupt(plan.model_dump())` which does three things:

1. **Serializes the plan** and returns it to the caller as the interrupt value
2. **Saves the full graph state** to the checkpointer (MemorySaver)
3. **Pauses execution** — the graph stops, the `invoke()` call returns

```python
def approval_gate_node(state: dict) -> dict:
    plan = state["plan"]
    # This line PAUSES the graph and returns the plan to the caller
    resume_value = interrupt(plan.model_dump())
    # Everything below here runs ONLY when the graph is RESUMED
    decision = resume_value.get("decision", "")
    feedback = resume_value.get("feedback", "")
    ...
```

The key insight: code AFTER `interrupt()` doesn't run immediately. It runs
only when the caller resumes the graph with `Command(resume=...)`. The
variable `resume_value` receives whatever the caller passes in the resume.

#### `Command(resume=...)` — Resuming with a Decision

The caller (your application code) resumes the interrupted graph by invoking
it again with a `Command`:

```python
# To approve:
graph.invoke(Command(resume={"decision": "approved"}),
             config={"configurable": {"thread_id": "same-thread-id"}})

# To reject with feedback:
graph.invoke(Command(resume={"decision": "rejected",
                              "feedback": "Add rollback steps for Gold layer"}),
             config={"configurable": {"thread_id": "same-thread-id"}})
```

Two critical requirements:
1. **Same `thread_id`** — the checkpointer uses this to find the saved state
2. **The resume value structure** must match what the node expects

#### Checkpointing — Why `MemorySaver` Is Required

Without a checkpointer, `interrupt()` cannot save state. The graph would
pause but the state would be lost — there would be nothing to resume from.

```python
# In build_backfill_graph():
compiled = graph.compile(checkpointer=MemorySaver())
```

`MemorySaver` stores checkpoints in a Python dict (in-memory). This means:
- ✅ Works for development and testing
- ❌ State is lost when the process restarts
- For production: `SqliteSaver` or `PostgresSaver` persist across restarts

The `thread_id` is the key that links multiple `invoke()` calls to the same
graph execution:

```python
config = {"configurable": {"thread_id": "backfill-2026-09-28-abc123"}}
# First invoke: runs investigation → planning → interrupt
result = graph.invoke(initial_state, config=config)
# Second invoke: resumes from interrupt → execution → END
result = graph.invoke(Command(resume={"decision": "approved"}), config=config)
```

#### The Rejection Loop

When the human rejects, the approval gate sets `approval_status = "rejected"`
and stores the feedback in `revision_feedback`. The conditional edge routes
to `revise_node`:

```python
def _route_after_approval(state: dict) -> str:
    status = state.get("approval_status", "")
    if status == "approved":
        return "execute_llm"      # → execution phase
    elif status == "rejected":
        return "revise_node"      # → inject feedback, re-plan
    return END                     # error case
```

The revise node injects the human's feedback as a HumanMessage and resets the
iteration counter (giving the LLM fresh tool-call budget):

```python
def revise_node(state: dict) -> dict:
    revision_messages = BACKFILL_REVISION_TEMPLATE.invoke({
        "plan_iterations": state["plan_iterations"],
        "max_plan_iterations": state["max_plan_iterations"],
        "revision_feedback": state["revision_feedback"],
    })
    return {
        "messages": revision_messages.to_messages(),
        "iteration": 0,  # RESET — fresh budget for revision
    }
```

Then it routes to `plan_node`, which produces a NEW `BackfillPlan` considering
the feedback. The plan goes through the approval gate again — this is the loop:

```
plan_node → approval_gate → (rejected) → revise_node → plan_node → approval_gate → ...
```

The loop has a safety cap: `max_plan_iterations` (default 3). If the human
rejects 3 times, the approval gate routes to END instead of revise_node.

#### Two ToolNodes, Three LLM Configurations

The Recon/DLQ graphs each had one ToolNode and one `.bind_tools()` call. The
Backfill graph has two of each, plus a `.with_structured_output()`:

```python
# In build_backfill_graph():

# Config 1: Investigation LLM — 5 read-only tools
model_with_investigation_tools = llm.bind_tools(BACKFILL_INVESTIGATION_TOOLS)

# Config 2: Planning LLM — structured output, no tools
#   (created inside plan_node factory via llm.with_structured_output(BackfillPlan))

# Config 3: Execution LLM — 3 side-effect tools
model_with_execution_tools = llm.bind_tools(BACKFILL_EXECUTION_TOOLS)

# Two separate ToolNodes
investigation_tool_node = ToolNode(BACKFILL_INVESTIGATION_TOOLS)
execution_tool_node = ToolNode(BACKFILL_EXECUTION_TOOLS)
```

This is the graph-level enforcement of two-registry tool separation. The
investigation LLM literally cannot call `execute_backfill_step` because that
tool isn't in its bound set. The safety comes from the TOPOLOGY, not just
the prompt.

#### Execution Phase — The Second ReAct Loop

After approval, the execution LLM node runs a standard ReAct loop but with
execution tools (lock, execute, release). One key difference from the
investigation loop: the execution system prompt is injected only on the
FIRST call (detected by an empty `execution_audit`):

```python
if not state.get("execution_audit"):
    # First execution call — inject the execution system prompt
    exec_system = SystemMessage(content=BACKFILL_EXECUTION_PROMPT)
    messages_for_llm = [exec_system] + [m for m in state["messages"]
                                         if not isinstance(m, SystemMessage)]
else:
    messages_for_llm = state["messages"]
```

This avoids repeatedly injecting the system prompt on every ReAct iteration
during execution (which would waste tokens).

#### Edge Wiring — The Complete Graph

The `build_backfill_graph()` function wires all 8 nodes:

```python
# Phase 1: Investigation
graph.set_entry_point("entry")
graph.add_edge("entry", "investigate_llm")
graph.add_conditional_edges("investigate_llm", _should_continue_investigating,
    {"investigation_tools": "investigation_tools", "plan_node": "plan_node"})
graph.add_edge("investigation_tools", "investigate_llm")

# Phase 2: Planning → Approval
graph.add_edge("plan_node", "approval_gate")
graph.add_conditional_edges("approval_gate", _route_after_approval,
    {"execute_llm": "execute_llm", "revise_node": "revise_node", END: END})
graph.add_edge("revise_node", "plan_node")

# Phase 3: Execution
graph.add_conditional_edges("execute_llm", _should_continue_executing,
    {"execution_tools": "execution_tools", END: END})
graph.add_edge("execution_tools", "execute_llm")
```

Three conditional edges (Recon/DLQ had one each) reflect the three decision
points: "done investigating?", "approved/rejected?", "done executing?".

#### Comparing Graph Builders

| Aspect | Recon/DLQ | Backfill |
|---|---|---|
| Nodes | 4 | 8 |
| ToolNodes | 1 | 2 |
| LLM configs | 2 (tools + structured output) | 3 (investigation + planning + execution) |
| Conditional edges | 1 (`_should_continue`) | 3 (investigate, approve, execute) |
| Checkpointer | None | `MemorySaver()` |
| Compilation | `graph.compile()` | `graph.compile(checkpointer=MemorySaver())` |
| HITL | No | `interrupt()` + `Command(resume=...)` |
| New imports | — | `from langgraph.types import Command, interrupt` |
| | | `from langgraph.checkpoint.memory import MemorySaver` |


---

## Concept Index

A quick lookup: which file teaches which concept.

| Concept | File(s) | Phase |
|---------|---------|-------|
| ReAct pattern (Reason-Act-Observe) | `experiments/calculator_agent.py`, `agents/reconciliation/graph.py` | 0, 2 |
| `@tool` decorator & tool design | `tools/pipeline/recon_tools.py` | 2 |
| Docstrings as prompt engineering | `tools/pipeline/recon_tools.py` | 2 |
| LangGraph StateGraph | `agents/reconciliation/state.py`, `graph.py` | 2 |
| Annotated reducers (`operator.add`) | `agents/reconciliation/state.py` | 2 |
| System prompt design | `agents/reconciliation/prompts.py` | 2 |
| `ChatPromptTemplate` | `agents/reconciliation/prompts.py` | 2 |
| Pydantic structured output | `schemas/reports.py` | 1 |
| LLM client factory | `core/llm.py` | 1 |
| YAML config management | `core/config.py` | 1 |
| Structured JSON logging | `core/logging.py` | 1 |
| Correlation IDs via `contextvars` | `core/logging.py` | 1 |
| Abstract base class pattern | `agents/base.py` | 1 |
| Rules-based routing | `core/router.py` | 1 |
| Click CLI framework | `cli/main.py` | 1 |
| `.bind_tools()` | `agents/reconciliation/graph.py` | 2 |
| Conditional edges | `agents/reconciliation/graph.py` | 2 |
| `with_structured_output()` | `agents/reconciliation/graph.py` | 2 |
| `ToolNode` (prebuilt) | `agents/reconciliation/graph.py` | 2 |
| Closure pattern (DI for nodes) | `agents/reconciliation/graph.py` | 2 |
| Graph compilation | `agents/reconciliation/graph.py` | 2 |
| Iteration safety valve | `agents/reconciliation/state.py`, `graph.py` | 2 |
| Adapter pattern (BaseAgent) | `agents/reconciliation/agent.py` | 2 |
| Lazy initialization | `agents/reconciliation/agent.py` | 2 |
| Error boundary pattern | `agents/reconciliation/agent.py` | 2 |
| `.model_dump()` at boundaries | `agents/reconciliation/agent.py` | 2 |
| Mocking LLM agents | `tests/agents/test_recon_agent.py` | 2 |
| `unittest.mock.patch` / `patch.object` | `tests/agents/test_recon_agent.py` | 2 |
| pytest-asyncio | `tests/agents/test_recon_agent.py` | 2 |
| Test isolation (helpers vs fixtures) | `tests/agents/test_recon_agent.py` | 2 |
| Integration testing (live LLM) | `experiments/recon_live_test.py` | 2 |
| `argparse` CLI | `experiments/recon_live_test.py` | 2 |
| `asyncio.run()` entry point | `experiments/recon_live_test.py` | 2 |

| Classification prompting | `agents/dlq_triage/prompts.py` | 3 |
| Confidence calibration | `agents/dlq_triage/prompts.py` | 3 |
| Few-shot examples in system prompt | `agents/dlq_triage/prompts.py` | 3 |
| Side-effect guardrails | `agents/dlq_triage/prompts.py`, `tools/pipeline/dlq_tools.py` | 3 |
| Idempotency guard | `tools/pipeline/dlq_tools.py` | 3 |
| Safety limit (rate limiting side effects) | `tools/pipeline/dlq_tools.py` | 3 |
| Incremental state accumulators | `agents/dlq_triage/state.py` | 3 |
| Dual data sources (Kafka DLQ + bad_files) | `tools/pipeline/dlq_tools.py` | 3 |
| ReAct topology reuse | `agents/dlq_triage/graph.py` | 3 |
| Requeue safety validation | `experiments/dlq_live_test.py` | 3 |
| Two-registry tool separation | `tools/pipeline/backfill_tools.py` | 4 |
| Pipeline locking (exclusive locks) | `tools/pipeline/backfill_tools.py` | 4 |
| Lock → Execute → Release pattern | `tools/pipeline/backfill_tools.py` | 4 |
| Per-step execution audit trail | `tools/pipeline/backfill_tools.py` | 4 |
| Step limit safety (`_MAX_STEPS_PER_INVOCATION`) | `tools/pipeline/backfill_tools.py` | 4 |
| Idempotent lock acquisition | `tools/pipeline/backfill_tools.py` | 4 |
| Timeout-based auto-release | `tools/pipeline/backfill_tools.py` | 4 |
| Two-level iteration control | `agents/backfill/state.py` | 4 |
| Plan as intermediate output | `agents/backfill/state.py` | 4 |
| HITL approval state fields | `agents/backfill/state.py` | 4 |
| Execution audit accumulator | `agents/backfill/state.py` | 4 |
| Multi-phase prompting | `agents/backfill/prompts.py` | 4 |
| Structured output rubric | `agents/backfill/prompts.py` | 4 |
| Revision prompt with feedback injection | `agents/backfill/prompts.py` | 4 |
| Multiple ChatPromptTemplates per agent | `agents/backfill/prompts.py` | 4 |
| `interrupt()` (HITL pause) | `agents/backfill/graph.py` | 4 |
| `Command(resume=...)` (HITL resume) | `agents/backfill/graph.py` | 4 |
| Checkpointing (`MemorySaver`) | `agents/backfill/graph.py` | 4 |
| `thread_id` for checkpoint identity | `agents/backfill/graph.py` | 4 |
| Multiple `ToolNode`s in one graph | `agents/backfill/graph.py` | 4 |
| Multiple LLM configs in one graph | `agents/backfill/graph.py` | 4 |
| Multi-phase graph topology (8 nodes) | `agents/backfill/graph.py` | 4 |
| Approval gate (conditional routing) | `agents/backfill/graph.py` | 4 |
| Rejection loop (revise → re-plan) | `agents/backfill/graph.py` | 4 |
| System prompt swapping between phases | `agents/backfill/graph.py` | 4 |
| Plan summary as AIMessage | `agents/backfill/graph.py` | 4 |
| Iteration counter reset on rejection | `agents/backfill/graph.py` | 4 |
| Two-phase invoke lifecycle | `agents/backfill/agent.py` | 4 |
| `_process_graph_result()` interrupt detection | `agents/backfill/agent.py` | 4 |
| `has_pending_approval` property | `agents/backfill/agent.py` | 4 |
| `_clear_phase_state()` lifecycle cleanup | `agents/backfill/agent.py` | 4 |
| Thread ID from correlation_id | `agents/backfill/agent.py` | 4 |
| Mocking interrupt detection (`get_state().next`) | `tests/agents/test_backfill_agent.py` | 4 |
| Two-phase test pattern (invoke + resume mocks) | `tests/agents/test_backfill_agent.py` | 4 |
| Rejection loop testing (multi-resume cycle) | `tests/agents/test_backfill_agent.py` | 4 |
| Guard rail testing (ValueError on bad state) | `tests/agents/test_backfill_agent.py` | 4 |
| `Command(resume=...)` verification in tests | `tests/agents/test_backfill_agent.py` | 4 |
| Phase state lifecycle testing | `tests/agents/test_backfill_agent.py` | 4 |
| Lazy graph building test | `tests/agents/test_backfill_agent.py` | 4 |
| Live two-phase lifecycle testing | `experiments/backfill_live_test.py` | 4 |
| Plan quality validation (structure checks) | `experiments/backfill_live_test.py` | 4 |
| Live rejection feedback (compare v1 vs v2) | `experiments/backfill_live_test.py` | 4 |
| Preflight dependency check | `experiments/backfill_live_test.py` | 4 |
| Execution audit display | `experiments/backfill_live_test.py` | 4 |
<!-- More rows will be added as new files are written -->

---

## Glossary

Terms used throughout the codebase, defined once.

**Agent** — An autonomous program that uses an LLM to decide which actions
to take. Unlike a simple LLM chat, an agent has tools it can call, state it
maintains, and a loop that continues until the task is done.

**ReAct** — Reason + Act. A pattern where the LLM alternates between
reasoning about what it knows and acting by calling a tool. The loop
continues until the LLM decides it has enough information to answer.

**LangGraph** — A framework for building agents as directed graphs. Nodes
are functions, edges connect them, and state flows through the graph.
Conditional edges let you branch (e.g., "if the LLM called a tool, go to
the tool node; otherwise, go to the output node").

**LangChain** — A library for building LLM applications. Argus uses
`langchain-core` (message types, tool decorator, prompt templates) but
not the higher-level chains. LangGraph is built on top of LangChain.

**StateGraph** — LangGraph's graph type where nodes share a typed state
dict. Each node receives the full state and returns a partial update.
Reducers control how updates merge.

**Tool** — A Python function decorated with `@tool` that an LLM can call.
The function's name, docstring, and type hints are serialized into a schema
the LLM understands. The LLM emits a `tool_calls` request; the framework
executes the function and returns the result as a `ToolMessage`.

**`.bind_tools()`** — A method on LangChain chat models that attaches tool
schemas to the LLM. After binding, the LLM can emit structured `tool_calls`
in its responses instead of (or alongside) text.

**Reducer** — In LangGraph, a function that controls how a node's return
value merges with existing state. `operator.add` for lists means "append";
no reducer means "replace" (last-write-wins).

**Gate** — In the TTAG pipeline, a validation checkpoint between layers.
Gate 3 checks Silver consistency before Gold runs. Gate 4 checks Gold-to-
Snowflake count match after Gold loads.

**Watermark** — A cursor tracking how far a pipeline has processed. In TTAG,
the `control.watermark` Iceberg table stores the last-processed snapshot ID
per table. If Silver's watermark is behind Bronze's, Silver hasn't caught up.

**Iceberg Snapshot** — Every write to an Iceberg table creates an immutable
snapshot with a unique ID. Snapshots form a chain (parent → child) and
record row-level statistics (added, deleted, updated). Time travel uses
snapshot IDs to read historical data.

**Correlation ID** — A unique identifier threaded through all log entries
for a single agent invocation. Lets you grep for one ID and see the complete
execution trace across tools, LLM calls, and infrastructure queries.

**TTAG** — Transactions Authorization Tag. The data pipeline that processes
travel-tagged card transactions through Bronze → Silver → Gold → Snowflake.
Argus sits on top of this pipeline as a diagnostic layer.

**Surrogate Key** — An auto-generated integer key in a dimension table
(e.g., `card_sk` in DIM_CARD). The fact table references dimensions by
surrogate key, not natural key. A NULL surrogate key in the fact table
means the dimension lookup failed — the natural key exists in Silver but
the dimension row is missing.

**Type 2 SCD** — Slowly Changing Dimension Type 2. When a dimension attribute
changes (e.g., a card's credit limit), the old row is marked inactive and a
new row is inserted with a new surrogate key. This preserves history — you
can see what the card's limit was when the transaction happened.

**ToolNode** — LangGraph's prebuilt graph node that automatically executes
tool calls from an AIMessage. It reads the `tool_calls` field, finds each
tool by name in the provided list, calls the function, and returns the
results as `ToolMessage`s. Eliminates manual tool dispatch boilerplate.

**Graph Compilation** — The `graph.compile()` step that freezes a StateGraph's
topology and returns a `CompiledStateGraph` runnable. Compilation validates
the structure (no orphan nodes, no missing edges, entry point is set) and
produces an object you `.invoke()` with initial state.

**Closure** — A function that captures variables from its enclosing scope.
In Argus, factory functions like `_make_llm_node(model)` return inner functions
that have access to `model` without it being a global or passed as an argument.
This is how LangGraph nodes get access to the LLM instance.

**MagicMock** — A mock object from Python's `unittest.mock` that automatically
creates attributes and methods on access. If you call `mock.invoke(state)`,
it doesn't crash — it records the call and returns another `MagicMock`. You
control what it returns via `mock.invoke.return_value = {...}`.

**`patch` / `patch.object`** — Context managers from `unittest.mock` that
temporarily replace a real object with a mock during a test. `patch.object(agent,
"build_graph")` replaces `agent.build_graph` with a mock, tracks calls, and
restores the original when the `with` block exits.

**pytest-asyncio** — A pytest plugin that lets you write `async def test_*()`
tests. Works with `@pytest.mark.asyncio` decorator or `asyncio_mode = "auto"`
in `pyproject.toml`. Without it, pytest doesn't know how to `await` async
test functions.

**Integration Test** — A test that exercises the full system with real
external dependencies (in Argus, a real LLM). Slower and non-deterministic,
but catches failures that unit tests can't: bad prompts, incorrect tool
schemas, structured output mismatches. Complements unit tests rather than
replacing them.

**Dead Letter Queue (DLQ)** — A holding area for messages or records that
failed processing. Instead of losing the data, the pipeline moves failures
to a DLQ for later investigation. In TTAG, the Benefit lane uses a Kafka
DLQ topic; the Booking lane uses an Iceberg quarantine table (`bad_files`).

**Classification Prompting** — A prompt engineering technique where the
system prompt provides a structured rubric mapping evidence patterns to
categories. Unlike open-ended investigation, classification constrains the
LLM's output to a fixed set of categories with clear criteria.

**Confidence Calibration** — Explicit guidance in the system prompt about
what confidence scores mean for specific evidence levels. Without calibration,
LLMs produce arbitrary confidence values. With it, "TimeoutException = 0.90"
anchors the scale so similar errors get similar scores.

**Idempotency Guard** — A mechanism that prevents the same operation from
being performed twice. In the DLQ agent, `_REQUEUED_RECORDS` is a set that
tracks which records have been requeued. If the LLM calls `requeue_message`
for the same record_id again (common in ReAct loops), the tool returns
"already requeued" instead of double-processing.

**Guarded Side Effect** — A tool call that changes external state (unlike
read-only queries) with safety mechanisms: prompt-level rules (don't call it
for the wrong category), idempotency guards (don't do it twice), rate limits
(don't do it too much), and audit trails (track what was done).

**Schema Changelog** — A record of schema evolution events in the pipeline.
The DLQ agent cross-references this to confirm SCHEMA_MISMATCH classifications
— a "pending_consumer_update" status in the changelog is strong evidence that
DLQ records with SchemaRegistryException are schema mismatches, not transient.

**Plan-then-Execute** — A workflow pattern where the agent first investigates
and produces a structured plan, then pauses for human approval before executing
the plan. Unlike ReAct (which loops freely between reasoning and acting),
Plan-then-Execute has a hard boundary between planning and execution. The
Backfill agent uses this because backfill execution has irreversible side
effects that require human sign-off.

**Human-in-the-Loop (HITL)** — A design pattern where an automated workflow
pauses at a designated point and waits for a human to make a decision before
continuing. In LangGraph, this is implemented with `interrupt()` (pauses the
graph) and `Command(resume=...)` (resumes with the human's decision). The graph
state is preserved across the pause via checkpointing.

**LangGraph Checkpointing** — The mechanism that saves graph state to persistent
storage so it survives across interrupt/resume cycles. `MemorySaver` stores
state in memory (dev/testing). `SqliteSaver` stores state in SQLite (production).
Without checkpointing, `interrupt()` would lose all state when the graph pauses.

**Pipeline Lock** — An exclusive lock that prevents concurrent writes to the
same pipeline entity during backfill. In Argus, `acquire_pipeline_lock` creates
a lock with ownership tracking and a 60-minute timeout. The lock prevents two
backfill operations from running on the same entity simultaneously, which would
corrupt data.

**Two-Registry Tool Separation** — A pattern where an agent's tools are split
into separate registries bound at different phases of the graph. Investigation
tools (read-only) are bound during the planning phase; execution tools (side
effects) are bound during the execution phase. This provides structural safety
beyond prompt-level guardrails — the LLM literally cannot call execution tools
during investigation because they aren't in its tool set.

**Rejection Loop** — In the HITL pattern, the ability for the human reviewer
to not just approve or reject, but to reject *with feedback*. The agent receives
the feedback, reworks its plan, and re-presents it for approval. This creates a
loop: plan → present → reject(feedback) → rework → present → approve → execute.

**`Command(resume=...)`** — LangGraph's mechanism for resuming an interrupted
graph with data from outside the graph (e.g., a human's approval decision).
When the graph calls `interrupt()`, it pauses. When the caller invokes the graph
again with `Command(resume={"approved": True})`, the interrupted node receives
the resume value and continues execution.

**Two-Level Iteration Control** — A safety pattern for agents with nested
loops. The inner loop caps how many tool calls the LLM can make in one
investigation round (`iteration` / `max_iterations`). The outer loop caps
how many times the plan can be rejected and reworked (`plan_iterations` /
`max_plan_iterations`). Each loop has its own counter and safety cap, and
the inner loop resets when the outer loop cycles — the LLM gets fresh
investigation budget for each rework attempt.

**Multi-Phase Prompting** — Using separate system prompts for each phase of
an agent's graph instead of one monolithic prompt. Each prompt focuses on
the LLM's role in that phase: investigation strategy, structured output
rubric, or execution safety rules. Prevents instruction leakage (the LLM
seeing execution rules during investigation) and reduces per-call token cost.

**Structured Output Rubric** — A prompt engineering technique that defines
quality criteria for each field of a structured output (like BackfillPlan).
For each field, the rubric provides good and bad examples so the LLM knows
what level of detail and specificity is expected. Similar to confidence
calibration but applied to plan quality rather than classification scores.

**Revision Prompt** — A template injected when a human rejects the agent's
plan. It frames the rejection explicitly, includes the human's feedback,
shows the iteration count for urgency ("attempt 2 of 3"), and instructs
the LLM to focus on what changed rather than starting over. Without the
revision prompt, the LLM would either reproduce the same plan or waste
iterations re-investigating from scratch.

**Plan as Intermediate Output** — A design pattern where the agent produces
its primary output (e.g., BackfillPlan) in the MIDDLE of the graph, not at
the end. The plan sits in state between the investigation and execution
phases, where the human reviews it. This contrasts with Recon and DLQ agents
where the report is the final step.

**Approval Flow State** — The set of state fields that drive the HITL
approval gate: `approval_status` (the routing signal: pending/approved/
rejected), `revision_feedback` (the human's rejection reason, injected as a
HumanMessage), and `plan_iterations` / `max_plan_iterations` (the outer
loop safety cap). These fields have no equivalent in autonomous agents.

**`interrupt()`** — LangGraph's function that pauses graph execution at a
node. The argument to `interrupt(value)` is returned to the caller (e.g.,
the plan for human review). The graph state is saved to the checkpointer,
and execution stops until the caller resumes with `Command(resume=...)`.
Code after the `interrupt()` call runs only upon resume.

**`Command(resume=...)`** — LangGraph's mechanism for resuming an interrupted
graph with external data. The caller passes
`graph.invoke(Command(resume={"decision": "approved"}), config=...)` and
the resume value is delivered to the node that called `interrupt()`. The
`config` must include the same `thread_id` so the checkpointer can find
the saved state.

**`MemorySaver`** — LangGraph's in-memory checkpointer for development and
testing. It stores graph checkpoints (state snapshots) in a Python dict.
Required for `interrupt()` to work — without a checkpointer, the graph
cannot save or restore state across pauses. Production alternatives include
`SqliteSaver` and `PostgresSaver`.

**`thread_id`** — The key that identifies a specific graph execution for
checkpointing. Passed in `config={"configurable": {"thread_id": "..."}}`.
The initial `invoke()` and the resuming `invoke()` must use the same
`thread_id` so the checkpointer links them to the same execution. Think
of it like a session ID for the graph.

**Approval Gate** — A graph node that pauses execution for human review
using `interrupt()`. It receives the human's decision via `Command(resume=...)`
and updates state fields (`approval_status`, `revision_feedback`) that drive
conditional routing to execution (approved), revision (rejected), or
termination (max revisions exceeded).

**System Prompt Swapping** — Replacing the system prompt between graph phases
so the LLM receives phase-appropriate instructions. The plan node filters out
`SystemMessage`s from the conversation history and prepends
`BACKFILL_PLANNING_PROMPT` instead of `BACKFILL_INVESTIGATION_PROMPT`. The
non-system messages (evidence from investigation) are preserved.

**Mocking Interrupt Detection** — The key test technique for the Backfill
agent. The real graph uses `graph.get_state(config).next` to detect whether
execution is paused at an interrupt (non-empty tuple like `("approval_gate",)`)
or completed (empty tuple `()`). In tests, you mock this by creating a
`MagicMock` with a `.next` attribute set to the desired value, then assigning
it as `mock_graph.get_state.return_value`. This avoids needing a real
checkpointer or graph compilation in unit tests.

**Two-Phase Test Pattern** — A testing approach for agents with invoke/resume
lifecycles. Unlike single-invoke agents where you set up one mock and call
once, two-phase tests configure the mock graph twice in the same test: first
for invoke (returning interrupted state), then reconfigure it for resume
(returning completed state). The mock's `invoke.return_value` and
`get_state().next` change between phases to simulate the graph's different
behaviors.

**Guard Rail Testing** — Tests that verify an agent rejects invalid usage
patterns with clear errors. For the Backfill agent, calling `resume()` without
a prior `invoke()` must raise `ValueError("No pending approval")`, because
there's no `_thread_id` or `_context` to resume from. Guard rails prevent
subtle bugs where the agent silently does the wrong thing instead of failing
fast.

**Plan Quality Validation** — Going beyond status-code checks (`result.status
== "needs_approval"`) to validate the structure and content of the plan itself.
Checks include: does it have an incident_summary, are proposed_steps ordered
correctly (step.order matches position), does it have a risk_assessment, and
is the estimated_duration non-empty. Catches cases where the LLM produces a
technically valid Pydantic model but with empty or nonsensical content.

**Preflight Check** — A dependency verification step at the start of a live
test that confirms required files exist before attempting to import them.
For `backfill_live_test.py`, this means checking that `graph.py` exists
before trying to build the agent, since missing graph.py would produce a
confusing ImportError deep in the call stack rather than a clear "build
this file first" message.

**Execution Audit Display** — A pretty-printing function in live tests that
renders the `execution_audit` trail from the Backfill agent's result. Each
entry is shown with a color-coded icon: 🔒 for lock acquisition, ⚙️ for
step execution, and 🔓 for lock release. This helps verify that the
Lock → Execute → Release pattern was followed correctly in real execution.

<!-- More terms will be added as new concepts are introduced -->
