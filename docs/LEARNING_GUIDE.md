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

<!-- More terms will be added as new concepts are introduced -->
