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

<!-- PHASE 2 REMAINING FILES WILL BE ADDED HERE -->

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

<!-- More terms will be added as new concepts are introduced -->
