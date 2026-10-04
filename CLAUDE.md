# Argus — Project Guidelines for Claude Code

## What This Is

Argus is a four-agent diagnostic platform that sits on top of the Travel Tag (TTAG) data pipeline. Named after the hundred-eyed watchman from Greek mythology — each agent watches a different failure surface in the pipeline.

**This is a learning project.** The owner is building it to deeply understand AI agent development — every concept, pattern, and mechanism — not to have code generated blindly. Explain thoroughly, walk through decisions, and make every concept stick.

## Repository Structure

```
D:\repos\argus/
├── CLAUDE.md
├── README.md
├── pyproject.toml
├── configs/
│   ├── dev/config.yaml
│   └── prod/config.yaml
├── argus/
│   ├── __init__.py
│   ├── core/
│   │   ├── config.py          # YAML config with env-var interpolation
│   │   ├── llm.py             # LLM client factory (provider abstraction)
│   │   ├── logging.py         # Structured JSON logging, correlation IDs
│   │   └── router.py          # Rules-based agent dispatcher
│   ├── agents/
│   │   ├── base.py            # Abstract agent interface (TriggerContext, AgentResult)
│   │   ├── reconciliation/
│   │   │   ├── state.py       # LangGraph state schema (ReconState)
│   │   │   ├── prompts.py     # System/human prompt templates
│   │   │   ├── graph.py       # LangGraph StateGraph (ReAct loop wiring)
│   │   │   └── agent.py       # BaseAgent subclass (platform adapter)
│   │   ├── dlq_triage/
│   │   │   ├── state.py       # LangGraph state schema (DLQTriageState)
│   │   │   ├── prompts.py     # Classification rubric + requeue safety rules
│   │   │   ├── graph.py       # LangGraph StateGraph (same ReAct topology)
│   │   │   └── agent.py       # BaseAgent subclass (DLQ adapter)
│   │   ├── backfill/
│   │   │   ├── __init__.py    # Package marker (exists)
│   │   │   ├── state.py       # BackfillState with HITL approval fields
│   │   │   ├── prompts.py     # Multi-phase prompts (investigation, planning, execution)
│   │   │   ├── graph.py       # Plan-then-Execute + HITL topology (TODO)
│   │   │   └── agent.py       # BaseAgent subclass, two-phase invoke (TODO)
│   │   └── spark_debugger/
│   │       ├── __init__.py    # Package marker
│   │       ├── state.py       # SparkDebuggerState with hypothesis + reflection fields
│   │       ├── prompts.py     # Investigation + reflection prompts, causal chain reasoning
│   │       ├── graph.py       # ReAct + Reflection topology (5 nodes, 2 conditional edges)
│   │       └── agent.py       # BaseAgent subclass with reflection metadata
│   ├── schemas/
│   │   └── reports.py         # Pydantic report models (all agents)
│   ├── tools/
│   │   ├── pipeline/
│   │   │   ├── recon_tools.py    # 6 @tool functions for Recon agent
│   │   │   ├── dlq_tools.py     # 3 @tool functions for DLQ agent (includes side effects)
│   │   │   └── backfill_tools.py # 8 @tool functions for Backfill agent (2 registries)
│   │   └── compute/
│   │       ├── __init__.py      # Package marker
│   │       └── spark_tools.py   # 6 @tool functions for Spark Debugger (SparkUI + event logs)
│   └── cli/
│       └── main.py            # Click CLI entry point
├── docs/
│   ├── LEARNING_GUIDE.md      # Living learning guide
│   └── code-guides/           # Per-file companion guides
│       ├── infrastructure-and-config/
│       ├── core-layer/
│       ├── agent-and-schema-layer/
│       ├── phase2-reconciliation/
│       ├── phase3-dlq-triage/
│       ├── phase4-backfill/
│       └── phase5-spark-debugger/
├── tests/
│   └── agents/
│       ├── test_recon_agent.py  # Recon unit tests (mocked LLM, 7 test classes)
│       ├── test_dlq_agent.py   # DLQ unit tests (mocked LLM, 7 test classes)
│       └── test_spark_debugger_agent.py  # Spark Debugger unit tests (8 test classes, FakeLLM)
├── experiments/
│   ├── calculator_agent.py    # Phase 0 — ReAct learning agent
│   ├── recon_live_test.py     # Phase 2 — Live integration test (real LLM)
│   ├── dlq_live_test.py       # Phase 3 — Live integration test (real LLM)
│   └── spark_debugger_live_test.py  # Phase 5 — Live integration test (2 scenarios: skew + GC)
└── .venv/
```

## Tech Stack

- **Python 3.13** (Windows venv for PyCharm) / **Python 3.10** (WSL — available but not primary)
- **LangGraph** — StateGraph, nodes, edges, conditional routing, checkpointing
- **LangChain Core** — @tool decorator, message types, .bind_tools()
- **LangChain Google GenAI** — ChatGoogleGenerativeAI (gemini-2.0-flash)
- **Pydantic v2** — state schemas, structured output (DiagnosticReport, BackfillPlan, etc.)
- **FastAPI** — REST trigger layer (future)
- **pytest** — testing

## The Four Agents

1. **Reconciliation Diagnostics** (pipeline-aware) — triggers on Gate 3/4 failure, investigates row counts, duplicate keys, NULL FKs, watermark gaps
2. **DLQ Triage & Auto-Remediation** (pipeline-aware) — classifies DLQ records (transient → requeue, schema mismatch, data quality, unknown → escalate)
3. **Incident & Backfill Planning** (pipeline-aware) — reads incident context, produces safe backfill plan with human-in-the-loop approval
4. **AI Spark Debugger** (compute-aware) — analyzes Spark logs/SparkUI/execution plans, surfaces actual bottleneck

## Architecture (6 Layers)

1. **Trigger Layer** — Airflow on_failure_callback, CLI, REST API
2. **Agent Router** — rules-based dispatcher (Phase 1), supervisor agent (Phase 6)
3. **Agent Layer** — each agent is a LangGraph StateGraph with state schema, system prompt, tool bindings, output schema
4. **Tool Layer** — Pipeline Tools (shared: query_watermark, compare_row_counts, etc.) + Compute Tools (Spark Debugger only)
5. **Infrastructure** — Iceberg, Kafka, Spark History Server, Airflow, Snowflake
6. **Output Layer** — structured Pydantic reports, Slack/PagerDuty notifications, audit log

## Development Phases

- **Phase 0** (COMPLETE): Agent foundations — calculator agent in `experiments/` to learn ReAct loop, tool design, LangGraph mechanics. Uses Gemini free tier.
- **Phase 1** (COMPLETE): Platform skeleton — config, LLM factory, logging, agent interface, schemas, router, CLI, YAML configs
- **Phase 2** (COMPLETE): Reconciliation Diagnostics agent
  - ✅ `tools/pipeline/recon_tools.py` — 6 investigation tools with simulated data
  - ✅ `agents/reconciliation/state.py` — LangGraph state schema (ReconState, make_initial_state)
  - ✅ `agents/reconciliation/prompts.py` — System prompt, ChatPromptTemplate
  - ✅ `agents/reconciliation/graph.py` — StateGraph with ReAct loop (entry, llm, tools, report nodes)
  - ✅ `agents/reconciliation/agent.py` — Agent class (BaseAgent subclass, invoke method)
  - ✅ `tests/agents/test_recon_agent.py` — Unit tests (7 test classes, mocked LLM)
  - ✅ `experiments/recon_live_test.py` — Live integration test with real LLM (2 scenarios)
- **Phase 3** (COMPLETE): DLQ Triage & Auto-Remediation agent
  - ✅ `tools/pipeline/dlq_tools.py` — 3 tools with simulated DLQ data + guarded side effects
  - ✅ `agents/dlq_triage/state.py` — LangGraph state schema (DLQTriageState, classification + requeue accumulators)
  - ✅ `agents/dlq_triage/prompts.py` — Classification rubric, confidence calibration, requeue safety rules
  - ✅ `agents/dlq_triage/graph.py` — StateGraph with same ReAct topology, DLQ-specific components
  - ✅ `agents/dlq_triage/agent.py` — Agent class (BaseAgent subclass, source_lane translation)
  - ✅ `tests/agents/test_dlq_agent.py` — Unit tests (7 test classes, mocked LLM)
  - ✅ `experiments/dlq_live_test.py` — Live integration test with real LLM (2 scenarios + requeue safety validation)
- **Phase 4** (IN PROGRESS): Incident & Backfill Planning agent (human-in-the-loop)
  - ✅ `tools/pipeline/backfill_tools.py` — 8 tools in 2 registries (5 investigation + 3 execution) with simulated data
  - ✅ `agents/backfill/state.py` — BackfillState with plan, approval_status, revision_feedback, two-level iteration control
  - ✅ `agents/backfill/prompts.py` — 3 system prompts (investigation, planning rubric, execution safety) + revision template
  - ✅ `agents/backfill/graph.py` — Plan-then-Execute + HITL interrupt/resume + rejection loop (8 nodes, 2 ToolNodes, 3 LLM configs, MemorySaver checkpointer)
  - ✅ `agents/backfill/agent.py` — BaseAgent subclass with two-phase invoke (invoke → needs_approval → resume, thread_id management, interrupt detection via graph_state.next, Command(resume=...))
  - ⬜ `tests/agents/test_backfill_agent.py` — Unit tests including interrupt/resume/rejection flows
  - ⬜ `experiments/backfill_live_test.py` — Live integration test with real LLM
- **Phase 5** (COMPLETE): AI Spark Debugger agent (ReAct + Reflection)
  - ✅ `tools/compute/spark_tools.py` — 6 compute-aware tools with simulated data (2 scenarios: data skew + GC pressure)
  - ✅ `agents/spark_debugger/state.py` — SparkDebuggerState with hypothesis, reflection_count, two-level iteration
  - ✅ `agents/spark_debugger/prompts.py` — Investigation + reflection prompts, causal chain reasoning, bottleneck categories
  - ✅ `agents/spark_debugger/graph.py` — ReAct + Reflection topology (5 nodes, 2 conditional edges, string-based routing)
  - ✅ `agents/spark_debugger/agent.py` — BaseAgent subclass with reflection metadata in results
  - ✅ `tests/agents/test_spark_debugger_agent.py` — Unit tests (8 test classes, FakeLLM, reflection loop testing)
  - ✅ `experiments/spark_debugger_live_test.py` — Live integration test (2 scenarios, 10 validation checks each)
- **Phase 6**: Integration, testing, observability

## Key Patterns

- **ReAct** (Reason → Act → Observe) — core loop for all agents
- **Plan-then-Execute** — Backfill agent
- **ReAct + Reflection** — Spark Debugger (hypothesis → evidence → refine)
- **Classification + Confidence Scoring** — DLQ Triage
- **Human-in-the-Loop** — LangGraph interrupt() for Backfill approval gates

## Phase 5 New Concepts (vs Phase 4)

- **ReAct + Reflection pattern** — after ReAct investigation loop, a reflect node evaluates hypothesis completeness before proceeding to report
- **Hypothesis as first-class state** — `hypothesis` field tracked across iterations, refined through reflection, audited in final results
- **Two-level iteration (reflection variant)** — inner loop (tool calls: `iteration`/`max_iterations=15`) + outer loop (reflections: `reflection_count`/`max_reflections=2`)
- **Reflect node with plain model** — LLM call without tools bound, forcing reasoning over action (can't investigate, must evaluate)
- **String-based routing for reflection** — `NEEDS_MORE_INVESTIGATION` / `HYPOTHESIS_CONFIRMED` keyword matching instead of structured output to preserve critique quality
- **Causal chain reasoning** — prompt engineering technique teaching agent to trace symptom → intermediate effect → root cause (not just list symptoms)
- **Compute-aware vs pipeline-aware tools** — completely different tool set from other agents (SparkUI REST API, event logs, physical plans vs watermarks, row counts, DLQ)
- **Spark execution hierarchy** — Application → Jobs → Stages → Tasks, tools organized around this hierarchy
- **Reflection metadata in results** — `_meta` dict (iterations, reflections, hypothesis) added to AgentResult for audit trail visibility
- **Three model configurations** — `model_with_tools` (investigation), plain `model` (reflection), `model.with_structured_output()` (report) in one graph

## Phase 4 New Concepts (vs Phase 3)

- **Two-registry tool separation** — `BACKFILL_INVESTIGATION_TOOLS` vs `BACKFILL_EXECUTION_TOOLS`, preventing LLM from accessing execution tools during investigation
- **Plan-then-Execute pattern** — agent investigates first, produces a BackfillPlan, pauses for human approval, then executes
- **Human-in-the-Loop (HITL)** — LangGraph `interrupt()` to pause graph for human approval, `Command(resume=...)` to continue
- **Rejection loop** — human can reject a plan with feedback, agent reworks and re-presents (not just approve/reject binary)
- **LangGraph checkpointing** — `MemorySaver` (dev) / `SqliteSaver` (prod) to persist state across interrupt/resume
- **Pipeline locking** — exclusive locks with ownership tracking, timeout-based auto-release, idempotent acquire
- **Lock → Execute → Release pattern** — enforced ordering for execution tools, lock check before every step
- **Per-step execution audit** — every `execute_backfill_step` call logged with timestamp and details
- **Step limit safety** — `_MAX_STEPS_PER_INVOCATION = 20` hard cap prevents runaway execution
- **Two-level iteration control** — inner loop (ReAct tool calls: `iteration`/`max_iterations`) + outer loop (plan revisions: `plan_iterations`/`max_plan_iterations`)
- **Plan as intermediate output** — `plan: BackfillPlan | None` is produced mid-graph (not at the end), sent to human for approval
- **Approval flow state** — `approval_status` (pending/approved/rejected) + `revision_feedback` drive the HITL conditional routing
- **Multi-phase prompting** — separate system prompts per graph phase (investigation, planning, execution) instead of one monolithic prompt
- **Structured output rubric** — planning prompt defines good vs bad examples for each BackfillPlan field (like DLQ's confidence calibration but for plan quality)
- **Revision prompt with feedback injection** — rejected plans get human feedback injected as a HumanMessage with iteration count for urgency

## Phase 3 New Concepts (vs Phase 2)

- **Classification prompting** — rubric with concrete examples, confidence calibration guidance, decision boundaries
- **Confidence scoring** — LLM outputs calibrated 0.0–1.0 scores with explicit "what makes X confidence" guidance
- **Guarded side effects** — `requeue_message` tool with idempotency guard, safety limit (10 max per invocation), audit trail
- **Side-effect audit trail** — `requeue_audit` state field tracks every requeue action for the final report
- **Incremental classification accumulator** — `classifications` state field with `operator.add` reducer
- **Dual DLQ lanes** — Kafka DLQ (Benefit) and bad_files Iceberg quarantine (Booking)
- **Safety validation in live tests** — post-hoc check that only TRANSIENT records with confidence ≥ 0.80 were requeued

## Environment

- **GOOGLE_API_KEY** must be set to run experiments (Gemini free tier)
- Future agents will use OpenAI or Anthropic (paid) — swap via config, no code changes
- IDE: PyCharm Community Edition (Windows venv required — CE doesn't support WSL interpreters)
- Venv activation:
  - Windows PowerShell: `.venv\Scripts\Activate.ps1`
  - Windows CMD: `.venv\Scripts\activate.bat`
  - WSL (if needed): `source .venv/bin/activate`
- Run calculator agent: `python experiments/calculator_agent.py`
- Run Recon live test: `python experiments/recon_live_test.py`
- Run DLQ live test: `python experiments/dlq_live_test.py`
- Run Spark Debugger live test: `python experiments/spark_debugger_live_test.py --scenario 1` (or `--scenario 2` for GC pressure)

## Conventions

- Every @tool function needs a detailed docstring — the LLM reads it to decide when/how to use the tool
- State schemas use Pydantic BaseModel with `Annotated[list[BaseMessage], add_messages]` for message history
- Structured output via Pydantic models (not raw dicts)
- Config via YAML per environment (dev/staging/prod)
- Structured logging with JSON + correlation IDs
- LangSmith tracing for debugging agent reasoning
- Side-effect tools must have idempotency guards and safety limits
- Classification prompts include calibration guidance and few-shot examples
