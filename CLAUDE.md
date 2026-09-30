# Argus — Project Guidelines for Claude Code

## What This Is

Argus is a four-agent diagnostic platform that sits on top of the Travel Tag (TTAG) data pipeline. Named after the hundred-eyed watchman from Greek mythology — each agent watches a different failure surface in the pipeline.

**This is a learning project.** The owner is building it to deeply understand AI agent development — every concept, pattern, and mechanism — not to have code generated blindly. Explain thoroughly, walk through decisions, and make every concept stick.

## Repository Structure

```
D:\repos\argus/
├── CLAUDE.md              # This file
├── README.md
├── experiments/           # Phase 0 — throwaway learning agents
│   └── calculator_agent.py  # ReAct loop, tool design, LangGraph basics (with verbose logging)
└── .venv/                 # Python venv (Windows Python 3.13 — for PyCharm compatibility)
```

## Tech Stack

- **Python 3.13** (Windows venv for PyCharm) / **Python 3.10** (WSL — available but not primary)
- **LangGraph** — StateGraph, nodes, edges, conditional routing, checkpointing
- **LangChain Core** — @tool decorator, message types, .bind_tools()
- **LangChain Google GenAI** — ChatGoogleGenerativeAI (gemini-3.8-flash for Phase 0 experiments)
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
- **Phase 1** (NEXT): Platform skeleton — config, tool registry, agent interface, repo structure
- **Phase 2**: Reconciliation Diagnostics agent (most bounded, pure investigation)
- **Phase 3**: DLQ Triage agent (classification + side effects)
- **Phase 4**: Backfill Planning agent (human-in-the-loop)
- **Phase 5**: Spark Debugger agent (most complex)
- **Phase 6**: Integration, testing, observability

## Key Patterns

- **ReAct** (Reason → Act → Observe) — core loop for all agents
- **Plan-then-Execute** — Backfill agent
- **ReAct + Reflection** — Spark Debugger (hypothesis → evidence → refine)
- **Classification + Confidence Scoring** — DLQ Triage
- **Human-in-the-Loop** — LangGraph interrupt() for Backfill approval gates

## Environment

- **GOOGLE_API_KEY** must be set to run Phase 0 experiments (Gemini free tier)
- Future agents will use OpenAI or Anthropic (paid) — swap via config, no code changes
- IDE: PyCharm Community Edition (Windows venv required — CE doesn't support WSL interpreters)
- Venv activation:
  - Windows PowerShell: `.venv\Scripts\Activate.ps1`
  - Windows CMD: `.venv\Scripts\activate.bat`
  - WSL (if needed): `source .venv/bin/activate`
- Run calculator agent: `python experiments/calculator_agent.py`

## Conventions

- Every @tool function needs a detailed docstring — the LLM reads it to decide when/how to use the tool
- State schemas use Pydantic BaseModel with `Annotated[list[BaseMessage], add_messages]` for message history
- Structured output via Pydantic models (not raw dicts)
- Config via YAML per environment (dev/staging/prod)
- Structured logging with JSON + correlation IDs
- LangSmith tracing for debugging agent reasoning
