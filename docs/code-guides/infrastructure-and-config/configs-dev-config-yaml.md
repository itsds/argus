# Code Guide: `configs/dev/config.yaml`

> **Read this BEFORE opening `configs/dev/config.yaml`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **development environment configuration** for Argus. It tells every component — the LLM client, the logger, the pipeline connectors, the agents — how to behave when running locally on a developer's machine.

---

## What Feature Does It Bring to Argus?

1. **Free-tier LLM for development** — uses `gemini-2.0-flash` via Google's free tier, so developers can iterate without incurring API costs
2. **Verbose logging** — DEBUG level with JSON format and correlation IDs for tracing agent behavior
3. **Local infrastructure** — points to localhost for Kafka, Schema Registry, Airflow, and Spark (these would run in Docker containers)
4. **Safe defaults** — notifications disabled (no accidental Slack/PagerDuty alerts), audit log goes to a local file

---

## What Technology Is Leveraged?

| Technology | Role in This File |
|---|---|
| **YAML** | The configuration format — human-readable, supports nesting and comments |
| **Gemini 2.0 Flash** | Google's free-tier LLM — fast and cost-free for development |
| **Apache Kafka** | Event streaming — Bronze layer ingests from Kafka topics |
| **Schema Registry** | Avro/Protobuf schema management for Kafka messages |
| **Apache Airflow** | Workflow orchestrator — triggers agents via callbacks |
| **Apache Spark** | Data processing engine — Spark History Server provides job metrics |
| **Iceberg** | Table format — `local` catalog means file-system-based metadata |

---

## Section-by-Section Walkthrough

### `llm:` — Language Model Settings

```yaml
llm:
  provider: google
  model: gemini-2.0-flash
  temperature: 0.0
  max_retries: 2
```

- **provider: google** → `core/llm.py` imports `ChatGoogleGenerativeAI`
- **model: gemini-2.0-flash** → Google's fast, free model (as of Phase 0)
- **temperature: 0.0** → Deterministic output. For diagnostics, you want reproducible reasoning, not creative variation
- **max_retries: 2** → If the API call fails (rate limit, timeout), retry twice before giving up

### `logging:` — Observability

```yaml
logging:
  level: DEBUG
  format: json
  correlation_id: true
```

- **DEBUG** in dev (see everything), **INFO** in prod (important events only)
- **json format** → structured logs that tools like `jq` can parse
- **correlation_id** → every log line from a single agent invocation shares the same ID, so you can trace a full investigation

### `pipeline:` — Infrastructure Endpoints

```yaml
pipeline:
  iceberg_catalog: local
  kafka_bootstrap: localhost:9092
  schema_registry: http://localhost:8081
  snowflake: null
  airflow_api: http://localhost:8080/api/v1
  spark_history: http://localhost:18080/api/v1
```

- **iceberg_catalog: local** → file-based catalog (no Hive Metastore needed in dev)
- **kafka_bootstrap: localhost:9092** → local Kafka broker
- **snowflake: null** → no Snowflake in dev (Gate 4 tools simulate the data)
- **airflow_api / spark_history** → REST APIs for the local Airflow and Spark instances

### `agents:` — Agent Behavior

```yaml
agents:
  max_iterations: 10
  timeout_seconds: 120
```

- **max_iterations: 10** → safety valve for the ReAct loop (prevents runaway tool calls)
- **timeout_seconds: 120** → kill the agent after 2 minutes (generous for dev)

### `output:` — Notifications & Audit

```yaml
output:
  notifications:
    slack_webhook: null
    pagerduty_key: null
    email: null
  audit_log:
    enabled: true
    destination: file
    path: logs/audit.jsonl
```

- All notifications are **null** (disabled) — you don't want dev runs paging anyone
- Audit log writes to a local JSONL file for inspection

---

## Who Uses This File?

- **`core/config.py`** → `load_config("dev")` reads and parses this file
- **`core/llm.py`** → reads `config.llm` to create the right chat model
- **`core/logging.py`** → reads `config.logging` for level and format
- **`cli/main.py`** → passes `--env dev` (default) which loads this config
- **Every agent** → reads `config.agents` for iteration limits and timeouts

---

## Where Does This File Fit?

```
configs/
├── dev/
│   └── config.yaml     <── YOU ARE HERE (local development)
└── prod/
    └── config.yaml     <── production (real infra, real LLM, real alerts)
```

The config system is environment-aware: `ARGUS_ENV=dev` (or `--env dev` on CLI) loads this file. Production loads `configs/prod/config.yaml` instead.

---

## How This Differs From Production Config

| Setting | Dev | Prod |
|---|---|---|
| LLM | gemini-2.0-flash (free) | gpt-4o (paid, more capable) |
| Log level | DEBUG | INFO |
| Kafka | localhost | `${KAFKA_BOOTSTRAP_SERVERS}` (env var) |
| Snowflake | null | Real connection with `${SNOWFLAKE_ACCOUNT}` |
| Notifications | All null | Slack + PagerDuty + email |
| Audit log | Local file | Database |
| Max iterations | 10 | 15 |
| Timeout | 120s | 300s |

The `${ENV_VAR}` syntax in production config is resolved by `core/config.py`'s `_resolve_env_vars()` function — secrets never live in the config file itself.
