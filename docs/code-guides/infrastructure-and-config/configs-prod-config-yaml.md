# Code Guide: `configs/prod/config.yaml`

> **Read this BEFORE opening `configs/prod/config.yaml`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **production environment configuration** for Argus. It defines how Argus behaves when running against real TTAG pipeline infrastructure — real LLM (GPT-4o), real Kafka, real Snowflake, real alerting.

---

## What Feature Does It Bring to Argus?

1. **Production-grade LLM** — uses `gpt-4o` (OpenAI) for higher-quality reasoning in production diagnostics
2. **Environment variable interpolation** — secrets and endpoints come from `${ENV_VAR}` placeholders, never hardcoded
3. **Real alerting** — Slack, PagerDuty, and email notifications for agent findings
4. **Higher limits** — more iterations (15) and longer timeout (300s) because production failures can be more complex
5. **Database audit trail** — audit logs go to a database instead of a local file

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **GPT-4o** | OpenAI's production LLM — more capable than Gemini Flash for complex diagnostic reasoning |
| **Snowflake** | Cloud data warehouse — the serving layer where Gold data lands |
| **Hive Metastore** | Iceberg catalog in production — centralized table metadata |
| **PagerDuty** | Incident management — P1 findings trigger pages |
| **Slack** | Team communication — P2 findings post to a channel |

---

## Key Concept: Environment Variable Interpolation

```yaml
kafka_bootstrap: ${KAFKA_BOOTSTRAP_SERVERS}
```

The `${...}` syntax is **not** a YAML feature — it's Argus's own pattern. When `core/config.py` loads this file, `_resolve_env_vars()` replaces each `${VAR}` with the matching OS environment variable. This keeps secrets out of version control.

Common pattern in production:
- Infrastructure sets env vars via Kubernetes secrets, AWS Parameter Store, or Docker env
- The config file references them by name
- `_resolve_env_vars()` resolves them at startup

If an env var isn't set, the placeholder stays as-is (no crash) — this is a design choice that lets config loading succeed even with partial environment setup, useful for testing.

---

## Who Uses This File?

Same consumers as the dev config — `core/config.py` loads it when `ARGUS_ENV=prod` or `--env prod` is passed.

---

## Where Does This File Fit?

```
configs/
├── dev/
│   └── config.yaml     <── local development (see that guide)
└── prod/
    └── config.yaml     <── YOU ARE HERE (production)
```

---

## Design Decision: Why Two Separate Files Instead of Override Layers?

Some frameworks (Spring Boot, etc.) use a base config with environment-specific overrides. Argus uses **complete, standalone config files per environment** instead. The trade-off:

- **Pros**: Each file is self-contained and readable. No "which value wins?" confusion. Easy to diff dev vs prod.
- **Cons**: Some duplication (structure is the same). If you add a new config key, you must add it to both files.

For a four-agent platform, the simplicity of standalone files wins over DRY purity.
