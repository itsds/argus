# Code Guide: `argus/core/logging.py`

> **Read this BEFORE opening `argus/core/logging.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is Argus's **structured logging system**. It provides JSON-formatted log output with automatic correlation ID injection, so every log line from a single agent investigation can be traced together — even across async operations and tool calls.

---

## What Feature Does It Bring to Argus?

1. **Structured JSON logs** — every log line is a JSON object, parseable by `jq`, ELK Stack, Datadog, or any log aggregation tool
2. **Correlation ID tracing** — every log line from a single agent invocation includes the same `correlation_id`, enabling end-to-end trace of an investigation
3. **Context-aware logging** — uses Python's `ContextVar` so the correlation ID follows the execution context through async code without explicit passing
4. **Agent-aware extra fields** — logs can include `agent`, `tool`, `action`, `duration_ms`, `error` as structured fields

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **Python `logging`** | The standard library logging framework — handlers, formatters, log levels |
| **`contextvars.ContextVar`** | Thread-safe and async-safe context storage — the correlation ID "follows" the execution without being passed as a parameter |
| **`json.dumps`** | Converts log entries to single-line JSON strings |
| **`uuid.uuid4()`** | Generates unique correlation IDs (truncated to 12 chars for readability) |

---

## Code Flow

### Setup (runs once at startup)

```
setup_logging(level="DEBUG", fmt="json")
    │
    ▼
Get root logger: logging.getLogger("argus")
    │
    ▼
Set level: DEBUG / INFO / etc.
    │
    ▼
Create StreamHandler (writes to stdout)
    │
    ▼
Attach JsonFormatter (or plain text if fmt != "json")
    │
    ▼
Ready — all argus.* loggers inherit this config
```

### Per-invocation (runs each time an agent starts)

```
cid = set_correlation_id()        # generates "a3f8b2c1e9d0"
    │
    ▼
_correlation_id ContextVar is set  # available anywhere in this execution context
    │
    ▼
logger.info("Starting investigation", extra={"agent": "recon"})
    │
    ▼
JsonFormatter.format(record):
    {
      "timestamp": "2026-09-28T06:15:00Z",
      "level": "INFO",
      "logger": "argus.recon",
      "message": "Starting investigation",
      "correlation_id": "a3f8b2c1e9d0",    ← auto-injected from ContextVar
      "agent": "recon"                       ← from extra={}
    }
```

---

## Function-by-Function Breakdown

### `set_correlation_id(cid=None) -> str`

Generates or sets a correlation ID in the current context. The ID is a 12-char truncated UUID (e.g., `"a3f8b2c1e9d0"`). Truncated for readability — full UUIDs are 36 chars and make log lines hard to scan.

### `get_correlation_id() -> str`

Retrieves the current correlation ID from the `ContextVar`. Called by the `JsonFormatter` on every log line.

### `class JsonFormatter(logging.Formatter)`

Custom formatter that converts `LogRecord` objects to JSON strings. Key behaviors:
- Always includes: `timestamp`, `level`, `logger`, `message`
- Includes `correlation_id` if one is set (always should be during agent execution)
- Includes agent-specific fields (`agent`, `tool`, `action`, etc.) if passed via `extra={}`
- Includes exception info if the log call caught an error

### `setup_logging(level, fmt)`

Configures the root `argus` logger. The `if root.handlers: return` guard prevents double-configuration (important when tests or multiple entry points call this).

### `get_logger(name: str)`

Returns a child logger under the `argus` namespace. `get_logger("router")` returns `logging.getLogger("argus.router")`. Child loggers inherit the root's handlers and level.

---

## Who Calls This File?

- **`cli/main.py`** → `setup_logging()` + `set_correlation_id()` at startup
- **`agents/base.py`** → future: `set_correlation_id(context.correlation_id)` per invocation
- **Every module in Argus** → `get_logger("module_name")` for logging
- **`core/router.py`** → already uses `get_logger("router")`

---

## Where Does This File Fit?

```
argus/core/
├── config.py            <── provides log level/format settings
├── llm.py               <── unrelated
├── logging.py           <── YOU ARE HERE (observability infrastructure)
└── router.py            <── uses get_logger() for dispatch logging
```

This is **infrastructure code** — it doesn't implement any agent logic, but every agent depends on it for observability.

---

## Key Concepts to Understand

1. **`ContextVar` — why not just a global variable?**

   A global variable would break in async code. If two agent invocations run concurrently (via `asyncio`), they'd overwrite each other's correlation ID. `ContextVar` is designed for this — each async task gets its own copy of the variable. Think of it as "thread-local storage that also works with async."

2. **Structured logging vs text logging**

   Text log: `2026-09-28 06:15:00 INFO [argus.recon] Starting investigation (cid=a3f8b2c1e9d0)`
   JSON log: `{"timestamp":"2026-09-28T06:15:00Z","level":"INFO","logger":"argus.recon","message":"Starting investigation","correlation_id":"a3f8b2c1e9d0"}`

   JSON is harder to read in a terminal but infinitely better for log aggregation tools (Elasticsearch, Datadog, CloudWatch). You can query: "show me all logs where `agent=recon` and `level=ERROR`" — impossible with text logs without regex.

3. **`extra={}` in logging**

   Python's `logger.info("msg", extra={"agent": "recon"})` attaches arbitrary fields to the `LogRecord`. The `JsonFormatter` picks these up and includes them in the JSON output. This is how Argus adds structured context to logs without modifying the message string.

4. **Logger hierarchy**

   `logging.getLogger("argus.recon.tools")` is a child of `argus.recon`, which is a child of `argus`. Configuration on `argus` (level, handlers) propagates down to all children. This means `setup_logging()` only configures the root `argus` logger, and all module-specific loggers inherit it.
