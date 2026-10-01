# Code Guide: `argus/core/config.py`

> **Read this BEFORE opening `argus/core/config.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is Argus's **configuration loader** — it reads a YAML config file, resolves environment variable placeholders (`${VAR}`), and provides a clean Python object (`ArgusConfig`) that every other module queries for settings.

---

## What Feature Does It Bring to Argus?

1. **Environment-aware configuration** — load different settings for dev/staging/prod with a single function call
2. **Secret management** — `${ENV_VAR}` placeholders in YAML get replaced with real values from the OS environment, keeping secrets out of version control
3. **Dot-notation access** — `config.get("llm.provider")` navigates nested YAML without chaining `.get()` calls
4. **Central config object** — every component gets the same `ArgusConfig` instance, ensuring consistency

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **PyYAML (`yaml.safe_load`)** | Parses YAML into Python dicts. `safe_load` is used (not `load`) to prevent arbitrary code execution from malicious YAML |
| **`re` (regex)** | `_resolve_env_vars()` uses a regex pattern `\$\{([^}]+)\}` to find and replace env var placeholders |
| **`os.environ`** | Reads environment variables for secret resolution |
| **`pathlib.Path`** | Modern Python path manipulation — `Path(__file__).parent.parent.parent` navigates from `argus/core/config.py` up to the project root |

---

## Code Flow

```
load_config("dev")
    │
    ▼
Determine env: "dev" (from arg, ARGUS_ENV, or default)
    │
    ▼
Build path: <project_root>/configs/dev/config.yaml
    │
    ▼
yaml.safe_load() → raw Python dict
    │
    ▼
_walk_and_resolve(raw)  ← recursively resolves ${ENV_VAR} in all strings
    │
    ├── _resolve_env_vars("${KAFKA_BOOTSTRAP_SERVERS}")
    │       → regex finds ${...} → os.environ.get() → resolved string
    │
    └── Recurses into nested dicts and lists
    │
    ▼
ArgusConfig(resolved_dict)  ← wraps dict with convenience accessors
    │
    ▼
Returns config object with:
    config.llm          → {"provider": "google", "model": "gemini-2.0-flash", ...}
    config.logging       → {"level": "DEBUG", ...}
    config.pipeline      → {"iceberg_catalog": "local", ...}
    config.get("llm.provider") → "google"
```

---

## Function-by-Function Breakdown

### `_resolve_env_vars(value: str) -> str`

**What**: Replaces `${VAR_NAME}` patterns in a string with the corresponding environment variable value.

**How**: Uses `re.compile(r"\$\{([^}]+)\}")` — this regex means:
- `\$\{` — literal `${`
- `([^}]+)` — capture group: one or more characters that aren't `}`
- `\}` — literal `}`

**Edge case**: If the env var isn't set, the placeholder stays as-is. This is intentional — it lets you load production config locally without all secrets set, useful for testing config structure.

### `_walk_and_resolve(obj: Any) -> Any`

**What**: Recursively walks a nested dict/list structure and resolves env vars in every string.

**Pattern**: This is the **visitor pattern** applied to a nested data structure — a common approach when you need to transform all leaf nodes (strings) without changing the tree structure (dicts/lists).

### `class ArgusConfig`

**What**: A thin wrapper around the resolved config dict.

**Why not just use the dict?** The class provides:
- Named attributes (`config.llm` vs `config["llm"]`) — more readable
- The `get()` method with dot-notation (`config.get("agents.reconciliation.max_iterations", 10)`) — avoids chained `.get()` calls
- A natural place to add config validation later

### `load_config(env: str | None = None) -> ArgusConfig`

**What**: The public entry point. Loads config for the given environment.

**Path resolution**: `Path(__file__).parent.parent.parent / "configs" / env / "config.yaml"`
- `__file__` = `.../argus/core/config.py`
- `.parent` = `.../argus/core/`
- `.parent.parent` = `.../argus/`
- `.parent.parent.parent` = `.../<project_root>/`
- So: `<project_root>/configs/dev/config.yaml`

---

## Who Calls This File?

- **`cli/main.py`** → `load_config(env)` is the first thing the CLI does
- **Every agent** → receives the `ArgusConfig` in its constructor via `BaseAgent.__init__(config)`
- **`core/llm.py`** → `create_llm(config)` reads `config.llm`
- **`core/logging.py`** → `setup_logging()` reads `config.logging`
- **Future: API server** → will call `load_config("prod")` at startup

---

## Where Does This File Fit?

```
argus/core/
├── config.py            <── YOU ARE HERE (loads and provides config)
├── llm.py               <── reads config.llm to create chat model
├── logging.py           <── reads config.logging for log setup
└── router.py            <── reads config (indirectly, through agents)
```

`config.py` is the **first thing that runs** in any Argus invocation. It's the foundation layer — everything else depends on it, but it depends on nothing else in Argus (only stdlib + PyYAML).

---

## Key Concepts to Understand

1. **Environment-based configuration**: The pattern of having separate config files per environment (dev/staging/prod) is industry-standard. It separates "what does this component need?" (config structure) from "what are the values for this deployment?" (config values).

2. **`yaml.safe_load` vs `yaml.load`**: Never use `yaml.load()` — it can execute arbitrary Python code embedded in YAML. `safe_load()` only allows basic types (str, int, list, dict, etc.).

3. **Graceful env var resolution**: Unreplaced `${VAR}` placeholders don't crash — they stay as literal strings. This is a design trade-off: safer startup vs potential silent misconfiguration. In production, you'd add validation to catch unresolved placeholders.

4. **`Path(__file__)` navigation**: This pattern anchors file paths relative to the source file's location, making them work regardless of where you run the project from (important when pip installs the package elsewhere).
