# Code Guide: `argus/cli/main.py`

> **Read this BEFORE opening `argus/cli/main.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **command-line interface** for Argus — the `argus` command that lets you invoke agents, list registered agents, and check the version. It's the primary way humans interact with Argus during development and manual operations.

---

## What Feature Does It Bring to Argus?

1. **Manual agent invocation** — run any agent from the terminal: `argus invoke recon --run-date 2026-09-28`
2. **Agent discovery** — `argus list-agents` shows all available agents
3. **Environment selection** — `--env dev|staging|prod` picks the config file
4. **Correlation ID generation** — each CLI invocation gets a unique trace ID

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **Click** | Python CLI framework — decorators turn functions into commands with argument parsing, help text, and validation |
| **`click.group()`** | Creates a command group — `argus` is the top-level group, `invoke` and `list-agents` are subcommands |
| **`click.argument()`** | Positional argument — `agent_name` is required, no flag needed |
| **`click.option()`** | Named option — `--run-date`, `--params`, `--env` with types and defaults |
| **`click.version_option()`** | Adds `argus --version` that reads from `pyproject.toml` |
| **Lazy imports** | Heavy modules imported inside the function, not at module level |

---

## Code Flow

### `argus invoke recon --run-date 2026-09-28 --params '{"gate_failure":"gate_3"}'`

```
cli()  ← Click group (does nothing itself)
  │
  └── invoke("recon", run_date="2026-09-28", params='{"gate_failure":"gate_3"}', env="dev")
        │
        ▼
      load_config("dev")          ← reads configs/dev/config.yaml
        │
        ▼
      setup_logging(DEBUG, json)   ← configures JSON logger
        │
        ▼
      set_correlation_id()         ← generates "a3f8b2c1e9d0"
        │
        ▼
      TriggerContext(              ← builds the agent input
        agent_name="recon",
        trigger_source="cli",
        run_date="2026-09-28",
        correlation_id="a3f8b2c1e9d0",
        params={"gate_failure": "gate_3"},
      )
        │
        ▼
      [TODO] router.route(context) → agent.invoke(context)
      [Currently] prints context and "skeleton only" message
```

### `argus list-agents`

```
cli()
  └── list_agents()
        │
        ▼
      Prints hardcoded list of 4 agents
      [Future] reads from router.registered_agents
```

---

## Function-by-Function Breakdown

### `cli()` — Command Group

```python
@click.group()
@click.version_option(package_name="argus")
def cli():
    """Argus - The Hundred-Eyed Watchman."""
    pass
```

This is the root command. `@click.group()` makes it a container for subcommands. The function body is `pass` — it does nothing itself. The docstring becomes the help text for `argus --help`.

`@click.version_option(package_name="argus")` adds `argus --version`, reading the version from `pyproject.toml` (or `argus.__version__`).

### `invoke()` — Agent Invocation

```python
@cli.command()
@click.argument("agent_name")
@click.option("--run-date", required=True, ...)
@click.option("--params", default="{}", ...)
@click.option("--env", default="dev", ...)
def invoke(agent_name, run_date, params, env):
```

**Why lazy imports?** The `import asyncio`, `from argus.core.config import ...` lines are inside the function, not at the top of the file. This is deliberate:
- Click needs to register commands fast (top-level imports run on every `argus` invocation, even `argus --help`)
- Heavy modules (config, logging, base) only load when actually needed
- Faster CLI startup = better developer experience

**`json.loads(params)`**: The `--params` option takes a JSON string from the command line and parses it into a Python dict. This lets you pass arbitrary trigger parameters.

### `list_agents()` — Agent Discovery

Currently hardcoded — future versions will read from `router.registered_agents`. The hardcoded list serves as documentation of the planned agents.

### `if __name__ == "__main__": cli()`

Allows running the CLI directly with `python -m argus.cli.main`. In practice, users will use the `argus` entry point from `pyproject.toml`.

---

## Who Calls This File?

- **Users** via the `argus` command (entry point defined in `pyproject.toml`)
- **`pyproject.toml`** → `argus = "argus.cli.main:cli"` makes this the CLI entry point
- **Developers** running `python -m argus.cli.main` during development

---

## Where Does This File Fit?

```
                    ┌──────────┐
                    │ Terminal  │
                    │ $ argus  │
                    └────┬─────┘
                         │
                         ▼
                    ┌──────────┐
                    │ cli/     │
                    │ main.py  │   <── YOU ARE HERE
                    └────┬─────┘
                         │
           ┌─────────────┼─────────────┐
           ▼             ▼             ▼
    ┌──────────┐  ┌──────────┐  ┌──────────┐
    │ config   │  │ logging  │  │ router   │
    │ .py      │  │ .py      │  │ .py      │  → agents
    └──────────┘  └──────────┘  └──────────┘
```

The CLI is the **human entry point** to Argus. It's one of three planned trigger sources (CLI, API, Airflow callback), all of which produce a `TriggerContext` and route it to an agent.

---

## Key Concepts to Understand

1. **Click vs argparse**: Click is a third-party CLI framework that's more Pythonic than the stdlib `argparse`. Key features:
   - Decorators (`@click.command()`, `@click.option()`) instead of `parser.add_argument()`
   - Command groups for subcommands
   - Built-in help text generation
   - Type validation on parameters

2. **Command groups**: `@click.group()` creates a parent command that dispatches to subcommands. `@cli.command()` registers a subcommand under the `cli` group. This gives you `argus invoke` and `argus list-agents` from one group.

3. **Entry points**: `[project.scripts]` in `pyproject.toml` creates a system-wide command. After `pip install -e .`, the `argus` command calls `argus.cli.main:cli` — the `cli` function in this file. This is how Python packages create CLI tools.

4. **Trigger source abstraction**: The CLI is just one way to trigger agents. The key insight is that every trigger source (CLI, API, Airflow) produces the same `TriggerContext` object. The agent doesn't know or care whether it was triggered by a human typing `argus invoke` or an Airflow callback hitting the API.
