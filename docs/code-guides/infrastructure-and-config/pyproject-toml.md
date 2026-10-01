# Code Guide: `pyproject.toml`

> **Read this BEFORE opening `pyproject.toml`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

`pyproject.toml` is the **project manifest** — the single file that tells Python's packaging ecosystem everything about Argus: its name, version, dependencies, CLI entry points, and development tooling. Think of it as the project's birth certificate and instruction manual rolled into one.

Before `pyproject.toml` existed (PEP 621, adopted ~2021), Python projects needed `setup.py`, `setup.cfg`, `requirements.txt`, and sometimes `MANIFEST.in` — four files doing what one does now.

---

## What Feature Does It Bring to Argus?

1. **Dependency declaration** — lists every library Argus needs at runtime (LangChain, LangGraph, Pydantic, PyYAML, Click) and at dev time (pytest, ruff)
2. **CLI entry point** — the `[project.scripts]` section makes `argus` a real command: after `pip install -e .`, typing `argus` in a terminal runs `argus.cli.main:cli`
3. **Optional dependency groups** — you can install just the core (`pip install .`), or add OpenAI/Anthropic/FastAPI/dev tools via extras like `pip install ".[all]"`
4. **Build system** — tells pip/build how to package Argus for distribution

---

## What Technology Is Leveraged?

| Technology | Role in This File |
|---|---|
| **TOML** | The file format itself — Tom's Obvious Minimal Language. Standardized for Python project config via PEP 518/621 |
| **setuptools** | The build backend (`build-system.requires`). Converts the project into an installable package |
| **pip** | Reads this file to resolve and install dependencies |
| **Click** | Listed as a dependency — powers the `argus` CLI |
| **Ruff** | Listed as a dev dependency — fast Python linter/formatter. Configured in `[tool.ruff]` |
| **pytest** | Listed as a dev dependency — test framework. Configured in `[tool.pytest.ini_options]` |

---

## Section-by-Section Walkthrough

### `[project]` — Identity & Dependencies

```toml
name = "argus"
version = "0.1.0"
description = "Argus — four-agent diagnostic platform for the TTAG pipeline"
requires-python = ">=3.11"
```

- **name**: The package name. `import argus` works because of this + the `argus/` directory.
- **version**: Semantic versioning. Also exposed as `argus.__version__`.
- **requires-python**: Argus needs Python 3.11+ because it uses features like `X | Y` union types and `match` statements.

```toml
dependencies = [
    "langchain-core>=0.3",
    "langgraph>=0.2",
    "langchain-google-genai>=2.0",
    "pydantic>=2.0",
    "pyyaml>=6.0",
    "click>=8.0",
]
```

These are the **core runtime dependencies** — installed whenever anyone does `pip install argus`:

| Dependency | Why Argus Needs It |
|---|---|
| `langchain-core` | Message types, tool decorator, prompt templates, BaseChatModel — the foundation |
| `langgraph` | StateGraph, conditional edges — the agent execution framework |
| `langchain-google-genai` | Gemini 2.0 Flash integration for Phase 0 (free tier dev) |
| `pydantic` | Structured output schemas (ReconReport, etc.) and data validation |
| `pyyaml` | Config file parsing (`configs/dev/config.yaml`) |
| `click` | CLI framework (`argus invoke`, `argus list-agents`) |

### `[project.optional-dependencies]` — Extras

```toml
openai = ["langchain-openai>=0.2"]
anthropic = ["langchain-anthropic>=0.2"]
api = ["fastapi>=0.110", "uvicorn>=0.30"]
dev = ["pytest>=8.0", "pytest-asyncio>=0.23", "ruff>=0.5"]
all = ["argus[openai,anthropic,api,dev]"]
```

Optional groups installed with `pip install "argus[openai]"` or `pip install "argus[all]"`:

- **openai/anthropic**: Alternative LLM providers for production (see `core/llm.py`)
- **api**: FastAPI + Uvicorn for Phase 5's REST API trigger endpoint
- **dev**: Testing and linting tools
- **all**: Everything at once — used in CI and local dev

### `[project.scripts]` — CLI Entry Point

```toml
argus = "argus.cli.main:cli"
```

This creates the `argus` command. `"argus.cli.main:cli"` means: import the `cli` object from `argus/cli/main.py` and call it. That `cli` object is a Click group (see the `cli/main.py` guide).

### `[build-system]` — How to Build

```toml
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"
```

Tells pip: "use setuptools to build this package." This is the standard choice — alternatives include `hatchling`, `flit`, and `poetry-core`.

### `[tool.ruff]` — Linter Config

```toml
line-length = 100
target-version = "py311"
```

Ruff is an extremely fast Python linter (written in Rust). We set line length to 100 (wider than PEP 8's 79, common in modern projects) and target Python 3.11.

### `[tool.pytest.ini_options]` — Test Config

```toml
asyncio_mode = "auto"
testpaths = ["tests"]
```

- **asyncio_mode = "auto"**: pytest-asyncio will automatically handle `async def test_*` functions without needing `@pytest.mark.asyncio` on each one. This matters because Argus agents are async.
- **testpaths**: Only look in `tests/` for test files.

---

## Who Uses This File?

- **pip** reads it during `pip install -e .` (editable install for development)
- **setuptools** reads it when building a distributable package
- **ruff** reads `[tool.ruff]` for linting configuration
- **pytest** reads `[tool.pytest.ini_options]` for test configuration
- **Developers** reference it to understand project dependencies

---

## Where Does This File Fit in the Argus Architecture?

```
pyproject.toml          <── YOU ARE HERE (project root)
├── configs/
│   ├── dev/config.yaml
│   └── prod/config.yaml
├── argus/              <── the Python package this file defines
│   ├── __init__.py
│   ├── core/
│   ├── agents/
│   ├── schemas/
│   ├── tools/
│   └── cli/
├── tests/
└── docs/
```

`pyproject.toml` is the **root of everything**. Without it, `argus` is just a folder of `.py` files. With it, `argus` becomes an installable Python package with a CLI, managed dependencies, and configured tooling.

---

## Key Concepts to Understand

1. **Editable install (`pip install -e .`)**: Installs the package in "development mode" — changes to source files take effect immediately without reinstalling. The `-e` flag creates a symlink rather than copying files.

2. **Extras/optional dependencies**: The `[all]` extra uses a self-referential syntax: `"argus[openai,anthropic,api,dev]"`. This is a pip feature — it installs all four groups at once.

3. **Entry points vs scripts**: `[project.scripts]` creates a console script. It's different from a shell script — pip generates a small wrapper that activates the right Python and calls the function.

4. **Version pinning strategy**: We use `>=` (minimum version) rather than `==` (exact pin). This allows compatible updates. For stricter reproducibility, you'd add a `requirements.lock` file.
