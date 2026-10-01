# Code Guide: `argus/__init__.py`

> **Read this BEFORE opening `argus/__init__.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **package initializer** for Argus. It's the smallest file in the codebase — just a docstring and a version number — but it plays a foundational role: it's what makes `argus/` a Python package rather than just a directory of `.py` files.

---

## What Feature Does It Bring to Argus?

1. **Package identity** — the presence of `__init__.py` tells Python "this directory is a package you can import"
2. **Version tracking** — `__version__ = "0.1.0"` provides a single source of truth (matches `pyproject.toml`)
3. **Project documentation** — the module docstring lists all four agents, giving any developer who does `import argus; help(argus)` a quick orientation

---

## What Technology Is Leveraged?

| Concept | Role |
|---|---|
| **Python packages** | `__init__.py` is the standard mechanism for declaring a directory as a Python package |
| **`__version__`** | Convention for exposing package version programmatically (`argus.__version__`) |
| **Module docstring** | The triple-quoted string at the top — accessible via `help(argus)` or `argus.__doc__` |

---

## Code Flow

There is no "flow" — this file runs once when `import argus` is first executed. Python reads it, sets `__version__`, and the package is ready. Subsequent imports of submodules (`from argus.core.config import ...`) don't re-execute this file.

---

## Who Calls / Imports This File?

- **Every import** of any `argus.*` module triggers this file (on first access)
- **`pyproject.toml`** references the version (should match `__version__`)
- **`click.version_option(package_name="argus")`** in `cli/main.py` reads this version for `argus --version`
- **Other packages** that depend on Argus can check `argus.__version__` at runtime

---

## Where Does This File Fit?

```
argus/
├── __init__.py          <── YOU ARE HERE (package root)
├── core/                <── shared infrastructure (config, LLM, logging, routing)
├── agents/              <── agent implementations
├── schemas/             <── Pydantic output schemas
├── tools/               <── LangChain tools for agents
└── cli/                 <── command-line interface
```

This is the top-level entry point. It's the "lobby" of the Argus building — every visitor passes through it.

---

## Key Concept: `__init__.py` in Modern Python

In Python 3.3+ you can technically have "implicit namespace packages" (directories without `__init__.py`). Argus uses an explicit `__init__.py` because:

- It's the universally understood convention
- It provides a natural place for `__version__` and the project docstring
- It makes the package boundary explicit to tools (linters, IDEs, build systems)
- It ensures `import argus` works predictably across all Python versions and environments
