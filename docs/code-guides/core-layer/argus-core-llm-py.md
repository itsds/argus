# Code Guide: `argus/core/llm.py`

> **Read this BEFORE opening `argus/core/llm.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **LLM client factory** — a single function that creates the right LangChain chat model based on Argus's configuration. It's the bridge between Argus's config system and LangChain's multi-provider ecosystem.

---

## What Feature Does It Bring to Argus?

1. **Provider abstraction** — agents never import a specific LLM library; they call `create_llm(config)` and get back a `BaseChatModel` they can use regardless of whether it's Gemini, GPT-4o, or Claude
2. **Zero-cost provider switching** — change one line in config (`provider: openai`) and the entire platform switches LLMs. No agent code changes needed
3. **Free-tier development** — defaults to `gemini-2.0-flash` which is free, so developers can iterate without API costs

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **LangChain Core** | `BaseChatModel` — the abstract interface all chat models implement |
| **langchain-google-genai** | `ChatGoogleGenerativeAI` — Gemini integration |
| **langchain-openai** | `ChatOpenAI` — GPT-4o / GPT-4 integration |
| **langchain-anthropic** | `ChatAnthropic` — Claude integration |
| **Factory pattern** | A creation pattern — one function decides which class to instantiate based on config |
| **Lazy imports** | Provider libraries are imported inside `if` branches, not at the top of the file |

---

## Code Flow

```
create_llm(config)
    │
    ▼
Read config.llm: provider, model, temperature
    │
    ├── provider == "google"
    │       → from langchain_google_genai import ChatGoogleGenerativeAI
    │       → return ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0.0)
    │
    ├── provider == "openai"
    │       → from langchain_openai import ChatOpenAI
    │       → return ChatOpenAI(model="gpt-4o", temperature=0.0)
    │
    ├── provider == "anthropic"
    │       → from langchain_anthropic import ChatAnthropic
    │       → return ChatAnthropic(model="claude-...", temperature=0.0)
    │
    └── else → raise ValueError("Unknown LLM provider")
```

---

## Key Design Decisions

### Why Lazy Imports?

```python
if provider == "google":
    from langchain_google_genai import ChatGoogleGenerativeAI
```

The imports are inside the `if` blocks, not at the top of the file. This is intentional:

- **Only the needed library is imported** — if you're using Google, the OpenAI and Anthropic packages don't need to be installed
- **This enables optional dependencies** — `pyproject.toml` lists `langchain-openai` and `langchain-anthropic` as optional extras, not core requirements
- **Faster startup** — unused providers don't add import overhead

### Why Return `BaseChatModel`?

```python
def create_llm(config: ArgusConfig) -> BaseChatModel:
```

`BaseChatModel` is LangChain's abstract base class for all chat models. By returning this type, the function's callers don't know (or care) which specific model they got. This is the **Liskov Substitution Principle** in action — any `BaseChatModel` can:
- Accept messages via `.invoke()` or `.ainvoke()`
- Bind tools via `.bind_tools()`
- Stream responses via `.stream()`

### Why `temperature: 0.0`?

For diagnostic agents, reproducibility matters more than creativity. A temperature of 0.0 makes the model deterministic — the same input produces (nearly) the same output every time. This is crucial for:
- Debugging agent behavior
- Regression testing
- Audit trails that need to be reproducible

---

## Who Calls This File?

- **`agents/reconciliation/agent.py`** (upcoming) → `create_llm(config)` to get the chat model, then `.bind_tools(RECON_TOOLS)` to give it tool access
- **Every future agent** → same pattern: get model from factory, bind agent-specific tools
- **`experiments/`** → test scripts use this to get a model for experimentation

---

## Where Does This File Fit?

```
argus/core/
├── config.py            <── provides config to this file
├── llm.py               <── YOU ARE HERE (creates LLM from config)
├── logging.py           <── unrelated (logging infra)
└── router.py            <── unrelated (agent dispatch)
```

The dependency chain: `config.yaml` → `config.py` → `llm.py` → agent. The LLM factory sits between config and agents, translating configuration into a usable LLM client.

---

## Key Concepts to Understand

1. **Factory pattern**: Instead of each agent constructing its own LLM (`ChatOpenAI(model=...)` everywhere), a single factory function centralizes the creation logic. Benefits: consistency (all agents use the same settings), configurability (change once in config), and testability (mock the factory in tests).

2. **LangChain's provider architecture**: LangChain separates the interface (`langchain-core`) from implementations (`langchain-openai`, `langchain-google-genai`, etc.). Your code depends on the interface; the implementation is pluggable.

3. **`BaseChatModel` interface**: The key methods agents use:
   - `.invoke(messages)` → single response
   - `.ainvoke(messages)` → async single response
   - `.bind_tools(tools)` → returns a new model instance that knows about tools
   - `.with_structured_output(schema)` → returns a model that outputs Pydantic objects

4. **API key management**: The LangChain provider classes automatically read API keys from environment variables (`GOOGLE_API_KEY`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`). This file doesn't handle keys — it delegates to each provider's built-in key resolution.
