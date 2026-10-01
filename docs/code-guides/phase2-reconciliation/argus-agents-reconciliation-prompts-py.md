# Code Guide: `argus/agents/reconciliation/prompts.py`

> **Read this BEFORE opening `argus/agents/reconciliation/prompts.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **brain of the Reconciliation agent** — the system prompt that defines WHO the agent is, WHAT it knows about the TTAG pipeline, HOW it should investigate failures, and WHAT rules it must follow. Combined with the tool docstrings in `recon_tools.py`, this prompt drives the entire ReAct investigation loop.

---

## What Feature Does It Bring to Argus?

1. **Agent identity** — establishes the LLM as "a senior data engineer" with specific domain expertise
2. **Domain knowledge injection** — bakes in the full TTAG pipeline architecture so the LLM doesn't need to discover it
3. **Investigation strategy** — a 6-step numbered sequence that guides the ReAct loop toward systematic diagnosis
4. **Guardrails** — rules that prevent common LLM failure modes (guessing root causes, premature conclusions)
5. **Per-run parameterization** — the human prompt template fills in run-specific data (date, gate, params)

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **`ChatPromptTemplate.from_messages()`** | LangChain's prompt composition — combines system and human messages into a reusable template |
| **System message** | The "role definition" — sets the LLM's persona, knowledge, and constraints for the entire conversation |
| **Human message template** | The "task assignment" — tells the LLM what specific failure to investigate THIS time |
| **`{variable}` placeholders** | Template variables filled from graph state at runtime |
| **Prompt engineering techniques** | Role anchoring, domain context, tool strategy, constraint enforcement |

---

## Prompt Engineering Concepts in This File

### 1. Role Anchoring

```
"You are a senior data engineer investigating a reconciliation failure..."
```

This isn't decoration — it fundamentally changes the LLM's output quality. Without role anchoring, the LLM defaults to a generalist tone and misses domain-specific reasoning patterns (like knowing that stale watermarks cause count mismatches).

### 2. Domain Context Injection

```
## Pipeline Architecture
Bronze → Silver → Gold → Snowflake
...
Key tables: bronze.booking_raw, silver.booking_detail, gold.fact_travel_tag
...
Gate 3 (pre-Gold): Verifies Silver row counts...
Gate 4 (post-Gold): Verifies Snowflake count matches Gold...
```

The LLM has no prior knowledge of the TTAG pipeline. Every piece of domain context it needs must be in the prompt. This section is the agent's "mental model" of the system it's investigating.

### 3. Investigation Strategy

```
## Investigation Strategy
1. START with query_gate_results...
2. USE compare_row_counts...
3. If Bronze-to-Silver drop, CHECK check_duplicate_keys...
4. If Silver-to-Gold drop, CHECK check_fk_integrity...
5. ALWAYS check query_watermark_gaps before concluding...
6. For deeper investigation, use query_iceberg_snapshots...
```

This is the ReAct strategy layer. It tells the LLM the ORDER in which to use tools and WHEN each tool is appropriate. Combined with each tool's docstring (the "what does it do" layer), the LLM has both the strategy and the tactics.

### 4. Guardrails / Constraint Enforcement

```
## Rules
- Do NOT guess the root cause — gather evidence from at least 2-3 tools
- NEVER suggest a fix you haven't verified with tool evidence
- When results are ambiguous, call another tool to cross-check
```

These prevent the LLM's most common failure modes:
- **Premature conclusion**: LLM sees one anomaly and declares root cause without verification
- **Hallucinated fixes**: LLM suggests a fix for something it hasn't confirmed
- **Assumption bias**: LLM picks the most "interesting" result instead of the correct one

### 5. Template Variables

```python
RECON_HUMAN_PROMPT = """\
Investigate the following reconciliation failure:
- Run date: {run_date}
- Gate failed: {gate_name}
- Trigger params: {trigger_params}
"""
```

These `{variables}` are filled at runtime from the graph state. The system prompt is static (pipeline architecture doesn't change per run), but the human prompt is dynamic (each investigation has a different date and gate).

---

## Code Flow

```
RECON_PROMPT_TEMPLATE.invoke({
    "run_date": "2026-09-28",
    "gate_name": "gate_3",
    "trigger_params": '{"dag_id": "ttag_daily_dag"}'
})
    │
    ▼
Returns: [
    SystemMessage(content="You are a senior data engineer..."),
    HumanMessage(content="Investigate the following reconciliation failure:\n- Run date: 2026-09-28\n- Gate failed: gate_3\n...")
]
    │
    ▼
These messages seed the ReconState.messages list
    │
    ▼
The LLM reads them and starts its investigation
```

---

## The System Prompt ↔ Tool Docstring Relationship

This is one of the most important agent engineering concepts:

```
System Prompt (prompts.py)          Tool Docstrings (recon_tools.py)
┌────────────────────────┐          ┌─────────────────────────────┐
│ STRATEGY LAYER         │          │ TACTICAL LAYER              │
│                        │          │                             │
│ "START with            │ ──────── │ "Use this FIRST when        │
│  query_gate_results"   │  synergy │  investigating a recon      │
│                        │          │  failure. It tells you      │
│ "If Bronze-to-Silver   │          │  which gate failed..."      │
│  drop, CHECK           │ ──────── │                             │
│  check_duplicate_keys" │  synergy │ "Use this when you see a    │
│                        │          │  row count discrepancy      │
│ "ALWAYS check          │          │  between Bronze and Silver" │
│  watermarks before     │ ──────── │                             │
│  concluding"           │  synergy │ "Use this to check whether  │
│                        │          │  all layers processed the   │
└────────────────────────┘          │  same data range"           │
                                    └─────────────────────────────┘
```

The system prompt says WHEN to use each tool (strategy). The tool docstring says WHAT the tool does and HOW to interpret results (tactics). Together, they guide the LLM through a systematic investigation.

---

## Design Decision: Why a Separate `prompts.py`?

1. **Review-friendly** — prompt changes show as clean diffs, reviewable without agent logic
2. **Testable** — you can unit-test prompt formatting independently (`RECON_PROMPT_TEMPLATE.invoke({...})`)
3. **Iterable** — prompt engineering is an iterative process; isolating prompts from code makes iteration faster
4. **Versionable** — track prompt evolution in git history

---

## Who Calls / Imports This File?

- **`agents/reconciliation/graph.py`** (upcoming) → entry node calls `RECON_PROMPT_TEMPLATE.invoke({...})` to seed the conversation
- **Tests** → verify prompt formatting with different inputs

---

## Where Does This File Fit?

```
argus/agents/reconciliation/
├── prompts.py           <── YOU ARE HERE (agent's brain — identity, knowledge, strategy)
├── state.py             <── data schema (prompt output → messages field)
├── graph.py             (upcoming — entry node invokes the prompt template)
└── agent.py             (upcoming — orchestrates everything)
```

---

## Key Concepts to Understand

1. **`ChatPromptTemplate.from_messages()`**: Takes a list of `(role, content)` tuples and returns a template that, when invoked with variables, produces a list of `Message` objects. The roles are: `"system"`, `"human"`, `"ai"`, `"tool"`.

2. **System vs Human message**: The system message sets the LLM's persona and constraints for the ENTIRE conversation. The human message is the specific task. In a ReAct loop, additional human messages aren't added — the LLM communicates via AIMessage/ToolMessage cycles.

3. **Line continuation with `\`**: The raw strings use `\` at end of lines to prevent unwanted newlines in the prompt. `"line one \\\nline two"` becomes `"line one line two"`. This keeps the prompt readable in code while compact in the actual message.

4. **Prompt as code**: In agent development, the prompt IS part of the implementation. A change to the investigation strategy is a code change that should be reviewed, tested, and versioned — just like changing a function's algorithm.
