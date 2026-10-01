# Code Guide: `argus/schemas/reports.py`

> **Read this BEFORE opening `argus/schemas/reports.py`.**
> This guide explains what the file does, why it exists, and every concept inside it.

---

## What Is This File About?

This is the **structured output schema library** — Pydantic models that define the exact shape of every agent's output. When the Recon agent produces a diagnosis, or the Backfill agent produces a plan, the output conforms to one of these schemas.

---

## What Feature Does It Bring to Argus?

1. **Machine-readable agent output** — every report is a structured Python object, not free-form text. Downstream systems (notification router, audit logger, API responses) can process fields directly
2. **LLM structured output** — these schemas can be passed to `model.with_structured_output(ReconReport)`, forcing the LLM to produce JSON that matches the schema exactly
3. **Validation** — Pydantic validates every field at construction time. If the LLM returns `severity: "CRITICAL"` instead of a valid `Severity` enum value, the error is caught immediately
4. **Documentation** — the schemas ARE the documentation for each agent's output format

---

## What Technology Is Leveraged?

| Technology | Role |
|---|---|
| **Pydantic v2 `BaseModel`** | Data validation, serialization, JSON schema generation |
| **`str, Enum`** | Python enum that's also a string — `Severity.P1 == "P1"` is True |
| **`Field(default_factory=...)`** | Safe mutable defaults for list/dict fields |
| **Type hints** | `list[ReconciliationFinding]`, `dict[str, Any]` — validated by Pydantic |

---

## Schema-by-Schema Breakdown

### Shared Schemas (used by all agents)

#### `Severity(str, Enum)`
```python
P1 = "P1"   # PagerDuty — immediate (pages the on-call engineer)
P2 = "P2"   # Slack — urgent (posts to channel)
P3 = "P3"   # Log only — informational
```

The `str` base class means `Severity.P1` serializes to `"P1"` in JSON (not `1` or `"Severity.P1"`). This dual inheritance (`str, Enum`) is a Pydantic pattern for enums that need to be JSON-serializable.

#### `Notification(BaseModel)`
Describes an alert to send. Each agent can return multiple notifications at different severities.

#### `AuditEntry(BaseModel)`
An immutable log record of an agent invocation. Stores everything needed for compliance and debugging: who triggered it, when it ran, what it decided, what it did, what went wrong.

### Reconciliation Schemas (Phase 2)

#### `ReconciliationFinding(BaseModel)`
A single piece of evidence from the Recon agent's investigation:
```python
check_name: str          # "duplicate_keys", "fk_integrity", etc.
table: str               # "silver.booking_detail"
partition: str | None     # "2026-09-28"
expected: str            # "14823 rows"
actual: str              # "14712 rows"
delta: str | None        # "111 rows missing"
possible_cause: str      # "Upstream re-delivery causing duplicates"
```

Each finding maps to one tool call's result. The Recon agent collects multiple findings during its investigation.

#### `ReconReport(BaseModel)`
The complete output of a reconciliation investigation:
```python
gate_failed: str                         # "gate_3"
run_date: str                            # "2026-09-28"
findings: list[ReconciliationFinding]    # all evidence gathered
root_cause_summary: str                  # "Stale watermark + duplicate booking_ids"
suggested_fix: str                       # "Re-run Silver job, then Gate 3"
recommended_severity: Severity           # P2
notifications: list[Notification]        # alerts to send
```

### Backfill Schemas (future — Phase 3)

#### `BackfillStep` / `BackfillPlan`
Structured backfill plan with ordered steps, watermark operations, collision checks, lock requirements, and approval gates.

### DLQ Schemas (future — Phase 4)

#### `DLQClassification(Enum)` / `DLQRecord` / `DLQTriageReport`
Dead Letter Queue record classification (transient, schema mismatch, data quality, unknown) with auto-requeue/quarantine/escalate actions.

### Spark Schemas (future — Phase 5)

#### `SparkBottleneck` / `SparkDiagnosis`
Performance diagnosis with bottleneck categories (skew, spill, small files, broadcast, GC pressure) and recommendations.

---

## How Structured Output Works with LLMs

This is a critical concept for agent development:

```python
# Without structured output:
response = model.invoke(messages)
# response.content = "I found 111 duplicates..."  ← free text, unparseable

# With structured output:
model_with_schema = model.with_structured_output(ReconReport)
response = model_with_schema.invoke(messages)
# response = ReconReport(gate_failed="gate_3", findings=[...])  ← validated Pydantic object
```

LangChain's `.with_structured_output(schema)` tells the LLM to return JSON matching the Pydantic schema. Under the hood, it:
1. Converts the Pydantic model to a JSON Schema
2. Passes the schema to the LLM as a function/tool definition
3. Parses the LLM's JSON response into a Pydantic object
4. Validates all fields

If the LLM returns invalid JSON, LangChain retries. This is why structured output is essential — it bridges the gap between "LLM generates text" and "downstream code needs data."

---

## Who Calls / Imports This File?

- **`agents/reconciliation/state.py`** → imports `ReconReport` for the graph state type
- **`agents/reconciliation/graph.py`** (upcoming) → uses `ReconReport` with `.with_structured_output()`
- **Future: notification router** → reads `report.notifications` to send alerts
- **Future: audit logger** → reads `AuditEntry` to write audit records
- **Tests** → construct report objects to test downstream processing

---

## Where Does This File Fit?

```
argus/
├── schemas/
│   └── reports.py       <── YOU ARE HERE (output schemas for all agents)
├── agents/
│   └── reconciliation/
│       ├── state.py     <── imports ReconReport
│       └── graph.py     <── will use ReconReport with structured output
└── core/                <── unrelated (infrastructure)
```

---

## Key Concepts to Understand

1. **Pydantic v2 vs v1**: Argus uses Pydantic v2, which is significantly faster and has a cleaner API. Key differences: `model_dump()` replaces `.dict()`, `model_dump_json()` replaces `.json()`, and `Field` is the primary way to set defaults and metadata.

2. **Why define all schemas in one file?** Because agents share concepts (Severity, Notification, AuditEntry). If each agent had its own schema file, these shared types would need to be imported across agents, creating circular dependencies. One file keeps it simple.

3. **`str, Enum` dual inheritance**: `class Severity(str, Enum)` means each enum value IS a string. This is important for JSON serialization — `json.dumps(Severity.P1)` produces `"P1"`, not an error.

4. **Schema as contract**: These models serve as the API contract between agents and their consumers. Changing a field name or type is a breaking change — downstream code that reads `report.root_cause_summary` would break if you renamed it. This is why versioning matters (future: v2 schemas alongside v1).

5. **Optional fields with `| None`**: Fields like `partition: str | None = None` and `delta: str | None = None` are nullable — not every finding has a partition or a delta. The `= None` default means the field can be omitted when constructing the object.
