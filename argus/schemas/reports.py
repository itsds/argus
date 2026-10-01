"""
Shared Pydantic schemas for Argus agent outputs.

Each agent defines its own report model that extends these bases.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Severity & Notification
# ---------------------------------------------------------------------------

class Severity(str, Enum):
    P1 = "P1"  # PagerDuty — immediate
    P2 = "P2"  # Slack — urgent
    P3 = "P3"  # Log only — informational


class Notification(BaseModel):
    """Notification to send after agent completes."""
    severity: Severity
    channel: str  # "pagerduty" | "slack" | "email" | "log"
    title: str
    body: str
    metadata: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Audit log entry
# ---------------------------------------------------------------------------

class AuditEntry(BaseModel):
    """Immutable record of an agent invocation."""
    correlation_id: str
    agent_name: str
    trigger_source: str
    triggered_at: datetime
    completed_at: datetime
    run_date: str
    status: str
    input_params: dict[str, Any]
    decisions: list[str] = Field(default_factory=list)
    actions_taken: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Backfill-specific schemas
# ---------------------------------------------------------------------------

class BackfillStep(BaseModel):
    """A single step in the proposed backfill plan."""
    order: int
    description: str
    watermark_key: str  # e.g. "booking__backfill"
    snapshot_range: str | None = None
    collision_check: str = ""
    requires_lock: bool = False
    requires_approval: bool = False


class BackfillPlan(BaseModel):
    """Structured output from the Backfill Planning agent."""
    incident_summary: str
    root_cause: str
    affected_partitions: list[str]
    proposed_steps: list[BackfillStep]
    estimated_duration_minutes: int | None = None
    risk_assessment: str = ""
    recommended_severity: Severity = Severity.P2
    notifications: list[Notification] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Reconciliation-specific schemas
# ---------------------------------------------------------------------------

class ReconciliationFinding(BaseModel):
    """A single finding from reconciliation diagnostics."""
    check_name: str
    table: str
    partition: str | None = None
    expected: str
    actual: str
    delta: str | None = None
    possible_cause: str = ""


class ReconReport(BaseModel):
    """Structured output from the Reconciliation Diagnostics agent."""
    gate_failed: str  # "gate_3" | "gate_4"
    run_date: str
    findings: list[ReconciliationFinding]
    root_cause_summary: str
    suggested_fix: str
    recommended_severity: Severity = Severity.P2
    notifications: list[Notification] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# DLQ-specific schemas
# ---------------------------------------------------------------------------

class DLQClassification(str, Enum):
    TRANSIENT = "transient"
    SCHEMA_MISMATCH = "schema_mismatch"
    DATA_QUALITY = "data_quality"
    UNKNOWN = "unknown"


class DLQRecord(BaseModel):
    """A single classified DLQ record."""
    record_id: str
    source_lane: str  # "kafka_dlq" | "bad_files"
    classification: DLQClassification
    confidence: float = 0.0
    reason: str = ""
    action_taken: str = ""


class DLQTriageReport(BaseModel):
    """Structured output from the DLQ Triage agent."""
    total_records: int
    records: list[DLQRecord]
    auto_requeued: int = 0
    quarantined: int = 0
    escalated: int = 0
    summary: str = ""
    recommended_severity: Severity = Severity.P3
    notifications: list[Notification] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Spark Debugger schemas
# ---------------------------------------------------------------------------

class SparkBottleneck(BaseModel):
    """A diagnosed performance bottleneck."""
    category: str  # "skew" | "spill" | "small_files" | "broadcast" | "gc_pressure"
    stage_id: int | None = None
    evidence: str
    impact: str  # "high" | "medium" | "low"
    recommendation: str


class SparkDiagnosis(BaseModel):
    """Structured output from the Spark Debugger agent."""
    app_id: str
    app_name: str | None = None
    total_duration_seconds: int | None = None
    bottlenecks: list[SparkBottleneck]
    root_cause_summary: str
    recommendations: list[str]
    recommended_severity: Severity = Severity.P3
    notifications: list[Notification] = Field(default_factory=list)
