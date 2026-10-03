"""
Backfill Planning tools for the Argus Backfill & Incident Planning agent.

These tools query TTAG pipeline infrastructure to investigate incidents and
plan safe backfill operations. The Backfill agent uses them in two phases:

  PHASE 1 — INVESTIGATION (ReAct loop):
    The agent investigates the incident using read-only tools to understand
    what happened, what data is affected, and whether it's safe to backfill.
    Tools: get_incident_context, assess_data_gaps, check_pipeline_locks,
    get_backfill_history, validate_source_readiness.

  PHASE 2 — EXECUTION (after human approval):
    Once the human approves the backfill plan, the agent executes it using
    side-effect tools. These acquire locks, trigger backfills, and release
    locks — each with safety guards.
    Tools: acquire_pipeline_lock, execute_backfill_step, release_pipeline_lock.

WHAT'S NEW IN PHASE 4 (compared to Phase 2 Recon and Phase 3 DLQ):

  1. INVESTIGATION + EXECUTION SPLIT — unlike Recon (read-only) and DLQ
     (classify + requeue), the Backfill agent has a clear separation:
     investigation tools are read-only, execution tools have side effects.
     The human approval gate sits between the two phases.

  2. PIPELINE LOCKING — backfill operations must acquire an exclusive lock
     on the pipeline segment being backfilled. This prevents concurrent
     writes from corrupting data. The lock has:
       - Ownership tracking (who holds it and why)
       - Timeout-based auto-release (prevents deadlocks)
       - Idempotency (same owner re-acquiring is a no-op)

  3. MULTI-STEP EXECUTION — unlike DLQ's single requeue action, backfill
     execution involves multiple ordered steps (acquire lock → replay
     partition 1 → replay partition 2 → ... → release lock). Each step
     is individually audited with success/failure status.

  4. SAFETY VALIDATION — validate_source_readiness checks that the source
     data actually exists before the agent includes it in the plan. No
     point planning a backfill for a snapshot that's been expired.

TOOL DESIGN NOTES:
  - Each docstring is prompt engineering: it tells the LLM what the
    tool does, when to use it, and what the output means.
  - Return types are always str (LangChain convention for tool output
    that goes back into the message stream).
  - Investigation tools are stateless; execution tools track state via
    module-level variables (lock registry, execution audit).

DEV MODE:
  In dev, these tools return simulated data mimicking real pipeline
  incidents and backfill scenarios. In production, they'd query Airflow
  DAG run history, the watermark control table, Iceberg metadata, and
  the pipeline lock service (ZooKeeper / database-backed).
"""

import json
from datetime import datetime, timezone

from langchain_core.tools import tool


# ---------------------------------------------------------------------------
# Simulated data store
# ---------------------------------------------------------------------------
# Each scenario represents a realistic pipeline incident that requires a
# backfill. Scenarios are selected by run_date and entity parameters.

_SIMULATED_INCIDENTS = {
    "2026-09-28": {
        "incident_id": "INC-20260928-001",
        "trigger": "gate_3_failure",
        "detected_at": "2026-09-28T06:45:00Z",
        "severity": "P2",
        "affected_entity": "booking",
        "affected_layers": ["silver", "gold"],
        "gate_details": {
            "gate": "gate_3",
            "status": "FAILED",
            "check": "pre_gold_reconciliation",
            "message": "Gate 3 pre-Gold reconciliation failed: Silver "
                       "booking_detail row count (14712) is 111 rows short "
                       "of Bronze booking_raw (14823). Proceeding to Gold "
                       "blocked.",
        },
        "dag_run": {
            "dag_id": "ttag_daily_dag",
            "execution_date": "2026-09-28T06:00:00Z",
            "state": "failed",
            "failed_task": "gate_3_pre_gold_recon",
        },
        "related_alerts": [
            "Kafka consumer lag spike on ttag.booking.raw at 05:42 UTC",
            "Silver MERGE INTO job completed but with 111 duplicate key "
            "warnings at 05:48 UTC",
        ],
        "timeline": [
            {"ts": "2026-09-28T05:30:00Z", "event": "Bronze ingestion started"},
            {"ts": "2026-09-28T05:42:00Z", "event": "Kafka consumer lag spike "
                                                      "detected on booking topic"},
            {"ts": "2026-09-28T05:48:00Z", "event": "Silver MERGE INTO completed "
                                                      "with 111 duplicate warnings"},
            {"ts": "2026-09-28T06:12:00Z", "event": "Bronze watermark updated"},
            {"ts": "2026-09-28T06:15:00Z", "event": "Gate 3 check started"},
            {"ts": "2026-09-28T06:45:00Z", "event": "Gate 3 FAILED — Gold blocked"},
        ],
    },
    "2026-09-27": {
        "incident_id": "INC-20260927-002",
        "trigger": "gate_4_failure",
        "detected_at": "2026-09-27T07:10:00Z",
        "severity": "P2",
        "affected_entity": "booking",
        "affected_layers": ["gold"],
        "gate_details": {
            "gate": "gate_4",
            "status": "FAILED",
            "check": "post_gold_snowflake_match",
            "message": "Gate 4 post-Gold check: Snowflake FACT_TRAVEL_TAG "
                       "count (15087) is 15 rows short of Gold Iceberg "
                       "(15102). Delta 0.099%. 15 rows have NULL card_sk "
                       "due to missing DIM_CARD entries.",
        },
        "dag_run": {
            "dag_id": "ttag_daily_dag",
            "execution_date": "2026-09-27T06:00:00Z",
            "state": "failed",
            "failed_task": "gate_4_post_gold_check",
        },
        "related_alerts": [
            "DIM_CARD refresh task failed at 06:20 UTC — account-management "
            "pipeline timeout",
            "15 booking records reference cards not yet in DIM_CARD",
        ],
        "timeline": [
            {"ts": "2026-09-27T06:00:00Z", "event": "DAG run started"},
            {"ts": "2026-09-27T06:10:00Z", "event": "Bronze ingestion complete"},
            {"ts": "2026-09-27T06:20:00Z", "event": "DIM_CARD refresh FAILED "
                                                      "(account-mgmt pipeline timeout)"},
            {"ts": "2026-09-27T06:22:00Z", "event": "Silver MERGE INTO complete"},
            {"ts": "2026-09-27T06:35:00Z", "event": "Gold job complete "
                                                      "(15 rows with NULL card_sk)"},
            {"ts": "2026-09-27T06:50:00Z", "event": "Snowflake sync complete "
                                                      "(15087 of 15102 rows)"},
            {"ts": "2026-09-27T07:10:00Z", "event": "Gate 4 FAILED"},
        ],
    },
}

_SIMULATED_DATA_GAPS = {
    "2026-09-28": {
        "booking": {
            "entity": "booking",
            "gaps": [
                {
                    "layer": "silver",
                    "table": "silver.booking_detail",
                    "partition": "2026-09-28",
                    "expected_rows": 14823,
                    "actual_rows": 14712,
                    "missing_rows": 111,
                    "gap_type": "row_count_mismatch",
                    "detail": "111 rows lost between Bronze and Silver. "
                              "Silver MERGE INTO deduplicated on booking_id "
                              "but 111 booking_ids appeared 2-3 times in "
                              "Bronze (upstream re-delivery).",
                },
                {
                    "layer": "gold",
                    "table": "gold.fact_travel_tag",
                    "partition": "2026-09-28",
                    "expected_rows": 14712,
                    "actual_rows": 0,
                    "missing_rows": 14712,
                    "gap_type": "partition_missing",
                    "detail": "Gold partition 2026-09-28 was never created — "
                              "Gate 3 blocked the Gold job.",
                },
            ],
            "watermarks": {
                "bronze.booking_raw": {
                    "last_snapshot_id": 5765432198,
                    "status": "current",
                },
                "silver.booking_detail": {
                    "last_snapshot_id": 5765432190,
                    "status": "STALE — behind Bronze by 1 snapshot",
                },
                "gold.fact_travel_tag": {
                    "last_snapshot_id": None,
                    "status": "NOT_PRODUCED — Gate 3 blocked",
                },
            },
        },
    },
    "2026-09-27": {
        "booking": {
            "entity": "booking",
            "gaps": [
                {
                    "layer": "gold",
                    "table": "gold.fact_travel_tag",
                    "partition": "2026-09-27",
                    "expected_rows": 15102,
                    "actual_rows": 15087,
                    "missing_rows": 15,
                    "gap_type": "fk_lookup_failure",
                    "detail": "15 rows have NULL card_sk because DIM_CARD "
                              "refresh failed. These cards exist in Silver "
                              "but are missing from the dimension table.",
                },
            ],
            "watermarks": {
                "bronze.booking_raw": {
                    "last_snapshot_id": 5765432190,
                    "status": "current",
                },
                "silver.booking_detail": {
                    "last_snapshot_id": 5765432190,
                    "status": "current",
                },
                "gold.fact_travel_tag": {
                    "last_snapshot_id": 8891234567,
                    "status": "current — but 15 rows have NULL FKs",
                },
            },
        },
    },
}

_SIMULATED_PIPELINE_LOCKS = {
    # No locks currently held — pipeline is idle
    "active_locks": [],
    "lock_history": [
        {
            "lock_id": "lock-20260928-daily",
            "entity": "booking",
            "layers": ["bronze", "silver", "gold"],
            "acquired_by": "ttag_daily_dag",
            "acquired_at": "2026-09-28T05:30:00Z",
            "released_at": "2026-09-28T06:45:00Z",
            "reason": "Daily pipeline run",
        },
        {
            "lock_id": "lock-20260927-daily",
            "entity": "booking",
            "layers": ["bronze", "silver", "gold"],
            "acquired_by": "ttag_daily_dag",
            "acquired_at": "2026-09-27T06:00:00Z",
            "released_at": "2026-09-27T07:10:00Z",
            "reason": "Daily pipeline run",
        },
    ],
}

_SIMULATED_BACKFILL_HISTORY = {
    "booking": [
        {
            "backfill_id": "bf-20260920-001",
            "entity": "booking",
            "layers": ["silver", "gold"],
            "partitions": ["2026-09-19"],
            "triggered_by": "manual — on-call engineer",
            "started_at": "2026-09-20T10:30:00Z",
            "completed_at": "2026-09-20T11:15:00Z",
            "status": "success",
            "duration_minutes": 45,
            "rows_processed": 13450,
            "notes": "Silver MERGE INTO timed out during daily run. "
                     "Backfill replayed Bronze snapshot 5765432101 "
                     "through Silver and Gold successfully.",
        },
    ],
    "benefit": [
        {
            "backfill_id": "bf-20260915-001",
            "entity": "benefit",
            "layers": ["bronze", "silver"],
            "partitions": ["2026-09-14", "2026-09-15"],
            "triggered_by": "manual — on-call engineer",
            "started_at": "2026-09-15T14:00:00Z",
            "completed_at": "2026-09-15T15:30:00Z",
            "status": "success",
            "duration_minutes": 90,
            "rows_processed": 28100,
            "notes": "Kafka consumer group offset reset after broker "
                     "migration. Two days of benefit data re-ingested "
                     "from Kafka topic retention.",
        },
    ],
}

_SIMULATED_SOURCE_READINESS = {
    "2026-09-28": {
        "booking": {
            "entity": "booking",
            "source_type": "iceberg_snapshot",
            "available_snapshots": [
                {
                    "snapshot_id": 5765432198,
                    "timestamp": "2026-09-28T06:12:44Z",
                    "parent_id": 5765432190,
                    "operation": "append",
                    "rows": 14823,
                    "status": "available",
                    "expires_at": "2026-10-28T06:12:44Z",
                    "note": "Latest Bronze snapshot for 2026-09-28. "
                            "Contains the full day's booking data "
                            "including the 111 re-delivered records.",
                },
                {
                    "snapshot_id": 5765432190,
                    "timestamp": "2026-09-27T06:10:22Z",
                    "parent_id": 5765432181,
                    "operation": "append",
                    "rows": 15102,
                    "status": "available",
                    "expires_at": "2026-10-27T06:10:22Z",
                    "note": "Bronze snapshot for 2026-09-27. Already "
                            "processed through Silver and Gold.",
                },
            ],
            "kafka_offsets": {
                "topic": "ttag.booking.raw",
                "earliest_available": "2026-09-21T00:00:00Z",
                "latest_offset": 298451,
                "retention_days": 7,
                "note": "Kafka topic retention covers the last 7 days. "
                        "Data older than 2026-09-21 is no longer available "
                        "via Kafka replay — use Iceberg snapshots instead.",
            },
            "ready_for_backfill": True,
            "recommendation": "Use Bronze Iceberg snapshot 5765432198 as "
                              "the source for Silver backfill. The snapshot "
                              "contains all 14823 records for 2026-09-28. "
                              "Silver MERGE INTO will handle deduplication "
                              "of the 111 re-delivered records.",
        },
    },
    "2026-09-27": {
        "booking": {
            "entity": "booking",
            "source_type": "dimension_refresh",
            "available_snapshots": [
                {
                    "snapshot_id": 5765432190,
                    "timestamp": "2026-09-27T06:10:22Z",
                    "parent_id": 5765432181,
                    "operation": "append",
                    "rows": 15102,
                    "status": "available",
                    "expires_at": "2026-10-27T06:10:22Z",
                    "note": "Bronze snapshot for 2026-09-27. Silver is "
                            "current. Issue is in Gold (missing DIM_CARD "
                            "entries).",
                },
            ],
            "dim_card_status": {
                "missing_cards": ["CARD-88712", "CARD-88713", "CARD-91004"],
                "account_mgmt_pipeline": "recovered — ran successfully at "
                                          "2026-09-27T09:30:00Z",
                "dim_card_now_current": True,
                "note": "The 15 missing cards are now in DIM_CARD (the "
                        "account-management pipeline recovered at 09:30 "
                        "UTC). A Gold re-run will resolve the NULL FKs.",
            },
            "ready_for_backfill": True,
            "recommendation": "Re-run the Gold job only — Silver data is "
                              "correct. The DIM_CARD dimension is now "
                              "current (account-mgmt pipeline recovered). "
                              "Gold re-run will resolve the 15 NULL card_sk "
                              "rows.",
        },
    },
}


# ---------------------------------------------------------------------------
# Execution state (module-level — tracks side effects within an invocation)
# ---------------------------------------------------------------------------

# Pipeline lock registry
_ACTIVE_LOCKS: dict[str, dict] = {}

# Execution audit trail
_EXECUTION_AUDIT: list[dict] = []

# Maximum backfill steps per invocation (safety cap)
_MAX_STEPS_PER_INVOCATION = 20


# ---------------------------------------------------------------------------
# Investigation tools (read-only — used in Phase 1)
# ---------------------------------------------------------------------------

@tool
def get_incident_context(run_date: str) -> str:
    """Get the full context of the pipeline incident for a given run date.

    Use this FIRST when starting a backfill investigation. It returns
    everything you need to understand what happened:
      - Which gate failed and why
      - Which entity/layers are affected
      - The DAG run details (which task failed)
      - Related alerts and timeline of events

    The incident context is your starting point — it tells you the SCOPE
    of the problem, which guides your data gap assessment and backfill
    planning.

    Args:
        run_date: The pipeline run date of the incident (ISO format,
                  e.g. "2026-09-28").

    Returns:
        JSON with incident details: ID, trigger, affected entity/layers,
        gate failure details, DAG run info, related alerts, and timeline.
        Returns "not found" if no incident exists for this date.
    """
    incident = _SIMULATED_INCIDENTS.get(run_date)
    if not incident:
        return json.dumps({
            "status": "not_found",
            "message": f"No incident recorded for run_date={run_date}. "
                       f"Either the pipeline succeeded or the incident "
                       f"hasn't been registered yet.",
        })
    return json.dumps(incident, indent=2)


@tool
def assess_data_gaps(run_date: str, entity: str) -> str:
    """Assess data gaps across pipeline layers for a specific entity and date.

    Use this AFTER get_incident_context to quantify the damage. It shows:
      - Which partitions are missing or incomplete in each layer
      - Expected vs actual row counts
      - Gap type (row_count_mismatch, partition_missing, fk_lookup_failure)
      - Current watermark status per layer

    This tells you EXACTLY what needs to be backfilled — which layers,
    which partitions, and how many rows are affected. Use this to build
    your backfill plan's affected_partitions and proposed_steps.

    Look for:
      - partition_missing: the layer never produced output for this date
        (e.g. Gate 3 blocked Gold) — needs full replay from upstream
      - row_count_mismatch: partial data — some rows were lost or
        deduplicated — needs targeted replay
      - fk_lookup_failure: data exists but FK lookups failed — may only
        need a dimension refresh + re-run, not a full replay

    Args:
        run_date: The pipeline run date to assess.
        entity: The pipeline entity to check: "booking" or "benefit".

    Returns:
        JSON with per-layer gap analysis, watermark status, and detail
        messages explaining each gap.
    """
    day_data = _SIMULATED_DATA_GAPS.get(run_date, {})
    entity_data = day_data.get(entity)
    if not entity_data:
        return json.dumps({
            "run_date": run_date,
            "entity": entity,
            "status": "no_gaps_found",
            "message": f"No data gaps detected for {entity} on {run_date}. "
                       f"Either no incident affected this entity or the "
                       f"data is consistent across all layers.",
        })
    return json.dumps(entity_data, indent=2)


@tool
def check_pipeline_locks(entity: str) -> str:
    """Check whether any pipeline locks are currently held for an entity.

    Use this BEFORE including "acquire lock" in your backfill plan.
    If a lock is already held, the backfill cannot proceed — you'll
    need to wait or coordinate with whoever holds the lock.

    The pipeline lock system prevents concurrent writes to the same
    tables. Only one process (daily DAG run, backfill, or manual job)
    can hold a lock on an entity's layers at a time.

    Returns both active locks and recent lock history, so you can see
    if the daily DAG is currently running or if a previous backfill
    recently completed.

    Args:
        entity: The pipeline entity to check locks for: "booking" or
                "benefit".

    Returns:
        JSON with active locks (if any) and recent lock history.
        Empty active_locks means the pipeline segment is available.
    """
    # Filter locks by entity
    active = [
        lock for lock in _SIMULATED_PIPELINE_LOCKS["active_locks"]
        if lock["entity"] == entity
    ]
    history = [
        lock for lock in _SIMULATED_PIPELINE_LOCKS["lock_history"]
        if lock["entity"] == entity
    ]

    result = {
        "entity": entity,
        "active_locks": active,
        "lock_count": len(active),
        "available": len(active) == 0,
        "recent_history": history[-3:],  # Last 3 locks
    }

    if active:
        result["message"] = (
            f"Pipeline lock is HELD for {entity}. Cannot proceed with "
            f"backfill until the lock is released. Check who holds it "
            f"and coordinate."
        )
    else:
        result["message"] = (
            f"No active locks for {entity}. Pipeline segment is "
            f"available for backfill operations."
        )

    return json.dumps(result, indent=2)


@tool
def get_backfill_history(entity: str) -> str:
    """Get recent backfill history for a pipeline entity.

    Use this to understand past backfill patterns and durations. Useful for:
      - Estimating how long the proposed backfill will take
      - Checking if this same data was already backfilled recently
        (avoid duplicate work)
      - Understanding what backfill approach worked before for similar
        incidents

    If a recent backfill covered the same partitions you're planning to
    backfill, flag this — it might indicate a recurring issue that needs
    a deeper fix, not just another backfill.

    Args:
        entity: The pipeline entity: "booking" or "benefit".

    Returns:
        JSON array of recent backfill records with timing, scope, and
        outcome details. Empty array if no backfill history exists.
    """
    history = _SIMULATED_BACKFILL_HISTORY.get(entity, [])
    if not history:
        return json.dumps({
            "entity": entity,
            "backfills": [],
            "message": f"No backfill history found for {entity}. "
                       f"This would be the first backfill for this entity.",
        })

    return json.dumps({
        "entity": entity,
        "backfill_count": len(history),
        "backfills": history,
    }, indent=2)


@tool
def validate_source_readiness(run_date: str, entity: str) -> str:
    """Validate that source data is available for a backfill operation.

    Use this BEFORE finalizing your backfill plan. It checks whether the
    data you'd need to replay actually exists and is accessible:
      - Iceberg snapshots: are the relevant snapshots still available?
        (Snapshots can be expired by retention policies)
      - Kafka offsets: is the data still within the topic retention window?
        (Default 7 days — older data is gone)
      - Dimension tables: for FK-related issues, are the dimensions now
        current? (e.g. DIM_CARD refresh that failed — has it recovered?)

    The result includes a ready_for_backfill flag and a recommendation
    for which source to use. Include this recommendation in your
    backfill plan's proposed_steps.

    If ready_for_backfill is False, DO NOT plan a backfill — explain
    why the source data isn't available and what needs to happen first.

    Args:
        run_date: The pipeline run date to validate source data for.
        entity: The pipeline entity: "booking" or "benefit".

    Returns:
        JSON with source availability details, snapshot/offset status,
        readiness flag, and recommendation for backfill approach.
    """
    day_data = _SIMULATED_SOURCE_READINESS.get(run_date, {})
    entity_data = day_data.get(entity)
    if not entity_data:
        return json.dumps({
            "run_date": run_date,
            "entity": entity,
            "ready_for_backfill": False,
            "message": f"No source readiness data available for {entity} "
                       f"on {run_date}. Cannot validate whether backfill "
                       f"source data exists. Check manually.",
        })
    return json.dumps(entity_data, indent=2)


# ---------------------------------------------------------------------------
# Execution tools (side effects — used in Phase 2, after human approval)
# ---------------------------------------------------------------------------

@tool
def acquire_pipeline_lock(entity: str, layers: list[str], reason: str) -> str:
    """Acquire an exclusive pipeline lock before executing backfill steps.

    *** THIS IS A SIDE-EFFECT TOOL — it changes pipeline state. ***

    You MUST acquire a lock before executing any backfill step. The lock
    prevents concurrent writes from corrupting data — if the daily DAG
    starts while a backfill is running, it would overwrite or conflict
    with the backfill output.

    Only call this AFTER the human has approved the backfill plan.

    Safety features:
      - Idempotency: re-acquiring a lock you already hold is a no-op
      - Timeout: locks auto-expire after 2 hours (prevents deadlocks)
      - Ownership: the lock tracks who acquired it and why

    IMPORTANT: Always release the lock when done (even if steps fail).
    An unreleased lock blocks the daily pipeline until it times out.

    Args:
        entity: The pipeline entity to lock: "booking" or "benefit".
        layers: Which layers to lock (e.g. ["silver", "gold"]).
        reason: Why the lock is needed — goes into the audit log.

    Returns:
        JSON with lock status: "acquired", "already_held" (idempotent),
        or "blocked" (another process holds the lock).
    """
    lock_key = f"{entity}__{'_'.join(sorted(layers))}"
    timestamp = datetime.now(timezone.utc).isoformat()

    # Idempotency — if we already hold this lock, return success
    if lock_key in _ACTIVE_LOCKS:
        existing = _ACTIVE_LOCKS[lock_key]
        if existing["acquired_by"] == "argus_backfill_agent":
            return json.dumps({
                "lock_key": lock_key,
                "status": "already_held",
                "message": f"Lock already held by this agent. "
                           f"No action needed (idempotent).",
                "lock": existing,
            }, indent=2)
        else:
            return json.dumps({
                "lock_key": lock_key,
                "status": "blocked",
                "message": f"Lock is held by '{existing['acquired_by']}' "
                           f"since {existing['acquired_at']}. Cannot "
                           f"acquire until released.",
                "lock": existing,
            }, indent=2)

    # Acquire the lock
    lock = {
        "lock_id": f"lock-backfill-{entity}-{timestamp[:10]}",
        "entity": entity,
        "layers": layers,
        "acquired_by": "argus_backfill_agent",
        "acquired_at": timestamp,
        "timeout_at": "2h from acquisition (auto-release)",
        "reason": reason,
    }
    _ACTIVE_LOCKS[lock_key] = lock

    audit_entry = {
        "action": "lock_acquired",
        "lock_key": lock_key,
        "entity": entity,
        "layers": layers,
        "reason": reason,
        "timestamp": timestamp,
    }
    _EXECUTION_AUDIT.append(audit_entry)

    return json.dumps({
        "lock_key": lock_key,
        "status": "acquired",
        "message": f"Pipeline lock acquired for {entity} "
                   f"layers {layers}. You may now execute "
                   f"backfill steps.",
        "lock": lock,
        "audit": audit_entry,
    }, indent=2)


@tool
def execute_backfill_step(
    step_order: int,
    entity: str,
    layer: str,
    partition: str,
    source_snapshot_id: int,
    watermark_key: str,
    description: str,
) -> str:
    """Execute a single backfill step (replay data for one partition/layer).

    *** THIS IS A SIDE-EFFECT TOOL — it changes pipeline state. ***

    Only call this AFTER:
      1. The human has approved the backfill plan
      2. You have acquired the pipeline lock (acquire_pipeline_lock)

    Each call replays one partition from a source snapshot through the
    target layer. Execute steps in order (step_order 1, then 2, etc.)
    — upstream layers must be backfilled before downstream ones.

    Safety features:
      - Lock check: verifies the pipeline lock is held before executing
      - Step limit: maximum {max_steps} steps per invocation
      - Audit trail: every step is logged with status and row counts

    In production, this would:
      - For Silver: run the MERGE INTO job against the source snapshot
      - For Gold: run the Gold transformation job with FK lookups
      - Update the watermark control table after completion

    Args:
        step_order: The step number in the backfill plan (1-based).
        entity: The pipeline entity: "booking" or "benefit".
        layer: Target layer to backfill: "silver" or "gold".
        partition: The partition date to backfill (e.g. "2026-09-28").
        source_snapshot_id: The Iceberg snapshot ID to replay from.
        watermark_key: The watermark key to update on completion
                       (e.g. "booking__backfill").
        description: What this step does — goes into the audit log.

    Returns:
        JSON with execution result: status, rows processed, duration,
        and audit entry.
    """
    timestamp = datetime.now(timezone.utc).isoformat()

    # Safety: check step limit
    step_count = sum(
        1 for entry in _EXECUTION_AUDIT if entry["action"] == "backfill_step"
    )
    if step_count >= _MAX_STEPS_PER_INVOCATION:
        return json.dumps({
            "step_order": step_order,
            "status": "rejected",
            "message": f"Step limit reached ({_MAX_STEPS_PER_INVOCATION} "
                       f"per invocation). Cannot execute more steps. "
                       f"This is a safety cap.",
        })

    # Safety: check that pipeline lock is held
    lock_key = None
    for key, lock in _ACTIVE_LOCKS.items():
        if (lock["entity"] == entity
                and lock["acquired_by"] == "argus_backfill_agent"):
            lock_key = key
            break

    if not lock_key:
        return json.dumps({
            "step_order": step_order,
            "status": "rejected",
            "message": f"No pipeline lock held for {entity}. You must "
                       f"acquire a lock with acquire_pipeline_lock before "
                       f"executing backfill steps.",
        })

    # Simulate execution (in production: run Spark job)
    # Row counts from simulated data to make it realistic
    simulated_rows = {
        ("booking", "silver", "2026-09-28"): 14823,
        ("booking", "gold", "2026-09-28"): 14712,
        ("booking", "gold", "2026-09-27"): 15102,
    }
    rows_processed = simulated_rows.get(
        (entity, layer, partition), 5000  # default fallback
    )

    # Simulate execution duration
    simulated_duration = {
        "silver": 12,  # Silver MERGE INTO typically ~12 minutes
        "gold": 18,    # Gold transformation typically ~18 minutes
    }
    duration_minutes = simulated_duration.get(layer, 15)

    audit_entry = {
        "action": "backfill_step",
        "step_order": step_order,
        "entity": entity,
        "layer": layer,
        "partition": partition,
        "source_snapshot_id": source_snapshot_id,
        "watermark_key": watermark_key,
        "description": description,
        "rows_processed": rows_processed,
        "duration_minutes": duration_minutes,
        "status": "success",
        "timestamp": timestamp,
    }
    _EXECUTION_AUDIT.append(audit_entry)

    return json.dumps({
        "step_order": step_order,
        "status": "success",
        "message": f"Step {step_order} complete: {description}",
        "result": {
            "entity": entity,
            "layer": layer,
            "partition": partition,
            "rows_processed": rows_processed,
            "duration_minutes": duration_minutes,
            "watermark_updated": watermark_key,
            "source_snapshot_id": source_snapshot_id,
        },
        "audit": audit_entry,
    }, indent=2)


@tool
def release_pipeline_lock(entity: str, layers: list[str]) -> str:
    """Release the pipeline lock after backfill execution is complete.

    *** THIS IS A SIDE-EFFECT TOOL — it changes pipeline state. ***

    ALWAYS call this after backfill execution, whether all steps
    succeeded or some failed. An unreleased lock blocks the daily
    pipeline from running until the lock times out (2 hours).

    If you encounter errors during execution and need to abort,
    release the lock BEFORE reporting the failure.

    Args:
        entity: The pipeline entity to unlock: "booking" or "benefit".
        layers: Which layers to unlock (must match what was locked).

    Returns:
        JSON with release status and the full execution audit trail.
    """
    lock_key = f"{entity}__{'_'.join(sorted(layers))}"
    timestamp = datetime.now(timezone.utc).isoformat()

    if lock_key not in _ACTIVE_LOCKS:
        return json.dumps({
            "lock_key": lock_key,
            "status": "not_held",
            "message": f"No lock found for {lock_key}. Either it was "
                       f"already released or was never acquired.",
        })

    released_lock = _ACTIVE_LOCKS.pop(lock_key)

    audit_entry = {
        "action": "lock_released",
        "lock_key": lock_key,
        "entity": entity,
        "layers": layers,
        "originally_acquired_at": released_lock["acquired_at"],
        "released_at": timestamp,
    }
    _EXECUTION_AUDIT.append(audit_entry)

    # Collect the full execution audit for this entity
    entity_audit = [
        entry for entry in _EXECUTION_AUDIT
        if entry.get("entity") == entity or entry.get("lock_key", "").startswith(f"{entity}__")
    ]

    return json.dumps({
        "lock_key": lock_key,
        "status": "released",
        "message": f"Pipeline lock released for {entity}. Daily pipeline "
                   f"can now run normally.",
        "released_lock": released_lock,
        "execution_summary": {
            "total_steps_executed": sum(
                1 for e in entity_audit if e["action"] == "backfill_step"
            ),
            "total_rows_processed": sum(
                e.get("rows_processed", 0) for e in entity_audit
                if e["action"] == "backfill_step"
            ),
            "all_steps_succeeded": all(
                e.get("status") == "success" for e in entity_audit
                if e["action"] == "backfill_step"
            ),
        },
        "audit_trail": entity_audit,
    }, indent=2)


# ---------------------------------------------------------------------------
# Tool registries — grouped by phase for clarity
# ---------------------------------------------------------------------------

BACKFILL_INVESTIGATION_TOOLS = [
    get_incident_context,
    assess_data_gaps,
    check_pipeline_locks,
    get_backfill_history,
    validate_source_readiness,
]
"""Read-only tools for the investigation phase (Phase 1 of the agent)."""

BACKFILL_EXECUTION_TOOLS = [
    acquire_pipeline_lock,
    execute_backfill_step,
    release_pipeline_lock,
]
"""Side-effect tools for the execution phase (Phase 2, after human approval)."""

BACKFILL_TOOLS = BACKFILL_INVESTIGATION_TOOLS + BACKFILL_EXECUTION_TOOLS
"""All tools available to the Backfill Planning agent."""
