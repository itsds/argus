"""
DLQ Triage tools for the Argus DLQ Triage & Auto-Remediation agent.

These tools query TTAG pipeline DLQ infrastructure to investigate and
classify dead-letter-queue records. The DLQ agent calls them in a ReAct
loop — reading records, cross-referencing schema changes, and (for
transient failures) requeuing records back to the source topic.

WHAT'S NEW IN PHASE 3 (compared to Phase 2 Recon tools):

  1. DUAL DLQ LANES — the TTAG pipeline has two DLQ mechanisms:
       - Kafka DLQ topic (Benefit lane): messages that failed Kafka
         consumer processing are dead-lettered to a DLQ topic. The
         original error is in the Kafka message headers.
       - bad_files Iceberg quarantine (Booking lane): files that fail
         ingestion (corrupt, schema mismatch, bad data) are moved to
         an Iceberg quarantine table instead of being loaded.
     The agent must handle both lanes with the same classification logic.

  2. SCHEMA CHANGELOG — the DLQ agent can cross-reference against
     schema_changelog (a control table that tracks schema evolution:
     when fields were added/renamed/retyped). If a DLQ record failed
     right after a schema change, that's strong evidence for a
     schema_mismatch classification.

  3. GUARDED SIDE EFFECT (requeue_message) — this is the first Argus
     tool that CHANGES STATE. The Recon agent was purely read-only.
     Design considerations for side-effect tools:
       - Explicit confirmation logging (audit trail)
       - Idempotency guard (won't requeue the same record twice)
       - Safety limits (max requeue count per invocation)
       - Clear docstring telling the LLM when NOT to use it
     In production, this would call Kafka producer API to republish
     the message to the source topic (with a retry header).

TOOL DESIGN NOTES:
  - Each docstring is prompt engineering: it tells the LLM what the
    tool does, when to use it, and what the output means.
  - Return types are always str (LangChain convention).
  - Simulated data is structured to exercise the agent's classification
    logic across multiple failure modes.

DEV MODE:
  In dev, these tools return simulated data mimicking real pipeline DLQ
  records. In production, they'd query the Kafka DLQ consumer, the
  bad_files Iceberg table, and the schema_changelog control table.
"""

import json
from datetime import datetime, timezone

from langchain_core.tools import tool


# ---------------------------------------------------------------------------
# Simulated data store
# ---------------------------------------------------------------------------
# Each scenario represents a realistic set of DLQ records that the agent
# must classify. The scenarios are selected based on the source_lane
# parameter (kafka_dlq or bad_files).

_SIMULATED_KAFKA_DLQ_RECORDS = {
    "2026-09-30": [
        {
            "record_id": "dlq-kafka-001",
            "source_lane": "kafka_dlq",
            "source_topic": "ttag.benefit.raw",
            "partition": 3,
            "offset": 148823,
            "timestamp": "2026-09-30T06:14:22Z",
            "error_class": "org.apache.kafka.common.errors.TimeoutException",
            "error_message": "Failed to send request to broker 2: "
                             "Request timed out after 30000ms",
            "retry_count": 0,
            "headers": {
                "x-original-topic": "ttag.benefit.raw",
                "x-error-timestamp": "2026-09-30T06:14:22Z",
                "x-consumer-group": "ttag-benefit-consumer-group",
                "x-exception-class": "TimeoutException",
            },
            "payload_preview": {
                "benefit_id": "BN-20260930-04412",
                "card_id": "CARD-71234",
                "benefit_type": "lounge_access",
                "event_ts": "2026-09-30T05:58:00Z",
            },
        },
        {
            "record_id": "dlq-kafka-002",
            "source_lane": "kafka_dlq",
            "source_topic": "ttag.benefit.raw",
            "partition": 1,
            "offset": 148901,
            "timestamp": "2026-09-30T06:15:01Z",
            "error_class": "org.apache.kafka.common.errors.TimeoutException",
            "error_message": "Failed to send request to broker 2: "
                             "Request timed out after 30000ms",
            "retry_count": 0,
            "headers": {
                "x-original-topic": "ttag.benefit.raw",
                "x-error-timestamp": "2026-09-30T06:15:01Z",
                "x-consumer-group": "ttag-benefit-consumer-group",
                "x-exception-class": "TimeoutException",
            },
            "payload_preview": {
                "benefit_id": "BN-20260930-04413",
                "card_id": "CARD-71235",
                "benefit_type": "travel_insurance",
                "event_ts": "2026-09-30T06:02:00Z",
            },
        },
        {
            "record_id": "dlq-kafka-003",
            "source_lane": "kafka_dlq",
            "source_topic": "ttag.benefit.raw",
            "partition": 2,
            "offset": 148955,
            "timestamp": "2026-09-30T06:22:15Z",
            "error_class": "io.confluent.kafka.serializers.subject.SchemaRegistryException",
            "error_message": "Schema ID 47 not found in registry; "
                             "expected schema ID 45 (benefit_value_v3). "
                             "Producer may be using a newer schema version.",
            "retry_count": 0,
            "headers": {
                "x-original-topic": "ttag.benefit.raw",
                "x-error-timestamp": "2026-09-30T06:22:15Z",
                "x-consumer-group": "ttag-benefit-consumer-group",
                "x-exception-class": "SchemaRegistryException",
                "x-schema-id": "47",
            },
            "payload_preview": {
                "benefit_id": "BN-20260930-05001",
                "card_id": "CARD-81002",
                "benefit_type": "priority_pass",
                "benefit_tier": "platinum",  # new field not in consumer schema
                "event_ts": "2026-09-30T06:19:00Z",
            },
        },
        {
            "record_id": "dlq-kafka-004",
            "source_lane": "kafka_dlq",
            "source_topic": "ttag.benefit.raw",
            "partition": 2,
            "offset": 148960,
            "timestamp": "2026-09-30T06:22:44Z",
            "error_class": "io.confluent.kafka.serializers.subject.SchemaRegistryException",
            "error_message": "Schema ID 47 not found in registry; "
                             "expected schema ID 45 (benefit_value_v3). "
                             "Producer may be using a newer schema version.",
            "retry_count": 0,
            "headers": {
                "x-original-topic": "ttag.benefit.raw",
                "x-error-timestamp": "2026-09-30T06:22:44Z",
                "x-consumer-group": "ttag-benefit-consumer-group",
                "x-exception-class": "SchemaRegistryException",
                "x-schema-id": "47",
            },
            "payload_preview": {
                "benefit_id": "BN-20260930-05002",
                "card_id": "CARD-81003",
                "benefit_type": "concierge",
                "benefit_tier": "gold",  # new field
                "event_ts": "2026-09-30T06:20:00Z",
            },
        },
        {
            "record_id": "dlq-kafka-005",
            "source_lane": "kafka_dlq",
            "source_topic": "ttag.benefit.raw",
            "partition": 0,
            "offset": 149010,
            "timestamp": "2026-09-30T06:31:08Z",
            "error_class": "java.lang.NullPointerException",
            "error_message": "Cannot invoke method on null object: "
                             "record.getBenefitType() returned null. "
                             "benefit_type is a required field.",
            "retry_count": 0,
            "headers": {
                "x-original-topic": "ttag.benefit.raw",
                "x-error-timestamp": "2026-09-30T06:31:08Z",
                "x-consumer-group": "ttag-benefit-consumer-group",
                "x-exception-class": "NullPointerException",
            },
            "payload_preview": {
                "benefit_id": "BN-20260930-06100",
                "card_id": "CARD-92001",
                "benefit_type": None,  # null — data quality issue
                "event_ts": "2026-09-30T06:28:00Z",
            },
        },
        {
            "record_id": "dlq-kafka-006",
            "source_lane": "kafka_dlq",
            "source_topic": "ttag.benefit.raw",
            "partition": 1,
            "offset": 149055,
            "timestamp": "2026-09-30T06:35:22Z",
            "error_class": "com.ttag.pipeline.UnknownProcessingException",
            "error_message": "Unexpected error during benefit enrichment: "
                             "ExternalServiceUnavailable — geocoding API "
                             "returned 503 for merchant_id=MCH-44201",
            "retry_count": 0,
            "headers": {
                "x-original-topic": "ttag.benefit.raw",
                "x-error-timestamp": "2026-09-30T06:35:22Z",
                "x-consumer-group": "ttag-benefit-consumer-group",
                "x-exception-class": "UnknownProcessingException",
            },
            "payload_preview": {
                "benefit_id": "BN-20260930-07200",
                "card_id": "CARD-55123",
                "benefit_type": "lounge_access",
                "event_ts": "2026-09-30T06:33:00Z",
            },
        },
    ],
}

_SIMULATED_BAD_FILES_RECORDS = {
    "2026-09-29": [
        {
            "record_id": "dlq-badfile-001",
            "source_lane": "bad_files",
            "file_path": "s3://ttag-raw/booking/2026-09-29/batch_0014.parquet",
            "file_size_bytes": 2_145_678,
            "quarantine_ts": "2026-09-29T07:12:33Z",
            "error_class": "org.apache.spark.sql.AnalysisException",
            "error_message": "Cannot read Parquet file: unexpected end of "
                             "stream at byte 2145000. File may be corrupted "
                             "or truncated during upload.",
            "retry_count": 0,
            "row_count_estimate": 0,
            "metadata": {
                "upload_source": "sftp_booking_feed",
                "upload_ts": "2026-09-29T06:58:12Z",
                "md5_expected": "a1b2c3d4e5f6",
                "md5_actual": "x9y8z7w6v5u4",
            },
        },
        {
            "record_id": "dlq-badfile-002",
            "source_lane": "bad_files",
            "file_path": "s3://ttag-raw/booking/2026-09-29/batch_0015.parquet",
            "file_size_bytes": 3_890_112,
            "quarantine_ts": "2026-09-29T07:14:10Z",
            "error_class": "org.apache.spark.sql.AnalysisException",
            "error_message": "Column 'transaction_amount' has type DOUBLE in "
                             "file but schema expects DECIMAL(18,2). This "
                             "changed in the source feed on 2026-09-28.",
            "retry_count": 0,
            "row_count_estimate": 4200,
            "metadata": {
                "upload_source": "sftp_booking_feed",
                "upload_ts": "2026-09-29T07:01:44Z",
                "schema_version": "booking_v5",
            },
        },
        {
            "record_id": "dlq-badfile-003",
            "source_lane": "bad_files",
            "file_path": "s3://ttag-raw/booking/2026-09-29/batch_0016.parquet",
            "file_size_bytes": 4_102_400,
            "quarantine_ts": "2026-09-29T07:18:55Z",
            "error_class": "com.ttag.pipeline.DataQualityException",
            "error_message": "12 rows failed data quality checks: "
                             "booking_date is in the future (2027-xx-xx), "
                             "transaction_amount is negative (-$450.00), "
                             "card_id references non-existent card.",
            "retry_count": 0,
            "row_count_estimate": 4150,
            "metadata": {
                "upload_source": "sftp_booking_feed",
                "upload_ts": "2026-09-29T07:05:22Z",
                "schema_version": "booking_v4",
                "dq_failures": [
                    {"rule": "booking_date_not_future", "failures": 5},
                    {"rule": "amount_non_negative", "failures": 4},
                    {"rule": "card_id_exists", "failures": 3},
                ],
            },
        },
    ],
}

_SIMULATED_SCHEMA_CHANGELOG = {
    "benefit": [
        {
            "version": "benefit_value_v3 (schema_id=45)",
            "effective_date": "2026-08-15",
            "change_type": "field_added",
            "description": "Added 'lounge_network' optional field for "
                           "lounge_access benefit tracking",
            "status": "active",
        },
        {
            "version": "benefit_value_v4 (schema_id=47)",
            "effective_date": "2026-09-30",
            "change_type": "field_added",
            "description": "Added 'benefit_tier' required field (platinum/"
                           "gold/silver) — producer updated but consumer "
                           "schema registry not yet updated",
            "status": "pending_consumer_update",
            "note": "Producer deployed schema v4 at 06:18 UTC. Consumer "
                    "group ttag-benefit-consumer-group still expects v3 "
                    "(schema_id=45). Records with schema_id=47 will fail "
                    "deserialization until consumer is updated.",
        },
    ],
    "booking": [
        {
            "version": "booking_v4",
            "effective_date": "2026-06-01",
            "change_type": "stable",
            "description": "Current production schema for booking data",
            "status": "active",
        },
        {
            "version": "booking_v5",
            "effective_date": "2026-09-28",
            "change_type": "column_type_change",
            "description": "transaction_amount changed from DECIMAL(18,2) "
                           "to DOUBLE in source feed. Parquet files with "
                           "booking_v5 schema will fail against current "
                           "Iceberg table schema (expects DECIMAL).",
            "status": "pending_table_migration",
            "note": "Source system (booking feed) deployed the type change "
                    "on 2026-09-28. Iceberg table schema migration is "
                    "pending — needs ALTER TABLE to update column type.",
        },
    ],
}

# Track which records have been requeued (idempotency guard)
_REQUEUED_RECORDS: set[str] = set()

# Maximum records that can be requeued in a single agent invocation
_MAX_REQUEUE_PER_INVOCATION = 10


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

@tool
def read_dlq_records(run_date: str, source_lane: str) -> str:
    """Read dead-letter-queue records from a specific DLQ lane for a given date.

    Use this FIRST when investigating DLQ records. It returns all records
    that landed in the DLQ on the given date, along with error details
    and payload previews.

    The TTAG pipeline has TWO DLQ lanes — you may need to check both:

      - "kafka_dlq": Records from the Kafka DLQ topic. These are Benefit
        lane messages that failed during Kafka consumer processing. The
        error class and message in the headers tell you what went wrong.
        Common causes: TimeoutException (transient), SchemaRegistryException
        (schema mismatch), NullPointerException (data quality).

      - "bad_files": Records from the Iceberg bad_files quarantine table.
        These are Booking lane files that failed during Spark ingestion.
        Common causes: corrupt/truncated files (transient — re-ingest),
        column type mismatches (schema change), data quality check failures.

    After reading, classify each record using your judgment:
      - TRANSIENT: TimeoutException, ConnectionReset, corrupt file with
        checksum mismatch — these are safe to requeue/re-ingest.
      - SCHEMA_MISMATCH: SchemaRegistryException, column type changes —
        cross-reference with query_schema_changelog to confirm.
      - DATA_QUALITY: null required fields, invalid values, future dates —
        these need quarantine and human review.
      - UNKNOWN: anything that doesn't fit above — escalate with context.

    Args:
        run_date: The date to check for DLQ records (ISO format, e.g. "2026-09-30").
        source_lane: Which DLQ lane to read: "kafka_dlq" or "bad_files".

    Returns:
        JSON array of DLQ records with error details and payload previews.
        Empty array if no records found for the given date/lane.
    """
    if source_lane == "kafka_dlq":
        records = _SIMULATED_KAFKA_DLQ_RECORDS.get(run_date, [])
    elif source_lane == "bad_files":
        records = _SIMULATED_BAD_FILES_RECORDS.get(run_date, [])
    else:
        return json.dumps({
            "error": f"Unknown source_lane: '{source_lane}'. "
                     f"Use 'kafka_dlq' or 'bad_files'.",
        })

    if not records:
        return json.dumps({
            "source_lane": source_lane,
            "run_date": run_date,
            "record_count": 0,
            "records": [],
            "message": f"No DLQ records found for {source_lane} on {run_date}.",
        })

    return json.dumps({
        "source_lane": source_lane,
        "run_date": run_date,
        "record_count": len(records),
        "records": records,
    }, indent=2)


@tool
def query_schema_changelog(entity: str) -> str:
    """Query the schema changelog for a pipeline entity to check for recent changes.

    Use this when you see SchemaRegistryException or column type mismatch
    errors in DLQ records. The schema changelog tracks every schema evolution
    event — field additions, type changes, renames — with dates and status.

    Cross-reference the DLQ record's error timestamp against the changelog:
    if a schema change was deployed right before the errors started, that's
    strong evidence for a SCHEMA_MISMATCH classification.

    Key fields in the result:
      - effective_date: when the change was deployed
      - change_type: "field_added", "column_type_change", "field_renamed", etc.
      - status: "active" (both sides updated), "pending_consumer_update"
        (producer updated but consumer not yet), "pending_table_migration"
        (source changed but table schema not yet altered)

    A "pending_consumer_update" or "pending_table_migration" status confirms
    the schema mismatch — the fix is to update the consumer/table, not to
    requeue the records.

    Args:
        entity: The pipeline entity to check: "benefit" or "booking".

    Returns:
        JSON array of schema changelog entries, newest first.
        Empty array if no changelog exists for the entity.
    """
    changelog = _SIMULATED_SCHEMA_CHANGELOG.get(entity, [])
    if not changelog:
        return json.dumps({
            "entity": entity,
            "entries": [],
            "message": f"No schema changelog found for entity '{entity}'.",
        })

    return json.dumps({
        "entity": entity,
        "entry_count": len(changelog),
        "entries": changelog,
    }, indent=2)


@tool
def requeue_message(record_id: str, source_lane: str, reason: str) -> str:
    """Requeue a DLQ record back to its source topic/ingestion path for retry.

    *** THIS IS A SIDE-EFFECT TOOL — it changes pipeline state. ***

    ONLY use this for records classified as TRANSIENT with HIGH confidence:
      - TimeoutException, ConnectionReset, BrokerNotAvailable (Kafka DLQ)
      - Corrupt/truncated files where checksum mismatch suggests upload failure
        (bad_files — the file will be re-fetched from source)

    NEVER requeue records that are:
      - SCHEMA_MISMATCH — requeuing will just fail again with the same error
      - DATA_QUALITY — the data itself is bad, requeuing won't fix it
      - UNKNOWN — don't requeue what you don't understand

    Safety features:
      - Idempotency: calling this twice with the same record_id is a no-op
      - Limit: maximum 10 requeues per agent invocation (safety cap)
      - Audit: every requeue is logged with the reason and timestamp

    In production, this would:
      - For kafka_dlq: republish the message to the original topic with a
        'x-retry-count' header incremented
      - For bad_files: move the file from quarantine back to the ingestion
        landing zone and trigger a re-ingest job

    Args:
        record_id: The DLQ record ID to requeue (e.g. "dlq-kafka-001").
        source_lane: The DLQ lane: "kafka_dlq" or "bad_files".
        reason: Brief explanation of why this record is safe to requeue.
                This goes into the audit log.

    Returns:
        JSON with requeue status: "requeued", "already_requeued" (idempotent),
        or "rejected" (limit reached or invalid record).
    """
    # Idempotency guard — don't requeue the same record twice
    if record_id in _REQUEUED_RECORDS:
        return json.dumps({
            "record_id": record_id,
            "status": "already_requeued",
            "message": f"Record {record_id} was already requeued in this "
                       f"invocation. No action taken (idempotent).",
        })

    # Safety limit — prevent runaway requeuing
    if len(_REQUEUED_RECORDS) >= _MAX_REQUEUE_PER_INVOCATION:
        return json.dumps({
            "record_id": record_id,
            "status": "rejected",
            "message": f"Requeue limit reached ({_MAX_REQUEUE_PER_INVOCATION} "
                       f"per invocation). Cannot requeue more records. "
                       f"This is a safety cap — if more records need "
                       f"requeuing, run the agent again.",
        })

    # Validate record exists in simulated data
    record_found = False
    all_records = []
    for records in _SIMULATED_KAFKA_DLQ_RECORDS.values():
        all_records.extend(records)
    for records in _SIMULATED_BAD_FILES_RECORDS.values():
        all_records.extend(records)

    for rec in all_records:
        if rec["record_id"] == record_id:
            record_found = True
            break

    if not record_found:
        return json.dumps({
            "record_id": record_id,
            "status": "rejected",
            "message": f"Record {record_id} not found in DLQ. Cannot requeue.",
        })

    # Perform the requeue (simulated)
    _REQUEUED_RECORDS.add(record_id)

    timestamp = datetime.now(timezone.utc).isoformat()

    return json.dumps({
        "record_id": record_id,
        "source_lane": source_lane,
        "status": "requeued",
        "message": f"Record {record_id} requeued to source "
                   f"({'original topic' if source_lane == 'kafka_dlq' else 'ingestion landing zone'}). "
                   f"Reason: {reason}",
        "audit": {
            "action": "requeue",
            "record_id": record_id,
            "source_lane": source_lane,
            "reason": reason,
            "timestamp": timestamp,
            "requeue_count_this_invocation": len(_REQUEUED_RECORDS),
        },
    }, indent=2)


# ---------------------------------------------------------------------------
# Tool registry — easy import for the agent
# ---------------------------------------------------------------------------

DLQ_TOOLS = [
    read_dlq_records,
    query_schema_changelog,
    requeue_message,
]
"""All tools available to the DLQ Triage & Auto-Remediation agent."""
