"""
Reconciliation diagnostic tools for the Argus Recon agent.

These tools query TTAG pipeline infrastructure to investigate why a
Gate 3 or Gate 4 reconciliation check failed. The Recon agent calls
them in a ReAct loop — reasoning about results, choosing the next
tool, until it has enough evidence for a root-cause report.

TOOL DESIGN NOTES (what the LLM sees):
  - Each docstring is prompt engineering: it tells the LLM what the
    tool does, when to use it, and what the output means.
  - Type hints guide the LLM on what arguments to pass.
  - Return types are always str (LangChain convention for tool output
    that goes back into the message stream).

DEV MODE:
  In dev, these tools return simulated data that mimics real pipeline
  failures. In production, they'd query Iceberg (Hive Metastore),
  Snowflake, and the watermark control table via Spark SQL / JDBC.
"""

import json
from datetime import datetime, timezone

from langchain_core.tools import tool


# ---------------------------------------------------------------------------
# Simulated data store
# ---------------------------------------------------------------------------
# These simulate what a real query would return from TTAG infrastructure.
# Each scenario represents a realistic failure mode. The active scenario
# is selected based on the run_date parameter.

_SIMULATED_GATE_RESULTS = {
    "2026-09-28": {
        "gate": "gate_3",
        "status": "FAILED",
        "run_date": "2026-09-28",
        "check": "pre_gold_reconciliation",
        "details": {
            "booking_silver_count": 14823,
            "benefit_silver_count": 14190,
            "expected_gold_input": 14823,
            "message": "Gate 3 pre-Gold reconciliation failed: "
                       "Booking Silver row count (14823) will be used as "
                       "Gold input baseline. Proceeding to Gold blocked.",
        },
        "dag_id": "ttag_daily_dag",
        "task_id": "gate_3_pre_gold_recon",
        "execution_date": "2026-09-28T06:00:00Z",
    },
    "2026-09-27": {
        "gate": "gate_4",
        "status": "FAILED",
        "run_date": "2026-09-27",
        "check": "post_gold_snowflake_match",
        "details": {
            "gold_iceberg_count": 15102,
            "snowflake_fact_count": 15087,
            "delta": 15,
            "threshold_pct": 0.1,
            "actual_pct": 0.099,
            "message": "Gate 4 post-Gold check: Snowflake FACT_TRAVEL_TAG "
                       "count (15087) is 15 rows short of Gold Iceberg "
                       "(15102). Delta 0.099% is within threshold but "
                       "flagged for investigation.",
        },
        "dag_id": "ttag_daily_dag",
        "task_id": "gate_4_post_gold_check",
        "execution_date": "2026-09-27T06:00:00Z",
    },
}

_SIMULATED_ROW_COUNTS = {
    "2026-09-28": {
        "bronze.booking_raw": {
            "total": 15012,
            "partitions": {
                "2026-09-28": 14823,
                "2026-09-27": 189,  # late-arriving records
            },
        },
        "bronze.benefit_raw": {
            "total": 14205,
            "partitions": {
                "2026-09-28": 14190,
                "2026-09-27": 15,
            },
        },
        "silver.booking_detail": {
            "total": 14712,  # less than Bronze — something dropped
            "partitions": {
                "2026-09-28": 14534,
                "2026-09-27": 178,
            },
        },
        "silver.benefit_detail": {
            "total": 14190,
            "partitions": {
                "2026-09-28": 14190,
            },
        },
    },
    "2026-09-27": {
        "bronze.booking_raw": {
            "total": 15102,
            "partitions": {
                "2026-09-27": 15102,
            },
        },
        "silver.booking_detail": {
            "total": 15102,
            "partitions": {
                "2026-09-27": 15102,
            },
        },
        "gold.fact_travel_tag": {
            "total": 15087,
            "partitions": {
                "2026-09-27": 15087,
            },
        },
    },
}

_SIMULATED_DUPLICATE_KEYS = {
    "2026-09-28": {
        "silver.booking_detail": {
            "has_duplicates": True,
            "duplicate_count": 111,
            "sample_keys": [
                {"booking_id": "BK-20260928-00412", "count": 2},
                {"booking_id": "BK-20260928-00413", "count": 2},
                {"booking_id": "BK-20260928-00519", "count": 3},
                {"booking_id": "BK-20260928-07821", "count": 2},
                {"booking_id": "BK-20260928-09033", "count": 2},
            ],
            "note": "111 booking_ids appear more than once in "
                    "silver.booking_detail for partition 2026-09-28. "
                    "This explains the Bronze-to-Silver row count drop "
                    "(MERGE INTO deduplicates on natural key, but these "
                    "duplicates suggest upstream re-delivery).",
        },
        "silver.benefit_detail": {
            "has_duplicates": False,
            "duplicate_count": 0,
            "sample_keys": [],
            "note": "No duplicate benefit_ids in silver.benefit_detail.",
        },
    },
    "2026-09-27": {
        "silver.booking_detail": {
            "has_duplicates": False,
            "duplicate_count": 0,
            "sample_keys": [],
            "note": "No duplicate booking_ids in silver.booking_detail.",
        },
    },
}

_SIMULATED_FK_INTEGRITY = {
    "2026-09-27": {
        "fact_travel_tag → dim_card": {
            "null_fk_count": 15,
            "total_rows": 15087,
            "sample_nulls": [
                {"booking_id": "BK-20260927-03419", "card_sk": None,
                 "card_natural_key": "CARD-88712"},
                {"booking_id": "BK-20260927-03420", "card_sk": None,
                 "card_natural_key": "CARD-88713"},
                {"booking_id": "BK-20260927-11287", "card_sk": None,
                 "card_natural_key": "CARD-91004"},
            ],
            "note": "15 rows in FACT_TRAVEL_TAG have NULL card_sk. "
                    "These cards exist in Silver but are missing from "
                    "DIM_CARD — the dimension refresh task may have "
                    "failed or the account-management pipeline hasn't "
                    "delivered them yet.",
        },
        "fact_travel_tag → dim_merchant": {
            "null_fk_count": 0,
            "total_rows": 15087,
            "sample_nulls": [],
            "note": "All merchant FKs resolved — no orphans.",
        },
        "fact_travel_tag → dim_benefit": {
            "null_fk_count": 2341,
            "total_rows": 15087,
            "sample_nulls": [],
            "note": "2341 rows have NULL benefit_sk. This is expected — "
                    "benefit FK is nullable by design (benefit data may "
                    "arrive after the booking).",
        },
    },
    "2026-09-28": {
        "fact_travel_tag → dim_card": {
            "null_fk_count": 0,
            "total_rows": 0,
            "sample_nulls": [],
            "note": "Gold was not produced for 2026-09-28 (Gate 3 blocked). "
                    "No FK integrity check applicable.",
        },
    },
}

_SIMULATED_WATERMARKS = {
    "2026-09-28": {
        "bronze.booking_raw": {
            "table_name": "bronze.booking_raw",
            "last_snapshot_id": 5765432198,
            "last_run_ts": "2026-09-28T06:12:44Z",
            "status": "current",
        },
        "bronze.benefit_raw": {
            "table_name": "bronze.benefit_raw",
            "last_snapshot_id": 3312987654,
            "last_run_ts": "2026-09-28T06:14:02Z",
            "status": "current",
        },
        "silver.booking_detail": {
            "table_name": "silver.booking_detail",
            "last_snapshot_id": 5765432190,
            "last_run_ts": "2026-09-28T05:48:31Z",
            "status": "STALE — last_snapshot_id (5765432190) is behind "
                      "bronze.booking_raw snapshot (5765432198). Silver "
                      "job may not have completed for the latest Bronze "
                      "data.",
        },
        "silver.benefit_detail": {
            "table_name": "silver.benefit_detail",
            "last_snapshot_id": 3312987654,
            "last_run_ts": "2026-09-28T06:15:10Z",
            "status": "current",
        },
    },
    "2026-09-27": {
        "bronze.booking_raw": {
            "table_name": "bronze.booking_raw",
            "last_snapshot_id": 5765432190,
            "last_run_ts": "2026-09-27T06:10:22Z",
            "status": "current",
        },
        "silver.booking_detail": {
            "table_name": "silver.booking_detail",
            "last_snapshot_id": 5765432190,
            "last_run_ts": "2026-09-27T06:22:15Z",
            "status": "current",
        },
        "gold.fact_travel_tag": {
            "table_name": "gold.fact_travel_tag",
            "last_snapshot_id": 8891234567,
            "last_run_ts": "2026-09-27T06:35:42Z",
            "status": "current",
        },
    },
}

_SIMULATED_SNAPSHOTS = {
    "2026-09-28": {
        "silver.booking_detail": [
            {
                "snapshot_id": 5765432190,
                "parent_id": 5765432181,
                "timestamp": "2026-09-28T05:48:31Z",
                "operation": "overwrite",  # MERGE INTO
                "summary": {
                    "added_rows": 14534,
                    "deleted_rows": 0,
                    "updated_rows": 178,
                    "total_files_after": 24,
                },
            },
            {
                "snapshot_id": 5765432181,
                "parent_id": 5765432170,
                "timestamp": "2026-09-27T06:22:15Z",
                "operation": "overwrite",
                "summary": {
                    "added_rows": 15102,
                    "deleted_rows": 0,
                    "updated_rows": 0,
                    "total_files_after": 22,
                },
            },
        ],
    },
    "2026-09-27": {
        "gold.fact_travel_tag": [
            {
                "snapshot_id": 8891234567,
                "parent_id": 8891234550,
                "timestamp": "2026-09-27T06:35:42Z",
                "operation": "overwrite",
                "summary": {
                    "added_rows": 15087,
                    "deleted_rows": 0,
                    "updated_rows": 0,
                    "total_files_after": 8,
                },
            },
        ],
    },
}


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

@tool
def query_gate_results(run_date: str) -> str:
    """Query the result of the reconciliation gate check for a given run date.

    Use this FIRST when investigating a reconciliation failure. It tells you
    which gate failed (Gate 3 = pre-Gold cross-layer check, Gate 4 = post-Gold
    Snowflake match), what counts it compared, and the error message.

    This is your starting point — the gate result tells you WHERE the mismatch
    is, which guides your next investigation steps.

    Args:
        run_date: The pipeline run date to check, in ISO format (e.g. "2026-09-28").

    Returns:
        JSON with gate name, status, check details, row counts, and error message.
        If no gate result exists for this date, returns a "not found" message.
    """
    result = _SIMULATED_GATE_RESULTS.get(run_date)
    if not result:
        return json.dumps({
            "status": "not_found",
            "message": f"No gate failure recorded for run_date={run_date}. "
                       f"Either the pipeline succeeded or hasn't run yet.",
        })
    return json.dumps(result, indent=2)


@tool
def compare_row_counts(run_date: str, tables: list[str]) -> str:
    """Compare row counts across pipeline layers for a given run date.

    Use this to find WHERE rows were lost or gained between layers.
    Pass the tables you want to compare — typically:
      - For Gate 3 failure: ["bronze.booking_raw", "silver.booking_detail",
        "bronze.benefit_raw", "silver.benefit_detail"]
      - For Gate 4 failure: ["silver.booking_detail", "gold.fact_travel_tag"]

    The result shows total count and per-partition breakdown. Look for:
      - Bronze > Silver: rows dropped during MERGE INTO (duplicates? schema reject?)
      - Silver > Gold: rows lost during Gold join (FK lookup failure? filter?)
      - Unexpected partitions: late-arriving data in wrong partition

    Args:
        run_date: The pipeline run date to check.
        tables: List of table names to compare (e.g. ["bronze.booking_raw",
                "silver.booking_detail"]).

    Returns:
        JSON with row counts per table and per partition. Missing tables
        return a "no data" entry.
    """
    day_data = _SIMULATED_ROW_COUNTS.get(run_date, {})
    result = {}
    for table in tables:
        if table in day_data:
            result[table] = day_data[table]
        else:
            result[table] = {
                "total": None,
                "partitions": {},
                "note": f"No row count data for {table} on {run_date}. "
                        f"Table may not have been written to yet.",
            }
    return json.dumps(result, indent=2)


@tool
def check_duplicate_keys(run_date: str, table: str) -> str:
    """Check for duplicate natural keys in a Silver table for a given run date.

    Use this when you see a row count discrepancy between Bronze and Silver.
    Duplicates in Silver indicate upstream re-delivery — the MERGE INTO on
    natural key should handle this, but if the natural key itself is wrong
    or the merge condition is off, duplicates slip through.

    Checks: booking_id for silver.booking_detail, benefit_id for
    silver.benefit_detail.

    Args:
        run_date: The pipeline run date to check.
        table: The Silver table to inspect (e.g. "silver.booking_detail").

    Returns:
        JSON with duplicate count, sample duplicate keys, and an explanation.
        If no duplicates found, confirms the table is clean.
    """
    day_data = _SIMULATED_DUPLICATE_KEYS.get(run_date, {})
    if table in day_data:
        return json.dumps(day_data[table], indent=2)
    return json.dumps({
        "has_duplicates": None,
        "note": f"No duplicate key data available for {table} on {run_date}.",
    })


@tool
def check_fk_integrity(run_date: str) -> str:
    """Check foreign key integrity in the Gold fact table for a given run date.

    Use this when investigating a Gate 4 failure (post-Gold Snowflake mismatch).
    Checks whether FACT_TRAVEL_TAG rows have valid surrogate key references
    to each dimension table (DIM_CARD, DIM_MERCHANT, DIM_BENEFIT).

    NULL FK means the dimension lookup failed during the Gold job — the
    natural key exists in Silver but the corresponding dimension row is
    missing (dimension refresh task may have failed, or the upstream
    account-management pipeline hasn't delivered the data yet).

    Note: dim_benefit FK is nullable by design (benefit data may arrive
    after the booking), so NULL benefit_sk is expected and not a defect.

    Args:
        run_date: The pipeline run date to check.

    Returns:
        JSON with per-dimension FK null counts, sample orphan rows, and notes.
    """
    day_data = _SIMULATED_FK_INTEGRITY.get(run_date, {})
    if not day_data:
        return json.dumps({
            "note": f"No FK integrity data for run_date={run_date}. "
                    f"Gold may not have been produced for this date.",
        })
    return json.dumps(day_data, indent=2)


@tool
def query_watermark_gaps(run_date: str) -> str:
    """Query the watermark control table to find gaps between layers.

    Use this to check whether all layers processed the same data range.
    The watermark control table (control.watermark) stores the last
    processed Iceberg snapshot ID per table. A stale watermark means
    a layer hasn't caught up to its upstream source.

    Look for:
      - Silver watermark behind Bronze: Silver job didn't process latest Bronze data
      - Gold watermark behind Silver: Gold job didn't process latest Silver data
      - Mismatched timestamps: timing issues between layer runs

    Args:
        run_date: The pipeline run date to check.

    Returns:
        JSON with watermark entries per table, including snapshot IDs,
        timestamps, and staleness indicators.
    """
    day_data = _SIMULATED_WATERMARKS.get(run_date, {})
    if not day_data:
        return json.dumps({
            "note": f"No watermark entries found for run_date={run_date}.",
        })
    return json.dumps(day_data, indent=2)


@tool
def query_iceberg_snapshots(run_date: str, table: str) -> str:
    """Query recent Iceberg snapshots for a table to understand write history.

    Use this for deeper investigation — when you need to understand WHAT
    was written and WHEN. Snapshot metadata shows:
      - operation type (append vs overwrite/merge)
      - row counts per operation (added, deleted, updated)
      - file counts (can reveal small-file issues)
      - timestamp (did the write happen in the expected window?)

    This is especially useful when watermarks look correct but row counts
    don't match — the snapshot history reveals whether an unexpected
    overwrite or a partial write occurred.

    Args:
        run_date: The pipeline run date context.
        table: The Iceberg table to inspect (e.g. "silver.booking_detail").

    Returns:
        JSON with recent snapshots: IDs, parent chain, timestamps,
        operations, and row-level summaries.
    """
    day_data = _SIMULATED_SNAPSHOTS.get(run_date, {})
    if table in day_data:
        return json.dumps({
            "table": table,
            "snapshots": day_data[table],
        }, indent=2)
    return json.dumps({
        "table": table,
        "snapshots": [],
        "note": f"No snapshot data available for {table} around {run_date}.",
    })


# ---------------------------------------------------------------------------
# Tool registry — easy import for the agent
# ---------------------------------------------------------------------------

RECON_TOOLS = [
    query_gate_results,
    compare_row_counts,
    check_duplicate_keys,
    check_fk_integrity,
    query_watermark_gaps,
    query_iceberg_snapshots,
]
"""All tools available to the Reconciliation Diagnostics agent."""
