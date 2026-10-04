"""
Spark compute tools for the Argus Spark Debugger agent.

These tools query Spark History Server REST API and event logs to
investigate performance bottlenecks — skew, spill, small files,
broadcast threshold misses, and GC pressure. The Spark Debugger
agent calls them in a ReAct + Reflection loop — gathering evidence,
forming hypotheses about the bottleneck, and refining them.

WHY THIS FILE MATTERS (learning concepts):

  The Spark Debugger's tools are COMPUTE-AWARE, not pipeline-aware.
  They don't know about TTAG tables, watermarks, or DLQ records.
  Instead, they understand Spark's execution model:

    Application → Jobs → Stages → Tasks

  Each level reveals different bottleneck signals:
    - Application: overall duration, executor count, memory config
    - Stage: shuffle read/write, spill, task count
    - Task: skew (median vs max duration), GC time, data size per task
    - Executor: memory usage, GC overhead, disk spill
    - Event log: physical plan (join strategies, partition counts)

TOOL DESIGN FOR COMPUTE DEBUGGING:

  Unlike pipeline tools (which answer "what happened to the data"),
  compute tools answer "why is this job slow/failing". This requires
  a different investigation pattern:

    1. Start broad: get_application_info → overall job metrics
    2. Find the slow stage: get_stage_metrics → stage-level shuffle/spill
    3. Detect skew: get_task_distribution → per-task variance within a stage
    4. Check resources: get_executor_metrics → memory/GC/disk per executor
    5. Understand strategy: parse_physical_plan → join types, scan pushdown
    6. Deep dive: read_event_log → raw Spark event details

  The LLM reasons about these results to connect symptoms to causes:
    - High shuffle write + skew in task durations → data skew on join key
    - High GC time + spill → insufficient executor memory for data volume
    - Many small tasks → small file problem (too many input partitions)
    - SortMergeJoin on small table → missed broadcast opportunity

DEV MODE:
  In dev, these tools return simulated data mimicking real SparkUI metrics
  for realistic failure scenarios. In production, they'd query:
    - Spark History Server REST API (http://host:18080/api/v1)
    - Event logs on HDFS/S3
    - Spark SQL EXPLAIN output
"""

import json
from datetime import datetime, timezone

from langchain_core.tools import tool


# ---------------------------------------------------------------------------
# Simulated data store
# ---------------------------------------------------------------------------
# Two scenarios designed to exercise different debugging paths:
#
# Scenario 1 (app-20260928-001): Data skew on booking join
#   - Stage 2 has massive shuffle, one task takes 10x longer than median
#   - Root cause: booking_id has hot key ("BK-PREMIUM-001") with 400K records
#   - Fix: salting the join key or pre-filtering the hot key
#
# Scenario 2 (app-20260927-001): GC pressure + memory spill
#   - All stages show high GC%, executor memory near limit
#   - Stage 1 has significant disk spill (execution memory exhausted)
#   - Root cause: executor memory too low for data volume, no AQE
#   - Fix: increase executor memory, enable AQE, tune spark.memory.fraction

_SIMULATED_APP_INFO = {
    "app-20260928-001": {
        "app_id": "app-20260928-001",
        "app_name": "ttag_silver_booking_merge",
        "status": "SUCCEEDED",
        "start_time": "2026-09-28T06:01:12Z",
        "end_time": "2026-09-28T06:48:37Z",
        "duration_seconds": 2845,
        "spark_version": "3.5.1",
        "executor_count": 8,
        "executor_memory": "4g",
        "executor_cores": 4,
        "driver_memory": "2g",
        "total_stages": 5,
        "failed_stages": 0,
        "total_tasks": 1624,
        "failed_tasks": 0,
        "spark_config": {
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.autoBroadcastJoinThreshold": "10485760",  # 10MB
            "spark.memory.fraction": "0.6",
            "spark.memory.storageFraction": "0.5",
        },
        "note": "Job completed but took 47 minutes — expected runtime is "
                "~12 minutes. Investigate Stage 2 (the SortMergeJoin stage) "
                "which accounts for 38 minutes of the total runtime.",
    },
    "app-20260927-001": {
        "app_id": "app-20260927-001",
        "app_name": "ttag_gold_fact_build",
        "status": "SUCCEEDED",
        "start_time": "2026-09-27T06:30:05Z",
        "end_time": "2026-09-27T07:15:42Z",
        "duration_seconds": 2737,
        "spark_version": "3.5.1",
        "executor_count": 6,
        "executor_memory": "2g",
        "executor_cores": 2,
        "driver_memory": "1g",
        "total_stages": 4,
        "failed_stages": 0,
        "total_tasks": 812,
        "failed_tasks": 0,
        "spark_config": {
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": "10485760",
            "spark.memory.fraction": "0.6",
            "spark.memory.storageFraction": "0.5",
        },
        "note": "Job completed but took 45 minutes — expected runtime is "
                "~15 minutes. Multiple stages show high GC overhead and "
                "executor memory pressure.",
    },
}

_SIMULATED_STAGE_METRICS = {
    "app-20260928-001": {
        0: {
            "stage_id": 0,
            "stage_name": "scan bronze.booking_raw",
            "status": "COMPLETE",
            "num_tasks": 24,
            "duration_ms": 18500,
            "input_bytes": 285_000_000,
            "input_records": 15012,
            "output_bytes": 275_000_000,
            "output_records": 15012,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 268_000_000,
            "spill_memory_bytes": 0,
            "spill_disk_bytes": 0,
            "gc_time_ms": 1200,
            "peak_execution_memory_bytes": 450_000_000,
            "note": "Clean scan stage — reads Bronze Booking Iceberg table. "
                    "No issues here.",
        },
        1: {
            "stage_id": 1,
            "stage_name": "scan silver.booking_detail (existing)",
            "status": "COMPLETE",
            "num_tasks": 22,
            "duration_ms": 15200,
            "input_bytes": 1_200_000_000,
            "input_records": 145_000,
            "output_bytes": 1_180_000_000,
            "output_records": 145_000,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 1_150_000_000,
            "spill_memory_bytes": 0,
            "spill_disk_bytes": 0,
            "gc_time_ms": 3800,
            "peak_execution_memory_bytes": 1_800_000_000,
            "note": "Scans existing Silver table for MERGE INTO. Large "
                    "shuffle write because the data is redistributed by "
                    "booking_id for the join in Stage 2.",
        },
        2: {
            "stage_id": 2,
            "stage_name": "SortMergeJoin (MERGE INTO booking_detail)",
            "status": "COMPLETE",
            "num_tasks": 200,
            "duration_ms": 2_280_000,  # 38 minutes!
            "input_bytes": 0,
            "input_records": 0,
            "shuffle_read_bytes": 1_418_000_000,
            "shuffle_write_bytes": 1_195_000_000,
            "spill_memory_bytes": 850_000_000,
            "spill_disk_bytes": 320_000_000,
            "gc_time_ms": 185_000,
            "peak_execution_memory_bytes": 3_200_000_000,
            "note": "THE BOTTLENECK — 38 minutes of the 47-minute total. "
                    "SortMergeJoin on booking_id between new Bronze rows and "
                    "existing Silver table. Massive shuffle read (1.4GB), "
                    "significant spill (320MB disk), and high GC time. "
                    "Use get_task_distribution(stage_id=2) to check for skew.",
        },
        3: {
            "stage_id": 3,
            "stage_name": "write silver.booking_detail (Iceberg overwrite)",
            "status": "COMPLETE",
            "num_tasks": 24,
            "duration_ms": 45000,
            "input_bytes": 1_195_000_000,
            "input_records": 159_534,
            "output_bytes": 1_210_000_000,
            "output_records": 159_534,
            "shuffle_read_bytes": 1_195_000_000,
            "shuffle_write_bytes": 0,
            "spill_memory_bytes": 0,
            "spill_disk_bytes": 0,
            "gc_time_ms": 8500,
            "peak_execution_memory_bytes": 900_000_000,
            "note": "Iceberg overwrite partition. Normal duration.",
        },
        4: {
            "stage_id": 4,
            "stage_name": "commit and update watermark",
            "status": "COMPLETE",
            "num_tasks": 1,
            "duration_ms": 2100,
            "input_bytes": 0,
            "input_records": 0,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 0,
            "spill_memory_bytes": 0,
            "spill_disk_bytes": 0,
            "gc_time_ms": 50,
            "peak_execution_memory_bytes": 50_000_000,
            "note": "Iceberg commit + watermark control table update.",
        },
    },
    "app-20260927-001": {
        0: {
            "stage_id": 0,
            "stage_name": "scan silver.booking_detail",
            "status": "COMPLETE",
            "num_tasks": 22,
            "duration_ms": 42000,
            "input_bytes": 1_200_000_000,
            "input_records": 145_000,
            "output_bytes": 1_180_000_000,
            "output_records": 145_000,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 1_150_000_000,
            "spill_memory_bytes": 380_000_000,
            "spill_disk_bytes": 210_000_000,
            "gc_time_ms": 18_500,
            "peak_execution_memory_bytes": 1_100_000_000,
            "note": "Already spilling at the scan stage — executor memory "
                    "(2g) is too small for the data volume. GC time is 44% "
                    "of stage duration.",
        },
        1: {
            "stage_id": 1,
            "stage_name": "scan silver.benefit_detail + dimension lookups",
            "status": "COMPLETE",
            "num_tasks": 18,
            "duration_ms": 38000,
            "input_bytes": 800_000_000,
            "input_records": 98_000,
            "output_bytes": 780_000_000,
            "output_records": 98_000,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 760_000_000,
            "spill_memory_bytes": 220_000_000,
            "spill_disk_bytes": 95_000_000,
            "gc_time_ms": 15_200,
            "peak_execution_memory_bytes": 950_000_000,
            "note": "Same pattern — spill and GC on scan. Benefit data plus "
                    "dimension broadcast joins.",
        },
        2: {
            "stage_id": 2,
            "stage_name": "join booking + benefit for fact table",
            "status": "COMPLETE",
            "num_tasks": 200,
            "duration_ms": 1_650_000,
            "input_bytes": 0,
            "input_records": 0,
            "shuffle_read_bytes": 1_910_000_000,
            "shuffle_write_bytes": 1_450_000_000,
            "spill_memory_bytes": 1_200_000_000,
            "spill_disk_bytes": 680_000_000,
            "gc_time_ms": 520_000,
            "peak_execution_memory_bytes": 1_800_000_000,
            "note": "Severe GC pressure (520s out of 1650s = 31.5% GC). "
                    "Massive spill to disk (680MB). AQE is disabled so "
                    "all 200 shuffle partitions are used even though many "
                    "are nearly empty.",
        },
        3: {
            "stage_id": 3,
            "stage_name": "write gold.fact_travel_tag",
            "status": "COMPLETE",
            "num_tasks": 8,
            "duration_ms": 85000,
            "input_bytes": 1_450_000_000,
            "input_records": 15087,
            "output_bytes": 1_460_000_000,
            "output_records": 15087,
            "shuffle_read_bytes": 1_450_000_000,
            "shuffle_write_bytes": 0,
            "spill_memory_bytes": 150_000_000,
            "spill_disk_bytes": 45_000_000,
            "gc_time_ms": 12_000,
            "peak_execution_memory_bytes": 800_000_000,
            "note": "Write stage also spilling — memory exhaustion persists "
                    "throughout the pipeline.",
        },
    },
}

_SIMULATED_TASK_DISTRIBUTION = {
    "app-20260928-001": {
        2: {
            "stage_id": 2,
            "num_tasks": 200,
            "duration_stats": {
                "min_ms": 850,
                "p25_ms": 2100,
                "median_ms": 3400,
                "p75_ms": 5800,
                "p90_ms": 12000,
                "p95_ms": 18500,
                "max_ms": 2_240_000,  # One task took 37 minutes!
                "mean_ms": 11400,
                "stddev_ms": 158_000,
            },
            "data_size_stats": {
                "min_bytes": 120_000,
                "median_bytes": 6_800_000,
                "max_bytes": 890_000_000,
                "mean_bytes": 7_090_000,
            },
            "skew_ratio": 329.4,  # max / median — extreme skew
            "top_5_slowest_tasks": [
                {"task_id": 142, "duration_ms": 2_240_000,
                 "input_bytes": 890_000_000, "gc_time_ms": 95_000,
                 "spill_disk_bytes": 310_000_000,
                 "partition_key": "booking_id hash partition 142"},
                {"task_id": 87, "duration_ms": 45_000,
                 "input_bytes": 42_000_000, "gc_time_ms": 8_200,
                 "spill_disk_bytes": 8_000_000,
                 "partition_key": "booking_id hash partition 87"},
                {"task_id": 15, "duration_ms": 28_000,
                 "input_bytes": 31_000_000, "gc_time_ms": 5_100,
                 "spill_disk_bytes": 2_000_000,
                 "partition_key": "booking_id hash partition 15"},
                {"task_id": 199, "duration_ms": 22_000,
                 "input_bytes": 25_000_000, "gc_time_ms": 3_800,
                 "spill_disk_bytes": 0,
                 "partition_key": "booking_id hash partition 199"},
                {"task_id": 33, "duration_ms": 19_500,
                 "input_bytes": 22_000_000, "gc_time_ms": 3_200,
                 "spill_disk_bytes": 0,
                 "partition_key": "booking_id hash partition 33"},
            ],
            "note": "EXTREME SKEW DETECTED — task 142 processed 890MB "
                    "(vs median 6.8MB = 130x data skew) and took 37 minutes "
                    "(vs median 3.4s = 659x time skew). This single task "
                    "dominates the stage. The booking_id hash partition 142 "
                    "contains a hot key — likely a high-frequency booking_id "
                    "pattern. Use parse_physical_plan to see the join strategy "
                    "and read_event_log for the actual hot key values.",
        },
    },
    "app-20260927-001": {
        2: {
            "stage_id": 2,
            "num_tasks": 200,
            "duration_stats": {
                "min_ms": 2800,
                "p25_ms": 5200,
                "median_ms": 7500,
                "p75_ms": 9800,
                "p90_ms": 12000,
                "p95_ms": 14500,
                "max_ms": 22_000,
                "mean_ms": 8250,
                "stddev_ms": 3200,
            },
            "data_size_stats": {
                "min_bytes": 4_500_000,
                "median_bytes": 9_550_000,
                "max_bytes": 18_000_000,
                "mean_bytes": 9_200_000,
            },
            "skew_ratio": 2.9,  # No significant skew
            "top_5_slowest_tasks": [
                {"task_id": 88, "duration_ms": 22_000,
                 "input_bytes": 18_000_000, "gc_time_ms": 9_800,
                 "spill_disk_bytes": 12_000_000,
                 "partition_key": "hash partition 88"},
                {"task_id": 12, "duration_ms": 19_500,
                 "input_bytes": 16_500_000, "gc_time_ms": 8_500,
                 "spill_disk_bytes": 9_000_000,
                 "partition_key": "hash partition 12"},
                {"task_id": 156, "duration_ms": 18_200,
                 "input_bytes": 15_800_000, "gc_time_ms": 8_100,
                 "spill_disk_bytes": 8_500_000,
                 "partition_key": "hash partition 156"},
                {"task_id": 42, "duration_ms": 17_800,
                 "input_bytes": 15_200_000, "gc_time_ms": 7_900,
                 "spill_disk_bytes": 7_800_000,
                 "partition_key": "hash partition 42"},
                {"task_id": 201, "duration_ms": 16_500,
                 "input_bytes": 14_000_000, "gc_time_ms": 7_200,
                 "spill_disk_bytes": 6_500_000,
                 "partition_key": "hash partition 201"},
            ],
            "note": "No significant skew (ratio 2.9x). All tasks have "
                    "uniformly high GC time and disk spill — this is a "
                    "systemic memory issue, not a skew problem. Every task "
                    "spills because executor memory (2g) is insufficient.",
        },
    },
}

_SIMULATED_EXECUTOR_METRICS = {
    "app-20260928-001": [
        {
            "executor_id": "1",
            "host": "worker-01",
            "total_cores": 4,
            "max_memory_bytes": 4_294_967_296,
            "memory_used_bytes": 3_850_000_000,
            "memory_used_pct": 89.6,
            "total_gc_time_ms": 52_000,
            "total_duration_ms": 720_000,
            "gc_pct": 7.2,
            "total_spill_disk_bytes": 85_000_000,
            "tasks_completed": 203,
            "tasks_failed": 0,
            "shuffle_read_bytes": 178_000_000,
            "shuffle_write_bytes": 165_000_000,
        },
        {
            "executor_id": "2",
            "host": "worker-01",
            "total_cores": 4,
            "max_memory_bytes": 4_294_967_296,
            "memory_used_bytes": 3_920_000_000,
            "memory_used_pct": 91.3,
            "total_gc_time_ms": 145_000,
            "total_duration_ms": 2_280_000,
            "gc_pct": 6.4,
            "total_spill_disk_bytes": 310_000_000,
            "tasks_completed": 205,
            "tasks_failed": 0,
            "shuffle_read_bytes": 920_000_000,
            "shuffle_write_bytes": 425_000_000,
            "note": "THIS executor handled the skewed task 142 — "
                    "310MB disk spill, 91% memory usage, longest duration.",
        },
        {
            "executor_id": "3",
            "host": "worker-02",
            "total_cores": 4,
            "max_memory_bytes": 4_294_967_296,
            "memory_used_bytes": 2_100_000_000,
            "memory_used_pct": 48.9,
            "total_gc_time_ms": 18_000,
            "total_duration_ms": 310_000,
            "gc_pct": 5.8,
            "total_spill_disk_bytes": 0,
            "tasks_completed": 204,
            "tasks_failed": 0,
            "shuffle_read_bytes": 165_000_000,
            "shuffle_write_bytes": 152_000_000,
        },
        {
            "executor_id": "4",
            "host": "worker-02",
            "total_cores": 4,
            "max_memory_bytes": 4_294_967_296,
            "memory_used_bytes": 2_250_000_000,
            "memory_used_pct": 52.4,
            "total_gc_time_ms": 19_500,
            "total_duration_ms": 325_000,
            "gc_pct": 6.0,
            "total_spill_disk_bytes": 0,
            "tasks_completed": 203,
            "tasks_failed": 0,
            "shuffle_read_bytes": 168_000_000,
            "shuffle_write_bytes": 155_000_000,
        },
    ],
    "app-20260927-001": [
        {
            "executor_id": "1",
            "host": "worker-01",
            "total_cores": 2,
            "max_memory_bytes": 2_147_483_648,
            "memory_used_bytes": 2_050_000_000,
            "memory_used_pct": 95.5,
            "total_gc_time_ms": 185_000,
            "total_duration_ms": 680_000,
            "gc_pct": 27.2,
            "total_spill_disk_bytes": 245_000_000,
            "tasks_completed": 136,
            "tasks_failed": 0,
            "shuffle_read_bytes": 520_000_000,
            "shuffle_write_bytes": 480_000_000,
            "note": "CRITICAL — 27% GC overhead, 95.5% memory usage. "
                    "Executor is memory-starved.",
        },
        {
            "executor_id": "2",
            "host": "worker-01",
            "total_cores": 2,
            "max_memory_bytes": 2_147_483_648,
            "memory_used_bytes": 2_080_000_000,
            "memory_used_pct": 96.9,
            "total_gc_time_ms": 195_000,
            "total_duration_ms": 695_000,
            "gc_pct": 28.1,
            "total_spill_disk_bytes": 260_000_000,
            "tasks_completed": 135,
            "tasks_failed": 0,
            "shuffle_read_bytes": 535_000_000,
            "shuffle_write_bytes": 495_000_000,
            "note": "CRITICAL — 28% GC overhead, 97% memory usage.",
        },
        {
            "executor_id": "3",
            "host": "worker-02",
            "total_cores": 2,
            "max_memory_bytes": 2_147_483_648,
            "memory_used_bytes": 2_020_000_000,
            "memory_used_pct": 94.1,
            "total_gc_time_ms": 175_000,
            "total_duration_ms": 670_000,
            "gc_pct": 26.1,
            "total_spill_disk_bytes": 230_000_000,
            "tasks_completed": 137,
            "tasks_failed": 0,
            "shuffle_read_bytes": 510_000_000,
            "shuffle_write_bytes": 475_000_000,
        },
    ],
}

_SIMULATED_PHYSICAL_PLANS = {
    "app-20260928-001": {
        "app_id": "app-20260928-001",
        "plan_text": (
            "== Physical Plan ==\n"
            "*(5) ColumnarToRow\n"
            "+- *(5) Project [booking_id, card_id, merchant_id, ...]\n"
            "   +- *(5) SortMergeJoin [booking_id], [booking_id], FullOuter\n"
            "      :- *(2) Sort [booking_id ASC], false, 0\n"
            "      :  +- Exchange hashpartitioning(booking_id, 200)\n"
            "      :     +- *(1) Filter (isnotnull(booking_id))\n"
            "      :        +- *(1) ColumnarToRow\n"
            "      :           +- BatchScan bronze.booking_raw [...]\n"
            "      +- *(4) Sort [booking_id ASC], false, 0\n"
            "         +- Exchange hashpartitioning(booking_id, 200)\n"
            "            +- *(3) ColumnarToRow\n"
            "               +- BatchScan silver.booking_detail [...]\n"
        ),
        "join_strategies": [
            {
                "join_type": "SortMergeJoin",
                "join_condition": "booking_id = booking_id",
                "left_table": "bronze.booking_raw",
                "right_table": "silver.booking_detail",
                "left_rows": 15012,
                "right_rows": 145_000,
                "right_size_bytes": 1_200_000_000,
                "broadcast_eligible": False,
                "reason_not_broadcast": "Right side (1.2GB) exceeds "
                                        "autoBroadcastJoinThreshold (10MB)",
                "note": "SortMergeJoin chosen because silver.booking_detail "
                        "(1.2GB) is too large to broadcast. Both sides are "
                        "hash-partitioned on booking_id with 200 partitions. "
                        "If booking_id has a hot key, the corresponding "
                        "partition will be massive — causing skew.",
            },
        ],
        "partition_info": {
            "shuffle_partitions": 200,
            "input_partitions_left": 24,
            "input_partitions_right": 22,
            "aqe_coalesced": False,
            "note": "AQE is enabled but coalescing didn't help the skewed "
                    "partition — AQE coalesces small partitions, it doesn't "
                    "split large ones (that requires AQE skew join "
                    "optimization: spark.sql.adaptive.skewJoin.enabled).",
        },
    },
    "app-20260927-001": {
        "app_id": "app-20260927-001",
        "plan_text": (
            "== Physical Plan ==\n"
            "*(6) ColumnarToRow\n"
            "+- *(6) Project [booking_id, benefit_id, card_sk, ...]\n"
            "   +- *(6) SortMergeJoin [card_id], [card_id], LeftOuter\n"
            "      :- *(4) SortMergeJoin [booking_id], [booking_id], LeftOuter\n"
            "      :  :- *(2) Sort [booking_id ASC], false, 0\n"
            "      :  :  +- Exchange hashpartitioning(booking_id, 200)\n"
            "      :  :     +- *(1) BatchScan silver.booking_detail [...]\n"
            "      :  +- *(3) Sort [booking_id ASC], false, 0\n"
            "      :     +- Exchange hashpartitioning(booking_id, 200)\n"
            "      :        +- *(3) BatchScan silver.benefit_detail [...]\n"
            "      +- *(5) Sort [card_id ASC], false, 0\n"
            "         +- Exchange hashpartitioning(card_id, 200)\n"
            "            +- *(5) BatchScan dim_card [...]\n"
        ),
        "join_strategies": [
            {
                "join_type": "SortMergeJoin",
                "join_condition": "booking_id = booking_id",
                "left_table": "silver.booking_detail",
                "right_table": "silver.benefit_detail",
                "left_rows": 145_000,
                "right_rows": 98_000,
                "right_size_bytes": 800_000_000,
                "broadcast_eligible": False,
                "reason_not_broadcast": "Right side (800MB) exceeds "
                                        "autoBroadcastJoinThreshold (10MB)",
            },
            {
                "join_type": "SortMergeJoin",
                "join_condition": "card_id = card_id",
                "left_table": "(booking ⋈ benefit)",
                "right_table": "dim_card",
                "left_rows": 145_000,
                "right_rows": 52_000,
                "right_size_bytes": 15_600_000,
                "broadcast_eligible": False,
                "reason_not_broadcast": "Right side (15.6MB) exceeds "
                                        "autoBroadcastJoinThreshold (10MB)",
                "note": "DIM_CARD (15.6MB) is close to broadcast threshold "
                        "(10MB). Increasing threshold to 20MB would allow "
                        "broadcast, eliminating one shuffle stage entirely.",
            },
        ],
        "partition_info": {
            "shuffle_partitions": 200,
            "input_partitions_booking": 22,
            "input_partitions_benefit": 18,
            "input_partitions_dim_card": 4,
            "aqe_coalesced": False,
            "note": "AQE is DISABLED. With 200 shuffle partitions and "
                    "relatively small data, many partitions are nearly "
                    "empty — wasting task overhead. AQE coalescing would "
                    "reduce effective partitions from 200 to ~40.",
        },
    },
}

_SIMULATED_EVENT_LOG = {
    "app-20260928-001": {
        "app_id": "app-20260928-001",
        "hot_keys_detected": [
            {
                "stage_id": 2,
                "partition": 142,
                "key": "booking_id",
                "hot_value": "BK-PREMIUM-001",
                "record_count": 398_000,
                "total_records_in_stage": 160_012,
                "pct_of_total": 248.7,  # more than total because it's the join fanout
                "note": "booking_id='BK-PREMIUM-001' is a corporate travel "
                        "account with 398K transaction records that all hash "
                        "to partition 142. This single key causes the extreme "
                        "skew. The MERGE INTO joins every new record against "
                        "all 398K existing records in this partition.",
            },
        ],
        "aqe_events": [
            {
                "event": "AdaptiveSparkPlanExec",
                "skew_join_enabled": False,
                "coalesce_enabled": True,
                "partitions_coalesced": 0,
                "note": "AQE skew join optimization is NOT enabled "
                        "(spark.sql.adaptive.skewJoin.enabled=false). "
                        "If enabled, Spark would automatically split the "
                        "skewed partition 142 into smaller sub-partitions.",
            },
        ],
        "gc_events": [
            {
                "executor_id": "2",
                "total_full_gc_count": 12,
                "total_full_gc_time_ms": 48_000,
                "longest_gc_pause_ms": 8_500,
                "note": "Executor 2 (handling skewed task) had 12 full GCs, "
                        "longest pause 8.5 seconds. GC pressure caused by "
                        "the 890MB partition being processed in 4GB memory.",
            },
        ],
    },
    "app-20260927-001": {
        "app_id": "app-20260927-001",
        "hot_keys_detected": [],
        "aqe_events": [
            {
                "event": "AdaptiveSparkPlanExec",
                "note": "AQE is DISABLED for this application. No adaptive "
                        "optimizations applied. With AQE disabled:\n"
                        "- No partition coalescing (200 partitions used even "
                        "when most are small)\n"
                        "- No skew join optimization\n"
                        "- No runtime join strategy switching (e.g., "
                        "SortMergeJoin → BroadcastHashJoin for small tables)",
            },
        ],
        "gc_events": [
            {
                "executor_id": "1",
                "total_full_gc_count": 45,
                "total_full_gc_time_ms": 125_000,
                "longest_gc_pause_ms": 12_000,
                "note": "SEVERE — 45 full GCs, 125 seconds total. Each GC "
                        "pause blocks all tasks on this executor.",
            },
            {
                "executor_id": "2",
                "total_full_gc_count": 48,
                "total_full_gc_time_ms": 132_000,
                "longest_gc_pause_ms": 14_000,
                "note": "SEVERE — 48 full GCs, longest pause 14 seconds.",
            },
            {
                "executor_id": "3",
                "total_full_gc_count": 42,
                "total_full_gc_time_ms": 118_000,
                "longest_gc_pause_ms": 11_500,
                "note": "SEVERE — 42 full GCs across the job lifetime.",
            },
        ],
        "memory_analysis": {
            "executor_memory_bytes": 2_147_483_648,
            "spark_memory_fraction": 0.6,
            "usable_memory_bytes": 1_288_490_189,
            "execution_memory_bytes": 644_245_094,
            "storage_memory_bytes": 644_245_094,
            "note": "With 2g executor memory and 0.6 fraction, only 1.2GB "
                    "is available for Spark's unified memory. Of that, "
                    "execution gets 644MB — but shuffle data per executor "
                    "averages 500MB+ per stage, causing constant spill. "
                    "Recommendation: increase to 4g-6g executor memory, or "
                    "increase spark.memory.fraction to 0.75.",
        },
    },
}


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

@tool
def get_application_info(app_id: str) -> str:
    """Get high-level information about a Spark application.

    Use this FIRST when investigating a slow or failed Spark job. It gives you
    the overall picture: duration, executor count, memory config, stage/task
    counts, and key Spark configurations.

    This orients your investigation — look for:
      - Duration much longer than expected → performance bottleneck
      - Failed stages/tasks → job failure, not just slowness
      - Low executor count or memory → possible resource starvation
      - AQE disabled → missed optimization opportunity
      - Low autoBroadcastJoinThreshold → potential missed broadcasts

    Args:
        app_id: The Spark application ID (e.g. "app-20260928-001").
                Found in Airflow task logs or Spark History Server.

    Returns:
        JSON with application metadata, timing, executor config, Spark
        settings, and an initial assessment note.
    """
    result = _SIMULATED_APP_INFO.get(app_id)
    if not result:
        return json.dumps({
            "status": "not_found",
            "message": f"No application found with id={app_id}. "
                       f"Check the Spark History Server or Airflow logs "
                       f"for the correct application ID.",
        })
    return json.dumps(result, indent=2)


@tool
def get_stage_metrics(app_id: str, stage_id: int | None = None) -> str:
    """Get detailed metrics for stages in a Spark application.

    Use this after get_application_info to find WHICH stage is the bottleneck.
    If stage_id is provided, returns metrics for that specific stage.
    If omitted, returns metrics for ALL stages (useful for comparison).

    Key metrics to evaluate per stage:
      - duration_ms: how long this stage took (compare to total app duration)
      - shuffle_read/write_bytes: data movement between stages (high = expensive)
      - spill_memory/disk_bytes: data that didn't fit in memory (spill = slow)
      - gc_time_ms: time spent in garbage collection (high = memory pressure)
      - peak_execution_memory: how much memory this stage actually used

    Look for:
      - One stage taking most of the total duration → focus there
      - Shuffle bytes >> input bytes → join or aggregation is expensive
      - Spill > 0 → memory insufficient for the operation
      - GC time > 10% of duration → executor memory pressure

    Args:
        app_id: The Spark application ID.
        stage_id: Optional specific stage to query. Omit for all stages.

    Returns:
        JSON with per-stage metrics including shuffle, spill, GC, memory,
        and task counts. Includes interpretive notes.
    """
    app_data = _SIMULATED_STAGE_METRICS.get(app_id)
    if not app_data:
        return json.dumps({
            "status": "not_found",
            "message": f"No stage metrics found for app_id={app_id}.",
        })

    if stage_id is not None:
        stage = app_data.get(stage_id)
        if not stage:
            return json.dumps({
                "status": "not_found",
                "message": f"Stage {stage_id} not found in {app_id}. "
                           f"Available stages: {list(app_data.keys())}",
            })
        return json.dumps(stage, indent=2)

    return json.dumps(
        {"app_id": app_id, "stages": app_data},
        indent=2,
    )


@tool
def get_task_distribution(app_id: str, stage_id: int) -> str:
    """Get task-level duration and data size distribution for a stage.

    Use this when you suspect DATA SKEW — one or a few tasks taking much
    longer than others within the same stage. This is the key tool for
    detecting skew because it shows:

      - Duration percentiles (min, p25, median, p75, p90, p95, max)
      - Data size percentiles (how much data each task processed)
      - Skew ratio (max / median) — ratio > 10x is significant skew
      - Top 5 slowest tasks with their specific metrics

    How to interpret:
      - skew_ratio > 10: significant skew — one partition has much more data
      - skew_ratio > 100: extreme skew — investigate the hot key
      - All tasks slow with low skew: systemic issue (memory, GC), not skew
      - High GC in slowest tasks: memory pressure on the skewed partition

    After confirming skew, use parse_physical_plan to understand the join
    strategy, and read_event_log to find the actual hot key values.

    Args:
        app_id: The Spark application ID.
        stage_id: The stage to analyze (use get_stage_metrics to find the
                  bottleneck stage first).

    Returns:
        JSON with duration/size percentiles, skew ratio, and details of
        the top 5 slowest tasks including their partition assignment.
    """
    app_data = _SIMULATED_TASK_DISTRIBUTION.get(app_id, {})
    stage = app_data.get(stage_id)
    if not stage:
        return json.dumps({
            "status": "not_found",
            "message": f"No task distribution data for stage {stage_id} "
                       f"in {app_id}. Available stages: "
                       f"{list(app_data.keys()) if app_data else 'none'}",
        })
    return json.dumps(stage, indent=2)


@tool
def get_executor_metrics(app_id: str) -> str:
    """Get per-executor resource utilization metrics.

    Use this to understand resource pressure across executors. Helps
    distinguish between:

      - Skew-related: one executor under heavy load, others idle
      - Systemic: ALL executors under memory/GC pressure
      - Resource starvation: executors at capacity, need more resources

    Key metrics per executor:
      - memory_used_pct: how close to memory limit (>90% = pressure)
      - gc_pct: GC time as percentage of total time (>15% = problematic)
      - total_spill_disk_bytes: data spilled to disk (>0 = memory issue)
      - tasks_completed/failed: executor health

    Patterns:
      - One executor high memory + spill, others low → skew (data routed
        to one executor by hash partitioning)
      - ALL executors high memory + GC → need more memory per executor
        or fewer concurrent tasks per executor
      - High shuffle bytes with low memory → shuffle data doesn't fit

    Args:
        app_id: The Spark application ID.

    Returns:
        JSON with per-executor metrics: memory, GC, spill, shuffle,
        task counts, and interpretive notes.
    """
    executors = _SIMULATED_EXECUTOR_METRICS.get(app_id)
    if not executors:
        return json.dumps({
            "status": "not_found",
            "message": f"No executor metrics found for app_id={app_id}.",
        })
    return json.dumps(
        {"app_id": app_id, "executors": executors},
        indent=2,
    )


@tool
def parse_physical_plan(app_id: str) -> str:
    """Parse the Spark physical execution plan for a job.

    Use this to understand HOW Spark is executing the job — specifically
    the join strategies, partition counts, and whether optimizations like
    broadcast join or predicate pushdown are being applied.

    Key things to look for:
      - SortMergeJoin on large tables: expected, but check if one side
        is small enough for BroadcastHashJoin (cheaper, no shuffle)
      - Exchange (hashpartitioning): shuffle operation — the number of
        partitions affects task count and potential skew
      - BatchScan with filters: predicate pushdown is working
      - No BroadcastHashJoin when table is small: autoBroadcastJoinThreshold
        may be too low

    After finding a SortMergeJoin with skew, the fix is usually one of:
      1. Enable AQE skew join (spark.sql.adaptive.skewJoin.enabled=true)
      2. Salt the join key to distribute the hot key across partitions
      3. Pre-filter the hot key for separate processing
      4. Increase broadcast threshold if one side is near the limit

    Args:
        app_id: The Spark application ID.

    Returns:
        JSON with the physical plan text, join strategy analysis,
        partition configuration, and optimization suggestions.
    """
    plan = _SIMULATED_PHYSICAL_PLANS.get(app_id)
    if not plan:
        return json.dumps({
            "status": "not_found",
            "message": f"No physical plan available for app_id={app_id}.",
        })
    return json.dumps(plan, indent=2)


@tool
def read_event_log(app_id: str, event_type: str | None = None) -> str:
    """Read Spark event log entries for deep investigation.

    Use this for the DEEPEST level of investigation — when you need to
    understand specific Spark runtime events. Useful for:

      - Finding hot key values (which booking_id caused the skew)
      - Checking AQE decisions (did Spark try to optimize?)
      - Analyzing GC events (full GC count, pause durations)
      - Understanding memory allocation and spill patterns

    Available event_type filters:
      - "hot_keys": detected hot partition keys (if Spark logged them)
      - "aqe": Adaptive Query Execution events and decisions
      - "gc": garbage collection events per executor
      - "memory": memory allocation and usage breakdown
      - None: returns all available event types

    Use this after you've formed a hypothesis from the higher-level
    tools (application, stages, tasks, executors) to confirm or refine
    your diagnosis.

    Args:
        app_id: The Spark application ID.
        event_type: Optional filter — "hot_keys", "aqe", "gc", "memory".
                    Omit for all events.

    Returns:
        JSON with event log entries matching the filter, including
        interpretive notes.
    """
    app_data = _SIMULATED_EVENT_LOG.get(app_id)
    if not app_data:
        return json.dumps({
            "status": "not_found",
            "message": f"No event log data available for app_id={app_id}.",
        })

    if event_type:
        # Map event_type to the correct data key
        type_map = {
            "hot_keys": "hot_keys_detected",
            "aqe": "aqe_events",
            "gc": "gc_events",
            "memory": "memory_analysis",
        }
        key = type_map.get(event_type)
        if key and key in app_data:
            return json.dumps(
                {"app_id": app_id, "event_type": event_type,
                 "data": app_data[key]},
                indent=2,
            )
        return json.dumps({
            "status": "not_found",
            "message": f"No '{event_type}' events for {app_id}. "
                       f"Available types: {[k for k in type_map if type_map[k] in app_data]}",
        })

    # Return everything
    return json.dumps(
        {"app_id": app_id, "events": app_data},
        indent=2,
    )


# ---------------------------------------------------------------------------
# Tool registry — easy import for the agent
# ---------------------------------------------------------------------------

SPARK_TOOLS = [
    get_application_info,
    get_stage_metrics,
    get_task_distribution,
    get_executor_metrics,
    parse_physical_plan,
    read_event_log,
]
"""All tools available to the Spark Debugger agent."""
