# Code Guide: `argus/tools/compute/spark_tools.py`

## Purpose

Six `@tool`-decorated functions that give the Spark Debugger agent access to Spark application data. Unlike the pipeline tools (recon_tools, dlq_tools, backfill_tools) which query pipeline metadata, these tools query **compute infrastructure** — SparkUI metrics, execution plans, task-level distributions, and event logs.

## Reading Order Context

**Read AFTER**: `recon_tools.py` (same @tool pattern), `reports.py` (SparkBottleneck/SparkDiagnosis schemas)
**Read BEFORE**: `state.py`, `prompts.py`, `graph.py` (tools are bound to the LLM in the graph)

## Key Concepts

### 1. Compute-Aware vs Pipeline-Aware Tools

The first four agents' tools (recon, DLQ, backfill) all deal with **pipeline state**: row counts, watermarks, DLQ records, backfill plans. The Spark Debugger's tools deal with **compute state**: how the Spark engine executed a job.

This distinction matters because:
- Pipeline tools answer "what happened to the data?"
- Compute tools answer "why was the processing slow/broken?"

### 2. Spark Application Hierarchy

The tools expose Spark's execution model:
```
Application (app_id)
  └── Jobs (triggered by actions like .count(), .write())
        └── Stages (separated by shuffles)
              └── Tasks (one per partition, run on executors)
```

Each tool targets a different level:
- `get_application_info` → Application level (overview, config, duration)
- `get_stage_metrics` → Stage level (shuffle bytes, spill, duration per stage)
- `get_task_distribution` → Task level (skew detection, per-task metrics)
- `get_executor_metrics` → Executor level (memory, GC, resource health)
- `parse_physical_plan` → Query plan (join strategies, AQE decisions)
- `read_event_log` → Raw events (hot keys, GC events, memory breakdown)

### 3. Two Simulated Scenarios

| App ID | Job Name | Primary Issue | Secondary Issues |
|--------|----------|---------------|-----------------|
| `app-20260928-001` | ttag_silver_booking_merge | Data skew (329x on booking_id) | Spill, GC on skewed partition |
| `app-20260927-001` | ttag_gold_fact_build | GC pressure (27%+ all executors) | Memory spill, AQE disabled |

Scenario 1 tests whether the agent can identify a **localized** problem (one hot key). Scenario 2 tests whether it can identify a **systemic** problem (all executors under-resourced).

### 4. Tool Docstrings as Prompt Engineering

Each tool's docstring is critical — the LLM reads it to decide when and how to use the tool. Key patterns:
- **When to use** — explicit guidance on investigation order
- **Key signals to look for** — tells the LLM what matters in the output
- **Parameter descriptions** — guides the LLM's argument construction

## Learning Checkpoint

After reading this file, you should understand:
- [ ] How Spark's Application → Job → Stage → Task hierarchy maps to the 6 tools
- [ ] Why compute tools are fundamentally different from pipeline tools
- [ ] How the two simulated scenarios exercise different diagnostic paths
- [ ] Why tool docstrings are a form of prompt engineering
