"""
Argus Phase 3 — DLQ Triage Agent Live Test
============================================
A live integration test that runs the full DLQ Triage & Auto-Remediation
agent against a real LLM (Gemini free tier) with simulated DLQ data.

This is NOT a unit test — it makes real API calls and costs (free) tokens.
Run this to verify the full agent pipeline works end-to-end:

  TriggerContext → DLQTriageAgent.invoke() → ReAct loop → DLQTriageReport

WHY THIS FILE EXISTS (learning concepts):

  Unit tests (test_dlq_agent.py) verify structure with mocked LLMs.
  This live test answers the questions mocks can't:

    1. Does the LLM understand the CLASSIFICATION RUBRIC well enough to
       classify records accurately? (New for Phase 3)
    2. Does the LLM respect the REQUEUE SAFETY RULES — only requeueing
       TRANSIENT records with confidence >= 0.80? (Side-effect guardrail)
    3. Does the LLM cross-reference query_schema_changelog before
       classifying SCHEMA_MISMATCH? (Investigation strategy)
    4. Does the ReAct loop converge to a DLQTriageReport with per-record
       classifications?
    5. Do the DLQ tool docstrings guide the LLM correctly?

WHAT'S DIFFERENT FROM recon_live_test.py:

  - TWO DLQ SCENARIOS instead of two gate scenarios:
      1. kafka_dlq — Benefit lane Kafka DLQ records (transient, schema, DQ, unknown)
      2. bad_files — Booking lane Iceberg quarantine (corrupt, schema, DQ)

  - REPORT FORMAT is different — per-record classifications with
    confidence scores, requeue counts, quarantine counts.

  - REQUEUE AUDIT — the live test shows which records were requeued,
    validating the safety guardrails.

  - The "both" scenario runs BOTH lanes, testing the agent's ability to
    triage records from different DLQ sources in a single invocation.

REQUIREMENTS:
  - GOOGLE_API_KEY must be set (Gemini free tier — no cost)
  - Run from repo root: python experiments/dlq_live_test.py

USAGE:
  # Run both DLQ lanes (default):
  python experiments/dlq_live_test.py

  # Run a specific lane:
  python experiments/dlq_live_test.py --scenario kafka
  python experiments/dlq_live_test.py --scenario bad_files

  # Show tool schemas (what the LLM sees):
  python experiments/dlq_live_test.py --show-tools
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

# ── Ensure repo root is on sys.path ──────────────────────────────────
# When running as `python experiments/dlq_live_test.py`, the repo root
# isn't automatically on sys.path. We add it so `from argus.xxx` works.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from argus.agents.base import TriggerContext
from argus.agents.dlq_triage.agent import DLQTriageAgent
from argus.core.config import load_config
from argus.tools.pipeline.dlq_tools import DLQ_TOOLS


# ---------------------------------------------------------------------------
# ANSI colors — same style as calculator_agent.py and recon_live_test.py
# ---------------------------------------------------------------------------

BOLD   = "\033[1m"
DIM    = "\033[2m"
GREEN  = "\033[32m"
BLUE   = "\033[34m"
CYAN   = "\033[36m"
YELLOW = "\033[33m"
RED    = "\033[31m"
MAGENTA = "\033[35m"
RESET  = "\033[0m"


def _separator(label: str, color: str = GREEN) -> None:
    """Print a separator line with a label."""
    print(f"\n{color}{'─' * 60}")
    print(f"  {label}")
    print(f"{'─' * 60}{RESET}\n")


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------
# These map to the simulated data in dlq_tools.py.
# Each scenario tests different classification paths.

SCENARIOS = {
    "kafka": {
        "name": "Kafka DLQ — Benefit Lane",
        "description": (
            "Kafka DLQ records from the Benefit lane on 2026-09-30.\n"
            "  6 records covering all 4 classification categories:\n"
            "    - 2x TimeoutException → TRANSIENT (should be requeued)\n"
            "    - 2x SchemaRegistryException → SCHEMA_MISMATCH (confirm via changelog)\n"
            "    - 1x NullPointerException → DATA_QUALITY (quarantine)\n"
            "    - 1x UnknownProcessingException → UNKNOWN (escalate)\n"
            "  Expected tools: read_dlq_records → query_schema_changelog → requeue_message"
        ),
        "context": TriggerContext(
            agent_name="dlq_triage",
            trigger_source="live_test",
            run_date="2026-09-30",
            correlation_id="live-test-kafka-001",
            params={
                "dlq_threshold_breached": True,
                "source_lane": "kafka_dlq",
            },
        ),
    },
    "bad_files": {
        "name": "Bad Files — Booking Lane",
        "description": (
            "Iceberg quarantine (bad_files) from the Booking lane on 2026-09-29.\n"
            "  3 records covering 3 classification categories:\n"
            "    - 1x corrupt file with checksum mismatch → TRANSIENT (re-fetch)\n"
            "    - 1x column type mismatch → SCHEMA_MISMATCH (confirm via changelog)\n"
            "    - 1x DQ check failures → DATA_QUALITY (quarantine)\n"
            "  Expected tools: read_dlq_records → query_schema_changelog → requeue_message"
        ),
        "context": TriggerContext(
            agent_name="dlq_triage",
            trigger_source="live_test",
            run_date="2026-09-29",
            correlation_id="live-test-badfiles-001",
            params={
                "dlq_threshold_breached": True,
                "source_lane": "bad_files",
            },
        ),
    },
}


# ---------------------------------------------------------------------------
# Tool schema display — shows what the LLM "sees"
# ---------------------------------------------------------------------------

def show_tool_schemas() -> None:
    """Print the DLQ tool schemas that get bound to the LLM."""
    _separator("DLQ TOOL SCHEMAS (what the LLM sees)", CYAN)

    for i, tool_fn in enumerate(DLQ_TOOLS, 1):
        print(f"  {BOLD}{i}. {tool_fn.name}{RESET}")
        print(f"     {DIM}{tool_fn.description[:120]}...{RESET}")

        # Show the input schema (this is what .bind_tools() sends to the LLM)
        schema = tool_fn.args_schema.model_json_schema() if tool_fn.args_schema else {}
        props = schema.get("properties", {})
        for param_name, param_info in props.items():
            param_type = param_info.get("type", "any")
            desc = param_info.get("description", "")
            print(f"     → {param_name}: {param_type}")
            if desc:
                print(f"       {DIM}{desc[:80]}{RESET}")
        print()

    print(f"  {DIM}Total tools: {len(DLQ_TOOLS)}{RESET}")
    print(f"  {DIM}These are bound via llm.bind_tools(DLQ_TOOLS){RESET}\n")


# ---------------------------------------------------------------------------
# Classification color helper
# ---------------------------------------------------------------------------

_CLASSIFICATION_COLORS = {
    "TRANSIENT": GREEN,
    "SCHEMA_MISMATCH": YELLOW,
    "DATA_QUALITY": MAGENTA,
    "UNKNOWN": RED,
}


def _color_classification(classification: str) -> str:
    """Return ANSI-colored classification string."""
    color = _CLASSIFICATION_COLORS.get(classification, RESET)
    return f"{color}{BOLD}{classification}{RESET}"


# ---------------------------------------------------------------------------
# Run a single scenario
# ---------------------------------------------------------------------------

async def run_scenario(
    scenario_key: str,
    agent: DLQTriageAgent,
) -> dict:
    """
    Run one DLQ triage scenario and print the results.

    Returns:
        The AgentResult for inspection.
    """
    scenario = SCENARIOS[scenario_key]
    context = scenario["context"]

    _separator(f"SCENARIO: {scenario['name']}", BLUE)
    print(f"  {scenario['description']}\n")
    print(f"  {DIM}Run date:        {context.run_date}{RESET}")
    print(f"  {DIM}Source lane:      {context.params.get('source_lane', 'both')}{RESET}")
    print(f"  {DIM}Correlation ID:   {context.correlation_id}{RESET}")
    print(f"  {DIM}Trigger source:   {context.trigger_source}{RESET}")
    print()

    # ── Run the agent ─────────────────────────────────────────────
    print(f"  {YELLOW}▶ Invoking DLQTriageAgent...{RESET}")
    print(f"  {DIM}(This makes real LLM API calls — watch the ReAct loop){RESET}\n")

    start_time = time.monotonic()
    result = await agent.invoke(context)
    elapsed = time.monotonic() - start_time

    # ── Display results ───────────────────────────────────────────
    _separator("RESULTS", GREEN if result.status == "success" else RED)

    # Status
    status_icon = "✅" if result.status == "success" else "❌"
    print(f"  {status_icon} Status: {BOLD}{result.status}{RESET}")
    print(f"  ⏱️  Duration: {elapsed:.1f}s")
    print(f"  🔗 Correlation ID: {result.correlation_id}")
    print()

    # Actions taken (tool calls audit trail)
    print(f"  {CYAN}Actions taken ({len(result.actions_taken)} tool calls):{RESET}")
    for i, action in enumerate(result.actions_taken, 1):
        print(f"    {i}. {action}")
    print()

    # Report
    if result.status == "success" and result.report:
        report = result.report

        print(f"  {GREEN}── Triage Report ──{RESET}")
        print(f"  Total records:   {report.get('total_records', 'N/A')}")
        print(f"  Auto-requeued:   {BOLD}{report.get('auto_requeued', 0)}{RESET}")
        print(f"  Quarantined:     {report.get('quarantined', 0)}")
        print(f"  Escalated:       {report.get('escalated', 0)}")
        print(f"  Severity:        {BOLD}{report.get('recommended_severity', 'N/A')}{RESET}")
        print()

        # Summary
        print(f"  {YELLOW}Summary:{RESET}")
        print(f"    {report.get('summary', 'N/A')}")
        print()

        # Per-record classifications (the new thing in Phase 3)
        records = report.get("records", [])
        print(f"  {YELLOW}Per-Record Classifications ({len(records)}):{RESET}")
        for j, rec in enumerate(records, 1):
            classification = rec.get("classification", "UNKNOWN")
            confidence = rec.get("confidence", 0.0)
            colored_class = _color_classification(classification)

            print(f"    {j}. {BOLD}{rec.get('record_id', '?')}{RESET}")
            print(f"       Lane:           {rec.get('source_lane', '?')}")
            print(f"       Classification: {colored_class}")
            print(f"       Confidence:     {confidence:.2f}")
            print(f"       Reason:         {rec.get('reason', 'N/A')}")
            print(f"       Action:         {rec.get('action_taken', 'N/A')}")
            print()

        # Notifications
        notifications = report.get("notifications", [])
        print(f"  {YELLOW}Notifications ({len(notifications)}):{RESET}")
        for notif in notifications:
            print(f"    [{notif.get('severity', '?')}] {notif.get('channel', '?')}: {notif.get('title', 'N/A')}")
        print()

    elif result.errors:
        print(f"  {RED}── Errors ──{RESET}")
        for err in result.errors:
            print(f"    ❌ {err}")
        print()

    # Timing
    print(f"  {DIM}Started:   {result.started_at}{RESET}")
    print(f"  {DIM}Completed: {result.completed_at}{RESET}")

    return result


# ---------------------------------------------------------------------------
# Requeue audit — validate safety guardrails
# ---------------------------------------------------------------------------

def _validate_requeue_safety(result) -> None:
    """
    Post-hoc validation of requeue safety.

    Checks that the agent only requeued records it classified as TRANSIENT
    with sufficient confidence. This validates the prompt's safety rules
    AND the tool's idempotency guard worked correctly.

    WHY THIS MATTERS:
      The requeue_message tool has its own safety checks (idempotency,
      rate limit), but the PROMPT ENGINEERING is the first line of defense.
      If the LLM tried to requeue a SCHEMA_MISMATCH record, the tool
      would succeed (it doesn't check classification). The prompt is what
      prevents the LLM from trying in the first place.

      This validation catches cases where the prompt failed to prevent
      unsafe requeues — which would mean the classification rubric or
      safety rules need tightening.
    """
    if result.status != "success" or not result.report:
        return

    records = result.report.get("records", [])
    requeued = [r for r in records if r.get("action_taken") == "requeued"]

    if not requeued:
        print(f"  {DIM}No records were requeued — nothing to validate.{RESET}")
        return

    _separator("REQUEUE SAFETY VALIDATION", CYAN)

    all_safe = True
    for rec in requeued:
        classification = rec.get("classification", "UNKNOWN")
        confidence = rec.get("confidence", 0.0)
        record_id = rec.get("record_id", "?")

        is_transient = classification == "TRANSIENT"
        is_confident = confidence >= 0.80

        if is_transient and is_confident:
            print(f"  ✅ {record_id}: {classification} @ {confidence:.2f} — safe to requeue")
        else:
            all_safe = False
            if not is_transient:
                print(f"  ❌ {record_id}: {classification} was requeued — UNSAFE!")
                print(f"     {RED}Only TRANSIENT records should be requeued!{RESET}")
            else:
                print(f"  ⚠️  {record_id}: {classification} @ {confidence:.2f} — below 0.80 threshold")
                print(f"     {YELLOW}Low-confidence requeues risk retrying permanent failures.{RESET}")

    print()
    if all_safe:
        print(f"  {GREEN}✅ All requeues passed safety validation.{RESET}")
    else:
        print(f"  {RED}❌ SAFETY VIOLATION DETECTED — review the classification rubric.{RESET}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    """Entry point for the DLQ live test."""
    parser = argparse.ArgumentParser(
        description="Argus Phase 3 — DLQ Triage Agent Live Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        choices=["kafka", "bad_files", "both"],
        default="both",
        help="Which DLQ lane to test (default: both)",
    )
    parser.add_argument(
        "--show-tools",
        action="store_true",
        help="Show DLQ tool schemas and exit",
    )
    args = parser.parse_args()

    # ── Header ────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  🔍 ARGUS PHASE 3 — DLQ Triage Agent Live Test")
    print(f"  Full agent with real LLM + simulated DLQ data")
    print(f"{'=' * 60}")

    # ── Show tools if requested ───────────────────────────────────
    if args.show_tools:
        show_tool_schemas()
        return

    # ── Load config and build agent ───────────────────────────────
    _separator("SETUP", CYAN)
    print(f"  Loading config: configs/dev/config.yaml")
    config = load_config("dev")
    print(f"  LLM provider:    {config.llm.get('provider', 'unknown')}")
    print(f"  LLM model:       {config.llm.get('model', 'unknown')}")
    print(f"  Max iterations:  {config.get('agents.dlq_triage.max_iterations', config.get('agents.max_iterations', 10))}")
    print()

    agent = DLQTriageAgent(config)
    print(f"  {GREEN}✓ Agent created: {agent.name}{RESET}")
    print(f"  {DIM}{agent.description}{RESET}")
    print()

    # ── Run scenarios ─────────────────────────────────────────────
    scenarios_to_run = (
        ["kafka", "bad_files"] if args.scenario == "both" else [args.scenario]
    )

    results = {}
    for scenario_key in scenarios_to_run:
        try:
            result = await run_scenario(scenario_key, agent)
            results[scenario_key] = result

            # Validate requeue safety after each scenario
            _validate_requeue_safety(result)

        except Exception as exc:
            print(f"\n  {RED}❌ Scenario {scenario_key} failed: {exc}{RESET}")
            print(f"  {DIM}Make sure GOOGLE_API_KEY is set in your environment.{RESET}\n")
            results[scenario_key] = None

    # ── Summary ───────────────────────────────────────────────────
    _separator("SUMMARY", GREEN)
    for key, result in results.items():
        scenario_name = SCENARIOS[key]["name"]
        if result is None:
            print(f"  ❌ {scenario_name}: CRASHED")
        elif result.status == "success":
            report = result.report
            tool_count = len(result.actions_taken)
            severity = report.get("recommended_severity", "?")
            requeued = report.get("auto_requeued", 0)
            quarantined = report.get("quarantined", 0)
            escalated = report.get("escalated", 0)
            print(f"  ✅ {scenario_name}")
            print(f"     Tools: {tool_count} | Severity: {severity}")
            print(f"     Requeued: {requeued} | Quarantined: {quarantined} | Escalated: {escalated}")
        else:
            print(f"  ❌ {scenario_name}: {result.status}")
            if result.errors:
                print(f"     Errors: {result.errors[0]}")
    print()

    print(f"  {DIM}Next up → Phase 4: Backfill Planning agent (human-in-the-loop) 🚀{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
