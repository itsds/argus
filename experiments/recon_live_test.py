"""
Argus Phase 2 — Reconciliation Agent Live Test
================================================
A live integration test that runs the full Reconciliation Diagnostics
agent against a real LLM (Gemini free tier) with simulated pipeline data.

This is NOT a unit test — it makes real API calls and costs (free) tokens.
Run this to verify the full agent pipeline works end-to-end:

  TriggerContext → ReconciliationAgent.invoke() → ReAct loop → ReconReport

WHY THIS FILE EXISTS (learning concepts):

  Unit tests (test_recon_agent.py) verify structure and plumbing with
  mocked LLMs. But mocks can't tell you:

    1. Does the LLM understand the system prompt well enough to use
       the right tools in the right order?
    2. Does the ReAct loop actually converge to a report, or does it
       spin endlessly?
    3. Does .with_structured_output() produce a valid ReconReport
       from the LLM's raw output?
    4. Do the tool docstrings guide the LLM correctly?

  This live test answers those questions. It uses the same simulated
  data store as the unit tests (no real pipeline access needed), but
  the LLM reasoning is REAL.

KEY CONCEPTS:

  1. Integration test vs unit test — this tests the full system
     including the LLM, not individual components.

  2. Two scenarios — the simulated data has two built-in failure modes:
       - 2026-09-28 Gate 3: Bronze→Silver row count mismatch (duplicates)
       - 2026-09-27 Gate 4: Gold→Snowflake count mismatch (NULL FKs)
     Running both validates the agent handles different failure types.

  3. Observability — this test prints every step of the ReAct loop
     so you can watch the agent reason. Similar to calculator_agent.py
     but using the full Argus platform stack.

REQUIREMENTS:
  - GOOGLE_API_KEY must be set (Gemini free tier — no cost)
  - Run from repo root: python experiments/recon_live_test.py

USAGE:
  # Run both scenarios (default):
  python experiments/recon_live_test.py

  # Run a specific scenario:
  python experiments/recon_live_test.py --scenario gate3
  python experiments/recon_live_test.py --scenario gate4

  # Show tool schemas (what the LLM sees):
  python experiments/recon_live_test.py --show-tools
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

# ── Ensure repo root is on sys.path ──────────────────────────────────
# When running as `python experiments/recon_live_test.py`, the repo root
# isn't automatically on sys.path. We add it so `from argus.xxx` works.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from argus.agents.base import TriggerContext
from argus.agents.reconciliation.agent import ReconciliationAgent
from argus.core.config import load_config
from argus.tools.pipeline.recon_tools import RECON_TOOLS


# ---------------------------------------------------------------------------
# ANSI colors — same style as calculator_agent.py
# ---------------------------------------------------------------------------

BOLD   = "\033[1m"
DIM    = "\033[2m"
GREEN  = "\033[32m"
BLUE   = "\033[34m"
CYAN   = "\033[36m"
YELLOW = "\033[33m"
RED    = "\033[31m"
RESET  = "\033[0m"


def _separator(label: str, color: str = GREEN) -> None:
    """Print a separator line with a label."""
    print(f"\n{color}{'─' * 60}")
    print(f"  {label}")
    print(f"{'─' * 60}{RESET}\n")


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------
# These map to the simulated data in recon_tools.py.
# Each scenario exercises a different investigation path.

SCENARIOS = {
    "gate3": {
        "name": "Gate 3 — Bronze→Silver Row Count Mismatch",
        "description": (
            "Gate 3 pre-Gold reconciliation failed on 2026-09-28.\n"
            "  Bronze booking has 15012 rows but Silver has only 14712.\n"
            "  Root cause: 111 duplicate booking_ids from upstream re-delivery.\n"
            "  Expected tools: query_gate_results → compare_row_counts → check_duplicate_keys"
        ),
        "context": TriggerContext(
            agent_name="reconciliation",
            trigger_source="live_test",
            run_date="2026-09-28",
            correlation_id="live-test-gate3-001",
            params={
                "gate_failure": "gate_3",
                "dag_id": "ttag_daily_dag",
            },
        ),
    },
    "gate4": {
        "name": "Gate 4 — Gold→Snowflake Count Mismatch",
        "description": (
            "Gate 4 post-Gold check failed on 2026-09-27.\n"
            "  Gold Iceberg has 15102 rows but Snowflake FACT has 15087.\n"
            "  Root cause: 15 rows have NULL card_sk (missing dimension keys).\n"
            "  Expected tools: query_gate_results → compare_row_counts → check_fk_integrity"
        ),
        "context": TriggerContext(
            agent_name="reconciliation",
            trigger_source="live_test",
            run_date="2026-09-27",
            correlation_id="live-test-gate4-001",
            params={
                "gate_failure": "gate_4",
                "dag_id": "ttag_daily_dag",
            },
        ),
    },
}


# ---------------------------------------------------------------------------
# Tool schema display — shows what the LLM "sees"
# ---------------------------------------------------------------------------

def show_tool_schemas() -> None:
    """Print the tool schemas that get bound to the LLM."""
    _separator("RECON TOOL SCHEMAS (what the LLM sees)", CYAN)

    for i, tool_fn in enumerate(RECON_TOOLS, 1):
        print(f"  {BOLD}{i}. {tool_fn.name}{RESET}")
        print(f"     {DIM}{tool_fn.description[:100]}...{RESET}")

        # Show the input schema (this is what .bind_tools() sends to the LLM)
        schema = tool_fn.args_schema.model_json_schema() if tool_fn.args_schema else {}
        props = schema.get("properties", {})
        for param_name, param_info in props.items():
            param_type = param_info.get("type", "any")
            print(f"     → {param_name}: {param_type}")
        print()

    print(f"  {DIM}Total tools: {len(RECON_TOOLS)}{RESET}")
    print(f"  {DIM}These are bound via llm.bind_tools(RECON_TOOLS){RESET}\n")


# ---------------------------------------------------------------------------
# Run a single scenario
# ---------------------------------------------------------------------------

async def run_scenario(
    scenario_key: str,
    agent: ReconciliationAgent,
) -> dict:
    """
    Run one scenario and print the results.

    Returns:
        The AgentResult as a dict for inspection.
    """
    scenario = SCENARIOS[scenario_key]
    context = scenario["context"]

    _separator(f"SCENARIO: {scenario['name']}", BLUE)
    print(f"  {scenario['description']}\n")
    print(f"  {DIM}Run date:       {context.run_date}{RESET}")
    print(f"  {DIM}Gate failure:   {context.params.get('gate_failure', 'unknown')}{RESET}")
    print(f"  {DIM}Correlation ID: {context.correlation_id}{RESET}")
    print(f"  {DIM}Trigger source: {context.trigger_source}{RESET}")
    print()

    # ── Run the agent ─────────────────────────────────────────────
    print(f"  {YELLOW}▶ Invoking ReconciliationAgent...{RESET}")
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

        print(f"  {GREEN}── Report ──{RESET}")
        print(f"  Gate failed:    {report.get('gate_failed', 'N/A')}")
        print(f"  Run date:       {report.get('run_date', 'N/A')}")
        print(f"  Severity:       {BOLD}{report.get('recommended_severity', 'N/A')}{RESET}")
        print()

        print(f"  {YELLOW}Root cause:{RESET}")
        print(f"    {report.get('root_cause_summary', 'N/A')}")
        print()

        print(f"  {YELLOW}Suggested fix:{RESET}")
        print(f"    {report.get('suggested_fix', 'N/A')}")
        print()

        # Findings
        findings = report.get("findings", [])
        print(f"  {YELLOW}Findings ({len(findings)}):{RESET}")
        for j, finding in enumerate(findings, 1):
            print(f"    {j}. {finding.get('check_name', 'unknown')}")
            print(f"       Table:    {finding.get('table', 'N/A')}")
            print(f"       Expected: {finding.get('expected', 'N/A')}")
            print(f"       Actual:   {finding.get('actual', 'N/A')}")
            print(f"       Delta:    {finding.get('delta', 'N/A')}")
            if finding.get("possible_cause"):
                print(f"       Cause:    {finding['possible_cause']}")
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
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    """Entry point for the live test."""
    parser = argparse.ArgumentParser(
        description="Argus Phase 2 — Reconciliation Agent Live Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        choices=["gate3", "gate4", "both"],
        default="both",
        help="Which scenario to run (default: both)",
    )
    parser.add_argument(
        "--show-tools",
        action="store_true",
        help="Show tool schemas and exit",
    )
    args = parser.parse_args()

    # ── Header ────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  🔍 ARGUS PHASE 2 — Reconciliation Agent Live Test")
    print(f"  Full agent with real LLM + simulated pipeline data")
    print(f"{'=' * 60}")

    # ── Show tools if requested ───────────────────────────────────
    if args.show_tools:
        show_tool_schemas()
        return

    # ── Load config and build agent ───────────────────────────────
    _separator("SETUP", CYAN)
    print(f"  Loading config: configs/dev/config.yaml")
    config = load_config("dev")
    print(f"  LLM provider:   {config.llm.get('provider', 'unknown')}")
    print(f"  LLM model:      {config.llm.get('model', 'unknown')}")
    print(f"  Max iterations:  {config.get('agents.reconciliation.max_iterations', 10)}")
    print()

    agent = ReconciliationAgent(config)
    print(f"  {GREEN}✓ Agent created: {agent.name}{RESET}")
    print(f"  {DIM}{agent.description}{RESET}")
    print()

    # ── Run scenarios ─────────────────────────────────────────────
    scenarios_to_run = (
        ["gate3", "gate4"] if args.scenario == "both" else [args.scenario]
    )

    results = {}
    for scenario_key in scenarios_to_run:
        try:
            result = await run_scenario(scenario_key, agent)
            results[scenario_key] = result
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
            tool_count = len(result.actions_taken)
            severity = result.report.get("recommended_severity", "?")
            print(f"  ✅ {scenario_name}")
            print(f"     Tools used: {tool_count} | Severity: {severity}")
        else:
            print(f"  ❌ {scenario_name}: {result.status}")
            if result.errors:
                print(f"     Errors: {result.errors[0]}")
    print()

    print(f"  {DIM}Next up → Phase 3: DLQ Triage agent 🚀{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
