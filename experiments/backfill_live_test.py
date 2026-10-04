"""
Argus Phase 4 — Backfill Agent Live Test
==========================================
A live integration test that runs the full Incident & Backfill Planning
agent against a real LLM (Gemini free tier) with simulated pipeline data.

This is NOT a unit test — it makes real API calls and costs (free) tokens.
Run this to verify the full TWO-PHASE agent lifecycle works end-to-end:

  TriggerContext → invoke() → needs_approval → resume() → success

WHY THIS FILE EXISTS (learning concepts):

  Unit tests (test_backfill_agent.py) verify structure and plumbing with
  mocked graphs. But mocks can't tell you:

    1. Does the LLM investigate the incident thoroughly enough to produce
       a meaningful BackfillPlan?
    2. Does interrupt() actually pause the graph and return the plan?
    3. Does Command(resume=...) correctly deliver the human's decision
       back into the paused approval_gate node?
    4. Can the LLM revise a plan after rejection feedback, or does it
       just repeat the same plan?
    5. Does the execution phase correctly lock, execute steps, and
       release in order?

  This live test answers those questions. It tests the COMPLETE lifecycle:

    invoke() → review plan → resume(approved) → verify execution
    invoke() → review plan → resume(rejected, feedback) → review revised plan → approve

WHAT'S DIFFERENT FROM RECON LIVE TEST:

  The Recon live test (experiments/recon_live_test.py) tests a single
  invoke() → result pattern. The Backfill live test has to test:

  1. TWO-PHASE LIFECYCLE — invoke() returns "needs_approval", then
     resume() completes the work. This means we need to verify the
     plan BETWEEN phases.

  2. REJECTION LOOP — we intentionally reject the first plan to test
     whether the LLM can revise based on feedback. This is the key
     differentiator of the HITL pattern.

  3. EXECUTION AUDIT — after approval, we verify the execution phase
     produced a meaningful audit trail (lock → steps → release).

  4. PLAN QUALITY — we inspect the BackfillPlan structure to verify
     the LLM produced reasonable steps, not just any valid Pydantic model.

SCENARIOS:

  1. APPROVE FIRST PLAN — invoke → review → approve → verify execution
     Tests the happy path through the two-phase lifecycle.

  2. REJECT THEN APPROVE — invoke → review → reject with feedback →
     review revised plan → approve → verify execution
     Tests the rejection loop and plan revision quality.

  3. SHOW TOOLS — displays the tool schemas the LLM sees, including
     both investigation and execution tools.

PREREQUISITES:
  - GOOGLE_API_KEY must be set (Gemini free tier — no cost)
  - Phase 4 graph.py and tools must be built:
      argus/agents/backfill/graph.py
      argus/tools/pipeline/backfill_investigation_tools.py
      argus/tools/pipeline/backfill_execution_tools.py
  - Run from repo root: python experiments/backfill_live_test.py

USAGE:
  # Run the approve-first scenario (default):
  python experiments/backfill_live_test.py

  # Run the rejection loop scenario:
  python experiments/backfill_live_test.py --scenario reject-then-approve

  # Run both scenarios:
  python experiments/backfill_live_test.py --scenario both

  # Show tool schemas (what the LLM sees):
  python experiments/backfill_live_test.py --show-tools
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

# ── Ensure repo root is on sys.path ──────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from argus.agents.base import TriggerContext
from argus.agents.backfill.agent import BackfillAgent
from argus.core.config import load_config


# ---------------------------------------------------------------------------
# ANSI colors — same style as recon_live_test.py
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
# Scenario contexts
# ---------------------------------------------------------------------------
# Each scenario simulates a different incident. The simulated data in the
# backfill investigation tools provides the responses the LLM investigates.

INCIDENT_CONTEXT = TriggerContext(
    agent_name="backfill",
    trigger_source="live_test",
    run_date="2026-09-28",
    correlation_id="live-test-backfill-001",
    params={
        "incident_id": "INC-2026-0928",
        "dag_id": "ttag_main",
        "error": "Gate 3 failure — 847 missing booking records in Silver",
        "source": "recon_agent_recommendation",
    },
)


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def _print_plan(plan: dict, label: str = "PLAN") -> None:
    """
    Pretty-print a BackfillPlan dict.

    This is the human review step in the HITL flow. In production, this
    would be a UI or Slack message. Here we print it so you can see
    exactly what the LLM produced.
    """
    _separator(f"📋 {label} FOR REVIEW", MAGENTA)

    print(f"  {BOLD}Incident Summary:{RESET}")
    print(f"    {plan.get('incident_summary', 'N/A')}")
    print()

    print(f"  {BOLD}Root Cause:{RESET}")
    print(f"    {plan.get('root_cause', 'N/A')}")
    print()

    print(f"  {BOLD}Affected Partitions:{RESET}")
    for partition in plan.get("affected_partitions", []):
        print(f"    • {partition}")
    print()

    print(f"  {BOLD}Risk Assessment:{RESET}")
    print(f"    {plan.get('risk_assessment', 'N/A')}")
    print()

    print(f"  {BOLD}Estimated Duration:{RESET}")
    print(f"    {plan.get('estimated_duration_minutes', 'N/A')} minutes")
    print()

    print(f"  {BOLD}Recommended Severity:{RESET}")
    print(f"    {plan.get('recommended_severity', 'N/A')}")
    print()

    # Proposed steps
    steps = plan.get("proposed_steps", [])
    print(f"  {BOLD}Proposed Steps ({len(steps)}):{RESET}")
    for step in steps:
        order = step.get("order", "?")
        desc = step.get("description", "N/A")
        lock = "🔒" if step.get("requires_lock") else "  "
        approval = "👤" if step.get("requires_approval") else "  "
        print(f"    {lock}{approval} Step {order}: {desc}")

        if step.get("watermark_key"):
            print(f"          Watermark: {step['watermark_key']}")
        if step.get("snapshot_range"):
            print(f"          Snapshot:  {step['snapshot_range']}")
        if step.get("collision_check"):
            print(f"          Check:    {step['collision_check']}")
    print()

    # Notifications
    notifications = plan.get("notifications", [])
    if notifications:
        print(f"  {BOLD}Notifications ({len(notifications)}):{RESET}")
        for notif in notifications:
            sev = notif.get("severity", "?")
            channel = notif.get("channel", "?")
            title = notif.get("title", "N/A")
            print(f"    [{sev}] {channel}: {title}")
        print()


def _print_execution_audit(audit: list[str]) -> None:
    """Pretty-print the execution audit trail."""
    _separator("📝 EXECUTION AUDIT", GREEN)
    for i, entry in enumerate(audit, 1):
        # Color-code by action type
        if "LOCK ACQUIRED" in entry:
            icon = "🔒"
            color = YELLOW
        elif "LOCK RELEASED" in entry:
            icon = "🔓"
            color = YELLOW
        elif "SUCCESS" in entry:
            icon = "✅"
            color = GREEN
        elif "FAILED" in entry or "ERROR" in entry:
            icon = "❌"
            color = RED
        else:
            icon = "  "
            color = DIM
        print(f"  {icon} {color}{entry}{RESET}")
    print()


# ---------------------------------------------------------------------------
# Scenario 1: Approve first plan
# ---------------------------------------------------------------------------

async def run_approve_first(agent: BackfillAgent) -> dict | None:
    """
    Test the happy path: invoke → approve → success.

    This tests:
    1. Investigation phase produces a meaningful plan
    2. interrupt() correctly pauses the graph
    3. resume("approved") runs execution to completion
    4. Execution audit trail has lock → steps → release
    """
    _separator("SCENARIO: Approve First Plan", BLUE)
    print(f"  Flow: invoke() → review plan → resume(approved) → success\n")
    print(f"  {DIM}This tests the happy path through the two-phase lifecycle.{RESET}")
    print(f"  {DIM}The LLM investigates, plans, we approve, it executes.{RESET}\n")

    context = INCIDENT_CONTEXT

    print(f"  {DIM}Incident:       {context.params.get('incident_id', 'N/A')}{RESET}")
    print(f"  {DIM}Run date:       {context.run_date}{RESET}")
    print(f"  {DIM}Error:          {context.params.get('error', 'N/A')}{RESET}")
    print(f"  {DIM}Correlation ID: {context.correlation_id}{RESET}")
    print()

    # ── Phase 1: Invoke (investigation + planning) ────────────────
    print(f"  {YELLOW}▶ Phase 1: Invoking BackfillAgent (investigate + plan)...{RESET}")
    print(f"  {DIM}(Real LLM calls — watch for investigation tool usage){RESET}\n")

    start_time = time.monotonic()
    result = await agent.invoke(context)
    phase1_time = time.monotonic() - start_time

    print(f"  {CYAN}Phase 1 completed in {phase1_time:.1f}s{RESET}")
    print(f"  Status: {BOLD}{result.status}{RESET}")
    print(f"  Tools used: {', '.join(result.actions_taken) or 'none'}")
    print()

    if result.status != "needs_approval":
        print(f"  {RED}❌ Expected 'needs_approval' but got '{result.status}'{RESET}")
        if result.errors:
            for err in result.errors:
                print(f"     Error: {err}")
        return result

    # ── Review the plan ──────────────────────────────────────────
    _print_plan(result.report, "INITIAL PLAN")

    # ── Validate plan structure ──────────────────────────────────
    plan = result.report
    _separator("PLAN VALIDATION", CYAN)

    checks = {
        "Has incident summary": bool(plan.get("incident_summary")),
        "Has root cause": bool(plan.get("root_cause")),
        "Has affected partitions": len(plan.get("affected_partitions", [])) > 0,
        "Has proposed steps": len(plan.get("proposed_steps", [])) > 0,
        "Has risk assessment": bool(plan.get("risk_assessment")),
        "Has estimated duration": plan.get("estimated_duration_minutes") is not None,
        "Steps are ordered": all(
            s.get("order") == i + 1
            for i, s in enumerate(plan.get("proposed_steps", []))
        ) if plan.get("proposed_steps") else False,
    }

    all_valid = True
    for check_name, passed in checks.items():
        icon = "✅" if passed else "❌"
        print(f"  {icon} {check_name}")
        if not passed:
            all_valid = False

    if not all_valid:
        print(f"\n  {YELLOW}⚠ Some plan quality checks failed — LLM may need prompt tuning{RESET}")
    print()

    # ── Phase 2: Resume with approval ─────────────────────────────
    print(f"  {YELLOW}▶ Phase 2: Resuming with approval (execute plan)...{RESET}")
    print(f"  {DIM}(Real LLM calls — watch for lock/execute/release){RESET}\n")

    phase2_start = time.monotonic()
    result = await agent.resume("approved")
    phase2_time = time.monotonic() - phase2_start

    print(f"  {CYAN}Phase 2 completed in {phase2_time:.1f}s{RESET}")
    print(f"  Status: {BOLD}{result.status}{RESET}")
    print(f"  Tools used: {', '.join(result.actions_taken) or 'none'}")
    print()

    # ── Display execution audit ───────────────────────────────────
    if result.status == "success" and result.report:
        execution_audit = result.report.get("execution_audit", [])
        if execution_audit:
            _print_execution_audit(execution_audit)

        # Verify plan is also in the success report
        if result.report.get("plan"):
            print(f"  {GREEN}✅ Final report includes approved plan{RESET}")
        else:
            print(f"  {YELLOW}⚠ Final report missing plan{RESET}")
    elif result.errors:
        print(f"  {RED}── Errors ──{RESET}")
        for err in result.errors:
            print(f"    ❌ {err}")

    # ── Total timing ──────────────────────────────────────────────
    total_time = phase1_time + phase2_time
    _separator("TIMING", DIM)
    print(f"  Phase 1 (investigate + plan): {phase1_time:.1f}s")
    print(f"  Phase 2 (execute):            {phase2_time:.1f}s")
    print(f"  Total:                        {total_time:.1f}s")
    print()

    return result


# ---------------------------------------------------------------------------
# Scenario 2: Reject then approve
# ---------------------------------------------------------------------------

async def run_reject_then_approve(agent: BackfillAgent) -> dict | None:
    """
    Test the rejection loop: invoke → reject → review revised → approve.

    This tests:
    1. resume("rejected", feedback) delivers feedback to the LLM
    2. The LLM revises the plan based on the feedback
    3. The revised plan is DIFFERENT from the original (not a copy)
    4. After approving the revised plan, execution completes normally
    """
    _separator("SCENARIO: Reject Then Approve", BLUE)
    print(f"  Flow: invoke() → reject + feedback → resume(rejected)")
    print(f"        → review revised plan → resume(approved) → success\n")
    print(f"  {DIM}This tests the rejection loop — the HITL differentiator.{RESET}")
    print(f"  {DIM}Can the LLM actually incorporate rejection feedback?{RESET}\n")

    context = TriggerContext(
        agent_name="backfill",
        trigger_source="live_test",
        run_date="2026-09-28",
        correlation_id="live-test-backfill-reject-001",
        params={
            "incident_id": "INC-2026-0928",
            "dag_id": "ttag_main",
            "error": "Gate 3 failure — 847 missing booking records in Silver",
            "source": "recon_agent_recommendation",
        },
    )

    # ── Phase 1: Invoke ───────────────────────────────────────────
    print(f"  {YELLOW}▶ Phase 1: Invoking BackfillAgent...{RESET}\n")

    start_time = time.monotonic()
    result = await agent.invoke(context)
    phase1_time = time.monotonic() - start_time

    print(f"  {CYAN}Phase 1 completed in {phase1_time:.1f}s — Status: {result.status}{RESET}\n")

    if result.status != "needs_approval":
        print(f"  {RED}❌ Expected 'needs_approval' — got '{result.status}'{RESET}")
        return result

    # ── Review and REJECT with feedback ──────────────────────────
    original_plan = result.report
    _print_plan(original_plan, "ORIGINAL PLAN (will reject)")

    rejection_feedback = (
        "Add explicit rollback steps for each backfill step in case of failure. "
        "Also add a data validation step after execution to verify row counts match."
    )

    print(f"  {RED}✋ REJECTING with feedback:{RESET}")
    print(f"  {DIM}\"{rejection_feedback}\"{RESET}\n")

    # ── Phase 2a: Resume with rejection ───────────────────────────
    print(f"  {YELLOW}▶ Phase 2a: Resuming with rejection...{RESET}\n")

    phase2a_start = time.monotonic()
    result = await agent.resume("rejected", rejection_feedback)
    phase2a_time = time.monotonic() - phase2a_start

    print(f"  {CYAN}Phase 2a completed in {phase2a_time:.1f}s — Status: {result.status}{RESET}\n")

    if result.status == "needs_approval":
        # ── Review the REVISED plan ──────────────────────────────
        revised_plan = result.report
        _print_plan(revised_plan, "REVISED PLAN (after rejection)")

        # ── Compare original vs revised ──────────────────────────
        _separator("PLAN COMPARISON", CYAN)

        orig_steps = len(original_plan.get("proposed_steps", []))
        rev_steps = len(revised_plan.get("proposed_steps", []))
        print(f"  Original steps: {orig_steps}")
        print(f"  Revised steps:  {rev_steps}")
        if rev_steps > orig_steps:
            print(f"  {GREEN}✅ Revised plan has more steps (incorporated feedback){RESET}")
        elif rev_steps == orig_steps:
            print(f"  {YELLOW}⚠ Same number of steps — check if content changed{RESET}")
        else:
            print(f"  {RED}⚠ Fewer steps — LLM may have simplified instead of expanding{RESET}")

        # Check if risk assessment changed (feedback should affect it)
        orig_risk = original_plan.get("risk_assessment", "")
        rev_risk = revised_plan.get("risk_assessment", "")
        if orig_risk != rev_risk:
            print(f"  {GREEN}✅ Risk assessment updated{RESET}")
        else:
            print(f"  {YELLOW}⚠ Risk assessment unchanged{RESET}")
        print()

        # ── Phase 2b: Approve the revised plan ───────────────────
        print(f"  {YELLOW}▶ Phase 2b: Approving revised plan...{RESET}\n")

        phase2b_start = time.monotonic()
        result = await agent.resume("approved")
        phase2b_time = time.monotonic() - phase2b_start

        print(f"  {CYAN}Phase 2b completed in {phase2b_time:.1f}s — Status: {result.status}{RESET}\n")

        if result.status == "success" and result.report:
            execution_audit = result.report.get("execution_audit", [])
            if execution_audit:
                _print_execution_audit(execution_audit)

        total_time = phase1_time + phase2a_time + phase2b_time
        _separator("TIMING", DIM)
        print(f"  Phase 1  (investigate + plan):   {phase1_time:.1f}s")
        print(f"  Phase 2a (rejection → revise):   {phase2a_time:.1f}s")
        print(f"  Phase 2b (approve → execute):    {phase2b_time:.1f}s")
        print(f"  Total:                           {total_time:.1f}s")
        print()

    elif result.status == "failure":
        print(f"  {RED}❌ Agent failed after rejection (possibly max revisions){RESET}")
        if result.errors:
            for err in result.errors:
                print(f"     Error: {err}")
    else:
        print(f"  {YELLOW}⚠ Unexpected status after rejection: {result.status}{RESET}")

    return result


# ---------------------------------------------------------------------------
# Show tool schemas
# ---------------------------------------------------------------------------

def show_tool_schemas() -> None:
    """
    Print the tool schemas that get bound to the LLM.

    The Backfill agent uses TWO sets of tools (unlike Recon which has one):
      1. Investigation tools — for understanding the incident
      2. Execution tools — for carrying out the approved plan

    These are bound to different LLM nodes in the graph:
      - investigation_llm gets investigation tools
      - execution_llm gets execution tools
    """
    _separator("BACKFILL TOOL SCHEMAS (what the LLM sees)", CYAN)

    try:
        # Try to import both tool sets
        # These may not exist yet if you're building Phase 4 incrementally.
        from argus.tools.pipeline.backfill_investigation_tools import (
            INVESTIGATION_TOOLS,
        )
        print(f"  {BOLD}Investigation Tools:{RESET}")
        for i, tool_fn in enumerate(INVESTIGATION_TOOLS, 1):
            print(f"    {i}. {tool_fn.name}")
            desc = tool_fn.description or ""
            print(f"       {DIM}{desc[:80]}{'...' if len(desc) > 80 else ''}{RESET}")
            if tool_fn.args_schema:
                schema = tool_fn.args_schema.model_json_schema()
                props = schema.get("properties", {})
                for param_name, param_info in props.items():
                    param_type = param_info.get("type", "any")
                    print(f"       → {param_name}: {param_type}")
            print()
    except ImportError:
        print(f"  {YELLOW}⚠ Investigation tools not yet built{RESET}")
        print(f"  {DIM}Build: argus/tools/pipeline/backfill_investigation_tools.py{RESET}\n")

    try:
        from argus.tools.pipeline.backfill_execution_tools import (
            EXECUTION_TOOLS,
        )
        print(f"  {BOLD}Execution Tools:{RESET}")
        for i, tool_fn in enumerate(EXECUTION_TOOLS, 1):
            print(f"    {i}. {tool_fn.name}")
            desc = tool_fn.description or ""
            print(f"       {DIM}{desc[:80]}{'...' if len(desc) > 80 else ''}{RESET}")
            if tool_fn.args_schema:
                schema = tool_fn.args_schema.model_json_schema()
                props = schema.get("properties", {})
                for param_name, param_info in props.items():
                    param_type = param_info.get("type", "any")
                    print(f"       → {param_name}: {param_type}")
            print()
    except ImportError:
        print(f"  {YELLOW}⚠ Execution tools not yet built{RESET}")
        print(f"  {DIM}Build: argus/tools/pipeline/backfill_execution_tools.py{RESET}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    """Entry point for the live test."""
    parser = argparse.ArgumentParser(
        description="Argus Phase 4 — Backfill Agent Live Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        choices=["approve-first", "reject-then-approve", "both"],
        default="approve-first",
        help="Which scenario to run (default: approve-first)",
    )
    parser.add_argument(
        "--show-tools",
        action="store_true",
        help="Show tool schemas and exit",
    )
    args = parser.parse_args()

    # ── Header ────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  🔧 ARGUS PHASE 4 — Backfill Agent Live Test")
    print(f"  Two-phase lifecycle: investigate → plan → approve → execute")
    print(f"{'=' * 60}")

    # ── Show tools if requested ───────────────────────────────────
    if args.show_tools:
        show_tool_schemas()
        return

    # ── Preflight check ───────────────────────────────────────────
    # Verify that Phase 4 graph.py exists before trying to run.
    graph_path = REPO_ROOT / "argus" / "agents" / "backfill" / "graph.py"
    if not graph_path.exists():
        _separator("PREFLIGHT CHECK FAILED", RED)
        print(f"  {RED}❌ Missing: argus/agents/backfill/graph.py{RESET}")
        print(f"  {DIM}The Backfill graph hasn't been built yet.{RESET}")
        print(f"  {DIM}Build the following Phase 4 files first:{RESET}")
        print(f"    1. argus/agents/backfill/graph.py")
        print(f"    2. argus/tools/pipeline/backfill_investigation_tools.py")
        print(f"    3. argus/tools/pipeline/backfill_execution_tools.py")
        print(f"  {DIM}Then re-run this test.{RESET}\n")
        return

    # ── Load config and build agent ───────────────────────────────
    _separator("SETUP", CYAN)
    print(f"  Loading config: configs/dev/config.yaml")

    try:
        config = load_config("dev")
    except Exception as exc:
        print(f"  {RED}❌ Config load failed: {exc}{RESET}")
        print(f"  {DIM}Make sure configs/dev/config.yaml exists.{RESET}\n")
        return

    print(f"  LLM provider:         {config.llm.get('provider', 'unknown')}")
    print(f"  LLM model:            {config.llm.get('model', 'unknown')}")
    print(f"  Max iterations:       {config.get('agents.backfill.max_iterations', 15)}")
    print(f"  Max plan iterations:  {config.get('agents.backfill.max_plan_iterations', 3)}")
    print()

    agent = BackfillAgent(config)
    print(f"  {GREEN}✓ Agent created: {agent.name}{RESET}")
    print(f"  {DIM}{agent.description}{RESET}")
    print()

    # ── Run scenarios ─────────────────────────────────────────────
    scenarios = {
        "approve-first": run_approve_first,
        "reject-then-approve": run_reject_then_approve,
    }

    scenarios_to_run = (
        ["approve-first", "reject-then-approve"]
        if args.scenario == "both"
        else [args.scenario]
    )

    results = {}
    for scenario_key in scenarios_to_run:
        try:
            result = await scenarios[scenario_key](agent)
            results[scenario_key] = result
        except Exception as exc:
            print(f"\n  {RED}❌ Scenario '{scenario_key}' crashed: {exc}{RESET}")
            print(f"  {DIM}Make sure GOOGLE_API_KEY is set.{RESET}")
            import traceback
            traceback.print_exc()
            results[scenario_key] = None

    # ── Summary ───────────────────────────────────────────────────
    _separator("SUMMARY", GREEN)

    for key, result in results.items():
        label = key.replace("-", " ").title()
        if result is None:
            print(f"  ❌ {label}: CRASHED")
        elif result.status == "success":
            tool_count = len(result.actions_taken)
            audit_count = len(result.report.get("execution_audit", []))
            print(f"  ✅ {label}")
            print(f"     Tools: {tool_count} | Audit entries: {audit_count}")
        elif result.status == "needs_approval":
            print(f"  ⏸️  {label}: Still waiting for approval (unexpected)")
        else:
            print(f"  ❌ {label}: {result.status}")
            if result.errors:
                print(f"     Error: {result.errors[0]}")
    print()

    # ── Agent state check ─────────────────────────────────────────
    print(f"  {DIM}Agent state after all scenarios:{RESET}")
    print(f"  {DIM}  has_pending_approval: {agent.has_pending_approval}{RESET}")
    print(f"  {DIM}  _thread_id:          {agent._thread_id}{RESET}")
    if not agent.has_pending_approval:
        print(f"  {GREEN}✓ Agent cleaned up — ready for new invoke(){RESET}")
    else:
        print(f"  {YELLOW}⚠ Agent still has pending state — cleanup may have failed{RESET}")
    print()

    print(f"  {DIM}Phase 4 two-phase lifecycle test complete! 🚀{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
