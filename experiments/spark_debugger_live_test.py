"""
Live integration test for the AI Spark Debugger agent.

PURPOSE:

  This test runs the Spark Debugger against a REAL LLM (Gemini) with
  simulated Spark data. It validates that the full ReAct + Reflection
  loop works end-to-end:

    1. The LLM investigates using Spark tools
    2. The LLM forms a hypothesis about the root cause
    3. The reflection node evaluates and potentially refines the hypothesis
    4. The report node produces a valid SparkDiagnosis

  Unlike the unit tests (which mock the LLM), this test calls the actual
  Gemini API. It requires:
    - GOOGLE_API_KEY environment variable set
    - Network access to Gemini API

TWO TEST SCENARIOS:

  Scenario 1: Data Skew (app-20260928-001)
    - ttag_silver_booking_merge job
    - booking_id='BK-PREMIUM-001' has 398K records
    - Skew ratio 329x on SortMergeJoin stage
    - Expected: agent identifies skew as root cause, recommends salting

  Scenario 2: GC Pressure + Memory Spill (app-20260927-001)
    - ttag_gold_fact_build job
    - 2g executor memory, AQE disabled
    - All executors at 95%+ memory, 27%+ GC
    - Expected: agent identifies systemic memory pressure, recommends
      increasing executor memory and enabling AQE

RUNNING:

  python experiments/spark_debugger_live_test.py

  Optional flags:
    --scenario 1     # run only scenario 1
    --scenario 2     # run only scenario 2
    --verbose        # print full message history

WHAT TO LOOK FOR:

  The Spark Debugger should:
    - Call 4-8 tools per scenario (systematic investigation)
    - Produce at least 1 reflection
    - Identify the correct root cause category
    - Provide specific Spark config recommendations
    - NOT just list symptoms — should trace the causal chain
"""

import argparse
import asyncio
import json
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path (for running from experiments/)
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from argus.agents.base import TriggerContext
from argus.agents.spark_debugger.agent import SparkDebuggerAgent
from argus.core.config import ArgusConfig
from argus.core.logging import setup_logging, set_correlation_id

# ── Color output ──────────────────────────────────────────────────────

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def header(text: str):
    print(f"\n{BOLD}{CYAN}{'='*70}")
    print(f"  {text}")
    print(f"{'='*70}{RESET}\n")


def success(text: str):
    print(f"  {GREEN}✓ {text}{RESET}")


def fail(text: str):
    print(f"  {RED}✗ {text}{RESET}")


def info(text: str):
    print(f"  {YELLOW}→ {text}{RESET}")


# ── Scenarios ─────────────────────────────────────────────────────────

SCENARIOS = {
    1: {
        "name": "Data Skew on Booking Join",
        "app_id": "app-20260928-001",
        "params": {
            "app_id": "app-20260928-001",
            "threshold_minutes": 30,
            "alert_source": "airflow_sla",
        },
        "expected_category": "skew",
        "expected_keywords": ["booking_id", "skew", "partition", "salt"],
        "description": (
            "The ttag_silver_booking_merge job runs 47 minutes (normally 8). "
            "Stage 2 (SortMergeJoin) dominates. One booking_id has 398K "
            "records causing 329x skew."
        ),
    },
    2: {
        "name": "GC Pressure + Memory Spill",
        "app_id": "app-20260927-001",
        "params": {
            "app_id": "app-20260927-001",
            "threshold_minutes": 45,
            "alert_source": "monitoring",
        },
        "expected_category": "gc_pressure",
        "expected_keywords": ["memory", "GC", "executor", "AQE"],
        "description": (
            "The ttag_gold_fact_build job with 2g executor memory and AQE "
            "disabled. All executors at 95%+ memory usage, 27%+ GC overhead. "
            "Systemic resource issue, not skew."
        ),
    },
}


# ── Test runner ───────────────────────────────────────────────────────

async def run_scenario(scenario_num: int, verbose: bool = False):
    """Run one test scenario and validate the diagnosis."""
    scenario = SCENARIOS[scenario_num]

    header(f"Scenario {scenario_num}: {scenario['name']}")
    print(f"  {scenario['description']}\n")

    # Load dev config
    config_path = project_root / "configs" / "dev" / "config.yaml"
    config = ArgusConfig.from_yaml(str(config_path))

    # Create agent and context
    agent = SparkDebuggerAgent(config)
    correlation_id = set_correlation_id(f"live-test-spark-{scenario_num}")

    context = TriggerContext(
        agent_name="spark_debugger",
        trigger_source="cli",
        run_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        correlation_id=correlation_id,
        params=scenario["params"],
    )

    info(f"Invoking agent for app_id={scenario['app_id']}...")
    started = datetime.now(timezone.utc)

    try:
        result = await agent.invoke(context)
    except Exception as exc:
        fail(f"Agent crashed: {exc}")
        return False

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    info(f"Completed in {elapsed:.1f}s\n")

    # ── Validate result ───────────────────────────────────────────

    all_passed = True

    # Check 1: Status
    if result.status == "success":
        success(f"Status: {result.status}")
    else:
        fail(f"Status: {result.status}")
        all_passed = False

    # Check 2: Report exists
    if result.report and "app_id" in result.report:
        success(f"Report generated for app_id={result.report['app_id']}")
    else:
        fail("No report generated")
        all_passed = False
        return all_passed

    # Check 3: Bottlenecks found
    bottlenecks = result.report.get("bottlenecks", [])
    if bottlenecks:
        success(f"Found {len(bottlenecks)} bottleneck(s)")
        for b in bottlenecks:
            category = b.get("category", "unknown")
            impact = b.get("impact", "?")
            info(f"  [{impact}] {category}: {b.get('evidence', '')[:80]}...")
    else:
        fail("No bottlenecks found")
        all_passed = False

    # Check 4: Expected category present
    categories = [b.get("category") for b in bottlenecks]
    if scenario["expected_category"] in categories:
        success(f"Expected category '{scenario['expected_category']}' found")
    else:
        fail(
            f"Expected category '{scenario['expected_category']}' not found "
            f"(got: {categories})"
        )
        all_passed = False

    # Check 5: Root cause summary
    root_cause = result.report.get("root_cause_summary", "")
    if root_cause:
        success(f"Root cause: {root_cause[:100]}...")
    else:
        fail("No root cause summary")
        all_passed = False

    # Check 6: Recommendations
    recommendations = result.report.get("recommendations", [])
    if recommendations:
        success(f"Recommendations: {len(recommendations)} provided")
        for r in recommendations[:3]:
            info(f"  • {r[:80]}")
    else:
        fail("No recommendations")
        all_passed = False

    # Check 7: Expected keywords in root cause or recommendations
    combined_text = (root_cause + " " + " ".join(recommendations)).lower()
    found_keywords = [k for k in scenario["expected_keywords"] if k.lower() in combined_text]
    missing = [k for k in scenario["expected_keywords"] if k.lower() not in combined_text]
    if len(found_keywords) >= 2:
        success(f"Found expected keywords: {found_keywords}")
    else:
        fail(f"Missing expected keywords: {missing}")
        all_passed = False

    # Check 8: Tool usage (should have used multiple tools)
    if len(result.actions_taken) >= 3:
        success(f"Used {len(result.actions_taken)} tools: {result.actions_taken}")
    else:
        fail(f"Too few tools used: {result.actions_taken}")
        all_passed = False

    # Check 9: Reflection metadata
    meta = result.report.get("_meta", {})
    reflections = meta.get("reflections", 0)
    iterations = meta.get("iterations", 0)
    info(f"Iterations: {iterations}, Reflections: {reflections}")

    if reflections > 0:
        success(f"Agent reflected {reflections} time(s)")
    else:
        info("No reflections (agent was confident on first pass)")

    # Check 10: Severity
    severity = result.report.get("recommended_severity", "unknown")
    info(f"Severity: {severity}")

    # ── Verbose output ────────────────────────────────────────────

    if verbose:
        print(f"\n{CYAN}Full report:{RESET}")
        # Remove _meta for cleaner output
        report_clean = {k: v for k, v in result.report.items() if k != "_meta"}
        print(json.dumps(report_clean, indent=2, default=str))

        if meta.get("hypothesis"):
            print(f"\n{CYAN}Final hypothesis:{RESET}")
            print(f"  {meta['hypothesis'][:200]}")

    return all_passed


async def main():
    parser = argparse.ArgumentParser(description="Spark Debugger live test")
    parser.add_argument(
        "--scenario", type=int, choices=[1, 2], default=None,
        help="Run specific scenario (default: both)"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print full report and message history"
    )
    args = parser.parse_args()

    # Check for API key
    if not os.environ.get("GOOGLE_API_KEY"):
        print(f"{RED}Error: GOOGLE_API_KEY environment variable not set.{RESET}")
        print("Set it with: export GOOGLE_API_KEY=your-key-here")
        sys.exit(1)

    setup_logging(level="WARNING")  # suppress debug logs during test

    header("AI Spark Debugger — Live Integration Test")

    scenarios_to_run = [args.scenario] if args.scenario else [1, 2]
    results = {}

    for num in scenarios_to_run:
        passed = await run_scenario(num, verbose=args.verbose)
        results[num] = passed

    # ── Summary ───────────────────────────────────────────────────

    header("Test Summary")
    all_passed = True
    for num, passed in results.items():
        scenario = SCENARIOS[num]
        status = f"{GREEN}PASSED{RESET}" if passed else f"{RED}FAILED{RESET}"
        print(f"  Scenario {num} ({scenario['name']}): {status}")
        if not passed:
            all_passed = False

    print()
    if all_passed:
        print(f"  {GREEN}{BOLD}All tests passed!{RESET}")
    else:
        print(f"  {RED}{BOLD}Some tests failed.{RESET}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
