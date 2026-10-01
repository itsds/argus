"""
Argus CLI — command-line interface for invoking agents.

Usage:
    argus invoke backfill --run-date 2026-09-30 --params '{"dag_id": "ttag_main"}'
    argus invoke recon --run-date 2026-09-30
    argus list-agents
"""

import json

import click


@click.group()
@click.version_option(package_name="argus")
def cli():
    """Argus - The Hundred-Eyed Watchman."""
    pass


@cli.command()
@click.argument("agent_name")
@click.option("--run-date", required=True, help="ISO date: 2026-09-30")
@click.option("--params", default="{}", help="JSON string of trigger params")
@click.option("--env", default="dev", help="Environment: dev|staging|prod")
def invoke(agent_name: str, run_date: str, params: str, env: str):
    """Invoke an agent by name."""
    import asyncio

    from argus.core.config import load_config
    from argus.core.logging import setup_logging, set_correlation_id, get_logger
    from argus.agents.base import TriggerContext

    config = load_config(env)
    setup_logging(
        level=config.logging.get("level", "INFO"),
        fmt=config.logging.get("format", "json"),
    )
    logger = get_logger("cli")

    cid = set_correlation_id()
    logger.info(
        f"CLI invoke: agent={agent_name}, run_date={run_date}",
        extra={"agent": agent_name, "action": "cli_invoke"},
    )

    context = TriggerContext(
        agent_name=agent_name,
        trigger_source="cli",
        run_date=run_date,
        correlation_id=cid,
        params=json.loads(params),
    )

    # TODO: Build router, register agents, dispatch
    click.echo(f"[Argus] Would invoke '{agent_name}' with context:")
    click.echo(context.model_dump_json(indent=2))
    click.echo("\n[Argus] Agent not yet implemented - skeleton only.")


@cli.command("list-agents")
def list_agents():
    """List all registered agents."""
    agents = [
        ("backfill", "Incident & Backfill Planning (pipeline-aware)"),
        ("dlq_triage", "DLQ Triage & Auto-Remediation (pipeline-aware)"),
        ("reconciliation", "Reconciliation Diagnostics (pipeline-aware)"),
        ("spark_debugger", "AI Spark Debugger (compute-aware)"),
    ]
    click.echo("Registered Agents:")
    for name, desc in agents:
        click.echo(f"  {name:20s} - {desc}")


if __name__ == "__main__":
    cli()
