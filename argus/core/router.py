"""
Agent Router — dispatches trigger context to the right agent(s).

Phase 1: Rules-based (deterministic, no LLM call).
Phase 6: Supervisor agent (LLM-based, can chain multiple agents).
"""

from argus.agents.base import BaseAgent, TriggerContext
from argus.core.logging import get_logger

logger = get_logger("router")


class AgentRouter:
    """
    Routes incoming trigger contexts to the appropriate agent(s).

    Register agents at startup, then call route() with a TriggerContext
    to get back the matching agent.
    """

    def __init__(self):
        self._agents: dict[str, BaseAgent] = {}

    def register(self, agent: BaseAgent) -> None:
        """Register an agent by its name."""
        self._agents[agent.name] = agent
        logger.info(
            f"Registered agent: {agent.name}",
            extra={"agent": agent.name, "action": "register"},
        )

    def route(self, context: TriggerContext) -> BaseAgent:
        """
        Determine which agent should handle this context.

        Rules (Phase 1):
          - context.agent_name is set explicitly -> direct dispatch
          - params contain 'gate_failure' -> reconciliation
          - params contain 'dlq_threshold_breached' -> dlq_triage
          - params contain 'backfill_requested' -> backfill
          - params contain 'spark_app_id' -> spark_debugger
        """
        # Direct dispatch — CLI or API specifies the agent
        if context.agent_name and context.agent_name in self._agents:
            logger.info(
                f"Direct dispatch to: {context.agent_name}",
                extra={"agent": context.agent_name, "action": "route"},
            )
            return self._agents[context.agent_name]

        # Rules-based routing from trigger params
        params = context.params

        if params.get("gate_failure"):
            return self._dispatch("reconciliation", context)

        if params.get("dlq_threshold_breached"):
            return self._dispatch("dlq_triage", context)

        if params.get("backfill_requested"):
            return self._dispatch("backfill", context)

        if params.get("spark_app_id"):
            return self._dispatch("spark_debugger", context)

        raise ValueError(
            f"No agent matched for context: {context.model_dump_json(indent=2)}"
        )

    def _dispatch(self, agent_name: str, context: TriggerContext) -> BaseAgent:
        if agent_name not in self._agents:
            raise ValueError(f"Agent '{agent_name}' not registered")
        logger.info(
            f"Rules-based dispatch to: {agent_name}",
            extra={"agent": agent_name, "action": "route"},
        )
        return self._agents[agent_name]

    @property
    def registered_agents(self) -> list[str]:
        return list(self._agents.keys())
