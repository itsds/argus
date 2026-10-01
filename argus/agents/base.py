"""
Base agent interface for Argus.

Every agent implements this contract:
  - Accepts a TriggerContext
  - Returns a structured AgentResult (report + audit entry)
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from argus.core.config import ArgusConfig


# ---------------------------------------------------------------------------
# Trigger context — what the agent receives when invoked
# ---------------------------------------------------------------------------

class TriggerContext(BaseModel):
    """Context passed to an agent on invocation."""
    agent_name: str
    trigger_source: str  # "airflow_callback" | "cli" | "api"
    run_date: str  # ISO date: "2026-09-30"
    correlation_id: str = ""
    params: dict[str, Any] = Field(default_factory=dict)
    # e.g. {"dag_id": "ttag_main", "task_id": "gate_3", "error": "..."}


# ---------------------------------------------------------------------------
# Agent result — what the agent returns
# ---------------------------------------------------------------------------

class AgentResult(BaseModel):
    """Standard result envelope for every agent."""
    agent_name: str
    correlation_id: str
    status: str  # "success" | "failure" | "needs_approval"
    started_at: datetime
    completed_at: datetime
    report: dict[str, Any]  # agent-specific structured report
    actions_taken: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Base agent class
# ---------------------------------------------------------------------------

class BaseAgent(ABC):
    """
    Abstract base for all Argus agents.

    Subclasses implement:
      - build_graph(): constructs the LangGraph StateGraph
      - invoke(context): runs the agent and returns AgentResult
    """

    def __init__(self, config: ArgusConfig):
        self.config = config
        self._graph = None

    @property
    @abstractmethod
    def name(self) -> str:
        """Agent identifier, e.g. 'backfill', 'dlq_triage'."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """One-line description for logging and routing."""
        ...

    @abstractmethod
    def build_graph(self):
        """Construct and return the LangGraph StateGraph."""
        ...

    @abstractmethod
    async def invoke(self, context: TriggerContext) -> AgentResult:
        """Execute the agent with the given trigger context."""
        ...

    def _make_result(
        self,
        context: TriggerContext,
        status: str,
        report: dict[str, Any],
        started_at: datetime,
        actions: list[str] | None = None,
        errors: list[str] | None = None,
    ) -> AgentResult:
        """Helper to build a standardized AgentResult."""
        return AgentResult(
            agent_name=self.name,
            correlation_id=context.correlation_id,
            status=status,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
            report=report,
            actions_taken=actions or [],
            errors=errors or [],
        )
