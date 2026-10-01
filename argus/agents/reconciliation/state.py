"""
LangGraph state schema for the Reconciliation Diagnostics agent.

WHY THIS FILE MATTERS (learning concepts):
  This TypedDict is the contract between every node in the LangGraph
  StateGraph. When the graph executes, it passes this state dict from
  node to node. Each node reads what it needs, writes what it produces,
  and the framework handles merging via *reducer functions*.

KEY LANGGRAPH CONCEPTS IN THIS FILE:

  1. TypedDict as state — LangGraph uses plain TypedDict (not Pydantic)
     for graph state because it needs shallow-merge semantics. When a
     node returns {"messages": [new_msg]}, LangGraph doesn't replace
     the messages list — it *reduces* it using the annotated function.

  2. Annotated[type, reducer] — the reducer tells LangGraph HOW to
     merge a node's partial return into the accumulated state:
       - operator.add for lists: appends new items to existing list
       - A custom function for anything that needs special logic

  3. Messages list — the core of the ReAct loop. Every LLM call and
     tool result becomes a message. The LLM sees the full conversation
     history to decide its next action. The reducer (operator.add)
     ensures messages accumulate rather than overwrite.

  4. Iteration tracking — prevents runaway loops. The LLM could keep
     calling tools forever; max_iterations is the safety valve.

REDUCER DEEP DIVE:
  Without reducers, returning {"messages": [new_msg]} would REPLACE
  the entire list. With Annotated[list[AnyMessage], operator.add],
  returning {"messages": [new_msg]} APPENDS to the existing list.

  Think of it as:
    state["messages"] = operator.add(state["messages"], node_return["messages"])
    # i.e. state["messages"] = state["messages"] + [new_msg]

  This is why every node that touches messages returns a list, even
  for a single message — operator.add needs list + list.
"""

from __future__ import annotations

import operator
from typing import Annotated, Any

from langchain_core.messages import AnyMessage

from argus.schemas.reports import ReconReport


# ---------------------------------------------------------------------------
# Graph state
# ---------------------------------------------------------------------------

class ReconState:
    """
    State schema for the Reconciliation Diagnostics graph.

    Declared as annotations on a class (LangGraph's dict-state pattern).
    LangGraph reads __annotations__ to build the state channels. Each
    field becomes a "channel" in the graph with its own reducer.

    Flow through the graph:
      1. Entry node seeds: run_date, gate_name, trigger_params, messages
      2. LLM node reads messages → produces AIMessage (with tool_calls)
      3. Tool node executes tool_calls → appends ToolMessages
      4. Router checks: more tools needed? → loop back to LLM
      5. Report node reads full message history → produces ReconReport
    """

    # ── Conversation history (the ReAct backbone) ─────────────────────
    # Every LLM turn (AIMessage) and tool result (ToolMessage) appends
    # here. The SystemMessage and first HumanMessage are seeded by the
    # entry node. operator.add means: accumulate, never overwrite.
    messages: Annotated[list[AnyMessage], operator.add]

    # ── Trigger metadata (set once by entry node, read-only after) ────
    # These come from the TriggerContext that started the agent.
    run_date: str                       # e.g. "2026-09-28"
    gate_name: str                      # "gate_3" or "gate_4"
    trigger_params: dict[str, Any]      # full params from Airflow callback
    correlation_id: str                 # for log tracing

    # ── Loop control ──────────────────────────────────────────────────
    # iteration increments each time the LLM node runs. The conditional
    # edge checks it against max_iterations to prevent infinite loops.
    iteration: int
    max_iterations: int                 # from config: agents.reconciliation.max_iterations

    # ── Final output (set by the report node) ─────────────────────────
    # None until the report node runs. The agent's invoke() method
    # reads this to build the AgentResult.
    report: ReconReport | None

    # ── Error accumulator ─────────────────────────────────────────────
    # Nodes can append errors without halting the graph. The report
    # node includes them in the final output.
    errors: Annotated[list[str], operator.add]


# ---------------------------------------------------------------------------
# Initial state factory
# ---------------------------------------------------------------------------

def make_initial_state(
    run_date: str,
    gate_name: str,
    trigger_params: dict[str, Any],
    correlation_id: str,
    max_iterations: int = 10,
) -> dict[str, Any]:
    """
    Build the seed state dict for graph.invoke().

    Why a factory function instead of constructing the dict inline?
      - Single place to set defaults (iteration=0, report=None)
      - Type-checks the required fields at the call site
      - Easy to extend when we add fields later

    Usage in the agent:
        state = make_initial_state(
            run_date=context.run_date,
            gate_name=context.params.get("gate_failure", "unknown"),
            trigger_params=context.params,
            correlation_id=context.correlation_id,
            max_iterations=config.get("agents.reconciliation.max_iterations", 10),
        )
        result = graph.invoke(state)

    Args:
        run_date: ISO date string from the trigger.
        gate_name: Which gate failed ("gate_3" or "gate_4").
        trigger_params: Full params dict from the Airflow callback.
        correlation_id: Tracing ID for structured logs.
        max_iterations: Safety cap on ReAct loop iterations.

    Returns:
        Dict matching ReconState's shape, ready for graph.invoke().
    """
    return {
        "messages": [],          # entry node will seed with System + Human
        "run_date": run_date,
        "gate_name": gate_name,
        "trigger_params": trigger_params,
        "correlation_id": correlation_id,
        "iteration": 0,
        "max_iterations": max_iterations,
        "report": None,
        "errors": [],
    }
