"""
LangGraph StateGraph for the Reconciliation Diagnostics agent.

WHY THIS FILE MATTERS (learning concepts):

  This is where the Reconciliation agent comes alive. The previous files
  defined the PIECES — tools, state, prompts — and this file WIRES them
  into a running graph. Understanding this file means understanding how
  LangGraph turns a collection of functions into an autonomous agent.

KEY LANGGRAPH CONCEPTS IN THIS FILE:

  1. StateGraph — a directed graph where nodes are functions that read
     and write to a shared state dict. Edges connect nodes. Conditional
     edges let the graph branch based on state values.

  2. Nodes — pure functions that take state (dict) and return a partial
     state update (dict). LangGraph merges the return into the accumulated
     state using reducers. A node that returns {"messages": [new_msg]}
     doesn't replace messages — operator.add APPENDS it.

  3. Conditional edges — routing decisions. After the LLM node runs, the
     graph checks: did the LLM call tools? → go to tool node. No tools
     (or max iterations hit)? → go to report node. This is the ReAct loop.

  4. ToolNode — LangGraph's prebuilt node that automatically executes
     tool calls from AIMessages. It reads the last AIMessage's tool_calls,
     runs each tool function, and returns ToolMessages with results.
     You DON'T need to write tool execution logic yourself.

  5. Closures for dependency injection — the LLM model is created from
     config at build time, then captured in closures so each node function
     has access to it without global state.

  6. .bind_tools() — attaches tool schemas to the LLM. After binding,
     the LLM can emit structured tool_calls in its responses. Without
     binding, the LLM doesn't know the tools exist.

  7. .with_structured_output() — tells the LLM to produce JSON matching
     a Pydantic schema (ReconReport). LangChain converts the schema to a
     JSON Schema, the LLM produces conformant JSON, and LangChain parses
     it back into a Pydantic object. Validation catches malformed output.

  8. Graph compilation — .compile() freezes the graph topology and returns
     a runnable. The compiled graph is what you .invoke() with initial state.

THE REACT LOOP IN THIS GRAPH:

  ┌─────────────────────────────────────────────────────────┐
  │                                                         │
  │    entry ──► llm ──► should_continue? ──► tools ──┐    │
  │                          │                         │    │
  │                          │ (no tools / max iter)   │    │
  │                          ▼                         │    │
  │                       report ──► END               │    │
  │                                                    │    │
  │              ◄─────────────────────────────────────┘    │
  │              (loop back to llm)                         │
  └─────────────────────────────────────────────────────────┘

DESIGN DECISIONS:

  - Closure pattern (not class or RunnableConfig) for passing the LLM to
    nodes — clearest for learning. Each node function is created by a
    factory that captures the model in its scope. No hidden state.

  - Separate report node (not inline in llm_node) — the report needs
    .with_structured_output(), which is a different model configuration
    than the tool-calling model. Keeping it separate also means the full
    investigation history is available for diagnosis.

  - build_recon_graph() as the public API — the agent.py file (next)
    calls this to get a compiled graph. Config flows in, compiled graph
    flows out. Clean boundary.
"""

from __future__ import annotations

import json

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode

from argus.agents.reconciliation.prompts import RECON_PROMPT_TEMPLATE
from argus.agents.reconciliation.state import ReconState
from argus.core.config import ArgusConfig
from argus.core.llm import create_llm
from argus.core.logging import get_logger
from argus.schemas.reports import ReconReport
from argus.tools.pipeline.recon_tools import RECON_TOOLS

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Node factories — each returns a node function with the LLM captured
# ---------------------------------------------------------------------------
# Why factories instead of plain functions?
#
# The LLM is created from config at build time. Node functions need
# access to it, but LangGraph calls nodes with (state) — there's no
# way to pass extra arguments. Three options:
#
#   1. Global variable — works but untestable, can't run two graphs
#      with different models in the same process
#   2. RunnableConfig — LangGraph's "configurable" dict, but hides the
#      dependency and adds framework-specific complexity
#   3. Closures — the model is captured in the closure's scope.
#      Explicit, testable, no hidden state.
#
# We use option 3. Each factory takes the model and returns a function
# that LangGraph can call as a node.


def _make_entry_node():
    """
    Factory for the entry node.

    The entry node runs ONCE at the start. It seeds the conversation with:
      1. SystemMessage — agent identity, pipeline architecture, strategy
      2. HumanMessage — the specific failure to investigate (date, gate, params)

    These come from RECON_PROMPT_TEMPLATE.invoke({...}), which fills the
    template variables and returns a list of Message objects.

    Note: the entry node doesn't need the LLM — it's pure template rendering.
    """

    def entry_node(state: dict) -> dict:
        """Seed the conversation with system prompt and investigation request."""
        # Convert trigger_params to a readable string for the prompt.
        # The LLM needs to see the params as text, not a Python dict repr.
        params_str = json.dumps(state["trigger_params"], indent=2)

        # Render the prompt template → [SystemMessage, HumanMessage]
        prompt_value = RECON_PROMPT_TEMPLATE.invoke({
            "run_date": state["run_date"],
            "gate_name": state["gate_name"],
            "trigger_params": params_str,
        })

        logger.info(
            "entry_node: seeded conversation",
            extra={
                "run_date": state["run_date"],
                "gate_name": state["gate_name"],
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        # Return the messages — the reducer (operator.add) appends them
        # to the empty messages list from make_initial_state()
        return {"messages": prompt_value.to_messages()}

    return entry_node


def _make_llm_node(model_with_tools):
    """
    Factory for the LLM node.

    This node is the REASON step in the ReAct loop. It:
      1. Sends the full message history to the LLM (with tools bound)
      2. The LLM reasons about what it knows and what to do next
      3. The LLM either:
         a) Emits tool_calls → the router sends us to the tool node
         b) Emits a text response → the router sends us to the report node

    The model_with_tools is an LLM with .bind_tools(RECON_TOOLS) applied.
    This means the LLM knows about our six investigation tools and can
    call them by name in its response.

    Why increment iteration HERE, not in the router?
      Because the LLM node is where the "thinking" happens. Each LLM call
      is one iteration of the ReAct loop. Tracking it here makes the count
      accurate — if the router ran without an LLM call, it wouldn't count.
    """

    def llm_node(state: dict) -> dict:
        """Call the LLM with the full conversation history."""
        logger.info(
            "llm_node: calling LLM",
            extra={
                "iteration": state["iteration"] + 1,
                "max_iterations": state["max_iterations"],
                "message_count": len(state["messages"]),
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        # The LLM sees ALL messages — system prompt, human request,
        # and every previous AI response + tool result. This is why
        # the messages reducer (operator.add) is critical — without it,
        # the LLM would only see the latest message.
        response = model_with_tools.invoke(state["messages"])

        # Log what the LLM decided to do
        if hasattr(response, "tool_calls") and response.tool_calls:
            tool_names = [tc["name"] for tc in response.tool_calls]
            logger.info(
                "llm_node: LLM requested tool calls",
                extra={
                    "tools": tool_names,
                    "correlation_id": state.get("correlation_id", ""),
                },
            )
        else:
            logger.info(
                "llm_node: LLM produced text response (no tool calls)",
                extra={"correlation_id": state.get("correlation_id", "")},
            )

        return {
            "messages": [response],     # reducer appends the AIMessage
            "iteration": state["iteration"] + 1,  # no reducer → replaces
        }

    return llm_node


def _make_report_node(model):
    """
    Factory for the report node.

    This node runs ONCE at the end, after the LLM has finished investigating.
    It takes the FULL conversation history (all the evidence gathered) and
    produces a structured ReconReport using .with_structured_output().

    Why a separate node instead of letting the LLM produce the report inline?

      1. The investigation model has tools bound (.bind_tools). Structured
         output (.with_structured_output) is a different configuration —
         you can't have both on the same model call.

      2. Separation of concerns: the LLM node's job is to investigate.
         The report node's job is to synthesize. Different prompts, different
         model configs, clear responsibility boundary.

      3. The report node can add a specific instruction ("produce your
         diagnosis now") that wouldn't make sense mid-investigation.

    How .with_structured_output(ReconReport) works:
      1. LangChain converts the Pydantic model to a JSON Schema
      2. The schema is passed to the LLM as a function/tool definition
      3. The LLM produces JSON conforming to the schema
      4. LangChain parses the JSON into a ReconReport Pydantic object
      5. Pydantic validates all fields (types, required, enum values)
      If the LLM returns invalid JSON, LangChain retries.
    """

    # Create a model configured for structured output (no tools bound).
    # This is a DIFFERENT model configuration than model_with_tools.
    report_model = model.with_structured_output(ReconReport)

    def report_node(state: dict) -> dict:
        """Synthesize the investigation into a structured ReconReport."""
        logger.info(
            "report_node: generating structured report",
            extra={
                "message_count": len(state["messages"]),
                "iteration": state["iteration"],
                "correlation_id": state.get("correlation_id", ""),
            },
        )

        # Add a final instruction telling the LLM to produce the diagnosis.
        # This message is NOT added to the graph state — it's only for
        # this one LLM call. We append it to a copy of the messages.
        report_instruction = HumanMessage(
            content=(
                "Based on your investigation above, produce your structured "
                "diagnosis report. Include:\n"
                f"- gate_failed: {state['gate_name']}\n"
                f"- run_date: {state['run_date']}\n"
                "- All findings from your investigation\n"
                "- A clear root_cause_summary\n"
                "- A specific suggested_fix\n"
                "- recommended_severity (P1/P2/P3)\n"
                "- Any notifications to send\n\n"
                "If errors occurred during investigation, note them. "
                "Be precise — cite specific counts, tables, and snapshot IDs "
                "from the tool results."
            )
        )

        # Combine the full conversation with the report instruction.
        # The report model sees everything the agent learned.
        messages_for_report = state["messages"] + [report_instruction]

        try:
            report = report_model.invoke(messages_for_report)
            logger.info(
                "report_node: report generated successfully",
                extra={
                    "root_cause": report.root_cause_summary[:100],
                    "severity": report.recommended_severity.value,
                    "finding_count": len(report.findings),
                    "correlation_id": state.get("correlation_id", ""),
                },
            )
            return {"report": report}

        except Exception as exc:
            # If structured output fails (malformed JSON, validation error),
            # log the error and put it in the errors accumulator rather than
            # crashing the graph. The agent.py layer will handle the None report.
            error_msg = f"report_node: structured output failed: {exc}"
            logger.error(
                error_msg,
                extra={"correlation_id": state.get("correlation_id", "")},
            )
            return {"errors": [error_msg]}

    return report_node


# ---------------------------------------------------------------------------
# Router — the conditional edge that controls the ReAct loop
# ---------------------------------------------------------------------------

def _should_continue(state: dict) -> str:
    """
    Decide the next step after the LLM node runs.

    This is the ReAct loop's control flow:

      1. Has the LLM hit max_iterations?
         → YES: force report generation (safety valve)

      2. Did the LLM emit tool_calls in its response?
         → YES: go to the tool node to execute them (ACT step)

      3. Did the LLM respond with text only (no tool calls)?
         → go to report node to synthesize the diagnosis

    Why check max_iterations FIRST?
      Because a runaway agent could keep calling tools forever. The safety
      valve must take priority over tool execution. Even if the LLM wants
      to call more tools, we force a report after N iterations.

    The return value is a string that matches the edge mapping in
    add_conditional_edges(). LangGraph uses it to route to the next node.
    """
    messages = state["messages"]
    iteration = state["iteration"]
    max_iterations = state["max_iterations"]

    # Safety valve: force report generation if we've hit the limit
    if iteration >= max_iterations:
        logger.warning(
            "should_continue: max iterations reached, forcing report",
            extra={
                "iteration": iteration,
                "max_iterations": max_iterations,
                "correlation_id": state.get("correlation_id", ""),
            },
        )
        return "report"

    # Check the last message — did the LLM want to call tools?
    last_message = messages[-1]

    if isinstance(last_message, AIMessage) and last_message.tool_calls:
        return "tools"

    # No tool calls — the LLM is done investigating
    return "report"


# ---------------------------------------------------------------------------
# Graph builder — the public API
# ---------------------------------------------------------------------------

def build_recon_graph(config: ArgusConfig):
    """
    Build and compile the Reconciliation Diagnostics StateGraph.

    This is the assembly function — it takes config, creates the LLM,
    builds the nodes, wires the edges, and returns a compiled graph
    ready to be invoked with initial state.

    The compiled graph is a LangGraph Runnable. You call it with:
        result = compiled_graph.invoke(initial_state)
    And it returns the final state dict after all nodes have run.

    Architecture:

        entry ──► llm ──► should_continue? ──► tools ──► (back to llm)
                              │
                              ▼
                           report ──► END

    Args:
        config: ArgusConfig with LLM settings and agent parameters.

    Returns:
        A compiled LangGraph StateGraph (CompiledStateGraph), which is
        a Runnable that accepts a state dict and returns the final state.

    Usage:
        config = load_config("dev")
        graph = build_recon_graph(config)
        result = graph.invoke(make_initial_state(...))
        report = result["report"]  # ReconReport or None
    """
    # --- Step 1: Create the LLM from config ---
    # This calls the factory in core/llm.py, which reads the provider
    # (google/openai/anthropic) and model name from config.
    llm = create_llm(config)

    # --- Step 2: Create two model configurations ---
    #
    # model_with_tools: the investigation model — knows about our 6 tools,
    #   can emit tool_calls in its responses. Used in the llm_node.
    #
    # llm (plain): the report model — used with .with_structured_output()
    #   in the report_node. Can't have both tools and structured output
    #   on the same model call.
    model_with_tools = llm.bind_tools(RECON_TOOLS)

    logger.info(
        "build_recon_graph: building graph",
        extra={
            "provider": config.llm.get("provider", "google"),
            "model": config.llm.get("model", "unknown"),
            "tool_count": len(RECON_TOOLS),
            "tool_names": [t.name for t in RECON_TOOLS],
        },
    )

    # --- Step 3: Create the tool node ---
    # ToolNode is LangGraph's prebuilt node for executing tool calls.
    # It reads the last AIMessage's tool_calls, looks up each tool by
    # name in the provided list, calls it, and returns ToolMessages.
    #
    # You DON'T write tool execution logic:
    #   - LLM says: tool_calls=[{"name": "query_gate_results", "args": {"run_date": "2026-09-28"}}]
    #   - ToolNode finds query_gate_results in RECON_TOOLS
    #   - ToolNode calls query_gate_results("2026-09-28")
    #   - ToolNode returns: {"messages": [ToolMessage(content=<result>)]}
    tool_node = ToolNode(RECON_TOOLS)

    # --- Step 4: Build the StateGraph ---
    # StateGraph(ReconState) creates a graph whose state has the shape
    # defined by ReconState's annotations. Each node reads and writes
    # fields in this state, with reducers handling merges.
    graph = StateGraph(ReconState)

    # --- Step 5: Add nodes ---
    # Each node is a function that takes state and returns a partial update.
    # The string names ("entry", "llm", etc.) are used in edge definitions.
    graph.add_node("entry", _make_entry_node())
    graph.add_node("llm", _make_llm_node(model_with_tools))
    graph.add_node("tools", tool_node)
    graph.add_node("report", _make_report_node(llm))

    # --- Step 6: Wire edges ---
    #
    # set_entry_point: where the graph starts
    graph.set_entry_point("entry")

    # entry → llm: always go from entry to the first LLM call
    graph.add_edge("entry", "llm")

    # llm → conditional: the router decides tools or report
    graph.add_conditional_edges(
        "llm",              # source node
        _should_continue,   # routing function
        {
            "tools": "tools",     # if router returns "tools" → go to tools node
            "report": "report",   # if router returns "report" → go to report node
        },
    )

    # tools → llm: after tool execution, always go back to the LLM
    # so it can reason about the results (OBSERVE → REASON)
    graph.add_edge("tools", "llm")

    # report → END: after producing the report, the graph is done
    graph.add_edge("report", END)

    # --- Step 7: Compile ---
    # .compile() freezes the graph topology and returns a CompiledStateGraph.
    # This is a Runnable — you call .invoke(state) to run the full graph.
    # Compilation validates the graph structure (no orphan nodes, no missing
    # edges, entry point is set, etc.).
    compiled = graph.compile()

    logger.info("build_recon_graph: graph compiled successfully")

    return compiled
