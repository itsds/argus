"""
Argus Phase 0 — Calculator Agent
=================================
A throwaway agent to learn three foundational concepts:
  1. Tool Design      — how to write functions the LLM can call
  2. ReAct Loop       — the Reason → Act → Observe cycle
  3. LangGraph        — StateGraph, nodes, edges, conditional routing

This is NOT part of Argus itself. It's a sandbox to learn the mechanics
before building the real agents (Recon, DLQ Triage, Backfill, Spark Debugger).

Run:  python experiments/calculator_agent.py
"""

# ─────────────────────────────────────────────────────────────
# SECTION 1: IMPORTS
# ─────────────────────────────────────────────────────────────
# These are the same building blocks every Argus agent will use.

import json
from typing import Annotated                 # For type hints in state
from langchain_core.tools import tool        # @tool decorator — turns a Python function into an LLM-callable tool
from langchain_core.messages import (
    HumanMessage,                            # What the user says
    AIMessage,                               # What the LLM responds
    ToolMessage,                             # The result after a tool executes
    BaseMessage,                             # Parent class for all message types
)
from langchain_google_genai import ChatGoogleGenerativeAI  # Free tier — no credit card needed
from langgraph.graph import StateGraph, END  # The graph builder + terminal node
from langgraph.graph.message import add_messages  # Reducer that appends messages to state
from langgraph.prebuilt import ToolNode      # Pre-built node that executes tool calls

# Pydantic for structured output (you'll use this heavily in Argus
# for DiagnosticReport, BackfillPlan, etc.)
from pydantic import BaseModel


# ─────────────────────────────────────────────────────────────
# SECTION 2: TOOL DESIGN
# ─────────────────────────────────────────────────────────────
# Key concept: The @tool decorator converts a regular Python function
# into something the LLM can "see" and "call".
#
# What the LLM actually sees is the DOCSTRING + TYPE HINTS.
# The docstring IS your prompt engineering for tools — the LLM reads
# it to decide WHEN and HOW to use the tool. Bad docstring = LLM
# picks the wrong tool or passes wrong args.
#
# In Argus, your tools will be things like:
#   @tool def query_watermark(pipeline_name: str, run_date: str) -> dict:
#       """Query the watermark control table for a pipeline's last
#       successfully processed timestamp..."""
#
# Same pattern — just different domain.

@tool
def add(a: float, b: float) -> float:
    """Add two numbers together.

    Use this when you need to find the sum of two values.
    Example: add(3, 5) returns 8.
    """
    return a + b


@tool
def subtract(a: float, b: float) -> float:
    """Subtract b from a.

    Use this when you need to find the difference between two values.
    Example: subtract(10, 3) returns 7.
    """
    return a - b


@tool
def multiply(a: float, b: float) -> float:
    """Multiply two numbers together.

    Use this when you need to find the product of two values.
    Example: multiply(4, 5) returns 20.
    """
    return a * b


@tool
def divide(a: float, b: float) -> float:
    """Divide a by b.

    Use this when you need to find the quotient.
    Returns an error message if b is zero.
    Example: divide(10, 2) returns 5.
    """
    if b == 0:
        return "Error: Division by zero is not allowed."
    return a / b


# Collect all tools into a list — this is what we'll bind to the LLM
# and pass to the ToolNode.
# In Argus, you'll have two lists:
#   pipeline_tools = [query_watermark, query_pipeline_lock, ...]
#   compute_tools  = [get_stage_metrics, get_task_distribution, ...]
tools = [add, subtract, multiply, divide]


# ─────────────────────────────────────────────────────────────
# SECTION 3: STATE SCHEMA
# ─────────────────────────────────────────────────────────────
# Every LangGraph agent has a STATE — a TypedDict or Pydantic model
# that flows through the graph. Think of it as the "memory" of the
# current agent run.
#
# The key field here is `messages` — the conversation history.
# The `add_messages` annotation is a REDUCER: when a node returns
# {"messages": [new_msg]}, it APPENDS to the list instead of replacing.
# Without this, each node would overwrite all previous messages.
#
# In Argus, your state will carry more than just messages:
#   class ReconState(TypedDict):
#       messages: Annotated[list[BaseMessage], add_messages]
#       run_date: str
#       pipeline_name: str
#       gate_results: dict | None
#       diagnosis: DiagnosticReport | None
#
# But for now, messages alone is enough.

class AgentState(BaseModel):
    """State that flows through the calculator agent graph.

    The `messages` list is the conversation history — it accumulates
    HumanMessage → AIMessage → ToolMessage → AIMessage as the agent
    reasons through the problem.
    """
    messages: Annotated[list[BaseMessage], add_messages] = []


# ─────────────────────────────────────────────────────────────
# SECTION 4: LLM SETUP + TOOL BINDING
# ─────────────────────────────────────────────────────────────
# .bind_tools() tells the LLM about the available tools.
# Under the hood, it converts each @tool's name, docstring, and
# type hints into the JSON schema that goes in the API request
# as `tools: [...]`.
#
# When the LLM decides to use a tool, its response includes a
# `tool_calls` field instead of (or alongside) regular text content.
# That's the "Act" in ReAct — the LLM chose an action.

llm = ChatGoogleGenerativeAI(model="gemini-3.8-flash", temperature=0)

# bind_tools modifies the LLM so every call includes tool definitions
llm_with_tools = llm.bind_tools(tools)


# ─────────────────────────────────────────────────────────────
# HELPER: VERBOSE LOGGING
# ─────────────────────────────────────────────────────────────

BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
MAGENTA = "\033[95m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"

def log_separator(label: str, color: str = CYAN):
    print(f"\n{color}{'─'*60}")
    print(f"  {label}")
    print(f"{'─'*60}{RESET}\n")

def log_tool_schemas():
    """Print the JSON schemas that get sent to the LLM API.
    This is EXACTLY what the LLM sees when deciding which tool to call."""
    log_separator("TOOL SCHEMAS (what the LLM sees via bind_tools)", MAGENTA)
    for t in tools:
        schema = t.get_input_schema().model_json_schema()
        print(f"  {BOLD}{t.name}{RESET}")
        print(f"  {DIM}Description:{RESET} {t.description}")
        print(f"  {DIM}Parameters:{RESET}  {json.dumps(schema.get('properties', {}), indent=2)}")
        print()

def log_message(msg: BaseMessage, prefix: str = ""):
    """Print detailed info about a message object."""
    msg_type = type(msg).__name__
    if isinstance(msg, HumanMessage):
        print(f"{prefix}{GREEN}[{msg_type}]{RESET}")
        print(f"{prefix}  content: \"{msg.content}\"")
    elif isinstance(msg, AIMessage):
        print(f"{prefix}{BLUE}[{msg_type}]{RESET}")
        if msg.content:
            print(f"{prefix}  content: \"{msg.content}\"")
        if msg.tool_calls:
            print(f"{prefix}  tool_calls:")
            for tc in msg.tool_calls:
                print(f"{prefix}    → {YELLOW}{tc['name']}{RESET}(args={tc['args']}, id={DIM}{tc.get('id', 'N/A')}{RESET})")
        if not msg.content and not msg.tool_calls:
            print(f"{prefix}  (empty — no content, no tool calls)")
    elif isinstance(msg, ToolMessage):
        print(f"{prefix}{CYAN}[{msg_type}]{RESET}")
        print(f"{prefix}  tool_call_id: {DIM}{msg.tool_call_id}{RESET}")
        print(f"{prefix}  name: {msg.name}")
        print(f"{prefix}  content: \"{msg.content}\"")

def log_state(messages: list, label: str = "Current State"):
    """Print the full conversation state — all messages so far."""
    print(f"  {DIM}┌── {label} ({len(messages)} messages) ──┐{RESET}")
    for i, msg in enumerate(messages):
        log_message(msg, prefix=f"  │ [{i}] ")
    print(f"  {DIM}└──{'─' * 40}┘{RESET}")


# ─────────────────────────────────────────────────────────────
# SECTION 5: GRAPH NODES
# ─────────────────────────────────────────────────────────────
# A LangGraph StateGraph has NODES (functions that process state)
# and EDGES (connections between nodes, including conditional ones).
#
# Our graph has two nodes:
#   1. "agent"  — calls the LLM, which either responds or requests tools
#   2. "tools"  — executes whatever tool the LLM requested
#
# The flow:
#   START → agent → (tool_calls?) → tools → agent → (no tool_calls?) → END
#                    ↑___________________________|
#                         (the ReAct loop!)

def agent_node(state: AgentState) -> dict:
    """The 'brain' of the agent — calls the LLM with the current
    conversation history.

    The LLM sees:
      - The full message history (user question + any prior tool results)
      - The available tools (bound via bind_tools)

    It then either:
      a) Returns a response with tool_calls → we route to "tools" node
      b) Returns a final text answer → we route to END

    This is the "Reason" step in ReAct.
    """
    messages = state.messages

    print(f"  {DIM}📤 Sending {len(messages)} message(s) to LLM...{RESET}")
    log_state(messages, "Messages sent to LLM")

    response = llm_with_tools.invoke(messages)

    print(f"\n  {DIM}📥 LLM Response:{RESET}")
    log_message(response, prefix="  ")

    # Return the LLM's response to be appended to state.messages
    # (the add_messages reducer handles the append)
    return {"messages": [response]}


# ToolNode is a pre-built LangGraph node that:
# 1. Reads the last AIMessage's tool_calls
# 2. Finds the matching @tool function
# 3. Executes it with the provided arguments
# 4. Returns a ToolMessage with the result
#
# This is the "Act" + "Observe" steps:
#   Act     = executing the tool (e.g., add(3, 5))
#   Observe = the result (8) wrapped in a ToolMessage
#
# You COULD write this manually, but ToolNode handles edge cases
# (multiple parallel tool calls, error handling, etc.)
tool_node = ToolNode(tools)


# ─────────────────────────────────────────────────────────────
# SECTION 6: CONDITIONAL ROUTING (THE DECISION POINT)
# ─────────────────────────────────────────────────────────────
# After the agent node runs, we need to decide:
#   → Did the LLM request tool calls? Route to "tools"
#   → Did the LLM give a final answer? Route to END
#
# This is a CONDITIONAL EDGE — it inspects the state and returns
# the name of the next node.
#
# In Argus, you'll have more complex routing:
#   - Recon agent: "Did I find the root cause, or do I need to
#     investigate more?" (loop vs. produce report)
#   - Backfill agent: "Is this a high-risk backfill? Route to
#     human approval (interrupt) vs. auto-execute"

def should_continue(state: AgentState) -> str:
    """Decide what happens after the agent (LLM) responds.

    Checks the last message in the conversation:
    - If it has tool_calls → the LLM wants to use a tool → go to "tools"
    - If no tool_calls → the LLM is done reasoning → go to END

    Returns the NAME of the next node (must match what we add to the graph).
    """
    last_message = state.messages[-1]

    has_tool_calls = isinstance(last_message, AIMessage) and last_message.tool_calls
    decision = "tools" if has_tool_calls else END

    print(f"  {YELLOW}🔀 ROUTING DECISION:{RESET}")
    print(f"     Last message type: {type(last_message).__name__}")
    print(f"     Has tool_calls:    {bool(has_tool_calls)}")
    print(f"     Route to:          {BOLD}{'\"tools\" (continue loop)' if decision == 'tools' else '\"END\" (stop graph)'}{RESET}")

    return decision


# ─────────────────────────────────────────────────────────────
# SECTION 7: BUILD THE GRAPH
# ─────────────────────────────────────────────────────────────
# This is where everything connects. Think of it as wiring a circuit:
#
#   ┌──────────┐       ┌──────────┐
#   │  agent   │──?──→ │  tools   │
#   │  (LLM)   │       │ (execute)│
#   └──────────┘       └──────────┘
#        ↑                   │
#        │                   │
#        └───────────────────┘
#        ↓ (if no tool calls)
#       END
#
# StateGraph takes the state schema as its type parameter.

# 1. Create the graph builder
graph_builder = StateGraph(AgentState)

# 2. Add nodes — each node is a (name, function) pair
graph_builder.add_node("agent", agent_node)
graph_builder.add_node("tools", tool_node)

# 3. Set the entry point — where the graph starts
graph_builder.set_entry_point("agent")

# 4. Add conditional edge from "agent" → either "tools" or END
#    The function `should_continue` returns the next node name
graph_builder.add_conditional_edges("agent", should_continue)

# 5. Add normal edge: "tools" always goes back to "agent"
#    (After executing tools, the agent needs to see the results
#     and decide what to do next)
graph_builder.add_edge("tools", "agent")

# 6. Compile — freezes the graph into a runnable
#    After this, `graph` behaves like any LangChain Runnable:
#    you can .invoke(), .stream(), .batch() it.
graph = graph_builder.compile()


# ─────────────────────────────────────────────────────────────
# SECTION 8: RUN THE AGENT
# ─────────────────────────────────────────────────────────────

def run_calculator(question: str) -> str:
    """Run the calculator agent with a question and print each
    step of the ReAct loop so you can see the reasoning.

    Args:
        question: A math question in natural language.

    Returns:
        The agent's final answer.
    """
    log_separator(f"QUESTION: {question}", GREEN)

    # Show what the LLM knows about tools (first run only hint)
    print(f"  {DIM}The LLM has {len(tools)} tools bound: {[t.name for t in tools]}{RESET}\n")

    # Invoke the graph with the initial state
    # The HumanMessage is the user's question
    initial_state = {"messages": [HumanMessage(content=question)]}
    print(f"  {DIM}Initial state: 1 HumanMessage → entering graph at \"agent\" node{RESET}\n")

    # .stream() yields state updates after each node executes
    # This lets us see the ReAct loop in action
    step = 0
    final_answer = ""
    loop_count = 0

    for event in graph.stream(initial_state):
        # event is a dict like {"agent": {"messages": [...]}}
        # The key is the node name that just ran
        for node_name, node_output in event.items():
            step += 1

            if node_name == "agent":
                loop_count += 1
                log_separator(f"Step {step}: AGENT NODE (ReAct loop #{loop_count})", BLUE)
            elif node_name == "tools":
                log_separator(f"Step {step}: TOOL NODE (executing tool call)", CYAN)

            for msg in node_output.get("messages", []):
                if isinstance(msg, AIMessage):
                    if msg.tool_calls:
                        # The LLM decided to use a tool (REASON + ACT)
                        for tc in msg.tool_calls:
                            print(f"  🧠 {YELLOW}REASON{RESET} → LLM chose tool: {BOLD}{tc['name']}{RESET}")
                            print(f"     Arguments: {json.dumps(tc['args'])}")
                            print(f"     Tool call ID: {DIM}{tc.get('id', 'N/A')}{RESET}")
                    else:
                        # The LLM produced a final answer
                        final_answer = msg.content
                        print(f"  💬 {GREEN}FINAL ANSWER{RESET} → {msg.content}")

                elif isinstance(msg, ToolMessage):
                    # Tool executed and returned a result (OBSERVE)
                    print(f"  ⚡ {CYAN}EXECUTE{RESET} → {msg.name}() ran Python function")
                    print(f"  👁️  {CYAN}OBSERVE{RESET} → Result: {BOLD}{msg.content}{RESET}")
                    print(f"     Tool call ID: {DIM}{msg.tool_call_id}{RESET} (matches the request)")

            print()

    log_separator("GRAPH COMPLETE", GREEN)
    print(f"  Total steps: {step}")
    print(f"  ReAct loops: {loop_count}")
    print(f"  Tool calls:  {loop_count - 1}")
    print(f"  Final answer: {BOLD}{final_answer}{RESET}\n")

    return final_answer


# ─────────────────────────────────────────────────────────────
# SECTION 9: INTERACTIVE LOOP
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"  🔢 ARGUS PHASE 0 — Calculator Agent")
    print(f"  Learn the ReAct loop before building real agents")
    print(f"{'='*60}")

    # Show tool schemas on startup so you see what the LLM receives
    log_tool_schemas()

    print("Type a math question, or 'quit' to exit.")
    print("Examples:")
    print("  • What is (3 + 5) * 2?")
    print("  • Divide 100 by 7 and then add 3")
    print("  • What is 15% of 240?")

    while True:
        print()
        user_input = input("You: ").strip()
        if user_input.lower() in ("quit", "exit", "q"):
            print("\nDone. Next up → Argus Phase 1: Platform Skeleton 🚀")
            break
        if not user_input:
            continue

        try:
            run_calculator(user_input)
        except Exception as e:
            print(f"\n❌ Error: {e}")
            print("Make sure GOOGLE_API_KEY is set in your environment.")
