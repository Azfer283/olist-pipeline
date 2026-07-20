"""
Agent core — wires the read-only tools to a local Ollama LLM via LangChain.

Runs entirely offline against a local Ollama model (default: llama3.1), so it costs nothing.
Tools are bound to a live SparkSession via functools.partial.
"""

from functools import partial

from pyspark.sql import SparkSession
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import StructuredTool
from langchain_ollama import ChatOllama

from agent import tools
from agent.prompts import SYSTEM_PROMPT


def build_agent(spark: SparkSession, model: str = "llama3.1", temperature: float = 0.0) -> AgentExecutor:
    """Build an AgentExecutor with the pipeline-debugging tools bound to `spark`."""

    lc_tools = [
        StructuredTool.from_function(partial(tools.inspect_table, spark), name="inspect_table"),
        StructuredTool.from_function(partial(tools.read_dq_logs, spark), name="read_dq_logs"),
        StructuredTool.from_function(partial(tools.read_quarantine, spark), name="read_quarantine"),
        StructuredTool.from_function(partial(tools.compare_counts, spark), name="compare_counts"),
        StructuredTool.from_function(partial(tools.trace_record, spark), name="trace_record"),
    ]

    llm = ChatOllama(model=model, temperature=temperature)
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("human", "{input}"),
        MessagesPlaceholder("agent_scratchpad"),
    ])
    agent = create_tool_calling_agent(llm, lc_tools, prompt)
    return AgentExecutor(
        agent=agent,
        tools=lc_tools,
        verbose=False,
        max_iterations=8,
        handle_parsing_errors=True,
    )
