"""
mar_orchestrator.py
High-level orchestrator for handling MAR queries with Snowflake + Pinecone (and optional Web search).
"""

from typing import List, Dict, Any
from services.agents.mar_helper import get_mar_table_schema, load_available_products, get_sql_examples
from services.agents.tools.openai_tools import get_query_analysis_tools, get_system_prompt, call_openai
from services.agents.tools.query_processor import parse_openai_response
from services.agents.tools.query_breakdown import break_down_query
from services.agents.data_model import (
    TodoIntent, BreakdownQueryResult, PlanningResult, SqlResult,
    AnswerPacket, ContextChunk, RetrievalResult
)
from services.constants import MAR_TABLE_PATH

from services.db import get_database
db = get_database()

def plan_query_action(user_query: str) -> PlanningResult:
    """
    Analyze user query to determine intent and generate appropriate helper (SQL/vector query).
    
    Args:
        user_query: The user's natural language query
        
    Returns:
        AnalysisResult containing:
        - todo_intent: The type of action needed (numeric/context)
        - helper_for_action: SQL query for numeric intent, search query for context intent
    """
    try:
        # Load required data
        schema = get_mar_table_schema()
        products = load_available_products()
        sql_examples = get_sql_examples()
        
        # Get tools and prompt
        tools = get_query_analysis_tools()
        system_prompt = get_system_prompt(schema, products, sql_examples)
        
        # Call OpenAI
        response = call_openai(system_prompt, user_query, tools)
        
        # Parse and validate response
        result = parse_openai_response(response)
        
        return PlanningResult(
            task_to_do=user_query,  # Pass through the original query
            todo_intent=TodoIntent(result["todo_intent"]),
            helper_for_action=result["helper_for_action"],
            confidence=result["confidence"],
            confidence_reason=result["confidence_reason"]
        )
        
    except Exception as e:
        print(f"Error analyzing query: {e}")
        return PlanningResult(
            task_to_do=user_query,  # Pass through the original query
            todo_intent=TodoIntent.CONTEXT,
            helper_for_action=None,
            confidence=0.0,  # Low confidence due to error
            confidence_reason=f"Error occurred during analysis: {str(e)}"
        )

def handle_user_query(user_query: str) -> str:

    pass

def handle_user_query(user_query: str) -> AnswerPacket:
    """
    High-level entrypoint:
    1. Break down query into sequential tasks
    2. Analyze each task to determine intent and generate helper
    3. Execute tasks in sequence
    4. Compose final answer
    """
    # First, break down the query into sequential tasks
    breakdown_results = break_down_query(user_query)
    print("\nQuery Breakdown:")
    print("---------------")
    for i, item in enumerate(breakdown_results, 1):
        print(f"\nTask {i}:")
        print(f"task_to_do: {item.task_to_do}")
        print(f"reason:  {item.reason}")
    print("\n---------------")
    
    # Now analyze each task to determine intent and generate helper
    for item in breakdown_results:
        task_to_do = item.task_to_do
        res = plan_query_action(task_to_do)
        print("\nTask Analysis:")
        print("---------------")
        print(f"task_to_do: {res.task_to_do}")
        print(f"todo_intent: {res.todo_intent}")
        print(f"helper_for_action: {res.helper_for_action}")
        print(f"confidence: {res.confidence}")
        print(f"confidence_reason: {res.confidence_reason}")
        print("\n---------------")
    
    # # Execute tasks and compose answer
    # results = []
    # for task in tasks:
    #     if task.intent == Intent.NUMERIC:
    #         results.append(run_numeric_task(task))
    #     elif task.intent == Intent.CONTEXT:
    #         results.append(run_context_task(task))
    #     else:
    #         return AnswerPacket(
    #             text="Sorry, I can only help with MAR numeric or context queries.",
    #             citations=[],
    #             confidence=0.99,
    #         )

    # return compose_final_answer(user_query, tasks, results)

def run_numeric_task() -> SqlResult:
    """
    Map helper name to SQL query, execute against Snowflake.
    Retry up to 3x if query fails.
    """
    
    return SqlResult(rows=[], cols=[])

def run_context_task() -> RetrievalResult:
    """
    Run Pinecone retrieval.
    Currently semantic-only, but keep structure flexible for filter-first.
    """
    # TODO: implement Pinecone query
    return RetrievalResult(chunks=[], confidence=0.0, strategy="semantic")

def compose_final_answer(user_query: str, results: List[Any]) -> AnswerPacket:
    """
    Main AI agent fuses multiple results (SQL + Pinecone + Web) into natural language.
    """
    # TODO: Implement with LLM call
    return AnswerPacket(
        text="This is a placeholder answer.",
        citations=[],
        confidence=0.7
    )