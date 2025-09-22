"""
mar_orchestrator.py
High-level orchestrator for handling MAR queries with Snowflake + Pinecone.
"""

from typing import List, Dict, Any
from services.ai_workflow.agents.query_breakdown import break_down_query
from services.ai_workflow.data_model import (
    TodoIntent, BreakdownQueryResult, PlanningResult, SqlResult,
    AnswerPacket, ContextChunk, RetrievalResult
)
from services.ai_workflow.agents.plan_query_action import plan_query_action, plan_all_tasks
from services.constants import MAR_TABLE_PATH


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
        print(f"task_id: {item.task_id}")
        print(f"task_to_do: {item.task_to_do}")
        print(f"reason:  {item.reason}")
        print(f"dependency_on: {item.dependency_on}")
    print("\n---------------")
    
    # Plan all tasks
    all_planned_results = plan_all_tasks(breakdown_results)
    print("\nAll Planned Results:")
    print("---------------")
    for task_id in sorted(all_planned_results.keys()):
        result = all_planned_results[task_id]
        print('\n')
        print(">>>>>>>>>>>>")
        print(f"task_id: {result.task_id}")
        print(">>>>>>>>>>>>")
        print(f"dependency_on: {result.dependency_on}")
        print(f"task_to_do: {result.task_to_do}")
        print(f"todo_intent: {result.todo_intent}")
        print(f"helper_for_action: {result.helper_for_action}")
        print(f"confidence: {result.confidence}")
        print(f"confidence_reason: {result.confidence_reason}")
    print("\n---------------")
    
    # # Execute tasks based on intent
    # results = []
    # for task_id in sorted(completed_tasks):
    #     res = task_results[task_id]
    #     if res.todo_intent == TodoIntent.NUMERIC:
    #         results.append(run_numeric_task())
    #     elif res.todo_intent == TodoIntent.CONTEXT:
    #         results.append(run_context_task())
    #     else:
    #         return AnswerPacket(
    #             text="Sorry, I can only help with MAR numeric or context queries.",
    #             citations=[],
    #             confidence=0.99,
    #         )

    # return compose_final_answer(user_query, results)


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