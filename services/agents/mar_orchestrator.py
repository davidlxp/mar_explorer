"""
mar_orchestrator.py
High-level orchestrator for handling MAR queries with Snowflake + Pinecone (and optional Web search).
"""

from typing import List, Dict, Any
from services.agents.mar_helper import get_mar_table_schema, load_available_products, get_sql_examples
from services.agents.tools.openai_tools import get_query_analysis_tools, get_system_prompt, call_openai, get_summarize_tools
from services.agents.tools.query_processor import parse_openai_response
from services.agents.tools.query_breakdown import break_down_query
from services.agents.data_model import (
    TodoIntent, BreakdownQueryResult, PlanningResult, SqlResult,
    AnswerPacket, ContextChunk, RetrievalResult
)
from services.constants import MAR_TABLE_PATH

from services.db import get_database
db = get_database()

def summarize_parent_plans(task: BreakdownQueryResult, parent_plans: Dict[int, PlanningResult]) -> str:
    """
    Generate a summary of parent tasks' plans to provide context for planning the current task.
    
    Args:
        task: Current task being planned
        parent_plans: Dictionary of all previously planned tasks
        
    Returns:
        A summary string describing what parent tasks plan to do
    """
    if not task.dependency_on or not parent_plans:
        return ""
        
    # Get all ancestors (parents and their parents)
    ancestors = set()
    to_process = task.dependency_on.copy()
    while to_process:
        parent_id = to_process.pop()
        if parent_id in ancestors:
            continue
        ancestors.add(parent_id)
        parent = parent_plans.get(parent_id)
        if parent and parent.dependency_on:
            to_process.update(parent.dependency_on)
    
    # Build context from ancestors
    ancestor_plans = []
    for ancestor_id in sorted(ancestors):
        plan = parent_plans[ancestor_id]
        ancestor_plans.append(f"Task {ancestor_id}: {plan.todo_intent} - {plan.helper_for_action}")
    
    # Get summary from AI
    system_prompt = """You are an expert at summarizing task plans.
    Your job is to create a concise summary of what parent tasks plan to do,
    focusing on how their actions and results might influence the current task."""
    
    response = call_openai(
        system_prompt,
        "Summarize these parent task plans:\n" + "\n".join(ancestor_plans),
        get_summarize_tools()
    )
    
    result = parse_openai_response(response)
    return result.get("summary", "No summary available")

def plan_query_action(task: BreakdownQueryResult, parent_plans: Dict[int, PlanningResult] = None) -> PlanningResult:
    """
    Analyze task to determine intent and generate appropriate helper (SQL/vector query).
    
    Args:
        task: The task to plan
        parent_plans: Dictionary of previously planned tasks, used for context
        
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
        
        # Get parent plan summary if available
        parent_summary = summarize_parent_plans(task, parent_plans) if parent_plans else ""
        parent_context = f"\nParent Task Context:\n{parent_summary}" if parent_summary else ""
        
        # Get tools and prompt
        tools = get_query_analysis_tools()
        system_prompt = get_system_prompt(schema, products, sql_examples) + parent_context
        
        # Call OpenAI
        response = call_openai(system_prompt, task.task_to_do, tools)
        
        # Parse and validate response
        result = parse_openai_response(response)
        
        return PlanningResult(
            task_id=task.task_id,
            dependency_on=task.dependency_on,
            task_to_do=task.task_to_do,
            todo_intent=TodoIntent(result["todo_intent"]),
            helper_for_action=result["helper_for_action"],
            confidence=result["confidence"],
            confidence_reason=result["confidence_reason"]
        )
        
    except Exception as e:
        print(f"Error analyzing query: {e}")
        return PlanningResult(
            task_id=task.task_id,
            dependency_on=task.dependency_on,
            task_to_do=task.task_to_do,
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
        print(f"\nTask {task_id}:")
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


def plan_all_tasks(breakdown_results: List[BreakdownQueryResult]) -> Dict[int, PlanningResult]:
    """
    Plan all tasks in dependency order.
    
    Args:
        breakdown_results: List of tasks with their dependencies
        
    Returns:
        Dictionary mapping task_id to its planning result
    """
    all_planned_results = {}
    
    # Keep processing until all tasks are done
    while len(all_planned_results) < len(breakdown_results):
        # Find tasks that can be executed (all dependencies satisfied)
        for task in breakdown_results:
            if task.task_id in all_planned_results:
                continue
                
            # If dependencies are not met yet, skip
            if task.dependency_on and not task.dependency_on.issubset(all_planned_results.keys()):
                continue
            
            print(f"\Planning Task {task.task_id}:")
            if task.dependency_on:
                print(f"Using results from Task {task.dependency_on}")
            
            # Execute the task with parent plans
            res = plan_query_action(task, all_planned_results)
            print("\Planned Task:")
            print("---------------")
            print(f"task_to_do: {res.task_to_do}")
            print(f"todo_intent: {res.todo_intent}")
            print(f"helper_for_action: {res.helper_for_action}")
            print(f"confidence: {res.confidence}")
            print(f"confidence_reason: {res.confidence_reason}")
            print("\n---------------")
            
            # Store results
            all_planned_results[task.task_id] = res
    
    return all_planned_results

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