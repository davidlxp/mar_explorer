"""
mar_orchestrator.py
High-level orchestrator for handling MAR queries with Snowflake + Pinecone.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from services.ai_workflow.agents.query_breakdown import break_down_query
from services.ai_workflow.agents.plan_query_action import plan_query_action
from services.ai_workflow.agents.aggregator import aggregate_results
from services.ai_workflow.data_model import (
    TodoIntent, BreakdownQueryResult, PlanningResult, SqlResult,
    AnswerPacket, ContextChunk, RetrievalResult
)
from services.ai_workflow.utils.common_utils import (
    execute_sql_query,
    execute_vector_query
)
from services.constants import MAR_TABLE_PATH
import services.task_handle_mar as task_handle_mar

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def handle_user_query(user_query: str) -> AnswerPacket:
    """
    High-level entrypoint for query processing:
    1. Break down query into tasks
    2. Process tasks one by one, with iterative refinement
    3. Aggregate results into final answer
    """
    # Maximum try times
    max_try_times = 10
    
    # Track completed tasks and their results
    completed_tasks: List[Dict[str, Any]] = []
    completed_results: List[Dict[str, Any]] = []
    
    while max_try_times > 0:
        print(f"max_try_times: {max_try_times}")
        
        # Get next set of tasks
        breakdown_results = break_down_query(
            user_query,
            completed_tasks=completed_tasks,
            completed_results=completed_results
        )

        # print("\n\n")
        # print("--------------------------------")
        # print("\n")
        # print("breakdown_results")
        # print(breakdown_results)
        # print("\n")
        # print("--------------------------------")
        # print("\n\n")
        
        # If no tasks returned, we're done
        if not breakdown_results:
            if not completed_tasks:
                return AnswerPacket(
                    text="Sorry, I couldn't figure out how to process your query.",
                    citations=[],
                    confidence=0.0
                )
            # Return aggregated results
            return aggregate_results(user_query, completed_tasks, completed_results)
            
        # Get the task with minimum task_id
        current_task = min(breakdown_results, key=lambda x: x.task_id)

        # print("\n\n")
        # print("--------------------------------")
        # print("\n")
        # print("current_task")
        # print(current_task)
        # print("\n")
        # print("--------------------------------")
        # print("\n\n")
        
        # Plan the current task
        plan = plan_query_action(current_task)

        # print("\n\n")
        # print("--------------------------------")
        # print("\n")
        # print("Plan the current task")
        # print(plan)
        # print("\n")
        # print("--------------------------------")
        # print("\n\n")
        
        # Execute the task based on intent
        result = execute_task(plan)

        # print("\n\n")
        # print("--------------------------------")
        # print("\n")
        # print("result")
        # print(result)
        # print("\n")
        # print("--------------------------------")
        # print("\n\n")

        if not result:
            return AnswerPacket(
                text=f"Failed to execute task: {current_task.task_to_do}",
                citations=[],
                confidence=0.0
            )
            
        # Store completed task and result
        completed_tasks.append({
            "task_id": current_task.task_id,
            "task_to_do": current_task.task_to_do,
            "reason": current_task.reason,
            "todo_intent": plan.todo_intent.value,
            "reference": ""
        })

        # :::::: If its a numeric task, add the reference to the completed tasks :::::: #

        if plan.todo_intent == TodoIntent.NUMERIC:

            # Get URL of the latest MAR meta data
            ref_url = task_handle_mar.get_latest_mar_meta_from_storage()['url']

            # Create the reference
            ref = f"""The latest MAR file is located at {ref_url},
            If you have access to Snowflake table, the SQL for querying your data is {plan.helper_for_action}."""

            # Add the SQL query as the reference
            completed_tasks[-1]["reference"] = ref

            completed_results.append(result)

        elif plan.todo_intent == TodoIntent.CONTEXT:

            # Get details from the Vector Search Result
            ref_report_name = result.result.hits[0].fields['report_name']
            ref_url = result.result.hits[0].fields['url']
            ref_text = result.result.hits[0].fields['text']

            # Add the report name and url as the reference
            completed_tasks[-1]["reference"] = f"{ref_report_name}, {ref_url}, {ref_text}"

            # Add the report name and text as the task's result
            completed_results.append(f"{ref_report_name}, {ref_text}")

        elif plan.todo_intent == TodoIntent.AGGREGATION:
            completed_tasks[-1]["reference"] = ""
            completed_results.append(result)


        print("\n\n")
        print("--------------------------------")
        print("\n")
        print("completed_tasks")
        print(completed_tasks)
        print("\n")
        print("--------------------------------")
        print("\n\n")

        print("\n\n")
        print("--------------------------------")
        print("\n")
        print("completed_results")
        print(completed_results)
        print("\n")
        print("--------------------------------")
        print("\n\n")
        
        # If this was the last task, aggregate and return
        remaining_tasks = [t for t in breakdown_results if t.task_id != current_task.task_id]
        if not remaining_tasks:
            return aggregate_results(user_query, completed_tasks, completed_results)
        
        max_try_times -= 1

def execute_task(plan: PlanningResult) -> Optional[Dict[str, Any]]:
    """
    Execute a planned task based on its intent.
    
    Args:
        plan: The planned task to execute
        
    Returns:
        Task result or None if execution failed
    """
    try:
        if plan.todo_intent == TodoIntent.NUMERIC:
            if not plan.helper_for_action:
                return None
            return execute_sql_query(plan.helper_for_action)
            
        elif plan.todo_intent == TodoIntent.CONTEXT:
            if not plan.helper_for_action:
                return None
            return execute_vector_query(plan.helper_for_action)
            
        elif plan.todo_intent == TodoIntent.AGGREGATION:
            # Aggregation tasks are handled by the aggregator agent
            return {"aggregation_result": "Task requires aggregation"}
            
        return None
        
    except Exception as e:
        print(f"Error executing task: {e}")
        return None