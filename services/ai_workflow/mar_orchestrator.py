"""
mar_orchestrator.py
High-level orchestrator for handling MAR queries with Snowflake + Pinecone.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from services.ai_workflow.agents.query_breaker import break_down_query
from services.ai_workflow.agents.task_planner import plan_query_action
from services.ai_workflow.agents.aggregator import aggregate_results
from services.ai_workflow.data_model import (
    TodoIntent, BreakdownQueryResult, PlanningResult, SqlResult,
    AnswerPacket, ContextChunk, RetrievalResult, InputForValidator, ExecutionOutput
)
from services.ai_workflow.utils.common_utils import (
    construct_input_for_validator,
    get_completed_tasks_info,
)
from services.constants import MAR_TABLE_PATH, DEBUG_MODE
import services.task_handle_mar as task_handle_mar
from services.ai_workflow.data_model import CompletedTask, CompletedTaskResult
import services.ai_workflow.utils.executor_logic as executor_logic
from services.ai_workflow.agents.validator import validate_task_result
from services.ai_workflow.utils.executor_logic import execute_task
from services.ai_workflow.utils.common_utils import contruct_task_info_str_for_aggregator

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
    max_task_tries = 3

    # All lists to track activities
    validator_confidence_for_pass = 0.8
    tasks_completed: List[CompletedTask] = []
    tasks_results: List[CompletedTaskResult] = []
    tasks_tried_times: List[int] = []
    
    current_try_times = 1
    while current_try_times <= max_try_times:
        
        if DEBUG_MODE:
            print("=*"*100)
            print(f"current_try_times: {current_try_times}")
            print(f"max_try_times: {max_try_times}")

        validator_confidence = 0.0
        while validator_confidence < validator_confidence_for_pass and tasks_tried_times[-1] < max_task_tries:

            # Get the prior tasks info
            prior_tasks_info = get_completed_tasks_info(tasks_completed, tasks_results)
            # Get next set of tasks
            breakdown_results = break_down_query(user_query, prior_tasks_info)

            if DEBUG_MODE:
                print("="*100)
                print("breakdown_results")
                print(breakdown_results)
                        
            # If no tasks returned, we're done
            if not breakdown_results:
                return AnswerPacket(
                    text=":( Sorry, I wasn’t able to identify the next step to do and complete the query. Please try again.",
                    citations=[],
                    confidence=0.0
                )

            # Get the current task
            current_task = breakdown_results

            if DEBUG_MODE:
                print("="*100)
                print("current_task")
                print(current_task)

            # Plan the current task
            plan = plan_query_action(current_task)

            if not plan:
                return AnswerPacket(
                    text=":( Sorry, I failed to plan the task. Please try again.",
                    citations=[],
                    confidence=0.0
                )
            
            # Execute the task based on intent
            result = execute_task(plan)

            if not result:
                return AnswerPacket(
                    text=f"Failed to execute task: {current_task.task_to_do}",
                    citations=[],
                    confidence=0.0
                )

            # Construct the input for the validator
            input_for_validator = construct_input_for_validator(user_query, current_task, plan, result)
            
            # Validate the task result
            prior_tasks_info = get_completed_tasks_info(tasks_completed, tasks_results)
            validator_opinion = validate_task_result(input_for_validator, prior_tasks_info)

            if not validator_opinion:
                return AnswerPacket(
                    text=f"Failed to validate task result: {current_task.task_to_do}",
                    citations=[],
                    confidence=0.0
                )
            
            # Increment the tried times regardless of the validator confidence (exit or not)
            tasks_tried_times.append(tasks_tried_times[-1] + 1)
            
            # When the validator is confident, prepare to exit the loop
            validator_confidence = validator_opinion.confidence_of_result
            if validator_confidence >= validator_confidence_for_pass:

                completed_task = CompletedTask(
                    task_to_do=current_task.task_to_do,
                    todo_intent=current_task.todo_intent,
                    task_reason=breakdown_results.reason,
                    helper_for_action=current_task.helper_for_action,
                )

                completed_task_result = CompletedTaskResult(
                    result=result, 
                    reference=result.reference,
                    validator_confidence=validator_confidence
                )

                tasks_completed.append(completed_task)
                tasks_results.append(completed_task_result)

        if DEBUG_MODE:
            print("="*100)
            print("tasks_completed")
            print(len(tasks_completed))
            print("="*100)
            print("tasks_results")
            print(len(tasks_results))
            print("="*100)

        current_try_times += 1
        
        # If the last task was AGGREGATION, aggregate and return
        if tasks_completed[-1].todo_intent == TodoIntent.AGGREGATION:
            all_task_info_str = contruct_task_info_str_for_aggregator(tasks_completed, tasks_results)
            return aggregate_results(user_query, all_task_info_str)