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
from services.ai_workflow.agents.receptionist import receive_query

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def handle_user_query(user_query: str, history: List[Dict[str, str]]) -> AnswerPacket:
    """
        High-level entrypoint for query processing:
        0. Receptionist: decide if we need to clarify with user or proceed
        1. Break down query into tasks
        2. Process tasks one by one, with iterative refinement
        3. Aggregate results into final answer
    """

    # --- Receptionist step ---

    reception_result = receive_query(user_query, history)

    if reception_result.next_step == "follow_up_user":
        return AnswerPacket(
            text=reception_result.next_step_content,
            citations=[],
            confidence=0.0,
            confidence_reason="Query unclear or outside scope, asked user to clarify"
        )

    # If we’re here, next_step = "start_task"
    cleaned_query = reception_result.next_step_content

    # --- Then continue your current pipeline ---
    return _process_tasks(cleaned_query)  # wrap your current loop into a helper

def _process_tasks(user_query: str) -> AnswerPacket:
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
    tasks_tried_times: List[int] = [0]
    
    current_try_times = 1
    while current_try_times <= max_try_times:
        
        if DEBUG_MODE:
            print("=*"*100)
            print(f"current_try_times: {current_try_times}")
            print(f"max_try_times: {max_try_times}")

        # Give 0.0 so it can enter the while loop
        validator_confidence = 0.0

        # Initialize the retry counter for the task
        tasks_tried_times.append(0)

        # The while loop is for the task level
        while validator_confidence < validator_confidence_for_pass and tasks_tried_times[-1] < max_task_tries:

            # Get the prior tasks info
            prior_tasks_info = get_completed_tasks_info(tasks_completed, tasks_results)

            if DEBUG_MODE:
                print('\n')
                print("=~"*20)
                print(f"tasks_tried_times for task {len(tasks_tried_times)}")
                print(tasks_tried_times)
                print("=~"*20)
                print('\n')

            # Get next set of tasks
            breakdown_result = break_down_query(user_query, prior_tasks_info)

            if DEBUG_MODE:
                print("="*100)
                print("breakdown_result")
                print(breakdown_result)
                        
            # If no tasks returned, we're done
            if not breakdown_result:
                return AnswerPacket(
                    text=":( Sorry, I wasn’t able to identify the next step to do and complete the query. Please try again.",
                    citations=[],
                    confidence=0.0
                )

            # Plan the current task
            plan = plan_query_action(breakdown_result, prior_tasks_info)

            if not plan:
                return AnswerPacket(
                    text=":( Sorry, I failed to plan the task. Please try again.",
                    citations=[],
                    confidence=0.0
                )
            
            if DEBUG_MODE:
                print("="*100)
                print("plan")
                print(plan)
                print("="*100)

            # Execute the task based on intent
            result = execute_task(plan)

            if not result:
                return AnswerPacket(
                    text=f"Failed to execute task: {breakdown_result.task_to_do}",
                    citations=[],
                    confidence=0.0
                )
            
            if DEBUG_MODE:
                print("="*100)
                print("result")
                print(result)
                print("="*100)

            # Construct the input for the validator
            input_for_validator = construct_input_for_validator(user_query, breakdown_result, plan, result)

            if DEBUG_MODE:
                print("="*100)
                print("input_for_validator")
                print(input_for_validator)
                print("="*100)
            
            # Validate the task result
            validator_opinion = validate_task_result(input_for_validator, prior_tasks_info)

            if DEBUG_MODE:
                print("="*100)
                print("validator_opinion")
                print(validator_opinion)
                print("="*100)

            if not validator_opinion:
                return AnswerPacket(
                    text=f"Failed to validate task result: {breakdown_result.task_to_do}",
                    citations=[],
                    confidence=0.0
                )
            
            # Increment the tried times regardless of the validator confidence (exit or not)
            tasks_tried_times[-1] += 1
            
            # When the validator is confident, prepare to exit the loop
            validator_confidence = validator_opinion.confidence_of_result
            if validator_confidence >= validator_confidence_for_pass:

                completed_task = CompletedTask(
                    task_to_do=breakdown_result.task_to_do,
                    todo_intent=plan.todo_intent,
                    task_reason=breakdown_result.reason,
                    helper_for_action=plan.helper_for_action,
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