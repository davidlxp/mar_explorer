"""
query_breakdown.py
AI agent for analyzing and breaking down user queries into sequential tasks.
"""

from typing import List, Dict, Any
from openai import OpenAI
from services.constants import MAR_ORCHESTRATOR_MODEL
from services.ai_workflow.data_model import BreakdownQueryResult
from services.ai_workflow.utils.openai_utils import call_openai
import logging

import json

model = MAR_ORCHESTRATOR_MODEL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_breakdown_tools() -> List[Dict[str, Any]]:
    """Get the function schema for query breakdown."""
    return [
        {
            "type": "function",
            "function": {
                "name": "break_down_query",
                "description": "Break down a user query into sequential tasks that need to be executed in order",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "tasks": {
                            "type": "array",
                            "description": "List of tasks in execution order",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "task_id": {
                                        "type": "integer",
                                        "description": "Unique identifier for this task, starting from 1"
                                    },
                                    "task": {
                                        "type": "string",
                                        "description": "Description of what needs to be done"
                                    },
                                    "reason": {
                                        "type": "string",
                                        "description": "Why this task is needed and why it can't be combined with other tasks"
                                    },
                                },
                                "required": ["task_id", "task", "reason"]
                            }
                        }
                    },
                    "required": ["tasks"]
                }
            }
        }
    ]

def get_breakdown_system_prompt(completed_tasks: List[Dict[str, Any]] = None, completed_results: List[Dict[str, Any]] = None) -> str:
    """
        Get the system prompt for query breakdown.
        
        Args:
            completed_tasks: Optional list of tasks that have been completed
            completed_results: Optional list of results from completed tasks
    """
    # Build completed tasks context if available
    completed_context = ""
    if completed_tasks and completed_results:
        completed_context = "\nCompleted Tasks and Results:\n"
        for task, result in zip(completed_tasks, completed_results):
            try:
                task_str = f"Task {task.get('task_id', '?')}: {task.get('task_to_do', 'Unknown task')}"
                result_str = json.dumps(result, indent=2) if result else "No result"
                completed_context += f"\n{task_str}\nResult: {result_str}\n"
            except Exception as e:
                print(f"Warning: Could not format task/result: {e}")
                continue
        completed_context += "\nPlease consider these completed tasks and their results when breaking down remaining work."

    the_prompt = f"""You are an expert at breaking down complex queries into sequential tasks.
        Your job is to analyze user queries and determine what tasks need to be done to answer them.
        {completed_context} """ + """
        CRITICAL: Task Optimization Rules
        1. Smart Task Breakdown:
        - Break down tasks when they involve different types of operations (SQL vs context vs aggregation)
        - Break down when later tasks need to process results from earlier tasks
        Example: "Compare YoY ADV for cash products"
        → GOOD: Break into two tasks:
            1. SQL task: Get ADV data for both years
            2. Aggregation task: Calculate and format YoY comparison
        
        Example: "Get ADV for cash and credit products"
        → BAD: Two separate SQL tasks
        → GOOD: One SQL task with IN clause for asset_class
        
        2. Valid Reasons for Task Breakdown:
        a) Different Query Types:
            - When mixing numeric (SQL) and context (search) queries
            Example: "What's the ADV trend and why did it change?"
            → Needs breakdown: One SQL task + one context search task
        
        b) Sequential Dependencies:
            - When later analysis needs results from earlier queries
            Example: "Show products with above-average ADV"
            → Needs breakdown: First get average, then filter using that value
        
        c) Complex Transformations:
            - When data needs significant post-processing that can't be done in SQL
            Example: "Calculate correlation between X and Y over time"
            → Needs breakdown: Get raw data first, then process
        
        3. Task Structure and Order:
        - Each task must have a unique task_id (starting from 1)
        - Tasks are executed in order of task_id (smaller task_id first)
        - Later tasks can build upon results from earlier tasks
        
        4. Task Components:
        - task_id: Unique integer identifier
        - task: Clear description of what to do
        - reason: MUST explain why this can't be combined with other tasks
        
        Example Good Breakdowns:
        
        Query 1: "Compare YoY ADV for cash products in August 2024 vs 2025"
        Tasks:
        {
        "tasks": [
            {
            "task_id": 1,
            "task": "Get ADV data for cash products for August 2024 and 2025",
            "reason": "Need raw ADV data for both years for comparison"
            },
            {
            "task_id": 2,
            "task": "Calculate and format YoY comparison",
            "reason": "Need to process the raw data to show meaningful YoY changes"
            }
        ]
        }
        
        Query 2: "What's our market share in credit products and why did it change?"
        Tasks:
        {
        "tasks": [
            {
            "task_id": 1,
            "task": "Calculate market share for credit products",
            "reason": "Need base numeric data before we can analyze changes"
            },
            {
            "task_id": 2,
            "task": "Search for context about credit market share changes",
            "reason": "Requires different query type (context) and can use task 1's data"
            }
        ]
        }
        
        Example Bad Breakdown (Don't Do This):
        Query: "Get ADV for US and EU products"
        Tasks: Don't split into separate US/EU tasks - use a single SQL query with region IN ('us', 'eu')
    """
    return the_prompt

def break_down_query(
    query: str,
    completed_tasks: List[Dict[str, Any]] = None,
    completed_results: List[Dict[str, Any]] = None
) -> List[BreakdownQueryResult]:
    """
    Break down a user query into sequential tasks.
    
    Args:
        query: The user's query
        completed_tasks: Optional list of tasks that have been completed
        completed_results: Optional list of results from completed tasks
        
    Returns:
        List of tasks with their reasons, in execution order
    """
    try:
        # Get tools and prompt
        tools = get_breakdown_tools()
        system_prompt = get_breakdown_system_prompt(completed_tasks, completed_results)

        # Build user message with completed tasks context
        user_message = query
        if completed_tasks and completed_results:
            user_message = f"""Original Query: {query}
                            Please break down the remaining work needed, considering the tasks already completed."""
        
        # Call OpenAI
        logger.info("Calling OpenAI with:")
        logger.info(f"System prompt length: {len(system_prompt)}")
        logger.info(f"User message: {user_message}")
        
        response = call_openai(system_prompt, user_message, tools)
        
        logger.info("Got OpenAI response")
        if response.choices and response.choices[0].message.tool_calls:
            logger.info("Response has tool calls")
            logger.info(f"Tool call arguments: {response.choices[0].message.tool_calls[0].function.arguments}")
        else:
            logger.info("Response has no tool calls")

        # Parse response
        if not response.choices[0].message.tool_calls:
            return [BreakdownQueryResult(
                task_id=1,
                task_to_do="Maybe your question is not related to MAR trading volumes or press releases? I wasn’t able to identify clear tasks to break down. Please provide more details so I can better assist you. :)",
                reason="Just can't figure it out."
            )]

        # Extract tasks
        try:
            result = response.choices[0].message.tool_calls[0].function.arguments
            data = json.loads(result)
            
            if not isinstance(data, dict) or "tasks" not in data:
                return [BreakdownQueryResult(
                    task_id=1,
                    task_to_do="Invalid response format",
                    reason="Missing tasks array"
                )]
                
            # Validate task IDs
            task_ids = set()
            tasks = []
            for task in data["tasks"]:
                if not isinstance(task, dict):
                    continue
                    
                try:
                    task_id = int(task.get("task_id", 0))
                    task_desc = str(task.get("task", ""))
                    reason = str(task.get("reason", ""))
                    
                    if task_id <= 0 or not task_desc or not reason:
                        continue
                        
                    if task_id in task_ids:
                        continue
                        
                    task_ids.add(task_id)
                    tasks.append(BreakdownQueryResult(
                        task_id=task_id,
                        task_to_do=task_desc,
                        reason=reason
                    ))
                except (ValueError, TypeError):
                    continue
                    
            if not tasks:
                return [BreakdownQueryResult(
                    task_id=1,
                    task_to_do="Could not parse any valid tasks",
                    reason="Invalid task data format"
                )]

            return tasks
                
        except json.JSONDecodeError:
            return [BreakdownQueryResult(
                task_id=1,
                task_to_do="Invalid JSON response",
                reason="Could not parse OpenAI response"
            )]
        
        return tasks
        
    except Exception as e:
        logger.error(f"Error breaking down query: {str(e)}")
        logger.error("Response content:", exc_info=True)
        
        # Create a safe error message
        error_msg = "Error analyzing query"
        try:
            error_details = str(e)
            if len(error_details) > 100:  # Truncate very long error messages
                error_details = error_details[:100] + "..."
            error_msg = f"Error: {error_details}"
        except:
            pass  # Keep the default error message if formatting fails
            
        return [BreakdownQueryResult(
            task_id=1,
            task_to_do=error_msg,
            reason="An error occurred while processing the query"
        )]
