"""
query_breakdown.py
AI agent for analyzing and breaking down user queries into sequential tasks.
"""

from typing import List, Dict, Any
from openai import OpenAI
from services.constants import MAR_ORCHESTRATOR_MODEL
from services.ai_workflow.data_model import BreakdownQueryResult
from services.ai_workflow.utils.openai_utils import call_openai

import json

model = MAR_ORCHESTRATOR_MODEL

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
                                    "dependency_on": {
                                        "type": "array",
                                        "items": {
                                            "type": "integer"
                                        },
                                        "description": "List of task_ids that must be completed before this task can start. Empty array means no dependencies. Please avoid circular dependencies!"
                                    }
                                },
                                "required": ["task_id", "task", "reason", "dependency_on"]
                            }
                        }
                    },
                    "required": ["tasks"]
                }
            }
        }
    ]

def get_breakdown_system_prompt() -> str:
    """Get the system prompt for query breakdown."""
    return """You are an expert at breaking down complex queries into sequential tasks.
    Your job is to analyze user queries and determine what tasks need to be done to answer them.

    CRITICAL: Task Optimization Rules
    1. Smart Task Breakdown:
       - Break down tasks when they involve different types of operations (SQL vs aggregation vs context)
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
    
    3. Dependency Management:
       - Each task must have a unique task_id (starting from 1)
       - dependency_on must list all task_ids that must complete first
       - NEVER create circular dependencies
       - A task can depend on multiple parents
       - Tasks with no dependencies should have empty dependency_on array
       
    4. Task Structure:
       - task_id: Unique integer identifier
       - task: Clear description of what to do
       - reason: MUST explain why this can't be combined with other tasks
       - dependency_on: Array of parent task_ids ([] if none)
       
    Example Good Breakdowns:
    
    Query 1: "Compare YoY ADV for cash products in August 2024 vs 2025"
    Tasks:
    {
      "tasks": [
        {
          "task_id": 1,
          "task": "Get ADV data for cash products for August 2024 and 2025",
          "reason": "Need raw ADV data for both years for comparison",
          "dependency_on": []
        },
        {
          "task_id": 2,
          "task": "Calculate and format YoY comparison",
          "reason": "Need to process the raw data to show meaningful YoY changes",
          "dependency_on": [1]
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
          "reason": "Need base numeric data before we can analyze changes",
          "dependency_on": []
        },
        {
          "task_id": 2,
          "task": "Search for context about credit market share changes",
          "reason": "Requires different query type (context) and needs task 1's data",
          "dependency_on": [1]
        }
      ]
    }
    
    Example Bad Breakdown (Don't Do This):
    Query: "Get ADV for US and EU products"
    Tasks: Don't split into separate US/EU tasks - use a single SQL query with region IN ('us', 'eu')
    """

def break_down_query(query: str) -> List[BreakdownQueryResult]:
    """
    Break down a user query into sequential tasks.
    
    Args:
        query: The user's query
        
    Returns:
        List of tasks with their reasons, in execution order
    """
    try:
        # Get tools and prompt
        tools = get_breakdown_tools()
        system_prompt = get_breakdown_system_prompt()
        
        # Call OpenAI
        response = call_openai(system_prompt, query, tools)
        
        # Parse response
        if not response.choices[0].message.tool_calls:
            return [BreakdownQueryResult(
                task_id=1,
                task_to_do="Can't figure out how to break down the query.",
                reason="Just can't figure it out.",
                dependency_on=None
            )]
            
        # Extract tasks
        result = response.choices[0].message.tool_calls[0].function.arguments
        data = json.loads(result)

        # Validate task IDs and dependencies
        task_ids = set()
        for task in data["tasks"]:
            task_id = task["task_id"]
            if task_id in task_ids:
                raise ValueError(f"Duplicate task_id: {task_id}")
            task_ids.add(task_id)
            
            # Validate dependencies
            for dep_id in task["dependency_on"]:
                if dep_id >= task_id:
                    raise ValueError(f"Invalid dependency: Task {task_id} cannot depend on future task {dep_id}")
                if dep_id not in task_ids:
                    raise ValueError(f"Invalid dependency: Task {dep_id} not found for task {task_id}")
        
        # Create BreakdownQueryResult objects
        tasks = []
        for task in data["tasks"]:
            tasks.append(BreakdownQueryResult(
                task_id=task["task_id"],
                task_to_do=task["task"],
                reason=task["reason"],
                dependency_on=set(task["dependency_on"]) if task["dependency_on"] else None
            ))
        
        return tasks
        
    except Exception as e:
        print(f"Error breaking down query: {e}")
        return [BreakdownQueryResult(
            task_id=1,
            task_to_do="Error analyzing query",
            reason=str(e),
            dependency_on=None  # No dependencies for error case
        )]
