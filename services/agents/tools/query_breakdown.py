"""
query_breakdown.py
Module for analyzing and breaking down user queries into sequential tasks.
"""

from typing import List, Dict, Any
from openai import OpenAI
from services.constants import MAR_ORCHESTRATOR_MODEL
from services.agents.data_model import BreakdownQueryResult

import json

client = OpenAI()
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
                                    "task": {
                                        "type": "string",
                                        "description": "Description of what needs to be done"
                                    },
                                    "reason": {
                                        "type": "string",
                                        "description": "Why this task is needed and why at this position in sequence"
                                    }
                                },
                                "required": ["task", "reason"]
                            }
                        }
                    },
                    "required": ["tasks"]
                }
            }
        }
    ]

def get_breakdown_prompt() -> str:
    """Get the system prompt for query breakdown."""
    return """You are an expert at breaking down complex queries into sequential tasks.
    Your job is to analyze user queries and determine what tasks need to be done to answer them.
    
    IMPORTANT: Task Sequencing Rules
    1. Data Dependencies:
       - If task B needs data from task A, task A must come first
       Example: "Compare ADV between cash and credit, and explain why credit is higher"
       → Tasks:
         1. Get ADV for cash products (needed for comparison)
         2. Get ADV for credit products (needed for comparison)
         3. Search context about credit performance (uses ADV comparison results)
    
    2. Comparative Analysis:
       - Get individual data points before computing comparisons
       Example: "What's the month-over-month change in volume for US ETFs?"
       → Tasks:
         1. Get volume for previous month
         2. Get volume for current month
         3. Calculate percentage change
    
    3. Context Enhancement:
       - Get numerical data before searching for explanatory context
       Example: "Why did US ETFs volume spike in August 2025?"
       → Tasks:
         1. Get volume trend leading up to August 2025 (establish spike)
         2. Search for news/context about US ETFs in that period
    
    4. Aggregation Requirements:
       - Get granular data before aggregating
       Example: "What's the total volume by product type in credit asset class?"
       → Tasks:
         1. Get volume data for all products in credit asset class
         2. Aggregate volumes by product type
    
    For each task, explain:
    1. What needs to be done
    2. Why it's needed
    3. Why it needs to be done at this position in the sequence
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
        system_prompt = get_breakdown_prompt()
        
        # Call OpenAI
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ],
            tools=tools,
            tool_choice="auto"
        )
        
        # Parse response
        if not response.choices[0].message.tool_calls:
            return [BreakdownQueryResult(task_to_do="Can't figure out how to break down the query.", reason="Just can't figure it out.")]
            
        # Extract tasks
        result = response.choices[0].message.tool_calls[0].function.arguments
        data = json.loads(result)

        tasks = []
        for task in data["tasks"]:
            tasks.append(BreakdownQueryResult(task_to_do=task["task"], reason=task["reason"]))
        
        return tasks
        
    except Exception as e:
        print(f"Error breaking down query: {e}")
        return [BreakdownQueryResult(task_to_do="Error analyzing query", reason=str(e))]
