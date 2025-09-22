"""
summarize_parent_plans.py
AI agent for summarizing parent task plans.
"""

from typing import Dict, Any, List
from services.ai_workflow.data_model import BreakdownQueryResult, PlanningResult
from services.ai_workflow.utils.openai_utils import call_openai
from services.ai_workflow.utils.common_utils import regularize_sql_query
import json

def get_summarize_tools() -> List[Dict[str, Any]]:
    """
    Get the function schema for summarizing parent task plans.
    """
    return [
        {
            "type": "function",
            "function": {
                "name": "summarize_plans",
                "description": "Create a concise summary of parent task plans",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "summary": {
                            "type": "string",
                            "description": "A concise summary of what parent tasks plan to do and how it might influence the current task"
                        }
                    },
                    "required": ["summary"]
                }
            }
        }
    ]

def get_summarize_system_prompt() -> str:
    """
    Get the system prompt for summarizing parent task plans.
    """
    return """You are an expert at summarizing task plans.
    Your job is to create a concise summary of what parent tasks plan to do,
    focusing on how their actions and results might influence the current task."""

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
    system_prompt = get_summarize_system_prompt()
    
    response = call_openai(
        system_prompt,
        "Summarize these parent task plans:\n" + "\n".join(ancestor_plans),
        get_summarize_tools()
    )
    
    result = parse_summarize_parent_plans_response(response)
    return result.get("summary", "No summary available")

def parse_summarize_parent_plans_response(response: Any) -> Dict[str, Any]:
    """
    Parse and validate the OpenAI response.
    
    Args:
        response: The OpenAI response object
        
    Returns:
        Dictionary containing todo_intent and helper_for_action
        
    Raises:
        json.JSONDecodeError: If response cannot be parsed as JSON
    """
    if not response.choices[0].message.tool_calls:
        return {
            "todo_intent": "context",  # Default to context if no clear action
            "helper_for_action": None
        }
    
    # Parse the tool call arguments
    tool_args = json.loads(response.choices[0].message.tool_calls[0].function.arguments)
    
    # Extract fields
    todo_intent = tool_args["todo_intent"]
    helper_for_action = tool_args["helper_for_action"]
    
    # Check and regularize SQL queries
    if todo_intent == "numeric" and helper_for_action:
        helper_for_action = regularize_sql_query(helper_for_action)
    
    # Extract confidence fields
    confidence = tool_args.get("confidence", 0.5)  # Default to 0.5 if not provided
    confidence_reason = tool_args.get("confidence_reason", "No confidence reason provided")
    
    return {
        "todo_intent": todo_intent,
        "helper_for_action": helper_for_action,
        "confidence": confidence,
        "confidence_reason": confidence_reason
    }