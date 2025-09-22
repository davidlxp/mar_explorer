"""
query_processor.py
Tools for processing and validating query responses.
"""

import json
from typing import Dict, Any, Optional
from services.constants import MAR_TABLE_PATH

def process_sql_query(query: str) -> str:
    """
    Process and validate a SQL query.
    
    Args:
        query: The SQL query to process
        
    Returns:
        The processed SQL query
    """
    if not query:
        return query
        
    # Ensure correct table name
    if MAR_TABLE_PATH not in query and "mar_combined_m" in query:
        query = query.replace("mar_combined_m", MAR_TABLE_PATH)
    
    # Validate string quotes (Snowflake prefers single quotes)
    if '"' in query:
        query = query.replace('"', "'")
        
    return query

def parse_openai_response(response: Any) -> Dict[str, Any]:
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
    
    # Process SQL queries
    if todo_intent == "numeric" and helper_for_action:
        helper_for_action = process_sql_query(helper_for_action)
    
    # Extract confidence fields
    confidence = tool_args.get("confidence", 0.5)  # Default to 0.5 if not provided
    confidence_reason = tool_args.get("confidence_reason", "No confidence reason provided")
    
    return {
        "todo_intent": todo_intent,
        "helper_for_action": helper_for_action,
        "confidence": confidence,
        "confidence_reason": confidence_reason
    }
