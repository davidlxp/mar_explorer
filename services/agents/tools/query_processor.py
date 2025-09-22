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

def parse_openai_response(response: Any, default_confidence: float = 0.0) -> Dict[str, Any]:
    """
    Parse and validate the OpenAI response.
    
    Args:
        response: The OpenAI response object
        default_confidence: Default confidence score if none provided
        
    Returns:
        Dictionary containing parsed response
        
    Raises:
        json.JSONDecodeError: If response cannot be parsed as JSON
    """
    if not response.choices[0].message.tool_calls:
        return {
            "intent": "irrelevant",
            "helper_for_action": None,
            "confidence": 0.0
        }
    
    # Parse the tool call arguments
    tool_args = json.loads(response.choices[0].message.tool_calls[0].function.arguments)
    
    # Extract and validate fields
    intent = tool_args["intent"]
    helper_for_action = tool_args.get("helper_for_action")
    confidence = tool_args.get("confidence", default_confidence)
    
    # Process SQL queries
    if intent == "numeric" and helper_for_action:
        helper_for_action = process_sql_query(helper_for_action)
    
    return {
        "intent": intent,
        "helper_for_action": helper_for_action,
        "confidence": confidence
    }
