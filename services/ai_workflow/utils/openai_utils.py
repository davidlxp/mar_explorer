"""
openai_tools.py
Tools for interacting with OpenAI API, including function definitions and call handling.
"""

from typing import Dict, Any, List
from openai import OpenAI
from services.constants import MAR_ORCHESTRATOR_MODEL, MAR_TABLE_PATH

# Initialize OpenAI client
client = OpenAI()
model = MAR_ORCHESTRATOR_MODEL

def call_openai(system_prompt: str, user_query: str, tools: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Make an OpenAI API call with function calling.
    
    Args:
        system_prompt: The system prompt to use
        user_query: The user's query
        tools: The function schema to use
        
    Returns:
        The parsed response from OpenAI
    """
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ],
        tools=tools,
        tool_choice="auto"
    )
    
    return response
