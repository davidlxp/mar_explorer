"""
openai_tools.py
Tools for interacting with OpenAI API, including function definitions and call handling.
"""

from typing import Dict, Any, List, Optional
from openai import OpenAI
from services.constants import MAR_ORCHESTRATOR_MODEL, MAR_TABLE_PATH

# Initialize OpenAI client
client = OpenAI()
model = MAR_ORCHESTRATOR_MODEL

import time
import logging
from typing import List, Dict, Any, Optional, Union
from openai import OpenAIError

logger = logging.getLogger(__name__)

def call_openai(
    system_prompt: str,
    user_query: str,
    tools: List[Dict[str, Any]],
    tool_choice: Union[str, Dict[str, Any]] = "auto",
    retries: int = 3,
    backoff: float = 2.0
) -> Optional[Dict[str, Any]]:
    """
    Make a robust OpenAI API call with function calling.

    Args:
        system_prompt: The system prompt to use
        user_query: The user's query
        tools: Function schema definitions
        tool_choice: One of "none", "auto", "required", or dict to force a specific tool
        retries: Number of retries on failure
        backoff: Backoff factor (seconds) between retries

    Returns:
        The OpenAI response object, or None if all retries fail
    """
    # Normalize tool_choice
    if isinstance(tool_choice, str):
        tool_choice = tool_choice.lower()
        if tool_choice not in ("none", "auto", "required"):
            logger.warning(f"Invalid tool_choice '{tool_choice}', defaulting to 'auto'")
            tool_choice = "auto"

    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_query}
                ],
                tools=tools,
                tool_choice=tool_choice,
            )
            return response

        except OpenAIError as e:
            wait_time = backoff * (2 ** attempt)
            logger.error(f"OpenAI API call failed (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                logger.info(f"Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            else:
                logger.critical("All retries exhausted. Returning None.")
                return None
        except Exception as e:
            logger.exception(f"Unexpected error in call_openai: {e}")
            return None

