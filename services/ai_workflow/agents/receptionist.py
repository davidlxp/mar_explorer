"""
receptionist.py
AI agent for handling user queries before task execution.
"""

from typing import Dict, Any, List
import json
import logging
from dataclasses import dataclass
from services.ai_workflow.utils.openai_utils import call_openai
from services.ai_workflow.data_model import ReceptionResult
import services.ai_workflow.utils.common_utils as common_utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_receptionist_tools() -> List[Dict[str, Any]]:
    """Tool schema for receptionist agent."""
    return [
        {
            "type": "function",
            "function": {
                "name": "decide_reception",
                "description": "Decide whether to ask the user for clarification or proceed with task execution.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "next_step": {
                            "type": "string",
                            "enum": ["follow_up_user", "start_task"],
                            "description": "What to do next based on the query clarity."
                        },
                        "next_step_content": {
                            "type": "string",
                            "description": "If follow_up_user: message to ask the user or explain why unsupported. "
                                           "If start_task: cleaned and well-formed version of the query to pass to next agent for task planning."
                        }
                    },
                    "required": ["next_step", "next_step_content"],
                    "additionalProperties": False
                }
            }
        }
    ]


def get_receptionist_system_prompt() -> str:
  
    task_breakdown_eg_str = common_utils.get_task_breakdown_eg_str()
    mar_table_schema_str = common_utils.get_mar_table_schema_str()
    available_products_str = common_utils.get_available_products_str()
    pr_available_in_storage_str = common_utils.get_pr_available_in_storage_str()

    """System prompt for receptionist agent."""
    return f"""
You are the Receptionist Agent. 
You decide if a user query is clear and answerable with available data, or if clarification is required.

Available knowledge:
- MAR table schema: {schema_str}
- Data coverage: Only MAR Excel tabs "ADV-M" and "Volume-M"
- Press releases available: {pr_list_str}

Rules:
1. If query is unclear, incomplete, irrelevant, or outside available data (e.g. asks about pricing, forecasts, or datasets not listed), set:
   next_step = "follow_up_user"
   next_step_content = A concise clarification question or polite refusal.

2. If query is clear and supported by MAR/PR data, set:
   next_step = "start_task"
   next_step_content = A cleaned, precise version of the user’s query suitable for task planning.

3. Always keep finance professionals in mind — responses must be professional, concise, and clear.

Examples:

Query: "What was ADV in August?"
→ start_task: "Get ADV for all products in August 2025"

Query: "Why is trading volume dropping in China?"
→ follow_up_user: "Sorry, we don’t have China-specific data. We only cover the Tradeweb MAR Excel (ADV-M, Volume-M) and press releases. Could you reframe your question?"

Query: "Show trend?"
→ follow_up_user: "Could you clarify which product and time range you’d like the trend for?"
"""


def receive_query(user_query: str, schema_str: str, pr_list_str: str) -> ReceptionResult:
    """
    Process the incoming user query and decide whether to clarify or proceed.
    """
    try:
        tools = get_receptionist_tools()
        system_prompt = get_receptionist_system_prompt(schema_str, pr_list_str)

        response = call_openai(
            system_prompt,
            user_query,
            tools,
            tool_choice={"type": "function", "function": {"name": "decide_reception"}}
        )

        tool_calls = response.choices[0].message.tool_calls or []
        if not tool_calls:
            return ReceptionResult(
                next_step="follow_up_user",
                next_step_content="Sorry, I couldn’t process your query. Could you rephrase it?"
            )

        args = json.loads(tool_calls[0].function.arguments)
        return ReceptionResult(
            next_step=args.get("next_step", "follow_up_user"),
            next_step_content=args.get("next_step_content", "Sorry, I couldn’t process your query.")
        )

    except Exception as e:
        logger.error(f"Error in receptionist agent: {e}", exc_info=True)
        return ReceptionResult(
            next_step="follow_up_user",
            next_step_content="Sorry, something went wrong. Could you rephrase your query?"
        )
