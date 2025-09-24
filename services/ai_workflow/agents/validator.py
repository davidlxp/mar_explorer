import logging
from typing import List, Dict, Any, Optional
from services.ai_workflow.data_model import ValidatorOpinion, InputForValidator
from services.ai_workflow.utils.openai_utils import call_openai
import json
import services.ai_workflow.utils.common_utils as common_utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_validator_tools() -> List[Dict[str, Any]]:
    """Tool schema: judge quality of a task result."""
    return [
        {
            "type": "function",
            "function": {
                "name": "validate_result",
                "description": "Judge whether the task result satisfies the task intent and is useful for the original query.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "confidence_of_result": {
                            "type": "number",
                            "description": "Confidence score between 0 and 1",
                            "minimum": 0,
                            "maximum": 1
                        },
                        "confidence_reason": {
                            "type": "string",
                            "description": "Explanation for the confidence rating"
                        }
                    },
                    "required": ["confidence_of_result", "confidence_reason"],
                    "additionalProperties": False
                }
            }
        }
    ]

def get_validator_system_prompt(input_for_validator: InputForValidator, prior_tasks_info: str) -> str:
    """
    Get the system prompt for the validator.
    """
    return f"""
You are the Validator. Your job is to judge if the result satisfies the subtask.

Original query:
{input_for_validator.org_query}

Task done:
{input_for_validator.task_done}

Reason for this task:
{input_for_validator.task_reason}

Task intent:
{input_for_validator.task_intent}

Task approach (SQL query, search string, math expression):
{input_for_validator.task_approach}

Task result:
{input_for_validator.task_result}

Rules:
- Confidence_of_result is between 0.0 and 1.0.
- High confidence if result matches intent and seems correct.
- Lower confidence if result is incomplete, irrelevant, or inconsistent.
- confidence_reason must clearly explain why the score was given.
- If it's a AGGREGATION task, you should let it pass! It's not necessary to validate the result of the AGGREGATION task.

Output: call the validate_result function with confidence_of_result and confidence_reason.

------
### This is just for your information, these are the tasks that have been previously completed and their results ###
{prior_tasks_info}
------
"""

def validate_task_result(input_for_validator: InputForValidator, prior_tasks_info: str) -> ValidatorOpinion:
    try:
        tools = get_validator_tools()
        system_prompt = get_validator_system_prompt(input_for_validator, prior_tasks_info)

        response = call_openai(
            system_prompt,
            "",  # no extra user message, context is all in system prompt
            tools,
            tool_choice={"type": "function", "function": {"name": "validate_result"}}
        )

        parsed = _parse_validator_response(response)
        if parsed:
            return parsed
        else:
            return None
    except Exception:
        logger.exception("Error in validate_task_result")
        return None

def _parse_validator_response(response: Any) -> Optional[ValidatorOpinion]:
    try:
        tool_calls = response.choices[0].message.tool_calls or []
        if not tool_calls:
            return None

        args = json.loads(tool_calls[0].function.arguments)
        conf = float(args.get("confidence_of_result", 0.0))
        reason = str(args.get("confidence_reason", "No reason provided"))

        return ValidatorOpinion(
            confidence_of_result=max(0.0, min(conf, 1.0)),
            confidence_reason=reason
        )
    except Exception:
        logger.exception("Failed to parse Validator response.")
        return None
