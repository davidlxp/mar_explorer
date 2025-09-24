"""
aggregator.py
AI agent for aggregating results from multiple tasks into a final answer.
"""

from typing import List, Dict, Any
import json
from services.ai_workflow.data_model import (
    BreakdownQueryResult,
    PlanningResult,
    SqlResult,
    RetrievalResult,
    AnswerPacket
)
from services.ai_workflow.utils.openai_utils import call_openai
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_aggregator_tools() -> List[Dict[str, Any]]:
    """Get the function schema for result aggregation."""
    return [
        {
            "type": "function",
            "function": {
                "name": "aggregate_results",
                "description": "Aggregate and format results from multiple tasks into a final answer",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "answer": {
                            "type": "string",
                            "description": "Final answer text that directly addresses the user's query"
                        },
                        "citations": {
                            "type": "array",
                            "description": "Citations for data sources used",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "source": {"type": "string"},
                                    "reference": {"type": "string"}
                                },
                                "required": ["source", "reference"]
                            }
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Confidence score between 0 and 1",
                            "minimum": 0,
                            "maximum": 1
                        },
                        "confidence_reason": {
                            "type": "string",
                            "description": "Reasoning for the confidence score"
                        }
                    },
                    "required": ["answer", "citations", "confidence", "confidence_reason"],
                    "additionalProperties": False
                }
            }
        }
    ]

def get_aggregator_system_prompt(all_task_info_str: str) -> str:
    """Get the system prompt for result aggregation."""
    return f"""
You are an expert at aggregating and presenting financial data analysis results.
Your job is to combine results from multiple tasks into a clear, concise, and accurate answer.
Your audience is finance professionals who may not have a technical background.

Guidelines:
1. Always ground your answer in provided task results — never hallucinate.
2. For volume or ADV, assume values are in Million USD unless specified otherwise.
3. Compute overall confidence as the average of task confidences, and explain your reasoning.
4. If data is insufficient, explain what was tried and invite the user to refine their query. And mention that you welcome their follow-up and you will try your best.
5. Citations:
   - For NUMERIC: cite MAR website URL + SQL query
   - For CONTEXT: cite report name + URL + text snippet
   - For CALCULATION: cite the math expression
   - For AGGREGATION: no direct citation

Example Good Output:
Answer: "The ADV for credit products rose 15% YoY to $25.3B in Aug 2025, driven by electronic trading adoption."
Citations: [{{"source":"SQL","reference":"SELECT SUM(adv)..."}},{{"source":"Press Release","reference":"credit PR excerpt"}}]
Confidence: 0.85
Confidence Reason: "Both SQL and PR results are consistent and strongly support the conclusion."

Example Bad Output:
- Just raw numbers without explanation
- Missing or vague citations
- No reasoning for confidence

### All Tasks Info ###
{all_task_info_str}
"""



def aggregate_results(
    user_query: str,
    all_task_info_str: str
) -> AnswerPacket:
    """
    Aggregate results from multiple tasks into a final answer.
    """
    try:
        user_message = f"""Original Query: {user_query}

In your system prompt, you will see the information of all completed tasks which are for answering the original query and their results.
Please aggregate the results of all completed tasks into a clear, concise answer that directly addresses the original query.
The key is making the response clear, concise andsuitable for finance professionals who may not have a technical background.
The goal is to make the answer immediately understandable and useful to someone in a finance department.
"""

        tools = get_aggregator_tools()
        system_prompt = get_aggregator_system_prompt(all_task_info_str)

        response = call_openai(system_prompt, user_message, tools)

        if not response.choices[0].message.tool_calls:
            return AnswerPacket(
                text="Error: Could not aggregate results.",
                citations=[],
                confidence=0.0,
                confidence_reason="No tool call returned"
            )

        result = json.loads(response.choices[0].message.tool_calls[0].function.arguments)

        return AnswerPacket(
            text=result.get("answer", "Error: No answer generated"),
            citations=result.get("citations", []),
            confidence=float(result.get("confidence", 0.0)),
            confidence_reason=result.get("confidence_reason", "No reason provided")
        )

    except Exception as e:
        logger.error(f"Error aggregating results: {e}", exc_info=True)
        return AnswerPacket(
            text=f"Error aggregating results: {str(e)}",
            citations=[],
            confidence=0.0,
            confidence_reason="Exception thrown during aggregation"
        )
