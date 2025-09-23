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
                            "description": "The final answer text that combines all task results"
                        },
                        "citations": {
                            "type": "array",
                            "description": "List of citations for data sources used",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "source": {
                                        "type": "string",
                                        "description": "Source of the data (e.g. 'SQL', 'VectorDB')"
                                    },
                                    "reference": {
                                        "type": "string",
                                        "description": "Reference details (e.g. SQL query, search query)"
                                    }
                                },
                                "required": ["source", "reference"]
                            }
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Confidence score between 0 and 1 for the aggregated answer",
                            "minimum": 0,
                            "maximum": 1
                        }
                    },
                    "required": ["answer", "citations", "confidence"]
                }
            }
        }
    ]

def get_aggregator_system_prompt() -> str:
    """Get the system prompt for result aggregation."""
    return """You are an expert at aggregating and presenting financial data analysis results.
    Your job is to combine results from multiple tasks into a clear, concise, and accurate answer.

    Guidelines for Aggregation:

    1. Result Types You'll Handle:
       - SQL Results: Contain numeric data from database queries
       - Vector Search Results: Contain contextual information from documents
       - Previous Task Results: May need to reference or build upon earlier findings

    2. Answer Format:
       - Start with the key findings/numbers
       - Follow with supporting context or explanations
       - Use clear, professional financial language
       - Format numbers appropriately (e.g., millions as 'MM', billions as 'B')
       - Include proper units (e.g., USD, %, bps)

    3. Confidence Scoring:
       - Consider completeness of data
       - Consider quality of each source
       - Consider how well sources complement each other
       - Lower confidence if data seems inconsistent
       - Lower confidence if key information is missing

    4. Citations:
       - Include ALL sources used
       - Reference specific queries or searches
       - Note which parts of answer came from where

    Example Good Aggregation:

    Tasks:
    1. SQL query for ADV data
    2. Vector search for market changes
    
    Answer:
    "The average daily volume (ADV) for credit products increased 15% YoY to $25.3B in August 2025. 
    This growth was primarily driven by increased electronic trading adoption in the corporate bond 
    market, particularly in investment grade securities."

    Citations:
    [
        {
            "source": "SQL",
            "reference": "SELECT SUM(adv)... GROUP BY year"
        },
        {
            "source": "VectorDB",
            "reference": "Search: credit products market changes 2025"
        }
    ]

    Confidence: 0.85 (strong data quality, consistent narrative)

    Example Bad Aggregation (Don't Do This):
    - Just listing raw numbers without context
    - Missing citations
    - Not explaining calculation methods
    - Inconsistent number formatting
    """

def aggregate_results(
    user_query: str,
    completed_tasks: List[Dict[str, Any]],
    task_results: Dict[int, Any]  # Maps task_id to SQL/Vector/Aggregation results
) -> AnswerPacket:
    """
    Aggregate results from multiple tasks into a final answer.
    
    Args:
        user_query: The original user query
        completed_tasks: List of completed tasks
        task_results: Dictionary mapping task_id to its results
        
    Returns:
        AnswerPacket containing the final answer
    """
    try:
        # Format task results for the prompt
        task_context = []

        print("\n\n")
        print("--------------------------------")
        print("\n")
        print(f"Completed tasks number: {len(completed_tasks)}")
        print(f"Task result number: {len(task_results)}")
        print("\n")
        print("--------------------------------")
        print("\n\n")

        for i in range(len(completed_tasks)):
            task = completed_tasks[i]

            task_id = task["task_id"]
            task_to_do = task["task_to_do"]
            result = task_results[i]

            curr_context = f""" Task {task_id}: The goal is to {str(task_to_do)}. The result is {str(result)}."""
            task_context.append(curr_context)

        # Build user message
        user_message = f"""Original Query: {user_query}

                    Completed Tasks and Results:
                    {task_context}

                    Please aggregate these results into a clear, concise answer that directly addresses the original query."""

        # Get tools and prompt
        tools = get_aggregator_tools()
        system_prompt = get_aggregator_system_prompt()
        
        # Call OpenAI
        response = call_openai(system_prompt, user_message, tools)
        
        # Parse response
        if not response.choices[0].message.tool_calls:
            return AnswerPacket(
                text="Error: Could not aggregate results.",
                citations=[],
                confidence=0.0
            )
            
        # Extract aggregation
        result = json.loads(response.choices[0].message.tool_calls[0].function.arguments)
        
        return AnswerPacket(
            text=result["answer"],
            citations=result["citations"],
            confidence=result["confidence"]
        )
        
    except Exception as e:
        print(f"Error aggregating results: {str(e)}")
        return AnswerPacket(
            text=f"Error aggregating results: {str(e)}",
            citations=[],
            confidence=0.0
        )
