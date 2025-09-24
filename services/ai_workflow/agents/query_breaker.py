"""
query_breakdown.py
AI agent for analyzing and breaking down user queries into sequential tasks.
"""

from typing import List, Dict, Any
from openai import OpenAI
from services.constants import MAR_ORCHESTRATOR_MODEL
from services.ai_workflow.data_model import BreakdownQueryResult
from services.ai_workflow.utils.openai_utils import call_openai
import logging
from services.ai_workflow.data_model import CompletedTask, CompletedTaskResult
import services.ai_workflow.utils.common_utils as common_utils
import json

model = MAR_ORCHESTRATOR_MODEL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_breakdown_tools() -> List[Dict[str, Any]]:
    """Single-function tool: return exactly one next atomic task."""
    return [
        {
            "type": "function",
            "function": {
                "name": "break_down_query",
                "description": "Propose the NEXT atomic task toward answering the query (one task only).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "task_to_do": {
                            "type": "string",
                            "description": "One minimal, self-contained action (e.g., 'Query ADV for cash in Aug 2024 & 2025')."
                        },
                        "reason": {
                            "type": "string",
                            "description": "Why this task is needed now, why this specific type of task is needed. And a rough idea of what you plan to do after this task?"
                        }
                    },
                    "required": ["task_to_do", "reason"],
                    "additionalProperties": False
                }
            }
        }
    ]

def get_breakdown_system_prompt(tasks_completed: List[CompletedTask], tasks_results: List[CompletedTaskResult]) -> str:
    """
        Get the system prompt for query breakdown.
        
        Args:
            completed_tasks: Optional list of tasks that have been completed
            completed_results: Optional list of results from completed tasks
    """
    # Output Format String
    output_format_str = """
    OUTPUT FORMAT:  
        A JSON object with a list of tasks:  
        {  
        "tasks": [  
            { "task_to_do": "...", "reason": "..." },  
            { "task_to_do": "...", "reason": "..." }  
        ]  
        }
    """

    # Ask to consider the completed tasks
    completed_tasks_ask = "\nPlease consider these completed tasks and their results when breaking down remaining work.\n"

    # Build completed tasks context if available
    tasks_completed_info = common_utils.get_completed_tasks_info(tasks_completed, tasks_results)

    task_breakdown_eg_str = common_utils.get_task_breakdown_eg_str()
    mar_table_schema_str = common_utils.get_mar_table_schema_str()
    available_products_str = common_utils.get_available_products_str()
    pr_available_in_storage_str = common_utils.get_pr_available_in_storage_str()

    the_prompt = f"""You are the QueryBreaker.  
Your job is to look into a user query and refer to the completed tasks and their results to break remaining work into the **next minimal atomic task(s)** needed to answer it.
You not necessarily will see completed tasks everytime, becuase it could be the beginning of the query handling process.

You must output one task, with these fields:
- task_to_do: what needs to be done (one atomic action)  
- reason: Why do you plan to do this task? And a rough idea of what you plan to do after this task?

CRITICAL RULES:

1. **No Repetition**  
   - Never repeat tasks already in ### tasks_completed ###.  
   - If the needed result already exists, skip and move on (or output AGGREGATION if ready).

2. **Next Step Only**  
   - Output exactly one atomic task at a time.  
   - Each task must be minimal and self-contained.  
   - Later tasks will be proposed in future turns, after this one is done.

3. **Breakdown Principles**  
   - Use a single task if multiple values can be retrieved together (e.g., one SQL query with IN).  
   - Split only if the next step you might imagine about requires a different type of operation (SQL vs Context vs Aggregation) or depends on new results.  
   - If all required inputs are available, propose an AGGREGATION task.

4. **Minimality**  
   - Do not over-split.  
   - Propose only what is strictly necessary for the next step.

------
### OUTPUT FORMAT ###
{output_format_str}
------

### tasks_completed ###
{completed_tasks_ask}
{tasks_completed_info}
------

------
### Some Examples of tasks_completed ###
{task_breakdown_eg_str}
------

------
### For your information below is the table we can run SQL on and the available products catalog ###
{mar_table_schema_str}
{available_products_str}
{pr_available_in_storage_str}    
------
"""
    return the_prompt

def break_down_query(
    query: str,
    completed_tasks: List[Dict[str, Any]] = [],
    completed_results: List[Dict[str, Any]] = [],
) -> List[BreakdownQueryResult]:
    """
    Call the QueryBreaker to get ONE next atomic task.
    Returns [] on any failure so the caller can choose a user-facing fallback.
    """
    try:
        # Get tools and prompt
        tools = get_breakdown_tools()
        system_prompt = get_breakdown_system_prompt(completed_tasks, completed_results)

        # Build user message with completed tasks context
        user_message = query
        if len(completed_tasks) > 0 and len(completed_results) > 0:
            user_message = f"""Original Query: {query}
                            Please break down the remaining work needed for completing the query, please consider the tasks that already completed and never repeat the tasks that already done."""

        # Prefer forcing tool usage if your wrapper supports tool_choice
        tools = get_breakdown_tools()
        response = call_openai(
            system_prompt,
            user_message,
            tools=tools,
            tool_choice={"type": "function", "function": {"name": "break_down_query"}}  # safe if your wrapper passes through
        )

        results = _parse_tool_call_to_result(response)
        if not results:
            logger.warning("QueryBreaker produced no valid next task.")
        return results

    except Exception as e:
        logger.error(f"Error in break_down_query: {e}", exc_info=True)
        return []


def _parse_tool_call_to_result(response) -> List[BreakdownQueryResult]:
    """
    Parse OpenAI tool call into BreakdownQueryResult list (length 1).
    Returns [] if parsing fails.
    """
    try:
        choice = response.choices[0]
        msg = choice.message
        tool_calls = getattr(msg, "tool_calls", None) or []
        if not tool_calls:
            # fallback: try content as JSON if model replied inline (rare)
            content = (msg.content or "").strip()
            if content:
                try:
                    data = json.loads(content)
                    if isinstance(data, dict) and "task_to_do" in data and "reason" in data:
                        return [BreakdownQueryResult(task_to_do=data["task_to_do"], reason=data["reason"])]
                except json.JSONDecodeError:
                    logger.warning("Model returned non-tool, non-JSON content for QueryBreaker.")
            return []

        # Use the first tool call named break_down_query
        tc = next((tc for tc in tool_calls if tc.function.name == "break_down_query"), tool_calls[0])
        args_raw = tc.function.arguments or "{}"
        data = json.loads(args_raw)

        task = data.get("task_to_do", "").strip()
        reason = data.get("reason", "").strip()
        if task and reason:
            return [BreakdownQueryResult(task_to_do=task, reason=reason)]
        else:
            logger.warning(f"Tool args missing fields: {data}")
            return []
    except Exception:
        logger.exception("Failed to parse QueryBreaker tool call.")
        return []