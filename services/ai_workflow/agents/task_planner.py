"""
plan_query_action.py
AI agent for planning actions for query.
"""
import json
from typing import Dict, Any, List
import logging
from services.ai_workflow.data_model import BreakdownQueryResult, PlanningResult, TodoIntent
from services.ai_workflow.utils.common_utils import get_mar_table_schema, load_available_products
from services.ai_workflow.utils.openai_utils import call_openai
from services.ai_workflow.utils.common_utils import regularize_sql_query
from services.ai_workflow.utils.common_utils import get_sql_eg_plan_query_action
import services.ai_workflow.utils.common_utils as common_utils

from services.constants import MAR_TABLE_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_plan_query_action_tools() -> List[Dict[str, Any]]:
    """Tool schema: analyze a single task into intent + helper."""
    return [
        {
            "type": "function",
            "function": {
                "name": "analyze_query",
                "description": "Analyze one task and decide if it's numeric (SQL), context (vector search), aggregation, or calculation.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "todo_intent": {
                            "type": "string",
                            "enum": ["numeric", "context", "aggregation", "calculation"],
                            "description": "Action type: SQL query, vector search, aggregation, or calculation."
                        },
                        "helper_for_action": {
                            "type": ["string", "null"],
                            "description": "SQL query for numeric; search string for context; null if aggregation; math expression for calculation."
                        }
                    },
                    "required": ["todo_intent", "helper_for_action"],
                    "additionalProperties": False
                }
            }
        }
    ]

def get_plan_query_action_system_prompt() -> str:
    schema_str = common_utils.get_mar_table_schema_str()
    products_str = common_utils.get_available_products_str()
    sql_examples_str = common_utils.get_sql_eg_plan_query_action()
    pr_available_in_storage_str = common_utils.get_pr_available_in_storage_str()
    
    return f"""
You are the Task Planner. Your job is to analyze ONE task and output:
- todo_intent: 'numeric', 'context', 'aggregation', or 'calculation'
- helper_for_action: SQL (if numeric), search query (if context), null if aggregation, math expression (string) if calculation.

Rules:
- For numeric: generate valid Snowflake SQL against the schema provided below.
  * For asset_class, product, product_type, only use values from product catalog.
  * All string literals lowercase.
- For context: produce a precise natural language search string for press releases.
- For aggregation: set helper_for_action to null.
- For calculation: 
  * Generate a safe, explicit math expression using numbers from previous results or the current task.
  * Good Example: (2500 - 2200) / 2200 * 100
  * Bad Example 1: 2500 - / 2200 (malformed, invalid operator sequence)
  * Bad Example 2: (growth_rate * revenue) (ambiguous variable names, not concrete numbers)
  * Avoid ambiguous symbols; use *, /, +, - only.
  * Always resolve percentages into decimals (e.g. 15% → 0.15).

### Schema ###
{schema_str}

### Products Catalog ###
{products_str}

### SQL Examples ###
{sql_examples_str}

### Press Releases Available in Storage ###
{pr_available_in_storage_str}
"""


def plan_query_action(task: BreakdownQueryResult) -> PlanningResult:
    try:
        tools = get_plan_query_action_tools()
        system_prompt = get_plan_query_action_system_prompt()

        response = call_openai(
            system_prompt,
            task.task_to_do,
            tools,
            tool_choice={"type": "function", "function": {"name": "analyze_query"}}
        )

        parsed = _parse_plan_response(response)
        if not parsed:
            return None

        return PlanningResult(
            todo_intent=TodoIntent(parsed["todo_intent"]),
            helper_for_action=parsed["helper_for_action"]
        )
    except Exception as e:
        logger.error(f"Error planning query action: {e}", exc_info=True)
        return None

def _parse_plan_response(response: Any) -> Dict[str, Any]:
    """Extract todo_intent + helper_for_action from tool call."""
    try:
        tool_calls = response.choices[0].message.tool_calls or []
        if not tool_calls:
            return {"todo_intent": "context", "helper_for_action": None}

        args = json.loads(tool_calls[0].function.arguments)
        intent = args.get("todo_intent", "context")
        helper = args.get("helper_for_action")

        if intent == "numeric" and helper:
            helper = regularize_sql_query(helper)

        return {"todo_intent": intent, "helper_for_action": helper}
    except Exception as e:
        logger.error(f"Error parsing plan response: {e}", exc_info=True)
        return None

