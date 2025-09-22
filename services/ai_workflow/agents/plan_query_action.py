"""
plan_query_action.py
AI agent for planning actions for query.
"""

from typing import Dict, Any, List
import json
from services.ai_workflow.data_model import BreakdownQueryResult, PlanningResult, TodoIntent
from services.ai_workflow.utils.common_utils import get_mar_table_schema, load_available_products
from services.ai_workflow.agents.summarize_parent_plans import summarize_parent_plans
from services.ai_workflow.utils.openai_utils import call_openai
from services.ai_workflow.utils.common_utils import regularize_sql_query

from services.constants import MAR_TABLE_PATH


def get_plan_query_action_tools() -> List[Dict[str, Any]]:
    """
    Get the function schema for query analysis.
    """
    return [
        {
            "type": "function",
            "function": {
                "name": "analyze_query",
                "description": "Analyze user query and determine how to resolve it",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "todo_intent": {
                            "type": "string",
                            "enum": ["numeric", "context", "aggregation"],
                            "description": "The type of action needed to resolve the query. Use 'aggregation' when the task requires combining or processing results from other tasks."
                        },
                        "helper_for_action": {
                            "type": "string",
                            "description": "SQL query for numeric intent, search query for context intent"
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Confidence score between 0 and 1 indicating how confident the model is about the intent and helper action are related to this query",
                            "minimum": 0,
                            "maximum": 1
                        },
                        "confidence_reason": {
                            "type": "string",
                            "description": "Explanation of why the model assigned this confidence score"
                        }
                    },
                    "required": ["todo_intent", "helper_for_action", "confidence", "confidence_reason"]
                }
            }
        }
    ]

def get_plan_query_action_system_prompt(schema: Any, products: Dict[str, Any], sql_examples: str) -> str:
    """
    Generate the system prompt for the OpenAI model.
    
    Args:
        schema: Table schema information
        products: Available products catalog
        sql_examples: SQL query examples
    """
    import json
    
    # Format available products
    asset_classes_str = ", ".join(products["asset_classes"])
    product_types_str = ", ".join(products["product_types"])
    products_by_type_str = json.dumps(products["products_by_type"], indent=2)

    return f"""You are an analyst expert for financial MAR data.
    When receiving a query from user, carefully analyze it and break it down into structured tasks as needed.
    
    Task types:
    - 'numeric' → It means to answer user's query, you need to need to generate a SQL query to execute against the Snowflake database.
    - 'context' → It means to answer user's query, you need to need to generate a natural language query to search financial press releases related content.
    - 'aggregation' → It means this task will combine, aggregate, or transform results from other tasks. No helper_for_action needed.
    
    IMPORTANT: Task Breakdown Rules
    1. If a query contains multiple questions or comparisons, create separate tasks for each one.
       Example: "What is ADV for cash products and credit products?"
       → Create two numeric tasks:
         - One for cash products ADV
         - One for credit products ADV
    
    2. If a query requires both data and context, create multiple tasks with different intents.
       Example: "What is the ADV trend and why did it change?"
       → Create two tasks:
         - numeric task for ADV data
         - context task for trend explanation
         
    3. Use aggregation intent for combining or processing tasks.
       Example: "What's the percentage difference in ADV between US and EU products?"
       → Create three tasks:
         - numeric task: Get US ADV data
         - numeric task: Get EU ADV data
         - aggregation task: Calculate percentage difference (depends on previous tasks)
    
    For numeric tasks:
    1. You need to generate a SQL query and populate the helper_for_action field of functional call.
    2. It must be a query that can be executed against the Snowflake database.
    3. It must be a query that can be executed against the {MAR_TABLE_PATH} table. See the schema below.
    4. When you need to filter by asset_class, product_type, or product, you need to use the values from the available products catalog below.
    5. All content in SQL if it's a string are in lowercase.

    For context tasks:
    1. You need to understand the intention of the user's query and generate a natural language query and populate the helper_for_action field of functional call.
    2. It will be used to search the financial press releases related content in Vector Database.

    For aggregation tasks:
    1. Set helper_for_action to null (aggregations are done after getting parent task results)
    2. Make sure to set appropriate dependencies on tasks that provide the data
    3. Set confidence based on how certain you are about the aggregation approach
    
    For irrelevant queries:
    1. Return null as helper_for_action
    2. Set confidence to 1.0.

    Available Products Catalog:
    - Asset Classes: {asset_classes_str}
    - Product Types: {product_types_str}
    - Products by Category:
    {products_by_type_str}

    Table Schema:
    - Name: {schema.name}
    - Description: {schema.description}
    - Columns:
      {json.dumps(schema.columns, indent=6)}

    SQL EXAMPLES:
    {sql_examples}
    
    IMPORTANT: Only use asset_class, product_type, and product values from the above catalog.
    If a filter value is not in the catalog, do not include that parameter."""

def plan_query_action(task: BreakdownQueryResult, parent_plans: Dict[int, PlanningResult] = None) -> PlanningResult:
    """
    Analyze task to determine intent and generate appropriate helper (SQL/vector query).
    
    Args:
        task: The task to plan
        parent_plans: Dictionary of previously planned tasks, used for context
        
    Returns:
        AnalysisResult containing:
        - todo_intent: The type of action needed (numeric/context)
        - helper_for_action: SQL query for numeric intent, search query for context intent
    """
    try:
        # Load required data
        schema = get_mar_table_schema()
        products = load_available_products()
        sql_examples = get_sql_eg_plan_query_action()
        
        # Get parent plan summary if available
        parent_summary = summarize_parent_plans(task, parent_plans) if parent_plans else ""
        parent_context = f"\nParent Task Context:\n{parent_summary}" if parent_summary else ""


        ###### Testing Below ######
        print("\n\n")
        print("--------------------------------")
        print("\n")
        print(parent_context)
        print("\n")
        print("--------------------------------")
        print("\n\n")

        ###### Testing Above ######
        
        # Get tools and prompt
        tools = get_plan_query_action_tools()
        system_prompt = get_plan_query_action_system_prompt(schema, products, sql_examples) + parent_context
        
        # Call OpenAI
        response = call_openai(system_prompt, task.task_to_do, tools)
        
        # Parse and validate response
        result = parse_plan_query_action_response(response)
        
        return PlanningResult(
            task_id=task.task_id,
            dependency_on=task.dependency_on,
            task_to_do=task.task_to_do,
            todo_intent=TodoIntent(result["todo_intent"]),
            helper_for_action=result["helper_for_action"],
            confidence=result["confidence"],
            confidence_reason=result["confidence_reason"]
        )
        
    except Exception as e:
        print(f"Error analyzing query: {e}")
        return PlanningResult(
            task_id=task.task_id,
            dependency_on=task.dependency_on,
            task_to_do=task.task_to_do,
            todo_intent=TodoIntent.CONTEXT,
            helper_for_action=None,
            confidence=0.0,  # Low confidence due to error
            confidence_reason=f"Error occurred during analysis: {str(e)}"
        )

def plan_all_tasks(breakdown_results: List[BreakdownQueryResult]) -> Dict[int, PlanningResult]:
    """
    Plan all tasks in dependency order.
    
    Args:
        breakdown_results: List of tasks with their dependencies
        
    Returns:
        Dictionary mapping task_id to its planning result
    """
    all_planned_results = {}
    
    # Keep processing until all tasks are done
    while len(all_planned_results) < len(breakdown_results):
        # Find tasks that can be executed (all dependencies satisfied)
        for task in breakdown_results:
            if task.task_id in all_planned_results:
                continue
                
            # If dependencies are not met yet, skip
            if task.dependency_on and not task.dependency_on.issubset(all_planned_results.keys()):
                continue
            
            print(f"\Planning Task {task.task_id}:")
            if task.dependency_on:
                print(f"Using results from Task {task.dependency_on}")
            
            # Execute the task with parent plans
            res = plan_query_action(task, all_planned_results)
            
            # Store results
            all_planned_results[task.task_id] = res
    
    return all_planned_results

def parse_plan_query_action_response(response: Any) -> Dict[str, Any]:
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
    
    # Check and regularize SQL queries
    if todo_intent == "numeric" and helper_for_action:
        helper_for_action = regularize_sql_query(helper_for_action)
    
    # Extract confidence fields
    confidence = tool_args.get("confidence", 0.5)  # Default to 0.5 if not provided
    confidence_reason = tool_args.get("confidence_reason", "No confidence reason provided")
    
    return {
        "todo_intent": todo_intent,
        "helper_for_action": helper_for_action,
        "confidence": confidence,
        "confidence_reason": confidence_reason
    }

def get_sql_eg_plan_query_action() -> str:
    """
    Returns example SQL queries for the mar_combined_m table.
    These examples help the AI understand proper Snowflake SQL syntax and table usage.
    """
    return f"""
Example Queries:

1. Get total volume for all products in August 2025:
   SELECT SUM(volume) as total_volume
   FROM {MAR_TABLE_PATH}
   WHERE year = 2025 
     AND month = 8;

2. Get total volume for credit derivatives in August 2025:
   SELECT SUM(volume) as total_volume
   FROM {MAR_TABLE_PATH}
   WHERE year = 2025 
     AND month = 8
     AND asset_class = 'credit'
     AND product_type = 'derivatives';

3. Get monthly ADV trend for US ETFs in 2025:
   SELECT year, month, AVG(adv) as average_daily_volume
   FROM {MAR_TABLE_PATH}
   WHERE year = 2025
     AND product = 'us etfs'
   GROUP BY year, month
   ORDER BY year, month;

Note: The table contains:
- Volumes are stored in the 'volume' column (DOUBLE PRECISION)
- ADV (Average Daily Volume) in the 'adv' column (DOUBLE PRECISION)
- Time dimensions: year (NUMBER) and month (NUMBER)
- Product dimensions: asset_class, product_type, product (all VARCHAR)
"""