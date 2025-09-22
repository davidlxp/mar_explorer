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

def get_query_analysis_tools() -> List[Dict[str, Any]]:
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

def get_system_prompt(schema: Any, products: Dict[str, Any], sql_examples: str) -> str:
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

def get_summarize_tools() -> List[Dict[str, Any]]:
    """
    Get the function schema for summarizing parent task plans.
    """
    return [
        {
            "type": "function",
            "function": {
                "name": "summarize_plans",
                "description": "Create a concise summary of parent task plans",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "summary": {
                            "type": "string",
                            "description": "A concise summary of what parent tasks plan to do and how it might influence the current task"
                        }
                    },
                    "required": ["summary"]
                }
            }
        }
    ]

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
