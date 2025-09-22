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
                "description": "Analyze user query and generate appropriate SQL or search query",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "intent": {
                            "type": "string",
                            "enum": ["numeric", "context", "irrelevant"],
                            "description": "The type of query being made"
                        },
                        "helper_for_action": {
                            "type": "string",
                            "description": "SQL query for numeric intent, search query for context intent, null for irrelevant"
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Confidence score between 0 and 1. How confident that you analyzed the user's intention correctly."
                        }
                    },
                    "required": ["intent", "helper_for_action", "confidence"]
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
    When receiving a query from user, break it down into structured tasks as needed.
    
    Task types:
    - 'numeric' → It means to answer user's query, you need to need to generate a SQL query to execute against the Snowflake database.
    - 'context' → It means to answer user's query, you need to need to generate a natural language query to search financial press releases related content.
    - 'irrelevant' → It means the query is outside the scope of the MAR data.
    
    For numeric tasks:
    1. You need to generate a SQL query and populate the helper_for_action field of functional call.
    2. It must be a query that can be executed against the Snowflake database.
    3. It must be a query that can be executed against the {MAR_TABLE_PATH} table. See the schema below.
    4. When you need to filter by asset_class, product_type, or product, you need to use the values from the available products catalog below.
    5. All content in SQL if it's a string are in lowercase.

    For context tasks:
    1. You need to understand the intention of the user's query and generate a natural language query and populate the helper_for_action field of functional call.
    2. It will be used to search the financial press releases related content in Vector Database.

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
