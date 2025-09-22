"""
mar_orchestrator.py
High-level orchestrator for handling MAR queries with Snowflake + Pinecone (and optional Web search).
"""

import json
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum
from openai import OpenAI
from services.constants import MAR_ORCHESTRATOR_MODEL, MAR_TABLE_PATH, MAR_TABLE_PATH
from services.agents.mar_helper import get_mar_table_schema, load_available_products, get_sql_examples

# Initialize OpenAI client
client = OpenAI()
model = MAR_ORCHESTRATOR_MODEL

class Intent(str, Enum):
    NUMERIC = "numeric"
    CONTEXT = "context"
    IRRELEVANT = "irrelevant"

@dataclass
class AnalysisResult:
    """Result of query analysis containing intent and action helper."""
    intent: Intent
    helper_for_action: Optional[str]  # SQL query or vector search query or None
    confidence: float = 0.0

# ================================================================
# ANALYSIS + DECOMPOSITION
# ================================================================

def analyze_and_decompose(user_query: str) -> AnalysisResult:
    """
    Analyze user query to determine intent and generate appropriate helper (SQL/vector query).
    
    Args:
        user_query: The user's natural language query
        
    Returns:
        AnalysisResult containing:
        - intent: The type of query (numeric/context/irrelevant)
        - helper_for_action: SQL query for numeric intent, search query for context intent, None for irrelevant
        - confidence: Model's confidence in the analysis
    """
    # Load schema and available products
    schema = get_mar_table_schema()
    products = load_available_products()
    
    # Format available products for the prompt
    asset_classes_str = ", ".join(products["asset_classes"])
    product_types_str = ", ".join(products["product_types"])
    products_by_type_str = json.dumps(products["products_by_type"], indent=2)

    # Define the function schema for OpenAI
    tools = [
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
                            "description": "Confidence score between 0 and 1"
                        }
                    },
                    "required": ["intent", "confidence"]
                }
            }
        }
    ]

    # 2. Call the LLM with function schema
    system_prompt = f"""You are an analyst expert for financial MAR data.
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

    Available Products Catalog:
    - Asset Classes: {asset_classes_str}
    - Product Types: {product_types_str}
    - Products by Category:
    {products_by_type_str}

    {MAR_TABLE_PATH} Schema:
    {schema}
    
    IMPORTANT: Only use asset_class, product_type, and product values from the above catalog.
    If a filter value is not in the catalog, do not include that parameter.
"""

    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {"role": "user", "content": user_query},
        ],
        tools=tools,
        tool_choice="auto",
    )

    # Parse response
    message = response.choices[0].message
    if not message.tool_calls:
        return AnalysisResult(
            intent=Intent.IRRELEVANT,
            helper_for_action=None,
            confidence=0.0
        )

    try:
        # Extract the analysis
        tool_args = json.loads(message.tool_calls[0].function.arguments)
        
        intent = Intent(tool_args["intent"])
        helper_for_action = tool_args.get("helper_for_action")
        confidence = tool_args.get("confidence", 0.0)

        # Additional validation for numeric queries
        if intent == Intent.NUMERIC and helper_for_action:
            # Ensure table name is correct
            if MAR_TABLE_PATH not in helper_for_action and "mar_combined_m" in helper_for_action:
                helper_for_action = helper_for_action.replace("mar_combined_m", MAR_TABLE_PATH)
            
            # Validate string quotes
            if '"' in helper_for_action:  # Snowflake prefers single quotes
                helper_for_action = helper_for_action.replace('"', "'")
        
        return AnalysisResult(
            intent=intent,
            helper_for_action=helper_for_action,
            confidence=confidence
        )
        
    except json.JSONDecodeError as e:
        print(f"Error parsing OpenAI response: {e}")
        return AnalysisResult(
            intent=Intent.IRRELEVANT,
            helper_for_action=None,
            confidence=0.0
        )
    except Exception as e:
        print(f"Unexpected error processing response: {e}")
        return AnalysisResult(
            intent=Intent.IRRELEVANT,
            helper_for_action=None,
            confidence=0.0
        )


# ================================================================
# 2. EXECUTION FUNCTIONS
# ================================================================

# def run_numeric_task(task: Task) -> SqlResult:
#     """
#     Map helper name to SQL query, execute against Snowflake.
#     Retry up to 3x if query fails.
#     """
#     # TODO: implement using Snowflake connector
#     return SqlResult(rows=[], cols=[])


# def run_context_task(task: Task) -> RetrievalResult:
#     """
#     Run Pinecone retrieval.
#     Currently semantic-only, but keep structure flexible for filter-first.
#     """
#     # TODO: implement Pinecone query
#     return RetrievalResult(chunks=[], confidence=0.0, strategy="semantic")



# ================================================================
# 3. VALIDATION FUNCTIONS
# ================================================================

def validate_numeric(rows: List[Dict[str, Any]]) -> bool:
    """Check if SQL returned non-empty and reasonable results."""
    return bool(rows)


# def validate_relevance(chunks: List[ContextChunk]) -> float:
#     """
#     Score relevance of Pinecone results.
#     Can be a mini-LLM, or heuristic with embedding similarity.
#     """
#     return 0.8 if chunks else 0.0


# ================================================================
# 4. ANSWER COMPOSITION
# ================================================================

# def compose_final_answer(user_query: str, tasks: List[Task], results: List[Any]) -> AnswerPacket:
#     """
#     Main AI agent fuses multiple results (SQL + Pinecone + Web) into natural language.
#     """
#     # TODO: Implement with LLM call
#     return AnswerPacket(
#         text="This is a placeholder answer.",
#         citations=[],
#         confidence=0.7
#     )


# ================================================================
# 5. UTILITY FUNCTIONS
# ================================================================

# def calculate_expression(expression: str) -> float:
#   """
#   Safely evaluate arithmetic expressions like:
#     "2500000000 / 365"
#     "(2.5e12 - 2.4e12) / 2.4e12 * 100"
#   """
#   # Use Python's ast or sympy for safe eval, not eval()
#   return simple_eval(expression)



