"""
mar_orchestrator.py
High-level orchestrator for handling MAR queries with Snowflake + Pinecone (and optional Web search).
"""

from typing import Optional
from dataclasses import dataclass
from enum import Enum
from services.agents.mar_helper import get_mar_table_schema, load_available_products, get_sql_examples
from services.agents.tools.openai_tools import get_query_analysis_tools, get_system_prompt, call_openai
from services.agents.tools.query_processor import parse_openai_response

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
    try:
        # Load required data
        schema = get_mar_table_schema()
        products = load_available_products()
        sql_examples = get_sql_examples()
        
        # Get tools and prompt
        tools = get_query_analysis_tools()
        system_prompt = get_system_prompt(schema, products, sql_examples)
        
        # Call OpenAI
        response = call_openai(system_prompt, user_query, tools)
        
        # Parse and validate response
        result = parse_openai_response(response)
        
        return AnalysisResult(
            intent=Intent(result["intent"]),
            helper_for_action=result["helper_for_action"],
            confidence=result["confidence"]
        )
        
    except Exception as e:
        print(f"Error analyzing query: {e}")
        return AnalysisResult(
            intent=Intent.IRRELEVANT,
            helper_for_action=None,
            confidence=0.0
        )