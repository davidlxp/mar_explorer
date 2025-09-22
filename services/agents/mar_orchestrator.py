"""
mar_orchestrator.py
High-level orchestrator for handling MAR queries with Snowflake + Pinecone (and optional Web search).
"""

from typing import Optional
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any

from services.agents.mar_helper import get_mar_table_schema, load_available_products, get_sql_examples
from services.agents.tools.openai_tools import get_query_analysis_tools, get_system_prompt, call_openai
from services.agents.tools.query_processor import parse_openai_response
from services.constants import MAR_TABLE_PATH


class Intent(str, Enum):
    NUMERIC = "numeric"
    CONTEXT = "context"
    IRRELEVANT = "irrelevant"

# Class holding the result of intent analysis
@dataclass
class AnalysisResult:
    """Result of query analysis containing intent and action helper."""
    intent: Intent
    helper_for_action: Optional[str]  # SQL query or vector search query or None
    confidence: float = 0.0

# Class holding the result of a SQL query (numeric task)
@dataclass
class SqlResult:
    rows: List[Dict[str, Any]]
    cols: List[str]
    source: str = f"snowflake.{MAR_TABLE_PATH}"

# Class holding the final answer to user's query
@dataclass
class AnswerPacket:
    text: str
    citations: List[Dict[str, Any]]
    confidence: float

# Class holding information about a context chunk
@dataclass
class ContextChunk:
    id: str
    text: str
    meta: Dict[str, Any]
    score: float

# Class holding the result of a context query against Vector Database
@dataclass
class RetrievalResult:
    chunks: List[ContextChunk]
    confidence: float
    strategy: str


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

def handle_user_query(user_query: str) -> AnswerPacket:
    """
    High-level entrypoint:
    1. Analyze & decompose query → tasks
    2. Execute tasks (SQL, Pinecone, Web)
    3. Validate results
    4. Compose final answer
    """
    tasks = analyze_and_decompose(user_query)

    results = []
    for task in tasks:
        if task.intent == Intent.NUMERIC:
            results.append(run_numeric_task(task))
        elif task.intent == Intent.CONTEXT:
            results.append(run_context_task(task))
        elif task.intent == Intent.WEB:
            results.append(run_web_task(task))  # Future extension
        else:
            return AnswerPacket(
                text="Sorry, I can only help with MAR numeric or context queries.",
                citations=[],
                confidence=0.99,
            )

    return compose_final_answer(user_query, tasks, results)

def run_numeric_task() -> SqlResult:
    """
    Map helper name to SQL query, execute against Snowflake.
    Retry up to 3x if query fails.
    """
    # TODO: implement using Snowflake connector
    return SqlResult(rows=[], cols=[])

def run_context_task() -> RetrievalResult:
    """
    Run Pinecone retrieval.
    Currently semantic-only, but keep structure flexible for filter-first.
    """
    # TODO: implement Pinecone query
    return RetrievalResult(chunks=[], confidence=0.0, strategy="semantic")

def compose_final_answer(user_query: str, results: List[Any]) -> AnswerPacket:
    """
    Main AI agent fuses multiple results (SQL + Pinecone + Web) into natural language.
    """
    # TODO: Implement with LLM call
    return AnswerPacket(
        text="This is a placeholder answer.",
        citations=[],
        confidence=0.7
    )