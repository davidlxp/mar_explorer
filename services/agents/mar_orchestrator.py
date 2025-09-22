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
from services.agents.tools.query_breakdown import break_down_query
from services.constants import MAR_TABLE_PATH

from services.db import get_database
db = get_database()


class Intent(str, Enum):
    NUMERIC = "numeric"
    CONTEXT = "context"
    IRRELEVANT = "irrelevant"

@dataclass
class Task:
    """Multiple tasks can be generated for a single user query."""
    intent: Intent
    helper_for_action: Optional[str]  # SQL query or vector search query or None
    confidence: float = 0.0

# Class holding the result of intent analysis
@dataclass
class AnalysisResult:
    """A list of all tasks generated for a single user query."""
    tasks: List[Task]

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
        
        # Convert results to Task objects
        tasks = []
        for task_data in result["tasks"]:
            tasks.append(Task(
                intent=Intent(task_data["intent"]),
                helper_for_action=task_data["helper_for_action"],
                confidence=task_data["confidence"]
            ))
            
        return AnalysisResult(tasks=tasks)
        
    except Exception as e:
        print(f"Error analyzing query: {e}")
        return AnalysisResult(tasks=[
            Task(
                intent=Intent.IRRELEVANT,
                helper_for_action=None,
                confidence=0.0
            )
        ])

def handle_user_query(user_query: str) -> str:

    pass

def handle_user_query(user_query: str) -> AnswerPacket:
    """
    High-level entrypoint:
    1. Break down query into sequential tasks
    2. Analyze each task to determine intent and generate helper
    3. Execute tasks in sequence
    4. Compose final answer
    """
    # First, break down the query into sequential tasks
    task_breakdown = break_down_query(user_query)
    print("\nQuery Breakdown:")
    print("---------------")
    for i, task in enumerate(task_breakdown, 1):
        print(f"\nTask {i}:")
        print(f"What: {task['task']}")
        print(f"Why:  {task['reason']}")
    print("\n---------------")
    
    # Now analyze each task to determine intent and generate helper
    # tasks = analyze_and_decompose(user_query)
    
    # # Execute tasks and compose answer
    # results = []
    # for task in tasks:
    #     if task.intent == Intent.NUMERIC:
    #         results.append(run_numeric_task(task))
    #     elif task.intent == Intent.CONTEXT:
    #         results.append(run_context_task(task))
    #     else:
    #         return AnswerPacket(
    #             text="Sorry, I can only help with MAR numeric or context queries.",
    #             citations=[],
    #             confidence=0.99,
    #         )

    # return compose_final_answer(user_query, tasks, results)

def run_numeric_task() -> SqlResult:
    """
    Map helper name to SQL query, execute against Snowflake.
    Retry up to 3x if query fails.
    """
    
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