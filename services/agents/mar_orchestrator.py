"""
mar_orchestrator.py
High-level orchestrator for handling MAR queries with Snowflake + Pinecone (and optional Web search).
"""

from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum
from simpleeval import simple_eval
from openai import OpenAI
from services.agents.mar_prompts import SQL_HELPER_CATALOG_STR

from services.constants import MAR_ORCHESTRATOR_MODEL, MAR_DATABASE_NAME

mar_database_name = MAR_DATABASE_NAME

client = OpenAI()
model = MAR_ORCHESTRATOR_MODEL

# ================================================================
# ENUMS, DATA MODELS
# ================================================================

class Intent(str, Enum):
    NUMERIC = "numeric"
    CONTEXT = "context"
    IRRELEVANT = "irrelevant"


@dataclass
class DateSpec:
    year: int
    month: int = -1      # -1 = no month
    quarter: int = -1    # -1 = no quarter


@dataclass
class Task:
    """A decomposed unit of work (SQL, Pinecone, Web)."""
    intent: Intent
    helper: str                # SQL helper name or strategy (semantic, filter)
    params: Dict[str, Any]     # product, year, month, etc.


@dataclass
class SqlResult:
    rows: List[Dict[str, Any]]
    cols: List[str]
    source: str = "snowflake"


@dataclass
class ContextChunk:
    id: str
    text: str
    meta: Dict[str, Any]
    score: float


@dataclass
class RetrievalResult:
    chunks: List[ContextChunk]
    confidence: float
    strategy: str


@dataclass
class AnswerPacket:
    text: str
    citations: List[Dict[str, Any]]
    confidence: float


# ================================================================
# ORCHESTRATOR ENTRYPOINT
# ================================================================

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


# ================================================================
# 1. ANALYSIS + DECOMPOSITION
# ================================================================

def analyze_and_decompose(user_query: str) -> List[Task]:
    """
    Use LLM function calling to parse query into structured tasks.
    """

    # 1. Define the schema for tasks
    tools = [
        {
            "type": "function",
            "function": {
                "name": "decompose_query",
                "description": "Decompose user query into multiple tasks as needed.for pulling financial MAR data use SQL, or getting context about the financial movement from Pinecone.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "tasks": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "intent": {
                                        "type": "string",
                                        "enum": ["numeric", "context", "irrelevant"]
                                    },
                                    "helper": {
                                        "type": "string",
                                        "description": "SQL helper name (for numeric), or 'semantic' (for context)."
                                    },
                                    "params": {
                                        "type": "object",
                                        "description": "Parameters like asset_class, product, product_type, year, month, quarter as needed."
                                    }
                                },
                                "required": ["intent", "helper", "params"]
                            }
                        }
                    },
                    "required": ["tasks"]
                },
            },
        }
    ]

    # 2. Call the LLM with function schema
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an analyst expert for financial MAR data.\n"
                    "When receiving query from user, break down user queries into structured tasks as needed.\n"
                    "- 'numeric' → SQL helper queries (pick best helper name)\n"
                    "- 'context' → Pinecone queries ('semantic')\n"
                    "- 'irrelevant' → outside scope\n"
                    "Each task must include 'params': product, year, month, quarter where relevant.\n"
                    "Multiple tasks if multiple comparisons or products are mentioned."
                    + SQL_HELPER_CATALOG_STR
                ),
            },
            {"role": "user", "content": user_query},
        ],
        tools=tools,
        tool_choice="auto",
    )

    # 3. Parse the tool call
    message = response.choices[0].message
    if not message.tool_calls:
        return [Task(intent=Intent.IRRELEVANT, helper="none", params={})]

    tool_args = message.tool_calls[0].function.arguments
    try:
        import json
        data = json.loads(tool_args)
        
        # 4. Convert to Task dataclasses
        tasks = []
        for t in data.get("tasks", []):
            # Validate required fields
            if not all(key in t for key in ["intent", "helper", "params"]):
                print(f"Warning: Malformed task data: {t}")
                continue
                
            intent = Intent(t["intent"])
            helper = t["helper"]
            params = t["params"]
            
            tasks.append(Task(intent=intent, helper=helper, params=params))
            
        if not tasks:
            return [Task(intent=Intent.IRRELEVANT, helper="none", params={})]
            
        return tasks
    except json.JSONDecodeError as e:
        print(f"Error parsing OpenAI response: {e}")
        return [Task(intent=Intent.IRRELEVANT, helper="none", params={})]
    except Exception as e:
        print(f"Unexpected error processing tasks: {e}")
        return [Task(intent=Intent.IRRELEVANT, helper="none", params={})]

    return tasks


# ================================================================
# 2. EXECUTION FUNCTIONS
# ================================================================

def run_numeric_task(task: Task) -> SqlResult:
    """
    Map helper name to SQL query, execute against Snowflake.
    Retry up to 3x if query fails.
    """
    # TODO: implement using Snowflake connector
    return SqlResult(rows=[], cols=[])


def run_context_task(task: Task) -> RetrievalResult:
    """
    Run Pinecone retrieval.
    Currently semantic-only, but keep structure flexible for filter-first.
    """
    # TODO: implement Pinecone query
    return RetrievalResult(chunks=[], confidence=0.0, strategy="semantic")


def run_web_task(task: Task) -> Dict[str, Any]:
    """
    Optional extension: query Google Search API / Gemini.
    Not implemented yet.
    """
    return {}


# ================================================================
# 3. VALIDATION FUNCTIONS
# ================================================================

def validate_numeric(rows: List[Dict[str, Any]]) -> bool:
    """Check if SQL returned non-empty and reasonable results."""
    return bool(rows)


def validate_relevance(chunks: List[ContextChunk]) -> float:
    """
    Score relevance of Pinecone results.
    Can be a mini-LLM, or heuristic with embedding similarity.
    """
    return 0.8 if chunks else 0.0


# ================================================================
# 4. ANSWER COMPOSITION
# ================================================================

def compose_final_answer(user_query: str, tasks: List[Task], results: List[Any]) -> AnswerPacket:
    """
    Main AI agent fuses multiple results (SQL + Pinecone + Web) into natural language.
    """
    # TODO: Implement with LLM call
    return AnswerPacket(
        text="This is a placeholder answer.",
        citations=[],
        confidence=0.7
    )


# ================================================================
# 5. UTILITY FUNCTIONS
# ================================================================

def calculate_expression(expression: str) -> float:
  """
  Safely evaluate arithmetic expressions like:
    "2500000000 / 365"
    "(2.5e12 - 2.4e12) / 2.4e12 * 100"
  """
  # Use Python's ast or sympy for safe eval, not eval()
  return simple_eval(expression)


# ================================================================
# 5. SQL HELPERS
# ================================================================

# :::::: Helper functions for SQL queries :::::: #

def _build_where_clause(
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    month_start: Optional[int] = None,
    month_end: Optional[int] = None,
    asset_class: Optional[List[str]] = None,
    product_type: Optional[List[str]] = None,
    product: Optional[List[str]] = None,
) -> str:
    """
    Build a dynamic WHERE clause with support for ranges and IN statements.
    Handles both single values and multiple selections.
    """
    conditions = []
    # Year filter
    if year_start is not None and year_end is not None:
        if year_start == year_end:
            conditions.append(f"year = {year_start}")
        else:
            conditions.append(f"year BETWEEN {year_start} AND {year_end}")
    elif year_start is not None:
        conditions.append(f"year = {year_start}")

    # Month filter
    if month_start is not None and month_end is not None:
        if month_start == month_end:
            conditions.append(f"month = {month_start}")
        else:
            conditions.append(f"month BETWEEN {month_start} AND {month_end}")
    elif month_start is not None:
        conditions.append(f"month = {month_start}")

    # Asset class filter
    if asset_class:
        if isinstance(asset_class, str):
            values = f"'{asset_class}'"
        else:
            values = ", ".join([f"'{v}'" for v in asset_class])
        conditions.append(f"asset_class IN ({values})")

    # Product filter
    if product:
        if isinstance(product, str):
            values = f"'{product}'"
        else:
            values = ", ".join([f"'{v}'" for v in product])
        conditions.append(f"product IN ({values})")

    # Product type filter
    if product_type:
        if isinstance(product_type, str):
            values = f"'{product_type}'"
        else:
            values = ", ".join([f"'{v}'" for v in product_type])
        conditions.append(f"product_type IN ({values})")

    return " AND ".join(conditions) if conditions else "1=1"

  
def _top_entities_by_metric(
    year: int,
    month: Optional[int] = None,
    entity: str = "product",   # "asset_class", "product_type", "product"
    metric: str = "volume",    # "volume" or "adv"
    top_n: int = 5,
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
    Generalized function to return SQL for top N entities (product, asset_class, product_type)
    ranked by volume or ADV, with optional filters.
    """
    where_clause = _build_where_clause(
        year_start=year, month_start=month,
        asset_class=asset_class, product=product, product_type=product_type
    )

    agg_expr = "SUM(volume)" if metric == "volume" else "AVG(adv)"

    return f"""
    SELECT {entity}, {agg_expr} AS {metric}
    FROM {mar_database_name}
    WHERE {where_clause}
    GROUP BY {entity}
    ORDER BY {metric} DESC
    LIMIT {top_n};
    """

# :::::: Point lookup :::::: #

def get_total_volume(
    year: int,
    month: int,
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
      Returns SQL for total trading volume with optional filters.
    """
    where_clause = _build_where_clause(
        year_start=year, month_start=month,
        asset_class=asset_class, product_type=product_type, product=product
    )
    return f"""
    SELECT SUM(volume) AS total_volume
    FROM {mar_database_name}
    WHERE {where_clause};
    """

def get_adv(
    year: int,
    month: int,
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
      Returns SQL for average daily volume (ADV) with optional filters.
    """
    where_clause = _build_where_clause(
        year_start=year, month_start=month,
        asset_class=asset_class, product_type=product_type, product=product
    )
    return f"""
    SELECT AVG(adv) AS adv
    FROM {mar_database_name}
    WHERE {where_clause};
    """

def compare_yoy_volume(
    year: int,
    month: int,
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
    Returns SQL for year-over-year comparison of volume for given filters.
    """
    prev_year = year - 1
    where_clause = _build_where_clause(
        year_start=prev_year, year_end=year,
        month_start=month, month_end=month,
        asset_class=asset_class, product_type=product_type, product=product
    )
    return f"""
    SELECT year, month, SUM(volume) AS total_volume
    FROM {mar_database_name}
    WHERE {where_clause}
    GROUP BY year, month
    ORDER BY year;
    """
  
def compare_mom_volume(
    year: int,
    month: int,
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
    Returns SQL for month-over-month comparison of volume for given filters.
    Example: compare Aug 2025 vs Jul 2025.
    """
    prev_year, prev_month = (year, month - 1) if month > 1 else (year - 1, 12)

    where_clause = _build_where_clause(
        year_start=prev_year, year_end=year,
        month_start=prev_month, month_end=month,
        asset_class=asset_class, product_type=product_type, product=product
    )

    return f"""
    SELECT year, month, SUM(volume) AS total_volume
    FROM {mar_database_name}
    WHERE {where_clause}
    GROUP BY year, month
    ORDER BY year, month;
    """

# Asset class-level
def top_asset_classes_by_volume(
    year: int,
    month: Optional[int] = None,
    top_n: int = 5,
    product: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
) -> str:
    """
        Returns SQL for top N asset classes by trading volume.

        Examples:
          top_asset_classes_by_volume(year=2025, month=8, top_n=3)
            → Top 3 asset classes by volume in Aug 2025.
          
          top_asset_classes_by_volume(year=2024, top_n=5, product_type="derivative")
            → Top 5 asset classes by volume in 2024, restricted to derivative products.

          top_asset_classes_by_volume(year=2025, month=6, product="swaps")
            → Top asset classes by volume in Jun 2025, but only counting swaps.
    """
    return _top_entities_by_metric(
        year=year, month=month, entity="asset_class", metric="volume",
        top_n=top_n, product=product, product_type=product_type
    )

def top_asset_classes_by_adv(
    year: int,
    month: Optional[int] = None,
    top_n: int = 5,
    product: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
) -> str:
    """
      Returns SQL for top N asset classes by ADV.
      Similar to top_asset_classes_by_volume, but with avg(ADV) instead of sum(volume).

      Examples:
        top_asset_classes_by_adv(year=2025, top_n=5)
          → Top 5 asset classes by ADV in 2025.
    """
    return _top_entities_by_metric(
        year=year, month=month, entity="asset_class", metric="adv",
        top_n=top_n, product=product, product_type=product_type
    )

# Product type-level
def top_product_types_by_volume(
    year: int,
    month: Optional[int] = None,
    top_n: int = 5,
    asset_class: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
      Returns SQL for top N product types by trading volume.

      Examples:
        top_product_types_by_volume(year=2025, month=8, top_n=1)
          → Top 1 product types (product type e.g., cash vs derivative) by volume in Aug 2025.
        
      It can filter on Asset Class and Product too.
    """
    return _top_entities_by_metric(
        year=year, month=month, entity="product_type", metric="volume",
        top_n=top_n, asset_class=asset_class, product=product
    )

def top_product_types_by_adv(
    year: int,
    month: Optional[int] = None,
    top_n: int = 5,
    asset_class: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
      Returns SQL for top N product types by ADV.
      Similar to top_product_types_by_volume, but with avg(ADV) instead of sum(volume).

      Examples:
        top_product_types_by_adv(year=2025, top_n=3)
          → Top 3 product types by ADV in 2025.
    """
    return _top_entities_by_metric(
        year=year, month=month, entity="product_type", metric="adv",
        top_n=top_n, asset_class=asset_class, product=product
    )

# Product-level
def top_products_by_volume(
    year: int,
    month: int,
    top_n: int = 5,
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
) -> str:
    """
      Returns SQL for top N products (leaf-level instruments, e.g. swaps, futures) by trading volume.

      Examples:
        top_products_by_volume(year=2025, month=8, top_n=5)
          → Top 5 products (e.g. swaps, futures) by volume in Aug 2025.

        top_products_by_volume(year=2024, month=12, top_n=3, asset_class="credit", product_type="derivative")
          → Top 3 product by volume in Dec 2024, under credit asset class and derivative product type.
    """
    return _top_entities_by_metric(
        year=year, month=month, entity="product", metric="volume",
        top_n=top_n, asset_class=asset_class, product_type=product_type
    )

def top_products_by_adv(
    year: int,
    month: Optional[int] = None,
    top_n: int = 5,
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
) -> str:
    """
      Returns SQL for top N products (leaf-level instruments) by ADV.
      Similar to top_products_by_volume, but with avg(ADV) instead of sum(volume).

      Examples:
        top_products_by_adv(year=2025, month=8, top_n=5)
          → Top 5 products (e.g. us government bonds, swaps) by ADV in Aug 2025.

        top_products_by_adv(year=2024, top_n=10, asset_class="rates", product_type="cash")
          → Top 10 cash products in Rates by ADV in 2024.
    """
    return _top_entities_by_metric(
        year=year, month=month, entity="product", metric="adv",
        top_n=top_n, asset_class=asset_class, product_type=product_type
    )

# :::::: Aggregates :::::: #

def total_volume_by_entity(
    year: int,
    month: Optional[int] = None,
    entity: str = "product_type",  # "product", "asset_class", "product_type"
    product_type: Optional[Union[str, List[str]]] = None,
    asset_class: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
    Returns SQL for total trading volume grouped by a given entity.
    
    Examples:
      total_volume_by_entity(year=2025, entity="asset_class")
        → Total volume by asset class in 2025.

      total_volume_by_entity(year=2024, month=8, entity="product_type")
        → Total volume by product type in Aug 2024.
    """
    where_clause = _build_where_clause(
        year_start=year, month_start=month,
        asset_class=asset_class, product=product, product_type=product_type
    )

    return f"""
    SELECT {entity}, SUM(volume) AS total_volume
    FROM {mar_database_name}
    WHERE {where_clause}
    GROUP BY {entity}
    ORDER BY total_volume DESC;
    """

def trend_adv(
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
    year_start: int = None,
    year_end: int = None,
) -> str:
    """
    Returns SQL for ADV trend for a given product/asset_class/product_type
    across a year range.

    Examples:
      - trend_adv(asset_class="credit", year_start=2025, year_end=2025)
        → ADV trend for credit in 2025.
      - trend_adv(product_type="chinese bonds", year_start=2020, year_end=2025)
        → ADV trend for Chinese Bonds from 2020 through 2025.
    """
    where_clause = _build_where_clause(
        year_start=year_start, year_end=year_end,
        asset_class=asset_class, product=product, product_type=product_type
    )

    return f"""
    SELECT year, month, AVG(adv) AS adv
    FROM {mar_database_name}
    WHERE {where_clause}
    GROUP BY year, month
    ORDER BY year, month;
    """

def month_over_month_volume(
    year: int,
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
    Returns SQL for monthly volume trend within a year.

    Examples:
      month_over_month_volume(year=2025, product_type="us etfs")
        → Monthly volume trend for US ETFs in 2025.
    """
    where_clause = _build_where_clause(
        year_start=year, year_end=year,
        asset_class=asset_class, product=product, product_type=product_type
    )

    return f"""
    SELECT year, month, SUM(volume) AS total_volume
    FROM {mar_database_name}
    WHERE {where_clause}
    GROUP BY year, month
    ORDER BY year, month;
    """

def ytd_volume(
    year: int,
    upto_month: int,
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
    Returns SQL for year-to-date volume up to a given month.

    Examples:
      ytd_volume(year=2025, upto_month=6, product="cash")
        → YTD Cash volume Jan–Jun 2025.
    """
    where_clause = _build_where_clause(
        year_start=year, month_start=1, month_end=upto_month,
        asset_class=asset_class, product=product, product_type=product_type
    )

    return f"""
    SELECT SUM(volume) AS ytd_volume
    FROM {mar_database_name}
    WHERE {where_clause};
    """

def pct_change_adv(
    year1: int, month1: int,
    year2: int, month2: int,
    asset_class: Optional[Union[str, List[str]]] = None,
    product_type: Optional[Union[str, List[str]]] = None,
    product: Optional[Union[str, List[str]]] = None,
) -> str:
    """
    Returns SQL for percent change in ADV between two periods.

    Examples:
      pct_change_adv(year1=2024, month1=8, year2=2025, month2=8, product="cash")
        → % change in cash ADV from Aug 2024 → Aug 2025.

      pct_change_adv(year1=2024, month1=12, year2=2025, month2=1, asset_class="rates")
        → % change in Rates ADV from Dec 2024 → Jan 2025.
    """
    where_clause = _build_where_clause(
        asset_class=asset_class, product=product, product_type=product_type
    )

    return f"""
    WITH base AS (
        SELECT year, month, AVG(adv) AS adv
        FROM {mar_database_name}
        WHERE {where_clause}
          AND ((year = {year1} AND month = {month1})
            OR (year = {year2} AND month = {month2}))
        GROUP BY year, month
    )
    SELECT
        MAX(CASE WHEN year = {year1} AND month = {month1} THEN adv END) AS adv1,
        MAX(CASE WHEN year = {year2} AND month = {month2} THEN adv END) AS adv2,
        (MAX(CASE WHEN year = {year2} AND month = {month2} THEN adv END) -
         MAX(CASE WHEN year = {year1} AND month = {month1} THEN adv END)) 
         / NULLIF(MAX(CASE WHEN year = {year1} AND month = {month1} THEN adv END), 0) * 100 
         AS pct_change
    FROM base;
    """
