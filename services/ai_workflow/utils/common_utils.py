"""
query_processor.py
Tools for processing and validating query responses.
"""

import json
from typing import Dict, Any, Optional, Union, List
from pathlib import Path
from services.constants import MAR_TABLE_PATH, NO_TASKS_COMPLETED_YET
from services.ai_workflow.data_model import TableSchema, InputForValidator, TodoIntent, BreakdownQueryResult, PlanningResult
from services.db import get_database
from services.vectorstores import pinecone_store
from services.ai_workflow.data_model import CompletedTask, CompletedTaskResult
import os
from services.constants import PR_FILES_FOLDER_PATH_STR
from services.ai_workflow.data_model import ContextChunk, RetrievalResult
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

db = get_database()

def regularize_sql_query(query: str) -> str:
    """
    Check and regularize a SQL query.
    
    Args:
        query: The SQL query to process
        
    Returns:
        The processed SQL query
    """
    if not query:
        return query
        
    # Ensure correct table name
    if MAR_TABLE_PATH not in query and "mar_combined_m" in query:
        query = query.replace("mar_combined_m", MAR_TABLE_PATH)
    
    # Validate string quotes (Snowflake prefers single quotes)
    if '"' in query:
        query = query.replace('"', "'")
        
    return query

def get_mar_table_schema() -> TableSchema:
    """
    Returns the schema for mar_combined_m table.
    This helps the AI understand the table structure for query generation.
    """
    return TableSchema(
        name=MAR_TABLE_PATH,
        columns={
            "asset_class": "VARCHAR",
            "product_type": "VARCHAR",
            "product": "VARCHAR",
            "year_month": "VARCHAR",
            "year": "NUMBER(4,0)",
            "month": "NUMBER(2,0)",
            "volume": "DOUBLE PRECISION",
            "adv": "DOUBLE PRECISION"
        },
        description="Monthly MAR (Market Activity Report) data containing trading volumes and average daily volumes (adv) by multiple dimensions. Their hierarchies are: asset_class -> product_type -> product. All text value in tablees are in lowercase."
    )

def load_available_products():
    products_file = Path('storage/nlq_context/tradeweb_available_products.json')
    with products_file.open('r') as f:
        products = json.load(f)

    return products

def execute_sql_query(query: str) -> str:
    """
    Submit a SQL query to the database and return the result.
    """
    rows, columns = db.fetchall_with_columns(query)
    result = [columns] + rows
    return result

def execute_vector_query(query: str) -> str:
    """
    Submit a vector query to the database and return the result.
    """
    result = pinecone_store.search_content(query)
    return result

def get_available_products_str() -> str:
    products = load_available_products()
    return f"""
     Available Products Catalog:
        (This is a list of dictionaries, where each dictionary represents a financial product with its asset class, product type, and product name.)
        {products}
    """

def get_mar_table_schema_str() -> str:
    schema = get_mar_table_schema()
    years = execute_sql_query("SELECT DISTINCT year FROM mar_combined_m")
    months = execute_sql_query("SELECT DISTINCT month FROM mar_combined_m")
    return f"""
        Table Schema:
        - Name: {schema.name}
        - Description: {schema.description}
        - Columns:
        {json.dumps(schema.columns, indent=6)}
        - All years available:
        {years}
        - All months available:
        {months}
    """

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
    
def get_completed_tasks_info(tasks_completed: List[CompletedTask] = [], tasks_results: List[CompletedTaskResult] = []) -> str:    
    """
    Get the information of completed tasks.
    """
    if len(tasks_completed) == 0 or len(tasks_results) == 0:
        return NO_TASKS_COMPLETED_YET
    
    tasks_info = []
    for i in range(len(tasks_completed)):
        task = tasks_completed[i]
        result = tasks_results[i]
    
        todo_intent = task.todo_intent.value
        helper_for_action = task.helper_for_action
        task_to_do = task.task_to_do

        summary = f'''
        ::::::::::::::::::::::::
        Task #{i+1}
        Task: {task_to_do}
        The type of the task is: {todo_intent} (numeric means SQL querying / context means vector searching / aggregation means combining results from other tasks)
        Task reason: {task.task_reason}
        The helper used to complete the task is: {helper_for_action}
        Result: {result}
        ::::::::::::::::::::::::
        '''
        tasks_info.append(summary)

    result = '\n'.join(tasks_info)
    return result 

def get_task_breakdown_eg_str() -> str:
    return """
        EXAMPLES:

        Example 1  
        User query: "Compare YoY ADV for cash products in August 2024 vs 2025"  
        Tasks:  
        [
        { "task_to_do": "Query ADV for cash products in August 2024 and 2025",  
            "reason": "Need numeric data for both years before comparison" },  
        { "task_to_do": "Calculate YoY change between 2024 and 2025",  
            "reason": "Computation must be done after retrieving numeric data" }  
        ]

        Example 2  
        User query: "Why did U.S. government bond ADV drop in August 2025?"  
        Tasks:  
        [
        { "task_to_do": "Query ADV for U.S. government bonds in August 2025 vs July 2025",  
            "reason": "Need to confirm that the drop occurred in numeric data" },  
        { "task_to_do": "Search press releases for reasons about U.S. government bond ADV in 2025",  
            "reason": "Requires contextual explanation not available in SQL" }  
        ]

        Example 3  
        BAD:  
        User query: "Get ADV for U.S. and EU credit products in August 2025"  
        ✘ Wrong:  
        - Task 1: Get ADV for U.S.  
        - Task 2: Get ADV for EU  
        ✔ Correct:  
        - Task 1: Query ADV for credit products where region IN ('US','EU') for August 2025  
        Reason: Same operation, one SQL query is enough.  
    """

def get_pr_available_in_storage_str() -> str:
    '''
    Get the press releases available in storage.
    '''
    # file_names = [
    #     file_name
    #     for file_name in os.listdir(PR_FILES_FOLDER_PATH_STR)
    #     if file_name.endswith(".md")
    # ]

    file_names = ['tradeweb_reports-monthly-2025_08']

    return f"""
        Press Releases Available in Storage:
        {file_names}
    """

def parse_pinecone_response(response: Dict[str, Any]) -> RetrievalResult:
    """Parse Pinecone search response into RetrievalResult."""
    try:
        hits = response.get("result", {}).get("hits", [])
        chunks: List[ContextChunk] = []

        for hit in hits:
            fields = hit.get("fields", {})
            chunks.append(
                ContextChunk(
                    id=hit.get("_id", ""),
                    text=fields.get("text", ""),
                    report_type=str(fields.get("report_type", "")),
                    report_name=str(fields.get("report_name", "")),
                    url=fields.get("url", ""),
                    relevance_score=float(hit.get("_score", 0.0))
                )
            )
        return RetrievalResult(chunks=chunks)
    except Exception as e:
        logger.error(f"Error in parse_pinecone_response: {e}", exc_info=True)
        return """ Failed to parse Pinecone response """

def construct_input_for_validator(org_query: str, breakdown_result: BreakdownQueryResult, plan: PlanningResult, task_result: str) -> InputForValidator:
    """
    Construct the input for the validator.
    """
    return InputForValidator(
        org_query=org_query,
        task_done=breakdown_result.task_to_do,
        task_reason=breakdown_result.reason,
        task_intent=plan.todo_intent,
        task_approach=plan.helper_for_action,
        task_result=task_result)

def contruct_task_info_str_for_aggregator(tasks: CompletedTask, tasks_result: CompletedTaskResult) -> str:
    '''
    Construct the task info for the aggregator.
    '''
    output = """
    Below are the tasks the other agents have completed and their results. You should use them to aggregate a final response tailored to the user's query.
    """

    for task, task_result in zip(tasks, tasks_result):
        output += f"""
    Task: {task.task_to_do}
    Task reason: {task.task_reason}
    Task type: {task.todo_intent}
    Helper that used for the task: {task.helper_for_action} (SQL query for NUMERIC; vector search query against press releases for CONTEXT; math expression for CALCULATION)
    Task result: {task_result}
    Task reference: {task.reference}
    """

    output += """
    This is the task that you should do:
    Task: {tasks[-1].task_to_do}
    Task reason: {tasks[-1].task_reason}
    Task type: {tasks[-1].todo_intent}
    """
    return output

