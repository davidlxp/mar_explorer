"""
query_processor.py
Tools for processing and validating query responses.
"""

import json
from typing import Dict, Any, Optional, Union, List
from pathlib import Path
from services.constants import MAR_TABLE_PATH
from services.ai_workflow.data_model import TableSchema
from services.db import get_database
from services.vectorstores import pinecone_store

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

def submit_sql_query(query: str) -> str:
    """
    Submit a SQL query to the database and return the result.
    """
    result = db.fetchall(query)
    return result

def submit_vector_query(query: str) -> str:
    """
    Submit a vector query to the database and return the result.
    """
    result = pinecone_store.search_content(query)
    return result