"""
query_processor.py
Tools for processing and validating query responses.
"""

import json
from typing import Dict, Any, Optional, Union, List
from pathlib import Path
from services.constants import MAR_TABLE_PATH
from services.ai_workflow.data_model import TableSchema

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
        description="Monthly MAR (Market Activity Report) data containing trading volumes and average daily volumes (adv) by multiple dimensions. Their hierarchies are: asset_class -> product_type -> product."
    )

def load_available_products() -> Dict[str, Union[List[str], Dict[str, List[str]]]]:
    """
    Load and parse the available products catalog.
    Returns a dictionary containing asset classes, product types, and products by type.
    """
    try:
        products_file = Path('storage/nlq_context/tradeweb_available_products.json')
        with products_file.open('r') as f:
            products = json.load(f)
            
        # Extract unique values
        asset_classes = sorted(set(p["ASSET_CLASS"] for p in products))
        product_types = sorted(set(p["PRODUCT_TYPE"] for p in products))
        products_by_type = {}
        
        # Group products by asset class and product type
        for p in products:
            key = f"{p['ASSET_CLASS']}/{p['PRODUCT_TYPE']}"
            if key not in products_by_type:
                products_by_type[key] = []
            products_by_type[key].append(p["PRODUCT"])
            
        return {
            "asset_classes": asset_classes,
            "product_types": product_types,
            "products_by_type": products_by_type
        }
    except Exception as e:
        print(f"Warning: Could not load available products: {e}")
        return {
            "asset_classes": [],
            "product_types": [],
            "products_by_type": {}
        }