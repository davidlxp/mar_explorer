"""
mar_helper.py
Helper functions for MAR data analysis and query generation.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Union
import json
from pathlib import Path
from services.constants import MAR_TABLE_PATH

@dataclass
class TableSchema:
    """Represents the schema of the mar_combined_m table"""
    name: str
    columns: Dict[str, str]  # column_name -> data_type
    description: str

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

def get_sql_examples() -> str:
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