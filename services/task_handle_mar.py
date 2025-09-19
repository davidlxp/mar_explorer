# ------------------------------
# Module: task_handle_mar.py
# Description: Module handles the mar upload.
# ------------------------------

import pandas as pd
from datetime import datetime
import os
import duckdb
from pathlib import Path
from services.db import Database
import logging
from services.schemas import *
from services.utils import enforce_schema
from services.constants import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Instantiate DB object
db = Database()

# Supported MAR tabs to parse
mar_tabs = MAR_SHEETS_TO_FILE_MAPPINGS.keys()

# For debugging purposes only
pd.set_option("display.max_rows", None)     # show all rows
pd.set_option("display.max_columns", None) # show all columns
pd.set_option("display.width", None)       # don't wrap to next line
pd.set_option("display.max_colwidth", None) # show full column content

def handle_mar_update(file):
    '''
        Module handles the mar upload process.
    '''
    # Parse the MAR file into files for each tab
    for tab in mar_tabs:
        parse_mar_to_file(file, tab)

    update_db_with_latest_mar()
    return True

def parse_mar_to_file(file, sheet_name):
    '''
        Module ingests TABs of MAR file into the database.
        It supports ADV, Volume, Trade Days tabs across monthly, quarterly and yearly data.

        Args:
            file: The path to the MAR file
            sheet_name: The name of the sheet to ingest
    '''
    # :::::: Data Loading :::::: #

    # Supported sheet names
    supported_sheet_names = ["ADV - M", "Volume - M", "Trade Days - M"]

    if sheet_name not in supported_sheet_names:
        raise ValueError(f"Unsupported sheet name: {sheet_name}")

    logger.info(f"Processing {sheet_name} from {file}")
    
    # Load starting from row 2 (headers are Asset Class, Product, then months)
    df = pd.read_excel(file, sheet_name=sheet_name, header=1)

    # :::::: Define Variables :::::: #

    # Define the value column name
    if sheet_name in ['ADV - M', 'Volume - M']:
        value_col_name = 'volume'
        schema = MAR_VOLUME_M_SCHEMA
    elif sheet_name in ['Trade Days - M']:
        value_col_name = 'trade_days'
        schema = MAR_TRADE_DAYS_M_SCHEMA

    # :::::: Data Cleaning :::::: #

    # Regularize column names and add name for the 3rd column
    df.rename(columns={df.columns[2]: 'product_type'}, inplace=True)
    df.rename(columns={col: col.lower().replace(" ", "_") for col in df.columns}, inplace=True)

    # Regularize the format for each cell to prepare for filtering process
    df = df.map(lambda x: x.strip().lower() if isinstance(x, str) else x)

    # Forward fill asset_class and product hierarchies
    ffill_cols = ['asset_class', 'product']
    for col in ffill_cols:
        df[col] = df[col].ffill()

    # Remove rows where it's standing for Total or Grand Total
    mask = (
        (df['asset_class'].astype(str).isin(['total', 'grand total'])) |
        (df['product'].astype(str).isin(['total', 'grand total'])) |
        (df['product_type'].astype(str).isin(['total', 'grand total']))
    )
    df = df[~mask]

    # Melt months into rows
    avoid_melt_cols = ['asset_class', 'product', 'product_type']
    df = df.melt(
        id_vars = avoid_melt_cols,
        var_name = 'month_year',
        value_name = value_col_name
    )

    # Create year and month columns
    df['month_year_dt'] = pd.to_datetime(df['month_year'], format='%b_%Y')
    df['year'] = df['month_year_dt'].dt.year
    df['month'] = df['month_year_dt'].dt.month

    # Convert year_month with format 'YYYY-MM'
    df['year_month'] = df['month_year_dt'].apply(lambda x: f"{x.year}-{x.month:02d}")

    # Drop unnecessary columns
    df = df.drop(columns=['month_year_dt', 'month_year'])

    # Fill the value column with 0 if it's NaN
    df[value_col_name] = df[value_col_name].fillna(0)

    # Enforce the schema
    df = enforce_schema(df, schema)

    logger.info(f'Data cleaning and schema enforcement complete for {sheet_name}')

    # :::::: Prepare to Save :::::: #

    # Get the latest month and year and use it as the output directory
    df_dt_sorted = df.sort_values(['year', 'month']).iloc[-1]
    latest_year_month = df_dt_sorted['year_month']

    # Build the output directory
    out_dir = f'storage/snapshots/mar/{latest_year_month}'
    os.makedirs(out_dir, exist_ok=True)

    # Save the latest parsed MAR file as a parquet
    out_file_name = MAR_SHEETS_TO_FILE_MAPPINGS[sheet_name]
    df.to_parquet(f'{out_dir}/{out_file_name}', index=False)

    logger.info(f'Saved {sheet_name} to {out_dir}/{out_file_name}')

    return True

def update_db_with_latest_mar():
    '''
        Module ingests all parsed MAR files into the database.
    '''
    # Get the folder with the latest data (folder name is the YYYY-MM)
    folder_path = Path(MAR_FILES_FOLDER_PATH_STR)
    latest_year_month = max((p for p in folder_path.iterdir() if p.is_dir()),
                            key=lambda x: datetime.strptime(x.name, "%Y-%m")).name
    
    # The files' path
    latest_files_path = f'{MAR_FILES_FOLDER_PATH_STR}/{latest_year_month}'

    # Load files to tables according to the mappings
    for file_name, table_name in MAR_FILE_TO_TABLE_MAPPINGS.items():
        db.run_query(f"""
            CREATE OR REPLACE VIEW {table_name} AS
            SELECT *
            FROM read_parquet('{latest_files_path}/{file_name}');
        """)

        logger.info(f'Loaded {file_name} to {table_name}')

    logger.info(f'Loaded the latest MAR files to tables. The latest year and month is {latest_year_month}')

    return True