# ------------------------------
# Module: task_handle_mar.py
# Description: Module handles the mar upload.
# ------------------------------

import pandas as pd
from datetime import datetime
import os
import duckdb
from pathlib import Path
from services.db import get_database
import logging
from services.schemas import MAR_VOLUME_SCHEMA, MAR_TRADE_DAYS_SCHEMA, MAR_COMBINED_SCHEMA
from services.utils import enforce_schema
from services.constants import *
import services.crawler as crawler
import asyncio
import services.utils as utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Instantiate DB object
db = get_database()

# Supported MAR tabs to parse
mar_tabs = MAR_SHEETS_TO_FILE_MAPPINGS.keys()

# For debugging purposes only
pd.set_option("display.max_rows", None)     # show all rows
pd.set_option("display.max_columns", None) # show all columns
pd.set_option("display.width", None)       # don't wrap to next line
pd.set_option("display.max_colwidth", None) # show full column content

def crawl_latest_mar_file():
    '''
        Module crawls the latest MAR file from the website
        For website location, see MAR_FIND_URL in constants.py
    '''
    # :::::: Crawl to find the MAR file :::::: #

    # Crawl the website
    result = asyncio.run(crawler.crawl_one(MAR_FIND_URL))
    internal_links = result['internal_links']

    # Get the hrefs
    hrefs = [link['href'] for link in internal_links if 
            link['href'] is not None and link['href'].startswith(MAR_URL_DOMAIN)]

    # Regularize the hrefs
    hrefs = [utils.regularize_url(href) for href in hrefs]

    # Find only the ones that end with .xlsx
    xlsx_hrefs = [href for href in hrefs if href.endswith('.xlsx')]

    # Find the one for MAR file
    mar_href = [href for href in xlsx_hrefs if MAR_URL_LAST_PART_NAME_PATTERN in href.split('/')[-1]][0]

    # :::::: Prepare to Download :::::: #

    # Get the last part from the href without the suffix
    # It looks like https://www.tradeweb.com/4a51d0/globalassets/newsroom/09.05.25-august-mar/tw-historical-adv-and-day-count-through-august-2025.xlsx
    last_part = mar_href.split('/')[-1]
    last_part = Path(last_part).stem
    
    # Extract the YYYY_MM from the last part
    year = last_part.split('-')[-1]
    month_str = last_part.split('-')[-2]

    # Convert the month to the format MM
    month = datetime.strptime(month_str, "%B").strftime("%m")

    # Name the file
    mar_file_name = f"{MAR_FILE_NAME_PATTERN}-{year}_{month}.xlsx"

    # :::::: Download the File :::::: #

    # Download the file
    crawler.download_file(mar_href, MAR_RAW_FILES_FOLDER_PATH_STR, mar_file_name)

def get_latest_mar_file_from_storage():
    """
    Find the latest MAR file in the raw files folder.
    The function looks for files starting with variable MAR_FILE_NAME_PATTERN in constants.py 
    and ending with '.xlsx', then finds the one with the latest date in its name (format: YYYY_MM).
    
    Returns:
        str: Path to the latest MAR file
    """
    # Get the directory path from constants
    mar_dir = MAR_RAW_FILES_FOLDER_PATH_STR
    
    # List all files in the directory that match the pattern
    mar_files = []
    for file in os.listdir(mar_dir):
        if file.startswith(MAR_FILE_NAME_PATTERN) and file.endswith('.xlsx'):

            # Extract the date part (YYYY_MM) from the filename
            date_str = file.replace(MAR_FILE_NAME_PATTERN + "-", '').replace('.xlsx', '')

            # Convert to datetime for comparison
            try:
                date = datetime.strptime(date_str, '%Y_%m')
                mar_files.append((date, os.path.join(mar_dir, file)))

            except ValueError:
                # Skip files that don't match the expected date format
                continue
    
    if not mar_files:
        raise FileNotFoundError(f"No MAR files found in {mar_dir}")
    
    # Sort by date and get the latest file
    latest_file = max(mar_files, key=lambda x: x[0])[1]
    return latest_file

def update_mar_with_latest_file():
    '''
        Module updates the MAR with the latest file in storage
        For storage location, see MAR_RAW_FILES_FOLDER_PATH_STR in constants.py
    '''
    # :::::: Crawl to find the latest MAR file and download it :::::: #
    crawl_latest_mar_file()

    # :::::: Get the latest MAR file :::::: #
    latest_file_path = get_latest_mar_file_from_storage()

    # :::::: Handle the MAR update :::::: #
    handle_mar_update(latest_file_path)

def handle_mar_update(file):
    '''
        Module handles the mar upload process.
    '''
    # Parse the MAR file into files for each tab
    for tab in mar_tabs:
        parse_mar_to_file(file, tab)

    # Combine the latest MAR files
    combine_latest_mar(file_type='monthly')

    # Update the database with the latest combined MAR files
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
        schema = MAR_VOLUME_SCHEMA
    elif sheet_name in ['Trade Days - M']:
        value_col_name = 'trade_days'
        schema = MAR_TRADE_DAYS_SCHEMA

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

    # Add updated_at timestamp as ISO format string
    df['updated_at'] = pd.Timestamp.now().isoformat()

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

def combine_latest_mar(file_type='monthly'):
    '''
    Combines the latest MAR files based on the file type (monthly, quarterly, yearly).
    Currently supports combining ADV and Volume files.
    
    Args:
        file_type (str): Type of files to combine. One of 'monthly', 'quarterly', 'yearly'
        
    Returns:
        bool: True if operation was successful
    '''
    # Map file_type to suffix
    type_suffix_map = {
        'monthly': '_m',
        'quarterly': '_q',
        'yearly': '_y'
    }
    
    if file_type not in type_suffix_map:
        logger.error(f'Invalid file_type: {file_type}. Must be one of {list(type_suffix_map.keys())}')
        return False
        
    suffix = type_suffix_map[file_type]
    
    # Get the folder with the latest data
    folder_path = Path(MAR_FILES_FOLDER_PATH_STR)
    latest_year_month = max((p for p in folder_path.iterdir() if p.is_dir()),
                        key=lambda x: datetime.strptime(x.name, "%Y-%m")).name
    
    # The files' path
    latest_files_path = f'{MAR_FILES_FOLDER_PATH_STR}/{latest_year_month}'
    adv_file = f'{latest_files_path}/mar_adv{suffix}.parquet'
    volume_file = f'{latest_files_path}/mar_volume{suffix}.parquet'
    
    # Check if both files exist
    if os.path.exists(adv_file) and os.path.exists(volume_file):
        logger.info(f'Found both ADV and Volume files ({file_type}) in {latest_year_month}. Combining them...')
        
        # Read both files
        df_adv = pd.read_parquet(adv_file)
        df_volume = pd.read_parquet(volume_file)
        
        # Rename volume column in ADV to avg_volume
        df_adv = df_adv.rename(columns={'volume': 'avg_volume'})
        
        # Perform full outer join
        df_combined = pd.merge(
            df_adv,
            df_volume,
            on=['asset_class', 'product', 'product_type', 'year_month'],
            how='outer',
            suffixes=('', '_y')  # Only add suffix to right table's duplicate columns
        )
        
        # Drop duplicate year and month columns from the right table
        columns_to_drop = ['year_y', 'month_y', 'updated_at_y']  # Also drop the duplicate updated_at
        df_combined = df_combined.drop(columns=[col for col in columns_to_drop if col in df_combined.columns])

        # Update the timestamp for the combined file as ISO format string
        df_combined['updated_at'] = pd.Timestamp.now().isoformat()

        # Enforce the schema
        df_combined = enforce_schema(df_combined, MAR_COMBINED_SCHEMA)
        
        # Save the combined file
        combined_file = f'{latest_files_path}/mar_combined{suffix}.parquet'
        df_combined.to_parquet(combined_file, index=False)
        logger.info(f'Saved combined file to {combined_file}')

        return True
    else:
        raise FileNotFoundError(f'Either ADV or Volume file ({file_type}) is missing. No combination can be done.')

def update_db_with_latest_mar():
    '''
    Module ingests the combined MAR files into the database.
    Uses MAR_FILE_NAMES_FOR_DB to determine which files to ingest
    and MAR_COMBINED_SCHEMA for schema validation.
    '''
    # Get the folder with the latest data (folder name is the YYYY-MM)
    folder_path = Path(MAR_FILES_FOLDER_PATH_STR)
    latest_year_month = max((p for p in folder_path.iterdir() if p.is_dir()),
                            key=lambda x: datetime.strptime(x.name, "%Y-%m")).name
    
    # The files' path
    latest_files_path = f'{MAR_FILES_FOLDER_PATH_STR}/{latest_year_month}'

    # Find all files to be ingested
    files_to_ingest = []
    for file_name in MAR_FILE_NAMES_FOR_DB:
        file_path = f'{latest_files_path}/{file_name}'
        if os.path.exists(file_path):
            # Get table name by removing file extension
            table_name = MAR_FILE_TO_TABLE_MAPPINGS[file_name]
            files_to_ingest.append((file_path, table_name))
            logger.info(f'Found file to ingest: {file_path}')

    # Load files to tables
    for file_path, table_name in files_to_ingest:
        try:
            db.replace_data_in_table(file_path, table_name, schema=MAR_COMBINED_SCHEMA)
            logger.info(f'Loaded {file_path} to {table_name}')
        except Exception as e:
            logger.error(f'Failed to load {file_path} to {table_name}: {str(e)}')

    if files_to_ingest:
        logger.info(f'Loaded the latest MAR files to tables. The latest year and month is {latest_year_month}')
    else:
        logger.info(f'No MAR files found to ingest in {latest_year_month}')
    
    return True