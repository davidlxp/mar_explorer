# ------------------------------
# Module: snowflake.py
# Description: Database class for the services
# ------------------------------

import snowflake.connector
from typing import Optional
import os
import duckdb
import pathlib
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MIGRATIONS_DIR = pathlib.Path("services/migrations/snowflake")

from .base import Database

class SnowflakeDB(Database):
    def __init__(self):
        self.conn = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
            database='mar_explorer',
            schema='main'
        )

    def _run_migrations(self):
        '''
            Run all .sql migrations in order.
        '''
        migrations = sorted(Path(MIGRATIONS_DIR).glob("*.sql"))
        for sql_file in migrations:
            logger.info(f"Running migration: {sql_file}")
            with open(sql_file, "r") as f:
                sql = f.read()
                cur = self.conn.cursor()
                cur.execute(sql)
    
    def run_query(self, query: str, params: Optional[tuple] = None):
        cur = self.conn.cursor()
        return cur.execute(query, params or ())
    
    def fetchall(self, query: str, params: Optional[tuple] = None):
        return self.run_query(query, params).fetchall()
    
    def fetchdf(self, query: str, params: Optional[tuple] = None):
        cur = self.conn.cursor()
        return cur.execute(query, params or ()).fetch_pandas_all()

    def _get_snowflake_type(self, python_type: str) -> str:
        """Convert Python/Pandas dtype to Snowflake type."""
        type_mapping = {
            "string": "VARCHAR",
            "str": "VARCHAR",
            "object": "VARCHAR",
            "int32": "NUMBER(10,0)",
            "int64": "NUMBER(19,0)",
            "float32": "FLOAT",
            "float64": "DOUBLE PRECISION",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP_NTZ",
        }
        return type_mapping.get(python_type, "VARCHAR")

    def replace_data_in_table(self, file_path: str, table_name: str, schema: Optional[dict] = None) -> None:
        """Replace table data with contents from a parquet file."""
        # Create a temporary stage
        stage_name = f"{table_name}_stage"
        self.run_query(f"""
            CREATE OR REPLACE TEMPORARY STAGE {stage_name}
            FILE_FORMAT = (TYPE = PARQUET);
        """)
        
        # Use Snowflake's PUT command to upload the file
        cur = self.conn.cursor()
        put_command = f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cur.execute(put_command)
        
        # If no schema provided, try to infer from parquet metadata
        if schema is None:
            # Use DuckDB to read parquet metadata (it's faster than pandas for this)
            con = duckdb.connect()
            schema = dict(con.execute(f"SELECT * FROM read_parquet('{file_path}') LIMIT 0").description)
            con.close()
        
        # Build the column definitions dynamically
        column_defs = []
        for col_name, col_type in schema.items():
            snowflake_type = self._get_snowflake_type(col_type)
            column_defs.append(f"$1:{col_name}::{snowflake_type} as {col_name}")
        
        # Create or replace the table from staged file
        self.run_query(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT {','.join(column_defs)}
            FROM @{stage_name}/{os.path.basename(file_path)};
        """)