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