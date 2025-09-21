# ------------------------------
# Module: duckdb.py
# Description: Database class for the services
# ------------------------------

import duckdb
import pathlib
from typing import Optional
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_PATH = pathlib.Path("storage/mar_explorer.duckdb")
MIGRATIONS_DIR = pathlib.Path("services/migrations/duckdb")

from .base import Database

class DuckDB(Database):
    def __init__(self, db_path: str = DB_PATH):
        self.con = duckdb.connect(str(db_path))
    
    def _run_migrations(self):
        '''
            Run all .sql migrations in order.
        '''
        migrations = sorted(Path(MIGRATIONS_DIR).glob("*.sql"))
        for sql_file in migrations:
            logger.info(f"Running migration: {sql_file}")
            with open(sql_file, "r") as f:
                sql = f.read()
                self.con.execute(sql)

    def run_query(self, query: str, params: Optional[tuple] = None):
        '''
            Run a SQL query safely across DBs.
        '''
        if hasattr(self.con, "execute"):
            # DuckDB style
            return self.con.execute(query, params or ())
        else:
            cur = self.con.cursor()
            return cur.execute(query, params or ())

    def fetchall(self, query: str, params: Optional[tuple] = None):
        '''
            Convenience method to return rows.
        '''
        return self.run_query(query, params).fetchall()

    def fetchdf(self, query: str, params: Optional[tuple] = None):
        '''
            DuckDB-specific: directly return a pandas DataFrame.
        '''
        return self.run_query(query, params).df()
