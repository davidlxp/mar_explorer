# ------------------------------
# Module: db.py
# Description: Database class for the services
# ------------------------------

import duckdb
import pathlib
from typing import Optional

DB_PATH = pathlib.Path("storage/mar_explorer.duckdb")

class Database:
    def __init__(self, db_path: str = DB_PATH):
        self.con = duckdb.connect(str(db_path))
        self._init_schema()

    def _init_schema(self): 
        # Create tables if they don’t already exist
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            ts TIMESTAMP DEFAULT current_timestamp,
            question TEXT,
            confidence FLOAT,
            citations TEXT
        );
        """)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS pr_index (
            id INTEGER,
            text TEXT,
            embedding FLOAT[1536]
        );
        """)

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
