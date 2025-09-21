# services/db/__init__.py
import os
from typing import Type
from .base import Database
from .duckdb import DuckDB
from .snowflake import SnowflakeDB
from services.constants import DB_PROVIDER

def get_database() -> Database:
    """
    Factory function to get the appropriate database instance based on environment configuration.
    Returns either a DuckDB or SnowflakeDB instance based on the DB_PROVIDER environment variable.
    """
    provider = DB_PROVIDER.lower()
    
    if provider == "duckdb":
        return DuckDB()
    elif provider == "snowflake":
        return SnowflakeDB()
    else:
        raise ValueError(f"Unknown DB_PROVIDER={provider}")
