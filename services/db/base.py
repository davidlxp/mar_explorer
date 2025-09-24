# services/db/base.py
from abc import ABC, abstractmethod
from typing import Optional, Any, Union
import pandas as pd

class Database(ABC):
    """Abstract base class that defines the interface for database implementations."""
    
    @abstractmethod
    def _run_migrations(self) -> None:
        """Run all SQL migrations in order."""
        pass

    @abstractmethod
    def run_query(self, query: str, params: Optional[tuple] = None) -> Any:
        """Run a SQL query safely across DBs."""
        pass

    @abstractmethod
    def fetchall(self, query: str, params: Optional[tuple] = None) -> list:
        """Convenience method to return all rows."""
        pass
    
    @abstractmethod
    def fetchall_with_columns(self, query: str, params: Optional[tuple] = None) -> tuple:
        """Convenience method to return all rows and columns."""
        pass

    @abstractmethod
    def fetchdf(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Return query results as a pandas DataFrame."""
        pass

    @abstractmethod
    def replace_data_in_table(self, file_path: str, table_name: str) -> None:
        """Replace all data in a table with data from a parquet file.
        
        Args:
            file_path: Path to the parquet file
            table_name: Name of the table to replace data in
        """
        pass
