"""
data_model.py
Data models for MAR query analysis and processing.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any, Optional

class TodoIntent(str, Enum):
    NUMERIC = "numeric"
    CONTEXT = "context"
    AGGREGATION = "aggregation"

@dataclass
class TableSchema:
    """Represents the schema of the mar_combined_m table"""
    name: str
    columns: Dict[str, str]  # column_name -> data_type
    description: str

@dataclass
class BreakdownQueryResult:
    """Information about a single task broken down from the query."""
    task_id: int
    task_to_do: str
    reason: str

@dataclass
class PlanningResult:
    """Result of analyzing a single task."""
    task_id: int
    task_to_do: str
    todo_intent: TodoIntent
    helper_for_action: Optional[str] = None  # SQL query or vector search query or None
    confidence: float
    confidence_reason: str

@dataclass
class SqlResult:
    """Result of executing a SQL query."""
    rows: List[Dict[str, Any]]
    cols: List[str]
    source: str

@dataclass
class ContextChunk:
    """Information about a context chunk from vector search."""
    id: str
    text: str
    meta: Dict[str, Any]
    score: float

@dataclass
class RetrievalResult:
    """Result of a context query against Vector Database."""
    chunks: List[ContextChunk]
    confidence: float
    strategy: str

@dataclass
class AnswerPacket:
    """Final answer to user's query."""
    text: str
    citations: List[Dict[str, Any]]
    confidence: float
