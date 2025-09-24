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
class ReceptionResult:
    """Result of receiving a query."""
    next_step: str
    next_step_content: str

@dataclass
class BreakdownQueryResult:
    """Information about a single task broken down from the query."""
    task_to_do: str
    reason: str

@dataclass
class PlanningResult:
    """Result of analyzing a single task."""
    todo_intent: TodoIntent
    helper_for_action: Optional[str] = None  # SQL query or vector search query or None

@dataclass
class CompletedTask:
    """Information about a completed task."""
    task_to_do: str
    todo_intent: TodoIntent
    helper_for_action: Optional[str] = None  # SQL query or vector search query or None

@dataclass
class CompletedTaskResult:
    """Result of a completed task."""
    result: Any

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
class CalculatorResult:
    """Result of the calculating agent."""
    result: float

@dataclass
class ValidatorOpinion:
    """Opinion of the validator."""
    confidence_of_result: float
    confidence_reason: str

@dataclass
class ValidatorResult:
    """Result of the validator."""
    confidence: float
    confidence_reason: str

@dataclass
class AnswerPacket:
    """Final answer to user's query."""
    text: str
    citations: List[Dict[str, Any]]
    confidence: float