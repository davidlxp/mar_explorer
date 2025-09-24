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
    CALCULATION = "calculation"
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
    report_type: str
    report_name: str
    text: str
    url: str
    relevance_score: float

@dataclass
class RetrievalResult:
    """Result of a context query against Vector Database."""
    chunks: List[ContextChunk]

@dataclass
class ExecutionOutput:
    """Unified wrapper for all task results."""
    intent: str                 # "numeric" | "context" | "aggregation"
    content: Any                # actual result (SQL table, RetrievalResult, dict, etc.)
    raw_query: Optional[str]    # SQL string, search query, or None
    reference: Optional[str]
    error: Optional[str] = None

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
class InputForValidator:
    """Input for the validator."""
    org_query: str
    task_done: str
    task_reason: str
    task_intent: TodoIntent
    task_approach: str
    task_result: str

@dataclass
class ValidatorResult:
    """Result of the validator."""
    confidence: float
    confidence_reason: str

@dataclass
class CompletedTask:
    """Information about a completed task."""
    task_to_do: str
    todo_intent: TodoIntent
    task_reason: str
    helper_for_action: Optional[str] = None  # SQL query or vector search query or None

@dataclass
class CompletedTaskResult:
    """Result of a completed task."""
    result: Any
    reference: str
    validator_confidence: float

@dataclass
class AnswerPacket:
    """Final answer to user's query."""
    text: str
    citations: List[Dict[str, Any]]
    confidence: float
    confidence_reason: str