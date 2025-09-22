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

@dataclass
class BreakdownQueryResult:
    """Information about a single task broken down from the query."""
    task_to_do: str
    reason: str

@dataclass
class AnalysisResult:
    """Result of analyzing a single task."""
    todo_intent: TodoIntent
    helper_for_action: Optional[str]  # SQL query or vector search query or None

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
