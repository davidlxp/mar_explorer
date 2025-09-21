import logging
from typing import Dict, List, Any
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

# In-memory storage for logs
_conversation_logs = []

def log_question(question: str, response: str, confidence: float, citations: List[Any]) -> None:
    """
    Log a question and its response details to in-memory storage.
    
    Args:
        question: The user's question
        response: The AI's response
        confidence: Confidence score of the answer
        citations: List of citations or data used to answer
    """
    try:
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "question": question,
            "response": response,
            "confidence": confidence,
            "citations": citations
        }
        _conversation_logs.append(log_entry)
        logger.info(f"Logged QA pair: {question[:50]}... -> {response[:50]}...")
    except Exception as e:
        logger.error(f"Error logging question: {str(e)}")

def get_all_logs() -> List[Dict]:
    """Get all logs, sorted by timestamp (newest first)."""
    return sorted(_conversation_logs, key=lambda x: x["timestamp"], reverse=True)

def get_latest_log() -> Dict:
    """Get the most recent log entry."""
    return _conversation_logs[-1] if _conversation_logs else None

def clear_logs() -> None:
    """Clear all logs from memory."""
    _conversation_logs.clear()
    logger.info("Cleared all conversation logs")