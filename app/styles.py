"""CSS styles for the application"""

STYLES = """
    /* General button styling */
    .stButton > button {
        background-color: #4CAF50;
        color: white;
        padding: 8px 16px;
        border-radius: 4px;
        border: none;
    }
    .stButton > button:hover {
        background-color: #45a049;
    }
    
    /* Log entry styling */
    .log-entry {
        background-color: #f8f9fa;
        border-left: 4px solid #4CAF50;
        padding: 15px;
        margin-bottom: 15px;
        border-radius: 4px;
    }
    
    .log-timestamp {
        color: #666;
        font-size: 0.9em;
    }
    
    .log-question {
        font-weight: bold;
        margin: 10px 0;
    }
    
    .log-response {
        margin: 10px 0;
        white-space: pre-wrap;
    }
    
    .confidence-high {
        color: #4CAF50;
    }
    
    .confidence-medium {
        color: #FFA726;
    }
    
    .confidence-low {
        color: #EF5350;
    }
    
    .citations-section {
        margin-top: 10px;
        font-size: 0.9em;
        color: #666;
        border-top: 1px solid #eee;
        padding-top: 10px;
    }

    /* Hide Streamlit elements when modal is open */
    [data-modal-is-open="true"] .main {
        filter: blur(4px);
    }
"""
