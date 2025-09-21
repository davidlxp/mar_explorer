import os
import sys
from pathlib import Path
import logging
import json
import plotly.io as pio
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
from services import task_handle_mar, task_handle_pr, nlq, logs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(layout="wide", page_title="Tradeweb MAR Explorer")

# Custom CSS
st.markdown("""
    <style>
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
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        display: flex;
        flex-direction: column;
    }
    .user-message {
        background-color: #e3f2fd;
        align-self: flex-end;
    }
    .assistant-message {
        background-color: #f5f5f5;
        align-self: flex-start;
    }
    .log-modal {
        max-height: 80vh;
        overflow-y: auto;
    }
    .log-entry {
        border: 1px solid #ddd;
        padding: 10px;
        margin: 10px 0;
        border-radius: 5px;
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
    </style>
""", unsafe_allow_html=True)

# Initialize session states
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'mar_status' not in st.session_state:
    st.session_state.mar_status = None
if 'show_logs' not in st.session_state:
    st.session_state.show_logs = False

# Sidebar for MAR updates and tools
with st.sidebar:
    st.markdown("### 📈 MAR Data")
    status_placeholder = st.empty()
    
    if st.button("🔄 Update Latest MAR"):
        with status_placeholder.container():
            st.info("⏳ Processing MAR update...")
            try:
                task_handle_mar.update_mar_with_latest_file()
                st.success("✅ MAR update completed!")
            except Exception as e:
                st.error("❌ Error during update")
                with st.expander("See error details"):
                    st.exception(e)
    
    # Logs viewer button
    st.markdown("### 🔍 Tools")
    if st.button("📋 View Interaction Logs"):
        st.session_state.show_logs = True

# Main content area
st.title("📊 Tradeweb MAR Explorer")

# Reserved space for dashboard/visualization
dashboard_container = st.container()
with dashboard_container:
    st.markdown("### 📈 Dashboard")
    st.info("Dashboard space reserved for future visualizations")
    st.markdown("---")

# Chat interface
st.markdown("### 💬 Ask MAR")

# Display chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])
        if msg.get("visualization"):
            try:
                fig = pio.from_json(json.dumps(msg["visualization"]))
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                logger.error(f"Error displaying visualization: {str(e)}")

# Chat input
if prompt := st.chat_input("Ask about MAR data or press releases..."):
    # Add user message to chat
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Get AI response
    with st.spinner("Thinking..."):
        try:
            answer, data, confidence = nlq.process_question(prompt)
            
            # Add assistant response to chat
            response_msg = {
                "role": "assistant",
                "content": answer,
            }
            
            # Add visualization if available
            if data and data.get("visualization"):
                response_msg["visualization"] = data["visualization"]
            
            st.session_state.messages.append(response_msg)
            
            # Log the interaction
            logs.log_question(
                question=prompt,
                response=answer,
                confidence=confidence,
                citations=data.get("results", []) if data else []
            )
            
        except Exception as e:
            st.error("❌ Error processing your question")
            logger.error(f"Error in chat: {str(e)}")
            with st.expander("See error details"):
                st.exception(e)

# Logs modal
if st.session_state.show_logs:
    with st.expander("Interaction Logs", expanded=True):
        st.markdown("### 📋 Interaction History")
        
        # Add close button
        if st.button("Close Logs"):
            st.session_state.show_logs = False
            st.rerun()
        
        # Display logs in scrollable container
        log_container = st.container()
        with log_container:
            all_logs = logs.get_all_logs()
            for log in all_logs:
                with st.container():
                    st.markdown(f"""
                    <div class="log-entry">
                        <p><strong>Time:</strong> {datetime.fromisoformat(log['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}</p>
                        <p><strong>Question:</strong> {log['question']}</p>
                        <p><strong>Response:</strong> {log['response']}</p>
                        <p><strong>Confidence:</strong> <span class="confidence-{'high' if log['confidence'] > 0.8 else 'medium' if log['confidence'] > 0.5 else 'low'}">{log['confidence']:.2f}</span></p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    with st.expander("View Citations"):
                        st.json(log['citations'])

# Clear chat button (at the bottom)
if st.session_state.messages and st.button("Clear Chat"):
    st.session_state.messages = []
    st.rerun()