import os
import sys
from pathlib import Path
import logging

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
st.title("📊 Tradeweb MAR Explorer")

# :::::: MAR Update Section :::::: #
st.sidebar.markdown("### 📈 MAR Data")

# Custom CSS for the button
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
    </style>
""", unsafe_allow_html=True)

# Initialize session state
if 'mar_status' not in st.session_state:
    st.session_state.mar_status = None

# Create placeholder for status
status_placeholder = st.sidebar.empty()

def update_mar():
    """Function to handle MAR update"""
    try:
        logger.info("Starting MAR update...")
        task_handle_mar.update_mar_with_latest_file()
        logger.info("MAR update completed")
        return True
    except Exception as e:
        logger.error(f"Error in MAR update: {str(e)}")
        return False

# MAR Update button
if st.sidebar.button("🔄 Update Latest MAR"):
    with status_placeholder.container():
        st.info("⏳ Processing MAR update...")
        try:
            success = update_mar()
            if success:
                st.success("✅ MAR update completed!")
            else:
                st.error("❌ MAR update failed!")
        except Exception as e:
            st.error("❌ Error during update")
            st.exception(e)

# :::::: Chat box :::::: #
st.markdown("---")
st.subheader("💬 Ask MAR")

question = st.text_input("Type your question:")
if question:
    answer, citations, confidence = nlq.handle_question(question)
    if answer:
        st.write(answer)
        st.caption(f"Citations: {citations}")
    else:
        st.warning("I couldn't find this in the MAR or Press Release.")
    logs.log_question(question, confidence, citations)