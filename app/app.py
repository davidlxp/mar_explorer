import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Get the project root directory
project_root = Path(__file__).parent.parent
# Load environment variables from .env file in project root
load_dotenv(project_root / '.env')

import streamlit as st
from services import task_handle_mar, task_handle_pr, nlq, logs

# Verify Snowflake credentials are loaded
required_env_vars = ['SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_WAREHOUSE']
missing_vars = [var for var in required_env_vars if var not in os.environ]

if missing_vars:
    st.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    st.info("Please check your .env file and ensure all required variables are set.")
    st.stop()

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

# Create a session state to track processing status
if 'processing' not in st.session_state:
    st.session_state.processing = False

# Single button for MAR update
if st.sidebar.button("🔄 Update Latest MAR", disabled=st.session_state.processing):
    st.session_state.processing = True
    status_container = st.sidebar.empty()
    
    try:
        status_container.info("⏳ Processing MAR update...")
        task_handle_mar.handle_mar_update(None)  # Pass None since we're not using a file
        status_container.success("✅ MAR update successful!")
    except Exception as e:
        import traceback
        status_container.error("❌ Error during MAR update")
        with st.sidebar.expander("See error details"):
            st.code(traceback.format_exc())
    finally:
        st.session_state.processing = False

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