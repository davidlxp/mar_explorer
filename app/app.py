"""
MAR Explorer Application

A Streamlit application for exploring and analyzing MAR (Monthly Activity Report) data.
"""
import os
import sys
from pathlib import Path
import logging
import streamlit as st

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Now we can import our modules
from services import task_handle_mar
from app.components import Dashboard, ChatManager, LogViewer
from app.styles import STYLES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def initialize_session_state():
    """Initialize session state variables"""
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    if 'mar_status' not in st.session_state:
        st.session_state.mar_status = None
    if 'show_logs' not in st.session_state:
        st.session_state.show_logs = False
    if 'filter_state' not in st.session_state:
        st.session_state.filter_state = None
    if 'visualizer' not in st.session_state:
        st.session_state.visualizer = None

def render_sidebar():
    """Render sidebar content"""
    with st.sidebar:
        st.markdown("### 📈 MAR Data")
        status_placeholder = st.empty()
        
        if st.button("🔄 Update Latest MAR"):
            with status_placeholder.container():
                st.info("⏳ Processing MAR update...")
                try:
                    task_handle_mar.update_mar_with_latest_file()
                    # Reset visualizer to force reinitialization
                    if 'visualizer' in st.session_state:
                        st.session_state.visualizer.reinitialize()
                    st.success("✅ MAR update completed!")
                except Exception as e:
                    st.error("❌ Error during update")
                    with st.expander("See error details"):
                        st.exception(e)
        
        st.markdown("### 🔍 Tools")
        
        # Render logs in sidebar
        LogViewer.render_logs()

def main():
    """Main application entry point"""
    # Page configuration
    st.set_page_config(layout="wide", page_title="Tradeweb MAR Explorer")
    st.markdown(f"<style>{STYLES}</style>", unsafe_allow_html=True)
    
    # Initialize session state
    initialize_session_state()
    
    # Render sidebar
    render_sidebar()
    
    # Main content area
    st.title("📊 Tradeweb MAR Explorer")
    
    # Dashboard section
    with st.container():
        dashboard = Dashboard()
        dashboard.render_dashboard()
        st.markdown("---")
    
    # Chat interface
    ChatManager.render_chat_interface()

if __name__ == "__main__":
    main()