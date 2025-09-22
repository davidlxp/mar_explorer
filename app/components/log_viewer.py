"""Log viewer component"""
from datetime import datetime
import streamlit as st
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from services import logs

class LogViewer:
    """Manages log viewing interface"""
    
    @staticmethod
    def render_log_entry(log: dict) -> None:
        """Render a single log entry"""
        st.markdown(f"""
        <div class="log-entry">
            <div class="log-timestamp">
                {datetime.fromisoformat(log['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}
            </div>
            <div class="log-question">Q: {log['question']}</div>
            <div class="log-response">A: {log['response']}</div>
            <div class="confidence-{
                'high' if log['confidence'] > 0.8
                else 'medium' if log['confidence'] > 0.5
                else 'low'
            }">
                Confidence: {log['confidence']:.2f}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        with st.expander("View Citations"):
            st.json(log['citations'])

    @staticmethod
    def render_logs() -> None:
        """Render the logs interface"""
        with st.expander("📋 Interaction History", expanded=True):
            col1, col2 = st.columns([10, 1])
            with col2:
                if st.button("✕", key="close_logs"):
                    st.session_state.show_logs = False
                    st.rerun()
            
            all_logs = logs.get_all_logs()
            if not all_logs:
                st.info("No interaction history yet.")
            else:
                for log in all_logs:
                    with st.container():
                        LogViewer.render_log_entry(log)
