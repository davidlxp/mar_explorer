"""Dashboard component"""
import streamlit as st
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from services.visualization_logic import VolumeVisualizer
from app.components.filter_manager import FilterManager

class Dashboard:
    """Manages dashboard interface and visualization"""
    
    def __init__(self):
        self.filter_manager = FilterManager()
    
    def initialize_visualizer(self) -> None:
        """Initialize the visualizer if needed"""
        if not st.session_state.visualizer:
            st.session_state.visualizer = VolumeVisualizer()
        
        # Always get fresh filter state to ensure we have latest data
        st.session_state.filter_state = st.session_state.visualizer.get_filter_state()
    
    def render_dashboard(self) -> None:
        """Render the dashboard interface"""
        st.markdown("### 📈 Dashboard")
        
        try:
            # Initialize components
            self.initialize_visualizer()
            
            # Render filters
            self.filter_manager.render_filters(
                st.session_state.filter_state,
                st.session_state.visualizer
            )
            
            # Update filter state after all changes
            st.session_state.filter_state = st.session_state.visualizer.get_filter_state()
            
            # Get and display dashboard data
            dashboard_data = st.session_state.visualizer.get_dashboard_data()
            
            if dashboard_data and dashboard_data['figure'] is not None:
                st.plotly_chart(dashboard_data['figure'], use_container_width=True)
            else:
                st.warning("No data to display. Please ensure you have selected at least one item for Product Type, Year, and Month filters.")
                
        except Exception as e:
            st.error("Error loading dashboard")
            with st.expander("See error details"):
                st.exception(e)
