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
    </style>
""", unsafe_allow_html=True)

# Initialize session states
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

# Dashboard/visualization section
dashboard_container = st.container()
with dashboard_container:
    st.markdown("### 📈 Dashboard")
    
    # Import visualization components
    from services.visualization_logic import VolumeVisualizer
    import plotly.io as pio
    
    # Initialize visualizer if not already initialized
    if not st.session_state.visualizer:
        st.session_state.visualizer = VolumeVisualizer()
    
    try:
        # Get current filter state
        if not st.session_state.filter_state:
            st.session_state.filter_state = st.session_state.visualizer.get_filter_state()
        
        filter_state = st.session_state.filter_state
        
        # Create filter columns
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            # Asset Class filter
            new_asset_classes = st.multiselect(
                "Asset Class",
                options=list(filter_state['available']['asset_classes']),
                default=list(filter_state['selected']['asset_classes'])
            )
            
            # Handle asset class changes
            if not new_asset_classes:  # All items removed
                st.session_state.visualizer.deselect_filter('asset_class')
            else:
                added_asset_classes = set(new_asset_classes) - filter_state['selected']['asset_classes']
                removed_asset_classes = filter_state['selected']['asset_classes'] - set(new_asset_classes)
                
                for asset_class in added_asset_classes:
                    st.session_state.visualizer.select_filter('asset_class', asset_class)
                for asset_class in removed_asset_classes:
                    st.session_state.visualizer.deselect_filter('asset_class', asset_class)
            
            # Year filter (independent)
            new_years = st.multiselect(
                "Year",
                options=sorted(list(filter_state['selected']['years'])),
                default=sorted(list(filter_state['selected']['years']))
            )
            # Update selected years
            st.session_state.visualizer.update_time_filters(years=set(new_years))
        
        with filter_col2:
            # Product filter
            new_products = st.multiselect(
                "Product",
                options=list(filter_state['available']['products']),
                default=list(filter_state['selected']['products'])
            )
            
            # Handle product changes
            if not new_products:  # All items removed
                st.session_state.visualizer.deselect_filter('product')
            else:
                added_products = set(new_products) - filter_state['selected']['products']
                removed_products = filter_state['selected']['products'] - set(new_products)
                
                for product in added_products:
                    st.session_state.visualizer.select_filter('product', product)
                for product in removed_products:
                    st.session_state.visualizer.deselect_filter('product', product)
            
            # Month filter (independent)
            new_months = st.multiselect(
                "Month",
                options=sorted(list(filter_state['selected']['months'])),
                default=sorted(list(filter_state['selected']['months']))
            )
            # Update selected months
            st.session_state.visualizer.update_time_filters(months=set(new_months))
        
        with filter_col3:
            # Product Type filter
            new_product_types = st.multiselect(
                "Product Type",
                options=list(filter_state['available']['product_types']),
                default=list(filter_state['selected']['product_types'])
            )
            
            # Handle product type changes
            added_product_types = set(new_product_types) - filter_state['selected']['product_types']
            removed_product_types = filter_state['selected']['product_types'] - set(new_product_types)
            
            for product_type in added_product_types:
                st.session_state.visualizer.select_filter('product_type', product_type)
            for product_type in removed_product_types:
                st.session_state.visualizer.deselect_filter('product_type', product_type)
        
        # Update filter state after all changes
        st.session_state.filter_state = st.session_state.visualizer.get_filter_state()
        
        # Get dashboard data
        dashboard_data = st.session_state.visualizer.get_dashboard_data()
        
        # Display the dashboard
        if dashboard_data:
            st.plotly_chart(dashboard_data['figure'], use_container_width=True)
            
            # Optional: Display raw data
            if st.checkbox("Show raw data"):
                tab1, tab2 = st.tabs(["Monthly Trend", "Asset Class Breakdown"])
                with tab1:
                    st.dataframe(dashboard_data['trend_data'])
                with tab2:
                    st.dataframe(dashboard_data['asset_data'])
        else:
            st.warning("No data available for the selected filters")
            
    except Exception as e:
        st.error("Error loading dashboard")
        with st.expander("See error details"):
            st.exception(e)
    
    st.markdown("---")

# Logs Dialog using Streamlit's native components
if st.session_state.show_logs:
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
    try:
        # Add user message to chat and display immediately
        user_msg = {"role": "user", "content": prompt}
        st.session_state.messages.append(user_msg)
        
        # Display user message
        with st.chat_message("user"):
            st.write(prompt)
        
        # Get AI response
        with st.spinner("Thinking..."):
            answer, data, confidence = nlq.process_question(prompt)
            
            # Create assistant response
            response_msg = {
                "role": "assistant",
                "content": answer,
            }
            
            # Add visualization if available
            if data and data.get("visualization"):
                response_msg["visualization"] = data["visualization"]
            
            # Add to session state
            st.session_state.messages.append(response_msg)
            
            # Display assistant response immediately
            with st.chat_message("assistant"):
                st.write(answer)
                if data and data.get("visualization"):
                    fig = pio.from_json(json.dumps(data["visualization"]))
                    st.plotly_chart(fig, use_container_width=True)
            
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

# Clear chat button
if st.session_state.messages and st.button("Clear Chat"):
    st.session_state.messages = []
    st.rerun()