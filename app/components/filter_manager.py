"""Filter management component for the dashboard"""
from typing import Dict, Set, Any, List
import streamlit as st

class FilterManager:
    """Manages filter UI and state"""
    
    @staticmethod
    def handle_hierarchical_filter_change(
        new_values: List[str],
        current_state: Dict[str, Set[str]],
        filter_type: str,
        visualizer
    ) -> None:
        """Handle changes in hierarchical filters (asset class, product, product type)"""
        # Map filter types to their plural forms in the state
        filter_type_map = {
            'asset_class': 'asset_classes',
            'product': 'products',
            'product_type': 'product_types'
        }
        
        if not new_values:  # All items removed
            visualizer.deselect_filter(filter_type)
        else:
            # Calculate added and removed items
            new_set = set(new_values)
            current_set = current_state['selected'][filter_type_map[filter_type]]
            
            # Handle additions
            for item in new_set - current_set:
                visualizer.select_filter(filter_type, item)
            # Handle removals
            for item in current_set - new_set:
                visualizer.deselect_filter(filter_type, item)

    def render_filters(self, filter_state: Dict[str, Dict[str, Set[Any]]], visualizer) -> None:
        """Render all filter UI components"""
        # Create filter columns
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            # Asset Class filter
            new_asset_classes = st.multiselect(
                "Asset Class",
                options=list(filter_state['available']['asset_classes']),
                default=list(filter_state['selected']['asset_classes'])
            )
            self.handle_hierarchical_filter_change(
                new_asset_classes, filter_state, 'asset_class', visualizer
            )
            
            # Year filter (independent)
            new_years = st.multiselect(
                "Year",
                options=sorted(list(filter_state['available']['years'])),
                default=sorted(list(filter_state['selected']['years']))
            )
            visualizer.update_time_filters(years=set(new_years))
        
        with filter_col2:
            # Product Type filter
            new_product_types = st.multiselect(
                "Product Type",
                options=list(filter_state['available']['product_types']),
                default=list(filter_state['selected']['product_types'])
            )
            self.handle_hierarchical_filter_change(
                new_product_types, filter_state, 'product_type', visualizer
            )
            
            # Month filter (independent)
            new_months = st.multiselect(
                "Month",
                options=sorted(list(filter_state['available']['months'])),
                default=sorted(list(filter_state['selected']['months']))
            )
            visualizer.update_time_filters(months=set(new_months))
        
        with filter_col3:
            # Product filter
            new_products = st.multiselect(
                "Product",
                options=list(filter_state['available']['products']),
                default=list(filter_state['selected']['products'])
            )
            self.handle_hierarchical_filter_change(
                new_products, filter_state, 'product', visualizer
            )
