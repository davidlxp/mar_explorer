import pandas as pd
from typing import List, Optional, Dict, Any, Set
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from services.db import get_database
from dataclasses import dataclass, field

# SQL queries as constants
HIERARCHY_QUERY = """
SELECT DISTINCT
    asset_class,
    product,
    product_type,
    year,
    month
FROM mar_combined_m
ORDER BY asset_class, product, product_type, year, month;
"""

TREND_QUERY = """
SELECT 
    year_month,
    SUM(volume) as total_volume,
    SUM(avg_volume) as total_avg_volume
FROM mar_combined_m
WHERE {where_clause}
GROUP BY year_month
ORDER BY year_month;
"""

ASSET_BREAKDOWN_QUERY = """
SELECT 
    asset_class,
    SUM(volume) as total_volume
FROM mar_combined_m
WHERE {where_clause}
GROUP BY asset_class
ORDER BY total_volume DESC;
"""

@dataclass
class FilterState:
    """Class to maintain filter state"""
    # Reference mappings (immutable after initialization)
    asset_class_to_products: Dict[str, Set[str]] = field(default_factory=dict)
    product_to_types: Dict[str, Set[str]] = field(default_factory=dict)
    
    # Available items for selection
    available_asset_classes: Set[str] = field(default_factory=set)
    available_products: Set[str] = field(default_factory=set)
    available_product_types: Set[str] = field(default_factory=set)
    
    # Currently selected items
    selected_asset_classes: Set[str] = field(default_factory=set)
    selected_products: Set[str] = field(default_factory=set)
    selected_product_types: Set[str] = field(default_factory=set)
    
    # Time filters reference sets (never change after initialization)
    available_years: Set[int] = field(default_factory=set)
    available_months: Set[int] = field(default_factory=set)
    
    # Time filters selected sets
    selected_years: Set[int] = field(default_factory=set)
    selected_months: Set[int] = field(default_factory=set)

class FilterStateManager:
    """Manages the hierarchical filter state"""
    def __init__(self, db):
        self.db = db
        self.state = FilterState()
        self._initialize_state()
    
    def _initialize_state(self):
        """Initialize filter state with all data"""
        # Fetch hierarchy data
        hierarchy_data = self.db.fetchdf(HIERARCHY_QUERY)
        
        # Build immutable hierarchy mappings
        for _, row in hierarchy_data.iterrows():
            asset_class = row['ASSET_CLASS']
            product = row['PRODUCT']
            product_type = row['PRODUCT_TYPE']
            
            # Build asset_class to products mapping
            if asset_class not in self.state.asset_class_to_products:
                self.state.asset_class_to_products[asset_class] = set()
            self.state.asset_class_to_products[asset_class].add(product)
            
            # Build product to product_types mapping
            if product not in self.state.product_to_types:
                self.state.product_to_types[product] = set()
            self.state.product_to_types[product].add(product_type)
        
        # Initialize time filters with reference sets that never change
        self.state.available_years = set(hierarchy_data['YEAR'])
        self.state.available_months = set(hierarchy_data['MONTH'])
        
        # Initialize selected sets with all items
        self.state.selected_years = self.state.available_years.copy()
        self.state.selected_months = self.state.available_months.copy()
        
        # Initialize with all items selected
        self.select_all()
    
    def select_all(self):
        """Select all items in the hierarchy"""
        # Set all asset classes as available and selected
        self.state.available_asset_classes = set(self.state.asset_class_to_products.keys())
        self.state.selected_asset_classes = self.state.available_asset_classes.copy()
        
        # Set all products as available and selected
        self.state.available_products = set()
        for products in self.state.asset_class_to_products.values():
            self.state.available_products.update(products)
        self.state.selected_products = self.state.available_products.copy()
        
        # Set all product types as available and selected
        self.state.available_product_types = set()
        for types in self.state.product_to_types.values():
            self.state.available_product_types.update(types)
        self.state.selected_product_types = self.state.available_product_types.copy()
    
    def _get_available_products_from_asset_classes(self, asset_classes: Set[str]) -> Set[str]:
        """Get all available products from a set of asset classes"""
        available_products = set()
        for ac in asset_classes:
            available_products.update(self.state.asset_class_to_products[ac])
        return available_products
    
    def _get_available_product_types_from_products(self, products: Set[str]) -> Set[str]:
        """Get all available product types from a set of products"""
        available_types = set()
        for prod in products:
            available_types.update(self.state.product_to_types[prod])
        return available_types
    
    def deselect_all_asset_classes(self):
        """Remove all asset classes and clear child filters"""
        # Clear asset class selected list (but keep available list unchanged)
        self.state.selected_asset_classes.clear()
        
        # Clear both selected and available lists for children
        self.state.selected_products.clear()
        self.state.available_products.clear()
        
        self.state.selected_product_types.clear()
        self.state.available_product_types.clear()

    def deselect_all_products(self):
        """Remove all products and clear child filters"""
        # Clear product selected list (but keep available list unchanged)
        self.state.selected_products.clear()
        
        # Clear both selected and available lists for product types
        self.state.selected_product_types.clear()
        self.state.available_product_types.clear()

    def deselect_product_type(self, product_type: str):
        """Simply remove a product type from selected list"""
        if product_type in self.state.selected_product_types:
            self.state.selected_product_types.remove(product_type)
    
    def deselect_product(self, product: str):
        """Remove a product and update product type lists"""
        if product in self.state.selected_products:
            # Remove the product
            self.state.selected_products.remove(product)
            
            # Get available product types from remaining selected products
            available_types = self._get_available_product_types_from_products(
                self.state.selected_products
            )
            
            # Update available and selected product types
            self.state.available_product_types = available_types
            self.state.selected_product_types.intersection_update(available_types)
    
    def deselect_asset_class(self, asset_class: str):
        """Remove an asset class and update both product and product type lists"""
        if asset_class in self.state.selected_asset_classes:
            # Remove the asset class
            self.state.selected_asset_classes.remove(asset_class)
            
            # Get available products from remaining asset classes
            available_products = self._get_available_products_from_asset_classes(
                self.state.selected_asset_classes
            )
            
            # Get available product types from these products
            available_types = self._get_available_product_types_from_products(
                available_products
            )
            
            # Update available and selected products
            self.state.available_products = available_products
            self.state.selected_products.intersection_update(available_products)
            
            # Update available and selected product types
            self.state.available_product_types = available_types
            self.state.selected_product_types.intersection_update(available_types)
    
    def select_product_type(self, product_type: str):
        """Add a product type to selected list if it's available"""
        if product_type in self.state.available_product_types:
            self.state.selected_product_types.add(product_type)
    
    def select_product(self, product: str):
        """Add a product and its types if it's available"""
        if product in self.state.available_products:
            self.state.selected_products.add(product)
            # Make its types available and selected
            types = self.state.product_to_types[product]
            self.state.available_product_types.update(types)
            self.state.selected_product_types.update(types)
    
    def select_asset_class(self, asset_class: str):
        """Add an asset class and its children if it's available"""
        if asset_class in self.state.available_asset_classes:
            self.state.selected_asset_classes.add(asset_class)
            # Get and select all child products
            products = self.state.asset_class_to_products[asset_class]
            self.state.available_products.update(products)
            self.state.selected_products.update(products)
            # Get and select all grandchild product types
            for product in products:
                types = self.state.product_to_types[product]
                self.state.available_product_types.update(types)
                self.state.selected_product_types.update(types)

class DataFetcher:
    """Handles all database interactions"""
    def __init__(self):
        self.db = get_database()
        self.filter_manager = FilterStateManager(self.db)

    def get_filter_state(self) -> Dict[str, Dict[str, Set[Any]]]:
        """Get current filter state"""
        state = self.filter_manager.state
        return {
            'available': {
                'asset_classes': state.available_asset_classes,
                'products': state.available_products,
                'product_types': state.available_product_types,
                'years': state.available_years,  # Include available years
                'months': state.available_months,  # Include available months
            },
            'selected': {
                'asset_classes': state.selected_asset_classes,
                'products': state.selected_products,
                'product_types': state.selected_product_types,
                'years': state.selected_years,
                'months': state.selected_months,
            }
        }

    def build_where_clause(self) -> tuple[str, list]:
        """Build WHERE clause from current filter state"""
        conditions = []
        params = []
        state = self.filter_manager.state
        
        if state.selected_asset_classes:
            conditions.append(f"asset_class IN ({','.join(['%s'] * len(state.selected_asset_classes))})")
            params.extend(state.selected_asset_classes)
        
        if state.selected_products:
            conditions.append(f"product IN ({','.join(['%s'] * len(state.selected_products))})")
            params.extend(state.selected_products)
        
        if state.selected_product_types:
            conditions.append(f"product_type IN ({','.join(['%s'] * len(state.selected_product_types))})")
            params.extend(state.selected_product_types)
        
        if state.selected_years:
            conditions.append(f"year IN ({','.join(['%s'] * len(state.selected_years))})")
            params.extend(state.selected_years)
        
        if state.selected_months:
            conditions.append(f"month IN ({','.join(['%s'] * len(state.selected_months))})")
            params.extend(state.selected_months)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return where_clause, params

    def get_dashboard_data(self) -> Optional[Dict[str, pd.DataFrame]]:
        """Fetch dashboard data based on current filter state"""
        # Check if any required filter is empty
        state = self.filter_manager.state
        if (not state.selected_product_types or 
            not state.selected_years or 
            not state.selected_months):
            return None  # Return None to indicate no data should be shown
            
        where_clause, params = self.build_where_clause()
        params_tuple = tuple(params) if params else None

        trend_data = self.db.fetchdf(
            TREND_QUERY.format(where_clause=where_clause), 
            params_tuple
        )
        
        asset_data = self.db.fetchdf(
            ASSET_BREAKDOWN_QUERY.format(where_clause=where_clause), 
            params_tuple
        )

        return {
            'trend_data': trend_data,
            'asset_data': asset_data
        }

class ChartBuilder:
    """Handles all visualization logic"""
    @staticmethod
    def create_dashboard_figure(trend_data: pd.DataFrame, asset_data: pd.DataFrame) -> go.Figure:
        """Create the dashboard figure with all charts"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Monthly Trading Volume', 'Volume by Asset Class'),
            specs=[[{"colspan": 2}, None],
                  [{"type": "pie"}, {"type": "bar"}]],
            vertical_spacing=0.12,
            horizontal_spacing=0.1
        )
        
        # Add monthly trend
        fig.add_trace(
            go.Bar(
                x=trend_data['YEAR_MONTH'],
                y=trend_data['TOTAL_VOLUME'],
                name='Volume',
                hovertemplate='Month: %{x}<br>Volume: %{y:,.0f}<extra></extra>'
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=trend_data['YEAR_MONTH'],
                y=trend_data['TOTAL_AVG_VOLUME'],
                name='Average Volume',
                line=dict(color='red'),
                hovertemplate='Month: %{x}<br>Avg Volume: %{y:,.0f}<extra></extra>'
            ),
            row=1, col=1
        )
        
        # Add asset class breakdown
        fig.add_trace(
            go.Pie(
                labels=asset_data['ASSET_CLASS'],
                values=asset_data['TOTAL_VOLUME'],
                name='Asset Classes',
                hovertemplate='Asset Class: %{label}<br>Volume: %{value:,.0f}<extra></extra>'
            ),
            row=2, col=1
        )
        
        # Update layout
        fig.update_layout(
            height=800,
            showlegend=True,
            title_text="Trading Volume Dashboard",
            hovermode='x unified'
        )
        
        # Update axes
        fig.update_xaxes(title_text="Month", row=1, col=1, tickangle=-45)
        fig.update_yaxes(title_text="Volume", row=1, col=1)
        
        return fig

class VolumeVisualizer:
    """Main interface for volume visualization"""
    def __init__(self):
        self.data_fetcher = DataFetcher()
        self.chart_builder = ChartBuilder()

    def get_filter_state(self) -> Dict[str, Dict[str, Set[Any]]]:
        """Get current filter state"""
        return self.data_fetcher.get_filter_state()

    def select_filter(self, filter_type: str, value: str):
        """Select a filter value"""
        manager = self.data_fetcher.filter_manager
        if filter_type == 'asset_class':
            manager.select_asset_class(value)
        elif filter_type == 'product':
            manager.select_product(value)
        elif filter_type == 'product_type':
            manager.select_product_type(value)

    def deselect_filter(self, filter_type: str, value: str = None):
        """Deselect a filter value or all values of a type"""
        manager = self.data_fetcher.filter_manager
        
        # Handle remove-all cases
        if value is None:
            if filter_type == 'asset_class':
                manager.deselect_all_asset_classes()
            elif filter_type == 'product':
                manager.deselect_all_products()
            return
        
        # Handle single item removal
        if filter_type == 'asset_class':
            manager.deselect_asset_class(value)
        elif filter_type == 'product':
            manager.deselect_product(value)
        elif filter_type == 'product_type':
            manager.deselect_product_type(value)

    def update_time_filters(self, years: Optional[Set[int]] = None, months: Optional[Set[int]] = None):
        """Update year and month filters"""
        if years is not None:
            self.data_fetcher.filter_manager.state.selected_years = years
        if months is not None:
            self.data_fetcher.filter_manager.state.selected_months = months

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get dashboard data and create visualization"""
        data = self.data_fetcher.get_dashboard_data()
        
        # If any required filter is empty, return only filter state
        if data is None:
            return {
                'figure': None,
                'trend_data': None,
                'asset_data': None,
                'filter_state': self.get_filter_state()
            }
            
        # Create visualization if we have data
        figure = self.chart_builder.create_dashboard_figure(
            data['trend_data'], 
            data['asset_data']
        )
        
        return {
            'figure': figure,
            'trend_data': data['trend_data'],
            'asset_data': data['asset_data'],
            'filter_state': self.get_filter_state()
        }