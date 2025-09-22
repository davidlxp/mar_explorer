import pandas as pd
from typing import List, Optional, Dict, Any
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from services.db import get_database

# SQL queries as constants for better maintainability
FILTER_OPTIONS_QUERY = """
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

class DataFetcher:
    """Handles all database interactions"""
    def __init__(self):
        self.db = get_database()

    def get_filter_options(self) -> Dict[str, List[Any]]:
        """Get all available filter options from the data"""
        result = self.db.fetchdf(FILTER_OPTIONS_QUERY)
        return {
            'asset_classes': sorted(result['ASSET_CLASS'].unique()),
            'products': sorted(result['PRODUCT'].unique()),
            'product_types': sorted(result['PRODUCT_TYPE'].unique()),
            'years': sorted(result['YEAR'].unique()),
            'months': sorted(result['MONTH'].unique())
        }

    def build_where_clause(self, filters: Dict[str, List[Any]]) -> tuple[str, list]:
        """Build WHERE clause and parameters from filters"""
        conditions = []
        params = []
        
        field_mappings = {
            'asset_classes': 'asset_class',
            'products': 'product',
            'product_types': 'product_type',
            'years': 'year',
            'months': 'month'
        }
        
        for filter_name, values in filters.items():
            if values and filter_name in field_mappings:
                field = field_mappings[filter_name]
                placeholders = ','.join(['%s'] * len(values))
                conditions.append(f"{field} IN ({placeholders})")
                params.extend(values)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return where_clause, params

    def get_dashboard_data(self, **filters) -> Dict[str, pd.DataFrame]:
        """Fetch all data needed for the dashboard"""
        where_clause, params = self.build_where_clause(filters)
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

    def get_filter_options(self) -> Dict[str, List[Any]]:
        """Get all available filter options"""
        return self.data_fetcher.get_filter_options()

    def get_dashboard_data(self,
                         asset_classes: Optional[List[str]] = None,
                         products: Optional[List[str]] = None,
                         product_types: Optional[List[str]] = None,
                         years: Optional[List[int]] = None,
                         months: Optional[List[int]] = None) -> Dict[str, Any]:
        """Get dashboard data and create visualization"""
        # Prepare filters
        filters = {
            'asset_classes': asset_classes,
            'products': products,
            'product_types': product_types,
            'years': years,
            'months': months
        }
        
        # Fetch data
        data = self.data_fetcher.get_dashboard_data(**filters)
        
        # Create visualization
        figure = self.chart_builder.create_dashboard_figure(
            data['trend_data'], 
            data['asset_data']
        )
        
        return {
            'figure': figure,
            'trend_data': data['trend_data'],
            'asset_data': data['asset_data']
        }