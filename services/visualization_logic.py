import pandas as pd
from services.db import get_database
from typing import List, Optional, Dict, Any
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class VolumeVisualizer:
    def __init__(self):
        self.db = get_database()

    def get_filter_options(self) -> Dict[str, List[Any]]:
        """Get all available filter options from the data"""
        query = """
        SELECT DISTINCT
            asset_class,
            product,
            product_type,
            year,
            month
        FROM mar_combined_m
        ORDER BY asset_class, product, product_type, year, month;
        """
        result = self.db.fetchdf(query)
        
        return {
            'asset_classes': sorted(result['ASSET_CLASS'].unique()),
            'products': sorted(result['PRODUCT'].unique()),
            'product_types': sorted(result['PRODUCT_TYPE'].unique()),
            'years': sorted(result['YEAR'].unique()),
            'months': sorted(result['MONTH'].unique())
        }

    def get_dashboard_data(self,
                         asset_classes: Optional[List[str]] = None,
                         products: Optional[List[str]] = None,
                         product_types: Optional[List[str]] = None,
                         years: Optional[List[int]] = None,
                         months: Optional[List[int]] = None) -> Dict[str, Any]:
        """Get all data needed for the dashboard"""
        # Build conditions and parameters
        conditions = []
        params = []
        
        if asset_classes:
            conditions.append("asset_class IN ({})".format(','.join(["%s" for _ in asset_classes])))
            params.extend(asset_classes)
        if products:
            conditions.append("product IN ({})".format(','.join(["%s" for _ in products])))
            params.extend(products)
        if product_types:
            conditions.append("product_type IN ({})".format(','.join(["%s" for _ in product_types])))
            params.extend(product_types)
        if years:
            conditions.append("year IN ({})".format(','.join(["%s" for _ in years])))
            params.extend(years)
        if months:
            conditions.append("month IN ({})".format(','.join(["%s" for _ in months])))
            params.extend(months)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Monthly trend data
        trend_query = f"""
        SELECT 
            year_month,
            SUM(volume) as total_volume,
            SUM(avg_volume) as total_avg_volume
        FROM mar_combined_m
        WHERE {where_clause}
        GROUP BY year_month
        ORDER BY year_month;
        """
        trend_data = self.db.fetchdf(trend_query, tuple(params) if params else None)
        
        # Asset class breakdown
        asset_query = f"""
        SELECT 
            asset_class,
            SUM(volume) as total_volume
        FROM mar_combined_m
        WHERE {where_clause}
        GROUP BY asset_class
        ORDER BY total_volume DESC;
        """
        asset_data = self.db.fetchdf(asset_query, tuple(params) if params else None)
        
        # Create figures
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
        
        return {
            'figure': fig,
            'trend_data': trend_data,
            'asset_data': asset_data
        }