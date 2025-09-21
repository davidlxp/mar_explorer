import os
from typing import Dict, List, Tuple, Optional
import json
import logging
from pathlib import Path
import openai
from services.db import get_database
from services.vectorstores import pinecone_store
import plotly.express as px
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)

# Initialize database connection
db = get_database()

# Function definitions for OpenAI
FUNCTION_DEFINITIONS = [
    {
        "name": "query_mar_data",
        "description": "Query MAR (Monthly Activity Report) data from the database. Use this for numerical/statistical questions.",
        "parameters": {
            "type": "object",
            "properties": {
                "metrics": {
                    "type": "array",
                    "description": "List of metrics to query (volume, avg_volume)",
                    "items": {"type": "string"}
                },
                "filters": {
                    "type": "object",
                    "description": "Filter conditions (asset_class, product, product_type, year_month)",
                    "properties": {
                        "asset_class": {"type": "string"},
                        "product": {"type": "string"},
                        "product_type": {"type": "string"},
                        "year_month": {"type": "string"},
                        "year": {"type": "integer"},
                        "month": {"type": "integer"}
                    }
                },
                "group_by": {
                    "type": "array",
                    "description": "Fields to group by",
                    "items": {"type": "string"}
                }
            },
            "required": ["metrics"]
        }
    },
    {
        "name": "search_context",
        "description": "Search press releases and documentation for contextual information. Use this for non-numerical questions.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 3}
            },
            "required": ["query"]
        }
    }
]

def build_sql_query(params: Dict) -> str:
    """Build SQL query from parameters"""
    metrics = ", ".join(params["metrics"])
    base_query = f"SELECT {metrics}"
    
    if params.get("group_by"):
        group_fields = ", ".join(params["group_by"])
        base_query += f", {group_fields}"
        
    base_query += " FROM mar_combined_m"
    
    if params.get("filters"):
        conditions = []
        for field, value in params["filters"].items():
            if value is not None:
                conditions.append(f"{field} = '{value}'")
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)
            
    if params.get("group_by"):
        base_query += f" GROUP BY {group_fields}"
        
    return base_query + " ORDER BY year_month DESC"

def execute_function(func_name: str, params: Dict) -> Dict:
    """Execute the specified function with parameters"""
    try:
        if func_name == "query_mar_data":
            query = build_sql_query(params)
            logger.info(f"Executing query: {query}")
            df = db.fetchdf(query)
            return {
                "type": "data",
                "content": df.to_dict(orient="records"),
                "columns": list(df.columns)
            }
            
        elif func_name == "search_context":
            results = pinecone_store.search_content(
                query=params["query"],
                limit=params.get("limit", 3)
            )
            return {
                "type": "context",
                "content": results
            }
            
    except Exception as e:
        logger.error(f"Error executing {func_name}: {str(e)}")
        return {"type": "error", "content": str(e)}

def generate_visualization(data: List[Dict], viz_type: str = "line") -> Optional[Dict]:
    """Generate visualization from data"""
    if not data:
        return None
        
    df = pd.DataFrame(data)
    
    if viz_type == "line":
        fig = px.line(df, x="year_month", y=["volume", "avg_volume"] if "avg_volume" in df.columns else ["volume"])
    elif viz_type == "bar":
        fig = px.bar(df, x="year_month", y=["volume", "avg_volume"] if "avg_volume" in df.columns else ["volume"])
    
    return fig.to_dict()

def process_question(question: str) -> Tuple[str, Dict, float]:
    """Process a question and return answer, visualization data, and confidence"""
    try:
        # Get function calls from OpenAI
        messages = [{"role": "user", "content": question}]
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=messages,
            functions=FUNCTION_DEFINITIONS,
            function_call="auto"
        )
        
        # Execute functions and collect results
        results = []
        function_calls = response.choices[0].message.get("function_call")
        if function_calls:
            func_name = function_calls["name"]
            func_args = json.loads(function_calls["arguments"])
            result = execute_function(func_name, func_args)
            results.append(result)
            
            # Generate visualization if we have data
            viz_data = None
            if result["type"] == "data":
                viz_data = generate_visualization(result["content"])
        
        # Get final answer from OpenAI
        final_response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                *messages,
                {"role": "function", "name": "results", "content": json.dumps(results)}
            ]
        )
        
        answer = final_response.choices[0].message["content"]
        confidence = 0.95 if not any(r.get("type") == "error" for r in results) else 0.5
        
        return answer, {"results": results, "visualization": viz_data}, confidence
        
    except Exception as e:
        logger.error(f"Error processing question: {str(e)}")
        return str(e), None, 0.0