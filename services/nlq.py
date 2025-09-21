import os
from typing import Dict, List, Tuple, Optional
import json
import logging
from pathlib import Path
from openai import OpenAI
from services.db import get_database
from services.vectorstores import pinecone_store
import plotly.express as px
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)

# Initialize OpenAI client
client = OpenAI()

# Initialize database connection
db = get_database()

# Load available products configuration
with open(Path(__file__).parent.parent / 'storage/nlq_context/tradeweb_available_products.json', 'r') as f:
    AVAILABLE_PRODUCTS = json.load(f)

# Create lookup sets for validation
VALID_ASSET_CLASSES = {p["ASSET_CLASS"].lower() for p in AVAILABLE_PRODUCTS}
VALID_PRODUCTS = {p["PRODUCT"].lower() for p in AVAILABLE_PRODUCTS}
VALID_PRODUCT_TYPES = {p["PRODUCT_TYPE"].lower() for p in AVAILABLE_PRODUCTS}

# Create full product combinations for validation
VALID_COMBINATIONS = {
    (p["ASSET_CLASS"].lower(), p["PRODUCT"].lower(), p["PRODUCT_TYPE"].lower())
    for p in AVAILABLE_PRODUCTS
}

def validate_product_query(asset_class: str = None, product: str = None, product_type: str = None) -> Tuple[bool, str]:
    """
    Validate if the product combination exists in our database.
    Returns: (is_valid, error_message)
    """
    if asset_class and asset_class.lower() not in VALID_ASSET_CLASSES:
        return False, f"Invalid asset class. Available options are: {', '.join(sorted(VALID_ASSET_CLASSES))}"
        
    if product and product.lower() not in VALID_PRODUCTS:
        return False, f"Invalid product. Available options are: {', '.join(sorted(VALID_PRODUCTS))}"
        
    if product_type and product_type.lower() not in VALID_PRODUCT_TYPES:
        return False, f"Invalid product type. Please check available combinations."
        
    if asset_class and product and product_type:
        combo = (asset_class.lower(), product.lower(), product_type.lower())
        if combo not in VALID_COMBINATIONS:
            return False, "This combination of asset class, product, and product type is not available."
            
    return True, ""

# Configuration for supported data types
SUPPORTED_DATA_TYPES = {
    "monthly": True,    # Currently supported
    "quarterly": False, # Not yet supported
    "yearly": False     # Not yet supported
}

# System prompt for the AI
SYSTEM_PROMPT = """You are TradeWeb's MAR Explorer, an AI assistant specifically designed to analyze and explain TradeWeb's Monthly Activity Report (MAR) data and financial press releases.

Your primary capabilities:
1. Query and analyze MAR data from our database, which contains:
   - Asset Classes: Rates, Credit, Equities, Money Markets
   - Products: Cash, Derivatives
   - Product Types: Various specific products under each asset class
   - Metrics: volume and avg_volume (ADV)
   - Time: monthly data points with year and month information

You must only use valid combinations of asset_class, product, and product_type as defined in our database. For example:
- Rates + Cash + Mortgages
- Credit + Cash + European Credit
- Equities + Derivatives + Futures
Invalid combinations will be rejected.

2. Search and explain TradeWeb's financial press releases for context and event analysis

Data Analysis Guidelines:
1. For ANY numerical questions (volumes, ADV, percentages):
   - ALWAYS query the database
   - REQUIRE specific time periods (month/year)
   - If time period is missing, ask for clarification
   - Example good query: "What was the Rates volume in August 2025?"
   - Example vague query: "How is Credit volume doing?"

2. For context or "why" questions:
   - Use press release search for explanations
   - Combine with data analysis when relevant
   - Example: "Why did Mortgage ADV increase in August 2025?"

Current Limitations:
1. Only monthly data analysis is supported
2. If users ask about quarterly or yearly data, inform them we currently only support monthly analysis
3. Time periods must be specific (e.g., "August 2025", "Sep 2025")

When to Ask for Clarification:
1. Missing time period: "Could you specify which month and year you're interested in?"
2. Vague asset class/product: "Which specific asset class or product would you like to analyze?"
3. Unclear comparison period: "Would you like to compare with a specific previous period?"

Remember: Always prioritize accuracy and clarity in financial data reporting."""

# Function definitions for OpenAI
FUNCTION_DEFINITIONS = [
    {
        "type": "function",
        "function": {
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
        }
    },
    {
        "type": "function",
        "function": {
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
    }
]

def analyze_question(question: str) -> Tuple[bool, str, float]:
    """
    Analyze if the question is relevant to MAR data or press releases.
    Returns: (should_proceed, response_or_clarification, confidence)
    """
    try:
        # First check if question is relevant and what type it is
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": """Analyze this question and respond with JSON:
                {
                    "is_relevant": true/false,
                    "type": "data_query"/"context_query"/"clarification_needed",
                    "needs_clarification": true/false,
                    "clarification_reason": "time_period"/"asset_class"/"comparison_period"/null,
                    "data_frequency": "monthly"/"quarterly"/"yearly"/null
                }
                
                Question: """ + question}
            ],
            temperature=0,
            max_tokens=150
        )
        
        try:
            analysis = json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            logger.error("Failed to parse analysis JSON")
            return True, "I'll help you analyze that.", 0.5

        # If question is not relevant
        if not analysis.get("is_relevant", False):
            return False, "I'd be happy to help you with questions about TradeWeb's Monthly Activity Report (MAR) data or financial press releases. What would you like to know about those topics?", 1.0

        # If question needs clarification
        if analysis.get("needs_clarification", False):
            reason = analysis.get("clarification_reason")
            if reason == "time_period":
                return False, "Could you specify which month and year you're interested in? For example, 'August 2025' or 'September 2025'.", 1.0
            elif reason == "asset_class":
                return False, "Which specific asset class or product would you like to analyze?", 1.0
            elif reason == "comparison_period":
                return False, "Would you like to compare with a specific previous period?", 1.0
            
        # Check if asking for unsupported data frequency
        data_frequency = analysis.get("data_frequency")
        if data_frequency in ["quarterly", "yearly"] and not SUPPORTED_DATA_TYPES.get(data_frequency, False):
            return False, f"Currently, I can only analyze monthly data. {data_frequency.capitalize()} data analysis will be supported in the future.", 1.0

        # Question is good to proceed
        return True, "Proceeding with analysis", 0.95
        
    except Exception as e:
        logger.error(f"Error analyzing question: {str(e)}")
        return True, "I'll try to help you with that.", 0.5

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
            # Validate product filters if present
            if "filters" in params:
                filters = params["filters"]
                is_valid, error_msg = validate_product_query(
                    asset_class=filters.get("asset_class"),
                    product=filters.get("product"),
                    product_type=filters.get("product_type")
                )
                if not is_valid:
                    return {
                        "type": "error",
                        "content": error_msg
                    }
            
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
        # First, analyze if question is relevant and needs clarification
        should_proceed, message, confidence = analyze_question(question)
        if not should_proceed:
            return message, None, confidence
            
        # Initialize variables
        results = []
        viz_data = None
        
        # Get function calls from OpenAI
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question}
        ]
        
        response = client.chat.completions.create(
            model="gpt-4",
            messages=messages,
            tools=FUNCTION_DEFINITIONS,
            tool_choice="auto"
        )
        
        # For simple responses that don't need function calls
        tool_calls = response.choices[0].message.tool_calls
        if not tool_calls:
            return response.choices[0].message.content, None, 1.0
        
        # Execute functions and collect results
        for tool_call in tool_calls:
            func_name = tool_call.function.name
            func_args = json.loads(tool_call.function.arguments)
            result = execute_function(func_name, func_args)
            results.append(result)
            
            # Generate visualization if we have data
            if result["type"] == "data":
                viz_data = generate_visualization(result["content"])
        
        # Get final answer from OpenAI
        final_response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                *messages[1:],  # Skip the system message as we're adding it again
                {"role": "assistant", "content": "Here are the results of my analysis:"},
                {"role": "function", "name": "results", "content": json.dumps(results)}
            ]
        )
        
        answer = final_response.choices[0].message.content
        confidence = 0.95 if not any(r.get("type") == "error" for r in results) else 0.5
        
        return answer, {"results": results, "visualization": viz_data}, confidence
        
    except Exception as e:
        logger.error(f"Error processing question: {str(e)}")
        return str(e), None, 0.0