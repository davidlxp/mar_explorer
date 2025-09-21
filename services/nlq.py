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

def validate_product_query(asset_class: str = None, product: str = None, product_type: str = None) -> Tuple[bool, str, Dict]:
    """
    Validate if the product combination exists in our database.
    Returns: (is_valid, error_message, normalized_values)
    """
    normalized = {}
    
    if asset_class:
        asset_class_lower = asset_class.lower()
        if asset_class_lower not in VALID_ASSET_CLASSES:
            return False, f"Invalid asset class. Available options are: {', '.join(sorted(VALID_ASSET_CLASSES))}", {}
        normalized['asset_class'] = asset_class_lower
        
    if product:
        product_lower = product.lower()
        if product_lower not in VALID_PRODUCTS:
            return False, f"Invalid product. Available options are: {', '.join(sorted(VALID_PRODUCTS))}", {}
        normalized['product'] = product_lower
        
    if product_type:
        product_type_lower = product_type.lower()
        if product_type_lower not in VALID_PRODUCT_TYPES:
            return False, f"Invalid product type. Please check available combinations.", {}
        normalized['product_type'] = product_type_lower
        
    if asset_class and product and product_type:
        combo = (asset_class.lower(), product.lower(), product_type.lower())
        if combo not in VALID_COMBINATIONS:
            return False, "This combination of asset class, product, and product type is not available.", {}
            
    return True, "", normalized

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
   - Metrics: volume (total trading volume) and avg_volume (ADV)
   - Time: monthly data points with year and month information

Data Query Guidelines:
1. For volume-related questions:
   - When users ask about "volume", "trading volume", "total volume" → query the 'volume' metric
   - When users ask about "ADV", "average volume", "average daily volume" → query the 'avg_volume' metric
   - If no specific asset class is mentioned, query total volume across all asset classes
   - Always include the 'volume' metric in your query when users ask about trading activity

2. For time periods:
   - Extract month and year from queries like "August 2025", "in 2025 August", "for August 2025"
   - Time period is mandatory for all queries
   - Current data is monthly only

3. For asset classes and products:
   - If not specified, aggregate across all asset classes
   - Use only valid combinations from our database:
     * Rates + Cash + Mortgages
     * Credit + Cash + European Credit
     * Equities + Derivatives + Futures
     * etc.

Example Queries:
- "What's the trading volume in August 2025?" → Query total volume across all assets
- "Show me Rates volume for August 2025" → Query volume filtered by Rates
- "What's the complete trading volume in August 2025?" → Query total volume across all assets

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
            "description": "Query MAR (Monthly Activity Report) data from the database. Use this for numerical/statistical questions about trading volumes and ADV.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "description": "List of metrics to query. Use 'volume' for total trading volume, 'avg_volume' for ADV. For general volume questions, always include 'volume'.",
                        "items": {"type": "string"},
                        "examples": [["volume"], ["volume", "avg_volume"]]
                    },
                    "filters": {
                        "type": "object",
                        "description": "Filter conditions. If no asset_class specified, query will return total volume across all asset classes.",
                        "properties": {
                            "asset_class": {
                                "type": "string",
                                "description": "Asset class to filter by (rates, credit, equities, money markets). Optional - if not provided, will aggregate across all."
                            },
                            "product": {
                                "type": "string",
                                "description": "Product type (cash, derivatives). Required if asset_class is provided."
                            },
                            "product_type": {
                                "type": "string",
                                "description": "Specific product type. Must be valid for the asset_class and product combination."
                            },
                            "year_month": {"type": "string"},
                            "year": {"type": "integer"},
                            "month": {"type": "integer"}
                        }
                    },
                    "group_by": {
                        "type": "array",
                        "description": "Fields to group by. Use for aggregating data.",
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

# Keep track of conversation state
_conversation_context = {
    "awaiting_clarification": False,
    "original_question": None,
    "clarification_type": None
}

def analyze_question(question: str) -> Tuple[bool, str, float]:
    """
    Analyze if the question is relevant to MAR data or press releases.
    Returns: (should_proceed, response_or_clarification, confidence)
    """
    try:
        # Check if this is a clarification to a previous question
        if _conversation_context["awaiting_clarification"]:
            # Try to extract time information
            if _conversation_context["clarification_type"] == "time_period":
                # Check if this is a valid time specification
                if "20" in question:  # Basic check for year
                    _conversation_context["awaiting_clarification"] = False
                    return True, "Proceeding with analysis", 0.95

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

        # If question is not relevant and we're not awaiting clarification
        if not analysis.get("is_relevant", False) and not _conversation_context["awaiting_clarification"]:
            return False, "I'd be happy to help you with questions about TradeWeb's Monthly Activity Report (MAR) data or financial press releases. What would you like to know about those topics?", 1.0

        # If question needs clarification
        if analysis.get("needs_clarification", False):
            reason = analysis.get("clarification_reason")
            # Store the context for follow-up
            _conversation_context["awaiting_clarification"] = True
            _conversation_context["original_question"] = question
            _conversation_context["clarification_type"] = reason
            
            if reason == "time_period":
                return False, "Could you specify which month and year you're interested in? For example, 'August 2025' or 'September 2025'.", 1.0
            elif reason == "asset_class":
                return False, "Which specific asset class or product would you like to analyze?", 1.0
            elif reason == "comparison_period":
                return False, "Would you like to compare with a specific previous period?", 1.0
            
        # Check if asking for unsupported data frequency
        data_frequency = analysis.get("data_frequency")
        if data_frequency in ["quarterly", "yearly"] and not SUPPORTED_DATA_TYPES.get(data_frequency, False):
            _conversation_context["awaiting_clarification"] = False  # Reset context
            return False, f"Currently, I can only analyze monthly data. {data_frequency.capitalize()} data analysis will be supported in the future.", 1.0

        # Question is good to proceed
        _conversation_context["awaiting_clarification"] = False  # Reset context
        return True, "Proceeding with analysis", 0.95
        
    except Exception as e:
        logger.error(f"Error analyzing question: {str(e)}")
        return True, "I'll try to help you with that.", 0.5

def build_sql_query(params: Dict) -> str:
    """Build SQL query from parameters"""
    # Handle metrics
    metrics = []
    for metric in params["metrics"]:
        if params.get("group_by"):
            metrics.append(f"SUM({metric}) as {metric}")
        else:
            metrics.append(metric)
    metrics_str = ", ".join(metrics)
    
    # Build base query
    base_query = f"SELECT {metrics_str}"
    
    if params.get("group_by"):
        group_fields = ", ".join(params["group_by"])
        base_query += f", {group_fields}"
        
    base_query += " FROM mar_explorer.main.mar_combined_m"
    
    if params.get("filters"):
        conditions = []
        for field, value in params["filters"].items():
            if value is not None:
                # Handle different types of values
                if isinstance(value, str):
                    # Convert string values to lowercase
                    conditions.append(f"{field} = '{value.lower()}'")
                elif isinstance(value, (int, float)):
                    # Numeric values don't need quotes
                    conditions.append(f"{field} = {value}")
                else:
                    conditions.append(f"{field} = '{str(value).lower()}'")
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
                is_valid, error_msg, normalized_values = validate_product_query(
                    asset_class=filters.get("asset_class"),
                    product=filters.get("product"),
                    product_type=filters.get("product_type")
                )
                if not is_valid:
                    return {
                        "type": "error",
                        "content": error_msg
                    }
                    
                # Update filters with normalized (lowercase) values
                params["filters"].update(normalized_values)
            
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

def format_data_summary(data: List[Dict]) -> str:
    """Format data results into a readable summary"""
    if not data:
        return "No data available."
        
    df = pd.DataFrame(data)
    
    # Convert column names to lowercase
    df.columns = [col.lower() for col in df.columns]
    
    summary = []
    
    # Add total volume if present
    if 'volume' in df.columns:
        total_volume = df['volume'].sum()
        summary.append(f"Total Volume: {total_volume:,.2f}")
        
    # Add average daily volume if present
    if 'avg_volume' in df.columns:
        avg_volume = df['avg_volume'].mean()
        summary.append(f"Average Daily Volume: {avg_volume:,.2f}")
    
    # Add time period context
    if 'year_month' in df.columns:
        periods = sorted(df['year_month'].unique())
        if len(periods) == 1:
            summary.append(f"Time Period: {periods[0]}")
        else:
            summary.append(f"Time Period: {periods[0]} to {periods[-1]}")
    
    # Add asset class breakdown if present
    if 'asset_class' in df.columns:
        asset_classes = df['asset_class'].unique()
        if len(asset_classes) == 1:
            summary.append(f"Asset Class: {asset_classes[0].title()}")
        elif len(asset_classes) > 1:
            summary.append("Asset Classes: " + ", ".join(ac.title() for ac in asset_classes))
    
    return "\n".join(summary)

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
            
            # Generate data summary if we have data
            if result["type"] == "data":
                data_summary = format_data_summary(result["content"])
                result["summary"] = data_summary
        
        # Get final answer from OpenAI with data summary
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
        
        return answer, {"results": results}, confidence
        
    except Exception as e:
        logger.error(f"Error processing question: {str(e)}")
        return str(e), None, 0.0

def run_cli():
    """Run a command-line interface for testing the Q&A system"""
    print("\n🤖 TradeWeb MAR Explorer CLI")
    print("Type 'exit' to quit, 'clear' to clear the conversation\n")
    
    while True:
        try:
            # Get user input
            question = input("\n👤 You: ")
            
            # Handle commands
            if question.lower() == 'exit':
                print("\n👋 Goodbye!")
                break
            elif question.lower() == 'clear':
                _conversation_context["awaiting_clarification"] = False
                _conversation_context["original_question"] = None
                _conversation_context["clarification_type"] = None
                print("\n🔄 Conversation cleared!")
                continue
            
            # Process question
            answer, data, confidence = process_question(question)
            
            # Print response
            print(f"\n🤖 Assistant: {answer}")
            
            # If we have data, print the summary
            if data and data.get("results"):
                for result in data["results"]:
                    if result.get("summary"):
                        print("\n📊 Data Summary:")
                        print(result["summary"])
            
            # Print confidence score
            confidence_indicator = "🟢" if confidence > 0.8 else "🟡" if confidence > 0.5 else "🔴"
            print(f"\n{confidence_indicator} Confidence: {confidence:.2f}")
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")

if __name__ == "__main__":
    run_cli()