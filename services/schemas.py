# ------------------------------
# Project schemas
# ------------------------------

MAR_VOLUME_SCHEMA = {
    "asset_class": "string",
    "product_type": "string",
    "product": "string",
    "year_month": "string",
    "year": "int32",
    "month": "int32",
    "volume": "float64",
    "updated_at": "string",  # Store as string in ISO format for better compatibility
}

MAR_TRADE_DAYS_SCHEMA = {
    "asset_class": "string",
    "product_type": "string",
    "product": "string",
    "year_month": "string",
    "year": "int32",
    "month": "int32",
    "trade_days": "float64",
    "updated_at": "string",  # Store as string in ISO format for better compatibility
}

MAR_COMBINED_SCHEMA = {
    "asset_class": "string",
    "product_type": "string",
    "product": "string",
    "year_month": "string",
    "year": "int32",
    "month": "int32",
    "volume": "float64",
    "adv": "float64",
    "updated_at": "string",  # Store as string in ISO format for better compatibility
}