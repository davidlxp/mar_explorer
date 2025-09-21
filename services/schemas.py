# ------------------------------
# Project schemas
# ------------------------------

MAR_VOLUME_SCHEMA = {
    "asset_class": "string",
    "product": "string",
    "product_type": "string",
    "year_month": "string",
    "year": "int32",
    "month": "int32",
    "volume": "float64",
}

MAR_TRADE_DAYS_SCHEMA = {
    "asset_class": "string",
    "product": "string",
    "product_type": "string",
    "year_month": "string",
    "year": "int32",
    "month": "int32",
    "trade_days": "float64",
}

MAR_COMBINED_SCHEMA = {
    "asset_class": "string",
    "product": "string",
    "product_type": "string",
    "year_month": "string",
    "year": "int32",
    "month": "int32",
    "volume": "float64",
    "avg_volume": "float64",
}