# ------------------------------
# Project schemas
# ------------------------------

MAR_VOLUME_M_SCHEMA = {
    "asset_class": "string",
    "product": "string",
    "product_type": "string",
    "year_month": "string",
    "year": "int32",
    "month": "int32",
    "volume": "float64",
}

MAR_TRADE_DAYS_M_SCHEMA = {
    "asset_class": "string",
    "product": "string",
    "product_type": "string",
    "year_month": "string",
    "year": "int32",
    "month": "int32",
    "trade_days": "float64",
}

PR_SCHEMA = {
    "id": "int32",
    "text": "string",
    "embedding": "object",  # store as Python object (list of floats)
}

LOG_SCHEMA = {
    "ts": "datetime64[ns]",
    "question": "string",
    "confidence": "float64",
    "citations": "string",
}