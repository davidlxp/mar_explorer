# ------------------------------
# Services's utils
# ------------------------------

import pandas as pd
import logging

logger = logging.getLogger(__name__)

def enforce_schema(df, schema, strict=True):
    '''
      Enforce a schema on a DataFrame.

      Args:
          df: DataFrame to enforce schema on
          schema: dict of {column: dtype}
          strict: if True, raise error if columns are missing

      Returns:
          DataFrame with schema applied
    '''
    expected_cols = set(schema.keys())
    actual_cols = set(df.columns)

    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    if missing:
        msg = f"Missing expected columns: {missing}"
        if strict:
            raise ValueError(msg)
        else:
            logger.warning(msg)

    if extra:
        logger.warning(f"Unexpected extra columns: {extra}")

    # Cast columns
    for col, dtype in schema.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                logger.error(f"Could not cast column {col} to {dtype}: {e}")
                raise
    return df
