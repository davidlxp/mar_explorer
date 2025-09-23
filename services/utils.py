# ------------------------------
# Services's utils
# ------------------------------

import pandas as pd
import tiktoken
import logging
import re
from services.constants import *
import json
import os
from pathlib import Path

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

def write_file(text: str, file_path: str):
    '''
      Write text to a file.
    '''
    with open(file_path, 'w') as f:
        f.write(text)
    logger.info(f"Wrote text to {file_path}")
    return file_path

def read_file(file_path: str) -> str:
    '''
      Read text from a file.
    '''
    with open(file_path, 'r') as f:
        return f.read()
    return text

def save_meta_file(meta_data: dict, org_file_dir: str, org_file_name: str):
    '''
      Save the meta data to a file.
      Args:
        meta_data: The meta data to save
        org_file_dir: The directory of the original file
        org_file_name: The name of the original file
    '''
    # Remove if the orignal file has any suffix using Path
    org_file_name = Path(org_file_name).with_suffix("")

    # Save the meta data
    os.makedirs(org_file_dir, exist_ok=True)
    with open(f'{org_file_dir}/{org_file_name}-meta.json', 'w') as f:
        json.dump(meta_data, f)

def get_meta_file(file_path: str) -> dict:
    """
    Get the metadata JSON corresponding to a file path.
    Example: myfile.md -> myfile-meta.json
    """
    base = Path(file_path).with_suffix("")  # drop the extension
    meta_file_path = f"{base}-meta.json"

    try:
        with open(meta_file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Meta file not found: {meta_file_path}")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Corrupt meta file {meta_file_path}: {e}")
        return {}

def regularize_url(url: str) -> str:
    '''
      Regularize the URL.
    '''
    if url.endswith("/"):
        url = url[:-1]
    return url

def get_url_last_part(url: str) -> str:
    '''
      Get the last part of the URL.
    '''
    url = regularize_url(url)
    return url.split("/")[-1]

def get_token_count(text: str, model_name: str = DEFAULT_EMBEDDING_MODEL) -> int:
    '''
      Get the token count of the text.
    '''
    encoding = tiktoken.encoding_for_model(model_name)
    tokens = encoding.encode(text)
    
    return len(tokens)