import os
from typing import List, Dict, Any, Optional
import logging
from pinecone import Pinecone, exceptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# :::::: Configuration :::::: #

PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY", "")
PINECONE_ENV = os.environ.get("PINECONE_ENV", "us-east1")  # or whichever region your Pinecone uses
INDEX_NAME = os.environ.get("PINECONE_INDEX_NAME", "pr-index")
PINECONE_NAMESPACE = os.environ.get("PINECONE_NAMESPACE", "tradeweb-namespace")

# :::::: Setup :::::: #

pc = Pinecone(api_key=PINECONE_API_KEY)

index = pc.Index(name=INDEX_NAME)

# :::::: Functions :::::: #

def upsert_records(
    records: List[Dict[str, Any]]
) -> None:
    """
    Upsert a batch of PR chunk records into Pinecone.

    Each record should be a dict with keys:
      - id: str
      - chunk_text: str
      - metadata: dict of additional metadata (year, month, report_type, etc.)

    If USE_EXTERNAL_EMBEDDING is True, it will encode chunk_text externally and send vector + metadata.
    If False, uses integrated embedding (Pinecone embeds chunk_text automatically).
    """
    try:
      
      # Integrated embedding: upsert_records
      # Flatten metadata into main record
      flattened_records = []
      for record in records:
          flattened_record = {
              "id": record["id"],
              "text": record["text"]
          }
          # Add metadata fields directly to record
          if "metadata" in record:
              flattened_record.update(record["metadata"])
          flattened_records.append(flattened_record)

      index.upsert_records(
          namespace=PINECONE_NAMESPACE,
          records=flattened_records
      )

      print(f"Auto-embedded & upserted {len(records)} chunks into '{INDEX_NAME}' in namespace '{PINECONE_NAMESPACE}'")
   
    except Exception as e:
      logger.exception(f"[upsert_records] Failed to upsert records: {records}")
      raise e


def search_content(
    query: str,
    top_k: int = 5,
    metadata: Optional[Dict[str, Any]] = None,
    fields: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Search content using semantic similarity with optional metadata filtering.
    
    Args:
        query: The search query text
        top_k: Number of top results to return (default: 5)
        metadata: Optional filter criteria with any of:
                 - report_type: str (e.g., "annual", "quarterly")
                 - year: int (e.g., 2023)
                 - month: int (1-12)
                 - quarter: int (1-4)
                 - url: str

    Examples:
        # Simple search
        results = search_content("financial performance 2023")

        # Search with metadata filter
        results = search_content(
            "revenue growth",
            metadata={"year": {"$gte": 2023}, "report_type": "annual"}
        )

        Refer to documentation at:
        https://docs.pinecone.io/guides/search/filter-by-metadata
    
    Returns:
        List of matches with id, score, text and metadata
    """
    try:
        logger.info(f"Searching content with query: {query} and fields: {fields}")

        # Construct the search query
        search_query = {
            "inputs" : {"text": query},
            "top_k" : top_k,
            "filter" : metadata if metadata else {},
        }

        # Execute search with integrated embedding
        if fields and len(fields) > 0:
          resp = index.search(
              namespace = PINECONE_NAMESPACE,
              query = search_query,
              fields = fields
          )
        else:
          resp = index.search(
              namespace = PINECONE_NAMESPACE,
              query = search_query
          )
        
        return resp

    except Exception as e:
      logger.exception(f"[search_content] Failed to search for query: {query} and fields: {fields}")
      raise e


def confirm_and_delete_all_records():
    '''
      Confirm and delete all records from the index
    '''

    def delete_all_records():
      index.delete(delete_all=True, namespace=PINECONE_NAMESPACE)
      print(f"Deleted all records from '{INDEX_NAME}' in namespace '{PINECONE_NAMESPACE}'")

    prompt = f"Are you sure you want to delete *all* records from index '{INDEX_NAME}' in namespace '{PINECONE_NAMESPACE}'? Type **Yes** to confirm: "
    answer = input(prompt)
    if answer == "Yes":
        delete_all_records()
    else:
        print("Deletion cancelled.")
