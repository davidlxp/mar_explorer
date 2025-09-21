import os
from typing import List, Dict, Any, Optional

from pinecone import Pinecone, exceptions

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


# def search_pr_vectors(
#     query: str,
#     top_k: int = 5,
#     filter_metadata: Optional[Dict[str, Any]] = None
# ) -> List[Dict[str, Any]]:
#     """
#     Perform a semantic search using vector similarity over all records.

#     If filtering is needed, use `search_pr_with_filter`.
#     This returns vector-based search (no filter) OR with filtering if given.
#     """

#     if USE_EXTERNAL_EMBEDDING:
#         # you need to encode externally
#         from sentence_transformers import SentenceTransformer
#         external_embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
#         q_vec = external_embedder.encode([query])[0].tolist()
#         # search by vector
#         resp = index.query(
#             vector=q_vec,
#             top_k=top_k,
#             include_metadata=True,
#             filter=filter_metadata
#         )
#     else:
#         # Integrated embedding: search by text
#         # the search method likely expects a "search" or "query" call depending on SDK version
#         resp = index.search(
#             top_k=top_k,
#             text=query,
#             include_metadata=True,
#             filter=filter_metadata
#         )

#     matches = []
#     for m in resp["matches"]:
#         # m.metadata contains your metadata + chunk_text
#         res = {
#             "id": m["id"],
#             "score": m["score"],
#             **m["metadata"]
#         }
#         matches.append(res)

#     return matches


# def search_pr_with_filter(
#     query: str,
#     top_k: int = 5,
#     year: Optional[int] = None,
#     month: Optional[int] = None,
#     quarter: Optional[int] = None,
#     report_type: Optional[str] = None
# ) -> List[Dict[str, Any]]:
#     """
#     Semantic search but only on items matching provided metadata filters.
#     """

#     filt: Dict[str, Any] = {}
#     if year is not None:
#         filt["year"] = year
#     if month is not None:
#         filt["month"] = month
#     if quarter is not None:
#         filt["quarter"] = quarter
#     if report_type is not None:
#         filt["report_type"] = report_type

#     return search_pr_vectors(query, top_k=top_k, filter_metadata=filt)
