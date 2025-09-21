# services/embeddings/providers/hf_embedder.py
from typing import List
from sentence_transformers import SentenceTransformer

# # Load model once
# _model = SentenceTransformer("all-MiniLM-L6-v2")

def embed_texts_hf(texts: List[str]) -> List[list[float]]:
    """
    Embed texts using SentenceTransformers MiniLM (local model).
    Returns list of embeddings (384-dim each).
    """
    # Temporary put it here to avoid loading the model every time
    _model = SentenceTransformer("all-MiniLM-L6-v2")

    embs = _model.encode(texts, batch_size=32, show_progress_bar=False)
    return [emb.tolist() for emb in embs]
