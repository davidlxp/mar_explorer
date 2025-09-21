# services/embeddings/__init__.py
import os
from typing import cast
from .providers.base import EmbeddingsProvider
from .providers.openai_embedder import embed_texts_openai
from .providers.hf_embedder import embed_texts_hf
from services.constants import EMBED_PROVIDER

def get_embedder() -> EmbeddingsProvider:
    provider = EMBED_PROVIDER.lower()
    if provider == "openai":
        return cast(EmbeddingsProvider, lambda texts: embed_texts_openai(texts))
    elif provider == "hf":
        return cast(EmbeddingsProvider, lambda texts: embed_texts_hf(texts))
    else:
        raise ValueError(f"Unknown EMBED_PROVIDER={provider}")
