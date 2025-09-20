# minimal, stateless embedder
from typing import List
from openai import OpenAI
from services.constants import *

_client = OpenAI()

def embed_texts_openai(texts: List[str], model: str = DEFAULT_EMBEDDING_MODEL) -> List[list[float]]:

    # OpenAI accepts a list of strings; response.data is aligned to inputs
    resp = _client.embeddings.create(model=model, input=texts)
    return [d.embedding for d in resp.data]
