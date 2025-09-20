from typing import List, Protocol

class EmbeddingsProvider(Protocol):
    def __call__(self, texts: List[str]) -> List[list[float]]: 
      ...
