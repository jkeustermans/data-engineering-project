"""Repository for generating text embeddings via OpenAI."""

import logging
from openai import AsyncOpenAI
from app.core.exceptions import EmbeddingError

logger = logging.getLogger(__name__)


class EmbeddingRepository:
    """Wraps OpenAI embedding API calls."""

    def __init__(self, client: AsyncOpenAI, model: str) -> None:
        self._client = client
        self._model = model

    async def embed(self, text: str) -> list[float]:
        """Embed a single text string. Returns a float vector."""
        try:
            logger.debug("Embedding text (len=%d)", len(text))
            response = await self._client.embeddings.create(
                input=text,
                model=self._model,
            )
            return response.data[0].embedding
        except Exception as exc:
            raise EmbeddingError(f"Embedding call failed: {exc}") from exc

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a list of texts in a single API call."""
        if not texts:
            return []
        try:
            logger.info("Batch embedding %d chunks", len(texts))
            response = await self._client.embeddings.create(
                input=texts,
                model=self._model,
            )
            # API returns embeddings in the same order as input
            return [item.embedding for item in sorted(response.data, key=lambda x: x.index)]
        except Exception as exc:
            raise EmbeddingError(f"Batch embedding call failed: {exc}") from exc
