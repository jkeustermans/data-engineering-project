from functools import lru_cache
from openai import AsyncOpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams
from app.core.config import get_settings

settings = get_settings()
print(">>> GROQ_BASE_URL:", settings.groq_base_url)
print(">>> GROQ_API_KEY starts with:", settings.groq_api_key[:8])

@lru_cache
def get_llm_client() -> AsyncOpenAI:
    settings = get_settings()
    print(">>> LLM client base_url:", settings.groq_base_url)
    print(">>> LLM URL:", settings.groq_base_url)
    print(">>> GROQ KEY starts with:", settings.groq_api_key[:8])
    return AsyncOpenAI(
        api_key=settings.groq_api_key,
        base_url=settings.groq_base_url,
    )


@lru_cache
def get_embedding_client() -> AsyncOpenAI:
    settings = get_settings()
    return AsyncOpenAI(
        api_key="ollama",
        base_url=f"{settings.ollama_base_url}/v1",
    )


@lru_cache
def get_qdrant_client():
    """Local Qdrant client — persists to disk, no server needed."""
    settings = get_settings()
    client = QdrantClient(path=settings.chroma_persist_dir)  # reuses same config key
    # Create collection if it doesn't exist yet
    existing = [c.name for c in client.get_collections().collections]
    if settings.chroma_collection_name not in existing:
        client.create_collection(
            collection_name=settings.chroma_collection_name,
            vectors_config=VectorParams(size=768, distance=Distance.COSINE),
            # nomic-embed-text produces 768-dimensional vectors
        )
    return client