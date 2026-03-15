"""Application configuration — all values loaded from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Groq (LLM)
    groq_api_key: str
    groq_base_url: str = "https://api.groq.com/openai/v1"
    groq_chat_model: str = "llama-3.1-8b-instant"
    openai_chat_temperature: float = 0.2

    # Embeddings via local Ollama
    ollama_base_url: str = "http://localhost:11434"
    ollama_embedding_model: str = "nomic-embed-text"

    # Chroma
    chroma_persist_dir: str = "data/chroma"
    chroma_collection_name: str = "gpps_docs"

    # RAG behaviour
    retrieval_top_k: int = 5
    confidence_threshold: float = 0.72
    chunk_size: int = 512
    chunk_overlap: int = 50

    # App
    environment: str = "development"
    log_level: str = "INFO"
    max_upload_size_mb: int = 20
    allowed_extensions: list[str] = [".pdf", ".txt", ".md"]

    escalation_email: str = "support@global-pps.com"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]

# from pydantic_settings import BaseSettings, SettingsConfigDict
# from functools import lru_cache
#
#
# class Settings(BaseSettings):
#     model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
#
#     # OpenAI
#     openai_api_key: str
#     openai_embedding_model: str = "text-embedding-3-large"
#     openai_chat_model: str = "gpt-4o"
#     openai_chat_temperature: float = 0.2
#
#     # Pinecone
#     pinecone_api_key: str
#     pinecone_index_name: str = "gpps-docs"
#     pinecone_environment: str = "us-east-1"
#
#     # RAG behaviour
#     retrieval_top_k: int = 5
#     confidence_threshold: float = 0.72
#     chunk_size: int = 512
#     chunk_overlap: int = 50
#
#     # App
#     environment: str = "development"
#     log_level: str = "INFO"
#     max_upload_size_mb: int = 20
#     allowed_extensions: list[str] = [".pdf", ".txt", ".md"]
#
#     # Escalation (stub for prototype)
#     escalation_email: str = "support@global-pps.com"
#
#
# @lru_cache
# def get_settings() -> Settings:
#     """Return cached settings instance."""
#     return Settings()  # type: ignore[call-arg]
