"""FastAPI dependency providers.

All service and repository instances are created here and injected via Depends().
"""

from fastapi import Depends

from app.core.clients import get_embedding_client, get_qdrant_client, get_llm_client
from app.core.config import get_settings, Settings
from app.repositories.document_repository import DocumentRepository
from app.repositories.embedding_repository import EmbeddingRepository
from app.repositories.llm_repository import LLMRepository
from app.repositories.vector_repository import VectorRepository
from app.services.chat_service import ChatService
from app.services.document_service import DocumentService


# ── Repositories ──────────────────────────────────────────────────────────────

def get_vector_repository() -> VectorRepository:
    return VectorRepository(collection=get_qdrant_client())


def get_document_repository() -> DocumentRepository:
    return DocumentRepository()


def get_embedding_repository(
    settings: Settings = Depends(get_settings),
) -> EmbeddingRepository:
    return EmbeddingRepository(
        # client=get_openai_client(),
        client=get_embedding_client(),
        model=settings.ollama_embedding_model,
    )


def get_llm_repository(
    settings: Settings = Depends(get_settings),
) -> LLMRepository:
    return LLMRepository(
        client=get_llm_client(),        # ← was get_embedding_client()
        model=settings.groq_chat_model,
        temperature=settings.openai_chat_temperature,
    )

# ── Services ──────────────────────────────────────────────────────────────────

def get_document_service(
    doc_repo: DocumentRepository = Depends(get_document_repository),
    vector_repo: VectorRepository = Depends(get_vector_repository),
    embedding_repo: EmbeddingRepository = Depends(get_embedding_repository),
    settings: Settings = Depends(get_settings),
) -> DocumentService:
    return DocumentService(
        doc_repo=doc_repo,
        vector_repo=vector_repo,
        embedding_repo=embedding_repo,
        settings=settings,
    )


def get_chat_service(
    vector_repo: VectorRepository = Depends(get_vector_repository),
    embedding_repo: EmbeddingRepository = Depends(get_embedding_repository),
    llm_repo: LLMRepository = Depends(get_llm_repository),
    settings: Settings = Depends(get_settings),
) -> ChatService:
    return ChatService(
        vector_repo=vector_repo,
        embedding_repo=embedding_repo,
        llm_repo=llm_repo,
        settings=settings,
    )
