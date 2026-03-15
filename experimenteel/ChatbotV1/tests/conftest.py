"""Shared pytest fixtures."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from httpx import AsyncClient, ASGITransport

from main import app
from app.core.dependencies import (
    get_document_service, get_chat_service
)
from app.services.document_service import DocumentService
from app.services.chat_service import ChatService
from app.repositories.vector_repository import VectorMatch


@pytest.fixture
def mock_vector_repo():
    repo = AsyncMock()
    repo.similarity_search.return_value = [
        VectorMatch(
            chunk_id="doc1::chunk::0",
            doc_id="doc1",
            filename="inpatient-protocol.pdf",
            text="The survey should be conducted on a single day.",
            chunk_index=0,
            similarity_score=0.88,
            language="en",
            doc_type="protocol",
        )
    ]
    repo.upsert_batch.return_value = 3
    repo.delete_by_doc_id.return_value = 3
    return repo


@pytest.fixture
def mock_embedding_repo():
    repo = AsyncMock()
    repo.embed.return_value = [0.1] * 3072
    repo.embed_batch.return_value = [[0.1] * 3072, [0.2] * 3072]
    return repo


@pytest.fixture
def mock_llm_repo():
    repo = AsyncMock()
    repo.complete.return_value = "The survey should be conducted on a single day."
    return repo


@pytest.fixture
def mock_doc_repo():
    from app.models.document import Document, DocumentStatus
    from datetime import datetime
    repo = AsyncMock()
    sample_doc = Document(
        id="doc1", filename="test.pdf", title="Test Doc",
        language="en", doc_type="protocol",
        status=DocumentStatus.INGESTED, chunk_count=3,
    )
    repo.save.return_value = sample_doc
    repo.get_by_id.return_value = sample_doc
    repo.list_all.return_value = [sample_doc]
    return repo


@pytest.fixture
def chat_service(mock_vector_repo, mock_embedding_repo, mock_llm_repo):
    from app.core.config import get_settings
    return ChatService(
        vector_repo=mock_vector_repo,
        embedding_repo=mock_embedding_repo,
        llm_repo=mock_llm_repo,
        settings=get_settings(),
    )


@pytest.fixture
def document_service(mock_doc_repo, mock_vector_repo, mock_embedding_repo):
    from app.core.config import get_settings
    return DocumentService(
        doc_repo=mock_doc_repo,
        vector_repo=mock_vector_repo,
        embedding_repo=mock_embedding_repo,
        settings=get_settings(),
    )


@pytest.fixture
async def client(chat_service, document_service):
    app.dependency_overrides[get_chat_service] = lambda: chat_service
    app.dependency_overrides[get_document_service] = lambda: document_service
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()
