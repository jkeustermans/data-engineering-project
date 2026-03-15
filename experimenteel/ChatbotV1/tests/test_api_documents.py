"""Integration tests for the Documents API endpoints."""

import pytest


@pytest.mark.asyncio
async def test_list_documents(client):
    resp = await client.get("/api/v1/documents/")
    assert resp.status_code == 200
    body = resp.json()
    assert "total" in body
    assert "documents" in body


@pytest.mark.asyncio
async def test_get_document(client):
    resp = await client.get("/api/v1/documents/doc1")
    assert resp.status_code == 200
    assert resp.json()["doc_id"] == "doc1"


@pytest.mark.asyncio
async def test_get_document_not_found(client, mock_doc_repo):
    from app.core.exceptions import DocumentNotFoundError
    mock_doc_repo.get_by_id.side_effect = DocumentNotFoundError("not found")
    resp = await client.get("/api/v1/documents/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_document(client):
    resp = await client.delete("/api/v1/documents/doc1")
    assert resp.status_code == 200
    assert resp.json()["doc_id"] == "doc1"


@pytest.mark.asyncio
async def test_health_check(client):
    resp = await client.get("/api/v1/health/")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
