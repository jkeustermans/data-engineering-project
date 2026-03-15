"""Integration tests for the Chat API endpoints."""

import pytest


@pytest.mark.asyncio
async def test_query_returns_200(client):
    resp = await client.post("/api/v1/chat/query", json={"question": "What is Global-PPS?"})
    assert resp.status_code == 200
    body = resp.json()
    assert "answer" in body
    assert "confidence" in body
    assert "sources" in body
    assert "escalate" in body


@pytest.mark.asyncio
async def test_query_rejects_short_question(client):
    resp = await client.post("/api/v1/chat/query", json={"question": "Hi"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_query_with_language(client):
    resp = await client.post("/api/v1/chat/query", json={"question": "Comment s'inscrire?", "language": "fr"})
    assert resp.status_code == 200
    assert resp.json()["language"] == "fr"
