"""Unit tests for ChatService."""

import pytest
from app.schemas.chat_schemas import ConfidenceLevel


@pytest.mark.asyncio
async def test_answer_returns_high_confidence_for_good_match(chat_service, mock_vector_repo):
    mock_vector_repo.similarity_search.return_value[0].similarity_score = 0.90
    result = await chat_service.answer("What day should the survey be conducted?")
    assert result.confidence == ConfidenceLevel.HIGH
    assert result.escalate is False
    assert len(result.sources) > 0


@pytest.mark.asyncio
async def test_answer_escalates_when_confidence_low(chat_service, mock_vector_repo):
    mock_vector_repo.similarity_search.return_value[0].similarity_score = 0.50
    result = await chat_service.answer("Some obscure question with no match")
    assert result.confidence == ConfidenceLevel.LOW
    assert result.escalate is True
    assert result.escalation_reason is not None


@pytest.mark.asyncio
async def test_answer_escalates_when_no_matches(chat_service, mock_vector_repo):
    mock_vector_repo.similarity_search.return_value = []
    result = await chat_service.answer("Unknown question")
    assert result.escalate is True
    assert result.confidence == ConfidenceLevel.LOW


@pytest.mark.asyncio
async def test_answer_escalates_when_llm_says_no_info(chat_service, mock_llm_repo, mock_vector_repo):
    mock_vector_repo.similarity_search.return_value[0].similarity_score = 0.85
    mock_llm_repo.complete.return_value = "I don't have enough information to answer."
    result = await chat_service.answer("Something the LLM can't answer")
    assert result.escalate is True


@pytest.mark.asyncio
async def test_answer_includes_sources(chat_service):
    result = await chat_service.answer("How do I register?")
    assert len(result.sources) >= 1
    assert result.sources[0].similarity_score > 0
