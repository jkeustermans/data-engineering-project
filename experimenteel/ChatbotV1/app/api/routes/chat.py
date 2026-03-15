"""Chat API router — POST /api/v1/chat/query."""

import logging
from fastapi import APIRouter, Depends, HTTPException

from app.schemas.chat_schemas import QueryRequest, AnswerResponse
from app.services.chat_service import ChatService
from app.core.dependencies import get_chat_service
from app.core.exceptions import EmbeddingError, RetrievalError, LLMError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["Chat"])


@router.post("/query", response_model=AnswerResponse, summary="Ask a question")
async def query(
    request: QueryRequest,
    chat_service: ChatService = Depends(get_chat_service),
) -> AnswerResponse:
    """
    Submit a natural-language question to the RAG pipeline.

    Returns a grounded answer with:
    - `confidence`: high / medium / low
    - `sources`: list of source document chunks used
    - `escalate`: true when the chatbot is not confident — route to human expert
    """
    try:
        return await chat_service.answer(
            question=request.question,
            language=request.language,
            top_k=request.top_k,
        )
    except (EmbeddingError, RetrievalError) as exc:
        logger.error("RAG pipeline error: %s", exc)
        raise HTTPException(status_code=503, detail="Knowledge base temporarily unavailable.")
    except LLMError as exc:
        logger.error("LLM error: %s", exc)
        raise HTTPException(status_code=503, detail="Language model temporarily unavailable.")
