"""Chat service — RAG pipeline: embed → retrieve → generate → score."""

import logging

from app.repositories.vector_repository import VectorRepository, VectorMatch
from app.repositories.embedding_repository import EmbeddingRepository
from app.repositories.llm_repository import LLMRepository
from app.schemas.chat_schemas import AnswerResponse, ConfidenceLevel, SourceReference
from app.core.config import Settings

logger = logging.getLogger(__name__)

_NO_INFO_PHRASE = "I don't have enough information"


class ChatService:
    """Orchestrates the RAG pipeline to produce grounded answers."""

    def __init__(
        self,
        vector_repo: VectorRepository,
        embedding_repo: EmbeddingRepository,
        llm_repo: LLMRepository,
        settings: Settings,
    ) -> None:
        self._vector_repo = vector_repo
        self._embedding_repo = embedding_repo
        self._llm_repo = llm_repo
        self._settings = settings

    async def answer(self, question: str, language: str = "en", top_k: int = 5) -> AnswerResponse:
        """Full RAG pipeline. Returns structured answer with confidence and sources."""

        # 1. Embed the question
        query_vector = await self._embedding_repo.embed(question)

        # 2. Retrieve top-k chunks, prefer matching language, fall back to en
        matches = await self._vector_repo.similarity_search(
            vector=query_vector,
            top_k=top_k,
            language_filter=language if language != "en" else None,
        )
        # If non-English retrieval returns too few results, supplement with English
        if len(matches) < 3 and language != "en":
            en_matches = await self._vector_repo.similarity_search(
                vector=query_vector,
                top_k=top_k,
                language_filter="en",
            )
            # Merge, deduplicate by chunk_id, keep top_k
            seen = {m.chunk_id for m in matches}
            for m in en_matches:
                if m.chunk_id not in seen:
                    matches.append(m)
                    seen.add(m.chunk_id)
            matches = sorted(matches, key=lambda m: m.similarity_score, reverse=True)[:top_k]

        # 3. Determine confidence from top similarity score
        max_score = matches[0].similarity_score if matches else 0.0
        confidence, escalate, escalation_reason = self._score_confidence(max_score, matches)

        # 4. Generate LLM answer from retrieved context
        context_chunks = [f"[Source: {m.filename}, chunk {m.chunk_index}]\n{m.text}" for m in matches]
        if matches:
            answer_text = await self._llm_repo.complete(question, context_chunks)
        else:
            answer_text = "I don't have enough information in my knowledge base to answer this question reliably. Please contact the Global-PPS support team."

        # 5. If LLM itself signals uncertainty, escalate regardless of score
        if _NO_INFO_PHRASE.lower() in answer_text.lower():
            escalate = True
            confidence = ConfidenceLevel.LOW
            escalation_reason = "LLM indicated insufficient context"

        logger.info(
            "Query answered: confidence=%s escalate=%s top_score=%.3f",
            confidence, escalate, max_score,
        )

        sources = [
            SourceReference(
                doc_id=m.doc_id,
                filename=m.filename,
                chunk_index=m.chunk_index,
                similarity_score=round(m.similarity_score, 4),
            )
            for m in matches
        ]

        return AnswerResponse(
            answer=answer_text,
            confidence=confidence,
            sources=sources,
            escalate=escalate,
            escalation_reason=escalation_reason,
            language=language,
        )

    def _score_confidence(
        self,
        max_score: float,
        matches: list[VectorMatch],
    ) -> tuple[ConfidenceLevel, bool, str | None]:
        """Compute confidence level, escalation flag, and reason."""
        threshold = self._settings.confidence_threshold

        if not matches:
            return ConfidenceLevel.LOW, True, "No relevant documents found in knowledge base"

        if max_score >= threshold + 0.10:
            return ConfidenceLevel.HIGH, False, None
        elif max_score >= threshold:
            return ConfidenceLevel.MEDIUM, False, None
        else:
            return (
                ConfidenceLevel.LOW,
                True,
                f"Best match similarity ({max_score:.2f}) below threshold ({threshold})",
            )
