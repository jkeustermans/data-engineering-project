"""Pydantic v2 request/response schemas for the Chat API."""

from pydantic import BaseModel, Field
from enum import Enum


class ConfidenceLevel(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class SourceReference(BaseModel):
    doc_id: str
    filename: str
    chunk_index: int
    similarity_score: float = Field(ge=0.0, le=1.0)


class QueryRequest(BaseModel):
    question: str = Field(min_length=3, max_length=1000)
    language: str = Field(default="en", pattern=r"^[a-z]{2}$")
    top_k: int = Field(default=5, ge=1, le=10)


class AnswerResponse(BaseModel):
    answer: str
    confidence: ConfidenceLevel
    sources: list[SourceReference]
    escalate: bool
    escalation_reason: str | None = None
    language: str
