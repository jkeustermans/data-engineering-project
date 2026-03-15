"""Pydantic v2 request/response schemas for the Documents API."""

from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class DocType(str, Enum):
    PROTOCOL = "protocol"
    FAQ = "faq"
    FORM = "form"
    ARTICLE = "article"
    OTHER = "other"


class IngestResponse(BaseModel):
    doc_id: str
    filename: str
    chunk_count: int
    status: str
    message: str


class DocumentMetaResponse(BaseModel):
    doc_id: str
    filename: str
    title: str
    language: str
    doc_type: DocType
    version: int
    status: str
    chunk_count: int
    created_at: datetime
    updated_at: datetime


class DocumentListResponse(BaseModel):
    total: int
    documents: list[DocumentMetaResponse]


class DeleteResponse(BaseModel):
    doc_id: str
    deleted_chunks: int
    message: str
