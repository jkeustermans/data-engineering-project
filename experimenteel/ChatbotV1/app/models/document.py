"""Domain models for documents stored in the metadata store."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class DocumentStatus(str, Enum):
    PENDING = "pending"
    INGESTED = "ingested"
    FAILED = "failed"


@dataclass
class Document:
    """Represents a source document ingested into the knowledge base."""

    id: str                          # UUID
    filename: str
    title: str
    language: str                    # ISO 639-1: en, fr, es, …
    doc_type: str                    # protocol | faq | form | article | other
    version: int = 1
    status: DocumentStatus = DocumentStatus.PENDING
    chunk_count: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DocumentChunk:
    """A single text chunk ready for embedding."""

    chunk_id: str                    # f"{doc_id}::chunk::{index}"
    doc_id: str
    text: str
    chunk_index: int
    language: str
    doc_type: str
