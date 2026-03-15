"""Domain exception hierarchy.

Services raise these; the API layer translates them to HTTPException.
Never use HTTPException outside the API layer.
"""


class GPPSBaseError(Exception):
    """Base for all domain errors."""


class DocumentNotFoundError(GPPSBaseError):
    """Raised when a document ID does not exist."""


class IngestError(GPPSBaseError):
    """Raised when document ingestion fails (parsing, embedding, upsert)."""


class UnsupportedFileTypeError(IngestError):
    """Raised when uploaded file extension is not allowed."""


class EmbeddingError(GPPSBaseError):
    """Raised when the embedding API call fails."""


class RetrievalError(GPPSBaseError):
    """Raised when the vector store query fails."""


class LLMError(GPPSBaseError):
    """Raised when the LLM completion call fails."""
