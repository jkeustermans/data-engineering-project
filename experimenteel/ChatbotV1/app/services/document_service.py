"""Document ingestion service — orchestrates parsing, chunking, embedding, storing."""

import logging
import uuid
from datetime import datetime
from pathlib import Path

from langchain_text_splitters import RecursiveCharacterTextSplitter
import pypdf

from app.models.document import Document, DocumentChunk, DocumentStatus
from app.repositories.document_repository import DocumentRepository
from app.repositories.vector_repository import VectorRepository
from app.repositories.embedding_repository import EmbeddingRepository
from app.core.config import Settings
from app.core.exceptions import IngestError, UnsupportedFileTypeError, DocumentNotFoundError

logger = logging.getLogger(__name__)


class DocumentService:
    """Handles document lifecycle: ingest, update (new version), delete, list."""

    def __init__(
        self,
        doc_repo: DocumentRepository,
        vector_repo: VectorRepository,
        embedding_repo: EmbeddingRepository,
        settings: Settings,
    ) -> None:
        self._doc_repo = doc_repo
        self._vector_repo = vector_repo
        self._embedding_repo = embedding_repo
        self._settings = settings
        self._splitter = RecursiveCharacterTextSplitter(
            chunk_size=settings.chunk_size,
            chunk_overlap=settings.chunk_overlap,
            separators=["\n\n", "\n", ". ", " ", ""],
        )

    async def ingest(
        self,
        file_bytes: bytes,
        filename: str,
        title: str,
        language: str,
        doc_type: str,
    ) -> Document:
        """Ingest a new document. Returns the saved Document with chunk_count."""
        ext = Path(filename).suffix.lower()
        if ext not in self._settings.allowed_extensions:
            raise UnsupportedFileTypeError(
                f"Extension '{ext}' not allowed. Allowed: {self._settings.allowed_extensions}"
            )

        doc = Document(
            id=str(uuid.uuid4()),
            filename=filename,
            title=title,
            language=language,
            doc_type=doc_type,
        )
        await self._doc_repo.save(doc)

        try:
            text = self._extract_text(file_bytes, ext)
            chunks = self._chunk(doc, text)
            chunk_count = await self._embed_and_upsert(chunks)

            doc.chunk_count = chunk_count
            doc.status = DocumentStatus.INGESTED
            doc.updated_at = datetime.utcnow()
            await self._doc_repo.save(doc)
            logger.info("Ingested document %s (%d chunks)", doc.id, chunk_count)
            return doc

        except Exception as exc:
            doc.status = DocumentStatus.FAILED
            await self._doc_repo.save(doc)
            raise IngestError(f"Ingestion failed for '{filename}': {exc}") from exc

    async def update(
        self,
        doc_id: str,
        file_bytes: bytes,
        filename: str,
        title: str | None = None,
        language: str | None = None,
        doc_type: str | None = None,
    ) -> Document:
        """Replace document content with a new version. Deletes old vectors first."""
        existing = await self._doc_repo.get_by_id(doc_id)
        deleted = await self._vector_repo.delete_by_doc_id(doc_id)
        logger.info("Removed %d old vectors for doc_id=%s", deleted, doc_id)

        ext = Path(filename).suffix.lower()
        if ext not in self._settings.allowed_extensions:
            raise UnsupportedFileTypeError(f"Extension '{ext}' not allowed.")

        existing.filename = filename
        existing.title = title or existing.title
        existing.language = language or existing.language
        existing.doc_type = doc_type or existing.doc_type
        existing.version += 1
        existing.status = DocumentStatus.PENDING
        existing.updated_at = datetime.utcnow()
        await self._doc_repo.save(existing)

        try:
            text = self._extract_text(file_bytes, ext)
            chunks = self._chunk(existing, text)
            chunk_count = await self._embed_and_upsert(chunks)

            existing.chunk_count = chunk_count
            existing.status = DocumentStatus.INGESTED
            existing.updated_at = datetime.utcnow()
            await self._doc_repo.save(existing)
            logger.info("Updated document %s to version %d", doc_id, existing.version)
            return existing
        except Exception as exc:
            existing.status = DocumentStatus.FAILED
            await self._doc_repo.save(existing)
            raise IngestError(f"Update failed for doc_id '{doc_id}': {exc}") from exc

    async def delete(self, doc_id: str) -> int:
        """Delete document metadata and all associated vectors. Returns deleted chunk count."""
        await self._doc_repo.get_by_id(doc_id)  # raises if not found
        deleted = await self._vector_repo.delete_by_doc_id(doc_id)
        await self._doc_repo.delete(doc_id)
        logger.info("Deleted document %s (%d chunks removed)", doc_id, deleted)
        return deleted

    async def list_documents(self) -> list[Document]:
        """Return all ingested documents."""
        return await self._doc_repo.list_all()

    async def get_document(self, doc_id: str) -> Document:
        """Fetch a single document by ID."""
        return await self._doc_repo.get_by_id(doc_id)

    # ── Private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _extract_text(file_bytes: bytes, ext: str) -> str:
        """Extract plain text from file bytes."""
        if ext == ".pdf":
            import io
            reader = pypdf.PdfReader(io.BytesIO(file_bytes))
            return "\n\n".join(
                page.extract_text() or "" for page in reader.pages
            ).strip()
        elif ext in (".txt", ".md"):
            return file_bytes.decode("utf-8", errors="replace").strip()
        raise UnsupportedFileTypeError(f"No extractor for extension '{ext}'")

    def _chunk(self, doc: Document, text: str) -> list[DocumentChunk]:
        """Split text into overlapping chunks."""
        raw_chunks = self._splitter.split_text(text)
        return [
            DocumentChunk(
                chunk_id=f"{doc.id}::chunk::{i}",
                doc_id=doc.id,
                text=chunk,
                chunk_index=i,
                language=doc.language,
                doc_type=doc.doc_type,
            )
            for i, chunk in enumerate(raw_chunks)
        ]

    async def _embed_and_upsert(self, chunks: list[DocumentChunk]) -> int:
        """Embed all chunks and upsert to Pinecone. Returns count."""
        if not chunks:
            return 0
        texts = [c.text for c in chunks]
        vectors = await self._embedding_repo.embed_batch(texts)
        metadatas = [
            {
                "doc_id": c.doc_id,
                "filename": chunks[0].chunk_id.split("::")[0],  # reuse doc_id
                "text": c.text,
                "chunk_index": c.chunk_index,
                "language": c.language,
                "doc_type": c.doc_type,
            }
            for c in chunks
        ]
        # Fix filename in metadata using doc info from first chunk
        for i, c in enumerate(chunks):
            metadatas[i]["filename"] = c.chunk_id.rsplit("::", 2)[0]  # doc_id used as key

        return await self._vector_repo.upsert_batch(
            chunk_ids=[c.chunk_id for c in chunks],
            vectors=vectors,
            metadatas=metadatas,
        )
