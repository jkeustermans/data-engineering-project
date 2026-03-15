"""Repository for document metadata persistence (SQLite via aiosqlite for prototype)."""

import logging
import aiosqlite
from pathlib import Path
from datetime import datetime

from app.models.document import Document, DocumentStatus
from app.core.exceptions import DocumentNotFoundError

logger = logging.getLogger(__name__)

DB_PATH = Path("data/gpps_docs.db")


class DocumentRepository:
    """SQLite-backed metadata store for ingested documents."""

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    async def init_schema(self) -> None:
        """Create tables if they don't exist (called at app startup)."""
        async with aiosqlite.connect(self._db_path) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    title TEXT NOT NULL,
                    language TEXT NOT NULL,
                    doc_type TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'pending',
                    chunk_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            await conn.commit()

    async def save(self, doc: Document) -> Document:
        """Insert or replace a document record."""
        async with aiosqlite.connect(self._db_path) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("""
                INSERT OR REPLACE INTO documents
                (id, filename, title, language, doc_type, version, status, chunk_count, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                doc.id, doc.filename, doc.title, doc.language, doc.doc_type,
                doc.version, doc.status.value, doc.chunk_count,
                doc.created_at.isoformat(), doc.updated_at.isoformat(),
            ))
            await conn.commit()
        logger.info("Saved document %s (status=%s)", doc.id, doc.status)
        return doc

    async def get_by_id(self, doc_id: str) -> Document:
        """Fetch a document by ID. Raises DocumentNotFoundError if missing."""
        async with aiosqlite.connect(self._db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT * FROM documents WHERE id = ?", (doc_id,)
            ) as cur:
                row = await cur.fetchone()
        if row is None:
            raise DocumentNotFoundError(f"Document {doc_id} not found")
        return self._row_to_doc(row)

    async def list_all(self) -> list[Document]:
        """Return all documents ordered by updated_at desc."""
        async with aiosqlite.connect(self._db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT * FROM documents ORDER BY updated_at DESC"
            ) as cur:
                rows = await cur.fetchall()
        return [self._row_to_doc(r) for r in rows]

    async def delete(self, doc_id: str) -> None:
        """Delete a document record. Raises DocumentNotFoundError if missing."""
        await self.get_by_id(doc_id)  # will raise if not found
        async with aiosqlite.connect(self._db_path) as conn:
            await conn.execute("DELETE FROM documents WHERE id = ?", (doc_id,))
            await conn.commit()

    @staticmethod
    def _row_to_doc(row) -> Document:
        return Document(
            id=row["id"],
            filename=row["filename"],
            title=row["title"],
            language=row["language"],
            doc_type=row["doc_type"],
            version=row["version"],
            status=DocumentStatus(row["status"]),
            chunk_count=row["chunk_count"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )