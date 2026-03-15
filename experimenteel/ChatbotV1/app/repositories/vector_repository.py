import logging
import uuid
from dataclasses import dataclass
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, Filter, FieldCondition, MatchValue
from app.core.exceptions import RetrievalError

logger = logging.getLogger(__name__)


@dataclass
class VectorMatch:
    chunk_id: str
    doc_id: str
    filename: str
    text: str
    chunk_index: int
    similarity_score: float
    language: str
    doc_type: str


class VectorRepository:
    """Qdrant-backed vector store. No C++ build tools required."""

    def __init__(self, collection) -> None:
        self._client: QdrantClient = collection  # client passed in
        from app.core.config import get_settings
        self._collection = get_settings().chroma_collection_name

    async def upsert_batch(
        self,
        chunk_ids: list[str],
        vectors: list[list[float]],
        metadatas: list[dict],
    ) -> int:
        if not chunk_ids:
            return 0
        try:
            points = [
                PointStruct(
                    id=str(uuid.uuid5(uuid.NAMESPACE_DNS, cid)),  # Qdrant needs UUID or int
                    vector=vec,
                    payload={**meta, "chunk_id": cid},
                )
                for cid, vec, meta in zip(chunk_ids, vectors, metadatas)
            ]
            self._client.upsert(collection_name=self._collection, points=points)
            logger.info("Upserted %d vectors to Qdrant", len(points))
            return len(points)
        except Exception as exc:
            raise RetrievalError(f"Qdrant upsert failed: {exc}") from exc

    async def similarity_search(
        self,
        vector: list[float],
        top_k: int = 5,
        language_filter: str | None = None,
    ) -> list[VectorMatch]:
        try:
            query_filter = None
            if language_filter:
                query_filter = Filter(
                    must=[FieldCondition(
                        key="language",
                        match=MatchValue(value=language_filter)
                    )]
                )
            results = self._client.search(
                collection_name=self._collection,
                query_vector=vector,
                limit=top_k,
                query_filter=query_filter,
                with_payload=True,
            )
            matches = []
            for r in results:
                p = r.payload or {}
                matches.append(VectorMatch(
                    chunk_id=p.get("chunk_id", str(r.id)),
                    doc_id=p.get("doc_id", ""),
                    filename=p.get("filename", ""),
                    text=p.get("text", ""),
                    chunk_index=int(p.get("chunk_index", 0)),
                    similarity_score=round(float(r.score), 4),
                    language=p.get("language", "en"),
                    doc_type=p.get("doc_type", "other"),
                ))
            return matches
        except Exception as exc:
            raise RetrievalError(f"Qdrant query failed: {exc}") from exc

    async def delete_by_doc_id(self, doc_id: str) -> int:
        try:
            # Scroll to find all points with this doc_id
            results, _ = self._client.scroll(
                collection_name=self._collection,
                scroll_filter=Filter(
                    must=[FieldCondition(
                        key="doc_id",
                        match=MatchValue(value=doc_id)
                    )]
                ),
                limit=10000,
                with_payload=False,
            )
            ids = [r.id for r in results]
            if ids:
                self._client.delete(
                    collection_name=self._collection,
                    points_selector=ids,
                )
            logger.info("Deleted %d vectors for doc_id=%s", len(ids), doc_id)
            return len(ids)
        except Exception as exc:
            raise RetrievalError(f"Qdrant delete failed: {exc}") from exc