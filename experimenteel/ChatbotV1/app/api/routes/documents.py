"""Documents API router — upload, update, list, get, delete."""

import logging
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form

from app.schemas.document_schemas import (
    IngestResponse, DocumentMetaResponse, DocumentListResponse, DeleteResponse, DocType
)
from app.services.document_service import DocumentService
from app.core.dependencies import get_document_service
from app.core.exceptions import (
    DocumentNotFoundError, IngestError, UnsupportedFileTypeError
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["Documents"])


@router.post("/upload", response_model=IngestResponse, status_code=201, summary="Upload a new document")
async def upload_document(
    file: UploadFile = File(..., description="PDF, TXT, or MD file"),
    title: str = Form(..., description="Human-readable title"),
    language: str = Form(default="en", description="ISO 639-1 language code"),
    doc_type: DocType = Form(default=DocType.OTHER),
    document_service: DocumentService = Depends(get_document_service),
) -> IngestResponse:
    """Upload and ingest a new document into the knowledge base."""
    file_bytes = await file.read()
    try:
        doc = await document_service.ingest(
            file_bytes=file_bytes,
            filename=file.filename or "upload",
            title=title,
            language=language,
            doc_type=doc_type.value,
        )
        return IngestResponse(
            doc_id=doc.id,
            filename=doc.filename,
            chunk_count=doc.chunk_count,
            status=doc.status.value,
            message=f"Document ingested successfully ({doc.chunk_count} chunks created).",
        )
    except UnsupportedFileTypeError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except IngestError as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.put("/{doc_id}", response_model=IngestResponse, summary="Update document (new version)")
async def update_document(
    doc_id: str,
    file: UploadFile = File(...),
    title: str | None = Form(default=None),
    language: str | None = Form(default=None),
    doc_type: DocType | None = Form(default=None),
    document_service: DocumentService = Depends(get_document_service),
) -> IngestResponse:
    """Replace an existing document with a new version. Old vectors are removed first."""
    file_bytes = await file.read()
    try:
        doc = await document_service.update(
            doc_id=doc_id,
            file_bytes=file_bytes,
            filename=file.filename or "upload",
            title=title,
            language=language,
            doc_type=doc_type.value if doc_type else None,
        )
        return IngestResponse(
            doc_id=doc.id,
            filename=doc.filename,
            chunk_count=doc.chunk_count,
            status=doc.status.value,
            message=f"Document updated to version {doc.version} ({doc.chunk_count} chunks).",
        )
    except DocumentNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except UnsupportedFileTypeError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except IngestError as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/", response_model=DocumentListResponse, summary="List all documents")
async def list_documents(
    document_service: DocumentService = Depends(get_document_service),
) -> DocumentListResponse:
    """Return metadata for all ingested documents."""
    docs = await document_service.list_documents()
    return DocumentListResponse(
        total=len(docs),
        documents=[_doc_to_response(d) for d in docs],
    )


@router.get("/{doc_id}", response_model=DocumentMetaResponse, summary="Get document metadata")
async def get_document(
    doc_id: str,
    document_service: DocumentService = Depends(get_document_service),
) -> DocumentMetaResponse:
    """Fetch metadata for a single document."""
    try:
        doc = await document_service.get_document(doc_id)
        return _doc_to_response(doc)
    except DocumentNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.delete("/{doc_id}", response_model=DeleteResponse, summary="Delete a document")
async def delete_document(
    doc_id: str,
    document_service: DocumentService = Depends(get_document_service),
) -> DeleteResponse:
    """Remove a document and all its vectors from the knowledge base."""
    try:
        deleted_chunks = await document_service.delete(doc_id)
        return DeleteResponse(
            doc_id=doc_id,
            deleted_chunks=deleted_chunks,
            message=f"Document and {deleted_chunks} chunks deleted.",
        )
    except DocumentNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


def _doc_to_response(doc) -> DocumentMetaResponse:
    from app.schemas.document_schemas import DocType as DT
    return DocumentMetaResponse(
        doc_id=doc.id,
        filename=doc.filename,
        title=doc.title,
        language=doc.language,
        doc_type=DT(doc.doc_type),
        version=doc.version,
        status=doc.status.value,
        chunk_count=doc.chunk_count,
        created_at=doc.created_at,
        updated_at=doc.updated_at,
    )
