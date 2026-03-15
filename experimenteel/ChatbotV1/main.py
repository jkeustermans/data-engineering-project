"""Global-PPS Support Chatbot — FastAPI application."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import chat, documents, health
from app.core.config import get_settings
from app.repositories.document_repository import DocumentRepository

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise resources on startup; clean up on shutdown."""
    logger.info("Starting Global-PPS Chatbot API (env=%s)", get_settings().environment)
    repo = DocumentRepository()
    await repo.init_schema()
    logger.info("Database schema ready")
    yield
    logger.info("Shutting down")


app = FastAPI(
    title="Global-PPS Support Chatbot API",
    description=(
        "RAG-based REST API for answering Global-PPS user questions. "
        "Supports document ingestion, versioning, and multilingual Q&A."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/api/v1")
app.include_router(documents.router, prefix="/api/v1")
app.include_router(chat.router, prefix="/api/v1")
