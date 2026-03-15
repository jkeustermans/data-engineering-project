# Global-PPS Support Chatbot — Project Memory

## Project Overview
RAG-based REST API chatbot for Global-PPS (global-pps.com). Answers user questions about
antimicrobial stewardship surveys using ingested documentation. Built with Python + FastAPI +
LangChain + OpenAI + Pinecone.

## Architecture: Strict Layered (never bypass layers)
```
API Layer       → app/api/routes/       FastAPI routers, request/response only
Service Layer   → app/services/         Business logic, orchestration
Repository Layer→ app/repositories/     All data access (Pinecone, disk)
Core Layer      → app/core/             Config, dependencies, LLM/embedding clients
Models          → app/models/           SQLAlchemy/Pydantic domain models
Schemas         → app/schemas/          Request/response DTOs
```
**Rule**: routes call services only. Services call repositories only. Repositories call external
APIs directly. Never skip a layer.

## Commands
- `make dev`       — start dev server (uvicorn, port 8000, reload)
- `make test`      — pytest with coverage
- `make ingest`    — run document ingestion pipeline
- `make lint`      — ruff + mypy

## Key Conventions
- All config via environment variables (never hardcode keys) — see `.env.example`
- Async throughout: use `async def` for all route handlers and service methods
- Every public function has a docstring
- Pydantic v2 schemas for all API I/O
- Log at INFO level for all ingestion/query events; DEBUG for retrieval details
- Raise `HTTPException` only in route layer; services raise domain exceptions

## Environment Variables (required)
- `OPENAI_API_KEY` — OpenAI API key
- `PINECONE_API_KEY` — Pinecone API key
- `PINECONE_INDEX_NAME` — Pinecone index name (default: gpps-docs)
- `ENVIRONMENT` — development | production

## Test Strategy
- Unit tests mock all external calls (OpenAI, Pinecone)
- Integration tests in `tests/integration/` require real API keys (skip in CI with `--no-integration`)
- Fixtures in `tests/conftest.py`

@docs/architecture.md
