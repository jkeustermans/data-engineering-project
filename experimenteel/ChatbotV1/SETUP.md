# Global-PPS Chatbot — Setup Guide

Complete step-by-step instructions for getting the prototype running.

---

## Prerequisites

- Python 3.11+
- An OpenAI account with API access
- A Pinecone account (free tier is sufficient for prototype)
- Claude Code installed (`npm install -g @anthropic-ai/claude-code`)

---

## Step 1 — Clone / open project in Claude Code

```bash
cd gpps-chatbot
claude   # opens Claude Code in this directory
```

Claude Code will automatically read `CLAUDE.md` and load project context.

---

## Step 2 — Create a Python virtual environment

```bash
python -m venv .venv
source .venv/bin/activate        # macOS/Linux
# or: .venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

---

## Step 3 — Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in:

| Variable | Where to get it |
|---|---|
| `OPENAI_API_KEY` | https://platform.openai.com/api-keys |
| `PINECONE_API_KEY` | https://app.pinecone.io → API Keys |
| `PINECONE_INDEX_NAME` | See Step 4 |

---

## Step 4 — Create the Pinecone index

1. Log in to https://app.pinecone.io
2. Create a new Index:
   - **Name**: `gpps-docs` (or whatever you set in `.env`)
   - **Dimensions**: `3072`  ← must match `text-embedding-3-large` output
   - **Metric**: `cosine`
   - **Cloud**: AWS / us-east-1 (free tier)
3. Wait for the index to become ready (green status)

---

## Step 5 — Start the API server

```bash
make dev
# or: uvicorn main:app --reload --port 8000
```

You should see:
```
INFO: Starting Global-PPS Chatbot API (env=development)
INFO: Database schema ready
INFO: Uvicorn running on http://127.0.0.1:8000
```

Open the interactive API docs: **http://localhost:8000/docs**

---

## Step 6 — Ingest your first documents

### Option A: Upload via Swagger UI
1. Go to http://localhost:8000/docs
2. Open `POST /api/v1/documents/upload`
3. Upload a PDF (e.g. the Global-PPS inpatient protocol)
4. Set `language=en`, `doc_type=protocol`

### Option B: Upload via curl
```bash
curl -X POST http://localhost:8000/api/v1/documents/upload \
  -F "file=@path/to/inpatient-protocol.pdf" \
  -F "title=Global-PPS Inpatient Protocol 2024" \
  -F "language=en" \
  -F "doc_type=protocol"
```

### Option C: Crawl the entire global-pps.com website
```bash
# Make sure the server is running first (Step 5)
pip install beautifulsoup4   # extra dependency for crawler
make ingest
# or: python scripts/ingest_website.py
```
This will crawl all pages and PDFs from global-pps.com and ingest them.
Estimated time: 5–15 minutes depending on site size.

---

## Step 7 — Ask your first question

```bash
curl -X POST http://localhost:8000/api/v1/chat/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What patients should be included in the inpatient survey?", "language": "en"}'
```

Expected response shape:
```json
{
  "answer": "According to the Global-PPS inpatient protocol...",
  "confidence": "high",
  "sources": [{"doc_id": "...", "filename": "inpatient-protocol.pdf", ...}],
  "escalate": false,
  "language": "en"
}
```

If `escalate: true` is returned, the chatbot was not confident — the question should be routed to a domain expert.

---

## Step 8 — Run the tests

```bash
make test
```

All tests mock OpenAI and Pinecone — no API keys needed to run the test suite.

---

## Step 9 — Update a document (new version)

When a new version of a protocol is published:

```bash
curl -X PUT http://localhost:8000/api/v1/documents/{doc_id} \
  -F "file=@path/to/new-protocol-2025.pdf" \
  -F "title=Global-PPS Inpatient Protocol 2025"
```

This will:
1. Delete all old vectors for this document from Pinecone
2. Parse and chunk the new file
3. Embed and upsert new vectors
4. Increment the `version` counter

---

## API Endpoints Summary

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/health/` | Health check |
| `POST` | `/api/v1/chat/query` | Ask a question (RAG pipeline) |
| `POST` | `/api/v1/documents/upload` | Upload new document |
| `PUT` | `/api/v1/documents/{id}` | Update document (new version) |
| `GET` | `/api/v1/documents/` | List all documents |
| `GET` | `/api/v1/documents/{id}` | Get document metadata |
| `DELETE` | `/api/v1/documents/{id}` | Delete document + vectors |

Full interactive docs: http://localhost:8000/docs

---

## Using Claude Code for development

With Claude Code open in the project directory, you can ask it to:

- `"Add an endpoint to search documents by language"`
- `"Write a test for the update endpoint"`
- `"Add rate limiting to the chat endpoint"`
- `"Explain the RAG pipeline in chat_service.py"`

Claude Code reads `CLAUDE.md` and the `.claude/rules/` files automatically,
so it understands the layered architecture and will respect it.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `EmbeddingError` on upload | Check `OPENAI_API_KEY` in `.env` |
| `RetrievalError` on query | Check `PINECONE_API_KEY` and index name; verify index is ready |
| Empty answers | Run ingestion first (Step 6); check index has vectors in Pinecone console |
| `escalate: true` always | Normal for questions not covered in knowledge base; ingest more documents |
| Dimension mismatch in Pinecone | Index must be created with dimensions=3072 for `text-embedding-3-large` |
