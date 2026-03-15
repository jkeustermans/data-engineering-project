# Architecture Reference

## Layer Responsibilities

### API Layer (`app/api/routes/`)
- Parse and validate HTTP requests using Pydantic schemas
- Call exactly one service method per endpoint
- Return HTTP responses; translate domain exceptions to HTTPException
- No business logic here

### Service Layer (`app/services/`)
- Orchestrate multi-step operations (e.g. embed query → retrieve → generate)
- Own all business rules (confidence thresholds, escalation logic, deduplication)
- Stateless; receive dependencies via constructor injection
- Raise domain exceptions (never HTTPException)

### Repository Layer (`app/repositories/`)
- All Pinecone vector operations
- All document metadata persistence (SQLite for prototype)
- Wrap external SDK calls; translate SDK errors to domain exceptions
- `VectorRepository` — upsert, query, delete vectors
- `DocumentRepository` — CRUD for document metadata records

### Core Layer (`app/core/`)
- `config.py` — Pydantic Settings, loads from env
- `dependencies.py` — FastAPI dependency providers (get_vector_repo, get_doc_service, …)
- `clients.py` — singleton OpenAI + Pinecone client construction
- `exceptions.py` — domain exception hierarchy

## Data Flow: Query
```
POST /api/v1/chat/query
  → ChatRouter
    → ChatService.answer(question, language)
      → EmbeddingRepository.embed(question)         # OpenAI embeddings
      → VectorRepository.similarity_search(vector)  # Pinecone query
      → ChatService._build_prompt(chunks, question)
      → LLMRepository.complete(prompt)              # OpenAI chat completion
      → ChatService._score_confidence(response)
      → return AnswerResponse(answer, sources, confidence, escalate)
```

## Data Flow: Document Upload
```
POST /api/v1/documents/upload
  → DocumentRouter
    → DocumentService.ingest(file, metadata)
      → DocumentRepository.save_metadata(doc)       # SQLite
      → TextExtractor.extract(file)                 # pypdf2 / plain text
      → Chunker.chunk(text)                         # LangChain splitter
      → EmbeddingRepository.embed_batch(chunks)     # OpenAI batch embed
      → VectorRepository.upsert_batch(vectors)      # Pinecone upsert
      → return IngestResponse(doc_id, chunk_count)
```

## Confidence & Escalation
- Cosine similarity of top-1 retrieved chunk stored per query
- If max_similarity < 0.72 → confidence = "low" → escalate = true
- Prompt instructs LLM to reply "I don't have enough information" if context insufficient
- escalate=true triggers email notification (stub in prototype, real in production)
