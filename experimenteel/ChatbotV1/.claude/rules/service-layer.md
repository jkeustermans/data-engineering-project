---
globs: app/services/**/*.py
---

# Service Layer Rules

- Import only from `app.repositories`, `app.models`, `app.schemas`, `app.core`
- NEVER import from `app.api` or use `HTTPException`
- Raise only exceptions defined in `app.core.exceptions`
- All methods `async def`
- Receive repository instances via `__init__` (constructor injection), never create them inside methods
- No direct calls to OpenAI, Pinecone SDK — always go through a repository
