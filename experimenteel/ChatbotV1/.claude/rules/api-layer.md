---
globs: app/api/**/*.py
---

# API Layer Rules

- Import only from `app.schemas`, `app.services`, `app.core.dependencies`, `app.core.exceptions`
- NEVER import from `app.repositories` directly in a route
- All route functions must be `async def`
- Use `Depends()` for all service injection — never instantiate services manually
- Translate `DocumentNotFoundError` → HTTP 404, `IngestError` → HTTP 422, all others → HTTP 500
- Every router must have a prefix and tags defined
