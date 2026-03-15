---
globs: tests/**/*.py
---

# Testing Rules

- Mock ALL external calls: OpenAI, Pinecone — use `unittest.mock.AsyncMock` or `pytest-mock`
- Test file naming: `test_<module_name>.py` matching the source path
- Each test function name describes the scenario: `test_answer_returns_escalate_when_confidence_low`
- Use fixtures from `conftest.py` — don't duplicate setup code
- Integration tests go in `tests/integration/` and are skipped unless `--run-integration` flag passed
- Assert on response shape AND business logic (e.g. escalate flag, source count)
