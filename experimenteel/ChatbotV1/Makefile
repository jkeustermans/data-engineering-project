.PHONY: dev test lint ingest

dev:
	uvicorn main:app --reload --port 8000

test:
	pytest tests/ -v --asyncio-mode=auto

test-coverage:
	pytest tests/ -v --asyncio-mode=auto --cov=app --cov-report=term-missing

lint:
	ruff check app/ tests/
	mypy app/

ingest:
	python scripts/ingest_website.py
