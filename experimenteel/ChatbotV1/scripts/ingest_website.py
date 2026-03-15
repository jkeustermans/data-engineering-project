"""
One-shot script to crawl global-pps.com and ingest all pages + PDFs into the knowledge base.
Run with: python scripts/ingest_website.py

This is a manual/scheduled script — NOT part of the API.
"""

import asyncio
import httpx
import logging
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.global-pps.com"
ALLOWED_LANGUAGES = ["en", "fr", "es", "ru", "ar", "ja"]
LANG_PATH_MAP = {"fr": "fr", "es": "es", "ru": "ru", "ar": "ar", "ja": "ja"}

# Detect language from URL path prefix
def detect_language(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    if parts and parts[0] in LANG_PATH_MAP.values():
        for lang, prefix in LANG_PATH_MAP.items():
            if parts[0] == prefix:
                return lang
    return "en"


async def crawl_and_ingest():
    """Crawl the site and POST each page/PDF to the document upload endpoint."""
    api_base = "http://localhost:8000/api/v1"
    visited = set()
    queue = [BASE_URL]

    async with httpx.AsyncClient(timeout=30) as http:
        while queue:
            url = queue.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = await http.get(url, follow_redirects=True)
                content_type = resp.headers.get("content-type", "")

                if "application/pdf" in content_type:
                    filename = url.split("/")[-1]
                    lang = detect_language(url)
                    logger.info("Ingesting PDF: %s (lang=%s)", filename, lang)
                    ingest_resp = await http.post(
                        f"{api_base}/documents/upload",
                        files={"file": (filename, resp.content, "application/pdf")},
                        data={"title": filename, "language": lang, "doc_type": "protocol"},
                    )
                    logger.info("  → %s", ingest_resp.json().get("message", ingest_resp.status_code))

                elif "text/html" in content_type:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    # Extract all links for continued crawling
                    for a in soup.find_all("a", href=True):
                        href = urljoin(url, a["href"])
                        if href.startswith(BASE_URL) and href not in visited:
                            queue.append(href)

                    # Extract page text and ingest
                    for tag in soup(["script", "style", "nav", "footer"]):
                        tag.decompose()
                    text = soup.get_text(separator="\n", strip=True)
                    if len(text) < 200:
                        continue  # skip near-empty pages

                    filename = urlparse(url).path.strip("/").replace("/", "_") + ".txt"
                    lang = detect_language(url)
                    logger.info("Ingesting page: %s (lang=%s)", url, lang)
                    ingest_resp = await http.post(
                        f"{api_base}/documents/upload",
                        files={"file": (filename, text.encode(), "text/plain")},
                        data={"title": soup.title.string if soup.title else filename,
                              "language": lang, "doc_type": "faq"},
                    )
                    logger.info("  → %s", ingest_resp.json().get("message", ingest_resp.status_code))

            except Exception as e:
                logger.warning("Error processing %s: %s", url, e)

    logger.info("Crawl complete. Visited %d URLs.", len(visited))


if __name__ == "__main__":
    asyncio.run(crawl_and_ingest())
