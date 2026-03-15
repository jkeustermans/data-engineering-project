"""Repository for LLM chat completions via OpenAI."""

import logging
from openai import AsyncOpenAI
from app.core.exceptions import LLMError

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a support assistant for the Global-PPS (Global Point Prevalence Survey) tool,
used by hospitals worldwide to collect data on antimicrobial use and healthcare-associated infections.

Your role:
- Answer questions using ONLY the context passages provided below.
- If the context does not contain enough information, say exactly:
  "I don't have enough information in my knowledge base to answer this question reliably."
- Never invent facts, drug names, or protocol details.
- Be concise and precise. Use bullet points for multi-step processes.
- Always cite which document/source your answer comes from.
- If the user writes in a language other than English, respond in the same language.
"""


class LLMRepository:
    """Wraps OpenAI chat completion calls."""

    def __init__(self, client: AsyncOpenAI, model: str, temperature: float) -> None:
        self._client = client
        self._model = model
        self._temperature = temperature

    async def complete(self, question: str, context_chunks: list[str]) -> str:
        """Generate an answer grounded in the provided context chunks."""
        context_text = "\n\n---\n\n".join(context_chunks)
        user_message = f"CONTEXT:\n{context_text}\n\nQUESTION:\n{question}"
        try:
            logger.info("Calling LLM model=%s question_len=%d", self._model, len(question))
            response = await self._client.chat.completions.create(
                model=self._model,
                temperature=self._temperature,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ],
            )
            answer = response.choices[0].message.content or ""
            logger.debug("LLM answer_len=%d", len(answer))
            return answer
        except Exception as exc:
            raise LLMError(f"LLM completion failed: {exc}") from exc
