"""Retry Gemini generate_content on 429 with server hint + exponential backoff."""

from __future__ import annotations

import os
import random
import re
import time
from typing import Any

from google import genai
from google.genai import types
from google.genai.errors import APIError


def _max_retries_from_env() -> int:
    raw = os.getenv("GEMINI_MAX_RETRIES", "3").strip()
    try:
        n = int(raw)
    except ValueError:
        return 3
    return max(0, min(n, 10))


def _base_delay_seconds() -> float:
    raw = os.getenv("GEMINI_RETRY_BASE_SECONDS", "1").strip()
    try:
        return max(0.1, float(raw))
    except ValueError:
        return 1.0


def _max_sleep_seconds() -> float:
    raw = os.getenv("GEMINI_RETRY_MAX_SLEEP_SECONDS", "120").strip()
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 120.0


def retry_delay_hint_seconds(exc: APIError) -> float | None:
    """Parse suggested wait from Google 429 payload / message, if present."""
    if exc.code != 429:
        return None
    blob = f"{exc.message or ''} {exc}"
    m = re.search(r"retry in ([\d.]+)\s*s", blob, re.I)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    m2 = re.search(r"retryDelay['\"]:\s*['\"](\d+)s['\"]", blob, re.I)
    if m2:
        return float(m2.group(1))
    return None


def generate_content_with_retry(
    client: genai.Client,
    *,
    model: str,
    contents: str | list[Any] | Any,
    config: types.GenerateContentConfig | None = None,
    max_retries: int | None = None,
) -> Any:
    """
    Call client.models.generate_content; on 429 RESOURCE_EXHAUSTED retry with backoff.

    max_retries: extra attempts after the first failure (default from GEMINI_MAX_RETRIES, else 3).
    Total attempts = 1 + max_retries.
    Sleep = max(exponential base * 2**attempt, server hint) capped by GEMINI_RETRY_MAX_SLEEP_SECONDS.
    """
    limit = _max_retries_from_env() if max_retries is None else max(0, min(int(max_retries), 10))
    base = _base_delay_seconds()
    cap = _max_sleep_seconds()
    attempts = limit + 1
    last: APIError | None = None

    for attempt in range(attempts):
        try:
            return client.models.generate_content(
                model=model,
                contents=contents,
                config=config,
            )
        except APIError as exc:
            last = exc
            if exc.code != 429 or attempt >= attempts - 1:
                raise
            hint = retry_delay_hint_seconds(exc)
            backoff = base * (2**attempt)
            wait = max(backoff, hint or 0.0)
            wait = min(cap, wait + random.uniform(0, 0.35))
            time.sleep(wait)

    assert last is not None
    raise last
