"""Get the QuickBooks-shaped JSON: either from the n8n webhook or a saved file."""

import json
from pathlib import Path

import httpx

from qbo_pipeline.config import Settings


def fetch_from_webhook(settings: Settings) -> dict:
    with httpx.Client(timeout=settings.n8n_http_timeout_seconds) as client:
        response = client.get(settings.n8n_webhook_url)
        response.raise_for_status()
    return response.json()


def load_local_json(path: str | Path) -> dict:
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"No file at {p.resolve()}")
    return json.loads(p.read_text(encoding="utf-8"))


def extract(settings: Settings, *, local_path: str | Path | None = None) -> dict:
    if local_path is not None:
        return load_local_json(local_path)
    return fetch_from_webhook(settings)
