"""Get the QuickBooks-shaped JSON: either from the n8n webhook or a saved file."""

import json
import tempfile
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


def fetch_webhook_to_tempfile(settings: Settings) -> str:
    """GET n8n webhook JSON and write it to a temp file. Returns path for XCom-sized handoff."""
    payload = fetch_from_webhook(settings)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix="qbo_n8n_",
        suffix=".json",
        delete=False,
    ) as tmp:
        path = str(Path(tmp.name))
        try:
            json.dump(payload, tmp)
        except Exception:
            Path(path).unlink(missing_ok=True)
            raise
        return path


def extract(settings: Settings, *, local_path: str | Path | None = None) -> dict:
    if local_path is not None:
        return load_local_json(local_path)
    return fetch_from_webhook(settings)
