"""Get the QuickBooks-shaped JSON: either from the n8n webhook or a saved file."""

import json
import tempfile
import time
from pathlib import Path

import httpx

from qbo_pipeline.config import Settings
from qbo_pipeline.observability import get_logger

_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
_MAX_WEBHOOK_ATTEMPTS = 5
_INITIAL_BACKOFF_SECONDS = 1.0
_MAX_BACKOFF_SECONDS = 30.0

logger = get_logger(__name__)


def _retry_delay_seconds(response: httpx.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                pass
    exponential = _INITIAL_BACKOFF_SECONDS * (2 ** max(0, attempt - 1))
    return min(exponential, _MAX_BACKOFF_SECONDS)


def fetch_from_webhook(settings: Settings) -> dict:
    logger.info("webhook_fetch_started", url=settings.n8n_webhook_url)
    auth: tuple[str, str] | None = None
    if settings.n8n_basic_auth_username and settings.n8n_basic_auth_password:
        auth = (settings.n8n_basic_auth_username, settings.n8n_basic_auth_password)
    with httpx.Client(timeout=settings.n8n_http_timeout_seconds) as client:
        for attempt in range(1, _MAX_WEBHOOK_ATTEMPTS + 1):
            try:
                response = client.get(settings.n8n_webhook_url, auth=auth)
                response.raise_for_status()
                logger.info("webhook_fetch_completed", attempt=attempt)
                return response.json()
            except httpx.HTTPStatusError as err:
                response = err.response
                if (
                    response.status_code not in _RETRYABLE_STATUS_CODES
                    or attempt == _MAX_WEBHOOK_ATTEMPTS
                ):
                    logger.warning(
                        "webhook_fetch_failed_non_retryable",
                        status=response.status_code,
                        attempt=attempt,
                    )
                    raise
                logger.warning(
                    "webhook_fetch_retrying_http",
                    status=response.status_code,
                    attempt=attempt,
                )
                time.sleep(_retry_delay_seconds(response, attempt))
            except httpx.RequestError:
                if attempt == _MAX_WEBHOOK_ATTEMPTS:
                    logger.warning("webhook_fetch_failed_request", attempt=attempt)
                    raise
                logger.warning("webhook_fetch_retrying_request", attempt=attempt)
                time.sleep(_retry_delay_seconds(None, attempt))
    raise RuntimeError("Webhook fetch failed after retries")


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
        logger.info("webhook_payload_tempfile_created", path=path)
        return path


def extract(settings: Settings, *, local_path: str | Path | None = None) -> dict:
    if local_path is not None:
        return load_local_json(local_path)
    return fetch_from_webhook(settings)
