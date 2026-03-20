"""Orchestration entrypoint — usable from CLI, Airflow, or other schedulers."""

from __future__ import annotations

from pathlib import Path

from qbo_pipeline.config import Settings
from qbo_pipeline.etl.extract import extract
from qbo_pipeline.etl.load import load
from qbo_pipeline.etl.transform import transform


def run_sync(
    settings: Settings,
    *,
    local_path: str | Path | None = None,
) -> str:
    """Fetch JSON, reshape for our tables, load in one DB transaction. Returns sync_runs.id."""
    payload = extract(settings, local_path=local_path)
    bundle = transform(payload)
    sync_id = load(settings, bundle)
    return sync_id
