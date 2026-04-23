"""Orchestration entrypoint — usable from CLI, Airflow, or other schedulers."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from qbo_pipeline.config import Settings
from qbo_pipeline.etl.extract import extract
from qbo_pipeline.etl.load import load, mark_sync_failed, run_delete_phase, run_insert_phase
from qbo_pipeline.etl.transform import transform
from qbo_pipeline.observability import get_logger

logger = get_logger(__name__)


def run_sync(
    settings: Settings,
    *,
    local_path: str | Path | None = None,
) -> str:
    """Fetch JSON, reshape for our tables, load in one DB transaction. Returns sync_runs.id."""
    logger.info("sync_extract_started")
    payload = extract(settings, local_path=local_path)
    logger.info("sync_extract_completed")
    logger.info("sync_transform_started")
    bundle = transform(payload)
    logger.info(
        "sync_transform_completed",
        customer_count=len(bundle.customers),
        invoice_count=len(bundle.invoices),
        payment_count=len(bundle.payments),
        allocation_count=len(bundle.payment_invoice_allocations),
    )
    logger.info("sync_load_started")
    sync_id = load(settings, bundle)
    logger.info("sync_load_completed", sync_run_id=sync_id)
    return sync_id


def start_sync_run(settings: Settings) -> str:
    """Create a sync run row immediately and return its id."""
    return run_delete_phase(settings)


def continue_sync_run(
    settings: Settings,
    sync_id: UUID | str,
    *,
    local_path: str | Path | None = None,
) -> str:
    """
    Continue a previously-started sync run.
    Intended for async workers so HTTP handlers can return quickly.
    """
    run_id = sync_id if isinstance(sync_id, UUID) else UUID(str(sync_id))
    logger.info("sync_worker_started", sync_run_id=run_id)
    try:
        logger.info("sync_extract_started", sync_run_id=run_id)
        payload = extract(settings, local_path=local_path)
        logger.info("sync_extract_completed", sync_run_id=run_id)
        logger.info("sync_transform_started", sync_run_id=run_id)
        bundle = transform(payload)
        logger.info(
            "sync_transform_completed",
            sync_run_id=run_id,
            customer_count=len(bundle.customers),
            invoice_count=len(bundle.invoices),
            payment_count=len(bundle.payments),
            allocation_count=len(bundle.payment_invoice_allocations),
        )
    except Exception as exc:
        mark_sync_failed(settings, run_id, str(exc))
        logger.exception("sync_preload_stage_failed", sync_run_id=run_id, error=str(exc))
        raise
    logger.info("sync_insert_started", sync_run_id=run_id)
    return run_insert_phase(settings, run_id, bundle)
