"""CLI: `python main.py` or `python -m qbo_pipeline.etl.run`. Airflow: `run_sync` in `etl.pipeline`."""

from __future__ import annotations

import argparse
import sys

from qbo_pipeline.config import Settings
from qbo_pipeline.etl.pipeline import run_sync as run_pipeline
from qbo_pipeline.observability import configure_logging, get_logger

logger = get_logger(__name__)


def main(argv: list[str] | None = None) -> int:
    configure_logging(service="qbo_pipeline_etl")
    parser = argparse.ArgumentParser(
        description="Fetch QuickBooks JSON from n8n (or a local file) and load into Supabase.",
    )
    parser.add_argument(
        "--local-file",
        type=str,
        default=None,
        help="Skip HTTP and read this JSON file (same shape as the webhook).",
    )
    args = parser.parse_args(argv)

    try:
        settings = Settings.from_env()
    except RuntimeError as exc:
        logger.error("etl_cli_config_error", error=str(exc))
        print(f"Config error: {exc}", file=sys.stderr)
        return 1

    try:
        logger.info("etl_cli_sync_requested", local_file=args.local_file)
        sync_id = run_pipeline(settings, local_path=args.local_file)
    except Exception as exc:
        logger.exception("etl_cli_sync_failed", error=str(exc))
        print(f"Sync failed: {exc}", file=sys.stderr)
        return 2

    logger.info("etl_cli_sync_completed", sync_run_id=sync_id)
    print(f"sync_runs.id={sync_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
