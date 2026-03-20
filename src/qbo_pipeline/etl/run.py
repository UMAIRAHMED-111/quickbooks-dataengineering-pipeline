"""CLI: `python main.py` or `python -m qbo_pipeline.etl.run`. Airflow: `run_sync` in `etl.pipeline`."""

from __future__ import annotations

import argparse
import sys

from qbo_pipeline.config import Settings
from qbo_pipeline.etl.pipeline import run_sync as run_pipeline


def main(argv: list[str] | None = None) -> int:
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
        print(f"Config error: {exc}", file=sys.stderr)
        return 1

    try:
        sync_id = run_pipeline(settings, local_path=args.local_file)
    except Exception as exc:
        print(f"Sync failed: {exc}", file=sys.stderr)
        return 2

    print(f"sync_runs.id={sync_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
