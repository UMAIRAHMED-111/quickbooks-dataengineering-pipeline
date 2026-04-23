#!/usr/bin/env python3
"""
Run the pipeline without `pip install -e .` — adds ./src and loads repo `.env`.

Still need deps: pip install -r requirements.txt

Usage:
  python main.py
  python main.py --local-file data/response.json
"""
from __future__ import annotations

from pathlib import Path

from repo_bootstrap import configure_for_checkout

configure_for_checkout(Path(__file__))

from qbo_pipeline.observability import configure_logging  # noqa: E402
from qbo_pipeline.etl.run import main  # noqa: E402

configure_logging(service="qbo_pipeline_cli")

if __name__ == "__main__":
    raise SystemExit(main())
