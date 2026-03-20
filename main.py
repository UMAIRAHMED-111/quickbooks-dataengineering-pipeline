#!/usr/bin/env python3
"""
Run the pipeline without `pip install -e .` — adds ./src to import path.

Still need deps: pip install -r requirements.txt

Usage:
  python main.py
  python main.py --local-file data/response.json
"""
from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent / "src"
sys.path.insert(0, str(_SRC)) 

from qbo_pipeline.etl.run import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
