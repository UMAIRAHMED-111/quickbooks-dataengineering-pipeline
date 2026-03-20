#!/usr/bin/env python3
"""Ask the warehouse a question in English (OpenAI gpt-4 first, Gemini fallback). Same as: python -m qbo_pipeline.qa.warehouse_qa \"...\""""
from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent / "src"
sys.path.insert(0, str(_SRC))

from qbo_pipeline.qa.warehouse_qa import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
