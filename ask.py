#!/usr/bin/env python3
"""Ask the warehouse a question in English (OpenAI gpt-4 first, Gemini fallback). Same as: python -m qbo_pipeline.qa.warehouse_qa \"...\""""
from __future__ import annotations

import sys
from pathlib import Path

from repo_bootstrap import configure_for_checkout

configure_for_checkout(Path(__file__))

from qbo_pipeline.observability import configure_logging  # noqa: E402
from qbo_pipeline.qa.warehouse_qa import main  # noqa: E402

configure_logging(service="qbo_pipeline_qa_cli")

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
