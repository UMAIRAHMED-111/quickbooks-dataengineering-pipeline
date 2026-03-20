#!/usr/bin/env python3
"""
Analytics API + sync trigger. From repo root:

  pip install -r requirements.txt
  python server.py

Or: flask --app server:app run --debug

POST /api/v1/sync — runs the same load as `python main.py` (requires env: DATABASE_URL, N8N_WEBHOOK_URL).
Optional: SYNC_API_SECRET — then send header X-Sync-Token or Authorization: Bearer …

POST /api/v1/qa — JSON {"question": "..."}; natural-language Q&A (same as `python ask.py`).
Requires DATABASE_URL plus OPENAI_API_KEY_1/2 and/or GEMINI_API_KEY (or GOOGLE_API_KEY). See README.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_SRC = Path(__file__).resolve().parent / "src"
sys.path.insert(0, str(_SRC))

from qbo_pipeline.web.app import create_app  # noqa: E402

app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5050"))
    debug = os.getenv("FLASK_DEBUG", "").strip().lower() in ("1", "true", "yes")
    app.run(host=os.getenv("FLASK_HOST", "127.0.0.1"), port=port, debug=debug)
