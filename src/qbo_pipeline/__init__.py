"""QuickBooks → Supabase: ``etl`` (sync), ``warehouse`` (SQL/analytics), ``qa`` (OpenAI/Gemini Q&A), ``web`` (Flask API)."""

from qbo_pipeline.etl.pipeline import run_sync

__all__ = ["run_sync"]
