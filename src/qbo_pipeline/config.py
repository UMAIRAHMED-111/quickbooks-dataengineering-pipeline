"""Load `.env` then read settings from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


@dataclass(frozen=True)
class Settings:
    n8n_webhook_url: str
    supabase_database_url: str
    n8n_http_timeout_seconds: float
    supabase_insert_chunk_size: int

    @staticmethod
    def from_env() -> Settings:
        db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
        if not db:
            raise RuntimeError(
                "Set SUPABASE_DB_URL or DATABASE_URL to your Postgres connection URI"
            )
        return Settings(
            n8n_webhook_url=_require("N8N_WEBHOOK_URL"),
            supabase_database_url=db,
            n8n_http_timeout_seconds=float(os.getenv("N8N_HTTP_TIMEOUT_SECONDS", "120")),
            supabase_insert_chunk_size=int(os.getenv("SUPABASE_INSERT_CHUNK_SIZE", "500")),
        )


@dataclass(frozen=True)
class WarehouseQaConfig:
    """OpenAI (primary) + Gemini (fallback) over read-only SQL snapshot / dynamic SQL."""

    database_url: str
    openai_api_key_1: str | None
    openai_api_key_2: str | None
    openai_model: str
    openai_planner_model: str
    openai_sql_model: str
    gemini_api_key: str | None
    gemini_model: str
    gemini_planner_model: str
    gemini_sql_model: str
    use_snapshot_planner: bool
    use_dynamic_sql: bool
    qa_context_max_chars: int
    qa_context_max_messages: int

    @staticmethod
    def from_env() -> WarehouseQaConfig:
        db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
        if not db:
            raise RuntimeError(
                "Set DATABASE_URL or SUPABASE_DB_URL for warehouse Q&A"
            )
        k1 = os.getenv("OPENAI_API_KEY_1", "").strip()
        k2 = os.getenv("OPENAI_API_KEY_2", "").strip()
        gem = (
            os.getenv("GEMINI_API_KEY", "").strip()
            or os.getenv("GOOGLE_API_KEY", "").strip()
        ) or None
        if not k1 and not k2 and not gem:
            raise RuntimeError(
                "Set OPENAI_API_KEY_1 and/or OPENAI_API_KEY_2 and/or GEMINI_API_KEY "
                "(or GOOGLE_API_KEY) for natural-language answers"
            )
        openai_model = os.getenv("OPENAI_MODEL", "gpt-4").strip() or "gpt-4"
        openai_planner = (os.getenv("OPENAI_PLANNER_MODEL") or "").strip() or openai_model
        openai_sql = (os.getenv("OPENAI_SQL_MODEL") or "").strip() or openai_model
        # Gemini defaults for fallback (when keys are present or for model strings)
        g_model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite").strip()
        g_planner = (os.getenv("GEMINI_PLANNER_MODEL") or "").strip() or g_model
        g_sql = (os.getenv("GEMINI_SQL_MODEL") or "").strip() or g_model
        no_planner = os.getenv("WAREHOUSE_QA_NO_PLANNER", "").strip().lower() in (
            "1",
            "true",
            "yes",
        )
        use_dynamic_sql = os.getenv(
            "WAREHOUSE_QA_DYNAMIC_SQL", ""
        ).strip().lower() in ("1", "true", "yes")
        try:
            qa_context_max_chars = int(
                os.getenv("WAREHOUSE_QA_CONTEXT_MAX_CHARS", "12000").strip()
            )
        except ValueError:
            qa_context_max_chars = 12_000
        qa_context_max_chars = max(0, min(qa_context_max_chars, 100_000))
        try:
            qa_context_max_messages = int(
                os.getenv("WAREHOUSE_QA_CONTEXT_MAX_MESSAGES", "24").strip()
            )
        except ValueError:
            qa_context_max_messages = 24
        qa_context_max_messages = max(0, min(qa_context_max_messages, 100))
        return WarehouseQaConfig(
            database_url=db,
            openai_api_key_1=k1 or None,
            openai_api_key_2=k2 or None,
            openai_model=openai_model,
            openai_planner_model=openai_planner,
            openai_sql_model=openai_sql,
            gemini_api_key=gem,
            gemini_model=g_model,
            gemini_planner_model=g_planner,
            gemini_sql_model=g_sql,
            use_snapshot_planner=not no_planner,
            use_dynamic_sql=use_dynamic_sql,
            qa_context_max_chars=qa_context_max_chars,
            qa_context_max_messages=qa_context_max_messages,
        )
