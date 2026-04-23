"""Flask HTTP API for dashboard charts, warehouse Q&A, and triggering pipeline sync."""

from __future__ import annotations

import os
from threading import Thread
from functools import wraps
from typing import Any, Callable

import psycopg2
from flask import Flask, jsonify, request
from flask_cors import CORS
from google.genai.errors import APIError as GeminiAPIError
from openai import APIError as OpenAIAPIError

from qbo_pipeline.config import Settings, WarehouseQaConfig
from qbo_pipeline.etl.pipeline import continue_sync_run, start_sync_run
from qbo_pipeline.observability import get_logger
from qbo_pipeline.qa.answer_structure import structure_qa_response
from qbo_pipeline.qa.context_window import normalize_context_turns
from qbo_pipeline.qa.warehouse_qa import answer_question
from qbo_pipeline.warehouse import analytics_queries as aq

logger = get_logger(__name__)


def _database_url() -> str:
    db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db:
        raise RuntimeError(
            "Set DATABASE_URL or SUPABASE_DB_URL for the analytics API"
        )
    return db


def _handle_db(fn: Callable[..., Any]):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            url = _database_url()
        except RuntimeError as exc:
            return jsonify({"error": str(exc)}), 503
        try:
            return fn(url, *args, **kwargs)
        except psycopg2.Error:
            logger.exception("database_query_failed")
            return jsonify({"error": "database_error"}), 500

    return wrapper


def _sync_auth_error():
    """If SYNC_API_SECRET is set, require X-Sync-Token or Authorization: Bearer."""
    secret = os.getenv("SYNC_API_SECRET", "").strip()
    if not secret:
        return None
    token = request.headers.get("X-Sync-Token", "").strip()
    auth = request.headers.get("Authorization", "").strip()
    if auth.lower().startswith("bearer "):
        token = token or auth[7:].strip()
    if token != secret:
        return jsonify({"error": "unauthorized", "hint": "Set X-Sync-Token or Bearer"}), 401
    return None


def _schedule_background_sync(job: Callable[[], None], *, sync_id: str) -> None:
    worker = Thread(
        target=job,
        name=f"qbo-sync-{sync_id}",
        daemon=True,
    )
    worker.start()


def create_app() -> Flask:
    app = Flask(__name__)
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "service": "qbo-analytics-api"})

    @app.post("/api/v1/sync")
    def trigger_sync():
        """
        Queue a sync run and return immediately (webhook or optional local_file JSON body / query).
        JSON body: {"local_file": "path/to.json"} optional.
        """
        auth_err = _sync_auth_error()
        if auth_err is not None:
            return auth_err
        try:
            settings = Settings.from_env()
        except RuntimeError as exc:
            return jsonify({"error": str(exc)}), 503
        local_file = None
        if request.is_json and request.json:
            local_file = request.json.get("local_file")
        if not local_file:
            local_file = request.args.get("local_file")
        try:
            sync_id = start_sync_run(settings)
        except Exception:
            logger.exception("sync_start_failed")
            return jsonify({"error": "sync_failed"}), 500

        def _run_sync_job() -> None:
            try:
                continue_sync_run(settings, sync_id, local_path=local_file)
            except Exception:
                logger.exception("api_sync_background_job_failed", sync_run_id=sync_id)

        _schedule_background_sync(_run_sync_job, sync_id=sync_id)
        logger.info("api_sync_accepted", sync_run_id=sync_id)
        return jsonify({"sync_run_id": str(sync_id), "status": "accepted"}), 202

    @app.post("/api/v1/qa")
    def warehouse_qa():
        """
        Natural-language Q&A over the loaded warehouse (same logic as `python ask.py`).
        JSON: question, optional context (conversation window), answer + display.
        """
        if not request.is_json or request.json is None:
            return (
                jsonify(
                    {
                        "error": "invalid_request",
                        "detail": 'Send JSON: {"question": "..."} optional "context": [{"role":"user|assistant","content":"..."}]',
                    }
                ),
                400,
            )
        raw = request.json.get("question")
        question = (raw if isinstance(raw, str) else "").strip()
        if not question:
            return (
                jsonify(
                    {
                        "error": "invalid_request",
                        "detail": "question must be a non-empty string",
                    }
                ),
                400,
            )
        try:
            cfg = WarehouseQaConfig.from_env()
        except RuntimeError as exc:
            return jsonify({"error": "service_unavailable", "detail": str(exc)}), 503
        ctx_turns = normalize_context_turns(request.json.get("context"))
        try:
            answer = answer_question(
                cfg,
                question,
                context=ctx_turns if ctx_turns else None,
            )
        except (GeminiAPIError, OpenAIAPIError) as exc:
            logger.exception("warehouse_qa_llm_error")
            code = getattr(exc, "code", None)
            if code is None:
                code = getattr(exc, "status_code", None)
            payload: dict[str, Any] = {
                "error": "llm_error",
                "detail": "LLM provider error. Retry or try again later.",
            }
            if code is not None:
                payload["code"] = code
            return jsonify(payload), 502
        except Exception:
            logger.exception("warehouse_qa_failed")
            return jsonify({"error": "qa_failed"}), 500
        return jsonify(structure_qa_response(question=question, answer=answer)), 200

    @app.get("/api/v1/metrics/overview")
    @_handle_db
    def overview(url: str):
        return jsonify(aq.overview(url))

    @app.get("/api/v1/metrics/invoices/paid-vs-unpaid")
    @_handle_db
    def paid_unpaid(url: str):
        return jsonify(aq.invoices_paid_vs_unpaid(url))

    @app.get("/api/v1/metrics/invoices/sent-vs-unsent")
    @_handle_db
    def sent_unsent(url: str):
        return jsonify(aq.invoices_sent_vs_unsent(url))

    @app.get("/api/v1/metrics/invoices/overdue-vs-current")
    @_handle_db
    def overdue(url: str):
        return jsonify(aq.invoices_overdue_vs_current(url))

    @app.get("/api/v1/metrics/invoices/paid-on-time-vs-late")
    @_handle_db
    def on_time_late(url: str):
        return jsonify(aq.invoices_paid_on_time_vs_late(url))

    @app.get("/api/v1/metrics/customers/top-paying")
    @_handle_db
    def top_paying(url: str):
        limit = request.args.get("limit", default=10, type=int)
        return jsonify(aq.customers_top_paying(url, limit))

    @app.get("/api/v1/metrics/customers/top-outstanding")
    @_handle_db
    def top_outstanding(url: str):
        limit = request.args.get("limit", default=10, type=int)
        return jsonify(aq.customers_top_outstanding(url, limit))

    @app.get("/api/v1/metrics/customers/top-overdue-debt")
    @_handle_db
    def top_overdue(url: str):
        limit = request.args.get("limit", default=10, type=int)
        return jsonify(aq.customers_top_overdue_debt(url, limit))

    @app.get("/api/v1/metrics/customers/best-on-time-payers")
    @_handle_db
    def best_on_time(url: str):
        limit = request.args.get("limit", default=10, type=int)
        return jsonify(aq.customers_best_on_time_payers(url, limit))

    @app.get("/api/v1/metrics/payments/by-month")
    @_handle_db
    def pay_month(url: str):
        return jsonify(aq.payments_by_month(url))

    @app.get("/api/v1/metrics/allocations/summary")
    @_handle_db
    def alloc_summary(url: str):
        return jsonify(aq.allocations_summary(url))

    @app.get("/api/v1/metrics/catalog")
    def catalog():
        """List endpoints for frontend wiring."""
        paths = [
            "POST /api/v1/sync",
            "POST /api/v1/qa",
            "/api/v1/metrics/overview",
            "/api/v1/metrics/invoices/paid-vs-unpaid",
            "/api/v1/metrics/invoices/sent-vs-unsent",
            "/api/v1/metrics/invoices/overdue-vs-current",
            "/api/v1/metrics/invoices/paid-on-time-vs-late",
            "/api/v1/metrics/customers/top-paying?limit=10",
            "/api/v1/metrics/customers/top-outstanding?limit=10",
            "/api/v1/metrics/customers/top-overdue-debt?limit=10",
            "/api/v1/metrics/customers/best-on-time-payers?limit=10",
            "/api/v1/metrics/payments/by-month",
            "/api/v1/metrics/allocations/summary",
        ]
        return jsonify({"endpoints": paths})

    return app
