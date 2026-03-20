"""Write rows to Postgres in one transaction so a failed load does not wipe data halfway."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import psycopg2
from psycopg2.extras import Json

from qbo_pipeline.config import Settings
from qbo_pipeline.etl.transform import LoadBundle

_ALLOWED_TABLES = frozenset(
    {"customers", "invoices", "payments", "payment_invoice_allocations"}
)

_NO_JSON_COLS: frozenset[str] = frozenset()

_CUSTOMER_COLS = (
    "id",
    "qbo_id",
    "display_name",
    "company_name",
    "given_name",
    "family_name",
    "fully_qualified_name",
    "primary_email",
    "primary_phone",
    "balance",
    "balance_with_jobs",
    "currency_code",
    "active",
    "taxable",
    "bill_address",
    "ship_address",
    "qbo_create_time",
    "qbo_last_updated_time",
)
_CUSTOMER_JSON = frozenset({"bill_address", "ship_address"})

_INVOICE_COLS = (
    "id",
    "qbo_id",
    "customer_id",
    "doc_number",
    "txn_date",
    "due_date",
    "total_amount",
    "balance",
    "currency_code",
    "email_status",
    "print_status",
    "is_email_sent",
    "bill_email",
    "qbo_create_time",
    "qbo_last_updated_time",
)

_PAYMENT_COLS = (
    "id",
    "qbo_id",
    "customer_id",
    "txn_date",
    "total_amount",
    "unapplied_amount",
    "currency_code",
    "qbo_create_time",
    "qbo_last_updated_time",
)

_ALLOC_COLS = ("id", "payment_id", "invoice_id", "amount")

_POOLER_HINT = (
    "\n\nHint: The direct host db.*.supabase.co is often IPv6-only. "
    "If you see “resolve host” / “nodename” errors, open Supabase → "
    "Project Settings → Database → Connection string → pooler, "
    "copy that URI into DATABASE_URL / SUPABASE_DB_URL (@ in password → %40)."
)


def _supabase_pooler_hint(conninfo: str, exc: BaseException) -> str:
    if "db." not in conninfo or ".supabase.co" not in conninfo.lower():
        return ""
    text = str(exc).lower()
    if any(
        s in text
        for s in ("resolve host", "nodename", "servname", "name or service", "gaierror")
    ):
        return _POOLER_HINT
    return ""


def _serialize_cell(column: str, value: Any, json_cols: frozenset[str]) -> Any:
    if value is None:
        return None
    if column in json_cols:
        return Json(value)
    return value


def _insert_batches(
    cur: Any,
    table: str,
    columns: tuple[str, ...],
    json_cols: frozenset[str],
    rows: list[dict[str, Any]],
    chunk_size: int,
) -> None:
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"unknown table: {table}")
    if not rows:
        return
    col_sql = ", ".join(columns)
    one_row = "(" + ", ".join(["%s"] * len(columns)) + ")"

    for i in range(0, len(rows), chunk_size):
        batch = rows[i : i + chunk_size]
        values_sql = ", ".join([one_row] * len(batch))
        sql = f"INSERT INTO public.{table} ({col_sql}) VALUES {values_sql}"
        flat: list[Any] = []
        for r in batch:
            for c in columns:
                flat.append(_serialize_cell(c, r.get(c), json_cols))
        cur.execute(sql, flat)


def _start_sync_run(conninfo: str) -> UUID:
    conn = psycopg2.connect(conninfo)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.sync_runs (status) VALUES ('running') RETURNING id"
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("sync_runs INSERT did not return an id")
            rid = row[0]
            return rid if isinstance(rid, UUID) else UUID(str(rid))
    finally:
        conn.close()


def _finalize_sync_failed(conninfo: str, sync_id: UUID, message: str) -> None:
    conn = psycopg2.connect(conninfo)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.sync_runs
                SET finished_at = %s,
                    status = 'failed',
                    error_message = %s
                WHERE id = %s
                """,
                (datetime.now(timezone.utc), message[:8000], str(sync_id)),
            )
    finally:
        conn.close()


def load(settings: Settings, bundle: LoadBundle) -> str:
    conninfo = settings.supabase_database_url
    try:
        sync_id = _start_sync_run(conninfo)
    except Exception as exc:
        hint = _supabase_pooler_hint(conninfo, exc)
        raise RuntimeError(f"{exc}{hint}") from exc

    chunk = settings.supabase_insert_chunk_size
    conn = psycopg2.connect(conninfo)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.customers")
            _insert_batches(
                cur,
                "customers",
                _CUSTOMER_COLS,
                _CUSTOMER_JSON,
                bundle.customers,
                chunk,
            )
            _insert_batches(
                cur, "invoices", _INVOICE_COLS, _NO_JSON_COLS, bundle.invoices, chunk
            )
            _insert_batches(
                cur, "payments", _PAYMENT_COLS, _NO_JSON_COLS, bundle.payments, chunk
            )
            _insert_batches(
                cur,
                "payment_invoice_allocations",
                _ALLOC_COLS,
                _NO_JSON_COLS,
                bundle.payment_invoice_allocations,
                chunk,
            )
            finished = datetime.now(timezone.utc)
            cur.execute(
                """
                UPDATE public.sync_runs
                SET finished_at = %s,
                    status = 'success',
                    customer_count = %s,
                    invoice_count = %s,
                    payment_count = %s,
                    allocation_count = %s
                WHERE id = %s
                """,
                (
                    finished,
                    len(bundle.customers),
                    len(bundle.invoices),
                    len(bundle.payments),
                    len(bundle.payment_invoice_allocations),
                    str(sync_id),
                ),
            )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        msg = str(exc) + _supabase_pooler_hint(conninfo, exc)
        _finalize_sync_failed(conninfo, sync_id, msg)
        raise RuntimeError(msg) from exc
    finally:
        conn.close()

    return str(sync_id)
