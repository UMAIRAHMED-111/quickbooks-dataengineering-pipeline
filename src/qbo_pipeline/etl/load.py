"""Write rows to Postgres in one transaction so a failed load does not wipe data halfway."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from psycopg2.extras import Json

from qbo_pipeline.config import Settings
from qbo_pipeline.db.pool import pooled_connection
from qbo_pipeline.etl.transform import LoadBundle
from qbo_pipeline.etl.validate import validate_bundle
from qbo_pipeline.observability import get_logger

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

logger = get_logger(__name__)


@dataclass(frozen=True)
class UpsertStats:
    inserted: int
    updated: int
    unchanged: int


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


def _upsert_batches(
    cur: Any,
    table: str,
    columns: tuple[str, ...],
    json_cols: frozenset[str],
    rows: list[dict[str, Any]],
    chunk_size: int,
    *,
    conflict_target: str,
    update_columns: tuple[str, ...],
    freshness_column: str | None = None,
    returning_columns: tuple[str, ...] = (),
) -> tuple[list[tuple[Any, ...]], UpsertStats]:
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"unknown table: {table}")
    if not rows:
        return [], UpsertStats(inserted=0, updated=0, unchanged=0)
    col_sql = ", ".join(columns)
    one_row = "(" + ", ".join(["%s"] * len(columns)) + ")"
    updates_sql = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
    changed_sql = " OR ".join(
        f"public.{table}.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in update_columns
    )
    where_sql = f" WHERE ({changed_sql})"
    if freshness_column:
        where_sql = (
            " WHERE ("
            f"EXCLUDED.{freshness_column} IS NULL OR "
            f"public.{table}.{freshness_column} IS NULL OR "
            f"EXCLUDED.{freshness_column} >= public.{table}.{freshness_column}"
            f") AND ({changed_sql})"
        )
    out: list[tuple[Any, ...]] = []
    inserted = 0
    updated = 0

    for i in range(0, len(rows), chunk_size):
        batch = rows[i : i + chunk_size]
        values_sql = ", ".join([one_row] * len(batch))
        ret_cols = [*returning_columns, "(xmax = 0) AS __inserted"]
        ret_sql = " RETURNING " + ", ".join(ret_cols)
        sql = (
            f"INSERT INTO public.{table} ({col_sql}) VALUES {values_sql} "
            f"ON CONFLICT ({conflict_target}) DO UPDATE SET {updates_sql}{where_sql}{ret_sql}"
        )
        flat: list[Any] = []
        for r in batch:
            for c in columns:
                flat.append(_serialize_cell(c, r.get(c), json_cols))
        cur.execute(sql, flat)
        returned = cur.fetchall()
        for record in returned:
            is_insert = bool(record[-1])
            if is_insert:
                inserted += 1
            else:
                updated += 1
            if returning_columns:
                out.append(record[: len(returning_columns)])
    unchanged = len(rows) - inserted - updated
    return out, UpsertStats(inserted=inserted, updated=updated, unchanged=unchanged)


def _reverse_qbo_by_id(rows: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in rows:
        rid = row.get("id")
        qbo_id = row.get("qbo_id")
        if rid is None or qbo_id is None:
            continue
        out[str(rid)] = str(qbo_id)
    return out


def _fetch_ids_by_qbo(
    cur: Any,
    *,
    table: str,
    qbo_ids: set[str],
) -> dict[str, str]:
    if not qbo_ids:
        return {}
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"unknown table: {table}")
    sql = f"SELECT qbo_id, id FROM public.{table} WHERE qbo_id = ANY(%s)"
    cur.execute(sql, (list(qbo_ids),))
    return {str(qbo_id): str(db_id) for qbo_id, db_id in cur.fetchall()}


def _remap_parent_ids(
    rows: list[dict[str, Any]],
    *,
    field: str,
    old_parent_qbo_by_id: dict[str, str],
    new_parent_id_by_qbo: dict[str, str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        parent_qbo = old_parent_qbo_by_id[str(row[field])]
        patched = dict(row)
        patched[field] = new_parent_id_by_qbo[parent_qbo]
        out.append(patched)
    return out


def _upsert_qbo_bundle(cur: Any, bundle: LoadBundle, chunk: int) -> UpsertStats:
    customers_by_qbo: dict[str, str] = {}
    customer_rows, customer_stats = _upsert_batches(
        cur,
        "customers",
        _CUSTOMER_COLS,
        _CUSTOMER_JSON,
        bundle.customers,
        chunk,
        conflict_target="qbo_id",
        update_columns=(
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
        ),
        freshness_column="qbo_last_updated_time",
        returning_columns=("qbo_id", "id"),
    )
    for qbo_id, db_id in customer_rows:
        customers_by_qbo[str(qbo_id)] = str(db_id)
    missing_customer_qbo_ids = {
        str(row["qbo_id"]) for row in bundle.customers
    } - set(customers_by_qbo.keys())
    customers_by_qbo.update(
        _fetch_ids_by_qbo(
            cur,
            table="customers",
            qbo_ids=missing_customer_qbo_ids,
        )
    )

    old_customer_qbo_by_id = _reverse_qbo_by_id(bundle.customers)
    invoice_rows = _remap_parent_ids(
        bundle.invoices,
        field="customer_id",
        old_parent_qbo_by_id=old_customer_qbo_by_id,
        new_parent_id_by_qbo=customers_by_qbo,
    )
    payment_rows = _remap_parent_ids(
        bundle.payments,
        field="customer_id",
        old_parent_qbo_by_id=old_customer_qbo_by_id,
        new_parent_id_by_qbo=customers_by_qbo,
    )

    invoices_by_qbo: dict[str, str] = {}
    invoice_db_rows, invoice_stats = _upsert_batches(
        cur,
        "invoices",
        _INVOICE_COLS,
        _NO_JSON_COLS,
        invoice_rows,
        chunk,
        conflict_target="qbo_id",
        update_columns=(
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
        ),
        freshness_column="qbo_last_updated_time",
        returning_columns=("qbo_id", "id"),
    )
    for qbo_id, db_id in invoice_db_rows:
        invoices_by_qbo[str(qbo_id)] = str(db_id)
    missing_invoice_qbo_ids = {
        str(row["qbo_id"]) for row in invoice_rows
    } - set(invoices_by_qbo.keys())
    invoices_by_qbo.update(
        _fetch_ids_by_qbo(
            cur,
            table="invoices",
            qbo_ids=missing_invoice_qbo_ids,
        )
    )

    payments_by_qbo: dict[str, str] = {}
    payment_db_rows, payment_stats = _upsert_batches(
        cur,
        "payments",
        _PAYMENT_COLS,
        _NO_JSON_COLS,
        payment_rows,
        chunk,
        conflict_target="qbo_id",
        update_columns=(
            "customer_id",
            "txn_date",
            "total_amount",
            "unapplied_amount",
            "currency_code",
            "qbo_create_time",
            "qbo_last_updated_time",
        ),
        freshness_column="qbo_last_updated_time",
        returning_columns=("qbo_id", "id"),
    )
    for qbo_id, db_id in payment_db_rows:
        payments_by_qbo[str(qbo_id)] = str(db_id)
    missing_payment_qbo_ids = {
        str(row["qbo_id"]) for row in payment_rows
    } - set(payments_by_qbo.keys())
    payments_by_qbo.update(
        _fetch_ids_by_qbo(
            cur,
            table="payments",
            qbo_ids=missing_payment_qbo_ids,
        )
    )

    old_invoice_qbo_by_id = _reverse_qbo_by_id(bundle.invoices)
    old_payment_qbo_by_id = _reverse_qbo_by_id(bundle.payments)
    allocation_rows: list[dict[str, Any]] = []
    for row in bundle.payment_invoice_allocations:
        payment_qbo = old_payment_qbo_by_id[str(row["payment_id"])]
        invoice_qbo = old_invoice_qbo_by_id[str(row["invoice_id"])]
        patched = dict(row)
        patched["payment_id"] = payments_by_qbo[payment_qbo]
        patched["invoice_id"] = invoices_by_qbo[invoice_qbo]
        allocation_rows.append(patched)

    _, allocation_stats = _upsert_batches(
        cur,
        "payment_invoice_allocations",
        _ALLOC_COLS,
        _NO_JSON_COLS,
        allocation_rows,
        chunk,
        conflict_target="payment_id, invoice_id",
        update_columns=("amount",),
    )
    return UpsertStats(
        inserted=(
            customer_stats.inserted
            + invoice_stats.inserted
            + payment_stats.inserted
            + allocation_stats.inserted
        ),
        updated=(
            customer_stats.updated
            + invoice_stats.updated
            + payment_stats.updated
            + allocation_stats.updated
        ),
        unchanged=(
            customer_stats.unchanged
            + invoice_stats.unchanged
            + payment_stats.unchanged
            + allocation_stats.unchanged
        ),
    )


def _start_sync_run(conninfo: str) -> UUID:
    with pooled_connection(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.sync_runs (status) VALUES ('running') RETURNING id"
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("sync_runs INSERT did not return an id")
            rid = row[0]
            return rid if isinstance(rid, UUID) else UUID(str(rid))


def _finalize_sync_failed(conninfo: str, sync_id: UUID, message: str) -> None:
    with pooled_connection(conninfo, autocommit=True) as conn:
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


def run_delete_phase(settings: Settings) -> str:
    """Start ``sync_runs`` for Airflow-style split phases (no table delete)."""
    conninfo = settings.supabase_database_url
    try:
        sync_id = _start_sync_run(conninfo)
    except Exception as exc:
        hint = _supabase_pooler_hint(conninfo, exc)
        raise RuntimeError(f"{exc}{hint}") from exc

    sync_id_str = str(sync_id)
    logger.info("sync_run_started", sync_run_id=sync_id_str)
    return sync_id_str


def run_insert_phase(settings: Settings, sync_id: UUID, bundle: LoadBundle) -> str:
    """Upsert bundle rows and mark ``sync_runs`` success (one transaction)."""
    conninfo = settings.supabase_database_url
    chunk = settings.supabase_insert_chunk_size
    logger.info("sync_run_insert_phase_started", sync_run_id=sync_id)
    try:
        validate_bundle(bundle)
        with pooled_connection(conninfo) as conn:
            with conn.cursor() as cur:
                stats = _upsert_qbo_bundle(cur, bundle, chunk)
                finished = datetime.now(timezone.utc)
                cur.execute(
                    """
                    UPDATE public.sync_runs
                    SET finished_at = %s,
                        status = 'success',
                        customer_count = %s,
                        invoice_count = %s,
                        payment_count = %s,
                        allocation_count = %s,
                        inserted_count = %s,
                        updated_count = %s,
                        unchanged_count = %s
                    WHERE id = %s
                    """,
                    (
                        finished,
                        len(bundle.customers),
                        len(bundle.invoices),
                        len(bundle.payments),
                        len(bundle.payment_invoice_allocations),
                        stats.inserted,
                        stats.updated,
                        stats.unchanged,
                        str(sync_id),
                    ),
                )
            conn.commit()
    except Exception as exc:
        msg = str(exc) + _supabase_pooler_hint(conninfo, exc)
        _finalize_sync_failed(conninfo, sync_id, msg)
        logger.exception("sync_run_failed", sync_run_id=sync_id, error=msg)
        raise RuntimeError(msg) from exc

    logger.info(
        "sync_run_completed",
        sync_run_id=sync_id,
        customer_count=len(bundle.customers),
        invoice_count=len(bundle.invoices),
        payment_count=len(bundle.payments),
        allocation_count=len(bundle.payment_invoice_allocations),
    )
    return str(sync_id)


def mark_sync_failed(settings: Settings, sync_id: UUID, message: str) -> None:
    """Mark an existing sync run as failed for pre-load stage errors."""
    msg = message[:8000]
    _finalize_sync_failed(settings.supabase_database_url, sync_id, msg)
    logger.error("sync_run_marked_failed", sync_run_id=sync_id, error=msg)


def load(settings: Settings, bundle: LoadBundle) -> str:
    conninfo = settings.supabase_database_url
    try:
        sync_id = _start_sync_run(conninfo)
    except Exception as exc:
        hint = _supabase_pooler_hint(conninfo, exc)
        raise RuntimeError(f"{exc}{hint}") from exc

    chunk = settings.supabase_insert_chunk_size
    try:
        validate_bundle(bundle)
        with pooled_connection(conninfo) as conn:
            with conn.cursor() as cur:
                stats = _upsert_qbo_bundle(cur, bundle, chunk)
                finished = datetime.now(timezone.utc)
                cur.execute(
                    """
                    UPDATE public.sync_runs
                    SET finished_at = %s,
                        status = 'success',
                        customer_count = %s,
                        invoice_count = %s,
                        payment_count = %s,
                        allocation_count = %s,
                        inserted_count = %s,
                        updated_count = %s,
                        unchanged_count = %s
                    WHERE id = %s
                    """,
                    (
                        finished,
                        len(bundle.customers),
                        len(bundle.invoices),
                        len(bundle.payments),
                        len(bundle.payment_invoice_allocations),
                        stats.inserted,
                        stats.updated,
                        stats.unchanged,
                        str(sync_id),
                    ),
                )
            conn.commit()
    except Exception as exc:
        msg = str(exc) + _supabase_pooler_hint(conninfo, exc)
        _finalize_sync_failed(conninfo, sync_id, msg)
        raise RuntimeError(msg) from exc

    return str(sync_id)
