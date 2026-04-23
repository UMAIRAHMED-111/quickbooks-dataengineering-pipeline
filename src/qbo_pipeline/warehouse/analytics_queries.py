"""Read-only analytics queries for dashboard / charts (Postgres warehouse)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID

from psycopg2.extras import RealDictCursor

from qbo_pipeline.db.pool import pooled_connection
from qbo_pipeline.observability import get_logger

logger = get_logger(__name__)


def _serialize(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (date,)):
        return val.isoformat()
    if isinstance(val, UUID):
        return str(val)
    return val


def _rows(cur) -> list[dict[str, Any]]:
    raw = cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in raw:
        out.append({k: _serialize(v) for k, v in dict(row).items()})
    return out


def _one(
    database_url: str, sql: str, params: tuple | None = None, *, query_name: str
) -> dict[str, Any] | None:
    logger.info("analytics_query_started", query_name=query_name)
    with pooled_connection(database_url) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            if not row:
                logger.info("analytics_query_completed", query_name=query_name, row_count=0)
                return None
            out = {k: _serialize(v) for k, v in dict(row).items()}
            logger.info("analytics_query_completed", query_name=query_name, row_count=1)
            return out


def _all(
    database_url: str, sql: str, params: tuple | None = None, *, query_name: str
) -> list[dict[str, Any]]:
    logger.info("analytics_query_started", query_name=query_name)
    with pooled_connection(database_url) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            out = _rows(cur)
            logger.info("analytics_query_completed", query_name=query_name, row_count=len(out))
            return out


def overview(database_url: str) -> dict[str, Any]:
    sql = """
    SELECT
      (SELECT COUNT(*) FROM public.customers) AS customer_count,
      (SELECT COUNT(*) FROM public.invoices) AS invoice_count,
      (SELECT COUNT(*) FROM public.payments) AS payment_count,
      (SELECT COALESCE(SUM(balance), 0) FROM public.invoices WHERE balance > 0) AS total_outstanding,
      (SELECT COALESCE(SUM(total_amount), 0) FROM public.invoices) AS total_invoiced,
      (SELECT COALESCE(SUM(total_amount), 0) FROM public.payments) AS total_payments_recorded
    """
    row = _one(database_url, sql, query_name="overview")
    return row or {}


def invoices_paid_vs_unpaid(database_url: str) -> dict[str, Any]:
    sql = """
    SELECT
      COUNT(*) FILTER (WHERE COALESCE(balance, 0) = 0) AS paid_count,
      COALESCE(SUM(total_amount) FILTER (WHERE COALESCE(balance, 0) = 0), 0) AS paid_total_billed,
      COUNT(*) FILTER (WHERE balance > 0) AS unpaid_count,
      COALESCE(SUM(balance) FILTER (WHERE balance > 0), 0) AS unpaid_open_balance,
      COALESCE(SUM(total_amount) FILTER (WHERE balance > 0), 0) AS unpaid_total_billed
    FROM public.invoices
    """
    row = _one(database_url, sql, query_name="invoices_paid_vs_unpaid")
    return row or {}


def invoices_sent_vs_unsent(database_url: str) -> dict[str, Any]:
    sql = """
    SELECT
      is_email_sent AS email_sent,
      COUNT(*) AS invoice_count,
      COALESCE(SUM(total_amount), 0) AS sum_total_amount,
      COALESCE(SUM(balance), 0) AS sum_open_balance
    FROM public.invoices
    GROUP BY is_email_sent
    ORDER BY is_email_sent DESC
    """
    rows = _all(database_url, sql, query_name="invoices_sent_vs_unsent")
    return {"buckets": rows}


def invoices_overdue_vs_current(database_url: str) -> dict[str, Any]:
    """Unpaid only: past due vs not yet due."""
    sql = """
    SELECT
      COUNT(*) FILTER (
        WHERE balance > 0 AND due_date IS NOT NULL AND due_date < CURRENT_DATE
      ) AS overdue_unpaid_count,
      COALESCE(SUM(balance) FILTER (
        WHERE balance > 0 AND due_date IS NOT NULL AND due_date < CURRENT_DATE
      ), 0) AS overdue_unpaid_amount,
      COUNT(*) FILTER (
        WHERE balance > 0 AND (due_date IS NULL OR due_date >= CURRENT_DATE)
      ) AS current_unpaid_count,
      COALESCE(SUM(balance) FILTER (
        WHERE balance > 0 AND (due_date IS NULL OR due_date >= CURRENT_DATE)
      ), 0) AS current_unpaid_amount
    FROM public.invoices
    """
    row = _one(database_url, sql, query_name="invoices_overdue_vs_current")
    return row or {}


def customers_top_paying(database_url: str, limit: int = 10) -> dict[str, Any]:
    lim = max(1, min(int(limit), 100))
    sql = """
    SELECT
      MAX(COALESCE(c.display_name, c.company_name, c.qbo_id)) AS customer_name,
      COALESCE(SUM(p.total_amount), 0) AS total_payments,
      COUNT(p.id) AS payment_row_count
    FROM public.customers c
    LEFT JOIN public.payments p ON p.customer_id = c.id
    GROUP BY c.id
    ORDER BY total_payments DESC NULLS LAST
    LIMIT %s
    """
    return {"customers": _all(database_url, sql, (lim,), query_name="customers_top_paying")}


def customers_top_outstanding(database_url: str, limit: int = 10) -> dict[str, Any]:
    lim = max(1, min(int(limit), 100))
    sql = """
    SELECT
      COALESCE(c.display_name, c.company_name, c.qbo_id) AS customer_name,
      COALESCE(c.balance, 0) AS customer_balance,
      (SELECT COUNT(*) FROM public.invoices i
       WHERE i.customer_id = c.id AND i.balance > 0) AS open_invoice_count
    FROM public.customers c
    WHERE c.balance > 0
    ORDER BY c.balance DESC
    LIMIT %s
    """
    return {
        "customers": _all(
            database_url, sql, (lim,), query_name="customers_top_outstanding"
        )
    }


def customers_top_overdue_debt(database_url: str, limit: int = 10) -> dict[str, Any]:
    """Customers with largest unpaid balance on past-due invoices ("not paying on time")."""
    lim = max(1, min(int(limit), 100))
    sql = """
    SELECT
      MAX(COALESCE(c.display_name, c.company_name, c.qbo_id)) AS customer_name,
      COALESCE(SUM(i.balance), 0) AS overdue_open_balance,
      COUNT(*) AS overdue_invoice_count
    FROM public.invoices i
    JOIN public.customers c ON c.id = i.customer_id
    WHERE i.balance > 0
      AND i.due_date IS NOT NULL
      AND i.due_date < CURRENT_DATE
    GROUP BY c.id
    ORDER BY overdue_open_balance DESC
    LIMIT %s
    """
    return {
        "customers": _all(
            database_url, sql, (lim,), query_name="customers_top_overdue_debt"
        )
    }


def invoices_paid_on_time_vs_late(database_url: str) -> dict[str, Any]:
    """Fully paid invoices: last payment date vs due date (requires allocations)."""
    sql = """
    WITH last_pay AS (
      SELECT
        i.id AS invoice_id,
        i.customer_id,
        i.due_date,
        MAX(p.txn_date) AS last_payment_date
      FROM public.invoices i
      JOIN public.payment_invoice_allocations pia ON pia.invoice_id = i.id
      JOIN public.payments p ON p.id = pia.payment_id
      WHERE COALESCE(i.balance, 0) = 0
      GROUP BY i.id, i.customer_id, i.due_date
    )
    SELECT
      COUNT(*) FILTER (
        WHERE due_date IS NOT NULL AND last_payment_date IS NOT NULL
          AND last_payment_date <= due_date
      ) AS paid_on_time_count,
      COUNT(*) FILTER (
        WHERE due_date IS NOT NULL AND last_payment_date IS NOT NULL
          AND last_payment_date > due_date
      ) AS paid_late_count,
      COUNT(*) FILTER (
        WHERE due_date IS NULL OR last_payment_date IS NULL
      ) AS paid_unknown_timing_count
    FROM last_pay
    """
    row = _one(database_url, sql, query_name="invoices_paid_on_time_vs_late")
    return row or {}


def customers_best_on_time_payers(database_url: str, limit: int = 10) -> dict[str, Any]:
    """Top customers by count of fully paid invoices settled on/before due date."""
    lim = max(1, min(int(limit), 100))
    sql = """
    WITH last_pay AS (
      SELECT
        i.id AS invoice_id,
        i.customer_id,
        i.due_date,
        MAX(p.txn_date) AS last_payment_date
      FROM public.invoices i
      JOIN public.payment_invoice_allocations pia ON pia.invoice_id = i.id
      JOIN public.payments p ON p.id = pia.payment_id
      WHERE COALESCE(i.balance, 0) = 0
      GROUP BY i.id, i.customer_id, i.due_date
    )
    SELECT
      MAX(COALESCE(c.display_name, c.company_name, c.qbo_id)) AS customer_name,
      COUNT(*) FILTER (
        WHERE lp.due_date IS NOT NULL AND lp.last_payment_date IS NOT NULL
          AND lp.last_payment_date <= lp.due_date
      ) AS on_time_paid_invoice_count,
      COUNT(*) FILTER (
        WHERE lp.due_date IS NOT NULL AND lp.last_payment_date IS NOT NULL
          AND lp.last_payment_date > lp.due_date
      ) AS late_paid_invoice_count
    FROM last_pay lp
    JOIN public.customers c ON c.id = lp.customer_id
    GROUP BY c.id
    HAVING COUNT(*) FILTER (
        WHERE lp.due_date IS NOT NULL AND lp.last_payment_date IS NOT NULL
          AND lp.last_payment_date <= lp.due_date
      ) > 0
    ORDER BY on_time_paid_invoice_count DESC, late_paid_invoice_count ASC
    LIMIT %s
    """
    return {
        "customers": _all(
            database_url, sql, (lim,), query_name="customers_best_on_time_payers"
        )
    }


def payments_by_month(database_url: str) -> dict[str, Any]:
    sql = """
    SELECT
      (DATE_TRUNC('month', txn_date))::date AS month,
      COALESCE(SUM(total_amount), 0) AS total_amount,
      COUNT(*) AS payment_count
    FROM public.payments
    WHERE txn_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', txn_date)
    ORDER BY month ASC
    """
    return {"series": _all(database_url, sql, query_name="payments_by_month")}


def allocations_summary(database_url: str) -> dict[str, Any]:
    sql = """
    SELECT
      COUNT(*) AS allocation_count,
      COALESCE(SUM(amount), 0) AS total_allocated,
      COUNT(DISTINCT payment_id) AS payments_with_allocations,
      COUNT(DISTINCT invoice_id) AS invoices_with_allocations
    FROM public.payment_invoice_allocations
    """
    row = _one(database_url, sql, query_name="allocations_summary")
    return row or {}
