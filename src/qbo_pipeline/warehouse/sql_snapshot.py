"""Read-only SQL slices from the warehouse for LLM context (exact queries, composed by pack id)."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from qbo_pipeline.db.pool import pooled_connection
from qbo_pipeline.observability import get_logger

Cursor = Any
PackFn = Callable[[Cursor], list[str]]
logger = get_logger(__name__)

# Order preserved when merging multiple packs for stable, readable summaries.
PACK_ORDER: tuple[str, ...] = (
    "counts_basic",
    "payments_summary",
    "unpaid_totals",
    "email_status",
    "customers_owing",
    "sample_open_invoices",
    "sample_unpaid_unsent",
)

ALL_PACK_IDS: frozenset[str] = frozenset(PACK_ORDER)

# One-line descriptions for the planner (keep in sync with PACK_ORDER).
PACK_DESCRIPTIONS: tuple[tuple[str, str], ...] = (
    ("counts_basic", "Counts of customers, invoices, and payment rows."),
    (
        "payments_summary",
        "Payment total_amount sum and count: all time, calendar current month (txn_date), last 30 days.",
    ),
    (
        "unpaid_totals",
        "Unpaid invoice count and open balance sum; paid-in-full invoice count.",
    ),
    (
        "email_status",
        "How many invoices are marked email-sent vs not (is_email_sent).",
    ),
    (
        "customers_owing",
        "How many customers have positive balance; top customers by open balance.",
    ),
    (
        "sample_open_invoices",
        "Sample rows: open invoices with customer, balance, due, email flag.",
    ),
    (
        "sample_unpaid_unsent",
        "Sample rows: unpaid invoices not marked email-sent.",
    ),
)


def _pack_counts_basic(cur: Cursor) -> list[str]:
    lines: list[str] = []
    cur.execute("SELECT COUNT(*) FROM public.customers")
    lines.append(f"- **Customers:** {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM public.invoices")
    lines.append(f"- **Invoices (total):** {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM public.payments")
    lines.append(f"- **Payments:** {cur.fetchone()[0]}")
    return lines


def _pack_payments_summary(cur: Cursor) -> list[str]:
    lines: list[str] = []
    cur.execute(
        """
        SELECT COALESCE(SUM(total_amount), 0), COUNT(*)
        FROM public.payments
        """
    )
    amt_all, n_all = cur.fetchone()
    lines.append(
        f"- **Payments (all time):** {n_all} rows, **SUM(total_amount)** ≈ {amt_all}"
    )
    cur.execute(
        """
        SELECT COALESCE(SUM(total_amount), 0), COUNT(*)
        FROM public.payments
        WHERE txn_date >= date_trunc('month', CURRENT_DATE)::date
          AND txn_date <= CURRENT_DATE
        """
    )
    amt_mo, n_mo = cur.fetchone()
    lines.append(
        f"- **Payments (calendar month to date, txn_date):** {n_mo} rows, **SUM(total_amount)** ≈ {amt_mo}"
    )
    cur.execute(
        """
        SELECT COALESCE(SUM(total_amount), 0), COUNT(*)
        FROM public.payments
        WHERE txn_date >= (CURRENT_DATE - INTERVAL '30 days')
        """
    )
    amt_30, n_30 = cur.fetchone()
    lines.append(
        f"- **Payments (last 30 days, txn_date):** {n_30} rows, **SUM(total_amount)** ≈ {amt_30}"
    )
    cur.execute(
        """
        SELECT COUNT(*) FROM public.payments
        WHERE txn_date IS NULL
        """
    )
    n_null = cur.fetchone()[0]
    if n_null:
        lines.append(
            f"- **Payments with NULL txn_date:** {n_null} (excluded from the 30-day window above)"
        )
    return lines


def _pack_unpaid_totals(cur: Cursor) -> list[str]:
    lines: list[str] = []
    cur.execute("SELECT COUNT(*) FROM public.invoices WHERE balance > 0")
    unpaid_n = cur.fetchone()[0]
    cur.execute(
        "SELECT COALESCE(SUM(balance), 0) FROM public.invoices WHERE balance > 0"
    )
    unpaid_amt = cur.fetchone()[0]
    lines.append(
        f"- **Unpaid invoices:** {unpaid_n} (open balance sum ≈ {unpaid_amt})"
    )
    cur.execute(
        "SELECT COUNT(*) FROM public.invoices WHERE balance = 0 OR balance IS NULL"
    )
    lines.append(
        f"- **Invoices with zero balance (likely paid in full):** {cur.fetchone()[0]}"
    )
    return lines


def _pack_email_status(cur: Cursor) -> list[str]:
    lines: list[str] = []
    cur.execute(
        "SELECT COUNT(*) FROM public.invoices WHERE is_email_sent = true"
    )
    lines.append(f"- **Invoices emailed (is_email_sent):** {cur.fetchone()[0]}")
    cur.execute(
        "SELECT COUNT(*) FROM public.invoices WHERE is_email_sent = false"
    )
    lines.append(f"- **Invoices not marked email-sent:** {cur.fetchone()[0]}")
    return lines


def _pack_customers_owing(cur: Cursor) -> list[str]:
    lines: list[str] = []
    cur.execute(
        """
        SELECT COUNT(*) FROM public.customers
        WHERE balance > 0
        """
    )
    lines.append(f"- **Customers with positive balance:** {cur.fetchone()[0]}")
    cur.execute(
        """
        SELECT COALESCE(display_name, company_name, qbo_id), balance
        FROM public.customers
        WHERE balance > 0
        ORDER BY balance DESC
        LIMIT 8
        """
    )
    rows = cur.fetchall()
    if rows:
        lines.append("\n**Top open balances (customers):**")
        for name, bal in rows:
            lines.append(f"  - {name}: {bal}")
    return lines


def _pack_sample_open_invoices(cur: Cursor) -> list[str]:
    lines: list[str] = []
    cur.execute(
        """
        SELECT i.doc_number, i.balance, i.due_date, i.is_email_sent,
               COALESCE(c.display_name, c.company_name, c.qbo_id)
        FROM public.invoices i
        LEFT JOIN public.customers c ON c.id = i.customer_id
        WHERE i.balance > 0
        ORDER BY i.balance DESC
        LIMIT 12
        """
    )
    inv_open = cur.fetchall()
    if inv_open:
        lines.append("\n**Sample open invoices (highest balance first):**")
        for doc, bal, due, sent, cust in inv_open:
            lines.append(
                f"  - Inv {doc or '?'} | {cust or '?'} | balance {bal} | due {due} | emailed {sent}"
            )
    return lines


def _pack_sample_unpaid_unsent(cur: Cursor) -> list[str]:
    lines: list[str] = []
    cur.execute(
        """
        SELECT i.doc_number, i.balance, i.due_date,
               COALESCE(c.display_name, c.company_name, c.qbo_id)
        FROM public.invoices i
        LEFT JOIN public.customers c ON c.id = i.customer_id
        WHERE i.is_email_sent = false AND i.balance > 0
        ORDER BY i.due_date NULLS LAST
        LIMIT 12
        """
    )
    inv_unsent = cur.fetchall()
    if inv_unsent:
        lines.append("\n**Sample unpaid + not email-sent:**")
        for doc, bal, due, cust in inv_unsent:
            lines.append(
                f"  - Inv {doc or '?'} | {cust or '?'} | balance {bal} | due {due}"
            )
    return lines


_PACK_REGISTRY: dict[str, PackFn] = {
    "counts_basic": _pack_counts_basic,
    "payments_summary": _pack_payments_summary,
    "unpaid_totals": _pack_unpaid_totals,
    "email_status": _pack_email_status,
    "customers_owing": _pack_customers_owing,
    "sample_open_invoices": _pack_sample_open_invoices,
    "sample_unpaid_unsent": _pack_sample_unpaid_unsent,
}


def fetch_warehouse_summary(
    database_url: str,
    pack_ids: Iterable[str] | None = None,
) -> str:
    """Build a text summary. If pack_ids is None, run every pack (legacy one-shot)."""
    if pack_ids is None:
        wanted: frozenset[str] = ALL_PACK_IDS
    else:
        wanted = frozenset(pack_ids) & ALL_PACK_IDS
        if not wanted:
            wanted = ALL_PACK_IDS
    logger.info(
        "warehouse_snapshot_fetch_started",
        selected_pack_count=len(wanted),
        selected_packs=",".join(sorted(wanted)),
    )

    with pooled_connection(database_url) as conn:
        with conn.cursor() as cur:
            lines: list[str] = ["## Warehouse summary (exact SQL)\n"]
            for pid in PACK_ORDER:
                if pid in wanted:
                    logger.info("warehouse_snapshot_pack_started", pack_id=pid)
                    chunk = _PACK_REGISTRY[pid](cur)
                    if chunk:
                        lines.extend(chunk)
                    logger.info(
                        "warehouse_snapshot_pack_completed",
                        pack_id=pid,
                        line_count=len(chunk),
                    )

    logger.info("warehouse_snapshot_fetch_completed")
    return "\n".join(lines)
