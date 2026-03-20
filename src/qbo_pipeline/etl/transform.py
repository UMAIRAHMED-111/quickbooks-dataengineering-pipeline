"""Turn raw QBO JSON lists into flat rows + FK maps for Postgres.

Skips invoices/payments whose CustomerRef is missing from the customer list (bad data / timing).
"""

from __future__ import annotations

import os
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any


@dataclass
class LoadBundle:
    customers: list[dict[str, Any]]
    invoices: list[dict[str, Any]]
    payments: list[dict[str, Any]]
    payment_invoice_allocations: list[dict[str, Any]]


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    # QBO sometimes uses Z; fromisoformat wants +00:00
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value[:10])


def _meta_time_to_str(meta: dict[str, Any], key: str) -> str | None:
    """Shared helper for QBO MetaData timestamps (CreateTime / LastUpdatedTime)."""
    raw = meta.get(key)
    if not raw:
        return None
    ts = _parse_ts(raw)
    return ts.isoformat() if ts else None


def _dec(val: Any) -> Decimal:
    if val is None:
        return Decimal("0")
    return Decimal(str(val))


def _allowed_email_sent_statuses() -> frozenset[str]:
    """
    Counts as emailed for ``is_email_sent``: ``Sent``, ``EmailSent``, and ``NeedToSend``
    (invoices queued for email / delivery type email in QBO).

    Override with env ``QBO_IS_EMAIL_SENT_STATUSES`` (comma-separated, case-insensitive).
    """
    raw = os.getenv(
        "QBO_IS_EMAIL_SENT_STATUSES", "Sent,EmailSent,NeedToSend"
    ).strip()
    if not raw:
        return frozenset()
    return frozenset(p.strip().lower() for p in raw.split(",") if p.strip())


def _invoice_email_sent(email_status: Any) -> bool:
    if email_status is None:
        return False
    s = str(email_status).strip()
    if not s:
        return False
    return s.lower() in _allowed_email_sent_statuses()


def _addr_blob(addr: dict[str, Any] | None) -> dict[str, Any] | None:
    if not addr:
        return None
    return dict(addr)


def transform(payload: dict[str, Any]) -> LoadBundle:
    raw_customers = payload.get("customers") or []
    raw_invoices = payload.get("invoices") or []
    raw_payments = payload.get("payments") or []

    # Map QuickBooks string Id -> new UUID we will insert as PK
    customer_by_qbo: dict[str, uuid.UUID] = {}
    customer_rows: list[dict[str, Any]] = []

    for c in raw_customers:
        qid = str(c.get("Id", "")).strip()
        if not qid:
            continue
        cid = uuid.uuid4()
        customer_by_qbo[qid] = cid
        meta = c.get("MetaData") or {}
        bill = c.get("BillAddr")
        ship = c.get("ShipAddr")
        customer_rows.append(
            {
                "id": str(cid),
                "qbo_id": qid,
                "display_name": c.get("DisplayName"),
                "company_name": c.get("CompanyName"),
                "given_name": c.get("GivenName"),
                "family_name": c.get("FamilyName"),
                "fully_qualified_name": c.get("FullyQualifiedName"),
                "primary_email": (c.get("PrimaryEmailAddr") or {}).get("Address"),
                "primary_phone": (c.get("PrimaryPhone") or {}).get("FreeFormNumber"),
                "balance": float(_dec(c.get("Balance"))),
                "balance_with_jobs": float(_dec(c.get("BalanceWithJobs"))),
                "currency_code": (c.get("CurrencyRef") or {}).get("value"),
                "active": c.get("Active"),
                "taxable": c.get("Taxable"),
                "bill_address": _addr_blob(bill) if isinstance(bill, dict) else None,
                "ship_address": _addr_blob(ship) if isinstance(ship, dict) else None,
                "qbo_create_time": _meta_time_to_str(meta, "CreateTime"),
                "qbo_last_updated_time": _meta_time_to_str(meta, "LastUpdatedTime"),
            }
        )

    invoice_by_qbo: dict[str, uuid.UUID] = {}
    invoice_rows: list[dict[str, Any]] = []

    for inv in raw_invoices:
        qid = str(inv.get("Id", "")).strip()
        if not qid:
            continue
        cref = inv.get("CustomerRef") or {}
        cust_q = str(cref.get("value", "")).strip()
        cust_id = customer_by_qbo.get(cust_q)
        if cust_id is None:
            # Orphan invoice relative to this payload — skip instead of breaking FK
            continue
        iid = uuid.uuid4()
        invoice_by_qbo[qid] = iid
        meta = inv.get("MetaData") or {}
        email_status = inv.get("EmailStatus")
        invoice_rows.append(
            {
                "id": str(iid),
                "qbo_id": qid,
                "customer_id": str(cust_id),
                "doc_number": inv.get("DocNumber"),
                "txn_date": _parse_date(inv.get("TxnDate")).isoformat()
                if inv.get("TxnDate")
                else None,
                "due_date": _parse_date(inv.get("DueDate")).isoformat()
                if inv.get("DueDate")
                else None,
                "total_amount": float(_dec(inv.get("TotalAmt"))),
                "balance": float(_dec(inv.get("Balance"))),
                "currency_code": (inv.get("CurrencyRef") or {}).get("value"),
                "email_status": email_status,
                "print_status": inv.get("PrintStatus"),
                "is_email_sent": _invoice_email_sent(email_status),
                "bill_email": (inv.get("BillEmail") or {}).get("Address"),
                "qbo_create_time": _meta_time_to_str(meta, "CreateTime"),
                "qbo_last_updated_time": _meta_time_to_str(meta, "LastUpdatedTime"),
            }
        )

    payment_by_qbo: dict[str, uuid.UUID] = {}
    payment_rows: list[dict[str, Any]] = []
    # Same payment can touch the same invoice on multiple lines — sum into one row later
    alloc_sums: defaultdict[tuple[str, str], Decimal] = defaultdict(Decimal)

    for pay in raw_payments:
        pqid = str(pay.get("Id", "")).strip()
        if not pqid:
            continue
        cref = pay.get("CustomerRef") or {}
        cust_q = str(cref.get("value", "")).strip()
        cust_id = customer_by_qbo.get(cust_q)
        if cust_id is None:
            continue
        pid = uuid.uuid4()
        payment_by_qbo[pqid] = pid
        meta = pay.get("MetaData") or {}
        payment_rows.append(
            {
                "id": str(pid),
                "qbo_id": pqid,
                "customer_id": str(cust_id),
                "txn_date": _parse_date(pay.get("TxnDate")).isoformat()
                if pay.get("TxnDate")
                else None,
                "total_amount": float(_dec(pay.get("TotalAmt"))),
                "unapplied_amount": float(_dec(pay.get("UnappliedAmt"))),
                "currency_code": (pay.get("CurrencyRef") or {}).get("value"),
                "qbo_create_time": _meta_time_to_str(meta, "CreateTime"),
                "qbo_last_updated_time": _meta_time_to_str(meta, "LastUpdatedTime"),
            }
        )
        for line in pay.get("Line") or []:
            amt = _dec(line.get("Amount"))
            for link in line.get("LinkedTxn") or []:
                if link.get("TxnType") != "Invoice":
                    continue
                inv_qbo_id = str(link.get("TxnId", "")).strip()
                if inv_qbo_id and amt != Decimal("0"):
                    alloc_sums[(pqid, inv_qbo_id)] += amt

    allocation_rows: list[dict[str, Any]] = []
    for (pqid, iqid), amt in alloc_sums.items():
        pid = payment_by_qbo.get(pqid)
        iid = invoice_by_qbo.get(iqid)
        if pid is None or iid is None:
            # Payment points at an invoice we did not load (e.g. skipped invoice)
            continue
        allocation_rows.append(
            {
                "id": str(uuid.uuid4()),
                "payment_id": str(pid),
                "invoice_id": str(iid),
                "amount": float(amt),
            }
        )

    return LoadBundle(
        customers=customer_rows,
        invoices=invoice_rows,
        payments=payment_rows,
        payment_invoice_allocations=allocation_rows,
    )
