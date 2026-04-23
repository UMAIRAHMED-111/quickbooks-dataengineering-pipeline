"""Data quality checks before warehouse writes."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from qbo_pipeline.etl.transform import LoadBundle


@dataclass(frozen=True)
class _TableSchema:
    name: str
    required_fields: tuple[str, ...]
    field_types: dict[str, tuple[type, ...]]


_QBO_TABLE_SCHEMAS: tuple[_TableSchema, ...] = (
    _TableSchema(
        name="customers",
        required_fields=("id", "qbo_id"),
        field_types={"id": (str,), "qbo_id": (str,)},
    ),
    _TableSchema(
        name="invoices",
        required_fields=("id", "qbo_id", "customer_id"),
        field_types={"id": (str,), "qbo_id": (str,), "customer_id": (str,)},
    ),
    _TableSchema(
        name="payments",
        required_fields=("id", "qbo_id", "customer_id"),
        field_types={"id": (str,), "qbo_id": (str,), "customer_id": (str,)},
    ),
    _TableSchema(
        name="payment_invoice_allocations",
        required_fields=("id", "payment_id", "invoice_id", "amount"),
        field_types={
            "id": (str,),
            "payment_id": (str,),
            "invoice_id": (str,),
            "amount": (int, float),
        },
    ),
)

def _validate_required_and_types(rows: list[dict[str, Any]], schema: _TableSchema) -> None:
    for idx, row in enumerate(rows):
        for field in schema.required_fields:
            value = row.get(field)
            expected_types = schema.field_types.get(field)
            if value is None:
                raise ValueError(f"{schema.name}[{idx}] missing required field `{field}`")
            if field != "amount" and isinstance(value, str) and not value.strip():
                raise ValueError(f"{schema.name}[{idx}] missing required field `{field}`")
            if expected_types and not isinstance(value, expected_types):
                expected = ", ".join(t.__name__ for t in expected_types)
                raise ValueError(
                    f"{schema.name}[{idx}] field `{field}` has invalid type "
                    f"{type(value).__name__}; expected {expected}"
                )


def _validate_unique_qbo_ids(rows: list[dict[str, Any]], table_name: str) -> None:
    qbo_ids = [str(row.get("qbo_id", "")).strip() for row in rows]
    dups = [qid for qid, count in Counter(qbo_ids).items() if qid and count > 1]
    if dups:
        sample = ", ".join(sorted(dups)[:5])
        raise ValueError(
            f"Duplicate `{table_name}.qbo_id` values in payload: {sample}"
        )


def _validate_referential_integrity(bundle: LoadBundle) -> None:
    customer_ids = {str(row["id"]) for row in bundle.customers if row.get("id")}
    invoice_ids = {str(row["id"]) for row in bundle.invoices if row.get("id")}
    payment_ids = {str(row["id"]) for row in bundle.payments if row.get("id")}

    missing_invoice_customers = sorted(
        {
            str(row.get("customer_id"))
            for row in bundle.invoices
            if row.get("customer_id") and str(row.get("customer_id")) not in customer_ids
        }
    )
    if missing_invoice_customers:
        sample = ", ".join(missing_invoice_customers[:5])
        raise ValueError(f"Invoice customer_id values missing in customers: {sample}")

    missing_payment_customers = sorted(
        {
            str(row.get("customer_id"))
            for row in bundle.payments
            if row.get("customer_id") and str(row.get("customer_id")) not in customer_ids
        }
    )
    if missing_payment_customers:
        sample = ", ".join(missing_payment_customers[:5])
        raise ValueError(f"Payment customer_id values missing in customers: {sample}")

    missing_alloc_refs = sorted(
        {
            f"payment_id={row.get('payment_id')} invoice_id={row.get('invoice_id')}"
            for row in bundle.payment_invoice_allocations
            if row.get("payment_id") not in payment_ids
            or row.get("invoice_id") not in invoice_ids
        }
    )
    if missing_alloc_refs:
        sample = ", ".join(missing_alloc_refs[:5])
        raise ValueError(f"Allocation references missing parent records: {sample}")


def _parse_iso_date(value: Any) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Expected ISO date string, got {type(value).__name__}")
    return date.fromisoformat(value[:10])


def _parse_iso_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Expected ISO timestamp string, got {type(value).__name__}")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _validate_non_negative_amounts(bundle: LoadBundle) -> None:
    for idx, row in enumerate(bundle.customers):
        for field in ("balance", "balance_with_jobs"):
            value = row.get(field)
            if value is not None and value < 0:
                raise ValueError(f"customers[{idx}] `{field}` cannot be negative")

    for idx, row in enumerate(bundle.invoices):
        for field in ("total_amount", "balance"):
            value = row.get(field)
            if value is not None and value < 0:
                raise ValueError(f"invoices[{idx}] `{field}` cannot be negative")

    for idx, row in enumerate(bundle.payments):
        for field in ("total_amount", "unapplied_amount"):
            value = row.get(field)
            if value is not None and value < 0:
                raise ValueError(f"payments[{idx}] `{field}` cannot be negative")

    for idx, row in enumerate(bundle.payment_invoice_allocations):
        value = row.get("amount")
        if value is not None and value < 0:
            raise ValueError(
                f"payment_invoice_allocations[{idx}] `amount` cannot be negative"
            )


def _validate_dates_not_in_future(bundle: LoadBundle) -> None:
    today = datetime.now(UTC).date()
    now_utc = datetime.now(UTC)

    for idx, row in enumerate(bundle.invoices):
        txn_date = _parse_iso_date(row.get("txn_date"))
        due_date = _parse_iso_date(row.get("due_date"))
        if txn_date and txn_date > today:
            raise ValueError(f"invoices[{idx}] `txn_date` cannot be in the future")
        if due_date and due_date > today:
            raise ValueError(f"invoices[{idx}] `due_date` cannot be in the future")
        if txn_date and due_date and due_date < txn_date:
            raise ValueError(
                f"invoices[{idx}] `due_date` cannot be earlier than `txn_date`"
            )

    for idx, row in enumerate(bundle.payments):
        txn_date = _parse_iso_date(row.get("txn_date"))
        if txn_date and txn_date > today:
            raise ValueError(f"payments[{idx}] `txn_date` cannot be in the future")

    for idx, row in enumerate(bundle.customers):
        for field in ("qbo_create_time", "qbo_last_updated_time"):
            ts = _parse_iso_timestamp(row.get(field))
            if ts and ts > now_utc:
                raise ValueError(f"customers[{idx}] `{field}` cannot be in the future")

    for idx, row in enumerate(bundle.invoices):
        for field in ("qbo_create_time", "qbo_last_updated_time"):
            ts = _parse_iso_timestamp(row.get(field))
            if ts and ts > now_utc:
                raise ValueError(f"invoices[{idx}] `{field}` cannot be in the future")

    for idx, row in enumerate(bundle.payments):
        for field in ("qbo_create_time", "qbo_last_updated_time"):
            ts = _parse_iso_timestamp(row.get(field))
            if ts and ts > now_utc:
                raise ValueError(f"payments[{idx}] `{field}` cannot be in the future")


def _validate_cross_row_consistency(bundle: LoadBundle) -> None:
    payment_total_by_id = {
        str(row["id"]): float(row.get("total_amount", 0) or 0) for row in bundle.payments
    }
    invoice_total_by_id = {
        str(row["id"]): float(row.get("total_amount", 0) or 0) for row in bundle.invoices
    }

    alloc_sum_by_payment: dict[str, float] = {}
    alloc_sum_by_invoice: dict[str, float] = {}
    for row in bundle.payment_invoice_allocations:
        payment_id = str(row["payment_id"])
        invoice_id = str(row["invoice_id"])
        amount = float(row["amount"])
        alloc_sum_by_payment[payment_id] = alloc_sum_by_payment.get(payment_id, 0.0) + amount
        alloc_sum_by_invoice[invoice_id] = alloc_sum_by_invoice.get(invoice_id, 0.0) + amount

    for payment_id, allocated in alloc_sum_by_payment.items():
        payment_total = payment_total_by_id.get(payment_id, 0.0)
        if allocated > payment_total + 1e-6:
            raise ValueError(
                f"Allocated amount {allocated:.6f} exceeds payment total "
                f"{payment_total:.6f} for payment_id={payment_id}"
            )

    for invoice_id, allocated in alloc_sum_by_invoice.items():
        invoice_total = invoice_total_by_id.get(invoice_id, 0.0)
        if allocated > invoice_total + 1e-6:
            raise ValueError(
                f"Allocated amount {allocated:.6f} exceeds invoice total "
                f"{invoice_total:.6f} for invoice_id={invoice_id}"
            )


def validate_technical(bundle: LoadBundle) -> None:
    """Raise ``ValueError`` when technical data quality checks fail."""
    table_rows: dict[str, list[dict[str, Any]]] = {
        "customers": bundle.customers,
        "invoices": bundle.invoices,
        "payments": bundle.payments,
        "payment_invoice_allocations": bundle.payment_invoice_allocations,
    }

    for schema in _QBO_TABLE_SCHEMAS:
        rows = table_rows[schema.name]
        _validate_required_and_types(rows, schema)

    _validate_unique_qbo_ids(bundle.customers, "customers")
    _validate_unique_qbo_ids(bundle.invoices, "invoices")
    _validate_unique_qbo_ids(bundle.payments, "payments")
    _validate_referential_integrity(bundle)


def validate_business(bundle: LoadBundle) -> None:
    """Raise ``ValueError`` when business data quality checks fail."""
    _validate_non_negative_amounts(bundle)
    _validate_dates_not_in_future(bundle)
    _validate_cross_row_consistency(bundle)


def validate_bundle(bundle: LoadBundle) -> None:
    """Validate technical integrity first, then business rules."""
    validate_technical(bundle)
    validate_business(bundle)
