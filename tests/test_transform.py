import json
from pathlib import Path

import pytest

from qbo_pipeline.etl.transform import transform

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "response.json"


@pytest.mark.skipif(not FIXTURE.is_file(), reason="data/response.json not present")
def test_transform_sample_fixture_counts_and_email_sent_flag():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    bundle = transform(payload)

    assert len(bundle.customers) >= 1
    assert len(bundle.invoices) >= 1
    assert len(bundle.payments) >= 1
    allowed = {"sent", "emailsent", "needtosend"}
    for inv in bundle.invoices:
        assert "is_email_sent" in inv
        es = inv.get("email_status")
        if es and str(es).strip().lower() in allowed:
            assert inv["is_email_sent"] is True
        else:
            assert inv["is_email_sent"] is False

    cust_uuids = {c["id"] for c in bundle.customers}
    for inv in bundle.invoices:
        assert inv["customer_id"] in cust_uuids

    pay_ids = {r["id"] for r in bundle.payments}
    inv_ids = {r["id"] for r in bundle.invoices}
    for a in bundle.payment_invoice_allocations:
        assert a["payment_id"] in pay_ids
        assert a["invoice_id"] in inv_ids


def test_transform_emailsent_and_case_insensitive(monkeypatch):
    monkeypatch.setenv("QBO_IS_EMAIL_SENT_STATUSES", "Sent,EmailSent")
    payload = {
        "customers": [
            {
                "Id": "1",
                "DisplayName": "Cust",
                "Balance": 0,
                "BalanceWithJobs": 0,
                "MetaData": {"CreateTime": "2025-01-01T00:00:00-00:00"},
            }
        ],
        "invoices": [
            {
                "Id": "10",
                "CustomerRef": {"value": "1"},
                "TxnDate": "2025-01-15",
                "TotalAmt": 1,
                "Balance": 1,
                "EmailStatus": "EmailSent",
                "MetaData": {"CreateTime": "2025-01-01T00:00:00-00:00"},
            },
            {
                "Id": "11",
                "CustomerRef": {"value": "1"},
                "TxnDate": "2025-01-16",
                "TotalAmt": 2,
                "Balance": 2,
                "EmailStatus": "SENT",
                "MetaData": {"CreateTime": "2025-01-01T00:00:00-00:00"},
            },
            {
                "Id": "12",
                "CustomerRef": {"value": "1"},
                "TxnDate": "2025-01-17",
                "TotalAmt": 3,
                "Balance": 3,
                "EmailStatus": "NotSet",
                "MetaData": {"CreateTime": "2025-01-01T00:00:00-00:00"},
            },
            {
                "Id": "13",
                "CustomerRef": {"value": "1"},
                "TxnDate": "2025-01-18",
                "TotalAmt": 4,
                "Balance": 4,
                "EmailStatus": "NeedToSend",
                "MetaData": {"CreateTime": "2025-01-01T00:00:00-00:00"},
            },
        ],
        "payments": [],
    }
    bundle = transform(payload)
    by_qbo = {i["qbo_id"]: i for i in bundle.invoices}
    assert by_qbo["10"]["is_email_sent"] is True
    assert by_qbo["11"]["is_email_sent"] is True
    assert by_qbo["12"]["is_email_sent"] is False
    assert by_qbo["13"]["is_email_sent"] is False


def test_needtosend_is_sent_under_default_env(monkeypatch):
    monkeypatch.delenv("QBO_IS_EMAIL_SENT_STATUSES", raising=False)
    payload = {
        "customers": [
            {
                "Id": "1",
                "DisplayName": "Cust",
                "Balance": 0,
                "BalanceWithJobs": 0,
                "MetaData": {"CreateTime": "2025-01-01T00:00:00-00:00"},
            }
        ],
        "invoices": [
            {
                "Id": "20",
                "CustomerRef": {"value": "1"},
                "TxnDate": "2025-01-15",
                "TotalAmt": 1,
                "Balance": 1,
                "EmailStatus": "NeedToSend",
                "DeliveryInfo": {"DeliveryType": "Email"},
                "MetaData": {"CreateTime": "2025-01-01T00:00:00-00:00"},
            },
        ],
        "payments": [],
    }
    bundle = transform(payload)
    assert bundle.invoices[0]["is_email_sent"] is True
