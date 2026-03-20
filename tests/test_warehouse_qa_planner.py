"""Tests for snapshot pack planning helpers (no DB, no API)."""

from qbo_pipeline.qa.warehouse_qa import _parse_pack_list
from qbo_pipeline.warehouse.sql_snapshot import ALL_PACK_IDS


def test_parse_pack_list_plain_json():
    assert _parse_pack_list('["unpaid_totals", "email_status"]') == frozenset(
        {"unpaid_totals", "email_status"}
    )


def test_parse_pack_list_fenced():
    raw = '```json\n["counts_basic", "sample_open_invoices"]\n```'
    assert _parse_pack_list(raw) == frozenset(
        {"counts_basic", "sample_open_invoices"}
    )


def test_parse_pack_list_invalid_falls_back_to_all():
    assert _parse_pack_list("not json") == ALL_PACK_IDS


def test_parse_pack_list_unknown_ids_dropped():
    # If everything unknown, fallback ALL
    assert _parse_pack_list('["made_up_pack"]') == ALL_PACK_IDS
