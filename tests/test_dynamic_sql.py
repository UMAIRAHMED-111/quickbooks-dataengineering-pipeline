import pytest

from qbo_pipeline.qa.dynamic_sql import validate_readonly_select


def test_accepts_simple_select():
    sql = "SELECT COUNT(*) AS n FROM invoices WHERE balance > 0"
    assert validate_readonly_select(sql).lower().startswith("select")


def test_accepts_cte():
    sql = """
    WITH t AS (SELECT id FROM invoices LIMIT 1)
    SELECT * FROM t
    """.strip()
    validate_readonly_select(sql)


def test_rejects_disallowed_table():
    with pytest.raises(ValueError, match="not allowed"):
        validate_readonly_select("SELECT * FROM pg_user")


def test_rejects_delete():
    with pytest.raises(ValueError, match="Forbidden"):
        validate_readonly_select("DELETE FROM invoices")


def test_strips_fences():
    sql = validate_readonly_select("```sql\nSELECT 1 FROM customers\n```")
    assert "customers" in sql.lower()
