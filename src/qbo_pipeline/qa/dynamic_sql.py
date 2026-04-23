"""LLM-proposed SELECT queries: validate (read-only + table allowlist), execute, format for answering."""

from __future__ import annotations

import re
from typing import Any

import sqlglot
from sqlglot import exp

from qbo_pipeline.db.pool import pooled_connection
from qbo_pipeline.observability import get_logger

ALLOWED_TABLES = frozenset(
    {
        "customers",
        "invoices",
        "payments",
        "payment_invoice_allocations",
        "sync_runs",
    }
)

MAX_ROWS_RETURNED = 500
STATEMENT_TIMEOUT_MS = 15_000
logger = get_logger(__name__)

# Compact schema hint for the SQL generator (keep aligned with migration).
SCHEMA_FOR_LLM = """
public schema only. Tables:

customers(id uuid, qbo_id text, display_name text, company_name text, balance numeric, ...)
invoices(id uuid, qbo_id text, customer_id uuid -> customers.id, doc_number text, txn_date date,
  due_date date, total_amount numeric, balance numeric, is_email_sent boolean, email_status text, ...)
payments(id uuid, qbo_id text, customer_id uuid -> customers.id, txn_date date, total_amount numeric, ...)
  This-month payments (copy this pattern; use alias p and keep p in FROM):
  SELECT COALESCE(SUM(p.total_amount), 0) AS total, COUNT(*) AS n FROM public.payments p
  WHERE p.txn_date >= date_trunc('month', CURRENT_DATE)::date AND p.txn_date <= CURRENT_DATE
payment_invoice_allocations(id uuid, payment_id uuid -> payments.id, invoice_id uuid -> invoices.id, amount numeric)
sync_runs(id uuid, started_at timestamptz, status text, customer_count int, invoice_count int, ...)
""".strip()

_FORBIDDEN_EXPRESSION_TYPES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Create,
    exp.Alter,
    exp.TruncateTable,
    exp.Merge,
    exp.Copy,
    exp.Command,
    exp.Commit,
    exp.Rollback,
    exp.Transaction,
    exp.Describe,
)


def _strip_sql_fences(raw: str) -> str:
    t = (raw or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```\s*$", "", t).strip()
    return t.strip().rstrip(";")


def _collect_cte_aliases(expr: exp.Expression) -> set[str]:
    names: set[str] = set()
    for with_ in expr.find_all(exp.With):
        for e in with_.expressions:
            if isinstance(e, exp.CTE) and e.alias:
                names.add(str(e.alias))
    return names


def validate_readonly_select(sql: str) -> str:
    """Parse SQL; allow only SELECT / WITH … SELECT; base tables must be in ALLOWED_TABLES (public)."""
    text = _strip_sql_fences(sql)
    logger.info("dynamic_sql_validate_started", sql_length=len(text))
    if not text:
        raise ValueError("Model returned empty SQL")
    if ";" in text.rstrip().rstrip(";"):
        raise ValueError("Multiple statements are not allowed")

    try:
        parsed = sqlglot.parse_one(text, dialect="postgres")
    except Exception as exc:
        raise ValueError(f"Invalid SQL: {exc}") from exc

    root = parsed
    for node in root.walk():
        if isinstance(node, _FORBIDDEN_EXPRESSION_TYPES):
            raise ValueError(f"Forbidden operation: {type(node).__name__}")

    if isinstance(root, exp.With):
        inner = root.this
        if not isinstance(inner, (exp.Select, exp.Union)):
            raise ValueError("WITH must wrap SELECT")
    elif isinstance(root, (exp.Select, exp.Union)):
        pass
    else:
        raise ValueError("Only SELECT (optional WITH / UNION) is allowed")

    cte_aliases = _collect_cte_aliases(root)

    for node in root.find_all(exp.Table):
        if not node.name:
            continue
        if isinstance(node.this, exp.Func):
            raise ValueError("Table sources must be named tables or CTEs")
        schema = (node.db or "").lower()
        if schema and schema != "public":
            raise ValueError(f"Schema not allowed: {node.db}")
        base = node.name
        if str(base) in cte_aliases:
            continue
        if str(base).lower() not in ALLOWED_TABLES:
            raise ValueError(f"Table not allowed: {base}")

    logger.info("dynamic_sql_validate_completed")
    return text


def execute_validated_select(
    database_url: str,
    sql: str,
    *,
    max_rows: int = MAX_ROWS_RETURNED,
    timeout_ms: int = STATEMENT_TIMEOUT_MS,
) -> tuple[list[str], list[tuple[Any, ...]], bool]:
    """
    Run validated SQL. Returns (column_names, rows, truncated).
    """
    truncated = False
    logger.info(
        "dynamic_sql_execute_started",
        max_rows=max_rows,
        timeout_ms=timeout_ms,
    )
    with pooled_connection(database_url, autocommit=True) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = %s", (int(timeout_ms),))
            cur.execute(sql)
            colnames = [d[0] for d in (cur.description or ())]
            rows = cur.fetchmany(max_rows + 1)
            if len(rows) > max_rows:
                truncated = True
                rows = rows[:max_rows]
    logger.info(
        "dynamic_sql_execute_completed",
        column_count=len(colnames),
        row_count=len(rows),
        truncated=truncated,
    )
    return colnames, rows, truncated


def format_result_for_llm(
    colnames: list[str],
    rows: list[tuple[Any, ...]],
    sql: str,
    *,
    truncated: bool,
) -> str:
    """Compact tabular text for the answer model."""
    lines: list[str] = [
        f"Executed SQL:\n{sql}\n",
        f"Rows: {len(rows)}" + (" (truncated to limit)" if truncated else ""),
        "",
    ]
    if not colnames:
        lines.append("(no columns)")
        return "\n".join(lines)
    lines.append(" | ".join(colnames))
    for tup in rows:
        lines.append(" | ".join(str(v) if v is not None else "" for v in tup))
    if len(rows) == 0:
        lines.append("")
        lines.append(
            "NOTE: Zero data rows matched this SQL. When answering the user: do NOT mention "
            '"query", "SQL", "rows", or "empty result". Say clearly that no matching payments/invoices '
            "were found in the warehouse for that filter or time range, and suggest trying a broader "
            "period (e.g. all time or last 30 days) if helpful."
        )
    return "\n".join(lines)
