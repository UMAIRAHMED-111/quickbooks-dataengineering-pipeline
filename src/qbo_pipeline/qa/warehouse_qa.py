"""Natural-language Q&A over the Postgres warehouse: optional generated SQL + snapshot packs + LLM."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

from google.genai.errors import APIError

from qbo_pipeline.config import WarehouseQaConfig
from qbo_pipeline.qa.dynamic_sql import (
    SCHEMA_FOR_LLM,
    execute_validated_select,
    format_result_for_llm,
    validate_readonly_select,
)
from qbo_pipeline.qa.llm_complete import complete_qa_llm
from qbo_pipeline.warehouse.sql_snapshot import (
    ALL_PACK_IDS,
    PACK_DESCRIPTIONS,
    fetch_warehouse_summary,
)

_PLANNER_SYSTEM = """You pick which read-only SQL summary sections are needed to answer a QuickBooks warehouse question.

Output rules:
- Reply with ONLY a JSON array of string ids. Example: ["counts_basic","unpaid_totals"]
- No markdown, no explanation, no code fences.
- Choose from the ids listed in the user message only. Omit sections that are clearly irrelevant to save tokens.
- For any question about money owed, unpaid bills, balances, or who owes: include "unpaid_totals" and usually "customers_owing"; add "sample_open_invoices" if they need names/examples.
- For payments, cash received, payment volume, totals paid, or last 7/30/90 days payment amounts: include "payments_summary".
- For email / sent invoice questions: include "email_status" and often "sample_unpaid_unsent".
- For vague or broad questions ("how is the business", "summary"): include all ids from the list.

Note: "counts_basic" is always added by the server; you may omit it from your array. When unsure, include more packs."""

_ANSWER_SYSTEM = """You are a concise business analyst for QuickBooks data in a SQL warehouse.

Rules:
1. Use **WAREHOUSE_SNAPSHOT** only — exact aggregates and sample rows from live SQL.
2. **Structure:** One short opening sentence on its own line (the key takeaway). Then a blank line. Then supporting facts as bullet lines, each starting with "- " (dash space). Use **bold** only for important numbers and entity names inside bullets or the opening line.
3. If something is not in the snapshot, say it is not in this sync.
4. Do not invent numbers or entities that are not in WAREHOUSE_SNAPSHOT.
"""

_SQL_GEN_SYSTEM = """You write one PostgreSQL SELECT for analytics (read-only).

Output rules:
- Output ONLY the SQL. No markdown fences, no explanation, no trailing semicolon.
- Single SELECT (WITH … CTEs allowed). Only base tables in public: customers, invoices, payments, payment_invoice_allocations, sync_runs.
- Join: invoices.customer_id = customers.id; payment_invoice_allocations links payments and invoices.
- Prefer clear column names. If the answer could be many detail rows, add LIMIT 200. Pure aggregates (COUNT/SUM) do not need LIMIT.
"""

_ANSWER_FROM_SQL_SYSTEM = """You summarize QUERY_RESULT for a business user.

Rules:
1. Every number and name must come from QUERY_RESULT; do not invent data.
2. If there are no rows, say the query returned no rows.
3. **Structure:** One short opening sentence (takeaway), blank line, then "- " bullets for details. Use **bold** sparingly for key figures and names.
"""


def _generate_sql(cfg: WarehouseQaConfig, question: str) -> str:
    user = f"Schema:\n{SCHEMA_FOR_LLM}\n\nQuestion: {question}\n\nSQL only:"
    return complete_qa_llm(
        cfg,
        task="sql_generate",
        system_instruction=_SQL_GEN_SYSTEM,
        user_content=user,
        temperature=0.0,
        max_output_tokens=512,
    )


def _answer_via_dynamic_sql(cfg: WarehouseQaConfig, question: str) -> str:
    raw = _generate_sql(cfg, question)
    sql = validate_readonly_select(raw)
    cols, rows, truncated = execute_validated_select(cfg.database_url, sql)
    block = format_result_for_llm(cols, rows, sql, truncated=truncated)
    user = f"Question: {question}\n\n--- QUERY_RESULT ---\n{block}\n"
    return complete_qa_llm(
        cfg,
        task="answer_from_sql",
        system_instruction=_ANSWER_FROM_SQL_SYSTEM,
        user_content=user,
        temperature=0.2,
        max_output_tokens=900,
    )


def _dynamic_sql_fallback_exc(exc: Exception) -> None:
    msg = str(exc).replace("\n", " ")
    if len(msg) > 200:
        msg = msg[:197] + "..."
    print(
        f"Note: dynamic SQL failed ({type(exc).__name__}: {msg}); using preset snapshot packs.",
        file=sys.stderr,
    )
    if os.getenv("WAREHOUSE_QA_VERBOSE", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        import traceback

        traceback.print_exc()


def _catalog_lines_for_planner() -> str:
    lines: list[str] = ["Allowed pack ids (pick a subset):"]
    for pid, desc in PACK_DESCRIPTIONS:
        lines.append(f'- "{pid}": {desc}')
    return "\n".join(lines)


def _parse_pack_list(raw: str) -> frozenset[str]:
    """Extract JSON array of pack ids from model output."""
    t = (raw or "").strip()
    if not t:
        return ALL_PACK_IDS
    low = t.lower()
    if low.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```\s*$", "", t).strip()
    try:
        data = json.loads(t)
    except json.JSONDecodeError:
        m = re.search(r"\[[\s\S]*?\]", t)
        if not m:
            return ALL_PACK_IDS
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError:
            return ALL_PACK_IDS
    if not isinstance(data, list):
        return ALL_PACK_IDS
    out: set[str] = set()
    for x in data:
        if isinstance(x, str) and x in ALL_PACK_IDS:
            out.add(x)
    return frozenset(out) if out else ALL_PACK_IDS


def plan_snapshot_packs(cfg: WarehouseQaConfig, question: str) -> frozenset[str]:
    catalog = _catalog_lines_for_planner()
    user = f"{catalog}\n\nQuestion: {question}\n\nJSON array of pack ids only:"
    raw = complete_qa_llm(
        cfg,
        task="planner",
        system_instruction=_PLANNER_SYSTEM,
        user_content=user,
        temperature=0.0,
        max_output_tokens=256,
    )
    packs = _parse_pack_list(raw)
    return frozenset(packs | {"counts_basic"})


def answer_question(cfg: WarehouseQaConfig, question: str) -> str:
    if cfg.use_dynamic_sql:
        try:
            return _answer_via_dynamic_sql(cfg, question)
        except APIError:
            raise
        except Exception as exc:
            _dynamic_sql_fallback_exc(exc)

    if cfg.use_snapshot_planner:
        packs = plan_snapshot_packs(cfg, question)
    else:
        packs = ALL_PACK_IDS

    summary = fetch_warehouse_summary(cfg.database_url, packs)
    user_content = (
        f"Question: {question}\n\n--- WAREHOUSE_SNAPSHOT ---\n{summary}\n"
    )
    return complete_qa_llm(
        cfg,
        task="answer_snapshot",
        system_instruction=_ANSWER_SYSTEM,
        user_content=user_content,
        temperature=0.2,
        max_output_tokens=900,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ask questions in plain English (OpenAI gpt-4 first, then Gemini fallback).",
    )
    parser.add_argument(
        "question",
        nargs="+",
        help="Your question, e.g. How many unpaid invoices are there?",
    )
    args = parser.parse_args(argv)
    question = " ".join(args.question).strip()
    if not question:
        print("Pass a non-empty question.", file=sys.stderr)
        return 1

    try:
        cfg = WarehouseQaConfig.from_env()
    except RuntimeError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 1

    try:
        print(answer_question(cfg, question))
    except APIError as exc:
        if exc.code == 429:
            print(
                "Q&A failed: Gemini quota or rate limit (429) after automatic retries.\n"
                "  (OpenAI keys were tried first; failure may be on Gemini fallback.)\n"
                "  Each Gemini call retries 429 up to GEMINI_MAX_RETRIES (default 3).\n"
                "  Fix: wait and run again; check OpenAI/Gemini limits.\n"
                "  Tip: set WAREHOUSE_QA_NO_PLANNER=1 to use one fewer API call per question when using snapshot packs.\n",
                file=sys.stderr,
            )
        print(f"  {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Q&A failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
