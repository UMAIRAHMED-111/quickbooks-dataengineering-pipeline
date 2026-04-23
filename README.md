# QuickBooks Data Engineering Pipeline

Production-oriented QuickBooks analytics platform built on Python + Supabase Postgres, with:

- deterministic ETL (`extract -> transform -> validate -> load`)
- async API-triggered sync runs
- analytics endpoints for dashboards
- natural-language warehouse Q&A (OpenAI primary, Gemini fallback)
- Airflow DAG orchestration option

---

## 1) What This Repository Contains

- `src/qbo_pipeline/etl`: core data pipeline logic and CLI runtime
- `src/qbo_pipeline/db`: shared Postgres connection pooling
- `src/qbo_pipeline/warehouse`: fixed analytics SQL and snapshot packs
- `src/qbo_pipeline/qa`: Q&A orchestration, prompt logic, dynamic SQL guardrails
- `src/qbo_pipeline/web`: Flask API endpoints
- `airflow/dags`: DAG for scheduled orchestration
- `supabase/migrations`: DDL and schema evolution for warehouse tables
- `tests`: unit/integration-style tests for ETL, API, and QA behavior
- `main.py`, `server.py`, `ask.py`: runnable entrypoints

---

## 2) High-Level Architecture

```text
QuickBooks (via n8n webhook)
  -> ETL Extract (HTTP/local file)
  -> Transform (canonical rows + relationships)
  -> Validate (technical + business checks)
  -> Load (transactional upsert into Postgres)
  -> Analytics API + Q&A over curated warehouse
```

### Sync execution modes

- CLI sync: `python main.py`
- API sync: `POST /api/v1/sync` (returns `202` immediately, runs in background)
- Airflow sync: `qbo_n8n_sync` DAG

---

## 3) Data Model and Warehouse Schema

Schema is managed by:

- `supabase/migrations/001_init.sql`
- `supabase/migrations/002_sync_runs_upsert_counters.sql`

### Core tables

- `public.sync_runs`: run lifecycle (`running/success/failed`) and per-run counters
- `public.customers`: normalized customer entities keyed by `qbo_id`
- `public.invoices`: invoice facts linked to customers
- `public.payments`: payment facts linked to customers
- `public.payment_invoice_allocations`: payment-to-invoice allocations

### Key guarantees

- Upsert strategy by `qbo_id` for idempotent reruns
- FK-enforced relationships across entities
- freshness-aware update logic via `qbo_last_updated_time`
- per-run observability via `sync_runs` + logs

---

## 4) ETL Workflow (Detailed)

Implemented in `src/qbo_pipeline/etl`.

1. **Extract**
   - Source: `N8N_WEBHOOK_URL` (or local JSON file)
   - Retries for transient HTTP failures (`408/429/5xx`) with backoff
2. **Transform**
   - Converts QuickBooks payloads into canonical warehouse rows
   - Creates stable relationship maps and allocation links
   - Normalizes types (dates/timestamps/numerics/bools)
3. **Validate**
   - Required fields/types, duplicate checks, referential integrity
   - business checks (non-negative amounts, date constraints, allocation consistency)
4. **Load**
   - Transactional upsert across all entities
   - run status/counters persisted in `sync_runs`
   - failure path marks run `failed` with error message

---

## 5) API Layer

Implemented in `src/qbo_pipeline/web/app.py`; entrypoint `python server.py`.

### Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | Service health check |
| `POST /api/v1/sync` | Queue ETL sync; returns `202` and `sync_run_id` |
| `POST /api/v1/qa` | Ask warehouse in natural language |
| `GET /api/v1/metrics/catalog` | List metric endpoints |
| `GET /api/v1/metrics/overview` | Top-level KPI summary |
| `GET /api/v1/metrics/invoices/paid-vs-unpaid` | Paid/unpaid invoice stats |
| `GET /api/v1/metrics/invoices/sent-vs-unsent` | Email sent status buckets |
| `GET /api/v1/metrics/invoices/overdue-vs-current` | Overdue vs current unpaid |
| `GET /api/v1/metrics/invoices/paid-on-time-vs-late` | Settlement timing |
| `GET /api/v1/metrics/customers/top-paying?limit=10` | Highest payment customers |
| `GET /api/v1/metrics/customers/top-outstanding?limit=10` | Highest outstanding balances |
| `GET /api/v1/metrics/customers/top-overdue-debt?limit=10` | Largest past-due debtors |
| `GET /api/v1/metrics/customers/best-on-time-payers?limit=10` | Best on-time payers |
| `GET /api/v1/metrics/payments/by-month` | Monthly payment series |
| `GET /api/v1/metrics/allocations/summary` | Allocation coverage summary |

### Error model (current)

- `400` invalid request payload
- `401` unauthorized sync trigger (when `SYNC_API_SECRET` set)
- `500` internal failures (`database_error`, `sync_failed`, `qa_failed`)
- `502` upstream LLM/provider issue (`llm_error`)
- `503` missing required runtime configuration (`service_unavailable`)

---

## 6) Q&A Subsystem

Implemented in `src/qbo_pipeline/qa/warehouse_qa.py`.

### Two answer paths

- **Snapshot path (default)**:
  - planner selects SQL packs from `warehouse/sql_snapshot.py`
  - LLM answers using precomputed summary text
- **Dynamic SQL path (optional)**:
  - enabled with `WAREHOUSE_QA_DYNAMIC_SQL=1`
  - LLM proposes one SELECT
  - SQL validated with strict read-only guardrails in `qa/dynamic_sql.py`
  - query executed with row/timeout limits
  - on non-provider failure, safely falls back to snapshot packs

### Guardrails

- allowlisted tables only
- read-only SQL AST validation
- statement timeout
- row cap (`MAX_ROWS_RETURNED`)
- strict response shaping for API display

---

## 7) Observability and Logging

Shared logging utility: `src/qbo_pipeline/observability.py`.

- single reusable logger API via `get_logger(...)`
- one-time bootstrap via `configure_logging(service=...)`
- structured event-style logs across ETL/API/QA/warehouse layers
- workflow trace fields include:
  - `sync_run_id`
  - stage boundaries (`*_started`, `*_completed`, `*_failed`)
  - row/entity counts and selected pack/query metadata

Runtime level can be controlled with `LOG_LEVEL` (default `INFO`).

---

## 8) Airflow Orchestration

DAG file: `airflow/dags/qbo_n8n_sync_dag.py`.

- task sequence:
  - `fetch_n8n_json`
  - `warehouse_delete` (starts `sync_runs` record)
  - `warehouse_insert` (transform + validated upsert)
- retries configured
- task timeout and dagrun timeout configured
- catchup disabled (`catchup=False`)
- payload handoff uses temp file path (avoids large XCom payloads)

---

## 9) Configuration Reference

Create a `.env` manually in repo root (no `.env.example` is currently checked in).

### Required for ETL/API sync

- `DATABASE_URL` or `SUPABASE_DB_URL`
- `N8N_WEBHOOK_URL`

### Optional ETL

- `N8N_BASIC_AUTH_USERNAME`
- `N8N_BASIC_AUTH_PASSWORD`
- `N8N_HTTP_TIMEOUT_SECONDS` (default `120`)
- `SUPABASE_INSERT_CHUNK_SIZE` (default `500`)
- `QBO_IS_EMAIL_SENT_STATUSES` (default `Sent,EmailSent,NeedToSend`)

### API runtime

- `PORT` (default `5050`)
- `FLASK_HOST` (default `127.0.0.1`)
- `FLASK_DEBUG` (`1|true|yes` to enable)
- `SYNC_API_SECRET` (protects `POST /api/v1/sync`)

### Q&A required (at least one provider key)

- `OPENAI_API_KEY_1` (primary)
- `OPENAI_API_KEY_2` (optional secondary)
- `GEMINI_API_KEY` or `GOOGLE_API_KEY` (fallback)

### Q&A tuning

- `OPENAI_MODEL`, `OPENAI_PLANNER_MODEL`, `OPENAI_SQL_MODEL`
- `GEMINI_MODEL`, `GEMINI_PLANNER_MODEL`, `GEMINI_SQL_MODEL`
- `WAREHOUSE_QA_DYNAMIC_SQL` (`1` enables dynamic SQL path)
- `WAREHOUSE_QA_NO_PLANNER` (`1` disables snapshot planner)
- `WAREHOUSE_QA_CONTEXT_MAX_CHARS` (default `12000`)
- `WAREHOUSE_QA_CONTEXT_MAX_MESSAGES` (default `24`)
- `WAREHOUSE_QA_VERBOSE` (extra fallback traceback logging)
- `GEMINI_MAX_RETRIES` (429 retry behavior in Gemini path)

### Logging

- `LOG_LEVEL` (`DEBUG|INFO|WARNING|ERROR`, default `INFO`)

---

## 10) Local Development

### Install

```bash
pip install -r requirements.txt
```

Optional editable install:

```bash
pip install -e .
```

Optional extras from `pyproject.toml`:

- dev: `pip install -e ".[dev]"`
- airflow: `pip install -e ".[airflow]"`

### Apply DB migrations

Run SQL files in order against Supabase/Postgres:

1. `supabase/migrations/001_init.sql`
2. `supabase/migrations/002_sync_runs_upsert_counters.sql`

### Run sync (CLI)

```bash
python main.py
python main.py --local-file data/response.json
```

### Run API

```bash
python server.py
```

### Run Q&A CLI

```bash
python ask.py "How many unpaid invoices do we have and who owes the most?"
```

---

## 11) Test Strategy

Test suite path: `tests/`.

Covers:

- transform behavior and edge cases
- API endpoint contracts
- dynamic SQL validation/execution guards
- Q&A answer shaping and context handling
- fallback/retry behavior

Run tests:

```bash
python -m pytest -q
```

---

## 12) Repo Tree (Primary Files)

```text
.
├── airflow/dags/qbo_n8n_sync_dag.py
├── supabase/migrations/
│   ├── 001_init.sql
│   └── 002_sync_runs_upsert_counters.sql
├── src/qbo_pipeline/
│   ├── __init__.py
│   ├── config.py
│   ├── observability.py
│   ├── db/pool.py
│   ├── etl/
│   │   ├── extract.py
│   │   ├── transform.py
│   │   ├── validate.py
│   │   ├── load.py
│   │   ├── pipeline.py
│   │   └── run.py
│   ├── warehouse/
│   │   ├── analytics_queries.py
│   │   └── sql_snapshot.py
│   ├── qa/
│   │   ├── warehouse_qa.py
│   │   ├── dynamic_sql.py
│   │   ├── llm_complete.py
│   │   ├── answer_structure.py
│   │   ├── context_window.py
│   │   ├── gemini_retry.py
│   │   └── small_talk.py
│   └── web/app.py
├── tests/
├── main.py
├── server.py
├── ask.py
├── requirements.txt
└── pyproject.toml
```

---

## 13) Production Readiness Notes

- Database writes are transactional and idempotent by `qbo_id`.
- Sync lifecycle is traceable in `sync_runs`.
- API sync trigger is async (`202` + background worker).
- SQL generation path is guarded and optional.
- Structured logs are emitted across all critical stages.

Recommended next production steps:

- add deployment manifests and infrastructure docs
- add runbook/SLO/alerting documentation
- add CI pipeline for lint/test/migration checks
- add secrets management policy (vault/provider-based)
