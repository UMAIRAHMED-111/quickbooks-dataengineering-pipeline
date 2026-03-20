# QuickBooks → Supabase

Loads n8n webhook JSON into Supabase (customers, invoices, payments).

1. Run `supabase/migrations/001_init.sql` in the Supabase SQL editor.
2. Copy `.env.example` to `.env`. Set **`DATABASE_URL`** (or **`SUPABASE_DB_URL`**) and **`N8N_WEBHOOK_URL`**.
3. In the project folder:

   ```bash
   pip install -r requirements.txt
   python main.py
   ```

Use `python main.py --local-file data/response.json` to read a file instead of the webhook.

**Invoice “email sent” (`is_email_sent`):** Derived from QBO **`EmailStatus`**. By default **`Sent`**, **`EmailSent`**, and **`NeedToSend`** count as sent (case-insensitive)—**`NeedToSend`** matches invoices queued for email (e.g. with **`DeliveryInfo.DeliveryType: Email`**). To use only “actually sent in QBO,” set **`QBO_IS_EMAIL_SENT_STATUSES=Sent,EmailSent`**. **Important:** **`NotSet`** usually means QBO has no email-send state; external-only email may not update **`EmailStatus`**. After changing env, run **`python main.py`** again to reload.

**Ask (English Q&A):** set **`OPENAI_API_KEY_1`** (and optionally **`OPENAI_API_KEY_2`** as a second key); each request tries key 1, then key 2, then **`GEMINI_API_KEY`** (or **`GOOGLE_API_KEY`**) as fallback. Default OpenAI model is **`gpt-4`** (`OPENAI_MODEL`). Sync data, then:

```bash
python ask.py "How many unpaid invoices do we have and who owes the most?"
```

After `pip install -e .`, you can use `python -m qbo_pipeline.qa.warehouse_qa "..."` instead.

**Package layout:** `qbo_pipeline.etl` (sync: extract / transform / load), `qbo_pipeline.warehouse` (SQL snapshots + analytics queries), `qbo_pipeline.qa` (OpenAI → Gemini LLM Q&A + dynamic SQL validation), `qbo_pipeline.web` (Flask API), and **`config.py`** at the package root.

**Dynamic SQL is off by default.** Turn it on with **`WAREHOUSE_QA_DYNAMIC_SQL=1`** in `.env`. Then the LLM (**OpenAI first**, then **Gemini**) proposes one **SELECT**; the app validates it (read-only, **[allowlisted tables](src/qbo_pipeline/qa/dynamic_sql.py)** only), runs it, and answers from the **real result set**. On validation/DB errors it **falls back** to snapshot packs (a line is printed to **stderr** so you know). Optional **`OPENAI_SQL_MODEL`** / **`GEMINI_SQL_MODEL`**. **`WAREHOUSE_QA_VERBOSE=1`** adds a full traceback on fallback.

**Preset packs (default when dynamic SQL is off):** a **planning call** picks which fixed SQL packs to run, then the **answer call** sees that snapshot. Set **`WAREHOUSE_QA_NO_PLANNER=1`** for one LLM call with the full snapshot. Optional **`OPENAI_PLANNER_MODEL`** / **`GEMINI_PLANNER_MODEL`**. No embeddings or vector DB.

Test questions and matching **verification SQL**: [docs/warehouse_qa_verification.md](docs/warehouse_qa_verification.md).

**Gemini fallback** **retries** HTTP **429** (default **3** extra attempts via **`GEMINI_MAX_RETRIES`**) with backoff when the request reaches Gemini. OpenAI errors on key 1 move to key 2, then Gemini.

Default **OpenAI** chat model is **`gpt-4`** (`OPENAI_MODEL`). Default **Gemini** fallback is **`gemini-2.5-flash-lite`** (`GEMINI_MODEL`). If you see **429** on Gemini, check [rate limits](https://ai.google.dev/gemini-api/docs/rate-limits)—planning uses two LLM requests when enabled.

## Analytics API (graphs / dashboards)

JSON endpoints over **`DATABASE_URL`** / **`SUPABASE_DB_URL`** for chart-friendly aggregates (CORS enabled for local frontends).

```bash
python server.py
# → http://127.0.0.1:5050/health
# List routes: GET /api/v1/metrics/catalog
```

**Refresh warehouse data:** `POST /api/v1/sync` runs the same pipeline as `python main.py` (`run_sync`: n8n webhook → full replace in Postgres). Needs **`N8N_WEBHOOK_URL`** and DB env. Optional JSON body `{"local_file": "data/response.json"}` or query `?local_file=...` to skip HTTP. If **`SYNC_API_SECRET`** is set, send **`X-Sync-Token`** or **`Authorization: Bearer <secret>`**.

**Ask the warehouse (HTTP):** `POST /api/v1/qa` with JSON `{"question": "…"}` returns structured JSON: **`answer`** (raw LLM text, same as CLI), plus **`display`** with **`markdown`** (render-friendly), **`headline`**, **`bullets`**, and **`paragraphs`** for UI layout. Same backend as **`python ask.py`** (OpenAI **gpt-4** by default, then **Gemini** fallback; optional **`WAREHOUSE_QA_DYNAMIC_SQL=1`**). Needs at least one LLM key and **`DATABASE_URL`**. Errors: **400** / **503** / **502** / **500** as before.

| Endpoint | Use |
|----------|-----|
| `GET /api/v1/metrics/overview` | Counts, total outstanding, invoiced, payments |
| `GET /api/v1/metrics/invoices/paid-vs-unpaid` | Paid vs unpaid counts & amounts |
| `GET /api/v1/metrics/invoices/sent-vs-unsent` | `is_email_sent` buckets |
| `GET /api/v1/metrics/invoices/overdue-vs-current` | Past-due unpaid vs not-yet-due unpaid |
| `GET /api/v1/metrics/invoices/paid-on-time-vs-late` | Settled invoices: on-time vs late vs unknown |
| `GET /api/v1/metrics/customers/top-paying?limit=10` | Highest payment totals |
| `GET /api/v1/metrics/customers/top-outstanding?limit=10` | Largest customer balances |
| `GET /api/v1/metrics/customers/top-overdue-debt?limit=10` | Biggest **past-due** open AR |
| `GET /api/v1/metrics/customers/best-on-time-payers?limit=10` | Most on-time **paid** invoices per customer |
| `GET /api/v1/metrics/payments/by-month` | Payment totals by calendar month |
| `GET /api/v1/metrics/allocations/summary` | Allocation counts & sums |
| `POST /api/v1/sync` | Run ETL sync (webhook or `local_file`) |
| `POST /api/v1/qa` | Natural-language Q&A over the warehouse (`question` → `answer`) |

Implementation: [`src/qbo_pipeline/warehouse/analytics_queries.py`](src/qbo_pipeline/warehouse/analytics_queries.py), [`src/qbo_pipeline/web/app.py`](src/qbo_pipeline/web/app.py), [`server.py`](server.py). Optional env: **`PORT`** (default 5050), **`FLASK_HOST`**, **`FLASK_DEBUG`**, **`SYNC_API_SECRET`**.

**Frontend:** See **[FRONTEND.md](FRONTEND.md)** for business context, API→chart mapping, and required stack (**shadcn/ui** + **Recharts**, white/black theme).

Airflow / scripts: `from qbo_pipeline import run_sync` (or `from qbo_pipeline.etl.pipeline import run_sync`) and `run_sync(Settings.from_env())`.
