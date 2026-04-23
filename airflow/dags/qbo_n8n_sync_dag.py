"""
Airflow DAG: n8n payload → temp file → start sync run → incremental upsert
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

# ✅ FIX: Point to src folder (THIS is the key change)
PROJECT_SRC = Path("/opt/airflow/project/src")
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

# ✅ Airflow imports
from airflow.decorators import dag, task

# ✅ Your project imports
from qbo_pipeline.config import Settings
from qbo_pipeline.etl.extract import fetch_webhook_to_tempfile
from qbo_pipeline.etl.load import run_delete_phase, run_insert_phase
from qbo_pipeline.observability import configure_logging, get_logger
from qbo_pipeline.etl.transform import transform

configure_logging(service="qbo_pipeline_airflow")
logger = get_logger(__name__)

# Default DAG arguments
_DEFAULT_ARGS = {
    "owner": "qbo-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="qbo_n8n_sync",
    default_args=_DEFAULT_ARGS,
    description="n8n fetch → sync run start → warehouse upsert (N8N_WEBHOOK_URL + Postgres).",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    tags=["qbo", "n8n", "supabase"],
)
def qbo_n8n_sync():

    # Task 1 — Fetch data from n8n webhook → temp file
    @task(task_id="fetch_n8n_json")
    def fetch_n8n_json() -> str:
        settings = Settings.from_env()
        path = fetch_webhook_to_tempfile(settings)
        logger.info("airflow_fetch_completed", payload_path=path)
        return path

    # Task 2 — Start sync run id (no delete in incremental mode)
    @task(task_id="warehouse_delete")
    def warehouse_delete() -> str:
        settings = Settings.from_env()
        sync_id = run_delete_phase(settings)
        logger.info("airflow_sync_run_started", sync_run_id=sync_id)
        return sync_id

    # Task 3 — Transform + Insert
    @task(task_id="warehouse_insert")
    def warehouse_insert(payload_path: str, sync_id: str) -> str:
        settings = Settings.from_env()
        path = Path(payload_path)

        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
            bundle = transform(raw_data)
            logger.info(
                "airflow_transform_completed",
                sync_run_id=sync_id,
                customer_count=len(bundle.customers),
                invoice_count=len(bundle.invoices),
                payment_count=len(bundle.payments),
                allocation_count=len(bundle.payment_invoice_allocations),
            )
            out_sync_id = run_insert_phase(
                settings,
                UUID(sync_id),
                bundle
            )
            logger.info("airflow_insert_completed", sync_run_id=out_sync_id)
            return out_sync_id
        finally:
            # Cleanup temp file
            path.unlink(missing_ok=True)

    # DAG execution flow
    payload_path = fetch_n8n_json()
    sync_run_id = warehouse_delete()

    # Ensure sync run id is available before upsert
    payload_path >> sync_run_id

    warehouse_insert(payload_path, sync_run_id)


# Required for Airflow to detect DAG
qbo_n8n_sync_dag = qbo_n8n_sync()