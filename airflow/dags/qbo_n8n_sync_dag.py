"""
Airflow DAG: n8n payload → temp file → delete warehouse snapshot → insert rows
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
from qbo_pipeline.etl.transform import transform


# Default DAG arguments
_DEFAULT_ARGS = {
    "owner": "qbo-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="qbo_n8n_sync",
    default_args=_DEFAULT_ARGS,
    description="n8n fetch → warehouse delete → warehouse insert (N8N_WEBHOOK_URL + Postgres).",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["qbo", "n8n", "supabase"],
)
def qbo_n8n_sync():

    # Task 1 — Fetch data from n8n webhook → temp file
    @task(task_id="fetch_n8n_json")
    def fetch_n8n_json() -> str:
        settings = Settings.from_env()
        return fetch_webhook_to_tempfile(settings)

    # Task 2 — Delete existing warehouse snapshot
    @task(task_id="warehouse_delete")
    def warehouse_delete() -> str:
        settings = Settings.from_env()
        return run_delete_phase(settings)

    # Task 3 — Transform + Insert
    @task(task_id="warehouse_insert")
    def warehouse_insert(payload_path: str, sync_id: str) -> str:
        settings = Settings.from_env()
        path = Path(payload_path)

        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
            bundle = transform(raw_data)

            return run_insert_phase(
                settings,
                UUID(sync_id),
                bundle
            )
        finally:
            # Cleanup temp file
            path.unlink(missing_ok=True)

    # DAG execution flow
    payload_path = fetch_n8n_json()
    sync_run_id = warehouse_delete()

    # Ensure delete runs before insert
    payload_path >> sync_run_id

    warehouse_insert(payload_path, sync_run_id)


# Required for Airflow to detect DAG
qbo_n8n_sync_dag = qbo_n8n_sync()