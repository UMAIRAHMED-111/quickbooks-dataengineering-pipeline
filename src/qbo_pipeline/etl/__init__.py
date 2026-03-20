"""ETL: extract QuickBooks JSON → transform → load into Postgres."""

from qbo_pipeline.etl.pipeline import run_sync

__all__ = ["run_sync"]
