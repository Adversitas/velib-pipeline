"""Airflow version of the pipeline (the repository runs the same steps on GitHub Actions).

    ingest -> load -> quality_checks -> report

Every 15 minutes. Ingestion is idempotent and loading only processes files not
yet in `meta.ingestion_log`, so retries and overlapping runs are safe, and a
backfill simply re-loads whatever the landing zone holds.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable


def _paths() -> tuple[Path, str]:
    lake = Path(Variable.get("velib_lake", default_var="/opt/airflow/data/velib"))
    return lake, str(lake / "warehouse.duckdb")


@dag(
    dag_id="velib_pipeline",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,  # one writer at a time on the DuckDB file
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["velib", "ingestion"],
)
def velib_pipeline():
    @task
    def ingest() -> list[dict]:
        from velib.ingest import ingest as run_ingest

        lake, _ = _paths()
        return [
            {"feed": r.feed, "published_at": r.published_at, "new": r.new} for r in run_ingest(lake)
        ]

    @task
    def load(_landed: list[dict]) -> dict:
        from velib.load import run

        lake, db = _paths()
        return run(db, lake)

    @task(retries=0)
    def quality_checks(_loaded: dict) -> list[dict]:
        import duckdb

        from velib.quality import as_dicts, run_checks

        _, db = _paths()
        with duckdb.connect(db, read_only=True) as con:
            checks = as_dicts(run_checks(con))
        failed = [c for c in checks if c["status"] == "fail"]
        if failed:
            raise AirflowFailException(f"quality checks failed: {failed}")
        return checks

    @task
    def report(_checks: list[dict]) -> str:
        from velib.report import build

        lake, db = _paths()
        return str(build(db, lake / "site" / "index.html"))

    report(quality_checks(load(ingest())))


velib_pipeline()
