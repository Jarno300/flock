from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
}


with DAG(
    dag_id="gbif_historic_bootstrap_once",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["flock", "ingestion", "historic"],
) as historic_dag:
    run_historic = BashOperator(
        task_id="run_historic_bulk_import",
        bash_command="cd /opt/flock && python ingestion/belgium_historic_bulk_import.py",
    )


with DAG(
    dag_id="gbif_incremental_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["flock", "ingestion", "incremental"],
) as incremental_dag:
    run_incremental = BashOperator(
        task_id="run_incremental_ingest",
        bash_command="cd /opt/flock && python ingestion/gbif_incremental_import.py",
    )

    run_dbt = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            "cd /opt/flock/dbt && dbt deps --profiles-dir /opt/flock/dbt "
            "&& dbt build --profiles-dir /opt/flock/dbt"
        ),
    )

    run_incremental >> run_dbt
