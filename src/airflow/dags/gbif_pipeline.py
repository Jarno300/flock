from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
}


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
        bash_command="cd /opt/flock && python -m src.ingestion.load_incremental_gbif",
    )

    run_dbt = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            "cd /opt/flock/src/dbt && dbt deps --profiles-dir /opt/flock/src/dbt "
            "&& dbt build --profiles-dir /opt/flock/src/dbt"
        ),
    )

    run_incremental >> run_dbt
