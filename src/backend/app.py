from __future__ import annotations

import os
import subprocess
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_API_BASE_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_API_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_API_PASSWORD", "admin")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/flock.duckdb")
FRONTEND_ORIGINS = [
    origin.strip()
    for origin in os.getenv("PIPELINE_FRONTEND_ORIGIN", "http://localhost:3000").split(",")
    if origin.strip()
]

INCREMENTAL_DAG_ID = "gbif_incremental_daily"

app = FastAPI(title="Flock Pipeline UI API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=FRONTEND_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class HistoricalRunRequest(BaseModel):
    start_year: int = Field(ge=1900, le=2100)
    end_year: int = Field(ge=1900, le=2100)
    max_per_day: int = Field(default=10000, ge=1)


class ScheduleUpdateRequest(BaseModel):
    is_paused: bool


historical_state: dict[str, Any] = {
    "status": "idle",
    "job_id": None,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "config": None,
}
historical_lock = threading.Lock()


def airflow_client() -> httpx.Client:
    return httpx.Client(auth=(AIRFLOW_USER, AIRFLOW_PASSWORD), timeout=30)


def airflow_get(path: str) -> dict[str, Any]:
    with airflow_client() as client:
        response = client.get(f"{AIRFLOW_BASE_URL}{path}")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Airflow GET failed: {response.text}")
    return response.json()


def airflow_post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    with airflow_client() as client:
        response = client.post(f"{AIRFLOW_BASE_URL}{path}", json=payload)
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Airflow POST failed: {response.text}")
    return response.json()


def airflow_patch(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    with airflow_client() as client:
        response = client.patch(f"{AIRFLOW_BASE_URL}{path}", json=payload)
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Airflow PATCH failed: {response.text}")
    return response.json()


def duckdb_file_exists() -> bool:
    return DUCKDB_PATH == ":memory:" or Path(DUCKDB_PATH).exists()


def connect_duckdb_readonly() -> duckdb.DuckDBPyConnection:
    if not duckdb_file_exists():
        raise FileNotFoundError(f"DuckDB database not found: {DUCKDB_PATH}")
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def fetch_df(query: str) -> list[dict[str, Any]]:
    conn = connect_duckdb_readonly()
    try:
        rows = conn.execute(query).fetchdf().to_dict(orient="records")
        return rows
    finally:
        conn.close()


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def safe_airflow_get(path: str) -> dict[str, Any] | None:
    try:
        return airflow_get(path)
    except HTTPException:
        return None
    except httpx.HTTPError:
        return None


@app.get("/api/pipeline/status")
def pipeline_status() -> dict[str, Any]:
    daily = safe_airflow_get(f"/dags/{INCREMENTAL_DAG_ID}")
    daily_runs = safe_airflow_get(
        f"/dags/{INCREMENTAL_DAG_ID}/dagRuns?order_by=-start_date&limit=10"
    )
    with historical_lock:
        historical = dict(historical_state)
    return {
        "daily": {
            "available": daily is not None,
            "is_paused": daily.get("is_paused") if daily else None,
            "next_dagrun": daily.get("next_dagrun") if daily else None,
            "last_parsed_time": daily.get("last_parsed_time") if daily else None,
            "runs": daily_runs.get("dag_runs", []) if daily_runs else [],
        },
        "historical": historical,
    }


@app.post("/api/historical/run")
def run_historical(request: HistoricalRunRequest) -> dict[str, Any]:
    if request.start_year > request.end_year:
        raise HTTPException(status_code=400, detail="start_year cannot be greater than end_year")

    with historical_lock:
        if historical_state["status"] == "running":
            raise HTTPException(status_code=409, detail="Historical job is already running")

        job_id = str(uuid.uuid4())
        cfg = {
            "start_year": request.start_year,
            "end_year": request.end_year,
            "country": "BE",
            "class_key": 212,
            "max_per_day": request.max_per_day,
            "mode": "download",
        }
        historical_state.update(
            {
                "status": "running",
                "job_id": job_id,
                "started_at": datetime.utcnow().isoformat() + "Z",
                "finished_at": None,
                "error": None,
                "config": cfg,
            }
        )

    def worker(config: dict[str, Any], active_job_id: str) -> None:
        env = os.environ.copy()
        env.update(
            {
                "START_YEAR": str(config["start_year"]),
                "END_YEAR": str(config["end_year"]),
                "GBIF_COUNTRY": config["country"],
                "GBIF_CLASS_KEY": str(config["class_key"]),
                "GBIF_MAX_PER_DAY": str(config["max_per_day"]),
                "GBIF_HISTORICAL_MODE": config["mode"],
            }
        )
        try:
            subprocess.run(
                ["python", "-m", "src.ingestion.load_historical_gbif"],
                check=True,
                env=env,
                cwd="/app",
            )
            with historical_lock:
                if historical_state["job_id"] == active_job_id:
                    historical_state["status"] = "succeeded"
                    historical_state["finished_at"] = datetime.utcnow().isoformat() + "Z"
        except Exception as exc:  # noqa: BLE001
            with historical_lock:
                if historical_state["job_id"] == active_job_id:
                    historical_state["status"] = "failed"
                    historical_state["finished_at"] = datetime.utcnow().isoformat() + "Z"
                    historical_state["error"] = str(exc)

    threading.Thread(target=worker, args=(cfg, job_id), daemon=True).start()
    return {"job_id": job_id, "status": "running"}


@app.post("/api/daily/schedule")
def update_schedule(request: ScheduleUpdateRequest) -> dict[str, Any]:
    return airflow_patch(f"/dags/{INCREMENTAL_DAG_ID}", {"is_paused": request.is_paused})


def table_count_or_none(conn: duckdb.DuckDBPyConnection, table_name: str) -> int | None:
    try:
        return conn.execute(f"select count(*) from {table_name}").fetchone()[0]
    except duckdb.Error:
        return None


def scalar_or_none(conn: duckdb.DuckDBPyConnection, query: str) -> Any:
    try:
        return conn.execute(query).fetchone()[0]
    except duckdb.Error:
        return None


@app.get("/api/data/summary")
def data_summary() -> dict[str, Any]:
    if not duckdb_file_exists():
        return {
            "raw_row_count": None,
            "stg_row_count": None,
            "mart_daily_rows": None,
            "mart_species_rows": None,
            "min_event_date": None,
            "max_event_date": None,
        }

    conn = connect_duckdb_readonly()
    try:
        return {
            "raw_row_count": table_count_or_none(conn, "raw.gbif_observations_raw"),
            "stg_row_count": table_count_or_none(conn, "stg_bird_data.stg_belgian_birds"),
            "mart_daily_rows": table_count_or_none(
                conn, "mart_bird_data.fct_daily_species_observations"
            ),
            "mart_species_rows": table_count_or_none(conn, "mart_bird_data.dim_species"),
            "min_event_date": scalar_or_none(
                conn, "select min(event_date) from raw.gbif_observations_raw"
            ),
            "max_event_date": scalar_or_none(
                conn, "select max(event_date) from raw.gbif_observations_raw"
            ),
        }
    finally:
        conn.close()


@app.get("/api/data/species")
def top_species(limit: int = 20) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 200))
    query = f"""
    select
        fact.species_id,
        coalesce(dim.species_name, fact.species_id) as species_name,
        sum(fact.individuals_observed) as individuals_observed,
        sum(fact.observation_records) as observation_records
    from mart_bird_data.fct_daily_species_observations as fact
    left join mart_bird_data.dim_species as dim
        on fact.species_id = dim.species_id
    group by 1, 2
    order by individuals_observed desc
    limit {safe_limit}
    """
    try:
        return fetch_df(query)
    except duckdb.Error:
        return []


@app.get("/api/data/daily")
def daily_trend(days: int = 90) -> list[dict[str, Any]]:
    safe_days = max(7, min(days, 3650))
    query = f"""
    select
        observation_date,
        sum(observation_records) as observation_records,
        sum(individuals_observed) as individuals_observed
    from mart_bird_data.fct_daily_species_observations
    where observation_date >= current_date - interval '{safe_days} days'
    group by 1
    order by 1
    """
    try:
        return fetch_df(query)
    except duckdb.Error:
        return []
