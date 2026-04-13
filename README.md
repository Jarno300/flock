# flock
bird migration data pipeline

Bird migration data pipeline using GBIF API, DuckDB, dbt, and Airflow.

## Architecture

- `ingestion/`: Python package (`ingestion.*`) for GBIF extract/load into DuckDB; canonical raw DDL reference in `ingestion/sql/raw_gbif_observations.sql`.
- `tests/`: unit tests for ingestion helpers (`python -m unittest discover -s tests`).
- `dbt/`: layered transformation models (`raw -> staging -> marts`).
- `airflow/`: orchestration for the daily incremental ingest and dbt build (historical backfill is manual; see Docker commands).
- `visualization/`: Superset runtime and dashboard query assets.
- `scripts/`: repository checks and governance utilities.

## Data Storage (DuckDB)

- Database file: `data/flock.duckdb`.
- Raw table: `raw.gbif_observations_raw`.
- Raw scope is intentionally wide and includes:
  - normalized core fields (`gbif_id`, `event_date`, `species`, `latitude`, `longitude`, etc.)
  - taxonomy/context fields (`scientific_name`, `kingdom`, `phylum`, `class_name`, etc.)
  - full JSON payload (`raw_payload`) for auditability and future extraction.

## Ingestion Modes

- Historical one-time load: module `ingestion.load_historical_gbif` (run manually, for example `docker compose run --rm ingest-historic`; not wired into Airflow)
  - **Default:** GBIF [download API](https://techdocs.gbif.org/en/data-use/api-downloads) — one asynchronous job for the full `START_YEAR`–`END_YEAR` range, then stream the `SIMPLE_CSV` zip into DuckDB (no day-by-day search).
  - Requires a GBIF.org account: set `GBIF_USER` (username), `GBIF_PASSWORD`, and `GBIF_NOTIFICATION_EMAIL` in `.env`.
  - Optional: `GBIF_HISTORICAL_MODE=search` restores the old occurrence **search** ingest (day-by-day, capped by `GBIF_MAX_PER_DAY`).
- Daily incremental load: module `ingestion.load_incremental_gbif`
  - starts from last stored `event_date` with a configurable lookback window
  - upserts by `gbif_id`, safe for reruns and overlaps

## Layer Conventions (Enforced)

- `dbt/models/raw`: source YAML only (`sources.yml`)
- `dbt/models/staging`: SQL models must start with `stg_`
- `dbt/models/marts`: SQL models must start with `dim_` or `fct_`

Validation:

```bash
python scripts/validate_dbt_structure.py
```

## Local Setup

1) Create `.env` in repo root:

```bash
DUCKDB_PATH=data/flock.duckdb
GBIF_COUNTRY=BE
GBIF_CLASS_KEY=212
# Historical download (default mode); create a free account at https://www.gbif.org/
GBIF_USER=your_gbif_username
GBIF_PASSWORD=your_gbif_password
GBIF_NOTIFICATION_EMAIL=you@example.com
GBIF_LIMIT=300
GBIF_MAX_PER_DAY=10000
GBIF_REQUEST_SLEEP_SECONDS=0.2
START_YEAR=2020
END_YEAR=2026
INCREMENTAL_DEFAULT_START_DATE=2025-01-01
INCREMENTAL_LOOKBACK_DAYS=2
LOG_LEVEL=INFO
```

2) Copy dbt profile:

```bash
copy dbt\profiles.yml.example dbt\profiles.yml
```

3) Run ingestion unit tests (from repo root):

```bash
python -m unittest discover -s tests -v
```

4) Run a loader locally (from repo root; same as Docker `python -m …`):

```bash
python -m ingestion.load_incremental_gbif
python -m ingestion.load_historical_gbif
```

## Docker Commands

Build images:

```bash
docker compose build
```

Run one-time historical import:

```bash
docker compose run --rm ingest-historic
```

Run incremental import:

```bash
docker compose run --rm ingest-incremental
```

Run dbt:

```bash
docker compose run --rm dbt dbt deps
docker compose run --rm dbt dbt build
```

## Airflow

Start Airflow services:

```bash
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler
```

Open Airflow UI at `http://localhost:8080` (`admin` / `admin`).

DAGs:

- `gbif_incremental_daily`: scheduled daily; ingests new data then runs `dbt build`.

## Superset Visualization

Start Superset:

```bash
docker compose up superset-init
docker compose up superset
```

Open UI at `http://localhost:8088` (`admin` / `admin`).

Connect DuckDB in Superset:

- Database type: `DuckDB`
- SQLAlchemy URI:
  - `duckdb:////app/data/flock.duckdb`

Recommended datasets for portfolio dashboards:

- `mart_bird_data.fct_daily_species_observations`
- `mart_bird_data.dim_species`

Starter SQL examples are in:

- `visualization/superset_starter_queries.sql`

## CI

Workflow: `.github/workflows/data-quality.yml`

- validates dbt folder/naming conventions
- runs `dbt deps` + `dbt parse` using DuckDB profile
