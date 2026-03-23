# flock
bird migration data pipeline

Bird migration data pipeline using GBIF API, DuckDB, dbt, and Airflow.

## Architecture

- `ingestion/`: API extraction and load into local DuckDB.
- `dbt/`: layered transformation models (`raw -> staging -> marts`).
- `airflow/`: orchestration with one-time historical bootstrap + daily incremental runs.
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

- Historical one-time load: `ingestion/load_historical_gbif.py`
- Daily incremental load: `ingestion/load_incremental_gbif.py`
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
GBIF_LIMIT=300
GBIF_MAX_PER_DAY=10000
GBIF_REQUEST_SLEEP_SECONDS=0.2
START_YEAR=2020
END_YEAR=2026
INCREMENTAL_DEFAULT_START_DATE=2025-01-01
INCREMENTAL_LOOKBACK_DAYS=2
```

2) Copy dbt profile:

```bash
copy dbt\profiles.yml.example dbt\profiles.yml
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

- `gbif_historic_bootstrap_once`: trigger manually once.
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
