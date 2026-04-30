# flock
bird migration data pipeline

Bird migration data pipeline using GBIF API, DuckDB, dbt, Airflow, FastAPI, React, and Superset.

## Architecture

- `src/ingestion/`: Python package (`src.ingestion.*`) for GBIF extract/load into DuckDB; canonical raw DDL reference in `src/ingestion/sql/raw_gbif_observations.sql`.
- `tests/`: unit tests for ingestion helpers (`python -m unittest discover -s tests`).
- `src/dbt/`: layered transformation models (`raw -> staging -> marts`).
- `src/airflow/`: orchestration for the daily incremental ingest and dbt build (historical backfill is manual; see Docker commands).
- `src/superset/`: Superset runtime and dashboard query assets.
- `src/frontend/`: React operations console for pipeline control and monitoring.
- `src/backend/`: FastAPI service that controls Airflow and queries DuckDB.
- `scripts/`: repository checks and governance utilities.

## Data Storage (DuckDB)

- Database file: `data/flock.duckdb`.
- Raw table: `raw.gbif_observations_raw`.
- Raw scope is intentionally wide and includes:
  - normalized core fields (`gbif_id`, `event_date`, `species`, `latitude`, `longitude`, etc.)
  - taxonomy/context fields (`scientific_name`, `kingdom`, `phylum`, `class_name`, etc.)
  - full JSON payload (`raw_payload`) for auditability and future extraction.

## Ingestion Modes

- Historical one-time load: module `src.ingestion.load_historical_gbif` (run manually, for example `docker compose run --rm ingest-historic`; not wired into Airflow)
  - **Default:** GBIF [download API](https://techdocs.gbif.org/en/data-use/api-downloads) — one job for the full `START_YEAR`–`END_YEAR` range, then stream the download zip into DuckDB. Set `GBIF_DOWNLOAD_FORMAT` to `SIMPLE_CSV` (default, one tab file in the zip) or `SIMPLE_PARQUET` (all `.parquet` shards in the zip, often thousands, read sequentially via PyArrow). Tunables: `GBIF_PARQUET_BATCH_ROWS` (default `65536`), `GBIF_PARQUET_FILE_LOG_INTERVAL` (log every N shards, default `100`).
  - Requires a GBIF.org account: set `GBIF_USER` (username), `GBIF_PASSWORD`, and `GBIF_NOTIFICATION_EMAIL` in `.env`.
  - Scope is fixed to Belgium birds: `GBIF_COUNTRY=BE`, `GBIF_CLASS_KEY=212`, download mode only.
- Daily incremental load: module `src.ingestion.load_incremental_gbif`
  - starts from last stored `event_date` with a configurable lookback window
  - upserts by `gbif_id`, safe for reruns and overlaps

## Layer Conventions (Enforced)

- `src/dbt/models/raw`: source YAML only (`sources.yml`)
- `src/dbt/models/staging`: SQL models must start with `stg_`
- `src/dbt/models/marts`: SQL models must start with `dim_` or `fct_`

Validation:

```bash
python scripts/validate_dbt_structure.py
```

## Local Setup

1) Create `.env` in repo root:

```bash
copy .env.example .env
```

Then replace the GBIF credential placeholders.

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
copy src\dbt\profiles.yml.example src\dbt\profiles.yml
```

3) Run ingestion unit tests (from repo root):

```bash
python -m unittest discover -s tests -v
```

4) Run a loader locally (from repo root; same as Docker `python -m …`):

```bash
python -m src.ingestion.load_incremental_gbif
python -m src.ingestion.load_historical_gbif
```

## Docker Commands

Build images:

```bash
docker compose build
```

Start the full operational stack:

```bash
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler pipeline-backend pipeline-frontend
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

### After ingestion (dbt)

When GBIF data is loaded into `data/flock.duckdb` (or your `DUCKDB_PATH`), refresh models and run tests:

```bash
docker compose run --rm dbt dbt deps
docker compose run --rm dbt dbt build
docker compose run --rm dbt dbt test
```

Locally, from the `src/dbt/` directory with `DUCKDB_PATH` set to the same file the loaders used:

```bash
cd src/dbt
dbt deps --profiles-dir .
dbt build --profiles-dir .
dbt test --profiles-dir .
```

`dbt test` enforces staging rules (including valid-coordinate logic) and mart grain / referential checks (`fct_daily_species_observations` to `dim_species`).

## Airflow

Start Airflow services:

```bash
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler
```

Open Airflow UI at `http://localhost:8080` (`admin` / `admin`).

DAGs:

- `gbif_incremental_daily`: scheduled daily; ingests new data then runs `dbt build`.

## Pipeline React Console

Start the operations UI services:

```bash
docker compose up pipeline-backend pipeline-frontend
```

Open UI at `http://localhost:3000`.

Backend API is exposed at `http://localhost:3001`.

Capabilities:

- launch historical bulk runs from the backend service with per-run year range (`start_year`, `end_year`) and max-per-day safety cap
- activate/pause the daily incremental schedule
- view backend historical job status and recent daily Airflow run states
- inspect data availability metrics from DuckDB and top-species/trend panes

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

- `src/superset/superset_starter_queries.sql`

## Architecture Review Notes

- DuckDB is used as a local analytical store to keep the project easy to run without cloud infrastructure. Treat `data/flock.duckdb` as a single-writer file: ingestion and dbt write to it; UI and Superset should only read from it.
- Airflow owns the daily incremental workflow only. Historical bulk loads are started from the backend service because they are operator-driven, long-running bootstrap jobs rather than scheduled work.
- dbt owns transformation and data quality contracts (`raw -> staging -> marts`), while the React console exposes operational controls and lightweight analytics.
- Docker Compose credentials are local-demo defaults. For any deployed environment, replace them via `.env` or a secret manager.

## CI

Workflow: `.github/workflows/data-quality.yml`

- validates dbt folder/naming conventions
- runs `dbt deps` + `dbt parse` using DuckDB profile
- runs Python ingestion unit tests
- runs React frontend dependency install and production build
