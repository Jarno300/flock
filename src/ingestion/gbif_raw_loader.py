from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping, NamedTuple

import duckdb
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger(__name__)


def _env_str(key: str, default: str) -> str:
    v = os.getenv(key)
    return default if v is None or v == "" else v


def _env_int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


def _env_float(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


def event_date_to_yyyy_mm_dd(raw: Any) -> str | None:
    if raw is None or raw == "":
        return None
    s = str(raw).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt.date().isoformat()
        return datetime.strptime(s[:10], "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def iter_days(range_start: date, range_end: date):
    d = range_start
    while d <= range_end:
        yield d
        d += timedelta(days=1)


def coerce_float(val: Any) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def coerce_int(val: Any, default: int | None = None) -> int | None:
    if val is None or val == "":
        return default
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return default


def issues_to_issues_json(issues: Any) -> str | None:
    if issues is None or issues == "":
        return None
    if isinstance(issues, str):
        parts = [p.strip() for p in issues.split(";") if p.strip()]
        return json.dumps(parts) if parts else None
    return json.dumps(issues)


def individual_count_from_raw(raw: Any) -> int:
    try:
        return int(raw) if raw is not None else 1
    except (TypeError, ValueError):
        return 1


def build_raw_row_dict(
    *,
    gbif_id: str,
    event_date: str | None,
    event_date_raw: Any,
    species: str,
    scientific_name: Any,
    taxon_rank: Any,
    basis_of_record: Any,
    occurrence_status: Any,
    individual_count: int,
    latitude: float | None,
    longitude: float | None,
    coordinate_uncertainty_m: float | None,
    country_code: Any,
    state_province: Any,
    dataset_key: Any,
    publishing_org_key: Any,
    key: Any,
    taxon_key: Any,
    genus: Any,
    family: Any,
    order_val: Any,
    class_name: Any,
    phylum: Any,
    kingdom: Any,
    issues_json: str | None,
    media_count: int,
    raw_payload: str,
) -> dict[str, Any]:
    return {
        "gbif_id": gbif_id,
        "event_date": event_date,
        "event_date_raw": event_date_raw,
        "species": species,
        "scientific_name": scientific_name,
        "taxon_rank": taxon_rank,
        "basis_of_record": basis_of_record,
        "occurrence_status": occurrence_status,
        "individual_count": individual_count,
        "latitude": latitude,
        "longitude": longitude,
        "coordinate_uncertainty_m": coordinate_uncertainty_m,
        "country_code": country_code,
        "state_province": state_province,
        "dataset_key": dataset_key,
        "publishing_org_key": publishing_org_key,
        "key": key,
        "taxon_key": taxon_key,
        "genus": genus,
        "family": family,
        "order": order_val,
        "class_name": class_name,
        "phylum": phylum,
        "kingdom": kingdom,
        "issues_json": issues_json,
        "media_count": media_count,
        "ingestion_ts": datetime.utcnow(),
        "raw_payload": raw_payload,
    }


def normalize_search_occurrence(obs: dict[str, Any]) -> dict[str, Any] | None:
    gbif_id = str(obs.get("gbifID") or obs.get("key") or "")
    if not gbif_id:
        return None

    event_raw = obs.get("eventDate")
    event_date = event_date_to_yyyy_mm_dd(event_raw)
    species = obs.get("species") or obs.get("genus") or "Unknown"
    issues = obs.get("issues")
    media = obs.get("media") or []

    return build_raw_row_dict(
        gbif_id=gbif_id,
        event_date=event_date,
        event_date_raw=event_raw,
        species=species,
        scientific_name=obs.get("scientificName"),
        taxon_rank=obs.get("taxonRank"),
        basis_of_record=obs.get("basisOfRecord"),
        occurrence_status=obs.get("occurrenceStatus"),
        individual_count=individual_count_from_raw(obs.get("individualCount")),
        latitude=coerce_float(obs.get("decimalLatitude")),
        longitude=coerce_float(obs.get("decimalLongitude")),
        coordinate_uncertainty_m=coerce_float(obs.get("coordinateUncertaintyInMeters")),
        country_code=obs.get("countryCode"),
        state_province=obs.get("stateProvince"),
        dataset_key=obs.get("datasetKey"),
        publishing_org_key=obs.get("publishingOrgKey"),
        key=obs.get("key"),
        taxon_key=obs.get("taxonKey"),
        genus=obs.get("genus"),
        family=obs.get("family"),
        order_val=obs.get("order"),
        class_name=obs.get("class"),
        phylum=obs.get("phylum"),
        kingdom=obs.get("kingdom"),
        issues_json=issues_to_issues_json(issues),
        media_count=len(media),
        raw_payload=json.dumps(obs, ensure_ascii=True),
    )


def normalize_download_csv_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Map a GBIF SIMPLE_CSV or SIMPLE_PARQUET row (same logical columns) to the shape used for DuckDB upserts."""
    gbif_id = str(row.get("gbifID") or row.get("gbifKey") or "").strip()
    if not gbif_id:
        return None

    event_raw = row.get("eventDate")
    event_date = event_date_to_yyyy_mm_dd(event_raw)
    ic = coerce_int(row.get("individualCount"), 1)
    if ic is None:
        ic = 1
    species = (row.get("species") or row.get("genus") or "").strip() or "Unknown"
    issues_raw = row.get("issues") or row.get("issue")

    return build_raw_row_dict(
        gbif_id=gbif_id,
        event_date=event_date,
        event_date_raw=event_raw,
        species=species,
        scientific_name=row.get("scientificName"),
        taxon_rank=row.get("taxonRank"),
        basis_of_record=row.get("basisOfRecord"),
        occurrence_status=row.get("occurrenceStatus"),
        individual_count=ic,
        latitude=coerce_float(row.get("decimalLatitude")),
        longitude=coerce_float(row.get("decimalLongitude")),
        coordinate_uncertainty_m=coerce_float(row.get("coordinateUncertaintyInMeters")),
        country_code=row.get("countryCode"),
        state_province=row.get("stateProvince"),
        dataset_key=row.get("datasetKey"),
        publishing_org_key=row.get("publishingOrgKey"),
        key=coerce_int(row.get("gbifID") or gbif_id, None),
        taxon_key=coerce_int(row.get("taxonKey"), None),
        genus=row.get("genus"),
        family=row.get("family"),
        order_val=row.get("order"),
        class_name=row.get("class"),
        phylum=row.get("phylum"),
        kingdom=row.get("kingdom"),
        issues_json=issues_to_issues_json(issues_raw),
        media_count=0,
        raw_payload=json.dumps(dict(row), ensure_ascii=True, default=str),
    )


def normalize_record(obs: dict[str, Any]) -> dict[str, Any] | None:
    """Backward-compatible alias for :func:`normalize_search_occurrence`."""
    return normalize_search_occurrence(obs)


@dataclass
class IngestConfig:
    gbif_api_url: str = field(default_factory=lambda: _env_str("GBIF_API_URL", "https://api.gbif.org/v1/occurrence/search"))
    country: str = field(default_factory=lambda: _env_str("GBIF_COUNTRY", "BE"))
    class_key: int = field(default_factory=lambda: _env_int("GBIF_CLASS_KEY", 212))
    limit: int = field(default_factory=lambda: _env_int("GBIF_LIMIT", 300))
    max_per_day: int = field(default_factory=lambda: _env_int("GBIF_MAX_PER_DAY", 10000))
    request_sleep_seconds: float = field(
        default_factory=lambda: _env_float("GBIF_REQUEST_SLEEP_SECONDS", 0.2)
    )
    db_path: str = field(default_factory=lambda: _env_str("DUCKDB_PATH", "data/flock.duckdb"))


class IngestStats(NamedTuple):
    """Row counts from an ingest run. ``rows_upsert_attempted`` counts upsert operations, not net-new rows."""

    rows_processed: int
    rows_upsert_attempted: int


def create_gbif_search_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=3,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_conn(config: IngestConfig) -> duckdb.DuckDBPyConnection:
    db_dir = os.path.dirname(config.db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = duckdb.connect(config.db_path)
    conn.execute("create schema if not exists raw")
    conn.execute(
        """
        create table if not exists raw.gbif_observations_raw (
            gbif_id varchar primary key,
            event_date date,
            event_date_raw varchar,
            species varchar,
            scientific_name varchar,
            taxon_rank varchar,
            basis_of_record varchar,
            occurrence_status varchar,
            individual_count integer,
            latitude double,
            longitude double,
            coordinate_uncertainty_m double,
            country_code varchar,
            state_province varchar,
            dataset_key varchar,
            publishing_org_key varchar,
            key bigint,
            taxon_key bigint,
            genus varchar,
            family varchar,
            "order" varchar,
            class_name varchar,
            phylum varchar,
            kingdom varchar,
            issues_json varchar,
            media_count integer,
            ingestion_ts timestamp,
            raw_payload varchar
        )
        """
    )
    return conn


def insert_batch(conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> int:
    """Upsert rows; returns how many upsert operations were executed (same as len(rows) when non-empty)."""
    if not rows:
        return 0

    conn.executemany(
        """
        insert into raw.gbif_observations_raw (
            gbif_id, event_date, event_date_raw, species, scientific_name, taxon_rank,
            basis_of_record, occurrence_status, individual_count, latitude, longitude,
            coordinate_uncertainty_m, country_code, state_province, dataset_key,
            publishing_org_key, key, taxon_key, genus, family, "order", class_name,
            phylum, kingdom, issues_json, media_count, ingestion_ts, raw_payload
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict (gbif_id) do update set
            event_date = excluded.event_date,
            event_date_raw = excluded.event_date_raw,
            species = excluded.species,
            scientific_name = excluded.scientific_name,
            taxon_rank = excluded.taxon_rank,
            basis_of_record = excluded.basis_of_record,
            occurrence_status = excluded.occurrence_status,
            individual_count = excluded.individual_count,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            coordinate_uncertainty_m = excluded.coordinate_uncertainty_m,
            country_code = excluded.country_code,
            state_province = excluded.state_province,
            dataset_key = excluded.dataset_key,
            publishing_org_key = excluded.publishing_org_key,
            key = excluded.key,
            taxon_key = excluded.taxon_key,
            genus = excluded.genus,
            family = excluded.family,
            "order" = excluded."order",
            class_name = excluded.class_name,
            phylum = excluded.phylum,
            kingdom = excluded.kingdom,
            issues_json = excluded.issues_json,
            media_count = excluded.media_count,
            ingestion_ts = excluded.ingestion_ts,
            raw_payload = excluded.raw_payload
        """,
        [
            (
                r["gbif_id"],
                r["event_date"],
                r["event_date_raw"],
                r["species"],
                r["scientific_name"],
                r["taxon_rank"],
                r["basis_of_record"],
                r["occurrence_status"],
                r["individual_count"],
                r["latitude"],
                r["longitude"],
                r["coordinate_uncertainty_m"],
                r["country_code"],
                r["state_province"],
                r["dataset_key"],
                r["publishing_org_key"],
                r["key"],
                r["taxon_key"],
                r["genus"],
                r["family"],
                r["order"],
                r["class_name"],
                r["phylum"],
                r["kingdom"],
                r["issues_json"],
                r["media_count"],
                r["ingestion_ts"],
                r["raw_payload"],
            )
            for r in rows
        ],
    )
    return len(rows)


def run_ingest(start_date: date, end_date: date, config: IngestConfig) -> IngestStats:
    conn = get_conn(config)
    rows_processed = 0
    rows_upsert_attempted = 0
    session = create_gbif_search_session()
    try:
        for day in iter_days(start_date, end_date):
            logger.info("Processing %s", day)
            offset = 0
            day_results = 0
            while day_results < config.max_per_day:
                params = {
                    "classKey": config.class_key,
                    "country": config.country,
                    "eventDate": day.isoformat(),
                    "limit": config.limit,
                    "offset": offset,
                }
                response = session.get(config.gbif_api_url, params=params, timeout=60)
                if response.status_code != 200:
                    logger.error(
                        "GBIF search API error %s for %s: %s",
                        response.status_code,
                        day,
                        response.text[:300],
                    )
                    break
                data = response.json()
                batch = data.get("results") or []
                if not batch:
                    break

                remaining = config.max_per_day - day_results
                selected_batch = batch[:remaining]
                normalized_rows: list[dict[str, Any]] = []
                for obs in selected_batch:
                    row = normalize_search_occurrence(obs)
                    if row is None:
                        continue
                    normalized_rows.append(row)
                    rows_processed += 1
                    day_results += 1

                rows_upsert_attempted += insert_batch(conn, normalized_rows)
                offset += len(selected_batch)
                logger.info(
                    "day=%s fetched=%s rows_upsert_attempted_total=%s",
                    day,
                    day_results,
                    rows_upsert_attempted,
                )
                if (
                    data.get("endOfRecords")
                    or day_results >= config.max_per_day
                    or len(selected_batch) < len(batch)
                ):
                    break
                time.sleep(config.request_sleep_seconds)
    finally:
        session.close()
        conn.close()

    return IngestStats(rows_processed=rows_processed, rows_upsert_attempted=rows_upsert_attempted)


def get_max_event_date(config: IngestConfig) -> date | None:
    conn = get_conn(config)
    try:
        max_date = conn.execute(
            "select max(event_date) from raw.gbif_observations_raw where event_date is not null"
        ).fetchone()[0]
        return max_date
    finally:
        conn.close()
