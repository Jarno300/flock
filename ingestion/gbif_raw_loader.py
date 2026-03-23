from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import duckdb
import requests


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


@dataclass
class IngestConfig:
    gbif_api_url: str = os.getenv("GBIF_API_URL", "https://api.gbif.org/v1/occurrence/search")
    country: str = os.getenv("GBIF_COUNTRY", "BE")
    class_key: int = int(os.getenv("GBIF_CLASS_KEY", "212"))
    limit: int = int(os.getenv("GBIF_LIMIT", "300"))
    max_per_day: int = int(os.getenv("GBIF_MAX_PER_DAY", "10000"))
    request_sleep_seconds: float = float(os.getenv("GBIF_REQUEST_SLEEP_SECONDS", "0.2"))
    db_path: str = os.getenv("DUCKDB_PATH", "data/flock.duckdb")


def get_conn(config: IngestConfig) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(config.db_path), exist_ok=True)
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


def normalize_record(obs: dict[str, Any]) -> dict[str, Any] | None:
    gbif_id = str(obs.get("gbifID") or obs.get("key") or "")
    if not gbif_id:
        return None

    event_date = event_date_to_yyyy_mm_dd(obs.get("eventDate"))
    individual_count_raw = obs.get("individualCount")
    try:
        individual_count = int(individual_count_raw) if individual_count_raw is not None else 1
    except (TypeError, ValueError):
        individual_count = 1

    lat = obs.get("decimalLatitude")
    lon = obs.get("decimalLongitude")
    lat = float(lat) if lat is not None else None
    lon = float(lon) if lon is not None else None

    species = obs.get("species") or obs.get("genus") or "Unknown"
    issues = obs.get("issues")
    media = obs.get("media") or []

    return {
        "gbif_id": gbif_id,
        "event_date": event_date,
        "event_date_raw": obs.get("eventDate"),
        "species": species,
        "scientific_name": obs.get("scientificName"),
        "taxon_rank": obs.get("taxonRank"),
        "basis_of_record": obs.get("basisOfRecord"),
        "occurrence_status": obs.get("occurrenceStatus"),
        "individual_count": individual_count,
        "latitude": lat,
        "longitude": lon,
        "coordinate_uncertainty_m": obs.get("coordinateUncertaintyInMeters"),
        "country_code": obs.get("countryCode"),
        "state_province": obs.get("stateProvince"),
        "dataset_key": obs.get("datasetKey"),
        "publishing_org_key": obs.get("publishingOrgKey"),
        "key": obs.get("key"),
        "taxon_key": obs.get("taxonKey"),
        "genus": obs.get("genus"),
        "family": obs.get("family"),
        "order": obs.get("order"),
        "class_name": obs.get("class"),
        "phylum": obs.get("phylum"),
        "kingdom": obs.get("kingdom"),
        "issues_json": json.dumps(issues) if issues is not None else None,
        "media_count": len(media),
        "ingestion_ts": datetime.utcnow(),
        "raw_payload": json.dumps(obs, ensure_ascii=True),
    }


def insert_batch(conn: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> int:
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


def run_ingest(start_date: date, end_date: date, config: IngestConfig) -> tuple[int, int]:
    conn = get_conn(config)
    rows_processed = 0
    rows_written = 0
    for day in iter_days(start_date, end_date):
        print(f"Processing {day}...")
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
            response = requests.get(config.gbif_api_url, params=params, timeout=60)
            if response.status_code != 200:
                print(f"API error {response.status_code} for {day}: {response.text[:300]}")
                break
            data = response.json()
            batch = data.get("results") or []
            if not batch:
                break

            normalized_rows: list[dict[str, Any]] = []
            for obs in batch:
                if day_results >= config.max_per_day:
                    break
                row = normalize_record(obs)
                if row is None:
                    continue
                normalized_rows.append(row)
                rows_processed += 1
                day_results += 1

            rows_written += insert_batch(conn, normalized_rows)
            offset += len(batch)
            print(f"  fetched={day_results} written_total={rows_written}")
            if data.get("endOfRecords") or day_results >= config.max_per_day:
                break
            time.sleep(config.request_sleep_seconds)

    conn.close()
    return rows_processed, rows_written


def get_max_event_date(config: IngestConfig) -> date | None:
    conn = get_conn(config)
    max_date = conn.execute(
        "select max(event_date) from raw.gbif_observations_raw where event_date is not null"
    ).fetchone()[0]
    conn.close()
    return max_date
