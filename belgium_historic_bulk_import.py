import time
import requests
from datetime import date, datetime, timedelta, timezone
from google.cloud import bigquery
from google.oauth2 import service_account

def event_date_to_yyyy_mm_dd(raw) -> str | None:
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

# CONFIG
url = "https://api.gbif.org/v1/occurrence/search"
country = "BE"
class_key = 212       # 212 = "Aves" (Birds)
start_year = 2025   #starts on 1-1
end_year = 2026     #ends on 31-12
limit = 300
max_per_day = 10000  # safety cap per calendar day (GBIF search offset is 100k, but very slow past 10K)
table_id = "flock-491009.raw_bird_data.belgian_birds_v3"
BQ_BATCH_SIZE = 10000  # rows per insert_rows_json call (stay under request size limits)

BQ_ROW_KEYS = (
    "gbif_id",
    "species",
    "date",
    "latitude",
    "longitude",
    "individual_count",
)


def iter_days(range_start: date, range_end: date):
    """Each calendar day from range_start through range_end (inclusive)."""
    d = range_start
    while d <= range_end:
        yield d
        d += timedelta(days=1)


def flush_bq_buffer(client, table_id: str, buffer: list, error_list: list) -> int:
    if not buffer:
        return 0
    errors = client.insert_rows_json(table_id, buffer)
    n = len(buffer)
    buffer.clear()
    if errors:
        error_list.extend(errors)
    return n


range_start = date(start_year, 1, 1)
range_end = date(end_year, 12, 31)
print(f"\nExtracting data by day: {range_start} .. {range_end}")

key_path = "gcp-key.json"
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project="flock-491009")

_t_parts = table_id.split(".")
if len(_t_parts) != 3:
    raise ValueError(f"table_id must be project.dataset.table, got {table_id!r}")
_project, _dataset, _ = _t_parts
client.create_dataset(bigquery.Dataset(f"{_project}.{_dataset}"), exists_ok=True)
_bq_schema = [
    bigquery.SchemaField("gbif_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("species", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("latitude", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("individual_count", "INT64", mode="NULLABLE"),
]
client.create_table(bigquery.Table(table_id, schema=_bq_schema), exists_ok=True)
print(f"BigQuery ready: {table_id}")

bq_buffer: list = []
bq_errors: list = []
rows_accepted = 0
rows_sent_to_bq = 0

for day in iter_days(range_start, range_end):
    print(f"\nProcessing {day} ...")

    offset = 0
    day_results = 0

    while day_results < max_per_day:
        params = {
            "classKey": class_key,
            "country": country,
            # Single calendar day (comma range also works: same date twice).
            "eventDate": day.isoformat(),
            "limit": limit,
            "offset": offset,
            "hasCoordinate": "true",
        }

        response = requests.get(url, params=params)

        if response.status_code != 200:
            print(
                f"\nAPI Error {response.status_code}: {response.text[:500]!r}. Batch skipped."
            )
            break

        data = response.json()
        batch = data.get("results") or []

        if not batch:
            print(f"\nNo data for {day}.")
            break

        for obs in batch:
            if day_results >= max_per_day:
                break

            lat, lon = obs.get("decimalLatitude"), obs.get("decimalLongitude")
            if lat is None or lon is None:
                continue

            obs_day = event_date_to_yyyy_mm_dd(obs.get("eventDate"))
            if obs_day is None:
                continue

            n = obs.get("individualCount")
            try:
                individual_count = int(n) if n is not None else 1
            except (TypeError, ValueError):
                individual_count = 1

            species_name = obs.get("species")
            if not species_name:
                species_name = obs.get("genus", "Unknown")

            clean_observations = {
                "gbif_id": str(obs.get("gbifID")),
                "species": species_name,
                "date": obs_day,
                "latitude": float(lat),
                "longitude": float(lon),
                "individual_count": individual_count,
            }
            bq_row = {k: clean_observations[k] for k in BQ_ROW_KEYS}
            bq_buffer.append(bq_row)
            rows_accepted += 1
            if len(bq_buffer) >= BQ_BATCH_SIZE:
                rows_sent_to_bq += flush_bq_buffer(client, table_id, bq_buffer, bq_errors)
                print(f"BigQuery: sent {rows_sent_to_bq} rows total")
            day_results += 1

        offset += len(batch)
        print(f"-> {day_results} results fetched for {day}")

        if data.get("endOfRecords") or day_results >= max_per_day:
            break

        time.sleep(0.3)

rows_sent_to_bq += flush_bq_buffer(client, table_id, bq_buffer, bq_errors)

print(f"\nExtract complete: {rows_accepted} records fetched, {rows_sent_to_bq} sent to BigQuery.")

if not bq_errors:
    print("BigQuery upload done!")
else:
    print(
        f"BigQuery row errors: {len(bq_errors)} (showing first 5): {bq_errors[:5]}"
    )