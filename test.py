import time
import requests
import json
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account


def event_date_to_yyyy_mm_dd(raw) -> str | None:
    """BigQuery DATE columns expect YYYY-MM-DD; GBIF eventDate is often ISO with time or Z."""
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

url = "https://api.gbif.org/v1/occurrence/search"
species_key = 2474950
country = "BE"
limit = 250
offset = 0
max_results = 250
output_path = "gbif_observations.json"
output_path_raw = "gbif_observations_raw.json"
# Must match raw_bird_data.grus_grus (gbif_id, species, date, latitude, longitude, individual_count).
BQ_ROW_KEYS = (
    "gbif_id",
    "species",
    "date",
    "latitude",
    "longitude",
    "individual_count",
)

all_results = []

print(f"Fetching results for species key {species_key} in {country}...")

while len(all_results) < max_results:
    params = {
        "speciesKey": species_key,
        "country": country,
        "limit": limit,
        "offset": offset
    }
    response = requests.get(url, params=params)
    data = response.json()

    with open(output_path_raw, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved to {output_path_raw}")

    batch = data.get("results") or []
    if not batch:
        break

    for obs in batch:
        if len(all_results) >= max_results:
            break
        lat, lon = obs.get("decimalLatitude"), obs.get("decimalLongitude")
        if lat is None or lon is None:
            continue
        day = event_date_to_yyyy_mm_dd(obs.get("eventDate"))
        if day is None:
            continue
        n = obs.get("individualCount")
        try:
            individual_count = int(n) if n is not None else 1
        except (TypeError, ValueError):
            individual_count = 1
        clean_observations = {
            "gbif_id": str(obs.get("gbifID")),
            "species": obs.get("species", "Unknown"),
            "date": day,
            "latitude": float(lat),
            "longitude": float(lon),
            "individual_count": individual_count,
        }
        all_results.append(clean_observations)

    print(f"[{len(all_results)}] results in memory (fetched offset {offset}, +{len(batch)} rows)")
    offset += len(batch)
    if data.get("endOfRecords") or len(all_results) >= max_results:
        break
    time.sleep(1)

print(f"Extraction complete: {len(all_results)} results")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(all_results, f, indent=2, ensure_ascii=False)
print(f"Saved to {output_path}")

print(f"sending to BigQuery")
key_path = "gcp-key.json"
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project="flock-491009")
table_id = "flock-491009.raw_bird_data.grus_grus"
rows_for_bq = [{k: row[k] for k in BQ_ROW_KEYS} for row in all_results]
errors = client.insert_rows_json(table_id, rows_for_bq)

if not errors:
    print("BiqQuery upload success")
else:
    print(f"Error during upload to BigQuery: {errors}")