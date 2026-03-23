import os
from datetime import date

try:
    from ingestion.gbif_duckdb_ingest import IngestConfig, run_ingest
except ImportError:
    from gbif_duckdb_ingest import IngestConfig, run_ingest


def main() -> None:
    config = IngestConfig()
    start_year = int(os.getenv("START_YEAR", "2025"))
    end_year = int(os.getenv("END_YEAR", str(date.today().year)))
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)
    print(f"Historical ingest range: {start_date} -> {end_date}")
    processed, written = run_ingest(start_date, end_date, config)
    print(f"Historical ingest finished. processed={processed} written={written}")


if __name__ == "__main__":
    main()
