from __future__ import annotations

import os
from datetime import date, timedelta

try:
    from ingestion.gbif_raw_loader import IngestConfig, get_max_event_date, run_ingest
except ImportError:
    from gbif_raw_loader import IngestConfig, get_max_event_date, run_ingest


def main() -> None:
    config = IngestConfig()
    default_start = os.getenv("INCREMENTAL_DEFAULT_START_DATE", "2025-01-01")
    lookback_days = int(os.getenv("INCREMENTAL_LOOKBACK_DAYS", "2"))

    max_date = get_max_event_date(config)
    if max_date is None:
        start_date = date.fromisoformat(default_start)
    else:
        start_date = max_date - timedelta(days=lookback_days)
        if start_date < date.fromisoformat(default_start):
            start_date = date.fromisoformat(default_start)

    end_date = date.today()
    if start_date > end_date:
        print(f"No incremental load needed. start={start_date} end={end_date}")
        return

    print(f"Incremental ingest range: {start_date} -> {end_date}")
    processed, written = run_ingest(start_date, end_date, config)
    print(f"Incremental ingest finished. processed={processed} written={written}")


if __name__ == "__main__":
    main()
