from __future__ import annotations

import logging
import os
import sys
from datetime import date, timedelta

from .gbif_raw_loader import IngestConfig, get_max_event_date, run_ingest


def _configure_logging() -> None:
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(levelname)s %(name)s %(message)s")


def main() -> None:
    _configure_logging()
    log = logging.getLogger(__name__)
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
        log.info("No incremental load needed start=%s end=%s", start_date, end_date)
        return

    log.info("Incremental ingest range %s -> %s", start_date, end_date)
    stats = run_ingest(start_date, end_date, config)
    log.info(
        "Incremental ingest finished processed=%s rows_upsert_attempted=%s",
        stats.rows_processed,
        stats.rows_upsert_attempted,
    )


if __name__ == "__main__":
    if __package__ is None:  # pragma: no cover
        sys.stderr.write(
            "Run from repo root as: python -m ingestion.load_incremental_gbif "
            "(with PYTHONPATH set to the project root, e.g. docker image default).\n"
        )
        raise SystemExit(2)
    main()
