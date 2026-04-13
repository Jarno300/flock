from __future__ import annotations

import logging
import os
import sys
from datetime import date

from .gbif_download_loader import run_download_ingest
from .gbif_raw_loader import IngestConfig, run_ingest


def _configure_logging() -> None:
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(levelname)s %(name)s %(message)s")


def main() -> None:
    _configure_logging()
    log = logging.getLogger(__name__)
    config = IngestConfig()
    start_year = int(os.getenv("START_YEAR", "2020"))
    end_year = int(os.getenv("END_YEAR", str(date.today().year)))
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)
    mode = os.getenv("GBIF_HISTORICAL_MODE", "download").strip().lower()
    log.info("Historical ingest range %s -> %s mode=%r", start_date, end_date, mode)
    if mode in ("search", "api", "day"):
        stats = run_ingest(start_date, end_date, config)
    elif mode == "download":
        stats = run_download_ingest(start_date, end_date, config)
    else:
        raise SystemExit(
            f"Unknown GBIF_HISTORICAL_MODE={mode!r}. Use 'download' (default) or 'search'."
        )
    log.info(
        "Historical ingest finished processed=%s rows_upsert_attempted=%s",
        stats.rows_processed,
        stats.rows_upsert_attempted,
    )


if __name__ == "__main__":
    if __package__ is None:  # pragma: no cover
        sys.stderr.write(
            "Run from repo root as: python -m ingestion.load_historical_gbif "
            "(with PYTHONPATH set to the project root, e.g. docker image default).\n"
        )
        raise SystemExit(2)
    main()
