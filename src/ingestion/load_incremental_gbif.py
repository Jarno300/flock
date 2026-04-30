from __future__ import annotations

import logging
import os
import sys
from datetime import date, timedelta

from .download_utils import run_download_ingest
from .gbif_raw_loader import IngestConfig, get_max_event_date, run_ingest


def _configure_logging() -> None:
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(levelname)s %(name)s %(message)s")


def calculate_incremental_start(
    default_start: date,
    max_event_date: date | None,
    lookback_days: int,
) -> date:
    if max_event_date is None:
        return default_start
    start = max_event_date - timedelta(days=lookback_days)
    return start if start >= default_start else default_start


def _resolve_start_date(config: IngestConfig) -> date:
    default_start = date.fromisoformat(os.getenv("INCREMENTAL_DEFAULT_START_DATE", "2025-01-01"))
    lookback_days = int(os.getenv("INCREMENTAL_LOOKBACK_DAYS", "2"))
    max_date = get_max_event_date(config)
    return calculate_incremental_start(default_start, max_date, lookback_days)


def main() -> None:
    _configure_logging()
    log = logging.getLogger(__name__)
    config = IngestConfig()
    mode = os.getenv("GBIF_INCREMENTAL_MODE", "download").strip().lower()
    start_date = _resolve_start_date(config)
    end_date = date.today()

    if start_date > end_date:
        log.info("No incremental range to ingest. start=%s end=%s", start_date, end_date)
        return

    log.info("Incremental ingest range %s -> %s mode=%r", start_date, end_date, mode)
    if mode == "download":
        stats = run_download_ingest(start_date, end_date, config)
    elif mode in ("search", "api", "day"):
        stats = run_ingest(start_date, end_date, config)
    else:
        raise SystemExit(
            f"Unknown GBIF_INCREMENTAL_MODE={mode!r}. Use 'download' (default) or 'search'."
        )

    log.info(
        "Incremental ingest finished processed=%s rows_upsert_attempted=%s",
        stats.rows_processed,
        stats.rows_upsert_attempted,
    )


if __name__ == "__main__":
    if __package__ is None:  # pragma: no cover
        sys.stderr.write(
            "Run from repo root as: python -m src.ingestion.load_incremental_gbif "
            "(with PYTHONPATH set to the project root, e.g. docker image default).\n"
        )
        raise SystemExit(2)
    main()
