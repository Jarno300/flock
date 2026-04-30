from __future__ import annotations

import csv
import io
import logging
import os
import shutil
import tempfile
import time
import zipfile
from datetime import date
from typing import Any, Iterator

import requests

from .gbif_raw_loader import (
    IngestConfig,
    IngestStats,
    create_gbif_search_session,
    get_conn,
    insert_batch,
    normalize_download_csv_row,
)

logger = logging.getLogger(__name__)

GBIF_DOWNLOAD_REQUEST_URL = os.getenv(
    "GBIF_DOWNLOAD_REQUEST_URL", "https://api.gbif.org/v1/occurrence/download/request"
)
GBIF_DOWNLOAD_STATUS_URL_TEMPLATE = os.getenv(
    "GBIF_DOWNLOAD_STATUS_URL_TEMPLATE", "https://api.gbif.org/v1/occurrence/download/{key}"
)
# Must include /v1/ — the host without /v1/ returns 404 for completed zips.
GBIF_DOWNLOAD_ZIP_URL_TEMPLATE = os.getenv(
    "GBIF_DOWNLOAD_ZIP_URL_TEMPLATE",
    "https://api.gbif.org/v1/occurrence/download/request/{key}.zip",
)


def _require_download_credentials() -> tuple[str, str, str]:
    user = os.getenv("GBIF_USER", "").strip()
    password = os.getenv("GBIF_PASSWORD", "").strip()
    email = os.getenv("GBIF_NOTIFICATION_EMAIL", "").strip()
    if not user or not password:
        raise SystemExit(
            "GBIF occurrence downloads require HTTP Basic auth. Set GBIF_USER and GBIF_PASSWORD "
            "(your GBIF.org username and password, not email for the username)."
        )
    if not email:
        raise SystemExit(
            "Set GBIF_NOTIFICATION_EMAIL to an address GBIF can use for download notifications "
            "(required in the download request body)."
        )
    return user, password, email


def build_download_predicate(config: IngestConfig, start_date: date, end_date: date) -> dict[str, Any]:
    return {
        "type": "and",
        "predicates": [
            {"type": "equals", "key": "CLASS_KEY", "value": str(config.class_key)},
            {"type": "equals", "key": "COUNTRY", "value": config.country},
            {
                "type": "greaterThanOrEquals",
                "key": "EVENT_DATE",
                "value": start_date.isoformat(),
            },
            {
                "type": "lessThanOrEquals",
                "key": "EVENT_DATE",
                "value": end_date.isoformat(),
            },
        ],
    }


def build_download_request_body(
    creator: str,
    notification_email: str,
    config: IngestConfig,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    fmt = os.getenv("GBIF_DOWNLOAD_FORMAT", "SIMPLE_CSV").strip().upper()
    send_notification = os.getenv("GBIF_DOWNLOAD_SEND_NOTIFICATION", "true").lower() in (
        "1",
        "true",
        "yes",
    )
    return {
        "creator": creator,
        "notificationAddresses": [notification_email],
        "sendNotification": send_notification,
        "format": fmt,
        "predicate": build_download_predicate(config, start_date, end_date),
    }


def post_download_request(body: dict[str, Any], user: str, password: str) -> str:
    # Intentionally not using a retried session: duplicate POST could create multiple jobs.
    response = requests.post(
        GBIF_DOWNLOAD_REQUEST_URL,
        json=body,
        auth=(user, password),
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"GBIF download request failed ({response.status_code}): {response.text[:800]}"
        )
    text = response.text.strip()
    if not text:
        raise RuntimeError("GBIF download request returned an empty body (expected download key).")
    if text.startswith("{"):
        payload = response.json()
        key = payload.get("key") or payload.get("downloadKey")
        if not key:
            raise RuntimeError(f"Unexpected GBIF download JSON response: {text[:500]}")
        return str(key)
    return text


def fetch_download_status(session: requests.Session, key: str) -> dict[str, Any]:
    url = GBIF_DOWNLOAD_STATUS_URL_TEMPLATE.format(key=key)
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


def wait_for_download(
    session: requests.Session,
    key: str,
    poll_seconds: float,
    max_wait_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + max_wait_seconds
    while time.monotonic() < deadline:
        meta = fetch_download_status(session, key)
        status = (meta.get("status") or "").upper()
        if status == "SUCCEEDED":
            return meta
        if status in ("FAILED", "KILLED", "CANCELLED", "ABORTED"):
            raise RuntimeError(f"GBIF download {key} ended with status={status!r}: {meta}")
        logger.info("Download %s status=%r — sleeping %ss", key, status, poll_seconds)
        time.sleep(poll_seconds)
    raise TimeoutError(f"Timed out waiting for GBIF download {key} after {max_wait_seconds}s")


def download_zip_to_tempfile(
    session: requests.Session, zip_url: str, user: str, password: str
) -> str:
    response = session.get(zip_url, auth=(user, password), stream=True, timeout=300)
    if response.status_code == 401:
        response = session.get(zip_url, stream=True, timeout=300)
    response.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    try:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            if chunk:
                tmp.write(chunk)
        tmp.close()
        return tmp.name
    except BaseException:
        tmp.close()
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
        raise


def _zip_member_is_skipped(lower: str) -> bool:
    skip = ("citation", "rights", "metadata", "meta.xml", "eml.xml")
    return any(s in lower for s in skip)


def list_parquet_members_sorted(zf: zipfile.ZipFile) -> list[str]:
    """
    All ``.parquet`` / ``.pq`` members GBIF ships in ``SIMPLE_PARQUET`` downloads
    (often thousands of shard files), excluding obvious metadata paths.
    """
    names: list[str] = []
    for info in zf.infolist():
        if info.is_dir():
            continue
        lower = info.filename.lower()
        if _zip_member_is_skipped(lower):
            continue
        if lower.endswith(".parquet") or lower.endswith(".pq"):
            names.append(info.filename)
    names.sort()
    return names


def pick_largest_tsv_member(zf: zipfile.ZipFile) -> str | None:
    """Single tab-separated export for ``SIMPLE_CSV`` (largest .csv/.txt by size)."""
    scored: list[tuple[int, str]] = []
    for info in zf.infolist():
        if info.is_dir():
            continue
        lower = info.filename.lower()
        if _zip_member_is_skipped(lower):
            continue
        if lower.endswith(".csv") or lower.endswith(".txt"):
            scored.append((info.file_size, info.filename))
    if not scored:
        return None
    scored.sort(reverse=True)
    return scored[0][1]


def _iter_all_parquet_members(
    zf: zipfile.ZipFile,
    members: list[str],
) -> Iterator[dict[str, Any]]:
    import pyarrow.parquet as pq

    batch_size = int(os.getenv("GBIF_PARQUET_BATCH_ROWS", "65536"))
    log_every = max(1, int(os.getenv("GBIF_PARQUET_FILE_LOG_INTERVAL", "100")))
    total = len(members)
    logger.info("Streaming %s parquet file(s) from GBIF zip", total)
    for idx, member in enumerate(members, start=1):
        if idx == 1 or idx % log_every == 0 or idx == total:
            logger.info("Parquet shard %s / %s: %s", idx, total, member)
        tmp_parquet = _extract_open_zip_member_to_temp(zf, member, ".parquet")
        try:
            pf = pq.ParquetFile(tmp_parquet)
            for batch in pf.iter_batches(batch_size=batch_size):
                pyd = batch.to_pydict()
                if not pyd:
                    continue
                cols = batch.schema.names
                n = len(next(iter(pyd.values())))
                for i in range(n):
                    yield {c: pyd[c][i] for c in cols}
        finally:
            try:
                os.unlink(tmp_parquet)
            except OSError:
                pass


def _extract_open_zip_member_to_temp(zf: zipfile.ZipFile, member: str, suffix: str) -> str:
    fd, out_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    try:
        with open(out_path, "wb") as out_f, zf.open(member, "r") as in_f:
            shutil.copyfileobj(in_f, out_f, length=16 * 1024 * 1024)
    except BaseException:
        try:
            os.unlink(out_path)
        except OSError:
            pass
        raise
    return out_path


def iter_zip_download_rows(zip_path: str) -> Iterator[dict[str, Any]]:
    """
    Stream rows from a GBIF occurrence download zip.

    - ``SIMPLE_CSV``: one tab-separated table (largest ``.csv`` / ``.txt`` in the zip).
    - ``SIMPLE_PARQUET``: **every** ``.parquet`` / ``.pq`` shard in the zip (often thousands),
      in sorted path order — each file is extracted to a temp path, streamed, then removed.
    """
    fmt = os.getenv("GBIF_DOWNLOAD_FORMAT", "SIMPLE_CSV").strip().upper()
    want_parquet = fmt == "SIMPLE_PARQUET"

    with zipfile.ZipFile(zip_path, "r") as zf:
        parquet_members = list_parquet_members_sorted(zf)
        if want_parquet and parquet_members:
            yield from _iter_all_parquet_members(zf, parquet_members)
            return

        if want_parquet and not parquet_members:
            logger.warning(
                "GBIF_DOWNLOAD_FORMAT=SIMPLE_PARQUET but no .parquet in zip; falling back to TSV if present"
            )

        tsv_member = pick_largest_tsv_member(zf)
        if tsv_member is not None:
            with zf.open(tsv_member, "r") as raw_stream:
                text_stream = io.TextIOWrapper(raw_stream, encoding="utf-8", newline="")
                reader = csv.DictReader(text_stream, delimiter="\t")
                if not reader.fieldnames:
                    raise ValueError(f"No header row in {tsv_member!r}")
                for table_row in reader:
                    yield dict(table_row)
            return

        if parquet_members:
            logger.info("No TSV in zip; reading %s parquet shard(s) anyway", len(parquet_members))
            yield from _iter_all_parquet_members(zf, parquet_members)
            return

    raise ValueError(f"No occurrence data (.parquet or .csv/.txt) in zip: open and inspect {zip_path!r}")


def run_download_ingest(start_date: date, end_date: date, config: IngestConfig) -> IngestStats:
    user, password, email = _require_download_credentials()
    poll = float(os.getenv("GBIF_DOWNLOAD_POLL_SECONDS", "20"))
    max_wait = float(os.getenv("GBIF_DOWNLOAD_MAX_WAIT_SECONDS", str(72 * 3600)))
    batch_size = int(os.getenv("GBIF_DOWNLOAD_INSERT_BATCH", "5000"))

    body = build_download_request_body(user, email, config, start_date, end_date)
    logger.info(
        "Requesting GBIF download format=%r range=%s..%s country=%r class_key=%s",
        body["format"],
        start_date,
        end_date,
        config.country,
        config.class_key,
    )
    key = post_download_request(body, user, password)
    logger.info("GBIF download key=%s — polling (interval=%ss max=%ss)", key, poll, max_wait)

    session = create_gbif_search_session()
    try:
        meta = wait_for_download(session, key, poll_seconds=poll, max_wait_seconds=max_wait)
        zip_url = (meta.get("downloadLink") or "").strip()
        if not zip_url:
            zip_url = GBIF_DOWNLOAD_ZIP_URL_TEMPLATE.format(key=key)
            logger.warning("No downloadLink in GBIF status payload; using constructed URL")
        else:
            logger.info("Using downloadLink from GBIF status")
        zip_path = download_zip_to_tempfile(session, zip_url, user, password)
    finally:
        session.close()

    rows_processed = 0
    rows_upsert_attempted = 0
    buffer: list[dict[str, Any]] = []
    conn = get_conn(config)
    try:
        for row in iter_zip_download_rows(zip_path):
            normalized = normalize_download_csv_row(row)
            if normalized is None:
                continue
            buffer.append(normalized)
            rows_processed += 1
            if len(buffer) >= batch_size:
                rows_upsert_attempted += insert_batch(conn, buffer)
                buffer.clear()
                logger.info(
                    "processed=%s rows_upsert_attempted_total=%s",
                    rows_processed,
                    rows_upsert_attempted,
                )
        if buffer:
            rows_upsert_attempted += insert_batch(conn, buffer)
    finally:
        conn.close()
        try:
            os.unlink(zip_path)
        except OSError:
            pass

    return IngestStats(rows_processed=rows_processed, rows_upsert_attempted=rows_upsert_attempted)
