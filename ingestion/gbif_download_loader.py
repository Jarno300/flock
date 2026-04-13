from __future__ import annotations

import csv
import io
import logging
import os
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
GBIF_DOWNLOAD_ZIP_URL_TEMPLATE = os.getenv(
    "GBIF_DOWNLOAD_ZIP_URL_TEMPLATE", "https://api.gbif.org/occurrence/download/request/{key}.zip"
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
    session: requests.Session, key: str, user: str, password: str
) -> str:
    url = GBIF_DOWNLOAD_ZIP_URL_TEMPLATE.format(key=key)
    response = session.get(url, auth=(user, password), stream=True, timeout=300)
    if response.status_code == 401:
        response = session.get(url, stream=True, timeout=300)
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


def pick_tabular_member(zf: zipfile.ZipFile) -> str:
    scored: list[tuple[int, str]] = []
    for info in zf.infolist():
        if info.is_dir():
            continue
        lower = info.filename.lower()
        if not (lower.endswith(".csv") or lower.endswith(".txt")):
            continue
        skip = ("citation", "rights", "metadata", "meta.xml", "eml.xml")
        if any(s in lower for s in skip):
            continue
        scored.append((info.file_size, info.filename))
    if not scored:
        raise ValueError(f"No tabular data file found in zip members: {zf.namelist()}")
    scored.sort(reverse=True)
    return scored[0][1]


def iter_zip_simple_csv_rows(zip_path: str) -> Iterator[dict[str, str]]:
    with zipfile.ZipFile(zip_path, "r") as zf:
        member = pick_tabular_member(zf)
        with zf.open(member, "r") as raw_stream:
            text_stream = io.TextIOWrapper(raw_stream, encoding="utf-8", newline="")
            reader = csv.DictReader(text_stream, delimiter="\t")
            if not reader.fieldnames:
                raise ValueError(f"No header row in {member!r}")
            for table_row in reader:
                yield table_row


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
        wait_for_download(session, key, poll_seconds=poll, max_wait_seconds=max_wait)
        zip_path = download_zip_to_tempfile(session, key, user, password)
    finally:
        session.close()

    rows_processed = 0
    rows_upsert_attempted = 0
    buffer: list[dict[str, Any]] = []
    conn = get_conn(config)
    try:
        for row in iter_zip_simple_csv_rows(zip_path):
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
