"""Unit tests for GBIF ingestion helpers (no network)."""

from __future__ import annotations

import json
import unittest
from datetime import date

from src.ingestion.download_utils import build_download_predicate
from src.ingestion.gbif_raw_loader import (
    IngestConfig,
    event_date_to_yyyy_mm_dd,
    issues_to_issues_json,
    normalize_download_csv_row,
    normalize_search_occurrence,
)
from src.ingestion.load_incremental_gbif import calculate_incremental_start


def _test_ingest_config() -> IngestConfig:
    return IngestConfig(
        gbif_api_url="https://api.gbif.org/v1/occurrence/search",
        country="BE",
        class_key=212,
        limit=300,
        max_per_day=10000,
        request_sleep_seconds=0.2,
        db_path=":memory:",
    )


class TestEventDate(unittest.TestCase):
    def test_none_empty(self) -> None:
        self.assertIsNone(event_date_to_yyyy_mm_dd(None))
        self.assertIsNone(event_date_to_yyyy_mm_dd(""))

    def test_iso_date(self) -> None:
        self.assertEqual(event_date_to_yyyy_mm_dd("2024-03-15"), "2024-03-15")

    def test_datetime_z(self) -> None:
        self.assertEqual(event_date_to_yyyy_mm_dd("2024-03-15T22:00:00Z"), "2024-03-15")


class TestIssuesJson(unittest.TestCase):
    def test_list(self) -> None:
        self.assertEqual(issues_to_issues_json(["A", "B"]), '["A", "B"]')

    def test_semicolon_string(self) -> None:
        self.assertEqual(issues_to_issues_json("A; B"), '["A", "B"]')

    def test_empty(self) -> None:
        self.assertIsNone(issues_to_issues_json(None))
        self.assertIsNone(issues_to_issues_json(""))


class TestNormalizeSearch(unittest.TestCase):
    def test_minimal(self) -> None:
        row = normalize_search_occurrence(
            {
                "gbifID": 42,
                "key": 42,
                "eventDate": "2020-01-02",
                "species": "Turdus merula",
                "decimalLatitude": 50.1,
                "decimalLongitude": 4.2,
                "individualCount": "2",
                "issues": ["COUNTRY_COORDINATE_MISMATCH"],
                "media": [{"type": "StillImage"}],
            }
        )
        assert row is not None
        self.assertEqual(row["gbif_id"], "42")
        self.assertEqual(row["event_date"], "2020-01-02")
        self.assertEqual(row["species"], "Turdus merula")
        self.assertEqual(row["latitude"], 50.1)
        self.assertEqual(row["longitude"], 4.2)
        self.assertEqual(row["individual_count"], 2)
        self.assertEqual(row["media_count"], 1)
        self.assertIn("COUNTRY_COORDINATE_MISMATCH", row["issues_json"] or "")

    def test_missing_gbif_id(self) -> None:
        self.assertIsNone(normalize_search_occurrence({"key": None}))


class TestNormalizeDownloadCsv(unittest.TestCase):
    def test_minimal_row(self) -> None:
        row = normalize_download_csv_row(
            {
                "gbifID": "99",
                "eventDate": "2021-06-01",
                "species": "Parus major",
                "decimalLatitude": "51.0",
                "decimalLongitude": "3.7",
                "issues": "A;B",
            }
        )
        assert row is not None
        self.assertEqual(row["gbif_id"], "99")
        self.assertEqual(row["event_date"], "2021-06-01")
        self.assertEqual(json.loads(row["issues_json"] or "[]"), ["A", "B"])
        self.assertEqual(row["media_count"], 0)


class TestDownloadPredicate(unittest.TestCase):
    def test_shape(self) -> None:
        pred = build_download_predicate(
            _test_ingest_config(), date(2020, 1, 1), date(2020, 12, 31)
        )
        self.assertEqual(pred["type"], "and")
        inner = pred["predicates"]
        self.assertEqual(len(inner), 4)
        keys = {p["key"] for p in inner if p.get("type") == "equals"}
        self.assertIn("CLASS_KEY", keys)
        self.assertIn("COUNTRY", keys)


class TestIncrementalStart(unittest.TestCase):
    def test_uses_default_when_empty(self) -> None:
        self.assertEqual(
            calculate_incremental_start(date(2025, 1, 1), None, 2),
            date(2025, 1, 1),
        )

    def test_applies_lookback(self) -> None:
        self.assertEqual(
            calculate_incremental_start(date(2025, 1, 1), date(2026, 4, 10), 2),
            date(2026, 4, 8),
        )

    def test_never_before_default(self) -> None:
        self.assertEqual(
            calculate_incremental_start(date(2025, 1, 1), date(2025, 1, 2), 7),
            date(2025, 1, 1),
        )


if __name__ == "__main__":
    unittest.main()
