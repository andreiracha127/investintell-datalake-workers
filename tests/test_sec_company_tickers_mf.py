"""Tests for the sec_company_tickers_mf SEC fund ticker-map parser.

Unit tests parse an in-memory fixture of the SEC's columnar JSON
(``{"fields": [...], "data": [[...]]}``); live network is only used by run().
"""

from __future__ import annotations

from src.db import LOCK_SEC_COMPANY_TICKERS_MF
from src.workers import sec_company_tickers_mf as scmf


def _payload() -> dict:
    # Mirrors https://www.sec.gov/files/company_tickers_mf.json shape.
    return {
        "fields": ["cik", "seriesId", "classId", "symbol"],
        "data": [
            [1350487, "S000012379", "C000033617", "DTD"],
            [1350487, "S000012380", "C000033618", "DXJ"],
            [2110, "S000009184", "C000024957", "ACRNX"],
            [9999, "", "C000000000", "NOSERIES"],   # dropped: empty series
            [9999, "S000000001", "C000000001", ""],  # dropped: empty ticker
        ],
    }


def test_parse_keeps_valid_rows_and_drops_incomplete() -> None:
    rows = scmf.parse_company_tickers_mf(_payload())
    by_ticker = {r.ticker: r for r in rows}
    assert set(by_ticker) == {"DTD", "DXJ", "ACRNX"}
    assert by_ticker["DTD"].series_id == "S000012379"
    assert by_ticker["DTD"].class_id == "C000033617"
    # CIK is zero-stripped to match registrant/N-PORT conventions.
    assert by_ticker["DTD"].cik == "1350487"


def test_parse_uppercases_ticker_and_dedups_by_class() -> None:
    payload = {
        "fields": ["cik", "seriesId", "classId", "symbol"],
        "data": [
            [1, "S1", "C1", "dtd"],
            [1, "S1", "C1", "DTD"],  # same class_id → deduped
        ],
    }
    rows = scmf.parse_company_tickers_mf(payload)
    assert len(rows) == 1
    assert rows[0].ticker == "DTD"


def test_parse_resolves_fields_by_name_not_position() -> None:
    # The resolver must key off the `fields` header, not column order.
    payload = {
        "fields": ["symbol", "classId", "seriesId", "cik"],
        "data": [["DTD", "C1", "S000012379", 1350487]],
    }
    rows = scmf.parse_company_tickers_mf(payload)
    assert rows[0].series_id == "S000012379"
    assert rows[0].ticker == "DTD"
    assert rows[0].cik == "1350487"


def test_parse_empty_or_malformed_payload_is_safe() -> None:
    assert scmf.parse_company_tickers_mf({}) == []
    assert scmf.parse_company_tickers_mf({"fields": ["cik"], "data": [[1]]}) == []


def test_advisory_lock_registered() -> None:
    assert LOCK_SEC_COMPANY_TICKERS_MF == 900_309
