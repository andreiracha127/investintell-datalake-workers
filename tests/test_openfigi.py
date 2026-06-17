"""Tests for the OpenFIGI client (ISIN → ticker/exchange bridge).

Pure parsing/batching only — no network. The HTTP path is exercised by the
enrichment worker's integration test with a fake client.
"""

from __future__ import annotations

from src.workers import _openfigi as of


def test_batches_splits_into_chunks():
    assert of._batches(list(range(250)), 100) == [
        list(range(0, 100)),
        list(range(100, 200)),
        list(range(200, 250)),
    ]
    assert of._batches([], 100) == []


def test_parse_mapping_picks_first_equity_ticker_per_isin():
    jobs = ["TW0002330008", "JP3633400001", "XX0000000000"]
    payload = [
        {"data": [
            {"figi": "BBG000A", "ticker": "2330", "exchCode": "TT",
             "securityType": "Common Stock", "marketSector": "Equity"},
            {"figi": "BBG000B", "ticker": "TSM", "exchCode": "US",
             "securityType": "Depositary Receipt", "marketSector": "Equity"},
        ]},
        {"data": [{"figi": "BBG000C", "ticker": "7203", "exchCode": "JT",
                   "securityType": "Common Stock", "marketSector": "Equity"}]},
        {"warning": "No identifier found."},
    ]
    out = of.parse_mapping_response(jobs, payload)
    assert out["TW0002330008"].ticker == "2330"
    assert out["TW0002330008"].exch_code == "TT"
    assert out["JP3633400001"].ticker == "7203"
    assert "XX0000000000" not in out  # warning → no match


def test_parse_mapping_prefers_equity_over_non_equity_and_skips_tickerless():
    jobs = ["A"]
    payload = [{"data": [
        {"figi": "BBG1", "ticker": None, "exchCode": "X", "securityType": "Common Stock"},
        {"figi": "BBG2", "ticker": "WT", "exchCode": "Y", "securityType": "Warrant"},
        {"figi": "BBG3", "ticker": "GOOD", "exchCode": "Z", "securityType": "Common Stock"},
    ]}]
    out = of.parse_mapping_response(jobs, payload)
    assert out["A"].ticker == "GOOD"  # equity-with-ticker beats warrant / tickerless


def test_parse_mapping_handles_empty_and_malformed_entries():
    jobs = ["A", "B", "C"]
    payload = [{"data": []}, {"error": "rate"}, "not-a-dict"]
    assert of.parse_mapping_response(jobs, payload) == {}
