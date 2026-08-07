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


def _capture_requests(client, ok_record):
    """Replace the HTTP edge with a canned per-job response, capturing jobs."""
    calls = []

    def fake_request(jobs):
        calls.append(jobs)
        return [{"data": [dict(ok_record)]} for _ in jobs]

    client._request = fake_request
    return calls


def test_map_cusips_asks_the_cusip_index_and_keys_by_cusip():
    record = {"figi": "BBG000X", "ticker": "APTV", "exchCode": "US",
              "securityType": "Common Stock", "marketSector": "Equity"}
    with of.OpenFigiClient(key="test-key") as client:
        calls = _capture_requests(client, record)
        cusips = [f"{i:09d}" for i in range(150)]
        out = client.map_cusips(cusips)

    assert [len(c) for c in calls] == [100, 50]  # keyed batch size, batched
    assert {j["idType"] for call in calls for j in call} == {"ID_CUSIP"}
    assert set(out) == set(cusips)  # keyed by the CUSIP that was asked
    assert out["000000000"].ticker == "APTV"


def test_map_isins_still_asks_the_isin_index():
    record = {"figi": "BBG000Y", "ticker": "NOVO-B", "exchCode": "DC",
              "securityType": "Common Stock", "marketSector": "Equity"}
    with of.OpenFigiClient(key="test-key") as client:
        calls = _capture_requests(client, record)
        out = client.map_isins(["DK0060534915"])

    assert calls == [[{"idType": "ID_ISIN", "idValue": "DK0060534915"}]]
    assert out["DK0060534915"].ticker == "NOVO-B"
