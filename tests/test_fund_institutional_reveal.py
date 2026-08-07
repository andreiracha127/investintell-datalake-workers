from contextlib import contextmanager

import pytest

import src.workers.fund_institutional_reveal as fir

def test_build_payload_aggregates_holders_and_overlap():
    # 13F rows: (cik, manager_name, period, report_date, cusip, name, value_usd, shares)
    rows = [
        {"cik": "1", "manager_name": "Alpha", "period": "2026-03-31", "report_date": "2026-03-31",
         "cusip": "AAA", "name": "Apple", "value_usd": 100.0, "shares": 10.0},
        {"cik": "2", "manager_name": "Beta", "period": "2026-03-31", "report_date": "2026-03-31",
         "cusip": "AAA", "name": "Apple", "value_usd": 50.0, "shares": 5.0},
    ]
    fund_pct = {"AAA": 0.05}
    payload = fir.build_payload("fund:1", "TST", rows, fund_pct)
    assert payload["schema_version"] == 1 or "top_holders" in payload
    assert len(payload["top_holders"]) == 2
    assert payload["overlap"][0]["cusip"] == "AAA"
    assert payload["overlap"][0]["institution_count"] == 2
    node_types = {n["type"] for n in payload["holder_network"]["nodes"]}
    assert {"fund", "security", "institution"} <= node_types


def test_build_payload_empty_rows():
    payload = fir.build_payload("fund:1", "TST", [], {})
    assert payload["top_holders"] == []
    assert payload["overlap"] == []


def test_prepare_reveal_holdings_quarantines_entire_series_for_unknown_weight():
    rows = [
        {
            "series_id": "S000001", "report_date": "2026-03-31", "rank": 1,
            "cusip": "AAA", "weight": 0.05, "source_row_count": 2,
            "nonnull_weight_count": 1, "null_weight_count": 1,
            "has_unknown_weight": True,
        },
        {
            "series_id": "S000001", "report_date": "2026-03-31", "rank": 2,
            "cusip": "BBB", "weight": None, "source_row_count": 2,
            "nonnull_weight_count": 1, "null_weight_count": 1,
            "has_unknown_weight": True,
        },
    ]

    holdings, quarantine = fir._prepare_reveal_holdings("S000001", rows)

    assert holdings is None
    assert quarantine == {
        "series_id": "S000001",
        "report_date": "2026-03-31",
        "reason": "unknown_weight",
    }


def test_prepare_reveal_holdings_keeps_ranked_known_weights():
    rows = [
        {
            "series_id": "S000002", "report_date": "2026-06-30", "rank": 2,
            "cusip": "BBB", "weight": 0.02, "source_row_count": 2,
            "nonnull_weight_count": 2, "null_weight_count": 0,
            "has_unknown_weight": False,
        },
        {
            "series_id": "S000002", "report_date": "2026-06-30", "rank": 1,
            "cusip": "AAA", "weight": 0.05, "source_row_count": 2,
            "nonnull_weight_count": 2, "null_weight_count": 0,
            "has_unknown_weight": False,
        },
    ]

    holdings, quarantine = fir._prepare_reveal_holdings("S000002", rows)

    assert quarantine is None
    assert holdings == (["AAA", "BBB"], {"AAA": 0.05, "BBB": 0.02}, "2026-06-30")


def test_prepare_reveal_holdings_honors_pre_top_100_unknown_gate():
    rows = [
        {
            "series_id": "S000004", "report_date": "2026-06-30", "rank": 1,
            "cusip": "AAA", "weight": 0.05, "source_row_count": 1,
            "nonnull_weight_count": 1, "null_weight_count": 0,
            "has_unknown_weight": True,
        }
    ]

    holdings, quarantine = fir._prepare_reveal_holdings("S000004", rows)

    assert holdings is None
    assert quarantine["reason"] == "unknown_weight"


def test_prepare_reveal_holdings_quarantines_no_joinable_cusips():
    holdings, quarantine = fir._prepare_reveal_holdings("S000005", [])

    assert holdings is None
    assert quarantine["reason"] == "no_joinable_cusips"


def test_run_quarantines_unknown_weights_before_13f_query(monkeypatch):
    class FakeConnection:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def commit(self): pass
        def cursor(self):
            raise AssertionError("quarantined series must not query 13F or upsert")

    @contextmanager
    def fake_lock(_conn, _lock):
        yield True

    monkeypatch.setattr(fir, "connect", lambda _dsn: FakeConnection())
    monkeypatch.setattr(fir, "advisory_lock", fake_lock)
    monkeypatch.setattr(fir, "_series_with_holdings", lambda _conn, _limit: ["S000003"])
    monkeypatch.setattr(fir, "_reveal_holdings_for_series", lambda _conn, _series_id: [
        {
            "series_id": "S000003", "report_date": "2026-06-30", "rank": 1,
            "cusip": "AAA", "weight": None, "source_row_count": 1,
            "nonnull_weight_count": 0, "null_weight_count": 1,
            "has_unknown_weight": True,
        }
    ])
    revoked = []
    monkeypatch.setattr(
        fir,
        "_revoke_series_artifacts",
        lambda _conn, series_id: revoked.append(series_id) or 2,
    )
    monkeypatch.setattr(fir, "_refresh_latest_mv", lambda _dsn: None)

    result = fir.run("postgres://x")

    assert result == {
        "processed": 0,
        "upserted": 0,
        "quarantined": 1,
        "revoked_artifacts": 2,
        "quarantine_samples": [{
            "series_id": "S000003", "report_date": "2026-06-30", "reason": "unknown_weight",
        }],
        "mv_refreshed": True,
    }
    assert revoked == ["S000003"]


def test_run_propagates_latest_mv_refresh_failure(monkeypatch):
    class FakeConnection:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def commit(self): pass

    @contextmanager
    def fake_lock(_conn, _lock):
        yield True

    monkeypatch.setattr(fir, "connect", lambda _dsn: FakeConnection())
    monkeypatch.setattr(fir, "advisory_lock", fake_lock)
    monkeypatch.setattr(fir, "_series_with_holdings", lambda _conn, _limit: [])
    monkeypatch.setattr(
        fir,
        "_refresh_latest_mv",
        lambda _dsn: (_ for _ in ()).throw(RuntimeError("refresh failed")),
    )

    with pytest.raises(RuntimeError, match="refresh failed"):
        fir.run("postgres://x")


class _FakeCursor:
    def __init__(self, sink): self._sink = sink
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, *_a): self._sink["sql"] = " ".join(str(sql).split())


class _FakeConn:
    def __init__(self, sink): self._sink = sink
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def cursor(self): return _FakeCursor(self._sink)


def test_refresh_latest_mv_concurrently_autocommit(monkeypatch):
    sink = {}
    def _fake_connect(dsn=None, *, autocommit=False):
        sink["autocommit"] = autocommit
        return _FakeConn(sink)
    monkeypatch.setattr(fir, "connect", _fake_connect)
    fir._refresh_latest_mv("postgres://x")
    assert sink["autocommit"] is True
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_institutional_reveal_latest_mv" in sink["sql"]
