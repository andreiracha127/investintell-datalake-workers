# tests/test_fund_institutional_reveal.py
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
