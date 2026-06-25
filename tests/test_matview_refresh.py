import src.workers.matview_refresh as mr


class _FakeCursor:
    def __init__(self, sink): self._sink = sink
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None):
        self._sink.setdefault("sql", []).append(sql)
    def fetchone(self): return (True,)


class _FakeConn:
    def __init__(self, sink, tag): self._sink = sink; self._tag = tag
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def cursor(self): return _FakeCursor(self._sink)


def test_refresh_runs_app_and_datalake_mvs(monkeypatch):
    sink: dict = {}

    def _fake_connect(dsn=None, *, autocommit=False):
        sink.setdefault("dsns", []).append(dsn)
        sink["autocommit"] = autocommit or sink.get("autocommit")
        return _FakeConn(sink, dsn)

    monkeypatch.setattr(mr, "connect", _fake_connect)
    result = mr.run("postgres://app", datalake_dsn="postgres://lake")

    joined = "\n".join(sink["sql"])
    # App DB MVs (Grupo D).
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY price_latest_mv" in joined
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY nav_latest_mv" in joined
    # App DB Grupo A aggregate MVs.
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_style_drift_mv" in joined
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_top_holdings_mv" in joined
    # fund_active_share_mv was removed — active share now lives on
    # fund_risk_latest_mv (refreshed by the risk_metrics worker, not here).
    assert "fund_active_share_mv" not in joined
    # Datalake MVs (Grupo B).
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY stock_institutional_holders_mv" in joined
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY stock_fund_holders_mv" in joined
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY holding_reverse_lookup_mv" in joined
    assert result["refreshed"] == [
        "price_latest_mv",
        "nav_latest_mv",
        "fund_style_drift_mv",
        "fund_top_holdings_mv",
    ]
    assert result["refreshed_datalake"] == [
        "stock_institutional_holders_mv",
        "stock_fund_holders_mv",
        "holding_reverse_lookup_mv",
    ]


def test_datalake_step_skipped_when_no_dsn(monkeypatch):
    sink: dict = {}

    def _fake_connect(dsn=None, *, autocommit=False):
        sink.setdefault("dsns", []).append(dsn)
        return _FakeConn(sink, dsn)

    monkeypatch.setattr(mr, "connect", _fake_connect)
    result = mr.run("postgres://app", datalake_dsn=None)
    assert result["refreshed_datalake"] == []
