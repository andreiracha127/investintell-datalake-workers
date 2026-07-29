import src.workers.matview_refresh as mr


class _FakeCursor:
    def __init__(self, sink): self._sink = sink
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None):
        self._sink.setdefault("sql", []).append(sql)
        self._sink.setdefault("events", []).append(("sql", sql))
        if self._sink.get("fail_on") and self._sink["fail_on"] in sql:
            raise RuntimeError("refresh failed")
    def fetchone(self): return (True,)


class _FakeConn:
    def __init__(self, sink, tag):
        self._sink = sink
        self._tag = tag
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
    monkeypatch.setattr(
        mr.market_overview_snapshot,
        "run",
        lambda dsn: sink.setdefault("events", []).append(("snapshot", dsn))
        or {"published": 1, "as_of": "2026-07-13"},
    )
    result = mr.run("postgres://app", datalake_dsn="postgres://lake")

    joined = "\n".join(sink["sql"])
    # App DB MVs (Grupo D).
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY price_latest_mv" in joined
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY nav_latest_mv" in joined
    # Cobertura de NAV por fundo: serve os gates de qualidade do universo do
    # builder, que antes reagregavam a hypertable nav_timeseries por request.
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_nav_coverage_mv" in joined
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
        "fund_nav_coverage_mv",
        "fund_style_drift_mv",
        "fund_top_holdings_mv",
    ]
    assert result["refreshed_datalake"] == [
        "stock_institutional_holders_mv",
        "stock_fund_holders_mv",
        "holding_reverse_lookup_mv",
    ]
    assert result["market_overview_snapshot"] == {
        "published": 1,
        "as_of": "2026-07-13",
    }
    event_names = [event[0] for event in sink["events"]]
    snapshot_index = event_names.index("snapshot")
    assert "fund_top_holdings_mv" in sink["events"][snapshot_index - 1][1]
    assert "stock_institutional_holders_mv" in sink["events"][snapshot_index + 1][1]


def test_datalake_step_skipped_when_no_dsn(monkeypatch):
    sink: dict = {}

    def _fake_connect(dsn=None, *, autocommit=False):
        sink.setdefault("dsns", []).append(dsn)
        return _FakeConn(sink, dsn)

    monkeypatch.setattr(mr, "connect", _fake_connect)
    monkeypatch.setattr(
        mr.market_overview_snapshot,
        "run",
        lambda dsn: {"published": 1, "as_of": "2026-07-13"},
    )
    result = mr.run("postgres://app", datalake_dsn=None)
    assert result["refreshed_datalake"] == []
    assert result["market_overview_snapshot"]["published"] == 1


def test_app_mv_failure_prevents_snapshot_publication(monkeypatch):
    sink: dict = {"fail_on": "nav_latest_mv"}

    def _fake_connect(dsn=None, *, autocommit=False):
        return _FakeConn(sink, dsn)

    called = {"snapshot": False}
    monkeypatch.setattr(mr, "connect", _fake_connect)
    monkeypatch.setattr(
        mr.market_overview_snapshot,
        "run",
        lambda dsn: called.__setitem__("snapshot", True),
    )

    try:
        mr.run("postgres://app", datalake_dsn="postgres://lake")
    except RuntimeError as exc:
        assert str(exc) == "refresh failed"
    else:
        raise AssertionError("MV failure must propagate")

    assert called["snapshot"] is False
    assert not any("stock_institutional_holders_mv" in sql for sql in sink["sql"])


def test_snapshot_failure_propagates_before_datalake_refresh(monkeypatch):
    sink: dict = {}

    def _fake_connect(dsn=None, *, autocommit=False):
        return _FakeConn(sink, dsn)

    def _fail_snapshot(dsn):
        raise RuntimeError("snapshot failed")

    monkeypatch.setattr(mr, "connect", _fake_connect)
    monkeypatch.setattr(mr.market_overview_snapshot, "run", _fail_snapshot)

    try:
        mr.run("postgres://app", datalake_dsn="postgres://lake")
    except RuntimeError as exc:
        assert str(exc) == "snapshot failed"
    else:
        raise AssertionError("snapshot failure must propagate")

    assert not any("stock_institutional_holders_mv" in sql for sql in sink["sql"])


def test_unpublished_snapshot_fails_before_datalake_refresh(monkeypatch):
    sink: dict = {}

    def _fake_connect(dsn=None, *, autocommit=False):
        return _FakeConn(sink, dsn)

    monkeypatch.setattr(mr, "connect", _fake_connect)
    monkeypatch.setattr(
        mr.market_overview_snapshot,
        "run",
        lambda dsn: {"published": 0, "skipped": "lock_busy"},
    )

    try:
        mr.run("postgres://app", datalake_dsn="postgres://lake")
    except RuntimeError as exc:
        assert "did not publish" in str(exc)
    else:
        raise AssertionError("unpublished snapshot must fail")

    assert not any("stock_institutional_holders_mv" in sql for sql in sink["sql"])
