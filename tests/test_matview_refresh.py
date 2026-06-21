import src.workers.matview_refresh as mr


class _FakeCursor:
    def __init__(self, sink):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._sink.setdefault("sql", []).append(sql)

    def fetchone(self):
        return (True,)  # pg_try_advisory_lock → got


class _FakeConn:
    def __init__(self, sink):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return _FakeCursor(self._sink)


def test_refresh_runs_both_mvs_concurrently_in_autocommit(monkeypatch):
    sink: dict = {}

    def _fake_connect(dsn=None, *, autocommit=False):
        sink["dsn"] = dsn
        sink["autocommit"] = autocommit
        return _FakeConn(sink)

    monkeypatch.setattr(mr, "connect", _fake_connect)
    result = mr.run("postgres://x")

    assert sink["autocommit"] is True  # CONCURRENTLY não roda em bloco de txn
    joined = "\n".join(sink["sql"])
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY price_latest_mv" in joined
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY nav_latest_mv" in joined
    assert result["refreshed"] == ["price_latest_mv", "nav_latest_mv"]
