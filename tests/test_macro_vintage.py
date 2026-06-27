import pathlib

import pytest

from src import db


def test_ddl_file_exists_and_declares_table() -> None:
    sql = pathlib.Path("schemas/macro_observation_vintage.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS macro_observation_vintage" in sql
    for col in ("series_id", "observation_period", "vintage_date", "value",
                "available_at", "revision_number", "source", "source_spec_version", "ingested_at"):
        assert col in sql, f"missing column {col}"
    assert "PRIMARY KEY (series_id, observation_period, vintage_date)" in sql
    assert "create_hypertable" in sql


def test_lock_id_registered_and_unique() -> None:
    assert db.LOCK_MACRO_VINTAGE == 900_321
    ids = [v for k, v in vars(db).items() if k.startswith("LOCK_") and isinstance(v, int)]
    assert ids.count(900_321) == 1


import datetime as _dt

from src.workers import macro_vintage as mv


def store_last_sql(store: dict) -> str:
    return store.get("last_sql", "")

# Real ALFRED output_type=2 for PAYEMS 2010-03 (trimmed to the transitions):
# 129750 (1st print 2010-04-02) -> 129871 -> 129849 (held) -> 129438 (benchmark 2011-02).
_ALFRED_PAYEMS = {
    "observations": [
        {
            "date": "2010-03-01",
            "PAYEMS_20100402": "129750",
            "PAYEMS_20100507": "129871",
            "PAYEMS_20100604": "129849",
            "PAYEMS_20100702": "129849",
            "PAYEMS_20110107": "129849",
            "PAYEMS_20110204": "129438",
            "PAYEMS_20111231": "129438",
        }
    ]
}


def test_parse_alfred_compresses_to_real_revisions() -> None:
    rows = mv.parse_alfred_vintages("PAYEMS", _ALFRED_PAYEMS)
    # 7 vintages collapse to 4 distinct-value revisions
    assert [(r["vintage_date"], r["value"], r["revision_number"]) for r in rows] == [
        (_dt.date(2010, 4, 2), 129750.0, 0),
        (_dt.date(2010, 5, 7), 129871.0, 1),
        (_dt.date(2010, 6, 4), 129849.0, 2),
        (_dt.date(2011, 2, 4), 129438.0, 3),
    ]
    assert all(r["observation_period"] == _dt.date(2010, 3, 1) for r in rows)
    assert all(r["series_id"] == "PAYEMS" for r in rows)


def test_parse_alfred_drops_missing_markers() -> None:
    payload = {"observations": [{"date": "2020-01-01", "X_20200115": ".", "X_20200215": "5.0"}]}
    rows = mv.parse_alfred_vintages("X", payload)
    assert [r["value"] for r in rows] == [5.0]
    assert rows[0]["revision_number"] == 0  # missing print does not consume a revision number


def test_parse_alfred_ignores_non_vintage_columns() -> None:
    payload = {"observations": [{"date": "2020-01-01", "realtime_start": "2020-01-01", "X_20200115": "3.0"}]}
    rows = mv.parse_alfred_vintages("X", payload)
    assert len(rows) == 1 and rows[0]["value"] == 3.0


class _FakeCur:
    def __init__(self, store): self.store = store
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None):
        self._last = (sql, params)
        if params is not None and "pg_try_advisory_lock" in sql:
            self.store["lock"] = True
    def executemany(self, sql, seq):
        self.store["last_sql"] = sql
        self.store.setdefault("rows", []).extend(seq)
    def fetchone(self): return (True,)


class _FakeConn:
    def __init__(self, store): self.store = store
    def cursor(self): return _FakeCur(self.store)
    def commit(self): self.store["committed"] = True
    def close(self): pass


def test_rows_to_records_sets_available_at_and_version() -> None:
    rows = mv.parse_alfred_vintages("PAYEMS", _ALFRED_PAYEMS)
    recs = mv.rows_to_records(rows, "macro_quadrant_us_v1.0")
    # record tuple: (series_id, observation_period, vintage_date, value, available_at, revision_number, source, source_spec_version)
    first = recs[0]
    assert first[0] == "PAYEMS"
    assert first[2] == _dt.date(2010, 4, 2)           # vintage_date
    assert first[4] == _dt.datetime(2010, 4, 2, tzinfo=_dt.timezone.utc)  # available_at = vintage 00:00 UTC
    assert first[5] == 0                               # revision_number
    assert first[6] == "alfred" and first[7] == "macro_quadrant_us_v1.0"


def test_upsert_vintages_sends_all_records() -> None:
    store: dict = {}
    recs = mv.rows_to_records(mv.parse_alfred_vintages("PAYEMS", _ALFRED_PAYEMS), "v")
    n = mv.upsert_vintages(_FakeConn(store), recs)
    assert n == len(recs) == 4
    assert "ON CONFLICT" in store_last_sql(store)


def test_run_returns_lock_busy_sentinel(monkeypatch) -> None:
    import contextlib

    @contextlib.contextmanager
    def _busy(conn, lock_id):
        yield False
    monkeypatch.setenv("FRED_API_KEY", "test-key")  # run() reads the key before the lock check
    monkeypatch.setattr(mv, "connect", lambda dsn, **k: _FakeConn({}))
    monkeypatch.setattr(mv, "advisory_lock", _busy)
    monkeypatch.setattr(mv, "ensure_schema", lambda conn: None)
    out = mv.run("postg://x", calc_date="2026-06-26")
    assert out["status"] == "lock_busy"


class _FakeBucket:
    def acquire(self) -> None:
        pass


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeClient:
    def __init__(self, responses: list[_FakeResponse]):
        self.responses = list(responses)

    def get(self, *args, **kwargs) -> _FakeResponse:
        return self.responses.pop(0)


def test_fetch_vintages_fails_closed_on_alfred_400() -> None:
    client = _FakeClient([
        _FakeResponse(400, {"error_message": "invalid api_key"}),
    ])

    with pytest.raises(mv.MacroVintageFetchError, match="invalid api_key"):
        mv.fetch_vintages(client, "bad-key", "PAYEMS", _FakeBucket())


def test_fetch_vintages_fails_closed_after_retry_exhaustion(monkeypatch) -> None:
    import time

    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    client = _FakeClient([
        _FakeResponse(503),
        _FakeResponse(503),
        _FakeResponse(503),
    ])

    with pytest.raises(mv.MacroVintageFetchError, match="retry exhaustion"):
        mv.fetch_vintages(client, "key", "PAYEMS", _FakeBucket())


import os as _os


@pytest.mark.skipif(not _os.getenv("FRED_API_KEY"), reason="needs FRED_API_KEY")
def test_smoke_fetch_real_payems_has_vintages() -> None:
    import httpx
    with httpx.Client(timeout=30.0) as client:
        payload = mv.fetch_vintages(client, _os.environ["FRED_API_KEY"], "PAYEMS", mv.TokenBucket())
    rows = mv.parse_alfred_vintages("PAYEMS", payload)
    assert len(rows) > 50  # decades of monthly revisions
    assert all(r["revision_number"] >= 0 for r in rows)
