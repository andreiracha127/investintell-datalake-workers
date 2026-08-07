"""DB-free tests for the bond_live_daily worker and its provider client.

The stage functions are exercised against a tiny fake connection rather than a
disposable Postgres, because what needs pinning here is ORCHESTRATION -- which
window each bond is asked for, what is written, when a commit lands, and how a
provider failure is reported -- none of which needs a real planner. The SQL these
stages emit is separately drift-locked below and validated against production
before deploy.
"""
from __future__ import annotations

import datetime as _dt

import pytest

from src.bonds import live_daily
from src.workers import _finnhub, bond_live_daily


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.rowcount = len(rows) or 1

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _Cursor:
    def __init__(self, conn) -> None:
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._conn.writes.append((sql, params))
        self.rowcount = 1


class FakeConn:
    """Answers queries from a prefix->rows table; records every write."""

    def __init__(self, answers: dict[str, list[tuple]]) -> None:
        self.answers = answers
        self.writes: list[tuple] = []
        self.commits = 0

    def execute(self, sql, params=None):
        for marker, rows in self.answers.items():
            if marker in sql:
                return _Result(rows)
        return _Result([])

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1


class FakeClient:
    """Records the windows it was asked for; replays scripted payloads."""

    def __init__(self, candles=None, curve=None, ticks=None, fail: set | None = None) -> None:
        self._candles = candles or {}
        self._curve = curve or {}
        self._ticks = ticks or {}
        self._fail = fail or set()
        self.candle_calls: list[tuple] = []
        self.tick_calls: list[tuple] = []

    def daily_candles(self, isin, from_ts, to_ts):
        self.candle_calls.append((isin, from_ts, to_ts))
        if isin in self._fail:
            raise _finnhub.FinnhubTransientError("down")
        return self._candles.get(isin, {"s": "no_data"})

    def yield_curve(self, code):
        if code in self._fail:
            raise _finnhub.FinnhubTransientError("down")
        return self._curve.get(code, {"data": []})

    def ticks(self, isin, day, **kwargs):
        self.tick_calls.append((isin, day))
        return self._ticks.get(isin, {"t": []})

    def stats(self):
        return {"http_calls": len(self.candle_calls)}


DAY = _dt.date(2026, 8, 6)
TODAY = _dt.date(2026, 8, 7)
UNIVERSE = [("912828XX1", "US912828XX10", 4.0, _dt.date(2031, 8, 6))]


def _candle_payload(day: _dt.date, price: float, yield_pct: float | None = 4.5):
    return {"s": "ok", "t": [live_daily.to_epoch(day)], "c": [price],
            "y": [yield_pct] if yield_pct is not None else [None]}


# --------------------------------------------------------------------------- #
# Stage 1: candles
# --------------------------------------------------------------------------- #
def test_the_window_starts_at_each_bond_s_own_watermark() -> None:
    conn = FakeConn({"max(o.day)": [("912828XX1", DAY)]})
    client = FakeClient(candles={"US912828XX10": _candle_payload(TODAY, 99.0)})

    stats = bond_live_daily._load_candles(conn, client, UNIVERSE, TODAY)

    (_, from_ts, to_ts) = client.candle_calls[0]
    # The watermark day itself is re-read (a revised close must not be frozen).
    assert from_ts == live_daily.to_epoch(DAY)
    assert to_ts == live_daily.to_epoch(TODAY)
    assert stats["with_data"] == 1 and stats["last_day"] == TODAY.isoformat()
    assert conn.commits >= 1


def test_a_bond_the_provider_has_nothing_for_is_reported_not_failed() -> None:
    conn = FakeConn({"max(o.day)": []})
    stats = bond_live_daily._load_candles(conn, FakeClient(), UNIVERSE, TODAY)
    assert stats["no_data"] == 1
    assert stats["transient_failures"] == 0
    assert stats["aborted"] is False


def test_transient_provider_failures_are_counted_and_the_sweep_continues() -> None:
    universe = UNIVERSE + [("912828XX2", "US912828XX28", 4.0, _dt.date(2031, 8, 6))]
    conn = FakeConn({"max(o.day)": []})
    client = FakeClient(
        candles={"US912828XX28": _candle_payload(TODAY, 99.0)},
        fail={"US912828XX10"},
    )
    stats = bond_live_daily._load_candles(conn, client, universe, TODAY)
    assert stats["transient_failures"] == 1
    assert stats["with_data"] == 1
    assert stats["aborted"] is False


def test_a_sustained_provider_outage_aborts_rather_than_burning_the_window() -> None:
    isins = [f"US91282800{i:02d}" for i in range(60)]
    universe = [(f"9128280{i:02d}", isin, 4.0, _dt.date(2031, 8, 6))
                for i, isin in enumerate(isins)]
    conn = FakeConn({"max(o.day)": []})
    client = FakeClient(fail=set(isins))

    stats = bond_live_daily._load_candles(conn, client, universe, TODAY)

    assert stats["aborted"] is True
    assert stats["swept"] == _finnhub.MAX_CONSECUTIVE_FAILURES
    assert stats["swept"] < len(universe), "the sweep must stop, not finish"


def test_the_upsert_carries_the_full_declared_column_protocol() -> None:
    conn = FakeConn({"max(o.day)": []})
    client = FakeClient(candles={"US912828XX10": _candle_payload(TODAY, 99.0)})
    bond_live_daily._load_candles(conn, client, UNIVERSE, TODAY)

    sql, params = conn.writes[0]
    assert "INSERT INTO bond_observation_daily" in sql
    assert len(params) == len(live_daily.OBSERVATION_COLUMNS)
    # The idempotency rule itself: same-rank rows refresh, higher-rank ones win.
    assert "EXCLUDED.source_rank >= bond_observation_daily.source_rank" in sql


# --------------------------------------------------------------------------- #
# Stage 2: curve
# --------------------------------------------------------------------------- #
def test_one_dead_tenor_does_not_cost_the_others() -> None:
    conn = FakeConn({"max(day) FROM bond_yield_curve_daily": [(None,)]})
    client = FakeClient(
        curve={"10y": {"data": [{"d": "2026-08-06", "v": 4.69}]}},
        fail={"30y"},
    )
    stats = bond_live_daily._load_curve(conn, client)
    assert stats["failed_tenors"] == ["30y"]
    assert stats["tenors"] == 1
    assert stats["latest_day"] == "2026-08-06"


# --------------------------------------------------------------------------- #
# Stage 3: ticks
# --------------------------------------------------------------------------- #
def test_the_tick_lane_asks_for_the_previous_session_only() -> None:
    conn = FakeConn({
        "coalesce(sum(o.volume)": [("912828XX1", 1_000_000)],
    })
    client = FakeClient(ticks={"US912828XX10": {
        "t": [1, 2], "p": [99.0, 101.0], "si": [1, 2], "v": [10, 20],
    }})
    stats = bond_live_daily._load_ticks(conn, client, UNIVERSE, TODAY)
    assert client.tick_calls == [("US912828XX10", DAY.isoformat())]
    assert stats["traded"] == 1 and stats["day"] == DAY.isoformat()


# --------------------------------------------------------------------------- #
# Stage 5: republish
# --------------------------------------------------------------------------- #
def test_a_failed_republication_is_flagged_so_the_run_exits_non_zero(monkeypatch) -> None:
    """"Load and recompute" -- a day whose recompute failed is not a green day.

    Nothing retries it before tomorrow: the 11:00 chain's run_id does not change
    just because this worker failed, so a silent success here would leave the
    product a day stale with no signal at all.
    """
    from src.workers import bond_metrics, bond_serving

    monkeypatch.setattr(bond_metrics, "run", lambda *a, **k: {"state": "ok"})
    monkeypatch.setattr(
        bond_serving, "run", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    out = bond_live_daily._republish("postgresql://x")
    assert out["failed"] is True
    assert out["bond_serving"]["state"] == "failed"

    # A worker that merely REPORTS a failed state counts too, not just a raise.
    monkeypatch.setattr(bond_serving, "run", lambda *a, **k: {"state": "failed"})
    assert bond_live_daily._republish("postgresql://x")["failed"] is True

    monkeypatch.setattr(bond_serving, "run", lambda *a, **k: {"state": "ok"})
    assert bond_live_daily._republish("postgresql://x")["failed"] is False


def test_run_worker_reads_the_top_level_aborted_key() -> None:
    """The exit-code contract lives on that exact key -- keep them wired."""
    import inspect

    from src import run_worker

    assert 'stats.get("aborted")' in inspect.getsource(run_worker.main)
    assert '"aborted": aborted' in inspect.getsource(bond_live_daily.run)


# --------------------------------------------------------------------------- #
# Provider client
# --------------------------------------------------------------------------- #
class _Response:
    def __init__(self, body: bytes, headers: dict | None = None) -> None:
        self._body = body
        self.headers = headers or {}

    def read(self):
        return self._body

    def close(self):
        return None


def test_the_tick_request_always_asks_for_json() -> None:
    """Without format=json the redirect target serves CSV with a Go-fmt bug."""
    seen: list[str] = []

    def opener(url, timeout):
        seen.append(url)
        return _Response(b'{"t": [], "total": 0}')

    client = _finnhub.FinnhubClient("k", opener=opener, sleep=lambda _s: None)
    client.ticks("US912828XX10", "2026-08-06")
    assert "format=json" in seen[0]
    assert "exchange=trace" in seen[0]


def test_a_429_is_retried_and_a_400_is_not() -> None:
    import urllib.error

    calls = {"n": 0}

    def flaky(url, timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            raise urllib.error.HTTPError(url, 429, "slow down", {}, None)
        return _Response(b'{"s": "ok"}')

    client = _finnhub.FinnhubClient("k", opener=flaky, sleep=lambda _s: None)
    assert client.daily_candles("X", 0, 1) == {"s": "ok"}
    assert client.retries == 1

    def forbidden(url, timeout):
        raise urllib.error.HTTPError(url, 403, "nope", {}, None)

    hard = _finnhub.FinnhubClient("k", opener=forbidden, sleep=lambda _s: None)
    with pytest.raises(_finnhub.FinnhubConfigError):
        hard.daily_candles("X", 0, 1)


def test_retries_are_finite_and_end_in_a_typed_transient_error() -> None:
    def dead(url, timeout):
        raise OSError("connection reset")

    client = _finnhub.FinnhubClient("k", opener=dead, sleep=lambda _s: None)
    with pytest.raises(_finnhub.FinnhubTransientError):
        client.daily_candles("X", 0, 1)
    assert client.errors["network"] == _finnhub.MAX_RETRIES + 1


def test_a_nearly_drained_window_sleeps_to_the_reset_instant() -> None:
    slept: list[float] = []
    headers = {"X-Ratelimit-Remaining": "1", "X-Ratelimit-Reset": "1000"}

    client = _finnhub.FinnhubClient(
        "k",
        opener=lambda url, timeout: _Response(b'{"s":"ok"}', headers),
        sleep=slept.append,
        clock=lambda: 940.0,
    )
    client.daily_candles("X", 0, 1)
    assert slept and slept[0] == pytest.approx(61.0)


def test_an_absent_key_is_a_configuration_fault_not_an_empty_day(monkeypatch) -> None:
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    with pytest.raises(_finnhub.FinnhubConfigError):
        _finnhub.client_from_env()


# --------------------------------------------------------------------------- #
# Freshness drift locks (the 2e mechanism)
# --------------------------------------------------------------------------- #
def test_the_metric_inputs_read_the_dense_series_per_field() -> None:
    """price and ytm resolve on their OWN latest day, and duration settles on ytm's.

    Folding them into one latest-row rule would let a fresh price erase an older
    bond's yield -- and the duration solved from it.
    """
    from src.workers import bond_metrics

    sql = bond_metrics._inputs_sql(governed=True, live=True)
    assert "live_price" in sql and "live_yield" in sql
    assert "price_date" in sql and "ytm_date" in sql
    # Deterministic pick across a security's multiple CUSIP9 aliases.
    assert "ORDER BY l.security_id, o.day DESC, o.source_rank DESC, l.cusip9" in sql
    # The duration settles on the YIELD's date, never on the price's.
    assert "i.maturity_date <= i.ytm_date" in bond_metrics._DURATION_LATERALS
    assert "i.observation_date" not in bond_metrics._DURATION_LATERALS


def test_every_assembled_inputs_variant_binds_the_same_parameter() -> None:
    """All four lane combinations keep the %(as_of)s placeholder, so one call
    site can always bind one dict."""
    from src.workers import bond_metrics

    for governed in (True, False):
        for live in (True, False):
            assert "%(as_of)s" in bond_metrics._inputs_sql(governed=governed, live=live)


def test_the_serving_latest_lane_reads_the_resolved_observation() -> None:
    from src.bonds import serving_materializer as materializer

    assert "_bond_latest_observation" in materializer._LATEST_PRICE_PCT_SQL
    assert "_bond_latest_observation" in materializer._OBSERVATIONS_SQL
    # The dense row wins only when STRICTLY newer, and the prune runs first so
    # the inline scalar subquery can never see two rows for one security.
    assert "v.observation_date > g.observation_date" in materializer._LATEST_OBSERVATION_PRUNE
    assert "NOT EXISTS" in materializer._LATEST_OBSERVATION_MERGE
    # The fund_asof (point-in-time) lane is deliberately untouched.
    assert "bond_price_fund_asof_v1" in materializer._OBSERVATIONS_SQL


def test_the_serving_as_of_follows_the_freshest_input() -> None:
    """Without this the publication identity replays and the payload never moves."""
    import inspect

    from src.workers import bond_serving

    source = inspect.getsource(bond_serving._resolve_as_of)
    assert "bond_observation_daily" in source
    assert "max(anchors)" in source


def test_retention_keeps_the_app_pinned_publication() -> None:
    """Deleting what the app still points at is the worst failure available."""
    from src.workers import bond_serving

    assert "bond_serving_app_current_pointer" in bond_serving._KEEP_APP_PINNED
    assert "LIMIT 2" in bond_serving._KEEP_TWO_MOST_RECENT
    assert "LIMIT %s" in bond_serving._PRUNE_BATCH_SQL, "the delete must be batched"
