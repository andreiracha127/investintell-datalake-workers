"""Tests for the international-equity sector enrichment worker.

Pure ``enrich_rows`` and the ``NPORT_ENRICH_WINDOW_DAYS`` resolution — no
network. The OpenFIGI/yfinance paths are exercised live in a smoke run
(rate-limited external APIs); the DB path is driven here through a fake
connection, because what the window binds into the gather query is exactly the
thing an operator cannot verify from a green deploy.
"""

from __future__ import annotations

import contextlib

import pytest

from src.workers import nport_cusip_enrichment as nce
from src.workers._openfigi import FigiMatch


def _m(ticker, exch):
    return FigiMatch(ticker=ticker, exch_code=exch, figi="BBG", market_sector="Equity",
                     security_type="Common Stock")


def test_enrich_rows_resolves_and_caches_each_miss_reason():
    isins = ["TW0002330008", "CNE000000001", "AEA000201011", "XX0000000000"]
    matches = {
        "TW0002330008": _m("2330", "TT"),       # resolves to a sector
        "CNE000000001": _m("600519", "CH"),      # symbol built, but no sector returned
        "AEA000201011": _m("ADCB", "UH"),        # exchange not in the crosswalk
        # "XX..." absent → OpenFIGI found nothing
    }
    sector_by_symbol = {"2330.TW": "Information Technology"}  # only TW has a sector

    rows = {r.isin: r for r in nce.enrich_rows(isins, matches, sector_by_symbol.get)}

    assert rows["TW0002330008"].gics_sector == "Information Technology"
    assert rows["TW0002330008"].yahoo_symbol == "2330.TW"
    assert rows["TW0002330008"].resolved_via == "openfigi+yfinance"

    assert rows["CNE000000001"].gics_sector is None
    assert rows["CNE000000001"].yahoo_symbol == "600519.SS"   # symbol built
    assert rows["CNE000000001"].resolved_via == "openfigi_no_sector"

    assert rows["AEA000201011"].gics_sector is None
    assert rows["AEA000201011"].yahoo_symbol is None          # unmapped exchange
    assert rows["AEA000201011"].ticker == "ADCB"
    assert rows["AEA000201011"].resolved_via == "no_yahoo_symbol"

    assert rows["XX0000000000"].gics_sector is None
    assert rows["XX0000000000"].resolved_via == "no_figi"


def test_enrich_rows_empty():
    assert nce.enrich_rows([], {}, lambda s: None) == []


# ── NPORT_ENRICH_WINDOW_DAYS ─────────────────────────────────────────────────
# The gather scan is anchored on max(report_date), so the 120-day default is
# blind to repaired history behind it: the 3.804 foreign-equity ISINs of the
# eight report_dates repaired on 2026-08-05 sit 8 to 26 months outside it, and
# before this variable no invocation of the worker could reach them.


class _FakeCursor:
    """Records every execute() so the bound parameters can be asserted."""

    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows=()):
        self.cursors = []
        self._rows = list(rows)

    def cursor(self):
        cur = _FakeCursor(self._rows)
        self.cursors.append(cur)
        return cur

    def commit(self):
        pass


def test_window_days_defaults_to_the_unchanged_120_day_sweep():
    assert nce._RECENT_REPORT_DAYS == 120
    assert nce.resolve_window_days(None) == 120
    assert nce.resolve_window_days("") == 120
    assert nce.resolve_window_days("   ") == 120


def test_window_days_accepts_a_plain_count_of_days():
    # 884 = 2026-01-31 − 2023-08-31, the window that reaches the oldest of the
    # eight repaired report_dates.
    assert nce.resolve_window_days("884") == 884
    assert nce.resolve_window_days(" 884 ") == 884
    assert nce.resolve_window_days("0") == 0


@pytest.mark.parametrize(
    "raw", ["120d", "1.5", "-30", "+120", "1_000", "twelve", "1 2 0", "１２０"]
)
def test_window_days_refuses_a_spelling_it_cannot_honour(raw):
    """Loud by name and by value — never a silent fallback to the default.

    Falling back would hand the operator a green run over the default window
    while the config claims a historical sweep, which is the same failure shape
    as a dropped WORKER_LIMIT: the deploy looks right and the verdict is wrong.
    """
    with pytest.raises(ValueError) as exc:
        nce.resolve_window_days(raw)
    message = str(exc.value)
    assert nce.WINDOW_DAYS_ENV in message
    assert repr(raw) in message


def test_the_resolved_window_is_the_first_bind_of_the_gather_query():
    conn = _FakeConn(rows=[("GB0000000001",), ("JP0000000002",)])

    isins = nce._gather_isins(conn, 5000, 90, 884)

    assert isins == ["GB0000000001", "JP0000000002"]
    sql, params = conn.cursors[0].executed[0]
    assert sql == nce._GATHER_SQL
    assert params == (884, 90, 5000)


def _stub_db(monkeypatch, conn):
    @contextlib.contextmanager
    def _connect(_dsn):
        yield conn

    @contextlib.contextmanager
    def _lock(_conn, _key):
        yield True

    monkeypatch.setattr(nce, "connect", _connect)
    monkeypatch.setattr(nce, "resolve_dsn", lambda dsn=None: "postgresql://stub")
    monkeypatch.setattr(nce, "advisory_lock", _lock)
    monkeypatch.setattr(nce, "ensure_schema", lambda _conn: None)


def test_run_reads_the_environment_and_reports_the_window(monkeypatch):
    monkeypatch.setenv(nce.WINDOW_DAYS_ENV, "884")
    seen = {}
    _stub_db(monkeypatch, _FakeConn())
    monkeypatch.setattr(
        nce,
        "_gather_isins",
        lambda conn, limit, ttl_days, window_days: seen.update(
            limit=limit, ttl_days=ttl_days, window_days=window_days
        )
        or [],
    )

    stats = nce.run("postgresql://stub")

    assert seen["window_days"] == 884
    assert seen["limit"] == nce.DEFAULT_RUN_LIMIT
    # The window is in the stats because the run log is the only place an
    # operator can check which history a green run actually scanned.
    assert stats["window_days"] == 884
    assert stats["gathered"] == 0


def test_run_without_the_variable_sweeps_the_120_day_default(monkeypatch):
    monkeypatch.delenv(nce.WINDOW_DAYS_ENV, raising=False)
    seen = {}
    _stub_db(monkeypatch, _FakeConn())
    monkeypatch.setattr(
        nce,
        "_gather_isins",
        lambda conn, limit, ttl_days, window_days: seen.update(
            window_days=window_days
        )
        or [],
    )

    stats = nce.run("postgresql://stub")

    assert seen["window_days"] == 120
    assert stats["window_days"] == 120


def test_run_refuses_a_bad_window_before_it_opens_a_connection(monkeypatch):
    monkeypatch.setenv(nce.WINDOW_DAYS_ENV, "4 months")

    def _never(*_a, **_k):
        raise AssertionError("connected despite an unusable window")

    monkeypatch.setattr(nce, "connect", _never)

    with pytest.raises(ValueError, match=nce.WINDOW_DAYS_ENV):
        nce.run("postgresql://stub")
