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
        lambda conn, limit, ttl_days, window_days, retry_no_figi=False: seen.update(
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
        lambda conn, limit, ttl_days, window_days, retry_no_figi=False: seen.update(
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


# ── issuer coherence (routes A/B/C) ──────────────────────────────────────────
# Deterministic, in-database resolution before any external call. The conflict
# guard is the load-bearing part: an issuer seen with more than one sector is
# never copied — measured at 27 of 41.9k resolved names (0,06 %), rare but the
# reason the copy stays honest.


def test_merge_takes_the_strongest_clean_route():
    rows = [
        ("KY0000000001", "c9", "Industrials"),
        ("KY0000000001", "c6", "Industrials"),
        ("KY0000000001", "name", "Health Care"),  # weaker route disagrees — c9 wins
        ("BR0000000002", "c6", "Materials"),
        ("BR0000000002", "name", "Materials"),
        ("AU0000000003", "name", "Financials"),
    ]

    resolved, conflicts = nce.merge_issuer_candidates(rows)

    assert resolved == {
        "KY0000000001": "Industrials",
        "BR0000000002": "Materials",
        "AU0000000003": "Financials",
    }
    assert conflicts == set()


def test_merge_conflict_guard_blocks_a_multi_sector_route_but_not_the_isin():
    rows = [
        # c9 saw two sectors (two candidate CUSIPs) — that route is out, but the
        # clean c6 route below still resolves the ISIN.
        ("CA0000000001", "c9", "Energy"),
        ("CA0000000001", "c9", "Utilities"),
        ("CA0000000001", "c6", "Energy"),
    ]

    resolved, conflicts = nce.merge_issuer_candidates(rows)

    assert resolved == {"CA0000000001": "Energy"}
    assert conflicts == set()


def test_merge_marks_a_conflict_when_no_route_is_clean():
    rows = [
        ("VG0000000001", "name", "Financials"),
        ("VG0000000001", "name", "Industrials"),
    ]

    resolved, conflicts = nce.merge_issuer_candidates(rows)

    assert resolved == {}
    assert conflicts == {"VG0000000001"}  # marked, counted, passed on — not copied


def test_merge_ignores_isins_no_route_saw():
    resolved, conflicts = nce.merge_issuer_candidates([])
    assert resolved == {}
    assert conflicts == set()


class _SeqCursor:
    """Returns one canned result set per execute(), recording each binding."""

    def __init__(self, results):
        self._results = list(results)
        self.executed = []
        self._current = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._current = self._results.pop(0)

    def fetchall(self):
        return self._current


class _SeqConn:
    def __init__(self, results):
        self.cursor_obj = _SeqCursor(results)

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass


def test_resolve_by_issuer_binds_the_three_route_queries_and_merges():
    isins = ["KY1234567890", "AU0000000001"]
    conn = _SeqConn([
        # candidate CUSIPs: the embedded one plus a reported one for the KY ISIN
        [("KY1234567890", "123456789"), ("KY1234567890", "011111111")],
        # routes A/B against the map
        [("KY1234567890", "c9", "Industrials")],
        # route C by issuer name
        [("AU0000000001", "Financials")],
    ])

    res = nce.resolve_by_issuer(conn, isins, 1100)

    sqls = [sql for sql, _ in conn.cursor_obj.executed]
    assert sqls == [
        nce._CUSIP_CANDIDATES_SQL,
        nce._CUSIP_SECTOR_SQL,
        nce._ISSUER_NAME_SECTOR_SQL,
    ]
    _, cand_params = conn.cursor_obj.executed[0]
    assert cand_params == {
        "isins": isins, "cgs": list(nce._CGS_PREFIXES), "window_days": 1100,
    }
    _, map_params = conn.cursor_obj.executed[1]
    assert map_params == {
        "isins": ["KY1234567890", "KY1234567890"],
        "cusips": ["123456789", "011111111"],
    }
    _, name_params = conn.cursor_obj.executed[2]
    assert name_params == {"isins": isins, "window_days": 1100}

    assert res.sectors == {
        "KY1234567890": "Industrials",
        "AU0000000001": "Financials",
    }
    assert res.conflicts == frozenset()
    # The embedded CUSIP leads even when a reported one sorts first
    # alphabetically — the ID_CUSIP leg tries the security's own NSIN first.
    assert res.cusips == {"KY1234567890": ["123456789", "011111111"]}


def test_resolve_by_issuer_skips_the_map_query_without_candidates():
    conn = _SeqConn([
        [],                          # no candidate CUSIPs at all
        [("IN0000000001", "Energy")],  # name route only
    ])

    res = nce.resolve_by_issuer(conn, ["IN0000000001"], 120)

    sqls = [sql for sql, _ in conn.cursor_obj.executed]
    assert sqls == [nce._CUSIP_CANDIDATES_SQL, nce._ISSUER_NAME_SECTOR_SQL]
    assert res.sectors == {"IN0000000001": "Energy"}
    assert res.cusips == {}


def test_resolve_by_issuer_empty_batch_touches_nothing():
    conn = _SeqConn([])
    res = nce.resolve_by_issuer(conn, [], 120)
    assert conn.cursor_obj.executed == []
    assert res == nce.IssuerResolution({}, frozenset(), {})


# ── ID_CUSIP provenance ──────────────────────────────────────────────────────


def test_enrich_rows_tags_cusip_matches_with_their_own_provenance():
    isins = ["KY0000000001", "CA0000000002", "VG0000000003"]
    matches = {
        "KY0000000001": _m("AAPL", "US"),     # matched through ID_CUSIP
        "CA0000000002": _m("SHOP", "CT"),     # matched through ID_ISIN as always
    }
    sectors = {"AAPL": "Information Technology"}

    rows = {
        r.isin: r
        for r in nce.enrich_rows(
            isins, matches, sectors.get, via_cusip={"KY0000000001"}
        )
    }

    assert rows["KY0000000001"].resolved_via == "openfigi_cusip+yfinance"
    assert rows["KY0000000001"].gics_sector == "Information Technology"
    assert rows["CA0000000002"].resolved_via == "openfigi_no_sector"  # unchanged leg
    # An ISIN nothing matched stays no_figi even if a CUSIP was tried for it.
    assert rows["VG0000000003"].resolved_via == "no_figi"


def test_enrich_rows_cusip_match_without_sector_is_its_own_miss_reason():
    matches = {"KY0000000001": _m("AAPL", "US")}
    rows = nce.enrich_rows(
        ["KY0000000001"], matches, lambda s: None, via_cusip={"KY0000000001"}
    )
    assert rows[0].resolved_via == "openfigi_cusip_no_sector"


# ── WORKER_RETRY_NO_FIGI ─────────────────────────────────────────────────────
# The 2026-08 campaign cached 1.635 fresh no_figi rows whose TTL only reopens
# in November; this switch lets one maintenance run re-attack them today.


def test_retry_no_figi_defaults_off_and_accepts_explicit_off():
    assert nce.resolve_retry_no_figi(None) is False
    assert nce.resolve_retry_no_figi("") is False
    assert nce.resolve_retry_no_figi("   ") is False
    assert nce.resolve_retry_no_figi("0") is False
    assert nce.resolve_retry_no_figi("1") is True
    assert nce.resolve_retry_no_figi(" 1 ") is True


@pytest.mark.parametrize("raw", ["true", "yes", "on", "2", "10", "１"])
def test_retry_no_figi_refuses_a_spelling_it_cannot_honour(raw):
    with pytest.raises(ValueError) as exc:
        nce.resolve_retry_no_figi(raw)
    message = str(exc.value)
    assert nce.RETRY_NO_FIGI_ENV in message
    assert repr(raw) in message


def test_gather_binds_the_retry_query_when_asked():
    conn = _FakeConn(rows=[("KY0000000001",)])

    isins = nce._gather_isins(conn, 100, 90, 1100, retry_no_figi=True)

    assert isins == ["KY0000000001"]
    sql, params = conn.cursors[0].executed[0]
    assert sql == nce._GATHER_RETRY_SQL
    assert params == (1100, 100)  # no TTL bind — the retry ignores freshness


def _stub_pipeline(monkeypatch, *, gathered, issuer, isin_matches, cusip_matches,
                   sectors):
    """Stub every network/DB edge of run() and capture the upserted rows."""
    _stub_db(monkeypatch, _FakeConn())
    monkeypatch.setattr(
        nce, "_gather_isins",
        lambda conn, limit, ttl_days, window_days, retry_no_figi=False: gathered,
    )
    monkeypatch.setattr(
        nce, "resolve_by_issuer", lambda conn, isins, window_days: issuer
    )

    class _FakeFigi:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def map_isins(self, isins):
            return dict(isin_matches)

        def map_cusips(self, cusips):
            self.cusips_asked = list(cusips)
            return dict(cusip_matches)

    fake_figi = _FakeFigi()
    monkeypatch.setattr(nce, "OpenFigiClient", lambda: fake_figi)
    monkeypatch.setattr(nce, "_fetch_sectors", lambda matches: dict(sectors))
    upserted = []
    monkeypatch.setattr(
        nce, "_upsert", lambda conn, rows: upserted.extend(rows)
    )
    return fake_figi, upserted


def test_retry_run_writes_issuer_coherence_and_redates_the_leftover_miss(
    monkeypatch,
):
    monkeypatch.setenv(nce.RETRY_NO_FIGI_ENV, "1")
    monkeypatch.delenv(nce.WINDOW_DAYS_ENV, raising=False)
    issuer = nce.IssuerResolution(
        sectors={"CA0000000002": "Industrials"},
        conflicts=frozenset({"BR0000000004"}),
        cusips={"KY0000000001": ["123456789"]},
    )
    _, upserted = _stub_pipeline(
        monkeypatch,
        gathered=["CA0000000002", "KY0000000001", "BR0000000004"],
        issuer=issuer,
        isin_matches={},
        cusip_matches={},
        sectors={},
    )

    stats = nce.run("postgresql://stub")

    rows = {r.isin: r for r in upserted}
    assert rows["CA0000000002"].resolved_via == "issuer_coherence"
    assert rows["CA0000000002"].gics_sector == "Industrials"
    assert rows["CA0000000002"].ticker is None
    assert rows["CA0000000002"].yahoo_symbol is None
    # Everything no route and no index resolved is rewritten as no_figi — the
    # upsert re-dates last_verified_at, so the miss goes back to sleep for a
    # TTL instead of being retried every cycle. Deliberate, not incidental.
    assert rows["KY0000000001"].resolved_via == "no_figi"
    assert rows["BR0000000004"].resolved_via == "no_figi"
    assert stats["retry_no_figi"] is True
    assert stats["gathered"] == 3
    assert stats["issuer_resolved"] == 1
    assert stats["issuer_conflicts"] == 1
    assert stats["figi_resolved"] == 0
    assert stats["figi_cusip_resolved"] == 0
    assert stats["sector_resolved"] == 1
    assert stats["upserted"] == 3


def test_run_maps_leftover_cusips_and_reports_the_leg_separately(monkeypatch):
    monkeypatch.delenv(nce.RETRY_NO_FIGI_ENV, raising=False)
    monkeypatch.delenv(nce.WINDOW_DAYS_ENV, raising=False)
    issuer = nce.IssuerResolution(
        sectors={},
        conflicts=frozenset(),
        cusips={"KY0000000001": ["123456789"], "CA0000000002": ["987654321"]},
    )
    fake_figi, upserted = _stub_pipeline(
        monkeypatch,
        gathered=["KY0000000001", "CA0000000002"],
        issuer=issuer,
        # The ISIN index resolves CA; only KY's CUSIP goes to ID_CUSIP.
        isin_matches={"CA0000000002": _m("SHOP", "CT")},
        cusip_matches={"123456789": _m("AAPL", "US")},
        sectors={"AAPL": "Information Technology", "SHOP.TO": "Financials"},
    )

    stats = nce.run("postgresql://stub")

    assert fake_figi.cusips_asked == ["123456789"]  # not CA's — ISIN already hit
    rows = {r.isin: r for r in upserted}
    assert rows["KY0000000001"].resolved_via == "openfigi_cusip+yfinance"
    assert rows["CA0000000002"].resolved_via == "openfigi+yfinance"
    assert stats["figi_resolved"] == 1
    assert stats["figi_cusip_resolved"] == 1
    assert stats["sector_resolved"] == 2
    assert stats["retry_no_figi"] is False
