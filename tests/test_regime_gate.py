"""Tests for the regime_gate worker (COMBO Sprint 1 — LIVE debounced 2-of-3 gate).

Port of the validated Lean harness ``_live_gate_riskoff`` / ``_market_stress`` /
``_macro_quadrant`` (``lean-research/TaaCvarSuite/main.py``). Two layers, no live
database:

- Pure engine (``market_stress``, ``gate_votes``, ``macro_quadrant``,
  ``build_rows``) — the daily state machine: 2-of-3 vote (trend SPY<SMA200,
  credit HYG/IEF<SMA60, drawdown SPY 63d-DD>=6%) with a 21-day dwell-time
  debounce, plus the growth/inflation quadrant. Tested without DB/API.
- I/O layer (``_align``, ``_upsert``) — aligns the Tiingo closes onto SPY's date
  spine and upserts in chunks; exercised with fakes.
- ``run`` — env-gated integration test (DATABASE_URL + TIINGO_API_KEY) plus a
  lock-busy unit test with a monkeypatched advisory lock.
"""

from __future__ import annotations

import datetime as _dt
import os
import pathlib

import pytest

from src.workers import regime_gate as rg


# ──────────────────────────────────────────────────────────────────────────────
# market_stress: SPY drawdown from trailing 63d high, 12% => 1.0 (newest-first)
# ──────────────────────────────────────────────────────────────────────────────
def test_market_stress_no_drawdown_is_zero():
    closes = [100.0] * 70  # flat, newest-first
    assert rg.market_stress(closes) == 0.0


def test_market_stress_full_at_12pct():
    # newest-first: now=88 (index 0), trailing-63 high=100 => dd=0.12 => 1.0
    closes = [88.0] + [100.0] * 63
    assert abs(rg.market_stress(closes) - 1.0) < 1e-9


def test_market_stress_insufficient_history():
    assert rg.market_stress([100.0, 99.0]) == 0.0


# ──────────────────────────────────────────────────────────────────────────────
# gate_votes: trend / credit / drawdown legs and the count
# ──────────────────────────────────────────────────────────────────────────────
def test_gate_votes_counts_two_of_three():
    # trend down (spy<sma200) + drawdown (stress*0.12>=0.06) => 2 votes
    t, c, d, n = rg.gate_votes(
        spy_close=90.0, spy_sma200=100.0, ratio=1.0, ratio_sma60=0.9,
        spy_stress=0.6, gate_dd=0.06,
    )
    assert t is True and c is False and d is True and n == 2


def test_gate_votes_credit_leg():
    t, c, d, n = rg.gate_votes(
        spy_close=110.0, spy_sma200=100.0, ratio=0.8, ratio_sma60=0.9,
        spy_stress=0.0, gate_dd=0.06,
    )
    assert c is True and t is False and d is False and n == 1


def test_gate_votes_handles_warmup_none_legs():
    # SMA200 not ready and ratio missing => only drawdown can vote
    t, c, d, n = rg.gate_votes(
        spy_close=90.0, spy_sma200=None, ratio=None, ratio_sma60=None,
        spy_stress=1.0, gate_dd=0.06,
    )
    assert t is False and c is False and d is True and n == 1


# ──────────────────────────────────────────────────────────────────────────────
# macro_quadrant: growth (SPY 126d) x inflation (TIP/IEF breakeven 126d)
# ──────────────────────────────────────────────────────────────────────────────
def test_macro_quadrant_slowdown_growth_down_inflation_up():
    # newest-first: SPY down over 126d (growth<0), breakeven up over 126d (infl>0)
    spy = [90.0] + [100.0] * 126     # now 90 vs 126d-ago 100 -> growth down
    be = [1.10] + [1.00] * 126       # now 1.10 vs 1.00 -> breakeven up
    quad, g, i = rg.macro_quadrant(spy, be)
    assert quad == "slowdown" and g < 0 and i > 0


def test_macro_quadrant_expansion_growth_up_inflation_up():
    spy = [110.0] + [100.0] * 126    # growth up
    be = [1.10] + [1.00] * 126       # inflation up
    quad, g, i = rg.macro_quadrant(spy, be)
    assert quad == "expansion" and g > 0 and i > 0


def test_macro_quadrant_recovery_growth_up_inflation_down():
    spy = [110.0] + [100.0] * 126    # growth up
    be = [0.90] + [1.00] * 126       # inflation down
    quad, g, i = rg.macro_quadrant(spy, be)
    assert quad == "recovery" and g > 0 and i < 0


def test_macro_quadrant_contraction_growth_down_inflation_down():
    spy = [90.0] + [100.0] * 126     # growth down
    be = [0.90] + [1.00] * 126       # inflation down
    quad, g, i = rg.macro_quadrant(spy, be)
    assert quad == "contraction" and g < 0 and i < 0


def test_macro_quadrant_warmup_is_none():
    quad, g, i = rg.macro_quadrant([100.0, 99.0], [1.0, 1.0])
    assert quad is None and g is None and i is None


# ──────────────────────────────────────────────────────────────────────────────
# build_rows: the full daily state machine with 21d dwell-time debounce
# ──────────────────────────────────────────────────────────────────────────────
def test_build_rows_debounce_holds_21_days_before_flip():
    # 30 days of deep stress then nothing should flip risk_off only after 21d
    n = 260
    dates = [_dt.date(2020, 1, 1) + _dt.timedelta(days=i) for i in range(n)]
    # SPY: rise for 210 days (warmup for SMA200), then crash hard and stay low.
    spy = [100.0 + i * 0.1 for i in range(210)] + [70.0] * (n - 210)
    ratio = [1.0] * n  # credit leg neutral (ratio == sma60 ~ no stress)
    breakeven = [1.0] * n  # neutral inflation leg
    rows = rg.build_rows(dates, spy, ratio, breakeven, gate_confirm=21)
    # find first risk_off row
    off = [r for r in rows if r["state"] == "risk_off"]
    assert off, "gate should eventually latch risk_off under a sustained crash"
    first_off_idx = rows.index(off[0])
    # the crash starts at index 210; latch needs 21 consecutive confirms
    assert first_off_idx >= 210 + 21 - 1
    # exactly one flip row at the latch boundary
    assert off[0]["flip"] is True
    assert rows[first_off_idx - 1]["flip"] is False


def test_build_rows_emits_one_row_per_ready_day_with_schema():
    n = 210
    dates = [_dt.date(2021, 1, 1) + _dt.timedelta(days=i) for i in range(n)]
    spy = [100.0 + i for i in range(n)]
    ratio = [1.0] * n
    breakeven = [1.0] * n
    rows = rg.build_rows(dates, spy, ratio, breakeven)
    assert rows, "should emit rows once SMA windows are warm"
    assert set(rows[-1]) >= {
        "regime_date", "state", "trend_vote", "credit_vote",
        "drawdown_vote", "vote_count", "flip", "dwell_days",
        "growth_score", "inflation_score", "quadrant",
        "spy_close", "hyg_ief_ratio", "tip_ief_ratio", "spy_dd",
    }
    assert rows[-1]["state"] in {"risk_on", "risk_off"}
    assert rows[-1]["quadrant"] in {
        "recovery", "expansion", "slowdown", "contraction", None,
    }


def test_build_rows_initial_state_is_risk_on_and_dwell_counts():
    n = 210
    dates = [_dt.date(2021, 1, 1) + _dt.timedelta(days=i) for i in range(n)]
    spy = [100.0 + i for i in range(n)]  # steady rise -> never risk_off
    ratio = [1.0] * n
    breakeven = [1.0] * n
    rows = rg.build_rows(dates, spy, ratio, breakeven)
    # all risk_on; dwell_days strictly increments and the first emitted row is 1.
    assert all(r["state"] == "risk_on" for r in rows)
    assert rows[0]["dwell_days"] == 1
    assert rows[1]["dwell_days"] == 2
    assert all(r["flip"] is False for r in rows)


# ──────────────────────────────────────────────────────────────────────────────
# Task 2: DDL + advisory lock registration
# ──────────────────────────────────────────────────────────────────────────────
def test_ddl_file_exists_and_declares_table():
    sql = (pathlib.Path(__file__).resolve().parents[1]
           / "schemas" / "regime_gate.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS regime_gate_daily" in sql
    assert "regime_gate_daily_pkey PRIMARY KEY (regime_date)" in sql
    assert "ck_regime_gate_state" in sql
    assert "ck_regime_gate_quadrant" in sql
    assert "quadrant" in sql and "growth_score" in sql


def test_lock_id_registered_and_unique():
    from src import db
    assert db.LOCK_REGIME_GATE == 900_207
    # no collision with the highest existing 900_2xx lock
    assert db.LOCK_REGIME_GATE != db.LOCK_REGIME_COMPOSITE


# ──────────────────────────────────────────────────────────────────────────────
# Task 3: I/O layer — align + upsert (fakes, no real DB)
# ──────────────────────────────────────────────────────────────────────────────
class _FakeCursor:
    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def executemany(self, sql, params):
        self.sink.extend(params)

    def execute(self, sql, params=None):
        pass


class _FakeConn:
    def __init__(self):
        self.sink = []

    def cursor(self):
        return _FakeCursor(self.sink)

    def execute(self, *a, **k):
        pass

    def commit(self):
        pass


def test_upsert_chunks_all_rows():
    conn = _FakeConn()
    rows = [{
        "regime_date": _dt.date(2020, 1, 1) + _dt.timedelta(days=i),
        "state": "risk_on", "trend_vote": False, "credit_vote": False,
        "drawdown_vote": False, "vote_count": 0, "flip": False,
        "dwell_days": i + 1, "growth_score": 0.0, "inflation_score": 0.0,
        "quadrant": None, "spy_close": 100.0, "hyg_ief_ratio": 1.0,
        "tip_ief_ratio": 1.0, "spy_dd": 0.0,
    } for i in range(2500)]
    n = rg._upsert(conn, rows)
    assert n == 2500
    assert len(conn.sink) == 2500


def test_align_builds_ratio_and_breakeven_carrying_forward():
    spy = [(_dt.date(2020, 1, d), 100.0 + d) for d in range(1, 6)]
    hyg = [(_dt.date(2020, 1, 2), 90.0), (_dt.date(2020, 1, 4), 88.0)]
    ief = [(_dt.date(2020, 1, 2), 100.0), (_dt.date(2020, 1, 4), 110.0)]
    tip = [(_dt.date(2020, 1, 3), 120.0)]
    dates, s, ratio, be = rg._align(spy, hyg, ief, tip)
    assert dates[0] == _dt.date(2020, 1, 1)
    assert ratio[0] is None          # before first HYG/IEF obs
    assert abs(ratio[1] - 0.90) < 1e-9   # 90/100
    assert abs(ratio[2] - 0.90) < 1e-9   # carried forward
    assert abs(ratio[3] - 0.80) < 1e-9   # 88/110
    assert be[1] is None             # before first TIP obs
    assert abs(be[2] - 1.20) < 1e-9      # 120/100 (IEF carried from day 2)


def test_align_raises_on_empty_spy():
    with pytest.raises(RuntimeError):
        rg._align([], [], [], [])


# ──────────────────────────────────────────────────────────────────────────────
# Task 4: run() entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def test_run_returns_lock_busy_sentinel(monkeypatch):
    """When the advisory lock can't be acquired, run() bails with a sentinel."""
    import contextlib

    @contextlib.contextmanager
    def _busy_lock(conn, lock_id):
        yield False

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def commit(self):
            pass

    monkeypatch.setattr(rg, "connect", lambda dsn: _Conn())
    monkeypatch.setattr(rg, "advisory_lock", _busy_lock)
    out = rg.run("postgresql://unused")
    assert out["skipped"] == "lock_busy"
    assert out["days"] == 0 and out["upserted"] == 0


def _env() -> dict[str, str]:
    env_file = pathlib.Path(__file__).resolve().parents[1] / ".env"
    out: dict[str, str] = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"')
    out.update({k: v for k, v in os.environ.items()
                if k in ("DATABASE_URL", "TIINGO_API_KEY")})
    return out


def test_run_real_history_is_idempotent():
    """Full run vs the cloud + Tiingo; ~2003->today daily. Idempotent recompute."""
    env = _env()
    for key in ("DATABASE_URL", "TIINGO_API_KEY"):
        if not env.get(key):
            pytest.skip(f"{key} not configured")
    os.environ.setdefault("TIINGO_API_KEY", env["TIINGO_API_KEY"])
    dsn = env["DATABASE_URL"]
    stats = rg.run(dsn)
    assert stats["days"] > 3_000          # ~2003->today daily, post warmup
    assert stats["state"] in {"risk_on", "risk_off"}
    assert stats["quadrant"] in {
        "recovery", "expansion", "slowdown", "contraction", None,
    }
    assert isinstance(stats["flips"], int)
    stats2 = rg.run(dsn)
    assert stats2["days"] == stats["days"]   # idempotent recompute
