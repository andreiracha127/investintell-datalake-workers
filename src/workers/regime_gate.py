"""regime_gate worker — LIVE debounced 2-of-3 risk-off gate + macro quadrant.

COMBO Sprint 1. Materializes a daily LIVE debounced 2-of-3 cross-asset risk-off
gate PLUS the growth/inflation quadrant into ``regime_gate_daily``, ported
faithfully from the validated Lean harness (``lean-research/TaaCvarSuite/main.py``,
variant COMBO): ``_live_gate_riskoff`` (main.py:674-708), ``_market_stress``
(main.py:1026-1037), ``_macro_quadrant`` (main.py:710-739).

  state = risk_off  ⇔  the raw 2-of-3 vote held gate_confirm=21 consecutive days
    (dwell-time hysteresis — the robust innovation the frozen regime_composite
    LACKED; it was stuck risk_on since 2020-06 and missed the entire 2022 bear).
  Raw vote (raw_off ⇔ >= 2 of):
    trend    : SPY < SMA200
    credit   : HYG/IEF ratio < SMA60(ratio)   (the VALIDATED rule; SMA60 from the
               raw HYG/IEF closes the worker fetches — NOT credit_regime_daily's
               ``ratio < p20_5y``, which is a DIFFERENT rule)
    drawdown : SPY 63d-drawdown >= gate_dd (0.06)
  Quadrant (growth x inflation clock): growth = SPY 126d return sign; inflation =
    (TIP/IEF breakeven) 126d momentum sign. SLOWDOWN (growth down, inflation up)
    routes the allocator to the gold haven (downstream sprint).

Decisions inherited from the design spec (decision A, §9 — not optional):
  * The worker is SELF-CONTAINED: it fetches SPY, HYG, IEF, TIP via Tiingo (the
    exact credit_regime._fetch_prices pattern, extended 2->4 tickers) and computes
    ratio = HYG/IEF, SMA60(ratio), SPY SMA200, SPY 63d drawdown, and the TIP/IEF
    breakeven from those raw closes. It does NOT reuse credit_regime_daily.ratio
    (different rule — would break backtest fidelity). TIP is required for the
    inflation leg because TIP/IEF are NOT in the backend's eod_prices, so the
    worker is the only place the quadrant can be computed.
  * The whole series is recomputed each run (adjusted closes change retroactively
    on dividends), upserted via INSERT ... ON CONFLICT DO UPDATE in 1000-row chunks.
  * SPY is the date spine; HYG/IEF/TIP are carried forward on non-print days. TIP
    gaps (or short TIP history) leave quadrant=None for those days without
    disabling the gate.

Contract:  run(dsn, *, calc_date=None, limit=None)
           -> {"days", "upserted", "state", "vote_count", "flips", "last_flip",
               "dwell_days", "quadrant", "calc_date"}
"""

from __future__ import annotations

import datetime as _dt
import os
from typing import Any

from src.db import LOCK_REGIME_GATE, advisory_lock, connect

# Engine / fetch constants (ported from main.py COMBO config).
GATE_CONFIRM_DEFAULT = 21    # dwell-time debounce: raw vote must hold 21 days
GATE_DD_DEFAULT = 0.06       # drawdown leg threshold (SPY 63d-DD >= 6%)
SMA_TREND = 200              # trend leg: SPY < SMA200
SMA_CREDIT = 60             # credit leg: HYG/IEF < SMA60(ratio)
STRESS_WINDOW = 63           # _market_stress trailing-high window
GROWTH_LOOK = 126            # growth axis: SPY 126d return
INFLATION_LOOK = 126         # inflation axis: TIP/IEF breakeven 126d momentum
STRESS_FULL_DD = 0.12        # _market_stress: 12% drawdown => full stress (1.0)

SPY_TICKER = "SPY"
HYG_TICKER = "HYG"
IEF_TICKER = "IEF"
TIP_TICKER = "TIP"
HISTORY_START = _dt.date(2003, 1, 1)   # IEF inception 2002-07; TIP 2003-12
INSERT_CHUNK = 1_000


# ──────────────────────────────────────────────────────────────────────────────
# Pure engine — port of the Lean harness, no I/O
# ──────────────────────────────────────────────────────────────────────────────
def market_stress(
    spy_closes_desc: list[float],
    *,
    window: int = STRESS_WINDOW,
) -> float:
    """Continuous market-stress score in [0,1]: SPY drawdown from its trailing
    ``window``-day high, scaled so a 12% drawdown => 1.0 (port _market_stress,
    main.py:1026-1037). ``spy_closes_desc`` is newest-first. Returns 0.0 with
    fewer than ``window + 1`` points.
    """
    if len(spy_closes_desc) < window + 1:
        return 0.0
    recent = spy_closes_desc[: window + 1]   # newest first
    hi = max(recent)
    now = recent[0]
    dd = (hi - now) / hi if hi > 0 else 0.0
    return min(1.0, max(0.0, dd / STRESS_FULL_DD))


def gate_votes(
    spy_close: float,
    spy_sma200: float | None,
    ratio: float | None,
    ratio_sma60: float | None,
    spy_stress: float,
    *,
    gate_dd: float = GATE_DD_DEFAULT,
) -> tuple[bool, bool, bool, int]:
    """The daily 2-of-3 raw vote legs (port _live_gate_riskoff, main.py:680-695).

    Returns ``(trend_down, credit_stress, drawdown_stress, vote_count)``.
      trend_down       = spy_sma200 is not None and spy_close < spy_sma200
      credit_stress    = ratio/ratio_sma60 ready and ratio < ratio_sma60
      drawdown_stress  = spy_stress * 0.12 >= gate_dd  (i.e. real dd >= gate_dd;
                         _market_stress = dd / 0.12, so this is exactly dd >= gate_dd)
    """
    trend_down = spy_sma200 is not None and spy_close < spy_sma200
    credit_stress = (
        ratio is not None and ratio_sma60 is not None and ratio < ratio_sma60
    )
    drawdown_stress = spy_stress * STRESS_FULL_DD >= gate_dd
    vote_count = int(trend_down) + int(credit_stress) + int(drawdown_stress)
    return trend_down, credit_stress, drawdown_stress, vote_count


def macro_quadrant(
    spy_126: list[float],
    tip_ief_126: list[float],
    *,
    g_look: int = GROWTH_LOOK,
    i_look: int = INFLATION_LOOK,
) -> tuple[str | None, float | None, float | None]:
    """Growth x inflation clock (port _macro_quadrant, main.py:710-739).

    ``spy_126`` / ``tip_ief_126`` are newest-first windows of the SPY close and
    the TIP/IEF breakeven ratio. growth = spy_126[0]/spy_126[g_look] - 1 (sign ->
    growth_up); infl = tip_ief_126[0]/tip_ief_126[i_look] - 1 (sign -> infl_up;
    rising breakeven => inflation up). Returns ``(quadrant, growth_score,
    inflation_score)`` with quadrant in {recovery, expansion, slowdown,
    contraction}, or ``(None, None, None)`` during the warmup (a window shorter
    than ``look + 1`` points, or a non-positive denominator).
    """
    def _ret(win: list[float], k: int) -> float | None:
        if len(win) <= k:
            return None
        now = win[0]
        then = win[k]
        return (now / then - 1.0) if then > 0 else None

    growth = _ret(spy_126, g_look)
    infl = _ret(tip_ief_126, i_look)
    if growth is None or infl is None:
        return None, None, None
    growth_up = growth > 0.0
    infl_up = infl > 0.0
    if growth_up and not infl_up:
        quad = "recovery"      # growth up, inflation down
    elif growth_up and infl_up:
        quad = "expansion"     # growth up, inflation up
    elif (not growth_up) and infl_up:
        quad = "slowdown"      # growth down, inflation up
    else:
        quad = "contraction"   # growth down, inflation down
    return quad, growth, infl


def _trailing_mean(values: list[float], end_exclusive: int, window: int) -> float | None:
    """Simple trailing mean of the ``window`` values ending at ``end_exclusive``
    (exclusive), i.e. ``values[end_exclusive-window:end_exclusive]``. Returns
    ``None`` until there are ``window`` points available — matches the harness's
    SMA ``is_ready`` warmup (the SMA does not see the current bar's future)."""
    if end_exclusive < window:
        return None
    return sum(values[end_exclusive - window:end_exclusive]) / window


def build_rows(
    dates: list[_dt.date],
    spy: list[float],
    ratio: list[float | None],
    breakeven: list[float | None],
    *,
    gate_confirm: int = GATE_CONFIRM_DEFAULT,
    gate_dd: float = GATE_DD_DEFAULT,
    sma_trend: int = SMA_TREND,
    sma_credit: int = SMA_CREDIT,
    stress_window: int = STRESS_WINDOW,
    g_look: int = GROWTH_LOOK,
    i_look: int = INFLATION_LOOK,
) -> list[dict[str, Any]]:
    """Run the full daily state machine over the aligned series (oldest->newest).

    One row per day. The latched ``state`` starts risk_on (gate_off=False in the
    harness) and flips via the dwell-time hysteresis exactly as main.py:698-707.
    SMAs are simple trailing means over the PRIOR closes (warmup => None leg, the
    vote simply does not fire). ``quadrant`` is None during the 126d warmup.
    ``flip`` marks a day whose latched state differs from the previous day;
    ``dwell_days`` counts consecutive days the latched state has held (reset to 1
    on a flip).
    """
    n = len(dates)
    if not (len(spy) == len(ratio) == len(breakeven) == n):
        raise ValueError("dates/spy/ratio/breakeven must be the same length")

    rows: list[dict[str, Any]] = []
    state = "risk_on"
    prev_state = "risk_on"
    on_streak = 0
    off_streak = 0
    dwell_days = 0

    for t in range(n):
        spy_t = spy[t]
        ratio_t = ratio[t]
        # Trailing SMAs over the prior closes (exclude the current bar).
        spy_sma200 = _trailing_mean(spy, t, sma_trend)
        # Credit ratio SMA60: only over the available (non-None) ratio history.
        ratio_sma60: float | None = None
        if ratio_t is not None and t >= sma_credit:
            window = [r for r in ratio[t - sma_credit:t] if r is not None]
            if len(window) == sma_credit:
                ratio_sma60 = sum(window) / sma_credit

        # Market stress from the trailing 63d high (newest-first window incl. today).
        lo = max(0, t - stress_window)
        spy_window_desc = spy[lo:t + 1][::-1]
        stress = market_stress(spy_window_desc, window=stress_window)

        trend_v, credit_v, dd_v, vote_count = gate_votes(
            spy_t, spy_sma200, ratio_t, ratio_sma60, stress, gate_dd=gate_dd,
        )
        raw_off = vote_count >= 2

        # Dwell-time hysteresis on the latched state (main.py:698-707).
        if raw_off:
            on_streak += 1
            off_streak = 0
        else:
            off_streak += 1
            on_streak = 0
        if state == "risk_on" and on_streak >= gate_confirm:
            state = "risk_off"
        elif state == "risk_off" and off_streak >= gate_confirm:
            state = "risk_on"

        flip = state != prev_state
        dwell_days = 1 if (flip or t == 0) else dwell_days + 1

        # Macro quadrant over the trailing 126d windows (newest-first).
        g_lo = max(0, t - g_look)
        spy_q = spy[g_lo:t + 1][::-1]
        be_lo = max(0, t - i_look)
        be_slice = breakeven[be_lo:t + 1]
        # The quadrant needs a contiguous newest-first breakeven window; if any
        # point in the look-back is None (pre-TIP), the inflation leg is unavailable.
        if any(b is None for b in be_slice):
            quad: str | None = None
            growth_score: float | None = None
            inflation_score: float | None = None
        else:
            be_q = [float(b) for b in be_slice][::-1]
            quad, growth_score, inflation_score = macro_quadrant(
                spy_q, be_q, g_look=g_look, i_look=i_look,
            )

        rows.append({
            "regime_date": dates[t],
            "state": state,
            "trend_vote": bool(trend_v),
            "credit_vote": bool(credit_v),
            "drawdown_vote": bool(dd_v),
            "vote_count": vote_count,
            "flip": flip,
            "dwell_days": dwell_days,
            "growth_score": growth_score,
            "inflation_score": inflation_score,
            "quadrant": quad,
            "spy_close": spy_t,
            "hyg_ief_ratio": ratio_t,
            "tip_ief_ratio": breakeven[t],
            "spy_dd": stress * STRESS_FULL_DD,
        })
        prev_state = state

    return rows


# ──────────────────────────────────────────────────────────────────────────────
# I/O
# ──────────────────────────────────────────────────────────────────────────────
def ensure_schema(conn) -> None:
    sql_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "schemas", "regime_gate.sql",
    )
    with open(sql_path, encoding="utf-8") as fh:
        conn.execute(fh.read())
    conn.commit()


def _fetch_prices(
    calc_date: _dt.date | None,
) -> tuple[
    list[tuple[_dt.date, float | None]],
    list[tuple[_dt.date, float | None]],
    list[tuple[_dt.date, float | None]],
    list[tuple[_dt.date, float | None]],
]:
    """Full history of SPY, HYG, IEF, TIP on Tiingo (adjusted closes).

    Always the entire series: adjClose is re-based retroactively on every
    distribution, so any incremental fetch would mix bases. Fail loud when SPY,
    HYG, or IEF is empty (no detector without them); TIP may be empty/short — the
    inflation leg (and thus the quadrant) is simply None for those days.
    """
    from src.workers._tiingo import TiingoClient

    with TiingoClient() as client:
        spy = client.fetch_daily_prices(SPY_TICKER, HISTORY_START, calc_date)
        hyg = client.fetch_daily_prices(HYG_TICKER, HISTORY_START, calc_date)
        ief = client.fetch_daily_prices(IEF_TICKER, HISTORY_START, calc_date)
        tip = client.fetch_daily_prices(TIP_TICKER, HISTORY_START, calc_date)
    if not spy or not hyg or not ief:
        raise RuntimeError(
            f"Tiingo returned empty history "
            f"(SPY={len(spy)}, HYG={len(hyg)}, IEF={len(ief)})"
        )
    return spy, hyg, ief, tip


def _align(
    spy: list[tuple[_dt.date, float | None]],
    hyg: list[tuple[_dt.date, float | None]],
    ief: list[tuple[_dt.date, float | None]],
    tip: list[tuple[_dt.date, float | None]],
) -> tuple[list[_dt.date], list[float], list[float | None], list[float | None]]:
    """Align all four series onto SPY's date grid (SPY is the spine — the trend
    and drawdown legs need it daily). For each SPY date compute
    ``ratio = HYG/IEF`` (carrying the last HYG and IEF close forward on non-print
    days; None before the first HYG/IEF obs) and ``breakeven = TIP/IEF``
    (carry-forward; None before the first TIP obs). Raises ``RuntimeError`` if SPY
    is empty.
    """
    if not spy:
        raise RuntimeError("cannot align: SPY history is empty")

    def _lookup(series: list[tuple[_dt.date, float | None]]) -> dict[_dt.date, float]:
        return {d: float(v) for d, v in series if v is not None and v > 0}

    hyg_by = _lookup(hyg)
    ief_by = _lookup(ief)
    tip_by = _lookup(tip)

    dates: list[_dt.date] = []
    spy_closes: list[float] = []
    ratio: list[float | None] = []
    breakeven: list[float | None] = []

    last_hyg: float | None = None
    last_ief: float | None = None
    last_tip: float | None = None

    for d, px in sorted(spy, key=lambda t: t[0]):
        if px is None or px <= 0:
            continue
        last_hyg = hyg_by.get(d, last_hyg)
        last_ief = ief_by.get(d, last_ief)
        last_tip = tip_by.get(d, last_tip)

        dates.append(d)
        spy_closes.append(float(px))
        if last_hyg is not None and last_ief is not None and last_ief > 0:
            ratio.append(last_hyg / last_ief)
        else:
            ratio.append(None)
        if last_tip is not None and last_ief is not None and last_ief > 0:
            breakeven.append(last_tip / last_ief)
        else:
            breakeven.append(None)

    return dates, spy_closes, ratio, breakeven


_UPSERT_COLS = (
    "regime_date", "state", "trend_vote", "credit_vote", "drawdown_vote",
    "vote_count", "flip", "dwell_days", "growth_score", "inflation_score",
    "quadrant", "spy_close", "hyg_ief_ratio", "tip_ief_ratio", "spy_dd",
)


def _upsert(conn, rows: list[dict[str, Any]]) -> int:
    update = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in _UPSERT_COLS if c != "regime_date"
    )
    sql = (
        f"INSERT INTO regime_gate_daily ({', '.join(_UPSERT_COLS)}, computed_at) "
        f"VALUES ({', '.join(['%s'] * len(_UPSERT_COLS))}, now()) "
        f"ON CONFLICT (regime_date) DO UPDATE SET {update}, computed_at = now()"
    )
    upserted = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), INSERT_CHUNK):
            chunk = rows[start:start + INSERT_CHUNK]
            cur.executemany(sql, [tuple(r[c] for c in _UPSERT_COLS) for r in chunk])
            upserted += len(chunk)
    return upserted


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,  # accepted by contract; no effect (single series)
) -> dict:
    """Recompute the full LIVE debounced gate + quadrant and upsert to the cloud."""
    cdate = _dt.date.fromisoformat(calc_date) if calc_date else None
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_REGIME_GATE) as got:
            if not got:
                return {"days": 0, "upserted": 0, "skipped": "lock_busy"}

            ensure_schema(conn)
            spy, hyg, ief, tip = _fetch_prices(cdate)
            dates, spy_closes, ratio, breakeven = _align(spy, hyg, ief, tip)
            rows = build_rows(dates, spy_closes, ratio, breakeven)
            upserted = _upsert(conn, rows)
            conn.commit()

    if not rows:
        return {
            "days": 0, "upserted": upserted, "state": None, "vote_count": None,
            "flips": 0, "last_flip": None, "dwell_days": None, "quadrant": None,
            "calc_date": None,
        }
    flips = [r for r in rows if r["flip"]]
    last = rows[-1]
    return {
        "days": len(rows),
        "upserted": upserted,
        "state": last["state"],
        "vote_count": last["vote_count"],
        "flips": len(flips),
        "last_flip": flips[-1]["regime_date"].isoformat() if flips else None,
        "dwell_days": last["dwell_days"],
        "quadrant": last["quadrant"],
        "calc_date": last["regime_date"].isoformat(),
    }
