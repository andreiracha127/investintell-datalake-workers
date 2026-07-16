"""Unit tests for the market-fused v3 candidate: the dual-sensor Kalman filter
(src.quadrant_confidence_v2.kalman_fused_filter_series) and the harness market
observation builder (harness.phase0q.decision_v3).

Fast and synthetic — the certified-pack evidence lives in the harness artifacts.
"""

from __future__ import annotations

import datetime as dt

import pytest

from harness.phase0q import decision_v3 as d3
from src import quadrant_confidence_v2 as c2


def _obs(scores, q=1.0):
    return [(s, q if s is not None else None) for s in scores]


def _steady(level, n=36, wobble=0.05):
    return [level + (wobble if i % 2 else -wobble) for i in range(n)]


# --------------------------------------------------------------------------- #
# Fused filter                                                                #
# --------------------------------------------------------------------------- #

def test_fusion_without_auxiliary_equals_single_sensor_filter():
    primary = _obs(_steady(0.8))
    aux_none = [(None, None)] * len(primary)
    fused = c2.kalman_fused_filter_series(primary, aux_none)
    single = c2.kalman_filter_series(primary)
    assert fused == single


def test_fusion_tightens_the_posterior_and_pulls_toward_the_auxiliary():
    primary = _obs(_steady(0.2))
    aux = _obs(_steady(1.2))          # market reads growth much stronger
    fused = c2.kalman_fused_filter_series(primary, aux)
    single = c2.kalman_filter_series(primary)
    m_f, p_f, _ = fused[-1]
    m_s, p_s, _ = single[-1]
    assert p_f < p_s                   # two sensors -> tighter posterior
    assert m_f > m_s                   # pulled toward the auxiliary level
    assert m_f < 1.2                   # but never past it (a weighted fusion)


def test_fusion_never_updates_on_a_missing_primary_month():
    """Freeze scope §1: market-implied is NEVER a fallback — a missing macro month
    is predict-only even when the market printed."""
    primary = _obs(_steady(0.5)) + [(None, None)]
    aux = _obs(_steady(2.0), q=1.0) + [(2.0, 1.0)]   # market prints on the gap
    fused = c2.kalman_fused_filter_series(primary, aux)
    m_last, p_last, r_last = fused[-1]
    m_prev, p_prev, _ = fused[-2]
    assert m_last == m_prev            # mean held: the market print changed nothing
    assert p_last > p_prev             # predict-only variance growth
    assert r_last is None


def test_fusion_weight_is_noise_ratio_not_a_knob():
    """A noisier auxiliary sensor moves the state less — the weight comes from the
    data-estimated R's, not from any configured blend parameter. (Noise built from
    mixed sinusoids: an exactly-bimodal alternation would hit the documented MAD
    breakdown and read as a QUIET sensor.)"""
    import math
    primary = _obs(_steady(0.0, wobble=0.02))
    quiet_aux = _obs(_steady(1.0, wobble=0.02))
    noisy_aux = _obs([1.0 + 2.0 * math.sin(i * 1.3) + 0.7 * math.sin(i * 0.7)
                      for i in range(36)])
    m_quiet = c2.kalman_fused_filter_series(primary, quiet_aux)[-1][0]
    m_noisy = c2.kalman_fused_filter_series(primary, noisy_aux)[-1][0]
    assert m_quiet > m_noisy


def test_fusion_requires_aligned_series():
    with pytest.raises(ValueError):
        c2.kalman_fused_filter_series(_obs(_steady(0.5)), [(None, None)])


# --------------------------------------------------------------------------- #
# Market observation builder (PIT, frozen conventions)                        #
# --------------------------------------------------------------------------- #

def _synthetic_eod(years=14, start=dt.date(2010, 1, 4)):
    """Deterministic drift + slow cycle so month-end 126bd returns are DISTINCT
    (a constant drift would standardize to None: < 24 distinct values)."""
    import math
    rows = []
    level = 100.0
    d = start
    i = 0
    while d < dt.date(start.year + years, 1, 1):
        if d.weekday() < 5:
            level *= (1.0 + 0.0004 + 0.0015 * math.sin(i / 40.0))
            rows.append({"ticker": "SPY", "date": d.isoformat(),
                         "adjusted_close": level})
            i += 1
        d += dt.timedelta(days=1)
    return rows


def _month_ends(start_year, end_year):
    out = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            nxt = dt.date(year + (month == 12), (month % 12) + 1, 1)
            out.append(nxt - dt.timedelta(days=1))
    return out


def test_market_observation_series_is_pit_and_standardized():
    rows = _synthetic_eod()
    months = _month_ends(2010, 2023)
    obs = d3.market_growth_observation_series(rows, months)
    assert len(obs) == len(months)
    # warmup (126bd + 24 distinct months) -> None early, observed later
    assert obs[0] == (None, None)
    tail = [o for o in obs[-12:]]
    assert all(z is not None and q == 1.0 for z, q in tail)
    assert all(abs(z) <= 4.0 for z, _ in tail)  # frozen ±4 clip


def test_market_observation_series_ignores_future_sessions():
    """PIT: truncating the eod rows after a month-end must not change that
    month-end's observation."""
    rows = _synthetic_eod()
    months = _month_ends(2010, 2022)
    cutoff = months[-14]
    full = d3.market_growth_observation_series(rows, months)
    truncated_rows = [r for r in rows
                      if dt.date.fromisoformat(r["date"]) <= cutoff]
    truncated = d3.market_growth_observation_series(
        truncated_rows, [m for m in months if m <= cutoff])
    idx = months.index(cutoff)
    assert full[idx] == truncated[-1]
