from __future__ import annotations

import datetime as dt

from src.macro_sources import SEED_SOURCES
from src.quadrant_score import axis_score, standardized_latest

_INDPRO = next(s for s in SEED_SOURCES if s.series_id == "INDPRO")  # log_3m3m_ann_v1


def _monthly(values: list[float], start=dt.date(2020, 1, 1)) -> dict[dt.date, float]:
    out: dict[dt.date, float] = {}
    y, m = start.year, start.month
    for v in values:
        out[dt.date(y, m, 1)] = v
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def test_standardized_latest_runs_transform_then_robust_z() -> None:
    # A long, rising-then-spiking series: the latest 3m3m impulse is far above the
    # historical median -> a large positive z (clipped at +4).
    base = [100.0 + 0.1 * i for i in range(60)]          # slow drift
    base += [base[-1] * 1.5]                             # a spike in the last month
    series = _monthly(base)
    as_of = max(series)
    z = standardized_latest(_INDPRO, series, as_of)
    assert z is not None and z > 0.0


def test_standardized_latest_none_when_no_period_at_or_before_as_of() -> None:
    series = _monthly([100.0 + i for i in range(30)])
    # as_of before any computable transform period -> None.
    assert standardized_latest(_INDPRO, series, dt.date(2019, 1, 1)) is None


def test_standardized_latest_respects_as_of_cutoff() -> None:
    series = _monthly([100.0 + 0.1 * i for i in range(60)])
    full = max(series)
    cut = dt.date(2021, 6, 1)  # well before the end
    z_full = standardized_latest(_INDPRO, series, full)
    z_cut = standardized_latest(_INDPRO, series, cut)
    assert z_full is not None and z_cut is not None
    # different as_of -> different latest standardized impulse (no look-ahead).
    assert z_full != z_cut


def test_axis_score_weighted_sum_over_available() -> None:
    weights = {"A": 0.5, "B": 0.5}
    score, contrib = axis_score(weights, {"A": 1.0, "B": -1.0})
    assert abs(score - 0.0) < 1e-9
    assert abs(contrib["A"] - 0.5) < 1e-9 and abs(contrib["B"] - (-0.5)) < 1e-9


def test_axis_score_renormalizes_when_one_series_missing() -> None:
    weights = {"A": 0.5, "B": 0.5}
    # B missing -> A carries full weight (renormalized to 1.0)
    score, contrib = axis_score(weights, {"A": 2.0, "B": None})
    assert abs(score - 2.0) < 1e-9
    assert "B" not in contrib


def test_axis_score_none_when_all_missing() -> None:
    score, contrib = axis_score({"A": 1.0}, {"A": None})
    assert score is None and contrib == {}
