"""Tranche W5 regression: the robust-baseline window is a PARAMETER, default frozen.

quadrant_score.standardized_latest already exposes ``window_years: int = 10`` (the
robust-z 10y baseline). This suite pins the freeze contract for the recalibration
instrumentation: calling with the DEFAULT is byte-identical to the frozen 10y baseline
(no behaviour change), while an explicit non-default window genuinely re-selects the
eligible history (the knob the recalibration experiment sweeps). No frozen-model default
is moved here — this is instrumentation only.

Network-free, DB-free (synthetic series + a real committed-pack series).
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from src.macro_sources import SEED_SOURCES
from src.quadrant_score import standardized_latest

ROOT = Path(__file__).resolve().parents[1]
PACK_DIR = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"

_INDPRO = next(s for s in SEED_SOURCES if s.series_id == "INDPRO")


def _synthetic_series() -> dict[dt.date, float]:
    """12+ years of monthly levels with a late regime shift, so the 5y and 10y robust
    baselines of the latest value genuinely differ."""
    series: dict[dt.date, float] = {}
    d = dt.date(2013, 1, 1)
    for k in range(150):
        series[dt.date(d.year, d.month, 1)] = 100.0 + 0.2 * k + (5.0 if k > 110 else 0.0)
        d = dt.date(d.year + (d.month // 12), (d.month % 12) + 1, 1)
    return series


def test_default_window_is_byte_identical_to_explicit_ten():
    series = _synthetic_series()
    as_of = dt.date(2025, 6, 1)
    assert standardized_latest(_INDPRO, series, as_of) == \
        standardized_latest(_INDPRO, series, as_of, window_years=10)


def test_non_default_window_actually_re_selects_history():
    series = _synthetic_series()
    as_of = dt.date(2025, 6, 1)
    ten = standardized_latest(_INDPRO, series, as_of, window_years=10)
    assert standardized_latest(_INDPRO, series, as_of, window_years=5) != ten
    assert standardized_latest(_INDPRO, series, as_of, window_years=7) != ten


def test_default_window_byte_identical_over_real_pack_series():
    rows = json.loads(
        (PACK_DIR / "data" / "canonical" / "macro_observation_vintage.json")
        .read_text(encoding="utf-8"))
    # reconstruct INDPRO's observation-period -> value map (latest vintage per period).
    series: dict[dt.date, float] = {}
    for r in sorted(rows, key=lambda r: (r["observation_period"], r["available_at"])):
        if r["series_id"] != "INDPRO":
            continue
        series[dt.date.fromisoformat(r["observation_period"][:10])] = float(r["value"])
    for as_of in (dt.date(2018, 6, 1), dt.date(2022, 1, 1), dt.date(2026, 6, 1)):
        assert standardized_latest(_INDPRO, series, as_of) == \
            standardized_latest(_INDPRO, series, as_of, window_years=10)
