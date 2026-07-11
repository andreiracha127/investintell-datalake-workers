"""Golden behavioural test of the open_macro_v03 regime TIMELINE (Tranche W4).

Deterministic replay of the CERTIFIED input pack v2 (no DB) through the frozen monthly
decision engine over the production chain window (CHAIN_START 2014-03 -> 2026-06), then
asserts the 2021-01..2026-06 timeline the regime audit ratified: 23 valid / 43
low_confidence months, the exact valid quadrants + confidences (tol 1e-6), and the
18-month valid gap between 2023-03 and 2024-08.

This DOCUMENTS THE CURRENT BEHAVIOUR OF MODEL v1 (macro_quadrant_us_v1): the abstention
density, the multi-year contraction anchor and the specific confidences. It is a
regression tripwire — any change here means the model's decision timeline moved, which
requires a REVIEWED model-version bump (new confidence_method / model_version), never a
silent edit. The audit's recommendation #5: version the expected 23/66 timeline so the
behaviour can never again go invisible.

Cost: the full production-chain replay is ~90s (< the 2-min budget). It runs ONCE via a
module-scoped fixture. The repo defines no slow-test marker; if the chain is later
shortened for speed it MUST still start early enough to reproduce these exact values (a
shorter warmup can change the hysteresis latch and therefore the valid set).
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from harness.phase0q import decision

ROOT = Path(__file__).resolve().parents[1]
PACK_DIR = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"

WINDOW_START = dt.date(2021, 1, 1)
WINDOW_END = dt.date(2026, 6, 30)
CHAIN_START = dt.date(2014, 3, 1)  # the production live_validation CHAIN_START

# The ratified golden: every valid consumable month in 2021-01..2026-06 with its
# quadrant and candidate_confidence (macro_quadrant_us_v1 over the certified pack).
EXPECTED_VALID: dict[dt.date, tuple[str, float]] = {
    dt.date(2021, 2, 28): ("expansion", 0.8468190517243848),
    dt.date(2021, 4, 30): ("expansion", 0.7142431328227061),
    dt.date(2021, 5, 31): ("expansion", 0.9840835909550201),
    dt.date(2021, 6, 30): ("expansion", 0.9910159759682184),
    dt.date(2021, 7, 31): ("expansion", 0.9980179661441742),
    dt.date(2021, 8, 31): ("expansion", 0.9970232721421881),
    dt.date(2021, 9, 30): ("expansion", 0.9902102561014525),
    dt.date(2021, 11, 30): ("recovery", 0.714150865600292),
    dt.date(2021, 12, 31): ("expansion", 0.7005988201474006),
    dt.date(2022, 1, 31): ("expansion", 0.8515052638661303),
    dt.date(2022, 2, 28): ("expansion", 0.7952967356219163),
    dt.date(2022, 4, 30): ("recovery", 0.8093480837086213),
    dt.date(2022, 5, 31): ("recovery", 0.8395630832112084),
    dt.date(2022, 7, 31): ("expansion", 0.7471448637484768),
    dt.date(2022, 8, 31): ("recovery", 0.709249245761684),
    dt.date(2023, 2, 28): ("contraction", 0.7308343658185967),
    dt.date(2024, 9, 30): ("contraction", 0.708997342695669),
    dt.date(2025, 6, 30): ("contraction", 0.919935917893653),
    dt.date(2025, 7, 31): ("contraction", 0.7954672485026368),
    dt.date(2026, 1, 31): ("contraction", 0.871687239341956),
    dt.date(2026, 2, 28): ("contraction", 0.8560171875103648),
    dt.date(2026, 4, 30): ("slowdown", 0.7053416832837262),
    dt.date(2026, 6, 30): ("expansion", 0.8121545618518331),
}


def _load_pack_macro_rows():
    path = PACK_DIR / "data" / "canonical" / "macro_observation_vintage.json"
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def window():
    """The 2021-01..2026-06 slice of the production-chain decision series (replayed once)."""
    series = decision.run_decision_series(_load_pack_macro_rows(), CHAIN_START, WINDOW_END)
    return [r for r in series if WINDOW_START <= r.as_of <= WINDOW_END]


def test_window_has_66_months(window):
    assert len(window) == 66


def test_valid_and_low_confidence_counts(window):
    valid = [r for r in window if r.has_valid_quadrant()]
    low = [r for r in window if r.status == "low_confidence"]
    assert len(valid) == 23
    assert len(low) == 43
    # the window partitions cleanly into valid + low_confidence (no unavailable/invalid).
    assert len(valid) + len(low) == 66


def test_exact_valid_months_quadrants_and_confidences(window):
    valid = {r.as_of: r for r in window if r.has_valid_quadrant()}
    assert set(valid) == set(EXPECTED_VALID), (
        "valid-month SET changed -> model timeline moved (needs a reviewed model-version bump)")
    for as_of, (quadrant, confidence) in EXPECTED_VALID.items():
        row = valid[as_of]
        assert row.quadrant == quadrant, f"{as_of}: quadrant {row.quadrant} != {quadrant}"
        assert row.candidate_confidence == pytest.approx(confidence, abs=1e-6), \
            f"{as_of}: confidence {row.candidate_confidence} != {confidence}"


def test_no_valid_decision_in_the_2023_to_2024_gap(window):
    gap = [r for r in window
           if dt.date(2023, 3, 1) <= r.as_of <= dt.date(2024, 8, 31)
           and r.has_valid_quadrant()]
    assert gap == [], "the 18-month 2023-03..2024-08 abstention gap must have no valid month"


def test_specific_defensive_anchor_months(window):
    """The audit's headline: the 2023-02 contraction seed and the recurring later
    contraction prints that anchored the defensive book for years."""
    by_date = {r.as_of: r for r in window}
    for as_of in (dt.date(2023, 2, 28), dt.date(2024, 9, 30), dt.date(2025, 6, 30),
                  dt.date(2025, 7, 31), dt.date(2026, 1, 31), dt.date(2026, 2, 28)):
        assert by_date[as_of].quadrant == "contraction"
    assert by_date[dt.date(2026, 4, 30)].quadrant == "slowdown"
    assert by_date[dt.date(2026, 6, 30)].quadrant == "expansion"
