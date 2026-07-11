"""TDD suite for carry_decay_v1 (Tranche W3): bounded carry-forward + CENTER degradation.

Covers: calendar-month age (0/1/3/4, chain-gap aging), seed selection, fresh reset,
CENTER-book degradation past the cap (active) vs byte-identical un-degraded behaviour
(default OFF), center-book constraints, provenance presence, and fail-loud guards.

Pure + DB-free (carry_decay imports only the pure sleeve module).
"""

from __future__ import annotations

import datetime as dt

import pytest

from harness.direct_activation import carry_decay as cd
from harness.phase0q import sleeve

PARAMS = sleeve.SleeveParams(candidate_id="open_macro_v03_compressed_50")
AVAIL = list(sleeve.SLEEVE_TICKERS)


class _Dec:
    def __init__(self, as_of: dt.date, quadrant: str | None, valid: bool):
        self.as_of = as_of
        self.quadrant = quadrant
        self._valid = valid

    def has_valid_quadrant(self) -> bool:
        return self._valid


def _me(y: int, m: int) -> dt.date:
    return dt.date(y, 12, 31) if m == 12 else dt.date(y, m + 1, 1) - dt.timedelta(days=1)


# --------------------------------------------------------------------------- #
# calendar-month age                                                          #
# --------------------------------------------------------------------------- #

def test_carry_age_is_calendar_months_not_row_count():
    assert cd.carry_age_months(dt.date(2026, 6, 30), dt.date(2026, 6, 30)) == 0
    assert cd.carry_age_months(dt.date(2026, 6, 30), dt.date(2026, 7, 3)) == 1
    # a chain gap ages the carry naturally (Feb 2023 seed, Aug 2024 as-of = 18 months).
    assert cd.carry_age_months(dt.date(2023, 2, 28), dt.date(2024, 8, 31)) == 18


def test_carry_age_out_of_order_fails_loud():
    with pytest.raises(ValueError, match="out-of-order"):
        cd.carry_age_months(dt.date(2026, 7, 31), dt.date(2026, 6, 30))


# --------------------------------------------------------------------------- #
# provenance + seed selection                                                 #
# --------------------------------------------------------------------------- #

def _gated_after_contraction():
    return [
        _Dec(_me(2026, 2), "contraction", True),
        _Dec(_me(2026, 3), None, False),
        _Dec(_me(2026, 4), None, False),
        _Dec(_me(2026, 5), None, False),
        _Dec(_me(2026, 6), None, False),
    ]


@pytest.mark.parametrize("as_of,age,expired,validity", [
    (_me(2026, 2), 0, False, "fresh"),
    (_me(2026, 3), 1, False, "carried"),
    (_me(2026, 5), 3, False, "carried"),   # age 3 == cap: still within
    (_me(2026, 6), 4, True, "carried"),    # age 4 > cap: expired
])
def test_provenance_age_and_expiry_boundary(as_of, age, expired, validity):
    prov = cd.carry_provenance(_gated_after_contraction(), as_of)
    assert prov["carry_seed_as_of"] == _me(2026, 2)
    assert prov["quadrant"] == "contraction"
    assert prov["carry_age_months"] == age
    assert prov["carry_expired"] is expired
    assert prov["decision_validity"] == validity
    assert prov["carry_policy"] == "carry_decay_v1"
    assert prov["max_carry_months"] == 3


def test_seed_is_last_valid_on_or_before_as_of():
    chain = [
        _Dec(_me(2025, 1), "expansion", True),
        _Dec(_me(2025, 6), "contraction", True),   # later valid wins
        _Dec(_me(2025, 7), None, False),
    ]
    prov = cd.carry_provenance(chain, _me(2025, 8))
    assert prov["carry_seed_as_of"] == _me(2025, 6)
    assert prov["quadrant"] == "contraction"
    assert prov["carry_age_months"] == 2


# --------------------------------------------------------------------------- #
# degradation to CENTER (active) vs byte-identical default (OFF)              #
# --------------------------------------------------------------------------- #

def test_default_flag_is_off():
    assert cd.CARRY_DECAY_V1_ACTIVE is False


@pytest.mark.parametrize("as_of,age", [
    (_me(2026, 2), 0), (_me(2026, 3), 1), (_me(2026, 5), 3),
])
def test_active_within_cap_carries_seed_book(as_of, age):
    ev = cd.evaluate(_gated_after_contraction(), as_of, PARAMS, AVAIL, active=True)
    assert ev["carry_age_months"] == age
    assert ev["carry_expired"] is False
    assert ev["degraded_to_center"] is False
    assert ev["book_id"] == "compressed_50"
    assert ev["quadrant_effective"] == "contraction"
    assert ev["weights"] == sleeve.target_weights("contraction", PARAMS, AVAIL, compressed=True)


def test_active_past_cap_degrades_to_center():
    ev = cd.evaluate(_gated_after_contraction(), _me(2026, 6), PARAMS, AVAIL, active=True)
    assert ev["carry_age_months"] == 4
    assert ev["carry_expired"] is True
    assert ev["degraded_to_center"] is True
    assert ev["book_id"] == "center_50"
    assert ev["quadrant_effective"] is None          # no longer positioned by the stale seed
    assert ev["seed_quadrant"] == "contraction"      # seed preserved as reference
    assert ev["weights"] == cd.center_book_50(PARAMS, AVAIL)


def test_inactive_never_degrades_byte_identical_to_seed_book():
    # DEFAULT (active=False): even a 4-month expired carry keeps the seed compressed_50
    # book byte-for-byte — the un-ratified policy changes nothing.
    ev = cd.evaluate(_gated_after_contraction(), _me(2026, 6), PARAMS, AVAIL)  # active default
    assert ev["carry_expired"] is True               # provenance still reports the expiry
    assert ev["degraded_to_center"] is False
    assert ev["book_id"] == "compressed_50"
    assert ev["weights"] == sleeve.target_weights("contraction", PARAMS, AVAIL, compressed=True)


def test_fresh_valid_decision_resets_age_and_book():
    chain = _gated_after_contraction() + [_Dec(_me(2026, 7), "expansion", True)]
    ev = cd.evaluate(chain, _me(2026, 7), PARAMS, AVAIL, active=True)
    assert ev["carry_age_months"] == 0
    assert ev["decision_validity"] == "fresh"
    assert ev["carry_expired"] is False
    assert ev["degraded_to_center"] is False
    assert ev["quadrant_effective"] == "expansion"
    assert ev["weights"] == sleeve.target_weights("expansion", PARAMS, AVAIL, compressed=True)


# --------------------------------------------------------------------------- #
# center book properties                                                       #
# --------------------------------------------------------------------------- #

def test_center_book_is_constrained_and_distinct_from_single_quadrants():
    cb = cd.center_book_50(PARAMS, AVAIL)
    assert sum(cb.values()) == pytest.approx(1.0)
    risk = cb.get("SPY", 0.0) + cb.get("DBC", 0.0)
    defensive = cb.get("TLT", 0.0) + cb.get("SHY", 0.0) + cb.get("TIP", 0.0)
    assert risk <= sleeve.RISK_CAP_BASELINE + 1e-9
    assert defensive >= sleeve.DEFENSIVE_FLOOR_BASELINE - 1e-9
    # the centroid is not identical to any single compressed_50 quadrant book.
    for q in ("recovery", "expansion", "slowdown", "contraction"):
        assert cb != sleeve.target_weights(q, PARAMS, AVAIL, compressed=True)


# --------------------------------------------------------------------------- #
# fail-loud guards                                                             #
# --------------------------------------------------------------------------- #

def test_no_valid_seed_fails_loud():
    chain = [_Dec(_me(2026, 3), None, False), _Dec(_me(2026, 4), None, False)]
    with pytest.raises(ValueError, match="no carry seed"):
        cd.carry_provenance(chain, _me(2026, 5))


def test_duplicate_month_fails_loud():
    chain = [_Dec(_me(2026, 2), "contraction", True),
             _Dec(_me(2026, 2), "expansion", True)]
    with pytest.raises(ValueError, match="duplicate decision month"):
        cd.carry_provenance(chain, _me(2026, 3))
