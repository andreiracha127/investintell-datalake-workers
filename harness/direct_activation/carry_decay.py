"""carry_decay_v1 — the bounded carry-forward policy for the open_macro_v03 regime
consumable (Tranche W3).

WHY THIS EXISTS
---------------
The ratified carry semantics (``live_validation.consumable_today`` and the Stage B
worker) carry the LAST valid latched decision forward INDEFINITELY: a single
2023-02 contraction seed anchored the consumed allocation from 2023 through 2026.
The audit's amplifier #3. ``carry_decay_v1`` bounds the carry age: a seed book is
consumable for at most ``MAX_CARRY_MONTHS`` (3) monthly decision points; past that the
consumable DEGRADES to the mandate-tilted CENTER book (the cross-quadrant mean of the
four ``compressed_50`` books) with ``carry_expired=True`` and keeps being re-evaluated
monthly — a fresh valid decision resets the age to 0.

Age is CALENDAR-MONTH distance from ``carry_seed_as_of`` to the as-of, NOT a row count,
so a chain gap ages the carry naturally (this MIRRORS the Light-repo backtest exactly,
which is a hard fidelity requirement — the two repos must consume the same policy).

GOVERNANCE (read before wiring this into any publish path)
----------------------------------------------------------
This module is PURE and NON-PINNED and it is DEFAULT-OFF (``CARRY_DECAY_V1_ACTIVE =
False``). It deliberately does NOT edit the ratified, hash-pinned decision-chain
modules (``harness/direct_activation/live_validation.py``, ``harness/phase0q/decision.py``,
``harness/phase0q/sleeve.py``): a byte change there would break the module-pin trust
base (``tests/test_direct_activation_stage_b.py::test_module_pins_match_recomputed_tree_hashes``)
and re-pinning it to bless this change would be a self-ratification of the activation
bundle. It also does NOT publish a degraded position: the ``open_macro_v03_decisions`` /
``open_macro_v03_allocations`` CHECK constraints admit only the four quadrant labels,
the ``fresh``/``carried`` validities and the ``compressed_50`` book, and those DDLs are
frozen by the Stage B ``immutability_constraint``. So until (a) the proposed
``timeline_gate_policy`` (phase0q_005) is ratified, (b) the DB schema is evolved to
persist ``carry_expired`` / center-book allocations, and (c) the decision-chain closure
is re-pinned under a governance-sanctioned deploy, the runtime COMPUTES and REPORTS this
provenance advisory-only. Backtests/harness legs may drive the degradation directly via
``evaluate(..., active=True)``.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Mapping, Sequence

from harness.phase0q import sleeve as _sleeve

# The policy id + bound. carry_decay_v1: a seed book is consumable for at most 3 monthly
# decision points; the 4th consecutive gated month degrades to the CENTER book.
CARRY_POLICY_ID = "carry_decay_v1"
MAX_CARRY_MONTHS = 3

# Default OFF. Production activation depends on deploy + ratification of the proposed
# phase0q_005 timeline_gate_policy AND a DB-schema evolution able to persist the
# carry_expired / center-book state (see the module docstring governance note). The
# harness/backtest legs pass ``active=True`` explicitly to measure the degraded policy.
CARRY_DECAY_V1_ACTIVE = False


def carry_age_months(seed_as_of: _dt.date, as_of: _dt.date) -> int:
    """CALENDAR-MONTH distance from the carry seed to the as-of (not a row count).

    ``0`` when the as-of falls in the seed's calendar month (a fresh decision), then one
    per elapsed calendar month regardless of chain gaps. A seed dated AFTER the as-of is
    an out-of-order chain and fails loud."""
    age = (as_of.year - seed_as_of.year) * 12 + (as_of.month - seed_as_of.month)
    if age < 0:
        raise ValueError(
            f"carry_age_months: seed {seed_as_of} is after as_of {as_of} (out-of-order chain)")
    return age


def _ordered_unique(chain: Sequence[Any]) -> list[Any]:
    """Chain ordered by ``as_of`` with a fail-loud duplicate-month guard (a monthly
    latched chain has at most one row per month; a duplicate is a corrupt chain)."""
    ordered = sorted(chain, key=lambda r: r.as_of)
    seen: set[_dt.date] = set()
    for row in ordered:
        if row.as_of in seen:
            raise ValueError(
                f"carry_decay_v1: duplicate decision month {row.as_of} in the chain")
        seen.add(row.as_of)
    return ordered


def carry_provenance(
    chain: Sequence[Any], as_of: _dt.date, *, max_carry_months: int = MAX_CARRY_MONTHS,
) -> dict[str, Any]:
    """Carry provenance for today's consumable decision (PURE; always safe to call).

    Selects the LAST valid latched decision on/before ``as_of`` (the same seed
    ``live_validation.consumable_today`` consumes) and computes its calendar carry age.
    ``chain`` rows are DecisionRow-shaped (``as_of`` / ``quadrant`` / ``has_valid_quadrant()``).

    Returns ``carry_seed_as_of``, seed ``quadrant``, ``decision_validity``
    (fresh iff age 0), ``carry_age_months`` and ``carry_expired`` (age > cap). Raises
    when the chain carries no valid seed (nothing consumable) — same fail-loud contract
    as the ratified path."""
    ordered = _ordered_unique(chain)
    valid = [r for r in ordered if r.as_of <= as_of and r.has_valid_quadrant()]
    if not valid:
        raise ValueError(
            "carry_decay_v1: no valid decision on/before as_of in the latched chain "
            "(no carry seed)")
    seed = valid[-1]
    age = carry_age_months(seed.as_of, as_of)
    return {
        "carry_policy": CARRY_POLICY_ID,
        "max_carry_months": max_carry_months,
        "carry_seed_as_of": seed.as_of,
        "quadrant": seed.quadrant,
        "decision_validity": "fresh" if age == 0 else "carried",
        "carry_age_months": age,
        "carry_expired": age > max_carry_months,
    }


def center_book_50(
    params: "_sleeve.SleeveParams", available: Sequence[str],
) -> dict[str, float]:
    """The mandate-tilted CENTER book: the cross-quadrant MEAN of the four
    ``compressed_50`` quadrant weight vectors, run through the SAME sleeve constraint
    machinery (risk tilt + risk cap + defensive floor + renormalize) any quadrant book
    passes. This is the degradation target once the carry expires: a neutral, mandate
    -constrained position that is NOT the stale seed quadrant.

    Deterministic. Mirrors the Light-repo ``_center_book`` (cross-quadrant mean of the
    compressed_50 books, mandate-tilted where applicable)."""
    compressed = _sleeve.compressed_quadrant_weights(0.5)
    keys = list(compressed)
    center = {t: sum(compressed[k].get(t, 0.0) for k in keys) / len(keys)
              for t in _sleeve.SLEEVE_TICKERS}
    # Every quadrant key maps to the same centroid vector; target_weights then applies
    # the mandate tilt/constraints for whichever label we look up (choice is immaterial).
    book = {key: dict(center) for key in _sleeve.QUADRANT_TO_KEY.values()}
    any_quadrant = next(iter(_sleeve.QUADRANT_TO_KEY))
    return _sleeve.target_weights(any_quadrant, params, available, book=book)


def evaluate(
    chain: Sequence[Any], as_of: _dt.date, params: "_sleeve.SleeveParams",
    available: Sequence[str], *, max_carry_months: int = MAX_CARRY_MONTHS,
    active: bool = CARRY_DECAY_V1_ACTIVE, compressed: bool = True,
) -> dict[str, Any]:
    """Today's consumable allocation under carry_decay_v1.

    Always returns the full carry provenance. When ``active`` and the carry has EXPIRED
    (age > cap) the consumable book DEGRADES to :func:`center_book_50` with
    ``book_id='center_50'`` and ``quadrant_effective=None`` (the strategy is no longer
    positioned by the stale seed quadrant, though the seed ``quadrant`` is preserved as a
    reference). Otherwise the book is the seed quadrant's ``compressed_50`` target — the
    un-degraded, byte-identical behaviour used while the policy is unratified.
    """
    prov = carry_provenance(chain, as_of, max_carry_months=max_carry_months)
    degraded = bool(active and prov["carry_expired"])
    if degraded:
        weights = center_book_50(params, available)
        book_id = "center_50"
        quadrant_effective: str | None = None
    else:
        weights = _sleeve.target_weights(prov["quadrant"], params, available,
                                         compressed=compressed)
        book_id = "compressed_50" if compressed else "baseline_100"
        quadrant_effective = prov["quadrant"]
    return {
        **prov,
        "active": active,
        "degraded_to_center": degraded,
        "book_id": book_id,
        "seed_quadrant": prov["quadrant"],
        "quadrant_effective": quadrant_effective,
        "weights": weights,
    }
