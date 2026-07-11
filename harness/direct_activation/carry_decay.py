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

GOVERNANCE (status: RATIFIED + ACTIVE)
--------------------------------------
The ``timeline_gate_policy`` (phase0q_005) was RATIFIED by the quant_owner (Andrei
Rachadel) on 2026-07-11 and carry_decay_v1 ships ACTIVE (``CARRY_DECAY_V1_ACTIVE =
True``): the Stage B worker publishes the degraded CENTER book (``center_50``) with
``decision_validity = 'carried_expired'`` and honest provenance columns when the carry
age exceeds the cap. The DB surface is the ADDITIVE migration
``schemas/open_macro_v03_carry_decay_v1_migration.sql`` (applied by the orchestrator in
a controlled step; the worker's ``verify_schema``/publish path fails loud against an
unmigrated catalog). The remaining dependencies are OPS steps — migration application +
worker redeploy — not approvals.

This module remains PURE and NON-PINNED: it deliberately does NOT edit the ratified,
hash-pinned decision-chain modules (``harness/direct_activation/live_validation.py``,
``harness/phase0q/decision.py``, ``harness/phase0q/sleeve.py``) — a byte change there
would break the module-pin trust base
(``tests/test_direct_activation_stage_b.py::test_module_pins_match_recomputed_tree_hashes``)
and re-pinning to bless it would be self-ratification of the activation bundle. The
non-pinned worker composes this module ON TOP of the pinned ``consumable_today`` seed
selection, so the frozen decision path is untouched.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Mapping, Sequence

from harness.phase0q import sleeve as _sleeve

# The policy id + bound. carry_decay_v1: a seed book is consumable for at most 3 monthly
# decision points; the 4th consecutive gated month degrades to the CENTER book.
CARRY_POLICY_ID = "carry_decay_v1"
MAX_CARRY_MONTHS = 3

# Default ON since the phase0q_005 timeline_gate_policy ratification (quant_owner,
# 2026-07-11): the publish path degrades an expired carry to the CENTER book. The DB
# surface is the additive carry_decay_v1 migration (applied by the orchestrator);
# against an unmigrated catalog the worker fails loud instead of publishing. Tests
# exercise the pre-ratification behaviour with an explicit ``active=False``.
CARRY_DECAY_V1_ACTIVE = True


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


def _validated_monthly_chain(chain: Sequence[Any]) -> list[Any]:
    """Fail-loud validation of the AS-SUPPLIED chain order and cadence.

    The latched chain is produced in strictly ascending CALENDAR-MONTH order (one
    decision row per month). This validates the ORIGINAL sequence — it never sorts,
    because sorting would silently repair an out-of-order (corrupt) chain — and keys
    duplicates on (year, month), not the full date, so two rows in the same month with
    different days are rejected too. Mirror of the Light-repo carry_decay_v1 semantics:
    duplicate/out-of-order months fail loud."""
    rows = list(chain)
    prev_key: tuple[int, int] | None = None
    prev_as_of: _dt.date | None = None
    for row in rows:
        key = (row.as_of.year, row.as_of.month)
        if prev_key is not None:
            if key == prev_key:
                raise ValueError(
                    f"carry_decay_v1: duplicate decision month {row.as_of} in the "
                    f"chain (already saw {prev_as_of} in {prev_key[0]}-{prev_key[1]:02d})")
            if key < prev_key:
                raise ValueError(
                    f"carry_decay_v1: out-of-order decision month {row.as_of} after "
                    f"{prev_as_of} (the latched chain must be strictly ascending by "
                    "calendar month; refusing to silently repair a corrupt chain)")
        prev_key = key
        prev_as_of = row.as_of
    return rows


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
    as the ratified path. The chain must arrive strictly ascending by calendar month
    (see :func:`_validated_monthly_chain`); out-of-order/duplicate months raise."""
    ordered = _validated_monthly_chain(chain)
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
