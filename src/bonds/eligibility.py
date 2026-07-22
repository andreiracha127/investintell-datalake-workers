"""Price-eligibility predicate: an ADDITIVE qualifier over bond_price_observation.

An observation is ELIGIBLE for downstream valuation when ALL hold:

* ``price_type`` is in the DECLARED eligible set (``{'trade','evaluated'}``);
* ``accrued_treatment`` is KNOWN (``'clean'`` or ``'dirty'``, never
  ``'not_reported'``);
* the identity is RESOLVED (``identity_state == 'resolved'``);
* the ``(security, date)`` key is NON-AMBIGUOUS
  (``daily_key_state == 'unique_in_matching_cohort'``); and
* the price is actually PRESENT (``price_state == 'present'``).

Any other observation is NOT eligible — an honest exclusion, never fabricated.
The Python predicate here mirrors the SQL ``bond_price_is_eligible`` function and
``bond_price_eligibility_v1`` view EXACTLY (same declared sets, same first-failing
reason order).  This module and its schema are ADDITIVE: they only READ the
immutable ``bond_price_observation`` inputs and never alter the
``bond_price_observation_v1`` product.
"""

from __future__ import annotations

from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_price_eligibility_v1.sql"

# Declared eligible vocabularies (mirrored by the SQL predicate).
ELIGIBLE_PRICE_TYPES = frozenset({"trade", "evaluated"})
KNOWN_ACCRUED_TREATMENTS = frozenset({"clean", "dirty"})


def price_observation_is_eligible(
    *,
    price_type: str,
    accrued_treatment: str,
    identity_state: str,
    daily_key_state: str,
    price_state: str,
) -> bool:
    """True iff the observation qualifies for downstream valuation."""
    return (
        price_type in ELIGIBLE_PRICE_TYPES
        and accrued_treatment in KNOWN_ACCRUED_TREATMENTS
        and identity_state == "resolved"
        and daily_key_state == "unique_in_matching_cohort"
        and price_state == "present"
    )


def eligibility_reason(
    *,
    price_type: str,
    accrued_treatment: str,
    identity_state: str,
    daily_key_state: str,
    price_state: str,
) -> str | None:
    """The first failing condition (fixed order), or ``None`` when eligible."""
    if price_type not in ELIGIBLE_PRICE_TYPES:
        return "price_type_not_eligible"
    if accrued_treatment not in KNOWN_ACCRUED_TREATMENTS:
        return "accrued_treatment_unknown"
    if identity_state != "resolved":
        return "identity_unresolved"
    if daily_key_state != "unique_in_matching_cohort":
        return "identity_ambiguous"
    if price_state != "present":
        return "price_absent"
    return None


def install_schema(conn: psycopg.Connection) -> None:
    """Apply the price-observation DDL (idempotent, so the observation table exists)
    plus the additive eligibility predicate/view idempotently."""
    with conn.cursor() as cur:
        cur.execute((ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8"))
        cur.execute((ROOT / "schemas" / "bond_price_observations_v1.sql").read_text(encoding="utf-8"))
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
