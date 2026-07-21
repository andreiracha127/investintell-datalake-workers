"""Workers-side ``mixed_quant_v1`` cross-repo contract counterpart (Task 6, Step 4).

This is the publisher half of the handshake the reader declares in the app repo
(``backend/app/contracts/mixed_quant_v1.py``). Both repositories build the SAME
canonical contract surface — the product string, the instrument families, the
observed vs return-estimated factor namespaces, the five named bond factors, the
observed income event types and the single (absent) observed-income-return key —
and pin the SAME frozen ``SURFACE_DIGEST``.

Mechanism mirrors the quant-engine bundle contract: a surface change on either
side changes the recomputed digest and fails that side's verifier until the
frozen constant is deliberately re-synced on BOTH repos. The workers surface is
built from THIS repo's own declarations (``publication.PRODUCT``,
``contracts.INSTRUMENT_TYPES`` / ``NAMED_BOND_FACTORS``), so a publisher drift is
caught here; ``verify_contract`` additionally cross-checks the DDL income-event
vocabulary via ``validate_income_event``.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any

from src.quant_data import contracts, publication as pub

CONTRACT_VERSION = "mixed_quant_v1"

# Namespaces the exposure stage emits; the reader distinguishes observed
# look-through from return-estimated class factors by these prefixes.
OBSERVED_LOOKTHROUGH_NAMESPACES: tuple[str, ...] = (
    "asset_class",
    "country",
    "currency",
    "issuer",
    "sector",
)
ESTIMATED_FACTOR_NAMESPACE = "class_factor"
BOND_FACTOR_NAMESPACE = "bond_factor"

# Observed income cash-event vocabulary (the quant_income_v1 DDL CHECK).
INCOME_EVENT_TYPES: tuple[str, ...] = (
    "coupon",
    "distribution",
    "dividend",
    "return_of_capital",
)
# Only these three are income evidence; ``return_of_capital`` is a capital return.
INCOME_SOURCE_EVENT_TYPES: tuple[str, ...] = ("coupon", "distribution", "dividend")

INCOME_RETURN_COVERAGE_KEY = "income_return_ann"
INCOME_RETURN_OBSERVED = False

# Frozen handshake digest — MUST equal the app repo's
# ``app.contracts.mixed_quant_v1.SURFACE_DIGEST`` byte-for-byte.
SURFACE_DIGEST = "sha256:d23f01f4ce421787235e2617461a0531b87464a7f3f890e01ccdd491e5bfb4e3"


def _surface() -> dict[str, Any]:
    """The canonical, order-stable contract surface hashed into the digest."""
    return {
        "product": pub.PRODUCT,
        "contract_version": CONTRACT_VERSION,
        "instrument_types": sorted(contracts.INSTRUMENT_TYPES),
        "observed_lookthrough_namespaces": sorted(OBSERVED_LOOKTHROUGH_NAMESPACES),
        "estimated_factor_namespace": ESTIMATED_FACTOR_NAMESPACE,
        "bond_factor_namespace": BOND_FACTOR_NAMESPACE,
        "named_bond_factors": sorted(contracts.NAMED_BOND_FACTORS),
        "income_event_types": sorted(INCOME_EVENT_TYPES),
        "income_source_event_types": sorted(INCOME_SOURCE_EVENT_TYPES),
        "income_return_coverage_key": INCOME_RETURN_COVERAGE_KEY,
        "income_return_observed": INCOME_RETURN_OBSERVED,
    }


def compute_surface_digest() -> str:
    """Deterministic digest of the declared contract surface."""
    canonical = json.dumps(_surface(), separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def verify_contract() -> dict[str, Any]:
    """Recompute the digest and cross-check the publisher declarations.

    ``ok`` is True only when the recomputed digest matches the frozen
    ``SURFACE_DIGEST`` AND the workers vocabulary (product, instrument types,
    named bond factors, income event types) matches the declared surface. Any
    drift is listed in ``mismatches`` so CI can fail loud.
    """
    recomputed = compute_surface_digest()
    mismatches: list[str] = []

    if recomputed != SURFACE_DIGEST:
        mismatches.append(f"surface digest {recomputed} != frozen {SURFACE_DIGEST}")
    if pub.PRODUCT != "mixed_quant_v1":
        mismatches.append(f"publication PRODUCT {pub.PRODUCT!r} != 'mixed_quant_v1'")
    if tuple(sorted(contracts.INSTRUMENT_TYPES)) != ("bond", "equity", "fund"):
        mismatches.append(
            f"INSTRUMENT_TYPES {sorted(contracts.INSTRUMENT_TYPES)} != "
            "['bond', 'equity', 'fund']"
        )
    if tuple(sorted(contracts.NAMED_BOND_FACTORS)) != tuple(
        sorted(("credit", "curve", "duration", "inflation", "liquidity"))
    ):
        mismatches.append(
            f"NAMED_BOND_FACTORS {sorted(contracts.NAMED_BOND_FACTORS)} drift"
        )

    # The DDL income-event vocabulary must accept exactly the declared types.
    for event_type in INCOME_EVENT_TYPES:
        try:
            contracts.validate_income_event(
                {
                    "event_date": date(2024, 1, 1),
                    "cash_amount": "1",
                    "currency": "USD",
                    "event_type": event_type,
                    "source_lineage": {"source": "contract_probe"},
                }
            )
        except contracts.ContractError as exc:  # pragma: no cover - drift only
            mismatches.append(f"income event_type {event_type!r} rejected: {exc}")

    return {
        "contract_version": CONTRACT_VERSION,
        "product": pub.PRODUCT,
        "surface_digest": SURFACE_DIGEST,
        "recomputed_surface_digest": recomputed,
        "mismatches": mismatches,
        "ok": not mismatches,
    }
