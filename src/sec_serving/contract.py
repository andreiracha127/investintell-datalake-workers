"""Workers-side ``sec_regulatory_serving_v1`` cross-repo contract counterpart.

SCOPE (2026-07-30): the serving surface carries the RR1 fee family and the
custom-tag crosswalk governance family ONLY. The nine N-CEN profile families and
the six other RR1 profile families were removed from the product together with
their builders and schemas: nothing downstream consumed them, they only backed
redundant dossier accordions, and one of them took the bond publication chain
down in production. The raw N-CEN/RR1/N-PORT landing tables and the
``*_effective_*`` views are untouched.

This is the publisher half of the serving handshake the reader declares in the
app repo (``backend/app/contracts/sec_regulatory_serving_v1.py``). Both
repositories build the SAME canonical serving surface -- the product string, the
public serving-fact columns, the ordered family catalogue with each family's
snapshot->serving state mapping and payload schema id, the publication/pointer
semantics, and the internal-key scrub blocklist -- and pin the SAME frozen
``SURFACE_DIGEST``.

Mechanism mirrors ``mixed_quant_contract`` exactly: a surface change on either
side changes the recomputed digest and fails that side's verifier until the
frozen constant is deliberately re-synced on BOTH repos. The workers verifier
additionally cross-checks that every family's declared ``source_view`` /
``source_product`` matches what this repo actually installs.

PINNED SERVING BOUNDARY (documented here so both repos carry the decision):
  * The serving DATA lives in the datalake ``market`` database and is produced
    ONLY by workers via the derived-publication protocol (``sec_derived_publications``
    + current pointer). A serving version is one complete, atomically promoted
    publication.
  * ``sec_regulatory_serving_facts`` projects PUBLIC columns only; the app reads
    it BY EXACT ``publication_id`` (its artifact pins the exact worker publication
    + ``publication_version``) via a read-only datalake session, and NEVER reads
    ``sec_current_*`` on the request path and NEVER writes the serving tables.
  * The app-owned composition layer (``fund_regulatory_serving_*``) that pins one
    immutable worker publication is a composition/backfill (ops) responsibility,
    not the app request path. Consistent with Global Constraints 7, 8 and 13.

COVERAGE_PCT SEMANTICS (contract note; carried in both repos -- Increment 2 Task 1e):
  ``coverage_pct`` is a per-FAMILY quantity and its denominator differs by family;
  it is a coarse completeness signal, NOT a single cross-family ratio:
    * ``rr1_fee`` -- RAW STATE PASS-THROUGH on the serving row (available -> 100,
      degraded -> 50, otherwise NULL).  The app composite recomputes it as
      USABLE / 7: the fraction of the seven closed-world over-assets fee concepts
      that resolved to a usable value for the class.
    * ``rr1_custom_tag_crosswalk`` -- a governance surface, not a per-fund fact:
      every emitted row is an approved, confidence-gated mapping (100).
  A consumer must read ``coverage_pct`` against the family it came from and never
  compare it across families as if it were one normalised ratio.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

CONTRACT_VERSION = "sec_regulatory_serving_v1"
SERVING_PRODUCT = "sec_regulatory_serving_v1"
POINTER_TABLE = "sec_derived_current_pointers"
APP_ARTIFACT_TABLE = "fund_regulatory_serving_artifacts"

# The public serving-fact column surface (ordered). No internal provenance,
# source_run_id, registrant_cik, raw_row_id, source_table, or narrative hash.
SERVING_FACT_COLUMNS: tuple[str, ...] = (
    "publication_id",
    "family",
    "series_id",
    "class_id",
    "fund_id",
    "fact_key",
    "grain_origin",
    "state",
    "reason_code",
    "snapshot_reason_code",
    "coverage_pct",
    "source_date",
    "accession_number",
    "document_id",
    "filing_date",
    "effective_date",
    "payload",
)

# The four-state serving vocabulary (app CapabilityState) every family maps onto.
SERVING_STATES: tuple[str, ...] = ("available", "degraded", "unavailable", "not_applicable")

# Typed reason vocabulary shared with the app CapabilityReasonCode.
#   * ``coverage_below_certified_threshold`` is a QUANTITATIVE degrade (an RR1
#     snapshot ``status='degraded'`` passed through -- fewer usable facts than the
#     certified threshold).
#   * ``disclosure_quality_degraded`` is a QUALITATIVE degrade computed on an
#     N-CEN family whose snapshot state is ``available`` but whose disclosure is
#     internally incomplete (an ETF with a one-/zero-legged authorized participant
#     so the aggregate net is untrustworthy; an expense fund with every expense leg
#     NULL).  These are NOT coverage-threshold shortfalls and must not collapse
#     into ``coverage_below_certified_threshold`` (Increment 2 Task 1c).
SERVING_REASON_CODES: tuple[str, ...] = (
    "asset_family_not_applicable",
    "class_context_ambiguous",
    "coverage_below_certified_threshold",
    "disclosure_quality_degraded",
    "publication_not_ready",
    "source_filing_unavailable",
    "source_stale",
)

# Internal-identifier keys the scrubber strips from every payload (leak contract).
SCRUB_BLOCKLIST: tuple[str, ...] = (
    "raw_row_id",
    "source_run_id",
    "ingestion_run_id",
    "source_table",
    "submission_raw_row_id",
    "registrant_raw_row_id",
    "fund_raw_row_id",
    "etf_detail_raw_row_id",
    "effective_raw_row_id",
    "text_block_md5",
    "registrant_cik",
    "custom_tag",
    "original_tag",
    "original_version",
)

_RR1_STATE_MAP = {
    "available": "available",
    "degraded": "degraded",
    "unavailable": "unavailable",
    "not_applicable": "not_applicable",
}

# Ordered family catalogue. ``source_view`` is the current snapshot the
# materializer reads; ``grain`` documents the serving grain; ``degraded_rule``
# names the forward-note-driven refinement applied on top of the state map.
FAMILIES: tuple[dict[str, Any], ...] = (
    {
        "family": "rr1_fee",
        "source_product": "rr1_fee_profile_v1",
        "source_view": "sec_current_rr1_fee_profiles",
        "grain": "series_class_fact",
        "grain_origin": "class",
        "snapshot_state_column": "status",
        "snapshot_state_vocab": ("available", "degraded", "unavailable", "not_applicable"),
        "state_map": _RR1_STATE_MAP,
        "payload_schema_id": "rr1_fee_v1",
        # forward-note 10 / Constraint 5: fractions stored, unit DECLARED here.
        # forward-notes 12 & 15: crosswalk_evidence (canonical_concept + crosswalk_version
        # + confidence) rides here for a fact resolved from a custom tag; never the tag.
        "payload_keys": ("canonical_concept", "value_numeric", "declared_unit",
                         "crosswalk_evidence"),
        "degraded_rule": "snapshot_status_passthrough",
    },
    {
        "family": "rr1_custom_tag_crosswalk",
        "source_product": "rr1_custom_tag_crosswalk",
        "source_view": "rr1_custom_tag_crosswalk",
        "grain": "crosswalk",
        "grain_origin": "crosswalk",
        "snapshot_state_column": "review_status",
        "snapshot_state_vocab": ("proposed", "approved", "rejected"),
        "state_map": {"approved": "available"},
        "payload_schema_id": "rr1_custom_tag_crosswalk_v1",
        # forward-notes 12 & 15: only approved + confidence>=threshold mappings;
        # public evidence is canonical_concept + crosswalk_version + confidence,
        # NEVER the internal custom tag; born empty -> zero serving rows.
        "payload_keys": ("canonical_concept", "crosswalk_version", "confidence"),
        "degraded_rule": "crosswalk_approved_confidence_gate",
    },
)

# Confidence gate for the crosswalk family (forward-note 12).
CROSSWALK_MIN_CONFIDENCE = 0.80

# Frozen handshake digest -- MUST equal the app repo's
# ``app.contracts.sec_regulatory_serving_v1.SURFACE_DIGEST`` byte-for-byte.
SURFACE_DIGEST = "sha256:1dafe89ba9a1997f9849a24c497998950d98d3d9c1371315c2eac22dd9d88708"


def _family_surface(family: dict[str, Any]) -> dict[str, Any]:
    """The order-stable, shared subset of one family entry hashed into the digest."""
    return {
        "family": family["family"],
        "source_product": family["source_product"],
        "source_view": family["source_view"],
        "grain": family["grain"],
        "grain_origin": family["grain_origin"],
        "snapshot_state_vocab": sorted(family["snapshot_state_vocab"]),
        "state_map": dict(sorted(family["state_map"].items())),
        "payload_schema_id": family["payload_schema_id"],
        "payload_keys": sorted(family["payload_keys"]),
        "degraded_rule": family["degraded_rule"],
    }


def surface() -> dict[str, Any]:
    """The canonical, order-stable serving contract surface hashed into the digest."""
    return {
        "contract_version": CONTRACT_VERSION,
        "serving_product": SERVING_PRODUCT,
        "serving_fact_columns": list(SERVING_FACT_COLUMNS),
        "serving_states": sorted(SERVING_STATES),
        "serving_reason_codes": sorted(SERVING_REASON_CODES),
        "scrub_blocklist": sorted(SCRUB_BLOCKLIST),
        "crosswalk_min_confidence": CROSSWALK_MIN_CONFIDENCE,
        "families": [_family_surface(f) for f in sorted(FAMILIES, key=lambda f: f["family"])],
        "publication": {
            "protocol": "sec_derived_publications",
            "pointer_table": POINTER_TABLE,
            "app_pin_table": APP_ARTIFACT_TABLE,
            "promotion": "current_pointer",
            "atomic": True,
            "version_pin": "publication_version",
        },
    }


def compute_surface_digest() -> str:
    """Deterministic digest of the declared serving contract surface."""
    canonical = json.dumps(surface(), separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def family_names() -> tuple[str, ...]:
    return tuple(f["family"] for f in FAMILIES)


def verify_contract() -> dict[str, Any]:
    """Recompute the digest and cross-check the publisher declarations.

    ``ok`` is True only when the recomputed digest matches the frozen
    ``SURFACE_DIGEST`` AND the declared surface is internally consistent (unique
    families, every serving state target inside the 4-state vocabulary, every
    reason code inside the shared vocabulary). Any drift is listed in
    ``mismatches`` so CI can fail loud, exactly like ``mixed_quant_contract``.
    """
    recomputed = compute_surface_digest()
    mismatches: list[str] = []

    if recomputed != SURFACE_DIGEST:
        mismatches.append(f"surface digest {recomputed} != frozen {SURFACE_DIGEST}")

    names = family_names()
    if len(names) != len(set(names)):
        mismatches.append("family names are not unique")
    if len(names) != 2:
        mismatches.append(f"expected 2 serving families, declared {len(names)}")

    for family in FAMILIES:
        for target in family["state_map"].values():
            if target not in SERVING_STATES:
                mismatches.append(
                    f"{family['family']} maps to unknown serving state {target!r}"
                )
        for key in family["payload_keys"]:
            if key in SCRUB_BLOCKLIST:
                mismatches.append(
                    f"{family['family']} payload key {key!r} is in the scrub blocklist"
                )

    return {
        "contract_version": CONTRACT_VERSION,
        "serving_product": SERVING_PRODUCT,
        "surface_digest": SURFACE_DIGEST,
        "recomputed_surface_digest": recomputed,
        "mismatches": mismatches,
        "ok": not mismatches,
    }
