"""Workers-side ``sec_regulatory_serving_v1`` cross-repo contract counterpart.

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
SERVING_REASON_CODES: tuple[str, ...] = (
    "asset_family_not_applicable",
    "class_context_ambiguous",
    "coverage_below_certified_threshold",
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

_NCEN_STATE_MAP = {
    "available": "available",
    "unavailable": "unavailable",
    "not_applicable": "not_applicable",
}
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
        "family": "ncen_structure",
        "source_product": "ncen_structure_profile_v1",
        "source_view": "sec_current_ncen_structure_profiles",
        "grain": "fund",
        "grain_origin": "fund",
        "snapshot_state_column": "structure_state",
        "snapshot_state_vocab": ("available", "unavailable", "not_applicable"),
        "state_map": _NCEN_STATE_MAP,
        "payload_schema_id": "ncen_structure_v1",
        "payload_keys": ("structure_flags", "regulatory_reliance", "report_period_lt_12month",
                         "reliance_state", "reliance_reason_code"),
        "degraded_rule": "none",
    },
    {
        "family": "ncen_provider_network",
        "source_product": "ncen_provider_network_v1",
        "source_view": "sec_current_ncen_provider_network_profiles",
        "grain": "fund",
        "grain_origin": "fund",
        "snapshot_state_column": "provider_network_state",
        "snapshot_state_vocab": ("available", "unavailable", "not_applicable"),
        "state_map": _NCEN_STATE_MAP,
        "payload_schema_id": "ncen_provider_network_v1",
        "payload_keys": ("provider_network",),
        "degraded_rule": "none",
    },
    {
        "family": "ncen_operational_event",
        "source_product": "ncen_operational_event_v1",
        "source_view": "sec_current_ncen_operational_event_profiles",
        "grain": "registrant_fanout_to_fund",
        "grain_origin": "registrant",
        "snapshot_state_column": "operational_event_state",
        "snapshot_state_vocab": ("available", "unavailable", "not_applicable"),
        "state_map": _NCEN_STATE_MAP,
        "payload_schema_id": "ncen_operational_event_v1",
        "payload_keys": ("operational_events",),
        # forward-note 2: registrant grain fanned out to each fund of the same
        # accession via the fund roster; grain_origin labels the origin.
        "degraded_rule": "registrant_to_fund_fanout",
    },
    {
        "family": "ncen_liquidity_backstop",
        "source_product": "ncen_liquidity_backstop_v1",
        "source_view": "sec_current_ncen_liquidity_backstop_profiles",
        "grain": "fund",
        "grain_origin": "fund",
        "snapshot_state_column": "liquidity_backstop_state",
        "snapshot_state_vocab": ("available", "unavailable", "not_applicable"),
        "state_map": _NCEN_STATE_MAP,
        "payload_schema_id": "ncen_liquidity_backstop_v1",
        "payload_keys": ("liquidity_backstop",),
        # forward-note 5: family stays available with per-sub-block states inside
        # the payload; the serving does not collapse sub-panel states.
        "degraded_rule": "sub_panel_states_in_payload",
    },
    {
        "family": "ncen_securities_lending",
        "source_product": "ncen_securities_lending_v1",
        "source_view": "sec_current_ncen_securities_lending_profiles",
        "grain": "fund",
        "grain_origin": "fund",
        "snapshot_state_column": "securities_lending_state",
        "snapshot_state_vocab": ("available", "unavailable", "not_applicable"),
        "state_map": _NCEN_STATE_MAP,
        "payload_schema_id": "ncen_securities_lending_v1",
        "payload_keys": ("securities_lending", "quality_flags"),
        # forward-note 6: IS_COLLATERAL_LIQUIDATED contract defect is surfaced as a
        # reduced-quality flag, never silently repaired (program pendency).
        "degraded_rule": "collateral_liquidated_quality_flag",
    },
    {
        "family": "ncen_etf_primary_market",
        "source_product": "ncen_etf_primary_market_v1",
        "source_view": "sec_current_ncen_etf_primary_market_profiles",
        "grain": "fund",
        "grain_origin": "fund",
        "snapshot_state_column": "etf_primary_market_state",
        "snapshot_state_vocab": ("available", "unavailable", "not_applicable"),
        "state_map": _NCEN_STATE_MAP,
        "payload_schema_id": "ncen_etf_primary_market_v1",
        "payload_keys": ("etf_primary_market",),
        # forward-note 4: never repass the snapshot's coerced net_primary_market_flow;
        # drop it and degrade when it was present (a leg was coerced).
        "degraded_rule": "drop_coerced_net_flow_degrade_if_present",
    },
    {
        "family": "ncen_closed_end",
        "source_product": "ncen_closed_end_v1",
        "source_view": "sec_current_ncen_closed_end_profiles",
        "grain": "fund",
        "grain_origin": "fund",
        "snapshot_state_column": "closed_end_state",
        "snapshot_state_vocab": ("available", "unavailable", "not_applicable"),
        "state_map": _NCEN_STATE_MAP,
        "payload_schema_id": "ncen_closed_end_v1",
        "payload_keys": ("closed_end",),
        # forward-note 7: preserve the typed not_applicable reason (evidence_absent)
        # in snapshot_reason_code without reclassifying.
        "degraded_rule": "preserve_snapshot_reason",
    },
    {
        "family": "ncen_expense_brokerage",
        "source_product": "ncen_expense_brokerage_v1",
        "source_view": "sec_current_ncen_expense_brokerage_profiles",
        "grain": "fund",
        "grain_origin": "fund",
        "snapshot_state_column": "expense_brokerage_state",
        "snapshot_state_vocab": ("available", "unavailable", "not_applicable"),
        "state_map": _NCEN_STATE_MAP,
        "payload_schema_id": "ncen_expense_brokerage_v1",
        "payload_keys": ("expense_brokerage",),
        # forward-note 8: the snapshot state cannot tell an empty fund from a
        # reported one; degrade when every expense leg is NULL.
        "degraded_rule": "degrade_if_all_expense_legs_null",
    },
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
        "payload_keys": ("canonical_concept", "value_numeric", "declared_unit"),
        "degraded_rule": "snapshot_status_passthrough",
    },
    {
        "family": "rr1_shareholder_cost",
        "source_product": "rr1_shareholder_cost_profile_v1",
        "source_view": "sec_current_rr1_shareholder_cost_profiles",
        "grain": "series_class_fact",
        "grain_origin": "class",
        "snapshot_state_column": "status",
        "snapshot_state_vocab": ("available", "degraded", "unavailable", "not_applicable"),
        "state_map": _RR1_STATE_MAP,
        "payload_schema_id": "rr1_shareholder_cost_v1",
        "payload_keys": ("canonical_concept", "cost_group", "value_numeric", "declared_unit"),
        "degraded_rule": "snapshot_status_passthrough",
    },
    {
        "family": "rr1_waiver",
        "source_product": "rr1_waiver_profile_v1",
        "source_view": "sec_current_rr1_waiver_profiles",
        "grain": "series_class_fact",
        "grain_origin": "class",
        "snapshot_state_column": "status",
        "snapshot_state_vocab": ("available", "degraded", "unavailable", "not_applicable"),
        "state_map": _RR1_STATE_MAP,
        "payload_schema_id": "rr1_waiver_v1",
        # forward-note 10: reconciliation divergence is a quality flag, never an
        # adjusted value; gross/net/waiver fractions with declared unit.
        "payload_keys": (
            "waiver_over_assets", "gross_expense_over_assets", "net_expense_over_assets",
            "declared_unit", "termination_date", "term_days", "remaining_days",
            "gross_minus_waiver", "net_reconstruction_gap", "reconciliation_status",
            "reconciliation_tolerance", "cliff_horizon_days", "cliff_flag",
            "termination_reason_code",
        ),
        "degraded_rule": "snapshot_status_passthrough",
    },
    {
        "family": "rr1_class_cost_dispersion",
        "source_product": "rr1_class_cost_dispersion_v1",
        "source_view": "sec_current_rr1_class_cost_dispersion",
        "grain": "series",
        "grain_origin": "series",
        "snapshot_state_column": "status",
        "snapshot_state_vocab": ("available", "unavailable", "not_applicable"),
        "state_map": _NCEN_STATE_MAP,  # 3-state, no degraded (matches snapshot)
        "payload_schema_id": "rr1_class_cost_dispersion_v1",
        # forward-note 9: consume post-rename names; numeric_class_count is NEVER
        # presented as the class count -- class_total is.
        "payload_keys": (
            "numeric_class_count", "class_total", "net_min", "net_max", "net_spread",
            "net_min_class_id", "net_max_class_id", "per_class_evidence",
        ),
        "degraded_rule": "none",
    },
    {
        "family": "rr1_turnover",
        "source_product": "rr1_turnover_profile_v1",
        "source_view": "sec_current_rr1_turnover_profiles",
        "grain": "series_class_fact",
        "grain_origin": "class",
        "snapshot_state_column": "status",
        "snapshot_state_vocab": ("available", "degraded", "unavailable", "not_applicable"),
        "state_map": _RR1_STATE_MAP,
        "payload_schema_id": "rr1_turnover_v1",
        # forward-note 13: number + consistency flag only; NEVER the narrative text.
        "payload_keys": (
            "turnover_rate", "declared_unit", "turnover_numeric_present",
            "turnover_text_present", "narrative_consistency",
        ),
        "degraded_rule": "snapshot_status_passthrough",
    },
    {
        "family": "rr1_reported_performance",
        "source_product": "rr1_reported_performance_profile_v1",
        "source_view": "sec_current_rr1_reported_performance_profiles",
        "grain": "series_class_fact",
        "grain_origin": "class",
        "snapshot_state_column": "status",
        "snapshot_state_vocab": ("available", "degraded", "unavailable", "not_applicable"),
        "state_map": _RR1_STATE_MAP,
        "payload_schema_id": "rr1_reported_performance_v1",
        # forward-notes 11 & 16: standalone reported-performance; treatment carries
        # the load/tax signal (incl. 'unclassified'); no fabricated boolean.
        "payload_keys": (
            "canonical_concept", "value_kind", "value_numeric", "value_date",
            "value_label", "declared_unit", "treatment",
        ),
        "degraded_rule": "snapshot_status_passthrough",
    },
    {
        "family": "rr1_benchmark",
        "source_product": "rr1_benchmark_profile_v1",
        "source_view": "sec_current_rr1_benchmark_profiles",
        "grain": "series_class",
        "grain_origin": "class",
        "snapshot_state_column": "status",
        "snapshot_state_vocab": ("available", "degraded", "unavailable", "not_applicable"),
        "state_map": _RR1_STATE_MAP,
        "payload_schema_id": "rr1_benchmark_v1",
        "payload_keys": (
            "primary_benchmark", "benchmark_consistency", "declared_benchmark_count",
            "observation_count", "context_count", "document_count", "period_count",
            "per_benchmark_evidence",
        ),
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
SURFACE_DIGEST = "sha256:bb51512f03a682d1d68f06693f3c73e6bff6e9ddaf06bf9c265b6fc9cfbd36e3"


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
    if len(names) != 16:
        mismatches.append(f"expected 16 serving families, declared {len(names)}")

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
