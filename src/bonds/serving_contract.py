"""Workers-side ``bond_serving_v1`` cross-repo contract counterpart.

This is the publisher half of the Bonds serving handshake the reader declares in
the app repo (``backend/app/contracts/bond_serving_v1.py``). Both repositories
build the SAME canonical serving surface -- the product string, the public
serving-fact columns, the ordered SURFACE catalogue (catalog / detail /
observations / fund_exposure) with each surface's grain, source projection and
payload schema id, the lane discriminator vocabulary, the identity/observation
ambiguity vocabulary, the publication/pointer semantics, and the internal-key
scrub blocklist -- and pin the SAME frozen ``SURFACE_DIGEST``.

Mechanism mirrors ``sec_serving.contract`` / ``mixed_quant_contract`` exactly: a
surface change on either side changes the recomputed digest and fails that side's
verifier until the frozen constant is deliberately re-synced on BOTH repos.

PRODUCT DECISION -- SIBLING PRODUCT, NOT A REGULATORY EXTENSION (Increment 2 Task 5).
  Bonds are published as a SEPARATE serving product ``bond_serving_v1`` with its
  own current pointer and its OWN digest constant pair, rather than as new
  families inside ``sec_regulatory_serving_v1``.  Rationale:
    * Bonds are security-centric (grain ``security`` / ``security_observation`` /
      ``security_fund``), while every regulatory family is fund/series/class-centric
      -- a single ``family`` facts table could not carry both grains cleanly.
    * Bonds publish on the daily chain (Task 6) at a different cadence and
      lifecycle from the fund regulatory families; a separate product keeps fund
      dossier freshness decoupled from bond freshness (a stale bond price never
      degrades a fund dossier, and vice versa).
    * Bonds DTOs must carry NO source/vendor literals and NEUTRAL product dates
      (``as_of`` / ``observation_date``) -- the regulatory family contract's
      ``source: "ncen"|"rr1"`` heritage is explicitly not copied here
      (plan Global Constraint 4).
  Consequently the regulatory ``SURFACE_DIGEST`` (``sha256:aa134a7e...``) is left
  untouched; this module owns an independent ``bond_serving_v1`` digest.

PINNED SERVING BOUNDARY (documented here so both repos carry the decision):
  * The serving DATA lives in the datalake ``market`` database and is produced
    ONLY by workers via the derived-publication protocol
    (``sec_derived_publications`` + current pointer). A serving version is one
    complete, atomically promoted publication.
  * ``bond_serving_facts`` projects PUBLIC columns only; the app reads it BY EXACT
    ``publication_id`` (its pin row records the exact worker publication +
    ``publication_version``) via a read-only datalake session, and NEVER reads
    ``sec_current_*`` on the request path and NEVER writes the serving tables.

LANE DISCRIMINATOR (spec §2 / plan Global Constraint 3):
  The ``observations`` surface carries a MANDATORY ``lane`` discriminator both as a
  fact column and inside every observation payload. The two lanes ``latest``
  (informative) and ``fund_asof`` (point-in-time, ``observation_date <= as_of``,
  no look-ahead) are NEVER interchangeable; the DDL forbids a non-observations row
  from carrying a lane and forbids an observations payload without one.

FRESHNESS ANCHOR (Phase 10 precondition -- documented in both mirrors):
  The ``fund_asof`` lane's freshness (``is_stale`` / ``observation_age_days``) is
  anchored at the publication's BUILD ``as_of``: the materializer measures staleness
  against the build as_of, not the caller's requested as_of. RE-ANCHORING freshness at
  the REQUESTED as_of (so a reader asking "as of date X" sees staleness measured
  against X) is a PRECONDITION for the ``fund_asof`` lane becoming LOAD-BEARING in
  Phase 10. Until then the lane is informative-only and the ``latest`` lane carries
  ``is_stale = NULL`` (honest absence -- no as_of anchor to measure against). This note
  is docstring-only and does NOT enter ``surface()``, so it does not move the digest.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

CONTRACT_VERSION = "bond_serving_v1"
SERVING_PRODUCT = "bond_serving_v1"
POINTER_TABLE = "sec_derived_current_pointers"
APP_PIN_TABLE = "bond_serving_publications"

# The public serving-fact column surface (ordered). No internal provenance,
# source_run_id, source_lineage, holding_id, accession_number, identity_key or
# observation_id ever reaches this surface.
SERVING_FACT_COLUMNS: tuple[str, ...] = (
    "publication_id",
    "surface",
    "security_id",
    "lane",
    "fund_key",
    "fact_key",
    "state",
    "reason_code",
    "identity_state",
    "ambiguity_state",
    "as_of",
    "observation_date",
    "coverage_pct",
    "payload",
)

# The four-state serving vocabulary (app CapabilityState) every surface maps onto.
SERVING_STATES: tuple[str, ...] = ("available", "degraded", "unavailable", "not_applicable")

# Typed reason vocabulary. Neutral, product-scoped, no source/vendor literals.
#   * ``identity_ambiguous``    -- catalog/detail: unresolved CUSIP/ISIN identity;
#                                  the conflicting values ride as NEUTRAL evidence,
#                                  never the internal conflict lineage.
#   * ``observation_ambiguous`` -- observations: a duplicate in the matching cohort
#                                  (``daily_key_state='duplicate_in_matching_cohort'``);
#                                  both rows are retained, never an arbitrary winner.
#   * ``observation_stale``     -- fund_asof lane observation age >= 31 days.
#   * ``terms_incomplete``      -- detail: reserved for degraded terms completeness.
#   * ``source_unavailable``    -- unavailable state.
#   * ``not_applicable``        -- not_applicable state.
SERVING_REASON_CODES: tuple[str, ...] = (
    "identity_ambiguous",
    "not_applicable",
    "observation_ambiguous",
    "observation_stale",
    "source_unavailable",
    "terms_incomplete",
)

# The mandatory observations lane discriminator (spec §2, Global Constraint 3).
LANE_VOCABULARY: tuple[str, ...] = ("latest", "fund_asof")

# Explicit identity/observation ambiguity vocabulary (spec §1/§2). This is a
# SEPARATE axis from the 4-state availability vocabulary: a duplicate observation
# is still ``available`` data but carries ``ambiguity_state='ambiguous'``.
AMBIGUITY_STATES: tuple[str, ...] = ("resolved", "ambiguous")

# Internal-identifier keys the scrubber strips from every payload (leak contract).
# Superset of the identifiers that could ride on a bond source row: raw ingestion
# ids, source lineage/provenance blobs, N-PORT filing keys, the internal identity
# key material (which embeds the raw CUSIP/ISIN prefix), and observation ids.
SCRUB_BLOCKLIST: tuple[str, ...] = (
    "accession_number",
    "contributing_observation_ids",
    "holding_id",
    "identity_key",
    "ingestion_run_id",
    "observation_id",
    "observation_ids",
    "provenance",
    "raw_row_id",
    "registrant_cik",
    "source_lineage",
    "source_run_id",
    "source_table",
    "source_typed_projection",
    "text_block_md5",
)

# Ordered SURFACE catalogue. ``source_view`` is the current-pointer relation the
# materializer projects; ``grain`` documents the serving grain; ``state_rule``
# names the availability/ambiguity rule the materializer applies.
SURFACES: tuple[dict[str, Any], ...] = (
    {
        "surface": "catalog",
        "source_product": "bond_security_v1",
        "source_view": "sec_current_bond_security_v1",
        "grain": "security",
        "lane_required": False,
        "state_rule": "identity_state_to_serving",
        "payload_schema_id": "bond_catalog_v1",
        # search-ready identity + summary terms + data state. Neutral keys only.
        # aliases_cusip9 / aliases_isin are arrays of PUBLIC normalized identifiers
        # (identity != source; only VALID aliases, never rejected/placeholder) so the
        # app catalog can be searched by CUSIP9/ISIN (spec §3 query por identificador).
        # Wave 1: + the computed summary values latest_price_pct (% of par, from the
        # promoted latest price lane) and security_ytm / security_ytw (decimal
        # fractions, from the promoted current metric view) — null-honest: a security
        # without an eligible price / available metric serves the key as JSON null.
        # Wave 1b: + issuer_country (ISO-3166-1 alpha-2) and issuer_sector (the
        # reported issuer category) — the reported classification resolved from the
        # holding grain to the security grain by reported consensus; a security no
        # holding classifies serves the key as JSON null, never a guess.
        # Wave 1c: + security_effective_duration (modified duration in years, the
        # analytic closed form over the published coupon/maturity and the observed
        # price-date yield). Publishing the key is what unlocks the reader's gated
        # duration filter — the app derives its answerable filter set FROM these
        # keys, so this addition is the whole mechanism, not a cosmetic one.
        "payload_keys": (
            "aliases_cusip9", "aliases_isin", "coupon_rate", "coupon_type", "currency",
            "display", "identity_state", "is_144a", "issuer_country", "issuer_name",
            "issuer_sector", "latest_price_pct", "maturity_date",
            "security_effective_duration", "security_ytm", "security_ytw",
        ),
    },
    {
        "surface": "detail",
        "source_product": "bond_security_v1",
        "source_view": "sec_current_bond_security_v1",
        "grain": "security",
        "lane_required": False,
        "state_rule": "identity_state_to_serving",
        "payload_schema_id": "bond_detail_v1",
        # full terms incl. call/put schedule, 144A, PIT aliases + neutral ambiguity
        # evidence (the conflicting identifier VALUES only, never observation ids).
        # Wave 1: + the computed metrics current_yield / security_ytm / security_ytw
        # (decimal fractions) and wal (years) from the promoted current metric view —
        # null-honest: any non-available metric row (or no row) serves JSON null,
        # never a synthetic 0. Coupon stays a reported TERM, never a yield.
        # Wave 1c: + security_effective_duration and latest_price_pct (so the
        # detail shows the same price the catalog filters on), + the two reference
        # terms the filing itself never reports: ``callable`` (a boolean fact, NOT
        # a call schedule — no dates are known, so no schedule is fabricated) and
        # ``amount_outstanding_mm`` (millions of the issue currency). Both stay
        # null-honest for a security the reference does not cover.
        "payload_keys": (
            "aliases", "amount_outstanding_mm", "call_schedule", "callable", "coupon_rate",
            "coupon_schedule", "coupon_type", "currency", "current_yield", "day_count",
            "identity_evidence", "identity_state", "is_144a", "issuer_name",
            "latest_price_pct", "maturity_date", "put_schedule", "secured",
            "security_effective_duration", "security_ytm", "security_ytw", "seniority",
            "settlement_convention", "wal",
        ),
    },
    {
        "surface": "observations",
        "source_product": "bond_price_observation_v1",
        "source_view": "bond_price_latest_v1",
        "grain": "security_observation",
        "lane_required": True,
        "state_rule": "freshness_and_ambiguity",
        "payload_schema_id": "bond_observations_v1",
        # WITH the mandatory ``lane`` discriminator + freshness/ambiguity states.
        "payload_keys": (
            "accrued_treatment", "daily_key_state", "is_144a", "is_stale", "lane",
            "observation_age_days", "observation_date", "price", "price_state",
            "price_type", "ytm",
        ),
    },
    {
        "surface": "fund_exposure",
        "source_product": "sec_nport_holdings_v2",
        "source_view": "sec_nport_holdings_v2_current",
        "grain": "security_fund",
        "lane_required": False,
        "state_rule": "reverse_lookup_pit",
        "payload_schema_id": "bond_fund_exposure_v1",
        # N-PORT point-in-time reverse lookup, pre-aggregated at fund (series) grain.
        "payload_keys": (
            "as_of", "holding_market_value", "holding_pct_of_nav", "position_lot_count",
            "report_date", "series_id",
        ),
    },
)

# Frozen handshake digest -- MUST equal the app repo's
# ``app.contracts.bond_serving_v1.SURFACE_DIGEST`` byte-for-byte. Independent of
# the regulatory ``sec_regulatory_serving_v1`` digest (sibling product decision).
#
# Deliberately re-synced for Bonds Activation Wave 1c (catalog
# security_effective_duration; detail security_effective_duration +
# latest_price_pct + callable + amount_outstanding_mm). THE APP REPO MUST BE
# RE-SYNCED TO THIS SAME VALUE: until it is, the two repos declare different
# surfaces and the reader keeps its duration filter 422-gated. Publishing the
# extra payload keys ahead of that sync is harmless (the reader ignores keys it
# does not map), so the two merges do not have to be simultaneous -- only both.
#
# Previous frozen values, newest first:
#   sha256:5f7fd708b5adb3f1ad638316ed38c243056c1bc413069ff4f3ca0d00551ca6fc
#     (Wave 1b catalog issuer_country / issuer_sector)
#   sha256:96d2f0317be3ae287fdae393a3851122321e65cb26b8aa0094e819085a971e0d
#     (Wave 1 catalog + detail computed metric keys)
#   sha256:ee64be3339843e73b2d93d4862796b3ac3a94f51e57fb8fb9472592b50771a35
SURFACE_DIGEST = "sha256:cd14dcbe08339b31176f0f6c65b00d2f15e4b05fbf9e943fc0ca98a158329999"


def _surface_surface(surface: dict[str, Any]) -> dict[str, Any]:
    """The order-stable, shared subset of one surface entry hashed into the digest."""
    return {
        "surface": surface["surface"],
        "source_product": surface["source_product"],
        "source_view": surface["source_view"],
        "grain": surface["grain"],
        "lane_required": surface["lane_required"],
        "state_rule": surface["state_rule"],
        "payload_schema_id": surface["payload_schema_id"],
        "payload_keys": sorted(surface["payload_keys"]),
    }


def surface() -> dict[str, Any]:
    """The canonical, order-stable serving contract surface hashed into the digest."""
    return {
        "contract_version": CONTRACT_VERSION,
        "serving_product": SERVING_PRODUCT,
        "serving_fact_columns": list(SERVING_FACT_COLUMNS),
        "serving_states": sorted(SERVING_STATES),
        "serving_reason_codes": sorted(SERVING_REASON_CODES),
        "lane_vocabulary": sorted(LANE_VOCABULARY),
        "ambiguity_states": sorted(AMBIGUITY_STATES),
        "scrub_blocklist": sorted(SCRUB_BLOCKLIST),
        "surfaces": [_surface_surface(s) for s in sorted(SURFACES, key=lambda s: s["surface"])],
        "publication": {
            "protocol": "sec_derived_publications",
            "pointer_table": POINTER_TABLE,
            "app_pin_table": APP_PIN_TABLE,
            "promotion": "current_pointer",
            "atomic": True,
            "version_pin": "publication_version",
        },
    }


def compute_surface_digest() -> str:
    """Deterministic digest of the declared serving contract surface."""
    canonical = json.dumps(surface(), separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def surface_names() -> tuple[str, ...]:
    return tuple(s["surface"] for s in SURFACES)


def verify_contract() -> dict[str, Any]:
    """Recompute the digest and cross-check the publisher declarations.

    ``ok`` is True only when the recomputed digest matches the frozen
    ``SURFACE_DIGEST`` AND the declared surface is internally consistent (unique
    surfaces, exactly four, exactly one lane-required surface which is
    ``observations`` whose lane vocabulary is the two lanes, every payload key
    outside the scrub blocklist). Any drift is listed in ``mismatches`` so CI can
    fail loud, exactly like ``sec_serving.contract``.
    """
    recomputed = compute_surface_digest()
    mismatches: list[str] = []

    if recomputed != SURFACE_DIGEST:
        mismatches.append(f"surface digest {recomputed} != frozen {SURFACE_DIGEST}")

    names = surface_names()
    if len(names) != len(set(names)):
        mismatches.append("surface names are not unique")
    if set(names) != {"catalog", "detail", "observations", "fund_exposure"}:
        mismatches.append(f"expected the four bond serving surfaces, declared {sorted(names)}")

    lane_required = [s["surface"] for s in SURFACES if s["lane_required"]]
    if lane_required != ["observations"]:
        mismatches.append(
            f"exactly the observations surface must require a lane, got {lane_required}"
        )

    for surface_entry in SURFACES:
        for key in surface_entry["payload_keys"]:
            if key in SCRUB_BLOCKLIST:
                mismatches.append(
                    f"{surface_entry['surface']} payload key {key!r} is in the scrub blocklist"
                )

    return {
        "contract_version": CONTRACT_VERSION,
        "serving_product": SERVING_PRODUCT,
        "surface_digest": SURFACE_DIGEST,
        "recomputed_surface_digest": recomputed,
        "mismatches": mismatches,
        "ok": not mismatches,
    }
