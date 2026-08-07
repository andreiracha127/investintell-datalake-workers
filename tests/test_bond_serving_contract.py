"""Workers half of the ``bond_serving_v1`` digest handshake.

Fails if the declared bond serving surface drifts from the frozen
``SURFACE_DIGEST`` or from the app repo's mirrored constant. Pure, no DB. The
sibling product owns its OWN digest, independent of ``sec_regulatory_serving_v1``.
"""

from __future__ import annotations

import re
from pathlib import Path

from src.bonds import serving_contract as contract

_SCHEMA_SQL = (
    Path(__file__).resolve().parents[1] / "schemas" / "bond_serving_v1.sql"
).read_text(encoding="utf-8")

# The shared handshake value; MUST equal the app repo's
# ``app.contracts.bond_serving_v1.SURFACE_DIGEST`` byte-for-byte. Re-synced for
# Bonds Activation Wave 1c (catalog security_effective_duration; detail
# security_effective_duration + latest_price_pct + callable +
# amount_outstanding_mm). The app repo carries the mirror edit.
SHARED_SURFACE_DIGEST = "sha256:cd14dcbe08339b31176f0f6c65b00d2f15e4b05fbf9e943fc0ca98a158329999"


def test_workers_declare_serving_product() -> None:
    assert contract.SERVING_PRODUCT == "bond_serving_v1"
    assert contract.CONTRACT_VERSION == "bond_serving_v1"


def test_frozen_digest_matches_declared_surface() -> None:
    assert contract.compute_surface_digest() == contract.SURFACE_DIGEST


def test_frozen_digest_equals_the_cross_repo_handshake() -> None:
    assert contract.SURFACE_DIGEST == SHARED_SURFACE_DIGEST


def test_bond_serving_digest_is_independent_of_the_regulatory_digest() -> None:
    from src.sec_serving import contract as regulatory

    assert contract.SURFACE_DIGEST != regulatory.SURFACE_DIGEST


def test_contract_verifies_clean() -> None:
    verdict = contract.verify_contract()
    assert verdict["ok"] is True, verdict["mismatches"]


def test_all_four_surfaces_declared() -> None:
    names = contract.surface_names()
    assert len(names) == len(set(names)) == 4
    assert set(names) == {"catalog", "detail", "observations", "fund_exposure"}


def test_only_observations_requires_a_lane() -> None:
    lane_required = [s["surface"] for s in contract.SURFACES if s["lane_required"]]
    assert lane_required == ["observations"]
    assert set(contract.LANE_VOCABULARY) == {"latest", "fund_asof"}


def test_serving_states_are_the_four_state_vocabulary() -> None:
    assert set(contract.SERVING_STATES) == {"available", "degraded", "unavailable", "not_applicable"}


def test_ambiguity_states_are_the_two_state_vocabulary() -> None:
    # Lock-step with the sibling vocabularies (states/reasons/lanes): the ambiguity
    # axis is exactly {resolved, ambiguous}, in that order, and is hashed into the
    # cross-repo digest so it cannot drift from the app mirror silently.
    assert contract.AMBIGUITY_STATES == ("resolved", "ambiguous")
    assert set(contract.AMBIGUITY_STATES) == {"resolved", "ambiguous"}
    assert contract.surface()["ambiguity_states"] == sorted(contract.AMBIGUITY_STATES)


def test_catalog_carries_searchable_alias_arrays() -> None:
    catalog = next(s for s in contract.SURFACES if s["surface"] == "catalog")
    assert {"aliases_cusip9", "aliases_isin"} <= set(catalog["payload_keys"])


def test_catalog_payload_serves_computed_metric_keys_in_order() -> None:
    """Wave 1 gave the catalog payload latest_price_pct + security_ytm +
    security_ytw; Wave 1b added issuer_country + issuer_sector; Wave 1c adds
    security_effective_duration. Each sits in its alphabetical position (the
    contract's key-ordering convention), pinned as a full-tuple equality so a
    drive-by key can't ride in."""
    catalog = next(s for s in contract.SURFACES if s["surface"] == "catalog")
    assert catalog["payload_keys"] == (
        "aliases_cusip9", "aliases_isin", "coupon_rate", "coupon_type", "currency",
        "display", "identity_state", "is_144a", "issuer_country", "issuer_name",
        "issuer_sector", "latest_price_pct", "maturity_date",
        "security_effective_duration", "security_ytm", "security_ytw",
    )


def test_detail_payload_serves_computed_metric_keys_in_order() -> None:
    """Wave 1 gave the detail payload current_yield + security_ytm +
    security_ytw + wal; Wave 1c adds security_effective_duration,
    latest_price_pct and the two reference terms (callable,
    amount_outstanding_mm). Full-tuple pin, alphabetical."""
    detail = next(s for s in contract.SURFACES if s["surface"] == "detail")
    assert detail["payload_keys"] == (
        "aliases", "amount_outstanding_mm", "call_schedule", "callable", "coupon_rate",
        "coupon_schedule", "coupon_type", "currency", "current_yield", "day_count",
        "identity_evidence", "identity_state", "is_144a", "issuer_name",
        "latest_price_pct", "maturity_date", "put_schedule", "secured",
        "security_effective_duration", "security_ytm", "security_ytw", "seniority",
        "settlement_convention", "wal",
    )


def test_callability_is_published_as_a_fact_and_never_as_a_schedule() -> None:
    """``callable`` reaching the detail must NOT be read as call DATES existing.

    The reference states only THAT a bond is callable; no call date is known, so
    the call_schedule key stays whatever the filing reported and the worst-case
    yield stays out of the metric vocabulary. If someone ever adds a call
    schedule derived from the callable flag, this pin is the thing that argues.
    """
    detail = next(s for s in contract.SURFACES if s["surface"] == "detail")
    assert "callable" in detail["payload_keys"]
    assert "call_schedule" in detail["payload_keys"]
    assert "security_ytw" in detail["payload_keys"]


def test_observations_raw_ytm_surface_is_untouched() -> None:
    """The observations surface (raw ``ytm`` included) is NOT part of the Wave-1
    extension — belt and suspenders: the app drops raw ytm, the workers keep
    serving it unchanged. Full-tuple pin so any drift moves this test first."""
    observations = next(s for s in contract.SURFACES if s["surface"] == "observations")
    assert observations["payload_keys"] == (
        "accrued_treatment", "daily_key_state", "is_144a", "is_stale", "lane",
        "observation_age_days", "observation_date", "price", "price_state",
        "price_type", "ytm",
    )


def test_no_payload_key_is_an_internal_identifier() -> None:
    for surface in contract.SURFACES:
        assert not (set(surface["payload_keys"]) & set(contract.SCRUB_BLOCKLIST)), surface["surface"]


def test_sql_scrub_blocklist_matches_the_contract_blocklist() -> None:
    """The plpgsql scrub blocklist and the Python contract blocklist are one source
    of truth (both hashed into the digest) -- assert they cannot silently diverge."""
    array_body = re.search(
        r"blocked text\[\]\s*:=\s*ARRAY\[(.*?)\]", _SCHEMA_SQL, re.DOTALL
    )
    assert array_body, "scrub blocklist ARRAY literal not found in DDL"
    sql_keys = set(re.findall(r"'([^']+)'", array_body.group(1)))
    assert sql_keys == set(contract.SCRUB_BLOCKLIST)


def test_latest_price_inline_eligibility_matches_the_canonical_predicate() -> None:
    """Review IMP-2: the materializer's inline latest_price_pct eligibility and the
    canonical ``bond_price_is_eligible`` predicate
    (schemas/bond_price_eligibility_v1.sql) are one source of truth -- same idiom
    as the scrub-blocklist parity test above: regex-extract each per-column
    condition from the DDL and from the inline SQL and require them identical
    after whitespace normalization, so future drift (a price_type widened, an
    accrued_treatment or price_state arm dropped) fails loud instead of silently
    serving a price the canonical predicate would reject.

    DELIBERATE, STRUCTURALLY SOUND omission -- ``identity_state``: the inline
    predicate reads ``bond_price_latest_v1``, which projects the PUBLISHED
    observation snapshot; published rows are resolved by construction (an
    unresolved identifier never reaches a lane, and the lane view carries no
    identity_state column). The parity check therefore requires identity_state to
    be the ONLY canonical condition without an inline counterpart.
    """
    from src.bonds import serving_materializer as materializer

    eligibility_sql = (
        Path(__file__).resolve().parents[1] / "schemas" / "bond_price_eligibility_v1.sql"
    ).read_text(encoding="utf-8")
    body = re.search(
        r"FUNCTION bond_price_is_eligible\(.*?\$\$(.*?)\$\$", eligibility_sql, re.DOTALL
    )
    assert body, "bond_price_is_eligible body not found in the eligibility DDL"

    def conditions(sql: str, column_prefix: str) -> dict[str, str]:
        pairs = re.findall(column_prefix + r"(\w+)\s+(IN\s*\([^)]*\)|=\s*'[^']*')", sql)
        return {column: re.sub(r"\s+", " ", condition) for column, condition in pairs}

    canonical = conditions(body.group(1), r"p_")
    inline = conditions(materializer._LATEST_PRICE_PCT_SQL, r"p\.")

    assert set(canonical) - set(inline) == {"identity_state"}, (
        "the inline predicate may omit ONLY identity_state (resolved by lane construction)"
    )
    assert set(inline) <= set(canonical), (
        "the inline predicate tests a column the canonical predicate does not"
    )
    for column, condition in inline.items():
        assert condition == canonical[column], (
            f"eligibility drift on {column!r}: inline {condition!r} "
            f"!= canonical {canonical[column]!r}"
        )
    # exactly price_type, accrued_treatment, daily_key_state, price_state.
    assert len(inline) == 4


def test_digest_moves_when_surface_changes(monkeypatch) -> None:
    baseline = contract.compute_surface_digest()
    monkeypatch.setattr(
        contract, "SERVING_FACT_COLUMNS", contract.SERVING_FACT_COLUMNS + ("smuggled",)
    )
    assert contract.compute_surface_digest() != baseline
