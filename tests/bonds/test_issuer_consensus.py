"""Issuer attribution by reported-name consensus (pure; no DB).

The rules under test are the PRE-REGISTERED ones (threshold 0.60, LEI veto,
CUSIP6-only vehicle veto) and, above all, the abstentions: this module exists to
turn reporting noise into a name WITHOUT ever inventing one, so most of what is
pinned here is what it refuses to say.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.bonds import issuer_consensus as ic
from src.bonds.security_master import (
    ResolvedSecurity,
    apply_reference_terms,
    attribute_issuers,
    resolve_securities,
)
from src.bonds.security_master import SecurityObservation


# --------------------------------------------------------------------------- #
# Normalisation
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "raw",
    [
        "AFLAC INC",
        "Aflac, Inc.",
        "AFLAC INCORPORATED",
        "Aflac Incorporated",
        "AFLAC INC 3.6% 04/01/30",
        "AFLAC INC 3.60%",
    ],
)
def test_reported_spellings_of_one_issuer_normalise_together(raw: str) -> None:
    """Every spelling measured on the live cohort for one CUSIP9 collapses.

    Case, punctuation, the corporate suffix and an appended coupon/maturity are
    exactly the axes filers vary on; none of them is evidence.
    """
    assert ic.normalize_issuer_name(raw) == "AFLAC"


def test_normalisation_never_returns_an_empty_key() -> None:
    """A name made entirely of suffix tokens keeps its punctuation-stripped form.

    Otherwise two unrelated issuers would both key on '' and vote together.
    """
    assert ic.normalize_issuer_name("The Company, Inc.") == "THE COMPANY INC"


def test_normalisation_of_blank_input_is_blank() -> None:
    assert ic.normalize_issuer_name(None) == ""
    assert ic.normalize_issuer_name("   ") == ""


def test_distinct_issuers_do_not_normalise_together() -> None:
    assert ic.normalize_issuer_name("ACME CORP") != ic.normalize_issuer_name("BETA CORP")


# --------------------------------------------------------------------------- #
# Prefix collapse
# --------------------------------------------------------------------------- #
def test_truncated_filings_collapse_into_their_complete_sibling() -> None:
    """A field-truncated name is the same string, not a competing claim."""
    canon = ic.collapse_prefix_clusters(
        ["AMC ENTERTAINMENT", "AMC ENTERTAINMENT HOLDINGS"]
    )
    assert canon["AMC ENTERTAINMENT"] == "AMC ENTERTAINMENT HOLDINGS"
    assert canon["AMC ENTERTAINMENT HOLDINGS"] == "AMC ENTERTAINMENT HOLDINGS"


def test_prefix_collapse_only_folds_on_a_word_boundary() -> None:
    """``ACME`` must not swallow ``ACMEX``: that is a different token, not a truncation."""
    canon = ic.collapse_prefix_clusters(["ACME", "ACMEX GROUP"])
    assert canon["ACME"] == "ACME"
    assert canon["ACMEX GROUP"] == "ACMEX GROUP"


def test_a_mid_word_truncation_does_not_fold_and_that_is_deliberate() -> None:
    """A NAMED limitation, pinned so nobody "fixes" it into a merge risk.

    ``AMC ENTERTAINMENT H`` cuts inside a token, so the word-boundary rule leaves
    it as its own cluster. Dropping the boundary requirement would also let
    ``ACME`` absorb ``ACMEX``, so the collapse stays conservative and the split
    is carried by the vote (and, failing that, by an honest abstention). The
    coverage numbers this campaign was sized against were measured under exactly
    this rule.
    """
    canon = ic.collapse_prefix_clusters(
        ["AMC ENTERTAINMENT H", "AMC ENTERTAINMENT HOLDINGS"]
    )
    assert canon["AMC ENTERTAINMENT H"] == "AMC ENTERTAINMENT H"


def test_the_served_name_is_never_a_truncation_when_a_complete_one_was_reported() -> None:
    verdict = ic.resolve_issuer(
        {"AMC ENTERTAINMENT": 6, "AMC Entertainment Holdings": 2},
        layer=ic.ATTRIBUTION_CUSIP9,
    )
    assert verdict.issuer_name == "AMC Entertainment Holdings"


# --------------------------------------------------------------------------- #
# Verdicts
# --------------------------------------------------------------------------- #
def test_unanimous_after_normalisation_publishes_the_modal_raw_spelling() -> None:
    verdict = ic.resolve_issuer(
        {"AFLAC INC": 5, "Aflac, Inc.": 2, "AFLAC INCORPORATED": 1},
        layer=ic.ATTRIBUTION_CUSIP9,
    )
    assert verdict.issuer_name == "AFLAC INC"      # a spelling a filer wrote
    assert verdict.attribution == ic.ATTRIBUTION_CUSIP9
    assert verdict.abstain_reason is None
    assert verdict.evidence["top_share"] == 1.0
    assert verdict.evidence["n_sources"] == 8


def test_a_clear_majority_wins_and_records_its_share() -> None:
    verdict = ic.resolve_issuer(
        {"ACME CORP": 7, "BETA HOLDINGS": 3}, layer=ic.ATTRIBUTION_CUSIP9,
    )
    assert verdict.issuer_name == "ACME CORP"
    assert verdict.evidence["top_share"] == 0.7
    assert verdict.evidence["clusters"] == 2


def test_a_split_vote_abstains_rather_than_guessing() -> None:
    verdict = ic.resolve_issuer(
        {"ACME CORP": 5, "BETA HOLDINGS": 5}, layer=ic.ATTRIBUTION_CUSIP9,
    )
    assert verdict.issuer_name is None
    assert verdict.abstain_reason == ic.ABSTAIN_NO_CONSENSUS
    # The losing candidates stay visible: an abstention is auditable, not silent.
    assert {c["name"] for c in verdict.evidence["candidates"]} == {"ACME", "BETA HOLDINGS"}
    assert verdict.evidence["top_share"] == 0.5


def test_disagreeing_legal_entities_veto_even_a_unanimous_name() -> None:
    """Two reported LEIs mean the entities really differ; the name cannot settle it."""
    verdict = ic.resolve_issuer(
        {"ACME CORP": 40}, layer=ic.ATTRIBUTION_CUSIP9,
        leis=["5493001KJTIIGC8Y1R12", "213800LBQA1Y9RPH2N54"],
    )
    assert verdict.issuer_name is None
    assert verdict.abstain_reason == ic.ABSTAIN_MULTIPLE_LEI
    assert len(verdict.evidence["distinct_lei"]) == 2


def test_one_reported_lei_does_not_veto() -> None:
    verdict = ic.resolve_issuer(
        {"ACME CORP": 4}, layer=ic.ATTRIBUTION_CUSIP9, leis=["5493001KJTIIGC8Y1R12"],
    )
    assert verdict.issuer_name == "ACME CORP"


def test_no_named_source_abstains() -> None:
    verdict = ic.resolve_issuer({}, layer=ic.ATTRIBUTION_CUSIP9)
    assert verdict.issuer_name is None
    assert verdict.abstain_reason == ic.ABSTAIN_NO_NAMED_SOURCE


def test_a_financing_vehicle_is_publishable_at_cusip9_but_not_inferred_at_cusip6() -> None:
    """The vehicle veto is a FALLBACK guard, not a blanket ban.

    At the exact CUSIP9 the trust IS the issuer of that bond and must be served.
    At CUSIP6 the name is being inferred onto neighbouring securities, where a
    vehicle name does not generalise the way an operating company's does.
    """
    votes = {"ACME CAPITAL TRUST": 9}
    exact = ic.resolve_issuer(votes, layer=ic.ATTRIBUTION_CUSIP9)
    assert exact.issuer_name == "ACME CAPITAL TRUST"

    inferred = ic.resolve_issuer(votes, layer=ic.ATTRIBUTION_CUSIP6)
    assert inferred.issuer_name is None
    assert inferred.abstain_reason == ic.ABSTAIN_VEHICLE_NAME


def test_verdicts_are_deterministic_under_a_tie_of_votes() -> None:
    """A replay must build the same publication byte-for-byte."""
    votes = {"ACME CORP": 6, "ACME GROUP": 4}
    first = ic.resolve_issuer(votes, layer=ic.ATTRIBUTION_CUSIP9)
    second = ic.resolve_issuer(dict(reversed(list(votes.items()))),
                               layer=ic.ATTRIBUTION_CUSIP9)
    assert first.issuer_name == second.issuer_name


# --------------------------------------------------------------------------- #
# Overlay wiring on resolved securities
# --------------------------------------------------------------------------- #
def _obs(oid: str, cusip9: str, issuer: str | None) -> SecurityObservation:
    return SecurityObservation(
        observation_id=oid, observation_date=date(2026, 6, 30),
        cusip9_input=cusip9, issuer_name=issuer,
        source_lineage={"source_surface": "test"},
    )


def _by_cusip9(securities: tuple[ResolvedSecurity, ...]) -> dict[str, ResolvedSecurity]:
    return {sec.cusip9 or "": sec for sec in securities}


def test_attribution_recovers_a_name_the_exact_cusip9_reports() -> None:
    resolved = resolve_securities([
        _obs("o1", "037833100", "AFLAC INC"),
        _obs("o2", "037833100", "Aflac, Inc."),
        _obs("o3", "037833100", "AFLAC INC 3.6% 04/01/30"),
    ])
    assert resolved.securities[0].measured_terms["issuer_name"] is None  # pre-overlay

    attributed = attribute_issuers(resolved.securities)
    sec = attributed[0]
    assert sec.measured_terms["issuer_name"] == "AFLAC INC"
    assert sec.terms["issuer_name"] == "AFLAC INC"
    evidence = sec.identity_evidence["issuer_attribution"]
    assert evidence["attribution"] == ic.ATTRIBUTION_CUSIP9
    assert evidence["n_sources"] == 3
    assert evidence["abstain_reason"] is None


def test_cusip6_fallback_only_fires_when_the_exact_cusip9_abstains() -> None:
    """A security nobody names at its own CUSIP9 borrows from its issuer block."""
    resolved = resolve_securities([
        # 037833 block: two securities named, one unnamed.
        _obs("o1", "037833100", "ACME CORP"),
        _obs("o2", "037833100", "ACME CORP"),
        _obs("o3", "037833AA1", "ACME CORP"),
        _obs("o4", "037833BB2", None),
    ])
    attributed = _by_cusip9(attribute_issuers(resolved.securities))

    borrowed = attributed["037833BB2"]
    assert borrowed.measured_terms["issuer_name"] == "ACME CORP"
    assert borrowed.identity_evidence["issuer_attribution"]["attribution"] == ic.ATTRIBUTION_CUSIP6
    # The exactly-named ones stayed on layer 1 and never touched the fallback.
    named = attributed["037833100"]
    assert named.measured_terms["issuer_name"] == "ACME CORP"
    assert named.identity_evidence["issuer_attribution"]["layer"] == ic.ATTRIBUTION_CUSIP9


def test_a_security_with_no_name_anywhere_abstains_and_says_why() -> None:
    resolved = resolve_securities([_obs("o1", "037833BB2", None)])
    sec = attribute_issuers(resolved.securities)[0]
    assert sec.measured_terms["issuer_name"] is None
    assert sec.identity_evidence["issuer_attribution"]["abstain_reason"] == (
        ic.ABSTAIN_NO_NAMED_SOURCE
    )


def test_the_cusip6_lei_veto_sees_siblings_this_batch_does_not_publish() -> None:
    """The fallback's LEI scope is the whole prefix, not just the built universe.

    A disagreement reported against a sibling CUSIP9 that never made it into this
    publication still means the filings disagree about who issues under this
    prefix. Scoping the veto to the published rows would make the fallback layer
    quietly more permissive than the rule its coverage was measured under.
    """
    resolved = resolve_securities([
        _obs("o1", "037833100", "ACME CORP"),
        _obs("o2", "037833100", "ACME CORP"),
        _obs("o3", "037833BB2", None),      # unnamed -> falls through to CUSIP6
    ])
    attributed = _by_cusip9(attribute_issuers(
        resolved.securities,
        lei_by_cusip9={
            "037833100": ["5493001KJTIIGC8Y1R12"],
            # Never published in this batch, but reported under the same prefix.
            "037833ZZ9": ["213800LBQA1Y9RPH2N54"],
        },
    ))
    borrowed = attributed["037833BB2"]
    assert borrowed.measured_terms["issuer_name"] is None
    assert borrowed.identity_evidence["issuer_attribution"]["abstain_reason"] == (
        ic.ABSTAIN_MULTIPLE_LEI
    )
    # The exact-CUSIP9 layer is untouched: only ITS own LEI is in ITS scope.
    assert attributed["037833100"].measured_terms["issuer_name"] == "ACME CORP"


def test_a_disagreeing_lei_blocks_attribution_end_to_end() -> None:
    resolved = resolve_securities([
        _obs("o1", "037833100", "ACME CORP"),
        _obs("o2", "037833100", "ACME CORP"),
    ])
    sec = attribute_issuers(
        resolved.securities,
        lei_by_cusip9={"037833100": ["5493001KJTIIGC8Y1R12", "213800LBQA1Y9RPH2N54"]},
    )[0]
    assert sec.measured_terms["issuer_name"] is None
    assert sec.identity_evidence["issuer_attribution"]["abstain_reason"] == ic.ABSTAIN_MULTIPLE_LEI


# --------------------------------------------------------------------------- #
# Reference-terms overlay
# --------------------------------------------------------------------------- #
def test_reference_terms_fill_only_what_the_chain_does_not_carry() -> None:
    resolved = resolve_securities([
        SecurityObservation(
            observation_id="o1", observation_date=date(2026, 6, 30),
            cusip9_input="037833100", issuer_name="ACME CORP",
            coupon_rate="5.25", maturity_date=date(2030, 1, 15), coupon_type="Fixed",
            source_lineage={"source_surface": "test"},
        )
    ])
    enriched = apply_reference_terms(resolved.securities, {
        "037833100": {
            "coupon_rate": 9.99,            # chain already has 5.25 -> must NOT win
            "seniority": "Senior",          # chain has nothing -> fills
            "callable": True,
            "amount_outstanding_mm": 525.0,
            "day_count": None,              # absent in the reference -> stays absent
        }
    })
    sec = enriched[0]
    assert sec.measured_terms["coupon_rate"] == "5.25"
    assert sec.measured_terms["seniority"] == "Senior"
    assert sec.terms["callable"] is True
    assert sec.terms["amount_outstanding_mm"] == 525.0
    assert sec.measured_terms["day_count"] is None
    assert sec.identity_evidence["reference_terms"] == {
        "basis": "vendor_reference",
        "fields": ["amount_outstanding_mm", "callable", "seniority"],
    }


def test_reference_terms_never_supply_an_issuer_name() -> None:
    """Names come from reported filings and their consensus, never the reference."""
    from src.bonds import security_master

    assert "issuer_name" not in security_master._REFERENCE_SCALARS
    assert "issuer_name" not in security_master._REFERENCE_TERMS_ONLY
    assert "issuer_name" not in security_master._REFERENCE_TRISTATE


def test_no_reference_table_is_a_no_op_not_a_failure() -> None:
    resolved = resolve_securities([_obs("o1", "037833100", "ACME CORP")])
    assert apply_reference_terms(resolved.securities, None) == resolved.securities
    assert apply_reference_terms(resolved.securities, {}) == resolved.securities
