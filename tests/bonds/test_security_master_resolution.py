"""Pure identity/terms resolution for the bond security master (DB-free).

These exercise the deterministic uuid5 identity key, the same-CUSIP-collapses
rule, honest rejection of placeholder/synthetic identifiers, the explicit
``ambiguous`` state for conflicting evidence (never an arbitrary winner), and the
point-in-time alias windows that close when superseded.
"""

from __future__ import annotations

from datetime import date
from uuid import uuid5

from src.bonds.security_master import (
    NAMESPACE_BOND_SECURITY,
    SecurityObservation,
    resolve_securities,
)


def _obs(observation_id: str, observation_date: date, **fields: object) -> SecurityObservation:
    return SecurityObservation(
        observation_id=observation_id,
        observation_date=observation_date,
        source_lineage={"engine": "test", "observation_id": observation_id},
        **fields,
    )


def test_same_cusip_in_multiple_lots_collapses_to_one_security() -> None:
    result = resolve_securities(
        [
            _obs("o1", date(2026, 3, 31), cusip9_input="037833100", issuer_name="Acme", currency="USD"),
            _obs("o2", date(2026, 6, 30), cusip9_input=" 037833100 ", issuer_name="Acme", currency="USD"),
        ]
    )

    assert result.rejected == ()
    assert len(result.securities) == 1
    sec = result.securities[0]
    assert sec.identity_key == "cusip9:037833100"
    assert sec.security_id == uuid5(NAMESPACE_BOND_SECURITY, "cusip9:037833100")
    assert sec.identity_state == "resolved"
    assert sec.identity_reason_code is None
    assert sorted(sec.contributing_observation_ids) == ["o1", "o2"]
    # One open cusip9 alias window anchored on the earliest observation date.
    cusip_aliases = [a for a in sec.aliases if a.alias_kind == "cusip9"]
    assert len(cusip_aliases) == 1
    assert cusip_aliases[0].alias_value == "037833100"
    assert cusip_aliases[0].valid_from == date(2026, 3, 31)
    assert cusip_aliases[0].valid_to is None


def test_security_id_is_deterministic_across_calls() -> None:
    a = resolve_securities([_obs("o1", date(2026, 6, 30), cusip9_input="037833100")])
    b = resolve_securities([_obs("z9", date(2026, 6, 30), cusip9_input="037833100")])
    assert a.securities[0].security_id == b.securities[0].security_id


def test_placeholder_cusip_without_isin_is_rejected_not_published() -> None:
    result = resolve_securities([_obs("o1", date(2026, 6, 30), cusip9_input="000000000")])
    assert result.securities == ()
    assert len(result.rejected) == 1
    rej = result.rejected[0]
    assert rej.observation_id == "o1"
    assert rej.cusip9_state == "placeholder"
    assert rej.reason_code == "no_qualified_identifier"


def test_synthetic_cusip_without_isin_is_rejected() -> None:
    result = resolve_securities([_obs("o1", date(2026, 6, 30), cusip9_input="CIK:12345")])
    assert result.securities == ()
    assert result.rejected[0].cusip9_state == "synthetic"


def test_placeholder_cusip_with_valid_isin_keys_on_isin_and_drops_cusip() -> None:
    result = resolve_securities(
        # A non-US/CA ISIN carries no embedded CUSIP9 to anchor on.
        [_obs("o1", date(2026, 6, 30), cusip9_input="000000000", isin_input="DE000BAY0017")]
    )
    assert result.rejected == ()
    sec = result.securities[0]
    assert sec.identity_key == "isin:DE000BAY0017"
    # The rejected placeholder CUSIP is never emitted as an alias.
    assert all(a.alias_kind != "cusip9" for a in sec.aliases)
    isin_aliases = [a for a in sec.aliases if a.alias_kind == "isin"]
    assert isin_aliases[0].alias_value == "DE000BAY0017"
    assert isin_aliases[0].valid_to is None


def test_isin_only_then_cusip9_arrival_yields_same_security_id() -> None:
    # (i) A US ISIN embeds its CUSIP9; the id is stable across the arrival of the
    # explicit CUSIP9 field in a later observation/snapshot.
    isin_only = resolve_securities([_obs("o1", date(2026, 3, 31), isin_input="US0378331005")])
    cusip_arrives = resolve_securities([_obs("o2", date(2026, 6, 30), cusip9_input="037833100")])
    assert isin_only.securities[0].identity_key == "cusip9:037833100"
    assert isin_only.securities[0].security_id == cusip_arrives.securities[0].security_id


def test_us_isin_and_cusip9_in_same_snapshot_collapse_to_one_security() -> None:
    # (ii) US ISIN lot + explicit CUSIP9 lot are the SAME instrument -> ONE security.
    result = resolve_securities(
        [
            _obs("o1", date(2026, 6, 30), isin_input="US0378331005"),
            _obs("o2", date(2026, 6, 30), cusip9_input="037833100"),
        ]
    )
    assert len(result.securities) == 1
    sec = result.securities[0]
    assert sec.identity_key == "cusip9:037833100"
    assert sorted(sec.contributing_observation_ids) == ["o1", "o2"]
    kinds = sorted(a.alias_kind for a in sec.aliases)
    assert kinds == ["cusip9", "isin"]


def test_non_us_ca_isin_keeps_isin_keying_unchanged() -> None:
    # (iii) A non-US/CA ISIN is never anchored to a CUSIP9.
    result = resolve_securities([_obs("o1", date(2026, 6, 30), isin_input="DE000BAY0017")])
    assert result.securities[0].identity_key == "isin:DE000BAY0017"
    assert all(a.alias_kind != "cusip9" for a in result.securities[0].aliases)


def test_us_isin_with_nsin_rejected_by_normalize_falls_back_to_isin_keying() -> None:
    # (iv) US ISIN whose embedded NSIN is a placeholder -> normalize_cusip9 rejects
    # it -> no repair, keying stays isin: (never a fabricated cusip9).
    result = resolve_securities([_obs("o1", date(2026, 6, 30), isin_input="US0000000005")])
    sec = result.securities[0]
    assert sec.identity_key == "isin:US0000000005"
    assert all(a.alias_kind != "cusip9" for a in sec.aliases)


def test_conflicting_isin_same_date_is_ambiguous_with_evidence_no_winner() -> None:
    result = resolve_securities(
        [
            _obs("o1", date(2026, 6, 30), cusip9_input="037833100", isin_input="US0378331005"),
            _obs("o2", date(2026, 6, 30), cusip9_input="037833100", isin_input="US0378331099"),
        ]
    )
    assert len(result.securities) == 1
    sec = result.securities[0]
    assert sec.identity_state == "ambiguous"
    assert sec.identity_reason_code == "conflicting_isin_evidence"
    # No arbitrary ISIN winner: only the unambiguous cusip9 identity alias survives.
    assert [a.alias_kind for a in sec.aliases] == ["cusip9"]
    assert sorted(sec.identity_evidence["conflicts"]["isin"]) == [
        "US0378331005",
        "US0378331099",
    ]


def test_differing_issuer_names_do_not_make_the_identity_ambiguous() -> None:
    """Reported issuer-name variance is a NAME question, never an identity one.

    Two funds spelling one bond's issuer differently says nothing about which
    instrument it is -- the CUSIP9 evidence is a singleton either way. Treating
    it as an identity conflict marked 9,688 of the 10,073 curated securities
    'ambiguous' (measured 2026-08-07), which the app serves as degraded. The
    resolver now records the variance and leaves the NAME null for the consensus
    pass to decide; it never invents a winner here.
    """
    result = resolve_securities(
        [
            _obs("o1", date(2026, 6, 30), cusip9_input="037833100", issuer_name="Acme Corp"),
            _obs("o2", date(2026, 6, 30), cusip9_input="037833100", issuer_name="Beta Corp"),
        ]
    )
    sec = result.securities[0]
    assert sec.identity_state == "resolved"
    assert sec.identity_reason_code is None
    assert "issuer_name" not in sec.identity_evidence["conflicts"]
    assert sec.identity_evidence["issuer_name_variance"] is True
    # No arbitrary winner at this stage: the name stays absent and every reported
    # spelling stays visible as evidence.
    assert sec.measured_terms["issuer_name"] is None
    assert sec.terms["issuer_name"] is None
    assert sorted(sec.identity_evidence["distinct_issuer_name"]) == ["Acme Corp", "Beta Corp"]
    # ...and the votes the consensus pass will use are carried on the row.
    assert dict(sec.issuer_votes) == {"Acme Corp": 1, "Beta Corp": 1}


def test_conflicting_isin_is_still_an_identity_ambiguity() -> None:
    """The narrowing above must not weaken the REAL identity conflict rule."""
    result = resolve_securities(
        [
            _obs("o1", date(2026, 6, 30), cusip9_input="037833100",
                 isin_input="US0378331005", issuer_name="Acme Corp"),
            _obs("o2", date(2026, 6, 30), cusip9_input="037833100",
                 isin_input="US0378331099", issuer_name="Acme Corp"),
        ]
    )
    sec = result.securities[0]
    assert sec.identity_state == "ambiguous"
    assert sec.identity_reason_code == "conflicting_isin_evidence"


def test_isin_alias_window_closes_when_superseded_across_dates() -> None:
    result = resolve_securities(
        [
            _obs("o1", date(2026, 3, 31), cusip9_input="037833100", isin_input="US0378331005"),
            _obs("o2", date(2026, 6, 30), cusip9_input="037833100", isin_input="US0378331099"),
        ]
    )
    sec = result.securities[0]
    assert sec.identity_state == "resolved"
    isin_aliases = sorted(
        (a for a in sec.aliases if a.alias_kind == "isin"), key=lambda a: a.valid_from
    )
    assert [(a.alias_value, a.valid_from, a.valid_to) for a in isin_aliases] == [
        ("US0378331005", date(2026, 3, 31), date(2026, 6, 30)),
        ("US0378331099", date(2026, 6, 30), None),
    ]
    # The identity cusip9 alias remains a single open window across both dates.
    cusip = [a for a in sec.aliases if a.alias_kind == "cusip9"][0]
    assert (cusip.valid_from, cusip.valid_to) == (date(2026, 3, 31), None)


def test_cross_identity_isin_collision_is_ambiguous_and_withheld() -> None:
    result = resolve_securities(
        [
            _obs("o1", date(2026, 6, 30), cusip9_input="037833100", isin_input="GB0000000001"),
            _obs("o2", date(2026, 6, 30), cusip9_input="459200101", isin_input="GB0000000001"),
        ]
    )
    assert len(result.securities) == 2
    for security in result.securities:
        assert security.identity_state == "ambiguous"
        assert security.identity_reason_code == "cross_identity_alias_collision"
        assert [alias.alias_kind for alias in security.aliases] == ["cusip9"]
        assert security.identity_evidence["conflicts"]["cross_identity_alias"] == [
            "isin:GB0000000001"
        ]


def test_cross_identity_isin_reassignment_keeps_non_overlapping_windows() -> None:
    result = resolve_securities(
        [
            _obs("o1", date(2026, 3, 31), cusip9_input="037833100", isin_input="GB0000000001"),
            _obs("o2", date(2026, 6, 30), cusip9_input="037833100", isin_input="GB0000000002"),
            _obs("o3", date(2026, 6, 30), cusip9_input="459200101", isin_input="GB0000000001"),
        ]
    )
    assert len(result.securities) == 2
    assert all(security.identity_reason_code is None for security in result.securities)
    first = next(security for security in result.securities if security.identity_key == "cusip9:037833100")
    second = next(security for security in result.securities if security.identity_key == "cusip9:459200101")
    assert [(alias.alias_value, alias.valid_from, alias.valid_to) for alias in first.aliases if alias.alias_kind == "isin"] == [
        ("GB0000000001", date(2026, 3, 31), date(2026, 6, 30)),
        ("GB0000000002", date(2026, 6, 30), None),
    ]
    assert [(alias.alias_value, alias.valid_from, alias.valid_to) for alias in second.aliases if alias.alias_kind == "isin"] == [
        ("GB0000000001", date(2026, 6, 30), None)
    ]


def test_terms_use_latest_observation_and_absent_terms_are_not_fabricated() -> None:
    result = resolve_securities(
        [
            _obs(
                "o1",
                date(2026, 3, 31),
                cusip9_input="037833100",
                coupon_type="fixed",
                coupon_rate="4.50",
                maturity_date=date(2030, 1, 1),
            ),
            _obs(
                "o2",
                date(2026, 6, 30),
                cusip9_input="037833100",
                coupon_type="fixed",
                coupon_rate="4.75",
                maturity_date=date(2030, 1, 1),
            ),
        ]
    )
    sec = result.securities[0]
    # Latest observation wins for the published summary term.
    assert sec.measured_terms["coupon_rate"] == "4.75"
    assert sec.measured_terms["coupon_type"] == "fixed"
    # Terms never observed are declared not_reported / null, never invented.
    assert sec.measured_terms["is_144a"] == "not_reported"
    assert sec.measured_terms["seniority"] is None
    assert sec.measured_terms["day_count"] is None
