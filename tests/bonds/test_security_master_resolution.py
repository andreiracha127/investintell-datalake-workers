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
        [_obs("o1", date(2026, 6, 30), cusip9_input="000000000", isin_input="US0378331005")]
    )
    assert result.rejected == ()
    sec = result.securities[0]
    assert sec.identity_key == "isin:US0378331005"
    # The rejected placeholder CUSIP is never emitted as an alias.
    assert all(a.alias_kind != "cusip9" for a in sec.aliases)
    isin_aliases = [a for a in sec.aliases if a.alias_kind == "isin"]
    assert isin_aliases[0].alias_value == "US0378331005"
    assert isin_aliases[0].valid_to is None


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


def test_conflicting_issuer_same_date_is_ambiguous() -> None:
    result = resolve_securities(
        [
            _obs("o1", date(2026, 6, 30), cusip9_input="037833100", issuer_name="Acme Corp"),
            _obs("o2", date(2026, 6, 30), cusip9_input="037833100", issuer_name="Beta Corp"),
        ]
    )
    sec = result.securities[0]
    assert sec.identity_state == "ambiguous"
    assert "issuer_name" in sec.identity_evidence["conflicts"]


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
