"""Pure resolver + structural lane-gate for bond price/trade observations.

DB-free: exercises ``resolve_price_observations`` (identity, duplicate ambiguity,
is_144a, price/accrued typing) and the point-in-time index builder / matcher
wiring, proving the informative ``latest`` lane is structurally refused — the
Python mirror of the matching library's ``invalid_observation_index`` refusal.
"""
from __future__ import annotations

from datetime import date
from uuid import uuid5

import pytest

from src.bonds.debt_mapping import DebtMapping
from src.bonds.errors import BondError
from src.bonds.matching import HoldingRecord
from src.bonds.price_observations import (
    PriceObservationInput,
    build_pit_index,
    match_fund_holdings_asof,
    resolve_price_observations,
)
from src.bonds.security_master import NAMESPACE_BOND_SECURITY
from src.bonds.states import MatchState


def _obs(**changes) -> PriceObservationInput:
    values = {
        "observation_id": changes.pop("observation_id", "obs-1"),
        "observation_date": date(2024, 1, 10),
        "cusip9_input": "037833100",
        "price": 99.5,
        "price_type": "evaluated",
        "accrued_treatment": "clean",
        "ytm": 4.2,
        "db_type": 1,
    }
    values.update(changes)
    return PriceObservationInput(**values)


def test_valid_cusip_resolves_to_task3_anchored_security_id() -> None:
    [resolved] = resolve_price_observations([_obs()])
    assert resolved.identity_state == "resolved"
    assert resolved.identity_reason_code is None
    assert resolved.normalized_cusip9 == "037833100"
    # security_id agrees with the Task 3 security master's CUSIP-anchored identity.
    assert resolved.security_id == uuid5(NAMESPACE_BOND_SECURITY, "cusip9:037833100")
    assert resolved.price_state == "present"
    assert resolved.price_type == "evaluated"
    assert resolved.accrued_treatment == "clean"


def test_unresolvable_identity_carries_normalized_identifier_and_state() -> None:
    [resolved] = resolve_price_observations([_obs(cusip9_input="000000000")])
    assert resolved.identity_state == "unresolved"
    assert resolved.security_id is None
    assert resolved.normalized_cusip9 is None
    assert resolved.identity_reason_code == "placeholder"
    assert resolved.cusip9_input == "000000000"  # raw identifier retained
    assert resolved.daily_key_state == "invalid_key"


def test_duplicate_security_date_from_distinct_rows_is_ambiguous_no_winner() -> None:
    resolved = resolve_price_observations(
        [
            _obs(observation_id="a", price=99.0),
            _obs(observation_id="b", price=101.0),  # same cusip + date, different price
        ]
    )
    assert {r.observation_id for r in resolved} == {"a", "b"}
    # Both retained; neither dropped; both flagged duplicate (no arbitrary winner).
    assert [r.daily_key_state for r in resolved] == [
        "duplicate_in_matching_cohort",
        "duplicate_in_matching_cohort",
    ]
    assert {r.price for r in resolved} == {99.0, 101.0}


def test_is_144a_derives_from_db_type_three_only() -> None:
    assert resolve_price_observations([_obs(db_type=3)])[0].is_144a is True
    assert resolve_price_observations([_obs(db_type=1)])[0].is_144a is False
    assert resolve_price_observations([_obs(db_type=None)])[0].is_144a is None
    assert resolve_price_observations([_obs(db_type=2.5)])[0].is_144a is None  # non-integral


def test_price_state_and_typed_fields() -> None:
    assert resolve_price_observations([_obs(price=None)])[0].price_state == "null"
    assert resolve_price_observations([_obs(price="abc")])[0].price_state == "invalid"
    # Absent typed fields default to not_reported; unknown values are rejected.
    bare = resolve_price_observations([_obs(price_type=None, accrued_treatment=None)])[0]
    assert bare.price_type == "not_reported"
    assert bare.accrued_treatment == "not_reported"
    with pytest.raises(BondError, match="invalid_price_type"):
        resolve_price_observations([_obs(price_type="mid")])
    with pytest.raises(BondError, match="invalid_accrued_treatment"):
        resolve_price_observations([_obs(accrued_treatment="gross")])


# --- Structural lane gate (matcher wiring) -------------------------------------
_PIT_ROW = {
    "normalized_cusip9": "123456789",
    "observation_date": "2024-01-15",
    "observation_date_state": "present",
    "source_row_number": 0,
    "pr": 100.0,
    "pr_state": "present",
    "ytm": None,
    "db_type": None,
    "db_type_state": "null",
    "daily_key_state": "unique_in_matching_cohort",
}


def _mapping() -> DebtMapping:
    return DebtMapping(rules=(("fixture_debt", "fixture_asset", "fixture_structure", "eligible_debt"),))


def _holding() -> HoldingRecord:
    return HoldingRecord(
        publication_id="pub", accession_number="acc", holding_id="h", source_run_id="run",
        report_date="2024-01-20", filing_date="2024-01-25", series_id="s", class_id="c",
        instrument_id="i", issuer_category="fixture_debt", asset_class="fixture_asset",
        instrument_structure="fixture_structure", original_cusip="123456789",
        signed_market_value=100.0, signed_pct_of_nav=10.0, currency="USD",
    )


def test_build_pit_index_accepts_unstamped_and_fund_asof_but_refuses_latest() -> None:
    with build_pit_index([dict(_PIT_ROW)], ["123456789"]) as index:
        assert index.is_universe_member("123456789")
    with build_pit_index([{**_PIT_ROW, "lane": "fund_asof"}], ["123456789"]) as index:
        assert index.is_universe_member("123456789")
    with pytest.raises(BondError, match="non_pit_lane_rows"):
        build_pit_index([{**_PIT_ROW, "lane": "latest"}], ["123456789"])


def test_matcher_wiring_only_accepts_pit_lane() -> None:
    """MANDATORY: the historical matcher wiring rejects latest-lane output."""
    matched = match_fund_holdings_asof(
        [_holding()], _mapping(), [dict(_PIT_ROW)], ["123456789"], "2024-01-01", "2024-01-31"
    )
    assert matched[0].state is MatchState.MATCHED
    with pytest.raises(BondError, match="non_pit_lane_rows"):
        match_fund_holdings_asof(
            [_holding()], _mapping(), [{**_PIT_ROW, "lane": "latest"}], ["123456789"],
            "2024-01-01", "2024-01-31",
        )
