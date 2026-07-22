"""Pure contract tests for mixed_quant_v1 identity, alias history and income.

No database: these exercise the deterministic identity/alias/income rules that
the publication worker relies on.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from src.quant_data import contracts


def _obs(**kw):
    base = dict(
        observation_id=uuid4(),
        instrument_type="bond",
        currency="USD",
        alias_type="cusip",
        alias_value="037833100",
        valid_from=date(2024, 1, 1),
        valid_to=None,
        observed_at=datetime(2024, 2, 1, tzinfo=timezone.utc),
        source_lineage={"source": "nport", "accession": "A1"},
        issuer_id=None,
        security_id=None,
        deterministic_key=None,
    )
    base.update(kw)
    return contracts.IdentityObservation(**base)


def test_mint_instrument_id_is_stable_and_deterministic() -> None:
    assert contracts.mint_instrument_id("issuer=APPLE|security=US0378331005") == \
        contracts.mint_instrument_id("issuer=APPLE|security=US0378331005")
    assert contracts.mint_instrument_id("k1") != contracts.mint_instrument_id("k2")


def test_deterministic_evidence_merges_into_one_identity() -> None:
    key = "security=US0378331005"
    a = _obs(alias_type="cusip", alias_value="037833100", deterministic_key=key)
    b = _obs(alias_type="isin", alias_value="US0378331005", deterministic_key=key)
    resolved = contracts.resolve_identities([a, b])
    assert len(resolved) == 1
    inst = resolved[0]
    assert inst.unresolved is False
    assert inst.instrument_id == contracts.mint_instrument_id(key)
    assert {(x.alias_type, x.alias_value) for x in inst.aliases} == {
        ("cusip", "037833100"),
        ("isin", "US0378331005"),
    }


def test_collision_preserved_as_separate_unresolved_records() -> None:
    # Same CUSIP observed with no deterministic evidence tying them together.
    a = _obs(alias_value="037833100", security_id="SEC-A", deterministic_key=None)
    b = _obs(alias_value="037833100", security_id="SEC-B", deterministic_key=None)
    resolved = contracts.resolve_identities([a, b])
    assert len(resolved) == 2
    assert all(inst.unresolved for inst in resolved)
    assert resolved[0].instrument_id != resolved[1].instrument_id


def test_alias_history_closes_prior_interval_on_change() -> None:
    key = "security=SER1"
    old = _obs(alias_type="ticker", alias_value="OLD", valid_from=date(2020, 1, 1),
               deterministic_key=key)
    new = _obs(alias_type="ticker", alias_value="NEW", valid_from=date(2022, 6, 1),
               deterministic_key=key)
    inst = contracts.resolve_identities([old, new])[0]
    by_value = {a.alias_value: a for a in inst.aliases if a.alias_type == "ticker"}
    assert by_value["OLD"].valid_to == date(2022, 6, 1)
    assert by_value["NEW"].valid_to is None
    assert by_value["OLD"].valid_from == date(2020, 1, 1)


def test_income_event_rejects_inferred_yield_metrics() -> None:
    good = {
        "event_date": date(2024, 3, 1),
        "cash_amount": "1.25",
        "currency": "USD",
        "event_type": "coupon",
        "source_lineage": {"source": "pilot"},
    }
    contracts.validate_income_event(good)  # does not raise
    for bad_key in ("ytm", "ytw", "oas", "z_spread", "yield", "price"):
        with pytest.raises(contracts.ContractError):
            contracts.validate_income_event({**good, bad_key: 0.05})


def test_income_event_requires_observed_fields_and_lineage() -> None:
    with pytest.raises(contracts.ContractError):
        contracts.validate_income_event({
            "event_date": date(2024, 3, 1), "currency": "USD",
            "event_type": "coupon", "source_lineage": {"s": 1},
        })  # missing cash_amount
    with pytest.raises(contracts.ContractError):
        contracts.validate_income_event({
            "event_date": date(2024, 3, 1), "cash_amount": "1", "currency": "USD",
            "event_type": "coupon", "source_lineage": {},
        })  # empty lineage


def test_require_lineage_rejects_empty() -> None:
    assert contracts.require_lineage({"source": "x"}) == {"source": "x"}
    with pytest.raises(contracts.ContractError):
        contracts.require_lineage({})
    with pytest.raises(contracts.ContractError):
        contracts.require_lineage(None)
