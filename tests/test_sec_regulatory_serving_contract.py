"""Workers half of the ``sec_regulatory_serving_v1`` digest handshake.

Fails if the declared serving surface drifts from the frozen ``SURFACE_DIGEST``
or from the app repo's mirrored constant. Pure, no DB.
"""

from __future__ import annotations

from src.sec_serving import contract


def test_workers_declare_serving_product() -> None:
    assert contract.SERVING_PRODUCT == "sec_regulatory_serving_v1"
    assert contract.CONTRACT_VERSION == "sec_regulatory_serving_v1"


def test_frozen_digest_matches_declared_surface() -> None:
    assert contract.compute_surface_digest() == contract.SURFACE_DIGEST


def test_contract_verifies_clean() -> None:
    verdict = contract.verify_contract()
    assert verdict["ok"] is True, verdict["mismatches"]


def test_all_sixteen_families_declared() -> None:
    names = contract.family_names()
    assert len(names) == 16
    assert len(set(names)) == 16
    # 8 N-CEN + 8 RR1 (fee, shareholder_cost, waiver, dispersion, turnover,
    # reported_performance, benchmark, crosswalk).
    assert sum(1 for n in names if n.startswith("ncen_")) == 8
    assert sum(1 for n in names if n.startswith("rr1_")) == 8


def test_no_payload_key_is_an_internal_identifier() -> None:
    for family in contract.FAMILIES:
        assert not (set(family["payload_keys"]) & set(contract.SCRUB_BLOCKLIST)), family["family"]


def test_digest_moves_when_surface_changes(monkeypatch) -> None:
    baseline = contract.compute_surface_digest()
    monkeypatch.setattr(contract, "SERVING_FACT_COLUMNS", contract.SERVING_FACT_COLUMNS + ("smuggled",))
    assert contract.compute_surface_digest() != baseline
