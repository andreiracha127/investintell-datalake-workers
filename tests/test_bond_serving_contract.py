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
# ``app.contracts.bond_serving_v1.SURFACE_DIGEST`` byte-for-byte.
SHARED_SURFACE_DIGEST = "sha256:ee64be3339843e73b2d93d4862796b3ac3a94f51e57fb8fb9472592b50771a35"


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


def test_catalog_carries_searchable_alias_arrays() -> None:
    catalog = next(s for s in contract.SURFACES if s["surface"] == "catalog")
    assert {"aliases_cusip9", "aliases_isin"} <= set(catalog["payload_keys"])


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


def test_digest_moves_when_surface_changes(monkeypatch) -> None:
    baseline = contract.compute_surface_digest()
    monkeypatch.setattr(
        contract, "SERVING_FACT_COLUMNS", contract.SERVING_FACT_COLUMNS + ("smuggled",)
    )
    assert contract.compute_surface_digest() != baseline
