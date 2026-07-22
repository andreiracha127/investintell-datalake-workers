"""Static contract checks for the bond_rating_history_v1 DDL and worker wiring.

The license gate must be present in the DDL: a mandatory per-observation
licensed_source_ref, a product-level license_verified flag, and a product-state
that goes not_applicable / no_licensed_source when the license is not verified.
Agency codes must stay OPAQUE (an internal code + an internal mapping table) —
NEVER an agency name in a serving-bound field.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_ddl_declares_license_gate_and_opaque_agency() -> None:
    ddl = (ROOT / "schemas" / "bond_rating_history_v1.sql").read_text(encoding="utf-8")
    for token in (
        "bond_rating_observation",
        "bond_rating_observation is immutable",
        # License gate — per-observation mandatory license reference.
        "licensed_source_ref text NOT NULL",
        # Product-level state + license flag.
        "product_state text NOT NULL CHECK (product_state IN ('active', 'not_applicable'))",
        "license_verified boolean NOT NULL",
        "no_licensed_source",
        # active iff license verified; reason present iff NOT active.
        "(product_state = 'active') = (license_verified)",
        "(reason_code IS NULL) = (product_state = 'active')",
        # Snapshot rows exist ONLY under a verified license.
        "license_verified boolean NOT NULL CHECK (license_verified)",
        # Opaque agency: an internal code + an internal mapping table.
        "bond_rating_agency_map",
        "agency_code text NOT NULL",
        # Half-open PIT windows (Task-3 convention).
        "valid_to IS NULL OR valid_to > valid_from",
        # Guarded snapshot + build under the shared protocol.
        "bond_rating_history_v1 build requires a prepared bond_rating_history_v1 publication",
        "bond_rating_history_v1 snapshot requires a prepared bond_rating_history_v1 publication",
        # DB-level cross-table license enforcement (row cannot coexist with unlicensed build).
        "bond_rating_history_v1 snapshot requires a license-verified build",
        "sec_derived_current_pointers",
    ):
        assert token in ddl, token


def test_agency_map_is_internal_only() -> None:
    ddl = (ROOT / "schemas" / "bond_rating_history_v1.sql").read_text(encoding="utf-8")
    # The opaque->label mapping is revoked from PUBLIC (datalake-internal only).
    assert "REVOKE ALL ON bond_rating_agency_map FROM PUBLIC" in ddl


def test_worker_uses_the_dedicated_advisory_lock_and_protocol() -> None:
    worker = (ROOT / "src" / "workers" / "bond_rating_history.py").read_text(encoding="utf-8")
    assert "LOCK_BOND_RATING_HISTORY" in worker
    assert "advisory_lock" in worker
    db = (ROOT / "src" / "db.py").read_text(encoding="utf-8")
    assert "LOCK_BOND_RATING_HISTORY = 900_346" in db
    materializer = (ROOT / "src" / "bonds" / "ratings.py").read_text(encoding="utf-8")
    assert "sec_validate_derived_publication" in materializer
    assert "sec_set_current_derived_publication" in materializer
