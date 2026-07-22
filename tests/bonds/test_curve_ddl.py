"""Static contract checks for the bond_curve_v1 DDL and worker wiring."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_ddl_declares_immutable_observation_guarded_snapshot_and_typed_nodes() -> None:
    ddl = (ROOT / "schemas" / "bond_curve_v1.sql").read_text(encoding="utf-8")
    for token in (
        "bond_curve_observation",
        "bond_curve_observation is immutable",
        "bond_curve_v1_builds",
        "input_fingerprint",
        "char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$')",
        "bond_curve_v1 build requires a prepared bond_curve_v1 publication",
        "bond_curve_v1 snapshot requires a prepared bond_curve_v1 publication",
        "matching pinned build metadata",
        # Interpolation declared as a snapshot attribute (linear only for now).
        "interpolation IN ('linear')",
        "curve_type IN ('spot', 'par')",
        # Minimum-2-nodes + typed nodes (tenor > 0, finite rate).
        "node_count >= 2",
        "tenor_years > 0",
        "rate <> 'NaN'",
        # Read only through the current pointer.
        "sec_derived_current_pointers",
    ):
        assert token in ddl, token


def test_worker_uses_the_dedicated_advisory_lock_and_protocol() -> None:
    worker = (ROOT / "src" / "workers" / "bond_curves.py").read_text(encoding="utf-8")
    assert "LOCK_BOND_CURVE" in worker
    assert "advisory_lock" in worker
    db = (ROOT / "src" / "db.py").read_text(encoding="utf-8")
    assert "LOCK_BOND_CURVE = 900_345" in db
    materializer = (ROOT / "src" / "bonds" / "curves.py").read_text(encoding="utf-8")
    assert "sec_validate_derived_publication" in materializer
    assert "sec_set_current_derived_publication" in materializer
