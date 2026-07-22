"""Static contract checks for the bond_price_observation_v1 DDL and worker wiring."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_ddl_declares_immutable_observation_guarded_snapshot_and_lanes() -> None:
    ddl = (ROOT / "schemas" / "bond_price_observations_v1.sql").read_text(encoding="utf-8")
    for token in (
        "bond_price_observation",
        "bond_price_observation is immutable",
        "bond_price_observation_v1_builds",
        "input_fingerprint",
        "char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$')",
        "bond_price_observation_v1 build requires a prepared bond_price_observation_v1 publication",
        "bond_price_observation_v1 snapshot requires a prepared bond_price_observation_v1 publication",
        "matching pinned build metadata",
        # Typed price/accrued vocabularies.
        "price_type IN ('trade', 'evaluated', 'model', 'not_reported')",
        "accrued_treatment IN ('clean', 'dirty', 'not_reported')",
        # Ambiguity vocabulary (panel_states daily-key state).
        "duplicate_in_matching_cohort",
        # Identity invariants (unresolvable identity carries raw + state).
        "identity_state IN ('resolved', 'unresolved')",
        "(identity_state = 'resolved') = (security_id IS NOT NULL)",
        "(identity_state = 'resolved') = (identity_reason_code IS NULL)",
        # is_144a derived from db_type == 3.
        "is_144a",
    ):
        assert token in ddl, token


def test_ddl_lane_discriminator_is_typed_and_lanes_hardcode_their_literal() -> None:
    ddl = (ROOT / "schemas" / "bond_price_observations_v1.sql").read_text(encoding="utf-8")
    # A CHECK-backed domain types the lane column with a closed vocabulary.
    assert "CREATE DOMAIN bond_price_lane AS text CHECK (VALUE IN ('latest', 'fund_asof'))" in ddl
    # Each lane hardcodes its literal, so the fund_asof access path can NEVER emit a
    # latest-stamped row (structural refusal).
    assert "'latest'::bond_price_lane AS lane" in ddl
    assert "'fund_asof'::bond_price_lane AS lane" in ddl
    assert "CREATE OR REPLACE VIEW bond_price_latest_v1" in ddl
    assert "CREATE OR REPLACE FUNCTION bond_price_fund_asof_v1(fund_as_of date)" in ddl
    # Point-in-time, no look-ahead + STALE at age >= 31 days.
    assert "s.observation_date <= fund_as_of" in ddl
    assert "(fund_as_of - s.observation_date) >= 31) AS is_stale" in ddl
    # Lanes read only through the current pointer, never raw/observation rows.
    assert "sec_derived_current_pointers" in ddl


def test_worker_uses_the_dedicated_advisory_lock_and_protocol() -> None:
    worker = (ROOT / "src" / "workers" / "bond_price_observations.py").read_text(encoding="utf-8")
    assert "LOCK_BOND_PRICE_OBSERVATIONS" in worker
    assert "advisory_lock" in worker
    db = (ROOT / "src" / "db.py").read_text(encoding="utf-8")
    assert "LOCK_BOND_PRICE_OBSERVATIONS = 900_342" in db
    materializer = (ROOT / "src" / "bonds" / "price_observations.py").read_text(encoding="utf-8")
    assert "sec_validate_derived_publication" in materializer
    assert "sec_set_current_derived_publication" in materializer
