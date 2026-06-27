"""Schema + lock id for the versioned QuadrantSnapshot table (freeze v1 §3/§7/§10)."""
from __future__ import annotations

import pathlib

from src import db

_SQL = (pathlib.Path(__file__).resolve().parents[1]
        / "schemas" / "regime_quadrant_snapshot.sql").read_text(encoding="utf-8")


def test_table_declares_all_snapshot_columns() -> None:
    assert "CREATE TABLE IF NOT EXISTS regime_quadrant_snapshot" in _SQL
    for col in (
        "snapshot_id", "previous_snapshot_id",
        "quadrant", "candidate_quadrant", "candidate_confidence",
        "growth_score", "growth_sign", "growth_internal_sign",
        "growth_candidate_confidence", "growth_margin",
        "growth_uncertainty_raw", "growth_uncertainty_adjusted",
        "inflation_score", "inflation_sign", "inflation_internal_sign",
        "inflation_candidate_confidence",
        "inflation_margin", "inflation_uncertainty_raw", "inflation_uncertainty_adjusted",
        "coverage_quality", "freshness_quality", "source_health_quality",
        "transition_pending", "transition_reason",
        "as_of", "available_at", "computed_at",
        "data_stale_after", "pipeline_stale_after", "stale_after",
        "status_at_compute", "model_version", "confidence_model_version",
        "confidence_method", "source_vintage_hash",
    ):
        assert col in _SQL, f"missing column {col}"


def test_pk_is_snapshot_id_and_unique_includes_previous() -> None:
    assert "PRIMARY KEY (snapshot_id)" in _SQL
    # owner decision: the UNIQUE includes previous_snapshot_id (latched identity),
    # de-duplicated with NULLS NOT DISTINCT so the genesis row (previous=NULL) collapses.
    # PG grammar requires NULLS NOT DISTINCT *before* the column list, so we match the
    # keyword and the column list independently (both must be present in the constraint).
    assert "UNIQUE NULLS NOT DISTINCT" in _SQL
    assert "(model_version, as_of, source_vintage_hash, previous_snapshot_id)" in _SQL


def test_coherence_checks_present() -> None:
    # §7: valid <=> quadrant+candidate filled & confidence>=0.70 & no pending;
    #     non-valid => quadrant NULL; unavailable/invalid => candidate_confidence NULL.
    assert "status_at_compute = 'valid'" in _SQL
    assert "candidate_confidence >= 0.70" in _SQL
    assert "transition_pending = FALSE" in _SQL
    assert "quadrant IS NULL" in _SQL
    assert "stale_after <= data_stale_after" in _SQL
    assert "stale_after <= pipeline_stale_after" in _SQL
    assert "computed_at >= available_at" in _SQL
    assert "as_of <= available_at" in _SQL
    # quality fields in [0,1]
    assert "coverage_quality BETWEEN 0 AND 1" in _SQL


def test_status_domain_check() -> None:
    for s in ("valid", "low_confidence", "unavailable", "invalid"):
        assert f"'{s}'" in _SQL


def test_operational_view_filters_valid_and_unexpired() -> None:
    assert "CREATE OR REPLACE VIEW regime_quadrant_current_v" in _SQL
    assert "status_at_compute = 'valid'" in _SQL
    assert "stale_after >" in _SQL  # current view excludes expired snapshots


def test_audit_table_has_lineage_columns() -> None:
    assert "CREATE TABLE IF NOT EXISTS regime_quadrant_indicator_audit" in _SQL
    for col in ("snapshot_id", "axis", "series_id", "z_score", "weight",
                "coverage", "freshness", "source_health", "anomaly",
                "observation_period", "vintage_id", "revision_number"):
        assert col in _SQL


def test_lock_id_registered_and_unique() -> None:
    assert db.LOCK_REGIME_QUADRANT == 900_208
    ids = [v for k, v in vars(db).items() if k.startswith("LOCK_") and isinstance(v, int)]
    assert ids.count(900_208) == 1
    assert db.LOCK_REGIME_QUADRANT != db.LOCK_REGIME_GATE
