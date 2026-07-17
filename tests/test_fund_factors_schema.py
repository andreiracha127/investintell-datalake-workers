from pathlib import Path


SQL = (Path(__file__).parents[1] / "schemas" / "fund_factors.sql").read_text(
    encoding="utf-8"
)
MIGRATION = (
    Path(__file__).parents[1] / "schemas" / "fund_factors_fit_versioning.sql"
).read_text(encoding="utf-8")


def test_exposure_natural_key_preserves_fit_history() -> None:
    normalized = " ".join(SQL.split())
    assert (
        "UNIQUE NULLS NOT DISTINCT ( instrument_id, factor, as_of, "
        "organization_id, fit_id )"
    ) in normalized


def test_latest_mv_selects_only_eligible_production_fit() -> None:
    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS fund_factor_exposures_latest_mv" in SQL
    assert "fits.production_fit IS TRUE" in SQL
    assert "fits.converged IS TRUE" in SQL
    assert "fits.degraded IS FALSE" in SQL
    assert "fund_factor_exposures_latest_mv_pk" in SQL
    assert "GRANT MAINTAIN" in SQL


def test_versioned_migration_replaces_existing_mv_transactionally() -> None:
    assert MIGRATION.startswith("-- Transactional production migration")
    assert "BEGIN;" in MIGRATION
    assert "DROP MATERIALIZED VIEW IF EXISTS fund_factor_exposures_latest_mv" in MIGRATION
    assert "CREATE MATERIALIZED VIEW fund_factor_exposures_latest_mv AS" in MIGRATION
    assert "production_fit IS TRUE" in MIGRATION
    assert MIGRATION.rstrip().endswith("COMMIT;")
