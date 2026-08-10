from pathlib import Path

from src.bonds.panel_config import config_hash


def test_panel_ddl_has_six_worker_owned_relations_and_fail_closed_controls() -> None:
    sql = Path("schemas/bond_panel_v1.sql").read_text(encoding="utf-8")
    assert "BEGIN;" not in sql
    assert "COMMIT;" not in sql
    for relation in ("bond_panel_publications", "bond_panel_app_pointer", "bond_panel_snapshot", "bond_panel_returns", "bond_panel_rv_signal", "bond_panel_rating_pit"):
        assert f"CREATE TABLE IF NOT EXISTS {relation}" in sql
        assert f"ALTER TABLE {relation} OWNER TO worker_writer" in sql
        assert f"REVOKE ALL ON TABLE {relation} FROM PUBLIC" in sql
    assert "UNIQUE (product)" in sql
    assert "publication_status = 'validated'" in sql
    assert "immutable" in sql.lower()
    assert "bond_panel_current_snapshot_v1" in sql
    assert "WITH RECURSIVE ancestry" in sql
    assert "failure_reason" in sql


def test_panel_ddl_is_rerunnable_and_enforces_lifecycle_and_ancestry() -> None:
    sql = Path("schemas/bond_panel_v1.sql").read_text(encoding="utf-8")
    assert "DROP TRIGGER IF EXISTS" in sql
    assert "bond_panel_assert_publication_transition" in sql
    assert "bond_panel_assert_parent" in sql
    assert "bond_panel_assert_pointer_validated" in sql
    assert "ALTER VIEW bond_panel_current_snapshot_v1 OWNER TO worker_writer" in sql
    rating_block = sql.split("CREATE TABLE IF NOT EXISTS bond_panel_rating_pit", 1)[1].split("CREATE OR REPLACE FUNCTION", 1)[0]
    assert "agency" not in rating_block.lower()
    assert "rating_reason" in rating_block


def test_panel_ddl_keeps_retired_publications_stored_but_never_serves_or_activates_them() -> None:
    sql = Path("schemas/bond_panel_v1.sql").read_text(encoding="utf-8")
    active_hash = config_hash()
    legacy_hash = "0c0d78a866bc1090"
    retired_reg_s_hash = "180a82b3f1413d43"

    assert active_hash == "1863d3d5fa3a0edf"
    stored_hashes = f"'{legacy_hash}', '{retired_reg_s_hash}', '{active_hash}'"
    serving_hashes = f"'{legacy_hash}', '{active_hash}'"
    assert f"CHECK (config_hash IN ({stored_hashes}))" in sql
    assert "DROP CONSTRAINT IF EXISTS bond_panel_publications_config_hash_check" in sql
    assert (
        "ADD CONSTRAINT bond_panel_publications_config_hash_check "
        f"CHECK (config_hash IN ({stored_hashes})) NOT VALID"
    ) in sql
    assert sql.count(f"p.config_hash IN ({serving_hashes})") == 4
    assert f"p.config_hash IN ({stored_hashes})" not in sql
    assert sql.count("p.config_hash = a.config_hash") == 4
    assert "pointer rejects retired Reg S-only activation target" in sql
    assert "schema install refuses an active retired Reg S-only pointer" in sql


def test_panel_ddl_requires_an_authorized_dual_series_child_of_the_legacy_pointer() -> None:
    sql = Path("schemas/bond_panel_v1.sql").read_text(encoding="utf-8")

    assert "candidate.config_hash <> prior.config_hash" in sql
    assert "candidate.parent_publication_id = OLD.publication_id" in sql
    assert "rule_144a_to_dual_series_delta_v1" in sql
    assert "authorized_code_revision', candidate.code_revision" in sql
    assert "candidate.source_lineage->>'distribution_rule' = 'rule_144a_and_reg_s'" in sql
    assert "f.distribution_rule IN ('rule_144a', 'reg_s')" in sql
    assert "f.distribution_rule = 'rule_144a' AND f.cusip_id = f.reference_cusip9" in sql
    assert "identity_rows.distribution_rule = 'reg_s'" in sql
    assert "nullif(f.distribution_decision_id, '') IS NOT NULL" in sql
    assert sql.count("AND f.distribution_rule = 'reg_s'") == 1
    assert sql.count("IS DISTINCT FROM") >= 3
    assert "identity_rows.distribution_rule = 'rule_144a'" in sql
    assert "identity_rows.cusip_id <> identity_rows.reference_cusip9" in sql
    assert "identity_rows.distribution_decision_id IS NOT NULL" in sql
    assert "candidate.first_month = prior.first_month" in sql
    assert "candidate.last_closed_month = COALESCE(prior.open_month, prior.last_closed_month + INTERVAL '1 month')::date" in sql
    assert "candidate.open_month = (candidate.last_closed_month + INTERVAL '1 month')::date" in sql
    assert "SELECT f.month, f.cusip_id FROM bond_panel_rv_signal f" in sql
    assert "AND f.eligibility_state = 'included'" in sql
    assert "SELECT f.month, f.cusip_id FROM bond_panel_returns f" in sql
    assert "pointer config transition requires an authorized dual-series delta child" in sql
    assert "pointer rejects config or month regression" in sql
    assert "pointer candidate must directly extend the current publication" in sql
    assert "initial dual pointer requires a fresh database without legacy history" in sql
    assert sql.count("p.config_hash AS config_hash") == 4
    assert "a.depth = 0" not in sql
    assert "pointer requires valid dual-series fact identity" in sql
    assert "pointer requires matching dual-series surface identity" in sql
    assert "btrim(a.config_hash::text) = '1863d3d5fa3a0edf'" in sql


def test_panel_ddl_adds_nullable_distribution_identity_to_every_fact_table() -> None:
    sql = Path("schemas/bond_panel_v1.sql").read_text(encoding="utf-8")

    for table in (
        "bond_panel_snapshot",
        "bond_panel_rv_signal",
        "bond_panel_returns",
        "bond_panel_rating_pit",
    ):
        assert f"ALTER TABLE {table}" in sql
        assert "ADD COLUMN IF NOT EXISTS distribution_rule" in sql
        assert "ADD COLUMN IF NOT EXISTS reference_cusip9" in sql
        assert "ADD COLUMN IF NOT EXISTS distribution_decision_id" in sql
    assert "CHECK (distribution_rule IS NULL OR distribution_rule IN ('rule_144a', 'reg_s'))" in sql
