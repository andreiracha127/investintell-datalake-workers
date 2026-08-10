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


def test_panel_ddl_matches_the_active_reg_s_configuration_in_publication_and_views() -> None:
    sql = Path("schemas/bond_panel_v1.sql").read_text(encoding="utf-8")
    active_hash = config_hash()
    legacy_hash = "0c0d78a866bc1090"

    assert active_hash == "180a82b3f1413d43"
    assert f"CHECK (config_hash IN ('{legacy_hash}', '{active_hash}'))" in sql
    assert "DROP CONSTRAINT IF EXISTS bond_panel_publications_config_hash_check" in sql
    assert (
        "ADD CONSTRAINT bond_panel_publications_config_hash_check "
        f"CHECK (config_hash IN ('{legacy_hash}', '{active_hash}')) NOT VALID"
    ) in sql
    assert sql.count(f"p.config_hash IN ('{legacy_hash}', '{active_hash}')") == 4
    assert sql.count("p.config_hash = a.config_hash") == 4


def test_panel_ddl_keeps_legacy_reads_until_a_parentless_reg_s_base_replaces_them() -> None:
    sql = Path("schemas/bond_panel_v1.sql").read_text(encoding="utf-8")

    assert "candidate.config_hash <> prior.config_hash" in sql
    assert "candidate.parent_publication_id IS NULL" in sql
    assert "rule_144a_to_reg_s_base_v1" in sql
    assert "authorized_code_revision', candidate.code_revision" in sql
    assert "candidate.source_lineage->>'distribution_rule' = 'reg_s'" in sql
    assert "generate_series(candidate.first_month" in sql
    assert sql.count("SELECT min(f.month) FROM bond_panel_rv_signal") == 1
    assert sql.count("SELECT min(f.month) FROM bond_panel_returns") == 1
    assert "SELECT f.month, f.cusip_id FROM bond_panel_rv_signal f" in sql
    assert "AND f.eligibility_state = 'included'" in sql
    assert "SELECT f.month, f.cusip_id FROM bond_panel_returns f" in sql
    assert "pointer config transition requires an authorized complete Reg S replacement base" in sql
    assert "pointer rejects config or month regression" in sql
