from pathlib import Path


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
