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


def test_panel_ddl_has_a_narrow_evidence_bound_legacy_root_repair_exception() -> None:
    sql = Path("schemas/bond_panel_v1.sql").read_text(encoding="utf-8")

    exception_at = sql.index("legacy_parentless_return_coverage_repair_v1")
    direct_child_at = sql.index("pointer candidate must directly extend the current publication")
    assert exception_at < direct_child_at
    assert "92740098-1571-559d-9fb3-119de8321754" in sql
    assert "candidate.parent_publication_id IS NULL" in sql
    assert "prior.parent_publication_id IS NULL" in sql
    assert "min(f.month)" in sql
    assert "candidate.gate_evidence @> jsonb_build_object('base_repair'" in sql
    assert "candidate.snapshot_rows = 3417683" in sql
    assert "candidate.rv_signal_rows = 1687524" in sql
    assert "candidate.returns_rows = 2801208" in sql
    assert "candidate.ratings_pit_rows = 3417683" in sql
    assert "candidate.code_revision = 't3_historical_base_return_coverage_repair_v1'" in sql
    assert "candidate.input_fingerprint = '6e00313b5f2774dbd71e4c6f96f8c628e3a19015e9a1775b0dac986c5fdf1e7e'" in sql
    assert "'tail_digest', 'e6f2911143d01b1417973714a7d35f0040af90b0747917d326c5d055c29c9663'" in sql
    assert "prior.input_fingerprint = '5a7af9e1adaed315e9940293cf3e9e789ca6350993688d58ab3e759cee37a3cb'" in sql
    assert "prior.gate_evidence @> jsonb_build_object(" in sql
    assert "'input_fingerprint', '5a7af9e1adaed315e9940293cf3e9e789ca6350993688d58ab3e759cee37a3cb'" in sql
    assert "'from_artifact_fingerprint', 'e963304af08c1f513d048e1e7eee9fbe334fc3fe01b1c80f3cd5b7f8acb19581'" in sql
    assert "candidate.source_lineage->'source_sha256' = jsonb_build_object(" in sql
    assert "= DATE '2002-08-01'" in sql

    repair_start = sql.rindex("IF TG_OP = 'UPDATE'", 0, exception_at)
    repair_block = sql[repair_start:direct_child_at]
    assert "candidate.first_month = prior.first_month" not in repair_block
    assert "candidate.last_closed_month = prior.last_closed_month" not in repair_block
    assert "'from_input_fingerprint', prior.input_fingerprint" not in repair_block
    assert "prior.gate_evidence @> jsonb_build_object('input_fingerprint', prior.input_fingerprint)" not in repair_block
    assert "candidate.source_lineage->'source_sha256' = prior.source_lineage->'source_sha256'" not in repair_block
    assert "= (SELECT min(f.month) FROM bond_panel_returns f WHERE f.publication_id = prior.publication_id)" not in repair_block

    authorized_source_sha256 = {
        "bond_monthly_returns.parquet": "d0c8827437d6a49c4481ead71eac69097d00db11a19d91e2b58dc3d714ae8179",
        "bond_panel_live.parquet": "3e4d451faa05bcedefa086903325e93842a59e31368c7e12aaa5a4972214e210",
        "bond_ratings_pit.parquet": "97c645ce7d98ad945288369e20ed40abe2d7d1590b4953f7a983bc6e719efcb4",
        "rv_signal_live.parquet": "b6afc8bc44dd11563b794b2c11a9d13eb9a882af4d364a728e87a34258c90e6e",
        "universe_snapshots_live.parquet": "ab48d99f466ae3a943ce0a2819175ab6efdd95212b4efc9079151750057b077a",
    }
    for artifact, digest in authorized_source_sha256.items():
        assert repair_block.count(f"'{artifact}', '{digest}'") == 2


def test_panel_ddl_allows_an_omission_authorized_144a_only_bootstrap() -> None:
    sql = Path("schemas/bond_panel_v1.sql").read_text(encoding="utf-8")

    # The legacy-to-dual transition normally proves both legs in the snapshot.
    # An approved snapshot with no active Reg S resolution may instead publish
    # only the durable Rule 144A rows, but only when both windows declare zero
    # mappings and typed omission evidence covers every Rule 144A row.
    assert "candidate.source_lineage->>'distribution_mapping_count' = '0'" in sql
    assert "candidate.source_lineage->>'distribution_mapping_open_count' = '0'" in sql
    assert "candidate.source_lineage->>'distribution_mapping_closed_count' = '0'" in sql
    assert "distribution_mapping_omission:%" in sql
    assert "distribution_mapping_closed_omission:%" in sql
    assert sql.count("omission.value IS NULL OR omission.value !~ '^[1-9][0-9]*$'") == 2
    assert sql.count("omission.value::numeric") == 2
    assert "f.month = candidate.open_month" in sql
    assert "f.month = candidate.last_closed_month" in sql


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
