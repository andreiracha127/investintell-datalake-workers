"""Structural contract for the v04 PIT evidence relations.

The worker publishes a deliberately non-sensitive status surface.  Numeric facts
and point-in-time lineage stay in the private append-only relation.
"""

from __future__ import annotations

from pathlib import Path
import re
import subprocess


SCHEMA = Path(__file__).resolve().parents[1] / "schemas" / "open_macro_v04_pit_evidence.sql"

PRIVATE_COLUMNS = {
    "decision_month",
    "decision_as_of",
    "decision_created_at",
    "decision_input_digest_sha256",
    "decision_basis",
    "series_key",
    "value",
    "unit",
    "observation_period",
    "release_at",
    "ingested_at",
    "vintage",
    "source",
    "source_health",
    "fingerprint",
    "cutoff_at",
    "carry_seed_decision_month",
    "carry_seed_fingerprint",
    "materialized_at",
}

HEADER_COLUMNS = {"decision_month", "publication_status", "coverage_state"}

ITEM_COLUMNS = {
    "decision_month",
    "group_key",
    "group_label",
    "group_role",
    "series_key",
    "series_label",
    "role",
    "display_state",
    "availability_state",
    "evidence_state",
    "freshness_state",
    "pit_state",
}

CATEGORICAL_COLUMNS = {
    "decision_month",
    "taxonomy_state",
    "fiscal_state",
    "fiscal_boundary",
    "guard_level",
    "guard_coverage",
    "quadrant",
    "cycle_direction",
    "decision_validity",
    "decision_basis",
    "quadrant_source",
    "book",
}


def _table_body(sql: str, relation: str) -> str:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS {relation} \((.*?)\n\);",
        sql,
        flags=re.DOTALL,
    )
    assert match, f"missing {relation}"
    return match.group(1)


def _declared_columns(body: str) -> set[str]:
    return {
        match.group(1)
        for match in re.finditer(r"^    ([a-z0-9_]+)\s+", body, flags=re.MULTILINE)
        if not match.group(1).startswith("CONSTRAINT")
    }


def test_railway_upload_includes_the_certified_runtime_pack() -> None:
    required = (
        "fixtures/p1_packs/open_macro_v03_certified_input_pack_003/"
        "data/canonical/macro_observation_vintage.json"
    )
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", required],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1, (
        "Railway respects .gitignore when building its upload context; the certified "
        f"runtime input must not be ignored: {result.stdout.strip()}"
    )


def test_dedicated_railway_config_parks_the_evidence_worker() -> None:
    config_path = (
        Path(__file__).resolve().parents[1]
        / "railway.open-macro-v04-pit-evidence.toml"
    )
    config = config_path.read_text(encoding="utf-8")

    assert 'startCommand = "python -m src.run_worker"' in config
    assert 'restartPolicyType = "never"' in config
    assert 'cronSchedule = "0 0 29 2 *"' in config


def test_schema_separates_private_lineage_from_public_status_and_taxonomy_relations() -> None:
    assert SCHEMA.is_file(), "the PIT evidence schema must be installed with the worker"
    sql = SCHEMA.read_text(encoding="utf-8")

    assert _declared_columns(_table_body(sql, "open_macro_v04_pit_evidence")) == PRIVATE_COLUMNS
    assert _declared_columns(_table_body(sql, "open_macro_v04_evidence_snapshots")) == HEADER_COLUMNS
    assert _declared_columns(_table_body(sql, "open_macro_v04_evidence_items")) == ITEM_COLUMNS
    assert _declared_columns(_table_body(sql, "open_macro_v04_categorical_taxonomy")) == CATEGORICAL_COLUMNS


def test_public_relations_cannot_expose_private_values_or_point_in_time_lineage() -> None:
    assert SCHEMA.is_file(), "the PIT evidence schema must be installed with the worker"
    sql = SCHEMA.read_text(encoding="utf-8")

    public_sql = "\n".join(
        (
            _table_body(sql, "open_macro_v04_evidence_snapshots"),
            _table_body(sql, "open_macro_v04_evidence_items"),
            _table_body(sql, "open_macro_v04_categorical_taxonomy"),
        )
    )
    for forbidden in (
        "value",
        "unit",
        "observation_period",
        "release_at",
        "ingested_at",
        "vintage",
        "fingerprint",
        "cutoff_at",
        "carry_seed",
        "materialized_at",
        "created_at",
        "run_id",
        "digest",
        "hash",
        "timestamp",
        "updated_at",
    ):
        assert forbidden not in public_sql


def test_schema_makes_lineage_append_only_and_finalizes_exactly_thirteen_fixed_items() -> None:
    assert SCHEMA.is_file(), "the PIT evidence schema must be installed with the worker"
    sql = SCHEMA.read_text(encoding="utf-8")

    assert "open_macro_v04_pit_evidence_reject_mutation" in sql
    assert "BEFORE UPDATE OR DELETE ON open_macro_v04_pit_evidence" in sql
    assert "open_macro_v04_evidence_items_reject_mutation" in sql
    assert "BEFORE UPDATE OR DELETE ON open_macro_v04_evidence_items" in sql
    assert "open_macro_v04_evidence_snapshots_reject_mutation" in sql
    assert "open_macro_v04_categorical_taxonomy_reject_mutation" in sql
    assert "BEFORE UPDATE OR DELETE ON open_macro_v04_categorical_taxonomy" in sql
    assert "open_macro_v04_evidence_snapshots_insert_guard" in sql
    assert "BEFORE UPDATE OR DELETE ON open_macro_v04_evidence_snapshots" in sql
    assert (
        "FOR EACH ROW EXECUTE FUNCTION open_macro_v04_evidence_snapshots_reject_mutation()"
        in sql
    )
    assert "RAISE EXCEPTION 'published evidence snapshots are append-only'" in sql
    assert "header_status" not in sql
    assert "item_count <> 13" in sql
    assert "taxonomy_count <> 1" in sql
    assert "derived_coverage" in sql
    assert "NEW.coverage_state <> derived_coverage" in sql
    assert "PRIMARY KEY (decision_month, series_key)" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    for key in (
        "INDPRO", "PCEC96", "PAYEMS", "ACOGNO", "CPILFESL", "PPIFIS", "AHETPI",
        "MICH", "SPY", "MTSDS133FMS", "GDP", "SUBLPDCILSLGNQ", "M2SL",
    ):
        assert f"'{key}'" in sql
    for label in (
        "Industrial Production",
        "Real Personal Consumption Expenditures",
        "Total Nonfarm Payrolls",
        "Manufacturers’ New Orders for Consumer Goods",
        "Core Consumer Price Index",
        "Producer Price Index: Final Demand Intermediate Services",
        "Average Hourly Earnings",
        "University of Michigan Inflation Expectations",
        "Cycle Market Leg",
        "Federal Surplus or Deficit",
        "Nominal GDP",
        "Bank Lending Standards",
        "M2 Money Stock",
    ):
        assert f"'{label}'" in sql


def test_public_status_vocabularies_match_the_light_evidence_contract() -> None:
    sql = SCHEMA.read_text(encoding="utf-8")
    public_sql = "\n".join(
        (
            _table_body(sql, "open_macro_v04_evidence_snapshots"),
            _table_body(sql, "open_macro_v04_evidence_items"),
        )
    )

    for vocabulary in (
        "publication_status = 'open'",
        "display_state IN ('ready', 'limited', 'unavailable')",
        "availability_state IN ('available', 'not_available', 'unknown')",
        "evidence_state IN ('observed', 'carried', 'missing', 'invalid')",
        "freshness_state IN ('current', 'stale', 'unknown')",
        "pit_state IN ('verified', 'unverified', 'unavailable')",
    ):
        assert vocabulary in public_sql
    assert "building" not in public_sql


def test_schema_limits_public_evidence_to_runtime_and_private_lineage_to_worker_writer() -> None:
    assert SCHEMA.is_file(), "the PIT evidence schema must be installed with the worker"
    sql = SCHEMA.read_text(encoding="utf-8")

    for relation in (
        "open_macro_v04_pit_evidence",
        "open_macro_v04_evidence_snapshots",
        "open_macro_v04_evidence_items",
        "open_macro_v04_categorical_taxonomy",
    ):
        assert f"REVOKE ALL ON TABLE {relation} FROM PUBLIC" in sql
    for role in ("worker_writer", "app_runtime"):
        assert f"rolname = '{role}'" in sql
    assert "rolname = 'app_analytics_ro'" in sql
    assert "GRANT SELECT, INSERT" in sql
    assert "GRANT SELECT ON TABLE open_macro_v04_evidence_snapshots" in sql
    assert "GRANT SELECT ON TABLE open_macro_v04_evidence_items" in sql
    assert "GRANT SELECT ON TABLE open_macro_v04_categorical_taxonomy" in sql
    assert "GRANT SELECT ON TABLE open_macro_v04_pit_evidence TO app_runtime" not in sql
    assert "GRANT SELECT, INSERT, UPDATE ON TABLE open_macro_v04_evidence_snapshots" not in sql
    for relation in (
        "open_macro_v04_pit_evidence",
        "open_macro_v04_evidence_snapshots",
        "open_macro_v04_evidence_items",
        "open_macro_v04_categorical_taxonomy",
        "open_macro_v04_decisions",
        "open_macro_v04_allocations",
    ):
        assert f"REVOKE ALL ON TABLE {relation} FROM app_analytics_ro" in sql
    for relation in ("open_macro_v04_decisions", "open_macro_v04_allocations"):
        assert f"REVOKE ALL ON TABLE {relation} FROM app_runtime" in sql


def test_admin_bootstrap_transfers_only_worker_owned_evidence_objects_to_worker_writer() -> None:
    sql = SCHEMA.read_text(encoding="utf-8")

    for relation in (
        "open_macro_v04_pit_evidence",
        "open_macro_v04_evidence_snapshots",
        "open_macro_v04_evidence_items",
        "open_macro_v04_categorical_taxonomy",
    ):
        assert f"ALTER TABLE {relation} OWNER TO worker_writer" in sql
    for function in (
        "open_macro_v04_pit_evidence_reject_mutation",
        "open_macro_v04_evidence_snapshots_insert_guard",
        "open_macro_v04_evidence_items_insert_guard",
        "open_macro_v04_evidence_items_reject_mutation",
        "open_macro_v04_evidence_snapshots_reject_mutation",
        "open_macro_v04_categorical_taxonomy_reject_mutation",
    ):
        assert f"ALTER FUNCTION {function}() OWNER TO worker_writer" in sql
    assert "ALTER TABLE open_macro_v04_decisions OWNER TO worker_writer" not in sql
    assert "ALTER TABLE open_macro_v04_allocations OWNER TO worker_writer" not in sql


def test_producer_relation_acl_changes_are_guarded_for_worker_writer_reapply() -> None:
    sql = SCHEMA.read_text(encoding="utf-8")

    assert "can_manage_producer_acl" in sql
    assert "rolsuper" in sql
    assert "has_table_privilege(application_role.oid, producer_relation.name, 'SELECT')" in sql
    assert "has_any_column_privilege(" in sql
    assert "open_macro_v04 producer ACLs are unsafe; owner bootstrap required" in sql
    assert "ERRCODE = '42501'" in sql
    assert "\nREVOKE ALL ON TABLE open_macro_v04_decisions FROM PUBLIC;" not in sql
    assert "\nREVOKE ALL ON TABLE open_macro_v04_allocations FROM PUBLIC;" not in sql
    for statement in (
        "REVOKE ALL ON TABLE open_macro_v04_decisions FROM PUBLIC",
        "REVOKE ALL ON TABLE open_macro_v04_allocations FROM PUBLIC",
        "REVOKE ALL ON TABLE open_macro_v04_decisions FROM app_runtime",
        "REVOKE ALL ON TABLE open_macro_v04_allocations FROM app_runtime",
        "REVOKE ALL ON TABLE open_macro_v04_decisions FROM app_analytics_ro",
        "REVOKE ALL ON TABLE open_macro_v04_allocations FROM app_analytics_ro",
        "GRANT SELECT ON TABLE open_macro_v04_decisions TO worker_writer",
        "GRANT SELECT ON TABLE open_macro_v04_allocations TO worker_writer",
    ):
        assert f"EXECUTE '{statement}'" in sql
