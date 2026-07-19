import json
import os
from pathlib import Path
import time
from uuid import UUID, uuid4

import pytest


def test_connection_parameter_adapter_requires_psycopg3_get_parameters() -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, _connection_parameters

    class Info:
        def get_parameters(self) -> dict[str, str]:
            return {"host": "db", "sslmode": "verify-full"}

    assert _connection_parameters(type("Connection", (), {"info": Info()})()) == {"host": "db", "sslmode": "verify-full"}
    with pytest.raises(BackfillSafetyError, match="connection parameters"):
        _connection_parameters(type("Connection", (), {"info": object()})())


@pytest.mark.parametrize("keyword", ("sslpassword", "sslkey", "sslcert", "sslrootcert", "sslcrl", "servicefile"))
def test_status_secret_scan_rejects_quoted_and_unquoted_libpq_paths_under_benign_keys(keyword: str) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, _assert_no_secret

    for value in (f"{keyword}=C:/private/material.pem", f"host=db {keyword}='C:/private material.pem'"):
        with pytest.raises(BackfillSafetyError, match="credential material"):
            _assert_no_secret({"diagnostic_note": value})

    class Info:
        def get_parameters(self) -> dict[str, str]:
            return {"host": "db", "sslmode": "verify-full", keyword: "C:/private/material.pem"}

    with pytest.raises(BackfillSafetyError, match="credential material"):
        from src.sec_regulatory.historical_backfill import _connection_parameters
        _connection_parameters(type("Connection", (), {"info": Info()})())


def test_psycopg3_connection_info_adapter_uses_real_disposable_connection() -> None:
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ.get("SEC_P4_DISPOSABLE_DSN")
    if not dsn:
        pytest.skip("SEC_P4_DISPOSABLE_DSN not supplied")
    from src.sec_regulatory.historical_backfill import _connection_parameters

    with psycopg.connect(dsn, connect_timeout=3) as connection:
        parameters = _connection_parameters(connection)
    assert parameters["host"] == "127.0.0.1"
    assert not {"password", "passfile", "sslpassword", "sslkey", "sslcert", "sslrootcert", "sslcrl", "service"} & set(parameters)


def test_real_disposable_collector_executes_every_fixed_select_before_fail_closed() -> None:
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ.get("SEC_P4_DISPOSABLE_DSN")
    if not dsn:
        pytest.skip("SEC_P4_DISPOSABLE_DSN not supplied")
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, _collect_production_preflight

    class Info:
        def get_parameters(self) -> dict[str, str]:
            return {"host": "127.0.0.1", "sslmode": "verify-full"}

    class Connection:
        info = Info()

        def __init__(self, raw: object) -> None:
            self.raw = raw

        def cursor(self) -> object:
            return self.raw.cursor()  # type: ignore[no-any-return,union-attr]

    authorization = _production_authorization("a" * 64)
    authorization["target"] = {**authorization["target"], "host": "127.0.0.1"}
    with psycopg.connect(dsn, connect_timeout=3) as raw:
        with pytest.raises(BackfillSafetyError) as error:
            _collect_production_preflight(Connection(raw), authorization)
    assert "query failed" not in str(error.value)


def test_real_disposable_target_inspection_detects_truncate_on_user_relation() -> None:
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ.get("SEC_P4_DISPOSABLE_DSN")
    if not dsn:
        pytest.skip("SEC_P4_DISPOSABLE_DSN not supplied")
    from src.sec_regulatory.historical_backfill import _inspect_connected_target

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE public.runner_preflight_truncate_probe (id integer)")
        observed = _inspect_connected_target(connection)
        connection.rollback()
    assert "public.runner_preflight_truncate_probe" in observed["writable_tables"]
    assert "public.runner_preflight_truncate_probe" in observed["truncate_tables"]


def test_real_disposable_governed_commit_wrapper_and_recovery_zero_queries_execute() -> None:
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ.get("SEC_P4_DISPOSABLE_DSN")
    if not dsn:
        pytest.skip("SEC_P4_DISPOSABLE_DSN not supplied")
    from src.sec_regulatory import manifests
    from src.sec_regulatory.historical_backfill import _get_recovery_governed_evidence, _get_recovery_zero_proof

    run_id = uuid4()
    supervisor_run_id = uuid4()
    with psycopg.connect(dsn) as connection:
        manifests.install_schema(connection)
        run = manifests.create_or_resume_run(
            connection, source_family="nport", package_sha256="c" * 64, parser_version=f"runner-probe-{run_id}",
            source_quarter="2024Q1", package_relative_path=f"runner-probe-{run_id}", run_id=run_id,
        )
        recorded = manifests.record_commit_outcome(
            connection, run_id=run.run_id, supervisor_run_id=supervisor_run_id,
            authorization_fingerprint="a" * 64, package_sha256="c" * 64, outcome="committed",
        )
        assert recorded.outcome == "committed"
        absent_package_id = uuid4()
        absent_run_id = uuid4()
        assert _get_recovery_governed_evidence(connection, package_id=absent_package_id, run_id=absent_run_id, authorization_fingerprint="a" * 64) is None
        assert _get_recovery_zero_proof(connection, package_id=absent_package_id, run_id=absent_run_id) == {
            "package_count": 0, "run_count": 0, "run_transition_count": 0,
            "source_transition_count": 0, "validated_visibility_count": 0,
        }
        connection.rollback()


def test_real_disposable_exact_16_table_snapshot_executes_after_transactional_install() -> None:
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ.get("SEC_P4_DISPOSABLE_DSN")
    if not dsn:
        pytest.skip("SEC_P4_DISPOSABLE_DSN not supplied")
    from src.nport import storage as nport_storage
    from src.ncen import storage as ncen_storage
    from src.rr1 import storage as rr1_storage
    from src.sec_regulatory import manifests
    from src.sec_regulatory.historical_backfill import _snapshot_exact_write_counts

    with psycopg.connect(dsn) as connection:
        manifests.install_schema(connection)
        nport_storage.install_schema(connection)
        ncen_storage.install_schema(connection)
        rr1_storage.install_schema(connection)
        counts = _snapshot_exact_write_counts(connection)
        connection.rollback()
    assert len(counts) == 16 and all(value >= 0 for value in counts.values())


def test_real_pg18_monitoring_inheritance_allows_select_but_not_set_role() -> None:
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ.get("SEC_P4_DISPOSABLE_DSN")
    if not dsn:
        pytest.skip("SEC_P4_DISPOSABLE_DSN not supplied")
    role = f"runner_monitor_probe_{os.getpid()}"
    from src.sec_regulatory.historical_backfill import _collect_monitoring_privileges
    admin = psycopg.connect(dsn, autocommit=True)
    try:
        with admin.cursor() as cursor:
            cursor.execute(f"DROP ROLE IF EXISTS {role}")
            cursor.execute(f"CREATE ROLE {role} NOLOGIN INHERIT")
            cursor.execute(f"GRANT pg_monitor TO {role} WITH INHERIT TRUE, SET FALSE")
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SET SESSION AUTHORIZATION {role}")
                cursor.execute("SELECT count(*) >= 0 FROM pg_stat_activity")
                assert cursor.fetchone() == (True,)
                cursor.execute("SELECT pg_has_role(current_user, 'pg_monitor', 'SET'), pg_has_role(current_user, 'pg_monitor', 'USAGE')")
                assert cursor.fetchone() == (False, True)
                assert _collect_monitoring_privileges(connection)["monitoring_privileges"] == {
                    "pg_stat_activity": ["SELECT"], "pg_locks": ["SELECT"],
                    "pg_monitor": ["DIRECT_MEMBER_NO_SET"], "pg_read_all_stats": ["INHERITED_USAGE"],
                }
                with pytest.raises(psycopg.Error):
                    cursor.execute("SET ROLE pg_monitor")
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f"DROP ROLE IF EXISTS {role}")
        admin.close()


def test_real_relation_security_hash_changes_for_rls_policy_without_oid_change() -> None:
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ.get("SEC_P4_DISPOSABLE_DSN")
    if not dsn:
        pytest.skip("SEC_P4_DISPOSABLE_DSN not supplied")
    from src.sec_regulatory.historical_backfill import _collect_relation_security

    table = f"runner_rls_probe_{os.getpid()}"
    qualified = f"public.{table}"
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {qualified} (id integer)")
            cursor.execute("SELECT %s::regclass::oid", (qualified,))
            oid = cursor.fetchone()[0]
        before = _collect_relation_security(connection, [qualified])["relations"][0]
        with connection.cursor() as cursor:
            cursor.execute(f"ALTER TABLE {qualified} ENABLE ROW LEVEL SECURITY")
            cursor.execute(f"CREATE POLICY runner_policy ON {qualified} USING (id > 0)")
            cursor.execute("SELECT %s::regclass::oid", (qualified,))
            assert cursor.fetchone()[0] == oid
        after = _collect_relation_security(connection, [qualified])["relations"][0]
        connection.rollback()
    assert before.split("|definition_sha256=")[0] == after.split("|definition_sha256=")[0]
    assert before != after


def test_real_target_inspection_includes_updatable_view_and_foreign_table_when_available() -> None:
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ.get("SEC_P4_DISPOSABLE_DSN")
    if not dsn:
        pytest.skip("SEC_P4_DISPOSABLE_DSN not supplied")
    from src.sec_regulatory.historical_backfill import _inspect_connected_target

    suffix = os.getpid()
    base = f"runner_write_base_{suffix}"
    view = f"runner_write_view_{suffix}"
    foreign = f"runner_write_foreign_{suffix}"
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE TABLE public.{base} (id integer)")
            cursor.execute(f"CREATE VIEW public.{view} AS SELECT id FROM public.{base}")
            cursor.execute("SAVEPOINT before_foreign_probe")
            foreign_available = True
            try:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS file_fdw")
                cursor.execute(f"CREATE SERVER runner_file_server_{suffix} FOREIGN DATA WRAPPER file_fdw")
                cursor.execute(f"CREATE FOREIGN TABLE public.{foreign} (id integer) SERVER runner_file_server_{suffix} OPTIONS (filename '/dev/null')")
            except psycopg.Error:
                foreign_available = False
                cursor.execute("ROLLBACK TO SAVEPOINT before_foreign_probe")
        observed = _inspect_connected_target(connection)
        connection.rollback()
    assert f"public.{view}" in observed["writable_tables"]
    if foreign_available:
        assert f"public.{foreign}" in observed["writable_tables"]


def _authorization(*, code_sha: str = "code-v1", inventory_hash: str = "inventory-v1") -> dict[str, object]:
    return {
        "schema_version": 3,
        "stage": "phase4_historical_backfill",
        "code_sha": code_sha,
        "inventory_hash": inventory_hash,
        "target_mode": "local_disposable",
        "dsn_env_var": "SEC_BACKFILL_FAKE_DSN",
        "target": {
            "project": "local-disposable",
            "vm": "local-disposable",
            "zone": "local-disposable",
            "host": "localhost",
            "resolved_addresses": ["127.0.0.1"],
            "database": "sec_backfill_test",
            "server_address": "127.0.0.1",
            "role": "sec_backfill_test",
            "secret_source": "pytest-disposable-fixture",
            "postgresql_identity": "PostgreSQL 18",
            "timescaledb_identity": "TimescaleDB 2.27",
        },
        "writable_tables": ["sec_raw.nport_filings"],
        "pointer_table_denylist": ["sec_current.provider_pointer"],
        "sanitized_command": ["historical-backfill", "start"],
        "run_directory": "E:/runs/fake",
        "source_roots": {"nport": "E:/Edgard/nport", "ncen": "E:/Edgard/ncen", "rr1": "E:/Edgard/RR1"},
        "authorization_id": "auth-local-001",
        "stop_contract_hash": "a" * 64,
        "reconciliation_contract_hash": "b" * 64,
        "preflight_attestation": None,
        "runner_attestation": {
            "project": "local-disposable",
            "service_account": "local-test-runner@example.test",
            "disk_identity": "local-disposable-disk",
        },
        "secret_version_resource": "projects/local-disposable/secrets/sec-backfill/versions/1",
        "execution_mode": "canary",
        "package_scope": [{"identity": "nport:2024Q1:fixture", "package_sha256": "c" * 64}],
        "canary_certificate": None,
    }


def _production_preflight(*, cluster_identity: str = "cluster-fake") -> dict[str, object]:
    from src.sec_regulatory.historical_backfill import (
        EXACT_DIRECT_EXECUTE_ROUTINES,
        EXACT_DIRECT_TABLE_PRIVILEGES,
        EXACT_DIRECT_USAGE_SEQUENCES,
        EXACT_DIRECT_WRITABLE_TABLES,
        EXACT_IDENTITY_SEQUENCES,
        EXACT_SECURITY_DEFINER_ROUTINES,
        EXACT_WRITABLE_TABLES,
        _canonical_json,
        _sha256_bytes,
    )

    attestation = {
        "cluster_identity": cluster_identity,
        "tls_identity": "verify-full:fake-ca",
        "role_identity": "sec_backfill_runner",
        "fixed_memberships": ["sec_backfill_executor"],
        "role_capabilities": {
            "is_superuser": False, "owns_any_table": False, "can_create_role": False,
            "can_create_database": False, "bypass_rls": False, "schema_create": False, "set_role": False, "no_memberships": False,
        },
        "object_catalog_hash": "d" * 64,
        "object_identities": {
            "relations": sorted(f"{table}|oid={index}|owner=sec_backfill_owner|relkind=r|definition_sha256={'a' * 64}" for index, table in enumerate(EXACT_WRITABLE_TABLES)), "columns": ["public.nport_raw_rows.raw_row_id:1"],
            "constraints": ["public.nport_raw_rows:nport_raw_rows_pkey:2"], "indexes": ["public.nport_raw_rows_pkey:3"],
            "triggers": ["public.nport_raw_rows:nport_validate_raw_statement:4"], "sequences": sorted(f"{name}:{index}" for index, name in enumerate(EXACT_IDENTITY_SEQUENCES)), "routines": sorted(f"{name}|oid={index}|owner=sec_backfill_owner|definition_sha256={'b' * 64}|proconfig_sha256={'c' * 64}" for index, name in enumerate(EXACT_SECURITY_DEFINER_ROUTINES)),
        },
        "table_privileges": {table: list(verbs) for table, verbs in EXACT_DIRECT_TABLE_PRIVILEGES.items()},
        "sequence_privileges": {name: ["USAGE"] for name in EXACT_DIRECT_USAGE_SEQUENCES},
        "function_privileges": {name: ["EXECUTE"] for name in EXACT_DIRECT_EXECUTE_ROUTINES},
        "monitoring_privileges": {"pg_stat_activity": ["SELECT"], "pg_locks": ["SELECT"], "pg_monitor": ["DIRECT_MEMBER_NO_SET"], "pg_read_all_stats": ["INHERITED_USAGE"]},
        "effective_writable_tables": sorted(EXACT_DIRECT_WRITABLE_TABLES),
        "truncate_tables": [],
        "public_acl": [],
        "unsafe_security_definers": [],
        "trigger_write_targets": [],
    }
    attestation["object_catalog_hash"] = _sha256_bytes(_canonical_json(attestation["object_identities"]).encode("ascii"))
    return attestation


def _production_authorization(inventory_hash: str) -> dict[str, object]:
    from src.sec_regulatory.historical_backfill import EXACT_WRITABLE_TABLES

    artifact = _authorization(inventory_hash=inventory_hash)
    artifact["target_mode"] = "production_authorized"
    artifact["target"] = {
        **artifact["target"],
        "project": "investintell-research-analisys", "vm": "timescale-sp", "zone": "southamerica-east1-a",
        "database": "market", "server_address": "10.0.0.1", "role": "sec_backfill_runner",
    }
    artifact["writable_tables"] = sorted(EXACT_WRITABLE_TABLES)
    artifact["source_roots"] = {"nport": "/srv/sec-corpus/nport", "ncen": "/srv/sec-corpus/ncen", "rr1": "/srv/sec-corpus/RR1"}
    artifact["run_directory"] = "/var/lib/sec-backfill/run-1"
    artifact["preflight_attestation"] = _production_preflight()
    return artifact


def _least_privilege_production_preflight() -> dict[str, object]:
    valid = _production_preflight()
    table_privileges = {
        "public.sec_ingestion_runs": ["SELECT", "INSERT", "UPDATE"],
        "public.sec_source_packages": ["SELECT", "INSERT", "UPDATE"],
        "public.sec_source_files": ["SELECT", "INSERT", "UPDATE"],
        "public.sec_source_package_transitions": ["SELECT"],
        "public.sec_table_reconciliations": ["SELECT", "INSERT", "UPDATE"],
        "public.sec_row_issues": ["SELECT", "INSERT"],
        "public.sec_run_transitions": ["SELECT"],
        "public.sec_validated_raw_visibility": ["SELECT"],
        "public.sec_raw_validation_tokens": ["SELECT"],
        "public.nport_raw_rows": ["SELECT", "INSERT", "UPDATE"],
        "public.nport_holding_accession_map": ["SELECT", "INSERT", "DELETE"],
        "public.nport_contract_tables": ["SELECT"],
        "public.ncen_raw_v2_rows": ["SELECT", "INSERT"],
        "public.ncen_contract_tables": ["SELECT"],
        "public.rr1_raw_v2_rows": ["SELECT", "INSERT"],
        "public.rr1_contract_tables": ["SELECT"],
    }
    sequence_privileges = {
        "public.sec_table_reconciliations_reconciliation_id_seq": ["USAGE"],
        "public.sec_row_issues_issue_id_seq": ["USAGE"],
        "public.nport_raw_rows_raw_row_id_seq": ["USAGE"],
        "public.ncen_raw_v2_rows_raw_row_id_seq": ["USAGE"],
        "public.rr1_raw_v2_rows_raw_row_id_seq": ["USAGE"],
    }
    function_privileges = {
        "public.sec_validate_raw_run(uuid,text)": ["EXECUTE"],
        "public.sec_record_commit_outcome(uuid,uuid,character,character,text)": ["EXECUTE"],
        "public.sec_resolve_ambiguous_commit_outcome(uuid,uuid,character,character,character,character,text)": ["EXECUTE"],
        "public.sec_query_governed_evidence(uuid,uuid,character)": ["EXECUTE"],
        "public.sec_promote_certified_canary_package(uuid,uuid,uuid,character,character,character,uuid,character)": ["EXECUTE"],
    }
    effective_writable_tables = sorted(
        table for table, verbs in table_privileges.items() if set(verbs) - {"SELECT"}
    )
    return {
        **valid,
        "table_privileges": table_privileges,
        "sequence_privileges": sequence_privileges,
        "function_privileges": function_privileges,
        "effective_writable_tables": effective_writable_tables,
    }


def test_preflight_requires_exact_least_privilege_direct_grant_surfaces() -> None:
    from src.sec_regulatory.historical_backfill import (
        BackfillSafetyError,
        EXACT_DIRECT_EXECUTE_ROUTINES,
        EXACT_DIRECT_TABLE_PRIVILEGES,
        EXACT_DIRECT_USAGE_SEQUENCES,
        _validate_preflight_attestation,
    )

    valid = _least_privilege_production_preflight()
    assert EXACT_DIRECT_TABLE_PRIVILEGES == {
        table: tuple(verbs) for table, verbs in valid["table_privileges"].items()
    }
    assert EXACT_DIRECT_USAGE_SEQUENCES == frozenset(valid["sequence_privileges"])
    assert EXACT_DIRECT_EXECUTE_ROUTINES == frozenset(valid["function_privileges"])
    assert len(valid["object_identities"]["relations"]) == 16
    assert len(valid["object_identities"]["sequences"]) == 7
    assert len(valid["object_identities"]["routines"]) == 12
    assert len(valid["effective_writable_tables"]) == 9
    assert _validate_preflight_attestation(valid)["table_privileges"] == valid["table_privileges"]

    for changed in (
        {**valid, "table_privileges": {**valid["table_privileges"], "public.sec_source_package_transitions": ["SELECT", "INSERT"]}},
        {**valid, "table_privileges": {key: value for key, value in valid["table_privileges"].items() if key != "public.rr1_contract_tables"}},
        {**valid, "sequence_privileges": {key: value for key, value in valid["sequence_privileges"].items() if key != "public.rr1_raw_v2_rows_raw_row_id_seq"}},
        {**valid, "sequence_privileges": {**valid["sequence_privileges"], "public.sec_run_transitions_transition_id_seq": ["USAGE"]}},
        {**valid, "function_privileges": {key: value for key, value in valid["function_privileges"].items() if key != "public.sec_validate_raw_run(uuid,text)"}},
        {**valid, "function_privileges": {**valid["function_privileges"], "public.sec_audit_run_lifecycle()": ["EXECUTE"]}},
        {**valid, "effective_writable_tables": [*valid["effective_writable_tables"], "public.unapproved"]},
    ):
        with pytest.raises(BackfillSafetyError, match="table privilege|privilege identity|effective writable"):
            _validate_preflight_attestation(changed)


def test_preflight_collector_queries_complete_non_system_privilege_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.sec_regulatory import historical_backfill as backfill

    expected = _production_preflight()
    authorization = _production_authorization("a" * 64)
    monkeypatch.setattr(
        backfill,
        "_connection_parameters",
        lambda _connection: {"host": authorization["target"]["host"], "sslmode": "verify-full"},
    )
    monkeypatch.setattr(
        backfill,
        "_collect_nonrelation_object_identities",
        lambda _connection: {**expected["object_identities"], "relations": []},
    )
    monkeypatch.setattr(
        backfill,
        "_collect_relation_security",
        lambda _connection: {"relations": expected["object_identities"]["relations"]},
    )
    monkeypatch.setattr(
        backfill,
        "_collect_monitoring_privileges",
        lambda _connection: {"monitoring_privileges": expected["monitoring_privileges"]},
    )

    def query_json(_connection: object, query: str, params: tuple[object, ...] = ()) -> object:
        if "'cluster_identity'" in query:
            return {field: expected[field] for field in ("cluster_identity", "tls_identity", "role_identity")}
        if "'memberships'" in query:
            return {"memberships": [], "capabilities": {**expected["role_capabilities"], "no_memberships": True}}
        if "'table_privileges'" in query:
            assert params == ()
            assert query.count("n.nspname !~ '^pg_'") >= 3
            assert query.count("n.nspname <> 'information_schema'") >= 3
            assert "has_sequence_privilege(current_user,c.oid,'SELECT')" in query
            assert "has_sequence_privilege(current_user,c.oid,'UPDATE')" in query
            assert "has_sequence_privilege(current_user,c.oid,'USAGE')" in query
            return {
                field: expected[field]
                for field in ("table_privileges", "sequence_privileges", "function_privileges")
            }
        if "'effective_writable_tables'" in query:
            return {field: expected[field] for field in ("effective_writable_tables", "truncate_tables")}
        if "'public_acl'" in query:
            return {field: expected[field] for field in ("public_acl", "unsafe_security_definers", "trigger_write_targets")}
        raise AssertionError("unexpected preflight query")

    monkeypatch.setattr(backfill, "_query_json", query_json)
    observed = backfill._collect_production_preflight(object(), authorization)
    assert observed["table_privileges"] == expected["table_privileges"]
    assert observed["sequence_privileges"] == expected["sequence_privileges"]
    assert observed["function_privileges"] == expected["function_privileges"]


@pytest.mark.parametrize(
    ("grant_kind", "grant_verb"),
    (
        ("baseline", ""),
        ("table", "SELECT"),
        ("sequence", "USAGE"),
        ("sequence", "SELECT"),
        ("sequence", "UPDATE"),
        ("routine", "EXECUTE"),
    ),
)
def test_real_pg18_preflight_accepts_exact_and_refuses_hidden_cross_schema_runner_grant(grant_kind: str, grant_verb: str) -> None:
    psycopg = pytest.importorskip("psycopg")
    dsn = os.environ.get("SEC_P4_DISPOSABLE_DSN")
    if not dsn:
        pytest.skip("SEC_P4_DISPOSABLE_DSN not supplied")
    from psycopg import sql

    from src.ncen import storage as ncen_storage
    from src.nport import storage as nport_storage
    from src.rr1 import storage as rr1_storage
    from src.sec_regulatory import manifests
    from src.sec_regulatory.historical_backfill import (
        BackfillSafetyError,
        EXACT_DIRECT_EXECUTE_ROUTINES,
        EXACT_DIRECT_TABLE_PRIVILEGES,
        EXACT_DIRECT_USAGE_SEQUENCES,
        EXACT_IDENTITY_SEQUENCES,
        EXACT_MONITORED_RELATIONS,
        EXACT_SECURITY_DEFINER_ROUTINES,
        _collect_production_preflight,
    )

    suffix = uuid4().hex[:12]
    role = f"runner_hidden_grant_{suffix}"
    schema = f"runner_hidden_schema_{suffix}"
    with psycopg.connect(dsn) as installer:
        manifests.install_schema(installer)
        nport_storage.install_schema(installer)
        ncen_storage.install_schema(installer)
        rr1_storage.install_schema(installer)
        installer.commit()

    admin = psycopg.connect(dsn, autocommit=True)
    try:
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE ROLE {} NOLOGIN INHERIT").format(sql.Identifier(role)))
            cursor.execute(
                sql.SQL("GRANT pg_monitor TO {} WITH INHERIT TRUE, SET FALSE").format(sql.Identifier(role))
            )
            cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
            for table, verbs in EXACT_DIRECT_TABLE_PRIVILEGES.items():
                namespace, relation = table.split(".", 1)
                cursor.execute(
                    sql.SQL("GRANT {} ON TABLE {}.{} TO {}").format(
                        sql.SQL(", ".join(verbs)),
                        sql.Identifier(namespace),
                        sql.Identifier(relation),
                        sql.Identifier(role),
                    )
                )
            for sequence in EXACT_DIRECT_USAGE_SEQUENCES:
                namespace, relation = sequence.split(".", 1)
                cursor.execute(
                    sql.SQL("GRANT USAGE ON SEQUENCE {}.{} TO {}").format(
                        sql.Identifier(namespace), sql.Identifier(relation), sql.Identifier(role)
                    )
                )
            for routine in EXACT_DIRECT_EXECUTE_ROUTINES:
                cursor.execute(
                    sql.SQL("GRANT EXECUTE ON FUNCTION {} TO {}").format(
                        sql.SQL(routine), sql.Identifier(role)
                    )
                )
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(schema), sql.Identifier(role)))
            cursor.execute(sql.SQL("CREATE TABLE {}.hidden_relation (id integer)").format(sql.Identifier(schema)))
            cursor.execute(sql.SQL("CREATE SEQUENCE {}.hidden_sequence").format(sql.Identifier(schema)))
            cursor.execute(
                sql.SQL(
                    "CREATE FUNCTION {}.hidden_security_definer() RETURNS integer LANGUAGE sql "
                    "SECURITY DEFINER SET search_path = pg_catalog AS 'SELECT 1'"
                ).format(sql.Identifier(schema))
            )
            cursor.execute(
                sql.SQL("REVOKE ALL ON FUNCTION {}.hidden_security_definer() FROM PUBLIC").format(
                    sql.Identifier(schema)
                )
            )
            if grant_kind == "table":
                grant_target = sql.SQL("TABLE {}.hidden_relation").format(sql.Identifier(schema))
            elif grant_kind == "sequence":
                grant_target = sql.SQL("SEQUENCE {}.hidden_sequence").format(sql.Identifier(schema))
            elif grant_kind == "routine":
                grant_target = sql.SQL("FUNCTION {}.hidden_security_definer()").format(sql.Identifier(schema))
            if grant_kind != "baseline":
                cursor.execute(
                    sql.SQL("GRANT {} ON {} TO {}").format(
                        sql.SQL(grant_verb), grant_target, sql.Identifier(role)
                    )
                )

        authorization = _production_authorization("a" * 64)
        authorization["target"] = {**authorization["target"], "host": "127.0.0.1"}

        class Info:
            def get_parameters(self) -> dict[str, str]:
                return {"host": "127.0.0.1", "sslmode": "verify-full"}

        class Connection:
            info = Info()

            def __init__(self, raw: object) -> None:
                self.raw = raw

            def cursor(self) -> object:
                return self.raw.cursor()  # type: ignore[no-any-return,union-attr]

        with psycopg.connect(dsn) as runner:
            with runner.cursor() as cursor:
                cursor.execute(sql.SQL("SET SESSION AUTHORIZATION {}").format(sql.Identifier(role)))
            if grant_kind == "baseline":
                observed = _collect_production_preflight(Connection(runner), authorization)
                object_identities = observed["object_identities"]
                assert all(values == sorted(set(values)) for values in object_identities.values())
                assert {
                    item.split("|", 1)[0] for item in object_identities["relations"]
                } == EXACT_MONITORED_RELATIONS
                assert {
                    item.rsplit(":", 1)[0] for item in object_identities["sequences"]
                } == EXACT_IDENTITY_SEQUENCES
                assert {
                    item.split("|", 1)[0] for item in object_identities["routines"]
                } == EXACT_SECURITY_DEFINER_ROUTINES
                assert set(observed["function_privileges"]) == EXACT_DIRECT_EXECUTE_ROUTINES
            else:
                with pytest.raises(BackfillSafetyError, match="table privilege|privilege identity|privilege matrix"):
                    _collect_production_preflight(Connection(runner), authorization)
    finally:
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            cursor.execute(sql.SQL("REVOKE pg_monitor FROM {}").format(sql.Identifier(role)))
            cursor.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            cursor.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
        admin.close()


def test_preflight_requires_exact_security_definer_recovery_routine_set() -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, EXACT_SECURITY_DEFINER_ROUTINES, _validate_preflight_attestation

    valid = _production_preflight()
    assert "public.sec_resolve_ambiguous_commit_outcome(uuid,uuid,character,character,character,character,text)" in EXACT_SECURITY_DEFINER_ROUTINES
    for routines in (
        [item for item in valid["object_identities"]["routines"] if not item.startswith("public.sec_resolve_ambiguous_commit_outcome(")],
        [*valid["object_identities"]["routines"], "public.unapproved_recovery():999"],
        [item.replace("character,character,text)", "text,text,text)") if item.startswith("public.sec_resolve_ambiguous_commit_outcome(") else item for item in valid["object_identities"]["routines"]],
    ):
        changed = {**valid, "object_identities": {**valid["object_identities"], "routines": sorted(routines)}}
        with pytest.raises(BackfillSafetyError, match="SECURITY DEFINER routine"):
            _validate_preflight_attestation(changed)
    missing_privilege = {**valid, "function_privileges": {key: value for key, value in valid["function_privileges"].items() if key != "public.sec_resolve_ambiguous_commit_outcome(uuid,uuid,character,character,character,character,text)"}}
    with pytest.raises(BackfillSafetyError, match="privilege identity"):
        _validate_preflight_attestation(missing_privilege)


def test_preflight_rejects_usage_only_monitoring_claim_and_requires_direct_no_set_membership() -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, _validate_preflight_attestation

    valid = _production_preflight()
    assert _validate_preflight_attestation(valid)["monitoring_privileges"]["pg_monitor"] == ["DIRECT_MEMBER_NO_SET"]
    inherited_only = {**valid, "monitoring_privileges": {**valid["monitoring_privileges"], "pg_monitor": ["INHERITED_USAGE"]}}
    with pytest.raises(BackfillSafetyError, match="monitoring"):
        _validate_preflight_attestation(inherited_only)


def test_preflight_rejects_truncate_or_any_outside_effective_write() -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, EXACT_WRITABLE_TABLES, _validate_preflight_attestation

    valid = _production_preflight()
    for changed in (
        {**valid, "truncate_tables": ["public.nport_raw_rows"]},
        {**valid, "effective_writable_tables": sorted({*EXACT_WRITABLE_TABLES, "public.unapproved"})},
    ):
        with pytest.raises(BackfillSafetyError, match="writable|TRUNCATE"):
            _validate_preflight_attestation(changed)


def test_preflight_object_catalog_hash_binds_routine_definition_and_owner() -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, _validate_preflight_attestation

    valid = _production_preflight()
    routine = valid["object_identities"]["routines"][0]
    changed = {**valid, "object_identities": {**valid["object_identities"], "routines": [routine.replace("definition_sha256=" + "b" * 64, "definition_sha256=" + "d" * 64) if item == routine else item for item in valid["object_identities"]["routines"]]}}
    with pytest.raises(BackfillSafetyError, match="catalog hash"):
        _validate_preflight_attestation(changed)


def test_execution_authorization_requires_exact_schema_and_matches_code_and_inventory(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, load_execution_authorization

    artifact = _authorization()
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")

    loaded = load_execution_authorization(path, code_sha="code-v1", inventory_hash="inventory-v1")

    assert loaded["authorization_id"] == "auth-local-001"
    with pytest.raises(BackfillSafetyError, match="authorization schema"):
        path.write_text(json.dumps({**artifact, "unexpected": True}), encoding="utf-8")
        load_execution_authorization(path, code_sha="code-v1", inventory_hash="inventory-v1")
    with pytest.raises(BackfillSafetyError, match="code SHA"):
        path.write_text(json.dumps(artifact), encoding="utf-8")
        load_execution_authorization(path, code_sha="other", inventory_hash="inventory-v1")


def test_authorization_fingerprint_binds_complete_artifact_command_and_run_directory(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, authorization_fingerprint, load_execution_authorization

    artifact = _authorization()
    artifact["run_directory"] = str((tmp_path / "run").resolve())
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")

    loaded = load_execution_authorization(
        path, code_sha="code-v1", inventory_hash="inventory-v1",
        run_directory=tmp_path / "run", command=("historical-backfill", "start"),
    )

    assert loaded["authorization_fingerprint"] == authorization_fingerprint(artifact)
    changed = {**artifact, "writable_tables": ["sec_raw.changed"]}
    path.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(BackfillSafetyError, match="run directory|command"):
        load_execution_authorization(path, code_sha="code-v1", inventory_hash="inventory-v1", run_directory=tmp_path / "other", command=("historical-backfill", "resume"))
    assert authorization_fingerprint(changed) != authorization_fingerprint(artifact)


def test_authorization_fingerprint_binds_runner_secret_scope_and_canary_lineage(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, authorization_fingerprint, load_execution_authorization

    artifact = _authorization()
    path = tmp_path / "authorization.json"
    for field, replacement in (
        ("runner_attestation", {"project": "other", "service_account": "runner@example.test", "disk_identity": "disk"}),
        ("secret_version_resource", "projects/local-disposable/secrets/sec-backfill/versions/2"),
        ("package_scope", [{"identity": "nport:2024Q1:other", "package_sha256": "d" * 64}]),
    ):
        changed = {**artifact, field: replacement}
        assert authorization_fingerprint(changed) != authorization_fingerprint(artifact)
    missing = dict(artifact)
    missing.pop("runner_attestation")
    path.write_text(json.dumps(missing), encoding="utf-8")
    with pytest.raises(BackfillSafetyError, match="authorization schema"):
        load_execution_authorization(path, code_sha="code-v1", inventory_hash="inventory-v1")


def test_authorization_and_connected_writable_tables_reject_current_namespace_and_duplicates(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, load_execution_authorization

    artifact = _authorization()
    path = tmp_path / "authorization.json"
    for writable in (["sec_current.other"], ["provider_pointer"], ["sec_raw.nport_filings", "sec_raw.nport_filings"]):
        path.write_text(json.dumps({**artifact, "writable_tables": writable}), encoding="utf-8")
        with pytest.raises(BackfillSafetyError, match="writable"):
            load_execution_authorization(path, code_sha="code-v1", inventory_hash="inventory-v1")


def test_invalid_authorization_never_resolves_the_dsn_environment_variable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, build_authorized_executor

    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(_authorization(code_sha="wrong")), encoding="utf-8")
    seen: list[str] = []
    monkeypatch.setattr(os, "environ", {"SEC_BACKFILL_FAKE_DSN": "postgresql://never:read@localhost/test"})

    with pytest.raises(BackfillSafetyError, match="code SHA"):
        build_authorized_executor(path, inventory={"inventory_hash": "inventory-v1"}, code_sha="code-v1", connection_factory=lambda value: seen.append(value))

    assert seen == []


class _Connection:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class _Cursor:
    def __init__(self, events: list[str], busy: bool = False) -> None:
        self.events = events
        self.busy = busy
        self.query = ""

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, _params: object = None) -> None:
        self.query = query
        self.events.append("try_lock" if "pg_try_advisory_lock" in query else "unlock")

    def fetchone(self) -> tuple[bool]:
        return (not self.busy,)


class _LockConnection(_Connection):
    def __init__(self, events: list[str], busy: bool = False) -> None:
        super().__init__()
        self.events = events
        self.busy = busy

    def cursor(self) -> _Cursor:
        return _Cursor(self.events, self.busy)


class _CommitFailureConnection(_LockConnection):
    def commit(self) -> None:
        self.commits += 1
        raise RuntimeError("connection lost during COMMIT")


def _single_package_inventory(tmp_path: Path) -> tuple[dict[str, object], Path]:
    from src.sec_regulatory.historical_backfill import SourceSpec, build_inventory

    root = tmp_path / "nport"
    package = root / "2024q1_nport"
    package.mkdir(parents=True)
    (package / "submission.tsv").write_text("a\tb\n1\t2\n", encoding="utf-8")
    return build_inventory((SourceSpec("nport", root, 1),)), package


def _three_form_inventory(tmp_path: Path) -> dict[str, object]:
    from src.sec_regulatory.historical_backfill import SourceSpec, build_inventory

    specs = []
    for form in ("nport", "ncen", "rr1"):
        root = tmp_path / form
        package = root / f"2024q1_{form}"
        package.mkdir(parents=True)
        (package / "submission.tsv").write_text("a\tb\n1\t2\n", encoding="utf-8")
        specs.append(SourceSpec(form, root, 1))
    return build_inventory(tuple(specs))


def test_authorized_executor_verifies_target_then_dispatches_exact_manifest_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, package_path = _single_package_inventory(tmp_path)
    artifact = _authorization(inventory_hash=str(inventory["inventory_hash"]))
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text(json.dumps(artifact), encoding="utf-8")
    connection = _LockConnection([])
    calls: list[tuple[Path, Path]] = []
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: f"nport:{'c' * 64}")
    monkeypatch.setattr(backfill, "_governed_package_id", lambda *_args, **_kwargs: UUID("33333333-3333-4333-8333-333333333333"))
    monkeypatch.setattr(backfill, "_governed_reconciliation_sha256", lambda *_args, **_kwargs: "f" * 64)

    executor = backfill.build_authorized_executor(
        authorization_path,
        inventory=inventory,
        code_sha="code-v1",
        connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {
            "database": "sec_backfill_test",
            "server_address": "127.0.0.1",
            "role": "sec_backfill_test",
            "postgresql_identity": "PostgreSQL 18",
            "timescaledb_identity": "TimescaleDB 2.27",
            "is_superuser": False,
            "owns_any_table": False,
            "writable_tables": ["sec_raw.nport_filings"],
        },
        schema_installers={"manifest": lambda _conn: None, "nport": lambda _conn: None},
        dispatchers={"nport": lambda _conn, *, package, source_root: calls.append((package, source_root)) or {"package": package.relative_to(source_root).as_posix(), "state": "raw_validated", "rows": 3, "run_id": "run-1"}},
    )

    result = executor(dict(inventory["packages"][0]))

    assert result == {"state": "raw_validated", "rows": 3, "run_id": "run-1"}
    assert calls == [(package_path, package_path.parent)]
    assert connection.commits == 1


def test_protected_commit_fsyncs_issued_records_outcome_then_confirms(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    artifact = _authorization(inventory_hash=str(inventory["inventory_hash"]))
    artifact["supervisor_run_id"] = "11111111-1111-4111-8111-111111111111"
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    connection = _LockConnection([])
    events: list[tuple[str, object]] = []
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: f"nport:{'c' * 64}")
    monkeypatch.setattr(backfill, "_governed_package_id", lambda *_args, **_kwargs: UUID("33333333-3333-4333-8333-333333333333"))
    monkeypatch.setattr(backfill, "_governed_reconciliation_sha256", lambda *_args, **_kwargs: "f" * 64)
    monkeypatch.setattr(backfill.manifests, "record_commit_outcome", lambda _conn, **kwargs: events.append(("manifest", kwargs)) or object())

    def dispatch(dispatch_connection: object, *, package: Path, source_root: Path) -> dict[str, object]:
        dispatch_connection.commit()  # type: ignore[attr-defined]  # ingester checkpoint must be suppressed
        return {"package": package.relative_to(source_root).as_posix(), "state": "raw_validated", "rows": 1, "run_id": "22222222-2222-4222-8222-222222222222", "reconciliation_hash": "f" * 64}

    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {"database": "sec_backfill_test", "server_address": "127.0.0.1", "role": "sec_backfill_test", "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27", "is_superuser": False, "owns_any_table": False, "writable_tables": ["sec_raw.nport_filings"]},
        schema_installers={"manifest": lambda _conn: None, "nport": lambda _conn: None},
        dispatchers={"nport": dispatch},
    )

    result = executor.execute_with_fence(dict(inventory["packages"][0]), lambda state, evidence: events.append((state, dict(evidence))))

    assert result["state"] == "raw_validated"
    assert [event[0] for event in events] == ["issued", "manifest", "confirmed"]
    assert connection.commits == 1 and connection.rollbacks == 0


def test_commit_exception_is_ambiguous_and_cleanup_does_not_retry_or_downgrade(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    artifact = _authorization(inventory_hash=str(inventory["inventory_hash"]))
    artifact["supervisor_run_id"] = "11111111-1111-4111-8111-111111111111"
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    connection = _CommitFailureConnection([])
    events: list[str] = []
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: f"nport:{'c' * 64}")
    monkeypatch.setattr(backfill, "_governed_package_id", lambda *_args, **_kwargs: UUID("33333333-3333-4333-8333-333333333333"))
    monkeypatch.setattr(backfill, "_governed_reconciliation_sha256", lambda *_args, **_kwargs: "f" * 64)
    monkeypatch.setattr(backfill.manifests, "record_commit_outcome", lambda *_args, **_kwargs: object())
    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {"database": "sec_backfill_test", "server_address": "127.0.0.1", "role": "sec_backfill_test", "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27", "is_superuser": False, "owns_any_table": False, "writable_tables": ["sec_raw.nport_filings"]},
        schema_installers={"manifest": lambda _conn: None, "nport": lambda _conn: None},
        dispatchers={"nport": lambda _conn, *, package, source_root: {"package": package.relative_to(source_root).as_posix(), "state": "raw_validated", "rows": 1, "run_id": "22222222-2222-4222-8222-222222222222", "reconciliation_hash": "f" * 64}},
    )

    with pytest.raises(backfill.AmbiguousCommitError):
        executor.execute_with_fence(dict(inventory["packages"][0]), lambda state, _evidence: events.append(state))
    assert events == ["issued", "ambiguous"]
    assert connection.commits == 1 and connection.rollbacks == 0
    assert connection.rollbacks == 0
    assert connection.closed


def test_authorized_executor_uses_nonblocking_form_lock_before_schema_install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(_authorization(inventory_hash=str(inventory["inventory_hash"]))), encoding="utf-8")
    events: list[str] = []
    connection = _LockConnection(events)
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: "nport:fixed-digest")
    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {"database": "sec_backfill_test", "server_address": "127.0.0.1", "role": "sec_backfill_test", "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27", "is_superuser": False, "owns_any_table": False, "writable_tables": ["sec_raw.nport_filings"]},
        schema_installers={"manifest": lambda _conn: events.append("manifest"), "nport": lambda _conn: events.append("form")},
        dispatchers={"nport": lambda _conn, *, package, source_root: {"package": package.relative_to(source_root).as_posix(), "state": "raw_validated", "rows": 0, "run_id": "run-1"}},
    )

    executor(dict(inventory["packages"][0]))

    assert events == ["try_lock", "manifest", "form", "unlock"]


def test_production_authorized_executor_never_runs_schema_installers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    artifact = _production_authorization(str(inventory["inventory_hash"]))
    artifact["package_scope"] = [{"identity": inventory["packages"][0]["identity"], "package_sha256": inventory["packages"][0]["package_sha256"]}]
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    installed: list[str] = []
    connection = _LockConnection([])
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: "nport:fixed-digest")

    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {
            "database": "market", "server_address": "10.0.0.1", "role": "sec_backfill_runner",
            "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27",
            "is_superuser": False, "owns_any_table": False, "writable_tables": artifact["writable_tables"], "truncate_tables": [],
        },
        schema_installers={"manifest": lambda _conn: installed.append("manifest"), "nport": lambda _conn: installed.append("nport")},
        dispatchers={"nport": lambda _conn, *, package, source_root: {"package": package.relative_to(source_root).as_posix(), "state": "raw_validated", "rows": 0, "run_id": "run-1"}},
        preflight_inspector=lambda _conn: _production_preflight(),
    )

    executor(dict(inventory["packages"][0]))

    assert installed == []


def test_production_preflight_refuses_attestation_drift_before_dispatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    artifact = _production_authorization(str(inventory["inventory_hash"]))
    artifact["package_scope"] = [{"identity": inventory["packages"][0]["identity"], "package_sha256": inventory["packages"][0]["package_sha256"]}]
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: _Connection(),
        target_inspector=lambda _conn: {
            "database": "market", "server_address": "10.0.0.1", "role": "sec_backfill_runner",
            "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27",
            "is_superuser": False, "owns_any_table": False, "writable_tables": artifact["writable_tables"], "truncate_tables": [],
        },
    )

    executor.preflight_inspector = lambda _conn: _production_preflight(cluster_identity="drifted")
    with pytest.raises(backfill.BackfillSafetyError, match="preflight"):
        executor.preflight()


def test_production_executor_uses_builtin_read_only_collector_without_injection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    artifact = _production_authorization(str(inventory["inventory_hash"]))
    artifact["package_scope"] = [{"identity": inventory["packages"][0]["identity"], "package_sha256": inventory["packages"][0]["package_sha256"]}]
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    collected: list[object] = []
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_collect_production_preflight", lambda connection, authorization: collected.append(connection) or _production_preflight())

    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: _Connection(),
        target_inspector=lambda _conn: {
            "database": "market", "server_address": "10.0.0.1", "role": "sec_backfill_runner",
            "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27",
            "is_superuser": False, "owns_any_table": False, "writable_tables": artifact["writable_tables"], "truncate_tables": [],
        },
    )

    executor.preflight()

    assert len(collected) == 1


def test_production_preflight_runs_before_dispatch_and_refuses_each_attestation_matrix_drift(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    artifact = _production_authorization(str(inventory["inventory_hash"]))
    artifact["package_scope"] = [{"identity": inventory["packages"][0]["identity"], "package_sha256": inventory["packages"][0]["package_sha256"]}]
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: "nport:fixed-digest")
    dispatched: list[bool] = []
    for field in (
        "cluster_identity", "tls_identity", "role_identity", "fixed_memberships", "object_catalog_hash",
        "object_identities", "table_privileges", "sequence_privileges", "function_privileges",
        "monitoring_privileges", "public_acl", "unsafe_security_definers", "trigger_write_targets",
    ):
        actual = _production_preflight()
        actual[field] = "drifted" if isinstance(actual[field], str) else {"drifted": []} if isinstance(actual[field], dict) else ["drifted"]
        executor = backfill.build_authorized_executor(
            path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: _LockConnection([]),
            target_inspector=lambda _conn: {
                "database": "market", "server_address": "10.0.0.1", "role": "sec_backfill_runner",
                "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27",
                "is_superuser": False, "owns_any_table": False, "writable_tables": artifact["writable_tables"], "truncate_tables": [],
            },
            preflight_inspector=lambda _conn, value=actual: value,
            dispatchers={"nport": lambda *_args, **_kwargs: dispatched.append(True) or pytest.fail("preflight drift must block DML")},
        )
        with pytest.raises(backfill.BackfillSafetyError, match="preflight"):
            executor(dict(inventory["packages"][0]))
    assert dispatched == []


def test_certificate_bound_promotion_is_exact_immutable_and_idempotent(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, promote_canary_packages

    inventory, _ = _single_package_inventory(tmp_path)
    package = inventory["packages"][0]
    certificate = {
        "certificate_id": "certificate-1", "canary_run_id": "canary-run-1",
        "canary_authorization_fingerprint": "e" * 64, "inventory_hash": inventory["inventory_hash"],
        "packages": [{"identity": package["identity"], "package_sha256": package["package_sha256"]}],
    }
    canary = {package["identity"]: {"state": "raw_validated", "package_sha256": package["package_sha256"], "run_id": "canary-run-1", "authorization_fingerprint": "e" * 64, "reconciliation_hash": "f" * 64}}
    appended: list[dict[str, object]] = []
    transitions = promote_canary_packages(certificate, inventory=inventory, canary_records=canary, full_records={}, existing_transitions=[], append_transition=lambda item: appended.append(dict(item)))

    assert transitions == appended and transitions[0]["state"] == "CANARY_PROMOTED"
    assert promote_canary_packages(certificate, inventory=inventory, canary_records=canary, full_records={}, existing_transitions=transitions, append_transition=lambda _item: pytest.fail("idempotent promotion must not append")) == transitions
    for invalid in (
        {},
        {package["identity"]: {**canary[package["identity"]], "state": "running"}},
        {package["identity"]: {**canary[package["identity"]], "reconciliation_hash": None}},
    ):
        with pytest.raises(BackfillSafetyError):
            promote_canary_packages(certificate, inventory=inventory, canary_records=invalid, full_records={}, existing_transitions=[], append_transition=lambda _item: None)
    with pytest.raises(BackfillSafetyError, match="cross-run duplicate"):
        promote_canary_packages(certificate, inventory=inventory, canary_records=canary, full_records={package["identity"]: {"run_id": "canary-run-1"}}, existing_transitions=[], append_transition=lambda _item: None)
    with pytest.raises(BackfillSafetyError, match="nonterminal|unreconciled"):
        promote_canary_packages(certificate, inventory=inventory, canary_records={package["identity"]: {**canary[package["identity"]], "authorization_fingerprint": "a" * 64}}, full_records={}, existing_transitions=[], append_transition=lambda _item: None)


def test_typed_canary_promotion_uses_governed_wrappers_and_one_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import types
    import src.sec_regulatory.historical_backfill as backfill

    inventory = _three_form_inventory(tmp_path)
    certificate_id = "11111111-1111-4111-8111-111111111111"
    supervisor_id = "22222222-2222-4222-8222-222222222222"
    canary_fp = "a" * 64
    packages = []
    governed_by_form: dict[str, str] = {}
    for index, package in enumerate(inventory["packages"], start=3):
        governed_hash = f"{index}" * 64
        governed_by_form[package["form"]] = governed_hash
        packages.append({
            "identity": package["identity"], "package_sha256": governed_hash,
            "package_id": f"{index:08d}-3333-4333-8333-333333333333",
            "ingestion_run_id": f"{index:08d}-4444-4444-8444-444444444444",
            "reconciliation_sha256": f"{index}" * 64,
        })
    certificate = {
        "certificate_id": certificate_id, "canary_supervisor_run_id": supervisor_id,
        "canary_authorization_fingerprint": canary_fp, "inventory_hash": inventory["inventory_hash"],
        "packages": packages,
    }
    certificate["certificate_sha256"] = backfill._sha256_bytes(backfill._canonical_json(certificate).encode("ascii"))
    connection = _Connection()
    promoted: list[object] = []

    def governed(_connection: object, **kwargs: object) -> object:
        package = next(item for item in packages if item["package_id"] == str(kwargs["package_id"]))
        return types.SimpleNamespace(
            package_sha256=package["package_sha256"], commit_outcome="committed", supervisor_run_id=supervisor_id,
            authorization_fingerprint=canary_fp,
        )

    monkeypatch.setattr(backfill, "_get_recovery_governed_evidence", governed)
    monkeypatch.setattr(backfill, "_governed_reconciliation_sha256", lambda _connection, *, run_id: next(item["reconciliation_sha256"] for item in packages if item["ingestion_run_id"] == str(run_id)))
    monkeypatch.setattr(backfill.manifests, "promote_certified_canary_package", lambda *_args, **kwargs: promoted.append(kwargs) or types.SimpleNamespace(package_transition_id=len(promoted)))
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda form, _package: f"{form}:{governed_by_form[form]}")

    seeds = backfill.promote_certified_canary_packages(connection, certificate=certificate, inventory=inventory)

    assert len(seeds) == len(promoted) == 3 and connection.commits == 1 and connection.rollbacks == 0
    assert {record["state"] for record in seeds.values()} == {"canary_promoted"}


def test_v4_production_canary_scope_is_exactly_one_package_per_form(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, _validate_execution_authorization

    inventory = _three_form_inventory(tmp_path)
    artifact = _production_authorization(str(inventory["inventory_hash"]))
    artifact["schema_version"] = 4
    artifact["supervisor_run_id"] = "11111111-1111-4111-8111-111111111111"
    artifact["package_scope"] = [{"identity": item["identity"], "package_sha256": item["package_sha256"]} for item in inventory["packages"]]

    assert _validate_execution_authorization(artifact, code_sha="code-v1", inventory_hash=str(inventory["inventory_hash"]))["execution_mode"] == "canary"
    for invalid_scope in (artifact["package_scope"][:2], [artifact["package_scope"][0], artifact["package_scope"][1], artifact["package_scope"][1]]):
        with pytest.raises(BackfillSafetyError, match="scope|package"):
            _validate_execution_authorization({**artifact, "package_scope": invalid_scope}, code_sha="code-v1", inventory_hash=str(inventory["inventory_hash"]))


def test_production_rollback_probe_uses_real_dispatch_path_and_zero_delta_snapshots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory = _three_form_inventory(tmp_path)
    artifact = _production_authorization(str(inventory["inventory_hash"]))
    artifact.update({"schema_version": 4, "supervisor_run_id": "11111111-1111-4111-8111-111111111111"})
    artifact["package_scope"] = [{"identity": item["identity"], "package_sha256": item["package_sha256"]} for item in inventory["packages"]]
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    connections = [_LockConnection([]) for _ in range(3)]
    dispatched: list[str] = []
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: "fixed")
    monkeypatch.setattr(backfill, "_snapshot_exact_write_counts", lambda _connection: {table: 0 for table in backfill.EXACT_WRITABLE_TABLES})
    target = {
        "database": "market", "server_address": "10.0.0.1", "role": "sec_backfill_runner",
        "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27",
        "is_superuser": False, "owns_any_table": False, "writable_tables": sorted(backfill.EXACT_WRITABLE_TABLES), "truncate_tables": [],
    }
    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connections.pop(0),
        target_inspector=lambda _connection: target, preflight_inspector=lambda _connection: _production_preflight(),
        dispatchers={form: (lambda _connection, *, package, source_root, form=form: dispatched.append(form) or {"package": package.relative_to(source_root).as_posix(), "state": "raw_validated", "rows": 0, "run_id": "22222222-2222-4222-8222-222222222222", "reconciliation_hash": "f" * 64}) for form in ("nport", "ncen", "rr1")},
    )

    evidence = executor.run_rollback_probe(evidence_path=tmp_path / "probe.json")

    assert evidence["state"] == "ROLLBACK_PROBED" and len(evidence["table_deltas"]) == 16
    assert len(dispatched) == 1 and all(value == 0 for value in evidence["table_deltas"].values())


def test_rollback_probe_and_expired_lease_recovery_are_zero_delta_and_lineage_bound() -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, rollback_probe, validate_recovery_outcome

    connection = _Connection()
    evidence: list[dict[str, object]] = []
    assert rollback_probe(connection, probe=lambda _conn: {"state": "rollback_probed", "table_delta": 0}, evidence_writer=lambda item: evidence.append(dict(item))) == {"state": "ROLLBACK_PROBED", "table_delta": 0}
    assert connection.rollbacks == 1 and connection.commits == 0 and evidence == [{"state": "ROLLBACK_PROBED", "table_delta": 0}]
    status = {"lease": {"owner": "old", "expires_at": "2000-01-01T00:00:00+00:00"}, "authorization_fingerprint": "a" * 64}
    assert validate_recovery_outcome(status, authorization_fingerprint="a" * 64, outcome={"commit_outcome": "committed", "authorization_fingerprint": "a" * 64, "terminal_result": {"state": "raw_validated"}}) == {"state": "raw_validated"}
    for outcome in (None, {"commit_outcome": "unknown"}, {"commit_outcome": "committed", "authorization_fingerprint": "b" * 64, "terminal_result": {"state": "raw_validated"}}):
        with pytest.raises(BackfillSafetyError):
            validate_recovery_outcome(status, authorization_fingerprint="a" * 64, outcome=outcome)


@pytest.mark.parametrize(("fence_state", "commit_window"), (("ambiguous_commit", "ambiguous"), ("recovery_required", "issued"), ("recovery_required", "confirmed")))
def test_recovery_uses_governed_evidence_and_persists_definitive_decision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fence_state: str, commit_window: str) -> None:
    import types
    import src.sec_regulatory.historical_backfill as backfill

    status_path = tmp_path / "run" / "status.json"
    identity = "nport:2024Q1:fixture"
    original = "a" * 64
    status = {
        "authorization_fingerprint": original, "active_package": identity, "active_attempt": 1,
        "lease": {"owner": "old", "expires_at": "2000-01-01T00:00:00+00:00"},
        "packages": {identity: {
            "state": fence_state, "commit_window": commit_window, "authorization_fingerprint": original,
            "supervisor_run_id": "11111111-1111-4111-8111-111111111111",
            "package_id": "33333333-3333-4333-8333-333333333333",
            "run_id": "22222222-2222-4222-8222-222222222222", "package_sha256": "b" * 64,
            "governed_package_sha256": "c" * 64,
            "reconciliation_hash": "d" * 64, "terminal_result": {"state": "raw_validated", "rows": 1, "run_id": "22222222-2222-4222-8222-222222222222", "reconciliation_hash": "d" * 64},
        }},
    }
    backfill._write_status(status_path, status)
    evidence = {"governed_query": "unique_commit"}
    recovery = {
        "identity": identity, "original_authorization_fingerprint": original,
        "supervisor_run_id": "11111111-1111-4111-8111-111111111111",
        "package_id": "33333333-3333-4333-8333-333333333333",
        "run_id": "22222222-2222-4222-8222-222222222222", "package_sha256": "c" * 64,
        "reconciliation_sha256": "d" * 64, "expected_outcome": "committed",
        "recovery_evidence_sha256": backfill._sha256_bytes(backfill._canonical_json(evidence).encode("ascii")),
        "recovery_authorization_fingerprint": "e" * 64,
    }
    governed = types.SimpleNamespace(
        commit_outcome="committed", package_id=UUID(recovery["package_id"]), run_id=UUID(recovery["run_id"]),
        package_sha256=recovery["package_sha256"], supervisor_run_id=UUID(recovery["supervisor_run_id"]),
        authorization_fingerprint=original,
    )
    monkeypatch.setattr(backfill, "_get_recovery_governed_evidence", lambda *_args, **_kwargs: governed)

    result = backfill.recover_ambiguous_commit(_Connection(), status_path=status_path, recovery_authorization=recovery, recovery_evidence=evidence)

    durable = json.loads(status_path.read_text(encoding="utf-8"))
    assert result["outcome"] == "committed" and durable["packages"][identity]["state"] == "raw_validated"
    assert durable["lease"] is None and durable["final_exit_state"] == "recovered_committed"


def test_lock_busy_refuses_before_schema_or_dispatch_and_rolls_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(_authorization(inventory_hash=str(inventory["inventory_hash"]))), encoding="utf-8")
    events: list[str] = []
    connection = _LockConnection(events, busy=True)
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: "nport:fixed-digest")
    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {"database": "sec_backfill_test", "server_address": "127.0.0.1", "role": "sec_backfill_test", "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27", "is_superuser": False, "owns_any_table": False, "writable_tables": ["sec_raw.nport_filings"]},
        schema_installers={"manifest": lambda _conn: events.append("manifest"), "nport": lambda _conn: events.append("form")},
        dispatchers={"nport": lambda *_args, **_kwargs: pytest.fail("busy lock must not dispatch")},
    )

    with pytest.raises(backfill.BackfillSafetyError, match="lock_busy"):
        executor(dict(inventory["packages"][0]))

    assert events == ["try_lock"]
    assert connection.rollbacks == 1
    assert connection.closed


def test_executor_commits_only_valid_explicit_failure_with_fixed_reason(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(_authorization(inventory_hash=str(inventory["inventory_hash"]))), encoding="utf-8")
    connection = _LockConnection([])
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    monkeypatch.setattr(backfill, "_derive_form_lock_key", lambda *_args: "nport:fixed-digest")
    executor = backfill.build_authorized_executor(
        path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: {"database": "sec_backfill_test", "server_address": "127.0.0.1", "role": "sec_backfill_test", "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27", "is_superuser": False, "owns_any_table": False, "writable_tables": ["sec_raw.nport_filings"]},
        schema_installers={"manifest": lambda _conn: None, "nport": lambda _conn: None},
        dispatchers={"nport": lambda _conn, *, package, source_root: {"package": package.relative_to(source_root).as_posix(), "state": "failed", "reason": "postgresql://user:secret@host/db"}},
    )

    assert executor(dict(inventory["packages"][0])) == {"state": "failed", "reason_code": "ingester_failed"}
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed


def test_supervisor_persists_fixed_refusal_code_and_nonsecret_error_digest(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"
    result = run_supervisor(
        inventory, status_path=status_path, code_sha="code-v1", lease_owner="unit-test",
        execute_package=lambda _package: {"state": "failed", "reason_code": "lock_busy", "dsn": "postgresql://fake:secret@localhost/test"},
    )

    assert result["reason"] == "lock_busy"
    record = json.loads(status_path.read_text(encoding="utf-8"))["packages"][inventory["packages"][0]["identity"]]
    assert record["reason_code"] == "lock_busy"
    assert len(record["error_digest"]) == 64
    assert "secret" not in status_path.read_text(encoding="utf-8")


def test_supervisor_distinguishes_executor_lock_refusal_from_source_drift(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    outcome = run_supervisor(
        inventory, status_path=tmp_path / "run" / "status.json", code_sha="code-v1", lease_owner="unit-test",
        execute_package=lambda _package: (_ for _ in ()).throw(BackfillSafetyError("lock_busy")),
    )

    assert outcome["reason"] == "lock_busy"


@pytest.mark.parametrize(
    "actual",
    (
        {"database": "market"},
        {"server_address": "10.0.0.1"},
        {"role": "postgres"},
        {"is_superuser": True},
        {"owns_any_table": True},
        {"writable_tables": ["sec_raw.nport_filings", "sec_current.provider_pointer"]},
    ),
)
def test_authorized_executor_refuses_identity_or_privilege_drift_before_schema_install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, actual: dict[str, object]) -> None:
    from src.sec_regulatory.historical_backfill import BackfillSafetyError, build_authorized_executor

    inventory, _ = _single_package_inventory(tmp_path)
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text(json.dumps(_authorization(inventory_hash=str(inventory["inventory_hash"]))), encoding="utf-8")
    connection = _Connection()
    monkeypatch.setenv("SEC_BACKFILL_FAKE_DSN", "postgresql://fake:secret@localhost/test")
    inspected = {
        "database": "sec_backfill_test", "server_address": "127.0.0.1", "role": "sec_backfill_test",
        "postgresql_identity": "PostgreSQL 18", "timescaledb_identity": "TimescaleDB 2.27",
        "is_superuser": False, "owns_any_table": False, "writable_tables": ["sec_raw.nport_filings"],
        **actual,
    }
    installed: list[str] = []
    executor = build_authorized_executor(
        authorization_path, inventory=inventory, code_sha="code-v1", connection_factory=lambda _dsn: connection,
        target_inspector=lambda _conn: inspected,
        schema_installers={"manifest": lambda _conn: installed.append("manifest"), "nport": lambda _conn: installed.append("nport")},
        dispatchers={"nport": lambda *_args, **_kwargs: pytest.fail("dispatch must not run")},
    )

    with pytest.raises(BackfillSafetyError, match="target|privilege|writable"):
        executor(dict(inventory["packages"][0]))

    assert installed == []
    assert connection.rollbacks == 1
    assert connection.closed


def test_cli_binds_explicit_authorization_to_status_and_rejects_resume_drift(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(backfill, "build_historical_inventory", lambda: inventory)
    monkeypatch.setattr(backfill, "_validate_historical_boundary", lambda _inventory: None)
    monkeypatch.setattr(backfill, "code_identity", lambda: "code-v1")

    class Executor:
        authorization_id = "auth-one"
        authorization_fingerprint = "a" * 64
        authorization_lineage = {"authorization_id": "auth-one", "sanitized_command": ["historical-backfill", "start"]}
        target_identity = {"kind": "local_disposable", "database": "sec_backfill_test"}

        def __call__(self, package: dict[str, object]) -> dict[str, object]:
            return {"package": package["relative_package_path"], "state": "raw_validated", "rows": 1}

    monkeypatch.setattr(backfill, "build_authorized_executor", lambda *_args, **_kwargs: Executor())
    assert backfill.cli(["start", "--run-dir", str(tmp_path / "run"), "--execution-authorization", str(authorization_path)]) == 0
    status = json.loads((tmp_path / "run" / "status.json").read_text(encoding="utf-8"))
    assert status["authorization_id"] == "auth-one"
    assert status["target_identity"] == {"kind": "local_disposable", "database": "sec_backfill_test"}

    class DriftedExecutor(Executor):
        authorization_id = "auth-two"

    monkeypatch.setattr(backfill, "build_authorized_executor", lambda *_args, **_kwargs: DriftedExecutor())
    with pytest.raises(backfill.BackfillSafetyError, match="authorization"):
        backfill.cli(["resume", "--run-dir", str(tmp_path / "run"), "--execution-authorization", str(authorization_path)])


def test_cli_lineage_persists_fingerprint_and_refuses_authorization_omission(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(backfill, "build_historical_inventory", lambda: inventory)
    monkeypatch.setattr(backfill, "_validate_historical_boundary", lambda _inventory: None)
    monkeypatch.setattr(backfill, "code_identity", lambda: "code-v1")
    seen: dict[str, object] = {}

    class Executor:
        authorization_id = "auth-one"
        authorization_fingerprint = "f" * 64
        authorization_lineage = {"authorization_id": "auth-one", "target_mode": "local_disposable"}
        target_identity = {"database": "sec_backfill_test"}

        def __call__(self, package: dict[str, object]) -> dict[str, object]:
            return {"package": package["relative_package_path"], "state": "raw_validated", "rows": 0, "run_id": "run-1"}

    def build(*_args: object, **kwargs: object) -> Executor:
        seen.update(kwargs)
        return Executor()

    monkeypatch.setattr(backfill, "build_authorized_executor", build)
    run_dir = tmp_path / "run"
    assert backfill.cli(["start", "--run-dir", str(run_dir), "--execution-authorization", str(authorization_path)]) == 0
    status = json.loads((run_dir / "status.json").read_text(encoding="utf-8"))
    assert status["authorization_fingerprint"] == "f" * 64
    assert status["packages"][inventory["packages"][0]["identity"]]["authorization_fingerprint"] == "f" * 64
    assert seen["run_directory"] == run_dir
    assert seen["command"] == ("historical-backfill", "start")

    with pytest.raises(backfill.BackfillSafetyError, match="authorization"):
        backfill.cli(["resume", "--run-dir", str(run_dir)])


def test_supervisor_heartbeats_a_blocking_authorized_executor_and_stops_after_completion(tmp_path: Path) -> None:
    from src.sec_regulatory.historical_backfill import run_supervisor

    inventory, _ = _single_package_inventory(tmp_path)
    status_path = tmp_path / "run" / "status.json"
    observed: list[tuple[str, str]] = []

    def blocking_executor(package: dict[str, object]) -> dict[str, object]:
        first = json.loads(status_path.read_text(encoding="utf-8"))["heartbeat_at"]
        time.sleep(0.05)
        observed.append((first, json.loads(status_path.read_text(encoding="utf-8"))["heartbeat_at"]))
        return {"package": package["relative_package_path"], "state": "raw_validated"}

    result = run_supervisor(
        inventory, status_path=status_path, code_sha="code-v1", execute_package=blocking_executor,
        lease_owner="unit-test", heartbeat_interval_seconds=0.01,
    )

    assert result["state"] == "ok"
    assert observed and observed[0][0] != observed[0][1]
    completed = json.loads(status_path.read_text(encoding="utf-8"))
    heartbeat_at_completion = completed["heartbeat_at"]
    time.sleep(0.03)
    assert json.loads(status_path.read_text(encoding="utf-8"))["heartbeat_at"] == heartbeat_at_completion


def test_authorized_cli_uses_sublease_heartbeat_and_surfaces_renewal_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.sec_regulatory.historical_backfill as backfill

    inventory, _ = _single_package_inventory(tmp_path)
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(backfill, "build_historical_inventory", lambda: inventory)
    monkeypatch.setattr(backfill, "_validate_historical_boundary", lambda _inventory: None)
    monkeypatch.setattr(backfill, "code_identity", lambda: "code-v1")
    monkeypatch.setattr(backfill, "AUTHORIZED_HEARTBEAT_INTERVAL_SECONDS", 0.01)

    class Executor:
        authorization_id = "auth-one"
        authorization_fingerprint = "f" * 64
        authorization_lineage = {"authorization_id": "auth-one"}
        target_identity = {"database": "sec_backfill_test"}

        def __call__(self, package: dict[str, object]) -> dict[str, object]:
            time.sleep(0.04)
            return {"package": package["relative_package_path"], "state": "raw_validated", "rows": 0, "run_id": "run-1"}

    monkeypatch.setattr(backfill, "build_authorized_executor", lambda *_args, **_kwargs: Executor())
    monkeypatch.setattr(backfill, "heartbeat", lambda *_args, **_kwargs: (_ for _ in ()).throw(backfill.BackfillSafetyError("renewal failure")))

    assert backfill.cli(["start", "--run-dir", str(tmp_path / "run"), "--execution-authorization", str(authorization_path)]) == 1
    status = json.loads((tmp_path / "run" / "status.json").read_text(encoding="utf-8"))
    assert status["packages"][inventory["packages"][0]["identity"]]["reason_code"] == "heartbeat_renewal_failed"
