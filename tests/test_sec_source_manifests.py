from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_sec_source_manifest_ddl_declares_the_required_shared_surfaces() -> None:
    ddl = (ROOT / "schemas" / "sec_source_manifests.sql").read_text(encoding="utf-8")

    for surface in (
        "sec_ingestion_runs",
        "sec_source_files",
        "sec_source_packages",
        "sec_table_reconciliations",
        "sec_row_issues",
        "sec_run_transitions",
        "sec_validated_raw_visibility",
    ):
        assert surface in ddl
    assert "CREATE EXTENSION" not in ddl


def test_sec_source_manifest_api_is_importable() -> None:
    from src.sec_regulatory import manifests

    for name in (
        "install_schema",
        "create_or_resume_run",
        "register_package_discovery",
        "retry_package_discovery",
        "register_file",
        "register_table_reconciliation",
        "record_issue",
        "transition_run",
        "fail_run",
        "retry_run",
        "validate_raw_run",
        "get_run_status",
        "is_raw_visible",
    ):
        assert callable(getattr(manifests, name))


def _test_dsn() -> str | None:
    dsn = os.getenv("SEC_TEST_DATABASE_URL")
    if not dsn:
        return None
    if "localhost" not in dsn and "127.0.0.1" not in dsn and os.getenv("SEC_TEST_ALLOW_REMOTE") != "1":
        pytest.fail("SEC_TEST_DATABASE_URL remoto requer SEC_TEST_ALLOW_REMOTE=1")
    return dsn


@pytest.mark.skipif(_test_dsn() is None, reason="SEC_TEST_DATABASE_URL ausente")
def test_real_db_schema_apply_is_idempotent() -> None:
    import psycopg
    from src.sec_regulatory.manifests import install_schema

    with psycopg.connect(_test_dsn()) as conn:
        install_schema(conn)
        install_schema(conn)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.sec_ingestion_runs')")
            assert cur.fetchone()[0] == "sec_ingestion_runs"


@pytest.fixture
def db_conn():
    dsn = _test_dsn()
    if dsn is None:
        pytest.skip("SEC_TEST_DATABASE_URL ausente")
    import psycopg
    from src.sec_regulatory.manifests import install_schema

    with psycopg.connect(dsn) as conn:
        install_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE sec_source_package_transitions, sec_source_packages, "
                "sec_row_issues, sec_table_reconciliations, sec_source_files, "
                "sec_validated_raw_visibility, sec_run_transitions, sec_ingestion_runs CASCADE"
            )
        conn.commit()
        yield conn
        conn.rollback()


def _run(conn, *, parser_version: str = "p1"):
    from src.sec_regulatory.manifests import create_or_resume_run

    return create_or_resume_run(
        conn,
        # Shared lifecycle tests intentionally use no installed family overlay.
        # N-PORT runs are validated end-to-end in test_nport_ingestion.py.
        source_family="manifest-test",
        package_sha256="a" * 64,
        parser_version=parser_version,
        source_quarter="2026Q2",
        package_relative_path="sec/nport/2026q2.zip",
    )


def _loading_run(conn, *, parser_version: str = "p1"):
    from src.sec_regulatory.manifests import transition_run

    run = _run(conn, parser_version=parser_version)
    run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
    return run


def _accounted_file(conn, run_id, *, expected: int = 0, source: int = 0, lexical: int = 0, typed: int = 0, quarantine: int = 0, rejected: int = 0, state: str = "accounted"):
    from src.sec_regulatory.manifests import register_file

    return register_file(
        conn,
        run_id=run_id,
        relative_path="tables/HOLDING.csv",
        sha256="b" * 64,
        byte_size=0,
        expected_count=expected,
        data_count=source,
        lexical_count=lexical,
        typed_success_count=typed,
        quarantine_count=quarantine,
        reject_count=rejected,
        state=state,
    )


def test_real_db_persists_idempotent_business_runs_and_new_parser_versions(db_conn) -> None:
    first = _run(db_conn)
    same = _run(db_conn)
    changed = _run(db_conn, parser_version="p2")
    db_conn.commit()

    assert first.run_id == same.run_id
    assert first.run_id != changed.run_id
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM sec_ingestion_runs")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT count(*) FROM sec_run_transitions WHERE event_type = 'created'")
        assert cur.fetchone()[0] == 2


def test_real_db_validates_exact_file_and_zero_row_table_accounting(db_conn) -> None:
    from src.sec_regulatory.manifests import (
        is_raw_visible,
        record_issue,
        register_table_reconciliation,
        validate_raw_run,
    )

    run = _loading_run(db_conn)
    file_id = _accounted_file(db_conn, run.run_id, expected=3, source=3, lexical=3, typed=2, quarantine=1)
    record_issue(
        db_conn,
        source_file_id=file_id,
        source_row_number=4,
        typed_error_code="invalid_date",
        status="quarantined",
    )
    register_table_reconciliation(
        db_conn,
        run_id=run.run_id,
        source_file_id=file_id,
        table_name="HOLDING",
        expected_count=0,
        source_count=0,
        lexical_count=0,
        typed_success_count=0,
    )
    assert is_raw_visible(db_conn, run_id=run.run_id) is False
    validated = validate_raw_run(db_conn, run_id=run.run_id)
    db_conn.commit()

    assert validated.current_state == "raw_validated"
    assert validated.raw_validated_at is not None
    assert is_raw_visible(db_conn, run_id=run.run_id) is True


def test_real_db_mismatched_counts_block_raw_validation_and_visibility(db_conn) -> None:
    from src.sec_regulatory.manifests import (
        RawValidationError,
        is_raw_visible,
        register_table_reconciliation,
        validate_raw_run,
    )

    run = _loading_run(db_conn)
    file_id = _accounted_file(db_conn, run.run_id, expected=2, source=2, lexical=2, typed=2)
    register_table_reconciliation(
        db_conn,
        run_id=run.run_id,
        source_file_id=file_id,
        table_name="SERIES",
        expected_count=2,
        source_count=2,
        lexical_count=2,
        typed_success_count=1,
    )

    with pytest.raises(RawValidationError):
        validate_raw_run(db_conn, run_id=run.run_id)
    assert is_raw_visible(db_conn, run_id=run.run_id) is False


def test_real_db_empty_or_not_accounted_manifests_block_raw_validation(db_conn) -> None:
    from src.sec_regulatory.manifests import (
        RawValidationError,
        register_table_reconciliation,
        validate_raw_run,
    )

    empty = _loading_run(db_conn)
    with pytest.raises(RawValidationError, match="arquivo"):
        validate_raw_run(db_conn, run_id=empty.run_id)

    run = _loading_run(db_conn, parser_version="p2")
    file_id = _accounted_file(db_conn, run.run_id, state="discovered")
    with pytest.raises(RawValidationError, match="accounted"):
        validate_raw_run(db_conn, run_id=run.run_id)

    _accounted_file(db_conn, run.run_id, state="loading")
    with pytest.raises(RawValidationError, match="accounted"):
        validate_raw_run(db_conn, run_id=run.run_id)

    _accounted_file(db_conn, run.run_id, state="accounted")
    register_table_reconciliation(
        db_conn,
        run_id=run.run_id,
        source_file_id=file_id,
        table_name="HOLDING",
        state="failed",
    )
    with pytest.raises(RawValidationError, match="accounted"):
        validate_raw_run(db_conn, run_id=run.run_id)


def test_real_db_raw_visibility_rolls_back_atomically_with_validation(db_conn) -> None:
    from src.sec_regulatory.manifests import is_raw_visible, validate_raw_run

    run = _loading_run(db_conn)
    _accounted_file(db_conn, run.run_id)
    db_conn.commit()
    try:
        with db_conn.transaction():
            validate_raw_run(db_conn, run_id=run.run_id)
            raise RuntimeError("simulated crash")
    except RuntimeError:
        pass

    assert is_raw_visible(db_conn, run_id=run.run_id) is False
    with db_conn.cursor() as cur:
        cur.execute("SELECT current_state, raw_validated_at FROM sec_ingestion_runs WHERE run_id = %s", (run.run_id,))
        assert cur.fetchone() == ("loading", None)


def test_real_db_raw_accounting_becomes_immutable_after_validation(db_conn) -> None:
    from src.sec_regulatory.manifests import record_issue, register_file, validate_raw_run

    run = _loading_run(db_conn)
    file_id = _accounted_file(db_conn, run.run_id)
    validate_raw_run(db_conn, run_id=run.run_id)
    db_conn.commit()

    with pytest.raises(Exception, match="immutable"):
        register_file(
            db_conn,
            run_id=run.run_id,
            relative_path="tables/HOLDING.csv",
            sha256="b" * 64,
            byte_size=0,
            expected_count=1,
        )
    db_conn.rollback()
    with db_conn.cursor() as cur:
        cur.execute("SELECT sha256, byte_size FROM sec_source_files WHERE source_file_id = %s", (file_id,))
        assert cur.fetchone() == ("b" * 64, 0)

    with pytest.raises(Exception, match="immutable"):
        record_issue(
            db_conn,
            source_file_id=file_id,
            source_row_number=1,
            typed_error_code="bad_date",
            status="quarantined",
        )
    db_conn.rollback()


def test_real_db_raw_validation_timestamp_is_irreversible_and_keeps_accounting_frozen(db_conn) -> None:
    from src.sec_regulatory.manifests import register_file, validate_raw_run

    run = _loading_run(db_conn)
    file_id = _accounted_file(db_conn, run.run_id)
    validate_raw_run(db_conn, run_id=run.run_id)
    db_conn.commit()

    with pytest.raises(Exception, match="irreversible"):
        with db_conn.cursor() as cur:
            cur.execute("UPDATE sec_ingestion_runs SET raw_validated_at = NULL WHERE run_id = %s", (run.run_id,))
    db_conn.rollback()
    with pytest.raises(Exception, match="immutable"):
        register_file(
            db_conn,
            run_id=run.run_id,
            relative_path="tables/new.csv",
            sha256="c" * 64,
            byte_size=1,
        )
    db_conn.rollback()
    with db_conn.cursor() as cur:
        cur.execute("SELECT sha256, byte_size FROM sec_source_files WHERE source_file_id = %s", (file_id,))
        assert cur.fetchone() == ("b" * 64, 0)


def test_real_db_failed_retry_is_audited_and_expected_state_is_concurrency_safe(db_conn) -> None:
    from src.sec_regulatory.manifests import ManifestStateError, fail_run, retry_run, transition_run

    run = _run(db_conn)
    with pytest.raises(ManifestStateError):
        transition_run(db_conn, run_id=run.run_id, expected_state="loading", target_state="derived_building")
    loaded = transition_run(
        db_conn, run_id=run.run_id, expected_state="discovered", target_state="loading", detail="começar carga"
    )
    failed = fail_run(db_conn, run_id=loaded.run_id, expected_state="loading", failure_code="network")
    retried = retry_run(db_conn, run_id=failed.run_id, detail="retry 1")
    db_conn.commit()

    assert failed.current_state == "failed"
    assert retried.current_state == "loading"
    assert retried.retry_count == 1
    with db_conn.cursor() as cur:
        cur.execute("SELECT from_state, to_state, event_type, detail FROM sec_run_transitions WHERE run_id = %s ORDER BY transition_id", (run.run_id,))
        transitions = cur.fetchall()
        assert ("discovered", "loading", "transition", "começar carga") in transitions
        assert ("loading", "failed", "failed", "network") in transitions
        assert ("failed", "loading", "retry", "retry 1") in transitions


def test_real_db_published_run_is_terminal(db_conn) -> None:
    from src.sec_regulatory.manifests import ManifestStateError, transition_run, validate_raw_run

    run = _loading_run(db_conn)
    _accounted_file(db_conn, run.run_id)
    raw = validate_raw_run(db_conn, run_id=run.run_id)
    building = transition_run(db_conn, run_id=raw.run_id, expected_state="raw_validated", target_state="derived_building")
    derived = transition_run(db_conn, run_id=building.run_id, expected_state="derived_building", target_state="derived_validated")
    published = transition_run(db_conn, run_id=derived.run_id, expected_state="derived_validated", target_state="published")
    db_conn.commit()

    assert published.published_at is not None
    with pytest.raises(ManifestStateError):
        transition_run(db_conn, run_id=published.run_id, expected_state="published", target_state="loading")
    with pytest.raises(Exception, match="terminal"):
        with db_conn.cursor() as cur:
            cur.execute("UPDATE sec_ingestion_runs SET failure_code = 'bad' WHERE run_id = %s", (published.run_id,))
    db_conn.rollback()


def test_real_db_stale_expected_state_rejects_second_connection_transition(db_conn) -> None:
    import psycopg
    from src.sec_regulatory.manifests import ManifestStateError, transition_run

    run = _run(db_conn)
    db_conn.commit()
    with psycopg.connect(_test_dsn()) as second:
        first_result = transition_run(
            db_conn,
            run_id=run.run_id,
            expected_state="discovered",
            target_state="loading",
        )
        db_conn.commit()
        assert first_result.current_state == "loading"
        with pytest.raises(ManifestStateError, match="estado esperado"):
            transition_run(
                second,
                run_id=run.run_id,
                expected_state="discovered",
                target_state="loading",
            )


def test_real_db_idempotent_run_rejects_conflicting_business_metadata(db_conn) -> None:
    from src.sec_regulatory.manifests import ManifestStateError, create_or_resume_run

    _run(db_conn)
    with pytest.raises(ManifestStateError, match="metadados"):
        create_or_resume_run(
            db_conn,
            source_family="manifest-test",
            package_sha256="a" * 64,
            parser_version="p1",
            source_quarter="2026Q1",
            package_relative_path="sec/nport/other.zip",
        )


def test_real_db_file_content_drift_and_cross_run_table_registration_are_rejected(db_conn) -> None:
    from src.sec_regulatory.manifests import (
        ManifestStateError,
        register_file,
        register_table_reconciliation,
    )

    first = _loading_run(db_conn)
    file_id = _accounted_file(db_conn, first.run_id)
    resumed_id = register_file(
        db_conn,
        run_id=first.run_id,
        relative_path="tables/HOLDING.csv",
        sha256="b" * 64,
        byte_size=0,
        schema_metadata={"version": 2},
    )
    assert resumed_id == file_id
    db_conn.commit()
    with pytest.raises(ManifestStateError, match="conteúdo conflitante"):
        register_file(
            db_conn,
            run_id=first.run_id,
            relative_path="tables/HOLDING.csv",
            sha256="c" * 64,
            byte_size=0,
        )

    other = _run(db_conn, parser_version="p2")
    with pytest.raises(ManifestStateError, match="não pertence"):
        register_table_reconciliation(
            db_conn,
            run_id=other.run_id,
            source_file_id=file_id,
            table_name="HOLDING",
        )


def test_real_db_package_discovery_keeps_duplicate_paths_distinct(db_conn) -> None:
    from src.sec_regulatory.manifests import register_package_discovery

    original = register_package_discovery(
        db_conn,
        source_family="nport",
        source_quarter="2026Q2",
        package_relative_path="packages/2026q2/original",
        package_state="discovered",
        package_sha256="d" * 64,
    )
    duplicate = register_package_discovery(
        db_conn,
        source_family="nport",
        source_quarter="2026Q2",
        package_relative_path="packages/2026q2/copy",
        package_state="duplicate",
        package_sha256="d" * 64,
        reason="same SEC bytes",
        duplicate_of_package_id=original.package_id,
    )
    db_conn.commit()

    assert duplicate.package_id != original.package_id
    assert duplicate.duplicate_of_package_id == original.package_id
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM sec_source_packages WHERE package_sha256 = %s", ("d" * 64,))
        assert cur.fetchone()[0] == 2


def test_real_db_package_discovery_is_idempotent_monotonic_and_keeps_immutable_metadata(db_conn) -> None:
    from src.sec_regulatory.manifests import ManifestStateError, register_package_discovery

    run = _run(db_conn)
    first = register_package_discovery(
        db_conn, source_family="nport", source_quarter="2026Q2",
        package_relative_path="packages/2026q2/monotonic", package_state="discovered",
        package_sha256="a" * 64, metadata_sha256="b" * 64,
    )
    identical = register_package_discovery(
        db_conn, source_family="nport", source_quarter="2026Q2",
        package_relative_path="packages/2026q2/monotonic", package_state="discovered",
        package_sha256="a" * 64, metadata_sha256="b" * 64,
    )
    loaded = register_package_discovery(
        db_conn, source_family="nport", source_quarter="2026Q2",
        package_relative_path="packages/2026q2/monotonic", package_state="loaded", run_id=run.run_id,
    )
    assert identical.package_id == first.package_id
    assert loaded.run_id == run.run_id
    db_conn.commit()
    with pytest.raises(Exception, match="invalid package discovery transition"):
        register_package_discovery(
            db_conn, source_family="nport", source_quarter="2026Q2",
            package_relative_path="packages/2026q2/monotonic", package_state="discovered",
        )
    db_conn.rollback()
    with pytest.raises(Exception, match="immutable metadata conflict"):
        register_package_discovery(
            db_conn, source_family="nport", source_quarter="2026Q1",
            package_relative_path="packages/2026q2/monotonic", package_state="loaded",
            package_sha256="c" * 64, run_id=run.run_id,
        )


def test_real_db_package_terminal_reason_and_duplicate_link_are_immutable(db_conn) -> None:
    from src.sec_regulatory.manifests import register_package_discovery

    original = register_package_discovery(
        db_conn, source_family="nport", source_quarter="2026Q2",
        package_relative_path="packages/original-terminal", package_state="discovered",
    )
    duplicate = register_package_discovery(
        db_conn, source_family="nport", source_quarter="2026Q2",
        package_relative_path="packages/duplicate-terminal", package_state="duplicate",
        reason="same bytes", duplicate_of_package_id=original.package_id,
    )
    db_conn.commit()
    with pytest.raises(Exception, match="immutable metadata conflict"):
        register_package_discovery(
            db_conn, source_family="nport", source_quarter="2026Q2",
            package_relative_path="packages/duplicate-terminal", package_state="duplicate",
            reason="changed explanation", duplicate_of_package_id=uuid4(),
        )
    db_conn.rollback()
    assert duplicate.duplicate_of_package_id == original.package_id


def test_real_db_explicit_package_retry_retains_evidence_and_loaded_remains_terminal(db_conn) -> None:
    from src.sec_regulatory.manifests import (
        get_run_status,
        retry_package_discovery,
        register_package_discovery,
    )

    old_run = _run(db_conn, parser_version="old-retry-package")
    failed = register_package_discovery(
        db_conn, source_family="nport", source_quarter="2026Q2",
        package_relative_path="packages/retryable", package_state="failed",
        package_sha256="a" * 64, metadata_sha256="b" * 64, reason="parser defect", run_id=old_run.run_id,
    )
    db_conn.commit()
    with pytest.raises(Exception, match="invalid package discovery transition"):
        register_package_discovery(
            db_conn, source_family="nport", source_quarter="2026Q2",
            package_relative_path="packages/retryable", package_state="discovered",
        )
    db_conn.rollback()
    retried = retry_package_discovery(
        db_conn, source_family="nport", package_relative_path="packages/retryable"
    )
    assert retried.package_state == "discovered"
    assert retried.retry_count == 1
    assert retried.package_sha256 == failed.package_sha256
    assert retried.reason is None
    assert retried.run_id is None
    assert get_run_status(db_conn, run_id=old_run.run_id) is not None
    with db_conn.cursor() as cur:
        cur.execute(
            """SELECT from_state, to_state, terminal_reason FROM sec_source_package_transitions
               WHERE package_id = %s ORDER BY package_transition_id DESC LIMIT 1""",
            (failed.package_id,),
        )
        assert cur.fetchone() == ("failed", "discovered", "parser defect")
    run = _run(db_conn, parser_version="retry-package")
    loaded = register_package_discovery(
        db_conn, source_family="nport", source_quarter="2026Q2",
        package_relative_path="packages/retryable", package_state="loaded", run_id=run.run_id,
    )
    assert loaded.retry_count == 1
    assert loaded.run_id == run.run_id
    db_conn.commit()
    with pytest.raises(Exception, match="somente pacote"):
        retry_package_discovery(
            db_conn, source_family="nport", package_relative_path="packages/retryable"
        )


def test_real_db_concurrent_package_retry_cannot_double_increment(db_conn) -> None:
    import psycopg
    from src.sec_regulatory.manifests import retry_package_discovery, register_package_discovery

    register_package_discovery(
        db_conn, source_family="nport", source_quarter="2026Q2",
        package_relative_path="packages/retry-race", package_state="quarantined",
        package_sha256="c" * 64, reason="bad date",
    )
    db_conn.commit()
    with psycopg.connect(_test_dsn()) as first, psycopg.connect(_test_dsn()) as second:
        retry_package_discovery(first, source_family="nport", package_relative_path="packages/retry-race")
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(
                retry_package_discovery, second,
                source_family="nport", package_relative_path="packages/retry-race",
            )
            first.commit()
            with pytest.raises(Exception, match="somente pacote"):
                pending.result(timeout=5)
        second.rollback()
    with db_conn.cursor() as cur:
        cur.execute("SELECT package_state, retry_count FROM sec_source_packages WHERE package_relative_path = 'packages/retry-race'")
        assert cur.fetchone() == ("discovered", 1)


def test_real_db_table_issue_counts_are_reconciled_per_declared_table(db_conn) -> None:
    from src.sec_regulatory.manifests import (
        RawValidationError,
        record_issue,
        register_table_reconciliation,
        validate_raw_run,
    )

    run = _loading_run(db_conn, parser_version="p8")
    file_id = _accounted_file(
        db_conn, run.run_id, expected=1, source=1, lexical=1, quarantine=1
    )
    register_table_reconciliation(db_conn, run_id=run.run_id, source_file_id=file_id, table_name="A")
    register_table_reconciliation(
        db_conn, run_id=run.run_id, source_file_id=file_id, table_name="B",
        expected_count=1, source_count=1, lexical_count=1, quarantine_count=1,
    )
    record_issue(db_conn, source_file_id=file_id, source_row_number=1, table_name="A",
                 typed_error_code="bad_date", status="quarantined")
    with pytest.raises(Exception, match="raw reconciliation failed"):
        validate_raw_run(db_conn, run_id=run.run_id)


def test_real_db_validation_function_sees_writer_changes_after_run_lock_wait(db_conn) -> None:
    import psycopg

    run = _loading_run(db_conn, parser_version="p9")
    file_id = _accounted_file(db_conn, run.run_id)
    db_conn.commit()
    with psycopg.connect(_test_dsn()) as writer, psycopg.connect(_test_dsn()) as validator:
        with writer.cursor() as cur:
            cur.execute("SELECT 1 FROM sec_ingestion_runs WHERE run_id = %s FOR UPDATE", (run.run_id,))
        with ThreadPoolExecutor(max_workers=1) as pool:
            def direct_validate() -> None:
                with validator.cursor() as cur:
                    cur.execute("SELECT sec_validate_raw_run(%s)", (run.run_id,))

            pending = pool.submit(direct_validate)
            with writer.cursor() as cur:
                cur.execute(
                    """INSERT INTO sec_table_reconciliations
                       (run_id, source_file_id, table_name, state)
                       VALUES (%s, %s, 'late_bad_table', 'loading')""",
                    (run.run_id, file_id),
                )
            writer.commit()
            with pytest.raises(Exception, match="raw reconciliation failed"):
                pending.result(timeout=5)
        validator.rollback()


def test_real_db_nonowner_cannot_spoof_raw_validation_authorization(db_conn) -> None:
    import psycopg

    with db_conn.cursor() as cur:
        cur.execute(
            """
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sec_manifest_app') THEN
                    CREATE ROLE sec_manifest_app LOGIN PASSWORD 'sec_manifest_app';
                END IF;
            END $$;
            GRANT USAGE ON SCHEMA public TO sec_manifest_app;
            GRANT SELECT, UPDATE ON sec_ingestion_runs TO sec_manifest_app;
            GRANT EXECUTE ON FUNCTION sec_validate_raw_run(uuid, text) TO sec_manifest_app;
            """
        )
    run = _loading_run(db_conn, parser_version="p10")
    db_conn.commit()
    app_dsn = _test_dsn().replace("postgres:sec_test", "sec_manifest_app:sec_manifest_app")
    with psycopg.connect(app_dsn) as app:
        with app.cursor() as cur:
            cur.execute("SELECT set_config('sec.authorized_raw_validation', '1', true)")
            with pytest.raises(Exception, match="invalid lifecycle"):
                cur.execute(
                    "UPDATE sec_ingestion_runs SET current_state = 'raw_validated', raw_validated_at = now() WHERE run_id = %s",
                    (run.run_id,),
                )
        app.rollback()
        with app.cursor() as cur:
            with pytest.raises(Exception, match="raw reconciliation failed"):
                cur.execute("SELECT sec_validate_raw_run(%s, 'app validation')", (run.run_id,))
        app.rollback()
        _accounted_file(db_conn, run.run_id)
        db_conn.commit()
        with app.cursor() as cur:
            cur.execute("SELECT sec_validate_raw_run(%s, 'app validation')", (run.run_id,))
            assert cur.fetchone()[0] is not None
        app.commit()
    with db_conn.cursor() as cur:
        cur.execute("SELECT current_state FROM sec_ingestion_runs WHERE run_id = %s", (run.run_id,))
        assert cur.fetchone()[0] == "raw_validated"


def test_real_db_lifecycle_guard_blocks_direct_raw_bypass_and_auto_audits(db_conn) -> None:
    from src.sec_regulatory.manifests import validate_raw_run

    run = _loading_run(db_conn)
    db_conn.commit()
    with pytest.raises(Exception, match="invalid lifecycle"):
        with db_conn.cursor() as cur:
            cur.execute(
                "UPDATE sec_ingestion_runs SET current_state = 'raw_validated', raw_validated_at = now() WHERE run_id = %s",
                (run.run_id,),
            )
    db_conn.rollback()
    _accounted_file(db_conn, run.run_id)
    validate_raw_run(db_conn, run_id=run.run_id)
    db_conn.commit()
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM sec_run_transitions WHERE run_id = %s AND event_type = 'raw_validated'", (run.run_id,))
        assert cur.fetchone()[0] == 1
    with pytest.raises(Exception, match="identity is immutable"):
        with db_conn.cursor() as cur:
            cur.execute("UPDATE sec_ingestion_runs SET source_quarter = '2026Q1' WHERE run_id = %s", (run.run_id,))
    db_conn.rollback()


def test_real_db_path_constraints_reject_absolute_drive_backslash_dot_and_empty_paths(db_conn) -> None:
    bad_paths = ("", "/absolute/path", "C:/drive/path", "a/../b", "./relative", r"unc\path")
    for index, path in enumerate(bad_paths):
        with pytest.raises(Exception):
            with db_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO sec_ingestion_runs
                       (run_id, source_family, package_sha256, parser_version, source_quarter, package_relative_path)
                       VALUES (%s, 'nport', %s, %s, '2026Q2', %s)""",
                    (uuid4(), "e" * 64, f"bad-run-{index}", path),
                )
        db_conn.rollback()

    run = _run(db_conn)
    db_conn.commit()
    for path in bad_paths:
        with pytest.raises(Exception):
            with db_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO sec_source_files (source_file_id, run_id, relative_path, sha256, byte_size)
                       VALUES (%s, %s, %s, %s, 0)""",
                    (uuid4(), run.run_id, path, "f" * 64),
                )
        db_conn.rollback()
        with pytest.raises(Exception):
            with db_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO sec_source_packages
                       (package_id, source_family, source_quarter, package_relative_path, package_state)
                       VALUES (%s, 'nport', '2026Q2', %s, 'discovered')""",
                    (uuid4(), path),
                )
        db_conn.rollback()


def test_real_db_multiple_issue_details_count_once_and_mixed_dispositions_fail(db_conn) -> None:
    from src.sec_regulatory.manifests import RawValidationError, record_issue, validate_raw_run

    good = _loading_run(db_conn, parser_version="p3")
    good_file = _accounted_file(
        db_conn, good.run_id, expected=1, source=1, lexical=1, quarantine=1
    )
    record_issue(db_conn, source_file_id=good_file, source_row_number=7, issue_sequence=1,
                 typed_error_code="invalid_date", status="quarantined")
    record_issue(db_conn, source_file_id=good_file, source_row_number=7, issue_sequence=2,
                 typed_error_code="invalid_timezone", status="quarantined")
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'advisory'")
        assert cur.fetchone()[0] == 1
    assert validate_raw_run(db_conn, run_id=good.run_id).current_state == "raw_validated"

    mixed = _loading_run(db_conn, parser_version="p4")
    mixed_file = _accounted_file(
        db_conn, mixed.run_id, expected=1, source=1, lexical=1, quarantine=1
    )
    record_issue(db_conn, source_file_id=mixed_file, source_row_number=8,
                 typed_error_code="invalid_date", status="quarantined")
    with pytest.raises(Exception, match="both quarantined and rejected"):
        record_issue(db_conn, source_file_id=mixed_file, source_row_number=8, issue_sequence=2,
                     typed_error_code="bad_code", status="rejected")
    db_conn.rollback()


def test_real_db_validated_run_blocks_direct_move_and_concurrent_file_mutation(db_conn) -> None:
    import psycopg
    from src.sec_regulatory.manifests import register_file, validate_raw_run

    run = _loading_run(db_conn, parser_version="p5")
    file_id = _accounted_file(db_conn, run.run_id)
    other = _run(db_conn, parser_version="p6")
    db_conn.commit()
    with psycopg.connect(_test_dsn()) as validator, psycopg.connect(_test_dsn()) as writer:
        validate_raw_run(validator, run_id=run.run_id)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(
                register_file,
                writer,
                run_id=run.run_id,
                relative_path="tables/race.csv",
                sha256="c" * 64,
                byte_size=1,
            )
            validator.commit()
            with pytest.raises(Exception, match="immutable"):
                pending.result(timeout=5)
        writer.rollback()
    with pytest.raises(Exception, match="immutable"):
        with db_conn.cursor() as cur:
            cur.execute("UPDATE sec_source_files SET run_id = %s WHERE source_file_id = %s", (other.run_id, file_id))
    db_conn.rollback()


def test_real_db_concurrent_file_content_conflict_does_not_overwrite_first_writer(db_conn) -> None:
    import psycopg
    from src.sec_regulatory.manifests import ManifestStateError, register_file

    run = _loading_run(db_conn, parser_version="p7")
    db_conn.commit()
    with psycopg.connect(_test_dsn()) as first, psycopg.connect(_test_dsn()) as second:
        register_file(
            first, run_id=run.run_id, relative_path="tables/concurrent.csv", sha256="1" * 64, byte_size=1
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(
                register_file,
                second,
                run_id=run.run_id,
                relative_path="tables/concurrent.csv",
                sha256="2" * 64,
                byte_size=2,
            )
            first.commit()
            with pytest.raises(ManifestStateError, match="conteúdo conflitante"):
                pending.result(timeout=5)
        second.rollback()
    with db_conn.cursor() as cur:
        cur.execute("SELECT sha256, byte_size FROM sec_source_files WHERE run_id = %s", (run.run_id,))
        assert cur.fetchone() == ("1" * 64, 1)


def test_real_db_issue_counts_must_reconcile_but_nonzero_quarantine_is_allowed(db_conn) -> None:
    from src.sec_regulatory.manifests import RawValidationError, record_issue, validate_raw_run

    blocked = _loading_run(db_conn)
    blocked_file = _accounted_file(
        db_conn,
        blocked.run_id,
        expected=2,
        source=2,
        lexical=2,
        typed=1,
        quarantine=1,
    )
    with pytest.raises(RawValidationError, match="issues"):
        validate_raw_run(db_conn, run_id=blocked.run_id)

    valid = _loading_run(db_conn, parser_version="p2")
    valid_file = _accounted_file(
        db_conn,
        valid.run_id,
        expected=2,
        source=2,
        lexical=2,
        typed=1,
        quarantine=1,
    )
    record_issue(
        db_conn,
        source_file_id=valid_file,
        source_row_number=2,
        typed_error_code="invalid_date",
        status="quarantined",
    )
    result = validate_raw_run(db_conn, run_id=valid.run_id)
    assert result.current_state == "raw_validated"
