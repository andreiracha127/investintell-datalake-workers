"""Contratos de ingestao bruta N-PORT."""

import hashlib
import json
import inspect
import os
from dataclasses import replace
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolated_nport_database() -> None:
    """O banco SEC descartável não deixa runs/pacotes N-PORT entre casos."""
    dsn = os.getenv("SEC_TEST_DATABASE_URL")
    if not dsn:
        yield
        return
    import psycopg
    from src.nport.storage import install_schema as install_nport_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    with psycopg.connect(dsn) as conn:
        install_manifest_schema(conn)
        install_nport_schema(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE nport_raw_rows, nport_holding_accession_map, sec_source_packages, sec_ingestion_runs CASCADE")
        conn.commit()
    yield


def test_nport_contract_is_closed_and_has_thirty_tables() -> None:
    from src.nport.schema import load_nport_contract

    contract = load_nport_contract()

    assert contract.family == "nport"
    assert len(contract.tables) == 30
    assert "SUBMISSION.tsv" in contract.required_filenames
    assert "FUND_VAR_INFO.tsv" in contract.required_filenames
    assert contract.table_for_filename("SUBMISSION.tsv").raw_target == "nport_submission_raw"


def test_streamer_does_not_need_full_file_reads(tmp_path: Path) -> None:
    from src.sec_regulatory.tsv import stream_tsv

    path = tmp_path / "one.tsv"
    path.write_text("A\tB\n1\t2\n", encoding="utf-8")

    header, rows = stream_tsv(path)

    assert header == ("A", "B")
    assert list(rows) == [(2, ("1", "2"))]


def test_declared_optional_tsv_is_zero_row_but_submission_is_foundational(tmp_path: Path) -> None:
    from src.nport.schema import load_nport_contract, verify_package
    from src.sec_regulatory.contracts import ContractError

    contract = _synthetic_contract(tmp_path)
    submission = contract.table_for_filename("SUBMISSION.tsv")
    (tmp_path / "SUBMISSION.tsv").write_text("\t".join(submission.headers) + "\n", encoding="utf-8")

    verified = verify_package(tmp_path, contract)
    assert set(verified.file_hashes) == {"SUBMISSION.tsv"}

    (tmp_path / "SUBMISSION.tsv").unlink()
    with pytest.raises(ContractError, match="SUBMISSION"):
        verify_package(tmp_path, contract)


def test_unknown_file_and_header_drift_fail_before_rows_are_parsed(tmp_path: Path) -> None:
    from src.nport.schema import verify_package
    from src.sec_regulatory.contracts import ContractError

    contract = _synthetic_contract(tmp_path)
    submission = contract.table_for_filename("SUBMISSION.tsv")
    (tmp_path / "SUBMISSION.tsv").write_text("\t".join(reversed(submission.headers)) + "\n", encoding="utf-8")
    with pytest.raises(ContractError, match="cabeçalho"):
        verify_package(tmp_path, contract)

    (tmp_path / "SUBMISSION.tsv").write_text("\t".join(submission.headers) + "\n", encoding="utf-8")
    (tmp_path / "UNDECLARED.tsv").write_text("X\n", encoding="utf-8")
    with pytest.raises(ContractError, match="unknown"):
        verify_package(tmp_path, contract)


def test_invalid_decimal_and_date_are_quarantined_with_lexical_evidence() -> None:
    from src.nport.schema import load_nport_contract, parse_row

    table = load_nport_contract().table_for_filename("DEBT_SECURITY.tsv")
    values = tuple("not-a-date" if column.name == "MATURITY_DATE" else "not-a-decimal" if column.name == "ANNUALIZED_RATE" else "holding-1"
                   for column in table.columns)
    parsed = parse_row(table.columns, values)

    assert parsed.parse_status == "quarantined"
    assert {"invalid_date", "invalid_decimal"} <= {issue.code for issue in parsed.issues}
    assert parsed.lexical["MATURITY_DATE"] == "not-a-date"


def test_raw_ddl_exposes_every_contract_target_and_immutability_guard() -> None:
    from src.nport.schema import load_nport_contract

    ddl = Path("schemas/nport_raw.sql").read_text(encoding="utf-8")

    assert "nport_raw_rows" in ddl
    assert "nport_lock_raw_insert_statement" in ddl
    for table in load_nport_contract().tables:
        assert table.raw_target in ddl


def test_nport_catalog_is_exact_contract_projection() -> None:
    """The database gate must retain the frozen table contract, not just names."""
    import json

    contract_path = Path("contracts/sec-regulatory/v1/source-tables/nport.json")
    frozen = json.loads(contract_path.read_text(encoding="utf-8"))["schema_variants"][0]["tables"]
    ddl = Path("schemas/nport_raw.sql").read_text(encoding="utf-8")

    assert "source_table text PRIMARY KEY" in ddl
    assert "ADD COLUMN IF NOT EXISTS raw_target text" in ddl
    assert "ADD COLUMN IF NOT EXISTS logical_parents text[]" in ddl
    assert "ADD COLUMN IF NOT EXISTS candidate_key text[]" in ddl
    assert "ALTER COLUMN raw_target SET NOT NULL" in ddl
    for table in frozen:
        assert table["source_file"] in ddl
        assert table["columns"][0]["raw_target"] in ddl
        for column in table["candidate_primary_key"]:
            assert column in ddl


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_real_db_nport_catalog_is_frozen_exact_contract_projection() -> None:
    import json
    import psycopg

    frozen = json.loads(Path("contracts/sec-regulatory/v1/source-tables/nport.json").read_text(encoding="utf-8"))["schema_variants"][0]["tables"]
    expected = {
        (
            ordinal, table["source_file"], table["columns"][0]["raw_target"],
            tuple(table["logical_parents"]), tuple(table["candidate_primary_key"]),
            tuple(column["name"] for column in table["columns"]),
            tuple(column["name"] for column in table["columns"] if column["required"]),
            tuple(
                json.dumps({
                    "name": column["name"], "parsing_policy": column["parsing_policy"],
                    "required": column["required"], "datatype": column["datatype"],
                }, sort_keys=True)
                for column in table["columns"]
            ),
        )
        for ordinal, table in enumerate(frozen, start=1)
    }
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT table_ordinal, source_table, raw_target, logical_parents, candidate_key,
                          columns, required_columns, column_specs FROM nport_contract_tables"""
            )
            actual = {
                (ordinal, source_table, raw_target, tuple(parents), tuple(key), tuple(columns), tuple(required),
                 tuple(json.dumps(spec, sort_keys=True) for spec in specs))
                for ordinal, source_table, raw_target, parents, key, columns, required, specs in cur.fetchall()
            }
            assert actual == expected
            cur.execute("SELECT nport_contract_catalog_sha256()")
            assert cur.fetchone()[0] == "688dc665eced0360b812e9b119c7e8fcd40c444cbdbe7a745dcdca90e662c896"
            with pytest.raises(psycopg.Error, match="immutable"):
                cur.execute("DELETE FROM nport_contract_tables WHERE source_table = 'SUBMISSION.tsv'")
            conn.rollback()
            cur.execute("SELECT tgname FROM pg_trigger WHERE tgrelid = 'nport_raw_rows'::regclass AND NOT tgisinternal AND (tgtype & 1) = 1")
            assert cur.fetchall() == []


def test_copy_and_tsv_code_paths_are_bounded_streaming_primitives() -> None:
    from src.nport.ingestion import _insert_rows
    from src.nport.schema import verify_package
    from src.sec_regulatory.tsv import stream_tsv

    assert "cur.copy" in inspect.getsource(_insert_rows)
    assert "executemany" not in inspect.getsource(_insert_rows)
    assert ".read_text" not in inspect.getsource(stream_tsv)
    assert ".read_text" not in inspect.getsource(verify_package)


def test_publication_reconciler_uses_run_wide_summaries_and_one_semantic_scan() -> None:
    """The publication gate must not return to the 40m-row correlated plan."""
    ddl = Path("schemas/nport_raw.sql").read_text(encoding="utf-8")
    body = ddl.split(
        "CREATE OR REPLACE FUNCTION nport_raw_run_reconciles(target_run_id uuid)", 1,
    )[1].split("DROP TRIGGER IF EXISTS nport_raw_publication_gate", 1)[0]

    assert "LEFT JOIN LATERAL (\n      SELECT count(DISTINCT r.raw_row_id)" not in body
    assert "issue_rows AS MATERIALIZED" in body
    assert "raw_actuals AS MATERIALIZED" in body
    assert "semantic_and_disposition" in body
    assert body.count("nport_expected_row(") == 1
    assert "jsonb_to_record(nport_expected_row(" in body
    assert "holding_rows AS MATERIALIZED" in body


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_real_db_zero_tables_are_accounted_and_validated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.nport.ingestion as ingestion
    from src.nport.storage import install_schema as install_nport_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    contract, package = _package(tmp_path, "2026q4_nport", accession="NPORT-ZERO-TEST")
    old = ingestion.load_nport_contract
    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)
    try:
        with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
            install_manifest_schema(conn)
            install_nport_schema(conn)
            result = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="nport-test-zero-v1")
            conn.commit()
            assert result["state"] == "raw_validated"
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM sec_table_reconciliations WHERE run_id = %s", (result["run_id"],))
                assert cur.fetchone()[0] == 30
                cur.execute("SELECT count(*) FROM nport_submission_raw WHERE ingestion_run_id = %s", (result["run_id"],))
                assert cur.fetchone()[0] == 1
                with pytest.raises(psycopg.Error):
                    cur.execute("UPDATE nport_raw_rows SET parser_version = 'mutated' WHERE ingestion_run_id = %s", (result["run_id"],))
    finally:
        monkeypatch.setattr(ingestion, "load_nport_contract", old)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_real_db_orphan_holding_id_fails_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.nport.ingestion as ingestion
    from src.nport.storage import install_schema as install_nport_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    contract, package = _package(tmp_path, "2027q3_nport", accession="NPORT-ORPHAN-TEST")
    holding = contract.table_for_filename("FUND_REPORTED_HOLDING.tsv")
    identifiers = contract.table_for_filename("DESC_REF_INDEX_COMPONENT.tsv")
    _write_row(package / holding.source_file, holding.headers, {"ACCESSION_NUMBER": "NPORT-ORPHAN-TEST", "HOLDING_ID": "101"})
    _write_row(package / identifiers.source_file, identifiers.headers, {
        "HOLDING_ID": "999", "DESC_REF_INDEX_COMPONENT_ID": "1", "NOTIONAL_AMOUNT": "not-a-decimal",
    })
    old = ingestion.load_nport_contract
    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)
    try:
        with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
            install_manifest_schema(conn)
            install_nport_schema(conn)
            result = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="nport-test-orphan-v1")
            conn.commit()
            assert result["state"] == "failed"
            assert "órfão" in str(result["reason"])
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT current_state, raw_validated_at,
                              (SELECT count(*) FROM nport_holding_accession_map
                               WHERE ingestion_run_id=sec_ingestion_runs.run_id)
                       FROM sec_ingestion_runs WHERE run_id=%s""",
                    (result["run_id"],),
                )
                assert cur.fetchone() == ("failed", None, 0)
    finally:
        monkeypatch.setattr(ingestion, "load_nport_contract", old)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_real_db_ambiguous_holding_id_fails_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.nport.ingestion as ingestion

    contract, package = _package(tmp_path, "2027q2_nport", accession="NPORT-AMBIG-ONE")
    holding = contract.table_for_filename("FUND_REPORTED_HOLDING.tsv")
    _write_rows(package / holding.source_file, holding.headers, [
        {"ACCESSION_NUMBER": "NPORT-AMBIG-ONE", "HOLDING_ID": "101"},
        {"ACCESSION_NUMBER": "NPORT-AMBIG-TWO", "HOLDING_ID": "101"},
    ])
    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        result = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="nport-test-ambig-v1")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT current_state, raw_validated_at FROM sec_ingestion_runs WHERE run_id=%s", (result["run_id"],))
            assert cur.fetchone() == ("failed", None)
    assert result["state"] == "failed"
    assert "ambíguo" in str(result["reason"])


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_real_db_accession_orphan_fails_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.nport.ingestion as ingestion

    contract, package = _package(tmp_path, "2028q2_nport", accession="NPORT-SUBMISSION")
    registrant = contract.table_for_filename("REGISTRANT.tsv")
    _write_row(package / registrant.source_file, registrant.headers, {"ACCESSION_NUMBER": "NPORT-ORPHAN"})
    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        result = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="nport-test-accession-v1")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT current_state, raw_validated_at FROM sec_ingestion_runs WHERE run_id=%s", (result["run_id"],))
            assert cur.fetchone() == ("failed", None)
    assert result["state"] == "failed"
    assert "ACCESSION_NUMBER órfão" in str(result["reason"])


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_real_db_duplicate_bytes_are_linked_to_canonical_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.nport.ingestion as ingestion

    contract, first = _package(tmp_path, "2027q1_nport", accession="NPORT-DUPLICATE")
    _, second = _package(tmp_path, "2027q2_nport", accession="NPORT-DUPLICATE")
    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        canonical = ingestion.ingest_package(conn, package=first, source_root=tmp_path, parser_version="nport-test-dup-v1")
        conn.commit()
        duplicate = ingestion.ingest_package(conn, package=second, source_root=tmp_path, parser_version="nport-test-dup-v1")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT package_state, duplicate_of_package_id FROM sec_source_packages WHERE package_relative_path = '2027q2_nport'")
            state, canonical_id = cur.fetchone()
    assert canonical["state"] == "raw_validated"
    assert duplicate["state"] == state == "duplicate"
    assert canonical_id is not None


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_real_db_failed_run_retries_exact_bytes_without_duplicate_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.nport.ingestion as ingestion

    contract, package = _package(tmp_path, "2027q4_nport", accession="NPORT-RETRY")
    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)
    original = ingestion._resolve_holding_parents
    monkeypatch.setattr(ingestion, "_resolve_holding_parents", lambda conn, *, run_id: (_ for _ in ()).throw(RuntimeError("transient test failure")))
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        failed = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="nport-test-retry-v1")
        conn.commit()
        monkeypatch.setattr(ingestion, "_resolve_holding_parents", original)
        completed = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="nport-test-retry-v1")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), count(DISTINCT source_row_number) FROM nport_raw_rows WHERE ingestion_run_id = %s", (completed["run_id"],))
            count, distinct_count = cur.fetchone()
            cur.execute("SELECT retry_count FROM sec_ingestion_runs WHERE run_id = %s", (completed["run_id"],))
            retries = cur.fetchone()[0]
    assert failed["state"] == "failed"
    assert completed["state"] == "raw_validated"
    assert count == distinct_count == 1
    assert retries == 1


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_loaded_inventory_failure_rolls_back_raw_validation_atomically(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.nport.ingestion as ingestion

    contract, package = _package(tmp_path, "2028q4_nport", accession="NPORT-ATOMIC")
    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)
    original = ingestion.register_package_discovery

    def fail_only_loaded(*args, **kwargs):
        if kwargs.get("package_state") == "loaded":
            raise RuntimeError("inventário loaded indisponível")
        return original(*args, **kwargs)

    monkeypatch.setattr(ingestion, "register_package_discovery", fail_only_loaded)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        result = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="nport-test-atomic-v1")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT current_state, raw_validated_at FROM sec_ingestion_runs WHERE run_id = %s", (result["run_id"],))
            state, raw_validated_at = cur.fetchone()
            cur.execute("SELECT package_state FROM sec_source_packages WHERE package_relative_path = '2028q4_nport'")
            package_state = cur.fetchone()[0]
    assert result["state"] == state == package_state == "failed"
    assert raw_validated_at is None


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_corrupt_table_reconciliation_is_not_a_skippable_checkpoint() -> None:
    import psycopg
    from src.nport.ingestion import _file_is_complete
    from src.sec_regulatory.manifests import create_or_resume_run, register_file, register_table_reconciliation, transition_run

    dsn = os.environ["SEC_TEST_DATABASE_URL"]
    sha = "b" * 64
    with psycopg.connect(dsn) as conn:
        run = create_or_resume_run(conn, source_family="nport", package_sha256=sha, parser_version="checkpoint-test", source_quarter="2028Q4", package_relative_path="checkpoint-test")
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
        file_id = register_file(conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256=sha, byte_size=1,
                                expected_count=1, data_count=1, lexical_count=1, typed_success_count=1, state="accounted")
        register_table_reconciliation(conn, run_id=run.run_id, source_file_id=file_id, table_name="SUBMISSION.tsv",
                                      expected_count=0, source_count=0, lexical_count=0, typed_success_count=0, state="accounted")
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO nport_raw_rows (ingestion_run_id, source_file_id, source_row_number, source_sha256,
                parser_version, source_table, original_lexical_row, typed_projection, parse_status, candidate_key_evidence, accession_number)
                VALUES (%s, %s, 2, %s, 'checkpoint-test', 'SUBMISSION.tsv', %s, %s, 'typed', %s, 'a')""", (run.run_id, file_id, sha, *_valid_submission_evidence()))
        assert not _file_is_complete(conn, run_id=run.run_id, source_file="SUBMISSION.tsv", source_sha256=sha, byte_size=1)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_real_db_malformed_date_preserves_lexical_row_and_reconciles(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.nport.ingestion as ingestion

    contract, package = _package(tmp_path, "2028q1_nport", accession="NPORT-BAD-DATE")
    submission = contract.table_for_filename("SUBMISSION.tsv")
    _write_rows(package / submission.source_file, submission.headers, [
        {"ACCESSION_NUMBER": "NPORT-BAD-DATE", "FILING_DATE": "not-a-date", "SUB_TYPE": "NPORT-P",
         "REPORT_ENDING_PERIOD": "31-DEC-2025", "REPORT_DATE": "31-DEC-2025", "IS_LAST_FILING": "N"},
        {"ACCESSION_NUMBER": "NPORT-GOOD-PARENT", "FILING_DATE": "25-FEB-2026", "SUB_TYPE": "NPORT-P",
         "REPORT_ENDING_PERIOD": "31-DEC-2025", "REPORT_DATE": "31-DEC-2025", "IS_LAST_FILING": "N"},
    ])
    holding = contract.table_for_filename("FUND_REPORTED_HOLDING.tsv")
    _write_row(package / holding.source_file, holding.headers, {
        "ACCESSION_NUMBER": "NPORT-GOOD-PARENT", "HOLDING_ID": "123", "BALANCE": "not-a-decimal",
    })
    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        result = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="nport-test-quarantine-v1")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT original_lexical_row->>'FILING_DATE', typed_projection->>'FILING_DATE', parse_status FROM nport_raw_rows WHERE ingestion_run_id = %s AND source_table = 'SUBMISSION.tsv' AND accession_number = 'NPORT-BAD-DATE'", (result["run_id"],))
            lexical, typed, status = cur.fetchone()
            cur.execute("SELECT array_agg(typed_error_code ORDER BY typed_error_code) FROM sec_row_issues WHERE source_file_id IN (SELECT source_file_id FROM sec_source_files WHERE run_id = %s)", (result["run_id"],))
            issues = cur.fetchone()[0]
            cur.execute("SELECT lexical_count, typed_success_count, quarantine_count FROM sec_source_files WHERE run_id = %s AND relative_path = 'SUBMISSION.tsv'", (result["run_id"],))
            counts = cur.fetchone()
    assert result["state"] == "raw_validated"
    assert (lexical, typed, status, issues, counts) == ("not-a-date", None, "quarantined", ["invalid_date", "invalid_decimal"], (2, 1, 1))


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_raw_writer_locks_parent_run_before_validator_can_publish() -> None:
    """O trigger N-PORT segura a mesma linhagem que validate_raw_run precisa fechar."""
    import psycopg
    from src.sec_regulatory.manifests import (
        create_or_resume_run, register_file, register_table_reconciliation, transition_run, validate_raw_run,
    )

    dsn = os.environ["SEC_TEST_DATABASE_URL"]
    sha = "a" * 64
    with psycopg.connect(dsn) as setup:
        run = create_or_resume_run(setup, source_family="nport", package_sha256=sha, parser_version="lock-test", source_quarter="2028Q3", package_relative_path="lock-test")
        run = transition_run(setup, run_id=run.run_id, expected_state="discovered", target_state="loading")
        file_id = register_file(setup, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256=sha, byte_size=0, state="accounted")
        register_table_reconciliation(setup, run_id=run.run_id, source_file_id=file_id, table_name="SUBMISSION.tsv", state="accounted")
        setup.commit()
    with psycopg.connect(dsn) as writer, psycopg.connect(dsn) as validator:
        with writer.cursor() as cur:
            cur.execute(
                """INSERT INTO nport_raw_rows (ingestion_run_id, source_file_id, source_row_number, source_sha256,
                    parser_version, source_table, original_lexical_row, typed_projection, parse_status,
                    candidate_key_evidence, accession_number) VALUES (%s, %s, 2, %s, 'lock-test', 'SUBMISSION.tsv', %s, %s, 'typed', %s, 'a')""",
                (run.run_id, file_id, sha, *_valid_submission_evidence()),
            )
        with validator.cursor() as cur:
            cur.execute("SET lock_timeout = '100ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                validate_raw_run(validator, run_id=run.run_id)
        writer.rollback()
        validator.rollback()
        with pytest.raises(psycopg.Error, match="N-PORT raw validation failed"):
            validate_raw_run(validator, run_id=run.run_id)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_direct_sql_lie_cannot_enter_raw_rows() -> None:
    import psycopg
    from src.sec_regulatory.manifests import create_or_resume_run, register_file, transition_run

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run = create_or_resume_run(conn, source_family="nport", package_sha256="1" * 64, parser_version="truth-v1", source_quarter="2028Q4", package_relative_path="lie-test")
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
        file_id = register_file(conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256="2" * 64, byte_size=0, state="accounted")
        with conn.cursor() as cur:
            with pytest.raises(psycopg.Error, match="invalid N-PORT raw provenance"):
                cur.execute("""INSERT INTO nport_raw_rows (ingestion_run_id, source_file_id, source_row_number, source_sha256,
                    parser_version, source_table, original_lexical_row, typed_projection, parse_status, candidate_key_evidence)
                    VALUES (%s, %s, 2, %s, 'LIE-v999', 'SUBMISSION.tsv', '{}', '{}', 'typed', '{}')""", (run.run_id, file_id, "3" * 64))


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_direct_sql_update_cannot_forge_provenance() -> None:
    import psycopg
    from src.sec_regulatory.manifests import create_or_resume_run, register_file, transition_run

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        sha = "4" * 64
        run = create_or_resume_run(conn, source_family="nport", package_sha256="5" * 64, parser_version="truth-update", source_quarter="2028Q4", package_relative_path="update-lie")
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
        file_id = register_file(conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256=sha, byte_size=0, state="accounted")
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO nport_raw_rows (ingestion_run_id, source_file_id, source_row_number, source_sha256, parser_version, source_table, original_lexical_row, typed_projection, parse_status, candidate_key_evidence, accession_number) VALUES (%s,%s,2,%s,'truth-update','SUBMISSION.tsv',%s,%s,'typed',%s,'a')""", (run.run_id,file_id,sha,*_valid_submission_evidence()))
            with pytest.raises(psycopg.Error, match="invalid N-PORT raw provenance"):
                cur.execute("UPDATE nport_raw_rows SET parser_version='forged' WHERE ingestion_run_id=%s", (run.run_id,))


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_generic_raw_validator_invokes_nport_db_gate_for_missing_coverage() -> None:
    """Calling the generic validator cannot bypass the N-PORT exact 30-table gate."""
    import psycopg
    from src.sec_regulatory.manifests import (
        create_or_resume_run, install_schema as install_manifest_schema, register_file,
        register_table_reconciliation, transition_run, validate_raw_run,
    )

    sha = "6" * 64
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run = create_or_resume_run(conn, source_family="nport", package_sha256=sha, parser_version="db-gate-v1", source_quarter="2028Q4", package_relative_path="db-gate")
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
        file_id = register_file(conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256=sha, byte_size=0, state="accounted")
        register_table_reconciliation(conn, run_id=run.run_id, source_file_id=file_id, table_name="SUBMISSION.tsv", state="accounted")
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO nport_raw_rows (ingestion_run_id, source_file_id, source_row_number, source_sha256,
                parser_version, source_table, original_lexical_row, typed_projection, parse_status, candidate_key_evidence,
                accession_number) VALUES (%s,%s,2,%s,'db-gate-v1','SUBMISSION.tsv',%s,%s,'typed',%s,'a')""", (run.run_id, file_id, sha, *_valid_submission_evidence()))
        # A reinstalação do schema compartilhado não pode apagar o hook N-PORT.
        install_manifest_schema(conn)
        with pytest.raises(psycopg.Error, match="N-PORT raw validation failed"):
            validate_raw_run(conn, run_id=run.run_id)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
@pytest.mark.parametrize("relative_path", ["EXTRA.tsv", "FUND_VAR_INFO.tsv"])
def test_extra_zero_count_source_file_cannot_hide_outside_closed_contract(relative_path: str) -> None:
    import psycopg
    from src.sec_regulatory.manifests import register_file, validate_raw_run

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run, _submission_file_id = _seed_zero_row_contract_run(conn, suffix="extra-file")
        register_file(
            conn, run_id=run.run_id, relative_path=relative_path, sha256="e" * 64,
            byte_size=0, state="accounted",
        )

        with pytest.raises(psycopg.Error, match="N-PORT raw validation failed"):
            validate_raw_run(conn, run_id=run.run_id)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_governed_family_fails_closed_when_overlay_reconciler_is_absent() -> None:
    import psycopg
    from src.sec_regulatory.manifests import register_file, validate_raw_run

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run, _ = _seed_zero_row_contract_run(conn, suffix="missing-overlay")
        with conn.cursor() as cur:
            cur.execute("DROP FUNCTION public.nport_raw_run_reconciles(uuid)")
        with pytest.raises(psycopg.Error, match="required N-PORT raw reconciler is absent"):
            validate_raw_run(conn, run_id=run.run_id)
        assert run.run_id is not None
        conn.rollback()  # restore the overlay function for the shared disposable DB


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
@pytest.mark.parametrize("mutation", ["missing-lexical-column", "invented-candidate-value"])
def test_direct_sql_cannot_forge_lexical_or_candidate_evidence(mutation: str) -> None:
    import psycopg
    from src.sec_regulatory.manifests import create_or_resume_run, register_file, transition_run

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        sha = hashlib.sha256(mutation.encode("utf-8")).hexdigest()
        run = create_or_resume_run(
            conn, source_family="nport", package_sha256=sha, parser_version=f"forge-{mutation}",
            source_quarter="2029Q2", package_relative_path=f"forge/{mutation}",
        )
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
        file_id = register_file(
            conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256="c" * 64,
            byte_size=0, state="accounted",
        )
        lexical, typed, evidence = map(json.loads, _valid_submission_evidence())
        if mutation == "missing-lexical-column":
            lexical.pop("FILE_NUM")
        else:
            evidence["values"] = ["invented"]
        with conn.cursor() as cur:
            with pytest.raises(psycopg.Error, match="invalid N-PORT raw provenance"):
                cur.execute(
                    """INSERT INTO nport_raw_rows (
                           ingestion_run_id, source_file_id, source_row_number, source_sha256,
                           parser_version, source_table, original_lexical_row, typed_projection,
                           parse_status, parse_errors, candidate_key_evidence, accession_number
                       ) VALUES (%s,%s,2,%s,%s,'SUBMISSION.tsv',%s,%s,'typed','[]',%s,'a')""",
                    (run.run_id, file_id, "c" * 64, f"forge-{mutation}",
                     json.dumps(lexical), json.dumps(typed), json.dumps(evidence)),
                )


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_publication_rejects_typed_projection_that_contradicts_lexical_source() -> None:
    """A direct SQL row can have complete keys yet cannot invent parsed values."""
    import psycopg
    from src.sec_regulatory.manifests import register_file, register_table_reconciliation, validate_raw_run

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run, source_file_id = _seed_zero_row_contract_run(conn, suffix="forged-typed-values")
        lexical, typed, evidence = map(json.loads, _valid_submission_evidence())
        typed["ACCESSION_NUMBER"] = "forged-accession"
        typed["FILING_DATE"] = "1999-01-01"
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO nport_raw_rows (
                       ingestion_run_id, source_file_id, source_row_number, source_sha256,
                       parser_version, source_table, original_lexical_row, typed_projection,
                       parse_status, parse_errors, candidate_key_evidence, accession_number
                   ) VALUES (%s,%s,2,%s,%s,'SUBMISSION.tsv',%s,%s,'typed','[]',%s,'a')""",
                (run.run_id, source_file_id, "a" * 64, "zero-forged-typed-values",
                 json.dumps(lexical), json.dumps(typed), json.dumps(evidence)),
            )
        register_file(
            conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256="a" * 64,
            byte_size=0, expected_count=1, data_count=1, lexical_count=1,
            typed_success_count=1, state="accounted", source_file_id=source_file_id,
        )
        register_table_reconciliation(
            conn, run_id=run.run_id, source_file_id=source_file_id, table_name="SUBMISSION.tsv",
            expected_count=1, source_count=1, lexical_count=1, typed_success_count=1, state="accounted",
        )
        with pytest.raises(psycopg.Error, match="N-PORT raw validation failed"):
            validate_raw_run(conn, run_id=run.run_id)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_publication_rejects_forged_required_blank_as_typed() -> None:
    """Required-blank derivation stays fail-closed inside the fused row scan."""
    import psycopg
    from src.nport.schema import json_typed_projection, load_nport_contract, parse_row
    from src.sec_regulatory.manifests import register_file, register_table_reconciliation, validate_raw_run

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run, _ = _seed_zero_row_contract_run(conn, suffix="required-blank-forgery")
        table = load_nport_contract().table_for_filename("DEBT_SECURITY.tsv")
        parsed = parse_row(table.columns, tuple("" for _ in table.columns))
        file_id = register_file(
            conn, run_id=run.run_id, relative_path=table.source_file, sha256="c" * 64,
            byte_size=0, expected_count=1, data_count=1, lexical_count=1,
            typed_success_count=1, state="accounted",
        )
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE sec_table_reconciliations
                   SET source_file_id=%s, expected_count=1, source_count=1,
                       lexical_count=1, typed_success_count=1
                   WHERE run_id=%s AND table_name=%s""",
                (file_id, run.run_id, table.source_file),
            )
            cur.execute(
                """INSERT INTO nport_raw_rows (
                       ingestion_run_id,source_file_id,source_row_number,source_sha256,
                       parser_version,source_table,original_lexical_row,typed_projection,
                       parse_status,parse_errors,candidate_key_evidence
                   ) VALUES (%s,%s,2,%s,%s,%s,%s,%s,'typed','[]',%s)""",
                (run.run_id, file_id, "c" * 64, "zero-required-blank-forgery",
                 table.source_file, json.dumps(parsed.lexical),
                 json.dumps(json_typed_projection(parsed.typed)),
                 json.dumps(parsed.candidate_key_evidence)),
            )
        with pytest.raises(psycopg.Error, match="N-PORT raw validation failed"):
            validate_raw_run(conn, run_id=run.run_id)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_publication_rejects_duplicate_candidate_key_in_one_grouped_scan() -> None:
    import psycopg
    from src.sec_regulatory.manifests import register_file, register_table_reconciliation, validate_raw_run

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run, file_id = _seed_zero_row_contract_run(conn, suffix="duplicate-candidate")
        lexical, typed, evidence = _valid_submission_evidence()
        with conn.cursor() as cur:
            for row_number in (2, 3):
                cur.execute(
                    """INSERT INTO nport_raw_rows (
                           ingestion_run_id,source_file_id,source_row_number,source_sha256,
                           parser_version,source_table,original_lexical_row,typed_projection,
                           parse_status,parse_errors,candidate_key_evidence,accession_number
                       ) VALUES (%s,%s,%s,%s,%s,'SUBMISSION.tsv',%s,%s,'typed','[]',%s,'a')""",
                    (run.run_id, file_id, row_number, "a" * 64, "zero-duplicate-candidate",
                     lexical, typed, evidence),
                )
        register_file(
            conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256="a" * 64,
            byte_size=0, expected_count=2, data_count=2, lexical_count=2,
            typed_success_count=2, state="accounted", source_file_id=file_id,
        )
        register_table_reconciliation(
            conn, run_id=run.run_id, source_file_id=file_id, table_name="SUBMISSION.tsv",
            expected_count=2, source_count=2, lexical_count=2,
            typed_success_count=2, state="accounted",
        )
        with pytest.raises(psycopg.Error, match="N-PORT raw validation failed"):
            validate_raw_run(conn, run_id=run.run_id)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_catalog_guc_cannot_authorize_direct_mutation() -> None:
    import psycopg

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute("SELECT set_config('nport.install_catalog', 'on', true)")
        with pytest.raises(psycopg.Error, match="immutable"):
            cur.execute(
                "UPDATE nport_contract_tables SET required_columns=ARRAY['FORGED'] WHERE source_table='SUBMISSION.tsv'"
            )


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
@pytest.mark.parametrize(
    ("policy", "raw", "expected_code", "expected_length"),
    [
        ("date_field_specific_fail_preserve_lexical", "1-Jan-2024", "invalid_date", None),
        ("date_field_specific_fail_preserve_lexical", "01-JAN-2024", None, 10),
        ("decimal_preserve_lexical", "1e200000", "decimal_out_of_domain", None),
        ("decimal_preserve_lexical", "NaN", "invalid_decimal", None),
        ("decimal_preserve_lexical", "1e131071", None, 131072),
        ("decimal_preserve_lexical", "1e-16384", "decimal_out_of_domain", None),
    ],
)
def test_sql_parser_has_the_same_anchored_date_and_bounded_decimal_domain(
    policy: str, raw: str, expected_code: str | None, expected_length: int | None,
) -> None:
    import psycopg

    specs = [{
        "name": "VALUE", "parsing_policy": policy, "required": False,
        "datatype": {"base": "date" if policy.startswith("date_") else "NUMBER"},
    }]
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute("SELECT nport_expected_row(%s::jsonb,%s::jsonb)", (
            json.dumps({"VALUE": raw}), json.dumps(specs),
        ))
        derived = cur.fetchone()[0]
    errors = derived["parse_errors"]
    assert (errors[0]["code"] if errors else None) == expected_code
    if expected_length is not None:
        assert len(derived["typed_projection"]["VALUE"]) == expected_length


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
@pytest.mark.parametrize("mutation", ["duplicate", "omit", "add", "reorder", "policy-swap"])
def test_publication_rejects_any_contract_spec_catalog_drift(mutation: str) -> None:
    import psycopg
    from src.sec_regulatory.manifests import validate_raw_run

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run, _ = _seed_zero_row_contract_run(conn, suffix=f"catalog-{mutation}")
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE nport_contract_tables DISABLE TRIGGER nport_contract_tables_immutable")
            if mutation == "duplicate":
                expression = "column_specs || jsonb_build_array(column_specs->0)"
            elif mutation == "omit":
                expression = "column_specs - 0"
            elif mutation == "add":
                expression = "column_specs || '[{\"name\":\"FORGED\",\"parsing_policy\":\"text_preserve_lexical\",\"required\":false,\"datatype\":{\"base\":\"string\"}}]'::jsonb"
            elif mutation == "reorder":
                expression = "(SELECT jsonb_agg(item ORDER BY CASE ordinal WHEN 1 THEN 2 WHEN 2 THEN 1 ELSE ordinal END) FROM jsonb_array_elements(column_specs) WITH ORDINALITY AS spec(item,ordinal))"
            else:
                expression = "jsonb_set(column_specs, '{0,parsing_policy}', '\"decimal_preserve_lexical\"')"
            cur.execute(
                f"UPDATE nport_contract_tables SET column_specs={expression} WHERE source_table='SUBMISSION.tsv'"
            )
            cur.execute("ALTER TABLE nport_contract_tables ENABLE TRIGGER nport_contract_tables_immutable")
        with pytest.raises(psycopg.Error, match="N-PORT raw validation failed"):
            validate_raw_run(conn, run_id=run.run_id)
        conn.rollback()


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_direct_sql_rejects_non_string_lexical_json_values() -> None:
    import psycopg
    from src.sec_regulatory.manifests import create_or_resume_run, register_file, transition_run

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        suffix = "non-string-lexical"
        sha = hashlib.sha256(suffix.encode()).hexdigest()
        run = create_or_resume_run(
            conn, source_family="nport", package_sha256=sha, parser_version=suffix,
            source_quarter="2029Q2", package_relative_path=suffix,
        )
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
        file_id = register_file(
            conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256="d" * 64,
            byte_size=0, state="accounted",
        )
        lexical, typed, evidence = map(json.loads, _valid_submission_evidence())
        lexical["FILE_NUM"] = 123
        with conn.cursor() as cur, pytest.raises(psycopg.Error, match="invalid N-PORT raw provenance"):
            cur.execute(
                """INSERT INTO nport_raw_rows (
                       ingestion_run_id,source_file_id,source_row_number,source_sha256,parser_version,
                       source_table,original_lexical_row,typed_projection,parse_status,parse_errors,
                       candidate_key_evidence,accession_number
                   ) VALUES (%s,%s,2,%s,%s,'SUBMISSION.tsv',%s,%s,'typed','[]',%s,'a')""",
                (run.run_id, file_id, "d" * 64, suffix, json.dumps(lexical),
                 json.dumps(typed), json.dumps(evidence)),
            )


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
@pytest.mark.parametrize("mutation", ["raw-value", "detail", "code", "disposition"])
def test_publication_derives_exact_error_issue_and_disposition(mutation: str) -> None:
    import psycopg
    from src.sec_regulatory.manifests import (
        record_issue, register_file, register_table_reconciliation, validate_raw_run,
    )

    suffix = f"derived-error-{mutation}"
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run, file_id = _seed_zero_row_contract_run(conn, suffix=suffix)
        lexical, typed, evidence = map(json.loads, _valid_submission_evidence())
        lexical["FILING_DATE"] = "bad-date"
        typed["FILING_DATE"] = None
        error = {
            "column_name": "FILING_DATE", "code": "invalid_date",
            "raw_value": "bad-date", "detail": "data inválida para o campo",
        }
        status = "quarantined"
        if mutation == "raw-value": error["raw_value"] = "invented"
        elif mutation == "detail": error["detail"] = "invented"
        elif mutation == "code": error["code"] = "invalid_text"
        else: status = "rejected"
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO nport_raw_rows (
                       ingestion_run_id,source_file_id,source_row_number,source_sha256,parser_version,
                       source_table,original_lexical_row,typed_projection,parse_status,parse_errors,
                       candidate_key_evidence,accession_number
                   ) VALUES (%s,%s,2,%s,%s,'SUBMISSION.tsv',%s,%s,%s,%s,%s,'a')""",
                (run.run_id, file_id, "a" * 64, f"zero-{suffix}", json.dumps(lexical),
                 json.dumps(typed), status, json.dumps([error]), json.dumps(evidence)),
            )
        record_issue(
            conn, source_file_id=file_id, source_row_number=2, table_name="SUBMISSION.tsv",
            column_name="FILING_DATE", raw_lexical_value=error["raw_value"],
            typed_error_code=error["code"], typed_error_detail=error["detail"], status=status,
        )
        register_file(
            conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256="a" * 64,
            byte_size=0, expected_count=1, data_count=1, lexical_count=1,
            quarantine_count=int(status == "quarantined"), reject_count=int(status == "rejected"),
            state="accounted", source_file_id=file_id,
        )
        register_table_reconciliation(
            conn, run_id=run.run_id, source_file_id=file_id, table_name="SUBMISSION.tsv",
            expected_count=1, source_count=1, lexical_count=1,
            quarantine_count=int(status == "quarantined"), reject_count=int(status == "rejected"), state="accounted",
        )
        with pytest.raises(psycopg.Error, match="N-PORT raw validation failed"):
            validate_raw_run(conn, run_id=run.run_id)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
@pytest.mark.parametrize(
    ("raw_status", "issue_status"),
    [
        ("rejected", "quarantined"),
        ("quarantined", "rejected"),
        ("typed", "resolved"),
        ("quarantined", None),
    ],
)
def test_publication_rejects_disposition_issue_mismatch(
    raw_status: str, issue_status: str | None,
) -> None:
    import psycopg
    from src.sec_regulatory.manifests import (
        RawValidationError, record_issue, register_file, register_table_reconciliation, validate_raw_run,
    )

    suffix = f"disposition-{raw_status}-{issue_status}"
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run, submission_file_id = _seed_zero_row_contract_run(conn, suffix=suffix)
        lexical, typed, evidence = _valid_submission_evidence()
        parse_errors = [] if raw_status == "typed" else [{
            "column_name": "FILING_DATE", "code": "invalid_date",
            "raw_value": "bad", "detail": "data inválida para o campo",
        }]
        typed_payload = json.loads(typed)
        if raw_status != "typed":
            typed_payload["FILING_DATE"] = None
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO nport_raw_rows (
                       ingestion_run_id, source_file_id, source_row_number, source_sha256,
                       parser_version, source_table, original_lexical_row, typed_projection,
                       parse_status, parse_errors, candidate_key_evidence, accession_number
                   ) VALUES (%s,%s,2,%s,%s,'SUBMISSION.tsv',%s,%s,%s,%s,%s,'a')""",
                (run.run_id, submission_file_id, "a" * 64, f"zero-{suffix}", lexical,
                 json.dumps(typed_payload), raw_status, json.dumps(parse_errors), evidence),
            )
        if issue_status is not None:
            record_issue(
                conn, source_file_id=submission_file_id, source_row_number=2,
                table_name="SUBMISSION.tsv", column_name="FILING_DATE",
                raw_lexical_value="bad", typed_error_code="invalid_date",
                typed_error_detail="data inválida para o campo", status=issue_status,
            )
        quarantine = int(issue_status == "quarantined")
        rejected = int(issue_status == "rejected")
        typed_count = int(raw_status == "typed")
        register_file(
            conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256="a" * 64,
            byte_size=0, expected_count=1, data_count=1, lexical_count=1,
            typed_success_count=typed_count, quarantine_count=quarantine,
            reject_count=rejected, state="accounted", source_file_id=submission_file_id,
        )
        register_table_reconciliation(
            conn, run_id=run.run_id, source_file_id=submission_file_id,
            table_name="SUBMISSION.tsv", expected_count=1, source_count=1,
            lexical_count=1, typed_success_count=typed_count,
            quarantine_count=quarantine, reject_count=rejected, state="accounted",
        )
        with pytest.raises((psycopg.Error, RawValidationError)):
            validate_raw_run(conn, run_id=run.run_id)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_same_header_file_replacement_fails_without_accounted_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg
    import src.nport.ingestion as ingestion

    contract, package = _package(tmp_path, "2029q3_nport", accession="ORIGINAL")
    submission = contract.table_for_filename("SUBMISSION.tsv")
    original_verify = ingestion.verify_package
    calls = 0

    def verify_then_replace(path, selected_contract):
        nonlocal calls
        verified = original_verify(path, selected_contract)
        calls += 1
        if calls == 2:
            _write_row(path / submission.source_file, submission.headers, {
                "ACCESSION_NUMBER": "REPLACED", "FILING_DATE": "25-FEB-2026",
                "SUB_TYPE": "NPORT-P", "REPORT_ENDING_PERIOD": "31-DEC-2025",
                "REPORT_DATE": "31-DEC-2025", "IS_LAST_FILING": "N",
            })
        return verified

    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)
    monkeypatch.setattr(ingestion, "verify_package", verify_then_replace)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        result = ingestion.ingest_package(
            conn, package=package, source_root=tmp_path, parser_version="replace-test-v1",
        )
        conn.commit()
        assert result["state"] == "failed"
        assert "SHA-256 mudou durante leitura" in str(result["reason"])
        with conn.cursor() as cur:
            cur.execute(
                """SELECT count(*) FROM sec_source_files
                   WHERE run_id=%s AND relative_path='SUBMISSION.tsv' AND state='accounted'""",
                (result["run_id"],),
            )
            assert cur.fetchone()[0] == 0


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_concurrent_same_package_ingestion_converges_without_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading
    from concurrent.futures import ThreadPoolExecutor
    import psycopg
    import src.nport.ingestion as ingestion

    contract, package = _package(tmp_path, "2029q4_nport", accession="CONCURRENT")
    original_load = ingestion._load_file
    first_started = threading.Event()
    release_first = threading.Event()
    first_lock = threading.Lock()
    first = True

    def slow_first_load(*args, **kwargs):
        nonlocal first
        with first_lock:
            is_first = first
            first = False
        if is_first:
            first_started.set()
            assert release_first.wait(timeout=10)
        return original_load(*args, **kwargs)

    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)
    monkeypatch.setattr(ingestion, "_load_file", slow_first_load)

    def run_one():
        with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
            result = ingestion.ingest_package(
                conn, package=package, source_root=tmp_path, parser_version="concurrent-v1",
            )
            conn.commit()
            return result

    with ThreadPoolExecutor(max_workers=2) as pool:
        first_future = pool.submit(run_one)
        assert first_started.wait(timeout=10)
        second_future = pool.submit(run_one)
        release_first.set()
        results = [first_future.result(timeout=20), second_future.result(timeout=20)]

    assert {result["state"] for result in results} == {"raw_validated"}
    assert sum(bool(result.get("resumed")) for result in results) == 1
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT count(*), min(package_state), max(package_state)
                   FROM sec_source_packages WHERE source_family='nport'
                     AND package_relative_path='2029q4_nport'"""
            )
            assert cur.fetchone() == (1, "loaded", "loaded")


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_issue_on_last_row_of_full_copy_batch_reconciles_at_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg
    import src.nport.ingestion as ingestion

    contract, package = _package(tmp_path, "2030q1_nport", accession="BATCH-0")
    submission = contract.table_for_filename("SUBMISSION.tsv")
    rows = [
        {
            "ACCESSION_NUMBER": f"BATCH-{index}",
            "FILING_DATE": "bad-date" if index == 999 else "25-FEB-2026",
            "SUB_TYPE": "NPORT-P", "REPORT_ENDING_PERIOD": "31-DEC-2025",
            "REPORT_DATE": "31-DEC-2025", "IS_LAST_FILING": "N",
        }
        for index in range(1_000)
    ]
    _write_rows(package / submission.source_file, submission.headers, rows)
    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        result = ingestion.ingest_package(
            conn, package=package, source_root=tmp_path, parser_version="batch-boundary-v1",
        )
        conn.commit()
        assert result["state"] == "raw_validated"
        with conn.cursor() as cur:
            cur.execute(
                """SELECT typed_success_count, quarantine_count
                   FROM sec_source_files WHERE run_id=%s AND relative_path='SUBMISSION.tsv'""",
                (result["run_id"],),
            )
            assert cur.fetchone() == (999, 1)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_multiple_issues_on_one_row_count_as_one_quarantine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue preaggregation must not multiply raw reconciliation counts."""
    import psycopg
    import src.nport.ingestion as ingestion

    contract, package = _package(tmp_path, "2030q2_nport", accession="TWO-ISSUES")
    submission = contract.table_for_filename("SUBMISSION.tsv")
    _write_row(package / submission.source_file, submission.headers, {
        "ACCESSION_NUMBER": "TWO-ISSUES", "FILING_DATE": "bad-date",
        "SUB_TYPE": "NPORT-P", "REPORT_ENDING_PERIOD": "31-DEC-2025",
        "REPORT_DATE": "also-bad", "IS_LAST_FILING": "N",
    })
    monkeypatch.setattr(ingestion, "load_nport_contract", lambda: contract)

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        result = ingestion.ingest_package(
            conn, package=package, source_root=tmp_path, parser_version="two-issues-v1",
        )
        conn.commit()
        assert result["state"] == "raw_validated"
        with conn.cursor() as cur:
            cur.execute(
                """SELECT f.lexical_count, f.quarantine_count, count(i.issue_id)
                   FROM sec_source_files f
                   LEFT JOIN sec_row_issues i ON i.source_file_id=f.source_file_id
                   WHERE f.run_id=%s AND f.relative_path='SUBMISSION.tsv'
                   GROUP BY f.source_file_id""",
                (result["run_id"],),
            )
            assert cur.fetchone() == (1, 1, 2)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_holding_map_must_reference_the_exact_raw_parent_row() -> None:
    import psycopg

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        run, submission_file_id = _seed_zero_row_contract_run(conn, suffix="map-fk")
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.ForeignKeyViolation):
                cur.execute(
                    """INSERT INTO nport_holding_accession_map
                           (ingestion_run_id, holding_id, accession_number, source_file_id, source_row_number)
                       VALUES (%s, 'missing-holding', 'missing-accession', %s, 99)""",
                    (run.run_id, submission_file_id),
                )


def _seed_zero_row_contract_run(conn, *, suffix: str):
    from src.nport.schema import load_nport_contract
    from src.sec_regulatory.manifests import (
        create_or_resume_run, register_file, register_table_reconciliation, transition_run,
    )

    digest = hashlib.sha256(suffix.encode("utf-8")).hexdigest()
    run = create_or_resume_run(
        conn, source_family="nport", package_sha256=digest, parser_version=f"zero-{suffix}",
        source_quarter="2029Q1", package_relative_path=f"zero/{suffix}",
    )
    run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
    submission_file_id = register_file(
        conn, run_id=run.run_id, relative_path="SUBMISSION.tsv", sha256="a" * 64,
        byte_size=0, state="accounted",
    )
    metadata_file_id = register_file(
        conn, run_id=run.run_id, relative_path="nport_metadata.json", sha256="b" * 64,
        byte_size=0, state="accounted",
    )
    for table in load_nport_contract().tables:
        register_table_reconciliation(
            conn, run_id=run.run_id,
            source_file_id=submission_file_id if table.source_file == "SUBMISSION.tsv" else metadata_file_id,
            table_name=table.source_file, state="accounted",
        )
    return run, submission_file_id


def _synthetic_contract(tmp_path: Path):
    from src.nport.schema import load_nport_contract

    metadata = b"{}"
    (tmp_path / "nport_metadata.json").write_bytes(metadata)
    (tmp_path / "nport_readme.htm").write_text("fixture", encoding="utf-8")
    return replace(load_nport_contract(), metadata_sha256=hashlib.sha256(metadata).hexdigest())


def _package(root: Path, name: str, *, accession: str):
    contract = _synthetic_contract(root)
    package = root / name
    package.mkdir()
    for filename in ("nport_metadata.json", "nport_readme.htm"):
        (package / filename).write_bytes((root / filename).read_bytes())
    submission = contract.table_for_filename("SUBMISSION.tsv")
    _write_row(package / submission.source_file, submission.headers, {
        "ACCESSION_NUMBER": accession, "FILING_DATE": "25-FEB-2026", "SUB_TYPE": "NPORT-P",
        "REPORT_ENDING_PERIOD": "31-DEC-2025", "REPORT_DATE": "31-DEC-2025", "IS_LAST_FILING": "N",
    })
    return contract, package


def _write_row(path: Path, headers: tuple[str, ...], values: dict[str, str]) -> None:
    _write_rows(path, headers, [values])


def _write_rows(path: Path, headers: tuple[str, ...], rows: list[dict[str, str]]) -> None:
    lines = ["\t".join(headers)]
    lines.extend("\t".join(row.get(header, "") for header in headers) for row in rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _valid_submission_evidence() -> tuple[str, str, str]:
    lexical = {"ACCESSION_NUMBER": "a", "FILING_DATE": "25-FEB-2026", "FILE_NUM": "", "SUB_TYPE": "", "REPORT_ENDING_PERIOD": "", "REPORT_DATE": "", "IS_LAST_FILING": ""}
    typed = {"ACCESSION_NUMBER": "a", "FILING_DATE": "2026-02-25", "FILE_NUM": None, "SUB_TYPE": None, "REPORT_ENDING_PERIOD": None, "REPORT_DATE": None, "IS_LAST_FILING": None}
    evidence = {"columns": ["ACCESSION_NUMBER"], "complete": True, "values": ["a"]}
    return json.dumps(lexical), json.dumps(typed), json.dumps(evidence)
