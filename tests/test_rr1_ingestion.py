import hashlib
import os
from pathlib import Path
from uuid import uuid4

import pytest

_RR1_METADATA_SHA256S = {
    "2196a3843eb45a9369f2165baa760c157b0efe0e4d52f0d5d7458fae8a0a412f",
    "327edcb278408a20fb141c170a66606ba4e62b9f92fd6bf85a0fd7cdcb175248",
    "41b01adfbfd3b404ea71202a127ad9af1837a0e7762d0051632ba93e42c3e6ba",
    "662208201be85603ab18172f375bbba957bdf7fb8d315694dde863790ee3e637",
    "88e29e3505ae5fa36d5bb2df174fac2780d3f746c5f6a84234d8dcd439d64c4c",
    "cbdf8ac1971b531874eb87fb11e0126440b704f12886fa989c067fc666c08783",
}
_FIXTURE_METADATA_SHA256 = "2196a3843eb45a9369f2165baa760c157b0efe0e4d52f0d5d7458fae8a0a412f"


@pytest.mark.skipif(not os.getenv("RR1_CORPUS_AUDIT"), reason="defina RR1_CORPUS_AUDIT=1 para auditar E:\\Edgard")
def test_real_packages_select_all_six_metadata_variants_and_pass_real_headers() -> None:
    from src.rr1.schema import _metadata_file, load_rr1_contract, verify_package
    from src.sec_regulatory.contracts import sha256_file

    root = Path(os.environ.get("RR1_CORPUS_ROOT", r"E:\Edgard\RR1"))
    packages = sorted(path for path in root.iterdir() if path.is_dir())
    assert len(packages) == 39
    assert {sha256_file(_metadata_file(path)) for path in packages} == _RR1_METADATA_SHA256S
    assert all(len(load_rr1_contract(sha256_file(_metadata_file(path))).tables) == 6 for path in packages)
    assert all(verify_package(package).contract.metadata_sha256 in _RR1_METADATA_SHA256S for package in packages)


def test_txt_uses_corrected_eleven_part_candidate_key_and_lang_is_not_key() -> None:
    from src.rr1.schema import load_rr1_contract

    contract = load_rr1_contract("2196a3843eb45a9369f2165baa760c157b0efe0e4d52f0d5d7458fae8a0a412f")
    txt = contract.table_for_filename("txt.tsv")
    assert txt.candidate_key == ("adsh", "tag", "version", "ddate", "series", "class", "measure", "document", "otherdims", "iprx")
    assert "lang" not in txt.candidate_key
    assert "qtrs" not in txt.headers and "dimh" not in txt.headers


def test_invalid_rr1_date_is_quarantined_and_has_incomplete_candidate_evidence() -> None:
    from src.rr1.schema import load_rr1_contract, parse_row

    contract = load_rr1_contract("2196a3843eb45a9369f2165baa760c157b0efe0e4d52f0d5d7458fae8a0a412f")
    table = contract.table_for_filename("txt.tsv")
    row = {name: "x" for name in table.headers}
    row.update(adsh="0000000000-00-000001", ddate="21020906")
    parsed = parse_row(table.columns, tuple(row[name] for name in table.headers))
    assert parsed.parse_status == "quarantined"
    assert any(issue.column_name == "ddate" and issue.code == "invalid_date" for issue in parsed.issues)
    assert parsed.candidate_key_evidence["complete"] is False


def test_submission_date_policy_accepts_timestamp_and_quarantines_future_effdate_losslessly() -> None:
    from src.rr1.schema import load_rr1_contract, parse_row

    sub = load_rr1_contract("2196a3843eb45a9369f2165baa760c157b0efe0e4d52f0d5d7458fae8a0a412f").table_for_filename("sub.tsv")
    row = _valid_values(sub)
    row.update(pdate="20191130", effdate="21020906", filed="20191219", accepted="2019-12-19 11:22:00.0")
    parsed = parse_row(sub.columns, tuple(row[name] for name in sub.headers))

    assert parsed.lexical["effdate"] == "21020906"
    assert parsed.typed["effdate"] is None
    assert parsed.typed["accepted"].isoformat() == "2019-12-19T11:22:00"
    assert any(issue.column_name == "effdate" and issue.code == "invalid_date" for issue in parsed.issues)
    assert parsed.parse_status == "quarantined"


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
@pytest.mark.parametrize(
    ("column", "value", "package_name"),
    (
        ("accepted", "2024-02-30 11:22:00.0", "2026q2_rr1"),
        ("pdate", "2024-02-30", "2026q3_rr1"),
            ("pdate", "31022024", "2026q4_rr1"),
    ),
)
def test_calendar_invalid_rr1_date_is_quarantined_without_sql_cast_error(column: str, value: str, package_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    from src.rr1.ingestion import ingest_package
    from src.rr1.schema import load_rr1_contract, parse_row
    from src.rr1.storage import install_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    table = load_rr1_contract(_FIXTURE_METADATA_SHA256).table_for_filename("sub.tsv")
    values = _valid_values(table, **{column: value})
    parsed = parse_row(table.columns, tuple(values[name] for name in table.headers))
    assert parsed.parse_status == "quarantined"
    assert any(issue.column_name == column and issue.code == "invalid_date" for issue in parsed.issues)

    package, verified = _build_package(tmp_path, package_name, **{column: value})
    monkeypatch.setattr("src.rr1.ingestion.verify_package", lambda _: verified)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        result = ingest_package(conn, package=package, source_root=tmp_path, parser_version="rr1-invalid-accepted-v1")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("""SELECT parse_status, parse_errors FROM rr1_raw_v2_rows
                WHERE ingestion_run_id=%s AND source_table='sub.tsv'""", (result["run_id"],))
            status, errors = cur.fetchone()
    assert result["state"] == "raw_validated", result
    assert status == "quarantined"
    assert any(issue["column_name"] == column and issue["code"] == "invalid_date" for issue in errors)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
@pytest.mark.parametrize(
    ("overrides", "column", "expected", "package_name"),
    (
            ({"accepted": "2019-12-19 11:22:00.123000"}, "accepted", "2019-12-19T11:22:00.123000", "2028q1_rr1"),
            ({"pdate": "30112019"}, "pdate", "2019-11-30", "2028q2_rr1"),
    ),
)
def test_db_rr1_evidence_matches_python_date_projection(
    overrides: dict[str, str], column: str, expected: str, package_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import psycopg
    from src.rr1.ingestion import ingest_package
    from src.rr1.storage import install_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    package, verified = _build_package(tmp_path, package_name, **overrides)
    monkeypatch.setattr("src.rr1.ingestion.verify_package", lambda _: verified)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        result = ingest_package(conn, package=package, source_root=tmp_path, parser_version="rr1-db-date-projection-v1")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("""SELECT parse_status, typed_projection->>%s FROM rr1_raw_v2_rows
                WHERE ingestion_run_id=%s AND source_table='sub.tsv'""", (column, result["run_id"]))
            status, value = cur.fetchone()
    assert result["state"] == "raw_validated", result
    assert (status, value) == ("typed", expected)


@pytest.mark.parametrize(
    ("table_name", "column", "value", "code"),
    (
        ("sub.tsv", "adsh", "0000000000-00-00001", "invalid_text"),
        ("tag.tsv", "custom", "2", "invalid_decimal"),
        ("num.tsv", "iprx", "-1", "invalid_decimal"),
        ("sub.tsv", "cik", "12345678901", "invalid_decimal"),
    ),
)
def test_contract_length_and_decimal_bounds_quarantine_losslessly(table_name: str, column: str, value: str, code: str) -> None:
    from src.rr1.schema import load_rr1_contract, parse_row

    table = load_rr1_contract(_FIXTURE_METADATA_SHA256).table_for_filename(table_name)
    row = _valid_values(table)
    row[column] = value
    parsed = parse_row(table.columns, tuple(row[name] for name in table.headers))

    assert parsed.lexical[column] == value
    assert parsed.typed[column] is None
    assert any(issue.column_name == column and issue.code == code for issue in parsed.issues)


def test_extra_file_and_missing_declared_file_fail_closed(tmp_path: Path) -> None:
    from src.rr1.schema import verify_package
    from src.sec_regulatory.contracts import ContractError

    package, _ = _build_package(tmp_path, "2019q4_rr1")
    (package / "txt.tsv").unlink()
    (package / "surplus.tsv").write_text("x\n", encoding="utf-8")
    with pytest.raises(ContractError):
        verify_package(package)


def test_rr1_worker_lock_is_registered_and_unique() -> None:
    import src.db as db
    assert db.LOCK_RR1_INGESTION == 900_313
    ids = [value for name, value in vars(db).items() if name.startswith("LOCK_") and isinstance(value, int)]
    assert ids.count(db.LOCK_RR1_INGESTION) == 1


def test_rr1_ddl_has_no_global_quarantine_rate_gate() -> None:
    assert "quarantine_rate" not in Path("schemas/rr1_raw_v2.sql").read_text(encoding="utf-8").lower()


def test_rr1_streaming_accepts_long_text_and_only_blank_trailing_cells(tmp_path: Path) -> None:
    from src.rr1.tsv import stream_tsv
    from src.sec_regulatory.tsv import TsvFormatError

    path = tmp_path / "rr1.tsv"
    path.write_text("a\tb\nvalue\t" + "x" * 150_000 + "\t\t\n", encoding="utf-8")
    header, rows = stream_tsv(path)
    assert header == ("a", "b")
    assert list(rows) == [(2, ("value", "x" * 150_000))]
    path.write_text("a\tb\nvalue\textra\tNOT-BLANK\n", encoding="utf-8")
    _, rows = stream_tsv(path)
    with pytest.raises(TsvFormatError, match="quantidade de colunas"):
        list(rows)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_invalid_parent_effdate_excludes_an_accession_with_typed_facts_from_current_candidates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    from src.rr1.ingestion import ingest_package
    from src.rr1.schema import load_rr1_contract
    from src.rr1.storage import install_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    package, verified = _build_package(tmp_path, "2026q1_rr1", ddate="20191130", effdate="21020906")
    monkeypatch.setattr("src.rr1.ingestion.verify_package", lambda _: verified)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        result = ingest_package(conn, package=package, source_root=tmp_path, parser_version="rr1-synthetic-v2")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM sec_table_reconciliations WHERE run_id=%s", (result["run_id"],))
            reconciliations = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM rr1_raw_v2_rows WHERE ingestion_run_id=%s", (result["run_id"],))
            rows = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM rr1_current_fact_candidates_v2 WHERE ingestion_run_id=%s", (result["run_id"],))
            current_facts = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM rr1_raw_v2_rows WHERE ingestion_run_id=%s AND source_table IN ('num.tsv','txt.tsv') AND parse_status='typed'", (result["run_id"],))
            typed_facts = cur.fetchone()[0]
            cur.execute("SELECT rr1_raw_run_reconciles(%s)", (result["run_id"],))
            reconciles = cur.fetchone()[0]
    assert result["state"] == "raw_validated", result
    assert (reconciliations, rows, typed_facts, current_facts, reconciles) == (6, 6, 2, 0, True)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
@pytest.mark.parametrize("forgery", ("lexical", "typed", "candidate", "candidate_extra", "denormalized_empty_as_null"))
def test_direct_sql_forged_raw_evidence_is_rejected(forgery: str) -> None:
    """The DB guard rejects evidence shape forgery even when COPY is bypassed."""
    import json
    import uuid
    import psycopg
    from src.rr1.schema import json_typed_projection, load_rr1_contract, parse_row
    from src.rr1.storage import install_schema
    from src.sec_regulatory.manifests import (create_or_resume_run, install_schema as install_manifest_schema,
        register_file, register_package_discovery, transition_run)

    contract = load_rr1_contract("2196a3843eb45a9369f2165baa760c157b0efe0e4d52f0d5d7458fae8a0a412f")
    table = contract.table_for_filename("sub.tsv")
    values = {column.name: "" for column in table.columns}
    for column in table.columns:
        if column.required:
            values[column.name] = "1" if column.parsing_policy == "decimal_preserve_lexical" else "2024-01-31" if column.parsing_policy == "date_field_specific_fail_preserve_lexical" else "x"
    values["adsh"] = "0000000000-00-000001"
    if forgery == "denormalized_empty_as_null":
        values["adsh"] = ""
    parsed = parse_row(table.columns, tuple(values[name] for name in table.headers))
    lexical = dict(parsed.lexical)
    typed = json_typed_projection(parsed.typed)
    candidate = dict(parsed.candidate_key_evidence)
    if forgery == "lexical":
        lexical.pop("adsh")
    elif forgery == "typed":
        typed["forged"] = "value"
    elif forgery == "candidate":
        candidate["columns"] = ["forged"]
    elif forgery == "candidate_extra":
        candidate["forged"] = "extra"
    nonce = uuid.uuid4().hex
    digest = (nonce * 2)[:64]
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        register_package_discovery(conn, source_family="rr1", source_quarter="2019Q4", package_relative_path=f"forged-{nonce}", package_sha256=digest, metadata_sha256=contract.metadata_sha256, readme_sha256="c" * 64, package_state="discovered")
        run = create_or_resume_run(conn, source_family="rr1", package_sha256=digest, parser_version="rr1-forged-test", source_quarter="2019Q4", package_relative_path=f"forged-{nonce}")
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
        register_package_discovery(conn, source_family="rr1", source_quarter="2019Q4", package_relative_path=f"forged-{nonce}", package_sha256=digest, metadata_sha256=contract.metadata_sha256, readme_sha256="c" * 64, package_state="discovered", run_id=run.run_id)
        file_id = register_file(conn, run_id=run.run_id, relative_path="sub.tsv", sha256="d" * 64, byte_size=1, schema_metadata={"headers": list(table.headers)}, state="loading")
        with pytest.raises(psycopg.Error, match="invalid RR1 V2 raw provenance"):
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO rr1_raw_v2_rows(ingestion_run_id,source_file_id,source_row_number,source_sha256,parser_version,source_table,original_lexical_row,typed_projection,parse_status,parse_errors,candidate_key_evidence,adsh) VALUES(%s,%s,2,%s,'rr1-forged-test','sub.tsv',%s,%s,%s,%s,%s,%s)""", (run.run_id, file_id, "d" * 64, json.dumps(lexical), json.dumps(typed), parsed.parse_status, json.dumps([issue.__dict__ for issue in parsed.issues]), json.dumps(candidate), "" if forgery == "denormalized_empty_as_null" else values["adsh"]))


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_direct_sql_same_shape_forged_typed_value_is_rejected() -> None:
    """A correctly-shaped JSON document cannot invent a typed value."""
    # The existing parameterized test provisions the same minimal manifest row;
    # this assertion is kept focused on value derivation rather than key shape.
    import json
    import uuid
    import psycopg
    from src.rr1.schema import json_typed_projection, load_rr1_contract, parse_row
    from src.rr1.storage import install_schema
    from src.sec_regulatory.manifests import (create_or_resume_run, install_schema as install_manifest_schema,
        register_file, register_package_discovery, transition_run)

    contract = load_rr1_contract(_FIXTURE_METADATA_SHA256)
    table = contract.table_for_filename("sub.tsv")
    values = _valid_values(table)
    parsed = parse_row(table.columns, tuple(values[name] for name in table.headers))
    typed = json_typed_projection(parsed.typed)
    typed["adsh"] = "0000000000-00-999999"
    nonce = uuid.uuid4().hex
    digest = (nonce * 2)[:64]
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        register_package_discovery(conn, source_family="rr1", source_quarter="2019Q4", package_relative_path=f"forged-value-{nonce}", package_sha256=digest, metadata_sha256=contract.metadata_sha256, readme_sha256="c" * 64, package_state="discovered")
        run = create_or_resume_run(conn, source_family="rr1", package_sha256=digest, parser_version="rr1-forged-value", source_quarter="2019Q4", package_relative_path=f"forged-value-{nonce}")
        run = transition_run(conn, run_id=run.run_id, expected_state="discovered", target_state="loading")
        register_package_discovery(conn, source_family="rr1", source_quarter="2019Q4", package_relative_path=f"forged-value-{nonce}", package_sha256=digest, metadata_sha256=contract.metadata_sha256, readme_sha256="c" * 64, package_state="discovered", run_id=run.run_id)
        file_id = register_file(conn, run_id=run.run_id, relative_path="sub.tsv", sha256="d" * 64, byte_size=1, schema_metadata={"headers": list(table.headers)}, state="loading")
        with pytest.raises(psycopg.Error, match="invalid RR1 V2 raw provenance"):
            with conn.transaction(), conn.cursor() as cur:
                cur.execute("""INSERT INTO rr1_raw_v2_rows(ingestion_run_id,source_file_id,source_row_number,source_sha256,parser_version,source_table,original_lexical_row,typed_projection,parse_status,parse_errors,candidate_key_evidence,adsh) VALUES(%s,%s,2,%s,'rr1-forged-value','sub.tsv',%s,%s,%s,%s,%s,%s)""", (run.run_id, file_id, "d" * 64, json.dumps(parsed.lexical), json.dumps(typed), parsed.parse_status, json.dumps([issue.__dict__ for issue in parsed.issues]), json.dumps(parsed.candidate_key_evidence), values["adsh"]))


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_duplicate_path_with_mutated_bytes_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.rr1.ingestion as ingestion
    from src.rr1.ingestion import ingest_package
    from src.rr1.schema import package_sha256
    from src.rr1.storage import install_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema, register_package_discovery

    package, verified = _build_package(tmp_path, "2027q4_rr1")
    monkeypatch.setattr(ingestion, "verify_package", lambda _: verified)
    digest = package_sha256(verified.file_hashes, metadata_sha256=verified.metadata_sha256,
        readme_sha256=verified.readme_sha256, metadata_filename=verified.metadata_filename)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        canonical = register_package_discovery(conn, source_family="rr1", source_quarter="2027Q4", package_relative_path=f"{package.name}-canonical",
            package_sha256=digest, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256,
            package_state="discovered")
        register_package_discovery(conn, source_family="rr1", source_quarter="2027Q4", package_relative_path=package.name,
            package_sha256="f" * 64, metadata_sha256=verified.metadata_sha256, readme_sha256=verified.readme_sha256,
            package_state="duplicate", reason="known duplicate", duplicate_of_package_id=canonical.package_id)
        result = ingest_package(conn, package=package, source_root=tmp_path)
    assert digest != "f" * 64
    assert result == {"package": package.name, "state": "failed", "reason": "bytes mudaram em caminho duplicate"}


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_same_header_content_replacement_fails_before_an_accounted_checkpoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.rr1.ingestion as ingestion
    from src.rr1.storage import install_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    package, verified = _build_package(tmp_path, "2027q1_rr1")
    calls = 0

    def verify_then_replace(path: Path):
        nonlocal calls
        calls += 1
        if calls == 2:
            table = verified.contract.table_for_filename("sub.tsv")
            _write_row(path / "sub.tsv", table.headers, _valid_values(table, adsh="0000000000-00-000099"))
        return verified

    monkeypatch.setattr(ingestion, "verify_package", verify_then_replace)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        result = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="rr1-replace-v1")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM sec_source_files WHERE run_id=%s AND state='accounted'", (result["run_id"],))
            accounted = cur.fetchone()[0]
    assert result["state"] == "failed"
    assert "SHA-256 mudou durante leitura" in str(result["reason"])
    assert accounted == 0


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_two_sessions_for_the_same_package_converge_to_one_loaded_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import threading
    from concurrent.futures import ThreadPoolExecutor
    import psycopg
    import src.rr1.ingestion as ingestion
    from src.rr1.storage import install_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    year = 2000 + int.from_bytes(uuid4().bytes[:2], "big") % 100
    package, verified = _build_package(tmp_path, f"{year}q2_rr1")
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as setup:
        install_manifest_schema(setup)
        install_schema(setup)
        setup.commit()
    original_load = ingestion._load_file
    first_started = threading.Event()
    release_first = threading.Event()
    lock = threading.Lock()
    first = True

    def slow_first_load(*args, **kwargs):
        nonlocal first
        with lock:
            is_first = first
            first = False
        if is_first:
            first_started.set()
            assert release_first.wait(timeout=10)
        return original_load(*args, **kwargs)

    monkeypatch.setattr(ingestion, "_load_file", slow_first_load)
    monkeypatch.setattr(ingestion, "verify_package", lambda _: verified)

    def run_one() -> dict[str, object]:
        with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
            result = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="rr1-concurrent-v1")
            conn.commit()
            return result

    with ThreadPoolExecutor(max_workers=2) as pool:
        first_future = pool.submit(run_one)
        assert first_started.wait(timeout=10)
        second_future = pool.submit(run_one)
        release_first.set()
        results = [first_future.result(timeout=30), second_future.result(timeout=30)]
    assert {result["state"] for result in results} == {"raw_validated"}
    assert sum(bool(result.get("resumed")) for result in results) == 1
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), min(package_state), max(package_state) FROM sec_source_packages WHERE source_family='rr1' AND package_relative_path=%s", (package.name,))
            assert cur.fetchone() == (1, "loaded", "loaded")


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_rr1_fails_closed_when_its_overlay_reconciler_is_absent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg
    import src.rr1.ingestion as ingestion
    from src.rr1.storage import install_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    package, verified = _build_package(tmp_path, "2027q3_rr1")
    monkeypatch.setattr(ingestion, "verify_package", lambda _: verified)
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("DROP FUNCTION public.rr1_raw_run_reconciles(uuid)")
        result = ingestion.ingest_package(conn, package=package, source_root=tmp_path, parser_version="rr1-missing-overlay-v1")
        assert result["state"] == "failed"
        assert "required RR1 raw reconciler is absent" in str(result["reason"])
        conn.rollback()


def _valid_values(table, **overrides: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for column in table.columns:
        if column.parsing_policy == "decimal_preserve_lexical":
            value = "1"
        elif column.parsing_policy == "date_field_specific_fail_preserve_lexical":
            value = "2019-12-19 11:22:00.0" if column.name == "accepted" else "20191130" if column.name == "ddate" else "2019-11-30"
        else:
            value = "x"
        values[column.name] = value if column.required else ""
    values.update({"adsh": "0000000000-00-000001", "tag": "Tag", "version": "rr/2019", "ptag": "Tag", "pversion": "rr/2019", "ctag": "Tag", "cversion": "rr/2019"})
    values.update(overrides)
    return values


def _build_package(root: Path, name: str, **overrides: str):
    """Materialize the checked-in minimal RR1 descriptor without E:\\Edgard."""
    import json
    from src.rr1.schema import VerifiedPackage, load_rr1_contract
    from src.sec_regulatory.contracts import sha256_file

    fixture = json.loads((Path(__file__).parent / "fixtures" / "sec" / "rr1" / "minimal-package.json").read_text(encoding="utf-8"))
    package = root / name
    package.mkdir()
    metadata = package / "fixture-metadata.json"
    metadata.write_text("{}\n", encoding="utf-8")
    readme = package / "readme.htm"
    readme.write_text(fixture["readme"] + "\n", encoding="utf-8")
    contract = load_rr1_contract(fixture["metadata_sha256"])
    adsh = "0000000000-00-" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:6]
    hashes: dict[str, str] = {}
    for table in contract.tables:
        _write_row(package / table.source_file, table.headers, _valid_values(table, adsh=adsh, **overrides))
        hashes[table.source_file] = sha256_file(package / table.source_file)
    return package, VerifiedPackage(contract, hashes, fixture["metadata_sha256"], sha256_file(readme), metadata.name)


def _write_row(path: Path, headers: tuple[str, ...], values: dict[str, str]) -> None:
    path.write_text("\t".join(headers) + "\n" + "\t".join(values.get(name, "") for name in headers) + "\n", encoding="utf-8")
