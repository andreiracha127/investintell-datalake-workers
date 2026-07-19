from pathlib import Path
import os

import pytest

from src.ncen.ingestion import _relationship_keys, source_quarter_from_package
from src.ncen.inventory import CANONICAL_PACKAGE_INVENTORIES, build_package_inventory
from src.ncen.schema import load_ncen_contract, parse_row, verify_package


def test_ncen_routines_revoke_public_execute() -> None:
    ddl = Path("schemas/ncen_raw_v2.sql").read_text(encoding="utf-8")

    for routine in (
        "ncen_contract_catalog_immutable()",
        "ncen_validate_raw_statement()",
        "ncen_lock_raw_insert_statement()",
        "ncen_lock_raw_update_statement()",
        "ncen_lock_raw_delete_statement()",
        "ncen_raw_run_reconciles(uuid)",
    ):
        assert f"REVOKE ALL ON FUNCTION {routine} FROM PUBLIC" in ddl


def test_ncen_worker_lock_is_registered_and_unique() -> None:
    import src.db as db
    assert db.LOCK_NCEN_INGESTION == 900_310
    ids = [value for name, value in vars(db).items() if name.startswith("LOCK_") and isinstance(value, int)]
    assert ids.count(db.LOCK_NCEN_INGESTION) == 1


def test_real_frozen_packages_select_both_metadata_variants() -> None:
    root = Path(r"E:\Edgard\ncen")
    hashes = {verify_package(root / name).metadata_sha256 for name in ("2021q3_ncen", "2024q1_ncen_0")}
    assert len(hashes) == 2
    assert all(len(load_ncen_contract(sha).tables) == 53 for sha in hashes)


def test_canonical_inventory_closes_all_17_real_packages() -> None:
    """Every observed N-CEN delivery is bound to its immutable physical evidence."""
    root = Path(r"E:\Edgard\ncen")
    assert set(CANONICAL_PACKAGE_INVENTORIES) == {path.name for path in root.iterdir() if path.is_dir()}
    for package_id, expected in CANONICAL_PACKAGE_INVENTORIES.items():
        verified = verify_package(root / package_id)
        assert verified.inventory_digest == expected.inventory_digest
        assert verified.file_inventory == expected.files
        assert verified.explicit_zero_tables == expected.explicit_zero_tables


def test_blank_declared_candidate_preserves_incomplete_evidence() -> None:
    contract = load_ncen_contract("fb55228ca976c43955c9a49bccf2bc21c8b70d3c7194f936f13289f06acca737")
    table = contract.table_for_filename("FUND_REPORTED_INFO.tsv")
    parsed = parse_row(table.columns, tuple("" for _ in table.columns))
    assert parsed.parse_status == "quarantined"
    assert parsed.candidate_key_evidence == {"columns": ["ACCESSION_NUMBER", "SERIES_ID"], "complete": False, "values": None}


def test_candidate_parse_error_makes_key_evidence_incomplete() -> None:
    contract = load_ncen_contract("fb55228ca976c43955c9a49bccf2bc21c8b70d3c7194f936f13289f06acca737")
    table = contract.table_for_filename("CHIEF_COMPLIANCE_OFFICER.tsv")
    values = {column.name: "" for column in table.columns}
    values.update({"ACCESSION_NUMBER": "x" * 21, "CCO_SEQNUM": "1e-7"})
    parsed = parse_row(table.columns, tuple(values[name] for name in table.headers))

    assert [issue.code for issue in parsed.issues] == ["invalid_text"]
    assert parsed.candidate_key_evidence == {
        "columns": ["ACCESSION_NUMBER", "CCO_SEQNUM"], "complete": False, "values": None,
    }


def test_fund_child_relationship_key_preserves_one_to_many_grain() -> None:
    contract = load_ncen_contract("fb55228ca976c43955c9a49bccf2bc21c8b70d3c7194f936f13289f06acca737")
    parent = contract.table_for_filename("FUND_REPORTED_INFO.tsv")
    child = contract.table_for_filename("INTER_FUND_LENDING_DETAIL.tsv")
    assert _relationship_keys(parent, {"FUND_ID": "F-1", "ACCESSION_NUMBER": "A"})[0] == "fund:F-1"
    assert _relationship_keys(child, {"FUND_ID": "F-1"})[1] == "fund:F-1"


def test_package_quarter_is_delivery_partition_not_economic_period() -> None:
    assert source_quarter_from_package(Path("2024q1_ncen_0")) == "2024Q1"
    with pytest.raises(Exception):
        source_quarter_from_package(Path("2024q5_ncen"))


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_synthetic_submission_package_materializes_all_53_reconciliations(tmp_path: Path) -> None:
    """One physical TSV plus 52 declared absent tables remains a valid V2 run."""
    import psycopg
    from src.ncen.ingestion import ingest_package
    from src.ncen.storage import install_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    source = Path(r"E:\Edgard\ncen\2021q3_ncen")
    package = tmp_path / "2027q1_ncen"
    package.mkdir()
    for name in ("ncen_metadata.json", "ncen_readme.htm"):
        (package / name).write_bytes((source / name).read_bytes())
    contract = verify_package(source).contract
    submission = contract.table_for_filename("SUBMISSION.tsv")
    values = {name: "" for name in submission.headers}
    values["ACCESSION_NUMBER"] = "NCEN-SYNTHETIC-0001"
    (package / "SUBMISSION.tsv").write_text("\t".join(submission.headers) + "\n" + "\t".join(values[name] for name in submission.headers) + "\n", encoding="utf-8")
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        result = ingest_package(conn, package=package, source_root=tmp_path, parser_version="ncen-synthetic-v2", inventory=build_package_inventory(package, contract))
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM sec_table_reconciliations WHERE run_id=%s", (result["run_id"],))
            reconciliations = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM ncen_raw_v2_rows WHERE ingestion_run_id=%s", (result["run_id"],))
            rows = cur.fetchone()[0]
            cur.execute("SELECT ncen_raw_run_reconciles(%s)", (result["run_id"],))
            reconciles = cur.fetchone()[0]
    assert result["state"] == "raw_validated"
    assert (reconciliations, rows, reconciles) == (53, 1, True)


@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_real_db_reconciles_candidate_parse_error_and_exponent_decimal(tmp_path: Path) -> None:
    """The Python and SQL derivations must agree on unusable keys and fixed-point decimals."""
    import psycopg
    from src.ncen.ingestion import ingest_package
    from src.ncen.storage import install_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    source = Path(r"E:\Edgard\ncen\2021q3_ncen")
    package = tmp_path / "2027q2_ncen"
    package.mkdir()
    for name in ("ncen_metadata.json", "ncen_readme.htm"):
        (package / name).write_bytes((source / name).read_bytes())
    contract = verify_package(source).contract
    for source_file, values in {
        "SUBMISSION.tsv": {"ACCESSION_NUMBER": "x" * 21},
        "CHIEF_COMPLIANCE_OFFICER.tsv": {"ACCESSION_NUMBER": "x" * 21, "CCO_SEQNUM": "1e-7"},
    }.items():
        table = contract.table_for_filename(source_file)
        row = {column.name: "" for column in table.columns}
        row.update(values)
        (package / source_file).write_text(
            "\t".join(table.headers) + "\n" + "\t".join(row[name] for name in table.headers) + "\n",
            encoding="utf-8",
        )
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        result = ingest_package(
            conn, package=package, source_root=tmp_path, parser_version="ncen-candidate-decimal-v2",
            inventory=build_package_inventory(package, contract),
        )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """SELECT typed_projection->>'CCO_SEQNUM', candidate_key_evidence
                     FROM ncen_raw_v2_rows
                     WHERE ingestion_run_id=%s AND source_table='CHIEF_COMPLIANCE_OFFICER.tsv'""",
                (result["run_id"],),
            )
            decimal, evidence = cur.fetchone()
    assert result["state"] == "raw_validated", result
    assert (decimal, evidence) == ("0.0000001", {"columns": ["ACCESSION_NUMBER", "CCO_SEQNUM"], "complete": False, "values": None})
