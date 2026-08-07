from pathlib import Path
import hashlib
import json
import os

import pytest

from src.ncen.ingestion import _relationship_keys, source_quarter_from_package
from src.ncen.inventory import CANONICAL_PACKAGE_INVENTORIES, build_package_inventory
from src.ncen.schema import load_ncen_contract, parse_row, verify_package
from src.sec_regulatory.contracts import ContractError

CORPUS = Path(r"E:\Edgard\ncen")
requires_corpus = pytest.mark.skipif(not CORPUS.is_dir(), reason="corpus físico N-CEN ausente")


def _column(name: str, *, target: str, policy: str = "text_preserve_lexical", key: bool = False) -> dict[str, object]:
    return {"name": name, "parsing_policy": policy, "required": False, "candidate_key": key,
            "datatype": {"base": "text", "maxLength": 64}, "raw_target": target}


def _synthetic_delivery(tmp_path: Path, monkeypatch, *, tsvs: dict[str, list[str]] | None = None) -> Path:
    """A governed two-table N-CEN contract plus a delivery that satisfies it.

    Nothing here touches the frozen corpus, so the relaxed verification path is
    exercised in any environment (including CI, which has no physical corpus).
    """
    package = tmp_path / "2099q1_ncen"
    package.mkdir()
    (package / "ncen_metadata.json").write_text('{"synthetic": true}', encoding="utf-8")
    (package / "ncen_readme.htm").write_text("<html></html>", encoding="utf-8")
    metadata_sha = hashlib.sha256((package / "ncen_metadata.json").read_bytes()).hexdigest()
    contract = {
        "family": "ncen", "contract_version": "synthetic-v1",
        "schema_variants": [{"metadata_sha256": metadata_sha, "tables": [
            {"source_file": "SUBMISSION.tsv", "raw_target": "ncen_submission_raw",
             "columns": [_column("ACCESSION_NUMBER", target="ncen_submission_raw", key=True)],
             "candidate_primary_key": ["ACCESSION_NUMBER"], "logical_parents": []},
            {"source_file": "REGISTRANT.tsv", "raw_target": "ncen_registrant_raw",
             "columns": [_column("ACCESSION_NUMBER", target="ncen_registrant_raw", key=True),
                         _column("REGISTRANT_NAME", target="ncen_registrant_raw")],
             "candidate_primary_key": ["ACCESSION_NUMBER"], "logical_parents": ["SUBMISSION.tsv"]},
        ]}],
    }
    contract_path = tmp_path / "ncen.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    monkeypatch.setattr("src.ncen.schema.CONTRACT_PATH", contract_path)
    for name, lines in (tsvs if tsvs is not None else {"SUBMISSION.tsv": ["ACCESSION_NUMBER", "NCEN-0001"]}).items():
        (package / name).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return package


def test_package_outside_corpus_self_certifies_without_committed_inventory(tmp_path, monkeypatch) -> None:
    """Activating a new delivery is a data operation, not a repository commit."""
    package = _synthetic_delivery(tmp_path, monkeypatch)
    assert package.name not in CANONICAL_PACKAGE_INVENTORIES

    verified = verify_package(package)

    assert set(verified.file_hashes) == {"SUBMISSION.tsv"}
    assert verified.explicit_zero_tables == ("REGISTRANT.tsv",)
    assert verified.file_inventory["SUBMISSION.tsv"].row_count == 1
    # Deriving the evidence twice from unchanged bytes is stable: the digest the
    # manifests record on the first ingestion is the digest later runs compare.
    assert verify_package(package) == verified


def test_self_certified_package_still_refuses_a_tsv_outside_the_contract(tmp_path, monkeypatch) -> None:
    package = _synthetic_delivery(tmp_path, monkeypatch, tsvs={
        "SUBMISSION.tsv": ["ACCESSION_NUMBER", "NCEN-0001"], "SURPRISE.tsv": ["ANYTHING", "x"],
    })
    with pytest.raises(ContractError, match="não fecham o contrato"):
        verify_package(package)


def test_self_certified_package_still_refuses_header_drift(tmp_path, monkeypatch) -> None:
    package = _synthetic_delivery(tmp_path, monkeypatch, tsvs={
        "SUBMISSION.tsv": ["ACCESSION_NUMBER", "NCEN-0001"],
        "REGISTRANT.tsv": ["REGISTRANT_NAME\tACCESSION_NUMBER", "Fundo\tNCEN-0001"],
    })
    with pytest.raises(ContractError, match="cabeçalho ou ordem divergente"):
        verify_package(package)


def test_self_certified_package_still_refuses_an_ungoverned_metadata_variant(tmp_path, monkeypatch) -> None:
    """Relaxing the inventory does not relax the contract: a new schema is still a code change."""
    package = _synthetic_delivery(tmp_path, monkeypatch)
    (package / "ncen_metadata.json").write_text('{"synthetic": "other"}', encoding="utf-8")
    with pytest.raises(ContractError, match="não seleciona exatamente uma variante"):
        verify_package(package)


def test_recorded_evidence_still_refuses_changed_bytes(tmp_path, monkeypatch) -> None:
    """The honesty scaffold that survives: bytes are compared against what was recorded."""
    package = _synthetic_delivery(tmp_path, monkeypatch)
    recorded = verify_package(package)
    (package / "SUBMISSION.tsv").write_text("ACCESSION_NUMBER\nNCEN-0002\n", encoding="utf-8")

    with pytest.raises(ContractError, match="evidência física divergente"):
        verify_package(package, inventory=recorded_inventory(recorded))
    # And the freshly derived evidence is a different package digest, which is
    # what ingest_package compares against the manifest of a loaded path.
    assert verify_package(package).package_sha256 != recorded.package_sha256


def recorded_inventory(verified):
    from src.ncen.inventory import PackageInventory
    return PackageInventory(verified.metadata_sha256, verified.file_inventory,
                            verified.explicit_zero_tables, verified.package_sha256, verified.inventory_digest)


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
    """The frozen corpus keeps verifying against its committed evidence.

    Deliveries that arrived after the corpus was frozen self-certify instead of
    demanding a commit, so the disk is a superset of the canonical inventory —
    the retroactive guarantee for the 17 inventoried packages is unchanged.
    """
    root = Path(r"E:\Edgard\ncen")
    assert set(CANONICAL_PACKAGE_INVENTORIES) <= {path.name for path in root.iterdir() if path.is_dir()}
    assert len(CANONICAL_PACKAGE_INVENTORIES) == 17
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


@requires_corpus
@pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente")
def test_uninventoried_package_lands_resumes_and_still_refuses_corruption(tmp_path: Path) -> None:
    """The whole point of the relaxation, end to end, with the scaffold intact."""
    import psycopg
    from src.ncen.ingestion import ingest_package
    from src.ncen.storage import install_schema
    from src.sec_regulatory.manifests import install_schema as install_manifest_schema

    source = CORPUS / "2021q3_ncen"
    package = tmp_path / "2028q1_ncen"
    package.mkdir()
    for name in ("ncen_metadata.json", "ncen_readme.htm"):
        (package / name).write_bytes((source / name).read_bytes())
    contract = verify_package(source).contract
    submission = contract.table_for_filename("SUBMISSION.tsv")
    values = {name: "" for name in submission.headers}
    values["ACCESSION_NUMBER"] = "NCEN-UNINVENTORIED-0001"
    body = "\t".join(submission.headers) + "\n" + "\t".join(values[name] for name in submission.headers) + "\n"
    (package / "SUBMISSION.tsv").write_text(body, encoding="utf-8")
    assert package.name not in CANONICAL_PACKAGE_INVENTORIES

    version = "ncen-uninventoried-v2"
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        install_manifest_schema(conn)
        install_schema(conn)
        landed = ingest_package(conn, package=package, source_root=tmp_path, parser_version=version)
        conn.commit()
        resumed = ingest_package(conn, package=package, source_root=tmp_path, parser_version=version)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM ncen_raw_v2_rows WHERE ingestion_run_id=%s", (landed["run_id"],))
            rows = cur.fetchone()[0]
            cur.execute("SELECT ncen_raw_run_reconciles(%s)", (landed["run_id"],))
            reconciles = cur.fetchone()[0]
            cur.execute(
                """SELECT package_sha256, count(*) FROM sec_source_packages
                    WHERE source_family='ncen' AND package_relative_path=%s GROUP BY 1""",
                (package.name,),
            )
            recorded_sha, package_rows = cur.fetchone()
        # The evidence recorded on first sight is what a corrupted re-delivery
        # is measured against; the loaded rows stay untouched.
        (package / "SUBMISSION.tsv").write_text(body.replace("0001", "0002"), encoding="utf-8")
        corrupted = ingest_package(conn, package=package, source_root=tmp_path, parser_version=version)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM ncen_raw_v2_rows WHERE ingestion_run_id=%s", (landed["run_id"],))
            rows_after = cur.fetchone()[0]
            cur.execute(
                "SELECT package_sha256 FROM sec_source_packages WHERE source_family='ncen' AND package_relative_path=%s",
                (package.name,),
            )
            sha_after = cur.fetchone()[0]

    assert landed["state"] == "raw_validated", landed
    assert (rows, reconciles, package_rows) == (1, True, 1)
    assert resumed == {"package": package.name, "run_id": landed["run_id"], "state": "raw_validated", "resumed": True}
    assert corrupted["state"] == "failed" and "bytes mudaram" in corrupted["reason"]
    assert (rows_after, sha_after) == (1, recorded_sha)
