"""Shared synthetic-fixture helpers for the N-CEN derived-profile snapshots.

DSN-agnostic by design (Global Constraint 9): every caller reads the disposable
Postgres endpoint from ``SEC_TEST_DATABASE_URL`` so the suite runs identically
under the keyword and URL DSN conventions.  The leading underscore keeps pytest
from collecting this module as a test file.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

from src.ncen.schema import json_typed_projection, load_ncen_contract, parse_row

ROOT = Path(__file__).resolve().parents[1]
NCEN_METADATA_SHA = "fb55228ca976c43955c9a49bccf2bc21c8b70d3c7194f936f13289f06acca737"


def dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def base_fixture(cur, product: str | None, ddl_files: tuple[str, ...], *, create_publication: bool = True):
    """Stand up an isolated schema, raw surface, and a prepared publication.

    ``ddl_files`` are applied in order (twice, proving idempotency) after the
    shared publication and effective-selection DDL.  When ``create_publication``
    is False (e.g. the materializer owns publication identity), the publication
    row is left for the caller and the returned ``publication_id`` is None.
    """
    schema = f"ncen_derived_fixture_{uuid4().hex}"
    run_id, package_id, publication_id = uuid4(), uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    cur.execute(
        """CREATE TABLE ncen_raw_v2_rows(
        raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ingestion_run_id uuid,
        source_table text, parse_status text, typed_projection jsonb, accession_number text, fund_id text)"""
    )
    for ddl_name in ("sec_derived_publications.sql", "ncen_effective_views.sql", *ddl_files):
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
        cur.execute(ddl)
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    if create_publication:
        cur.execute(
            """INSERT INTO sec_derived_publications
            (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
            VALUES(%s,%s,1,%s,%s,%s)""",
            (publication_id, product, run_id, package_id, "a" * 64),
        )
    else:
        publication_id = None
    return schema, run_id, package_id, publication_id


def raw(cur, run_id, table, body, *, accession=None, fund_id=None):
    cur.execute(
        """INSERT INTO ncen_raw_v2_rows
        (ingestion_run_id,source_table,parse_status,typed_projection,accession_number,fund_id)
        VALUES(%s,%s,'typed',%s::jsonb,%s,%s)""",
        (run_id, table, json.dumps(body), accession, fund_id),
    )


def submission(cur, run_id, accession, *, cik="C1", form="N-CEN", filing_date="2026-02-01",
               ending="2025-12-31", lt_12month=None):
    body = {
        "ACCESSION_NUMBER": accession, "CIK": cik, "SUBMISSION_TYPE": form,
        "FILING_DATE": filing_date, "REPORT_ENDING_PERIOD": ending,
    }
    if lt_12month is not None:
        body["IS_REPORT_PERIOD_LT_12MONTH"] = lt_12month
    raw(cur, run_id, "SUBMISSION.tsv", body, accession=accession)


def fund(cur, run_id, accession, fund_id, series_id, **fields):
    table = load_ncen_contract(NCEN_METADATA_SHA).table_for_filename("FUND_REPORTED_INFO.tsv")
    lexical = {name: "" for name in table.headers}
    lexical.update({"ACCESSION_NUMBER": accession, "FUND_ID": fund_id, "SERIES_ID": series_id, **fields})
    parsed = parse_row(table.columns, tuple(lexical[name] for name in table.headers))
    assert parsed.parse_status == "typed", parsed.issues
    raw(cur, run_id, "FUND_REPORTED_INFO.tsv", json_typed_projection(parsed.typed),
        accession=accession, fund_id=fund_id)


def registrant(cur, run_id, accession, **fields):
    body = {"ACCESSION_NUMBER": accession, **fields}
    raw(cur, run_id, "REGISTRANT.tsv", body, accession=accession)


def prepare_second_run(cur):
    run_id, package_id = uuid4(), uuid4()
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    return run_id
