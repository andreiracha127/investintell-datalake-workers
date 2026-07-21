"""Shared synthetic-fixture helpers for the RR1 derived-profile snapshots.

DSN-agnostic by design (Global Constraint 9): every caller reads the disposable
Postgres endpoint from ``SEC_TEST_DATABASE_URL`` so the suite runs identically
under the keyword and URL DSN conventions.  The leading underscore keeps pytest
from collecting this module as a test file.

The snapshots consume only the amendment-aware effective selection, so the fixture
stands up the ``rr1_effective_facts`` shape directly (the same contract the frozen
``rr1_effective_views.sql`` emits) rather than the heavy raw-provenance surface.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def dsn() -> str:
    return os.environ["SEC_TEST_DATABASE_URL"]


def base_fixture(cur, product: str | None, ddl_files: tuple[str, ...], *, create_publication: bool = True):
    """Stand up an isolated schema, effective-fact surface, and a prepared publication.

    ``ddl_files`` are applied in order (twice, proving idempotency) after the
    shared publication, derived-common, and effective-fact DDL.  When
    ``create_publication`` is False the publication row is left for the caller
    (e.g. the materializer owns publication identity) and ``publication_id`` is None.
    """
    schema = f"rr1_derived_fixture_{uuid4().hex}"
    run_id, package_id, publication_id = uuid4(), uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    cur.execute(
        """CREATE TABLE rr1_effective_facts(
        raw_row_id bigint, ingestion_run_id uuid, source_table text, accession_number text,
        tag text, version text, data_date date, series_id text, class_id text, measure_id text,
        document_id text, dimensions text, occurrence text, fact_typed_projection jsonb,
        effective_date date, accepted_at timestamptz, filed_date date, form text)"""
    )
    for ddl_name in ("sec_derived_publications.sql", "rr1_derived_common.sql", *ddl_files):
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


def fact(cur, run_id, tag, value, *, source_table="num.tsv", version="rr/2025", uom="pure",
         accession="A1", series="S1", class_id="C1", document="D1", measure="", dimensions="",
         occurrence="1", data_date="2025-12-31", effective="2026-01-01", filed="2026-01-02",
         form="N-1A", raw_row_id=1):
    """Insert one effective fact.  ``value``/``uom`` land in the typed projection;
    a ``value`` of None models a reported-but-empty numeric fact."""
    projection: dict[str, str] = {}
    if uom is not None:
        projection["uom"] = uom
    if value is not None:
        projection["value"] = value
    cur.execute(
        """INSERT INTO rr1_effective_facts VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,now(),%s,%s)""",
        (raw_row_id, run_id, source_table, accession, tag, version, data_date, series, class_id,
         measure, document, dimensions, occurrence, json.dumps(projection), effective, filed, form),
    )
