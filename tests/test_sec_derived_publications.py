from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_derived_publication_ddl_declares_immutable_validated_publications_and_atomic_pointer() -> None:
    ddl = (ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8")

    for token in (
        "sec_derived_publications",
        "sec_derived_current_pointers",
        "publication_version",
        "source_run_id",
        "source_package_id",
        "sec_validate_derived_publication",
        "sec_set_current_derived_publication",
        "pg_advisory_xact_lock",
        "raw_validated_at IS NOT NULL",
    ):
        assert token in ddl
    assert "UPDATE ncen_raw_v2_rows" not in ddl
    assert "UPDATE rr1_raw_v2_rows" not in ddl
    assert "DELETE FROM ncen_raw_v2_rows" not in ddl
    assert "DELETE FROM rr1_raw_v2_rows" not in ddl


def test_derived_publication_ddl_blocks_direct_mutation_after_validation() -> None:
    ddl = (ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8")
    assert "derived publication is immutable" in ddl
    assert "validated derived publication cannot be deleted" in ddl
    assert "current pointer is managed by sec_set_current_derived_publication" in ddl


def test_derived_publication_ddl_declares_monotonic_promotion() -> None:
    """The current pointer never moves backward in data date unless asked to."""
    ddl = (ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8")
    assert "sec_derived_publication_as_of" in ddl
    assert "allow_as_of_regression boolean DEFAULT false" in ddl
    assert "would regress from as_of" in ddl
    # The 2-arg form must be dropped, or 2-arg calls become ambiguous.
    assert "DROP FUNCTION IF EXISTS sec_set_current_derived_publication(text, uuid);" in ddl


def test_derived_publications_apply_twice_and_validation_and_pointer_are_immutable() -> None:
    import psycopg

    schema = f"sec_derived_fixture_{uuid4().hex}"
    dsn = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
    run_id, package_id, publication_id = uuid4(), uuid4(), uuid4()
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}"')
            cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
            cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
            cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
            ddl = (ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8")
            cur.execute(ddl)
            cur.execute(ddl)
            cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
            cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
            cur.execute("""INSERT INTO sec_derived_publications
                (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
                VALUES(%s,'regulatory_mandate',1,%s,%s,%s)""", (publication_id, run_id, package_id, "a" * 64))
            cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
            with pytest.raises(psycopg.Error, match="derived publication is immutable"):
                cur.execute("UPDATE sec_derived_publications SET build_fingerprint=%s WHERE publication_id=%s", ("b" * 64, publication_id))
            conn.rollback()
            cur.execute(f'SET search_path TO "{schema}"')
            cur.execute("BEGIN")
            cur.execute("SELECT sec_set_current_derived_publication('regulatory_mandate',%s)", (publication_id,))
            cur.execute("ROLLBACK")
            cur.execute(f'SET search_path TO "{schema}"')
            cur.execute("SELECT count(*) FROM sec_derived_current_pointers")
            assert cur.fetchone()[0] == 0
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
