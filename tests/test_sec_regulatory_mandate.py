from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_mandate_product_has_independent_fields_with_stable_states_and_provenance() -> None:
    ddl = (ROOT / "schemas" / "sec_regulatory_mandate.sql").read_text(encoding="utf-8")
    for token in (
        "sec_regulatory_mandate_profiles",
        "objective_state",
        "strategy_state",
        "concentration_policy_state",
        "principal_risks_state",
        "available",
        "unavailable",
        "not_applicable",
        "degraded",
        "reason_code",
        "objective_source_date",
        "strategy_source_date",
        "concentration_policy_source_date",
        "principal_risks_source_date",
        "objective_provenance",
        "principal_risks_provenance",
    ):
        assert token in ddl
    assert "issuerCat" not in ddl
    assert "rr1_raw_v2_rows" not in ddl


def test_mandate_product_requires_validated_derived_publication() -> None:
    ddl = (ROOT / "schemas" / "sec_regulatory_mandate.sql").read_text(encoding="utf-8")
    assert "sec_derived_publication_is_validated" in ddl
    assert "mandate profile requires a validated mandate publication" in ddl


def test_mandate_profile_accepts_missing_and_not_applicable_but_is_immutable() -> None:
    import psycopg

    schema = f"mandate_fixture_{uuid4().hex}"
    run_id, package_id, publication_id = uuid4(), uuid4(), uuid4()
    dsn = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
            cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
            cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
            cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
            cur.execute((ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8"))
            cur.execute((ROOT / "schemas" / "sec_regulatory_mandate.sql").read_text(encoding="utf-8"))
            cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
            cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
            cur.execute("""INSERT INTO sec_derived_publications VALUES(%s,'regulatory_mandate',1,%s,%s,%s,now(),NULL,'prepared')""", (publication_id, run_id, package_id, "a" * 64))
            cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
            cur.execute("""INSERT INTO sec_regulatory_mandate_profiles(
                publication_id,series_id,objective_state,objective_reason_code,strategy_state,strategy_reason_code,
                concentration_policy_state,concentration_policy_reason_code,principal_risks_state,principal_risks_reason_code)
                VALUES(%s,'S','unavailable','objective_not_reported','not_applicable','strategy_not_applicable',
                       'unavailable','concentration_not_reported','unavailable','principal_risks_not_reported')""", (publication_id,))
            with pytest.raises(psycopg.Error, match="regulatory mandate profile is immutable"):
                cur.execute("UPDATE sec_regulatory_mandate_profiles SET objective_state='available' WHERE publication_id=%s", (publication_id,))
            conn.rollback()
            cur.execute(f'SET search_path TO "{schema}"')
            cur.execute("SELECT objective_state,strategy_state FROM sec_regulatory_mandate_profiles")
            assert cur.fetchone() == ("unavailable", "not_applicable")
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
