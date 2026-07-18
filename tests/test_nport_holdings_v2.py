from __future__ import annotations

from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"


def test_holdings_v2_current_only_publishes_resolved_temporal_bridge_rows() -> None:
    import psycopg

    schema = f"nport_holdings_v2_fixture_{uuid4().hex}"
    run_id, package_id, publication_id = uuid4(), uuid4(), uuid4()
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
            cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
            cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
            cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid)")
            cur.execute((ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8"))
            ddl = (ROOT / "schemas" / "nport_holdings_v2.sql").read_text(encoding="utf-8")
            cur.execute(ddl)
            cur.execute(ddl)
            cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
            cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
            cur.execute("""INSERT INTO sec_derived_publications
                (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint,validated_at,lifecycle_state)
                VALUES(%s,'sec_nport_holdings_v2',1,%s,%s,%s,now(),'validated')""",
                (publication_id, run_id, package_id, "a" * 64))
            for holding, state, instrument, series, klass, start, end in (
                ("H1", "resolved", "I1", "S1", "C1", "2024-01-01", None),
                ("H2", "ambiguous", "I2", None, None, "2024-01-01", None),
                ("H3", "orphan", "I3", None, None, "2024-02-01", None),
            ):
                cur.execute("""INSERT INTO sec_nport_instrument_class_bridge
                    (publication_id,accession_number,holding_id,instrument_id,series_id,class_id,valid_from,valid_to,resolution_state)
                    VALUES(%s,'A1',%s,%s,%s,%s,%s,%s,%s)""",
                    (publication_id, holding, instrument, series, klass, start, end, state))
                cur.execute("""INSERT INTO sec_nport_holdings_v2
                    (publication_id,accession_number,holding_id,source_run_id,report_date,filing_date,source_series_id,
                     issuer_name,issuer_category,cusip,signed_market_value,signed_pct_of_nav,payoff_profile,source_typed_projection)
                    VALUES(%s,'A1',%s,%s,'2024-01-31','2024-02-15','S-source',%s,'Corporate','037833100',%s,%s,'Long','{}')""",
                    (publication_id, holding, run_id, holding, 100 if holding == "H1" else -1, 10 if holding == "H1" else -1))
            cur.execute("SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)", (publication_id,))
            cur.execute("SELECT holding_id,series_id,class_id,instrument_id,issuer_category,signed_market_value,signed_pct_of_nav FROM sec_nport_holdings_v2_current")
            assert cur.fetchall() == [("H1", "S1", "C1", "I1", "Corporate", 100, 10)]
            cur.execute("SELECT resolution_state,count(*) FROM sec_nport_holdings_v2_bridge_status GROUP BY resolution_state ORDER BY resolution_state")
            assert cur.fetchall() == [("ambiguous", 1), ("orphan", 1), ("resolved", 1)]
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_holdings_v2_uses_versioned_publication_and_never_relabels_issuer_category() -> None:
    ddl = (ROOT / "schemas" / "nport_holdings_v2.sql").read_text(encoding="utf-8")
    assert "sec_derived_publications" in ddl
    assert "sec_current_derived_publications" in ddl
    assert "resolution_state='resolved'" in ddl
    assert "issuer_category" in ddl
    assert "economic_sector" not in ddl
    assert "UPDATE nport_raw_rows" not in ddl
    assert "DELETE FROM nport_raw_rows" not in ddl
