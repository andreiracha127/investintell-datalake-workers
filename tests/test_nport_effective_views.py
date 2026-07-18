from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"


def _raw_schema(cur) -> None:
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
    cur.execute("""CREATE TABLE nport_raw_rows(
        raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ingestion_run_id uuid,
        source_table text, parse_status text, typed_projection jsonb,
        candidate_key_evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
        accession_number text, holding_id text)""")
    cur.execute("""CREATE TABLE nport_holding_accession_map(
        ingestion_run_id uuid, holding_id text, accession_number text)""")


def _insert_raw(cur, run_id, table: str, body: dict[str, str], *, accession: str | None = None,
                holding: str | None = None, evidence: dict | None = None) -> None:
    cur.execute("""INSERT INTO nport_raw_rows
        (ingestion_run_id,source_table,parse_status,typed_projection,candidate_key_evidence,accession_number,holding_id)
        VALUES(%s,%s,'typed',%s::jsonb,%s::jsonb,%s,%s)""",
        (run_id, table, json.dumps(body), json.dumps(evidence or {"complete": True}), accession, holding))


def test_effective_holdings_and_identifier_surface_preserve_lots_and_fail_closed() -> None:
    import psycopg

    schema = f"nport_effective_fixture_{uuid4().hex}"
    original, amendment, tie_a, tie_b, invalid = (uuid4() for _ in range(5))
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
            _raw_schema(cur)
            cur.executemany("INSERT INTO sec_ingestion_runs VALUES(%s,now())", [(original,), (amendment,), (tie_a,), (tie_b,)])
            cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,NULL)", (invalid,))
            submissions = [
                (original, "O1", "NPORT-P", "2024-02-01", "S1"),
                (amendment, "A1", "NPORT-P/A", "2024-03-01", "S1"),
                (tie_a, "T1", "NPORT-P", "2024-02-01", "S2"),
                (tie_b, "T2", "NPORT-P", "2024-02-01", "S2"),
                (invalid, "BAD", "NPORT-P/A", "2024-12-01", "S1"),
            ]
            for run_id, accession, form, filing, series in submissions:
                submission = {"ACCESSION_NUMBER": accession, "SUB_TYPE": form, "FILING_DATE": filing, "REPORT_DATE": "2024-01-31"}
                _insert_raw(cur, run_id, "SUBMISSION.tsv", submission, accession=accession)
                _insert_raw(cur, run_id, "FUND_REPORTED_INFO.tsv", {"ACCESSION_NUMBER": accession, "SERIES_ID": series}, accession=accession)
            for holding, cusip, lei, value, pct in (
                ("H1", "037833100", "5493001KJTIIGC8Y1R12", "123.45", "12.5"),
                ("H2", "037833100", "", "-20", "-2"),
                ("H3", "N/A", "", "0", "0"),
            ):
                _insert_raw(cur, amendment, "FUND_REPORTED_HOLDING.tsv", {
                    "ACCESSION_NUMBER": "A1", "HOLDING_ID": holding, "ISSUER_NAME": holding,
                    "ISSUER_CUSIP": cusip, "ISSUER_LEI": lei, "ISSUER_TYPE": "Corporate",
                    "CURRENCY_VALUE": value, "PERCENTAGE": pct, "PAYOFF_PROFILE": "Long",
                }, accession="A1", holding=holding)
                cur.execute("INSERT INTO nport_holding_accession_map VALUES(%s,%s,'A1')", (amendment, holding))
            _insert_raw(cur, amendment, "IDENTIFIERS.tsv", {"HOLDING_ID": "H1", "IDENTIFIER_ISIN": "us0378331005"}, holding="H1")
            _insert_raw(cur, amendment, "IDENTIFIERS.tsv", {"HOLDING_ID": "H1", "IDENTIFIER_ISIN": "IE00B4L5Y983"}, holding="H1")
            _insert_raw(cur, amendment, "IDENTIFIERS.tsv", {"HOLDING_ID": "H2", "IDENTIFIER_ISIN": "US0378331005"}, holding="H2", evidence={"complete": False})
            _insert_raw(cur, amendment, "IDENTIFIERS.tsv", {"HOLDING_ID": "H3", "IDENTIFIER_ISIN": "IE00B4L5Y983"}, holding="H3")
            for filename in ("nport_effective_views.sql", "nport_identifier_surface.sql"):
                ddl = (ROOT / "schemas" / filename).read_text(encoding="utf-8")
                cur.execute(ddl)
                cur.execute(ddl)

            cur.execute("SELECT accession_number FROM nport_effective_filings WHERE series_id='S1'")
            assert cur.fetchone() == ("A1",)
            cur.execute("SELECT deterministic_order,selection_state FROM nport_effective_filing_selection WHERE accession_number IN ('T1','T2') ORDER BY accession_number")
            assert cur.fetchall() == [(2, "ambiguous"), (1, "ambiguous")]
            cur.execute("SELECT count(*) FROM nport_effective_filings WHERE series_id='S2'")
            assert cur.fetchone() == (0,)
            cur.execute("SELECT holding_id,signed_market_value,signed_pct_of_nav,issuer_category FROM nport_current_holdings ORDER BY holding_id")
            assert cur.fetchall() == [
                ("H1", Decimal("123.45"), Decimal("12.5"), "Corporate"),
                ("H2", Decimal("-20"), Decimal("-2"), "Corporate"),
                ("H3", Decimal("0"), Decimal("0"), "Corporate"),
            ]
            cur.execute("""SELECT holding_id,cusip,issuer_lei,isin_count,isin_conflict,cusip_placeholder,
                                  incomplete_identifier_candidate_key_evidence
                           FROM nport_identifier_surface ORDER BY holding_id""")
            assert cur.fetchall() == [
                ("H1", "037833100", "5493001KJTIIGC8Y1R12", 2, True, False, False),
                ("H2", "037833100", None, 0, False, False, True),
                ("H3", None, None, 1, False, True, False),
            ]
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_effective_and_identifier_views_are_derived_only() -> None:
    ddl = "\n".join((ROOT / "schemas" / name).read_text(encoding="utf-8")
                    for name in ("nport_effective_views.sql", "nport_identifier_surface.sql"))
    assert "sec_validated_raw_runs" in ddl
    assert "selection_state" in ddl
    assert "ambiguous" in ddl
    assert "UPDATE nport_raw_rows" not in ddl
    assert "DELETE FROM nport_raw_rows" not in ddl
    assert "sec_w1_nport_real" not in ddl
