from __future__ import annotations

from pathlib import Path
import json
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]

_PRIOR_NCEN_VIEW_DDL = """
CREATE VIEW ncen_effective_filing_candidates AS
SELECT r.raw_row_id, r.ingestion_run_id,
       r.typed_projection->>'ACCESSION_NUMBER' AS accession_number,
       r.typed_projection->>'CIK' AS registrant_cik,
       r.typed_projection->>'SUBMISSION_TYPE' AS form,
       (r.typed_projection->>'FILING_DATE')::date AS filing_date,
       NULL::timestamptz AS accepted_at,
       (r.typed_projection->>'REPORT_ENDING_PERIOD')::date AS effective_date,
       CASE WHEN r.typed_projection->>'SUBMISSION_TYPE'='N-CEN/A' THEN 1 ELSE 0 END AS is_amendment,
       NULL::text AS amends_accession_number
FROM ncen_raw_v2_rows r JOIN sec_validated_raw_runs v ON v.run_id=r.ingestion_run_id
WHERE r.source_table='SUBMISSION.tsv' AND r.parse_status='typed';

CREATE VIEW ncen_effective_filing_selection AS
SELECT c.*, 1::bigint AS precedence_rank, 1::bigint AS winning_candidate_count,
       'publishable'::text AS selection_state
FROM ncen_effective_filing_candidates c;

CREATE VIEW ncen_effective_filings AS
SELECT raw_row_id,ingestion_run_id,accession_number,registrant_cik,form,filing_date,
       accepted_at,effective_date,is_amendment,amends_accession_number
FROM ncen_effective_filing_selection;
"""


def test_ncen_effective_view_uses_validated_typed_submission_rows_and_rejects_notice_forms() -> None:
    ddl = (ROOT / "schemas" / "ncen_effective_views.sql").read_text(encoding="utf-8")
    for token in (
        "ncen_effective_filing_candidates",
        "ncen_effective_filing_selection",
        "ncen_effective_filings",
        "sec_validated_raw_runs",
        "SUBMISSION.tsv",
        "parse_status='typed'",
        "IS_REPORT_PERIOD_LT_12MONTH",
        "jsonb_array_length(to_jsonb(r)->'parse_errors')=1",
        "N-CEN/A",
        "NT N-CEN",
        "ambiguous",
        "amends_accession_number",
    ):
        assert token in ddl


def test_ncen_effective_view_never_uses_raw_run_state_as_a_serving_surface() -> None:
    ddl = (ROOT / "schemas" / "ncen_effective_views.sql").read_text(encoding="utf-8")
    assert "sec_w1_nport_real" not in ddl
    assert "MAX(" not in ddl


def test_ncen_effective_selection_prefers_amendment_excludes_unvalidated_and_marks_ties_ambiguous() -> None:
    import psycopg

    schema = f"ncen_effective_fixture_{uuid4().hex}"
    run_a, run_b, invalid = uuid4(), uuid4(), uuid4()
    dsn = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
    rows = [
        (run_a, {"ACCESSION_NUMBER": "A0", "CIK": "C1", "SUBMISSION_TYPE": "N-CEN", "FILING_DATE": "2024-01-10", "REPORT_ENDING_PERIOD": "2023-12-31"}),
        (run_b, {"ACCESSION_NUMBER": "A1", "CIK": "C1", "SUBMISSION_TYPE": "N-CEN/A", "FILING_DATE": "2024-02-10", "REPORT_ENDING_PERIOD": "2023-12-31"}),
        (invalid, {"ACCESSION_NUMBER": "BAD", "CIK": "C1", "SUBMISSION_TYPE": "N-CEN/A", "FILING_DATE": "2025-01-01", "REPORT_ENDING_PERIOD": "2023-12-31"}),
        (run_a, {"ACCESSION_NUMBER": "T1", "CIK": "C2", "SUBMISSION_TYPE": "N-CEN", "FILING_DATE": "2024-01-10", "REPORT_ENDING_PERIOD": "2023-12-31"}),
        (run_b, {"ACCESSION_NUMBER": "T2", "CIK": "C2", "SUBMISSION_TYPE": "N-CEN", "FILING_DATE": "2024-01-10", "REPORT_ENDING_PERIOD": "2023-12-31"}),
        (run_a, {"ACCESSION_NUMBER": "M0", "CIK": "C3", "SUBMISSION_TYPE": "N-CEN", "FILING_DATE": "2024-01-01", "REPORT_ENDING_PERIOD": "2023-12-31"}),
        (run_b, {"ACCESSION_NUMBER": "M1", "CIK": "C3", "SUBMISSION_TYPE": "N-CEN", "FILING_DATE": "2024-01-02", "REPORT_ENDING_PERIOD": "2023-12-31"}),
        (run_b, {"ACCESSION_NUMBER": "MA", "CIK": "C3", "SUBMISSION_TYPE": "N-CEN/A", "FILING_DATE": "2024-02-01", "REPORT_ENDING_PERIOD": "2023-12-31"}),
        (run_a, {"ACCESSION_NUMBER": "OA", "CIK": "C4", "SUBMISSION_TYPE": "N-CEN/A", "FILING_DATE": "2024-02-01", "REPORT_ENDING_PERIOD": "2023-12-31"}),
    ]
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
            cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
            cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
            cur.execute("CREATE TABLE ncen_raw_v2_rows(raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ingestion_run_id uuid, source_table text, parse_status text, typed_projection jsonb, parse_errors jsonb NOT NULL DEFAULT '[]', original_lexical_row jsonb NOT NULL DEFAULT '{}')")
            cur.executemany("INSERT INTO sec_ingestion_runs VALUES(%s,now())", [(run_a,), (run_b,)])
            cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,NULL)", (invalid,))
            cur.executemany("INSERT INTO ncen_raw_v2_rows(ingestion_run_id,source_table,parse_status,typed_projection) VALUES(%s,'SUBMISSION.tsv','typed',%s::jsonb)", [(run, json.dumps(body)) for run, body in rows])
            cur.execute((ROOT / "schemas" / "ncen_effective_views.sql").read_text(encoding="utf-8"))
            cur.execute("SELECT accession_number FROM ncen_effective_filings WHERE registrant_cik='C1'")
            assert cur.fetchone()[0] == "A1"
            cur.execute("SELECT count(*) FROM ncen_effective_filings WHERE registrant_cik='C2'")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM ncen_effective_filing_selection WHERE registrant_cik='C2' AND selection_state='ambiguous'")
            assert cur.fetchone()[0] == 2
            cur.execute("SELECT count(*) FROM ncen_effective_filings WHERE registrant_cik IN ('C3','C4')")
            assert cur.fetchone()[0] == 0
            cur.execute("""SELECT registrant_cik,base_accession_count,amends_accession_number,selection_state
                FROM ncen_effective_filing_selection WHERE accession_number IN ('MA','OA') ORDER BY registrant_cik""")
            assert cur.fetchall() == [("C3", 2, None, "ambiguous"), ("C4", 0, None, "ambiguous")]
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_ncen_effective_views_upgrade_from_prior_column_contract() -> None:
    import psycopg

    schema = f"ncen_upgrade_fixture_{uuid4().hex}"
    run_id = uuid4()
    dsn = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
    rows = (
        {"ACCESSION_NUMBER": "B0", "CIK": "C1", "SUBMISSION_TYPE": "N-CEN", "FILING_DATE": "2024-01-01", "REPORT_ENDING_PERIOD": "2023-12-31"},
        {"ACCESSION_NUMBER": "B1", "CIK": "C1", "SUBMISSION_TYPE": "N-CEN", "FILING_DATE": "2024-01-02", "REPORT_ENDING_PERIOD": "2023-12-31"},
        {"ACCESSION_NUMBER": "BA", "CIK": "C1", "SUBMISSION_TYPE": "N-CEN/A", "FILING_DATE": "2024-02-01", "REPORT_ENDING_PERIOD": "2023-12-31"},
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
            cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
            cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
            cur.execute("CREATE TABLE ncen_raw_v2_rows(raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ingestion_run_id uuid, source_table text, parse_status text, typed_projection jsonb, parse_errors jsonb NOT NULL DEFAULT '[]', original_lexical_row jsonb NOT NULL DEFAULT '{}')")
            cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
            cur.executemany("""INSERT INTO ncen_raw_v2_rows(ingestion_run_id,source_table,parse_status,typed_projection)
                VALUES(%s,'SUBMISSION.tsv','typed',%s::jsonb)""", [(run_id, json.dumps(row)) for row in rows])
            cur.execute(_PRIOR_NCEN_VIEW_DDL)
            cur.execute((ROOT / "schemas" / "ncen_effective_views.sql").read_text(encoding="utf-8"))
            cur.execute("""SELECT column_name FROM information_schema.columns
                WHERE table_schema=%s AND table_name='ncen_effective_filing_selection' ORDER BY ordinal_position""", (schema,))
            assert [row[0] for row in cur.fetchall()][-2:] == ["selection_state", "base_accession_count"]
            cur.execute("SELECT base_accession_count,selection_state FROM ncen_effective_filing_selection WHERE accession_number='BA'")
            assert cur.fetchone() == (2, "ambiguous")
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_ncen_effective_view_admits_only_the_known_boolean_contract_quarantine() -> None:
    import psycopg

    schema = f"ncen_quarantine_fixture_{uuid4().hex}"
    run_id = uuid4()
    dsn = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
    body = {
        "ACCESSION_NUMBER": "KNOWN",
        "CIK": "C1",
        "SUBMISSION_TYPE": "N-CEN",
        "FILING_DATE": "2024-01-10",
        "REPORT_ENDING_PERIOD": "2023-12-31",
    }
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
        cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
        cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
        cur.execute("CREATE TABLE ncen_raw_v2_rows(raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ingestion_run_id uuid, source_table text, parse_status text, typed_projection jsonb, parse_errors jsonb NOT NULL, original_lexical_row jsonb NOT NULL)")
        cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
        cur.execute(
            """INSERT INTO ncen_raw_v2_rows
               (ingestion_run_id,source_table,parse_status,typed_projection,parse_errors,original_lexical_row)
               VALUES(%s,'SUBMISSION.tsv','quarantined',%s::jsonb,%s::jsonb,%s::jsonb)""",
            (
                run_id,
                json.dumps(body),
                json.dumps([{
                    "code": "invalid_date",
                    "column_name": "IS_REPORT_PERIOD_LT_12MONTH",
                }]),
                json.dumps({"IS_REPORT_PERIOD_LT_12MONTH": "N"}),
            ),
        )
        other = dict(body, ACCESSION_NUMBER="OTHER", CIK="C2")
        cur.execute(
            """INSERT INTO ncen_raw_v2_rows
               (ingestion_run_id,source_table,parse_status,typed_projection,parse_errors,original_lexical_row)
               VALUES(%s,'SUBMISSION.tsv','quarantined',%s::jsonb,%s::jsonb,%s::jsonb)""",
            (
                run_id,
                json.dumps(other),
                json.dumps([{"code": "invalid_date", "column_name": "TERMINATION_DATE"}]),
                json.dumps({"TERMINATION_DATE": "not-a-date"}),
            ),
        )
        cur.execute((ROOT / "schemas" / "ncen_effective_views.sql").read_text(encoding="utf-8"))
        cur.execute("SELECT accession_number FROM ncen_effective_filings ORDER BY accession_number")
        assert cur.fetchall() == [("KNOWN",)]
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
