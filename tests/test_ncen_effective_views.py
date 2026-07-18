from __future__ import annotations

from pathlib import Path
import json
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]


def test_ncen_effective_view_uses_validated_typed_submission_rows_and_rejects_notice_forms() -> None:
    ddl = (ROOT / "schemas" / "ncen_effective_views.sql").read_text(encoding="utf-8")
    for token in (
        "ncen_effective_filing_candidates",
        "ncen_effective_filing_selection",
        "ncen_effective_filings",
        "sec_validated_raw_runs",
        "SUBMISSION.tsv",
        "parse_status='typed'",
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
    ]
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
            cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
            cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
            cur.execute("CREATE TABLE ncen_raw_v2_rows(raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ingestion_run_id uuid, source_table text, parse_status text, typed_projection jsonb)")
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
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
