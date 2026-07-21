from __future__ import annotations

from pathlib import Path
import json
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]


def test_rr1_effective_view_preserves_context_and_marks_ties_nonpublishable() -> None:
    ddl = (ROOT / "schemas" / "rr1_effective_views.sql").read_text(encoding="utf-8")
    for token in (
        "rr1_effective_fact_candidates",
        "rr1_effective_fact_selection",
        "rr1_effective_facts",
        "series_id",
        "class_id",
        "measure_id",
        "document_id",
        "dimensions",
        "accepted_at",
        "occurrence",
        "ambiguous",
        "sec_validated_raw_runs",
    ):
        assert token in ddl
    assert "MAX(filed" not in ddl
    assert "sec_w1_nport_real" not in ddl


def test_rr1_effective_selection_uses_effective_then_accepted_dates_and_fails_closed_on_ties() -> None:
    import psycopg

    schema = f"rr1_effective_fixture_{uuid4().hex}"
    first, second, tie_a, tie_b = uuid4(), uuid4(), uuid4(), uuid4()
    dsn = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
    def sub(effective: str, accepted: str, filed: str) -> dict[str, str]:
        return {"effdate": effective, "accepted": accepted, "filed": filed, "form": "N-1A"}
    def fact(adsh: str, ddate: str) -> dict[str, str]:
        return {"ddate": ddate, "adsh": adsh}
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
            cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
            cur.execute("CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL")
            cur.execute("""CREATE TABLE rr1_raw_v2_rows(raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                ingestion_run_id uuid, source_table text, parse_status text, typed_projection jsonb, adsh text, tag text,
                version text, series text, class text, measure text, document text, otherdims text, iprx text)""")
            cur.executemany("INSERT INTO sec_ingestion_runs VALUES(%s,now())", [(first,), (second,), (tie_a,), (tie_b,)])
            records = [
                (first, "sub.tsv", sub("2024-01-01", "2024-01-03T10:00:00", "2024-12-31"), "OLD"),
                (second, "sub.tsv", sub("2024-02-01", "2024-02-03T10:00:00", "2024-01-02"), "NEW"),
                (tie_a, "sub.tsv", sub("2024-03-01", "2024-03-03T10:00:00", "2024-03-04"), "TA"),
                (tie_b, "sub.tsv", sub("2024-03-01", "2024-03-03T10:00:00", "2024-03-04"), "TB"),
            ]
            cur.executemany("INSERT INTO rr1_raw_v2_rows(ingestion_run_id,source_table,parse_status,typed_projection,adsh) VALUES(%s,%s,'typed',%s::jsonb,%s)", [(r, t, json.dumps(body), adsh) for r,t,body,adsh in records])
            for run, adsh, day in ((first,"OLD","2024-01-31"),(second,"NEW","2024-01-31"),(tie_a,"TA","2024-02-29"),(tie_b,"TB","2024-02-29")):
                cur.execute("""INSERT INTO rr1_raw_v2_rows(ingestion_run_id,source_table,parse_status,typed_projection,adsh,tag,version,series,class,measure,document,otherdims,iprx)
                    VALUES(%s,'num.tsv','typed',%s::jsonb,%s,'Tag','rr/1','S','C','M','D','X','1')""", (run, json.dumps(fact(adsh, day)), adsh))
            cur.execute((ROOT / "schemas" / "rr1_effective_views.sql").read_text(encoding="utf-8"))
            cur.execute("SELECT accession_number FROM rr1_effective_facts WHERE data_date='2024-01-31'")
            assert cur.fetchone()[0] == "NEW"
            cur.execute("SELECT count(*) FROM rr1_effective_facts WHERE data_date='2024-02-29'")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM rr1_effective_fact_selection WHERE data_date='2024-02-29' AND selection_state='ambiguous'")
            assert cur.fetchone()[0] == 2
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
