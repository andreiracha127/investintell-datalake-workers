"""The effective-selection matview cache: equivalence, freshness and fallback.

The whole point of the cache is that it can never answer differently from the
view it mirrors.  These tests pin the three ways that is enforced:

* the matview content equals the view content (both directions of EXCEPT ALL);
* a matview whose family landed a new validated run is NOT read (the recorded
  signature no longer matches), so the answer comes from the view;
* an absent/unpopulated matview, or an unusable state table, resolves to the view.
"""
from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from src import sec_effective_matviews as mvs

ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"


def _schema_dsn(schema: str) -> str:
    return f"{DSN} options=-csearch_path={schema}"


def _install_effective_surface(cur, schema: str) -> None:
    """A minimal N-CEN + RR1 landing surface plus the two effective views."""
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute(
        "CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, source_family text,"
        " raw_validated_at timestamptz)"
    )
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, source_family, raw_validated_at FROM sec_ingestion_runs "
        "WHERE raw_validated_at IS NOT NULL"
    )
    cur.execute(
        "CREATE TABLE ncen_raw_v2_rows(raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
        " ingestion_run_id uuid, source_table text, parse_status text, typed_projection jsonb,"
        " parse_errors jsonb NOT NULL DEFAULT '[]', original_lexical_row jsonb NOT NULL DEFAULT '{}')"
    )
    cur.execute(
        "CREATE TABLE rr1_raw_v2_rows(raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
        " ingestion_run_id uuid, source_table text, parse_status text, typed_projection jsonb,"
        " adsh text, tag text, version text, series text, class text, measure text,"
        " document text, otherdims text, iprx text)"
    )
    for name in ("ncen_effective_views.sql", "rr1_effective_views.sql"):
        cur.execute((ROOT / "schemas" / name).read_text(encoding="utf-8"))


def _land_ncen(cur, run_id, *, accession: str, cik: str, period: str, filed: str) -> None:
    cur.execute(
        "INSERT INTO ncen_raw_v2_rows(ingestion_run_id,source_table,parse_status,typed_projection)"
        " VALUES(%s,'SUBMISSION.tsv','typed',%s::jsonb)",
        (run_id, json.dumps({
            "ACCESSION_NUMBER": accession, "CIK": cik, "SUBMISSION_TYPE": "N-CEN",
            "FILING_DATE": filed, "REPORT_ENDING_PERIOD": period,
        })),
    )


def _land_rr1(cur, run_id, *, adsh: str, effective: str, ddate: str) -> None:
    cur.execute(
        "INSERT INTO rr1_raw_v2_rows(ingestion_run_id,source_table,parse_status,typed_projection,adsh)"
        " VALUES(%s,'sub.tsv','typed',%s::jsonb,%s)",
        (run_id, json.dumps({
            "effdate": effective, "accepted": f"{effective}T10:00:00",
            "filed": effective, "form": "N-1A",
        }), adsh),
    )
    cur.execute(
        "INSERT INTO rr1_raw_v2_rows(ingestion_run_id,source_table,parse_status,typed_projection,"
        "adsh,tag,version,series,class,measure,document,otherdims,iprx)"
        " VALUES(%s,'num.tsv','typed',%s::jsonb,%s,'Tag','rr/1','S','C','M','D','X','1')",
        (run_id, json.dumps({"ddate": ddate, "adsh": adsh}), adsh),
    )


@pytest.fixture()
def effective_schema():
    import psycopg

    schema = f"effective_mv_fixture_{uuid4().hex}"
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        _install_effective_surface(cur, schema)
        yield schema, conn, cur
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_matviews_mirror_their_views_and_serve_the_same_watermark(effective_schema) -> None:
    import psycopg

    schema, conn, cur = effective_schema
    ncen_run, rr1_run = uuid4(), uuid4()
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,'ncen',now())", (ncen_run,))
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,'rr1',now())", (rr1_run,))
    _land_ncen(cur, ncen_run, accession="A0", cik="C1", period="2023-12-31", filed="2024-01-10")
    _land_ncen(cur, ncen_run, accession="A1", cik="C2", period="2024-06-30", filed="2024-07-10")
    _land_rr1(cur, rr1_run, adsh="R0", effective="2024-03-01", ddate="2024-02-29")
    _land_rr1(cur, rr1_run, adsh="R1", effective="2024-09-01", ddate="2024-08-31")

    cur.execute((ROOT / "schemas" / "sec_effective_matviews.sql").read_text(encoding="utf-8"))

    # Before the first refresh the matviews exist but carry no data: the resolver
    # must keep every caller on the views.
    assert mvs.resolve_relation(conn, "ncen_effective_filings") == "ncen_effective_filings"
    assert mvs.resolve_relation(conn, "rr1_effective_facts") == "rr1_effective_facts"

    outcomes = mvs.refresh_stale(_schema_dsn(schema))
    assert [o["state"] for o in outcomes] == ["refreshed", "refreshed"]
    # The first refresh cannot be CONCURRENTLY (the matviews are created WITH NO DATA).
    assert [o["concurrently"] for o in outcomes] == [False, False]

    assert mvs.resolve_relation(conn, "ncen_effective_filings") == "ncen_effective_filings_mv"
    assert mvs.resolve_relation(conn, "rr1_effective_facts") == "rr1_effective_fact_calendar_mv"

    # Equivalence, both directions (this is the query the swap plan runs in prod).
    cur.execute(
        "SELECT (SELECT count(*) FROM (SELECT * FROM ncen_effective_filings"
        "        EXCEPT ALL SELECT * FROM ncen_effective_filings_mv) d),"
        "       (SELECT count(*) FROM (SELECT * FROM ncen_effective_filings_mv"
        "        EXCEPT ALL SELECT * FROM ncen_effective_filings) d)"
    )
    assert cur.fetchone() == (0, 0)
    cur.execute(
        "WITH live AS (SELECT effective_date, count(*)::bigint AS publishable_rows,"
        "                     count(DISTINCT accession_number)::bigint AS publishable_accessions"
        "              FROM rr1_effective_facts GROUP BY effective_date)"
        "SELECT (SELECT count(*) FROM (TABLE live EXCEPT ALL"
        "        SELECT * FROM rr1_effective_fact_calendar_mv) d),"
        "       (SELECT count(*) FROM (SELECT * FROM rr1_effective_fact_calendar_mv"
        "        EXCEPT ALL TABLE live) d)"
    )
    assert cur.fetchone() == (0, 0)

    # The watermark the daily chain actually reads is identical either way.
    cur.execute(
        "SELECT (SELECT max(effective_date) FROM ncen_effective_filings)"
        "     = (SELECT max(effective_date) FROM ncen_effective_filings_mv),"
        "       (SELECT max(effective_date) FROM rr1_effective_facts)"
        "     = (SELECT max(effective_date) FROM rr1_effective_fact_calendar_mv)"
    )
    assert cur.fetchone() == (True, True)

    # A second pass with nothing landed is a no-op: no raw selection is expanded.
    assert [o["state"] for o in mvs.refresh_stale(_schema_dsn(schema))] == ["fresh", "fresh"]

    # A newly validated run moves the family signature, so the cache stops being
    # read BEFORE anyone refreshes it -- the fallback is the view, never a stale
    # matview.
    later = uuid4()
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,'ncen',now())", (later,))
    _land_ncen(cur, later, accession="A2", cik="C3", period="2025-06-30", filed="2025-07-10")
    assert mvs.resolve_relation(conn, "ncen_effective_filings") == "ncen_effective_filings"
    assert mvs.resolve_relation(conn, "rr1_effective_facts") == "rr1_effective_fact_calendar_mv"
    cur.execute("SELECT max(effective_date) FROM ncen_effective_filings")
    assert cur.fetchone()[0].isoformat() == "2025-06-30"

    outcomes = mvs.refresh_stale(_schema_dsn(schema))
    by_name = {o["matview"]: o for o in outcomes}
    assert by_name["ncen_effective_filings_mv"]["state"] == "refreshed"
    # The refresh of a populated matview is the CONCURRENTLY form.
    assert by_name["ncen_effective_filings_mv"]["concurrently"] is True
    assert by_name["rr1_effective_fact_calendar_mv"]["state"] == "fresh"
    assert mvs.resolve_relation(conn, "ncen_effective_filings") == "ncen_effective_filings_mv"
    cur.execute("SELECT max(effective_date) FROM ncen_effective_filings_mv")
    assert cur.fetchone()[0].isoformat() == "2025-06-30"

    # force re-refreshes a fresh matview (the operator's re-proof path).
    assert [o["state"] for o in mvs.refresh_stale(_schema_dsn(schema), force=True)] == [
        "refreshed", "refreshed",
    ]
    assert psycopg is not None  # import kept meaningful for the fixture contract


def test_uninstalled_cache_reports_and_never_diverts_a_read(effective_schema) -> None:
    schema, conn, cur = effective_schema
    assert mvs.refresh_stale(_schema_dsn(schema)) == [
        {"state": "not_installed", "detail": "sec_effective_matview_state"}
    ]
    assert mvs.resolve_relation(conn, "ncen_effective_filings") == "ncen_effective_filings"
    assert mvs.resolve_relation(conn, "rr1_effective_facts") == "rr1_effective_facts"
    # An unregistered relation is returned untouched.
    assert mvs.resolve_relation(conn, "bond_price_observation") == "bond_price_observation"


def test_registry_covers_every_matview_in_the_ddl() -> None:
    ddl = (ROOT / "schemas" / "sec_effective_matviews.sql").read_text(encoding="utf-8")
    for entry in mvs.REGISTRY:
        assert f"CREATE MATERIALIZED VIEW IF NOT EXISTS {entry.name}" in ddl
        # REFRESH ... CONCURRENTLY is impossible without a UNIQUE index.
        assert f"CREATE UNIQUE INDEX IF NOT EXISTS {entry.name}_pk" in ddl
    assert "sec_effective_matview_state" in ddl
    # The DDL is a migration: it must never be wired into a worker's install path.
    assert "sec_effective_matviews.sql" not in (
        (ROOT / "src" / "rr1" / "derived_profiles.py").read_text(encoding="utf-8")
    )
