"""Contract tests for the append-only SEC API Render fallback overlay."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4, uuid5

import pytest


ROOT = Path(__file__).resolve().parents[1]
V1_DDL = ROOT / "schemas" / "nport_fixed_income_secapi_sidecars_v1.sql"
DDL = ROOT / "schemas" / "nport_fixed_income_secapi_fallback_v2.sql"
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
SHA = "b" * 64
A1 = "0000000001-26-000001"
A2 = "0000000001-26-000002"


def _v1_document_id(publication_id: str, accession: str) -> str:
    return str(uuid5(UUID(int=0), f"{publication_id}|{accession}"))


def test_fallback_schema_is_a_compact_append_only_overlay() -> None:
    ddl = DDL.read_text(encoding="utf-8")

    assert "nport_fixed_income_secapi_fallback_manifest_v2" in ddl
    assert "nport_fixed_income_secapi_fallback_fund_info_v2" in ddl
    assert "nport_fixed_income_secapi_fallback_rate_risk_v2" in ddl
    assert "nport_fixed_income_secapi_fallback_scope_ready_v2" in ddl
    assert "nport_fixed_income_fund_info_source_v2" in ddl
    assert "nport_fixed_income_rate_risk_source_v2" in ddl
    assert "form_nport_exact_zero" in ddl
    assert "QueryApi" in ddl and "RenderApi" in ddl
    assert "raw_xml" not in ddl.lower()
    assert "render_raw_sha256" in ddl


def _install(cur) -> tuple[str, str, str]:
    schema = f"secapi_fallback_{uuid4().hex}"
    publication_id, run_id = str(uuid4()), str(uuid4())
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute(
        "CREATE MATERIALIZED VIEW nport_holdings_snapshot_identity_v1 AS "
        f"SELECT '{publication_id}'::uuid AS publication_id, '{A1}'::text AS accession_number "
        f"UNION ALL SELECT '{publication_id}'::uuid, '{A2}'::text"
    )
    cur.execute(V1_DDL.read_text(encoding="utf-8"))
    cur.execute(DDL.read_text(encoding="utf-8"))
    cur.execute(DDL.read_text(encoding="utf-8"))
    return schema, publication_id, run_id


def _v1_manifest(cur, publication_id: str, run_id: str, accession: str, status: str) -> None:
    values = [publication_id, run_id, accession, _v1_document_id(publication_id, accession), status]
    if status == "success":
        cur.execute(
            """INSERT INTO nport_fixed_income_secapi_recovery_v1
            (source_holdings_publication_id,source_run_id,accession_number,source_document_id,
             source_row_number,extractor_version,status,attempt_count,payload_sha256,provider_response_sha256)
            VALUES(%s,%s,%s,%s,0,'v1-active',%s,1,%s,%s)""",
            [*values, SHA, SHA],
        )
    else:
        cur.execute(
            """INSERT INTO nport_fixed_income_secapi_recovery_v1
            (source_holdings_publication_id,source_run_id,accession_number,source_document_id,
             source_row_number,extractor_version,status,attempt_count)
            VALUES(%s,%s,%s,%s,0,'v1-active',%s,1)""",
            values,
        )


def _v1_fund(cur, publication_id: str, run_id: str, accession: str) -> None:
    cur.execute(
        """INSERT INTO nport_fixed_income_secapi_fund_info_v1
        (source_holdings_publication_id,source_run_id,accession_number,source_document_id,
         source_row_number,extractor_version,payload_sha256,projection_sha256,compact_payload,
         presence_map,cur_metric_state,cur_metric_count)
        VALUES(%s,%s,%s,%s,0,'v1-active',%s,%s,'{}','{}','empty',0)""",
        (publication_id, run_id, accession, _v1_document_id(publication_id, accession), SHA, SHA),
    )


def _fallback_manifest(cur, publication_id: str, run_id: str, accession: str, *, parser: str = "v2-active") -> str:
    document_id = cur.execute(
        "SELECT nport_fixed_income_secapi_fallback_document_id_v2(%s,%s,%s)",
        (publication_id, run_id, accession),
    ).fetchone()[0]
    cur.execute(
        """INSERT INTO nport_fixed_income_secapi_fallback_manifest_v2
        (source_holdings_publication_id,source_run_id,accession_number,source_document_id,
         parser_version,resolver_version,form_type,document_name,document_url,
         form_nport_response_sha256,query_response_sha256,render_raw_sha256,compact_payload_sha256,status)
        VALUES(%s,%s,%s,%s,%s,'query-render-v1','NPORT-P','primary_doc.xml',
               %s,%s,%s,%s,%s,'success')""",
        (
            publication_id,
            run_id,
            accession,
            document_id,
            parser,
            "https://www.sec.gov/Archives/edgar/data/1/"
            f"{accession.replace('-', '')}/primary_doc.xml",
            SHA,
            SHA,
            SHA,
            SHA,
        ),
    )
    return document_id


def _fallback_fund(cur, publication_id: str, run_id: str, accession: str, document_id: str, *, compact_sha: str = SHA) -> None:
    cur.execute(
        """INSERT INTO nport_fixed_income_secapi_fallback_fund_info_v2
        (source_holdings_publication_id,source_run_id,accession_number,source_document_id,
         parser_version,resolver_version,compact_payload_sha256,projection_sha256,compact_payload,
         presence_map,cur_metric_state,cur_metric_count)
        VALUES(%s,%s,%s,%s,'v2-active','query-render-v1',%s,%s,'{}','{}','empty',0)""",
        (publication_id, run_id, accession, document_id, compact_sha, SHA),
    )


def test_fallback_only_overlays_immutable_v1_terminal_rows_and_requires_complete_scope() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, publication_id, run_id = _install(cur)
        try:
            _v1_manifest(cur, publication_id, run_id, A1, "success")
            _v1_fund(cur, publication_id, run_id, A1)
            _v1_manifest(cur, publication_id, run_id, A2, "terminal_error")

            blocked = cur.execute(
                "SELECT nport_fixed_income_secapi_fallback_scope_ready_v2(%s,%s,%s,%s,%s)",
                (publication_id, run_id, "v1-active", "v2-active", "query-render-v1"),
            ).fetchone()[0]
            assert blocked["ready"] is False
            assert blocked["missing_fallback_count"] == 1

            document_id = _fallback_manifest(cur, publication_id, run_id, A2)
            _fallback_fund(cur, publication_id, run_id, A2, document_id)
            ready = cur.execute(
                "SELECT nport_fixed_income_secapi_fallback_scope_ready_v2(%s,%s,%s,%s,%s)",
                (publication_id, run_id, "v1-active", "v2-active", "query-render-v1"),
            ).fetchone()[0]
            assert ready["ready"] is True
            assert ready["expected_count"] == 2
            assert ready["selected_v1_count"] == ready["selected_v2_count"] == 1

            rows = cur.execute(
                "SELECT accession_number FROM nport_fixed_income_fund_info_source_v2(%s,%s,'sec_api',%s,%s,%s) ORDER BY accession_number",
                (publication_id, run_id, "v1-active", "v2-active", "query-render-v1"),
            ).fetchall()
            assert rows == [(A1,), (A2,)]

            with pytest.raises(psycopg.errors.RaiseException, match="terminal"):
                _fallback_manifest(cur, publication_id, run_id, A1)
            with pytest.raises(psycopg.errors.RaiseException):
                cur.execute(
                    "UPDATE nport_fixed_income_secapi_recovery_v1 SET attempt_count=2 "
                    "WHERE source_holdings_publication_id=%s AND source_run_id=%s AND accession_number=%s",
                    (publication_id, run_id, A2),
                )
            with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
                cur.execute(
                    "UPDATE nport_fixed_income_secapi_fallback_manifest_v2 SET status='terminal_error' "
                    "WHERE source_holdings_publication_id=%s AND source_run_id=%s AND accession_number=%s",
                    (publication_id, run_id, A2),
                )
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_fallback_hash_linkage_and_active_parameters_fail_closed() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, publication_id, run_id = _install(cur)
        try:
            _v1_manifest(cur, publication_id, run_id, A1, "success")
            _v1_fund(cur, publication_id, run_id, A1)
            _v1_manifest(cur, publication_id, run_id, A2, "terminal_error")
            document_id = _fallback_manifest(cur, publication_id, run_id, A2)
            with pytest.raises(psycopg.errors.RaiseException, match="hash"):
                _fallback_fund(cur, publication_id, run_id, A2, document_id, compact_sha="c" * 64)
            _fallback_fund(cur, publication_id, run_id, A2, document_id)

            mismatch = cur.execute(
                "SELECT nport_fixed_income_secapi_fallback_scope_ready_v2(%s,%s,%s,%s,%s)",
                (publication_id, run_id, "v1-active", "wrong-parser", "query-render-v1"),
            ).fetchone()[0]
            assert mismatch["ready"] is False
            assert mismatch["active_parameter_mismatch_count"] == 1
            with pytest.raises(psycopg.errors.RaiseException, match="not ready"):
                cur.execute(
                    "SELECT * FROM nport_fixed_income_fund_info_source_v2(%s,%s,'sec_api',%s,%s,%s)",
                    (publication_id, run_id, "v1-active", "wrong-parser", "query-render-v1"),
                )
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
