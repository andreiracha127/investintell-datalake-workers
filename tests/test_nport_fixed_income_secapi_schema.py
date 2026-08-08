"""Contract tests for the native SEC API fixed-income evidence sidecars."""
from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4, uuid5

import pytest


ROOT = Path(__file__).resolve().parents[1]
DDL = ROOT / "schemas" / "nport_fixed_income_secapi_sidecars_v1.sql"
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
SHA = "a" * 64


def _document_id(publication_id: str, accession: str) -> str:
    return str(uuid5(UUID(int=0), f"{publication_id}|{accession}"))


def test_sidecar_schema_is_native_and_covers_the_builder_surface() -> None:
    ddl = DDL.read_text(encoding="utf-8")

    assert "nport_fixed_income_secapi_recovery_v1" in ddl
    assert "nport_fixed_income_secapi_fund_info_v1" in ddl
    assert "nport_fixed_income_secapi_rate_risk_v1" in ddl
    assert "nport_fixed_income_secapi_scope_ready" in ddl
    assert "nport_fixed_income_fund_info_source_v1" in ddl
    assert "nport_fixed_income_rate_risk_source_v1" in ddl
    for key in (
        "TOTAL_ASSETS", "TOTAL_LIABILITIES", "NET_ASSETS",
        "BORROWING_PAY_WITHIN_1YR", "CTRLD_COMPANIES_PAY_AFTER_1YR",
        "DELAYED_DELIVERY", "STANDBY_COMMITMENT", "CASH_NOT_RPTD_IN_C_OR_D",
        "CREDIT_SPREAD_30YR_INVEST", "CREDIT_SPREAD_30YR_NONINVEST",
        "INTRST_RATE_CHANGE_30YR_DV01", "INTRST_RATE_CHANGE_30YR_DV100",
    ):
        assert key in ddl
    assert "bond_price_observation" not in ddl
    # The adapter may read legacy raw views for the explicit dera_raw branch,
    # but native objects must never impersonate them.
    assert "CREATE TABLE IF NOT EXISTS nport_fund_reported_info_raw" not in ddl
    assert "CREATE VIEW nport_fund_reported_info_raw" not in ddl
    assert "CREATE TABLE IF NOT EXISTS nport_interest_rate_risk_raw" not in ddl
    assert "CREATE VIEW nport_interest_rate_risk_raw" not in ddl
    assert "INSERT INTO nport_raw_rows" not in ddl


def _install(cur) -> tuple[str, str, str]:
    schema = f"secapi_sidecar_{uuid4().hex}"
    publication_id, run_id = (str(uuid4()), str(uuid4()))
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute(
        "CREATE MATERIALIZED VIEW nport_holdings_snapshot_identity_v1 AS "
        f"SELECT '{publication_id}'::uuid AS publication_id, 'A1'::text AS accession_number "
        f"UNION ALL SELECT '{publication_id}'::uuid, 'A2'::text"
    )
    cur.execute(DDL.read_text(encoding="utf-8"))
    cur.execute(DDL.read_text(encoding="utf-8"))
    return schema, publication_id, run_id


def _recovery(cur, publication_id: str, run_id: str, accession: str) -> None:
    cur.execute(
        """INSERT INTO nport_fixed_income_secapi_recovery_v1
        (source_holdings_publication_id, source_run_id, accession_number,
         source_document_id, source_row_number, extractor_version, status,
         attempt_count, payload_sha256, provider_response_sha256)
        VALUES (%s,%s,%s,%s,1,'test-v1','success',1,%s,%s)""",
        (publication_id, run_id, accession, _document_id(publication_id, accession), SHA, SHA),
    )


def _fund(cur, publication_id: str, run_id: str, accession: str, *, state: str, count: int,
          payload: str = "{}", presence: str = "{}") -> None:
    cur.execute(
        """INSERT INTO nport_fixed_income_secapi_fund_info_v1
        (source_holdings_publication_id, source_run_id, accession_number,
         source_document_id, source_row_number, extractor_version, payload_sha256, projection_sha256,
         compact_payload, presence_map, cur_metric_state, cur_metric_count)
        VALUES (%s,%s,%s,%s,2,'test-v1',%s,%s,%s::jsonb,%s::jsonb,%s,%s)""",
        (publication_id, run_id, accession, _document_id(publication_id, accession), SHA, SHA, payload, presence, state, count),
    )


def _rate(cur, publication_id: str, run_id: str, accession: str) -> None:
    cur.execute(
        """INSERT INTO nport_fixed_income_secapi_rate_risk_v1
        (source_holdings_publication_id, source_run_id, accession_number,
         source_document_id, source_row_number, provider_ordinal, provider_rate_risk_id, extractor_version,
         currency_code, payload_sha256, projection_sha256, compact_payload, presence_map,
         dv01_3mon, dv100_3mon)
        VALUES (%s,%s,%s,%s,3,0,'risk-1','test-v1','USD',%s,%s,'{}'::jsonb,'{}'::jsonb,1,100)""",
        (publication_id, run_id, accession, _document_id(publication_id, accession), SHA, SHA),
    )


def test_sidecar_scope_readiness_is_identity_bounded_and_rejects_compact_holdings() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, publication_id, run_id = _install(cur)
        try:
            # A planned request exists before any provider response does. Hash
            # columns therefore stay NULL until the atomic success transition;
            # placeholder hashes would be false provenance.
            cur.execute(
                """INSERT INTO nport_fixed_income_secapi_recovery_v1
                (source_holdings_publication_id,source_run_id,accession_number,
                 source_document_id,source_row_number,extractor_version,status)
                VALUES(%s,%s,'PENDING',%s,0,'test-v1','pending')""",
                (publication_id, run_id, str(uuid4())),
            )
            _recovery(cur, publication_id, run_id, "A1")
            _recovery(cur, publication_id, run_id, "A2")
            _fund(cur, publication_id, run_id, "A1", state="present", count=1)
            _rate(cur, publication_id, run_id, "A1")
            # It proves that pending work is a fail-closed scope contaminant,
            # then gets removed so this fixture can prove the green condition.
            cur.execute(
                "DELETE FROM nport_fixed_income_secapi_recovery_v1 "
                "WHERE source_holdings_publication_id=%s AND source_run_id=%s "
                "AND accession_number='PENDING'",
                (publication_id, run_id),
            )

            first = cur.execute(
                "SELECT nport_fixed_income_secapi_scope_ready(%s,%s,%s)",
                (publication_id, run_id, "test-v1"),
            ).fetchone()[0]
            assert first["ready"] is False
            assert first["missing_fund_count"] == 1

            _fund(cur, publication_id, run_id, "A2", state="empty", count=0)
            ready = cur.execute(
                "SELECT nport_fixed_income_secapi_scope_ready(%s,%s,%s)",
                (publication_id, run_id, "test-v1"),
            ).fetchone()[0]
            assert ready["ready"] is True
            assert ready["expected_count"] == 2
            assert ready["declared_cur_metric_count"] == ready["rate_row_count"] == 1

            _recovery(cur, publication_id, run_id, "A3")
            with pytest.raises(psycopg.errors.CheckViolation):
                _fund(cur, publication_id, run_id, "A3", state="empty", count=0, payload='{"invstOrSecs":[]}')
            _recovery(cur, publication_id, run_id, "A4")
            with pytest.raises(psycopg.errors.CheckViolation):
                _fund(cur, publication_id, run_id, "A4", state="empty", count=0, presence='{"invstOrSecs":"present"}')
            with pytest.raises(psycopg.errors.RaiseException):
                cur.execute(
                    "UPDATE nport_fixed_income_secapi_recovery_v1 SET attempt_count=2 "
                    "WHERE source_holdings_publication_id=%s AND source_run_id=%s AND accession_number='A1'",
                    (publication_id, run_id),
                )
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_postgres_writer_advances_pending_manifest_before_sidecar_insert() -> None:
    import psycopg

    from src.nport import secapi_fixed_income
    from src.workers.nport_fixed_income_secapi_recovery import PostgresRecoveryDb

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, publication_id, run_id = _install(cur)
        try:
            projection = secapi_fixed_income.extract_filing(
                {"accessionNo": "A1", "formType": "NPORT-P", "fundInfo": {}},
                publication_id=str(publication_id),
                source_run_id=str(run_id),
            )
            cur.execute(
                """INSERT INTO nport_fixed_income_secapi_recovery_v1
                (source_holdings_publication_id,source_run_id,accession_number,
                 source_document_id,source_row_number,extractor_version,status)
                VALUES(%s,%s,'A1',%s,0,%s,'pending')""",
                (
                    publication_id,
                    run_id,
                    projection.source_document_id,
                    projection.extractor_version,
                ),
            )
            db = PostgresRecoveryDb(conn)
            db.initialize_manifests(str(publication_id), str(run_id), ["A1", "A2"])
            assert cur.execute(
                "SELECT count(*) FROM nport_fixed_income_secapi_recovery_v1 "
                "WHERE source_holdings_publication_id=%s AND source_run_id=%s",
                (publication_id, run_id),
            ).fetchone() == (2,)
            with conn.transaction():
                db.write(projection)

            assert cur.execute(
                "SELECT status,attempt_count FROM nport_fixed_income_secapi_recovery_v1 "
                "WHERE source_holdings_publication_id=%s AND source_run_id=%s "
                "AND accession_number='A1'",
                (publication_id, run_id),
            ).fetchone() == ("success", 1)
            assert cur.execute(
                "SELECT count(*) FROM nport_fixed_income_secapi_fund_info_v1 "
                "WHERE source_holdings_publication_id=%s AND source_run_id=%s",
                (publication_id, run_id),
            ).fetchone() == (1,)
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_recovery_transitions_preserve_provenance_and_freeze_terminal_evidence() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, publication_id, run_id = _install(cur)
        try:
            source_document_id = str(uuid4())
            cur.execute(
                """INSERT INTO nport_fixed_income_secapi_recovery_v1
                (source_holdings_publication_id,source_run_id,accession_number,
                 source_document_id,source_row_number,extractor_version,status)
                VALUES(%s,%s,'STATE',%s,0,'test-v1','pending')""",
                (publication_id, run_id, source_document_id),
            )
            with pytest.raises(psycopg.errors.RaiseException):
                cur.execute(
                    "UPDATE nport_fixed_income_secapi_recovery_v1 SET source_document_id=%s "
                    "WHERE source_holdings_publication_id=%s AND source_run_id=%s AND accession_number='STATE'",
                    (str(uuid4()), publication_id, run_id),
                )

            cur.execute(
                """UPDATE nport_fixed_income_secapi_recovery_v1
                   SET status='success', attempt_count=1, payload_sha256=%s, provider_response_sha256=%s
                 WHERE source_holdings_publication_id=%s AND source_run_id=%s AND accession_number='STATE'""",
                (SHA, SHA, publication_id, run_id),
            )
            with pytest.raises(psycopg.errors.RaiseException, match="provenance"):
                cur.execute(
                    """INSERT INTO nport_fixed_income_secapi_fund_info_v1
                    (source_holdings_publication_id,source_run_id,accession_number,
                     source_document_id,source_row_number,extractor_version,payload_sha256,
                     projection_sha256,compact_payload,presence_map,cur_metric_state,cur_metric_count)
                    VALUES(%s,%s,'STATE',%s,0,'different-v1',%s,%s,'{}','{}','empty',0)""",
                    (publication_id, run_id, str(uuid4()), SHA, SHA),
                )
            with pytest.raises(psycopg.errors.RaiseException):
                cur.execute(
                    "UPDATE nport_fixed_income_secapi_recovery_v1 SET status='retryable_error', attempt_count=2 "
                    "WHERE source_holdings_publication_id=%s AND source_run_id=%s AND accession_number='STATE'",
                    (publication_id, run_id),
                )

            cur.execute(
                """INSERT INTO nport_fixed_income_secapi_recovery_v1
                (source_holdings_publication_id,source_run_id,accession_number,
                 source_document_id,source_row_number,extractor_version,status,attempt_count)
                VALUES(%s,%s,'TERMINAL',%s,0,'test-v1','terminal_error',1)""",
                (publication_id, run_id, str(uuid4())),
            )
            with pytest.raises(psycopg.errors.RaiseException):
                cur.execute(
                    "UPDATE nport_fixed_income_secapi_recovery_v1 SET status='pending', attempt_count=2 "
                    "WHERE source_holdings_publication_id=%s AND source_run_id=%s AND accession_number='TERMINAL'",
                    (publication_id, run_id),
                )
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
