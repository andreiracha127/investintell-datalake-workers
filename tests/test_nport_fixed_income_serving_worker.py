"""The fixed-income producer must be reachable as a registered worker.

Before Wave 3 the only writer of ``nport_fixed_income_features_v1`` was a human
running a separately-attested local PostgreSQL, so the eight ``_v2`` relations
the app's fixed-income dossier reads were empty in every deployed environment.
These tests pin the properties that make it a real worker: dispatchable,
idempotent, fail-closed, and promoting through the shared publication protocol.
"""
from __future__ import annotations

import importlib
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest

from src.nport import fixed_income_local_materializer as materializer
from src.workers import nport_fixed_income_serving as worker

ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"


def test_worker_is_dispatchable_by_name() -> None:
    """``python -m src.run nport_fixed_income_serving`` must resolve."""
    module = importlib.import_module("src.workers.nport_fixed_income_serving")
    assert callable(module.run)


def test_publication_identity_is_deterministic_and_revision_sensitive() -> None:
    source = "62ba191f-5dcf-4e69-b863-3e343db010c2"
    as_of = date(2026, 7, 24)
    first = worker._publication_id("abc1234", source, as_of)
    assert first == worker._publication_id("abc1234", source, as_of)
    # A code change must move the identity, otherwise ``materialize`` would
    # treat an existing id as already built and silently re-serve stale rows.
    assert first != worker._publication_id("def5678", source, as_of)
    assert first != worker._publication_id("abc1234", source, date(2026, 7, 25))


def test_code_revision_ladder_prefers_explicit_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in worker._REVISION_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", "railway-sha")
    assert worker._code_revision() == "railway-sha"
    monkeypatch.setenv("SOURCE_COMMIT", "source-sha")
    assert worker._code_revision() == "source-sha"
    monkeypatch.setenv("GIT_SHA", "git-sha")
    assert worker._code_revision() == "git-sha"
    monkeypatch.setenv("CODE_REVISION", "code-revision")
    assert worker._code_revision() == "code-revision"


def test_contract_drift_fails_closed_before_touching_the_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(materializer, "CONTRACT_DIGEST", "sha256:" + "0" * 64)
    with pytest.raises(materializer.ArtifactIntegrityError):
        worker.run("host=127.0.0.1 port=1 dbname=nope user=nobody")


def test_builder_install_refuses_a_drifted_resource(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(materializer, "APPROVED_LOCAL_ORACLE_SHA256", "0" * 64)

    class _ExplodingCursor:
        def execute(self, *_args: object, **_kwargs: object) -> None:  # pragma: no cover
            raise AssertionError("drifted builder SQL must never reach EXECUTE")

    with pytest.raises(materializer.ArtifactIntegrityError):
        materializer.install_builder(_ExplodingCursor())


def _seed(cur) -> tuple[str, str, str, str]:
    schema = f"fi_worker_{uuid4().hex}"
    run_id, package_id, holdings_id = (uuid4() for _ in range(3))
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at "
        "FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    cur.execute(
        """CREATE TABLE nport_raw_rows(
        raw_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        ingestion_run_id uuid NOT NULL, source_file_id uuid NOT NULL, source_row_number bigint NOT NULL,
        source_table text NOT NULL, accession_number text, holding_id text, typed_projection jsonb NOT NULL,
        UNIQUE(source_file_id,source_row_number))"""
    )
    for relation, source_table in (
        ("nport_interest_rate_risk_raw", "INTEREST_RATE_RISK.tsv"),
        ("nport_fund_reported_info_raw", "FUND_REPORTED_INFO.tsv"),
        ("nport_borrow_aggregate_raw", "BORROW_AGGREGATE.tsv"),
        ("nport_repurchase_agreement_raw", "REPURCHASE_AGREEMENT.tsv"),
        ("nport_repurchase_collateral_raw", "REPURCHASE_COLLATERAL.tsv"),
        ("nport_repurchase_counterparty_raw", "REPURCHASE_COUNTERPARTY.tsv"),
        ("nport_securities_lending_raw", "SECURITIES_LENDING.tsv"),
    ):
        cur.execute(
            f"""CREATE VIEW {relation} AS
            SELECT r.* FROM nport_raw_rows r JOIN sec_validated_raw_runs v ON v.run_id=r.ingestion_run_id
            WHERE r.source_table='{source_table}'"""
        )
    for ddl_name in ("sec_derived_publications.sql", "nport_holdings_v2.sql"):
        ddl = (ROOT / "schemas" / ddl_name).read_text(encoding="utf-8")
        cur.execute(ddl)
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    cur.execute(
        """INSERT INTO sec_derived_publications
        (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
        VALUES(%s,'sec_nport_holdings_v2',1,%s,%s,%s)""",
        (holdings_id, run_id, package_id, "a" * 64),
    )
    return schema, str(run_id), str(package_id), str(holdings_id)


def _holding(cur, publication_id, run_id, holding_id, series_id, report_date, market_value, projection):
    cur.execute(
        """INSERT INTO sec_nport_instrument_class_bridge
        (publication_id,accession_number,holding_id,instrument_id,series_id,class_id,valid_from,resolution_state)
        VALUES(%s,'A1',%s,%s,%s,'C1','2020-01-01','resolved')""",
        (publication_id, holding_id, f"I-{holding_id}", series_id),
    )
    cur.execute(
        """INSERT INTO sec_nport_holdings_v2
        (publication_id,accession_number,holding_id,source_run_id,report_date,filing_date,source_series_id,
         signed_market_value,signed_pct_of_nav,cusip,source_typed_projection)
        VALUES(%s,'A1',%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
        (publication_id, holding_id, run_id, report_date, report_date, series_id, market_value, 10, "111111111", projection),
    )


def _run_in_schema(dsn: str, schema: str, **kwargs: object) -> dict:
    """Run the worker with ``search_path`` pinned to the fixture schema."""
    import psycopg

    real_connect = worker.connect

    def _connect(_dsn, **connect_kwargs):
        conn = real_connect(dsn, **connect_kwargs)
        conn.execute(f'SET search_path TO "{schema}"')
        return conn

    worker.connect = _connect  # type: ignore[assignment]
    try:
        return worker.run(dsn, **kwargs)  # type: ignore[arg-type]
    finally:
        worker.connect = real_connect  # type: ignore[assignment]
        del psycopg


def test_run_is_a_no_op_without_a_current_holdings_publication() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, *_ = _seed(cur)
        try:
            result = _run_in_schema(DSN, schema)
            assert result["state"] == "no_source"
            assert result["reason"] == "no_current_holdings_publication"
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_run_publishes_then_is_idempotent() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _package_id, holdings_id = _seed(cur)
        try:
            _holding(
                cur, holdings_id, run_id, "C1", "SER1", "2026-01-31", 100,
                '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"5.0","MATURITY_DATE":"2026-12-31"}}',
            )
            cur.execute("SELECT sec_validate_derived_publication(%s)", (holdings_id,))
            cur.execute("SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)", (holdings_id,))

            first = _run_in_schema(DSN, schema)
            assert first["state"] == "published"
            assert first["as_of"] == "2026-01-31"
            assert first["counts"]["nport_fixed_income_features"] == 1

            cur.execute(
                "SELECT lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
                (first["publication_id"],),
            )
            assert cur.fetchone() == ("validated",)
            cur.execute(
                "SELECT publication_id::text FROM sec_derived_current_pointers WHERE product=%s",
                (worker.PRODUCT,),
            )
            assert cur.fetchone() == (first["publication_id"],)
            cur.execute(
                "SELECT manifest->>'format' FROM nport_fixed_income_publication_manifests WHERE publication_id=%s",
                (first["publication_id"],),
            )
            assert cur.fetchone() == (worker.MANIFEST_FORMAT,)
            cur.execute("SELECT count(*) FROM sec_current_nport_fixed_income_features")
            assert cur.fetchone() == (1,)

            second = _run_in_schema(DSN, schema)
            assert second["state"] == "already_published"
            assert second["publication_id"] == first["publication_id"]
            cur.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s", (worker.PRODUCT,)
            )
            assert cur.fetchone() == (1,)
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
