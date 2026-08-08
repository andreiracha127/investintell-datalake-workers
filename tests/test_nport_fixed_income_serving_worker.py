"""The fixed-income producer must be reachable as a registered worker.

Before Wave 3 the only writer of ``nport_fixed_income_features_v1`` was a human
running a separately-attested local PostgreSQL, so the eight ``_v2`` relations
the app's fixed-income dossier reads were empty in every deployed environment.
These tests pin the properties that make it a real worker: dispatchable,
idempotent, fail-closed, and promoting through the shared publication protocol.
"""
from __future__ import annotations

import importlib
import logging
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


def test_builder_uses_one_scoped_supplemental_source_adapter() -> None:
    builder = (
        ROOT / "src" / "nport" / "sql" / "nport_fixed_income_features_builder.sql"
    ).read_text(encoding="utf-8")
    build_function = builder[builder.index("CREATE OR REPLACE FUNCTION build_nport_fixed_income_features"):]
    assert "supplemental_source_kind text" in builder
    assert "nport_fixed_income_fund_info_source_v1(" in builder
    assert "nport_fixed_income_rate_risk_source_v1(" in builder
    assert "FROM nport_fund_reported_info_raw" not in build_function
    assert "JOIN nport_interest_rate_risk_raw" not in build_function
    # Existing local/offline callers retain an explicit DERA wrapper; the
    # production worker calls the three-argument implementation.
    assert "supplemental_source_kind text DEFAULT 'dera_raw'" in builder
    assert "bond_price_observation" not in builder.lower()


def _identity(revision="abc1234", source="62ba191f-5dcf-4e69-b863-3e343db010c2",
              as_of=date(2026, 7, 24), contract="sha256:aa", builder="bb",
              source_kind="dera_raw", source_hash="sha256:raw") -> str:
    return worker._publication_id(
        revision, source, as_of, contract_digest=contract, builder_sha256=builder,
        supplemental_source_kind=source_kind,
        supplemental_source_hash=source_hash,
    )


def test_publication_identity_is_deterministic_and_revision_sensitive() -> None:
    first = _identity()
    assert first == _identity()
    # A code change must move the identity, otherwise ``materialize`` would
    # treat an existing id as already built and silently re-serve stale rows.
    assert first != _identity(revision="def5678")
    assert first != _identity(as_of=date(2026, 7, 25))


def test_publication_identity_does_not_depend_on_the_revision_env_var() -> None:
    """What decides the payload must decide the identity.

    With the documented ``unknown`` revision fallback, deriving identity from the
    revision alone meant a contract or builder change produced the SAME id, so
    the run short-circuited as already_published and the new shape was never
    published. Our jobs do set CODE_REVISION; correctness must not depend on it.
    """
    unknown = _identity(revision="unknown")
    assert unknown != _identity(revision="unknown", contract="sha256:cc")
    assert unknown != _identity(revision="unknown", builder="dd")


def test_publication_identity_is_bound_to_the_supplemental_evidence() -> None:
    baseline = _identity()
    assert baseline != _identity(source_kind="sec_api")
    assert baseline != _identity(source_hash="sha256:different")


def test_secapi_approval_hash_is_bound_to_publication_run_and_extractor() -> None:
    baseline = worker._secapi_source_hash("publication-a", "run-a", "ordered-evidence")
    assert baseline != worker._secapi_source_hash("publication-b", "run-a", "ordered-evidence")
    assert baseline != worker._secapi_source_hash("publication-a", "run-b", "ordered-evidence")


def test_supplemental_source_never_mixes_partial_legacy_evidence() -> None:
    ready = {
        "ready": True,
        "source_hash": "sha256:sidecar",
        "expected_count": 10,
    }
    assert worker._choose_supplemental_source(
        {
            "nport_fund_reported_info_raw": True,
            "nport_interest_rate_risk_raw": True,
        },
        ready,
    ) == {"kind": "dera_raw", "source_hash": "sha256:legacy-raw"}
    assert worker._choose_supplemental_source(
        {
            "nport_fund_reported_info_raw": False,
            "nport_interest_rate_risk_raw": False,
        },
        ready,
    ) == {"kind": "sec_api", "source_hash": "sha256:sidecar"}

    for partial in (
        {
            "nport_fund_reported_info_raw": True,
            "nport_interest_rate_risk_raw": False,
        },
        {
            "nport_fund_reported_info_raw": False,
            "nport_interest_rate_risk_raw": True,
        },
    ):
        with pytest.raises(RuntimeError, match="partial legacy raw evidence"):
            worker._choose_supplemental_source(partial, ready)


def test_supplemental_source_fails_closed_when_no_complete_source_exists() -> None:
    with pytest.raises(RuntimeError, match="no complete supplemental source"):
        worker._choose_supplemental_source(
            {
                "nport_fund_reported_info_raw": False,
                "nport_interest_rate_risk_raw": False,
            },
            {"ready": False, "source_hash": None, "expected_count": 10},
        )


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


def test_both_producer_routes_assert_completeness_before_validating() -> None:
    """The gate has to be symmetric, or the other route becomes the way around it.

    The product has two producers -- this worker and the offline artifact route --
    and they stopped being equivalent the day production pruned its raw rows. A
    guard on only one of them is a guard on neither.
    """
    worker_source = Path(worker.__file__).read_text(encoding="utf-8")
    materializer_source = Path(materializer.__file__).read_text(encoding="utf-8")
    publish = materializer_source[materializer_source.index("def publish_artifact"):]
    for source in (worker_source, publish):
        assert source.index("assert_publication_complete") < source.index(
            "sec_validate_derived_publication"
        )


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
        source_sha256 char(64) NOT NULL, source_table text NOT NULL, accession_number text,
        holding_id text, typed_projection jsonb NOT NULL,
        UNIQUE(source_file_id,source_row_number))"""
    )
    cur.execute(
        """CREATE TABLE nport_holdings_snapshot_identity_v1(
        publication_id uuid NOT NULL,
        accession_number text NOT NULL,
        holding_id text NOT NULL,
        report_date date NOT NULL,
        PRIMARY KEY(publication_id,accession_number,holding_id))"""
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


def _raw(cur, run_id, source_table, accession, projection, *, holding_id=None):
    cur.execute(
        """INSERT INTO nport_raw_rows
        (ingestion_run_id,source_file_id,source_row_number,source_sha256,source_table,
         accession_number,holding_id,typed_projection)
        VALUES(%s,%s,2,%s,%s,%s,%s,%s::jsonb)""",
        (run_id, uuid4(), "a" * 64, source_table, accession, holding_id, projection),
    )


def _seed_reported_evidence(cur, run_id, accession="A1") -> None:
    """The reported N-PORT facts four of the six contract relations exist through.

    Production PRUNES ``nport_raw_rows`` once a run is attested, so a fixture
    that omits these rows is not a smaller fixture -- it is the pruned-source
    incident, and a suite built on it can only ever assert the degenerate build.
    """
    _raw(
        cur, run_id, "INTEREST_RATE_RISK.tsv", accession,
        '{"CURRENCY_CODE":"USD","INTEREST_RATE_RISK_ID":"RISK-1",'
        '"INTRST_RATE_CHANGE_3MON_DV01":"-12","INTRST_RATE_CHANGE_3MON_DV100":"-120"}',
    )
    _raw(
        cur, run_id, "FUND_REPORTED_INFO.tsv", accession,
        '{"NET_ASSETS":"200","CREDIT_SPREAD_3MON_INVEST":"3",'
        '"CREDIT_SPREAD_3MON_NONINVEST":"-4","BORROWING_PAY_WITHIN_1YR":"10",'
        '"STANDBY_COMMITMENT":"5"}',
    )


def _holding(cur, publication_id, run_id, holding_id, series_id, report_date, market_value, projection):
    cur.execute(
        """INSERT INTO nport_holdings_snapshot_identity_v1
        (publication_id,accession_number,holding_id,report_date)
        VALUES(%s,'A1',%s,%s)""",
        (publication_id, holding_id, report_date),
    )
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


def _publish_a_complete_publication(cur, schema, run_id, holdings_id) -> dict:
    """Seed holdings + reported evidence, publish, and return the run result."""
    _holding(
        cur, holdings_id, run_id, "C1", "SER1", "2026-01-31", 100,
        '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"5.0","MATURITY_DATE":"2026-12-31","IS_DEFAULT":"N"}}',
    )
    _seed_reported_evidence(cur, run_id)
    cur.execute("SELECT sec_validate_derived_publication(%s)", (holdings_id,))
    cur.execute("SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)", (holdings_id,))
    return _run_in_schema(DSN, schema)


def test_run_publishes_then_is_idempotent() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _package_id, holdings_id = _seed(cur)
        try:
            first = _publish_a_complete_publication(cur, schema, run_id, holdings_id)
            assert first["state"] == "published"
            assert first["as_of"] == "2026-01-31"
            # The whole family, not just the two holdings-derived relations. The
            # suite used to seed an EMPTY nport_raw_rows and assert only
            # ``features == 1``: the other five came out at zero exactly as they
            # did in production on 2026-08-01, and the test passed.
            assert first["counts"]["nport_fixed_income_features"] == 1
            for relation in (
                "nport_fixed_income_key_rate_sensitivities_v2",
                "nport_fixed_income_credit_spread_sensitivities_v2",
                "nport_fixed_income_balance_sheet_primitives_v2",
                "nport_fixed_income_debt_flag_features_v2",
                "nport_fixed_income_metric_coverage_v2",
                "nport_fixed_income_metric_coverage_snapshot_v1",
            ):
                assert first["counts"][relation] > 0, relation

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


def test_run_refuses_to_build_when_the_pinned_raw_evidence_was_pruned(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The 2026-08-01 incident, as a test.

    ``nport_raw_rows`` had been pruned, so the two raw views the key-rate,
    credit-spread, balance-sheet and coverage relations join returned nothing for
    the pinned run. The build "succeeded" with four empty relations, wrote the
    zeros into its own manifest, and moved the current pointer. A producer that
    cannot see its evidence must publish NOTHING.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _package_id, holdings_id = _seed(cur)
        try:
            _holding(
                cur, holdings_id, run_id, "C1", "SER1", "2026-01-31", 100,
                '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed","ANNUALIZED_RATE":"5.0","MATURITY_DATE":"2026-12-31"}}',
            )
            # Deliberately no raw rows: this is the pruned production database.
            cur.execute("SELECT sec_validate_derived_publication(%s)", (holdings_id,))
            cur.execute("SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)", (holdings_id,))

            with caplog.at_level(logging.WARNING, logger=worker.__name__):
                result = _run_in_schema(DSN, schema)
            assert result["state"] == "no_source"
            assert result["reason"] == "pinned_raw_evidence_pruned"
            # Production prunes raw by policy, so this state PERSISTS and the job
            # stays green forever. A silent standstill is the failure mode; the
            # WARNING with the identifiers is what makes it legible.
            warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
            assert len(warnings) == 1
            message = warnings[0].getMessage()
            assert "pruned raw evidence" in message
            assert run_id in message
            assert "nport_interest_rate_risk_raw" in message
            assert "docs/runbooks/fixed-income-publication-closure.md" in message
            assert result["pruned_raw_relations"] == [
                "nport_fund_reported_info_raw",
                "nport_interest_rate_risk_raw",
            ]
            assert result["source_run_id"] == run_id

            # Nothing was created, validated or pinned.
            cur.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s", (worker.PRODUCT,)
            )
            assert cur.fetchone() == (0,)
            cur.execute(
                "SELECT count(*) FROM sec_derived_current_pointers WHERE product=%s", (worker.PRODUCT,)
            )
            assert cur.fetchone() == (0,)
            cur.execute("SELECT count(*) FROM nport_fixed_income_features")
            assert cur.fetchone() == (0,)
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_run_publishes_from_complete_secapi_sidecar_when_raw_was_pruned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _package_id, holdings_id = _seed(cur)
        try:
            _holding(
                cur, holdings_id, run_id, "C1", "SER1", "2026-01-31", 100,
                '{"ASSET_CAT":"DBT","DEBT_SECURITY":{"COUPON_TYPE":"fixed",'
                '"ANNUALIZED_RATE":"5.0","MATURITY_DATE":"2026-12-31"}}',
            )
            cur.execute("SELECT sec_validate_derived_publication(%s)", (holdings_id,))
            cur.execute(
                "SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)",
                (holdings_id,),
            )
            cur.execute(
                (ROOT / "schemas" / "nport_fixed_income_secapi_sidecars_v1.sql").read_text(
                    encoding="utf-8"
                )
            )
            document_id = str(uuid4())
            payload_hash = "b" * 64
            response_hash = "c" * 64
            cur.execute(
                """INSERT INTO nport_fixed_income_secapi_recovery_v1
                (source_holdings_publication_id,source_run_id,accession_number,
                 source_document_id,source_row_number,extractor_version,status,attempt_count,
                 payload_sha256,provider_response_sha256)
                    VALUES(%s,%s,'A1',%s,0,%s,'success',1,%s,%s)""",
                    (
                        holdings_id,
                        run_id,
                        document_id,
                        worker.secapi_parser.EXTRACTOR_VERSION,
                        payload_hash,
                        response_hash,
                    ),
            )
            cur.execute(
                """INSERT INTO nport_fixed_income_secapi_fund_info_v1
                (source_holdings_publication_id,source_run_id,accession_number,
                 source_document_id,source_row_number,extractor_version,payload_sha256,projection_sha256,
                 compact_payload,presence_map,cur_metric_state,cur_metric_count,
                 net_assets,credit_spread_3mon_invest,credit_spread_3mon_noninvest)
                VALUES(%s,%s,'A1',%s,0,%s,%s,%s,'{}','{}','present',1,200,3,-4)""",
                (
                    holdings_id,
                    run_id,
                    document_id,
                    worker.secapi_parser.EXTRACTOR_VERSION,
                    payload_hash,
                    payload_hash,
                ),
            )
            cur.execute(
                """INSERT INTO nport_fixed_income_secapi_rate_risk_v1
                (source_holdings_publication_id,source_run_id,accession_number,
                 source_document_id,source_row_number,provider_ordinal,provider_rate_risk_id,
                 extractor_version,currency_code,payload_sha256,projection_sha256,compact_payload,presence_map,
                 dv01_3mon,dv100_3mon)
                VALUES(%s,%s,'A1',%s,0,0,'risk-1',%s,'USD',%s,%s,'{}','{}',-12,-120)""",
                (
                    holdings_id,
                    run_id,
                    document_id,
                    worker.secapi_parser.EXTRACTOR_VERSION,
                    payload_hash,
                    payload_hash,
                ),
            )

            readiness = worker._secapi_scope_state(conn, holdings_id, run_id)
            refused = _run_in_schema(DSN, schema)
            assert refused["state"] == "no_source"
            assert refused["reason"] == "secapi_activation_not_approved"

            monkeypatch.setenv(
                "NPORT_FI_SECAPI_APPROVED_SOURCE_HASH", readiness["source_hash"]
            )
            result = _run_in_schema(DSN, schema)

            assert result["state"] == "published"
            assert result["supplemental_source_kind"] == "sec_api"
            assert result["counts"]["nport_fixed_income_key_rate_sensitivities_v2"] > 0
            cur.execute(
                "SELECT manifest->'identity'->>'supplemental_source_kind' "
                "FROM nport_fixed_income_publication_manifests WHERE publication_id=%s",
                (result["publication_id"],),
            )
            assert cur.fetchone() == ("sec_api",)
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_serving_locks_upstream_pointer_before_its_own_product() -> None:
    source = Path(worker.__file__).read_text(encoding="utf-8")
    transaction = source[source.index("with conn.transaction():") :]
    source_lock = transaction.index("(SOURCE_PRODUCT,)")
    product_lock = transaction.index("(PRODUCT,)")
    source_recheck = transaction.index("if _current_source(conn)")
    assert source_lock < product_lock < source_recheck


def test_an_already_published_identity_is_still_repaired_after_the_raw_is_pruned() -> None:
    """Pruning the evidence must not orphan a publication that was already built.

    The raw probe gates the BUILD, so it sits AFTER the idempotency
    short-circuit: neither the publication identity nor the as_of depends on the
    raw rows, and production prunes them by policy days after a successful
    publish. A probe placed earlier would turn every later re-run into
    ``no_source`` and permanently cut off the coverage-rollup repair the dossier
    depends on.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _package_id, holdings_id = _seed(cur)
        try:
            first = _publish_a_complete_publication(cur, schema, run_id, holdings_id)
            assert first["state"] == "published"

            cur.execute("DELETE FROM nport_raw_rows WHERE ingestion_run_id=%s", (run_id,))
            second = _run_in_schema(DSN, schema)
            assert second["state"] == "already_published"
            assert second["publication_id"] == first["publication_id"]
            assert second["counts"] == first["counts"]
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def _degrade_evidence_to_an_unjoinable_accession(cur, run_id) -> None:
    """Keep raw rows for the pinned run, but under an accession nothing joins.

    This is the residual class the raw precondition CANNOT catch: the evidence
    probe passes (the run still has rows) while every relation that joins the
    evidence to a filing comes out empty -- a broken predicate, an unresolved
    bridge, a partial ingestion.
    """
    cur.execute("DELETE FROM nport_raw_rows WHERE ingestion_run_id=%s", (run_id,))
    _seed_reported_evidence(cur, run_id, accession="ZZ")


def test_promotion_is_refused_when_a_served_relation_regresses_to_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _package_id, holdings_id = _seed(cur)
        try:
            first = _publish_a_complete_publication(cur, schema, run_id, holdings_id)
            assert first["state"] == "published"

            _degrade_evidence_to_an_unjoinable_accession(cur, run_id)
            # A code revision change is what moves the publication identity, so
            # this is a genuinely new build over the same pinned source.
            monkeypatch.setenv("CODE_REVISION", "degraded-build")
            with pytest.raises(psycopg.Error, match="regressed to zero rows"):
                _run_in_schema(DSN, schema)

            # The refused build left nothing behind and the pointer never moved.
            cur.execute(
                "SELECT publication_id::text FROM sec_derived_current_pointers WHERE product=%s",
                (worker.PRODUCT,),
            )
            assert cur.fetchone() == (first["publication_id"],)
            cur.execute(
                "SELECT count(*) FROM sec_derived_publications WHERE product=%s", (worker.PRODUCT,)
            )
            assert cur.fetchone() == (1,)
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_an_explicit_override_promotes_a_regressed_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The override exists so a verified, deliberate emptiness stays publishable.

    It is a human assertion, never a default: without the environment switch the
    same run is refused by the test above.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, _package_id, holdings_id = _seed(cur)
        try:
            first = _publish_a_complete_publication(cur, schema, run_id, holdings_id)
            assert first["state"] == "published"

            _degrade_evidence_to_an_unjoinable_accession(cur, run_id)
            monkeypatch.setenv("CODE_REVISION", "degraded-build")
            monkeypatch.setenv("NPORT_FI_ALLOW_RELATION_REGRESSION", "1")
            second = _run_in_schema(DSN, schema)
            assert second["state"] == "published"
            assert second["publication_id"] != first["publication_id"]
            assert second["counts"]["nport_fixed_income_key_rate_sensitivities_v2"] == 0

            cur.execute(
                "SELECT publication_id::text FROM sec_derived_current_pointers WHERE product=%s",
                (worker.PRODUCT,),
            )
            assert cur.fetchone() == (second["publication_id"],)
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_the_first_publication_of_a_product_has_no_shape_to_regress_from() -> None:
    """No current pointer means no baseline: the assertion must not block a debut."""
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, run_id, package_id, _holdings_id = _seed(cur)
        debut = str(uuid4())
        try:
            cur.execute(
                (ROOT / "schemas" / "nport_fixed_income_features.sql").read_text(encoding="utf-8")
            )
            cur.execute(
                """INSERT INTO sec_derived_publications
                (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
                VALUES(%s,'nport_fixed_income_features_v1',1,%s,%s,%s)""",
                (debut, run_id, package_id, "d" * 64),
            )
            cur.execute(
                "SELECT nport_fixed_income_assert_publication_complete(%s)", (debut,)
            )
            assert cur.fetchone() == ("",)  # void
            # An unknown publication is not "trivially complete": it is a caller
            # error, and the assertion says so instead of returning silently.
            with pytest.raises(psycopg.Error, match="requires a fixed-income publication"):
                cur.execute(
                    "SELECT nport_fixed_income_assert_publication_complete(%s)", (str(uuid4()),)
                )
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
