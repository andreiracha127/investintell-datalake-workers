"""The identity matview must not be able to lie about how fresh it is.

``nport_holdings_snapshot_identity_v1`` is refreshed OUT OF BAND, and the app
anchors the whole fixed-income dossier on ``MAX(report_date)`` of its consumer
view.  A matview that is behind WITHIN the pinned publication therefore serves an
older report_date under a current publication id -- stale, but labelled.  These
tests pin the two properties that close that hole: the verdict is proven (never
assumed), and a proven divergence stops the caller.
"""
from __future__ import annotations

import importlib
import logging
from pathlib import Path
from uuid import uuid4

import pytest

from src.workers import nport_holdings_identity_freshness as freshness

ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"


def _seed(cur) -> tuple[str, str, str]:
    """A minimal holdings surface. The matview is deliberately NOT created here.

    Production owns the matview as ``postgres`` and this repository never creates
    it; a probe that installed its own subject would be asserting against itself.
    """
    schema = f"identity_fresh_{uuid4().hex}"
    run_id, package_id, holdings_id = (uuid4() for _ in range(3))
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS SELECT run_id,raw_validated_at "
        "FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    for ddl_name in ("sec_derived_publications.sql", "nport_holdings_v2.sql"):
        cur.execute((ROOT / "schemas" / ddl_name).read_text(encoding="utf-8"))
    cur.execute((ROOT / "schemas" / freshness.SCHEMA_FILE.name).read_text(encoding="utf-8"))
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s,now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
    cur.execute(
        """INSERT INTO sec_derived_publications
        (publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)
        VALUES(%s,'sec_nport_holdings_v2',1,%s,%s,%s)""",
        (holdings_id, run_id, package_id, "a" * 64),
    )
    return schema, str(holdings_id), str(run_id)


def _holding(cur, publication_id, run_id, holding_id, report_date, *, resolution="resolved") -> None:
    cur.execute(
        """INSERT INTO sec_nport_instrument_class_bridge
        (publication_id,accession_number,holding_id,instrument_id,series_id,class_id,valid_from,resolution_state)
        VALUES(%s,'A1',%s,%s,'SER1','C1','2020-01-01',%s)""",
        (publication_id, holding_id, f"I-{holding_id}", resolution),
    )
    cur.execute(
        """INSERT INTO sec_nport_holdings_v2
        (publication_id,accession_number,holding_id,source_run_id,report_date,filing_date,source_series_id,
         signed_market_value,signed_pct_of_nav,cusip,source_typed_projection)
        VALUES(%s,'A1',%s,%s,%s,%s,'SER1',100,10,'111111111','{}'::jsonb)""",
        (publication_id, holding_id, run_id, report_date, report_date),
    )


def _create_matview(cur, *, with_data: bool = True) -> None:
    """The operator-owned matview, verbatim from its production definition."""
    cur.execute(
        """CREATE MATERIALIZED VIEW nport_holdings_snapshot_identity_v1 AS
        SELECT DISTINCT h.publication_id, b.series_id, h.source_series_id, h.report_date,
               h.accession_number, h.filing_date
        FROM sec_nport_holdings_v2 h
        JOIN sec_nport_instrument_class_bridge b
          ON b.publication_id=h.publication_id AND b.accession_number=h.accession_number
         AND b.holding_id=h.holding_id
        WHERE b.resolution_state='resolved' AND h.report_date >= b.valid_from
          AND (b.valid_to IS NULL OR h.report_date <= b.valid_to)"""
        + ("" if with_data else " WITH NO DATA")
    )


def _promote(cur, holdings_id) -> None:
    cur.execute("SELECT sec_validate_derived_publication(%s)", (holdings_id,))
    cur.execute("SELECT sec_set_current_derived_publication('sec_nport_holdings_v2',%s)", (holdings_id,))


def _verdict(cur) -> dict:
    return cur.execute("SELECT nport_holdings_snapshot_identity_freshness()").fetchone()[0]


def _run_in_schema(schema: str) -> dict:
    """Run the dispatchable job with ``search_path`` pinned to the fixture schema."""
    real_connect = freshness.connect

    def _connect(_dsn, **kwargs):
        conn = real_connect(DSN, **kwargs)
        conn.execute(f'SET search_path TO "{schema}"')
        return conn

    freshness.connect = _connect  # type: ignore[assignment]
    try:
        return freshness.run(DSN)
    finally:
        freshness.connect = real_connect  # type: ignore[assignment]


def test_worker_is_dispatchable_by_name() -> None:
    """``python -m src.run nport_holdings_identity_freshness`` must resolve."""
    module = importlib.import_module("src.workers.nport_holdings_identity_freshness")
    assert callable(module.run)


def test_absent_matview_is_unreadable_and_stops_the_caller() -> None:
    """An absent anchor is not "nothing to compare" -- it is a broken read path.

    The runbook tells an operator to call the assertion right after a REFRESH. A
    silent pass there, on a matview that does not exist, tells the caller the
    opposite of the truth.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, holdings_id, run_id = _seed(cur)
        try:
            _holding(cur, holdings_id, run_id, "H1", "2026-01-31")
            _promote(cur, holdings_id)
            verdict = _verdict(cur)
            assert verdict["state"] == "unreadable"
            assert verdict["reason"] == "matview_absent"
            with pytest.raises(psycopg.errors.UndefinedTable):
                cur.execute("SELECT nport_holdings_snapshot_identity_assert_fresh()")
            with pytest.raises(freshness.IdentityMatviewUnreadable):
                _run_in_schema(schema)
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_never_refreshed_matview_is_unreadable_not_fresh() -> None:
    """``WITH NO DATA`` leaves a relation that raises on every SELECT."""
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, holdings_id, run_id = _seed(cur)
        try:
            _holding(cur, holdings_id, run_id, "H1", "2026-01-31")
            _promote(cur, holdings_id)
            _create_matview(cur, with_data=False)
            verdict = _verdict(cur)
            assert verdict["state"] == "unreadable"
            assert verdict["reason"] == "matview_never_refreshed"
            with pytest.raises(psycopg.errors.UndefinedTable):
                cur.execute("SELECT nport_holdings_snapshot_identity_assert_fresh()")
            with pytest.raises(freshness.IdentityMatviewUnreadable):
                _run_in_schema(schema)
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_missing_pointer_is_soft_and_never_fresh() -> None:
    """Undecidable UPSTREAM: there is nothing for the matview to be fresh against.

    Soft on purpose -- a datalake between publications is not a broken read path,
    and turning this red would make the job cry wolf. It still never says fresh.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, holdings_id, run_id = _seed(cur)
        try:
            _holding(cur, holdings_id, run_id, "H1", "2026-01-31")
            _create_matview(cur)  # no pointer was ever set
            verdict = _verdict(cur)
            assert verdict["state"] == "unavailable"
            assert verdict["reason"] == "no_current_holdings_publication"
            cur.execute("SELECT nport_holdings_snapshot_identity_assert_fresh()")
            assert _run_in_schema(schema)["state"] == "unavailable"
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_pinned_publication_without_holdings_is_soft_and_never_fresh() -> None:
    """A pinned publication carrying no holdings rows is likewise undecidable."""
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, holdings_id, _run_id = _seed(cur)
        try:
            _create_matview(cur)
            _promote(cur, holdings_id)  # validated with zero holdings rows
            verdict = _verdict(cur)
            assert verdict["state"] == "unavailable"
            assert verdict["reason"] == "pinned_publication_has_no_holdings"
            cur.execute("SELECT nport_holdings_snapshot_identity_assert_fresh()")
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_absent_holdings_surface_is_soft_and_never_fresh() -> None:
    """No holdings surface at all: the matview exists, the thing to compare does not."""
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema = f"identity_bare_{uuid4().hex}"
        cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
        try:
            cur.execute(
                (ROOT / "schemas" / freshness.SCHEMA_FILE.name).read_text(encoding="utf-8")
            )
            # A populated matview standing alone, with no holdings surface behind it.
            cur.execute(
                "CREATE MATERIALIZED VIEW nport_holdings_snapshot_identity_v1 AS "
                "SELECT NULL::uuid AS publication_id, ''::text AS series_id, "
                "''::text AS source_series_id, NULL::date AS report_date, "
                "''::text AS accession_number, NULL::date AS filing_date WHERE false"
            )
            verdict = _verdict(cur)
            assert verdict["state"] == "unavailable"
            assert verdict["reason"] == "holdings_surface_absent"
            cur.execute("SELECT nport_holdings_snapshot_identity_assert_fresh()")
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_matview_refreshed_after_the_publication_closed_is_fresh() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, holdings_id, run_id = _seed(cur)
        try:
            _holding(cur, holdings_id, run_id, "H1", "2026-01-31")
            _holding(cur, holdings_id, run_id, "H2", "2026-02-28")
            _promote(cur, holdings_id)
            _create_matview(cur)
            verdict = _verdict(cur)
            assert verdict["state"] == "fresh"
            assert verdict["matview_max_report_date"] == "2026-02-28"
            assert verdict["source_max_report_date"] == "2026-02-28"
            # The happy path must not have paid for the bridge-filtered maximum.
            assert verdict["confirmed_against_bridge"] is False
            cur.execute("SELECT nport_holdings_snapshot_identity_assert_fresh()")
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_matview_refreshed_during_the_prepared_window_is_stale_not_empty() -> None:
    """The undetected mode: behind WITHIN the pinned publication.

    The matview is captured while the publication is still ``prepared`` and
    missing its newest report_date; the publication is then validated and pinned.
    Nothing is empty and nothing degrades -- the app would simply anchor a month
    earlier and label it with the current publication id.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, holdings_id, run_id = _seed(cur)
        try:
            _holding(cur, holdings_id, run_id, "H1", "2026-01-31")
            _create_matview(cur)  # captured mid-build
            _holding(cur, holdings_id, run_id, "H2", "2026-02-28")
            _promote(cur, holdings_id)
            verdict = _verdict(cur)
            assert verdict["state"] == "stale"
            assert verdict["matview_max_report_date"] == "2026-01-31"
            assert verdict["source_max_report_date"] == "2026-02-28"
            assert verdict["bridge_resolved_max_report_date"] == "2026-02-28"
            # The exact re-check is paid for only on suspicion.
            assert verdict["confirmed_against_bridge"] is True
            with pytest.raises(psycopg.errors.DataException):
                cur.execute("SELECT nport_holdings_snapshot_identity_assert_fresh()")
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_bridge_filtered_matview_is_fresh_not_stale() -> None:
    """The cheap source-side maximum ignores the bridge filter; the probe must not.

    A newest holding whose bridge row is unresolved is legitimately absent from
    the matview.  Reporting that as stale would make the assertion cry wolf on
    every publication whose newest filing is still resolving.
    """
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, holdings_id, run_id = _seed(cur)
        try:
            _holding(cur, holdings_id, run_id, "H1", "2026-01-31")
            _holding(cur, holdings_id, run_id, "H2", "2026-02-28", resolution="orphan")
            _promote(cur, holdings_id)
            _create_matview(cur)
            verdict = _verdict(cur)
            assert verdict["state"] == "fresh"
            assert verdict["reason"] == "matview_carries_the_bridge_resolved_maxima"
            assert verdict["matview_max_report_date"] == "2026-01-31"
            assert verdict["source_max_report_date"] == "2026-02-28"
            assert verdict["confirmed_against_bridge"] is True
            cur.execute("SELECT nport_holdings_snapshot_identity_assert_fresh()")
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_pointer_past_the_matview_is_reported_as_behind_pointer() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, holdings_id, run_id = _seed(cur)
        try:
            _create_matview(cur)  # refreshed before this publication existed
            _holding(cur, holdings_id, run_id, "H1", "2026-01-31")
            _promote(cur, holdings_id)
            verdict = _verdict(cur)
            assert verdict["state"] == "behind_pointer"
            assert verdict["source_max_report_date"] == "2026-01-31"
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_run_goes_red_on_a_divergence_and_logs_it(caplog: pytest.LogCaptureFixture) -> None:
    """The job must exit non-zero, and the WARNING must name what to do."""
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, holdings_id, run_id = _seed(cur)
        try:
            _holding(cur, holdings_id, run_id, "H1", "2026-01-31")
            _create_matview(cur)
            _holding(cur, holdings_id, run_id, "H2", "2026-02-28")
            _promote(cur, holdings_id)
            with caplog.at_level(logging.WARNING):
                with pytest.raises(freshness.IdentityMatviewStale) as excinfo:
                    _run_in_schema(schema)
            assert excinfo.value.verdict["state"] == "stale"
            assert "is behind the pinned holdings publication" in caplog.text
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_run_returns_the_verdict_when_the_matview_is_fresh() -> None:
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, holdings_id, run_id = _seed(cur)
        try:
            _holding(cur, holdings_id, run_id, "H1", "2026-01-31")
            _promote(cur, holdings_id)
            _create_matview(cur)
            stats = _run_in_schema(schema)
            assert stats["state"] == "fresh"
            assert stats["rows"] == 0
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_install_tolerates_not_owning_the_functions_on_a_pooled_connection() -> None:
    """The privilege fallback must survive a connection that is NOT in autocommit.

    ``run()`` uses autocommit, so the failure is latent for any OTHER caller of
    ``probe()``: without a savepoint the privilege error leaves an aborted
    transaction and the existence probe raises InFailedSqlTransaction, masking the
    very error the tolerance exists for.
    """
    import psycopg

    role = f"identity_probe_{uuid4().hex[:8]}"
    with psycopg.connect(DSN, autocommit=True) as owner, owner.cursor() as cur:
        schema, holdings_id, run_id = _seed(cur)
        cur.execute(f'CREATE ROLE "{role}" LOGIN')
        try:
            _holding(cur, holdings_id, run_id, "H1", "2026-01-31")
            _promote(cur, holdings_id)
            _create_matview(cur)
            cur.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
            cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"')

            # A non-owner, on a NON-autocommit connection: the install must fail
            # with InsufficientPrivilege and be recovered from, not masked.
            guest = psycopg.connect(DSN.replace("user=postgres", f"user={role}"))
            with guest:
                guest.execute(f'SET search_path TO "{schema}"')
                assert freshness.install_schema(guest) is False
                verdict = freshness.probe(guest)
                assert verdict["state"] == "fresh"
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
            cur.execute(f'DROP OWNED BY "{role}"')
            cur.execute(f'DROP ROLE "{role}"')
