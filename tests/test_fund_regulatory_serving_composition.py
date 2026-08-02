"""Composer suite for the app-owned ``fund_regulatory_serving_*`` layer.

Runs against the ephemeral PostgreSQL harness (``SEC_TEST_DATABASE_URL``, the
same 127.0.0.1:65431 instance the rest of the worker DB suites use).

The fixtures here are deliberately CLASS-GRAIN: every mapping the base
publication carries names a real ``C0000...`` class through at least one
cascade source, and the tail instrument that names none stays ambiguous.  The
app repo's ``test_sec_regulatory_vertical.py::_insert_mapping`` defaults to
``class_id=""`` + ``mapping_state="resolved"`` -- exactly the shape that let the
production incident through -- and that habit is not propagated here.
"""

from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID, uuid4, uuid5

import psycopg
import pytest

from src.sec_serving import app_composition
from src.sec_serving.app_composition import (
    CLASS_SOURCE_CATALOG,
    CLASS_SOURCE_IDENTITY,
    CLASS_SOURCE_TICKERS,
    GateFailed,
    GateThresholds,
    compose,
    current_pointer,
    promote_publication,
    resolve_class_cascade,
    read_base_publication,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_SQL = ROOT / "tests" / "fixtures" / "sec" / "app_regulatory_serving_v2.sql"

# One deterministic instrument per scenario so assertions can name them.
_NS = UUID("2c9f4d3e-0000-5000-8000-000000000000")
INSTRUMENT_IDENTITY_A = uuid5(_NS, "identity-a")
INSTRUMENT_IDENTITY_B = uuid5(_NS, "identity-b")
INSTRUMENT_TICKER = uuid5(_NS, "ticker")
INSTRUMENT_CATALOG = uuid5(_NS, "catalog")
INSTRUMENT_TAIL = uuid5(_NS, "tail")

WORKER_PRODUCTS = (
    "regulatory_mandate",
    "ncen_operating_profile_v1",
    "rr1_fee_profile_v1",
    "sec_regulatory_serving_v1",
)

BASE_APP_ID = uuid5(_NS, "base-app-publication")

TEST_THRESHOLDS = GateThresholds(
    mappings=5,
    resolved=4,
    class_grain=4,
    fee_matched_instruments=4,
    mandate_published=4,
)


def _dsn() -> str:
    return os.environ.get(
        "SEC_TEST_DATABASE_URL", "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
    )


def _install(schema: str) -> None:
    """Build the worker + app relations the composer touches, then base v1."""
    with psycopg.connect(_dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{schema}"')
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
        cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
        cur.execute(
            "CREATE VIEW sec_validated_raw_runs AS "
            "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
        )
        cur.execute((ROOT / "schemas" / "sec_derived_publications.sql").read_text(encoding="utf-8"))
        cur.execute((ROOT / "schemas" / "sec_regulatory_mandate.sql").read_text(encoding="utf-8"))
        cur.execute((ROOT / "schemas" / "sec_regulatory_serving.sql").read_text(encoding="utf-8"))

        run_id, package_id = uuid4(), uuid4()
        cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
        cur.execute("INSERT INTO sec_source_packages VALUES(%s,%s)", (package_id, run_id))
        worker_pubs: dict[str, UUID] = {}
        for product in WORKER_PRODUCTS:
            publication_id = uuid5(_NS, f"worker-{product}")
            worker_pubs[product] = publication_id
            cur.execute(
                "INSERT INTO sec_derived_publications"
                "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)"
                " VALUES(%s,%s,1,%s,%s,%s)",
                (publication_id, product, run_id, package_id, "a" * 64),
            )

        _install_class_sources(cur)
        # Product rows are written while their publication is still prepared:
        # both worker surfaces refuse writes once validated.
        _install_facts(cur, worker_pubs)
        for publication_id in worker_pubs.values():
            cur.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))
        cur.execute(FIXTURE_SQL.read_text(encoding="utf-8"))
        _install_base_publication(cur, worker_pubs)


def _install_class_sources(cur) -> None:
    """instrument_identity + the two ticker crosswalks + the POISONED relation."""
    cur.execute(
        "CREATE TABLE instrument_identity("
        " instrument_id uuid PRIMARY KEY, sec_series_id text, sec_class_id text, ticker text)"
    )
    cur.executemany(
        "INSERT INTO instrument_identity VALUES(%s,%s,%s,%s)",
        [
            # 1) resolved straight off the identity column
            (INSTRUMENT_IDENTITY_A, "S000000001", "C000000001", "AAAAX"),
            (INSTRUMENT_IDENTITY_B, "S000000001", "C000000002", "BBBBX"),
            # 2) identity blank -> resolved by the company-tickers crosswalk
            (INSTRUMENT_TICKER, "S000000002", "", "CCCCX"),
            # 3) identity blank and no company-tickers row -> series/class catalog
            (INSTRUMENT_CATALOG, "S000000003", "", "DDDDX"),
            # 4) no class anywhere: the legitimate class_context_ambiguous tail
            (INSTRUMENT_TAIL, "S000000004", "", "ZZZZX"),
        ],
    )

    cur.execute(
        "CREATE TABLE sec_company_tickers_mf("
        " class_id text, cik text, series_id text, ticker text, fetched_at timestamptz)"
    )
    cur.executemany(
        "INSERT INTO sec_company_tickers_mf VALUES(%s,'0000000001',%s,%s,now())",
        [
            # agrees with instrument_identity for the identity-resolved rows
            ("C000000001", "S000000001", "AAAAX"),
            ("C000000002", "S000000001", "BBBBX"),
            ("C000000003", "S000000002", "ccccx"),
            # the worker writes a synthetic "S...:TICKER" key when the source has
            # no class id; the 'C%' filter must drop it.
            ("S000000004:ZZZZX", "S000000004", "ZZZZX"),
        ],
    )

    cur.execute(
        "CREATE TABLE sec_series_class_catalog("
        " cik text, series_id text, series_name text, class_id text, class_name text,"
        " class_ticker text, source_file text, loaded_at timestamptz)"
    )
    cur.executemany(
        "INSERT INTO sec_series_class_catalog VALUES('0000000001',%s,'S',%s,'C',%s,'f',now())",
        [
            ("S000000003", "C000000004", "DDDDX"),
            ("S000000001", "C000000001", "AAAAX"),
        ],
    )

    # POISONED: 11 773 production rows carry the SERIES id in class_id.  A
    # composer that reads this writes a series id into class_id and every fee
    # join collapses to zero.  Present here precisely so the suite proves it is
    # never consulted.
    cur.execute("CREATE TABLE sec_fund_classes(series_id text, class_id text, class_ticker text)")
    cur.executemany(
        "INSERT INTO sec_fund_classes VALUES(%s,%s,%s)",
        [
            ("S000000001", "S000000001", "AAAAX"),
            ("S000000002", "S000000002", "CCCCX"),
            ("S000000003", "S000000003", "DDDDX"),
            ("S000000004", "S000000004", "ZZZZX"),
        ],
    )


def _install_facts(cur, worker_pubs: dict[str, UUID]) -> None:
    serving = worker_pubs["sec_regulatory_serving_v1"]
    cur.executemany(
        "INSERT INTO sec_regulatory_serving_facts"
        "(publication_id,family,series_id,class_id,fact_key,grain_origin,state,coverage_pct,payload)"
        " VALUES(%s,'rr1_fee',%s,%s,%s,'class','available',100,'{\"canonical_concept\":\"net_expense\"}')",
        [
            (serving, "S000000001", "C000000001", "net_expense"),
            (serving, "S000000001", "C000000002", "net_expense"),
            (serving, "S000000002", "C000000003", "net_expense"),
            (serving, "S000000003", "C000000004", "net_expense"),
        ],
    )

    mandate = worker_pubs["regulatory_mandate"]
    # Fund-level mandate rows (class_id=''), exactly one document per series:
    # the class_id is not what unblocks mandate -- mapping_state is.
    cur.executemany(
        "INSERT INTO sec_regulatory_mandate_profiles"
        "(publication_id,series_id,class_id,document_id,"
        " objective_text,objective_state,objective_source_date,objective_provenance,"
        " strategy_state,strategy_reason_code,"
        " concentration_policy_state,concentration_policy_reason_code,"
        " principal_risks_state,principal_risks_reason_code)"
        " VALUES(%s,%s,'',%s,'objective','available','2026-01-01','{}',"
        " 'unavailable','source_filing_unavailable',"
        " 'unavailable','source_filing_unavailable',"
        " 'unavailable','source_filing_unavailable')",
        [
            (mandate, "S000000001", "DOC-1"),
            (mandate, "S000000002", "DOC-2"),
            (mandate, "S000000003", "DOC-3"),
            (mandate, "S000000004", "DOC-4"),
        ],
    )


def _install_base_publication(cur, worker_pubs: dict[str, UUID]) -> None:
    """The broken v1: every mapping class-blank and ambiguous, as in production."""
    cur.execute(
        "INSERT INTO fund_regulatory_serving_publications"
        "(app_publication_id,app_publication_version,worker_publication_id,worker_publication_version)"
        " VALUES(%s,1,%s,1)",
        (BASE_APP_ID, worker_pubs["regulatory_mandate"]),
    )
    cur.executemany(
        "INSERT INTO fund_regulatory_serving_artifacts VALUES(%s,%s,%s,1)",
        [(BASE_APP_ID, product, worker_pubs[product]) for product in WORKER_PRODUCTS],
    )
    cur.executemany(
        "INSERT INTO fund_regulatory_serving_instrument_mappings"
        "(app_publication_id,instrument_id,series_id,class_id,ncen_fund_id,mapping_state)"
        " VALUES(%s,%s,%s,'',%s,'class_context_ambiguous')",
        [
            (BASE_APP_ID, INSTRUMENT_IDENTITY_A, "S000000001", "F1"),
            (BASE_APP_ID, INSTRUMENT_IDENTITY_B, "S000000001", "F1"),
            (BASE_APP_ID, INSTRUMENT_TICKER, "S000000002", "F2"),
            (BASE_APP_ID, INSTRUMENT_CATALOG, "S000000003", "F3"),
            (BASE_APP_ID, INSTRUMENT_TAIL, "S000000004", "F4"),
        ],
    )
    cur.execute("SELECT fund_validate_regulatory_serving_publication_v2(%s)", (BASE_APP_ID,))
    cur.execute("SELECT fund_set_current_regulatory_serving_publication_v2(%s)", (BASE_APP_ID,))


@pytest.fixture()
def schema() -> str:
    name = f"app_serving_{uuid4().hex}"
    _install(name)
    yield name
    with psycopg.connect(_dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture()
def conn(schema: str):
    # search_path via connection options, not SET: a rolled-back gate failure
    # would otherwise revert an in-transaction SET and leave the session blind.
    with psycopg.connect(_dsn(), options=f"-c search_path={schema}") as connection:
        yield connection


def test_cascade_resolves_each_source_and_keeps_the_irreducible_tail(conn) -> None:
    base = read_base_publication(conn)
    rows = {row.instrument_id: row for row in resolve_class_cascade(conn, base.app_publication_id)}

    assert rows[INSTRUMENT_IDENTITY_A].class_id == "C000000001"
    assert rows[INSTRUMENT_IDENTITY_A].class_source == CLASS_SOURCE_IDENTITY
    assert rows[INSTRUMENT_TICKER].class_id == "C000000003"
    assert rows[INSTRUMENT_TICKER].class_source == CLASS_SOURCE_TICKERS
    assert rows[INSTRUMENT_CATALOG].class_id == "C000000004"
    assert rows[INSTRUMENT_CATALOG].class_source == CLASS_SOURCE_CATALOG

    tail = rows[INSTRUMENT_TAIL]
    assert tail.class_id is None
    assert tail.mapping_state == "class_context_ambiguous"
    assert all(row.disagrees is False for row in rows.values())


def test_composition_writes_class_grain_mappings_and_passes_the_gate(conn) -> None:
    result = compose(conn, thresholds=TEST_THRESHOLDS)

    assert result.created is True
    assert result.validated is True
    assert result.promoted is False
    assert result.gate.ok, result.gate.failures
    assert result.gate.metrics["resolved"] == 4
    assert result.gate.metrics["class_grain"] == 4
    assert result.gate.metrics["fee_matched_instruments"] == 4
    assert result.gate.metrics["mandate_published"] == 4
    assert result.gate.metrics["class_equals_series"] == 0
    assert result.cascade.from_identity == 2
    assert result.cascade.from_tickers == 1
    assert result.cascade.from_catalog == 1
    assert result.cascade.unresolved_instruments == (str(INSTRUMENT_TAIL),)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_id, class_id, mapping_state"
            " FROM fund_regulatory_serving_instrument_mappings WHERE app_publication_id=%s",
            (result.app_publication_id,),
        )
        mapped = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    assert mapped[INSTRUMENT_IDENTITY_B] == ("C000000002", "resolved")
    assert mapped[INSTRUMENT_TAIL] == ("", "class_context_ambiguous")
    # composing does NOT move the pointer
    assert current_pointer(conn) == BASE_APP_ID


def test_poisoned_class_source_is_never_consulted(conn) -> None:
    assert "sec_fund_classes" not in app_composition.CASCADE_SQL
    assert "fund_classes_latest_mv" not in app_composition.CASCADE_SQL

    result = compose(conn, thresholds=TEST_THRESHOLDS)
    assert result.gate.ok, result.gate.failures

    with conn.cursor() as cur:
        # the poisoned relation is populated for every series in this fixture;
        # if it had been consulted, class_id would equal series_id here.
        cur.execute("SELECT count(*) FROM sec_fund_classes WHERE class_id = series_id")
        assert cur.fetchone()[0] == 4
        cur.execute(
            "SELECT count(*) FROM fund_regulatory_serving_instrument_mappings"
            " WHERE app_publication_id=%s AND class_id = series_id",
            (result.app_publication_id,),
        )
        assert cur.fetchone()[0] == 0


def test_gate_blocks_and_rolls_back_when_a_metric_regresses(conn) -> None:
    strict = GateThresholds(
        mappings=5, resolved=4, class_grain=4, fee_matched_instruments=5, mandate_published=4
    )
    # A rejected gate RAISES: a programmatic caller must not be able to read it
    # as success by ignoring a flag.
    with pytest.raises(GateFailed) as excinfo:
        compose(conn, thresholds=strict, promote=True)

    result = excinfo.value.result
    assert result.gate.ok is False
    assert any("rr1_fee" in failure for failure in result.gate.failures)
    assert result.validated is False
    assert result.promoted is False
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fund_regulatory_serving_publications")
        assert cur.fetchone()[0] == 1
    assert current_pointer(conn) == BASE_APP_ID


def test_promotion_moves_the_pointer_and_rollback_returns_it(conn) -> None:
    def published_counts() -> tuple[int, int]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM fund_regulatory_mandate_profiles_v WHERE selection_state='published'"
            )
            mandate = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(DISTINCT instrument_id) FROM fund_regulatory_serving_facts_v"
                " WHERE selection_state='published' AND family='rr1_fee'"
            )
            fees = int(cur.fetchone()[0])
        return mandate, fees

    assert published_counts() == (0, 0)

    result = compose(conn, thresholds=TEST_THRESHOLDS, promote=True)
    assert result.promoted is True
    assert current_pointer(conn) == result.app_publication_id
    assert published_counts() == (4, 4)

    promote_publication(conn, BASE_APP_ID)
    conn.commit()
    assert current_pointer(conn) == BASE_APP_ID
    assert published_counts() == (0, 0)


def test_recomposition_with_unchanged_inputs_is_an_explicit_no_op(conn) -> None:
    first = compose(conn, thresholds=TEST_THRESHOLDS, promote=True)
    second = compose(conn, base_app_id=BASE_APP_ID, thresholds=TEST_THRESHOLDS)

    assert second.app_publication_id == first.app_publication_id
    assert second.created is False
    assert second.fingerprint == first.fingerprint
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fund_regulatory_serving_publications")
        assert cur.fetchone()[0] == 2


def test_changed_class_sources_mint_a_different_publication(conn) -> None:
    first = compose(conn, thresholds=TEST_THRESHOLDS)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO sec_company_tickers_mf VALUES('C000000009','0000000001','S000000004','ZZZZX',now())"
        )
    conn.commit()

    relaxed = GateThresholds(
        mappings=5, resolved=5, class_grain=5, fee_matched_instruments=4, mandate_published=5
    )
    second = compose(conn, thresholds=relaxed)
    assert second.app_publication_id != first.app_publication_id
    assert second.created is True
    assert second.cascade.unresolved == 0
    assert second.gate.ok, second.gate.failures


def test_fixture_keeps_the_app_write_guards() -> None:
    """The fixture is a mirror; a weakened mirror would prove nothing."""
    sql = FIXTURE_SQL.read_text(encoding="utf-8")
    for token in (
        "regulatory serving publication is immutable",
        "regulatory serving mappings require a prepared app publication",
        "serving artifacts require a prepared app publication",
        "serving app pointer is managed by its switch function",
        "fund_validate_regulatory_serving_publication_v2",
        "fund_set_current_regulatory_serving_publication_v2",
        "pg_advisory_xact_lock",
    ):
        assert token in sql
