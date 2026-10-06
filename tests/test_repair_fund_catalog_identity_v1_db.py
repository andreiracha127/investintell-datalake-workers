"""Real PostgreSQL tests of scripts/repair_fund_catalog_identity_v1.py.

Each test creates its own disposable database (``fund_catalog_repair_disposable_*``)
on a loopback server named by ``FUND_CATALOG_REPAIR_TEST_DATABASE_URL`` (or the
CI ``SEC_TEST_DATABASE_URL``) and drops it afterwards; without either DSN the
module is skipped. The catalog mirrors production where the repair depends on
it: ``funds_v`` is a VIEW projecting the registry ticker, the registry status
is the production enum, IU ``isin``/``ticker`` carry the production UNIQUE
indexes and ``updated_at``/``identity_sources`` exist with their defaults.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scripts import repair_fund_catalog_identity_v1 as repair
from src.db import (
    LOCK_FUND_NAV_READINESS,
    LOCK_INSTRUMENT_INGESTION,
    LOCK_SEC_COMPANY_TICKERS_MF,
)
from tests._nav_identity_fixtures import synthetic_cusip, synthetic_isin

ROOT = Path(__file__).parents[1]
SEC_TABLE_SQL = (ROOT / "schemas" / "sec_company_tickers_mf.sql").read_text(
    encoding="utf-8"
)
ENV = "FUND_CATALOG_REPAIR_TEST_DSN"
CATALOG_DDL = """
CREATE TYPE instrument_identity_resolution_status AS ENUM
    ('unresolved', 'canonical', 'provisional', 'conflict');
CREATE TABLE public.instruments_universe (
    instrument_id uuid PRIMARY KEY,
    instrument_type varchar NOT NULL,
    name varchar NOT NULL,
    isin varchar,
    ticker varchar,
    currency varchar NOT NULL DEFAULT 'USD',
    is_active boolean DEFAULT true,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz DEFAULT now()
);
CREATE UNIQUE INDEX uq_iu_isin ON public.instruments_universe (isin);
CREATE UNIQUE INDEX uq_iu_ticker ON public.instruments_universe (ticker);
CREATE TABLE public.instrument_identity (
    instrument_id uuid PRIMARY KEY,
    sec_series_id varchar,
    sec_class_id varchar,
    ticker varchar,
    isin varchar,
    cusip_9 varchar,
    figi varchar,
    resolution_status instrument_identity_resolution_status NOT NULL DEFAULT 'unresolved',
    conflict_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    identity_sources jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE public.fixture_fund_types (
    instrument_id uuid PRIMARY KEY, fund_type text NOT NULL, currency text NOT NULL
);
CREATE VIEW public.funds_v AS
SELECT ii.instrument_id, ii.sec_series_id AS series_id,
       NULLIF(btrim(ii.ticker::text), '') AS ticker, ii.isin, ii.cusip_9 AS cusip,
       t.currency, t.fund_type
  FROM public.instrument_identity ii
  JOIN public.fixture_fund_types t USING (instrument_id)
 WHERE ii.sec_series_id IS NOT NULL;
"""
OLD = dt.datetime(2026, 4, 1, 12, 34, 56, 123456, tzinfo=dt.timezone.utc)
SOURCES = {
    "ticker": {
        "source": "sec_company_tickers_mf",
        "observed_at": "2026-05-16T05:55:55+00:00",
    },
    "sec_class_id": {
        "source": "sec_company_tickers_mf",
        "observed_at": "2026-05-16T05:55:55+00:00",
    },
    "figi": {"source": "openfigi", "observed_at": "2026-05-09T16:53:01+00:00"},
}


def _uid(n: int) -> uuid.UUID:
    return uuid.UUID(int=n)


@pytest.fixture(scope="module")
def admin_dsn() -> str:
    value = os.environ.get("FUND_CATALOG_REPAIR_TEST_DATABASE_URL") or os.environ.get(
        "SEC_TEST_DATABASE_URL"
    )
    if not value:
        pytest.skip("no disposable PostgreSQL DSN")
    assert psycopg.conninfo.conninfo_to_dict(value).get("host") in (
        "127.0.0.1",
        "localhost",
        "::1",
    )
    return value


def _seed(conn) -> None:
    conn.execute(CATALOG_DDL)
    conn.execute(SEC_TABLE_SQL)
    iu = conn.cursor()
    rows = [
        # n, iu ticker, iu isin, registry ticker, class, series, status, conflict, iu updated_at
        (
            1,
            "T1",
            "S000000001",
            "T1",
            "C000000001",
            "S000000001",
            "canonical",
            {},
            None,
        ),
        (2, "I2", "S000000002", "R2", "C000000002", "S000000002", "canonical", {}, OLD),
        (
            3,
            "T3",
            synthetic_isin(3),
            "T3",
            "C000000003",
            "S000000003",
            "canonical",
            {},
            OLD,
        ),
        (4, "I4", None, "R4", "C000000004", "S000000004", "canonical", {"x": 1}, OLD),
        (5, "T5", "S000000999", "T5", "C000000005", "S000000005", "canonical", {}, OLD),
    ]
    for (
        n,
        iu_ticker,
        iu_isin,
        ticker,
        class_id,
        series,
        status,
        conflict,
        updated,
    ) in rows:
        iu.execute(
            "INSERT INTO public.instruments_universe "
            "(instrument_id, instrument_type, name, isin, ticker, updated_at) "
            "VALUES (%s, 'fund', %s, %s, %s, %s)",
            (_uid(n), f"Fund {n}", iu_isin, iu_ticker, updated),
        )
        claims = (synthetic_isin(3), synthetic_cusip(3)) if n == 3 else (None, None)
        iu.execute(
            "INSERT INTO public.instrument_identity (instrument_id, sec_series_id, "
            "sec_class_id, ticker, isin, cusip_9, figi, resolution_status, "
            "conflict_state, identity_sources, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s, %s, %s)",
            (
                _uid(n),
                series,
                class_id,
                ticker,
                *claims,
                status,
                Jsonb(conflict),
                Jsonb(SOURCES),
                OLD,
            ),
        )
        iu.execute(
            "INSERT INTO public.fixture_fund_types VALUES (%s, 'mutual_fund', 'USD')",
            (_uid(n),),
        )
        iu.execute(
            "INSERT INTO public.sec_company_tickers_mf "
            "(class_id, cik, series_id, ticker, updated_at) "
            "VALUES (%s, '1', %s, %s, now() - interval '1 day')",
            (class_id, series, ticker),
        )
    for n in (2, 4):  # the IU class of each sibling-class fund
        iu.execute(
            "INSERT INTO public.sec_company_tickers_mf "
            "(class_id, cik, series_id, ticker, updated_at) "
            "VALUES (%s, '1', %s, %s, now() - interval '1 day')",
            (f"C{n + 500_000:09d}", f"S{n:09d}", f"I{n}"),
        )
    conn.execute(
        "CREATE MATERIALIZED VIEW public.funds_profile_mv AS "
        "SELECT instrument_id FROM public.funds_v"
    )


@pytest.fixture
def dsn(admin_dsn, monkeypatch) -> str:
    name = f"fund_catalog_repair_disposable_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    info = psycopg.conninfo.conninfo_to_dict(admin_dsn)
    info["dbname"] = name
    target = psycopg.conninfo.make_conninfo(**info)
    try:
        with psycopg.connect(target, autocommit=True) as conn:
            _seed(conn)
        monkeypatch.setenv(ENV, target)
        yield target
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)
                )
            )


def _run(capsys, *argv: str) -> tuple[int, dict]:
    code = repair.main(["--dsn-env", ENV, *argv])
    return code, json.loads(capsys.readouterr().out)


def _apply(capsys, plan_sha256: str) -> tuple[int, dict]:
    return _run(
        capsys,
        "--apply",
        "--confirm",
        repair.CONFIRM_TOKEN,
        "--plan-sha256",
        plan_sha256,
    )


def _rows(dsn: str) -> dict:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return {
            table: conn.execute(
                f"SELECT * FROM public.{table} ORDER BY instrument_id"
            ).fetchall()
            for table in ("instruments_universe", "instrument_identity")
        }


def _ledger(dsn: str) -> tuple[int, int] | None:
    with psycopg.connect(dsn) as conn:
        if (
            conn.execute(
                "SELECT to_regclass('public.fund_catalog_identity_repair_runs')"
            ).fetchone()[0]
            is None
        ):
            return None
        return (
            conn.execute(
                "SELECT count(*) FROM fund_catalog_identity_repair_runs"
            ).fetchone()[0],
            conn.execute(
                "SELECT count(*) FROM fund_catalog_identity_repair_receipts"
            ).fetchone()[0],
        )


def test_dry_run_apply_noop_and_exact_rollback(dsn, capsys, tmp_path):
    original = _rows(dsn)
    plan_file = tmp_path / "plan.json"
    code, planned = _run(capsys, "--plan-output", str(plan_file))
    assert code == 0 and planned["status"] == "planned"
    assert planned["changes"] == {
        "instrument_identity_ticker_and_class": 1,
        "instruments_universe_isin_to_null": 2,
        "ticker_passes": 1,
    }
    assert planned["excluded"] == {
        "isin": {"registry_series_differs": 1},
        "ticker": {"registry_conflict_state_not_empty": 1},
    }
    classification = planned["classification"]
    assert classification["before"]["active"] == 1
    assert classification["after"]["active"] == 3
    assert classification["after"]["cohort"] == {
        "relation": repair.COHORT_RELATION,
        "size": 5,
        "active": 3,
    }
    assert (classification["gained_active"], classification["lost_active"]) == (2, 0)
    assert planned["guard_violations"] == []
    assert planned["preflight"]["ledger_present"] is False
    document = json.loads(plan_file.read_bytes())
    assert document["plan_sha256"] == planned["plan_sha256"]
    assert {
        row["instrument_id"] for row in document["changes"]["instruments_universe"]
    } == {
        str(_uid(1)),
        str(_uid(2)),
    }
    assert _rows(dsn) == original and _ledger(dsn) is None

    code, refused = _apply(capsys, "0" * 64)
    assert (code, refused["code"]) == (repair.EXIT_FAILED, "plan_sha256_mismatch")
    assert _rows(dsn) == original and _ledger(dsn) is None

    code, applied = _apply(capsys, planned["plan_sha256"])
    assert code == 0 and applied["status"] == "applied"
    run_id = applied["run_id"]
    after = _rows(dsn)
    iu = {row["instrument_id"]: row for row in after["instruments_universe"]}
    registry = {row["instrument_id"]: row for row in after["instrument_identity"]}
    assert iu[_uid(1)]["isin"] is None and iu[_uid(2)]["isin"] is None
    assert iu[_uid(5)]["isin"] == "S000000999"  # registry series differs: untouched
    assert (registry[_uid(2)]["ticker"], registry[_uid(2)]["sec_class_id"]) == (
        "I2",
        "C000500002",
    )
    provenance = registry[_uid(2)]["identity_sources"]
    assert provenance["ticker"]["repair"] == repair.REPAIR_VERSION
    assert provenance["sec_class_id"]["source"] == "sec_company_tickers_mf"
    assert provenance["figi"] == SOURCES["figi"]
    assert registry[_uid(2)]["updated_at"] > OLD
    for table, untouched in (
        ("instruments_universe", {_uid(3), _uid(4), _uid(5)}),
        ("instrument_identity", {_uid(1), _uid(3), _uid(4), _uid(5)}),
    ):
        assert [r for r in after[table] if r["instrument_id"] in untouched] == [
            r for r in original[table] if r["instrument_id"] in untouched
        ]
    assert _ledger(dsn) == (1, 3)

    code, replanned = _run(capsys)
    assert code == 0 and replanned["changes"]["instruments_universe_isin_to_null"] == 0
    assert replanned["changes"]["instrument_identity_ticker_and_class"] == 0
    assert replanned["classification"]["before"]["active"] == 3
    code, noop = _apply(capsys, planned["plan_sha256"])
    assert code == 0 and noop["status"] == "noop"
    assert _ledger(dsn) == (1, 3)

    code, rolled = _run(capsys, "--rollback", run_id, "--confirm", repair.CONFIRM_TOKEN)
    assert code == 0 and rolled["status"] == "rolled_back"
    assert rolled["instruments_universe_isin_restored"] == 2
    assert rolled["instrument_identity_ticker_and_class_restored"] == 1
    assert _rows(dsn) == original  # byte-exact, updated_at and NULLs included
    assert _ledger(dsn) == (2, 6)
    code, again = _run(capsys, "--rollback", run_id, "--confirm", repair.CONFIRM_TOKEN)
    assert code == 0 and again["status"] == "noop"
    assert again["rollback_run_id"] == rolled["rollback_run_id"]


def test_ledger_is_append_only(dsn, capsys):
    _code, planned = _run(capsys)
    code, _applied = _apply(capsys, planned["plan_sha256"])
    assert code == 0
    for statement in (
        "UPDATE fund_catalog_identity_repair_receipts SET rule = 'x'",
        "DELETE FROM fund_catalog_identity_repair_receipts",
        "TRUNCATE fund_catalog_identity_repair_receipts CASCADE",
        "UPDATE fund_catalog_identity_repair_runs SET kind = 'apply'",
        "DELETE FROM fund_catalog_identity_repair_runs",
    ):
        with psycopg.connect(dsn) as conn:
            with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
                conn.execute(statement)


@pytest.mark.parametrize(
    ("table", "assignment", "n"),
    [
        ("instrument_identity", "ticker = 'MANUAL'", 2),
        # Another writer touched only the timestamp: still a conflict.
        ("instrument_identity", "updated_at = now() + interval '1 second'", 2),
        ("instruments_universe", "updated_at = now() + interval '1 second'", 1),
    ],
)
def test_rollback_refuses_when_a_repaired_row_changed_since(
    dsn, capsys, table, assignment, n
):
    _code, planned = _run(capsys)
    _code, applied = _apply(capsys, planned["plan_sha256"])
    with psycopg.connect(dsn) as conn:
        conn.execute(
            f"UPDATE public.{table} SET {assignment} WHERE instrument_id = %s",
            (_uid(n),),
        )
    changed = _rows(dsn)
    code, refused = _run(
        capsys, "--rollback", applied["run_id"], "--confirm", repair.CONFIRM_TOKEN
    )
    assert (code, refused["code"]) == (repair.EXIT_FAILED, "rollback_conflict")
    assert _rows(dsn) == changed  # all or nothing
    assert _ledger(dsn) == (1, 3)


@pytest.mark.parametrize(
    "lock",
    [LOCK_INSTRUMENT_INGESTION, LOCK_FUND_NAV_READINESS, LOCK_SEC_COMPANY_TICKERS_MF],
)
def test_apply_is_lock_busy_while_a_writer_holds_its_lock(dsn, capsys, lock):
    original = _rows(dsn)
    _code, planned = _run(capsys)
    with psycopg.connect(dsn, autocommit=True) as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (lock,))
        code, busy = _apply(capsys, planned["plan_sha256"])
    assert (code, busy["status"]) == (repair.EXIT_LOCK_BUSY, "lock_busy")
    assert _rows(dsn) == original and _ledger(dsn) is None


def test_apply_waits_out_an_in_flight_crosswalk_write_then_reports_busy(dsn, capsys):
    # An uncommitted SEC upsert holds the crosswalk: the apply cannot take its
    # SHARE lock, so it never validates against a snapshot that write will
    # invalidate at COMMIT.
    original = _rows(dsn)
    _code, planned = _run(capsys)
    with psycopg.connect(dsn) as writer:
        writer.execute("UPDATE public.sec_company_tickers_mf SET updated_at = now()")
        code, busy = _apply(capsys, planned["plan_sha256"])
        writer.rollback()
    assert (code, busy["status"]) == (repair.EXIT_LOCK_BUSY, "lock_busy")
    assert _rows(dsn) == original and _ledger(dsn) is None


def test_crosswalk_refresh_after_review_requires_a_new_approval(dsn, capsys):
    original = _rows(dsn)
    _code, planned = _run(capsys)
    with psycopg.connect(dsn) as conn:  # the SEC worker's upsert moves updated_at
        conn.execute(
            "UPDATE public.sec_company_tickers_mf "
            "SET updated_at = updated_at + interval '1 hour'"
        )
    code, refused = _apply(capsys, planned["plan_sha256"])
    assert (code, refused["code"]) == (repair.EXIT_FAILED, "plan_sha256_mismatch")
    assert _rows(dsn) == original and _ledger(dsn) is None
    _code, replanned = _run(capsys)
    assert replanned["changes"] == planned["changes"]
    assert replanned["plan_sha256"] != planned["plan_sha256"]
    code, applied = _apply(capsys, replanned["plan_sha256"])
    assert code == 0 and applied["status"] == "applied"


def test_provenance_change_after_review_requires_a_new_approval(dsn, capsys):
    original = _rows(dsn)
    _code, planned = _run(capsys)
    with psycopg.connect(dsn) as conn:  # ticker/class untouched, provenance moved
        conn.execute(
            "UPDATE public.instrument_identity SET identity_sources = identity_sources "
            '|| \'{"lei": {"source": "esma"}}\'::jsonb WHERE instrument_id = %s',
            (_uid(2),),
        )
    touched = _rows(dsn)
    code, refused = _apply(capsys, planned["plan_sha256"])
    assert (code, refused["code"]) == (repair.EXIT_FAILED, "plan_sha256_mismatch")
    assert _rows(dsn) == touched != original and _ledger(dsn) is None
    _code, replanned = _run(capsys)
    assert replanned["plan_sha256"] != planned["plan_sha256"]
    code, applied = _apply(capsys, replanned["plan_sha256"])
    assert code == 0 and applied["status"] == "applied"
    registry = {r["instrument_id"]: r for r in _rows(dsn)["instrument_identity"]}
    sources = registry[_uid(2)]["identity_sources"]
    assert sources["lei"] == {"source": "esma"}  # the approved provenance survives
    assert sources["ticker"]["repair"] == repair.REPAIR_VERSION
