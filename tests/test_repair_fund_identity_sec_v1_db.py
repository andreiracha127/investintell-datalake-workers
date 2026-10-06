"""Apply/rollback of repair_fund_identity_sec_v1 on a real PostgreSQL.

Each test creates its own disposable database (``fund_identity_sec_disposable_*``)
on the loopback server named by ``FUND_IDENTITY_SEC_REPAIR_TEST_DATABASE_URL``
(or the CI ``SEC_TEST_DATABASE_URL``) and drops it afterwards; without either
DSN the module is skipped.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from scripts import repair_fund_identity_sec_v1 as repair

ADMIN_DSN_ENV = ("FUND_IDENTITY_SEC_REPAIR_TEST_DATABASE_URL", "SEC_TEST_DATABASE_URL")

SCHEMA = """
CREATE TABLE instruments_universe (
    instrument_id uuid PRIMARY KEY, instrument_type varchar NOT NULL, name varchar NOT NULL,
    isin varchar, ticker varchar, currency varchar NOT NULL DEFAULT 'USD',
    is_active boolean DEFAULT true, attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz DEFAULT now());
CREATE UNIQUE INDEX uq_iu_isin ON instruments_universe (isin);
CREATE UNIQUE INDEX uq_iu_ticker ON instruments_universe (ticker);
CREATE TABLE instrument_identity (
    instrument_id uuid PRIMARY KEY, cik_padded varchar, cik_unpadded varchar,
    sec_series_id varchar, sec_class_id varchar, cusip_9 varchar, isin varchar, figi varchar,
    ticker varchar, resolution_status text NOT NULL DEFAULT 'canonical',
    conflict_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    identity_sources jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now());
CREATE VIEW funds_v AS
    SELECT ii.instrument_id, ii.sec_series_id AS series_id, NULLIF(btrim(ii.ticker), '') AS ticker,
           ii.isin, ii.cusip_9 AS cusip, 'USD'::text AS currency, 'etf'::text AS fund_type
    FROM instrument_identity ii WHERE ii.sec_series_id IS NOT NULL;
CREATE TABLE sec_company_tickers_mf (
    class_id text PRIMARY KEY, cik text NOT NULL, series_id text NOT NULL, ticker text NOT NULL,
    fetched_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE nav_timeseries (instrument_id uuid NOT NULL, nav_date date NOT NULL, nav numeric,
    PRIMARY KEY (instrument_id, nav_date));
CREATE TABLE nav_ingestion_runs (run_id uuid PRIMARY KEY, status text NOT NULL);
CREATE TABLE nav_ingestion_attempts (run_id uuid NOT NULL, instrument_id uuid NOT NULL,
    provider text NOT NULL, status text NOT NULL, newest_observed_date date,
    persisted_at timestamptz NOT NULL DEFAULT clock_timestamp());
"""

QUVU, VTI, DEAD, HYG = (str(uuid.UUID(int=i)) for i in (1, 2, 3, 4))
HISTORY = repair.build_history([
    (2025, "C000244148", "S000081376", "0001501825", "QUVU"),
    (2026, "C000007808", "S000002848", "0000036405", "VTI"),
    (2024, "C000000003", "S000000003", "0000000003", "DEADX"),
])


def _evidence():
    raw = _evidence_raw()
    return repair.parse_evidence(raw, hashlib.sha256(raw).hexdigest())


def _evidence_raw() -> bytes:
    doc = {"kind": repair.EVIDENCE_KIND, "class_ticker_filings": [], "class_last_filings": [],
           "series_last_filings": [],
           "tiingo_meta": {t: {"status": 200, "endDate": dt.date.today().isoformat(),
                               "observed_at": dt.datetime.now(dt.timezone.utc).isoformat()}
                           for t in ("VTI", "ACVU")},
           "ncen_series": {"S000002848": {"fund_types": ["Exchange-Traded Fund", "Index Fund"],
                                          "accession_no": "0000932471-26-000001",
                                          "filed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                                          "registrant_cik": "36405"}},
           "insurance_prospectus": []}
    return json.dumps(doc).encode()


@pytest.fixture(scope="module")
def admin_dsn() -> str:
    value = next((os.environ[name] for name in ADMIN_DSN_ENV if os.environ.get(name)), None)
    if not value:
        pytest.skip("no disposable PostgreSQL DSN")
    assert psycopg.conninfo.conninfo_to_dict(value).get("host") in ("127.0.0.1", "localhost", "::1")
    return value


@pytest.fixture()
def db(admin_dsn):
    name = f"fund_identity_sec_disposable_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    dsn = psycopg.conninfo.make_conninfo(admin_dsn, dbname=name)
    try:
        _seed(dsn)
        yield dsn
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


def _seed(dsn: str) -> None:
    conflict = {"ticker": {"values": [{"value": "QUVU"}, {"value": "ACVU"}], "resolved": False}}
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(SCHEMA)
        rows = [(QUVU, "QUVU", "S000081376", True), (VTI, "VTI", None, False), (DEAD, "DEADX", None, True),
                (HYG, "HYG", None, True)]
        for iid, ticker, isin, active in rows:
            conn.execute(
                "INSERT INTO instruments_universe (instrument_id, instrument_type, name, isin, ticker, is_active) "
                "VALUES (%s, 'fund', %s, %s, %s, %s)", (iid, ticker, isin, ticker, active))
        identity = [
            (QUVU, "S000081376", "C000244148", "QUVU", "0001501825", conflict),
            (VTI, "S000002848", "C000007808", "VTI", "0000036405", {}),
            (DEAD, "S000000003", "C000000003", "DEADX", "0000000003", {}),
        ]
        for iid, series, cls, ticker, cik, state in identity:
            conn.execute(
                "INSERT INTO instrument_identity (instrument_id, sec_series_id, sec_class_id, ticker, "
                "cik_padded, cik_unpadded, conflict_state) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (iid, series, cls, ticker, cik, str(int(cik)), Jsonb(state)))
        # A benchmark-proxy registry row: ticker only (R7 fills series/class/CIK).
        conn.execute("INSERT INTO instrument_identity (instrument_id, ticker) VALUES (%s, 'HYG')", (HYG,))
        conn.execute("INSERT INTO sec_company_tickers_mf (class_id, cik, series_id, ticker) VALUES "
                     "('C000244148', '1501825', 'S000081376', 'ACVU'), "
                     "('C000007808', '36405', 'S000002848', 'VTI'), "
                     "('C000046846', '1100663', 'S000016772', 'HYG')")
        conn.execute("INSERT INTO nav_timeseries VALUES (%s, '2024-12-27', 10)", (DEAD,))
        run_id = uuid.uuid4()
        conn.execute("INSERT INTO nav_ingestion_runs VALUES (%s, 'completed')", (run_id,))
        conn.execute("INSERT INTO nav_ingestion_attempts (run_id, instrument_id, provider, status, "
                     "newest_observed_date) VALUES (%s, %s, 'tiingo', 'success_no_new', '2024-12-27')",
                     (run_id, DEAD))


def _rows(dsn):
    with psycopg.connect(dsn) as conn:
        iu = conn.execute("SELECT instrument_id::text, to_jsonb(t) FROM instruments_universe t "
                          "ORDER BY 1").fetchall()
        reg = conn.execute("SELECT instrument_id::text, to_jsonb(t) FROM instrument_identity t "
                           "ORDER BY 1").fetchall()
    return dict(iu), dict(reg)


def test_apply_is_receipted_idempotent_and_rolls_back_exactly(db):
    ev = _evidence()
    pins = {"test": True}
    plan, _snapshot, report = repair.run_plan(db, HISTORY, ev, pins, include_class_repoint=False)
    assert report["changes_by_rule"] == {
        "R1_iu_isin_edgar_identifier": 1, "R2_ticker_renamed_same_class": 2,
        "R4_conflict_resolved_by_sec": 1, "R5_activate_live_orphan": 1, "R6_deactivate_terminated": 1,
        "R7_registry_series_from_sec": 1,
    }
    assert report["generator_after"]["active"] == report["generator_before"]["active"] + 2
    before_iu, before_reg = _rows(db)

    with pytest.raises(repair.RepairError, match="plan_sha256_mismatch"):
        repair.run_apply(db, HISTORY, ev, pins, include_class_repoint=False, expect_sha256="0" * 64)
    assert _rows(db) == (before_iu, before_reg)  # refused apply wrote nothing

    result = repair.run_apply(db, HISTORY, ev, pins, include_class_repoint=False,
                              expect_sha256=report["plan_sha256"])
    assert result["status"] == "committed"
    after_iu, after_reg = _rows(db)
    assert after_iu[QUVU]["ticker"] == "ACVU" and after_iu[QUVU]["isin"] is None
    assert after_reg[QUVU]["ticker"] == "ACVU" and after_reg[QUVU]["conflict_state"] == {}
    assert after_iu[VTI]["is_active"] is True and after_iu[DEAD]["is_active"] is False
    assert (after_reg[HYG]["sec_series_id"], after_reg[HYG]["sec_class_id"], after_reg[HYG]["cik_padded"],
            after_reg[HYG]["cik_unpadded"]) == ("S000016772", "C000046846", "0001100663", "1100663")
    assert after_reg[HYG]["identity_sources"]["sec_series_id"]["source"] == "sec_company_tickers_mf"

    _plan, _snap, again = repair.run_plan(db, HISTORY, ev, pins, include_class_repoint=False)
    assert again["rows_changed"] == {}
    noop = repair.run_apply(db, HISTORY, ev, pins, include_class_repoint=False,
                            expect_sha256=again["plan_sha256"])
    assert noop["status"] == "noop"

    with psycopg.connect(db, autocommit=True) as conn:
        receipts = conn.execute("SELECT count(*) FROM fund_identity_sec_repair_receipts").fetchone()[0]
        assert receipts == 5  # QUVU (IU + registry), VTI, DEADX, HYG (registry)
        with pytest.raises(psycopg.errors.RaiseException):
            conn.execute("UPDATE fund_identity_sec_repair_receipts SET rules = rules")

    rolled = repair.run_rollback(db, result["run_id"])
    assert rolled["rows_restored"] == 5
    assert _rows(db) == (before_iu, before_reg)  # byte-exact, updated_at included
    with pytest.raises(psycopg.errors.UniqueViolation):
        repair.run_rollback(db, result["run_id"])  # one rollback per apply


def test_rollback_refuses_rows_changed_after_apply(db):
    ev = _evidence()
    _plan, _snap, report = repair.run_plan(db, HISTORY, ev, {}, include_class_repoint=False)
    result = repair.run_apply(db, HISTORY, ev, {}, include_class_repoint=False,
                              expect_sha256=report["plan_sha256"])
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("UPDATE instruments_universe SET is_active = false WHERE instrument_id = %s", (VTI,))
    with pytest.raises(repair.RepairError, match="compare_and_swap"):
        repair.run_rollback(db, result["run_id"])


def test_apply_refuses_while_the_sec_sync_or_ingestion_holds_its_lock(db):
    ev = _evidence()
    _plan, _snap, report = repair.run_plan(db, HISTORY, ev, {}, include_class_repoint=False)
    for key in repair.APPLY_LOCKS:
        with psycopg.connect(db, autocommit=True) as holder:
            holder.execute("SELECT pg_advisory_lock(%s)", (key,))
            with pytest.raises(repair.RepairError, match="writer_lock_busy"):
                repair.run_apply(db, HISTORY, ev, {}, include_class_repoint=False,
                                 expect_sha256=report["plan_sha256"])


def test_repoint_rollback_is_refused_once_nav_moved_with_the_new_class(db):
    dead, live = str(uuid.UUID(int=5)), "PINZX"
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("INSERT INTO instruments_universe (instrument_id, instrument_type, name, ticker) "
                     "VALUES (%s, 'fund', 'Overseas', 'PINUX')", (dead,))
        conn.execute("INSERT INTO instrument_identity (instrument_id, sec_series_id, sec_class_id, ticker, "
                     "cik_padded, cik_unpadded) VALUES (%s, 'S000023512', 'C000069149', %s, "
                     "'0000898745', '898745')", (dead, live))
        conn.execute("INSERT INTO sec_company_tickers_mf (class_id, cik, series_id, ticker) "
                     "VALUES ('C000069149', '898745', 'S000023512', %s)", (live,))
    history = repair.build_history([
        *[(y, c, s, k, t) for c, rows in HISTORY.classes.items() for y, s, k, t in rows],
        (2025, "C000111522", "S000023512", "0000898745", "PINUX"),
        (2026, "C000069149", "S000023512", "0000898745", live),
    ])
    doc = json.loads(_evidence_raw())
    doc["tiingo_meta"][live] = doc["tiingo_meta"]["VTI"]
    raw = json.dumps(doc).encode()
    ev = repair.parse_evidence(raw, hashlib.sha256(raw).hexdigest())
    _plan, _snap, report = repair.run_plan(db, history, ev, {}, include_class_repoint=True)
    assert report["changes_by_rule"]["R3_iu_class_terminated_repoint"] == 1
    result = repair.run_apply(db, history, ev, {}, include_class_repoint=True,
                              expect_sha256=report["plan_sha256"])
    with psycopg.connect(db, autocommit=True) as conn:
        run = uuid.uuid4()
        conn.execute("INSERT INTO nav_ingestion_runs VALUES (%s, 'completed')", (run,))
        conn.execute("INSERT INTO nav_ingestion_attempts (run_id, instrument_id, provider, status, "
                     "newest_observed_date) VALUES (%s, %s, 'tiingo', 'success_new', current_date)",
                     (run, dead))
    with pytest.raises(repair.RepairError, match="rollback_repoint_after_nav_writes"):
        repair.run_rollback(db, result["run_id"])
