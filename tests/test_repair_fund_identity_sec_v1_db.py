"""Apply/rollback of repair_fund_identity_sec_v1 on a disposable local Postgres.

Runs only when FUND_IDENTITY_SEC_REPAIR_TEST_DSN points at a local database
whose name starts with ``fund_identity_sec_test`` (it is dropped and rebuilt).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid

import psycopg
import pytest
from psycopg.types.json import Jsonb

from scripts import repair_fund_identity_sec_v1 as repair

DSN = os.environ.get("FUND_IDENTITY_SEC_REPAIR_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="FUND_IDENTITY_SEC_REPAIR_TEST_DSN not set")

SCHEMA = """
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
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

QUVU, VTI, DEAD = (str(uuid.UUID(int=i)) for i in (1, 2, 3))
HISTORY = repair.build_history([
    (2025, "C000244148", "S000081376", "0001501825", "QUVU"),
    (2026, "C000007808", "S000002848", "0000036405", "VTI"),
    (2024, "C000000003", "S000000003", "0000000003", "DEADX"),
])


def _evidence():
    doc = {"kind": repair.EVIDENCE_KIND, "class_ticker_filings": [], "class_last_filings": [],
           "series_last_filings": [],
           "tiingo_meta": {t: {"status": 200, "endDate": dt.date.today().isoformat(),
                               "observed_at": dt.datetime.now(dt.timezone.utc).isoformat()}
                           for t in ("VTI", "ACVU")}}
    raw = json.dumps(doc).encode()
    return repair.parse_evidence(raw, hashlib.sha256(raw).hexdigest())


@pytest.fixture()
def db():
    info = psycopg.conninfo.conninfo_to_dict(DSN)
    assert info.get("host") in ("127.0.0.1", "localhost", "::1")
    assert info.get("dbname", "").startswith("fund_identity_sec_test")
    conflict = {"ticker": {"values": [{"value": "QUVU"}, {"value": "ACVU"}], "resolved": False}}
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(SCHEMA)
        rows = [(QUVU, "QUVU", "S000081376", True), (VTI, "VTI", None, False), (DEAD, "DEADX", None, True)]
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
        conn.execute("INSERT INTO sec_company_tickers_mf (class_id, cik, series_id, ticker) VALUES "
                     "('C000244148', '1501825', 'S000081376', 'ACVU'), "
                     "('C000007808', '36405', 'S000002848', 'VTI')")
        conn.execute("INSERT INTO nav_timeseries VALUES (%s, '2024-12-27', 10)", (DEAD,))
        run_id = uuid.uuid4()
        conn.execute("INSERT INTO nav_ingestion_runs VALUES (%s, 'completed')", (run_id,))
        conn.execute("INSERT INTO nav_ingestion_attempts (run_id, instrument_id, provider, status, "
                     "newest_observed_date) VALUES (%s, %s, 'tiingo', 'success_no_new', '2024-12-27')",
                     (run_id, DEAD))
    yield DSN


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

    _plan, _snap, again = repair.run_plan(db, HISTORY, ev, pins, include_class_repoint=False)
    assert again["rows_changed"] == {}
    noop = repair.run_apply(db, HISTORY, ev, pins, include_class_repoint=False,
                            expect_sha256=again["plan_sha256"])
    assert noop["status"] == "noop"

    with psycopg.connect(db, autocommit=True) as conn:
        receipts = conn.execute("SELECT count(*) FROM fund_identity_sec_repair_receipts").fetchone()[0]
        assert receipts == 4  # QUVU (IU + registry), VTI, DEADX
        with pytest.raises(psycopg.errors.RaiseException):
            conn.execute("UPDATE fund_identity_sec_repair_receipts SET rules = rules")

    rolled = repair.run_rollback(db, result["run_id"])
    assert rolled["rows_restored"] == 4
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
