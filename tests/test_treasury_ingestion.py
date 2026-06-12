"""Tests for the treasury_ingestion worker (Fiscal Data API → treasury_data).

Pure-helper tests (series_id naming, value parsing, record mapping, dedup) run
anywhere; the upsert/idempotency test uses a throwaway schema in the DB-mãe
(self-skips if unreachable). No network calls.
"""

from __future__ import annotations

import datetime as _dt

import psycopg
import pytest

from src.db import LOCK_TREASURY_INGESTION, advisory_lock
from src.workers import treasury_ingestion as ti

MAE_DSN = "host=localhost port=5434 dbname=investintell_alloc user=investintell password=investintell"


def _mae():
    try:
        return psycopg.connect(MAE_DSN, connect_timeout=5)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"DB-mãe unreachable: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# Value parsing
# ──────────────────────────────────────────────────────────────────────────────
def test_parse_float_handles_missing_markers_and_commas():
    assert ti.parse_float("4.33") == 4.33
    assert ti.parse_float("1,234,567.89") == 1234567.89
    assert ti.parse_float("null") is None
    assert ti.parse_float(".") is None
    assert ti.parse_float("") is None
    assert ti.parse_float(None) is None
    assert ti.parse_float("inf") is None
    assert ti.parse_float("garbage") is None


# ──────────────────────────────────────────────────────────────────────────────
# series_id naming (design §1C: RATE_/DEBT_/AUCTION_/FX_/INTEREST_)
# ──────────────────────────────────────────────────────────────────────────────
def test_rate_series_id_from_security_desc():
    rec = {"record_date": "2026-05-31", "security_desc": "Treasury Bills",
           "avg_interest_rate_amt": "4.20"}
    rows = ti.rows_from_rates([rec])
    assert rows == [{
        "obs_date": _dt.date(2026, 5, 31), "series_id": "RATE_TREASURY_BILLS",
        "value": 4.20, "source": "treasury_api", "metadata_json": None,
    }]


def test_debt_record_fans_out_three_series():
    rec = {"record_date": "2026-06-05", "tot_pub_debt_out_amt": "36000000000000.10",
           "intragov_hold_amt": "7000000000000.20", "debt_held_public_amt": "29000000000000.30"}
    rows = ti.rows_from_debt([rec])
    by_sid = {r["series_id"]: r["value"] for r in rows}
    assert by_sid == {
        "DEBT_TOTAL_PUBLIC": 36000000000000.10,
        "DEBT_INTRAGOV": 7000000000000.20,
        "DEBT_HELD_PUBLIC": 29000000000000.30,
    }


def test_auction_series_id_and_metadata():
    rec = {"auction_date": "2026-05-12", "security_type": "Note",
           "security_term": "10-Year", "high_yield": "4.45",
           "bid_to_cover_ratio": "2.50"}
    rows = ti.rows_from_auctions([rec])
    assert len(rows) == 1
    r = rows[0]
    assert r["series_id"] == "AUCTION_NOTE_10-YEAR"
    assert r["value"] == 4.45
    assert r["metadata_json"] == {
        "security_type": "Note", "security_term": "10-Year", "bid_to_cover": 2.50,
    }


def test_fx_series_id_normalizes_spaces_and_dashes():
    rec = {"record_date": "2026-03-31",
           "country_currency_desc": "Antigua & Barbuda-East Caribbean Dollar",
           "exchange_rate": "2.70"}
    rows = ti.rows_from_fx([rec])
    assert rows[0]["series_id"] == "FX_ANTIGUA_&_BARBUDA_EAST_CARIBBEAN_DOLLAR"


def test_interest_expense_two_series_per_category():
    rec = {"record_date": "2026-05-31", "expense_catg_desc": "Interest on Public Debt",
           "month_expense_amt": "90000000000", "fytd_expense_amt": "700000000000"}
    rows = ti.rows_from_interest([rec])
    sids = sorted(r["series_id"] for r in rows)
    assert sids == ["INTEREST_INTEREST_ON_PUBLIC_DEBT_FYTD",
                    "INTEREST_INTEREST_ON_PUBLIC_DEBT_MONTH"]


def test_series_id_truncated_to_80_chars():
    rec = {"record_date": "2026-03-31",
           "country_currency_desc": "X" * 120, "exchange_rate": "1.0"}
    rows = ti.rows_from_fx([rec])
    assert len(rows[0]["series_id"]) <= 80


def test_rows_with_unparseable_value_are_dropped():
    assert ti.rows_from_rates([{"record_date": "2026-05-31",
                                "security_desc": "Bills",
                                "avg_interest_rate_amt": "null"}]) == []


# ──────────────────────────────────────────────────────────────────────────────
# Dedup
# ──────────────────────────────────────────────────────────────────────────────
def test_dedup_rows_by_pk():
    rows = [
        {"obs_date": _dt.date(2026, 6, 5), "series_id": "DEBT_TOTAL_PUBLIC", "value": 1.0},
        {"obs_date": _dt.date(2026, 6, 5), "series_id": "DEBT_TOTAL_PUBLIC", "value": 2.0},
    ]
    out = ti.dedup_rows(rows)
    assert len(out) == 1 and out[0]["value"] == 2.0


# ──────────────────────────────────────────────────────────────────────────────
# Upsert / idempotency (throwaway schema in the DB-mãe)
# ──────────────────────────────────────────────────────────────────────────────
def test_upsert_treasury_data_idempotent():
    conn = _mae()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_treasury CASCADE")
            cur.execute("CREATE SCHEMA _dlw_test_treasury")
            cur.execute("SET search_path TO _dlw_test_treasury")
            cur.execute(
                """CREATE TABLE treasury_data (
                       obs_date date NOT NULL,
                       series_id varchar(80) NOT NULL,
                       value numeric(24,6),
                       source varchar(40) NOT NULL DEFAULT 'treasury_api',
                       metadata_json jsonb,
                       created_at timestamptz NOT NULL DEFAULT now(),
                       PRIMARY KEY (obs_date, series_id))"""
            )
        rows = [
            {"obs_date": _dt.date(2026, 6, 5), "series_id": "DEBT_TOTAL_PUBLIC",
             "value": 36.0, "source": "treasury_api", "metadata_json": None},
            {"obs_date": _dt.date(2026, 5, 12), "series_id": "AUCTION_NOTE_10-YEAR",
             "value": 4.45, "source": "treasury_api",
             "metadata_json": {"bid_to_cover": 2.5, "security_type": "Note",
                               "security_term": "10-Year"}},
        ]
        n1 = ti.upsert_treasury_data(conn, rows)
        conn.commit()
        rows[0]["value"] = 36.5  # revision
        n2 = ti.upsert_treasury_data(conn, rows)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM treasury_data")
            count = cur.fetchone()[0]
            cur.execute("SELECT value FROM treasury_data WHERE series_id='DEBT_TOTAL_PUBLIC'")
            v = float(cur.fetchone()[0])
            cur.execute("SELECT metadata_json->>'bid_to_cover' FROM treasury_data "
                        "WHERE series_id='AUCTION_NOTE_10-YEAR'")
            btc = cur.fetchone()[0]
        assert n1 == 2 and n2 == 2
        assert count == 2 and v == pytest.approx(36.5)
        assert float(btc) == pytest.approx(2.5)
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS _dlw_test_treasury CASCADE")
        conn.commit()
        conn.close()


def test_advisory_lock_is_distinct():
    assert LOCK_TREASURY_INGESTION == 900_324
    conn = _mae()
    try:
        with advisory_lock(conn, LOCK_TREASURY_INGESTION) as got:
            assert got is True
    finally:
        conn.close()
