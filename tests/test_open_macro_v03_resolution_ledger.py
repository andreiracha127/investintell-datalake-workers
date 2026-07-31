"""The staleness-block RESOLUTION ledger against a REAL PostgreSQL.

The worker's unit suite drives publish()/record_staleness_block through a duck-typed
fake conn, which proves the CONTROL FLOW but can never catch a typo in the SQL itself
(a wrong column count, a parameter name that never binds, a missing ``::jsonb`` cast) —
those would surface for the first time in production, on the day an operator is trying
to clear a blocked day. These tests execute the real statements against a throwaway
schema and also assert that the worker's ``EXPECTED_SCHEMA`` — the dict ``verify_schema``
compares the LIVE prod catalog against, Gate 5 — actually mirrors the committed DDL
column signature for column and constraint definition for constraint definition.
"""

from __future__ import annotations

import datetime as _dt
import json
import uuid

import pytest

import src.workers.open_macro_v03 as w

DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"
AS_OF = _dt.date(2026, 7, 17)
BLOCK_RUN_ID = "open_macro_v03-2026-07-17-deadbeef"
PUBLISH_RUN_ID = "open_macro_v03-2026-07-17-cafe0001"
PROOF = {
    "verified_as_of": AS_OF.isoformat(),
    "criteria": {"monthly_max_age_days": 45, "mich_max_age_days": 90,
                 "price_max_age_business_days": 3},
    "series": {"INDPRO": {"last_available_at": "2026-07-15", "age_days": 2,
                          "bound_days": 45}},
    "prices": {"SPY": {"last_date": "2026-07-16", "age_business_days": 1}},
    "breaches": [],
}


@pytest.fixture()
def conn():
    psycopg = pytest.importorskip("psycopg")
    schema = f"open_macro_rt_{uuid.uuid4().hex[:12]}"
    connection = psycopg.connect(DSN)
    try:
        connection.execute(f'CREATE SCHEMA "{schema}"')
        connection.execute(f'SET search_path TO "{schema}"')
        connection.commit()
        with connection.cursor() as cur:
            for rel in w._SCHEMAS:
                cur.execute((w.ROOT / rel).read_text(encoding="utf-8"))
            cur.execute((w.ROOT / "schemas"
                         / "open_macro_v03_carry_decay_v1_migration.sql")
                        .read_text(encoding="utf-8"))
        connection.commit()
        yield connection
    finally:
        try:
            connection.rollback()
            connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            connection.commit()
        finally:
            connection.close()


def _schema_of(connection) -> str:
    with connection.cursor() as cur:
        cur.execute("SELECT current_schema()")
        return cur.fetchone()[0]


def _record_block(connection) -> None:
    w.record_staleness_block(connection, {
        "as_of": AS_OF, "reason": "staleness SLO breach: SPY",
        "stale_detail": json.dumps({"breaches": [{"ticker": "SPY"}]}),
        "input_vintage_sha256": "1" * 64, "input_prices_sha256": "2" * 64,
        "pack_v2_sha256": "3" * 64, "module_pins_sha256": "4" * 64,
        "code_commit": "a" * 40, "run_id": BLOCK_RUN_ID})


def _append_resolution(connection) -> str:
    resolution_id = str(uuid.uuid4())
    with connection.cursor() as cur:
        cur.execute(w._RESOLUTION_INSERT_SQL, {
            "resolution_id": resolution_id, "as_of": AS_OF,
            "resolution_state": "resolved", "resolved_by": "Andrei Rachadel",
            "reason": "ALFRED published the July prints",
            "freshness_proof": json.dumps(PROOF, sort_keys=True, default=str),
            "block_run_id": BLOCK_RUN_ID,
            "block_input_vintage_sha256": "1" * 64,
            "block_input_prices_sha256": "2" * 64,
            "input_vintage_sha256": "5" * 64, "input_prices_sha256": "6" * 64,
            "pack_v2_sha256": "3" * 64, "module_pins_sha256": "4" * 64,
            "code_commit": "b" * 40,
            "run_id": "open_macro_v03_resolve-2026-07-17-abcd1234"})
    connection.commit()
    return resolution_id


def _rows() -> tuple[dict, dict]:
    valid_until = _dt.datetime(2026, 7, 20, 14, tzinfo=_dt.timezone.utc)
    decision = {
        "as_of": AS_OF, "quadrant": "expansion", "decision_validity": "fresh",
        "carry_seed_as_of": AS_OF, "carry_age_months": 0, "carry_expired": False,
        "candidate_confidence": 0.8121545618518331, "coverage_quality": 1.0,
        "growth_score": 0.1, "inflation_score": -0.2,
        "input_vintage_sha256": "5" * 64, "input_prices_sha256": "6" * 64,
        "pack_v2_sha256": "3" * 64, "module_pins_sha256": "4" * 64,
        "judgment_ref": "j", "threshold_ref": "t", "code_commit": "b" * 40,
        "run_id": PUBLISH_RUN_ID, "valid_until": valid_until,
    }
    allocation = {
        "as_of": AS_OF, "book": "compressed_50",
        "w_spy": 0.3, "w_tlt": 0.2, "w_tip": 0.2, "w_gld": 0.1, "w_dbc": 0.1,
        "w_shy": 0.1, "risk_assets_weight": 0.5, "defensive_assets_weight": 0.5,
        "risk_cap": 0.65, "defensive_floor": 0.20, "priced_at": AS_OF,
        "carry_age_months": 0, "carry_seed_as_of": AS_OF, "carry_expired": False,
        "input_prices_sha256": "6" * 64, "pack_v2_sha256": "3" * 64,
        "module_pins_sha256": "4" * 64, "code_commit": "b" * 40,
        "run_id": PUBLISH_RUN_ID, "valid_until": valid_until,
    }
    return decision, allocation


def test_expected_schema_mirrors_the_live_catalog_for_the_resolution_ledger(conn):
    """Gate 5 compares the LIVE catalog against EXPECTED_SCHEMA and aborts on any
    divergence. If the dict and the DDL disagree, the daily run fails in production
    with zero writes — so the two are compared here against a real catalog."""
    schema = _schema_of(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type, character_maximum_length, is_nullable, "
            "column_default FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (schema, w.RESOLUTIONS_TABLE))
        columns = {r[0]: (r[1], r[2], r[3], r[4]) for r in cur.fetchall()}
        cur.execute(
            "SELECT conname, contype::text, pg_get_constraintdef(oid) "
            "FROM pg_constraint WHERE connamespace = %s::regnamespace "
            "AND contype IN ('c','p','f') AND conrelid::regclass::text LIKE %s",
            (schema, f"%{w.RESOLUTIONS_TABLE}"))
        # the throwaway schema qualifies the FK target; prod resolves it unqualified
        constraints = {r[0]: (r[1], r[2].replace(f"{schema}.", ""))
                       for r in cur.fetchall()}
    conn.commit()
    expected = w.EXPECTED_SCHEMA[w.RESOLUTIONS_TABLE]
    assert columns == expected["columns"]
    assert constraints == expected["constraints"]


def test_block_then_resolution_then_publication_walks_the_real_ledger(conn):
    """The full recovery path with the REAL statements: a blocked day refuses to
    publish and names the CLI; the appended 'resolved' event unlocks it; the
    publication appends 'superseded'; a re-run adds no second event; and the
    immutable block row is still there, untouched."""
    _record_block(conn)

    with pytest.raises(w.OpenMacroV03Error, match="resolve-staleness"):
        w.publish(conn, *_rows(), proof=PROOF)
    conn.rollback()

    _append_resolution(conn)
    w.publish(conn, *_rows(), proof=PROOF)
    w.post_write_verify(conn, AS_OF, {"SPY": 0.3, "TLT": 0.2, "TIP": 0.2,
                                      "GLD": 0.1, "DBC": 0.1, "SHY": 0.1})
    w.publish(conn, *_rows(), proof=PROOF)  # idempotent re-run

    with conn.cursor() as cur:
        cur.execute(f"SELECT resolution_state, resolved_by, block_run_id, run_id, "
                    f"freshness_proof FROM {w.RESOLUTIONS_TABLE} ORDER BY created_at")
        events = cur.fetchall()
        cur.execute("SELECT run_id FROM open_macro_v03_staleness_blocks")
        blocks = cur.fetchall()
    conn.commit()

    assert [e[0] for e in events] == ["resolved", "superseded"]
    assert events[0][1] == "Andrei Rachadel"
    assert events[1][1] == w.APPROVED_WRITER_IDENTITY
    assert {e[2] for e in events} == {BLOCK_RUN_ID}
    assert events[1][3] == PUBLISH_RUN_ID
    # the proof travels intact: per-source ages against the bounds in force
    assert events[1][4]["series"]["INDPRO"]["age_days"] == 2
    assert events[1][4]["breaches"] == []
    # the immutable ledger still holds exactly the original block
    assert blocks == [(BLOCK_RUN_ID,)]


def test_the_resolution_ledger_rejects_an_unknown_state_and_a_phantom_day(conn):
    """DDL-level honesty: only the two sanctioned event kinds exist, and a resolution
    cannot reference a day that was never blocked (FK)."""
    psycopg = pytest.importorskip("psycopg")
    _record_block(conn)
    base = {
        "resolution_id": str(uuid.uuid4()), "as_of": AS_OF,
        "resolution_state": "resolved", "resolved_by": "op", "reason": "r",
        "freshness_proof": json.dumps(PROOF), "block_run_id": BLOCK_RUN_ID,
        "block_input_vintage_sha256": "1" * 64, "block_input_prices_sha256": "2" * 64,
        "input_vintage_sha256": "5" * 64, "input_prices_sha256": "6" * 64,
        "pack_v2_sha256": "3" * 64, "module_pins_sha256": "4" * 64,
        "code_commit": "b" * 40, "run_id": "rid",
    }
    with conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute(w._RESOLUTION_INSERT_SQL, {**base, "resolution_state": "cleared"})
    conn.rollback()
    with conn.cursor() as cur:
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            cur.execute(w._RESOLUTION_INSERT_SQL,
                        {**base, "resolution_id": str(uuid.uuid4()),
                         "as_of": _dt.date(2026, 7, 16)})
    conn.rollback()
