"""The certified pre-cut prefix is verified through a checkpoint, not re-hashed daily.

``read_prefix`` pulls the ENTIRE pre-cut window -- every seed-series vintage and
every sleeve price from 1998 through the pack cut -- ships it to the worker,
re-serializes it in the certified canonical format and re-hashes it, to compare
the digest with a pin that is a constant of the committed pack. On a day where
nothing pre-cut moved, that whole pass reproduces a known answer.

The checkpoint records "this pin was proven at THIS signature", where the
signature is a cheap server-side aggregate over the same window. These tests pin
the property that makes the shortcut legitimate: **any** pre-cut mutation --
insert, delete, or an in-place correction that keeps the row count -- moves the
signature, so the full read and the byte-exact comparison run and the retroactive
-mutation alarm still fires.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from uuid import uuid4

import pytest

from src.workers import open_macro_v03 as w

ROOT = Path(__file__).resolve().parents[1]
DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"

PRE_CUT = "2026-06-01"
POST_CUT = "2026-07-15"


@pytest.fixture()
def prefix_db():
    """A scratch schema carrying the two prefix relations and the checkpoint table."""
    import psycopg

    schema = f"open_macro_prefix_fixture_{uuid4().hex}"
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
            cur.execute(
                "CREATE TABLE macro_observation_vintage("
                " series_id text, observation_period date, vintage_date date,"
                " value numeric, available_at timestamptz, revision_number integer,"
                " source text, source_spec_version text)"
            )
            cur.execute(
                "CREATE TABLE eod_prices(ticker text, date date, close numeric,"
                " adj_close numeric, volume bigint)"
            )
            cur.execute(
                (ROOT / "schemas" / "open_macro_v03_prefix_checkpoint.sql")
                .read_text(encoding="utf-8")
            )
            series = sorted(w.seed_series_ids())[0]
            for period, value in (("2024-01-31", 1), ("2024-02-29", 2)):
                cur.execute(
                    "INSERT INTO macro_observation_vintage VALUES"
                    "(%s,%s,%s,%s,%s,0,'fixture','v1')",
                    (series, period, period, value, f"{PRE_CUT} 00:00:00+00"),
                )
            for day, close in (("2024-01-31", 100), ("2024-02-29", 101)):
                cur.execute("INSERT INTO eod_prices VALUES(%s,%s,%s,%s,1000)",
                            (w.SLEEVE_TICKERS[0], day, close, close))
            conn.commit()
        yield conn, schema, series
        conn.rollback()
    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_signature_moves_on_insert_delete_and_in_place_correction(prefix_db) -> None:
    conn, _schema, series = prefix_db
    baseline = w.prefix_signature(conn)
    assert set(baseline) == {"macro_observation_vintage", "eod_prices"}
    # Idempotent: the same window yields the same signature.
    assert w.prefix_signature(conn) == baseline

    # A POST-cut arrival is the normal daily case and must NOT move the prefix
    # signature -- it belongs to the delta, which is read incrementally already.
    conn.execute(
        "INSERT INTO macro_observation_vintage VALUES(%s,'2026-07-31','2026-07-31',9,%s,0,'fixture','v1')",
        (series, f"{POST_CUT} 00:00:00+00"),
    )
    conn.execute("INSERT INTO eod_prices VALUES(%s,'2026-07-31',9,9,1)", (w.SLEEVE_TICKERS[0],))
    assert w.prefix_signature(conn) == baseline

    # An in-place PRE-cut correction keeps the row count and the max date; only the
    # hash sums move. This is the retroactive mutation the pin exists to catch, so
    # it is exactly the case the signature must not miss.
    conn.execute(
        "UPDATE macro_observation_vintage SET value = value + 1"
        " WHERE observation_period = '2024-01-31' AND available_at <= %s",
        (f"{PRE_CUT} 00:00:00+00",),
    )
    corrected = w.prefix_signature(conn)
    assert corrected["macro_observation_vintage"] != baseline["macro_observation_vintage"]
    assert corrected["eod_prices"] == baseline["eod_prices"]
    conn.rollback()

    # A pre-cut delete moves it too.
    conn.execute("DELETE FROM eod_prices WHERE date = '2024-01-31'")
    assert w.prefix_signature(conn)["eod_prices"] != baseline["eod_prices"]
    conn.rollback()


def test_checkpoint_round_trip_and_every_rejection_reason(prefix_db, monkeypatch) -> None:
    conn, _schema, series = prefix_db
    pins = {"macro_observation_vintage": "a" * 64, "eod_prices": "b" * 64}
    signatures = w.prefix_signature(conn)

    # No checkpoint yet.
    assert w.read_prefix_checkpoint(conn, pins, signatures) == (
        "no_checkpoint_for_macro_observation_vintage")

    assert w.write_prefix_checkpoint(conn, pins, signatures,
                                    {"macro_observation_vintage": 2, "eod_prices": 2})
    conn.commit()  # the rollbacks below must not undo the checkpoint itself
    assert w.read_prefix_checkpoint(conn, pins, signatures) is None

    # A moved pin (a re-cut pack, or a checkpoint written for another digest) is
    # never honoured: the checkpoint attests THE pin it proved.
    moved_pin = dict(pins, eod_prices="c" * 64)
    assert w.read_prefix_checkpoint(conn, moved_pin, signatures) == "pin_moved_for_eod_prices"

    # A moved signature forces the full path.
    conn.execute(
        "UPDATE macro_observation_vintage SET value = value + 1"
        " WHERE observation_period = '2024-01-31' AND available_at <= %s",
        (f"{PRE_CUT} 00:00:00+00",),
    )
    assert w.read_prefix_checkpoint(conn, pins, w.prefix_signature(conn)) == (
        "signature_moved_for_macro_observation_vintage")
    conn.rollback()

    # The checkpoint expires on its own, so the byte-exact re-proof happens at
    # least once a week even if nothing ever moves.
    monkeypatch.setenv("OPEN_MACRO_V03_PREFIX_CHECKPOINT_MAX_AGE_HOURS", "1")
    conn.execute(
        f"UPDATE {w.PREFIX_CHECKPOINT_TABLE} SET verified_at = now() - interval '2 hours'")
    assert w.read_prefix_checkpoint(conn, pins, signatures) == (
        "checkpoint_expired_for_macro_observation_vintage")
    monkeypatch.delenv("OPEN_MACRO_V03_PREFIX_CHECKPOINT_MAX_AGE_HOURS")

    # An operator can always force the full path.
    monkeypatch.setenv("OPEN_MACRO_V03_FULL_PREFIX_HASH", "1")
    assert w.read_prefix_checkpoint(conn, pins, signatures) == "forced_by_operator"
    monkeypatch.delenv("OPEN_MACRO_V03_FULL_PREFIX_HASH")
    assert series  # the fixture's seed series is the one the signature covers


def test_checkpoint_write_is_best_effort_and_absent_table_takes_the_full_path(
    prefix_db,
) -> None:
    conn, _schema, _series = prefix_db
    pins = {"macro_observation_vintage": "a" * 64, "eod_prices": "b" * 64}
    signatures = w.prefix_signature(conn)
    conn.execute(f"DROP TABLE {w.PREFIX_CHECKPOINT_TABLE}")
    # Without the migration the worker behaves exactly as before.
    assert w.read_prefix_checkpoint(conn, pins, signatures) == "checkpoint_table_absent"
    assert w.write_prefix_checkpoint(conn, pins, signatures,
                                    {"macro_observation_vintage": 2, "eod_prices": 2}) is False
    # The caller's transaction survives a refused write (the read-only monitor
    # shares this path).
    assert conn.execute("SELECT 1").fetchone() == (1,)
    conn.rollback()


def _stub_compose(monkeypatch, calls: list[str]):
    """Neutralise everything in compose_inputs except the prefix decision."""
    monkeypatch.setattr(w, "_load_json", lambda path: (
        {"input_pack_sha256": w.PACK_SHA256_PIN} if path.name == "manifest.json" else []))
    monkeypatch.setattr(w, "read_delta", lambda conn, as_of: ([], []))
    monkeypatch.setattr(w, "compose_rows",
                        lambda pack, delta, key, what=None: list(pack) + list(delta))
    monkeypatch.setattr(w, "prefix_pins", lambda: {
        "macro_observation_vintage": "a" * 64, "eod_prices": "b" * 64})

    def _read_prefix(conn):
        calls.append("read_prefix")
        return [], []

    def _verify(macro_rows, eod_rows, pins):
        calls.append("verify_prefix_hashes")

    monkeypatch.setattr(w, "read_prefix", _read_prefix)
    monkeypatch.setattr(w, "verify_prefix_hashes", _verify)


def test_compose_inputs_skips_the_full_rehash_only_on_a_checkpoint_hit(
    prefix_db, monkeypatch
) -> None:
    conn, _schema, series = prefix_db
    calls: list[str] = []
    _stub_compose(monkeypatch, calls)
    as_of = _dt.date(2026, 7, 31)

    # First run: no checkpoint -> full read + byte-exact comparison, then record.
    w.compose_inputs(conn, as_of)
    assert calls == ["read_prefix", "verify_prefix_hashes"]

    # Second run, nothing moved: the full pass is skipped.
    calls.clear()
    w.compose_inputs(conn, as_of)
    assert calls == []

    # A retroactive pre-cut correction puts the run straight back on the full path,
    # which is where verify_prefix_hashes raises on a real divergence.
    conn.execute(
        "UPDATE macro_observation_vintage SET value = value + 1"
        " WHERE observation_period = '2024-01-31' AND available_at <= %s",
        (f"{PRE_CUT} 00:00:00+00",),
    )
    calls.clear()
    w.compose_inputs(conn, as_of)
    assert calls == ["read_prefix", "verify_prefix_hashes"]

    # A post-cut arrival (the normal daily case) does NOT re-arm the full pass.
    conn.execute(
        "INSERT INTO macro_observation_vintage VALUES(%s,'2026-07-31','2026-07-31',9,%s,0,'fixture','v1')",
        (series, f"{POST_CUT} 00:00:00+00"),
    )
    calls.clear()
    w.compose_inputs(conn, as_of)
    assert calls == []
    conn.rollback()


def test_a_mismatching_prefix_still_fails_loud_through_the_checkpoint_path(
    prefix_db, monkeypatch
) -> None:
    """The alarm is unchanged: the shortcut only decides WHEN the comparison runs."""
    conn, _schema, _series = prefix_db
    calls: list[str] = []
    _stub_compose(monkeypatch, calls)

    def _raise(macro_rows, eod_rows, pins):
        raise w.OpenMacroV03Error("pre-cut vintage prefix hash deadbeef != pack pin")

    monkeypatch.setattr(w, "verify_prefix_hashes", _raise)
    with pytest.raises(w.OpenMacroV03Error, match="prefix hash"):
        w.compose_inputs(conn, _dt.date(2026, 7, 31))
    # Nothing was checkpointed, so the next run re-runs the comparison too.
    conn.rollback()
    assert conn.execute(
        f"SELECT count(*) FROM {w.PREFIX_CHECKPOINT_TABLE}").fetchone() == (0,)
