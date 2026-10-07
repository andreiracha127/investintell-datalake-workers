"""PostgreSQL: a returns tombstone hides its key from the served ancestry overlay.

``bond_panel_current_returns_v1`` serves the nearest-depth row of every key in the
pointer's ancestry, so a key a child merely omits is served from its parent. The
coupon-PIT repair child tombstones the keys its resolver has no return for; this
proves, on the production DDL text of ``schemas/bond_panel_v1.sql``, that such a
key is absent after the pointer switch, that a later child can publish it again,
and that tombstones are write-once facts of a prepared publication.
"""
from __future__ import annotations

import os
import re
import uuid
from datetime import date
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg import sql  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PANEL_SQL = ROOT / "schemas" / "bond_panel_v1.sql"
CONFIG_HASH = "1863d3d5fa3a0edf"

pytestmark = pytest.mark.skipif(not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL unavailable")


def _ddl() -> list[str]:
    """The relations, guards and view under test, verbatim from the production DDL."""
    text = PANEL_SQL.read_text(encoding="utf-8")
    pieces: list[str] = []
    for table in ("bond_panel_publications", "bond_panel_app_pointer", "bond_panel_returns", "bond_panel_returns_tombstone"):
        match = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \(\n.*?\n\);\n", text, re.DOTALL)
        assert match, table
        pieces.append(match.group(0))
    for function in ("bond_panel_assert_immutable", "bond_panel_assert_returns_tombstone"):
        match = re.search(rf"CREATE OR REPLACE FUNCTION {function}\(\)\n.*?\n\$\$;\n", text, re.DOTALL)
        assert match, function
        pieces.append(match.group(0))
    for trigger in ("bond_panel_returns_immutable", "bond_panel_returns_tombstone_immutable"):
        match = re.search(rf"CREATE TRIGGER {trigger} .*?;\n", text, re.DOTALL)
        assert match, trigger
        pieces.append(match.group(0))
    match = re.search(r"CREATE OR REPLACE VIEW bond_panel_current_returns_v1 AS\n.*?ORDER BY f\.month, f\.cusip_id, a\.depth;\n", text, re.DOTALL)
    assert match and "bond_panel_returns_tombstone" in match.group(0)
    pieces.append(match.group(0))
    return pieces


@pytest.fixture()
def conn():
    schema = f"tomb_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"], autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            connection.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
            for statement in _ddl():
                connection.execute(statement)
            yield connection
        finally:
            connection.execute("SET search_path TO public")
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def _publication(conn, parent: uuid.UUID | None, returns_rows: int, last_closed: date) -> uuid.UUID:
    publication_id = uuid.uuid4()
    conn.execute(
        "INSERT INTO bond_panel_publications (publication_id, parent_publication_id, publication_status, config_hash, input_fingerprint, "
        "code_revision, first_month, last_closed_month, open_month, snapshot_rows, rv_signal_rows, returns_rows, ratings_pit_rows, "
        "source_lineage, gate_evidence) VALUES (%s, %s, 'prepared', %s, %s, 'test', '2021-01-01', %s, %s, 1, 1, %s, 1, '{}', '{}')",
        (publication_id, parent, CONFIG_HASH, uuid.uuid4().hex + uuid.uuid4().hex, last_closed,
         None if parent is None else date(last_closed.year + (last_closed.month == 12), last_closed.month % 12 + 1, 1), returns_rows),
    )
    return publication_id


def _validate(conn, publication_id: uuid.UUID) -> None:
    conn.execute("UPDATE bond_panel_publications SET publication_status = 'validated', validated_at = now() WHERE publication_id = %s", (publication_id,))


def _returns(conn, publication_id: uuid.UUID, rows: list[tuple[date, str, float]]) -> None:
    for month, cusip, total in rows:
        conn.execute(
            "INSERT INTO bond_panel_returns (publication_id, month, cusip_id, total_return, price_return, carry_return, exit_basis, suspect, payload) "
            "VALUES (%s, %s, %s, %s, %s, 0, 'observed', false, '{}')",
            (publication_id, month, cusip, total, total),
        )


def _point(conn, publication_id: uuid.UUID) -> None:
    conn.execute(
        "INSERT INTO bond_panel_app_pointer (product, publication_id) VALUES ('bond_panel_v1', %s) "
        "ON CONFLICT (product) DO UPDATE SET publication_id = EXCLUDED.publication_id",
        (publication_id,),
    )


def _served(conn) -> dict[tuple[date, str], tuple[uuid.UUID, float]]:
    rows = conn.execute("SELECT month, cusip_id, publication_id, total_return FROM bond_panel_current_returns_v1").fetchall()
    return {(month, cusip): (publication, float(total)) for month, cusip, publication, total in rows}


def test_a_tombstoned_key_is_absent_after_the_pointer_switch_and_can_be_republished(conn) -> None:
    dropped, kept, other = (date(2021, 2, 1), "DDD000004"), (date(2021, 4, 1), "DDD000004"), (date(2021, 2, 1), "AAA000001")
    head = _publication(conn, None, 3, date(2021, 4, 1))
    _returns(conn, head, [(*dropped, 0.010), (*kept, 0.020), (*other, 0.030)])
    _validate(conn, head)
    _point(conn, head)
    assert set(_served(conn)) == {dropped, kept, other}

    # The repair child re-prices two keys and has no return for the third.
    child = _publication(conn, head, 2, date(2021, 4, 1))
    _returns(conn, child, [(*kept, 0.021), (*other, 0.031)])
    conn.execute(
        "INSERT INTO bond_panel_returns_tombstone (publication_id, month, cusip_id, reason, payload) VALUES (%s, %s, %s, 'coupon_pit_no_coupon_basis', '{}')",
        (child, *dropped),
    )
    _validate(conn, child)
    _point(conn, child)

    served = _served(conn)
    assert dropped not in served
    assert served == {kept: (child, 0.021), other: (child, 0.031)}
    declared = conn.execute("SELECT returns_rows FROM bond_panel_publications WHERE publication_id = %s", (child,)).fetchone()[0]
    assert len(served) == declared

    # A later child that publishes the key again (smaller depth) is served normally;
    # the tombstone keeps hiding only the rows at its depth or deeper.
    later = _publication(conn, child, 2, date(2021, 5, 1))
    _returns(conn, later, [(*dropped, 0.099), (date(2021, 5, 1), "DDD000004", 0.040)])
    _validate(conn, later)
    _point(conn, later)
    served = _served(conn)
    assert served[dropped] == (later, 0.099)
    assert served[kept] == (child, 0.021)


def test_tombstones_are_write_once_facts_of_a_prepared_publication(conn) -> None:
    head = _publication(conn, None, 1, date(2021, 4, 1))
    _returns(conn, head, [(date(2021, 2, 1), "DDD000004", 0.01)])
    insert = "INSERT INTO bond_panel_returns_tombstone (publication_id, month, cusip_id, reason, payload) VALUES (%s, %s, %s, 'r', '{}')"
    with pytest.raises(psycopg.errors.RaiseException, match="conflicts with a returns row"):
        conn.execute(insert, (head, date(2021, 2, 1), "DDD000004"))
    conn.execute(insert, (head, date(2021, 3, 1), "DDD000004"))
    with pytest.raises(psycopg.errors.RaiseException, match="immutable bond panel returns tombstones"):
        conn.execute("DELETE FROM bond_panel_returns_tombstone WHERE publication_id = %s", (head,))
    _validate(conn, head)
    with pytest.raises(psycopg.errors.RaiseException, match="only write during prepared lifecycle"):
        conn.execute(insert, (head, date(2021, 4, 1), "DDD000004"))


@pytest.mark.parametrize("tombstone_first", [False, True])
def test_return_and_tombstone_are_exclusive_in_both_insert_orders(conn, tombstone_first) -> None:
    publication = _publication(conn, None, 1, date(2021, 4, 1))
    key = (date(2021, 2, 1), "DDD000004")

    def insert_return():
        _returns(conn, publication, [(*key, 0.01)])

    def insert_tombstone():
        conn.execute(
            "INSERT INTO bond_panel_returns_tombstone (publication_id, month, cusip_id, reason, payload) "
            "VALUES (%s, %s, %s, 'r', '{}')",
            (publication, *key),
        )

    first, second = (insert_tombstone, insert_return) if tombstone_first else (insert_return, insert_tombstone)
    first()
    with pytest.raises(psycopg.errors.RaiseException, match="conflicts with"):
        second()
    for table, expected in (("bond_panel_returns", int(not tombstone_first)), ("bond_panel_returns_tombstone", int(tombstone_first))):
        count = conn.execute(sql.SQL("SELECT count(*) FROM {} WHERE publication_id = %s").format(sql.Identifier(table)), (publication,)).fetchone()[0]
        assert count == expected


@pytest.fixture()
def governed_conn():
    """Install the complete production protocol, including the pointer guard."""
    schema = f"rollback_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"], autocommit=True) as connection:
        if not connection.execute("SELECT 1 FROM pg_roles WHERE rolname = 'worker_writer'").fetchone():
            connection.execute("CREATE ROLE worker_writer")
        connection.execute(sql.SQL("CREATE SCHEMA {} AUTHORIZATION worker_writer").format(sql.Identifier(schema)))
        try:
            connection.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
            connection.execute(PANEL_SQL.read_text(encoding="utf-8"))
            yield connection
        finally:
            connection.execute("ROLLBACK; RESET ROLE; SET search_path TO public")
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def _complete_facts(conn, publication, rows, *, legacy_identity=False):
    """Tiny valid dual-series surfaces, with distinctive payload and carry."""
    for month, cusip, total in rows:
        identity = (publication, month, cusip, None if legacy_identity else cusip, None if legacy_identity else "rule_144a")
        conn.execute(
            "INSERT INTO bond_panel_snapshot (publication_id, month, cusip_id, reference_cusip9, distribution_rule, "
            "eligibility_state, eligibility_reason, price, payload) "
            "VALUES (%s, %s, %s, %s, %s, 'included', 'eligible', 99, '{\"original\":true}')", identity,
        )
        conn.execute(
            "INSERT INTO bond_panel_rv_signal (publication_id, month, cusip_id, reference_cusip9, distribution_rule, "
            "eligibility_state, eligibility_reason, residual_bps, payload) "
            "VALUES (%s, %s, %s, %s, %s, 'included', 'eligible', 17, '{}')", identity,
        )
        conn.execute(
            "INSERT INTO bond_panel_rating_pit (publication_id, month, cusip_id, reference_cusip9, distribution_rule, "
            "rating_bucket, rating_state, rating_reason, payload) "
            "VALUES (%s, %s, %s, %s, %s, 'BBB', 'historical_pit', 'test', '{}')", identity,
        )
        conn.execute(
            "INSERT INTO bond_panel_returns (publication_id, month, cusip_id, reference_cusip9, distribution_rule, "
            "total_return, price_return, carry_return, exit_basis, suspect, payload) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s - 0.001, 0.001, 'observed', false, '{\"original\":true}')",
            (*identity, total, total),
        )


def _rollback_history(conn, *, legacy_identity=False):
    month = date(2021, 4, 1)
    dropped, kept, hidden = "DDD000004", "AAA000001", "HHH000001"
    root = _publication(conn, None, 3, month)
    _complete_facts(conn, root, [(month, dropped, 0.01), (month, kept, 0.02), (month, hidden, 0.04)], legacy_identity=legacy_identity)
    _validate(conn, root)
    head = _publication(conn, root, 1, month)
    _complete_facts(conn, head, [(month, kept, 0.03)])
    conn.execute(
        "INSERT INTO bond_panel_returns_tombstone VALUES (%s, %s, %s, 'parent_absence', '{}')", (head, month, hidden),
    )
    _validate(conn, head)
    _point(conn, head)
    before = {
        surface: conn.execute(f"SELECT to_jsonb(s) - 'publication_id' FROM bond_panel_current_{surface}_v1 s ORDER BY month, cusip_id").fetchall()
        for surface in ("snapshot", "rv_signal", "returns", "rating_pit")
    }
    if legacy_identity:
        for rows in before.values():
            for (row,) in rows:
                if row["distribution_rule"] is None:
                    row.update(distribution_rule="rule_144a", reference_cusip9=row["cusip_id"], distribution_decision_id=None)
    child = uuid.uuid4()
    conn.execute(
        "INSERT INTO bond_panel_publications (publication_id, parent_publication_id, publication_status, config_hash, "
        "input_fingerprint, code_revision, first_month, last_closed_month, open_month, snapshot_rows, rv_signal_rows, "
        "returns_rows, ratings_pit_rows, source_lineage, gate_evidence) "
        "SELECT %s, publication_id, 'prepared', config_hash, %s, 't3_returns_coupon_pit_repair_v2', first_month, "
        "last_closed_month, open_month, 2, 2, 2, 2, "
        "jsonb_build_object('coupon_pit_repair', jsonb_build_object('from_head_publication_id', publication_id::text)), "
        "jsonb_build_object('coupon_pit_repair', jsonb_build_object('from_head_publication_id', publication_id::text)) "
        "FROM bond_panel_publications WHERE publication_id = %s",
        (child, uuid.uuid4().hex + uuid.uuid4().hex, head),
    )
    _complete_facts(conn, child, [(month, kept, 0.09), (month, hidden, 0.08)])
    conn.execute(
        "INSERT INTO bond_panel_returns_tombstone VALUES (%s, %s, %s, 'coupon_pit_no_coupon_basis', '{}')",
        (child, month, dropped),
    )
    _validate(conn, child)
    _point(conn, child)
    return root, head, child, before


def _run_rollback(conn, child, parent):
    """Execute the checked-in psql script with equivalent quoted variable values."""
    script = (ROOT / "scripts" / "rollback_bond_panel_coupon_pit.sql").read_text(encoding="utf-8")
    values = {
        "failed_child": str(child), "restore_parent": str(parent),
        "authorization": "owner's reviewed change 164", "code_revision": "a" * 40,
    }
    for name, value in values.items():
        script = script.replace(f":'{name}'", sql.Literal(value).as_string(conn))
    script = re.sub(r"^\\set ON_ERROR_STOP on\n", "", script, flags=re.MULTILINE)
    conn.execute(script)


@pytest.mark.parametrize("legacy_identity", [False, True])
def test_governed_rollback_restores_parent_projection_and_refreshes_mirrors(governed_conn, legacy_identity):
    conn = governed_conn
    _root, head, child, before = _rollback_history(conn, legacy_identity=legacy_identity)
    assert (date(2021, 4, 1), "DDD000004") not in _served(conn)
    assert _served(conn)[date(2021, 4, 1), "AAA000001"] == (child, 0.09)
    # The old runbook's child -> parent UPDATE is rejected by the real guard.
    with pytest.raises(psycopg.errors.RaiseException, match="must directly extend"):
        _point(conn, head)
    _run_rollback(conn, child, head)
    rollback = conn.execute("SELECT publication_id FROM bond_panel_app_pointer").fetchone()[0]
    assert rollback not in (head, child)
    publication = conn.execute(
        "SELECT parent_publication_id, publication_status, source_lineage->'coupon_pit_rollback', "
        "gate_evidence->'coupon_pit_rollback', input_fingerprint FROM bond_panel_publications WHERE publication_id = %s", (rollback,),
    ).fetchone()
    assert publication[:2] == (child, "validated")
    assert publication[2] == publication[3]
    assert publication[2]["owner_authorization"] == "owner's reviewed change 164"
    assert publication[2]["restore_parent_publication_id"] == str(head)
    assert publication[2]["authorized_code_revision"] == "a" * 40
    assert str(rollback).replace("-", "") == publication[4][:32]
    for surface, expected in before.items():
        actual = conn.execute(
            f"SELECT to_jsonb(s) - 'publication_id' FROM bond_panel_current_{surface}_v1 s ORDER BY month, cusip_id"
        ).fetchall()
        assert actual == expected
        # The exact script also refreshes every application-facing mirror.
        mirror_count = conn.execute(f"SELECT count(*) FROM bond_panel_current_{surface}_v1_mat").fetchone()[0]
        assert mirror_count == len(expected)
        assert conn.execute(f"SELECT DISTINCT publication_id FROM bond_panel_current_{surface}_v1_mat").fetchall() == [(rollback,)]
    assert conn.execute("SELECT cusip_id FROM bond_panel_returns_tombstone WHERE publication_id = %s", (rollback,)).fetchall() == [("HHH000001",)]


@pytest.mark.parametrize("wrong_input", ["pointer", "parent"])
def test_governed_rollback_refuses_changed_pointer_or_wrong_parent(governed_conn, wrong_input):
    conn = governed_conn
    root, head, child, _before = _rollback_history(conn)
    expected_error = "expected repair child pointer" if wrong_input == "pointer" else "unchanged-window parent"
    count = conn.execute("SELECT count(*) FROM bond_panel_publications").fetchone()[0]
    with pytest.raises(psycopg.errors.RaiseException, match=expected_error):
        _run_rollback(conn, head if wrong_input == "pointer" else child, root if wrong_input == "parent" else head)
    conn.execute("ROLLBACK; RESET ROLE")
    assert conn.execute("SELECT publication_id FROM bond_panel_app_pointer").fetchone()[0] == child
    assert conn.execute("SELECT count(*) FROM bond_panel_publications").fetchone()[0] == count
