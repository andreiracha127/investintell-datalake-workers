"""Read-only publication verifier: recomputes rows_digest by UUID against the build pin.

Fake connection replaying DB-shaped rows (double precision -> float, date ->
date, NULL -> None); no DSN, no database.
"""
from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest

from scripts import verify_bond_market_implied_rating_publication as verifier
from src.bonds import implied_rating as policy

PUBLICATION = "11111111-1111-4111-8111-111111111111"
MONTHS = ("2026-06-01", "2026-07-01", "2026-08-01")


def _snapshot() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "cusip_id": cusip, "month": pd.Timestamp(month), "price": 95.0,
            "spread_final_bps": spread, "mod_dur": 5.0, "trade_count": 10,
            "dollar_volume": 1_000_000.0, "maturity_date": date(2035, 1, 1),
        }
        for cusip, spread in (("AAA000001", 90.0), ("BBB000002", 500.0))
        for month in MONTHS
    ])


def _db_rows(rows: pd.DataFrame) -> list[tuple]:
    """What psycopg returns for the published table: floats, dates, None."""
    out = []
    for record in rows.to_dict(orient="records"):
        tuple_ = []
        for column in policy.PUBLICATION_COLUMNS:
            value = record[column]
            if column in {"month", "d_event_month"}:
                value = None if pd.isna(value) else pd.Timestamp(value).date()
            elif isinstance(value, float) and pd.isna(value):
                value = None
            tuple_.append(value)
        out.append(tuple(tuple_))
    return out


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = None
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        assert sql == verifier.ROWS_SQL and params == (self.conn.publication_id,)
        self.description = [type("C", (), {"name": name})() for name in policy.PUBLICATION_COLUMNS]
        self._rows = self.conn.db_rows

    def fetchall(self):
        return list(self._rows)


class _FakeConnection:
    def __init__(self, *, build, db_rows, pointer, publication_id=PUBLICATION):
        self.build = build
        self.db_rows = db_rows
        self.pointer = pointer
        self.publication_id = publication_id
        self.commits = 0
        self._last = None

    def execute(self, sql, params=None):
        if sql == verifier.BUILD_SQL:
            assert params == (self.publication_id,)
            self._last = self.build
        elif sql == verifier.POINTER_SQL:
            self._last = None if self.pointer is None else (self.pointer,)
        else:
            raise AssertionError(sql)
        return self

    def fetchone(self):
        return self._last

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1


def _fixture(*, tamper=None, pointer=PUBLICATION):
    snapshot = _snapshot()
    rows = policy.build_publication_rows(snapshot, last_closed_month=date(2026, 8, 1))
    digest = policy.rows_digest(rows)
    confirmed, candidates = policy.default_counts(rows)
    build = (
        "panel-1", policy.POLICY_DIGEST, "rev", date(2026, 8, 1), date(2026, 6, 1), date(2026, 8, 1),
        "f" * 64, policy.policy_l_anchor(), len(rows), digest, confirmed, candidates, "validated",
    )
    db_rows = _db_rows(rows)
    if tamper == "cell":
        first = list(db_rows[0])
        first[policy.PUBLICATION_COLUMNS.index("carry_months")] += 1
        db_rows[0] = tuple(first)
    elif tamper == "drop_row":
        db_rows = db_rows[1:]
    return _FakeConnection(build=build, db_rows=db_rows, pointer=pointer), digest


def test_stored_rows_reproduce_the_pinned_digest_and_pass():
    conn, digest = _fixture()
    result = verifier.verify(conn, publication_id=PUBLICATION, expect_rows_digest=digest,
                             expect_policy_digest=policy.POLICY_DIGEST, expect_current=True)
    assert result["ok"] is True, result
    assert result["reasons"] == []
    assert result["recomputed_rows_digest"] == digest == result["stored"]["rows_digest"]
    assert result["is_current"] is True
    assert result["stored"]["l_anchor"]["hex"] == float(policy.policy_l_anchor()).hex()
    assert conn.commits == 1
    json.dumps(result, default=str)


@pytest.mark.parametrize(
    ("tamper", "pointer", "expected"),
    [
        ("cell", PUBLICATION, ["rows_digest_not_reproduced"]),
        ("drop_row", PUBLICATION, ["rows_digest_not_reproduced", "row_count_mismatch"]),
        (None, "22222222-2222-4222-8222-222222222222", ["not_current_pointer"]),
    ],
)
def test_every_divergence_is_a_typed_reason(tamper, pointer, expected):
    conn, _ = _fixture(tamper=tamper, pointer=pointer)
    result = verifier.verify(conn, publication_id=PUBLICATION, expect_current=True)
    assert result["ok"] is False
    for reason in expected:
        assert reason in result["reasons"]


def test_expectations_are_compared():
    conn, _ = _fixture()
    result = verifier.verify(
        conn, publication_id=PUBLICATION, expect_rows_digest="0" * 64, expect_policy_digest="1" * 64,
    )
    assert set(result["reasons"]) == {"expected_rows_digest_mismatch", "expected_policy_digest_mismatch"}


def test_absent_build_is_reported():
    conn = _FakeConnection(build=None, db_rows=[], pointer=None)
    assert verifier.verify(conn, publication_id=PUBLICATION) == {
        "publication_id": PUBLICATION, "ok": False, "reasons": ["build_absent"],
    }


def test_cli_uses_a_read_only_session_and_exits_on_the_verdict(monkeypatch, capsys):
    conn, digest = _fixture()
    seen = {}

    class _Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *exc):
            return False

    def read_only_connect(dsn, **kwargs):
        seen["dsn"] = dsn
        seen["kwargs"] = kwargs
        return _Ctx()

    monkeypatch.setattr(verifier, "read_only_connect", read_only_connect)
    assert verifier.main([
        "--dsn", "postgresql://x", "--publication-id", PUBLICATION, "--expect-rows-digest", digest,
        "--statement-timeout-seconds", "60",
    ]) == 0
    assert seen == {"dsn": "postgresql://x", "kwargs": {"statement_timeout_s": 60}}
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert verifier.main(["--dsn", "postgresql://x", "--publication-id", PUBLICATION,
                          "--expect-rows-digest", "0" * 64]) == 1
