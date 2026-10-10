"""Small public SEC golden cases prevent another real-issuer coverage collapse.

Expected outcomes are the Round 4 baseline, backed by the exact filing titles,
members and count-context quotes preserved with each fixture. Positive proof
belongs to the count; another class's evidence is not a veto of that count.
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from test_sec_foreign_equity_sizing import (
    _round5_count_with_filing_observations,
    db as db,
    refused,
    resolve,
    sql_database as sql_database,
)

FIXTURE = Path(__file__).parent / "fixtures/sec_foreign_equity_sizing/real_configurations.json"
GOLDEN = json.loads(FIXTURE.read_text(encoding="utf-8"))
NUMERIC_FIELDS = {"ordinary_shares", "ratio_numerator", "ratio_denominator"}


def _load_golden_rows(conn, case):
    from psycopg import sql

    for table, rows in case["tables"].items():
        assert table in GOLDEN["columns"]
        columns = GOLDEN["columns"][table]
        statement = sql.SQL(
            "INSERT INTO public.{} ({}) OVERRIDING SYSTEM VALUE VALUES ({})"
        ).format(
            sql.Identifier(table),
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in columns),
        )
        with conn.cursor() as cursor:
            cursor.executemany(statement, rows)


@pytest.mark.parametrize("case", GOLDEN["configurations"], ids=lambda c: c["name"])
def test_real_filing_configuration_keeps_quoted_round4_outcome(db, case):
    """Round 4 outcomes and quoted export rows define this coverage acceptance.

    SHOP, TEAM and TECK are dual-class issuers; TAL, TNK, BLX, CPA, LX and
    GSL retain ordinary counts despite unrelated classes in the same filing.
    ASML's direct line, TSM's ADS line and DLO's A+B total retain the baseline
    total-scope refusal. GSL 2020 retains its missing own-unit-proof refusal.
    """
    assert case["quoted_counts"]
    _load_golden_rows(db, case)
    row = resolve(
        db, ticker=case["symbol"], cik=case["cik"],
        members=case["line_members"], day=case["as_of"],
    )
    for name, expected in case["expected"].items():
        actual = row[name]
        if name in NUMERIC_FIELDS and expected is not None:
            assert actual == Decimal(expected), (case["name"], name, actual, expected)
        elif name == "shares_as_of" and expected is not None:
            assert str(actual) == expected
        else:
            assert actual == expected, (case["name"], name, actual, expected)
    if row["status"] == "resolved":
        assert row["basis"] == "class"
        assert row["share_unit"] == "ordinary"
        assert row["class_binding"] == "explicit"
    else:
        assert row["ordinary_shares"] is None


@pytest.mark.parametrize(
    "kind,title,code", [
        ("preferred", "Class A preferred shares", "share_count_unit_unverified"),
        ("depositary", "Class A American Depositary Shares", "share_count_unit_unverified"),
    ],
    ids=["preferred-same-member-other-context", "depositary-same-member-other-context"],
)
def test_gate_unit_veto_keeps_its_quoted_refusal(db, kind, title, code):
    """Gate quotes remain refusals beside the real Round 4 coverage fixtures.

    The own ordinary title is "Class A ordinary shares". The quoted veto
    names that same member in another context, conflicting with its own
    ordinary proof. Narrowing unrelated-class evidence must keep both refused.
    """
    row = _round5_count_with_filing_observations(
        db, own_positive=True, veto_kind=kind, veto_title=title,
        veto_member=None, same_raw_member=True,
    )
    refused(row, code)
    assert row["share_unit"] != "ordinary"
