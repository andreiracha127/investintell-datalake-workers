"""V3 contract regressions and independent lifecycle checks on a disposable DB.

Set SEC_TEST_DATABASE_URL to a loopback PostgreSQL database. The production gate
uses timescale/timescaledb:2.27.2-pg18, work_mem=16MB, temp_buffers=8MB, jit=off.
SEC_TEST_SCHEMA_VERSION=2 runs the same assertions against the baseline to prove
the regressions. Controls and invariant checks intentionally also pass on v2.
"""

from __future__ import annotations

import datetime as dt
import itertools
import os
from pathlib import Path
from uuid import uuid4

import pytest

import test_sec_ticker_cik_history as base


# Pin a previous candidate without modifying the checked-out schema during gate
# regression comparisons. The default always tests the working tree's migration.
V3_TEST_SQL = (Path(os.environ["SEC_TEST_V3_SQL_PATH"]).read_text(encoding="utf-8")
               if os.getenv("SEC_TEST_V3_SQL_PATH") else base.V3_SQL)


@pytest.fixture
def schema_dsn():
    import psycopg
    from psycopg import sql

    dsn = base._dsn()
    schema = f"sec_ticker_v3_{uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        conn.execute("SET work_mem = '16MB'")
        conn.execute("SET temp_buffers = '8MB'")
        conn.execute("SET jit = off")
        try:
            conn.execute(base.V1_SQL + base.V2_SQL)
            if os.getenv("SEC_TEST_SCHEMA_VERSION", "3") == "3":
                conn.execute(V3_TEST_SQL)
            yield conn, psycopg.conninfo.make_conninfo(dsn, options=f"-csearch_path={schema}")
        finally:
            conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE")
                         .format(sql.Identifier(schema)))


def _cover(conn, cik, filed, rows, complete=True):
    return base._cover(conn, cik, filed, rows, complete=complete)


def _row(symbol, label="A", namespace="Class", kind="equity"):
    return symbol, f"{namespace}{label}", f"{namespace} {label} common stock", kind


def _end(conn, cik, filed="2024-03-01", description="Class A common stock", **kw):
    return base._event(conn, cik, "25-NSE", filed, kind=kw.pop("kind", "equity"),
                       venue_kind="primary", description=description,
                       extinguished=kw.pop("extinguished", True), **kw)


def _engines(conn, ticker, cik, key, on):
    """Each engine is queried independently; never condition run checks on holds."""
    hold = conn.execute("SELECT state FROM sec_ticker_holds(%s, %s) WHERE cik=%s",
                        (ticker, on, cik)).fetchone()
    line = conn.execute("SELECT status FROM sec_issuer_line_at(%s,%s,%s)",
                        (cik, key, on)).fetchone()
    ticker_run = conn.execute(
        "SELECT EXISTS(SELECT 1 FROM sec_ticker_line_runs(%s,400) r "
        "JOIN sec_issuer_lines(%s) l ON l.line_key=r.line_key AND l.class_key=%s "
        "WHERE r.cik=%s AND r.valid_from<=%s AND (r.valid_to IS NULL OR %s<r.valid_to))",
        (ticker, cik, key, cik, on, on)).fetchone()[0]
    alive_run = conn.execute(
        "SELECT EXISTS(SELECT 1 FROM sec_issuer_lines(%s) l "
        "CROSS JOIN LATERAL sec_line_alive_runs(%s,l.line_key,400) a "
        "WHERE l.class_key=%s AND a.valid_from<=%s AND (a.valid_to IS NULL OR %s<a.valid_to))",
        (cik, cik, key, on, on)).fetchone()[0]
    return {"sec_ticker_holds": bool(hold and hold[0] == "active"),
            "sec_issuer_line_at": bool(line and line[0] == "resolved"),
            "sec_ticker_line_runs": ticker_run, "sec_line_alive_runs": alive_run}


def _assert_engines(conn, ticker, cik, key, on, alive):
    got = _engines(conn, ticker, cik, key, on)
    assert got == dict.fromkeys(got, alive), (ticker, cik, key, on, got)


def test_4226858592_ticker_rename_does_not_make_one_class_tentative(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 101, "2024-01-01", [_row("OLD")])
    _cover(conn, 101, "2024-02-01", [_row("NEW")], complete=False)
    _end(conn, 101)
    _cover(conn, 101, "2024-04-01", [_row("NEW")], complete=False)
    end = conn.execute("SELECT definitive,sec_end_role(class_keys,tentative_keys,"
                       "class_kind,named_kinds,'ClassA','equity'),tentative_keys "
                       "FROM sec_issuer_end_events(101,'2024-05-01')").fetchone()
    assert end == (True, "identified", [])
    _assert_engines(conn, "NEW", 101, "ClassA", "2024-05-01", False)


@pytest.mark.parametrize("form", ["8-K12B", "8-K12G3"])
def test_4226487988_successor_amendment_names_only_its_continued_class(schema_dsn, form):
    conn, _ = schema_dsn
    _cover(conn, 102, "2024-01-01", [_row("SCA"), _row("SCB", "B")])
    base._event(conn, 102, form, "2024-02-28", kind="equity",
                description="Ordinary shares")
    _end(conn, 102, description="Classes A and B common stock", count=2)
    base._event(conn, 102, form + "/A", "2024-03-05", kind="equity", effect="restates",
                description="Class B common stock")
    assert base._issuer(conn, "SCB", "2024-03-05")[:2] == ("ended", None)
    assert base._issuer(conn, "SCB", "2024-03-06")[:2] == ("resolved", 102)
    assert base._issuer(conn, "SCA", "2024-03-06")[:2] == ("ended", None)
    _assert_engines(conn, "SCB", 102, "ClassB", "2024-03-07", True)
    _assert_engines(conn, "SCA", 102, "ClassA", "2024-03-07", False)


@pytest.mark.parametrize("form", ["8-K12B", "8-K12G3"])
def test_successor_cancellation_and_parser_restatement_visibility(schema_dsn, form):
    conn, _ = schema_dsn
    _cover(conn, 103, "2024-01-01", [_row("CSA"), _row("CSB", "B")])
    original = base._event(conn, 103, form, "2024-02-28", kind="equity",
                           description="Class A common stock")
    _end(conn, 103, description="Classes A and B common stock", count=2)
    correction = base._event(conn, 103, form, "2024-02-28", kind="equity",
                             description="Class B common stock", adsh=original)
    assert correction == original
    conn.execute("UPDATE sec_registration_events SET retired_on='2024-06-01', "
                 "retired_reason='parser_correction' WHERE adsh=%s "
                 "AND class_description='Class A common stock'", (original,))
    assert base._issuer(conn, "CSA", "2024-03-03")[:2] == ("ended", None)
    assert base._issuer(conn, "CSB", "2024-03-03")[:2] == ("resolved", 103)
    base._event(conn, 103, form + "/A", "2024-03-05", kind="equity", effect="cancels",
                description="Class B common stock")
    assert base._issuer(conn, "CSB", "2024-03-06")[:2] == ("ended", None)


NON_EQUITY = [("preferred", "Series P preferred stock"), ("warrant", "Warrants"),
              ("unit", "Units"), ("right", "Rights"), ("debt", "Notes due 2030")]


@pytest.mark.parametrize(("kind", "title"), NON_EQUITY)
def test_4226487993_non_equity_end_keeps_its_named_kind_scope(schema_dsn, kind, title):
    conn, _ = schema_dsn
    _cover(conn, 104, "2024-01-01", [_row("CEQ"), ("CNE", "Instrument", title, kind),
                                       ("OTHER", "OtherKind", "Warrants" if kind != "warrant"
                                        else "Notes due 2030",
                                        "warrant" if kind != "warrant" else "debt")])
    _end(conn, 104, description=title, kind="other")
    _assert_engines(conn, "CNE", 104, "Instrument", "2024-03-03", False)
    _assert_engines(conn, "CEQ", 104, "ClassA", "2024-03-03", True)
    _assert_engines(conn, "OTHER", 104, "OtherKind", "2024-03-03", True)


@pytest.mark.parametrize("attachment", [
    "together with associated rights to purchase Series A Preferred Stock",
    "and attached rights to purchase Series A Preferred Stock",
    "and associated Series A Preferred Stock Purchase Rights",
])
def test_4226858584_dependent_rights_are_not_separate_ended_instruments(schema_dsn, attachment):
    conn, _ = schema_dsn
    _cover(conn, 105, "2024-01-01", [_row("EQA"),
                                     ("PRA", "PreferredA", "Series A preferred stock", "preferred"),
                                     ("RTA", "RightA", "Rights", "right")])
    _end(conn, 105, description="Class A Common Stock " + attachment)
    _assert_engines(conn, "EQA", 105, "ClassA", "2024-03-03", False)
    _assert_engines(conn, "PRA", 105, "PreferredA", "2024-03-03", True)
    _assert_engines(conn, "RTA", 105, "RightA", "2024-03-03", True)


def test_standalone_purchase_right_ends_right_but_not_its_underlying_preferred(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 116, "2024-01-01", [_row("REQ"),
                                     ("RPR", "PreferredA", "Series A preferred stock", "preferred"),
                                     ("RRT", "RightA", "Rights to Purchase Series A Preferred Stock", "right")])
    _end(conn, 116, description="Rights to Purchase Series A Preferred Stock", kind="other")
    _assert_engines(conn, "RRT", 116, "RightA", "2024-03-03", False)
    _assert_engines(conn, "RPR", 116, "PreferredA", "2024-03-03", True)
    _assert_engines(conn, "REQ", 116, "ClassA", "2024-03-03", True)


def test_4225233253_class_and_series_are_separate_namespaces(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 106, "2024-01-01", [_row("CLASSA"), _row("SERIESA", namespace="Series")])
    _end(conn, 106, description="Series A common stock")
    assert conn.execute("SELECT sec_class_label('Class II common stock',NULL), "
                        "sec_class_label('Series II common stock',NULL), "
                        "sec_named_classes('Class A and Series A common stock')").fetchone() == (
                            "class:2", "series:2", ["class:a", "series:a"])
    _assert_engines(conn, "CLASSA", 106, "ClassA", "2024-03-03", True)
    _assert_engines(conn, "SERIESA", 106, "SeriesA", "2024-03-03", False)


@pytest.mark.parametrize("description", ["Class B common stock", "Classes A and B common stock"])
def test_4225233257_new_end_scope_starts_at_amendment_filing(schema_dsn, description):
    conn, _ = schema_dsn
    _cover(conn, 107, "2024-01-01", [_row("AMA"), _row("AMB", "B")])
    _end(conn, 107, extinguished=False)
    _cover(conn, 107, "2024-03-15", [_row("AMB", "B")], complete=False)
    base._event(conn, 107, "25-NSE/A", "2024-04-01", kind="equity",
                venue_kind="primary", effect="restates", description=description,
                count=2 if description.startswith("Classes") else 1)
    assert base._issuer(conn, "AMB", "2024-04-01")[:2] == ("resolved", 107)
    _assert_engines(conn, "AMB", 107, "ClassB", "2024-04-02", False)
    dates = conn.execute("SELECT effective_on,class_keys FROM "
                         "sec_issuer_end_events(107,'2024-04-03')").fetchall()
    assert [(day, keys) for day, keys in dates if "ClassB" in (keys or [])] == [
        (dt.date(2024, 4, 2), ["ClassB"])]
    if description.startswith("Classes"):
        assert (dt.date(2024, 3, 2), ["ClassA"]) in dates


def test_4225169087_republished_cover_keeps_filing_order_and_visibility_gate(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 108, "2024-01-01", [_row("PUB")])
    _end(conn, 108, extinguished=False)
    adsh = _cover(conn, 108, "2024-02-01", [_row("PUB")], complete=False)
    conn.execute("UPDATE sec_ticker_cik_observations SET available_on='2024-04-01' "
                 "WHERE adsh=%s", (adsh,))
    assert conn.execute("SELECT count(*) FROM sec_observations_at('2024-03-15',false) "
                        "WHERE adsh=%s", (adsh,)).fetchone()[0] == 0
    _assert_engines(conn, "PUB", 108, "ClassA", "2024-04-03", False)
    # A truly later filing, even learned later still, can reopen a tentative end.
    fresh = _cover(conn, 108, "2024-05-01", [_row("PUB")], complete=False)
    conn.execute("UPDATE sec_ticker_cik_observations SET available_on='2024-06-01' "
                 "WHERE adsh=%s", (fresh,))
    assert base._issuer(conn, "PUB", "2024-05-15")[:2] == ("ended", None)
    _assert_engines(conn, "PUB", 108, "ClassA", "2024-06-02", True)


@pytest.mark.parametrize("form", ["8-A12B/A", "8-A12G/A"])
@pytest.mark.parametrize("has_original", [False, True])
def test_8a_equity_amendment_can_relist_after_definitive_end(schema_dsn, form, has_original):
    conn, _ = schema_dsn
    _cover(conn, 109, "2024-01-01", [_row("RELA"), _row("RELB", "B")])
    if has_original:
        # Carlyle's later amendment registers common again; its scope may equal
        # the old registration without moving the new registration back in time.
        base._event(conn, 109, form.removesuffix("/A"), "2024-01-05", kind="equity",
                    description="Class B common stock")
    _end(conn, 109, description="Classes A and B common stock", count=2)
    base._event(conn, 109, form, "2024-04-01", kind="equity", effect="restates",
                description="Class B common stock")
    _cover(conn, 109, "2024-05-01", [_row("RELA"), _row("RELB", "B")], complete=False)
    _assert_engines(conn, "RELB", 109, "ClassB", "2024-05-03", True)
    _assert_engines(conn, "RELA", 109, "ClassA", "2024-05-03", False)


@pytest.mark.parametrize("kind", ["other", "unknown", None])
def test_registration_amendment_of_preferred_cannot_relist_equity(schema_dsn, kind):
    """Arlington 2023's rights-plan amendment is not an equity relisting."""
    conn, _ = schema_dsn
    _cover(conn, 110, "2024-01-01", [_row("NOREL")])
    _end(conn, 110)
    base._event(conn, 110, "8-A12B/A", "2024-04-01", kind=kind, effect="restates",
                description="Rights to Purchase Series A Junior Preferred Stock")
    _cover(conn, 110, "2024-05-01", [_row("NOREL")], complete=False)
    _assert_engines(conn, "NOREL", 110, "ClassA", "2024-05-03", False)


@pytest.mark.parametrize("registered", ["B", "A", None])
def test_same_symbol_reclassification_requires_matching_equity_registration(schema_dsn, registered):
    """Liberty: Aug 1 registration, Aug 3 old-class end, later LSXMB cover.

    A positively read matching 8-A makes the old class's end non-definitive;
    the end still closes the line until another statement confirms that class.
    Another class's registration and an unread 8-A cannot rescue it.
    """
    conn, _ = schema_dsn
    _cover(conn, 112, "2023-07-01", [_row("LSXMA", "A", "Series"),
                                     _row("LSXMB", "B", "Series")])
    base._event(conn, 112, "8-A12B", "2023-08-01",
                kind="equity" if registered else None,
                description=f"Series {registered} Liberty SiriusXM common stock" if registered else None)
    _end(conn, 112, "2023-08-03", "Series B Liberty SiriusXM common stock")
    _cover(conn, 112, "2023-08-10", [_row("LSXMB", "B", "Series")], complete=False)
    _assert_engines(conn, "LSXMB", 112, "SeriesB", "2023-08-05", False)
    _assert_engines(conn, "LSXMB", 112, "SeriesB", "2023-08-11", registered == "B")


def test_expanded_end_keeps_old_scope_date_and_discovers_newly_listed_class(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 113, "2024-01-01", [_row("OLDA")])
    _end(conn, 113)
    # The stale A cannot reset the definitive March end; B first appears now.
    _cover(conn, 113, "2024-04-01", [_row("OLDA"), _row("NEWB", "B")], complete=False)
    base._event(conn, 113, "25-NSE/A", "2024-05-01", kind="equity", count=2,
                venue_kind="primary", effect="restates", extinguished=True,
                description="Classes A and B common stock")
    assert base._issuer(conn, "NEWB", "2024-04-15")[:2] == ("resolved", 113)
    _assert_engines(conn, "OLDA", 113, "ClassA", "2024-04-15", False)
    _assert_engines(conn, "NEWB", 113, "ClassB", "2024-05-02", False)
    assert conn.execute("SELECT effective_on,class_keys FROM sec_issuer_end_events(113,'2024-06-01') "
                        "ORDER BY effective_on").fetchall() == [
                            (dt.date(2024, 3, 2), ["ClassA"]),
                            (dt.date(2024, 5, 2), ["ClassB"])]


def test_amendment_chain_preserves_each_class_first_scope_date(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 117, "2024-01-01", [_row("CHA"), _row("CHB", "B"), _row("CHC", "C")])
    _end(conn, 117, extinguished=False)
    base._event(conn, 117, "25-NSE/A", "2024-04-01", kind="equity", count=2,
                venue_kind="primary", effect="restates", extinguished=False,
                description="Classes A and B common stock")
    _cover(conn, 117, "2024-04-15", [_row("CHB", "B")], complete=False)
    base._event(conn, 117, "25-NSE/A", "2024-05-01", kind="equity", count=3,
                venue_kind="primary", effect="restates", extinguished=False,
                description="Classes A, B and C common stock")
    assert conn.execute("SELECT effective_on,class_keys FROM sec_issuer_end_events(117,'2024-06-01') "
                        "ORDER BY effective_on").fetchall() == [
                            (dt.date(2024, 3, 2), ["ClassA"]),
                            (dt.date(2024, 4, 2), ["ClassB"]),
                            (dt.date(2024, 5, 2), ["ClassC"])]
    _assert_engines(conn, "CHA", 117, "ClassA", "2024-05-03", False)
    _assert_engines(conn, "CHB", 117, "ClassB", "2024-05-03", True)
    _assert_engines(conn, "CHC", 117, "ClassC", "2024-05-03", False)


def test_mixed_common_and_preferred_description_preserves_only_common_labels(schema_dsn):
    conn, _ = schema_dsn
    assert conn.execute("SELECT sec_named_classes("
                        "'Series A Common Stock; Series P Preferred Stock')").fetchone()[0] == ["series:a"]
    _cover(conn, 118, "2024-01-01", [_row("MXA", "A", "Series"),
                                     _row("MXP", "P", "Series"),
                                     ("MXPR", "PreferredP", "Series P preferred stock", "preferred")])
    _end(conn, 118, description="Series A Common Stock; Series P Preferred Stock", count=2)
    _assert_engines(conn, "MXA", 118, "SeriesA", "2024-03-03", False)
    _assert_engines(conn, "MXP", 118, "SeriesP", "2024-03-03", True)
    _assert_engines(conn, "MXPR", 118, "PreferredP", "2024-03-03", False)


def test_registration_before_amendment_added_scope_cannot_rescue_stale_cover(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 119, "2024-01-01", [_row("RBA"), _row("RBB", "B")])
    _end(conn, 119)
    base._event(conn, 119, "8-A12B", "2024-04-01", kind="equity",
                description="Class B common stock")
    # More than 30 days later: the independent reclassification window cannot
    # demote this definitive closure. The April start is after the original end
    # but before the actual class B end and therefore cannot reopen B.
    base._event(conn, 119, "25-NSE/A", "2024-05-15", kind="equity", count=2,
                venue_kind="primary", effect="restates", extinguished=True,
                description="Classes A and B common stock")
    _cover(conn, 119, "2024-06-01", [_row("RBB", "B")], complete=False)
    _assert_engines(conn, "RBB", 119, "ClassB", "2024-06-03", False)


@pytest.mark.parametrize("middle_effect", ["restates", "cancels"])
def test_removed_or_cancelled_scope_restarts_when_later_amendment_restores_it(schema_dsn, middle_effect):
    conn, _ = schema_dsn
    _cover(conn, 120, "2024-01-01", [_row("RMA"), _row("RMB", "B")])
    _end(conn, 120, description="Classes A and B common stock", count=2, extinguished=False)
    base._event(conn, 120, "25-NSE/A", "2024-04-01", kind="equity", count=1,
                venue_kind="primary", effect=middle_effect, description="Class A common stock")
    _cover(conn, 120, "2024-04-15", [_row("RMB", "B")], complete=False)
    base._event(conn, 120, "25-NSE/A", "2024-05-01", kind="equity", count=2,
                venue_kind="primary", effect="restates", description="Classes A and B common stock")
    assert base._issuer(conn, "RMB", "2024-04-20")[:2] == ("resolved", 120)
    _assert_engines(conn, "RMB", 120, "ClassB", "2024-05-03", False)


def test_registration_amendment_chain_preserves_earlier_relisting_for_runs(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 121, "2024-01-01", [_row("SMA"), _row("SMB", "B"), _row("SMC", "C")])
    _end(conn, 121, description="Class B common stock")
    base._event(conn, 121, "8-K12B", "2024-04-01", kind="equity",
                description="Class A common stock")
    base._event(conn, 121, "8-K12B/A", "2024-05-01", kind="equity", count=2,
                effect="restates", description="Classes A and B common stock")
    _cover(conn, 121, "2024-05-15", [_row("SMB", "B")], complete=False)
    base._event(conn, 121, "8-K12B/A", "2024-06-01", kind="equity", count=3,
                effect="restates", description="Classes A, B and C common stock")
    _assert_engines(conn, "SMB", 121, "ClassB", "2024-05-16", True)


def test_split_scoped_ends_together_close_all_classes_sharing_one_ticker(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 122, "2024-01-01", [_row("SPLIT"), _row("SPLIT", "B")])
    _end(conn, 122)
    base._event(conn, 122, "25-NSE/A", "2024-05-01", kind="equity", count=2,
                venue_kind="primary", effect="restates", extinguished=True,
                description="Classes A and B common stock")
    assert conn.execute("SELECT state FROM sec_ticker_holds('SPLIT','2024-04-01') "
                        "WHERE cik=122").fetchone()[0] == "active"
    assert base._line(conn, 122, "ClassA", "2024-04-01")[0] == "ended"
    assert base._line(conn, 122, "ClassB", "2024-04-01")[0] == "resolved"
    _assert_engines(conn, "SPLIT", 122, "ClassA", "2024-05-03", False)
    _assert_engines(conn, "SPLIT", 122, "ClassB", "2024-05-03", False)
    _cover(conn, 122, "2024-06-01", [_row("SPLIT"), _row("SPLIT", "B")], complete=False)
    _assert_engines(conn, "SPLIT", 122, "ClassA", "2024-06-03", False)
    _assert_engines(conn, "SPLIT", 122, "ClassB", "2024-06-03", False)


@pytest.mark.parametrize("alias", [False, True])
@pytest.mark.parametrize("taint_on", ["2024-02-15", "2024-07-01"])
def test_nonlisted_reading_cannot_make_preferred_end_close_common_history(schema_dsn, alias, taint_on):
    """USB/KKR: kinds belong to their candidate rows, not an alias's whole history.

    A misread preferred observation is suppressed while common T is alive. It
    cannot make a preferred-only end close either the common key or its linked
    predecessor, whether that observation precedes or follows the end.
    """
    conn, _ = schema_dsn
    first_key = "" if alias else "CommonStock"
    preferred = ("OWNP", "PreferredD", "Series D preferred stock", "preferred")
    _cover(conn, 123, "2024-01-01", [("COMMON", first_key, "Common stock", "equity"), preferred])
    _cover(conn, 123, "2024-02-01", [("COMMON", "CommonStock", "Common stock", "equity"), preferred])
    base._observe(conn, 123, "COMMON", taint_on, class_key="CommonStock", kind="preferred",
                  title="Series D preferred stock")
    _end(conn, 123, description="Series D preferred stock", kind="other")
    _cover(conn, 123, "2024-05-01", [("COMMON", "CommonStock", "Common stock", "equity")])
    for on in ("2024-04-01", "2024-06-01"):
        _assert_engines(conn, "COMMON", 123, first_key, on, True)
        _assert_engines(conn, "COMMON", 123, "CommonStock", on, True)
        _assert_engines(conn, "OWNP", 123, "PreferredD", on, False)
        assert conn.execute("SELECT security_kind FROM sec_issuer_line_at(123,'CommonStock',%s)",
                            (on,)).fetchone()[0] == "equity"
    # The legitimate independent preferred line closes at its own end.
    assert conn.execute("SELECT valid_to FROM sec_line_alive_runs(123,'PreferredD',400) "
                        "ORDER BY valid_from").fetchall() == [(dt.date(2024, 3, 2),)]


def test_candidate_kind_scope_keeps_definitive_end_across_class_aliases(schema_dsn):
    """Line-aware engines must match the candidate kind across all aliases."""
    conn, _ = schema_dsn
    _cover(conn, 124, "2024-01-01", [("ALIASED", "ClassAold", "Class A common stock", "equity"),
                                     _row("OTHERCLASS", "C")])
    _end(conn, 124)
    _cover(conn, 124, "2024-05-01", [("ALIASED", "ClassAnew", "Class A common stock", "equity")],
           complete=False)
    for key in ("ClassAold", "ClassAnew"):
        got = _engines(conn, "ALIASED", 124, key, "2024-06-01")
        assert not got["sec_ticker_line_runs"], (key, got)
        assert not got["sec_line_alive_runs"], (key, got)


@pytest.mark.parametrize("relist_b", [False, True])
def test_explicit_series_title_survives_later_missing_title_member_fallback(schema_dsn, relist_b):
    """LTRPA/B: an explicit Series label outranks a generic CommonClass member."""
    conn, _ = schema_dsn
    known = [("FALLA", "CommonClassA", "Series A common stock", "equity"),
             ("FALLB", "CommonClassB", "Series B common stock", "equity")]
    missing = [(symbol, key, None, kind) for symbol, key, _, kind in known]
    _cover(conn, 125, "2024-01-01", known)
    _cover(conn, 125, "2024-02-01", missing)
    _end(conn, 125, description="Series A and Series B common stock", count=2)
    assert conn.execute("SELECT bool_and(definitive) FROM sec_issuer_end_events(125,'2024-03-03')"
                        ).fetchone()[0] is True
    if relist_b:
        base._event(conn, 125, "8-A12B", "2024-04-01", kind="equity",
                    description="Series B common stock")
    _cover(conn, 125, "2024-05-01", missing, complete=False)
    _assert_engines(conn, "FALLA", 125, "CommonClassA", "2024-05-03", False)
    _assert_engines(conn, "FALLB", 125, "CommonClassB", "2024-05-03", relist_b)


def test_future_explicit_namespace_does_not_backfill_an_earlier_end(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 126, "2024-01-01", [("FUTA", "CommonClassA", None, "equity"),
                                     ("FUTB", "CommonClassB", None, "equity")])
    _end(conn, 126, description="Series A common stock")
    before = base._issuer(conn, "FUTA", "2024-04-01")
    assert before[:2] == ("resolved", 126)
    _cover(conn, 126, "2024-05-01", [("FUTA", "CommonClassA", "Series A common stock", "equity")],
           complete=False)
    assert base._issuer(conn, "FUTA", "2024-04-01") == before
    _assert_engines(conn, "FUTA", 126, "CommonClassA", "2024-04-01", True)


def test_missing_common_title_does_not_inherit_another_instrument_kind_title(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 127, "2024-01-01", [("KINDLABEL", "CommonClassA", "Class A common stock", "equity")])
    base._observe(conn, 127, "KINDLABEL", "2024-02-01", class_key="CommonClassA", kind="preferred",
                  title="Series D preferred stock")
    _cover(conn, 127, "2024-03-01", [("KINDLABEL", "CommonClassA", None, "equity")])
    _end(conn, 127, "2024-04-01")
    _cover(conn, 127, "2024-05-01", [("KINDLABEL", "CommonClassA", None, "equity")], complete=False)
    _assert_engines(conn, "KINDLABEL", 127, "CommonClassA", "2024-05-03", False)


def test_all_text_tiebreaks_are_pinned_to_byte_collation(schema_dsn):
    """Runs even on Alpine where en_US currently orders like C: inspect the pin."""
    conn, _ = schema_dsn
    definitions = dict(conn.execute(
        "SELECT proname,pg_get_functiondef(oid) FROM pg_proc "
        "WHERE pronamespace=current_schema()::regnamespace "
        # Public line/end routines now project the implementations below.
        "AND proname IN ('sec_issuer_lines_at','sec_issuer_end_scopes',"
        "'sec_ticker_holds_at','sec_issuer_line_at')").fetchall())
    assert len(definitions) == 4
    for name, definition in definitions.items():
        assert 'COLLATE "C"' in definition, name
    # Punctuation/case create an adversarial tie for the first class of one line.
    _cover(conn, 111, "2024-01-01", [("ORDER", "a", "Common stock", "equity")])
    _cover(conn, 111, "2024-01-01", [("ORDER", "Z", "Common stock", "equity")])
    assert conn.execute("SELECT DISTINCT line_key FROM sec_issuer_lines(111)").fetchall() == [("Z",)]


def test_price_evidence_hoists_registration_and_closure_queries(schema_dsn):
    """Guard the hoist itself; wall-clock before/after belongs to the full reload gate.

    Replacing SQL functions with instrumented wrappers changes inlining and can
    hide the old repeated subplans, so inspect the actual deployed definitions.
    """
    conn, _ = schema_dsn
    definitions = dict(conn.execute(
        "SELECT proname,pg_get_functiondef(oid) FROM pg_proc "
        "WHERE pronamespace=current_schema()::regnamespace AND proname IN "
        "('sec_ticker_line_runs_from','sec_line_alive_runs')").fetchall())
    assert len(definitions) == 2
    for name, definition in definitions.items():
        assert "starts AS MATERIALIZED" in definition, name
        assert "ends AS MATERIALIZED" in definition, name
        assert definition.count("sec_registration_starts(") == 1, name
    assert "line_ends AS MATERIALIZED" in definitions["sec_ticker_line_runs_from"]
    _cover(conn, 114, "2024-01-01", [_row("PERF")])
    _end(conn, 114)
    base._event(conn, 114, "8-A12B", "2024-04-01", kind="equity", description="Class A common stock")
    _cover(conn, 114, "2024-05-01", [_row("PERF")], complete=False)
    alive = conn.execute("SELECT valid_from,valid_to FROM sec_line_price_evidence('PERF',114,'ClassA') "
                         "WHERE evidence='alive' ORDER BY valid_from").fetchall()
    assert alive[0] == (dt.date(2024, 1, 2), dt.date(2024, 3, 2))
    assert alive[1][0] == dt.date(2024, 5, 2)


def test_v3_reapply_rollback_preserve_rows_relfilenodes_and_v2_definitions(schema_dsn):
    conn, _ = schema_dsn
    v3 = base.V3_SQL
    rollback = base.V3_ROLLBACK_SQL
    relations = ("SELECT relname,relfilenode FROM pg_class WHERE relnamespace="
                 "current_schema()::regnamespace AND relkind IN ('r','i') ORDER BY relname")
    routines = ("SELECT proname,pg_get_function_identity_arguments(oid),pg_get_functiondef(oid) "
                "FROM pg_proc WHERE pronamespace=current_schema()::regnamespace ORDER BY 1,2")
    conn.execute(rollback)
    v2_definitions = conn.execute(routines).fetchall()
    _cover(conn, 115, "2024-01-01", [_row("MIG")])
    before = conn.execute(relations).fetchall()
    count = conn.execute("SELECT count(*) FROM sec_ticker_cik_observations").fetchone()
    for migration in (v3, v3, rollback, v3):
        conn.execute(migration)
        assert conn.execute(relations).fetchall() == before
        assert conn.execute("SELECT count(*) FROM sec_ticker_cik_observations").fetchone() == count
        if migration == rollback:
            assert conn.execute(routines).fetchall() == v2_definitions


def _invariant_failures(conn, tickers):
    """Check every observation, including non-listed rows of historically listed CIKs.

    Suppression needs an actually active listed hold and listed run. Reopening is
    checked independently against *each* of the four engines, even when the hold
    stays ended. Candidate labels come only from that filing's own class rows.
    """
    failures = []
    label_line_counts = {}
    for ticker in tickers:
        rows = conn.execute(
            "SELECT cik,class_key,security_kind,source_available_on,adsh,security_title "
            "FROM sec_observations_at('infinity',true) WHERE ticker_key=%s",
            (ticker,)).fetchall()
        for cik, key, kind, on, adsh, title in rows:
            engines = _engines(conn, ticker, cik, key, on)
            if kind not in base.LISTED_KINDS:
                witnesses = [(c, k) for c, k, typ, filed, _, _ in rows
                             if typ in base.LISTED_KINDS and filed <= on]
                listed = []
                for c, k in witnesses:
                    witness = _engines(conn, ticker, c, k, on)
                    statement_symbols = conn.execute(
                        "SELECT tickers FROM sec_issuer_line_at(%s,%s,%s)", (c, k, on)).fetchone()[0]
                    witness["sec_issuer_line_at"] &= ticker in statement_symbols
                    listed.append(witness)
                # Holds aggregate a CIK: inspect its returned statement, not mere
                # presence of an old ended common hold for this same CIK.
                statement = conn.execute(
                    "SELECT statement_adsh FROM sec_ticker_holds(%s,%s) "
                    "WHERE cik=%s AND state='active'", (ticker, on, cik)).fetchone()
                if not statement or statement[0] != adsh:
                    if not any(w["sec_ticker_holds"] and w["sec_issuer_line_at"] for w in listed):
                        failures.append(("nonlisted suppressed without active listed hold", ticker, cik, key))
                if not engines["sec_ticker_line_runs"]:
                    if not any(w["sec_ticker_line_runs"] for w in listed):
                        failures.append(("nonlisted suppressed without listed run", ticker, cik, key))
            ends = conn.execute(
                "SELECT filed,effective_on,issuer_symbols,class_keys,tentative_keys,class_kind,named_kinds "
                "FROM sec_issuer_end_events(%s,'infinity',true) WHERE definitive AND effective_on<=%s",
                (cik, on)).fetchall()
            for filed, effective, symbols, keys, tentative, end_kind, named in ends:
                if not any(c == cik and k == key and before < effective
                           for c, k, _, before, _, _ in rows):
                    continue
                role = conn.execute("SELECT sec_end_role(%s,%s,%s,%s,%s,%s)",
                                    (keys, tentative, end_kind, named, key, kind)).fetchone()[0]
                if role != "identified":
                    continue
                label = conn.execute("SELECT sec_class_label(%s,%s)", (title, key)).fetchone()[0]
                starts = conn.execute("SELECT classes FROM sec_registration_starts(%s,'infinity',true) "
                                      "WHERE filed>%s AND available_on<=%s", (cik, filed, on)).fetchall()
                identity = cik, on, label
                if identity not in label_line_counts:
                    label_line_counts[identity] = conn.execute(
                        "SELECT count(DISTINCT l.line_key) FROM sec_issuer_lines(%s) l "
                        "JOIN sec_observations_at('infinity',true) o ON o.class_key=l.class_key "
                        "WHERE o.cik=%s AND o.source_available_on<=%s "
                        "AND o.security_kind IN ('equity','depositary','unknown') "
                        "AND sec_class_label(o.security_title,o.class_key)=%s",
                        (cik, cik, on, label)).fetchone()[0]
                supported = any((label in labels and label_line_counts[identity] == 1)
                                or (not labels and symbols == 1) for (labels,) in starts)
                if not supported:
                    for engine, active in engines.items():
                        if active:
                            # A ticker hold can correctly belong to another class;
                            # only its own candidate row is a reopening of this one.
                            if engine == "sec_ticker_holds":
                                own = conn.execute("SELECT statement_adsh FROM sec_ticker_holds(%s,%s) "
                                                   "WHERE cik=%s", (ticker, on, cik)).fetchone()
                                if not own or own[0] != adsh:
                                    continue
                            elif engine == "sec_issuer_line_at":
                                own = conn.execute("SELECT adsh FROM sec_issuer_line_at(%s,%s,%s)",
                                                   (cik, key, on)).fetchone()
                                if not own or own[0] != adsh:
                                    continue
                            failures.append(("unsupported reopening", engine, ticker, cik, key, on))
    return failures


def test_four_engine_invariants_cover_same_cik_nonlisted_and_class_move(schema_dsn):
    conn, _ = schema_dsn
    tickers = []
    ids = itertools.count(200)
    for same_cik, kind in itertools.product((True, False), ("preferred", "debt")):
        cik = next(ids)
        claimant = cik if same_cik else cik + 10000
        ticker = f"INV{cik}"
        _cover(conn, cik, "2024-01-01", [_row(ticker)])
        _end(conn, cik)
        _cover(conn, cik, "2024-04-01", [_row(ticker)], complete=False)
        title = "Series P preferred stock" if kind == "preferred" else "Notes due 2030"
        base._observe(conn, claimant, ticker, "2024-05-01", class_key="Instrument", kind=kind, title=title)
        # Non-equity evidence is kept even under a CIK with old listed history.
        _assert_engines(conn, ticker, claimant, "Instrument", "2024-05-02", True)
        for engine, active in _engines(conn, ticker, cik, "ClassA", "2024-05-02").items():
            if engine != "sec_ticker_holds" or not same_cik:
                assert not active, (same_cik, kind, engine)
        tickers.append(ticker)
    for registered in ("A", "B", None):
        cik = next(ids)
        ticker = f"MOVE{cik}"
        _cover(conn, cik, "2024-01-01", [_row(ticker), _row(ticker + "B", "B")])
        _cover(conn, cik, "2024-02-01", [_row(ticker + "A"), _row(ticker, "B")])
        _end(conn, cik, description="Class B common stock")
        if registered:
            base._event(conn, cik, "8-A12B", "2024-04-01", kind="equity",
                        description=f"Class {registered} common stock")
        _cover(conn, cik, "2024-05-01", [_row(ticker, "B")], complete=False)
        _assert_engines(conn, ticker, cik, "ClassB", "2024-05-02", registered == "B")
        tickers.append(ticker)
    observed = conn.execute("SELECT count(*) FROM sec_observations_at('infinity',true) "
                            "WHERE ticker_key=ANY(%s)", (tickers,)).fetchone()[0]
    assert (len(tickers), observed) == (7, 21)
    failures = _invariant_failures(conn, tickers)
    assert failures == []
    print(f"Invariant cross-check: {len(tickers)} scenarios, {observed} observations, "
          f"{4 * observed} independent engine evaluations, {len(failures)} violations")


def test_existing_generated_invariant_space_checked_in_all_four_engines(schema_dsn, monkeypatch):
    """Reuse the established scenario generator with the expanded independent checker."""
    seen = []

    def expanded_check(conn, tickers):
        observed = conn.execute("SELECT count(*) FROM sec_observations_at('infinity',true) "
                                "WHERE ticker_key=ANY(%s)", (tickers,)).fetchone()[0]
        failures = _invariant_failures(conn, tickers)
        seen.append((len(tickers), observed, len(failures)))
        print(f"Expanded generated cross-check: {len(tickers)} scenarios, {observed} observations, "
              f"{4 * observed} independent engine evaluations, {len(failures)} violations")
        return failures

    monkeypatch.setattr(base, "_admission_invariant_failures", expanded_check)
    base.test_generated_admission_invariants(schema_dsn)
    assert len(seen) == 1 and seen[0][0] == 93 and seen[0][2] == 0


def _gate_alias_fixture(conn, cik):
    first, second = "CommonClassA", "ClassACommonStock"
    _cover(conn, cik, "2026-04-01", [("CWENA", first, "Class A common stock", "equity")])
    _cover(conn, cik, "2026-04-15", [("CWENA", second, "Class A common stock", "equity")],
           complete=False)
    _end(conn, cik, "2026-05-01")
    _cover(conn, cik, "2026-05-07", [("CWENA", second, "Class A common stock", "equity")],
           complete=False)
    return first, second


def test_gate_alias_keys_count_as_one_line_for_definitive_label_scope(schema_dsn):
    conn, _ = schema_dsn
    keys = _gate_alias_fixture(conn, 301)
    assert conn.execute("SELECT count(DISTINCT line_key) FROM sec_issuer_lines(301)").fetchone()[0] == 1
    assert conn.execute("SELECT bool_and(definitive),bool_and(cardinality(tentative_keys)=0) "
                        "FROM sec_issuer_end_events(301,'2026-05-10')").fetchone() == (True, True)
    for key in keys:
        _assert_engines(conn, "CWENA", 301, key, "2026-05-10", False)


def test_gate_dated_alias_uniqueness_does_not_use_future_coexistence(schema_dsn):
    conn, _ = schema_dsn
    first, second = _gate_alias_fixture(conn, 302)
    before = (base._issuer(conn, "CWENA", "2026-05-10"),
              base._line(conn, 302, first, "2026-05-10"))
    assert before[0][:2] == ("ended", None)
    assert before[1][0] == "ended"
    # Only this later filing establishes that the keys coexist as separate lines.
    _cover(conn, 302, "2026-09-01", [("CWENA", first, "Class A common stock", "equity"),
                                     ("CWENB", second, "Class A common stock", "equity")])
    assert (base._issuer(conn, "CWENA", "2026-05-10"),
            base._line(conn, 302, first, "2026-05-10")) == before


def _gate_non_equity_fixture(conn, cik, kind, same_cik, description=None):
    noun = "preferred stock" if kind == "preferred" else "warrants"
    ticker, other = f"GT{cik}", f"GA{cik}"
    _cover(conn, cik, "2024-01-01", [(other, "InstrumentA", f"Series A {noun}", kind),
                                     (ticker, "InstrumentB", f"Series B {noun}", kind)])
    common_cik = cik if same_cik else cik + 10000
    # An incomplete cover adds common T without asserting absence of the
    # previously observed preferred/warrant line under the same CIK.
    _cover(conn, common_cik, "2024-02-01", [(ticker, "Common", "Common stock", "equity")],
           complete=False)
    _end(conn, cik, description=description or f"Series A {noun}", kind="other")
    return ticker, other, common_cik


def _other_line_covers(conn, ticker, common_cik, holder_cik, holder_key, on):
    return conn.execute(
        "SELECT EXISTS(SELECT 1 FROM sec_line_price_evidence(%s,%s,'Common') e "
        "JOIN sec_issuer_lines(%s) l ON l.line_key=e.line_key AND l.class_key=%s "
        "WHERE e.evidence='other_holder' AND e.holder_cik=%s "
        "AND e.valid_from<=%s AND (e.valid_to IS NULL OR %s<e.valid_to))",
        (ticker, common_cik, holder_cik, holder_key, holder_cik, on, on)).fetchone()[0]


@pytest.mark.parametrize("kind", ["preferred", "warrant"])
@pytest.mark.parametrize("same_cik", [False, True])
def test_gate_non_equity_end_leaves_named_kinds_other_series_competing(schema_dsn, kind, same_cik):
    conn, _ = schema_dsn
    ticker, other, common_cik = _gate_non_equity_fixture(conn, 303, kind, same_cik)
    _assert_engines(conn, other, 303, "InstrumentA", "2024-04-01", False)
    _assert_engines(conn, ticker, 303, "InstrumentB", "2024-04-01", True)
    _assert_engines(conn, ticker, common_cik, "Common", "2024-04-01", True)
    assert _other_line_covers(conn, ticker, common_cik, 303, "InstrumentB", "2024-04-01")


@pytest.mark.parametrize("kind", ["preferred", "warrant"])
def test_gate_unnamed_multiple_same_kind_lines_are_tentative_not_definitive(schema_dsn, kind):
    conn, _ = schema_dsn
    noun = "Preferred stock" if kind == "preferred" else "Warrants"
    _cover(conn, 304, "2024-01-01", [("UNA", "InstrumentA", f"Series A {noun}", kind),
                                     ("UNB", "InstrumentB", f"Series B {noun}", kind)])
    _end(conn, 304, description=noun, kind="other", count=1)
    end = conn.execute("SELECT definitive,sec_end_role(class_keys,tentative_keys,class_kind,named_kinds,"
                       "'InstrumentA',%s),sec_end_role(class_keys,tentative_keys,class_kind,named_kinds,"
                       "'InstrumentB',%s) FROM sec_issuer_end_events(304,'2024-03-03')", (kind, kind)).fetchone()
    assert end == (False, "tentative", "tentative")
    _cover(conn, 304, "2024-04-01", [("UNA", "InstrumentA", f"Series A {noun}", kind),
                                     ("UNB", "InstrumentB", f"Series B {noun}", kind)], complete=False)
    _assert_engines(conn, "UNA", 304, "InstrumentA", "2024-04-03", True)
    _assert_engines(conn, "UNB", 304, "InstrumentB", "2024-04-03", True)


@pytest.mark.parametrize("kind", ["preferred", "warrant"])
def test_gate_unnamed_single_kind_line_is_identified(schema_dsn, kind):
    conn, _ = schema_dsn
    noun = "Preferred stock" if kind == "preferred" else "Warrants"
    _cover(conn, 305, "2024-01-01", [("SOLE", "InstrumentA", f"Series A {noun}", kind), _row("COMMON")])
    _end(conn, 305, description=noun, kind="other")
    assert conn.execute("SELECT definitive,sec_end_role(class_keys,tentative_keys,class_kind,named_kinds,"
                        "'InstrumentA',%s) FROM sec_issuer_end_events(305,'2024-03-03')", (kind,)).fetchone() == (
                            True, "identified")
    _cover(conn, 305, "2024-04-01", [("SOLE", "InstrumentA", f"Series A {noun}", kind)], complete=False)
    _assert_engines(conn, "SOLE", 305, "InstrumentA", "2024-04-03", False)
    _assert_engines(conn, "COMMON", 305, "ClassA", "2024-04-03", True)


@pytest.mark.parametrize("kind", ["preferred", "warrant"])
@pytest.mark.parametrize("phrase", ["trading symbol", "trading symbol is"])
def test_gate_non_equity_symbol_identifies_one_of_same_kind_siblings(schema_dsn, kind, phrase):
    conn, _ = schema_dsn
    noun = "Preferred stock" if kind == "preferred" else "Warrants"
    ticker, other, common_cik = _gate_non_equity_fixture(
        conn, 306, kind, False, description=f"{noun}, {phrase} GA306")
    _assert_engines(conn, other, 306, "InstrumentA", "2024-04-01", False)
    _assert_engines(conn, ticker, 306, "InstrumentB", "2024-04-01", True)
    assert _other_line_covers(conn, ticker, common_cik, 306, "InstrumentB", "2024-04-01")


def test_gate_symbol_prose_is_not_a_positive_instrument_identifier(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 308, "2024-01-01", [("PRA", "InstrumentA", "Series A preferred stock", "preferred"),
                                     ("OF", "InstrumentB", "Series B preferred stock", "preferred")])
    _end(conn, 308, description="The trading symbol of the preferred stock", kind="other")
    assert conn.execute("SELECT definitive,sec_end_role(class_keys,tentative_keys,class_kind,named_kinds,"
                        "'InstrumentA','preferred'),sec_end_role(class_keys,tentative_keys,class_kind,named_kinds,"
                        "'InstrumentB','preferred') FROM sec_issuer_end_events(308,'2024-03-03')"
                        ).fetchone() == (False, "tentative", "tentative")


def test_gate_warrant_end_does_not_end_its_preferred_purchase_target(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 309, "2024-01-01", [("WA", "WarrantA", "Class A warrants", "warrant"),
                                     ("WB", "WarrantB", "Class B warrants", "warrant"),
                                     ("PB", "PreferredB", "Series B preferred stock", "preferred")])
    _end(conn, 309, description="Class A warrants to purchase Series B preferred stock", kind="other")
    _assert_engines(conn, "WA", 309, "WarrantA", "2024-04-01", False)
    _assert_engines(conn, "WB", 309, "WarrantB", "2024-04-01", True)
    _assert_engines(conn, "PB", 309, "PreferredB", "2024-04-01", True)


@pytest.mark.parametrize("rename_complete", [False, True])
@pytest.mark.parametrize("corroborating_symbol", [False, True])
def test_gate_named_non_equity_class_end_survives_ticker_rename(schema_dsn, rename_complete, corroborating_symbol):
    conn, _ = schema_dsn
    common = ("UNCHANGED", "Common", "Common stock", "equity")
    _cover(conn, 312, "2024-01-01", [("OLD", "PreferredA", "Series A preferred stock", "preferred"), common])
    _cover(conn, 312, "2024-02-01", [("NEW", "PreferredA", "Series A preferred stock", "preferred"), common],
           complete=rename_complete)
    description = "Series A preferred stock" + (", trading symbol NEW" if corroborating_symbol else "")
    _end(conn, 312, description=description, kind="other")
    assert conn.execute("SELECT bool_and(definitive) FROM sec_issuer_end_events(312,'2024-03-03')"
                        ).fetchone()[0] is True
    _cover(conn, 312, "2024-05-01", [("OLD", "PreferredA", "Series A preferred stock", "preferred")],
           complete=False)
    _assert_engines(conn, "OLD", 312, "PreferredA", "2024-05-03", False)
    _assert_engines(conn, "NEW", 312, "PreferredA", "2024-05-03", False)
    _assert_engines(conn, "UNCHANGED", 312, "Common", "2024-05-03", True)


@pytest.mark.parametrize(("title", "member", "kind", "expected"), [
    (None, "ClassOfStock=SeriesA;", "preferred", "series:a"),
    (None, "ClassOfStock=CommonStockSeriesA;", "preferred", None),
    (None, "ClassOfStock=WarrantsToPurchaseSeriesA;", "warrant", None),
    (None, "ClassOfStock=RightsToPurchaseSeriesA;", "right", None),
    (None, "ClassOfStock=WarrantsSeriesBToPurchaseSeriesA;", "warrant", "series:b"),
    ("Series A", "PreferredSeriesB", "preferred", "series:a"),
    ("Common stock", "PreferredSeriesB", "preferred", None),
    ("Rights to purchase Series A preferred stock", "PreferredSeriesB", "preferred", None),
    ("Warrants to purchase Series A common stock", "WarrantsSeriesA", "warrant", "series:a"),
    ("Rights to purchase Series A preferred stock", "RightsSeriesA", "right", "series:a"),
    ("Series A preferred stock; Series B preferred stock", None, "preferred", None),
    ("Series A preferred stock; Series A preferred stock", None, "preferred", "series:a"),
])
def test_instrument_label_uses_own_title_or_member_kind(schema_dsn, title, member, kind, expected):
    conn, _ = schema_dsn
    assert conn.execute("SELECT sec_instrument_label(%s,%s,%s)", (title, member, kind)).fetchone()[0] == expected


def test_gate_kind_free_own_member_label_preserves_definitive_end(schema_dsn):
    conn, _ = schema_dsn
    key = "ClassOfStock=SeriesA;"
    _cover(conn, 313, "2024-01-01", [("BARE", key, None, "preferred")])
    _end(conn, 313, description="Series A preferred stock", kind="other")
    assert conn.execute("SELECT bool_and(definitive) FROM sec_issuer_end_events(313,'2024-03-03')"
                        ).fetchone()[0] is True
    _cover(conn, 313, "2024-05-01", [("BARE", key, None, "preferred")], complete=False)
    _assert_engines(conn, "BARE", 313, key, "2024-05-03", False)


@pytest.mark.parametrize("description", [
    'Preferred stock (trading symbol "ON")',
    "Preferred stock (NYSE: ON)",
    "Preferred stock, trading symbol: ON",
])
def test_gate_delimited_symbol_is_not_discarded_as_an_ordinary_word(schema_dsn, description):
    conn, _ = schema_dsn
    _cover(conn, 314, "2024-01-01", [("PRA", "InstrumentA", "Series A preferred stock", "preferred"),
                                     ("ON", "InstrumentB", "Series B preferred stock", "preferred")])
    _end(conn, 314, description=description, kind="other")
    _assert_engines(conn, "ON", 314, "InstrumentB", "2024-04-01", False)
    _assert_engines(conn, "PRA", 314, "InstrumentA", "2024-04-01", True)


def test_gate_bare_right_purchase_target_is_not_the_rights_own_series(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 315, "2024-01-01", [("RA", "RightA", "Series A rights", "right"),
                                     ("RB", "RightB", "Series B rights", "right")])
    _end(conn, 315, description="Rights to purchase Series A preferred stock", kind="other")
    assert conn.execute("SELECT definitive,sec_end_role(class_keys,tentative_keys,class_kind,named_kinds,"
                        "'RightA','right'),sec_end_role(class_keys,tentative_keys,class_kind,named_kinds,"
                        "'RightB','right') FROM sec_issuer_end_events(315,'2024-03-03')"
                        ).fetchone() == (False, "tentative", "tentative")


def test_gate_same_kind_clauses_keep_their_own_optional_symbol_scopes(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 316, "2024-01-01", [("PRA", "PreferredA", "Series A preferred stock", "preferred"),
                                     ("PRB", "PreferredB", "Series B preferred stock", "preferred"),
                                     ("T", "PreferredC", "Series C preferred stock", "preferred")])
    _cover(conn, 10316, "2024-02-01", [("T", "Common", "Common stock", "equity")], complete=False)
    _end(conn, 316, description="Series A preferred stock; Series B preferred stock, trading symbol PRB",
         kind="other", count=2)
    _assert_engines(conn, "PRA", 316, "PreferredA", "2024-04-01", False)
    _assert_engines(conn, "PRB", 316, "PreferredB", "2024-04-01", False)
    _assert_engines(conn, "T", 316, "PreferredC", "2024-04-01", True)
    _assert_engines(conn, "T", 10316, "Common", "2024-04-01", True)
    assert _other_line_covers(conn, "T", 10316, 316, "PreferredC", "2024-04-01")


def test_gate_label_symbol_pairs_do_not_cross_match_same_kind_siblings(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 317, "2024-01-01", [("PRA", "PreferredA", "Series A preferred stock", "preferred"),
                                     ("PRB", "PreferredB", "Series B preferred stock", "preferred"),
                                     ("PRB", "SwappedA", "Series A other preferred stock", "preferred")])
    _cover(conn, 10317, "2024-02-01", [("PRB", "Common", "Common stock", "equity")], complete=False)
    _end(conn, 317, description="Series A preferred stock, trading symbol PRA; "
                               "Series B preferred stock, trading symbol PRB", kind="other", count=2)
    _assert_engines(conn, "PRA", 317, "PreferredA", "2024-04-01", False)
    # The CIK-level PRB hold belongs to the surviving swapped-label sibling.
    ended = _engines(conn, "PRB", 317, "PreferredB", "2024-04-01")
    assert ended == {"sec_ticker_holds": True, "sec_issuer_line_at": False,
                     "sec_ticker_line_runs": False, "sec_line_alive_runs": False}
    _assert_engines(conn, "PRB", 317, "SwappedA", "2024-04-01", True)
    _assert_engines(conn, "PRB", 10317, "Common", "2024-04-01", True)
    assert _other_line_covers(conn, "PRB", 10317, 317, "SwappedA", "2024-04-01")


def test_gate_historical_unknown_label_cannot_erase_a_declared_series_constraint(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 318, "2024-01-01", [("T", "Preferred", "Preferred stock", "preferred"),
                                     ("OTHER", "CommonOther", "Common stock", "equity")])
    _cover(conn, 318, "2024-02-01", [("T", "Preferred", "Series B preferred stock", "preferred")],
           complete=False)
    _cover(conn, 10318, "2024-02-15", [("T", "Common", "Common stock", "equity")], complete=False)
    _end(conn, 318, description="Series A preferred stock", kind="other")
    _assert_engines(conn, "T", 318, "Preferred", "2024-04-01", True)
    _assert_engines(conn, "T", 10318, "Common", "2024-04-01", True)
    assert _other_line_covers(conn, "T", 10318, 318, "Preferred", "2024-04-01")


def test_gate_retained_non_equity_label_keeps_original_end_date_when_sibling_removed(schema_dsn):
    conn, _ = schema_dsn
    _cover(conn, 319, "2024-01-01", [("PRA", "PreferredA", "Series A preferred stock", "preferred"),
                                     ("PRB", "PreferredB", "Series B preferred stock", "preferred")])
    _end(conn, 319, description="Series A and Series B preferred stock", kind="other", count=2)
    _cover(conn, 319, "2024-04-01", [("PRA", "PreferredA", "Series A preferred stock", "preferred")],
           complete=False)
    base._event(conn, 319, "25-NSE/A", "2024-05-01", kind="other", count=1, venue_kind="primary",
                extinguished=True, effect="restates", description="Series A preferred stock")
    assert conn.execute("SELECT effective_on FROM sec_issuer_end_events(319,'2024-06-01') e "
                        "WHERE sec_end_role(e.class_keys,e.tentative_keys,e.class_kind,e.named_kinds,"
                        "'PreferredA','preferred') IS NOT NULL").fetchall() == [(dt.date(2024, 3, 2),)]
    _assert_engines(conn, "PRA", 319, "PreferredA", "2024-04-03", False)
    _assert_engines(conn, "PRB", 319, "PreferredB", "2024-06-01", True)


def test_gate_named_historical_instrument_end_keeps_direct_resolver_compatibility(schema_dsn):
    conn, _ = schema_dsn
    jan = _cover(conn, 320, "2024-01-01", [("PRA", "PA", "Series A preferred stock", "preferred"),
                                         ("T", "PB", "Series B preferred stock", "preferred")], complete=False)
    conn.execute("UPDATE sec_ticker_cik_observations SET filing_complete=true WHERE adsh=%s", (jan,))
    _cover(conn, 320, "2024-02-01", [("T", "Common", "Common stock", "equity")])
    _end(conn, 320, description="Series A preferred stock", kind="other")
    # Preserve the established complete-cover lifecycle. The raw-class dated
    # resolver did not follow that cover for non-equity keys even on e5f18d24.
    for ticker, key in (("PRA", "PA"), ("T", "PB")):
        before = _engines(conn, ticker, 320, key, "2024-02-15")
        assert before == {"sec_ticker_holds": ticker == "T", "sec_issuer_line_at": True,
                          "sec_ticker_line_runs": False, "sec_line_alive_runs": False}
    _assert_engines(conn, "PRA", 320, "PA", "2024-03-15", False)
    assert _engines(conn, "T", 320, "PB", "2024-03-15") == {
        "sec_ticker_holds": True, "sec_issuer_line_at": True,
        "sec_ticker_line_runs": False, "sec_line_alive_runs": False}
    _assert_engines(conn, "T", 320, "Common", "2024-03-15", True)


def _gate_tracking_fixture(conn, cik, groups, form):
    rows = [(f"G{cik}{n}", f"Group{n}Original", f"Series A Group {n} Common Stock", "equity")
            for n in range(1, groups + 1)]
    _cover(conn, cik, "2024-01-01", rows)
    aliases = [(ticker, f"Group{n}Alias", title, kind)
               for n, (ticker, _, title, kind) in enumerate(rows, 1) if n <= 3]
    _cover(conn, cik, "2024-02-01", aliases, complete=False)
    mapping = dict(conn.execute("SELECT class_key,line_key FROM sec_issuer_lines(%s)", (cik,)).fetchall())
    components = {frozenset(key for key, owner in mapping.items() if owner == line)
                  for line in mapping.values()}
    assert components == {frozenset((f"Group{n}Original", f"Group{n}Alias")) if n <= 3
                          else frozenset((f"Group{n}Original",)) for n in range(1, groups + 1)}
    # Isolate the registration guard from the separate raw-alias counting bug:
    # the declared whole-class end covers even the baseline's raw-key count.
    _end(conn, cik, description="Common stock", count=len(rows) + len(aliases))
    assert conn.execute("SELECT bool_and(definitive) FROM sec_issuer_end_events(%s,'2024-03-03')",
                        (cik,)).fetchone()[0] is True
    base._event(conn, cik, form, "2024-04-01", kind="equity", effect="restates" if form.endswith("/A") else None,
                description="Series A Group 1 Common Stock")
    _cover(conn, cik, "2024-05-01", rows, complete=False)
    return rows


@pytest.mark.parametrize("groups", [1, 4])
@pytest.mark.parametrize("form", ["8-A12B", "8-A12B/A", "8-K12B/A"])
def test_gate_registration_label_must_identify_one_line_not_one_raw_key(schema_dsn, groups, form):
    conn, _ = schema_dsn
    rows = _gate_tracking_fixture(conn, 307, groups, form)
    assert conn.execute("SELECT count(*),count(DISTINCT line_key) FROM sec_issuer_lines(307)").fetchone() == (
        2 if groups == 1 else 7, groups)
    for ticker, key, _, _ in rows:
        _assert_engines(conn, ticker, 307, key, "2024-05-03", groups == 1)


def test_gate_dated_registration_uniqueness_ignores_future_line_split(schema_dsn):
    conn, _ = schema_dsn
    rows = _gate_tracking_fixture(conn, 310, 1, "8-A12B/A")
    ticker, key, title, kind = rows[0]
    before = (base._issuer(conn, ticker, "2024-05-03"), base._line(conn, 310, key, "2024-05-03"))
    assert before[0][:2] == ("resolved", 310)
    assert before[1][0] == "resolved"
    _cover(conn, 310, "2024-09-01", [(ticker, key, title, kind),
                                     ("SPLITLATER", "Group1Alias", title, kind)])
    assert (base._issuer(conn, ticker, "2024-05-03"), base._line(conn, 310, key, "2024-05-03")) == before


def test_gate_ambiguous_named_registration_cannot_use_unnamed_one_symbol_fallback(schema_dsn):
    conn, _ = schema_dsn
    rows = [("SHARED", "Group1", "Series A Group One Common Stock", "equity"),
            ("SHARED", "Group2", "Series A Group Two Common Stock", "equity")]
    _cover(conn, 311, "2024-01-01", rows)
    _end(conn, 311, description="Common stock", count=2)
    assert conn.execute("SELECT bool_and(definitive) FROM sec_issuer_end_events(311,'2024-03-03')"
                        ).fetchone()[0] is True
    base._event(conn, 311, "8-A12B/A", "2024-04-01", kind="equity", effect="restates",
                description="Series A Group One Common Stock")
    _cover(conn, 311, "2024-05-01", rows, complete=False)
    for _, key, _, _ in rows:
        _assert_engines(conn, "SHARED", 311, key, "2024-05-03", False)


def test_gate_independent_crosscheck_covers_aliases_siblings_and_shared_labels(schema_dsn):
    conn, _ = schema_dsn
    expected = []
    tickers = ["CWENA"]
    for key in _gate_alias_fixture(conn, 401):
        expected.append(("CWENA", 401, key, "2026-05-10", False))
    competitor_checks = []
    for cik, (kind, same_cik) in enumerate(itertools.product(("preferred", "warrant"), (False, True)), 402):
        ticker, other, common_cik = _gate_non_equity_fixture(conn, cik, kind, same_cik)
        tickers.extend((ticker, other))
        expected.extend(((other, cik, "InstrumentA", "2024-04-01", False),
                         (ticker, cik, "InstrumentB", "2024-04-01", True),
                         (ticker, common_cik, "Common", "2024-04-01", True)))
        competitor_checks.append((ticker, common_cik, cik, "InstrumentB", "2024-04-01"))
    for cik, groups in ((406, 1), (407, 4)):
        for ticker, key, _, _ in _gate_tracking_fixture(conn, cik, groups, "8-A12B/A"):
            tickers.append(ticker)
            expected.append((ticker, cik, key, "2024-05-03", groups == 1))
    failures = _invariant_failures(conn, tickers)
    for ticker, cik, key, on, alive in expected:
        failures.extend(("wrong scoped state", engine, ticker, cik, key, on, state, alive)
                        for engine, state in _engines(conn, ticker, cik, key, on).items() if state != alive)
    for check in competitor_checks:
        if not _other_line_covers(conn, *check):
            failures.append(("missing competing line", *check))
    observed = conn.execute("SELECT count(*) FROM sec_observations_at('infinity',true) "
                            "WHERE ticker_key=ANY(%s)", (tickers,)).fetchone()[0]
    assert (len(tickers), observed, len(expected)) == (14, 29, 19)
    assert failures == []
    print(f"Gate cross-check: 7 scenarios, {observed} observations, {4 * observed} lifecycle engine "
          f"evaluations, {4 * len(expected)} expected-state evaluations, 4 competing-line checks, "
          f"{len(failures)} violations")
