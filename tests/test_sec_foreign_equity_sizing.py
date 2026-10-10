"""B2 phase 1 on a disposable, database-isolated PostgreSQL 18 installation.

The source schemas execute verbatim in a unique database, so public-qualified
helpers cannot see another test's rows or the production-equivalent measurement.
"""

from __future__ import annotations

import datetime as dt
import os
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = (
    "sec_ticker_cik_history_v1.sql",
    "sec_ticker_cik_history_v2.sql",
    "sec_ticker_cik_history_v3.sql",
    "sec_foreign_listing_evidence.sql",
    "sec_foreign_listing_evidence_v2.sql",
    "sec_foreign_equity_sizing_v1.sql",
)
A = "ClassOfStock=CommonClassA;"
B = "ClassOfStock=CommonClassB;"
ADS = "ClassOfStock=AmericanDepositaryShares;"
DAY = "2025-12-31"


@pytest.fixture(scope="module")
def sql_database():
    dsn = os.environ.get("SEC_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SEC_TEST_DATABASE_URL not set (disposable local PostgreSQL only)")
    psycopg = pytest.importorskip("psycopg")
    from psycopg import sql

    info = psycopg.conninfo.conninfo_to_dict(dsn)
    if info.get("host") not in {"127.0.0.1", "localhost", "::1"}:
        pytest.fail("SEC_TEST_DATABASE_URL must name a disposable loopback database")
    if info.get("port") == "65432" or info.get("user") == "mcp_ro":
        pytest.fail("Refusing a production connection")
    database_name = "b2_sizing_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        assert admin.execute("SHOW server_version_num").fetchone()[0].startswith("18")
        admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(database_name)))
        isolated_dsn = psycopg.conninfo.make_conninfo(dsn, dbname=database_name)
        try:
            with psycopg.connect(isolated_dsn, autocommit=True) as conn:
                for name in MIGRATIONS:
                    conn.execute((ROOT / "schemas" / name).read_text(encoding="utf-8"))
                yield conn
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database_name)))


@pytest.fixture
def db(sql_database):
    conn = sql_database
    conn.execute("BEGIN")
    conn.execute("SET LOCAL jit = off")
    try:
        yield conn
    finally:
        conn.execute("ROLLBACK")


def accession() -> str:
    return f"0000000001-25-{int(uuid4().hex[:6], 16) % 1_000_000:06d}"


def observe(db, *, ticker="TSM", cik=1, member="", classes=1, kind="equity",
            title="Common Shares", exchange="New York Stock Exchange", adsh=None,
            filed="2025-03-01", form="20-F", available=None):
    adsh = adsh or accession()
    db.execute(
        "INSERT INTO public.sec_ticker_cik_observations "
        "(fact_hash,adsh,cik,dimh,segments,class_key,ticker,ticker_raw,security_title,exchange,"
        "security_kind,filing_equity_classes,filing_complete,form,filed,available_on,"
        "loaded_on,source_package) VALUES (%s,%s,%s,'test',%s,%s,%s,%s,%s,%s,%s,%s,true,"
        "%s,%s,COALESCE(%s::date,%s::date+1),'2026-10-10','test')",
        (uuid4().hex, adsh, cik, member, member, ticker, ticker, title, exchange, kind,
         classes, form, filed, available, filed),
    )
    return adsh


def count(db, *, adsh, cik=1, member="", shares=1_000_000, stated="2024-12-31",
          filed="2025-03-01", form="20-F", available=None, retired=None,
          retired_reason=None):
    db.execute(
        "INSERT INTO public.sec_cover_share_counts "
        "(fact_hash,adsh,cik,dimh,segments,class_key,stated_on,ddate_rounded,shares,form,"
        "filed,available_on,retired_on,retired_reason,loaded_on,source_package) "
        "VALUES (%s,%s,%s,'test',%s,%s,%s,%s,%s,%s,%s,COALESCE(%s::date,%s::date+1),"
        "%s,%s,'2026-10-10','test')",
        (uuid4().hex, adsh, cik, member, member, stated, stated, shares, form, filed,
         available, filed, retired, retired_reason),
    )


def listing(db, *, ticker="TSM", cik=1, kind="listed_type", listed_type="ads",
            source="cover_12b", ratio=None, class_token=None, ordinary=True,
            filed="2020-01-01", effective=None, until=None, available=None,
            pending=False, conflict=False, adsh=None, retired=None,
            retired_reason=None, program=None):
    db.execute(
        "INSERT INTO public.sec_foreign_listing_evidence "
        "(fact_hash,cik,symbol,underlying_class,adsh,form,filed,source_url,source_sha256,"
        "source_kind,evidence_kind,listed_type,ordinary_candidate,ratio_numerator,"
        "ratio_denominator,effective_from,effective_to,effective_date_explicit,"
        "ratio_effectiveness_pending,ratio_effectiveness_pending_text,"
        "ratio_effectiveness_conditions,operative_date_conflict,operative_date_candidates,"
        "operative_date_conflict_text,ratio_change_program_key,evidence_text,evidence_location,parser_version,"
        "available_on,retired_on,retired_reason,loaded_on,source_package) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,'https://www.sec.gov/Archives/test',%s,%s,%s,%s,"
        "%s,%s,%s,COALESCE(%s::date,%s::date+1),%s,%s,%s,%s,%s,%s,%s,%s,%s,"
        "'Synthetic contract assertion','test','test-v1',COALESCE(%s::date,%s::date+1),"
        "%s,%s,'2026-10-10',%s)",
        (uuid4().hex, cik, ticker, class_token, adsh or accession(),
         "6-K" if source == "ratio_change_6k" else "20-F", filed, "a" * 64,
         source, kind, listed_type if kind == "listed_type" else None, ordinary,
         ratio[0] if ratio else None, ratio[1] if ratio else None, effective, filed,
         until, effective is not None, pending, "Pending depositary date" if pending else None,
         ["unknown_condition"] if pending else None, conflict,
         ["2025-06-01", "2025-06-02"] if conflict else None,
         "Conflicting operative dates" if conflict else None, program, available, filed,
         retired, retired_reason, uuid4().hex),
    )


def ads_contract(db, *, ticker="TSM", cik=1, ratio=(5, 1), class_token=None,
                 ordinary=True):
    listing(db, ticker=ticker, cik=cik, class_token=class_token, ordinary=ordinary)
    for source in ("f6", "item_12d"):
        listing(db, ticker=ticker, cik=cik, kind="ads_ratio", source=source,
                ratio=ratio, class_token=class_token, ordinary=ordinary)


def resolve(db, *, ticker="TSM", cik=1, members=None, day=DAY, max_age=400):
    result = db.execute(
        "SELECT * FROM public.sec_cover_ticker_size_basis_at(%s,%s,%s,%s,%s)",
        (ticker, cik, [""] if members is None else members, day, max_age),
    )
    names = [column.name for column in result.description]
    rows = result.fetchall()
    assert len(rows) == 1
    return dict(zip(names, rows[0], strict=True))


def refused(row, code):
    assert row["status"] != "resolved"
    assert row["ordinary_shares"] is None
    assert row["refusal"].startswith(code + ": ")


def test_no_evidence_and_null_inputs_return_one_row_without_identity_defaults(db):
    row = resolve(db)
    refused(row, "class_shares_unavailable")
    assert row["ratio_numerator"] is row["ratio_denominator"] is None
    assert row["evidence"]["count_found"] is False
    row = resolve(db, ticker=None, cik=None, members=[], day=None, max_age=None)
    refused(row, "class_shares_unavailable")


@pytest.mark.parametrize("raw,expected", [
    ("class_a", "class:a"), ("series_a", "series:a"),
    ("class_II", "class:2"), ("series_IV", "series:4"),
    ("class_XXXIX", "class:39"), ("Class A", "class:a"),
    ("series:VI", "series:6"), (None, None), ("ordinary", None),
])
def test_one_normalizer_keeps_class_series_and_roman_semantics(db, raw, expected):
    assert db.execute("SELECT public.sec_foreign_class_key(%s)", (raw,)).fetchone()[0] == expected


def test_tsm_mislabelled_ordinary_cover_total_refuses_even_with_five_to_one(db):
    adsh = observe(db, ticker="TSM", kind="equity", title="Common Shares")
    count(db, adsh=adsh)
    ads_contract(db)
    row = resolve(db)
    refused(row, "share_total_class_scope_unverified")
    assert row["listed_type"] == "ads" and row["basis"] == "sole_class_total"
    assert row["class_binding"] is row["canonical_underlying_class_id"] is None
    assert row["evidence"]["listing_ratio_numerator"] == 5
    # Stricter sizing does not alter the legacy W1 foreign policy.
    assert db.execute("SELECT status,shares,refusal FROM public.sec_cover_ticker_shares_at('TSM',1,%s)", (DAY,)).fetchone() == (
        "refused", None, "foreign_issuer_listing_unverified")


def test_explicit_ordinary_class_under_mislabelled_cover_keeps_five_to_one_arithmetic(db):
    adsh = observe(db, ticker="TSM", member=A, kind="equity", title="Class A Common Shares")
    count(db, adsh=adsh, member=A)
    ads_contract(db, class_token="class_a")
    row = resolve(db, members=[A])
    assert row["status"] == "resolved"
    assert row["ordinary_shares"] == Decimal(1_000_000)
    assert row["listed_type"] == "ads"
    assert row["class_binding"] == "explicit"
    assert row["canonical_underlying_class_id"] == "class:a"
    assert row["share_unit"] == "ordinary" and row["basis"] == "class"
    assert (row["ratio_numerator"], row["ratio_denominator"]) == (5, 1)
    assert (row["count_ratio_numerator"], row["count_ratio_denominator"]) == (5, 1)
    assert row["ordinary_shares"] * 100 * row["ratio_denominator"] / row["ratio_numerator"] == 20_000_000
    assert row["exchange_name"] == "New York Stock Exchange"


@pytest.mark.parametrize("ticker", ["ZIM", "QGEN"])
def test_ordinary_direct_worldwide_unbound_total_refuses(db, ticker):
    adsh = observe(db, ticker=ticker)
    count(db, adsh=adsh)
    listing(db, ticker=ticker, listed_type="ordinary_direct")
    row = resolve(db, ticker=ticker)
    refused(row, "share_total_class_scope_unverified")
    assert row["evidence"]["listing_ratio_numerator"] == 1
    assert row["evidence"]["listing_ratio_denominator"] == 1


@pytest.mark.parametrize("ticker", ["ZIM", "QGEN"])
def test_ordinary_direct_explicit_class_count_is_one_to_one(db, ticker):
    adsh = observe(db, ticker=ticker, member=A, title="Class A Common Shares")
    count(db, adsh=adsh, member=A)
    listing(db, ticker=ticker, listed_type="ordinary_direct", class_token="class_a")
    row = resolve(db, ticker=ticker, members=[A])
    assert row["status"] == "resolved" and row["class_binding"] == "explicit"
    assert row["ordinary_shares"] == 1_000_000
    assert (row["ratio_numerator"], row["ratio_denominator"]) == (1, 1)


@pytest.mark.parametrize("class_token", [None, "class_a"])
@pytest.mark.parametrize("listed_type", ["ordinary_direct", "ads"])
def test_dlo_incomplete_tagged_census_never_binds_an_undimensioned_ab_total(db, class_token, listed_type):
    # The accepted 20-F has only the listed A title in W1's tagged observations.
    # Untagged B supply is real: 151,420,944 A + 134,054,192 B = 285,475,136.
    # An EFM per-class requirement cannot make that incomplete census complete.
    cik, adsh = 1846832, "0000950170-25-058197"
    observe(db, ticker="DLO", cik=cik, adsh=adsh, classes=1,
            title="Class A common shares", filed="2025-04-25")
    count(db, cik=cik, adsh=adsh, shares=285_475_136, filed="2025-04-25")
    if listed_type == "ads":
        ads_contract(db, ticker="DLO", cik=cik, class_token=class_token)
    else:
        listing(db, ticker="DLO", cik=cik, listed_type=listed_type, class_token=class_token)
    row = resolve(db, ticker="DLO", cik=cik)
    refused(row, "share_total_class_scope_unverified")
    assert row["adsh"] == adsh and row["count_class_key"] == ""
    assert row["class_binding"] is row["canonical_underlying_class_id"] is None
    assert row["evidence"]["class_proof"] is False
    assert row["evidence"]["class_binding_valid"] is False
    assert row["evidence"]["filing_count_classes"] == 1
    assert row["evidence"]["count_labels"] == ["class:a"]
    assert db.execute("SELECT count(*) FROM public.sec_ticker_cik_observations "
                      "WHERE cik=%s AND adsh=%s", (cik, adsh)).fetchone() == (1,)


def test_null_ratio_class_needs_positive_proof_even_with_one_tagged_class(db):
    adsh = observe(db, member=A, title="Class A ordinary shares", classes=1)
    count(db, adsh=adsh, member=A)
    ads_contract(db)
    row = resolve(db, members=[A])
    refused(row, "foreign_listing_class_ambiguous")
    assert row["evidence"]["class_proof"] is True
    assert row["class_binding"] is None


def test_cnq_40f_explicit_ordinary_class_count_resolves_but_optional_total_refuses(db):
    adsh = observe(db, ticker="CNQ", member=A, title="Class A Common Shares", form="40-F")
    count(db, adsh=adsh, member=A, form="40-F")
    listing(db, ticker="CNQ", listed_type="ordinary_direct", class_token="class_a")
    row = resolve(db, ticker="CNQ", members=[A])
    assert row["status"] == "resolved" and row["class_binding"] == "explicit"
    assert row["canonical_underlying_class_id"] == "class:a"
    newer = observe(db, ticker="CNQ", title="Common Shares", form="40-F", filed="2025-09-01")
    count(db, adsh=newer, form="40-F", filed="2025-09-01", stated="2025-06-30")
    row = resolve(db, ticker="CNQ", members=[A, ""])
    refused(row, "share_total_class_scope_unverified")
    assert row["adsh"] == newer


def test_class_a_ads_with_unlisted_b_admits_a_count_and_refuses_ab_total(db):
    adsh = observe(db, member=A, title="Class A ordinary shares", classes=2)
    observe(db, ticker="UNLISTED", member=B, title="Class B ordinary shares", classes=2, adsh=adsh)
    count(db, adsh=adsh, member=A, shares=900_000)
    count(db, adsh=adsh, member=B, shares=100_000)
    ads_contract(db, class_token="class_a")
    row = resolve(db, members=[A])
    assert row["status"] == "resolved" and row["ordinary_shares"] == 900_000
    assert row["canonical_underlying_class_id"] == "class:a"
    newer = observe(db, member=A, title="Class A ordinary shares", classes=2, filed="2025-09-01")
    observe(db, ticker="UNLISTED", member=B, title="Class B ordinary shares", classes=2,
            adsh=newer, filed="2025-09-01")
    count(db, adsh=newer, shares=1_000_000, stated="2025-06-30", filed="2025-09-01")
    row = resolve(db, members=[A])
    refused(row, "share_total_class_scope_unverified")
    assert row["adsh"] == newer


def test_underlying_ordinary_member_can_bind_below_a_separate_ads_member(db):
    adsh = observe(db, member=ADS, kind="depositary", title="American Depositary Shares", classes=2)
    observe(db, ticker="UNLISTED", member=A, title="Class A ordinary shares", classes=2, adsh=adsh)
    count(db, adsh=adsh, member=A)
    count(db, adsh=adsh, member=ADS, shares=30_000)
    ads_contract(db, class_token="class_a")
    row = resolve(db, members=[ADS])
    assert row["status"] == "resolved" and row["ordinary_shares"] == 1_000_000
    assert row["count_class_key"] == A and row["share_unit"] == "ordinary"


def _dlo_ads_class_filing(db, *, b_count=True, total=False, total_only=False):
    cik, adsh = 1846832, "0000950170-25-058197"
    observe(db, ticker="DLO", cik=cik, member=ADS, kind="depositary", classes=2,
            title="Class A American Depositary Shares", adsh=adsh, filed="2025-04-25")
    if total_only:
        count(db, cik=cik, adsh=adsh, shares=285_475_136, filed="2025-04-25")
        return cik, adsh
    for member, token in ((A, "A"), (B, "B")):
        observe(db, ticker=f"UNLISTED{token}", cik=cik, member=member, classes=2,
                title=f"Class {token} ordinary shares", adsh=adsh, filed="2025-04-25")
    count(db, cik=cik, adsh=adsh, member=A, shares=151_420_944, filed="2025-04-25")
    if b_count:
        count(db, cik=cik, adsh=adsh, member=B, shares=134_054_192, filed="2025-04-25")
    if total:
        count(db, cik=cik, adsh=adsh, shares=285_475_136, filed="2025-04-25")
    return cik, adsh


@pytest.mark.parametrize("ratio_class,b_count,total", [
    (None, True, False), (None, False, False), (None, True, True),
    ("class_a", True, True),
])
def test_ads_listing_class_binds_null_ratio_class_and_selects_dlo_a_count(db, ratio_class, b_count, total):
    # The ordinary A and B members belong to unlisted lines, not the ADS line.
    # Null-class ratios from the same unkeyed program inherit elected listing A;
    # their candidate election must select A, never B or DLO's A+B total.
    cik, adsh = _dlo_ads_class_filing(db, b_count=b_count, total=total)
    listing(db, ticker="DLO", cik=cik, class_token="class_a")
    for source in ("f6", "item_12d"):
        listing(db, ticker="DLO", cik=cik, kind="ads_ratio", source=source,
                ratio=(5, 1), class_token=ratio_class)
    row = resolve(db, ticker="DLO", cik=cik, members=[ADS])
    assert row["status"] == "resolved" and row["refusal"] is None
    assert row["ordinary_shares"] == 151_420_944 and row["adsh"] == adsh
    assert row["basis"] == "class" and row["count_class_key"] == A
    assert row["class_binding"] == "explicit" and row["canonical_underlying_class_id"] == "class:a"
    assert (row["ratio_numerator"], row["ratio_denominator"]) == (5, 1)
    assert (row["count_ratio_numerator"], row["count_ratio_denominator"]) == (5, 1)
    assert row["evidence"]["listing_class"] == row["evidence"]["count_listing_class"] == "class_a"
    assert row["evidence"]["ratio_class"] == row["evidence"]["count_ratio_class"] == ratio_class
    assert row["evidence"]["class_binding_status"] == row["evidence"]["count_class_binding_status"] == "explicit"
    assert row["evidence"]["count_class_keys"] == [A]
    assert row["evidence"]["count_ratio_refusal"] is None
    assert row["program_key"] is row["evidence"]["count_program_key"] is None


def test_ads_listing_class_fallback_never_binds_dlo_unbound_total(db):
    cik, adsh = _dlo_ads_class_filing(db, total_only=True)
    listing(db, ticker="DLO", cik=cik, class_token="class_a")
    for source in ("f6", "item_12d"):
        listing(db, ticker="DLO", cik=cik, kind="ads_ratio", source=source, ratio=(5, 1))
    row = resolve(db, ticker="DLO", cik=cik, members=[ADS])
    refused(row, "share_total_class_scope_unverified")
    assert row["adsh"] == adsh and row["count_class_key"] == ""
    assert row["class_binding"] is row["canonical_underlying_class_id"] is None
    assert row["evidence"]["class_proof"] is False
    assert row["evidence"]["class_binding_valid"] is False


def test_ads_listing_class_fallback_preserves_conflicting_nonnull_ratio_class(db):
    cik, _ = _dlo_ads_class_filing(db, b_count=False)
    listing(db, ticker="DLO", cik=cik, class_token="class_a")
    for source in ("f6", "item_12d"):
        listing(db, ticker="DLO", cik=cik, kind="ads_ratio", source=source,
                ratio=(5, 1), class_token="class_b")
    row = resolve(db, ticker="DLO", cik=cik, members=[ADS])
    # The established listing/ratio ambiguity refusal precedes binding refusal.
    refused(row, "foreign_listing_ambiguous")
    assert row["evidence"]["listing_class"] == "class_a"
    assert row["evidence"]["ratio_class"] == "class_b"
    assert row["evidence"]["class_binding_status"] == "mismatch"
    assert row["evidence"]["class_binding_valid"] is False


@pytest.mark.parametrize("programs", [
    ("old_cusip:123456789",), ("old_cusip:123456789", "old_cusip:987654321"),
])
def test_null_ratio_class_never_falls_back_from_another_or_ambiguous_program(db, programs):
    cik, _ = _dlo_ads_class_filing(db, b_count=False)
    listing(db, ticker="DLO", cik=cik, class_token="class_a")
    for source in ("f6", "item_12d"):
        listing(db, ticker="DLO", cik=cik, kind="ads_ratio", source=source, ratio=(5, 1))
    for program in programs:
        listing(db, ticker="DLO", cik=cik, kind="ads_ratio", source="ratio_change_6k",
                ratio=(5, 1), filed="2023-01-01", effective="2023-06-01", program=program)
    row = resolve(db, ticker="DLO", cik=cik, members=[ADS])
    refused(row, "foreign_listing_class_ambiguous")
    assert row["listing_status"] == row["ratio_status"] == "resolved"
    assert row["program_key"] == (programs[0] if len(programs) == 1 else None)
    assert row["evidence"]["program_ambiguous"] is (len(programs) > 1)
    assert row["evidence"]["class_binding_status"] == "ambiguous"
    assert row["evidence"]["class_binding_valid"] is False
    # The data contract assigns keys only to ratio-change facts, never listings.
    assert db.execute("SELECT bool_and(ratio_change_program_key IS NULL) "
                      "FROM public.sec_foreign_listing_evidence "
                      "WHERE cik=%s AND evidence_kind='listed_type'", (cik,)).fetchone() == (True,)


def test_null_ratio_class_with_two_classes_is_ambiguous_even_when_ratios_match(db):
    adsh = observe(db, member=A, title="Class A ordinary shares", classes=2)
    observe(db, ticker="UNLISTED", member=B, title="Class B ordinary shares", classes=2, adsh=adsh)
    count(db, adsh=adsh, member=A)
    count(db, adsh=adsh, member=B, shares=200_000)
    ads_contract(db)
    row = resolve(db, members=[A])
    refused(row, "foreign_listing_class_ambiguous")
    assert row["listing_status"] == row["ratio_status"] == "resolved"


def test_known_class_mismatch_does_not_use_numeric_ratio_as_binding(db):
    adsh = observe(db, member=B, title="Class B ordinary shares", classes=2)
    count(db, adsh=adsh, member=B)
    ads_contract(db, class_token="class_a")
    row = resolve(db, members=[B])
    refused(row, "foreign_listing_class_mismatch")


def test_newest_contradictory_underlying_count_cannot_rescue_an_older_bound_one(db):
    old = observe(db, member=ADS, kind="depositary", classes=2)
    observe(db, ticker="UNLISTED", member=A, title="Class A ordinary shares", classes=2, adsh=old)
    count(db, adsh=old, member=A)
    newer = observe(db, member=ADS, kind="depositary", classes=2, filed="2025-09-01")
    observe(db, ticker="UNLISTED", member=B, title="Class B ordinary shares", classes=2,
            adsh=newer, filed="2025-09-01")
    count(db, adsh=newer, member=B, stated="2025-06-30", filed="2025-09-01")
    ads_contract(db, class_token="class_a")
    row = resolve(db, members=[ADS])
    refused(row, "foreign_listing_class_mismatch")
    assert row["adsh"] == newer


def test_same_member_conflicting_class_titles_never_elect_the_smallest_label(db):
    adsh = observe(db, member=A, title="Class A ordinary shares")
    observe(db, member=A, title="Class B ordinary shares", adsh=adsh)
    count(db, adsh=adsh, member=A)
    ads_contract(db, class_token="class_a")
    row = resolve(db, members=[A])
    refused(row, "foreign_listing_class_ambiguous")
    assert row["evidence"]["count_labels_ambiguous"] is True


def test_explicit_ads_member_count_never_becomes_an_ordinary_class_count(db):
    adsh = observe(db, member=ADS, kind="depositary", title="American Depositary Shares")
    count(db, adsh=adsh, member=ADS)
    ads_contract(db)
    row = resolve(db, members=[ADS])
    refused(row, "ordinary_class_shares_unavailable")
    assert row["share_unit"] == "ads"


def test_preferred_ads_is_not_admitted_as_an_ordinary_program(db):
    adsh = observe(db, member=A, title="Class A Ordinary Shares")
    count(db, adsh=adsh, member=A)
    ads_contract(db, class_token="class_a", ordinary=False)
    row = resolve(db, members=[A])
    refused(row, "depositary_ratio_unsourced")
    assert row["listing_status"] == "resolved" and row["ratio_status"] == "none"


@pytest.mark.parametrize("kind,title,unit", [
    ("preferred", "Class A preferred shares", "preferred"),
    ("equity", "Class A preferred shares", "preferred"),
    ("unknown", "Class A securities", "unknown"),
])
def test_a_label_is_not_evidence_of_ordinary_count_units(db, kind, title, unit):
    member = "ClassOfStock=ClassA;"
    adsh = observe(db, member=member, kind=kind, title=title)
    count(db, adsh=adsh, member=member)
    ads_contract(db, class_token="class_a")
    row = resolve(db, members=[member])
    refused(row, "share_count_unit_unverified")
    assert row["share_unit"] == unit


def test_sole_proof_is_from_selected_counts_own_filing_never_a_later_filing(db):
    old = observe(db, classes=2)
    count(db, adsh=old)
    observe(db, classes=1, filed="2025-10-01")
    ads_contract(db)
    row = resolve(db)
    refused(row, "share_total_class_scope_unverified")
    assert row["adsh"] == old


@pytest.mark.parametrize("form", ["20-F", "40-F", "6-K", "20-FR"])
def test_foreign_forms_cannot_prove_unbound_total_scope_from_tagging_obligations(db, form):
    adsh = observe(db, form=form)
    count(db, adsh=adsh, form=form)
    ads_contract(db)
    refused(resolve(db), "share_total_class_scope_unverified")


def test_two_share_count_classes_fail_total_proof_even_with_incorrect_census_one(db):
    adsh = observe(db, classes=1)
    count(db, adsh=adsh)
    count(db, adsh=adsh, member=B, shares=100_000, stated="2024-01-01")
    ads_contract(db)
    refused(resolve(db), "share_total_class_scope_unverified")


def test_future_public_count_or_ratio_does_not_change_the_earlier_cutoff(db):
    adsh = observe(db, member=A, title="Class A ordinary shares", available="2026-01-02")
    count(db, adsh=adsh, member=A, available="2026-01-02")
    ads_contract(db, class_token="class_a")
    refused(resolve(db, members=[A]), "class_shares_unavailable")
    row = resolve(db, members=[A], day="2026-01-02")
    assert row["status"] == "resolved"
    # Independent future-public ratio cannot manufacture a 2025 entitlement.
    listing(db, kind="ads_ratio", source="ratio_change_6k", ratio=(10, 1),
            class_token="class_a", filed="2025-12-30", available="2026-01-02", effective="2025-06-01")
    assert resolve(db, members=[A])["ratio_numerator"] is None
    assert db.execute("SELECT ratio_numerator FROM public.sec_foreign_listing_context_at(1,'TSM',%s,%s)", (DAY, DAY)).fetchone()[0] == 5


def test_count_economic_date_uses_cutoff_knowledge_after_an_ads_only_ratio_change(db):
    adsh = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=adsh, member=A)
    ads_contract(db, ratio=(25, 1), class_token="class_a")
    listing(db, kind="ads_ratio", source="ratio_change_6k", ratio=(5, 1),
            class_token="class_a", filed="2025-05-01", effective="2025-06-01")
    listing(db, kind="ads_ratio", source="f6", ratio=(5, 1),
            class_token="class_a", filed="2025-05-01", effective="2025-06-01")
    row = resolve(db, members=[A])
    assert row["status"] == "resolved" and row["ordinary_shares"] == 1_000_000
    assert (row["ratio_numerator"], row["ratio_denominator"]) == (5, 1)
    assert (row["count_ratio_numerator"], row["count_ratio_denominator"]) == (25, 1)
    # Workers returns the two facts; Light refuses their inequality in phase 1.
    assert row["count_ratio_numerator"] * row["ratio_denominator"] != row["ratio_numerator"] * row["count_ratio_denominator"]


def test_historical_ratio_can_be_learned_after_count_date_but_by_cutoff(db):
    adsh = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=adsh, member=A)
    listing(db, class_token="class_a")
    for source in ("f6", "item_12d"):
        listing(db, kind="ads_ratio", source=source, ratio=(5, 1),
                class_token="class_a", filed="2025-02-01", effective="2020-01-02")
    row = resolve(db, members=[A])
    assert row["status"] == "resolved"
    assert (row["count_ratio_numerator"], row["count_ratio_denominator"]) == (5, 1)
    assert db.execute("SELECT ratio_status FROM public.sec_foreign_listing_at(1,'TSM','2024-12-31')").fetchone()[0] == "none"


@pytest.mark.parametrize("control", ["pending", "conflict"])
def test_pending_or_conflicting_ratio_plans_stay_ambiguous(db, control):
    adsh = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=adsh, member=A)
    ads_contract(db, class_token="class_a")
    listing(db, kind="ads_ratio", source="ratio_change_6k", ratio=(10, 1),
            class_token="class_a", filed="2025-05-01", effective="2025-06-01",
            pending=control == "pending", conflict=control == "conflict")
    row = resolve(db, members=[A])
    refused(row, "foreign_listing_ambiguous")
    assert row["ratio_status"] == "ambiguous"


def test_identical_ratios_with_competing_programs_do_not_prove_one_program(db):
    adsh = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=adsh, member=A)
    ads_contract(db, class_token="class_a")
    for program in ("old_cusip:123456789", "old_cusip:987654321"):
        listing(db, kind="ads_ratio", source="ratio_change_6k", ratio=(5, 1),
                class_token="class_a", filed="2025-05-01", effective="2025-06-01", program=program)
    row = resolve(db, members=[A])
    refused(row, "foreign_listing_class_ambiguous")
    assert row["evidence"]["program_ambiguous"] is True
    assert row["evidence"]["class_binding_valid"] is False
    assert row["listing_status"] == row["ratio_status"] == "resolved"


def test_historical_competing_programs_null_count_ratio_for_light_equality_gate(db):
    adsh = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=adsh, member=A)
    ads_contract(db, class_token="class_a")
    for program in ("old_cusip:123456789", "old_cusip:987654321"):
        listing(db, kind="ads_ratio", source="ratio_change_6k", ratio=(5, 1),
                class_token="class_a", filed="2020-06-01", effective="2020-07-01",
                until="2025-06-01", program=program)
    listing(db, kind="ads_ratio", source="ratio_change_6k", ratio=(5, 1),
            class_token="class_a", filed="2025-05-01", effective="2025-06-01",
            program="old_cusip:123456789")
    row = resolve(db, members=[A])
    assert row["status"] == "resolved"
    assert (row["ratio_numerator"], row["ratio_denominator"]) == (5, 1)
    assert row["count_ratio_numerator"] is row["count_ratio_denominator"] is None
    assert row["evidence"]["count_program_ambiguous"] is True
    assert row["evidence"]["count_ratio_numerator"] == 5


@pytest.mark.parametrize("historical_class,binding", [
    (None, "ambiguous"), ("class_b", "mismatch"), ("class_a", "explicit"),
])
def test_historical_ratio_binds_selected_a_count_before_numeric_equality(db, historical_class, binding):
    # Both numeric ratios are 5/1. The historical program must still describe
    # the selected A ordinary count; a NULL or B class cannot pass by equality.
    adsh = observe(db, member=A, title="Class A ordinary shares", classes=2)
    observe(db, ticker="UNLISTED", member=B, title="Class B ordinary shares", classes=2, adsh=adsh)
    count(db, adsh=adsh, member=A, shares=1_000_000)
    count(db, adsh=adsh, member=B, shares=200_000)
    listing(db, class_token=historical_class, until="2025-06-01")
    for source in ("f6", "item_12d"):
        listing(db, kind="ads_ratio", source=source, ratio=(5, 1),
                class_token=historical_class, until="2025-06-01")
    # A 12(b) cover takes effect on filing+1; explicit future effective dates
    # belong to ratio evidence, rather than to the cover's listing assertion.
    listing(db, class_token="class_a", filed="2025-05-31")
    for source in ("f6", "item_12d"):
        listing(db, kind="ads_ratio", source=source, ratio=(5, 1),
                class_token="class_a", filed="2025-05-01", effective="2025-06-01")
    row = resolve(db, members=[A])
    assert row["status"] == "resolved" and row["ordinary_shares"] == 1_000_000
    assert (row["ratio_numerator"], row["ratio_denominator"]) == (5, 1)
    assert row["evidence"]["count_class_binding_status"] == binding
    assert row["evidence"]["count_class_binding_valid"] is (binding == "explicit")
    assert row["evidence"]["count_ratio_status"] == "resolved"
    assert row["evidence"]["count_listing_contract_status"] == "resolved"
    assert row["evidence"]["count_ratio_class"] == historical_class
    assert row["evidence"]["count_listing_class"] == historical_class
    assert (row["evidence"]["count_ratio_numerator"], row["evidence"]["count_ratio_denominator"]) == (5, 1)
    expected = (5, 1) if binding == "explicit" else (None, None)
    assert (row["count_ratio_numerator"], row["count_ratio_denominator"]) == expected


def test_exchange_comes_from_latest_line_observation_at_cutoff_not_old_count(db):
    adsh = observe(db, member=A, title="Class A ordinary shares", exchange="New York Stock Exchange")
    count(db, adsh=adsh, member=A)
    ads_contract(db, class_token="class_a")
    newer = observe(db, member=A, title="Class A ordinary shares", filed="2025-09-01", exchange="OTC Markets")
    row = resolve(db, members=[A])
    assert row["status"] == "resolved"
    assert row["adsh"] == adsh and row["exchange_name"] == "OTC Markets"
    assert row["evidence"]["exchange_adsh"] == newer
    assert row["evidence"]["exchange_available_on"] == "2025-09-02"


def test_competing_latest_line_exchanges_return_no_currency_proxy(db):
    adsh = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=adsh, member=A)
    ads_contract(db, class_token="class_a")
    newer = observe(db, member=A, title="Class A ordinary shares", filed="2025-09-01", exchange="New York Stock Exchange")
    observe(db, member=A, title="Class A ordinary shares", filed="2025-09-01", exchange="OTC Markets", adsh=newer)
    row = resolve(db, members=[A])
    assert row["status"] == "resolved" and row["exchange_name"] is None
    assert row["evidence"]["exchange_ambiguous"] is True


def test_parser_correction_old_count_is_invisible_at_every_date(db):
    adsh = observe(db)
    count(db, adsh=adsh, retired="2026-10-10", retired_reason="parser_correction")
    ads_contract(db)
    refused(resolve(db), "class_shares_unavailable")


def test_latest_stated_date_then_filing_conflict_does_not_fall_back(db):
    old = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=old, member=A, shares=100)
    newer = observe(db, member=A, title="Class A ordinary shares", filed="2025-04-01")
    count(db, adsh=newer, member=A, shares=200, filed="2025-04-01")
    count(db, adsh=newer, member=A, shares=201, filed="2025-04-01")
    ads_contract(db, class_token="class_a")
    row = resolve(db, members=[A])
    refused(row, "ambiguous")
    assert row["adsh"] == newer


@pytest.mark.parametrize("age,expected", [(400, "resolved"), (401, "stale")])
def test_count_age_boundary_is_inclusive(db, age, expected):
    adsh = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=adsh, member=A, stated=(dt.date.fromisoformat(DAY) - dt.timedelta(days=age)).isoformat())
    ads_contract(db, class_token="class_a")
    row = resolve(db, members=[A])
    assert row["status"] == expected
    if expected == "stale":
        refused(row, "stale")


@pytest.mark.parametrize("shares", [Decimal(0), Decimal("NaN"), Decimal("Infinity")])
def test_zero_or_nonfinite_ordinary_count_refuses(db, shares):
    adsh = observe(db)
    count(db, adsh=adsh, shares=shares)
    ads_contract(db)
    refused(resolve(db), "nonpositive_share_count")


def test_negative_count_and_invalid_ratio_are_rejected_by_evidence_constraints(db):
    psycopg = pytest.importorskip("psycopg")
    adsh = observe(db)
    with pytest.raises(psycopg.errors.CheckViolation), db.transaction():
        count(db, adsh=adsh, shares=-1)
    for ratio in ((0, 1), (-1, 1), (1, 0), (Decimal("NaN"), 1), (Decimal("Infinity"), 1)):
        with pytest.raises(psycopg.errors.CheckViolation), db.transaction():
            listing(db, kind="ads_ratio", source="f6", ratio=ratio)


def test_fractional_ratio_and_huge_integer_count_remain_exact_numeric(db):
    huge = Decimal("10000000000000000000000000000000000000003")
    adsh = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=adsh, member=A, shares=huge)
    ads_contract(db, ratio=(3, 2), class_token="class_a")
    row = resolve(db, members=[A])
    assert row["status"] == "resolved" and row["ordinary_shares"] == huge
    assert (row["ratio_numerator"], row["ratio_denominator"]) == (3, 2)
    value = db.execute("SELECT ordinary_shares * 3 * ratio_denominator / ratio_numerator "
                       "FROM public.sec_cover_ticker_size_basis_at('TSM',1,%s,%s)", ([A], DAY)).fetchone()[0]
    assert value == Decimal(int(huge) * 2)


def test_new_point_functions_have_no_set_and_inline_without_sec_function_scans(db):
    adsh = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=adsh, member=A)
    ads_contract(db, class_token="class_a")
    assert db.execute("SHOW jit").fetchone()[0] == "off"
    plan = db.execute("EXPLAIN SELECT b.* FROM (VALUES ('TSM'::text,1::bigint)) r(t,c) "
                      "CROSS JOIN LATERAL public.sec_cover_ticker_size_basis_at(r.t,r.c,%s,%s) b", ([A], DAY)).fetchall()
    assert not any("Function Scan on sec_" in line[0] for line in plan)
    rows = db.execute("SELECT p.proconfig,p.prosecdef,p.provolatile,p.proparallel "
                      "FROM pg_catalog.pg_proc p WHERE p.oid IN ("
                      "'public.sec_cover_ticker_size_basis_at(text,bigint,text[],date,integer)'::regprocedure,"
                      "'public.sec_cover_share_election_at(bigint,text,text,text[],date,text,text)'::regprocedure,"
                      "'public.sec_cover_sizing_share_detail_at(bigint,text,text[],date,text)'::regprocedure,"
                      "'public.sec_foreign_listing_context_at(bigint,text,date,date)'::regprocedure)").fetchall()
    assert len(rows) == 4 and all(row == (None, False, "s", "s") for row in rows)


@pytest.mark.parametrize("api", ("class", "ticker"))
def test_legacy_share_plans_prune_sizing_labels_and_filing_proof(db, api):
    # The all-equity W1 callers need election and ticker binding only. Running
    # the foreign-size label/census work on every old candidate was an order of
    # magnitude regression; check the work in the plan, not wall-clock timing.
    import json

    adsh = observe(db, ticker="DOM", member=A, title="Class A ordinary shares", form="10-K")
    count(db, adsh=adsh, member=A, form="10-K")
    if api == "class":
        query = "SELECT * FROM public.sec_cover_class_shares_at(1,%s,%s)"
        params = (A, DAY)
    else:
        query = "SELECT * FROM public.sec_cover_ticker_shares_at('DOM',1,%s)"
        params = (DAY,)
    plan = db.execute("EXPLAIN (VERBOSE, FORMAT JSON) " + query, params).fetchone()[0]
    rendered = json.dumps(plan)
    assert "sec_first_label(" not in rendered
    assert "sec_class_label(" not in rendered
    assert "jsonb_build_object(" not in rendered

    def proof_expressions(node):
        for expression in node.get("Output", []):
            yield expression
        for key in ("Filter", "Join Filter", "One-Time Filter"):
            if key in node:
                yield node[key]
        for child in node.get("Plans", []):
            yield from proof_expressions(child)

    assert not any("bool_and(" in expression and "filing_complete" in expression
                   for expression in proof_expressions(plan[0]["Plan"]))
    assert db.execute(query, params).fetchone()[0] == "resolved"
    if api == "class":
        def relations(node):
            result = {node["Relation Name"]} if "Relation Name" in node else set()
            for child in node.get("Plans", []):
                result.update(relations(child))
            return result

        assert "sec_ticker_cik_observations" not in relations(plan[0]["Plan"])


def test_legacy_count_apis_match_pristine_v3_across_ambiguity_age_and_foreign_gates(db):
    # Install the frozen v3 bodies under reference names; new migration bytes
    # stay unchanged and reference functions see exactly the same test rows.
    import re

    v3 = (ROOT / "schemas" / "sec_ticker_cik_history_v3.sql").read_text(encoding="utf-8")
    for name in ("sec_cover_class_shares_at", "sec_cover_ticker_shares_at"):
        body = re.search(r"CREATE OR REPLACE FUNCTION " + name + r"\(.*?\$fn\$;", v3, re.S).group(0)
        db.execute(body.replace(name, "b2_reference_" + name, 1))
    first = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=first, member=A, shares=100)
    count(db, adsh=first, member=A, shares=101)
    total = observe(db, ticker="ZIM", cik=2)
    count(db, adsh=total, cik=2, shares=200)
    wrapper = observe(db, ticker="ADS", cik=3, member=ADS, kind="depositary")
    count(db, adsh=wrapper, cik=3, member=ADS, shares=300)
    for day in ("2020-01-01", "2025-03-02", DAY, "2026-12-31"):
        for ticker, cik, member in (("TSM", 1, A), ("ZIM", 2, ""), ("ADS", 3, ADS), ("MISS", 99, "")):
            actual = db.execute("SELECT * FROM public.sec_cover_class_shares_at(%s,%s,%s)", (cik, member, day)).fetchone()
            baseline = db.execute("SELECT * FROM public.b2_reference_sec_cover_class_shares_at(%s,%s,%s)", (cik, member, day)).fetchone()
            assert actual == baseline
            actual = db.execute("SELECT * FROM public.sec_cover_ticker_shares_at(%s,%s,%s)", (ticker, cik, day)).fetchone()
            baseline = db.execute("SELECT * FROM public.b2_reference_sec_cover_ticker_shares_at(%s,%s,%s)", (ticker, cik, day)).fetchone()
            assert actual == baseline


def test_class_count_ambiguity_includes_all_values_in_elected_filing(db):
    # Class W1 never partitions a filing's competing numeric values by its
    # foreign-policy annotation. Mixed versions must stay ambiguous even when
    # the elected annotation is domestic and another value was marked foreign.
    adsh = observe(db, ticker="DOM", form="10-K")
    count(db, adsh=adsh, shares=100, form="20-F", available="2025-03-02")
    count(db, adsh=adsh, shares=101, form="10-K", available="2025-03-03")
    row = db.execute("SELECT * FROM public.sec_cover_class_shares_at(1,'',%s)", (DAY,)).fetchone()
    assert row[0] == "ambiguous" and row[1] is None and row[4] is None


@pytest.mark.parametrize("shares", ("0", "NaN", "Infinity", "10000000000000000000000000000000000000003"))
def test_legacy_election_retains_numeric_edge_values_exactly(db, shares):
    import re

    v3 = (ROOT / "schemas" / "sec_ticker_cik_history_v3.sql").read_text(encoding="utf-8")
    for name in ("sec_cover_class_shares_at", "sec_cover_ticker_shares_at"):
        body = re.search(r"CREATE OR REPLACE FUNCTION " + name + r"\(.*?\$fn\$;", v3, re.S).group(0)
        db.execute(body.replace(name, "b2_reference_" + name, 1))
    adsh = observe(db, ticker="DOM", member=A, form="10-K")
    count(db, adsh=adsh, member=A, shares=Decimal(shares), form="10-K")
    # PostgreSQL numeric NaN compares equal to itself; Python Decimal NaN does
    # not. Compare every output column using PostgreSQL's null-safe row rules.
    assert db.execute(
        "SELECT n IS NOT DISTINCT FROM o "
        "FROM public.sec_cover_class_shares_at(1,%s,%s) n "
        "CROSS JOIN public.b2_reference_sec_cover_class_shares_at(1,%s,%s) o",
        (A, DAY, A, DAY),
    ).fetchone() == (True,)
    assert db.execute(
        "SELECT n IS NOT DISTINCT FROM o "
        "FROM public.sec_cover_ticker_shares_at('DOM',1,%s) n "
        "CROSS JOIN public.b2_reference_sec_cover_ticker_shares_at('DOM',1,%s) o",
        (DAY, DAY),
    ).fetchone() == (True,)


@pytest.mark.parametrize("control", ["pending", "conflict", "listing_ambiguous", "resolved"])
def test_count_date_ratio_requires_resolved_historical_listing_and_ratio(db, control):
    # GitHub thread 4237461490: a same-number later entitlement cannot make
    # the count-date contract resolved. On round 2 the core already withheld
    # these numbers; the explicit sizing guards and named audit diagnostic
    # make that requirement independently visible to the Light gate.
    adsh = observe(db, member=A, title="Class A ordinary shares")
    count(db, adsh=adsh, member=A)
    ads_contract(db, ratio=(5, 1), class_token="class_a")
    if control in {"pending", "conflict"}:
        listing(db, kind="ads_ratio", source="ratio_change_6k", ratio=(5, 1),
                class_token="class_a", filed="2024-05-01", effective="2024-06-01",
                pending=control == "pending")
        if control == "conflict":
            # The earliest conflicted date must equal effective_from. The
            # common listing helper's fixed 2025 candidates are not valid for
            # this older control, so populate the legally consistent dates.
            db.execute(
                "UPDATE public.sec_foreign_listing_evidence "
                "SET operative_date_conflict=true, "
                "operative_date_candidates=ARRAY['2024-06-01'::date,'2024-06-02'::date], "
                "operative_date_conflict_text='Conflicting operative dates' "
                "WHERE source_kind='ratio_change_6k' AND filed='2024-05-01'"
            )
        listing(db, kind="ads_ratio", source="ratio_change_6k", ratio=(5, 1),
                class_token="class_a", filed="2025-05-01", effective="2025-06-01")
        db.execute(
            "UPDATE public.sec_foreign_listing_evidence "
            "SET ratio_effectiveness_confirmed=true, "
            "ratio_effectiveness_confirmation_text='Event completed', "
            "ratio_effectiveness_confirmed_conditions=ARRAY['ratio_effective'] "
            "WHERE source_kind='ratio_change_6k' AND filed='2025-05-01'"
        )
        listing(db, kind="ads_ratio", source="f6", ratio=(5, 1),
                class_token="class_a", filed="2025-05-01", effective="2025-06-01")
    elif control == "listing_ambiguous":
        # Same class, conflicting old listed types: the historical ratio is
        # resolved but the historical listing is not. A later cover resolves D.
        listing(db, listed_type="ordinary_direct", class_token="class_a")
        listing(db, class_token="class_a", filed="2025-05-31")

    row = resolve(db, members=[A])
    audit = row["evidence"]
    assert row["status"] == "resolved" and row["refusal"] is None
    assert row["ordinary_shares"] == 1_000_000
    assert (row["ratio_numerator"], row["ratio_denominator"]) == (5, 1)
    assert row["listing_status"] == row["ratio_status"] == "resolved"
    assert audit["count_class_binding_status"] == "explicit"
    assert audit["count_class_binding_valid"] is True
    assert audit["count_program_ambiguous"] is False
    assert audit["count_listing_status"] == ("ambiguous" if control == "listing_ambiguous" else "resolved")
    assert audit["count_ratio_status"] == ("ambiguous" if control in {"pending", "conflict"} else "resolved")
    expected_ratio = (5, 1) if control == "resolved" else (None, None)
    assert (row["count_ratio_numerator"], row["count_ratio_denominator"]) == expected_ratio
    if control == "resolved":
        assert audit["count_ratio_refusal"] is None
    else:
        assert audit["count_ratio_refusal"].startswith("foreign_listing_ambiguous: TSM ")
        # The core's returned historical numerator/denominator are already
        # NULL. Source-level numerical facts must survive for audit only.
        assert audit["count_ratio_numerator"] is audit["count_ratio_denominator"] is None
    facts = audit["count_ratio_evidence_facts"]
    assert isinstance(facts, list) and facts
    assert all(fact["id"] in audit["count_listing_evidence_ids"] for fact in facts)
    assert any(
        fact["evidence_kind"] == "ads_ratio"
        and fact["source_kind"] == "f6"
        and fact["underlying_class"] == "class_a"
        and fact["ratio_numerator"] == 5
        and fact["ratio_denominator"] == 1
        for fact in facts
    )


@pytest.mark.parametrize("programs", [
    ("old_cusip:123456789",), ("old_cusip:123456789", "old_cusip:987654321"),
])
def test_historical_null_ratio_class_cannot_borrow_listing_from_keyed_program(db, programs):
    # D's unkeyed NULL-class ratio may borrow its elected listing A. At S the
    # equal 5/1 ratio belongs to keyed programme facts, so that same fallback
    # is unavailable even when the historical listing and ratio both resolve.
    adsh = observe(db, member=ADS, kind="depositary", classes=2,
                   title="Class A American Depositary Shares")
    for member, token in ((A, "A"), (B, "B")):
        observe(db, ticker=f"UNLISTED{token}", member=member, classes=2,
                title=f"Class {token} ordinary shares", adsh=adsh)
    count(db, adsh=adsh, member=A, shares=1_000_000)
    count(db, adsh=adsh, member=B, shares=200_000)
    listing(db, class_token="class_a", until="2025-06-01")
    for source in ("f6", "item_12d"):
        listing(db, kind="ads_ratio", source=source, ratio=(5, 1),
                effective="2020-01-02", until="2025-06-01")
    for program in programs:
        listing(db, kind="ads_ratio", source="ratio_change_6k", ratio=(5, 1),
                filed="2020-06-01", effective="2020-07-01",
                until="2025-06-01", program=program)
    listing(db, class_token="class_a", filed="2025-05-31")
    for source in ("f6", "item_12d"):
        listing(db, kind="ads_ratio", source=source, ratio=(5, 1),
                filed="2025-05-01", effective="2025-06-01")

    row = resolve(db, members=[ADS])
    audit = row["evidence"]
    assert row["status"] == "resolved" and row["refusal"] is None
    assert row["ordinary_shares"] == 1_000_000 and row["count_class_key"] == A
    assert row["class_binding"] == "explicit" and row["program_key"] is None
    assert (row["ratio_numerator"], row["ratio_denominator"]) == (5, 1)
    assert row["listing_status"] == row["ratio_status"] == "resolved"
    assert audit["count_listing_status"] == audit["count_ratio_status"] == "resolved"
    assert audit["count_listing_class"] == "class_a" and audit["count_ratio_class"] is None
    assert audit["count_program_key"] == (programs[0] if len(programs) == 1 else None)
    assert audit["count_program_ambiguous"] is (len(programs) > 1)
    assert audit["count_class_binding_status"] == "ambiguous"
    assert audit["count_class_binding_valid"] is False
    assert (audit["count_ratio_numerator"], audit["count_ratio_denominator"]) == (5, 1)
    assert row["count_ratio_numerator"] is row["count_ratio_denominator"] is None
    assert audit["count_ratio_refusal"].startswith("foreign_listing_class_ambiguous: TSM ")
    assert {
        fact["program_key"] for fact in audit["count_ratio_evidence_facts"]
        if fact["source_kind"] == "ratio_change_6k"
    } == set(programs)
