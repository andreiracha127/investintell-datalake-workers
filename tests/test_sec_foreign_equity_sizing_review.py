"""Round 7 exact review reproductions and their narrow valid controls.

The preserved-export exposure precedes these changes. These cases supplement
the frozen real configurations; they do not replace the real coverage floor.
"""
from __future__ import annotations

import pytest

from test_sec_foreign_equity_sizing import (
    A, B, ADS, _round4_set_observation_context, ads_contract, count,
    db as db, listing, observe, refused, resolve, sql_database as sql_database,
)

SERIES_A = "ClassOfStock=SeriesACommonShares;"
TRACKING_A = "ClassOfStock=TrackingSeriesACommonShares;"


@pytest.mark.parametrize("second_count,title", [
    (True, "Series A ordinary shares"),
    (False, "Series A ordinary shares"),
    (False, "Series A shares"),
    (False, "Series A tracking stock"),
], ids=["older-competing-count", "observation-only-peer", "plain-stock-caption", "tracking-stock-caption"])
def test_review_4237326705_same_label_on_two_stock_classes_is_not_explicit(db, second_count, title):
    """Only ONE has the latest count; TWO is still a separate Series A line."""
    adsh = observe(db, ticker="ONE", member=SERIES_A, title="Series A ordinary shares", classes=2)
    observe(db, ticker="TWO", member=TRACKING_A, title=title, classes=2, adsh=adsh)
    count(db, adsh=adsh, member=SERIES_A, shares=100_000)
    if second_count:
        count(db, adsh=adsh, member=TRACKING_A, shares=200_000, stated="2024-06-30")
    listing(db, ticker="ONE", listed_type="ordinary_direct", class_token="series_a")
    row = resolve(db, ticker="ONE", members=[SERIES_A])
    refused(row, "foreign_listing_class_ambiguous")
    assert row["class_binding"] is None
    assert row["evidence"]["count_labels_ambiguous"] is True


@pytest.mark.parametrize("control", ["other-label", "earlier-alias", "same-raw-context", "same-raw-ticker-alias", "registration-count-alias"])
def test_review_4237326705_preserves_one_canonical_stock_class(db, control):
    """Separate labels and aliases must not repeat the Round 5 over-tightening."""
    adsh = observe(db, ticker="ONE", member=SERIES_A, title="Series A ordinary shares")
    count(db, adsh=adsh, member=SERIES_A, shares=100_000)
    if control == "other-label":
        observe(db, ticker="TWO", member="ClassOfStock=SeriesBCommonShares;", title="Series B ordinary shares", adsh=adsh)
    elif control == "earlier-alias":
        observe(db, ticker="ONE", member=TRACKING_A, title="Series A ordinary shares", filed="2020-03-01")
    elif control == "same-raw-context":
        title = "Series A ordinary shares, par value $0.01 per share"
        observe(db, ticker="ONE", member=SERIES_A, title=title, adsh=adsh)
        _round4_set_observation_context(db, adsh=adsh, member=SERIES_A, title=title, dimh="other-context")
    elif control == "same-raw-ticker-alias":
        observe(db, ticker="ALIAS", member=SERIES_A, title="Series A ordinary shares", adsh=adsh)
    else:
        # Real UK-like structure: one registered stock member, a differently
        # tagged count member, and no second registered stock class.
        db.execute("DELETE FROM public.sec_ticker_cik_observations WHERE adsh=%s", (adsh,))
        observe(db, ticker="ONE", member=TRACKING_A, title="Series A ordinary shares", adsh=adsh)
    listing(db, ticker="ONE", listed_type="ordinary_direct", class_token="series_a")
    row = resolve(db, ticker="ONE", members=[SERIES_A, TRACKING_A])
    assert row["status"] == "resolved" and row["ordinary_shares"] == 100_000
    assert row["canonical_underlying_class_id"] == "series:a"
    assert row["class_binding"] == "explicit"


def test_review_4237326705_ads_wrapper_is_not_a_second_ordinary_class(db):
    adsh = observe(db, member=ADS, kind="depositary", title="Series A American Depositary Shares")
    observe(db, ticker="UNDERLYING", member=SERIES_A, title="Series A ordinary shares", adsh=adsh)
    count(db, adsh=adsh, member=SERIES_A, shares=100_000)
    ads_contract(db, class_token="series_a")
    row = resolve(db, members=[ADS])
    assert row["status"] == "resolved" and row["ordinary_shares"] == 100_000
    assert row["class_binding"] == "explicit"


@pytest.mark.parametrize("reset_complete", [False, True], ids=["incomplete-cannot-hide-peer", "later-complete-resets-cohort"])
def test_review_4237326705_issuer_cohort_survives_an_incomplete_count_filing(db, reset_complete):
    """W1's source-date cohort preserves peers until a complete cover replaces it."""
    earlier = observe(db, ticker="ONE", member=SERIES_A, title="Series A ordinary shares", filed="2024-02-01", classes=2)
    observe(db, ticker="TWO", member=TRACKING_A, title="Series A ordinary shares", filed="2024-02-01", classes=2, adsh=earlier)
    adsh = observe(db, ticker="ONE", member=SERIES_A, title="Series A ordinary shares", form="20-F" if reset_complete else "6-K")
    db.execute("UPDATE public.sec_ticker_cik_observations SET filing_complete=%s WHERE adsh=%s", (reset_complete, adsh))
    count(db, adsh=adsh, member=SERIES_A, shares=100_000, form="20-F" if reset_complete else "6-K")
    listing(db, ticker="ONE", listed_type="ordinary_direct", class_token="series_a")
    row = resolve(db, ticker="ONE", members=[SERIES_A])
    if reset_complete:
        assert row["status"] == "resolved" and row["ordinary_shares"] == 100_000
    else:
        refused(row, "foreign_listing_class_ambiguous")


def test_review_4237326705_dated_raw_aliases_share_w1_canonical_line(db):
    """Two raw keys in the cohort are one line when W1 proves a dated alias."""
    observe(db, ticker="ONE", member=SERIES_A, title="Series A ordinary shares", filed="2024-02-01")
    adsh = observe(db, ticker="ONE", member=TRACKING_A, title="Series A ordinary shares", form="6-K")
    db.execute("UPDATE public.sec_ticker_cik_observations SET filing_complete=false WHERE adsh=%s", (adsh,))
    count(db, adsh=adsh, member=TRACKING_A, shares=100_000, form="6-K")
    listing(db, ticker="ONE", listed_type="ordinary_direct", class_token="series_a")
    row = resolve(db, ticker="ONE", members=[TRACKING_A])
    assert row["status"] == "resolved" and row["ordinary_shares"] == 100_000


def test_review_4237326711_ticker_changes_class_without_changing_ratio(db):
    """Already fixed on f2da13ba: raw historical 5/1 cannot pass Light equality."""
    observe(db, member=B, title="Class B ordinary shares", filed="2020-03-01", classes=2)
    adsh = observe(db, member=A, title="Class A ordinary shares", classes=2)
    observe(db, ticker="UNLISTED", member=B, title="Class B ordinary shares", adsh=adsh, classes=2)
    count(db, adsh=adsh, member=A, shares=1_000_000)
    count(db, adsh=adsh, member=B, shares=200_000)
    listing(db, class_token="class_b", until="2025-06-01")
    for source in ("f6", "item_12d"):
        listing(db, kind="ads_ratio", source=source, ratio=(5, 1), class_token="class_b", until="2025-06-01")
    listing(db, class_token="class_a", filed="2025-05-31")
    for source in ("f6", "item_12d"):
        listing(db, kind="ads_ratio", source=source, ratio=(5, 1), class_token="class_a", filed="2025-05-01", effective="2025-06-01")
    row = resolve(db, members=[A])
    assert row["status"] == "resolved"
    assert (row["ratio_numerator"], row["ratio_denominator"]) == (5, 1)
    assert row["count_ratio_numerator"] is row["count_ratio_denominator"] is None
    audit = row["evidence"]
    assert audit["count_ratio_class"] == audit["count_listing_class"] == "class_b"
    assert (audit["count_ratio_numerator"], audit["count_ratio_denominator"]) == (5, 1)
    assert audit["count_class_binding_status"] == "mismatch"
    assert audit["count_ratio_refusal"].startswith("foreign_listing_class_mismatch: ")


@pytest.mark.parametrize("title", [
    "Class A common shares, par value of $0.01 per share",
    "Class A Common Stock, without par value",
    "Class A ordinary shares, no-par value",
    "Class A common stock, nominal value of EUR 0.01 per share",
    "Class A common stock, par value USD 0.01 per share",
    "Class A ordinary shares, par value HK$0.01 per share",
    "Class A ordinary shares, par value of US$.01 per share",
    "Class A common stock, $0.01 par value per share",
    "Class A common stock, GBP 0.01 nominal value",
])
def test_review_4238321735_accepts_closed_standard_par_metadata(db, title):
    member = "ClassOfStock=ClassACommonShares;"
    adsh = observe(db, member=member, title=title)
    count(db, adsh=adsh, member=member, shares=100_000)
    ads_contract(db, class_token="class_a")
    row = resolve(db, members=[member])
    assert row["status"] == "resolved" and row["ordinary_shares"] == 100_000
    assert row["canonical_underlying_class_id"] == "class:a"
    assert row["class_binding"] == "explicit"


@pytest.mark.parametrize("title", [
    "Class A common shares, par value of $0.01 per share and Class B shares",
    "Class A common shares, without par value; Series A ordinary shares",
    "Class A common shares, par value Class B",
    "Class A common stock, nominal value USD 0.01 and Series B",
    "Class A common stock, without par value or Class B",
    "Class A common stock, par value of $0.01 per share, Class B common stock",
])
def test_review_4238321735_metadata_cannot_hide_another_identity(db, title):
    member = "ClassOfStock=ClassACommonShares;"
    adsh = observe(db, member=member, title=title)
    count(db, adsh=adsh, member=member, shares=100_000)
    ads_contract(db, class_token="class_a")
    refused(resolve(db, members=[member]), "foreign_listing_class_ambiguous")


@pytest.mark.parametrize("prefix", [
    "8.250%", "8.25 %", ".25%", "8.25 percent", "8.25 per cent",
    "8.00% Fixed-to-Floating Rate", "8.25% Fixed Rate", "8.25% Floating-Rate",
])
def test_review_4238321738_coupon_preferred_caption_vetoes_same_class(db, prefix):
    member, opaque = "ClassOfStock=SeriesBCommonShares;", "ClassOfStock=OpaqueSecurity;"
    adsh = observe(db, member=member, title="Series B ordinary shares")
    count(db, adsh=adsh, member=member, shares=100_000)
    caption = prefix + " Series B Cumulative Redeemable Preferred Stock"
    observe(db, ticker="UNLISTED", member=opaque, kind="preferred", title=caption, adsh=adsh)
    _round4_set_observation_context(db, adsh=adsh, member=opaque, title=caption, dimh="other-context")
    ads_contract(db, class_token="series_b")
    row = resolve(db, members=[member])
    refused(row, "share_count_unit_unverified")
    assert any(v["same_canonical_class"] and v["security_title"] == caption
               for v in row["evidence"]["count_unit_veto_evidence"])


@pytest.mark.parametrize("caption,kind", [
    ("8.250% Series C Cumulative Preferred Stock", "preferred"),
    ("8.250% Warrants to purchase Series B ordinary shares", "warrant"),
    ("2025 Series B Preferred Stock", "preferred"),
    ("8.25 Series B Preferred Stock", "preferred"),
])
def test_review_4238321738_coupon_extension_keeps_its_subject_boundary(db, caption, kind):
    member, opaque = "ClassOfStock=SeriesBCommonShares;", "ClassOfStock=OpaqueSecurity;"
    adsh = observe(db, member=member, title="Series B ordinary shares")
    count(db, adsh=adsh, member=member, shares=100_000)
    observe(db, ticker="UNLISTED", member=opaque, kind=kind, title=caption, adsh=adsh)
    ads_contract(db, class_token="series_b")
    row = resolve(db, members=[member])
    assert row["status"] == "resolved" and row["ordinary_shares"] == 100_000
