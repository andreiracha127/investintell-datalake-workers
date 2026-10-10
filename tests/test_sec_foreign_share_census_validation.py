"""Class-scope proof regressions and independent source-review selection."""
from __future__ import annotations

from copy import deepcopy

import pytest

from scripts.sec_foreign_share_census_parser import parse_share_census
from scripts.validate_sec_foreign_share_census import (
    compare_censuses, impact, precision_packet, select_precision_sample,
)

PROMPT = ("Indicate the number of outstanding shares of each of the issuer's "
          "classes of capital or common stock as of the close of the period "
          "covered by the annual report: ")


def census(response: str, *, cik: int = 1, adsh: str = "0000000001-25-000001") -> dict:
    row = parse_share_census("<p>For the fiscal year ended December 31, 2024</p><p>"
        + PROMPT + response + "</p><p>Indicate by check mark whether...</p>")
    return {**row, "cik": cik, "adsh": adsh,
            "source_url": f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh}/cover.htm",
            "source_sha256": "0" * 64, "filed": "2025-03-01", "form": "20-F",
            "source_available_on": "2025-03-02"}


def line(row: dict) -> dict:
    return {"cik": row["cik"], "symbol": "TEST", "adsh": row["adsh"],
            "as_of": "2025-12-31", "shares_as_of": "2024-12-31",
            "refusal": "share_total_class_scope_unverified: test",
            "w1c_status": "resolved", "evidence": {"count_non_stale": True}}


@pytest.mark.parametrize("spelling", ["Founder", "Founders", "FOUNDERS", "founder"])
def test_founder_and_founders_have_the_same_positive_nonordinary_proof(spelling):
    row = census(f"Ordinary shares: 100; {spelling} shares: 1")
    assert row["complete"], row
    assert [entry["class_kind"] for entry in row["classes"]] == ["ordinary", "other"]
    result = impact([line(row)], [row])
    assert result["scope_proof_lines"] == 1
    assert result["lines"][0]["category"] == "single_ordinary_class"


@pytest.mark.parametrize("name", ["A shares", "Subordinate shares", "Foundership shares"])
def test_untyped_other_class_cannot_become_founder_proof(name):
    row = census("Ordinary shares: 100; A shares: 0")
    assert row["complete"], row
    row["classes"][1]["class_name"] = name
    result = impact([line(row)], [row])
    assert result["scope_proof_lines"] == 0
    assert result["lines"][0]["reason"] == "other_class_kind_unverified"


def test_nonpositive_ordinary_supply_does_not_qualify_with_founders():
    row = census("Ordinary shares: nil; Founders shares: 1")
    assert row["complete"], row
    assert impact([line(row)], [row])["scope_proof_lines"] == 0


@pytest.mark.parametrize("spelling", ["Founder", "Founders"])
def test_founder_spelling_retains_preference_deferred_and_unknown_class_guards(spelling):
    row = census(f"Ordinary shares: 100; {spelling} shares: 1; Preference shares: 2; Deferred shares: 3")
    assert row["complete"], row
    assert impact([line(row)], [row])["scope_proof_lines"] == 1
    row["classes"].append({"class_name": "A shares", "class_key": "class:a",
                           "class_kind": "other", "shares": 0})
    row["computed_total"] += 0
    result = impact([line(row)], [row])
    assert result["scope_proof_lines"] == 0
    assert result["lines"][0]["reason"] == "other_class_kind_unverified"


def test_independent_sample_is_order_invariant_and_uses_distinct_issuers():
    rows = []
    for cik in range(1, 65):
        for suffix in range(2):
            row = census("Ordinary shares: 100", cik=cik,
                         adsh=f"0000000001-25-{cik * 2 + suffix:06}")
            row["form"] = ("20-F", "20-F/A", "40-F", "40-F/A")[cik % 4]
            row["filed"] = f"{(2003, 2008, 2013, 2018, 2023)[cik % 5]}-03-01"
            rows.append(row)
    first = select_precision_sample(rows, "independent-test")
    second = select_precision_sample(list(reversed(rows)), "independent-test")
    assert [row["adsh"] for row in first] == [row["adsh"] for row in second]
    assert len(first) == 40
    assert len({row["cik"] for row in first}) == 40
    assert {row["form"] for row in first} == {"20-F", "20-F/A", "40-F", "40-F/A"}
    excluded = {row["adsh"] for row in first}
    fresh = select_precision_sample(rows, "independent-test", excluded)
    assert not excluded.intersection(row["adsh"] for row in fresh)
    packet = precision_packet(rows, "hash", None, seed="independent-test")
    assert packet["review_status"] == "pending"
    assert packet["reviewed"] == 0
    assert packet["precision"] is None


def test_flip_matrix_retains_all_changes_and_review_spreads_reasons():
    old, new = [], []
    for cik in range(1, 45):
        before = census("Ordinary shares: 100", cik=cik,
                        adsh=f"0000000001-25-{cik:06}")
        after = deepcopy(before)
        after.update(complete=False, status="incomplete",
                     reasons=[("footnoted_count", "ambiguous_cells", "bare_class")[cik % 3]])
        old.append(before)
        new.append(after)
    new[-1].update(conflicting=True, status="conflicting")
    result = compare_censuses(old, new)
    assert result["status_flip_matrix"]["complete"]["incomplete"] == 43
    assert result["status_flip_matrix"]["complete"]["conflicting"] == 1
    assert result["complete_to_incomplete_filings"] == 43
    assert result["flip_review"]["sample_size"] >= 30
    assert result["flip_review"]["represented_reasons"] == ["ambiguous_cells", "bare_class", "footnoted_count"]
    assert result["flip_review"]["review_status"] == "pending"
    assert all(record["review_status"] == "pending" for record in result["flip_review"]["records"])
