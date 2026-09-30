"""EDGAR bulk submissions enumeration and typed corroboration queue (SYNTHETIC filings)."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any

import pytest

from src.bonds.default_events import edgar
from src.bonds.default_events import sec_acquisition as sa

UTC = dt.timezone.utc
FIXTURES = Path(__file__).parent / "fixtures" / "bond_default_events" / "edgar"
KEYS = ("accessionNumber", "filingDate", "reportDate", "acceptanceDateTime", "form", "items", "primaryDocument")


def arrays(rows: list[tuple[str, ...]], *, acceptance: str = "2023-01-10T16:59:59.000Z") -> dict[str, list[Any]]:
    """rows: (accession, filing_date, form, items[, report_date[, primary_document]])."""
    out: dict[str, list[Any]] = {key: [] for key in KEYS}
    for row in rows:
        accession, filed, form, items = row[:4]
        out["accessionNumber"].append(accession)
        out["filingDate"].append(filed)
        out["reportDate"].append(row[4] if len(row) > 4 else filed)
        out["acceptanceDateTime"].append(acceptance)
        out["form"].append(form)
        out["items"].append(items)
        out["primaryDocument"].append(row[5] if len(row) > 5 else "doc.htm")
    return out


def build(path: Path, members: dict[str, Any]) -> str:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload if isinstance(payload, bytes) else json.dumps(payload))
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(cik: int, recent: dict[str, list[Any]], files: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"cik": str(cik), "name": f"Synthetic {cik}", "filings": {"recent": recent, "files": files or []}}


@pytest.fixture()
def bulk(tmp_path: Path) -> tuple[Path, str]:
    supplemental = arrays([
        ("0000000001-21-000005", "2021-10-01", "8-K", "1.03,9.01"),
        ("0000000001-19-000001", "2019-05-01", "8-K", "1.03"),
    ])
    members = {
        "CIK0000000001.json": main(1, arrays([
            ("0000000001-23-000001", "2023-01-10", "8-K", "1.03,2.04,9.01"),
            ("0000000001-23-000002", "2023-02-10", "8-K/A", "2.04"),
            ("0000000001-23-000003", "2023-03-10", "8-K", "11.03,12.04"),  # substrings must not match
            ("0000000001-23-000004", "2023-03-11", "8-K", "8.01"),
            ("0000000001-23-000005", "2023-03-12", "10-Q", ""),
            ("0000000001-23-000006", "2023-03-13", "8-K", "1.031, 2.04x"),
        ]), [{"name": "CIK0000000001-submissions-001.json", "filingCount": 2}]),
        "CIK0000000001-submissions-001.json": supplemental,
        # Former registrant, co-registrant copy of a shared accession.
        "CIK0000000002.json": main(2, arrays([("0000000001-23-000001", "2023-01-10", "8-K", "1.03,2.04,9.01")])),
    }
    path = tmp_path / "submissions.zip"
    return path, build(path, members)


def test_enumerates_recent_and_supplemental_with_token_items(bulk: tuple[Path, str]) -> None:
    path, sha = bulk
    result = edgar.enumerate_candidates(path, expected_sha256=sha, start=dt.date(2019, 1, 1),
                                        end=dt.date(2026, 9, 25))
    found = {c.accession_number: c for c in result.candidates}
    assert set(found) == {"0000000001-23-000001", "0000000001-23-000002", "0000000001-21-000005",
                          "0000000001-19-000001"}
    shared = found["0000000001-23-000001"]
    assert shared.ciks == ("0000000001", "0000000002") and shared.candidate_items == ("1.03", "2.04")
    assert not shared.metadata_conflict and len(shared.variants) == 1 and shared.variants[0].copies == 2
    assert found["0000000001-23-000002"].is_amendment
    assert result.stats["supplemental_files"] == 1 and result.stats["supplemental_rows"] == 2
    assert result.stats["invalid_item_tokens"] == 2
    assert result.count("1.03", "8-K") == 3 and result.count("2.04", "8-K/A") == 1
    assert result.count("1.03", "8-K", dt.date(2021, 9, 1), dt.date(2026, 8, 31)) == 2


def test_window_filtering_keeps_prehistory_optional(bulk: tuple[Path, str]) -> None:
    path, sha = bulk
    result = edgar.enumerate_candidates(path, expected_sha256=sha, start=dt.date(2021, 9, 1),
                                        end=dt.date(2026, 8, 31))
    assert "0000000001-19-000001" not in {c.accession_number for c in result.candidates}


def test_parallel_matches_serial(bulk: tuple[Path, str]) -> None:
    path, sha = bulk
    serial = edgar.enumerate_candidates(path, expected_sha256=sha, groups_per_task=1)
    parallel = edgar.enumerate_candidates(path, expected_sha256=sha, workers=2, groups_per_task=1)
    assert serial.candidates == parallel.candidates and serial.stats == parallel.stats


def test_hash_mismatch_and_unsafe_members(tmp_path: Path, bulk: tuple[Path, str]) -> None:
    path, _ = bulk
    with pytest.raises(edgar.EdgarError):
        edgar.enumerate_candidates(path, expected_sha256="0" * 64)
    evil = tmp_path / "evil.zip"
    sha = build(evil, {"../CIK0000000001.json": main(1, arrays([]))})
    with pytest.raises(edgar.EdgarError):
        edgar.enumerate_candidates(evil, expected_sha256=sha)


def test_array_misalignment_and_supplemental_integrity(tmp_path: Path) -> None:
    bad = arrays([("0000000009-23-000001", "2023-01-10", "8-K", "1.03")])
    bad["form"].append("8-K")
    members = {
        "CIK0000000009.json": main(9, bad, [{"name": "CIK0000000009-submissions-001.json", "filingCount": 5},
                                            {"name": "CIK0000000009-submissions-002.json", "filingCount": 1}]),
        "CIK0000000009-submissions-001.json": arrays([("0000000009-20-000001", "2020-01-10", "8-K", "2.04")]),
        "CIK0000000009-submissions-003.json": arrays([]),
    }
    path = tmp_path / "s.zip"
    result = edgar.enumerate_candidates(path, expected_sha256=build(path, members))
    assert result.stats["files_array_misaligned"] == 1
    assert result.stats["supplemental_filing_count_mismatch"] == 1
    assert result.stats["supplemental_missing"] == 1 and result.stats["supplemental_unreferenced"] == 1
    assert [c.accession_number for c in result.candidates] == ["0000000009-20-000001"]


def test_duplicate_accessions_identical_vs_conflicting(tmp_path: Path) -> None:
    recent = arrays([("0000000009-23-000001", "2023-01-10", "8-K", "1.03"),
                     ("0000000009-23-000001", "2023-01-10", "8-K", "1.03"),
                     ("0000000009-23-000002", "2023-01-11", "8-K", "2.04"),
                     ("0000000009-23-000002", "2023-01-11", "8-K", "2.04,9.01")])
    path = tmp_path / "s.zip"
    result = edgar.enumerate_candidates(path, expected_sha256=build(path, {"CIK0000000009.json": main(9, recent)}))
    assert result.stats["duplicate_accession_identical"] == 1
    assert result.stats["duplicate_accession_conflicting"] == 1
    found = {c.accession_number: c for c in result.candidates}
    assert set(found) == {"0000000009-23-000001", "0000000009-23-000002"}  # preserved, not dropped
    assert not found["0000000009-23-000001"].metadata_conflict
    conflicted = found["0000000009-23-000002"]
    assert conflicted.metadata_conflict and conflicted.conflict_scopes == ("same_cik",)
    assert conflicted.conflict_fields == ("items",) and len(conflicted.variants) == 2
    assert any(a.startswith("accession_metadata_conflict:same_cik:0000000009-23-000002") for a in result.anomalies)


@pytest.mark.parametrize(
    ("row_a", "row_b", "field"),
    [
        (("0000000009-23-000003", "2023-01-10", "8-K", "1.03", "2023-01-09", "a.htm"),
         ("0000000009-23-000003", "2023-01-10", "8-K", "1.03", "2023-01-09", "b.htm"), "primary_document"),
        (("0000000009-23-000003", "2023-01-10", "8-K", "1.03", "2023-01-09"),
         ("0000000009-23-000003", "2023-01-10", "8-K", "1.03", "2023-01-05"), "report_date"),
    ],
)
def test_same_cik_document_and_report_metadata_conflicts_detected(tmp_path: Path, row_a, row_b, field) -> None:
    path = tmp_path / "s.zip"
    sha = build(path, {"CIK0000000009.json": main(9, arrays([row_a, row_b]))})
    result = edgar.enumerate_candidates(path, expected_sha256=sha)
    (candidate,) = result.candidates
    assert candidate.conflict_fields == (field,) and candidate.metadata_conflict
    assert getattr(candidate, field) is None  # never resolved silently
    assert result.stats["duplicate_accession_conflicting"] == 1


def test_co_registrant_disagreement_flagged(tmp_path: Path) -> None:
    members = {
        "CIK0000000001.json": main(1, arrays([("0000000001-23-000001", "2023-01-10", "8-K", "1.03")])),
        "CIK0000000002.json": main(2, arrays([("0000000001-23-000001", "2023-01-10", "8-K", "1.03,2.04")])),
    }
    path = tmp_path / "s.zip"
    result = edgar.enumerate_candidates(path, expected_sha256=build(path, members))
    (candidate,) = result.candidates
    assert candidate.metadata_conflict and candidate.conflict_scopes == ("cross_cik",)
    assert candidate.candidate_items == ("1.03", "2.04") and candidate.conflict_fields == ("items",)
    assert {v.ciks for v in candidate.variants} == {("0000000001",), ("0000000002",)}
    assert result.stats["accession_conflict_field_items"] == 1


def test_co_registrant_copy_outside_filters_is_reconciled(tmp_path: Path) -> None:
    """The other CIK's copy does not match the item filter, form or window: it is still
    collected before filtering, so the disagreement is visible instead of silently lost."""
    members = {
        "CIK0000000001.json": main(1, arrays([("0000000001-23-000001", "2023-01-10", "8-K", "1.03")])),
        "CIK0000000002.json": main(2, arrays([("0000000001-23-000001", "2024-02-01", "8-K/A", "8.01",
                                               "2024-01-30", "other.htm")])),
    }
    path = tmp_path / "s.zip"
    result = edgar.enumerate_candidates(path, expected_sha256=build(path, members), start=dt.date(2023, 1, 1),
                                        end=dt.date(2023, 12, 31))
    (candidate,) = result.candidates
    assert candidate.ciks == ("0000000001", "0000000002")
    assert set(candidate.conflict_fields) == {"filing_date", "form", "items", "primary_document", "report_date"}
    assert candidate.form is None and candidate.filing_date is None  # no lowest-CIK choice
    assert result.count("1.03", "8-K") == 0 and result.stats["candidates_conflicted_form_or_date"] == 1
    queue = edgar.build_corroboration_queue(result.candidates)
    assert queue and all("accession_metadata_conflict" in " ".join(p.notes) for p in queue)


def test_discovery_forms_only_for_requested_ciks(bulk: tuple[Path, str]) -> None:
    path, sha = bulk
    none = edgar.enumerate_candidates(path, expected_sha256=sha)
    assert all(not c.discovery_only for c in none.candidates)
    some = edgar.enumerate_candidates(path, expected_sha256=sha, discovery_ciks={"0000000001"})
    discovery = {c.accession_number: c for c in some.candidates if c.discovery_only}
    # 000006 has only malformed item tokens ("1.031", "2.04x"): neither a candidate nor 8.01 discovery.
    assert set(discovery) == {"0000000001-23-000004", "0000000001-23-000005"}
    assert discovery["0000000001-23-000004"].candidate_items == ("8.01",)


@pytest.mark.parametrize(
    ("raw", "valid", "invalid"),
    [("1.03,2.04", ("1.03", "2.04"), ()), ("11.03", ("11.03",), ()), ("1.031", (), ("1.031",)),
     ("", (), ()), (None, (), ()), (" 2.04 , 9.01 ", ("2.04", "9.01"), ())],
)
def test_item_token_parsing(raw: object, valid: tuple[str, ...], invalid: tuple[str, ...]) -> None:
    assert edgar.parse_item_tokens(raw) == (valid, invalid)


def test_json_acceptance_is_eastern_wall_time_matching_real_header() -> None:
    # Stage 1A sample 0001193125-23-005670: header ACCEPTANCE-DATETIME 20230110165959,
    # submissions JSON acceptanceDateTime "2023-01-10T16:59:59.000Z" (digits are Eastern).
    header = sa.parse_acceptance_header(
        (FIXTURES / "0001752724-21-029444-index-headers.html").read_bytes(),
        accession_number="0001752724-21-029444", url="u", document_sha256="0" * 64,
        retrieved_at=dt.datetime(2026, 9, 25, tzinfo=UTC))
    assert edgar.provisional_acceptance_utc("2021-02-19T11:44:16.000Z") == header.acceptance_at
    assert edgar.provisional_acceptance_utc("2023-01-10T16:59:59.000Z") == dt.datetime(2023, 1, 10, 21, 59, 59,
                                                                                     tzinfo=UTC)
    assert edgar.provisional_acceptance_utc("2023-07-10T16:59:59.000Z") == dt.datetime(2023, 7, 10, 20, 59, 59,
                                                                                     tzinfo=UTC)
    assert edgar.provisional_acceptance_utc("2021-03-14T02:30:00.000Z") is None
    assert edgar.provisional_acceptance_utc("garbage") is None


def test_full_index_cross_check(bulk: tuple[Path, str]) -> None:
    path, sha = bulk
    result = edgar.enumerate_candidates(path, expected_sha256=sha, crosscheck_quarters=("2023Q1",))
    entries = [
        sa.FormIndexEntry("8-K", "X", "0000000001", dt.date(2023, 1, 10), "f", "0000000001-23-000001"),
        sa.FormIndexEntry("8-K", "X", "0000000003", dt.date(2023, 2, 1), "f", "0000000003-23-000001"),
    ]
    check = edgar.cross_check_full_index(result, "2023Q1", entries)
    assert check["only_in_index"] == 1 and check["in_both"] == 1
    assert check["only_in_submissions"] == check["submissions_accessions"] - 1
    with pytest.raises(edgar.EdgarError):
        edgar.cross_check_full_index(result, "2024Q1", entries)


def _header(accession: str, form: str, cik: str) -> sa.AcceptanceHeader:
    doc = (f"<SEC-HEADER>\n<ACCEPTANCE-DATETIME>20230110165959\n<ACCESSION-NUMBER>{accession}\n<TYPE>{form}\n"
           f"<ITEMS>1.03\n<CIK>{cik}\n</SEC-HEADER>").encode()
    return sa.parse_acceptance_header(doc, accession_number=accession, url="u", document_sha256="0" * 64,
                                      retrieved_at=dt.datetime(2026, 9, 25, tzinfo=UTC))


def test_queue_is_typed_candidate_only_never_auto_admitted(bulk: tuple[Path, str]) -> None:
    path, sha = bulk
    result = edgar.enumerate_candidates(path, expected_sha256=sha)
    headers = {"0000000001-23-000001": _header("0000000001-23-000001", "8-K", "0000000002")}
    queue = edgar.build_corroboration_queue(result.candidates, acceptance_headers=headers,
                                            linked_issue_hints={"0000000001": ["037833100"]})
    assert queue and all(p.proposed_status == "candidate" and p.reviewer_role == "extraction_proposer"
                         and not p.auto_admissible and not p.document_retrieval_allowed for p in queue)
    by_key = {p.proposal_key: p for p in queue}
    bankruptcy = by_key["edgar:0000000001-23-000001:1.03"]
    acceleration = by_key["edgar:0000000001-23-000001:2.04"]
    assert bankruptcy.candidate_event_type == "bankruptcy" and "not_plan_confirmation_only" in bankruptcy.required_evidence
    assert acceleration.candidate_event_type == "payment_default"
    assert "grace_period_end_uncured" in acceleration.missing_evidence
    assert bankruptcy.public_known_at == dt.datetime(2023, 1, 10, 21, 59, 59, tzinfo=UTC)
    assert bankruptcy.public_time_basis == "edgar_acceptance_datetime"
    assert bankruptcy.linked_issue_hints == ("037833100",) and "document_text" in bankruptcy.missing_evidence
    assert any("holdout_custody_filter_not_in_place" in n for n in bankruptcy.notes)
    amendment = by_key["edgar:0000000001-23-000002:2.04"]
    assert amendment.is_amendment and amendment.public_known_at is None
    assert "acceptance_header" in amendment.missing_evidence


def test_queue_header_mismatch_and_custody_filter(bulk: tuple[Path, str]) -> None:
    path, sha = bulk
    result = edgar.enumerate_candidates(path, expected_sha256=sha)
    wrong = {"0000000001-23-000001": _header("0000000001-23-000001", "8-K", "0000000077")}
    queue = edgar.build_corroboration_queue(result.candidates, acceptance_headers=wrong,
                                            custody_filter=lambda accession, ciks: accession == "0000000001-23-000001")
    shared = [p for p in queue if p.accession_number == "0000000001-23-000001"]
    assert all(p.public_known_at is None and "acceptance_header_mismatch" in p.notes for p in shared)
    assert all(p.document_retrieval_allowed and p.custody_filter_applied for p in shared)
    others = [p for p in queue if p.accession_number != "0000000001-23-000001"]
    assert others and not any(p.document_retrieval_allowed for p in others)
