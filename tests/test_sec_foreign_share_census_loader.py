"""Offline artifact integrity and W1c-compatible census reconciliation.

Run this file alone against disposable loopback PG18 with PYTEST_WORKERS=2.
Source caches are synthetic and no network or production connection is used.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
import json
from pathlib import Path

import pytest

from scripts import load_sec_foreign_share_census as loader
from scripts import load_sec_foreign_listing_evidence as w1c
from test_sec_foreign_share_census import sql_database


@pytest.fixture
def db(sql_database):
    sql_database.execute("BEGIN")
    sql_database.execute("TRUNCATE public.sec_foreign_share_census, public.sec_foreign_share_census_sources")
    try:
        yield sql_database
    finally:
        sql_database.execute("ROLLBACK")


def source(*, url="report.htm", sha="a", parser="test-v1", count=1,
           filed="2025-04-15", floor=None, adsh="0000950170-25-058197", cik=1846832):
    floor = floor or filed
    return w1c.canonical_document({
        "cik": cik, "adsh": adsh, "form": "20-F", "document_role": "primary",
        "accepted": filed + "T09:00:00Z", "filed": filed, "period": "2024-12-31",
        "query_accepted_on": floor, "publication_floor_on": floor,
        "publication_floor_proof": {"source": "discovery_reported_publication_floor", "publication_floor_on": floor},
        "filing_date_status": "resolved",
        "filing_date_proof": {"source": "w1_same_accession", "filed": filed,
                              "records": [{"cik": cik, "adsh": adsh, "filed": filed}]},
        "source_url": "https://www.sec.gov/Archives/edgar/data/" + str(cik) + "/" + adsh.replace("-", "") + "/" + url,
        "source_sha256": sha * 64, "parser_version": parser,
        "census_count": count, "census_status": "complete" if count else "none",
    })


def fact(document, *, shares=100, revision="1", **updates):
    available = max(date.fromisoformat(document["filed"]) + timedelta(days=1),
                    date.fromisoformat(document["publication_floor_on"])).isoformat()
    row = {
        **document, "period_end": "2024-12-31", "shares_as_of": "2024-12-31", "date_explicit": True,
        "classes": [{"class_name": "Ordinary shares", "class_key": "ordinary", "class_kind": "ordinary", "shares": shares}],
        "stated_total": shares, "computed_total": shares, "complete": True, "conflicting": False,
        "status": "complete", "reasons": [], "cross_checks": {},
        "source_available_on": available, "available_on": available,
        "evidence_text": "Ordinary shares: " + str(shares) + "; reading " + revision,
        "evidence_location": "cover:outstanding-shares",
    }
    row.update(updates)
    row["fact_hash"] = loader.census_fact_hash(row)
    return row


def manifest(*documents):
    return {"manifest_version": loader.MANIFEST_VERSION, "complete": True, "parse_complete": True,
            "census_complete": True,
            "filing_date_enrichment": {"complete": True, "version": "sec-official-filing-date-v2"},
            "documents": list(documents)}


def bundle(document=None, **updates):
    document = source() if document is None else deepcopy(document)
    row = fact(document, **updates)
    document.update(census_fact_hash=row["fact_hash"], census_status=row["status"])
    return manifest(document), [row]


def empty_bundle(document):
    return bundle(document, complete=False, conflicting=False, status="none", classes=[],
                  shares_as_of=None, date_explicit=False, stated_total=None, computed_total=None,
                  evidence_text="", evidence_location="")


def apply(db, document=None, *, observed="2026-10-10", **updates):
    plan, rows = bundle(document, **updates)
    return loader.apply_census(db, plan, rows, date.fromisoformat(observed))


def at(db, day):
    return db.execute("SELECT fact_hash,available_on,source_available_on,parser_version,classes,status "
                      "FROM public.sec_foreign_share_census_at(1846832,%s::date)", [day]).fetchone()


def w1_count(*, class_key="", normalized=None, shares="100", stated_on="2024-12-31", **updates):
    row = {"id": 9, "class_key": class_key, "normalized_class_key": normalized,
           "shares": shares, "stated_on": stated_on}
    row.update(updates)
    return row


def test_cross_check_exact_integral_numeric_and_dates_are_retained():
    row = fact(source(), shares=9007199254740993)
    counts = [w1_count(shares="9007199254740993.00000000", stated_on="2025-01-31")]
    result = loader.cross_check(row, counts, "b" * 64)
    check = result["cross_checks"]["undimensioned"][0]
    assert check["status"] == "match" and not check["date_match"]
    assert check["stated_on"] == "2025-01-31"
    assert check["w1_shares"] == "9007199254740993.00000000"
    assert check["census_shares"] == 9007199254740993
    assert result["cross_checks"]["w1_input_sha256"] == "b" * 64
    assert not result["conflicting"] and result["classes"][0]["shares"] == 9007199254740993


@pytest.mark.parametrize("class_key,normalized", [("", None), ("raw-ordinary", "ordinary")])
def test_cross_check_mismatch_marks_conflict_even_if_statement_dates_differ(class_key, normalized):
    row = fact(source())
    result = loader.cross_check(row, [w1_count(class_key=class_key, normalized=normalized, shares="101", stated_on="2025-01-31")], "c" * 64)
    assert result["conflicting"] and result["status"] == "conflicting"
    assert "w1_count_mismatch" in result["reasons"]
    assert result["classes"][0]["shares"] == 100 and result["stated_total"] == 100
    checks = result["cross_checks"]["undimensioned" if not class_key else "class_dimensioned"]
    assert checks[0]["status"] == "mismatch" and checks[0]["date_match"] is False


def test_cross_check_unsupported_w1_label_is_unbound_without_inventing_class():
    normalized = None
    row = fact(source())
    result = loader.cross_check(row, [w1_count(class_key="unsupported", normalized=normalized)], "a" * 64)
    check = result["cross_checks"]["class_dimensioned"][0]
    assert check["status"] == "unbound" and check["census_shares"] is None
    assert check["class_key"] == "unsupported" and check["normalized_class_key"] == normalized
    assert result["classes"] == row["classes"] and not result["conflicting"]


def test_cross_check_known_w1_class_absent_from_complete_census_conflicts():
    row = fact(source(), classes=[{"class_name": "Class A shares", "class_key": "class:a", "class_kind": "ordinary", "shares": 100}])
    result = loader.cross_check(row, [w1_count(class_key="ClassBOrdinarySharesMember", normalized="class:b", shares="200")], "a" * 64)
    check = result["cross_checks"]["class_dimensioned"][0]
    assert check["status"] == "mismatch" and check["mismatch_reason"] == "class_absent_from_census"
    assert check["census_shares"] is None and result["conflicting"]
    assert result["status"] == "conflicting" and "w1_count_mismatch" in result["reasons"]
    assert result["classes"] == [{"class_name": "Class A shares", "class_key": "class:a", "class_kind": "ordinary", "shares": 100}]


def test_cross_check_depositary_member_is_not_another_underlying_class():
    row = fact(source())
    result = loader.cross_check(row, [w1_count(class_key="AmericanDepositarySharesMember", normalized="class:a", shares="200")], "a" * 64)
    assert result["cross_checks"]["class_dimensioned"][0]["status"] == "unbound"
    assert not result["conflicting"]


def test_cross_check_ambiguous_present_class_is_unbound_not_absent():
    row = fact(source(), classes=[{"class_name": "Class A ordinary shares", "class_key": "class:a", "class_kind": "ordinary", "shares": 100},
                                 {"class_name": "Class A preference shares", "class_key": "class:a", "class_kind": "preferred", "shares": 20}],
               stated_total=120, computed_total=120)
    loader.cross_check(row, [w1_count(class_key="ClassAMember", normalized="class:a", shares="100")], "a" * 64)
    check = row["cross_checks"]["class_dimensioned"][0]
    assert check["status"] == "unbound" and check["census_shares"] is None
    assert check["mismatch_reason"] is None and not row["conflicting"]


def test_cross_check_missing_count_and_ambiguous_class_do_not_choose_value():
    row = fact(source(), complete=False, status="incomplete", computed_total=None, stated_total=None,
               classes=[{"class_name": "Class A shares", "class_key": "class:a", "class_kind": "ordinary", "shares": None}])
    loader.cross_check(row, [w1_count(), w1_count(class_key="raw-a", normalized="class:a")], "a" * 64)
    assert row["cross_checks"]["undimensioned"][0]["status"] == "census_count_missing"
    assert row["cross_checks"]["class_dimensioned"][0]["status"] == "census_count_missing"
    row["classes"].append({"class_name": "Other Class A", "class_key": "class:a", "class_kind": "ordinary", "shares": 100})
    loader.cross_check(row, [w1_count(class_key="raw-a", normalized="class:a")], "a" * 64)
    assert row["cross_checks"]["class_dimensioned"][0]["status"] == "unbound"


def test_cross_check_uses_class_sum_when_no_stated_total_and_preserves_nil():
    row = fact(source(), stated_total=None, computed_total=100,
               classes=[{"class_name": "Ordinary shares", "class_key": "ordinary", "class_kind": "ordinary", "shares": 100},
                        {"class_name": "Series A preference shares", "class_key": "series:a", "class_kind": "preferred", "shares": 0}])
    loader.cross_check(row, [w1_count(), w1_count(class_key="raw-pref", normalized="series:a", shares="0")], "a" * 64)
    assert row["cross_checks"]["undimensioned"][0]["census_shares"] == 100
    assert row["cross_checks"]["class_dimensioned"][0]["status"] == "match"
    assert not row["conflicting"]


def cache_document(raw_root, document, content):
    directory = raw_root / "documents"
    directory.mkdir(parents=True, exist_ok=True)
    key = w1c.digest(document["source_url"].encode())
    (directory / (key + ".bin")).write_bytes(content)
    w1c.write_json(directory / (key + ".json"), {"url": document["source_url"], "sha256": w1c.digest(content)})


@pytest.fixture
def replay_inputs(tmp_path, monkeypatch):
    raw_root, stage = tmp_path / "raw", tmp_path / "stage"
    raw_root.mkdir()
    counts = tmp_path / "counts.json"
    counts.write_bytes(b"[]\n")
    content = ("<p>For the fiscal year ended December 31, 2024</p><p>Indicate the number of outstanding shares "
               "of each of the issuer's classes of capital or common stock as of the close of the period covered "
               "by the annual report: 100 Ordinary shares.</p><p>Indicate by check mark whether the registrant...</p>").encode()
    document = source()
    document["source_sha256"] = w1c.digest(content)
    cache_document(raw_root, document, content)
    def refuse_network(*_args, **_kwargs):
        pytest.fail("Offline census replay attempted a network request")
    monkeypatch.setattr(w1c.SecClient, "request", refuse_network)
    return manifest(document), raw_root, stage, counts, content


def test_offline_replay_is_verified_read_only_and_deterministic(replay_inputs):
    plan, raw_root, stage, counts, _ = replay_inputs
    before = {path.relative_to(raw_root): path.read_bytes() for path in raw_root.rglob("*") if path.is_file()}
    output = stage / "census.jsonl"
    first = loader.replay(plan, stage, raw_root, output, counts, workers=1)
    manifest_path = stage / "manifest.json"
    verified, rows = loader.verified_artifact(manifest_path, output)
    assert first["census_complete"] and first["run_metrics"]["network_requests"] == 0
    assert first["run_metrics"]["raw_cache_read_only"] is True
    assert verified["census_sha256"] == w1c.digest(output.read_bytes())
    assert first["w1_counts_sha256"] == w1c.digest(counts.read_bytes())
    assert rows[0]["source_sha256"] == plan["documents"][0]["source_sha256"]
    assert rows[0]["classes"][0]["shares"] == 100 and rows[0]["status"] == "complete"
    initial = output.read_bytes()
    loader.replay(plan, stage, raw_root, output, counts, workers=1)
    assert output.read_bytes() == initial
    assert {path.relative_to(raw_root): path.read_bytes() for path in raw_root.rglob("*") if path.is_file()} == before


def test_actual_pdf_cover_replay_extracts_source_bytes_and_maps_evidence_pages(tmp_path, monkeypatch):
    import io
    import re
    from pypdf import PdfReader, PdfWriter
    from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject

    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    font = DictionaryObject({NameObject("/Type"): NameObject("/Font"),
                             NameObject("/Subtype"): NameObject("/Type1"),
                             NameObject("/BaseFont"): NameObject("/Helvetica")})
    page[NameObject("/Resources")] = DictionaryObject({
        NameObject("/Font"): DictionaryObject({NameObject("/F1"): writer._add_object(font)})})
    lines = ["For the fiscal year ended December 31, 2024",
             "Indicate the number of outstanding shares of each of the issuer's",
             "classes of capital or common stock as of the close of the period",
             "covered by the annual report:",
             "Class A common shares: 400",
             "Class B common shares: 100",
             "Total: 500",
             "Indicate by check mark whether the registrant"]
    commands = ["BT /F1 10 Tf 36 750 Td 16 TL"]
    for index, line in enumerate(lines):
        if index:
            commands.append("T*")
        commands.append("(" + line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)") + ") Tj")
    commands.append("ET")
    stream = DecodedStreamObject()
    stream.set_data("\n".join(commands).encode("ascii"))
    page[NameObject("/Contents")] = writer._add_object(stream)
    buffer = io.BytesIO()
    writer.write(buffer)
    raw = buffer.getvalue()
    raw_root, stage = tmp_path / "raw", tmp_path / "stage"
    document = source(url="cover.pdf")
    document["source_sha256"] = w1c.digest(raw)
    cache_document(raw_root, document, raw)
    counts = tmp_path / "counts.json"
    counts.write_bytes(b"[]\n")
    def refuse_network(*_args, **_kwargs):
        pytest.fail("PDF replay attempted a network request")
    monkeypatch.setattr(w1c.SecClient, "request", refuse_network)
    result = loader.replay(manifest(document), stage, raw_root, stage / "census.jsonl", counts, workers=1)
    assert result["census_complete"], result["gaps"]
    _, rows = loader.verified_artifact(stage / "manifest.json", stage / "census.jsonl")
    assert rows[0]["complete"] and rows[0]["computed_total"] == 500
    assert [cls["shares"] for cls in rows[0]["classes"]] == [400, 100]
    assert rows[0]["source_sha256"] == w1c.digest(raw)
    assert result["documents"][0]["content_format"] == "pdf"
    match = re.search(r"pdf/pages=1;normalized-text-span=(\d+):(\d+)", rows[0]["evidence_location"])
    assert match, rows[0]["evidence_location"]
    pdf_pages = [p.extract_text(extraction_mode="layout") for p in PdfReader(io.BytesIO(raw)).pages]
    _, normalized, _ = w1c.pdf_parser_input(pdf_pages)
    assert normalized[int(match[1]):int(match[2])] == rows[0]["evidence_text"]


@pytest.mark.parametrize("mutation", ["missing", "tampered", "manifest_hash"])
def test_replay_cache_failure_is_gap_preserves_last_artifact_and_cannot_apply(replay_inputs, mutation):
    plan, raw_root, stage, counts, _ = replay_inputs
    stage.mkdir()
    output = stage / "census.jsonl"
    output.write_bytes(b"last-good\n")
    if mutation == "missing":
        next((raw_root / "documents").glob("*.bin")).unlink()
    elif mutation == "tampered":
        next((raw_root / "documents").glob("*.bin")).write_bytes(b"tampered")
    else:
        plan["documents"][0]["source_sha256"] = "f" * 64
    result = loader.replay(plan, stage, raw_root, output, counts, workers=1)
    assert not result["census_complete"] and not result["parse_complete"]
    assert result["run_metrics"]["gaps"] == 1 and result["documents"][0]["census_status"] == "gap"
    assert output.read_bytes() == b"last-good\n"
    with pytest.raises(ValueError, match="complete"):
        loader.apply_census(None, result, [], date(2026, 10, 10))


@pytest.mark.parametrize("workers", [0, -1, 13, 24])
def test_worker_cap_rejects_before_any_parse_or_write(replay_inputs, workers):
    plan, raw_root, stage, counts, _ = replay_inputs
    with pytest.raises(ValueError, match="between 1 and 12"):
        loader.replay(plan, stage, raw_root, stage / "census.jsonl", counts, workers=workers)
    assert not stage.exists()


@pytest.mark.parametrize("target", ["cache", "output"])
def test_raw_cache_rejects_nested_writable_artifact_paths(replay_inputs, target):
    plan, raw_root, stage, counts, _ = replay_inputs
    cache = raw_root / "new-cache" if target == "cache" else stage
    output = raw_root / "new-output.jsonl" if target == "output" else stage / "census.jsonl"
    with pytest.raises(ValueError, match="read-only"):
        loader.replay(plan, cache, raw_root, output, counts, workers=1)
    assert not output.exists() and not cache.exists()


def test_replay_refuses_duplicate_sources_and_unproven_filing_dates(replay_inputs):
    plan, raw_root, stage, counts, _ = replay_inputs
    plan["documents"].append(deepcopy(plan["documents"][0]))
    with pytest.raises(ValueError, match="Duplicate"):
        loader.replay(plan, stage, raw_root, stage / "census.jsonl", counts, workers=1)
    plan["documents"].pop()
    plan["documents"][0]["filing_date_proof"]["filed"] = "2025-04-14"
    with pytest.raises(ValueError, match="authoritative"):
        loader.replay(plan, stage, raw_root, stage / "census.jsonl", counts, workers=1)


def test_initialization_cross_checks_only_active_same_filing_counts(tmp_path):
    counts = [dict(w1_count(), cik=1846832, adsh="0000950170-25-058197", retired_on=None),
              dict(w1_count(shares="200"), cik=1846832, adsh="0000950170-25-058198", retired_on=None),
              dict(w1_count(shares="300"), cik=1846832, adsh="0000950170-25-058197", retired_on="2026-01-01")]
    path = tmp_path / "counts.json"
    w1c.write_json(path, counts)
    loader._initialize(str(tmp_path / "stage"), str(tmp_path / "raw"), str(path))
    assert loader._COUNTS[(1846832, "0000950170-25-058197")] == counts[:1]
    assert loader._COUNT_SHA == w1c.digest(path.read_bytes())


def test_verified_artifact_rejects_even_whitespace_byte_changes(tmp_path):
    plan, rows = bundle()
    output = tmp_path / "census.jsonl"
    w1c.write_bytes(output, (w1c.canonical_json(rows[0]) + "\n").encode())
    plan["census_sha256"] = w1c.digest(output.read_bytes())
    path = tmp_path / "manifest.json"
    w1c.write_json(path, plan)
    assert loader.verified_artifact(path, output) == (plan, rows)
    output.write_bytes(output.read_bytes() + b" \n")
    with pytest.raises(ValueError, match="hash mismatch"):
        loader.verified_artifact(path, output)


@pytest.mark.parametrize("field,value", [
    ("class_count", 1.5),
    ("class_count", "1.5"),
    ("class_count", -1),
    ("class_count", "NaN"),
    ("class_count", "Infinity"),
    ("class_count", "-Infinity"),
    ("class_count", None),
    ("computed_total", None),
    ("computed_total", 101),
    ("computed_total_missing", None),
    ("numeric_residue", ["500"]),
    ("text_residue", ["Founder"]),
])
def test_semantic_artifact_forgery_refuses_even_with_all_hashes_recomputed(tmp_path, field, value):
    plan, rows = bundle(stated_total=None)
    row = rows[0]
    if field == "class_count":
        row["classes"][0]["shares"] = value
    elif field == "computed_total_missing":
        row.pop("computed_total")
    else:
        row[field] = value
    # The attacker controls the manifest and every checksum. The semantic
    # completeness guard must still refuse before opening a DB transaction.
    row["fact_hash"] = loader.census_fact_hash(row)
    plan["documents"][0]["census_fact_hash"] = row["fact_hash"]
    output = tmp_path / "census.jsonl"
    w1c.write_bytes(output, (w1c.canonical_json(row) + "\n").encode())
    plan["census_sha256"] = w1c.digest(output.read_bytes())
    manifest_path = tmp_path / "manifest.json"
    w1c.write_json(manifest_path, plan)
    verified_plan, verified_rows = loader.verified_artifact(manifest_path, output)
    assert verified_plan == plan and verified_rows == rows
    with pytest.raises(ValueError, match="finite|integral|nonnegative|count|sum|residue|grammar"):
        loader.apply_census(None, verified_plan, verified_rows, date(2026, 10, 10))


def test_same_byte_parser_correction_inherits_date_and_removes_old_reading_all_dates(db):
    first_plan, first_rows = bundle()
    loader.apply_census(db, first_plan, first_rows, date(2026, 10, 9))
    corrected = source(parser="test-v2")
    apply(db, corrected, shares=101, revision="2")
    for day in ("2025-04-16", "2025-12-31", "2026-10-09", "2026-10-10"):
        row = at(db, day)
        assert row[3] == "test-v2" and row[4][0]["shares"] == 101
        assert row[1] == date(2025, 4, 16)
    assert db.execute("SELECT retired_on,retired_reason FROM public.sec_foreign_share_census WHERE fact_hash=%s", [first_rows[0]["fact_hash"]]).fetchone() == (date(2026, 10, 10), "parser_correction")
    assert at(db, "2025-04-15") is None


def test_revised_source_is_prospective_and_later_same_byte_correction_inherits_republication(db):
    apply(db, observed="2026-10-08")
    revised = source(sha="b", parser="test-v2")
    apply(db, revised, observed="2026-10-09", shares=200, revision="2")
    assert at(db, "2026-10-08")[4][0]["shares"] == 100
    assert at(db, "2026-10-09")[1] == date(2026, 10, 9)
    apply(db, source(sha="b", parser="test-v3"), shares=201, revision="3")
    assert at(db, "2025-04-16")[4][0]["shares"] == 100
    assert at(db, "2026-10-08")[4][0]["shares"] == 100
    assert at(db, "2026-10-09")[4][0]["shares"] == 201
    assert at(db, "2026-10-09")[1] == date(2026, 10, 9)
    assert db.execute("SELECT retired_reason FROM public.sec_foreign_share_census ORDER BY id").fetchall() == [("source",), ("parser_correction",), (None,)]


@pytest.mark.parametrize("sha,reason", [("a", "parser_correction"), ("b", "source")])
def test_successful_zero_census_parse_retires_with_correct_history(db, sha, reason):
    apply(db, observed="2026-10-09")
    empty = source(sha=sha, parser="test-v2", count=0)
    plan, rows = empty_bundle(empty)
    result = loader.apply_census(db, plan, rows, date(2026, 10, 10))
    assert result == {"inserted": 0, "retired": 1, "unchanged": 0, "sources": 1}
    assert (at(db, "2025-04-16") is None) == (reason == "parser_correction")
    assert at(db, "2026-10-10") is None
    assert db.execute("SELECT census_count,first_loaded_on,last_loaded_on FROM public.sec_foreign_share_census_sources").fetchone() == (0, date(2026, 10, 9), date(2026, 10, 10))
    assert db.execute("SELECT retired_reason FROM public.sec_foreign_share_census").fetchone() == (reason,)


def test_same_byte_addition_after_zero_parse_and_noop_uses_source_public_date(db):
    empty = source(count=0)
    plan, rows = empty_bundle(empty)
    loader.apply_census(db, plan, rows, date(2026, 10, 8))
    loader.apply_census(db, plan, rows, date(2026, 10, 9))
    apply(db, source(parser="test-v2"))
    assert at(db, "2025-04-16")[1:3] == (date(2025, 4, 16), date(2025, 4, 16))
    assert db.execute("SELECT first_loaded_on,last_loaded_on FROM public.sec_foreign_share_census_sources").fetchone() == (date(2026, 10, 8), date(2026, 10, 10))


def test_publication_floor_and_noop_replay_preserve_dates(db):
    document = source(floor="2026-10-09")
    apply(db, document, observed="2026-10-09")
    assert at(db, "2026-10-08") is None
    apply(db, source(floor="2026-10-09", parser="test-v2"), shares=101)
    assert at(db, "2026-10-09")[1:3] == (date(2026, 10, 9), date(2026, 10, 9))
    noop = apply(db, source(floor="2026-10-09", parser="test-v2"), observed="2026-10-11", shares=101)
    assert noop == {"inserted": 0, "retired": 0, "unchanged": 1, "sources": 1}
    assert at(db, "2026-10-09")[1] == date(2026, 10, 9)


def test_initial_batch_all_sources_use_public_date_but_later_new_source_is_prospective(db):
    first, second = source(), source(url="second.htm")
    first_plan, first_rows = bundle(first)
    second_plan, second_rows = bundle(second)
    loader.apply_census(db, manifest(*first_plan["documents"], *second_plan["documents"]), first_rows + second_rows, date(2026, 10, 8))
    assert db.execute("SELECT DISTINCT available_on FROM public.sec_foreign_share_census").fetchall() == [(date(2025, 4, 16),)]
    apply(db, source(url="later.htm"), observed="2026-10-10", shares=300)
    assert db.execute("SELECT available_on FROM public.sec_foreign_share_census WHERE source_url LIKE '%later.htm'").fetchone() == (date(2026, 10, 10),)


@pytest.mark.parametrize("key,value", [("complete", False), ("parse_complete", False), ("census_complete", False), ("manifest_version", "old-v0")])
def test_apply_incomplete_manifest_refuses_before_database_access(key, value):
    plan, rows = bundle()
    plan[key] = value
    with pytest.raises(ValueError, match="complete"):
        loader.apply_census(None, plan, rows, date(2026, 10, 10))


def test_apply_unmanifested_source_and_changed_fact_hash_refuse_before_database_access():
    plan, rows = bundle()
    unknown = deepcopy(rows[0])
    unknown["source_package"] = "unmanifested"
    with pytest.raises(ValueError, match="unmanifested"):
        loader.apply_census(None, plan, [unknown], date(2026, 10, 10))
    rows[0]["classes"][0]["shares"] = 101
    with pytest.raises(ValueError, match="hash mismatch"):
        loader.apply_census(None, plan, rows, date(2026, 10, 10))


@pytest.mark.parametrize("key,value", [("cik", 99), ("adsh", "0000950170-25-058198"), ("form", "40-F"),
    ("filed", "2025-04-14"), ("publication_floor_on", "2025-04-17"), ("source_sha256", "f" * 64),
    ("parser_version", "forged-v0"), ("source_url", "https://www.sec.gov/other.htm")])
def test_apply_provenance_must_exactly_match_source_manifest(key, value):
    plan, rows = bundle()
    rows[0][key] = value
    rows[0]["fact_hash"] = loader.census_fact_hash(rows[0])
    plan["documents"][0]["census_fact_hash"] = rows[0]["fact_hash"]
    with pytest.raises(ValueError, match="provenance"):
        loader.apply_census(None, plan, rows, date(2026, 10, 10))


def test_apply_refuses_duplicate_manifest_sources(db):
    plan, rows = bundle()
    plan["documents"].append(deepcopy(plan["documents"][0]))
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        loader.apply_census(db, plan, rows, date(2026, 10, 10))
    assert db.execute("SELECT count(*) FROM public.sec_foreign_share_census_sources").fetchone() == (0,)


@pytest.mark.parametrize("mutation", ["hash", "status", "forged_none"])
def test_apply_requires_exact_manifest_fact_identity_and_consistent_status(db, mutation):
    plan, rows = bundle()
    if mutation == "hash":
        plan["documents"][0]["census_fact_hash"] = "f" * 32
    elif mutation == "status":
        plan["documents"][0]["census_status"] = "incomplete"
    else:
        rows[0]["status"] = "none"
        plan["documents"][0].update(census_status="none", census_count=0)
    with pytest.raises(ValueError, match="[Ii]dentity|[Ss]tatus|[Ff]act hash|manifest"):
        loader.apply_census(db, plan, rows, date(2026, 10, 10))
    assert db.execute("SELECT count(*) FROM public.sec_foreign_share_census_sources").fetchone() == (0,)


def test_late_manifest_count_validation_cannot_retire_or_insert_partial_batch(db):
    apply(db, observed="2026-10-09")
    plan, rows = bundle(source(parser="test-v2"), shares=200)
    invalid_plan, invalid_rows = bundle(source(url="invalid.htm", count=2))
    plan["documents"].extend(invalid_plan["documents"])
    rows.extend(invalid_rows)
    with pytest.raises(ValueError, match="count"):
        loader.apply_census(db, plan, rows, date(2026, 10, 10))
    assert db.execute("SELECT count(*),count(*) FILTER (WHERE retired_on IS NOT NULL) FROM public.sec_foreign_share_census").fetchone() == (1, 0)
    assert at(db, "2025-04-16")[4][0]["shares"] == 100
    assert db.execute("SELECT last_loaded_on FROM public.sec_foreign_share_census_sources").fetchone() == (date(2026, 10, 9),)


def test_transaction_rollback_restores_retirement_when_database_insert_fails(db):
    psycopg = pytest.importorskip("psycopg")
    apply(db, observed="2026-10-09")
    db.execute("SAVEPOINT census_failed_apply")
    with pytest.raises(psycopg.errors.CheckViolation):
        apply(db, source(parser="test-v2"), shares=200, evidence_text=" ")
    db.execute("ROLLBACK TO SAVEPOINT census_failed_apply")
    assert db.execute("SELECT count(*),count(*) FILTER (WHERE retired_on IS NOT NULL) FROM public.sec_foreign_share_census").fetchone() == (1, 0)
    assert db.execute("SELECT last_loaded_on FROM public.sec_foreign_share_census_sources").fetchone() == (date(2026, 10, 9),)
    assert at(db, "2025-04-16")[4][0]["shares"] == 100


def test_backdated_reconciliation_rejected_for_existing_and_new_sources(db):
    apply(db, observed="2026-10-10")
    for document in (source(parser="test-v2"), source(url="new.htm")):
        with pytest.raises(ValueError, match="precede prior load"):
            apply(db, document, observed="2026-10-09")
    assert db.execute("SELECT count(*) FROM public.sec_foreign_share_census_sources").fetchone() == (1,)


class ContractCursor:
    def __init__(self, *, body=None, owner="worker_writer", ownership_count=2):
        import re
        migration = Path(__file__).resolve().parents[1] / "schemas/sec_foreign_share_census_v1.sql"
        expected_body = re.search(r"AS \$fn\$(.*?)\$fn\$;", migration.read_text(encoding="utf-8"), re.S).group(1)
        self.body = expected_body if body is None else body
        self.owner, self.ownership_count = owner, ownership_count
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, *_args):
        self.queries.append(query)

    def fetchone(self):
        if "SELECT count(*) FROM pg_class" in self.queries[-1]:
            return (self.ownership_count,)
        return ("contract", "s", False, False, None, "sql", self.owner, self.body)

    def fetchall(self):
        return []


@pytest.mark.parametrize("drift", [{"body": "SELECT c.* FROM public.sec_foreign_share_census c"},
                                    {"owner": "postgres"}, {"ownership_count": 1}])
def test_schema_guard_refuses_stale_resolver_body_or_wrong_owners(drift):
    cursor = ContractCursor(**drift)
    with pytest.raises(ValueError, match="drifted"):
        loader.require_schema(cursor)


def test_apply_checks_contract_after_shared_lock_before_snapshot():
    cursor = ContractCursor()
    class Connection:
        def cursor(self):
            return cursor
    assert loader.apply_census(Connection(), manifest(), [], date(2026, 10, 10)) == {
        "inserted": 0, "retired": 0, "unchanged": 0, "sources": 0}
    assert cursor.queries[0] == "SELECT pg_advisory_xact_lock(79311, 173)"
    assert "obj_description" in cursor.queries[1]
    assert "SELECT count(*) FROM pg_class" in cursor.queries[2]
    assert "FROM public.sec_foreign_share_census_sources FOR UPDATE" in cursor.queries[3]


def test_census_apply_does_not_call_or_mutate_existing_w1c_evidence(db, monkeypatch):
    def refuse_evidence_apply(*_args, **_kwargs):
        pytest.fail("Census reconciliation invoked the existing W1c evidence apply")
    monkeypatch.setattr(w1c, "apply_evidence", refuse_evidence_apply)
    db.execute("CREATE TABLE public.sec_foreign_listing_evidence (proof text)")
    db.execute("INSERT INTO public.sec_foreign_listing_evidence VALUES ('existing W1c evidence')")
    apply(db)
    assert db.execute("SELECT proof FROM public.sec_foreign_listing_evidence").fetchall() == [("existing W1c evidence",)]


def test_new_loader_test_file_is_lf_only():
    assert b"\r" not in Path(__file__).read_bytes()
