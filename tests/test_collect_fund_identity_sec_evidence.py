"""Pure parts of scripts/collect_fund_identity_sec_evidence (no network)."""

from __future__ import annotations

import hashlib
import json

from scripts import collect_fund_identity_sec_evidence as collect
from scripts import repair_fund_identity_sec_v1 as repair


def test_targets_cover_every_input_a_plan_rests_on():
    plan = {
        "changes": [
            {"evidence": {repair.RULES[1]: {"class_id": "C1", "old_ticker": "OLD", "new_ticker": "NEW"}}},
            {"evidence": {repair.RULES[2]: {"terminated_class_id": "C2", "old_ticker": "DEADX",
                                            "new_ticker": "LIVEX"}}},
            {"evidence": {repair.RULES[4]: {"ticker": "VTI"}}},
            {"evidence": {repair.RULES[5]: {"series_ids": ["S1"]}}},
            {"evidence": {repair.RULES[7]: {"class_id": "C3", "old_class_id": "C4", "ticker": "VVPLX"}}},
        ],
        "review": {
            "orphan_no_current_tiingo_nav": [{"ticker": "FEOTX"}],
            "class_repoint_candidate": [{"terminated_class_id": "C5", "old_ticker": "PINUX",
                                         "new_ticker": "PINZX"}],
            "series_moved_unproven": [{"ticker": "STNC", "sec_class": "C6", "registry_class": "C7"}],
        },
    }
    targets = collect.targets_from_plan(plan)
    assert targets["tiingo"] == ["FEOTX", "LIVEX", "NEW", "PINZX", "VTI"]
    assert ("C1", "OLD") in targets["pairs"] and ("C1", "NEW") in targets["pairs"]
    assert {("C2", "DEADX"), ("C3", "VVPLX"), ("C4", "VVPLX"), ("C5", "PINUX"),
            ("C6", "STNC"), ("C7", "STNC")} <= set(targets["pairs"])
    assert targets["classes"] == ["C2", "C4", "C5"] and targets["series"] == ["S1"]
    assert targets["insurance"] == []


def test_insurance_targets_and_prospectus_parsing():
    plan = {"changes": [{"evidence": {repair.RULES[4]: {"series_id": "S9", "registrant_cik": "0000036405",
                                                        "ticker": "VTI"}}}],
            "review": {"orphan_insurance_status_unverified": [{"series_id": "S8", "registrant_cik": "0000918294",
                                                               "ticker": "QAAAJX"}]}}
    assert collect.targets_from_plan(plan)["insurance"] == [("S8", "0000918294"), ("S9", "0000036405")]
    page = ("<p>RISKS</p><p>T. ROWE PRICE QAOSWX All-Cap Opportunities Portfolio The fund is generally "
            "available only through variable annuity or variable life insurance contracts. Other text.</p>")
    assert collect.extract_restriction(page) == (
        "The fund is generally available only through variable annuity or variable life insurance contracts.")
    assert collect.extract_restriction("<p>Shares are offered to everyone.</p>") is None
    header = "&lt;SERIES-ID&gt;S000002081 x &lt;SERIES-ID&gt;S000002077 &lt;SERIES-ID&gt;S000002081"
    assert collect.header_series(header) == ["S000002077", "S000002081"]


def _datasets(path=None):
    import tempfile
    from pathlib import Path

    path = Path(path or tempfile.mkdtemp())
    for name, _sha in repair.SERIES_CLASS_FILES.values():
        (path / name).write_bytes(name.encode())
    (path / collect.TICKERS_JSON_NAME).write_bytes(b"{}")
    return path


def test_assemble_is_deterministic_and_loads_as_evidence(tmp_path):
    _datasets(tmp_path)
    filing = {"accessionNo": "0001-26-1", "formType": "497", "filedAt": "2026-09-29T00:00:00-04:00",
              "cik": "1", "classes": [{"series": "S1", "class": "C1", "ticker": "NEW", "name": "x"}]}
    sec = {"C1|NEW|first": {"filings": [filing]}, "C1|latest": {"filings": [filing]},
           "S1|series_latest": {"filings": [filing]}}
    tiingo = {"NEW": {"status": 200, "endDate": "2026-10-05", "observed_at": "2026-10-06T06:00:00+00:00"},
              "UNSTAMPED": {"status": 200}}
    ncen = {"cik|1": {"accession_no": "N1", "filed_at": "2026-03-12T16:00:00-04:00", "registrant_cik": "1",
                      "series": [{"series_id": "S1", "name": "x", "fund_types": ["Underlying fund"]}]},
            "series|S1": {"accession_no": "N0", "filed_at": "2025-03-12T16:00:00-04:00", "registrant_cik": "1",
                          "series": [{"series_id": "S1", "name": "x", "fund_types": []}]}}
    prospectus = {"cik|1": {"registrant_cik": "1", "match": {
        "accession_no": "P1", "form_type": "485BPOS", "filed_at": "2026-04-24", "document": "d.htm",
        "quote": "The fund is available only through variable annuity contracts.",
        "series_ids_header": ["S1"]}}, "cik|2": {"registrant_cik": "2", "match": None}}
    raw = collect.assemble(sec, tiingo, tmp_path, "2026-10-06", ncen, prospectus)
    assert raw == collect.assemble(dict(reversed(sec.items())), tiingo, tmp_path, "2026-10-06",
                                   dict(reversed(ncen.items())), prospectus)
    evidence = repair.parse_evidence(raw, hashlib.sha256(raw).hexdigest())
    assert set(evidence.tiingo) == {"NEW"}
    assert evidence.class_ticker_filings[("C1", "NEW")][0]["role"] == "first_with_ticker"
    assert evidence.class_last_filings["C1"]["accession_no"] == "0001-26-1"
    assert evidence.ncen_series["S1"]["accession_no"] == "N1"  # newest N-CEN wins
    assert evidence.insurance_prospectus["S1"][0]["accession_no"] == "P1"
    assert json.loads(raw)["sec_series_class_datasets"]["2026"]["sha256"] == hashlib.sha256(
        repair.SERIES_CLASS_FILES[2026][0].encode()).hexdigest()


def test_assemble_reads_multi_filing_prospectus_entries_and_refresh_keys():
    prospectus = {"cik|1": {"registrant_cik": "1", "wanted": ["S1", "S2"], "matches": [
        {"accession_no": "P2", "form_type": "485BPOS", "filed_at": "2026-04-24", "document": "a.htm",
         "quote": "The fund is available only through variable annuity contracts.",
         "series_ids_header": ["S1"]},
        {"accession_no": "P1", "form_type": "485BPOS", "filed_at": "2026-02-01", "document": None,
         "quote": None, "series_ids_header": ["S2"]}]}}
    doc = json.loads(collect.assemble({}, {}, _datasets(), "2026-10-06", {}, prospectus))
    assert [p["accession_no"] for p in doc["insurance_prospectus"]] == ["P2"]  # no sentence, no evidence
    assert collect._TIME_VARYING_SEC.search("C1|NEW|last") and collect._TIME_VARYING_SEC.search("S1|series_latest")
    assert not collect._TIME_VARYING_SEC.search("C1|NEW|first")


def test_an_exhausted_sec_query_aborts_collection(monkeypatch):
    import sys
    import types

    class Failing:
        def __init__(self, api_key):
            pass

        def get_filings(self, _query):
            raise Exception("API error: 500 - token=SECRET")

        get_data = get_filings

    monkeypatch.setitem(sys.modules, "sec_api", types.SimpleNamespace(
        QueryApi=Failing, FormNcenApi=Failing, FullTextSearchApi=Failing))
    monkeypatch.setattr(collect.time, "sleep", lambda _s: None)
    targets = {"pairs": [("C1", "OLD")], "classes": [], "series": [], "insurance": [("S1", "1")]}
    import pytest

    with pytest.raises(collect.CollectionError, match="sec_query_exhausted"):
        collect.collect_sec(targets, {}, lambda: None, api_key="k")
    with pytest.raises(collect.CollectionError, match="ncen_query_exhausted"):
        collect.collect_ncen(targets["insurance"], {}, lambda: None, api_key="k")


def _targets():
    return {"tiingo": ["NEW"], "pairs": [("C000000001", "OLD")], "classes": [], "series": ["S000000009"],
            "insurance": [("S000000001", "0000000042"), ("S000000002", "42")]}


def _complete_caches():
    sec = {"C000000001|latest": {}, "C000000001|OLD|first": {}, "C000000001|OLD|last": {},
           "S000000009|series_latest": {}}
    tiingo = {"NEW": {"status": 200, "observed_at": "2026-10-06T05:00:00+00:00"}}
    ncen = {"cik|42": {"series": [{"series_id": "S000000001"}]}, "series|S000000002": {}}
    prospectus = {"cik|42": {"wanted": ["S000000001", "S000000002"], "matches": []}}
    return sec, tiingo, ncen, prospectus


def test_every_planned_answer_must_be_cached_before_a_bundle_is_written(tmp_path, monkeypatch):
    sec, tiingo, ncen, prospectus = _complete_caches()
    assert collect.missing_cache_entries(_targets(), sec, tiingo, ncen, prospectus) == []
    del sec["C000000001|OLD|first"]
    tiingo["NEW"] = {"status": 429}
    del ncen["series|S000000002"]
    prospectus["cik|42"]["wanted"] = ["S000000001"]
    assert collect.missing_cache_entries(_targets(), sec, tiingo, ncen, prospectus) == [
        "C000000001|OLD|first", "tiingo|NEW", "ncen|series|S000000002", "prospectus|cik|42"]

    # --offline refuses instead of writing a bundle from a partial cache.
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"changes": [], "review": {}}))
    monkeypatch.setattr(collect, "targets_from_plan", lambda _plan: _targets())
    out = tmp_path / "bundle.json"
    code = collect.main(["--plan-file", str(plan), "--dataset-dir", str(tmp_path), "--cache-dir",
                         str(tmp_path), "--out", str(out), "--offline"])
    assert code == 2 and not out.exists()


def test_tiingo_retries_are_bounded(monkeypatch):
    import urllib.error

    def unreachable(*_a, **_k):
        raise urllib.error.URLError("down")

    monkeypatch.setattr(collect.urllib.request, "urlopen", unreachable)
    monkeypatch.setattr(collect.time, "sleep", lambda _s: None)
    cache = {}
    import pytest

    with pytest.raises(collect.CollectionError, match="tiingo_exhausted:NEW"):
        collect.collect_tiingo(["NEW"], cache, lambda: None, api_key="k", pace=0, refresh=False)
    assert cache == {}


def test_the_restriction_is_searched_in_every_document_of_the_filing(tmp_path, monkeypatch):
    import sys
    import types

    accession, folder = "0000000042-26-000001", "000000004226000001"
    docs = tmp_path / "edgar"
    docs.mkdir()
    (docs / f"{folder}_{accession}-index-headers.html").write_text("&lt;SERIES-ID&gt;S000000001")
    items = [{"name": f"exhibit{n}.htm", "size": str(1000 - n)} for n in range(8)]
    (docs / f"{folder}_index.json").write_text(json.dumps({"directory": {"item": items}}))
    for n in range(8):
        (docs / f"{folder}_exhibit{n}.htm").write_text("<p>Nothing restrictive here.</p>")
    (docs / f"{folder}_exhibit7.htm").write_text(
        "<p>Shares of the Fund are offered only to separate accounts of insurance companies "
        "to fund variable annuity contracts.</p>")

    class Search:
        def __init__(self, api_key):
            pass

        def get_filings(self, _query):
            return {"filings": [{"accessionNo": accession, "formType": "485BPOS",
                                 "filedAt": "2026-04-24T16:00:00-04:00"}]}

    monkeypatch.setitem(sys.modules, "sec_api", types.SimpleNamespace(FullTextSearchApi=Search))
    monkeypatch.setattr(collect.time, "sleep", lambda _s: None)
    cache = {}
    collect.collect_prospectus([("S000000001", "42")], cache, lambda: None, api_key="k",
                               user_agent="ua", doc_dir=docs)
    (match,) = cache["cik|42"]["matches"]
    assert match["document"] == "exhibit7.htm" and "separate accounts" in match["quote"]


def test_a_purchase_restriction_is_recognized():
    quote = collect.extract_restriction(
        "<p>Shares may be purchased only by insurance company separate accounts.</p>")
    assert quote and repair.prospectus_says_insurance_only(quote)


def test_prospectus_search_reads_every_result_page(tmp_path, monkeypatch):
    import sys
    import types

    accession, folder = "0000000042-26-000009", "000000004226000009"
    docs = tmp_path / "edgar"
    docs.mkdir()
    (docs / f"{folder}_{accession}-index-headers.html").write_text("&lt;SERIES-ID&gt;S000000001")
    (docs / f"{folder}_index.json").write_text(json.dumps({"directory": {"item": [
        {"name": "prospectus.htm", "size": "10"}]}}))
    (docs / f"{folder}_prospectus.htm").write_text(
        "<p>The Fund offers its shares only to separate accounts of insurance companies.</p>")
    old = [{"accessionNo": f"0000000042-24-{n:06d}", "formType": "485BPOS", "filedAt": "2024-02-01"}
           for n in range(100)]
    pages = []

    class Search:
        def __init__(self, api_key):
            pass

        def get_filings(self, query):
            pages.append(query["page"])
            if query["page"] == "1":  # relevance order: the newest filing is on page 2
                return {"total": {"value": 101}, "filings": old}
            return {"total": {"value": 101}, "filings": [
                {"accessionNo": accession, "formType": "485BPOS", "filedAt": "2026-04-24T16:00:00-04:00"}]}

    monkeypatch.setitem(sys.modules, "sec_api", types.SimpleNamespace(FullTextSearchApi=Search))
    monkeypatch.setattr(collect.time, "sleep", lambda _s: None)
    cache = {}
    collect.collect_prospectus([("S000000001", "42")], cache, lambda: None, api_key="k",
                               user_agent="ua", doc_dir=docs)
    assert "2" in pages
    (match,) = cache["cik|42"]["matches"]
    assert match["accession_no"] == accession and match["quote"]
