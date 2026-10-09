"""Discovery completeness, cache integrity and point-in-time binding regressions."""
from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from scripts import load_sec_foreign_listing_evidence as loader


def test_atomic_checkpoint_retries_transient_windows_replace(tmp_path, monkeypatch):
    destination = tmp_path / "manifest.json"
    destination.write_text('{"old":true}\n', encoding="utf-8", newline="\n")
    original_replace = Path.replace
    attempts = []
    monkeypatch.setattr(loader.time, "sleep", lambda _: None)
    def replace(path, target):
        attempts.append(path)
        if len(attempts) < 3:
            assert json.loads(destination.read_text()) == {"old": True}
            raise PermissionError("Simulated Windows read sharing")
        return original_replace(path, target)
    monkeypatch.setattr(Path, "replace", replace)
    loader.write_json(destination, {"complete": True})
    assert len(attempts) == 3
    assert len(set(attempts)) == 1
    assert json.loads(destination.read_text()) == {"complete": True}
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_checkpoint_permanent_failure_keeps_previous_file(tmp_path, monkeypatch):
    destination = tmp_path / "manifest.json"
    destination.write_text('{"old":true}\n', encoding="utf-8", newline="\n")
    attempts = []
    monkeypatch.setattr(loader.time, "sleep", lambda _: None)
    def replace(path, target):
        attempts.append(path)
        raise PermissionError("Persistent simulated sharing denial")
    monkeypatch.setattr(Path, "replace", replace)
    with pytest.raises(PermissionError):
        loader.write_json(destination, {"complete": True})
    assert len(attempts) == 12
    assert json.loads(destination.read_text()) == {"old": True}
    assert not list(tmp_path.glob("*.tmp"))


def test_concurrent_checkpoint_writers_have_unique_temporary_paths(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    destination = tmp_path / "shared-query.json"
    original_replace = Path.replace
    barrier = threading.Barrier(2)
    temporary_paths = []
    def replace(path, target):
        temporary_paths.append(path)
        barrier.wait(timeout=5)
        return original_replace(path, target)
    monkeypatch.setattr(Path, "replace", replace)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(loader.write_json, destination, {"writer": number}) for number in (1, 2)]
        for future in futures:
            future.result()
    assert len(set(temporary_paths)) == 2
    assert json.loads(destination.read_text())["writer"] in {1, 2}
    assert not list(tmp_path.glob("*.tmp"))


class SearchClient:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def search(self, url, payload):
        self.calls.append((url, payload))
        return next(self.responses)


def response(rows, total=None, relation="eq"):
    return {"total": {"value": len(rows) if total is None else total, "relation": relation}, "filings": rows}


def test_query_pagination_consumes_declared_total():
    client = SearchClient([response([{"accessionNo": "a"}], 3), response([{"accessionNo": "b"}]), response([{"accessionNo": "c"}])])
    rows = loader.search_filings(client, "cik:1", date(2020, 1, 1), date(2020, 12, 31))
    assert [row["accessionNo"] for row in rows] == ["a", "b", "c"]
    assert [payload["from"] for _, payload in client.calls] == ["0", "1", "2"]


def test_query_truncated_page_is_not_success():
    client = SearchClient([response([{"accessionNo": "a"}], 2), response([])])
    with pytest.raises(ValueError, match="before declared total"):
        loader.search_filings(client, "cik:1", date(2020, 1, 1), date(2020, 12, 31))


def test_query_repeated_page_is_not_success():
    client = SearchClient([response([{"accessionNo": "a"}], 2), response([{"accessionNo": "a"}])])
    with pytest.raises(ValueError, match="repeated accession"):
        loader.search_filings(client, "cik:1", date(2020, 1, 1), date(2020, 12, 31))


def test_query_splits_result_window_without_date_gap():
    client = SearchClient([response([], 10000, "gte"), response([{"accessionNo": "a"}]), response([{"accessionNo": "b"}])])
    rows = loader.search_filings(client, "cik:1", date(2020, 1, 1), date(2020, 1, 2))
    assert len(rows) == 2
    assert "[2020-01-01 TO 2020-01-01]" in client.calls[1][1]["query"]
    assert "[2020-01-02 TO 2020-01-02]" in client.calls[2][1]["query"]


def test_unpartitionable_window_fails_explicitly():
    client = SearchClient([response([], 10000, "gte")])
    with pytest.raises(ValueError, match="Single-day"):
        loader.search_filings(client, "cik:1", date(2020, 1, 1), date(2020, 1, 1))


def test_full_text_keeps_issuer_form_and_date_filters_each_page():
    client = SearchClient([response([{"filingUrl": "a"}], 2), response([{"filingUrl": "b"}])])
    rows = loader.search_text(client, '"ratio change"', ["6-K"], date(2010, 1, 1), date(2025, 12, 31), cik=123)
    assert len(rows) == 2
    for _, payload in client.calls:
        assert payload["ciks"] == ["123"]
        assert payload["formTypes"] == ["6-K"]
        assert payload["startDate"] == "2010-01-01"


def test_full_text_truncation_is_not_success():
    client = SearchClient([response([{"filingUrl": "a"}], 2), response([])])
    with pytest.raises(ValueError, match="before declared total"):
        loader.search_text(client, "ratio", ["6-K"], date(2020, 1, 1), date(2020, 12, 31))


def test_discovery_version_requires_explicit_refresh_and_retains_old_manifest(tmp_path, monkeypatch):
    universe = [{"cik": 123, "symbol": "ABC", "issuer_name": ""}]
    client = loader.SecClient(tmp_path, offline=True)
    monkeypatch.setattr(loader, "discover_issuer", lambda *_: [])
    manifest = loader.discover(client, universe, date(2020, 1, 1), date(2020, 12, 31), workers=1)
    manifest["discovery_version"] = "outdated-query-version"
    loader.write_json(tmp_path / "manifest.json", manifest)
    with pytest.raises(ValueError, match="refresh-discovery"):
        loader.discover(client, universe, date(2020, 1, 1), date(2020, 12, 31), workers=1)
    refreshed = loader.discover(client, universe, date(2020, 1, 1), date(2020, 12, 31), workers=1, refresh=True)
    assert refreshed["complete"]
    previous = list((tmp_path / "manifest-history").glob("*.json"))
    assert len(previous) == 1
    assert json.loads(previous[0].read_text())["discovery_version"] == "outdated-query-version"


def test_annual_query_tokenization_does_not_admit_late_notices(tmp_path, monkeypatch):
    def row(form):
        return {"accessionNo": "0001234567-20-000001", "formType": form, "filedAt": "2020-03-01",
                "linkToFilingDetails": "https://www.sec.gov/Archives/edgar/data/123/report.htm"}
    monkeypatch.setattr(loader, "search_filings", lambda *_: [row("NT 20-F")])
    monkeypatch.setattr(loader, "search_text", lambda *_, **__: [])
    assert loader.discover_issuer(loader.SecClient(tmp_path, offline=True),
                                  [{"cik": 123, "symbol": "ABC", "issuer_name": ""}],
                                  date(2020, 1, 1), date(2020, 12, 31)) == []


def test_securities_description_candidates_preserve_filing_date_and_role():
    url = "https://www.sec.gov/Archives/edgar/data/1046179/000119312525083423/d896993dex2a1.htm"
    filing = {"accessionNo": "0001193125-25-083423", "formType": "20-F", "filedAt": "2025-04-17",
              "documentFormatFiles": [
                  {"type": "EX-2.(A)(1)", "description": "EX-2.(A)(1)", "documentUrl": url},
                  {"type": "EX-2.2", "description": "Description of Registered Securities", "documentUrl": url.replace("ex2a1", "ex22")},
                  {"type": "EX-2.3", "description": "Debt indenture", "documentUrl": url.replace("ex2a1", "ex23")},
                  {"type": "EX-12.1", "description": "Description of Securities", "documentUrl": url.replace("ex2a1", "ex121")},
              ]}
    candidates = loader.securities_description_exhibits(filing)
    assert len(candidates) == 2
    assert all(row["document_role"] == "securities_description" and row["filedAt"] == "2025-04-17" for row in candidates)
    assert all(row["formType"] == "20-F" for row in candidates)
    assert candidates[0]["filingUrl"] == url
    assert loader.securities_description_exhibits({**filing, "formType": "6-K"}) == []


@pytest.mark.parametrize("text,expected", [
    ("<h1>Description of Securities</h1><p>American Depositary Shares each representing five ordinary shares</p>", True),
    ("Description of the Securities Registered under Section 12(b) of the Securities Exchange Act", True),
    ("Exhibit 2.1 Debt Indenture. American Depositary Shares appear in an incidental reference.", False),
    ("Description of Securities. No listed security or ADS description here.", False),
])
def test_description_attachment_needs_its_own_heading_and_security_context(text, expected):
    assert loader.verify_securities_description(text) is expected


def test_non_description_attachment_is_rejected_without_parse_failure(tmp_path):
    document = {"cik": 123, "adsh": "0001234567-20-000001", "source_url": "https://www.sec.gov/Archives/edgar/data/123/ex21.htm",
                "form": "20-F", "filed": "2020-03-01", "symbols": ["ABC"], "binding": "registrant_cik",
                "document_role": "securities_description"}
    updated, facts = loader.parse_document(loader.SecClient(tmp_path, offline=True), document,
                                          downloaded=(b"<h1>Loan agreement</h1>", "a" * 64))
    assert updated["status"] == "not_securities_description"
    assert updated["evidence_count"] == 0
    assert facts == []


def test_pdf_extraction_uses_owned_files_utf8_and_page_boundaries(tmp_path, monkeypatch):
    from types import SimpleNamespace
    calls = []
    monkeypatch.setattr(loader.shutil, "which", lambda _: "pdftotext")
    def run(command, **kwargs):
        calls.append((command, kwargs))
        assert Path(command[-2]).read_bytes().startswith(b"%PDF-")
        Path(command[-1]).write_bytes("Page one <script> & text\fPage two\f".encode())
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(loader.subprocess, "run", run)
    pages, method = loader.extract_pdf_pages(b"%PDF-1.4 untrusted input", cache_dir=tmp_path)
    assert pages == ["Page one <script> & text", "Page two"]
    assert method == "pdftotext-layout-utf8"
    command, kwargs = calls[0]
    assert command[:4] == ["pdftotext", "-layout", "-enc", "UTF-8"]
    assert Path(command[-2]).is_relative_to(tmp_path)
    assert not Path(command[-2]).exists() and not Path(command[-1]).exists()
    assert kwargs["shell"] is False
    html, normalized, ranges = loader.pdf_parser_input(pages)
    assert "&lt;script&gt;" in html and "&amp;" in html
    from scripts.sec_foreign_listing_parser import _Document
    assert _Document(html).text == normalized
    assert ranges == [(1, 0, len(pages[0]) + 1), (2, len(pages[0]) + 1, len(normalized))]


@pytest.mark.parametrize("returncode,output,match", [(1, b"", "exit code"), (0, b" \f", "no extractable text")])
def test_unreadable_pdf_is_an_explicit_error(tmp_path, monkeypatch, returncode, output, match):
    from types import SimpleNamespace
    monkeypatch.setattr(loader.shutil, "which", lambda _: "pdftotext")
    def run(command, **kwargs):
        Path(command[-1]).write_bytes(output)
        return SimpleNamespace(returncode=returncode)
    monkeypatch.setattr(loader.subprocess, "run", run)
    with pytest.raises(ValueError, match=match):
        loader.extract_pdf_pages(b"%PDF-1.4 fixture", cache_dir=tmp_path)


def test_pdf_ratio_evidence_keeps_original_hash_and_locatable_pages(tmp_path, monkeypatch):
    document = {"cik": 123, "adsh": "0001234567-20-000001", "source_url": "https://www.sec.gov/Archives/edgar/data/123/ex99.pdf",
                "form": "6-K", "filed": "2020-09-20", "symbols": ["ABC"], "binding": "registrant_cik"}
    pages = ["Background " * 100,
             "Ratio change. Effective October 1, 2020, each American Depositary Share represents five ordinary shares."]
    monkeypatch.setattr(loader, "extract_pdf_pages", lambda _, **kwargs: (pages, "test-converter"))
    raw = b"%PDF-1.4 original fixture bytes"
    updated, facts = loader.parse_document(loader.SecClient(tmp_path, offline=True), document,
                                          downloaded=(raw, loader.digest(raw)))
    assert updated["content_format"] == "pdf" and updated["pdf_pages"] == 2
    assert len(facts) == 1
    fact = facts[0]
    assert fact["source_sha256"] == loader.digest(raw)
    assert fact["ratio_numerator"] == 5 and fact["ratio_denominator"] == 1
    assert fact["effective_from"] == "2020-10-01"
    assert fact["evidence_location"].startswith("pdf/pages=1-2;normalized-text-span=")
    _, normalized, _ = loader.pdf_parser_input(pages)
    import re
    match = re.search(r"normalized-text-span=(\d+):(\d+)", fact["evidence_location"])
    assert normalized[int(match[1]):int(match[2])] == fact["evidence_text"]


def test_pdf_uses_pypdf_layout_when_no_native_converter(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import sys
    monkeypatch.setattr(loader.shutil, "which", lambda _: None)
    monkeypatch.setattr(Path, "is_file", lambda _: False)
    modes = []
    class Page:
        def extract_text(self, *, extraction_mode):
            modes.append(extraction_mode)
            return "Original PDF page text"
    def reader(stream):
        assert stream.read() == b"%PDF-original"
        return SimpleNamespace(pages=[Page(), Page()])
    monkeypatch.setitem(sys.modules, "pypdf", SimpleNamespace(PdfReader=reader))
    pages, method = loader.extract_pdf_pages(b"%PDF-original", cache_dir=tmp_path)
    assert pages == ["Original PDF page text", "Original PDF page text"]
    assert modes == ["layout", "layout"]
    assert method == "pypdf-layout"


def test_pdf_missing_extraction_tools_is_explicit(tmp_path, monkeypatch):
    import sys
    monkeypatch.setattr(loader.shutil, "which", lambda _: None)
    monkeypatch.setattr(Path, "is_file", lambda _: False)
    monkeypatch.setitem(sys.modules, "pypdf", None)
    with pytest.raises(RuntimeError, match="requires pdftotext"):
        loader.extract_pdf_pages(b"%PDF-original", cache_dir=tmp_path)


def test_search_cache_integrity_and_offline_replay(tmp_path, monkeypatch):
    client = loader.SecClient(tmp_path, "not-a-real-key")
    monkeypatch.setattr(client, "request", lambda *_: json.dumps(response([])).encode())
    payload = {"query": "cik:123"}
    assert client.search(loader.QUERY_URL, payload)["filings"] == []
    offline = loader.SecClient(tmp_path, offline=True)
    assert offline.search(loader.QUERY_URL, payload)["filings"] == []
    path = next((tmp_path / "search").glob("*.json"))
    envelope = json.loads(path.read_text())
    envelope["response"]["filings"] = [{"tampered": True}]
    loader.write_json(path, envelope)
    with pytest.raises(ValueError, match="hash mismatch"):
        offline.search(loader.QUERY_URL, payload)


def test_document_cache_rejects_tampering_and_untrusted_hosts(tmp_path, monkeypatch):
    client = loader.SecClient(tmp_path, "fake")
    monkeypatch.setattr(client, "request", lambda *_: b"<html>" + b"evidence " * 100 + b"</html>")
    url = "https://www.sec.gov/Archives/edgar/data/123/000123456725000001/report.htm"
    _, sha = client.document(url)
    offline = loader.SecClient(tmp_path, offline=True)
    assert offline.document(url, expected_sha256=sha)[1] == sha
    path = next((tmp_path / "documents").glob("*.bin.xz"))
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="integrity mismatch"):
        offline.document(url)
    with pytest.raises(ValueError, match="not an SEC"):
        client.document("https://example.org/private")


def test_compressed_cache_preserves_original_bytes_and_legacy_cache(tmp_path, monkeypatch):
    import gzip
    import lzma
    client = loader.SecClient(tmp_path, "fake")
    raw = b"<html>" + b"original SEC evidence " * 1000 + b"</html>"
    monkeypatch.setattr(client, "request", lambda *_: raw)
    url = "https://www.sec.gov/Archives/edgar/data/123/report.htm"
    assert client.document(url) == (raw, loader.digest(raw))
    compressed = next((tmp_path / "documents").glob("*.bin.xz"))
    assert lzma.decompress(compressed.read_bytes()) == raw
    assert compressed.stat().st_size < len(raw) / 5
    gzip_path = compressed.with_suffix(".gz")
    gzip_path.write_bytes(gzip.compress(raw, mtime=0))
    compressed.unlink()
    assert loader.SecClient(tmp_path, offline=True).document(url) == (raw, loader.digest(raw))
    legacy = compressed.with_suffix("")
    legacy.write_bytes(raw)
    gzip_path.unlink()
    assert loader.SecClient(tmp_path, offline=True).document(url) == (raw, loader.digest(raw))


def test_low_disk_stops_new_downloads_without_network(tmp_path, monkeypatch):
    from collections import namedtuple
    usage = namedtuple("usage", "total used free")
    monkeypatch.setattr(loader.shutil, "disk_usage", lambda *_: usage(100, 99, 1))
    client = loader.SecClient(tmp_path, "fake")
    monkeypatch.setattr(client, "request", lambda *_: pytest.fail("No download is allowed when cache disk is full"))
    with pytest.raises(OSError, match="Insufficient cache disk space"):
        client.document("https://www.sec.gov/Archives/edgar/data/123/report.htm")
    assert client.disk_full.is_set()


def test_sec_inline_viewer_url_has_one_canonical_source_and_cache(tmp_path, monkeypatch):
    path = "/Archives/edgar/data/313216/000031321618000007/report.htm"
    viewer = "https://www.sec.gov/ix?doc=" + path
    canonical = "https://www.sec.gov" + path
    assert loader.canonical_sec_url(viewer) == canonical
    assert loader.canonical_sec_url("https://www.sec.gov/ix.xhtml?doc=%2FArchives%2Fedgar%2Fdata%2F313216%2F000031321618000007%2Freport.htm") == canonical
    source = {"cik": 313216, "adsh": "0000313216-18-000007", "source_url": viewer}
    normalized = loader.canonical_document(source)
    assert normalized["source_package"] == loader.canonical_document({**source, "source_url": canonical})["source_package"]
    assert normalized["discovered_source_url"] == viewer
    calls = []
    client = loader.SecClient(tmp_path, "fake")
    def request(url):
        calls.append(url)
        return b"<html>" + b"original " * 100 + b"</html>"
    monkeypatch.setattr(client, "request", request)
    assert client.document(viewer) == client.document(canonical)
    assert len(calls) == 1
    assert calls[0] == "https://archive.sec-api.io/313216/000031321618000007/report.htm"


@pytest.mark.parametrize("url", [
    "https://www.sec.gov/ix?doc=https://example.org/Archives/edgar/data/123/report.htm",
    "https://www.sec.gov/ix?doc=//example.org/Archives/edgar/data/123/report.htm",
    "https://www.sec.gov/ix?doc=/Archives/edgar/data/123/../private",
    "https://www.sec.gov/ix?doc=/Archives/edgar/data/123/%2E%2E/private",
    "https://www.sec.gov/ix?doc=/Archives/edgar/data/123/a&doc=/Archives/edgar/data/123/b",
    "https://www.sec.gov/ix?url=/Archives/edgar/data/123/report.htm",
    "https://www.sec.gov@evil.example/ix?doc=/Archives/edgar/data/123/report.htm",
])
def test_sec_inline_viewer_cannot_redirect_outside_edgar(url):
    with pytest.raises(ValueError):
        loader.canonical_sec_url(url)


def test_manifest_replay_canonicalizes_existing_viewer_identity(tmp_path, monkeypatch):
    viewer = "https://www.sec.gov/ix?doc=/Archives/edgar/data/123/000123456720000001/report.htm"
    canonical = loader.canonical_sec_url(viewer)
    raw_document = {"cik": 123, "adsh": "0001234567-20-000001", "source_url": viewer,
                    "source_package": "old-viewer-key", "binding": "registrant_cik", "symbols": ["ABC"],
                    "filed": "2020-03-01", "form": "20-F"}
    manifest = {"complete": True, "documents": [raw_document, {**raw_document, "source_url": canonical}]}
    client = loader.SecClient(tmp_path, offline=True)
    monkeypatch.setattr(client, "document", lambda *_args, **_kwargs: (b"content", "a" * 64))
    def parse_document(_client, document, _observations, *, downloaded):
        assert downloaded == (b"content", "a" * 64)
        assert document["source_url"] == canonical
        assert document["source_package"] == loader.canonical_document(raw_document)["source_package"]
        return {**document, "source_sha256": "a" * 64, "status": "parsed", "evidence_count": 0}, []
    monkeypatch.setattr(loader, "parse_document", parse_document)
    summary = loader.parse_manifest(client, manifest, tmp_path / "evidence.jsonl", workers=1)
    assert summary["documents"] == 1
    assert summary["parse_complete"]


def test_pipeline_downloads_shared_url_once_and_reparses_deterministically(tmp_path, monkeypatch):
    url = "https://www.sec.gov/Archives/edgar/data/123/000123456720000001/report.htm"
    base = {"adsh": "0001234567-20-000001", "source_url": url, "source_package": "old-key",
            "binding": "registrant_cik", "symbols": ["ABC"], "filed": "2020-03-01", "form": "20-F"}
    documents = [{**base, "cik": 123}, {**base, "cik": 456}, {**base, "cik": 789, "source_url": url.replace("report.htm", "second.htm")}]
    manifest = {"complete": True, "documents": documents}
    client = loader.SecClient(tmp_path, offline=True)
    downloads = []
    parse_calls = []
    generation = [1]
    fail_cik = [None]

    def download(source_url, **_kwargs):
        downloads.append(source_url)
        return b"same-original-bytes", "a" * 64

    def parse(_client, document, _observations, *, downloaded):
        assert downloaded == (b"same-original-bytes", "a" * 64)
        parse_calls.append(document["cik"])
        if document["cik"] == fail_cik[0]:
            raise ValueError("Intentional parser failure")
        row = {"cik": document["cik"], "source_package": document["source_package"], "revision": generation[0]}
        return {**document, "status": "parsed", "evidence_count": 1, "source_sha256": "a" * 64}, [row]

    monkeypatch.setattr(client, "document", download)
    monkeypatch.setattr(loader, "parse_document", parse)
    output = tmp_path / "evidence.jsonl"
    summary = loader.parse_manifest(client, manifest, output, workers=2)
    assert summary["evidence_rows"] == 3
    assert len(downloads) == 2  # two issuer candidates share one original
    first_hash = manifest["evidence_sha256"]
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert [row["source_package"] for row in rows] == sorted(row["source_package"] for row in rows)
    generation[0] = 2
    loader.parse_manifest(client, manifest, output, workers=2)
    assert len(parse_calls) == 6
    assert manifest["evidence_sha256"] != first_hash
    assert {json.loads(line)["revision"] for line in output.read_text().splitlines()} == {2}
    # Old spool data for a source that now fails must never leak into output.
    fail_cik[0] = 456
    summary = loader.parse_manifest(client, manifest, output, workers=2)
    assert summary["failed_documents"] == 1
    assert not summary["parse_complete"]
    assert {json.loads(line)["cik"] for line in output.read_text().splitlines()} == {123, 789}


def test_f6_search_name_is_not_sufficient_issuer_binding():
    text = "ACME Corp (Exact name of issuer of deposited securities) OtherCo Ltd is a customer"
    assert loader.verify_f6_issuer(text, "ACME Corp")
    assert not loader.verify_f6_issuer(text, "OtherCo Ltd")
    assert not loader.verify_f6_issuer("ACME Corp is mentioned somewhere", "ACME Corp")


def _observation(**updates):
    return {"cik": 123, "adsh": "0001234567-20-000001", "ticker": "OLD", "security_kind": "equity",
            "filing_complete": True, "available_on": "2020-03-01", "retired_on": None, **updates}


def _type_row():
    return {"symbol": None, "evidence_kind": "listed_type", "evidence_location": "cover table", "evidence_text": "Common shares NYSE"}


def test_historical_symbol_binding_requires_same_accession_and_public_evidence():
    document = {"cik": 123, "adsh": "0001234567-20-000001", "filed": "2020-03-01"}
    rows = loader.bind_historical_symbols([_type_row()], document, [_observation()])
    assert rows[0]["symbol"] == "OLD"
    for wrong in [_observation(adsh="0001234567-25-000001", ticker="NEW"),
                  _observation(available_on="2021-01-01"), _observation(retired_on="2020-03-01"),
                  _observation(security_kind="debt"), _observation(filing_complete=False)]:
        assert loader.bind_historical_symbols([_type_row()], document, [wrong])[0]["symbol"] is None


def test_historical_symbol_binding_does_not_choose_between_classes():
    document = {"cik": 123, "adsh": "0001234567-20-000001", "filed": "2020-03-01"}
    result = loader.bind_historical_symbols([_type_row()], document, [_observation(), _observation(ticker="OTHER")])
    assert result[0]["symbol"] is None


def test_issuer_ratios_are_never_blindly_assigned_to_historical_symbol():
    document = {"cik": 123, "adsh": "0001234567-20-000001", "filed": "2020-03-01"}
    row = {"symbol": None, "evidence_kind": "ads_ratio"}
    assert loader.bind_historical_symbols([row], document, [_observation()])[0]["symbol"] is None


def test_incomplete_manifest_never_reconciles():
    with pytest.raises(ValueError, match="complete discovery"):
        loader.apply_evidence(None, {"complete": False, "parse_complete": True}, [], date.today())


def test_unmanifested_evidence_never_reconciles():
    with pytest.raises(ValueError, match="unmanifested source"):
        loader.apply_evidence(None, {"complete": True, "parse_complete": True, "documents": []},
                              [{"source_package": "injected"}], date.today())


@pytest.mark.parametrize("metadata,expected", [
    ({}, (None, True)),
    ({"underlying_class": "class_a", "ordinary_candidate": True}, ("class_a", True)),
    ({"underlying_class": "series_b", "ordinary_candidate": False}, ("series_b", False)),
])
def test_apply_preserves_class_and_ordinary_candidate_metadata(metadata, expected):
    class Cursor:
        rowcount = 0

        def __init__(self):
            self.inserted = None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, query, parameters=()):
            if query.startswith("INSERT INTO public.sec_foreign_listing_evidence"):
                self.inserted = dict(zip(loader.FACT_COLUMNS, parameters))

        def executemany(self, query, parameters):
            for values in parameters:
                self.execute(query, values)

        def fetchall(self):
            return []

        def fetchone(self):
            return None

    class Connection:
        def __init__(self):
            self.result = Cursor()

        def cursor(self):
            return self.result

    source = _source()
    fact = {**_fact(source), **metadata}
    connection = Connection()
    loader.apply_evidence(connection, _manifest(source), [fact], date(2026, 10, 9))
    actual = connection.result.inserted
    assert (actual["underlying_class"], actual["ordinary_candidate"]) == expected


def test_load_key_never_needs_a_secret_cli_argument(tmp_path, monkeypatch):
    monkeypatch.delenv("SEC_API_IO_KEY", raising=False)
    monkeypatch.delenv("SEC_API_KEY", raising=False)
    path = tmp_path / "source.env"
    path.write_text('SEC_API_KEY="test-secret"\n', encoding="utf-8", newline="\n")
    assert loader.load_key(path) == "test-secret"
    monkeypatch.setenv("SEC_API_IO_KEY", "preferred")
    assert loader.load_key(path) == "preferred"


@pytest.fixture
def db():
    dsn = os.environ.get("SEC_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SEC_TEST_DATABASE_URL is unset")
    assert urlsplit(dsn).hostname in {"127.0.0.1", "localhost"}, "Tests require disposable local Postgres"
    psycopg = pytest.importorskip("psycopg")
    connection = psycopg.connect(dsn)
    connection.execute("TRUNCATE public.sec_foreign_listing_evidence, public.sec_foreign_listing_sources")
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


def _source(package="source-a", adsh="0001234567-20-000001", count=1):
    return {"source_package": package, "cik": 123, "adsh": adsh, "evidence_count": count,
            "source_url": "https://www.sec.gov/Archives/edgar/data/123/report.htm",
            "source_sha256": "a" * 64, "parser_version": "test-v1"}


def _fact(source, *, listed_type="ordinary_direct", revision="1"):
    row = {**source, "symbol": "ABC", "form": "20-F", "filed": "2020-03-01",
           "source_kind": "cover_12b", "evidence_kind": "listed_type", "listed_type": listed_type,
           "ratio_numerator": None, "ratio_denominator": None, "effective_from": "2020-03-02",
           "effective_to": None, "evidence_text": "Common Shares NYSE revision " + revision,
           "evidence_location": "cover table", "available_on": "2020-03-02"}
    row.pop("evidence_count")
    row["fact_hash"] = loader.hashlib.md5(loader.canonical_json(row).encode(), usedforsecurity=False).hexdigest()
    return row


def _manifest(*sources):
    return {"complete": True, "parse_complete": True, "documents": list(sources)}


def test_initial_batch_multiple_exhibits_preserves_original_availability(db):
    first, second = _source(), _source("source-b")
    result = loader.apply_evidence(db, _manifest(first, second), [_fact(first), _fact(second)], date(2026, 10, 9))
    assert result["inserted"] == 2
    assert db.execute("SELECT DISTINCT available_on FROM public.sec_foreign_listing_evidence").fetchall() == [(date(2020, 3, 2),)]
    replay = loader.apply_evidence(db, _manifest(first, second), [_fact(first), _fact(second)], date(2026, 10, 10))
    assert replay == {"inserted": 0, "retired": 0, "unchanged": 2, "sources": 2}


def test_new_exhibit_after_zero_fact_accession_is_a_correction(db):
    first, later = _source(count=0), _source("new-exhibit")
    loader.apply_evidence(db, _manifest(first), [], date(2026, 10, 9))
    loader.apply_evidence(db, _manifest(first, later), [_fact(later)], date(2026, 10, 10))
    assert db.execute("SELECT available_on FROM public.sec_foreign_listing_evidence").fetchone()[0] == date(2026, 10, 10)
    assert db.execute("SELECT status FROM public.sec_foreign_listing_at(123,'ABC','2025-12-31')").fetchone()[0] == "none"


def test_correction_retains_previous_version_and_withdrawal_is_dated(db):
    source = _source()
    loader.apply_evidence(db, _manifest(source), [_fact(source)], date(2026, 10, 9))
    loader.apply_evidence(db, _manifest(source), [_fact(source, listed_type="ads", revision="2")], date(2026, 10, 10))
    rows = db.execute("SELECT listed_type,available_on,retired_on FROM public.sec_foreign_listing_evidence ORDER BY id").fetchall()
    assert rows == [("ordinary_direct", date(2020, 3, 2), date(2026, 10, 10)), ("ads", date(2026, 10, 10), None)]
    assert db.execute("SELECT listed_type FROM public.sec_foreign_listing_at(123,'ABC','2025-12-31')").fetchone()[0] == "ordinary_direct"
    loader.apply_evidence(db, _manifest(_source(count=0)), [], date(2026, 10, 11))
    assert db.execute("SELECT status FROM public.sec_foreign_listing_at(123,'ABC','2026-10-11')").fetchone()[0] == "none"


def test_reconciliation_rejects_backdated_source_observation(db):
    source = _source()
    loader.apply_evidence(db, _manifest(source), [_fact(source)], date(2026, 10, 9))
    with pytest.raises(ValueError, match="cannot precede"):
        loader.apply_evidence(db, _manifest(source), [_fact(source)], date(2026, 10, 8))


def test_apply_reads_snapshots_once_and_bounds_write_batches(monkeypatch):
    monkeypatch.setattr(loader, "APPLY_BATCH_SIZE", 2)
    class Cursor:
        rowcount = 0

        def __init__(self):
            self.reads = []
            self.batches = []

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, query, parameters=()):
            self.reads.append(query)

        def executemany(self, query, parameters):
            self.batches.append((query, len(parameters)))

        def fetchall(self):
            return []

    class Connection:
        def __init__(self):
            self.result = Cursor()

        def cursor(self):
            return self.result

    sources = [_source(f"package-{number}") for number in range(5)]
    connection = Connection()
    result = loader.apply_evidence(connection, _manifest(*sources), [_fact(source) for source in sources], date(2026, 10, 9))
    assert result == {"inserted": 5, "retired": 0, "unchanged": 0, "sources": 5}
    assert len(connection.result.reads) == 3  # advisory lock + two snapshots
    fact_batches = [size for query, size in connection.result.batches if "INSERT INTO public.sec_foreign_listing_evidence" in query]
    source_batches = [size for query, size in connection.result.batches if "INSERT INTO public.sec_foreign_listing_sources" in query]
    assert fact_batches == source_batches == [2, 2, 1]


def test_batched_corrections_preserve_retirement_counts(db, monkeypatch):
    monkeypatch.setattr(loader, "APPLY_BATCH_SIZE", 2)
    sources = [_source(f"package-{number}") for number in range(5)]
    loader.apply_evidence(db, _manifest(*sources), [_fact(source) for source in sources], date(2026, 10, 9))
    result = loader.apply_evidence(db, _manifest(*sources), [_fact(source, revision="2") for source in sources], date(2026, 10, 10))
    assert result == {"inserted": 5, "retired": 5, "unchanged": 0, "sources": 5}
    assert db.execute("SELECT count(*) FROM public.sec_foreign_listing_evidence WHERE retired_on IS NULL").fetchone()[0] == 5
    assert db.execute("SELECT count(*) FROM public.sec_foreign_listing_evidence WHERE retired_on='2026-10-10'").fetchone()[0] == 5


def test_late_validation_failure_rolls_back_already_flushed_batches(db, monkeypatch):
    monkeypatch.setattr(loader, "APPLY_BATCH_SIZE", 1)
    first, bad = _source(), _source("late-invalid-source", count=2)
    with pytest.raises(ValueError, match="Evidence count"):
        with db.transaction():
            loader.apply_evidence(db, _manifest(first, bad), [_fact(first), _fact(bad)], date(2026, 10, 9))
    assert db.execute("SELECT count(*) FROM public.sec_foreign_listing_evidence").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM public.sec_foreign_listing_sources").fetchone()[0] == 0


def _completed_shard_artifacts(tmp_path):
    from scripts import run_sec_foreign_listing_evidence_shards as shards
    documents = []
    for number in range(6):
        source = {**_source(), "source_url": f"https://www.sec.gov/Archives/edgar/data/123/report-{number}.htm",
                  "form": "20-F", "filed": "2020-03-01", "binding": "registrant_cik", "symbols": ["ABC"]}
        documents.append(loader.canonical_document(source))
    assert {shards.partition(document) for document in documents} == {0, 1}
    parent = {"complete": True, "universe_sha256": "a" * 64, "documents": documents}
    loader.write_json(tmp_path / "manifest.json", parent)
    result = shards.prepare(tmp_path)
    for part in range(2):
        directory = tmp_path / "parts" / str(part)
        child = json.loads((directory / "input-manifest.json").read_text())
        child["parse_complete"] = True
        child["observations_sha256"] = "b" * 64
        rows = []
        for document in child["documents"]:
            document["status"] = "parsed"
            rows.append(_fact(document))
        rows.sort(key=lambda row: row["source_package"])
        artifact = directory / "evidence.jsonl"
        loader.write_bytes(artifact, "".join(loader.canonical_json(row) + "\n" for row in rows).encode())
        child["evidence_count"] = len(rows)
        child["evidence_sha256"] = shards.file_sha256(artifact)
        loader.write_json(directory / "manifest.json", child)
    return shards, parent, result


def test_shard_combine_exact_coverage_and_deterministic_order(tmp_path):
    shards, parent, preparation = _completed_shard_artifacts(tmp_path)
    output = tmp_path / "combined.jsonl"
    summary = shards.combine(tmp_path, output)
    first = output.read_bytes()
    assert summary["documents"] == summary["evidence_rows"] == len(parent["documents"])
    rows = [json.loads(line) for line in first.splitlines()]
    assert [row["source_package"] for row in rows] == sorted(document["source_package"] for document in parent["documents"])
    combined = json.loads((tmp_path / "manifest.json").read_text())
    assert combined["complete"] and combined["parse_complete"]
    assert combined["universe_sha256"] == parent["universe_sha256"]
    assert combined["observations_sha256"] == "b" * 64
    assert combined["shard_provenance"]["parent_manifest_sha256"] == preparation["parent_manifest_sha256"]
    shards.combine(tmp_path, output)
    assert output.read_bytes() == first
    for part in range(2):
        child = json.loads((tmp_path / "parts" / str(part) / "manifest.json").read_text())
        assert child["complete"] is False
        with pytest.raises(ValueError, match="complete discovery"):
            loader.apply_evidence(None, child, [], date.today())


@pytest.mark.parametrize("mutation,match", [
    ("missing", "coverage"), ("duplicate", "coverage"), ("hash", "artifact hash"),
    ("parent", "parent identity"), ("count", "per-document counts"),
    ("incomplete", "incomplete"), ("observations", "observation hashes"),
])
def test_shard_combine_rejects_incomplete_or_changed_artifacts(tmp_path, mutation, match):
    shards, _, _ = _completed_shard_artifacts(tmp_path)
    child_path = tmp_path / "parts" / "0" / "manifest.json"
    child = json.loads(child_path.read_text())
    if mutation == "missing":
        child["documents"].pop()
    elif mutation == "duplicate":
        child["documents"].append(child["documents"][0])
    elif mutation == "hash":
        with (child_path.parent / "evidence.jsonl").open("ab") as handle:
            handle.write(b" ")
    elif mutation == "parent":
        child["shard"]["parent_manifest_sha256"] = "c" * 64
    elif mutation == "count":
        child["documents"][0]["evidence_count"] += 1
    elif mutation == "incomplete":
        child["parse_complete"] = False
    elif mutation == "observations":
        child["observations_sha256"] = "d" * 64
    loader.write_json(child_path, child)
    output = tmp_path / "combined.jsonl"
    output.write_bytes(b"previous-artifact\n")
    with pytest.raises(ValueError, match=match):
        shards.combine(tmp_path, output)
    assert output.read_bytes() == b"previous-artifact\n"


def test_shard_partition_keeps_all_issuer_bindings_of_same_url_together():
    from scripts import run_sec_foreign_listing_evidence_shards as shards
    source = {"source_url": "https://www.sec.gov/Archives/edgar/data/123/report.htm", "cik": 123}
    assert shards.partition(source) == shards.partition({**source, "cik": 456})
