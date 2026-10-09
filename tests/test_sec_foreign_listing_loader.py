"""Discovery completeness, cache integrity and point-in-time binding regressions."""
from __future__ import annotations

from datetime import date, timedelta
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from scripts import load_sec_foreign_listing_evidence as loader


def _date_authority(filed="2020-03-01", adsh="0001234567-20-000001", publication_floor_on=None):
    """Explicit synthetic W1 date evidence for tests unrelated to discovery."""
    floor = publication_floor_on or filed
    return {"filed": filed, "filing_date_status": "resolved", "query_accepted_on": floor,
            "publication_floor_on": floor,
            "publication_floor_proof": {"source": "discovery_reported_publication_floor", "publication_floor_on": floor},
            "filing_date_proof": {"source": "w1_same_accession", "filed": filed,
                                  "records": [{"cik": 123, "adsh": adsh, "filed": filed}]}}


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
    first_attempts = set()
    attempt_lock = threading.Lock()
    def replace(path, target):
        # Windows may retry an actual replace after the competing writer exits.
        # Synchronize the two first attempts, not a retry with no remaining peer.
        with attempt_lock:
            first_attempt = threading.get_ident() not in first_attempts
            if first_attempt:
                first_attempts.add(threading.get_ident())
                temporary_paths.append(path)
        if first_attempt:
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
    base = "https://www.sec.gov/Archives/edgar/data/123/"
    client = SearchClient([response([{"accessionNo": "a", "filingUrl": base + "a.htm"}], 2),
                           response([{"accessionNo": "b", "filingUrl": base + "b.htm"}])])
    rows = loader.search_text(client, '"ratio change"', ["6-K"], date(2010, 1, 1), date(2025, 12, 31), cik=123)
    assert len(rows) == 2
    for _, payload in client.calls:
        assert payload["ciks"] == ["123"]
        assert payload["formTypes"] == ["6-K"]
        assert payload["startDate"] == "2010-01-01"


def test_full_text_truncation_is_not_success():
    client = SearchClient([response([{"accessionNo": "a", "filingUrl": "https://www.sec.gov/Archives/edgar/data/123/a.htm"}], 2), response([])])
    with pytest.raises(ValueError, match="before declared total"):
        loader.search_text(client, "ratio", ["6-K"], date(2020, 1, 1), date(2020, 12, 31))


@pytest.mark.parametrize("same_page", [False, True])
def test_full_text_repeated_hit_cannot_satisfy_declared_total(same_page):
    hit = {"accessionNo": "a", "filingUrl": "https://www.sec.gov/Archives/edgar/data/123/a.htm", "type": "6-K"}
    pages = [response([hit, hit], 2)] if same_page else [response([hit], 2), response([hit])]
    with pytest.raises(ValueError, match="repeated accession/document"):
        loader.search_text(SearchClient(pages), "ratio", ["6-K"], date(2020, 1, 1), date(2020, 12, 31))


def test_full_text_retains_distinct_same_accession_exhibits_in_declared_total():
    hit = {"accessionNo": "a", "filingUrl": "https://www.sec.gov/Archives/edgar/data/123/a.txt", "type": "6-K"}
    exhibit = {**hit, "type": "EX-99.1"}
    other_exhibit = {**hit, "filingUrl": hit["filingUrl"].replace("a.txt", "b.htm"), "type": "EX-99.1"}
    client = SearchClient([response([hit], 3), response([exhibit, other_exhibit])])
    assert loader.search_text(client, "ratio", ["6-K"], date(2020, 1, 1), date(2020, 12, 31)) == [hit, exhibit, other_exhibit]


def test_full_text_overstated_unique_page_is_not_success():
    hits = [{"accessionNo": name, "filingUrl": f"https://www.sec.gov/Archives/edgar/data/123/{name}.htm"} for name in ("a", "b")]
    with pytest.raises(ValueError, match="do not match declared total"):
        loader.search_text(SearchClient([response(hits, 1)]), "ratio", ["6-K"], date(2020, 1, 1), date(2020, 12, 31))


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


@pytest.mark.parametrize("form", ["40FR12B", "40FR12B/A"])
def test_canadian_registration_discovery_reaches_supported_parser(tmp_path, monkeypatch, form):
    from scripts.sec_foreign_listing_parser import parse_filing
    def search(client, query, start, end):
        assert f'formType:"{form}"' in query
        return [{"accessionNo": "0000000123-20-000001", "formType": form,
                 "filedAt": "2020-03-01", "cik": "123",
                 "filingUrl": "https://www.sec.gov/Archives/edgar/data/123/registration.htm"}]
    monkeypatch.setattr(loader, "search_filings", search)
    monkeypatch.setattr(loader, "search_text", lambda *_, **__: [])
    documents = loader.discover_issuer(loader.SecClient(tmp_path, offline=True),
                                      [{"cik": 123, "symbol": "TEST", "issuer_name": ""}],
                                      date(2020, 1, 1), date(2020, 12, 31))
    assert len(documents) == 1 and documents[0]["form"] == form
    rows = parse_filing(
        '<p>Securities registered pursuant to Section 12(b) of the Act:</p>'
        '<table><tr><th>Title of each class</th><th>Trading Symbol</th>'
        '<th>Name of each exchange on which registered</th></tr>'
        '<tr><td>Common shares</td><td>TEST</td><td>NYSE</td></tr></table>',
        cik=123, form_type=form, accession_number=documents[0]["adsh"],
        filing_date=documents[0]["filed"], source_url=documents[0]["source_url"])
    assert any(row["listed_type"] == "ordinary_direct" for row in rows)


@pytest.mark.parametrize("term", ["ADS", "ADR", "GDR", "GDS", "depositary shares",
                                  "depositary receipts", "depository shares", "depository receipts",
                                  "depositary share", "depositary receipt", "depository share", "depository receipt",
                                  "American shares", "American share"])
def test_ratio_change_discovery_covers_parser_depositary_terms(tmp_path, monkeypatch, term):
    from scripts.sec_foreign_listing_parser import parse_filing
    monkeypatch.setattr(loader, "search_filings", lambda *_, **__: [])
    def search(client, query, forms, start, end, *, cik):
        # Model a filing returned only when the query includes its terminology.
        assert term in query
        assert tuple(forms) == ("6-K", "6-K/A") and cik == 123
        if term.startswith("American share"):
            assert "receipts" in query
        return [{"accessionNo": "0000000123-20-000001", "formType": "6-K",
                 "filedAt": "2020-03-01", "cik": "123",
                 "filingUrl": "https://www.sec.gov/Archives/edgar/data/123/change.htm"}]
    monkeypatch.setattr(loader, "search_text", search)
    documents = loader.discover_issuer(loader.SecClient(tmp_path, offline=True),
                                      [{"cik": 123, "symbol": "TEST", "issuer_name": ""}],
                                      date(2020, 1, 1), date(2020, 12, 31))
    assert len(documents) == 1
    wording = term + " (evidenced by depositary receipts)" if term.startswith("American share") else term
    rows = parse_filing(f'The ratio will change: {wording}, each representing five ordinary shares. '
                       'The ratio change is effective on March 5, 2020.',
                       cik=123, form_type="6-K", accession_number=documents[0]["adsh"],
                       filing_date="2020-03-01", source_url=documents[0]["source_url"], symbols=["TEST"])
    assert any(row["ratio_numerator"] == 5 and row["ratio_denominator"] == 1 for row in rows)


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
                    **_date_authority(), "form": "20-F"}
    manifest = {"complete": True, "filing_date_enrichment": {"complete": True, "version": "sec-official-filing-date-v2"},
                "documents": [raw_document, {**raw_document, "source_url": canonical}]}
    client = loader.SecClient(tmp_path, offline=True)
    monkeypatch.setattr(client, "document", lambda *_args, **_kwargs: (b"content", "a" * 64))
    def parse_document(_client, document, _observations, *, downloaded, binding_sources):
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
            "binding": "registrant_cik", "symbols": ["ABC"], **_date_authority(), "form": "20-F"}
    documents = [{**base, "cik": 123}, {**base, "cik": 456}, {**base, "cik": 789, "source_url": url.replace("report.htm", "second.htm")}]
    manifest = {"complete": True, "filing_date_enrichment": {"complete": True, "version": "sec-official-filing-date-v2"}, "documents": documents}
    client = loader.SecClient(tmp_path, offline=True)
    downloads = []
    parse_calls = []
    generation = [1]
    fail_cik = [None]

    def download(source_url, **_kwargs):
        downloads.append(source_url)
        return b"same-original-bytes", "a" * 64

    def parse(_client, document, _observations, *, downloaded, binding_sources):
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


def _real_azn_parent_and_amendment(tmp_path, monkeypatch):
    fixtures = Path(__file__).parent / "fixtures" / "sec_foreign_listing_evidence"
    manifest = json.loads((fixtures / "manifest.json").read_text(encoding="utf-8"))
    by_name = {item["name"]: item for item in manifest}
    documents = []
    payloads = {}
    for name, binding in [("azn_2015_f6_parent_full", "registrant_cik"),
                          ("azn_2015_amendment_full", "issuer_name_in_f6")]:
        item = by_name[name]
        documents.append({"cik": item["cik"], "adsh": item["accession_number"],
                          "form": item["form_type"], "filed": item["filing_date"],
                          "source_url": item["source_url"], "source_package": name,
                          "symbols": ["AZN"], "binding": binding,
                          "registrant_cik": str(item["cik"]),
                          "issuer_name": "ASTRAZENECA PLC" if binding == "issuer_name_in_f6" else ""})
        payloads[item["source_url"]] = (fixtures / item["fixture"]).read_bytes()
    client = loader.SecClient(tmp_path, offline=True)
    def document(url, **_kwargs):
        raw = payloads[url]
        return raw, loader.digest(raw)
    monkeypatch.setattr(client, "document", document)
    return client, documents[0], documents[1], payloads


def test_real_f6_attachment_inherits_verified_same_accession_parent_identity(tmp_path, monkeypatch):
    client, parent, amendment, payloads = _real_azn_parent_and_amendment(tmp_path, monkeypatch)
    assert loader.verify_f6_issuer(payloads[parent["source_url"]].decode("utf-8"), "ASTRAZENECA PLC")
    assert not loader.verify_f6_issuer(payloads[amendment["source_url"]].decode("utf-8"), "ASTRAZENECA PLC")
    rejected, rows = loader.parse_document(client, amendment)
    assert rejected["status"] == "issuer_binding_unverified" and not rows
    accepted, rows = loader.parse_document(client, amendment, binding_sources=[parent])
    assert accepted["status"] == "parsed" and rows
    proof = accepted["issuer_binding_proof"]
    assert proof["source_url"] == parent["source_url"]
    assert proof["source_sha256"] == loader.digest(payloads[parent["source_url"]])
    assert proof["adsh"] == amendment["adsh"] and proof["cik"] == 901832
    assert proof["issuer_name"] == "ASTRAZENECA PLC"


@pytest.mark.parametrize("mutation", ["different_accession", "different_issuer", "unverified_cover"])
def test_f6_attachment_parent_proof_cannot_be_inferred_from_cik_or_search_name(tmp_path, monkeypatch, mutation):
    client, parent, amendment, payloads = _real_azn_parent_and_amendment(tmp_path, monkeypatch)
    if mutation == "different_accession":
        parent = {**parent, "adsh": "0001193805-14-002183"}
    elif mutation == "different_issuer":
        parent = {**parent, "cik": 123}
    else:
        payloads[parent["source_url"]] = b"AstraZeneca PLC is mentioned in a service contract with a depositary bank."
    updated, rows = loader.parse_document(client, amendment, binding_sources=[parent])
    assert updated["status"] == "issuer_binding_unverified"
    assert rows == []


def _date_document(*, accepted="2024-03-22", adsh="0001104659-24-037982", cik=2809):
    return {"cik": cik, "adsh": adsh, "filed": accepted, "form": "40-F",
            "source_url": f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh.replace('-', '')}/report.htm",
            "source_package": "date-test-" + adsh, "binding": "registrant_cik", "symbols": ["AEM"]}


def _write_date_index(path, lines):
    text = "CIK|Company Name|Form Type|Date Filed|Filename\n" + "\n".join(lines) + "\n"
    path.write_text(text, encoding="utf-8", newline="\n")
    return text


def test_real_aem_friday_acceptance_uses_monday_sec_filing_and_tuesday_availability(tmp_path, monkeypatch):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    fixtures = Path(__file__).parent / "fixtures" / "sec_foreign_listing_evidence"
    raw = (fixtures / "master_2024_q1_aem.idx").read_bytes()
    (tmp_path / "master-2024-Q1.idx").write_bytes(raw)
    client = loader.SecClient(tmp_path, offline=True)
    enriched, _ = dates.enrich_manifest(client, {"complete": True, "documents": [_date_document()]}, index_root=tmp_path)
    document = enriched["documents"][0]
    assert document["query_accepted_on"] == "2024-03-22"
    assert document["filed"] == "2024-03-25"
    assert document["publication_floor_on"] == "2024-03-22"
    assert document["filing_date_status"] == "resolved"
    proof = document["filing_date_proof"]
    assert proof["source"] == "sec_quarter_master_index"
    assert proof["filed"] == "2024-03-25"
    assert any(record["source_sha256"] == loader.digest(raw) for record in proof["records"])
    cover = b"""<p>Securities registered pursuant to Section 12(b) of the Act:</p>
    <table><tr><th>Title of each class</th><th>Trading Symbol</th><th>Name of each exchange on which registered</th></tr>
    <tr><td>Common Shares</td><td>AEM</td><td>New York Stock Exchange</td></tr></table>"""
    monkeypatch.setattr(client, "document", lambda *_args, **_kwargs: (cover, loader.digest(cover)))
    _, rows = loader.parse_document(client, document)
    assert rows and {row["available_on"] for row in rows} == {"2024-03-26"}


@pytest.mark.parametrize("accepted,filed,index_name,adsh", [
    ("2024-03-22", "2024-03-25", "master-2024-Q1.idx", "0001104659-24-037982"),
    ("2024-03-29", "2024-04-01", "master-2024-Q2.idx", "0001104659-24-000001"),
    ("2023-12-29", "2024-01-02", "master-2024-Q1.idx", "0001104659-23-000001"),
])
def test_official_filing_lookup_crosses_weekends_quarters_and_years(tmp_path, accepted, filed, index_name, adsh):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    _write_date_index(tmp_path / index_name, [f"2809|AEM|40-F|{filed}|edgar/data/2809/{adsh}.txt"])
    document = _date_document(accepted=accepted, adsh=adsh)
    enriched, _ = dates.enrich_manifest(loader.SecClient(tmp_path, offline=True), {"complete": True, "documents": [document]}, index_root=tmp_path)
    assert enriched["documents"][0]["filed"] == filed
    assert enriched["documents"][0]["query_accepted_on"] == accepted


def test_same_accession_cofilers_with_one_date_are_one_filing_date(tmp_path):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    adsh = "0001104659-24-037982"
    _write_date_index(tmp_path / "master-2024-Q1.idx", [
        f"2809|AEM|40-F|2024-03-25|edgar/data/2809/{adsh}.txt",
        f"99999|COFILER|40-F|2024-03-25|edgar/data/99999/{adsh}.txt",
    ])
    enriched, _ = dates.enrich_manifest(loader.SecClient(tmp_path, offline=True), {"complete": True, "documents": [_date_document()]}, index_root=tmp_path)
    document = enriched["documents"][0]
    assert document["filed"] == "2024-03-25"
    assert document["filing_date_status"] == "resolved"
    assert len(document["filing_date_proof"]["records"]) == 2


def test_conflicting_official_dates_never_choose_by_issuer_or_query_acceptance(tmp_path):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    adsh = "0001104659-24-037982"
    _write_date_index(tmp_path / "master-2024-Q1.idx", [
        f"2809|AEM|40-F|2024-03-25|edgar/data/2809/{adsh}.txt",
        f"99999|COFILER|40-F|2024-03-26|edgar/data/99999/{adsh}.txt",
    ])
    enriched, _ = dates.enrich_manifest(loader.SecClient(tmp_path, offline=True), {"complete": True, "documents": [_date_document()]}, index_root=tmp_path)
    document = enriched["documents"][0]
    assert document["filed"] is None
    assert document["filing_date_status"] == "ambiguous"
    assert enriched["complete"] is False


def test_unverified_filing_date_never_falls_back_to_query_acceptance(tmp_path):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    enriched, _ = dates.enrich_manifest(loader.SecClient(tmp_path, offline=True), {"complete": True, "documents": [_date_document()]}, index_root=tmp_path)
    document = enriched["documents"][0]
    assert document["query_accepted_on"] == "2024-03-22"
    assert document["filed"] is None
    assert document["filing_date_status"] == "none"
    assert enriched["complete"] is False


def test_date_fallback_uses_same_accession_w1_filed_date_with_proof(tmp_path):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    observations = [{"cik": 2809, "adsh": "0001104659-24-037982", "filed": "2024-03-25", "accepted": "2024-03-22T17:31:00"}]
    enriched, _ = dates.enrich_manifest(loader.SecClient(tmp_path, offline=True), {"complete": True, "documents": [_date_document()]}, observations=observations, index_root=tmp_path)
    document = enriched["documents"][0]
    assert document["filed"] == "2024-03-25"
    assert document["filing_date_proof"]["source"] == "w1_same_accession"
    assert document["filing_date_proof"]["records"][0]["adsh"] == observations[0]["adsh"]


def test_w1_date_from_another_accession_cannot_fill_a_missing_index_entry(tmp_path):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    observations = [{"cik": 2809, "adsh": "0001104659-24-000001", "filed": "2024-03-25"}]
    enriched, _ = dates.enrich_manifest(loader.SecClient(tmp_path, offline=True), {"complete": True, "documents": [_date_document()]}, observations=observations, index_root=tmp_path)
    assert enriched["documents"][0]["filed"] is None


def test_date_fallback_uses_exact_sgml_accession_and_filed_as_of_date(tmp_path, monkeypatch):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    raw = (b"<SEC-DOCUMENT>0001104659-24-037982.txt\n<SEC-HEADER>\n"
           b"ACCESSION NUMBER: 0001104659-24-037982\n"
           b"ACCEPTANCE-DATETIME: 20240322173100\nFILED AS OF DATE: 20240325\n"
           b"</SEC-HEADER><DOCUMENT><TYPE>40-F</TYPE></DOCUMENT>")
    client = loader.SecClient(tmp_path, offline=True)
    monkeypatch.setattr(client, "document", lambda *_args, **_kwargs: (raw, loader.digest(raw)))
    enriched, _ = dates.enrich_manifest(client, {"complete": True, "documents": [_date_document()]}, index_root=tmp_path)
    document = enriched["documents"][0]
    assert document["filed"] == "2024-03-25"
    assert document["filing_date_proof"]["source"] == "sec_submission_header"
    assert document["filing_date_proof"]["records"][0]["source_sha256"] == loader.digest(raw)


def test_sgml_header_for_another_accession_does_not_prove_filing_date(tmp_path, monkeypatch):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    raw = b"<SEC-HEADER>\nACCESSION NUMBER: 0001104659-24-000001\nFILED AS OF DATE: 20240325\n</SEC-HEADER>"
    client = loader.SecClient(tmp_path, offline=True)
    monkeypatch.setattr(client, "document", lambda *_args, **_kwargs: (raw, loader.digest(raw)))
    enriched, _ = dates.enrich_manifest(client, {"complete": True, "documents": [_date_document()]}, index_root=tmp_path)
    assert enriched["documents"][0]["filed"] is None


def test_historical_daily_index_recovers_compact_filing_date(tmp_path):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    adsh = "0001104659-18-000001"
    _write_date_index(tmp_path / "master.20180608.idx", [
        f"2809|AEM|40-F|20180608|edgar/data/2809/{adsh}.txt",
    ])
    document = _date_document(accepted="2018-06-07", adsh=adsh)
    enriched, _ = dates.enrich_manifest(loader.SecClient(tmp_path, offline=True), {"complete": True, "documents": [document]}, index_root=tmp_path)
    result = enriched["documents"][0]
    assert result["filed"] == "2018-06-08"
    assert result["filing_date_proof"]["source"] == "sec_daily_master_index"
    assert result["filing_date_proof"]["records"][0]["source_url"].endswith("/2018/QTR2/master.20180608.idx")


def test_conflicting_sgml_filing_dates_remain_ambiguous(tmp_path, monkeypatch):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    raw = (b"<SEC-HEADER>\nACCESSION NUMBER: 0001104659-24-037982\n"
           b"FILED AS OF DATE: 20240325\nFILED AS OF DATE: 20240326\n</SEC-HEADER>")
    client = loader.SecClient(tmp_path, offline=True)
    monkeypatch.setattr(client, "document", lambda *_args, **_kwargs: (raw, loader.digest(raw)))
    enriched, _ = dates.enrich_manifest(client, {"complete": True, "documents": [_date_document()]}, index_root=tmp_path)
    result = enriched["documents"][0]
    assert result["filed"] is None and result["filing_date_status"] == "ambiguous"
    assert result["filing_date_proof"]["source"] == "sec_submission_header"


def test_manifest_without_authoritative_dates_cannot_be_parsed(tmp_path):
    with pytest.raises(ValueError, match="filing-date enrichment"):
        loader.parse_manifest(loader.SecClient(tmp_path, offline=True),
                              {"complete": True, "documents": [_date_document()]}, tmp_path / "rows.jsonl")


@pytest.mark.parametrize("cik,adsh,filed,published", [
    (839923, "0001104659-22-116238", "2018-06-08", "2022-11-09"),
    (932782, "9999999997-23-003670", "2020-07-10", "2023-07-26"),
])
def test_old_legal_filing_date_preserves_later_reported_publication_floor(tmp_path, cik, adsh, filed, published):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    document = _date_document(cik=cik, adsh=adsh, accepted=published)
    observations = [{"cik": cik, "adsh": adsh, "filed": filed}]
    enriched, _ = dates.enrich_manifest(loader.SecClient(tmp_path, offline=True),
                                      {"complete": True, "documents": [document]},
                                      observations=observations, index_root=tmp_path)
    result = enriched["documents"][0]
    assert result["filed"] == filed
    assert result["publication_floor_on"] == published
    assert result["publication_floor_proof"]["source"] == "discovery_reported_publication_floor"
    assert enriched["filing_date_enrichment"]["version"] == "sec-official-filing-date-v2"


def test_publication_floor_is_the_maximum_reported_date_across_cofilers(tmp_path):
    from scripts import enrich_sec_foreign_listing_filing_dates as dates
    first = _date_document(accepted="2023-07-25", adsh="9999999997-23-003670", cik=932782)
    second = {**first, "cik": 123, "filed": "2023-07-26", "source_package": "cofiler"}
    observations = [{"cik": 932782, "adsh": first["adsh"], "filed": "2020-07-10"}]
    enriched, _ = dates.enrich_manifest(loader.SecClient(tmp_path, offline=True),
                                      {"complete": True, "documents": [first, second]},
                                      observations=observations, index_root=tmp_path)
    assert {item["publication_floor_on"] for item in enriched["documents"]} == {"2023-07-26"}
    assert {item["filed"] for item in enriched["documents"]} == {"2020-07-10"}


def test_parser_keeps_legal_effective_date_but_uses_later_publication_floor(tmp_path):
    adsh = "9999999997-23-003670"
    document = {**_date_document(accepted="2020-07-10", adsh=adsh),
                **_date_authority("2020-07-10", adsh, publication_floor_on="2023-07-26")}
    raw = b"""<p>Securities registered pursuant to Section 12(b) of the Act:</p>
    <table><tr><th>Title of each class</th><th>Trading Symbol</th><th>Name of each exchange on which registered</th></tr>
    <tr><td>Common Shares</td><td>AEM</td><td>New York Stock Exchange</td></tr></table>"""
    _, rows = loader.parse_document(loader.SecClient(tmp_path, offline=True), document,
                                    downloaded=(raw, loader.digest(raw)))
    assert rows
    assert {row["effective_from"] for row in rows} == {"2020-07-11"}
    assert {row["available_on"] for row in rows} == {"2023-07-26"}
    assert {row["publication_floor_on"] for row in rows} == {"2023-07-26"}


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
    ({}, (None, True, False)),
    ({"underlying_class": "class_a", "ordinary_candidate": True}, ("class_a", True, False)),
    ({"underlying_class": "series_b", "ordinary_candidate": False}, ("series_b", False, False)),
    ({"effective_date_explicit": True}, (None, True, True)),
    ({"effective_date_explicit": False}, (None, True, False)),
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
    assert (actual["underlying_class"], actual["ordinary_candidate"], actual["effective_date_explicit"]) == expected


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


def _source(package="source-a", adsh="0001234567-20-000001", count=1, filed="2020-03-01", publication_floor_on=None):
    return {"source_package": package, "cik": 123, "adsh": adsh, "evidence_count": count,
            "source_url": "https://www.sec.gov/Archives/edgar/data/123/report.htm",
            "source_sha256": "a" * 64, "parser_version": "test-v1", **_date_authority(filed, adsh, publication_floor_on)}


def _fact(source, *, listed_type="ordinary_direct", revision="1"):
    effective = (date.fromisoformat(source["filed"]) + timedelta(days=1)).isoformat()
    available = max(effective, source.get("publication_floor_on") or effective)
    row = {**source, "symbol": "ABC", "form": "20-F",
           "source_kind": "cover_12b", "evidence_kind": "listed_type", "listed_type": listed_type,
           "ratio_numerator": None, "ratio_denominator": None, "effective_from": effective,
           "effective_to": None, "evidence_text": "Common Shares NYSE revision " + revision,
           "evidence_location": "cover table", "available_on": available}
    row.pop("evidence_count")
    row["fact_hash"] = loader.hashlib.md5(loader.canonical_json(row).encode(), usedforsecurity=False).hexdigest()
    return row


def _manifest(*sources):
    return {"complete": True, "parse_complete": True, "filing_date_enrichment": {"complete": True, "version": "sec-official-filing-date-v2"},
            "documents": list(sources)}


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


def test_same_day_correction_before_source_availability_never_exposes_old_fact(db):
    source = _source(filed="2026-10-09")
    loader.apply_evidence(db, _manifest(source), [_fact(source)], date(2026, 10, 9))
    loader.apply_evidence(db, _manifest(source), [_fact(source, listed_type="ads", revision="2")], date(2026, 10, 9))
    rows = db.execute("SELECT listed_type,available_on,retired_on FROM public.sec_foreign_listing_evidence ORDER BY id").fetchall()
    assert rows == [("ordinary_direct", date(2026, 10, 10), date(2026, 10, 9)),
                    ("ads", date(2026, 10, 10), None)]
    assert db.execute("SELECT status FROM public.sec_foreign_listing_at(123,'ABC','2026-10-09')").fetchone()[0] == "none"
    assert db.execute("SELECT listed_type FROM public.sec_foreign_listing_at(123,'ABC','2026-10-10')").fetchone()[0] == "ads"


def test_initial_load_and_correction_honor_publication_floor(db):
    source = _source(filed="2020-07-10", publication_floor_on="2023-07-26")
    loader.apply_evidence(db, _manifest(source), [_fact(source)], date(2026, 10, 9))
    assert db.execute("SELECT available_on,source_available_on,effective_from FROM public.sec_foreign_listing_evidence").fetchone() == (
        date(2023, 7, 26), date(2023, 7, 26), date(2020, 7, 11))
    assert db.execute("SELECT status FROM public.sec_foreign_listing_at(123,'ABC','2020-12-31')").fetchone()[0] == "none"
    assert db.execute("SELECT status FROM public.sec_foreign_listing_at(123,'ABC','2023-07-26')").fetchone()[0] == "resolved"
    loader.apply_evidence(db, _manifest(source), [_fact(source, listed_type="ads", revision="2")], date(2026, 10, 10))
    assert db.execute("SELECT available_on FROM public.sec_foreign_listing_evidence WHERE retired_on IS NULL").fetchone()[0] == date(2026, 10, 10)


def test_fact_filing_date_must_match_the_authoritative_manifest(db):
    source = _source()
    wrong = {**_fact(source), "filed": "2020-02-28"}
    with pytest.raises(ValueError, match="filing date does not match"):
        loader.apply_evidence(db, _manifest(source), [wrong], date(2026, 10, 9))


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


def _completed_shard_artifacts(tmp_path, *, binding="registrant_cik"):
    from scripts import run_sec_foreign_listing_evidence_shards as shards
    documents = []
    for number in range(6):
        source = {**_source(), "source_url": f"https://www.sec.gov/Archives/edgar/data/123/report-{number}.htm",
                  "form": "20-F", "filed": "2020-03-01", "binding": binding,
                  "issuer_name": "ACME PLC", "symbols": ["ABC"]}
        documents.append(loader.canonical_document(source))
    assert {shards.partition(document) for document in documents} == {0, 1}
    parent = {"complete": True, "filing_date_enrichment": {"complete": True, "version": "sec-official-filing-date-v2"},
              "universe_sha256": "a" * 64, "documents": documents}
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


@pytest.mark.parametrize("field,value", [
    ("binding", "registrant_cik"), ("issuer_name", "A DIFFERENT ISSUER"),
    ("cik", 123.0),
    ("symbols", ["OTHER"]), ("registrant_cik", "999"),
    ("document_role", "securities_description"), ("attachment_type", "EX-2.1"),
    ("attachment_description", "Different securities"),
    ("query_accepted_on", "2020-03-02"), ("query_filed_at", "2020-03-02T12:00:00Z"),
    ("filing_date_proof", {"source": "w1_same_accession", "filed": "2020-03-01", "records": []}),
    ("publication_floor_on", "2020-03-02"),
    ("publication_floor_proof", {"source": "discovery_reported_publication_floor",
                                 "publication_floor_on": "2020-03-01", "tampered": True}),
    ("source_sha256", "c" * 64), ("new_discovery_metadata", "tampered"),
])
def test_shards_reject_changed_parsing_and_binding_metadata_before_collection_and_combine(
        tmp_path, monkeypatch, field, value):
    shards, _, _ = _completed_shard_artifacts(tmp_path, binding="issuer_name_in_f6")
    directory = tmp_path / "parts" / "0"
    input_path = directory / "input-manifest.json"
    child_path = directory / "manifest.json"
    input_manifest = json.loads(input_path.read_text())
    input_manifest["documents"][0][field] = value
    loader.write_json(input_path, input_manifest)
    monkeypatch.setattr(loader, "parse_manifest", lambda *_args, **_kwargs: pytest.fail("Altered child reached parsing"))
    monkeypatch.setattr(loader, "load_key", lambda *_args: pytest.fail("Altered child reached credentials"))
    with pytest.raises(ValueError, match="immutable parent"):
        shards.collect(tmp_path, 0)
    child = json.loads(child_path.read_text())
    child["documents"][0][field] = value
    loader.write_json(child_path, child)
    output = tmp_path / "combined.jsonl"
    output.write_bytes(b"previous-artifact\n")
    with pytest.raises(ValueError):
        shards.combine(tmp_path, output)
    assert output.read_bytes() == b"previous-artifact\n"


def test_historical_coverage_excludes_retired_tickers_outside_fixed_universe(tmp_path, monkeypatch):
    import sys
    import psycopg
    from scripts import validate_sec_foreign_listing_evidence as validator

    universe = tmp_path / "universe.json"
    observations = tmp_path / "observations.json"
    output = tmp_path / "validation"
    loader.write_json(universe, [{"cik": 123, "symbol": "ABC"}])
    loader.write_json(observations, [
        {"cik": 123, "ticker_key": "ABC", "available_on": "2009-01-01", "retired_on": "2015-12-31"},
        {"cik": 456, "ticker_key": "RETIRED", "available_on": "2009-01-01", "retired_on": "2015-12-31"},
    ])

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def execute(self, query, parameters=()):
            if parameters:
                assert parameters[:2] == (123, "ABC")
            return self

        def fetchone(self):
            return {"status": "resolved", "listing_status": "resolved", "listed_type": "ordinary_direct"}

    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: Connection())
    monkeypatch.setenv("SEC_FOREIGN_TEST_DATABASE_URL", "unused-local-test-connection")
    monkeypatch.setattr(sys, "argv", ["validator", "--universe", str(universe),
                                     "--observations", str(observations), "--output-dir", str(output),
                                     "--sample-size", "0"])
    assert validator.main() == 0
    coverage = json.loads((output / "coverage.json").read_text())["coverage"]
    assert [row["w1_evidenced_lines"] for row in coverage] == [1, 0, 0, 0]
    assert [row["resolved_among_w1_evidenced"] for row in coverage] == [1, 0, 0, 0]
    assert all(row["w1_evidenced_lines"] <= row["foreign_lines"] for row in coverage)


def _sgml_submission(original, *, accession="0001234567-20-000001", duplicate=False):
    block = b"<DOCUMENT>\n<TYPE>F-6\n<SEQUENCE>1\n<FILENAME>primary.htm\n<TEXT>\n" + original + b"</TEXT>\n</DOCUMENT>\n"
    return (f"<SEC-DOCUMENT>{accession}.txt\nACCESSION NUMBER:\t{accession}\n".encode()
            + block + (block if duplicate else b"") + b"</SEC-DOCUMENT>\n")


def test_sgml_selection_preserves_exact_original_bytes_and_rejects_duplicates():
    original = b"\n<HTML>" + b"Original SEC filing text. " * 10 + b"</HTML>\n"
    selected, form = loader.extract_sgml_document(_sgml_submission(original), accession="0001234567-20-000001", filename="primary.htm")
    assert selected == original  # Its own leading/trailing newlines are retained.
    assert form == "F-6"
    with pytest.raises(ValueError, match="exactly one"):
        loader.extract_sgml_document(_sgml_submission(original, duplicate=True), accession="0001234567-20-000001", filename="primary.htm")
    with pytest.raises(ValueError, match="exactly one"):
        loader.extract_sgml_document(_sgml_submission(original), accession="0001234567-20-000001", filename="another.htm")
    with pytest.raises(ValueError, match="accession"):
        loader.extract_sgml_document(_sgml_submission(original), accession="0001234567-21-000001", filename="primary.htm")


def test_offline_sgml_recovery_is_audited_and_retains_primary_identity(tmp_path, monkeypatch):
    primary = "https://www.sec.gov/Archives/edgar/data/123/000123456720000001/primary.htm"
    submission, accession, filename = loader.sgml_recovery_locator(primary)
    original = (b"<HTML><BODY>Depositary registration statement. "
                b"Each American Depositary Share represents five ordinary shares. "
                b"Terms of the deposited securities are described below.</BODY></HTML>\n")
    full = _sgml_submission(original)
    source = loader.SecClient(tmp_path, "fake")
    monkeypatch.setattr(source, "request", lambda *_: full)
    source.document(submission)
    # An offline primary miss recovers solely from the already verified complete
    # submission. The actual HTTP primitive must never be reached.
    monkeypatch.setattr(loader, "urlopen", lambda *_, **__: pytest.fail("Offline recovery attempted HTTP"))
    offline = loader.SecClient(tmp_path, offline=True)
    data, sha = offline.document(primary)
    assert data == original and sha == loader.digest(original)
    proof = offline.recovery_proofs[primary]
    assert proof["recovery_source_url"] == submission
    assert proof["recovery_source_sha256"] == loader.digest(full)
    assert proof["recovery_filename"] == filename
    document = {"cik": 123, "adsh": accession, "form": "F-6", "filed": "2020-03-01", "symbols": [],
                "binding": "registrant_cik", "source_url": primary}
    metadata, facts = loader.parse_document(offline, document)
    assert metadata["source_url"] == primary and metadata["source_sha256"] == sha
    assert metadata["document_recovery_proof"] == proof
    assert facts
    assert all(row["source_url"] == primary and row["source_sha256"] == sha for row in facts)
    assert all(row["document_recovery_proof"] == proof and "recovery-submission-sha256=" in row["evidence_location"] for row in facts)
    # A fresh process reads both recovered original bytes and their provenance.
    reloaded = loader.SecClient(tmp_path, offline=True)
    assert reloaded.document(primary) == (original, sha)
    assert reloaded.recovery_proofs[primary] == proof


@pytest.mark.parametrize("filename", ["0001234567-20-000001.txt", "exhibit.pdf"])
def test_sgml_fallback_does_not_recurse_or_decode_binary_pdf(tmp_path, monkeypatch, filename):
    client = loader.SecClient(tmp_path, "fake")
    calls = []
    def request(url):
        calls.append(url)
        return b"short"
    monkeypatch.setattr(client, "request", request)
    with pytest.raises(ValueError, match="empty document"):
        client.document("https://www.sec.gov/Archives/edgar/data/123/000123456720000001/" + filename)
    assert len(calls) == 1


def _write_relocation_index(cache, *, duplicate=False):
    row = "456|ACME PLC/ADR|F-6|2020-03-01|edgar/data/456/0001234567-20-000001.txt"
    text = "CIK|Company Name|Form Type|Date Filed|Filename\n" + row + "\n"
    if duplicate:
        text += "789" + row[3:].replace("/456/", "/789/") + "\n"
    loader.write_bytes(cache / "master-2020-Q1.idx", text.encode())
    return row, text.encode()


def test_official_index_relocation_requires_one_exact_accession_mapping(tmp_path):
    primary = "https://www.sec.gov/Archives/edgar/data/123/000123456720000001/primary.htm"
    row, raw = _write_relocation_index(tmp_path)
    actual, proof = loader.indexed_relocation(tmp_path, primary)
    assert actual == primary.replace("/123/", "/456/")
    assert proof["recovery_index_line"] == row
    assert proof["recovery_index_sha256"] == loader.digest(raw)
    assert proof["recovery_index_url"].endswith("/2020/QTR1/master.idx")
    assert loader.indexed_relocation(tmp_path, primary.replace("720000001", "720000002")) is None
    _write_relocation_index(tmp_path, duplicate=True)
    with pytest.raises(ValueError, match="ambiguous accession"):
        loader.indexed_relocation(tmp_path, primary)


def test_offline_index_recovery_preserves_identity_and_binding_provenance(tmp_path, monkeypatch):
    primary = "https://www.sec.gov/Archives/edgar/data/123/000123456720000001/primary.htm"
    actual = primary.replace("/123/", "/456/")
    _write_relocation_index(tmp_path)
    raw = (b"<HTML><BODY>ACME PLC (Exact name of issuer of deposited securities) "
           b"Each American Depositary Share represents five ordinary shares. "
           b"Terms of the registered deposited securities.</BODY></HTML>")
    seed = loader.SecClient(tmp_path, "fake")
    monkeypatch.setattr(seed, "request", lambda *_: raw)
    seed.document(actual)
    monkeypatch.setattr(loader, "urlopen", lambda *_, **__: pytest.fail("Offline relocation attempted HTTP"))
    client = loader.SecClient(tmp_path, offline=True)
    assert client.document(primary) == (raw, loader.digest(raw))
    proof = client.recovery_proofs[primary]
    assert proof["recovery_retrieval_url"] == actual
    document = {"cik": 123, "adsh": "0001234567-20-000001", "form": "F-6", "filed": "2020-03-01", "symbols": [],
                "binding": "registrant_cik", "source_url": primary}
    updated, facts = loader.parse_document(client, document)
    assert updated["cik"] == 123 and updated["source_url"] == primary
    assert updated["document_recovery_proof"]["recovery_archive_cik"] == 456
    assert facts and all("sec-index-sha256=" in fact["evidence_location"] for fact in facts)
    exhibit = {**document, "source_url": primary.replace("primary.htm", "deposit-agreement.htm"),
               "binding": "issuer_name_in_f6", "issuer_name": "ACME PLC"}
    parent_proof = loader.f6_attachment_binding_proof(client, exhibit, [document])
    assert parent_proof["source_url"] == actual
    assert parent_proof["discovery_source_url"] == primary
    assert parent_proof["source_sha256"] == loader.digest(raw)
    assert parent_proof["source_recovery_proof"]["recovery_index_line"].startswith("456|ACME")
