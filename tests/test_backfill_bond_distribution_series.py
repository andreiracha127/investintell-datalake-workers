"""Contracts for the non-production SEC 144A-to-Reg-S evidence backfill."""
from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from datetime import date

import pytest

from scripts import backfill_bond_distribution_series as backfill


FIXTURES = Path(__file__).parent / "fixtures" / "bond_distribution"
ROOT = Path(__file__).resolve().parents[1]


class _Response:
    def __init__(self, payload: object, *, content: bytes | None = None, status: int = 200) -> None:
        self.payload, self.content, self.status = payload, content, status
        self.headers = {"content-type": "application/json"}

    def json(self) -> object:
        return self.payload


class _Client:
    def __init__(self, responses: list[_Response | BaseException]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, object], dict[str, str]]] = []

    def post_json(self, url: str, payload: dict[str, object], headers: dict[str, str]) -> _Response:
        self.calls.append((url, payload, headers))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    def get_bytes(self, url: str, headers: dict[str, str]) -> _Response:
        self.calls.append((url, {}, headers))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _document(accession: str, *, document_type: str = "EX-4.1", suffix: str = "", description: str | None = None) -> dict[str, str]:
    document = {
        "accessionNo": accession,
        "formType": "424B2",
        "documentType": document_type,
        "filedAt": "2025-07-22T12:00:00-04:00",
        "linkToFilingDetails": f"https://www.sec.gov/Archives/edgar/data/1/{accession}{suffix}.htm",
    }
    if description:
        document["description"] = description
    return document


def _approved_preview_record(*, label: str = "CUSIP", value: str = "344045AB5") -> dict[str, object]:
    return {
        "status": "candidate", "adjudication": "approved", "draft_snapshot_id": "snapshot-draft",
        "valid_from": "2025-01-01", "accession": "a-1", "block_locator": "table[0]/row[0]",
        "parent_form": "424B2", "document_type": "EX-4.1", "document_hash": "a" * 64,
        "parser_version": "explicit-label-v1",
        "evidence_link": {"filing_url": "https://www.sec.gov/example.htm", "retrieved_at": "2025-07-23T00:00:00+00:00"},
        "reg_s": [{"identifier_label": "CUSIP", "exact_label": "Reg S CUSIP", "source_value": "G35906AC3", "normalized_value": "G35906AC3", "tenure": "not_stated"}],
        "rule_144a": [{"identifier_label": label, "exact_label": f"Rule 144A {label}", "source_value": value, "normalized_value": value, "tenure": "not_stated"}],
    }


def test_evidence_only_cli_starts_without_importing_publish_dependencies() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "backfill_bond_distribution_series.py"), "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "targeted-discover" in result.stdout


def test_discover_paginates_versioned_queries_and_resumes_from_checkpoint(tmp_path: Path) -> None:
    client = _Client([
        _Response({"filings": [_document("a-1")]}),
        _Response({"filings": []}),
        _Response({"filings": []}),
        _Response({"filings": []}),
    ])

    first = backfill.discover(client, tmp_path, api_key="top-secret", start_date="2025-01-01", end_date="2025-12-31", limit=4)
    second = backfill.discover(_Client([]), tmp_path, api_key="top-secret", start_date="2025-01-01", end_date="2025-12-31", limit=1)

    assert first["documents_added"] == 1
    assert first["completed_queries"] == len(backfill.SEARCH_QUERY_VERSIONS)
    assert second["documents_added"] == 0
    assert all(call[0] == "https://api.sec-api.io/full-text-search" for call in client.calls)
    assert [call[1]["query"] for call in client.calls] == [
        item["query"] for item in backfill.SEARCH_QUERY_VERSIONS for _ in ([1, 2] if item["query"] == backfill.SEARCH_QUERY_VERSIONS[0]["query"] else [1])
    ]
    assert [call[1]["page"] for call in client.calls] == [1, 2, 1, 1]
    assert json.loads((tmp_path / "metadata.json").read_text())[0]["accession"] == "a-1"
    assert all(call[2]["Authorization"] == "top-secret" for call in client.calls)


def test_url_response_decodes_gzip_json_and_keeps_plain_json_unchanged() -> None:
    payload = {"filings": [{"accessionNo": "gzip-ok"}]}

    assert backfill._UrlResponse(
        gzip.compress(json.dumps(payload).encode("utf-8")), {"Content-Encoding": "gzip"}, 200
    ).json() == payload
    assert backfill._UrlResponse(
        json.dumps(payload).encode("utf-8"), {"content-type": "application/json"}, 200
    ).json() == payload

def test_discover_records_failure_without_advancing_checkpoint_or_leaking_secret(tmp_path: Path) -> None:
    key = "do-not-leak-this"
    client = _Client([RuntimeError(f"upstream rejected {key}")])

    summary = backfill.discover(client, tmp_path, api_key=key, start_date="2025-01-01", end_date="2025-12-31", limit=1)

    assert summary["failures"] == 1
    assert key not in json.dumps(summary)
    state = json.loads((tmp_path / "state" / "discover.json").read_text())
    assert state["query_index"] == 0 and state["page"] == 1
    assert state["last_error"] == "request_failed"


def test_failed_discovery_retries_the_same_query_and_page_on_the_next_invocation(tmp_path: Path) -> None:
    first = backfill.discover(
        _Client([RuntimeError("timeout")]), tmp_path, api_key="key", start_date="2025-01-01", end_date="2025-12-31", limit=1
    )
    retry_client = _Client([
        _Response({"filings": []}), _Response({"filings": []}), _Response({"filings": []}),
    ])

    second = backfill.discover(
        retry_client, tmp_path, api_key="key", start_date="2025-01-01", end_date="2025-12-31", limit=3
    )

    assert first["failures"] == 1 and second["completed_queries"] == 3
    assert retry_client.calls[0][1]["query"] == backfill.SEARCH_QUERY_VERSIONS[0]["query"]
    assert retry_client.calls[0][1]["page"] == 1


def test_broad_discovery_enforces_budget_and_resumes_its_checkpoint(tmp_path: Path) -> None:
    first = backfill.discover(
        _Client([_Response({"filings": [_document("a-1")]})]), tmp_path, api_key="key",
        start_date="2025-01-01", end_date="2025-12-31", limit=1,
    )
    checkpoint = json.loads((tmp_path / "state" / "discover.json").read_text())
    retry_client = _Client([_Response({"filings": []}), _Response({"filings": []}), _Response({"filings": []})])
    second = backfill.discover(
        retry_client, tmp_path, api_key="key", start_date="2025-01-01", end_date="2025-12-31", limit=3,
    )

    assert first["requests"] == 1 and first["budget"] == 1
    assert checkpoint["query_index"] == 0 and checkpoint["page"] == 2 and not checkpoint["completed"]
    assert retry_client.calls[0][1]["page"] == 2
    assert second["completed_queries"] == len(backfill.SEARCH_QUERY_VERSIONS)


@pytest.mark.parametrize(
    ("references", "start_date", "end_date", "query_version"),
    [
        (["037833100"], "2025-01-01", "2025-12-31", "v1"),
        (["344045AB5"], "2025-02-01", "2025-12-31", "v1"),
        (["344045AB5"], "2025-01-01", "2025-12-31", "v2"),
    ],
)
def test_targeted_discovery_refuses_a_checkpoint_with_incompatible_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, references: list[str], start_date: str,
    end_date: str, query_version: str,
) -> None:
    backfill.targeted_discover(
        _Client([_Response({"filings": []})]), tmp_path, api_key="key", reference_cusips=["344045AB5"],
        start_date="2025-01-01", end_date="2025-12-31", limit=1,
    )
    monkeypatch.setattr(backfill, "TARGETED_QUERY_VERSION", query_version)

    with pytest.raises(ValueError, match="checkpoint scope mismatch"):
        backfill.targeted_discover(
            _Client([]), tmp_path, api_key="key", reference_cusips=references,
            start_date=start_date, end_date=end_date, limit=1,
        )


def test_discover_and_download_keep_distinct_exhibits_with_the_same_accession(tmp_path: Path) -> None:
    first, second = (
        _document("same-accession", document_type="EX-4.1", suffix="-one", description="USD notes"),
        _document("same-accession", document_type="EX-4.2", suffix="-two", description="EUR notes"),
    )
    discover_client = _Client([
        _Response({"filings": [first, second]}), _Response({"filings": []}), _Response({"filings": []}), _Response({"filings": []}),
    ])

    discovered = backfill.discover(
        discover_client, tmp_path, api_key="key", start_date="2025-01-01", end_date="2025-12-31", limit=4
    )
    download_client = _Client([_Response({}, content=b"first exhibit"), _Response({}, content=b"second exhibit")])
    downloaded = backfill.download(download_client, tmp_path, edgar_identity="Analyst analyst@example.test")

    metadata = json.loads((tmp_path / "metadata.json").read_text())
    downloads = json.loads((tmp_path / "downloads.json").read_text())
    assert discovered["documents_added"] == 2 and downloaded["downloaded"] == 2
    assert len(metadata) == len(downloads) == 2
    assert {item["description"] for item in metadata} == {"USD notes", "EUR notes"}
    assert len({item["document_key"] for item in metadata}) == 2
    assert {item["document_key"] for item in downloads} == {item["document_key"] for item in metadata}
    assert [call[0] for call in download_client.calls] == [first["linkToFilingDetails"], second["linkToFilingDetails"]]


def test_targeted_discovery_uses_compact_and_filing_formatted_cusip_queries() -> None:
    assert backfill.targeted_query_variants("344045AB5") == (
        '"344045AB5" "Rule 144A" "Reg S"',
        '"344045 AB 5" "Rule 144A" "Reg S"',
    )


def test_targeted_discovery_enforces_request_budget_and_resumes_next_query_variant(tmp_path: Path) -> None:
    client = _Client([_Response({"filings": []})])

    first = backfill.targeted_discover(
        client, tmp_path, api_key="key", reference_cusips=["344045AB5"], start_date="2025-01-01", end_date="2025-12-31", limit=1,
    )
    checkpoint = json.loads((tmp_path / "state" / "targeted-discover.json").read_text())
    retry_client = _Client([_Response({"filings": []})])
    second = backfill.targeted_discover(
        retry_client, tmp_path, api_key="key", reference_cusips=["344045AB5"], start_date="2025-01-01", end_date="2025-12-31", limit=1,
    )

    assert first == {"budget": 1, "documents_added": 0, "failures": 0, "requests": 1, "references_completed": 0}
    assert checkpoint == {
        "completed": False, "page": 1, "reference_index": 0,
        "scope_fingerprint": backfill.targeted_scope_fingerprint(
            ["344045AB5"], start_date="2025-01-01", end_date="2025-12-31"
        ),
        "total_documents": 0, "variant_index": 1,
    }
    assert retry_client.calls[0][1]["query"] == backfill.targeted_query_variants("344045AB5")[1]
    assert second["references_completed"] == 1


def test_targeted_discovery_failure_retries_the_same_reference_and_page(tmp_path: Path) -> None:
    first = backfill.targeted_discover(
        _Client([RuntimeError("timeout")]), tmp_path, api_key="key", reference_cusips=["344045AB5"], start_date="2025-01-01", end_date="2025-12-31", limit=1,
    )
    state = json.loads((tmp_path / "state" / "targeted-discover.json").read_text())
    retry_client = _Client([_Response({"filings": []})])
    second = backfill.targeted_discover(
        retry_client, tmp_path, api_key="key", reference_cusips=["344045AB5"], start_date="2025-01-01", end_date="2025-12-31", limit=1,
    )

    assert first["failures"] == 1
    assert state["reference_index"] == 0 and state["variant_index"] == 0 and state["page"] == 1
    assert retry_client.calls[0][1]["page"] == 1
    assert second["requests"] == 1


def test_broad_omission_never_marks_a_reference_mapped_and_targeted_dedupe_is_shared(tmp_path: Path) -> None:
    broad = backfill.discover(
        _Client([_Response({"filings": []}), _Response({"filings": []}), _Response({"filings": []})]),
        tmp_path, api_key="key", start_date="2025-01-01", end_date="2025-12-31", limit=3,
    )
    document = _document("a-1", suffix="-target")
    targeted = backfill.targeted_discover(
        _Client([_Response({"filings": [document]})]), tmp_path, api_key="key", reference_cusips=["344045AB5"],
        start_date="2025-01-01", end_date="2025-12-31", limit=1,
    )

    metadata = json.loads((tmp_path / "metadata.json").read_text())
    assert broad["documents_added"] == 0 and targeted["documents_added"] == 1
    assert len(metadata) == 1 and "mapped" not in metadata[0]
    assert all("mapping" not in key for key in targeted)


def test_targeted_discovery_cli_requires_a_file_and_explicit_request_budget(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    references = tmp_path / "reference-cusips.txt"
    references.write_text("344045AB5\n037833100\n", encoding="utf-8")
    captured: dict[str, object] = {}

    def fake_targeted(_client: object, _root: Path, **kwargs: object) -> dict[str, int]:
        captured.update(kwargs)
        return {"budget": 2, "documents_added": 0, "failures": 0, "requests": 0, "references_completed": 0}

    monkeypatch.setattr(backfill, "targeted_discover", fake_targeted)

    assert backfill.main([
        "--output-root", str(tmp_path), "targeted-discover", "--reference-cusips-file", str(references), "--limit", "2",
        "--start-date", "2025-01-01", "--end-date", "2025-12-31", "--sec-api-key", "key",
    ]) == 0
    assert captured["reference_cusips"] == ["344045AB5", "037833100"]
    assert captured["limit"] == 2


def test_download_reuses_immutable_hash_and_requires_identity_header(tmp_path: Path) -> None:
    metadata = [_document("a-1")]
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    raw = b"<table>small evidence</table>"
    client = _Client([_Response({}, content=raw)])

    first = backfill.download(client, tmp_path, edgar_identity="Analyst analyst@example.test")
    second = backfill.download(_Client([]), tmp_path, edgar_identity="Analyst analyst@example.test")

    digest = hashlib.sha256(raw).hexdigest()
    assert first["downloaded"] == 1 and second["reused"] == 1
    assert (tmp_path / "raw" / f"{digest}.bin").read_bytes() == raw
    assert client.calls[0][2]["User-Agent"] == "Analyst analyst@example.test"


def test_download_redownloads_cached_document_when_raw_bytes_no_longer_match_recorded_hash(tmp_path: Path) -> None:
    metadata = [_document("a-1")]
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    expected_raw = b"<table>original immutable evidence</table>"
    expected_digest = hashlib.sha256(expected_raw).hexdigest()
    cached_path = tmp_path / "raw" / f"{expected_digest}.bin"
    cached_path.parent.mkdir()
    cached_path.write_bytes(b"tampered cached evidence")
    (tmp_path / "downloads.json").write_text(json.dumps([{
        "document_key": backfill._document_key("a-1", metadata[0]["linkToFilingDetails"], "EX-4.1"),
        "document_hash": expected_digest,
        "raw_path": f"raw/{expected_digest}.bin",
    }]), encoding="utf-8")
    refreshed_raw = b"<table>fresh immutable evidence</table>"
    client = _Client([_Response({}, content=refreshed_raw)])

    summary = backfill.download(client, tmp_path, edgar_identity="Analyst analyst@example.test")

    refreshed_digest = hashlib.sha256(refreshed_raw).hexdigest()
    saved = json.loads((tmp_path / "downloads.json").read_text(encoding="utf-8"))
    assert summary["downloaded"] == 1 and summary["reused"] == 0
    assert len(client.calls) == 1
    assert saved[0]["document_hash"] == refreshed_digest
    assert (tmp_path / saved[0]["raw_path"]).read_bytes() == refreshed_raw


def test_download_repairs_a_tampered_content_addressed_file_when_redownload_hash_is_unchanged(
    tmp_path: Path,
) -> None:
    metadata = [_document("a-1")]
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    expected_raw = b"<table>original immutable evidence</table>"
    expected_digest = hashlib.sha256(expected_raw).hexdigest()
    cached_path = tmp_path / "raw" / f"{expected_digest}.bin"
    cached_path.parent.mkdir()
    cached_path.write_bytes(b"tampered cached evidence")
    (tmp_path / "downloads.json").write_text(json.dumps([{
        "document_key": backfill._document_key("a-1", metadata[0]["linkToFilingDetails"], "EX-4.1"),
        "document_hash": expected_digest,
        "raw_path": f"raw/{expected_digest}.bin",
    }]), encoding="utf-8")

    summary = backfill.download(
        _Client([_Response({}, content=expected_raw)]),
        tmp_path,
        edgar_identity="Analyst analyst@example.test",
    )

    assert summary["downloaded"] == 1 and summary["reused"] == 0
    assert cached_path.read_bytes() == expected_raw


def test_download_retries_one_transient_document_failure_then_checkpoints_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = [_document("a-1")]
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    raw = b"<table>retry evidence</table>"
    client = _Client([TimeoutError("temporary SEC timeout"), _Response({}, content=raw)])
    monkeypatch.setattr(backfill, "_DOWNLOAD_RETRY_DELAYS", (0.0, 0.0))

    summary = backfill.download(client, tmp_path, edgar_identity="Analyst analyst@example.test")

    state = json.loads((tmp_path / "state" / "download.json").read_text())
    assert summary["downloaded"] == 1 and summary["failures"] == 0
    assert summary["retries"] == 1 and len(client.calls) == 2
    assert state == {
        "attempts": 0, "completed": True, "failed_document_key": None,
        "last_error": None,
    }


def test_download_exhaustion_records_only_sanitized_failure_and_preserves_resume_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "do-not-leak-this"
    metadata = [_document("a-1")]
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    client = _Client([TimeoutError(secret), TimeoutError(secret), TimeoutError(secret)])
    monkeypatch.setattr(backfill, "_DOWNLOAD_RETRY_DELAYS", (0.0, 0.0))

    summary = backfill.download(client, tmp_path, edgar_identity="Analyst analyst@example.test")

    expected_key = backfill._document_key(
        metadata[0]["accessionNo"], metadata[0]["linkToFilingDetails"], metadata[0]["documentType"],
    )
    state = json.loads((tmp_path / "state" / "download.json").read_text())
    assert summary == {
        "downloaded": 0, "reused": 0, "failures": 1, "retries": 2,
        "last_error": "TimeoutError", "failed_document_key": expected_key,
    }
    assert state == {
        "attempts": 3, "completed": False, "failed_document_key": expected_key,
        "last_error": "TimeoutError",
    }
    assert secret not in json.dumps(summary) and secret not in json.dumps(state)


def test_download_refuses_missing_url_instead_of_claiming_completion(tmp_path: Path) -> None:
    metadata = [_document("a-1")]
    metadata[0].pop("linkToFilingDetails")
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    summary = backfill.download(_Client([]), tmp_path, edgar_identity="Analyst analyst@example.test")

    state = json.loads((tmp_path / "state" / "download.json").read_text())
    assert summary["failures"] == 1 and summary["last_error"] == "missing_filing_url"
    assert summary["failed_document_key"] and state == {
        "attempts": 0, "completed": False,
        "failed_document_key": summary["failed_document_key"],
        "last_error": "missing_filing_url",
    }


def test_download_records_permanent_http_404_as_terminal_evidence_and_continues(tmp_path: Path) -> None:
    class PermanentHttpError(RuntimeError):
        code = 404

    metadata = [_document("a-1"), _document("a-2")]
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    client = _Client([
        PermanentHttpError("URL and server detail must not persist"),
        _Response({}, content=b"remaining immutable evidence"),
    ])

    summary = backfill.download(client, tmp_path, edgar_identity="Analyst analyst@example.test")

    document_key = backfill._document_key(
        metadata[0]["accessionNo"], metadata[0]["linkToFilingDetails"], metadata[0]["documentType"],
    )
    terminal = json.loads((tmp_path / "downloads.json").read_text(encoding="utf-8"))[0]
    state = json.loads((tmp_path / "state" / "download.json").read_text(encoding="utf-8"))
    assert summary["downloaded"] == 1 and summary["failures"] == 0 and summary["retries"] == 0
    assert summary["permanently_unavailable"] == 1 and len(client.calls) == 2
    assert terminal == {
        "accession": "a-1", "availability": {"reason": "http_404", "state": "permanently_unavailable"},
        "description": None, "document_key": document_key, "document_type": "EX-4.1",
        "filed_at": "2025-07-22T12:00:00-04:00", "filing_url": metadata[0]["linkToFilingDetails"],
        "parent_form": "424B2",
    }
    assert "raw_path" not in terminal and "document_hash" not in terminal
    assert state["completed"] and state["permanently_unavailable"] == 1
    assert "server detail" not in json.dumps(summary) and "server detail" not in json.dumps(terminal)


def test_download_reuses_terminal_permanent_evidence_without_another_request(tmp_path: Path) -> None:
    class PermanentHttpError(RuntimeError):
        code = 404

    metadata = [_document("a-1")]
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    first = backfill.download(
        _Client([PermanentHttpError("missing")]), tmp_path, edgar_identity="Analyst analyst@example.test",
    )
    retry_client = _Client([])
    second = backfill.download(retry_client, tmp_path, edgar_identity="Analyst analyst@example.test")

    assert first["permanently_unavailable"] == 1
    assert second["terminal_reused"] == 1 and second["permanently_unavailable"] == 1
    assert retry_client.calls == []


def test_download_does_not_reuse_cached_auth_error_as_terminal_evidence(tmp_path: Path) -> None:
    metadata = [_document("a-1")]
    document_key = backfill._document_key(
        metadata[0]["accessionNo"], metadata[0]["linkToFilingDetails"], metadata[0]["documentType"],
    )
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    (tmp_path / "downloads.json").write_text(json.dumps([{
        "accession": "a-1", "availability": {
            "reason": "http_401", "state": "permanently_unavailable",
        },
        "document_key": document_key, "document_type": "EX-4.1",
        "filing_url": metadata[0]["linkToFilingDetails"],
    }]), encoding="utf-8")

    client = _Client([_Response({}, content=b"authenticated immutable evidence")])
    summary = backfill.download(client, tmp_path, edgar_identity="Analyst analyst@example.test")

    assert summary["downloaded"] == 1 and summary.get("terminal_reused", 0) == 0
    assert len(client.calls) == 1
    persisted = json.loads((tmp_path / "downloads.json").read_text(encoding="utf-8"))[0]
    assert "availability" not in persisted and persisted["document_hash"]


@pytest.mark.parametrize("status", [401, 403, 407])
def test_download_authentication_http_errors_remain_fail_closed(tmp_path: Path, status: int) -> None:
    error_type = type("AuthenticationHttpError", (RuntimeError,), {"code": status})
    metadata = [_document("a-1"), _document("a-2")]
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    client = _Client([error_type("credential detail must not persist")])

    summary = backfill.download(client, tmp_path, edgar_identity="Analyst analyst@example.test")

    assert summary["failures"] == 1 and summary["last_error"] == f"http_{status}"
    assert len(client.calls) == 1
    assert not (tmp_path / "downloads.json").exists()
    assert not json.loads((tmp_path / "state" / "download.json").read_text(encoding="utf-8"))["completed"]
    assert "credential detail" not in json.dumps(summary)


def test_parse_and_manifest_seal_report_terminal_unavailable_evidence_without_approval(tmp_path: Path) -> None:
    class PermanentHttpError(RuntimeError):
        code = 404

    metadata = [_document("a-1")]
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    backfill.download(
        _Client([PermanentHttpError("missing")]), tmp_path, edgar_identity="Analyst analyst@example.test",
    )

    parsed = backfill.parse(tmp_path)
    exported = backfill.adjudication_export(tmp_path)
    payload = json.loads((tmp_path / "adjudication" / "manifest.json").read_text(encoding="utf-8"))
    sealed = backfill.seal_adjudication(tmp_path)
    bundle = backfill.build_registry_bundle(tmp_path)

    assert parsed == {"records": 0, "candidates": 0, "permanently_unavailable": 1}
    assert exported["permanently_unavailable"] == 1
    assert payload["records"] == []
    assert payload["permanently_unavailable"]["count"] == 1
    assert payload["permanently_unavailable"]["records"][0]["availability"]["reason"] == "http_404"
    assert payload["permanently_unavailable"]["sha256"] == hashlib.sha256(
        backfill.canonical_json(payload["permanently_unavailable"]["records"]).encode("utf-8")
    ).hexdigest()
    assert payload["sha256"] != hashlib.sha256(backfill.canonical_json([]).encode("utf-8")).hexdigest()
    assert sealed["permanently_unavailable"] == 1
    assert sealed["permanently_unavailable_sha256"] == payload["permanently_unavailable"]["sha256"]
    assert bundle["source_evidence_rows"] == bundle["pair_decision_rows"] == []


def test_terminal_unavailable_evidence_cannot_be_removed_or_rewritten_before_publish(tmp_path: Path) -> None:
    class PermanentHttpError(RuntimeError):
        code = 404

    (tmp_path / "metadata.json").write_text(json.dumps([_document("a-1")]), encoding="utf-8")
    backfill.download(
        _Client([PermanentHttpError("missing")]), tmp_path, edgar_identity="Analyst analyst@example.test",
    )
    backfill.parse(tmp_path)
    backfill.adjudication_export(tmp_path)
    manifest_path = tmp_path / "adjudication" / "manifest.json"
    original = json.loads(manifest_path.read_text(encoding="utf-8"))

    removed = {key: value for key, value in original.items() if key != "permanently_unavailable"}
    manifest_path.write_text(json.dumps(removed), encoding="utf-8")
    with pytest.raises(ValueError, match="invalid permanently unavailable evidence"):
        backfill.seal_adjudication(tmp_path)
    with pytest.raises(ValueError, match="invalid permanently unavailable evidence"):
        backfill.build_registry_bundle(tmp_path)

    rewritten = json.loads(json.dumps(original))
    rewritten["permanently_unavailable"]["records"][0]["filing_url"] = "https://example.invalid/forged"
    rewritten["permanently_unavailable"]["sha256"] = hashlib.sha256(
        backfill.canonical_json(rewritten["permanently_unavailable"]["records"]).encode("utf-8")
    ).hexdigest()
    rewritten["sha256"] = backfill._adjudication_digest(
        rewritten["records"], rewritten["permanently_unavailable"],
    )
    manifest_path.write_text(json.dumps(rewritten), encoding="utf-8")
    with pytest.raises(ValueError, match="invalid permanently unavailable evidence"):
        backfill.seal_adjudication(tmp_path)
    with pytest.raises(ValueError, match="invalid permanently unavailable evidence"):
        backfill.build_registry_bundle(tmp_path)


def test_download_retries_http_request_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RequestTimeout(RuntimeError):
        code = 408

    metadata = [_document("a-1")]
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    client = _Client([RequestTimeout("temporary"), _Response({}, content=b"evidence")])
    monkeypatch.setattr(backfill, "_DOWNLOAD_RETRY_DELAYS", (0.0,))

    summary = backfill.download(client, tmp_path, edgar_identity="Analyst analyst@example.test")

    assert summary["downloaded"] == 1 and summary["failures"] == 0
    assert summary["retries"] == 1 and len(client.calls) == 2


def test_parse_official_minimal_table_groups_separate_identification_rows_per_currency_section() -> None:
    records = backfill.parse_document(
        FIXTURES.joinpath("official_minimal_table.html").read_bytes(), document_hash="abc", accession="000119312525175440"
    )

    assert [record["status"] for record in records] == ["candidate", "candidate", "candidate"]
    usd, eur, gbp = records
    assert usd["block_locator"] == "table[0]/identification-numbers"
    assert usd["reg_s"][2]["normalized_value"] == "USG35906AC33"
    assert usd["reg_s"][3]["source_value"] == "G35906 AC3"
    assert usd["reg_s"][3]["normalized_value"] == "G35906AC3"
    assert usd["rule_144a"][1]["normalized_value"] == "344045AB5"
    assert eur["reg_s"][3]["identifier_label"] == "Common Code"
    assert eur["rule_144a"][1]["normalized_value"] == "304981598"
    assert gbp["reg_s"][0]["tenure"] == "temporary"
    assert gbp["reg_s"][2]["tenure"] == "permanent"
    assert all(record["document_hash"] == "abc" for record in records)


def test_parser_segments_currency_sections_inside_one_identification_numbers_table() -> None:
    records = backfill.parse_document(
        FIXTURES.joinpath("multi_section_identification_table.html").read_bytes(), document_hash="abc", accession="accession"
    )

    assert [record["status"] for record in records] == ["candidate", "candidate"]
    assert records[0]["block_locator"] != records[1]["block_locator"]
    assert [item["normalized_value"] for item in records[0]["rule_144a"]] == ["US344045AB55", "344045AB5"]
    assert [item["normalized_value"] for item in records[1]["reg_s"]] == ["XS3049816013", "304981601"]


def test_parser_keeps_generic_series_and_unsupported_currency_sections_separate() -> None:
    records = backfill.parse_document(
        FIXTURES.joinpath("generic_section_boundaries.html").read_bytes(), document_hash="abc", accession="accession"
    )

    assert [record["status"] for record in records] == ["ambiguous", "ambiguous", "ambiguous", "ambiguous"]
    assert all(record["reason"] == "missing_paired_side" for record in records)
    assert [record["reg_s"] for record in records] == [
        [{"identifier_label": "ISIN", "source_value": "USG35906AC33", "normalized_value": "USG35906AC33", "tenure": "not_stated", "exact_label": "Reg S ISIN"}],
        [],
        [{"identifier_label": "ISIN", "source_value": "XS3049816013", "normalized_value": "XS3049816013", "tenure": "not_stated", "exact_label": "Reg S ISIN"}],
        [],
    ]


def test_parser_refuses_to_assign_a_bare_common_code_to_the_preceding_side() -> None:
    source = b"""
    <table><tr><th>Identification numbers</th></tr>
    <tr><td>Reg S ISIN</td><td>XS3049816013</td><td>Common Code</td><td>304981601</td></tr>
    <tr><td>Rule 144A ISIN</td><td>XS3049815981</td><td>Common Code</td><td>304981598</td></tr></table>
    """

    records = backfill.parse_document(source, document_hash="hash", accession="accession")

    assert [record["status"] for record in records] == ["ambiguous"]
    assert records[0]["reason"] == "bare_common_code_without_side"
    assert records[0]["reg_s"] == [
        {"identifier_label": "ISIN", "source_value": "XS3049816013", "normalized_value": "XS3049816013", "tenure": "not_stated", "exact_label": "Reg S ISIN"}
    ]
    assert records[0]["rule_144a"] == [
        {"identifier_label": "ISIN", "source_value": "XS3049815981", "normalized_value": "XS3049815981", "tenure": "not_stated", "exact_label": "Rule 144A ISIN"}
    ]
    assert records[0]["unpaired_identifiers"] == [
        {"identifier_label": "Common Code", "source_value": "304981601", "normalized_value": "304981601", "reason": "missing_distribution_side"},
        {"identifier_label": "Common Code", "source_value": "304981598", "normalized_value": "304981598", "reason": "missing_distribution_side"},
    ]


def test_parser_does_not_treat_common_code_prose_as_identifier_evidence() -> None:
    records = backfill.parse_document(
        b"<p>The Common Code numbers are supplied elsewhere.</p>", document_hash="hash", accession="accession"
    )

    assert records == [{
        "status": "zero_match", "reason": "no_explicit_labeled_identifiers", "accession": "accession",
        "block_locator": "text[0]", "document_hash": "hash", "parser_version": backfill.PARSER_VERSION,
        "reg_s": [], "rule_144a": [], "unpaired_identifiers": [],
    }]


def test_parser_handles_cins_common_code_and_explicit_tenure_without_identifier_shape_inference() -> None:
    source = b"""
    <table><tr><td>Temporary Regulation S CINS AB12CD345</td><td>Permanent Rule 144A CUSIP 999999AA0</td></tr></table>
    <table><tr><td>Regulation S Common Code 123456789</td><td>144A Common Code 987654321</td></tr></table>
    <p>US111111AA11 XS2222222222 333333AA3</p>
    """

    records = backfill.parse_document(source, document_hash="hash", accession="accession")

    assert [record["status"] for record in records] == ["candidate", "candidate", "zero_match"]
    assert records[0]["reg_s"][0]["identifier_label"] == "CINS"
    assert records[0]["reg_s"][0]["tenure"] == "temporary"
    assert records[0]["rule_144a"][0]["tenure"] == "permanent"
    assert records[1]["reg_s"][0]["identifier_label"] == "Common Code"


def test_parser_preserves_explicit_6_2_1_cusip_and_cins_display_groups_only() -> None:
    source = b"""
    <table><tr><td>Reg S CINS G35906 AC 3</td><td>Rule 144A CUSIP 344045 AB 5</td></tr></table>
    <table><tr><td>Reg S CINS G35906 AC 33</td><td>Rule 144A CUSIP 344045 AB 55</td></tr></table>
    """

    records = backfill.parse_document(source, document_hash="hash", accession="accession")

    assert records[0]["status"] == "candidate"
    assert records[0]["reg_s"][0]["source_value"] == "G35906 AC 3"
    assert records[0]["reg_s"][0]["normalized_value"] == "G35906AC3"
    assert records[0]["rule_144a"][0]["source_value"] == "344045 AB 5"
    assert records[0]["rule_144a"][0]["normalized_value"] == "344045AB5"
    assert [item["source_value"] for item in records[1]["reg_s"] + records[1]["rule_144a"]] == [
        "G35906", "344045",
    ]


def test_parser_refuses_cross_block_pairing_and_marks_duplicates_ambiguous() -> None:
    source = b"""
    <table><tr><td>Regulation S ISIN USG35906AC33</td></tr></table>
    <table><tr><td>Rule 144A ISIN US344045AB55</td></tr></table>
    <table><tr><td>Reg S ISIN XS1111111111 Reg S ISIN XS2222222222 Rule 144A ISIN XS3333333333</td></tr></table>
    """

    records = backfill.parse_document(source, document_hash="hash", accession="accession")

    assert [record["status"] for record in records] == ["ambiguous", "ambiguous", "ambiguous"]
    assert all(record["reason"] in {"missing_paired_side", "duplicate_identifier_label"} for record in records)


def test_parse_and_adjudication_manifest_are_stable_and_keep_ambiguous_unapproved(tmp_path: Path) -> None:
    raw = FIXTURES.joinpath("official_minimal_table.html").read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / f"{digest}.bin").write_bytes(raw)
    (tmp_path / "downloads.json").write_text(json.dumps([{
        "accession": "a-1", "document_key": "document-a-1", "document_hash": digest,
        "raw_path": f"raw/{digest}.bin",
    }]))

    first = backfill.parse(tmp_path)
    manifest_one = backfill.adjudication_export(tmp_path)
    second = backfill.parse(tmp_path)
    manifest_two = backfill.adjudication_export(tmp_path)

    assert first == second
    assert manifest_one == manifest_two
    payload = json.loads((tmp_path / "adjudication" / "manifest.json").read_text())
    assert payload["sha256"] == hashlib.sha256(backfill.canonical_json(payload["records"]).encode()).hexdigest()
    assert all(record["adjudication"] == "pending" for record in payload["records"])


def test_publish_dry_run_emits_only_explicitly_approved_schema_compatible_rows(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "adjudication").mkdir()
    approved = {
        "status": "candidate", "accession": "a-1", "block_locator": "table[0]/row[0]",
        "adjudication": "approved", "draft_snapshot_id": "snapshot-draft", "valid_from": "2025-01-01", "parent_form": "424B2",
        "document_type": "EX-4.1", "filed_at": "2025-07-22T12:00:00-04:00", "document_hash": "a" * 64,
        "parser_version": "explicit-label-v1",
        "evidence_link": {"filing_url": "https://www.sec.gov/example.htm", "retrieved_at": "2025-07-23T00:00:00+00:00"},
        "reg_s": [
            {"identifier_label": "CINS", "exact_label": "Reg S CINS", "source_value": "G35906AC3", "normalized_value": "G35906AC3", "tenure": "not_stated"},
            {"identifier_label": "Common Code", "exact_label": "Reg S Common Code", "source_value": "304981601", "normalized_value": "304981601", "tenure": "not_stated"},
        ],
        "rule_144a": [
            {"identifier_label": "CUSIP", "exact_label": "Rule 144A CUSIP", "source_value": "344045AB5", "normalized_value": "344045AB5", "tenure": "not_stated"},
            {"identifier_label": "ISIN", "exact_label": "Rule 144A ISIN", "source_value": "US344045AB55", "normalized_value": "US344045AB55", "tenure": "not_stated"},
        ],
    }
    pending = {**approved, "adjudication": "pending"}
    ambiguous = {**approved, "status": "ambiguous", "adjudication": "pending"}
    rejected = {**approved, "adjudication": "rejected"}
    records = [approved, pending, ambiguous, rejected]
    (tmp_path / "adjudication" / "manifest.json").write_text(json.dumps({"records": records, "sha256": hashlib.sha256(backfill.canonical_json(records).encode()).hexdigest()}))

    summary = backfill.publish(tmp_path, dry_run=True, draft_snapshot_id="snapshot-draft")

    assert summary == {"approved_records": 1, "database_writes": 0, "dry_run": True, "skipped_records": 3}
    printed = json.loads(capsys.readouterr().out)
    assert printed["mapping_snapshot_rows"] == [{
        "content_hash": printed["snapshot_approval_rows"][0]["content_hash"],
        "snapshot_id": "snapshot-draft", "snapshot_status": "draft",
    }]
    assert printed["source_evidence_rows"][0]["form_type"] == "424B2"
    assert all(row["observation_state"] == "validated" for row in printed["parser_observation_rows"])
    assert printed["pair_decision_rows"][0]["snapshot_id"] == "snapshot-draft"
    assert {row["identifier_kind"] for row in printed["pair_identifier_rows"]} == {"cusip9", "isin", "common_code"}
    assert {row["identifier_value"] for row in printed["pair_identifier_rows"]} >= {"G35906AC3", "344045AB5"}
    assert [item["reason"] for item in printed["skipped_records"]] == ["pending", "ambiguous", "rejected"]
    from src.bonds.distribution_series import (
        DistributionMappingSnapshot, DistributionPairDecision, DistributionPairIdentifier,
        DistributionSnapshotApproval, validate_distribution_snapshot_approval,
    )
    decisions = [DistributionPairDecision(
        row["decision_id"], row["snapshot_id"], row["decision_state"], row["source_observation_id"],
        date.fromisoformat(row["valid_from"]), date.fromisoformat(row["valid_to"]) if row["valid_to"] else None,
        row["pair_key"],
    ) for row in printed["pair_decision_rows"]]
    identifiers = [DistributionPairIdentifier(
        row["identifier_id"], row["decision_id"], row["source_observation_id"], row["distribution_rule"],
        row["identifier_kind"], row["identifier_value"], row["identifier_tenure"],
        date.fromisoformat(row["valid_from"]), date.fromisoformat(row["valid_to"]) if row["valid_to"] else None,
    ) for row in printed["pair_identifier_rows"]]
    snapshot_row = printed["mapping_snapshot_rows"][0]
    approval_row = printed["snapshot_approval_rows"][0]
    assert validate_distribution_snapshot_approval(
        DistributionMappingSnapshot(snapshot_row["snapshot_id"], snapshot_row["snapshot_status"], snapshot_row["content_hash"]),
        DistributionSnapshotApproval(approval_row["snapshot_id"], approval_row["content_hash"]), decisions, identifiers,
    ) == snapshot_row["content_hash"]
    with pytest.raises(backfill.PublishRefused):
        backfill.publish(tmp_path, dry_run=False)


def test_build_registry_bundle_matches_dry_run_payload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "adjudication").mkdir()
    record = _approved_preview_record()
    records = [record]
    (tmp_path / "adjudication" / "manifest.json").write_text(json.dumps({
        "records": records, "sha256": hashlib.sha256(backfill.canonical_json(records).encode()).hexdigest(),
    }), encoding="utf-8")

    bundle = backfill.build_registry_bundle(tmp_path, "snapshot-draft")
    backfill.publish(tmp_path, dry_run=True, draft_snapshot_id="snapshot-draft")

    assert bundle["database_writes"] == 0
    assert bundle == json.loads(capsys.readouterr().out)


def test_publish_dry_run_never_turns_pending_candidate_into_registry_facts(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "adjudication").mkdir()
    records = [{
        "status": "candidate", "adjudication": "pending", "accession": "a-1", "block_locator": "block",
        "reg_s": [], "rule_144a": [],
    }]
    (tmp_path / "adjudication" / "manifest.json").write_text(json.dumps({"records": records, "sha256": hashlib.sha256(backfill.canonical_json(records).encode()).hexdigest()}))

    summary = backfill.publish(tmp_path, dry_run=True)

    printed = json.loads(capsys.readouterr().out)
    assert summary["approved_records"] == 0 and summary["database_writes"] == 0
    assert printed["source_evidence_rows"] == printed["pair_identifier_rows"] == []
    assert printed["skipped_records"] == [{"reason": "pending", "record_id": backfill.record_id(records[0])}]


def test_seal_adjudication_preserves_human_decision_then_enables_dry_run(tmp_path: Path) -> None:
    (tmp_path / "adjudication").mkdir()
    record = {
        "status": "candidate", "adjudication": "approved", "draft_snapshot_id": "snapshot-draft",
        "valid_from": "2025-01-01", "accession": "a-1", "block_locator": "table[0]/row[0]",
        "parent_form": "424B2", "document_type": "EX-4.1", "document_hash": "a" * 64,
        "parser_version": "explicit-label-v1",
        "evidence_link": {"filing_url": "https://www.sec.gov/example.htm", "retrieved_at": "2025-07-23T00:00:00+00:00"},
        "reg_s": [{"identifier_label": "CUSIP", "exact_label": "Reg S CUSIP", "source_value": "G35906AC3", "normalized_value": "G35906AC3", "tenure": "not_stated"}],
        "rule_144a": [{"identifier_label": "CUSIP", "exact_label": "Rule 144A CUSIP", "source_value": "344045AB5", "normalized_value": "344045AB5", "tenure": "not_stated"}],
    }
    records = [record]
    manifest = tmp_path / "adjudication" / "manifest.json"
    manifest.write_text(json.dumps({"records": records, "sha256": "0" * 64}), encoding="utf-8")

    with pytest.raises(ValueError, match="manifest checksum mismatch"):
        backfill.publish(tmp_path, dry_run=True)
    sealed = backfill.seal_adjudication(tmp_path)
    persisted = json.loads(manifest.read_text())

    assert persisted["records"] == records
    assert persisted["sha256"] == sealed["sha256"]
    assert (tmp_path / "adjudication" / "manifest.sha256").read_text().strip() == sealed["sha256"]
    assert backfill.publish(tmp_path, dry_run=True)["approved_records"] == 1


@pytest.mark.parametrize("adjudication", ["", "candidate", "invalid"])
def test_seal_adjudication_refuses_invalid_human_state(tmp_path: Path, adjudication: str) -> None:
    (tmp_path / "adjudication").mkdir()
    records = [{"status": "candidate", "adjudication": adjudication, "accession": "a-1"}]
    (tmp_path / "adjudication" / "manifest.json").write_text(json.dumps({"records": records, "sha256": "0" * 64}))

    with pytest.raises(ValueError, match="invalid adjudication"):
        backfill.seal_adjudication(tmp_path)


def test_publish_has_no_approved_snapshot_alias_and_cli_accepts_only_draft_snapshot_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    actual_publish = backfill.publish

    def fake_publish(_root: Path, *, dry_run: bool, draft_snapshot_id: str | None = None) -> dict[str, int | bool]:
        captured.update({"dry_run": dry_run, "draft_snapshot_id": draft_snapshot_id})
        return {"approved_records": 0, "database_writes": 0, "dry_run": True, "skipped_records": 0}

    monkeypatch.setattr(backfill, "publish", fake_publish)
    assert backfill.main([
        "--output-root", str(tmp_path), "publish", "--dry-run", "--draft-snapshot-id", "snapshot-draft",
    ]) == 0
    assert captured == {"dry_run": True, "draft_snapshot_id": "snapshot-draft"}
    with pytest.raises(TypeError):
        actual_publish(tmp_path, dry_run=True, approved_snapshot_id="snapshot-approved")  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("label", "value", "reason"),
    [
        ("CUSIP", "344045AB", "invalid cusip9"),
        ("CINS", "g35906ac3", "invalid cusip9"),
        ("CUSIP", "000000000", "invalid cusip9"),
        ("CINS", "XXXXXXXXX", "invalid cusip9"),
        ("CUSIP", "NNNNNNNNN", "invalid cusip9"),
        ("CINS", "999999999", "invalid cusip9"),
        ("ISIN", "US344045AB5", "invalid isin"),
        ("ISIN", "XXXXXXXXXXXX", "invalid isin"),
        ("ISIN", "123456789012", "invalid isin"),
        ("Common Code", "30498160X", "invalid common code"),
    ],
)
def test_schema_preview_refuses_malformed_normalized_identifiers(
    label: str, value: str, reason: str,
) -> None:
    with pytest.raises(ValueError, match=reason):
        backfill._prospective_registry_rows(_approved_preview_record(label=label, value=value), "snapshot-draft")
