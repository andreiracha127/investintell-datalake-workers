"""Resumable, non-production SEC evidence collection for 144A-to-Reg-S series.

The pipeline intentionally produces evidence artifacts only.  It does not
write a database, create identifiers, or approve an inferred relationship.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timezone
import gzip
import hashlib
from html import unescape
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Protocol
from urllib.request import Request, urlopen


SEC_API_FULL_TEXT_SEARCH = "https://api.sec-api.io/full-text-search"
PARSER_VERSION = "explicit-label-v1"
TARGETED_QUERY_VERSION = "targeted-exact-v1"
SEARCH_QUERY_VERSIONS = (
    {"version": "v1", "query": '"Rule 144A CUSIP" "Regulation S ISIN"'},
    {"version": "v2", "query": '"144A CUSIP" "Reg S ISIN"'},
    {"version": "v3", "query": '"Rule 144A" "Regulation S" "Common Code"'},
)
_LABEL = re.compile(
    r"(?:(?P<tenure>Temporary|Permanent)\s+)?"
    r"(?P<side>Rule\s*144A|144A|Regulation\s*S|Reg\s*S)\s+"
    r"(?P<label>ISIN|CUSIP|CINS|Common\s+Code)\s*[:\-]?\s*"
    r"(?P<value>(?:[A-Za-z0-9]{6}\s+[A-Za-z0-9]{2}\s+[A-Za-z0-9]\b)|[A-Za-z0-9]+(?:\s+(?!Rule\b|Reg\b|Regulation\b)[A-Za-z0-9]{3}\b)?)",
    re.IGNORECASE,
)
_BARE_COMMON_CODE = re.compile(r"\b(Common\s+Code)\s*[:\-]?\s*([0-9]{9})\b", re.IGNORECASE)


class PublishRefused(RuntimeError):
    """Raised because this evidence tool is never a production publisher."""


class HttpClient(Protocol):
    def post_json(self, url: str, payload: dict[str, object], headers: dict[str, str]) -> Any: ...

    def get_bytes(self, url: str, headers: dict[str, str]) -> Any: ...


@dataclass(frozen=True)
class _UrlResponse:
    content: bytes
    headers: dict[str, str]
    status: int

    def json(self) -> object:
        content_encoding = next(
            (value for key, value in self.headers.items() if key.lower() == "content-encoding"), ""
        )
        encoded_gzip = any(value.strip().lower() == "gzip" for value in content_encoding.split(","))
        payload = gzip.decompress(self.content) if encoded_gzip or self.content.startswith(b"\x1f\x8b") else self.content
        return json.loads(payload.decode("utf-8"))


class UrllibClient:
    """Small transport seam; tests supply an in-memory client instead."""

    def post_json(self, url: str, payload: dict[str, object], headers: dict[str, str]) -> _UrlResponse:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = Request(url, data=body, headers={**headers, "Content-Type": "application/json"})
        with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed SEC endpoint from caller
            return _UrlResponse(response.read(), dict(response.headers.items()), response.status)

    def get_bytes(self, url: str, headers: dict[str, str]) -> _UrlResponse:
        with urlopen(Request(url, headers=headers), timeout=30) as response:  # noqa: S310 - URL from SEC metadata
            return _UrlResponse(response.read(), dict(response.headers.items()), response.status)


def canonical_json(value: object) -> str:
    """Return the stable serialization used for every durable JSON artifact."""
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _read_json(path: Path, default: Any) -> Any:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(canonical_json(value) + "\n", encoding="utf-8")
    temporary.replace(path)


def _write_text(path: Path, value: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding=encoding)
    temporary.replace(path)


def _safe_error(_: BaseException) -> str:
    """Keep credentials and server diagnostics out of checkpoints and stdout."""
    return "request_failed"


def _document_key(accession: str, document_url: str | None, document_type: str | None) -> str:
    """A filing accession can contain several independently relevant exhibits."""
    source = canonical_json([accession, document_url or "", document_type or ""])
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def _metadata_entry(document: dict[str, Any]) -> dict[str, str | None]:
    accession = str(document.get("accessionNo") or document.get("accession") or "").strip()
    document_type = _optional(document, "documentType", "type")
    document_url = _optional(
        document, "linkToDocument", "documentUrl", "linkToFilingDetails", "filingUrl", "link"
    )
    return {
        "accession": accession,
        "document_key": _document_key(accession, document_url, document_type),
        "parent_form": _optional(document, "parentFormType", "formType", "form"),
        "document_type": document_type,
        "filed_at": _optional(document, "filedAt", "filed_at"),
        "filing_url": document_url,
        "description": _optional(document, "description", "documentDescription"),
    }


def _optional(document: dict[str, Any], *names: str) -> str | None:
    for name in names:
        value = document.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _response_documents(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    for name in ("filings", "documents", "data"):
        value = payload.get(name)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _metadata_by_document_key(path: Path) -> dict[str, dict[str, str | None]]:
    return {item["document_key"]: item for item in _read_json(path, []) if item.get("document_key")}


def _add_document_metadata(
    known: dict[str, dict[str, str | None]], documents: list[dict[str, Any]]
) -> int:
    """Use the same exhibit-level dedupe for broad and reference-targeted search."""
    added = 0
    for document in documents:
        entry = _metadata_entry(document)
        if entry["accession"] and entry["document_key"] not in known:
            known[entry["document_key"]] = entry
            added += 1
    return added


def discover(
    client: HttpClient,
    output_root: Path,
    *,
    api_key: str,
    start_date: str,
    end_date: str,
    limit: int,
) -> dict[str, int]:
    """Query every versioned search phrase and checkpoint after each successful page."""
    if not api_key:
        raise ValueError("SEC API credential is required")
    if limit < 1:
        raise ValueError("broad discovery limit must be positive")
    state_path = output_root / "state" / "discover.json"
    state = _read_json(state_path, {"query_index": 0, "page": 1, "total_documents": 0, "completed": False})
    state["page"] = max(1, int(state.get("page", 1)))
    metadata_path = output_root / "metadata.json"
    known = _metadata_by_document_key(metadata_path)
    added = failures = requests = 0
    while state["query_index"] < len(SEARCH_QUERY_VERSIONS) and requests < limit:
        query = SEARCH_QUERY_VERSIONS[state["query_index"]]
        payload: dict[str, object] = {
            "query": query["query"], "startDate": start_date, "endDate": end_date, "page": state["page"],
        }
        requests += 1
        try:
            response = client.post_json(SEC_API_FULL_TEXT_SEARCH, payload, {"Authorization": api_key})
            documents = _response_documents(response.json())
        except Exception as exc:  # transport errors are nonterminal and checkpointed in-place
            state["last_error"] = _safe_error(exc)
            _write_json(state_path, state)
            failures += 1
            break
        added += _add_document_metadata(known, documents)
        state["total_documents"] += len(documents)
        state.pop("last_error", None)
        if documents:
            state["page"] += 1
        else:
            state["query_index"] += 1
            state["page"] = 1
        _write_json(state_path, state)
    state["completed"] = state["query_index"] >= len(SEARCH_QUERY_VERSIONS)
    _write_json(state_path, state)
    _write_json(metadata_path, [known[key] for key in sorted(known)])
    return {
        "budget": limit, "completed_queries": state["query_index"],
        "documents_added": added, "failures": failures, "requests": requests,
    }


def normalize_reference_cusip9(value: str) -> str:
    """Normalize an input CUSIP9 without treating it as a relationship proof."""
    normalized = re.sub(r"[^A-Za-z0-9]", "", value).upper()
    if not re.fullmatch(r"[A-Z0-9]{9}", normalized):
        raise ValueError(f"invalid reference CUSIP9: {value!r}")
    return normalized


def targeted_query_variants(cusip9: str) -> tuple[str, str]:
    """Return the compact and conventional 6-2-1 filing spellings for one CUSIP9."""
    normalized = normalize_reference_cusip9(cusip9)
    formatted = f"{normalized[:6]} {normalized[6:8]} {normalized[8]}"
    return (
        f'"{normalized}" "Rule 144A" "Reg S"',
        f'"{formatted}" "Rule 144A" "Reg S"',
    )


def targeted_scope_fingerprint(
    references: list[str], *, start_date: str, end_date: str
) -> str:
    """Bind a resume ledger to every input that can alter targeted search semantics."""
    scope = {
        "end_date": end_date,
        "parser_version": PARSER_VERSION,
        "reference_cusips": references,
        "search_query_versions": SEARCH_QUERY_VERSIONS,
        "start_date": start_date,
        "targeted_query_templates": [
            '"{cusip9}" "Rule 144A" "Reg S"',
            '"{cusip6} {cusip2} {check}" "Rule 144A" "Reg S"',
        ],
        "targeted_query_version": TARGETED_QUERY_VERSION,
    }
    return hashlib.sha256(canonical_json(scope).encode("utf-8")).hexdigest()


def targeted_discover(
    client: HttpClient,
    output_root: Path,
    *,
    api_key: str,
    reference_cusips: list[str],
    start_date: str,
    end_date: str,
    limit: int,
) -> dict[str, int]:
    """Run a bounded, resumable exact-query sweep over an explicit reference list.

    Search results are document candidates only: they intentionally do not set a
    mapping status for any reference CUSIP.
    """
    if not api_key:
        raise ValueError("SEC API credential is required")
    if limit < 1:
        raise ValueError("targeted discovery limit must be positive")
    references = sorted({normalize_reference_cusip9(value) for value in reference_cusips})
    scope_fingerprint = targeted_scope_fingerprint(references, start_date=start_date, end_date=end_date)
    state_path = output_root / "state" / "targeted-discover.json"
    state = _read_json(state_path, {})
    if state and state.get("scope_fingerprint") != scope_fingerprint:
        raise ValueError("targeted discovery checkpoint scope mismatch")
    if not state:
        state = {
            "completed": False, "page": 1, "reference_index": 0,
            "scope_fingerprint": scope_fingerprint, "total_documents": 0, "variant_index": 0,
        }
    state["page"] = max(1, int(state.get("page", 1)))
    metadata_path = output_root / "metadata.json"
    known = _metadata_by_document_key(metadata_path)
    requests = failures = added = completed_references = 0
    while state["reference_index"] < len(references) and requests < limit:
        reference = references[state["reference_index"]]
        variants = targeted_query_variants(reference)
        query = variants[state["variant_index"]]
        payload: dict[str, object] = {
            "query": query, "startDate": start_date, "endDate": end_date, "page": state["page"],
        }
        requests += 1
        try:
            response = client.post_json(SEC_API_FULL_TEXT_SEARCH, payload, {"Authorization": api_key})
            documents = _response_documents(response.json())
        except Exception:
            _write_json(state_path, state)
            failures += 1
            break
        added += _add_document_metadata(known, documents)
        state["total_documents"] += len(documents)
        if documents:
            state["page"] += 1
        else:
            state["variant_index"] += 1
            state["page"] = 1
            if state["variant_index"] >= len(variants):
                state["reference_index"] += 1
                state["variant_index"] = 0
                completed_references += 1
        _write_json(state_path, state)
    state["completed"] = state["reference_index"] >= len(references)
    _write_json(state_path, state)
    _write_json(metadata_path, [known[key] for key in sorted(known)])
    return {
        "budget": limit, "documents_added": added, "failures": failures,
        "requests": requests, "references_completed": completed_references,
    }


def download(client: HttpClient, output_root: Path, *, edgar_identity: str) -> dict[str, int]:
    """Fetch filing HTML only after discovery, retaining byte-exact immutable evidence."""
    if not edgar_identity.strip():
        raise ValueError("SEC_USER_AGENT or EDGAR_IDENTITY is required")
    metadata = sorted(_read_json(output_root / "metadata.json", []), key=lambda item: item.get("document_key", ""))
    downloads_path = output_root / "downloads.json"
    existing = {item["document_key"]: item for item in _read_json(downloads_path, []) if item.get("document_key")}
    downloaded = reused = failures = 0
    for document in metadata:
        accession = document.get("accession") or document.get("accessionNo")
        url = document.get("filing_url") or document.get("linkToFilingDetails")
        if not accession or not url:
            continue
        document_type = document.get("document_type") or document.get("documentType")
        document_key = document.get("document_key") or _document_key(accession, url, document_type)
        old = existing.get(document_key)
        if old and (output_root / old["raw_path"]).exists():
            reused += 1
            continue
        try:
            response = client.get_bytes(url, {"User-Agent": edgar_identity, "Accept-Encoding": "identity"})
            raw = bytes(response.content)
        except Exception:
            failures += 1
            break
        digest = hashlib.sha256(raw).hexdigest()
        raw_path = output_root / "raw" / f"{digest}.bin"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if not raw_path.exists():
            raw_path.write_bytes(raw)
        existing[document_key] = {
            "accession": accession, "document_key": document_key, "document_type": document_type,
            "description": document.get("description"), "parent_form": document.get("parent_form") or document.get("formType"),
            "filed_at": document.get("filed_at") or document.get("filedAt"), "filing_url": url,
            "document_hash": digest, "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "raw_path": raw_path.relative_to(output_root).as_posix(), "status": getattr(response, "status", None),
            "headers": dict(getattr(response, "headers", {})),
        }
        _write_json(downloads_path, [existing[key] for key in sorted(existing)])
        downloaded += 1
    return {"downloaded": downloaded, "reused": reused, "failures": failures}


class _TableRows(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag == "table" and self._table is None:
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._cell is not None and self._row is not None:
            self._row.append("".join(self._cell))
            self._cell = None
        elif tag == "tr" and self._row is not None and self._table is not None:
            self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            self.tables.append(self._table)
            self._table = None


_SECTION_TITLE = re.compile(r"\b(?:series|tranche|notes|bonds|securities)\b", re.IGNORECASE)


def _identification_blocks(table_index: int, rows: list[str]) -> list[tuple[str, str]]:
    """Split one table at explicit identification or currency/issue boundaries."""
    headers = [
        index for index, row in enumerate(rows)
        if re.search(r"\bIdentification\s+numbers\b", row, re.IGNORECASE)
    ]
    if not headers:
        return []
    blocks: list[tuple[str, str]] = []
    for header_position, header in enumerate(headers):
        end = headers[header_position + 1] if header_position + 1 < len(headers) else len(rows)
        titles = [
            index for index in range(header + 1, end)
            if _SECTION_TITLE.search(rows[index])
        ]
        starts = titles or [header]
        for section_index, start in enumerate(starts):
            section_end = starts[section_index + 1] if section_index + 1 < len(starts) else end
            section_text = " ".join(row for row in rows[start:section_end] if row)
            if not section_text:
                continue
            locator = f"table[{table_index}]/identification-numbers"
            if len(headers) > 1 or len(starts) > 1:
                locator += f"[{header_position}:{section_index}]"
            blocks.append((locator, section_text))
    return blocks


def _blocks(raw: bytes) -> list[tuple[str, str]]:
    text = raw.decode("utf-8", errors="replace")
    parser = _TableRows()
    parser.feed(text)
    blocks: list[tuple[str, str]] = []
    for table_index, table in enumerate(parser.tables):
        rows = [" ".join(cell for cell in row if cell.strip()) for row in table]
        identification_blocks = _identification_blocks(table_index, rows)
        if identification_blocks:
            blocks.extend(identification_blocks)
        else:
            blocks.extend((f"table[{table_index}]/row[{row_index}]", row) for row_index, row in enumerate(rows))
    outside = re.sub(r"<table\b.*?</table\s*>", "", text, flags=re.IGNORECASE | re.DOTALL)
    outside = re.sub(r"</(?:p|div|li|tr|br)\s*>", "\n", outside, flags=re.IGNORECASE)
    outside = re.sub(r"<[^>]+>", " ", outside)
    for index, chunk in enumerate(unescape(outside).splitlines()):
        if chunk.strip():
            blocks.append((f"text[{index}]", chunk))
    return blocks or [("text[0]", unescape(re.sub(r"<[^>]+>", " ", text)))]


def _side_name(raw: str) -> str:
    return "reg_s" if re.fullmatch(r"(?:Regulation|Reg)\s*S", raw, re.IGNORECASE) else "rule_144a"


def _tenure(value: str | None) -> str:
    return value.lower() if value and value.lower() in {"temporary", "permanent"} else "not_stated"


def _evidence(match: re.Match[str], *, side: str | None = None, label: str | None = None, value: str | None = None) -> dict[str, str]:
    raw_label = label or match.group("label")
    raw_value = value or match.group("value")
    raw_side = side or match.group("side")
    return {
        "identifier_label": re.sub(r"\s+", " ", raw_label).upper().replace("COMMON CODE", "Common Code"),
        "source_value": raw_value,
        "normalized_value": re.sub(r"[^A-Za-z0-9]", "", raw_value).upper(),
        "tenure": _tenure(match.groupdict().get("tenure")),
        "exact_label": f"{re.sub(r'\s+', ' ', raw_side).strip()} {re.sub(r'\s+', ' ', raw_label).strip()}",
    }


def _record_for_block(locator: str, block: str, *, document_hash: str, accession: str) -> dict[str, Any]:
    entries: list[tuple[int, str, dict[str, str]]] = []
    occupied: list[tuple[int, int]] = []
    for match in _LABEL.finditer(block):
        side = _side_name(match.group("side"))
        entries.append((match.start(), side, _evidence(match)))
        occupied.append((match.start(), match.end()))
    unpaired_identifiers: list[dict[str, str]] = []
    for match in _BARE_COMMON_CODE.finditer(block):
        if any(start <= match.start() < end for start, end in occupied):
            continue
        unpaired_identifiers.append({
            "identifier_label": "Common Code", "source_value": match.group(2),
            "normalized_value": re.sub(r"[^A-Za-z0-9]", "", match.group(2)).upper(),
            "reason": "missing_distribution_side",
        })
    entries.sort(key=lambda item: item[0])
    sides = {"reg_s": [], "rule_144a": []}
    for _, side, evidence in entries:
        sides[side].append(evidence)
    if unpaired_identifiers:
        status, reason = "ambiguous", "bare_common_code_without_side"
    elif not entries:
        status, reason = "zero_match", "no_explicit_labeled_identifiers"
    elif not sides["reg_s"] or not sides["rule_144a"]:
        status, reason = "ambiguous", "missing_paired_side"
    elif any(
        len([item for item in values if (item["identifier_label"], item["tenure"]) == identity]) > 1
        for values in sides.values()
        for identity in {(item["identifier_label"], item["tenure"]) for item in values}
    ):
        status, reason = "ambiguous", "duplicate_identifier_label"
    else:
        status, reason = "candidate", "same_explicit_block"
    return {
        "status": status, "reason": reason, "accession": accession, "block_locator": locator,
        "document_hash": document_hash, "parser_version": PARSER_VERSION, "reg_s": sides["reg_s"],
        "rule_144a": sides["rule_144a"], "unpaired_identifiers": unpaired_identifiers,
    }


def parse_document(raw: bytes, *, document_hash: str, accession: str) -> list[dict[str, Any]]:
    """Extract only explicitly labelled identifiers within one table row/text block."""
    return [_record_for_block(locator, block, document_hash=document_hash, accession=accession) for locator, block in _blocks(raw)]


def parse(output_root: Path) -> dict[str, int]:
    records: list[dict[str, Any]] = []
    for item in sorted(_read_json(output_root / "downloads.json", []), key=lambda value: value["document_key"]):
        raw_path = output_root / item["raw_path"]
        for record in parse_document(raw_path.read_bytes(), document_hash=item["document_hash"], accession=item["accession"]):
            record["parent_form"] = item.get("parent_form")
            record["document_type"] = item.get("document_type")
            record["filed_at"] = item.get("filed_at")
            record["evidence_link"] = {
                "filing_url": item.get("filing_url"), "raw_path": item["raw_path"],
                "retrieved_at": item.get("retrieved_at"),
            }
            record["document_key"] = item["document_key"]
            records.append(record)
    _write_json(output_root / "parse" / "records.json", records)
    return {"records": len(records), "candidates": sum(record["status"] == "candidate" for record in records)}


def adjudication_export(output_root: Path) -> dict[str, int | str]:
    records = _read_json(output_root / "parse" / "records.json", [])
    pending = [{**record, "adjudication": "pending"} for record in records]
    digest = hashlib.sha256(canonical_json(pending).encode("utf-8")).hexdigest()
    payload = {"manifest_version": 1, "parser_version": PARSER_VERSION, "records": pending, "sha256": digest}
    _write_json(output_root / "adjudication" / "manifest.json", payload)
    _write_text(output_root / "adjudication" / "manifest.sha256", digest + "\n", encoding="ascii")
    return {"records": len(pending), "sha256": digest}


_ADJUDICATION_STATES = frozenset({"pending", "approved", "rejected"})


def seal_adjudication(output_root: Path) -> dict[str, int | str]:
    """Seal human adjudications by validating them and refreshing only manifest hashes."""
    manifest_path = output_root / "adjudication" / "manifest.json"
    manifest = _read_json(manifest_path, {})
    records = manifest.get("records")
    if not isinstance(records, list):
        raise ValueError("manifest records must be a list")
    for record in records:
        if not isinstance(record, dict) or record.get("adjudication") not in _ADJUDICATION_STATES:
            raise ValueError("invalid adjudication")
        if record["adjudication"] == "approved":
            snapshot_id = record.get("draft_snapshot_id")
            if not isinstance(snapshot_id, str) or not snapshot_id.strip():
                raise ValueError("approved record missing draft_snapshot_id")
    digest = hashlib.sha256(canonical_json(records).encode("utf-8")).hexdigest()
    _write_json(manifest_path, {**manifest, "sha256": digest})
    _write_text(output_root / "adjudication" / "manifest.sha256", digest + "\n", encoding="ascii")
    return {"records": len(records), "sha256": digest}


def _stable_id(prefix: str, value: object) -> str:
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest[:32]}"


def record_id(record: dict[str, Any]) -> str:
    """A stable manifest-local handle for dry-run skip reports and prospective facts."""
    return _stable_id("record", record)


def _identifier_kind(label: object) -> str:
    normalized = str(label).strip().upper()
    if normalized in {"CUSIP", "CINS"}:
        return "cusip9"
    if normalized == "ISIN":
        return "isin"
    if normalized == "COMMON CODE":
        return "common_code"
    raise ValueError(f"unsupported identifier label: {label!r}")


def _required(record: dict[str, Any], key: str) -> str:
    value = record.get(key)
    if value is None or not str(value).strip():
        raise ValueError(f"approved record missing {key}")
    return str(value).strip()


def _prospective_registry_rows(
    record: dict[str, Any], draft_snapshot_id: str
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """Build schema-shaped rows without creating a snapshot or writing a database."""
    evidence_link = record.get("evidence_link")
    if not isinstance(evidence_link, dict):
        raise ValueError("approved record missing evidence link")
    accession = _required(record, "accession")
    document_hash = _required(record, "document_hash")
    if not re.fullmatch(r"[0-9a-f]{64}", document_hash):
        raise ValueError("approved record has invalid document hash")
    source_url = _required(evidence_link, "filing_url")
    retrieved_at = _required(evidence_link, "retrieved_at")
    source_evidence_id = _stable_id(
        "source-evidence", [record.get("document_key"), accession, document_hash, source_url]
    )
    source_row = {
        "source_evidence_id": source_evidence_id, "sec_accession": accession,
        "form_type": _required(record, "parent_form"), "document_type": _required(record, "document_type"),
        "filed_at": record.get("filed_at"), "search_query_id": None, "source_url": source_url,
        "document_url": source_url, "retrieved_at": retrieved_at, "raw_document_sha256": document_hash,
        "parser_version": _required(record, "parser_version"),
    }
    observations: list[dict[str, Any]] = []
    identifiers: list[dict[str, Any]] = []
    observation_by_evidence: dict[tuple[str, int], str] = {}
    for distribution_rule in ("reg_s", "rule_144a"):
        evidence_items = record.get(distribution_rule)
        if not isinstance(evidence_items, list) or not evidence_items:
            raise ValueError(f"approved record missing {distribution_rule} evidence")
        for index, evidence in enumerate(evidence_items):
            if not isinstance(evidence, dict):
                raise ValueError("approved record has invalid identifier evidence")
            source_value = _required(evidence, "source_value")
            normalized_value = _required(evidence, "normalized_value")
            exact_label = _required(evidence, "exact_label")
            observation_id = _stable_id(
                "parser-observation", [source_evidence_id, record["block_locator"], exact_label, source_value, normalized_value]
            )
            observation_by_evidence[(distribution_rule, index)] = observation_id
            observations.append({
                "parser_observation_id": observation_id, "source_evidence_id": source_evidence_id,
                "parser_version": source_row["parser_version"], "block_locator": _required(record, "block_locator"),
                "exact_source_label": exact_label, "source_value": source_value,
                "normalized_value": normalized_value, "observation_state": "validated",
            })
    pair_key = _stable_id("pair", [source_evidence_id, record["block_locator"], record["reg_s"], record["rule_144a"]])
    decision_id = _stable_id("pair-decision", [draft_snapshot_id, pair_key, record.get("valid_from"), record.get("valid_to")])
    decision_row = {
        "decision_id": decision_id, "snapshot_id": draft_snapshot_id, "pair_key": pair_key,
        "decision_state": "approved", "source_observation_id": observation_by_evidence[("rule_144a", 0)],
        "valid_from": _required(record, "valid_from"), "valid_to": record.get("valid_to"),
    }
    for distribution_rule in ("reg_s", "rule_144a"):
        for index, evidence in enumerate(record[distribution_rule]):
            identifier_kind = _identifier_kind(evidence.get("identifier_label"))
            identifier_value = _required(evidence, "normalized_value")
            if identifier_kind == "cusip9" and not re.fullmatch(r"[A-Z0-9]{9}", identifier_value):
                raise ValueError("approved record has invalid cusip9 evidence")
            if identifier_kind == "isin" and not re.fullmatch(r"[A-Z0-9]{12}", identifier_value):
                raise ValueError("approved record has invalid isin evidence")
            if identifier_kind == "common_code" and not re.fullmatch(r"[0-9]{9}", identifier_value):
                raise ValueError("approved record has invalid common code evidence")
            identifier_id = _stable_id(
                "pair-identifier", [decision_id, distribution_rule, identifier_kind, identifier_value, record["valid_from"]]
            )
            identifiers.append({
                "identifier_id": identifier_id, "decision_id": decision_id,
                "source_observation_id": observation_by_evidence[(distribution_rule, index)],
                "distribution_rule": distribution_rule, "identifier_kind": identifier_kind,
                "identifier_value": identifier_value, "identifier_tenure": _required(evidence, "tenure"),
                "valid_from": _required(record, "valid_from"), "valid_to": record.get("valid_to"),
            })
    return source_row, observations, decision_row, identifiers


def _draft_snapshot_preview(
    draft_snapshot_id: str,
    decision_rows: dict[str, dict[str, Any]],
    identifier_rows: dict[str, dict[str, Any]],
) -> tuple[dict[str, str], dict[str, str]]:
    """Bind the prospective composition with the registry's canonical hash function."""
    from src.bonds.distribution_series import (
        DistributionPairDecision,
        DistributionPairIdentifier,
        distribution_snapshot_content_hash,
    )

    decisions = [
        DistributionPairDecision(
            row["decision_id"], row["snapshot_id"], row["decision_state"], row["source_observation_id"],
            date.fromisoformat(row["valid_from"]),
            date.fromisoformat(row["valid_to"]) if row["valid_to"] else None, row["pair_key"],
        )
        for row in decision_rows.values()
    ]
    identifiers = [
        DistributionPairIdentifier(
            row["identifier_id"], row["decision_id"], row["source_observation_id"],
            row["distribution_rule"], row["identifier_kind"], row["identifier_value"],
            row["identifier_tenure"], date.fromisoformat(row["valid_from"]),
            date.fromisoformat(row["valid_to"]) if row["valid_to"] else None,
        )
        for row in identifier_rows.values()
    ]
    content_hash = distribution_snapshot_content_hash(draft_snapshot_id, decisions, identifiers)
    return (
        {"snapshot_id": draft_snapshot_id, "snapshot_status": "draft", "content_hash": content_hash},
        {"snapshot_id": draft_snapshot_id, "content_hash": content_hash},
    )


def build_registry_bundle(
    output_root: Path, draft_snapshot_id: str | None = None
) -> dict[str, object]:
    """Build the deterministic, sealed registry payload without any database effect."""
    manifest = _read_json(output_root / "adjudication" / "manifest.json", {})
    records = manifest.get("records", [])
    expected = hashlib.sha256(canonical_json(records).encode("utf-8")).hexdigest()
    if manifest.get("sha256") != expected:
        raise ValueError("manifest checksum mismatch")
    for record in records:
        if record.get("status") not in {"candidate", "ambiguous", "zero_match"}:
            raise ValueError("unknown record status")
    source_rows: dict[str, dict[str, Any]] = {}
    observation_rows: dict[str, dict[str, Any]] = {}
    decision_rows: dict[str, dict[str, Any]] = {}
    identifier_rows: dict[str, dict[str, Any]] = {}
    skipped: list[dict[str, str]] = []
    draft_snapshot_ids: set[str] = set()
    for record in records:
        status = record.get("status")
        adjudication = record.get("adjudication", "pending")
        if adjudication == "rejected":
            skipped.append({"record_id": record_id(record), "reason": "rejected"})
        elif status == "ambiguous":
            skipped.append({"record_id": record_id(record), "reason": "ambiguous"})
        elif status == "zero_match":
            skipped.append({"record_id": record_id(record), "reason": "zero_match"})
        elif adjudication != "approved":
            skipped.append({"record_id": record_id(record), "reason": "pending"})
        else:
            snapshot_id = record.get("draft_snapshot_id")
            if not isinstance(snapshot_id, str) or not snapshot_id.strip():
                skipped.append({"record_id": record_id(record), "reason": "draft_snapshot_id_required"})
                continue
            if draft_snapshot_id is not None and snapshot_id != draft_snapshot_id:
                raise ValueError("draft snapshot id does not match sealed record")
            source_row, observations, decision_row, identifiers = _prospective_registry_rows(record, snapshot_id)
            draft_snapshot_ids.add(snapshot_id)
            source_rows[source_row["source_evidence_id"]] = source_row
            observation_rows.update({row["parser_observation_id"]: row for row in observations})
            decision_rows[decision_row["decision_id"]] = decision_row
            identifier_rows.update({row["identifier_id"]: row for row in identifiers})
    if len(draft_snapshot_ids) > 1:
        raise ValueError("approved records must share one draft_snapshot_id")
    mapping_snapshot_rows: list[dict[str, str]] = []
    snapshot_approval_rows: list[dict[str, str]] = []
    if draft_snapshot_ids:
        mapping_snapshot, snapshot_approval = _draft_snapshot_preview(
            next(iter(draft_snapshot_ids)), decision_rows, identifier_rows
        )
        mapping_snapshot_rows.append(mapping_snapshot)
        snapshot_approval_rows.append(snapshot_approval)
    return {
        "database_writes": 0, "mapping_snapshot_rows": mapping_snapshot_rows,
        "pair_decision_rows": [decision_rows[key] for key in sorted(decision_rows)],
        "pair_identifier_rows": [identifier_rows[key] for key in sorted(identifier_rows)],
        "parser_observation_rows": [observation_rows[key] for key in sorted(observation_rows)],
        "registry_load_order": [
            "source_evidence_rows", "parser_observation_rows", "mapping_snapshot_rows",
            "pair_decision_rows", "pair_identifier_rows", "snapshot_approval_rows",
        ],
        "skipped_records": skipped,
        "snapshot_approval_rows": snapshot_approval_rows,
        "source_evidence_rows": [source_rows[key] for key in sorted(source_rows)],
    }


def publish(
    output_root: Path, *, dry_run: bool, draft_snapshot_id: str | None = None
) -> dict[str, int | bool]:
    """Print only explicitly approved, schema-compatible prospective facts; never write a DB."""
    if not dry_run:
        raise PublishRefused("live publication is unavailable; use --dry-run")
    bundle = build_registry_bundle(output_root, draft_snapshot_id)
    print(canonical_json(bundle))
    return {
        "approved_records": len(bundle["pair_decision_rows"]), "database_writes": 0, "dry_run": True,
        "skipped_records": len(bundle["skipped_records"]),
    }


def _credential(explicit: str | None) -> str | None:
    return explicit or os.environ.get("SEC_API_IO_KEY") or os.environ.get("SEC_API_KEY")


def _reference_cusips_from_file(path: Path) -> list[str]:
    """Read one CUSIP9 per line; blank lines and comments are intentionally ignored."""
    return [line.split("#", 1)[0].strip() for line in path.read_text(encoding="utf-8").splitlines() if line.split("#", 1)[0].strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=Path("artifacts/bond-distribution-series"))
    commands = parser.add_subparsers(dest="stage", required=True)
    discover_parser = commands.add_parser("discover")
    discover_parser.add_argument("--limit", type=int, required=True)
    discover_parser.add_argument("--start-date", required=True)
    discover_parser.add_argument("--end-date", required=True)
    discover_parser.add_argument("--sec-api-key")
    targeted_parser = commands.add_parser("targeted-discover")
    targeted_parser.add_argument("--reference-cusips-file", type=Path, required=True)
    targeted_parser.add_argument("--limit", type=int, required=True)
    targeted_parser.add_argument("--start-date", required=True)
    targeted_parser.add_argument("--end-date", required=True)
    targeted_parser.add_argument("--sec-api-key")
    commands.add_parser("download")
    commands.add_parser("parse")
    commands.add_parser("adjudication-export")
    commands.add_parser("seal-adjudication")
    publish_parser = commands.add_parser("publish")
    publish_parser.add_argument("--draft-snapshot-id")
    publish_parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.stage == "discover":
            key = _credential(args.sec_api_key)
            if not key:
                raise ValueError("SEC API credential is required")
            result = discover(
                UrllibClient(), args.output_root, api_key=key, start_date=args.start_date,
                end_date=args.end_date, limit=args.limit,
            )
        elif args.stage == "targeted-discover":
            key = _credential(args.sec_api_key)
            if not key:
                raise ValueError("SEC API credential is required")
            result = targeted_discover(
                UrllibClient(), args.output_root, api_key=key,
                reference_cusips=_reference_cusips_from_file(args.reference_cusips_file),
                start_date=args.start_date, end_date=args.end_date, limit=args.limit,
            )
        elif args.stage == "download":
            identity = os.environ.get("SEC_USER_AGENT") or os.environ.get("EDGAR_IDENTITY") or ""
            result = download(UrllibClient(), args.output_root, edgar_identity=identity)
        elif args.stage == "parse":
            result = parse(args.output_root)
        elif args.stage == "adjudication-export":
            result = adjudication_export(args.output_root)
        elif args.stage == "seal-adjudication":
            result = seal_adjudication(args.output_root)
        else:
            result = publish(
                args.output_root, dry_run=args.dry_run,
                draft_snapshot_id=args.draft_snapshot_id,
            )
    except (PublishRefused, ValueError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2
    print(canonical_json(result))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
