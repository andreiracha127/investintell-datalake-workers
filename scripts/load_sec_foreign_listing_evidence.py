"""Collect foreign-listing evidence from original SEC filings; replay or load it.

The default command only writes local artifacts. Schema installation is separate;
``--apply`` is the only path that connects to a database. Credentials are read
from environment variables (or the explicit local dotenv) and never serialized.
Search and original-document caches are content-verified, resumable and confined
to the caller's external cache directory. No downloaded content is executable.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
from html import escape, unescape
import io
import json
import lzma
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

USER_AGENT = "InvestIntell-SEP-Ingestion/1.0 (+https://hub.investintell.com)"
QUERY_URL = "https://api.sec-api.io"
FULLTEXT_URL = QUERY_URL + "/full-text-search"
ANNUAL_FORMS = ("20-F", "20-F/A", "40-F", "40-F/A", "20FR12B", "20FR12G", "20FR12B/A", "20FR12G/A", "40FR12B", "40FR12B/A")
F6_FORMS = ("F-6", "F-6/A", "F-6EF", "F-6 POS")
MANIFEST_VERSION = 1
DISCOVERY_VERSION = "foreign-listing-discovery-v6-complete-depositary-terms"
RATIO_CHANGE_QUERY = (
    'ratio AND (ADS OR ADR OR GDR OR GDS OR ADSs OR ADRs OR GDRs OR GDSs '
    'OR "depositary shares" OR "depositary receipts" '
    'OR "depository shares" OR "depository receipts" '
    'OR "depositary share" OR "depositary receipt" '
    'OR "depository share" OR "depository receipt" '
    'OR ("American shares" AND receipts) OR ("American share" AND receipts))'
)


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_sec_url(url: str) -> str:
    """Resolve SEC inline-viewer wrappers without accepting arbitrary redirects."""
    parts = urlsplit(url)
    if (parts.scheme not in {"http", "https"}
            or parts.hostname not in {"www.sec.gov", "sec.gov", "archives.sec.gov"}
            or parts.username or parts.password or parts.port):
        raise ValueError("Document URL is not an SEC EDGAR data URL")
    path = parts.path
    if path in {"/ix", "/ix.xhtml"}:
        query = parse_qs(parts.query, keep_blank_values=True)
        if len(query.get("doc", [])) != 1:
            raise ValueError("SEC viewer URL requires exactly one document path")
        path = query["doc"][0]
    decoded = unquote(path)
    if (not decoded.startswith("/Archives/edgar/data/")
            or any(segment in {".", ".."} for segment in decoded.split("/"))
            or any(character in decoded for character in ("\\", "?", "#", "\r", "\n"))):
        raise ValueError("Document URL is not an SEC EDGAR data URL")
    return "https://www.sec.gov" + path


def canonical_document(document: dict) -> dict:
    """Use the same URL identity for discovery, replay, cache and evidence."""
    url = canonical_sec_url(document["source_url"])
    original = document["source_url"]
    result = {**document, "source_url": url,
              "source_package": digest(f"{document['cik']}|{document['adsh']}|{url}".encode())}
    if url != original:
        result["discovered_source_url"] = original
    return result


def replace_file(temporary: Path, destination: Path) -> None:
    """Bounded retry for Windows readers briefly denying delete/replace sharing."""
    for attempt in range(12):
        try:
            temporary.replace(destination)
            return
        except PermissionError:
            if attempt == 11:
                raise
            time.sleep(min(0.05 * (2 ** attempt), 0.8))


def write_bytes(path: Path, payload: bytes) -> None:
    """Publish one complete file; concurrent writers never share a temporary."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        temporary.write_bytes(payload)
        replace_file(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            # Preserve the original replacement error; a failed private temp
            # can be removed after the interfering reader releases its handle.
            pass


def write_json(path: Path, value: object) -> None:
    write_bytes(path, (canonical_json(value) + "\n").encode("utf-8"))


def load_key(dotenv: Path | None = None) -> str:
    for key in ("SEC_API_IO_KEY", "SEC_API_KEY"):
        if os.environ.get(key):
            return os.environ[key].strip()
    if dotenv and dotenv.is_file():
        for line in dotenv.read_text(encoding="utf-8-sig").splitlines():
            match = re.match(r"\s*(?:export\s+)?(SEC_API_IO_KEY|SEC_API_KEY)\s*=\s*(.*?)\s*$", line)
            if match:
                value = match.group(2)
                if value[:1] in ("'", '"'):
                    value = value[1:value.rfind(value[0])]
                else:
                    value = value.split(" #", 1)[0].strip()
                if value:
                    return value
    raise ValueError("SEC API key unavailable in environment or configured dotenv")


def read_universe(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    else:
        text = path.read_text(encoding="utf-8-sig")
        parsed = json.loads(text) if text.lstrip().startswith(("[", "{")) and path.suffix != ".jsonl" else None
        rows = parsed.get("lines", parsed.get("universe", [])) if isinstance(parsed, dict) else parsed
        if rows is None:
            rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    result = {}
    for row in rows:
        cik = int(row["cik"])
        symbol = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
        if cik <= 0 or not symbol:
            raise ValueError("Universe requires positive cik and nonempty symbol/ticker")
        result[(cik, symbol)] = {**row, "cik": cik, "symbol": symbol,
                               "issuer_name": row.get("issuer_name") or row.get("name") or row.get("company_name") or ""}
    if not result:
        raise ValueError("Foreign universe is empty")
    return [result[key] for key in sorted(result)]


def sgml_recovery_locator(url: str) -> tuple[str, str, str] | None:
    """Derive only this accession's complete submission for a text document."""
    parts = urlsplit(canonical_sec_url(url))
    parent, filename = parts.path.rsplit("/", 1)
    compact = parent.rsplit("/", 1)[-1]
    if not re.fullmatch(r"\d{18}", compact) or not filename.lower().endswith((".htm", ".html", ".txt")):
        return None
    accession = compact[:10] + "-" + compact[10:12] + "-" + compact[12:]
    if filename == accession + ".txt":
        return None  # Complete submissions must never recursively recover themselves.
    return "https://www.sec.gov" + parent + "/" + accession + ".txt", accession, unquote(filename)


def extract_sgml_document(payload: bytes, *, accession: str, filename: str) -> tuple[bytes, str]:
    """Select one exact SEC DOCUMENT and preserve its original TEXT bytes."""
    header = re.search(rb"(?im)^(?:ACCESSION NUMBER:|<ACCESSION-NUMBER>)[ \t]*([0-9-]+)", payload[:65536])
    if header is None:
        header = re.search(rb"(?im)^<SEC-DOCUMENT>([0-9-]+)\.txt", payload[:65536])
    if header is None or header.group(1).decode("ascii") != accession:
        raise ValueError("Complete submission accession does not match the requested document")
    matches = []
    for block in re.findall(rb"(?ms)^<DOCUMENT>[ \t]*\r?\n(.*?)^</DOCUMENT>[ \t]*\r?$", payload):
        name = re.search(rb"(?im)^<FILENAME>[ \t]*([^\r\n]+)", block)
        if name is None or name.group(1).strip().decode("utf-8", errors="strict") != filename:
            continue
        text = re.search(rb"(?ms)^<TEXT>(.*?)^</TEXT>[ \t]*\r?$", block)
        form = re.search(rb"(?im)^<TYPE>[ \t]*([^\r\n]+)", block)
        if text is None or form is None:
            raise ValueError("Matching SGML document has no TEXT body or TYPE")
        data = text.group(1)
        # The first line break separates the SGML tag from the original file.
        # Remove exactly that delimiter, retaining every original trailing byte.
        if data.startswith(b"\r\n"):
            data = data[2:]
        elif data.startswith(b"\n"):
            data = data[1:]
        document_type = form.group(1).strip().decode("ascii", errors="strict")
        if (document_type.upper() in {"GRAPHIC", "PDF", "ZIP", "EXCEL"}
                or data.lstrip().startswith(b"%PDF-") or b"\x00" in data[:1024]):
            raise ValueError("Matching SGML document is not an HTML or text source")
        matches.append((data, document_type))
    if len(matches) != 1:
        raise ValueError("Complete submission must contain exactly one matching document filename")
    return matches[0]


def indexed_relocation(cache_dir: Path, url: str) -> tuple[str, dict] | None:
    """Resolve a moved archival CIK only from a unique official index entry."""
    locator = sgml_recovery_locator(url)
    if locator is None:
        return None
    _, accession, filename = locator
    year_number = int(accession[11:13])
    year = 1900 + year_number if year_number >= 93 else 2000 + year_number
    # Shards share their originals through cache/documents. Index evidence is
    # stored beside that shared directory, never inferred from another issuer.
    shared_root = (cache_dir / "documents").resolve().parent
    matches = []
    for index_file in sorted(shared_root.glob(f"master-{year}-Q[1-4].idx")):
        quarter = int(index_file.stem[-1])
        raw = index_file.read_bytes()
        for line in raw.decode("utf-8", errors="replace").splitlines():
            fields = line.split("|")
            if len(fields) != 5:
                continue
            path = re.fullmatch(r"edgar/data/(\d+)/(\d{10}-\d{2}-\d{6})\.txt", fields[4].strip())
            if path is None or path.group(2) != accession:
                continue
            if not fields[0].strip().isdigit() or int(fields[0]) != int(path.group(1)):
                raise ValueError("SEC index CIK and archival path disagree")
            filed = date.fromisoformat(fields[3].strip())
            if filed.year != year or (filed.month - 1) // 3 + 1 != quarter:
                raise ValueError("SEC index row date does not match its published quarter")
            actual = ("https://www.sec.gov/Archives/edgar/data/" + path.group(1) + "/"
                      + accession.replace("-", "") + "/" + urlsplit(url).path.rsplit("/", 1)[-1])
            proof = {"recovery_document_url": actual,
                     "recovery_index_url": f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx",
                     "recovery_index_sha256": digest(raw), "recovery_index_line": line,
                     "recovery_index_filed": str(filed), "recovery_archive_cik": int(path.group(1)),
                     "recovery_accession": accession, "recovery_filename": filename}
            matches.append((actual, proof))
    if len(matches) > 1:
        raise ValueError("SEC index has ambiguous accession mappings")
    if not matches or matches[0][0] == canonical_sec_url(url):
        return None
    return matches[0]


class SecClient:
    """Thread-safe rate limiter shared by searches and SEC/access-layer downloads."""

    def __init__(self, cache: Path, key: str = "", *, offline: bool = False, requests_per_second: float = 5):
        if not 0 < requests_per_second <= 10:
            raise ValueError("SEC request rate must be in (0, 10]")
        self.cache, self.key, self.offline = cache, key, offline
        self.interval = 1 / requests_per_second
        self.lock = threading.Lock()
        self.next_request = 0.0
        self.disk_full = threading.Event()
        self.recovery_proofs: dict[str, dict] = {}

    def request(self, url: str, payload: dict | None = None) -> bytes:
        if self.offline:
            raise RuntimeError("Offline mode cannot make network requests")
        for attempt in range(5):
            with self.lock:
                delay = self.next_request - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                self.next_request = time.monotonic() + self.interval
            headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
            if urlsplit(url).hostname in {"api.sec-api.io", "archive.sec-api.io", "edgar-mirror.sec-api.io"}:
                headers["Authorization"] = self.key
            body = None if payload is None else canonical_json(payload).encode()
            if body:
                headers["Content-Type"] = "application/json"
            try:
                with urlopen(Request(url, data=body, headers=headers), timeout=90) as response:
                    data = response.read()
                    return gzip.decompress(data) if data.startswith(b"\x1f\x8b") else data
            except HTTPError as exc:
                if exc.code not in {408, 429, 500, 502, 503, 504} or attempt == 4:
                    raise RuntimeError(f"SEC HTTP {exc.code}") from None
            except (URLError, TimeoutError, OSError):
                if attempt == 4:
                    raise RuntimeError("SEC transport failed after five attempts") from None
            time.sleep(min(2 ** attempt, 16))
        raise RuntimeError("SEC transport retries exhausted")

    def search(self, url: str, payload: dict) -> dict:
        key = digest(canonical_json({"url": url, "payload": payload}).encode())
        path = self.cache / "search" / (key + ".json")
        if path.exists():
            envelope = json.loads(path.read_text(encoding="utf-8"))
            if envelope["sha256"] != digest(canonical_json(envelope["response"]).encode()):
                raise ValueError("Search cache hash mismatch: " + key)
            return envelope["response"]
        response = json.loads(self.request(url, payload))
        if not isinstance(response, dict) or "filings" not in response:
            raise ValueError("SEC search returned an unexpected response shape")
        write_json(path, {"request": payload, "endpoint": url, "response": response,
                          "sha256": digest(canonical_json(response).encode())})
        return response

    def document(self, url: str, *, expected_sha256: str | None = None,
                 _allow_sgml_recovery: bool = True) -> tuple[bytes, str]:
        url = canonical_sec_url(url)
        parts = urlsplit(url)
        key = digest(url.encode())
        directory = self.cache / "documents"
        path = directory / (key + ".bin.xz")
        gzip_path = directory / (key + ".bin.gz")
        legacy_path = directory / (key + ".bin")
        meta_path = directory / (key + ".json")
        if not path.exists():
            if gzip_path.exists():
                path = gzip_path
            elif legacy_path.exists():
                path = legacy_path
        if path.exists() and meta_path.exists():
            try:
                stored = path.read_bytes()
                if path.suffix == ".xz":
                    data = lzma.decompress(stored)
                else:
                    data = gzip.decompress(stored) if path.suffix == ".gz" else stored
            except (OSError, EOFError, lzma.LZMAError):
                raise ValueError("Document cache integrity mismatch: " + key) from None
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if meta.get("url") != url or meta.get("sha256") != digest(data):
                raise ValueError("Document cache integrity mismatch: " + key)
        else:
            directory.mkdir(parents=True, exist_ok=True)
            if self.disk_full.is_set() or shutil.disk_usage(directory).free < 64 * 1024 * 1024:
                self.disk_full.set()
                raise OSError("Insufficient cache disk space; resume after freeing space")
            download_url = "https://archive.sec-api.io/" + parts.path.split("/data/", 1)[1] if self.key else url
            recovery = {}
            try:
                data = self.request(download_url)
                if len(data) < 100 or b"Request Rate Threshold Exceeded" in data[:2000]:
                    raise ValueError("SEC returned an empty document or rate-limit page")
            except (RuntimeError, ValueError):
                locator = sgml_recovery_locator(url) if _allow_sgml_recovery else None
                if locator is None:
                    raise
                submission_url, accession, filename = locator
                try:
                    submission, submission_sha = self.document(submission_url, _allow_sgml_recovery=False)
                    data, document_type = extract_sgml_document(submission, accession=accession, filename=filename)
                    recovery = {"recovery_source_url": submission_url, "recovery_source_sha256": submission_sha,
                                "recovery_filename": filename, "recovery_accession": accession,
                                "recovery_document_type": document_type}
                except (RuntimeError, ValueError, OSError):
                    relocated = indexed_relocation(self.cache, url)
                    if relocated is None:
                        raise
                    actual_url, recovery = relocated
                    try:
                        data, _ = self.document(actual_url, _allow_sgml_recovery=False)
                        recovery["recovery_retrieval_url"] = actual_url
                    except (RuntimeError, ValueError, OSError):
                        actual_submission, _, _ = sgml_recovery_locator(actual_url)
                        submission, submission_sha = self.document(actual_submission, _allow_sgml_recovery=False)
                        data, document_type = extract_sgml_document(submission, accession=accession, filename=filename)
                        recovery.update({"recovery_retrieval_url": actual_submission, "recovery_source_url": actual_submission,
                                         "recovery_source_sha256": submission_sha, "recovery_document_type": document_type})
            if len(data) < 100 or b"Request Rate Threshold Exceeded" in data[:2000]:
                raise ValueError("SEC returned an empty document or rate-limit page")
            compressed = lzma.compress(data, preset=3)
            if shutil.disk_usage(directory).free < len(compressed) + 64 * 1024 * 1024:
                self.disk_full.set()
                raise OSError("Insufficient cache disk space; resume after freeing space")
            path = directory / (key + ".bin.xz")
            write_bytes(path, compressed)
            meta = {"url": url, "sha256": digest(data), "bytes": len(data),
                    "stored_bytes": len(compressed), "encoding": "xz", **recovery}
            write_json(meta_path, meta)
        actual = digest(data)
        if expected_sha256 and expected_sha256 != actual:
            raise ValueError("Manifest/document SHA256 mismatch: " + key)
        if any(name.startswith("recovery_") for name in meta):
            self.recovery_proofs[url] = {name: value for name, value in meta.items() if name.startswith("recovery_")}
        else:
            self.recovery_proofs.pop(url, None)
        return data, actual


def _total(response: dict) -> tuple[int, bool]:
    value = response.get("total", {})
    return (int(value.get("value", 0)), value.get("relation", "eq") == "eq") if isinstance(value, dict) else (int(value), True)


def search_filings(client: SecClient, query: str, start: date, end: date) -> list[dict]:
    """Bisect large date ranges, avoiding the API's 10,000-hit result window."""
    base = {"query": query + f" AND filedAt:[{start.isoformat()} TO {end.isoformat()}]",
            "from": "0", "size": "50", "sort": [{"filedAt": {"order": "asc"}}]}
    first = client.search(QUERY_URL, base)
    total, exact = _total(first)
    if total >= 10000 or not exact:
        if start == end:
            raise ValueError("Single-day query exceeds SEC API result window; discovery is incomplete")
        middle = start + (end - start) // 2
        return search_filings(client, query, start, middle) + search_filings(client, query, middle + timedelta(days=1), end)
    rows = list(first["filings"])
    while len(rows) < total:
        page = client.search(QUERY_URL, {**base, "from": str(len(rows))})["filings"]
        if not page:
            raise ValueError("SEC query stopped before declared total")
        rows.extend(page)
    if len({row.get("accessionNo") for row in rows}) != len(rows):
        raise ValueError("SEC search repeated accession across pages; rerun with narrower dates")
    return rows


def search_text(client: SecClient, query: str, forms: Iterable[str], start: date, end: date,
                *, cik: int | None = None) -> list[dict]:
    """Require distinct document hits to satisfy the full-text declared total.

    This endpoint counts filings AND exhibits, unlike filing search. Distinct
    attachments legitimately share an accession; legacy SGML attachments can
    also share their URL. Their document type completes the hit identity.
    """
    forms = tuple(forms)
    base = {"query": query, "formTypes": list(forms), "startDate": str(start), "endDate": str(end)}
    if cik is not None:
        base["ciks"] = [str(cik)]
    first = client.search(FULLTEXT_URL, {**base, "page": "1"})
    total, exact = _total(first)
    if total >= 10000 or not exact:
        if start == end:
            raise ValueError("Single-day full-text query exceeds result window")
        middle = start + (end - start) // 2
        return (search_text(client, query, forms, start, middle, cik=cik)
                + search_text(client, query, forms, middle + timedelta(days=1), end, cik=cik))
    rows = []
    seen = set()

    def add(page_rows: list[dict]) -> None:
        for row in page_rows:
            accession = row.get("accessionNo") or row.get("accessionNumber")
            url = row.get("filingUrl") or row.get("linkToFilingDetails")
            if not accession or not url:
                raise ValueError("SEC full-text hit lacks accession or document URL")
            identity = (accession, canonical_sec_url(url), str(row.get("type") or ""))
            if identity in seen:
                raise ValueError("SEC full-text search repeated accession/document hit; rerun with narrower dates")
            seen.add(identity)
            rows.append(row)

    add(first["filings"])
    page = 2
    while len(seen) < total:
        more = client.search(FULLTEXT_URL, {**base, "page": str(page)})["filings"]
        if not more:
            raise ValueError("SEC full-text query stopped before declared total")
        add(more)
        page += 1
    if len(seen) != total:
        raise ValueError("SEC full-text unique document hits do not match declared total")
    return rows


def _quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _form_query(forms: Iterable[str]) -> str:
    return "(" + " OR ".join("formType:" + _quote(form) for form in forms) + ")"


def document_metadata(row: dict, cik: int, symbols: list[str], *, binding: str, issuer_name: str = "") -> dict | None:
    url = row.get("filingUrl") or row.get("linkToFilingDetails")
    accession = row.get("accessionNo") or row.get("accessionNumber")
    if not url or not accession:
        raise ValueError("Search hit lacks filing URL or accession")
    if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession):
        raise ValueError("Invalid SEC accession number")
    filed = str(row.get("filedAt") or row.get("filed") or "")[:10]
    date.fromisoformat(filed)
    return canonical_document({"cik": cik, "symbols": symbols, "adsh": accession, "form": row["formType"],
            "filed": filed, "query_accepted_on": filed, "query_filed_at": str(row.get("filedAt") or ""),
            "filing_date_status": "unverified", "source_url": url, "binding": binding, "issuer_name": issuer_name,
            "registrant_cik": str(row.get("cik") or ""),
            "source_package": digest(f"{cik}|{accession}|{url}".encode())})


def securities_description_exhibits(filing: dict) -> list[dict]:
    """Discover 20-F securities-description candidates, retaining parent dates."""
    if str(filing.get("formType", "")).removesuffix("/A") not in {"20-F", "20FR12B", "20FR12G"}:
        return []
    result = []
    for attachment in filing.get("documentFormatFiles") or []:
        form = str(attachment.get("type", "")).upper()
        if not re.match(r"EX-2(?:[.\-(]|$)", form):
            continue
        description = str(attachment.get("description", ""))
        url = attachment.get("documentUrl")
        if not url:
            continue
        filename = unquote(urlsplit(url).path.rsplit("/", 1)[-1]).lower()
        named_description = re.search(r"\bdescription\b.{0,160}\bsecurities\b", description, re.I)
        conventional_name = (re.sub(r"[^A-Z0-9]", "", form) == "EX2A1"
                             or re.search(r"(?:ex|exhibit)[_.-]?2[_.-]?a[_.-]?1(?:\D|$)", filename))
        if named_description or conventional_name:
            result.append({**filing, "filingUrl": url, "document_role": "securities_description",
                           "attachment_type": form, "attachment_description": description})
    return result


def discover_issuer(client: SecClient, lines: list[dict], start: date, end: date) -> list[dict]:
    cik = lines[0]["cik"]
    symbols = sorted({symbol for line in lines for symbol in (line.get("source_symbols") or [line["symbol"]])})
    names = sorted({str(line["issuer_name"]).strip() for line in lines if line.get("issuer_name")})
    docs: dict[str, dict] = {}

    def add(rows: list[dict], binding: str, name: str = "") -> None:
        for row in rows:
            # Lucene analyzes punctuation: formType:"20-F" can return NT 20-F.
            # The source's exact metadata remains the form authority.
            if row.get("formType") not in ANNUAL_FORMS + F6_FORMS + ("6-K", "6-K/A"):
                continue
            entry = document_metadata(row, cik, symbols, binding=binding, issuer_name=name)
            if row.get("document_role"):
                entry.update({key: row[key] for key in ("document_role", "attachment_type", "attachment_description") if key in row})
            docs.setdefault(entry["source_package"], entry)

    annual_and_f6 = search_filings(client, f"cik:{cik} AND {_form_query(ANNUAL_FORMS + F6_FORMS)}", start, end)
    add(annual_and_f6, "registrant_cik")
    for filing in annual_and_f6:
        add(securities_description_exhibits(filing), "registrant_cik")
    names = sorted(set(names) | {str(row["companyName"]).strip() for row in annual_and_f6
                                 if row.get("companyName") and row.get("formType") in ANNUAL_FORMS})
    # Depositary banks may file F-6 under their own CIK. Exact issuer-name full
    # text search finds those documents, then the downloaded text verifies it.
    for name in names:
        if len(re.sub(r"[^A-Za-z0-9]", "", name)) >= 6:
            add(search_text(client, _quote(name), F6_FORMS, start, end), "issuer_name_in_f6", name)
    add(search_text(client, RATIO_CHANGE_QUERY,
                    ("6-K", "6-K/A"), start, end, cik=cik), "registrant_cik")
    return [docs[key] for key in sorted(docs)]


def discover(client: SecClient, universe: list[dict], start: date, end: date, workers: int = 4,
             *, refresh: bool = False, observations: list[dict] | None = None) -> dict:
    by_cik: dict[int, list[dict]] = {}
    for row in universe:
        by_cik.setdefault(row["cik"], []).append(row)
    path = client.cache / "manifest.json"
    manifest = {"version": MANIFEST_VERSION, "discovery_version": DISCOVERY_VERSION,
                "universe_sha256": digest(canonical_json(universe).encode()),
                "start_date": str(start), "end_date": str(end), "documents": [], "issuers": {}, "complete": False}
    if path.exists():
        old = json.loads(path.read_text(encoding="utf-8"))
        if any(old.get(key) != manifest[key] for key in ("version", "universe_sha256", "start_date", "end_date")):
            raise ValueError("Existing manifest belongs to a different universe/date range")
        if old.get("discovery_version") != DISCOVERY_VERSION and not refresh:
            raise ValueError("Discovery implementation changed; pass --refresh-discovery to recheck all issuers")
        if not refresh:
            manifest = old
        else:
            # Exact query cache pages and original documents remain reusable.
            # The old manifest is retained outside the active discovery set.
            prior_hash = digest(canonical_json(old).encode())
            write_json(client.cache / "manifest-history" / (prior_hash + ".json"), old)
    docs = {row["source_package"]: row for row in manifest["documents"]}
    pending = {cik: lines for cik, lines in by_cik.items() if manifest["issuers"].get(str(cik), {}).get("status") != "complete"}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(discover_issuer, client, lines, start, end): cik for cik, lines in pending.items()}
        for future in as_completed(futures):
            cik = futures[future]
            try:
                rows = future.result()
                docs.update({row["source_package"]: row for row in rows})
                manifest["issuers"][str(cik)] = {"status": "complete", "documents": len(rows)}
            except Exception as exc:
                manifest["issuers"][str(cik)] = {"status": "failed", "error": type(exc).__name__ + ": " + str(exc)[:200]}
            manifest["documents"] = [docs[key] for key in sorted(docs)]
            try:
                write_json(path, manifest)
            except BaseException as exc:
                # ThreadPoolExecutor otherwise waits for every queued issuer
                # before surfacing a checkpoint failure, hiding the fatal error
                # behind continued network activity with no progress output.
                print(canonical_json({"event": "checkpoint_failed", "error_type": type(exc).__name__}), flush=True)
                for queued in futures:
                    queued.cancel()
                raise
            print(canonical_json({"event": "discovery", "cik": cik, **manifest["issuers"][str(cik)]}), flush=True)
    manifest["complete"] = len(manifest["issuers"]) == len(by_cik) and all(row["status"] == "complete" for row in manifest["issuers"].values())
    manifest["discovery_complete"] = manifest["complete"]
    if manifest["discovery_complete"]:
        if __package__:
            from .enrich_sec_foreign_listing_filing_dates import enrich_manifest
        else:
            from enrich_sec_foreign_listing_filing_dates import enrich_manifest
        manifest, date_summary = enrich_manifest(client, manifest, observations=observations,
                                                 download_indexes=not client.offline)
        write_json(client.cache / "filing-date-summary.json", date_summary)
    write_json(path, manifest)
    return manifest


def _normalized_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", unescape(re.sub(r"<[^>]+>", " ", value)).lower()).strip()


def verify_f6_issuer(content: str, issuer_name: str) -> bool:
    """A search hit is not issuer identity: require the F-6 issuer cover field."""
    text = _normalized_text(content[:100000])
    name = _normalized_text(issuer_name)
    if not name:
        return False
    for marker in re.finditer(r"exact name of (?:the )?issuer of (?:the )?deposited securities", text):
        field = text[max(0, marker.start() - 600):marker.start()]
        if re.search(r"(?:^| )" + re.escape(name) + r"(?: |$)", field):
            return True
    return False


def verify_securities_description(content: str) -> bool:
    """Require the attachment's own description heading and listing/ADS text."""
    visible = _normalized_text(content[:150000])
    heading = re.search(r"\bdescription of\b.{0,180}\bsecurities\b", visible[:6000])
    listing = re.search(r"\bsection 12 b\b|\bamerican depositary (?:shares|receipts)\b", visible)
    return bool(heading and listing)


def bind_historical_symbols(rows: list[dict], document: dict, observations: list[dict]) -> list[dict]:
    """Bind a symbol-less cover only to the same SEC accession's W1 cover.

    Modern aliases or later filings are never used. Even a sole modern ADS line
    cannot manufacture a trading symbol for an earlier document.
    """
    public_on = date.fromisoformat(document["filed"]) + timedelta(days=1)
    if document.get("publication_floor_on"):
        public_on = max(public_on, date.fromisoformat(document["publication_floor_on"]))
    matches = [observation for observation in observations
               if int(observation["cik"]) == int(document["cik"])
               and observation["adsh"] == document["adsh"]
               and observation.get("security_kind") in {"equity", "depositary", "unknown"}
               and observation.get("filing_complete")
               and date.fromisoformat(observation["available_on"]) <= public_on
               and (not observation.get("retired_on") or date.fromisoformat(observation["retired_on"]) > public_on)]
    keys = {re.sub(r"[^A-Z0-9]", "", observation["ticker"].upper()) for observation in matches}
    if len(keys) != 1:
        return rows
    symbol = sorted({observation["ticker"].upper().replace(".", "-") for observation in matches})[0]
    for row in rows:
        if row["evidence_kind"] == "listed_type" and row.get("symbol") is None:
            row["symbol"] = symbol
            row["evidence_location"] += "; W1 same-accession cover symbol " + symbol
            row["evidence_text"] += " [Same-accession W1 symbol: " + symbol + "; accession " + document["adsh"] + "]"
    return rows


def extract_pdf_pages(raw: bytes, *, cache_dir: Path) -> tuple[list[str], str]:
    """Extract untrusted PDF bytes as data; never invoke a shell or PDF actions."""
    executable = shutil.which("pdftotext")
    if not executable and os.name == "nt":
        bundled = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Git/mingw64/bin/pdftotext.exe"
        if bundled.is_file():
            executable = str(bundled)
    if executable:
        work = cache_dir / "pdf-work"
        work.mkdir(parents=True, exist_ok=True)
        # Older Windows Xpdf builds do not support stdin. Owned temporary
        # input/output files work with both Xpdf and Poppler without a shell.
        with tempfile.TemporaryDirectory(prefix="extract-", dir=work) as temporary:
            original = Path(temporary) / "source.pdf"
            extracted = Path(temporary) / "source.txt"
            original.write_bytes(raw)
            process = subprocess.run(
                [executable, "-layout", "-enc", "UTF-8", str(original), str(extracted)],
                capture_output=True, timeout=120, shell=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0,
            )
            if process.returncode:
                raise ValueError(f"PDF text extraction failed with exit code {process.returncode}")
            pages = extracted.read_text(encoding="utf-8", errors="replace").split("\f")
        if pages and not pages[-1].strip():
            pages.pop()  # Poppler terminates the final page with a form feed.
        method = "pdftotext-layout-utf8"
    else:
        try:
            from pypdf import PdfReader
        except ImportError:
            raise RuntimeError("PDF extraction requires pdftotext (Poppler) or pypdf") from None
        reader = PdfReader(io.BytesIO(raw))
        pages = [page.extract_text(extraction_mode="layout") or "" for page in reader.pages]
        method = "pypdf-layout"
    if not any(page.strip() for page in pages):
        raise ValueError("PDF has no extractable text")
    return pages, method


def pdf_parser_input(pages: list[str]) -> tuple[str, str, list[tuple[int, int, int]]]:
    """Build inert HTML text plus exact offsets in the parser's normalized text."""
    normalized_parts = []
    ranges = []
    length = 0
    for number, page in enumerate(pages, 1):
        # Match the filing parser's whitespace normalization, including its
        # one trailing separator for each nonempty HTML text node/page.
        clean = re.sub(r"\s+", " ", page.replace("\xa0", " ").replace("\u200b", "")).strip()
        if clean:
            normalized_parts.append(clean + " ")
            ranges.append((number, length, length + len(clean) + 1))
            length += len(clean) + 1
    html = "".join("<div>" + escape(page) + "</div>" for page in pages)
    return html, "".join(normalized_parts), ranges


def locate_pdf_evidence(row: dict, normalized: str, ranges: list[tuple[int, int, int]]) -> None:
    location = re.search(r"(?:^|;)text-offset=(\d+)", row["evidence_location"])
    needle = row["evidence_text"].split(" [Same-accession W1 symbol:", 1)[0]
    needle = re.sub(r"\s+", " ", needle.replace("\xa0", " ").replace("\u200b", "")).strip()
    anchor = int(location.group(1)) if location else -1
    start = normalized.find(needle, max(0, anchor - len(needle)))
    if start >= 0 and (anchor < 0 or start <= anchor < start + len(needle)):
        end = start + len(needle)
    elif anchor >= 0:
        start, end = anchor, anchor + 1
    else:
        raise ValueError("PDF evidence normalized position is unavailable")
    pages = [number for number, first, last in ranges if start < last and end > first]
    if not pages:
        raise ValueError("PDF evidence offset is outside the extracted pages")
    page_label = str(pages[0]) if len(pages) == 1 else f"{pages[0]}-{pages[-1]}"
    row["evidence_location"] = (f"pdf/pages={page_label};normalized-text-span={start}:{end};" + row["evidence_location"])


def f6_attachment_binding_proof(client: SecClient, document: dict, sources: list[dict]) -> dict | None:
    """Bind an F-6 exhibit through its actual issuer field in the same filing.

    A depositary's registrant CIK is not issuer proof. Every candidate parent is
    read and must explicitly name the expected issuer in its F-6 cover field.
    The client enforces offline replay and original-byte cache verification.
    """
    issuer_name = document.get("issuer_name", "")
    if not issuer_name or document["form"].removesuffix("/A") not in F6_FORMS:
        return None
    candidates = [source for source in sources
                  if int(source["cik"]) == int(document["cik"])
                  and source["adsh"] == document["adsh"] and source["filed"] == document["filed"]
                  and source["form"].removesuffix("/A") in F6_FORMS
                  and source["source_url"] != document["source_url"]]
    candidates.sort(key=lambda source: (source.get("binding") != "registrant_cik", source["source_url"]))
    for candidate in candidates:
        try:
            raw, sha256 = client.document(candidate["source_url"], expected_sha256=candidate.get("source_sha256"))
        except (ValueError, RuntimeError, OSError):
            continue
        if verify_f6_issuer(raw.decode("utf-8-sig", errors="replace"), issuer_name):
            recovery = getattr(client, "recovery_proofs", {}).get(candidate["source_url"], {})
            return {"source_url": recovery.get("recovery_document_url", candidate["source_url"]), "source_sha256": sha256,
                    "adsh": candidate["adsh"], "cik": int(document["cik"]), "issuer_name": issuer_name,
                    **({"discovery_source_url": candidate["source_url"], "source_recovery_proof": recovery} if recovery else {}),
                    "evidence_location": "f6/cover/exact-name-of-issuer-of-deposited-securities"}
    return None


def parse_document(client: SecClient, document: dict, observations: list[dict] | None = None,
                   *, downloaded: tuple[bytes, str] | None = None,
                   binding_sources: list[dict] | None = None) -> tuple[dict, list[dict]]:
    document = canonical_document(document)
    document.pop("issuer_binding_proof", None)  # Re-establish proof from verified source bytes on every replay.
    document.pop("document_recovery_proof", None)
    if __package__:
        from .sec_foreign_listing_parser import PARSER_VERSION, parse_filing
    else:
        from sec_foreign_listing_parser import PARSER_VERSION, parse_filing

    raw, sha256 = downloaded if downloaded is not None else client.document(
        document["source_url"], expected_sha256=document.get("source_sha256"))
    recovery = getattr(client, "recovery_proofs", {}).get(document["source_url"])
    if recovery:
        document = {**document, "document_recovery_proof": recovery}
    pdf_ranges = None
    if raw.lstrip().startswith(b"%PDF-"):
        pages, method = extract_pdf_pages(raw, cache_dir=client.cache)
        text, pdf_text, pdf_ranges = pdf_parser_input(pages)
        document = {**document, "content_format": "pdf", "pdf_text_extractor": method, "pdf_pages": len(pages)}
    else:
        text = raw.decode("utf-8-sig", errors="replace")
    role = document.get("document_role", "primary")
    if role == "securities_description" and not verify_securities_description(text):
        return {**document, "source_sha256": sha256, "parser_version": PARSER_VERSION,
                "status": "not_securities_description", "evidence_count": 0}, []
    if document["binding"] == "issuer_name_in_f6":
        if not verify_f6_issuer(text, document["issuer_name"]):
            proof = f6_attachment_binding_proof(client, document, binding_sources or [])
            if proof is None:
                return {**document, "source_sha256": sha256, "parser_version": PARSER_VERSION,
                        "status": "issuer_binding_unverified", "evidence_count": 0}, []
            document = {**document, "issuer_binding_proof": proof}
    rows = parse_filing(text, cik=document["cik"], form_type=document["form"],
                        accession_number=document["adsh"], filing_date=document["filed"],
                        source_url=document["source_url"], symbols=document["symbols"], document_role=role)
    if observations:
        rows = bind_historical_symbols(rows, document, observations)
    for row in rows:
        if document.get("publication_floor_on"):
            row["publication_floor_on"] = document["publication_floor_on"]
            row["available_on"] = max(str(row["available_on"]), document["publication_floor_on"])
            row["publication_floor_proof"] = document.get("publication_floor_proof")
            row["evidence_location"] += (";publication-floor=" + document["publication_floor_on"]
                                         + ";publication-floor-source="
                                         + (document.get("publication_floor_proof") or {}).get("source", "discovery_reported_publication_floor"))
        if pdf_ranges is not None:
            locate_pdf_evidence(row, pdf_text, pdf_ranges)
        row["source_sha256"] = sha256
        row["source_package"] = document["source_package"]
        if document.get("issuer_binding_proof"):
            proof = document["issuer_binding_proof"]
            row["issuer_binding_proof"] = proof
            row["evidence_location"] += (";issuer-cover=" + proof["source_url"]
                                         + ";issuer-cover-sha256=" + proof["source_sha256"])
        if recovery:
            row["document_recovery_proof"] = recovery
            if recovery.get("recovery_source_url"):
                row["evidence_location"] += (";recovery-submission=" + recovery["recovery_source_url"]
                                             + ";recovery-submission-sha256=" + recovery["recovery_source_sha256"])
            if recovery.get("recovery_retrieval_url"):
                row["evidence_location"] += ";retrieved-from=" + recovery["recovery_retrieval_url"]
            if recovery.get("recovery_index_url"):
                row["evidence_location"] += (";sec-index=" + recovery["recovery_index_url"]
                                             + ";sec-index-sha256=" + recovery["recovery_index_sha256"]
                                             + ";sec-index-row=" + recovery["recovery_index_line"])
            row["evidence_location"] += ";recovery-filename=" + recovery["recovery_filename"]
        if document.get("filing_date_proof"):
            proof = document["filing_date_proof"]
            row["filing_date_proof"] = proof
            row["evidence_location"] += ";filed-date=" + document["filed"] + ";filed-date-source=" + proof["source"]
            locations = {(record.get("source_url"), record.get("source_sha256")) for record in proof.get("records", [])
                         if record.get("source_url") and record.get("source_sha256")}
            for proof_url, proof_sha in sorted(locations):
                row["evidence_location"] += ";filed-date-reference=" + proof_url + ";filed-date-reference-sha256=" + proof_sha
            if proof.get("observations_sha256"):
                row["evidence_location"] += ";filed-date-w1-export-sha256=" + proof["observations_sha256"]
        row["fact_hash"] = hashlib.md5(canonical_json(row).encode(), usedforsecurity=False).hexdigest()
    return {**document, "source_sha256": sha256, "parser_version": PARSER_VERSION,
            "status": "parsed", "evidence_count": len(rows)}, rows


def require_authoritative_filing_dates(manifest: dict) -> None:
    enrichment = manifest.get("filing_date_enrichment", {})
    if not enrichment.get("complete") or enrichment.get("version") != "sec-official-filing-date-v2":
        raise ValueError("Authoritative SEC filing-date enrichment is required before parsing or applying evidence")
    groups: dict[str, list[dict]] = {}
    for document in manifest["documents"]:
        proof = document.get("filing_date_proof") or {}
        if (document.get("filing_date_status") != "resolved" or not document.get("filed")
                or proof.get("filed") != document["filed"]
                or proof.get("source") not in {"sec_quarter_master_index", "sec_daily_master_index", "sec_submission_header", "w1_same_accession"}):
            raise ValueError("Source document has no matching authoritative SEC filing-date proof")
        groups.setdefault(document["adsh"], []).append(document)
    for accession, documents in groups.items():
        reported = {document.get("query_accepted_on") for document in documents}
        floors = {document.get("publication_floor_on") for document in documents}
        if None in reported or None in floors or len(floors) != 1 or next(iter(floors)) < max(reported):
            raise ValueError("Source accession publication floor is missing or precedes reported publication")
        floor = next(iter(floors))
        date.fromisoformat(floor)
        year = int(accession[11:13])
        year = year + (1900 if year >= 93 else 2000)
        if int(documents[0]["filed"][:4]) < year and int(floor[:4]) < year:
            raise ValueError("Backdated accession needs its actual SEC publication/acceptance date")
        if any((document.get("publication_floor_proof") or {}).get("source") not in
               {"discovery_reported_publication_floor", "sec_acceptance_header"} for document in documents):
            raise ValueError("Source publication floor has no matching provenance")


def parse_manifest(client: SecClient, manifest: dict, output: Path, workers: int = 4,
                   observations: list[dict] | None = None,
                   binding_sources: list[dict] | None = None) -> dict:
    require_authoritative_filing_dates(manifest)
    # A URL can be associated with two issuer candidates. One worker downloads
    # and parses all its candidates, retaining at most one original per worker.
    # Legacy manifests may contain SEC inline-viewer URLs. Canonicalize before
    # deduplication/download and recompute the stable source identity consistently.
    canonical_documents: dict[str, dict] = {}
    for raw_document in manifest["documents"]:
        document = canonical_document(raw_document)
        previous = canonical_documents.get(document["source_package"])
        if previous and previous.get("source_sha256") and document.get("source_sha256") and previous["source_sha256"] != document["source_sha256"]:
            raise ValueError("Conflicting content hashes for the same canonical source")
        if not previous or (previous["binding"] == "issuer_name_in_f6" and document["binding"] == "registrant_cik"):
            canonical_documents[document["source_package"]] = document
    documents = [canonical_documents[key] for key in sorted(canonical_documents)]
    # Apply the exact-form rule on replay too, including manifests discovered
    # before this parser version. Keep exclusions in an auditable side list.
    excluded = [row for row in documents if row["form"] not in ANNUAL_FORMS + F6_FORMS + ("6-K", "6-K/A")]
    manifest["excluded_forms"] = excluded
    documents = [row for row in documents if row not in excluded]
    manifest["documents"] = documents
    observations_by_filing: dict[tuple[int, str], list[dict]] = {}
    for observation in observations or []:
        observations_by_filing.setdefault((int(observation["cik"]), observation["adsh"]), []).append(observation)
    binding_sources_by_filing: dict[tuple[int, str], list[dict]] = {}
    for source in binding_sources if binding_sources is not None else documents:
        binding_sources_by_filing.setdefault((int(source["cik"]), source["adsh"]), []).append(source)
    by_url: dict[str, list[dict]] = {}
    for document in documents:
        by_url.setdefault(document["source_url"], []).append(document)
    spool = client.cache / "parsed"
    spool.mkdir(parents=True, exist_ok=True)
    results: dict[str, dict] = {}

    def download_and_parse(url: str, candidates: list[dict]) -> list[dict]:
        expected = {item["source_sha256"] for item in candidates if item.get("source_sha256")}
        if len(expected) > 1:
            raise ValueError("Conflicting content hashes for a shared document URL")
        downloaded = client.document(url, expected_sha256=next(iter(expected), None))
        updated_documents = []
        for document in candidates:
            try:
                filing_observations = observations_by_filing.get((int(document["cik"]), document["adsh"]))
                filing_sources = binding_sources_by_filing.get((int(document["cik"]), document["adsh"]), [])
                updated, rows = parse_document(client, document, filing_observations,
                                               downloaded=downloaded, binding_sources=filing_sources)
                updated.pop("error", None)
                # Always reparse and replace the spool, even on offline replay.
                # It is an output staging file, never an input/cache authority.
                payload = "".join(canonical_json(row) + "\n" for row in rows).encode("utf-8")
                write_bytes(spool / (document["source_package"] + ".jsonl"), payload)
                updated_documents.append(updated)
            except Exception as exc:
                updated_documents.append({**document, "status": "failed", "error": type(exc).__name__ + ": " + str(exc)[:200]})
        # Futures retain only small metadata. Original bytes and parsed rows are
        # released inside this worker before another unique URL is scheduled.
        return updated_documents

    errors = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(download_and_parse, url, candidates): url for url, candidates in by_url.items()}
        for index, future in enumerate(as_completed(futures), 1):
            url = futures.pop(future)
            try:
                updated_documents = future.result()
            except Exception as exc:
                updated_documents = [{**document, "status": "failed", "error": type(exc).__name__ + ": " + str(exc)[:200]}
                                     for document in by_url[url]]
            for updated in updated_documents:
                results[updated["source_package"]] = updated
                errors += updated.get("status") == "failed"
                if updated.get("status") == "failed":
                    failure = {"event": "source_failed", "cik": updated["cik"], "adsh": updated["adsh"],
                               "source_url": updated["source_url"], "source_package": updated["source_package"],
                               "error": updated["error"]}
                    print(canonical_json(failure), flush=True)
                    try:
                        write_json(client.cache / "failures" / (updated["source_package"] + ".json"), failure)
                    except OSError as exc:
                        print(canonical_json({"event": "failure_log_write_failed", "error_type": type(exc).__name__}), flush=True)
            if index % 100 == 0 or index == len(by_url):
                print(canonical_json({"event": "download", "completed": index, "total": len(by_url),
                                      "parsed_documents": len(results), "errors": errors}), flush=True)
    documents = [results[key] for key in sorted(results)]
    manifest["documents"] = documents
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + "." + uuid4().hex + ".tmp")
    count = 0
    with temporary.open("wb") as handle:
        for document in documents:
            if document["status"] == "failed":
                continue
            with (spool / (document["source_package"] + ".jsonl")).open("rb") as parsed:
                shutil.copyfileobj(parsed, handle, length=1024 * 1024)
            count += document["evidence_count"]
    replace_file(temporary, output)
    manifest["evidence_sha256"] = digest(output.read_bytes())
    manifest["observations_sha256"] = digest(canonical_json(observations).encode()) if observations is not None else None
    manifest["evidence_count"] = count
    manifest["parse_complete"] = all(row.get("status") in {"parsed", "issuer_binding_unverified", "not_securities_description"} for row in documents)
    write_json(client.cache / "manifest.json", manifest)
    return {"documents": len(documents), "evidence_rows": count, "failed_documents": sum(row.get("status") == "failed" for row in documents),
            "unverified_bindings": sum(row.get("status") == "issuer_binding_unverified" for row in documents),
            "rejected_description_candidates": sum(row.get("status") == "not_securities_description" for row in documents),
            "discovery_complete": manifest["complete"], "parse_complete": manifest["parse_complete"]}


FACT_COLUMNS = ("fact_hash", "cik", "symbol", "adsh", "form", "filed", "source_url", "source_sha256", "source_kind",
                "evidence_kind", "listed_type", "underlying_class", "ordinary_candidate",
                "ratio_numerator", "ratio_denominator", "effective_from", "effective_to", "effective_date_explicit",
                "evidence_text", "evidence_location", "parser_version", "publication_floor_on", "available_on", "loaded_on", "source_package")
APPLY_BATCH_SIZE = 1000


def apply_evidence(connection: Any, manifest: dict, rows: Iterable[dict], observed_on: date) -> dict:
    """Reconcile complete source documents transactionally, retaining old versions.

    Passing a DB connection is deliberate: offline collection never creates one.
    An initial source uses filing+1 availability; additions to an already-loaded
    source become public no earlier than the reconciliation date. Missing files
    cannot retire facts. A successfully parsed empty document can retire them.
    """
    if not manifest.get("complete") or not manifest.get("parse_complete"):
        raise ValueError("Only a complete discovery and parse manifest can be applied")
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["source_package"], []).append(row)
    if set(grouped) - {document["source_package"] for document in manifest["documents"]}:
        raise ValueError("Evidence artifact contains an unmanifested source")
    require_authoritative_filing_dates(manifest)
    counters = {"inserted": 0, "retired": 0, "unchanged": 0, "sources": 0}
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(79311, 173)")
        # Snapshot before inserting any document: all documents of an accession
        # in its first batch inherit the source date. A later-added exhibit is a
        # correction even if its individual source_package has never been seen.
        cursor.execute("SELECT source_package,cik,adsh,last_loaded_on FROM public.sec_foreign_listing_sources FOR UPDATE")
        prior_sources = {}
        prior_accessions = {}
        for package, cik, adsh, loaded in cursor.fetchall():
            prior_sources[package] = loaded
            key = (int(cik), adsh)
            prior_accessions[key] = max(loaded, prior_accessions.get(key, loaded))
        cursor.execute("SELECT source_package,fact_hash FROM public.sec_foreign_listing_evidence WHERE retired_on IS NULL")
        current: dict[str, set[str]] = {}
        for package, fact_hash in cursor.fetchall():
            current.setdefault(package, set()).add(fact_hash)
        insert_sql = ("INSERT INTO public.sec_foreign_listing_evidence (" + ",".join(FACT_COLUMNS) + ") VALUES ("
                      + ",".join(["%s"] * len(FACT_COLUMNS)) + ")")
        retire_sql = ("UPDATE public.sec_foreign_listing_evidence SET retired_on=%s "
                      "WHERE source_package=%s AND retired_on IS NULL AND fact_hash=ANY(%s)")
        source_sql = """INSERT INTO public.sec_foreign_listing_sources
            (source_package,adsh,cik,source_url,source_sha256,parser_version,first_loaded_on,last_loaded_on,evidence_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (source_package) DO UPDATE SET source_sha256=EXCLUDED.source_sha256,
            parser_version=EXCLUDED.parser_version,last_loaded_on=EXCLUDED.last_loaded_on,evidence_count=EXCLUDED.evidence_count"""
        insert_batch: list[tuple] = []
        retire_batch: list[tuple] = []
        source_batch: list[tuple] = []

        def flush(sql: str, batch: list[tuple], *, retirement: bool = False) -> None:
            if batch:
                # psycopg 3 pipelines executemany on supported connections;
                # bounded batches avoid one tunnel round trip per fact/source.
                cursor.executemany(sql, batch)
                if retirement:
                    counters["retired"] += cursor.rowcount
                batch.clear()

        for document in manifest["documents"]:
            package = document["source_package"]
            facts = grouped.get(package, [])
            if len(facts) != document["evidence_count"]:
                raise ValueError("Evidence count does not match source manifest")
            previous = prior_sources.get(package)
            prior_accession = prior_accessions.get((int(document["cik"]), document["adsh"]))
            if (previous and previous > observed_on) or (prior_accession and prior_accession > observed_on):
                raise ValueError("Reconciliation date cannot precede prior load")
            existing = current.get(package, set())
            incoming = {row["fact_hash"] for row in facts}
            if len(incoming) != len(facts):
                raise ValueError("Evidence artifact contains duplicate facts")
            removed = existing - incoming
            if removed:
                retire_batch.append((observed_on, package, sorted(removed)))
                if len(retire_batch) >= APPLY_BATCH_SIZE:
                    flush(retire_sql, retire_batch, retirement=True)
            for fact in facts:
                if str(fact["filed"]) != str(document["filed"]):
                    raise ValueError("Evidence filing date does not match its authoritative source manifest")
                if fact.get("publication_floor_on") != document.get("publication_floor_on"):
                    raise ValueError("Evidence publication floor does not match its source manifest")
                if fact["fact_hash"] in existing:
                    counters["unchanged"] += 1
                    continue
                source_available = date.fromisoformat(str(fact["filed"])) + timedelta(days=1)
                if fact.get("publication_floor_on"):
                    source_available = max(source_available, date.fromisoformat(str(fact["publication_floor_on"])))
                available = max(source_available, observed_on) if prior_accession else source_available
                values = {"ordinary_candidate": True, "effective_date_explicit": False,
                          **fact, "available_on": available, "loaded_on": observed_on}
                insert_batch.append(tuple(values.get(column) for column in FACT_COLUMNS))
                if len(insert_batch) >= APPLY_BATCH_SIZE:
                    flush(insert_sql, insert_batch)
                counters["inserted"] += 1
            parser_version = facts[0]["parser_version"] if facts else document["parser_version"]
            source_batch.append((package, document["adsh"], document["cik"], document["source_url"],
                                 document["source_sha256"], parser_version, observed_on, observed_on, len(facts)))
            if len(source_batch) >= APPLY_BATCH_SIZE:
                flush(source_sql, source_batch)
            counters["sources"] += 1
        flush(retire_sql, retire_batch, retirement=True)
        flush(insert_sql, insert_batch)
        flush(source_sql, source_batch)
    return counters


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--observations", type=Path, help="Read-only W1 JSON export for same-accession historical symbol binding")
    parser.add_argument("--discover", action="store_true")
    parser.add_argument("--refresh-discovery", action="store_true", help="Recheck every issuer using current queries and cached exact query responses")
    parser.add_argument("--download", action="store_true", help="Download and parse manifested documents")
    parser.add_argument("--offline", action="store_true", help="Verify and parse cached documents without network")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(1993, 1, 1))
    parser.add_argument("--end-date", type=date.fromisoformat, default=datetime.now(timezone.utc).date())
    parser.add_argument("--dotenv", type=Path, default=Path("E:/investintell-light/backend/.env"))
    parser.add_argument("--workers", type=int, choices=range(1, 9), default=4)
    parser.add_argument("--requests-per-second", type=float, default=5)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--database-url-env", default="FOREIGN_EVIDENCE_DATABASE_URL")
    parser.add_argument("--observed-on", type=date.fromisoformat, default=datetime.now(timezone.utc).date())
    args = parser.parse_args(argv)
    if args.start_date > args.end_date:
        parser.error("start-date must be <= end-date")
    repository = Path(__file__).resolve().parents[1]
    if args.cache_dir.resolve().is_relative_to(repository):
        parser.error("cache-dir must be outside the repository")
    if args.offline and args.discover:
        parser.error("--offline and --discover are mutually exclusive")
    if args.refresh_discovery and not args.discover:
        parser.error("--refresh-discovery requires --discover")
    universe = read_universe(args.universe)
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    key = load_key(args.dotenv) if not args.offline and (args.discover or args.download) else ""
    client = SecClient(args.cache_dir, key, offline=args.offline, requests_per_second=args.requests_per_second)
    manifest_path = args.cache_dir / "manifest.json"
    observations = json.loads(args.observations.read_text(encoding="utf-8-sig")) if args.observations else None
    manifest = discover(client, universe, args.start_date, args.end_date, args.workers,
                        refresh=args.refresh_discovery, observations=observations) if args.discover else json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest["universe_sha256"] != digest(canonical_json(universe).encode()):
        raise ValueError("Manifest universe hash mismatch")
    if args.download or args.offline:
        summary = parse_manifest(client, manifest, args.output, args.workers, observations)
        write_json(args.output.with_suffix(".summary.json"), summary)
        print(canonical_json(summary), flush=True)
    if args.apply:
        if digest(args.output.read_bytes()) != manifest.get("evidence_sha256"):
            raise ValueError("Evidence artifact hash mismatch")
        dsn = os.environ.get(args.database_url_env)
        if not dsn:
            raise ValueError("The configured database URL environment variable is empty")
        import psycopg
        with psycopg.connect(dsn) as connection:
            with args.output.open(encoding="utf-8") as handle:
                result = apply_evidence(connection, manifest, (json.loads(line) for line in handle if line.strip()), args.observed_on)
        print(canonical_json({"event": "applied", **result}), flush=True)
    return 0 if manifest.get("complete") and (not (args.download or args.offline or args.apply) or manifest.get("parse_complete")) else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        # HTTP libraries can put credentials in URLs; never print their raw
        # exception object here. The run's manifest retains safe stage errors.
        print(canonical_json({"status": "failed", "error_type": type(error).__name__}), file=sys.stderr)
        raise SystemExit(1) from None
