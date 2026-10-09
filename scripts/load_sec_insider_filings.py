"""Load point-in-time ticker -> CIK evidence from SEC Forms 3, 4 and 5.

DERA quarterly packages supply SUBMISSION.tsv from 2006 onward. Original
sec-api.io form-3/4/5-files monthly archives supply ownership XML from May
2003 through 2005. Only filing identifiers, issuer symbols and filing dates
are read; archive members are never extracted or executed.

Each package is reconciled in one transaction. Republication retires facts
and their package carriers; corrections become available on the reconciliation
date. An unchanged fact retains its original availability. No outside CIK
mapping is consulted. Schema application is an explicit operator action.

Examples (run from the repository root):
  py -3.13 -m scripts.load_sec_insider_filings --packages-dir E:/investintell-data/w1b/dera --download-dera --verify-cache
  py -3.13 -m scripts.load_sec_insider_filings --secapi-dir E:/investintell-data/w1b/secapi --download-secapi
  py -3.13 -m scripts.load_sec_insider_filings --packages-dir E:/investintell-data/w1b/dera --secapi-dir E:/investintell-data/w1b/secapi --dsn postgresql://postgres@127.0.0.1:55439/insider
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import html
import io
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path, PurePosixPath
from typing import Iterator
from zoneinfo import ZoneInfo

# W1's contract module is imported read-only: extra filer-text rules live here.
from scripts.load_sec_ticker_cik_history import (
    EXCHANGE_TOKENS,
    PLACEHOLDER_KEYS,
    _is_suffix_token,
    normalize_symbol,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schemas" / "sec_insider_ticker_evidence.sql"
PARSER_VERSION = "sec_insider_v4"
USER_AGENT = "InvestIntell-SEP-Ingestion/1.0 (+https://hub.investintell.com)"
DERA_LISTING_URL = "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets"
SECAPI_BASE = "https://api.sec-api.io"
SECAPI_STREAM_RETRY = frozenset({400})
DEFAULT_ROOT = Path("E:/investintell-data/w1b")
DEFAULT_DOTENV = Path("E:/investintell-light/backend/.env")
DERA_RE = re.compile(r"^(?P<year>\d{4})q(?P<quarter>[1-4])_form345\.zip$", re.I)
ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
FORMS = frozenset({"3", "3/A", "4", "4/A", "5", "5/A"})
DATASETS = ("form-3-files", "form-4-files", "form-5-files")
_MONTHS = {name: n for n, name in enumerate(
    ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1
)}
_PREFIX = re.compile(r"^(?:NYSE(?:\s*(?:AMERICAN|ARCA|MKT))?|NASDAQ(?:\s*(?:GS|GM|CM))?|AMEX|OTC(?:BB|QB|QX)?|OTC\s+MARKETS|CBOE|TSX|LSE)\s*[:/\s]\s*", re.I)
_OTC_SUFFIX = re.compile(r"(?<=[A-Z0-9])(?:[.,]\s*|\s+)(?:OB|PK)\b", re.I)
_WRAPPER = re.compile(r"\(([^()]*)\)|\[([^\[\]]*)\]")
# Whole-field placeholders: W1's set plus phrases insider filers type (DERA 2006-2026).
# W1b keeps its own copy, so a change to W1's set needs a parser version here.
# sec_insider_v4 reads TRUE and OB as symbols, as W1 does. In DERA and the 2003-2005
# XML every whole-field TRUE is uppercase and the issuer's own symbol (TrueCar,
# Centrue: 1,545 filings); OB alone is OneBeacon's or Outbrain's (394 of 396).
# A lowercase or mixed-case "true" stays a boolean, and OB beside a symbol still
# qualifies it (EDLG, OB). FALSE never occurs; OTCBB alone never names a symbol.
_PLACEHOLDERS = PLACEHOLDER_KEYS | {
    "FALSE", "OTCBB",
    "", "NOSYMBOL", "NOTRADINGSYMBOL", "NOTICKER", "NOTPUBLIC", "NOTTRADING", "NONEYET",
    "TOCOME", "SEEREMARK", "SEEREMARKS", "INREMARKS", "APPFOR", "APPLIED", "APPLIEDFOR",
    "PENDING", "UNKNOWN", "PRIVATE", "SYMBOL", "XXXXXXXXXX",
}
# In a multi-word field these qualify a symbol rather than name one: exchanges and
# OTC tiers (FLL AMEX), country codes (FF US, TAM LN) and when-issued (CARR WI).
_QUALIFIERS = EXCHANGE_TOKENS | {"NSYE", "OTBB", "OB", "PK", "US", "LN", "WI"}
_EXCHANGE_LABEL = re.compile(r"[A-Za-z.]{2,6}:")  # ASX: HTW, TSX.V: AFH
# A corporate form makes a multi-word field an issuer name (DEERE & CO, XPEL, INC.).
_COMPANY_WORDS = frozenset({"INC", "CORP", "CO", "COMPANY", "LTD", "LLC", "LP", "PLC", "GROUP"})
_LIST_DELIMITER = re.compile(r"[,;&]|\s+AND\s+", re.I)
_STATE_DESIGNATOR = re.compile(r"(?<![A-Za-z0-9])/[A-Z]{2}/")
_TOKEN_SECRET = re.compile(r"((?:token|api[_-]?key|password)=)[^&\s'\"]+", re.I)
_SECRETS: set[str] = set()
NEW_YORK = ZoneInfo("America/New_York")
MAX_XML_BYTES = 16 * 1024 * 1024
FACT_COLUMNS = ("accession", "cik", "raw_symbol", "normalized_symbols", "form", "filed", "accepted", "source")
STAGE_COLUMNS = FACT_COLUMNS + ("source_package", "fact_hash", "source_available_on")


def scrub(value: object) -> str:
    """Prevent credentials from reaching console output or exception messages."""
    result = _TOKEN_SECRET.sub(r"\1***", str(value))
    result = re.sub(r"(postgres(?:ql)?://[^:/\s]+:)[^@\s]+@", r"\1***@", result, flags=re.I)
    for secret in _SECRETS:
        if secret:
            result = result.replace(secret, "***")
    return result


def log(value: object) -> None:
    print(scrub(json.dumps(value, default=str, sort_keys=True)), flush=True)


def _key(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def _is_placeholder(value: str) -> bool:
    key = _key(value)
    if key == "TRUE":  # the symbol only as written in uppercase
        return not re.sub(r"[^A-Za-z]", "", value).isupper()
    return key in _PLACEHOLDERS or bool(re.fullmatch(r"X{3,}", key))


def _letters(value: str) -> int:
    return len(re.sub(r"[^A-Za-z]", "", value))


def _is_suffix(token: str) -> bool:
    """A class, series, line or preferred suffix (``A``, ``WS``, ``PrB``, ``PR.A``)."""
    return _is_suffix_token(token) or _is_suffix_token(re.sub(r"[.\-]", "", token).upper())


def _item_tokens(item: str) -> list[tuple[str, bool]]:
    """Tokens of one list item. True marks a token separated from the previous
    one by whitespace alone; a slash or a standalone dash (``PHC - PIHC``) lists."""
    tokens: list[tuple[str, bool]] = []
    spaced = True
    for piece in re.split(r"(/)|\s+", item):
        if not piece:
            continue
        if piece == "/" or re.fullmatch(r"[-.:]+", piece):
            spaced = False
            continue
        tokens.append((piece, spaced))
        spaced = True
    return tokens


def _sibling_line(previous: str, token: str) -> str | None:
    """The sibling class a lone listed class names: ``BWINA / B`` is BWINB,
    ``CTMMA,B`` is CTMMB and ``SEAL-PA/PB`` is SEAL-PB."""
    if re.fullmatch(r"[ABCK]", token, re.I) and re.fullmatch(r"[A-Z]{4,}[ABCK]", previous, re.I):
        return previous[:-1] + token if previous[-1].upper() != token.upper() else None
    match = re.fullmatch(r"(.+[-.])([^-.]+)", previous)
    if (match and len(match[2]) == len(token) and match[2].upper() != token.upper()
            and _is_suffix_token(match[2].upper()) and _is_suffix_token(token.upper())):
        return match[1] + token
    return None


def _drop_words(items: list, unwanted) -> list:
    """Remove unwanted words, unless they are the only words in the field."""
    kept = [[(t, s) for t, s in tokens if not unwanted(t)] for tokens in items]
    return [tokens for tokens in kept if tokens] if any(kept) else items


def normalize_symbols(raw: str) -> list[str]:
    """Normalize the free-text issuer symbol without changing W1's normalizer.

    Placeholders and prose are rejected whole before any split: ``NOT LISTED``,
    ``SEE REMARK``, issuer names (``LEE ENT``, ``XPEL, INC.``). Explicit list
    delimiters split (``LTR;CG``, ``Z AND ZG``), as does a slash (``ABI/CRA``).
    A space splits only class variants of one root (``CRDA CRDB``). Exchange,
    country and when-issued words are dropped (``FF US``). A class, series or
    preferred suffix stays attached (``BRK/A``, ``BF'B``, ``HFC PrB``); a lone
    listed class names the sibling line (``BWINA / B``).
    """
    value = html.unescape(raw).strip()
    value = _WRAPPER.sub(lambda m: m.group(1) or m.group(2) or "", value)
    value = value.strip('"\u201c\u201d?').strip()
    value = value.strip("'\u2018\u2019").strip()
    if _is_placeholder(value):
        return []
    value = _OTC_SUFFIX.sub("", value)
    # EDGAR's state-of-incorporation designator belongs to a name (/DE/CHD).
    value = _STATE_DESIGNATOR.sub(" ", value).strip()
    items: list[list[tuple[str, bool]]] = []
    for item in _LIST_DELIMITER.split(value):
        item = _PREFIX.sub("", item.strip()).strip('"?').strip()
        # Apostrophes are symbol separators, not letters to be silently glued.
        tokens = _item_tokens(re.sub(r"['\u2018\u2019]", "-", item))
        if tokens:
            items.append(tokens)
    items = _drop_words(items, lambda t: _key(t) in _QUALIFIERS)
    items = _drop_words(items, _EXCHANGE_LABEL.fullmatch)  # BDGV: OTC keeps BDGV
    words = [_key(t) for tokens in items for t, _ in tokens]
    if len(words) > 1 and any(w and (w in _PLACEHOLDERS or w in _COMPANY_WORDS) for w in words):
        return []
    groups: list[str] = []
    for tokens in items:
        for position, (token, spaced) in enumerate(tokens):
            sibling = _sibling_line(groups[-1], token) if groups and (position == 0 or not spaced) else None
            if sibling:
                groups.append(sibling)
            elif position == 0:
                # A lone class letter after a list delimiter belongs to the symbol before it.
                if groups and len(tokens) == 1 and len(_key(token)) == 1 and _letters(token) == 1:
                    groups[-1] += "-" + token
                else:
                    groups.append(token)
            elif _letters(token) < 2 or _letters(groups[-1]) < 2 or _is_suffix(token):
                groups[-1] += "-" + token
            elif spaced and len(os.path.commonprefix([_key(groups[-1]), _key(token)])) < 2:
                return []  # words of a name or a sentence, not class variants of one root
            else:
                groups.append(token)
    symbols: list[str] = []
    for group in groups:
        if _is_placeholder(group):
            continue
        ticker, _ = normalize_symbol(group)
        if ticker and ticker not in symbols:
            symbols.append(ticker)
    return symbols


@dataclass(frozen=True)
class InsiderFiling:
    accession: str
    cik: int
    raw_symbol: str
    normalized_symbols: tuple[str, ...]
    form: str
    filed: dt.date
    accepted: dt.datetime | None
    available_on: dt.date
    source: str
    source_package: str

    @property
    def fact_hash(self) -> str:
        values = [getattr(self, column) for column in FACT_COLUMNS]
        if self.accepted is not None:
            values[6] = self.accepted.astimezone(dt.timezone.utc).isoformat()
        payload = json.dumps(values, default=str, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def stage_row(self) -> tuple:
        return (self.accession, self.cik, self.raw_symbol, list(self.normalized_symbols),
                self.form, self.filed, self.accepted, self.source, self.source_package,
                self.fact_hash, self.available_on)


def package_name(path: Path, source: str) -> str:
    if source == "dera":
        return path.name
    dataset = next((part for part in path.parts if part in DATASETS), None)
    if dataset is None:
        raise ValueError("sec-api archives must be beneath a form-3/4/5-files directory")
    return f"{dataset}/{path.parent.name}/{path.name}"


def source_of(path: Path) -> str:
    if DERA_RE.fullmatch(path.name):
        return "dera"
    if any(part in DATASETS for part in path.parts):
        return "sec-api"
    raise ValueError(f"unrecognized insider package: {path.name}")


def parse_filed_date(value: str) -> dt.date:
    value = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return dt.date.fromisoformat(value)
    day, month, year = value.split("-")
    return dt.date(int(year), _MONTHS[month.upper()], int(day))


def _validate_filing(accession: str, cik: int, form: str) -> None:
    if not ACCESSION_RE.fullmatch(accession):
        raise ValueError(f"invalid accession {accession!r}")
    if not 0 < cik < 10**10:
        raise ValueError(f"invalid issuer CIK for {accession}")
    if form not in FORMS:
        raise ValueError(f"unexpected insider form {form!r} for {accession}")


def iter_dera_filings(path: Path, *, stats: Counter | None = None) -> Iterator[InsiderFiling]:
    """Stream only SUBMISSION.tsv; exhausting it verifies the member's CRC."""
    stats = stats if stats is not None else Counter()
    required = {"ACCESSION_NUMBER", "ISSUERCIK", "ISSUERTRADINGSYMBOL", "DOCUMENT_TYPE", "FILING_DATE"}
    with zipfile.ZipFile(path) as archive:
        members = [m for m in archive.infolist() if PurePosixPath(m.filename).name.upper() == "SUBMISSION.TSV"]
        if len(members) != 1:
            raise ValueError(f"{path.name}: expected one SUBMISSION.tsv, found {len(members)}")
        with archive.open(members[0]) as binary, io.TextIOWrapper(binary, encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream, delimiter="\t", quoting=csv.QUOTE_NONE)
            if not required.issubset(reader.fieldnames or []):
                raise ValueError(f"{path.name}: missing submission fields {sorted(required - set(reader.fieldnames or []))}")
            for line, row in enumerate(reader, 2):
                if any(row.get(k) is None for k in required):
                    raise ValueError(f"{path.name}: incomplete submission row {line}")
                accession = row["ACCESSION_NUMBER"].strip()
                cik, form = int(row["ISSUERCIK"]), row["DOCUMENT_TYPE"].strip().upper()
                _validate_filing(accession, cik, form)
                filed = parse_filed_date(row["FILING_DATE"])
                raw = row["ISSUERTRADINGSYMBOL"]
                symbols = tuple(normalize_symbols(raw))
                stats["filings"] += 1
                stats["symbol_facts"] += len(symbols)
                if not symbols:
                    stats["no_usable_symbol"] += 1
                yield InsiderFiling(accession, cik, raw, symbols, form, filed, None,
                                    filed + dt.timedelta(days=1), "dera", path.name)


def _xml_text(root: ET.Element, name: str, *, strip: bool = True) -> str:
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] == name:
            value = element.text or ""
            return value.strip() if strip else value
    return ""


def parse_ownership_xml(raw: bytes, metadata: dict, *, source_package: str,
                        accession: str | None = None) -> InsiderFiling | None:
    """Read actual ownership XML; an HTML rendering with .xml suffix is ignored."""
    if not re.search(rb"<(?:[A-Za-z_][\w.-]*:)?ownershipDocument(?:\s|>)", raw):
        return None
    if len(raw) > MAX_XML_BYTES:
        raise ValueError("ownership XML exceeds 16 MiB")
    if re.search(rb"<!\s*(?:DOCTYPE|ENTITY)\b", raw, flags=re.I):
        raise ValueError("ownership XML contains an unsupported DTD or entity declaration")
    root = ET.fromstring(raw)
    if root.tag.rsplit("}", 1)[-1] != "ownershipDocument":
        return None
    accession = accession or metadata.get("accessionNo") or metadata.get("accessionNumber")
    if not accession:
        raise ValueError("ownership XML metadata has no accession")
    form = _xml_text(root, "documentType").upper()
    metadata_form = str(metadata.get("formType", "")).upper()
    # EDGAR metadata supplies the actual submission form. Early XML sometimes
    # omits /A or incorrectly nests noSecuritiesOwned inside documentType
    # (real May 2003 accession 0001209191-03-003774). Neither changes the issuer.
    form = metadata_form or form
    cik = int(_xml_text(root, "issuerCik"))
    _validate_filing(accession, cik, form)
    timestamp = metadata.get("filedAt") or metadata.get("accepted")
    if not timestamp:
        raise ValueError(f"ownership XML metadata has no acceptance time for {accession}")
    accepted = dt.datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    if accepted.tzinfo is None:
        raise ValueError(f"ownership XML acceptance time lacks timezone for {accession}")
    filed = accepted.astimezone(NEW_YORK).date()
    symbol = _xml_text(root, "issuerTradingSymbol", strip=False)
    return InsiderFiling(accession, cik, symbol, tuple(normalize_symbols(symbol)), form,
                        filed, accepted, filed, "sec-api", source_package)


def _read_member(archive: zipfile.ZipFile, member: zipfile.ZipInfo, limit: int) -> bytes:
    if member.file_size > limit:
        raise ValueError(f"archive member too large: {member.filename}")
    with archive.open(member) as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError(f"archive member exceeds limit: {member.filename}")
    return data


def iter_secapi_filings(path: Path, *, stats: Counter | None = None,
                        metadata_only: list[str] | None = None) -> Iterator[InsiderFiling]:
    """One observation per accession, ignoring duplicate HTML/XSL renderings.

    Accessions with metadata but no ownership XML yield no fact; they are added
    to ``metadata_only`` so the package still records them as members.
    """
    stats = stats if stats is not None else Counter()
    package = package_name(path, "sec-api")
    with zipfile.ZipFile(path) as archive:
        by_directory: dict[str, list[zipfile.ZipInfo]] = {}
        metadata_members: list[zipfile.ZipInfo] = []
        for member in archive.infolist():
            name = PurePosixPath(member.filename)
            if name.name == "metadata.json":
                metadata_members.append(member)
            elif name.suffix.lower() == ".xml" and not any(p.lower().startswith("xslf345") for p in name.parts):
                by_directory.setdefault(str(name.parent), []).append(member)
        if not metadata_members:
            raise ValueError(f"{package}: no filing metadata found")
        for member in metadata_members:
            metadata = json.loads(_read_member(archive, member, MAX_XML_BYTES))
            directory = str(PurePosixPath(member.filename).parent)
            found: InsiderFiling | None = None
            for xml_member in by_directory.get(directory, []):
                filing = parse_ownership_xml(_read_member(archive, xml_member, MAX_XML_BYTES), metadata,
                                             source_package=package)
                if filing is None:
                    continue
                if found and found.fact_hash != filing.fact_hash:
                    raise ValueError(f"{package}: conflicting ownership XML for {filing.accession}")
                found = filing
            stats["metadata_filings"] += 1
            if found is None:
                accession = str(metadata.get("accessionNo") or metadata.get("accessionNumber") or "")
                if not ACCESSION_RE.fullmatch(accession):
                    raise ValueError(f"{package}: filing metadata without a valid accession")
                if metadata_only is not None:
                    metadata_only.append(accession)
                stats["non_xml_filings"] += 1
                continue
            stats["filings"] += 1
            stats["symbol_facts"] += len(found.normalized_symbols)
            if not found.normalized_symbols:
                stats["no_usable_symbol"] += 1
            yield found


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_package(conn, path: Path, *, source: str | None = None,
                 reconciled_on: dt.date | None = None, validators: dict | None = None) -> dict:
    """Atomically stage, validate and reconcile one package, keeping old versions."""
    source = source or source_of(path)
    package = package_name(path, source)
    digest, size = sha256_file(path), path.stat().st_size
    on = reconciled_on or dt.datetime.now(dt.timezone.utc).date()
    validators = validators or {}
    stats: Counter = Counter()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(900, 345)")
            cur.execute("SELECT package_sha256, parser_version FROM sec_insider_packages WHERE source_package = %s", (package,))
            previous = cur.fetchone()
            if previous == (digest, PARSER_VERSION):
                cur.execute("UPDATE sec_insider_packages SET remote_etag = COALESCE(%s, remote_etag), remote_last_modified = COALESCE(%s, remote_last_modified) WHERE source_package = %s",
                            (validators.get("etag"), validators.get("last_modified") or validators.get("updatedAt"), package))
                return {"package": package, "skipped": "unchanged", "sha256": digest}
            cur.execute("CREATE TEMP TABLE tmp_insider_stage (accession text PRIMARY KEY, cik bigint NOT NULL, raw_symbol text NOT NULL, normalized_symbols text[] NOT NULL, form text NOT NULL, filed date NOT NULL, accepted timestamptz, source text NOT NULL, source_package text NOT NULL, fact_hash text NOT NULL, source_available_on date NOT NULL) ON COMMIT DROP")
            metadata_only: list[str] = []
            iterator = (iter_dera_filings(path, stats=stats) if source == "dera"
                        else iter_secapi_filings(path, stats=stats, metadata_only=metadata_only))
            with cur.copy(f"COPY tmp_insider_stage ({', '.join(STAGE_COLUMNS)}) FROM STDIN") as copy:
                for filing in iterator:
                    copy.write_row(filing.stage_row())
            if not stats["filings"]:
                raise ValueError(f"{package}: no structured insider filings found")
            # Members include accessions that yielded no fact (metadata without
            # ownership XML), so a fact learned later is dated by reconciliation.
            cur.execute("CREATE TEMP TABLE tmp_insider_members (accession text PRIMARY KEY) ON COMMIT DROP")
            with cur.copy("COPY tmp_insider_members (accession) FROM STDIN") as copy:
                for accession in sorted(set(metadata_only)):
                    copy.write_row((accession,))
            cur.execute("INSERT INTO tmp_insider_members SELECT accession FROM tmp_insider_stage ON CONFLICT DO NOTHING")
            cur.execute("CREATE TEMP TABLE tmp_insider_old ON COMMIT DROP AS SELECT fact_hash FROM sec_insider_package_facts WHERE source_package = %s AND retired_on IS NULL", (package,))
            # Retire only carriers that disappeared from this fully staged revision.
            cur.execute("UPDATE sec_insider_package_facts p SET retired_on = %s WHERE p.source_package = %s AND p.retired_on IS NULL AND NOT EXISTS (SELECT 1 FROM tmp_insider_stage s WHERE s.fact_hash = p.fact_hash)", (on, package))
            cur.execute("INSERT INTO sec_insider_package_facts (source_package, fact_hash, loaded_on) SELECT %s, s.fact_hash, %s FROM tmp_insider_stage s WHERE NOT EXISTS (SELECT 1 FROM sec_insider_package_facts p WHERE p.source_package = %s AND p.fact_hash = s.fact_hash AND p.retired_on IS NULL)", (package, on, package))
            cur.execute("UPDATE sec_insider_filings f SET retired_on = %s WHERE f.retired_on IS NULL AND EXISTS (SELECT 1 FROM tmp_insider_old o WHERE o.fact_hash = f.fact_hash) AND NOT EXISTS (SELECT 1 FROM sec_insider_package_facts p WHERE p.fact_hash = f.fact_hash AND p.retired_on IS NULL)", (on,))
            retired = cur.rowcount
            # The accession history is checked before new package memberships are inserted.
            cur.execute(f"INSERT INTO sec_insider_filings ({', '.join(FACT_COLUMNS)}, source_package, fact_hash, source_version, available_on, loaded_on) SELECT {', '.join('s.' + c for c in FACT_COLUMNS)}, s.source_package, s.fact_hash, %s, CASE WHEN EXISTS (SELECT 1 FROM sec_insider_package_members m WHERE m.accession = s.accession) THEN GREATEST(s.source_available_on, %s) ELSE s.source_available_on END, %s FROM tmp_insider_stage s WHERE NOT EXISTS (SELECT 1 FROM sec_insider_filings f WHERE f.fact_hash = s.fact_hash AND f.retired_on IS NULL)", (digest, on, on))
            inserted = cur.rowcount
            cur.execute("UPDATE sec_insider_package_members m SET retired_on = %s WHERE m.source_package = %s AND m.retired_on IS NULL AND NOT EXISTS (SELECT 1 FROM tmp_insider_members s WHERE s.accession = m.accession)", (on, package))
            cur.execute("INSERT INTO sec_insider_package_members (source_package, accession, loaded_on) SELECT %s, s.accession, %s FROM tmp_insider_members s WHERE NOT EXISTS (SELECT 1 FROM sec_insider_package_members m WHERE m.source_package = %s AND m.accession = s.accession AND m.retired_on IS NULL)", (package, on, package))
            rejected = {key: value for key, value in stats.items() if key not in ("filings", "symbol_facts", "metadata_filings")}
            cur.execute("INSERT INTO sec_insider_packages (source_package, source, source_version, package_sha256, package_bytes, parser_version, filings, rejected, remote_etag, remote_last_modified) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s) ON CONFLICT (source_package) DO UPDATE SET source = EXCLUDED.source, source_version = EXCLUDED.source_version, package_sha256 = EXCLUDED.package_sha256, package_bytes = EXCLUDED.package_bytes, parser_version = EXCLUDED.parser_version, filings = EXCLUDED.filings, rejected = EXCLUDED.rejected, loaded_at = now(), remote_etag = EXCLUDED.remote_etag, remote_last_modified = EXCLUDED.remote_last_modified",
                        (package, source, digest, digest, size, PARSER_VERSION, stats["filings"], json.dumps(rejected), validators.get("etag"), validators.get("last_modified") or validators.get("updatedAt")))
            # A caller may wrap several packages in its own transaction. ON
            # COMMIT DROP alone would retain these names until that outer commit.
            cur.execute("DROP TABLE tmp_insider_stage, tmp_insider_old, tmp_insider_members")
    return {"package": package, "sha256": digest, **dict(stats), "inserted": inserted, "retired": retired}


class _SecApiRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parsed = urllib.parse.urlsplit(newurl)
        if parsed.hostname != "api.sec-api.io" or parsed.scheme != "https":
            raise ValueError("refusing to forward sec-api credentials through a cross-origin redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class HttpClient:
    """Sequential, rate-limited HTTP with bounded retries and safe error output."""

    def __init__(self, api_key: str | None = None, *, spacing: float = 0.3):
        self.api_key, self.spacing, self.last_request = api_key, max(0.11, spacing), 0.0
        if api_key:
            _SECRETS.add(api_key)

    def request(self, url: str, *, method: str = "GET", authenticated: bool = False,
                retry_statuses: frozenset[int] = frozenset()):
        """429 and 5xx are retried; ``retry_statuses`` adds provider-specific ones."""
        parsed = urllib.parse.urlsplit(url)
        if authenticated and (parsed.hostname != "api.sec-api.io" or parsed.scheme != "https"):
            raise ValueError("refusing to send sec-api credentials to another host")
        for attempt in range(4):
            spacing = max(0.5, self.spacing) if urllib.parse.urlsplit(url).hostname == "api.sec-api.io" else self.spacing
            time.sleep(max(0.0, spacing - (time.monotonic() - self.last_request)))
            headers = {"User-Agent": USER_AGENT}
            if authenticated:
                if not self.api_key:
                    raise ValueError("sec-api download requires an API key")
                headers["Authorization"] = self.api_key
            request = urllib.request.Request(url, headers=headers, method=method)
            self.last_request = time.monotonic()
            try:
                if authenticated:
                    return urllib.request.build_opener(_SecApiRedirect()).open(request, timeout=90)
                return urllib.request.urlopen(request, timeout=90)
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
                status = getattr(exc, "code", None)
                if attempt == 3 or (status and status != 429 and status < 500 and status not in retry_statuses):
                    raise RuntimeError(f"HTTP request failed for {urllib.parse.urlsplit(url).path}: {scrub(exc)}") from None
                time.sleep(min(2**attempt, 10))
        raise AssertionError("unreachable")

    def json(self, url: str) -> dict:
        with self.request(url) as response:
            return json.loads(_http_body(response))

    def download(self, url: str, path: Path, *, expected_size: int | None = None,
                 authenticated: bool = False) -> dict:
        path.parent.mkdir(parents=True, exist_ok=True)
        # sec-api's Bulk Datasets API answers 400 when streaming an archive fails
        # and documents it as retryable; SEC and DERA keep the default policy.
        retry = SECAPI_STREAM_RETRY if authenticated else frozenset()
        with tempfile.TemporaryDirectory(dir=path.parent, prefix=".insider-download-") as directory:
            staged = Path(directory) / "package.zip"
            with self.request(url, authenticated=authenticated, retry_statuses=retry) as response, staged.open("wb") as output:
                headers = dict(response.headers)
                for chunk in iter(lambda: response.read(1024 * 1024), b""):
                    output.write(chunk)
                expected = expected_size or int(response.headers.get("Content-Length", "0")) or None
            if expected is not None and staged.stat().st_size != expected:
                raise ValueError(f"{path.name}: short download ({staged.stat().st_size} != {expected})")
            with zipfile.ZipFile(staged):
                pass  # check the central directory; selected-member CRC is verified while parsing
            digest = sha256_file(staged)
            staged.replace(path)
        return {"sha256": digest, "size": path.stat().st_size,
                "etag": headers.get("ETag"), "last_modified": headers.get("Last-Modified")}


def _meta_path(path: Path) -> Path:
    return path.with_name(path.name + ".meta.json")


def _http_body(response) -> bytes:
    """Decode HTTP content encoding for listings; ZIP downloads remain raw."""
    data = response.read()
    encodings = [value.strip().lower() for value in response.headers.get("Content-Encoding", "").split(",") if value.strip()]
    for encoding in reversed(encodings):
        if encoding in {"gzip", "x-gzip"}:
            data = gzip.decompress(data)
        elif encoding != "identity":
            raise ValueError(f"unsupported HTTP listing content encoding: {encoding}")
    return data


def _read_meta(path: Path) -> dict:
    try:
        value = json.loads(_meta_path(path).read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_meta(path: Path, value: dict) -> None:
    target = _meta_path(path)
    staged = target.with_suffix(target.suffix + ".tmp")
    staged.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    staged.replace(target)


def listed_dera_urls(text: str) -> list[str]:
    urls = []
    for href in re.findall(r'''href\s*=\s*["']([^"']+)["']''', text, flags=re.I):
        url = urllib.parse.urljoin("https://www.sec.gov", html.unescape(href))
        parsed = urllib.parse.urlsplit(url)
        if parsed.hostname in {"www.sec.gov", "sec.gov"} and DERA_RE.fullmatch(PurePosixPath(parsed.path).name):
            urls.append(url)
    if not urls:
        raise ValueError("DERA insider listing contains no quarterly packages")
    return sorted(set(urls))


def sync_dera(out_dir: Path, *, verify_cache: bool = False, client: HttpClient | None = None) -> list[Path]:
    client = client or HttpClient()
    out_dir.mkdir(parents=True, exist_ok=True)
    with client.request(DERA_LISTING_URL) as response:
        urls = listed_dera_urls(_http_body(response).decode("utf-8"))
    paths = []
    for url in urls:
        name = PurePosixPath(urllib.parse.urlsplit(url).path).name
        path, fetched = out_dir / name, False
        meta = _read_meta(path)
        valid = path.is_file()
        if valid and meta.get("sha256"):
            valid = sha256_file(path) == meta["sha256"]
        headers: dict = {}
        if verify_cache:
            with client.request(url, method="HEAD") as response:
                headers = {"etag": response.headers.get("ETag"), "last_modified": response.headers.get("Last-Modified"),
                           "size": int(response.headers.get("Content-Length", "0")) or None}
            if valid and headers["size"] is not None and path.stat().st_size != headers["size"]:
                valid = False
            if valid and headers["etag"] and meta.get("etag") != headers["etag"]:
                valid = False
            if valid and headers["last_modified"] and meta.get("last_modified") != headers["last_modified"]:
                valid = False
            # A copied file without a recorded revision, or a remote server
            # without validators, cannot prove that same-size bytes are current.
            if valid and (not meta.get("sha256") or not (headers["etag"] or headers["last_modified"])):
                valid = False
            if valid and headers["last_modified"]:
                modified = parsedate_to_datetime(headers["last_modified"])
                modified = modified.replace(tzinfo=dt.timezone.utc) if modified.tzinfo is None else modified
                valid = modified.timestamp() <= path.stat().st_mtime
            if valid and meta.get("sha256"):
                valid = sha256_file(path) == meta["sha256"]
        if not valid:
            meta = client.download(url, path, expected_size=headers.get("size"))
            fetched = True
        else:
            meta = {**meta, **headers, "size": path.stat().st_size, "sha256": sha256_file(path)}
        _write_meta(path, {**meta, "source_url": url})
        paths.append(path)
        log({"download": name, "fetched": fetched, "bytes": path.stat().st_size})
    return paths


def months_between(first: str, last: str) -> list[str]:
    def index(month: str) -> int:
        date = dt.date.fromisoformat(month + "-01")
        return date.year * 12 + date.month - 1
    start, end = index(first), index(last)
    if start > end:
        raise ValueError("reversed sec-api month window")
    return [f"{n // 12:04d}-{n % 12 + 1:02d}" for n in range(start, end + 1)]


def load_api_key(dotenv: Path = DEFAULT_DOTENV) -> str:
    names = ("SEC_API_IO_KEY", "SEC_API_KEY", "SEC_API_IO_API_KEY")
    for name in names:
        if os.environ.get(name, "").strip():
            value = os.environ[name].strip()
            _SECRETS.add(value)
            return value
    if dotenv.is_file():
        for line in dotenv.read_text(encoding="utf-8").splitlines():
            name, delimiter, value = line.strip().partition("=")
            if delimiter and name.strip() in names:
                value = value.strip().strip("'\"")
                if value:
                    _SECRETS.add(value)
                    return value
    raise ValueError("no sec-api API key in environment or configured dotenv")


def sync_secapi(out_dir: Path, *, first: str = "2003-05", last: str = "2005-12",
                api_key: str | None = None, client: HttpClient | None = None) -> list[Path]:
    """Discover the complete bulk catalogue and fetch each requested monthly page.

    The documented dataset index has a complete containers array, with no
    pagination token. Monthly ZIPs are the pages of the historical archive.
    updatedAt + size + local SHA256 detect same-size republication/corruption.
    """
    requested = set(months_between(first, last))
    client = client or HttpClient(api_key)
    result = []
    for dataset in DATASETS:
        detail = client.json(f"{SECAPI_BASE}/datasets/{dataset}.json")
        containers = []
        for container in detail.get("containers", []):
            key = container.get("key", "")
            match = re.fullmatch(r"(\d{4})/(\d{4}-\d{2})\.zip", key)
            if match and match.group(2) in requested:
                if match.group(1) != match.group(2)[:4]:
                    raise ValueError(f"{dataset}: inconsistent container year")
                containers.append(container)
        present_months = {PurePosixPath(c["key"]).stem for c in containers}
        if requested != present_months or len(containers) != len(requested):
            raise ValueError(f"{dataset}: incomplete or duplicate monthly catalogue ({sorted(requested - present_months)})")
        for container in sorted(containers, key=lambda c: c["key"]):
            path = out_dir / dataset / container["key"]
            size, updated = container.get("size"), container.get("updatedAt")
            meta = _read_meta(path)
            cached = path.is_file() and size is not None and bool(updated) and path.stat().st_size == size and meta.get("updatedAt") == updated and bool(meta.get("sha256"))
            if cached:
                cached = sha256_file(path) == meta["sha256"]
            if not cached:
                url = container.get("downloadUrl") or f"{SECAPI_BASE}/datasets/{dataset}/{container['key']}"
                # Credentials use headers, never URL query parameters.
                url = urllib.parse.urlunsplit(urllib.parse.urlsplit(url)._replace(query=""))
                meta = client.download(url, path, expected_size=size, authenticated=True)
                _write_meta(path, {**meta, "updatedAt": updated, "records": container.get("records")})
            result.append(path)
            log({"download": f"{dataset}/{container['key']}", "fetched": not cached, "bytes": path.stat().st_size})
    return result


def discover_packages(dera_dir: Path, secapi_dir: Path) -> list[Path]:
    dera = sorted((p for p in dera_dir.glob("*.zip") if DERA_RE.fullmatch(p.name)), key=lambda p: p.name)
    secapi = sorted(p for dataset in DATASETS for p in (secapi_dir / dataset).glob("*/*.zip")
                    if re.fullmatch(r"\d{4}-\d{2}\.zip", p.name))
    return secapi + dera


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("packages", nargs="*", type=Path)
    parser.add_argument("--packages-dir", type=Path, default=DEFAULT_ROOT / "dera")
    parser.add_argument("--secapi-dir", type=Path, default=DEFAULT_ROOT / "secapi")
    parser.add_argument("--download-dera", action="store_true")
    parser.add_argument("--verify-cache", action="store_true", help="verify every DERA cached package against current remote validators")
    parser.add_argument("--download-secapi", action="store_true")
    parser.add_argument("--from", dest="first", default="2003-05")
    parser.add_argument("--to", dest="last", default="2005-12")
    parser.add_argument("--dotenv", type=Path, default=DEFAULT_DOTENV)
    parser.add_argument("--dsn", default=None, help="connection DSN; default libpq PG* environment")
    parser.add_argument("--dry-run", action="store_true", help="parse all selected packages without connecting to a database")
    parser.add_argument("--download-only", action="store_true")
    parser.add_argument("--apply-schema", action="store_true")
    parser.add_argument("--reconciled-on", type=dt.date.fromisoformat, default=None)
    args = parser.parse_args(argv)
    try:
        selected_remote: list[Path] = []
        if args.download_dera or args.verify_cache:
            selected_remote += sync_dera(args.packages_dir, verify_cache=args.verify_cache)
        if args.download_secapi:
            selected_remote += sync_secapi(args.secapi_dir, first=args.first, last=args.last, api_key=load_api_key(args.dotenv))
        if args.download_only:
            return 0
        paths = args.packages or discover_packages(args.packages_dir, args.secapi_dir)
        if args.verify_cache:
            # A ZIP removed from the remote catalogue is not freshly verified.
            paths = [p for p in paths if source_of(p) != "dera" or p in selected_remote]
        if args.download_secapi and not args.packages:
            # The requested month window bounds the load, not only the download:
            # a persistent cache may hold archives outside it.
            window = {p.resolve() for p in selected_remote}
            paths = [p for p in paths if source_of(p) != "sec-api" or p.resolve() in window]
        if not paths:
            parser.error("no insider packages selected")
        if args.dry_run:
            for path in paths:
                stats: Counter = Counter()
                iterator = iter_dera_filings(path, stats=stats) if source_of(path) == "dera" else iter_secapi_filings(path, stats=stats)
                for _ in iterator:
                    pass
                log({"package": package_name(path, source_of(path)), **dict(stats)})
            return 0
        import psycopg
        with psycopg.connect(args.dsn or "", autocommit=True) as conn:
            if args.apply_schema:
                conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
            row = conn.execute("SELECT to_regclass('sec_insider_filings'), to_regclass('sec_insider_packages'), to_regclass('sec_insider_package_facts'), to_regclass('sec_insider_package_members')").fetchone()
            if not all(row):
                raise ValueError("insider schema is missing; apply it as the database owner")
            for path in paths:
                log(load_package(conn, path, reconciled_on=args.reconciled_on, validators=_read_meta(path)))
        return 0
    except Exception as exc:
        print(scrub(f"insider load failed: {exc}"), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
