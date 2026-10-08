"""Load the SEC cover-page ticker -> (CIK, class) history from public SEC data.

Sources (both public, fetched with the SEC User-Agent, one request at a time):

* DERA "Financial Statement and Notes" packages (quarterly ``YYYYqN_notes.zip``,
  later monthly ``YYYY_MM_notes.zip``). Four members are streamed from the zip
  without extraction: ``sub.tsv`` (adsh, cik, form, period, filed, accepted),
  ``txt.tsv`` (the ``dei:TradingSymbol`` / ``dei:Security12bTitle`` /
  ``dei:SecurityExchangeName`` facts), ``num.tsv`` (``dei:EntityCommonStockSharesOutstanding``)
  and ``dim.tsv`` (the dimension segments of those facts' contexts). Reading a
  member to its end checks its CRC, so a corrupt package fails the run instead
  of loading partial data.
* EDGAR full-index form indexes (``full-index/YYYY/QTRn/form.gz``): Forms
  15-12B, 15-12G, 15-15D, 25 and 25-NSE, the deregistration/delisting events
  that end an issuer's hold on its symbols.

Rules (schemas/sec_ticker_cik_history_v1.sql documents the tables):

* every form that carries a tagged cover page is kept (10-K/10-Q/8-K/20-F/40-F,
  amendments and others); ``form`` is stored;
* a fact's security LINE is its class: the context's dimension segments
  without the listing-exchange and legal-entity axes (``ClassOfStock=CommonClassA;``),
  '' without dimensions. Symbols and share counts of the same class join on it;
* facts with a co-registrant (``coreg``) belong to that entity, not to
  ``sub.cik``: skipped and counted;
* the knowledge date comes from ``sub.tsv`` (``accepted``, else ``filed`` + 1);
  the fact's ``ddate`` is stored but never dates a symbol statement;
* symbols are normalized to the eod_prices / universe_constituents style
  (``BRK.B`` -> ``BRK-B``, ``USB PrA`` -> ``USB-PA``); ``ticker_raw`` keeps the
  filer's spelling; placeholders (``None``, ``N/A``, ``true``...) are rejected
  and counted; each line gets a ``security_kind`` (equity, depositary,
  preferred, debt, warrant, unit, right) from its title, else its symbol.

Writes are idempotent upserts, one transaction per package; rows of a
package's submissions that the current rules no longer produce are removed, so
a re-run converges. ``--dry-run`` parses and reports without a database. The
schema is governed: it is applied only with ``--apply-schema``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import io
import json
import re
import time
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import IO, Iterable

from src.db import LOCK_SEC_TICKER_CIK_HISTORY, connect

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PACKAGES_DIR = Path("E:/Edgard/fsn")
DEFAULT_INDEX_DIR = Path("E:/Edgard/edgar-index")
LISTING_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets"
)
SEC_BASE_URL = "https://www.sec.gov"
EDGAR_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/form.gz"
FIRST_INDEX_QUARTER = (2009, 1)  # the first FSN package
USER_AGENT = "InvestIntell-SEP-Ingestion/1.0 (+https://hub.investintell.com)"
# SEC fair access allows 10 requests/s; downloads run one at a time, spaced.
DOWNLOAD_SPACING_S = 0.5
SCHEMA_PATH = ROOT / "schemas" / "sec_ticker_cik_history_v1.sql"

SYMBOL_TAG = "TradingSymbol"
TITLE_TAG = "Security12bTitle"
EXCHANGE_TAG = "SecurityExchangeName"
SHARES_TAG = "EntityCommonStockSharesOutstanding"
COVER_TAGS = frozenset({SYMBOL_TAG, TITLE_TAG, EXCHANGE_TAG})
# Byte prefilters: the tag is the second tab-separated field of txt/num rows.
_COVER_TAG_MARKERS = tuple(f"\t{tag}\t".encode() for tag in COVER_TAGS)
_SHARES_MARKER = f"\t{SHARES_TAG}\t".encode()
NO_DIMENSIONS = "0x00000000"
# Axes that qualify where/for whom a class is reported, not which class it is.
NON_CLASS_AXES = frozenset({"EntityListingsExchange", "LegalEntity"})
EVENT_FORMS = ("15-12B", "15-12G", "15-15D", "25", "25-NSE")

PACKAGE_RE = re.compile(
    r"^(?P<year>\d{4})(?:q(?P<quarter>[1-4])|_(?P<month>\d{2}))_notes(?:_\d+)?\.zip$"
)
INDEX_RE = re.compile(r"^(?P<year>\d{4})QTR(?P<quarter>[1-4])\.form\.gz$")
_INDEX_LINE_RE = re.compile(
    r"^(?P<form>\S+)\s+.*?\s(?P<cik>\d+)\s+(?P<filed>\d{4}-\d{2}-\d{2})\s+"
    r"edgar/data/\d+/(?P<adsh>\d{10}-\d{2}-\d{6})\.txt\s*$"
)

OBSERVATION_COLUMNS = (
    "adsh", "cik", "dimh", "segments", "class_key", "ticker", "ticker_raw",
    "security_title", "exchange", "security_kind", "ddate", "form", "period",
    "filed", "accepted", "source_package",
)
SHARE_COLUMNS = (
    "adsh", "cik", "dimh", "segments", "class_key", "ddate", "shares", "form",
    "filed", "accepted", "source_package",
)
EVENT_COLUMNS = ("adsh", "cik", "form", "filed", "source_package")

# Values filers put in dei:TradingSymbol when a security has no symbol.
PLACEHOLDER_KEYS = frozenset({
    "NONE", "NA", "NOTAPPLICABLE", "NOTAVAILABLE", "TRUE", "FALSE", "NULL", "NIL",
    "NOSYMBOL", "NOTRADINGSYMBOL", "NOTLISTED", "NOTTRADED", "UNLISTED", "TBD",
})
MAX_KEY_LENGTH = 12
_EXCHANGE_PREFIX_RE = re.compile(
    r"^(?:NYSE\s*AMERICAN|NYSE\s*ARCA|NYSE\s*MKT|NYSE|NASDAQ|AMEX|OTCQX|OTCQB|OTC|CBOE|TSX)"
    r"\s*[:\-]\s*",
    re.IGNORECASE,
)
# Preferred series: an uppercase root, a preferred marker, one series letter.
# A lowercase marker (``Pr``, ``pr``, ``p``) is unambiguous even without a
# separator (PSAPrM, CDRpB); an uppercase marker needs one (WFC.PRA, USB PRA).
_PREFERRED_RE = re.compile(
    r"^(?P<root>[A-Z0-9]+)(?P<sep>\s*[.\-/]?\s*)(?P<marker>Pr|pr|p|PR|P)"
    r"\s*[.\-/]?\s*(?P<series>[A-Za-z])$"
)
_SEPARATORS_RE = re.compile(r"[\s./\-_:]+")
_TICKER_RE = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)*$")

# Security kind from the Security12bTitle, checked in this order. A rights
# plan attached to common stock ("... and associated Preferred Stock Purchase
# Rights") is removed first so it does not read as preferred or rights.
_ATTACHED_RIGHTS_RE = re.compile(
    r"(?:,?\s*(?:together\s+with|and|including|with)\s+(?:the\s+)?(?:associated|attached)?"
    r".*?purchase\s+rights?.*$)",
    re.IGNORECASE,
)
_KIND_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("debt", re.compile(
        r"\bnotes?\b|debentures?|\bbonds?\b|\bdue\s+(?:19|20)\d\d\b|senior\s+(?:un)?secured"
        r"|subordinated|medium[-\s]term|\bloan\b", re.IGNORECASE)),
    ("preferred", re.compile(r"preferred|preference|\bperpetual\b", re.IGNORECASE)),
    ("unit", re.compile(r"^\s*units?\b|\bunits?,?\s+each\b|\beach\s+unit\b", re.IGNORECASE)),
    ("warrant", re.compile(r"warrant", re.IGNORECASE)),
    ("right", re.compile(r"^\s*rights?\b|\brights?,?\s+each\b|\bcontingent\s+value\b",
                         re.IGNORECASE)),
    ("depositary", re.compile(
        r"american\s+depositary|depositary\s+(?:shares|receipts)|\bADSs?\b|\bADRs?\b",
        re.IGNORECASE)),
)
_SEGMENT_KIND_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("debt", re.compile(r"Debt|Notes?(?:Due|\b)|Debenture|Bond", re.IGNORECASE)),
    ("preferred", re.compile(r"Preferred", re.IGNORECASE)),
    ("warrant", re.compile(r"Warrant", re.IGNORECASE)),
    ("unit", re.compile(r"=Units?Member|CapitalUnits", re.IGNORECASE)),
    ("depositary", re.compile(r"Depositary|\bADS", re.IGNORECASE)),
)


@dataclass(frozen=True)
class Submission:
    cik: int
    form: str
    period: dt.date | None
    filed: dt.date
    accepted: dt.datetime | None


@dataclass(frozen=True)
class Observation:
    adsh: str
    cik: int
    dimh: str
    segments: str
    class_key: str
    ticker: str
    ticker_raw: str
    security_title: str | None
    exchange: str | None
    security_kind: str
    ddate: dt.date | None
    form: str
    period: dt.date | None
    filed: dt.date
    accepted: dt.datetime | None
    source_package: str

    def as_tuple(self) -> tuple:
        return tuple(getattr(self, column) for column in OBSERVATION_COLUMNS)


@dataclass(frozen=True)
class ShareCount:
    adsh: str
    cik: int
    dimh: str
    segments: str
    class_key: str
    ddate: dt.date
    shares: Decimal
    form: str
    filed: dt.date
    accepted: dt.datetime | None
    source_package: str

    def as_tuple(self) -> tuple:
        return tuple(getattr(self, column) for column in SHARE_COLUMNS)


@dataclass(frozen=True)
class RegistrationEvent:
    adsh: str
    cik: int
    form: str
    filed: dt.date
    source_package: str

    def as_tuple(self) -> tuple:
        return tuple(getattr(self, column) for column in EVENT_COLUMNS)


@dataclass
class PackageResult:
    package: str
    sha256: str
    size_bytes: int
    submissions: dict[str, Submission]
    observations: list[Observation]
    share_counts: list[ShareCount]
    symbol_facts: int
    rejected: Counter = field(default_factory=Counter)
    parse_seconds: float = 0.0

    def stats(self) -> dict[str, object]:
        return {
            "package": self.package,
            "submissions": len(self.submissions),
            "symbol_facts": self.symbol_facts,
            "observations": len(self.observations),
            "share_counts": len(self.share_counts),
            "distinct_tickers": len({o.ticker for o in self.observations}),
            "distinct_ciks": len({o.cik for o in self.observations}),
            "kinds": dict(Counter(o.security_kind for o in self.observations).most_common()),
            "forms": dict(Counter(o.form for o in self.observations).most_common(6)),
            "rejected": dict(sorted(self.rejected.items())),
            "parse_seconds": round(self.parse_seconds, 1),
        }


# --------------------------------------------------------------------------- #
# Normalization
# --------------------------------------------------------------------------- #
def ticker_key(ticker: str) -> str:
    """The resolver's match key: uppercase alphanumerics only."""
    return re.sub(r"[^A-Z0-9]", "", ticker.upper())


def normalize_symbol(raw: str) -> tuple[str | None, str | None]:
    """One filer-typed symbol -> (ticker, None) or (None, rejection reason)."""
    value = re.sub(r"\([^)]*\)", " ", raw).strip()
    value = _EXCHANGE_PREFIX_RE.sub("", value).strip()
    if not value:
        return None, "empty"
    preferred = _PREFERRED_RE.match(value)
    if preferred and (
        preferred.group("marker") in ("Pr", "pr", "p") or preferred.group("sep")
    ):
        ticker = f"{preferred.group('root')}-P{preferred.group('series').upper()}"
    else:
        ticker = _SEPARATORS_RE.sub("-", value.upper()).strip("-")
    key = ticker_key(ticker)
    if key in PLACEHOLDER_KEYS:
        return None, "placeholder"
    if not _TICKER_RE.match(ticker):
        return None, "malformed"
    if len(key) > MAX_KEY_LENGTH:
        return None, "too_long"
    return ticker, None


def normalize_symbols(raw: str) -> tuple[list[str], list[str]]:
    """A dei:TradingSymbol value may list several symbols (``BIP; BIP UN``)."""
    tickers: list[str] = []
    rejections: list[str] = []
    for part in re.split(r"[;,]", raw):
        ticker, reason = normalize_symbol(part)
        if ticker is not None:
            if ticker not in tickers:
                tickers.append(ticker)
        elif reason != "empty" or not tickers:
            rejections.append(reason or "malformed")
    return tickers, rejections


def class_key(segments: str) -> str:
    """The class part of a context's segments, in a stable order."""
    parts = [part for part in segments.split(";") if part]
    kept = sorted(part for part in parts if part.split("=", 1)[0] not in NON_CLASS_AXES)
    return "".join(f"{part};" for part in kept)


def security_kind(title: str | None, ticker: str, segments: str) -> str:
    """What a cover line is: from its title, else its segments, else its symbol."""
    if title:
        text = _ATTACHED_RIGHTS_RE.sub("", title)
        for kind, pattern in _KIND_RULES:
            if pattern.search(text):
                return kind
        return "equity"
    for kind, pattern in _SEGMENT_KIND_RULES:
        if pattern.search(segments):
            return kind
    if re.search(r"-P[A-Z]$", ticker):
        return "preferred"
    if re.search(r"-(?:WS|WT|W)$", ticker):
        return "warrant"
    if re.search(r"-(?:U|UN)$", ticker):
        return "unit"
    if re.search(r"-(?:R|RT)$", ticker):
        return "right"
    if re.search(r"\d", ticker):
        return "debt"
    return "equity"


# --------------------------------------------------------------------------- #
# FSN package parsing
# --------------------------------------------------------------------------- #
def package_sort_key(path: Path) -> tuple[int, int, str]:
    match = PACKAGE_RE.match(path.name)
    if match is None:
        raise ValueError(f"not a DERA FSN package name: {path.name}")
    year = int(match.group("year"))
    month = (
        3 * (int(match.group("quarter")) - 1) + 1
        if match.group("quarter")
        else int(match.group("month"))
    )
    return year, month, path.name


def parse_fsn_date(value: str) -> dt.date | None:
    value = value.strip()
    if not value:
        return None
    return dt.datetime.strptime(value, "%Y%m%d").date()


def parse_accepted(value: str) -> dt.datetime | None:
    """EDGAR acceptance datetime (America/New_York wall clock, no zone)."""
    value = value.strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"unparseable accepted datetime: {value!r}")


def _split(raw: bytes) -> list[str]:
    return raw.decode("utf-8", "replace").rstrip("\r\n").split("\t")


def _header(stream: IO[bytes]) -> dict[str, int]:
    return {name: i for i, name in enumerate(_split(stream.readline()))}


def read_submissions(stream: IO[bytes]) -> dict[str, Submission]:
    index = _header(stream)
    submissions: dict[str, Submission] = {}
    for raw in stream:
        fields = _split(raw)
        if len(fields) != len(index):
            raise ValueError(f"sub.tsv row has {len(fields)} fields, expected {len(index)}")
        filed = parse_fsn_date(fields[index["filed"]])
        cik = int(fields[index["cik"]] or 0)
        if filed is None or cik <= 0:
            continue
        submissions[fields[index["adsh"]]] = Submission(
            cik=cik,
            form=fields[index["form"]].strip(),
            period=parse_fsn_date(fields[index["period"]]),
            filed=filed,
            accepted=parse_accepted(fields[index["accepted"]]) if "accepted" in index else None,
        )
    return submissions


@dataclass
class _CoverFacts:
    symbols: list[tuple[int, str, str]] = field(default_factory=list)  # iprx, value, ddate
    titles: list[tuple[int, str]] = field(default_factory=list)
    exchanges: list[tuple[int, str]] = field(default_factory=list)


def read_cover_facts(
    stream: IO[bytes], rejected: Counter
) -> tuple[dict[tuple[str, str], _CoverFacts], int]:
    """dei cover facts of the registrant keyed by (adsh, dimh); all symbol facts seen."""
    index = _header(stream)
    width = len(index)
    facts: dict[tuple[str, str], _CoverFacts] = {}
    symbol_facts = 0
    for raw in stream:
        if not any(marker in raw for marker in _COVER_TAG_MARKERS):
            continue
        fields = _split(raw)
        if len(fields) != width:
            raise ValueError(f"txt.tsv row has {len(fields)} fields, expected {width}")
        tag = fields[index["tag"]]
        if tag not in COVER_TAGS:
            continue
        if tag == SYMBOL_TAG:
            symbol_facts += 1
        if not fields[index["version"]].startswith("dei/"):
            if tag == SYMBOL_TAG:
                rejected["non_dei_tag"] += 1
            continue
        if fields[index["coreg"]].strip():
            if tag == SYMBOL_TAG:
                rejected["coregistrant"] += 1
            continue
        value = fields[index["value"]].strip()
        iprx = int(fields[index["iprx"]] or 0)
        entry = facts.setdefault((fields[index["adsh"]], fields[index["dimh"]]), _CoverFacts())
        if tag == SYMBOL_TAG:
            entry.symbols.append((iprx, value, fields[index["ddate"]]))
        elif tag == TITLE_TAG:
            entry.titles.append((iprx, value))
        else:
            entry.exchanges.append((iprx, value))
    return facts, symbol_facts


def read_share_facts(
    stream: IO[bytes], rejected: Counter
) -> list[tuple[str, str, str, Decimal]]:
    """(adsh, dimh, ddate, shares) of the registrant's cover share counts."""
    index = _header(stream)
    width = len(index)
    found: list[tuple[str, str, str, Decimal]] = []
    for raw in stream:
        if _SHARES_MARKER not in raw:
            continue
        fields = _split(raw)
        if len(fields) != width:
            raise ValueError(f"num.tsv row has {len(fields)} fields, expected {width}")
        if fields[index["tag"]] != SHARES_TAG or not fields[index["version"]].startswith("dei/"):
            continue
        if fields[index["coreg"]].strip() or fields[index["uom"]] != "shares":
            rejected["share_count_other_entity_or_unit"] += 1
            continue
        try:
            shares = Decimal(fields[index["value"]])
        except InvalidOperation:
            rejected["share_count_unparseable"] += 1
            continue
        if not shares.is_finite() or shares < 0:
            rejected["share_count_unparseable"] += 1
            continue
        found.append((fields[index["adsh"]], fields[index["dimh"]], fields[index["ddate"]], shares))
    return found


def read_segments(stream: IO[bytes], wanted: set[str]) -> dict[str, str]:
    index = _header(stream)
    segments = {NO_DIMENSIONS: ""}
    for raw in stream:
        fields = _split(raw)
        dimh = fields[index["dimhash"]]
        if dimh in wanted:
            segments[dimh] = fields[index["segments"]].strip()
    return segments


def _first(values: list[tuple[int, str]]) -> str | None:
    for _, value in sorted(values):
        if value:
            return value
    return None


def build_observations(
    package: str,
    submissions: dict[str, Submission],
    facts: dict[tuple[str, str], _CoverFacts],
    segments: dict[str, str],
    rejected: Counter,
) -> list[Observation]:
    """Join symbols to their submission and class; one row per (adsh, dimh, ticker)."""
    rows: dict[tuple[str, str, str], Observation] = {}
    for (adsh, dimh), entry in sorted(facts.items()):
        if not entry.symbols:
            continue
        submission = submissions.get(adsh)
        title = _first(entry.titles)
        exchange = _first(entry.exchanges)
        context = segments.get(dimh)
        for iprx, raw, ddate in sorted(entry.symbols):
            if submission is None:
                rejected["no_submission"] += 1
                continue
            if context is None:
                rejected["unknown_dimension"] += 1
                continue
            tickers, reasons = normalize_symbols(raw)
            rejected.update(reasons)
            for ticker in tickers:
                if (adsh, dimh, ticker) in rows:
                    rejected["duplicate_in_context"] += 1
                    continue
                rows[(adsh, dimh, ticker)] = Observation(
                    adsh=adsh,
                    cik=submission.cik,
                    dimh=dimh,
                    segments=context,
                    class_key=class_key(context),
                    ticker=ticker,
                    ticker_raw=raw,
                    security_title=title,
                    exchange=exchange,
                    security_kind=security_kind(title, ticker, context),
                    ddate=parse_fsn_date(ddate),
                    form=submission.form,
                    period=submission.period,
                    filed=submission.filed,
                    accepted=submission.accepted,
                    source_package=package,
                )
    return [rows[key] for key in sorted(rows)]


def build_share_counts(
    package: str,
    submissions: dict[str, Submission],
    facts: list[tuple[str, str, str, Decimal]],
    segments: dict[str, str],
    rejected: Counter,
) -> list[ShareCount]:
    rows: dict[tuple[str, str, dt.date, Decimal], ShareCount] = {}
    for adsh, dimh, ddate, shares in facts:
        submission = submissions.get(adsh)
        context = segments.get(dimh)
        as_of = parse_fsn_date(ddate)
        if submission is None or context is None or as_of is None:
            rejected["share_count_unjoined"] += 1
            continue
        rows.setdefault((adsh, dimh, as_of, shares), ShareCount(
            adsh=adsh,
            cik=submission.cik,
            dimh=dimh,
            segments=context,
            class_key=class_key(context),
            ddate=as_of,
            shares=shares,
            form=submission.form,
            filed=submission.filed,
            accepted=submission.accepted,
            source_package=package,
        ))
    return [rows[key] for key in sorted(rows)]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_package(path: Path) -> PackageResult:
    started = time.monotonic()
    rejected: Counter = Counter()
    with zipfile.ZipFile(path) as archive:
        with archive.open("sub.tsv") as stream:
            submissions = read_submissions(stream)
        with archive.open("txt.tsv") as stream:
            facts, symbol_facts = read_cover_facts(io.BufferedReader(stream, 1 << 20), rejected)
        with archive.open("num.tsv") as stream:
            share_facts = read_share_facts(io.BufferedReader(stream, 1 << 20), rejected)
        wanted = {dimh for _, dimh in facts} | {dimh for _, dimh, _, _ in share_facts}
        with archive.open("dim.tsv") as stream:
            segments = read_segments(io.BufferedReader(stream, 1 << 20), wanted)
    observations = build_observations(path.name, submissions, facts, segments, rejected)
    share_counts = build_share_counts(path.name, submissions, share_facts, segments, rejected)
    return PackageResult(
        package=path.name,
        sha256=_sha256(path),
        size_bytes=path.stat().st_size,
        submissions=submissions,
        observations=observations,
        share_counts=share_counts,
        symbol_facts=symbol_facts,
        rejected=rejected,
        parse_seconds=time.monotonic() - started,
    )


# --------------------------------------------------------------------------- #
# EDGAR form indexes
# --------------------------------------------------------------------------- #
def index_sort_key(path: Path) -> tuple[int, int]:
    match = INDEX_RE.match(path.name)
    if match is None:
        raise ValueError(f"not an EDGAR form index name: {path.name}")
    return int(match.group("year")), int(match.group("quarter"))


def parse_form_index(path: Path) -> tuple[list[RegistrationEvent], str, int]:
    """Deregistration/delisting rows of one ``form.gz``; also its digest and size."""
    events: dict[tuple[str, int], RegistrationEvent] = {}
    with gzip.open(path, "rt", encoding="latin-1") as fh:
        for line in fh:
            form = line.split(" ", 1)[0]
            if form not in EVENT_FORMS:
                continue
            match = _INDEX_LINE_RE.match(line.rstrip("\n"))
            if match is None:
                raise ValueError(f"unparseable {path.name} row: {line!r}")
            event = RegistrationEvent(
                adsh=match.group("adsh"),
                cik=int(match.group("cik")),
                form=form,
                filed=dt.date.fromisoformat(match.group("filed")),
                source_package=path.name,
            )
            events.setdefault((event.adsh, event.cik), event)
    return [events[key] for key in sorted(events)], _sha256(path), path.stat().st_size


def quarters_through(today: dt.date) -> list[tuple[int, int]]:
    year, quarter = FIRST_INDEX_QUARTER
    last = (today.year, (today.month - 1) // 3 + 1)
    out = []
    while (year, quarter) <= last:
        out.append((year, quarter))
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    return out


def sec_client():
    """An HTTP client that identifies itself to the SEC as fair access requires."""
    import httpx

    return httpx.Client(headers={"User-Agent": USER_AGENT}, follow_redirects=True,
                        timeout=600.0)


def fetch_form_index(client, year: int, quarter: int, target: Path) -> Path:
    """Download one quarter's form.gz, validated before it replaces ``target``."""
    response = client.get(EDGAR_INDEX_URL.format(year=year, quarter=quarter))
    response.raise_for_status()
    partial = target.with_name(target.name + ".part")
    partial.write_bytes(response.content)
    gzip.decompress(partial.read_bytes())
    partial.replace(target)
    time.sleep(DOWNLOAD_SPACING_S)
    return target


def download_form_indexes(index_dir: Path, *, refresh_current: bool = True) -> list[Path]:
    """Fetch every quarterly form index since 2009 (the open quarter is re-fetched)."""
    index_dir.mkdir(parents=True, exist_ok=True)
    quarters = quarters_through(dt.date.today())
    with sec_client() as client:
        for year, quarter in quarters:
            target = index_dir / f"{year}QTR{quarter}.form.gz"
            if target.exists() and not (refresh_current and (year, quarter) == quarters[-1]):
                continue
            fetch_form_index(client, year, quarter, target)
    return sorted(index_dir.glob("*.form.gz"), key=index_sort_key)


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #
def apply_schema(dsn: str | None) -> None:
    """Local/dev only: production applies the governed DDL by hand."""
    with connect(dsn, autocommit=True) as conn:
        conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def require_schema(conn) -> None:
    present = conn.execute(
        "SELECT to_regclass('sec_ticker_cik_observations') IS NOT NULL "
        "AND to_regclass('sec_cover_share_counts') IS NOT NULL "
        "AND to_regclass('sec_registration_events') IS NOT NULL "
        "AND to_regclass('sec_ticker_cik_packages') IS NOT NULL"
    ).fetchone()[0]
    if not present:
        raise RuntimeError(
            "sec_ticker_cik_observations is missing: apply "
            "schemas/sec_ticker_cik_history_v1.sql first"
        )


def _copy(cur, table: str, columns: tuple[str, ...], rows: Iterable[tuple]) -> None:
    with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)


def _record_package(cur, *, package: str, sha256: str, size: int, submissions: int,
                    symbol_facts: int, observations: int, share_counts: int, events: int,
                    rejected: Counter) -> None:
    cur.execute(
        """
        INSERT INTO sec_ticker_cik_packages (
            source_package, package_sha256, package_bytes, submissions, symbol_facts,
            observations, share_counts, events, rejected
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (source_package) DO UPDATE SET
            package_sha256 = EXCLUDED.package_sha256,
            package_bytes = EXCLUDED.package_bytes,
            submissions = EXCLUDED.submissions,
            symbol_facts = EXCLUDED.symbol_facts,
            observations = EXCLUDED.observations,
            share_counts = EXCLUDED.share_counts,
            events = EXCLUDED.events,
            rejected = EXCLUDED.rejected,
            loaded_at = now()
        """,
        (package, sha256, size, submissions, symbol_facts, observations, share_counts,
         events, json.dumps(dict(sorted(rejected.items())))),
    )


_OBSERVATION_UPDATE = ", ".join(
    f"{column} = EXCLUDED.{column}"
    for column in OBSERVATION_COLUMNS if column not in ("adsh", "dimh", "ticker")
)
_OBSERVATION_CHANGED = (
    "(" + ", ".join(f"sec_ticker_cik_observations.{c}" for c in OBSERVATION_COLUMNS) + ")"
    " IS DISTINCT FROM (" + ", ".join(f"EXCLUDED.{c}" for c in OBSERVATION_COLUMNS) + ")"
)


def load_package(conn, result: PackageResult) -> dict[str, int]:
    """Upsert one package in a single transaction; converge its submissions."""
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE tmp_sec_obs (LIKE sec_ticker_cik_observations "
                "INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            cur.execute(
                "CREATE TEMP TABLE tmp_sec_shares (LIKE sec_cover_share_counts "
                "INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            cur.execute("CREATE TEMP TABLE tmp_sec_adsh (adsh text PRIMARY KEY) ON COMMIT DROP")
            _copy(cur, "tmp_sec_obs", OBSERVATION_COLUMNS,
                  (o.as_tuple() for o in result.observations))
            _copy(cur, "tmp_sec_shares", SHARE_COLUMNS,
                  (s.as_tuple() for s in result.share_counts))
            _copy(cur, "tmp_sec_adsh", ("adsh",), ((adsh,) for adsh in result.submissions))
            cur.execute(
                """
                DELETE FROM sec_ticker_cik_observations o USING tmp_sec_adsh a
                WHERE o.adsh = a.adsh AND NOT EXISTS (
                    SELECT 1 FROM tmp_sec_obs t
                    WHERE t.adsh = o.adsh AND t.dimh = o.dimh AND t.ticker = o.ticker)
                """
            )
            removed = cur.rowcount
            cur.execute(
                f"""
                INSERT INTO sec_ticker_cik_observations ({", ".join(OBSERVATION_COLUMNS)})
                SELECT {", ".join(OBSERVATION_COLUMNS)} FROM tmp_sec_obs
                ON CONFLICT (adsh, dimh, ticker) DO UPDATE SET
                    {_OBSERVATION_UPDATE}, updated_at = now()
                WHERE {_OBSERVATION_CHANGED}
                RETURNING (xmax = 0) AS inserted
                """
            )
            written = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                DELETE FROM sec_cover_share_counts c USING tmp_sec_adsh a
                WHERE c.adsh = a.adsh AND NOT EXISTS (
                    SELECT 1 FROM tmp_sec_shares t
                    WHERE (t.adsh, t.dimh, t.ddate, t.shares) = (c.adsh, c.dimh, c.ddate, c.shares))
                """
            )
            shares_removed = cur.rowcount
            cur.execute(
                f"""
                INSERT INTO sec_cover_share_counts ({", ".join(SHARE_COLUMNS)})
                SELECT {", ".join(SHARE_COLUMNS)} FROM tmp_sec_shares
                ON CONFLICT (adsh, dimh, ddate, shares) DO UPDATE SET
                    cik = EXCLUDED.cik, segments = EXCLUDED.segments,
                    class_key = EXCLUDED.class_key, form = EXCLUDED.form,
                    filed = EXCLUDED.filed, accepted = EXCLUDED.accepted,
                    source_package = EXCLUDED.source_package
                WHERE (sec_cover_share_counts.cik, sec_cover_share_counts.segments,
                       sec_cover_share_counts.class_key, sec_cover_share_counts.form,
                       sec_cover_share_counts.filed, sec_cover_share_counts.accepted,
                       sec_cover_share_counts.source_package)
                    IS DISTINCT FROM (EXCLUDED.cik, EXCLUDED.segments, EXCLUDED.class_key,
                       EXCLUDED.form, EXCLUDED.filed, EXCLUDED.accepted,
                       EXCLUDED.source_package)
                RETURNING (xmax = 0)
                """
            )
            shares_written = [row[0] for row in cur.fetchall()]
            _record_package(
                cur, package=result.package, sha256=result.sha256, size=result.size_bytes,
                submissions=len(result.submissions), symbol_facts=result.symbol_facts,
                observations=len(result.observations), share_counts=len(result.share_counts),
                events=0, rejected=result.rejected,
            )
    return {
        "inserted": sum(1 for inserted in written if inserted),
        "updated": sum(1 for inserted in written if not inserted),
        "removed": removed,
        "shares_inserted": sum(1 for inserted in shares_written if inserted),
        "shares_removed": shares_removed,
    }


def load_form_index(conn, path: Path) -> dict[str, object]:
    events, sha256, size = parse_form_index(path)
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE tmp_sec_events (LIKE sec_registration_events "
                "INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            _copy(cur, "tmp_sec_events", EVENT_COLUMNS, (e.as_tuple() for e in events))
            cur.execute(
                f"""
                INSERT INTO sec_registration_events ({", ".join(EVENT_COLUMNS)})
                SELECT {", ".join(EVENT_COLUMNS)} FROM tmp_sec_events
                ON CONFLICT (adsh, cik) DO UPDATE SET
                    form = EXCLUDED.form, filed = EXCLUDED.filed,
                    source_package = EXCLUDED.source_package
                WHERE (sec_registration_events.form, sec_registration_events.filed,
                       sec_registration_events.source_package)
                    IS DISTINCT FROM (EXCLUDED.form, EXCLUDED.filed, EXCLUDED.source_package)
                RETURNING (xmax = 0)
                """
            )
            written = [row[0] for row in cur.fetchall()]
            _record_package(
                cur, package=path.name, sha256=sha256, size=size, submissions=0,
                symbol_facts=0, observations=0, share_counts=0, events=len(events),
                rejected=Counter(),
            )
    return {
        "package": path.name,
        "events": len(events),
        "forms": dict(Counter(e.form for e in events).most_common()),
        "inserted": sum(1 for inserted in written if inserted),
        "updated": sum(1 for inserted in written if not inserted),
    }


# --------------------------------------------------------------------------- #
# Packages
# --------------------------------------------------------------------------- #
def listed_package_urls(html: str) -> list[str]:
    paths = re.findall(
        r'href="(/files/dera/data/financial-statement-notes-data-sets/[0-9a-z_]+\.zip)"', html
    )
    return [SEC_BASE_URL + path for path in dict.fromkeys(paths)]


def list_package_urls(client) -> list[str]:
    listing = client.get(LISTING_URL)
    listing.raise_for_status()
    time.sleep(DOWNLOAD_SPACING_S)
    return listed_package_urls(listing.text)


def fetch_package(client, url: str, target: Path) -> Path:
    """Stream one FSN zip to disk; it replaces ``target`` only once it opens."""
    partial = target.with_name(target.name + ".part")
    with client.stream("GET", url) as response:
        response.raise_for_status()
        with partial.open("wb") as fh:
            for chunk in response.iter_bytes(1 << 20):
                fh.write(chunk)
    with zipfile.ZipFile(partial):
        pass
    partial.replace(target)
    time.sleep(DOWNLOAD_SPACING_S)
    return target


def download_packages(packages_dir: Path) -> list[Path]:
    """Fetch every listed FSN package not already present (sequential, SEC UA)."""
    packages_dir.mkdir(parents=True, exist_ok=True)
    fetched: list[Path] = []
    with sec_client() as client:
        for url in list_package_urls(client):
            target = packages_dir / url.rsplit("/", 1)[1]
            if target.exists():
                continue
            fetched.append(fetch_package(client, url, target))
            print(json.dumps({"downloaded": target.name, "bytes": target.stat().st_size}))
    return fetched


def discover_packages(packages_dir: Path) -> list[Path]:
    found = [path for path in packages_dir.glob("*_notes*.zip") if PACKAGE_RE.match(path.name)]
    return sorted(found, key=package_sort_key)


def run(
    packages: Iterable[Path],
    *,
    dsn: str | None,
    dry_run: bool,
    form_indexes: Iterable[Path] = (),
) -> list[dict[str, object]]:
    stats: list[dict[str, object]] = []
    # Autocommit: each package commits in its own explicit transaction. The
    # session lock keeps this script and the recurring worker from interleaving.
    conn = None if dry_run else connect(dsn, autocommit=True)
    try:
        if conn is not None:
            locked = conn.execute(
                "SELECT pg_try_advisory_lock(%s)", (LOCK_SEC_TICKER_CIK_HISTORY,)
            ).fetchone()[0]
            if not locked:
                raise RuntimeError("another sec_ticker_cik_history load holds the lock")
            require_schema(conn)
        for path in packages:
            result = parse_package(path)
            item = result.stats()
            if conn is not None:
                started = time.monotonic()
                item.update(load_package(conn, result))
                item["load_seconds"] = round(time.monotonic() - started, 1)
            print(json.dumps(item), flush=True)
            stats.append(item)
        for path in form_indexes:
            if conn is None:
                events, _, _ = parse_form_index(path)
                item = {"package": path.name, "events": len(events)}
            else:
                item = load_form_index(conn, path)
            print(json.dumps(item), flush=True)
            stats.append(item)
    finally:
        if conn is not None:
            conn.close()
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("packages", nargs="*", type=Path,
                        help="FSN package zips (default: all in --packages-dir)")
    parser.add_argument("--packages-dir", type=Path, default=DEFAULT_PACKAGES_DIR)
    parser.add_argument("--index-dir", type=Path, default=DEFAULT_INDEX_DIR)
    parser.add_argument("--download", action="store_true",
                        help="fetch listed FSN packages and EDGAR form indexes missing locally")
    parser.add_argument("--no-events", action="store_true",
                        help="skip the EDGAR deregistration/delisting events")
    parser.add_argument("--dsn", default=None, help="Database DSN; defaults to DATABASE_URL")
    parser.add_argument("--dry-run", action="store_true", help="parse and report; no database")
    parser.add_argument("--apply-schema", action="store_true",
                        help="local/dev: apply the schema first")
    args = parser.parse_args(argv)

    if args.download:
        download_packages(args.packages_dir)
        if not args.no_events:
            download_form_indexes(args.index_dir)
    packages = (
        sorted(args.packages, key=package_sort_key)
        if args.packages else discover_packages(args.packages_dir)
    )
    if not packages:
        parser.error(f"no FSN packages found in {args.packages_dir}")
    form_indexes = (
        [] if args.no_events
        else sorted(args.index_dir.glob("*.form.gz"), key=index_sort_key)
    )
    if args.apply_schema and not args.dry_run:
        apply_schema(args.dsn)
    started = time.monotonic()
    stats = run(packages, dsn=args.dsn, dry_run=args.dry_run, form_indexes=form_indexes)
    totals: Counter = Counter()
    for item in stats:
        for key in ("submissions", "symbol_facts", "observations", "share_counts", "events",
                    "inserted", "updated", "removed"):
            totals[key] += int(item.get(key, 0) or 0)  # type: ignore[call-overload]
    print(json.dumps({
        "packages": len(packages),
        "form_indexes": len(form_indexes),
        **dict(totals),
        "dry_run": args.dry_run,
        "seconds": round(time.monotonic() - started, 1),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
