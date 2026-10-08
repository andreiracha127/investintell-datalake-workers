"""Load the SEC cover-page ticker -> (CIK, class) history from public SEC data.

Sources (public, fetched with the SEC User-Agent, one request at a time):

* DERA "Financial Statement and Notes" packages (quarterly ``YYYYqN_notes.zip``,
  later monthly ``YYYY_MM_notes.zip``). Four members are streamed from the zip
  without extraction: ``sub.tsv`` (adsh, cik, form, period, filed, accepted),
  ``txt.tsv`` (the ``dei:TradingSymbol`` / ``dei:Security12bTitle`` /
  ``dei:SecurityExchangeName`` facts), ``num.tsv`` (``dei:EntityCommonStockSharesOutstanding``)
  and ``dim.tsv`` (the dimension segments of those facts' contexts). Reading a
  member to its end checks its CRC, so a corrupt package fails the run instead
  of loading partial data.
* EDGAR full-index form indexes (``full-index/YYYY/QTRn/form.gz``): the ends
  (Forms 15-12B, 15-12G, 15-15D, their foreign private issuer forms 15F-12B,
  15F-12G, 15F-15D, 25, 25-NSE) and starts (8-A12B, 8-A12G, 10-12B, 10-12G) of
  registrations, and their amendments.
* The EDGAR filings of those ends for CIKs with cover data
  (``Archives/edgar/data/<cik>/<adsh>.txt``), read for the class, rule provision
  and exchange they state, cached on disk one file per accession.

Rules (schemas/sec_ticker_cik_history_v1.sql documents the tables, _v2.sql the
functions it changes):

* only periodic and current reports carry cover evidence (PERIODIC_FORMS:
  10-K, 10-Q, 8-K, 20-F, 40-F, 6-K, 10-KT, 10-QT and their amendments);
  registration statements state the securities of other or future entities;
* a fact's class is the context's dimension segments without the
  listing-exchange axis, and without the legal-entity axis when it names a
  registrant (``ClassOfStock=CommonClassA;``), '' without dimensions;
* facts with a co-registrant (``coreg``) belong to that entity, not to
  ``sub.cik``;
* the knowledge date comes from ``sub.tsv`` (``accepted``, else ``filed`` + 1);
* symbols are normalized to the eod_prices / universe_constituents style
  (``BRK.B`` -> ``BRK-B``, ``USB PrA`` -> ``USB-PA``, ``(SIRI)`` -> ``SIRI``,
  ``JWA/JWB`` -> two symbols); ``ticker_raw`` keeps the filer's spelling;
  placeholders (``None``, ``N/A``, ``true``...) are rejected and counted.

Storage is bitemporal: one transaction per package or index reconciles its
facts against the stored ones; a fact the source no longer carries (and no other
loaded source does) is retired, never deleted, and a fact newly carried for an
accession already loaded is a correction knowable from the reconciliation date.
``--dry-run`` parses and reports without a database. The schema is governed: it
is applied only with ``--apply-schema``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import html
import io
import json
import re
import time
import zipfile
from collections import Counter
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import IO, Iterable

from src.db import LOCK_SEC_TICKER_CIK_HISTORY, connect

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PACKAGES_DIR = Path("E:/Edgard/fsn")
DEFAULT_INDEX_DIR = Path("E:/Edgard/edgar-index")
DEFAULT_EVENT_DOCS_DIR = Path("E:/Edgard/edgar-event-docs")
LISTING_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets"
)
SEC_BASE_URL = "https://www.sec.gov"
EDGAR_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/form.gz"
FIRST_INDEX_QUARTER = (2009, 1)  # the first FSN package
USER_AGENT = "InvestIntell-SEP-Ingestion/1.0 (+https://hub.investintell.com)"
# A submission's full text in its accession folder.
EDGAR_FILING_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{folder}/{adsh}.txt"
# SEC fair access: at most 10 requests per second.
FILING_SPACING_S = 0.11
# SEC fair access allows 10 requests/s; downloads run one at a time, spaced.
DOWNLOAD_SPACING_S = 0.5
SCHEMA_PATHS = (ROOT / "schemas" / "sec_ticker_cik_history_v1.sql",
                ROOT / "schemas" / "sec_ticker_cik_history_v2.sql")

SYMBOL_TAG = "TradingSymbol"
TITLE_TAG = "Security12bTitle"
EXCHANGE_TAG = "SecurityExchangeName"
ENTITY_CIK_TAG = "EntityCentralIndexKey"
SHARES_TAG = "EntityCommonStockSharesOutstanding"
COVER_TAGS = frozenset({SYMBOL_TAG, TITLE_TAG, EXCHANGE_TAG, ENTITY_CIK_TAG})
# Byte prefilters: the tag is the second tab-separated field of txt/num rows.
_COVER_TAG_MARKERS = tuple(f"\t{tag}\t".encode() for tag in COVER_TAGS)
_SHARES_MARKER = f"\t{SHARES_TAG}\t".encode()
NO_DIMENSIONS = "0x00000000"
# Axes that qualify where/for whom a class is reported, not which class it is.
EXCHANGE_AXIS = "EntityListingsExchange"
LEGAL_ENTITY_AXIS = "LegalEntity"
EQUITY_KINDS = frozenset({"equity", "depositary"})
# A listed line whose kind nothing in the filing identifies (an untitled,
# undimensioned symbol on a foreign private issuer's cover: TSM's 2018 20-F tags
# TSM, its ADS, beside the count of its ordinary shares). It is the filer's listed
# security, but neither an ordinary nor a depositary line.
UNKNOWN_KIND = "unknown"
LISTED_KINDS = EQUITY_KINDS | {UNKNOWN_KIND}
# Registration filings from the form indexes. Ends: termination of a class's
# registration or reporting duty (15-12B/15-12G/15-15D, and 15F-12B/15F-12G/
# 15F-15D by a foreign private issuer under Rule 12h-6) and removal from listing
# (25 by the issuer, 25-NSE by the exchange). Starts: registration of a class
# (8-A12B/8-A12G/10-12B/10-12G). An '/A' amends the latest original of its form.
END_FORMS = ("15-12B", "15-12G", "15-15D", "15F-12B", "15F-12G", "15F-15D", "25", "25-NSE")
REGISTRATION_FORMS = ("8-A12B", "8-A12G", "10-12B", "10-12G")
EVENT_ORIGINAL_FORMS = END_FORMS + REGISTRATION_FORMS
EVENT_FORMS = EVENT_ORIGINAL_FORMS + tuple(f"{form}/A" for form in EVENT_ORIGINAL_FORMS)
END_EVENT_FORMS = frozenset(END_FORMS + tuple(f"{form}/A" for form in END_FORMS))
# Registrations read for the class they register: a delisting is a transfer only
# when a registration of the same class is filed near it (PepsiCo's 8-A12B of its
# common stock), not of notes or preferred. A Form 10 (a spin-off's registration
# statement) is not read: its class stays unknown.
READ_REGISTRATION_FORMS = frozenset({"8-A12B", "8-A12G"})
READ_EVENT_FORMS = END_EVENT_FORMS | READ_REGISTRATION_FORMS
# Names the parser of the end and registration filings; a new version re-derives
# every such event as a correction (derive_event_classes). v4: equity classes named
# without a Class/Series label count (equity_class_names); Forms 8-A are read.
EVENT_PARSER_VERSION = "sec_event_class_v4"
# Names the package parser (symbols, classes, share counts). Recorded on each
# package and on each fact version it inserts; not part of a fact's hash.
FSN_PARSER_VERSION = "sec_fsn_v2"
# Why a fact version was retired (sec_*.retired_reason). SOURCE: the public record
# changed (a republished package, an index that dropped or reassigned a row, a
# monthly package superseded by its quarter): the old version stays visible before
# its retirement. PARSER_CORRECTION: the same public filing read again by another
# parser version: the old reading was never true, so it is visible at no date and
# the new reading is knowable from the filing's own public date.
SOURCE = "source"
PARSER_CORRECTION = "parser_correction"
CLASS_STAT_KEYS = ("class_equity", "class_other", "class_unknown", "class_carried",
                   "class_reused", "class_unread", "filings_missing")
FETCH_STAT_KEYS = ("filings_fetched", "filings_failed", "filings_rejected")

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
    "security_title", "exchange", "security_kind", "filing_equity_classes",
    "filing_complete", "ddate", "form", "period", "filed", "accepted", "source_package",
)
SHARE_COLUMNS = (
    "adsh", "cik", "dimh", "segments", "class_key", "stated_on", "ddate_rounded", "shares",
    "form",
    "filed", "accepted", "source_package",
)
EVENT_COLUMNS = (
    "adsh", "cik", "form", "filed", "class_description", "class_kind", "class_count",
    "provision", "extinguished", "venue", "venue_kind", "amendment_effect", "parser_version",
    "source_package",
)

# Forms in which the filer reports on its own listed securities. Registration
# statements (S-1, S-3, S-4, S-8, F-1, F-4, POS AM...) state securities of other
# or future entities (Medtronic plc's S-4 named MDT months before it existed).
PERIODIC_FORMS = frozenset({
    "10-K", "10-Q", "8-K", "8-K12B", "8-K12G3", "8-K15D5", "20-F", "40-F", "6-K",
    "10-KT", "10-QT",
})


def is_periodic_form(form: str) -> bool:
    return form.removesuffix("/A") in PERIODIC_FORMS


# Forms whose cover enumerates every class with its share count. An 8-K or 6-K
# count is never a complete inventory.
INVENTORY_FORMS = frozenset({"10-K", "10-Q", "20-F", "40-F", "10-KT", "10-QT"})


def is_inventory_form(form: str) -> bool:
    return form.removesuffix("/A") in INVENTORY_FORMS


# Forms of foreign private issuers. Their listed line is often an ADS whose cover
# row names (or tags) the underlying class, so no count from these forms sizes a
# listed line in the schema (sec_cover_ticker_shares_at), and an unidentified
# line on them is 'unknown'.
FOREIGN_FORMS = frozenset({"20-F", "40-F", "6-K", "20-FR"})


def is_foreign_form(form: str) -> bool:
    return form.removesuffix("/A") in FOREIGN_FORMS


# Values filers put in dei:TradingSymbol when a security has no symbol, by their
# separator-free key (None, N/A, "Not Applicable"...). Every FSN value with one
# of these keys is a placeholder (NONE: 122 facts of 30-odd filers; NA: 62).
PLACEHOLDER_KEYS = frozenset({
    "NONE", "NA", "NOTAPPLICABLE", "NOTAVAILABLE", "NULL", "NIL",
    "NOSYMBOL", "NOTRADINGSYMBOL", "NOTLISTED", "NOTTRADED", "UNLISTED", "TBD",
})
# XBRL booleans typed as a symbol ("true", "True", "False": 107 facts of a dozen
# filers) are placeholders in that lexical form only: an all-uppercase TRUE is
# TrueCar's symbol (all 94 FSN facts written TRUE are TrueCar's).
BOOLEAN_KEYS = frozenset({"TRUE", "FALSE"})
_FILLER_RE = re.compile(r"^X{3,}$")
MAX_KEY_LENGTH = 12
_EXCHANGE_PREFIX_RE = re.compile(
    r"^(?:NYSE\s*AMERICAN|NYSE\s*ARCA|NYSE\s*MKT|NYSE|NASDAQ(?:GS|GM|CM)?|AMEX|OTCQX|"
    r"OTCQB|OTCBB|OTC\s*PINK|OTC|CBOE|TSX)\s*[:\-/]\s*",
    re.IGNORECASE,
)
EXCHANGE_TOKENS = frozenset({
    "NYSE", "NASDAQ", "NASDAQGS", "NASDAQGM", "NASDAQCM", "AMEX", "ARCA", "NYSEARCA",
    "NYSEAMERICAN", "NYSEMKT", "AMERICAN", "MKT", "CBOE", "BATS", "OTC", "OTCBB", "OTCQB",
    "OTCQX", "TSX",
})
# Venue names that are also the symbol their operator lists under: a field that is
# only CBOE is Cboe Global Markets' own symbol (all 113 FSN facts), not a venue.
# Any other venue alone (OTC, OTCQB, OTCQX, NYSE: 5 facts) is a placeholder.
LISTED_VENUE_SYMBOLS = frozenset({"CBOE", "BATS"})
# The OTC Bulletin Board suffix: beside a symbol it qualifies it ("EDLG, OB",
# WELPP.OB); alone it is Outbrain's symbol (all 63 FSN facts written OB).
QUALIFIER_KEYS = EXCHANGE_TOKENS | {"OB"}
_OTC_SUFFIX_RE = re.compile(r"[.\s]+(?:OB|OTCBB|OTCQB|OTCQX|OTC)$", re.IGNORECASE)
# A token that qualifies the symbol before it rather than naming another one:
# a class letter, a preferred/series marker, warrants, units, rights, a note year.
_SUFFIX_TOKEN_RE = re.compile(r"^(?:[A-Z]|WS|WT|W|U|UN|R|RT|PR|PR[A-Z]|P[A-Z]|[0-9][0-9A-Z]*)$")
_GROUP_RE = re.compile(r"[(\[{]([^)\]}]*)[)\]}]")
_QUOTES_RE = re.compile(r"[\"\u201c\u201d\u00ab\u00bb]")
# Preferred series: an uppercase root, a preferred marker, one series letter.
# A lowercase marker (``Pr``, ``pr``, ``p``) is unambiguous even without a
# separator (PSAPrM, CDRpB); an uppercase marker needs one (WFC.PRA, USB PRA).
_PREFERRED_RE = re.compile(
    r"^(?P<root>[A-Z0-9]+)(?P<sep>\s*[.\-/]?\s*)(?P<marker>Pr|pr|p|PR|P)"
    r"\s*[.\-/]?\s*(?P<series>[A-Za-z])$"
)
_SEPARATORS_RE = re.compile(r"[\s./\-_:']+")
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
    ("preferred", re.compile(r"preferred|preference|\bperpetual\b|\bpref\b", re.IGNORECASE)),
    ("unit", re.compile(r"^\s*units?\b|\bunits?,?\s+each\b|\beach\s+unit\b", re.IGNORECASE)),
    ("warrant", re.compile(r"warrant", re.IGNORECASE)),
    ("right", re.compile(r"^\s*rights?\b|\brights?,?\s+each\b|\bcontingent\s+value\b",
                         re.IGNORECASE)),
    ("depositary", re.compile(
        r"american\s*deposit[ao]ry|deposit[ao]ry\s*(?:shares|receipts)|\bADSs?\b|\bADRs?\b",
        re.IGNORECASE)),
)
_SEGMENT_KIND_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("debt", re.compile(r"Debt|Notes?(?:Due|\b)|Debenture|Bond", re.IGNORECASE)),
    ("preferred", re.compile(r"Preferred", re.IGNORECASE)),
    ("warrant", re.compile(r"Warrant", re.IGNORECASE)),
    ("unit", re.compile(r"=Units?Member|CapitalUnits", re.IGNORECASE)),
    ("depositary", re.compile(r"Deposit[ao]ry|\bADS|\bADRs?(?:\d|Member|;|$)", re.IGNORECASE)),
)
# An ordinary or common share member: on a foreign private issuer's form, the
# segments that identify an untitled line as the ordinary class.
_EQUITY_SEGMENT_RE = re.compile(r"Ordinary|Common(?:Stock|Shares?|Class)", re.IGNORECASE)


@dataclass(frozen=True)
class Submission:
    cik: int
    form: str
    period: dt.date | None
    filed: dt.date
    accepted: dt.datetime | None
    nciks: int = 1


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
    filing_equity_classes: int
    filing_complete: bool
    ddate: dt.date | None
    form: str
    period: dt.date | None
    filed: dt.date
    accepted: dt.datetime | None
    source_package: str

    def as_tuple(self) -> tuple:
        return tuple(getattr(self, column) for column in OBSERVATION_COLUMNS)

    @property
    def fact_hash(self) -> str:
        return _fact_hash(getattr(self, c) for c in OBSERVATION_COLUMNS if c != "source_package")


@dataclass(frozen=True)
class ShareCount:
    adsh: str
    cik: int
    dimh: str
    segments: str
    class_key: str
    stated_on: dt.date
    ddate_rounded: dt.date
    shares: Decimal
    form: str
    filed: dt.date
    accepted: dt.datetime | None
    source_package: str

    def as_tuple(self) -> tuple:
        return tuple(getattr(self, column) for column in SHARE_COLUMNS)

    @property
    def fact_hash(self) -> str:
        return _fact_hash(getattr(self, c) for c in SHARE_COLUMNS if c != "source_package")


@dataclass(frozen=True)
class RegistrationEvent:
    adsh: str
    cik: int
    form: str
    filed: dt.date
    source_package: str
    # What the filing states (end filings of CIKs with cover data; None: not read).
    class_description: str | None = None
    class_kind: str | None = None
    class_count: int | None = None
    provision: str | None = None
    extinguished: bool | None = None
    venue: str | None = None
    venue_kind: str | None = None
    amendment_effect: str | None = None
    parser_version: str | None = None

    def as_tuple(self) -> tuple:
        return tuple(getattr(self, column) for column in EVENT_COLUMNS)

    @property
    def fact_hash(self) -> str:
        return _fact_hash(getattr(self, c) for c in EVENT_COLUMNS if c != "source_package")


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


def _unwrap(raw: str) -> str:
    """Drop quotes; unwrap bracketed symbols; drop bracketed exchange names or prose."""
    def group(match: re.Match[str]) -> str:
        inner = match.group(1).strip()
        stripped = _EXCHANGE_PREFIX_RE.sub("", inner).strip()
        if stripped != inner:
            return f" {stripped} "  # (NYSE:FBC)
        words = inner.split()
        if not words or all(w.upper() in EXCHANGE_TOKENS for w in words):
            return " "  # (NYSE)
        if len(words) > 1 and re.search(r"[a-z]", inner):
            return " "  # (The Nasdaq Stock Market LLC)
        return f" {inner} "  # (SIRI)
    return _QUOTES_RE.sub(" ", _GROUP_RE.sub(group, raw))


def is_placeholder(value: str) -> bool:
    """Whether a value as the filer wrote it says "no symbol": a placeholder key
    (None, N/A, "Not Applicable"), a filler (XXXXX), or an XBRL boolean in its
    lexical form ("true", "False"; TRUE is a symbol)."""
    key = ticker_key(_SEPARATORS_RE.sub("", value))
    if key in BOOLEAN_KEYS:
        return not value.strip().isupper()
    return key in PLACEHOLDER_KEYS or bool(_FILLER_RE.match(key))


def normalize_symbol(raw: str) -> tuple[str | None, str | None]:
    """One filer-typed symbol -> (ticker, None) or (None, rejection reason)."""
    value = _EXCHANGE_PREFIX_RE.sub("", raw.strip()).strip()
    value = _OTC_SUFFIX_RE.sub("", value).strip()
    if not value:
        return None, "empty"
    if is_placeholder(value):
        return None, "placeholder"
    preferred = _PREFERRED_RE.match(value)
    if preferred and (
        preferred.group("marker") in ("Pr", "pr", "p") or preferred.group("sep")
    ):
        ticker = f"{preferred.group('root')}-P{preferred.group('series').upper()}"
    else:
        ticker = _SEPARATORS_RE.sub("-", value.upper()).strip("-")
    key = ticker_key(ticker)
    if key in PLACEHOLDER_KEYS or _FILLER_RE.match(key):
        return None, "placeholder"
    if not _TICKER_RE.match(ticker) or not re.search(r"[A-Z]", key):
        return None, "malformed"
    if len(key) > MAX_KEY_LENGTH:
        return None, "too_long"
    return ticker, None


def _is_suffix_token(token: str) -> bool:
    """A token that qualifies the symbol before it (class, series, warrant, unit,
    note year) rather than naming another security: BRK B, ACHR WS, USB PrA, T 25."""
    return bool(_SUFFIX_TOKEN_RE.match(token)) or bool(re.fullmatch(r"[Pp]r[A-Za-z]?", token))


def _split_field(field: str) -> list[str]:
    """Symbols written in one field: separated by space or slash unless the next
    token only qualifies the previous symbol. An exchange name is dropped only
    when it qualifies another token ("BAX NYSE"); alone it is the field (CBOE)."""
    field = _EXCHANGE_PREFIX_RE.sub("", field.strip()).strip()
    tokens = [t for t in re.split(r"[\s/]+", field) if t and not re.fullmatch(r"[-.:]+", t)]
    if len(tokens) > 1:
        tokens = [t for t in tokens if t.upper() not in EXCHANGE_TOKENS]
    symbols: list[list[str]] = []
    for token in tokens:
        if symbols and _is_suffix_token(token):
            symbols[-1].append(token)
        else:
            symbols.append([token])
    return [" ".join(parts) for parts in symbols]


def normalize_symbols(raw: str) -> tuple[list[str], list[str]]:
    """A dei:TradingSymbol value -> its symbols, and the reasons for rejected parts.

    Brackets and quotes are unwrapped (``(SIRI)``, ``[USG]``, ``"WM"``); exchange
    prefixes and OTC suffixes are dropped (``NYSE: KO``, ``NYSE/TRN``,
    ``WELPP.OB``); a field lists several symbols when they are separated by
    ``,`` ``;`` ``&`` ``AND``, or by a space or slash before a full symbol
    (``JWA/JWB``, ``jwa/jwb``, ``CRDA CRDB``) - not before a class or series
    suffix (``BRK B``, ``USB PrA``, ``USB/28``). A field that reads as prose or a
    placeholder (``No Trading Symbol``, ``Common Stock par value``, ``true``) is
    rejected whole; so is a venue name alone (``OTCQB``), unless the venue's
    operator lists under it (``CBOE``).
    """
    unwrapped = _unwrap(raw)
    whole = ticker_key(_SEPARATORS_RE.sub("", unwrapped))
    if is_placeholder(unwrapped) or (
            whole in EXCHANGE_TOKENS and whole not in LISTED_VENUE_SYMBOLS):
        return [], ["placeholder"]
    if not whole:
        return [], ["empty"]
    tickers: list[str] = []
    rejections: list[str] = []
    parts = [p.strip() for p in re.split(r"[;,&]|\s+AND\s+", unwrapped, flags=re.IGNORECASE)
             if p.strip()]
    for part in parts:
        part = _EXCHANGE_PREFIX_RE.sub("", part).strip()  # OTC Pink: IRRX
        if not part:
            continue
        if len(parts) > 1 and ticker_key(part) in QUALIFIER_KEYS:
            rejections.append("placeholder")  # "EDLG, OB": the market, not a symbol
            continue
        # Prose is several words, one of them a written word ("Common Stock par
        # value"); slash-joined lowercase symbols (jwa/jwb) are one word.
        words = part.split()
        if len(words) > 1 and any(
            len(w) >= 3 and re.search(r"[a-z]", w) and not _is_suffix_token(w)
            for w in words
        ):
            rejections.append("malformed")  # prose, not symbols
            continue
        for symbol in _split_field(part):
            ticker, reason = normalize_symbol(symbol)
            if ticker is not None:
                if ticker not in tickers:
                    tickers.append(ticker)
            elif reason != "empty":
                rejections.append(reason or "malformed")
    if not tickers and not rejections:
        rejections.append("empty")
    return tickers, rejections


def _segment_parts(segments: str) -> list[str]:
    return [part for part in segments.split(";") if part]


def legal_entity_member(segments: str) -> str | None:
    """The LegalEntityAxis member of a context, if any."""
    for part in _segment_parts(segments):
        axis, _, member = part.partition("=")
        if axis == LEGAL_ENTITY_AXIS:
            return member
    return None


def class_key(segments: str, *, keep_entity: bool = False) -> str:
    """The class part of a context's segments, in a stable order.

    The listing-exchange axis never distinguishes a class. The legal-entity axis
    is dropped when it names a registrant, and kept when a single registrant
    uses it to name one of its own classes (Renalytix's
    ``LegalEntity=AmericanDepositaryShares``).
    """
    dropped = {EXCHANGE_AXIS} if keep_entity else {EXCHANGE_AXIS, LEGAL_ENTITY_AXIS}
    kept = sorted(
        part for part in _segment_parts(segments) if part.split("=", 1)[0] not in dropped
    )
    return "".join(f"{part};" for part in kept)


def security_kind(title: str | None, ticker: str, segments: str, *,
                  foreign: bool = False) -> str:
    """What a cover line is: from its title, else its segments, else its symbol.
    With no such evidence a line is equity, except on a foreign private issuer's
    form (``foreign``), where it is ``unknown``: an ADS and an ordinary share look
    alike without a title."""
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
    if foreign and not _EQUITY_SEGMENT_RE.search(segments):
        return UNKNOWN_KIND
    return "equity"


# --------------------------------------------------------------------------- #
# FSN package parsing
# --------------------------------------------------------------------------- #
def package_sort_key(path: Path) -> tuple[int, int, str]:
    """(year, last month covered, name): a quarterly package sorts after the
    monthly packages of its months, which it supersedes."""
    match = PACKAGE_RE.match(path.name)
    if match is None:
        raise ValueError(f"not a DERA FSN package name: {path.name}")
    year = int(match.group("year"))
    month = (
        3 * int(match.group("quarter"))
        if match.group("quarter")
        else int(match.group("month"))
    )
    return year, month, path.name


def quarter_months(name: str) -> tuple[int, tuple[int, ...]] | None:
    """(year, months) a quarterly package covers; None for a monthly one. DERA's
    YYYYqN holds the filings of months 3N-2..3N, as its YYYY_MM packages did
    (2025q3_notes.zip: filed 2025-07-01..2025-09-30; 2025_10_notes.zip: October)."""
    match = PACKAGE_RE.match(name)
    if match is None or not match.group("quarter"):
        return None
    quarter = int(match.group("quarter"))
    return int(match.group("year")), tuple(range(3 * quarter - 2, 3 * quarter + 1))


def covering_quarter(name: str) -> str | None:
    """The quarterly package name prefix (YYYYqN) that supersedes a monthly one."""
    match = PACKAGE_RE.match(name)
    if match is None or not match.group("month"):
        return None
    return f"{match.group('year')}q{(int(match.group('month')) - 1) // 3 + 1}"


def parse_fsn_date(value: str) -> dt.date | None:
    value = value.strip()
    if not value:
        return None
    return dt.datetime.strptime(value, "%Y%m%d").date()


def stated_date(ddate: str, datp: str) -> tuple[dt.date, dt.date] | None:
    """(stated date, DERA rounded ddate) of a numeric fact.

    DERA rounds ``ddate`` to the nearest month end and reports ``datp``, the days
    from the stated date to that month end, so the stated date is ``ddate - datp``.
    Checked against the exact XBRL context dates (companyfacts ``period_end`` of
    the same accession): 352 of 353 sampled cover counts with a nonzero datp in
    2010-2024 packages. Campbell 0000016732-26-000026 (ddate 2026-09-30, datp 14)
    states 2026-09-16 on its cover.
    """
    rounded = parse_fsn_date(ddate)
    if rounded is None:
        return None
    try:
        days = round(float(datp)) if datp.strip() else 0
    except ValueError:
        return None
    return rounded - dt.timedelta(days=days), rounded


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
        nciks = fields[index["nciks"]].strip() if "nciks" in index else ""
        submissions[fields[index["adsh"]]] = Submission(
            cik=cik,
            form=fields[index["form"]].strip(),
            period=parse_fsn_date(fields[index["period"]]),
            filed=filed,
            accepted=parse_accepted(fields[index["accepted"]]) if "accepted" in index else None,
            nciks=int(nciks) if nciks.isdigit() else 1,
        )
    return submissions


@dataclass
class _CoverFacts:
    coreg: str = ""
    symbols: list[tuple[int, str, str]] = field(default_factory=list)  # iprx, value, ddate
    titles: list[tuple[int, str]] = field(default_factory=list)
    exchanges: list[tuple[int, str]] = field(default_factory=list)
    entity_ciks: list[tuple[int, str]] = field(default_factory=list)


def read_cover_facts(
    stream: IO[bytes], rejected: Counter
) -> tuple[dict[tuple[str, str, str], _CoverFacts], int]:
    """dei cover facts keyed by (adsh, dimh, coreg): two registrants of a combined
    filing can share a context hash (the undimensioned one), so the co-registrant
    is part of the key; also all symbol facts seen."""
    index = _header(stream)
    width = len(index)
    facts: dict[tuple[str, str, str], _CoverFacts] = {}
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
        value = fields[index["value"]].strip()
        iprx = int(fields[index["iprx"]] or 0)
        coreg = fields[index["coreg"]].strip()
        entry = facts.setdefault((fields[index["adsh"]], fields[index["dimh"]], coreg),
                                 _CoverFacts(coreg=coreg))
        if tag == SYMBOL_TAG:
            entry.symbols.append((iprx, value, fields[index["ddate"]]))
        elif tag == TITLE_TAG:
            entry.titles.append((iprx, value))
        elif tag == EXCHANGE_TAG:
            entry.exchanges.append((iprx, value))
        else:
            entry.entity_ciks.append((iprx, value))
    return facts, symbol_facts


@dataclass(frozen=True)
class _ShareFact:
    adsh: str
    dimh: str
    coreg: str
    ddate: str
    datp: str
    shares: Decimal


def read_share_facts(stream: IO[bytes], rejected: Counter) -> list[_ShareFact]:
    """The cover share counts (any entity; resolved against the contexts later)."""
    index = _header(stream)
    width = len(index)
    found: list[_ShareFact] = []
    for raw in stream:
        if _SHARES_MARKER not in raw:
            continue
        fields = _split(raw)
        if len(fields) != width:
            raise ValueError(f"num.tsv row has {len(fields)} fields, expected {width}")
        if fields[index["tag"]] != SHARES_TAG or not fields[index["version"]].startswith("dei/"):
            continue
        if fields[index["uom"]] != "shares":
            rejected["share_count_unit"] += 1
            continue
        try:
            shares = Decimal(fields[index["value"]])
        except InvalidOperation:
            rejected["share_count_unparseable"] += 1
            continue
        if not shares.is_finite() or shares < 0:
            rejected["share_count_unparseable"] += 1
            continue
        found.append(_ShareFact(
            adsh=fields[index["adsh"]],
            dimh=fields[index["dimh"]],
            coreg=fields[index["coreg"]].strip(),
            ddate=fields[index["ddate"]],
            datp=fields[index["datp"]] if "datp" in index else "",
            shares=shares,
        ))
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


@dataclass(frozen=True)
class _Entities:
    """Who each legal-entity context of a filing is, from dei:EntityCentralIndexKey."""

    by_member: dict[tuple[str, str], int]  # (adsh, LegalEntity member) -> CIK
    registrant_members: set[str]  # adsh in which some legal-entity member names a CIK
    titled_members: set[tuple[str, str]]  # (adsh, member) whose context titles a security


def _entities(facts: dict[tuple[str, str, str], _CoverFacts],
              segments: dict[str, str]) -> _Entities:
    by_member: dict[tuple[str, str], int] = {}
    titled: set[tuple[str, str]] = set()
    for (adsh, dimh, _), entry in facts.items():
        member = legal_entity_member(segments.get(dimh, ""))
        if member is None:
            continue
        value = _first(entry.entity_ciks)
        if value and value.isdigit() and int(value) > 0:
            by_member[(adsh, member)] = int(value)
        if _first(entry.titles):
            titled.add((adsh, member))
    return _Entities(by_member, {adsh for adsh, _ in by_member}, titled)


def resolve_owner(
    adsh: str, coreg: str, context: str, submission: Submission, entities: _Entities
) -> tuple[int, str] | None:
    """(CIK, class_key) a cover fact belongs to, or None when it cannot be attributed.

    A fact without a co-registrant belongs to ``sub.cik``. A legal-entity context
    whose member carries its own dei:EntityCentralIndexKey belongs to that CIK (a
    genuine co-registrant, or the registrant itself). A member without one is the
    registrant's own class only in a single-registrant filing in which no member
    names a registrant and the member's context titles a security: Renalytix
    and Nano Dimension name their ADS that way. Anything else is unattributable.
    """
    if not coreg:
        return submission.cik, class_key(context)
    member = legal_entity_member(context)
    if member is None:
        return None
    mapped = entities.by_member.get((adsh, member))
    if mapped is not None:
        return mapped, class_key(context)
    if (submission.nciks <= 1 and adsh not in entities.registrant_members
            and (adsh, member) in entities.titled_members):
        return submission.cik, class_key(context, keep_entity=True)
    return None


def _filing_profiles(
    symbol_classes: dict[tuple[str, int], dict[str, set[str]]],
    other_classes: dict[tuple[str, int], set[str]],
    counted: set[tuple[str, int]],
) -> dict[tuple[str, int], tuple[int, bool]]:
    """(equity classes shown in total, complete) per (accession, CIK); complete:
    a 10-K/10-Q/20-F/40-F/10-KT/10-QT (or /A) cover that reports a share count.

    The classes are those of the filing's equity/depositary symbols, plus the
    dimensioned classes of its share counts and of its titled equity classes
    without an accepted symbol (an unlisted class B with ticker "N/A" still
    counts). ``symbol_classes`` maps each class key to the distinct symbols shown
    on it: distinct symbols sharing one context are distinct classes (an
    undimensioned "GOOG, GOOGL" or "JWA/JWB" is two). Undimensioned symbols next
    to dimensioned classes that no symbol names are those classes (a filer tags
    the symbol without a member and the counts with members: American Greetings'
    AM beside its Class A and Class B counts is two classes, not three).
    """
    profiles: dict[tuple[str, int], tuple[int, bool]] = {}
    for filing in set(symbol_classes) | set(other_classes):
        symbols = symbol_classes.get(filing, {})
        others = other_classes.get(filing, set())
        named = sum(len(tickers) for key, tickers in symbols.items() if key)
        unnamed = len(others - set(symbols))
        undimensioned = len(symbols.get("", ()))
        count = named + (max(undimensioned, unnamed) if undimensioned else unnamed)
        profiles[filing] = (count, filing in counted)
    return profiles


def build_share_counts(
    package: str,
    submissions: dict[str, Submission],
    facts: list[_ShareFact],
    cover: dict[tuple[str, str, str], _CoverFacts],
    segments: dict[str, str],
    rejected: Counter,
) -> list[ShareCount]:
    entities = _entities(cover, segments)
    # One count per (accession, owner, context, stated day, value): two
    # co-registrants of a combined filing may state the same count (100 shares
    # each) in one context.
    rows: dict[tuple[str, int, str, str, dt.date, Decimal], ShareCount] = {}
    for fact in facts:
        submission = submissions.get(fact.adsh)
        context = segments.get(fact.dimh)
        dates = stated_date(fact.ddate, fact.datp)
        if submission is None or context is None or dates is None:
            rejected["share_count_unjoined"] += 1
            continue
        if not is_periodic_form(submission.form):
            rejected["share_count_non_periodic_form"] += 1
            continue
        owner = resolve_owner(fact.adsh, fact.coreg, context, submission, entities)
        if owner is None:
            rejected["share_count_coregistrant"] += 1
            continue
        cik, key = owner
        stated_on, rounded = dates
        rows.setdefault((fact.adsh, cik, fact.coreg, fact.dimh, stated_on, fact.shares), ShareCount(
            adsh=fact.adsh,
            cik=cik,
            dimh=fact.dimh,
            segments=context,
            class_key=key,
            stated_on=stated_on,
            ddate_rounded=rounded,
            shares=fact.shares,
            form=submission.form,
            filed=submission.filed,
            accepted=submission.accepted,
            source_package=package,
        ))
    return [rows[key] for key in sorted(rows)]


def build_observations(
    package: str,
    submissions: dict[str, Submission],
    facts: dict[tuple[str, str, str], _CoverFacts],
    segments: dict[str, str],
    share_counts: list[ShareCount],
    rejected: Counter,
) -> list[Observation]:
    """Join symbols to submission, owner and class; one row per (adsh, dimh, ticker).

    Only periodic reports and current reports describe the filer's own listed
    securities (PERIODIC_FORMS); a registration statement (S-1, S-4, F-4...)
    states the securities of other or future entities and is skipped.
    """
    entities = _entities(facts, segments)
    pending: list[dict] = []
    symbol_classes: dict[tuple[str, int], dict[str, set[str]]] = {}
    other_classes: dict[tuple[str, int], set[str]] = {}
    for share in share_counts:
        if share.class_key:
            other_classes.setdefault((share.adsh, share.cik), set()).add(share.class_key)
    counted = {(share.adsh, share.cik) for share in share_counts if is_inventory_form(share.form)}
    seen: set[tuple[str, str, str, str]] = set()
    for (adsh, dimh, coreg), entry in sorted(facts.items()):
        if not entry.symbols and not entry.titles:
            continue
        submission = submissions.get(adsh)
        context = segments.get(dimh)
        title = _first(entry.titles)
        if submission is None or context is None:
            if entry.symbols:
                rejected["no_submission" if submission is None else "unknown_dimension"] += (
                    len(entry.symbols))
            continue
        if not is_periodic_form(submission.form):
            rejected["non_periodic_form"] += len(entry.symbols)
            continue
        owner = resolve_owner(adsh, coreg, context, submission, entities)
        if owner is None:
            rejected["coregistrant"] += len(entry.symbols)
            continue
        cik, key = owner
        accepted: list[tuple[str, str, str]] = []
        for iprx, raw, ddate in sorted(entry.symbols):
            tickers, reasons = normalize_symbols(raw)
            rejected.update(reasons)
            for ticker in tickers:
                if (adsh, dimh, coreg, ticker) in seen:
                    rejected["duplicate_in_context"] += 1
                    continue
                seen.add((adsh, dimh, coreg, ticker))
                accepted.append((ticker, raw, ddate))
        if accepted and cik != submission.cik:
            rejected["attributed_to_coregistrant"] += len(accepted)
        for ticker, raw, ddate in accepted:
            kind = security_kind(title, ticker, context,
                                 foreign=is_foreign_form(submission.form))
            if kind in LISTED_KINDS:
                symbol_classes.setdefault((adsh, cik), {}).setdefault(key, set()).add(
                    ticker_key(ticker))
            pending.append({
                "adsh": adsh, "cik": cik, "dimh": dimh, "segments": context, "class_key": key,
                "ticker": ticker, "ticker_raw": raw, "security_title": title,
                "exchange": _first(entry.exchanges), "security_kind": kind,
                "ddate": parse_fsn_date(ddate), "form": submission.form,
                "period": submission.period, "filed": submission.filed,
                "accepted": submission.accepted, "source_package": package,
            })
        if not accepted and title and key and security_kind(title, "", context) in EQUITY_KINDS:
            other_classes.setdefault((adsh, cik), set()).add(key)  # an unlisted class
    profiles = _filing_profiles(symbol_classes, other_classes, counted)
    rows = [
        Observation(**row, filing_equity_classes=profiles.get((row["adsh"], row["cik"]),
                                                              (0, False))[0],
                    filing_complete=profiles.get((row["adsh"], row["cik"]), (0, False))[1])
        for row in pending
    ]
    return sorted(rows, key=lambda o: (o.adsh, o.dimh, o.ticker))


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
        wanted = {dimh for _, dimh, _ in facts} | {fact.dimh for fact in share_facts}
        with archive.open("dim.tsv") as stream:
            segments = read_segments(io.BufferedReader(stream, 1 << 20), wanted)
    share_counts = build_share_counts(
        path.name, submissions, share_facts, facts, segments, rejected
    )
    observations = build_observations(
        path.name, submissions, facts, segments, share_counts, rejected
    )
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
    """Registration end/start rows of one ``form.gz``; also its digest and size."""
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


def quarter_closed_on(year: int, quarter: int) -> dt.date:
    """The day after a quarter's last day, from which its index is complete."""
    return dt.date(year + 1, 1, 1) if quarter == 4 else dt.date(year, 3 * quarter + 1, 1)


def index_needs_refresh(path: Path, year: int, quarter: int, today: dt.date) -> bool:
    """A cached index is refreshed when it is missing, when its quarter is the
    current or the previous one, or when it was downloaded (file time, UTC)
    before the day after its quarter closed: EDGAR adds the quarter's last
    filings to it up to then."""
    if not path.exists():
        return True
    current = (today.year, (today.month - 1) // 3 + 1)
    previous = (current[0] - 1, 4) if current[1] == 1 else (current[0], current[1] - 1)
    if (year, quarter) in (current, previous):
        return True
    fetched_on = dt.datetime.fromtimestamp(path.stat().st_mtime, dt.timezone.utc).date()
    return fetched_on <= quarter_closed_on(year, quarter)


def download_form_indexes(index_dir: Path, *, today: dt.date | None = None,
                          client=None) -> list[Path]:
    """Fetch every quarterly form index since 2009 that is missing or may be
    incomplete (index_needs_refresh)."""
    index_dir.mkdir(parents=True, exist_ok=True)
    today = today or dt.date.today()
    owns_client = client is None
    client = client or sec_client()
    try:
        for year, quarter in quarters_through(today):
            target = index_dir / f"{year}QTR{quarter}.form.gz"
            if index_needs_refresh(target, year, quarter, today):
                fetch_form_index(client, year, quarter, target)
    finally:
        if owns_client:
            client.close()
    return sorted(index_dir.glob("*.form.gz"), key=index_sort_key)


# --------------------------------------------------------------------------- #
# Event filings: the class a Form 15/25 concerns
# --------------------------------------------------------------------------- #
_XML_DESCRIPTION_RE = re.compile(
    r"<descriptionClassSecurity>(.*?)</descriptionClassSecurity>", re.S | re.I)
_XML_PROVISION_RE = re.compile(r"<ruleProvision>(.*?)</ruleProvision>", re.S | re.I)
_XML_EXCHANGE_RE = re.compile(
    r"<exchange>.*?<entityName>(.*?)</entityName>.*?</exchange>", re.S | re.I)
_DOCUMENT_RE = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.S | re.I)
_TAG_RE = re.compile(r"<[^>]+>")
_CLASS_LABEL_RE = re.compile(
    r"\(\s*(?:description\s+of\s+(?:the\s+)?(?:class(?:es)?\s+of\s+)?securit(?:y|ies)"
    r"|title\s+of\s+(?:each\s+)?class(?:es)?\s+of\s+securit(?:y|ies)\s+covered\s+by"
    r"\s+(?:this|the)\s+form)\s*\)",
    re.I,
)
_ADDRESS_LABEL_RE = re.compile(r"principal\s+executive\s+offices?\s*\)", re.I)
_ISSUER_LABEL_RE = re.compile(r"\(\s*exact\s+name\s+of\s+(?:the\s+)?issuer", re.I)
_FILE_NUMBER_RE = re.compile(r"commission\s+file\s+number", re.I)
# Exchanges where issuers keep second listings next to a primary market.
_SECONDARY_VENUE_RE = re.compile(
    r"chicago\s+stock\s+exchange|nyse\s+chicago|\bchx\b|boston\s+stock\s+exchange"
    r"|philadelphia\s+stock\s+exchange|\bphlx\b|nasdaq\s+(?:omx\s+)?bx\b"
    r"|national\s+stock\s+exchange|cincinnati\s+stock\s+exchange"
    r"|pacific\s+(?:stock\s+)?exchange|nyse\s+arca|archipelago",
    re.I,
)
_PRIMARY_VENUE_RE = re.compile(
    r"new\s+york\s+stock\s+exchange|\bnyse\b|nasdaq|american\s+stock\s+exchange|\bamex\b"
    r"|\bbats\b|\bcboe\b|investors\s+exchange",
    re.I,
)
# Interests in an employee plan, not a traded class.
_PLAN_RE = re.compile(
    r"\bplans?\b|\bsavings\b|401\s*\(?k\)?|profit[-\s]sharing|\bthrift\b|\besop\b"
    r"|\bretirement\b|\bdeferred\s+compensation\b",
    re.I,
)
# Phrases naming an instrument that only refers to an equity class.
_DEPENDENT_RES = (
    # units composed of shares and warrants
    re.compile(r"\bunits?\b[^;]*?\b(?:consisting|comprised|composed|representing)\b[^;]*", re.I),
    re.compile(r"\bunits?\s*,?\s*each\b[^;]*", re.I),
    # rights, warrants or options to buy a class
    re.compile(
        r"\b(?:rights?|warrants?|options?)\b[^;]*?\b(?:to\s+(?:purchase|acquire|buy|subscribe)"
        r"|exercisable|for\s+the\s+purchase\s+of)\b[^;]*",
        re.I,
    ),
    re.compile(
        r"(?:\b(?:common|preferred|preference|ordinary|capital|junior|participating|cumulative"
        r"|series\s+\w+|class\s+\w|share|shares|stock)\s+)*(?:purchase|subscription)\s+"
        r"(?:rights?|warrants?)",
        re.I,
    ),
    # depositary shares of a preferred share or a debt instrument
    re.compile(r"\bdepositary\s+(?:shares?|receipts?)\b[^;]*?\b(?:preferred|preference"
               r"|notes?|debentures?)\b[^;]*", re.I),
    # instruments convertible into or exchangeable for a class
    re.compile(r"\b(?:convertible|exchangeable|exercisable)\s+(?:into|for)\b[^;]*", re.I),
    re.compile(r"\bguarantee[sd]?\b[^;]*", re.I),
)
_EQUITY_CLASS_RE = re.compile(
    r"\b(?:common|ordinary|capital)\s+(?:stock|shares?|units?)\b"
    r"|\bshares?\s+of\s+beneficial\s+interest\b"
    r"|\b(?:american\s+)?depositary\s+(?:shares?|receipts?)\b|\bADSs?\b|\bADRs?\b"
    r"|\b(?:limited\s+)?partnership\s+(?:units|interests)\b|\btracking\s+stock\b"
    r"|\bcommon\b|\bordinary\s+shares?\b",
    re.I,
)
_PREFERRED_BEFORE_RE = re.compile(r"\b(?:preferred|preference)\s*(?:shares?|stock)?\s*$", re.I)
# A class name: a letter, a number, or a Roman numeral ("Class A", "Class 1",
# "Class II").
_CLASS_ID = r"(?:[A-Z]|\d{1,2}|I{1,3}|IV|VI{0,3})"
# "Class A", "Class A and B", "Classes A, B and C", "Class A/B", "Class A and Class B",
# "Class 1 and Class 2".
_CLASS_MENTION_RE = re.compile(
    rf"\bclass(?:es)?\s+({_CLASS_ID}(?:\s*(?:,|/|&|\band\b|\bor\b)\s*(?:class\s+)?"
    rf"{_CLASS_ID})*)\b",
    re.I,
)
# "Series A Common Stock and Series B Common Stock", "Series A and B common shares":
# a series counts as a class only when it names common, ordinary or capital stock
# ("Series A Preferred Stock" does not).
_SERIES_MENTION_RE = re.compile(
    rf"\bseries\s+({_CLASS_ID}(?:\s*(?:,|/|&|\band\b|\bor\b)\s*(?:series\s+)?"
    rf"{_CLASS_ID})*)\s+(?:(?:non-?)?voting\s+)?(?:common|ordinary|capital)\b",
    re.I,
)
_CLASS_ID_RE = re.compile(rf"(?<![A-Za-z0-9])({_CLASS_ID})(?![A-Za-z0-9])", re.I)
# A depositary share and the shares it represents are one listed line ("American
# Depositary Shares, each representing ten Ordinary Shares").
_DEPOSITARY_PHRASE_RE = re.compile(
    r"\b(?:(?:american\s+)?deposit[ao]ry\s+(?:shares?|receipts?)|ADS[sR]?s?)\b[^;]*?"
    r"\brepresent\w*\b[^;]*?\b(?:shares?|stock)\b",
    re.I,
)
_EQUITY_NOUN_RE = re.compile(r"\b(common|ordinary|capital)\s+(?:stock|shares?|units?)\b", re.I)
# Words that end a class name read backwards from its noun ("par value $0.01 per
# share Common Stock", "the Common Stock", "ten Ordinary Shares").
_NAME_STOP_WORDS = frozenset({
    "a", "an", "the", "of", "to", "for", "and", "or", "per", "each", "with", "in", "on", "its",
    "our", "any", "all", "by", "into", "as", "such", "share", "shares", "stock", "par", "value",
    "no", "nominal", "underlying", "representing", "represented", "represents", "including",
    "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven",
    "twelve", "twenty", "hundred", "thousand",
    # an exchange or an issuer named before the class ("Nasdaq Stock Market Common
    # Stock", "Shell plc Class A Ordinary Shares")
    "market", "exchange", "nasdaq", "nyse", "llc", "inc", "corporation", "corp", "plc", "ltd",
    "limited", "company", "co",
})


def equity_class_names(description: str | None) -> set[str]:
    """The distinct equity classes a Form 15/25/8-A description names by name, with
    or without a Class/Series label: "Common Stock" and "Non-Voting Common Stock"
    are two, as are the four of "Series A Liberty Capital Common Stock, Series B
    Liberty Capital Common Stock, Liberty Starz Ser A Common Stock, Liberty Starz
    Ser B Common Stock". Instruments that only refer to a class (warrants, rights,
    units), depositary shares of a class and a parenthesized alias ("Ordinary
    Shares (Common Stock)", '(the "Common Stock")') are not classes of their own."""
    if not description:
        return set()
    text = re.sub(r"\([^()]*\)", " ", description)
    for pattern in (*_DEPENDENT_RES, _DEPOSITARY_PHRASE_RE):
        text = pattern.sub(" ; ", text)
    names: set[str] = set()
    for match in _EQUITY_NOUN_RE.finditer(text):
        if _PREFERRED_BEFORE_RE.search(text[: match.start()]):
            continue
        words: list[str] = []
        tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9\-]*|[^\sA-Za-z0-9]", text[: match.start()])
        for i in range(len(tokens) - 1, -1, -1):
            token = tokens[i]
            labelled = i > 0 and tokens[i - 1].lower() in ("class", "series", "ser")
            if (not token[0].isalnum() or len(words) == 4
                    or (not labelled and (token[0].isdigit()
                                          or token.lower() in _NAME_STOP_WORDS))):
                break
            words.append(token.lower().replace("-", ""))
        names.add(" ".join([*reversed(words), match.group(1).lower()]))
    return names
_EXTINGUISHED_RE = re.compile(r"12d2-2\s*\(\s*a\s*\)", re.I)
# An amendment that withdraws the removal (Minim's 25-NSE/A of 2025-04-09: "will
# not be delisting the common stock ... per the Form 25 filed on October 24, 2024").
_CANCELS_RE = re.compile(
    r"\b(?:will|shall)\s+not\s+(?:be\s+)?delist|\bnot\s+be\s+delisting\b"
    r"|\b(?:withdraw(?:s|n|ing)?|withdrawal\s+of|rescind(?:s|ed|ing)?|rescission\s+of"
    r"|cancel(?:s|l?ed|l?ing|l?ation\s+of)?)\s+(?:the\s+|its\s+|this\s+|our\s+|that\s+)?"
    r"(?:previously\s+filed\s+|prior\s+|original\s+|above\s+)?(?:form\s*25|notification"
    r"|notice\s+of\s+removal|delisting|removal)",
    re.I,
)


@dataclass(frozen=True)
class EventClass:
    """What a Form 15/25 filing states about the class it concerns."""

    class_description: str | None
    class_kind: str  # equity | other | unknown
    class_count: int
    provision: str | None
    extinguished: bool | None
    venue: str | None
    venue_kind: str  # primary | secondary | unknown
    amendment_effect: str | None  # cancels | restates (amendments only)


def _plain(text: str) -> str:
    text = _TAG_RE.sub(" ", text)
    text = html.unescape(text).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def event_class_kind(description: str | None) -> str:
    """'equity' when a Form 15/25 class description names an equity class, 'other'
    when it names only other instruments, 'unknown' when there is none."""
    if not description:
        return "unknown"
    if _PLAN_RE.search(description):
        return "other"
    text = description
    for pattern in _DEPENDENT_RES:
        text = pattern.sub(" ; ", text)
    for match in _EQUITY_CLASS_RE.finditer(text):
        if not _PREFERRED_BEFORE_RE.search(text[: match.start()]):
            return "equity"  # but not "Preferred Shares of Beneficial Interest"
    return "other"


def class_count(description: str | None) -> int:
    """Distinct share classes a description names: by Class/Series enumeration
    ("Class A ... Class B", "Class A and B", "Classes A, B and C", "Class A/B",
    "Class 1 and Class 2", "Series A Common Stock and Series B Common Stock"), or
    by name ("Common Stock; Non-Voting Common Stock", equity_class_names),
    whichever names more; at least 1."""
    if not description:
        return 1
    names = {
        (kind, name.upper())
        for kind, pattern in (("class", _CLASS_MENTION_RE), ("series", _SERIES_MENTION_RE))
        for group in pattern.findall(description)
        for name in _CLASS_ID_RE.findall(
            re.sub(r"(?i)\b(?:and|or|class|series)\b", " ", group))
    }
    return max(1, len(names), len(equity_class_names(description)))


def venue_kind(venue: str | None) -> str:
    if not venue:
        return "unknown"
    if _SECONDARY_VENUE_RE.search(venue):
        return "secondary"
    if _PRIMARY_VENUE_RE.search(venue):
        return "primary"
    return "unknown"


_REGISTERED_12B_RE = re.compile(
    r"title\s+of\s+each\s+class\s+to\s+be\s+so\s+registered"
    r"(?:\s+name\s+of\s+each\s+exchange\s+on\s+which\s+each\s+class\s+is\s+to\s+be\s+registered)?",
    re.I,
)
_REGISTERED_12B_END_RE = re.compile(
    r"if\s+this\s+form\s+relates|securities\s+to\s+be\s+registered\s+pursuant\s+to\s+section"
    r"\s+12\s*\(\s*g\s*\)", re.I)
_REGISTERED_12G_RE = re.compile(
    r"securities\s+to\s+be\s+registered\s+pursuant\s+to\s+section\s+12\s*\(\s*g\s*\)\s+of\s+"
    r"the\s+act\s*:?", re.I)
_TITLE_OF_CLASS_RE = re.compile(r"\(\s*title\s+of\s+(?:each\s+)?class(?:es)?\s*\)", re.I)
_NO_CLASS_WORDS = frozenset({"NOT", "APPLICABLE", "NONE", "N", "A", "NA"})


def _names_a_class(block: str) -> bool:
    """Whether a registration block names something ("Not Applicable Not
    Applicable", "None" and "N/A" do not)."""
    words = {word.upper() for word in re.findall(r"[A-Za-z]+", block)}
    return bool(words) and not words <= _NO_CLASS_WORDS


def parse_registration_document(raw: str) -> EventClass:
    """The class a Form 8-A registers: the 12(b) table ("Title of each class to be
    so registered" / "Name of each exchange ...": PepsiCo's common stock on Nasdaq
    in 2017, its notes in 2018), else the 12(g) line above "(Title of class)"
    (Statera's Series B Preferred Stock of 2023). No class -> 'unknown'."""
    documents = _DOCUMENT_RE.findall(raw)
    body = _plain(documents[0] if documents else raw)
    description = None
    table = _REGISTERED_12B_RE.search(body)
    if table:
        end = _REGISTERED_12B_END_RE.search(body, table.end())
        block = body[table.end(): end.start() if end else table.end() + 500].strip(" :;,.-")
        if _names_a_class(block):
            description = block[:500]
    if description is None:
        section = _REGISTERED_12G_RE.search(body)
        label = _TITLE_OF_CLASS_RE.search(body, section.end()) if section else None
        if label:
            block = body[section.end(): label.start()].strip(" :;,.-")
            if _names_a_class(block):
                description = block[-500:]
    return EventClass(
        class_description=description,
        class_kind=event_class_kind(description),
        class_count=class_count(description),
        provision=None,
        extinguished=None,
        venue=None,
        venue_kind="unknown",
        amendment_effect=None,
    )


def parse_event_document(raw: str, form: str) -> EventClass:
    """The class, rule provision and exchange a Form 15/25 filing states.

    A 25-NSE carries them as XML (``descriptionClassSecurity``,
    ``ruleProvision``, ``exchange/entityName``). Forms 25, 15 and 15F are HTML or
    text: the class is the block between the address label and "(Description of
    class of securities)" or "(Title of each class of securities covered by this
    Form)"; Form 25's exchange is named next to the issuer above "(Exact name of
    Issuer ...)". No such block -> class_kind 'unknown'. A registration (8-A) is
    read by parse_registration_document.
    """
    if form.removesuffix("/A") in REGISTRATION_FORMS:
        return parse_registration_document(raw)
    amendment_effect = None
    if form.endswith("/A"):
        amendment_effect = "cancels" if _CANCELS_RE.search(_plain(raw)) else "restates"
    descriptions = [_plain(m) for m in _XML_DESCRIPTION_RE.findall(raw)]
    if descriptions:
        description = "; ".join(d for d in descriptions if d)[:500] or None
        found = _XML_PROVISION_RE.search(raw)
        provision = _plain(found.group(1)) if found else None
        found = _XML_EXCHANGE_RE.search(raw)
        venue = _plain(found.group(1)) if found else None
    else:
        documents = _DOCUMENT_RE.findall(raw)
        body = _plain(documents[0] if documents else raw)
        description = provision = venue = None
        label = _CLASS_LABEL_RE.search(body)
        if label:
            before = body[: label.start()]
            addresses = list(_ADDRESS_LABEL_RE.finditer(before))
            if addresses:
                block = before[addresses[-1].end():]
            else:
                cut = before.rfind(")")
                block = before[cut + 1:] if cut >= 0 else before[-300:]
            description = block.strip(" :;,.-")[-500:] or None
        issuer = _ISSUER_LABEL_RE.search(body)
        if issuer and form.startswith("25"):
            numbers = list(_FILE_NUMBER_RE.finditer(body[: issuer.start()]))
            start = numbers[-1].end() if numbers else max(0, issuer.start() - 300)
            venue = body[start: issuer.start()].strip(" :;,.-")[-300:] or None
    return EventClass(
        class_description=description,
        class_kind=event_class_kind(description),
        class_count=class_count(description),
        provision=provision,
        extinguished=bool(_EXTINGUISHED_RE.search(provision)) if provision else None,
        venue=venue,
        venue_kind=venue_kind(venue),
        amendment_effect=amendment_effect,
    )


def is_submission(raw: str, adsh: str) -> bool:
    """Whether a body is the SEC submission ``adsh``: its SEC header names the
    accession (every one of the 15,888 cached filings does, PEM-wrapped or not); a
    maintenance or throttling page answered with 200 does not."""
    return re.search(rf"ACCESSION\s+NUMBER:\s*{re.escape(adsh)}\b", raw[:20000]) is not None


class EventDocuments:
    """The EDGAR filings of end and registration events, kept in ``cache_dir``
    (one file per accession) so a re-parse never fetches again. A missing file is
    fetched with ``client`` (the SEC User-Agent) at most once per ``spacing``
    seconds; 429 and 5xx answers and transport errors back off (Retry-After when
    given) and retry. Without a client only cached filings are read. A filing that
    cannot be fetched (404 or any other HTTP error, or no answer after the
    retries) is not a document: ``text`` returns None and ``failed`` counts it. A
    body that is not the requested submission (a 200 HTML maintenance page) is
    retried, never cached, and if it persists counted in ``rejected``; a cached
    file that is not the submission is ignored and fetched again."""

    def __init__(self, cache_dir: Path, client=None, *, spacing: float | None = None,
                 retries: int = 6) -> None:
        self.cache_dir = cache_dir
        self.client = client
        self.spacing = FILING_SPACING_S if spacing is None else spacing
        self.retries = retries
        self.fetched = 0
        self.failed = 0
        self.rejected = 0
        self._last = 0.0

    def text(self, cik: int, adsh: str) -> str | None:
        """The filing's full text, or None when it is not cached and cannot be
        fetched now."""
        target = self.cache_dir / f"{adsh}.txt"
        if target.exists():
            cached = target.read_bytes().decode("latin-1")
            if is_submission(cached, adsh):
                return cached
            if self.client is None:
                self.rejected += 1
                return None
        if self.client is None:
            return None
        try:
            import httpx

            transport_errors: tuple[type[BaseException], ...] = (httpx.TransportError, OSError)
        except ImportError:  # a client that is not httpx
            transport_errors = (OSError,)
        url = EDGAR_FILING_URL.format(cik=cik, folder=adsh.replace("-", ""), adsh=adsh)
        not_submission = False
        for attempt in range(self.retries):
            wait = self.spacing - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            backoff = min(60.0, 2.0 ** attempt)
            try:
                response = self.client.get(url)
            except transport_errors:
                time.sleep(backoff)
                continue
            if response.status_code == 200:
                text = response.content.decode("latin-1")
                if not is_submission(text, adsh):
                    not_submission = True  # a maintenance or throttling page
                    if attempt >= 2:
                        break
                    time.sleep(backoff)
                    continue
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                partial = target.with_name(target.name + ".part")
                partial.write_bytes(response.content)
                partial.replace(target)
                self.fetched += 1
                return text
            not_submission = False
            if response.status_code in (429, 500, 502, 503, 504):
                retry_after = response.headers.get("retry-after", "")
                time.sleep(float(retry_after) if retry_after.isdigit() else backoff)
                continue
            break  # 404 or another HTTP error: not fetched
        if not_submission:
            self.rejected += 1
        else:
            self.failed += 1
        return None


def _event_key(event: RegistrationEvent) -> tuple[str, int, str, dt.date]:
    return event.adsh, event.cik, event.form, event.filed


def describe_events(
    events: list[RegistrationEvent], documents: EventDocuments | None, ciks: set[int],
    known: dict[tuple[str, int, str, dt.date], RegistrationEvent] | None = None,
) -> tuple[list[RegistrationEvent], Counter]:
    """Read the end filings (and their amendments) and the Forms 8-A of CIKs with
    cover data.

    An event already read by this parser version (``known``) is carried as read
    (``class_reused``), without fetching its filing again: a worker without a
    persistent cache would otherwise fetch both quarters' filings every week. A
    filing that cannot be read now (not cached, no client, not the submission) is
    no evidence: the class already derived for that event is carried forward
    unchanged, and only a parse replaces it. Each such miss counts as
    ``filings_missing``.
    """
    described: list[RegistrationEvent] = []
    stats: Counter = Counter()
    for event in events:
        prior = (known or {}).get(_event_key(event))
        if documents is None and prior is not None:
            # No filings to read in this run: the class read before stands.
            stats["class_carried"] += 1
            described.append(replace(prior, source_package=event.source_package))
            continue
        if documents is None or event.form not in READ_EVENT_FORMS or event.cik not in ciks:
            described.append(event)
            continue
        if prior is not None and prior.parser_version == EVENT_PARSER_VERSION:
            stats["class_reused"] += 1
            described.append(replace(prior, source_package=event.source_package))
            continue
        raw = documents.text(event.cik, event.adsh)
        if raw is None:
            stats["filings_missing"] += 1
            prior = (known or {}).get(_event_key(event))
            if prior is not None:
                stats["class_carried"] += 1
                described.append(replace(prior, source_package=event.source_package))
            else:
                stats["class_unread"] += 1
                described.append(event)
            continue
        parsed = parse_event_document(raw, event.form)
        stats[f"class_{parsed.class_kind}"] += 1
        described.append(replace(event, **asdict(parsed), parser_version=EVENT_PARSER_VERSION))
    return described, stats


def derived_events(conn, adshs: Iterable[str]) -> dict[tuple[str, int, str, dt.date],
                                                      RegistrationEvent]:
    """The latest version whose filing was read of each event of these
    accessions: the current one, else the latest retired one (an event an index
    dropped and lists again keeps the class read before)."""
    rows = conn.execute(
        f"SELECT DISTINCT ON (adsh, cik, form, filed) {', '.join(EVENT_COLUMNS)} "
        "FROM sec_registration_events "
        "WHERE parser_version IS NOT NULL AND adsh = ANY(%s) "
        "ORDER BY adsh, cik, form, filed, (retired_on IS NULL) DESC, id DESC",
        (sorted(set(adshs)),),
    ).fetchall()
    events = [RegistrationEvent(**dict(zip(EVENT_COLUMNS, row))) for row in rows]
    return {_event_key(e): e for e in events}


def cover_ciks(conn) -> set[int]:
    """CIKs with any cover observation, current or retired: their end filings are
    read. Runs load packages before indexes, so on a first load every CIK with
    cover data is known before its events are inserted."""
    return {cik for (cik,) in conn.execute(
        "SELECT DISTINCT cik FROM sec_ticker_cik_observations"
    ).fetchall()}


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #
def apply_schema(dsn: str | None) -> None:
    """Local/dev only: production applies the governed DDL by hand, v1 then v2."""
    with connect(dsn, autocommit=True) as conn:
        for path in SCHEMA_PATHS:
            conn.execute(path.read_text(encoding="utf-8"))


def require_schema(conn) -> None:
    """Refuse a database without the governed tables (v1) or without the v2
    functions this loader's rows are judged by (sec_issuer_end_events with
    effective_on)."""
    present, v2 = conn.execute(
        "SELECT to_regclass('sec_ticker_cik_observations') IS NOT NULL "
        "AND to_regclass('sec_cover_share_counts') IS NOT NULL "
        "AND to_regclass('sec_registration_events') IS NOT NULL "
        "AND to_regclass('sec_ticker_cik_packages') IS NOT NULL "
        "AND to_regclass('sec_ticker_cik_package_members') IS NOT NULL "
        "AND to_regclass('sec_ticker_cik_package_facts') IS NOT NULL, "
        "COALESCE((SELECT 'effective_on' = ANY(p.proargnames) FROM pg_catalog.pg_proc p "
        "WHERE p.oid = to_regprocedure('sec_issuer_end_events(bigint,date,boolean)')), false) "
        "AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a "
        "WHERE a.attrelid = to_regclass('sec_ticker_cik_observations') "
        "AND a.attname = 'retired_reason' AND NOT a.attisdropped)"
    ).fetchone()
    if not present:
        raise RuntimeError(
            "sec_ticker_cik_observations is missing: apply "
            "schemas/sec_ticker_cik_history_v1.sql, then schemas/sec_ticker_cik_history_v2.sql"
        )
    if not v2:
        raise RuntimeError(
            "the sec_ticker_cik_history functions are v1: apply "
            "schemas/sec_ticker_cik_history_v2.sql first"
        )


def _copy(cur, table: str, columns: tuple[str, ...], rows: Iterable[tuple]) -> None:
    with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)


def _record_package(cur, *, package: str, sha256: str, size: int, submissions: int,
                    symbol_facts: int, observations: int, share_counts: int, events: int,
                    rejected: Counter, parser_version: str,
                    validators: tuple[str | None, str | None] | None = None) -> None:
    """Record a loaded version with the parser that read it and the SEC's
    validators (ETag, Last-Modified) of the very bytes loaded, in the load's
    transaction; None: not known for them."""
    etag, last_modified = validators or (None, None)
    cur.execute(
        """
        INSERT INTO sec_ticker_cik_packages (
            source_package, package_sha256, package_bytes, submissions, symbol_facts,
            observations, share_counts, events, rejected, remote_etag, remote_last_modified,
            parser_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
        ON CONFLICT (source_package) DO UPDATE SET
            package_sha256 = EXCLUDED.package_sha256,
            package_bytes = EXCLUDED.package_bytes,
            submissions = EXCLUDED.submissions,
            symbol_facts = EXCLUDED.symbol_facts,
            observations = EXCLUDED.observations,
            share_counts = EXCLUDED.share_counts,
            events = EXCLUDED.events,
            rejected = EXCLUDED.rejected,
            remote_etag = EXCLUDED.remote_etag,
            remote_last_modified = EXCLUDED.remote_last_modified,
            parser_version = EXCLUDED.parser_version,
            loaded_at = now()
        """,
        (package, sha256, size, submissions, symbol_facts, observations, share_counts,
         events, json.dumps(dict(sorted(rejected.items()))), etag, last_modified,
         parser_version),
    )


def _fact_hash(values: Iterable[object]) -> str:
    """md5 of a fact's content columns: one version of one fact."""
    text = "\x1f".join("\x00" if v is None else str(v) for v in values)
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _replace_members(cur, package: str, members: Iterable[tuple[str, str, int]],
                     on: dt.date) -> None:
    """The package's (fact family, accession, CIK) triples as of ``on``: triples
    it no longer contains are retired, new ones inserted; history is kept."""
    cur.execute("CREATE TEMP TABLE tmp_sec_members (fact_table text, adsh text, cik bigint) "
                "ON COMMIT DROP")
    _copy(cur, "tmp_sec_members", ("fact_table", "adsh", "cik"), sorted(set(members)))
    cur.execute(
        "UPDATE sec_ticker_cik_package_members m SET retired_on = %(on)s "
        "WHERE m.source_package = %(p)s AND m.retired_on IS NULL AND NOT EXISTS ("
        "SELECT 1 FROM tmp_sec_members n WHERE n.fact_table = m.fact_table "
        "AND n.adsh = m.adsh AND n.cik = m.cik)",
        {"on": on, "p": package},
    )
    cur.execute(
        "INSERT INTO sec_ticker_cik_package_members (source_package, fact_table, adsh, cik, "
        "loaded_on) SELECT %(p)s, n.fact_table, n.adsh, n.cik, %(on)s FROM tmp_sec_members n "
        "WHERE NOT EXISTS (SELECT 1 FROM sec_ticker_cik_package_members m "
        "WHERE m.source_package = %(p)s AND m.fact_table = n.fact_table AND m.adsh = n.adsh "
        "AND m.cik = n.cik AND m.retired_on IS NULL)",
        {"on": on, "p": package},
    )


def _replace_facts(cur, package: str, facts: Iterable[tuple[str, str]], on: dt.date) -> None:
    """The fact versions the package carries as of ``on`` (history is kept)."""
    cur.execute("CREATE TEMP TABLE tmp_sec_carried (fact_table text, fact_hash text) "
                "ON COMMIT DROP")
    _copy(cur, "tmp_sec_carried", ("fact_table", "fact_hash"), sorted(set(facts)))
    cur.execute(
        "UPDATE sec_ticker_cik_package_facts f SET retired_on = %(on)s "
        "WHERE f.source_package = %(p)s AND f.retired_on IS NULL AND NOT EXISTS ("
        "SELECT 1 FROM tmp_sec_carried n WHERE n.fact_table = f.fact_table "
        "AND n.fact_hash = f.fact_hash)",
        {"on": on, "p": package},
    )
    cur.execute(
        "INSERT INTO sec_ticker_cik_package_facts (source_package, fact_table, fact_hash, "
        "loaded_on) SELECT %(p)s, n.fact_table, n.fact_hash, %(on)s FROM tmp_sec_carried n "
        "WHERE NOT EXISTS (SELECT 1 FROM sec_ticker_cik_package_facts f "
        "WHERE f.source_package = %(p)s AND f.fact_table = n.fact_table "
        "AND f.fact_hash = n.fact_hash AND f.retired_on IS NULL)",
        {"on": on, "p": package},
    )


_BITEMPORAL_TABLES = {
    "observation": "sec_ticker_cik_observations",
    "share_count": "sec_cover_share_counts",
    "event": "sec_registration_events",
}


def _reconcile(cur, *, package: str, fact_table: str, temp: str | None,
               columns: tuple[str, ...], availability: str, reconciled_on: dt.date,
               reason: str = SOURCE, key_columns: tuple[str, ...] = (),
               parser_version: str | None = None) -> dict[str, int]:
    """Retire what the package no longer carries (and no other current package
    does); add what it newly carries. Never deletes or overwrites a fact row.

    ``reason`` says why the package's facts differ from the stored ones.
    SOURCE (the public record changed): the retired version keeps its interval,
    and a newly carried fact of an accession any package ever contained for the
    same fact family (current or retired membership) is a correction knowable
    from the later of its filing's public date and the reconciliation date.
    Another family's history does not count (a 10-12B carried by an FSN package
    is first seen as an index event when the index lists it). That includes a
    fact retired earlier and carried again: it is available again from this
    reconciliation, and its retired interval stays. A fact of an accession never
    loaded before is knowable from its filing's public date.
    PARSER_CORRECTION (the same bytes read by another parser version): the retired
    version is marked so (visible at no date) and the new one is knowable from its
    filing's public date. With ``key_columns`` (events), a version that replaces a
    current one of the same key (the same index row read differently) is a parser
    correction whatever ``reason`` says. ``temp`` None carries nothing (a
    superseded package). ``parser_version`` is stamped on inserted rows.
    """
    table = _BITEMPORAL_TABLES[fact_table]
    carried = (f"AND NOT EXISTS (SELECT 1 FROM {temp} n WHERE n.fact_hash = f.fact_hash)"
               if temp else "")
    if temp is not None and key_columns:
        keys = ", ".join(f"n.{c}" for c in key_columns)
        on_keys = " AND ".join(f"t.{c} = n.{c}" for c in key_columns)
        cur.execute(
            f"CREATE TEMP TABLE tmp_sec_corrected ON COMMIT DROP AS "
            f"SELECT DISTINCT {keys} FROM {temp} n JOIN {table} t ON {on_keys} "
            f"WHERE t.retired_on IS NULL AND t.fact_hash <> n.fact_hash "
            f"AND NOT EXISTS (SELECT 1 FROM {table} u "
            f"WHERE u.fact_hash = n.fact_hash AND u.retired_on IS NULL)")

        def corrected(alias: str) -> str:
            match = " AND ".join(f"k.{c} = {alias}.{c}" for c in key_columns)
            return f"EXISTS (SELECT 1 FROM tmp_sec_corrected k WHERE {match})"

        retired_reason = f"CASE WHEN {corrected('t')} THEN '{PARSER_CORRECTION}' ELSE %(reason)s END"
        restated = corrected("n")
    else:
        retired_reason = "%(reason)s"
        restated = "true" if reason == PARSER_CORRECTION else "false"
    cur.execute(
        f"""
        UPDATE {table} t SET retired_on = %(on)s, retired_reason = {retired_reason}
        WHERE t.retired_on IS NULL AND t.fact_hash IN (
            SELECT f.fact_hash FROM sec_ticker_cik_package_facts f
            WHERE f.source_package = %(package)s AND f.fact_table = %(fact_table)s
              AND f.retired_on IS NULL {carried}
              AND NOT EXISTS (
                  SELECT 1 FROM sec_ticker_cik_package_facts o
                  WHERE o.fact_table = f.fact_table AND o.fact_hash = f.fact_hash
                    AND o.source_package <> %(package)s AND o.retired_on IS NULL))
        """,
        {"on": reconciled_on, "package": package, "fact_table": fact_table, "reason": reason},
    )
    retired = cur.rowcount
    if temp is None:
        return {"retired": retired, "inserted": 0}
    stamp = ", parser_version" if parser_version else ""
    cur.execute(
        f"""
        INSERT INTO {table} ({", ".join(columns)}, fact_hash, available_on, loaded_on{stamp})
        SELECT {", ".join(f"n.{c}" for c in columns)}, n.fact_hash,
               CASE WHEN {restated} THEN {availability}
                    WHEN EXISTS (
                        SELECT 1 FROM sec_ticker_cik_package_members m
                        WHERE m.adsh = n.adsh AND m.fact_table = %(fact_table)s)
                    THEN GREATEST({availability}, %(on)s)
                    ELSE {availability}
               END,
               %(on)s{", %(parser)s" if parser_version else ""}
        FROM {temp} n
        WHERE NOT EXISTS (
            SELECT 1 FROM {table} t WHERE t.fact_hash = n.fact_hash AND t.retired_on IS NULL)
        """,
        {"on": reconciled_on, "fact_table": fact_table, "parser": parser_version},
    )
    return {"retired": retired, "inserted": cur.rowcount}


_OBS_AVAILABILITY = "COALESCE(n.accepted::date, n.filed + 1)"
_EVENT_AVAILABILITY = "n.filed + 1"


def load_package(conn, result: PackageResult, *, reconciled_on: dt.date | None = None,
                 validators: tuple[str | None, str | None] | None = None) -> dict[str, int]:
    """Reconcile one package version in a single transaction (bitemporal), and
    record it with ``validators``: the SEC's (ETag, Last-Modified) of the bytes
    parsed (the GET that fetched them, or a --verify-cache HEAD of that file)."""
    package = result.package
    on = reconciled_on or dt.date.today()
    obs_rows = [(*o.as_tuple(), o.fact_hash) for o in result.observations]
    share_rows = [(*s.as_tuple(), s.fact_hash) for s in result.share_counts]
    with conn.transaction():
        with conn.cursor() as cur:
            # The same bytes as the version loaded before: whatever differs is
            # this parser's reading, not the SEC's data (a re-derivation).
            recorded = cur.execute(
                "SELECT package_sha256 FROM sec_ticker_cik_packages WHERE source_package = %s",
                (package,)).fetchone()
            reason = (PARSER_CORRECTION if recorded is not None and recorded[0] == result.sha256
                      else SOURCE)
            cur.execute(
                f"CREATE TEMP TABLE tmp_sec_obs ON COMMIT DROP AS SELECT "
                f"{', '.join(OBSERVATION_COLUMNS)}, fact_hash "
                f"FROM sec_ticker_cik_observations WITH NO DATA"
            )
            cur.execute(
                f"CREATE TEMP TABLE tmp_sec_shares ON COMMIT DROP AS SELECT "
                f"{', '.join(SHARE_COLUMNS)}, fact_hash "
                f"FROM sec_cover_share_counts WITH NO DATA"
            )
            _copy(cur, "tmp_sec_obs", (*OBSERVATION_COLUMNS, "fact_hash"), obs_rows)
            _copy(cur, "tmp_sec_shares", (*SHARE_COLUMNS, "fact_hash"), share_rows)
            observations = _reconcile(
                cur, package=package, fact_table="observation", temp="tmp_sec_obs",
                columns=OBSERVATION_COLUMNS, availability=_OBS_AVAILABILITY,
                reconciled_on=on, reason=reason, parser_version=FSN_PARSER_VERSION,
            )
            shares = _reconcile(
                cur, package=package, fact_table="share_count", temp="tmp_sec_shares",
                columns=SHARE_COLUMNS, availability=_OBS_AVAILABILITY, reconciled_on=on,
                reason=reason, parser_version=FSN_PARSER_VERSION,
            )
            _replace_facts(cur, package, [
                *(("observation", o.fact_hash) for o in result.observations),
                *(("share_count", s.fact_hash) for s in result.share_counts),
            ], on)
            _replace_members(cur, package, [
                *(("observation", adsh, submission.cik)
                  for adsh, submission in result.submissions.items()),
                *(("share_count", adsh, submission.cik)
                  for adsh, submission in result.submissions.items()),
                *(("observation", o.adsh, o.cik) for o in result.observations),
                *(("share_count", s.adsh, s.cik) for s in result.share_counts),
            ], on)
            _record_package(
                cur, package=package, sha256=result.sha256, size=result.size_bytes,
                submissions=len(result.submissions), symbol_facts=result.symbol_facts,
                observations=len(result.observations), share_counts=len(result.share_counts),
                events=0, rejected=result.rejected, validators=validators,
                parser_version=FSN_PARSER_VERSION,
            )
    return {
        "reconciled_as": reason,
        "inserted": observations["inserted"],
        "retired": observations["retired"],
        "shares_inserted": shares["inserted"],
        "shares_retired": shares["retired"],
    }


def supersede_monthly_packages(conn, quarterly: str, *,
                               reconciled_on: dt.date | None = None) -> dict[str, object]:
    """After quarterly package ``quarterly`` is loaded, retire the monthly packages
    of its months: each of their facts retires on ``reconciled_on`` unless the
    quarterly or another current package carries it, their memberships retire,
    and the package rows record superseded_by/superseded_on."""
    covered = quarter_months(quarterly)
    if covered is None:
        return {"superseded": []}
    year, months = covered
    on = reconciled_on or dt.date.today()
    names = [name for (name,) in conn.execute(
        "SELECT source_package FROM sec_ticker_cik_packages WHERE superseded_by IS NULL "
        "AND source_package <> %s", (quarterly,)).fetchall()
        if (m := PACKAGE_RE.match(name)) and m.group("month")
        and int(m.group("year")) == year and int(m.group("month")) in months]
    counts: Counter = Counter()
    for name in sorted(names):
        with conn.transaction():
            with conn.cursor() as cur:
                for fact_table, key in (("observation", "superseded_retired"),
                                        ("share_count", "superseded_shares_retired")):
                    counts[key] += _reconcile(
                        cur, package=name, fact_table=fact_table, temp=None,
                        columns=(), availability="", reconciled_on=on)["retired"]
                cur.execute("UPDATE sec_ticker_cik_package_facts SET retired_on = %s "
                            "WHERE source_package = %s AND retired_on IS NULL", (on, name))
                cur.execute("UPDATE sec_ticker_cik_package_members SET retired_on = %s "
                            "WHERE source_package = %s AND retired_on IS NULL", (on, name))
                cur.execute("UPDATE sec_ticker_cik_packages SET superseded_by = %s, "
                            "superseded_on = %s WHERE source_package = %s",
                            (quarterly, on, name))
    return {"superseded": sorted(names), **counts}


def resume_supersession(conn, *, reconciled_on: dt.date | None = None) -> list[dict]:
    """Supersede the monthly packages still active under every loaded quarterly
    package (idempotent). Runs first in every run, so a run that stopped between
    a quarterly's load and its supersession is completed; each month's facts
    retire on the date the supersession actually runs."""
    done = []
    for (name,) in conn.execute(
        "SELECT source_package FROM sec_ticker_cik_packages WHERE superseded_by IS NULL "
        "ORDER BY source_package"
    ).fetchall():
        if quarter_months(name) is None:
            continue
        item = supersede_monthly_packages(conn, name, reconciled_on=reconciled_on)
        if item["superseded"]:
            done.append({"package": name, **item})
    return done


def record_remote_validators(conn, package: str, *, etag: str | None,
                             last_modified: str | None) -> None:
    """The SEC's ETag / Last-Modified of the loaded package version, when a fresh
    download proved the loaded version current (its SHA-256 matched)."""
    conn.execute(
        "UPDATE sec_ticker_cik_packages SET remote_etag = %s, remote_last_modified = %s "
        "WHERE source_package = %s", (etag, last_modified, package),
    )


def superseded_by(conn, name: str) -> str | None:
    """The current quarterly package that supersedes monthly package ``name``."""
    quarter = covering_quarter(name)
    if quarter is None:
        return None
    row = conn.execute(
        "SELECT source_package FROM sec_ticker_cik_packages WHERE superseded_by IS NULL "
        "AND source_package LIKE %s ORDER BY source_package LIMIT 1", (f"{quarter}_notes%",),
    ).fetchone()
    return row[0] if row else None


def load_form_index(conn, path: Path, *, reconciled_on: dt.date | None = None,
                    documents: EventDocuments | None = None,
                    ciks: set[int] | None = None) -> dict[str, object]:
    """Reconcile one quarterly index version in a single transaction (bitemporal).

    An event the previous version listed and this one does not (removed, or its
    CIK corrected) is retired unless another loaded index still lists it. With
    ``documents``, the end filings of CIKs with cover data (``ciks``, default:
    all with current observations) are read for their class first.
    """
    events, sha256, size = parse_form_index(path)
    if documents is not None and ciks is None:
        ciks = cover_ciks(conn)
    known = derived_events(conn, (e.adsh for e in events))
    events, classes = describe_events(events, documents, ciks or set(), known)
    package = path.name
    on = reconciled_on or dt.date.today()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TEMP TABLE tmp_sec_events ON COMMIT DROP AS SELECT "
                f"{', '.join(EVENT_COLUMNS)}, fact_hash FROM sec_registration_events WITH NO DATA"
            )
            _copy(cur, "tmp_sec_events", (*EVENT_COLUMNS, "fact_hash"),
                  ((*e.as_tuple(), e.fact_hash) for e in events))
            counts = _reconcile(
                cur, package=package, fact_table="event", temp="tmp_sec_events",
                columns=EVENT_COLUMNS, availability=_EVENT_AVAILABILITY, reconciled_on=on,
                key_columns=("adsh", "cik", "form", "filed"),
            )
            _replace_facts(cur, package, (("event", e.fact_hash) for e in events), on)
            _replace_members(cur, package, (("event", e.adsh, e.cik) for e in events), on)
            _record_package(
                cur, package=package, sha256=sha256, size=size, submissions=0,
                symbol_facts=0, observations=0, share_counts=0, events=len(events),
                rejected=Counter(), parser_version=EVENT_PARSER_VERSION,
            )
    return {
        "package": package,
        "events": len(events),
        "forms": dict(Counter(e.form for e in events).most_common()),
        **{key: classes.get(key, 0) for key in CLASS_STAT_KEYS},
        **counts,
    }


def derive_event_classes(conn, documents: EventDocuments, *,
                         reconciled_on: dt.date | None = None,
                         ciks: set[int] | None = None) -> dict[str, int]:
    """Re-derive the class of current end and 8-A events of CIKs with cover data
    whose filing was not read, or was read by another parser version.

    A changed event is a parser correction (the filing did not change, our
    reading did): its version is retired as such (visible at no date) and the
    re-derived one is knowable from the filing date + 1; the indexes that carried
    the old version carry the new one.
    """
    on = reconciled_on or dt.date.today()
    ciks = cover_ciks(conn) if ciks is None else ciks
    rows = conn.execute(
        f"SELECT {', '.join(EVENT_COLUMNS)}, fact_hash FROM sec_registration_events "
        "WHERE retired_on IS NULL AND form = ANY(%s) AND parser_version IS DISTINCT FROM %s",
        (sorted(READ_EVENT_FORMS), EVENT_PARSER_VERSION),
    ).fetchall()
    stats: Counter = Counter()
    changed: list[tuple[str, RegistrationEvent]] = []
    for row in rows:
        event = RegistrationEvent(**dict(zip(EVENT_COLUMNS, row[:-1])))
        if event.cik not in ciks:
            continue
        (derived,), classes = describe_events([event], documents, ciks)
        stats.update(classes)
        if (derived.parser_version is not None and classes.get("filings_missing", 0) == 0
                and derived.fact_hash != row[-1]):
            changed.append((row[-1], derived))
    with conn.transaction():
        with conn.cursor() as cur:
            for old_hash, event in changed:
                cur.execute(
                    "UPDATE sec_registration_events SET retired_on = %s, retired_reason = %s "
                    "WHERE fact_hash = %s AND retired_on IS NULL",
                    (on, PARSER_CORRECTION, old_hash),
                )
                cur.execute(
                    f"INSERT INTO sec_registration_events "
                    f"({', '.join(EVENT_COLUMNS)}, fact_hash, available_on, loaded_on) "
                    f"SELECT {', '.join(['%s'] * len(EVENT_COLUMNS))}, %s, %s::date + 1, %s "
                    f"WHERE NOT EXISTS (SELECT 1 FROM sec_registration_events "
                    f"WHERE fact_hash = %s AND retired_on IS NULL)",
                    (*event.as_tuple(), event.fact_hash, event.filed, on, event.fact_hash),
                )
                cur.execute(
                    "INSERT INTO sec_ticker_cik_package_facts (source_package, fact_table, "
                    "fact_hash, loaded_on) SELECT f.source_package, f.fact_table, %s, %s "
                    "FROM sec_ticker_cik_package_facts f "
                    "WHERE f.fact_table = 'event' AND f.fact_hash = %s AND f.retired_on IS NULL "
                    "AND NOT EXISTS (SELECT 1 FROM sec_ticker_cik_package_facts g "
                    "WHERE g.source_package = f.source_package AND g.fact_table = 'event' "
                    "AND g.fact_hash = %s AND g.retired_on IS NULL)",
                    (event.fact_hash, on, old_hash, event.fact_hash),
                )
                cur.execute(
                    "UPDATE sec_ticker_cik_package_facts SET retired_on = %s "
                    "WHERE fact_table = 'event' AND fact_hash = %s AND retired_on IS NULL",
                    (on, old_hash),
                )
    return {
        "derived": len(changed),
        **{key: stats.get(key, 0) for key in CLASS_STAT_KEYS},
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
    """The listed DERA package URLs. A response other than 200, or a page without a
    package link, is an error: an empty listing must never read as "nothing new"."""
    listing = client.get(LISTING_URL)
    listing.raise_for_status()
    if listing.status_code != 200:
        raise RuntimeError(f"DERA listing answered {listing.status_code}: {LISTING_URL}")
    time.sleep(DOWNLOAD_SPACING_S)
    urls = listed_package_urls(listing.text)
    if not urls:
        raise RuntimeError(f"DERA listing has no package links: {LISTING_URL}")
    return urls


def fetch_package(client, url: str, target: Path) -> tuple[str | None, str | None]:
    """Stream one FSN zip to disk; it replaces ``target`` only once it opens.
    Returns the SEC's (ETag, Last-Modified) of the bytes fetched."""
    partial = target.with_name(target.name + ".part")
    with client.stream("GET", url) as response:
        response.raise_for_status()
        validators = (response.headers.get("etag"), response.headers.get("last-modified"))
        with partial.open("wb") as fh:
            for chunk in response.iter_bytes(1 << 20):
                fh.write(chunk)
    with zipfile.ZipFile(partial):
        pass
    partial.replace(target)
    time.sleep(DOWNLOAD_SPACING_S)
    return validators


def download_packages(packages_dir: Path) -> list[Path]:
    """Fetch every listed FSN package not already present (sequential, SEC UA)."""
    packages_dir.mkdir(parents=True, exist_ok=True)
    fetched: list[Path] = []
    with sec_client() as client:
        for url in list_package_urls(client):
            target = packages_dir / url.rsplit("/", 1)[1]
            if target.exists():
                continue
            fetch_package(client, url, target)
            fetched.append(target)
            print(json.dumps({"downloaded": target.name, "bytes": target.stat().st_size}))
    return fetched


VALIDATORS_FILE = "validators.json"


def verify_package_cache(
    client, packages_dir: Path, paths: Iterable[Path] = (),
) -> dict[Path, tuple[str | None, str | None]]:
    """Before a load from a workstation cache: HEAD every listed package and fetch
    again any cached zip that is missing, whose size differs, whose ETag differs
    from the one recorded when it was fetched, or whose Last-Modified is newer
    than the cached file. The zip checked is the one the load will read: a path
    named on the command line (``paths``), else ``packages_dir/<name>``. Returns
    the fresh (ETag, Last-Modified) per verified path (resolved), the only
    validators the load records; prints how many were fetched again."""
    from email.utils import parsedate_to_datetime

    packages_dir.mkdir(parents=True, exist_ok=True)
    named: dict[str, Path] = {}
    for path in paths:
        if named.setdefault(path.name, path).resolve() != path.resolve():
            raise ValueError(f"two packages named {path.name}: {named[path.name]}, {path}")
    sidecars: dict[Path, dict] = {}
    validators: dict[Path, tuple[str | None, str | None]] = {}
    reasons: Counter = Counter()
    listed = [url for url in list_package_urls(client) if PACKAGE_RE.match(url.rsplit("/", 1)[1])]
    for url in listed:
        name = url.rsplit("/", 1)[1]
        target = named.get(name, packages_dir / name)
        if target.parent not in sidecars:
            try:
                sidecars[target.parent] = json.loads(
                    (target.parent / VALIDATORS_FILE).read_text(encoding="utf-8"))
            except (OSError, ValueError):
                sidecars[target.parent] = {}
        sidecar = sidecars[target.parent]
        head = client.head(url)
        head.raise_for_status()
        time.sleep(DOWNLOAD_SPACING_S)
        length = head.headers.get("content-length")
        size = int(length) if length else None
        etag, modified = head.headers.get("etag"), head.headers.get("last-modified")
        reason = None
        if not target.exists():
            reason = "missing"
        elif size is not None and size != target.stat().st_size:
            reason = "size"
        elif etag and sidecar.get(name, {}).get("etag") not in (None, etag):
            reason = "etag"
        elif modified:
            stamp = parsedate_to_datetime(modified)
            cached = dt.datetime.fromtimestamp(target.stat().st_mtime, dt.timezone.utc)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=dt.timezone.utc)
            if stamp > cached:
                reason = "last_modified"
        if reason:
            target.parent.mkdir(parents=True, exist_ok=True)
            etag, modified = fetch_package(client, url, target)  # those of the bytes fetched
            reasons[reason] += 1
        sidecar[name] = {"etag": etag, "last_modified": modified, "size": size}
        validators[target.resolve()] = (etag, modified)
    for directory, sidecar in sidecars.items():
        (directory / VALIDATORS_FILE).write_text(json.dumps(sidecar, indent=1, sort_keys=True),
                                                 encoding="utf-8")
    print(json.dumps({"verify_cache": {"listed": len(listed),
                                       "fetched_again": sum(reasons.values()),
                                       **dict(sorted(reasons.items()))}}), flush=True)
    return validators


def discover_packages(packages_dir: Path) -> list[Path]:
    found = [path for path in packages_dir.glob("*_notes*.zip") if PACKAGE_RE.match(path.name)]
    return sorted(found, key=package_sort_key)


def listed_packages(packages: Iterable[Path], validators: dict[Path, object]) -> list[Path]:
    """After --verify-cache: only the package files the SEC lists and that were
    just verified, by path, load. Any other zip, such as a package the SEC no
    longer lists, is ignored and logged."""
    packages = list(packages)
    unlisted = sorted(path.name for path in packages if path.resolve() not in validators)
    if unlisted:
        print(json.dumps({"ignored_unlisted_packages": unlisted}), flush=True)
    return [path for path in packages if path.resolve() in validators]


def run(
    packages: Iterable[Path],
    *,
    dsn: str | None,
    dry_run: bool,
    form_indexes: Iterable[Path] = (),
    reconciled_on: dt.date | None = None,
    documents: EventDocuments | None = None,
    validators: dict[Path, tuple[str | None, str | None]] | None = None,
) -> list[dict[str, object]]:
    """Parse and reconcile packages, then indexes (reading end filings with
    ``documents``), then re-derive end events read by another parser version;
    ``reconciled_on`` dates every retirement and correction (default: today)."""
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
            for item in resume_supersession(conn, reconciled_on=reconciled_on):
                print(json.dumps(item), flush=True)
                stats.append(item)
        for path in packages:
            quarterly = superseded_by(conn, path.name) if conn is not None else None
            if quarterly is not None:
                item = {"package": path.name, "skipped": f"superseded by {quarterly}"}
                print(json.dumps(item), flush=True)
                stats.append(item)
                continue
            result = parse_package(path)
            item = result.stats()
            if conn is not None:
                started = time.monotonic()
                item.update(load_package(conn, result, reconciled_on=reconciled_on,
                                         validators=(validators or {}).get(path.resolve())))
                item.update(supersede_monthly_packages(conn, path.name,
                                                       reconciled_on=reconciled_on))
                item["load_seconds"] = round(time.monotonic() - started, 1)
            print(json.dumps(item), flush=True)
            stats.append(item)
        ciks = cover_ciks(conn) if conn is not None and documents is not None else None
        for path in form_indexes:
            if conn is None:
                events, _, _ = parse_form_index(path)
                item = {"package": path.name, "events": len(events)}
            else:
                item = load_form_index(conn, path, reconciled_on=reconciled_on,
                                       documents=documents, ciks=ciks)
            print(json.dumps(item), flush=True)
            stats.append(item)
        if conn is not None and documents is not None:
            item = {"package": "derive_event_classes",
                    **derive_event_classes(conn, documents, reconciled_on=reconciled_on,
                                           ciks=ciks),
                    "filings_fetched": documents.fetched, "filings_failed": documents.failed,
                    "filings_rejected": documents.rejected}
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
    parser.add_argument("--verify-cache", action="store_true",
                        help="HEAD every listed package first; fetch again any cached zip "
                             "that is missing, changed size/ETag or is older than its "
                             "Last-Modified; record only those fresh validators; load "
                             "only listed packages (unlisted zips are ignored, logged)")
    parser.add_argument("--no-events", action="store_true",
                        help="skip the EDGAR registration events")
    parser.add_argument("--event-docs-dir", type=Path, default=DEFAULT_EVENT_DOCS_DIR,
                        help="cache of the end filings read for their class")
    parser.add_argument("--no-fetch", action="store_true",
                        help="read only end filings already in --event-docs-dir")
    parser.add_argument("--dsn", default=None, help="Database DSN; defaults to DATABASE_URL")
    parser.add_argument("--dry-run", action="store_true", help="parse and report; no database")
    parser.add_argument("--apply-schema", action="store_true",
                        help="local/dev: apply the schema first")
    parser.add_argument("--reconciled-on", type=dt.date.fromisoformat, default=None,
                        help="date of retirements and corrections (default: today)")
    args = parser.parse_args(argv)

    validators = None
    if args.verify_cache:
        with sec_client() as verifier:
            validators = verify_package_cache(verifier, args.packages_dir, args.packages)
    if args.download:
        download_packages(args.packages_dir)
        if not args.no_events:
            download_form_indexes(args.index_dir)
    packages = (
        sorted(args.packages, key=package_sort_key)
        if args.packages else discover_packages(args.packages_dir)
    )
    if validators is not None:
        packages = listed_packages(packages, validators)
    if not packages:
        parser.error(f"no FSN packages found in {args.packages_dir}")
    form_indexes = (
        [] if args.no_events
        else sorted(args.index_dir.glob("*.form.gz"), key=index_sort_key)
    )
    if args.apply_schema and not args.dry_run:
        apply_schema(args.dsn)
    started = time.monotonic()
    client = None if args.dry_run or args.no_events or args.no_fetch else sec_client()
    documents = None if args.dry_run or args.no_events else EventDocuments(
        args.event_docs_dir, client)
    try:
        stats = run(packages, dsn=args.dsn, dry_run=args.dry_run, form_indexes=form_indexes,
                    reconciled_on=args.reconciled_on, documents=documents,
                    validators=validators)
    finally:
        if client is not None:
            client.close()
    totals: Counter = Counter()
    for item in stats:
        for key in ("submissions", "symbol_facts", "observations", "share_counts", "events",
                    "inserted", "retired", "shares_inserted", "shares_retired", "derived",
                    *FETCH_STAT_KEYS, *CLASS_STAT_KEYS):
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
