"""Generate and verify a governed XNYS current-daily NAV policy without applying it.

The CLI keeps its v1 filename so existing invocations do not break, but it
implements the current contract ``fund-nav-policy-generator-v2``: identity is
registry + ticker + series with optional, strictly validated ISIN/CUSIP/FIGI
claims (``registry-ticker-series-claims-v2``). v1 artifacts are rejected.

Run from the repository root as ``python -m scripts.generate_fund_nav_policy_v1
{calendar,build,verify} ...`` so the package root is importable.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.metadata
import json
import os
import re
import secrets
import stat
import string
import sys
import tempfile
import uuid
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

from scripts.fund_nav_readiness_schema import _policy
from src.workers._nav_policy import (
    ADJUSTED_OVERLAP_ABS_TOL,
    ADJUSTED_OVERLAP_REL_TOL,
    CATALOG_EVIDENCE_REFERENCE,
    CATALOG_SOURCE_RELATIONS,
    CLAIM_FAILURE_FAMILIES,
    CURRENT_CATALOG_QUERY_VERSION,
    FUNDS_QUERY,
    GENERATOR_VERSION,
    IDENTITY_FAILURE_CODES,
    IDENTITY_QUERY,
    INSTRUMENTS_QUERY,
    ISIN_PRESENCE_CATEGORIES,
    PROVIDER_CONTRACT_VERSION,
    SOURCE_QUERY_SHA256,
    calendar_digest,
    generation_metadata_digest,
    instrument_evidence_digest,
    policy_content_digest,
    uuid_set_digest,
)
from src.workers._nav_sanitize import REPAIRED_NAV_KINDS

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_VERSION = "4.13.2"
CALENDAR_NAME = "XNYS"
CALENDAR_SOURCE = "exchange_calendars/XNYS"
NY = ZoneInfo("America/New_York")
UTC = dt.timezone.utc
ICI_REFERENCE_URL = "https://www.ici.org/faqs/faq/mfs/faqs_navs"
SOURCE_REFERENCE = (
    f"exchange_calendars=={PACKAGE_VERSION}/XNYS; "
    f"ICI {ICI_REFERENCE_URL} (6:05 p.m. ET Nasdaq delivery context); "
    "18:05 ET is this policy's operational due time, not a fund-specific legal deadline"
)
PROVIDER_CONTRACT = PROVIDER_CONTRACT_VERSION
EVIDENCE_REFERENCE = CATALOG_EVIDENCE_REFERENCE
SUPPORTED_DAILY_FUND_TYPES = frozenset({"etf", "mutual_fund"})
KNOWN_FUND_TYPES = SUPPORTED_DAILY_FUND_TYPES | {"mmf"}
MAX_SOURCE_ROWS = 100_000
ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]*\Z")
EVIDENCE_FIELDS = frozenset(
    {
        "instrument_id",
        "known_at",
        "effective_at",
        "fund_status",
        "valuation_frequency",
        "identity_verified",
        "return_basis_verified",
        "currency_verified",
        "evidence_reference",
    }
)
# Exact projections of the three canonical queries (same column order).
SOURCE_FIELDS = {
    "instruments": (
        "instrument_id",
        "instrument_type",
        "ticker",
        "isin",
        "currency",
        "is_active",
    ),
    "funds": (
        "instrument_id",
        "series_id",
        "ticker",
        "isin",
        "cusip",
        "currency",
        "fund_type",
    ),
    "identity": (
        "instrument_id",
        "sec_series_id",
        "sec_class_id",
        "ticker",
        "isin",
        "cusip_9",
        "figi",
        "resolution_status",
        "conflict_state",
    ),
}
BOOLEAN_SOURCE_FIELDS = frozenset({"is_active"})
JSON_SOURCE_FIELDS = frozenset({"conflict_state"})
SOURCE_SNAPSHOT_KIND = "nav_policy_source_snapshot_v2"
_UUID_TEXT = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\Z"
)
_ASCII_UPPER = str.maketrans(string.ascii_lowercase, string.ascii_uppercase)
# Claim formats (ASCII classes only; ``[0-9]`` never matches Unicode digits).
_US_ISIN = re.compile(r"US[A-Z0-9]{9}[0-9]\Z")
_CUSIP9 = re.compile(r"[A-Z0-9*@#]{8}[0-9]\Z")
_FIGI = re.compile(r"[B-DF-HJ-NP-TV-Z]{2}G[B-DF-HJ-NP-TV-Z0-9]{8}[0-9]\Z")
_FIGI_RESERVED_PREFIXES = frozenset({"BS", "BM", "GG", "GB", "GH", "KY", "VG"})
_CUSIP_SPECIAL_VALUES = {"*": 36, "@": 37, "#": 38}


class PolicyGenerationError(ValueError):
    """A static, sanitized reason; provider responses and DSNs never enter it."""


def canonical_json(value: dict) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("ascii")


def _unique_object(pairs: list) -> dict:
    keys = [key for key, _value in pairs]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate_key")
    return dict(pairs)


def _reject_constant(_name: str):
    raise ValueError("non_finite_constant")


def load_json_object(raw: bytes, code: str) -> dict:
    """Strict JSON object: no duplicate keys, NaN/Infinity or non-object root.

    Any violation raises ``PolicyGenerationError(code)``; nothing of the
    payload reaches the error.
    """
    try:
        value = json.loads(
            raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
    except (ValueError, UnicodeDecodeError, RecursionError) as exc:
        raise PolicyGenerationError(code) from exc
    if not isinstance(value, dict):
        raise PolicyGenerationError(code)
    return value


def _calendar():
    try:
        installed_version = importlib.metadata.version("exchange_calendars")
    except importlib.metadata.PackageNotFoundError as exc:
        raise PolicyGenerationError("calendar_package_unavailable") from exc
    if installed_version != PACKAGE_VERSION:
        raise PolicyGenerationError("calendar_package_version_mismatch")
    try:
        import exchange_calendars as xcals
    except ImportError as exc:
        raise PolicyGenerationError("calendar_package_unavailable") from exc

    if xcals.__version__ != PACKAGE_VERSION:
        raise PolicyGenerationError("calendar_package_version_mismatch")
    return xcals


@lru_cache(maxsize=8)
def _sessions_for_range(
    requested_start: dt.date, requested_end: dt.date
) -> tuple[tuple[dt.date, dt.datetime, dt.datetime, str], ...]:
    import exchange_calendars as xcals

    try:
        calendar = xcals.get_calendar(
            CALENDAR_NAME,
            start=requested_start.isoformat(),
            end=requested_end.isoformat(),
        )
        # sessions_in_range rejects a requested holiday before first_session.
        sessions = calendar.sessions
    except (ValueError, OverflowError) as exc:
        raise PolicyGenerationError("calendar_coverage_unavailable") from exc
    if len(sessions) < 401:
        raise PolicyGenerationError("calendar_window_too_short")
    prepared = []
    for session in sessions:
        day = session.date()
        close = calendar.session_close(session).to_pydatetime()
        if close.tzinfo is None:
            raise PolicyGenerationError("calendar_close_naive")
        close_utc = close.astimezone(UTC)
        local_close = close_utc.astimezone(NY)
        due = dt.datetime.combine(day, dt.time(18, 5), NY)
        if (
            day < requested_start
            or day > requested_end
            or local_close.date() != day
            or due.date() != day
            or due < local_close
        ):
            raise PolicyGenerationError("calendar_close_or_deadline_invalid")
        prepared.append((day, close_utc, due.astimezone(UTC), SOURCE_REFERENCE))
    return tuple(prepared)


def build_calendar(requested_start: dt.date, requested_end: dt.date) -> dict:
    if requested_start >= requested_end:
        raise PolicyGenerationError("calendar_coverage_invalid")
    _calendar()  # Check the installed distribution on every call, even with cached sessions.
    prepared = _sessions_for_range(requested_start, requested_end)
    digest = calendar_digest(prepared)
    result = {
        "kind": "xnys_nav_calendar_v1",
        "generator_version": GENERATOR_VERSION,
        "publication_state": "unpublished",
        "calendar_id": CALENDAR_NAME,
        "calendar_version": f"exchange_calendars-{PACKAGE_VERSION}-{digest[:16]}-v1",
        "calendar_source": CALENDAR_SOURCE,
        "timezone": "America/New_York",
        "source_reference": SOURCE_REFERENCE,
        "requested_coverage_start": requested_start.isoformat(),
        "requested_coverage_end": requested_end.isoformat(),
        "coverage_start": prepared[0][0].isoformat(),
        "coverage_end": prepared[-1][0].isoformat(),
        "valid_through": prepared[-1][2].isoformat(),
        "calendar_session_count": len(prepared),
        "calendar_digest": digest,
        "sessions": [
            {
                "session_date": day.isoformat(),
                "valuation_close_at": close.isoformat(),
                "nav_due_at": due.isoformat(),
                "source_reference": reference,
            }
            for day, close, due, reference in prepared
        ],
    }
    return result


def _fetch_source(cursor, query: str) -> list[dict]:
    cursor.execute(query)
    rows = cursor.fetchall()
    # Each query carries LIMIT 100001: the sentinel row aborts before classifying.
    if len(rows) > MAX_SOURCE_ROWS:
        raise PolicyGenerationError("catalog_row_limit_exceeded")
    return rows


def _catalog_rows(cursor) -> tuple[list[dict], list[dict], list[dict]]:
    return (
        _fetch_source(cursor, INSTRUMENTS_QUERY),
        _fetch_source(cursor, FUNDS_QUERY),
        _fetch_source(cursor, IDENTITY_QUERY),
    )


def _require_source_privileges(cursor) -> None:
    """SELECT on a view does not imply SELECT on the registry base table.

    A missing relation raises (psycopg UndefinedTable); a missing privilege is
    a static blocked reason. Nothing is granted automatically.
    """
    cursor.execute(
        "SELECT "
        + ",".join(
            f"has_table_privilege(current_user, %s, 'SELECT') AS p{index}"
            for index in range(len(CATALOG_SOURCE_RELATIONS))
        ),
        CATALOG_SOURCE_RELATIONS,
    )
    row = cursor.fetchone()
    if not all(
        row[f"p{index}"] is True for index in range(len(CATALOG_SOURCE_RELATIONS))
    ):
        raise PolicyGenerationError("catalog_source_privilege_missing")


def read_catalog_snapshot(
    dsn: str,
) -> tuple[dt.datetime, list[dict], list[dict], list[dict]]:
    """Pin the three current catalog sources to one snapshot, never assign an xid."""
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            try:
                cursor.execute("SET LOCAL statement_timeout='30s'")
                cursor.execute("SET LOCAL lock_timeout='1s'")
                cursor.execute(
                    "SELECT clock_timestamp() AS decision_at, "
                    "current_setting('transaction_read_only') AS read_only, "
                    "txid_current_if_assigned() AS xid"
                )
                meta = cursor.fetchone()
                if meta["read_only"] != "on" or meta["xid"] is not None:
                    raise PolicyGenerationError("catalog_transaction_not_read_only")
                _require_source_privileges(cursor)
                instruments, funds, identity = _catalog_rows(cursor)
                cursor.execute("SELECT txid_current_if_assigned() AS xid")
                if cursor.fetchone()["xid"] is not None:
                    raise PolicyGenerationError("catalog_transaction_assigned_xid")
                return meta["decision_at"], instruments, funds, identity
            finally:
                cursor.execute("ROLLBACK")


def _canonical_uuid(value: object) -> str:
    """Canonical lowercase UUID text; any other shape is a source error."""
    if isinstance(value, uuid.UUID):
        return str(value)
    if type(value) is str and _UUID_TEXT.fullmatch(value):
        return str(uuid.UUID(value))
    raise PolicyGenerationError("catalog_source_uuid_invalid")


def _check_json_value(value: object) -> None:
    try:
        json.dumps(value, allow_nan=False, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise PolicyGenerationError("catalog_source_type_invalid") from exc


def canonical_source_rows(rows: list[dict], source: str) -> list[dict]:
    """Exact query projection with strict value types; values are NOT normalized.

    ``instrument_id`` becomes canonical lowercase UUID text; text columns must
    be ``str`` or NULL (never ``str()`` of another object); ``is_active`` a real
    boolean or NULL; ``conflict_state`` any JSON value (classified later, a
    non-object is not an empty conflict). Violations abort the whole build.
    """
    fields = SOURCE_FIELDS[source]
    result = []
    for row in rows:
        try:
            values = {field: row[field] for field in fields}
        except (KeyError, TypeError) as exc:
            raise PolicyGenerationError("catalog_source_schema_invalid") from exc
        values["instrument_id"] = _canonical_uuid(values["instrument_id"])
        for field in fields[1:]:
            value = values[field]
            if field in BOOLEAN_SOURCE_FIELDS:
                if value is not None and type(value) is not bool:
                    raise PolicyGenerationError("catalog_source_type_invalid")
            elif field in JSON_SOURCE_FIELDS:
                _check_json_value(value)
            elif value is not None and type(value) is not str:
                raise PolicyGenerationError("catalog_source_type_invalid")
        result.append(values)
    return result


def _row_key(row: dict) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def source_snapshot_content(
    instruments: list[dict], funds: list[dict], identity: list[dict]
) -> dict:
    """Canonically ordered source rows, multiplicity preserved (no dedup)."""
    return {
        "funds": sorted(funds, key=_row_key),
        "identity": sorted(identity, key=_row_key),
        "instruments": sorted(instruments, key=_row_key),
    }


def _text(value: str | None) -> str | None:
    """Outer trim; empty or whitespace-only text is absence."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _identifier(value: str | None) -> str | None:
    """Trim + ASCII uppercase only: no transliteration, no interior edits."""
    text = _text(value)
    return text.translate(_ASCII_UPPER) if text else None


def _cusip_value(char: str) -> int:
    if "0" <= char <= "9":
        return ord(char) - ord("0")
    if "A" <= char <= "Z":
        return ord(char) - ord("A") + 10
    return _CUSIP_SPECIAL_VALUES[char]


def _weighted_digit_sum(values: list[int]) -> int:
    """Weights 1,2,1,2… from the left; sum the decimal digits of each product."""
    total = 0
    for position, value in enumerate(values):
        product = value * (2 if position % 2 else 1)
        total += product // 10 + product % 10
    return total


def cusip_problem(cusip: str) -> str | None:
    if not _CUSIP9.fullmatch(cusip):
        return "invalid_format"
    check = (10 - _weighted_digit_sum([_cusip_value(c) for c in cusip[:8]]) % 10) % 10
    return None if check == int(cusip[8]) else "checksum"


def isin_problem(isin: str) -> str | None:
    """Only US ISINs are supported claims; the embedded CUSIP must also pass."""
    if not isin.startswith("US"):
        return "unsupported_prefix"
    if not _US_ISIN.fullmatch(isin):
        return "invalid_format"
    if cusip_problem(isin[2:11]) is not None:
        return "checksum"
    expanded = "".join(
        str(ord(char) - ord("A") + 10) if "A" <= char <= "Z" else char for char in isin
    )
    luhn = 0
    for offset, digit in enumerate(reversed(expanded)):
        value = int(digit) * (2 if offset % 2 else 1)
        luhn += value - 9 if value > 9 else value
    return None if luhn % 10 == 0 else "checksum"


def figi_problem(figi: str) -> str | None:
    if not _FIGI.fullmatch(figi):
        return "invalid_format"
    if figi[:2] in _FIGI_RESERVED_PREFIXES:
        return "reserved_prefix"
    values = [
        ord(char) - ord("0") if "0" <= char <= "9" else ord(char) - ord("A") + 10
        for char in figi[:11]
    ]
    check = (10 - _weighted_digit_sum(values) % 10) % 10
    return None if check == int(figi[11]) else "checksum"


def _derived_cusip(isin: str | None) -> str | None:
    """CUSIP9 of every structurally extractable US ISIN, even with a bad checksum."""
    if isin is not None and len(isin) == 12 and isin.startswith("US"):
        return isin[2:11]
    return None


class _CatalogIndex:
    """Per-UUID source rows plus global claim → owner-UUID indexes.

    Claims are indexed over every row of every source (all asset types,
    candidates, inactive and registry-only rows) so a concurrent claimant
    blocks all of its owners, never just the "second" one.
    """

    def __init__(
        self, instruments: list[dict], funds: list[dict], identity: list[dict]
    ):
        self.iu: dict[str, list[dict]] = defaultdict(list)
        self.funds: dict[str, list[dict]] = defaultdict(list)
        self.registry: dict[str, list[dict]] = defaultdict(list)
        self.owners: dict[tuple, set[str]] = defaultdict(set)
        for row in instruments:
            owner = row["instrument_id"]
            self.iu[owner].append(row)
            self._claim(owner, "ticker", _identifier(row["ticker"]))
            self._claim_isin(owner, _identifier(row["isin"]))
        for row in identity:
            owner = row["instrument_id"]
            self.registry[owner].append(row)
            ticker = _identifier(row["ticker"])
            self._claim(owner, "ticker", ticker)
            self._claim_pair(owner, _identifier(row["sec_series_id"]), ticker)
            self._claim_isin(owner, _identifier(row["isin"]))
            self._claim(owner, "cusip", _identifier(row["cusip_9"]))
            self._claim(owner, "figi", _identifier(row["figi"]))
        for row in funds:
            owner = row["instrument_id"]
            self.funds[owner].append(row)
            ticker = _identifier(row["ticker"])
            self._claim(owner, "ticker", ticker)
            self._claim_pair(owner, _identifier(row["series_id"]), ticker)
            self._claim_isin(owner, _identifier(row["isin"]))
            self._claim(owner, "cusip", _identifier(row["cusip"]))

    def _claim(self, owner: str, kind: str, value: object) -> None:
        if value is not None:
            self.owners[(kind, value)].add(owner)

    def _claim_pair(self, owner: str, series: str | None, ticker: str | None) -> None:
        if series is not None and ticker is not None:
            self.owners[("series_ticker", (series, ticker))].add(owner)

    def _claim_isin(self, owner: str, isin: str | None) -> None:
        self._claim(owner, "isin", isin)
        self._claim(owner, "cusip", _derived_cusip(isin))

    def sole_owner(self, owner: str, kind: str, value: object) -> bool:
        return self.owners.get((kind, value), set()) == {owner}

    def universe(self) -> list[str]:
        return sorted(
            set(self.funds)
            | {
                owner
                for owner, rows in self.iu.items()
                if any(_text(row["instrument_type"]) == "fund" for row in rows)
            }
        )


def _assert_projection_consistent(index: _CatalogIndex) -> None:
    """funds_v must project the registry exactly wherever one registry row exists.

    Any divergent funds_v row (series/ticker/ISIN/CUSIP after normalization,
    NULL versus value included) aborts the whole artifact: a view that
    disagrees with its base table is a catalog defect, not an UNKNOWN fund.
    """
    for owner, registry_rows in index.registry.items():
        if len(registry_rows) != 1:
            continue
        registry = registry_rows[0]
        expected = (
            _identifier(registry["sec_series_id"]),
            _identifier(registry["ticker"]),
            _identifier(registry["isin"]),
            _identifier(registry["cusip_9"]),
        )
        for fund in index.funds.get(owner, ()):
            if (
                _identifier(fund["series_id"]),
                _identifier(fund["ticker"]),
                _identifier(fund["isin"]),
                _identifier(fund["cusip"]),
            ) != expected:
                raise PolicyGenerationError("catalog_identity_projection_mismatch")


def _is_inactive(index: _CatalogIndex, owner: str) -> bool:
    """Unchanged v1 rule: exactly one IU row, is_active False, no funds_v row."""
    iu_rows = index.iu.get(owner, [])
    return (
        len(iu_rows) == 1
        and iu_rows[0]["is_active"] is False
        and not index.funds.get(owner)
    )


def _first_failure(
    index: _CatalogIndex, owner: str, *, claims: bool = True
) -> str | None:
    """The first failing gate in contract precedence, or None when ACTIVE.

    ``claims=False`` skips the ISIN/CUSIP/FIGI families (structural potential).
    """
    for family, rows in (
        ("iu", index.iu.get(owner, [])),
        ("funds_v", index.funds.get(owner, [])),
        ("registry", index.registry.get(owner, [])),
    ):
        if not rows:
            return f"cardinality.{family}_missing"
        if len(rows) > 1:
            return f"cardinality.{family}_duplicate"
    iu = index.iu[owner][0]
    fund = index.funds[owner][0]
    registry = index.registry[owner][0]
    if _text(registry["resolution_status"]) != "canonical":
        return "registry.status_not_canonical"
    conflict = registry["conflict_state"]
    if type(conflict) is not dict or conflict:
        return "registry.conflict_state_not_empty"
    if _text(iu["instrument_type"]) != "fund":
        return "instrument_type.not_fund"
    tickers = {
        _identifier(iu["ticker"]),
        _identifier(fund["ticker"]),
        _identifier(registry["ticker"]),
    }
    if None in tickers:
        return "ticker.missing"
    if len(tickers) != 1:
        return "ticker.mismatch"
    (ticker,) = tickers
    if not index.sole_owner(owner, "ticker", ticker):
        return "ticker.global_conflict"
    series = {_identifier(registry["sec_series_id"]), _identifier(fund["series_id"])}
    if None in series:
        return "series.missing"
    if len(series) != 1:
        return "series.mismatch"
    (series_id,) = series
    if not index.sole_owner(owner, "series_ticker", (series_id, ticker)):
        return "series.global_conflict"
    if claims:
        # funds_v ISIN/CUSIP equal the registry here (projection preflight).
        isins = [
            value
            for value in (_identifier(iu["isin"]), _identifier(registry["isin"]))
            if value is not None
        ]
        problems = {isin_problem(isin) for isin in isins}
        # Rank order inside the family, independent of which source is listed first.
        for problem in ("unsupported_prefix", "invalid_format", "checksum"):
            if problem in problems:
                return f"isin.{problem}"
        if len(set(isins)) > 1:
            return "isin.mismatch"
        if any(not index.sole_owner(owner, "isin", isin) for isin in set(isins)):
            return "isin.global_conflict"
        explicit = _identifier(registry["cusip_9"])
        if explicit is not None:
            problem = cusip_problem(explicit)
            if problem is not None:
                return f"cusip.{problem}"
        cusips = {isin[2:11] for isin in isins} | ({explicit} if explicit else set())
        if len(cusips) > 1:
            return "cusip.mismatch"
        if any(not index.sole_owner(owner, "cusip", cusip) for cusip in cusips):
            return "cusip.global_conflict"
        figi = _identifier(registry["figi"])
        if figi is not None:
            problem = figi_problem(figi)
            if problem is not None:
                return f"figi.{problem}"
            if not index.sole_owner(owner, "figi", figi):
                return "figi.global_conflict"
    if _text(iu["currency"]) != "USD":
        return "currency.iu_not_usd"
    if _text(fund["currency"]) != "USD":
        return "currency.funds_v_not_usd"
    if _text(fund["fund_type"]) not in KNOWN_FUND_TYPES:
        return "fund_type.unsupported"
    if iu["is_active"] is not True:
        return "activity.not_active" if iu["is_active"] is False else "activity.unknown"
    return None


def _isin_presence(index: _CatalogIndex, owner: str) -> str:
    iu = _identifier(index.iu[owner][0]["isin"]) is not None
    registry = _identifier(index.registry[owner][0]["isin"]) is not None
    return {
        (True, True): "both",
        (True, False): "iu_only",
        (False, True): "registry_only",
        (False, False): "neither",
    }[(iu, registry)]


def classify_catalog(
    instruments: list[dict],
    funds: list[dict],
    identity: list[dict],
    observed_at: dt.datetime,
) -> tuple[list[dict], dict, dict]:
    """Lifecycle evidence, aggregate counts and ACTIVE digests for one snapshot.

    Returns ``(evidence, counts, digests)``; counts/digests never carry IDs.
    """
    if observed_at.tzinfo is None or not instruments or not funds or not identity:
        raise PolicyGenerationError("catalog_snapshot_unavailable")
    if max(len(instruments), len(funds), len(identity)) > MAX_SOURCE_ROWS:
        raise PolicyGenerationError("catalog_row_limit_exceeded")
    index = _CatalogIndex(
        canonical_source_rows(instruments, "instruments"),
        canonical_source_rows(funds, "funds"),
        canonical_source_rows(identity, "identity"),
    )
    _assert_projection_consistent(index)
    instant = observed_at.astimezone(UTC).isoformat()
    result = []
    statuses: Counter[str] = Counter()
    frequency: Counter[str] = Counter()
    first_failures: Counter[str] = Counter()
    inactive_reasons: Counter[str] = Counter()
    presence = dict.fromkeys(ISIN_PRESENCE_CATEGORIES, 0)
    presence_daily = dict.fromkeys(ISIN_PRESENCE_CATEGORIES, 0)
    structural = structural_daily = structural_claim_failures = 0
    active: list[str] = []
    active_daily: list[str] = []
    for instrument_id in index.universe():
        failure = None
        if _is_inactive(index, instrument_id):
            status = "INACTIVE"
            inactive_reasons["inactive_without_funds_v"] += 1
        else:
            failure = _first_failure(index, instrument_id)
            status = "UNKNOWN" if failure is not None else "ACTIVE"
            if failure is not None:
                first_failures[failure] += 1
            if _first_failure(index, instrument_id, claims=False) is None:
                # Passing every non-claim gate: the only possible first failure
                # is an ISIN/CUSIP/FIGI family, so structural - ACTIVE reconciles.
                structural += 1
                fund_type = _text(index.funds[instrument_id][0]["fund_type"])
                structural_daily += fund_type in SUPPORTED_DAILY_FUND_TYPES
                if failure is not None:
                    if failure.split(".", 1)[0] not in CLAIM_FAILURE_FAMILIES:
                        raise PolicyGenerationError("structural_reconciliation_invalid")
                    structural_claim_failures += 1
        daily = status == "ACTIVE" and (
            _text(index.funds[instrument_id][0]["fund_type"])
            in SUPPORTED_DAILY_FUND_TYPES
        )
        if status == "ACTIVE":
            category = _isin_presence(index, instrument_id)
            active.append(instrument_id)
            presence[category] += 1
            if daily:
                active_daily.append(instrument_id)
                presence_daily[category] += 1
        cadence = "daily" if daily else "unknown"
        result.append(
            {
                "instrument_id": instrument_id,
                "known_at": instant,
                "effective_at": instant,
                "fund_status": status,
                "valuation_frequency": cadence,
                "identity_verified": status == "ACTIVE",
                "return_basis_verified": daily,
                "currency_verified": status == "ACTIVE",
                "evidence_reference": EVIDENCE_REFERENCE,
            }
        )
        statuses[status] += 1
        frequency[cadence] += 1
    unknown_codes = set(first_failures) - set(IDENTITY_FAILURE_CODES)
    if unknown_codes:  # Defensive: every emitted reason belongs to the closed set.
        raise PolicyGenerationError("identity_failure_code_unregistered")
    counts = {
        "instruments_universe": len(instruments),
        "funds_v": len(funds),
        "instrument_identity": len(identity),
        "instrument_evidence": len(result),
        "fund_status": dict(sorted(statuses.items())),
        "valuation_frequency": dict(sorted(frequency.items())),
        "identity_first_failure": dict(sorted(first_failures.items())),
        "inactive_reason": dict(sorted(inactive_reasons.items())),
        "structural_pre_claims": structural,
        "structural_pre_claims_daily": structural_daily,
        "structural_claim_failures": structural_claim_failures,
        "active": len(active),
        "active_daily": len(active_daily),
        "isin_presence_active": presence,
        "isin_presence_active_daily": presence_daily,
    }
    digests = {
        "active_set_sha256": uuid_set_digest(active),
        "active_daily_set_sha256": uuid_set_digest(active_daily),
    }
    return result, counts, digests


def build_policy(
    calendar: dict,
    instruments: list[dict],
    funds: list[dict],
    identity: list[dict],
    observed_at: dt.datetime,
    policy_id: str,
    policy_version: str,
) -> dict:
    if (
        not policy_id
        or not policy_version
        or len(policy_id) > 128
        or len(policy_version) > 64
    ):
        raise PolicyGenerationError("policy_identity_invalid")
    evidence, counts, digests = classify_catalog(
        instruments, funds, identity, observed_at
    )
    if not evidence:
        raise PolicyGenerationError("catalog_snapshot_empty")
    policy = {
        "readiness_profile": "current_daily_nav_v1",
        "readiness_version": 1,
        "generator_version": GENERATOR_VERSION,
        "provider_contract": PROVIDER_CONTRACT,
        "publication_state": "approved",
        "policy_id": policy_id,
        "policy_version": policy_version,
        "valuation_frequency": "daily",
        "timezone": calendar["timezone"],
        "calendar_id": calendar["calendar_id"],
        "calendar_version": calendar["calendar_version"],
        "calendar_source": calendar["calendar_source"],
        "source_reference": calendar["source_reference"],
        "coverage_start": calendar["coverage_start"],
        "coverage_end": calendar["coverage_end"],
        "valid_through": calendar["valid_through"],
        "calendar_session_count": calendar["calendar_session_count"],
        "calendar_digest": calendar["calendar_digest"],
        "sample_intervals": 400,
        "required_endpoints": 401,
        "annualization_sessions": 252,
        "required_nav_kind": "adjusted",
        "required_return_semantics": "observed_interval_log_ratio",
        "modeling_currency": "USD",
        "currency_treatment": "native_only",
        "repaired_nav_kinds": sorted(REPAIRED_NAV_KINDS),
        "adjusted_overlap_absolute_tolerance": ADJUSTED_OVERLAP_ABS_TOL,
        "adjusted_overlap_relative_tolerance": ADJUSTED_OVERLAP_REL_TOL,
        "sessions": calendar["sessions"],
        "instrument_evidence": evidence,
    }
    snapshot_digest = source_snapshot_sha256(instruments, funds, identity)
    policy["generation"] = {
        "generator_version": GENERATOR_VERSION,
        "generated_at": observed_at.astimezone(UTC).isoformat(),
        "requested_coverage_start": calendar["requested_coverage_start"],
        "requested_coverage_end": calendar["requested_coverage_end"],
        "calendar_package": f"exchange_calendars=={PACKAGE_VERSION}",
        "calendar_digest": calendar["calendar_digest"],
        "source_query_version": CURRENT_CATALOG_QUERY_VERSION,
        "source_query_sha256": SOURCE_QUERY_SHA256,
        "source_snapshot_sha256": snapshot_digest,
        "instrument_evidence_digest": instrument_evidence_digest(evidence),
        "policy_hash": policy_content_digest(policy),
        "provider_contract": PROVIDER_CONTRACT,
        "counts": counts,
        **digests,
    }
    policy["generation"]["generation_sha256"] = generation_metadata_digest(
        policy["generation"]
    )
    _policy(policy)
    return policy


def source_snapshot_sha256(
    instruments: list[dict], funds: list[dict], identity: list[dict]
) -> str:
    content = source_snapshot_content(
        canonical_source_rows(instruments, "instruments"),
        canonical_source_rows(funds, "funds"),
        canonical_source_rows(identity, "identity"),
    )
    return hashlib.sha256(canonical_json(content)).hexdigest()


def build_source_snapshot(
    policy: dict, instruments: list[dict], funds: list[dict], identity: list[dict]
) -> dict:
    """Private export of the exact projections classified, hash-linked to the policy.

    It contains production identifiers and must only be written to custody.
    """
    content = source_snapshot_content(
        canonical_source_rows(instruments, "instruments"),
        canonical_source_rows(funds, "funds"),
        canonical_source_rows(identity, "identity"),
    )
    generation = policy["generation"]
    digest = hashlib.sha256(canonical_json(content)).hexdigest()
    if digest != generation["source_snapshot_sha256"]:
        raise PolicyGenerationError("source_snapshot_link_invalid")
    return {
        "kind": SOURCE_SNAPSHOT_KIND,
        "generator_version": GENERATOR_VERSION,
        "decision_at": generation["generated_at"],
        "source_query_version": CURRENT_CATALOG_QUERY_VERSION,
        "source_query_sha256": SOURCE_QUERY_SHA256,
        "source_snapshot_sha256": digest,
        "row_counts": {name: len(rows) for name, rows in sorted(content.items())},
        "policy_id": policy["policy_id"],
        "policy_version": policy["policy_version"],
        "policy_hash": generation["policy_hash"],
        "policy_generation_sha256": generation["generation_sha256"],
        "policy_artifact_sha256": hashlib.sha256(canonical_json(policy)).hexdigest(),
        "sources": content,
    }


SOURCE_SNAPSHOT_KEYS = frozenset(
    {
        "kind",
        "generator_version",
        "decision_at",
        "source_query_version",
        "source_query_sha256",
        "source_snapshot_sha256",
        "row_counts",
        "policy_id",
        "policy_version",
        "policy_hash",
        "policy_generation_sha256",
        "policy_artifact_sha256",
        "sources",
    }
)
_SNAPSHOT_SOURCE_LABELS = {
    "funds": "funds",
    "identity": "identity",
    "instruments": "instruments",
}


def _snapshot_rows(rows: object, source: str) -> list[dict]:
    """Rows must already be the exact canonical projection of their label."""
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise PolicyGenerationError("source_snapshot_contract_invalid")
    fields = set(SOURCE_FIELDS[source])
    if any(set(row) != fields for row in rows):
        raise PolicyGenerationError("source_snapshot_contract_invalid")
    try:
        canonical = canonical_source_rows(rows, source)
    except PolicyGenerationError as exc:
        raise PolicyGenerationError("source_snapshot_contract_invalid") from exc
    if canonical != rows:
        raise PolicyGenerationError("source_snapshot_contract_invalid")
    return rows


def verify_source_snapshot(
    snapshot: object, policy: object, *, raw: bytes | None = None
) -> dict:
    """Offline hash-link check between a source export and its policy (no DB).

    Shapes are validated before any lookup, iteration or canonicalization;
    every failure is one of the static codes ``source_snapshot_requires_policy``,
    ``source_snapshot_not_canonical``, ``source_snapshot_contract_invalid`` or
    ``source_snapshot_link_invalid``.
    """
    if not isinstance(policy, dict) or not isinstance(policy.get("generation"), dict):
        raise PolicyGenerationError("source_snapshot_requires_policy")
    if not isinstance(snapshot, dict) or set(snapshot) != SOURCE_SNAPSHOT_KEYS:
        raise PolicyGenerationError("source_snapshot_contract_invalid")
    if raw is not None and raw != canonical_json(snapshot):
        raise PolicyGenerationError("source_snapshot_not_canonical")
    generation = policy["generation"]
    sources = snapshot["sources"]
    row_counts = snapshot["row_counts"]
    if (
        snapshot["kind"] != SOURCE_SNAPSHOT_KIND
        or snapshot["generator_version"] != GENERATOR_VERSION
        or snapshot["source_query_version"] != CURRENT_CATALOG_QUERY_VERSION
        or snapshot["source_query_sha256"] != SOURCE_QUERY_SHA256
        or not isinstance(sources, dict)
        or set(sources) != set(_SNAPSHOT_SOURCE_LABELS)
        or not isinstance(row_counts, dict)
        or set(row_counts) != set(_SNAPSHOT_SOURCE_LABELS)
        or any(type(count) is not int or count < 0 for count in row_counts.values())
    ):
        raise PolicyGenerationError("source_snapshot_contract_invalid")
    checked = {
        label: _snapshot_rows(sources[label], source)
        for label, source in _SNAPSHOT_SOURCE_LABELS.items()
    }
    digest = hashlib.sha256(
        canonical_json(
            source_snapshot_content(
                checked["instruments"], checked["funds"], checked["identity"]
            )
        )
    ).hexdigest()
    if (
        digest != snapshot["source_snapshot_sha256"]
        or digest != generation.get("source_snapshot_sha256")
        or snapshot["policy_id"] != policy.get("policy_id")
        or snapshot["policy_version"] != policy.get("policy_version")
        or generation.get("generator_version") != GENERATOR_VERSION
        or generation.get("source_query_version") != CURRENT_CATALOG_QUERY_VERSION
        or generation.get("source_query_sha256") != SOURCE_QUERY_SHA256
        or snapshot["decision_at"] != generation.get("generated_at")
        or snapshot["policy_hash"] != generation.get("policy_hash")
        or snapshot["policy_generation_sha256"] != generation.get("generation_sha256")
        or snapshot["policy_artifact_sha256"]
        != hashlib.sha256(canonical_json(policy)).hexdigest()
        or row_counts != {label: len(rows) for label, rows in checked.items()}
        or [checked[label] for label in sorted(checked)]
        != [sorted(checked[label], key=_row_key) for label in sorted(checked)]
    ):
        raise PolicyGenerationError("source_snapshot_link_invalid")
    return {"source_snapshot_sha256": digest}


def verify_artifact(artifact: object, *, raw: bytes | None = None) -> dict:
    if not isinstance(artifact, dict):
        raise PolicyGenerationError("artifact_not_object")
    if raw is not None and raw != canonical_json(artifact):
        raise PolicyGenerationError("artifact_not_canonical")
    generation = artifact.get("generation", artifact)
    if not isinstance(generation, dict):
        raise PolicyGenerationError("artifact_generation_invalid")
    try:
        requested_start = dt.date.fromisoformat(generation["requested_coverage_start"])
        requested_end = dt.date.fromisoformat(generation["requested_coverage_end"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PolicyGenerationError("artifact_generation_invalid") from exc
    expected = build_calendar(requested_start, requested_end)
    fields = (
        "calendar_id",
        "calendar_version",
        "calendar_source",
        "timezone",
        "source_reference",
        "coverage_start",
        "coverage_end",
        "valid_through",
        "calendar_session_count",
        "calendar_digest",
        "sessions",
    )
    if any(artifact.get(field) != expected[field] for field in fields):
        raise PolicyGenerationError("calendar_artifact_mismatch")
    if artifact.get("kind") == "xnys_nav_calendar_v1":
        if (
            artifact.get("publication_state") != "unpublished"
            or "instrument_evidence" in artifact
            or artifact.get("generator_version") != GENERATOR_VERSION
        ):
            raise PolicyGenerationError("calendar_reference_not_unpublished")
        return {
            "mode": "calendar",
            "calendar_digest": expected["calendar_digest"],
            "calendar_session_count": expected["calendar_session_count"],
            "coverage_end": expected["coverage_end"],
        }
    if artifact.get("publication_state") != "approved":
        raise PolicyGenerationError("policy_not_approved")
    evidence = artifact.get("instrument_evidence")
    if not isinstance(evidence, list) or any(
        not isinstance(row, dict) or set(row) != EVIDENCE_FIELDS for row in evidence
    ):
        raise PolicyGenerationError("instrument_evidence_shape_invalid")
    if not isinstance(artifact.get("generation"), dict):
        raise PolicyGenerationError("artifact_generation_invalid")
    _policy(artifact)
    if [row["instrument_id"] for row in evidence] != sorted(
        row["instrument_id"] for row in evidence
    ):
        raise PolicyGenerationError("instrument_evidence_order_invalid")
    counts = artifact["generation"]["counts"]
    for key in ("fund_status", "valuation_frequency"):
        field = "fund_status" if key == "fund_status" else "valuation_frequency"
        actual = dict(sorted(Counter(row[field] for row in evidence).items()))
        if counts[key] != actual:
            raise PolicyGenerationError("instrument_evidence_counts_invalid")
    return {
        "mode": "build",
        "calendar_digest": expected["calendar_digest"],
        "calendar_session_count": expected["calendar_session_count"],
        "coverage_end": expected["coverage_end"],
        "policy_hash": artifact["generation"]["policy_hash"],
        "instrument_evidence_digest": artifact["generation"][
            "instrument_evidence_digest"
        ],
        "counts": counts,
        # Aggregate identities only: digests, never instrument identifiers.
        "policy_id": artifact["policy_id"],
        "policy_version": artifact["policy_version"],
        "generator_version": artifact["generation"]["generator_version"],
        "generation_sha256": artifact["generation"]["generation_sha256"],
        "source_query_sha256": artifact["generation"]["source_query_sha256"],
        "source_snapshot_sha256": artifact["generation"]["source_snapshot_sha256"],
        "active_set_sha256": artifact["generation"]["active_set_sha256"],
        "active_daily_set_sha256": artifact["generation"]["active_daily_set_sha256"],
    }


def _platform_name() -> str:
    return os.name


def _reject_git_checkout(destination: Path) -> None:
    for ancestor in destination.parents:
        try:
            (ancestor / ".git").lstat()  # Presence only; never read its contents.
        except FileNotFoundError:
            continue
        raise PolicyGenerationError("artifact_inside_git_checkout")


def _custody_destination(path: Path, custody_root: Path | None) -> Path:
    if custody_root is None:
        raise PolicyGenerationError("custody_root_required")
    raw_path = path.expanduser().absolute()
    resolved_path = raw_path.resolve(strict=False)
    _reject_git_checkout(raw_path)
    _reject_git_checkout(resolved_path)
    if raw_path != resolved_path or path.is_symlink():
        raise PolicyGenerationError("artifact_symlink_path_invalid")
    raw_root = custody_root.expanduser().absolute()
    try:
        root = raw_root.resolve(strict=True)
        root_stat = root.stat()
    except FileNotFoundError as exc:
        raise PolicyGenerationError("custody_root_missing") from exc
    _reject_git_checkout(root)
    if raw_root != root or not root.is_dir():
        raise PolicyGenerationError("custody_root_invalid")
    if root_stat.st_uid != os.geteuid() or stat.S_IMODE(root_stat.st_mode) & 0o077:
        raise PolicyGenerationError("custody_root_not_private")
    if not resolved_path.is_relative_to(root) or not resolved_path.parent.is_dir():
        raise PolicyGenerationError("artifact_outside_custody_root")
    return resolved_path


def _pinned_parent(path: Path) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    parts = path.parent.parts
    fd = os.open(parts[0], flags)
    try:
        for component in parts[1:]:
            following = os.open(component, flags, dir_fd=fd)
            os.close(fd)
            fd = following
        current = os.fstat(fd)
        original = os.stat(path.parent, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (original.st_dev, original.st_ino):
            raise PolicyGenerationError("artifact_parent_changed")
        _reject_git_checkout(path)
        return fd
    except OSError as exc:
        os.close(fd)
        raise PolicyGenerationError("artifact_parent_changed") from exc
    except Exception:
        os.close(fd)
        raise


def _check_parent_and_target(path: Path, parent_fd: int, *, force: bool) -> None:
    pinned = os.fstat(parent_fd)
    try:
        current = os.stat(path.parent, follow_symlinks=False)
        still_resolved = path.parent.resolve(strict=True) == path.parent
    except FileNotFoundError as exc:
        raise PolicyGenerationError("artifact_parent_changed") from exc
    if not still_resolved or (pinned.st_dev, pinned.st_ino) != (
        current.st_dev,
        current.st_ino,
    ):
        raise PolicyGenerationError("artifact_parent_changed")
    _reject_git_checkout(path)
    try:
        target = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(target.st_mode) or (force and not stat.S_ISREG(target.st_mode)):
        raise PolicyGenerationError("artifact_target_not_regular")


def _write_posix(path: Path, content: bytes, *, force: bool) -> None:
    parent_fd = _pinned_parent(path)
    temporary = None
    published = False
    try:
        for _ in range(4):
            candidate = f".nav-policy-{secrets.token_hex(16)}.tmp"
            try:
                file_fd = os.open(
                    candidate,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=parent_fd,
                )
                temporary = candidate
                break
            except FileExistsError:
                continue
        if temporary is None:
            raise PolicyGenerationError("artifact_private_temp_unavailable")
        with os.fdopen(file_fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        _check_parent_and_target(path, parent_fd, force=force)
        if force:
            os.replace(temporary, path.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            temporary = None
            published = True
        else:
            try:
                os.link(
                    temporary,
                    path.name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise PolicyGenerationError("artifact_already_exists") from exc
            published = True
        os.fsync(parent_fd)
    finally:
        try:
            if temporary is not None:
                try:
                    os.unlink(temporary, dir_fd=parent_fd)
                    if published:
                        os.fsync(parent_fd)
                except FileNotFoundError:
                    pass
        finally:
            os.close(parent_fd)


def _write_calendar_nonposix(path: Path, content: bytes, *, force: bool) -> None:
    # No identity data; Windows has no portable directory fsync/dir_fd custody.
    fd, temporary = tempfile.mkstemp(prefix=".nav-calendar-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if force:
            os.replace(temporary, path)
        else:
            try:
                os.link(temporary, path)
            except FileExistsError as exc:
                raise PolicyGenerationError("artifact_already_exists") from exc
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_artifact(
    path: Path,
    content: bytes,
    *,
    force: bool,
    build: bool,
    custody_root: Path | None = None,
) -> None:
    if build and _platform_name() != "posix":
        raise PolicyGenerationError("productive_artifact_requires_posix")
    if build:
        path = _custody_destination(path, custody_root)
    else:
        path = path.expanduser().absolute()
        if not path.parent.is_dir() or path.is_symlink():
            raise PolicyGenerationError("artifact_output_path_invalid")
    if _platform_name() == "posix":
        _write_posix(path, content, force=force)
    else:
        _write_calendar_nonposix(path, content, force=force)


def _blocked(exc: Exception, **extra) -> dict:
    return {
        "status": "blocked",
        "reason": type(exc).__name__,
        # PolicyGenerationError messages are static codes by construction.
        "code": str(exc) if isinstance(exc, PolicyGenerationError) else None,
        "sqlstate": exc.sqlstate if isinstance(exc, psycopg.Error) else None,
        **extra,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    for mode in ("calendar", "build"):
        cmd = sub.add_parser(mode)
        cmd.add_argument("--coverage-start", type=dt.date.fromisoformat, required=True)
        cmd.add_argument("--coverage-end", type=dt.date.fromisoformat, required=True)
        cmd.add_argument("--output", type=Path, required=True)
        cmd.add_argument("--force", action="store_true")
        if mode == "build":
            cmd.add_argument("--dsn-env", default="NAV_READINESS_DATABASE_URL")
            cmd.add_argument("--custody-root", type=Path, required=True)
            cmd.add_argument("--policy-id", required=True)
            cmd.add_argument("--policy-version", required=True)
            cmd.add_argument(
                "--source-snapshot-output",
                type=Path,
                help="private custody export of the three classified projections",
            )
    verify = sub.add_parser("verify")
    verify.add_argument("--policy-file", type=Path, required=True)
    verify.add_argument("--source-snapshot-file", type=Path)
    args = parser.parse_args(argv)
    written = None
    try:
        if args.mode == "verify":
            raw = args.policy_file.read_bytes()
            artifact = load_json_object(raw, "artifact_not_object")
            report = verify_artifact(artifact, raw=raw)
            report["artifact_sha256"] = hashlib.sha256(raw).hexdigest()
            if args.source_snapshot_file is not None:
                if report["mode"] != "build":
                    raise PolicyGenerationError("source_snapshot_requires_policy")
                snapshot_raw = args.source_snapshot_file.read_bytes()
                report.update(
                    verify_source_snapshot(
                        load_json_object(
                            snapshot_raw, "source_snapshot_contract_invalid"
                        ),
                        artifact,
                        raw=snapshot_raw,
                    )
                )
                report["source_snapshot_file_sha256"] = hashlib.sha256(
                    snapshot_raw
                ).hexdigest()
        else:
            calendar = build_calendar(args.coverage_start, args.coverage_end)
            snapshot = None
            if args.mode == "calendar":
                artifact = calendar
            else:
                if not ENV_NAME.fullmatch(args.dsn_env) or not os.environ.get(
                    args.dsn_env
                ):
                    raise PolicyGenerationError("dsn_environment_missing")
                if args.source_snapshot_output is not None and (
                    args.source_snapshot_output.expanduser().absolute()
                    == args.output.expanduser().absolute()
                ):
                    raise PolicyGenerationError("source_snapshot_output_collides")
                instant, instruments, funds, identity = read_catalog_snapshot(
                    os.environ[args.dsn_env]
                )
                artifact = build_policy(
                    calendar,
                    instruments,
                    funds,
                    identity,
                    instant,
                    args.policy_id,
                    args.policy_version,
                )
                if args.source_snapshot_output is not None:
                    snapshot = build_source_snapshot(
                        artifact, instruments, funds, identity
                    )
            report = verify_artifact(artifact)
            content = canonical_json(artifact)
            write_artifact(
                args.output,
                content,
                force=args.force,
                build=args.mode == "build",
                custody_root=args.custody_root if args.mode == "build" else None,
            )
            written = hashlib.sha256(content).hexdigest()
            report["artifact_sha256"] = written
            if snapshot is not None:
                snapshot_content = canonical_json(snapshot)
                try:
                    write_artifact(
                        args.source_snapshot_output,
                        snapshot_content,
                        force=args.force,
                        build=True,
                        custody_root=args.custody_root,
                    )
                except (PolicyGenerationError, OSError) as exc:
                    # The policy stays on disk as an incomplete bundle: never
                    # deleted or overwritten automatically, never rollout-ready.
                    print(
                        json.dumps(
                            _blocked(
                                exc,
                                stage="source_snapshot_export",
                                bundle="incomplete",
                                artifact_sha256=written,
                            ),
                            sort_keys=True,
                        )
                    )
                    return 2
                report["source_snapshot_sha256"] = snapshot["source_snapshot_sha256"]
                report["source_snapshot_file_sha256"] = hashlib.sha256(
                    snapshot_content
                ).hexdigest()
        print(json.dumps({"status": "ok", **report}, sort_keys=True))
        return 0
    except (
        PolicyGenerationError,
        ValueError,
        KeyError,
        TypeError,
        OSError,
        psycopg.Error,
    ) as exc:
        print(json.dumps(_blocked(exc), sort_keys=True))
        return 2


if __name__ == "__main__":
    sys.exit(main())
