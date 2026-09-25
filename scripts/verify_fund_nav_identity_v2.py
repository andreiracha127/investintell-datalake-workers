"""Independent audit (A1–A8) of a fund-nav-policy-generator-v2 artifact.

Deliberately NOT a reuse of the generator: this module re-implements, from the
written contract, the source typing, normalization, claim checksums, global
claim ownership, projection check, INACTIVE rule and gate precedence, and it
never imports the classifier, normalizers, checksum helpers or decision
constants of ``scripts.generate_fund_nav_policy_v1`` / ``src.workers._nav_policy``.
The only generator import is the POSIX custody writer (no domain decision).

Inputs: the policy artifact, its private source-snapshot export, and either a
new READ ONLY REPEATABLE READ capture (``--dsn-env``: the three sources, the
builder cohort and the SEC classes in ONE snapshot) or a previously persisted
capture bundle (``--capture-file``, fully offline), an optional hash-pinned v1
artifact (read as data, never re-verified) and an audit config pinning the
audit contract version, the ceiling, the SEC freshness criterion, the Light
revision/cohort query/strategy→sleeve map/Stage-1 quotas and the canary salt.

Outputs, all through the private POSIX custody writer (root 0700, files 0600,
atomic, never overwritten, never inside a Git checkout):

1. ``--capture-output`` (required with ``--dsn-env``): the canonical capture
   bundle ``nav-identity-audit-capture-v2`` — every captured row, multiplicity
   preserved, with per-input digests. Written BEFORE the audit runs.
2. ``--output``: the dossier ``nav-identity-audit-v2``. Always written once an
   audit result exists (PASS or FAIL), before any canary decision; it pins the
   hashes/counts of every input (policy, snapshots, capture, cohort, SEC,
   config, audit contract, verifier source) and may list identifiers.
3. ``--canary-output``: only when every A1–A8 gate is PASS (a canary request
   implies ``--strict``); bound to the dossier SHA, the capture SHA and the
   eligible-cohort digest.

stdout carries aggregate counts/codes/digests only (never identifiers, DSNs or
provider payloads). No provider call and no database write is ever performed.

Exit codes: 0 audit passed (``--strict``: every A1–A8 gate PASS; otherwise the
offline core A1/A2/A4/A5/A6 PASS and A3/A7/A8 PASS or NOT_EVALUATED); 3 gate
failure or canary refused (the dossier is still written); 2 blocked
input/capture/output.

Usage, from the repository root (``python -m`` puts the package root on the
path; a live capture and an offline replay of the persisted bundle)::

    python -m scripts.verify_fund_nav_identity_v2 --policy-file P
      --source-snapshot-file S --dsn-env READONLY_DSN_ENV --capture-output C
      --audit-config A --previous-policy-file V1 --previous-policy-sha256 H
      --custody-root R --output D --strict
    python -m scripts.verify_fund_nav_identity_v2 --policy-file P
      --source-snapshot-file S --capture-file C --audit-config A
      --previous-policy-file V1 --previous-policy-sha256 H --custody-root R
      --output D2 --canary-output M
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import re
import sys
import uuid
from collections import Counter, defaultdict
from fractions import Fraction
from pathlib import Path

# Declarative contract leaf (pure data; no decision logic is shared).
from scripts.nav_identity_audit_contract import (
    A4_DETAIL_KEYS,
    A8_OUTCOMES,
    AUDIT_CONFIG_VERSION,
    AUDIT_CONTRACT,
    AUDIT_CONTRACT_SHA256,
    AUDIT_CONTRACT_VERSION,
    AUDIT_VERSION,
    CANARY_KIND,
    CANARY_MAX,
    CANARY_SELECTION_KEYS,
    CAPTURE_KIND,
    DEFAULT_STRUCTURAL_DAILY_CEILING,
    FETCH_BATCH,
    GATE_CHECKS,
    RETIRED_CEILING_KEYS,
    ROW_CEILING,
    SEC_CLASS_PATTERN,
    SEC_LINEAGE_SQL,
    SEC_MAX_SYNCED_AGE_DAYS,
    SEC_QUERY_CONTRACT_SHA256,
    SEC_RELATION,
    SEC_SERIES_PATTERN,
    SEC_SOURCE_CONTRACT,
    SEC_SQL,
    SEC_TIMESTAMP_COLUMN,
    STAGE1_MARGIN_TEXT,
    STRUCTURAL_DAILY_CEILING_KEY,
)

# Custody writer only: private POSIX no-overwrite publication, no domain logic.
from scripts.generate_fund_nav_policy_v1 import PolicyGenerationError, write_artifact

STAGE1_MARGIN = Fraction(1, 10)
SEC_CLASS_RE = re.compile(SEC_CLASS_PATTERN)
SEC_SERIES_RE = re.compile(SEC_SERIES_PATTERN)
# Contract texts the audited artifact must carry (independent literal copies).
EXPECTED_GENERATOR = "fund-nav-policy-generator-v2"
RETIRED_GENERATORS = ("fund-nav-policy-generator-v1",)
EXPECTED_QUERY_VERSION = "nav-current-catalog-snapshot-v2"
EXPECTED_PROVIDER = "w1-tiingo-adjusted-daily-v1"
EXPECTED_CALENDAR_PACKAGE = "exchange_calendars==4.13.2"
EXPECTED_REFERENCE = (
    "nav-current-catalog-snapshot-v2:public.instruments_universe+public.funds_v+"
    "public.instrument_identity:w1-tiingo-adjusted-daily-v1:current_only_not_pit:"
    "identity=registry-ticker-series-claims-v2"
)
SNAPSHOT_KIND = "nav_policy_source_snapshot_v2"
SOURCE_SQL = {
    "instruments": (
        "SELECT instrument_id,instrument_type,ticker,isin,currency,is_active "
        "FROM public.instruments_universe ORDER BY instrument_id LIMIT 100001"
    ),
    "funds": (
        "SELECT instrument_id,series_id,ticker,isin,cusip,currency,fund_type "
        "FROM public.funds_v ORDER BY instrument_id,series_id LIMIT 100001"
    ),
    "identity": (
        "SELECT instrument_id,sec_series_id,sec_class_id,ticker,isin,cusip_9,figi,"
        "resolution_status,conflict_state "
        "FROM public.instrument_identity ORDER BY instrument_id LIMIT 100001"
    ),
}
SOURCE_RELATION = {
    "instruments": "public.instruments_universe",
    "funds": "public.funds_v",
    "identity": "public.instrument_identity",
}
SOURCE_COLUMNS = {
    name: tuple(
        column.strip()
        for column in sql.split("SELECT ", 1)[1].split(" FROM ", 1)[0].split(",")
    )
    for name, sql in SOURCE_SQL.items()
}
QUERY_SHA256 = hashlib.sha256(
    "\n".join(SOURCE_SQL[name] for name in ("instruments", "funds", "identity")).encode(
        "utf-8"
    )
).hexdigest()
SEC_QUERY_SHA256 = hashlib.sha256(SEC_SQL.encode("utf-8")).hexdigest()
SEC_LINEAGE_QUERY_SHA256 = hashlib.sha256(SEC_LINEAGE_SQL.encode("utf-8")).hexdigest()
PRE_CLAIM_FAMILIES = frozenset({"cardinality", "registry", "ticker", "series"})
# Precedence (index = rank). A first failure is the lowest-ranked failing code.
GATE_CODES = (
    "cardinality.iu_missing",
    "cardinality.iu_duplicate",
    "cardinality.funds_v_missing",
    "cardinality.funds_v_duplicate",
    "cardinality.registry_missing",
    "cardinality.registry_duplicate",
    "registry.status_not_canonical",
    "registry.conflict_state_not_empty",
    "instrument_type.not_fund",
    "ticker.missing",
    "ticker.mismatch",
    "ticker.global_conflict",
    "series.missing",
    "series.mismatch",
    "series.global_conflict",
    "isin.unsupported_prefix",
    "isin.invalid_format",
    "isin.checksum",
    "isin.mismatch",
    "isin.global_conflict",
    "cusip.invalid_format",
    "cusip.checksum",
    "cusip.mismatch",
    "cusip.global_conflict",
    "figi.invalid_format",
    "figi.reserved_prefix",
    "figi.checksum",
    "figi.global_conflict",
    "currency.iu_not_usd",
    "currency.funds_v_not_usd",
    "fund_type.unsupported",
    "activity.not_active",
    "activity.unknown",
)
RANK = {code: position for position, code in enumerate(GATE_CODES)}
CLAIM_FAMILIES = frozenset({"isin", "cusip", "figi"})
DIGITS = "0123456789"
ALNUM = DIGITS + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
CUSIP_EXTRA = {"*": 36, "@": 37, "#": 38}
FIGI_LETTERS = "BCDFGHJKLMNPQRSTVWXYZ"
FIGI_RESERVED = ("BS", "BM", "GG", "GB", "GH", "KY", "VG")
KNOWN_TYPES = ("etf", "mutual_fund", "mmf")
DAILY_TYPES = ("etf", "mutual_fund")
ISIN_CLASSES = ("both", "iu_only", "registry_only", "neither")
CANARY_TYPES = ("etf", "mutual_fund")
LOWER_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
HEX64 = re.compile(r"^[0-9a-f]{64}$")
ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]*$")
EVIDENCE_KEYS = frozenset(
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
# The configured cohort SELECT is bounded server-side: it is wrapped as a
# subquery with an outer LIMIT (sentinel row = ceiling + 1) and read through a
# server-side cursor in FETCH_BATCH chunks, so at most ceiling + batch rows are
# ever held client-side. statement_timeout (30s) bounds wall time per
# DECLARE/FETCH; it is not a cardinality limit, and the LIMIT is not a time limit.
COHORT_WRAPPER_PREFIX = "SELECT * FROM ("
COHORT_WRAPPER_SUFFIX = f") AS nav_identity_audit_cohort LIMIT {ROW_CEILING + 1}"
_SELECT_START = re.compile(r"\A\s*select\s", re.IGNORECASE)
# Defense in depth on the OPS-authored cohort query; READ ONLY still blocks
# writes. Rejects statement separators, comments, write/DDL/transaction/locking
# keywords and functions that act outside the snapshot (advisory locks, dblink,
# sequences, sleeps, backend signals, file/large-object access).
_UNSAFE_SQL = re.compile(
    r";|--|/\*|\*/"
    r"|\b(?:insert|update|delete|merge|truncate|copy|create|alter|drop|grant"
    r"|revoke|call|lock|vacuum|refresh|begin|commit|rollback|savepoint|declare"
    r"|listen|notify|nextval|setval|set_config|dblink\w*|pg_advisory\w*"
    r"|pg_try_advisory\w*|pg_terminate_backend|pg_cancel_backend|pg_sleep\w*"
    r"|pg_read_\w+|pg_ls_dir|pg_stat_file|lo_\w+)\b"
    r"|\bfor\s+(?:share|no\s+key|key\s+share)\b",
    re.IGNORECASE,
)


class AuditBlocked(Exception):
    """A static blocked code; never identifiers, DSNs or payload fragments."""


# ── canonical encodings (format contract, re-implemented) ────────────────────
def _compact(value, *, sort_keys: bool = True) -> str:
    return json.dumps(
        value, sort_keys=sort_keys, separators=(",", ":"), ensure_ascii=True
    )


def _unique_pairs(pairs: list) -> dict:
    keys = [key for key, _value in pairs]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate_key")
    return dict(pairs)


def _no_constant(_name: str):
    raise ValueError("non_finite_constant")


def _strict_json(raw: bytes, code: str, *, require_object: bool = True):
    """Hostile-input JSON: duplicate keys, NaN/Infinity and non-objects block."""
    try:
        value = json.loads(
            raw, object_pairs_hook=_unique_pairs, parse_constant=_no_constant
        )
    except (ValueError, UnicodeDecodeError, RecursionError) as exc:
        raise AuditBlocked(code) from exc
    if require_object and not isinstance(value, dict):
        raise AuditBlocked(code)
    return value


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _document_bytes(document: dict) -> bytes:
    return (_compact(document) + "\n").encode("ascii")


def _policy_hash(policy: dict) -> str:
    return _sha(
        _compact(
            {
                k: v
                for k, v in policy.items()
                if k not in ("instrument_evidence", "generation")
            }
        ).encode("utf-8")
    )


def _evidence_digest(rows: list) -> str:
    return _sha(
        _compact(
            [
                {k: v for k, v in row.items() if k not in ("known_at", "effective_at")}
                for row in rows
            ]
        ).encode("utf-8")
    )


def _generation_digest(generation: dict) -> str:
    return _sha(
        _compact(
            {k: v for k, v in generation.items() if k != "generation_sha256"}
        ).encode("utf-8")
    )


def _set_digest(ids) -> str:
    return _sha(json.dumps(sorted(ids), separators=(",", ":")).encode("utf-8"))


def selection_digest(selection: dict) -> str:
    """SHA-256 of the canonical canary selection object (contract rule)."""
    return _sha(_document_bytes(selection))


def _rows_sha(rows: list) -> str:
    """Digest of an already canonically ordered row list (multiplicity kept)."""
    return _sha(_document_bytes(rows))


def _contract_consistent() -> bool:
    """The frozen leaf literal must describe this auditor's runtime semantics."""
    return (
        _sha(_compact(AUDIT_CONTRACT).encode("utf-8")) == AUDIT_CONTRACT_SHA256
        and AUDIT_CONTRACT["catalog_source_query_sha256"] == QUERY_SHA256
        and AUDIT_CONTRACT["cohort_wrapper"]
        == COHORT_WRAPPER_PREFIX + "<cohort_query>" + COHORT_WRAPPER_SUFFIX
        and AUDIT_CONTRACT["canary"]["strata"]
        == [f"{c}|{t}" for c in ISIN_CLASSES for t in CANARY_TYPES]
        and _sha((SEC_SQL + "\n" + SEC_LINEAGE_SQL).encode("utf-8"))
        == SEC_QUERY_CONTRACT_SHA256
        and AUDIT_CONTRACT["a8"]["max_synced_age_days"] == SEC_MAX_SYNCED_AGE_DAYS
    )


if not _contract_consistent():  # pragma: no cover - import-time guard
    raise RuntimeError("audit_contract_literal_mismatch")


# ── source typing and normalization (own implementation) ─────────────────────
def _source_rows(rows, source: str, *, from_database: bool) -> list[dict]:
    columns = SOURCE_COLUMNS[source]
    typed = []
    for row in rows:
        if not isinstance(row, dict) or (
            not from_database and set(row) != set(columns)
        ):
            raise AuditBlocked("source_row_shape_invalid")
        try:
            record = {column: row[column] for column in columns}
        except KeyError as exc:
            raise AuditBlocked("source_row_shape_invalid") from exc
        identifier = record["instrument_id"]
        if from_database and isinstance(identifier, uuid.UUID):
            identifier = str(identifier)
        if not isinstance(identifier, str) or not LOWER_UUID.match(identifier):
            raise AuditBlocked("source_uuid_invalid")
        record["instrument_id"] = identifier
        for column in columns[1:]:
            value = record[column]
            if column == "is_active":
                ok = value is None or value is True or value is False
            elif column == "conflict_state":
                try:
                    json.dumps(value, allow_nan=False)
                    ok = True
                except (TypeError, ValueError):
                    ok = False
            else:
                ok = value is None or isinstance(value, str)
            if not ok:
                raise AuditBlocked("source_value_type_invalid")
        typed.append(record)
    return typed


def _snapshot_sha(sources: dict) -> str:
    ordered = {
        name: sorted(sources[name], key=_compact)
        for name in ("funds", "identity", "instruments")
    }
    return _sha(_document_bytes(ordered))


def _clean(value):
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed if trimmed else None


def _upper(value):
    trimmed = _clean(value)
    if trimmed is None:
        return None
    return "".join(chr(ord(ch) - 32) if "a" <= ch <= "z" else ch for ch in trimmed)


def _digit_sum(number: int) -> int:
    return sum(int(d) for d in str(number))


def cusip_status(value: str) -> str | None:
    if (
        len(value) != 9
        or value[8] not in DIGITS
        or any(ch not in ALNUM and ch not in CUSIP_EXTRA for ch in value[:8])
    ):
        return "invalid_format"
    total = 0
    for position in range(8):
        ch = value[position]
        worth = ALNUM.index(ch) if ch in ALNUM else CUSIP_EXTRA[ch]
        total += _digit_sum(worth * (2 if position % 2 == 1 else 1))
    return None if (10 - total % 10) % 10 == DIGITS.index(value[8]) else "checksum"


def isin_status(value: str) -> str | None:
    if value[:2] != "US":
        return "unsupported_prefix"
    if (
        len(value) != 12
        or value[11] not in DIGITS
        or any(ch not in ALNUM for ch in value[:11])
    ):
        return "invalid_format"
    if cusip_status(value[2:11]) is not None:
        return "checksum"
    digits = [int(d) for ch in value for d in str(ALNUM.index(ch))]
    total = 0
    for offset, digit in enumerate(digits[::-1]):
        if offset % 2 == 1:
            digit = digit * 2
            digit = digit - 9 if digit > 9 else digit
        total += digit
    return None if total % 10 == 0 else "checksum"


def figi_status(value: str) -> str | None:
    if (
        len(value) != 12
        or value[0] not in FIGI_LETTERS
        or value[1] not in FIGI_LETTERS
        or value[2] != "G"
        or any(ch not in FIGI_LETTERS and ch not in DIGITS for ch in value[3:11])
        or value[11] not in DIGITS
    ):
        return "invalid_format"
    if value[:2] in FIGI_RESERVED:
        return "reserved_prefix"
    total = sum(
        _digit_sum(ALNUM.index(ch) * (2 if i % 2 == 1 else 1))
        for i, ch in enumerate(value[:11])
    )
    return None if (10 - total % 10) % 10 == DIGITS.index(value[11]) else "checksum"


def _embedded_cusip(isin):
    return (
        isin[2:11]
        if isin is not None and len(isin) == 12 and isin[:2] == "US"
        else None
    )


# ── independent classification ───────────────────────────────────────────────
class Catalog:
    """Per-UUID source rows and global claim owners over every source row."""

    def __init__(self, sources: dict):
        self.rows = {key: defaultdict(list) for key in ("iu", "fv", "reg")}
        self.owner = {
            kind: defaultdict(set)
            for kind in ("ticker", "pair", "isin", "cusip", "figi")
        }
        for key, source in (
            ("iu", "instruments"),
            ("fv", "funds"),
            ("reg", "identity"),
        ):
            for row in sources[source]:
                self.rows[key][row["instrument_id"]].append(row)
        for row in sources["instruments"]:
            self._own(row["instrument_id"], ticker=row["ticker"], isin=row["isin"])
        for row in sources["identity"]:
            self._own(
                row["instrument_id"],
                ticker=row["ticker"],
                series=row["sec_series_id"],
                isin=row["isin"],
                cusip=row["cusip_9"],
                figi=row["figi"],
            )
        for row in sources["funds"]:
            self._own(
                row["instrument_id"],
                ticker=row["ticker"],
                series=row["series_id"],
                isin=row["isin"],
                cusip=row["cusip"],
            )

    def _own(self, uid, *, ticker, isin, series=None, cusip=None, figi=None):
        t, s, i = _upper(ticker), _upper(series), _upper(isin)
        if t:
            self.owner["ticker"][t].add(uid)
        if t and s:
            self.owner["pair"][(s, t)].add(uid)
        if i:
            self.owner["isin"][i].add(uid)
        for c in (_upper(cusip), _embedded_cusip(i)):
            if c:
                self.owner["cusip"][c].add(uid)
        f = _upper(figi)
        if f:
            self.owner["figi"][f].add(uid)

    def universe(self) -> set:
        return set(self.rows["fv"]) | {
            uid
            for uid, rows in self.rows["iu"].items()
            if any(_clean(r["instrument_type"]) == "fund" for r in rows)
        }

    def projection_divergent(self) -> list:
        divergent = []
        for uid, registry_rows in self.rows["reg"].items():
            if len(registry_rows) == 1:
                reg = registry_rows[0]
                want = [
                    _upper(reg[k])
                    for k in ("sec_series_id", "ticker", "isin", "cusip_9")
                ]
                if any(
                    [_upper(fund[k]) for k in ("series_id", "ticker", "isin", "cusip")]
                    != want
                    for fund in self.rows["fv"].get(uid, [])
                ):
                    divergent.append(uid)
        return sorted(divergent)

    def inactive(self, uid) -> bool:
        iu = self.rows["iu"].get(uid, [])
        return (
            len(iu) == 1
            and iu[0]["is_active"] is False
            and len(self.rows["fv"].get(uid, [])) == 0
        )

    def _sole(self, kind, value, uid) -> bool:
        return self.owner[kind].get(value, set()) == {uid}

    def failures(self, uid, *, claims: bool = True) -> set:
        """Every evaluable failing gate (not just the first)."""
        found = set()
        for family, key in (("iu", "iu"), ("funds_v", "fv"), ("registry", "reg")):
            n = len(self.rows[key].get(uid, []))
            if n != 1:
                found.add(
                    f"cardinality.{family}_{'missing' if n == 0 else 'duplicate'}"
                )
        if found:
            return found
        iu, fv, reg = (self.rows[k][uid][0] for k in ("iu", "fv", "reg"))
        if _clean(reg["resolution_status"]) != "canonical":
            found.add("registry.status_not_canonical")
        if not (
            isinstance(reg["conflict_state"], dict) and len(reg["conflict_state"]) == 0
        ):
            found.add("registry.conflict_state_not_empty")
        if _clean(iu["instrument_type"]) != "fund":
            found.add("instrument_type.not_fund")
        tickers = [_upper(iu["ticker"]), _upper(fv["ticker"]), _upper(reg["ticker"])]
        distinct_tickers = {t for t in tickers if t is not None}
        if None in tickers:
            found.add("ticker.missing")
        if len(distinct_tickers) > 1:
            found.add("ticker.mismatch")
        ticker = (
            next(iter(distinct_tickers))
            if len(distinct_tickers) == 1 and None not in tickers
            else None
        )
        if ticker is not None and not self._sole("ticker", ticker, uid):
            found.add("ticker.global_conflict")
        series = [_upper(reg["sec_series_id"]), _upper(fv["series_id"])]
        distinct_series = {s for s in series if s is not None}
        if None in series:
            found.add("series.missing")
        if len(distinct_series) > 1:
            found.add("series.mismatch")
        if (
            ticker is not None
            and None not in series
            and len(distinct_series) == 1
            and not self._sole("pair", (series[0], ticker), uid)
        ):
            found.add("series.global_conflict")
        if claims:
            isins = [
                v for v in (_upper(iu["isin"]), _upper(reg["isin"])) if v is not None
            ]
            for value in isins:
                problem = isin_status(value)
                if problem:
                    found.add(f"isin.{problem}")
            if len(set(isins)) > 1:
                found.add("isin.mismatch")
            if any(not self._sole("isin", v, uid) for v in set(isins)):
                found.add("isin.global_conflict")
            explicit = _upper(reg["cusip_9"])
            if explicit is not None and cusip_status(explicit):
                found.add(f"cusip.{cusip_status(explicit)}")
            cusips = {c for c in (_embedded_cusip(v) for v in isins) if c}
            if explicit is not None:
                cusips.add(explicit)
            if len(cusips) > 1:
                found.add("cusip.mismatch")
            if any(not self._sole("cusip", c, uid) for c in cusips):
                found.add("cusip.global_conflict")
            figi = _upper(reg["figi"])
            if figi is not None:
                if figi_status(figi):
                    found.add(f"figi.{figi_status(figi)}")
                if not self._sole("figi", figi, uid):
                    found.add("figi.global_conflict")
        if _clean(iu["currency"]) != "USD":
            found.add("currency.iu_not_usd")
        if _clean(fv["currency"]) != "USD":
            found.add("currency.funds_v_not_usd")
        if _clean(fv["fund_type"]) not in KNOWN_TYPES:
            found.add("fund_type.unsupported")
        if iu["is_active"] is not True:
            found.add(
                "activity.not_active"
                if iu["is_active"] is False
                else "activity.unknown"
            )
        return found

    def fund_type(self, uid):
        return _clean(self.rows["fv"][uid][0]["fund_type"])

    def isin_class(self, uid) -> str:
        iu = _upper(self.rows["iu"][uid][0]["isin"]) is not None
        reg = _upper(self.rows["reg"][uid][0]["isin"]) is not None
        if iu and reg:
            return "both"
        if iu:
            return "iu_only"
        return "registry_only" if reg else "neither"

    def v1_structural_daily(self) -> tuple[int, int]:
        """Counts derived from ``v1_structural_sets`` (kept for consumers)."""
        total, daily = self.v1_structural_sets()
        return len(total), len(daily)

    def v1_structural_sets(self) -> tuple[set, set]:
        """v1 rule without the mandatory ISIN (baseline B): total and daily sets."""
        iu_tickers, fv_tickers, fv_pairs = (
            defaultdict(int),
            defaultdict(int),
            defaultdict(int),
        )
        for rows in self.rows["iu"].values():
            for row in rows:
                if _upper(row["ticker"]):
                    iu_tickers[_upper(row["ticker"])] += 1
        for rows in self.rows["fv"].values():
            for row in rows:
                t, s = _upper(row["ticker"]), _clean(row["series_id"])
                if t:
                    fv_tickers[t] += 1
                if t and s:
                    fv_pairs[(s, t)] += 1
        total, daily = set(), set()
        for uid in self.universe():
            iu, fv = self.rows["iu"].get(uid, []), self.rows["fv"].get(uid, [])
            if len(iu) != 1 or len(fv) != 1:
                continue
            i, f = iu[0], fv[0]
            t = _upper(i["ticker"])
            s = _clean(f["series_id"])
            if (
                _clean(i["instrument_type"]) == "fund"
                and t
                and t == _upper(f["ticker"])
                and s
                and iu_tickers[t] == 1
                and fv_tickers[t] == 1
                and fv_pairs[(s, t)] == 1
                and _clean(i["currency"]) == "USD"
                and _clean(f["currency"]) == "USD"
                and _clean(f["fund_type"]) in KNOWN_TYPES
                and i["is_active"] is True
            ):
                total.add(uid)
                if _clean(f["fund_type"]) in DAILY_TYPES:
                    daily.add(uid)
        return total, daily


def classify(catalog: Catalog) -> dict:
    """Independent lifecycle per UUID with first failure and every failure."""
    result = {}
    for uid in sorted(catalog.universe()):
        if catalog.inactive(uid):
            result[uid] = {
                "status": "INACTIVE",
                "daily": False,
                "first": None,
                "all": set(),
            }
            continue
        failing = catalog.failures(uid)
        if failing:
            first = min(failing, key=RANK.__getitem__)
            result[uid] = {
                "status": "UNKNOWN",
                "daily": False,
                "first": first,
                "all": failing,
            }
        else:
            result[uid] = {
                "status": "ACTIVE",
                "daily": catalog.fund_type(uid) in DAILY_TYPES,
                "first": None,
                "all": set(),
            }
    return result


# ── gates ────────────────────────────────────────────────────────────────────
def _gate(checks: dict, *, evaluated: bool = True, code: str | None = None) -> dict:
    if not evaluated:
        return {"status": "NOT_EVALUATED", "code": code, "checks": checks}
    return {
        "status": "PASS" if all(value is True for value in checks.values()) else "FAIL",
        "code": None,
        "checks": checks,
    }


def _count_dict(values) -> dict:
    counts = defaultdict(int)
    for value in values:
        counts[value] += 1
    return dict(sorted(counts.items()))


def _int_tree_ok(value) -> bool:
    """Every leaf is a non-negative ``int`` (``bool`` is not an int here)."""
    if isinstance(value, dict):
        return all(isinstance(k, str) and _int_tree_ok(v) for k, v in value.items())
    return type(value) is int and value >= 0


def cohort_query_problem(query: object) -> str | None:
    if not isinstance(query, str) or not _SELECT_START.match(query):
        return "audit_config_builder_invalid"
    if _UNSAFE_SQL.search(query):
        return "audit_config_cohort_query_unsafe"
    return None


def _load_config(raw: bytes | None) -> dict:
    if raw is None:
        return {}
    config = _strict_json(raw, "audit_config_not_json", require_object=False)
    if (
        not isinstance(config, dict)
        or config.get("audit_config_version") != AUDIT_CONFIG_VERSION
    ):
        raise AuditBlocked("audit_config_version_invalid")
    if config.get("audit_contract_version") != AUDIT_CONTRACT_VERSION:
        raise AuditBlocked("audit_contract_version_invalid")
    # The ceiling applies to P (structural daily pre-claims); the retired key
    # named the wrong population and blocks even next to the new one.
    if any(key in config for key in RETIRED_CEILING_KEYS):
        raise AuditBlocked("audit_config_ceiling_key_retired")
    ceiling = config.get(STRUCTURAL_DAILY_CEILING_KEY)
    if type(ceiling) is not int or ceiling < 0:
        raise AuditBlocked("audit_config_ceiling_invalid")
    for key in (
        "structural_baseline",
        "accepted_structural_delta",
        "expected_previous_inactive",
    ):
        value = config.get(key)
        if value is not None and type(value) is not int:
            raise AuditBlocked("audit_config_count_invalid")
    sec = config.get("sec")
    if sec is not None and not isinstance(sec, dict):
        raise AuditBlocked("audit_config_sec_invalid")
    if sec is not None:
        if (
            sec.get("source_contract") != SEC_SOURCE_CONTRACT
            or sec.get("relation") != SEC_RELATION
            or sec.get("timestamp_column") != SEC_TIMESTAMP_COLUMN
            or sec.get("query_contract_sha256") != SEC_QUERY_CONTRACT_SHA256
        ):
            raise AuditBlocked("audit_config_sec_source_invalid")
        max_age = sec.get("max_synced_age_days")
        # Exactly the contract value: no runtime override (bool/0/8 all block).
        if type(max_age) is not int or max_age != SEC_MAX_SYNCED_AGE_DAYS:
            raise AuditBlocked("audit_config_sec_freshness_invalid")
    salt = config.get("canary_salt")
    if salt is not None and (not isinstance(salt, str) or not salt):
        raise AuditBlocked("audit_config_canary_salt_invalid")
    builder = config.get("builder")
    if builder is not None:
        quotas = builder.get("stage1_quotas") if isinstance(builder, dict) else None
        mapping = builder.get("label_to_sleeve") if isinstance(builder, dict) else None
        if (
            not isinstance(builder, dict)
            or not isinstance(quotas, dict)
            or not quotas
            or any(
                not isinstance(k, str) or type(v) is not int or v < 0
                for k, v in quotas.items()
            )
            or not isinstance(mapping, dict)
            or any(
                not isinstance(k, str) or not isinstance(v, str)
                for k, v in mapping.items()
            )
            or not isinstance(builder.get("light_revision"), str)
            or not re.fullmatch(r"[0-9a-f]{40}", builder["light_revision"])
            or not isinstance(builder.get("cohort_parameters", {}), dict)
        ):
            raise AuditBlocked("audit_config_builder_invalid")
        # Contract constant, not a tunable: a different margin is a new contract.
        if builder.get("margin_fraction") != STAGE1_MARGIN_TEXT:
            raise AuditBlocked("audit_config_margin_invalid")
        problem = cohort_query_problem(builder.get("cohort_query"))
        if problem is not None:
            raise AuditBlocked(problem)
    return config


# ── canonical capture bundle (private; every captured row, multiplicity kept) ─
def _as_text_time(value):
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    raise AuditBlocked("capture_timestamp_invalid")


def _not_captured(state: object, base: dict, **extra) -> dict:
    code = state.get("code") if isinstance(state, dict) else None
    name = state.get("state") if isinstance(state, dict) else None
    return {
        **base,
        "state": name if isinstance(name, str) else "not_captured",
        "code": code if isinstance(code, str) else "not_captured",
        "rows": [],
        "row_count": 0,
        "rows_sha256": _rows_sha([]),
        **extra,
    }


def _canonical_cohort(cohort: object, builder: dict | None) -> dict:
    base = {
        "query_sha256": None
        if builder is None
        else _sha(builder["cohort_query"].encode("utf-8")),
        "parameters_sha256": None
        if builder is None
        else _sha(_compact(builder.get("cohort_parameters") or {}).encode("utf-8")),
    }
    if not isinstance(cohort, dict) or cohort.get("state") != "captured":
        return _not_captured(cohort, base)
    rows, code = [], None
    for row in cohort.get("rows") or []:
        if not isinstance(row, dict) or not {"instrument_id", "strategy_label"} <= set(
            row
        ):
            code = "cohort_shape_invalid"
            break
        uid = row["instrument_id"]
        uid = str(uid) if isinstance(uid, uuid.UUID) else uid
        if not isinstance(uid, str) or not LOWER_UUID.match(uid):
            code = "cohort_uuid_invalid"
            break
        label = row["strategy_label"]
        if label is not None and not isinstance(label, str):
            code = "cohort_label_invalid"
            break
        rows.append({"instrument_id": uid, "strategy_label": label})
    if code is None and len(rows) > ROW_CEILING:
        code = "cohort_row_ceiling_exceeded"
    if code is not None:
        return _not_captured({"state": "invalid", "code": code}, base)
    rows.sort(key=_compact)
    return {
        **base,
        "state": "captured",
        "code": None,
        "rows": rows,
        "row_count": len(rows),
        "rows_sha256": _rows_sha(rows),
    }


SEC_SOURCE_PINS = {
    "source_contract": SEC_SOURCE_CONTRACT,
    "relation": SEC_RELATION,
    "timestamp_column": SEC_TIMESTAMP_COLUMN,
    "query_sha256": SEC_QUERY_SHA256,
    "lineage_query_sha256": SEC_LINEAGE_QUERY_SHA256,
    "query_contract_sha256": SEC_QUERY_CONTRACT_SHA256,
}


def _aware_text(value) -> str | None:
    """Timestamp as ISO text; a naive or non-timestamp value is not accepted."""
    if isinstance(value, dt.datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise AuditBlocked("sec_timestamp_not_aware")
        return value.isoformat()
    if isinstance(value, str) and _aware(value) is not None:
        return value
    raise AuditBlocked("sec_timestamp_not_aware")


def _sec_unavailable(state: object) -> dict:
    """Explicit unavailability: pins kept, no fabricated rows or lineage."""
    code = state.get("code") if isinstance(state, dict) else None
    name = state.get("state") if isinstance(state, dict) else None
    return {
        **SEC_SOURCE_PINS,
        "state": name
        if isinstance(name, str) and name != "captured"
        else "not_captured",
        "code": code if isinstance(code, str) else "sec_not_captured",
    }


def _canonical_sec(sec: object) -> dict:
    """Canonical SEC capture: exact row shape, aware timestamps, lineage match.

    A malformed row TYPE makes the whole capture invalid (NOT_EVALUATED). A
    textual but malformed VALUE (empty text, S.../series:ticker class ids) is
    kept verbatim: it is counted, hashed and judged by A8, never repaired.
    """
    if not isinstance(sec, dict) or sec.get("state") != "captured":
        return _sec_unavailable(sec)
    raw_rows = sec.get("rows")
    lineage = sec.get("lineage")
    if not isinstance(raw_rows, list) or not isinstance(lineage, dict):
        return _sec_unavailable(
            {"state": "invalid", "code": "sec_source_shape_invalid"}
        )
    rows = []
    try:
        for row in raw_rows:
            if not isinstance(row, dict) or set(row) != {
                "class_id",
                "series_id",
                "ticker",
                "synced_at",
            }:
                raise AuditBlocked("sec_source_shape_invalid")
            if any(
                row[k] is not None and not isinstance(row[k], str)
                for k in ("class_id", "series_id", "ticker")
            ):
                raise AuditBlocked("sec_source_shape_invalid")
            rows.append(
                {
                    "class_id": row["class_id"],
                    "series_id": row["series_id"],
                    "ticker": row["ticker"],
                    "synced_at": _aware_text(row["synced_at"]),
                }
            )
        count = lineage.get("row_count")
        if set(lineage) != {"row_count", "min_synced_at", "max_synced_at"} or (
            type(count) is not int or count < 0
        ):
            raise AuditBlocked("sec_lineage_invalid")
        low = (
            None
            if lineage["min_synced_at"] is None
            else _aware_text(lineage["min_synced_at"])
        )
        high = (
            None
            if lineage["max_synced_at"] is None
            else _aware_text(lineage["max_synced_at"])
        )
    except AuditBlocked as exc:
        return _sec_unavailable({"state": "invalid", "code": str(exc)})
    if len(rows) > ROW_CEILING:
        return _sec_unavailable(
            {"state": "invalid", "code": "sec_row_ceiling_exceeded"}
        )
    instants = [_aware(row["synced_at"]) for row in rows]
    if (
        count != len(rows)
        or (low is None) != (not rows)
        or (high is None) != (not rows)
        or (rows and (_aware(low) != min(instants) or _aware(high) != max(instants)))
    ):
        # The lineage aggregate must describe exactly the captured rows.
        return _sec_unavailable({"state": "invalid", "code": "sec_lineage_mismatch"})
    rows.sort(key=_compact)
    return {
        **SEC_SOURCE_PINS,
        "state": "captured",
        "rows": rows,
        "row_count": len(rows),
        "rows_sha256": _rows_sha(rows),
        "lineage": {"row_count": count, "min_synced_at": low, "max_synced_at": high},
    }


def canonical_capture(live: dict, config: dict) -> dict:
    """Canonical bundle of one live capture (sources + cohort + SEC)."""
    if not isinstance(live, dict) or not isinstance(live.get("sources"), dict):
        raise AuditBlocked("capture_invalid")
    sources = {}
    for name in ("funds", "identity", "instruments"):
        rows = _source_rows(live["sources"].get(name, []), name, from_database=True)
        if len(rows) > ROW_CEILING:
            raise AuditBlocked("source_row_ceiling_exceeded")
        sources[name] = sorted(rows, key=_compact)
    return {
        "kind": CAPTURE_KIND,
        "audit_contract_version": AUDIT_CONTRACT_VERSION,
        "captured_at": _as_text_time(live.get("captured_at")),
        "source_query_sha256": QUERY_SHA256,
        "sources": sources,
        "source_row_counts": {name: len(rows) for name, rows in sources.items()},
        "source_snapshot_sha256": _snapshot_sha(sources),
        "cohort": _canonical_cohort(live.get("cohort"), config.get("builder")),
        "sec": _canonical_sec(live.get("sec")),
    }


def capture_bytes(live: dict, config: dict) -> bytes:
    return _document_bytes(canonical_capture(live, config))


def load_capture(raw: bytes, config: dict) -> dict:
    """Re-derive a persisted bundle from its rows; any mismatch blocks.

    A bundle of an older capture kind is rejected, never reinterpreted.
    """
    bundle = _strict_json(raw, "capture_not_json", require_object=False)
    if not isinstance(bundle, dict) or raw != _document_bytes(bundle):
        raise AuditBlocked("capture_not_canonical")
    if (
        bundle.get("kind") != CAPTURE_KIND
        or bundle.get("audit_contract_version") != AUDIT_CONTRACT_VERSION
        or bundle.get("source_query_sha256") != QUERY_SHA256
    ):
        raise AuditBlocked("capture_contract_invalid")
    sources = bundle.get("sources")
    cohort = bundle.get("cohort")
    sec = bundle.get("sec")
    if (
        not isinstance(sources, dict)
        or set(sources) != {"funds", "identity", "instruments"}
        or not isinstance(cohort, dict)
        or not isinstance(sec, dict)
    ):
        raise AuditBlocked("capture_bundle_invalid")
    for name, rows in sources.items():
        if not isinstance(rows, list):
            raise AuditBlocked("capture_bundle_invalid")
        _source_rows(rows, name, from_database=False)  # exact shapes and types
    expected_query = _canonical_cohort(None, config.get("builder"))["query_sha256"]
    if (
        cohort.get("query_sha256") != expected_query
        or cohort.get("parameters_sha256")
        != _canonical_cohort(None, config.get("builder"))["parameters_sha256"]
    ):
        raise AuditBlocked("capture_cohort_query_mismatch")
    if any(sec.get(key) != value for key, value in SEC_SOURCE_PINS.items()):
        raise AuditBlocked("capture_sec_contract_invalid")
    rebuilt = canonical_capture(
        {
            "captured_at": bundle.get("captured_at"),
            "sources": sources,
            "cohort": {k: cohort.get(k) for k in ("state", "code", "rows")},
            "sec": {k: sec.get(k) for k in ("state", "code", "rows", "lineage")},
        },
        config,
    )
    if rebuilt != bundle:
        raise AuditBlocked("capture_bundle_invalid")
    return bundle


# ── A7 / A8 ──────────────────────────────────────────────────────────────────
def _a7(cohort, builder: dict | None, verdict: dict, catalog: Catalog):
    if builder is None:
        return _gate({}, evaluated=False, code="builder_config_absent"), {}, []
    if not isinstance(cohort, dict) or cohort.get("state") != "captured":
        code = (cohort or {}).get("code") if isinstance(cohort, dict) else None
        return _gate({}, evaluated=False, code=code or "cohort_not_captured"), {}, []
    mapping = builder["label_to_sleeve"]
    members, seen = [], defaultdict(int)
    partition = defaultdict(int)
    reasons = defaultdict(int)
    for row in cohort["rows"]:
        uid, label = row["instrument_id"], row["strategy_label"]
        seen[uid] += 1
        if seen[uid] > 1:
            continue  # multiplicity is reported and fails cohort_unique
        sleeve = mapping.get(label) if label is not None else None
        entry = verdict.get(uid)
        if entry is None:
            status_key, reason = "NOT_IN_UNIVERSE", "not_in_evidence_universe"
        elif entry["status"] == "ACTIVE":
            status_key = "ACTIVE_DAILY" if entry["daily"] else "ACTIVE_NONDAILY"
            reason = None if entry["daily"] else "not_daily"
        elif entry["status"] == "INACTIVE":
            status_key, reason = "INACTIVE", "inactive"
        else:
            status_key, reason = "UNKNOWN", entry["first"]
        if sleeve is None:
            reason = "uncertified_strategy" if reason is None else reason
        isin_class = (
            catalog.isin_class(uid)
            if entry is not None and entry["status"] == "ACTIVE"
            else "n/a"
        )
        members.append(
            {"uid": uid, "sleeve": sleeve, "status_key": status_key, "reason": reason}
        )
        partition[(sleeve or "uncertified", isin_class, status_key)] += 1
        if reason is not None:
            reasons[(sleeve or "uncertified", reason)] += 1
    duplicates = sum(1 for count in seen.values() if count > 1)
    usable = [
        m
        for m in members
        if m["sleeve"] is not None and m["status_key"] == "ACTIVE_DAILY"
    ]
    sleeves = {}
    for sleeve, quota in sorted(builder["stage1_quotas"].items()):
        margin = max(1, math.ceil(STAGE1_MARGIN * quota)) if quota > 0 else 0
        available = sum(1 for m in usable if m["sleeve"] == sleeve)
        sleeves[sleeve] = {
            "quota": quota,
            "margin": margin,
            "required": quota + margin,
            "available_daily_active": available,
            "sufficient": available >= quota + margin,
        }
    checks = {
        "cohort_nonempty": bool(members),
        "cohort_unique": duplicates == 0,
        "every_exclusion_has_reason": all(
            m["reason"] is not None
            or (m["sleeve"] is not None and m["status_key"] == "ACTIVE_DAILY")
            for m in members
        ),
        "sleeve_quota_margin": all(item["sufficient"] for item in sleeves.values()),
    }
    detail = {
        "cohort_rows": len(cohort["rows"]),
        "cohort_distinct": len(members),
        "duplicate_uuids": duplicates,
        "partition": {"|".join(key): value for key, value in sorted(partition.items())},
        "exclusion_reasons": {
            "|".join(key): value for key, value in sorted(reasons.items())
        },
        "sleeves": sleeves,
        "margin_contract": STAGE1_MARGIN_TEXT,
        "eligible_cohort_size": len(usable),
        "eligible_cohort_sha256": _set_digest(m["uid"] for m in usable),
        "light_revision": builder["light_revision"],
        "cohort_query_sha256": cohort["query_sha256"],
        "cohort_parameters_sha256": cohort["parameters_sha256"],
        "cohort_rows_sha256": cohort["rows_sha256"],
    }
    return _gate(checks), detail, usable


def _aware(value) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _sec_value_valid(row: dict) -> bool:
    """A usable correspondence row: exact class/series patterns, non-empty ticker."""
    class_id, series_id, ticker = (
        _upper(row["class_id"]),
        _upper(row["series_id"]),
        _upper(row["ticker"]),
    )
    return bool(
        class_id is not None
        and SEC_CLASS_RE.fullmatch(class_id)
        and series_id is not None
        and SEC_SERIES_RE.fullmatch(series_id)
        and ticker is not None
    )


def _sec_class_valid(row: dict) -> bool:
    class_id = _upper(row["class_id"])
    return class_id is not None and SEC_CLASS_RE.fullmatch(class_id) is not None


def _sec_row_poisoned(row: dict) -> bool:
    """A malformed POPULATED identifier: invalid/empty class or invalid series.

    An empty series or ticker is incompleteness (partial), not poison.
    """
    series_id = _upper(row["series_id"])
    return not _sec_class_valid(row) or (
        series_id is not None and SEC_SERIES_RE.fullmatch(series_id) is None
    )


def _a8(sec, verdict: dict, catalog: Catalog, config: dict, decision_at):
    """SEC class/series/ticker corroboration of EVERY ACTIVE (MMF included).

    Returns ``(gate, detail, private_differences)``. Every related row of an
    ACTIVE (same ticker, same series+ticker, or its declared class) is
    classified BEFORE a match is declared, so a valid companion never hides a
    malformed, contradicting or incomplete related row. Per ACTIVE:
    poison (malformed populated class/series) > contradiction (a populated
    identifier differing from the ACTIVE) > ambiguous (duplicates or more than
    one valid mapping) > partial (an incomplete related row) > matched
    (exactly one valid complete related row) > missing. Failures dominate
    gaps: poison/contradiction/ambiguity/duplicate/future/stale → FAIL;
    otherwise any missing/partial → NOT_EVALUATED; only a complete, fresh,
    unique valid mapping of every ACTIVE → PASS. Never vacuously PASS.
    Unrelated poison is counted and excluded, never fatal.
    """
    unavailable = {"source_contract": SEC_SOURCE_CONTRACT, "relation": SEC_RELATION}
    if not isinstance(sec, dict) or sec.get("state") != "captured":
        code = sec.get("code") if isinstance(sec, dict) else None
        return (
            _gate({}, evaluated=False, code=code or "sec_not_captured"),
            unavailable,
            {},
        )
    if (config.get("sec") or {}).get("max_synced_age_days") != SEC_MAX_SYNCED_AGE_DAYS:
        return (
            _gate({}, evaluated=False, code="sec_freshness_criterion_absent"),
            unavailable,
            {},
        )
    decided = _aware(decision_at)
    if decided is None:
        return (
            _gate({}, evaluated=False, code="sec_decision_time_unverifiable"),
            unavailable,
            {},
        )
    rows = sec["rows"]
    lineage = sec["lineage"]
    max_age = dt.timedelta(days=SEC_MAX_SYNCED_AGE_DAYS)
    raw_keys = Counter(
        (row["class_id"], row["series_id"], row["ticker"], row["synced_at"])
        for row in rows
    )
    duplicate_source_rows = sum(count - 1 for count in raw_keys.values() if count > 1)
    invalid_rows = [row for row in rows if not _sec_value_valid(row)]
    by_ticker, by_class, by_pair = (
        defaultdict(list),
        defaultdict(list),
        defaultdict(list),
    )
    for position, row in enumerate(rows):
        ticker, class_id, series_id = (
            _upper(row["ticker"]),
            _upper(row["class_id"]),
            _upper(row["series_id"]),
        )
        if ticker is not None:
            by_ticker[ticker].append(position)
        if class_id is not None:
            by_class[class_id].append(position)
        if ticker is not None and series_id is not None:
            by_pair[(series_id, ticker)].append(position)
    future_rows = sum(
        1
        for row in rows
        if _aware(row["synced_at"]) is not None and _aware(row["synced_at"]) > decided
    )
    outcome = Counter()
    per_uid: dict[str, str] = {}
    matched_times: list[dt.datetime] = []
    related_invalid: set[int] = set()
    active_duplicates = 0
    stale_uids, future_uids = [], []
    active = sorted(u for u, e in verdict.items() if e["status"] == "ACTIVE")
    for uid in active:
        reg = catalog.rows["reg"][uid][0]
        series = _upper(reg["sec_series_id"])
        ticker = _upper(reg["ticker"])
        declared = _upper(reg["sec_class_id"])
        related = set(by_ticker.get(ticker, [])) | set(
            by_pair.get((series, ticker), [])
        )
        if declared is not None:
            related |= set(by_class.get(declared, []))
        related = sorted(related)
        # Every related row is classified before any match is declared.
        poisoned = [p for p in related if _sec_row_poisoned(rows[p])]
        usable = [p for p in related if not _sec_row_poisoned(rows[p])]
        valid_related = [p for p in usable if _sec_value_valid(rows[p])]
        incomplete_related = [p for p in usable if not _sec_value_valid(rows[p])]
        # Exact-duplicate related rows are duplicates even when identical.
        related_keys = Counter(
            (
                rows[p]["class_id"],
                rows[p]["series_id"],
                rows[p]["ticker"],
                rows[p]["synced_at"],
            )
            for p in related
        )
        duplicated = any(count > 1 for count in related_keys.values())

        def _differs(value, expected) -> bool:
            # Only a POPULATED identifier can contradict; empty is partial.
            return value is not None and value != expected

        contradiction = (
            declared is not None and SEC_CLASS_RE.fullmatch(declared) is None
        ) or any(
            _differs(_upper(rows[p]["series_id"]), series)
            or _differs(_upper(rows[p]["ticker"]), ticker)
            or (declared is not None and _upper(rows[p]["class_id"]) != declared)
            for p in usable
        )
        pair_classes = {
            _upper(rows[p]["class_id"])
            for p in by_pair.get((series, ticker), [])
            if p in valid_related
        }
        if poisoned:
            result = "poisoned_active_mapping"
            related_invalid.update(poisoned)
        elif contradiction:
            result = "contradiction"
        elif duplicated or len(pair_classes) > 1 or len(valid_related) > 1:
            result = "ambiguous"
        elif incomplete_related:
            result = "partial"
        elif len(valid_related) == 1:
            result = "matched"
        else:
            result = "missing"
        if duplicated:
            active_duplicates += 1
        if result == "matched":
            synced = _aware(rows[valid_related[0]]["synced_at"])
            matched_times.append(synced)
            if synced > decided:
                future_uids.append(uid)
            elif decided - synced > max_age:
                stale_uids.append(uid)
        outcome[result] += 1
        per_uid[uid] = result
    excluded_poisoned = sum(
        1
        for position, row in enumerate(rows)
        if _sec_row_poisoned(row) and position not in related_invalid
    )
    # An empty source leaves every ACTIVE "missing" (NOT_EVALUATED), never PASS.
    lineage_ok = lineage["row_count"] == len(rows) and sec["row_count"] == len(rows)
    min_matched = min(matched_times) if matched_times else None
    max_matched = max(matched_times) if matched_times else None
    freshness = {
        "decision_at": decided.isoformat(),
        "max_synced_age_days": SEC_MAX_SYNCED_AGE_DAYS,
        "min_matched_synced_at": None
        if min_matched is None
        else min_matched.isoformat(),
        "max_matched_synced_at": None
        if max_matched is None
        else max_matched.isoformat(),
        "valid_until": None
        if min_matched is None
        else (min_matched + max_age).isoformat(),
        "lineage_max_synced_at": lineage["max_synced_at"],
    }
    detail = {
        "source_contract": SEC_SOURCE_CONTRACT,
        "relation": SEC_RELATION,
        "timestamp_column": SEC_TIMESTAMP_COLUMN,
        "sec_query_sha256": sec["query_sha256"],
        "sec_lineage_query_sha256": sec["lineage_query_sha256"],
        "sec_query_contract_sha256": sec["query_contract_sha256"],
        "sec_rows_sha256": sec["rows_sha256"],
        "sec_row_count": sec["row_count"],
        "lineage": lineage,
        "invalid_source_rows": len(invalid_rows),
        "excluded_poisoned_count": excluded_poisoned,
        "duplicate_source_rows": duplicate_source_rows,
        "active_poisoned_count": outcome.get("poisoned_active_mapping", 0),
        "outcomes": {name: outcome.get(name, 0) for name in A8_OUTCOMES},
        "freshness": freshness,
    }
    checks = {
        "source_lineage_verified": lineage_ok,
        "active_nonempty": bool(active),
        "all_active_matched": bool(active) and outcome.get("matched", 0) == len(active),
        "zero_contradictions": outcome.get("contradiction", 0) == 0
        and outcome.get("ambiguous", 0) == 0,
        "zero_active_poisoned": outcome.get("poisoned_active_mapping", 0) == 0,
        "zero_active_duplicates": active_duplicates == 0,
        "synced_not_in_future": not future_uids and future_rows == 0,
        "fresh_within_max_age": not stale_uids,
    }
    private = {
        "per_active_outcome": {u: r for u, r in per_uid.items() if r != "matched"},
        "stale_matched": stale_uids,
        "future_matched": future_uids,
    }
    demonstrated_failure = not all(
        checks[name]
        for name in (
            "source_lineage_verified",
            "active_nonempty",
            "zero_contradictions",
            "zero_active_poisoned",
            "zero_active_duplicates",
            "synced_not_in_future",
            "fresh_within_max_age",
        )
    )
    gaps = outcome.get("missing", 0) + outcome.get("partial", 0)
    if not demonstrated_failure and gaps:
        return (
            _gate(checks, evaluated=False, code="sec_active_mapping_incomplete"),
            detail,
            private,
        )
    return _gate(checks), detail, private


def _canary(usable: list, catalog: Catalog, builder: dict, salt: str) -> dict:
    ranked = sorted(
        usable,
        key=lambda m: _sha((salt + m["uid"]).encode("utf-8")),
    )
    n = min(CANARY_MAX, len(ranked))
    chosen: list[str] = []

    def take(candidates):
        for member in candidates:
            if member["uid"] not in chosen and len(chosen) < n:
                chosen.append(member["uid"])
                return True
        return False

    strata = {}
    for isin_class in ISIN_CLASSES:
        for fund_type in CANARY_TYPES:
            pool = [
                m
                for m in ranked
                if catalog.isin_class(m["uid"]) == isin_class
                and catalog.fund_type(m["uid"]) == fund_type
            ]
            strata[f"{isin_class}|{fund_type}"] = len(pool)
            take(pool)
    for sleeve, quota in sorted(builder["stage1_quotas"].items()):
        if quota > 0 and not any(
            m["sleeve"] == sleeve and m["uid"] in chosen for m in ranked
        ):
            take([m for m in ranked if m["sleeve"] == sleeve])
    for member in ranked:
        if len(chosen) >= n:
            break
        take([member])
    return {
        "allowlist": sorted(chosen),
        "size": len(chosen),
        "cohort_size": len(ranked),
        "strata": strata,
    }


# ── audit core ───────────────────────────────────────────────────────────────
def _expected_counts(
    sources: dict,
    verdict: dict,
    catalog: Catalog,
    *,
    structural: list,
    structural_daily: list,
    structural_unknown: list,
    active_ids: list,
    daily_ids: list,
) -> dict:
    """Every generation count recomputed from the source rows (no classifier)."""
    status = _count_dict(entry["status"] for entry in verdict.values())
    inactive = status.get("INACTIVE", 0)
    presence = _count_dict(catalog.isin_class(u) for u in active_ids)
    presence_daily = _count_dict(catalog.isin_class(u) for u in daily_ids)
    return {
        "instruments_universe": len(sources["instruments"]),
        "funds_v": len(sources["funds"]),
        "instrument_identity": len(sources["identity"]),
        "instrument_evidence": len(verdict),
        "fund_status": status,
        "valuation_frequency": _count_dict(
            "daily" if entry["daily"] else "unknown" for entry in verdict.values()
        ),
        "identity_first_failure": _count_dict(
            e["first"] for e in verdict.values() if e["status"] == "UNKNOWN"
        ),
        "inactive_reason": {"inactive_without_funds_v": inactive} if inactive else {},
        "structural_pre_claims": len(structural),
        "structural_pre_claims_daily": len(structural_daily),
        "structural_claim_failures": len(structural_unknown),
        "active": len(active_ids),
        "active_daily": len(daily_ids),
        "isin_presence_active": {k: presence.get(k, 0) for k in ISIN_CLASSES},
        "isin_presence_active_daily": {
            k: presence_daily.get(k, 0) for k in ISIN_CLASSES
        },
    }


SNAPSHOT_KEYS = frozenset(
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
CALENDAR_CONTINUITY_FIELDS = (
    "calendar_id",
    "calendar_version",
    "calendar_source",
    "calendar_digest",
    "calendar_session_count",
    "coverage_start",
    "coverage_end",
    "sessions",
    "timezone",
    "source_reference",
    "valid_through",
)
PREVIOUS_STATUSES = frozenset({"ACTIVE", "INACTIVE", "UNKNOWN"})


def _load_snapshot(raw: bytes) -> tuple[dict, dict]:
    """Hostile snapshot: object/labels/rows validated before any lookup."""
    snapshot = _strict_json(raw, "source_snapshot_contract_invalid")
    sources = snapshot.get("sources")
    counts = snapshot.get("row_counts")
    if (
        set(snapshot) != SNAPSHOT_KEYS
        or not isinstance(sources, dict)
        or set(sources) != {"funds", "identity", "instruments"}
        or not isinstance(counts, dict)
        or set(counts) != {"funds", "identity", "instruments"}
        or any(type(v) is not int or v < 0 for v in counts.values())
        or any(not isinstance(rows, list) for rows in sources.values())
    ):
        raise AuditBlocked("source_snapshot_contract_invalid")
    try:
        rows = {
            name: _source_rows(sources[name], name, from_database=False)
            for name in ("instruments", "funds", "identity")
        }
    except AuditBlocked as exc:
        raise AuditBlocked("source_snapshot_contract_invalid") from exc
    return snapshot, rows


def _load_previous(raw: bytes) -> tuple[dict, list]:
    """Hostile previous artifact; identity is computed, never trusted."""
    previous = _strict_json(raw, "previous_policy_invalid")
    evidence = previous.get("instrument_evidence")
    generation = previous.get("generation")
    if (
        not isinstance(previous.get("policy_id"), str)
        or not previous["policy_id"].strip()
        or not isinstance(previous.get("policy_version"), str)
        or not previous["policy_version"].strip()
        or not isinstance(evidence, list)
        or ("generation" in previous and not isinstance(generation, dict))
        or any(
            not isinstance(row, dict)
            or not isinstance(row.get("instrument_id"), str)
            or not LOWER_UUID.match(row["instrument_id"])
            or row.get("fund_status") not in PREVIOUS_STATUSES
            for row in evidence
        )
        or len({row["instrument_id"] for row in evidence}) != len(evidence)
    ):
        raise AuditBlocked("previous_policy_invalid")
    computed = _policy_hash(previous)
    if (
        isinstance(generation, dict)
        and "policy_hash" in generation
        and (generation["policy_hash"] != computed)
    ):
        raise AuditBlocked("previous_policy_invalid")
    return previous, [previous["policy_id"], previous["policy_version"], computed]


def _deadline_check(policy: dict) -> bool:
    """valid_through is aware and equals the latest session nav_due_at instant."""
    valid = _aware(policy.get("valid_through"))
    sessions = policy.get("sessions")
    if valid is None or not isinstance(sessions, list) or not sessions:
        return False
    dues = []
    for session in sessions:
        due = _aware(session.get("nav_due_at")) if isinstance(session, dict) else None
        if due is None:
            return False
        dues.append(due)
    return valid == max(dues)


def audit(
    policy_raw: bytes,
    *,
    snapshot_raw: bytes | None = None,
    live: dict | None = None,
    capture_raw: bytes | None = None,
    previous_raw: bytes | None = None,
    config_raw: bytes | None = None,
) -> dict:
    """Pure audit core.

    ``live`` is an in-memory capture (``capture_live`` or a fake) and is
    canonicalized exactly like ``capture_raw``, the persisted bundle bytes; the
    dossier always hashes the canonical bundle, never a transient structure.
    """
    config = _load_config(config_raw)
    if live is not None and capture_raw is not None:
        raise AuditBlocked("capture_source_ambiguous")
    if live is not None:
        capture_raw = capture_bytes(live, config)
    capture = None if capture_raw is None else load_capture(capture_raw, config)
    policy = _strict_json(policy_raw, "policy_not_json", require_object=False)
    if not isinstance(policy, dict):
        raise AuditBlocked("artifact_not_object")
    if not isinstance(policy.get("generation"), dict):
        raise AuditBlocked("policy_generation_missing")
    generation = policy["generation"]
    evidence = policy.get("instrument_evidence")
    if not isinstance(evidence, list) or not all(isinstance(r, dict) for r in evidence):
        raise AuditBlocked("policy_evidence_missing")
    if (
        policy.get("generator_version") in RETIRED_GENERATORS
        or generation.get("generator_version") in RETIRED_GENERATORS
    ):
        raise AuditBlocked("retired_generator_version")
    previous, previous_identity = (
        (None, None) if previous_raw is None else _load_previous(previous_raw)
    )
    differences: dict = {}

    # Sources: snapshot export and/or the capture bundle (same canonical form).
    snapshot = None
    if snapshot_raw is not None:
        snapshot, file_sources = _load_snapshot(snapshot_raw)
    capture_sources = None if capture is None else capture["sources"]
    if snapshot is None and capture_sources is None:
        raise AuditBlocked("source_input_required")
    sources = file_sources if snapshot is not None else capture_sources
    for name, rows in sources.items():
        if len(rows) > ROW_CEILING:
            raise AuditBlocked("source_row_ceiling_exceeded")
    source_sha = _snapshot_sha(sources)
    live_sha = None if capture is None else capture["source_snapshot_sha256"]

    catalog = Catalog(sources)
    verdict = classify(catalog)
    divergent = catalog.projection_divergent()
    active_ids = sorted(u for u, e in verdict.items() if e["status"] == "ACTIVE")
    daily_ids = sorted(u for u in active_ids if verdict[u]["daily"])
    structural = [
        u
        for u in verdict
        if verdict[u]["status"] != "INACTIVE" and not catalog.failures(u, claims=False)
    ]
    structural_daily = [u for u in structural if catalog.fund_type(u) in DAILY_TYPES]
    structural_unknown = [u for u in structural if verdict[u]["status"] == "UNKNOWN"]
    expected_counts = _expected_counts(
        sources,
        verdict,
        catalog,
        structural=structural,
        structural_daily=structural_daily,
        structural_unknown=structural_unknown,
        active_ids=active_ids,
        daily_ids=daily_ids,
    )

    # A1 integrity, continuity, state and deadline.
    evidence_ids = [row.get("instrument_id") for row in evidence]
    generated_at = generation.get("generated_at")
    a1 = {
        "canonical_bytes": policy_raw == _document_bytes(policy),
        "generator_version": policy.get("generator_version") == EXPECTED_GENERATOR
        and generation.get("generator_version") == EXPECTED_GENERATOR,
        "provider_contract": policy.get("provider_contract") == EXPECTED_PROVIDER
        and generation.get("provider_contract") == EXPECTED_PROVIDER,
        "query_version": generation.get("source_query_version")
        == EXPECTED_QUERY_VERSION,
        "query_sha256": generation.get("source_query_sha256") == QUERY_SHA256,
        "evidence_reference": bool(evidence)
        and all(
            row.get("evidence_reference") == EXPECTED_REFERENCE for row in evidence
        ),
        "evidence_shape": all(
            set(row) == EVIDENCE_KEYS
            and row.get("known_at") == generated_at
            and row.get("effective_at") == generated_at
            for row in evidence
        ),
        "calendar_package": generation.get("calendar_package")
        == EXPECTED_CALENDAR_PACKAGE,
        "policy_hash": generation.get("policy_hash") == _policy_hash(policy),
        "evidence_digest": generation.get("instrument_evidence_digest")
        == _evidence_digest(evidence),
        "generation_digest": generation.get("generation_sha256")
        == _generation_digest(generation),
        "source_snapshot_sha256": generation.get("source_snapshot_sha256")
        == source_sha,
        "publication_state_approved": policy.get("publication_state") == "approved",
        "timezone_new_york": policy.get("timezone") == "America/New_York",
        "source_reference_present": isinstance(policy.get("source_reference"), str)
        and bool(policy["source_reference"].strip()),
        "valid_through_equals_last_deadline": _deadline_check(policy),
    }
    if snapshot is not None:
        a1["snapshot_link"] = (
            snapshot["kind"] == SNAPSHOT_KIND
            and snapshot["generator_version"] == EXPECTED_GENERATOR
            and snapshot["source_query_version"] == EXPECTED_QUERY_VERSION
            and snapshot["source_query_sha256"] == QUERY_SHA256
            and snapshot["source_snapshot_sha256"] == source_sha
            and snapshot["policy_id"] == policy.get("policy_id")
            and snapshot["policy_version"] == policy.get("policy_version")
            and snapshot["policy_hash"] == generation.get("policy_hash")
            and snapshot["policy_generation_sha256"]
            == generation.get("generation_sha256")
            and snapshot["policy_artifact_sha256"] == _sha(policy_raw)
            and snapshot["decision_at"] == generated_at
            and snapshot["row_counts"]
            == {name: len(rows) for name, rows in file_sources.items()}
            and all(
                snapshot["sources"][name]
                == sorted(snapshot["sources"][name], key=_compact)
                for name in ("funds", "identity", "instruments")
            )
        )
    if previous is not None:
        a1["hash_distinct_from_previous"] = (
            generation.get("policy_hash") != previous_identity[2]
        )
        # Exact string equality (an equivalent instant spelled differently fails).
        a1["calendar_identical_to_previous"] = all(
            field in previous and policy.get(field) == previous.get(field)
            for field in CALENDAR_CONTINUITY_FIELDS
        )

    # A2 partition, first-failure closure and EVERY declared generation count.
    counts = generation.get("counts")
    counts = counts if isinstance(counts, dict) else {}
    declared_first = counts.get("identity_first_failure")
    declared_first = declared_first if isinstance(declared_first, dict) else {}
    statuses = _count_dict(row.get("fund_status") for row in evidence)
    independent_status = expected_counts["fund_status"]
    independent_first = expected_counts["identity_first_failure"]
    count_mismatch = sorted(
        key
        for key in set(counts) | set(expected_counts)
        if key not in counts
        or key not in expected_counts
        or counts[key] != expected_counts[key]
        or not _int_tree_ok(counts[key])
    )
    a2 = {
        "one_row_per_uuid": len(set(evidence_ids)) == len(evidence_ids),
        "evidence_equals_universe": set(evidence_ids) == set(verdict),
        "partition_sum": sum(statuses.values()) == len(evidence) == len(verdict),
        "reported_status_counts": counts.get("fund_status") == statuses,
        "first_failure_sum_equals_unknown": sum(
            v for v in declared_first.values() if type(v) is int
        )
        == statuses.get("UNKNOWN", 0),
        "first_failure_equals_independent": counts.get("identity_first_failure")
        == independent_first,
        "generation_counts_types": _int_tree_ok(counts),
        "generation_counts_exact": not count_mismatch,
    }
    a2_detail = {
        "independent_status": independent_status,
        "count_mismatch_keys": count_mismatch,
    }
    if previous is not None:
        prev_status = {
            row["instrument_id"]: row["fund_status"]
            for row in previous["instrument_evidence"]
        }
        prev_inactive = {u for u, s in prev_status.items() if s == "INACTIVE"}
        cur_inactive = {u for u, e in verdict.items() if e["status"] == "INACTIVE"}
        exits, entries = {}, {}
        for uid in sorted(prev_inactive - cur_inactive):
            iu = catalog.rows["iu"].get(uid, [])
            if uid not in verdict:
                exits[uid] = "absent_from_universe"
            elif len(iu) != 1:
                exits[uid] = "iu_cardinality_changed"
            elif iu[0]["is_active"] is not False:
                exits[uid] = "activity_changed"
            elif catalog.rows["fv"].get(uid):
                exits[uid] = "gained_funds_v"
            else:
                exits[uid] = "unexplained"
        for uid in sorted(cur_inactive - prev_inactive):
            entries[uid] = (
                f"previous_{prev_status[uid].lower()}"
                if uid in prev_status
                else "absent_from_previous"
            )
        expected = config.get("expected_previous_inactive")
        a2["inactive_delta_explained"] = "unexplained" not in exits.values()
        a2["previous_inactive_pin"] = expected is None or expected == len(prev_inactive)
        a2_detail["inactive_continuity"] = {
            "previous_inactive": len(prev_inactive),
            "current_inactive": len(cur_inactive),
            "exits": _count_dict(exits.values()),
            "entries": _count_dict(entries.values()),
        }
        differences["inactive_exits"] = exits
        differences["inactive_entries"] = entries

    # A3 continuity of the previously ACTIVE set.
    policy_status = {row.get("instrument_id"): row for row in evidence}
    if previous is None:
        a3_gate, a3_detail = (
            _gate({}, evaluated=False, code="previous_policy_absent"),
            {},
        )
    else:
        preserved, explained, unexplained = [], {}, []
        for uid in sorted(u for u, s in prev_status.items() if s == "ACTIVE"):
            row = policy_status.get(uid)
            entry = verdict.get(uid)
            if (
                row is not None
                and row.get("fund_status") == "ACTIVE"
                and entry
                and entry["status"] == "ACTIVE"
            ):
                preserved.append(uid)
            elif entry is None:
                explained[uid] = "absent_from_sources"
            elif entry["status"] != "ACTIVE":
                explained[uid] = entry["first"] or entry["status"].lower()
            else:
                unexplained.append(uid)
        a3_gate = _gate({"no_unexplained_loss": not unexplained})
        a3_detail = {
            "previous_active": len(preserved) + len(explained) + len(unexplained),
            "preserved": len(preserved),
            "explained": _count_dict(explained.values()),
            "unexplained": len(unexplained),
        }
        differences["previous_active_explained"] = explained
        differences["previous_active_unexplained"] = unexplained

    # A4: the ceiling bounds P (structural daily pre-claims), B is the drift
    # baseline (v1 structural daily without ISIN) and D the ACTIVE daily set.
    # D ⊆ P ⊆ B with B∖P explained by pre-claim and P∖D by claim failures.
    _b_total, baseline_set = catalog.v1_structural_sets()
    p_set, d_set = set(structural_daily), set(daily_ids)
    b_minus_p, p_minus_d = baseline_set - p_set, p_set - d_set
    pre_claim_first = _count_dict(
        verdict[u]["first"] if u in verdict else "absent" for u in b_minus_p
    )
    claim_first = _count_dict(
        verdict[u]["first"] if u in verdict else "absent" for u in p_minus_d
    )
    if config_raw is None:
        ceiling = DEFAULT_STRUCTURAL_DAILY_CEILING
    else:
        ceiling = config[STRUCTURAL_DAILY_CEILING_KEY]
    baseline = config.get("structural_baseline")
    delta = None if baseline is None else len(baseline_set) - baseline
    accepted = config.get("accepted_structural_delta")

    def _family(uid, families):
        entry = verdict.get(uid)
        return (
            entry is not None
            and entry["status"] == "UNKNOWN"
            and entry["first"] is not None
            and entry["first"].split(".", 1)[0] in families
        )

    a4 = {
        "structural_baseline_accepted": delta in (None, 0) or accepted == delta,
        # An accepted baseline delta never raises the ceiling on P.
        "structural_daily_within_ceiling": len(p_set) <= ceiling,
        "active_daily_subset_of_structural_daily": d_set <= p_set,
        "structural_daily_subset_of_baseline": p_set <= baseline_set,
        "baseline_first_failure_closure": all(
            _family(u, PRE_CLAIM_FAMILIES) for u in b_minus_p
        )
        and all(_family(u, CLAIM_FAMILIES) for u in p_minus_d)
        and not (b_minus_p & p_minus_d)
        and not (b_minus_p & d_set)
        and not (p_minus_d & d_set)
        and len(baseline_set) == len(d_set) + len(b_minus_p) + len(p_minus_d),
        "structural_reconciled_by_claim_failures": len(structural) - len(active_ids)
        == len(structural_unknown)
        and all(
            verdict[u]["first"].split(".")[0] in CLAIM_FAMILIES
            for u in structural_unknown
        ),
    }
    a4_detail = {
        "ceiling_population": "structural_pre_claims_daily",
        "baseline_count": len(baseline_set),
        "structural_daily_count": len(p_set),
        "active_daily_count": len(d_set),
        "structural_daily_ceiling": ceiling,
        "structural_baseline": baseline,
        "structural_delta": delta,
        "accepted_structural_delta": accepted,
        "pre_claim_first_failure": pre_claim_first,
        "claim_first_failure": claim_first,
    }
    if set(a4_detail) != set(A4_DETAIL_KEYS):  # pragma: no cover - contract guard
        raise AuditBlocked("audit_contract_literal_mismatch")

    # A5 independence: exact sets, digests, per-UUID lifecycle/frequency, reasons.
    policy_active = sorted(
        u for u, r in policy_status.items() if r.get("fund_status") == "ACTIVE"
    )
    policy_daily = sorted(
        u
        for u, r in policy_status.items()
        if r.get("fund_status") == "ACTIVE" and r.get("valuation_frequency") == "daily"
    )
    lifecycle_mismatch = sorted(
        u
        for u in set(policy_status) | set(verdict)
        if u not in policy_status
        or u not in verdict
        or policy_status[u].get("fund_status") != verdict[u]["status"]
        # Exact expected frequency: an ACTIVE mmf is "unknown", never "weekly".
        or policy_status[u].get("valuation_frequency")
        != ("daily" if verdict[u]["daily"] else "unknown")
    )
    generated_instant = _aware(generated_at)
    captured_instant = None if capture is None else _aware(capture["captured_at"])
    a5 = {
        "active_set_equal": policy_active == active_ids,
        "active_daily_set_equal": policy_daily == daily_ids,
        "active_digest": generation.get("active_set_sha256")
        == _set_digest(active_ids)
        == _set_digest(policy_active),
        "active_daily_digest": generation.get("active_daily_set_sha256")
        == _set_digest(daily_ids)
        == _set_digest(policy_daily),
        "lifecycle_equal": not lifecycle_mismatch,
        "reason_counts_equal": counts.get("identity_first_failure")
        == independent_first,
        "isin_presence_equal": counts.get("isin_presence_active")
        == expected_counts["isin_presence_active"]
        and counts.get("isin_presence_active_daily")
        == expected_counts["isin_presence_active_daily"],
        "no_source_drift": live_sha is None
        or live_sha == generation.get("source_snapshot_sha256"),
        # The audit clock is the capture's DB clock; generation must precede it.
        "generation_precedes_capture": capture is None
        or (
            generated_instant is not None
            and captured_instant is not None
            and generated_instant <= captured_instant
        ),
    }
    differences["lifecycle_mismatch"] = lifecycle_mismatch

    # A6 contradictions (every gate, not only the first failure).
    active_violations = sorted(
        u
        for u in policy_active
        if u not in verdict or verdict[u]["all"] or catalog.inactive(u)
    )
    flag_violations = sorted(
        u
        for u, r in policy_status.items()
        if (
            r.get("fund_status") == "ACTIVE"
            and (
                r.get("identity_verified") is not True
                or r.get("currency_verified") is not True
                or r.get("return_basis_verified")
                is not (r.get("valuation_frequency") == "daily")
                or r.get("valuation_frequency") not in ("daily", "unknown")
            )
        )
        or (
            r.get("fund_status") != "ACTIVE"
            and (
                r.get("identity_verified") is not False
                or r.get("currency_verified") is not False
                or r.get("return_basis_verified") is not False
                or r.get("valuation_frequency") != "unknown"
            )
        )
    )
    a6 = {
        "active_passes_every_gate": not active_violations,
        "flags_consistent": not flag_violations,
        "no_projection_divergence": not divergent,
    }
    differences["a6_active_violations"] = {
        u: sorted(verdict[u]["all"]) if u in verdict else ["absent"]
        for u in active_violations
    }
    differences["a6_flag_violations"] = flag_violations
    differences["projection_divergent"] = divergent

    # A7/A8 need a capture of the SAME source snapshot as the policy.
    builder = config.get("builder")
    same_snapshot = live_sha is not None and live_sha == generation.get(
        "source_snapshot_sha256"
    )
    drifted = {"state": "not_captured", "code": "live_capture_absent_or_drifted"}
    cohort = capture["cohort"] if same_snapshot else drifted
    sec = capture["sec"] if same_snapshot else drifted
    a7_gate, a7_detail, usable = _a7(cohort, builder, verdict, catalog)
    a8_gate, a8_detail, sec_private = _a8(
        sec,
        verdict,
        catalog,
        config,
        None if capture is None else capture["captured_at"],
    )
    differences["sec"] = sec_private

    # Deterministic canary selection, fixed BEFORE the dossier is serialized;
    # the dossier carries only its digest (no manifest hash, no cycle).
    selection = None
    salt = config.get("canary_salt")
    if builder is not None and isinstance(salt, str) and salt and a7_detail:
        selection = _canary(usable, catalog, builder, salt)

    gates = {
        "A1": _gate(a1),
        "A2": _gate(a2),
        "A3": a3_gate,
        "A4": _gate(a4),
        "A5": _gate(a5),
        "A6": _gate(a6),
        "A7": a7_gate,
        "A8": a8_gate,
    }
    canary_strata = {
        f"{c}|{t}": sum(
            1
            for u in daily_ids
            if catalog.isin_class(u) == c and catalog.fund_type(u) == t
        )
        for c in ISIN_CLASSES
        for t in CANARY_TYPES
    }
    capture_inputs = None
    if capture is not None:
        capture_inputs = {
            "capture_bundle_sha256": _sha(capture_raw),
            "captured_at": capture["captured_at"],
            "source_snapshot_sha256": capture["source_snapshot_sha256"],
            "source_row_counts": capture["source_row_counts"],
            "cohort": {
                k: capture["cohort"][k]
                for k in (
                    "state",
                    "code",
                    "row_count",
                    "rows_sha256",
                    "query_sha256",
                    "parameters_sha256",
                )
            },
            "sec": {
                k: capture["sec"].get(k)
                for k in (
                    "state",
                    "code",
                    "source_contract",
                    "relation",
                    "timestamp_column",
                    "query_sha256",
                    "lineage_query_sha256",
                    "query_contract_sha256",
                    "row_count",
                    "rows_sha256",
                    "lineage",
                )
            },
        }
    report = {
        "audit_version": AUDIT_VERSION,
        "inputs": {
            "audit_contract_version": AUDIT_CONTRACT_VERSION,
            "audit_contract_sha256": AUDIT_CONTRACT_SHA256,
            "policy_artifact_sha256": _sha(policy_raw),
            "policy_id": policy.get("policy_id"),
            "policy_version": policy.get("policy_version"),
            "policy_hash": generation.get("policy_hash"),
            "policy_generation_sha256": generation.get("generation_sha256"),
            "source_snapshot_file_sha256": None
            if snapshot_raw is None
            else _sha(snapshot_raw),
            "source_snapshot_sha256": source_sha,
            "live_source_snapshot_sha256": live_sha,
            "capture": capture_inputs,
            "previous_policy_sha256": None
            if previous_raw is None
            else _sha(previous_raw),
            "previous_policy_identity": previous_identity,
            "audit_config_sha256": None if config_raw is None else _sha(config_raw),
            "verifier_source_sha256": _sha(Path(__file__).read_bytes()),
            "source_query_sha256": QUERY_SHA256,
            "sec_source_contract": SEC_SOURCE_CONTRACT,
            "sec_query_contract_sha256": SEC_QUERY_CONTRACT_SHA256,
        },
        "gates": gates,
        "details": {
            "A2": a2_detail,
            "A3": a3_detail,
            "A4": a4_detail,
            "A5": {
                "active_set_sha256": _set_digest(active_ids),
                "active_daily_set_sha256": _set_digest(daily_ids),
                "lifecycle_mismatch": len(lifecycle_mismatch),
            },
            "A6": {
                "active_violations": len(active_violations),
                "flag_violations": len(flag_violations),
                "projection_divergent": len(divergent),
            },
            "A7": a7_detail,
            "A8": a8_detail,
            "canary_selection_sha256": None
            if selection is None
            else selection_digest(selection),
        },
        "counts": {
            "evidence": len(evidence),
            "fund_status": independent_status,
            "active_daily": len(daily_ids),
            "identity_first_failure": independent_first,
            "isin_presence_active": expected_counts["isin_presence_active"],
            "canary_strata_active_daily": canary_strata,
        },
        "differences": differences,
    }
    # Private, never serialized: needed only to derive a canary manifest.
    report["_canary_selection"] = selection
    report["_catalog"] = catalog
    return report


def verdict_of(report: dict, *, strict: bool) -> bool:
    """Strict: every gate PASS with exactly the contract's check set, all True."""
    gates = report["gates"]
    if strict:
        return set(gates) == set(GATE_CHECKS) and all(
            gates[name]["status"] == "PASS"
            and set(gates[name]["checks"]) == set(GATE_CHECKS[name])
            and all(value is True for value in gates[name]["checks"].values())
            for name in GATE_CHECKS
        )
    core = all(
        gates[name]["status"] == "PASS" for name in ("A1", "A2", "A4", "A5", "A6")
    )
    return core and all(
        gates[name]["status"] in ("PASS", "NOT_EVALUATED")
        for name in ("A3", "A7", "A8")
    )


def _serializable(report: dict) -> dict:
    return {k: v for k, v in report.items() if not k.startswith("_")}


def _json_default(value):
    if isinstance(value, set):
        return sorted(value)
    raise TypeError("unserializable")


def report_bytes(report: dict) -> bytes:
    return (
        json.dumps(
            _serializable(report),
            sort_keys=True,
            separators=(",", ":"),
            default=_json_default,
        )
        + "\n"
    ).encode("ascii")


def build_canary(
    report: dict, policy_raw: bytes, config: dict, *, audit_report_sha256: str
) -> dict:
    """Manifest bound to the persisted dossier, capture and audited selection."""
    if not verdict_of(report, strict=True):
        raise AuditBlocked("canary_requires_strict_audit_pass")
    if not isinstance(audit_report_sha256, str) or not HEX64.match(audit_report_sha256):
        raise AuditBlocked("canary_report_digest_invalid")
    salt = config.get("canary_salt")
    selection = report.get("_canary_selection")
    if not isinstance(salt, str) or not salt or selection is None:
        raise AuditBlocked("canary_salt_missing")
    digest = selection_digest(selection)
    if digest != report["details"]["canary_selection_sha256"]:
        raise AuditBlocked("canary_selection_mismatch")
    if not 1 <= selection["size"] <= CANARY_MAX or set(selection) != set(
        CANARY_SELECTION_KEYS
    ):
        raise AuditBlocked("canary_selection_empty")
    policy = json.loads(policy_raw)
    a7 = report["details"]["A7"]
    inputs = report["inputs"]
    return {
        "kind": CANARY_KIND,
        "audit_contract_version": AUDIT_CONTRACT_VERSION,
        "audit_contract_sha256": AUDIT_CONTRACT_SHA256,
        "audit_report_sha256": audit_report_sha256,
        "audit_config_sha256": inputs["audit_config_sha256"],
        "verifier_source_sha256": inputs["verifier_source_sha256"],
        "sec_source_contract": SEC_SOURCE_CONTRACT,
        "sec_query_contract_sha256": SEC_QUERY_CONTRACT_SHA256,
        "capture_bundle_sha256": inputs["capture"]["capture_bundle_sha256"],
        "eligible_cohort_sha256": a7["eligible_cohort_sha256"],
        "eligible_cohort_size": a7["eligible_cohort_size"],
        "light_revision": a7["light_revision"],
        "cohort_query_sha256": a7["cohort_query_sha256"],
        "policy_id": policy["policy_id"],
        "policy_version": policy["policy_version"],
        "policy_hash": policy["generation"]["policy_hash"],
        "policy_artifact_sha256": _sha(policy_raw),
        "active_daily_set_sha256": policy["generation"]["active_daily_set_sha256"],
        "salt": salt,
        "max_size": CANARY_MAX,
        "selection_sha256": digest,
        **selection,
    }


# ── live read-only capture ───────────────────────────────────────────────────
class _RowCeilingExceeded(Exception):
    pass


def drain_bounded(fetchmany, *, ceiling: int = ROW_CEILING, batch: int = FETCH_BATCH):
    """Read at most ``ceiling + batch`` rows; the sentinel row aborts."""
    rows: list = []
    while True:
        chunk = fetchmany(batch)
        if not chunk:
            return rows
        rows.extend(chunk)
        if len(rows) > ceiling:
            raise _RowCeilingExceeded


def capture_live(dsn: str, config: dict) -> dict:
    """One REPEATABLE READ READ ONLY snapshot: sources, cohort and SEC classes."""
    import psycopg
    from psycopg.rows import dict_row

    def fetch(cur, query, params=None):
        cur.execute(query, params)
        rows = cur.fetchall()  # every fixed query carries LIMIT ceiling + 1
        if len(rows) > ROW_CEILING:
            raise AuditBlocked("capture_row_ceiling_exceeded")
        return rows

    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            try:
                cur.execute("SET LOCAL statement_timeout='30s'")
                cur.execute("SET LOCAL lock_timeout='1s'")
                cur.execute(
                    "SELECT clock_timestamp() AS at, current_setting('transaction_read_only') AS ro, "
                    "txid_current_if_assigned() AS xid"
                )
                meta = cur.fetchone()
                if meta["ro"] != "on" or meta["xid"] is not None:
                    raise AuditBlocked("capture_not_read_only")
                for name, relation in SOURCE_RELATION.items():
                    cur.execute(
                        "SELECT has_table_privilege(current_user, %s, 'SELECT') AS ok",
                        (relation,),
                    )
                    if cur.fetchone()["ok"] is not True:
                        raise AuditBlocked("capture_source_privilege_missing")
                sources = {name: fetch(cur, SOURCE_SQL[name]) for name in SOURCE_SQL}
                cohort = {"state": "not_configured", "code": "builder_config_absent"}
                builder = config.get("builder")
                if builder is not None:
                    problem = cohort_query_problem(builder.get("cohort_query"))
                    if problem is not None:  # _load_config already enforces this
                        raise AuditBlocked(problem)
                    wrapped = (
                        COHORT_WRAPPER_PREFIX
                        + builder["cohort_query"]
                        + COHORT_WRAPPER_SUFFIX
                    )
                    cur.execute("SAVEPOINT audit_cohort")
                    try:
                        with conn.cursor(
                            name="nav_identity_audit_cohort", row_factory=dict_row
                        ) as server:
                            server.execute(
                                wrapped, builder.get("cohort_parameters") or {}
                            )
                            rows = drain_bounded(server.fetchmany)
                        cur.execute("RELEASE SAVEPOINT audit_cohort")
                        cohort = {"state": "captured", "rows": rows}
                    except _RowCeilingExceeded:
                        cur.execute("ROLLBACK TO SAVEPOINT audit_cohort")
                        cohort = {
                            "state": "failed",
                            "code": "cohort_row_ceiling_exceeded",
                        }
                    except psycopg.Error as exc:
                        cur.execute("ROLLBACK TO SAVEPOINT audit_cohort")
                        cohort = {
                            "state": "failed",
                            "code": f"cohort_query_failed:{exc.sqlstate}",
                        }
                # A8 source: exactly SEC_RELATION; absence, missing privilege,
                # missing column or truncation is explicit unavailability and
                # never triggers a switch to another relation.
                sec = {"state": "unavailable", "code": "sec_relation_missing"}
                cur.execute(
                    "SELECT to_regclass(%s) IS NOT NULL AS present", (SEC_RELATION,)
                )
                if cur.fetchone()["present"]:
                    cur.execute(
                        "SELECT has_table_privilege(current_user, %s, 'SELECT') AS ok",
                        (SEC_RELATION,),
                    )
                    if cur.fetchone()["ok"] is not True:
                        sec = {"state": "unavailable", "code": "sec_privilege_missing"}
                    else:
                        cur.execute("SAVEPOINT audit_sec")
                        try:
                            cur.execute(SEC_SQL)
                            rows = cur.fetchall()  # the pinned SQL carries LIMIT 100001
                            if len(rows) > ROW_CEILING:
                                raise _RowCeilingExceeded
                            cur.execute(SEC_LINEAGE_SQL)
                            lineage = cur.fetchone()
                            cur.execute("RELEASE SAVEPOINT audit_sec")
                            sec = {
                                "state": "captured",
                                "rows": rows,
                                "lineage": {
                                    "row_count": lineage["row_count"],
                                    "min_synced_at": lineage["min_synced_at"],
                                    "max_synced_at": lineage["max_synced_at"],
                                },
                            }
                        except _RowCeilingExceeded:
                            cur.execute("ROLLBACK TO SAVEPOINT audit_sec")
                            sec = {
                                "state": "failed",
                                "code": "sec_row_ceiling_exceeded",
                            }
                        except psycopg.Error as exc:
                            cur.execute("ROLLBACK TO SAVEPOINT audit_sec")
                            sec = {
                                "state": "failed",
                                "code": f"sec_query_failed:{exc.sqlstate}",
                            }
                cur.execute("SELECT txid_current_if_assigned() AS xid")
                if cur.fetchone()["xid"] is not None:
                    raise AuditBlocked("capture_assigned_xid")
                return {
                    "captured_at": meta["at"].isoformat(),
                    "sources": sources,
                    "cohort": cohort,
                    "sec": sec,
                }
            finally:
                cur.execute("ROLLBACK")


# ── CLI ──────────────────────────────────────────────────────────────────────
def _write(path: Path, content: bytes, custody_root: Path) -> None:
    write_artifact(path, content, force=False, build=True, custody_root=custody_root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--policy-file", type=Path, required=True)
    parser.add_argument("--source-snapshot-file", type=Path)
    parser.add_argument(
        "--dsn-env", help="env var with a READ ONLY capable DSN (never printed)"
    )
    parser.add_argument(
        "--capture-output",
        type=Path,
        help="private capture bundle; required with --dsn-env",
    )
    parser.add_argument(
        "--capture-file", type=Path, help="persisted capture bundle (offline)"
    )
    parser.add_argument("--audit-config", type=Path)
    parser.add_argument("--previous-policy-file", type=Path)
    parser.add_argument("--previous-policy-sha256")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--custody-root", type=Path, required=True)
    parser.add_argument(
        "--canary-output", type=Path, help="implies --strict; refused unless all PASS"
    )
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args(argv)
    strict = args.strict or args.canary_output is not None
    try:
        outputs = [
            path
            for path in (args.output, args.capture_output, args.canary_output)
            if path is not None
        ]
        resolved = [path.expanduser().absolute() for path in outputs]
        if len(set(resolved)) != len(resolved):
            raise AuditBlocked("output_paths_collide")
        if any(os.path.lexists(path) for path in resolved):
            raise AuditBlocked("artifact_already_exists")
        if args.dsn_env is not None and args.capture_file is not None:
            raise AuditBlocked("capture_source_ambiguous")
        if args.dsn_env is not None and args.capture_output is None:
            raise AuditBlocked("capture_output_required")
        if args.dsn_env is None and args.capture_output is not None:
            raise AuditBlocked("capture_output_requires_dsn")
        policy_raw = args.policy_file.read_bytes()
        snapshot_raw = (
            None
            if args.source_snapshot_file is None
            else args.source_snapshot_file.read_bytes()
        )
        config_raw = (
            None if args.audit_config is None else args.audit_config.read_bytes()
        )
        config = _load_config(config_raw)
        if args.canary_output is not None and (
            config.get("builder") is None or not config.get("canary_salt")
        ):
            raise AuditBlocked("canary_config_incomplete")
        previous_raw = None
        if (args.previous_policy_file is None) != (args.previous_policy_sha256 is None):
            raise AuditBlocked("previous_policy_pin_incomplete")
        if args.previous_policy_file is not None:
            previous_raw = args.previous_policy_file.read_bytes()
            if (
                not HEX64.match(args.previous_policy_sha256)
                or _sha(previous_raw) != args.previous_policy_sha256
            ):
                raise AuditBlocked("previous_policy_sha256_mismatch")
        capture_raw = None
        if args.dsn_env is not None:
            if not ENV_NAME.match(args.dsn_env) or not os.environ.get(args.dsn_env):
                raise AuditBlocked("dsn_environment_missing")
            capture_raw = capture_bytes(
                capture_live(os.environ[args.dsn_env], config), config
            )
            # Persist the exact inputs BEFORE auditing them.
            _write(args.capture_output, capture_raw, args.custody_root)
        elif args.capture_file is not None:
            capture_raw = args.capture_file.read_bytes()
        report = audit(
            policy_raw,
            snapshot_raw=snapshot_raw,
            capture_raw=capture_raw,
            previous_raw=previous_raw,
            config_raw=config_raw,
        )
        passed = verdict_of(report, strict=strict)
        report["result"] = "pass" if passed else "fail"
        report["strict"] = strict
        report["canary_requested"] = args.canary_output is not None
        content = report_bytes(report)
        # The dossier is persisted for every audit result, before any canary.
        _write(args.output, content, args.custody_root)
        a7 = report["details"]["A7"]
        capture_inputs = report["inputs"]["capture"]
        summary = {
            "status": report["result"],
            "audit_version": AUDIT_VERSION,
            "audit_contract_sha256": AUDIT_CONTRACT_SHA256,
            "strict": strict,
            "audit_sha256": _sha(content),
            "gates": {name: gate["status"] for name, gate in report["gates"].items()},
            "counts": report["counts"],
            "capture": None
            if capture_inputs is None
            else {
                "capture_bundle_sha256": capture_inputs["capture_bundle_sha256"],
                "cohort_rows": capture_inputs["cohort"]["row_count"],
                "cohort_rows_sha256": capture_inputs["cohort"]["rows_sha256"],
                "sec_rows": capture_inputs["sec"]["row_count"],
                "sec_rows_sha256": capture_inputs["sec"]["rows_sha256"],
            },
            "eligible_cohort": None
            if not a7
            else {
                "size": a7["eligible_cohort_size"],
                "sha256": a7["eligible_cohort_sha256"],
            },
            "sleeves": a7.get("sleeves"),
            "sec_outcomes": report["details"]["A8"].get("outcomes"),
        }
        if args.canary_output is not None:
            if passed:
                canary = build_canary(
                    report, policy_raw, config, audit_report_sha256=_sha(content)
                )
                canary_content = (
                    json.dumps(canary, sort_keys=True, separators=(",", ":")) + "\n"
                ).encode("ascii")
                _write(args.canary_output, canary_content, args.custody_root)
                summary["canary"] = {
                    "status": "written",
                    "size": canary["size"],
                    "cohort_size": canary["cohort_size"],
                    "strata": canary["strata"],
                    "manifest_sha256": _sha(canary_content),
                }
            else:
                summary["canary"] = {
                    "status": "refused",
                    "code": "canary_requires_strict_audit_pass",
                }
        print(json.dumps(summary, sort_keys=True))
        return 0 if passed else 3
    except (AuditBlocked, PolicyGenerationError) as exc:
        print(json.dumps({"status": "blocked", "code": str(exc)}, sort_keys=True))
        return 2
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(
            json.dumps(
                {"status": "blocked", "reason": type(exc).__name__}, sort_keys=True
            )
        )
        return 2
    except Exception as exc:  # psycopg errors: sqlstate only, never the message
        sqlstate = getattr(exc, "sqlstate", None)
        if sqlstate is None and type(exc).__module__.split(".")[0] != "psycopg":
            raise
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "reason": type(exc).__name__,
                    "sqlstate": sqlstate,
                },
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
