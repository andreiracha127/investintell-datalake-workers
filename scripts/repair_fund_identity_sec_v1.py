"""Governed, SEC-proven repair of the fund catalog identity (rules R1-R9).

Companion of ``repair_fund_catalog_identity_v1`` (rules A/B: series id stored
as ISIN, registry share class aligned to the IU ticker). This repair runs AFTER
A/B and corrects only what current SEC data proves unambiguously:

* ``R1 iu_isin_edgar_identifier`` - ``instruments_universe.isin`` holding an
  EDGAR identifier (series ``S#########``, class ``C#########`` or a CIK) is not
  an ISIN under any format rule: set it NULL (a superset of rule A).
* ``R2 ticker_renamed_same_class`` - the IU ticker left current SEC data, the
  SAME share class (registry class, or the class the pinned SEC dataset ties to
  the old ticker) is current under a new ticker in the same series, and Tiingo
  serves the new ticker: rename the IU ticker (and the registry ticker when it
  still holds the old one).
* ``R3 iu_class_terminated_repoint`` (opt-in, ``--include-class-repoint``) -
  the IU ticker's class was terminated (absent from current SEC data AND from
  the current-year SEC series/class dataset) while the registry declares a live
  class of the same series whose current ticker the registry already holds and
  Tiingo serves: repoint the IU ticker to that class. The instrument's NAV history then
  belongs to another class, so a governed NAV rebase must follow.
* ``R4 conflict_resolved_by_sec`` - drop registry ``conflict_state`` keys of
  SEC identity fields (ticker/class/series/CIK) whose registry value now equals
  the single current SEC row of the instrument's ticker. When the conflict is
  only about ticker/class, the registry still holds a class SEC terminated
  (absent from current SEC data and the current-year dataset) and the IU
  ticker's single current SEC row in the registry series is one of the
  observed values, the registry takes that row's ticker/class first (the
  rule-B alignment, which B itself skips on conflicted rows).
* ``R5 activate_live_orphan`` - ``is_active=false`` fund in ``funds_v`` whose
  series has no active instrument, that is not a phase-B historical sibling,
  that carries no product exclusion
  (``exclusion_reason``/``strategic_excluded_reason``/non-institutional), whose
  ticker maps to exactly one current SEC class of its registry series, whose
  ticker has a current Tiingo price history in the pinned evidence, and whose
  series is proven NOT to be offered only through insurance-company separate
  accounts: activate it. Insurance-only is proven by the series' newest N-CEN
  (Item C.3 fund type "Underlying fund", i.e. underlying fund of a variable
  annuity/life separate account, and not an ETF) or by a pinned 485BPOS
  covering the series whose text says its shares are offered only to
  insurance separate accounts / variable contracts; such classes stay
  inactive. Without a pinned N-CEN for the series no reactivation happens.
  A registry row with a non-empty ``conflict_state``, or whose ticker another
  IU or registry row claims, is never activated (the generator would stop it
  there, and its series would count as represented).
  When R7/R8 bring a row into ``funds_v`` in the same apply, its activation is
  planned by the next run, not this one.
* ``R6 deactivate_terminated`` - ``is_active=true`` fund whose ticker and
  series are absent from current SEC data and from the current-year SEC
  series/class dataset, and whose NAV stopped more than 90 days ago: deactivate.
* ``R7 registry_series_from_sec`` - a registry row with neither series nor
  class whose ticker (equal to the IU ticker) maps to exactly one current SEC
  class: fill series, class and CIK from that row. Whether the fund then enters
  ``funds_v`` stays the decision of the existing N-PORT eligibility gate.
* ``R8 registry_series_moved_per_sec`` - the registry ticker equals the IU
  ticker, and SEC's latest sync lists that ticker exactly once, under another
  series (a fund reorganized into a new series/trust) while the registry's
  old class/series no longer carries it, and a pinned prospectus/N-PORT
  filing (not an N-CEN) shows the ticker under the new class AFTER the last
  pinned filing showing it under the old class: move registry series, class
  and CIK to SEC's current mapping (the owner chose alignment over a
  ``conflict_state`` exclusion). Instruments that are neither active nor in
  ``funds_v`` are only reported.
* ``R9 quarantine_sec_self_contradiction`` (opt-in, ``--quarantine-sec-
  contradictions``) - a ``funds_v`` fund that R8 refused because SEC's ticker
  file maps its ticker to another series while the fund's own newer filings
  keep it in the registry series (SEC contradicts itself) gets a
  ``conflict_state.sec_series_id`` entry recording both values and the
  filings. The fund then stops at ``registry.conflict_state_not_empty``
  instead of being an SEC integrity failure; R4 never clears that key while
  SEC still disagrees, and a rollback restores the row. ``funds_v`` re-keys to the new series, whose
  membership is again the eligibility gate's decision. Until the withdrawn
  row ages past the 7-day window the generator still sees both rows.

Everything else SEC cannot settle (series reorganizations, SEC source gaps,
classes chosen among phase-B siblings, non-SEC conflicts...) is reported in the
plan's ``review`` section and never changed.

Modes
-----
Both ``plan`` and ``apply`` take ``--sec-tickers-json``: SEC's
``company_tickers_mf.json`` as downloaded after the day's sync. The newest
sync batch must be exactly the sync worker's own parse of that file (see
below), and its sha256 is part of the plan digest.

``--mode plan`` (default): ONE ``REPEATABLE READ READ ONLY`` snapshot; prints
counts, the plan digest and the generator's ACTIVE count before/after the plan
(in memory). ``--plan-file`` writes the full plan (production identifiers; it
must be outside any git checkout).

``--mode apply --confirm repair_fund_identity_sec_v1 --expect-plan-sha256 H``:
ONE read-write ``REPEATABLE READ`` transaction holding the instrument-ingestion
advisory lock; re-plans on its own snapshot, refuses a different digest, writes
every change with a compare-and-swap on the before-values, records a receipt
(before/after values + evidence) per row, re-plans on the written state (must
be empty: a re-run is a no-op) and commits.

``--mode rollback --rollback-run-id R --confirm repair_fund_identity_sec_v1``:
restores the before-values of run R byte for byte (compare-and-swap on its
after-values) and records the rollback as its own run.

SEC evidence is pinned: the SEC "Investment Company Series and Class
Information" CSVs (sha256 below; ``--sec-cache-dir``) and the committed
evidence bundle (sec-api accession numbers + Tiingo observations, sha256
below). ``public.sec_company_tickers_mf`` (the daily SEC sync the NAV policy
generator reads) is the "current SEC" source, judged with the generator's
7-day freshness window in the same snapshot and, for every rule, restricted to
the newest sync batch: one sync run upserts every listed class in one
transaction (one ``now()``, so one exact ``updated_at``), and a row missing from
the newest batch was withdrawn from SEC's ticker file even while it is younger
than 7 days. That inference holds only for a complete run, and the sync takes a
``WORKER_LIMIT`` that commits a prefix: the plan therefore refuses
(``sec_latest_batch_not_the_ticker_file``) unless the newest batch equals,
class for class, the worker's parse of the ``--sec-tickers-json`` file. Rows
whose class or series is not an EDGAR identifier (the worker's
``<series>:<ticker>`` fallback key for a payload row without a class id) are
compared but never used by a rule.

Run from the repository root: ``python -m scripts.repair_fund_identity_sec_v1``.
"""

from __future__ import annotations

import argparse
import copy
import csv
import datetime as dt
import hashlib
import json
import re
import sys
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db import LOCK_INSTRUMENT_INGESTION, LOCK_SEC_COMPANY_TICKERS_MF  # noqa: E402
from src.workers._nav_policy import SEC_MAX_SYNCED_AGE  # noqa: E402

REPAIR_VERSION = "repair-fund-identity-sec-v1"
CONFIRM_TOKEN = "repair_fund_identity_sec_v1"
LEDGER_DDL = ROOT / "schemas" / "fund_identity_sec_repair_v1.sql"
EVIDENCE_PATH = ROOT / "contracts" / "fund-identity-sec" / "evidence_v1.json"
EVIDENCE_SHA256 = "a19d7d5d0865878ee0c1a3b1a70648cc50c6feb145df41ccabeb1baed520e407"
EVIDENCE_KIND = "fund-identity-sec-evidence-v1"
# Apply holds, for its whole transaction, the session locks of the NAV ingestion
# run (tickers are never renamed under a running sweep) and of the SEC ticker
# sync (the crosswalk the plan rests on cannot change before COMMIT).
APPLY_LOCKS = (LOCK_INSTRUMENT_INGESTION, LOCK_SEC_COMPANY_TICKERS_MF)
ROLLBACK_LOCKS = (LOCK_INSTRUMENT_INGESTION,)
CURRENT_DATASET_YEAR = 2026
# SEC "Investment Company Series and Class Information" (annual, all series and
# classes not yet reclassified inactive). Pinned by sha256.
SERIES_CLASS_FILES: dict[int, tuple[str, str]] = {
    2026: (
        "investment-company-series-class-2026.csv",
        "9fdb6d24157bbec44244366dfddebe2300404ab591da479cf537db884078af6a",
    ),
    2025: (
        "investment-company-series-class-2025.csv",
        "a5d267968f8f64bf64b09669179adce7d5c2b0c4911b2a3fae64e1f1ff10433c",
    ),
    2024: (
        "investment-company-series-class-2024.csv",
        "3fda2bbe552034e76a5a1c7b03017006159d8a01fb0d19b368f9db8eb08326c2",
    ),
    2023: (
        "investment_company_series_class_2023.csv",
        "42befad11a9680a44c679d9ea5ac53f7fe104960f05f979a652598a3529eb47b",
    ),
}
NAV_STALE_AFTER = dt.timedelta(days=90)
TIINGO_CURRENT_WITHIN = dt.timedelta(days=7)
EVIDENCE_MAX_AGE = dt.timedelta(days=30)
# N-CEN is annual (due 75 days after the fiscal year end): two years covers a
# late filer without accepting a fund's pre-reorganization census.
NCEN_MAX_AGE = dt.timedelta(days=730)
NCEN_UNDERLYING_FUND = "Underlying fund"
NCEN_ETF = "Exchange-Traded Fund"
_INSURANCE_TARGET = re.compile(
    r"separate accounts?|variable annuit|variable life|insurance compan|insurance contracts?|"
    r"insurance products?|insurance policies", re.I)
_ONLY_WORD = re.compile(r"\b(only|exclusively|solely)\b", re.I)
# A sentence that also names another channel does not say "insurance-only".
_OTHER_CHANNELS = re.compile(
    r"funds? of funds|collective investment|retirement plans?|institutional investors?|\b529\b|"
    r"wrap (fee|program)|advisory (accounts?|programs?)|qualified plans?|general public", re.I)
DEAD_TIINGO_STATUSES = frozenset(
    {"success_no_new", "empty", "not_found", "invalid_payload"}
)
SEC_CONFLICT_KEYS = ("ticker", "sec_class_id", "sec_series_id", "cik_padded", "cik_unpadded")
IU_COLUMNS = ("ticker", "isin", "is_active", "updated_at")
REGISTRY_COLUMNS = (
    "ticker", "sec_series_id", "sec_class_id", "cik_padded", "cik_unpadded",
    "conflict_state", "identity_sources", "updated_at",
)
REGISTRY_PLANNED = REGISTRY_COLUMNS[:-1]
RULES = (
    "R1_iu_isin_edgar_identifier",
    "R2_ticker_renamed_same_class",
    "R3_iu_class_terminated_repoint",
    "R4_conflict_resolved_by_sec",
    "R5_activate_live_orphan",
    "R6_deactivate_terminated",
    "R7_registry_series_from_sec",
    "R8_registry_series_moved_per_sec",
    "R9_quarantine_sec_self_contradiction",
)
_EDGAR_ID = re.compile(r"(?:S[0-9]{9}|C[0-9]{9}|[0-9]{1,10})\Z")
_SEC_CLASS = re.compile(r"C[0-9]{9}\Z")
_SEC_SERIES = re.compile(r"S[0-9]{9}\Z")
_NULL_TICKERS = frozenset({"", "[NULL]", "NULL", "N/A"})


class RepairError(RuntimeError):
    """A static, sanitized reason (never a DSN, payload or exception text)."""

    def __init__(self, code: str, exit_code: int = 2):
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code


# ---------------------------------------------------------------------------
# Normalization and pinned SEC history
# ---------------------------------------------------------------------------
def ident(value: object) -> str | None:
    """Trim + ASCII uppercase; empty is absence (the generator's ``_identifier``)."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.upper() if text.isascii() else text


def cik10(value: object) -> str | None:
    text = ident(value)
    if text is None or not text.isdigit():
        return None
    return text.zfill(10)


@dataclass(frozen=True)
class SecHistory:
    """The pinned SEC series/class datasets (one per year)."""

    # class_id -> [(year, series_id, cik10, ticker|None)]
    classes: dict[str, list[tuple[int, str, str | None, str | None]]]
    # TICKER -> [(year, class_id, series_id)]
    tickers: dict[str, list[tuple[int, str, str]]]
    current_year: int = CURRENT_DATASET_YEAR

    @property
    def current_classes(self) -> set[str]:
        return {
            cls for cls, rows in self.classes.items()
            if any(year == self.current_year for year, *_ in rows)
        }

    def current_series(self) -> set[str]:
        return {
            series for rows in self.classes.values()
            for year, series, _cik, _ticker in rows if year == self.current_year
        }

    def current_tickers(self) -> set[str]:
        return {
            ticker for ticker, rows in self.tickers.items()
            if any(year == self.current_year for year, *_ in rows)
        }

    def class_had_ticker(self, class_id: str, ticker: str) -> list[int]:
        return sorted({
            year for year, _series, _cik, row_ticker in self.classes.get(class_id, ())
            if row_ticker == ticker
        })


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_series_class_rows(year: int, rows: list[dict]) -> list[tuple[int, str, str, str | None, str | None]]:
    """(year, class_id, series_id, cik10, ticker) for every well-formed row."""
    out = []
    for row in rows:
        clean = {str(k).strip().lstrip("﻿"): (v or "").strip() for k, v in row.items() if k}
        class_id = ident(clean.get("Class ID"))
        series_id = ident(clean.get("Series ID"))
        if not class_id or not series_id:
            continue
        if not _SEC_CLASS.fullmatch(class_id) or not _SEC_SERIES.fullmatch(series_id):
            continue
        ticker = ident(clean.get("Class Ticker"))
        if ticker in _NULL_TICKERS:
            ticker = None
        out.append((year, class_id, series_id, cik10(clean.get("CIK Number")), ticker))
    return out


def build_history(parsed: list[tuple[int, str, str, str | None, str | None]],
                  current_year: int = CURRENT_DATASET_YEAR) -> SecHistory:
    classes: dict[str, list] = defaultdict(list)
    tickers: dict[str, list] = defaultdict(list)
    for year, class_id, series_id, cik, ticker in parsed:
        classes[class_id].append((year, series_id, cik, ticker))
        if ticker:
            tickers[ticker].append((year, class_id, series_id))
    return SecHistory(dict(classes), dict(tickers), current_year)


def load_history(cache_dir: Path) -> tuple[SecHistory, dict]:
    parsed = []
    pins = {}
    for year, (name, expected) in sorted(SERIES_CLASS_FILES.items()):
        path = cache_dir / name
        if not path.is_file():
            raise RepairError("sec_series_class_file_missing", 3)
        actual = sha256_file(path)
        if actual != expected:
            raise RepairError("sec_series_class_file_sha256_mismatch", 3)
        with path.open(encoding="utf-8-sig", errors="replace", newline="") as handle:
            parsed.extend(parse_series_class_rows(year, list(csv.DictReader(handle))))
        pins[str(year)] = {"file": name, "sha256": actual}
    return build_history(parsed), pins


@dataclass(frozen=True)
class Evidence:
    sha256: str
    tiingo: dict[str, dict]
    # (class_id, ticker) -> [filing]
    class_ticker_filings: dict[tuple[str, str], list[dict]]
    class_last_filings: dict[str, dict]
    series_last_filings: dict[str, dict]
    # series_id -> newest N-CEN entry {fund_types, accession_no, filed_at, registrant_cik}
    ncen_series: dict[str, dict] = field(default_factory=dict)
    # series_id -> [485BPOS evidence {accession_no, filed_at, quote, registrant_cik}]
    insurance_prospectus: dict[str, list[dict]] = field(default_factory=dict)


def parse_evidence(raw: bytes, sha256: str) -> Evidence:
    try:
        doc = json.loads(raw)
    except ValueError as exc:
        raise RepairError("evidence_not_json", 3) from exc
    if not isinstance(doc, dict) or doc.get("kind") != EVIDENCE_KIND:
        raise RepairError("evidence_kind_invalid", 3)
    pairs: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for filing in doc.get("class_ticker_filings", []):
        pairs[(filing["class_id"], filing["ticker"])].append(filing)
    prospectus: dict[str, list[dict]] = defaultdict(list)
    for filing in doc.get("insurance_prospectus", []):
        for series in filing.get("series_ids", []):
            prospectus[series].append(filing)
    return Evidence(
        sha256=sha256,
        tiingo={k.upper(): v for k, v in doc.get("tiingo_meta", {}).items()},
        class_ticker_filings=dict(pairs),
        class_last_filings={f["class_id"]: f for f in doc.get("class_last_filings", [])},
        series_last_filings={f["series_id"]: f for f in doc.get("series_last_filings", [])},
        ncen_series=dict(doc.get("ncen_series", {})),
        insurance_prospectus=dict(prospectus),
    )


def load_evidence(path: Path = EVIDENCE_PATH, expected: str | None = None) -> Evidence:
    if not path.is_file():
        raise RepairError("evidence_file_missing", 3)
    raw = path.read_bytes()
    actual = hashlib.sha256(raw).hexdigest()
    if actual != (expected or _pinned_evidence_sha256()):
        raise RepairError("evidence_sha256_mismatch", 3)
    return parse_evidence(raw, actual)


# ---------------------------------------------------------------------------
# Snapshot model
# ---------------------------------------------------------------------------
@dataclass
class Snapshot:
    decision_at: dt.datetime
    instruments: dict[str, dict]  # iid -> {instrument_type, ticker, isin, is_active, historical_sibling, excluded}
    registry: dict[str, dict]  # iid -> identity columns
    funds: set[str]  # instrument ids with a funds_v row
    sec: list[dict]  # {class_id, series_id, ticker, cik, synced_at}
    nav_last: dict[str, dt.date | None] = field(default_factory=dict)
    tiingo_attempts: dict[str, tuple[str, dt.date | None]] = field(default_factory=dict)


@dataclass(frozen=True)
class SecRow:
    class_id: str
    series_id: str
    ticker: str
    cik: str | None
    synced_at: dt.datetime


class CurrentSec:
    """What SEC's ticker file lists at the decision instant.

    Only rows of the newest sync batch count (its exact ``updated_at``; never
    one older than the generator's 7-day window): the upsert-only sync leaves
    withdrawn classes behind, and no rule may rename to, activate on or fill
    from a withdrawn row. ``read_snapshot`` has already proven that batch to be
    SEC's whole ticker file. Rows without EDGAR class/series identifiers are
    skipped. ``latest_by_ticker`` is kept as an alias.
    """

    def __init__(self, rows: list[dict], decision_at: dt.datetime):
        self.by_ticker: dict[str, list[SecRow]] = defaultdict(list)
        self.by_class: dict[str, list[SecRow]] = defaultdict(list)
        self.latest_by_ticker: dict[str, list[SecRow]] = defaultdict(list)
        self.series: set[str] = set()
        newest = max((r["synced_at"] for r in rows if r["synced_at"] <= decision_at), default=None)
        for row in rows:
            synced = row["synced_at"]
            if synced > decision_at or decision_at - synced > SEC_MAX_SYNCED_AGE:
                continue
            class_id, series_id, ticker = (ident(row[k]) for k in ("class_id", "series_id", "ticker"))
            if not class_id or not series_id or not ticker:
                continue
            if not _SEC_CLASS.fullmatch(class_id) or not _SEC_SERIES.fullmatch(series_id):
                continue  # e.g. the worker's "<series>:<ticker>" key: never an SEC class
            if synced != newest:
                continue  # withdrawn from SEC's ticker file before the newest sync
            sec_row = SecRow(class_id, series_id, ticker, cik10(row.get("cik")), synced)
            self.by_ticker[ticker].append(sec_row)
            self.by_class[class_id].append(sec_row)
            self.series.add(series_id)
            self.latest_by_ticker[ticker].append(sec_row)

    def unique_ticker(self, ticker: str | None) -> SecRow | None:
        rows = self.by_ticker.get(ticker or "", [])
        return rows[0] if len(rows) == 1 else None

    def unique_class(self, class_id: str | None) -> SecRow | None:
        rows = self.by_class.get(class_id or "", [])
        return rows[0] if len(rows) == 1 else None


# ---------------------------------------------------------------------------
# Planning (pure)
# ---------------------------------------------------------------------------
@dataclass
class Plan:
    options: dict
    iu_after: dict[str, dict] = field(default_factory=dict)
    registry_after: dict[str, dict] = field(default_factory=dict)
    rules: dict[tuple[str, str], list[str]] = field(default_factory=lambda: defaultdict(list))
    evidence: dict[tuple[str, str], dict] = field(default_factory=lambda: defaultdict(dict))
    review: dict[str, list[dict]] = field(default_factory=lambda: defaultdict(list))

    def changes(self, snapshot: Snapshot) -> list[dict]:
        out = []
        for relation, before_map, after_map, columns in (
            ("instruments_universe", snapshot.instruments, self.iu_after, ("ticker", "isin", "is_active")),
            ("instrument_identity", snapshot.registry, self.registry_after, REGISTRY_PLANNED),
        ):
            for iid in sorted(after_map):
                before = {c: before_map[iid].get(c) for c in columns}
                after = {c: after_map[iid].get(c) for c in columns}
                if before == after:
                    continue
                key = (relation, iid)
                out.append({
                    "relation": relation,
                    "instrument_id": iid,
                    "rules": sorted(set(self.rules[key])),
                    "before": before,
                    "after": after,
                    "evidence": self.evidence[key],
                })
        return out


def _json_default(value: object) -> str:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(type(value).__name__)


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      default=_json_default).encode("ascii")


def plan_digest(plan: Plan, snapshot: Snapshot, pins: dict) -> str:
    body = {
        "repair_version": REPAIR_VERSION,
        "options": plan.options,
        "pins": pins,
        "changes": [
            {k: c[k] for k in ("relation", "instrument_id", "rules", "before", "after")}
            for c in plan.changes(snapshot)
        ],
    }
    return hashlib.sha256(canonical(body)).hexdigest()


class _State:
    """Working copies the rules read and write, plus ownership indexes."""

    def __init__(self, snapshot: Snapshot, plan: Plan):
        self.snapshot = snapshot
        self.plan = plan
        self.iu = {iid: dict(row) for iid, row in snapshot.instruments.items()}
        self.reg = {iid: copy.deepcopy(row) for iid, row in snapshot.registry.items()}

    def iu_ticker_owners(self, ticker: str) -> set[str]:
        return {iid for iid, row in self.iu.items() if ident(row.get("ticker")) == ticker}

    def registry_ticker_owners(self, ticker: str) -> set[str]:
        return {iid for iid, row in self.reg.items() if ident(row.get("ticker")) == ticker}

    def set_iu(self, iid: str, column: str, value: object, rule: str, evidence: dict) -> None:
        self.iu[iid][column] = value
        self.plan.iu_after[iid] = self.iu[iid]
        self.plan.rules[("instruments_universe", iid)].append(rule)
        self.plan.evidence[("instruments_universe", iid)][rule] = evidence

    def set_reg(self, iid: str, column: str, value: object, rule: str, evidence: dict,
                source: SecRow | None = None) -> None:
        row = self.reg[iid]
        row[column] = value
        if source is not None and column in REGISTRY_PLANNED[:5]:
            sources = dict(row.get("identity_sources") or {})
            sources[column] = {
                "source": "sec_company_tickers_mf",
                "observed_at": source.synced_at.isoformat(),
                "repair": REPAIR_VERSION,
            }
            row["identity_sources"] = sources
        self.plan.registry_after[iid] = row
        self.plan.rules[("instrument_identity", iid)].append(rule)
        self.plan.evidence[("instrument_identity", iid)][rule] = evidence

    def review(self, bucket: str, iid: str, **facts: object) -> None:
        self.plan.review[bucket].append({"instrument_id": iid, **facts})

    def fund_ids(self) -> list[str]:
        return sorted(iid for iid, row in self.iu.items() if ident(row.get("instrument_type")) == "FUND")


def _filings(evidence: Evidence, class_id: str, ticker: str, series: str | None = None) -> list[dict]:
    """Pinned filings listing ``class_id`` under ``ticker`` (and under ``series`` when given)."""
    return [
        {k: f.get(k) for k in ("accession_no", "form_type", "filed_at", "role")}
        for f in evidence.class_ticker_filings.get((class_id, ticker), [])
        if series is None or ident(f.get("series_id")) == series
    ]


def rule_r1_isin(state: _State) -> None:
    for iid in state.fund_ids():
        value = ident(state.iu[iid].get("isin"))
        if value is not None and _EDGAR_ID.fullmatch(value):
            kind = "series_id" if value[0] == "S" else "class_id" if value[0] == "C" else "cik"
            state.set_iu(iid, "isin", None, RULES[0], {"value_kind": kind})


def _ticker_free(state: _State, iid: str, ticker: str) -> bool:
    return not (state.iu_ticker_owners(ticker) - {iid}) and not (
        state.registry_ticker_owners(ticker) - {iid}
    )


def rule_r2_renamed(state: _State, sec: CurrentSec, history: SecHistory, evidence: Evidence) -> None:
    for iid in state.fund_ids():
        reg = state.reg.get(iid)
        old = ident(state.iu[iid].get("ticker"))
        if reg is None or old is None or sec.by_ticker.get(old):
            continue
        series = ident(reg.get("sec_series_id"))
        class_id = ident(reg.get("sec_class_id"))
        if class_id is None:
            tied = {cls for _y, cls, ser in history.tickers.get(old, ()) if ser == series}
            if len(tied) != 1:
                continue
            (class_id,) = tied
        current = sec.unique_class(class_id)
        if current is None or current.ticker == old:
            continue
        years = history.class_had_ticker(class_id, old)
        filings = _filings(evidence, class_id, old) + _filings(evidence, class_id, current.ticker)
        if not years and not _filings(evidence, class_id, old):
            continue  # the old ticker is not proven to be this class
        if series is not None and current.series_id != series:
            state.review("ticker_renamed_series_moved", iid, ticker=old, new_ticker=current.ticker,
                         registry_series=series, sec_series=current.series_id)
            continue
        new = current.ticker
        if sec.unique_ticker(new) is None or not _ticker_free(state, iid, new):
            state.review("ticker_renamed_target_taken", iid, ticker=old, new_ticker=new)
            continue
        if ident(reg.get("sec_class_id")) is None and _owned_by_other(state, iid, "sec_class_id", class_id):
            # One SEC share class, one catalog identity (as R7 and R8).
            state.review("ticker_renamed_class_taken", iid, ticker=old, new_ticker=new, class_id=class_id)
            continue
        tiingo = _tiingo_current(evidence, new, state.snapshot.decision_at)
        if tiingo is None:
            # Never trade a working NAV feed for a symbol the provider does not serve yet.
            state.review("ticker_renamed_no_current_tiingo", iid, ticker=old, new_ticker=new,
                         tiingo=evidence.tiingo.get(new))
            continue
        facts = {
            "class_id": class_id, "series_id": current.series_id, "old_ticker": old,
            "new_ticker": new, "sec_synced_at": current.synced_at.isoformat(),
            "dataset_years_with_old_ticker": years, "filings": filings, **tiingo,
        }
        state.set_iu(iid, "ticker", new, RULES[1], facts)
        if ident(reg.get("ticker")) != new:
            state.set_reg(iid, "ticker", new, RULES[1], facts, current)
        if ident(reg.get("sec_class_id")) is None:
            state.set_reg(iid, "sec_class_id", class_id, RULES[1], facts, current)


def rule_r3_repoint(state: _State, sec: CurrentSec, history: SecHistory, evidence: Evidence,
                    *, enabled: bool) -> None:
    current_classes = history.current_classes
    current_tickers = history.current_tickers()
    for iid in state.fund_ids():
        reg = state.reg.get(iid)
        old = ident(state.iu[iid].get("ticker"))
        if reg is None or old is None or sec.by_ticker.get(old) or old in current_tickers:
            continue
        series, class_id = ident(reg.get("sec_series_id")), ident(reg.get("sec_class_id"))
        if series is None or class_id is None:
            continue
        tied = {cls for _y, cls, ser in history.tickers.get(old, ()) if ser == series}
        if len(tied) != 1:
            continue
        (old_class,) = tied
        if old_class == class_id:
            if class_id not in current_classes and not sec.by_class.get(class_id):
                state.review("registry_class_terminated", iid, ticker=old, series_id=series,
                             class_id=class_id)
            continue
        if old_class in current_classes or sec.by_class.get(old_class):
            continue
        live = sec.unique_class(class_id)
        if live is None or live.series_id != series:
            continue
        reg_ticker = ident(reg.get("ticker"))
        if reg_ticker != live.ticker and not history.class_had_ticker(class_id, reg_ticker or ""):
            continue
        if sec.unique_ticker(live.ticker) is None or not _ticker_free(state, iid, live.ticker):
            state.review("class_repoint_target_taken", iid, ticker=old, new_ticker=live.ticker)
            continue
        tiingo = _tiingo_current(evidence, live.ticker, state.snapshot.decision_at)
        if tiingo is None:
            state.review("class_repoint_no_current_tiingo", iid, ticker=old, new_ticker=live.ticker,
                         tiingo=evidence.tiingo.get(live.ticker))
            continue
        facts = {
            "terminated_class_id": old_class, "series_id": series, "old_ticker": old,
            "live_class_id": class_id, "new_ticker": live.ticker,
            "sec_synced_at": live.synced_at.isoformat(),
            "old_class_dataset_years": sorted({y for y, *_ in history.classes.get(old_class, ())}),
            "old_class_last_filing": evidence.class_last_filings.get(old_class),
            "nav_rebase_required": True,
            **tiingo,
        }
        if not enabled:
            state.review("class_repoint_candidate", iid, **facts)
            continue
        state.set_iu(iid, "ticker", live.ticker, RULES[2], facts)
        if reg_ticker != live.ticker:
            state.set_reg(iid, "ticker", live.ticker, RULES[2], facts, live)


def rule_r7_registry_series(state: _State, sec: CurrentSec) -> None:
    for iid in state.fund_ids():
        reg = state.reg.get(iid)
        if reg is None or ident(reg.get("sec_series_id")) or ident(reg.get("sec_class_id")):
            continue
        ticker = ident(state.iu[iid].get("ticker"))
        current = sec.unique_ticker(ticker)
        if ticker is None or current is None or ident(reg.get("ticker")) not in (None, ticker):
            continue
        stored = [reg.get(c) for c in ("cik_padded", "cik_unpadded") if ident(reg.get(c)) is not None]
        if any(cik10(value) != current.cik for value in stored):
            # A malformed or different registrant in either form is evidence to review.
            state.review("registry_cik_disagrees_with_sec", iid, ticker=ticker,
                         registry_cik=reg.get("cik_padded"),
                         registry_cik_unpadded=reg.get("cik_unpadded"), sec_cik=current.cik)
            continue
        if not _ticker_free(state, iid, ticker):
            continue
        if _owned_by_other(state, iid, "sec_class_id", current.class_id):
            state.review("registry_class_taken", iid, ticker=ticker, class_id=current.class_id)
            continue
        facts = {"ticker": ticker, "series_id": current.series_id, "class_id": current.class_id,
                 "cik": current.cik, "sec_synced_at": current.synced_at.isoformat()}
        values = {
            "ticker": ticker,
            "sec_series_id": current.series_id,
            "sec_class_id": current.class_id,
            "cik_padded": current.cik,
            "cik_unpadded": str(int(current.cik)) if current.cik else None,
        }
        for column, value in values.items():
            if value is not None and ident(reg.get(column)) != value:
                state.set_reg(iid, column, value, RULES[6], facts, current)


def _owned_by_other(state: _State, iid: str, column: str, value: str) -> bool:
    return any(
        other != iid and ident(row.get(column)) == value for other, row in state.reg.items()
    )


def _newest(filings: list[dict], *, skip_ncen: bool) -> str | None:
    """Newest filing date; ``skip_ncen`` keeps only forms that corroborate a live series.

    N-CEN (a census that relisted STNC/OIODX) and N-8F (an application to
    deregister) never show a series continuing.
    """
    dates = [
        str(f.get("filed_at"))[:10] for f in filings
        if f.get("filed_at") and not (
            skip_ncen and str(f.get("form_type", "")).upper().startswith(("N-CEN", "N-8F")))
    ]
    return max(dates) if dates else None


def _quarantine(state: _State, iid: str, reg: dict, series: str, new: SecRow,
                filings: list[dict], old_filings: list[dict]) -> None:
    conflict = reg.get("conflict_state")
    if conflict is not None and not isinstance(conflict, dict):
        # The generator rejects a non-object state on its own; never overwrite it.
        state.review("quarantine_conflict_state_not_object", iid, ticker=ident(reg.get("ticker")))
        return
    conflict = dict(conflict or {})
    if "sec_series_id" in conflict:
        return  # already recorded: a re-run is a no-op
    conflict["sec_series_id"] = {
        "values": [
            {"value": series, "source": "fund_filings", "filings": old_filings},
            {"value": new.series_id, "source": "sec_company_tickers_mf",
             "observed_at": new.synced_at.isoformat(), "filings": filings},
        ],
        "resolved": False,
        "recorded_by": REPAIR_VERSION,
    }
    state.set_reg(iid, "conflict_state", conflict, RULES[8], {
        "ticker": ident(reg.get("ticker")), "registry_series": series,
        "sec_ticker_file_series": new.series_id, "fund_filings": old_filings,
        "sec_ticker_file_filings": filings,
    })


def rule_r8_series_moved(state: _State, sec: CurrentSec, history: SecHistory,
                         evidence: Evidence, *, quarantine: bool = False) -> None:
    for iid in state.fund_ids():
        reg = state.reg.get(iid)
        ticker = ident(state.iu[iid].get("ticker"))
        if reg is None or ticker is None or ticker != ident(reg.get("ticker")):
            continue
        series, class_id = ident(reg.get("sec_series_id")), ident(reg.get("sec_class_id"))
        if series is None:
            continue  # R7's domain
        latest = sec.latest_by_ticker.get(ticker, [])
        if not latest or any(row.series_id == series for row in latest):
            continue
        if len(latest) != 1:
            state.review("series_moved_ambiguous", iid, ticker=ticker, registry_series=series,
                         sec_series=sorted({r.series_id for r in latest}))
            continue
        (new,) = latest
        if state.iu[iid].get("is_active") is not True and iid not in state.snapshot.funds:
            state.review("series_moved_inactive_instrument", iid, ticker=ticker,
                         registry_series=series, sec_series=new.series_id)
            continue
        years = sorted({
            year for year, cls, ser in history.tickers.get(ticker, ())
            if cls == new.class_id and ser == new.series_id
        })
        filings = _filings(evidence, new.class_id, ticker, new.series_id)
        old_filings = _filings(evidence, class_id, ticker, series) if class_id else []
        new_date = _newest(filings, skip_ncen=True)
        old_date = _newest(old_filings, skip_ncen=False)
        if new_date is None or (old_date is not None and new_date <= old_date):
            # SEC's ticker file and the fund's own filings disagree about the
            # live series: a choice for the owner, never a guess.
            state.review("series_moved_unproven", iid, ticker=ticker, registry_series=series,
                         sec_series=new.series_id, registry_class=class_id,
                         sec_class=new.class_id, newest_new_filing=new_date,
                         newest_old_filing=old_date)
            # Quarantine only what the fund's own filings corroborate: a pinned
            # filing under the registry class, no newer one under SEC's class.
            if quarantine and iid in state.snapshot.funds and old_date is not None:
                _quarantine(state, iid, reg, series, new, filings, old_filings)
            continue
        if _owned_by_other(state, iid, "sec_class_id", new.class_id):
            state.review("series_moved_class_taken", iid, ticker=ticker, sec_class=new.class_id)
            continue
        facts = {
            "ticker": ticker, "old_series_id": series, "old_class_id": class_id,
            "old_cik": cik10(reg.get("cik_padded")), "series_id": new.series_id,
            "class_id": new.class_id, "cik": new.cik,
            "sec_synced_at": new.synced_at.isoformat(),
            "dataset_years_new_mapping": years, "filings": filings,
            "old_class_filings": old_filings,
            "funds_v_rekeyed_to_new_series": True,
        }
        values = {
            "sec_series_id": new.series_id,
            "sec_class_id": new.class_id,
            "cik_padded": new.cik,
            "cik_unpadded": str(int(new.cik)) if new.cik else None,
        }
        for column, value in values.items():
            if value is not None and ident(reg.get(column)) != value:
                state.set_reg(iid, column, value, RULES[7], facts, new)


def _observed(conflict: dict, key: str) -> set:
    entry = conflict.get(key)
    if not isinstance(entry, dict):
        return set()
    return {ident(v.get("value")) for v in entry.get("values", []) if isinstance(v, dict)}


def _settle_dead_class(state: _State, sec: CurrentSec, history: SecHistory, iid: str,
                       reg: dict, conflict: dict, ticker: str | None) -> None:
    """Align a conflicted registry row still on a terminated class (see R4)."""
    current = sec.unique_ticker(ticker)
    old_class = ident(reg.get("sec_class_id"))
    if (
        ticker is None or current is None or ticker == ident(reg.get("ticker"))
        or not set(conflict) <= {"ticker", "sec_class_id"}
        or current.series_id != ident(reg.get("sec_series_id"))
        or old_class is None or sec.by_class.get(old_class)
        or old_class in history.current_classes
        or ticker not in _observed(conflict, "ticker")
        or ("sec_class_id" in conflict and current.class_id not in _observed(conflict, "sec_class_id"))
        or not _ticker_free(state, iid, ticker)
        or _owned_by_other(state, iid, "sec_class_id", current.class_id)
    ):
        return
    facts = {"ticker": ticker, "class_id": current.class_id, "terminated_class_id": old_class,
             "observed": {k: sorted(v for v in _observed(conflict, k) if v) for k in conflict},
             "sec_synced_at": current.synced_at.isoformat()}
    state.set_reg(iid, "ticker", ticker, RULES[3], facts, current)
    state.set_reg(iid, "sec_class_id", current.class_id, RULES[3], facts, current)


def rule_r4_conflicts(state: _State, sec: CurrentSec, history: SecHistory | None = None) -> None:
    for iid in sorted(state.reg):
        reg = state.reg[iid]
        conflict = reg.get("conflict_state")
        if not isinstance(conflict, dict) or not conflict or iid not in state.iu:
            continue
        ticker = ident(state.iu[iid].get("ticker"))
        if history is not None:
            _settle_dead_class(state, sec, history, iid, reg, conflict, ticker)
        current = sec.unique_ticker(ticker)
        if ticker is None or ticker != ident(reg.get("ticker")) or current is None:
            continue
        if ident(reg.get("sec_series_id")) != current.series_id:
            continue
        if ident(reg.get("sec_class_id")) not in (None, current.class_id):
            continue
        sec_values = {
            "ticker": current.ticker,
            "sec_class_id": current.class_id,
            "sec_series_id": current.series_id,
            "cik_padded": current.cik,
            "cik_unpadded": str(int(current.cik)) if current.cik else None,
        }
        resolved = {}
        for key in SEC_CONFLICT_KEYS:
            entry = conflict.get(key)
            if not isinstance(entry, dict) or sec_values[key] is None:
                continue
            observed = {ident(v.get("value")) for v in entry.get("values", []) if isinstance(v, dict)}
            registry_value = cik10(reg.get(key)) if key == "cik_padded" else ident(reg.get(key))
            if registry_value == sec_values[key] and sec_values[key] in observed:
                resolved[key] = sec_values[key]
        remaining = sorted(set(conflict) - set(resolved))
        if remaining:
            state.review("conflict_not_sec_resolvable", iid, keys=remaining, ticker=ticker)
        if resolved:
            new_conflict = {k: v for k, v in conflict.items() if k not in resolved}
            state.set_reg(iid, "conflict_state", new_conflict, RULES[3],
                          {"resolved_keys": resolved, "sec_synced_at": current.synced_at.isoformat()})


def _tiingo_current(evidence: Evidence, ticker: str, decision_at: dt.datetime) -> dict | None:
    meta = evidence.tiingo.get(ticker)
    if not meta or meta.get("status") != 200 or not meta.get("endDate") or not meta.get("observed_at"):
        return None
    observed = dt.datetime.fromisoformat(meta["observed_at"])
    end = dt.date.fromisoformat(str(meta["endDate"])[:10])
    if observed > decision_at + dt.timedelta(days=1) or decision_at - observed > EVIDENCE_MAX_AGE:
        return None
    if observed.date() - end > TIINGO_CURRENT_WITHIN:
        return None
    return {"tiingo_end_date": end.isoformat(), "tiingo_observed_at": meta["observed_at"]}


def prospectus_says_insurance_only(quote: object) -> bool:
    """A prospectus sentence restricting the shares to insurance separate accounts."""
    if not isinstance(quote, str):
        return False
    return bool(
        _ONLY_WORD.search(quote) and _INSURANCE_TARGET.search(quote)
        and not _OTHER_CHANNELS.search(quote)
    )


def insurance_status(evidence: Evidence, series: str, decision_at: dt.datetime) -> tuple[str, dict]:
    """``insurance_only`` / ``not_insurance`` / ``unverified`` for one series, with its proof."""
    ncen = evidence.ncen_series.get(series)
    proof: dict = {"ncen": None if not ncen else {
        k: ncen.get(k) for k in ("accession_no", "filed_at", "fund_types")}}
    covering = sorted(evidence.insurance_prospectus.get(series, []),
                      key=lambda f: str(f.get("filed_at") or ""), reverse=True)
    if covering and prospectus_says_insurance_only(covering[0].get("quote")):
        # The newest prospectus sentence covering the series speaks for it.
        proof["prospectus"] = {k: covering[0].get(k) for k in ("accession_no", "form_type", "filed_at", "quote")}
        return "insurance_only", proof
    if not ncen or not ncen.get("filed_at"):
        return "unverified", proof
    filed = dt.datetime.fromisoformat(str(ncen["filed_at"]))
    if filed.tzinfo is None:
        filed = filed.replace(tzinfo=dt.timezone.utc)
    if filed > decision_at + dt.timedelta(days=1) or decision_at - filed > NCEN_MAX_AGE:
        return "unverified", proof
    types = set(ncen.get("fund_types") or ())
    if NCEN_UNDERLYING_FUND in types and NCEN_ETF not in types:
        return "insurance_only", proof
    return "not_insurance", proof


def rule_r5_activate(state: _State, sec: CurrentSec, evidence: Evidence) -> None:
    series_of = {iid: ident(row.get("sec_series_id")) for iid, row in state.reg.items()}
    active_series = {
        series_of.get(iid) for iid, row in state.iu.items() if row.get("is_active") is True
    } - {None}
    candidates: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for iid in state.fund_ids():
        row = state.iu[iid]
        reg = state.reg.get(iid)
        if row.get("is_active") is not False or iid not in state.snapshot.funds or reg is None:
            continue
        series = series_of.get(iid)
        if series is None or series in active_series:
            continue  # the series is represented by another active instrument
        ticker = ident(row.get("ticker"))
        if row.get("historical_sibling"):
            state.review("orphan_series_historical_sibling", iid, ticker=ticker, series_id=series)
            continue
        if row.get("excluded"):
            # A product exclusion (leveraged/inverse, sub-scale, municipal...) is not an identity fact.
            state.review("orphan_deliberately_excluded", iid, ticker=ticker, series_id=series,
                         reason=row["excluded"])
            continue
        status = reg.get("resolution_status")
        if not isinstance(status, str) or status.strip() != "canonical":
            # The generator stops it at registry.status_not_canonical.
            state.review("orphan_registry_not_canonical", iid, ticker=ticker, series_id=series,
                         resolution_status=status)
            continue
        conflict = reg.get("conflict_state")
        if type(conflict) is not dict or conflict:
            # The generator stops it at registry.conflict_state_not_empty (any
            # non-object state included), and an active row would mark its
            # series as represented.
            state.review("orphan_registry_conflict", iid, ticker=ticker, series_id=series,
                         conflict_keys=sorted(conflict) if type(conflict) is dict
                         else f"<{type(conflict).__name__}>")
            continue
        current = sec.unique_ticker(ticker)
        if ticker is None or ticker != ident(reg.get("ticker")) or current is None:
            state.review("orphan_identity_not_sec_current", iid, ticker=ticker, series_id=series)
            continue
        if not _ticker_free(state, iid, ticker):
            # The generator rejects it at ticker.global_conflict, and an active row
            # would count its series as represented.
            state.review("orphan_ticker_claimed_elsewhere", iid, ticker=ticker, series_id=series)
            continue
        if current.series_id != series or ident(reg.get("sec_class_id")) not in (None, current.class_id):
            state.review("orphan_identity_not_sec_current", iid, ticker=ticker, series_id=series)
            continue
        tiingo = _tiingo_current(evidence, ticker, state.snapshot.decision_at)
        if tiingo is None:
            state.review("orphan_no_current_tiingo_nav", iid, ticker=ticker, series_id=series,
                         tiingo=evidence.tiingo.get(ticker))
            continue
        status, insurance = insurance_status(evidence, series, state.snapshot.decision_at)
        if status != "not_insurance":
            bucket = ("orphan_insurance_only_class" if status == "insurance_only"
                      else "orphan_insurance_status_unverified")
            state.review(bucket, iid, ticker=ticker, series_id=series,
                         registrant_cik=current.cik, **insurance)
            continue
        candidates[series].append((iid, {
            "ticker": ticker, "class_id": current.class_id, "series_id": series,
            "registrant_cik": current.cik, "sec_synced_at": current.synced_at.isoformat(),
            **tiingo, **insurance,
        }))
    for _series, items in sorted(candidates.items()):
        # Every candidate is individually proven live; an orphan series with two
        # live classes (e.g. an ETF share class next to the canonical class, as
        # VNQ/VGSNX and BND/VBTLX already are) gets both.
        for iid, facts in items:
            state.set_iu(iid, "is_active", True, RULES[4], {**facts, "series_candidates": len(items)})


def rule_r6_deactivate(state: _State, sec: CurrentSec, history: SecHistory, evidence: Evidence) -> None:
    current_tickers = history.current_tickers()
    current_series = history.current_series()
    decision_day = state.snapshot.decision_at.date()
    for iid in state.fund_ids():
        row = state.iu[iid]
        ticker = ident(row.get("ticker"))
        if row.get("is_active") is not True or ticker is None:
            continue
        if sec.by_ticker.get(ticker) or ticker in current_tickers:
            continue
        reg = state.reg.get(iid) or {}
        registry_series = ident(reg.get("sec_series_id"))
        series = {registry_series} if registry_series else {
            ser for _y, _cls, ser in history.tickers.get(ticker, ())
        }
        if not series:
            continue  # ticker unknown to SEC: not provable either way
        if any(s in current_series or s in sec.series for s in series):
            continue
        attempt = state.snapshot.tiingo_attempts.get(iid)
        last_nav = state.snapshot.nav_last.get(iid)
        observed = [d for d in (last_nav, attempt[1] if attempt else None) if d is not None]
        newest = max(observed) if observed else None
        if attempt is not None and attempt[0] not in DEAD_TIINGO_STATUSES:
            continue
        if newest is not None and decision_day - newest <= NAV_STALE_AFTER:
            state.review("series_terminated_nav_current", iid, ticker=ticker, series=sorted(series),
                         newest_nav=newest.isoformat())
            continue
        if newest is None:
            # Never deactivate without a dated NAV that proves the stop.
            state.review("series_terminated_no_dated_nav", iid, ticker=ticker, series=sorted(series),
                         tiingo_attempt_status=attempt[0] if attempt else None)
            continue
        facts = {
            "ticker": ticker,
            "series_ids": sorted(series),
            "newest_nav_date": newest.isoformat() if newest else None,
            "tiingo_attempt_status": attempt[0] if attempt else None,
            "series_last_filings": [evidence.series_last_filings.get(s) for s in sorted(series)
                                    if evidence.series_last_filings.get(s)],
        }
        state.set_iu(iid, "is_active", False, RULES[5], facts)


def review_catalog(state: _State, sec: CurrentSec, history: SecHistory) -> None:
    """Report-only findings for funds_v members SEC cannot settle."""
    current_tickers = history.current_tickers()
    for iid in sorted(state.snapshot.funds):
        row = state.iu.get(iid)
        reg = state.reg.get(iid)
        if row is None or reg is None:
            continue
        ticker = ident(row.get("ticker"))
        series = ident(reg.get("sec_series_id"))
        if ticker is None:
            state.review("no_ticker", iid, series_id=series)
            continue
        rows = sec.by_ticker.get(ticker, [])
        if ("instrument_identity", iid) in state.plan.rules and RULES[7] in state.plan.rules[
            ("instrument_identity", iid)
        ]:
            continue
        if rows and series and all(r.series_id != series for r in rows):
            state.review("series_reorganized", iid, ticker=ticker, registry_series=series,
                         sec_series=sorted({r.series_id for r in rows}),
                         sec_cik=sorted({r.cik for r in rows if r.cik}))
        elif not rows and ticker in current_tickers:
            state.review("sec_current_source_gap", iid, ticker=ticker, series_id=series)


def plan_repairs(snapshot: Snapshot, history: SecHistory, evidence: Evidence,
                 *, include_class_repoint: bool = False,
                 quarantine_sec_contradictions: bool = False) -> Plan:
    plan = Plan(options={
        "include_class_repoint": bool(include_class_repoint),
        "quarantine_sec_contradictions": bool(quarantine_sec_contradictions),
    })
    state = _State(snapshot, plan)
    sec = CurrentSec(snapshot.sec, snapshot.decision_at)
    rule_r1_isin(state)
    rule_r2_renamed(state, sec, history, evidence)
    rule_r3_repoint(state, sec, history, evidence, enabled=include_class_repoint)
    rule_r7_registry_series(state, sec)
    rule_r8_series_moved(state, sec, history, evidence, quarantine=quarantine_sec_contradictions)
    rule_r4_conflicts(state, sec, history)
    rule_r5_activate(state, sec, evidence)
    rule_r6_deactivate(state, sec, history, evidence)
    review_catalog(state, sec, history)
    return plan


def summarize(plan: Plan, snapshot: Snapshot) -> dict:
    changes = plan.changes(snapshot)
    by_rule: Counter[str] = Counter()
    by_relation: Counter[str] = Counter()
    in_funds: Counter[str] = Counter()
    for change in changes:
        by_relation[change["relation"]] += 1
        for rule in change["rules"]:
            by_rule[rule] += 1
            if change["instrument_id"] in snapshot.funds:
                in_funds[rule] += 1
    return {
        "rows_changed": dict(sorted(by_relation.items())),
        "changes_by_rule": dict(sorted(by_rule.items())),
        "changes_by_rule_in_funds_v": dict(sorted(in_funds.items())),
        "review": {k: len(v) for k, v in sorted(plan.review.items())},
    }


# ---------------------------------------------------------------------------
# Generator classification (in memory, no writes)
# ---------------------------------------------------------------------------
def apply_plan_to_generator_rows(plan: Plan, snapshot: Snapshot, instruments: list[dict],
                                 funds: list[dict], identity: list[dict], *,
                                 eligible_series: set[str] | None = None) -> tuple[list, list, list]:
    """The four generator sources as they would read after the plan.

    ``funds_v`` is a view over the registry gated by N-PORT eligibility. When
    ``eligible_series`` (series the gate admits) is given, a registry row whose
    series the plan sets or moves enters/keeps its ``funds_v`` row only if the
    new series is eligible; without it, ``funds_v`` membership is left as read
    (series moves keep their row, re-keyed).
    """
    iu_new, reg_new = plan.iu_after, plan.registry_after
    out_iu = []
    for row in instruments:
        row = dict(row)
        new = iu_new.get(str(row["instrument_id"]))
        if new is not None:
            row.update({k: new.get(k) for k in ("ticker", "isin", "is_active")})
        out_iu.append(row)
    out_reg = []
    for row in identity:
        row = dict(row)
        new = reg_new.get(str(row["instrument_id"]))
        if new is not None:
            row.update({k: new.get(k) for k in ("ticker", "sec_series_id", "sec_class_id", "conflict_state")})
        out_reg.append(row)
    out_funds = []
    present = set()
    for row in funds:
        row = dict(row)
        owner = str(row["instrument_id"])
        present.add(owner)
        new = reg_new.get(owner)
        if new is not None:
            # funds_v projects NULLIF(btrim(registry.ticker), '') and the registry series.
            ticker = new.get("ticker")
            row["ticker"] = ticker.strip() or None if isinstance(ticker, str) else ticker
            series = ident(new.get("sec_series_id"))
            if series != ident(row.get("series_id")):
                if eligible_series is not None and series not in eligible_series:
                    continue  # the gate would drop the re-keyed fund
                row["series_id"] = series
        out_funds.append(row)
    if eligible_series is not None:
        by_id = {str(r["instrument_id"]): r for r in out_reg}
        for owner, new in sorted(reg_new.items()):
            series = ident(new.get("sec_series_id"))
            if owner in present or series is None or series not in eligible_series:
                continue
            reg_row = by_id[owner]
            out_funds.append({
                "instrument_id": reg_row["instrument_id"], "series_id": series,
                "ticker": ident(new.get("ticker")), "isin": reg_row.get("isin"),
                "cusip": reg_row.get("cusip_9"), "currency": "USD", "fund_type": "etf",
            })
    return out_iu, out_funds, out_reg


def classify_counts(instruments, funds, identity, sec_rows, decision_at) -> dict:
    from scripts import generate_fund_nav_policy_v1 as generator

    _evidence, counts, digests = generator.classify_catalog(
        instruments, funds, identity, sec_rows, decision_at
    )
    return {
        "active": counts["active"],
        "active_daily": counts["active_daily"],
        "fund_status": counts["fund_status"],
        "active_set_sha256": digests["active_set_sha256"],
    }


# ---------------------------------------------------------------------------
# Database I/O
# ---------------------------------------------------------------------------
IU_QUERY = (
    "SELECT instrument_id, instrument_type, ticker, isin, is_active, "
    "(attributes ? 'historical_nav_ticker') AS historical_sibling, "
    "COALESCE(NULLIF(attributes->>'exclusion_reason', ''), "
    "NULLIF(attributes->>'strategic_excluded_reason', ''), "
    "CASE WHEN attributes->>'is_institutional' = 'false' THEN 'not_institutional' END) AS excluded "
    "FROM public.instruments_universe"
)
REGISTRY_QUERY = (
    "SELECT instrument_id, sec_series_id, sec_class_id, ticker, cik_padded, cik_unpadded, "
    "conflict_state, identity_sources, resolution_status FROM public.instrument_identity"
)
# funds_v rows only appear for a filled series after the eligibility view sees it;
# the in-memory "after" classification therefore never invents funds_v rows.
SEC_ROWS_QUERY = (
    "SELECT class_id, series_id, ticker, cik, updated_at AS synced_at "
    "FROM public.sec_company_tickers_mf"
)
FUNDS_IDS_QUERY = "SELECT DISTINCT instrument_id FROM public.funds_v"
TIINGO_ATTEMPTS_QUERY = (
    "SELECT DISTINCT ON (a.instrument_id) a.instrument_id, a.status, a.newest_observed_date "
    "FROM public.nav_ingestion_attempts a JOIN public.nav_ingestion_runs r USING (run_id) "
    "WHERE a.provider = 'tiingo' AND r.status = 'completed' "
    "AND a.status <> 'not_attempted_budget' "
    "ORDER BY a.instrument_id, a.persisted_at DESC"
)
NAV_LAST_QUERY = (
    "SELECT i.id AS instrument_id, "
    "(SELECT max(n.nav_date) FROM public.nav_timeseries n WHERE n.instrument_id = i.id) AS nav_last "
    "FROM unnest(%s::uuid[]) AS i(id)"
)


@dataclass(frozen=True)
class SecTickerFile:
    """SEC's ``company_tickers_mf.json`` as the sync worker parses it."""

    rows: dict[str, tuple[str, str, str]]  # worker class key -> (series_id, ticker, cik)
    sha256: str


def parse_sec_ticker_file(raw: bytes) -> SecTickerFile:
    from src.workers.sec_company_tickers_mf import parse_company_tickers_mf

    try:
        payload = json.loads(raw)
    except ValueError:
        raise RepairError("sec_ticker_file_invalid", 3) from None
    if not isinstance(payload, dict):
        raise RepairError("sec_ticker_file_invalid", 3)
    rows = {r.class_id: (r.series_id, r.ticker, r.cik) for r in parse_company_tickers_mf(payload)}
    if not rows:
        raise RepairError("sec_ticker_file_invalid", 3)
    return SecTickerFile(rows, hashlib.sha256(raw).hexdigest())


def load_sec_ticker_file(path: Path) -> SecTickerFile:
    if not path.is_file():
        raise RepairError("sec_ticker_file_missing", 3)
    return parse_sec_ticker_file(path.read_bytes())


def assert_latest_sec_batch_complete(rows: list[dict], decision_at: dt.datetime,
                                     ticker_file: SecTickerFile) -> dict:
    """Refuse unless the newest sync batch is SEC's whole ticker file.

    A ``WORKER_LIMIT`` run commits a prefix of the file under its own instant,
    and no share of rows tells a 99% prefix from a day's withdrawals. The batch
    is therefore compared, class for class, with the worker's parse of the file
    the operator downloaded after that sync: any truncation, later SEC update
    or older file refuses the plan.
    """
    fresh = [r for r in rows if r["synced_at"] <= decision_at]
    newest = max((r["synced_at"] for r in fresh), default=None)
    if newest is None or decision_at - newest > SEC_MAX_SYNCED_AGE:
        raise RepairError("sec_crosswalk_not_fresh", 3)
    batch = {str(r["class_id"]): (str(r["series_id"]), str(r["ticker"]), str(r["cik"]))
             for r in fresh if r["synced_at"] == newest}
    if batch != ticker_file.rows:
        raise RepairError("sec_latest_batch_not_the_ticker_file", 3)
    return {"newest_sync": newest.isoformat(), "latest_batch_rows": len(batch),
            "ticker_file_sha256": ticker_file.sha256}


def _nav_candidates(snapshot: Snapshot, history: SecHistory) -> list[str]:
    """Active funds whose ticker is absent from current SEC data (R6 prefilter)."""
    sec = CurrentSec(snapshot.sec, snapshot.decision_at)
    current = history.current_tickers()
    out = []
    for iid, row in snapshot.instruments.items():
        ticker = ident(row.get("ticker"))
        if (ident(row.get("instrument_type")) == "FUND" and row.get("is_active") is True
                and ticker and not sec.by_ticker.get(ticker) and ticker not in current):
            out.append(iid)
    return sorted(out)


def read_snapshot(cursor, history: SecHistory, ticker_file: SecTickerFile) -> tuple[Snapshot, tuple]:
    """Everything inside the caller's transaction (one snapshot)."""
    from scripts import generate_fund_nav_policy_v1 as generator

    cursor.execute("SELECT clock_timestamp() AS decision_at")
    decision_at = cursor.fetchone()["decision_at"]
    instruments, funds_rows, identity_rows, sec_gen = generator._catalog_rows(cursor)
    cursor.execute(IU_QUERY)
    iu = {str(r["instrument_id"]): dict(r, instrument_id=str(r["instrument_id"])) for r in cursor.fetchall()}
    cursor.execute(REGISTRY_QUERY)
    reg = {str(r["instrument_id"]): dict(r, instrument_id=str(r["instrument_id"])) for r in cursor.fetchall()}
    cursor.execute(SEC_ROWS_QUERY)
    sec_rows = cursor.fetchall()
    cursor.execute(FUNDS_IDS_QUERY)
    funds = {str(r["instrument_id"]) for r in cursor.fetchall()}
    cursor.execute(TIINGO_ATTEMPTS_QUERY)
    attempts = {
        str(r["instrument_id"]): (r["status"], r["newest_observed_date"]) for r in cursor.fetchall()
    }
    assert_latest_sec_batch_complete(sec_rows, decision_at, ticker_file)
    snapshot = Snapshot(decision_at, iu, reg, funds, sec_rows, {}, attempts)
    candidates = _nav_candidates(snapshot, history)
    if candidates:
        cursor.execute(NAV_LAST_QUERY, ([uuid.UUID(c) for c in candidates],))
        snapshot.nav_last = {str(r["instrument_id"]): r["nav_last"] for r in cursor.fetchall()}
    return snapshot, (instruments, funds_rows, identity_rows, sec_gen)


def _row_json_sql(relation: str) -> str:
    columns = IU_COLUMNS if relation == "instruments_universe" else REGISTRY_COLUMNS
    return "jsonb_build_object(" + ",".join(f"'{c}',t.{c}" for c in columns) + ")"


def _write_change(cursor, relation: str, iid: str, after_planned: dict, now_json: str,
                  before_planned: dict) -> tuple[dict, dict]:
    """CAS update of one row; returns the DB-rendered (before, after) values."""
    columns = IU_COLUMNS if relation == "instruments_universe" else REGISTRY_COLUMNS
    table = f"public.{relation}"
    cursor.execute(
        f"SELECT {_row_json_sql(relation)} AS v FROM {table} t WHERE t.instrument_id = %s FOR UPDATE",
        (uuid.UUID(iid),),
    )
    found = cursor.fetchall()
    if len(found) != 1:
        raise RepairError("apply_row_missing")
    before = found[0]["v"]
    for column, value in before_planned.items():
        if _norm(before.get(column)) != _norm(value):
            raise RepairError("apply_before_value_changed")
    after = dict(before)
    after.update({c: after_planned[c] for c in after_planned})
    after["updated_at"] = json.loads(now_json)
    _cas_update(cursor, table, columns, iid, before, after)
    return before, after


def _norm(value: object) -> object:
    return json.loads(canonical(value)) if value is not None else None


def _cas_update(cursor, table: str, columns: tuple, iid: str, expect: dict, target: dict) -> None:
    assign = ", ".join(f"{c} = a.{c}" for c in columns)
    compare = " AND ".join(f"t.{c} IS NOT DISTINCT FROM b.{c}" for c in columns)
    cursor.execute(
        f"UPDATE {table} t SET {assign} "
        f"FROM jsonb_populate_record(NULL::{table}, %s::jsonb) a, "
        f"jsonb_populate_record(NULL::{table}, %s::jsonb) b "
        f"WHERE t.instrument_id = %s AND {compare}",
        (json.dumps(target), json.dumps(expect), uuid.UUID(iid)),
    )
    if cursor.rowcount != 1:
        raise RepairError("apply_compare_and_swap_failed")


def _begin(cursor, *, read_only: bool) -> None:
    mode = "READ ONLY" if read_only else "READ WRITE"
    cursor.execute(f"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ {mode}")
    cursor.execute("SET LOCAL statement_timeout = '300s'")
    cursor.execute("SET LOCAL lock_timeout = '5s'")


def _safe_rollback(cursor) -> None:
    """End the transaction without masking the exception already in flight."""
    try:
        cursor.execute("ROLLBACK")
    except Exception:  # noqa: BLE001 - the original error is the one to report
        pass


def _session_locks(cursor, keys: tuple[int, ...]) -> None:
    """Take every advisory lock BEFORE the snapshot exists (autocommit, session scope).

    A writer that committed before the lock is visible to the snapshot taken
    after it; one that wants to write afterwards waits for the connection to
    close. Released when the connection closes.
    """
    for key in keys:
        cursor.execute("SELECT pg_try_advisory_lock(%s) AS ok", (key,))
        if cursor.fetchone()["ok"] is not True:
            raise RepairError("writer_lock_busy", 4)


def run_plan(dsn: str, history: SecHistory, evidence: Evidence, pins: dict, *,
             ticker_file: SecTickerFile, include_class_repoint: bool,
             quarantine_sec_contradictions: bool = False) -> tuple[Plan, Snapshot, dict]:
    import psycopg
    from psycopg.rows import dict_row

    # Plan mode never writes: the session itself is read-only, not just the transaction.
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10,
                         options="-c default_transaction_read_only=on") as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            _begin(cursor, read_only=True)
            try:
                snapshot, generator_rows = read_snapshot(cursor, history, ticker_file)
            finally:
                cursor.execute("ROLLBACK")
    plan = plan_repairs(snapshot, history, evidence, include_class_repoint=include_class_repoint,
                        quarantine_sec_contradictions=quarantine_sec_contradictions)
    instruments, funds_rows, identity_rows, sec_gen = generator_rows
    report = summarize(plan, snapshot)
    report["plan_sha256"] = plan_digest(plan, snapshot, pins)
    report["decision_at"] = snapshot.decision_at.isoformat()
    report["pins"] = pins
    report["generator_before"] = classify_counts(instruments, funds_rows, identity_rows, sec_gen,
                                                 snapshot.decision_at)
    after = apply_plan_to_generator_rows(plan, snapshot, instruments, funds_rows, identity_rows)
    report["generator_after"] = classify_counts(*after, sec_gen, snapshot.decision_at)
    return plan, snapshot, report


def run_apply(dsn: str, history: SecHistory, evidence: Evidence, pins: dict, *,
              ticker_file: SecTickerFile, include_class_repoint: bool, expect_sha256: str,
              quarantine_sec_contradictions: bool = False) -> dict:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            _session_locks(cursor, APPLY_LOCKS)
            _begin(cursor, read_only=False)
            try:
                cursor.execute(LEDGER_DDL.read_text(encoding="utf-8"))
                snapshot, _rows = read_snapshot(cursor, history, ticker_file)
                plan = plan_repairs(snapshot, history, evidence,
                                    include_class_repoint=include_class_repoint,
                                    quarantine_sec_contradictions=quarantine_sec_contradictions)
                digest = plan_digest(plan, snapshot, pins)
                if digest != expect_sha256:
                    raise RepairError("plan_sha256_mismatch")
                changes = plan.changes(snapshot)
                if not changes:
                    cursor.execute("ROLLBACK")
                    return {"mode": "apply", "status": "noop", "plan_sha256": digest}
                run_id = uuid.uuid4()
                cursor.execute("SELECT to_jsonb(now()) AS now")
                now_json = json.dumps(cursor.fetchone()["now"])
                summary = summarize(plan, snapshot)
                summary["pins"] = pins
                cursor.execute(
                    "INSERT INTO fund_identity_sec_repair_runs "
                    "(run_id, kind, repair_version, plan_sha256, evidence_sha256, decision_at, counts) "
                    "VALUES (%s, 'apply', %s, %s, %s, %s, %s::jsonb)",
                    (run_id, REPAIR_VERSION, digest, evidence.sha256, snapshot.decision_at,
                     json.dumps(summary)),
                )
                for change in changes:
                    before, after = _write_change(
                        cursor, change["relation"], change["instrument_id"], change["after"],
                        now_json, change["before"],
                    )
                    cursor.execute(
                        "INSERT INTO fund_identity_sec_repair_receipts "
                        "(run_id, relation, instrument_id, rules, before_values, after_values, evidence) "
                        "VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)",
                        (run_id, change["relation"], uuid.UUID(change["instrument_id"]),
                         change["rules"], json.dumps(before), json.dumps(after),
                         canonical(change["evidence"]).decode()),
                    )
                replay, _rows = read_snapshot(cursor, history, ticker_file)
                # Idempotency of THIS plan: judge the written rows with the funds_v
                # membership the plan was made on. A row R7/R8 has just admitted to
                # funds_v (the eligibility view reads the new series) is the next
                # run's to activate, after its own review.
                replay.funds = set(snapshot.funds)
                residual = plan_repairs(
                    replay, history, evidence, include_class_repoint=include_class_repoint,
                    quarantine_sec_contradictions=quarantine_sec_contradictions,
                ).changes(replay)
                if residual:
                    raise RepairError("apply_not_idempotent")
                cursor.execute("COMMIT")
            except BaseException:
                _safe_rollback(cursor)
                raise
    return {"mode": "apply", "status": "committed", "run_id": str(run_id),
            "plan_sha256": digest, **summary}


def _refuse_repoint_rollback_after_nav_writes(cursor, run_id: uuid.UUID, applied_at) -> None:
    """An R3 repoint cannot be undone in the catalog alone once NAV moved with it.

    After the repoint, ingestion appends (and a governed rebase rewrites) the
    NEW class's NAV under the instrument; restoring the old ticker would label
    those rows with the terminated class. Any successful NAV write for a
    repointed instrument after the apply run blocks the rollback, and it stays
    blocked: attempts are append-only, so a later reverse rebase cannot be told
    apart from forward writes here. Undo such a repoint forward instead (a new
    reviewed repoint plus its own governed rebase).
    """
    cursor.execute(
        "SELECT array_agg(instrument_id) AS ids FROM fund_identity_sec_repair_receipts "
        "WHERE run_id = %s AND relation = 'instruments_universe' AND %s = ANY(rules)",
        (run_id, RULES[2]),
    )
    ids = cursor.fetchone()["ids"]
    if not ids:
        return
    cursor.execute("SELECT to_regclass('public.nav_ingestion_attempts') IS NOT NULL AS present")
    if cursor.fetchone()["present"] is not True:
        return
    cursor.execute(
        "SELECT count(*) AS n FROM public.nav_ingestion_attempts "
        "WHERE instrument_id = ANY(%s) AND persisted_at > %s AND status = 'success_new'",
        (ids, applied_at),
    )
    if cursor.fetchone()["n"]:
        raise RepairError("rollback_repoint_after_nav_writes")


def run_rollback(dsn: str, run_id: str) -> dict:
    import psycopg
    from psycopg.rows import dict_row

    target = uuid.UUID(run_id)
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            _session_locks(cursor, ROLLBACK_LOCKS)
            _begin(cursor, read_only=False)
            try:
                cursor.execute(LEDGER_DDL.read_text(encoding="utf-8"))
                cursor.execute(
                    "SELECT kind, plan_sha256, evidence_sha256, created_at "
                    "FROM fund_identity_sec_repair_runs WHERE run_id = %s", (target,))
                run = cursor.fetchone()
                if run is None or run["kind"] != "apply":
                    raise RepairError("rollback_run_not_found")
                _refuse_repoint_rollback_after_nav_writes(cursor, target, run["created_at"])
                cursor.execute("SELECT clock_timestamp() AS t")
                decision_at = cursor.fetchone()["t"]
                cursor.execute(
                    "SELECT relation, instrument_id, rules, before_values, after_values "
                    "FROM fund_identity_sec_repair_receipts WHERE run_id = %s "
                    "ORDER BY relation, instrument_id", (target,))
                # Undo in the exact reverse of the apply's write order (Plan.changes:
                # instruments_universe then instrument_identity, each by instrument
                # id): every intermediate state existed, so unique keys never clash.
                apply_order = {"instruments_universe": 0, "instrument_identity": 1}
                receipts = sorted(cursor.fetchall(), reverse=True,
                                  key=lambda r: (apply_order[r["relation"]], str(r["instrument_id"])))
                new_run = uuid.uuid4()
                cursor.execute(
                    "INSERT INTO fund_identity_sec_repair_runs (run_id, kind, repair_version, "
                    "plan_sha256, evidence_sha256, rolls_back_run_id, decision_at, counts) "
                    "VALUES (%s, 'rollback', %s, %s, %s, %s, %s, %s::jsonb)",
                    (new_run, REPAIR_VERSION, run["plan_sha256"], run["evidence_sha256"], target,
                     decision_at, json.dumps({"rows_restored": len(receipts)})),
                )
                for receipt in receipts:
                    relation = receipt["relation"]
                    columns = IU_COLUMNS if relation == "instruments_universe" else REGISTRY_COLUMNS
                    iid = str(receipt["instrument_id"])
                    cursor.execute(
                        f"SELECT 1 FROM public.{relation} WHERE instrument_id = %s FOR UPDATE",
                        (uuid.UUID(iid),))
                    _cas_update(cursor, f"public.{relation}", columns, iid,
                                receipt["after_values"], receipt["before_values"])
                    cursor.execute(
                        "INSERT INTO fund_identity_sec_repair_receipts (run_id, relation, instrument_id, "
                        "rules, before_values, after_values, evidence) "
                        "VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)",
                        (new_run, relation, uuid.UUID(iid), receipt["rules"],
                         json.dumps(receipt["after_values"]), json.dumps(receipt["before_values"]),
                         json.dumps({"rollback_of": str(target)})),
                    )
                cursor.execute("COMMIT")
            except BaseException:
                _safe_rollback(cursor)
                raise
    return {"mode": "rollback", "status": "committed", "run_id": str(new_run),
            "rolled_back_run_id": run_id, "rows_restored": len(receipts)}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _pinned_evidence_sha256() -> str:
    return EVIDENCE_SHA256


def _reject_git_checkout(destination: Path) -> None:
    for ancestor in destination.resolve().parents:
        if (ancestor / ".git").exists():
            raise RepairError("plan_file_inside_git_checkout")


def override_host(dsn: str, host_port: str | None) -> str:
    """Point a URL DSN at another host:port (e.g. the public TCP proxy), keeping credentials."""
    if not host_port:
        return dsn
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(dsn)
    if not parts.scheme.startswith("postgres") or parts.username is None:
        raise RepairError("db_host_override_needs_url_dsn", 3)
    userinfo = parts.netloc.rsplit("@", 1)[0]
    scheme = "postgresql" if parts.scheme.startswith("postgres") else parts.scheme
    return urlunsplit((scheme, f"{userinfo}@{host_port}", parts.path, parts.query, parts.fragment))


def _emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True, default=_json_default))


def main(argv: list[str] | None = None) -> int:
    import os

    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--mode", choices=("plan", "apply", "rollback"), default="plan")
    parser.add_argument("--sec-cache-dir", type=Path, help="directory with the pinned SEC series/class CSVs")
    parser.add_argument("--sec-tickers-json", type=Path,
                        help="SEC company_tickers_mf.json downloaded after the newest sync (plan/apply)")
    parser.add_argument("--include-class-repoint", action="store_true",
                        help="apply R3 (terminated IU class -> registry live class; needs NAV rebase)")
    parser.add_argument("--quarantine-sec-contradictions", action="store_true",
                        help="apply R9 (record conflict_state where SEC's ticker file contradicts the fund's filings)")
    parser.add_argument("--plan-file", type=Path, help="write the full plan JSON (outside any git checkout)")
    parser.add_argument("--confirm", default=None)
    parser.add_argument("--expect-plan-sha256", default=None)
    parser.add_argument("--rollback-run-id", default=None)
    parser.add_argument("--dsn-env", default="DATABASE_URL", help="environment variable holding the DSN")
    parser.add_argument("--db-host", default=None,
                        help="host:port replacing the DSN host (e.g. the public TCP proxy); credentials kept")
    args = parser.parse_args(argv)
    try:
        dsn = os.environ.get(args.dsn_env)
        if not dsn:
            raise RepairError("dsn_missing", 3)
        dsn = override_host(dsn, args.db_host)
        if args.mode == "rollback":
            if args.confirm != CONFIRM_TOKEN or not args.rollback_run_id:
                raise RepairError("rollback_requires_confirm_and_run_id")
            _emit(run_rollback(dsn, args.rollback_run_id))
            return 0
        if args.sec_cache_dir is None:
            raise RepairError("sec_cache_dir_required", 3)
        if args.sec_tickers_json is None:
            raise RepairError("sec_tickers_json_required", 3)
        history, file_pins = load_history(args.sec_cache_dir)
        evidence = load_evidence()
        ticker_file = load_sec_ticker_file(args.sec_tickers_json)
        pins = {"series_class_files": file_pins, "evidence_sha256": evidence.sha256,
                "sec_ticker_file_sha256": ticker_file.sha256}
        if args.mode == "apply":
            if args.confirm != CONFIRM_TOKEN or not args.expect_plan_sha256:
                raise RepairError("apply_requires_confirm_and_plan_sha256")
            _emit(run_apply(dsn, history, evidence, pins, ticker_file=ticker_file,
                            include_class_repoint=args.include_class_repoint,
                            expect_sha256=args.expect_plan_sha256,
                            quarantine_sec_contradictions=args.quarantine_sec_contradictions))
            return 0
        plan, snapshot, report = run_plan(dsn, history, evidence, pins, ticker_file=ticker_file,
                                          include_class_repoint=args.include_class_repoint,
                                          quarantine_sec_contradictions=args.quarantine_sec_contradictions)
        if args.plan_file is not None:
            _reject_git_checkout(args.plan_file)
            payload = {"report": report, "changes": plan.changes(snapshot), "review": plan.review}
            with args.plan_file.open("x", encoding="utf-8") as handle:
                json.dump(payload, handle, sort_keys=True, indent=1, default=_json_default)
        _emit({"mode": "plan", **report})
        return 0
    except RepairError as exc:
        _emit({"mode": args.mode, "status": "refused", "reason": exc.code})
        return exc.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
