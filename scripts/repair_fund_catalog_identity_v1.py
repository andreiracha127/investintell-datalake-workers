"""Governed, idempotent repair of two legacy fund catalog identity defects (v1).

Both defects come from rows a one-off ``universe_sync`` wrote in 2026-03/04;
no current writer in this repository or in Light produces them. They make the
NAV policy generator (``scripts.generate_fund_nav_policy_v1``) classify most
funds UNKNOWN, which keeps them out of the builder:

(a) ``instruments_universe.isin`` holds the SEC series id (``^S[0-9]{9}$``)
    instead of an ISIN (first failure ``isin.unsupported_prefix``). It is set
    to NULL only where it equals that instrument's
    ``instrument_identity.sec_series_id``.
(b) ``instrument_identity.ticker``/``sec_class_id`` name another share class
    of the same series than ``instruments_universe.ticker`` (first failure
    ``ticker.mismatch``). NAV ingestion fetches by the IU ticker, so the
    instrument IS the IU class and the registry is aligned to it, only when:
    the registry row is ``canonical`` with an empty ``conflict_state``; exactly
    one fresh SEC class of the SAME series carries the IU ticker; the
    generator's own SEC judge admits (IU ticker, series, that class) as exactly
    one fresh complete row (no contradiction/ambiguity/staleness, so a repaired
    fund can never become an A8 integrity failure); and neither the ticker, the
    (series, ticker) pair nor the class is claimed by another instrument. The
    rule is iterated to a fixpoint, so a re-run after apply plans nothing.
    ``funds_v`` projects the registry, so it follows automatically.
    Class-level claims (registry ISIN/CUSIP/FIGI) are NOT touched: on the
    production candidates every one with ``sec_cusip_ticker_map`` evidence
    belongs to the IU ticker (they were resolved before the registry ticker was
    overwritten), so they are already the IU class's identifiers.

Modes (one at a time):

* default, dry run: ONE ``REPEATABLE READ READ ONLY`` snapshot of the four
  generator sources (+ the ``funds_profile_mv`` cohort), the exact plan and its
  digest, exclusion counts by reason, the generator's classification before
  and after the plan, and the NAV-policy publication gates (A4 structural
  ceiling/baseline, A8 SEC bounds) on the repaired catalog. Stdout carries
  aggregates and digests only; ``--plan-output`` writes the per-row plan
  (instrument ids, before/after values) to a NEW file.
* ``--apply --confirm repair_fund_catalog_identity_v1 --plan-sha256 HEX``: ONE
  transaction. Locks ``instruments_universe`` and ``instrument_identity``
  (SHARE ROW EXCLUSIVE) and the SEC crosswalk (SHARE, so its evidence cannot
  change before COMMIT), then the NAV writer advisory locks (ingestion ->
  readiness) and the crosswalk writer's lock; recomputes the plan and refuses
  unless its digest (which covers the SEC evidence) equals
  ``--plan-sha256``; refuses if any ACTIVE fund would be demoted or a
  repaired registry row would end in an SEC integrity failure (contradictions
  that (a) merely unmasks pre-exist and are reported, never written), writes
  one append-only receipt per changed row
  (exact before/after values, ``updated_at`` and ``identity_sources``
  included; ``schemas/fund_catalog_identity_repair_v1.sql``), applies the
  updates with a compare-and-swap on the before-values, re-reads the catalog
  in the same transaction and requires an empty re-plan plus the predicted
  classification, then COMMITs. An empty plan is a no-op (nothing written).
* ``--rollback RUN_ID --confirm repair_fund_catalog_identity_v1``: ONE
  transaction restoring every before-value of that apply run exactly, only if
  every touched row still holds the run's after-values (``updated_at``
  included, so a row any other writer touched since is refused); appends its own run
  and receipts. A second rollback of the same run is a no-op.

DSN only from ``--dsn-env`` (default ``NAV_READINESS_DATABASE_URL``).

Exit codes: 0 planned/applied/no-op/rolled back; 2 validation, plan mismatch,
guard or SQL failure (everything rolled back); 3 privilege or catalog source
incompatible; 4 lock busy (nothing written).

Run from the repository root:
``python -m scripts.repair_fund_catalog_identity_v1 [--plan-output PATH]``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from scripts import generate_fund_nav_policy_v1 as generator
from scripts import verify_fund_nav_identity_v2 as auditor
from scripts.nav_identity_audit_contract import (
    DEFAULT_STRUCTURAL_DAILY_CEILING,
    SEC_EXCLUSION_FRACTION_DENOMINATOR,
    SEC_EXCLUSION_FRACTION_NUMERATOR,
    SEC_INTEGRITY_CODES,
    SEC_MISSING_CODE,
    SEC_STALE_CODE,
)
from src.db import (
    LOCK_FUND_NAV_READINESS,
    LOCK_INSTRUMENT_INGESTION,
    LOCK_SEC_COMPANY_TICKERS_MF,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = ROOT / "schemas" / "fund_catalog_identity_repair_v1.sql"
REPAIR_VERSION = "repair_fund_catalog_identity_v1"
CONFIRM_TOKEN = REPAIR_VERSION
PLAN_KIND = "fund-catalog-identity-repair-plan-v1"
RULE_ISIN = "iu_isin_is_registry_series_id"
RULE_TICKER = "registry_class_aligned_to_iu_ticker"
RULE_ROLLBACK = "rollback"
MAX_PASSES = 8
DEFAULT_DSN_ENV = "NAV_READINESS_DATABASE_URL"
COHORT_RELATION = "public.funds_profile_mv"
EXIT_OK, EXIT_FAILED, EXIT_INCOMPATIBLE, EXIT_LOCK_BUSY = 0, 2, 3, 4
_SERIES = re.compile(r"S[0-9]{9}\Z")
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_text = generator._text
_identifier = generator._identifier


class RepairError(Exception):
    """A static, sanitized reason code; never carries DSNs or row values."""

    def __init__(self, code: str, exit_code: int = EXIT_FAILED):
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code


class LockBusy(RepairError):
    def __init__(self, code: str = "lock_busy"):
        super().__init__(code, EXIT_LOCK_BUSY)


# ── pure planning ────────────────────────────────────────────────────────────


@dataclass
class RepairPlan:
    """Planned changes (raw before-values for compare-and-swap) and the repaired rows."""

    isin_changes: list[dict]
    ticker_changes: list[dict]
    excluded: dict[str, Counter]
    passes: int
    instruments: list[dict]
    funds: list[dict]
    identity: list[dict]
    evidence: dict[str, str] = field(default_factory=dict)

    def changes(self) -> dict:
        """The approved document: rows, values AND the SEC evidence they rest on.

        ``sec_evidence`` is the ``synced_at`` of the SEC row proving each
        ticker repair (it is written into ``identity_sources``), so a crosswalk
        refresh between review and apply changes the digest and needs a new
        approval.
        """
        return {
            "repair_version": REPAIR_VERSION,
            "instruments_universe": sorted(
                self.isin_changes, key=lambda row: row["instrument_id"]
            ),
            "instrument_identity": sorted(
                self.ticker_changes, key=lambda row: row["instrument_id"]
            ),
            "sec_evidence": dict(sorted(self.evidence.items())),
        }

    def sha256(self) -> str:
        return hashlib.sha256(generator.canonical_json(self.changes())).hexdigest()

    def empty(self) -> bool:
        return not self.isin_changes and not self.ticker_changes

    def counts(self) -> dict:
        return {
            "instruments_universe_isin_to_null": len(self.isin_changes),
            "instrument_identity_ticker_and_class": len(self.ticker_changes),
            "ticker_passes": self.passes,
        }


def _by_id(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["instrument_id"]].append(row)
    return grouped


def _plan_isin(
    instruments: list[dict], identity: list[dict]
) -> tuple[list[dict], Counter]:
    """(a): IU ISIN equal to the instrument's own registry series id -> NULL."""
    registry = _by_id(identity)
    iu_rows = _by_id(instruments)
    changes, excluded = [], Counter()
    for row in instruments:
        value = _identifier(row["isin"])
        if value is None or not _SERIES.fullmatch(value):
            continue
        owner = row["instrument_id"]
        regs = registry.get(owner, [])
        if len(iu_rows[owner]) != 1:
            excluded["iu_row_duplicate"] += 1
        elif len(regs) != 1:
            excluded["registry_row_missing_or_duplicate"] += 1
        elif _identifier(regs[0]["sec_series_id"]) != value:
            excluded["registry_series_differs"] += 1
        else:
            changes.append(
                {"instrument_id": owner, "isin_before": row["isin"], "isin_after": None}
            )
    return changes, excluded


def _plan_ticker_pass(
    instruments: list[dict],
    funds: list[dict],
    identity: list[dict],
    sec_index: generator._SecIndex,
) -> tuple[list[dict], Counter, dict[str, str]]:
    """One pass of (b) over the current rows; returns admitted changes and exclusions."""
    index = generator._CatalogIndex(instruments, funds, identity)
    class_owners: dict[str, set[str]] = defaultdict(set)
    for row in identity:
        class_id = _identifier(row["sec_class_id"])
        if class_id is not None:
            class_owners[class_id].add(row["instrument_id"])
    batch, excluded, evidence = [], Counter(), {}
    for owner in sorted(index.registry):
        regs, ius = index.registry[owner], index.iu.get(owner, [])
        if not ius:
            continue
        if len(regs) != 1 or len(ius) != 1:
            excluded["cardinality_not_one"] += 1
            continue
        registry, iu = regs[0], ius[0]
        ticker, current = _identifier(iu["ticker"]), _identifier(registry["ticker"])
        if ticker is None or current is None or ticker == current:
            continue
        if _text(registry["resolution_status"]) != "canonical":
            excluded["registry_not_canonical"] += 1
            continue
        conflict = registry["conflict_state"]
        if type(conflict) is not dict or conflict:
            excluded["registry_conflict_state_not_empty"] += 1
            continue
        series = _identifier(registry["sec_series_id"])
        if series is None or not _SERIES.fullmatch(series):
            excluded["registry_series_invalid"] += 1
            continue
        positions = [
            p for p in sec_index.by_ticker.get(ticker, ()) if sec_index.fresh[p]
        ]
        if not positions:
            excluded["sec_iu_ticker_not_fresh"] += 1
            continue
        same_series = {
            sec_index.rows[p][0]
            for p in positions
            if sec_index.rows[p][1] == series and sec_index.rows[p][0] is not None
        }
        if not same_series:
            excluded["sec_iu_ticker_other_series"] += 1
            continue
        if len(same_series) != 1:
            excluded["sec_iu_ticker_ambiguous_class"] += 1
            continue
        (class_id,) = same_series
        failure = sec_index.first_failure(ticker, series, class_id)
        if failure is not None:
            excluded[f"sec_judge_{failure.split('.', 1)[1]}"] += 1
            continue
        if not index.sole_owner(owner, "ticker", ticker):
            excluded["ticker_claimed_by_other_instrument"] += 1
            continue
        if index.owners.get(("series_ticker", (series, ticker)), set()) - {owner}:
            excluded["series_ticker_claimed_by_other_instrument"] += 1
            continue
        if class_owners.get(class_id, set()) - {owner}:
            excluded["class_claimed_by_other_instrument"] += 1
            continue
        (position,) = [p for p in positions if sec_index.rows[p][0] == class_id]
        evidence[owner] = sec_index.synced_at[position]
        batch.append(
            {
                "instrument_id": owner,
                "sec_series_id": registry["sec_series_id"],
                "ticker_before": registry["ticker"],
                "ticker_after": ticker,
                "sec_class_id_before": registry["sec_class_id"],
                "sec_class_id_after": class_id,
            }
        )
    # Two admitted rows can never share a ticker or class (sole ownership and
    # SEC uniqueness); keep the guard explicit rather than implicit.
    for key in ("ticker_after", "sec_class_id_after"):
        repeated = {k for k, n in Counter(row[key] for row in batch).items() if n > 1}
        if repeated:
            dropped = [row for row in batch if row[key] in repeated]
            excluded["batch_collision"] += len(dropped)
            batch = [row for row in batch if row[key] not in repeated]
    return batch, excluded, evidence


def _with_isin(instruments: list[dict], changes: list[dict]) -> list[dict]:
    nulled = {row["instrument_id"] for row in changes}
    return [
        {**row, "isin": None} if row["instrument_id"] in nulled else row
        for row in instruments
    ]


def _with_tickers(
    funds: list[dict], identity: list[dict], changes: list[dict]
) -> tuple[list[dict], list[dict]]:
    by_owner = {row["instrument_id"]: row for row in changes}
    identity = [
        {
            **row,
            "ticker": by_owner[row["instrument_id"]]["ticker_after"],
            "sec_class_id": by_owner[row["instrument_id"]]["sec_class_id_after"],
        }
        if row["instrument_id"] in by_owner
        else row
        for row in identity
    ]
    # funds_v projects NULLIF(btrim(instrument_identity.ticker), '').
    funds = [
        {**row, "ticker": by_owner[row["instrument_id"]]["ticker_after"]}
        if row["instrument_id"] in by_owner
        else row
        for row in funds
    ]
    return funds, identity


class _SecEvidence(generator._SecIndex):
    """The generator's SEC index plus each row's canonical ``synced_at`` text."""

    def __init__(self, rows: list[dict], observed_at: dt.datetime):
        super().__init__(rows, observed_at)
        self.synced_at = [row["synced_at"] for row in rows]


def plan_repairs(
    instruments: list[dict],
    funds: list[dict],
    identity: list[dict],
    sec: list[dict],
    observed_at: dt.datetime,
) -> RepairPlan:
    """The complete repair plan of one snapshot (pure; raw DB rows in)."""
    iu = generator.canonical_source_rows(instruments, "instruments")
    fv = generator.canonical_source_rows(funds, "funds")
    reg = generator.canonical_source_rows(identity, "identity")
    sec_index = _SecEvidence(generator.canonical_source_rows(sec, "sec"), observed_at)
    isin_changes, isin_excluded = _plan_isin(iu, reg)
    iu = _with_isin(iu, isin_changes)
    ticker_changes: list[dict] = []
    evidence: dict[str, str] = {}
    for passes in range(1, MAX_PASSES + 1):
        batch, ticker_excluded, found = _plan_ticker_pass(iu, fv, reg, sec_index)
        if not batch:
            break
        ticker_changes.extend(batch)
        evidence.update(found)
        fv, reg = _with_tickers(fv, reg, batch)
    else:
        raise RepairError("ticker_plan_not_converged")
    return RepairPlan(
        isin_changes=isin_changes,
        ticker_changes=ticker_changes,
        excluded={"isin": isin_excluded, "ticker": ticker_excluded},
        passes=passes - 1,
        instruments=iu,
        funds=fv,
        identity=reg,
        evidence=evidence,
    )


# ── pure classification and publication-gate preview ────────────────────────


def _first_failures_and_structural(
    instruments: list[dict],
    funds: list[dict],
    identity: list[dict],
    sec: list[dict],
    observed_at: dt.datetime,
) -> tuple[dict[str, str | None], set[str], set[str]]:
    """Per-fund first failure (generator precedence, SEC last), P and B.

    P is the generator's structural pre-claims daily set; B the auditor's own
    v1 baseline daily set (A4), computed by the auditor's code.
    """
    index = generator._CatalogIndex(
        generator.canonical_source_rows(instruments, "instruments"),
        generator.canonical_source_rows(funds, "funds"),
        generator.canonical_source_rows(identity, "identity"),
    )
    sec_index = generator._SecIndex(
        generator.canonical_source_rows(sec, "sec"), observed_at
    )
    first: dict[str, str | None] = {}
    structural = set()
    for owner in index.universe():
        if generator._is_inactive(index, owner):
            first[owner] = "inactive"
            continue
        first[owner] = generator._first_failure(index, owner) or (
            generator._sec_first_failure(index, sec_index, owner)
        )
        if generator._first_failure(index, owner, claims=False) is None and (
            _text(index.funds[owner][0]["fund_type"])
            in generator.SUPPORTED_DAILY_FUND_TYPES
        ):
            structural.add(owner)
    catalog = auditor.Catalog(
        {
            source: auditor._source_rows(rows, source, from_database=True)
            for source, rows in (
                ("instruments", instruments),
                ("funds", funds),
                ("identity", identity),
            )
        }
    )
    _total, baseline = catalog.v1_structural_sets()
    return first, structural, baseline


def classify(
    instruments: list[dict],
    funds: list[dict],
    identity: list[dict],
    sec: list[dict],
    observed_at: dt.datetime,
    cohort: set[str] | None,
) -> dict:
    """Generator classification of one catalog state plus the A4/A8 gate preview."""
    evidence, counts, digests = generator.classify_catalog(
        instruments, funds, identity, sec, observed_at
    )
    status = {row["instrument_id"]: row["fund_status"] for row in evidence}
    daily = {
        row["instrument_id"]
        for row in evidence
        if row["fund_status"] == "ACTIVE" and row["valuation_frequency"] == "daily"
    }
    first, structural, baseline = _first_failures_and_structural(
        instruments, funds, identity, sec, observed_at
    )
    if len(structural) != counts["structural_pre_claims_daily"] or Counter(
        code for code in first.values() if code not in (None, "inactive")
    ) != Counter(counts["identity_first_failure"]):
        raise RepairError("classification_preview_inconsistent")
    failures = counts["identity_first_failure"]
    sec_failures = {code: n for code, n in failures.items() if code.startswith("sec.")}
    reaching_sec = counts["active"] + sum(sec_failures.values())
    bound = (
        reaching_sec
        * SEC_EXCLUSION_FRACTION_NUMERATOR
        // SEC_EXCLUSION_FRACTION_DENOMINATOR
    )
    stale = sec_failures.get(SEC_STALE_CODE, 0)
    missing = sec_failures.get(SEC_MISSING_CODE, 0)
    integrity = sum(sec_failures.get(code, 0) for code in SEC_INTEGRITY_CODES)
    summary = {
        "active": counts["active"],
        "active_daily": counts["active_daily"],
        "fund_status": counts["fund_status"],
        "identity_first_failure": dict(
            sorted(failures.items(), key=lambda item: (-item[1], item[0]))
        ),
        "active_set_sha256": digests["active_set_sha256"],
        "cohort": None,
        "gates": {
            "a4": {
                "structural_daily": len(structural),
                "default_ceiling": DEFAULT_STRUCTURAL_DAILY_CEILING,
                "within_default_ceiling": len(structural)
                <= DEFAULT_STRUCTURAL_DAILY_CEILING,
                "baseline": len(baseline),
                "structural_daily_subset_of_baseline": structural <= baseline,
                "active_daily_subset_of_structural_daily": daily <= structural,
            },
            "a8": {
                "reaching_sec": reaching_sec,
                "bound": bound,
                "stale": stale,
                "missing": missing,
                "integrity": integrity,
                "stale_within_bound": stale <= bound,
                "missing_within_bound": missing <= bound,
                "integrity_zero": integrity == 0,
            },
        },
    }
    if cohort is not None:
        summary["cohort"] = {
            "relation": COHORT_RELATION,
            "size": len(cohort),
            "active": sum(1 for owner in cohort if status.get(owner) == "ACTIVE"),
        }
    return {"summary": summary, "status": status, "first": first}


def compare(before: dict, after: dict, plan: RepairPlan) -> dict:
    """ACTIVE gained/lost, and where every SEC integrity failure after the plan comes from.

    A ticker-repaired fund is admitted by the generator's SEC judge, so it can
    never be an integrity failure. A fund repaired by (a) only reaches the SEC
    stage once its series-id ISIN stops failing first; a contradiction found
    there pre-exists the repair (the registry ticker/class it judges are
    untouched) and is reported as ``unmasked_by_isin_repair``.
    """
    old, new = before["status"], after["status"]
    integrity = set(SEC_INTEGRITY_CODES)
    ticker_ids = {row["instrument_id"] for row in plan.ticker_changes}
    isin_ids = {row["instrument_id"] for row in plan.isin_changes}
    after_integrity = {o for o, code in after["first"].items() if code in integrity}
    before_integrity = {o for o, code in before["first"].items() if code in integrity}
    return {
        "gained_active": sum(
            1
            for owner, value in new.items()
            if value == "ACTIVE" and old.get(owner) != "ACTIVE"
        ),
        "lost_active": sum(
            1
            for owner, value in old.items()
            if value == "ACTIVE" and new.get(owner) != "ACTIVE"
        ),
        "sec_integrity_after": {
            "total": len(after_integrity),
            "pre_existing": len(after_integrity & before_integrity),
            "on_ticker_repaired_rows": len(after_integrity & ticker_ids),
            "unmasked_by_isin_repair": len(
                (after_integrity - before_integrity - ticker_ids) & isin_ids
            ),
            "other": len(after_integrity - before_integrity - ticker_ids - isin_ids),
        },
    }


def guard_violations(before: dict, after: dict, plan: RepairPlan) -> list[str]:
    """Reasons an apply must refuse; any one blocks the whole transaction."""
    reasons = []
    outcome = compare(before, after, plan)
    if outcome["lost_active"]:
        reasons.append("repair_would_demote_active")
    integrity = outcome["sec_integrity_after"]
    if integrity["on_ticker_repaired_rows"] or integrity["other"]:
        reasons.append("repair_would_create_sec_integrity_failure")
    return reasons


# ── database access ─────────────────────────────────────────────────────────


@dataclass
class Snapshot:
    decision_at: dt.datetime
    instruments: list[dict]
    funds: list[dict]
    identity: list[dict]
    sec: list[dict]
    cohort: set[str] | None
    etp_tickers: set[str] | None


def _relation_present(cursor, name: str) -> bool:
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL AS present", (name,))
    return cursor.fetchone()["present"] is True


def _read_catalog(cursor, decision_at: dt.datetime) -> Snapshot:
    generator._require_source_privileges(cursor)
    instruments, funds, identity, sec = generator._catalog_rows(cursor)
    cohort = None
    if _relation_present(cursor, COHORT_RELATION):
        cursor.execute(f"SELECT instrument_id FROM {COHORT_RELATION}")
        cohort = {str(row["instrument_id"]) for row in cursor.fetchall()}
    etp = None
    if _relation_present(cursor, "public.sec_cusip_ticker_map"):
        # The ETP set funds_v uses to type a fund from its registry ticker.
        cursor.execute(
            "SELECT DISTINCT upper(ticker) AS ticker FROM public.sec_cusip_ticker_map "
            "WHERE security_type = 'ETP' AND ticker IS NOT NULL"
        )
        etp = {row["ticker"] for row in cursor.fetchall()}
    return Snapshot(decision_at, instruments, funds, identity, sec, cohort, etp)


def _preflight(cursor) -> dict:
    cursor.execute(
        "SELECT has_table_privilege(current_user, 'public.instruments_universe', "
        "'UPDATE') AS iu_update, has_table_privilege(current_user, "
        "'public.instrument_identity', 'UPDATE') AS registry_update, "
        "has_schema_privilege(current_user, 'public', 'CREATE') AS schema_create, "
        # LOCK ... IN SHARE MODE needs UPDATE, DELETE or TRUNCATE on the relation.
        "has_table_privilege(current_user, 'public.sec_company_tickers_mf', "
        "'UPDATE,DELETE,TRUNCATE') AS sec_share_lock, "
        "to_regclass('public.fund_catalog_identity_repair_runs') IS NOT NULL "
        "AS ledger_present"
    )
    result = dict(cursor.fetchone())
    result["apply_runs"] = None
    if result["ledger_present"]:
        cursor.execute(
            "SELECT count(*) FILTER (WHERE kind = 'apply') AS applies, "
            "count(*) FILTER (WHERE kind = 'rollback') AS rollbacks "
            "FROM public.fund_catalog_identity_repair_runs"
        )
        result["apply_runs"] = dict(cursor.fetchone())
    return result


def read_snapshot(dsn: str) -> tuple[Snapshot, dict]:
    """The dry-run snapshot: READ ONLY, never assigns an xid (like the generator)."""
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            try:
                cursor.execute("SET LOCAL statement_timeout = '180s'")
                cursor.execute("SET LOCAL lock_timeout = '1s'")
                cursor.execute(
                    "SELECT clock_timestamp() AS decision_at, "
                    "current_setting('transaction_read_only') AS read_only, "
                    "txid_current_if_assigned() AS xid"
                )
                meta = cursor.fetchone()
                if meta["read_only"] != "on" or meta["xid"] is not None:
                    raise RepairError("snapshot_not_read_only")
                snapshot = _read_catalog(cursor, meta["decision_at"])
                preflight = _preflight(cursor)
                cursor.execute("SELECT txid_current_if_assigned() AS xid")
                if cursor.fetchone()["xid"] is not None:
                    raise RepairError("snapshot_assigned_xid")
                return snapshot, preflight
            finally:
                cursor.execute("ROLLBACK")


def _evaluate(snapshot: Snapshot) -> tuple[RepairPlan, dict, dict]:
    plan = plan_repairs(
        snapshot.instruments,
        snapshot.funds,
        snapshot.identity,
        snapshot.sec,
        snapshot.decision_at,
    )
    before = classify(
        snapshot.instruments,
        snapshot.funds,
        snapshot.identity,
        snapshot.sec,
        snapshot.decision_at,
        snapshot.cohort,
    )
    after = classify(
        plan.instruments,
        plan.funds,
        plan.identity,
        snapshot.sec,
        snapshot.decision_at,
        snapshot.cohort,
    )
    return plan, before, after


def _diagnostics(snapshot: Snapshot, plan: RepairPlan) -> dict:
    registry_series_like = sum(
        1
        for row in snapshot.identity
        if (value := _identifier(row["isin"])) is not None and _SERIES.fullmatch(value)
    )
    flips = None
    if snapshot.etp_tickers is not None:
        etp = snapshot.etp_tickers
        flips = sum(
            1
            for row in plan.ticker_changes
            if ((row["ticker_before"] or "").upper() in etp)
            != (row["ticker_after"] in etp)
        )
    return {
        "registry_isin_series_like": registry_series_like,
        "funds_v_etp_type_flips": flips,
    }


def _report(snapshot: Snapshot, plan: RepairPlan, before: dict, after: dict) -> dict:
    return {
        "decision_at": generator.utc_text(snapshot.decision_at),
        "repair_version": REPAIR_VERSION,
        "plan_sha256": plan.sha256(),
        "changes": plan.counts(),
        "excluded": {
            family: dict(sorted(counter.items()))
            for family, counter in plan.excluded.items()
        },
        "diagnostics": _diagnostics(snapshot, plan),
        "classification": {
            "before": before["summary"],
            "after": after["summary"],
            **compare(before, after, plan),
        },
        "guard_violations": guard_violations(before, after, plan),
    }


def write_plan_file(path: Path, snapshot: Snapshot, plan: RepairPlan) -> str:
    document = {
        "kind": PLAN_KIND,
        "decision_at": generator.utc_text(snapshot.decision_at),
        "plan_sha256": plan.sha256(),
        "changes": plan.changes(),
        "excluded": {
            family: dict(sorted(counter.items()))
            for family, counter in plan.excluded.items()
        },
    }
    content = generator.canonical_json(document)
    with open(path, "xb") as stream:  # never overwrite a reviewed plan
        stream.write(content)
    return hashlib.sha256(content).hexdigest()


def _try_writer_locks(cursor) -> bool:
    """INGESTION -> READINESS (the operator's order), then the SEC crosswalk writer."""
    for key in (
        LOCK_INSTRUMENT_INGESTION,
        LOCK_FUND_NAV_READINESS,
        LOCK_SEC_COMPANY_TICKERS_MF,
    ):
        cursor.execute("SELECT pg_try_advisory_xact_lock(%s) AS ok", (key,))
        if cursor.fetchone()["ok"] is not True:
            return False
    return True


def _begin_write(cursor) -> dt.datetime:
    """Table locks BEFORE the snapshot is taken, then the writer advisory locks.

    The SEC crosswalk is the evidence the plan is judged on: SHARE mode blocks
    every writer of it until COMMIT, so the evidence validated here is still
    the live crosswalk when the repaired rows commit.
    """
    cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
    cursor.execute("SET LOCAL statement_timeout = '300s'")
    cursor.execute("SET LOCAL lock_timeout = '10s'")
    cursor.execute("SET LOCAL search_path = public, pg_catalog")
    cursor.execute(
        "LOCK TABLE public.instruments_universe, public.instrument_identity "
        "IN SHARE ROW EXCLUSIVE MODE"
    )
    cursor.execute(f"LOCK TABLE {generator.SEC_RELATION} IN SHARE MODE")
    if not _try_writer_locks(cursor):
        raise LockBusy()
    cursor.execute("SELECT clock_timestamp() AS decision_at")
    return cursor.fetchone()["decision_at"]


_RECEIPT_ISIN = """
INSERT INTO fund_catalog_identity_repair_receipts
    (run_id, relation, instrument_id, rule, before_values, after_values)
SELECT %(run_id)s::uuid, 'instruments_universe', iu.instrument_id, %(rule)s::text,
       jsonb_build_object('isin', iu.isin, 'updated_at', iu.updated_at),
       jsonb_build_object('isin', NULL, 'updated_at', now())
  FROM unnest(%(ids)s::uuid[], %(isins)s::text[]) AS p(instrument_id, isin)
  JOIN public.instruments_universe iu ON iu.instrument_id = p.instrument_id
  JOIN public.instrument_identity ii ON ii.instrument_id = p.instrument_id
 WHERE iu.isin = p.isin
   AND upper(btrim(iu.isin)) ~ '^S[0-9]{9}$'
   AND upper(btrim(iu.isin)) = upper(btrim(ii.sec_series_id))
"""
_UPDATE_ISIN = """
UPDATE public.instruments_universe iu
   SET isin = NULL, updated_at = now()
  FROM fund_catalog_identity_repair_receipts r
 WHERE r.run_id = %(run_id)s::uuid AND r.relation = 'instruments_universe'
   AND iu.instrument_id = r.instrument_id
   AND iu.isin IS NOT DISTINCT FROM (r.before_values ->> 'isin')
"""
_RECEIPT_TICKER = """
INSERT INTO fund_catalog_identity_repair_receipts
    (run_id, relation, instrument_id, rule, before_values, after_values)
SELECT %(run_id)s::uuid, 'instrument_identity', ii.instrument_id, %(rule)s::text,
       jsonb_build_object(
           'ticker', ii.ticker, 'sec_class_id', ii.sec_class_id,
           'identity_sources', ii.identity_sources, 'updated_at', ii.updated_at),
       jsonb_build_object(
           'ticker', p.ticker_after, 'sec_class_id', p.class_after,
           'identity_sources', ii.identity_sources
               || jsonb_build_object('ticker', pv.provenance, 'sec_class_id', pv.provenance),
           'updated_at', now())
  FROM unnest(%(ids)s::uuid[], %(series)s::text[], %(tickers_before)s::text[],
              %(classes_before)s::text[], %(tickers_after)s::text[],
              %(classes_after)s::text[], %(synced_at)s::text[])
       AS p(instrument_id, series, ticker_before, class_before, ticker_after,
            class_after, synced_at)
  JOIN public.instrument_identity ii ON ii.instrument_id = p.instrument_id
 CROSS JOIN LATERAL (
       SELECT jsonb_build_object(
           'source', 'sec_company_tickers_mf', 'observed_at', p.synced_at,
           'basis', 'instruments_universe.ticker', 'repair', %(version)s::text,
           'repaired_at', now()) AS provenance) pv
 WHERE ii.ticker IS NOT DISTINCT FROM p.ticker_before
   AND ii.sec_class_id IS NOT DISTINCT FROM p.class_before
   AND ii.sec_series_id IS NOT DISTINCT FROM p.series
   AND ii.resolution_status::text = 'canonical'
   AND ii.conflict_state = '{}'::jsonb
"""
_UPDATE_TICKER = """
UPDATE public.instrument_identity ii
   SET ticker = r.after_values ->> 'ticker',
       sec_class_id = r.after_values ->> 'sec_class_id',
       identity_sources = r.after_values -> 'identity_sources',
       updated_at = now()
  FROM fund_catalog_identity_repair_receipts r
 WHERE r.run_id = %(run_id)s::uuid AND r.relation = 'instrument_identity'
   AND ii.instrument_id = r.instrument_id
   AND ii.ticker IS NOT DISTINCT FROM (r.before_values ->> 'ticker')
   AND ii.sec_class_id IS NOT DISTINCT FROM (r.before_values ->> 'sec_class_id')
   AND ii.identity_sources = r.before_values -> 'identity_sources'
"""
_INSERT_RUN = """
INSERT INTO fund_catalog_identity_repair_runs
    (run_id, kind, repair_version, plan_sha256, rolls_back_run_id, decision_at, counts)
VALUES (%(run_id)s, %(kind)s, %(version)s, %(plan_sha256)s, %(rolls_back)s,
        %(decision_at)s, %(counts)s::jsonb)
"""


def _expect(cursor, statement: str, params: dict, expected: int, code: str) -> None:
    cursor.execute(statement, params)
    if cursor.rowcount != expected:
        raise RepairError(code)


_LEDGER = (
    "public.fund_catalog_identity_repair_runs",
    "public.fund_catalog_identity_repair_receipts",
)


def _ensure_ledger(cursor) -> None:
    """Install the append-only ledger once; never re-run DDL over an existing one."""
    if not all(_relation_present(cursor, name) for name in _LEDGER):
        cursor.execute(SCHEMA_SQL.read_text(encoding="utf-8"))


def _write_plan(
    cursor, run_id: str, plan: RepairPlan, decision_at: dt.datetime
) -> None:
    _ensure_ledger(cursor)
    cursor.execute(
        _INSERT_RUN,
        {
            "run_id": run_id,
            "kind": "apply",
            "version": REPAIR_VERSION,
            "plan_sha256": plan.sha256(),
            "rolls_back": None,
            "decision_at": decision_at,
            "counts": json.dumps(plan.counts(), sort_keys=True),
        },
    )
    isin = sorted(plan.isin_changes, key=lambda row: row["instrument_id"])
    params = {
        "run_id": run_id,
        "rule": RULE_ISIN,
        "ids": [row["instrument_id"] for row in isin],
        "isins": [row["isin_before"] for row in isin],
    }
    _expect(cursor, _RECEIPT_ISIN, params, len(isin), "isin_receipt_mismatch")
    _expect(cursor, _UPDATE_ISIN, params, len(isin), "isin_update_mismatch")
    tickers = sorted(plan.ticker_changes, key=lambda row: row["instrument_id"])
    params = {
        "run_id": run_id,
        "rule": RULE_TICKER,
        "version": REPAIR_VERSION,
        "ids": [row["instrument_id"] for row in tickers],
        "series": [row["sec_series_id"] for row in tickers],
        "tickers_before": [row["ticker_before"] for row in tickers],
        "classes_before": [row["sec_class_id_before"] for row in tickers],
        "tickers_after": [row["ticker_after"] for row in tickers],
        "classes_after": [row["sec_class_id_after"] for row in tickers],
        "synced_at": [plan.evidence[row["instrument_id"]] for row in tickers],
    }
    _expect(cursor, _RECEIPT_TICKER, params, len(tickers), "ticker_receipt_mismatch")
    _expect(cursor, _UPDATE_TICKER, params, len(tickers), "ticker_update_mismatch")


def apply(dsn: str, plan_sha256: str) -> dict:
    """Recompute, verify and write the plan in ONE transaction."""
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            decision_at = _begin_write(cursor)
            committed = False
            try:
                snapshot = _read_catalog(cursor, decision_at)
                plan, before, after = _evaluate(snapshot)
                report = _report(snapshot, plan, before, after)
                if plan.empty():
                    return {"status": "noop", **report}
                if plan.sha256() != plan_sha256:
                    raise RepairError("plan_sha256_mismatch")
                violations = guard_violations(before, after, plan)
                if violations:
                    raise RepairError(violations[0])
                run_id = str(uuid.uuid4())
                _write_plan(cursor, run_id, plan, decision_at)
                # Same transaction: the catalog now reads the repaired rows.
                post = _read_catalog(cursor, decision_at)
                replan = plan_repairs(
                    post.instruments, post.funds, post.identity, post.sec, decision_at
                )
                if not replan.empty():
                    raise RepairError("post_apply_replan_not_empty")
                verified = classify(
                    post.instruments,
                    post.funds,
                    post.identity,
                    post.sec,
                    decision_at,
                    post.cohort,
                )
                if verified["status"] != after["status"]:
                    raise RepairError("post_apply_classification_mismatch")
                try:
                    cursor.execute("COMMIT")
                    committed = True
                except psycopg.OperationalError:
                    committed = True  # Never retried: the outcome is unknown.
                    return {"status": "commit_unknown", "run_id": run_id, **report}
                return {"status": "applied", "run_id": run_id, **report}
            finally:
                if not committed:
                    cursor.execute("ROLLBACK")


_ROLLBACK_RECEIPT_ISIN = """
INSERT INTO fund_catalog_identity_repair_receipts
    (run_id, relation, instrument_id, rule, before_values, after_values)
SELECT %(new_run)s::uuid, 'instruments_universe', iu.instrument_id, %(rule)s::text,
       jsonb_build_object('isin', iu.isin, 'updated_at', iu.updated_at), r.before_values
  FROM fund_catalog_identity_repair_receipts r
  JOIN public.instruments_universe iu ON iu.instrument_id = r.instrument_id
 WHERE r.run_id = %(run_id)s::uuid AND r.relation = 'instruments_universe'
   AND iu.isin IS NOT DISTINCT FROM (r.after_values ->> 'isin')
   AND iu.updated_at IS NOT DISTINCT FROM
       (r.after_values ->> 'updated_at')::timestamptz
"""
_ROLLBACK_ISIN = """
UPDATE public.instruments_universe iu
   SET isin = r.before_values ->> 'isin',
       updated_at = (r.before_values ->> 'updated_at')::timestamptz
  FROM fund_catalog_identity_repair_receipts r
 WHERE r.run_id = %(run_id)s::uuid AND r.relation = 'instruments_universe'
   AND iu.instrument_id = r.instrument_id
   AND iu.isin IS NOT DISTINCT FROM (r.after_values ->> 'isin')
   AND iu.updated_at IS NOT DISTINCT FROM
       (r.after_values ->> 'updated_at')::timestamptz
"""
_ROLLBACK_RECEIPT_TICKER = """
INSERT INTO fund_catalog_identity_repair_receipts
    (run_id, relation, instrument_id, rule, before_values, after_values)
SELECT %(new_run)s::uuid, 'instrument_identity', ii.instrument_id, %(rule)s::text,
       jsonb_build_object(
           'ticker', ii.ticker, 'sec_class_id', ii.sec_class_id,
           'identity_sources', ii.identity_sources, 'updated_at', ii.updated_at),
       r.before_values
  FROM fund_catalog_identity_repair_receipts r
  JOIN public.instrument_identity ii ON ii.instrument_id = r.instrument_id
 WHERE r.run_id = %(run_id)s::uuid AND r.relation = 'instrument_identity'
   AND ii.ticker IS NOT DISTINCT FROM (r.after_values ->> 'ticker')
   AND ii.sec_class_id IS NOT DISTINCT FROM (r.after_values ->> 'sec_class_id')
   AND ii.identity_sources = r.after_values -> 'identity_sources'
   AND ii.updated_at IS NOT DISTINCT FROM
       (r.after_values ->> 'updated_at')::timestamptz
"""
_ROLLBACK_TICKER = """
UPDATE public.instrument_identity ii
   SET ticker = r.before_values ->> 'ticker',
       sec_class_id = r.before_values ->> 'sec_class_id',
       identity_sources = r.before_values -> 'identity_sources',
       updated_at = (r.before_values ->> 'updated_at')::timestamptz
  FROM fund_catalog_identity_repair_receipts r
 WHERE r.run_id = %(run_id)s::uuid AND r.relation = 'instrument_identity'
   AND ii.instrument_id = r.instrument_id
   AND ii.ticker IS NOT DISTINCT FROM (r.after_values ->> 'ticker')
   AND ii.sec_class_id IS NOT DISTINCT FROM (r.after_values ->> 'sec_class_id')
   AND ii.identity_sources = r.after_values -> 'identity_sources'
   AND ii.updated_at IS NOT DISTINCT FROM
       (r.after_values ->> 'updated_at')::timestamptz
"""


def rollback(dsn: str, run_id: str) -> dict:
    """Restore every before-value of one apply run, all or nothing."""
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            decision_at = _begin_write(cursor)
            committed = False
            try:
                if not all(_relation_present(cursor, name) for name in _LEDGER):
                    raise RepairError("receipt_ledger_missing")
                cursor.execute(
                    "SELECT plan_sha256 FROM fund_catalog_identity_repair_runs "
                    "WHERE run_id = %s AND kind = 'apply'",
                    (run_id,),
                )
                run = cursor.fetchone()
                if run is None:
                    raise RepairError("apply_run_not_found")
                cursor.execute(
                    "SELECT run_id FROM fund_catalog_identity_repair_runs "
                    "WHERE rolls_back_run_id = %s",
                    (run_id,),
                )
                previous = cursor.fetchone()
                if previous is not None:
                    return {
                        "status": "noop",
                        "rollback_run_id": str(previous["run_id"]),
                    }
                cursor.execute(
                    "SELECT relation, count(*) AS n FROM fund_catalog_identity_repair_receipts "
                    "WHERE run_id = %s GROUP BY relation",
                    (run_id,),
                )
                sizes = {row["relation"]: row["n"] for row in cursor.fetchall()}
                n_iu = sizes.get("instruments_universe", 0)
                n_reg = sizes.get("instrument_identity", 0)
                new_run = str(uuid.uuid4())
                counts = {
                    "instruments_universe_isin_restored": n_iu,
                    "instrument_identity_ticker_and_class_restored": n_reg,
                }
                cursor.execute(
                    _INSERT_RUN,
                    {
                        "run_id": new_run,
                        "kind": "rollback",
                        "version": REPAIR_VERSION,
                        "plan_sha256": run["plan_sha256"],
                        "rolls_back": run_id,
                        "decision_at": decision_at,
                        "counts": json.dumps(counts, sort_keys=True),
                    },
                )
                params = {"run_id": run_id, "new_run": new_run, "rule": RULE_ROLLBACK}
                _expect(
                    cursor, _ROLLBACK_RECEIPT_ISIN, params, n_iu, "rollback_conflict"
                )
                _expect(cursor, _ROLLBACK_ISIN, params, n_iu, "rollback_conflict")
                _expect(
                    cursor, _ROLLBACK_RECEIPT_TICKER, params, n_reg, "rollback_conflict"
                )
                _expect(cursor, _ROLLBACK_TICKER, params, n_reg, "rollback_conflict")
                try:
                    cursor.execute("COMMIT")
                    committed = True
                except psycopg.OperationalError:
                    committed = True
                    return {
                        "status": "commit_unknown",
                        "rollback_run_id": new_run,
                        **counts,
                    }
                return {"status": "rolled_back", "rollback_run_id": new_run, **counts}
            finally:
                if not committed:
                    cursor.execute("ROLLBACK")


# ── CLI ─────────────────────────────────────────────────────────────────────


def _emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dsn-env", default=DEFAULT_DSN_ENV)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--rollback", metavar="RUN_ID")
    parser.add_argument("--confirm")
    parser.add_argument("--plan-sha256")
    parser.add_argument("--plan-output", type=Path)
    return parser


def _dsn(name: str) -> str:
    if not generator.ENV_NAME.fullmatch(name) or not os.environ.get(name):
        raise RepairError("dsn_environment_missing")
    return os.environ[name]


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    mode = "apply" if args.apply else "rollback" if args.rollback else "dry_run"
    try:
        if mode != "dry_run":
            if args.confirm != CONFIRM_TOKEN:
                raise RepairError("confirmation_required")
            if args.plan_output is not None:
                raise RepairError("plan_output_is_dry_run_only")
        if mode == "apply" and not (
            isinstance(args.plan_sha256, str) and _HEX64.fullmatch(args.plan_sha256)
        ):
            raise RepairError("plan_sha256_required")
        if mode != "apply" and args.plan_sha256 is not None:
            raise RepairError("plan_sha256_is_apply_only")
        if mode == "rollback":
            try:
                run_id = str(uuid.UUID(args.rollback))
            except ValueError as exc:
                raise RepairError("run_id_invalid") from exc
        if args.plan_output is not None and args.plan_output.exists():
            raise RepairError("plan_output_exists")
        dsn = _dsn(args.dsn_env)
        if mode == "dry_run":
            snapshot, preflight = read_snapshot(dsn)
            plan, before, after = _evaluate(snapshot)
            result = {"status": "planned", **_report(snapshot, plan, before, after)}
            result["preflight"] = preflight
            if args.plan_output is not None:
                result["plan_file_sha256"] = write_plan_file(
                    args.plan_output, snapshot, plan
                )
        elif mode == "apply":
            result = apply(dsn, args.plan_sha256)
        else:
            result = rollback(dsn, run_id)
        _emit({"mode": mode, **result})
        # A lost COMMIT acknowledgement is never reported as success: re-run the
        # dry run (an empty plan means the apply landed) or read the ledger.
        return EXIT_FAILED if result.get("status") == "commit_unknown" else EXIT_OK
    except LockBusy as exc:
        _emit({"mode": mode, "status": "lock_busy", "code": exc.code})
        return EXIT_LOCK_BUSY
    except RepairError as exc:
        _emit({"mode": mode, "status": "blocked", "code": exc.code})
        return exc.exit_code
    except generator.PolicyGenerationError as exc:
        code = str(exc)
        exit_code = (
            EXIT_INCOMPATIBLE
            if "privilege" in code or "missing" in code
            else EXIT_FAILED
        )
        _emit({"mode": mode, "status": "blocked", "code": code})
        return exit_code
    except psycopg.errors.LockNotAvailable as exc:
        _emit({"mode": mode, "status": "lock_busy", "sqlstate": exc.sqlstate})
        return EXIT_LOCK_BUSY
    except (psycopg.errors.InsufficientPrivilege, psycopg.errors.UndefinedTable) as exc:
        _emit({"mode": mode, "status": "blocked", "sqlstate": exc.sqlstate})
        return EXIT_INCOMPATIBLE
    except psycopg.Error as exc:
        _emit({"mode": mode, "status": "blocked", "sqlstate": exc.sqlstate})
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
