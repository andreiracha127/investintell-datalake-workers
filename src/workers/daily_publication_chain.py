"""Daily publication chain worker (Increment 2, Task 6; frozen spec §5).

Thin ORCHESTRATION entry point: it maps the eight frozen stages to the existing,
review-approved workers and drives them with the ``src.bonds.daily_chain`` engine
under a single chain-run advisory lock. It does NOT reimplement any publication
logic — every stage invokes an existing ``worker.run(dsn, ...)`` (each of which
still opens its own connection and takes its own per-worker lock) and classifies
the result. Nothing here creates a schedule or deploys anything (SEM deploy).

Stage -> existing worker mapping (spec §5):
  1. ingest      -> ncen_ingestion / rr1_ingestion / nport_ingestion (raw landing)
  2. pit_update  -> bond_security_master + bond_price_observations
  3. materialize -> ncen_derived_profiles + rr1_derived_profiles
  4. mixed_build -> mixed_quant_publication (builds inactive; promote is separate)
  5. validate    -> read-only reconciliation of the current derived pointers
  6. promote     -> promote the ready mixed_quant_v1 publication (the derived and
                    bond products self-promote atomically inside their stages)
  7. refresh     -> sec_regulatory_serving + bond_serving (serving projections)
  8. probe       -> read-only smoke over the current pointers

Global Constraint 9 / handoff: no production source is authorized yet, so in any
current environment every stage legitimately reports ``dark_no_source`` and the
run completes in DARK mode with nothing promoted (reported, never a silent
success). Full promotion/refresh is exercised once sources are authorized at the
future, explicit activation.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
"""
from __future__ import annotations

import importlib
import subprocess
from datetime import date
from typing import Any, Callable

from src.bonds import daily_chain
from src.bonds.daily_chain import (
    Stage,
    StageContext,
    StageOutcome,
    classify_worker_result,
)
from src.db import LOCK_DAILY_PUBLICATION_CHAIN, advisory_lock, connect, resolve_dsn

CHAIN = daily_chain.DEFAULT_CHAIN


def _code_revision() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _worker(name: str) -> Callable[..., dict[str, Any]]:
    """Lazily resolve ``src.workers.<name>.run`` (never import eagerly)."""
    return importlib.import_module(f"src.workers.{name}").run


def _compose(outcomes: list[StageOutcome], *, empty_reason: str = "dark_no_source") -> StageOutcome:
    """Fold several sub-worker outcomes into one stage outcome.

    Any succeeded -> the stage succeeded (aggregate detail). All skipped -> a
    reported skip. (A terminal sub-worker failure raises before we get here and
    is classified by the engine.)
    """
    if not outcomes:
        return StageOutcome.skipped(empty_reason)
    detail: dict[str, Any] = {}
    watermarks: dict[str, Any] = {}
    any_ok = False
    for i, oc in enumerate(outcomes):
        detail[f"unit_{i}"] = {"status": oc.status.value, "reason": oc.reason, **dict(oc.detail)}
        if oc.watermarks:
            watermarks.update(dict(oc.watermarks))
        if oc.status.value == "succeeded":
            any_ok = True
    if any_ok:
        return StageOutcome.succeeded(units=detail, watermarks=watermarks or None)
    # All dark: surface the reported skip.
    return StageOutcome.skipped("dark_no_source", units=detail)


def _invoke(ctx: StageContext, worker_name: str) -> StageOutcome:
    result = _worker(worker_name)(ctx.dsn, calc_date=ctx.source_day.isoformat())
    return classify_worker_result(dict(result))


# --------------------------------------------------------------------------- #
# Stage units (spec §5). Each invokes existing workers only.
# --------------------------------------------------------------------------- #

def stage_ingest(ctx: StageContext) -> StageOutcome:
    outcomes: list[StageOutcome] = []
    for name in ("ncen_ingestion", "rr1_ingestion", "nport_ingestion"):
        try:
            outcomes.append(_invoke(ctx, name))
        except FileNotFoundError:
            # A missing local SOURCE_ROOT is "no authorized source", not a crash:
            # a REPORTED dark skip, reconciled with the required-stage rule.
            outcomes.append(StageOutcome.skipped("dark_no_source", unit=name, reason_detail="source_root_absent"))
    return _compose(outcomes)


def stage_pit_update(ctx: StageContext) -> StageOutcome:
    return _compose([_invoke(ctx, "bond_security_master"), _invoke(ctx, "bond_price_observations")])


def stage_materialize(ctx: StageContext) -> StageOutcome:
    return _compose([_invoke(ctx, "ncen_derived_profiles"), _invoke(ctx, "rr1_derived_profiles")])


def stage_mixed_build(ctx: StageContext) -> StageOutcome:
    return _invoke(ctx, "mixed_quant_publication")


def stage_validate(ctx: StageContext) -> StageOutcome:
    """Read-only reconciliation: confirm current derived pointers are validated.

    The materializer stages self-validate through the derived-publication
    protocol before setting their pointers, so this stage never re-validates raw
    rows; it confirms nothing was left half-built. With no pointers set (fully
    dark) it reports ``dark_no_source``.
    """
    conn = ctx.conn
    if not conn.execute("SELECT to_regclass('sec_derived_current_pointers') IS NOT NULL").fetchone()[0]:
        return StageOutcome.skipped("dark_no_source", detail="no_derived_protocol")
    rows = conn.execute(
        "SELECT c.product, c.publication_id, "
        "       sec_derived_publication_is_validated(c.publication_id, c.product) "
        "FROM sec_derived_current_pointers c"
    ).fetchall()
    if not rows:
        return StageOutcome.skipped("dark_no_source", pointers=0)
    invalid = [str(r[0]) for r in rows if not r[2]]
    if invalid:
        # A current pointer that is not validated is a hard integrity breach.
        return StageOutcome.failed("current_pointer_not_validated", classification="terminal",
                                   products=invalid)
    return StageOutcome.succeeded(validated_pointers=len(rows))


def stage_promote(ctx: StageContext) -> StageOutcome:
    """Promote the ready mixed_quant_v1 publication for this source-day.

    The derived/bond products self-promote atomically inside their own stages;
    mixed_quant deliberately separates build from promotion, so the chain
    performs that one atomic promotion here. No ready publication -> dark.
    """
    from src.quant_data import publication as pub

    with connect(ctx.dsn) as conn:
        if not conn.execute("SELECT to_regclass('quant_publication_v1') IS NOT NULL").fetchone()[0]:
            conn.commit()
            return StageOutcome.skipped("dark_no_source", detail="no_mixed_schema")
        row = conn.execute(
            "SELECT publication_id FROM quant_publication_v1 "
            "WHERE product=%s AND as_of=%s AND status IN ('ready','active') "
            "ORDER BY status='active' DESC LIMIT 1",
            (pub.PRODUCT, ctx.source_day),
        ).fetchone()
        if row is None:
            conn.commit()
            return StageOutcome.skipped("dark_no_source", product=pub.PRODUCT)
        pub.promote(conn, pub.PRODUCT, row[0])
        conn.commit()
    return StageOutcome.succeeded(product=pub.PRODUCT, publication_id=str(row[0]))


def stage_refresh(ctx: StageContext) -> StageOutcome:
    return _compose([_invoke(ctx, "sec_regulatory_serving"), _invoke(ctx, "bond_serving")])


def stage_probe(ctx: StageContext) -> StageOutcome:
    """Read-only smoke over the current pointers (never mutates)."""
    conn = ctx.conn
    counts: dict[str, Any] = {}
    if conn.execute("SELECT to_regclass('sec_derived_current_pointers') IS NOT NULL").fetchone()[0]:
        counts["derived_pointers"] = conn.execute(
            "SELECT count(*) FROM sec_derived_current_pointers"
        ).fetchone()[0]
    with connect(ctx.dsn) as probe_conn:
        if probe_conn.execute("SELECT to_regclass('active_quant_publication_v1') IS NOT NULL").fetchone()[0]:
            counts["active_quant_publications"] = probe_conn.execute(
                "SELECT count(*) FROM active_quant_publication_v1"
            ).fetchone()[0]
        probe_conn.commit()
    return StageOutcome.succeeded(pointers=counts)


STAGE_BUILDERS: dict[str, Callable[[StageContext], StageOutcome]] = {
    "ingest": stage_ingest,
    "pit_update": stage_pit_update,
    "materialize": stage_materialize,
    "mixed_build": stage_mixed_build,
    "validate": stage_validate,
    "promote": stage_promote,
    "refresh": stage_refresh,
    "probe": stage_probe,
}


def build_default_stages() -> list[Stage]:
    """The eight frozen stages, in binding order, wired to existing workers."""
    return [Stage(name, STAGE_BUILDERS[name]) for name in daily_chain.STAGE_ORDER]


def build_stages(names: list[str]) -> list[Stage]:
    """A subset of the default stages, preserving frozen order (for focused smoke)."""
    ordered = [n for n in daily_chain.STAGE_ORDER if n in set(names)]
    return [Stage(name, STAGE_BUILDERS[name]) for name in ordered]


def discover_source_days(conn: Any, *, limit: int | None = None) -> list[date]:
    """Watermark-driven eligible source-days (ascending). Absent tables -> []."""
    days: set[date] = set()
    for table, col in (
        ("bond_security_observation", "as_of"),
        ("bond_price_observation", "as_of"),
        ("ncen_effective_filings", "effective_date"),
        ("rr1_effective_facts", "effective_date"),
    ):
        if not conn.execute("SELECT to_regclass(%s) IS NOT NULL", (table,)).fetchone()[0]:
            continue
        for (day,) in conn.execute(f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL"):
            days.add(day)
    ordered = sorted(days)
    return ordered[:limit] if limit is not None else ordered


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    """Drive the daily publication chain under the chain-run advisory lock.

    Overlapping runs are impossible: the whole run holds
    ``LOCK_DAILY_PUBLICATION_CHAIN`` (session-level, survives the per-stage
    commits). ``calc_date`` pins a single source-day; otherwise all eligible
    watermark days are processed in ascending (catch-up) order.
    """
    resolved = resolve_dsn(dsn)
    with connect(resolved) as conn, advisory_lock(conn, LOCK_DAILY_PUBLICATION_CHAIN) as acquired:
        if not acquired:
            return {"state": "locked", "chain": CHAIN, "runs": []}
        daily_chain.install_schema(conn)
        conn.commit()
        if calc_date:
            source_days = [date.fromisoformat(calc_date)]
        else:
            source_days = discover_source_days(conn, limit=limit)
        if not source_days:
            return {"state": "no_source_days", "chain": CHAIN, "runs": []}
        summaries = daily_chain.run_chain(
            conn, stages=build_default_stages(), source_days=source_days,
            code_revision=_code_revision(), config_version="v1", dsn=resolved,
        )
    failed = any(s["status"] == "failed" for s in summaries)
    return {
        "state": "failed" if failed else "ok",
        "chain": CHAIN,
        "runs_processed": len(summaries),
        "runs": summaries,
    }
