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
  3. materialize -> ncen_derived_profiles + rr1_derived_profiles + bond_metrics
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
import os
import subprocess
import uuid
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

# Build stamps a deploy may inject (checked before shelling out to git, which is
# often unavailable in a container). See the runbook's ACTIVATION NOTE.
_REVISION_ENV_VARS = ("CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA")


def _code_revision() -> str:
    for var in _REVISION_ENV_VARS:
        value = os.getenv(var)
        if value:
            return value.strip()
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        stamped = out.stdout.strip()
        if stamped:
            return stamped
    except Exception:
        pass
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
    # bond_metrics runs AFTER the pit_update stage published the security/price
    # snapshots it consumes; it self-promotes bond_metric_v1 atomically, so the
    # chain's table-driven pointer-diff compensation covers it automatically.
    return _compose([
        _invoke(ctx, "ncen_derived_profiles"),
        _invoke(ctx, "rr1_derived_profiles"),
        _invoke(ctx, "bond_metrics"),
    ])


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


# The chain's source products and the column carrying each one's observation/effective
# date. The watermark per source is max(date); the same set drives eligible source-days.
_WATERMARK_SOURCES: tuple[tuple[str, str], ...] = (
    ("bond_security_observation", "as_of"),
    ("bond_price_observation", "as_of"),
    ("ncen_effective_filings", "effective_date"),
    ("rr1_effective_facts", "effective_date"),
)

# Staleness threshold (days) for the freshness alert; operators override it with
# DAILY_CHAIN_STALENESS_THRESHOLD_DAYS. A source whose latest input lags the processed
# source-day by at least this many days raises a staleness alert distinct from failure.
_DEFAULT_STALENESS_THRESHOLD_DAYS = 3


def _staleness_threshold() -> int | None:
    raw = os.getenv("DAILY_CHAIN_STALENESS_THRESHOLD_DAYS")
    if raw is None or raw.strip() == "":
        return _DEFAULT_STALENESS_THRESHOLD_DAYS
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_STALENESS_THRESHOLD_DAYS
    return value if value >= 0 else None  # a negative value disables the alert


def discover_source_days(conn: Any, *, limit: int | None = None) -> list[date]:
    """Watermark-driven eligible source-days (ascending). Absent tables -> []."""
    days: set[date] = set()
    for table, col in _WATERMARK_SOURCES:
        if not conn.execute("SELECT to_regclass(%s) IS NOT NULL", (table,)).fetchone()[0]:
            continue
        for (day,) in conn.execute(f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL"):
            days.add(day)
    ordered = sorted(days)
    return ordered[:limit] if limit is not None else ordered


def build_watermarks(conn: Any, source_day: date) -> dict[str, Any]:
    """Per-source input watermarks: the latest observed/effective date per source product.

    Recorded on the run row (``input_watermarks``) and turned into the run summary's
    freshness metric + staleness alert. Absent source tables are skipped and a source
    with no rows contributes ``None`` (honest absence, never a fabricated date), so a
    fully dark run records ``{}`` / all-``None`` and raises no false staleness.
    """
    watermarks: dict[str, Any] = {}
    for table, col in _WATERMARK_SOURCES:
        if not conn.execute("SELECT to_regclass(%s) IS NOT NULL", (table,)).fetchone()[0]:
            continue
        row = conn.execute(f"SELECT max({col}) FROM {table}").fetchone()
        wm = row[0] if row else None
        watermarks[table] = wm.isoformat() if isinstance(wm, date) else None
    return watermarks


def _snapshot_pointers(ctx: StageContext) -> dict[str, uuid.UUID]:
    """Current derived + mixed_quant pointers keyed by product (read on the dsn).

    The chain diffs this before/after each stage to enumerate what a worker's
    atomic self-promotion made current, and to know the compensation targets.
    """
    snapshot: dict[str, uuid.UUID] = {}
    with connect(ctx.dsn) as conn:
        if conn.execute("SELECT to_regclass('sec_derived_current_pointers') IS NOT NULL").fetchone()[0]:
            for product, pub_id in conn.execute(
                "SELECT product, publication_id FROM sec_derived_current_pointers"
            ):
                snapshot[product] = pub_id
        if conn.execute("SELECT to_regclass('active_quant_publication_v1') IS NOT NULL").fetchone()[0]:
            for product, pub_id in conn.execute(
                "SELECT product, publication_id FROM active_quant_publication_v1"
            ):
                snapshot[product] = pub_id
        conn.commit()
    return snapshot


def _restore_pointer(ctx: StageContext, product: str, prior: uuid.UUID | None) -> bool:
    """Restore ``product``'s current pointer to ``prior`` (or clear it if None).

    Routes to the correct pointer mechanism by which table owns the product:
    derived products via the validated-only ``sec_set_current_derived_publication``;
    the mixed_quant active pointer via ``promote_quant_publication``. Raises on any
    failure so the engine records a compensation FAILURE (distinct alert), never a
    silent partial restore.
    """
    with connect(ctx.dsn) as conn:
        mixed = False
        if conn.execute("SELECT to_regclass('quant_publication_v1') IS NOT NULL").fetchone()[0]:
            mixed = bool(conn.execute(
                "SELECT EXISTS (SELECT 1 FROM quant_publication_v1 WHERE product=%s)", (product,)
            ).fetchone()[0])
        if mixed:
            _restore_mixed(conn, product, prior)
        else:
            _restore_derived(conn, product, prior)
        conn.commit()
    return True


def _restore_derived(conn: Any, product: str, prior: uuid.UUID | None) -> None:
    if prior is not None:
        # Compensation restores the PRE-RUN pointer, which is by construction older
        # than what this run made current: an explicit, deliberate regression. Opting
        # out of the monotonic-promotion guard here keeps compensation from turning a
        # stage failure into a compensation FAILURE; normal promotion keeps the guard.
        conn.execute(
            "SELECT sec_set_current_derived_publication(%s,%s,allow_as_of_regression => true)",
            (product, prior),
        )
        return
    # First publication of this product this run: clear the pointer through the
    # protocol's own sanctioned token path (the guard permits the mutation while
    # the product's backend_pid token is present).
    conn.execute(
        "INSERT INTO sec_derived_pointer_tokens(product, backend_pid) VALUES (%s, pg_backend_pid()) "
        "ON CONFLICT (product) DO UPDATE SET backend_pid=EXCLUDED.backend_pid", (product,))
    conn.execute("DELETE FROM sec_derived_current_pointers WHERE product=%s", (product,))
    conn.execute(
        "DELETE FROM sec_derived_pointer_tokens WHERE product=%s AND backend_pid=pg_backend_pid()",
        (product,))


def _restore_mixed(conn: Any, product: str, prior: uuid.UUID | None) -> None:
    from src.quant_data import publication as pub

    if prior is not None:
        # Re-activate the prior publication (it was retired to 'superseded' on the
        # promotion we are undoing). Clear activated_at when moving it back to 'ready'
        # so the publication lifecycle CHECK ((status='active') = (activated_at IS NOT
        # NULL) OR status='superseded') holds, then re-promote it atomically.
        conn.execute(
            "UPDATE quant_publication_v1 SET status='ready', activated_at=NULL "
            "WHERE publication_id=%s AND status <> 'active'", (prior,))
        # Deliberate compensation to the pre-run pointer (see _restore_derived).
        pub.promote(conn, product, prior, allow_as_of_regression=True)
        return
    # No prior active pointer: drop the active row and make the current publication
    # writable again (back to 'ready'); activated_at is cleared for the same CHECK.
    conn.execute(
        "UPDATE quant_publication_v1 SET status='ready', activated_at=NULL "
        "WHERE product=%s AND status='active'", (product,))
    conn.execute("DELETE FROM active_quant_publication_v1 WHERE product=%s", (product,))


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
            watermarks_for=build_watermarks,
            snapshot_pointers=_snapshot_pointers, restore_pointer=_restore_pointer,
            staleness_threshold_days=_staleness_threshold(),
        )
    failed = any(s["status"] == "failed" for s in summaries)
    stale = [s["staleness_alert"] for s in summaries if s.get("staleness_alert")]
    return {
        "state": "failed" if failed else "ok",
        "chain": CHAIN,
        "runs_processed": len(summaries),
        # Freshness is surfaced separately from failure: a run can be 'ok' yet carry
        # a staleness alert operators should see.
        "staleness_alerts": stale,
        "runs": summaries,
    }
