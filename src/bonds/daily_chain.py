"""Daily publication chain orchestrator engine (Increment 2, Task 6; spec §5).

This module is the reusable *engine* behind the daily publication chain. It does
NOT reimplement any publication logic: it ORCHESTRATES the existing,
review-approved workers (ingest / pit_update / materialize / mixed_build /
validate / promote / refresh / probe) as opaque stage units and records the
bookkeeping the spec requires:

* a single chain-run advisory lock (``LOCK_DAILY_PUBLICATION_CHAIN`` in db.py)
  held for the whole run so overlapping runs cannot interleave stages;
* a deterministic run identity ``uuid5(chain, source_day, code_revision,
  config_version)`` (``chain_run_id_for``, mirroring ``publication_id_for``);
* per-source input watermarks;
* per-stage checkpoints so a replay is idempotent and a restart resumes
  mid-chain (a stage already at a terminal per-stage status is never re-run);
* bounded retries with backoff for *transient* failures; typed *terminal*
  failures fail closed immediately;
* deterministic catch-up: missed source-days are processed in ascending order,
  same mechanism, no special mode;
* chain-level "a partial run never stays current" delivered by COMPENSATION: each
  product self-promotes atomically per-publication inside its own stage (the
  Inc.1-approved architecture; "partial never becomes current" holds
  per-publication). If a later stage fails terminally, the chain rolls back EVERY
  product it advanced during THIS run to its prior current pointer, so the stable
  post-failure state is "no product advanced". A rollback that itself fails is
  flagged with a distinct alert, never silence. Readers pin an exact
  publication_id, so the brief window between an auto-promotion and its
  compensation is safe by design (a pinned reader never resolves "current");
* crash-safe compensation: the compensation set is derived from the
  ``bond_daily_chain_promotions`` ledger (promotes net of rollbacks) for the run,
  so an orphan promotion from a crashed invocation is reconciled and compensated on
  restart rather than left current on a failed run. RESIDUAL (named, not eliminated):
  the worker flips the pointer on its OWN connection and the chain records the ledger
  row on a SEPARATE, later transaction, so a crash between the checkpoint-succeeded
  commit and the ledger-insert commit leaves a pointer orphan INVISIBLE to
  ``_load_run_promotions`` (no ledger row, checkpoint present -> restart skips the
  stage). This is bounded, not fixed: readers pin an exact publication_id (never a
  live "current") and no Phase 10 value reaches serving, so the orphan is invisible
  to consumers; real elimination would require the worker to record its own promotion
  in the SAME transaction as the pointer flip (future candidate);
* per-product promotion enumeration in the summary (every product the run made
  current: product, previous -> new publication_id, stage);
* pointer rollback available (``rollback_pointer``);
* the required-stage-skipped rule reconciled with dark mode: a no-op because
  there is NO AUTHORIZED SOURCE is a REPORTED skip (``dark_no_source``); a
  required stage that produces no recognised result and no allowed skip reason
  is a FAILURE, never a silent success;
* exactly ONE summary per run.

Placement: this lives in ``src/bonds/`` next to its sibling engines
(``security_master``, ``price_observations``, ``serving_materializer``) because
the chain is the Bonds-family daily publication deliverable and shares their
fixtures/protocol. The thin worker entry point that wires the real workers to the
eight stages is ``src/workers/daily_publication_chain.py`` (mirroring the
``bond_serving`` worker -> ``serving_materializer`` engine split).
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import psycopg
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_daily_chain.sql"

DEFAULT_CHAIN = "bond_daily_publication"

# The eight frozen stages, in binding order (spec §5).
STAGE_ORDER: tuple[str, ...] = (
    "ingest",
    "pit_update",
    "materialize",
    "mixed_build",
    "validate",
    "promote",
    "refresh",
    "probe",
)

# The ONLY reasons a (required) stage may legitimately be skipped rather than run:
#   dark_no_source  -> the underlying worker no-oped because there is NO
#                      AUTHORIZED SOURCE (dark mode). Reported, not silent.
#   input_unchanged -> the stage's inputs are unchanged vs the current pinned
#                      fingerprint, so the product is deliberately not rebuilt.
# Any other "skip" of a required stage is upgraded to a run failure.
#
# NOTE (deliberate future-proofing): no stage unit emits ``input_unchanged``
# today — the existing workers rebuild-or-dark and never report an unchanged-input
# skip. It is in the allow-list now so that a future fingerprint-pinned "skip a
# product whose inputs are unchanged" (spec §5) is a REPORTED skip the moment a
# worker starts emitting it, without re-touching this frozen policy.
ALLOWED_SKIP_REASONS: frozenset[str] = frozenset({"dark_no_source", "input_unchanged"})

# Worker result vocabulary (state/status keys) -> chain classification.
_DARK_STATES: frozenset[str] = frozenset({
    "no_source", "no_observations", "no_securities",
    "no_effective_filings", "no_effective_facts",
})
# ``current`` is the shared publication protocol's own success state: the
# materializer workers build their envelope as ``{"state": "ok", **result}``
# where ``result`` carries the protocol's ``state='current'`` (self-promoted
# publication), which overrides the literal. A live (non-dark) run therefore
# reports ``current`` — a success, not an unrecognised result. (Latent until
# Wave 1: every earlier chain run was dark, so no materializer envelope ever
# reached this classification with a promoted state.)
_SUCCESS_STATES: frozenset[str] = frozenset({"ok", "ready", "current"})

# Namespace for reproducible chain run ids (distinct from the publication one).
_NAMESPACE_CHAIN_RUN = uuid.UUID("00000000-0000-5000-a000-636861696e01")


class StageStatus(str, Enum):
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    FAILED = "failed"


class TransientStageError(RuntimeError):
    """A retryable failure (connection blip, a busy sub-worker lock, ...)."""


class TerminalStageError(RuntimeError):
    """A non-retryable, fail-closed failure (coverage/integrity/contract)."""


@dataclass(frozen=True)
class StageOutcome:
    """The result of running one stage unit for one source-day."""

    status: StageStatus
    detail: Mapping[str, Any] = field(default_factory=dict)
    reason: str | None = None
    classification: str | None = None  # 'transient' | 'terminal' (FAILED only)
    watermarks: Mapping[str, Any] | None = None

    @classmethod
    def succeeded(cls, **detail: Any) -> "StageOutcome":
        wm = detail.pop("watermarks", None)
        return cls(StageStatus.SUCCEEDED, detail=detail, watermarks=wm)

    @classmethod
    def skipped(cls, reason: str, **detail: Any) -> "StageOutcome":
        wm = detail.pop("watermarks", None)
        return cls(StageStatus.SKIPPED, detail=detail, reason=reason, watermarks=wm)

    @classmethod
    def failed(cls, reason: str, classification: str = "terminal", **detail: Any) -> "StageOutcome":
        wm = detail.pop("watermarks", None)
        return cls(StageStatus.FAILED, detail=detail, reason=reason,
                   classification=classification, watermarks=wm)


@dataclass
class StageContext:
    """Everything a stage unit needs to run for one source-day."""

    conn: psycopg.Connection
    dsn: str | None
    source_day: date
    run_id: uuid.UUID
    chain: str
    code_revision: str
    config_version: str
    attempt: int = 1


# A stage unit: given a context, do its work and return a StageOutcome. It may
# instead raise TransientStageError/TerminalStageError (or any exception, which
# is classified by ``classify_exception``).
StageFn = Callable[[StageContext], StageOutcome]


@dataclass(frozen=True)
class Stage:
    name: str
    run: StageFn
    required: bool = True


# --------------------------------------------------------------------------- #
# Deterministic identity + schema
# --------------------------------------------------------------------------- #

def chain_run_id_for(chain: str, source_day: date, code_revision: str, config_version: str) -> uuid.UUID:
    return uuid.uuid5(
        _NAMESPACE_CHAIN_RUN,
        f"{chain}|{source_day.isoformat()}|{code_revision}|{config_version}",
    )


def install_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def default_backoff(attempt: int, *, base: float = 0.5, cap: float = 30.0) -> float:
    """Exponential backoff (seconds) for the ``attempt``-th try (1-based)."""
    return min(cap, base * (2 ** (attempt - 1)))


# --------------------------------------------------------------------------- #
# Worker-result / exception classification
# --------------------------------------------------------------------------- #

def classify_worker_result(result: Mapping[str, Any]) -> StageOutcome:
    """Map an existing worker's ``run()`` dict to a StageOutcome.

    ``ok``/``ready`` -> succeeded; a dark no-op state (``no_source`` etc.) ->
    reported skip(``dark_no_source``); ``locked`` -> transient (a sibling worker
    holds the sub-lock, retry); ``failed`` -> terminal. Anything unrecognised is
    a terminal failure — never a silent success.
    """
    state = str(result.get("state") or result.get("status") or "")
    if state in _SUCCESS_STATES:
        return StageOutcome.succeeded(**_result_detail(result))
    if state in _DARK_STATES:
        return StageOutcome.skipped("dark_no_source", **_result_detail(result))
    if state == "locked":
        raise TransientStageError(f"sub-worker locked: {result!r}")
    if state == "failed":
        raise TerminalStageError(f"sub-worker failed: {result!r}")
    raise TerminalStageError(f"unrecognised worker result: {result!r}")


def _result_detail(result: Mapping[str, Any]) -> dict[str, Any]:
    # Keep the worker's own state string plus small scalar counts as stage detail;
    # never carry nested payloads (avoid leaking internal blobs into the summary).
    out: dict[str, Any] = {"worker_state": str(result.get("state") or result.get("status") or "")}
    for key, value in result.items():
        if key in ("state", "status"):
            continue
        if isinstance(value, (int, float, str, bool)) or value is None:
            out[key] = value
    return out


def default_classify_exception(exc: BaseException) -> str:
    """Return 'transient' or 'terminal' for an exception raised by a stage.

    Fail-closed default: only explicitly transient conditions retry; everything
    else (coverage/integrity/contract errors, programming errors) is terminal.
    """
    if isinstance(exc, TransientStageError):
        return "transient"
    if isinstance(exc, TerminalStageError):
        return "terminal"
    if isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError)):
        return "transient"
    return "terminal"


# --------------------------------------------------------------------------- #
# Run/stage bookkeeping
# --------------------------------------------------------------------------- #

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _get_run(conn: psycopg.Connection, run_id: uuid.UUID) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT run_id, chain, source_day, code_revision, config_version, status, "
        "input_watermarks, summary FROM bond_daily_chain_runs WHERE run_id=%s",
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    keys = ("run_id", "chain", "source_day", "code_revision", "config_version",
            "status", "input_watermarks", "summary")
    return dict(zip(keys, row))


def _open_run(
    conn: psycopg.Connection, *, run_id: uuid.UUID, chain: str, source_day: date,
    code_revision: str, config_version: str, watermarks: Mapping[str, Any],
) -> None:
    conn.execute(
        "INSERT INTO bond_daily_chain_runs "
        "(run_id, chain, source_day, code_revision, config_version, status, input_watermarks) "
        "VALUES (%s,%s,%s,%s,%s,'running',%s) "
        "ON CONFLICT (run_id) DO UPDATE SET status='running', "
        "input_watermarks=EXCLUDED.input_watermarks, summary=NULL, finished_at=NULL",
        (run_id, chain, source_day, code_revision, config_version, Jsonb(dict(watermarks))),
    )


def _load_stage_runs(conn: psycopg.Connection, run_id: uuid.UUID) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        "SELECT stage, stage_order, status, reason, classification, attempts, detail, watermarks "
        "FROM bond_daily_chain_stage_runs WHERE run_id=%s",
        (run_id,),
    ).fetchall()
    keys = ("stage", "stage_order", "status", "reason", "classification", "attempts", "detail", "watermarks")
    return {r[0]: dict(zip(keys, r)) for r in rows}


def _load_run_promotions(conn: psycopg.Connection, run_id: uuid.UUID) -> dict[str, dict[str, Any]]:
    """Products THIS run promoted, net of rollbacks, from the promotion ledger.

    The crash-safe compensation set. After a restart the in-memory advance tracking
    is empty, but ``bond_daily_chain_promotions`` still records what this run made
    current (``action='promote'``) and the pre-run pointer it moved from
    (``previous_publication_id``). A product later rolled back (``action='rollback'``)
    is already compensated and drops out. Returns
    ``{product: {"previous", "current", "stage"}}`` (stage is ``"recovered"`` because
    the ledger does not record which stage promoted it).
    """
    rows = conn.execute(
        "SELECT product, action, publication_id, previous_publication_id "
        "FROM bond_daily_chain_promotions WHERE run_id=%s ORDER BY promotion_id",
        (run_id,),
    ).fetchall()
    net: dict[str, dict[str, Any]] = {}
    for product, action, pub_id, prev_id in rows:
        if action == "promote":
            if product in net:
                net[product]["current"] = pub_id
            else:
                net[product] = {"previous": prev_id, "current": pub_id, "stage": "recovered"}
        elif action == "rollback":
            net.pop(product, None)
    return net


def _record_stage(
    conn: psycopg.Connection, *, run_id: uuid.UUID, stage: str, order: int,
    outcome: StageOutcome, attempts: int, started_at: datetime,
) -> None:
    conn.execute(
        "INSERT INTO bond_daily_chain_stage_runs "
        "(run_id, stage, stage_order, status, reason, classification, attempts, detail, watermarks, "
        " started_at, finished_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()) "
        "ON CONFLICT (run_id, stage) DO UPDATE SET "
        "stage_order=EXCLUDED.stage_order, status=EXCLUDED.status, reason=EXCLUDED.reason, "
        "classification=EXCLUDED.classification, attempts=EXCLUDED.attempts, detail=EXCLUDED.detail, "
        "watermarks=EXCLUDED.watermarks, started_at=EXCLUDED.started_at, finished_at=now()",
        (run_id, stage, order, outcome.status.value, outcome.reason, outcome.classification,
         attempts, Jsonb(dict(outcome.detail)), Jsonb(dict(outcome.watermarks)) if outcome.watermarks else None,
         started_at),
    )


def _finish_run(
    conn: psycopg.Connection, *, run_id: uuid.UUID, status: str, summary: Mapping[str, Any],
) -> None:
    conn.execute(
        "UPDATE bond_daily_chain_runs SET status=%s, summary=%s, finished_at=now() WHERE run_id=%s",
        (status, Jsonb(dict(summary)), run_id),
    )


# --------------------------------------------------------------------------- #
# Stage execution with bounded retry/backoff
# --------------------------------------------------------------------------- #

def _reconcile_skip(stage: Stage, outcome: StageOutcome) -> StageOutcome:
    """Enforce the required-stage-skipped rule.

    A skip is legitimate only for an allowed reason; a required stage skipped for
    any other reason becomes a terminal failure (``required_stage_skipped``).
    """
    if outcome.status is StageStatus.SKIPPED and outcome.reason not in ALLOWED_SKIP_REASONS:
        if stage.required:
            return StageOutcome.failed(
                "required_stage_skipped", classification="terminal",
                original_reason=outcome.reason, **dict(outcome.detail),
            )
    return outcome


def _run_stage(
    stage: Stage, ctx: StageContext, *, max_attempts: int,
    backoff: Callable[[int], float], sleep: Callable[[float], None],
    classify_exception: Callable[[BaseException], str],
) -> tuple[StageOutcome, int]:
    """Run one stage with bounded retries. Returns (outcome, attempts_used).

    On the final attempt no branch continues, so the loop always returns from its
    body; there is no post-loop path.
    """
    for attempt in range(1, max_attempts + 1):
        retriable = attempt < max_attempts
        stage_ctx = StageContext(
            conn=ctx.conn, dsn=ctx.dsn, source_day=ctx.source_day, run_id=ctx.run_id,
            chain=ctx.chain, code_revision=ctx.code_revision,
            config_version=ctx.config_version, attempt=attempt,
        )
        try:
            outcome = stage.run(stage_ctx)
        except BaseException as exc:  # noqa: BLE001 — classified, not swallowed
            classification = classify_exception(exc)
            outcome = StageOutcome.failed(str(exc) or type(exc).__name__,
                                          classification=classification)
            # A stage that ran SQL on the chain's own connection may have left its
            # transaction aborted; roll it back so the checkpoint write (and any
            # retry) runs cleanly instead of raising InFailedSqlTransaction — which
            # would crash out of the run and skip compensation entirely.
            try:
                ctx.conn.rollback()
            except Exception:  # noqa: BLE001
                pass
        else:
            outcome = _reconcile_skip(stage, outcome)
        if outcome.status is StageStatus.FAILED and outcome.classification == "transient" and retriable:
            sleep(backoff(attempt))
            continue
        return (outcome, attempt)
    raise AssertionError("unreachable: the loop returns on the final attempt")


# --------------------------------------------------------------------------- #
# Chain driver
# --------------------------------------------------------------------------- #

def run_chain(
    conn: psycopg.Connection,
    *,
    stages: Sequence[Stage],
    source_days: Sequence[date],
    code_revision: str,
    config_version: str,
    chain: str = DEFAULT_CHAIN,
    dsn: str | None = None,
    watermarks_for: Callable[[psycopg.Connection, date], Mapping[str, Any]] | None = None,
    snapshot_pointers: Callable[[StageContext], Mapping[str, uuid.UUID]] | None = None,
    restore_pointer: Callable[[StageContext, str, uuid.UUID | None], bool] | None = None,
    max_attempts: int = 3,
    backoff: Callable[[int], float] = default_backoff,
    sleep: Callable[[float], None] = time.sleep,
    classify_exception: Callable[[BaseException], str] = default_classify_exception,
    staleness_threshold_days: int | None = None,
) -> list[dict[str, Any]]:
    """Drive the chain over ``source_days`` (deterministic catch-up = ascending).

    Returns one summary dict per source-day processed (already-completed runs are
    replayed as no-ops and their stored summary is returned). ``conn`` is the
    chain's own bookkeeping connection; it is committed after each stage so a
    restart resumes from the last checkpoint. The individual workers open their
    own connections via ``dsn`` (or the injected stage does its own I/O).

    ``snapshot_pointers`` / ``restore_pointer`` enable the compensation guarantee:
    when both are given the chain snapshots the current pointers around each stage,
    enumerates every product the run made current, and — on a terminal mid-chain
    failure — rolls each advanced product back to its pre-run pointer. Omit them
    (injected-stage tests that never promote) to disable compensation.

    ``watermarks_for`` records the per-source input watermarks on the run row;
    ``staleness_threshold_days`` (when set) turns those watermarks into a freshness
    metric and a distinct staleness alert (separate from the failure alert) when any
    source's lag behind the processed source-day reaches the threshold.
    """
    install_schema(conn)
    conn.commit()
    summaries: list[dict[str, Any]] = []
    for source_day in sorted(source_days):
        summaries.append(_run_one_day(
            conn, stages=stages, source_day=source_day, code_revision=code_revision,
            config_version=config_version, chain=chain, dsn=dsn,
            watermarks_for=watermarks_for, snapshot_pointers=snapshot_pointers,
            restore_pointer=restore_pointer, max_attempts=max_attempts, backoff=backoff,
            sleep=sleep, classify_exception=classify_exception,
            staleness_threshold_days=staleness_threshold_days,
        ))
    return summaries


def _run_one_day(
    conn: psycopg.Connection, *, stages: Sequence[Stage], source_day: date,
    code_revision: str, config_version: str, chain: str, dsn: str | None,
    watermarks_for: Callable[[psycopg.Connection, date], Mapping[str, Any]] | None,
    snapshot_pointers: Callable[[StageContext], Mapping[str, uuid.UUID]] | None,
    restore_pointer: Callable[[StageContext, str, uuid.UUID | None], bool] | None,
    max_attempts: int, backoff: Callable[[int], float], sleep: Callable[[float], None],
    classify_exception: Callable[[BaseException], str],
    staleness_threshold_days: int | None = None,
) -> dict[str, Any]:
    run_id = chain_run_id_for(chain, source_day, code_revision, config_version)

    existing = _get_run(conn, run_id)
    if existing is not None and existing["status"] == "completed" and existing["summary"]:
        # Idempotent replay: a completed run is never re-executed; its single
        # summary is returned verbatim.
        return dict(existing["summary"])

    watermarks = dict(watermarks_for(conn, source_day)) if watermarks_for else {}
    _open_run(conn, run_id=run_id, chain=chain, source_day=source_day,
              code_revision=code_revision, config_version=config_version, watermarks=watermarks)
    conn.commit()

    prior = _load_stage_runs(conn, run_id)
    ctx = StageContext(conn=conn, dsn=dsn, source_day=source_day, run_id=run_id,
                       chain=chain, code_revision=code_revision, config_version=config_version)

    track = snapshot_pointers is not None and restore_pointer is not None
    # Ledger-derived compensation set (crash-safe): every product THIS run already
    # promoted, net of rollbacks, read from ``bond_daily_chain_promotions``. After a
    # process crash the in-memory advance tracking is gone, but the ledger still
    # records the pre-crash promotion together with its authoritative pre-run pointer
    # (``previous_publication_id``). Reconciling those orphan promotions here means a
    # later terminal failure compensates them too — closing the crash-window the
    # Inc.2 final review flagged (a re-snapshot would wrongly treat the crashed
    # attempt's promotion as the baseline and leave it current on a failed run).
    ledger_advanced = _load_run_promotions(conn, run_id) if track else {}
    # baseline = the pre-run current pointers; the restore target for compensation.
    # For a product this run already promoted, the LEDGER's previous_publication_id
    # is the authoritative pre-run pointer (never the re-snapshotted current).
    baseline: dict[str, uuid.UUID | None] = dict(snapshot_pointers(ctx)) if track else {}
    for product, info in ledger_advanced.items():
        baseline[product] = info["previous"]
    last_seen: dict[str, uuid.UUID] = dict(snapshot_pointers(ctx)) if track else {}
    # product -> {previous (pre-run), current (latest), stage}
    advanced: dict[str, dict[str, Any]] = dict(ledger_advanced)

    stage_records: list[dict[str, Any]] = []
    failed = False
    for order, stage in enumerate(stages):
        checkpoint = prior.get(stage.name)
        if checkpoint is not None and checkpoint["status"] in ("succeeded", "skipped"):
            # Restart honours the terminal per-stage checkpoint: do not re-run.
            stage_records.append(_stage_summary(stage.name, order, checkpoint, resumed=True))
            continue

        started_at = _now()
        outcome, attempts = _run_stage(
            stage, ctx, max_attempts=max_attempts, backoff=backoff, sleep=sleep,
            classify_exception=classify_exception,
        )
        _record_stage(conn, run_id=run_id, stage=stage.name, order=order,
                      outcome=outcome, attempts=attempts, started_at=started_at)
        conn.commit()
        stage_records.append(_stage_summary_from_outcome(stage.name, order, outcome, attempts))

        if track and outcome.status is StageStatus.SUCCEEDED:
            # A worker self-promotes atomically inside its stage; detect which
            # products this stage made current by diffing the pointer snapshot.
            after = dict(snapshot_pointers(ctx))
            for product, pub_id in after.items():
                if last_seen.get(product) != pub_id:
                    # Preserve the ORIGINAL pre-run pointer if this run already
                    # tracked the product (a re-promotion must not overwrite the true
                    # restore target — ledger-seeded or first-seen — with an interim).
                    previous = advanced[product]["previous"] if product in advanced else baseline.get(product)
                    advanced[product] = {"previous": previous, "current": pub_id, "stage": stage.name}
                    record_promotion(conn, product=product, publication_id=pub_id,
                                     previous_publication_id=previous,
                                     run_id=run_id, action="promote")
            last_seen = after
            conn.commit()

        if outcome.status is StageStatus.FAILED:
            failed = True
            break

    compensations: list[dict[str, Any]] = []
    compensation_failures: list[dict[str, Any]] = []
    if failed and track and advanced:
        compensations, compensation_failures = _compensate(
            conn, ctx, advanced=advanced, restore_pointer=restore_pointer, run_id=run_id)
        conn.commit()

    status = "failed" if failed else "completed"
    promoted = [] if failed else [
        {"product": p, "previous_publication_id": _str_or_none(info["previous"]),
         "publication_id": _str_or_none(info["current"]), "stage": info["stage"]}
        for p, info in advanced.items()
    ]
    freshness = _freshness(source_day, watermarks, staleness_threshold_days)
    summary = _build_summary(
        run_id=run_id, chain=chain, source_day=source_day, code_revision=code_revision,
        config_version=config_version, status=status, watermarks=watermarks,
        stage_records=stage_records, promoted=promoted,
        compensations=compensations, compensation_failures=compensation_failures,
        freshness=freshness, staleness_alert=_staleness_alert(chain, source_day, freshness),
    )
    _finish_run(conn, run_id=run_id, status=status, summary=summary)
    conn.commit()
    return summary


def _str_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


def _compensate(
    conn: psycopg.Connection, ctx: StageContext, *, advanced: Mapping[str, dict[str, Any]],
    restore_pointer: Callable[[StageContext, str, uuid.UUID | None], bool], run_id: uuid.UUID,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Roll each product the run advanced back to its pre-run pointer.

    Stable post-failure state = "no product advanced". A restore that itself
    raises is captured as a compensation FAILURE (surfaced via a distinct alert),
    never silently dropped. The advancing stages' checkpoints are reset so a
    restart re-executes (re-promotes) them consistently.
    """
    compensations: list[dict[str, Any]] = []
    compensation_failures: list[dict[str, Any]] = []
    reset_stages: set[str] = set()
    reset_all = False
    for product, info in advanced.items():
        prior_target = info["previous"]
        try:
            restore_pointer(ctx, product, prior_target)
            record_promotion(conn, product=product, publication_id=prior_target,
                             previous_publication_id=info["current"], run_id=run_id,
                             action="rollback")
            compensations.append({
                "product": product, "restored_to": _str_or_none(prior_target),
                "rolled_back_from": _str_or_none(info["current"]), "stage": info["stage"],
            })
            if info["stage"] == "recovered":
                # Ledger-derived orphan (a promotion recovered from a prior crashed
                # invocation): the promoting stage is unknown, so reset the whole
                # run's checkpoints to force a clean, idempotent re-run that
                # re-promotes on restart rather than resuming past the undone advance.
                reset_all = True
            else:
                reset_stages.add(info["stage"])
        except Exception as exc:  # noqa: BLE001 — recorded, never silent
            compensation_failures.append({
                "product": product, "attempted_restore_to": _str_or_none(prior_target),
                "still_current": _str_or_none(info["current"]), "error": str(exc) or type(exc).__name__,
            })
    if reset_all:
        conn.execute("DELETE FROM bond_daily_chain_stage_runs WHERE run_id=%s", (run_id,))
    elif reset_stages:
        _delete_stage_checkpoints(conn, run_id, reset_stages)
    return compensations, compensation_failures


def _delete_stage_checkpoints(conn: psycopg.Connection, run_id: uuid.UUID, stages: set[str]) -> None:
    conn.execute(
        "DELETE FROM bond_daily_chain_stage_runs WHERE run_id=%s AND stage = ANY(%s)",
        (run_id, list(stages)),
    )


def _stage_summary_from_outcome(name: str, order: int, outcome: StageOutcome, attempts: int) -> dict[str, Any]:
    return {
        "stage": name,
        "order": order,
        "status": outcome.status.value,
        "reason": outcome.reason,
        "classification": outcome.classification,
        "attempts": attempts,
        "detail": dict(outcome.detail),
        "watermarks": dict(outcome.watermarks) if outcome.watermarks else None,
        "resumed": False,
    }


def _stage_summary(name: str, order: int, checkpoint: Mapping[str, Any], *, resumed: bool) -> dict[str, Any]:
    return {
        "stage": name,
        "order": order,
        "status": checkpoint["status"],
        "reason": checkpoint["reason"],
        "classification": checkpoint["classification"],
        "attempts": checkpoint["attempts"],
        "detail": dict(checkpoint["detail"] or {}),
        "watermarks": dict(checkpoint["watermarks"]) if checkpoint["watermarks"] else None,
        "resumed": resumed,
    }


def _as_date(value: Any) -> date | None:
    """Parse a watermark value into a date (ISO date/datetime string or date), else None."""
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _freshness(
    source_day: date, watermarks: Mapping[str, Any], threshold_days: int | None,
) -> dict[str, Any]:
    """Freshness of the chain's source inputs relative to the processed source-day.

    For every watermark that is a date, the lag in days behind ``source_day`` is
    measured; a source whose lag reaches ``threshold_days`` (when configured) is
    stale. Non-date/None watermarks (a dark source with no data) are ignored, so the
    metric is empty in dark mode and never fabricates freshness.
    """
    lags: dict[str, int] = {}
    stale: list[str] = []
    for source, value in watermarks.items():
        wm_date = _as_date(value)
        if wm_date is None:
            continue
        lag = (source_day - wm_date).days
        lags[source] = lag
        if threshold_days is not None and lag >= threshold_days:
            stale.append(source)
    return {
        "source_day": source_day.isoformat(),
        "threshold_days": threshold_days,
        "lags_days": lags,
        "max_lag_days": max(lags.values()) if lags else None,
        "stale_sources": sorted(stale),
    }


def _staleness_alert(chain: str, source_day: date, freshness: Mapping[str, Any]) -> str | None:
    """A distinct staleness alert (separate from the failure alert); None when fresh."""
    stale = list(freshness.get("stale_sources") or [])
    if not stale:
        return None
    return (
        f"[{chain}] run for {source_day.isoformat()} STALE INPUT: {len(stale)} "
        f"source(s) at/over {freshness.get('threshold_days')}d [{', '.join(stale)}]; "
        f"max lag {freshness.get('max_lag_days')}d"
    )


def _build_summary(
    *, run_id: uuid.UUID, chain: str, source_day: date, code_revision: str,
    config_version: str, status: str, watermarks: Mapping[str, Any],
    stage_records: Sequence[Mapping[str, Any]],
    promoted: Sequence[Mapping[str, Any]],
    compensations: Sequence[Mapping[str, Any]],
    compensation_failures: Sequence[Mapping[str, Any]],
    freshness: Mapping[str, Any],
    staleness_alert: str | None,
) -> dict[str, Any]:
    failures = [
        {"stage": s["stage"], "reason": s["reason"], "classification": s["classification"]}
        for s in stage_records if s["status"] == "failed"
    ]
    skips = [
        {"stage": s["stage"], "reason": s["reason"]}
        for s in stage_records if s["status"] == "skipped"
    ]
    return {
        "run_id": str(run_id),
        "chain": chain,
        "source_day": source_day.isoformat(),
        "code_revision": code_revision,
        "config_version": config_version,
        "status": status,
        # Enumeration of every product the run made current (product, prev->new,
        # stage). Empty on a failed run (all advances were compensated).
        "promoted": [dict(p) for p in promoted],
        "promoted_count": len(promoted),
        "compensations": [dict(c) for c in compensations],
        "compensation_failures": [dict(c) for c in compensation_failures],
        "input_watermarks": dict(watermarks),
        # Freshness of the source inputs vs the processed source-day + a staleness
        # alert kept DISTINCT from the pipeline failure ``alert`` below.
        "freshness": dict(freshness),
        "staleness_alert": staleness_alert,
        "stages": [dict(s) for s in stage_records],
        "skips": skips,
        "failures": failures,
        # A single actionable alert string when the run did not cleanly complete
        # its full pipeline (failure/compensation OR a dark skip worth noting).
        "alert": _alert_line(chain, source_day, status, failures, skips,
                             compensations, compensation_failures),
    }


def _alert_line(
    chain: str, source_day: date, status: str,
    failures: Sequence[Mapping[str, Any]], skips: Sequence[Mapping[str, Any]],
    compensations: Sequence[Mapping[str, Any]] = (),
    compensation_failures: Sequence[Mapping[str, Any]] = (),
) -> str | None:
    tag = f"[{chain}] run for {source_day.isoformat()}"
    if status == "failed":
        first = failures[0] if failures else {"stage": "?", "reason": "?"}
        line = (f"{tag} FAILED at stage {first['stage']} "
                f"({first.get('classification')}: {first['reason']})")
        if compensation_failures:
            # Distinct, louder alert: some pointers could NOT be rolled back.
            products = ", ".join(c["product"] for c in compensation_failures)
            return (f"{line}; COMPENSATION FAILED for {len(compensation_failures)} "
                    f"product(s) [{products}] — MANUAL rollback required")
        if compensations:
            products = ", ".join(c["product"] for c in compensations)
            return (f"{line}; {len(compensations)} product(s) auto-rolled back to prior "
                    f"current [{products}] — no product advanced")
        return line
    dark = [s for s in skips if s["reason"] == "dark_no_source"]
    if dark:
        return (f"{tag} completed in DARK mode "
                f"({len(dark)} stage(s) had no authorized source): nothing promoted")
    return None


# --------------------------------------------------------------------------- #
# Pointer rollback (spec §5: "current version remains available for rollback")
# --------------------------------------------------------------------------- #

def record_promotion(
    conn: psycopg.Connection, *, product: str, publication_id: uuid.UUID | None,
    previous_publication_id: uuid.UUID | None, run_id: uuid.UUID | None = None,
    action: str = "promote",
) -> None:
    """Record a pointer move so a later rollback has a deterministic prior target."""
    conn.execute(
        "INSERT INTO bond_daily_chain_promotions "
        "(run_id, product, action, publication_id, previous_publication_id) "
        "VALUES (%s,%s,%s,%s,%s)",
        (run_id, product, action, publication_id, previous_publication_id),
    )


def current_pointer(conn: psycopg.Connection, product: str) -> uuid.UUID | None:
    """The product's current derived publication pointer (None if unset)."""
    row = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
        (product,),
    ).fetchone()
    return None if row is None else row[0]


def rollback_pointer(
    conn: psycopg.Connection, product: str, *, run_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    """Restore the product's current pointer to its immediately-prior target.

    Reads the most recent *promote* record for the product and re-points the
    current pointer at ``previous_publication_id`` via the atomic, validated-only
    ``sec_set_current_derived_publication``. Records the rollback in the ledger.
    Raises if there is no prior target to restore to.
    """
    row = conn.execute(
        "SELECT publication_id, previous_publication_id FROM bond_daily_chain_promotions "
        "WHERE product=%s AND action='promote' ORDER BY promotion_id DESC LIMIT 1",
        (product,),
    ).fetchone()
    if row is None:
        raise TerminalStageError(f"no promotion history for product {product!r}")
    rolled_from, restore_to = row
    if restore_to is None:
        raise TerminalStageError(
            f"product {product!r} has no prior pointer to roll back to (first publication)")
    conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (product, restore_to))
    record_promotion(conn, product=product, publication_id=restore_to,
                     previous_publication_id=rolled_from, run_id=run_id, action="rollback")
    return {"product": product, "rolled_back_from": str(rolled_from), "restored_to": str(restore_to)}


def promote_derived(
    conn: psycopg.Connection, product: str, publication_id: uuid.UUID, *,
    run_id: uuid.UUID | None = None,
) -> uuid.UUID | None:
    """Atomically set the product's current pointer, recording history for rollback.

    Uses the existing ``sec_set_current_derived_publication`` (validated-only,
    per-product advisory xact lock). Returns the previous pointer (or None).
    """
    previous = current_pointer(conn, product)
    conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (product, publication_id))
    record_promotion(conn, product=product, publication_id=publication_id,
                     previous_publication_id=previous, run_id=run_id, action="promote")
    return previous
