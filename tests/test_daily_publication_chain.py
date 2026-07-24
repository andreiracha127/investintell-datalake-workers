"""Engine tests for the daily publication chain (Increment 2, Task 6; spec §5).

Every required behavior is exercised against the disposable Postgres at
``SEC_TEST_DATABASE_URL`` (PG 65431):
  * advisory lock prevents overlapping runs (second run blocked);
  * replay is idempotent (same run identity, checkpoints honoured, no re-run);
  * restart resumes from the mid-chain checkpoint;
  * catch-up processes missed days in ascending source-day order;
  * partial promotion is impossible (a failing stage aborts before promote,
    prior current pointer intact);
  * the pointer rollback operation restores the prior pointer;
  * bounded retries with backoff for transient failures; typed terminal fails
    closed immediately;
  * the skip-vs-failure distinction (dark/input_unchanged skip vs required-stage
    skipped = failure);
  * exactly ONE summary per run, with per-stage detail.

The chain-run advisory lock is database-global, so the lock-overlap test uses the
worker entry point and the engine tests inject their own stage units.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _daily_chain_fixtures import (  # noqa: E402
    admin_connect,
    base_dsn,
    install_derived_protocol,
    make_validated_publication,
    new_schema,
    worker_conn,
)

from src.bonds import daily_chain  # noqa: E402
from src.bonds.daily_chain import (  # noqa: E402
    Stage,
    StageContext,
    StageOutcome,
    TransientStageError,
)
from src.quant_data import publication as quant_pub  # noqa: E402
from src.workers import daily_publication_chain as chain_worker  # noqa: E402


def _search_path_dsn(schema: str) -> str:
    base = base_dsn()
    if base.startswith("postgres"):
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}options=-c%20search_path%3D{schema}"
    return f"{base} options='-c search_path={schema}'"

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL not set"
)

D1, D2, D3 = date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)
REV, CFG = "revA", "v1"


@pytest.fixture()
def env():
    admin = admin_connect()
    schema = new_schema(admin)
    conn = worker_conn(schema)
    try:
        yield conn, schema, admin
    finally:
        conn.close()
        admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


# --------------------------------------------------------------------------- #
# Injected stage helpers
# --------------------------------------------------------------------------- #

def ok_stage(name: str, calls: list[str] | None = None):
    def _fn(ctx: StageContext) -> StageOutcome:
        if calls is not None:
            calls.append(f"{name}:{ctx.source_day.isoformat()}")
        return StageOutcome.succeeded(marker=name)
    return Stage(name, _fn)


def _stages(names, calls=None):
    return [ok_stage(n, calls) for n in names]


# --------------------------------------------------------------------------- #
# Replay idempotence
# --------------------------------------------------------------------------- #

def test_replay_is_idempotent_same_identity_and_no_rerun(env):
    conn, _, _ = env
    calls: list[str] = []
    stages = _stages(["ingest", "pit_update", "probe"], calls)
    first = daily_chain.run_chain(conn, stages=stages, source_days=[D1],
                                  code_revision=REV, config_version=CFG)
    calls_after_first = list(calls)
    second = daily_chain.run_chain(conn, stages=stages, source_days=[D1],
                                   code_revision=REV, config_version=CFG)
    assert first[0]["run_id"] == second[0]["run_id"]
    assert first[0]["status"] == "completed" == second[0]["status"]
    # Replay of a completed run re-invokes NOTHING.
    assert calls == calls_after_first
    # Exactly one run row and one summary for the identity.
    n = conn.execute("SELECT count(*) FROM bond_daily_chain_runs WHERE run_id=%s",
                     (UUID(first[0]["run_id"]),)).fetchone()[0]
    assert n == 1


# --------------------------------------------------------------------------- #
# Restart from a mid-chain checkpoint
# --------------------------------------------------------------------------- #

def test_restart_resumes_from_midchain_checkpoint(env):
    conn, _, _ = env
    calls: list[str] = []
    fail_flag = {"fail": True}

    def flaky(ctx: StageContext) -> StageOutcome:
        calls.append("materialize")
        if fail_flag["fail"]:
            return StageOutcome.failed("boom", classification="terminal")
        return StageOutcome.succeeded()

    stages = [ok_stage("ingest", calls), ok_stage("pit_update", calls),
              Stage("materialize", flaky), ok_stage("probe", calls)]

    first = daily_chain.run_chain(conn, stages=stages, source_days=[D1],
                                  code_revision=REV, config_version=CFG)
    assert first[0]["status"] == "failed"
    # Later stage never ran; earlier stages checkpointed.
    assert "probe:2025-01-01" not in calls
    assert calls.count("ingest:2025-01-01") == 1

    fail_flag["fail"] = False
    second = daily_chain.run_chain(conn, stages=stages, source_days=[D1],
                                   code_revision=REV, config_version=CFG)
    assert second[0]["status"] == "completed"
    # ingest/pit_update honoured from checkpoint (NOT re-invoked); materialize
    # retried; probe now runs.
    assert calls.count("ingest:2025-01-01") == 1
    assert calls.count("pit_update:2025-01-01") == 1
    assert calls.count("materialize") == 2
    assert "probe:2025-01-01" in calls
    # Resumed stages are marked as such in the summary.
    resumed = {s["stage"]: s["resumed"] for s in second[0]["stages"]}
    assert resumed["ingest"] is True and resumed["materialize"] is False


# --------------------------------------------------------------------------- #
# Catch-up: missed days processed in ascending order
# --------------------------------------------------------------------------- #

def test_catch_up_processes_missed_days_in_order(env):
    conn, _, _ = env
    seen: list[str] = []

    def recorder(ctx: StageContext) -> StageOutcome:
        seen.append(ctx.source_day.isoformat())
        return StageOutcome.succeeded()

    stages = [Stage("probe", recorder)]
    # Pre-complete D1 so catch-up must skip it and process D2, D3 only, in order.
    daily_chain.run_chain(conn, stages=stages, source_days=[D1],
                          code_revision=REV, config_version=CFG)
    seen.clear()
    summaries = daily_chain.run_chain(conn, stages=stages, source_days=[D3, D1, D2],
                                      code_revision=REV, config_version=CFG)
    assert seen == ["2025-01-02", "2025-01-03"]  # D1 replayed (no re-run), ascending
    assert [s["source_day"] for s in summaries] == ["2025-01-01", "2025-01-02", "2025-01-03"]


# --------------------------------------------------------------------------- #
# Bounded retries with backoff (transient) vs terminal fail-closed
# --------------------------------------------------------------------------- #

def test_transient_failure_retries_with_backoff_then_succeeds(env):
    conn, _, _ = env
    sleeps: list[float] = []
    attempts = {"n": 0}

    def flaky(ctx: StageContext) -> StageOutcome:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise TransientStageError("blip")
        return StageOutcome.succeeded()

    summaries = daily_chain.run_chain(
        conn, stages=[Stage("ingest", flaky)], source_days=[D1],
        code_revision=REV, config_version=CFG, max_attempts=3,
        backoff=lambda a: float(a), sleep=sleeps.append,
    )
    assert summaries[0]["status"] == "completed"
    stage = summaries[0]["stages"][0]
    assert stage["status"] == "succeeded" and stage["attempts"] == 3
    assert sleeps == [1.0, 2.0]  # backoff called between the three attempts


def test_transient_failure_exhausts_to_terminal_failure(env):
    conn, _, _ = env

    def always_transient(ctx: StageContext) -> StageOutcome:
        raise TransientStageError("still down")

    summaries = daily_chain.run_chain(
        conn, stages=[Stage("ingest", always_transient), ok_stage("probe")],
        source_days=[D1], code_revision=REV, config_version=CFG,
        max_attempts=2, backoff=lambda a: 0.0, sleep=lambda s: None,
    )
    assert summaries[0]["status"] == "failed"
    stage = summaries[0]["stages"][0]
    assert stage["status"] == "failed" and stage["classification"] == "transient"
    assert stage["attempts"] == 2
    # The later stage never ran (fail-closed).
    assert all(s["stage"] != "probe" for s in summaries[0]["stages"])


def test_terminal_failure_fails_closed_immediately(env):
    conn, _, _ = env
    calls = {"n": 0}

    def terminal(ctx: StageContext) -> StageOutcome:
        calls["n"] += 1
        raise ValueError("coverage breach")  # not a transient type -> terminal

    summaries = daily_chain.run_chain(
        conn, stages=[Stage("validate", terminal)], source_days=[D1],
        code_revision=REV, config_version=CFG, max_attempts=3,
        backoff=lambda a: 0.0, sleep=lambda s: None,
    )
    assert summaries[0]["status"] == "failed"
    assert summaries[0]["stages"][0]["classification"] == "terminal"
    assert calls["n"] == 1  # no retries on a terminal failure


# --------------------------------------------------------------------------- #
# Skip vs failure distinction
# --------------------------------------------------------------------------- #

def test_dark_no_source_is_reported_skip_not_failure(env):
    conn, _, _ = env

    def dark(ctx: StageContext) -> StageOutcome:
        return StageOutcome.skipped("dark_no_source")

    summaries = daily_chain.run_chain(
        conn, stages=[Stage("ingest", dark), Stage("pit_update", dark)],
        source_days=[D1], code_revision=REV, config_version=CFG,
    )
    s = summaries[0]
    assert s["status"] == "completed"  # reported skip, not a failure
    assert all(st["status"] == "skipped" and st["reason"] == "dark_no_source" for st in s["stages"])
    assert s["promoted"] == []  # nothing made current this run
    assert "DARK mode" in (s["alert"] or "")


def test_input_unchanged_is_allowed_skip(env):
    conn, _, _ = env

    def unchanged(ctx: StageContext) -> StageOutcome:
        return StageOutcome.skipped("input_unchanged", fingerprint="pinned")

    summaries = daily_chain.run_chain(
        conn, stages=[Stage("materialize", unchanged)], source_days=[D1],
        code_revision=REV, config_version=CFG,
    )
    assert summaries[0]["status"] == "completed"
    assert summaries[0]["stages"][0]["reason"] == "input_unchanged"


def test_required_stage_skipped_without_allowed_reason_is_failure(env):
    conn, _, _ = env

    def bogus_skip(ctx: StageContext) -> StageOutcome:
        return StageOutcome.skipped("stage_not_implemented")  # not an allowed reason

    summaries = daily_chain.run_chain(
        conn, stages=[Stage("materialize", bogus_skip, required=True), ok_stage("probe")],
        source_days=[D1], code_revision=REV, config_version=CFG,
    )
    s = summaries[0]
    assert s["status"] == "failed"
    stage = s["stages"][0]
    assert stage["status"] == "failed" and stage["reason"] == "required_stage_skipped"
    assert stage["detail"]["original_reason"] == "stage_not_implemented"
    # fail-closed: probe never ran.
    assert all(st["stage"] != "probe" for st in s["stages"])


# --------------------------------------------------------------------------- #
# One summary per run, with per-stage detail
# --------------------------------------------------------------------------- #

def test_exactly_one_summary_per_run_with_per_stage_detail(env):
    conn, _, _ = env
    names = ["ingest", "pit_update", "materialize", "mixed_build", "validate",
             "promote", "refresh", "probe"]
    summaries = daily_chain.run_chain(conn, stages=_stages(names), source_days=[D1],
                                      code_revision=REV, config_version=CFG)
    run_id = UUID(summaries[0]["run_id"])
    # Persisted summary is singular.
    rows = conn.execute(
        "SELECT summary FROM bond_daily_chain_runs WHERE run_id=%s", (run_id,)
    ).fetchall()
    assert len(rows) == 1
    persisted = rows[0][0]
    assert persisted["status"] == "completed"
    assert [s["stage"] for s in persisted["stages"]] == names
    # Every stage carries its per-stage status/attempts/detail.
    for s in persisted["stages"]:
        assert s["status"] in ("succeeded", "skipped", "failed")
        assert "attempts" in s and "detail" in s
    # One stage_run checkpoint row per stage, no duplicates.
    n_stage_rows = conn.execute(
        "SELECT count(*) FROM bond_daily_chain_stage_runs WHERE run_id=%s", (run_id,)
    ).fetchone()[0]
    assert n_stage_rows == len(names)


# --------------------------------------------------------------------------- #
# Partial promotion impossible (real derived-publication protocol)
# --------------------------------------------------------------------------- #

def test_partial_promotion_impossible_prior_pointer_intact(env):
    conn, _, _ = env
    install_derived_protocol(conn)
    daily_chain.install_schema(conn)
    conn.commit()
    product = "bond_serving_v1"
    pub_a = make_validated_publication(conn, product, 1)
    pub_b = make_validated_publication(conn, product, 2)
    daily_chain.promote_derived(conn, product, pub_a)  # current = A
    conn.commit()
    assert daily_chain.current_pointer(conn, product) == pub_a

    promote_called = {"n": 0}

    def failing_validate(ctx: StageContext) -> StageOutcome:
        return StageOutcome.failed("coverage_gate_failed", classification="terminal")

    def promote_b(ctx: StageContext) -> StageOutcome:
        promote_called["n"] += 1
        daily_chain.promote_derived(ctx.conn, product, pub_b)
        return StageOutcome.succeeded()

    stages = [ok_stage("materialize"), Stage("validate", failing_validate),
              Stage("promote", promote_b)]
    summaries = daily_chain.run_chain(conn, stages=stages, source_days=[D1],
                                      code_revision=REV, config_version=CFG)
    assert summaries[0]["status"] == "failed"
    assert summaries[0]["promoted"] == []
    # promote stage never executed; prior current pointer intact.
    assert promote_called["n"] == 0
    assert daily_chain.current_pointer(conn, product) == pub_a


# --------------------------------------------------------------------------- #
# Pointer rollback restores the prior pointer
# --------------------------------------------------------------------------- #

def test_rollback_pointer_restores_prior_target(env):
    conn, _, _ = env
    install_derived_protocol(conn)
    daily_chain.install_schema(conn)
    conn.commit()
    product = "bond_serving_v1"
    pub_a = make_validated_publication(conn, product, 1)
    pub_b = make_validated_publication(conn, product, 2)
    daily_chain.promote_derived(conn, product, pub_a)
    daily_chain.promote_derived(conn, product, pub_b)
    conn.commit()
    assert daily_chain.current_pointer(conn, product) == pub_b

    result = daily_chain.rollback_pointer(conn, product)
    conn.commit()
    assert result["restored_to"] == str(pub_a)
    assert daily_chain.current_pointer(conn, product) == pub_a
    # The rollback is itself recorded in the ledger.
    action = conn.execute(
        "SELECT action FROM bond_daily_chain_promotions WHERE product=%s ORDER BY promotion_id DESC LIMIT 1",
        (product,),
    ).fetchone()[0]
    assert action == "rollback"


def test_rollback_pointer_without_prior_raises(env):
    conn, _, _ = env
    install_derived_protocol(conn)
    daily_chain.install_schema(conn)
    conn.commit()
    product = "bond_serving_v1"
    pub_a = make_validated_publication(conn, product, 1)
    daily_chain.promote_derived(conn, product, pub_a)  # first promotion, no prior
    conn.commit()
    with pytest.raises(daily_chain.TerminalStageError):
        daily_chain.rollback_pointer(conn, product)


# --------------------------------------------------------------------------- #
# Per-product promotion enumeration + auto-rollback compensation (spec §5 via
# per-product atomic self-promotion; chain-level guarantee by compensation)
# --------------------------------------------------------------------------- #

def _pointer_snapshot(ctx: StageContext) -> dict:
    return {p: pid for p, pid in ctx.conn.execute(
        "SELECT product, publication_id FROM sec_derived_current_pointers").fetchall()}


def _pointer_restore(ctx: StageContext, product: str, prior) -> bool:
    if prior is None:
        return False
    ctx.conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (product, prior))
    return True


def _self_promote_stage(name: str, product: str, pub):
    def _fn(ctx: StageContext) -> StageOutcome:
        # A worker self-promotes atomically inside its own stage (Inc.1 architecture).
        ctx.conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (product, pub))
        return StageOutcome.succeeded(product=product)
    return Stage(name, _fn)


def _seed_two_products(conn):
    install_derived_protocol(conn)
    daily_chain.install_schema(conn)
    conn.commit()
    ps, pp = "bond_security_v1", "bond_price_observation_v1"
    a1 = make_validated_publication(conn, ps, 1)
    a2 = make_validated_publication(conn, ps, 2)
    b1 = make_validated_publication(conn, pp, 1)
    b2 = make_validated_publication(conn, pp, 2)
    conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (ps, a1))
    conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (pp, b1))
    conn.commit()
    return ps, pp, a1, a2, b1, b2


def test_clean_run_enumerates_every_product_made_current(env):
    conn, _, _ = env
    ps, pp, a1, a2, b1, b2 = _seed_two_products(conn)
    stages = [_self_promote_stage("pit_update", ps, a2),
              _self_promote_stage("materialize", pp, b2),
              ok_stage("refresh")]
    summaries = daily_chain.run_chain(
        conn, stages=stages, source_days=[D1], code_revision=REV, config_version=CFG,
        snapshot_pointers=_pointer_snapshot, restore_pointer=_pointer_restore,
    )
    s = summaries[0]
    assert s["status"] == "completed"
    promoted = {p["product"]: p for p in s["promoted"]}
    # Enumeration names every product the chain made current, prev->new + stage.
    assert promoted[ps]["previous_publication_id"] == str(a1)
    assert promoted[ps]["publication_id"] == str(a2)
    assert promoted[ps]["stage"] == "pit_update"
    assert promoted[pp]["publication_id"] == str(b2) and promoted[pp]["stage"] == "materialize"
    assert s["promoted_count"] == 2
    assert s["compensations"] == []


def test_terminal_failure_after_autopromotions_compensates_all_products(env):
    conn, _, _ = env
    ps, pp, a1, a2, b1, b2 = _seed_two_products(conn)

    def refresh_fail(ctx: StageContext) -> StageOutcome:
        return StageOutcome.failed("serving_coverage_below_threshold", classification="terminal")

    stages = [_self_promote_stage("pit_update", ps, a2),
              _self_promote_stage("materialize", pp, b2),
              Stage("refresh", refresh_fail)]
    summaries = daily_chain.run_chain(
        conn, stages=stages, source_days=[D1], code_revision=REV, config_version=CFG,
        snapshot_pointers=_pointer_snapshot, restore_pointer=_pointer_restore,
    )
    s = summaries[0]
    assert s["status"] == "failed"
    # EVERY advanced pointer restored to its prior current -> no product advanced.
    assert daily_chain.current_pointer(conn, ps) == a1
    assert daily_chain.current_pointer(conn, pp) == b1
    comp = {c["product"]: c for c in s["compensations"]}
    assert comp[ps]["restored_to"] == str(a1) and comp[ps]["rolled_back_from"] == str(a2)
    assert comp[pp]["restored_to"] == str(b1) and comp[pp]["rolled_back_from"] == str(b2)
    assert s["compensation_failures"] == []
    assert s["promoted"] == []  # nothing STAYS current from a failed run
    assert "rolled back" in (s["alert"] or "").lower()
    assert "no product advanced" in (s["alert"] or "").lower()
    # The rollbacks are recorded in the promotion ledger.
    n_rollback = conn.execute(
        "SELECT count(*) FROM bond_daily_chain_promotions WHERE action='rollback'"
    ).fetchone()[0]
    assert n_rollback == 2
    # Compensated advancing stages are reset so a restart re-runs (re-promotes) them.
    remaining = {r[0] for r in conn.execute(
        "SELECT stage FROM bond_daily_chain_stage_runs WHERE run_id=%s",
        (UUID(s["run_id"]),)).fetchall()}
    assert "pit_update" not in remaining and "materialize" not in remaining


def test_compensation_failure_is_flagged_distinctly_never_silent(env):
    conn, _, _ = env
    ps, pp, a1, a2, b1, b2 = _seed_two_products(conn)

    def restore_that_fails(ctx: StageContext, product: str, prior) -> bool:
        if product == pp:
            raise RuntimeError("pointer restore blew up")
        return _pointer_restore(ctx, product, prior)

    def refresh_fail(ctx: StageContext) -> StageOutcome:
        return StageOutcome.failed("boom", classification="terminal")

    stages = [_self_promote_stage("pit_update", ps, a2),
              _self_promote_stage("materialize", pp, b2),
              Stage("refresh", refresh_fail)]
    summaries = daily_chain.run_chain(
        conn, stages=stages, source_days=[D1], code_revision=REV, config_version=CFG,
        snapshot_pointers=_pointer_snapshot, restore_pointer=restore_that_fails,
    )
    s = summaries[0]
    assert s["status"] == "failed"
    # ps restored; pp restore failed -> flagged distinctly, never silent.
    assert daily_chain.current_pointer(conn, ps) == a1
    failed = {c["product"] for c in s["compensation_failures"]}
    assert pp in failed
    assert "COMPENSATION FAILED" in (s["alert"] or "")


# --------------------------------------------------------------------------- #
# Crash-safe compensation: ledger-derived set + restart-with-orphan handled
# --------------------------------------------------------------------------- #

def test_restart_compensates_orphan_promotion_derived_from_ledger(env):
    """The crash-window the Inc.2 final review flagged.

    A process crash after a stage promoted leaves the promotion committed (in
    ``bond_daily_chain_promotions`` with the run's id and pre-run pointer) but the
    in-memory advance tracking gone and no compensation performed. On restart the
    compensation set is DERIVED FROM THE LEDGER (net of rollbacks), so a later
    terminal failure rolls the orphan promotion back to its pre-run pointer instead
    of leaving it current on a failed run.
    """
    conn, _, _ = env
    install_derived_protocol(conn)
    daily_chain.install_schema(conn)
    conn.commit()
    product = "bond_serving_v1"
    a1 = make_validated_publication(conn, product, 1)
    a2 = make_validated_publication(conn, product, 2)
    conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (product, a1))
    conn.commit()

    run_id = daily_chain.chain_run_id_for(daily_chain.DEFAULT_CHAIN, D1, REV, CFG)
    # Reconstruct the crashed run: run row 'running', the advancing stage
    # checkpointed 'succeeded', the product promoted for real + a ledger 'promote'
    # (previous=a1), and NO compensation — exactly the state a crash leaves behind.
    daily_chain._open_run(conn, run_id=run_id, chain=daily_chain.DEFAULT_CHAIN,
                          source_day=D1, code_revision=REV, config_version=CFG, watermarks={})
    daily_chain._record_stage(conn, run_id=run_id, stage="pit_update", order=0,
                              outcome=StageOutcome.succeeded(), attempts=1,
                              started_at=daily_chain._now())
    conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (product, a2))
    daily_chain.record_promotion(conn, product=product, publication_id=a2,
                                 previous_publication_id=a1, run_id=run_id, action="promote")
    conn.commit()
    assert daily_chain.current_pointer(conn, product) == a2

    def refresh_fail(ctx: StageContext) -> StageOutcome:
        return StageOutcome.failed("boom", classification="terminal")

    # Restart the SAME identity: pit_update honoured from its checkpoint (never
    # re-run, so its promotion is invisible to in-memory tracking), refresh fails.
    stages = [ok_stage("pit_update"), Stage("refresh", refresh_fail)]
    summaries = daily_chain.run_chain(
        conn, stages=stages, source_days=[D1], code_revision=REV, config_version=CFG,
        snapshot_pointers=_pointer_snapshot, restore_pointer=_pointer_restore,
    )
    s = summaries[0]
    assert s["run_id"] == str(run_id)
    assert s["status"] == "failed"
    # The orphan pre-crash promotion is rolled back to its pre-run pointer via the
    # ledger — without the ledger-derived set it would stay a2 on a failed run.
    assert daily_chain.current_pointer(conn, product) == a1
    comp = {c["product"]: c for c in s["compensations"]}
    assert comp[product]["restored_to"] == str(a1)
    assert comp[product]["rolled_back_from"] == str(a2)
    assert s["promoted"] == []
    # Rollback recorded; the orphan is now net-absent from the ledger set.
    assert daily_chain._load_run_promotions(conn, run_id) == {}


def test_e2e_fail_compensate_restart_repromote_success(env):
    """One end-to-end scenario with the REAL worker restore routing:

    run -> two products self-promote FOR REAL -> terminal failure -> compensation
    -> RESTART same run identity -> re-promote -> success. Coverage includes the
    prior-is-None restore path (a derived product with NO previous current -- T6a)
    and the mixed ``active_quant_publication_v1`` restore path (a product with a
    prior active pointer -- T6b), exercised through ``_restore_pointer``'s real
    derived-vs-mixed routing rather than a stand-in.
    """
    conn, schema, _ = env
    install_derived_protocol(conn)
    daily_chain.install_schema(conn)
    quant_pub.install_schema(conn)
    conn.commit()
    dsn = _search_path_dsn(schema)

    derived = "bond_serving_v1"          # derived product, NO prior pointer (T6a)
    d1 = make_validated_publication(conn, derived, 1)

    mixed = quant_pub.PRODUCT            # 'mixed_quant_v1', HAS a prior active (T6b)
    m0 = quant_pub.open_publication(conn, product=mixed, as_of=date(2024, 12, 31),
                                    code_revision="r0", config_version="v")
    quant_pub.mark_ready(conn, m0, {})
    quant_pub.promote(conn, mixed, m0)   # m0 is the prior active (current) pointer
    m1 = quant_pub.open_publication(conn, product=mixed, as_of=D1,
                                    code_revision="r1", config_version="v")
    quant_pub.mark_ready(conn, m1, {})
    conn.commit()
    assert quant_pub.active_publication_id(conn, mixed) == m0

    def promote_derived_stage(ctx: StageContext) -> StageOutcome:
        ctx.conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (derived, d1))
        return StageOutcome.succeeded(product=derived)

    def promote_mixed_stage(ctx: StageContext) -> StageOutcome:
        # A rebuild leaves the target 'ready' before the promote stage flips it active
        # (on restart the target was retired to 'superseded' by the prior promotion;
        # clearing activated_at keeps the publication lifecycle CHECK satisfied).
        ctx.conn.execute(
            "UPDATE quant_publication_v1 SET status='ready', activated_at=NULL "
            "WHERE publication_id=%s AND status <> 'active'", (m1,))
        quant_pub.promote(ctx.conn, mixed, m1)
        return StageOutcome.succeeded(product=mixed)

    fail = {"on": True}

    def refresh(ctx: StageContext) -> StageOutcome:
        if fail["on"]:
            return StageOutcome.failed("serving_coverage_below_threshold", classification="terminal")
        return StageOutcome.succeeded()

    stages = [Stage("pit_update", promote_derived_stage),
              Stage("promote", promote_mixed_stage),
              Stage("refresh", refresh)]
    kw = dict(source_days=[D1], code_revision=REV, config_version=CFG, dsn=dsn,
              snapshot_pointers=chain_worker._snapshot_pointers,
              restore_pointer=chain_worker._restore_pointer)

    # Phase 1: promote both for real, then a terminal failure -> compensate BOTH.
    first = daily_chain.run_chain(conn, stages=stages, **kw)[0]
    assert first["status"] == "failed"
    # T6a: prior-is-None restore cleared the derived pointer entirely.
    assert daily_chain.current_pointer(conn, derived) is None
    # T6b: the mixed active pointer is restored to the prior active publication.
    assert quant_pub.active_publication_id(conn, mixed) == m0
    comp = {c["product"]: c for c in first["compensations"]}
    assert comp[derived]["restored_to"] is None and comp[derived]["rolled_back_from"] == str(d1)
    assert comp[mixed]["restored_to"] == str(m0) and comp[mixed]["rolled_back_from"] == str(m1)
    assert first["promoted"] == []

    # Phase 2: restart the SAME identity; refresh now succeeds -> re-promote -> success.
    fail["on"] = False
    second = daily_chain.run_chain(conn, stages=stages, **kw)[0]
    assert second["run_id"] == first["run_id"]           # same deterministic identity
    assert second["status"] == "completed"
    assert daily_chain.current_pointer(conn, derived) == d1
    assert quant_pub.active_publication_id(conn, mixed) == m1
    promoted = {p["product"] for p in second["promoted"]}
    assert derived in promoted and mixed in promoted
    assert second["compensations"] == []


def test_poisoned_chain_connection_still_records_and_compensates(env):
    """Discriminant test for the chain-connection rollback.

    A stage that runs FAILING SQL on the chain's own connection aborts its
    transaction. The engine must roll it back so the failed-stage checkpoint write AND
    the compensation still run. Without the rollback in ``_run_stage`` the run crashes
    out of ``_record_stage`` with ``InFailedSqlTransaction`` — skipping the checkpoint
    AND the compensation and leaving the already-promoted product current. (RED is
    proven by reverting only that rollback: the run then raises / leaves ps at a2.)
    """
    conn, _, _ = env
    ps, _pp, a1, a2, _b1, _b2 = _seed_two_products(conn)

    def poison_then_fail(ctx: StageContext) -> StageOutcome:
        # Abort the chain connection's transaction, then let it propagate (terminal).
        ctx.conn.execute("SELECT 1 / 0")  # division_by_zero -> tx aborted + raises
        return StageOutcome.succeeded()  # unreachable

    stages = [_self_promote_stage("pit_update", ps, a2), Stage("refresh", poison_then_fail)]
    summaries = daily_chain.run_chain(
        conn, stages=stages, source_days=[D1], code_revision=REV, config_version=CFG,
        snapshot_pointers=_pointer_snapshot, restore_pointer=_pointer_restore,
    )
    s = summaries[0]
    assert s["status"] == "failed"
    # The failed-stage checkpoint write ran (the poisoned tx was rolled back first).
    stage_status = {r[0]: r[1] for r in conn.execute(
        "SELECT stage, status FROM bond_daily_chain_stage_runs WHERE run_id=%s",
        (UUID(s["run_id"]),)).fetchall()}
    assert stage_status.get("refresh") == "failed"
    # Compensation still ran: the advanced product is restored to its prior pointer.
    assert daily_chain.current_pointer(conn, ps) == a1
    comp = {c["product"]: c for c in s["compensations"]}
    assert comp[ps]["restored_to"] == str(a1)
    assert s["promoted"] == []


def test_restore_mixed_no_prior_active_drops_the_active_pointer(env):
    """The mixed restore branch when the product had NO prior active publication.

    ``_restore_mixed(None)`` drops the active row and returns the promoted publication
    to 'ready' (clearing activated_at so the lifecycle CHECK holds). Covers the second
    activated_at fix, which the e2e (always seeded with a prior m0) does not reach.
    """
    conn, schema, _ = env
    quant_pub.install_schema(conn)
    conn.commit()
    dsn = _search_path_dsn(schema)
    mixed = quant_pub.PRODUCT
    m1 = quant_pub.open_publication(conn, product=mixed, as_of=D1,
                                    code_revision="r1", config_version="v")
    quant_pub.mark_ready(conn, m1, {})
    conn.commit()
    assert quant_pub.active_publication_id(conn, mixed) is None   # NO prior active

    def promote_mixed_stage(ctx: StageContext) -> StageOutcome:
        quant_pub.promote(ctx.conn, mixed, m1)
        return StageOutcome.succeeded(product=mixed)

    def refresh_fail(ctx: StageContext) -> StageOutcome:
        return StageOutcome.failed("boom", classification="terminal")

    stages = [Stage("promote", promote_mixed_stage), Stage("refresh", refresh_fail)]
    s = daily_chain.run_chain(
        conn, stages=stages, source_days=[D1], code_revision=REV, config_version=CFG,
        dsn=dsn, snapshot_pointers=chain_worker._snapshot_pointers,
        restore_pointer=chain_worker._restore_pointer,
    )[0]
    assert s["status"] == "failed"
    # No-prior restore: active pointer dropped entirely, publication back to writable.
    assert quant_pub.active_publication_id(conn, mixed) is None
    comp = {c["product"]: c for c in s["compensations"]}
    assert comp[mixed]["restored_to"] is None and comp[mixed]["rolled_back_from"] == str(m1)
    assert s["compensation_failures"] == []
    status = conn.execute(
        "SELECT status FROM quant_publication_v1 WHERE publication_id=%s", (m1,)).fetchone()[0]
    assert status == "ready"


# --------------------------------------------------------------------------- #
# Real watermarks + freshness metric + distinct staleness alert
# --------------------------------------------------------------------------- #

def _wm_for(_conn, day: date) -> dict:
    # One source 2 days behind the processed day, one 15 days behind.
    return {
        "bond_price_observation": (day - timedelta(days=2)).isoformat(),
        "ncen_effective_filings": (day - timedelta(days=15)).isoformat(),
    }


def test_watermarks_recorded_and_freshness_below_threshold_no_alert(env):
    conn, _, _ = env
    day = date(2025, 3, 20)
    summaries = daily_chain.run_chain(
        conn, stages=_stages(["ingest", "probe"]), source_days=[day],
        code_revision=REV, config_version=CFG, watermarks_for=_wm_for,
        staleness_threshold_days=30,
    )
    s = summaries[0]
    # input_watermarks are populated on the run row AND in the summary.
    assert s["input_watermarks"] == _wm_for(None, day)
    row_wm = conn.execute(
        "SELECT input_watermarks FROM bond_daily_chain_runs WHERE run_id=%s",
        (UUID(s["run_id"]),),
    ).fetchone()[0]
    assert row_wm == _wm_for(None, day)
    # Freshness metric over the source watermarks; max lag 15 < 30 -> no staleness.
    assert s["freshness"]["lags_days"] == {"bond_price_observation": 2, "ncen_effective_filings": 15}
    assert s["freshness"]["max_lag_days"] == 15
    assert s["freshness"]["stale_sources"] == []
    assert s["staleness_alert"] is None


def test_freshness_above_threshold_raises_distinct_staleness_alert(env):
    conn, _, _ = env
    day = date(2025, 3, 21)  # distinct source-day -> distinct run identity
    summaries = daily_chain.run_chain(
        conn, stages=_stages(["ingest", "probe"]), source_days=[day],
        code_revision=REV, config_version=CFG, watermarks_for=_wm_for,
        staleness_threshold_days=10,
    )
    s = summaries[0]
    assert s["status"] == "completed"          # staleness never fails the run
    assert s["alert"] is None                   # the pipeline (failure/dark) alert
    assert s["freshness"]["stale_sources"] == ["ncen_effective_filings"]  # 15 >= 10
    # The staleness alert is a DISTINCT field, never the failure alert.
    assert "STALE INPUT" in (s["staleness_alert"] or "")
    assert "ncen_effective_filings" in s["staleness_alert"]


# --------------------------------------------------------------------------- #
# Deterministic run identity
# --------------------------------------------------------------------------- #

def test_run_identity_is_deterministic_uuid5():
    a = daily_chain.chain_run_id_for("bond_daily_publication", D1, REV, CFG)
    b = daily_chain.chain_run_id_for("bond_daily_publication", D1, REV, CFG)
    c = daily_chain.chain_run_id_for("bond_daily_publication", D2, REV, CFG)
    assert a == b and a != c
    assert a.version == 5


# --------------------------------------------------------------------------- #
# Wave 1, Task 3: bond_metrics joins the materialize stage worker set
# --------------------------------------------------------------------------- #

_SCHEMAS = Path(__file__).resolve().parents[1] / "schemas"


def test_materialize_stage_worker_set_includes_bond_metrics(monkeypatch):
    """The materialize stage invokes bond_metrics after the ncen/rr1 units."""
    invoked: list[str] = []

    def fake_worker(name):
        def _run(dsn, *, calc_date=None):
            invoked.append(name)
            return {"state": "no_source"}
        return _run

    monkeypatch.setattr(chain_worker, "_worker", fake_worker)
    ctx = StageContext(conn=None, dsn="unused", source_day=D1,
                       run_id=daily_chain.chain_run_id_for("c", D1, "r", "v"),
                       chain="c", code_revision="r", config_version="v")
    outcome = chain_worker.stage_materialize(ctx)
    assert invoked == ["ncen_derived_profiles", "rr1_derived_profiles", "bond_metrics"]
    # A fully dark worker set is still a REPORTED skip, never a silent success.
    assert outcome.status.value == "skipped" and outcome.reason == "dark_no_source"


def test_compose_stage_precedence_one_success_beats_dark_skips(monkeypatch):
    """Aggregation-precedence pin (PR #51 triage): with MIXED unit outcomes —
    dark skips plus one success — the STAGE is succeeded; dark units never mask
    a delivered publication. ``_compose`` already implements the correct
    precedence (a unit exception fails the whole stage via the engine; >=1
    success wins over skips; all-skip is a reported ``dark_no_source``), so
    this is a regression pin, not a fix: the observed CI 'skipped' came from
    EVERY unit going dark on the merge tree (see ``_seed_wave1_public_data``),
    not from the aggregation."""
    states = {
        "ncen_derived_profiles": {"state": "no_source"},
        "rr1_derived_profiles": {"state": "ok", "products": 2},
        "bond_metrics": {"state": "no_securities"},
    }

    def fake_worker(name):
        def _run(dsn, *, calc_date=None):
            return states[name]
        return _run

    monkeypatch.setattr(chain_worker, "_worker", fake_worker)
    ctx = StageContext(conn=None, dsn="unused", source_day=D1,
                       run_id=daily_chain.chain_run_id_for("c", D1, "r", "v"),
                       chain="c", code_revision="r", config_version="v")
    outcome = chain_worker.stage_materialize(ctx)
    assert outcome.status.value == "succeeded"
    units = outcome.detail["units"]
    assert units["unit_0"]["status"] == "skipped"
    assert units["unit_1"]["status"] == "succeeded"
    assert units["unit_2"]["status"] == "skipped"


def test_classify_recognises_the_protocol_current_success_state():
    """The materializer envelope ``{"state": "ok", **result}`` is overridden by
    the protocol's ``state='current'`` (self-promoted publication). Latent until
    Wave 1 (every earlier run was dark); a live run must classify it a SUCCESS,
    never an unrecognised terminal failure."""
    from src.bonds.daily_chain import StageStatus, classify_worker_result

    outcome = classify_worker_result(
        {"state": "current", "product": "bond_metric_v1", "rows": 4}
    )
    assert outcome.status is StageStatus.SUCCEEDED


def _seed_wave1_public_data() -> None:
    """Wave-1 fixture data in the PUBLIC schema (truncate-isolated).

    The manifests lifecycle triggers are SECURITY DEFINER with
    ``search_path = pg_catalog, public``, so the REAL provenance protocol lives
    in public (the Task-1 ingest suite's idiom: install + TRUNCATE). Seeds one
    fixed 10% semiannual bond (Fabozzi-style) with a clean trade price of 96.23
    landed for D1, one validated ``bond_price`` run+package pair through the
    REAL lifecycle (family-invisible to the ncen/rr1 materialize units,
    discovered by the bond lane's family-agnostic SELECT), and all four Wave-1
    metrics qualified. The SECURITY UNIVERSE is deliberately NOT published
    here: the chain's pit_update stage must publish it before materialize can
    serve metrics — proving the stage ordering causally.
    """
    import json
    from decimal import Decimal

    import psycopg

    from _bond_price_fixtures import price_input
    from src.bonds import price_observations
    from src.sec_regulatory import manifests

    with psycopg.connect(base_dsn()) as conn:
        manifests.install_schema(conn)
        # The ncen/rr1 materialize units' self-installing effective views need
        # the raw tables their INGEST stage installs in a real run (ingest is
        # not part of this focused stage subset).
        for ddl_name in (
            "sec_derived_publications.sql",
            "ncen_raw_v2.sql",
            "rr1_raw_v2.sql",
            "bond_security_v1.sql",
            "bond_price_observations_v1.sql",
            "bond_price_eligibility_v1.sql",
            "bond_source_qualification.sql",
            "bond_metric_v1.sql",
        ):
            conn.execute((_SCHEMAS / ddl_name).read_text(encoding="utf-8"))
        conn.execute(
            "TRUNCATE sec_source_package_transitions, sec_source_packages, "
            "sec_row_issues, sec_table_reconciliations, sec_source_files, "
            "sec_validated_raw_visibility, sec_run_transitions, sec_ingestion_runs, "
            "bond_security_observation, bond_price_observation, "
            "bond_source_qualification CASCADE"
        )
        sha = "ab" * 32
        run = manifests.create_or_resume_run(
            conn, source_family="bond_price", package_sha256=sha,
            parser_version="ingest_v1", source_quarter="2025Q1",
            package_relative_path="bond_price/source.parquet",
        )
        manifests.transition_run(conn, run_id=run.run_id,
                                 expected_state="discovered", target_state="loading")
        manifests.register_file(conn, run_id=run.run_id, relative_path="source.parquet",
                                sha256=sha, byte_size=1)
        manifests.validate_raw_run(conn, run_id=run.run_id)
        package = manifests.register_package_discovery(
            conn, source_family="bond_price", source_quarter="2025Q1",
            package_relative_path="bond_price/source.parquet", package_state="loaded",
            package_sha256=sha, run_id=run.run_id,
        )
        # The MERGED security master (origin/main post-fork) pins its debt-cohort
        # source to the EXACT current validated sec_nport_holdings_v2 publication
        # (worker _current_nport_source); without one it is honestly dark and the
        # whole bond lane cascades dark (the exact PR #51 CI failure). Seed one
        # current validated publication whose lineage is this fixture's validated
        # run/package. Its N-PORT loader is a no-op here (no matching
        # sec_nport_holdings_v2_current rows for this publication), so the
        # universe still comes from the directly landed observation below. On the
        # pre-merge tree (family-agnostic discovery) this row is inert.
        nport_pub = uuid4()
        conn.execute(
            "INSERT INTO sec_derived_publications "
            "(publication_id, product, publication_version, source_run_id, "
            " source_package_id, build_fingerprint) "
            "VALUES (%s, 'sec_nport_holdings_v2', 1, %s, %s, %s)",
            (nport_pub, run.run_id, package.package_id, "cd" * 32),
        )
        conn.execute("SELECT sec_validate_derived_publication(%s)", (nport_pub,))
        conn.execute(
            "SELECT sec_set_current_derived_publication('sec_nport_holdings_v2', %s)",
            (nport_pub,),
        )
        oid = uuid4()
        conn.execute(
            "INSERT INTO bond_security_observation "
            "(observation_id, as_of, observation_date, source_run_id, cusip9_input, coupon_type, "
            " coupon_rate, maturity_date, day_count, coupon_schedule, source_lineage) "
            "VALUES (%s,%s,%s,%s,'BNDFIX001','fixed',10.0,'2030-01-01','30/360 US',%s::jsonb,%s::jsonb)",
            (
                oid, D1, D1, run.run_id,
                json.dumps([{"date": "2025-07-01", "rate": 10.0},
                            {"date": "2026-01-01", "rate": 10.0}]),
                json.dumps({"engine": "fixture", "observation_id": str(oid)}),
            ),
        )
        price_observations.load_price_observations(
            conn,
            [price_input(observation_date=D1, cusip9="BNDFIX001", price=Decimal("96.23"),
                         price_type="trade", accrued_treatment="clean", ytm=0.076)],
            as_of=D1, source_run_id=run.run_id,
        )
        for metric in ("security_ytm", "security_ytw", "current_yield", "wal"):
            conn.execute(
                "INSERT INTO bond_source_qualification "
                "(metric_id, source_contract_ref, qualified_from, qualified_to) "
                "VALUES (%s, 'bond_price_source_v1@aaaaaaaaaaaa', now(), NULL)",
                (metric,),
            )
        conn.commit()


def test_chain_materialize_runs_bond_metrics_and_promotes_available_ytm():
    """Requirement 5: a chain run over fixture data yields a promoted
    bond_metric_v1 with >=1 available (recomputed) YTM row, produced by the
    materialize stage AFTER the pit_update security/price publications."""
    admin = admin_connect()
    book_schema = None
    try:
        _seed_wave1_public_data()
        book_schema = new_schema(admin)
        book = worker_conn(book_schema)
        # The chain's own conn sees the data lane too (in production both live
        # in the same schema); the ledger still lands in the isolated schema.
        book.execute(f'SET search_path TO "{book_schema}", public')
        book.commit()
        stages = chain_worker.build_stages(["pit_update", "materialize", "validate", "probe"])
        s = daily_chain.run_chain(
            book, stages=stages, source_days=[D1], code_revision="revw1",
            config_version=CFG, dsn=base_dsn(),
            snapshot_pointers=chain_worker._snapshot_pointers,
            restore_pointer=chain_worker._restore_pointer,
        )[0]
        book.close()

        assert s["status"] == "completed"
        by_stage = {st["stage"]: st for st in s["stages"]}
        assert by_stage["pit_update"]["status"] == "succeeded"
        assert by_stage["materialize"]["status"] == "succeeded"
        assert by_stage["validate"]["status"] == "succeeded"
        # Inside materialize the ncen/rr1 units ran dark (no source of their
        # family); the SUCCESS was carried by the bond_metrics unit.
        units = by_stage["materialize"]["detail"]["units"]
        assert units["unit_0"]["status"] == "skipped"
        assert units["unit_1"]["status"] == "skipped"
        assert units["unit_2"]["status"] == "succeeded"
        assert units["unit_2"]["product"] == "bond_metric_v1"

        promoted = {p["product"]: p for p in s["promoted"]}
        assert promoted["bond_metric_v1"]["stage"] == "materialize"
        assert "bond_security_v1" in promoted and "bond_price_observation_v1" in promoted

        rows = admin.execute(
            "SELECT metric_id, value, status FROM public.sec_current_bond_metric_v1"
        ).fetchall()
        available = {r[0]: float(r[1]) for r in rows if r[2] == "available"}
        assert "security_ytm" in available
        # Recomputed by the engine (Fabozzi ~11%), never the source's raw 0.076.
        assert abs(available["security_ytm"] - 0.11) < 1e-3
    finally:
        if book_schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{book_schema}" CASCADE')
        admin.close()


def test_later_stage_failure_compensates_bond_metric_v1_automatically():
    """Requirement 5 (compensation): the compensation set is table-driven
    (pointer-snapshot diff over sec_derived_current_pointers), so an injected
    later-stage terminal failure rolls the NEW product's pointer back with NO
    product-specific chain code."""
    admin = admin_connect()
    book_schema = None
    try:
        _seed_wave1_public_data()
        book_schema = new_schema(admin)
        book = worker_conn(book_schema)

        def failing_validate(ctx: StageContext) -> StageOutcome:
            return StageOutcome.failed("injected_terminal", classification="terminal")

        stages = chain_worker.build_stages(["pit_update", "materialize"]) + [
            Stage("validate", failing_validate)
        ]
        s = daily_chain.run_chain(
            book, stages=stages, source_days=[D1], code_revision="revw1c",
            config_version=CFG, dsn=base_dsn(),
            snapshot_pointers=chain_worker._snapshot_pointers,
            restore_pointer=chain_worker._restore_pointer,
        )[0]
        book.close()

        assert s["status"] == "failed"
        comp = {c["product"]: c for c in s["compensations"]}
        assert "bond_metric_v1" in comp
        assert comp["bond_metric_v1"]["stage"] == "materialize"
        assert comp["bond_metric_v1"]["restored_to"] is None  # first publication
        assert s["promoted"] == []
        pointer_rows = admin.execute(
            "SELECT count(*) FROM public.sec_derived_current_pointers "
            "WHERE product='bond_metric_v1'"
        ).fetchone()[0]
        assert pointer_rows == 0
    finally:
        if book_schema:
            admin.execute(f'DROP SCHEMA IF EXISTS "{book_schema}" CASCADE')
        admin.close()
