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
from datetime import date
from pathlib import Path
from uuid import UUID

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _daily_chain_fixtures import (  # noqa: E402
    admin_connect,
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
# Deterministic run identity
# --------------------------------------------------------------------------- #

def test_run_identity_is_deterministic_uuid5():
    a = daily_chain.chain_run_id_for("bond_daily_publication", D1, REV, CFG)
    b = daily_chain.chain_run_id_for("bond_daily_publication", D1, REV, CFG)
    c = daily_chain.chain_run_id_for("bond_daily_publication", D2, REV, CFG)
    assert a == b and a != c
    assert a.version == 5
