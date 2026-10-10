"""Lineage repair cohort driver and lane: batching, stops, second pass, lane guard.

No DB, no network. The database seam is a fake whose ``repair`` runs the REAL
``run_repair`` (its guards, exit codes and result shape) on a stub connection;
only the per-instrument ``_apply_instrument`` is scripted, and it charges one
request per bad row against the real per-batch budget
(``tests/test_nav_return_lineage_repair_lane_db.py`` runs everything for real).
"""

from __future__ import annotations

import datetime as dt
import json
import socket
import uuid
from types import SimpleNamespace

import psycopg
import pytest
from psycopg.pq import TransactionStatus

from scripts import repair_nav_return_lineage_cohort as driver
from src.workers import nav_return_lineage_repair as repair
from src.workers import nav_return_lineage_repair_lane as lane
from src.workers._tiingo import TiingoBudgetExceeded

SECRET_DSN = "postgresql://worker_writer:s3cr3t@db.internal:5432/market"
SUMMARY_KEYS = {
    "event", "driver", "operator_version", "status", "exit", "clean", "stop_code",
    "dry_run", "validate_only", "plans", "passes", "batches", "plan_sha256",
    "max_requests", "instruments_planned", "rows_planned", "eligible_instruments",
    "not_eligible_rows_by_reason", "submitted", "committed_instruments",
    "committed_rows", "validated_instruments", "validated_rows", "already_repaired",
    "skipped_instruments", "skipped_rows_by_code", "failed_instruments",
    "failed_by_code", "unknown_instruments", "remaining_instruments", "requests_used",
    "errors_by_code", "remaining_bad_rows", "readiness_republish_required", "elapsed_s",
}


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def refuse(*_args, **_kwargs):
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    for name in (
        "TIINGO_API_KEY", "DATABASE_URL", "WORKER_LIMIT", "WORKER_CALC_DATE",
        driver.DSN_ENV, driver.ALLOW_DATABASE_URL_ENV, lane.CONFIRM_ENV,
        *(lane.ENV_PREFIX + n for n in (
            "SCHEMA", "MAX_SECONDS", "RATE_PER_SECOND", "MAX_BATCHES", "DRY_RUN",
            "VALIDATE_ONLY")),
    ):
        monkeypatch.delenv(name, raising=False)


def _ids(n: int, offset: int = 0) -> list[str]:
    return [str(uuid.UUID(int=offset + i + 1)) for i in range(n)]


def _item(iid: str, rows: int = 1, reasons=()) -> dict:
    """A plan item shaped like ``build_plan``'s (only what the driver reads matters)."""
    return {
        "instrument_id": iid,
        "identity": {"ticker": "T" + iid[-4:], "is_active": True},
        "rows": [{"nav_date": f"d{n}", "source": "tiingo"} for n in range(rows)],
        "open_events": [],
        "revision_head": 1,
        "neighborhood_sha256": "0" * 64,
        "last_nav_date": "2026-10-09",
        "history_older_than_30_days": False,
        "reasons": list(reasons),
    }


class StubConn:
    """Just enough connection for run_repair's idle and clock-skew preflight;
    ``clock_error`` makes the clock query fail like a dropped connection."""

    closed = False
    info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)

    def __init__(self, clock_error=False):
        self.clock_error = clock_error

    def execute(self, query, params=None):
        assert query == "SELECT clock_timestamp()", query
        if self.clock_error:
            raise psycopg.OperationalError("server closed the connection " + SECRET_DSN)
        now = dt.datetime.now(dt.timezone.utc)
        return SimpleNamespace(fetchone=lambda: (now,))

    def rollback(self):
        pass


class FakeDatabase:
    """The driver's database seam; ``repair`` is the real operator entry point.

    ``check_errors[n]`` is raised by the n-th ``check`` call (0 = before the
    plan, 1 = before batch 1, ...); ``clock_errors`` repair calls fail in the
    operator's preflight before any instrument."""

    def __init__(self, items, *, planner=None, remaining=0, check_errors=None,
                 clock_errors=0):
        self.items, self.planner, self.remaining = items, planner, remaining
        self.check_errors = dict(check_errors or {})
        self.clock_errors = clock_errors
        self.plans: list[dict] = []
        self.applies: list[dict] = []
        self.checks: list[dict | None] = []
        self.closed = False

    @property
    def checked(self):
        return len(self.checks)

    def check(self, plan=None):
        self.checks.append(plan)
        error = self.check_errors.get(len(self.checks) - 1)
        if error is not None:
            raise error

    def plan(self, limits):
        items = self.planner(len(self.plans)) if self.planner else self.items
        plan = {"version": repair.VERSION, "schema": "public", "schema_pins": {},
                "as_of": "2026-10-09", "limits": limits.canonical(),
                "items": [dict(item) for item in items]}
        self.plans.append(plan)
        return plan

    def repair(self, plan, ids, sha, limits, client, validate_only):
        self.applies.append({"ids": list(ids), "sha": sha, "plan": len(self.plans) - 1,
                             "limits": limits.canonical()})
        conn = StubConn(clock_error=self.clock_errors > 0)
        self.clock_errors -= 1
        return repair.run_repair(conn, plan, instrument_ids=ids, supplied_sha256=sha,
                                 limits=limits, client=client, validate_only=validate_only)

    def count_bad(self):
        return self.remaining

    def close(self):
        self.closed = True


@pytest.fixture
def script(monkeypatch):
    """Scripted ``_apply_instrument``: ``behaviour[iid]`` is a list of actions,
    one per call (then the default: committed, or validated)."""
    behaviour: dict[str, list] = {}

    def apply_instrument(conn, approved, as_of, client, budget, validate_only):
        iid = approved["instrument_id"]
        rows = len(approved["rows"])
        # One request per bad row against the per-batch budget, as the real one.
        if budget.requests + rows > budget.limits.max_requests:
            raise repair.RepairError("REQUEST_BUDGET", 5)
        budget.requests += rows
        actions = behaviour.get(iid)
        if actions:
            return actions.pop(0)(approved)
        status = "validated" if validate_only else "committed"
        return {"instrument_id": iid, "status": status, "code": None, "rows": rows}

    monkeypatch.setattr(repair, "_apply_instrument", apply_instrument)
    return behaviour


def skipped(code):
    return lambda item: {"instrument_id": item["instrument_id"], "status": "skipped",
                         "code": code, "rows": len(item["rows"])}


def raises(exc):
    def action(_item):
        raise exc
    return action


def commit_unknown(item):
    return {"instrument_id": item["instrument_id"], "status": "unknown",
            "code": "COMMIT_UNKNOWN", "rows": len(item["rows"]),
            "run_ids": [str(uuid.UUID(int=99))]}


def provider_outage(item):
    return {"instrument_id": item["instrument_id"], "status": "stopped",
            "code": "PROVIDER_UNAVAILABLE", "rows": len(item["rows"])}


def _run(db, **config):
    lines: list[dict] = []
    clients: list = []

    def factory(limits):
        clients.append(limits)
        return SimpleNamespace(close=lambda: None)

    code, summary = driver.run_cohort(
        driver.CohortConfig(**config), database=db, client_factory=factory, emit=lines.append
    )
    json.dumps([lines, summary])  # every line is plain JSON
    return code, summary, lines, clients


# ── plan, batching and the request budget ───────────────────────────────────
def test_one_plan_then_allowlists_of_at_most_20_with_the_plan_limits(script):
    ids = _ids(45)
    ineligible = [_item(i, 3, ["MISSING_TICKER"]) for i in _ids(2, offset=100)]
    db = FakeDatabase([_item(i) for i in ids] + ineligible, remaining=6)
    code, summary, lines, clients = _run(db)
    assert code == 0
    assert [a["ids"] for a in db.applies] == [ids[:20], ids[20:40], ids[40:]]
    plan = db.plans[0]
    # The plan carries the run limits, and every apply uses that plan's hash.
    assert plan["limits"] == driver.CohortConfig().limits(20).canonical()
    assert {a["sha"] for a in db.applies} == {repair.sha256(plan)}
    assert {json.dumps(a["limits"], sort_keys=True) for a in db.applies} == {
        json.dumps(plan["limits"], sort_keys=True)}
    assert len(clients) == 1 and db.closed
    # Schema/access before the plan, then schema/access and pins before each batch.
    assert db.checks[0] is None and db.checks[1:] == [plan] * 3
    assert [line["event"] for line in lines] == ["plan", "batch", "batch", "batch"]
    assert (lines[0]["to_apply"], lines[0]["max_requests"]) == (45, 20)
    assert set(summary) == SUMMARY_KEYS
    assert (summary["status"], summary["clean"], summary["passes"]) == ("completed", True, 1)
    assert (summary["instruments_planned"], summary["eligible_instruments"]) == (47, 45)
    assert summary["not_eligible_rows_by_reason"] == {"MISSING_TICKER": 6}
    assert (summary["committed_instruments"], summary["committed_rows"]) == (45, 45)
    assert (summary["requests_used"], summary["remaining_instruments"]) == (45, 0)
    assert summary["remaining_bad_rows"] == 6
    assert summary["readiness_republish_required"] is False


def test_max_requests_is_the_largest_item_and_allowlists_fit_the_batch_budget(script):
    ones, big, twos = _ids(10), _ids(1, offset=10), _ids(25, offset=11)
    db = FakeDatabase([_item(i) for i in ones] + [_item(big[0], 30)]
                      + [_item(i, 2) for i in twos])
    code, summary, lines, _ = _run(db)
    # The fake charges one request per row against the real per-batch budget:
    # packing by count alone would stop with REQUEST_BUDGET here.
    assert code == 0 and summary["stop_code"] is None
    assert lines[0]["max_requests"] == 30
    assert [a["ids"] for a in db.applies] == [ones, big, twos[:15], twos[15:]]
    assert {a["limits"]["max_requests"] for a in db.applies} == {30}
    assert summary["committed_rows"] == summary["requests_used"] == 10 + 30 + 50


def test_slow_rate_packs_by_the_time_budget_and_oversized_items_are_residuals(script):
    ids = _ids(25)
    db = FakeDatabase([_item(i) for i in ids])
    code, _, lines, _ = _run(db, rate_per_second=0.05, max_seconds=400)
    assert code == 0 and lines[0]["rows_per_batch"] == 10  # 400 s * 0.05/s * 0.5
    assert [len(a["ids"]) for a in db.applies] == [10, 10, 5]
    huge = _ids(1, offset=500)[0]
    db = FakeDatabase([_item(i) for i in ids[:3]] + [_item(huge, driver.MAX_REQUESTS + 1)])
    code, summary, lines, _ = _run(db)
    assert [a["ids"] for a in db.applies] == [ids[:3]]  # never submitted
    assert (lines[0]["over_request_cap"], lines[0]["max_requests"]) == (1, 20)
    assert summary["skipped_rows_by_code"] == {driver.OVERSIZED: driver.MAX_REQUESTS + 1}
    assert (code, summary["status"], summary["clean"]) == (2, "completed", False)


def test_canary_caps_and_dry_run(script, monkeypatch):
    ids = _ids(45)
    capped = FakeDatabase([_item(i) for i in ids])
    code, summary, _, _ = _run(capped, max_instruments_total=5)
    assert [a["ids"] for a in capped.applies] == [ids[:5]]
    assert (code, summary["status"], summary["submitted"]) == (0, "limit_reached", 5)
    assert summary["remaining_instruments"] == 40
    one = FakeDatabase([_item(i) for i in ids])
    code, summary, _, _ = _run(one, max_batches=1)
    assert (code, summary["status"], summary["committed_instruments"]) == (0, "limit_reached", 20)
    monkeypatch.setattr(repair, "ProviderClient", lambda *_: pytest.fail("no provider"))
    dry = FakeDatabase([_item(i) for i in ids])
    lines: list[dict] = []
    code, summary = driver.run_cohort(  # no key, no client factory: still fine
        driver.CohortConfig(dry_run=True), database=dry, emit=lines.append)
    assert (code, summary["status"], dry.applies) == (0, "planned", [])
    assert [line["event"] for line in lines] == ["plan"]
    assert (summary["eligible_instruments"], summary["remaining_instruments"]) == (45, 45)


def test_validate_only_validates_and_writes_nothing(script):
    db = FakeDatabase([_item(i, 2) for i in _ids(3)])
    code, summary, _, _ = _run(db, validate_only=True)
    assert code == 0 and summary["validate_only"] is True
    assert (summary["validated_instruments"], summary["validated_rows"]) == (3, 6)
    assert summary["committed_instruments"] == 0


def test_missing_tiingo_key_is_refused_before_any_database_work():
    db = FakeDatabase([_item(i) for i in _ids(3)])
    code, summary = driver.run_cohort(driver.CohortConfig(), database=db, emit=lambda _: None)
    assert (code, summary["stop_code"], summary["status"]) == (2, "PROVIDER_NOT_CONFIGURED", "stopped")
    assert (db.checked, db.plans, summary["batches"]) == (0, [], 0)


# ── stop conditions ─────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("position", "action", "exit_expected", "stop_code", "committed"),
    [
        (0, raises(repair.RepairError("TIME_BUDGET", 5)), 5, "TIME_BUDGET", 20),
        (2, provider_outage, 5, "PROVIDER_UNAVAILABLE", 22),
        (1, raises(TiingoBudgetExceeded("429 budget")), 5, "PROVIDER_BUDGET", 21),
        (0, raises(KeyboardInterrupt()), 5, "INTERRUPTED", 20),
        # A lock stop after this run already wrote is exit 5, not 4.
        (0, raises(repair.RepairError("LOCK_BUSY", 4)), 5, "LOCK_BUSY", 20),
        (0, commit_unknown, 2, "COMMIT_UNKNOWN", 20),
        (3, raises(repair.RepairError("CLOCK_SKEW")), 2, "CLOCK_SKEW", 23),
        (0, raises(psycopg.errors.lookup("42501")()), 3, "SCHEMA_ACCESS_INCOMPATIBLE", 20),
    ],
)
def test_stops_end_the_run_at_once_and_report_what_remains(
    script, position, action, exit_expected, stop_code, committed
):
    ids = _ids(45)
    script[ids[20 + position]] = [action]
    db = FakeDatabase([_item(i) for i in ids])
    code, summary, lines, _ = _run(db)
    assert (code, summary["status"], summary["stop_code"]) == (exit_expected, "stopped", stop_code)
    assert len(db.applies) == 2  # nothing after the stopping batch
    assert summary["committed_instruments"] == committed
    unknown = 1 if stop_code == "COMMIT_UNKNOWN" else 0
    assert summary["unknown_instruments"] == unknown
    assert summary["remaining_instruments"] == 45 - committed - unknown
    assert lines[-1]["event"] == "batch" and lines[-1]["exit"] == (
        4 if stop_code == "LOCK_BUSY" and position == 0 else
        2 if stop_code in ("COMMIT_UNKNOWN", "CLOCK_SKEW") else exit_expected)
    if unknown:  # the batch line carries the reconciliation handle, the summary never
        assert lines[-1]["commit_unknown"] == {
            "instrument_id": ids[20], "run_ids": [str(uuid.UUID(int=99))]}
    assert "commit_unknown" not in summary and "run_ids" not in json.dumps(summary)


def test_lock_busy_before_any_commit_keeps_exit_4(script):
    ids = _ids(3)
    script[ids[0]] = [raises(repair.RepairError("LOCK_BUSY", 4))]
    code, summary, _, _ = _run(FakeDatabase([_item(i) for i in ids]))
    assert (code, summary["stop_code"], summary["remaining_instruments"]) == (4, "LOCK_BUSY", 3)


def test_a_batch_that_fails_before_any_instrument_is_retried_whole(script):
    ids = _ids(45)
    db = FakeDatabase([_item(i) for i in ids], clock_errors=1)
    code, summary, lines, _ = _run(db)
    # The operator's clock query failed: nobody to blame, the same chunk again.
    assert lines[1]["code"] == "DATABASE_ERROR" and lines[1]["exit"] == 2
    assert [a["ids"] for a in db.applies] == [ids[:20], ids[:20], ids[20:40], ids[40:]]
    assert (code, summary["status"], summary["clean"]) == (0, "completed", True)
    assert (summary["committed_instruments"], summary["failed_instruments"]) == (45, 0)
    assert "s3cr3t" not in json.dumps([lines, summary])


def test_an_unreachable_database_at_the_batch_check_is_retried(script):
    ids = _ids(25)
    db = FakeDatabase([_item(i) for i in ids],
                      check_errors={1: psycopg.OperationalError("down " + SECRET_DSN)})
    code, summary, lines, _ = _run(db)
    assert [a["ids"] for a in db.applies] == [ids[:20], ids[20:]]
    assert lines[1]["code"] == "DATABASE_ERROR" and summary["batches"] == 3
    assert (code, summary["committed_instruments"]) == (0, 25)


def test_three_failed_batches_in_a_row_stop_without_blaming_an_instrument(script):
    ids = _ids(45)
    db = FakeDatabase([_item(i) for i in ids], clock_errors=3)
    code, summary, _, _ = _run(db)
    assert (code, summary["stop_code"], len(db.applies)) == (2, "CONSECUTIVE_FAILED_BATCHES", 3)
    assert {tuple(a["ids"]) for a in db.applies} == {tuple(ids[:20])}
    assert (summary["failed_instruments"], summary["remaining_instruments"]) == (0, 45)


def test_an_instrument_the_operator_fails_on_is_retried_once_then_recorded(script):
    ids = _ids(45)
    flaky, broken = ids[3], ids[30]
    script[flaky] = [raises(psycopg.OperationalError("connection reset"))]
    script[broken] = [raises(repair.RepairError("UNEXPECTED_NAV_CHANGE"))] * 2
    db = FakeDatabase([_item(i) for i in ids])
    code, summary, _, _ = _run(db)
    sent = [i for a in db.applies for i in a["ids"]]
    assert (sent.count(flaky), sent.count(broken)) == (2, 2)
    # Each failing batch puts the culprit and everything untried back in front.
    assert [a["ids"][0] for a in db.applies] == [ids[0], flaky, ids[23], broken, ids[31]]
    assert summary["failed_by_code"] == {"UNEXPECTED_NAV_CHANGE": 1}
    assert (summary["committed_instruments"], summary["remaining_instruments"]) == (44, 0)
    assert (code, summary["status"], summary["stop_code"]) == (2, "completed", None)


@pytest.mark.parametrize("error", [
    repair.RepairError("SCHEMA_ACCESS_INCOMPATIBLE", 3),  # not ready, or pins moved
    psycopg.errors.lookup("42501")(),  # access revoked under the check
])
def test_schema_or_access_change_between_batches_stops_with_exit_3(script, error):
    ids = _ids(45)
    db = FakeDatabase([_item(i) for i in ids], check_errors={2: error})
    code, summary, lines, _ = _run(db)
    assert (code, summary["status"], summary["stop_code"]) == (
        3, "stopped", "SCHEMA_ACCESS_INCOMPATIBLE")
    assert [a["ids"] for a in db.applies] == [ids[:20]]  # earlier commits stay
    assert (summary["committed_instruments"], summary["remaining_instruments"]) == (20, 25)
    assert summary["batches"] == 1  # the refused batch never ran
    assert [line["event"] for line in lines] == ["plan", "batch"]


def test_operator_contract_errors_stop_at_once(script, monkeypatch):
    real = repair.run_repair

    def tampered(conn, plan, **kwargs):
        return real(conn, plan, **{**kwargs, "supplied_sha256": "0" * 64})

    monkeypatch.setattr(repair, "run_repair", tampered)
    db = FakeDatabase([_item(i) for i in _ids(45)])
    code, summary, _, _ = _run(db)
    assert (code, summary["stop_code"], len(db.applies)) == (2, "PLAN_DIGEST_MISMATCH", 1)


# ── second pass ─────────────────────────────────────────────────────────────
def test_plan_stale_skips_are_replanned_once_and_only_they_are_retried(script):
    ids = _ids(30)
    stale = [ids[3], ids[25]]
    for iid in stale:
        script[iid] = [skipped("PLAN_STALE")]

    def planner(index):
        if index == 0:
            return [_item(i) for i in ids]
        # Still bad: the stale two, and an unrelated eligible newcomer.
        return [_item(i) for i in [*stale, _ids(1, offset=900)[0]]]

    db = FakeDatabase([], planner=planner)
    code, summary, lines, _ = _run(db)
    assert len(db.plans) == 2
    assert [a["ids"] for a in db.applies if a["plan"] == 1] == [stale]
    assert db.applies[-1]["sha"] == repair.sha256(db.plans[1])
    assert [line["pass"] for line in lines if line["event"] == "plan"] == [1, 2]
    assert (code, summary["status"], summary["clean"]) == (0, "completed", True)
    assert (summary["plans"], summary["passes"], summary["committed_instruments"]) == (2, 2, 30)
    assert summary["skipped_rows_by_code"] == {}


def test_replan_reconciles_stale_items_no_longer_bad_and_keeps_new_reasons(script):
    ids = _ids(10)
    gone, held, retried = ids[2], ids[5], ids[8]

    def planner(index):
        if index == 0:
            return [_item(i) for i in ids]
        # gone: repaired concurrently; held: now under a reexpression hold.
        return [_item(held, 2, ["OPEN_REEXPRESSION"]), _item(retried)]

    for iid in (gone, held, retried):
        script[iid] = [skipped("PLAN_STALE")]
    db = FakeDatabase([], planner=planner)
    code, summary, _, _ = _run(db)
    assert [a["ids"] for a in db.applies if a["plan"] == 1] == [[retried]]
    assert (summary["already_repaired"], summary["committed_instruments"]) == (1, 8)
    assert summary["skipped_rows_by_code"] == {"OPEN_REEXPRESSION": 2}
    assert (code, summary["status"], summary["clean"]) == (2, "completed", False)
    # Only the concurrently repaired one: a clean run.
    script[gone] = [skipped("PLAN_STALE")]
    db = FakeDatabase([], planner=lambda index: [_item(i) for i in ids] if index == 0 else [])
    code, summary, _, _ = _run(db)
    assert (len(db.plans), summary["already_repaired"], summary["skipped_rows_by_code"]) == (
        2, 1, {})
    assert (code, summary["status"], summary["clean"]) == (0, "completed", True)


def test_still_stale_after_the_replan_is_a_residual_and_no_third_plan(script):
    ids = _ids(10)
    script[ids[4]] = [skipped("PLAN_STALE"), skipped("PLAN_STALE")]
    db = FakeDatabase([_item(i) for i in ids])
    code, summary, _, _ = _run(db)
    assert (len(db.plans), summary["passes"]) == (2, 2)
    assert summary["skipped_rows_by_code"] == {"PLAN_STALE": 1}
    assert (code, summary["status"], summary["clean"]) == (2, "completed", False)


def test_a_pass_that_commits_nothing_is_not_replanned(script):
    ids = _ids(3)
    for iid in ids:
        script[iid] = [skipped("PLAN_STALE")]
    db = FakeDatabase([_item(i) for i in ids])
    code, summary, _, _ = _run(db)
    assert (len(db.plans), summary["passes"], code) == (1, 1, 2)
    assert summary["skipped_rows_by_code"] == {"PLAN_STALE": 3}


# ── run_worker lane ─────────────────────────────────────────────────────────
def _run_worker(monkeypatch, capsys, env):
    import src.run_worker as rw

    monkeypatch.setenv("WORKER", "nav_return_lineage_repair_lane")
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(rw, "resolve_dsn", lambda: SECRET_DSN)
    try:
        rw.main()
        code = 0
    except SystemExit as exc:
        code = exc.code
    out = capsys.readouterr().out
    return code, json.loads(out.splitlines()[-1]) if out.strip() else None


def test_lane_refuses_without_confirmation_and_never_touches_the_driver(monkeypatch, capsys):
    monkeypatch.setattr(driver, "run_cohort", lambda *a, **k: pytest.fail("driver must not run"))
    for env in ({}, {lane.CONFIRM_ENV: "yes"}, {lane.CONFIRM_ENV: "nav_return_lineage_repair_v0"}):
        monkeypatch.delenv(lane.CONFIRM_ENV, raising=False)
        code, stats = _run_worker(monkeypatch, capsys, env)
        assert code == 1
        assert (stats["state"], stats["code"]) == ("blocked", "CONFIRMATION_REQUIRED")
        assert "s3cr3t" not in json.dumps(stats)


def test_lane_without_tiingo_key_is_blocked_before_connecting(monkeypatch, capsys):
    code, stats = _run_worker(monkeypatch, capsys, {
        lane.CONFIRM_ENV: lane.CONFIRM_VALUE, driver.ALLOW_DATABASE_URL_ENV: "1"})
    assert code == 1
    assert (stats["state"], stats["stop_code"], stats["batches"]) == (
        "blocked", "PROVIDER_NOT_CONFIGURED", 0)
    assert "s3cr3t" not in json.dumps(stats)


def test_lane_maps_env_dsn_and_state(monkeypatch, capsys):
    calls: list[dict] = []
    outcome = {"value": (0, "limit_reached", 1)}

    def fake_run_cohort(config, *, dsn=None, **_kwargs):
        calls.append({"config": config, "dsn": dsn})
        exit_status, status, batches = outcome["value"]
        return exit_status, {"event": "summary", "status": status, "exit": exit_status,
                             "batches": batches, "dry_run": config.dry_run}

    monkeypatch.setattr(driver, "run_cohort", fake_run_cohort)
    env = {
        lane.CONFIRM_ENV: lane.CONFIRM_VALUE,
        "WORKER_LIMIT": "20",
        "NAV_LINEAGE_REPAIR_MAX_BATCHES": "1",
        "NAV_LINEAGE_REPAIR_RATE_PER_SECOND": "2.5",
        "NAV_LINEAGE_REPAIR_MAX_SECONDS": "300",
        "NAV_LINEAGE_REPAIR_VALIDATE_ONLY": "1",
    }
    code, stats = _run_worker(monkeypatch, capsys, env)
    assert (code, stats["state"], stats["worker"]) == (0, "complete", "nav_return_lineage_repair_lane")
    config = calls[-1]["config"]
    assert (config.max_instruments_total, config.max_batches, config.rate_per_second,
            config.max_seconds, config.schema, config.validate_only, config.dry_run) == (
        20, 1, 2.5, 300.0, "public", True, False)
    assert calls[-1]["dsn"] is None  # DATABASE_URL fallback not allowed
    monkeypatch.setenv(driver.ALLOW_DATABASE_URL_ENV, "1")
    _run_worker(monkeypatch, capsys, env)
    assert calls[-1]["dsn"] == SECRET_DSN
    monkeypatch.setenv(driver.DSN_ENV, "postgresql://explicit")
    _run_worker(monkeypatch, capsys, env)
    assert calls[-1]["dsn"] == "postgresql://explicit"  # the explicit DSN wins
    for value, state, aborted in (
        ((5, "stopped", 3), "failed", True),
        ((2, "completed", 3), "failed", False),  # residuals: never green
        ((3, "stopped", 0), "blocked", True),
    ):
        outcome["value"] = value
        code, stats = _run_worker(monkeypatch, capsys, env)
        assert (code, stats["state"], stats["aborted"]) == (1, state, aborted)


@pytest.mark.parametrize(
    ("env", "code"),
    [
        ({"NAV_LINEAGE_REPAIR_RATE_PER_SECOND": "3"}, "RATE_INVALID"),
        ({"NAV_LINEAGE_REPAIR_MAX_BATCHES": "one"}, "CONFIG_INVALID"),
        ({"NAV_LINEAGE_REPAIR_MAX_BATCHES": "0"}, "MAX_BATCHES_INVALID"),
        ({"NAV_LINEAGE_REPAIR_SCHEMA": "public; drop"}, "SCHEMA_INVALID"),
        ({"WORKER_LIMIT": "0"}, None),  # run_worker itself refuses a zero cap
    ],
)
def test_lane_rejects_bad_configuration_without_running(monkeypatch, capsys, env, code):
    monkeypatch.setattr(driver, "run_cohort", lambda *a, **k: pytest.fail("driver must not run"))
    exit_status, stats = _run_worker(
        monkeypatch, capsys, {lane.CONFIRM_ENV: lane.CONFIRM_VALUE, **env})
    assert exit_status not in (0, None)
    if code is not None:
        assert (stats["state"], stats["code"]) == ("blocked", code)


def test_cli_needs_the_operator_confirm_token_unless_dry_run(monkeypatch, capsys):
    monkeypatch.setattr(driver, "run_cohort", lambda *a, **k: pytest.fail("driver must not run"))
    assert driver.main([]) == 2
    out = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert (out["stop_code"], out["batches"]) == ("CONFIRMATION_REQUIRED", 0)
