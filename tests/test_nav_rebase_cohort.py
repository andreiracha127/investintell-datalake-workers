"""Cohort driver for the governed NAV rebase: loop, re-plan, stops, lane guard.

No DB, no network. The operator is replaced by a scripted fake that parses
every argv with the operator's OWN parser and answers with results built by
the library's own ``_finish``/``exit_code``, so codes, statuses and exits are
the real contract (``tests/test_nav_rebase_cohort_db.py`` runs the real
operator end to end on PostgreSQL).
"""

from __future__ import annotations

import hashlib
import json
import signal
import socket
import uuid
from pathlib import Path

import pytest

from scripts import rebase_fund_nav_cohort as driver
from scripts import rebase_fund_nav_window as cli
from src.workers import nav_economic_rebase as rebase
from src.workers import nav_rebase_cohort as lane

SECRET_DSN = "postgresql://worker_writer:s3cr3t@db.internal:5432/market"


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def refuse(*_args, **_kwargs):
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    for name in (
        "TIINGO_API_KEY",
        "DATABASE_URL",
        driver.DSN_ENV,
        driver.ALLOW_DATABASE_URL_ENV,
        lane.CONFIRM_ENV,
        "NAV_REBASE_MAX_BATCHES",
        "NAV_REBASE_RATE_PER_SECOND",
        "NAV_REBASE_MAX_SECONDS",
        "NAV_REBASE_SCHEMA",
        "NAV_REBASE_DRY_RUN",
        "WORKER_LIMIT",
        "WORKER_CALC_DATE",
    ):
        monkeypatch.delenv(name, raising=False)


def _ids(n: int, offset: int = 0) -> list[str]:
    return [str(uuid.UUID(int=offset + i + 1)) for i in range(n)]


def _outcome(iid, status, code=None):
    return rebase.InstrumentRebaseResult(iid, status, code)


def _result(sha, outcomes, *, status=None, code=None, stop=None, retryable=None):
    """A result exactly as ``run_rebase`` shapes it."""
    result = rebase._empty_result(sha)
    result["planned"] = len(outcomes)
    result["requests_used"] = sum(
        1 for o in outcomes if o.status not in ("not_attempted", "already_applied")
    )
    if result["requests_used"]:
        result["run_id"] = str(uuid.uuid4())
    done = rebase._finish(
        result,
        status=status,
        code=code,
        retryable=retryable,
        outcomes={o.instrument_id: o for o in outcomes},
        stop_code=stop,
    )
    return rebase.exit_code(done), done


def committed(sha, ids):
    return _result(sha, [_outcome(i, "committed") for i in ids])


def preflight_stale(sha, ids):
    return _result(
        sha,
        [_outcome(i, "not_attempted", "PLAN_STALE") for i in ids],
        status="blocked",
        code="PLAN_STALE",
    )


class FakeOperator:
    """Scripted ``rebase_fund_nav_window`` (argv -> (exit, result)).

    ``planner(scope, plan_index) -> (planned_ids, excluded_pairs)`` and
    ``applier(sha, ids, batch_no) -> (exit, result)``; plans are written like
    the operator (new file, canonical JSON, reported SHA256 == file SHA256).
    """

    def __init__(self, cohort, *, excluded=(), applier=None, planner=None, plan_exit=None):
        self.cohort = list(cohort)
        self.excluded = [list(pair) for pair in excluded]
        self.applier = applier or (lambda sha, ids, n: committed(sha, ids))
        self.planner = planner
        self.plan_exit = plan_exit
        self.plans: list[dict] = []
        self.applies: list[dict] = []
        self.budgets: set[tuple] = set()

    def __call__(self, argv):
        args = cli._parser().parse_args(argv)  # the operator's own parser
        assert args.contract == rebase.CONTRACT_VERSION
        self.budgets.add(
            (
                args.batch_size,
                args.max_instruments,
                args.max_requests,
                args.max_seconds,
                args.rate_per_second,
            )
        )
        if args.mode == "plan":
            return self._plan(args)
        assert len(args.instrument_id) <= rebase.MAX_BATCH
        plan_bytes = Path(args.plan_file).read_bytes()
        assert hashlib.sha256(plan_bytes).hexdigest() == args.plan_sha256
        planned = {i["instrument_id"] for i in json.loads(plan_bytes)["instruments"]}
        assert set(args.instrument_id) <= planned
        self.applies.append(
            {"plan_file": args.plan_file, "sha": args.plan_sha256, "ids": args.instrument_id}
        )
        return self.applier(args.plan_sha256, list(args.instrument_id), len(self.applies))

    def _plan(self, args):
        scope = list(args.instrument_id) or None
        if self.plan_exit is not None:
            return self.plan_exit
        if self.planner is not None:
            planned, excluded = self.planner(scope, len(self.plans))
        elif scope is None:
            planned, excluded = self.cohort, self.excluded
        else:
            planned, excluded = scope, []
        manifest = {
            "plan_version": rebase.PLAN_VERSION,
            "contract_version": rebase.CONTRACT_VERSION,
            "limits": {
                "batch_size": args.batch_size,
                "max_instruments": args.max_instruments,
                "max_requests": args.max_requests,
                "max_seconds": args.max_seconds,
                "rate_per_second": args.rate_per_second,
            },
            "scope": scope,
            "plan_index": len(self.plans),  # distinct bytes per plan
            "instruments": [
                {"instrument_id": i, "needs": ["LINEAGE_MISSING"]} for i in planned
            ],
            "excluded": [list(pair) for pair in excluded],
        }
        data = json.dumps(manifest, sort_keys=True).encode()
        with open(args.plan_file, "xb") as handle:  # never overwrite, like the operator
            handle.write(data)
        sha = hashlib.sha256(data).hexdigest()
        self.plans.append({"scope": scope, "sha": sha, "file": args.plan_file})
        return 0, {
            "status": "planned",
            "code": None,
            "plan_sha256": sha,
            "planned": len(planned),
            "instruments": [{"instrument_id": i} for i in planned],
        }


def _run(fake, tmp_path, **config):
    lines: list[dict] = []
    code, summary = driver.run_cohort(
        driver.CohortConfig(work_dir=str(tmp_path), **config),
        invoke=fake,
        emit=lines.append,
    )
    json.dumps([lines, summary])  # every line is plain JSON
    return code, summary, lines


# ── loop ─────────────────────────────────────────────────────────────────────
def test_one_plan_then_allowlists_of_at_most_20_with_the_pinned_plan_and_budgets(
    tmp_path,
):
    ids = _ids(45)
    excluded = [[str(uuid.uuid4()), "FUND_NOT_ACTIVE"]] * 2 + [
        [str(uuid.uuid4()), "NAV_STALE"]
    ]
    fake = FakeOperator(ids, excluded=excluded)
    code, summary, lines = _run(fake, tmp_path)
    assert code == 0
    assert [len(a["ids"]) for a in fake.applies] == [20, 20, 5]
    assert [i for a in fake.applies for i in a["ids"]] == ids  # plan order, no repeats
    assert {a["sha"] for a in fake.applies} == {fake.plans[0]["sha"]}
    assert len(fake.plans) == 1 and fake.plans[0]["scope"] is None
    # One budget tuple for the plan and every apply (PLAN_LIMITS_MISMATCH otherwise).
    assert fake.budgets == {(20, 20, 20, driver.DEFAULT_MAX_SECONDS, 2.5)}
    assert [line["event"] for line in lines] == ["plan", "batch", "batch", "batch"]
    assert lines[0]["excluded_counts"] == {"FUND_NOT_ACTIVE": 2, "NAV_STALE": 1}
    assert lines[0]["needs_counts"] == {"LINEAGE_MISSING": 45}
    assert summary["status"] == "completed" and summary["clean"] is True
    assert (summary["planned_initial"], summary["committed"], summary["batches"]) == (
        45,
        45,
        3,
    )
    assert summary["not_planned_by_code"] == {"FUND_NOT_ACTIVE": 2, "NAV_STALE": 1}
    assert summary["requests_used"] == 45 and summary["remaining"] == 0
    assert summary["readiness_republish_required"] is True


def test_lower_rate_and_batch_size_are_pinned_identically_everywhere(tmp_path):
    fake = FakeOperator(_ids(7))
    code, summary, _ = _run(fake, tmp_path, batch_size=3, rate_per_second=0.5)
    assert code == 0 and summary["committed"] == 7
    assert [len(a["ids"]) for a in fake.applies] == [3, 3, 1]
    assert fake.budgets == {(3, 3, 3, driver.DEFAULT_MAX_SECONDS, 0.5)}


# ── stale plans ─────────────────────────────────────────────────────────────
def test_batch_level_plan_stale_replans_exactly_the_remaining_and_continues(tmp_path):
    ids = _ids(45)

    def applier(sha, chunk, n):
        return preflight_stale(sha, chunk) if n == 2 else committed(sha, chunk)

    fake = FakeOperator(ids, applier=applier)
    code, summary, lines = _run(fake, tmp_path)
    assert code == 0 and summary["status"] == "completed"
    assert [p["scope"] for p in fake.plans] == [None, ids[20:]]
    # After the re-plan every apply uses the NEW plan file and hash.
    assert [a["sha"] for a in fake.applies] == [fake.plans[0]["sha"]] * 2 + [
        fake.plans[1]["sha"]
    ] * 2
    assert [a["ids"] for a in fake.applies[2:]] == [ids[20:40], ids[40:]]
    assert (summary["committed"], summary["replans"], summary["plans"]) == (45, 1, 2)
    assert [line["event"] for line in lines] == [
        "plan",
        "batch",
        "batch",
        "plan",
        "batch",
        "batch",
    ]
    assert lines[2]["code"] == "PLAN_STALE" and lines[2]["exit"] == 2


def test_instrument_plan_stale_is_replanned_then_recorded_failed_at_the_cap(tmp_path):
    ids = _ids(5)
    moving = ids[1]

    def applier(sha, chunk, n):
        return _result(
            sha,
            [
                _outcome(i, "failed", "PLAN_STALE")
                if i == moving
                else _outcome(i, "committed")
                for i in chunk
            ],
        )

    fake = FakeOperator(ids, applier=applier)
    code, summary, _ = _run(fake, tmp_path)
    cap = driver.MAX_STALE_REPLANS_PER_INSTRUMENT
    assert [p["scope"] for p in fake.plans] == [None] + [[moving]] * cap
    assert [a["ids"] for a in fake.applies] == [ids] + [[moving]] * cap
    assert (summary["committed"], summary["failed"], summary["replans"]) == (4, 1, cap)
    assert summary["failed_by_code"] == {"PLAN_STALE": 1}
    assert summary["failed_instruments"] == [{"instrument_id": moving, "code": "PLAN_STALE"}]
    # The queue drained, but not every instrument reconciled: never painted clean.
    assert (summary["status"], summary["clean"], code) == ("completed", False, 2)


def test_replan_that_drops_an_instrument_is_reported_and_reconciled_is_clean(tmp_path):
    ids = _ids(4)

    def planner(scope, index):
        if scope is None:
            return ids, []
        return [], [[ids[2], "ALREADY_RECONCILED"], [ids[3], "NAV_STALE"]]

    def applier(sha, chunk, n):
        return preflight_stale(sha, chunk) if n == 2 else committed(sha, chunk)

    fake = FakeOperator(ids, planner=planner, applier=applier)
    code, summary, _ = _run(fake, tmp_path, batch_size=2)
    assert fake.plans[1]["scope"] == ids[2:]
    assert summary["replan_excluded_by_code"] == {"ALREADY_RECONCILED": 1, "NAV_STALE": 1}
    # NAV_STALE after a re-plan means the session rolled before the chain ran.
    assert (summary["status"], summary["clean"], code) == ("completed", False, 2)
    def reconciled_planner(scope, index):
        if scope is None:
            return ids, []
        return [], [[iid, "ALREADY_RECONCILED"] for iid in ids[2:]]

    reconciled = FakeOperator(ids, planner=reconciled_planner, applier=applier)
    code, summary, _ = _run(reconciled, tmp_path / "b", batch_size=2)
    assert (summary["clean"], code) == (True, 0)


def test_replan_storm_and_total_replans_are_capped(tmp_path):
    fake = FakeOperator(_ids(3), applier=lambda sha, chunk, n: preflight_stale(sha, chunk))
    code, summary, _ = _run(fake, tmp_path)
    assert (code, summary["stop_code"]) == (2, "REPLAN_NO_PROGRESS")
    assert len(fake.plans) == 1 + driver.MAX_IDLE_REPLANS
    assert len(fake.applies) == 1 + driver.MAX_IDLE_REPLANS
    assert summary["remaining"] == 3 and summary["committed"] == 0
    capped = FakeOperator(_ids(3), applier=lambda sha, chunk, n: preflight_stale(sha, chunk))
    code, summary, _ = _run(capped, tmp_path / "b", max_replans=1)
    assert (code, summary["stop_code"], len(capped.plans)) == (2, "REPLAN_LIMIT", 2)


# ── stop conditions ─────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("second", "exit_expected", "stop_code"),
    [
        # Exit 3: schema/access/dependency incompatible (decided by the CLI).
        (
            lambda sha, ids: (
                3,
                {**rebase._empty_result(None), "code": "blocked_access"},
            ),
            3,
            "blocked_access",
        ),
        # Exit 4 with commits earlier in this run: a lock stop AFTER work -> 5.
        (
            lambda sha, ids: _result(
                sha,
                [_outcome(ids[0], "lock_busy", "LOCK_BUSY")]
                + [_outcome(i, "not_attempted", "LOCK_BUSY") for i in ids[1:]],
                stop="LOCK_BUSY",
                retryable=True,
            ),
            5,
            "LOCK_BUSY",
        ),
        # Exit 5: budget exhausted after a commit.
        (
            lambda sha, ids: _result(
                sha,
                [_outcome(ids[0], "committed")]
                + [_outcome(i, "not_attempted", "BUDGET_EXHAUSTED") for i in ids[1:]],
                stop="BUDGET_EXHAUSTED",
            ),
            5,
            "BUDGET_EXHAUSTED",
        ),
        # Exit 5: operator interrupt (SIGTERM is routed to it).
        (
            lambda sha, ids: _result(
                sha,
                [_outcome(i, "not_attempted", "INTERRUPTED") for i in ids],
                status="partial",
                code="INTERRUPTED",
                stop="INTERRUPTED",
            ),
            5,
            "INTERRUPTED",
        ),
    ],
)
def test_operator_exit_3_4_5_stops_cleanly_and_reports_untried(
    tmp_path, second, exit_expected, stop_code
):
    ids = _ids(45)

    def applier(sha, chunk, n):
        return committed(sha, chunk) if n == 1 else second(sha, chunk)

    fake = FakeOperator(ids, applier=applier)
    code, summary, lines = _run(fake, tmp_path)
    assert (code, summary["status"], summary["stop_code"]) == (
        exit_expected,
        "stopped",
        stop_code,
    )
    assert len(fake.applies) == 2  # nothing after the stop
    attempted = 20 + (1 if stop_code == "BUDGET_EXHAUSTED" else 0)
    assert summary["committed"] == attempted
    assert summary["remaining"] == 45 - attempted
    assert lines[-1]["event"] == "batch"  # the stopping batch is still reported


def test_lock_busy_before_any_commit_keeps_exit_4(tmp_path):
    ids = _ids(3)
    fake = FakeOperator(
        ids,
        applier=lambda sha, chunk, n: _result(
            sha,
            [_outcome(chunk[0], "lock_busy", "LOCK_BUSY")]
            + [_outcome(i, "not_attempted", "LOCK_BUSY") for i in chunk[1:]],
            stop="LOCK_BUSY",
            retryable=True,
        ),
    )
    code, summary, _ = _run(fake, tmp_path)
    assert (code, summary["stop_code"], summary["remaining"]) == (4, "LOCK_BUSY", 3)


def test_failures_are_recorded_never_retried_and_n_failed_batches_stop(tmp_path):
    ids = _ids(100)

    def applier(sha, chunk, n):
        if n == 2:
            return committed(sha, chunk)
        return _result(sha, [_outcome(i, "failed", "PROVIDER_NOT_FOUND") for i in chunk])

    fake = FakeOperator(ids, applier=applier)
    code, summary, _ = _run(fake, tmp_path)
    # b1 fails, b2 progresses (resets), b3..b5 fail -> stop at the third in a row.
    assert (code, summary["stop_code"], len(fake.applies)) == (
        2,
        "CONSECUTIVE_FAILED_BATCHES",
        5,
    )
    sent = [i for a in fake.applies for i in a["ids"]]
    assert len(sent) == len(set(sent)) == 100  # a failed instrument is never resent
    assert (summary["failed"], summary["committed"]) == (80, 20)
    assert summary["failed_by_code"] == {"PROVIDER_NOT_FOUND": 80}
    assert len(summary["failed_instruments"]) == 80


def test_untried_instruments_of_a_failed_batch_go_back_to_the_front(tmp_path):
    ids = _ids(25)

    def applier(sha, chunk, n):
        if n == 1:  # psycopg error on the 2nd instrument stops the batch
            return _result(
                sha,
                [_outcome(chunk[0], "committed"), _outcome(chunk[1], "failed", "DATABASE_ERROR")]
                + [_outcome(i, "not_attempted", "DATABASE_ERROR") for i in chunk[2:]],
                stop="DATABASE_ERROR",
                retryable=True,
            )
        return committed(sha, chunk)

    fake = FakeOperator(ids, applier=applier)
    code, summary, _ = _run(fake, tmp_path)
    assert fake.applies[1]["ids"] == ids[2:22]
    assert fake.applies[2]["ids"] == ids[22:]
    assert (summary["committed"], summary["failed"], summary["remaining"]) == (24, 1, 0)
    assert (summary["status"], code) == ("completed", 2)


def test_rate_limit_and_configuration_codes_stop_at_once(tmp_path):
    ids = _ids(45)
    limited = FakeOperator(
        ids,
        applier=lambda sha, chunk, n: _result(
            sha,
            [_outcome(chunk[0], "failed", "PROVIDER_RATE_LIMITED")]
            + [_outcome(i, "committed") for i in chunk[1:]],
        ),
    )
    code, summary, _ = _run(limited, tmp_path)
    assert (code, summary["stop_code"], len(limited.applies)) == (
        5,
        "PROVIDER_RATE_LIMITED",
        1,
    )
    unconfigured = FakeOperator(
        ids,
        applier=lambda sha, chunk, n: (
            2,
            {**rebase._empty_result(sha), "code": "PROVIDER_NOT_CONFIGURED"},
        ),
    )
    code, summary, _ = _run(unconfigured, tmp_path / "b")
    assert (code, summary["stop_code"], len(unconfigured.applies)) == (
        2,
        "PROVIDER_NOT_CONFIGURED",
        1,
    )


def test_plan_failures_stop_before_any_apply(tmp_path):
    expired = FakeOperator(
        [], plan_exit=(2, {**rebase._empty_result(None), "code": "POLICY_EXPIRED"})
    )
    code, summary, lines = _run(expired, tmp_path)
    assert (code, summary["stop_code"], expired.applies) == (2, "POLICY_EXPIRED", [])
    assert lines[0]["event"] == "plan" and lines[0]["code"] == "POLICY_EXPIRED"
    incompatible = FakeOperator(
        [], plan_exit=(3, {**rebase._empty_result(None), "code": "incompatible_schema"})
    )
    code, summary, _ = _run(incompatible, tmp_path / "b")
    assert (code, summary["stop_code"]) == (3, "incompatible_schema")

    class Tampered(FakeOperator):
        def _plan(self, args):
            code, result = super()._plan(args)
            return code, {**result, "plan_sha256": "0" * 64}

    code, summary, _ = _run(Tampered(_ids(2)), tmp_path / "c")
    assert (code, summary["stop_code"]) == (2, "PLAN_FILE_MISMATCH")


def test_interrupt_between_operator_calls_stops_with_exit_5(tmp_path):
    def applier(sha, chunk, n):
        if n == 2:
            raise KeyboardInterrupt
        return committed(sha, chunk)

    code, summary, _ = _run(FakeOperator(_ids(45), applier=applier), tmp_path)
    assert (code, summary["stop_code"], summary["committed"]) == (5, "INTERRUPTED", 20)


# ── canary caps and dry run ─────────────────────────────────────────────────
def test_canary_caps_and_dry_run(tmp_path):
    ids = _ids(45)
    one = FakeOperator(ids)
    code, summary, _ = _run(one, tmp_path / "a", max_batches=1)
    assert (code, summary["status"], summary["committed"], summary["remaining"]) == (
        0,
        "limit_reached",
        20,
        25,
    )
    five = FakeOperator(ids)
    code, summary, _ = _run(five, tmp_path / "b", max_instruments_total=5)
    assert [a["ids"] for a in five.applies] == [ids[:5]]
    assert (code, summary["status"], summary["submitted"]) == (0, "limit_reached", 5)
    dry = FakeOperator(ids)
    code, summary, lines = _run(dry, tmp_path / "c", dry_run=True)
    assert (code, summary["status"], dry.applies) == (0, "planned", [])
    assert [line["event"] for line in lines] == ["plan"]
    assert summary["planned_initial"] == 45


def test_total_cap_does_not_charge_requeued_instruments_twice(tmp_path):
    ids = _ids(10)

    def applier(sha, chunk, n):
        if n == 1:
            return _result(
                sha,
                [
                    _outcome(chunk[0], "committed"),
                    _outcome(chunk[1], "failed", "DATABASE_ERROR"),
                ]
                + [_outcome(i, "not_attempted", "DATABASE_ERROR") for i in chunk[2:]],
                stop="DATABASE_ERROR",
                retryable=True,
            )
        return committed(sha, chunk)

    fake = FakeOperator(ids, applier=applier)
    code, summary, _ = _run(fake, tmp_path, max_instruments_total=4)
    assert [a["ids"] for a in fake.applies] == [ids[:4], ids[2:4]]
    assert (
        summary["submitted"],
        summary["committed"],
        summary["failed"],
        summary["remaining"],
    ) == (4, 3, 1, 6)
    assert (summary["status"], code) == ("limit_reached", 2)


def test_explicit_scope_is_planned_as_given(tmp_path):
    scope = _ids(3, offset=50)
    fake = FakeOperator(_ids(10))
    code, summary, _ = _run(fake, tmp_path, scope=tuple(scope))
    assert fake.plans[0]["scope"] == scope and summary["committed"] == 3 and code == 0


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"batch_size": 21}, "BATCH_SIZE_INVALID"),
        ({"rate_per_second": 2.6}, "RATE_INVALID"),
        ({"max_seconds": 0}, "MAX_SECONDS_INVALID"),
        ({"max_batches": 0}, "MAX_BATCHES_INVALID"),
        ({"max_instruments_total": 0}, "MAX_INSTRUMENTS_TOTAL_INVALID"),
        ({"max_failed_batches": 0}, "MAX_FAILED_BATCHES_INVALID"),
        ({"scope": ("not-a-uuid",)}, "SCOPE_INVALID"),
    ],
)
def test_invalid_driver_config_is_refused_before_the_operator(tmp_path, changes, code):
    fake = FakeOperator(_ids(3))
    exit_status, summary, _ = _run(fake, tmp_path, **changes)
    assert (exit_status, summary["stop_code"], fake.plans) == (2, code, [])


def test_real_client_without_key_is_refused_before_the_cohort_plan(monkeypatch, tmp_path):
    def forbidden(*_a, **_k):
        raise AssertionError("the operator must not run without a provider key")

    monkeypatch.setattr(cli, "main", forbidden)
    code, summary = driver.run_cohort(driver.CohortConfig(work_dir=str(tmp_path)))
    assert (code, summary["stop_code"], summary["plans"]) == (2, "PROVIDER_NOT_CONFIGURED", 0)


# ── in-process operator, DSN scope and sanitization ─────────────────────────
def _scripted_main(seen, results):
    """Stand-in for ``rebase_fund_nav_window.main``: prints one JSON object."""

    def main(argv, *, client_factory=None):
        seen.append(dict(dsn=__import__("os").environ.get(driver.DSN_ENV), argv=argv))
        exit_status, payload = results(argv)
        print(json.dumps(payload))
        return exit_status

    return main


def test_cli_main_uses_database_url_only_when_explicitly_allowed(
    monkeypatch, tmp_path, capsys
):
    import os

    monkeypatch.setenv("DATABASE_URL", SECRET_DSN)
    monkeypatch.setenv("TIINGO_API_KEY", "test-key-not-real")
    seen: list[dict] = []

    def results(argv):
        if os.environ.get(driver.DSN_ENV) is None:
            return 2, {**rebase._empty_result(None), "code": "DSN_REQUIRED"}
        return 2, {**rebase._empty_result(None), "code": "POLICY_UNAVAILABLE"}

    monkeypatch.setattr(cli, "main", _scripted_main(seen, results))
    argv = ["--work-dir", str(tmp_path / "a")]
    assert driver.main(argv) == 2
    out = capsys.readouterr().out
    assert json.loads(out.splitlines()[-1])["stop_code"] == "DSN_REQUIRED"
    assert seen[-1]["dsn"] is None
    assert driver.main([*argv[:1], str(tmp_path / "b"), "--allow-database-url"]) == 2
    assert seen[-1]["dsn"] == SECRET_DSN
    assert os.environ.get(driver.DSN_ENV) is None  # scoped to the run
    monkeypatch.setenv(driver.ALLOW_DATABASE_URL_ENV, "1")
    monkeypatch.setenv(driver.DSN_ENV, "postgresql://explicit")
    driver.main(["--work-dir", str(tmp_path / "c")])
    assert seen[-1]["dsn"] == "postgresql://explicit"  # the explicit DSN wins
    out = capsys.readouterr().out
    assert "s3cr3t" not in out and "db.internal" not in out


def test_operator_output_is_captured_and_unsafe_text_never_reaches_stdout(
    monkeypatch, tmp_path, capsys
):
    seen: list[dict] = []
    ids = _ids(2)

    def results(argv):
        args = cli._parser().parse_args(argv)
        if args.mode == "plan":
            data = json.dumps(
                {"instruments": [{"instrument_id": i, "needs": []} for i in ids], "excluded": []}
            ).encode()
            Path(args.plan_file).write_bytes(data)
            return 0, {
                "status": "planned",
                "plan_sha256": hashlib.sha256(data).hexdigest(),
                "instruments": [{"instrument_id": i, "secret": SECRET_DSN} for i in ids],
            }
        return 2, {**rebase._empty_result(args.plan_sha256), "code": SECRET_DSN}

    monkeypatch.setattr(cli, "main", _scripted_main(seen, results))
    code, summary = driver.run_cohort(
        driver.CohortConfig(work_dir=str(tmp_path)), client_factory=lambda limits: None
    )
    out = capsys.readouterr().out
    assert (code, summary["stop_code"]) == (2, "UNSAFE_CODE")
    assert SECRET_DSN not in out and "s3cr3t" not in json.dumps(summary)
    assert [json.loads(line)["event"] for line in out.splitlines()] == ["plan", "batch"]

    def boom(argv, *, client_factory=None):
        raise RuntimeError(f"could not connect to {SECRET_DSN}")

    monkeypatch.setattr(cli, "main", boom)
    code, summary = driver.run_cohort(
        driver.CohortConfig(work_dir=str(tmp_path / "b")), client_factory=lambda limits: None
    )
    assert (code, summary["stop_code"]) == (2, "OPERATOR_EXCEPTION")
    assert summary["errors_by_code"] == {"OPERATOR_EXCEPTION:RuntimeError": 1}
    assert "s3cr3t" not in json.dumps(summary) + capsys.readouterr().out


def test_sigterm_is_routed_to_the_operator_interrupt_path():
    before = signal.getsignal(signal.SIGTERM)
    with driver.sigterm_as_interrupt():
        handler = signal.getsignal(signal.SIGTERM)
        with pytest.raises(KeyboardInterrupt):
            handler(signal.SIGTERM, None)
    assert signal.getsignal(signal.SIGTERM) == before


# ── run_worker lane ─────────────────────────────────────────────────────────
def _run_worker(monkeypatch, capsys, env):
    import src.run_worker as rw

    monkeypatch.setenv("WORKER", "nav_rebase_cohort")
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


def test_lane_refuses_without_confirmation_and_never_touches_the_driver(
    monkeypatch, capsys
):
    def forbidden(*_a, **_k):
        raise AssertionError("an unconfirmed lane must not run the driver")

    monkeypatch.setattr(driver, "run_cohort", forbidden)
    for env in ({}, {lane.CONFIRM_ENV: "yes"}, {lane.CONFIRM_ENV: "nav_rebase_cohort_v0"}):
        monkeypatch.delenv(lane.CONFIRM_ENV, raising=False)
        code, stats = _run_worker(monkeypatch, capsys, env)
        assert code == 1
        assert (stats["state"], stats["code"]) == ("blocked", "CONFIRMATION_REQUIRED")
        assert "s3cr3t" not in json.dumps(stats)


def test_lane_maps_worker_limit_env_and_dsn_and_exit(monkeypatch, capsys):
    import os

    calls: list[dict] = []
    exit_and_status = {"value": (0, "limit_reached")}

    def fake_run_cohort(config, **_kwargs):
        calls.append({"config": config, "dsn": os.environ.get(driver.DSN_ENV)})
        exit_status, status = exit_and_status["value"]
        return exit_status, {
            "event": "summary",
            "status": status,
            "exit": exit_status,
            "batches": 1,
            "dry_run": config.dry_run,
            "committed": 20,
        }

    monkeypatch.setattr(driver, "run_cohort", fake_run_cohort)
    env = {
        lane.CONFIRM_ENV: lane.CONFIRM_VALUE,
        "WORKER_LIMIT": "7",
        "NAV_REBASE_MAX_BATCHES": "1",
        "NAV_REBASE_RATE_PER_SECOND": "1.5",
        "NAV_REBASE_MAX_SECONDS": "300",
    }
    code, stats = _run_worker(monkeypatch, capsys, env)
    assert code == 0 and stats["state"] == "complete" and stats["worker"] == "nav_rebase_cohort"
    config = calls[-1]["config"]
    assert (
        config.max_instruments_total,
        config.max_batches,
        config.rate_per_second,
        config.max_seconds,
        config.schema,
        config.dry_run,
    ) == (7, 1, 1.5, 300.0, "public", False)
    assert calls[-1]["dsn"] is None  # DATABASE_URL fallback not allowed
    monkeypatch.setenv(driver.ALLOW_DATABASE_URL_ENV, "1")
    _run_worker(monkeypatch, capsys, env)
    assert calls[-1]["dsn"] == SECRET_DSN and os.environ.get(driver.DSN_ENV) is None
    exit_and_status["value"] = (5, "stopped")
    code, stats = _run_worker(monkeypatch, capsys, env)
    assert code == 1 and (stats["state"], stats["aborted"]) == ("failed", True)
    exit_and_status["value"] = (2, "completed")  # failed instruments: not green
    code, stats = _run_worker(monkeypatch, capsys, env)
    assert code == 1 and stats["state"] == "failed"


@pytest.mark.parametrize(
    ("env", "code"),
    [
        ({"NAV_REBASE_RATE_PER_SECOND": "3"}, "RATE_INVALID"),
        ({"NAV_REBASE_MAX_BATCHES": "one"}, "CONFIG_INVALID"),
        ({"NAV_REBASE_MAX_BATCHES": "0"}, "MAX_BATCHES_INVALID"),
        ({"WORKER_LIMIT": "0"}, None),  # run_worker itself refuses a zero cap
    ],
)
def test_lane_rejects_bad_configuration_without_running(monkeypatch, capsys, env, code):
    monkeypatch.setattr(
        driver, "run_cohort", lambda *a, **k: pytest.fail("driver must not run")
    )
    exit_status, stats = _run_worker(
        monkeypatch, capsys, {lane.CONFIRM_ENV: lane.CONFIRM_VALUE, **env}
    )
    assert exit_status not in (0, None)
    if code is not None:
        assert (stats["state"], stats["code"]) == ("blocked", code)


def test_lane_refuses_a_calc_date(monkeypatch, capsys):
    monkeypatch.setattr(
        driver, "run_cohort", lambda *a, **k: pytest.fail("driver must not run")
    )
    code, _ = _run_worker(
        monkeypatch,
        capsys,
        {lane.CONFIRM_ENV: lane.CONFIRM_VALUE, "WORKER_CALC_DATE": "2026-10-05"},
    )
    assert isinstance(code, str) and "calc_date" in code
