"""Pure contracts of the governed economic NAV rebase (no DB, no network)."""

from __future__ import annotations

import datetime as dt
import json
import math
import socket
import uuid

import pytest

from scripts import rebase_fund_nav_window as cli
from src.workers import _tiingo
from src.workers import nav_economic_rebase as rebase
from src.workers._tiingo import NavFetchResult, NavObservation

START = dt.date(2025, 1, 2)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Any Python-level socket connection fails the test (provider is mocked)."""

    def refuse(*_args, **_kwargs):
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)


def _sessions(count=405, start=START):
    out, day = [], start
    while len(out) < count:
        if day.weekday() < 5:
            out.append(day)
        day += dt.timedelta(days=1)
    return out


SESSIONS = _sessions()
DUE = SESSIONS[:-2]  # the last two sessions are closed/listed but not yet due


def _item(**changes):
    base = dict(
        instrument_id=str(uuid.UUID(int=1)),
        ticker="SYN",
        currency="USD",
        lifecycle_evidence_id=str(uuid.UUID(int=2)),
        revision_head=7,
        active_event_ids=(),
        needs=("LINEAGE_MISSING",),
        window_start=SESSIONS[0],
        window_end=SESSIONS[-1],
        session_count=len(SESSIONS),
        sessions_digest="d" * 64,
    )
    base.update(changes)
    return rebase.PlanItem(**base)


def _fetch(days=None, *, status="success_new", kind="adjusted", tweak=None):
    now = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    days = SESSIONS if days is None else days
    obs = [
        NavObservation(d, round(100 + i * 0.01, 6), kind) for i, d in enumerate(days)
    ]
    if tweak:
        obs = tweak(obs)
    return NavFetchResult(status, tuple(obs), now - dt.timedelta(seconds=2), now)


def _validate(fetch, item=None):
    return rebase.validate_adjusted_snapshot(
        item or _item(), fetch, sessions=SESSIONS, due_sessions=DUE
    )


def test_complete_adjusted_snapshot_is_accepted_and_digest_is_canonical():
    snapshot = _validate(_fetch())
    assert len(snapshot.observations) == len(SESSIONS)
    again = _validate(_fetch())
    assert snapshot.provider_snapshot_sha256 == again.provider_snapshot_sha256
    moved = _validate(
        _fetch(
            tweak=lambda o: [
                *o[:-1],
                NavObservation(o[-1].date, o[-1].price + 0.02, "adjusted"),
            ]
        )
    )
    assert moved.provider_snapshot_sha256 != snapshot.provider_snapshot_sha256
    # Not-yet-due closed sessions may be absent; due sessions may not.
    assert len(_validate(_fetch(SESSIONS[:-1])).observations) == len(SESSIONS) - 1


@pytest.mark.parametrize(
    ("fetch", "code"),
    [
        (_fetch(kind="raw"), "RAW_OR_MIXED_KIND"),
        (_fetch(kind="unknown"), "RAW_OR_MIXED_KIND"),
        (
            _fetch(
                tweak=lambda o: [
                    *o[:10],
                    NavObservation(o[10].date, o[10].price, "raw"),
                    *o[11:],
                ]
            ),
            "RAW_OR_MIXED_KIND",
        ),
        (
            _fetch(
                tweak=lambda o: [
                    *o[:10],
                    NavObservation(o[10].date, math.nan, "adjusted"),
                    *o[11:],
                ]
            ),
            "NONFINITE_OR_NONPOSITIVE",
        ),
        (
            _fetch(
                tweak=lambda o: [
                    *o[:10],
                    NavObservation(o[10].date, math.inf, "adjusted"),
                    *o[11:],
                ]
            ),
            "NONFINITE_OR_NONPOSITIVE",
        ),
        (
            _fetch(
                tweak=lambda o: [
                    *o[:10],
                    NavObservation(o[10].date, 0.0, "adjusted"),
                    *o[11:],
                ]
            ),
            "NONFINITE_OR_NONPOSITIVE",
        ),
        (
            _fetch(
                tweak=lambda o: [
                    *o[:10],
                    NavObservation(o[10].date, None, "adjusted"),
                    *o[11:],
                ]
            ),
            "NONFINITE_OR_NONPOSITIVE",
        ),
        (_fetch(tweak=lambda o: [*o, o[5]]), "DUPLICATE_DATE"),
        (
            _fetch(
                tweak=lambda o: [
                    NavObservation(
                        SESSIONS[0] - dt.timedelta(days=3), 99.0, "adjusted"
                    ),
                    *o,
                ]
            ),
            "OUT_OF_WINDOW",
        ),
        (
            _fetch(
                tweak=lambda o: [
                    *o,
                    NavObservation(
                        SESSIONS[-1] + dt.timedelta(days=1), 99.0, "adjusted"
                    ),
                ]
            ),
            "OUT_OF_WINDOW",
        ),
        (
            _fetch(
                tweak=lambda o: [
                    *o,
                    NavObservation(dt.date(2025, 1, 4), 99.0, "adjusted"),
                ]
            ),
            "NON_SESSION_DATE",
        ),
        (_fetch(tweak=lambda o: o[:100] + o[101:]), "GRID_INCOMPLETE"),
        (
            _fetch(
                tweak=lambda o: [
                    *o[:200],
                    NavObservation(o[200].date, o[200].price * 1e-4, "adjusted"),
                    *o[201:],
                ]
            ),
            "REPAIR_REQUIRED",
        ),
        (_fetch(status="not_found"), "PROVIDER_NOT_FOUND"),
        (_fetch(status="rate_limited"), "PROVIDER_RATE_LIMITED"),
        (_fetch(status="transient_error"), "PROVIDER_TRANSIENT_ERROR"),
        (_fetch(status="not_configured"), "PROVIDER_NOT_CONFIGURED"),
        (_fetch(status="weird"), "PROVIDER_INVALID_PAYLOAD"),
        (NavFetchResult("success_no_new"), "PROVIDER_EMPTY"),
        (
            NavFetchResult("success_new", _fetch().observations),
            "PROVIDER_INVALID_PAYLOAD",
        ),
    ],
)
def test_snapshot_rejections_fail_the_whole_instrument(fetch, code):
    with pytest.raises(rebase.RebaseError) as info:
        _validate(fetch)
    assert info.value.code == code


def test_rebase_rows_are_observed_levels_without_repair_or_inherited_metadata():
    snapshot = _validate(_fetch())
    rows = rebase.build_rebase_rows(_item(), snapshot, ("CAL", "v1", "src"))
    assert len(rows) == len(SESSIONS)
    assert all(
        r["nav"] == r["source_nav"]
        and r["source_nav_kind"] == "adjusted"
        and r["nav_repair_kind"] == "none"
        and r["source"] == "tiingo"
        and r["currency"] == "USD"
        and r["return_1d"] is None
        and (r["calendar_id"], r["calendar_version"], r["calendar_source"])
        == ("CAL", "v1", "src")
        for r in rows
    )


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"batch_size": 21}, "BATCH_SIZE_INVALID"),
        ({"batch_size": 0}, "BATCH_SIZE_INVALID"),
        ({"max_instruments": 6}, "MAX_INSTRUMENTS_INVALID"),
        ({"max_requests": 6}, "MAX_REQUESTS_INVALID"),
        ({"max_seconds": 0.0}, "MAX_SECONDS_INVALID"),
        ({"max_seconds": math.inf}, "MAX_SECONDS_INVALID"),
        ({"rate_per_second": 2.6}, "RATE_INVALID"),
        ({"rate_per_second": 0.0}, "RATE_INVALID"),
    ],
)
def test_limits_are_bounded(changes, code):
    base = dict(
        batch_size=5,
        max_instruments=5,
        max_requests=5,
        max_seconds=60.0,
        rate_per_second=1.0,
    )
    base.update(changes)
    with pytest.raises(rebase.RebaseError) as info:
        rebase.RebaseLimits(**base).validate()
    assert info.value.code == code
    assert rebase.MAX_BATCH == 20
    assert rebase.MAX_RATE_PER_SECOND == _tiingo.DEFAULT_RATE_PER_S


def test_plan_manifest_round_trip_recomputes_hash_and_rejects_versions():
    item = _item()
    manifest = {
        "plan_version": rebase.PLAN_VERSION,
        "contract_version": rebase.CONTRACT_VERSION,
        "instruments": [item.canonical()],
        "limits": {},
    }
    plan = rebase.RebasePlan.from_manifest(json.loads(json.dumps(manifest)))
    assert plan.items == (item,)
    assert plan.sha256 == rebase.canonical_digest(manifest)
    assert (
        rebase.plan_bytes(plan)
        == json.dumps(
            manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
    )
    for key, value in (
        ("plan_version", "nav-rebase-plan-v0"),
        ("contract_version", "other"),
    ):
        with pytest.raises(rebase.RebaseError, match="PLAN_VERSION_MISMATCH"):
            rebase.RebasePlan.from_manifest({**manifest, key: value})


def _result(**counts):
    items = []
    for status, n in counts.items():
        items += [
            rebase.InstrumentRebaseResult(
                str(uuid.uuid4()),
                status,
                None if status == "committed" else "X",
                retryable=status != "failed",
            )
            for _ in range(n)
        ]
    return {i.instrument_id: i for i in items}


@pytest.mark.parametrize(
    ("outcomes", "stop", "status", "code"),
    [
        (_result(committed=2), None, "completed", 0),
        (_result(already_applied=2), None, "completed", 0),
        (_result(committed=1, failed=1), None, "partial", 2),
        (_result(failed=2), None, "blocked", 2),
        (_result(committed=1, not_attempted=1), "BUDGET_EXHAUSTED", "partial", 5),
        (_result(not_attempted=2), "BUDGET_EXHAUSTED", "partial", 5),
        (_result(lock_busy=1, not_attempted=1), "LOCK_BUSY", "lock_busy", 4),
        (_result(committed=1, lock_busy=1), "LOCK_BUSY", "partial", 5),
        # A lost COMMIT acknowledgement is never "zero writes".
        (_result(committed_unverified=1), "DATABASE_ERROR", "partial", 5),
        (_result(unknown=1, not_attempted=1), "DATABASE_ERROR", "partial", 2),
        (_result(committed=1, unknown=1), "DATABASE_ERROR", "partial", 2),
    ],
)
def test_result_status_and_exit_contract(outcomes, stop, status, code):
    result = rebase._finish(
        rebase._empty_result("a" * 64), outcomes=outcomes, stop_code=stop
    )
    assert (result["status"], rebase.exit_code(result)) == (status, code)
    wrote = result["committed"] + result["committed_unverified"] + result["unknown"]
    assert result["readiness_republish_required"] is (wrote > 0)
    assert set(result) == {
        "status",
        "contract_version",
        "plan_sha256",
        "run_id",
        "planned",
        "fetched",
        "committed",
        "committed_unverified",
        "unknown",
        "already_applied",
        "failed",
        "not_attempted",
        "requests_used",
        "orphan_runs_recovered",
        "retryable",
        "reason_counts",
        "instruments",
        "readiness_republish_required",
        "errors",
        "code",
    }
    assert rebase.exit_code({"status": "planned"}) == 0


def test_bookkeeping_error_keeps_confirmed_outcomes_and_fails_exit():
    result = rebase._empty_result("a" * 64)
    result["errors"] = ["FINALIZE_FAILED", "MUTEX_RELEASE_FAILED"]
    done = rebase._finish(result, outcomes=_result(committed=1))
    assert (
        done["status"],
        done["committed"],
        done["readiness_republish_required"],
    ) == ("partial", 1, True)
    assert (done["code"], rebase.exit_code(done)) == ("FINALIZE_FAILED", 2)


class _Response:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or []

    def json(self):
        return self._payload


class _HttpStub:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, *_args, **_kwargs):
        self.calls += 1
        return self.responses.pop(0)

    def close(self):
        pass


def _client(responses):
    client = _tiingo.TiingoClient(
        key="test-key-not-real",
        bucket=_tiingo.TokenBucket(max_tokens=10, refill_rate=2.5),
    )
    client._client.close()
    client._client = _HttpStub(responses)
    return client


def test_rebase_fetch_is_exactly_one_request_and_default_ladder_is_unchanged(
    monkeypatch,
):
    slept = []
    monkeypatch.setattr(_tiingo.time, "sleep", slept.append)
    one = _client([_Response(500), _Response(500), _Response(500)])
    result = one.fetch_daily_observations(
        "SYN", START, START + dt.timedelta(days=5), max_attempts=1
    )
    assert (result.status, one._client.calls, one.requests_made, slept) == (
        "transient_error",
        1,
        1,
        [0.0],
    )
    slept.clear()
    ladder = _client([_Response(500), _Response(500), _Response(500)])
    result = ladder.fetch_daily_observations("SYN", START, START + dt.timedelta(days=5))
    assert (result.status, ladder._client.calls) == ("transient_error", 3)
    assert slept == list(_tiingo._RETRY_SLEEPS)
    ok = _client(
        [
            _Response(
                200,
                [{"date": "2025-01-02T00:00:00.000Z", "adjClose": 10.0, "close": 11.0}],
            )
        ]
    )
    result = ok.fetch_daily_observations("SYN", START, START, max_attempts=1)
    assert result.observations == (NavObservation(START, 10.0, "adjusted"),)
    with pytest.raises(ValueError):
        ok._request_bars("SYN", START, START, max_attempts=0)


def test_deadline_bounds_pacing_and_request_without_consuming(monkeypatch):
    slept = []
    monkeypatch.setattr(_tiingo.time, "sleep", slept.append)
    # Budget already exhausted: nothing paced, nothing requested.
    client = _client([_Response(200, [])])
    with pytest.raises(_tiingo.TiingoDeadlineExceeded):
        client.fetch_daily_observations(
            "SYN", START, START, max_attempts=1, remaining=lambda: 0.0
        )
    assert (client._client.calls, client.requests_made, slept) == (0, 0, [])
    # Empty bucket: the pacing wait (1/rate) exceeds the remaining budget, so
    # it raises before sleeping and without taking a token.
    bucket = _tiingo.TokenBucket(max_tokens=1, refill_rate=0.5)
    bucket._tokens = 0.0
    with pytest.raises(_tiingo.TiingoDeadlineExceeded):
        bucket.acquire(max_wait=1.0)
    assert slept == [] and bucket._tokens < 1.0
    # Budget expires during the pacing wait: re-checked before sending.
    left = iter([5.0, 0.0])
    paced = _client([_Response(200, [])])
    paced._bucket = _tiingo.TokenBucket(max_tokens=1, refill_rate=2.5)
    paced._bucket._tokens = 0.0
    with pytest.raises(_tiingo.TiingoDeadlineExceeded):
        paced.fetch_daily_observations(
            "SYN", START, START, max_attempts=1, remaining=lambda: next(left)
        )
    assert paced._client.calls == 0 and len(slept) >= 1
    # The request timeout never exceeds the remaining budget.
    seen = {}

    class _Recorder(_HttpStub):
        def get(self, *args, **kwargs):
            seen.update(kwargs)
            return super().get(*args, **kwargs)

    capped = _client([])
    capped._client = _Recorder(
        [_Response(200, [{"date": "2025-01-02", "adjClose": 1.0}])]
    )
    capped.fetch_daily_observations(
        "SYN", START, START, max_attempts=1, remaining=lambda: 7.5
    )
    assert seen["timeout"] == 7.5 and capped.requests_made == 1


def test_cli_validation_never_touches_db_or_network(monkeypatch, capsys):
    monkeypatch.delenv("NAV_READINESS_DATABASE_URL", raising=False)
    base = [
        "--schema",
        "s",
        "--contract",
        rebase.CONTRACT_VERSION,
        "--max-requests",
        "1",
        "--max-seconds",
        "10",
        "--rate-per-second",
        "1",
        "--batch-size",
        "1",
    ]
    cases = [
        (
            [
                "--schema",
                "s",
                "--contract",
                "other",
                "--max-requests",
                "1",
                "--max-seconds",
                "1",
                "--rate-per-second",
                "1",
            ],
            "CONTRACT_UNSUPPORTED",
        ),
        (["--schema", "s", "--contract", rebase.CONTRACT_VERSION], "BUDGETS_REQUIRED"),
        ([*base[:-2], "--batch-size", "21"], "BATCH_SIZE_INVALID"),
        (["--mode", "apply", *base], "APPLY_REQUIRES_PLAN_HASH_AND_ALLOWLIST"),
        (base, "DSN_REQUIRED"),
        (["--schema", "bad;name", *base[2:]], "SCHEMA_INVALID"),
    ]
    for argv, code in cases:
        assert cli.main(argv) == rebase.EXIT_FAILED
        out = json.loads(capsys.readouterr().out)
        assert out["code"] == code and out["status"] == "blocked"
        assert "postgres" not in json.dumps(out)
