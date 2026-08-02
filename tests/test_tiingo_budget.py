"""Guards for the account-wide Tiingo request budget.

The 2026-08-02 incident: ``eod_prices_warmer`` paced its own ``TokenBucket`` at
25 req/s (90k req/h) against a 10k req/h account cap. It drained the whole hourly
budget in ~6min40s, the 30x429 breaker aborted it "cleanly" (exit 0 -> Railway
SUCCESS), and every other Tiingo consumer that ran inside the same rolling hour --
credit_regime, regime_composite, regime_gate at 07:0x UTC -- got 429s that
``_get_bars`` masks as an empty list, surfacing as "Tiingo returned empty history".
The regime chain sat stale for five days while every card stayed green.

Two independent things have to hold so that cannot repeat: no caller may pace
above the account budget, and a run that aborts on the budget must not report
success.
"""

from __future__ import annotations

import json

import pytest

from src.workers._tiingo import (
    DEFAULT_RATE_PER_S,
    TIINGO_MAX_REQUESTS_PER_HOUR,
    TiingoClient,
    TokenBucket,
)


# ── the budget itself ────────────────────────────────────────────────────────
def test_documented_budget_matches_the_account_plan():
    """10k req/h is the plan's ceiling, not a guess — keep it pinned."""
    assert TIINGO_MAX_REQUESTS_PER_HOUR == 10_000


def test_default_rate_stays_under_the_hourly_budget():
    assert DEFAULT_RATE_PER_S * 3600 <= TIINGO_MAX_REQUESTS_PER_HOUR
    assert TokenBucket().refill_rate == DEFAULT_RATE_PER_S


@pytest.mark.parametrize("rate", [25.0, 10.0, 3.0, 2.7778])
def test_client_rejects_a_bucket_above_the_hourly_budget(rate):
    """A caller may pace slower than the default, never faster than the account."""
    with pytest.raises(ValueError, match="req/h"):
        TiingoClient(key="stub", bucket=TokenBucket(refill_rate=rate))


def test_client_accepts_a_bucket_at_or_below_the_budget():
    for rate in (DEFAULT_RATE_PER_S, 1.0, 0.25):
        with TiingoClient(key="stub", bucket=TokenBucket(refill_rate=rate)) as c:
            assert c._bucket.refill_rate == rate


def test_token_bucket_stays_provider_agnostic():
    """_fallback_nav (EODHD/Yahoo) and _openfigi pace this same bucket under
    *their* limits — the Tiingo ceiling must not leak into the generic class."""
    for rate in (10.0, 4.0, 25.0):
        assert TokenBucket(refill_rate=rate).refill_rate == rate


# ── the worker that broke it ─────────────────────────────────────────────────
def test_eod_prices_warmer_paces_within_the_budget():
    """Regression guard for the exact constant that caused the incident."""
    from src.workers import eod_prices_warmer as w

    assert w.FETCH_RATE_PER_S * 3600 <= TIINGO_MAX_REQUESTS_PER_HOUR


def test_every_worker_bucket_override_stays_within_the_budget():
    """Any future worker that builds its own bucket is covered too."""
    import importlib
    import pkgutil

    import src.workers as pkg

    offenders = []
    for mod_info in pkgutil.iter_modules(pkg.__path__):
        mod = importlib.import_module(f"src.workers.{mod_info.name}")
        for attr in dir(mod):
            if not attr.endswith("RATE_PER_S"):
                continue
            rate = getattr(mod, attr)
            if isinstance(rate, (int, float)) and rate * 3600 > TIINGO_MAX_REQUESTS_PER_HOUR:
                offenders.append(f"{mod_info.name}.{attr}={rate}")
    assert not offenders, f"pacing above {TIINGO_MAX_REQUESTS_PER_HOUR} req/h: {offenders}"


# ── a budget abort must not look like success ────────────────────────────────
def _run_main(monkeypatch, capsys, stats, *, run=None):
    """Drive run_worker.main() with a stubbed worker module.

    ``run`` overrides the stub entry point when a test needs a specific
    signature (e.g. one that does not accept ``limit``).
    """
    import src.run_worker as rw

    calls = []

    def _default_run(dsn, *, limit=None):
        calls.append({"dsn": dsn, "limit": limit})
        return stats

    monkeypatch.setenv("WORKER", "stub_worker")
    monkeypatch.setattr(rw, "resolve_dsn", lambda: "postgresql://stub")
    monkeypatch.setattr(
        rw.importlib,
        "import_module",
        lambda name: type("M", (), {"run": staticmethod(run or _default_run)}),
    )
    try:
        rw.main()
        code = 0
    except SystemExit as exc:  # non-zero exit is the behaviour under test
        code = exc.code
    return code, capsys.readouterr().out, calls


def test_main_exits_nonzero_when_the_run_aborted_on_budget(monkeypatch, capsys):
    code, out, _ = _run_main(
        monkeypatch, capsys, {"upserted": 120, "aborted": "30 consecutive 429s — aborting cleanly"}
    )
    assert code != 0, "a budget abort reported success — this is the bug that hid the incident"
    # the stats line still has to be emitted: the sweep is resumable and the
    # cursor advanced, so operators need to see how far it got.
    assert json.loads(out.strip())["aborted"].startswith("30 consecutive 429s")


def test_main_exits_zero_on_a_clean_run(monkeypatch, capsys):
    code, out, _ = _run_main(monkeypatch, capsys, {"upserted": 120})
    assert code == 0
    assert json.loads(out.strip())["upserted"] == 120


def test_main_exits_zero_when_aborted_is_absent_or_empty(monkeypatch, capsys):
    assert _run_main(monkeypatch, capsys, {"aborted": None})[0] == 0


# ── WORKER_LIMIT: batching one sweep across several runs ─────────────────────
def test_worker_limit_is_passed_through(monkeypatch, capsys):
    """Lets a ring sweep be split into per-run batches from Railway config alone.

    ``eod_prices_warmer`` already resumes from a cursor and rotates its tail, so
    capping the tickers per run is all that is needed to spread one sweep over
    several crons and keep each run well inside the hourly budget.
    """
    monkeypatch.setenv("WORKER_LIMIT", "3200")
    code, _, calls = _run_main(monkeypatch, capsys, {"upserted": 1})
    assert code == 0
    assert calls == [{"dsn": "postgresql://stub", "limit": 3200}]


def test_no_worker_limit_leaves_the_worker_default(monkeypatch, capsys):
    monkeypatch.delenv("WORKER_LIMIT", raising=False)
    code, _, calls = _run_main(monkeypatch, capsys, {"upserted": 1})
    assert code == 0
    assert calls == [{"dsn": "postgresql://stub", "limit": None}]


def test_worker_limit_on_a_worker_without_limit_fails_loudly(monkeypatch, capsys):
    """A silently ignored cap would be the same class of bug we are fixing:
    config that looks applied while the sweep runs unbounded."""
    monkeypatch.setenv("WORKER_LIMIT", "3200")
    code, _, _ = _run_main(
        monkeypatch, capsys, {"upserted": 1}, run=lambda dsn: {"upserted": 1}
    )
    assert code != 0


@pytest.mark.parametrize("bad", ["", "abc", "0", "-5"])
def test_worker_limit_rejects_a_value_that_would_cap_nothing(monkeypatch, capsys, bad):
    monkeypatch.setenv("WORKER_LIMIT", bad)
    code, _, calls = _run_main(monkeypatch, capsys, {"upserted": 1})
    if bad == "":  # unset-equivalent: fall through to the worker default
        assert code == 0 and calls[0]["limit"] is None
    else:
        assert code != 0, f"WORKER_LIMIT={bad!r} silently did nothing"
