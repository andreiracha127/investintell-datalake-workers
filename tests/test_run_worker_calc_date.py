"""WORKER_CALC_DATE — aiming the Railway entry point at a chosen as-of date.

``python -m src.run <worker> --calc-date …`` already exists, and no Railway
service that builds this repo can reach it: the root ``railway.toml`` is
config-as-code and replaces the whole ``[deploy]`` block, so a per-service
``startCommand`` is discarded and every service runs ``python -m src.run_worker``.
Whatever that entry point does not read from the environment is unreachable.

The cost of that was measured after the 2026-08-05 N-PORT identifier repair: the
weekly ``nport_lookthrough`` cron runs at ``calc_date = max(report_date)``, which
rewrites a series only at its LAST report — 499 of the 16.774 repaired
series-dates, 3 %. The other 97 % keep the bad identifiers until someone can aim
the worker at a historical date.

These tests hold the same line the WORKER_LIMIT ones hold: a value the platform
accepts but the worker never receives is worse than a crash, because the deploy
goes green and the operator reads the wrong verdict off it.
"""

from __future__ import annotations

import importlib
import inspect

import pytest


# Distinguishes "run() was called without the kwarg" from "run() was called with
# None". Passing None would silently look like the worker's own default.
_DEFAULT = "<worker default>"


def _run_main(monkeypatch, capsys, *, calc_date=None, limit=None, run=None, stats=None):
    """Drive run_worker.main() with a stubbed worker module.

    ``run`` overrides the stub entry point when a test needs a specific
    signature (e.g. one that does not accept ``calc_date``).
    """
    import src.run_worker as rw

    calls = []

    def _default_run(dsn, *, calc_date=_DEFAULT, limit=_DEFAULT):
        calls.append({"dsn": dsn, "calc_date": calc_date, "limit": limit})
        return {"upserted": 1} if stats is None else stats

    monkeypatch.setenv("WORKER", "stub_worker")
    if calc_date is None:
        monkeypatch.delenv("WORKER_CALC_DATE", raising=False)
    else:
        monkeypatch.setenv("WORKER_CALC_DATE", calc_date)
    if limit is None:
        monkeypatch.delenv("WORKER_LIMIT", raising=False)
    else:
        monkeypatch.setenv("WORKER_LIMIT", limit)
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


def test_calc_date_is_passed_through_as_the_iso_string(monkeypatch, capsys):
    """The workers parse the date themselves; handing them the string they
    already accept keeps a single interpretation of what it means."""
    code, _, calls = _run_main(monkeypatch, capsys, calc_date="2025-01-31")
    assert code == 0
    assert calls == [
        {"dsn": "postgresql://stub", "calc_date": "2025-01-31", "limit": _DEFAULT}
    ]


def test_no_calc_date_leaves_the_worker_default(monkeypatch, capsys):
    """Every service running today has no WORKER_CALC_DATE set; none of them may
    start resolving a different date because this variable now exists."""
    code, _, calls = _run_main(monkeypatch, capsys)
    assert code == 0
    assert calls == [
        {"dsn": "postgresql://stub", "calc_date": _DEFAULT, "limit": _DEFAULT}
    ]


@pytest.mark.parametrize("blank", ["", "   "])
def test_a_blank_calc_date_is_unset_not_an_error(monkeypatch, capsys, blank):
    """Railway keeps emptied variables as empty strings rather than dropping
    them, so blank has to mean 'not asked for', exactly as for WORKER_LIMIT."""
    code, _, calls = _run_main(monkeypatch, capsys, calc_date=blank)
    assert code == 0
    assert calls[0]["calc_date"] == _DEFAULT


@pytest.mark.parametrize(
    "bad",
    [
        "abc",
        "2025-1-31",  # unpadded
        "2025-01-31T00:00:00",  # a timestamp, not a date
        "2025-13-01",  # month out of range
        "2025-02-30",  # day out of range for the month
        # Accepted by date.fromisoformat since 3.11, rejected by the strptime
        # the workers use. Letting these through would move the failure three
        # layers down, into a run that already took the advisory lock.
        "20250131",
        "2025-W05-5",
    ],
)
def test_a_calc_date_that_is_not_yyyy_mm_dd_fails_before_the_run(
    monkeypatch, capsys, bad
):
    code, _, calls = _run_main(monkeypatch, capsys, calc_date=bad)
    assert code != 0, f"WORKER_CALC_DATE={bad!r} was accepted"
    assert calls == [], "the worker ran despite an unusable calc_date"
    assert bad in str(code), "the failure has to name the value that caused it"


def test_calc_date_on_a_worker_without_calc_date_fails_loudly(monkeypatch, capsys):
    """Dropping it silently is the WORKER_LIMIT bug in another costume: the
    config would claim a historical run while the worker swept its own default
    date, and a green deploy would certify the wrong rows."""
    code, _, _ = _run_main(
        monkeypatch,
        capsys,
        calc_date="2025-01-31",
        run=lambda dsn: {"upserted": 1},
    )
    assert code != 0
    assert "calc_date" in str(code)


def test_calc_date_and_limit_travel_together(monkeypatch, capsys):
    """A dated run of a worker that also batches has to be able to do both from
    config alone — that is the whole point of not needing a startCommand."""
    code, _, calls = _run_main(
        monkeypatch, capsys, calc_date="2023-10-31", limit="3200"
    )
    assert code == 0
    assert calls == [
        {"dsn": "postgresql://stub", "calc_date": "2023-10-31", "limit": 3200}
    ]


def test_an_aborted_dated_run_still_exits_nonzero(monkeypatch, capsys):
    """The budget contract does not weaken because the date was pinned."""
    code, out, _ = _run_main(
        monkeypatch,
        capsys,
        calc_date="2025-01-31",
        stats={"upserted": 7, "aborted": "provider budget"},
    )
    assert code != 0
    assert "aborted" in out


@pytest.mark.parametrize(
    "worker", ["nport_lookthrough", "characteristics", "active_share_metrics"]
)
def test_the_workers_this_variable_exists_for_accept_calc_date(worker):
    """The three as-of workers reachable only through run_worker. If one loses
    the parameter, WORKER_CALC_DATE on its service starts exiting non-zero and
    this test says why before an operator finds out from a failed cron."""
    mod = importlib.import_module(f"src.workers.{worker}")
    assert "calc_date" in inspect.signature(mod.run).parameters
