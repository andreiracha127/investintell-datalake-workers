from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from scripts import rebuild_certified_metric_pack as rebuild


EXPECTED_WORKERS = (
    "factor_model",
    "fund_factors",
    "risk_metrics",
    "active_share_metrics",
    "momentum_metrics",
    "screener_metrics",
    "matview_refresh",
)


def _module(worker: str, run):
    return SimpleNamespace(run=lambda dsn: run(worker, dsn))


def test_runs_workers_in_dependency_order_and_reuses_dsn(monkeypatch) -> None:
    resolved_dsn = object()
    resolve_calls = 0
    calls: list[tuple[str, object]] = []

    def resolve_dsn():
        nonlocal resolve_calls
        resolve_calls += 1
        return resolved_dsn

    def run(worker: str, dsn: object) -> dict:
        calls.append((worker, dsn))
        return {"status": "succeeded"}

    monkeypatch.setattr(rebuild, "resolve_dsn", resolve_dsn)
    monkeypatch.setattr(
        rebuild.importlib,
        "import_module",
        lambda name: _module(name.removeprefix("src.workers."), run),
    )

    rebuild.main()

    assert rebuild.WORKERS == EXPECTED_WORKERS
    assert resolve_calls == 1
    assert [worker for worker, _ in calls] == list(EXPECTED_WORKERS)
    assert all(dsn is resolved_dsn for _, dsn in calls)


@pytest.mark.parametrize("status", ["failed", "partial"])
def test_fails_fast_on_failed_or_partial(monkeypatch, capsys, status: str) -> None:
    calls: list[str] = []

    def run(worker: str, _dsn: object) -> dict:
        calls.append(worker)
        if worker == "risk_metrics":
            return {"status": status, "detail": "test failure"}
        return {"status": "succeeded"}

    monkeypatch.setattr(rebuild, "resolve_dsn", lambda: "resolved-dsn")
    monkeypatch.setattr(
        rebuild.importlib,
        "import_module",
        lambda name: _module(name.removeprefix("src.workers."), run),
    )

    with pytest.raises(RuntimeError, match="incomplete at risk_metrics"):
        rebuild.main()

    assert calls == ["factor_model", "fund_factors", "risk_metrics"]
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert records[-1] == {
        "worker": "risk_metrics",
        "status": status,
        "detail": "test failure",
    }
    assert not any(
        record.get("status") == "succeeded" and "worker" not in record
        for record in records
    )


@pytest.mark.parametrize(
    "result",
    [
        {"status": "skipped", "reason": "lock_busy"},
        {"status": "no_data"},
        {"skipped": "lock_busy"},
        {"mv_refreshed": False, "mv_refresh_error": "refresh failed"},
    ],
)
def test_fails_fast_on_incomplete_worker_result(monkeypatch, result: dict) -> None:
    calls: list[str] = []

    def run(worker: str, _dsn: object) -> dict:
        calls.append(worker)
        return result if worker == "fund_factors" else {"status": "succeeded"}

    monkeypatch.setattr(rebuild, "resolve_dsn", lambda: "resolved-dsn")
    monkeypatch.setattr(
        rebuild.importlib,
        "import_module",
        lambda name: _module(name.removeprefix("src.workers."), run),
    )

    with pytest.raises(RuntimeError, match="incomplete at fund_factors"):
        rebuild.main()

    assert calls == ["factor_model", "fund_factors"]


def test_emits_json_lines_and_final_summary(monkeypatch, capsys) -> None:
    positions = {
        worker: position
        for position, worker in enumerate(EXPECTED_WORKERS, start=1)
    }

    def run(worker: str, _dsn: object) -> dict:
        return {"status": "succeeded", "position": positions[worker]}

    monkeypatch.setattr(rebuild, "resolve_dsn", lambda: "resolved-dsn")
    monkeypatch.setattr(
        rebuild.importlib,
        "import_module",
        lambda name: _module(name.removeprefix("src.workers."), run),
    )

    rebuild.main()

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert records[:-1] == [
        {
            "worker": worker,
            "status": "succeeded",
            "position": position,
        }
        for position, worker in enumerate(EXPECTED_WORKERS, start=1)
    ]
    assert records[-1] == {
        "status": "succeeded",
        "workers": len(EXPECTED_WORKERS),
    }


def test_rejects_non_mapping_worker_result(monkeypatch) -> None:
    monkeypatch.setattr(rebuild, "resolve_dsn", lambda: "resolved-dsn")
    monkeypatch.setattr(
        rebuild.importlib,
        "import_module",
        lambda _name: SimpleNamespace(run=lambda _dsn: ["not", "a", "dict"]),
    )

    with pytest.raises(TypeError, match="expected dict"):
        rebuild.main()
