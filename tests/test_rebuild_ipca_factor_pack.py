from __future__ import annotations

import contextlib
from types import SimpleNamespace

import pytest

from scripts import rebuild_ipca_factor_pack as rebuild


def _healthy(worker: str) -> dict:
    results = {
        "characteristics": {
            "status": "succeeded",
            "upserted": 10,
            "equity_upserted": 5,
        },
        "factor_model": {
            "status": "succeeded",
            "fit_id": "fit-6",
            "universe_hash": "universe",
            "k_factors": 6,
            "converged": True,
            "degraded": False,
            "oos_r_squared": 0.07,
        },
        "gamma_drift": {
            "status": "succeeded",
            "monitored": 1,
            "alerts": 0,
            "target_fit_id": "fit-6",
        },
        "fund_factors": {
            "fit_id": "fit-6",
            "k_factors": 6,
            "processed": 2,
            "upserted": 12,
            "mv_refreshed": False,
            "mv_refresh_reason": "deferred_until_activation",
        },
        "ipca_production_gate": {
            "status": "succeeded",
            "fit_id": "fit-6",
            "quality_warnings": ["bounded"],
            "activated": True,
        },
    }
    return results[worker]


@pytest.fixture(autouse=True)
def _pack_lock(monkeypatch):
    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    @contextlib.contextmanager
    def acquired(_conn, _lock_id):
        yield True

    monkeypatch.setattr(rebuild, "connect", lambda _dsn: _Conn())
    monkeypatch.setattr(rebuild, "advisory_lock", acquired)


def test_runs_ipca_steps_in_dependency_order(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    def import_module(name: str):
        worker = name.removeprefix("src.workers.")

        def run(_dsn, **kwargs):
            calls.append((worker, kwargs))
            return _healthy(worker)

        return SimpleNamespace(run=run)

    monkeypatch.setattr(rebuild.importlib, "import_module", import_module)

    result = rebuild.run_pack("dsn")

    assert [worker for worker, _ in calls] == list(rebuild.STEPS)
    assert calls[1][1] == {"production_fit": False}
    assert calls[2][1] == {"target_fit_id": "fit-6"}
    assert calls[3][1] == {"fit_id": "fit-6", "refresh_mv": False}
    assert calls[4][1]["expected_fit_id"] == "fit-6"
    assert calls[4][1]["activate"] is True
    assert "min_characteristics_computed_at" in calls[4][1]
    assert result == {
        "status": "succeeded",
        "fit_id": "fit-6",
        "steps": 5,
        "quality_warnings": ["bounded"],
    }


def test_stops_when_fund_factors_publish_stale_fit(monkeypatch) -> None:
    def import_module(name: str):
        worker = name.removeprefix("src.workers.")

        def run(_dsn, **_kwargs):
            result = _healthy(worker)
            if worker == "fund_factors":
                result = {**result, "fit_id": "stale-fit"}
            return result

        return SimpleNamespace(run=run)

    monkeypatch.setattr(rebuild.importlib, "import_module", import_module)

    with pytest.raises(RuntimeError, match="did not publish the new fit"):
        rebuild.run_pack("dsn")


def test_stops_on_gamma_alert(monkeypatch) -> None:
    def import_module(name: str):
        worker = name.removeprefix("src.workers.")

        def run(_dsn, **_kwargs):
            result = _healthy(worker)
            if worker == "gamma_drift":
                result = {**result, "alerts": 1}
            return result

        return SimpleNamespace(run=run)

    monkeypatch.setattr(rebuild.importlib, "import_module", import_module)

    with pytest.raises(RuntimeError, match="did not certify"):
        rebuild.run_pack("dsn")
