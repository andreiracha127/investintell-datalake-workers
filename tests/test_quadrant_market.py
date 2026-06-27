from __future__ import annotations

from src.workers import quadrant_market as qmk


def test_window_return_newest_first() -> None:
    levels = [110.0] + [100.0] * 130  # now 110 vs 126d-ago 100 -> +10%
    assert abs(qmk.window_return(levels, 126) - 0.10) < 1e-9


def test_window_return_warmup_none() -> None:
    assert qmk.window_return([100.0, 99.0], 126) is None


def test_rolling_history_collects_returns() -> None:
    levels = [100.0 + i for i in range(400)][::-1]  # newest-first rising
    hist = qmk.rolling_score_history(levels, 126, 252)
    assert len(hist) >= 200 and all(isinstance(x, float) for x in hist)


def test_market_versions() -> None:
    assert qmk.MODEL_VERSION == "market_implied_quadrant_v0"
    assert qmk.CONFIDENCE_METHOD == "rolling_score_mad_252bd_v1"


def test_market_run_returns_lock_busy_sentinel(monkeypatch) -> None:
    import contextlib

    from src import quadrant_assemble as qa

    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def commit(self): pass

    @contextlib.contextmanager
    def _busy(conn, lock_id):
        yield False

    monkeypatch.setattr(qmk, "connect", lambda dsn: _Conn())
    monkeypatch.setattr(qmk, "advisory_lock", _busy)
    monkeypatch.setattr(qa, "ensure_schema", lambda conn: None)
    out = qmk.run("postgresql://unused")
    assert out["skipped"] == "lock_busy"


import os as _os

import pytest


@pytest.mark.skipif(
    not (_os.getenv("DATABASE_URL") and _os.getenv("TIINGO_API_KEY")),
    reason="needs DATABASE_URL + TIINGO_API_KEY")
def test_smoke_market_run_emits_a_snapshot() -> None:
    out = qmk.run(_os.environ["DATABASE_URL"])
    assert out["model_version"] == "market_implied_quadrant_v0"
    assert out["status"] in {"valid", "low_confidence", "unavailable", "invalid"}
