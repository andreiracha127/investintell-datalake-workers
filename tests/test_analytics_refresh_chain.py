from contextlib import contextmanager

import pytest

from src.workers import analytics_refresh_chain as chain


class _Conn:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


@contextmanager
def _lock(_conn, _lock_id):
    yield True


def _wire(monkeypatch):
    monkeypatch.setattr(chain, "connect", lambda _dsn: _Conn())
    monkeypatch.setattr(chain, "advisory_lock", _lock)


def test_runs_risk_then_momentum_and_catalogue(monkeypatch):
    _wire(monkeypatch)
    calls = []

    def risk(_dsn, **kwargs):
        calls.append(("risk", kwargs))
        return {"calc_date": "2026-08-07", "mv_refreshed": True, "upserted": 4}

    def momentum(_dsn, **kwargs):
        calls.append(("momentum", kwargs))
        return {"calc_date": "2026-08-07", "upserted": 4}

    result = chain.run("db", calc_date="2026-08-07", limit=10,
                       risk_runner=risk, momentum_runner=momentum)

    assert result["published"] is True
    assert [stage["name"] for stage in result["stages"]] == [
        "risk_metrics", "momentum_metrics", "funds_list_mv"
    ]
    assert calls == [
        ("risk", {"calc_date": "2026-08-07", "limit": 10}),
        ("momentum", {"calc_date": "2026-08-07", "limit": 10}),
    ]


@pytest.mark.parametrize(
    "risk_stats, message",
    [
        ({"skipped": "lock_busy"}, "risk_metrics blocked"),
        ({"aborted": True}, "risk_metrics aborted"),
        ({"calc_date": "2026-08-07", "mv_refreshed": False}, "momentum_metrics blocked"),
        ({"mv_refreshed": True}, "calc_date missing"),
    ],
)
def test_risk_failure_blocks_momentum(monkeypatch, risk_stats, message):
    _wire(monkeypatch)
    called = False

    def momentum(*_args, **_kwargs):
        nonlocal called
        called = True
        return {}

    with pytest.raises(chain.DependencyBlocked, match=message):
        chain.run("db", risk_runner=lambda *_a, **_k: risk_stats,
                  momentum_runner=momentum)
    assert called is False


@pytest.mark.parametrize(
    "reason",
    ["LIMITED_RUN", "LOCK_BUSY", "SUPERSEDED", "POLICY_CHANGED", "MV_RUN_MISMATCH", None],
)
def test_nav_publication_outcome_never_blocks_analytics(monkeypatch, reason):
    """W3: analytics depends on computed metrics + MV, not on NAV publication."""
    _wire(monkeypatch)
    risk_stats = {
        "processed": 2,
        "upserted": 2,
        "calc_date": "2026-08-07",
        "workers": 1,
        "risk_run_id": "r",
        "mv_refreshed": True,
        "risk_publication": {
            "eligible": reason is None,
            "published": False,
            "reason": reason,
            "risk_run_id": "r",
            "as_of_session": None,
            "retryable": reason != "LIMITED_RUN",
        },
    }
    result = chain.run(
        "db",
        risk_runner=lambda *_a, **_k: risk_stats,
        momentum_runner=lambda *_a, **_k: {"calc_date": "2026-08-07", "upserted": 2},
    )
    assert result["published"] is True
    assert result["stages"][0]["stats"]["risk_publication"]["published"] is False


def test_stale_momentum_watermark_blocks_catalogue_publication(monkeypatch):
    _wire(monkeypatch)

    def risk(*_args, **_kwargs):
        return {"calc_date": "2026-08-07", "mv_refreshed": True}

    def momentum(*_args, **_kwargs):
        return {"calc_date": "2026-08-06", "upserted": 1}

    with pytest.raises(chain.DependencyBlocked, match="watermark"):
        chain.run("db", risk_runner=risk, momentum_runner=momentum)


def test_outer_lock_contention_is_visible(monkeypatch):
    monkeypatch.setattr(chain, "connect", lambda _dsn: _Conn())

    @contextmanager
    def busy(_conn, _lock_id):
        yield False

    monkeypatch.setattr(chain, "advisory_lock", busy)
    result = chain.run("db")
    assert result == {
        "published": False,
        "stages": [],
        "skipped": "lock_busy",
        "blocked_dependency": "analytics_refresh_chain",
    }
