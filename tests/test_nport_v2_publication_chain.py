"""Contracts for the post-publication N-PORT V2 coupling worker."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from src.workers import nport_v2_publication_chain as chain


class _Conn:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


@contextmanager
def _lock(_conn, _lock_id):
    yield True


def _wire(monkeypatch) -> None:
    monkeypatch.setattr(chain, "connect", lambda _dsn, **_kwargs: _Conn())
    monkeypatch.setattr(chain, "advisory_lock", _lock)


def _fresh(publication_id: str = "publication-1") -> dict:
    return {"state": "fresh", "publication_id": publication_id}


def test_refreshes_and_proves_identity_before_v2_dependent_publication(monkeypatch) -> None:
    _wire(monkeypatch)
    calls: list[object] = []

    def source(_conn):
        calls.append("source")
        return "publication-1", "run-1"

    def refresh(_dsn):
        calls.append("refresh")
        return {"refreshed": True, "bootstrap": True}

    def probe(_dsn):
        calls.append("probe")
        return _fresh()

    def downstream(_dsn, **kwargs):
        calls.append(("downstream", kwargs))
        return {"state": "published", "publication_id": "features-1"}

    result = chain.run(
        "db", calc_date="2026-08-07", limit=10, current_publication=source,
        identity_refresher=refresh, identity_probe=probe, downstream_runner=downstream,
    )

    assert result["published"] is True
    assert [stage["name"] for stage in result["stages"]] == [
        "sec_nport_holdings_v2",
        "nport_holdings_snapshot_identity_v1",
        "nport_holdings_identity_freshness",
        "nport_fixed_income_serving",
    ]
    assert calls == [
        "source", "refresh", "probe", "source",
        ("downstream", {"calc_date": "2026-08-07", "limit": 10}),
        "source",
    ]


def test_missing_current_publication_fails_closed_before_identity_work(monkeypatch) -> None:
    _wire(monkeypatch)
    called = False

    def unexpected(*_args, **_kwargs):
        nonlocal called
        called = True
        return {}

    result = chain.run(
        "db", current_publication=lambda _conn: None, identity_refresher=unexpected,
        identity_probe=unexpected, downstream_runner=unexpected,
    )

    assert result["aborted"] is True
    assert result["blocked_dependency"] == "no_current_validated_v2_publication"
    assert result["stages"] == []
    assert called is False


def test_stale_identity_blocks_analytics_after_refresh(monkeypatch) -> None:
    _wire(monkeypatch)
    called = False

    def downstream(*_args, **_kwargs):
        nonlocal called
        called = True
        return {"state": "published"}

    verdict = {"state": "stale", "publication_id": "publication-1", "reason": "behind"}
    result = chain.run(
        "db", current_publication=lambda _conn: ("publication-1", "run-1"),
        identity_refresher=lambda _dsn: {"refreshed": True, "bootstrap": False},
        identity_probe=lambda _dsn: verdict, downstream_runner=downstream,
    )

    assert result["aborted"] is True
    assert result["blocked_dependency"] == "identity_not_fresh"
    assert result["identity_verdict"] == verdict
    assert [stage["name"] for stage in result["stages"]] == [
        "sec_nport_holdings_v2",
        "nport_holdings_snapshot_identity_v1",
        "nport_holdings_identity_freshness",
    ]
    assert called is False


def test_downstream_failure_preserves_completed_stage_evidence(monkeypatch) -> None:
    _wire(monkeypatch)
    result = chain.run(
        "db", current_publication=lambda _conn: ("publication-1", "run-1"),
        identity_refresher=lambda _dsn: {"refreshed": True, "bootstrap": False},
        identity_probe=lambda _dsn: _fresh(),
        downstream_runner=lambda *_args, **_kwargs: {"state": "no_source", "reason": "raw_pruned"},
    )

    assert result["aborted"] is True
    assert result["blocked_dependency"] == "fixed_income_serving_not_published"
    assert [stage["name"] for stage in result["stages"]] == [
        "sec_nport_holdings_v2",
        "nport_holdings_snapshot_identity_v1",
        "nport_holdings_identity_freshness",
        "nport_fixed_income_serving",
    ]


def test_downstream_exception_preserves_completed_stage_evidence(monkeypatch) -> None:
    _wire(monkeypatch)

    def boom(*_args, **_kwargs):
        raise RuntimeError("analytics build failed")

    result = chain.run(
        "db", current_publication=lambda _conn: ("publication-1", "run-1"),
        identity_refresher=lambda _dsn: {"refreshed": True, "bootstrap": False},
        identity_probe=lambda _dsn: _fresh(), downstream_runner=boom,
    )

    assert result["aborted"] is True
    assert result["blocked_dependency"] == "fixed_income_serving_failed"
    assert result["error"] == "analytics build failed"
    assert [stage["name"] for stage in result["stages"]] == [
        "sec_nport_holdings_v2",
        "nport_holdings_snapshot_identity_v1",
        "nport_holdings_identity_freshness",
    ]


def test_pointer_change_after_identity_proof_blocks_downstream(monkeypatch) -> None:
    _wire(monkeypatch)
    sources = iter((("publication-1", "run-1"), ("publication-2", "run-2")))
    called = False

    def downstream(*_args, **_kwargs):
        nonlocal called
        called = True
        return {"state": "published"}

    result = chain.run(
        "db", current_publication=lambda _conn: next(sources),
        identity_refresher=lambda _dsn: {"refreshed": True, "bootstrap": False},
        identity_probe=lambda _dsn: _fresh(), downstream_runner=downstream,
    )

    assert result["aborted"] is True
    assert result["blocked_dependency"] == "current_v2_pointer_changed_before_downstream"
    assert called is False


def test_outer_lock_contention_is_visible(monkeypatch) -> None:
    monkeypatch.setattr(chain, "connect", lambda _dsn, **_kwargs: _Conn())

    @contextmanager
    def busy(_conn, _lock_id):
        yield False

    monkeypatch.setattr(chain, "advisory_lock", busy)
    result = chain.run("db")
    assert result == {
        "published": False,
        "stages": [],
        "skipped": "lock_busy",
        "blocked_dependency": "nport_v2_publication_chain",
    }


def test_rerun_reuses_current_publication_and_allows_downstream_idempotency(monkeypatch) -> None:
    _wire(monkeypatch)
    downstream_states = iter(("published", "already_published"))

    def run_once() -> dict:
        return chain.run(
            "db", current_publication=lambda _conn: ("publication-1", "run-1"),
            identity_refresher=lambda _dsn: {"refreshed": True, "bootstrap": False},
            identity_probe=lambda _dsn: _fresh(),
            downstream_runner=lambda *_args, **_kwargs: {"state": next(downstream_states)},
        )

    first, second = run_once(), run_once()
    assert first["published"] is True
    assert second["published"] is True
    assert first["source_publication_id"] == second["source_publication_id"] == "publication-1"
    assert second["downstream"]["state"] == "already_published"


def test_lookthrough_schema_refuses_in_place_populated_conversion() -> None:
    schema = (Path(__file__).resolve().parents[1] / "schemas" / "nport_lookthrough.sql").read_text(
        encoding="utf-8"
    )

    assert "migrate_data" not in schema
    assert schema.count("refusing to convert populated") == 2
    assert schema.count("bounded backfill, and cutover") == 2
