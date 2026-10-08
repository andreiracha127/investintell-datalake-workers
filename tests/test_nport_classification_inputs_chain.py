"""Dependencies are retried in order; a scheduled refresh is not completion."""

from __future__ import annotations

import datetime as dt
import re
from contextlib import contextmanager
from pathlib import Path

from src import db
from src.workers import _fund_pipeline_freshness as freshness
from src.workers import nport_classification_inputs_chain as chain


def _patch(monkeypatch, cagg_alarms):
    calls = []
    source = freshness.SourceCohort(
        dt.date(2026, 7, 31), dt.date(2026, 8, 31), dt.date(2026, 5, 1),
        verdict={"alarm": False},
    )

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, *_args):
            calls.append("request")
            return self

        def fetchone(self):
            return (1078,)

    @contextmanager
    def lock(*_a):
        yield True

    monkeypatch.setattr(chain, "connect", lambda *_a, **_k: Conn())
    monkeypatch.setattr(chain, "advisory_lock", lock)
    monkeypatch.setattr(chain.freshness, "read_source_cohort", lambda *_a, **_k: source)
    alarms = iter(cagg_alarms)

    def probe(_conn, _source, stage):
        calls.append(stage)
        return {"stage": stage, "alarm": next(alarms) if stage == "cagg" else False}

    monkeypatch.setattr(chain.freshness, "probe_stage", probe)
    return source, calls


def test_chain_waits_for_cagg_proof_before_characteristics_and_lookthrough(monkeypatch):
    _source, calls = _patch(monkeypatch, [True, True, False])

    def characteristics(*_a, **_k):
        calls.append("compute_characteristics")
        return {"status": "succeeded"}

    def lookthrough(*_a, **_k):
        calls.append("compute_lookthrough")
        return {"status": "complete", "published": True}

    stats = chain.run("unused", characteristics_runner=characteristics,
                      lookthrough_runner=lookthrough, sleeper=lambda _s: None)
    assert stats["state"] == "complete"
    assert calls.index("request") < calls.index("compute_characteristics")
    assert calls.index("compute_characteristics") < calls.index("compute_lookthrough")
    assert calls[:4] == ["cagg", "request", "cagg", "cagg"]


def test_pending_owner_refresh_does_not_run_or_publish_downstream(monkeypatch):
    _source, calls = _patch(monkeypatch, [True] * 20)
    stats = chain.run("unused", characteristics_runner=lambda *_a, **_k: calls.append("bad"),
                      lookthrough_runner=lambda *_a, **_k: calls.append("bad"),
                      sleeper=lambda _s: None)
    assert stats["state"] == "blocked"
    assert stats["reason"] == "cagg_refresh_pending"
    assert stats["last_good_preserved"] is True
    assert "bad" not in calls


def test_characteristics_failure_stops_before_lookthrough(monkeypatch):
    _source, calls = _patch(monkeypatch, [False])
    stats = chain.run("unused", characteristics_runner=lambda *_a, **_k: {"status": "partial"},
                      lookthrough_runner=lambda *_a, **_k: calls.append("bad"),
                      sleeper=lambda _s: None)
    assert stats["state"] == "blocked"
    assert "bad" not in calls


def test_stale_source_cannot_trigger_cagg_or_downstream_work(monkeypatch):
    source, calls = _patch(monkeypatch, [])
    source.verdict = {"alarm": True, "breaches": ["SOURCE_REPORT_STALE"]}
    stats = chain.run("unused", characteristics_runner=lambda *_a, **_k: calls.append("bad"),
                      lookthrough_runner=lambda *_a, **_k: calls.append("bad"))
    assert stats["state"] == "blocked"
    assert calls == []


def test_cagg_request_lock_is_registered_and_not_shared():
    sql = (Path(__file__).resolve().parents[1]
           / "schemas" / "nport_series_profile_refresh_request_v1.sql").read_text(encoding="utf-8")
    assert re.findall(r"advisory_xact_lock\((\d+)::bigint\)", sql) == [
        str(db.LOCK_NPORT_SERIES_PROFILE_REFRESH_REQUEST)]
    ids = [v for k, v in vars(db).items() if k.startswith("LOCK_") and isinstance(v, int)]
    assert ids.count(db.LOCK_NPORT_SERIES_PROFILE_REFRESH_REQUEST) == 1
    assert ids.count(db.LOCK_NPORT_CLASSIFICATION_INPUTS_CHAIN) == 1
