"""A failed look-through candidate cannot change the serving materialization."""

from __future__ import annotations

import datetime as dt
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from src import db
from src.workers import _fund_pipeline_freshness as freshness
from src.workers import nport_lookthrough as worker

READ_SOURCE_COHORT = freshness.read_source_cohort


class Conn:
    load_running = False

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def rollback(self):
        pass

    def commit(self):
        pass

    def execute(self, query, params=()):
        """Promotion's statements; a running load holds LOCK_NPORT_LOAD."""
        if "pg_try_advisory_xact_lock" in query:
            assert params == (db.LOCK_NPORT_LOAD,)
            self.result = [(not self.load_running,)]
        return self

    def fetchone(self):
        return self.result[0]


class Lake(Conn):
    """Grouped sec_nport_holdings rows and cagg profiles, filtered as the SQL bounds them."""

    def __init__(self, raw, profiles):
        self.raw, self.profiles = raw, profiles

    def cursor(self):
        return self

    def execute(self, query, params=()):
        if "advisory" in query or "ISOLATION" in query:
            return super().execute(query, params)
        if "to_regclass" in query:
            self.result = [(True, True)]
            return self
        if "max(report_date)" in query:
            self.result = [(max((row[1] for row in self.raw), default=None),)]
        else:
            low, high = params
            rows = self.profiles if "cagg_nport_series_profile" in query else self.raw
            self.result = [row for row in rows if low <= row[1] <= high]
        return self

    def fetchall(self):
        return self.result


def _patch(monkeypatch):
    source = freshness.SourceCohort(
        dt.date(2026, 7, 31), dt.date(2026, 8, 31), dt.date(2026, 5, 1),
        series={"S1": freshness.Observation("S1", dt.date(2026, 7, 31), 4)},
        verdict={"alarm": False},
    )

    @contextmanager
    def lock(*_args):
        yield True

    monkeypatch.setattr(worker, "connect", lambda *_a: Conn())
    monkeypatch.setattr(worker, "advisory_lock", lock)
    monkeypatch.setattr(worker, "ensure_schema", lambda *_a: None, raising=False)
    monkeypatch.setattr(worker, "inputs", SimpleNamespace(
        snapshot=lambda *_a: {}, guard=lambda *_a: None, certify=lambda *_a: None,
    ), raising=False)
    monkeypatch.setattr(worker, "_cleanup_orphan_candidates", lambda *_a: None)
    monkeypatch.setattr(worker.freshness, "read_source_cohort", lambda *_a, **_k: source)
    monkeypatch.setattr(worker.freshness, "probe_stage", lambda _c, _s, stage: {
        "alarm": stage == "lookthrough", "matched_series_count": 0, "expected_series_count": 1,
    })
    monkeypatch.setattr(worker.nport_identifier_coverage, "probe", lambda *_a, **_k: {"state": "clean"})
    monkeypatch.setattr(worker, "build_fund_map", lambda *_a: {})
    monkeypatch.setattr(worker, "build_sector_map", lambda *_a: {})
    published, cleaned = [], []
    monkeypatch.setattr(worker, "_publish_staged", lambda *_a: published.append(True))
    monkeypatch.setattr(worker, "_cleanup_staged", lambda *_a: cleaned.append(True))
    return source, published, cleaned


def test_shard_failure_preserves_prior_serving_output(monkeypatch):
    _source, published, cleaned = _patch(monkeypatch)

    def shard(*_args):
        raise RuntimeError("failed second parent after staging first")

    monkeypatch.setattr(worker, "_process_shard", shard)
    with pytest.raises(RuntimeError, match="failed second parent"):
        worker.run("unused", serial=True)
    assert published == []
    assert cleaned == [True]


def test_insufficient_candidate_is_not_published(monkeypatch):
    _source, published, cleaned = _patch(monkeypatch)
    monkeypatch.setattr(worker, "_process_shard", lambda *_a: (1, 1, 3))
    monkeypatch.setattr(worker, "_probe_staged", lambda *_a: {
        "alarm": True, "breaches": ["DERIVED_COHORT_COVERAGE_BELOW_FLOOR"],
    })
    with pytest.raises(freshness.FundPipelineBlocked):
        worker.run("unused", serial=True)
    assert published == []
    assert cleaned == [True]


def test_valid_candidate_publishes_once_after_source_recheck(monkeypatch):
    source, published, _cleaned = _patch(monkeypatch)
    cutoffs = []

    def shard(_dsn, cutoff, _map, _sectors, parents, run_id):
        cutoffs.append((cutoff, parents, run_id))
        return 1, 1, 3

    monkeypatch.setattr(worker, "_process_shard", shard)
    monkeypatch.setattr(worker, "_probe_staged", lambda *_a: {"alarm": False})
    stats = worker.run("unused", serial=True)
    assert published == [True]
    assert cutoffs[0][0] == source.as_of.isoformat()
    assert cutoffs[0][1] == ["S1"]
    assert cutoffs[0][2]
    assert stats["published"] is True


@pytest.mark.parametrize("newer_month_loads", [False, True])
def test_chain_capped_rebuild_blocks_when_a_newer_month_loads_before_promotion(
    monkeypatch, newer_month_loads,
):
    _source, published, cleaned = _patch(monkeypatch)
    today = freshness.utc_today()
    anchor = freshness.month_start(today, -1) - dt.timedelta(days=1)
    loaded = dt.datetime.now(dt.UTC) - dt.timedelta(days=10)
    series = [f"S{i:04d}" for i in range(1000)]
    lake = Lake([(sid, anchor, 4, loaded, True) for sid in series],
                [(sid, freshness.month_start(anchor, -2) - dt.timedelta(days=1), 4) for sid in series])
    monkeypatch.setattr(worker, "connect", lambda *_a: lake)
    monkeypatch.setattr(worker.freshness, "read_source_cohort", READ_SOURCE_COHORT)
    monkeypatch.setattr(worker, "_probe_staged", lambda *_a: {"alarm": False})
    source = READ_SOURCE_COHORT(lake)  # the chain's read, before its upstream stages

    def shard(*_args):
        if newer_month_loads:
            newer = freshness.month_start(today) - dt.timedelta(days=1)
            lake.raw += [(sid, newer, 4, loaded + dt.timedelta(days=9), True) for sid in series]
        return 1, 1, 3

    monkeypatch.setattr(worker, "_process_shard", shard)
    def run():  # capped at the anchor, exactly as the chain calls it
        return worker.run("unused", serial=True, calc_date=str(source.as_of), source=source)

    if not newer_month_loads:
        assert run()["published"] is True
        return
    with pytest.raises(freshness.FundPipelineBlocked) as blocked:
        run()
    assert blocked.value.verdict["breaches"] == ["SOURCE_CHANGED_DURING_BUILD"]
    assert published == []
    assert cleaned == [True]


def test_a_running_load_blocks_promotion_and_keeps_last_good(monkeypatch):
    _source, published, cleaned = _patch(monkeypatch)
    conn = Conn()
    conn.load_running = True
    monkeypatch.setattr(worker, "connect", lambda *_a: conn)
    monkeypatch.setattr(worker, "_process_shard", lambda *_a: (1, 1, 3))
    monkeypatch.setattr(worker, "_probe_staged", lambda *_a: {"alarm": False})
    with pytest.raises(freshness.FundPipelineBlocked) as blocked:
        worker.run("unused", serial=True)
    assert blocked.value.verdict["breaches"] == ["SOURCE_LOAD_IN_PROGRESS"]
    assert published == []
    assert cleaned == [True]


def test_degraded_identifier_inputs_fail_before_schema_or_shards(monkeypatch):
    _source, published, _cleaned = _patch(monkeypatch)
    monkeypatch.setattr(worker.nport_identifier_coverage, "probe", lambda *_a, **_k: {"state": "degraded"})
    monkeypatch.setattr(worker, "ensure_schema", lambda *_a: pytest.fail("schema write"))
    with pytest.raises(freshness.FundPipelineBlocked):
        worker.run("unused", serial=True)
    assert published == []


def test_explicit_mapping_repair_rebuilds_a_fully_matching_raw_cohort(monkeypatch):
    _source, published, _cleaned = _patch(monkeypatch)
    monkeypatch.setattr(worker.freshness, "probe_stage", lambda *_a: {
        "alarm": False, "matched_series_count": 1, "expected_series_count": 1,
    })
    monkeypatch.setattr(worker, "_process_shard", lambda *_a: (1, 1, 3))
    monkeypatch.setattr(worker, "_probe_staged", lambda *_a: {"alarm": False})
    stats = worker.run("unused", serial=True, force_rebuild=True)
    assert published == [True]
    assert stats["published"] is True


def test_expired_child_is_kept_as_an_explicit_unexpanded_fund_residual():
    root_day = dt.date(2026, 7, 31)
    root = {"cusip": "123456789", "issuer_name": "Child fund", "asset_class": "EC",
            "currency": "USD", "pct_of_nav": 100.0}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, _sql, params):
            self.series = params[0]

        def fetchone(self):
            return (root_day if self.series == "ROOT" else dt.date(2026, 1, 31),)

        def fetchall(self):
            assert self.series == "ROOT", "expired child holdings must never be read"
            return [tuple(root.get(column) for column in worker.HOLDING_COLS)]

    class Database:
        def cursor(self):
            return Cursor()

    get_holdings = worker.make_db_get_holdings(
        Database(), root_day, minimum_report_date=dt.date(2026, 4, 10),
    )
    _exposures, summary = worker.expand_series(
        "ROOT", get_holdings, {"cusip": {"123456789": "OLD_CHILD"}, "isin": {}},
    )
    assert summary["nondecomposable_fund_pct"] == 100
    assert summary["n_children_expanded"] == 0
    assert summary["oldest_report_date"] == root_day


def test_first_lookthrough_probe_follows_schema_installation(monkeypatch):
    _source, _published, _cleaned = _patch(monkeypatch)
    installed = []
    monkeypatch.setattr(worker, "ensure_schema", lambda *_a: installed.append(True))
    def probe(_conn, _source, stage):
        if stage == "lookthrough":
            assert installed, "fresh database has no lookthrough tables yet"
        return {"alarm": False, "matched_series_count": 1, "expected_series_count": 1}
    monkeypatch.setattr(worker.freshness, "probe_stage", probe)
    assert worker.run("unused", serial=True)["status"] == "current"
