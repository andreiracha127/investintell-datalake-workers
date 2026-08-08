"""Incremental materialization contracts for stock fundamentals statements."""

from __future__ import annotations

import datetime as dt
import tomllib
from contextlib import contextmanager
from pathlib import Path

import pytest

import src.workers.stock_fundamentals_statements as statements


ROOT = Path(__file__).resolve().parents[1]


class _Conn:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0
        self.events: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def commit(self) -> None:
        self.commits += 1
        self.events.append("commit")

    def rollback(self) -> None:
        self.rollbacks += 1


def _fact(
    identity: str,
    fingerprint: str,
    cik: int = 320193,
    *,
    period_start: dt.date | None = dt.date(2025, 1, 1),
    period_end: dt.date = dt.date(2025, 12, 31),
) -> statements.SourceFact:
    return statements.SourceFact(
        identity=identity,
        fingerprint=fingerprint,
        cik=cik,
        accession="0000320193-26-000001",
        period_start=period_start,
        period_end=period_end,
    )


def _wire_run(monkeypatch, source, watermarks, *, acquired=True, recompute=None):
    conn = _Conn()
    applied: list[statements.ChangePlan] = []
    quarantined: list[statements.SourceFact] = []

    @contextmanager
    def _lock(_conn, _lock_id):
        yield acquired

    monkeypatch.setattr(statements, "connect", lambda _dsn: conn)
    monkeypatch.setattr(statements, "advisory_lock", _lock)
    monkeypatch.setattr(statements, "install_schema", lambda _conn: None)
    monkeypatch.setattr(statements, "load_source_facts", lambda _conn: source)
    monkeypatch.setattr(statements, "load_watermarks", lambda _conn: watermarks)
    monkeypatch.setattr(statements, "target_is_empty", lambda _conn: False)
    monkeypatch.setattr(statements, "load_universe_constituents", lambda _conn: [])
    monkeypatch.setattr(statements, "load_universe_watermarks", lambda _conn: {})
    monkeypatch.setattr(
        statements,
        "quarantine_invalid_facts",
        lambda _conn, facts: quarantined.extend(facts),
    )
    monkeypatch.setattr(
        statements,
        "apply_watermark_changes",
        lambda _conn, plan: (applied.append(plan), conn.events.append("watermarks")),
    )
    monkeypatch.setattr(statements, "record_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        statements,
        "recompute_scoped",
        recompute or (lambda _conn, ciks: (len(ciks), len(ciks) * 2)),
    )
    return conn, applied, quarantined


def test_empty_first_install_bootstraps_from_semantic_source_mv(monkeypatch) -> None:
    conn, applied, _ = _wire_run(monkeypatch, [_fact("fact-a", "v1")], {})
    monkeypatch.setattr(statements, "target_is_empty", lambda _conn: True)
    monkeypatch.setattr(
        statements,
        "recompute_scoped",
        lambda *_args: pytest.fail("first install must not recompute the 24 GB source"),
    )
    monkeypatch.setattr(statements, "bootstrap_from_source_mv", lambda _conn: 7)

    result = statements.run("postgres://test")

    assert result["rows_deleted"] == 0
    assert result["rows_upserted"] == 7
    assert applied
    assert conn.events == ["commit", "watermarks", "commit"]


def test_first_run_materializes_new_fact_and_advances_its_watermark(monkeypatch) -> None:
    conn, applied, quarantined = _wire_run(monkeypatch, [_fact("fact-a", "v1")], {})

    result = statements.run("postgres://test")

    assert result["changed_facts"] == 1
    assert result["affected_ciks"] == 1
    assert result["rows_deleted"] == 1
    assert result["rows_upserted"] == 2
    assert [fact.identity for fact in applied[0].upserts] == ["fact-a"]
    assert not quarantined
    assert conn.commits == 2
    assert conn.events == ["commit", "watermarks", "commit"]


def test_unchanged_source_is_a_noop(monkeypatch) -> None:
    source = [_fact("fact-a", "v1")]
    watermarks = {"fact-a": statements.Watermark(fingerprint="v1", cik=320193)}
    conn, applied, _ = _wire_run(monkeypatch, source, watermarks)

    result = statements.run("postgres://test")

    assert result["changed_facts"] == 0
    assert result["affected_ciks"] == 0
    assert result["skipped"] == "no_changes"
    assert applied == []
    assert conn.commits == 0


def test_changed_and_removed_facts_recompute_their_ciks_and_cleanup_watermarks(monkeypatch) -> None:
    source = [_fact("fact-a", "v2", cik=1)]
    watermarks = {
        "fact-a": statements.Watermark(fingerprint="v1", cik=1),
        "fact-removed": statements.Watermark(fingerprint="old", cik=2),
    }
    _, applied, _ = _wire_run(monkeypatch, source, watermarks)

    result = statements.run("postgres://test")

    assert result["changed_facts"] == 2
    assert result["affected_ciks"] == 2
    assert [fact.identity for fact in applied[0].upserts] == ["fact-a"]
    assert applied[0].deletes == ("fact-removed",)


def test_universe_ticker_reassignment_recomputes_both_ciks_without_a_fact_change() -> None:
    """A ticker moving CIK must refresh its old and new statement rows."""
    current = [
        statements.UniverseConstituent(ticker="ACME", fingerprint="new-cik", cik=2),
    ]
    prior = {
        statements.universe_identity("ACME", 1): statements.UniverseWatermark(
            fingerprint="old-cik", cik=1
        ),
    }

    plan = statements.plan_changes([], {}, universe=current, universe_watermarks=prior)

    assert plan.upserts == ()
    assert plan.universe_upserts == tuple(current)
    assert plan.universe_deletes == (statements.universe_identity("ACME", 1),)
    assert plan.affected_ciks == (1, 2)


def test_run_advances_universe_state_only_after_recomputing_unchanged_facts(monkeypatch) -> None:
    source = [_fact("fact-a", "v1", cik=2)]
    watermarks = {"fact-a": statements.Watermark(fingerprint="v1", cik=2)}
    conn, applied, _ = _wire_run(monkeypatch, source, watermarks)
    monkeypatch.setattr(
        statements,
        "load_universe_constituents",
        lambda _conn: [statements.UniverseConstituent(ticker="ACME", fingerprint="new-cik", cik=2)],
    )
    monkeypatch.setattr(
        statements,
        "load_universe_watermarks",
        lambda _conn: {
            statements.universe_identity("ACME", 1): statements.UniverseWatermark(
                fingerprint="old-cik", cik=1
            )
        },
    )

    result = statements.run("postgres://test")

    assert result["changed_facts"] == 0
    assert result["changed_universe_constituents"] == 2
    assert result["affected_ciks"] == 2
    assert applied[0].universe_deletes == (statements.universe_identity("ACME", 1),)
    assert conn.events == ["commit", "watermarks", "commit"]


def test_impossible_source_dates_are_quarantined_and_never_enter_the_watermark(monkeypatch) -> None:
    invalid = _fact("fact-6016", "v1", period_end=dt.date(6016, 12, 31))
    conn, applied, quarantined = _wire_run(monkeypatch, [invalid], {})

    result = statements.run("postgres://test")

    assert result["quarantined_facts"] == 1
    assert result["affected_ciks"] == 0
    assert [fact.identity for fact in quarantined] == ["fact-6016"]
    assert applied == []
    assert conn.commits == 1


def test_failure_rolls_back_before_any_watermark_can_advance(monkeypatch) -> None:
    def _fail(_conn, _ciks):
        raise RuntimeError("scoped materialization failed")

    conn, applied, _ = _wire_run(monkeypatch, [_fact("fact-a", "v1")], {}, recompute=_fail)

    try:
        statements.run("postgres://test")
    except RuntimeError as exc:
        assert str(exc) == "scoped materialization failed"
    else:
        raise AssertionError("materialization failure must propagate")

    assert applied == []
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_lock_contention_returns_without_schema_or_state_work(monkeypatch) -> None:
    conn, applied, _ = _wire_run(monkeypatch, [_fact("fact-a", "v1")], {}, acquired=False)

    result = statements.run("postgres://test")

    assert result == {
        "affected_ciks": 0,
        "changed_facts": 0,
        "rows_deleted": 0,
        "rows_upserted": 0,
        "skipped": "lock_busy",
    }
    assert applied == []
    assert conn.commits == 0


def test_scoped_recompute_filters_impossible_periods_from_mv_definition() -> None:
    source = Path(statements.__file__).read_text(encoding="utf-8")
    assert "period_end >= DATE '1900-01-01'" in source
    assert "period_end <= CURRENT_DATE + INTERVAL '1 year'" in source


def test_scoped_definition_rebinds_the_universe_before_the_multiuse_fact_cte() -> None:
    definition = """
        WITH uni AS (
            SELECT DISTINCT upper(universe_constituents.ticker::text) AS ticker,
                universe_constituents.cik
            FROM universe_constituents
            WHERE universe_constituents.cik IS NOT NULL
        ) SELECT * FROM uni
    """

    scoped = statements.scope_definition_to_changed_universe(definition)

    assert "FROM stock_fundamentals_statements_scope_universe" in scoped
    assert "universe_constituents" not in scoped
    with pytest.raises(RuntimeError, match="unexpected universe relation"):
        statements.scope_definition_to_changed_universe(
            "SELECT * FROM universe_constituents JOIN universe_constituents u2 ON true"
        )


def test_railway_recurring_run_precedes_snapshot_refresh_without_healthcheck() -> None:
    config = tomllib.loads(
        (ROOT / "railway.stock-fundamentals-statements.toml").read_text(encoding="utf-8")
    )

    assert config["deploy"] == {
        "startCommand": "python -m src.run_worker",
        "restartPolicyType": "never",
        "cronSchedule": "0 7 * * *",
    }
