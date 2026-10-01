"""Latest-month guard: a dark last closed month is reported by plan() and refused by run().

Real pure build on small synthetic panels; fake connections; no DSN, no DB.
The guard changes no row, policy or digest -- only whether run() writes.
"""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from src.bonds import implied_rating as policy
from src.bonds import implied_rating_replay as replay
from src.bonds.implied_rating_build import (
    build_payload_from_snapshot,
    latest_month_witnessed_count,
)
from src.workers import bond_market_implied_rating as worker

MONTHS = ("2026-05-01", "2026-06-01", "2026-07-01", "2026-08-01")
PARENT = {
    "publication_id": "panel-current",
    "first_month": date(2026, 5, 1),
    "last_closed_month": date(2026, 8, 1),
    "open_month": date(2026, 9, 1),
}
REVISION = "guard-revision"


def _snapshot(*, dark_last_month: bool) -> pd.DataFrame:
    rows = []
    for cusip, spread in (("AAA000001", 90.0), ("BBB000002", 450.0)):
        for month in MONTHS:
            dark = dark_last_month and month == MONTHS[-1]
            rows.append({
                "cusip_id": cusip, "month": pd.Timestamp(month), "price": 95.0,
                "spread_final_bps": spread, "mod_dur": 5.0,
                "trade_count": 0 if dark else 10,  # below n_min => unwitnessed
                "dollar_volume": 1_000_000.0, "maturity_date": date(2035, 1, 1),
            })
    return pd.DataFrame(rows)


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def commit(self):
        pass

    def execute(self, sql, params=None):
        return self

    def fetchone(self):
        return None


def _patch(monkeypatch, snapshot):
    events = []
    monkeypatch.setattr(worker, "connect", lambda _dsn: _FakeConnection())
    monkeypatch.setattr(worker, "resolve_dsn", lambda _dsn: "postgresql://example")
    monkeypatch.setattr(worker, "_code_revision", lambda: REVISION)
    monkeypatch.setenv("BOND_IMPLIED_RATING_FORCE_REPUBLISH", "1")
    monkeypatch.setattr(worker, "_relation_exists", lambda _conn, name: True)
    monkeypatch.setattr(worker, "_current_panel", lambda _conn: dict(PARENT))
    monkeypatch.setattr(worker, "_current_pointer", lambda _conn: None)
    monkeypatch.setattr(worker, "_mirror_serves_panel", lambda _conn, **kwargs: True)
    monkeypatch.setattr(worker, "current_pinned_anchor", lambda _conn: None)
    monkeypatch.setattr(worker, "_read_snapshot", lambda _conn, **kwargs: snapshot)
    monkeypatch.setattr(worker, "install_schema", lambda _conn: events.append("install_schema"))

    def materialize(_conn, publication, rows, *, expected_pointer):
        events.append("materialize")
        return SimpleNamespace(publication_id=publication.publication_id, row_count=len(rows), reused=False)

    monkeypatch.setattr(worker, "materialize", materialize)
    return events


def test_pure_count_is_the_witnessed_rows_of_the_last_closed_month():
    for dark in (False, True):
        snapshot = _snapshot(dark_last_month=dark)
        rows = policy.build_publication_rows(snapshot, last_closed_month=PARENT["last_closed_month"])
        count = latest_month_witnessed_count(rows, month=PARENT["last_closed_month"])
        assert count == (0 if dark else 2)
        built = build_payload_from_snapshot(
            snapshot, last_closed_month=PARENT["last_closed_month"], revision=REVISION,
            panel_publication_id=PARENT["publication_id"],
            input_fingerprint=policy.snapshot_fingerprint(snapshot),
        )
        assert built["latest_month_witnessed_count"] == count
    assert latest_month_witnessed_count(pd.DataFrame(), month=PARENT["last_closed_month"]) == 0


def test_plan_reports_the_count_and_run_refuses_a_dark_last_month(monkeypatch):
    snapshot = _snapshot(dark_last_month=True)
    events = _patch(monkeypatch, snapshot)
    planned = worker.plan("postgresql://example")
    assert planned["state"] == "planned"
    assert planned["latest_month_witnessed_count"] == 0
    assert planned["last_month"] == PARENT["last_closed_month"].isoformat()

    result = worker.run("postgresql://example")
    assert result["state"] == "latest_month_unwitnessed"
    assert result["reason"] == "implied_rating_latest_month_unwitnessed"
    assert result["aborted"] is True
    assert result["input_reasons"] == ["latest_month_unwitnessed"]
    assert result["latest_month_witnessed_count"] == 0
    assert result["panel_last_closed_month"] == PARENT["last_closed_month"].isoformat()
    # The refusal names exactly the publication it declined to write.
    assert result["publication_id"] == planned["publication_id"]
    assert result["rows_digest"] == planned["rows_digest"]
    assert result["input_fingerprint"] == planned["input_fingerprint"]
    assert result["row_count"] == planned["row_count"]
    assert "materialize" not in events
    assert events == ["install_schema"]  # the (idempotent) DDL replay precedes the build, as before


def test_run_publishes_a_witnessed_last_month_with_the_count(monkeypatch):
    snapshot = _snapshot(dark_last_month=False)
    events = _patch(monkeypatch, snapshot)
    result = worker.run("postgresql://example")
    assert result["state"] == "published_no_defaults"
    assert result["latest_month_witnessed_count"] == 2
    assert events == ["install_schema", "materialize"]


def test_guard_changes_no_row_policy_or_digest(monkeypatch):
    """The dark-month build is byte-identical with and without the guard: it only refuses."""
    snapshot = _snapshot(dark_last_month=True)
    _patch(monkeypatch, snapshot)
    planned = worker.plan("postgresql://example")
    rows = policy.build_publication_rows(snapshot, last_closed_month=PARENT["last_closed_month"])
    assert policy.rows_digest(rows) == planned["rows_digest"]
    assert planned["policy_digest"] == policy.POLICY_DIGEST
    assert planned["row_count"] == len(rows)
    assert planned["input_fingerprint"] == policy.snapshot_fingerprint(snapshot)


def test_determinism_receipt_reports_the_latest_month_witnessed_count(monkeypatch, tmp_path):
    snapshot = _snapshot(dark_last_month=True)
    monkeypatch.setattr(replay, "read_only_connect", lambda dsn, **kwargs: _FakeConnection())
    monkeypatch.setattr(worker, "_code_revision", lambda: REVISION)
    monkeypatch.setattr(worker, "_relation_exists", lambda _conn, name: True)
    monkeypatch.setattr(worker, "_current_panel", lambda _conn: dict(PARENT))
    monkeypatch.setattr(worker, "_current_pointer", lambda _conn: None)
    monkeypatch.setattr(worker, "_mirror_serves_panel", lambda _conn, **kwargs: True)
    monkeypatch.setattr(worker, "_read_snapshot", lambda _conn, **kwargs: snapshot)
    monkeypatch.setattr(worker, "_pointer_build", lambda _conn, p: None)
    monkeypatch.setattr(replay, "current_pinned_anchor", lambda _conn: None)
    monkeypatch.setattr(worker, "materialize", lambda *a, **k: pytest.fail("write"))
    code, receipt = replay.determinism_check("postgresql://example", work_dir=tmp_path)
    assert code == 0, receipt
    assert receipt["verdict"] == "deterministic"  # a dark month is deterministic; publishing it is what run() refuses
    assert receipt["latest_month_witnessed_count"] == 0
    assert all(child["latest_month_witnessed_count"] == 0 for child in receipt["children"])
