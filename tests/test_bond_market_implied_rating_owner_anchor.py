"""Actual fixed-authoritative policy, independent of legacy mechanics fixtures.

Small synthetic panels and fake connections only; no DB, DSN/env credentials,
production artifact or scientific replay is involved.
"""
from __future__ import annotations

import math
from datetime import date
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.bonds import implied_rating as policy
from src.bonds.errors import BondError
from src.bonds.implied_rating_materializer import publication_id_for
from src.workers import bond_market_implied_rating as worker

OFFICIAL_ANCHOR = -0.8864114120812487
OFFICIAL_DECIMAL = "-0.8864114120812487"
NEW_POLICY_DIGEST = "4b752a3fc5d5222b398f1e2a073b7c428b068e77968f8fb79edd20957202a08a"


def panel(months=("2026-06-01", "2026-07-01", "2026-08-01"), spreads=None):
    values = spreads if spreads is not None else [100.0] * len(months)
    return pd.DataFrame([
        {
            "cusip_id": "SYNTH0001", "month": pd.Timestamp(month), "price": 95.0,
            "spread_final_bps": spread, "mod_dur": 5.0, "trade_count": 10,
            "dollar_volume": 1_000_000.0, "maturity_date": date(2035, 1, 1),
        }
        for month, spread in zip(months, values, strict=True)
    ])


class FakeConnection:
    def __init__(self):
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def commit(self):
        self.commits += 1


def stub_worker(monkeypatch, snapshot, *, previous_anchor=OFFICIAL_ANCHOR):
    captured = {}
    conn = FakeConnection()
    parent = {
        "publication_id": "synthetic-panel", "first_month": snapshot["month"].min().date(),
        "last_closed_month": snapshot["month"].max().date(), "open_month": None,
    }
    pointer = None if previous_anchor is None else "synthetic-old-pointer"
    monkeypatch.setattr(worker, "resolve_dsn", lambda _dsn: "postgresql://synthetic")
    monkeypatch.setattr(worker, "connect", lambda _dsn: conn)
    monkeypatch.setattr(worker, "_code_revision", lambda: "fixed-anchor-test-revision")
    monkeypatch.setattr(worker, "_relation_exists", lambda *args: True)
    monkeypatch.setattr(worker, "_current_panel", lambda _conn: dict(parent))
    monkeypatch.setattr(worker, "_current_pointer", lambda _conn: pointer)
    monkeypatch.setattr(worker, "_mirror_serves_panel", lambda *args, **kwargs: True)
    monkeypatch.setattr(worker, "_currentness", lambda *args, **kwargs: (None, ["policy_digest_changed"]))
    monkeypatch.setattr(worker, "_read_snapshot", lambda *args, **kwargs: snapshot.copy())
    monkeypatch.setattr(worker, "current_pinned_anchor", lambda _conn: previous_anchor)
    monkeypatch.setattr(worker, "install_schema", lambda _conn: None)
    monkeypatch.delenv("BOND_IMPLIED_RATING_FORCE_REPUBLISH", raising=False)

    def materialize(_conn, publication, rows, *, expected_pointer):
        captured.update(publication=publication, rows=rows, expected_pointer=expected_pointer)
        return SimpleNamespace(
            publication_id=publication.publication_id, row_count=publication.row_count, reused=False
        )

    monkeypatch.setattr(worker, "materialize", materialize)
    return captured


def test_policy_canonically_binds_the_exact_official_decimal():
    assert policy.POLICY["market_level"]["anchor"]["kind"] == "fixed_authoritative"
    assert policy.POLICY["market_level"]["anchor"]["l_anchor"] == OFFICIAL_DECIMAL
    assert policy.policy_l_anchor() == OFFICIAL_ANCHOR
    assert policy.POLICY_DIGEST == NEW_POLICY_DIGEST


@pytest.mark.parametrize("measured", [OFFICIAL_ANCHOR, 0.0, 2.0, 1e6])
def test_corrected_median_never_replaces_or_invalidates_official_anchor(measured):
    levels = pd.Series([measured], index=pd.to_datetime(["2026-08-01"]))
    assert policy.market_anchor(levels) == OFFICIAL_ANCHOR
    assert policy.resolved_market_anchor(levels) == measured


def test_recomputed_median_changes_after_data_correction_without_collapsing_ratings():
    before = panel(spreads=[100.0, 100.0, 100.0])
    after = panel(spreads=[100.0, 130.0, 150.0])
    end = date(2026, 8, 1)
    old = policy.market_anchor_diagnostics_for_snapshot(before, last_closed_month=end)
    corrected = policy.market_anchor_diagnostics_for_snapshot(after, last_closed_month=end)
    assert old.l_anchor == corrected.l_anchor == OFFICIAL_ANCHOR
    assert old.resolved_l_anchor == 0.0
    assert corrected.resolved_l_anchor == pytest.approx(math.log(1.3))
    rows = policy.build_publication_rows(after, last_closed_month=end)
    assert len(rows) == 3
    assert rows["witnessed"].all()
    assert rows["neutralized_score"].notna().all()
    assert rows["implied_bucket"].isin(policy.RATED_BUCKETS).all()


def test_buckets_and_scores_are_unchanged_when_old_measured_anchor_equals_pin(monkeypatch):
    snapshot = panel(months=("2023-08-01", "2023-09-01", "2023-10-01"))
    # Explicit known chain: its first observed level is zero; frozen-window
    # levels equal the old pin exactly, avoiding environment float roundoff.
    levels = pd.Series([0.0, OFFICIAL_ANCHOR, OFFICIAL_ANCHOR], index=snapshot["month"])
    monkeypatch.setattr(policy, "market_level", lambda *args, **kwargs: levels.copy())
    end = date(2023, 10, 1)
    fixed = policy.build_publication_rows(snapshot, last_closed_month=end)
    with monkeypatch.context() as legacy:
        legacy.setitem(policy.POLICY["market_level"]["anchor"], "l_anchor", None)
        legacy.setitem(policy.POLICY["market_level"]["anchor"], "kind", "calibration_window_median_of_l")
        legacy.setattr(policy, "POLICY_DIGEST", "28f70b9bd8f617fedf6104deb86cd518d43307e88b3bbd1fcf53aa1fae869a3b")
        assert policy.market_anchor_for_snapshot(snapshot, last_closed_month=end) == OFFICIAL_ANCHOR
        old = policy.build_publication_rows(snapshot, last_closed_month=end)
    pd.testing.assert_series_equal(fixed["implied_bucket"], old["implied_bucket"])
    pd.testing.assert_series_equal(fixed["neutralized_score"], old["neutralized_score"])
    pd.testing.assert_series_equal(fixed["spread_norm_log"], old["spread_norm_log"])


def test_empty_old_window_with_real_later_witnesses_is_explicit_not_fabricated():
    snapshot = panel(months=("2026-09-01", "2026-10-01"))
    end = date(2026, 10, 1)
    diagnostics = policy.market_anchor_diagnostics_for_snapshot(snapshot, last_closed_month=end)
    assert diagnostics.l_anchor == OFFICIAL_ANCHOR
    assert diagnostics.resolved_l_anchor is None
    assert diagnostics.anchor_source == "fixed_policy"
    assert diagnostics.diagnostic_reason == "anchor_window_empty"
    rows = policy.build_publication_rows(snapshot, last_closed_month=end)
    assert rows["witnessed"].all()
    assert rows["neutralized_score"].notna().all()


def test_globally_dark_history_still_refuses_even_with_a_fixed_pin():
    snapshot = panel()
    snapshot["dollar_volume"] = None
    with pytest.raises(policy.AnchorWindowEmpty, match="closed history has no market-level observation"):
        policy.market_anchor_diagnostics_for_snapshot(snapshot, last_closed_month=date(2026, 8, 1))
    with pytest.raises(policy.AnchorWindowEmpty):
        policy.build_publication_rows(snapshot, last_closed_month=date(2026, 8, 1), l_anchor=OFFICIAL_ANCHOR)


def test_open_month_witness_cannot_rescue_a_globally_dark_closed_history():
    snapshot = panel(months=("2026-08-01", "2026-09-01"))
    snapshot.loc[snapshot["month"].eq(pd.Timestamp("2026-08-01")), "dollar_volume"] = None
    with pytest.raises(policy.AnchorWindowEmpty):
        policy.market_anchor_for_snapshot(snapshot, last_closed_month=date(2026, 8, 1))


@pytest.mark.parametrize("invalid", [None, float("nan"), float("inf"), -float("inf"), True, "bad"])
def test_nonfinite_or_malformed_policy_pins_are_typed(monkeypatch, invalid):
    monkeypatch.setitem(policy.POLICY["market_level"]["anchor"], "l_anchor", invalid)
    with pytest.raises(policy.InvalidAnchor, match="policy.l_anchor"):
        policy.market_anchor_for_snapshot(panel(), last_closed_month=date(2026, 8, 1))


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), True, "bad"])
def test_invalid_caller_anchor_is_typed(invalid):
    with pytest.raises(policy.InvalidAnchor, match="l_anchor"):
        policy.build_publication_rows(panel(), last_closed_month=date(2026, 8, 1), l_anchor=invalid)


@pytest.mark.parametrize("override", [0.0, OFFICIAL_ANCHOR + 1e-12, OFFICIAL_ANCHOR - 1.0])
def test_finite_caller_cannot_override_official_pin_even_within_old_tolerance(override):
    with pytest.raises(policy.AnchorOverrideRejected):
        policy.build_publication_rows(panel(), last_closed_month=date(2026, 8, 1), l_anchor=override)


@pytest.mark.parametrize("invalid", [np.inf, "bad"])
def test_nonfinite_market_level_never_becomes_a_rating_or_diagnostic(invalid):
    levels = pd.Series([invalid], index=pd.to_datetime(["2026-08-01"]))
    with pytest.raises(policy.InvalidAnchor, match="market_level_l"):
        policy.market_anchor(levels)


@pytest.mark.parametrize("entrypoint", [worker.plan, worker.run], ids=["plan", "run"])
@pytest.mark.parametrize("previous", [None, OFFICIAL_ANCHOR, OFFICIAL_ANCHOR + 1e-10])
def test_worker_keeps_official_pin_reports_drift_and_uses_plain_bound_identity(monkeypatch, entrypoint, previous):
    snapshot = panel()
    captured = stub_worker(monkeypatch, snapshot, previous_anchor=previous)
    result = entrypoint("ignored-synthetic-dsn")
    assert result["state"] == ("planned" if entrypoint is worker.plan else "published_no_defaults")
    assert result["l_anchor"] == OFFICIAL_ANCHOR
    assert result["policy_l_anchor"] == OFFICIAL_ANCHOR
    assert result["resolved_l_anchor"] == 0.0
    assert result["anchor_source"] == "fixed_policy"
    assert result["anchor_diagnostic_drift"] is True
    assert result["anchor_diagnostic_reason"] is None
    assert result["publication_id"] == publication_id_for(
        NEW_POLICY_DIGEST, "fixed-anchor-test-revision", policy.snapshot_fingerprint(snapshot)
    )
    if entrypoint is worker.run:
        assert captured["publication"].l_anchor == OFFICIAL_ANCHOR
        assert captured["expected_pointer"] == (None if previous is None else "synthetic-old-pointer")
    else:
        assert captured == {}
        assert result["anchor_drift"] is False


@pytest.mark.parametrize("entrypoint", [worker.plan, worker.run], ids=["plan", "run"])
def test_worker_empty_window_diagnostic_does_not_block_valid_fixed_policy(monkeypatch, entrypoint):
    snapshot = panel(months=("2026-09-01",))
    stub_worker(monkeypatch, snapshot)
    result = entrypoint("ignored-synthetic-dsn")
    assert result["state"] == ("planned" if entrypoint is worker.plan else "published_no_defaults")
    assert result["l_anchor"] == OFFICIAL_ANCHOR
    assert result["resolved_l_anchor"] is None
    assert result["anchor_diagnostic_reason"] == "anchor_window_empty"
    assert result["anchor_diagnostic_drift"] is None
    assert result["panel_last_closed_month"] == "2026-09-01"


@pytest.mark.parametrize("entrypoint", [worker.plan, worker.run], ids=["plan", "run"])
def test_worker_rejects_a_foreign_pin_not_a_corrected_median(monkeypatch, entrypoint):
    captured = stub_worker(monkeypatch, panel(), previous_anchor=OFFICIAL_ANCHOR + 1e-6)
    result = entrypoint("ignored-synthetic-dsn")
    assert result["state"] == "gate_failed"
    assert result["input_reasons"] == ["anchor_policy_mismatch"]
    assert result["policy_l_anchor"] == OFFICIAL_ANCHOR
    assert captured == {}


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), True, "bad"])
@pytest.mark.parametrize("field", ["policy", "inherited", "resolved", "chosen"])
def test_worker_typed_refusal_for_invalid_pins_or_diagnostics(monkeypatch, invalid, field):
    captured = stub_worker(monkeypatch, panel(), previous_anchor=invalid if field == "inherited" else OFFICIAL_ANCHOR)
    if field == "policy":
        monkeypatch.setitem(policy.POLICY["market_level"]["anchor"], "l_anchor", invalid)
    elif field in {"resolved", "chosen"}:
        diagnostics = policy.MarketAnchorDiagnostics(
            invalid if field == "chosen" else OFFICIAL_ANCHOR,
            invalid if field == "resolved" else 0.0, "fixed_policy"
        )
        monkeypatch.setattr(policy, "market_anchor_diagnostics_for_snapshot", lambda *args, **kwargs: diagnostics)
    result = worker.run("ignored-synthetic-dsn")
    assert result["state"] == "gate_failed"
    assert result["input_reasons"] == ["invalid_anchor"]
    assert captured == {}


def test_worker_fixed_anchor_still_obeys_pointer_compare_and_set(monkeypatch):
    captured = stub_worker(monkeypatch, panel())

    def pointer_moved(*args, **kwargs):
        assert kwargs["expected_pointer"] == "synthetic-old-pointer"
        raise BondError("pointer_moved", {"expected": kwargs["expected_pointer"]})

    monkeypatch.setattr(worker, "materialize", pointer_moved)
    result = worker.run("ignored-synthetic-dsn")
    assert result["state"] == "gate_failed"
    assert result["input_reasons"] == ["pointer_moved"]
    assert captured == {}


def test_fixed_anchor_repeated_plan_and_run_are_idempotent(monkeypatch):
    stub_worker(monkeypatch, panel())
    first = worker.plan("ignored-synthetic-dsn")
    repeat = worker.plan("ignored-synthetic-dsn")
    published = worker.run("ignored-synthetic-dsn")
    assert first["publication_id"] == repeat["publication_id"] == published["publication_id"]
    assert first["rows_digest"] == repeat["rows_digest"] == published["rows_digest"]
