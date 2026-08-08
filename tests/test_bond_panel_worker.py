"""Focused orchestration tests for the daily bond-panel stage."""
from __future__ import annotations

import contextlib
from datetime import date

import pandas as pd

from src.bonds.panel_materializer import MaterializationResult
from src.workers import bond_panel


def test_panel_refuses_to_bootstrap_a_two_month_history(monkeypatch) -> None:
    """A daily delta needs a validated compatible parent; it never self-bases."""
    monkeypatch.setattr(bond_panel, "_required_relations", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_missing_columns", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_current_parent", lambda _conn: None)
    monkeypatch.setattr(bond_panel, "connect", lambda _dsn: contextlib.nullcontext(object()))
    monkeypatch.setenv("CODE_REVISION", "test")

    outcome = bond_panel.run("postgresql://example", as_of=date(2026, 8, 8))

    assert outcome["state"] == "gate_failed"
    assert outcome["reason"] == "panel_no_parent"
    assert outcome["aborted"] is True


def test_db_loader_uses_curated_candidates_and_pins_the_static_rating_sha(monkeypatch) -> None:
    sql_seen: list[str] = []

    def frame(_conn, sql, _params=()):
        sql_seen.append(sql)
        if "FROM bond_rating_static" in sql:
            return pd.DataFrame({
                "cusip9": ["037833100"],
                "rating_bucket": ["A"],
                "rating_as_of_month": [date(2026, 7, 1)],
                "rating_state": ["static_current"],
                "rating_reason": ["static_backfill"],
                "source_sha256": ["a" * 64],
            })
        return pd.DataFrame()

    monkeypatch.setattr(bond_panel, "_frame", frame)

    _inputs, lineage = bond_panel._load_inputs(
        object(), pd.Timestamp("2026-07-01"), pd.Timestamp("2026-08-01"), date(2026, 8, 8)
    )

    issuer_sql = next(sql for sql in sql_seen if "resolved" in sql and "sec_cusip_ticker_map" in sql)
    assert "FROM bond_curated_universe u" in issuer_sql
    assert "ILIKE '%%corporate%%'" in issuer_sql
    assert lineage["static_rating_mapping"] == f"bond_rating_static:{'a' * 64}"


def test_panel_publishes_only_closed_month_signals_and_returns(monkeypatch) -> None:
    closed = pd.Timestamp("2026-07-01")
    open_month = pd.Timestamp("2026-08-01")
    panel = pd.DataFrame(
        {
            "cusip_id": ["AAA", "AAA"],
            "month": [closed, open_month],
            "pr": [99.0, 100.0],
            "ytm": [0.05, 0.05],
            "bond_maturity": [5.0, 5.0],
            "issuer_id": ["issuer-1", "issuer-1"],
            "ff17num": [1, 1],
            "currency": ["USD", "USD"],
            "asset_class": ["corporate", "corporate"],
            "amt_outstanding_k": [500_000, 500_000],
            "traded_days": [10, 5],
            "trade_count": [10, 5],
            "dollar_volume": [1000.0, 500.0],
            "quoted_days": [1, 1],
            "rel_bid_ask_bps": [10.0, 10.0],
            "coupon_pct": [5.0, 5.0],
            "maturity_date": [pd.Timestamp("2031-01-01"), pd.Timestamp("2031-01-01")],
            "spread_final": [0.01, 0.01],
            "spread_final_bps": [100.0, 100.0],
            "spread_definition": ["ytm_minus_interpolated_dgs"] * 2,
                "rating_bucket": ["BBB", "A"],
            "rating_as_of_month": [pd.NaT, pd.NaT],
                "rating_state": ["static_current", "static_carry_forward"],
                "rating_reason": ["static_present", "static_present"],
                "rating_staleness_months": [pd.NA, pd.NA],
                "reason_code": ["live_tick_median_valid_bps", None],
        }
    )
    parent = {"publication_id": "parent", "first_month": date(2020, 1, 1)}
    captured: dict[str, object] = {}

    monkeypatch.setattr(bond_panel, "_required_relations", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_missing_columns", lambda _conn: [])
    monkeypatch.setattr(bond_panel, "_current_parent", lambda _conn: parent)
    monkeypatch.setattr(bond_panel, "connect", lambda _dsn: contextlib.nullcontext(object()))
    monkeypatch.setenv("CODE_REVISION", "test")
    monkeypatch.setattr(bond_panel, "_load_inputs", lambda _conn, _closed, _open, _as_of: ({}, {}))
    monkeypatch.setattr(bond_panel, "build_db_monthly_panel", lambda **_kwargs: panel.copy())
    def snapshots(frame, ratings_pit=None):
        included = frame.iloc[[0]].merge(ratings_pit, on=["cusip_id", "month"], how="left").assign(eligibility_state="included", eligibility_reason="eligible")
        excluded = frame.iloc[[1]].assign(eligibility_state="excluded", eligibility_reason="missing_terms")
        return included, excluded
    monkeypatch.setattr(bond_panel, "build_snapshots", snapshots)
    monkeypatch.setattr(bond_panel, "_parent_return_anchor", lambda _conn, _closed: pd.DataFrame())
    monkeypatch.setattr(bond_panel, "monthly_returns", lambda _panel, terminal_exits=None: pd.DataFrame({"cusip_id": ["AAA"], "month": [closed], "total_return": [0.01], "exit_basis": ["observed"], "exit_reason": [None], "price_return": [0.01], "carry_return": [0.0], "suspect": [False]}))
    monkeypatch.setattr(bond_panel, "fit_all_months", lambda frame, *, as_of: (pd.DataFrame({"cusip_id": ["AAA"], "month": [closed], "rv_signal": [1.0]}), pd.DataFrame()))

    def materialize(_conn, **kwargs):
        captured.update(kwargs)
        return MaterializationResult("published", "fingerprint", "validated", {name: len(rows) for name, rows in kwargs["facts"].items()}, "parent")

    monkeypatch.setattr(bond_panel, "materialize_panel", materialize)

    outcome = bond_panel.run("postgresql://example", as_of=date(2026, 8, 8))

    assert outcome["state"] == "published", outcome
    facts = captured["facts"]
    assert {row["month"] for row in facts["rv_signal"]} == {"2026-07-01"}
    assert {row["month"] for row in facts["returns"]} == {"2026-07-01"}
    assert {row["month"] for row in facts["snapshot"]} == {"2026-07-01", "2026-08-01"}
    excluded = next(row for row in facts["snapshot"] if row["eligibility_state"] == "excluded")
    assert (excluded["rating_bucket"], excluded["rating_state"], excluded["rating_reason"]) == ("A", "static_carry_forward", "static_present")
    assert excluded["liquidity_reason"] == "monthly_liquidity_absent"
    included = next(row for row in facts["snapshot"] if row["eligibility_state"] == "included")
    assert included["liquidity_reason"] == "live_tick_median_valid_bps"
    assert included["terms_source"] == "bond_reference_terms"
    assert captured["first_month"] == date(2020, 1, 1)


def test_terminal_exit_rows_are_closed_month_only_and_typed() -> None:
    closed = pd.Timestamp("2026-07-01")
    exits = pd.DataFrame({
        "cusip_id": ["MATURED", "DISTRESS", "UNKNOWN"],
        "month": [closed, closed, closed],
        "pr": [100.0, 60.0, 80.0],
        "ytm": [0.12, 0.10, 0.10],
        "bond_maturity": [1.0, 5.0, 5.0],
        "rating_bucket": ["BBB", "D", "BBB"],
    })

    rows = bond_panel.monthly_returns(
        pd.DataFrame({"cusip_id": ["OLD"], "month": [pd.Timestamp("2026-06-01")], "pr": [100.0], "ytm": [0.05], "bond_maturity": [5.0]}),
        terminal_exits=exits,
    )

    assert set(rows["exit_basis"]) == {"matured", "distressed", "unexplained"}
    assert set(rows["month"]) == {closed}
