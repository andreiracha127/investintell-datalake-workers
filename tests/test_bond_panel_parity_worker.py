"""Focused contracts for the read-only bond-panel parity worker."""
from __future__ import annotations

import contextlib
from datetime import date

import pandas as pd

from src.workers import bond_panel_parity as parity


def _snapshot(month: pd.Timestamp, *, ytm: float = 0.05) -> pd.DataFrame:
    return pd.DataFrame({
        "cusip_id": ["AAA"], "month": [month], "eligibility_state": ["included"],
        "eligibility_reason": ["eligible"], "ytm": [ytm], "mod_dur": [4.0],
        "maturity_years": [4.0], "bond_maturity": [4.0],
        "spread_final": [0.01], "spread_final_bps": [100.0],
        "spread_definition": ["ytm_minus_interpolated_dgs"],
        "source_lineage": [{"daily_observations": "bond_observation_daily"}],
    })


def _rv(month: pd.Timestamp, *, signal: float = 0.1) -> pd.DataFrame:
    return pd.DataFrame({
        "cusip_id": ["AAA"], "month": [month], "rv_signal": [signal],
        "spread_definition": ["ytm_minus_interpolated_dgs"],
        "source_lineage": [{"daily_observations": "bond_observation_daily"}],
    })


def _curve(month: pd.Timestamp, *, rate: float = 0.04) -> pd.DataFrame:
    return pd.DataFrame({"DGS3": [rate], "DGS5": [rate]}, index=[month])


def test_compare_month_passes_exact_rebuild_and_records_all_gates() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._compare_month(
        month, _snapshot(month), _rv(month), _snapshot(month), _rv(month),
        input_max_day=date(2025, 1, 31), fit_as_of=month,
        monthly_curve=_curve(month),
    )

    assert result["state"] == "parity_passed"
    assert result["aborted"] is False
    assert result["matched_coverage"] == 1.0
    assert result["spread_definition"] == "ytm_minus_interpolated_dgs"
    assert result["walk_forward"]["fit_as_of"] == "2025-01-01"


def test_compare_month_fails_ytm_threshold_and_empty_frozen_month() -> None:
    month = pd.Timestamp("2025-01-01")
    failed = parity._compare_month(
        month, _snapshot(month), _rv(month), _snapshot(month, ytm=0.051), _rv(month),
        input_max_day=date(2025, 1, 31), fit_as_of=month,
        monthly_curve=_curve(month),
    )
    empty = parity._compare_month(
        month, pd.DataFrame(), _rv(month), _snapshot(month), _rv(month),
        input_max_day=date(2025, 1, 31), fit_as_of=month,
        monthly_curve=_curve(month),
    )

    assert failed["state"] == "parity_failed"
    assert "ytm_abs_bps" in failed["failed_gates"]
    assert empty["reason"] == "frozen_snapshot_empty"
    assert empty["aborted"] is True


def test_compare_month_checks_spread_against_interpolated_curve_not_only_bps() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._compare_month(
        month, _snapshot(month), _rv(month), _snapshot(month), _rv(month),
        input_max_day=date(2025, 1, 31), fit_as_of=month,
        monthly_curve=_curve(month, rate=0.03),
    )

    assert result["state"] == "parity_failed"
    assert "spread_numeric_semantics" in result["failed_gates"]


def test_compare_month_refuses_materially_incomplete_rv_surface() -> None:
    month = pd.Timestamp("2025-01-01")
    frozen_rv = pd.concat(
        [_rv(month).assign(cusip_id="AAA" if index == 0 else f"CUSIP{index:03d}") for index in range(30)],
        ignore_index=True,
    )

    result = parity._compare_month(
        month, _snapshot(month), frozen_rv, _snapshot(month), _rv(month),
        input_max_day=date(2025, 1, 31), fit_as_of=month,
        monthly_curve=_curve(month),
    )

    assert result["state"] == "parity_failed"
    assert "rv_universe_delta" in result["failed_gates"]


def test_compare_month_requires_99_percent_rv_key_overlap_at_equal_size() -> None:
    month = pd.Timestamp("2025-01-01")
    frozen_ids = [f"CUSIP{index:03d}" for index in range(100)]
    rebuilt_ids = [*frozen_ids[:98], "OTHER001", "OTHER002"]
    frozen_rv = pd.concat(
        [_rv(month).assign(cusip_id=cusip) for cusip in frozen_ids], ignore_index=True,
    )
    rebuilt_rv = pd.concat(
        [_rv(month).assign(cusip_id=cusip) for cusip in rebuilt_ids], ignore_index=True,
    )

    result = parity._compare_month(
        month, _snapshot(month), frozen_rv, _snapshot(month), rebuilt_rv,
        input_max_day=date(2025, 1, 31), fit_as_of=month,
        monthly_curve=_curve(month),
    )

    assert result["state"] == "parity_failed"
    assert result["rv_matched_coverage"] == 0.98
    assert "rv_matched_coverage" in result["failed_gates"]


def test_compare_month_measures_snapshot_gates_when_rebuilt_rv_is_empty() -> None:
    month = pd.Timestamp("2025-01-01")

    result = parity._compare_month(
        month, _snapshot(month), _rv(month), _snapshot(month), pd.DataFrame(),
        input_max_day=date(2025, 1, 31), fit_as_of=month,
        monthly_curve=_curve(month),
    )

    assert result["state"] == "parity_failed"
    assert result["rebuilt_rv_size"] == 0
    assert result["metrics"]["ytm_abs_bps"]["median"] == 0
    assert result["metrics"]["rv_abs"] == {"median": None, "p90": None, "p99": None}
    assert "rebuilt_rv_nonempty" in result["failed_gates"]


def test_compare_month_records_universe_sizes_when_overlap_is_zero() -> None:
    month = pd.Timestamp("2025-01-01")
    rebuilt = _snapshot(month).assign(cusip_id="OTHER")

    result = parity._compare_month(
        month, _snapshot(month), _rv(month), rebuilt, pd.DataFrame(),
        input_max_day=date(2025, 1, 31), fit_as_of=month,
        monthly_curve=_curve(month),
    )

    assert result["reason"] == "zero_overlap"
    assert result["frozen_universe_size"] == 1
    assert result["rebuilt_universe_size"] == 1
    assert result["matched_bonds"] == 0
    assert result["metrics_unavailable_reason"] == "zero_overlap"


def test_run_refuses_config_mismatch_without_connecting(monkeypatch) -> None:
    monkeypatch.setattr(parity, "config_hash", lambda: "wrong")
    monkeypatch.setattr(parity, "connect", lambda _dsn: (_ for _ in ()).throw(AssertionError("no DB")))

    outcome = parity.run("postgresql://example")

    assert outcome == {
        "state": "parity_failed", "reason": "config_hash_mismatch", "aborted": True,
        "months": [],
    }


def test_run_uses_exact_month_clock_and_issues_no_writes(monkeypatch) -> None:
    calls: list[tuple[pd.Timestamp, pd.Timestamp, date]] = []
    fit_calls: list[pd.Timestamp] = []
    sql: list[str] = []
    month_rows = {month: (_snapshot(month), _rv(month)) for month in parity.PARITY_MONTHS}

    class Connection:
        def execute(self, statement, _params=()):
            sql.append(statement)
            return type("Result", (), {"fetchone": lambda self: (
                parity.BASE_PUBLICATION_ID,
                parity.PANEL_CONFIG_HASH,
                parity.BASE_INPUT_FINGERPRINT,
                "validated",
            )})()

    monkeypatch.setattr(parity, "connect", lambda _dsn: contextlib.nullcontext(Connection()))
    monkeypatch.setattr(parity, "resolve_dsn", lambda dsn: dsn)
    def frame(_conn, statement, params=()):
        sql.append(statement)
        return month_rows[pd.Timestamp(params[-1])][0 if "snapshot" in statement else 1].copy()

    monkeypatch.setattr(parity, "_frame", frame)
    monkeypatch.setattr(parity.bond_panel, "_load_inputs", lambda _conn, closed, opened, as_of: calls.append((closed, opened, as_of)) or ({
        "daily_observations": pd.DataFrame({"day": [as_of]}),
        "monthly_curve": pd.DataFrame({
            "day": [as_of, as_of], "tenor": ["3y", "5y"], "yield_pct": [4.0, 4.0],
        }),
        "static_rating_mapping": pd.DataFrame({
            "cusip9": ["AAA"],
            "rating_as_of_month": [pd.Timestamp("2026-07-01")],
        }),
    }, {"x": "x"}))
    monkeypatch.setattr(parity, "build_db_monthly_panel", lambda **_kwargs: _snapshot(_kwargs["months"][0]).assign(coupon_pct=5.0, maturity_date=pd.Timestamp("2030-01-01"), reason_code="quoted", rating_bucket="A", rating_as_of_month=pd.Timestamp("2025-01-01"), rating_state="static_current", rating_reason="static_rating_current", rating_staleness_months=0))
    monkeypatch.setattr(parity, "build_snapshots", lambda frame, ratings_pit=None: (frame.copy(), pd.DataFrame(columns=frame.columns)))
    monkeypatch.setattr(parity, "fit_all_months", lambda frame, *, as_of: fit_calls.append(as_of) or (
        _rv(as_of)[["cusip_id", "month", "rv_signal"]], pd.DataFrame(),
    ))

    outcome = parity.run("postgresql://example")

    assert outcome["state"] == "parity_passed", outcome
    assert calls == [
        (pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-01"), date(2025, 1, 31)),
        (pd.Timestamp("2026-06-01"), pd.Timestamp("2026-06-01"), date(2026, 6, 30)),
    ]
    assert fit_calls == list(parity.PARITY_MONTHS)
    assert all(
        month_result["walk_forward"]["input_exclusions"] == {"static_rating_after_month": 1}
        for month_result in outcome["months"]
    )
    assert any(statement == "SET TRANSACTION READ ONLY" for statement in sql)
    assert all("bond_panel_current_" not in statement for statement in sql)
    assert any("FROM bond_panel_snapshot WHERE publication_id" in statement for statement in sql)
    assert any("FROM bond_panel_rv_signal WHERE publication_id" in statement for statement in sql)
    assert all(not statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "ALTER", "CREATE")) for statement in sql)


def test_run_refuses_a_different_current_publication_identity(monkeypatch) -> None:
    class Connection:
        def execute(self, statement, _params=()):
            row = (
                "different-publication",
                parity.PANEL_CONFIG_HASH,
                parity.BASE_INPUT_FINGERPRINT,
                "validated",
            )
            return type("Result", (), {"fetchone": lambda self: row})()

    monkeypatch.setattr(parity, "connect", lambda _dsn: contextlib.nullcontext(Connection()))
    monkeypatch.setattr(parity, "resolve_dsn", lambda dsn: dsn)

    outcome = parity.run("postgresql://example")

    assert outcome["state"] == "parity_failed"
    assert outcome["reason"] == "current_publication_id_mismatch"
    assert outcome["aborted"] is True


def test_run_refuses_a_different_base_fingerprint(monkeypatch) -> None:
    class Connection:
        def execute(self, statement, _params=()):
            row = (
                parity.BASE_PUBLICATION_ID,
                parity.PANEL_CONFIG_HASH,
                "0" * 64,
                "validated",
            )
            return type("Result", (), {"fetchone": lambda self: row})()

    monkeypatch.setattr(parity, "connect", lambda _dsn: contextlib.nullcontext(Connection()))
    monkeypatch.setattr(parity, "resolve_dsn", lambda dsn: dsn)

    outcome = parity.run("postgresql://example")

    assert outcome["state"] == "parity_failed"
    assert outcome["reason"] == "current_publication_fingerprint_mismatch"
