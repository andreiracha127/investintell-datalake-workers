"""Read-only reproducibility check for the two frozen bond-panel months.

This is deliberately a one-shot Railway worker: it reads the publication currently
pointed at by ``bond_panel_v1``, rebuilds the two ratified months through Stage 6's
runtime path, and reports an all-gates parity verdict.  It never materializes facts
or moves a publication pointer.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from src.bonds.panel_config import config_hash
from src.bonds.panel_resolvers import compute_spread, monthly_treasury_curve
from src.db import connect, resolve_dsn
from src.workers import bond_panel

PANEL_CONFIG_HASH = "0c0d78a866bc1090"
BASE_PUBLICATION_ID = "92740098-1571-559d-9fb3-119de8321754"
BASE_INPUT_FINGERPRINT = "5a7af9e1adaed315e9940293cf3e9e789ca6350993688d58ab3e759cee37a3cb"
PARITY_MONTHS = (pd.Timestamp("2025-01-01"), pd.Timestamp("2026-06-01"))
SPREAD_DEFINITION = "ytm_minus_interpolated_dgs"

# Aliases make the runtime seams explicit and let focused tests exercise the
# orchestration without replacing the Stage 6 module itself.
_frame = bond_panel._frame
build_db_monthly_panel = bond_panel.build_db_monthly_panel
build_snapshots = bond_panel.build_snapshots
fit_all_months = bond_panel.fit_all_months


def _failure(reason: str, months: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {"state": "parity_failed", "reason": reason, "aborted": True, "months": months or []}


def _next_month(month: pd.Timestamp) -> pd.Timestamp:
    return month + pd.offsets.MonthBegin(1)


def _month_end(month: pd.Timestamp) -> date:
    return (month + pd.offsets.MonthEnd(1)).date()


def _current_publication(conn: Any) -> tuple[str, str, str, str] | None:
    row = conn.execute(
        "SELECT p.publication_id::text, p.config_hash, p.input_fingerprint, "
        "p.publication_status "
        "FROM bond_panel_app_pointer pointer "
        "JOIN bond_panel_publications p ON p.publication_id = pointer.publication_id "
        "WHERE pointer.product = 'bond_panel_v1'"
    ).fetchone()
    return None if row is None else tuple(str(value) for value in row)


def _frozen_snapshot(conn: Any, month: pd.Timestamp) -> pd.DataFrame:
    frame = _frame(
        conn,
        "SELECT cusip_id, month, eligibility_state, eligibility_reason, ytm, mod_dur, "
        "maturity_years, spread_final, spread_final_bps, spread_definition, source_lineage "
        "FROM bond_panel_snapshot WHERE publication_id = %s AND month = %s",
        (BASE_PUBLICATION_ID, month.date()),
    )
    if "month" in frame:
        frame["month"] = pd.to_datetime(frame["month"])
    return frame


def _frozen_rv(conn: Any, month: pd.Timestamp) -> pd.DataFrame:
    frame = _frame(
        conn,
        "SELECT cusip_id, month, rv_signal, spread_definition, source_lineage "
        "FROM bond_panel_rv_signal WHERE publication_id = %s AND month = %s",
        (BASE_PUBLICATION_ID, month.date()),
    )
    if "month" in frame:
        frame["month"] = pd.to_datetime(frame["month"])
    return frame


def _input_max_day(inputs: dict[str, pd.DataFrame]) -> date | None:
    days: list[pd.Timestamp] = []
    for frame in inputs.values():
        for column in ("day", "month", "rating_as_of_month"):
            if column in frame:
                values = pd.to_datetime(frame[column], errors="coerce").dropna()
                days.extend(values.tolist())
    return max(days).date() if days else None


def _filter_future_static_ratings(
    inputs: dict[str, pd.DataFrame], as_of: date,
) -> tuple[dict[str, pd.DataFrame], dict[str, int]]:
    """Remove static ratings that were not available at t, with typed evidence."""
    filtered = dict(inputs)
    ratings = filtered.get("static_rating_mapping")
    if ratings is None or "rating_as_of_month" not in ratings:
        return filtered, {"static_rating_after_month": 0}
    observed_at = pd.to_datetime(ratings["rating_as_of_month"], errors="coerce")
    future = observed_at > pd.Timestamp(as_of)
    filtered["static_rating_mapping"] = ratings.loc[~future].copy()
    return filtered, {"static_rating_after_month": int(future.sum())}


def _normalized_monthly_curve(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"tenor", "day", "yield_pct"}
    if frame.empty or not required.issubset(frame):
        raise ValueError("monthly_curve_untyped_or_empty")
    curve = frame.copy()
    tenor_map = {
        "1y": "DGS1", "2y": "DGS2", "3y": "DGS3", "5y": "DGS5",
        "7y": "DGS7", "10y": "DGS10", "20y": "DGS20", "30y": "DGS30",
    }
    curve["tenor"] = curve["tenor"].astype(str).str.lower().str.strip().map(tenor_map)
    curve = curve.dropna(subset=["tenor"])
    daily = curve.pivot_table(
        index=pd.to_datetime(curve["day"]),
        columns="tenor",
        values="yield_pct",
        aggfunc="median",
    )
    return monthly_treasury_curve(daily)


def _rebuild_month(
    conn: Any, month: pd.Timestamp,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    date | None,
    pd.Timestamp,
    pd.DataFrame,
    dict[str, int],
]:
    """Use precisely the Stage 6 input loader and resolver seams for one month."""
    as_of = _month_end(month)
    # Parity is a one-month walk-forward reconstruction. Passing t+1 to the
    # daily loader would admit its historical monthly-liquidity row even though
    # Stage 6 later fits only t; pin both loader month arguments to t instead.
    inputs, lineage = bond_panel._load_inputs(conn, month, month, as_of)
    inputs, input_exclusions = _filter_future_static_ratings(inputs, as_of)
    if not lineage or any(not value for value in lineage.values()):
        raise ValueError("input_lineage_missing")
    max_day = _input_max_day(inputs)
    if max_day is None:
        raise ValueError("input_day_missing")
    if max_day > as_of:
        raise ValueError("input_day_after_month_end")
    normalized_curve = _normalized_monthly_curve(inputs["monthly_curve"])
    panel = build_db_monthly_panel(**inputs, months=[month])
    if panel.empty:
        raise ValueError("panel_rebuild_empty")
    # These are the exact Stage 6 terms/liquidity annotations before snapshots.
    panel["issuer_identity_state"] = panel["issuer_identity_state"].fillna("unresolved") if "issuer_identity_state" in panel else "unresolved"
    panel["liquidity_reason"] = panel["reason_code"].fillna("monthly_liquidity_absent") if "reason_code" in panel else "monthly_liquidity_absent"
    terms_present = panel.get("coupon_pct", pd.Series(index=panel.index, dtype=float)).notna() & panel.get("maturity_date", pd.Series(index=panel.index, dtype=object)).notna() & panel.get("amt_outstanding_k", pd.Series(index=panel.index, dtype=float)).notna()
    panel["terms_source"] = terms_present.map({True: "bond_reference_terms", False: "terms_missing"})
    panel["terms_reason"] = terms_present.map({True: "terms_present", False: "terms_missing"})
    panel["spread_source"] = panel["spread_final"].notna().map({True: "computed", False: "missing_curve"})
    ratings = panel[["cusip_id", "month", "rating_bucket", "rating_as_of_month", "rating_state", "rating_reason", "rating_staleness_months"]].copy()
    panel_without_ratings = panel.drop(columns=[column for column in ratings.columns if column in panel and column not in {"cusip_id", "month"}])
    snapshots, exclusions = build_snapshots(panel_without_ratings, ratings_pit=ratings)
    if not exclusions.empty:
        exclusions = exclusions.merge(ratings, on=["cusip_id", "month"], how="left")
    snapshot = pd.concat([snapshots, exclusions], ignore_index=True).sort_values(["month", "cusip_id"])
    snapshot["month"] = pd.to_datetime(snapshot["month"])
    rebuilt_snapshot = snapshot[snapshot["month"].eq(month)].reset_index(drop=True)
    included = snapshots[snapshots["month"].eq(month)]
    signals, _diagnostics = fit_all_months(included, as_of=month)
    if not signals.empty:
        signals = signals.merge(
            included,
            on=["cusip_id", "month"],
            how="left",
            suffixes=("", "_snapshot"),
        )
    rebuilt_rv = signals[signals["month"].eq(month)].reset_index(drop=True) if not signals.empty else signals
    if not rebuilt_rv.empty:
        rebuilt_rv["month"] = pd.to_datetime(rebuilt_rv["month"])
    return rebuilt_snapshot, rebuilt_rv, max_day, month, normalized_curve, input_exclusions


def _valid_lineage(frame: pd.DataFrame) -> bool:
    return "source_lineage" in frame and all(isinstance(value, dict) and bool(value) for value in frame["source_lineage"])


def _typed_exclusions(frame: pd.DataFrame) -> float:
    if frame.empty or not {"eligibility_state", "eligibility_reason"}.issubset(frame):
        return 0.0
    excluded = frame[frame["eligibility_state"].eq("excluded")]
    if excluded.empty:
        return 1.0
    return float(excluded["eligibility_reason"].map(lambda value: isinstance(value, str) and bool(value.strip())).mean())


def _quantile_gate(
    values: pd.Series, limits: tuple[float, float, float],
) -> tuple[dict[str, float | None], bool]:
    if values.empty or values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        return {"median": None, "p90": None, "p99": None}, False
    stats = {"median": float(values.quantile(.5)), "p90": float(values.quantile(.9)), "p99": float(values.quantile(.99))}
    return stats, all(stats[key] <= limit for key, limit in zip(("median", "p90", "p99"), limits))


def _spread_semantics(
    frame: pd.DataFrame, monthly_curve: pd.DataFrame,
) -> tuple[dict[str, float | int | None], bool]:
    maturity = "bond_maturity" if "bond_maturity" in frame else "maturity_years"
    required = {"month", "ytm", "spread_final", "spread_final_bps", maturity}
    if frame.empty or not required.issubset(frame):
        return {"rows": int(len(frame)), "max_abs_error": None, "max_bps_conversion_error": None}, False
    candidate = frame[["month", "ytm", "spread_final", "spread_final_bps", maturity]].copy()
    candidate = candidate.rename(columns={maturity: "bond_maturity"}).reset_index(drop=True)
    expected = pd.to_numeric(compute_spread(candidate, monthly_curve), errors="coerce")
    observed = pd.to_numeric(candidate["spread_final"], errors="coerce")
    observed_bps = pd.to_numeric(candidate["spread_final_bps"], errors="coerce")
    finite = (
        expected.notna().all()
        and observed.notna().all()
        and observed_bps.notna().all()
        and np.isfinite(expected.to_numpy(dtype=float)).all()
        and np.isfinite(observed.to_numpy(dtype=float)).all()
        and np.isfinite(observed_bps.to_numpy(dtype=float)).all()
    )
    if not finite:
        return {"rows": int(len(candidate)), "max_abs_error": None, "max_bps_conversion_error": None}, False
    semantic_error = (observed - expected).abs()
    conversion_error = (observed * 10_000 - observed_bps).abs()
    evidence: dict[str, float | int | None] = {
        "rows": int(len(candidate)),
        "max_abs_error": float(semantic_error.max()),
        "max_bps_conversion_error": float(conversion_error.max()),
    }
    return evidence, bool(
        (semantic_error <= 1e-10).all() and (conversion_error <= 1e-10).all()
    )


def _compare_month(
    month: pd.Timestamp,
    frozen_snapshot: pd.DataFrame,
    frozen_rv: pd.DataFrame,
    rebuilt_snapshot: pd.DataFrame,
    rebuilt_rv: pd.DataFrame,
    *,
    input_max_day: date | None,
    fit_as_of: pd.Timestamp,
    monthly_curve: pd.DataFrame,
    input_exclusions: dict[str, int] | None = None,
) -> dict[str, object]:
    """Return a JSON-safe, conjunctive gate record for one frozen month."""
    label = month.date().isoformat()
    if frozen_snapshot.empty:
        return {"month": label, "state": "parity_failed", "reason": "frozen_snapshot_empty", "aborted": True, "failed_gates": ["frozen_snapshot_empty"]}
    if frozen_rv.empty:
        return {"month": label, "state": "parity_failed", "reason": "frozen_rv_empty", "aborted": True, "failed_gates": ["frozen_rv_empty"]}
    if rebuilt_snapshot.empty:
        return {"month": label, "state": "parity_failed", "reason": "rebuilt_snapshot_empty", "aborted": True, "failed_gates": ["rebuilt_snapshot_empty"]}
    snapshot_fields = {"cusip_id", "month", "eligibility_state", "eligibility_reason", "ytm", "mod_dur", "spread_final", "spread_final_bps", "spread_definition"}
    rv_fields = {"cusip_id", "month", "rv_signal", "spread_definition"}
    if not snapshot_fields.issubset(frozen_snapshot) or not snapshot_fields.issubset(rebuilt_snapshot):
        return {"month": label, "state": "parity_failed", "reason": "snapshot_untyped", "aborted": True, "failed_gates": ["snapshot_types"]}
    if not rv_fields.issubset(frozen_rv) or (
        not rebuilt_rv.empty and not rv_fields.issubset(rebuilt_rv)
    ):
        return {"month": label, "state": "parity_failed", "reason": "rv_untyped", "aborted": True, "failed_gates": ["rv_types"]}
    if not _valid_lineage(frozen_snapshot) or not _valid_lineage(frozen_rv):
        return {"month": label, "state": "parity_failed", "reason": "frozen_lineage_missing", "aborted": True, "failed_gates": ["frozen_lineage"]}

    frozen_included = frozen_snapshot[frozen_snapshot["eligibility_state"].eq("included")].copy()
    rebuilt_included = rebuilt_snapshot[rebuilt_snapshot["eligibility_state"].eq("included")].copy()
    duplicate_keys = bool(
        frozen_included.duplicated(["cusip_id", "month"]).any()
        or rebuilt_included.duplicated(["cusip_id", "month"]).any()
    )
    merged = frozen_included.merge(rebuilt_included, on=["cusip_id", "month"], suffixes=("_frozen", "_rebuilt"))
    frozen_n, rebuilt_n, matched = len(frozen_included), len(rebuilt_included), len(merged)
    if not frozen_n or not rebuilt_n or not matched:
        return {"month": label, "state": "parity_failed", "reason": "zero_overlap", "aborted": True, "failed_gates": ["zero_overlap"]}
    smaller = min(frozen_n, rebuilt_n)
    universe_limit = max(25, .005 * max(frozen_n, rebuilt_n))
    coverage = matched / smaller
    gates: dict[str, bool] = {
        "unique_universe_keys": not duplicate_keys,
        "universe_delta": abs(frozen_n - rebuilt_n) <= universe_limit,
        "matched_coverage": coverage >= .99,
        "typed_exclusions": _typed_exclusions(frozen_snapshot) == 1.0 and _typed_exclusions(rebuilt_snapshot) == 1.0,
        "spread_definition": set(frozen_snapshot["spread_definition"].dropna()) == {SPREAD_DEFINITION} and set(rebuilt_snapshot["spread_definition"].dropna()) == {SPREAD_DEFINITION} and set(frozen_rv["spread_definition"].dropna()) == {SPREAD_DEFINITION} and "spread_definition" in rebuilt_rv and set(rebuilt_rv["spread_definition"].dropna()) == {SPREAD_DEFINITION},
        "spread_numeric_semantics": False,
        "walk_forward": input_max_day is not None and input_max_day <= _month_end(month) and fit_as_of == month,
    }
    frozen_semantics, frozen_semantics_ok = _spread_semantics(frozen_included, monthly_curve)
    rebuilt_semantics, rebuilt_semantics_ok = _spread_semantics(rebuilt_included, monthly_curve)
    gates["spread_numeric_semantics"] = frozen_semantics_ok and rebuilt_semantics_ok
    metric_specs = {
        "ytm_abs_bps": ("ytm", "ytm", 10_000.0, (1.0, 5.0, 25.0)),
        "duration_abs_years": ("mod_dur", "mod_dur", 1.0, (.10, .50, 1.0)),
        "spread_abs_bps": ("spread_final_bps", "spread_final_bps", 1.0, (5.0, 25.0, 75.0)),
    }
    metrics: dict[str, dict[str, float | None]] = {}
    for gate, (frozen_key, rebuilt_key, scale, limits) in metric_specs.items():
        delta = (pd.to_numeric(merged[f"{frozen_key}_frozen"], errors="coerce") - pd.to_numeric(merged[f"{rebuilt_key}_rebuilt"], errors="coerce")).abs() * scale
        metrics[gate], gates[gate] = _quantile_gate(delta, limits)
    relative = (pd.to_numeric(merged["mod_dur_frozen"], errors="coerce") - pd.to_numeric(merged["mod_dur_rebuilt"], errors="coerce")).abs() / pd.to_numeric(merged["mod_dur_frozen"], errors="coerce").abs().clip(lower=1e-12)
    metrics["duration_relative"], gates["duration_relative"] = _quantile_gate(relative, (.02, .10, float("inf")))
    frozen_rv_n, rebuilt_rv_n = len(frozen_rv), len(rebuilt_rv)
    rv_merged = (
        frozen_rv.merge(
            rebuilt_rv,
            on=["cusip_id", "month"],
            suffixes=("_frozen", "_rebuilt"),
        )
        if rebuilt_rv_n
        else pd.DataFrame()
    )
    rv_limit = max(25, .005 * max(frozen_rv_n, rebuilt_rv_n))
    rv_smaller = min(frozen_rv_n, rebuilt_rv_n)
    rv_coverage = len(rv_merged) / rv_smaller if rv_smaller else 0.0
    gates["rebuilt_rv_nonempty"] = rebuilt_rv_n > 0
    gates["unique_rv_keys"] = not (
        frozen_rv.duplicated(["cusip_id", "month"]).any()
        or rebuilt_rv.duplicated(["cusip_id", "month"]).any()
    )
    gates["rv_universe_delta"] = abs(frozen_rv_n - rebuilt_rv_n) <= rv_limit
    gates["rv_matched_coverage"] = rv_coverage >= .99
    if rv_merged.empty:
        metrics["rv_abs"], gates["rv_abs"] = _quantile_gate(
            pd.Series(dtype=float), (.05, .25, .75),
        )
    else:
        rv_delta = (pd.to_numeric(rv_merged.get("rv_signal_frozen"), errors="coerce") - pd.to_numeric(rv_merged.get("rv_signal_rebuilt"), errors="coerce")).abs()
        metrics["rv_abs"], gates["rv_abs"] = _quantile_gate(rv_delta, (.05, .25, .75))
    failed = [name for name, passed in gates.items() if not passed]
    return {
        "month": label, "state": "parity_passed" if not failed else "parity_failed", "reason": None if not failed else "gate_failed", "aborted": bool(failed),
        "frozen_universe_size": frozen_n, "rebuilt_universe_size": rebuilt_n, "universe_delta_limit": universe_limit,
        "frozen_rv_size": frozen_rv_n, "rebuilt_rv_size": rebuilt_rv_n,
        "rv_universe_delta_limit": rv_limit,
        "rv_matched_coverage": rv_coverage,
        "matched_coverage": coverage, "typed_exclusions": {"frozen": _typed_exclusions(frozen_snapshot), "rebuilt": _typed_exclusions(rebuilt_snapshot)},
        "spread_definition": SPREAD_DEFINITION,
        "spread_semantics": {"frozen": frozen_semantics, "rebuilt": rebuilt_semantics},
        "metrics": metrics,
        "walk_forward": {
            "max_input_day": input_max_day.isoformat() if input_max_day else None,
            "calendar_month_end": _month_end(month).isoformat(),
            "fit_as_of": fit_as_of.date().isoformat(),
            "input_exclusions": input_exclusions or {"static_rating_after_month": 0},
        },
        "failed_gates": failed,
    }


def run(dsn: str | None = None) -> dict[str, object]:
    """Run the two-month, DB-only parity gate without modifying any relation."""
    if config_hash() != PANEL_CONFIG_HASH:
        return _failure("config_hash_mismatch")
    results: list[dict[str, object]] = []
    with connect(resolve_dsn(dsn)) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        current = _current_publication(conn)
        if current is None:
            return _failure("current_publication_missing")
        if current[0] != BASE_PUBLICATION_ID:
            return _failure("current_publication_id_mismatch")
        if current[1] != PANEL_CONFIG_HASH:
            return _failure("current_publication_config_mismatch")
        if current[2] != BASE_INPUT_FINGERPRINT:
            return _failure("current_publication_fingerprint_mismatch")
        if current[3] != "validated":
            return _failure("current_publication_status_mismatch")
        for month in PARITY_MONTHS:
            frozen_snapshot, frozen_rv = _frozen_snapshot(conn, month), _frozen_rv(conn, month)
            try:
                (
                    rebuilt_snapshot,
                    rebuilt_rv,
                    max_day,
                    fit_as_of,
                    monthly_curve,
                    input_exclusions,
                ) = _rebuild_month(conn, month)
            except (KeyError, TypeError, ValueError) as exc:
                results.append({"month": month.date().isoformat(), "state": "parity_failed", "reason": f"rebuild_error:{exc}", "aborted": True, "failed_gates": ["rebuild"]})
                continue
            results.append(_compare_month(
                month,
                frozen_snapshot,
                frozen_rv,
                rebuilt_snapshot,
                rebuilt_rv,
                input_max_day=max_day,
                fit_as_of=fit_as_of,
                monthly_curve=monthly_curve,
                input_exclusions=input_exclusions,
            ))
    failed = [result for result in results if result["state"] != "parity_passed"]
    if failed:
        return _failure(str(failed[0]["reason"]), results)
    return {"state": "parity_passed", "reason": None, "aborted": False, "months": results}
