"""Pure monthly bond-panel research resolvers; no cache, file, or DB access."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor

from .panel_config import FROZEN

DGS_TENORS = {"DGS1": 1.0, "DGS2": 2.0, "DGS3": 3.0, "DGS5": 5.0, "DGS7": 7.0, "DGS10": 10.0, "DGS20": 20.0, "DGS30": 30.0}
BUCKET_ORDER = ("AAA", "AA", "A", "BBB", "BB", "B", "CCC", "D")
BUCKET_NUMERIC = {bucket: i + 1 for i, bucket in enumerate(BUCKET_ORDER)}
SPREAD_WINSOR_BPS = (1.0, 3000.0)
MIN_MONTH_ROWS = 300


@dataclass(frozen=True)
class SpreadGate:
    max_median_abs_err_bps: float
    max_p90_abs_err_bps: float


DEFAULT_GATE = SpreadGate(25.0, 75.0)


def build_monthly_panel(frame: pd.DataFrame) -> pd.DataFrame:
    source = ("cusip_id", "trd_exctn_dt", "pr", "ytm", "mod_dur", "bond_maturity", "credit_spread", "trade_count", "dvolume", "prc_bid", "prc_ask", "bond_amt_outstanding", "ff17num", "db_type")
    missing = [column for column in source if column not in frame]
    if missing:
        raise ValueError(f"source frame is missing columns: {missing}")
    df = frame.loc[:, list(source)].copy()
    df["cusip_id"] = df["cusip_id"].astype(str)
    df["trd_exctn_dt"] = pd.to_datetime(df["trd_exctn_dt"])
    df = df.sort_values(["cusip_id", "trd_exctn_dt"], kind="stable")
    df["month"] = df["trd_exctn_dt"].dt.to_period("M").dt.to_timestamp()
    mid = (df["prc_bid"] + df["prc_ask"]) / 2
    df["rel_bid_ask_bps"] = ((df["prc_ask"] - df["prc_bid"]) / mid * 10_000).where(df["prc_bid"].notna() & df["prc_ask"].notna() & (df["prc_ask"] > df["prc_bid"]) & (mid > 0))
    g = df.groupby(["cusip_id", "month"], observed=True, sort=False)
    result = g.agg(pr=("pr", "median"), ytm=("ytm", "median"), mod_dur=("mod_dur", "median"), bond_maturity=("bond_maturity", "median"), credit_spread=("credit_spread", "median"), trade_count=("trade_count", "sum"), dollar_volume=("dvolume", "sum"), traded_days=("trd_exctn_dt", "nunique"), prc_bid=("prc_bid", "median"), prc_ask=("prc_ask", "median"), rel_bid_ask_bps=("rel_bid_ask_bps", "median"), quoted_days=("rel_bid_ask_bps", "count"), amt_outstanding_k=("bond_amt_outstanding", "last"), ff17num=("ff17num", "last"), db_type=("db_type", "last")).reset_index()
    for column in ("trade_count", "traded_days", "quoted_days"):
        result[column] = result[column].astype("int64")
    return result


def validate_panel(panel: pd.DataFrame) -> dict[str, Any]:
    months = panel["month"]
    amounts = panel["amt_outstanding_k"].dropna()
    amounts = amounts[amounts > 0]
    p50 = float(amounts.median()) if len(amounts) else float("nan")
    duplicate = int(panel.duplicated(["cusip_id", "month"]).sum())
    report = {"rows": int(len(panel)), "cusips": int(panel["cusip_id"].nunique()), "month_min": str(months.min().date()) if len(panel) else None, "month_max": str(months.max().date()) if len(panel) else None, "spread_coverage_by_year": {int(year): round(float(group["credit_spread"].notna().mean()), 4) for year, group in panel.assign(year=months.dt.year).groupby("year")}, "quote_coverage": round(float((panel["quoted_days"] > 0).mean()), 4), "amt_outstanding_k_p50": p50, "amt_unit_consistent_with_thousands": bool(1e4 <= p50 <= 5e6), "duplicate_cusip_months": duplicate}
    report["ok"] = bool(report["rows"] > 0 and duplicate == 0 and report["amt_unit_consistent_with_thousands"])
    return report


def monthly_treasury_curve(daily: pd.DataFrame) -> pd.DataFrame:
    known = [column for column in daily if column in DGS_TENORS]
    if not known:
        raise ValueError("no known DGS columns in the daily curve frame")
    frame = daily[known].apply(pd.to_numeric, errors="coerce") / 100
    frame.index = pd.to_datetime(daily.index)
    result = frame.groupby(frame.index.to_period("M")).median()
    result.index = result.index.to_timestamp()
    return result


def interpolate_treasury(curve_row: pd.Series, maturity_years: np.ndarray) -> np.ndarray:
    cols = [column for column in curve_row.index if column in DGS_TENORS]
    tenors, values = np.array([DGS_TENORS[column] for column in cols]), np.array([curve_row[column] for column in cols])
    mask = ~np.isnan(values)
    if mask.sum() < 2:
        return np.full(len(maturity_years), np.nan)
    order = np.argsort(tenors[mask])
    t, v = tenors[mask][order], values[mask][order]
    return np.interp(np.clip(maturity_years, t[0], t[-1]), t, v)


def compute_spread(panel: pd.DataFrame, monthly_curve: pd.DataFrame) -> pd.Series:
    out = np.full(len(panel), np.nan)
    for month, index in panel.groupby("month").indices.items():
        if month in monthly_curve.index:
            rows = panel.iloc[index]
            out[index] = rows["ytm"].to_numpy(dtype=float) - interpolate_treasury(monthly_curve.loc[month], rows["bond_maturity"].to_numpy(dtype=float))
    return pd.Series(out, index=panel.index, name="spread_final")


def validate_computed_spread(panel: pd.DataFrame, spread_final: pd.Series, gate: SpreadGate = DEFAULT_GATE) -> dict[str, Any]:
    overlap = panel["credit_spread"].notna() & spread_final.notna()
    both = overlap & (panel["credit_spread"] > 0) & (spread_final > 0)
    errors = (spread_final[both] - panel.loc[both, "credit_spread"]) * 10_000
    absolute = errors.abs()
    report: dict[str, Any] = {"gate_scope": "positive_spread_rows", "overlap_rows": int(both.sum()), "nonpositive_rows_excluded": int((overlap & ~both).sum()), "median_err_bps": round(float(errors.median()), 2), "median_abs_err_bps": round(float(absolute.median()), 2), "p90_abs_err_bps": round(float(absolute.quantile(.9)), 2), "gate": {"max_median_abs_err_bps": gate.max_median_abs_err_bps, "max_p90_abs_err_bps": gate.max_p90_abs_err_bps}}
    report["passes_gate"] = bool(both.sum() and report["median_abs_err_bps"] <= gate.max_median_abs_err_bps and report["p90_abs_err_bps"] <= gate.max_p90_abs_err_bps)
    return report


def eligibility(panel: pd.DataFrame) -> pd.Series:
    required = ("ytm", "mod_dur", "pr", "amt_outstanding_k", "bond_maturity", "traded_days", "issuer_id", "ff17num", "currency", "asset_class")
    missing = [name for name in required if name not in panel]
    if missing:
        raise ValueError(f"eligibility frame missing columns: {missing}")
    reasons: list[str] = []
    for row in panel.itertuples(index=False):
        value = row._asdict()
        checks = (
            (pd.isna(value["currency"]) or not str(value["currency"]).strip(), "missing_currency"),
            (not pd.isna(value["currency"]) and str(value["currency"]).strip().upper() != "USD", "non_usd"),
            (pd.isna(value["asset_class"]) or str(value["asset_class"]).strip().lower() in {"", "missing"}, "missing_asset_class"),
            (not pd.isna(value["asset_class"]) and str(value["asset_class"]).strip().lower() not in {"", "missing", "corporate"}, "noncorporate"),
            (pd.isna(value["issuer_id"]) or not str(value["issuer_id"]).strip(), "unresolved_issuer"),
            (pd.isna(value["ff17num"]), "missing_sector"),
            (pd.isna(value.get("db_type")), "missing_db_type"),
            (not pd.isna(value.get("db_type")) and float(value["db_type"]) == 3, "unsupported_144a"),
            (pd.isna(value["amt_outstanding_k"]), "missing_amount"),
            (value["amt_outstanding_k"] < 250_000, "too_small"),
            (pd.isna(value["bond_maturity"]), "missing_maturity"),
            (value["bond_maturity"] < 1, "matured_or_short"),
            (pd.isna(value["traded_days"]), "missing_traded_days"),
            (value["traded_days"] < 5, "illiquid"),
            (pd.isna(value["pr"]), "missing_price"),
            (not pd.isna(value["pr"]) and not 1 <= value["pr"] <= 300, "invalid_price"),
            (pd.isna(value["ytm"]), "missing_ytm"),
            (not pd.isna(value["ytm"]) and not -.02 <= value["ytm"] <= .60, "invalid_ytm"),
            (pd.isna(value["mod_dur"]), "missing_duration"),
            (not pd.isna(value["mod_dur"]) and not .05 <= value["mod_dur"] <= 40, "invalid_duration"),
        )
        reasons.append(next((code for failed, code in checks if failed), "eligible"))
    return pd.Series(reasons, index=panel.index, dtype="object")


def build_universe_snapshot(panel: pd.DataFrame) -> pd.DataFrame:
    """Keep every candidate CUSIP with one stable, typed inclusion decision."""
    result = panel.copy()
    result["eligibility_reason"] = eligibility(result)
    result["eligibility_state"] = np.where(
        result["eligibility_reason"].eq("eligible"), "included", "excluded"
    )
    return result


def build_snapshots(panel: pd.DataFrame, ratings_pit: pd.DataFrame | None = None, hold_windows: pd.DataFrame | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    classified = build_universe_snapshot(panel)
    exclusions = classified[classified["eligibility_state"].eq("excluded")].copy()
    snap = classified[classified["eligibility_state"].eq("included")].copy()
    if ratings_pit is not None:
        rating_columns = [column for column in ratings_pit.columns if column not in {"cusip_id", "month"}]
        snap = snap.merge(
            ratings_pit[["cusip_id", "month", *rating_columns]],
            on=["cusip_id", "month"],
            how="left",
        )
        snap["rating_bucket"] = snap["rating_bucket"].fillna("NR")
    else:
        snap["rating_bucket"] = "NR"
    if hold_windows is None:
        snap["held_by_funds"] = pd.NA
        return snap.reset_index(drop=True), exclusions
    held = hold_windows.rename(columns={"cusip": "cusip_id"})
    snap = snap.merge(held[["cusip_id", "first_held", "last_held"]], on="cusip_id", how="left")
    grace = pd.Timedelta(days=45)
    snap["held_by_funds"] = (snap["first_held"].notna() & (snap["month"] >= snap["first_held"] - grace) & (snap["month"] <= snap["last_held"] + grace)).astype("boolean")
    snap.loc[snap["month"] < pd.Timestamp("2019-09-01"), "held_by_funds"] = pd.NA
    return snap.drop(columns=["first_held", "last_held"]).reset_index(drop=True), exclusions


def snapshot_summary(snap: pd.DataFrame) -> dict[str, Any]:
    per_month = snap.groupby("month")["cusip_id"].nunique()
    return {"rows": int(len(snap)), "cusips": int(snap["cusip_id"].nunique()), "months": int(len(per_month)), "bonds_per_month_median": float(per_month.median()), "rated_share": float(snap["rating_bucket"].ne("NR").mean())}


def ratings_static_mapping(mapping: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    """Attach the frozen pack's generic static rating mapping by CUSIP/month.

    This intentionally carries no source identity: it reproduces the
    already-carried-forward pack bucket and exposes absence/staleness as typed
    neutral state, reason, and source month.
    """
    mapping = mapping.rename(columns={"cusip9": "cusip_id", "reason_code": "rating_reason"}).copy()
    required = {"cusip_id", "rating_bucket", "rating_as_of_month", "rating_state"}
    if mapping.empty:
        mapping = pd.DataFrame(columns=list(required))
    missing = required.difference(mapping.columns)
    if missing:
        raise ValueError(f"static rating mapping missing columns: {sorted(missing)}")
    columns = list(required | {"rating_reason"})
    if "rating_reason" not in mapping:
        mapping["rating_reason"] = None
    result = targets[["cusip_id", "month"]].merge(mapping[columns], on="cusip_id", how="left")
    result["rating_as_of_month"] = pd.to_datetime(result["rating_as_of_month"], errors="coerce")
    matched = result["rating_as_of_month"].notna()
    available = matched & (result["rating_as_of_month"] <= result["month"])
    future = matched & ~available
    current = available & (result["month"] == result["rating_as_of_month"])
    result["rating_bucket"] = result["rating_bucket"].where(available, "NR").fillna("NR")
    result["rating_state"] = np.where(
        current,
        "static_current",
        np.where(available, "static_carry_forward", "static_missing"),
    )
    result["rating_reason"] = np.where(
        current,
        "static_rating_current",
        np.where(available, "static_rating_carry_forward", np.where(future, "static_rating_future", "static_rating_absent")),
    )
    result.loc[future, "rating_as_of_month"] = pd.NaT
    result["rating_staleness_months"] = (
        result["month"].dt.to_period("M") - result["rating_as_of_month"].dt.to_period("M")
    ).apply(lambda value: value.n if pd.notna(value) else pd.NA)
    return result[["cusip_id", "month", "rating_bucket", "rating_as_of_month", "rating_state", "rating_reason", "rating_staleness_months"]]


def rating_coverage(targets: pd.DataFrame, buckets: pd.Series) -> pd.Series:
    return buckets.ne("NR").groupby(targets["month"].dt.year).mean().round(4)


def coupon_from_price_ytm(price: pd.Series, ytm: pd.Series, maturity_years: pd.Series) -> pd.Series:
    y = ytm / 2
    periods = (2 * maturity_years).round().clip(lower=1).astype(int)
    disc = (1 + y) ** (-periods)
    with np.errstate(divide="ignore", invalid="ignore"):
        annuity = (1 - disc) / y
        coupon = (price / 100 - disc) / annuity * 200
    return coupon.where(annuity > 1e-9, ytm * 100).clip(0, 20)


def bond_coupons(panel: pd.DataFrame) -> pd.Series:
    return coupon_from_price_ytm(panel["pr"], panel["ytm"], panel["bond_maturity"]).groupby(panel["cusip_id"], observed=True).transform("median")


def monthly_returns(
    panel: pd.DataFrame, terminal_exits: pd.DataFrame | None = None
) -> pd.DataFrame:
    df = panel[["cusip_id", "month", "pr", "ytm", "bond_maturity"]].copy().sort_values(["cusip_id", "month"])
    df["coupon"] = bond_coupons(panel.loc[df.index])
    group = df.groupby("cusip_id", observed=True)
    previous_price, previous_month = group["pr"].shift(), group["month"].shift()
    consecutive = (df["month"] - previous_month).dt.days.between(28, 31)
    price_return = (df["pr"] - previous_price) / previous_price
    carry_return = (df["coupon"] / 12) / previous_price
    out = pd.DataFrame({"cusip_id": df["cusip_id"], "month": df["month"], "total_return": (price_return + carry_return).where(consecutive), "price_return": price_return.where(consecutive), "carry_return": carry_return.where(consecutive)}).dropna(subset=["total_return"]).reset_index(drop=True)
    out["exit_basis"] = "observed"
    out["exit_reason"] = None
    if terminal_exits is not None and not terminal_exits.empty:
        required = {"cusip_id", "month", "bond_maturity", "pr", "ytm", "rating_bucket"}
        missing = required.difference(terminal_exits.columns)
        if missing:
            raise ValueError(f"terminal exits missing columns: {sorted(missing)}")
        attrs = terminal_exits.reset_index(drop=True)
        realized, _closed, reasons = apply_typed_exits(
            np.full(len(attrs), np.nan), attrs, np.ones(len(attrs), dtype=bool), return_reasons=True
        )
        terminal = pd.DataFrame({
            "cusip_id": attrs["cusip_id"], "month": attrs["month"], "total_return": realized,
            "price_return": np.nan, "carry_return": np.nan, "exit_basis": reasons,
            "exit_reason": reasons,
        })
        existing = pd.MultiIndex.from_frame(out[["cusip_id", "month"]])
        terminal = terminal[~pd.MultiIndex.from_frame(terminal[["cusip_id", "month"]]).isin(existing)]
        out = pd.concat([out, terminal], ignore_index=True).sort_values(["cusip_id", "month"]).reset_index(drop=True)
    out["suspect"] = out["total_return"].abs() > .5
    return out


def returns_report(returns: pd.DataFrame) -> dict[str, Any]:
    r = returns["total_return"]
    return {"rows": int(len(returns)), "cusips": int(returns["cusip_id"].nunique()), "mean_monthly_pct": round(float(r.mean()) * 100, 4), "median_monthly_pct": round(float(r.median()) * 100, 4), "p01_pct": round(float(r.quantile(.01)) * 100, 2), "p99_pct": round(float(r.quantile(.99)) * 100, 2), "suspect_share": round(float(returns["suspect"].mean()), 6)}


def apply_typed_exits(r: np.ndarray, attrs: pd.DataFrame, active: np.ndarray, counts: dict[str, int] | None = None, *, return_reasons: bool = False) -> tuple[Any, ...]:
    out, exited = np.array(r, dtype=float, copy=True), np.zeros(len(r), dtype=bool)
    reasons = np.full(len(r), None, dtype=object)
    for i in np.where(np.isnan(out) & active)[0]:
        row = attrs.iloc[i]
        if pd.notna(row.get("bond_maturity")) and float(row["bond_maturity"]) <= 1.25:
            price = float(row["pr"])
            coupon = coupon_from_price_ytm(
                pd.Series([price]),
                pd.Series([row.get("ytm")]),
                pd.Series([row["bond_maturity"]]),
            ).iloc[0]
            carry = float(coupon) / 12 if pd.notna(coupon) else 0.0
            out[i], key = (100.0 + carry - price) / price, "matured"
        elif pd.notna(row.get("pr")) and (float(row["pr"]) < 70 or row.get("rating_bucket") in ("CCC", "D")):
            out[i], key = (FROZEN["recovery_rate"] * 100 - float(row["pr"])) / float(row["pr"]), "distressed"
        else:
            out[i], key = 0., "unexplained"
        exited[i], reasons[i] = True, key
        if counts is not None:
            counts[key] = counts.get(key, 0) + 1
    return (out, exited, reasons) if return_reasons else (out, exited)


def analytical_mod_dur(ytm: pd.Series, coupon_pct: pd.Series, maturity_years: pd.Series) -> pd.Series:
    y, coupon, periods = ytm.astype(float) / 2, coupon_pct.astype(float) / 200, (2 * maturity_years.astype(float)).round().clip(lower=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        annuity = (1 - (1 + y) ** (-periods)) / y
        price = coupon * annuity + (1 + y) ** (-periods)
        half = (coupon * ((1 + y) / y * annuity - periods * (1 + y) ** (-periods) / y) + periods * (1 + y) ** (-periods)) / price
    return pd.Series((half / 2) / (1 + y), index=ytm.index).where(ytm.between(-.02, .60) & (maturity_years > 0))


def _bullet_dirty_price(ytm: np.ndarray, coupon_pct: np.ndarray, periods: np.ndarray, period_frac: np.ndarray) -> np.ndarray:
    half_y, half_coupon = ytm / 2, coupon_pct / 2
    safe = np.where(np.abs(half_y) < 1e-12, 1e-12, half_y)
    annuity = (1 - (1 + safe) ** (-periods)) / safe
    return half_coupon * annuity * (1 + safe) ** (1 - period_frac) + 100 * (1 + safe) ** (-(periods - 1 + period_frac))


def analytical_ytm(clean_price: pd.Series, coupon_pct: pd.Series, maturity_years: pd.Series) -> pd.Series:
    price, coupon, years = clean_price.astype(float).to_numpy(), coupon_pct.astype(float).to_numpy(), maturity_years.astype(float).to_numpy()
    usable = np.isfinite(price) & np.isfinite(coupon) & np.isfinite(years) & (price > 0) & (coupon >= 0) & (years > 0)
    periods = np.maximum(np.where(usable, np.ceil(years * 2), 1), 1)
    frac = np.clip(years * 2 - (periods - 1), 1e-6, 1)
    dirty = price + coupon / 2 * (1 - frac)
    low, high = -0.5, 5.0
    lo, hi = np.full(price.shape, low), np.full(price.shape, high)
    for _ in range(80):
        mid = (lo + hi) / 2
        lo = np.where(_bullet_dirty_price(mid, coupon, periods, frac) > dirty, mid, lo)
        hi = np.where(_bullet_dirty_price(mid, coupon, periods, frac) > dirty, hi, mid)
    bracket = (dirty <= _bullet_dirty_price(np.full(price.shape, low), coupon, periods, frac)) & (dirty >= _bullet_dirty_price(np.full(price.shape, high), coupon, periods, frac))
    return pd.Series(np.where(usable & bracket, (lo + hi) / 2, np.nan), index=clean_price.index)


def structural_carry_forward(panel: pd.DataFrame) -> pd.DataFrame:
    return panel.sort_values("month").groupby("cusip_id", observed=True).last().reset_index()[["cusip_id", "amt_outstanding_k", "ff17num", "db_type"]]


def build_db_monthly_panel(
    daily_observations: pd.DataFrame,
    reference_terms: pd.DataFrame,
    monthly_curve: pd.DataFrame,
    resolved_issuer_sector: pd.DataFrame,
    monthly_liquidity: pd.DataFrame,
    static_rating_mapping: pd.DataFrame,
    *,
    months: list[pd.Timestamp],
) -> pd.DataFrame:
    """Build runtime rows solely from DB-shaped observation/reference surfaces.

    ``daily_observations`` corresponds to ``bond_observation_daily``; terms,
    issuer/sector, liquidity and ratings are likewise database query frames.
    This is deliberately separate from the parity/backfill-only frame fusion.
    """
    obs = daily_observations.rename(columns={"cusip9": "cusip_id", "day": "observation_date", "volume": "dollar_volume"}).copy()
    if obs.empty:
        values = pd.DataFrame(columns=["cusip_id", "month", "price", "ytm", "trade_count", "dollar_volume", "observed_days"])
    else:
        if "trade_count" not in obs:
            obs["trade_count"] = 1
        obs["observation_date"] = pd.to_datetime(obs["observation_date"])
        obs["month"] = obs["observation_date"].dt.to_period("M").dt.to_timestamp()
        obs = obs[obs["month"].isin(months)]
        values = obs.groupby(["cusip_id", "month"], observed=True).agg(
            price=("price", "median"), ytm=("ytm", "median"), trade_count=("trade_count", "sum"),
            dollar_volume=("dollar_volume", lambda values: values.sum(min_count=1)), observed_days=("observation_date", "nunique"),
        ).reset_index()
    terms = reference_terms.rename(columns={"cusip9": "cusip_id", "amount_outstanding_k": "amt_outstanding_k", "coupon_rate": "coupon_pct"}).copy()
    if "amt_outstanding_k" not in terms:
        terms["amt_outstanding_k"] = np.nan
    if {"tenor", "day", "yield_pct"}.issubset(monthly_curve.columns):
        curve = monthly_curve.copy()
        tenor_map = {"1y": "DGS1", "2y": "DGS2", "3y": "DGS3", "5y": "DGS5", "7y": "DGS7", "10y": "DGS10", "20y": "DGS20", "30y": "DGS30"}
        curve["tenor"] = curve["tenor"].astype(str).str.lower().str.strip().map(tenor_map)
        curve = curve.dropna(subset=["tenor"])
        daily_curve = curve.pivot_table(index=pd.to_datetime(curve["day"]), columns="tenor", values="yield_pct", aggfunc="median")
        monthly_curve = monthly_treasury_curve(daily_curve)
    sector = resolved_issuer_sector.rename(columns={"cusip9": "cusip_id"}).copy()
    liquidity = monthly_liquidity.rename(columns={"cusip9": "cusip_id"}).copy()
    if liquidity.empty and not {"cusip_id", "month"}.issubset(liquidity.columns):
        liquidity = pd.DataFrame(columns=["cusip_id", "month"])
    if "month" in liquidity:
        # psycopg returns PostgreSQL DATE as datetime.date/object; the resolver's
        # canonical monthly key is Timestamp, matching observation-derived rows.
        liquidity["month"] = pd.to_datetime(liquidity["month"])
    ratings_input = static_rating_mapping.rename(columns={"cusip9": "cusip_id"}).copy()
    if "issuer_id" not in sector.columns:
        raise ValueError("resolved issuer_id is required for the DB monthly panel")
    if "month" in sector:
        sector["month"] = pd.to_datetime(sector["month"])
        if sector.duplicated(["cusip_id", "month"]).any():
            raise ValueError("resolved issuer/sector candidates contain duplicate CUSIP-months")
        candidates = sector[sector["month"].isin(pd.to_datetime(months))].copy()
    else:
        if sector["cusip_id"].duplicated().any():
            raise ValueError("resolved issuer/sector candidates contain duplicate CUSIPs")
        month_frame = pd.DataFrame({"month": pd.to_datetime(months)})
        sector["_join"] = 1
        month_frame["_join"] = 1
        candidates = sector.merge(month_frame, on="_join", how="inner").drop(columns="_join")
    out = candidates.merge(values, on=["cusip_id", "month"], how="left").merge(terms, on="cusip_id", how="left")
    out = out.merge(liquidity, on=["cusip_id", "month"], how="left", suffixes=("", "_liquidity"))
    if "dollar_volume_liquidity" in out:
        out["dollar_volume"] = out["dollar_volume"].combine_first(out["dollar_volume_liquidity"])
    out["traded_days"] = out.get("traded_days", out["observed_days"]).fillna(out["observed_days"])
    out["maturity_date"] = pd.to_datetime(out["maturity_date"], errors="coerce")
    out["bond_maturity"] = (out["maturity_date"] - out["month"]).dt.days / 365.25
    observed = out["ytm"].notna()
    solved = analytical_ytm(out["price"], out["coupon_pct"], out["bond_maturity"])
    out["ytm"] = out["ytm"].combine_first(solved)
    out["ytm_basis"] = np.where(observed, "observed", np.where(solved.notna(), "analytical", "missing"))
    observed_duration = out.get("mod_dur", pd.Series(np.nan, index=out.index)).notna()
    duration = analytical_mod_dur(out["ytm"], out["coupon_pct"], out["bond_maturity"])
    out["mod_dur"] = out.get("mod_dur", pd.Series(np.nan, index=out.index)).combine_first(duration)
    out["mod_dur_source"] = np.where(observed_duration, "observed", np.where(duration.notna(), "analytical", "missing"))
    out["spread_final"] = compute_spread(out, monthly_curve)
    out["spread_final_bps"] = out["spread_final"] * 10_000
    out["spread_definition"] = "ytm_minus_interpolated_dgs"
    targets = out[["cusip_id", "month"]]
    ratings = ratings_static_mapping(ratings_input, targets)
    out = out.merge(ratings, on=["cusip_id", "month"], how="left")
    out["pr"] = out["price"]
    out["amt_outstanding_k"] = out["amt_outstanding_k"]
    return out


def _extension_rows(
    monthly: pd.DataFrame,
    structurals: pd.DataFrame,
    source: str,
    monthly_curve: pd.DataFrame | None,
) -> pd.DataFrame:
    """Pure live-tail harmonization; profiles are caller-provided columns, never files."""
    if monthly.empty:
        return monthly.copy()
    df = monthly.copy().merge(structurals, on="cusip_id", how="left", suffixes=("", "_structural"))
    for column in ("amt_outstanding_k", "ff17num", "db_type"):
        structural = f"{column}_structural"
        if structural in df:
            df[column] = df.get(column, pd.Series(np.nan, index=df.index)).combine_first(df[structural])
            df = df.drop(columns=structural)
    df["structural_basis"] = "carry_forward"
    df["maturity_date"] = pd.to_datetime(df.get("maturity_date"), errors="coerce")
    df["bond_maturity"] = (df["maturity_date"] - pd.to_datetime(df["month"])).dt.days / 365.25
    observed = df.get("ytm", pd.Series(np.nan, index=df.index)).notna()
    solved = analytical_ytm(df["pr"], df["coupon_pct"], df["bond_maturity"])
    df["ytm"] = df.get("ytm", pd.Series(np.nan, index=df.index)).combine_first(solved)
    df["ytm_basis"] = np.where(observed, "observed", np.where(solved.notna(), "analytical", None))
    observed_duration = df.get("mod_dur", pd.Series(np.nan, index=df.index)).notna()
    duration = analytical_mod_dur(df["ytm"], df["coupon_pct"], df["bond_maturity"])
    df["mod_dur"] = df.get("mod_dur", pd.Series(np.nan, index=df.index)).combine_first(duration)
    df["mod_dur_source"] = np.where(observed_duration, "observed", np.where(duration.notna(), "analytical", None))
    df["price_source"] = source
    if monthly_curve is None:
        df["spread_final"] = np.nan
        df["spread_source"] = None
    else:
        df["spread_final"] = compute_spread(df, monthly_curve)
        df["spread_source"] = np.where(df["spread_final"].notna(), "computed", None)
    return df


def fuse_backfill_reference_frames(
    osbap_panel: pd.DataFrame,
    trace_monthly: pd.DataFrame,
    finnhub_monthly: pd.DataFrame,
    monthly_curve: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """One-time parity/backfill-only frame fusion; never a production runtime input."""
    base = osbap_panel.copy()
    base["price_source"] = "osbap"
    base["ytm_basis"] = base.get("ytm_basis", pd.Series("observed", index=base.index)).fillna("observed")
    base["mod_dur_source"] = base.get("mod_dur_source", pd.Series("observed", index=base.index)).fillna("observed")
    base["structural_basis"] = base.get("structural_basis", pd.Series("observed", index=base.index)).fillna("observed")
    structurals = structural_carry_forward(base)
    cutoff = base["month"].max() if not base.empty else pd.Timestamp.min
    trace = _extension_rows(trace_monthly[trace_monthly["month"] > cutoff], structurals, "trace_local", monthly_curve) if not trace_monthly.empty else trace_monthly.copy()
    finnhub = finnhub_monthly[finnhub_monthly["month"] > cutoff].copy() if not finnhub_monthly.empty else finnhub_monthly.copy()
    if not trace.empty and not finnhub.empty:
        covered = pd.MultiIndex.from_frame(trace[["cusip_id", "month"]])
        finnhub = finnhub[~pd.MultiIndex.from_frame(finnhub[["cusip_id", "month"]]).isin(covered)]
    finnhub = _extension_rows(finnhub, structurals, "finnhub", monthly_curve)
    out = pd.concat([base, trace, finnhub], ignore_index=True, sort=False)
    if out.duplicated(["cusip_id", "month"]).any():
        raise ValueError("duplicate cusip-months after fusion")
    return out.sort_values(["cusip_id", "month"]).reset_index(drop=True)


# Compatibility only for existing parity tests; production uses build_db_monthly_panel.
fuse_live_panel = fuse_backfill_reference_frames


def _design(frame: pd.DataFrame) -> pd.DataFrame:
    maturity = pd.to_numeric(frame["bond_maturity"], errors="coerce").astype(float)
    amount = pd.to_numeric(frame["amt_outstanding_k"], errors="coerce").astype(float)
    volume = pd.to_numeric(frame["dollar_volume"], errors="coerce").astype(float)
    x = pd.DataFrame({"log_maturity": np.log(maturity.clip(lower=.25)), "log_amt": np.log(amount.clip(lower=1)), "log_volume": np.log1p(volume.clip(lower=0))}, index=frame.index)
    rating = pd.get_dummies(frame["rating_bucket"], prefix="q", dtype=float).drop(columns=["q_BBB"], errors="ignore")
    sector = pd.get_dummies(frame["ff17num"].astype(int), prefix="s", dtype=float)
    return pd.concat([x, rating, sector.iloc[:, 1:] if len(sector.columns) > 1 else sector], axis=1)


def fit_month(
    snapshot: pd.DataFrame, as_of: pd.Timestamp | None = None
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if as_of is not None and (pd.to_datetime(snapshot["month"]) > as_of).any():
        raise ValueError("future rows are forbidden in a walk-forward fit")
    df = snapshot[snapshot["spread_final"].notna() & snapshot["ff17num"].notna()].copy()
    df["spread_bps"] = (df["spread_final"] * 10_000).clip(*SPREAD_WINSOR_BPS)
    if len(df) < MIN_MONTH_ROWS:
        return pd.DataFrame(), {"n": int(len(df)), "skipped": True}
    x = sm.add_constant(_design(df)).astype("float64")
    x = x.drop(columns=[column for column in x if x[column].isna().all()])
    df, x = df[x.notna().all(axis=1)], x[x.notna().all(axis=1)]
    if len(df) < MIN_MONTH_ROWS:
        return pd.DataFrame(), {"n": int(len(df)), "skipped": True}
    if "issuer_id" not in df or df["issuer_id"].isna().any():
        raise ValueError("spread model requires resolved issuer_id for clustered errors")
    fit = sm.OLS(df["spread_bps"].astype(float), x).fit(
        cov_type="cluster", cov_kwds={"groups": df["issuer_id"]}
    )
    residual = df["spread_bps"] - fit.predict(x)
    continuous = [column for column in ("log_maturity", "log_amt", "log_volume") if column in x]
    try:
        max_vif = float(np.nanmax([variance_inflation_factor(x[continuous].to_numpy(), index) for index in range(len(continuous))]))
    except Exception:
        max_vif = float("nan")
    return pd.DataFrame({"cusip_id": df["cusip_id"], "month": df["month"], "spread_bps": df["spread_bps"], "fitted_bps": fit.predict(x), "residual_bps": residual, "rv_signal": (residual - residual.mean()) / residual.std(ddof=0)}).reset_index(drop=True), {"n": int(len(df)), "r2": round(float(fit.rsquared), 4), "max_vif_continuous": round(max_vif, 2), "skipped": False}


def fit_all_months(
    snapshots: pd.DataFrame, as_of: pd.Timestamp | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    signals, diagnostics = [], []
    for month, frame in snapshots.groupby("month"):
        signal, diag = fit_month(frame, as_of=as_of)
        diagnostics.append({**diag, "month": month})
        if not signal.empty:
            signals.append(signal)
    return (pd.concat(signals, ignore_index=True) if signals else pd.DataFrame(), pd.DataFrame(diagnostics))
