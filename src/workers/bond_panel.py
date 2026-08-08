"""DB-only daily delta publication for the bond research panel.

This worker is intentionally a thin orchestration layer.  The panel arithmetic
lives in :mod:`src.bonds.panel_resolvers` and the immutable publication protocol
lives in :mod:`src.bonds.panel_materializer`; this module only reads the current
database surfaces, pins the two permitted months, and classifies failures.
"""
from __future__ import annotations

import os
import time
from datetime import date
from typing import Any

import pandas as pd

from src.bonds.panel_config import config_hash
from src.bonds.panel_materializer import MaterializationError, materialize_panel
from src.bonds.panel_resolvers import (
    build_db_monthly_panel,
    build_snapshots,
    fit_all_months,
    monthly_returns,
)
from src.db import connect, resolve_dsn

PANEL_CONFIG_HASH = "0c0d78a866bc1090"
REQUIRED_RELATIONS = (
    "bond_observation_daily",
    "bond_reference_terms",
    "bond_yield_curve_daily",
    "bond_issuer_sector",
    "bond_liquidity_monthly",
    "bond_tick_daily",
    "bond_rating_static",
    "bond_curated_universe",
    "sec_cusip_ticker_map",
    "sec_current_bond_security_v1",
    "sec_current_bond_security_alias_v1",
    "bond_panel_publications",
    "bond_panel_app_pointer",
    "bond_panel_current_snapshot_v1",
)
REQUIRED_COLUMNS = {
    "bond_observation_daily": {"cusip9", "day", "price", "ytm", "volume"},
    "bond_reference_terms": {"cusip9", "coupon_rate", "maturity_date", "amount_outstanding_mm", "asset", "asset_type", "bond_type", "debt_type"},
    "bond_yield_curve_daily": {"day", "tenor", "yield_pct"},
    "bond_issuer_sector": {"cusip9", "ff17num"},
    "bond_liquidity_monthly": {"cusip9", "month", "quoted_days", "rel_bid_ask_bps", "dollar_volume", "quote_state", "reason_code"},
    "bond_tick_daily": {"cusip9", "day", "bid_ask_bps", "par_volume", "price_median"},
    "bond_rating_static": {"cusip9", "rating_bucket", "rating_as_of_month", "rating_state", "reason_code", "source_sha256"},
    "bond_curated_universe": {"cusip9"},
    "sec_cusip_ticker_map": {"cusip", "issuer_cik"},
    "sec_current_bond_security_v1": {"security_id", "issuer_name", "identity_state", "currency"},
    "sec_current_bond_security_alias_v1": {"security_id", "alias_kind", "alias_value", "valid_from", "valid_to"},
    "bond_panel_publications": {"publication_id", "publication_status", "config_hash", "first_month"},
    "bond_panel_app_pointer": {"product", "publication_id"},
    "bond_panel_current_snapshot_v1": {"cusip_id", "month", "price", "ytm", "maturity_years"},
}


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _previous_month(value: date) -> date:
    return (value.replace(day=1) - pd.Timedelta(days=1)).replace(day=1)


def _frame(conn: Any, sql: str, params: tuple[object, ...] = ()) -> pd.DataFrame:
    """Read one relation into a named frame without any local-file fallback."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [column.name for column in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def _required_relations(conn: Any) -> list[str]:
    rows = conn.execute(
        "SELECT relation FROM unnest(%s::text[]) AS required(relation) "
        "WHERE to_regclass(relation) IS NULL",
        (list(REQUIRED_RELATIONS),),
    ).fetchall()
    return [str(row[0]) for row in rows]


def _missing_columns(conn: Any) -> list[str]:
    """Fail closed on schema drift before a loader can mistake it for an outage."""
    rows = conn.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = ANY(%s)",
        (list(REQUIRED_COLUMNS),),
    ).fetchall()
    present: dict[str, set[str]] = {relation: set() for relation in REQUIRED_COLUMNS}
    for relation, column in rows:
        if relation in present:
            present[relation].add(str(column))
    return [
        f"column_absent:{relation}.{column}"
        for relation, columns in REQUIRED_COLUMNS.items()
        for column in sorted(columns.difference(present[relation]))
    ]


def _current_parent(conn: Any) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT p.publication_id::text, p.first_month "
        "FROM bond_panel_app_pointer pointer "
        "JOIN bond_panel_publications p ON p.publication_id = pointer.publication_id "
        "WHERE pointer.product = 'bond_panel_v1' "
        "AND p.publication_status = 'validated' AND p.config_hash = %s",
        (PANEL_CONFIG_HASH,),
    ).fetchone()
    if row is None:
        return None
    return {"publication_id": str(row[0]), "first_month": row[1]}


def _parent_return_anchor(conn: Any, closed_month: pd.Timestamp) -> pd.DataFrame:
    """The parent supplies only the predecessor needed to realize closed returns."""
    return _frame(
        conn,
        "SELECT cusip_id, month, price AS pr, ytm, maturity_years AS bond_maturity, rating_bucket, eligibility_state "
        "FROM bond_panel_current_snapshot_v1 "
        "WHERE eligibility_state = 'included' AND month = (SELECT max(month) FROM bond_panel_current_snapshot_v1 WHERE month < %s)",
        (closed_month.date(),),
    )


def _load_inputs(conn: Any, closed_month: pd.Timestamp, open_month: pd.Timestamp, as_of: date) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    """Load all production inputs for the closed/open delta, directly from DB."""
    start = closed_month.date()
    end = as_of
    inputs = {
        "daily_observations": _frame(
            conn,
            "SELECT cusip9, day, price, ytm, volume FROM bond_observation_daily "
            "WHERE day >= %s AND day <= %s AND (price IS NOT NULL OR ytm IS NOT NULL)",
            (start, end),
        ),
        "reference_terms": _frame(
            conn,
            "SELECT cusip9, coupon_rate, maturity_date, amount_outstanding_mm, asset, asset_type, bond_type, debt_type "
            "FROM bond_reference_terms",
        ),
        "monthly_curve": _frame(
            conn,
            "SELECT day, tenor, yield_pct FROM bond_yield_curve_daily "
            "WHERE day >= %s AND day <= %s", (start, end),
        ),
        "resolved_issuer_sector": _frame(
            conn,
            "SELECT upper(btrim(u.cusip9)) AS cusip9, map.issuer_cik AS issuer_id, "
            "COALESCE(map.issuer_identity_state, 'unresolved') AS issuer_identity_state, s.currency, "
            "CASE WHEN concat_ws(' ', r.asset, r.asset_type, r.bond_type, r.debt_type) ILIKE '%corporate%' THEN 'corporate' "
            "WHEN concat_ws(' ', r.asset, r.asset_type, r.bond_type, r.debt_type) <> '' THEN 'noncorporate' ELSE 'missing' END AS asset_class, i.ff17num "
            "FROM bond_curated_universe u "
            "LEFT JOIN sec_current_bond_security_alias_v1 a ON a.alias_kind = 'cusip9' "
            "AND upper(btrim(a.alias_value)) = upper(btrim(u.cusip9)) AND a.valid_from <= %s "
            "AND (a.valid_to IS NULL OR a.valid_to > %s) "
            "LEFT JOIN sec_current_bond_security_v1 s ON s.security_id = a.security_id "
            "LEFT JOIN bond_reference_terms r ON r.cusip9 = u.cusip9 "
            "LEFT JOIN bond_issuer_sector i ON i.cusip9 = u.cusip9 "
            "LEFT JOIN (SELECT upper(btrim(cusip)) AS cusip9, CASE WHEN count(DISTINCT issuer_cik) FILTER (WHERE issuer_cik IS NOT NULL) = 1 THEN max(issuer_cik::text) FILTER (WHERE issuer_cik IS NOT NULL) END AS issuer_cik, "
            "CASE WHEN count(DISTINCT issuer_cik) FILTER (WHERE issuer_cik IS NOT NULL) = 1 THEN 'resolved' WHEN count(DISTINCT issuer_cik) FILTER (WHERE issuer_cik IS NOT NULL) = 0 THEN 'missing_cik' ELSE 'conflicting_cik' END AS issuer_identity_state FROM sec_cusip_ticker_map GROUP BY upper(btrim(cusip))) map ON map.cusip9 = upper(btrim(u.cusip9))",
            (open_month.date(), open_month.date()),
        ),
        "monthly_liquidity": _frame(
            conn,
            "WITH historical AS (SELECT cusip9, month, quoted_days, rel_bid_ask_bps, dollar_volume, quote_state, reason_code, 1 AS priority FROM bond_liquidity_monthly WHERE month IN (%s, %s)), "
            "live AS (SELECT cusip9, date_trunc('month', day)::date AS month, count(*) FILTER (WHERE bid_ask_bps >= 0)::int AS quoted_days, percentile_cont(.5) WITHIN GROUP (ORDER BY bid_ask_bps) FILTER (WHERE bid_ask_bps >= 0) AS rel_bid_ask_bps, sum(par_volume * price_median / 100.0) FILTER (WHERE par_volume IS NOT NULL AND price_median IS NOT NULL) AS dollar_volume, CASE WHEN count(*) FILTER (WHERE bid_ask_bps >= 0) > 0 THEN 'quoted' ELSE 'unquoted' END AS quote_state, CASE WHEN count(*) FILTER (WHERE bid_ask_bps >= 0) > 0 THEN 'live_tick_median_valid_bps' ELSE 'live_tick_missing_or_crossed_bps' END AS reason_code, 0 AS priority FROM bond_tick_daily WHERE day >= %s AND day <= %s GROUP BY cusip9, date_trunc('month', day)::date), "
            "all_rows AS (SELECT * FROM live UNION ALL SELECT * FROM historical) SELECT DISTINCT ON (cusip9, month) cusip9, month, quoted_days, rel_bid_ask_bps, dollar_volume, quote_state, reason_code FROM all_rows ORDER BY cusip9, month, priority",
            (closed_month.date(), open_month.date(), start, end),
        ),
    }
    inputs["static_rating_mapping"] = _frame(
            conn,
            "SELECT cusip9, rating_bucket, rating_as_of_month, "
            "CASE WHEN rating_state = 'rated' THEN 'static_current' ELSE 'static_carry_forward' END AS rating_state, "
            "reason_code AS rating_reason, source_sha256 FROM bond_rating_static",
        )
    rating_hashes = inputs["static_rating_mapping"]["source_sha256"].dropna().astype(str).unique().tolist()
    if len(rating_hashes) != 1:
        raise ValueError("bond_rating_static must contain exactly one source_sha256")
    lineage = {name: name for name in inputs}
    lineage["static_rating_mapping"] = f"bond_rating_static:{rating_hashes[0]}"
    return inputs, lineage


def _code_revision() -> str | None:
    for name in ("CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA"):
        if value := (os.getenv(name) or "").strip():
            return value
    return None


def _records(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Make materializer payloads JSON-safe while preserving typed nulls."""
    output: list[dict[str, object]] = []
    for row in frame.to_dict(orient="records"):
        cleaned: dict[str, object] = {}
        for key, raw_value in row.items():
            value: Any = raw_value
            if value is None or value is pd.NA or (not isinstance(value, (list, dict)) and pd.isna(value)):
                cleaned[key] = None
            elif callable(getattr(value, "date", None)):
                cleaned[key] = value.date().isoformat()
            elif isinstance(value, date):
                cleaned[key] = value.isoformat()
            else:
                item = getattr(value, "item", None)
                cleaned[key] = item() if callable(item) else value
        output.append(cleaned)
    return output


def _failure(reason: str, *, elapsed: float, input_reasons: list[str] | None = None, **extra: object) -> dict[str, object]:
    return {
        "state": reason.removeprefix("panel_"),
        "reason": reason,
        "aborted": True,
        "elapsed_seconds": round(elapsed, 3),
        "input_relation_reasons": input_reasons or [],
        **extra,
    }


def run(dsn: str | None = None, *, as_of: date | None = None) -> dict[str, object]:
    """Publish the DB-only closed/open panel delta, or return a typed refusal."""
    started = time.monotonic()
    if config_hash() != PANEL_CONFIG_HASH:
        return _failure("panel_gate_failed", elapsed=time.monotonic() - started, input_reasons=["config_hash_mismatch"])
    today = as_of or date.today()
    revision = _code_revision()
    if revision is None:
        return _failure("panel_gate_failed", elapsed=time.monotonic() - started, input_reasons=["code_revision_absent"])
    open_month = pd.Timestamp(_month_start(today))
    closed_month = pd.Timestamp(_previous_month(today))
    with connect(resolve_dsn(dsn)) as conn:
        missing = _required_relations(conn)
        if missing:
            return _failure("panel_gate_failed", elapsed=time.monotonic() - started, input_reasons=[f"relation_absent:{name}" for name in missing])
        missing_columns = _missing_columns(conn)
        if missing_columns:
            return _failure("panel_gate_failed", elapsed=time.monotonic() - started, input_reasons=missing_columns)
        parent = _current_parent(conn)
        if parent is None:
            outcome = _failure("panel_gate_failed", elapsed=time.monotonic() - started, input_reasons=["panel_no_parent"])
            outcome["reason"] = "panel_no_parent"
            return outcome
        try:
            inputs, lineage = _load_inputs(conn, closed_month, open_month, today)
            stage_input_reasons: list[str] = []
            empty = [name for name, frame in inputs.items() if name not in {"monthly_liquidity"} and frame.empty]
            if empty:
                return _failure("panel_failed", elapsed=time.monotonic() - started, input_reasons=[f"relation_empty:{name}" for name in empty], closed_month=closed_month.date().isoformat(), open_month=open_month.date().isoformat())
            panel = build_db_monthly_panel(
                **inputs,
                months=[closed_month, open_month],
            )
            if panel.empty:
                return _failure("panel_failed", elapsed=time.monotonic() - started, input_reasons=["panel_rebuild_empty"], closed_month=closed_month.date().isoformat(), open_month=open_month.date().isoformat())
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
            included_closed = snapshots[snapshots["month"].eq(closed_month)]
            signals, diagnostics = fit_all_months(included_closed, as_of=closed_month)
            if not signals.empty:
                signals = signals.merge(included_closed, on=["cusip_id", "month"], how="left", suffixes=("", "_snapshot"))
            anchor = _parent_return_anchor(conn, closed_month)
            closed_snapshot = snapshots[snapshots["month"].eq(closed_month)]
            if anchor.empty:
                anchor = pd.DataFrame(columns=["cusip_id", "month", "pr", "ytm", "bond_maturity", "rating_bucket"])
            terminal_exits = anchor[~anchor["cusip_id"].isin(closed_snapshot["cusip_id"])].copy()
            terminal_exits["month"] = closed_month
            returns_input = pd.concat([anchor, closed_snapshot], ignore_index=True)
            returns = monthly_returns(returns_input, terminal_exits=terminal_exits)
            returns = returns[returns["month"].eq(closed_month)]
            rating_pit = snapshot[["cusip_id", "month", "rating_bucket", "rating_as_of_month", "rating_state", "rating_reason", "rating_staleness_months"]].copy()
            facts = {
                "snapshot": _records(snapshot),
                "rv_signal": _records(signals),
                "returns": _records(returns),
                "rating_pit": _records(rating_pit),
            }
            if any(not rows for rows in facts.values()):
                return _failure("panel_failed", elapsed=time.monotonic() - started, input_reasons=[f"surface_empty:{name}" for name, rows in facts.items() if not rows], closed_month=closed_month.date().isoformat(), open_month=open_month.date().isoformat(), diagnostics=_records(diagnostics))
            result = materialize_panel(
                conn,
                as_of=today,
                code_revision=revision,
                facts=facts,
                source_lineage=lineage,
                parent_publication_id=str(parent["publication_id"]),
                first_month=parent["first_month"],
                last_closed_month=closed_month.date(),
                open_month=open_month.date(),
            )
        except MaterializationError as exc:
            return _failure("panel_gate_failed", elapsed=time.monotonic() - started, input_reasons=[str(exc)])
        except (ValueError, KeyError) as exc:
            return _failure("panel_failed", elapsed=time.monotonic() - started, input_reasons=[f"panel_rebuild:{type(exc).__name__}:{exc}"])
        except Exception as exc:
            return _failure("panel_publish_failed", elapsed=time.monotonic() - started, input_reasons=[f"materializer:{type(exc).__name__}"])
    return {
        "state": "published",
        "aborted": False,
        "publication_id": result.publication_id,
        "parent_publication_id": result.parent_publication_id,
        "row_counts": result.row_counts,
        "config_hash": PANEL_CONFIG_HASH,
        "closed_month": closed_month.date().isoformat(),
        "open_month": open_month.date().isoformat(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "input_relation_reasons": stage_input_reasons,
    }
