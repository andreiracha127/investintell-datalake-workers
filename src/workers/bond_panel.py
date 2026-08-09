"""DB-only daily delta publication for the bond research panel.

This worker is intentionally a thin orchestration layer.  The panel arithmetic
lives in :mod:`src.bonds.panel_resolvers` and the immutable publication protocol
lives in :mod:`src.bonds.panel_materializer`; this module only reads the current
database surfaces, pins the two permitted months, and classifies failures.
"""
from __future__ import annotations

import os
import time
import json
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
from src.bonds.distribution_series import (
    DistributionSeriesError,
    resolve_reg_s_cusip_map_from_db,
)
from src.db import connect, resolve_dsn

PANEL_CONFIG_HASH = "180a82b3f1413d43"
REQUIRED_RELATIONS = (
    "bond_observation_daily",
    "bond_price_observation",
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
    "bond_panel_snapshot",
    "bond_panel_returns",
    "bond_panel_current_snapshot_v1",
    "bond_distribution_mapping_snapshot",
    "bond_distribution_snapshot_approval",
    "bond_distribution_pair_decision",
    "bond_distribution_pair_identifier",
    "bond_distribution_parser_observation",
)
REQUIRED_COLUMNS = {
    "bond_observation_daily": {"cusip9", "day", "price", "ytm", "volume"},
    "bond_price_observation": {"security_id", "observation_date", "db_type", "db_type_state"},
    "bond_reference_terms": {"cusip9", "coupon_rate", "maturity_date", "amount_outstanding_mm", "amount_outstanding_vendor", "asset", "asset_type", "bond_type", "debt_type"},
    "bond_yield_curve_daily": {"day", "tenor", "yield_pct"},
    "bond_issuer_sector": {"cusip9", "ff17num"},
    "bond_liquidity_monthly": {"cusip9", "month", "quoted_days", "rel_bid_ask_bps", "dollar_volume", "quote_state", "reason_code"},
    "bond_tick_daily": {"cusip9", "day", "bid_ask_bps", "par_volume", "price_median"},
    "bond_rating_static": {"cusip9", "rating_bucket", "rating_as_of_month", "rating_state", "reason_code", "source_sha256"},
    "bond_curated_universe": {"cusip9"},
    "sec_cusip_ticker_map": {"cusip", "issuer_cik"},
    "sec_current_bond_security_v1": {"security_id", "issuer_name", "identity_state", "currency"},
    "sec_current_bond_security_alias_v1": {"security_id", "alias_kind", "alias_value", "valid_from", "valid_to"},
    "bond_panel_publications": {"publication_id", "parent_publication_id", "publication_status", "config_hash", "first_month", "last_closed_month", "open_month", "source_lineage"},
    "bond_panel_app_pointer": {"product", "publication_id"},
    "bond_panel_snapshot": {"publication_id", "month", "cusip_id", "amount_outstanding_k"},
    "bond_panel_returns": {"publication_id", "month", "cusip_id", "total_return"},
    "bond_panel_current_snapshot_v1": {"cusip_id", "month", "price", "ytm", "maturity_years"},
    "bond_distribution_mapping_snapshot": {"snapshot_id", "snapshot_status", "content_hash"},
    "bond_distribution_snapshot_approval": {"snapshot_id", "content_hash"},
    "bond_distribution_pair_decision": {"decision_id", "snapshot_id", "decision_state", "source_observation_id", "valid_from", "valid_to", "pair_key"},
    "bond_distribution_pair_identifier": {"identifier_id", "decision_id", "source_observation_id", "distribution_rule", "identifier_kind", "identifier_value", "identifier_tenure", "valid_from", "valid_to"},
    "bond_distribution_parser_observation": {"parser_observation_id", "source_evidence_id", "parser_version", "block_locator", "exact_source_label", "source_value", "normalized_value", "observation_state"},
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
        "SELECT p.publication_id::text, p.parent_publication_id::text, p.first_month, "
        "p.last_closed_month, p.open_month, "
        "(SELECT max(s.month) FROM bond_panel_snapshot s WHERE s.publication_id = p.publication_id), "
        "(SELECT max(r.month) FROM bond_panel_returns r WHERE r.publication_id = p.publication_id), "
        "p.source_lineage "
        "FROM bond_panel_app_pointer pointer "
        "JOIN bond_panel_publications p ON p.publication_id = pointer.publication_id "
        "WHERE pointer.product = 'bond_panel_v1' "
        "AND p.publication_status = 'validated' AND p.config_hash = %s",
        (PANEL_CONFIG_HASH,),
    ).fetchone()
    if row is None:
        return None
    return {
        "publication_id": str(row[0]),
        "parent_publication_id": str(row[1]) if row[1] is not None else None,
        "first_month": row[2],
        "last_closed_month": row[3],
        "open_month": row[4],
        "snapshot_max_month": row[5],
        "returns_max_month": row[6],
        "source_lineage": row[7] if isinstance(row[7], dict) else {},
    }


def _parent_return_anchor(conn: Any, closed_month: pd.Timestamp) -> pd.DataFrame:
    """The parent supplies only the predecessor needed to realize closed returns."""
    anchor = _frame(
        conn,
        "SELECT cusip_id, month, price AS pr, ytm, maturity_years AS bond_maturity, "
        "rating_bucket, rating_state, NULLIF(payload ->> 'rating_as_of_month', '')::date AS rating_as_of_month, "
        "payload ->> 'rating_reason' AS rating_reason, "
        "NULLIF(payload ->> 'rating_staleness_months', '')::int AS rating_staleness_months, "
        "eligibility_state, issuer_id, issuer_identity_state, ff17num, currency, asset_class, "
        "amount_outstanding_k AS amt_outstanding_k, maturity_date, coupon_pct, db_type "
        "FROM bond_panel_current_snapshot_v1 "
        "WHERE eligibility_state = 'included' AND month = (SELECT max(month) FROM bond_panel_current_snapshot_v1 WHERE month < %s)",
        (closed_month.date(),),
    )
    anchor["month"] = pd.to_datetime(anchor["month"])
    for column in ("pr", "ytm", "bond_maturity", "amt_outstanding_k", "coupon_pct", "db_type"):
        if column in anchor:
            anchor[column] = pd.to_numeric(anchor[column], errors="coerce")
    return anchor


def _load_inputs(
    conn: Any,
    closed_month: pd.Timestamp,
    open_month: pd.Timestamp,
    as_of: date,
    *,
    mapping_snapshot_id: str,
    structural_publication_id: str | None = None,
    structural_month: date | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    """Load a strict Reg S execution panel from one approved mapping snapshot."""
    start = closed_month.date()
    end = as_of
    closed_as_of = (closed_month + pd.offsets.MonthEnd(1)).date()
    references = _frame(
        conn,
        "SELECT upper(btrim(cusip9)) AS reference_cusip9 FROM bond_curated_universe "
        "WHERE nullif(btrim(cusip9), '') IS NOT NULL ORDER BY upper(btrim(cusip9))",
    )
    reference_cusip9s = references["reference_cusip9"].astype(str).tolist()
    closed_resolution_map = resolve_reg_s_cusip_map_from_db(
        conn,
        snapshot_id=mapping_snapshot_id,
        as_of=closed_as_of,
        reference_cusip9s=reference_cusip9s,
    )
    open_resolution_map = resolve_reg_s_cusip_map_from_db(
        conn,
        snapshot_id=mapping_snapshot_id,
        as_of=as_of,
        reference_cusip9s=reference_cusip9s,
    )
    resolution_windows = (
        (closed_month.date(), closed_as_of, closed_resolution_map),
        (open_month.date(), as_of, open_resolution_map),
    )
    mapping_rows = [
        {
            "reference_cusip9": resolution.reference_cusip9,
            "execution_cusip9": resolution.reg_s_cusip9,
            "decision_id": resolution.decision_id,
            "month": month.isoformat(),
        }
        for month, _mapping_as_of, resolution_map in resolution_windows
        for _reference, resolution in sorted(resolution_map.resolutions.items())
    ]
    for month, _mapping_as_of, resolution_map in resolution_windows:
        if not resolution_map.resolutions:
            raise ValueError(f"reg_s_mapping_zero_approved:{month.isoformat()}")
    mapping_json = json.dumps(mapping_rows, sort_keys=True, separators=(",", ":"))
    mapping_cte = (
        "WITH mapping AS (SELECT reference_cusip9, execution_cusip9, decision_id, month "
        "FROM jsonb_to_recordset(%s::jsonb) AS mapped("
        "reference_cusip9 text, execution_cusip9 text, decision_id text, month date))"
    )

    inputs = {
        "daily_observations": _frame(
            conn,
            mapping_cte
            + " SELECT m.execution_cusip9 AS cusip9, o.day, o.price, o.ytm, o.volume "
            "FROM bond_observation_daily o JOIN mapping m "
            "ON upper(btrim(o.cusip9)) = m.execution_cusip9 "
            "AND date_trunc('month', o.day)::date = m.month "
            "WHERE o.day >= %s AND o.day <= %s AND (o.price IS NOT NULL OR o.ytm IS NOT NULL)",
            (mapping_json, start, end),
        ),
        "reference_terms": _frame(
            conn,
            mapping_cte
            + " SELECT DISTINCT m.execution_cusip9 AS cusip9, r.coupon_rate, r.maturity_date, r.amount_outstanding_mm, "
            "r.amount_outstanding_vendor, prior.amount_outstanding_k, r.asset, r.asset_type, r.bond_type, r.debt_type "
            "FROM mapping m JOIN bond_reference_terms r ON upper(btrim(r.cusip9)) = m.reference_cusip9 "
            "LEFT JOIN bond_panel_snapshot prior "
            "ON prior.publication_id = %s::uuid AND prior.month = %s "
            "AND upper(btrim(prior.cusip_id)) = m.execution_cusip9",
            (mapping_json, structural_publication_id, structural_month),
        ),
        "monthly_curve": _frame(
            conn,
            "SELECT day, tenor, yield_pct FROM bond_yield_curve_daily "
            "WHERE day >= %s AND day <= %s", (start, end),
        ),
        "resolved_issuer_sector": _frame(
            conn,
            mapping_cte
            + ", panel_months(month, price_as_of) AS (SELECT DISTINCT month, price_as_of FROM "
            "(VALUES (%s::date, %s::date), (%s::date, %s::date)) AS requested(month, price_as_of)), "
            "aliases AS (SELECT pm.month, upper(btrim(a.alias_value)) AS cusip9, a.security_id "
            "FROM panel_months pm JOIN sec_current_bond_security_alias_v1 a ON a.alias_kind = 'cusip9' "
            "AND a.valid_from <= pm.price_as_of AND (a.valid_to IS NULL OR a.valid_to > pm.price_as_of)), "
            "price AS (SELECT pm.month, p.security_id, "
            "CASE WHEN count(DISTINCT p.db_type) FILTER (WHERE p.db_type_state = 'present' AND p.db_type IS NOT NULL AND p.db_type <> 'NaN'::numeric AND p.db_type = trunc(p.db_type)) = 1 "
            "THEN max(p.db_type) FILTER (WHERE p.db_type_state = 'present' AND p.db_type IS NOT NULL AND p.db_type <> 'NaN'::numeric AND p.db_type = trunc(p.db_type)) END AS db_type, "
            "CASE WHEN count(DISTINCT p.db_type) FILTER (WHERE p.db_type_state = 'present' AND p.db_type IS NOT NULL AND p.db_type <> 'NaN'::numeric AND p.db_type = trunc(p.db_type)) = 1 THEN 'pit_present' "
            "WHEN count(DISTINCT p.db_type) FILTER (WHERE p.db_type_state = 'present' AND p.db_type IS NOT NULL AND p.db_type <> 'NaN'::numeric AND p.db_type = trunc(p.db_type)) = 0 THEN 'pit_missing_or_invalid' ELSE 'pit_conflicting' END AS db_type_reason "
            "FROM panel_months pm CROSS JOIN LATERAL bond_price_fund_asof_v1(pm.price_as_of) p GROUP BY pm.month, p.security_id) "
            "SELECT m.execution_cusip9 AS cusip9, pm.month, map.issuer_cik AS issuer_id, "
            "COALESCE(map.issuer_identity_state, 'unresolved') AS issuer_identity_state, s.currency, "
            "CASE WHEN concat_ws(' ', r.asset, r.asset_type, r.bond_type, r.debt_type) ~* '(^|[^[:alnum:]])non[-[:space:]]*corporate([^[:alnum:]]|$)' THEN 'noncorporate' "
            "WHEN concat_ws(' ', r.asset, r.asset_type, r.bond_type, r.debt_type) ~* '(^|[^[:alnum:]])corporate([^[:alnum:]]|$)' THEN 'corporate' "
            "WHEN concat_ws(' ', r.asset, r.asset_type, r.bond_type, r.debt_type) <> '' THEN 'noncorporate' ELSE 'missing' END AS asset_class, i.ff17num, "
            "price.db_type, COALESCE(price.db_type_reason, CASE WHEN a.security_id IS NULL THEN 'security_alias_missing' ELSE 'db_type_pit_absent' END) AS db_type_reason "
            "FROM mapping m JOIN panel_months pm ON pm.month = m.month "
            "LEFT JOIN aliases a ON a.month = pm.month AND a.cusip9 = m.execution_cusip9 "
            "LEFT JOIN sec_current_bond_security_v1 s ON s.security_id = a.security_id "
            "LEFT JOIN bond_reference_terms r ON upper(btrim(r.cusip9)) = m.reference_cusip9 "
            "LEFT JOIN bond_issuer_sector i ON upper(btrim(i.cusip9)) = m.reference_cusip9 "
            "LEFT JOIN price ON price.month = pm.month AND price.security_id = a.security_id "
            "LEFT JOIN (SELECT upper(btrim(cusip)) AS cusip9, CASE WHEN count(DISTINCT issuer_cik) FILTER (WHERE issuer_cik IS NOT NULL) = 1 THEN max(issuer_cik::text) FILTER (WHERE issuer_cik IS NOT NULL) END AS issuer_cik, "
            "CASE WHEN count(DISTINCT issuer_cik) FILTER (WHERE issuer_cik IS NOT NULL) = 1 THEN 'resolved' WHEN count(DISTINCT issuer_cik) FILTER (WHERE issuer_cik IS NOT NULL) = 0 THEN 'missing_cik' ELSE 'conflicting_cik' END AS issuer_identity_state FROM sec_cusip_ticker_map GROUP BY upper(btrim(cusip))) map ON map.cusip9 = m.reference_cusip9",
            (mapping_json, closed_month.date(), closed_as_of, open_month.date(), as_of),
        ),
        "monthly_liquidity": _frame(
            conn,
            mapping_cte
            + ", historical AS (SELECT m.execution_cusip9 AS cusip9, l.month, l.quoted_days, l.rel_bid_ask_bps, l.dollar_volume, l.quote_state, l.reason_code, 1 AS priority FROM bond_liquidity_monthly l JOIN mapping m ON upper(btrim(l.cusip9)) = m.execution_cusip9 AND l.month = m.month WHERE l.month IN (%s, %s)), "
            "live AS (SELECT m.execution_cusip9 AS cusip9, date_trunc('month', t.day)::date AS month, count(*) FILTER (WHERE t.bid_ask_bps >= 0)::int AS quoted_days, percentile_cont(.5) WITHIN GROUP (ORDER BY t.bid_ask_bps) FILTER (WHERE t.bid_ask_bps >= 0) AS rel_bid_ask_bps, sum(t.par_volume * t.price_median / 100.0) FILTER (WHERE t.par_volume IS NOT NULL AND t.price_median IS NOT NULL) AS dollar_volume, CASE WHEN count(*) FILTER (WHERE t.bid_ask_bps >= 0) > 0 THEN 'quoted' ELSE 'unquoted' END AS quote_state, CASE WHEN count(*) FILTER (WHERE t.bid_ask_bps >= 0) > 0 THEN 'live_tick_median_valid_bps' ELSE 'live_tick_missing_or_crossed_bps' END AS reason_code, 0 AS priority FROM bond_tick_daily t JOIN mapping m ON upper(btrim(t.cusip9)) = m.execution_cusip9 AND date_trunc('month', t.day)::date = m.month AND m.month = %s WHERE t.day >= %s AND t.day <= %s GROUP BY m.execution_cusip9, date_trunc('month', t.day)::date), "
            "all_rows AS (SELECT * FROM live UNION ALL SELECT * FROM historical) SELECT DISTINCT ON (cusip9, month) cusip9, month, quoted_days, rel_bid_ask_bps, dollar_volume, quote_state, reason_code FROM all_rows ORDER BY cusip9, month, priority",
            (mapping_json, closed_month.date(), open_month.date(), open_month.date(), start, end),
        ),
    }
    rating_sources = _frame(
        conn,
        "SELECT DISTINCT source_sha256 FROM bond_rating_static WHERE source_sha256 IS NOT NULL",
    )
    rating_hashes = rating_sources["source_sha256"].dropna().astype(str).unique().tolist()
    if len(rating_hashes) != 1:
        raise ValueError("bond_rating_static must contain exactly one source_sha256")
    inputs["static_rating_mapping"] = _frame(
            conn,
            mapping_cte
            + " SELECT DISTINCT m.execution_cusip9 AS cusip9, r.rating_bucket, r.rating_as_of_month, "
            "CASE WHEN r.rating_state = 'rated' THEN 'static_current' ELSE 'static_carry_forward' END AS rating_state, "
            "r.reason_code AS rating_reason, r.source_sha256 FROM bond_rating_static r JOIN mapping m "
            "ON upper(btrim(r.cusip9)) = m.reference_cusip9",
            (mapping_json,),
        )
    if inputs["static_rating_mapping"].empty:
        inputs["static_rating_mapping"] = pd.DataFrame(
            columns=[
                "cusip9", "rating_bucket", "rating_as_of_month", "rating_state",
                "rating_reason", "source_sha256",
            ]
        )
    lineage = {name: name for name in inputs}
    if structural_publication_id and structural_month:
        lineage["reference_terms"] = (
            f"bond_reference_terms+bond_panel_snapshot:{structural_publication_id}:{structural_month.isoformat()}"
        )
    lineage["static_rating_mapping"] = f"bond_rating_static:{rating_hashes[0]}"
    lineage["distribution_rule"] = "reg_s"
    lineage["distribution_mapping_snapshot_id"] = mapping_snapshot_id
    lineage["distribution_mapping_count"] = str(len(open_resolution_map.resolutions))
    lineage["distribution_mapping_closed_as_of"] = closed_as_of.isoformat()
    lineage["distribution_mapping_open_as_of"] = as_of.isoformat()
    lineage["distribution_mapping_closed_count"] = str(len(closed_resolution_map.resolutions))
    lineage["distribution_mapping_open_count"] = str(len(open_resolution_map.resolutions))
    for reason, count in sorted(
        (
            reason,
            sum(1 for value in open_resolution_map.reason_by_reference.values() if value == reason),
        )
        for reason in set(open_resolution_map.reason_by_reference.values())
    ):
        lineage[f"distribution_mapping_omission:{reason}"] = str(count)
    for reason, count in sorted(
        (
            reason,
            sum(1 for value in closed_resolution_map.reason_by_reference.values() if value == reason),
        )
        for reason in set(closed_resolution_map.reason_by_reference.values())
    ):
        lineage[f"distribution_mapping_closed_omission:{reason}"] = str(count)
    return inputs, lineage


def _code_revision() -> str | None:
    for name in ("CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA"):
        if value := (os.getenv(name) or "").strip():
            return value
    return None


def _initial_stage6_authorized(parent: dict[str, Any], revision: str) -> bool:
    """Require a revision-bound token only while the pointer still targets the base."""
    if parent.get("parent_publication_id") is not None:
        return True
    authorization = (os.getenv("BOND_PANEL_STAGE6_INITIAL_AUTHORIZATION") or "").strip()
    return bool(authorization) and authorization == revision


def _parent_distribution_reasons(
    parent: dict[str, Any], mapping_snapshot_id: str
) -> list[str]:
    """Require an exact approved Reg S mapping lineage before extending a pack."""
    lineage = parent.get("source_lineage")
    if not isinstance(lineage, dict) or lineage.get("distribution_rule") != "reg_s":
        return ["parent_distribution_rule_not_reg_s"]
    if lineage.get("distribution_mapping_snapshot_id") != mapping_snapshot_id:
        return ["parent_distribution_mapping_snapshot_mismatch"]
    return []


def _parent_integrity_reasons(parent: dict[str, Any]) -> list[str]:
    """Refuse to extend a publication whose declared partition is already false."""
    last_closed = parent.get("last_closed_month")
    if not isinstance(last_closed, date):
        return ["parent_last_closed_month_absent"]
    open_month = parent.get("open_month")
    expected_snapshot = open_month if isinstance(open_month, date) else last_closed
    snapshot_max = parent.get("snapshot_max_month")
    returns_max = parent.get("returns_max_month")
    reasons: list[str] = []
    if snapshot_max != expected_snapshot:
        actual = snapshot_max.isoformat() if isinstance(snapshot_max, date) else "absent"
        reasons.append(
            f"parent_snapshot_max_month_mismatch:{actual}:{expected_snapshot.isoformat()}"
        )
    if returns_max != last_closed:
        actual = returns_max.isoformat() if isinstance(returns_max, date) else "absent"
        reasons.append(
            f"parent_returns_max_month_mismatch:{actual}:{last_closed.isoformat()}"
        )
    return reasons


def _closed_returns_and_tombstones(
    anchor: pd.DataFrame,
    current_snapshot: pd.DataFrame,
    closed_month: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Realize observed exits and retain removed parent bonds as typed exclusions."""
    required_anchor = ["cusip_id", "month", "pr", "ytm", "bond_maturity", "rating_bucket"]
    if anchor.empty:
        anchor = pd.DataFrame(columns=required_anchor)
    current_ids = set(current_snapshot.get("cusip_id", pd.Series(dtype=object)).astype(str))
    observed_ids = set(
        current_snapshot.loc[
            current_snapshot.get("pr", pd.Series(index=current_snapshot.index, dtype=float)).notna(),
            "cusip_id",
        ].astype(str)
    ) if "cusip_id" in current_snapshot else set()
    terminal_exits = anchor[~anchor["cusip_id"].astype(str).isin(observed_ids)].copy()
    terminal_exits["month"] = closed_month
    returns_input = pd.concat([anchor, current_snapshot], ignore_index=True, sort=False)
    returns = monthly_returns(returns_input, terminal_exits=terminal_exits)
    returns = returns[returns["month"].eq(closed_month)].reset_index(drop=True)

    tombstones = terminal_exits[
        ~terminal_exits["cusip_id"].astype(str).isin(current_ids)
    ].copy().reset_index(drop=True)
    if tombstones.empty:
        return returns, tombstones
    tombstones["month"] = closed_month
    tombstones["eligibility_state"] = "excluded"
    tombstones["eligibility_reason"] = "terminal_exit_removed"
    tombstones["issuer_identity_state"] = tombstones.get(
        "issuer_identity_state", pd.Series("unresolved", index=tombstones.index)
    ).fillna("unresolved")
    tombstones["rating_bucket"] = tombstones.get(
        "rating_bucket", pd.Series("NR", index=tombstones.index)
    ).fillna("NR")
    tombstones["rating_state"] = tombstones.get(
        "rating_state", pd.Series("missing", index=tombstones.index)
    ).fillna("missing")
    tombstones["price_source"] = "terminal_parent_tombstone"
    tombstones["spread_definition"] = "ytm_minus_interpolated_dgs"
    tombstones["flags"] = [
        {"terminal_exit": True, "source": "parent_snapshot"}
        for _ in range(len(tombstones))
    ]
    for column in (
        "pr",
        "ytm",
        "ytm_basis",
        "mod_dur",
        "mod_dur_source",
        "spread_final",
        "spread_final_bps",
        "spread_source",
        "traded_days",
        "trade_count",
        "dollar_volume",
        "rel_bid_ask_bps",
        "quoted_days",
    ):
        tombstones[column] = None
    return returns, tombstones


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
        if not _initial_stage6_authorized(parent, revision):
            return _failure(
                "panel_gate_failed",
                elapsed=time.monotonic() - started,
                input_reasons=["initial_stage6_authorization_absent_or_mismatch"],
            )
        parent_reasons = _parent_integrity_reasons(parent)
        if parent_reasons:
            return _failure(
                "panel_gate_failed",
                elapsed=time.monotonic() - started,
                input_reasons=parent_reasons,
            )
        mapping_snapshot_id = (os.getenv("BOND_PANEL_REG_S_MAPPING_SNAPSHOT_ID") or "").strip()
        if not mapping_snapshot_id:
            return _failure(
                "panel_gate_failed",
                elapsed=time.monotonic() - started,
                input_reasons=["distribution_mapping_snapshot_id_absent"],
            )
        distribution_reasons = _parent_distribution_reasons(parent, mapping_snapshot_id)
        if distribution_reasons:
            return _failure(
                "panel_gate_failed",
                elapsed=time.monotonic() - started,
                input_reasons=distribution_reasons,
            )
        if (
            parent.get("last_closed_month") == closed_month.date()
            and parent.get("open_month") == open_month.date()
        ):
            return {
                "state": "current",
                "aborted": False,
                "reason": "panel_month_already_current",
                "publication_id": str(parent["publication_id"]),
                "config_hash": PANEL_CONFIG_HASH,
                "closed_month": closed_month.date().isoformat(),
                "open_month": open_month.date().isoformat(),
                "distribution_mapping_snapshot_id": mapping_snapshot_id,
            }
        try:
            inputs, lineage = _load_inputs(
                conn,
                closed_month,
                open_month,
                today,
                mapping_snapshot_id=mapping_snapshot_id,
                structural_publication_id=str(parent["publication_id"]),
                structural_month=parent["last_closed_month"],
            )
            stage_input_reasons: list[str] = []
            empty = [name for name, frame in inputs.items() if name not in {"monthly_liquidity", "static_rating_mapping"} and frame.empty]
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
            closed_snapshot = snapshot[snapshot["month"].eq(closed_month)]
            returns, tombstones = _closed_returns_and_tombstones(
                anchor, closed_snapshot, closed_month
            )
            if not tombstones.empty:
                snapshot = pd.concat([snapshot, tombstones], ignore_index=True, sort=False).sort_values(
                    ["month", "cusip_id"]
                )
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
        except DistributionSeriesError as exc:
            return _failure(
                "panel_gate_failed",
                elapsed=time.monotonic() - started,
                input_reasons=[f"distribution_mapping:{exc}"],
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
        "distribution_mapping_snapshot_id": mapping_snapshot_id,
        "distribution_mapping_coverage": {
            "mapped": int(lineage["distribution_mapping_count"]),
            "omissions": {
                key.removeprefix("distribution_mapping_omission:"): int(value)
                for key, value in lineage.items()
                if key.startswith("distribution_mapping_omission:")
            },
        },
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "input_relation_reasons": stage_input_reasons,
    }
