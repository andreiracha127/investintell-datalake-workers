"""Recurring active-share owner for ``fund_risk_metrics``.

Reads N-PORT holdings already present in the data-lake and updates only the
active-share/holdings family for a target ``calc_date``. The benchmark mapping
is the explicit strategy-to-ETF contract observed in the live table, resolved to
current instrument/series ids at runtime.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from src.db import LOCK_ACTIVE_SHARE_METRICS, advisory_lock, connect
from src.workers.risk_metric_ownership import ACTIVE_SHARE_COLUMNS

STRATEGY_BENCHMARK_TICKERS: dict[tuple[str, str], str] = {
    ("alternatives", "Alternative"): "QAI",
    ("alternatives", "Commodities"): "GCC",
    ("alternatives", "Crypto / Digital Assets"): "BITO",
    ("alternatives", "Defined Outcome / Option Income"): "BUFR",
    ("alternatives", "Inverse / Hedge"): "SH",
    ("alternatives", "Leveraged"): "SSO",
    ("alternatives", "Precious Metals"): "RING",
    ("alternatives", "Real Estate"): "VNQ",
    ("equity", "Asian Equity"): "AAXJ",
    ("equity", "Biotechnology Equity"): "IBB",
    ("equity", "Clean Energy Equity"): "ICLN",
    ("equity", "Communication Services Equity"): "XLC",
    ("equity", "Consumer Discretionary Equity"): "XLY",
    ("equity", "Consumer Staples Equity"): "XLP",
    ("equity", "Emerging Markets Equity"): "IEMG",
    ("equity", "Energy Equity"): "XLE",
    ("equity", "ESG/Sustainable Equity"): "ESGV",
    ("equity", "European Equity"): "FEZ",
    ("equity", "Financials Equity"): "XLF",
    ("equity", "Global Equity"): "VT",
    ("equity", "Health Care Equity"): "XLV",
    ("equity", "Index / Passive"): "IVV",
    ("equity", "Industrials Equity"): "XLI",
    ("equity", "Infrastructure Equity"): "IFRA",
    ("equity", "International Equity"): "IEFA",
    ("equity", "Large Blend"): "IVV",
    ("equity", "Large Growth"): "QQQ",
    ("equity", "Large Value"): "VOOV",
    ("equity", "Long/Short Equity"): "FTLS",
    ("equity", "Materials Equity"): "XLB",
    ("equity", "Mid Blend"): "SCHM",
    ("equity", "Mid Growth"): "IWP",
    ("equity", "Mid Value"): "IWS",
    ("equity", "Natural Resources Equity"): "GUNR",
    ("equity", "Sector Rotation Equity"): "EQL",
    ("equity", "Size-Focused Equity"): "SIZE",
    ("equity", "Small Blend"): "IWM",
    ("equity", "Small Growth"): "IWO",
    ("equity", "Small Value"): "IWN",
    ("equity", "Technology"): "XLK",
    ("equity", "Utilities Equity"): "XLU",
    ("fixed_income", "Asset-Backed Securities"): "DEED",
    ("fixed_income", "Convertible Securities"): "ICVT",
    ("fixed_income", "Emerging Markets Debt"): "EMB",
    ("fixed_income", "ESG/Sustainable Bond"): "VCEB",
    ("fixed_income", "Government Bond"): "GOVT",
    ("fixed_income", "High Yield Bond"): "HYG",
    ("fixed_income", "Inflation-Linked Bond"): "TIP",
    ("fixed_income", "Intermediate-Term Bond"): "BND",
    ("fixed_income", "Investment Grade Bond"): "LQD",
    ("fixed_income", "Mortgage-Backed Securities"): "MBB",
    ("fixed_income", "Municipal Bond"): "MUB",
    ("fixed_income", "Preferred Securities"): "PFF",
    ("fixed_income", "Private Credit"): "BIZD",
    ("fixed_income", "Structured Credit"): "PAAA",
    ("cash", "Cash Equivalent"): "BIL",
    ("multi_asset", "Balanced"): "AOR",
    ("multi_asset", "Multi-Asset"): "AOR",
    ("multi_asset", "Target Date"): "AOR",
}

ASSET_CLASS_BENCHMARK_TICKERS = {
    "alternatives": "QAI",
    "cash": "BIL",
    "equity": "IVV",
    "fixed_income": "BND",
    "multi_asset": "AOR",
}


def compute_active_share_from_weights(
    fund_weights: dict[str, float],
    benchmark_weights: dict[str, float],
) -> dict[str, float | int | None]:
    """Pure active-share math over already-aggregated NAV weights.

    Weights are fractions of NAV (``pct_of_nav / 100``). They are not
    renormalized; shorts and leverage are preserved because they are part of the
    strategy difference.
    """

    keys = set(fund_weights) | set(benchmark_weights)
    common = set(fund_weights) & set(benchmark_weights)
    fund_coverage = sum(fund_weights.values())
    benchmark_coverage = sum(benchmark_weights.values())
    denom = min(fund_coverage, benchmark_coverage)
    active_raw = 0.5 * sum(
        abs(fund_weights.get(k, 0.0) - benchmark_weights.get(k, 0.0))
        for k in keys
    )
    overlap_raw = sum(min(fund_weights[k], benchmark_weights[k]) for k in common)
    n_common = len(common)
    n_fund = len(fund_weights)
    n_bench = len(benchmark_weights)
    n_union = len(keys)
    return {
        "active_share_normalized": active_raw / denom if denom > 0 else None,
        "overlap_normalized": overlap_raw / denom if denom > 0 else None,
        "overlap_nav_raw": overlap_raw,
        "fund_cusip_coverage_nav": fund_coverage,
        "benchmark_cusip_coverage_nav": benchmark_coverage,
        "n_fund_holdings": n_fund,
        "n_benchmark_holdings": n_bench,
        "n_common_holdings": n_common,
        "n_fund_only": n_fund - n_common,
        "n_benchmark_only": n_bench - n_common,
        "holdings_jaccard": n_common / n_union if n_union else None,
    }


def _values_clause(rows: list[tuple[Any, ...]]) -> tuple[str, list[Any]]:
    placeholders = ", ".join(
        "(" + ", ".join(["%s"] * len(rows[0])) + ")" for _ in rows
    )
    params = [value for row in rows for value in row]
    return placeholders, params


def _resolve_calc_date(conn, calc_date: str | None) -> _dt.date:
    if calc_date:
        return _dt.date.fromisoformat(calc_date)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(calc_date) FROM fund_risk_metrics "
            "WHERE organization_id IS NULL"
        )
        cdate = cur.fetchone()[0]
    if cdate is None:
        raise RuntimeError("fund_risk_metrics has no global rows")
    return cdate


def _build_temp_inputs(conn, calc_date: _dt.date, limit: int | None) -> int:
    strategy_rows = [
        (asset_class, strategy_label, ticker)
        for (asset_class, strategy_label), ticker
        in sorted(STRATEGY_BENCHMARK_TICKERS.items())
    ]
    asset_rows = sorted(ASSET_CLASS_BENCHMARK_TICKERS.items())
    strategy_values, strategy_params = _values_clause(strategy_rows)
    asset_values, asset_params = _values_clause(asset_rows)
    limit_clause = " LIMIT %(limit)s" if limit else ""
    params: dict[str, Any] = {"calc_date": calc_date}
    if limit:
        params["limit"] = limit
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE tmp_active_share_strategy_map
            (asset_class text, strategy_label text, benchmark_ticker text)
            ON COMMIT DROP
            """,
        )
        cur.execute(
            f"INSERT INTO tmp_active_share_strategy_map VALUES {strategy_values}",
            strategy_params,
        )
        cur.execute(
            """
            CREATE TEMP TABLE tmp_active_share_asset_map
            (asset_class text, benchmark_ticker text)
            ON COMMIT DROP
            """,
        )
        cur.execute(
            f"INSERT INTO tmp_active_share_asset_map VALUES {asset_values}",
            asset_params,
        )
        cur.execute(
            f"""
            CREATE TEMP TABLE tmp_active_share_targets ON COMMIT DROP AS
            WITH target_funds AS (
                SELECT m.instrument_id,
                       COALESCE(NULLIF(fv.series_id, ''),
                                NULLIF(ii.sec_series_id, ''),
                                NULLIF(iu.attributes->>'sec_series_id', ''),
                                NULLIF(iu.attributes->>'series_id', '')) AS fund_series_id,
                       fv.asset_class,
                       fv.strategy_label,
                       COALESCE(sm.benchmark_ticker, am.benchmark_ticker) AS benchmark_ticker
                FROM fund_risk_metrics m
                JOIN funds_v fv ON fv.instrument_id = m.instrument_id
                LEFT JOIN instruments_universe iu ON iu.instrument_id = m.instrument_id
                LEFT JOIN instrument_identity ii ON ii.instrument_id = m.instrument_id
                LEFT JOIN tmp_active_share_strategy_map sm
                  ON sm.asset_class = fv.asset_class
                 AND sm.strategy_label = fv.strategy_label
                LEFT JOIN tmp_active_share_asset_map am
                  ON am.asset_class = fv.asset_class
                WHERE m.calc_date = %(calc_date)s
                  AND m.organization_id IS NULL
                  AND COALESCE(NULLIF(fv.series_id, ''),
                               NULLIF(ii.sec_series_id, ''),
                               NULLIF(iu.attributes->>'sec_series_id', ''),
                               NULLIF(iu.attributes->>'series_id', '')) IS NOT NULL
                  AND COALESCE(sm.benchmark_ticker, am.benchmark_ticker) IS NOT NULL
                ORDER BY fv.asset_class, fv.strategy_label, m.instrument_id
                {limit_clause}
            ),
            benchmark_universe AS (
                SELECT DISTINCT ON (upper(iu.ticker))
                       upper(iu.ticker) AS benchmark_ticker,
                       iu.instrument_id AS benchmark_instrument_id,
                       COALESCE(NULLIF(fv.series_id, ''),
                                NULLIF(ii.sec_series_id, ''),
                                NULLIF(iu.attributes->>'sec_series_id', ''),
                                NULLIF(iu.attributes->>'series_id', '')) AS benchmark_series_id
                FROM instruments_universe iu
                LEFT JOIN instrument_identity ii ON ii.instrument_id = iu.instrument_id
                LEFT JOIN funds_v fv ON fv.instrument_id = iu.instrument_id
                WHERE upper(iu.ticker) IN (
                    SELECT DISTINCT upper(benchmark_ticker)
                    FROM tmp_active_share_strategy_map
                    UNION
                    SELECT DISTINCT upper(benchmark_ticker)
                    FROM tmp_active_share_asset_map
                )
                ORDER BY upper(iu.ticker),
                         iu.is_active DESC NULLS LAST,
                         (COALESCE(NULLIF(fv.series_id, ''),
                                   NULLIF(ii.sec_series_id, ''),
                                   NULLIF(iu.attributes->>'sec_series_id', ''),
                                   NULLIF(iu.attributes->>'series_id', '')) IS NOT NULL) DESC
            )
            SELECT tf.instrument_id,
                   tf.fund_series_id,
                   bu.benchmark_instrument_id,
                   bu.benchmark_series_id,
                   fund_rd.report_date AS fund_report_date,
                   bench_rd.report_date AS benchmark_report_date
            FROM target_funds tf
            JOIN benchmark_universe bu
              ON bu.benchmark_ticker = upper(tf.benchmark_ticker)
             AND bu.benchmark_series_id IS NOT NULL
            JOIN LATERAL (
                SELECT max(report_date) AS report_date
                FROM sec_nport_holdings
                WHERE series_id = tf.fund_series_id
                  AND report_date <= %(calc_date)s
            ) fund_rd ON fund_rd.report_date IS NOT NULL
            JOIN LATERAL (
                SELECT max(report_date) AS report_date
                FROM sec_nport_holdings
                WHERE series_id = bu.benchmark_series_id
                  AND report_date <= %(calc_date)s
            ) bench_rd ON bench_rd.report_date IS NOT NULL
            """,
            params,
        )
        cur.execute("CREATE INDEX ON tmp_active_share_targets (fund_series_id, fund_report_date)")
        cur.execute("CREATE INDEX ON tmp_active_share_targets (benchmark_series_id, benchmark_report_date)")
        cur.execute("ANALYZE tmp_active_share_targets")
        cur.execute("SELECT count(*) FROM tmp_active_share_targets")
        return int(cur.fetchone()[0])


def _compute_and_upsert(conn, calc_date: _dt.date) -> int:
    active_cols = ", ".join(ACTIVE_SHARE_COLUMNS)
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in ACTIVE_SHARE_COLUMNS)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE tmp_active_share_fund_holdings ON COMMIT DROP AS
            SELECT t.instrument_id,
                   upper(COALESCE(NULLIF(h.cusip, ''), 'IS:' || NULLIF(h.isin, ''))) AS holding_key,
                   sum(h.pct_of_nav::numeric / 100.0) AS weight
            FROM tmp_active_share_targets t
            JOIN sec_nport_holdings h
              ON h.series_id = t.fund_series_id
             AND h.report_date = t.fund_report_date
            WHERE h.pct_of_nav IS NOT NULL
              AND COALESCE(NULLIF(h.cusip, ''), NULLIF(h.isin, '')) IS NOT NULL
              AND COALESCE(h.cusip, '') !~ '^(LE:|H:|CIK:)'
            GROUP BY t.instrument_id, holding_key
            """,
        )
        cur.execute(
            """
            CREATE TEMP TABLE tmp_active_share_benchmark_holdings ON COMMIT DROP AS
            SELECT t.benchmark_series_id,
                   t.benchmark_report_date,
                   upper(COALESCE(NULLIF(h.cusip, ''), 'IS:' || NULLIF(h.isin, ''))) AS holding_key,
                   sum(h.pct_of_nav::numeric / 100.0) AS weight
            FROM (
                SELECT DISTINCT benchmark_series_id, benchmark_report_date
                FROM tmp_active_share_targets
            ) t
            JOIN sec_nport_holdings h
              ON h.series_id = t.benchmark_series_id
             AND h.report_date = t.benchmark_report_date
            WHERE h.pct_of_nav IS NOT NULL
              AND COALESCE(NULLIF(h.cusip, ''), NULLIF(h.isin, '')) IS NOT NULL
              AND COALESCE(h.cusip, '') !~ '^(LE:|H:|CIK:)'
            GROUP BY t.benchmark_series_id, t.benchmark_report_date, holding_key
            """,
        )
        cur.execute("CREATE INDEX ON tmp_active_share_fund_holdings (instrument_id, holding_key)")
        cur.execute("CREATE INDEX ON tmp_active_share_benchmark_holdings (benchmark_series_id, benchmark_report_date, holding_key)")
        cur.execute("ANALYZE tmp_active_share_fund_holdings")
        cur.execute("ANALYZE tmp_active_share_benchmark_holdings")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_active_share_metrics ON COMMIT DROP AS
            WITH union_keys AS (
                SELECT instrument_id, holding_key
                FROM tmp_active_share_fund_holdings
                UNION
                SELECT t.instrument_id, b.holding_key
                FROM tmp_active_share_targets t
                JOIN tmp_active_share_benchmark_holdings b
                  ON b.benchmark_series_id = t.benchmark_series_id
                 AND b.benchmark_report_date = t.benchmark_report_date
            ),
            pair_stats AS (
                SELECT t.instrument_id,
                       0.5 * sum(abs(COALESCE(f.weight, 0) - COALESCE(b.weight, 0))) AS active_raw,
                       sum(LEAST(f.weight, b.weight)) FILTER (
                           WHERE f.holding_key IS NOT NULL AND b.holding_key IS NOT NULL
                       ) AS overlap_raw,
                       count(*) FILTER (
                           WHERE f.holding_key IS NOT NULL AND b.holding_key IS NOT NULL
                       ) AS n_common,
                       count(*) FILTER (
                           WHERE f.holding_key IS NOT NULL AND b.holding_key IS NULL
                       ) AS n_fund_only,
                       count(*) FILTER (
                           WHERE f.holding_key IS NULL AND b.holding_key IS NOT NULL
                       ) AS n_benchmark_only
                FROM tmp_active_share_targets t
                JOIN union_keys k ON k.instrument_id = t.instrument_id
                LEFT JOIN tmp_active_share_fund_holdings f
                  ON f.instrument_id = t.instrument_id
                 AND f.holding_key = k.holding_key
                LEFT JOIN tmp_active_share_benchmark_holdings b
                  ON b.benchmark_series_id = t.benchmark_series_id
                 AND b.benchmark_report_date = t.benchmark_report_date
                 AND b.holding_key = k.holding_key
                GROUP BY t.instrument_id
            ),
            fund_stats AS (
                SELECT instrument_id, count(*) AS n_fund_holdings,
                       sum(weight) AS fund_cusip_coverage_nav
                FROM tmp_active_share_fund_holdings
                GROUP BY instrument_id
            ),
            bench_stats AS (
                SELECT benchmark_series_id, benchmark_report_date,
                       count(*) AS n_benchmark_holdings,
                       sum(weight) AS benchmark_cusip_coverage_nav
                FROM tmp_active_share_benchmark_holdings
                GROUP BY benchmark_series_id, benchmark_report_date
            )
            SELECT t.instrument_id,
                   CASE WHEN LEAST(fs.fund_cusip_coverage_nav,
                                   bs.benchmark_cusip_coverage_nav) > 0
                        THEN ps.active_raw / LEAST(fs.fund_cusip_coverage_nav,
                                                   bs.benchmark_cusip_coverage_nav)
                   END AS active_share_normalized,
                   CASE WHEN LEAST(fs.fund_cusip_coverage_nav,
                                   bs.benchmark_cusip_coverage_nav) > 0
                        THEN COALESCE(ps.overlap_raw, 0) / LEAST(fs.fund_cusip_coverage_nav,
                                                                 bs.benchmark_cusip_coverage_nav)
                   END AS overlap_normalized,
                   COALESCE(ps.overlap_raw, 0) AS overlap_nav_raw,
                   fs.fund_cusip_coverage_nav,
                   bs.benchmark_cusip_coverage_nav,
                   fs.n_fund_holdings::integer,
                   bs.n_benchmark_holdings::integer,
                   ps.n_common::integer AS n_common_holdings,
                   ps.n_fund_only::integer AS n_fund_only,
                   ps.n_benchmark_only::integer AS n_benchmark_only,
                   ps.n_common::numeric / NULLIF(
                       ps.n_common + ps.n_fund_only + ps.n_benchmark_only, 0
                   ) AS holdings_jaccard,
                   (%(calc_date)s::date - t.fund_report_date)::integer AS fund_report_age_days,
                   (%(calc_date)s::date - t.benchmark_report_date)::integer AS benchmark_report_age_days,
                   abs(t.fund_report_date - t.benchmark_report_date)::integer AS report_date_gap_days,
                   t.benchmark_instrument_id AS active_share_benchmark_instrument_id,
                   t.benchmark_series_id AS active_share_benchmark_series_id,
                   t.fund_report_date AS active_share_fund_report_date,
                   t.benchmark_report_date AS active_share_benchmark_report_date
            FROM tmp_active_share_targets t
            JOIN pair_stats ps ON ps.instrument_id = t.instrument_id
            JOIN fund_stats fs ON fs.instrument_id = t.instrument_id
            JOIN bench_stats bs
              ON bs.benchmark_series_id = t.benchmark_series_id
             AND bs.benchmark_report_date = t.benchmark_report_date
            """,
            {"calc_date": calc_date},
        )
        cur.execute(
            f"""
            INSERT INTO fund_risk_metrics
                (instrument_id, calc_date, organization_id, {active_cols})
            SELECT instrument_id, %(calc_date)s, NULL, {active_cols}
            FROM tmp_active_share_metrics
            ON CONFLICT (instrument_id, calc_date, organization_id)
            DO UPDATE SET {update_clause}
            """,
            {"calc_date": calc_date},
        )
        return cur.rowcount


def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
) -> dict:
    """Update active-share columns for one risk calc date."""

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_ACTIVE_SHARE_METRICS) as got:
            if not got:
                return {"processed": 0, "upserted": 0, "skipped": "lock_busy"}
            cdate = _resolve_calc_date(conn, calc_date)
            targets = _build_temp_inputs(conn, cdate, limit)
            upserted = _compute_and_upsert(conn, cdate) if targets else 0
            conn.commit()
    return {"processed": targets, "upserted": upserted, "calc_date": cdate}
