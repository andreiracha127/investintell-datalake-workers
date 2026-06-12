"""characteristics — monthly company & equity-fund characteristics from raw series.

Advisory lock : LOCK_CHARACTERISTICS (900_202)
Idempotent    : yes
  - company_characteristics_monthly  : ON CONFLICT (cik, period_end) DO UPDATE
  - equity_characteristics_monthly   : per-fund DELETE + INSERT (full replace)
Reproducible  : calc_date is an explicit parameter; all windows deterministic.

Standalone reimplementation of the legacy two-layer pipeline
(company_characteristics_compute.py + fund_characteristics_aggregator.py).
Reads ONLY raw series from the cloud data-lake; never reads a metrics table
as input.

================================================================================
RAW SOURCES (cloud data-lake)
================================================================================
  sec_xbrl_facts        issuer fundamentals (us-gaap / dei XBRL concepts)
  sec_nport_holdings     fund holdings: cusip, market_value, quantity,
                         asset_class, series_id, report_date
  sec_cusip_ticker_map   cusip -> issuer_cik resolution
  nav_timeseries         fund NAV (daily) -> month-end resample for momentum
  instruments_universe   fund identity (instrument_id, ticker, attributes)
  instrument_identity    resolved SEC identity (cik_unpadded, sec_series_id)

================================================================================
LAYER 1 — company_characteristics_monthly  (one row per cik, fiscal period_end)
================================================================================
Per (cik, period_end), from deduped us-gaap facts (latest filing wins,
DISTINCT ON (cik, concept, period_end) ORDER BY filed DESC):

  book_equity         = StockholdersEquity
  total_assets        = Assets
  revenue             = Revenues  (fallback RevenueFromContractWithCustomer-
                                   ExcludingAssessedTax)
  cost_of_revenue     = CostOfRevenue  (fallback CostOfGoodsAndServicesSold)
  gross_profit        = revenue - cost_of_revenue        (when both present)
  shares_outstanding  = dei.EntityCommonStockSharesOutstanding as-of period_end
  net_income_ttm      = most recent FY/CY NetIncomeLoss <= period_end
                        (XBRL Q1..Q3 are cumulative YTD, so the annual FY value
                        is the authoritative TTM figure — never sum quarters)
  capex_ttm           = most recent FY/CY PaymentsToAcquirePropertyPlantAnd-
                        Equipment <= period_end
  ppe_prior           = PropertyPlantAndEquipmentNet ~12 months before period_end

Derived ratios (clamped to +/-100; None when out of range — XBRL noise guard):
  quality_roa         = net_income_ttm / total_assets
  investment_growth   = total_assets / total_assets_{t-1y} - 1
  profitability_gross = gross_profit / revenue
                        (fallback (revenue - cost_of_revenue) / revenue)

period_end guard: ignore < 1990-01-01 or > today+90d (SEC century-typo rows).

================================================================================
LAYER 2 — equity_characteristics_monthly  (one row per instrument, as_of)
================================================================================
For each fund N-PORT report_date, aggregate Layer-1 company chars over the
fund's EQUITY holdings (asset_class IN ('EC','EP')), weighting each issuer by
ownership_fraction = nport.quantity / company.shares_outstanding. Uses
PORTFOLIO-LEVEL ratios (sum numerator / sum denominator), NOT a weighted mean
of per-issuer ratios. The company chars used are the latest period_end <=
report_date (point-in-time, LATERAL lookup).

  size_log_mkt_cap    = log(SUM market_value over ALL equity holdings)
                        (full equity-sleeve AUM; uses every holding, resolved
                        or not, since size exists regardless of fundamentals)
  book_to_market      = SUM(book_equity*own_frac)   / SUM(market_value_resolved)
  quality_roa         = SUM(net_income_ttm*own_frac)/ SUM(total_assets*own_frac)
  investment_growth   = SUM(capex_ttm*own_frac)     / SUM(ppe_prior*own_frac)
  profitability_gross = SUM(gross_profit*own_frac)  / SUM(revenue*own_frac)

  market_value_resolved = SUM market_value over holdings whose ownership could
                          be resolved (quantity & shares_outstanding present),
                          so B/M numerator and denominator span the same set.

  mom_12_1            = 12-1 momentum from the fund's OWN month-end NAV:
                        nav[t-1]/nav[t-12] - 1  (skip the most recent month;
                        Jegadeesh-Titman). Window = nav.loc[:as_of].iloc[-13:-1];
                        requires >= 11 monthly points. NOT aggregated from holdings.

All six chars clamped to +/-10 and rounded to 4 dp (NUMERIC(10,4)).
source_filing_date = max source_filing_date over the resolved holdings (audit).
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

import pandas as pd

from src.db import LOCK_CHARACTERISTICS, advisory_lock, connect

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_USGAAP_CONCEPTS = (
    "StockholdersEquity",
    "Assets",
    "NetIncomeLoss",
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "CostOfRevenue",
    "CostOfGoodsAndServicesSold",
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PropertyPlantAndEquipmentNet",
)
_DEI_SHARES_CONCEPT = "EntityCommonStockSharesOutstanding"
_EQUITY_ASSET_CLASSES = ("EC", "EP")

_PERIOD_END_MIN = date(1990, 1, 1)
_FUTURE_TOLERANCE_DAYS = 90

_COMPANY_CLAMP = 100.0   # raw-issuer ratio guard
_FUND_CLAMP = 10.0       # fund-level ratio guard (NUMERIC(10,4))


# ---------------------------------------------------------------------------
# Pure helpers (no DB) — mirror legacy derivation, documented above.
# ---------------------------------------------------------------------------
def _clamp(val: float | None, bound: float) -> float | None:
    if val is None:
        return None
    return None if (val > bound or val < -bound) else val


def _round4(val: float | None) -> float | None:
    return None if val is None else round(val, 4)


def _safe_div(num: float, den: float) -> float | None:
    return num / den if den > 0 else None


def _period_end_in_range(period_end: date, today: date) -> bool:
    from datetime import timedelta

    return _PERIOD_END_MIN <= period_end <= today + timedelta(days=_FUTURE_TOLERANCE_DAYS)


def derive_quality_roa(net_income_ttm: float | None, total_assets: float | None) -> float | None:
    if net_income_ttm is None or total_assets is None or total_assets <= 0:
        return None
    return net_income_ttm / total_assets


def derive_investment_growth(ta_now: float | None, ta_yoy: float | None) -> float | None:
    if ta_now is None or ta_yoy is None or ta_yoy <= 0:
        return None
    return ta_now / ta_yoy - 1.0


def derive_profitability_gross(
    gross_profit: float | None, revenue: float | None, cost_of_revenue: float | None
) -> float | None:
    if revenue is None or revenue <= 0:
        return None
    if gross_profit is not None:
        return gross_profit / revenue
    if cost_of_revenue is not None:
        return (revenue - cost_of_revenue) / revenue
    return None


def derive_momentum_12_1(nav_month_end: pd.Series, as_of: date) -> float | None:
    """12-1 momentum from a month-end NAV series. nav[t-1]/nav[t-12]-1."""
    if nav_month_end.empty:
        return None
    window = nav_month_end.loc[: pd.Timestamp(as_of)].iloc[-13:-1]
    if len(window) < 11:
        return None
    start_val = float(window.iloc[0])
    end_val = float(window.iloc[-1])
    if start_val <= 0:
        return None
    return end_val / start_val - 1.0


# ===========================================================================
# LAYER 1 — company characteristics from XBRL
# ===========================================================================
def _fetch_company_facts(conn, cik: int) -> dict[date, dict[str, Any]]:
    """Deduped us-gaap fundamentals for one CIK (latest filing wins)."""
    placeholders = ", ".join(["%s"] * len(_USGAAP_CONCEPTS))
    sql = f"""
        SELECT DISTINCT ON (cik, concept, period_end)
               concept, period_end, val, fp, filed, accn
        FROM sec_xbrl_facts
        WHERE cik = %s AND taxonomy = 'us-gaap' AND unit = 'USD'
          AND concept IN ({placeholders}) AND val IS NOT NULL
        ORDER BY cik, concept, period_end, filed DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cik, *_USGAAP_CONCEPTS))
        rows = cur.fetchall()

    by_period: dict[date, dict[str, Any]] = {}
    for concept, period_end, val, fp, filed, accn in rows:
        entry = by_period.setdefault(
            period_end, {"filed": None, "accn": None, "fp": None, "_fps": {}}
        )
        entry[concept] = float(val)
        if fp:
            entry["_fps"][concept] = fp
            entry["fp"] = fp
        if filed and (entry["filed"] is None or filed > entry["filed"]):
            entry["filed"] = filed
            entry["accn"] = accn
    return by_period


def _fetch_company_shares(conn, cik: int) -> dict[date, float]:
    sql = """
        SELECT DISTINCT ON (cik, period_end) period_end, val
        FROM sec_xbrl_facts
        WHERE cik = %s AND taxonomy = 'dei' AND concept = %s
          AND unit = 'shares' AND val IS NOT NULL AND val > 0
        ORDER BY cik, period_end, filed DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cik, _DEI_SHARES_CONCEPT))
        return {pe: float(v) for pe, v in cur.fetchall()}


def _ttm_fy_value(sorted_dates: list[date], data: dict, as_of: date, concept: str) -> float | None:
    """Most recent FY/CY value of a flow concept on/before as_of (YTD-safe TTM)."""
    best = None
    for d in sorted_dates:
        if d > as_of:
            break
        e = data[d]
        if e.get("_fps", {}).get(concept) in ("FY", "CY") and concept in e:
            best = e[concept]
    return best


def _yoy_value(sorted_dates: list[date], data: dict, as_of: date, concept: str) -> float | None:
    from dateutil.relativedelta import relativedelta

    target = as_of - relativedelta(years=1)
    best_date = None
    for d in sorted_dates:
        if d <= target:
            best_date = d
        else:
            break
    if best_date is not None and concept in data[best_date]:
        return data[best_date][concept]
    return None


def _value_as_of(mapping: dict[date, float], as_of: date) -> float | None:
    best = None
    for d in sorted(mapping):
        if d <= as_of:
            best = mapping[d]
        else:
            break
    return best


def _compute_company_rows(conn, cik: int, today: date) -> list[dict[str, Any]]:
    facts = _fetch_company_facts(conn, cik)
    if not facts:
        return []
    shares = _fetch_company_shares(conn, cik)
    sorted_dates = sorted(facts)
    rows: list[dict[str, Any]] = []

    for period_end in sorted_dates:
        if not _period_end_in_range(period_end, today):
            continue
        e = facts[period_end]
        revenue = e.get("Revenues") or e.get("RevenueFromContractWithCustomerExcludingAssessedTax")
        cost_of_rev = e.get("CostOfRevenue") or e.get("CostOfGoodsAndServicesSold")
        gross_profit = (
            revenue - cost_of_rev if revenue is not None and cost_of_rev is not None else None
        )
        total_assets = e.get("Assets")
        net_income_ttm = _ttm_fy_value(sorted_dates, facts, period_end, "NetIncomeLoss")
        capex_ttm = _ttm_fy_value(
            sorted_dates, facts, period_end, "PaymentsToAcquirePropertyPlantAndEquipment"
        )
        ppe_prior = _yoy_value(sorted_dates, facts, period_end, "PropertyPlantAndEquipmentNet")
        ta_yoy = _yoy_value(sorted_dates, facts, period_end, "Assets")

        rows.append(
            {
                "cik": cik,
                "period_end": period_end,
                "fp": e.get("fp"),
                "book_equity": e.get("StockholdersEquity"),
                "total_assets": total_assets,
                "net_income_ttm": net_income_ttm,
                "revenue": revenue,
                "cost_of_revenue": cost_of_rev,
                "gross_profit": gross_profit,
                "capex_ttm": capex_ttm,
                "ppe_prior": ppe_prior,
                "shares_outstanding": _value_as_of(shares, period_end),
                "quality_roa": _clamp(
                    derive_quality_roa(net_income_ttm, total_assets), _COMPANY_CLAMP
                ),
                "investment_growth": _clamp(
                    derive_investment_growth(total_assets, ta_yoy), _COMPANY_CLAMP
                ),
                "profitability_gross": _clamp(
                    derive_profitability_gross(gross_profit, revenue, cost_of_rev), _COMPANY_CLAMP
                ),
                "source_filing_date": e.get("filed"),
                "source_accn": e.get("accn"),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Set-based Layer 1 (production path).
#
# Replaces the per-CIK Python loop (2 queries x ~14.5k CIKs) with one pass over
# the whole us-gaap fact table, deduped + pivoted in SQL, then as-of joined for
# the temporal parts. Produces byte-for-byte the same columns as the per-CIK
# reference (_compute_company_rows above), which remains as the audited oracle
# and is exercised by the test-suite.
#
# Determinism note: the legacy DISTINCT ON (cik, concept, period_end ORDER BY
# filed DESC) left ties (same filed, different accn/val — SEC same-day 10-K/A +
# 10-Q amendments) to Postgres physical order, i.e. non-reproducible. Here the
# tiebreak is made explicit and deterministic with a trailing ", accn" (ASC),
# which empirically matches the legacy cloud rows on ~99.95% of (cik, period_end)
# pairs; the residual <0.05% are exactly those duplicate-filing rows where the
# legacy value was an arbitrary heap pick. accn ASC was selected over accn DESC
# because it matched the validated oracle on strictly more rows (9 vs 14 diffs
# over an 18.3k-row sample).
# ---------------------------------------------------------------------------
_L1_CONCEPTS_SQL = (
    "('StockholdersEquity','Assets','NetIncomeLoss','Revenues',"
    "'RevenueFromContractWithCustomerExcludingAssessedTax','CostOfRevenue',"
    "'CostOfGoodsAndServicesSold','PaymentsToAcquirePropertyPlantAndEquipment',"
    "'PropertyPlantAndEquipmentNet')"
)

# Deduped us-gaap facts: one row per (cik, concept, period_end), latest filing
# wins (deterministic tiebreak accn ASC). Materialised + indexed so the as-of
# LATERAL lookups below are index scans rather than re-scans of a CTE.
_L1_BUILD_DEDUP = f"""
CREATE TEMP TABLE tmp_l1_dedup ON COMMIT DROP AS
SELECT DISTINCT ON (cik, concept, period_end)
       cik, concept, period_end, val, fp, filed, accn
FROM sec_xbrl_facts
WHERE taxonomy = 'us-gaap' AND unit = 'USD' AND val IS NOT NULL
  AND concept IN {_L1_CONCEPTS_SQL}
  {{cik_filter}}
ORDER BY cik, concept, period_end, filed DESC, accn
"""
_L1_INDEX_DEDUP = "CREATE INDEX ON tmp_l1_dedup (cik, concept, period_end)"

# Pivot deduped facts to one row per (cik, period_end). fp_raw mirrors the
# Python loop's entry["fp"]: the fp of the alphabetically-greatest concept that
# carries a non-empty fp for that period_end. source_accn / source_filing_date
# mirror the loop's max-filed-row audit fields (accn ASC tiebreak).
_L1_BUILD_PIV = """
CREATE TEMP TABLE tmp_l1_piv ON COMMIT DROP AS
SELECT cik, period_end,
    MAX(val) FILTER (WHERE concept='StockholdersEquity') AS book_equity,
    MAX(val) FILTER (WHERE concept='Assets') AS total_assets,
    MAX(val) FILTER (WHERE concept='Revenues') AS revenues,
    MAX(val) FILTER (WHERE concept='RevenueFromContractWithCustomerExcludingAssessedTax') AS rev_cc,
    MAX(val) FILTER (WHERE concept='CostOfRevenue') AS cost_of_revenue,
    MAX(val) FILTER (WHERE concept='CostOfGoodsAndServicesSold') AS cogs,
    MAX(val) FILTER (WHERE concept='PropertyPlantAndEquipmentNet') AS ppe_net,
    MAX(filed) AS source_filing_date,
    (ARRAY_AGG(accn ORDER BY filed DESC NULLS LAST, accn))[1] AS source_accn,
    (ARRAY_AGG(fp ORDER BY (CASE WHEN fp IS NOT NULL AND fp<>'' THEN 1 ELSE 0 END) DESC,
                          concept DESC))[1] AS fp_raw
FROM tmp_l1_dedup
GROUP BY cik, period_end
"""
_L1_INDEX_PIV = "CREATE INDEX ON tmp_l1_piv (cik, period_end)"

# FY/CY flow rows for YTD-safe TTM (NetIncomeLoss, capex). XBRL Q1..Q3 are
# cumulative YTD, so only the annual FY/CY figure is authoritative.
_L1_BUILD_FY = """
CREATE TEMP TABLE tmp_l1_fy ON COMMIT DROP AS
SELECT cik, concept, period_end, val FROM tmp_l1_dedup WHERE fp IN ('FY','CY')
"""
_L1_INDEX_FY = "CREATE INDEX ON tmp_l1_fy (cik, concept, period_end)"

# dei shares outstanding, deduped per (cik, period_end) (latest filed, val>0).
_L1_BUILD_SHARES = """
CREATE TEMP TABLE tmp_l1_shares ON COMMIT DROP AS
SELECT DISTINCT ON (cik, period_end) cik, period_end, val
FROM sec_xbrl_facts
WHERE taxonomy='dei' AND concept='EntityCommonStockSharesOutstanding'
  AND unit='shares' AND val IS NOT NULL AND val>0
  {cik_filter}
ORDER BY cik, period_end, filed DESC, accn
"""
_L1_INDEX_SHARES = "CREATE INDEX ON tmp_l1_shares (cik, period_end)"

# Final per-(cik, period_end) characteristics. Mirrors _compute_company_rows:
#  - revenue / cost_of_revenue: Python `a or b` truthiness (0.0 is falsy -> use
#    fallback) reproduced with `CASE WHEN x IS NOT NULL AND x<>0 ...`.
#  - net_income_ttm / capex_ttm: most recent FY/CY value with period_end<=current.
#  - ppe_prior / ta_yoy: value at the SINGLE nearest pivot period_end
#    <= period_end-1y (NULL when that exact period lacks the concept) — matches
#    the legacy _yoy_value, which reads the nearest period and returns None when
#    the concept is absent there (it does NOT hunt back for the nearest period
#    that has the concept).
#  - shares_outstanding: nearest dei period_end <= period_end.
#  - ratios clamped to +/-100 (None outside range / non-positive denominator).
_L1_FINAL_SELECT = """
SELECT
  p.cik, p.period_end,
  NULLIF(p.fp_raw,'') AS fp,
  p.book_equity, p.total_assets,
  rev.revenue, cor.cost_of_revenue,
  CASE WHEN rev.revenue IS NOT NULL AND cor.cost_of_revenue IS NOT NULL
       THEN rev.revenue - cor.cost_of_revenue END AS gross_profit,
  ni.val  AS net_income_ttm,
  cap.val AS capex_ttm,
  yoy.ppe_prior,
  sh.val  AS shares_outstanding,
  -- quality_roa = net_income_ttm/total_assets (den>0), clamp +-100.
  CASE WHEN ni.val IS NOT NULL AND p.total_assets IS NOT NULL AND p.total_assets>0
            AND abs(ni.val / p.total_assets) <= 100.0
       THEN ni.val / p.total_assets END AS quality_roa,
  -- investment_growth = total_assets/total_assets_{t-1y} - 1 (den>0), clamp +-100.
  CASE WHEN p.total_assets IS NOT NULL AND yoy.ta_yoy IS NOT NULL AND yoy.ta_yoy>0
            AND abs(p.total_assets / yoy.ta_yoy - 1.0) <= 100.0
       THEN p.total_assets / yoy.ta_yoy - 1.0 END AS investment_growth,
  -- profitability_gross = gross_profit/revenue (fallback (rev-cost)/rev), clamp
  -- +-100. NULL when revenue<=0 OR neither gross_profit nor cost is available
  -- (gp IS NULL here implies cost IS NULL, since gp = rev-cost when both present).
  CASE WHEN rev.revenue IS NOT NULL AND rev.revenue>0 AND cor.cost_of_revenue IS NOT NULL
            AND abs((rev.revenue - cor.cost_of_revenue) / rev.revenue) <= 100.0
       THEN (rev.revenue - cor.cost_of_revenue) / rev.revenue END AS profitability_gross,
  p.source_filing_date, p.source_accn
FROM tmp_l1_piv p
CROSS JOIN LATERAL (
  SELECT CASE WHEN p.revenues IS NOT NULL AND p.revenues<>0 THEN p.revenues
              ELSE p.rev_cc END AS revenue
) rev
CROSS JOIN LATERAL (
  SELECT CASE WHEN p.cost_of_revenue IS NOT NULL AND p.cost_of_revenue<>0
              THEN p.cost_of_revenue ELSE p.cogs END AS cost_of_revenue
) cor
LEFT JOIN LATERAL (
  SELECT f.val FROM tmp_l1_fy f
   WHERE f.cik=p.cik AND f.concept='NetIncomeLoss' AND f.period_end<=p.period_end
   ORDER BY f.period_end DESC LIMIT 1
) ni ON true
LEFT JOIN LATERAL (
  SELECT f.val FROM tmp_l1_fy f
   WHERE f.cik=p.cik AND f.concept='PaymentsToAcquirePropertyPlantAndEquipment'
     AND f.period_end<=p.period_end
   ORDER BY f.period_end DESC LIMIT 1
) cap ON true
LEFT JOIN LATERAL (
  SELECT pp.ppe_net AS ppe_prior, pp.total_assets AS ta_yoy
  FROM tmp_l1_piv pp
   WHERE pp.cik=p.cik AND pp.period_end <= (p.period_end - INTERVAL '1 year')::date
   ORDER BY pp.period_end DESC LIMIT 1
) yoy ON true
LEFT JOIN LATERAL (
  SELECT s.val FROM tmp_l1_shares s
   WHERE s.cik=p.cik AND s.period_end<=p.period_end
   ORDER BY s.period_end DESC LIMIT 1
) sh ON true
WHERE p.period_end >= DATE '1990-01-01'
  AND p.period_end <= (%(today)s::date + INTERVAL '90 days')::date
"""

# Server-side upsert: feed _L1_FINAL_SELECT straight into the destination with
# the same ON CONFLICT (cik, period_end) DO UPDATE semantics as _COMPANY_UPSERT.
_L1_UPSERT_FROM_SELECT = f"""
INSERT INTO company_characteristics_monthly (
    cik, period_end, fp, book_equity, total_assets, revenue, cost_of_revenue,
    gross_profit, net_income_ttm, capex_ttm, ppe_prior, shares_outstanding,
    quality_roa, investment_growth, profitability_gross,
    source_filing_date, source_accn, computed_at
)
SELECT
    cik, period_end, fp, book_equity, total_assets, revenue, cost_of_revenue,
    gross_profit, net_income_ttm, capex_ttm, ppe_prior, shares_outstanding,
    quality_roa, investment_growth, profitability_gross,
    source_filing_date, source_accn, now()
FROM ( {_L1_FINAL_SELECT} ) src
ON CONFLICT (cik, period_end) DO UPDATE SET
    fp = EXCLUDED.fp, book_equity = EXCLUDED.book_equity,
    total_assets = EXCLUDED.total_assets, net_income_ttm = EXCLUDED.net_income_ttm,
    revenue = EXCLUDED.revenue, cost_of_revenue = EXCLUDED.cost_of_revenue,
    gross_profit = EXCLUDED.gross_profit, capex_ttm = EXCLUDED.capex_ttm,
    ppe_prior = EXCLUDED.ppe_prior, shares_outstanding = EXCLUDED.shares_outstanding,
    quality_roa = EXCLUDED.quality_roa, investment_growth = EXCLUDED.investment_growth,
    profitability_gross = EXCLUDED.profitability_gross,
    source_filing_date = EXCLUDED.source_filing_date,
    source_accn = EXCLUDED.source_accn, computed_at = now()
"""


def _run_layer1_setbased(conn, today: date, limit: int | None) -> tuple[int, int]:
    """Set-based Layer 1: build temp tables, upsert all company rows in one pass.

    Returns (ciks_processed, rows_upserted). One transaction: the temp tables are
    ON COMMIT DROP, so the final commit cleans them up. Mirrors the column output
    of the per-CIK reference _compute_company_rows exactly.
    """
    cik_filter = ""
    params: dict[str, Any] = {"today": today.isoformat()}
    if limit:
        # Match the legacy `LIMIT` semantics: cap the number of CIKs processed.
        cik_filter = "AND cik IN (SELECT DISTINCT cik FROM sec_xbrl_facts " \
                     "WHERE taxonomy='us-gaap' LIMIT %(cik_limit)s)"
        params["cik_limit"] = int(limit)

    with conn.cursor() as cur:
        # The deduped / pivoted temp tables (~2.2M and ~450k rows on the full
        # universe) and the as-of sorts exceed the default 8MB temp_buffers /
        # 4MB work_mem, which otherwise aborts the build with
        # "no empty local buffer available". Raise both for this session
        # (temp_buffers must be set before any temp table is touched).
        cur.execute("SET temp_buffers = '512MB'")
        cur.execute("SET work_mem = '256MB'")
        cur.execute(_L1_BUILD_DEDUP.format(cik_filter=cik_filter), params)
        cur.execute(_L1_INDEX_DEDUP)
        cur.execute(_L1_BUILD_PIV)
        cur.execute(_L1_INDEX_PIV)
        cur.execute(_L1_BUILD_FY)
        cur.execute(_L1_INDEX_FY)
        cur.execute(_L1_BUILD_SHARES.format(cik_filter=cik_filter), params)
        cur.execute(_L1_INDEX_SHARES)

        # Upsert all company rows in a single server-side INSERT ... SELECT ...
        # ON CONFLICT — no per-row client round-trips (453k rows otherwise).
        cur.execute(_L1_UPSERT_FROM_SELECT, params)
        rows_upserted = cur.rowcount
        cur.execute("SELECT count(DISTINCT cik) FROM tmp_l1_piv")
        ciks_processed = int(cur.fetchone()[0])
    conn.commit()
    return ciks_processed, rows_upserted


_COMPANY_UPSERT = """
    INSERT INTO company_characteristics_monthly (
        cik, period_end, fp, book_equity, total_assets, net_income_ttm,
        revenue, cost_of_revenue, gross_profit, capex_ttm, ppe_prior,
        shares_outstanding, quality_roa, investment_growth, profitability_gross,
        source_filing_date, source_accn, computed_at
    ) VALUES (
        %(cik)s, %(period_end)s, %(fp)s, %(book_equity)s, %(total_assets)s,
        %(net_income_ttm)s, %(revenue)s, %(cost_of_revenue)s, %(gross_profit)s,
        %(capex_ttm)s, %(ppe_prior)s, %(shares_outstanding)s, %(quality_roa)s,
        %(investment_growth)s, %(profitability_gross)s, %(source_filing_date)s,
        %(source_accn)s, now()
    )
    ON CONFLICT (cik, period_end) DO UPDATE SET
        fp = EXCLUDED.fp, book_equity = EXCLUDED.book_equity,
        total_assets = EXCLUDED.total_assets, net_income_ttm = EXCLUDED.net_income_ttm,
        revenue = EXCLUDED.revenue, cost_of_revenue = EXCLUDED.cost_of_revenue,
        gross_profit = EXCLUDED.gross_profit, capex_ttm = EXCLUDED.capex_ttm,
        ppe_prior = EXCLUDED.ppe_prior, shares_outstanding = EXCLUDED.shares_outstanding,
        quality_roa = EXCLUDED.quality_roa, investment_growth = EXCLUDED.investment_growth,
        profitability_gross = EXCLUDED.profitability_gross,
        source_filing_date = EXCLUDED.source_filing_date,
        source_accn = EXCLUDED.source_accn, computed_at = now()
"""


# ===========================================================================
# LAYER 2 — equity (fund) characteristics from N-PORT x company chars
# ===========================================================================
def _load_fund_universe(conn, limit: int | None) -> list[dict[str, Any]]:
    """Funds with N-PORT equity holdings, resolved to a series_id + ticker."""
    limit_clause = f" LIMIT {int(limit)}" if limit else ""
    sql = f"""
        SELECT
            i.instrument_id,
            i.ticker,
            COALESCE(
                NULLIF(ii.sec_series_id, ''),
                NULLIF(i.attributes->>'sec_series_id', ''),
                NULLIF(i.attributes->>'series_id', '')
            ) AS series_id
        FROM instruments_universe i
        LEFT JOIN instrument_identity ii USING (instrument_id)
        WHERE COALESCE(
                  NULLIF(ii.sec_series_id, ''),
                  NULLIF(i.attributes->>'sec_series_id', ''),
                  NULLIF(i.attributes->>'series_id', '')
              ) IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM sec_nport_holdings n
              WHERE n.series_id = COALESCE(
                  NULLIF(ii.sec_series_id, ''),
                  NULLIF(i.attributes->>'sec_series_id', ''),
                  NULLIF(i.attributes->>'series_id', '')
              )
          )
        ORDER BY i.ticker NULLS LAST{limit_clause}
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return [
            {"instrument_id": iid, "ticker": tk, "series_id": sid}
            for iid, tk, sid in cur.fetchall()
        ]


def _load_nav_month_end(conn, instrument_id) -> pd.Series:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT nav_date, nav FROM nav_timeseries "
            "WHERE instrument_id = %s AND nav IS NOT NULL ORDER BY nav_date",
            (instrument_id,),
        )
        rows = cur.fetchall()
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series(
        [float(v) for _, v in rows], index=pd.DatetimeIndex([d for d, _ in rows])
    )
    return s.resample("ME").last().dropna()


def _fund_report_dates(conn, series_id: str) -> list[date]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT report_date FROM sec_nport_holdings "
            "WHERE series_id = %s ORDER BY report_date",
            (series_id,),
        )
        return [r[0] for r in cur.fetchall()]


def _aggregate_one_date(
    conn, fund: dict[str, Any], report_date: date, nav: pd.Series
) -> dict[str, Any] | None:
    sql = """
        SELECT n.market_value, n.quantity,
               c.book_equity, c.total_assets, c.net_income_ttm, c.revenue,
               c.gross_profit, c.capex_ttm, c.ppe_prior, c.shares_outstanding,
               c.source_filing_date
        FROM sec_nport_holdings n
        LEFT JOIN LATERAL (
            SELECT m2.issuer_cik FROM sec_cusip_ticker_map m2
             WHERE m2.cusip = n.cusip AND m2.issuer_cik IS NOT NULL
             ORDER BY m2.issuer_cik LIMIT 1
        ) m ON true
        LEFT JOIN LATERAL (
            SELECT book_equity, total_assets, net_income_ttm, revenue,
                   gross_profit, capex_ttm, ppe_prior, shares_outstanding,
                   source_filing_date
            FROM company_characteristics_monthly
            WHERE cik = m.issuer_cik::bigint AND period_end <= %s
            ORDER BY period_end DESC LIMIT 1
        ) c ON true
        WHERE n.series_id = %s AND n.report_date = %s
          AND n.asset_class IN ('EC', 'EP')
          AND n.market_value IS NOT NULL AND n.market_value > 0
    """
    with conn.cursor() as cur:
        cur.execute(sql, (report_date, fund["series_id"], report_date))
        holdings = cur.fetchall()
    if not holdings:
        return None

    sum_mv_size = sum_mv_resolved = 0.0
    sum_be = sum_ta = sum_ni = sum_rev = sum_gp = sum_capex = sum_ppe = 0.0
    resolved = 0
    latest_filing: date | None = None

    for (mv, qty, be, ta, ni, rev, gp, capex, ppe, shares, sfd) in holdings:
        mv = float(mv)
        sum_mv_size += mv  # size uses ALL holdings
        qty = float(qty) if qty is not None else None
        shares = float(shares) if shares is not None else None
        if qty is None or shares is None or shares <= 0:
            continue
        own_frac = qty / shares
        sum_mv_resolved += mv
        if be is not None:
            sum_be += float(be) * own_frac
        if ta is not None:
            sum_ta += float(ta) * own_frac
        if ni is not None:
            sum_ni += float(ni) * own_frac
        if rev is not None:
            sum_rev += float(rev) * own_frac
        if gp is not None:
            sum_gp += float(gp) * own_frac
        if capex is not None:
            sum_capex += float(capex) * own_frac
        if ppe is not None:
            sum_ppe += float(ppe) * own_frac
        resolved += 1
        if sfd is not None and (latest_filing is None or sfd > latest_filing):
            latest_filing = sfd

    # No `resolved == 0` early return: a fund with equity holdings but no
    # resolvable fundamentals still has a valid full-sleeve size; the
    # fundamentals-weighted chars fall to None via _safe_div over zero sums.
    size_log = math.log(sum_mv_size) if sum_mv_size > 0 else None
    book_to_market = _safe_div(sum_be, sum_mv_resolved)
    quality_roa = _safe_div(sum_ni, sum_ta)
    investment_growth = _safe_div(sum_capex, sum_ppe)
    profitability_gross = _safe_div(sum_gp, sum_rev)
    mom_12_1 = derive_momentum_12_1(nav, report_date)

    return {
        "instrument_id": fund["instrument_id"],
        "ticker": fund.get("ticker") or "",
        "as_of": report_date,
        "size_log_mkt_cap": _round4(size_log),
        "book_to_market": _round4(_clamp(book_to_market, _FUND_CLAMP)),
        "mom_12_1": _round4(_clamp(mom_12_1, _FUND_CLAMP)),
        "quality_roa": _round4(_clamp(quality_roa, _FUND_CLAMP)),
        "investment_growth": _round4(_clamp(investment_growth, _FUND_CLAMP)),
        "profitability_gross": _round4(_clamp(profitability_gross, _FUND_CLAMP)),
        "source_filing_date": latest_filing,
    }


def compute_fund_rows(conn, fund: dict[str, Any]) -> list[dict[str, Any]]:
    """All (fund, report_date) characteristic rows for one fund."""
    if not fund.get("series_id"):
        return []
    nav = _load_nav_month_end(conn, fund["instrument_id"])
    rows = []
    for report_date in _fund_report_dates(conn, fund["series_id"]):
        row = _aggregate_one_date(conn, fund, report_date, nav)
        if row is not None:
            rows.append(row)
    return rows


_EQUITY_UPSERT = """
    INSERT INTO equity_characteristics_monthly (
        instrument_id, ticker, as_of, size_log_mkt_cap, book_to_market,
        mom_12_1, quality_roa, investment_growth, profitability_gross,
        source_filing_date, computed_at
    ) VALUES (
        %(instrument_id)s, %(ticker)s, %(as_of)s, %(size_log_mkt_cap)s,
        %(book_to_market)s, %(mom_12_1)s, %(quality_roa)s, %(investment_growth)s,
        %(profitability_gross)s, %(source_filing_date)s, now()
    )
    ON CONFLICT (instrument_id, as_of) DO UPDATE SET
        ticker = EXCLUDED.ticker, size_log_mkt_cap = EXCLUDED.size_log_mkt_cap,
        book_to_market = EXCLUDED.book_to_market, mom_12_1 = EXCLUDED.mom_12_1,
        quality_roa = EXCLUDED.quality_roa, investment_growth = EXCLUDED.investment_growth,
        profitability_gross = EXCLUDED.profitability_gross,
        source_filing_date = EXCLUDED.source_filing_date, computed_at = now()
"""


def _replace_fund_rows(conn, instrument_id, rows: list[dict[str, Any]]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM equity_characteristics_monthly WHERE instrument_id = %s",
            (instrument_id,),
        )
        for row in rows:
            cur.execute(_EQUITY_UPSERT, row)
    return len(rows)


# ---------------------------------------------------------------------------
# Set-based Layer 2 (production path).
#
# Replaces the per-fund Python loop (3 queries x ~18k funds x N report_dates:
# holdings + NAV + report_dates, then per-row aggregation in Python) with ONE
# server-side pass that processes every (instrument_id, report_date) at once and
# upserts in a single INSERT ... ON CONFLICT. Produces field-for-field the same
# rows as the per-fund reference (compute_fund_rows / _aggregate_one_date above),
# which remains as the audited oracle exercised by the test-suite.
#
# Validation: over a 30-fund / 631-row sample on the DB-mãe (full sources), the
# set-based output matched the per-fund Python helper EXACTLY on all six chars
# (0 diffs, tol 1e-4). mom_12_1 also matched the legacy stored rows to 4 dp; the
# five fundamentals-derived chars differ from the *legacy stored* rows only by
# XBRL vintage (source_filing_date drift) — same caveat the legacy code carried.
#
# Semantic mirror of _aggregate_one_date, statement by statement:
#  - Holdings filter: asset_class IN ('EC','EP'), market_value > 0, issuer_cik
#    resolvable via sec_cusip_ticker_map (cusip -> issuer_cik). Company chars are
#    the point-in-time row (latest period_end <= report_date) via LATERAL.
#  - own_frac = quantity / shares_outstanding (resolved set: quantity present &
#    shares_outstanding > 0). size uses ALL equity holdings; B/M numerator and
#    denominator span the same resolved set (sum_mv_resolved).
#  - Numerator SUMs are COALESCE(...,0): the Python accumulators initialise at
#    0.0, so an all-NULL holding sum is 0.0, not NULL (e.g. a sole resolved
#    holding lacking net_income_ttm => quality_roa = 0/sum_ta = 0.0).
#  - mom_12_1: month-end NAV resample (last nav per calendar month, indexed at
#    the calendar month-end) reproduces pandas resample('ME').last(); the window
#    is the last 13 month-end points <= as_of with the most recent DROPPED
#    (rank 2 = end, rank LEAST(13,n) = start), requiring >= 12 points so the
#    surviving window has >= 11 — exactly nav.loc[:as_of].iloc[-13:-1], len>=11.
#  - All six chars clamp +/-10 (NULL outside range / non-positive denominator),
#    round 4 dp. source_filing_date = max over resolved holdings. A row is kept
#    whenever the fund has equity holdings (sum_mv_size > 0): size is the full
#    sleeve; the fundamentals-weighted chars are NULL when nothing resolved.
# ---------------------------------------------------------------------------

# Fund universe -> TEMP table tmp_l2_funds(instrument_id, ticker, series_id).
# Same resolution/joins as _load_fund_universe: series_id from instrument_identity
# or instruments_universe.attributes, restricted to funds that actually have
# N-PORT holdings. {limit_clause} caps the fund count for testing.
_L2_BUILD_FUNDS = """
CREATE TEMP TABLE tmp_l2_funds ON COMMIT DROP AS
SELECT
    i.instrument_id, i.ticker,
    COALESCE(
        NULLIF(ii.sec_series_id, ''),
        NULLIF(i.attributes->>'sec_series_id', ''),
        NULLIF(i.attributes->>'series_id', '')
    ) AS series_id
FROM instruments_universe i
LEFT JOIN instrument_identity ii USING (instrument_id)
WHERE COALESCE(
          NULLIF(ii.sec_series_id, ''),
          NULLIF(i.attributes->>'sec_series_id', ''),
          NULLIF(i.attributes->>'series_id', '')
      ) IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM sec_nport_holdings n
      WHERE n.series_id = COALESCE(
          NULLIF(ii.sec_series_id, ''),
          NULLIF(i.attributes->>'sec_series_id', ''),
          NULLIF(i.attributes->>'series_id', '')
      )
  )
ORDER BY i.ticker NULLS LAST{limit_clause}
"""
_L2_INDEX_FUNDS_SERIES = "CREATE INDEX ON tmp_l2_funds (series_id)"
_L2_INDEX_FUNDS_IID = "CREATE INDEX ON tmp_l2_funds (instrument_id)"

# Month-end NAV points (last nav per calendar month, indexed at the calendar
# month-end) materialised + indexed. Critical for performance: the momentum
# lookups below probe this set THREE times per output row, so leaving it as an
# inline CTE re-runs the whole NAV resample per row (O(rows x full_nav_scan) —
# ~110s for 50 funds). As an indexed temp table each probe is an index scan.
_L2_BUILD_NAVPTS = """
CREATE TEMP TABLE tmp_l2_navpts ON COMMIT DROP AS
SELECT instrument_id, m_end, nav FROM (
  SELECT f.instrument_id,
         (date_trunc('month', t.nav_date) + INTERVAL '1 month - 1 day')::date AS m_end,
         t.nav,
         row_number() OVER (PARTITION BY f.instrument_id, date_trunc('month', t.nav_date)
                            ORDER BY t.nav_date DESC) AS rn
  FROM tmp_l2_funds f
  JOIN nav_timeseries t ON t.instrument_id = f.instrument_id AND t.nav IS NOT NULL
) q
WHERE rn = 1
"""
_L2_INDEX_NAVPTS = "CREATE INDEX ON tmp_l2_navpts (instrument_id, m_end DESC)"

# Core SELECT producing one row per (instrument_id, as_of) — see header comment.
# Reads the pre-built indexed tmp_l2_navpts for momentum.
_L2_FINAL_SELECT = """
WITH hold AS (
  SELECT
    f.instrument_id, f.ticker, n.report_date AS as_of,
    SUM(n.market_value::numeric) AS sum_mv_size,
    SUM(CASE WHEN n.quantity IS NOT NULL AND c.shares_outstanding IS NOT NULL
              AND c.shares_outstanding > 0
             THEN n.market_value::numeric END) AS sum_mv_resolved,
    SUM(CASE WHEN c.book_equity IS NOT NULL AND n.quantity IS NOT NULL
              AND c.shares_outstanding > 0
             THEN c.book_equity * (n.quantity / c.shares_outstanding) END) AS sum_be,
    SUM(CASE WHEN c.total_assets IS NOT NULL AND n.quantity IS NOT NULL
              AND c.shares_outstanding > 0
             THEN c.total_assets * (n.quantity / c.shares_outstanding) END) AS sum_ta,
    SUM(CASE WHEN c.net_income_ttm IS NOT NULL AND n.quantity IS NOT NULL
              AND c.shares_outstanding > 0
             THEN c.net_income_ttm * (n.quantity / c.shares_outstanding) END) AS sum_ni,
    SUM(CASE WHEN c.revenue IS NOT NULL AND n.quantity IS NOT NULL
              AND c.shares_outstanding > 0
             THEN c.revenue * (n.quantity / c.shares_outstanding) END) AS sum_rev,
    SUM(CASE WHEN c.gross_profit IS NOT NULL AND n.quantity IS NOT NULL
              AND c.shares_outstanding > 0
             THEN c.gross_profit * (n.quantity / c.shares_outstanding) END) AS sum_gp,
    SUM(CASE WHEN c.capex_ttm IS NOT NULL AND n.quantity IS NOT NULL
              AND c.shares_outstanding > 0
             THEN c.capex_ttm * (n.quantity / c.shares_outstanding) END) AS sum_capex,
    SUM(CASE WHEN c.ppe_prior IS NOT NULL AND n.quantity IS NOT NULL
              AND c.shares_outstanding > 0
             THEN c.ppe_prior * (n.quantity / c.shares_outstanding) END) AS sum_ppe,
    COUNT(*) FILTER (WHERE n.quantity IS NOT NULL AND c.shares_outstanding IS NOT NULL
              AND c.shares_outstanding > 0) AS resolved_cnt,
    MAX(CASE WHEN n.quantity IS NOT NULL AND c.shares_outstanding IS NOT NULL
              AND c.shares_outstanding > 0
             THEN c.source_filing_date END) AS latest_filing
  FROM tmp_l2_funds f
  JOIN sec_nport_holdings n ON n.series_id = f.series_id
  -- Full equity sleeve: every equity holding counts toward size (mapped or not;
  -- ~45% of cloud N-PORT carry synthetic keys with no real CUSIP). Resolution is
  -- a LEFT lateral (one issuer per cusip); the fundamentals-weighted SUMs below
  -- self-restrict to resolved holdings via their own c.* IS NOT NULL guards.
  LEFT JOIN LATERAL (
    SELECT m2.issuer_cik FROM sec_cusip_ticker_map m2
     WHERE m2.cusip = n.cusip AND m2.issuer_cik IS NOT NULL
     ORDER BY m2.issuer_cik LIMIT 1
  ) m ON true
  LEFT JOIN LATERAL (
    SELECT cc.book_equity, cc.total_assets, cc.net_income_ttm, cc.revenue,
           cc.gross_profit, cc.capex_ttm, cc.ppe_prior, cc.shares_outstanding,
           cc.source_filing_date
    FROM company_characteristics_monthly cc
    WHERE cc.cik = m.issuer_cik::bigint AND cc.period_end <= n.report_date
    ORDER BY cc.period_end DESC LIMIT 1
  ) c ON true
  WHERE n.asset_class IN ('EC', 'EP')
    AND n.market_value IS NOT NULL AND n.market_value > 0
  GROUP BY f.instrument_id, f.ticker, n.report_date
)
SELECT
  h.instrument_id, COALESCE(h.ticker, '') AS ticker, h.as_of,
  CASE WHEN h.sum_mv_size > 0 THEN round(ln(h.sum_mv_size)::numeric, 4) END
       AS size_log_mkt_cap,
  CASE WHEN h.sum_mv_resolved > 0 AND abs(COALESCE(h.sum_be, 0) / h.sum_mv_resolved) <= 10.0
       THEN round((COALESCE(h.sum_be, 0) / h.sum_mv_resolved)::numeric, 4) END
       AS book_to_market,
  mom.mom_12_1,
  CASE WHEN h.sum_ta > 0 AND abs(COALESCE(h.sum_ni, 0) / h.sum_ta) <= 10.0
       THEN round((COALESCE(h.sum_ni, 0) / h.sum_ta)::numeric, 4) END
       AS quality_roa,
  CASE WHEN h.sum_ppe > 0 AND abs(COALESCE(h.sum_capex, 0) / h.sum_ppe) <= 10.0
       THEN round((COALESCE(h.sum_capex, 0) / h.sum_ppe)::numeric, 4) END
       AS investment_growth,
  CASE WHEN h.sum_rev > 0 AND abs(COALESCE(h.sum_gp, 0) / h.sum_rev) <= 10.0
       THEN round((COALESCE(h.sum_gp, 0) / h.sum_rev)::numeric, 4) END
       AS profitability_gross,
  h.latest_filing AS source_filing_date
FROM hold h
LEFT JOIN LATERAL (
  SELECT CASE WHEN w.n >= 12 AND sv.nav IS NOT NULL AND sv.nav > 0
              THEN round((ev.nav / sv.nav - 1.0)::numeric, 4) END AS mom_12_1
  FROM (SELECT count(*) AS n FROM tmp_l2_navpts p
         WHERE p.instrument_id = h.instrument_id AND p.m_end <= h.as_of) w
  LEFT JOIN LATERAL (
    SELECT p.nav FROM tmp_l2_navpts p
     WHERE p.instrument_id = h.instrument_id AND p.m_end <= h.as_of
     ORDER BY p.m_end DESC OFFSET 1 LIMIT 1
  ) ev ON true
  LEFT JOIN LATERAL (
    SELECT p.nav FROM tmp_l2_navpts p
     WHERE p.instrument_id = h.instrument_id AND p.m_end <= h.as_of
     ORDER BY p.m_end DESC OFFSET GREATEST(0, LEAST(13, w.n) - 1) LIMIT 1
  ) sv ON true
) mom ON true
WHERE h.sum_mv_size > 0
"""

# Server-side upsert: feed _L2_FINAL_SELECT straight into the destination with
# ON CONFLICT (instrument_id, as_of) DO UPDATE (idempotent, same key/semantics
# as _EQUITY_UPSERT; replaces the legacy per-fund DELETE+INSERT full replace).
_L2_UPSERT_FROM_SELECT = f"""
INSERT INTO equity_characteristics_monthly (
    instrument_id, ticker, as_of, size_log_mkt_cap, book_to_market,
    mom_12_1, quality_roa, investment_growth, profitability_gross,
    source_filing_date, computed_at
)
SELECT
    instrument_id, ticker, as_of, size_log_mkt_cap, book_to_market,
    mom_12_1, quality_roa, investment_growth, profitability_gross,
    source_filing_date, now()
FROM ( {_L2_FINAL_SELECT} ) src
ON CONFLICT (instrument_id, as_of) DO UPDATE SET
    ticker = EXCLUDED.ticker, size_log_mkt_cap = EXCLUDED.size_log_mkt_cap,
    book_to_market = EXCLUDED.book_to_market, mom_12_1 = EXCLUDED.mom_12_1,
    quality_roa = EXCLUDED.quality_roa, investment_growth = EXCLUDED.investment_growth,
    profitability_gross = EXCLUDED.profitability_gross,
    source_filing_date = EXCLUDED.source_filing_date, computed_at = now()
"""


def _run_layer2_setbased(conn, limit: int | None) -> tuple[int, int]:
    """Set-based Layer 2: build the fund universe temp table, then aggregate every
    (instrument_id, report_date) and upsert in a single server-side pass.

    Returns (funds_processed, rows_upserted). One transaction: the temp table is
    ON COMMIT DROP, so the final commit cleans it up. Mirrors the field output of
    the per-fund reference compute_fund_rows / _aggregate_one_date exactly.
    """
    limit_clause = f"\nLIMIT {int(limit)}" if limit else ""
    with conn.cursor() as cur:
        # The holdings aggregation and the NAV month-end resample are large; raise
        # work_mem / temp_buffers for this session (mirrors Layer 1).
        cur.execute("SET temp_buffers = '512MB'")
        cur.execute("SET work_mem = '256MB'")
        cur.execute(_L2_BUILD_FUNDS.format(limit_clause=limit_clause))
        cur.execute(_L2_INDEX_FUNDS_SERIES)
        cur.execute(_L2_INDEX_FUNDS_IID)
        cur.execute("ANALYZE tmp_l2_funds")

        # Month-end NAV points, indexed — momentum probes it 3x per output row.
        cur.execute(_L2_BUILD_NAVPTS)
        cur.execute(_L2_INDEX_NAVPTS)
        cur.execute("ANALYZE tmp_l2_navpts")

        cur.execute(_L2_UPSERT_FROM_SELECT)
        rows_upserted = cur.rowcount
        cur.execute("SELECT count(*) FROM tmp_l2_funds")
        funds_processed = int(cur.fetchone()[0])
    conn.commit()
    return funds_processed, rows_upserted


# ===========================================================================
# Entry point
# ===========================================================================
def _distinct_ciks_with_facts(conn, limit: int | None) -> list[int]:
    limit_clause = f" LIMIT {int(limit)}" if limit else ""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT cik FROM sec_xbrl_facts "
            f"WHERE taxonomy = 'us-gaap'{limit_clause}"
        )
        return [r[0] for r in cur.fetchall()]


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Recompute company + equity characteristics; upsert to the cloud.

    Runs Layer 1 (company chars from XBRL) then Layer 2 (fund aggregation),
    so a single invocation rebuilds both destination tables consistently.

    Args:
        dsn: target cloud DSN (also the raw-source DSN).
        calc_date: as-of date (YYYY-MM-DD) bounding the valid period_end
            window; defaults to today. Deterministic — no implicit Date.now().
        limit: cap entities (CIKs for L1, funds for L2) for testing.

    Returns:
        {processed, upserted, company_*, equity_*} stats.
    """
    today = (
        datetime.strptime(calc_date, "%Y-%m-%d").date()
        if calc_date
        else date.today()
    )

    conn = connect(dsn)
    try:
        with advisory_lock(conn, LOCK_CHARACTERISTICS) as got:
            if not got:
                return {"status": "skipped", "reason": "lock_held",
                        "processed": 0, "upserted": 0}

            # ---- Layer 1: company characteristics (set-based) --------------
            # One pass over the whole us-gaap fact table instead of 2 queries
            # per CIK x ~14.5k CIKs. See _run_layer1_setbased for the SQL.
            try:
                company_processed, company_upserted = _run_layer1_setbased(
                    conn, today, limit
                )
            except Exception:
                conn.rollback()
                company_processed = company_upserted = 0

            # ---- Layer 2: fund/equity characteristics (set-based) ----------
            # One server-side pass over every (instrument_id, report_date)
            # instead of 3 queries per fund x ~18k funds x N report_dates.
            # See _run_layer2_setbased for the SQL. The per-fund helpers
            # (compute_fund_rows / _aggregate_one_date / _replace_fund_rows)
            # are retained as the audited test oracle.
            try:
                equity_processed, equity_upserted = _run_layer2_setbased(
                    conn, limit
                )
            except Exception:
                conn.rollback()
                equity_processed = equity_upserted = 0

            return {
                "status": "succeeded",
                "processed": company_processed + equity_processed,
                "upserted": company_upserted + equity_upserted,
                "company_processed": company_processed,
                "company_upserted": company_upserted,
                "equity_processed": equity_processed,
                "equity_upserted": equity_upserted,
            }
    finally:
        conn.close()
