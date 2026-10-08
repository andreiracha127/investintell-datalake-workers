"""Content lineage for inputs that can change independently of raw N-PORT loads.

Receipts are committed with serving rows. Fingerprints include deletions and
same-date repairs, and exclude computation timestamps that do not change values.
The aggregate uses constant memory, even for the holding-weight sidecar.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

from psycopg.errors import LockNotAvailable

from src.workers import _fund_pipeline_freshness as freshness

# Bump when changing the set or meaning of certified calculation inputs.
LINEAGE_VERSION = 1


def ensure_schema(conn):
    conn.execute((Path(__file__).resolve().parents[2] / "schemas"
                  / "nport_pipeline_publications_v1.sql").read_text(encoding="utf-8"))


def _relations(stage):
    common = {
        "instruments_universe": ("instrument_id, ticker, isin, attributes->>'sec_series_id', "
                                 "attributes->>'series_id'", "TRUE"),
        "instrument_identity": ("instrument_id, sec_series_id, cusip_9, isin", "TRUE"),
    }
    if stage == "characteristics":
        return common | {
            "sec_cusip_ticker_map": ("cusip, issuer_cik", "TRUE"),
            "company_characteristics_monthly": (
                "cik, period_end, book_equity, total_assets, net_income_ttm, revenue, "
                "gross_profit, capex_ttm, ppe_prior, shares_outstanding, source_filing_date",
                "period_end <= %(as_of)s"),
            "nav_timeseries": (
                "instrument_id, nav_date, nav",
                """nav_date <= %(as_of)s AND instrument_id IN (
                    SELECT i.instrument_id FROM public.instruments_universe i
                    LEFT JOIN public.instrument_identity ii USING (instrument_id)
                    WHERE COALESCE(NULLIF(ii.sec_series_id, ''),
                        NULLIF(i.attributes->>'sec_series_id', ''),
                        NULLIF(i.attributes->>'series_id', '')) = ANY(%(series)s::text[])
                )"""),
        }
    if stage == "lookthrough":
        return common | {
            "sec_cusip_ticker_map": ("cusip, ticker, gics_sector", "TRUE"),
            "sec_fund_classes": ("ticker, series_id", "TRUE"),
            "sec_etfs": ("ticker, series_id, isin", "TRUE"),
            "sec_company_tickers_mf": ("ticker, series_id", "TRUE"),
            "sec_isin_sector": ("isin, gics_sector", "TRUE"),
            "nport_equity_exposure_summary": (
                "series_id, report_date, gross_equity_pct, net_equity_pct",
                "report_date BETWEEN %(child_start)s AND %(as_of)s"),
            "nport_equity_country_exposures": (
                "series_id, report_date, country, direct_pct",
                "report_date BETWEEN %(child_start)s AND %(as_of)s"),
            "nport_equity_holding_weights": (
                "series_id, report_date, cusip, signed_pct_of_nav, gross_pct_of_nav",
                "report_date BETWEEN %(child_start)s AND %(as_of)s"),
        }
    raise ValueError(stage)


def snapshot(conn, source, stage):
    """Fingerprint auxiliary values; the raw signature/watermark is separate.

    Include all potentially expanded children, not just current parent series.
    Use a fixed window relative to the anchor so passing midnight does not by
    itself invalidate a receipt. It contains the complete admissible child window.
    """
    params = {"as_of": source.as_of, "series": list(source.series),
              "child_start": source.as_of - dt.timedelta(days=source.policy.max_chain_report_age_days)}
    result = {}
    for table, (columns, predicate) in sorted(_relations(stage).items()):
        if not conn.execute("SELECT to_regclass(%s)", (f"public.{table}",)).fetchone()[0]:
            result[table] = None
            continue
        row = conn.execute(
            f"""SELECT count(*),
                       sum(('x' || substr(h, 1, 16))::bit(64)::bigint::numeric),
                       sum(('x' || substr(h, 17, 16))::bit(64)::bigint::numeric)
                FROM (SELECT md5(ROW({columns})::text) h FROM public.{table}
                      WHERE {predicate}) inputs""", params,
        ).fetchone()
        result[table] = [str(value) for value in row]
    return result


def signature(source, inputs):
    return hashlib.sha256(json.dumps(
        [LINEAGE_VERSION, source.signature, source.load_watermark, inputs], sort_keys=True,
    ).encode()).hexdigest()


def certify(conn, source, stage, inputs):
    conn.execute(
        """INSERT INTO public.nport_pipeline_publications (stage, input_signature)
           VALUES (%s, %s) ON CONFLICT (stage) DO UPDATE
           SET input_signature = EXCLUDED.input_signature, published_at = clock_timestamp()""",
        (stage, signature(source, inputs)),
    )


def current(conn, source, stage):
    if not conn.execute("SELECT to_regclass('public.nport_pipeline_publications')").fetchone()[0]:
        return False
    row = conn.execute(
        "SELECT input_signature FROM public.nport_pipeline_publications WHERE stage = %s", (stage,),
    ).fetchone()
    return bool(row and row[0] == signature(source, snapshot(conn, source, stage)))


def guard(conn, source, stage, before):
    """Close auxiliary-writer races through commit, without waiting on writers.

    SHARE conflicts with ordinary DML, including the independent exact-sidecar
    backfill. Runtime needs existing write grants (SELECT alone is insufficient).
    Sorted NOWAIT acquisition cannot deadlock; a conflict aborts the candidate.
    Missing optional tables remain part of the fingerprint.
    """
    try:
        for table, value in sorted(before.items()):
            if value is not None:
                conn.execute(f"LOCK TABLE public.{table} IN SHARE MODE NOWAIT")
    except LockNotAvailable as exc:
        raise freshness.FundPipelineBlocked({
            "stage": stage, "alarm": True, "breaches": ["INPUT_WRITE_IN_PROGRESS"],
            "last_good_preserved": True,
        }) from exc
    if snapshot(conn, source, stage) != before:
        raise freshness.FundPipelineBlocked({
            "stage": stage, "alarm": True, "breaches": ["INPUT_CHANGED_DURING_BUILD"],
            "last_good_preserved": True,
        })
