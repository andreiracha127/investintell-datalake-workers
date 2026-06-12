"""Audit tests for src/workers/characteristics.py.

These recompute characteristics from the RAW series in the legacy DB-mãe
(localhost:5434, which still holds every source table) using the worker's
own functions, then compare against the LEGACY stored values in
equity_characteristics_monthly / company_characteristics_monthly.

The DB-mãe is used as the validation oracle because the cloud data-lake
currently only carries sec_nport_holdings — nav_timeseries, sec_xbrl_facts,
sec_cusip_ticker_map, instruments_universe and instrument_identity (all
inputs to the recompute) are not yet replicated there. The worker reads
identical SQL against either DSN, so a DB-mãe match proves the cloud run
once the sources land.

Run:
    pytest tests/test_characteristics.py -v -s
Requires DB-mãe reachable; skips cleanly otherwise.
"""

from __future__ import annotations

import pytest

from src.workers import characteristics as C

MAE_DSN = (
    "host=localhost port=5434 dbname=investintell_alloc "
    "user=investintell password=investintell"
)

# Pure-NAV momentum is exactly reproducible; ratios depend on the XBRL vintage
# the legacy row was computed against (source_filing_date drift), so allow a
# wider band there. size_log is a log of summed AUM — tight.
TOL = {
    "mom_12_1": 1e-4,
    "size_log_mkt_cap": 0.25,
    "book_to_market": 0.10,
    "quality_roa": 0.10,
    "investment_growth": 0.10,
    "profitability_gross": 0.10,
}


@pytest.fixture(scope="module")
def conn():
    psycopg = pytest.importorskip("psycopg")
    try:
        c = psycopg.connect(MAE_DSN)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"DB-mãe unreachable: {exc}")
    yield c
    c.close()


def _legacy_equity_row(conn, instrument_id, as_of) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT size_log_mkt_cap, book_to_market, mom_12_1, quality_roa,
                      investment_growth, profitability_gross
               FROM equity_characteristics_monthly
               WHERE instrument_id = %s AND as_of = %s""",
            (instrument_id, as_of),
        )
        r = cur.fetchone()
    if r is None:
        return None
    cols = ("size_log_mkt_cap", "book_to_market", "mom_12_1", "quality_roa",
            "investment_growth", "profitability_gross")
    return {c: (float(v) if v is not None else None) for c, v in zip(cols, r)}


def _resolve_fund(conn, instrument_id) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT i.instrument_id, i.ticker,
                      COALESCE(NULLIF(ii.sec_series_id,''),
                               NULLIF(i.attributes->>'sec_series_id',''),
                               NULLIF(i.attributes->>'series_id','')) AS series_id
               FROM instruments_universe i
               LEFT JOIN instrument_identity ii USING (instrument_id)
               WHERE i.instrument_id = %s""",
            (instrument_id,),
        )
        iid, tk, sid = cur.fetchone()
    return {"instrument_id": iid, "ticker": tk, "series_id": sid}


# Two real entity-months (fund AAAAX, series S000032019) with legacy rows.
EQUITY_CASES = [
    ("e7a44503-cb14-4541-b25e-3be90aeecab4", "2020-03-31"),
    ("e7a44503-cb14-4541-b25e-3be90aeecab4", "2021-03-31"),
]


@pytest.mark.parametrize("instrument_id,as_of", EQUITY_CASES)
def test_equity_recalc_matches_legacy(conn, instrument_id, as_of):
    import datetime as dt

    fund = _resolve_fund(conn, instrument_id)
    assert fund["series_id"], "fund must resolve to a series_id"

    nav = C._load_nav_month_end(conn, fund["instrument_id"])
    as_of_date = dt.date.fromisoformat(as_of)
    recalc = C._aggregate_one_date(conn, fund, as_of_date, nav)
    assert recalc is not None, "recompute produced no row"

    legacy = _legacy_equity_row(conn, instrument_id, as_of)
    assert legacy is not None, "legacy row missing"

    diffs = []
    for char, tol in TOL.items():
        rv, lv = recalc[char], legacy[char]
        if rv is None or lv is None:
            diffs.append(f"{char}: recalc={rv} legacy={lv} (None)")
            continue
        d = abs(rv - lv)
        flag = "OK" if d <= tol else "FAIL"
        diffs.append(f"{char}: recalc={rv:.4f} legacy={lv:.4f} |d|={d:.4f} tol={tol} {flag}")
    report = f"\n[{fund['ticker']} @ {as_of}]\n  " + "\n  ".join(diffs)
    print(report)

    # mom_12_1 must match to 4dp (pure NAV, no vintage dependency).
    if recalc["mom_12_1"] is not None and legacy["mom_12_1"] is not None:
        assert abs(recalc["mom_12_1"] - legacy["mom_12_1"]) <= TOL["mom_12_1"], report

    # Remaining chars within their (vintage-tolerant) bands.
    for char in ("size_log_mkt_cap", "book_to_market", "quality_roa",
                 "investment_growth", "profitability_gross"):
        rv, lv = recalc[char], legacy[char]
        if rv is not None and lv is not None:
            assert abs(rv - lv) <= TOL[char], report


def test_company_recalc_for_one_cik(conn):
    """Recompute Layer-1 chars for a CIK and sanity-check vs legacy rows."""
    import datetime as dt

    # Pick a CIK that has legacy company rows.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cik FROM company_characteristics_monthly "
            "GROUP BY cik ORDER BY count(*) DESC LIMIT 1"
        )
        cik = cur.fetchone()[0]

    rows = C._compute_company_rows(conn, int(cik), dt.date(2026, 6, 11))
    assert rows, f"no recomputed company rows for cik={cik}"

    recalc_by_pe = {r["period_end"]: r for r in rows}
    with conn.cursor() as cur:
        cur.execute(
            """SELECT period_end, total_assets, quality_roa, profitability_gross
               FROM company_characteristics_monthly
               WHERE cik = %s ORDER BY period_end DESC LIMIT 3""",
            (cik,),
        )
        legacy = cur.fetchall()

    compared = 0
    for period_end, ta, qroa, pg in legacy:
        rc = recalc_by_pe.get(period_end)
        if rc is None:
            continue
        compared += 1
        if ta is not None and rc["total_assets"] is not None:
            # total_assets is a raw passthrough — should match closely.
            assert abs(float(rc["total_assets"]) - float(ta)) <= abs(float(ta)) * 0.01 + 1, (
                f"cik={cik} pe={period_end} total_assets recalc={rc['total_assets']} legacy={ta}"
            )
        print(
            f"cik={cik} pe={period_end} "
            f"total_assets recalc={rc['total_assets']} legacy={ta} | "
            f"quality_roa recalc={rc['quality_roa']} legacy={qroa} | "
            f"profitability_gross recalc={rc['profitability_gross']} legacy={pg}"
        )
    assert compared >= 1, "no overlapping period_end to compare"
