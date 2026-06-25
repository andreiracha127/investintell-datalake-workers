"""nport_cusip_enrichment worker — ISIN → GICS sector for international equities.

Foreign N-PORT equity holdings store a synthetic ``IS:<isin>`` in ``cusip``, so
the US-centric ``sec_cusip_ticker_map`` (CUSIP-6 keyed) can never resolve them
and the look-through "sector" collapses to "Unclassified". This worker bridges
each foreign equity ISIN to a sector: OpenFIGI (ISIN→ticker/exchange) → yfinance
(ticker→sector) → canonical GICS, cached in ``sec_isin_sector`` (misses cached
too, TTL-refreshed via ``last_verified_at``). The ``nport_lookthrough`` worker
reads this alongside the CUSIP map.

Incremental: each run takes ISINs absent from / stale in ``sec_isin_sector``,
capped by ``limit``; the backlog drains over successive runs. Idempotent upsert.
yfinance is fetched in a small thread pool (it is slow and unofficial; every
per-symbol failure degrades to a NULL-sector cache row rather than crashing).

Contract: ``run(dsn, *, limit=None, ttl_days=90) -> {"gathered", "figi_resolved",
"sector_resolved", "upserted"}``. Env: ``OPENFIGI_API_KEY`` (keyed plan ≈250/min).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from src.db import (
    LOCK_NPORT_CUSIP_ENRICHMENT,
    advisory_lock,
    connect,
    resolve_dsn,
)
from src.workers._openfigi import FigiMatch, OpenFigiClient
from src.workers._yahoo_sector import fetch_sector, yahoo_symbol

DEFAULT_RUN_LIMIT = 5000          # ISINs per run — backlog drains over weeks
DEFAULT_TTL_DAYS = 90             # re-verify a cached ISIN after this
YF_WORKERS = 3                    # modest overlap; the global token bucket paces
# Only recent reports define the live look-through universe — bound the scan.
_RECENT_REPORT_DAYS = 120

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sec_isin_sector (
    isin             text PRIMARY KEY,
    gics_sector      text,
    ticker           text,
    yahoo_symbol     text,
    resolved_via     text,
    last_verified_at timestamptz NOT NULL DEFAULT now()
);
"""

_GATHER_SQL = """
SELECT h.isin
FROM sec_nport_holdings h
LEFT JOIN sec_isin_sector s ON s.isin = h.isin
WHERE h.asset_class IN ('EC', 'EP')
  AND h.isin IS NOT NULL
  AND left(h.isin, 2) <> 'US'
  AND h.report_date >= (SELECT max(report_date) FROM sec_nport_holdings)
                       - make_interval(days => %s)
  AND (s.isin IS NULL OR s.last_verified_at < now() - make_interval(days => %s))
GROUP BY h.isin
ORDER BY sum(h.pct_of_nav) DESC NULLS LAST
LIMIT %s
"""

_UPSERT_SQL = """
INSERT INTO sec_isin_sector
    (isin, gics_sector, ticker, yahoo_symbol, resolved_via, last_verified_at)
VALUES (%s, %s, %s, %s, %s, now())
ON CONFLICT (isin) DO UPDATE SET
    gics_sector      = EXCLUDED.gics_sector,
    ticker           = EXCLUDED.ticker,
    yahoo_symbol     = EXCLUDED.yahoo_symbol,
    resolved_via     = EXCLUDED.resolved_via,
    last_verified_at = now()
"""


@dataclass(frozen=True)
class EnrichRow:
    """One cache row: the ISIN, its resolved sector (or None), and provenance."""

    isin: str
    gics_sector: str | None
    ticker: str | None
    yahoo_symbol: str | None
    resolved_via: str


def enrich_rows(
    isins: list[str],
    figi_matches: dict[str, FigiMatch],
    sector_fn,
) -> list[EnrichRow]:
    """Pure: ISINs + OpenFIGI matches + (yahoo_symbol→GICS) fn → cache rows.

    No network. Misses are cached with a reason so they are not re-queried each
    run: ``no_figi`` (OpenFIGI found nothing), ``no_yahoo_symbol`` (exchange not
    in the crosswalk), ``openfigi_no_sector`` (symbol built but yfinance had no
    usable sector), ``openfigi+yfinance`` (resolved).
    """
    out: list[EnrichRow] = []
    for isin in isins:
        match = figi_matches.get(isin)
        if match is None:
            out.append(EnrichRow(isin, None, None, None, "no_figi"))
            continue
        symbol = yahoo_symbol(match.ticker, match.exch_code)
        if not symbol:
            out.append(EnrichRow(isin, None, match.ticker, None, "no_yahoo_symbol"))
            continue
        gics = sector_fn(symbol)
        via = "openfigi+yfinance" if gics else "openfigi_no_sector"
        out.append(EnrichRow(isin, gics, match.ticker, symbol, via))
    return out


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
    conn.commit()


def _gather_isins(conn, limit: int, ttl_days: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(_GATHER_SQL, (_RECENT_REPORT_DAYS, ttl_days, limit))
        return [row[0] for row in cur.fetchall()]


def _fetch_sectors(figi_matches: dict[str, FigiMatch]) -> dict[str, str | None]:
    """yahoo_symbol → GICS for every distinct resolvable symbol, in parallel."""
    symbols = sorted(
        {
            s
            for m in figi_matches.values()
            if (s := yahoo_symbol(m.ticker, m.exch_code))
        }
    )
    if not symbols:
        return {}
    with ThreadPoolExecutor(max_workers=YF_WORKERS) as pool:
        results = list(pool.map(fetch_sector, symbols))
    return dict(zip(symbols, results))


def _upsert(conn, rows: list[EnrichRow]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            _UPSERT_SQL,
            [(r.isin, r.gics_sector, r.ticker, r.yahoo_symbol, r.resolved_via) for r in rows],
        )
    conn.commit()


def run(
    dsn: str | None = None,
    *,
    limit: int | None = None,
    ttl_days: int = DEFAULT_TTL_DAYS,
) -> dict:
    """Enrich one batch of un-cached foreign equity ISINs with a GICS sector."""
    with connect(resolve_dsn(dsn)) as conn:
        with advisory_lock(conn, LOCK_NPORT_CUSIP_ENRICHMENT) as got:
            if not got:
                return {"skipped": "lock_busy"}
            ensure_schema(conn)
            isins = _gather_isins(conn, limit or DEFAULT_RUN_LIMIT, ttl_days)
            if not isins:
                return {"gathered": 0, "figi_resolved": 0,
                        "sector_resolved": 0, "upserted": 0}
            with OpenFigiClient() as figi:
                matches = figi.map_isins(isins)
            sector_by_symbol = _fetch_sectors(matches)
            rows = enrich_rows(isins, matches, sector_by_symbol.get)
            _upsert(conn, rows)
            return {
                "gathered": len(isins),
                "figi_resolved": len(matches),
                "sector_resolved": sum(1 for r in rows if r.gics_sector),
                "upserted": len(rows),
            }
