"""nport_cusip_enrichment worker — ISIN → GICS sector for international equities.

Foreign N-PORT equity holdings store a synthetic ``IS:<isin>`` in ``cusip``, so
the US-centric ``sec_cusip_ticker_map`` (CUSIP-6 keyed) can never resolve them
and the look-through "sector" collapses to "Unclassified". This worker bridges
each foreign equity ISIN to a sector, cheapest source first, cached in
``sec_isin_sector`` (misses cached too, TTL-refreshed via ``last_verified_at``).
The ``nport_lookthrough`` worker reads this alongside the CUSIP map.

1. ``resolve_by_issuer`` — deterministic, in-database, no network. A CUSIP-9
   embedded in a CGS-prefix ISIN (KY/CA/VG/BM/…: chars 3-11 ARE the CUSIP) or
   reported on the holding line resolves against ``sec_cusip_ticker_map``
   (exact CUSIP-9, then CUSIP-6 issuer level), and a normalised
   ``issuer_name`` copies the sector of an already-resolved sibling ISIN. A
   route only resolves when it saw exactly one distinct sector — an issuer
   with more than one sector is a conflict: not copied, counted in the stats,
   passed on to the external path. Cached as ``issuer_coherence``.
2. OpenFIGI (ISIN→ticker/exchange) → yfinance (ticker→sector), as before.
3. OpenFIGI by CUSIP (``ID_CUSIP``) for the leftovers that carry a CUSIP-9:
   the CUSIP index covers listings the ISIN index has expunged (retired ISINs
   of live issuers). Cached as ``openfigi_cusip+yfinance``.

Incremental: each run takes ISINs absent from / stale in ``sec_isin_sector``,
capped by ``limit``; the backlog drains over successive runs. Idempotent upsert.
yfinance is fetched in a small thread pool (it is slow and unofficial; every
per-symbol failure degrades to a NULL-sector cache row rather than crashing).

Contract: ``run(dsn, *, limit=None, ttl_days=90) -> {"gathered",
"issuer_resolved", "issuer_conflicts", "figi_resolved", "figi_cusip_resolved",
"sector_resolved", "upserted", "window_days", "retry_no_figi"}``. Env:
``OPENFIGI_API_KEY`` (keyed plan ≈250/min), ``NPORT_ENRICH_WINDOW_DAYS`` (see
``resolve_window_days``) and ``WORKER_RETRY_NO_FIGI`` (see
``resolve_retry_no_figi``: gather re-attacks cached ``no_figi`` rows inside
the window instead of waiting out their TTL).
"""

from __future__ import annotations

import os
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
WINDOW_DAYS_ENV = "NPORT_ENRICH_WINDOW_DAYS"
RETRY_NO_FIGI_ENV = "WORKER_RETRY_NO_FIGI"

# ISIN prefixes whose NSIN is a CGS-issued CUSIP-9: chars 3-11 of the ISIN ARE
# the security's CUSIP (the check digit is shared). Caribbean + Canada + US.
_CGS_PREFIXES = (
    "CA", "KY", "VG", "BM", "BB", "VI", "MH", "PA", "BS", "AI", "AN", "JM",
    "TT", "US",
)

# Route precedence inside resolve_by_issuer: an exact CUSIP-9 hit is the
# security itself, CUSIP-6 is the issuer, the normalised name is the loosest
# join — take the strongest route that saw exactly one distinct sector.
_ISSUER_ROUTES = ("c9", "c6", "name")


def resolve_window_days(raw: str | None) -> int:
    """Days of report history the gather scan covers, from ``raw``.

    Unset or empty means ``_RECENT_REPORT_DAYS`` — the 120-day window the cron
    has always swept, unchanged. The variable exists because that window is
    anchored on ``max(report_date)`` and is therefore blind to any repaired
    history behind it: after the 2026-08-05 N-PORT identifier repair, the 3.804
    foreign-equity ISINs of the eight repaired report_dates sit 8 to 26 months
    outside it, so no invocation of this worker could reach them and their
    holdings stay "Unclassified" in the look-through's sector dimension.

    Spelling is strict — plain ASCII digits, no sign, no decimal point, no unit
    suffix — and anything else raises instead of falling back to the default. A
    window the platform accepts but this worker silently ignores is the worse
    failure: the deploy goes green, the run reports success, and the operator
    reads a verdict for dates that were never scanned.
    """
    if raw is None or not raw.strip():
        return _RECENT_REPORT_DAYS
    value = raw.strip()
    if not (value.isascii() and value.isdigit()):
        raise ValueError(
            f"{WINDOW_DAYS_ENV}={raw!r} is not a whole number of days"
        )
    return int(value)


def resolve_retry_no_figi(raw: str | None) -> bool:
    """Whether this run re-attacks cached ``no_figi`` rows, from ``raw``.

    The 2026-08 campaign left 1.635 fresh ``no_figi`` rows whose TTL only
    reopens in November; the issuer-coherence and ID_CUSIP legs can resolve a
    third of them today. This one-shot switch makes the gather select exactly
    the cached ``resolved_via='no_figi'`` ISINs inside the window, TTL
    ignored — the ``ON CONFLICT DO UPDATE`` upsert then rewrites each row with
    whatever this run learned (including a fresh ``no_figi``, which is the
    point: the miss is re-dated, not re-queried every cycle).

    Spelling is strict for the same reason ``resolve_window_days`` is: ``1``
    is on, unset/empty/``0`` is off, anything else raises instead of silently
    running the default sweep while the config claims a retry.
    """
    if raw is None or not raw.strip():
        return False
    value = raw.strip()
    if value == "1":
        return True
    if value == "0":
        return False
    raise ValueError(f"{RETRY_NO_FIGI_ENV}={raw!r} is not 0 or 1")

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

# WORKER_RETRY_NO_FIGI=1: the same gather, but aimed at the cached misses —
# every ISIN the cache filed as no_figi that still has EC/EP holdings inside
# the window, regardless of how fresh the miss is.
_GATHER_RETRY_SQL = """
SELECT h.isin
FROM sec_nport_holdings h
JOIN sec_isin_sector s ON s.isin = h.isin
WHERE s.resolved_via = 'no_figi'
  AND h.asset_class IN ('EC', 'EP')
  AND h.isin IS NOT NULL
  AND left(h.isin, 2) <> 'US'
  AND h.report_date >= (SELECT max(report_date) FROM sec_nport_holdings)
                       - make_interval(days => %s)
GROUP BY h.isin
ORDER BY sum(h.pct_of_nav) DESC NULLS LAST
LIMIT %s
"""

# Candidate CUSIP-9s per gathered ISIN: the one embedded in a CGS-prefix ISIN,
# plus any real CUSIP reported on the holding lines themselves (foreign
# holdings usually carry the synthetic IS:<isin>, but 8.5k lines carry the
# actual CUSIP).
_CUSIP_CANDIDATES_SQL = """
SELECT isin, c9
FROM (
    SELECT t.isin, substring(t.isin, 3, 9) AS c9
    FROM unnest(%(isins)s::text[]) AS t(isin)
    WHERE left(t.isin, 2) = ANY(%(cgs)s::text[])
    UNION
    SELECT h.isin, h.cusip
    FROM sec_nport_holdings h
    WHERE h.isin = ANY(%(isins)s::text[])
      AND h.report_date >= (SELECT max(report_date) FROM sec_nport_holdings)
                           - make_interval(days => %(window_days)s)
      AND h.cusip IS NOT NULL
      AND h.cusip !~ '^IS:'
      AND length(h.cusip) = 9
) cand
"""

# Routes A/B of the issuer-coherence step: candidate CUSIP against the map,
# exact CUSIP-9 first (the security itself), then CUSIP-6 (the issuer). Rows
# come back per route; the conflict guard is applied in Python where it is
# testable.
_CUSIP_SECTOR_SQL = """
WITH cand AS (
    SELECT DISTINCT isin, c9
    FROM unnest(%(isins)s::text[], %(cusips)s::text[]) AS c(isin, c9)
)
SELECT c.isin, 'c9' AS route, m.gics_sector
FROM cand c
JOIN sec_cusip_ticker_map m ON m.cusip = c.c9
WHERE m.gics_sector IS NOT NULL
UNION
SELECT c.isin, 'c6', m.gics_sector
FROM cand c
JOIN sec_cusip_ticker_map m ON left(m.cusip, 6) = left(c.c9, 6)
WHERE m.gics_sector IS NOT NULL
"""

# Route C: the normalised issuer_name of a gathered ISIN copies the sector of
# an already-resolved sibling ISIN of the same issuer. The gathered batch is
# excluded from the sibling side so a stale row can never re-affirm itself
# through its own name.
_ISSUER_NAME_SECTOR_SQL = """
WITH names AS (
    SELECT DISTINCT h.isin, upper(btrim(h.issuer_name)) AS iname
    FROM sec_nport_holdings h
    WHERE h.isin = ANY(%(isins)s::text[])
      AND h.report_date >= (SELECT max(report_date) FROM sec_nport_holdings)
                           - make_interval(days => %(window_days)s)
      AND h.issuer_name IS NOT NULL
      AND btrim(h.issuer_name) <> ''
),
siblings AS (
    SELECT DISTINCT upper(btrim(h.issuer_name)) AS iname, s.gics_sector
    FROM sec_nport_holdings h
    JOIN sec_isin_sector s ON s.isin = h.isin
    WHERE s.gics_sector IS NOT NULL
      AND NOT (h.isin = ANY(%(isins)s::text[]))
      AND h.report_date >= (SELECT max(report_date) FROM sec_nport_holdings)
                           - make_interval(days => %(window_days)s)
      AND h.issuer_name IS NOT NULL
      AND btrim(h.issuer_name) <> ''
      AND upper(btrim(h.issuer_name)) IN (SELECT iname FROM names)
)
SELECT DISTINCT n.isin, sib.gics_sector
FROM names n
JOIN siblings sib ON sib.iname = n.iname
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


@dataclass(frozen=True)
class IssuerResolution:
    """What the deterministic step learned about one gathered batch.

    ``sectors`` maps each resolved ISIN to its sector; ``conflicts`` are the
    ISINs some route saw but every seeing route was multi-sector (not copied);
    ``cusips`` maps each ISIN to its candidate CUSIP-9s (embedded one first)
    for the ID_CUSIP leg downstream.
    """

    sectors: dict[str, str]
    conflicts: frozenset[str]
    cusips: dict[str, list[str]]


def merge_issuer_candidates(
    rows: list[tuple[str, str, str]],
) -> tuple[dict[str, str], set[str]]:
    """Pure: (isin, route, sector) rows → ({isin: sector}, conflicted isins).

    The conflict guard, per route: an ISIN resolves through a route only when
    that route saw exactly one distinct sector. Routes are then taken in
    ``_ISSUER_ROUTES`` precedence — the strongest clean route wins, a weaker
    route's disagreement does not poison it. An ISIN that some route saw but
    no route saw cleanly is a conflict: not resolved, counted, passed on to
    the external path. (Measured on 41.9k resolved issuer names: 27 — 0,06 % —
    map to more than one sector, so the guard bites rarely but keeps the copy
    honest.)
    """
    by_isin: dict[str, dict[str, set[str]]] = {}
    for isin, route, sector in rows:
        by_isin.setdefault(isin, {}).setdefault(route, set()).add(sector)
    resolved: dict[str, str] = {}
    conflicts: set[str] = set()
    for isin, routes in by_isin.items():
        for route in _ISSUER_ROUTES:
            sectors = routes.get(route)
            if sectors is not None and len(sectors) == 1:
                resolved[isin] = next(iter(sectors))
                break
        else:
            conflicts.add(isin)
    return resolved, conflicts


def resolve_by_issuer(conn, isins: list[str], window_days: int) -> IssuerResolution:
    """Deterministic sector resolution from data already in the database.

    Runs before any external call: routes A/B (CUSIP-9 embedded in a
    CGS-prefix ISIN or reported on the holding line → ``sec_cusip_ticker_map``)
    and route C (normalised ``issuer_name`` → sector of an already-resolved
    sibling ISIN), merged under the per-route conflict guard in
    ``merge_issuer_candidates``. Also returns the candidate CUSIPs so the
    OpenFIGI ``ID_CUSIP`` leg does not re-derive them.
    """
    if not isins:
        return IssuerResolution({}, frozenset(), {})
    batch = list(isins)
    cusips: dict[str, list[str]] = {}
    route_rows: list[tuple[str, str, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            _CUSIP_CANDIDATES_SQL,
            {"isins": batch, "cgs": list(_CGS_PREFIXES), "window_days": window_days},
        )
        pairs = cur.fetchall()
        for isin, c9 in pairs:
            cusips.setdefault(isin, []).append(c9)
        for isin, cands in cusips.items():
            embedded = isin[2:11] if isin[:2] in _CGS_PREFIXES else None
            cands.sort(key=lambda c: (c != embedded, c))
        if pairs:
            cur.execute(
                _CUSIP_SECTOR_SQL,
                {"isins": [p[0] for p in pairs], "cusips": [p[1] for p in pairs]},
            )
            route_rows.extend(cur.fetchall())
        cur.execute(
            _ISSUER_NAME_SECTOR_SQL,
            {"isins": batch, "window_days": window_days},
        )
        route_rows.extend((isin, "name", sector) for isin, sector in cur.fetchall())
    sectors, conflicts = merge_issuer_candidates(route_rows)
    return IssuerResolution(
        sectors=sectors, conflicts=frozenset(conflicts), cusips=cusips
    )


def enrich_rows(
    isins: list[str],
    figi_matches: dict[str, FigiMatch],
    sector_fn,
    *,
    via_cusip: frozenset[str] | set[str] = frozenset(),
) -> list[EnrichRow]:
    """Pure: ISINs + OpenFIGI matches + (yahoo_symbol→GICS) fn → cache rows.

    No network. Misses are cached with a reason so they are not re-queried each
    run: ``no_figi`` (OpenFIGI found nothing), ``no_yahoo_symbol`` (exchange not
    in the crosswalk), ``openfigi_no_sector`` (symbol built but yfinance had no
    usable sector), ``openfigi+yfinance`` (resolved). ISINs in ``via_cusip``
    were matched through the ``ID_CUSIP`` index instead of the ISIN one and
    carry that in their provenance (``openfigi_cusip+yfinance`` /
    ``openfigi_cusip_no_sector``) so the two legs stay auditable apart.
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
        source = "openfigi_cusip" if isin in via_cusip else "openfigi"
        via = f"{source}+yfinance" if gics else f"{source}_no_sector"
        out.append(EnrichRow(isin, gics, match.ticker, symbol, via))
    return out


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
    conn.commit()


def _gather_isins(
    conn,
    limit: int,
    ttl_days: int,
    window_days: int,
    *,
    retry_no_figi: bool = False,
) -> list[str]:
    with conn.cursor() as cur:
        if retry_no_figi:
            cur.execute(_GATHER_RETRY_SQL, (window_days, limit))
        else:
            cur.execute(_GATHER_SQL, (window_days, ttl_days, limit))
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
    # Resolved before the connection so a mistyped window fails on its own name,
    # not three layers down after the advisory lock is taken. It rides in the
    # stats for the same reason ``calc_date`` does: the run log is the only place
    # an operator can check which history a green run actually scanned.
    window_days = resolve_window_days(os.getenv(WINDOW_DAYS_ENV))
    retry_no_figi = resolve_retry_no_figi(os.getenv(RETRY_NO_FIGI_ENV))
    with connect(resolve_dsn(dsn)) as conn:
        with advisory_lock(conn, LOCK_NPORT_CUSIP_ENRICHMENT) as got:
            if not got:
                return {"skipped": "lock_busy"}
            ensure_schema(conn)
            isins = _gather_isins(
                conn, limit or DEFAULT_RUN_LIMIT, ttl_days, window_days,
                retry_no_figi=retry_no_figi,
            )
            if not isins:
                return {"gathered": 0, "issuer_resolved": 0,
                        "issuer_conflicts": 0, "figi_resolved": 0,
                        "figi_cusip_resolved": 0, "sector_resolved": 0,
                        "upserted": 0, "window_days": window_days,
                        "retry_no_figi": retry_no_figi}
            issuer = resolve_by_issuer(conn, isins, window_days)
            issuer_rows = [
                EnrichRow(isin, sector, None, None, "issuer_coherence")
                for isin, sector in sorted(issuer.sectors.items())
            ]
            remaining = [i for i in isins if i not in issuer.sectors]
            via_cusip: set[str] = set()
            with OpenFigiClient() as figi:
                matches = figi.map_isins(remaining)
                figi_by_isin = len(matches)
                # Leftovers with a CUSIP-9 in hand: the ID_CUSIP index covers
                # listings the ISIN index has expunged. One extra request per
                # 100 candidate CUSIPs, same token bucket.
                cusip_isins = [
                    i for i in remaining if i not in matches and i in issuer.cusips
                ]
                cusip_pool = sorted(
                    {c for i in cusip_isins for c in issuer.cusips[i]}
                )
                cusip_matches = figi.map_cusips(cusip_pool) if cusip_pool else {}
                for isin in cusip_isins:
                    for cusip in issuer.cusips[isin]:
                        match = cusip_matches.get(cusip)
                        if match is not None:
                            matches[isin] = match
                            via_cusip.add(isin)
                            break
            sector_by_symbol = _fetch_sectors(matches)
            rows = enrich_rows(
                remaining, matches, sector_by_symbol.get, via_cusip=via_cusip
            )
            all_rows = issuer_rows + rows
            _upsert(conn, all_rows)
            return {
                "gathered": len(isins),
                "issuer_resolved": len(issuer_rows),
                "issuer_conflicts": len(issuer.conflicts),
                "figi_resolved": figi_by_isin,
                "figi_cusip_resolved": len(via_cusip),
                "sector_resolved": sum(1 for r in all_rows if r.gics_sector),
                "upserted": len(all_rows),
                "window_days": window_days,
                "retry_no_figi": retry_no_figi,
            }
