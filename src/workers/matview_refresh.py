"""Refresh Light read models and publish the complete Stocks overview.

The locked run refreshes app MVs first, publishes market_overview_snapshot from
those fresh inputs, then refreshes the datalake relationship MVs. MV refreshes
use autocommit because REFRESH ... CONCURRENTLY cannot run in a transaction and
require the UNIQUE indexes versioned in the Light DDLs.
"""
from __future__ import annotations

import os

from src import sec_effective_matviews
from src.db import LOCK_MATVIEW_REFRESH, advisory_lock, connect
from src.workers import market_overview_snapshot

_APP_MVS = [
    "price_latest_mv",
    "nav_latest_mv",
    # Cobertura de NAV por fundo (n_obs / first_nav / last_nav sobre história
    # completa). Serve os dois gates de qualidade do universo do builder, que
    # antes reagregavam a hypertable nav_timeseries inteira a CADA request
    # (35,7M linhas em 115 chunks; 7,96s + 8,31s medidos, ~16,3s dos ~17s de um
    # POST /builder/optimize). Fica ao lado de nav_latest_mv por ler a mesma
    # fonte — o NAV que os workers do dia acabaram de escrever.
    # DDL: Light backend/db/ddl/2026-07-29_fund_nav_coverage_mv.sql
    "fund_nav_coverage_mv",
    # Grupo A: read-models de fund analytics agregados (style-drift/top-holdings).
    # Active-share deixou de ser MV standalone — vive em colunas de
    # fund_risk_metrics, projetadas em fund_risk_latest_mv (refrescada pelo worker
    # risk_metrics). As *_latest_mv de fatores/reveal são refrescadas pelos seus
    # próprios workers (fund_factors / fund_institutional_reveal), não aqui.
    "fund_style_drift_mv",
    "fund_top_holdings_mv",
]
_APP_BOOTSTRAP_MVS = [
    # This additive view is installed WITH NO DATA and depends on the completed
    # fund_top_holdings_mv snapshot above. Its first refresh must be plain; all
    # pre-existing MVs retain their original always-concurrent path.
    "fund_reveal_holdings_mv",
    # sec_13f_holdings is compressed by report date. This indexed projection
    # keeps only each CUSIP's latest rows so reveal does not rescan all 97
    # chunks for every fund. It follows the N-PORT read model to keep the
    # reveal dependency chain explicit.
    "fund_reveal_13f_holdings_mv",
]
_DATALAKE_MVS = [
    "stock_institutional_holders_mv",
    "stock_fund_holders_mv",
    "holding_reverse_lookup_mv",
]
# The SEC "effective" selection caches are refreshed CONDITIONALLY, next to the
# unconditional list rather than in it: their sources (N-CEN / RR1 packages) land
# on a quarterly-ish cadence, so refreshing them daily would re-expand the whole
# raw selection to rebuild an identical relation. src.sec_effective_matviews
# compares the family's validated-run signature and skips when nothing landed.
# Registered here so the caches stay fresh even on days the publication chain
# does not run.


def _refresh_all(dsn: str, mvs: list[str]) -> list[str]:
    refreshed: list[str] = []
    with connect(dsn, autocommit=True) as conn:
        for mv in mvs:
            with conn.cursor() as cur:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
            refreshed.append(mv)
    return refreshed


def _refresh_bootstrap_mvs(dsn: str, mvs: list[str]) -> list[str]:
    refreshed: list[str] = []
    with connect(dsn, autocommit=True) as conn:
        for mv in mvs:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT c.relispopulated FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relkind = 'm' AND c.relname = %s "
                    "  AND n.nspname = ANY (current_schemas(true)) "
                    "ORDER BY array_position(current_schemas(true), n.nspname) LIMIT 1",
                    (mv,),
                )
                row = cur.fetchone()
                populated = bool(row and row[0])
                concurrently = "CONCURRENTLY " if populated else ""
                cur.execute(f"REFRESH MATERIALIZED VIEW {concurrently}{mv}")
            refreshed.append(mv)
    return refreshed


def run(dsn: str, *, datalake_dsn: str | None = None) -> dict:
    if datalake_dsn is None:
        datalake_dsn = os.getenv("DATALAKE_DB_URL")
    # Lock só serializa este worker contra si mesmo; CONCURRENTLY precisa de
    # autocommit, então cada REFRESH roda em conexão autocommit própria.
    with connect(dsn) as guard:
        with advisory_lock(guard, LOCK_MATVIEW_REFRESH) as got:
            if not got:
                return {"refreshed": [], "refreshed_datalake": [], "skipped": "lock_busy"}
            refreshed = _refresh_all(dsn, _APP_MVS)
            refreshed.extend(_refresh_bootstrap_mvs(dsn, _APP_BOOTSTRAP_MVS))
            snapshot_stats = market_overview_snapshot.run(dsn)
            if snapshot_stats.get("published") != 1:
                raise RuntimeError("market overview snapshot did not publish")
            refreshed_datalake: list[str] = []
            effective_matviews: list[dict] = []
            if datalake_dsn:
                refreshed_datalake = _refresh_all(datalake_dsn, _DATALAKE_MVS)
                effective_matviews = sec_effective_matviews.refresh_stale(datalake_dsn)
            return {
                "refreshed": refreshed,
                "market_overview_snapshot": snapshot_stats,
                "refreshed_datalake": refreshed_datalake,
                "effective_matviews": effective_matviews,
            }
