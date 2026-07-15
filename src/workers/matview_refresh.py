"""Refresh Light read models and publish the complete Stocks overview.

The locked run refreshes app MVs first, publishes market_overview_snapshot from
those fresh inputs, then refreshes the datalake relationship MVs. MV refreshes
use autocommit because REFRESH ... CONCURRENTLY cannot run in a transaction and
require the UNIQUE indexes versioned in the Light DDLs.
"""
from __future__ import annotations

import os

from src.db import LOCK_MATVIEW_REFRESH, advisory_lock, connect
from src.workers import market_overview_snapshot

_APP_MVS = [
    "price_latest_mv",
    "nav_latest_mv",
    # Grupo A: read-models de fund analytics agregados (style-drift/top-holdings).
    # Active-share deixou de ser MV standalone — vive em colunas de
    # fund_risk_metrics, projetadas em fund_risk_latest_mv (refrescada pelo worker
    # risk_metrics). As *_latest_mv de fatores/reveal são refrescadas pelos seus
    # próprios workers (fund_factors / fund_institutional_reveal), não aqui.
    "fund_style_drift_mv",
    "fund_top_holdings_mv",
]
_DATALAKE_MVS = [
    "stock_institutional_holders_mv",
    "stock_fund_holders_mv",
    "holding_reverse_lookup_mv",
]


def _refresh_all(dsn: str, mvs: list[str]) -> list[str]:
    refreshed: list[str] = []
    with connect(dsn, autocommit=True) as conn:
        for mv in mvs:
            with conn.cursor() as cur:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
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
            snapshot_stats = market_overview_snapshot.run(dsn)
            refreshed_datalake: list[str] = []
            if datalake_dsn:
                refreshed_datalake = _refresh_all(datalake_dsn, _DATALAKE_MVS)
            return {
                "refreshed": refreshed,
                "market_overview_snapshot": snapshot_stats,
                "refreshed_datalake": refreshed_datalake,
            }
