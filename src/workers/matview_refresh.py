"""Refresca os read-model MVs do Light.

App DB (price_latest_mv / nav_latest_mv, Grupo D) e datalake DB
(stock_institutional_holders_mv / stock_fund_holders_mv /
holding_reverse_lookup_mv, Grupo B). Nenhum tem worker computacional próprio;
este worker apenas dá REFRESH ... CONCURRENTLY em cada um, num cron, em conexão
autocommit (CONCURRENTLY não roda em bloco de transação) e exige os índices
UNIQUE definidos nos DDLs em backend/db/ddl/. O advisory lock evita refreshes
concorrentes do mesmo conjunto entre execuções.
"""
from __future__ import annotations

import os

from src.db import LOCK_MATVIEW_REFRESH, advisory_lock, connect

_APP_MVS = ["price_latest_mv", "nav_latest_mv"]
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
            refreshed_datalake: list[str] = []
            if datalake_dsn:
                refreshed_datalake = _refresh_all(datalake_dsn, _DATALAKE_MVS)
            return {"refreshed": refreshed, "refreshed_datalake": refreshed_datalake}
