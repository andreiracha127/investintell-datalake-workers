"""Refresca os read-model MVs de preço/NAV do Light no DB principal.

price_latest_mv / nav_latest_mv não têm worker computacional próprio
(eod_prices é populado pelo backfill/warming worker out-of-band;
nav_timeseries pelo instrument_ingestion). Este worker dedicado dá
REFRESH … CONCURRENTLY em ambos num cron, em conexão autocommit
(CONCURRENTLY não roda em bloco de transação) e exige os índices UNIQUE
definidos em backend/db/ddl/2026-06-21_price_nav_latest_mv.sql.
O advisory lock evita refreshes concorrentes do mesmo MV entre execuções.
"""
from __future__ import annotations

from src.db import LOCK_MATVIEW_REFRESH, advisory_lock, connect

_MVS = ["price_latest_mv", "nav_latest_mv"]


def run(dsn: str) -> dict:
    # Lock só serializa este worker contra si mesmo; CONCURRENTLY precisa de
    # autocommit, então cada REFRESH roda em conexão autocommit própria.
    with connect(dsn) as guard:
        with advisory_lock(guard, LOCK_MATVIEW_REFRESH) as got:
            if not got:
                return {"refreshed": [], "skipped": "lock_busy"}
            refreshed: list[str] = []
            with connect(dsn, autocommit=True) as conn:
                for mv in _MVS:
                    with conn.cursor() as cur:
                        cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
                    refreshed.append(mv)
            return {"refreshed": refreshed}
