"""Ingest 13F and refresh dependent read models in a fail-closed order."""
from __future__ import annotations

import datetime as dt
from typing import Any, Callable

from src.db import LOCK_SEC_13F_PUBLICATION_CHAIN, advisory_lock, connect
from src.workers import sec_13f_ingestion

_TOTALS_CAGG = "institution_13f_totals_history_cagg"
_SECTOR_CAGG = "institution_13f_sector_history_cagg"


def _quarter_start(value: dt.date) -> dt.date:
    return dt.date(value.year, ((value.month - 1) // 3) * 3 + 1, 1)


def _next_quarter(value: dt.date) -> dt.date:
    start = _quarter_start(value)
    return dt.date(start.year + (1 if start.month == 10 else 0),
                   1 if start.month == 10 else start.month + 3, 1)


def _refresh_mv(dsn: str, name: str, *, bootstrap: bool = False) -> None:
    with connect(dsn, autocommit=True) as conn:
        populated = True
        if bootstrap:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT relispopulated FROM pg_class WHERE oid=to_regclass(%s)",
                    (f"public.{name}",),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(f"required materialized view public.{name} is missing")
                populated = bool(row[0])
        with conn.cursor() as cur:
            concurrently = "CONCURRENTLY " if populated else ""
            cur.execute(f"REFRESH MATERIALIZED VIEW {concurrently}public.{name}")


def _refresh_caggs(dsn: str, start: dt.date, end: dt.date) -> None:
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            for name in (_TOTALS_CAGG, _SECTOR_CAGG):
                cur.execute(
                    "CALL refresh_continuous_aggregate(%s::regclass, %s, %s)",
                    (f"public.{name}", start, end),
                )


def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
    ingestion_runner: Callable[..., dict[str, Any]] = sec_13f_ingestion.run,
) -> dict[str, Any]:
    """Publish source first, then holdings, reverse lookup, and history CAGGs."""
    with connect(dsn) as guard:
        with advisory_lock(guard, LOCK_SEC_13F_PUBLICATION_CHAIN) as got:
            if not got:
                return {"published": False, "stages": [], "skipped": "lock_busy"}

            ingestion = ingestion_runner(dsn, calc_date=calc_date, limit=limit)
            if ingestion.get("skipped") or ingestion.get("failed_packages"):
                raise RuntimeError(f"13F publication did not complete: {ingestion}")
            start_raw = ingestion.get("affected_report_date_start")
            end_raw = ingestion.get("affected_report_date_end")
            if not start_raw or not end_raw:
                return {
                    "published": True,
                    "stages": [{"name": "sec_13f_ingestion", "stats": ingestion}],
                    "refresh_skipped": "no_changed_filings",
                }

            start = _quarter_start(dt.date.fromisoformat(str(start_raw)))
            end = _next_quarter(dt.date.fromisoformat(str(end_raw)))
            _refresh_mv(dsn, "fund_reveal_13f_holdings_mv", bootstrap=True)
            _refresh_mv(dsn, "holding_reverse_lookup_mv")
            _refresh_caggs(dsn, start, end)
            return {
                "published": True,
                "affected_window": {"start": start, "end": end},
                "stages": [
                    {"name": "sec_13f_ingestion", "stats": ingestion},
                    {"name": "fund_reveal_13f_holdings_mv", "refreshed": True},
                    {"name": "holding_reverse_lookup_mv", "refreshed": True},
                    {"name": _TOTALS_CAGG, "refreshed": True},
                    {"name": _SECTOR_CAGG, "refreshed": True},
                ],
            }
