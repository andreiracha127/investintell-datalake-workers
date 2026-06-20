"""sec_company_tickers_mf worker — SEC fund ticker <-> series crosswalk.

Ingests https://www.sec.gov/files/company_tickers_mf.json, the SEC's
authoritative map of ``(cik, seriesId, classId, symbol)`` for every registered
fund share class (mutual funds AND ETFs). This supplies the ticker -> series_id
edge the look-through fund-of-fund resolver was missing: a held ETF's CUSIP
resolves to a ticker via ``sec_cusip_ticker_map``, and this table turns that
ticker into the child ``series_id`` — so funds present in N-PORT but absent
from the N-CEN-derived ``sec_etfs`` catalog (e.g. WisdomTree's DTD/DEM/DXJ)
become expandable.

The file is a compact columnar JSON: ``{"fields": [...], "data": [[...], ...]}``.
Unit tests parse an in-memory fixture; live network is only used by ``run()``.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

import httpx

from src.db import LOCK_SEC_COMPANY_TICKERS_MF, advisory_lock, connect

URL = "https://www.sec.gov/files/company_tickers_mf.json"
UPSERT_CHUNK = 2000


@dataclass(frozen=True)
class FundTicker:
    cik: str
    series_id: str
    class_id: str
    ticker: str


def _headers() -> dict[str, str]:
    user_agent = os.getenv("SEC_USER_AGENT") or os.getenv("EDGAR_IDENTITY")
    if not user_agent:
        raise RuntimeError("SEC_USER_AGENT or EDGAR_IDENTITY is required for SEC requests")
    return {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}


def parse_company_tickers_mf(payload: dict[str, Any]) -> list[FundTicker]:
    """Parse the columnar ``{"fields": [...], "data": [[...]]}`` SEC payload.

    Fields are resolved by name (case-insensitive), never by position. Rows
    without a series id or ticker are dropped; ``class_id`` (globally unique)
    dedups. CIK is zero-stripped to match the registrant/N-PORT conventions.
    """
    fields = [str(f).lower() for f in payload.get("fields", [])]
    try:
        i_cik = fields.index("cik")
        i_series = fields.index("seriesid")
        i_class = fields.index("classid")
        i_symbol = fields.index("symbol")
    except ValueError:
        return []

    need = max(i_cik, i_series, i_class, i_symbol)
    out: list[FundTicker] = []
    seen: set[str] = set()
    for row in payload.get("data", []):
        if len(row) <= need:
            continue
        series_id = str(row[i_series] or "").strip()
        ticker = str(row[i_symbol] or "").strip().upper()
        if not series_id or not ticker:
            continue
        class_id = str(row[i_class] or "").strip()
        cik = str(row[i_cik] or "").strip().lstrip("0") or "0"
        key = class_id or f"{series_id}:{ticker}"
        if key in seen:
            continue
        seen.add(key)
        out.append(
            FundTicker(cik=cik, series_id=series_id, class_id=key, ticker=ticker)
        )
    return out


def ensure_schema(conn) -> None:
    sql_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "schemas",
        "sec_company_tickers_mf.sql",
    )
    with open(sql_path, encoding="utf-8") as fh:
        ddl = fh.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def fetch_company_tickers_mf() -> dict[str, Any]:
    with httpx.Client(timeout=60.0, headers=_headers()) as client:
        resp = client.get(URL)
        resp.raise_for_status()
        return resp.json()


def upsert_tickers(conn, rows: list[FundTicker]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO sec_company_tickers_mf
            (class_id, cik, series_id, ticker, updated_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (class_id) DO UPDATE SET
            cik = EXCLUDED.cik,
            series_id = EXCLUDED.series_id,
            ticker = EXCLUDED.ticker,
            updated_at = now()
    """
    payload = [(r.class_id, r.cik, r.series_id, r.ticker) for r in rows]
    with conn.cursor() as cur:
        for start in range(0, len(payload), UPSERT_CHUNK):
            cur.executemany(sql, payload[start:start + UPSERT_CHUNK])
    return len(rows)


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Fetch the SEC mutual-fund/ETF ticker map and upsert it (full refresh)."""
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_SEC_COMPANY_TICKERS_MF) as got:
            if not got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}
            ensure_schema(conn)
            payload = fetch_company_tickers_mf()
            rows = parse_company_tickers_mf(payload)
            if limit:
                rows = rows[:limit]
            upserted = upsert_tickers(conn, rows)
            conn.commit()
    return {"fetched": len(rows), "upserted": upserted}
