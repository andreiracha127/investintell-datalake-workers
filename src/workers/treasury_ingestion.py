"""treasury_ingestion worker — US Treasury Fiscal Data API → treasury_data.

Standalone reimplementation of the monolith ``treasury_ingestion`` worker
(reference: ``app/jobs/workers/treasury_ingestion.py`` + ``data_providers/
treasury``). Pulls five Fiscal Data endpoints (average interest rates, debt to
the penny, auctions, exchange rates, interest expense), maps each record to a
prefixed series_id (RATE_/DEBT_/AUCTION_/FX_/INTEREST_, ≤80 chars), and upserts
into ``treasury_data`` with conflict key (obs_date, series_id). Auctions carry
``metadata_json`` (security_type, security_term, bid_to_cover).

Faithful to the monolith: 365-day lookback, no watermark (window re-fetched,
idempotent via ON CONFLICT DO UPDATE), batch deduped by PK before INSERT,
pagination via page[size]=10000 + meta.total-pages, 5 req/s token bucket,
missing-value markers parsed to None and rows without a value dropped.

Contract:  run(dsn, *, calc_date=None, limit=None) -> {"fetched", "upserted", ...}
``limit`` caps rows per endpoint (smoke runs). No API key required.
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import threading
import time
from typing import Any

from src.db import LOCK_TREASURY_INGESTION, advisory_lock, connect

BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
LOOKBACK_DAYS = 365
PAGE_SIZE = 10000
UPSERT_CHUNK = 2000
_MISSING_VALUES = frozenset((".", "#N/A", "", "NaN", "nan", "null", "None"))

# endpoint path, date field, fields requested
ENDPOINTS = {
    "rates": ("/v2/accounting/od/avg_interest_rates", "record_date",
              "record_date,security_desc,avg_interest_rate_amt"),
    "debt": ("/v2/accounting/od/debt_to_penny", "record_date",
             "record_date,tot_pub_debt_out_amt,intragov_hold_amt,debt_held_public_amt"),
    "auctions": ("/v1/accounting/od/auctions_query", "auction_date",
                 "auction_date,security_type,security_term,high_yield,bid_to_cover_ratio"),
    "fx": ("/v1/accounting/od/rates_of_exchange", "record_date",
           "country_currency_desc,exchange_rate,record_date"),
    "interest": ("/v2/accounting/od/interest_expense", "record_date",
                 "record_date,expense_catg_desc,month_expense_amt,fytd_expense_amt"),
}


class TokenBucket:
    """Thread-safe token bucket — Fiscal Data is polled at ≤5 req/s."""

    def __init__(self, max_tokens: float = 5.0, refill_rate: float = 5.0) -> None:
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self._tokens = max_tokens
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self.max_tokens, self._tokens + (now - self._last) * self.refill_rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.refill_rate
            time.sleep(wait)


# ──────────────────────────────────────────────────────────────────────────────
# Pure transforms
# ──────────────────────────────────────────────────────────────────────────────
def parse_float(raw: Any) -> float | None:
    """Fiscal Data numeric field → float; missing markers / non-finite → None."""
    if raw is None:
        return None
    s = str(raw).strip()
    if s in _MISSING_VALUES:
        return None
    try:
        val = float(s.replace(",", ""))
    except (ValueError, TypeError):
        return None
    return val if math.isfinite(val) else None


def _series_id(prefix: str, label: str) -> str:
    sid = f"{prefix}{label}".upper().replace(" ", "_").replace("-", "_") \
        if prefix == "FX_" else f"{prefix}{label}".upper().replace(" ", "_")
    return sid[:80]


def _row(obs_date: str, series_id: str, value: float,
         metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "obs_date": _dt.date.fromisoformat(obs_date),
        "series_id": series_id,
        "value": value,
        "source": "treasury_api",
        "metadata_json": metadata,
    }


def rows_from_rates(records: list[dict]) -> list[dict[str, Any]]:
    rows = []
    for rec in records:
        v = parse_float(rec.get("avg_interest_rate_amt"))
        if v is None or not rec.get("record_date") or not rec.get("security_desc"):
            continue
        rows.append(_row(rec["record_date"], _series_id("RATE_", rec["security_desc"]), v))
    return rows


def rows_from_debt(records: list[dict]) -> list[dict[str, Any]]:
    fields = {
        "tot_pub_debt_out_amt": "DEBT_TOTAL_PUBLIC",
        "intragov_hold_amt": "DEBT_INTRAGOV",
        "debt_held_public_amt": "DEBT_HELD_PUBLIC",
    }
    rows = []
    for rec in records:
        if not rec.get("record_date"):
            continue
        for field, sid in fields.items():
            v = parse_float(rec.get(field))
            if v is not None:
                rows.append(_row(rec["record_date"], sid, v))
    return rows


def rows_from_auctions(records: list[dict]) -> list[dict[str, Any]]:
    rows = []
    for rec in records:
        v = parse_float(rec.get("high_yield"))
        if v is None or not rec.get("auction_date"):
            continue
        stype, sterm = rec.get("security_type", ""), rec.get("security_term", "")
        meta: dict[str, Any] = {"security_type": stype, "security_term": sterm}
        btc = parse_float(rec.get("bid_to_cover_ratio"))
        if btc is not None:
            meta["bid_to_cover"] = btc
        rows.append(_row(rec["auction_date"],
                         _series_id("AUCTION_", f"{stype}_{sterm}"), v, meta))
    return rows


def rows_from_fx(records: list[dict]) -> list[dict[str, Any]]:
    rows = []
    for rec in records:
        v = parse_float(rec.get("exchange_rate"))
        if v is None or not rec.get("record_date") or not rec.get("country_currency_desc"):
            continue
        rows.append(_row(rec["record_date"],
                         _series_id("FX_", rec["country_currency_desc"]), v))
    return rows


def rows_from_interest(records: list[dict]) -> list[dict[str, Any]]:
    rows = []
    for rec in records:
        if not rec.get("record_date") or not rec.get("expense_catg_desc"):
            continue
        catg = rec["expense_catg_desc"]
        for field, suffix in (("month_expense_amt", "MONTH"), ("fytd_expense_amt", "FYTD")):
            v = parse_float(rec.get(field))
            if v is not None:
                rows.append(_row(rec["record_date"],
                                 _series_id("INTEREST_", f"{catg}_{suffix}"), v))
    return rows


_ROW_BUILDERS = {
    "rates": rows_from_rates,
    "debt": rows_from_debt,
    "auctions": rows_from_auctions,
    "fx": rows_from_fx,
    "interest": rows_from_interest,
}


def dedup_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Dedup by (obs_date, series_id), keeping the last value seen."""
    seen: dict[tuple[_dt.date, str], dict[str, Any]] = {}
    for r in rows:
        seen[(r["obs_date"], r["series_id"])] = r
    return list(seen.values())


# ──────────────────────────────────────────────────────────────────────────────
# Fetch (paginated, rate-limited)
# ──────────────────────────────────────────────────────────────────────────────
def _fetch_endpoint(client, key: str, start_date: str, bucket: TokenBucket,
                    limit: int | None = None) -> list[dict]:
    path, date_field, fields = ENDPOINTS[key]
    records: list[dict] = []
    page = 1
    while True:
        params = {
            "fields": fields,
            "filter": f"{date_field}:gte:{start_date}",
            "sort": f"-{date_field}",
            "page[size]": PAGE_SIZE,
            "page[number]": page,
            "format": "json",
        }
        payload = None
        for attempt in range(3):
            bucket.acquire()
            resp = client.get(f"{BASE_URL}{path}", params=params)
            if resp.status_code in (429, 503) or resp.status_code >= 500:
                time.sleep(min(30.0, 2.0 * (2 ** attempt)))
                continue
            if resp.status_code >= 400:  # 4xx: return what we have, don't fail run
                return records
            payload = resp.json()
            break
        if payload is None:
            return records
        records.extend(payload.get("data", []))
        if limit and len(records) >= limit:
            return records[:limit]
        total_pages = int(payload.get("meta", {}).get("total-pages", 1) or 1)
        if page >= total_pages:
            return records
        page += 1


def fetch_all(start_date: str, limit: int | None = None) -> dict[str, list[dict]]:
    """Fetch the 5 endpoints concurrently (shared 5 req/s bucket)."""
    import concurrent.futures

    import httpx

    bucket = TokenBucket()
    out: dict[str, list[dict]] = {}
    with httpx.Client(timeout=60.0) as client:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futures = {
                pool.submit(_fetch_endpoint, client, key, start_date, bucket, limit): key
                for key in ENDPOINTS
            }
            for fut in concurrent.futures.as_completed(futures):
                out[futures[fut]] = fut.result()
    return out


# ──────────────────────────────────────────────────────────────────────────────
# DB I/O
# ──────────────────────────────────────────────────────────────────────────────
def upsert_treasury_data(conn, rows: list[dict[str, Any]]) -> int:
    """Chunked idempotent upsert into treasury_data. Caller commits."""
    upserted = 0
    sql = """
        INSERT INTO treasury_data (obs_date, series_id, value, source, metadata_json)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (obs_date, series_id) DO UPDATE SET
            value = EXCLUDED.value,
            source = EXCLUDED.source,
            metadata_json = EXCLUDED.metadata_json
    """
    with conn.cursor() as cur:
        for i in range(0, len(rows), UPSERT_CHUNK):
            chunk = rows[i:i + UPSERT_CHUNK]
            cur.executemany(sql, [
                (r["obs_date"], r["series_id"], r["value"], r["source"],
                 json.dumps(r["metadata_json"]) if r["metadata_json"] is not None else None)
                for r in chunk
            ])
            upserted += len(chunk)
    return upserted


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Ingest the 5 Fiscal Data endpoints and upsert. Returns stats."""
    as_of = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()
    start_date = (as_of - _dt.timedelta(days=LOOKBACK_DAYS)).isoformat()

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_TREASURY_INGESTION) as got:
            if not got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}

            raw = fetch_all(start_date, limit)
            rows: list[dict[str, Any]] = []
            for key, records in raw.items():
                rows.extend(_ROW_BUILDERS[key](records))
            rows = dedup_rows(rows)
            upserted = upsert_treasury_data(conn, rows)
            conn.commit()

    return {
        "fetched": sum(len(v) for v in raw.values()),
        "upserted": upserted,
        "start_date": start_date,
        "per_endpoint": {k: len(v) for k, v in raw.items()},
    }
