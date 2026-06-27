"""macro_vintage worker — point-in-time vintage ingestion for the macro quadrant.

Fetches each basket series from ALFRED (output_type=2 = all vintages in one call),
compresses to real revisions (a new row only when the value changes across vintage
dates), and upserts idempotently into macro_observation_vintage (vintages are
immutable -> ON CONFLICT DO NOTHING). Reuses the FRED TokenBucket. The latest-
revision macro_data table is untouched.
"""
from __future__ import annotations

import datetime as _dt
import math
import re
from typing import Any

_VINTAGE_COL = re.compile(r"_(\d{8})$")
_MISSING = frozenset((".", "#N/A", "", "NaN", "nan", "null", "None"))


def parse_alfred_vintages(series_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """ALFRED output_type=2 JSON -> compressed vintage rows (one per real revision).

    Columns named ``<SERIES>_YYYYMMDD`` carry the value as known on that vintage
    date; non-vintage columns (e.g. ``date``) and missing markers are skipped.
    Within each observation period, vintages are sorted by date and a row is
    emitted only when the value differs from the previous kept value.
    """
    by_period: dict[_dt.date, list[tuple[_dt.date, float]]] = {}
    for obs in payload.get("observations", []):
        try:
            period = _dt.date.fromisoformat(obs["date"])
        except (KeyError, ValueError):
            continue
        for col, raw in obs.items():
            m = _VINTAGE_COL.search(col)
            if not m:
                continue
            s = str(raw).strip()
            if s in _MISSING:
                continue
            try:
                v = float(s)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(v):
                continue
            vd = _dt.datetime.strptime(m.group(1), "%Y%m%d").date()
            by_period.setdefault(period, []).append((vd, v))

    rows: list[dict[str, Any]] = []
    for period in sorted(by_period):
        last_val: float | None = None
        rev = 0
        for vd, v in sorted(by_period[period], key=lambda t: t[0]):
            if last_val is None or v != last_val:
                rows.append({
                    "series_id": series_id, "observation_period": period,
                    "vintage_date": vd, "value": v, "revision_number": rev,
                })
                last_val = v
                rev += 1
    return rows


import os

from src.db import LOCK_MACRO_VINTAGE, advisory_lock, connect
from src.macro_sources import SEED_SOURCES, SOURCE_SPEC_VERSION
from src.workers.macro_ingestion import FRED_BASE_URL, TokenBucket

_REALTIME_ALL = {"realtime_start": "1776-07-04", "realtime_end": "9999-12-31"}
_SCHEMA = "schemas/macro_observation_vintage.sql"


class MacroVintageFetchError(RuntimeError):
    """ALFRED/FRED request failed for a mandatory macro vintage source."""


def ensure_schema(conn) -> None:
    import pathlib
    sql = pathlib.Path(_SCHEMA).read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def fetch_vintages(client, api_key: str, series_id: str, bucket: TokenBucket) -> dict:
    """ALFRED all-vintages fetch (output_type=2) for one series. Retries on 5xx/429;
    request failures fail closed because these sources are mandatory provenance."""
    import time
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json",
              "output_type": 2, **_REALTIME_ALL}
    last_retry_status: int | None = None
    for attempt in range(3):
        bucket.acquire()
        resp = client.get(f"{FRED_BASE_URL}/series/observations", params=params)
        if resp.status_code in (429, 503) or resp.status_code >= 500:
            last_retry_status = resp.status_code
            time.sleep(min(30.0, 2.0 * (2 ** attempt)))
            continue
        if resp.status_code == 400:
            raise MacroVintageFetchError(_alfred_error_message(resp, series_id))
        try:
            resp.raise_for_status()
        except Exception as exc:
            raise MacroVintageFetchError(_alfred_error_message(resp, series_id)) from exc
        return resp.json()
    raise MacroVintageFetchError(
        f"ALFRED request for {series_id} failed after retry exhaustion"
        + (f" (last_status={last_retry_status})" if last_retry_status else "")
    )


def _alfred_error_message(resp, series_id: str) -> str:
    try:
        payload = resp.json()
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        detail = payload.get("error_message") or payload.get("message")
        if detail:
            return f"ALFRED request for {series_id} failed: {detail}"
    return f"ALFRED request for {series_id} failed with status {resp.status_code}"


def rows_to_records(rows: list[dict], source_spec_version: str) -> list[tuple]:
    """Parsed rows -> DB tuples; available_at = vintage_date at 00:00 UTC."""
    out = []
    for r in rows:
        vd = r["vintage_date"]
        available_at = _dt.datetime(vd.year, vd.month, vd.day, tzinfo=_dt.timezone.utc)
        out.append((r["series_id"], r["observation_period"], vd, r["value"],
                    available_at, r["revision_number"], "alfred", source_spec_version))
    return out


def upsert_vintages(conn, records: list[tuple]) -> int:
    """Idempotent insert — vintages are immutable, so ON CONFLICT DO NOTHING."""
    if not records:
        return 0
    sql = (
        "INSERT INTO macro_observation_vintage "
        "(series_id, observation_period, vintage_date, value, available_at, "
        " revision_number, source, source_spec_version) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (series_id, observation_period, vintage_date) DO NOTHING"
    )
    with conn.cursor() as cur:
        cur.executemany(sql, records)
    conn.commit()
    return len(records)


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Backfill + refresh all basket vintages. Idempotent (DO NOTHING). Re-runs
    only add newly-published vintages. ``calc_date`` is accepted for the shared
    runner contract and ignored; ``limit`` caps series count (smoke runs)."""
    api_key = os.environ["FRED_API_KEY"]
    specs = list(SEED_SOURCES)[: limit or len(SEED_SOURCES)]
    conn = connect(dsn)
    try:
        ensure_schema(conn)
        with advisory_lock(conn, LOCK_MACRO_VINTAGE) as got:
            if not got:
                return {"status": "lock_busy"}
            import httpx
            bucket = TokenBucket()
            upserted = 0
            with httpx.Client(timeout=30.0) as client:
                for spec in specs:
                    payload = fetch_vintages(client, api_key, spec.series_id, bucket)
                    rows = parse_alfred_vintages(spec.series_id, payload)
                    upserted += upsert_vintages(conn, rows_to_records(rows, SOURCE_SPEC_VERSION))
            return {"status": "ok", "series": len(specs), "upserted": upserted}
    finally:
        conn.close()
