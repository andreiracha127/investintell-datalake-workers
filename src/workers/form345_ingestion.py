"""form345_ingestion worker — SEC Form 3/4/5 structured-data ZIP.

P7 product capability ``insider_ingestion`` maps to this worker name because
the SEC bulk source is published as Form 345 structured data. Unit tests use
tiny local ZIP fixtures; live network is only used by ``run()``.
"""

from __future__ import annotations

import csv
import datetime as _dt
import io
import os
import re
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from src.db import LOCK_FORM345_INGESTION, advisory_lock, connect

BASE_URL = "https://www.sec.gov/files/structureddata/data/form-345"
UPSERT_CHUNK = 2000


@dataclass(frozen=True)
class InsiderTransaction:
    accession_number: str
    trans_sk: str
    issuer_cik: str
    issuer_name: str | None
    reporting_owner_cik: str | None
    reporting_owner_name: str | None
    transaction_date: _dt.date
    transaction_code: str | None
    shares: Decimal | None
    value_usd: Decimal | None
    buy_sell: str
    source_period: str


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _normalize_cik(value: str | int | None) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    return (digits.lstrip("0") or "0") if digits else None


def _decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    text = value.replace(",", "").strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _date(value: str | None) -> _dt.date | None:
    if not value:
        return None
    try:
        return _dt.date.fromisoformat(value[:10])
    except ValueError:
        return None


def _first(row: dict[str, str], *keys: str) -> str | None:
    for key in keys:
        value = row.get(_normalize_key(key))
        if value is not None and value.strip() != "":
            return value.strip()
    return None


def _read_tsv(zf: zipfile.ZipFile, suffix: str) -> list[dict[str, str]]:
    member = next(
        (
            name
            for name in zf.namelist()
            if name.upper().endswith(suffix.upper())
        ),
        None,
    )
    if member is None:
        return []
    raw = zf.read(member).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(raw), delimiter="\t")
    rows: list[dict[str, str]] = []
    for row in reader:
        rows.append({_normalize_key(k): v for k, v in row.items() if k is not None})
    return rows


def classify_transaction(
    transaction_code: str | None,
    acquired_disposed_code: str | None = None,
) -> str:
    code = (transaction_code or "").strip().upper()
    side = (acquired_disposed_code or "").strip().upper()
    if code == "P" or (side == "A" and code in {"", "P"}):
        return "buy"
    if code == "S" or (side == "D" and code in {"", "S"}):
        return "sell"
    return "other"


def quarter_start(value: _dt.date) -> _dt.date:
    month = ((value.month - 1) // 3) * 3 + 1
    return _dt.date(value.year, month, 1)


def parse_form345_zip(payload: bytes, *, source_period: str) -> list[InsiderTransaction]:
    """Parse SUBMISSION, REPORTINGOWNER, and NONDERIV_TRANS rows from a ZIP."""
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        submissions = _read_tsv(zf, "SUBMISSION.tsv")
        owners = _read_tsv(zf, "REPORTINGOWNER.tsv")
        transactions = _read_tsv(zf, "NONDERIV_TRANS.tsv")

    submission_by_accession = {
        _first(row, "ACCESSION_NUMBER", "ACCESSIONNO"): row for row in submissions
    }
    owner_by_accession: dict[str, dict[str, str]] = {}
    for row in owners:
        accession = _first(row, "ACCESSION_NUMBER", "ACCESSIONNO")
        if accession and accession not in owner_by_accession:
            owner_by_accession[accession] = row

    parsed: list[InsiderTransaction] = []
    for index, row in enumerate(transactions, start=1):
        accession = _first(row, "ACCESSION_NUMBER", "ACCESSIONNO")
        if not accession:
            continue
        submission = submission_by_accession.get(accession, {})
        owner = owner_by_accession.get(accession, {})
        issuer_cik = _normalize_cik(
            _first(submission, "ISSUERCIK", "ISSUER_CIK", "CIK")
        )
        trans_date = _date(_first(row, "TRANSACTION_DATE", "TRANS_DATE"))
        if issuer_cik is None or trans_date is None:
            continue

        shares = _decimal(_first(row, "TRANSACTION_SHARES", "SHARES"))
        price = _decimal(
            _first(row, "TRANSACTION_PRICEPERSHARE", "PRICE_PER_SHARE", "PRICE")
        )
        explicit_value = _decimal(_first(row, "TRANSACTION_VALUE", "VALUE"))
        value_usd = explicit_value
        if value_usd is None and shares is not None and price is not None:
            value_usd = shares * price
        code = _first(row, "TRANSACTION_CODE", "TRANS_CODE")
        buy_sell = classify_transaction(
            code, _first(row, "TRANSACTION_ACQUIRED_DISPOSED_CODE", "ACQUIRED_DISPOSED_CODE")
        )
        parsed.append(
            InsiderTransaction(
                accession_number=accession,
                trans_sk=_first(row, "TRANS_SK", "TRANSACTION_SK") or str(index),
                issuer_cik=issuer_cik,
                issuer_name=_first(submission, "ISSUERNAME", "ISSUER_NAME"),
                reporting_owner_cik=_normalize_cik(
                    _first(owner, "RPTOWNERCIK", "REPORTING_OWNER_CIK", "OWNERCIK")
                ),
                reporting_owner_name=_first(
                    owner, "RPTOWNERNAME", "REPORTING_OWNER_NAME", "OWNERNAME"
                ),
                transaction_date=trans_date,
                transaction_code=code,
                shares=shares,
                value_usd=value_usd,
                buy_sell=buy_sell,
                source_period=source_period,
            )
        )
    return parsed


def aggregate_sentiment(
    rows: list[InsiderTransaction],
) -> dict[tuple[str, _dt.date], dict[str, Any]]:
    out: dict[tuple[str, _dt.date], dict[str, Any]] = {}
    for row in rows:
        if row.buy_sell not in {"buy", "sell"}:
            continue
        key = (row.issuer_cik, quarter_start(row.transaction_date))
        bucket = out.setdefault(
            key,
            {
                "buy_value": Decimal(0),
                "sell_value": Decimal(0),
                "buy_count": 0,
                "sell_count": 0,
            },
        )
        value = row.value_usd or Decimal(0)
        if row.buy_sell == "buy":
            bucket["buy_value"] += value
            bucket["buy_count"] += 1
        else:
            bucket["sell_value"] += value
            bucket["sell_count"] += 1
    for bucket in out.values():
        bucket["net_value"] = bucket["buy_value"] - bucket["sell_value"]
    return out


def ensure_schema(conn) -> None:
    sql_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "schemas",
        "form345_ingestion.sql",
    )
    with open(sql_path, encoding="utf-8") as fh:
        ddl = fh.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def _headers() -> dict[str, str]:
    user_agent = os.getenv("SEC_USER_AGENT") or os.getenv("EDGAR_IDENTITY")
    if not user_agent:
        raise RuntimeError("SEC_USER_AGENT or EDGAR_IDENTITY is required for SEC requests")
    return {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}


def _source_period(calc_date: str | None) -> tuple[int, int]:
    as_of = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()
    return as_of.year, ((as_of.month - 1) // 3) + 1


def fetch_form345_zip(year: int, quarter: int) -> bytes:
    url = f"{BASE_URL}/{year}q{quarter}_form345.zip"
    with httpx.Client(timeout=120.0, headers=_headers()) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.content


def upsert_transactions(conn, rows: list[InsiderTransaction]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO sec_insider_transactions
            (accession_number, trans_sk, issuer_cik, issuer_name,
             reporting_owner_cik, reporting_owner_name, transaction_date,
             transaction_code, shares, value_usd, buy_sell, source_period,
             updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (accession_number, trans_sk) DO UPDATE SET
            issuer_cik = EXCLUDED.issuer_cik,
            issuer_name = EXCLUDED.issuer_name,
            reporting_owner_cik = EXCLUDED.reporting_owner_cik,
            reporting_owner_name = EXCLUDED.reporting_owner_name,
            transaction_date = EXCLUDED.transaction_date,
            transaction_code = EXCLUDED.transaction_code,
            shares = EXCLUDED.shares,
            value_usd = EXCLUDED.value_usd,
            buy_sell = EXCLUDED.buy_sell,
            source_period = EXCLUDED.source_period,
            updated_at = now()
    """
    payload = [
        (
            row.accession_number,
            row.trans_sk,
            row.issuer_cik,
            row.issuer_name,
            row.reporting_owner_cik,
            row.reporting_owner_name,
            row.transaction_date,
            row.transaction_code,
            row.shares,
            row.value_usd,
            row.buy_sell,
            row.source_period,
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        for start in range(0, len(payload), UPSERT_CHUNK):
            cur.executemany(sql, payload[start:start + UPSERT_CHUNK])
    return len(rows)


def refresh_sentiment(conn, rows: list[InsiderTransaction]) -> int:
    keys = sorted(
        {
            (row.issuer_cik, quarter_start(row.transaction_date))
            for row in rows
            if row.buy_sell in {"buy", "sell"}
        }
    )
    if not keys:
        return 0
    with conn.cursor() as cur:
        for cik, quarter in keys:
            quarter_end = (
                quarter + _dt.timedelta(days=95)
            ).replace(day=1) - _dt.timedelta(days=1)
            cur.execute(
                """
                INSERT INTO sec_insider_sentiment
                    (cik, quarter, buy_value, sell_value, buy_count, sell_count,
                     net_value, updated_at)
                SELECT
                    issuer_cik,
                    %s::date,
                    COALESCE(SUM(value_usd) FILTER (WHERE buy_sell = 'buy'), 0),
                    COALESCE(SUM(value_usd) FILTER (WHERE buy_sell = 'sell'), 0),
                    COUNT(*) FILTER (WHERE buy_sell = 'buy'),
                    COUNT(*) FILTER (WHERE buy_sell = 'sell'),
                    COALESCE(SUM(value_usd) FILTER (WHERE buy_sell = 'buy'), 0)
                    - COALESCE(SUM(value_usd) FILTER (WHERE buy_sell = 'sell'), 0),
                    now()
                FROM sec_insider_transactions
                WHERE issuer_cik = %s
                  AND transaction_date >= %s
                  AND transaction_date <= %s
                  AND buy_sell IN ('buy', 'sell')
                GROUP BY issuer_cik
                ON CONFLICT (cik, quarter) DO UPDATE SET
                    buy_value = EXCLUDED.buy_value,
                    sell_value = EXCLUDED.sell_value,
                    buy_count = EXCLUDED.buy_count,
                    sell_count = EXCLUDED.sell_count,
                    net_value = EXCLUDED.net_value,
                    updated_at = now()
                """,
                (quarter, cik, quarter, quarter_end),
            )
    return len(keys)


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Fetch one quarterly SEC Form 345 ZIP and upsert raw + aggregate rows."""
    year, quarter = _source_period(calc_date)
    source_period = f"{year}q{quarter}"
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_FORM345_INGESTION) as got:
            if not got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}
            ensure_schema(conn)
            payload = fetch_form345_zip(year, quarter)
            rows = parse_form345_zip(payload, source_period=source_period)
            if limit:
                rows = rows[:limit]
            upserted = upsert_transactions(conn, rows)
            sentiment_rows = refresh_sentiment(conn, rows)
            conn.commit()
    return {
        "fetched": len(rows),
        "upserted": upserted,
        "sentiment_rows": sentiment_rows,
        "source_period": source_period,
    }
