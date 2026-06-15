"""sec_13f_ingestion worker — SEC 13F-HR information tables.

Reads a bounded manager universe from ``sec_managers``/``curated_institutions``
and fetches recent 13F-HR information tables from SEC EDGAR. The ADV pipeline is
the intended long-term source of ``sec_managers``; until it exists, operators can
seed a small explicit universe with ``SEC_13F_CIKS``:

    SEC_13F_CIKS="0001067983:Berkshire Hathaway;0000102909:Manager Name"

No manager rows means no synthetic data: the worker returns
``{"skipped": "missing_sec_managers"}``.
"""

from __future__ import annotations

import datetime as _dt
import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from src.db import LOCK_SEC_13F_INGESTION, advisory_lock, connect

SEC_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
SEC_SUBMISSIONS = "https://data.sec.gov/submissions"
DEFAULT_FILINGS_PER_MANAGER = 1
UPSERT_CHUNK = 1000


@dataclass(frozen=True)
class Manager:
    cik: str
    manager_name: str


@dataclass(frozen=True)
class FilingRef:
    cik: str
    accession_number: str
    form_type: str
    report_date: _dt.date
    filing_date: _dt.date


@dataclass(frozen=True)
class Holding13F:
    cik: str
    manager_name: str
    period: _dt.date
    report_date: _dt.date
    cusip: str
    name: str | None
    value_usd: Decimal | None
    shares: Decimal | None
    accession_number: str
    form_type: str
    source_url: str


def _normalize_cik(value: str | int) -> str:
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        raise ValueError("CIK must contain digits")
    return digits.lstrip("0") or "0"


def _padded_cik(cik: str) -> str:
    return _normalize_cik(cik).zfill(10)


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _child_text(node: ET.Element, path: tuple[str, ...]) -> str | None:
    current = node
    for part in path:
        match = next((child for child in current if _local(child.tag) == part), None)
        if match is None:
            return None
        current = match
    text = current.text.strip() if current.text else ""
    return text or None


def _decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _date(value: str | None) -> _dt.date | None:
    if not value:
        return None
    try:
        return _dt.date.fromisoformat(value[:10])
    except ValueError:
        return None


def parse_information_table_xml(
    xml_text: str,
    *,
    cik: str,
    manager_name: str,
    accession_number: str,
    report_date: _dt.date,
    form_type: str,
    source_url: str,
) -> list[Holding13F]:
    """Parse one SEC 13F information table XML document.

    SEC stores ``value`` in thousands of dollars; the worker persists USD.
    Namespace and casing drift are tolerated, but rows without CUSIP are
    skipped because the Light Tier C joins depend on CUSIP.
    """
    start = xml_text.find("<informationTable")
    if start > 0:
        xml_text = xml_text[start:]
    root = ET.fromstring(xml_text)
    rows: list[Holding13F] = []
    for node in root.iter():
        if _local(node.tag) != "infotable":
            continue
        cusip = (_child_text(node, ("cusip",)) or "").strip().upper()
        if not cusip:
            continue
        value = _decimal(_child_text(node, ("value",)))
        shares = _decimal(_child_text(node, ("shrsorprnamt", "sshprnamt")))
        rows.append(
            Holding13F(
                cik=_normalize_cik(cik),
                manager_name=manager_name,
                period=report_date,
                report_date=report_date,
                cusip=cusip,
                name=_child_text(node, ("nameofissuer",)),
                value_usd=value * Decimal(1000) if value is not None else None,
                shares=shares,
                accession_number=accession_number,
                form_type=form_type,
                source_url=source_url,
            )
        )
    return rows


def _headers() -> dict[str, str]:
    user_agent = os.getenv("SEC_USER_AGENT") or os.getenv("EDGAR_IDENTITY")
    if not user_agent:
        raise RuntimeError("SEC_USER_AGENT or EDGAR_IDENTITY is required for SEC requests")
    return {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}


def _latest_filings(
    client: httpx.Client,
    cik: str,
    *,
    calc_date: _dt.date | None,
    limit: int,
) -> list[FilingRef]:
    resp = client.get(f"{SEC_SUBMISSIONS}/CIK{_padded_cik(cik)}.json")
    resp.raise_for_status()
    recent = resp.json().get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])

    refs: list[FilingRef] = []
    for idx, form_type in enumerate(forms):
        if form_type not in {"13F-HR", "13F-HR/A"}:
            continue
        accession = accessions[idx]
        filing_date = _date(filing_dates[idx]) if idx < len(filing_dates) else None
        report_date = _date(report_dates[idx]) if idx < len(report_dates) else None
        if filing_date is None:
            continue
        period = report_date or filing_date
        if calc_date is not None and period > calc_date:
            continue
        refs.append(
            FilingRef(
                cik=_normalize_cik(cik),
                accession_number=accession,
                form_type=form_type,
                report_date=period,
                filing_date=filing_date,
            )
        )
        if len(refs) >= limit:
            break
    return refs


def _information_table_url(client: httpx.Client, filing: FilingRef) -> str | None:
    accession_path = filing.accession_number.replace("-", "")
    index_url = f"{SEC_ARCHIVES}/{_normalize_cik(filing.cik)}/{accession_path}/index.json"
    resp = client.get(index_url)
    resp.raise_for_status()
    items = resp.json().get("directory", {}).get("item", [])
    xml_names = [
        item.get("name", "")
        for item in items
        if str(item.get("name", "")).lower().endswith(".xml")
    ]
    if not xml_names:
        return None
    preferred = [
        name for name in xml_names if "info" in name.lower() or "table" in name.lower()
    ]
    name = (preferred or xml_names)[0]
    return f"{SEC_ARCHIVES}/{_normalize_cik(filing.cik)}/{accession_path}/{name}"


def fetch_manager_holdings(
    client: httpx.Client,
    manager: Manager,
    *,
    calc_date: _dt.date | None = None,
    filings_per_manager: int = DEFAULT_FILINGS_PER_MANAGER,
) -> list[Holding13F]:
    holdings: list[Holding13F] = []
    for filing in _latest_filings(
        client,
        manager.cik,
        calc_date=calc_date,
        limit=filings_per_manager,
    ):
        source_url = _information_table_url(client, filing)
        if source_url is None:
            continue
        resp = client.get(source_url)
        resp.raise_for_status()
        holdings.extend(
            parse_information_table_xml(
                resp.text,
                cik=manager.cik,
                manager_name=manager.manager_name,
                accession_number=filing.accession_number,
                report_date=filing.report_date,
                form_type=filing.form_type,
                source_url=source_url,
            )
        )
    return holdings


def ensure_schema(conn) -> None:
    sql_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "schemas",
        "sec_13f_ingestion.sql",
    )
    with open(sql_path, encoding="utf-8") as fh:
        ddl = fh.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def _seed_managers_from_env(conn) -> int:
    raw = os.getenv("SEC_13F_CIKS", "").strip()
    if not raw:
        return 0
    rows: list[tuple[str, str]] = []
    for item in re.split(r"[;\n]+", raw):
        if not item.strip():
            continue
        cik, _, name = item.partition(":")
        rows.append((_normalize_cik(cik), name.strip() or _normalize_cik(cik)))
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO sec_managers (cik, manager_name, source, updated_at)
            VALUES (%s, %s, 'sec_13f_env_seed', now())
            ON CONFLICT (cik) DO UPDATE SET
                manager_name = EXCLUDED.manager_name,
                updated_at = now()
            """,
            rows,
        )
        cur.executemany(
            """
            INSERT INTO curated_institutions (cik, manager_name, source, updated_at)
            VALUES (%s, %s, 'sec_13f_env_seed', now())
            ON CONFLICT (cik) DO UPDATE SET
                manager_name = EXCLUDED.manager_name,
                is_active = true,
                updated_at = now()
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def _list_managers(conn, limit: int | None = None) -> list[Manager]:
    sql = """
        SELECT cik, manager_name FROM (
            SELECT cik, manager_name, 0 AS sort_order
            FROM curated_institutions
            WHERE is_active
            UNION ALL
            SELECT cik, manager_name, 1 AS sort_order
            FROM sec_managers
            WHERE COALESCE(aum_total, 100000000) >= 100000000
        ) managers
        GROUP BY cik, manager_name
        ORDER BY min(sort_order), manager_name
    """
    params: list[Any] = []
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [Manager(cik=_normalize_cik(cik), manager_name=name) for cik, name in cur.fetchall()]


def upsert_holdings(conn, rows: list[Holding13F]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO sec_13f_holdings
            (cik, manager_name, period, report_date, cusip, name, value_usd,
             shares, accession_number, form_type, source_url, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (cik, period, accession_number, cusip) DO UPDATE SET
            manager_name = EXCLUDED.manager_name,
            report_date = EXCLUDED.report_date,
            name = EXCLUDED.name,
            value_usd = EXCLUDED.value_usd,
            shares = EXCLUDED.shares,
            form_type = EXCLUDED.form_type,
            source_url = EXCLUDED.source_url,
            updated_at = now()
    """
    payload = [
        (
            row.cik,
            row.manager_name,
            row.period,
            row.report_date,
            row.cusip,
            row.name,
            row.value_usd,
            row.shares,
            row.accession_number,
            row.form_type,
            row.source_url,
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        for start in range(0, len(payload), UPSERT_CHUNK):
            cur.executemany(sql, payload[start:start + UPSERT_CHUNK])
    return len(rows)


def refresh_diffs(conn, ciks: list[str]) -> int:
    """Refresh latest-vs-previous 13F diffs for processed managers."""
    refreshed = 0
    with conn.cursor() as cur:
        for cik in ciks:
            cur.execute(
                """
                WITH periods AS (
                    SELECT DISTINCT period
                    FROM sec_13f_holdings
                    WHERE cik = %s
                    ORDER BY period DESC
                    LIMIT 2
                ),
                ranked AS (
                    SELECT period, row_number() OVER (ORDER BY period DESC) AS rn
                    FROM periods
                ),
                latest AS (
                    SELECT h.*
                    FROM sec_13f_holdings h
                    JOIN ranked r ON r.period = h.period AND r.rn = 1
                    WHERE h.cik = %s
                ),
                previous AS (
                    SELECT h.*
                    FROM sec_13f_holdings h
                    JOIN ranked r ON r.period = h.period AND r.rn = 2
                    WHERE h.cik = %s
                )
                INSERT INTO sec_13f_diffs
                    (cik, manager_name, cusip, name, period, previous_period,
                     value_usd, previous_value_usd, value_change_usd,
                     shares, previous_shares, shares_change, updated_at)
                SELECT
                    l.cik, l.manager_name, l.cusip, l.name, l.period, p.period,
                    l.value_usd, p.value_usd,
                    COALESCE(l.value_usd, 0) - COALESCE(p.value_usd, 0),
                    l.shares, p.shares,
                    COALESCE(l.shares, 0) - COALESCE(p.shares, 0),
                    now()
                FROM latest l
                LEFT JOIN previous p ON p.cusip = l.cusip
                ON CONFLICT (cik, cusip, period) DO UPDATE SET
                    manager_name = EXCLUDED.manager_name,
                    name = EXCLUDED.name,
                    previous_period = EXCLUDED.previous_period,
                    value_usd = EXCLUDED.value_usd,
                    previous_value_usd = EXCLUDED.previous_value_usd,
                    value_change_usd = EXCLUDED.value_change_usd,
                    shares = EXCLUDED.shares,
                    previous_shares = EXCLUDED.previous_shares,
                    shares_change = EXCLUDED.shares_change,
                    updated_at = now()
                """,
                (cik, cik, cik),
            )
            refreshed += cur.rowcount or 0
    return refreshed


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict:
    """Ingest recent 13F information tables for a bounded manager universe."""
    cdate = _dt.date.fromisoformat(calc_date) if calc_date else None
    filings_per_manager = int(os.getenv("SEC_13F_FILINGS_PER_MANAGER", "1"))
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_SEC_13F_INGESTION) as got:
            if not got:
                return {"processed": 0, "upserted": 0, "skipped": "lock_busy"}
            ensure_schema(conn)
            seeded = _seed_managers_from_env(conn)
            managers = _list_managers(conn, limit)
            if not managers:
                return {
                    "processed": 0,
                    "upserted": 0,
                    "seeded": seeded,
                    "skipped": "missing_sec_managers",
                }

            fetched = upserted = diff_rows = 0
            processed_ciks: list[str] = []
            with httpx.Client(timeout=60.0, headers=_headers()) as client:
                for manager in managers:
                    rows = fetch_manager_holdings(
                        client,
                        manager,
                        calc_date=cdate,
                        filings_per_manager=filings_per_manager,
                    )
                    fetched += len(rows)
                    upserted += upsert_holdings(conn, rows)
                    if rows:
                        processed_ciks.append(manager.cik)
                    conn.commit()
            if processed_ciks:
                diff_rows = refresh_diffs(conn, processed_ciks)
                conn.commit()

    return {
        "processed": len(managers),
        "fetched": fetched,
        "upserted": upserted,
        "diff_rows": diff_rows,
        "seeded": seeded,
    }
