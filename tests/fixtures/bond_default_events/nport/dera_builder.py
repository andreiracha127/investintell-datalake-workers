"""Synthetic DERA N-PORT package builder (SYNTHETIC data, real Stage 1A header lines).

Header lines of the six pinned tables come from ``vintage_headers.json`` (read from the
sealed 2021q1 and 2026q2 packages after SHA-256 verification). Rows are synthetic.
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from src.bonds.default_events.contracts import cusip_check_digit, is_valid_isin

HERE = Path(__file__).resolve().parent
VINTAGES: dict[str, Any] = json.loads((HERE / "vintage_headers.json").read_text(encoding="utf-8"))["packages"]
SIX = ("SUBMISSION", "REGISTRANT", "FUND_REPORTED_INFO", "FUND_REPORTED_HOLDING", "IDENTIFIERS", "DEBT_SECURITY")


def make_cusip(base8: str) -> str:
    return base8 + cusip_check_digit(base8)


def make_isin(cusip9: str, country: str = "US") -> str:
    for digit in "0123456789":
        candidate = f"{country}{cusip9}{digit}"
        if is_valid_isin(candidate):
            return candidate
    raise AssertionError("no ISIN check digit")


def vintage_header(vintage: str, table: str) -> list[str]:
    return VINTAGES[f"{vintage}_nport.zip"]["headers"][f"{table}.tsv"].split("\t")


def _cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if any(ch in text for ch in ('"', "\t", "\n")):
        return '"' + text.replace('"', '""') + '"'
    return text


def tsv(header: list[str], rows: Iterable[Mapping[str, Any]]) -> bytes:
    lines = ["\t".join(header)]
    for row in rows:
        unknown = set(row) - set(header)
        if unknown:
            raise AssertionError(f"unknown columns {sorted(unknown)}")
        lines.append("\t".join(_cell(row.get(column)) for column in header))
    return ("\n".join(lines) + "\n").encode("utf-8")


def build_zip(
    path: Path,
    rows: Mapping[str, list[Mapping[str, Any]]],
    *,
    vintage: str = "2021q1",
    headers: Mapping[str, list[str]] | None = None,
    omit: Iterable[str] = (),
    raw_members: Mapping[str, bytes] | None = None,
) -> str:
    """Write a DERA-shaped ZIP and return its SHA-256."""
    omitted = set(omit)
    members = VINTAGES[f"{vintage}_nport.zip"]["members"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for member in members:
            table = member[:-4] if member.endswith(".tsv") else None
            if member in omitted:
                continue
            if raw_members and member in raw_members:
                archive.writestr(member, raw_members[member])
            elif table in SIX:
                header = (headers or {}).get(table) or vintage_header(vintage, table)
                archive.writestr(member, tsv(header, rows.get(table, [])))
            elif member.endswith(".tsv"):
                archive.writestr(member, b"ACCESSION_NUMBER\n")
            else:
                archive.writestr(member, b"{}" if member.endswith(".json") else b"<html></html>")
        for member, data in (raw_members or {}).items():
            if member not in members:
                archive.writestr(member, data)
    return hashlib.sha256(path.read_bytes()).hexdigest()


class Filing:
    """One synthetic accession: submission/registrant/series plus holdings."""

    def __init__(
        self,
        accession: str,
        *,
        cik: str,
        series: str | None,
        report_date: str = "31-MAR-2021",
        filing_date: str = "20-MAY-2021",
        sub_type: str = "NPORT-P",
    ) -> None:
        self.accession = accession
        self.cik = cik
        self.series = series
        self.report_date = report_date
        self.filing_date = filing_date
        self.sub_type = sub_type
        self.holdings: list[dict[str, Any]] = []
        self.debts: list[dict[str, Any]] = []
        self.identifiers: list[dict[str, Any]] = []

    def hold(
        self,
        holding_id: int,
        cusip: str | None,
        *,
        is_default: str | None = "N",
        arrears: str | None = "N",
        pik: str | None = "N",
        issuer_type: str = "CORP",
        asset_cat: str = "DBT",
        isins: Iterable[str] = (),
        debt: bool = True,
        balance: str = "1000",
        title: str = "SYNTHETIC NOTE",
        name: str = "Synthetic Issuer",
        value: str | None = None,
        maturity: str = "15-JAN-2030",
        rate: str = "5",
    ) -> Filing:
        self.holdings.append(
            {
                "ACCESSION_NUMBER": self.accession, "HOLDING_ID": str(holding_id),
                "ISSUER_NAME": name, "ISSUER_LEI": "N/A", "ISSUER_TITLE": title,
                "ISSUER_CUSIP": cusip, "BALANCE": balance, "UNIT": "PA", "CURRENCY_CODE": "USD",
                "CURRENCY_VALUE": balance if value is None else value, "PERCENTAGE": ".1", "PAYOFF_PROFILE": "Long",
                "ASSET_CAT": asset_cat, "ISSUER_TYPE": issuer_type, "INVESTMENT_COUNTRY": "US",
                "IS_RESTRICTED_SECURITY": "N", "FAIR_VALUE_LEVEL": "2",
            }
        )
        if debt:
            self.debts.append(
                {
                    "HOLDING_ID": str(holding_id), "MATURITY_DATE": maturity, "COUPON_TYPE": "Fixed",
                    "ANNUALIZED_RATE": rate, "IS_DEFAULT": is_default, "ARE_ANY_INTEREST_PAYMENT": arrears,
                    "IS_ANY_PORTION_INTEREST_PAID": pik,
                }
            )
        for index, isin in enumerate(isins, start=1):
            self.identifiers.append(
                {"HOLDING_ID": str(holding_id), "IDENTIFIERS_ID": str(holding_id * 100 + index),
                 "IDENTIFIER_ISIN": isin}
            )
        return self


def tables(filings: Iterable[Filing]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {name: [] for name in SIX}
    for filing in filings:
        out["SUBMISSION"].append(
            {"ACCESSION_NUMBER": filing.accession, "FILING_DATE": filing.filing_date, "SUB_TYPE": filing.sub_type,
             "REPORT_ENDING_PERIOD": "31-DEC-2021", "REPORT_DATE": filing.report_date, "IS_LAST_FILING": "N"}
        )
        out["REGISTRANT"].append(
            {"ACCESSION_NUMBER": filing.accession, "CIK": filing.cik, "REGISTRANT_NAME": f"Registrant {filing.cik}"}
        )
        out["FUND_REPORTED_INFO"].append(
            {"ACCESSION_NUMBER": filing.accession, "SERIES_ID": filing.series, "SERIES_NAME": "Synthetic Fund"}
        )
        out["FUND_REPORTED_HOLDING"].extend(filing.holdings)
        out["DEBT_SECURITY"].extend(filing.debts)
        out["IDENTIFIERS"].extend(filing.identifiers)
    return out
