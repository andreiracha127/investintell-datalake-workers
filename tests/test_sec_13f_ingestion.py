"""Tests for SEC 13F ingestion parser and worker registry."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

from src.db import LOCK_SEC_13F_INGESTION
from src.workers import sec_13f_ingestion as sec13f


TINY_INFORMATION_TABLE = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <cusip>037833100</cusip>
    <value>123</value>
    <shrsOrPrnAmt>
      <sshPrnamt>4500</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
  </infoTable>
  <infoTable>
    <nameOfIssuer>NO CUSIP ROW</nameOfIssuer>
    <value>9</value>
  </infoTable>
</informationTable>
"""


def test_parse_information_table_multiplies_value_thousands() -> None:
    rows = sec13f.parse_information_table_xml(
        TINY_INFORMATION_TABLE,
        cik="0001067983",
        manager_name="Berkshire Hathaway",
        accession_number="0000950123-26-000001",
        report_date=dt.date(2026, 3, 31),
        form_type="13F-HR",
        source_url="https://sec.test/info.xml",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.cik == "1067983"
    assert row.cusip == "037833100"
    assert row.name == "APPLE INC"
    assert row.value_usd == Decimal("123000")
    assert row.shares == Decimal("4500")
    assert row.period == dt.date(2026, 3, 31)


def test_advisory_lock_registry_uses_dispatch_id() -> None:
    assert LOCK_SEC_13F_INGESTION == 900_305
