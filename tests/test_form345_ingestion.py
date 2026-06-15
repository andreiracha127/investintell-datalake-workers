"""Tests for SEC Form 345 insider ingestion parser and aggregation."""

from __future__ import annotations

import io
import zipfile
from decimal import Decimal

from src.db import LOCK_FORM345_INGESTION
from src.workers import form345_ingestion as f345


def _zip_fixture() -> bytes:
    files = {
        "SUBMISSION.tsv": (
            "ACCESSION_NUMBER\tISSUERCIK\tISSUERNAME\n"
            "0001\t0000320193\tApple Inc.\n"
            "0002\t0000320193\tApple Inc.\n"
        ),
        "REPORTINGOWNER.tsv": (
            "ACCESSION_NUMBER\tRPTOWNERCIK\tRPTOWNERNAME\n"
            "0001\t0000001111\tJane Insider\n"
            "0002\t0000002222\tJohn Insider\n"
        ),
        "NONDERIV_TRANS.tsv": (
            "ACCESSION_NUMBER\tTRANS_SK\tTRANSACTION_DATE\tTRANSACTION_CODE\t"
            "TRANSACTION_SHARES\tTRANSACTION_PRICEPERSHARE\t"
            "TRANSACTION_ACQUIRED_DISPOSED_CODE\n"
            "0001\tT1\t2026-02-10\tP\t10\t12.50\tA\n"
            "0002\tT2\t2026-03-11\tS\t4\t20.00\tD\n"
            "0002\tT3\t2026-03-12\tM\t2\t1.00\tA\n"
        ),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, body in files.items():
            zf.writestr(name, body)
    return buf.getvalue()


def test_parse_form345_zip_classifies_and_values_transactions() -> None:
    rows = f345.parse_form345_zip(_zip_fixture(), source_period="2026q1")

    assert [row.buy_sell for row in rows] == ["buy", "sell", "other"]
    assert rows[0].issuer_cik == "320193"
    assert rows[0].reporting_owner_name == "Jane Insider"
    assert rows[0].value_usd == Decimal("125.00")
    assert rows[1].value_usd == Decimal("80.00")


def test_aggregate_sentiment_by_issuer_and_quarter() -> None:
    rows = f345.parse_form345_zip(_zip_fixture(), source_period="2026q1")
    aggregate = f345.aggregate_sentiment(rows)

    bucket = aggregate[("320193", f345.quarter_start(rows[0].transaction_date))]
    assert bucket["buy_value"] == Decimal("125.00")
    assert bucket["sell_value"] == Decimal("80.00")
    assert bucket["net_value"] == Decimal("45.00")
    assert bucket["buy_count"] == 1
    assert bucket["sell_count"] == 1


def test_transaction_classifier_is_deterministic() -> None:
    assert f345.classify_transaction("P", "A") == "buy"
    assert f345.classify_transaction("S", "D") == "sell"
    assert f345.classify_transaction("M", "A") == "other"


def test_advisory_lock_registry_uses_dispatch_id() -> None:
    assert LOCK_FORM345_INGESTION == 900_306
