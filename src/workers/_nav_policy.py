"""Canonical NAV policy evidence shared by the publisher and readiness worker."""

from __future__ import annotations

import datetime as dt
import hashlib
import json

ADJUSTED_OVERLAP_ABS_TOL = 0.0000005
ADJUSTED_OVERLAP_REL_TOL = 0.00000001
CURRENT_CATALOG_QUERY_VERSION = "nav-current-catalog-snapshot-v1"
GENERATOR_VERSION = "fund-nav-policy-generator-v1"
PROVIDER_CONTRACT_VERSION = "w1-tiingo-adjusted-daily-v1"
CATALOG_EVIDENCE_REFERENCE = (
    f"{CURRENT_CATALOG_QUERY_VERSION}:public.instruments_universe+public.funds_v:"
    f"{PROVIDER_CONTRACT_VERSION}:current_only"
)
INSTRUMENTS_QUERY = (
    "SELECT instrument_id,instrument_type,ticker,isin,currency,is_active "
    "FROM public.instruments_universe ORDER BY instrument_id LIMIT 100001"
)
FUNDS_QUERY = (
    "SELECT instrument_id,series_id,ticker,isin,currency,fund_type "
    "FROM public.funds_v ORDER BY instrument_id,series_id LIMIT 100001"
)
SOURCE_QUERY_SHA256 = hashlib.sha256(
    (INSTRUMENTS_QUERY + "\n" + FUNDS_QUERY).encode()
).hexdigest()


def policy_content_digest(policy: dict) -> str:
    """Stable policy identity excludes run metadata and temporal instrument facts."""
    content = {
        key: value
        for key, value in policy.items()
        if key not in ("instrument_evidence", "generation")
    }
    return hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def instrument_evidence_digest(rows: list[dict]) -> str:
    """Snapshot content identity excludes the capture timestamp, not instrument IDs."""
    content = [
        {
            key: value
            for key, value in row.items()
            if key not in ("known_at", "effective_at")
        }
        for row in rows
    ]
    return hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def generation_metadata_digest(generation: dict) -> str:
    content = {
        key: value for key, value in generation.items() if key != "generation_sha256"
    }
    return hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def calendar_digest(
    sessions: list[tuple[dt.date, dt.datetime, dt.datetime, str]],
) -> str:
    normalized = [
        (
            day.isoformat(),
            close.astimezone(dt.timezone.utc).isoformat(),
            due.astimezone(dt.timezone.utc).isoformat(),
            reference,
        )
        for day, close, due, reference in sessions
    ]
    return hashlib.sha256(
        json.dumps(
            normalized,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
    ).hexdigest()
