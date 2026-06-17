"""Shared OpenFIGI client — ISIN → {ticker, exchange, FIGI, marketSector}.

``api.openfigi.com/v3/mapping`` maps identifiers in batch: up to 100 jobs/request
with an API key (``X-OPENFIGI-APIKEY``), 10 without. The keyed plan allows ~25
requests / 6 s (≈250/min). We pace with a token bucket (modelled on the Tiingo
client) and abort cleanly after ``MAX_CONSECUTIVE_429`` consecutive 429s, so the
next scheduled run resumes from the un-enriched remainder.

Purpose: foreign N-PORT holdings store a synthetic ``IS:<isin>`` in the ``cusip``
field, so the CUSIP-6 → GICS map can never resolve them. This bridges the ISIN
to a tradeable ticker so a global sector source (yfinance) can classify it.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
MAX_CONSECUTIVE_429 = 20
BATCH_KEYED = 100
BATCH_UNKEYED = 10
_RETRY_SLEEPS = (1.0, 4.0, 16.0)

# Equity-like security types worth a sector lookup — skip warrants, rights,
# debt, etc. (a fixed-income ISIN never needs a GICS sector here).
_EQUITY_TYPES = frozenset({
    "Common Stock", "REIT", "Depositary Receipt", "ADR", "GDR", "NVDR",
    "Preference", "Preferred Stock", "Closed-End Fund", "ETP", "Mutual Fund",
    "Unit", "Tracking Stk",
})


class OpenFigiBudgetExceeded(RuntimeError):
    """Raised after MAX_CONSECUTIVE_429 consecutive 429s — resume next cycle."""


@dataclass(frozen=True)
class FigiMatch:
    """Best ISIN → listing resolution from OpenFIGI."""

    ticker: str
    exch_code: str | None
    figi: str | None
    market_sector: str | None
    security_type: str | None


class TokenBucket:
    """Thread-safe token bucket (same posture as the Tiingo client)."""

    def __init__(self, max_tokens: float = 10.0, refill_rate: float = 4.0) -> None:
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self._tokens = max_tokens
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self.max_tokens, self._tokens + (now - self._last) * self.refill_rate
                )
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.refill_rate
            time.sleep(wait)


def api_key() -> str | None:
    """OpenFIGI key, or None (the unkeyed tier still works, just slower)."""
    return os.getenv("OPENFIGI_API_KEY") or None


def _batches(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _best_record(records: list[dict]) -> dict | None:
    """Pick the listing to feed the sector lookup: an equity type with a ticker,
    else the first record with any ticker. OpenFIGI lists one record per
    exchange; the first is typically the primary/composite listing."""
    equity = [
        r for r in records
        if r.get("ticker") and r.get("securityType") in _EQUITY_TYPES
    ]
    pool = equity or [r for r in records if r.get("ticker")]
    return pool[0] if pool else None


def parse_mapping_response(
    jobs: list[str], payload: list
) -> dict[str, FigiMatch]:
    """Zip request ISINs with the per-job OpenFIGI results → isin → FigiMatch.

    Jobs that returned a warning/error or no usable ticker are simply omitted.
    """
    out: dict[str, FigiMatch] = {}
    for isin, entry in zip(jobs, payload):
        if not isinstance(entry, dict):
            continue
        rec = _best_record(entry.get("data") or [])
        if not rec:
            continue
        out[isin] = FigiMatch(
            ticker=str(rec["ticker"]),
            exch_code=rec.get("exchCode"),
            figi=rec.get("figi"),
            market_sector=rec.get("marketSector"),
            security_type=rec.get("securityType"),
        )
    return out


class OpenFigiClient:
    """Paced OpenFIGI ISIN→ticker mapper with the 20×429 circuit breaker."""

    def __init__(self, key: str | None = None, *, bucket: TokenBucket | None = None) -> None:
        import httpx

        self._key = key if key is not None else api_key()
        self.batch_size = BATCH_KEYED if self._key else BATCH_UNKEYED
        headers = {"Content-Type": "application/json"}
        if self._key:
            headers["X-OPENFIGI-APIKEY"] = self._key
        self._client = httpx.Client(timeout=30.0, headers=headers)
        self._bucket = bucket or TokenBucket(refill_rate=4.0 if self._key else 0.4)
        self.consecutive_429 = 0
        self.requests_made = 0

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "OpenFigiClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _post(self, jobs_payload: list[dict]):
        return self._client.post(OPENFIGI_URL, json=jobs_payload)

    def _request(self, jobs_payload: list[dict]) -> list | None:
        for sleep_s in _RETRY_SLEEPS:
            self._bucket.acquire()
            self.requests_made += 1
            try:
                resp = self._post(jobs_payload)
            except Exception:
                time.sleep(sleep_s)
                continue
            if resp.status_code == 429:
                self.consecutive_429 += 1
                if self.consecutive_429 >= MAX_CONSECUTIVE_429:
                    raise OpenFigiBudgetExceeded(
                        f"{self.consecutive_429} consecutive 429s — aborting cleanly"
                    )
                time.sleep(sleep_s)
                continue
            self.consecutive_429 = 0
            if resp.status_code >= 500:
                time.sleep(sleep_s)
                continue
            if resp.status_code >= 400:
                return None
            payload = resp.json()
            return payload if isinstance(payload, list) else None
        return None

    def map_isins(self, isins: list[str]) -> dict[str, FigiMatch]:
        """Resolve many ISINs → FigiMatch (batched, paced). Unresolved omitted."""
        out: dict[str, FigiMatch] = {}
        for chunk in _batches(list(isins), self.batch_size):
            payload = self._request(
                [{"idType": "ID_ISIN", "idValue": isin} for isin in chunk]
            )
            if payload is not None:
                out.update(parse_mapping_response(chunk, payload))
        return out
