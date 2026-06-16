"""Shared Tiingo client for the NAV/benchmark ingestion workers.

Standalone reimplementation of the monolith Tiingo provider essentials:
``GET https://api.tiingo.com/tiingo/daily/{ticker}/prices`` with
``Authorization: Token <TIINGO_API_KEY>``, preferring ``adjClose`` (split/
dividend-adjusted) and falling back to ``close``.

Rate-limit posture (design §5.1, revised): the account is on a high-volume
tier — empirically verified 2026-06-12 (150 requests in 2.9s, zero 429), i.e.
the Power-tier 10k req/h budget, not the "~130 req/h" the design memo assumed.
Tiingo still exposes **no** ``X-RateLimit-*`` headers — the only safe signal
is a 429. We pace with an in-process token bucket just under the hourly budget
and abort cleanly after ``MAX_CONSECUTIVE_429`` consecutive 429s (the
monolith's breaker), letting the next scheduled run resume from the watermark.
"""

from __future__ import annotations

import datetime as _dt
import os
import threading
import time

TIINGO_BASE_URL = "https://api.tiingo.com"
MAX_CONSECUTIVE_429 = 30
_RETRY_SLEEPS = (1.0, 4.0, 16.0)


class TiingoBudgetExceeded(RuntimeError):
    """Raised after MAX_CONSECUTIVE_429 consecutive 429s — resume next cycle."""


class TokenBucket:
    """Thread-safe token bucket pacing Tiingo calls.

    Default 2.5 req/s sustained ≈ 9k req/h — just under the Power-tier
    10k req/h budget, leaving headroom for other Tiingo consumers."""

    def __init__(self, max_tokens: float = 10.0, refill_rate: float = 2.5) -> None:
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


def api_key() -> str:
    key = os.getenv("TIINGO_API_KEY")
    if not key:
        raise RuntimeError("TIINGO_API_KEY not set")
    return key


def parse_price_bars(bars: list[dict]) -> list[tuple[_dt.date, float | None]]:
    """Tiingo daily bars → [(date, adjClose-or-close)]; missing price → None."""
    out: list[tuple[_dt.date, float | None]] = []
    for bar in bars:
        d = _dt.date.fromisoformat(str(bar["date"])[:10])
        price = bar.get("adjClose")
        if price is None:
            price = bar.get("close")
        out.append((d, float(price) if price is not None else None))
    return out


class TiingoClient:
    """Paced Tiingo daily-price fetcher with the 30×429 circuit breaker."""

    def __init__(self, key: str | None = None, *,
                 bucket: TokenBucket | None = None) -> None:
        import httpx

        self._key = key or api_key()
        self._bucket = bucket or TokenBucket()
        self._client = httpx.Client(
            timeout=30.0,
            headers={"Content-Type": "application/json",
                     "Authorization": f"Token {self._key}"},
        )
        self.consecutive_429 = 0
        self.requests_made = 0

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "TiingoClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _get_bars(self, ticker: str, start_date: _dt.date,
                  end_date: _dt.date | None = None) -> list[dict]:
        """Raw Tiingo daily bars for one ticker; [] on 404/no data.

        Paced by the token bucket and protected by the 30×429 breaker. Shared
        by ``fetch_daily_prices`` (NAV: date+adjClose) and ``fetch_daily_bars``
        (full OHLCV+adj rows for eod_prices)."""
        params = {"format": "json", "resampleFreq": "daily",
                  "startDate": start_date.isoformat()}
        if end_date:
            params["endDate"] = end_date.isoformat()
        url = f"{TIINGO_BASE_URL}/tiingo/daily/{ticker}/prices"
        for attempt, sleep_s in enumerate(_RETRY_SLEEPS):
            self._bucket.acquire()
            self.requests_made += 1
            try:
                resp = self._client.get(url, params=params)
            except Exception:
                time.sleep(sleep_s)
                continue
            if resp.status_code == 429:
                self.consecutive_429 += 1
                if self.consecutive_429 >= MAX_CONSECUTIVE_429:
                    raise TiingoBudgetExceeded(
                        f"{self.consecutive_429} consecutive 429s — aborting cleanly")
                time.sleep(sleep_s)
                continue
            self.consecutive_429 = 0
            if resp.status_code == 404:
                return []
            if resp.status_code >= 500:
                time.sleep(sleep_s)
                continue
            if resp.status_code >= 400:
                return []
            payload = resp.json()
            if not isinstance(payload, list):  # error body, e.g. unknown ticker
                return []
            return payload
        return []

    def fetch_daily_prices(self, ticker: str, start_date: _dt.date,
                           end_date: _dt.date | None = None) -> list[tuple[_dt.date, float | None]]:
        """Daily price history for one ticker; [] on 404/no data."""
        return parse_price_bars(self._get_bars(ticker, start_date, end_date))

    def fetch_daily_bars(self, ticker: str, start_date: _dt.date,
                         end_date: _dt.date | None = None) -> list[dict]:
        """Full raw daily bars (all OHLCV + adjusted fields) for one ticker.

        Used by ``eod_prices_warmer`` to refresh the API's ``eod_prices`` table
        (every column NOT NULL). ``[]`` on 404/no data."""
        return self._get_bars(ticker, start_date, end_date)
