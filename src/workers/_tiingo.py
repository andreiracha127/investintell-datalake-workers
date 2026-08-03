"""Shared Tiingo client for the NAV/benchmark ingestion workers.

Standalone reimplementation of the monolith Tiingo provider essentials:
``GET https://api.tiingo.com/tiingo/daily/{ticker}/prices`` with
``Authorization: Token <TIINGO_API_KEY>``, preferring ``adjClose`` (split/
dividend-adjusted) and falling back to ``close``.

Rate-limit posture (design §5.1, revised again 2026-08-02): the Power-tier plan
allows 10k req/h and 100k req/day, account-wide. Tiingo exposes **no**
``X-RateLimit-*`` headers — the only safe signal is a 429 — so the ceiling lives
here as ``TIINGO_MAX_REQUESTS_PER_HOUR`` and every bucket is checked against it.
We pace with an in-process token bucket just under the hourly budget and abort
cleanly after ``MAX_CONSECUTIVE_429`` consecutive 429s (the monolith's breaker),
letting the next scheduled run resume from the watermark.

The budget is a **shared** resource: it is per account, not per process, so a
single greedy worker starves every other consumer for the rest of the rolling
hour. That is not hypothetical — the earlier "empirically verified 2026-06-12
(150 requests in 2.9s, zero 429)" note measured a *burst*, which says nothing
about an hourly ceiling, and on its strength ``eod_prices_warmer`` paced itself
at 25 req/s (90k req/h). It drained the hourly budget every morning; the regime
workers that ran inside the same rolling hour then saw only 429s, which
``_get_bars`` masks as ``[]``, and the regime chain sat stale for five days.

Note the incident ran on the *previous* API key, whose tier was well below the
10k req/h pinned here — so 25 req/s overshot by even more than 9x and drained the
budget in well under the ~6min40s that 10k/25 would suggest. The key was rotated
2026-08-02; ``TIINGO_MAX_REQUESTS_PER_HOUR`` tracks the plan of the key currently
in use and has to be revisited whenever the key or tier changes, since the guard
below is only as honest as this number.

Hence the guard in ``TiingoClient``: pacing slower than the default is always
fine, pacing faster than the account is a bug, so it fails loudly at construction
rather than silently at someone else's expense. (The guard lives on the client,
not on ``TokenBucket`` — the bucket is provider-agnostic and also paces EODHD,
Yahoo and OpenFIGI, which have their own, different limits.)
"""

from __future__ import annotations

import datetime as _dt
import os
import threading
import time

TIINGO_BASE_URL = "https://api.tiingo.com"
MAX_CONSECUTIVE_429 = 30
_RETRY_SLEEPS = (1.0, 4.0, 16.0)

# Account plan ceilings, shared across every worker in the fleet. These track the
# plan of the key in TIINGO_API_KEY (Power tier, key rotated 2026-08-02) — NOT a
# property of the API. Rotating to a key on a different tier makes the guard below
# lie in whichever direction the tier moved, so update these with the key.
TIINGO_MAX_REQUESTS_PER_HOUR = 10_000
TIINGO_MAX_REQUESTS_PER_DAY = 100_000
# 2.5 req/s ≈ 9k req/h — just under the hourly ceiling, leaving headroom for the
# other consumers that share the same token.
DEFAULT_RATE_PER_S = 2.5


class TiingoBudgetExceeded(RuntimeError):
    """Raised after MAX_CONSECUTIVE_429 consecutive 429s — resume next cycle."""


class TokenBucket:
    """Thread-safe token bucket pacing Tiingo calls.

    Default ``DEFAULT_RATE_PER_S`` (2.5 req/s ≈ 9k req/h) — just under the
    Power-tier 10k req/h budget, leaving headroom for other Tiingo consumers.

    Deliberately provider-agnostic: ``_fallback_nav`` and ``_openfigi`` pace
    EODHD, Yahoo and OpenFIGI with this same bucket under *their* limits, so the
    Tiingo ceiling is enforced in ``TiingoClient``, not here."""

    def __init__(
        self,
        max_tokens: float = 10.0,
        refill_rate: float = DEFAULT_RATE_PER_S,
    ) -> None:
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
    """Paced Tiingo daily-price fetcher with the 30×429 circuit breaker.

    A caller may hand in a bucket paced *slower* than the default for a
    low-priority sweep. Pacing faster than the account allows is rejected: the
    budget is shared across the whole fleet, so the cost of exceeding it lands on
    whichever worker happens to run next — far from the code that caused it. The
    ``ValueError`` is a programming error caught by tests, not a runtime state."""

    def __init__(self, key: str | None = None, *,
                 bucket: TokenBucket | None = None) -> None:
        import httpx

        if bucket is not None and bucket.refill_rate * 3600 > TIINGO_MAX_REQUESTS_PER_HOUR:
            raise ValueError(
                f"bucket paces {bucket.refill_rate} req/s = "
                f"{bucket.refill_rate * 3600:,.0f} req/h, above the shared Tiingo "
                f"account budget of {TIINGO_MAX_REQUESTS_PER_HOUR:,} req/h "
                f"(use DEFAULT_RATE_PER_S={DEFAULT_RATE_PER_S} or slower)"
            )
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

    def fetch_meta(self, ticker: str) -> dict | None:
        """Tiingo end-of-day metadata for one ticker; ``None`` on 404/unknown.

        ``GET https://api.tiingo.com/tiingo/daily/{ticker}`` returns a single JSON
        object ``{ticker, name, description, startDate, endDate, exchangeCode}``.
        Used by ``tiingo_fund_meta`` for the fund catalog's descriptive prose and
        inception (startDate). Paced by the same token bucket and protected by the
        same 30×429 breaker as ``_get_bars``. Returns ``None`` for an unknown
        ticker (404) or any error/non-object body so the caller can record
        ``source_status='not_found'`` without crashing the sweep."""
        url = f"{TIINGO_BASE_URL}/tiingo/daily/{ticker}"
        for sleep_s in _RETRY_SLEEPS:
            self._bucket.acquire()
            self.requests_made += 1
            try:
                resp = self._client.get(url)
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
                return None
            if resp.status_code >= 500:
                time.sleep(sleep_s)
                continue
            if resp.status_code >= 400:
                return None
            payload = resp.json()
            if not isinstance(payload, dict):  # error body, e.g. unknown ticker
                return None
            return payload
        return None
