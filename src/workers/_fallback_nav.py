"""UCITS NAV fallback chain for instrument_ingestion: EODHD → Yahoo.

Tiingo (the primary provider) returns empty series for ~278 European UCITS
share classes (`.L/.PA/.MI/.SW/...`) and for delisted tickers — the provider
gap documented in docs/INGESTION_DESIGN.md §1D/§5.1d. When the primary comes
back empty, this chain is tried in order:

  1. **EODHD** (`eodhd.com/api/eod/{symbol}`) — paid, best UCITS coverage.
     Activated automatically when ``EODHD_API_KEY`` is set; skipped otherwise.
  2. **Yahoo** (chart v8 JSON API, no key) — unofficial but it is what fed the
     622k ``source='yahoo'`` rows already in ``nav_timeseries`` (2,274
     instruments, mostly UCITS), so coverage is proven for this universe.

Both produce the same ``[(date, adj-close-or-close)]`` shape as the Tiingo
client, and rows written through the fallback carry ``source='eodhd'`` /
``source='yahoo'`` so provenance stays visible per row.

Ticker symbology: the universe stores Yahoo-style suffixes (``VWRL.L``),
which Yahoo accepts verbatim; EODHD needs ``.L→.LSE`` and ``.DE→.XETRA``
(other exchange codes coincide). Suffix-less tickers map to ``.US``.
"""

from __future__ import annotations

import datetime as _dt
import os
import time
from typing import Any

from src.workers._tiingo import TokenBucket

EODHD_BASE_URL = "https://eodhd.com/api"
YAHOO_BASE_URL = "https://query1.finance.yahoo.com"
# Yahoo throttles aggressively without a browser-ish User-Agent.
_YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 investintell-datalake/1.0"}

# Yahoo-style suffix → EODHD exchange code (identical codes omitted).
_EODHD_SUFFIX_MAP = {"L": "LSE", "DE": "XETRA"}


def eodhd_symbol(ticker: str) -> str:
    """Yahoo-style ticker → EODHD symbol (``VWRL.L`` → ``VWRL.LSE``)."""
    if "." in ticker:
        base, suffix = ticker.rsplit(".", 1)
        return f"{base}.{_EODHD_SUFFIX_MAP.get(suffix.upper(), suffix.upper())}"
    return f"{ticker}.US"


def parse_eodhd(payload: Any) -> list[tuple[_dt.date, float | None]]:
    """EODHD EOD JSON → [(date, adjusted_close-or-close)]."""
    if not isinstance(payload, list):
        return []
    out: list[tuple[_dt.date, float | None]] = []
    for bar in payload:
        price = bar.get("adjusted_close")
        if price is None:
            price = bar.get("close")
        out.append((_dt.date.fromisoformat(bar["date"]),
                    float(price) if price is not None else None))
    return out


def parse_yahoo_chart(payload: Any) -> list[tuple[_dt.date, float | None]]:
    """Yahoo chart v8 JSON → [(date, adjclose-or-close)]."""
    try:
        result = payload["chart"]["result"][0]
        timestamps = result["timestamp"]
        quote = result["indicators"]["quote"][0].get("close", [])
        adj = (result["indicators"].get("adjclose") or [{}])[0].get("adjclose", [])
    except (KeyError, IndexError, TypeError):
        return []
    out: list[tuple[_dt.date, float | None]] = []
    for i, ts in enumerate(timestamps):
        price = adj[i] if i < len(adj) and adj[i] is not None else (
            quote[i] if i < len(quote) else None)
        d = _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).date()
        out.append((d, float(price) if price is not None else None))
    return out


class FallbackNav:
    """Provider chain tried in order when the primary returns empty.

    fetch() returns ``(series, source)`` — the first provider with data wins;
    ``([], None)`` when every provider comes back empty.
    """

    def __init__(self, eodhd_key: str | None = None) -> None:
        self._eodhd_key = (eodhd_key if eodhd_key is not None
                           else os.getenv("EODHD_API_KEY"))
        self.providers: tuple[str, ...] = (
            ("eodhd", "yahoo") if self._eodhd_key else ("yahoo",))
        self._client = None
        # EODHD paid plans allow ~1000 req/min; Yahoo is unofficial — be gentle.
        self._eodhd_bucket = TokenBucket(max_tokens=10, refill_rate=10.0)
        self._yahoo_bucket = TokenBucket(max_tokens=5, refill_rate=4.0)

    def _http(self):
        if self._client is None:
            import httpx
            self._client = httpx.Client(timeout=30.0)
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def __enter__(self) -> "FallbackNav":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _get_json(self, url: str, *, params: dict | None = None,
                  headers: dict | None = None, bucket: TokenBucket) -> Any:
        for sleep_s in (1.0, 4.0):
            bucket.acquire()
            try:
                resp = self._http().get(url, params=params, headers=headers)
            except Exception:
                time.sleep(sleep_s)
                continue
            if resp.status_code in (429, 503) or resp.status_code >= 500:
                time.sleep(sleep_s)
                continue
            if resp.status_code >= 400:
                return None
            try:
                return resp.json()
            except Exception:
                return None
        return None

    def _fetch_eodhd(self, ticker: str, start: _dt.date,
                     end: _dt.date) -> list[tuple[_dt.date, float | None]]:
        payload = self._get_json(
            f"{EODHD_BASE_URL}/eod/{eodhd_symbol(ticker)}",
            params={"api_token": self._eodhd_key, "fmt": "json",
                    "from": start.isoformat(), "to": end.isoformat(),
                    "period": "d"},
            bucket=self._eodhd_bucket)
        return parse_eodhd(payload) if payload is not None else []

    def _fetch_yahoo(self, ticker: str, start: _dt.date,
                     end: _dt.date) -> list[tuple[_dt.date, float | None]]:
        p1 = int(_dt.datetime(start.year, start.month, start.day,
                              tzinfo=_dt.timezone.utc).timestamp())
        p2 = int(_dt.datetime(end.year, end.month, end.day,
                              tzinfo=_dt.timezone.utc).timestamp()) + 86400
        payload = self._get_json(
            f"{YAHOO_BASE_URL}/v8/finance/chart/{ticker}",
            params={"period1": p1, "period2": p2, "interval": "1d",
                    "events": "div,splits"},
            headers=_YAHOO_HEADERS,
            bucket=self._yahoo_bucket)
        return parse_yahoo_chart(payload) if payload is not None else []

    def fetch(self, ticker: str, start: _dt.date,
              end: _dt.date) -> tuple[list[tuple[_dt.date, float | None]], str | None]:
        for provider in self.providers:
            series = (self._fetch_eodhd(ticker, start, end) if provider == "eodhd"
                      else self._fetch_yahoo(ticker, start, end))
            if any(v is not None for _, v in series):
                return series, provider
        return [], None
