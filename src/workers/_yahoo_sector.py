"""Yahoo/yfinance sector helper for international-equity enrichment.

Bridges an OpenFIGI (ticker, exchange) → a Yahoo Finance symbol → yfinance
``info['sector']`` → the canonical GICS sector name already used in
``sec_cusip_ticker_map.gics_sector`` (so CUSIP-6- and ISIN-resolved sectors
merge into one breakdown). yfinance is unofficial and flaky — every fetch is
wrapped so a failure yields ``None`` (the holding stays "Unclassified") rather
than crashing the run.
"""

from __future__ import annotations

import re
from typing import Callable

# Yahoo's sector vocabulary → the 11 canonical GICS sectors (verbatim from
# sec_cusip_ticker_map.gics_sector). Yahoo uses GICS-like but differently-named
# buckets; this is the 1:1 crosswalk.
_YAHOO_SECTOR_TO_GICS = {
    "Technology": "Information Technology",
    "Financial Services": "Financials",
    "Healthcare": "Health Care",
    "Industrials": "Industrials",
    "Energy": "Energy",
    "Utilities": "Utilities",
    "Basic Materials": "Materials",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Communication Services": "Communication Services",
    "Real Estate": "Real Estate",
}

# OpenFIGI composite exchCode → Yahoo Finance suffix. US venues get no suffix;
# "CH" (China composite) is split SS/SZ/BJ by board (see _china_suffix).
# Unmapped exchanges return None so we never issue a wrong-market yfinance call.
_US_EXCH = frozenset({"US", "UN", "UW", "UQ", "UR", "UA", "UP", "UV", "UF", "UD", "PQ"})
_EXCH_TO_YAHOO = {
    "HK": ".HK",            # Hong Kong
    "JP": ".T", "JT": ".T",  # Tokyo
    "TT": ".TW",            # Taiwan
    "KS": ".KS", "KP": ".KS",  # Korea (KOSPI)
    "IN": ".NS", "IS": ".NS", "IB": ".NS",  # India (NSE)
    "AT": ".AX",            # Australia
    "LN": ".L",             # London
    "FP": ".PA",            # Euronext Paris
    "GR": ".DE", "GY": ".DE",  # Germany (XETRA)
    "NA": ".AS",            # Euronext Amsterdam
    "BB": ".BR",            # Euronext Brussels
    "PL": ".LS", "PW": ".WA",  # Lisbon / Warsaw
    "SM": ".MC",            # Madrid
    "IM": ".MI",            # Milan
    "SW": ".SW", "VX": ".SW", "SE": ".SW",  # Switzerland
    "SP": ".SI",            # Singapore
    "CT": ".TO", "CN": ".TO",  # Toronto
    "BZ": ".SA",            # Brazil (B3)
    "MM": ".MX",            # Mexico
    "NO": ".OL",            # Oslo
    "SS": ".ST",            # Stockholm
    "DC": ".CO",            # Copenhagen
    "FH": ".HE",            # Helsinki
    "ID": ".IR",            # Dublin
    "TI": ".IS",            # Istanbul
    "IJ": ".JK", "JN": ".JK",  # Jakarta
    "MK": ".KL",            # Malaysia
    "TB": ".BK",            # Thailand
    "PM": ".PS",            # Philippines
    "SJ": ".JO",            # Johannesburg
    "NZ": ".NZ",            # New Zealand
}


def to_gics_sector(yahoo_sector: str | None) -> str | None:
    """Yahoo sector name → canonical GICS sector, or None if unknown/empty."""
    if not yahoo_sector:
        return None
    return _YAHOO_SECTOR_TO_GICS.get(yahoo_sector.strip())


def _china_suffix(ticker: str) -> str:
    """Shanghai (.SS, 6xx), Shenzhen (.SZ, 0/2/3xx), Beijing (.BJ, 4/8xx)."""
    head = ticker[:1]
    if head == "6":
        return ".SS"
    if head in {"0", "2", "3"}:
        return ".SZ"
    if head in {"4", "8"}:
        return ".BJ"
    return ".SS"


def yahoo_symbol(ticker: str | None, exch_code: str | None) -> str | None:
    """Build the Yahoo Finance symbol from an OpenFIGI (ticker, exchCode).

    US venues → the bare ticker. Mapped foreign exchanges → ticker + suffix.
    China composite → SS/SZ/BJ by board. Unmapped exchange → None (skip, so we
    never query yfinance for a symbol it cannot resolve).
    """
    if not ticker:
        return None
    # OpenFIGI sometimes returns a verbose exchCode ("TT (Taiwan Stock
    # Exchange)"); keep only the leading code token.
    exch = re.split(r"[\s(]", (exch_code or "").strip())[0].upper()
    if exch in _US_EXCH:
        return ticker
    if exch == "CH":
        return ticker + _china_suffix(ticker)
    suffix = _EXCH_TO_YAHOO.get(exch)
    return ticker + suffix if suffix else None


def _yf_info_sector(yahoo_sym: str) -> str | None:
    import yfinance as yf

    info = yf.Ticker(yahoo_sym).info
    return (info or {}).get("sector")


def fetch_sector(
    yahoo_sym: str | None,
    *,
    fetch: Callable[[str], str | None] | None = None,
) -> str | None:
    """Canonical GICS sector for a Yahoo symbol, or None. Never raises.

    ``fetch`` (injectable for tests) maps a Yahoo symbol → Yahoo sector string;
    defaults to a real yfinance lookup. Any failure or unknown sector → None.
    """
    if not yahoo_sym:
        return None
    getter = fetch or _yf_info_sector
    try:
        yahoo_sector = getter(yahoo_sym)
    except Exception:
        return None
    return to_gics_sector(yahoo_sector)
