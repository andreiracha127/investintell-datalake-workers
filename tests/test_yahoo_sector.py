"""Tests for the Yahoo/yfinance sector helper (crosswalks + symbol building).

Pure crosswalks and symbol construction — no network. The yfinance fetch is
exercised via an injected ``fetch`` callable.
"""

from __future__ import annotations

from src.workers import _yahoo_sector as ys


def test_yahoo_sector_maps_to_canonical_gics():
    assert ys.to_gics_sector("Technology") == "Information Technology"
    assert ys.to_gics_sector("Financial Services") == "Financials"
    assert ys.to_gics_sector("Healthcare") == "Health Care"
    assert ys.to_gics_sector("Basic Materials") == "Materials"
    assert ys.to_gics_sector("Consumer Cyclical") == "Consumer Discretionary"
    assert ys.to_gics_sector("Consumer Defensive") == "Consumer Staples"
    assert ys.to_gics_sector("Communication Services") == "Communication Services"
    assert ys.to_gics_sector("Real Estate") == "Real Estate"
    # Unknown / empty → None (caller leaves it Unclassified).
    assert ys.to_gics_sector("Something Else") is None
    assert ys.to_gics_sector("") is None
    assert ys.to_gics_sector(None) is None


def test_yahoo_symbol_us_and_unmapped():
    assert ys.yahoo_symbol("AAPL", "US") == "AAPL"        # US → no suffix
    assert ys.yahoo_symbol("X", "ZZ") is None             # unmapped exch → skip
    assert ys.yahoo_symbol("", "HK") is None              # no ticker → None


def test_yahoo_symbol_international_suffixes():
    assert ys.yahoo_symbol("257", "HK") == "257.HK"
    assert ys.yahoo_symbol("VIRI", "FP") == "VIRI.PA"
    assert ys.yahoo_symbol("7203", "JP") == "7203.T"
    assert ys.yahoo_symbol("2330", "TT") == "2330.TW"
    # OpenFIGI sometimes returns a verbose exchCode — normalize to the code.
    assert ys.yahoo_symbol("1101", "TT (Taiwan Stock Exchange)") == "1101.TW"


def test_yahoo_symbol_china_board_split():
    assert ys.yahoo_symbol("600909", "CH") == "600909.SS"  # Shanghai main board
    assert ys.yahoo_symbol("002127", "CH") == "002127.SZ"  # Shenzhen
    assert ys.yahoo_symbol("300219", "CH") == "300219.SZ"  # ChiNext (Shenzhen)


def test_fetch_sector_uses_injected_fetch_and_maps_to_gics():
    assert ys.fetch_sector("2330.TW", fetch=lambda s: "Technology") == "Information Technology"
    assert ys.fetch_sector("X.HK", fetch=lambda s: None) is None          # no sector
    assert ys.fetch_sector("X.HK", fetch=lambda s: "Bogus") is None       # unmapped sector
    assert ys.fetch_sector(None, fetch=lambda s: "Technology") is None    # no symbol
    def boom(_):
        raise RuntimeError("yf down")
    assert ys.fetch_sector("X.HK", fetch=boom) is None                    # error → None
