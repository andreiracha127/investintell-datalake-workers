"""Tests for the UCITS NAV fallback chain (EODHD → Yahoo) used by
instrument_ingestion when Tiingo returns an empty series.

Pure parsing/mapping tests only — no network calls.
"""

from __future__ import annotations

import datetime as _dt

from src.workers import _fallback_nav as fb
from src.workers import instrument_ingestion as ii


# ──────────────────────────────────────────────────────────────────────────────
# EODHD symbol mapping (Yahoo-style suffixes in our DB → EODHD exchange codes)
# ──────────────────────────────────────────────────────────────────────────────
def test_eodhd_symbol_mapping():
    assert fb.eodhd_symbol("VWRL.L") == "VWRL.LSE"      # London is the odd one
    assert fb.eodhd_symbol("CW8.PA") == "CW8.PA"        # Euronext Paris: same
    assert fb.eodhd_symbol("CSPX.MI") == "CSPX.MI"      # Borsa Italiana: same
    assert fb.eodhd_symbol("CHSPI.SW") == "CHSPI.SW"    # SIX: same
    assert fb.eodhd_symbol("IWDA.AS") == "IWDA.AS"      # Euronext Amsterdam: same
    assert fb.eodhd_symbol("ABC.DE") == "ABC.XETRA"     # Xetra
    assert fb.eodhd_symbol("SPY") == "SPY.US"           # no suffix → US


# ──────────────────────────────────────────────────────────────────────────────
# Payload parsing
# ──────────────────────────────────────────────────────────────────────────────
def test_parse_eodhd_prefers_adjusted_close():
    payload = [
        {"date": "2026-06-10", "close": 100.0, "adjusted_close": 99.5},
        {"date": "2026-06-11", "close": 101.0},           # no adjusted_close
        {"date": "2026-06-12", "close": None},            # null bar → None
    ]
    assert fb.parse_eodhd(payload) == [
        (_dt.date(2026, 6, 10), 99.5),
        (_dt.date(2026, 6, 11), 101.0),
        (_dt.date(2026, 6, 12), None),
    ]
    assert fb.parse_eodhd({"error": "x"}) == []  # non-list error body


def test_parse_yahoo_chart_uses_adjclose_and_handles_nulls():
    ts = [1781049600, 1781136000]  # 2026-06-10, 2026-06-11 (UTC midnights)
    payload = {
        "chart": {"result": [{
            "timestamp": ts,
            "indicators": {
                "quote": [{"close": [100.0, 101.0]}],
                "adjclose": [{"adjclose": [99.5, None]}],
            },
        }]}
    }
    parsed = fb.parse_yahoo_chart(payload)
    assert parsed[0] == (_dt.date(2026, 6, 10), 99.5)
    # adjclose null → falls back to close for that bar
    assert parsed[1] == (_dt.date(2026, 6, 11), 101.0)
    assert fb.parse_yahoo_chart({"chart": {"result": None, "error": "x"}}) == []


# ──────────────────────────────────────────────────────────────────────────────
# Chain composition
# ──────────────────────────────────────────────────────────────────────────────
def test_chain_uses_eodhd_only_when_key_present():
    assert fb.FallbackNav(eodhd_key=None).providers == ("yahoo",)
    assert fb.FallbackNav(eodhd_key="k").providers == ("eodhd", "yahoo")


# ──────────────────────────────────────────────────────────────────────────────
# instrument_ingestion integration: rows carry the fallback source
# ──────────────────────────────────────────────────────────────────────────────
def test_build_rows_carries_fallback_source():
    import uuid
    rows = ii.build_rows([(_dt.date(2026, 6, 11), 10.0)],
                         [(uuid.uuid4(), "EUR")], source="yahoo")
    assert rows[0]["source"] == "yahoo"
    # default unchanged
    rows = ii.build_rows([(_dt.date(2026, 6, 11), 10.0)], [(uuid.uuid4(), "USD")])
    assert rows[0]["source"] == "tiingo"
