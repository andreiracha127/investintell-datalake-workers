"""Tests for the international-equity sector enrichment worker.

Pure ``enrich_rows`` only — no network/DB. The OpenFIGI/yfinance/DB paths are
exercised live in a smoke run (rate-limited external APIs).
"""

from __future__ import annotations

from src.workers import nport_cusip_enrichment as nce
from src.workers._openfigi import FigiMatch


def _m(ticker, exch):
    return FigiMatch(ticker=ticker, exch_code=exch, figi="BBG", market_sector="Equity",
                     security_type="Common Stock")


def test_enrich_rows_resolves_and_caches_each_miss_reason():
    isins = ["TW0002330008", "CNE000000001", "AEA000201011", "XX0000000000"]
    matches = {
        "TW0002330008": _m("2330", "TT"),       # resolves to a sector
        "CNE000000001": _m("600519", "CH"),      # symbol built, but no sector returned
        "AEA000201011": _m("ADCB", "UH"),        # exchange not in the crosswalk
        # "XX..." absent → OpenFIGI found nothing
    }
    sector_by_symbol = {"2330.TW": "Information Technology"}  # only TW has a sector

    rows = {r.isin: r for r in nce.enrich_rows(isins, matches, sector_by_symbol.get)}

    assert rows["TW0002330008"].gics_sector == "Information Technology"
    assert rows["TW0002330008"].yahoo_symbol == "2330.TW"
    assert rows["TW0002330008"].resolved_via == "openfigi+yfinance"

    assert rows["CNE000000001"].gics_sector is None
    assert rows["CNE000000001"].yahoo_symbol == "600519.SS"   # symbol built
    assert rows["CNE000000001"].resolved_via == "openfigi_no_sector"

    assert rows["AEA000201011"].gics_sector is None
    assert rows["AEA000201011"].yahoo_symbol is None          # unmapped exchange
    assert rows["AEA000201011"].ticker == "ADCB"
    assert rows["AEA000201011"].resolved_via == "no_yahoo_symbol"

    assert rows["XX0000000000"].gics_sector is None
    assert rows["XX0000000000"].resolved_via == "no_figi"


def test_enrich_rows_empty():
    assert nce.enrich_rows([], {}, lambda s: None) == []
