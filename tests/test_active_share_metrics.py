from __future__ import annotations

import pytest

from src.workers import active_share_metrics as asm


def test_active_share_identical_portfolios_are_zero_active_share():
    out = asm.compute_active_share_from_weights(
        {"A": 0.60, "B": 0.40},
        {"A": 0.60, "B": 0.40},
    )
    assert out["active_share_normalized"] == pytest.approx(0.0)
    assert out["overlap_normalized"] == pytest.approx(1.0)
    assert out["holdings_jaccard"] == pytest.approx(1.0)
    assert out["n_common_holdings"] == 2


def test_active_share_preserves_shorts_and_low_net_coverage():
    out = asm.compute_active_share_from_weights(
        {"A": -0.40, "B": 0.427175},
        {"A": 0.20, "C": 0.801654},
    )
    assert out["fund_cusip_coverage_nav"] == pytest.approx(0.027175)
    assert out["overlap_nav_raw"] == pytest.approx(-0.40)
    assert out["overlap_normalized"] < 0
    assert out["active_share_normalized"] > 1
    assert out["n_fund_only"] == 1
    assert out["n_benchmark_only"] == 1


def test_strategy_benchmark_registry_covers_observed_live_families():
    assert asm.STRATEGY_BENCHMARK_TICKERS[("equity", "Large Blend")] == "IVV"
    assert asm.STRATEGY_BENCHMARK_TICKERS[("fixed_income", "Government Bond")] == "GOVT"
    assert asm.STRATEGY_BENCHMARK_TICKERS[("alternatives", "Alternative")] == "QAI"
    assert asm.ASSET_CLASS_BENCHMARK_TICKERS["cash"] == "BIL"

