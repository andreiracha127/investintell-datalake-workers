"""Pre-registered bond-panel research inputs.  This dictionary is immutable by policy."""

from __future__ import annotations

import hashlib
import json


FROZEN = {
    "clock": {"signal_month": "t", "execution_month": "t+1", "return_window": "t+1->t+2"},
    "spread_definition": "ytm_minus_interpolated_dgs",
    "signal_name": "yield_spread_residual",
    "signal_window_start": "2013-01-01",
    "holdout_start": "2023-04-01",
    "rv_polarity": "+residual=cheap=long",
    "spread_winsor_bps": [1.0, 3000.0],
    "official_returns": "raw",
    "kill_gates": {"min_mean_monthly_ic": 0.02, "min_newey_west_t": 2.0, "min_q5_q1_net_annualized": 0.0},
    "cost_unquoted_floor": "expanding_p75_halfspread_by_liquidity_tercile",
    "recovery_rate": 0.40,
    "book_label": "small_notional_research_backtest",
    "vol_target_annual": 0.05,
    "distribution_rule": "reg_s_explicit_mapping_v1",
}


def config_hash() -> str:
    return hashlib.sha256(json.dumps(FROZEN, sort_keys=True).encode()).hexdigest()[:16]
