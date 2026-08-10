from src.bonds.panel_config import FROZEN, config_hash


def test_frozen_config_hash_is_the_preregistered_literal() -> None:
    assert config_hash() == "180a82b3f1413d43"
    assert FROZEN["clock"] == {
        "signal_month": "t", "execution_month": "t+1", "return_window": "t+1->t+2"
    }
    assert FROZEN["spread_definition"] == "ytm_minus_interpolated_dgs"
    assert FROZEN["signal_name"] == "yield_spread_residual"
    assert FROZEN["distribution_rule"] == "reg_s_explicit_mapping_v1"
