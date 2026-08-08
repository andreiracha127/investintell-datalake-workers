"""DB-free contracts for the static FF17 issuer-sector source resolver."""
from __future__ import annotations

from datetime import date

import pytest

from src.bonds.errors import BondError
from src.bonds import panel_sources


@pytest.mark.parametrize(
    ("sic", "expected_ff17"),
    [
        (100, 1), (5191, 1),
        (1000, 2), (5052, 2),
        (1300, 3), (5172, 3),
        (2200, 4), (5139, 4),
        (2510, 5), (5099, 5),
        (2800, 6), (5169, 6),
        (2100, 7), (5194, 7),
        (800, 8), (5251, 8),
        (3300, 9), (3399, 9),
        (3410, 10), (3499, 10),
        (3510, 11), (5081, 11),
        (3710, 12), (5599, 12),
        (3713, 13), (4789, 13),
        (4900, 14), (4942, 14),
        (5260, 15), (5999, 15),
        (6010, 16), (6799, 16),
        (2520, 17), (7549, 17), (8999, 17),
    ],
)
def test_canonical_sic_to_ff17_maps_official_bucket_boundaries(sic: int, expected_ff17: int) -> None:
    """A missing/wrong official range must not silently change factor exposure."""
    resolution = panel_sources.resolve_sic_to_ff17(sic)
    assert resolution.ff17num == expected_ff17
    assert resolution.reason is None


def test_canonical_sic_to_ff17_only_emits_other_for_explicit_official_other_ranges() -> None:
    # 2835 is explicitly Other; 2069 is not listed in the French definition.
    assert panel_sources.resolve_sic_to_ff17(2835).ff17num == 17
    unresolved = panel_sources.resolve_sic_to_ff17(2069)
    assert unresolved.ff17num is None
    assert unresolved.reason == "sic_not_in_ff17_definition"


@pytest.mark.parametrize("value, reason", [(None, "missing_sic"), ("abc", "invalid_sic"), (0, "invalid_sic"), (10000, "invalid_sic")])
def test_canonical_sic_to_ff17_refuses_missing_or_invalid_sic(value: object, reason: str) -> None:
    resolution = panel_sources.resolve_sic_to_ff17(value)
    assert resolution.ff17num is None
    assert resolution.reason == reason


def test_modal_osbap_resolver_uses_lowest_ff17_for_a_tie_and_keeps_disagreement_evidence() -> None:
    resolution = panel_sources.resolve_modal_ff17([8, 4, 8, 4])
    assert resolution.ff17num == 4
    assert resolution.disagreement_count == 2
    assert resolution.reason is None


def test_modal_osbap_resolver_rejects_invalid_values_instead_of_defaulting_sector() -> None:
    resolution = panel_sources.resolve_modal_ff17([None, 0, "x"])
    assert resolution.ff17num is None
    assert resolution.disagreement_count == 0
    assert resolution.reason == "no_valid_ff17num"


def test_panel_cusip_requires_exact_cusip9_never_a_cusip6_repair() -> None:
    with pytest.raises(BondError) as excinfo:
        panel_sources.normalize_cusip9("037833")
    assert excinfo.value.code == "invalid_cusip9"


def test_liquidity_resolver_normalizes_a_closed_month_and_preserves_real_quote_volume() -> None:
    resolved = panel_sources.resolve_monthly_liquidity(
        "037833100", "2025-01-31", 12.5, 18, 1_250_000.0
    )

    assert resolved.cusip9 == "037833100"
    assert resolved.month == date(2025, 1, 1)
    assert resolved.rel_bid_ask_bps == 12.5
    assert resolved.quoted_days == 18
    assert resolved.dollar_volume == 1_250_000.0
    assert resolved.quote_state == "quoted"
    assert resolved.reason_code == "valid_quote_valid_dollar_volume"


def test_liquidity_resolver_demotes_crossed_or_missing_quotes_without_fabricating_zero() -> None:
    crossed = panel_sources.resolve_monthly_liquidity(
        "037833100", "2025-01", -0.1, 12, 99.0
    )
    missing = panel_sources.resolve_monthly_liquidity(
        "037833100", "2025-01", None, 12, 99.0
    )

    assert (crossed.rel_bid_ask_bps, crossed.quoted_days, crossed.quote_state, crossed.reason_code) == (
        None, 0, "unquoted", "crossed_rel_bid_ask_bps"
    )
    assert (missing.rel_bid_ask_bps, missing.quoted_days, missing.quote_state, missing.reason_code) == (
        None, 0, "unquoted", "missing_rel_bid_ask_bps"
    )


def test_liquidity_resolver_preserves_a_valid_quote_when_dollar_volume_is_unavailable() -> None:
    resolved = panel_sources.resolve_monthly_liquidity(
        "037833100", "2025-01", 10.0, 6, float("nan")
    )

    assert resolved.rel_bid_ask_bps == 10.0
    assert resolved.dollar_volume is None
    assert resolved.quote_state == "quoted"
    assert resolved.reason_code == "valid_quote_invalid_dollar_volume"


@pytest.mark.parametrize("quoted_days", [6, "6"])
def test_liquidity_resolver_accepts_integer_or_canonical_integer_string_quoted_days(quoted_days: object) -> None:
    resolved = panel_sources.resolve_monthly_liquidity(
        "037833100", "2025-01", 10.0, quoted_days, 5.0
    )
    assert (resolved.quote_state, resolved.quoted_days, resolved.reason_code) == (
        "quoted", 6, "valid_quote_valid_dollar_volume"
    )


def test_liquidity_resolver_keeps_valid_quote_when_quoted_days_is_invalid_with_an_explicit_reason() -> None:
    resolved = panel_sources.resolve_monthly_liquidity(
        "037833100", "2025-01", 10.0, "six", 5.0
    )
    assert (resolved.rel_bid_ask_bps, resolved.quote_state, resolved.quoted_days) == (10.0, "quoted", 0)
    assert resolved.reason_code == "valid_quote_invalid_quoted_days_valid_dollar_volume"


def test_closed_month_normalizer_refuses_current_or_future_month_deterministically() -> None:
    with pytest.raises(BondError) as excinfo:
        panel_sources.normalize_closed_month("2025-05", today=date(2025, 5, 20))
    assert excinfo.value.code == "open_or_future_month"


@pytest.mark.parametrize(
    ("cusip9", "month", "reason"),
    [("037833", "2025-01", "invalid_cusip9"), ("037833100", "bad", "invalid_month")],
)
def test_liquidity_resolver_refuses_invalid_identity_fields(cusip9: str, month: str, reason: str) -> None:
    with pytest.raises(BondError) as excinfo:
        panel_sources.resolve_monthly_liquidity(cusip9, month, 1.0, 1, 1.0)
    assert excinfo.value.code == reason
