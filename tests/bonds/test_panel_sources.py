"""DB-free contracts for the static FF17 issuer-sector source resolver."""
from __future__ import annotations

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
