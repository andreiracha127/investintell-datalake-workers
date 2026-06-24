# tests/test_macro_sources.py
from src.macro_sources import SEED_SOURCES, SOURCE_SPEC_VERSION, axis_weights


def test_seed_has_both_axes_with_3_to_5_families_each() -> None:
    for axis in ("growth", "inflation"):
        specs = [s for s in SEED_SOURCES if s.axis == axis]
        families = {s.family for s in specs}
        assert 3 <= len(families) <= 5, f"{axis}: {families}"


def test_weights_normalize_to_one_per_axis() -> None:
    for axis in ("growth", "inflation"):
        w = axis_weights(axis)
        assert abs(sum(w.values()) - 1.0) < 1e-9


def test_macro_sources_are_vintage_policy_and_versioned() -> None:
    assert SOURCE_SPEC_VERSION
    for s in SEED_SOURCES:
        assert s.revision_policy == "vintage"
        assert s.direction in (-1, 1)
        assert s.cadence in ("daily", "weekly", "monthly", "quarterly")
        assert s.source_spec_version == SOURCE_SPEC_VERSION


def test_series_ids_unique() -> None:
    ids = [s.series_id for s in SEED_SOURCES]
    assert len(ids) == len(set(ids))
