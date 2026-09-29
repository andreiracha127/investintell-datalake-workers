"""W2b: full-grid public rating resolution (public_ratings.py). SYNTHETIC data only."""

from __future__ import annotations

import datetime as dt
import importlib.util
import random
from pathlib import Path

import pytest

from src.bonds.default_events import contracts as c
from src.bonds.default_events import public_ratings as pr
from src.bonds.default_events.publication import check_bundle

HERE = Path(__file__).resolve().parent / "fixtures" / "bond_default_events" / "public_ratings"


def _load_builders():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location("bond_default_public_ratings_builders", HERE / "builders.py")
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


b = _load_builders()
syn = b.syn
UTC = dt.timezone.utc
VIEWS = ("effective_audit", "public_pit")
X, Y, Z, W = b.cusip(61), b.cusip(62), b.cusip(63), b.cusip(64)
D = dt.date


def _keys(res) -> set[tuple[str, dt.date, str]]:  # type: ignore[no-untyped-def]
    return {(r.cusip_id, r.month, r.view_kind) for r in res.rows}


def _row(res, cusip9, month, view):  # type: ignore[no-untyped-def]
    return res.by_key()[(cusip9, month, view)]


# ---------------------------------------------------------------------------
# Full grid and unrated baselines
# ---------------------------------------------------------------------------
def test_full_grid_has_exactly_one_row_per_key_and_view_including_excluded_issues_and_boundary() -> None:
    boundary_start = D(2021, 8, 1)  # T=2026-08: first of the 61 snapshot keys
    keys = (
        b.grid([X], b.months(boundary_start, 3))  # retained issue, boundary start month included
        + [(Y, D(2026, 8, 1))]  # target month only
        + [(Z, boundary_start), (Z, D(2023, 2, 1))]  # excluded (ineligible) issue: never filtered
    )
    a = b.action("full-1", X, symbol="A-", on=D(2021, 9, 3), public=b.at(D(2021, 9, 4)))
    res = b.resolve([*keys, keys[0]], observations=[a])  # duplicate grid key is one key
    expected = {(x, m, v) for x, m in keys for v in VIEWS}
    assert _keys(res) == expected and len(res.rows) == len(expected)
    assert [r.key() for r in res.rows] == sorted(r.key() for r in res.rows)
    assert res.stats["grid_keys"] == len(keys) and res.stats["rows"] == len(expected)

    pit_only = b.resolve(keys, observations=[a], views=("public_pit",))
    assert _keys(pit_only) == {(x, m, "public_pit") for x, m in keys}


def test_zero_approved_rating_input_leaves_every_key_missing() -> None:
    keys = b.grid([X, Y], b.months(D(2021, 8, 1), 61))
    res = pr.build_full_grid_ratings(keys, knowledge_cutoff=b.K)  # strict default: no issues, no raise
    assert len(res.rows) == len(keys) * 2 and res.issues == ()
    assert {r.state for r in res.rows} == {"missing"}
    assert all(r.bucket is None and r.agency_source_ids == () and r.action_date is None
               and r.public_known_at is None and r.action_input_digest is None for r in res.rows)


def test_uncleared_agency_material_marks_covered_keys_rights_unverified() -> None:
    mirror = pr.UnclearedRatingSource("SYNTHETIC-MIRROR-1", "unverified", D(2021, 1, 1), D(2025, 8, 31))
    res = b.resolve(b.grid([X], [D(2025, 7, 1), D(2025, 8, 1), D(2025, 9, 1)]), uncleared=[mirror])
    for view in VIEWS:
        assert b.state_map(res, X, view) == {
            D(2025, 7, 1): ("rights_unverified", None), D(2025, 8, 1): ("rights_unverified", None),
            D(2025, 9, 1): ("missing", None),
        }
    with pytest.raises(pr.RatingResolveError, match="not_approved"):
        pr.UnclearedRatingSource("SYNTHETIC-MIRROR-2", "approved")


def test_agency_action_outside_an_approved_package_is_unused_and_reported() -> None:
    o = b.action("unapproved-1", X, on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)), package=b.EDGAR)
    keys = b.grid([X, Y], [D(2024, 2, 1)])
    res = b.resolve(keys, packages=(b.PKG, b.EDGAR), observations=[o])
    assert b.state_map(res, X, "effective_audit") == {D(2024, 2, 1): ("rights_unverified", None)}
    assert b.state_map(res, Y, "effective_audit") == {D(2024, 2, 1): ("missing", None)}
    assert [i.reason for i in res.issues] == ["agency_action_rights_not_approved"]
    with pytest.raises(pr.RatingResolveError) as err:
        b.resolve(keys, packages=(b.PKG,), observations=[o], strict=True)
    assert err.value.code == "rating_resolution_failed"
    assert [i.reason for i in err.value.issues] == ["agency_action_package_outside_inventory"]


# ---------------------------------------------------------------------------
# Views, timing and K
# ---------------------------------------------------------------------------
def test_public_pit_requires_public_knowledge_before_the_month_boundary() -> None:
    a = b.action("pit-1", X, symbol="BBB-", on=D(2024, 3, 5), public=b.at(D(2024, 4, 2)))
    res = b.resolve(b.grid([X], [D(2024, 3, 1), D(2024, 4, 1)]), observations=[a])
    assert b.state_map(res, X, "effective_audit") == {
        D(2024, 3, 1): ("observed", "BBB"), D(2024, 4, 1): ("carried_verified", "BBB"),
    }
    assert b.state_map(res, X, "public_pit") == {
        D(2024, 3, 1): ("pit_unverified", None), D(2024, 4, 1): ("carried_verified", "BBB"),
    }
    unverified = _row(res, X, D(2024, 3, 1), "public_pit")
    assert unverified.agency_source_ids == () and unverified.public_known_at is None
    carried = _row(res, X, D(2024, 4, 1), "public_pit")
    assert carried.action_date == D(2024, 3, 5) and carried.public_known_at == a.public_available_at


def test_effective_audit_and_public_pit_diverge_on_lagged_rating_files() -> None:
    pkg = b.agency_package("SYNTHETIC-ROCR-LAGGED", effective=(D(2020, 1, 1), D(2024, 1, 31)),
                           public=(D(2024, 2, 15), D(2024, 3, 31)))
    a = b.action("lag-1", X, symbol="Ba2", on=D(2023, 3, 5), public=b.at(D(2024, 2, 15)), package=pkg)
    keys = b.grid([X], [D(2023, 3, 1), D(2023, 4, 1), D(2024, 1, 1), D(2024, 2, 1)])
    res = b.resolve(keys, packages=(pkg,), observations=[a])
    assert b.state_map(res, X, "effective_audit") == {
        D(2023, 3, 1): ("observed", "BB"), D(2023, 4, 1): ("carried_verified", "BB"),
        D(2024, 1, 1): ("carried_verified", "BB"), D(2024, 2, 1): ("stale", None),
    }
    assert b.state_map(res, X, "public_pit") == {
        D(2023, 3, 1): ("pit_unverified", None), D(2023, 4, 1): ("pit_unverified", None),
        D(2024, 1, 1): ("pit_unverified", None), D(2024, 2, 1): ("carried_verified", "BB"),
    }
    audit, pit = _row(res, X, D(2024, 1, 1), "effective_audit"), _row(res, X, D(2024, 2, 1), "public_pit")
    assert audit.coverage_frontier == D(2024, 1, 31) and pit.coverage_frontier == D(2024, 3, 31)
    assert audit.action_input_digest != pit.action_input_digest


def test_knowledge_cutoff_bounds_public_time_and_ingestion() -> None:
    keys = b.grid([X], [D(2024, 2, 1)])
    late = b.action("k-1", X, on=D(2024, 1, 10), public=b.at(D(2026, 9, 26)))
    res = b.resolve(keys, observations=[late])
    assert {r.state for r in res.rows} == {"missing"}
    assert res.stats["excluded:observation_public_after_cutoff"] == 1

    unseen = b.action("k-2", X, on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)), first_seen=b.at(D(2026, 9, 26)))
    assert {r.state for r in b.resolve(keys, observations=[unseen]).rows} == {"missing"}
    rebuilt = b.resolve(keys, observations=[unseen], knowledge_mode="historical_reconstruction")
    assert {(r.state, r.bucket) for r in rebuilt.rows} == {("carried_verified", "BB")}
    earlier_k = b.resolve(keys, observations=[unseen], k=b.at(D(2024, 1, 10)),
                          knowledge_mode="historical_reconstruction")
    assert {r.state for r in earlier_k.rows} == {"missing"}


# ---------------------------------------------------------------------------
# Revision admissibility by knowledge mode (F2)
# ---------------------------------------------------------------------------
REV_MONTH = D(2024, 5, 1)
BEFORE_K, AFTER_K = dt.datetime(2026, 9, 24, tzinfo=UTC), dt.datetime(2026, 9, 26, tzinfo=UTC)


def _original(cusip9: str = X) -> c.CreditObservation:
    return b.action("orig-1", cusip9, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)),
                    first_seen=b.at(D(2024, 5, 3)))


def test_current_run_ignores_an_uningested_correction() -> None:
    original = _original()
    correction = b.action("corr-1", X, symbol="B", on=D(2024, 5, 2), public=b.at(D(2024, 5, 20)),
                          first_seen=AFTER_K, revision="correction", supersedes=original)
    keys = b.grid([X], [REV_MONTH])
    current = b.resolve(keys, observations=[original, correction])
    assert {(r.state, r.bucket) for r in current.rows} == {("observed", "BB")}
    assert current.stats["excluded:observation_not_ingested_by_cutoff"] == 1
    rebuilt = b.resolve(keys, observations=[original, correction], knowledge_mode="historical_reconstruction")
    assert {(r.state, r.bucket) for r in rebuilt.rows} == {("observed", "B")}


# (mode, public_available_at, first_seen_at, revision counts?)
_ADMISSIBILITY = [
    ("current_run", b.at(D(2024, 5, 20)), BEFORE_K, True),
    ("current_run", b.at(D(2024, 5, 20)), AFTER_K, False),
    ("current_run", AFTER_K, BEFORE_K, False),
    ("historical_reconstruction", b.at(D(2024, 5, 20)), BEFORE_K, True),
    ("historical_reconstruction", b.at(D(2024, 5, 20)), AFTER_K, True),
    ("historical_reconstruction", AFTER_K, BEFORE_K, False),
]


@pytest.mark.parametrize(("mode", "public", "seen", "counts"), _ADMISSIBILITY)
def test_correction_admissibility_on_both_sides_of_both_boundaries(
    mode: str, public: dt.datetime, seen: dt.datetime, counts: bool,
) -> None:
    original = _original()
    correction = b.action("corr-2", X, symbol="B", on=D(2024, 5, 2), public=public, first_seen=seen,
                          revision="correction", supersedes=original)
    res = b.resolve(b.grid([X], [REV_MONTH]), observations=[original, correction], knowledge_mode=mode)
    expected = "B" if counts else "BB"
    assert {(r.state, r.bucket) for r in res.rows if r.view_kind == "effective_audit"} == {("observed", expected)}


@pytest.mark.parametrize(("mode", "public", "seen", "counts"), _ADMISSIBILITY)
def test_retraction_admissibility_on_both_sides_of_both_boundaries(
    mode: str, public: dt.datetime, seen: dt.datetime, counts: bool,
) -> None:
    original = _original()
    retraction = b.action("retr-1", X, symbol="BB", on=D(2024, 5, 2), public=public, first_seen=seen,
                          revision="retraction", supersedes=original)
    res = b.resolve(b.grid([X], [REV_MONTH]), observations=[original, retraction], knowledge_mode=mode)
    expected = ("missing", None) if counts else ("observed", "BB")
    assert {(r.state, r.bucket) for r in res.rows} == {expected}


@pytest.mark.parametrize(("mode", "public", "seen", "counts"), _ADMISSIBILITY)
def test_fork_detection_counts_only_admissible_revisions(
    mode: str, public: dt.datetime, seen: dt.datetime, counts: bool,
) -> None:
    original = _original()
    first = b.action("fork-1", X, symbol="B", on=D(2024, 5, 2), public=b.at(D(2024, 5, 20)), first_seen=BEFORE_K,
                     revision="correction", supersedes=original)
    second = b.action("fork-2", X, symbol="B-", on=D(2024, 5, 2), public=public, first_seen=seen,
                      revision="correction", supersedes=original)
    args = {"observations": [original, first, second], "knowledge_mode": mode, "strict": False}
    if counts:
        with pytest.raises(pr.RatingResolveError) as err:
            b.resolve(b.grid([X], [REV_MONTH]), **args)
        assert err.value.code == "observation_revision_fork" and err.value.issues == (str(original.observation_id),)
    else:
        res = b.resolve(b.grid([X], [REV_MONTH]), **args)
        assert {(r.state, r.bucket) for r in res.rows if r.view_kind == "effective_audit"} == {("observed", "B")}


def test_supersession_cycle_fails_loud() -> None:
    a_ref = b.action("cyc-a", X, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    b_row = b.action("cyc-b", X, symbol="B", on=D(2024, 5, 2), public=b.at(D(2024, 5, 4)),
                     revision="correction", supersedes=a_ref)
    a_row = b.action("cyc-a", X, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)),
                     revision="correction", supersedes=b_row)
    with pytest.raises(pr.RatingResolveError) as err:
        b.resolve(b.grid([X], [REV_MONTH]), observations=[a_row, b_row], strict=False)
    assert err.value.code == "observation_supersession_cycle"


def test_package_eligibility_precedes_revision_processing() -> None:
    original = _original()
    unapproved = b.action("elig-1", X, symbol="CCC", on=D(2024, 5, 2), public=b.at(D(2024, 5, 20)),
                          package=b.EDGAR, revision="correction", supersedes=original)
    res = b.resolve(b.grid([X], [REV_MONTH]), packages=(b.PKG, b.EDGAR), observations=[original, unapproved])
    assert {(r.state, r.bucket) for r in res.rows} == {("observed", "BB")}
    assert [i.reason for i in res.issues] == ["agency_action_rights_not_approved"]


# ---------------------------------------------------------------------------
# Carry-forward, coverage frontier, withdrawals
# ---------------------------------------------------------------------------
def test_carry_forward_is_verified_only_within_the_relied_coverage_frontier() -> None:
    pkg = b.agency_package("SYNTHETIC-ROCR-SHORT", effective=(D(2020, 1, 1), D(2024, 6, 30)),
                           public=(D(2020, 1, 1), D(2024, 6, 30)))
    a = b.action("carry-1", X, symbol="B", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)), package=pkg)
    res = b.resolve(b.grid([X], b.months(D(2024, 1, 1), 8)), packages=(pkg,), observations=[a])
    for view in VIEWS:
        states = b.state_map(res, X, view)
        assert states[D(2024, 1, 1)] == ("observed", "B")
        assert all(states[m] == ("carried_verified", "B") for m in b.months(D(2024, 2, 1), 5))
        assert states[D(2024, 7, 1)] == states[D(2024, 8, 1)] == ("stale", None)
    stale = _row(res, X, D(2024, 8, 1), "public_pit")
    assert stale.action_date == D(2024, 1, 10) and stale.coverage_frontier == D(2024, 6, 30)
    assert stale.agency_source_ids == b.ids(a)
    carried = [_row(res, X, m, "effective_audit") for m in b.months(D(2024, 2, 1), 5)]
    assert len({r.action_input_digest for r in carried}) == 1


def test_action_before_verified_coverage_start_is_stale_not_carried() -> None:
    pkg = b.agency_package("SYNTHETIC-ROCR-LATE-START", effective=(D(2023, 1, 1), D(2026, 6, 30)),
                           public=(D(2023, 1, 1), D(2026, 6, 30)))
    a = b.action("start-1", X, symbol="A", on=D(2022, 5, 1), public=b.at(D(2022, 5, 2)), package=pkg)
    res = b.resolve(b.grid([X], [D(2022, 5, 1), D(2023, 6, 1)]), packages=(pkg,), observations=[a])
    assert {(r.state, r.bucket) for r in res.rows} == {("stale", None)}


def test_no_2025_03_cutoff_in_the_new_path() -> None:
    a = b.action("recent-1", X, symbol="BB-", on=D(2025, 5, 20), public=b.at(D(2025, 5, 21)))
    res = b.resolve(b.grid([X], [D(2025, 3, 1), D(2025, 5, 1), D(2026, 6, 1)]), observations=[a])
    assert b.state_map(res, X, "public_pit") == {
        D(2025, 3, 1): ("missing", None), D(2025, 5, 1): ("observed", "BB"),
        D(2026, 6, 1): ("carried_verified", "BB"),
    }


def test_withdrawals_are_unrated_and_rac_wd_never_becomes_bucket_d() -> None:
    rated = b.action("wd-0", X, symbol="BB", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    rac_wd = b.action("wd-1", X, symbol="D", rac="WD", on=D(2024, 3, 15), public=b.at(D(2024, 3, 16)))
    symbol_wd = b.action("wd-2", Y, symbol="WD", rac="DG", on=D(2024, 3, 15), public=b.at(D(2024, 3, 16)))
    moody_wr = b.action("wd-3", Z, symbol="WR", rac=None, on=D(2024, 3, 15), public=b.at(D(2024, 3, 16)))
    rerated = b.action("wd-4", X, symbol="BB+", rac="NW", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    keys = b.grid([X, Y, Z], b.months(D(2024, 1, 1), 5))
    res = b.resolve(keys, observations=[rated, rac_wd, symbol_wd, moody_wr, rerated])
    assert b.state_map(res, X, "public_pit") == {
        D(2024, 1, 1): ("observed", "BB"), D(2024, 2, 1): ("carried_verified", "BB"),
        D(2024, 3, 1): ("withdrawn", None), D(2024, 4, 1): ("withdrawn", None),
        D(2024, 5, 1): ("observed", "BB"),
    }
    for cusip9 in (Y, Z):
        assert b.state_map(res, cusip9, "effective_audit")[D(2024, 4, 1)] == ("withdrawn", None)
    assert _row(res, X, D(2024, 4, 1), "effective_audit").agency_source_ids == b.ids(rac_wd)
    assert all(r.bucket != "D" and r.default_overlay_episode_id is None for r in res.rows)
    assert res.issues == ()


@pytest.mark.parametrize(("symbol", "rac", "expected"), [
    ("Aaa", "AF", ("rated", "AAA")), ("Aa2", "AF", ("rated", "AA")), ("A3", "AF", ("rated", "A")),
    ("Baa3", "AF", ("rated", "BBB")), ("Ba1", "AF", ("rated", "BB")), ("B2", "AF", ("rated", "B")),
    ("Caa1", "AF", ("rated", "CCC")), ("Ca", "DG", ("rated", "CCC")), ("C", "DG", ("rated", "CCC")),
    ("AA+", "AF", ("rated", "AA")), ("BBB-", "AF", ("rated", "BBB")), ("BB (high)", "AF", ("rated", "BB")),
    ("CC", "DG", ("rated", "CCC")), ("D", "DG", ("rated", "D")),
    # Moody's generic (unmodified) symbols.
    ("Caa", "DG", ("rated", "CCC")), ("Ba", "AF", ("rated", "BB")), ("Baa", "AF", ("rated", "BBB")),
    ("Aa", "AF", ("rated", "AA")),
    ("SD", "DG", ("unmapped", None)), ("RD", "DG", ("unmapped", None)), ("(P)Baa1", "PR", ("unmapped", None)),
    ("A-1+", "AF", ("unmapped", None)), ("BBB+ *-", "AF", ("unmapped", None)), (None, "AF", ("unmapped", None)),
    ("WR", None, ("withdrawn", None)), ("NR", "AF", ("withdrawn", None)), ("D", "WD", ("withdrawn", None)),
    ("BB", "WO", ("withdrawn", None)), ("BB", "WE", ("withdrawn", None)),
])
def test_symbol_classification(symbol: str | None, rac: str | None, expected: tuple[str, str | None]) -> None:
    o = b.action("cls-1", X, symbol=symbol, rac=rac, on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    assert pr.classify_agency_action(o) == expected


# ---------------------------------------------------------------------------
# Subject level, scope, mapping, conflicts
# ---------------------------------------------------------------------------
def test_issuer_level_default_never_rates_an_instrument_and_only_instrument_d_is_d() -> None:
    issuer_sd = b.action("iss-1", None, symbol="SD", subject="issuer", on=D(2024, 2, 1), public=b.at(D(2024, 2, 2)))
    issuer_d = b.action("iss-2", X, symbol="D", subject="issuer", on=D(2024, 2, 1), public=b.at(D(2024, 2, 2)))
    scoped = b.link(issuer_sd, X, scope="issuer_affected_obligation")
    instrument_d = b.action("ins-1", Y, symbol="D", on=D(2024, 2, 1), public=b.at(D(2024, 2, 2)))
    instrument_sd = b.action("ins-2", Z, symbol="SD", on=D(2024, 2, 1), public=b.at(D(2024, 2, 2)))
    moody_ca = b.action("ins-3", W, symbol="Ca", rac="DG", on=D(2024, 2, 1), public=b.at(D(2024, 2, 2)))
    res = b.resolve(b.grid([X, Y, Z, W], [D(2024, 2, 1)]),
                    observations=[issuer_sd, issuer_d, instrument_d, instrument_sd, moody_ca], links=[scoped])
    month = D(2024, 2, 1)
    assert b.state_map(res, X, "public_pit") == {month: ("missing", None)}
    assert b.state_map(res, Y, "public_pit") == {month: ("observed", "D")}
    assert b.state_map(res, Z, "public_pit") == {month: ("missing", None)}
    assert b.state_map(res, W, "public_pit") == {month: ("observed", "CCC")}
    assert res.stats["excluded:issuer_level_action"] == 2
    assert {(i.reason, i.cusip_id) for i in res.issues} == {("rating_symbol_unmapped", Z)}


def test_undeclared_rating_scope_is_reported_and_short_term_is_excluded() -> None:
    long_b = b.action("scope-1", X, symbol="BB", on=D(2024, 2, 1), public=b.at(D(2024, 2, 2)))
    short_b = b.action("scope-2", Y, symbol="B", rating_type="short_term", on=D(2024, 2, 1), public=b.at(D(2024, 2, 2)))
    keys = b.grid([X, Y], [D(2024, 2, 1)])
    undeclared = b.resolve(keys, observations=[long_b], scopes=())
    assert {r.state for r in undeclared.rows} == {"missing"}
    assert [i.reason for i in undeclared.issues] == ["rating_scope_undeclared"]
    declared = b.resolve(keys, observations=[long_b, short_b])
    assert b.state_map(declared, Y, "effective_audit") == {D(2024, 2, 1): ("missing", None)}
    assert b.state_map(declared, X, "effective_audit") == {D(2024, 2, 1): ("observed", "BB")}
    assert declared.issues == () and declared.stats["excluded:rating_scope"] == 1


def test_same_day_conflict_has_no_winner_and_is_reported() -> None:
    first = b.action("conf-1", X, symbol="BB", rac="DG", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    second = b.action("conf-2", X, symbol="B", rac="DG", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    earlier = b.action("conf-0", X, symbol="BBB", on=D(2024, 1, 5), public=b.at(D(2024, 1, 6)))
    keys = b.grid([X], [D(2024, 4, 1), D(2024, 5, 1), D(2024, 6, 1)])
    res = b.resolve(keys, observations=[earlier, first, second])
    for view in VIEWS:
        assert b.state_map(res, X, view) == {
            D(2024, 4, 1): ("carried_verified", "BBB"), D(2024, 5, 1): ("missing", None),
            D(2024, 6, 1): ("missing", None),
        }
    assert {(i.reason, i.month, i.view_kind) for i in res.issues} == {
        ("same_day_conflict", m, v) for m in (D(2024, 5, 1), D(2024, 6, 1)) for v in VIEWS
    }
    assert all(i.observation_ids == b.ids(first, second) for i in res.issues)
    with pytest.raises(pr.RatingResolveError) as err:
        b.resolve(keys, observations=[earlier, first, second], strict=True)
    assert len(err.value.issues) == 4


def test_duplicate_records_and_revisions_are_not_conflicts() -> None:
    in_pkg1 = b.action("dup-1", X, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    in_pkg2 = b.action("dup-1", X, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 4)), package=b.PKG_2)
    original = b.action("rev-0", Y, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    corrected = b.action("rev-1", Y, symbol="B", on=D(2024, 5, 2), public=b.at(D(2024, 5, 20)),
                         revision="correction", supersedes=original)
    withdrawn_record = b.action("rev-3", Z, symbol="A", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    retraction = b.action("rev-4", Z, symbol="A", on=D(2024, 5, 2), public=b.at(D(2024, 5, 25)),
                          revision="retraction", supersedes=withdrawn_record)
    res = b.resolve(b.grid([X, Y, Z], [D(2024, 5, 1)]),
                    observations=[in_pkg1, in_pkg2, original, corrected, withdrawn_record, retraction])
    month = D(2024, 5, 1)
    dup = _row(res, X, month, "effective_audit")
    assert (dup.state, dup.bucket, dup.agency_source_ids) == ("observed", "BB", b.ids(in_pkg1, in_pkg2))
    assert dup.public_known_at == in_pkg2.public_available_at
    for view in VIEWS:
        assert b.state_map(res, Y, view) == {month: ("observed", "B")}
        assert b.state_map(res, Z, view) == {month: ("missing", None)}
    assert res.issues == ()


def test_bare_moodys_caa_resolves_to_ccc() -> None:
    caa = b.action("caa-1", X, symbol="Caa", rac="DG", on=D(2024, 2, 5), public=b.at(D(2024, 2, 6)))
    res = b.resolve(b.grid([X], [D(2024, 2, 1)]), observations=[caa])
    assert {(r.state, r.bucket) for r in res.rows} == {("observed", "CCC")} and res.issues == ()


# ---------------------------------------------------------------------------
# Links: month-end snapshot validity, known time, revisions, digest (F1, F4)
# ---------------------------------------------------------------------------
EARLY = b.at(D(2024, 1, 5))


def test_link_binds_only_when_valid_at_the_month_end_snapshot() -> None:
    unlabelled = b.action("lnk-1", None, symbol="A+", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    other = b.action("lnk-2", Y, symbol="CCC", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    links = [
        b.link(unlabelled, X, valid_to=D(2024, 2, 15), known=EARLY),  # expires mid-February
        b.link(unlabelled, Z, valid_to=D(2024, 2, 29), known=EARLY),  # ends exactly at February's end
        b.link(unlabelled, W, valid_from=D(2024, 2, 15), known=EARLY),  # starts after the action
        b.link(unlabelled, Y, status="quarantined", known=EARLY),
        b.link(other, X, known=EARLY),  # contradicts the action's own CUSIP: ignored
    ]
    res = b.resolve(b.grid([X, Y, Z, W], b.months(D(2024, 1, 1), 3)), observations=[unlabelled, other], links=links)
    for view in VIEWS:
        assert b.state_map(res, X, view) == {
            D(2024, 1, 1): ("observed", "A"), D(2024, 2, 1): ("missing", None), D(2024, 3, 1): ("missing", None),
        }
        assert b.state_map(res, Z, view) == {
            D(2024, 1, 1): ("observed", "A"), D(2024, 2, 1): ("carried_verified", "A"),
            D(2024, 3, 1): ("missing", None),
        }
        assert b.state_map(res, W, view) == {
            D(2024, 1, 1): ("missing", None), D(2024, 2, 1): ("carried_verified", "A"),
            D(2024, 3, 1): ("carried_verified", "A"),
        }
        assert b.state_map(res, Y, view) == {m: ("carried_verified", "CCC") for m in b.months(D(2024, 2, 1), 2)} | {
            D(2024, 1, 1): ("observed", "CCC")}


def test_public_pit_needs_the_binding_link_known_by_month_end() -> None:
    unlabelled = b.action("pitlink-1", None, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    late_link = b.link(unlabelled, X, known=dt.datetime(2026, 9, 21, 9, tzinfo=UTC))
    timely_link = b.link(unlabelled, Y, known=b.at(D(2024, 5, 20)))
    after_k = b.link(unlabelled, Z, known=dt.datetime(2026, 9, 26, tzinfo=UTC))
    keys = b.grid([X, Y, Z], [D(2024, 5, 1), D(2024, 6, 1)])
    res = b.resolve(keys, observations=[unlabelled], links=[late_link, timely_link, after_k])
    assert b.state_map(res, X, "effective_audit") == {
        D(2024, 5, 1): ("observed", "BB"), D(2024, 6, 1): ("carried_verified", "BB"),
    }
    assert b.state_map(res, X, "public_pit") == {
        D(2024, 5, 1): ("pit_unverified", None), D(2024, 6, 1): ("pit_unverified", None),
    }
    assert all(r.agency_source_ids == () and r.public_known_at is None
               for r in res.rows if r.cusip_id == X and r.view_kind == "public_pit")
    for view in VIEWS:
        assert b.state_map(res, Y, view) == {
            D(2024, 5, 1): ("observed", "BB"), D(2024, 6, 1): ("carried_verified", "BB"),
        }
        assert {r.state for r in res.rows if r.cusip_id == Z and r.view_kind == view} == {"missing"}
    assert _row(res, Y, D(2024, 5, 1), "public_pit").public_known_at == timely_link.link_known_at

    reconstructed = b.resolve(
        keys,
        observations=[unlabelled],
        links=[after_k],
        knowledge_mode="historical_reconstruction",
    )
    assert {r.state for r in reconstructed.rows if r.cusip_id == Z} == {"missing"}
    assert all(r.public_known_at is None for r in reconstructed.rows if r.cusip_id == Z)


def test_link_revision_selected_as_of_k_governs_both_views() -> None:
    unlabelled = b.action("rev-link-1", None, symbol="B+", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    original_x = b.link(unlabelled, X, known=b.at(D(2024, 5, 10)))
    revision_x = b.link(unlabelled, X, valid_from=D(2021, 1, 1), known=dt.datetime(2026, 9, 1, tzinfo=UTC),
                        supersedes=original_x)
    original_y = b.link(unlabelled, Y, known=b.at(D(2024, 5, 10)))
    later_revision_y = b.link(unlabelled, Y, status="rejected", known=dt.datetime(2026, 9, 26, tzinfo=UTC),
                              supersedes=original_y)
    original_z = b.link(unlabelled, Z, known=b.at(D(2024, 5, 10)))
    rejected_z = b.link(unlabelled, Z, status="rejected", known=dt.datetime(2026, 9, 1, tzinfo=UTC),
                        supersedes=original_z)
    month = D(2024, 5, 1)
    res = b.resolve(b.grid([X, Y, Z], [month]), observations=[unlabelled],
                    links=[original_x, revision_x, original_y, later_revision_y, original_z, rejected_z])
    # X: the selected revision is known only after the month boundary.
    assert b.state_map(res, X, "effective_audit") == {month: ("observed", "B")}
    assert b.state_map(res, X, "public_pit") == {month: ("pit_unverified", None)}
    # Y: a revision known after K is ignored; the original binds both views.
    for view in VIEWS:
        assert b.state_map(res, Y, view) == {month: ("observed", "B")}
        assert b.state_map(res, Z, view) == {month: ("missing", None)}
    assert res.stats["links:superseded"] == 2


def test_link_revision_fork_fails_loud_only_when_both_revisions_are_known() -> None:
    unlabelled = b.action("fork-link-1", None, symbol="B+", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    parent = b.link(unlabelled, X, known=b.at(D(2024, 5, 10)))
    first = b.link(unlabelled, X, valid_from=D(2021, 1, 1), known=b.at(D(2025, 1, 1)), supersedes=parent)
    known_second = b.link(unlabelled, X, valid_from=D(2022, 1, 1), known=b.at(D(2025, 2, 1)), supersedes=parent)
    late_second = b.link(unlabelled, X, valid_from=D(2022, 1, 1), known=dt.datetime(2026, 9, 26, tzinfo=UTC),
                         supersedes=parent)
    keys = b.grid([X], [D(2024, 5, 1)])
    with pytest.raises(pr.RatingResolveError) as err:
        b.resolve(keys, observations=[unlabelled], links=[parent, first, known_second], strict=False)
    assert err.value.code == "link_revision_fork" and err.value.issues == (str(parent.link_id),)
    res = b.resolve(keys, observations=[unlabelled], links=[parent, first, late_second])
    assert b.state_map(res, X, "effective_audit") == {D(2024, 5, 1): ("observed", "B")}


def test_action_input_digest_binds_the_binding_link() -> None:
    unlabelled = b.action("dig-1", None, symbol="BBB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    link_a = b.link(unlabelled, X, valid_from=D(2020, 1, 1), known=EARLY)
    link_b = b.link(unlabelled, X, valid_from=D(2021, 1, 1), known=EARLY)
    keys = b.grid([X], [D(2024, 5, 1)])
    first = b.resolve(keys, observations=[unlabelled], links=[link_a]).rows
    second = b.resolve(keys, observations=[unlabelled], links=[link_b]).rows
    both = b.resolve(keys, observations=[unlabelled], links=[link_a, link_b]).rows
    for one, two, three in zip(first, second, both):
        assert (one.state, one.bucket, one.agency_source_ids) == (two.state, two.bucket, two.agency_source_ids)
        assert len({one.action_input_digest, two.action_input_digest, three.action_input_digest}) == 3


def test_multi_agency_evidence_rates_only_on_agreement() -> None:
    a1 = b.action("ma-1", X, symbol="BBB", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    a2 = b.action("ma-2", X, symbol="Baa2", on=D(2024, 1, 20), public=b.at(D(2024, 1, 21)),
                  agency=b.AGENCY_2, package=b.PKG_2)
    y1 = b.action("ma-3", Y, symbol="BB", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    y2 = b.action("ma-4", Y, symbol="B1", on=D(2024, 1, 20), public=b.at(D(2024, 1, 21)),
                  agency=b.AGENCY_2, package=b.PKG_2)
    res = b.resolve(b.grid([X, Y], [D(2024, 2, 1)]), observations=[a1, a2, y1, y2])
    agree = _row(res, X, D(2024, 2, 1), "public_pit")
    assert (agree.state, agree.bucket, agree.agency_source_ids) == ("carried_verified", "BBB", b.ids(a1, a2))
    assert agree.action_date == D(2024, 1, 20)
    assert agree.public_known_at == max(a1.public_available_at, a2.public_available_at)
    assert b.state_map(res, Y, "public_pit") == {D(2024, 2, 1): ("missing", None)}
    assert {i.reason for i in res.issues} == {"multi_agency_composite_undefined"}


def test_same_day_distinct_raw_semantics_conflict_even_within_one_bucket() -> None:
    plus = b.action("sem-1", X, symbol="BB+", rac="DG", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    minus = b.action("sem-2", X, symbol="BB-", rac="DG", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    affirmed = b.action("sem-3", Y, symbol="BB", rac="AF", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    upgraded = b.action("sem-4", Y, symbol="BB", rac="UG", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    duplicate = b.action("sem-5", Z, symbol="BB+", rac="DG", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    duplicate_2 = b.action("sem-5", Z, symbol=" BB+", rac="dg", on=D(2024, 5, 2), public=b.at(D(2024, 5, 9)),
                           package=b.PKG_2)
    month = D(2024, 5, 1)
    res = b.resolve(b.grid([X, Y, Z], [month]), observations=[plus, minus, affirmed, upgraded, duplicate, duplicate_2])
    for view in VIEWS:
        assert b.state_map(res, X, view) == {month: ("missing", None)}
        assert b.state_map(res, Y, view) == {month: ("missing", None)}
        assert b.state_map(res, Z, view) == {month: ("observed", "BB")}
    assert {(i.reason, i.cusip_id, i.observation_ids) for i in res.issues} == {
        ("same_day_conflict", X, b.ids(plus, minus)), ("same_day_conflict", Y, b.ids(affirmed, upgraded)),
    }
    assert len(res.issues) == 4


def test_timing_and_coverage_outrank_problem_states_and_problems_accumulate() -> None:
    month = D(2024, 5, 1)
    short = b.agency_package("SYNTHETIC-ROCR-SHORT-2", effective=(D(2020, 1, 1), D(2024, 4, 30)),
                             public=(D(2020, 1, 1), D(2024, 4, 30)))
    # X: agency 1 unmapped symbol + agency 2 not public by month-end.
    x_unmapped = b.action("prec-1", X, symbol="SD", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    x_late = b.action("prec-2", X, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 6, 3)),
                      agency=b.AGENCY_2, package=b.PKG_2)
    # Y: agency 1 same-day conflict + agency 2 beyond its verified coverage (stale).
    y_1 = b.action("prec-3", Y, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    y_2 = b.action("prec-4", Y, symbol="B", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    y_stale = b.action("prec-5", Y, symbol="BB", on=D(2024, 3, 2), public=b.at(D(2024, 3, 3)),
                       agency=b.AGENCY_2, package=short)
    # Z: single-agency conflict inside a package whose coverage ends before the month end.
    z_1 = b.action("prec-6", Z, symbol="A", on=D(2024, 4, 2), public=b.at(D(2024, 4, 3)), package=short)
    z_2 = b.action("prec-7", Z, symbol="A-", on=D(2024, 4, 2), public=b.at(D(2024, 4, 3)), package=short)
    # W: agencies 1 and 2 disagree (clean) while agency 3 is not public by month-end.
    w_1 = b.action("prec-8", W, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    w_2 = b.action("prec-9", W, symbol="B1", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)),
                   agency=b.AGENCY_2, package=b.PKG_2)
    w_3 = b.action("prec-10", W, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 6, 3)),
                   agency=b.AGENCY_3, package=b.PKG_3)
    res = b.resolve(b.grid([X, Y, Z, W], [month]), packages=(b.PKG, b.PKG_2, b.PKG_3, short),
                    observations=[x_unmapped, x_late, y_1, y_2, y_stale, z_1, z_2, w_1, w_2, w_3])
    assert b.state_map(res, X, "public_pit") == {month: ("pit_unverified", None)}
    assert b.state_map(res, X, "effective_audit") == {month: ("missing", None)}
    for view in VIEWS:
        assert b.state_map(res, Y, view) == {month: ("stale", None)}
        assert _row(res, Y, month, view).agency_source_ids == b.ids(y_stale)
        assert b.state_map(res, Z, view) == {month: ("stale", None)}
        assert _row(res, Z, month, view).agency_source_ids == b.ids(z_1, z_2)
    assert b.state_map(res, W, "public_pit") == {month: ("pit_unverified", None)}
    reasons = {(i.cusip_id, i.view_kind, i.reason) for i in res.issues}
    assert reasons == {
        (X, v, "rating_symbol_unmapped") for v in VIEWS
    } | {(Y, v, "same_day_conflict") for v in VIEWS} | {(Z, v, "same_day_conflict") for v in VIEWS} | {
        (W, v, "multi_agency_composite_undefined") for v in VIEWS
    }


# ---------------------------------------------------------------------------
# Default overlay
# ---------------------------------------------------------------------------
def test_default_overlay_marks_post_onset_months_without_claiming_agency_d() -> None:
    rated = b.action("ov-1", X, symbol="BB", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    exact = b.episode(X, "ov-x", lower=D(2024, 4, 14), upper=D(2024, 4, 15))
    resolved = b.episode(Y, "ov-y", upper=D(2024, 2, 29), resolution=D(2024, 5, 10))
    late = b.episode(Z, "ov-z", upper=D(2024, 2, 1), known=b.at(D(2026, 9, 26)))
    keys = b.grid([X, Y, Z], b.months(D(2024, 1, 1), 6))
    res = b.resolve(keys, observations=[rated], episodes=[exact, resolved, late])
    for view in VIEWS:
        x_rows = {r.month: r for r in res.rows if r.cusip_id == X and r.view_kind == view}
        assert [m for m, r in x_rows.items() if r.default_overlay_episode_id == exact.episode_id] == b.months(
            D(2024, 4, 1), 3)
        assert x_rows[D(2024, 5, 1)].bucket == "BB" and x_rows[D(2024, 5, 1)].state == "carried_verified"
        y_overlay = [r.month for r in res.rows if r.cusip_id == Y and r.view_kind == view
                     and r.default_overlay_episode_id == resolved.episode_id]
        assert y_overlay == b.months(D(2024, 2, 1), 3)
    assert all(r.default_overlay_episode_id is None for r in res.rows if r.cusip_id == Z)
    assert all(r.bucket != "D" for r in res.rows)
    assert [(i.reason, i.cusip_id) for i in res.issues] == [("episode_known_after_cutoff", Z)]


def test_resolution_known_after_cutoff_does_not_end_the_overlay() -> None:
    e = b.episode(X, "ov-late-res", upper=D(2024, 2, 10), resolution=D(2024, 3, 5),
                  resolution_known=b.at(D(2026, 9, 26)))
    res = b.resolve(b.grid([X], b.months(D(2024, 2, 1), 3)), episodes=[e])
    assert {r.default_overlay_episode_id for r in res.rows} == {e.episode_id}


def test_overlapping_episodes_pick_the_earliest_onset_and_are_reported() -> None:
    first = b.episode(X, "ovl-1", upper=D(2024, 2, 10))
    second = b.episode(X, "ovl-2", lower=D(2024, 2, 29), upper=D(2024, 3, 20))
    res = b.resolve(b.grid([X], [D(2024, 2, 1), D(2024, 3, 1)]), episodes=[second, first])
    assert {r.default_overlay_episode_id for r in res.rows} == {first.episode_id}
    assert [(i.reason, i.month) for i in res.issues] == [("overlapping_default_episodes", D(2024, 3, 1))]


# ---------------------------------------------------------------------------
# Determinism and malformed inputs
# ---------------------------------------------------------------------------
def test_output_is_independent_of_input_order() -> None:
    obs = [
        b.action("ord-1", X, symbol="BB", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11))),
        b.action("ord-2", X, symbol="B", rac="DG", on=D(2024, 3, 2), public=b.at(D(2024, 4, 3))),
        b.action("ord-3", Y, symbol="A", on=D(2024, 2, 2), public=b.at(D(2024, 2, 3))),
        b.action("ord-4", Y, symbol="BBB", on=D(2024, 2, 2), public=b.at(D(2024, 2, 3))),
        b.action("ord-5", None, symbol="Baa1", on=D(2024, 1, 5), public=b.at(D(2024, 1, 6)), agency=b.AGENCY_2,
                 package=b.PKG_2),
    ]
    links = [b.link(obs[4], Z), b.link(obs[4], W, status="quarantined")]
    episodes = [b.episode(X, "ord-x", upper=D(2024, 3, 31)), b.episode(W, "ord-w", lower=D(2024, 1, 31),
                                                                        upper=D(2024, 3, 15))]
    keys = b.grid([X, Y, Z, W], b.months(D(2024, 1, 1), 5))
    base = b.resolve(keys, observations=obs, links=links, episodes=episodes)
    rng = random.Random(20260925)
    for _ in range(3):
        # Identical duplicates of every keyed input collapse.
        shuffled = [list(x) + list(x[:2]) for x in (keys, obs, links, episodes, (b.PKG, b.PKG_2), b.SCOPES)]
        for items in shuffled:
            rng.shuffle(items)
        again = b.resolve(shuffled[0], observations=shuffled[1], links=shuffled[2], episodes=shuffled[3],
                          packages=shuffled[4], scopes=shuffled[5])
        assert again.rows == base.rows and again.issues == base.issues and again.stats == base.stats
    assert {i.reason for i in base.issues} == {"same_day_conflict"}


def _collision_cases() -> dict[str, tuple[str, dict[str, list[object]]]]:
    """Frame -> (error code, resolver kwargs holding one ID with two different bodies)."""
    bb = b.action("coll-1", X, symbol="BB", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    b_ = b.action("coll-1", X, symbol="B", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    assert bb.observation_id == b_.observation_id and bb != b_
    other_pkg = syn.replace_row(b.PKG, rights_ref="SYNTHETIC-AUTHORIZATION-OTHER")
    assert other_pkg.package_id == b.PKG.package_id
    episode = b.episode(X, "coll-e", upper=D(2024, 1, 20))
    other_episode = syn.replace_row(episode, obligor_id="SYN-OBLIGOR-OTHER")
    mirror = pr.UnclearedRatingSource("SYNTHETIC-MIRROR-1", "unverified")
    other_mirror = pr.UnclearedRatingSource("SYNTHETIC-MIRROR-1", "unverified", D(2020, 1, 1), D(2021, 1, 1))
    return {
        "observations": ("input_id_collision:observations", {"observations": [bb, b_]}),
        "packages": ("input_id_collision:packages", {"packages": [b.PKG, other_pkg], "observations": [bb]}),
        "episodes": ("input_id_collision:episodes", {"episodes": [episode, other_episode]}),
        "uncleared_sources": ("input_id_collision:uncleared_sources", {"uncleared": [mirror, other_mirror]}),
    }


@pytest.mark.parametrize("frame", ["observations", "packages", "episodes", "uncleared_sources"])
@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_duplicate_ids_fail_regardless_of_order_and_strict(frame: str, reverse: bool) -> None:
    code, kwargs = _collision_cases()[frame]
    if reverse:
        kwargs = {name: list(reversed(values)) for name, values in kwargs.items()}
    for strict in (False, True):
        with pytest.raises(pr.RatingResolveError) as err:
            b.resolve(b.grid([X], [D(2024, 1, 1)]), strict=strict, **kwargs)
        assert err.value.code == code and len(err.value.issues) == 1


def test_identical_duplicates_collapse_and_wrong_input_types_fail_loud() -> None:
    a = b.action("same-1", X, symbol="BB", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    keys = b.grid([X], [D(2024, 1, 1)])
    once = b.resolve(keys, observations=[a])
    twice = b.resolve(keys, observations=[a, b.action("same-1", X, symbol="BB", on=D(2024, 1, 10),
                                                      public=b.at(D(2024, 1, 11)))])
    assert once.rows == twice.rows and once.stats == twice.stats
    with pytest.raises(pr.RatingResolveError) as err:
        b.resolve(keys, observations=[a, b.PKG])
    assert err.value.code == "input_type_invalid:observations"
    with pytest.raises(pr.RatingResolveError) as err:
        b.resolve(keys, scopes=[("SYNTHETIC-AGENCY-1", "long_term", "global")])
    assert err.value.code == "input_type_invalid:rating_scopes"


BAD_CHECK_DIGIT = X[:8] + str((int(X[8]) + 1) % 10)


@pytest.mark.parametrize(("kwargs", "code"), [
    ({"panel_grid": [(BAD_CHECK_DIGIT, D(2024, 1, 1))]}, "panel_grid:invalid_cusip9"),
    ({"panel_grid": [(X, D(2024, 1, 15))]}, "panel_grid:month_key_expected"),
    ({"panel_grid": [(X, dt.datetime(2024, 1, 1, tzinfo=UTC))]}, "panel_grid:month_key_expected"),
    ({"knowledge_cutoff": b.K.replace(tzinfo=None)}, "knowledge_cutoff:timezone_required"),
    ({"views": ("public_pit", "latest")}, "views:invalid"),
    ({"views": "public_pit"}, "views:iterable_of_view_kinds_expected"),
    ({"knowledge_mode": "live"}, "knowledge_mode:invalid:live"),
])
def test_malformed_inputs_fail_loud(kwargs: dict, code: str) -> None:  # type: ignore[type-arg]
    args = {"panel_grid": [(X, D(2024, 1, 1))], "knowledge_cutoff": b.K, **kwargs}
    grid_arg = args.pop("panel_grid")
    with pytest.raises(pr.RatingResolveError) as err:
        pr.build_full_grid_ratings(grid_arg, **args)
    assert err.value.code == code


# ---------------------------------------------------------------------------
# W0 bundle integration
# ---------------------------------------------------------------------------
def _resolve_bundle(bundle: c.CreditBundle, **kwargs):  # type: ignore[no-untyped-def]
    fr = bundle.frames
    return pr.build_full_grid_ratings(
        bundle.panel_grid, knowledge_cutoff=bundle.manifest["knowledge_cutoff"],
        packages=fr["source_packages"], observations=fr["observations"], links=fr["event_links"],
        episodes=fr["events"], rating_scopes=[pr.RatingScope("SYNTHETIC-AGENCY", "long_term", "global")], **kwargs,
    )


def _semantics(rows):  # type: ignore[no-untyped-def]
    return {
        (r.cusip_id, r.month, r.view_kind): (r.state, r.bucket, r.action_date, r.public_known_at, r.agency_source_ids,
                                             r.coverage_frontier, r.default_overlay_episode_id)
        for r in rows
    }


def test_resolved_ratings_frame_passes_check_bundle_with_the_qualified_synthetic_bundle() -> None:
    q = syn.build_bundle()
    res = _resolve_bundle(q)
    assert _semantics(res.rows) == _semantics(q.frames["ratings"])
    # v2: identical rows, including binding links and the W0-derived action_input_digest.
    assert res.rows == q.frames["ratings"]
    bundle = syn.reassemble(q, frames={"ratings": res.rows})
    check_bundle(bundle)
    assert bundle.canonical_bytes() == q.canonical_bytes()
    assert bundle.manifest["ratings_count"] == 2 * len(q.panel_grid)


def test_partial_synthetic_bundle_reproduces_rights_unverified_grid_exactly() -> None:
    p = syn.build_bundle(quality_state="partial")
    mirror = pr.UnclearedRatingSource("SYNTHETIC-MIRROR-1", "unverified")
    res = _resolve_bundle(p, uncleared_sources=[mirror])
    assert res.rows == p.frames["ratings"]
    rebuilt = syn.reassemble(p, frames={"ratings": res.rows})
    assert rebuilt.canonical_bytes() == p.canonical_bytes()
    check_bundle(rebuilt)


def test_conflict_missing_remains_valid_with_an_uncleared_source_declaration() -> None:
    q = syn.build_bundle()
    package, action = syn._agency_inputs(q)
    changes = {
        name: getattr(action, name)
        for name, _kind in action.SPEC
        if name not in {"observation_id", "package_id", "member_name", "row_locator", "observation_kind", "semantic_key"}
    }
    conflict = syn._observation(
        package,
        "action-conflict",
        "agency_action",
        **{**changes, "agency_rating_symbol": "B+"},
    )
    declarations = pr.rating_declarations_record(syn.RATING_SCOPES, syn.UNCLEARED_RATING_SOURCES)
    source = syn.reassemble(
        q,
        frames={"observations": (*q.frames["observations"], conflict)},
        rating_declarations=declarations,
        quality_state="partial",
    )
    resolved = _resolve_bundle(
        source,
        uncleared_sources=syn.UNCLEARED_RATING_SOURCES,
        strict=False,
    )
    assert {issue.reason for issue in resolved.issues} == {"same_day_conflict"}
    assert {
        row.state for row in resolved.rows
        if row.cusip_id == syn.CUSIP_C and row.month == syn.GRID_MONTHS[0]
    } == {"missing"}
    check_bundle(syn.reassemble(source, frames={"ratings": resolved.rows}))


def _tamper(rows, **changes):  # type: ignore[no-untyped-def]
    target = next(r for r in rows if r.state == "carried_verified" and r.view_kind == "public_pit")
    return tuple(syn.replace_row(r, **changes) if r is target else r for r in rows)


@pytest.mark.parametrize("changes", [
    {"action_date": D(2026, 4, 9)},
    {"coverage_frontier": D(2026, 5, 31)},
    {"public_known_at": dt.datetime(2026, 4, 11, 13, 0, tzinfo=UTC)},
])
def test_check_bundle_rejects_a_tampered_rated_row(changes: dict) -> None:  # type: ignore[type-arg]
    q = syn.build_bundle()
    rows = _tamper(_resolve_bundle(q).rows, **changes)
    with pytest.raises(c.ContractError, match="rating_row_invalid"):
        check_bundle(syn.reassemble(q, frames={"ratings": rows}))


# ---------------------------------------------------------------------------
# Bundle v2: persisted binding links, W0-derived bucket and digest
# ---------------------------------------------------------------------------
APRIL = syn.GRID_MONTHS[0]
EARLY_LINK_AT = dt.datetime(2026, 4, 20, 9, 30, tzinfo=UTC)
LATE_LINK_AT = dt.datetime(2026, 9, 21, 9, 30, tzinfo=UTC)


def _linked_inputs(q: c.CreditBundle, *extra_link_changes: dict) -> tuple:  # type: ignore[type-arg]
    """A CUSIP-less SYNTHETIC agency action bound to CUSIP A by an issue link known 2026-04-20
    (plus one extra link per ``extra_link_changes``); returns frames, package, action, links."""
    fr = q.frames
    package = next(p for p in fr["source_packages"] if p.source_family == "agency_rocr_xbrl")  # type: ignore[attr-defined]
    link_pkg = next(p for p in fr["source_packages"] if p.source_family == "link_batch")  # type: ignore[attr-defined]
    public = dt.datetime(2026, 4, 13, 12, 0, tzinfo=UTC)
    action = syn._observation(
        package, "action-linked-1", "agency_action", effective_date=D(2026, 4, 12), date_precision="day",
        public_available_at=public, public_time_basis="rocr_file_creation", agency_name="SYNTHETIC-AGENCY",
        agency_subject_kind="instrument", agency_rating_type="long_term", agency_scale="global",
        agency_currency="USD", agency_rating_symbol="BB-", agency_action_classification="AF",
        agency_action_date=D(2026, 4, 12), agency_file_creation_at=public,
    )
    link = syn._link(link_pkg, action, syn.CUSIP_A, "SYNTHETIC-OBLIGOR-A", "issue", EARLY_LINK_AT)
    links = [link]
    for changes in extra_link_changes:
        values = {n: getattr(link, n) for n, _ in link.SPEC if n != "link_id"}
        links.append(c.EventLink.create(**{**values, **changes}))
    frames = {"observations": (*fr["observations"], action), "event_links": (*fr["event_links"], *links)}
    return frames, package, action, links


def _resolve_linked(q: c.CreditBundle, frames: dict):  # type: ignore[no-untyped-def,type-arg]
    return _resolve_bundle(syn.reassemble(q, frames=frames))


def test_resolved_binding_links_are_persisted_and_pass_check_bundle() -> None:
    q = syn.build_bundle()
    frames, package, action, (link, late) = _linked_inputs(q, {"link_known_at": LATE_LINK_AT,
                                                               "valid_from": D(2021, 1, 1)})
    res = _resolve_linked(q, frames)
    rows = res.by_key()
    for month in syn.GRID_MONTHS:
        audit, pit = rows[(syn.CUSIP_A, month, "effective_audit")], rows[(syn.CUSIP_A, month, "public_pit")]
        assert (audit.bucket, pit.bucket) == ("BB", "BB")
        assert audit.agency_source_ids == pit.agency_source_ids == (action.observation_id,)
        # Selected binding links, canonical (sorted) order; PIT only the link known by month end.
        assert audit.binding_link_ids == c.sorted_uuids([link.link_id, late.link_id])
        assert pit.binding_link_ids == (link.link_id,)
        assert audit.public_known_at == late.link_known_at
        assert pit.public_known_at == link.link_known_at
        assert audit.action_input_digest == c.rating_action_input_digest(
            "effective_audit", [action], [package], [link, late])
        assert pit.action_input_digest == c.rating_action_input_digest("public_pit", [action], [package], [link])
    assert rows[(syn.CUSIP_A, APRIL, "public_pit")].state == "observed"
    # Rows naming their CUSIP carry no binding links.
    assert all(r.binding_link_ids == () for r in res.rows if r.cusip_id != syn.CUSIP_A)
    check_bundle(syn.reassemble(q, frames={**frames, "ratings": res.rows}))


def _linked_row_swap(q: c.CreditBundle, frames: dict, res, view: str, **changes: object) -> dict:  # type: ignore[no-untyped-def,type-arg]
    target = res.by_key()[(syn.CUSIP_A, APRIL, view)]
    return {**frames, "ratings": tuple(syn.replace_row(r, **changes) if r is target else r for r in res.rows)}


def _rebound(frames: dict, package: c.SourcePackage, action: c.CreditObservation, view: str,  # type: ignore[type-arg]
             bad: c.EventLink) -> dict[str, object]:
    """Row changes binding ``bad`` with a digest W0 would accept for that link."""
    return {"binding_link_ids": (bad.link_id,),
            "action_input_digest": c.rating_action_input_digest(view, [action], [package], [bad])}


def test_check_bundle_refuses_tampered_resolver_rows() -> None:
    q = syn.build_bundle()
    frames, package, action, (_link, late, expired) = _linked_inputs(
        q, {"link_known_at": LATE_LINK_AT, "valid_from": D(2021, 1, 1)},
        {"valid_from": D(2022, 1, 1), "valid_to": D(2026, 4, 15)})
    res = _resolve_linked(q, frames)
    assert expired.link_id not in {x for r in res.rows for x in r.binding_link_ids}  # resolver never binds it
    pit = res.by_key()[(syn.CUSIP_A, APRIL, "public_pit")]
    audit = res.by_key()[(syn.CUSIP_A, APRIL, "effective_audit")]
    assert (pit.state, pit.bucket) == ("observed", "BB")
    tampers = {
        "bucket_BB_to_AAA": _linked_row_swap(q, frames, res, "public_pit", bucket="AAA"),
        "swapped_valid_digest": _linked_row_swap(q, frames, res, "public_pit",
                                                 action_input_digest=audit.action_input_digest),
        "pit_link_known_after_boundary": _linked_row_swap(
            q, frames, res, "public_pit", **_rebound(frames, package, action, "public_pit", late)),
        "link_expired_mid_month": _linked_row_swap(
            q, frames, res, "effective_audit", **_rebound(frames, package, action, "effective_audit", expired)),
    }
    for name, tampered in tampers.items():
        with pytest.raises(c.ContractError, match="rating_row_invalid"):
            check_bundle(syn.reassemble(q, frames=tampered))
        assert name  # every tamper is refused
    # Controls: the untampered frame passes, and the late link alone is acceptable for the
    # retrospective view (the PIT refusal is the boundary rule, not the link itself).
    check_bundle(syn.reassemble(q, frames={**frames, "ratings": res.rows}))
    check_bundle(syn.reassemble(q, frames=_linked_row_swap(
        q, frames, res, "effective_audit", **_rebound(frames, package, action, "effective_audit", late))))


def test_link_revision_naming_another_observation_still_supersedes_the_binding() -> None:
    unlabelled = b.action("xrev-1", None, symbol="BB", on=D(2024, 5, 2), public=b.at(D(2024, 5, 3)))
    original = b.link(unlabelled, X, known=EARLY)
    other = syn._observation(b.EDGAR, "xrev-doc", "edgar_passage", public_available_at=b.at(D(2024, 5, 4)),
                             public_time_basis="first_verified_retrieval", first_seen_at=b.at(D(2024, 5, 4)),
                             document_quote="SYNTHETIC", document_location="x#p1", document_sha256=syn._sha("x"))
    revision = b.link(other, X, known=b.at(D(2025, 1, 1)), supersedes=original)
    keys = b.grid([X], [D(2024, 5, 1)])
    res = b.resolve(keys, packages=(b.PKG, b.EDGAR), observations=[unlabelled, other], links=[original, revision])
    assert {r.state for r in res.rows} == {"missing"} and res.stats["links:superseded"] == 1
    late = b.link(other, X, known=dt.datetime(2026, 9, 26, tzinfo=UTC), supersedes=original)
    kept = b.resolve(keys, packages=(b.PKG, b.EDGAR), observations=[unlabelled, other], links=[original, late])
    assert {(r.state, r.bucket) for r in kept.rows} == {("observed", "BB")}
    assert {r.binding_link_ids for r in kept.rows} == {(original.link_id,)}


# ---------------------------------------------------------------------------
# W0 alignment: classifier, resolver identity
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("symbol", ["BB", "\u00a0BB", "BB\u2003", "\x1cBB", " BB\t", "Caa", "Ca", "C", "NR", "wr",
                                    "SD", "Baa", "Aa", "Ba", "BB (high)"])
@pytest.mark.parametrize("rac", [None, "AF", "wd", " WO ", "\u00a0WD", "DG"])
def test_classifier_is_exactly_w0_classify_rating_action(symbol: str, rac: str | None) -> None:
    o = b.action("w0cls-1", X, symbol=symbol, rac=rac, on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    assert pr.classify_agency_action(o) == c.classify_rating_action(o)


def test_non_ascii_padding_is_unmapped_like_w0() -> None:
    padded = b.action("pad-1", X, symbol="\u00a0BB", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    wd_padded = b.action("pad-2", Y, symbol="BB", rac="\u00a0WD", on=D(2024, 1, 10), public=b.at(D(2024, 1, 11)))
    res = b.resolve(b.grid([X, Y], [D(2024, 1, 1)]), observations=[padded, wd_padded])
    assert b.state_map(res, X, "public_pit") == {D(2024, 1, 1): ("missing", None)}
    assert b.state_map(res, Y, "public_pit") == {D(2024, 1, 1): ("observed", "BB")}
    assert {(i.reason, i.cusip_id) for i in res.issues} == {("rating_symbol_unmapped", X)}


def test_resolver_identity_is_the_w0_pin() -> None:
    assert pr.RESOLVER_ID == c.RATING_RESOLVER_ID == "bond_public_ratings_v1"


# ---------------------------------------------------------------------------
# Integrator-local declarations: canonical serialization (plan amendment 1, section 4.9)
# ---------------------------------------------------------------------------
MIRROR = pr.UnclearedRatingSource("SYNTHETIC-MIRROR-1", "unverified", D(2021, 1, 1), D(2025, 8, 31))
OPEN_MIRROR = pr.UnclearedRatingSource("SYNTHETIC-MIRROR-2", "denied")


def test_declarations_record_is_canonical_and_order_independent() -> None:
    scopes = [*b.SCOPES, pr.RatingScope(b.AGENCY, None, "global"), pr.RatingScope(b.AGENCY, "", "global")]
    record = pr.rating_declarations_record(scopes, [MIRROR, OPEN_MIRROR])
    assert record == pr.rating_declarations_record([*reversed(scopes), scopes[0]], [OPEN_MIRROR, MIRROR, MIRROR])
    assert record["version"] == pr.DECLARATIONS_VERSION
    assert record["rating_scopes"] == sorted(record["rating_scopes"], key=c.canonical_json_bytes)
    assert {"agency_name": b.AGENCY, "rating_type": None, "scale": "global"} in record["rating_scopes"]
    assert {"agency_name": b.AGENCY, "rating_type": "", "scale": "global"} in record["rating_scopes"]
    assert record["uncleared_rating_sources"] == [
        {"source_ref": "SYNTHETIC-MIRROR-1", "rights_state": "unverified",
         "coverage_start": "2021-01-01", "coverage_end": "2025-08-31"},
        {"source_ref": "SYNTHETIC-MIRROR-2", "rights_state": "denied", "coverage_start": None, "coverage_end": None},
    ]
    assert c.canonical_json_bytes(record) == c.canonical_json_bytes(
        pr.rating_declarations_record(list(scopes), (MIRROR, OPEN_MIRROR)))
    empty = pr.rating_declarations_record([], [])
    assert empty == {"version": pr.DECLARATIONS_VERSION, "rating_scopes": [], "uncleared_rating_sources": []}
    assert pr.rating_declarations_digest([], []) == c.digest_of(empty)


def test_declarations_digest_changes_with_every_declared_field() -> None:
    base = pr.rating_declarations_digest(b.SCOPES, [MIRROR])
    variants = [
        (b.SCOPES[:-1], [MIRROR]),
        ((*b.SCOPES[:-1], pr.RatingScope(b.AGENCY_3, "long_term", "national")), [MIRROR]),
        ((*b.SCOPES[:-1], pr.RatingScope(b.AGENCY_3, None, "global")), [MIRROR]),
        (b.SCOPES, []),
        (b.SCOPES, [pr.UnclearedRatingSource("SYNTHETIC-MIRROR-1", "denied", D(2021, 1, 1), D(2025, 8, 31))]),
        (b.SCOPES, [pr.UnclearedRatingSource("SYNTHETIC-MIRROR-1", "unverified", D(2021, 1, 1), D(2025, 9, 30))]),
        (b.SCOPES, [pr.UnclearedRatingSource("SYNTHETIC-MIRROR-1", "unverified")]),
        (b.SCOPES, [MIRROR, OPEN_MIRROR]),
    ]
    digests = {pr.rating_declarations_digest(scopes, uncleared) for scopes, uncleared in variants}
    assert len(digests) == len(variants) and base not in digests

    manifest = pr.rating_input_manifest_record(b.SCOPES, [MIRROR], [b.PKG])
    assert manifest == {
        "version": pr.INPUT_MANIFEST_VERSION,
        "resolver_id": pr.RESOLVER_ID,
        "rating_declarations": pr.rating_declarations_record(b.SCOPES, [MIRROR]),
        "rating_declarations_digest": base,
        "rating_package_digest": c.rating_package_digest([b.PKG]),
    }
    assert pr.rating_input_manifest_digest(b.SCOPES, [MIRROR], [b.PKG]) == c.digest_of(manifest)


def test_declarations_reject_conflicts_and_malformed_values() -> None:
    other = pr.UnclearedRatingSource("SYNTHETIC-MIRROR-1", "denied")
    for order in ([MIRROR, other], [other, MIRROR]):
        with pytest.raises(pr.RatingResolveError) as err:
            pr.rating_declarations_record(b.SCOPES, order)
        assert err.value.code == "input_id_collision:uncleared_sources"
    with pytest.raises(pr.RatingResolveError, match="coverage_dates_expected"):
        pr.UnclearedRatingSource("SYNTHETIC-MIRROR-3", "unverified", dt.datetime(2021, 1, 1, tzinfo=UTC),
                                 dt.datetime(2022, 1, 1, tzinfo=UTC))
    with pytest.raises(pr.RatingResolveError, match="raw_values_must_be_text"):
        pr.RatingScope(b.AGENCY, 1, "global")  # type: ignore[arg-type]
    with pytest.raises(pr.RatingResolveError) as err:
        pr.rating_declarations_record([(b.AGENCY, "long_term", "global")], [])  # type: ignore[list-item]
    assert err.value.code == "input_type_invalid:rating_scopes"


# ---------------------------------------------------------------------------
# Performance (F8)
# ---------------------------------------------------------------------------
def test_sweep_work_is_linear_in_months_plus_bindings() -> None:
    months = 480
    steps = {}
    for n_actions in (60, 600):
        keys, obs = b.dense_history(1, months, n_actions, first=D(1986, 1, 1))
        linked = b.action("lin-1", None, symbol="B", on=D(1990, 1, 10), public=b.at(D(1990, 1, 11)),
                          package=b.DENSE_PKG)
        windows = [b.link(linked, keys[0][0], valid_from=D(1990 + i, 1, 1), valid_to=D(1990 + i, 6, 15), known=EARLY)
                   for i in range(20)]
        res = b.resolve(keys, packages=(b.DENSE_PKG,), observations=[*obs, linked], links=windows)
        assert len(res.rows) == 2 * months
        steps[n_actions] = res.stats["sweep_steps"]
        assert steps[n_actions] <= 2 * (months + n_actions + len(windows)) + months
    assert steps[600] - steps[60] <= 2 * (600 - 60)


def test_dense_history_benchmark_has_no_quadratic_blow_up() -> None:
    import time

    keys, obs = b.dense_history(2000, 160, 40)
    started = time.perf_counter()
    res = b.resolve(keys, packages=(b.DENSE_PKG,), observations=obs)
    elapsed = time.perf_counter() - started
    assert len(res.rows) == 2 * 2000 * 160 and res.issues == ()
    assert res.stats["sweep_steps"] <= 2 * (len(keys) + len(obs))
    assert elapsed < 180, f"dense full-grid resolution took {elapsed:.1f}s"
