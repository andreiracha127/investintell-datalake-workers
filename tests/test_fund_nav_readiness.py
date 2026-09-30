"""Synthetic daily NAV contract, no external provider and no calendar inference."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest

from src.workers._tiingo import NavObservation
from src.workers._nav_policy import (
    CalendarSessionProof,
    calendar_equivalence_digest,
    derive_calendar_equivalence,
    effective_calendar_tuple,
    load_calendar_equivalence,
)
from src.workers._nav_sanitize import REPAIRED_NAV_KINDS
from src.workers import nav_current_daily_chain as chain
from src.workers.fund_nav_readiness import _per_date_lineage, assess_instrument, sample_id
from src.workers.instrument_ingestion import build_rows

HOLIDAY = dt.date(2026, 9, 7)


def _grid(end: dt.date = dt.date(2026, 9, 8)) -> list[dt.date]:
    out = []
    d = end
    while len(out) < 401:
        if d.weekday() < 5 and d != HOLIDAY:
            out.append(d)
        d -= dt.timedelta(days=1)
    return list(reversed(out))


def _case():
    grid = _grid()
    policy = {
        "policy_id": "synthetic",
        "policy_version": "v1",
        "policy_hash": "f" * 64,
        "calendar_id": "NYSE-TEST",
        "calendar_version": "v1",
        "calendar_source": "fixture-closed-sessions",
        "required_nav_kind": "adjusted",
        "required_return_semantics": "observed_interval_log_ratio",
        "modeling_currency": "USD",
    }
    observations = tuple(
        NavObservation(d, round(100 + i * 0.01, 6), "adjusted")
        for i, d in enumerate(grid)
    )
    rows = build_rows(
        observations,
        [(uuid.uuid4(), "USD")],
        calendar={d: ("NYSE-TEST", "v1", "fixture-closed-sessions") for d in grid},
    )
    lifecycle = {
        "evidence_id": uuid.uuid4(),
        "fund_status": "ACTIVE",
        "valuation_frequency": "daily",
        "identity_verified": True,
        "return_basis_verified": True,
        "currency_verified": True,
        "known_at": dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
        "effective_at": dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
    }
    feature = {
        "risk_run_id": uuid.uuid4(),
        "input_fingerprint": "a" * 64,
        "calc_date": grid[-1],
        "feature_as_of": grid[-1],
        "input_max_date": grid[-1],
        "exclusion_reason": None,
    }
    attempt = {"run_id": uuid.uuid4(), "status": "success_new"}
    return policy, grid, rows, lifecycle, feature, attempt


def _session_rows(policy, grid, stamp=None, *, tweak=None):
    """Synthetic ``EQUIVALENCE_ROW_FIELDS`` rows: same-session current/observed."""
    stamp = stamp or (
        policy["calendar_id"],
        policy["calendar_version"],
        policy["calendar_source"],
    )
    rows = []
    for index, day in enumerate(grid):
        close = dt.datetime.combine(day, dt.time(20), dt.timezone.utc)
        due = close + dt.timedelta(minutes=5)
        observed_due = due + dt.timedelta(minutes=1) if index == tweak else due
        rows.append(
            (day, *stamp, policy["policy_id"], policy["policy_version"],
             policy["policy_hash"], close, due, policy["calendar_source"], "ref",
             close, observed_due, stamp[2], "ref")
        )
    return rows


def _equivalence(policy, grid):
    return derive_calendar_equivalence(policy, _session_rows(policy, grid))


def _assess(
    policy,
    grid,
    rows,
    lifecycle,
    feature,
    attempt,
    *,
    last=None,
    closed=None,
    input_matches=True,
    hold=False,
    revision_verified=True,
    equivalence=None,
):
    return assess_instrument(
        "fund-1",
        policy,
        grid,
        rows,
        grid[-1] if last is None else last,
        grid[-1] if closed is None else closed,
        lifecycle,
        attempt,
        feature,
        input_matches,
        reexpression_hold=hold,
        revision_source_verified=revision_verified,
        equivalence=_equivalence(policy, grid) if equivalence is None else equivalence,
    )


def test_assess_requires_explicit_session_proof_no_literal_fallback():
    policy, grid, rows, lifecycle, feature, attempt = _case()
    with pytest.raises(TypeError):
        assess_instrument(
            "fund-1", policy, grid, rows, grid[-1], grid[-1], lifecycle, attempt,
            feature, True,
        )
    empty = _assess(policy, grid, rows, lifecycle, feature, attempt, equivalence={})
    assert empty["reason_code"] == "RETURN_INTERVAL_INCOMPATIBLE"
    missing_first = {
        key: value
        for key, value in _equivalence(policy, grid).items()
        if key[0] != grid[0]
    }
    result = _assess(
        policy, grid, rows, lifecycle, feature, attempt, equivalence=missing_first
    )
    assert result["reason_code"] == "RETURN_INTERVAL_INCOMPATIBLE"
    assert result["admissible_returns_count"] == 399


def test_rollover_proof_rejects_one_changed_due_or_other_calendar_id():
    policy, grid, *_ = _case()
    old = (policy["calendar_id"], "v0", policy["calendar_source"])
    rows = _session_rows(policy, grid, old, tweak=0)
    mapping = derive_calendar_equivalence(policy, rows)
    assert (grid[0], *old) not in mapping and len(mapping) == 400
    foreign = ("OTHER-CAL", "v0", policy["calendar_source"])
    assert derive_calendar_equivalence(
        policy, _session_rows(policy, grid, foreign)
    ) == {}
    naive = list(rows[1])
    naive[7] = naive[7].replace(tzinfo=None)
    with pytest.raises(ValueError, match="naive"):
        derive_calendar_equivalence(policy, [tuple(naive)])


# Frozen W↔L vector (§11.4): payload bytes and SHA256 are the shared contract.
_VECTOR_POLICY = {
    "policy_id": "pol-ü",
    "policy_version": "v2",
    "policy_hash": "a" * 64,
    "calendar_id": "XNYS",
    "calendar_version": "2026.2",
    "calendar_source": "src/é",
}
_VECTOR_JSON = (
    '[["2026-09-21",["XNYS","2026.1","src/\\u00e9"],'
    '["XNYS","2026.2","src/\\u00e9","2026-09-21T20:00:00+00:00",'
    '"2026-09-21T22:05:00+00:00","ref \\u00e7\\u00e3o"],'
    '["pol-\\u00fc","v1","' + "b" * 64 + '"]],'
    '["2026-09-22",["XNYS","2026.2","src/\\u00e9"],'
    '["XNYS","2026.2","src/\\u00e9","2026-09-22T20:00:00+00:00",'
    '"2026-09-22T22:05:00+00:00","ref \\u00e7\\u00e3o"],'
    '["pol-\\u00fc","v2","' + "a" * 64 + '"]],'
    '["2026-09-23",null,null,null]]'
)


def test_calendar_equivalence_canonical_vector_is_frozen():
    import hashlib

    et = dt.timezone(dt.timedelta(hours=-4))
    grid = [dt.date(2026, 9, 21), dt.date(2026, 9, 22), dt.date(2026, 9, 23)]
    old = ("XNYS", "2026.1", "src/é")
    new = ("XNYS", "2026.2", "src/é")

    def row(day, stamp, proof_version, proof_hash):
        close = dt.datetime.combine(day, dt.time(16), et)  # rendered as UTC
        due = dt.datetime.combine(day, dt.time(18, 5), et)
        return (day, *stamp, "pol-ü", proof_version, proof_hash, close, due,
                "src/é", "ref ção", close, due, "src/é", "ref ção")

    mapping = derive_calendar_equivalence(
        _VECTOR_POLICY,
        [row(grid[0], old, "v1", "b" * 64), row(grid[1], new, "v2", "a" * 64)],
    )
    by_date = {
        grid[0]: dict(zip(("calendar_id", "calendar_version", "calendar_source"), old)),
        grid[1]: dict(zip(("calendar_id", "calendar_version", "calendar_source"), new)),
    }
    proof = [
        mapping[(day, *by_date[day].values())].canonical()
        if day in by_date
        else [day.isoformat(), None, None, None]
        for day in grid
    ]
    payload = json.dumps(proof, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    assert payload == _VECTOR_JSON
    expected = hashlib.sha256(_VECTOR_JSON.encode("utf-8")).hexdigest()
    assert calendar_equivalence_digest(grid, by_date, mapping) == expected
    assert expected == (
        "8e4cc9d4c943fe94d21e9e226c6ba08a598edda08068441f5329d77ed887af06"
    )


def test_exact_401_endpoints_400_returns_over_weekend_and_holiday():
    policy, grid, rows, lifecycle, feature, attempt = _case()
    assert grid[-2:] == [dt.date(2026, 9, 4), dt.date(2026, 9, 8)]
    assert rows[-1]["return_start_date"] == grid[-2]
    result = _assess(policy, grid, rows, lifecycle, feature, attempt)
    assert result["admissible"] and result["reason_code"] is None
    assert result["observed_levels_count"] == 401
    assert result["admissible_returns_count"] == 400
    assert result["missed_due_sessions"] == 0
    assert sample_id(policy, grid) == sample_id(policy, list(grid))


@pytest.mark.parametrize("frequency", ["weekly", "monthly", "unknown"])
def test_no_resampling_of_nondaily_fund(frequency):
    p, g, r, lifecycle, f, a = _case()
    lifecycle["valuation_frequency"] = frequency
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "UNSUPPORTED_VALUATION_FREQUENCY"
    )


def test_current_gap_is_never_dropped_or_forward_filled():
    p, g, r, lifecycle, f, a = _case()
    missing = g[-20]
    r = [row for row in r if row["nav_date"] != missing]
    result = _assess(p, g, r, lifecycle, f, a)
    assert result["reason_code"] == "RETURN_INTERVAL_INCOMPATIBLE"
    assert result["missing_session_count"] == 1
    assert result["missed_due_sessions"] == 0
    assert len(g) == 401


def test_400_levels_are_only_399_returns():
    p, g, r, lifecycle, f, a = _case()
    result = _assess(p, g, r[1:], lifecycle, f, a)
    assert result["reason_code"] == "RETURN_INTERVAL_INCOMPATIBLE"
    assert result["observed_levels_count"] == 400
    assert not result["admissible"]


def test_missing_calendar_mapping_preserves_whole_tuple_and_partial_rejected():
    old = {"calendar_id": "XNYS", "calendar_version": "v1", "calendar_source": "src"}
    assert effective_calendar_tuple({"calendar_id": None}, old) == ("XNYS", "v1", "src")
    assert effective_calendar_tuple({}, None) is None
    with pytest.raises(ValueError, match="partial"):
        effective_calendar_tuple({"calendar_id": "XNYS"}, old)


def test_calendar_proof_digest_covers_first_predecessor_and_all_401_dates():
    policy, grid, rows, *_ = _case()
    stamp = (policy["calendar_id"], policy["calendar_version"], policy["calendar_source"])
    proof = {
        (day, *stamp): CalendarSessionProof(
            day, stamp, (*stamp, "close", "due", "reference"),
            (policy["policy_id"], policy["policy_version"], policy["policy_hash"]),
        )
        for day in grid
    }
    by_date = {row["nav_date"]: row for row in rows}
    original = calendar_equivalence_digest(grid, by_date, proof)
    assert len(original) == 64
    without_first = {key: value for key, value in proof.items() if key[0] != grid[0]}
    assert calendar_equivalence_digest(grid, by_date, without_first) != original


@pytest.mark.parametrize("mismatched_date", [None, 0, 200])
def test_batched_calendar_equivalence_requires_each_session_tuple(mismatched_date):
    policy, grid, *_ = _case()
    old = (policy["calendar_id"], "v0", policy["calendar_source"])
    close = dt.datetime(2026, 1, 1, 20, tzinfo=dt.timezone.utc)
    due = close + dt.timedelta(hours=3)

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _query, params):
            assert params["grid"] == grid
            assert old[1] in params["versions"]

        def fetchall(self):
            return [
                (day, *old, "synthetic", "v0", "e" * 64, close, due,
                 old[2], "reference", close,
                 due + dt.timedelta(minutes=1) if index == mismatched_date else due,
                 old[2], "reference")
                for index, day in enumerate(grid)
            ]

    class Connection:
        def cursor(self):
            return Cursor()

    mapping = load_calendar_equivalence(
        Connection(), policy, grid, [old], dt.datetime.now(dt.timezone.utc)
    )
    assert len(mapping) == 401 - (mismatched_date is not None)
    if mismatched_date is not None:
        assert (grid[mismatched_date], *old) not in mapping


class _LineageCursor:
    """Scripted cursor: rows, revisions, row evidence, then attempts (if asked)."""

    def __init__(self, rows, revisions=(), evidence=(), attempts=()):
        self.results = iter((list(rows), list(revisions), list(evidence), list(attempts)))

    def execute(self, *_args):
        return None

    def fetchall(self):
        return next(self.results)


_NOW = dt.datetime(2026, 9, 10, 12, tzinfo=dt.timezone.utc)
_DAY = dt.date(2026, 9, 4)
_PRIOR = dt.date(2026, 9, 3)


def _lineage_row(day=_DAY, nav=100.0):
    return {"nav_date": day, "nav": nav, "source_nav": nav, "source": "tiingo",
            "source_nav_kind": "adjusted", "currency": "USD", "nav_repair_kind": "none",
            "calendar_id": None, "calendar_version": None, "calendar_source": None}


def _attempt_row(run_id, xid, *, start=_DAY, end=_DAY, status="success_new",
                 finished=_NOW - dt.timedelta(hours=1)):
    return {"run_id": run_id, "provider": "tiingo", "status": status, "commit_xid": xid,
            "requested_start": start, "requested_end": end, "finished_at": finished,
            "persisted_at": finished}


def _revision(day=_DAY, *, level=None, derived=None, dependency=None):
    empty = {f"{kind}_{field}": None for kind in ("level", "derived", "calendar")
             for field in ("run_id", "provider", "xid", "recorded_at")}
    out = {**empty, "nav_date": day, "level_revision_id": None, "derived_revision_id": None,
           "calendar_revision_id": None, "dependency_start_date": dependency,
           "calendar_maintenance_id": None}
    for kind, spec in (("level", level), ("derived", derived)):
        if spec is not None:
            rev_id, run_id, xid = spec
            out.update({f"{kind}_revision_id": rev_id, f"{kind}_run_id": run_id,
                        f"{kind}_provider": "tiingo", f"{kind}_xid": xid,
                        f"{kind}_recorded_at": _NOW - dt.timedelta(hours=1)})
    return out


def _evidence(run_id, head, xid, *, digest=None, attempt_xid=None, start=_DAY):
    from src.workers._nav_policy import level_evidence_digest

    row = _lineage_row()
    return {"nav_date": _DAY, "run_id": run_id, "provider": "tiingo",
            "level_digest": digest or level_evidence_digest(
                _DAY, row["nav"], row["source_nav"], row["source"], row["source_nav_kind"],
                row["currency"], row["nav_repair_kind"]),
            "revision_head": head, "commit_xid": xid,
            "recorded_at": _NOW - dt.timedelta(hours=1),
            "attempt_status": "success_no_new", "attempt_xid": attempt_xid or xid,
            "finished_at": _NOW - dt.timedelta(hours=1),
            "persisted_at": _NOW - dt.timedelta(hours=1),
            "requested_start": start, "requested_end": _DAY}


def test_per_date_lineage_cannot_accept_missing_grid_date_or_revision():
    row = _lineage_row()
    assert _per_date_lineage(_LineageCursor([row]), uuid.uuid4(), [_DAY], _DAY, _NOW)[0] is False
    assert _per_date_lineage(
        _LineageCursor([row]), uuid.uuid4(), [_DAY, _DAY + dt.timedelta(days=1)],
        _DAY + dt.timedelta(days=1), _NOW,
    )[0] is False


def test_per_date_lineage_accepts_same_xid_attempt_regardless_of_parent():
    run, xid = uuid.uuid4(), "900"
    cursor = _LineageCursor([_lineage_row()], [_revision(level=(7, run, xid))], [],
                            [_attempt_row(run, xid)])
    verified, lineage = _per_date_lineage(cursor, uuid.uuid4(), [_DAY], _DAY, _NOW)
    assert verified is True and lineage[0][1][0] == "revision"
    # Other-xid, failed, uncovering or future attempts prove nothing.
    for attempt in (_attempt_row(run, "901"), _attempt_row(run, xid, status="empty"),
                    _attempt_row(run, xid, start=_PRIOR, end=_PRIOR),
                    _attempt_row(run, xid, finished=_NOW + dt.timedelta(seconds=1))):
        cursor = _LineageCursor([_lineage_row()], [_revision(level=(7, run, xid))], [],
                                [attempt])
        assert _per_date_lineage(cursor, uuid.uuid4(), [_DAY], _DAY, _NOW)[0] is False


def test_per_date_lineage_row_evidence_proves_unchanged_level_until_superseded():
    run, xid = uuid.uuid4(), "910"
    # Typed level with no revision at all (head 0): evidence alone proves it.
    cursor = _LineageCursor([_lineage_row()], [], [_evidence(run, 0, xid)])
    verified, lineage = _per_date_lineage(cursor, uuid.uuid4(), [_DAY], _DAY, _NOW)
    assert verified is True and lineage[0][1][0] == "evidence"
    # Evidence recorded at head 5 survives a later revision on ANOTHER date
    # (not in this date's revisions) but not a later level revision of this date.
    other = uuid.uuid4()
    assert _per_date_lineage(_LineageCursor(
        [_lineage_row()], [_revision(level=(5, other, "1"))], [_evidence(run, 5, xid)],
        [_attempt_row(other, "2")]), uuid.uuid4(), [_DAY], _DAY, _NOW)[0] is True
    assert _per_date_lineage(_LineageCursor(
        [_lineage_row()], [_revision(level=(6, other, "1"))], [_evidence(run, 5, xid)],
        [_attempt_row(other, "2")]), uuid.uuid4(), [_DAY], _DAY, _NOW)[0] is False
    # Digest of another projection, other-xid attempt, or window not covering.
    for evidence in (_evidence(run, 0, xid, digest="0" * 64),
                     _evidence(run, 0, xid, attempt_xid="911"),
                     _evidence(run, 0, xid, start=_DAY + dt.timedelta(days=1))):
        assert _per_date_lineage(_LineageCursor([_lineage_row()], [], [evidence]),
                                 uuid.uuid4(), [_DAY], _DAY, _NOW)[0] is False


def test_per_date_lineage_derived_return_needs_attributed_dependency_not_a_fetch():
    level_run, level_xid = uuid.uuid4(), "920"
    derived_run, derived_xid = uuid.uuid4(), "921"
    revisions = [_revision(level=(3, level_run, level_xid),
                           derived=(8, derived_run, derived_xid), dependency=_PRIOR)]
    attempts = [_attempt_row(level_run, level_xid),
                # The derived writer fetched only the predecessor (_PRIOR).
                _attempt_row(derived_run, derived_xid, start=_PRIOR, end=_PRIOR)]
    cursor = _LineageCursor([_lineage_row()], revisions, [], attempts)
    verified, lineage = _per_date_lineage(cursor, uuid.uuid4(), [_DAY], _DAY, _NOW)
    # The level keeps its own origin; the derived revision is attributed via
    # its dependency without claiming _DAY was fetched by derived_run.
    assert verified is True and lineage[0][1] == ["revision", 3, str(level_run)]
    assert lineage[0][2] == 8
    # An unattributed derived revision (unknown writer) invalidates.
    unknown = [_revision(level=(3, level_run, level_xid), derived=(9, None, None),
                         dependency=_PRIOR)]
    unknown[0]["derived_run_id"] = None
    assert _per_date_lineage(_LineageCursor([_lineage_row()], unknown, [], attempts[:1]),
                             uuid.uuid4(), [_DAY], _DAY, _NOW)[0] is False
    # Dependency outside the derived attempt window invalidates too.
    far = [_revision(level=(3, level_run, level_xid), derived=(8, derived_run, derived_xid),
                     dependency=_PRIOR - dt.timedelta(days=5))]
    assert _per_date_lineage(_LineageCursor([_lineage_row()], far, [], attempts),
                             uuid.uuid4(), [_DAY], _DAY, _NOW)[0] is False


def test_reason_precedence_matrix_section_12_2d():
    """NULL kind -> missing; explicit raw/unknown/other conventions -> semantics
    (even without lineage); other gaps -> missing; then hold/interval."""
    p, g, r, lifecycle, f, a = _case()

    def reason(rows, **kwargs):
        return _assess(p, g, rows, lifecycle, f, a, **kwargs)["reason_code"]

    def with_kind(kind, index=-1):
        rows = [dict(row) for row in r]
        rows[index]["source_nav_kind"] = kind
        return rows

    assert reason(with_kind(None)) == "NAV_DATA_UNAVAILABLE"
    assert reason(with_kind("raw")) == "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    assert reason(with_kind("unknown")) == "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    # Known incompatibility is not hidden behind missing lineage or a hold.
    assert reason(with_kind("raw"), revision_verified=False) == (
        "NAV_RETURN_SEMANTICS_UNSUPPORTED")
    assert reason(with_kind("raw"), hold=True) == "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    # Absent kind outranks an explicit incompatibility elsewhere in the window.
    mixed = with_kind("raw")
    mixed[5]["source_nav_kind"] = None
    assert reason(mixed) == "NAV_DATA_UNAVAILABLE"
    # A declared non-required return convention is also a known incompatibility.
    convention = [dict(row) for row in r]
    convention[-1]["return_semantics"] = "close_to_close_simple"
    assert reason(convention, revision_verified=False) == "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    # Missing lineage/attempt outranks hold and interval problems.
    assert reason(r, revision_verified=False, hold=True) == "NAV_DATA_UNAVAILABLE"
    assert _assess(p, g, r, lifecycle, f, None, hold=True)["reason_code"] == (
        "NAV_DATA_UNAVAILABLE")
    # Only then: active hold or conflicting interval.
    assert reason(r, hold=True) == "RETURN_INTERVAL_INCOMPATIBLE"
    gap = [dict(row) for row in r]
    gap[-1]["return_semantics"] = None
    gap[-1]["return_1d"] = None
    assert reason(gap) == "RETURN_INTERVAL_INCOMPATIBLE"
    # Lifecycle gates stay first; rejected rows never become admissible.
    lifecycle["identity_verified"] = False
    assert reason(with_kind("raw")) == "NAV_DATA_UNAVAILABLE"
    lifecycle["identity_verified"] = True
    assert all(not _assess(p, g, rows, lifecycle, f, a)["admissible"]
               for rows in (with_kind(None), with_kind("raw"), mixed, convention, gap))


def test_catalog_manifest_is_reproducible_from_pinned_signature():
    from scripts.generate_fund_nav_readiness_catalog import manifest_bytes

    root = Path(__file__).parents[1]
    ddl = (root / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    current = (root / "schemas" / "fund_nav_readiness_v1.catalog.json").read_bytes()
    manifest = json.loads(current)
    assert manifest_bytes(
        manifest["signature"], manifest["access_profile"]["signature"], ddl
    ) == current


def test_29_march_15_to_18_vs_one_march_14_to_18_is_not_a_daily_cohort():
    p, g, r, lifecycle, f, a = _case()
    grid = _grid(dt.date(2024, 3, 18))
    assert grid[-3:] == [
        dt.date(2024, 3, 14),
        dt.date(2024, 3, 15),
        dt.date(2024, 3, 18),
    ]
    r = build_rows(
        tuple(
            NavObservation(day, round(100 + i * 0.01, 6), "adjusted")
            for i, day in enumerate(grid)
        ),
        [(uuid.uuid4(), "USD")],
        calendar={
            day: (p["calendar_id"], p["calendar_version"], p["calendar_source"])
            for day in grid
        },
    )
    f["calc_date"] = f["feature_as_of"] = f["input_max_date"] = grid[-1]
    assert all(_assess(p, grid, r, lifecycle, f, a)["admissible"] for _ in range(29))
    r[-1]["return_start_date"] = grid[-3]
    mismatched = _assess(p, grid, r, lifecycle, f, a)
    assert mismatched["admissible"] is False
    assert mismatched["reason_code"] == "RETURN_INTERVAL_INCOMPATIBLE"


def test_inactive_fresh_unknown_active_stale_and_future():
    p, g, r, lifecycle, f, a = _case()
    lifecycle["fund_status"] = "INACTIVE"
    assert _assess(p, g, r, lifecycle, f, a)["reason_code"] == "INACTIVE_FUND"
    lifecycle["fund_status"] = "UNKNOWN"
    assert _assess(p, g, r, lifecycle, f, a)["reason_code"] == "UNKNOWN_FUND_STATUS"
    lifecycle["fund_status"] = "ACTIVE"
    assert (
        _assess(p, g, r[:-1], lifecycle, f, a, last=g[-2])["reason_code"] == "NAV_STALE"
    )
    assert (
        _assess(p, g, r, lifecycle, f, a, last=g[-1] + dt.timedelta(days=1))[
            "reason_code"
        ]
        == "NAV_DATA_UNAVAILABLE"
    )


def test_extra_observed_nav_is_allowed_only_through_pinned_closed_session():
    p, g, r, lifecycle, f, a = _case()
    next_closed = g[-1] + dt.timedelta(days=1)
    assert _assess(p, g, r, lifecycle, f, a, last=next_closed, closed=next_closed)[
        "admissible"
    ]
    assert (
        _assess(p, g, r, lifecycle, f, a, last=next_closed, closed=g[-1])["reason_code"]
        == "NAV_DATA_UNAVAILABLE"
    )


def test_no_attempt_and_provider_error_are_not_success():
    p, g, r, lifecycle, f, a = _case()
    assert _assess(p, g, r, lifecycle, f, None)["reason_code"] == "NAV_DATA_UNAVAILABLE"
    a["status"] = "transient_error"
    assert _assess(p, g, r, lifecycle, f, a)["reason_code"] == "NAV_DATA_UNAVAILABLE"


def test_source_boundary_repair_and_predecessor_mismatch_are_rejected():
    p, g, r, lifecycle, f, a = _case()
    r[-1]["source"] = "yahoo"
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "RETURN_INTERVAL_INCOMPATIBLE"
    )


@pytest.mark.parametrize("kind", sorted(REPAIRED_NAV_KINDS))
def test_all_legacy_and_current_repair_kinds_fail_current_daily(kind):
    p, g, r, lifecycle, f, a = _case()
    r[-2]["nav_repair_kind"] = kind
    r[-1]["return_uses_repaired_nav"] = True
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "RETURN_INTERVAL_INCOMPATIBLE"
    )


def test_adjusted_overlap_hold_remains_incompatible_even_after_all_returns_recomputed():
    p, g, r, lifecycle, f, a = _case()
    assert _assess(p, g, r, lifecycle, f, a)["admissible"]
    assert _assess(p, g, r, lifecycle, f, a, hold=True)["reason_code"] == (
        "RETURN_INTERVAL_INCOMPATIBLE"
    )


def test_unattributed_nav_revision_requires_completed_source_run():
    p, g, r, lifecycle, f, a = _case()
    assert _assess(p, g, r, lifecycle, f, a, revision_verified=False)[
        "reason_code"
    ] == ("NAV_DATA_UNAVAILABLE")
    r[-1]["source"] = "tiingo"
    r[-1]["nav_repair_kind"] = "centered_interpolation_v1"
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "RETURN_INTERVAL_INCOMPATIBLE"
    )
    r[-1]["nav_repair_kind"] = "none"
    r[-1]["return_start_date"] = g[-3]
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "RETURN_INTERVAL_INCOMPATIBLE"
    )


def test_risk_input_older_than_current_or_modified_after_calculation():
    p, g, r, lifecycle, f, a = _case()
    f["input_max_date"] = g[-2]
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"] == "RETURN_SAMPLE_NOT_CURRENT"
    )
    f["input_max_date"] = g[-1]
    assert (
        _assess(p, g, r, lifecycle, f, a, input_matches=False)["reason_code"]
        == "RETURN_SAMPLE_NOT_CURRENT"
    )


def test_raw_unknown_or_unproven_identity_never_admissible():
    p, g, r, lifecycle, f, a = _case()
    lifecycle["return_basis_verified"] = False
    assert _assess(p, g, r, lifecycle, f, a)["reason_code"] == "NAV_DATA_UNAVAILABLE"
    lifecycle["return_basis_verified"] = True
    r[-1]["source_nav_kind"] = "raw"
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    )
    r[-1]["source_nav_kind"] = "unknown"
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    )


def test_non_usd_without_fx_and_wrong_return_type_are_not_ready():
    p, g, r, lifecycle, f, a = _case()
    r[-1]["currency"] = "EUR"
    assert _assess(p, g, r, lifecycle, f, a)["reason_code"] == "NAV_DATA_UNAVAILABLE"
    r[-1]["currency"] = "USD"
    r[-1]["return_type"] = "arithmetic"
    assert (
        _assess(p, g, r, lifecycle, f, a)["reason_code"]
        == "NAV_RETURN_SEMANTICS_UNSUPPORTED"
    )


def test_ordered_chain_publishes_only_after_coverage_and_risk(monkeypatch):
    events = []

    class Guard:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    @contextmanager
    def lock(*_args):
        yield True

    monkeypatch.setattr(chain, "connect", lambda *_args: Guard())
    monkeypatch.setattr(chain, "advisory_lock", lock)
    monkeypatch.setattr(
        chain, "_due_session", lambda *_args: events.append("policy") or "2026-09-08"
    )
    monkeypatch.setattr(
        chain.matview_refresh,
        "_refresh_all",
        lambda *_args: events.append("coverage") or ["fund_nav_coverage_mv"],
    )

    def ingest(*_args, **_kwargs):
        events.append("ingest")
        assert _kwargs["target_session"] == dt.date(2026, 9, 8)
        return {"ingestion_run_id": "ingested"}

    def risk(*_args, **_kwargs):
        events.append("risk")
        assert _kwargs == {"calc_date": "2026-09-08"}
        return _risk_stats()

    def publish(*_args):
        events.append("readiness")
        return {
            "state": "complete",
            "published": True,
            "run_id": "readiness",
            "sample_id": "s" * 64,
            "ready_count": 1,
            "as_of_session": "2026-09-08",
        }

    stats = chain.run(
        "unused", ingestion_runner=ingest, risk_runner=risk, readiness_runner=publish
    )
    assert events == ["policy", "ingest", "coverage", "risk", "readiness"]
    assert stats["published"] and stats["readiness_run_id"] == "readiness"
    assert (stats["status"], stats["state"]) == ("complete", "complete")

    events.clear()

    def stale_risk(*_args, **_kwargs):
        events.append("risk")
        return _risk_stats(mv_refreshed=False)

    stats = chain.run(
        "unused",
        ingestion_runner=ingest,
        risk_runner=stale_risk,
        readiness_runner=publish,
    )
    assert stats == {
        "status": "blocked",
        "state": "blocked",
        "published": False,
        "retryable": True,
        "reason": "MV_REFRESH_FAILED",
        "blocked_stage": "risk_metrics",
    }
    assert events == ["policy", "ingest", "coverage", "risk"]


def _risk_stats(*, mv_refreshed=True, calc_date="2026-09-08", **publication):
    payload = {
        "eligible": True,
        "published": True,
        "reason": None,
        "risk_run_id": "risk",
        "as_of_session": "2026-09-08",
        "retryable": False,
    }
    payload.update(publication)
    return {
        "processed": 1,
        "upserted": 1,
        "calc_date": calc_date,
        "workers": 1,
        "risk_run_id": "risk",
        "mv_refreshed": mv_refreshed,
        "risk_publication": payload,
    }


_BUSY = {"status": "lock_busy", "state": "lock_busy", "published": False, "retryable": True}


@pytest.mark.parametrize(
    ("risk_stats", "readiness_stats", "expected"),
    [
        (_risk_stats(eligible=False, published=False, reason="LIMITED_RUN"), None,
         {"status": "blocked", "reason": "LIMITED_RUN", "retryable": False,
          "blocked_stage": "risk_metrics"}),
        (_risk_stats(published=False, reason="SUPERSEDED", retryable=True), None,
         {"status": "blocked", "reason": "SUPERSEDED", "retryable": True,
          "blocked_stage": "risk_metrics"}),
        (_risk_stats(published=False, reason="MV_RUN_MISMATCH", retryable=True), None,
         {"status": "blocked", "reason": "MV_RUN_MISMATCH", "retryable": True,
          "blocked_stage": "risk_metrics"}),
        (_risk_stats(published=False, reason="LOCK_BUSY", retryable=True), None,
         {**_BUSY, "blocked_stage": "risk_metrics"}),
        ({"processed": 0, "upserted": 0, "skipped": "lock_busy", "mv_refreshed": False,
          "risk_publication": {"eligible": False, "published": False,
                               "reason": "LOCK_BUSY", "retryable": True}}, None,
         {**_BUSY, "blocked_stage": "risk_metrics"}),
        (_risk_stats(calc_date="2026-09-07"), None,
         {"status": "blocked", "reason": "DUE_SESSION_CHANGED", "retryable": True,
          "blocked_stage": "risk_metrics"}),
        (_risk_stats(as_of_session="2026-09-09"), None,
         {"status": "blocked", "reason": "DUE_SESSION_CHANGED", "retryable": True,
          "blocked_stage": "risk_metrics"}),
        (_risk_stats(), dict(_BUSY),
         {**_BUSY, "blocked_stage": "fund_nav_readiness"}),
        (_risk_stats(), {"state": "complete", "published": True, "run_id": "r",
                         "sample_id": "s", "ready_count": 1,
                         "as_of_session": "2026-09-09"},
         {"status": "blocked", "reason": "DUE_SESSION_CHANGED", "retryable": True,
          "blocked_stage": "fund_nav_readiness"}),
    ],
)
def test_chain_never_reports_success_without_pinned_publication(
    monkeypatch, risk_stats, readiness_stats, expected
):
    @contextmanager
    def lock(*_args):
        yield True

    class Guard:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    monkeypatch.setattr(chain, "connect", lambda *_args: Guard())
    monkeypatch.setattr(chain, "advisory_lock", lock)
    monkeypatch.setattr(chain, "_due_session", lambda *_args: "2026-09-08")
    monkeypatch.setattr(
        chain.matview_refresh, "_refresh_all", lambda *_a: ["fund_nav_coverage_mv"]
    )
    readiness_calls = []

    def readiness_runner(*_args):
        readiness_calls.append(1)
        return readiness_stats

    stats = chain.run(
        "unused",
        ingestion_runner=lambda *_a, **_k: {"ingestion_run_id": "i"},
        risk_runner=lambda *_a, **_k: risk_stats,
        readiness_runner=readiness_runner,
    )
    assert stats["published"] is False
    for key, value in expected.items():
        assert stats[key] == value, (key, stats)
    assert stats["state"] == stats["status"]
    assert bool(readiness_calls) == (readiness_stats is not None)


def test_chain_outer_lock_and_ingestion_contention_are_normalized(monkeypatch):
    class Guard:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    monkeypatch.setattr(chain, "connect", lambda *_args: Guard())

    @contextmanager
    def busy(*_args):
        yield False

    monkeypatch.setattr(chain, "advisory_lock", busy)
    assert chain.run("unused") == {**_BUSY, "blocked_stage": "nav_current_daily_chain"}

    @contextmanager
    def granted(*_args):
        yield True

    monkeypatch.setattr(chain, "advisory_lock", granted)
    monkeypatch.setattr(chain, "_due_session", lambda *_args: "2026-09-08")
    risk_calls = []
    stats = chain.run(
        "unused",
        ingestion_runner=lambda *_a, **_k: {"skipped": "lock_busy"},
        risk_runner=lambda *_a, **_k: risk_calls.append(1),
    )
    assert stats == {**_BUSY, "blocked_stage": "instrument_ingestion"}
    assert risk_calls == []


# ──────────────────────────────────────────────────────────────────────────────
# R2-A pure contracts: PR132 extractor matrix (N5) and level evidence digest.
# ──────────────────────────────────────────────────────────────────────────────
def _pr132_exact():
    from scripts.nav_timeseries_provenance_schema import EXPECTED_COLUMNS

    return {name: (type_name, False, "", "", False) for name, type_name in EXPECTED_COLUMNS}


def test_pr132_extractor_matrix_every_column_and_attribute():
    """Controlled catalog: combinations PG cannot build on these types (identity on
    numeric/varchar/date/boolean/text) are exercised only through the extractor."""
    from scripts.fund_nav_readiness_schema import pr132_violations

    exact = _pr132_exact()
    assert pr132_violations("r", exact) == []
    assert pr132_violations(None, {}) == ["relation:missing"]
    for relkind in ("v", "p", "m", "f"):
        assert pr132_violations(relkind, exact) == [f"relkind:{relkind}"]
    variants = {
        "type": lambda t: ("text" if t[0] != "text" else "character varying(1)",) + t[1:],
        "notnull": lambda t: (t[0], True) + t[2:],
        "generated": lambda t: t[:2] + ("s",) + t[3:],
        "identity_always": lambda t: t[:3] + ("a",) + t[4:],
        "identity_default": lambda t: t[:3] + ("d",) + t[4:],
        "default": lambda t: t[:4] + (True,),
    }
    for column in exact:
        for name, mutate in variants.items():
            state = {**exact, column: mutate(exact[column])}
            assert pr132_violations("r", state) == [f"mismatch:{column}"], (column, name)
        missing = {k: v for k, v in exact.items() if k != column}
        assert pr132_violations("r", missing) == [f"missing:{column}"]
    extra = {**exact, "unrelated": ("integer", True, "", "a", True)}
    assert pr132_violations("r", extra) == []  # only the 11 PR132 columns are contract


def test_level_evidence_digest_vectors_are_frozen():
    from decimal import Decimal

    from src.workers._nav_policy import level_evidence_digest

    assert level_evidence_digest(
        dt.date(2026, 9, 22), Decimal("100.12"), Decimal("100.12"),
        "tiingo", "adjusted", "USD", "none",
    ) == "3e579fa89be3638c7628e37f43b11a48b759b54a34be16e9bf0458785d668770"
    # Numerics are rendered at NUMERIC(18,6) scale; NULLs are JSON null.
    assert level_evidence_digest(
        dt.date(2026, 9, 21), 5, None, None, None, None, None,
    ) == "bfa9840f79ad4dea3f7c680eeb2b7d52622a7ed4b9eb72635faa31a422addccb"
