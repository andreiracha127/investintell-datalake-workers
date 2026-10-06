"""Pure rules of scripts/repair_fund_catalog_identity_v1.py (no database).

The two legacy defects are built with the shared NAV identity fixtures: an IU
ISIN that is the registry series id (a), and a registry ticker/class naming a
sibling share class of the IU ticker's class in the same series (b). The
classification assertions run the generator's own classifier, so they pin the
repair to the gates the next policy build will apply.
"""

from __future__ import annotations

import datetime as dt
import json
import random
import uuid

import pytest

from scripts import repair_fund_catalog_identity_v1 as repair
from tests._nav_identity_fixtures import SEC_AT, catalog, entity, sec_row

OBSERVED = dt.datetime(2026, 9, 23, 10, 0, tzinfo=dt.timezone.utc)


def _uid(n: int) -> str:
    return str(uuid.UUID(int=n))


def series_isin(n: int, **kwargs):
    """Defect (a): the IU ISIN is the instrument's own series id."""
    return entity(n, iu_isin=f"S{n:09d}", **kwargs)


def sibling_class(n: int, *, iu_ticker: str | None = None, **kwargs):
    """Defect (b): IU holds class ``I<n>``; the registry names sibling ``R<n>``."""
    iu, fund, registry = entity(n, ticker=f"R{n}", class_id=f"C{n:09d}", **kwargs)
    iu["ticker"] = iu_ticker or f"I{n}"
    return iu, fund, registry


def iu_class_row(n: int, **kwargs) -> dict:
    """The SEC row proving the IU ticker is a class of the registry series."""
    base = {"class_id": f"C{n + 500_000:09d}", "series": f"S{n:09d}", "ticker": f"I{n}"}
    return sec_row(n, **{**base, **kwargs})


def _plan(*entities, sec_extra=(), sec=None):
    instruments, funds, identity, sec_rows = catalog(
        *entities, sec=sec, sec_extra=sec_extra
    )
    return (
        repair.plan_repairs(instruments, funds, identity, sec_rows, OBSERVED),
        (instruments, funds, identity, sec_rows),
    )


def _status(rows, plan=None):
    instruments, funds, identity, sec_rows = rows
    if plan is not None:
        instruments, funds, identity = plan.instruments, plan.funds, plan.identity
    return repair.classify(instruments, funds, identity, sec_rows, OBSERVED, None)


# ── (a) series id stored as ISIN ────────────────────────────────────────────


def test_series_id_isin_is_nulled_and_the_fund_becomes_active():
    plan, rows = _plan(series_isin(1), entity(2))
    assert plan.isin_changes == [
        {"instrument_id": _uid(1), "isin_before": "S000000001", "isin_after": None}
    ]
    assert plan.ticker_changes == []
    before, after = _status(rows), _status(rows, plan)
    assert before["first"][_uid(1)] == "isin.unsupported_prefix"
    assert after["status"][_uid(1)] == "ACTIVE"
    assert after["status"][_uid(2)] == "ACTIVE"  # control untouched
    assert repair.compare(before, after, plan)["gained_active"] == 1


def test_series_like_isin_keeps_raw_before_value_and_matches_normalized():
    plan, _rows = _plan(entity(3, iu_isin=" s000000003 "))
    assert plan.isin_changes == [
        {"instrument_id": _uid(3), "isin_before": " s000000003 ", "isin_after": None}
    ]


@pytest.mark.parametrize(
    ("subject", "reason"),
    [
        (entity(4, iu_isin="S000000999"), "registry_series_differs"),
        (
            (entity(5, iu_isin="S000000005")[0], None, None),
            "registry_row_missing_or_duplicate",
        ),
    ],
)
def test_series_like_isin_not_proven_by_the_registry_is_excluded(subject, reason):
    plan, _rows = _plan(subject, entity(6))
    assert plan.isin_changes == []
    assert plan.excluded["isin"] == {reason: 1}


def test_real_isins_and_absent_isins_are_never_planned():
    plan, _rows = _plan(
        entity(7), entity(8, iu_isin=None), entity(9, iu_isin="XS0000000009")
    )
    assert plan.isin_changes == [] and not plan.excluded["isin"]


# ── (b) registry names a sibling share class ────────────────────────────────


def test_registry_ticker_and_class_follow_the_iu_class_proven_by_sec():
    plan, rows = _plan(sibling_class(10), sec_extra=[iu_class_row(10)])
    assert plan.ticker_changes == [
        {
            "instrument_id": _uid(10),
            "sec_series_id": "S000000010",
            "ticker_before": "R10",
            "ticker_after": "I10",
            "sec_class_id_before": "C000000010",
            "sec_class_id_after": "C000500010",
        }
    ]
    assert plan.evidence == {_uid(10): SEC_AT.isoformat(timespec="microseconds")}
    (registry,) = plan.identity
    (fund,) = plan.funds
    assert (registry["ticker"], registry["sec_class_id"], fund["ticker"]) == (
        "I10",
        "C000500010",
        "I10",
    )
    # Class-level claims are left exactly as they were.
    (original,) = rows[2]
    assert all(registry[k] == original[k] for k in ("isin", "cusip_9", "figi"))
    before, after = _status(rows), _status(rows, plan)
    assert before["first"][_uid(10)] == "ticker.mismatch"
    assert after["status"][_uid(10)] == "ACTIVE"


def test_both_defects_on_one_fund_are_repaired_together():
    iu, fund, registry = sibling_class(11)
    iu["isin"] = "S000000011"
    plan, rows = _plan((iu, fund, registry), sec_extra=[iu_class_row(11)])
    assert len(plan.isin_changes) == len(plan.ticker_changes) == 1
    assert _status(rows, plan)["status"][_uid(11)] == "ACTIVE"


def _stale(n):
    return iu_class_row(n, synced=OBSERVED - dt.timedelta(days=7, microseconds=1))


@pytest.mark.parametrize(
    ("subject", "extra", "reason"),
    [
        (sibling_class(20), [], "sec_iu_ticker_not_fresh"),
        (sibling_class(21), [_stale(21)], "sec_iu_ticker_not_fresh"),
        (
            sibling_class(22),
            [iu_class_row(22, series="S000000999")],
            "sec_iu_ticker_other_series",
        ),
        (
            sibling_class(23),
            [iu_class_row(23), iu_class_row(23, class_id="C000700023")],
            "sec_iu_ticker_ambiguous_class",
        ),
        (
            sibling_class(24),
            [
                iu_class_row(24),
                iu_class_row(24, class_id="C000700024", series="S000000998"),
            ],
            "sec_judge_contradiction",
        ),
        (
            sibling_class(25, conflict={"ticker": {"values": []}}),
            [iu_class_row(25)],
            "registry_conflict_state_not_empty",
        ),
        (
            sibling_class(26, status="provisional"),
            [iu_class_row(26)],
            "registry_not_canonical",
        ),
        (
            sibling_class(27, series="X27"),
            [iu_class_row(27, series="X27")],
            "registry_series_invalid",
        ),
    ],
)
def test_ticker_repair_requires_a_unique_fresh_same_series_sec_proof(
    subject, extra, reason
):
    plan, _rows = _plan(subject, sec_extra=extra)
    assert plan.ticker_changes == []
    assert plan.excluded["ticker"] == {reason: 1}


def _registry_class_row(n: int) -> dict:
    return sec_row(n, class_id=f"C{n:09d}", series=f"S{n:09d}", ticker=f"R{n}")


def test_ticker_or_class_claimed_by_another_instrument_is_excluded():
    # SEC proves fund 30's IU class, but another instrument's registry (and
    # funds_v) already carries that ticker: never create a global conflict.
    other = entity(31, ticker="I30")
    sec = [_registry_class_row(30), iu_class_row(30), sec_row(31, ticker="Z31")]
    plan, _rows = _plan(sibling_class(30), other, sec=sec)
    assert plan.ticker_changes == []
    assert plan.excluded["ticker"] == {"ticker_claimed_by_other_instrument": 1}
    # Another registry row already declares the IU class of fund 32.
    other = entity(33, class_id="C000500032")
    sec = [_registry_class_row(32), iu_class_row(32), sec_row(33)]
    plan, _rows = _plan(sibling_class(32), other, sec=sec)
    assert plan.excluded["ticker"] == {"class_claimed_by_other_instrument": 1}


def test_chained_repairs_converge_and_a_replan_is_empty():
    # Fund 41's IU ticker R40 is the registry ticker of fund 40 until 40 is
    # repaired; the second pass then admits 41. A replan of the result is empty.
    first = sibling_class(40)
    iu, fund, registry = entity(41, ticker="X41", class_id="C000000041")
    iu["ticker"] = "R40"
    second = (iu, fund, registry)
    sec = [
        sec_row(40, class_id="C000000040", series="S000000040", ticker="X40"),
        iu_class_row(40),
        sec_row(41, class_id="C000000041", series="S000000041", ticker="X41"),
        sec_row(41, class_id="C000600041", series="S000000041", ticker="R40"),
    ]
    plan, rows = _plan(first, second, sec=sec)
    assert plan.passes == 2
    assert [c["instrument_id"] for c in plan.ticker_changes] == [_uid(40), _uid(41)]
    replan = repair.plan_repairs(
        plan.instruments, plan.funds, plan.identity, rows[3], OBSERVED
    )
    assert replan.empty()
    after = _status(rows, plan)
    assert after["status"][_uid(40)] == after["status"][_uid(41)] == "ACTIVE"


def test_plan_digest_is_independent_of_row_order():
    entities = [series_isin(50), sibling_class(51), series_isin(52), entity(53)]
    extra = [iu_class_row(51)]
    plan, rows = _plan(*entities, sec_extra=extra)
    shuffled = [list(source) for source in rows]
    random.Random(7).shuffle(shuffled[0])
    random.Random(8).shuffle(shuffled[2])
    again = repair.plan_repairs(*shuffled, OBSERVED)
    assert again.sha256() == plan.sha256()
    assert len(plan.sha256()) == 64


# ── guards ──────────────────────────────────────────────────────────────────


def test_unmasked_contradiction_is_reported_but_does_not_block():
    # Fund 60's ISIN fix lets it reach the SEC stage, where its (untouched)
    # registry ticker also maps to a class of another series.
    sec = [
        sec_row(60, class_id="C000000060", series="S000000060", ticker="T60"),
        sec_row(61, class_id="C000000961", series="S000000961", ticker="T60"),
    ]
    plan, rows = _plan(series_isin(60), sec=sec)
    before, after = _status(rows), _status(rows, plan)
    assert after["first"][_uid(60)] == "sec.contradiction"
    outcome = repair.compare(before, after, plan)
    assert outcome["sec_integrity_after"] == {
        "total": 1,
        "pre_existing": 0,
        "on_ticker_repaired_rows": 0,
        "unmasked_by_isin_repair": 1,
        "other": 0,
    }
    assert repair.guard_violations(before, after, plan) == []
    assert after["summary"]["gates"]["a8"]["integrity_zero"] is False


def test_guard_refuses_demotion_and_integrity_failures_on_repaired_rows():
    plan, _rows = _plan(sibling_class(70), sec_extra=[iu_class_row(70)])
    uid = _uid(70)
    before = {"status": {uid: "ACTIVE"}, "first": {uid: None}}
    after = {"status": {uid: "UNKNOWN"}, "first": {uid: "sec.ambiguous"}}
    assert repair.guard_violations(before, after, plan) == [
        "repair_would_demote_active",
        "repair_would_create_sec_integrity_failure",
    ]


def test_gate_preview_matches_the_generator_counts():
    plan, rows = _plan(
        series_isin(80), sibling_class(81), entity(82), sec_extra=[iu_class_row(81)]
    )
    after = _status(rows, plan)["summary"]
    assert after["active"] == 3
    assert after["gates"]["a4"]["structural_daily"] == 3
    assert after["gates"]["a4"]["structural_daily_subset_of_baseline"] is True
    assert after["gates"]["a8"] == {
        "reaching_sec": 3,
        "bound": 0,
        "stale": 0,
        "missing": 0,
        "integrity": 0,
        "stale_within_bound": True,
        "missing_within_bound": True,
        "integrity_zero": True,
    }


# ── CLI validation (no database is reached) ─────────────────────────────────


@pytest.mark.parametrize(
    ("argv", "code"),
    [
        (["--apply"], "confirmation_required"),
        (["--apply", "--confirm", "yes"], "confirmation_required"),
        (["--apply", "--confirm", repair.CONFIRM_TOKEN], "plan_sha256_required"),
        (
            ["--apply", "--confirm", repair.CONFIRM_TOKEN, "--plan-sha256", "AB" * 32],
            "plan_sha256_required",
        ),
        (["--plan-sha256", "ab" * 32], "plan_sha256_is_apply_only"),
        (["--rollback", "nope", "--confirm", repair.CONFIRM_TOKEN], "run_id_invalid"),
        (["--dsn-env", "REPAIR_TEST_UNSET_DSN"], "dsn_environment_missing"),
        (["--dsn-env", "lower-case"], "dsn_environment_missing"),
    ],
)
def test_cli_refuses_before_touching_the_database(argv, code, capsys, monkeypatch):
    monkeypatch.delenv("REPAIR_TEST_UNSET_DSN", raising=False)
    monkeypatch.delenv(repair.DEFAULT_DSN_ENV, raising=False)
    assert repair.main(argv) == repair.EXIT_FAILED
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked" and payload["code"] == code


def test_cli_apply_and_rollback_are_mutually_exclusive(capsys):
    with pytest.raises(SystemExit):
        repair.main(["--apply", "--rollback", str(uuid.uuid4())])
