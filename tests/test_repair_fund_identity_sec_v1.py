"""Pure decision rules of scripts/repair_fund_identity_sec_v1 (no database, no network)."""

from __future__ import annotations

import datetime as dt
import hashlib
import json

import pytest

from scripts import repair_fund_identity_sec_v1 as repair

NOW = dt.datetime(2026, 10, 6, 6, 0, tzinfo=dt.timezone.utc)
FRESH = NOW - dt.timedelta(hours=3)
STALE = NOW - dt.timedelta(days=20)


def iu(iid, ticker, *, active=True, isin=None, historical=False, kind="fund", excluded=None):
    return iid, {"instrument_id": iid, "instrument_type": kind, "ticker": ticker, "isin": isin,
                 "is_active": active, "historical_sibling": historical, "excluded": excluded}


def reg(iid, ticker, series, cls, *, cik="0000000001", conflict=None):
    return iid, {"instrument_id": iid, "sec_series_id": series, "sec_class_id": cls, "ticker": ticker,
                 "cik_padded": cik, "cik_unpadded": str(int(cik)) if cik else None,
                 "conflict_state": conflict if conflict is not None else {}, "identity_sources": {},
                 "resolution_status": "canonical"}


def sec(cls, series, ticker, synced=FRESH, cik="1"):
    return {"class_id": cls, "series_id": series, "ticker": ticker, "cik": cik, "synced_at": synced}


def snapshot(instruments, registry, secs, *, funds=None, nav_last=None, attempts=None):
    instruments = dict(instruments)
    return repair.Snapshot(
        decision_at=NOW,
        instruments=instruments,
        registry=dict(registry),
        funds=set(instruments) if funds is None else set(funds),
        sec=secs,
        nav_last=nav_last or {},
        tiingo_attempts=attempts or {},
    )


def history(rows):
    """rows: (year, class_id, series_id, ticker)."""
    return repair.build_history([(y, c, s, "0000000001", t) for y, c, s, t in rows])


RETAIL_NCEN = {"fund_types": ["Exchange-Traded Fund", "Index Fund"], "accession_no": "0000932471-26-001",
               "filed_at": "2026-03-12T16:00:00-04:00", "registrant_cik": "36405"}
# Every test series not named otherwise has a current, non-insurance N-CEN.
NCEN_DEFAULT = {s: RETAIL_NCEN for s in ("S000000001", "S000002848", "S000081376", "S000000003")}


def evidence(tiingo=None, filings=None, ncen=None, prospectus=None):
    doc = {"kind": repair.EVIDENCE_KIND, "tiingo_meta": tiingo or {},
           "class_ticker_filings": filings or [], "class_last_filings": [], "series_last_filings": [],
           "ncen_series": NCEN_DEFAULT if ncen is None else ncen,
           "insurance_prospectus": prospectus or []}
    raw = json.dumps(doc).encode()
    return repair.parse_evidence(raw, hashlib.sha256(raw).hexdigest())


def tiingo_ok(end="2026-10-05"):
    return {"status": 200, "endDate": end, "observed_at": "2026-10-06T05:00:00+00:00"}


def changes(plan, snap):
    return {(c["relation"], c["instrument_id"]): c for c in plan.changes(snap)}


CURRENT = {t: tiingo_ok() for t in ("NEW", "LIVEX", "ACVU", "ADME", "VTI")}


def run(snap, hist=None, ev=None, **kw):
    plan = repair.plan_repairs(snap, hist or history([]), ev or evidence(CURRENT), **kw)
    return plan, changes(plan, snap)


# R1 -----------------------------------------------------------------------
@pytest.mark.parametrize("value", ["S000004310", "C000012040", "0001841440", "1604174", " s000004310 "])
def test_r1_nulls_edgar_identifiers_stored_as_isin(value):
    snap = snapshot([iu("a", "AAA", isin=value)], [reg("a", "AAA", "S000000001", None)],
                    [sec("C000000001", "S000000001", "AAA")])
    _plan, out = run(snap)
    change = out[("instruments_universe", "a")]
    assert change["after"]["isin"] is None and change["rules"] == [repair.RULES[0]]


@pytest.mark.parametrize("value", ["US0378331005", "LU1829219390", None])
def test_r1_keeps_real_isins(value):
    snap = snapshot([iu("a", "AAA", isin=value)], [reg("a", "AAA", "S000000001", None)],
                    [sec("C000000001", "S000000001", "AAA")])
    assert run(snap)[1] == {}


# R2 -----------------------------------------------------------------------
def test_r2_renames_same_class_proven_by_dataset():
    snap = snapshot([iu("a", "OLD")], [reg("a", "NEW", "S000000001", "C000000001")],
                    [sec("C000000001", "S000000001", "NEW")])
    hist = history([(2025, "C000000001", "S000000001", "OLD")])
    plan, out = run(snap, hist)
    change = out[("instruments_universe", "a")]
    assert change["after"]["ticker"] == "NEW"
    assert ("instrument_identity", "a") not in out  # registry already current
    facts = change["evidence"][repair.RULES[1]]
    assert facts["dataset_years_with_old_ticker"] == [2025] and facts["class_id"] == "C000000001"


def test_r2_renames_registry_and_fills_class_from_dataset():
    snap = snapshot([iu("a", "OLD")], [reg("a", "OLD", "S000000001", None)],
                    [sec("C000000001", "S000000001", "NEW")])
    hist = history([(2026, "C000000001", "S000000001", "OLD")])
    _plan, out = run(snap, hist)
    registry = out[("instrument_identity", "a")]["after"]
    assert registry["ticker"] == "NEW" and registry["sec_class_id"] == "C000000001"
    assert registry["identity_sources"]["ticker"]["source"] == "sec_company_tickers_mf"


def test_r2_accepts_pinned_filing_when_dataset_lacks_old_ticker():
    snap = snapshot([iu("a", "BEMO", active=False)], [reg("a", "ADME", "S000000001", "C000000001")],
                    [sec("C000000001", "S000000001", "ADME")])
    filing = {"class_id": "C000000001", "ticker": "BEMO", "series_id": "S000000001",
              "role": "last_with_ticker", "accession_no": "0000894189-19-007478",
              "form_type": "N-14", "filed_at": "2019-11-06"}
    assert run(snap)[1] == {}  # unproven: no dataset row, no filing
    _plan, out = run(snap, ev=evidence(CURRENT, filings=[filing]))
    assert out[("instruments_universe", "a")]["after"]["ticker"] == "ADME"


def test_r2_ignores_stale_sec_rows_and_unproven_classes():
    stale = snapshot([iu("a", "OLD")], [reg("a", "NEW", "S000000001", "C000000001")],
                     [sec("C000000001", "S000000001", "NEW", synced=STALE)])
    hist = history([(2025, "C000000001", "S000000001", "OLD")])
    assert run(stale, hist)[1] == {}
    other_class = history([(2025, "C000000009", "S000000001", "OLD")])
    fresh = snapshot([iu("a", "OLD")], [reg("a", "NEW", "S000000001", "C000000001")],
                     [sec("C000000001", "S000000001", "NEW")])
    assert run(fresh, other_class)[1] == {}


def test_r2_keeps_the_old_ticker_until_tiingo_serves_the_new_one():
    snap = snapshot([iu("a", "OLD")], [reg("a", "NEW", "S000000001", "C000000001")],
                    [sec("C000000001", "S000000001", "NEW")])
    hist = history([(2025, "C000000001", "S000000001", "OLD")])
    for meta in ({}, {"NEW": {"status": 200, "observed_at": "2026-10-06T05:00:00+00:00"}},
                 {"NEW": tiingo_ok(end="2026-06-08")}):
        plan, out = run(snap, hist, ev=evidence(meta))
        assert out == {} and plan.review["ticker_renamed_no_current_tiingo"]


def test_r2_target_taken_or_series_moved_goes_to_review():
    hist = history([(2025, "C000000001", "S000000001", "OLD")])
    taken = snapshot([iu("a", "OLD"), iu("b", "NEW")],
                     [reg("a", "OLD", "S000000001", "C000000001")],
                     [sec("C000000001", "S000000001", "NEW")])
    plan, out = run(taken, hist)
    assert out == {} and plan.review["ticker_renamed_target_taken"]
    moved = snapshot([iu("a", "OLD")], [reg("a", "OLD", "S000000001", "C000000001")],
                     [sec("C000000001", "S000000002", "NEW")])
    plan, out = run(moved, hist)
    assert out == {} and plan.review["ticker_renamed_series_moved"]


# R3 -----------------------------------------------------------------------
def _repoint_case(old_class_current=False):
    snap = snapshot([iu("a", "DEADX")], [reg("a", "LIVEX", "S000000001", "C000000002")],
                    [sec("C000000002", "S000000001", "LIVEX")])
    rows = [(2025, "C000000001", "S000000001", "DEADX"), (2026, "C000000002", "S000000001", "LIVEX")]
    if old_class_current:
        rows.append((2026, "C000000001", "S000000001", None))
    return snap, history(rows)


def test_r3_is_review_only_unless_enabled():
    snap, hist = _repoint_case()
    plan, out = run(snap, hist)
    assert out == {} and plan.review["class_repoint_candidate"][0]["nav_rebase_required"] is True
    _plan, out = run(snap, hist, include_class_repoint=True)
    change = out[("instruments_universe", "a")]
    assert change["after"]["ticker"] == "LIVEX" and change["rules"] == [repair.RULES[2]]


def test_r3_requires_a_current_tiingo_history_for_the_live_class():
    snap, hist = _repoint_case()
    plan, out = run(snap, hist, ev=evidence({}), include_class_repoint=True)
    assert out == {} and plan.review["class_repoint_no_current_tiingo"]


def test_r3_requires_the_old_class_to_be_terminated():
    snap, hist = _repoint_case(old_class_current=True)
    assert run(snap, hist, include_class_repoint=True)[1] == {}


def test_r3_flags_registry_pointing_at_terminated_class():
    snap = snapshot([iu("a", "DEADX")], [reg("a", "DEADX", "S000000001", "C000000001")],
                    [sec("C000000002", "S000000001", "LIVEX")])
    hist = history([(2025, "C000000001", "S000000001", "DEADX"), (2026, "C000000002", "S000000001", "LIVEX")])
    plan, out = run(snap, hist, include_class_repoint=True)
    assert out == {} and plan.review["registry_class_terminated"]


# R4 -----------------------------------------------------------------------
def test_r4_drops_only_sec_resolved_conflict_keys():
    conflict = {
        "ticker": {"values": [{"value": "OLDX"}, {"value": "NEWX"}], "resolved": False},
        "sec_class_id": {"values": [{"value": "C000000001"}, {"value": "C000000002"}], "resolved": False},
        "sec_private_fund_id": {"values": [{"value": "805-1"}, {"value": "805-2"}], "resolved": False},
    }
    snap = snapshot([iu("a", "NEWX")],
                    [reg("a", "NEWX", "S000000001", "C000000002", cik="0000923202", conflict=conflict)],
                    [sec("C000000002", "S000000001", "NEWX", cik="923202")])
    plan, out = run(snap)
    after = out[("instrument_identity", "a")]["after"]["conflict_state"]
    assert set(after) == {"sec_private_fund_id"}
    assert plan.review["conflict_not_sec_resolvable"][0]["keys"] == ["sec_private_fund_id"]


def test_r4_leaves_conflict_when_registry_disagrees_with_sec():
    conflict = {"ticker": {"values": [{"value": "OLDX"}, {"value": "NEWX"}], "resolved": False}}
    snap = snapshot([iu("a", "OLDX")], [reg("a", "OLDX", "S000000001", "C000000002", conflict=conflict)],
                    [sec("C000000002", "S000000001", "NEWX")])
    assert ("instrument_identity", "a") not in run(snap)[1]


# R5 -----------------------------------------------------------------------
def _orphan(**kw):
    return snapshot([iu("a", "VTI", active=False, **kw)], [reg("a", "VTI", "S000000001", "C000000001")],
                    [sec("C000000001", "S000000001", "VTI")])


def test_r5_activates_live_orphan_with_current_tiingo():
    _plan, out = run(_orphan(), ev=evidence({"VTI": tiingo_ok()}))
    change = out[("instruments_universe", "a")]
    assert change["after"]["is_active"] is True and change["rules"] == [repair.RULES[4]]


@pytest.mark.parametrize("meta", [None, {"status": 404, "observed_at": "2026-10-06T05:00:00+00:00"},
                                  tiingo_ok(end="2026-07-01")])
def test_r5_needs_current_tiingo_nav(meta):
    plan, out = run(_orphan(), ev=evidence({"VTI": meta} if meta else {}))
    assert out == {} and plan.review["orphan_no_current_tiingo_nav"]


def test_r5_skips_series_with_active_sibling_historical_and_non_members():
    snap = snapshot([iu("a", "VTI", active=False), iu("b", "VTSAX")],
                    [reg("a", "VTI", "S000000001", "C000000001"), reg("b", "VTSAX", "S000000001", "C000000002")],
                    [sec("C000000001", "S000000001", "VTI"), sec("C000000002", "S000000001", "VTSAX")])
    assert run(snap, ev=evidence({"VTI": tiingo_ok()}))[1] == {}
    plan, out = run(_orphan(historical=True), ev=evidence({"VTI": tiingo_ok()}))
    assert out == {} and plan.review["orphan_series_historical_sibling"]
    plan, out = run(_orphan(excluded="leveraged_inverse"), ev=evidence({"VTI": tiingo_ok()}))
    assert out == {} and plan.review["orphan_deliberately_excluded"][0]["reason"] == "leveraged_inverse"
    outside = snapshot([iu("a", "VTI", active=False)], [reg("a", "VTI", "S000000001", "C000000001")],
                       [sec("C000000001", "S000000001", "VTI")], funds=set())
    assert run(outside, ev=evidence({"VTI": tiingo_ok()}))[1] == {}


def test_r5_activates_every_proven_live_class_of_an_orphan_series():
    snap = snapshot([iu("a", "VTI", active=False), iu("b", "VSTSX", active=False)],
                    [reg("a", "VTI", "S000002848", "C000007808"), reg("b", "VSTSX", "S000002848", "C000170276")],
                    [sec("C000007808", "S000002848", "VTI"), sec("C000170276", "S000002848", "VSTSX")])
    _plan, out = run(snap, ev=evidence({"VTI": tiingo_ok(), "VSTSX": tiingo_ok()}))
    assert {k[1] for k in out} == {"a", "b"}
    assert out[("instruments_universe", "a")]["evidence"][repair.RULES[4]]["series_candidates"] == 2
    _plan, out = run(snap, ev=evidence({"VTI": tiingo_ok()}))
    assert set(out) == {("instruments_universe", "a")}


# R6 -----------------------------------------------------------------------
def _dead(nav, attempt=("success_no_new", None), current_series=False):
    snap = snapshot([iu("a", "DEADX")], [reg("a", "DEADX", "S000000001", "C000000001")], [],
                    nav_last={"a": nav}, attempts={"a": attempt} if attempt else {})
    rows = [(2024, "C000000001", "S000000001", "DEADX")]
    if current_series:
        rows.append((2026, "C000000009", "S000000001", "LIVEX"))
    return snap, history(rows)


def test_r6_deactivates_terminated_series_with_stale_nav():
    snap, hist = _dead(dt.date(2024, 12, 27))
    _plan, out = run(snap, hist)
    change = out[("instruments_universe", "a")]
    assert change["after"]["is_active"] is False and change["rules"] == [repair.RULES[5]]


def test_r6_keeps_live_series_recent_nav_and_successful_fetches():
    snap, hist = _dead(dt.date(2024, 12, 27), current_series=True)
    assert run(snap, hist)[1] == {}
    snap, hist = _dead(dt.date(2026, 10, 2))
    plan, out = run(snap, hist)
    assert out == {} and plan.review["series_terminated_nav_current"]
    snap, hist = _dead(dt.date(2024, 12, 27), attempt=("success_new", dt.date(2026, 10, 5)))
    assert run(snap, hist)[1] == {}


def test_r6_never_acts_on_tickers_unknown_to_sec():
    snap = snapshot([iu("a", "GLD")], [], [], nav_last={"a": dt.date(2020, 1, 2)})
    assert run(snap)[1] == {}


# Plan properties ------------------------------------------------------------
def _apply(snap, plan):
    for iid, row in plan.iu_after.items():
        snap.instruments[iid] = {k: row[k] for k in snap.instruments[iid]}
    for iid, row in plan.registry_after.items():
        snap.registry[iid] = {k: row[k] for k in snap.registry[iid]}


def test_replan_after_apply_is_a_noop_and_digest_is_stable():
    conflict = {"ticker": {"values": [{"value": "QUVU"}, {"value": "ACVU"}], "resolved": False}}
    snap = snapshot(
        [iu("a", "QUVU", isin="S000081376"), iu("b", "VTI", active=False), iu("c", "DEADX")],
        [reg("a", "QUVU", "S000081376", "C000244148", conflict=conflict),
         reg("b", "VTI", "S000002848", "C000007808"), reg("c", "DEADX", "S000000003", "C000000003")],
        [sec("C000244148", "S000081376", "ACVU"), sec("C000007808", "S000002848", "VTI")],
        nav_last={"c": dt.date(2024, 1, 2)}, attempts={"c": ("empty", None)},
    )
    hist = history([(2025, "C000244148", "S000081376", "QUVU"), (2024, "C000000003", "S000000003", "DEADX")])
    ev = evidence({"VTI": tiingo_ok(), "ACVU": tiingo_ok()})
    plan = repair.plan_repairs(snap, hist, ev)
    pins = {"x": 1}
    assert repair.plan_digest(plan, snap, pins) == repair.plan_digest(
        repair.plan_repairs(snap, hist, ev), snap, pins)
    out = changes(plan, snap)
    assert out[("instruments_universe", "a")]["after"] == {"ticker": "ACVU", "isin": None, "is_active": True}
    assert out[("instrument_identity", "a")]["after"]["conflict_state"] == {}
    assert out[("instruments_universe", "b")]["after"]["is_active"] is True
    assert out[("instruments_universe", "c")]["after"]["is_active"] is False
    _apply(snap, plan)
    assert repair.plan_repairs(snap, hist, ev).changes(snap) == []


def test_series_class_parser_handles_bom_null_tickers_and_malformed_rows():
    rows = [
        {"﻿Reporting File Number": "811-1", "CIK Number": "0000036405", "Series ID": "S000002848",
         "Class ID": "C000007808", "Class Ticker": "vti"},
        {"CIK Number": "1", "Series ID": "S000000001", "Class ID": "C000000001", "Class Ticker": "[NULL]"},
        {"CIK Number": "1", "Series ID": "S000000001", "Class ID": "", "Class Ticker": "X"},
    ]
    parsed = repair.parse_series_class_rows(2026, rows)
    assert parsed == [(2026, "C000007808", "S000002848", "0000036405", "VTI"),
                      (2026, "C000000001", "S000000001", "0000000001", None)]


def test_generator_rows_follow_the_plan_and_keep_the_projection():
    snap = snapshot([iu("a", "OLD")], [reg("a", "OLD", "S000000001", None)],
                    [sec("C000000001", "S000000001", "NEW")])
    plan = repair.plan_repairs(snap, history([(2026, "C000000001", "S000000001", "OLD")]), evidence(CURRENT))
    instruments = [{"instrument_id": "a", "instrument_type": "fund", "ticker": "OLD", "isin": None,
                    "currency": "USD", "is_active": True}]
    funds = [{"instrument_id": "a", "series_id": "S000000001", "ticker": "OLD", "isin": None,
              "cusip": None, "currency": "USD", "fund_type": "etf"}]
    identity = [{"instrument_id": "a", "sec_series_id": "S000000001", "sec_class_id": None, "ticker": "OLD",
                 "isin": None, "cusip_9": None, "figi": None, "resolution_status": "canonical",
                 "conflict_state": {}}]
    i2, f2, d2 = repair.apply_plan_to_generator_rows(plan, snap, instruments, funds, identity)
    assert i2[0]["ticker"] == f2[0]["ticker"] == d2[0]["ticker"] == "NEW"
    assert d2[0]["sec_class_id"] == "C000000001" and instruments[0]["ticker"] == "OLD"


# R7 -----------------------------------------------------------------------
def test_r7_fills_registry_series_class_and_cik_from_unique_sec_row():
    snap = snapshot([iu("a", "HYG")], [reg("a", "HYG", None, None, cik=None)],
                    [sec("C000045000", "S000016772", "HYG", cik="1100663")], funds=set())
    _plan, out = run(snap)
    after = out[("instrument_identity", "a")]["after"]
    assert (after["sec_series_id"], after["sec_class_id"], after["cik_padded"], after["cik_unpadded"]) == (
        "S000016772", "C000045000", "0001100663", "1100663")
    assert out[("instrument_identity", "a")]["rules"] == [repair.RULES[6]]


def test_r7_needs_a_unique_current_row_and_a_consistent_registry():
    shared = snapshot([iu("a", "HYG")], [reg("a", "HYG", None, None, cik=None)],
                      [sec("C000045000", "S000016772", "HYG"), sec("C000045001", "S000016773", "HYG")])
    assert run(shared)[1] == {}
    other_ticker = snapshot([iu("a", "HYG")], [reg("a", "JNK", None, None, cik=None)],
                            [sec("C000045000", "S000016772", "HYG")])
    assert run(other_ticker)[1] == {}
    wrong_cik = snapshot([iu("a", "HYG")], [reg("a", "HYG", None, None, cik="0000000099")],
                         [sec("C000045000", "S000016772", "HYG", cik="1100663")])
    plan, out = run(wrong_cik)
    assert out == {} and plan.review["registry_cik_disagrees_with_sec"]


def test_host_override_keeps_credentials_and_database():
    dsn = "postgres://worker_writer:p%2Fw@postgres.railway.internal:5432/railway?sslmode=require"
    assert repair.override_host(dsn, "proxy.example:36616") == (
        "postgresql://worker_writer:p%2Fw@proxy.example:36616/railway?sslmode=require")
    assert repair.override_host(dsn, None) == dsn
    with pytest.raises(repair.RepairError):
        repair.override_host("host=localhost dbname=x", "proxy.example:1")


def test_committed_evidence_bundle_matches_its_pin():
    evidence = repair.load_evidence()
    assert evidence.sha256 == repair.EVIDENCE_SHA256
    assert evidence.tiingo and evidence.class_ticker_filings and evidence.series_last_filings


# R8 / R9 --------------------------------------------------------------------
OLDER = NOW - dt.timedelta(days=5)  # still inside the 7-day window, not the newest batch


SERIES_OF_CLASS = {"C000259241": "S000091565", "C000082313": "S000027283"}


def _filing(cls, ticker, form, filed, role="last_with_ticker"):
    return {"class_id": cls, "ticker": ticker, "series_id": SERIES_OF_CLASS.get(cls), "role": role,
            "accession_no": f"0000000000-26-{filed.replace('-', '')[2:]}",
            "form_type": form, "filed_at": filed}


def _moved(active=True, funds=None):
    return snapshot([iu("a", "VVPLX", active=active)],
                    [reg("a", "VVPLX", "S000027283", "C000082313", cik="0000915802")],
                    [sec("C000082313", "S000027283", "VVPLX", synced=OLDER, cik="915802"),
                     sec("C000259241", "S000091565", "VVPLX", cik="1936157")], funds=funds)


def test_r8_moves_the_registry_to_secs_newest_series_with_a_newer_filing():
    ev = evidence(CURRENT, filings=[_filing("C000259241", "VVPLX", "NPORT-P", "2026-09-28"),
                                    _filing("C000082313", "VVPLX", "N-CEN", "2026-07-13")])
    _plan, out = run(_moved(), ev=ev)
    after = out[("instrument_identity", "a")]["after"]
    assert (after["sec_series_id"], after["sec_class_id"], after["cik_padded"]) == (
        "S000091565", "C000259241", "0001936157")
    assert out[("instrument_identity", "a")]["rules"] == [repair.RULES[7]]


@pytest.mark.parametrize("filings", [
    [],  # no filing at all
    [_filing("C000259241", "VVPLX", "N-CEN", "2026-09-28")],  # census only
    [_filing("C000259241", "VVPLX", "NPORT-P", "2026-07-01"),
     _filing("C000082313", "VVPLX", "NPORT-P", "2026-09-28")],  # the fund still files under the old series
])
def test_r8_never_moves_when_the_funds_own_filings_disagree(filings):
    plan, out = run(_moved(), ev=evidence(CURRENT, filings=filings))
    assert out == {} and plan.review["series_moved_unproven"]


def test_r9_quarantines_the_contradiction_once_and_only_when_asked():
    filings = [_filing("C000259241", "VVPLX", "N-CEN", "2026-07-14"),
               _filing("C000082313", "VVPLX", "NPORT-P", "2026-09-28")]
    snap = _moved()
    plan, out = run(snap, ev=evidence(CURRENT, filings=filings), quarantine_sec_contradictions=True)
    change = out[("instrument_identity", "a")]
    entry = change["after"]["conflict_state"]["sec_series_id"]
    assert change["rules"] == [repair.RULES[8]] and entry["resolved"] is False
    assert [v["value"] for v in entry["values"]] == ["S000027283", "S000091565"]
    _apply(snap, plan)
    again = repair.plan_repairs(snap, history([]), evidence(CURRENT, filings=filings),
                                quarantine_sec_contradictions=True)
    assert again.changes(snap) == []  # R4 keeps the key, R9 does not repeat


def test_r9_never_quarantines_without_the_funds_own_filings():
    # no pinned filing at all: an unproven move, not an evidenced contradiction
    plan, out = run(_moved(), ev=evidence(CURRENT), quarantine_sec_contradictions=True)
    assert out == {} and plan.review["series_moved_unproven"]


def test_r8_reports_inactive_non_members_and_two_live_series():
    ev = evidence(CURRENT, filings=[_filing("C000259241", "VVPLX", "NPORT-P", "2026-09-28")])
    plan, out = run(_moved(active=False, funds=set()), ev=ev)
    assert out == {} and plan.review["series_moved_inactive_instrument"]
    both = snapshot([iu("a", "VVPLX")], [reg("a", "VVPLX", "S000027283", "C000082313")],
                    [sec("C000082313", "S000099999", "VVPLX"), sec("C000259241", "S000091565", "VVPLX")])
    plan, out = run(both, ev=ev)
    assert out == {} and plan.review["series_moved_ambiguous"]


def test_preview_rekeys_funds_v_only_through_the_eligibility_gate():
    ev = evidence(CURRENT, filings=[_filing("C000259241", "VVPLX", "NPORT-P", "2026-09-28")])
    snap = _moved()
    plan = repair.plan_repairs(snap, history([]), ev)
    funds = [{"instrument_id": "a", "series_id": "S000027283", "ticker": "VVPLX", "isin": None,
              "cusip": None, "currency": "USD", "fund_type": "mutual_fund"}]
    identity = [{"instrument_id": "a", "sec_series_id": "S000027283", "sec_class_id": "C000082313",
                 "ticker": "VVPLX", "isin": None, "cusip_9": None, "figi": None,
                 "resolution_status": "canonical", "conflict_state": {}}]
    _i, kept, _r = repair.apply_plan_to_generator_rows(plan, snap, [], funds, identity,
                                                      eligible_series={"S000091565"})
    assert kept[0]["series_id"] == "S000091565"
    _i, dropped, _r = repair.apply_plan_to_generator_rows(plan, snap, [], funds, identity,
                                                         eligible_series=set())
    assert dropped == []


def test_r4_settles_a_conflict_left_on_a_terminated_class():
    conflict = {
        "ticker": {"values": [{"value": "VMCAX"}, {"value": "VTCLX"}], "resolved": False},
        "sec_class_id": {"values": [{"value": "C000012134"}, {"value": "C000012135"}], "resolved": False},
    }
    snap = snapshot([iu("a", "VTCLX")],
                    [reg("a", "VMCAX", "S000004384", "C000012134", cik="0000923202", conflict=conflict)],
                    [sec("C000012135", "S000004384", "VTCLX", cik="923202")])
    hist = history([(2024, "C000012134", "S000004384", "VMCAX"), (2026, "C000012135", "S000004384", "VTCLX")])
    _plan, out = run(snap, hist)
    after = out[("instrument_identity", "a")]["after"]
    assert (after["ticker"], after["sec_class_id"], after["conflict_state"]) == ("VTCLX", "C000012135", {})
    # the old class still current in SEC: not proven dead, nothing moves
    live = history([(2026, "C000012134", "S000004384", "VMCAX"), (2026, "C000012135", "S000004384", "VTCLX")])
    assert run(snap, live)[1] == {}


# R5 insurance-only exclusion ------------------------------------------------
UNDERLYING_NCEN = {"fund_types": ["Underlying fund"], "accession_no": "0000720318-26-000001",
                   "filed_at": "2026-03-12T16:00:00-04:00", "registrant_cik": "720318"}


def _activated(ev):
    return ("instruments_universe", "a") in run(_orphan(), ev=ev)[1]


def test_r5_keeps_insurance_only_series_inactive_on_ncen_evidence():
    plan, out = run(_orphan(), ev=evidence({"VTI": tiingo_ok()}, ncen={"S000000001": UNDERLYING_NCEN}))
    assert out == {}
    entry = plan.review["orphan_insurance_only_class"][0]
    assert entry["ncen"]["accession_no"] == "0000720318-26-000001" and entry["registrant_cik"] == "0000000001"
    # an ETF used by insurers is still an exchange-traded fund: not insurance-only
    etf = {**UNDERLYING_NCEN, "fund_types": ["Exchange-Traded Fund", "Underlying fund"]}
    assert _activated(evidence({"VTI": tiingo_ok()}, ncen={"S000000001": etf}))


def test_r5_keeps_insurance_only_series_inactive_on_prospectus_evidence():
    insurance = {"registrant_cik": "918294", "accession_no": "0001999371-26-008879", "form_type": "485BPOS",
                 "filed_at": "2026-04-24", "series_ids": ["S000000001"],
                 "quote": "The fund is generally available only through variable annuity or variable "
                          "life insurance contracts."}
    plan, out = run(_orphan(), ev=evidence({"VTI": tiingo_ok()}, prospectus=[insurance]))
    assert out == {} and plan.review["orphan_insurance_only_class"][0]["prospectus"]["accession_no"] == (
        "0001999371-26-008879")
    # a prospectus of another series, or a class sold through several channels, proves nothing
    elsewhere = {**insurance, "series_ids": ["S000000099"]}
    mixed = {**insurance, "quote": "I3 shares are only available to certain funds of funds, registered and "
                                   "unregistered insurance company separate accounts and collective "
                                   "investment trusts."}
    assert _activated(evidence({"VTI": tiingo_ok()}, prospectus=[elsewhere]))
    assert _activated(evidence({"VTI": tiingo_ok()}, prospectus=[mixed]))


def test_r5_needs_a_current_ncen_to_prove_the_class_is_not_insurance_only():
    plan, out = run(_orphan(), ev=evidence({"VTI": tiingo_ok()}, ncen={}))
    assert out == {} and plan.review["orphan_insurance_status_unverified"]
    old = {**RETAIL_NCEN, "filed_at": "2024-01-10T16:00:00-05:00"}
    plan, out = run(_orphan(), ev=evidence({"VTI": tiingo_ok()}, ncen={"S000000001": old}))
    assert out == {} and plan.review["orphan_insurance_status_unverified"]


def test_prospectus_restriction_classifier():
    assert repair.prospectus_says_insurance_only(
        "Each fund offers its shares only to separate accounts of insurance companies that offer "
        "variable annuity and variable life insurance products.")
    assert not repair.prospectus_says_insurance_only("GICs are generally guaranteed only by the insurer.")
    assert not repair.prospectus_says_insurance_only(
        "Shares are offered to the general public and to insurance company separate accounts only.")
    assert not repair.prospectus_says_insurance_only(None)


# Second review round ----------------------------------------------------------
def test_withdrawn_rows_of_an_older_sync_batch_are_not_current():
    # the class's only row is 5 days old while today's batch lists other classes
    snap = snapshot([iu("a", "VTI", active=False)], [reg("a", "VTI", "S000000001", "C000000001")],
                    [sec("C000000001", "S000000001", "VTI", synced=NOW - dt.timedelta(days=5)),
                     sec("C000000009", "S000000009", "OTHER")])
    plan, out = run(snap, ev=evidence({"VTI": tiingo_ok()}))
    assert out == {} and plan.review["orphan_identity_not_sec_current"]


def test_r6_needs_a_dated_nav_to_deactivate():
    snap, hist = _dead(None, attempt=("not_found", None))
    plan, out = run(snap, hist)
    assert out == {} and plan.review["series_terminated_no_dated_nav"]


def test_newest_prospectus_sentence_decides_insurance_status():
    old = {"registrant_cik": "1", "accession_no": "OLD", "form_type": "485BPOS", "filed_at": "2025-04-24",
           "series_ids": ["S000000001"],
           "quote": "The fund is generally available only through variable annuity contracts."}
    new = {**old, "accession_no": "NEW", "filed_at": "2026-04-24",
           "quote": "Shares are offered only to funds of funds and insurance company separate accounts."}
    assert _activated(evidence({"VTI": tiingo_ok()}, prospectus=[old, new]))
    plan, out = run(_orphan(), ev=evidence({"VTI": tiingo_ok()}, prospectus=[new, {**old, "filed_at": "2026-05-01"}]))
    assert out == {} and plan.review["orphan_insurance_only_class"]


# Third review round -----------------------------------------------------------
def test_r8_filing_must_list_the_class_under_the_destination_series():
    elsewhere = {**_filing("C000259241", "VVPLX", "NPORT-P", "2026-09-28"), "series_id": "S000099999"}
    plan, out = run(_moved(), ev=evidence(CURRENT, filings=[elsewhere]))
    assert out == {} and plan.review["series_moved_unproven"]
    under_new = {**elsewhere, "series_id": "S000091565"}
    assert ("instrument_identity", "a") in run(_moved(), ev=evidence(CURRENT, filings=[under_new]))[1]


def test_r7_never_gives_one_sec_class_two_catalog_identities():
    snap = snapshot([iu("a", "HYG"), iu("b", "HYGOLD")],
                    [reg("a", "HYG", None, None, cik=None), reg("b", "HYGOLD", "S000016772", "C000045000")],
                    [sec("C000045000", "S000016772", "HYG", cik="1100663")], funds=set())
    plan, out = run(snap)
    assert ("instrument_identity", "a") not in out and plan.review["registry_class_taken"]


def ticker_file(rows):
    """company_tickers_mf.json carrying (class, series, ticker, cik) rows, through the worker parser."""
    payload = {"fields": ["cik", "seriesId", "classId", "symbol"],
               "data": [[int(cik), series, cls, ticker] for cls, series, ticker, cik in rows]}
    return repair.parse_sec_ticker_file(json.dumps(payload).encode())


def test_only_a_batch_equal_to_the_sec_ticker_file_is_accepted():
    listed = [(f"C{n:09d}", "S000000001", f"T{n}", "1") for n in range(100)]
    file = ticker_file(listed)
    yesterday = NOW - dt.timedelta(days=1)
    full = [sec(c, s, t, synced=yesterday, cik=k) for c, s, t, k in listed]
    assert repair.assert_latest_sec_batch_complete(full, NOW, file)["latest_batch_rows"] == 100
    # A WORKER_LIMIT run re-stamps a 99% prefix: no share of rows can tell it apart.
    prefix = [dict(r, synced_at=FRESH) for r in full[:99]] + full[99:]
    with pytest.raises(repair.RepairError, match="sec_latest_batch_not_the_ticker_file"):
        repair.assert_latest_sec_batch_complete(prefix, NOW, file)
    # SEC changed a mapping after the sync (or the file is older than the sync).
    with pytest.raises(repair.RepairError, match="sec_latest_batch_not_the_ticker_file"):
        repair.assert_latest_sec_batch_complete(full, NOW, ticker_file([*listed[:99], (
            "C000000099", "S000000002", "T99", "1")]))
    with pytest.raises(repair.RepairError, match="sec_crosswalk_not_fresh"):
        repair.assert_latest_sec_batch_complete([sec("C000000001", "S1", "T1", synced=STALE)], NOW, file)
    with pytest.raises(repair.RepairError, match="sec_ticker_file_invalid"):
        repair.parse_sec_ticker_file(b'{"fields": [], "data": []}')


def test_a_previous_run_hours_earlier_is_not_the_newest_batch():
    # Two complete runs a few hours apart: the class dropped from the second
    # keeps the first run's instant and must read as withdrawn.
    snap = snapshot([iu("a", "VTI", active=False)], [reg("a", "VTI", "S000000001", "C000000001")],
                    [sec("C000000001", "S000000001", "VTI", synced=FRESH - dt.timedelta(hours=2)),
                     sec("C000000002", "S000000002", "OTHER", synced=FRESH)])
    plan, out = run(snap, ev=evidence({"VTI": tiingo_ok()}))
    assert out == {} and plan.review["orphan_identity_not_sec_current"]


def test_the_workers_synthetic_class_key_is_never_an_sec_class():
    # sec_company_tickers_mf stores "<series>:<ticker>" when SEC's row has no class id.
    snap = snapshot([iu("a", "HYG")], [reg("a", "HYG", None, None, cik=None)],
                    [sec("S000016772:HYG", "S000016772", "HYG", cik="1100663")], funds=set())
    assert run(snap)[1] == {}


def test_r5_never_activates_a_row_with_a_registry_conflict():
    conflict = {"sec_private_fund_id": {"values": [{"value": "805-1"}, {"value": "805-2"}], "resolved": False}}
    snap = snapshot([iu("a", "VTI", active=False)],
                    [reg("a", "VTI", "S000000001", "C000000001", conflict=conflict)],
                    [sec("C000000001", "S000000001", "VTI")])
    plan, out = run(snap, ev=evidence({"VTI": tiingo_ok()}))
    assert out == {}
    assert plan.review["orphan_registry_conflict"][0]["conflict_keys"] == ["sec_private_fund_id"]


# Fifth review round -----------------------------------------------------------
def test_r2_never_fills_a_class_another_registry_row_owns():
    snap = snapshot([iu("a", "OLD"), iu("b", "STALE")],
                    [reg("a", "OLD", "S000000001", None), reg("b", "STALE", "S000000001", "C000000001")],
                    [sec("C000000001", "S000000001", "NEW")])
    hist = history([(2026, "C000000001", "S000000001", "OLD")])
    plan, out = run(snap, hist)
    assert ("instruments_universe", "a") not in out and ("instrument_identity", "a") not in out
    assert plan.review["ticker_renamed_class_taken"][0]["class_id"] == "C000000001"


def test_r5_never_activates_a_ticker_another_row_claims():
    snap = snapshot([iu("a", "VTI", active=False), iu("b", "VTIX")],
                    [reg("a", "VTI", "S000000001", "C000000001"), reg("b", "VTI", "S000000009", None)],
                    [sec("C000000001", "S000000001", "VTI")], funds={"a"})
    plan, out = run(snap, ev=evidence({"VTI": tiingo_ok()}))
    assert ("instruments_universe", "a") not in out
    assert plan.review["orphan_ticker_claimed_elsewhere"][0]["ticker"] == "VTI"


def test_r9_never_overwrites_a_conflict_state_that_is_not_an_object():
    filings = [_filing("C000259241", "VVPLX", "N-CEN", "2026-07-14"),
               _filing("C000082313", "VVPLX", "NPORT-P", "2026-09-28")]
    snap = _moved()
    snap.registry["a"]["conflict_state"] = ["legacy", "evidence"]
    plan, out = run(snap, ev=evidence(CURRENT, filings=filings), quarantine_sec_contradictions=True)
    assert out == {} and plan.review["quarantine_conflict_state_not_object"]


# Seventh review round ---------------------------------------------------------
@pytest.mark.parametrize("state", [[], False, 0, 5, True, "x", None])
def test_r5_treats_every_non_object_conflict_state_as_a_conflict(state):
    snap = _orphan()
    snap.registry["a"]["conflict_state"] = state
    plan, out = run(snap, ev=evidence({"VTI": tiingo_ok()}))
    assert out == {} and plan.review["orphan_registry_conflict"]


def test_r5_needs_a_canonical_registry_row():
    snap = _orphan()
    snap.registry["a"]["resolution_status"] = "candidate"
    plan, out = run(snap, ev=evidence({"VTI": tiingo_ok()}))
    assert out == {} and plan.review["orphan_registry_not_canonical"]


@pytest.mark.parametrize("padded,unpadded", [(None, "999"), ("0001100663", "999"), ("bad", None)])
def test_r7_reviews_any_stored_cik_form_that_disagrees(padded, unpadded):
    snap = snapshot([iu("a", "HYG")], [reg("a", "HYG", None, None, cik=None)],
                    [sec("C000046846", "S000016772", "HYG", cik="1100663")], funds=set())
    snap.registry["a"].update(cik_padded=padded, cik_unpadded=unpadded)
    plan, out = run(snap)
    assert out == {} and plan.review["registry_cik_disagrees_with_sec"]


def test_r8_never_reads_a_deregistration_as_the_new_series_continuing():
    ev = evidence(CURRENT, filings=[_filing("C000259241", "VVPLX", "N-8F", "2026-09-28"),
                                    _filing("C000082313", "VVPLX", "NPORT-P", "2026-07-13")])
    plan, out = run(_moved(), ev=ev)
    assert out == {} and plan.review["series_moved_unproven"]


# Eighth review round ----------------------------------------------------------
def test_r8_never_follows_a_reused_ticker_away_from_a_current_registry_class():
    # The registry class is still listed (renamed VVPLY); VVPLX now names another fund.
    snap = snapshot([iu("a", "VVPLX")],
                    [reg("a", "VVPLX", "S000027283", "C000082313", cik="0000915802")],
                    [sec("C000082313", "S000027283", "VVPLY", cik="915802"),
                     sec("C000259241", "S000091565", "VVPLX", cik="1936157")])
    ev = evidence(CURRENT, filings=[_filing("C000259241", "VVPLX", "NPORT-P", "2026-09-28"),
                                    _filing("C000082313", "VVPLX", "N-CEN", "2026-07-13")])
    plan, out = run(snap, ev=ev)
    assert ("instrument_identity", "a") not in out
    assert plan.review["series_moved_registry_class_current"][0]["registry_class_ticker"] == "VVPLY"


@pytest.mark.parametrize("sources", [["legacy"], True, "x", 0, False])
def test_registry_provenance_of_an_unexpected_shape_refuses_the_plan(sources):
    snap = snapshot([iu("a", "HYG")], [reg("a", "HYG", None, None, cik=None)],
                    [sec("C000046846", "S000016772", "HYG", cik="1100663")], funds=set())
    snap.registry["a"]["identity_sources"] = sources
    with pytest.raises(repair.RepairError, match="registry_identity_sources_not_object"):
        run(snap)
