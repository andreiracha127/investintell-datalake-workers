"""Stage C independent verifier — intensive supervision over LIVE production.

Usage (from the repo root)::

    python -m harness.direct_activation.stage_c_verifier --as-of YYYY-MM-DD
    python -m harness.direct_activation.stage_c_verifier report

Read-only re-computation of each published open_macro_v03 decision + allocation from
the SAME pinned inputs the worker read (committed pack v2 prefix + live delta via the
worker's own helpers, imported never duplicated), asserting equality of the logical
outputs field by field (zero tolerance — exact equality of the serialized values),
the input hashes, the expected ``valid_until`` horizon, and the pinned Stage C abort
criteria: any mismatch, any NaN/Inf, any staleness BYPASS (rows published despite a
breach), any FALSE staleness-block (a ledger row on inputs that were actually fresh
— a false block would silently pause the window), and any missing or partial daily
output (the inherited ``missing_output_slo``). An abort writes the day record and
exits non-zero.

Backend route evidence: when ``OPEN_MACRO_BACKEND_URL`` is set and the day carries a
published pair, the verifier fetches ``{url}/macro/open-macro-v03/allocation``
(read-only) and asserts the consumer-visible payload matches the recomputed output
(divergence = abort ``route_divergence``; a 404 while the flag is expected on = abort
``route_inactive_during_window``), pinning the response as ``route_evidence`` in the
day record. When the env is NOT set the day record marks ``route_evidence``
"unavailable" — the plan permits backend-supplied route-level evidence where
cross-repo access is unavailable, but the REAL supervision window requires the route
leg (or pinned backend evidence) for every counted day.

Window accounting (``report``): counted days are business days AFTER the post-merge
``backend_cutover_record.json`` (Stage B artifact root) with outcome ``verified`` —
days verified while the route is still inert are verified-but-not-consumed and do
NOT count (plan B3/Stage C); justified staleness-blocks PAUSE the count (not counted,
not aborts); ``window_complete`` only with >= 10 counted days and zero aborts.

The verifier never writes to the database (guard-tested: no DML/DDL keyword in this
module). Its only filesystem output is the day record / window report artifacts.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import math
import os
from pathlib import Path
from typing import Any

from src.db import connect, resolve_dsn
from src.workers.open_macro_v03 import (
    BOOK,
    CANDIDATE_ID,
    PACK_SHA256_PIN,
    _canonical_sha256,
    build_allocation,
    code_commit,
    compose_inputs,
    pin_search_path,
    resolve_as_of,
    valid_until,
)
from harness.direct_activation.live_validation import (
    CHAIN_START,
    consumable_today,
    staleness_report,
)
from harness.phase0q.decision import run_decision_series
from harness.phase0q.sleeve import SLEEVE_TICKERS

ROOT = Path(__file__).resolve().parents[2]
STAGE_C = ROOT / "artifacts" / "a5" / "open_macro_v03_direct_activation_stage_c_001"
WINDOW_DIR = STAGE_C / "supervision_window"
CUTOVER_PATH = (ROOT / "artifacts" / "a5"
                / "open_macro_v03_direct_activation_stage_b_001"
                / "backend_cutover_record.json")

ROUTE_PATH = "/macro/open-macro-v03/allocation"
BACKEND_URL_ENV = "OPEN_MACRO_BACKEND_URL"
WINDOW_TARGET_DAYS = 10

_DECISION_COLS = (
    "quadrant", "decision_validity", "carry_seed_as_of", "candidate_confidence",
    "coverage_quality", "growth_score", "inflation_score", "input_vintage_sha256",
    "input_prices_sha256", "publish_state", "valid_status", "valid_until",
)
_ALLOCATION_COLS = (
    "book", "w_spy", "w_tlt", "w_tip", "w_gld", "w_dbc", "w_shy",
    "risk_assets_weight", "defensive_assets_weight", "priced_at",
    "input_prices_sha256", "publish_state", "valid_status", "valid_until",
)
_LEDGER_COLS = ("reason", "input_vintage_sha256", "input_prices_sha256")

_DECISION_SQL = (
    "SELECT quadrant, decision_validity, carry_seed_as_of, candidate_confidence, "
    "coverage_quality, growth_score, inflation_score, input_vintage_sha256, "
    "input_prices_sha256, publish_state, valid_status, valid_until "
    "FROM open_macro_v03_decisions WHERE as_of = %s")
_ALLOCATION_SQL = (
    "SELECT book, w_spy, w_tlt, w_tip, w_gld, w_dbc, w_shy, risk_assets_weight, "
    "defensive_assets_weight, priced_at, input_prices_sha256, publish_state, "
    "valid_status, valid_until "
    "FROM open_macro_v03_allocations WHERE as_of = %s")
_LEDGER_SQL = (
    "SELECT reason, input_vintage_sha256, input_prices_sha256 "
    "FROM open_macro_v03_staleness_blocks WHERE as_of = %s")

_WEIGHT_COLS = {"SPY": "w_spy", "TLT": "w_tlt", "TIP": "w_tip",
                "GLD": "w_gld", "DBC": "w_dbc", "SHY": "w_shy"}


# --------------------------------------------------------------------------- #
# DB reads (SELECT only)
# --------------------------------------------------------------------------- #
def _row_dict(cur_row, cols):
    return dict(zip(cols, cur_row, strict=True)) if cur_row is not None else None


def fetch_day(conn, as_of: _dt.date):
    """(decision, allocation, ledger) dicts (or None) for one business day."""
    with conn.cursor() as cur:
        cur.execute(_DECISION_SQL, (as_of,))
        decision = _row_dict(cur.fetchone(), _DECISION_COLS)
        cur.execute(_ALLOCATION_SQL, (as_of,))
        allocation = _row_dict(cur.fetchone(), _ALLOCATION_COLS)
        cur.execute(_LEDGER_SQL, (as_of,))
        ledger = _row_dict(cur.fetchone(), _LEDGER_COLS)
    return decision, allocation, ledger


# --------------------------------------------------------------------------- #
# Normalization / comparison helpers (zero tolerance)
# --------------------------------------------------------------------------- #
def _num(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _date_iso(value: Any) -> str:
    if isinstance(value, _dt.datetime):
        return value.date().isoformat()
    if isinstance(value, _dt.date):
        return value.isoformat()
    return str(value)[:10]


def _utc(value: Any) -> _dt.datetime:
    if isinstance(value, str):
        value = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=_dt.timezone.utc)
    return value.astimezone(_dt.timezone.utc)


def _finite_or_none(name: str, value: Any, problems: list[str]) -> None:
    if value is None:
        return
    v = float(value)
    if not math.isfinite(v):
        problems.append(f"nan_inf: {name}={v!r}")


# --------------------------------------------------------------------------- #
# Independent recomputation
# --------------------------------------------------------------------------- #
def recompute(conn, as_of: _dt.date) -> dict[str, Any]:
    """Recompute the day's logical outputs from pinned inputs — the SAME helpers the
    worker uses (imported), so parity is by construction, divergence is evidence."""
    vintage_rows, price_rows = compose_inputs(conn, as_of)
    report = staleness_report(vintage_rows, price_rows, as_of)
    result: dict[str, Any] = {
        "input_vintage_sha256": _canonical_sha256(vintage_rows),
        "input_prices_sha256": _canonical_sha256(price_rows),
        "pack_v2_sha256": PACK_SHA256_PIN,
        "staleness_breaches": report["breaches"],
    }
    if report["breaches"]:
        return result  # a breach day has no legitimate published pair to recompute
    chain = run_decision_series(vintage_rows, CHAIN_START, as_of)
    last, validity, seed_as_of = consumable_today(chain, as_of)
    allocation = build_allocation(last.quadrant, price_rows, as_of)
    result.update({
        "quadrant": last.quadrant,
        "decision_validity": validity,
        "carry_seed_as_of": seed_as_of.isoformat(),
        "candidate_confidence": last.candidate_confidence,
        "coverage_quality": last.coverage_quality,
        "growth_score": last.growth_score,
        "inflation_score": last.inflation_score,
        "weights": allocation["weights"],
        "risk_assets_weight": allocation["risk_assets_weight"],
        "defensive_assets_weight": allocation["defensive_assets_weight"],
        "priced_at": allocation["priced_at"].isoformat(),
    })
    return result


def _compare_pair(decision: dict, allocation: dict, rec: dict,
                  as_of: _dt.date) -> list[str]:
    """Field-by-field zero-tolerance comparison of the published pair against the
    recomputed outputs. Returns the (possibly empty) list of abort reasons."""
    problems: list[str] = []

    for name, row in (("decision", decision), ("allocation", allocation)):
        if row["publish_state"] != "published" or row["valid_status"] != "valid":
            problems.append(
                f"unpublished_or_invalid_row: {name} is "
                f"{row['publish_state']}/{row['valid_status']}")

    # staleness BYPASS: rows were published although the recomputed inputs breach
    if rec["staleness_breaches"]:
        problems.append(
            "staleness_bypass: pair published despite recomputed breaches "
            f"{[b.get('series_id') or b.get('ticker') for b in rec['staleness_breaches']]}")
        return problems  # no recomputed pair exists to compare against

    # NaN/Inf over every numeric of the published rows and the recomputation
    for key in ("candidate_confidence", "coverage_quality", "growth_score",
                "inflation_score"):
        _finite_or_none(f"decision.{key}", decision[key], problems)
        _finite_or_none(f"recompute.{key}", rec[key], problems)
    for col in (*_WEIGHT_COLS.values(), "risk_assets_weight", "defensive_assets_weight"):
        _finite_or_none(f"allocation.{col}", allocation[col], problems)
    for ticker, weight in rec["weights"].items():
        _finite_or_none(f"recompute.weights.{ticker}", weight, problems)
    if problems:
        return problems

    # decision fields
    if decision["quadrant"] != rec["quadrant"]:
        problems.append(f"field_mismatch: quadrant {decision['quadrant']!r} != "
                        f"recomputed {rec['quadrant']!r}")
    if decision["decision_validity"] != rec["decision_validity"]:
        problems.append(
            f"field_mismatch: decision_validity {decision['decision_validity']!r} != "
            f"recomputed {rec['decision_validity']!r}")
    if _date_iso(decision["carry_seed_as_of"]) != rec["carry_seed_as_of"]:
        problems.append(
            f"field_mismatch: carry_seed_as_of {_date_iso(decision['carry_seed_as_of'])} "
            f"!= recomputed {rec['carry_seed_as_of']}")
    for key in ("candidate_confidence", "coverage_quality", "growth_score",
                "inflation_score"):
        if _num(decision[key]) != _num(rec[key]):
            problems.append(f"field_mismatch: {key} {decision[key]} != "
                            f"recomputed {rec[key]}")

    # allocation fields
    if allocation["book"] != BOOK:
        problems.append(f"field_mismatch: book {allocation['book']!r} != {BOOK!r}")
    for ticker, col in _WEIGHT_COLS.items():
        if _num(allocation[col]) != _num(rec["weights"][ticker]):
            problems.append(f"field_mismatch: {col} {allocation[col]} != "
                            f"recomputed {rec['weights'][ticker]}")
    for key in ("risk_assets_weight", "defensive_assets_weight"):
        if _num(allocation[key]) != _num(rec[key]):
            problems.append(f"field_mismatch: {key} {allocation[key]} != "
                            f"recomputed {rec[key]}")
    if _date_iso(allocation["priced_at"]) != rec["priced_at"]:
        problems.append(f"field_mismatch: priced_at {_date_iso(allocation['priced_at'])} "
                        f"!= recomputed {rec['priced_at']}")

    # input hashes
    if (decision["input_vintage_sha256"] or "").strip() != rec["input_vintage_sha256"]:
        problems.append("input_hash_mismatch: decision.input_vintage_sha256")
    if (decision["input_prices_sha256"] or "").strip() != rec["input_prices_sha256"]:
        problems.append("input_hash_mismatch: decision.input_prices_sha256")
    if (allocation["input_prices_sha256"] or "").strip() != rec["input_prices_sha256"]:
        problems.append("input_hash_mismatch: allocation.input_prices_sha256")

    # expected freshness horizon
    expected_vu = valid_until(as_of)
    for name, row in (("decision", decision), ("allocation", allocation)):
        if _utc(row["valid_until"]) != expected_vu:
            problems.append(
                f"valid_until_mismatch: {name} {row['valid_until']} != "
                f"expected {expected_vu.isoformat()}")
    return problems


def _check_block(ledger: dict, rec: dict) -> list[str]:
    """A recorded staleness-block is honoured only if JUSTIFIED (plan Stage C):
    the verifier independently recomputes the staleness determination and aborts
    when the recomputed inputs are actually fresh.

    The ledger row's input hashes are a FROZEN first-write snapshot (the ledger is
    immutable — ON CONFLICT DO NOTHING), so they deliberately do NOT have to equal
    the verifier's later same-day recomputation: a normal late macro-vintage arrival
    that leaves the breach standing must not turn a still-justified block into a
    false abort (the round-8 monitor semantics ratified on main). Both hash sets are
    still pinned side by side in the day record as evidence."""
    problems: list[str] = []
    if not rec["staleness_breaches"]:
        problems.append(
            "false_block: ledger row present but the recomputed inputs are fresh "
            "(a false block would silently pause the window)")
    return problems


# --------------------------------------------------------------------------- #
# Backend route evidence
# --------------------------------------------------------------------------- #
def _fetch_route(backend_url: str) -> tuple[int, Any]:
    """GET the sanctioned backend route; returns (status_code, json payload|None).
    Isolated for test monkeypatching."""
    import httpx
    resp = httpx.get(backend_url.rstrip("/") + ROUTE_PATH, timeout=30.0)
    try:
        payload = resp.json()
    except Exception:
        payload = None
    return resp.status_code, payload


def check_route(backend_url: str, as_of: _dt.date, rec: dict) -> tuple[Any, list[str]]:
    """(route_evidence, abort_reasons) for the consumer-visible payload leg."""
    status, payload = _fetch_route(backend_url)
    if status == 404:
        return ({"status_code": 404, "payload": payload},
                ["route_inactive_during_window: sanctioned route returned 404 while "
                 "the backend flag is expected on"])
    evidence = {"status_code": status, "payload": payload}
    problems: list[str] = []
    if status != 200 or not isinstance(payload, dict):
        problems.append(f"route_divergence: unexpected response status={status}")
        return evidence, problems
    if payload.get("as_of") != as_of.isoformat():
        problems.append(f"route_divergence: as_of {payload.get('as_of')!r} != "
                        f"{as_of.isoformat()}")
    for key in ("quadrant", "decision_validity"):
        if payload.get(key) != rec[key]:
            problems.append(f"route_divergence: {key} {payload.get(key)!r} != "
                            f"recomputed {rec[key]!r}")
    if payload.get("book") != BOOK:
        problems.append(f"route_divergence: book {payload.get('book')!r} != {BOOK!r}")
    route_weights = payload.get("weights")
    if not isinstance(route_weights, dict):
        problems.append("route_divergence: weights missing from payload")
    else:
        for ticker in SLEEVE_TICKERS:
            if _num(route_weights.get(ticker)) != _num(rec["weights"][ticker]):
                problems.append(
                    f"route_divergence: weights.{ticker} {route_weights.get(ticker)!r} "
                    f"!= recomputed {rec['weights'][ticker]!r}")
    return evidence, problems


# --------------------------------------------------------------------------- #
# Day verification
# --------------------------------------------------------------------------- #
def verify_day(conn, as_of: _dt.date, *, backend_url: str | None = None) -> dict[str, Any]:
    """One supervised day: classify, recompute, compare. Returns the day record."""
    decision, allocation, ledger = fetch_day(conn, as_of)
    abort_reasons: list[str] = []
    outcome = "abort"
    rec: dict[str, Any] = {}
    route_evidence: Any = "unavailable" if not backend_url else None

    if ledger is not None and (decision is not None or allocation is not None):
        abort_reasons.append(
            "block_and_output_coexist: ledger row and output row(s) both present")
    elif ledger is not None:
        rec = recompute(conn, as_of)
        abort_reasons.extend(_check_block(ledger, rec))
        if not abort_reasons:
            outcome = "staleness_block_justified"
    elif decision is None and allocation is None:
        abort_reasons.append(
            "missing_output: no output rows and no staleness-block ledger row "
            "(silent worker exit — inherited missing_output_slo)")
    elif decision is None or allocation is None:
        present = "decision" if decision is not None else "allocation"
        abort_reasons.append(f"partial_pair: only the {present} row exists")
    else:
        rec = recompute(conn, as_of)
        abort_reasons.extend(_compare_pair(decision, allocation, rec, as_of))
        if backend_url and "staleness_bypass" not in " ".join(abort_reasons):
            route_evidence, route_problems = check_route(backend_url, as_of, rec)
            abort_reasons.extend(route_problems)
        if not abort_reasons:
            outcome = "verified"

    # the ledger's frozen first-write hashes are pinned SIDE BY SIDE with the live
    # recompute (evidence, not an abort criterion — see _check_block).
    ledger_hashes = None
    if ledger is not None:
        ledger_hashes = {
            "input_vintage_sha256": (ledger.get("input_vintage_sha256") or "").strip(),
            "input_prices_sha256": (ledger.get("input_prices_sha256") or "").strip(),
        }

    return {
        "artifact_type": "stage_c_supervision_day_record",
        "schema_version": 1,
        "stage": "C",
        "stage_c_id": "open_macro_v03_direct_activation_stage_c_001",
        "as_of": as_of.isoformat(),
        "outcome": outcome,
        "abort_reasons": abort_reasons,
        "recompute": {
            "input_vintage_sha256": rec.get("input_vintage_sha256"),
            "input_prices_sha256": rec.get("input_prices_sha256"),
            "pack_v2_sha256": rec.get("pack_v2_sha256", PACK_SHA256_PIN),
            "quadrant": rec.get("quadrant"),
            "decision_validity": rec.get("decision_validity"),
            "weights": rec.get("weights"),
            "candidate_id": CANDIDATE_ID,
        },
        "ledger_input_hashes": ledger_hashes,
        "route_evidence": route_evidence,
        "verifier_commit": code_commit(),
    }


def write_day_record(record: dict[str, Any], window_dir: Path | None = None) -> Path:
    out_dir = window_dir if window_dir is not None else WINDOW_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"day_{record['as_of']}.json"
    out.write_text(
        json.dumps(record, sort_keys=True, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8", newline="\n")
    return out


# --------------------------------------------------------------------------- #
# Window report
# --------------------------------------------------------------------------- #
def build_window_report(window_dir: Path | None = None,
                        cutover_path: Path | None = None) -> dict[str, Any]:
    """Aggregate the day records into the window report. Counted days are business
    days on/after the post-merge cutover with outcome ``verified``; without a cutover
    record the count is 0 and every verified day is verified-but-not-consumed."""
    window_dir = window_dir if window_dir is not None else WINDOW_DIR
    cutover_path = cutover_path if cutover_path is not None else CUTOVER_PATH

    cutover_date: _dt.date | None = None
    if cutover_path.is_file():
        cutover = json.loads(cutover_path.read_text(encoding="utf-8"))
        cutover_date = _dt.date.fromisoformat(cutover["cutover_date"])

    days: list[dict[str, Any]] = []
    counted = 0
    aborts: list[dict[str, Any]] = []
    blocks_paused = 0
    for path in sorted(window_dir.glob("day_*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        as_of = _dt.date.fromisoformat(record["as_of"])
        outcome = record["outcome"]
        is_business = as_of.weekday() < 5
        consumed = cutover_date is not None and as_of >= cutover_date
        counts = outcome == "verified" and is_business and consumed
        if counts:
            counted += 1
        if outcome == "abort":
            aborts.append({"as_of": record["as_of"],
                           "abort_reasons": record["abort_reasons"]})
        if outcome == "staleness_block_justified":
            blocks_paused += 1
        days.append({
            "as_of": record["as_of"],
            "outcome": outcome,
            "counted": counts,
            "consumed": consumed,
            "note": (None if consumed or outcome != "verified"
                     else "verified_but_not_consumed: route still inert (pre-cutover)"),
        })

    return {
        "artifact_type": "stage_c_window_report",
        "schema_version": 1,
        "stage": "C",
        "stage_c_id": "open_macro_v03_direct_activation_stage_c_001",
        "target_days": WINDOW_TARGET_DAYS,
        "cutover": {"present": cutover_date is not None,
                    "cutover_date": cutover_date.isoformat() if cutover_date else None},
        "days": days,
        "counted_days": counted,
        "staleness_blocks_paused": blocks_paused,
        "aborts": aborts,
        "window_complete": counted >= WINDOW_TARGET_DAYS and not aborts,
    }


def write_window_report(report: dict[str, Any], stage_c_dir: Path | None = None) -> Path:
    out_dir = stage_c_dir if stage_c_dir is not None else STAGE_C
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "window_report.json"
    out.write_text(
        json.dumps(report, sort_keys=True, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8", newline="\n")
    return out


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m harness.direct_activation.stage_c_verifier")
    parser.add_argument("command", nargs="?", choices=("verify", "report"),
                        default="verify")
    parser.add_argument("--as-of", dest="as_of", default=None,
                        help="business day to verify (YYYY-MM-DD; default: current "
                             "America/New_York business day)")
    args = parser.parse_args(argv)

    if args.command == "report":
        report = build_window_report()
        out = write_window_report(report)
        print(json.dumps({"counted_days": report["counted_days"],
                          "aborts": len(report["aborts"]),
                          "window_complete": report["window_complete"],
                          "report": str(out)}, sort_keys=True))
        return 0

    as_of = resolve_as_of(args.as_of)
    if as_of is None:
        print(json.dumps({"status": "non_business_day"}))
        return 0
    backend_url = os.environ.get(BACKEND_URL_ENV) or None

    conn = connect(resolve_dsn())
    try:
        # session-local schema pin (the worker's own helper): the verifier must read
        # the SAME public tables the worker wrote, immune to a role search_path.
        pin_search_path(conn)
        record = verify_day(conn, as_of, backend_url=backend_url)
    finally:
        conn.close()
    out = write_day_record(record)
    print(json.dumps({"as_of": record["as_of"], "outcome": record["outcome"],
                      "abort_reasons": record["abort_reasons"],
                      "day_record": str(out)}, sort_keys=True))
    return 0 if record["outcome"] != "abort" else 1


if __name__ == "__main__":
    raise SystemExit(main())
