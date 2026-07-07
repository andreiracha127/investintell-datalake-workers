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
published pair whose DB comparison is clean (a DB-divergent day aborts regardless,
and its abort evidence must never be lost to a route-leg transport re-raise), the
verifier fetches ``{url}/macro/open-macro-v03/allocation``
(read-only) and asserts the consumer-visible payload matches the recomputed output
(divergence = abort ``route_divergence``; a 404 while the flag is expected on = abort
``route_inactive_during_window``), pinning the response as ``route_evidence`` in the
day record. When the env is NOT set the day record marks ``route_evidence``
"unavailable" — the plan permits backend-supplied route-level evidence where
cross-repo access is unavailable, but the REAL supervision window requires the route
leg (or pinned backend evidence) for every counted day.

Window accounting (``report``): counted days are business days AFTER the post-merge
``backend_cutover_record.json`` (Stage B artifact root) with outcome ``verified`` AND
carrying route evidence (a verified DB-only day whose ``route_evidence`` is
"unavailable"/absent does NOT count — the REAL window requires the consumer leg for
every counted day); days verified while the route is still inert are
verified-but-not-consumed and do NOT count (plan B3/Stage C); justified
staleness-blocks PAUSE the count (not counted, not aborts). A skipped run on a
post-cutover business day is reported as a ``supervision_gap``. ``window_complete``
only with >= 10 counted days, zero aborts, and zero gaps.

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
    PINS_PATH,
    OpenMacroV03Error,
    _canonical_sha256,
    _load_json,
    build_allocation,
    code_commit,
    compose_inputs,
    pin_search_path,
    resolve_as_of,
    valid_until,
    verify_module_pins,
    verify_pack_bytes,
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
# the exact ``artifact_type`` write_day_record stamps — a day record only counts toward
# the window if it carries this identity (a truncated/hand-authored file is excluded and
# surfaced as a malformed_day_record, never silently counted).
DAY_RECORD_ARTIFACT_TYPE = "stage_c_supervision_day_record"
# a non-JSON route body is preserved verbatim (truncated to this many chars) as evidence
# instead of being silently discarded as None.
ROUTE_BODY_EVIDENCE_MAX = 2000

_DECISION_COLS = (
    "quadrant", "decision_validity", "carry_seed_as_of", "candidate_confidence",
    "coverage_quality", "growth_score", "inflation_score", "input_vintage_sha256",
    "input_prices_sha256", "publish_state", "valid_status", "valid_until",
    "pack_v2_sha256", "module_pins_sha256",
)
_ALLOCATION_COLS = (
    "book", "w_spy", "w_tlt", "w_tip", "w_gld", "w_dbc", "w_shy",
    "risk_assets_weight", "defensive_assets_weight", "priced_at",
    "input_prices_sha256", "publish_state", "valid_status", "valid_until",
    "pack_v2_sha256", "module_pins_sha256",
)
_LEDGER_COLS = ("reason", "input_vintage_sha256", "input_prices_sha256")

_DECISION_SQL = (
    "SELECT quadrant, decision_validity, carry_seed_as_of, candidate_confidence, "
    "coverage_quality, growth_score, inflation_score, input_vintage_sha256, "
    "input_prices_sha256, publish_state, valid_status, valid_until, "
    "pack_v2_sha256, module_pins_sha256 "
    "FROM open_macro_v03_decisions WHERE as_of = %s")
_ALLOCATION_SQL = (
    "SELECT book, w_spy, w_tlt, w_tip, w_gld, w_dbc, w_shy, risk_assets_weight, "
    "defensive_assets_weight, priced_at, input_prices_sha256, publish_state, "
    "valid_status, valid_until, pack_v2_sha256, module_pins_sha256 "
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
def _expected_module_pins_sha256() -> str:
    """The canonical ``module_pins_sha256`` the worker STAMPS on every published row —
    read from the SAME source of truth the worker uses (the committed ``module_pins.json``
    under the Stage B artifact root), AFTER ``verify_module_pins`` confirms the pin bundle
    is intact: the complete worker-owned module closure, each module's CRLF→LF sha256
    against the tree, the certified pack block, and a self-consistent aggregate. A
    published row whose ``module_pins_sha256`` diverges from this value carries STALE or
    forged provenance (stamped by a different pin bundle) and aborts."""
    pins = _load_json(PINS_PATH)
    verify_module_pins(pins)
    return pins["module_pins_sha256"]


def recompute(conn, as_of: _dt.date) -> dict[str, Any]:
    """Recompute the day's logical outputs from pinned inputs — the SAME helpers the
    worker uses (imported), so parity is by construction, divergence is evidence."""
    vintage_rows, price_rows = compose_inputs(conn, as_of)
    report = staleness_report(vintage_rows, price_rows, as_of)
    result: dict[str, Any] = {
        "input_vintage_sha256": _canonical_sha256(vintage_rows),
        "input_prices_sha256": _canonical_sha256(price_rows),
        "pack_v2_sha256": PACK_SHA256_PIN,
        "module_pins_sha256": _expected_module_pins_sha256(),
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

    # published provenance pins — the two hashes the worker STAMPS on every official row
    # (Gate 3/3b): the signed pack pin and the canonical module-pins hash. A row whose
    # LOGICAL outputs are correct but that carries a STALE/forged pack_v2_sha256 or
    # module_pins_sha256 (e.g. stamped by an earlier pin bundle) would otherwise verify
    # while corrupting the provenance the monitor / Stage C evidence relies on. Compare the
    # PUBLISHED pins against the signed pack pin and the freshly-recomputed canonical
    # module-pins hash (same source of truth the worker reads).
    for name, row in (("decision", decision), ("allocation", allocation)):
        if (row["pack_v2_sha256"] or "").strip() != PACK_SHA256_PIN:
            problems.append(f"provenance_pin_mismatch: {name}.pack_v2_sha256")
        if (row["module_pins_sha256"] or "").strip() != rec["module_pins_sha256"]:
            problems.append(f"provenance_pin_mismatch: {name}.module_pins_sha256")

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
class _AmbiguousRoutePayload(Exception):
    """The route wire payload contains duplicate JSON object keys: different consumers
    may legally read different values from the SAME bytes (first-wins vs last-wins —
    RFC 8259 leaves duplicate-name behaviour unspecified), so the consumer-visible
    state is ambiguous. Python's ``json`` silently keeps the LAST duplicate — a payload
    whose final duplicate happens to match the recomputation would otherwise verify.
    Raised by ``_parse_route_payload``; ``check_route`` converts it into a
    ``route_divergence`` with the raw wire text preserved as evidence (it is a payload
    defect, never re-raised as a transport outage)."""

    def __init__(self, detail: str, status_code: int | None = None,
                 raw_text: str | None = None):
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code
        self.raw_text = raw_text


def _pairs_rejecting_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise _AmbiguousRoutePayload(f"duplicate JSON key {key!r}")
        out[key] = value
    return out


class _NonJSONRouteBody:
    """A route response body that is not valid JSON (an HTML error page, a truncated or
    malformed JSON document). Rather than silently discarding the bytes as ``None``, the
    raw body is preserved (truncated to ``ROUTE_BODY_EVIDENCE_MAX`` chars) together with
    the parser's error message; ``check_route`` pins both in ``route_evidence`` and aborts
    with ``route_non_json_body`` so an auditor can see WHAT the backend actually served."""

    __slots__ = ("raw_body", "parse_error")

    def __init__(self, raw_body: str, parse_error: str):
        self.raw_body = raw_body
        self.parse_error = parse_error


def _parse_route_payload(status_code: int, text: str) -> Any:
    """Parse the route body as JSON with duplicate-key rejection at EVERY object level.
    A non-JSON body returns a ``_NonJSONRouteBody`` carrying the (truncated) raw bytes and
    the parse error — NOT ``None`` — so the offending body is preserved as evidence rather
    than discarded; duplicate keys raise ``_AmbiguousRoutePayload`` with the wire text
    attached."""
    try:
        return json.loads(text, object_pairs_hook=_pairs_rejecting_duplicates)
    except _AmbiguousRoutePayload as exc:
        exc.status_code = status_code
        exc.raw_text = text
        raise
    except Exception as exc:
        body = text if isinstance(text, str) else str(text)
        if len(body) > ROUTE_BODY_EVIDENCE_MAX:
            body = body[:ROUTE_BODY_EVIDENCE_MAX] + "…(truncated)"
        return _NonJSONRouteBody(body, f"{type(exc).__name__}: {exc}")


def _route_url(backend_url: str) -> str:
    """The resolved URL the route leg actually exercises — pinned into every
    ``route_evidence`` dict so an auditor can see WHICH host served the evidence
    (the sanctioned production service vs a mispointed staging/local clone)."""
    return backend_url.rstrip("/") + ROUTE_PATH


def _fetch_route(backend_url: str) -> tuple[int, Any]:
    """GET the sanctioned backend route; returns (status_code, json payload|None).
    Isolated for test monkeypatching.

    A transport-layer failure (DNS/TLS/connect/read timeout — ``httpx.RequestError``,
    raised BEFORE any HTTP response is received) is left to propagate: it is a
    verifier-side outage, NOT evidence about production's correctness, and
    ``verify_day`` re-raises it fail-loud (see ``_is_route_transport_error``). A
    payload with duplicate JSON keys raises ``_AmbiguousRoutePayload`` instead — an
    ambiguous consumer-visible wire contract, which ``check_route`` records as a
    ``route_divergence`` with the raw text pinned as evidence."""
    import httpx
    resp = httpx.get(_route_url(backend_url), timeout=30.0)
    return resp.status_code, _parse_route_payload(resp.status_code, resp.text)


def _is_route_transport_error(exc: BaseException) -> bool:
    """True iff *exc* is an httpx transport-layer failure — no HTTP response was ever
    received (DNS resolution, TLS handshake, connect/read timeout, connection reset).

    These are verifier-side/network outages, categorically different from a genuine
    ``route_divergence`` (which is returned by ``check_route`` with the offending
    response pinned as ``route_evidence`` — a divergence NEVER raises). Recording a
    transport outage as an ``outcome='abort'`` day record would (a) conflate an
    infrastructure outage with a real payload divergence and (b), as a consumed
    business-day abort, block the supervision window with NO route response captured.
    So the caller re-raises these: fail-loud, no artifact, re-run required (a skipped
    day surfaces as a ``supervision_gap`` until a clean re-run)."""
    try:
        import httpx
    except ImportError:  # httpx only imports when the route leg actually ran
        return False
    return isinstance(exc, httpx.RequestError)


def _json_safe(obj: Any) -> Any:
    """Recursively replace non-finite floats (``NaN``/``Infinity`` — valid Python floats
    but NOT valid strict JSON) with their string form, so a captured route payload is
    always serializable as strict JSON in the day record. A non-finite value is
    separately flagged as ``route_divergence`` by ``_route_number``; here we only make
    the preserved EVIDENCE strict-JSON-valid instead of emitting an invalid
    ``day_*.json`` (Python's ``json`` accepts ``NaN``/``Infinity`` on write by default —
    the writers below additionally pin ``allow_nan=False`` as a hard fail-loud guard)."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else repr(obj)
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    return obj


def _route_number(name: str, value: Any, problems: list[str]) -> float | None:
    """Route numerics are the consumer WIRE contract: a JSON number, never a string
    or bool. ``_num`` would silently coerce ``"0.5"`` → 0.5 and hide a consumer-visible
    serialization regression, so the raw JSON type is asserted BEFORE coercion. A
    non-finite constant (``NaN``/``Infinity`` — Python's JSON parser accepts them as
    floats) is also a divergence AND must never survive into the serialized day record,
    so it is rejected here before the payload is preserved."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        problems.append(f"route_divergence: {name} not a JSON number ({value!r})")
        return None
    num = float(value)
    if not math.isfinite(num):
        problems.append(f"route_divergence: {name} non-finite ({value!r})")
        return None
    return num


def _route_timestamp_is_naive(value: Any) -> bool:
    """True iff a ROUTE ``valid_until`` wire value is a timestamp string that carries NO
    timezone (naive). On the consumer route leg a naive ``valid_until`` must NOT be
    silently assumed UTC (unlike the DB column, which is ``timestamptz`` and tz-aware by
    construction): a horizon served without its own offset is an ambiguous consumer-visible
    contract and aborts as ``route_naive_valid_until``. Non-string / unparseable values are
    not classified here — they fall through to the ordinary divergence path."""
    if not isinstance(value, str):
        return False
    try:
        parsed = _dt.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is None


def check_route(backend_url: str, as_of: _dt.date, rec: dict) -> tuple[Any, list[str]]:
    """(route_evidence, abort_reasons) for the consumer-visible payload leg.

    REAL payload shape (investintell-light /macro/open-macro-v03/allocation):
    ``{as_of, quadrant, decision_validity, carry_seed_as_of, candidate_confidence,
    book, priced_at, risk_assets_weight, defensive_assets_weight, valid_until,
    positions: [{ticker, weight, asset_class, strategy_label}]}``.

    * weights come from ``positions`` (ticker → weight); the API OMITS zero-weight
      positions, so an absent sleeve ticker compares as 0.0 — never as missing.
    * numeric fields (candidate_confidence, risk/defensive, per-position weight)
      are compared with EXACT float64 equality: the payload is served from the DB
      and, with the NUMERIC write-fidelity fix in production, the stored values
      round-trip the exact floats the worker computed (a divergence is evidence,
      not noise).

    The route is CURRENT-ONLY: it always serves the MOST RECENT published pair and is
    NOT parametrizable by date, so the CURRENT day is the one that proves the consumed
    leg. When ``--as-of`` is re-run for an EARLIER day after production has advanced, the
    live route serves a later ``as_of`` it cannot rewind — that is recorded as
    non-servable backfill evidence (``route_backfill_not_servable``), NOT a divergence,
    and the day does not count as route-verified (the DB recompute leg still
    verifies/aborts normally). A served ``as_of`` BEFORE the requested day (a route that
    cannot serve the requested pair at all) is a real ``route_stale_before_asof`` abort.
    """
    route_url = _route_url(backend_url)
    try:
        status, payload = _fetch_route(backend_url)
    except _AmbiguousRoutePayload as exc:
        # duplicate JSON keys: the wire bytes are ambiguous (consumers may read
        # divergent values), so this can NEVER verify — even when the surviving
        # last-wins value would match. Preserve the raw wire text as evidence.
        evidence = {"url": route_url, "status_code": exc.status_code,
                    "payload": None, "raw_wire_text": exc.raw_text}
        return evidence, [f"route_divergence: ambiguous wire payload — {exc.detail}"]
    if isinstance(payload, _NonJSONRouteBody):
        # a non-JSON body (HTML error page / malformed JSON) is preserved verbatim
        # (truncated) as evidence instead of being discarded as None. A 404 still reads as
        # route inactivity; any other status carrying a non-JSON body is a
        # route_non_json_body divergence with the offending bytes pinned.
        evidence = {"url": route_url, "status_code": status, "payload": None,
                    "raw_body": payload.raw_body, "parse_error": payload.parse_error}
        if status == 404:
            return evidence, [
                "route_inactive_during_window: sanctioned route returned 404 while the "
                "backend flag is expected on"]
        return evidence, [
            f"route_divergence: route_non_json_body (status={status}) — "
            f"{payload.parse_error}"]
    if status == 404:
        return ({"url": route_url, "status_code": 404, "payload": _json_safe(payload)},
                ["route_inactive_during_window: sanctioned route returned 404 while "
                 "the backend flag is expected on"])
    # compare against the RAW payload (so a non-finite is caught by _route_number) but
    # persist a strict-JSON-safe copy as evidence (never an invalid day_*.json).
    evidence = {"url": route_url, "status_code": status, "payload": _json_safe(payload)}
    problems: list[str] = []
    if status != 200 or not isinstance(payload, dict):
        problems.append(f"route_divergence: unexpected response status={status}")
        return evidence, problems
    # current-only route: reconcile the served as_of against the requested one before the
    # field-by-field compare (see the docstring). served == requested → full compare;
    # served > requested → non-servable backfill (evidence only, no abort, not counted);
    # served < requested → the route cannot serve the requested pair (abort).
    served_as_of_raw = payload.get("as_of")
    served_date: _dt.date | None = None
    if isinstance(served_as_of_raw, str):
        try:
            served_date = _dt.date.fromisoformat(served_as_of_raw)
        except ValueError:
            served_date = None
    if served_date is None:
        # a non-string / unparseable as_of is an outright wire divergence — flag it and
        # fall through to the field-by-field compare (unchanged behaviour).
        problems.append(f"route_divergence: as_of {served_as_of_raw!r} != "
                        f"{as_of.isoformat()}")
    elif served_date > as_of:
        # production advanced past a backfilled (earlier) day: the current-only route
        # cannot rewind to the historical pair. Record it as evidence, do NOT abort, and
        # mark the day non-route-servable so build_window_report does NOT count it.
        evidence["served_as_of"] = served_as_of_raw
        evidence["route_backfill_not_servable"] = True
        evidence["note"] = (
            f"route_advanced_backfill: live route serves {served_as_of_raw}, cannot "
            f"serve historical {as_of.isoformat()}")
        return evidence, []
    elif served_date < as_of:
        # the current-only route cannot serve a FUTURE pair: a served as_of before the
        # requested day means the route cannot serve the requested pair at all — abort.
        problems.append(
            f"route_stale_before_asof: route serves {served_as_of_raw} < requested "
            f"{as_of.isoformat()} (a current-only route cannot serve the requested day)")
        return evidence, problems
    # served_date == as_of: fall through to the full consumer-visible compare below.
    for key in ("quadrant", "decision_validity"):
        if payload.get(key) != rec[key]:
            problems.append(f"route_divergence: {key} {payload.get(key)!r} != "
                            f"recomputed {rec[key]!r}")
    if payload.get("book") != BOOK:
        problems.append(f"route_divergence: book {payload.get('book')!r} != {BOOK!r}")
    # freshness/provenance fields the consumer sees and the verifier recomputes: a
    # correct weight vector served with a stale carry seed, priced_at, or valid_until
    # is still a consumer-visible divergence (a wrong valid_until makes the route look
    # fresh/stale incorrectly).
    # The route serves these as date-only ISO strings ("YYYY-MM-DD") and ``rec`` holds
    # the same date-only form. Compare the EXACT wire string (NOT _date_iso, which slices
    # to the first ten chars): a timestamp/padded serialization like
    # "2026-07-02T00:00:00Z" whose first ten chars match must count as a consumer-visible
    # payload divergence, never silently pass as equal.
    if payload.get("carry_seed_as_of") != rec["carry_seed_as_of"]:
        problems.append(
            f"route_divergence: carry_seed_as_of {payload.get('carry_seed_as_of')!r} "
            f"!= recomputed {rec['carry_seed_as_of']}")
    if payload.get("priced_at") != rec["priced_at"]:
        problems.append(f"route_divergence: priced_at {payload.get('priced_at')!r} "
                        f"!= recomputed {rec['priced_at']}")
    expected_vu = valid_until(as_of)
    raw_vu = payload.get("valid_until")
    if _route_timestamp_is_naive(raw_vu):
        # a naive route valid_until must NOT be assumed UTC (the DB timestamptz column is
        # tz-aware by construction; the wire value must carry its own offset).
        problems.append(
            f"route_naive_valid_until: valid_until {raw_vu!r} carries no timezone offset "
            "(naive) — refusing to assume UTC")
    else:
        try:
            route_vu: _dt.datetime | None = _utc(raw_vu)
        except (TypeError, ValueError, AttributeError):
            route_vu = None
        if route_vu != expected_vu:
            problems.append(
                f"route_divergence: valid_until {raw_vu!r} != "
                f"expected {expected_vu.isoformat()}")
    for key in ("candidate_confidence", "risk_assets_weight", "defensive_assets_weight"):
        served_num = _route_number(key, payload.get(key), problems)
        if served_num is not None and served_num != _num(rec[key]):
            problems.append(f"route_divergence: {key} {payload.get(key)!r} != "
                            f"recomputed {rec[key]!r}")
    positions = payload.get("positions")
    if not isinstance(positions, list):
        problems.append("route_divergence: positions missing from payload")
    else:
        route_weights: dict[str, Any] = {}
        for position in positions:
            if not isinstance(position, dict) or "ticker" not in position:
                problems.append(f"route_divergence: malformed position {position!r}")
                continue
            ticker = position["ticker"]
            if not isinstance(ticker, str):
                # a non-string ticker (JSON array/object) is unhashable: using it as a
                # dict key below would raise TypeError and abort verify_day() WITHOUT
                # writing the day record, losing the serialization failure — flag it as
                # divergence and preserve it as evidence instead.
                problems.append(
                    f"route_divergence: non-string position ticker {ticker!r}")
                continue
            if ticker in route_weights:
                # a duplicate row would silently overwrite: consumers receive/sum a
                # different exposure — treat as divergence, never keep the last one.
                problems.append(
                    f"route_divergence: duplicate position ticker {ticker!r}")
                continue
            route_weights[ticker] = position.get("weight")
        unexpected = sorted(set(route_weights) - set(SLEEVE_TICKERS))
        if unexpected:
            problems.append(
                f"route_divergence: positions carry non-sleeve tickers {unexpected}")
        for ticker in SLEEVE_TICKERS:
            # zero-weight positions are omitted by the API: absent == 0.0
            served = route_weights.get(ticker, 0.0)
            served_num = _route_number(f"positions[{ticker}].weight", served, problems)
            if served_num is not None and served_num != _num(rec["weights"][ticker]):
                problems.append(
                    f"route_divergence: positions[{ticker}].weight {served!r} "
                    f"!= recomputed {rec['weights'][ticker]!r}")
    return evidence, problems


# --------------------------------------------------------------------------- #
# Day verification
# --------------------------------------------------------------------------- #
def _published_provenance(decision: dict | None,
                          allocation: dict | None) -> dict[str, Any]:
    """The provenance pins (``pack_v2_sha256`` / ``module_pins_sha256``) the worker
    STAMPED on the published rows, extracted verbatim for the day record. Pinned beside
    the recompute block's EXPECTED values so a ``provenance_pin_mismatch`` abort — or a
    clean pass — is validated from the artifact itself, never re-derived against DB rows
    that have since moved. A missing row (partial/absent pair, staleness block) yields
    ``None`` for that side."""
    def _pick(row: dict | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "pack_v2_sha256": (row.get("pack_v2_sha256") or "").strip() or None,
            "module_pins_sha256": (row.get("module_pins_sha256") or "").strip() or None,
        }
    return {"decision": _pick(decision), "allocation": _pick(allocation)}


def verify_day(conn, as_of: _dt.date, *, backend_url: str | None = None) -> dict[str, Any]:
    """One supervised day: classify, recompute, compare. Returns the day record."""
    decision, allocation, ledger = fetch_day(conn, as_of)
    abort_reasons: list[str] = []
    outcome = "abort"
    rec: dict[str, Any] = {}
    route_evidence: Any = "unavailable" if not backend_url else None

    try:
        # Pack-byte integrity gate (fail-loud, BEFORE any recompute reads the pack).
        # compose_inputs() only checks the pack manifest VALUE plus the live-prefix
        # hashes; a checkout whose canonical pack bytes drifted — while the manifest
        # still names the signed PACK_SHA256_PIN — would otherwise be recomputed against
        # mutated bytes. verify_pack_bytes (the worker's own publish-path gate, imported
        # never duplicated) recomputes the canonical sha256 of the REAL pack files vs the
        # SOURCE.json per-table pins and the signed aggregate; a single mutated byte fails
        # here, loud. It reads only the committed pack and runs once per day.
        try:
            verify_pack_bytes()
        except OpenMacroV03Error as exc:
            abort_reasons.append(f"pack_bytes_mismatch: {exc}")

        if abort_reasons:
            pass  # a drifted pack aborts before recomputing against the mutated bytes
        elif ledger is not None and (decision is not None or allocation is not None):
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
            # The route leg runs ONLY while the day is still clean after the DB
            # comparison. A DB-divergent day aborts regardless of what the route
            # serves, and fetching anyway would let a transport failure on the route
            # leg re-raise (fail-loud, no artifact) and DESTROY the already-detected
            # DB abort evidence. Skipping also subsumes the old staleness_bypass
            # check: a bypass is itself an abort reason.
            if backend_url and not abort_reasons:
                route_evidence, route_problems = check_route(backend_url, as_of, rec)
                abort_reasons.extend(route_problems)
            if not abort_reasons:
                outcome = "verified"
    except Exception as exc:  # a production recompute/route failure must be PRESERVED
        # A verifier-side transport failure reaching the sanctioned route (DNS/TLS/
        # timeout — httpx.RequestError, raised BEFORE any response) is NOT evidence about
        # production's correctness. Recording it as an abort day would conflate a network
        # outage with a genuine route_divergence and — as a consumed business-day abort —
        # block the window with no route response captured. A real divergence returns via
        # check_route() with the response pinned as evidence and NEVER raises, so transport
        # failures re-raise: fail-loud, no artifact, re-run required (surfaced as a
        # supervision_gap until a clean re-run). This does not weaken fail-loud — it makes
        # the transport leg STRICTLY louder than a recorded abort.
        if _is_route_transport_error(exc):
            raise
        # If recompute() raises (e.g. compose_inputs() detects prefix-hash drift or
        # build_allocation() refuses unusable prices) — or any comparison raises — the
        # exception would escape verify_day() and main() would never call
        # write_day_record(), so the abort evidence is lost and the failure is invisible
        # until a later run infers a gap. Convert it into an abort day record (outcome
        # stays "abort", main() still exits non-zero) so it is preserved as supervision
        # evidence. This never masks a pass: outcome is only set to verified/justified at
        # the END of a branch, after the raising call.
        abort_reasons.append(f"recompute_error: {type(exc).__name__}: {exc}")
        outcome = "abort"

    # the expected freshness horizon is a pure function of as_of — pin it so the day
    # record is self-substantiating (an auditor validates the artifact without re-deriving
    # valid_until against a live clock).
    expected_valid_until = valid_until(as_of).isoformat()

    # the ledger's frozen first-write hashes are pinned SIDE BY SIDE with the live
    # recompute (evidence, not an abort criterion — see _check_block).
    ledger_hashes = None
    if ledger is not None:
        ledger_hashes = {
            "input_vintage_sha256": (ledger.get("input_vintage_sha256") or "").strip(),
            "input_prices_sha256": (ledger.get("input_prices_sha256") or "").strip(),
        }

    return {
        "artifact_type": DAY_RECORD_ARTIFACT_TYPE,
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
            # the canonical module-pins hash the published rows are compared against
            # (the EXPECTED side of provenance_pin_mismatch) — pinned beside the published
            # values in published_provenance so the artifact carries both.
            "module_pins_sha256": rec.get("module_pins_sha256"),
            "quadrant": rec.get("quadrant"),
            "decision_validity": rec.get("decision_validity"),
            "carry_seed_as_of": rec.get("carry_seed_as_of"),
            # EVERY scalar the verifier compares field-by-field is serialized here so the
            # bundle is self-substantiating — an auditor re-validates the day record itself
            # instead of re-running against live inputs that have since moved. Numerics ride
            # through _json_safe: a non-finite recomputed value is already flagged nan_inf by
            # _compare_pair, but the raw float would make write_day_record() (allow_nan=False)
            # raise and the abort artifact would never be preserved.
            "candidate_confidence": _json_safe(rec.get("candidate_confidence")),
            "coverage_quality": _json_safe(rec.get("coverage_quality")),
            "growth_score": _json_safe(rec.get("growth_score")),
            "inflation_score": _json_safe(rec.get("inflation_score")),
            "risk_assets_weight": _json_safe(rec.get("risk_assets_weight")),
            "defensive_assets_weight": _json_safe(rec.get("defensive_assets_weight")),
            "priced_at": rec.get("priced_at"),
            "weights": _json_safe(rec.get("weights")),
            # the expected freshness horizon compared against the published/route rows —
            # pinned so the artifact carries its own valid_until reference.
            "expected_valid_until": expected_valid_until,
            # the recomputed breach details are the JUSTIFICATION evidence for a
            # staleness_block_justified day (why the pause was honoured) and for a
            # staleness_bypass abort (what the worker ignored): pin them in the
            # artifact so an auditor validates the day record itself instead of
            # re-running against live inputs that have since moved.
            "staleness_breaches": _json_safe(rec.get("staleness_breaches")),
            "candidate_id": CANDIDATE_ID,
        },
        # the ledger row's own frozen reason string, pinned verbatim beside the
        # recomputed breaches (first-write snapshot vs live recompute — see
        # _check_block for why the two may legitimately differ in hashes).
        "ledger_reason": ledger.get("reason") if ledger is not None else None,
        "ledger_input_hashes": ledger_hashes,
        # the provenance pins the worker STAMPED on the published rows, pinned verbatim
        # (self-substantiating evidence beside the recompute block's EXPECTED pack_v2 /
        # module_pins — a provenance_pin_mismatch abort is validated from the artifact).
        "published_provenance": _published_provenance(decision, allocation),
        "route_evidence": route_evidence,
        "verifier_commit": code_commit(),
    }


def write_day_record(record: dict[str, Any], window_dir: Path | None = None) -> Path:
    out_dir = window_dir if window_dir is not None else WINDOW_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"day_{record['as_of']}.json"
    out.write_text(
        json.dumps(record, sort_keys=True, indent=1, ensure_ascii=False,
                   allow_nan=False) + "\n",
        encoding="utf-8", newline="\n")
    return out


# --------------------------------------------------------------------------- #
# Window report
# --------------------------------------------------------------------------- #
def _malformed_day_record(record: dict[str, Any]) -> bool:
    """True iff *record* lacks the identity fields ``write_day_record`` stamps and so must
    NOT be counted toward the window. A truncated / hand-authored file that matches the
    ``day_*.json`` glob but carries only a subset (e.g. just ``as_of`` + ``outcome`` +
    ``route_evidence``) is an ambiguous supervision artifact: it is excluded from the count
    and surfaced as a ``malformed_day_record``, never silently counted. Identity =
    the exact ``artifact_type``, ``as_of``, ``outcome``, a non-empty ``verifier_commit``,
    and (for a verified day) the ``recompute`` block."""
    if record.get("artifact_type") != DAY_RECORD_ARTIFACT_TYPE:
        return True
    if not isinstance(record.get("as_of"), str) or not record.get("as_of"):
        return True
    if not record.get("outcome"):
        return True
    verifier_commit = record.get("verifier_commit")
    if not isinstance(verifier_commit, str) or not verifier_commit.strip():
        return True
    if record.get("outcome") == "verified" and not isinstance(
            record.get("recompute"), dict):
        return True
    return False


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
    seen_dates: set[str] = set()
    counted_dates: set[str] = set()
    duplicate_dates: list[str] = []
    malformed_day_records: list[str] = []
    for path in sorted(window_dir.glob("day_*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        as_of_str = record.get("as_of")
        if not isinstance(as_of_str, str) or not as_of_str:
            # a record without a placeable as_of cannot be positioned in the window at all;
            # surface it by filename as malformed and skip (it never counts, never a gap).
            malformed_day_records.append(path.name)
            continue
        as_of = _dt.date.fromisoformat(as_of_str)
        # The Stage C exit requirement is ten DISTINCT verified production days. The
        # pipeline writes exactly one deterministic day_{as_of}.json per date, so two
        # files naming the same as_of (e.g. a copied/attempt file committed beside the
        # canonical record — still matching the day_*.json glob) are an ambiguous
        # supervision state: counting both could reach the target with fewer than ten
        # distinct days. Surface the duplicate and refuse completion, and count each
        # date at most once, instead of counting files.
        if as_of_str in seen_dates and as_of_str not in duplicate_dates:
            duplicate_dates.append(as_of_str)
        seen_dates.add(as_of_str)
        outcome = record.get("outcome")
        is_business = as_of.weekday() < 5
        consumed = cutover_date is not None and as_of >= cutover_date
        # a truncated/hand-authored file that matches the glob but lacks the identity
        # fields write_day_record stamps is an ambiguous artifact: it must NOT count and is
        # surfaced as a malformed_day_record (window_complete requires zero malformed).
        malformed = _malformed_day_record(record)
        if malformed and as_of_str not in malformed_day_records:
            malformed_day_records.append(as_of_str)
        # a counted production day must carry the consumer-visible route leg with a RESOLVED
        # url (or pinned backend evidence): a verified day whose route evidence is
        # "unavailable"/absent (DB-only) — or a dict missing a resolved url — must not close
        # the REAL window.
        route_evidence = record.get("route_evidence")
        has_route_dict = isinstance(route_evidence, dict)
        has_route_url = (has_route_dict
                         and isinstance(route_evidence.get("url"), str)
                         and bool(route_evidence.get("url").strip()))
        # a backfilled day whose current-only route could not serve the historical as_of
        # carries route evidence WITH a url, but was NOT proven consumed by this run — it
        # must NOT satisfy the route-verified count (the current day proves the leg).
        route_backfill = (has_route_dict
                          and bool(route_evidence.get("route_backfill_not_servable")))
        counts = (outcome == "verified" and is_business and consumed
                  and has_route_url and not malformed and not route_backfill)
        if counts and as_of_str not in counted_dates:
            counted += 1
            counted_dates.add(as_of_str)
        if outcome == "abort":
            # Keep EVERY abort visible, but tag whether it lands inside the real
            # post-cutover production window. A pre-cutover (non-consumed) or non-business
            # abort — e.g. a dry-run attempt written while the sanctioned route is
            # intentionally inert — is OUTSIDE the supervised window (mirrors
            # verified-but-not-consumed) and must NOT permanently block completion; only
            # a consumed business-day abort invalidates the window (see window_complete).
            aborts.append({"as_of": as_of_str,
                           "abort_reasons": record.get("abort_reasons", []),
                           "consumed": consumed,
                           "is_business": is_business})
        if outcome == "staleness_block_justified":
            blocks_paused += 1
        if malformed:
            note = "malformed_day_record: missing identity fields (not counted)"
        elif consumed and outcome == "verified" and route_backfill:
            note = ("route_backfill_not_servable: current-only route serves a later "
                    "as_of (historical day not counted)")
        elif consumed and outcome == "verified" and not has_route_dict:
            note = "verified_without_route_evidence: not counted (DB-only leg)"
        elif consumed and outcome == "verified" and not has_route_url:
            note = ("verified_without_route_url: route evidence missing a resolved url "
                    "(not counted)")
        elif not consumed and outcome == "verified":
            note = "verified_but_not_consumed: route still inert (pre-cutover)"
        elif not (consumed and is_business) and outcome == "abort":
            note = "pre_cutover_abort: outside the production window (not blocking)"
        else:
            note = None
        days.append({
            "as_of": as_of_str,
            "outcome": outcome,
            "counted": counts,
            "consumed": consumed,
            "note": note,
        })

    # A skipped verifier run on a post-cutover business day is otherwise INVISIBLE
    # (the loop only sees files that exist), so ten later verified files could close
    # the window without covering every consumed production day. Derive the expected
    # business-day sequence cutover→latest record and surface any missing weekday as a
    # gap (calendar policy: Mon–Fri, no holiday calendar — every weekday is supervised).
    gaps: list[str] = []
    if cutover_date is not None and seen_dates:
        latest = max(_dt.date.fromisoformat(d) for d in seen_dates)
        cursor = cutover_date
        while cursor <= latest:
            if cursor.weekday() < 5 and cursor.isoformat() not in seen_dates:
                gaps.append(cursor.isoformat())
            cursor += _dt.timedelta(days=1)

    # Only aborts on a CONSUMED business day (inside the real production window)
    # invalidate completion; pre-cutover/non-business aborts stay visible in ``aborts``
    # but never permanently block ten clean consumed days.
    blocking_aborts = [a for a in aborts if a["consumed"] and a["is_business"]]

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
        "supervision_gaps": gaps,
        "duplicate_dates": duplicate_dates,
        "malformed_day_records": malformed_day_records,
        "window_complete": (counted >= WINDOW_TARGET_DAYS
                            and not blocking_aborts and not gaps
                            and not duplicate_dates
                            and not malformed_day_records),
    }


def write_window_report(report: dict[str, Any], stage_c_dir: Path | None = None) -> Path:
    out_dir = stage_c_dir if stage_c_dir is not None else STAGE_C
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "window_report.json"
    out.write_text(
        json.dumps(report, sort_keys=True, indent=1, ensure_ascii=False,
                   allow_nan=False) + "\n",
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
