"""``open_macro_v03_decision_chain`` producer — the live, append-only publisher.

WHAT THIS PUBLISHES
-------------------
ONE row per calendar month in ``open_macro_v03_decision_chain``: the monthly latched
quadrant decision of the certified ``macro_quadrant_us_v3`` model. Until 2026-08 the
only writer of this table was the MANUAL script ``scripts/rebuild_decision_chain_v3.py``
(owner order, 2026-07-17, DELETE+INSERT of the whole table, ``CHAIN_END`` pinned to
2026-06-30). This worker replaces the *pinned end*, NOT the script: history revision
stays a manual, owner-ordered operation; forward publication becomes a producer.

APPEND-ONLY IS THE CONTRACT, NOT A CONVENTION
---------------------------------------------
The 148 rows already in the table are certified history (the +11,55pp fwd12m
separation was measured ON THEM). This module therefore contains exactly ONE write
statement — :data:`INSERT_SQL`, an ``INSERT ... ON CONFLICT (as_of) DO NOTHING`` — and
no DELETE and no UPDATE anywhere. A month that is already present is a no-op at the
DATABASE level (the primary key decides), not because Python remembered to check.
``tests/test_open_macro_v03_chain.py`` pins both halves: the module's SQL surface
carries no DELETE/UPDATE, and a second run over a published month issues no insert.

At most ONE month is published per run, the oldest missing one, so a daily cron is
idempotent and a catch-up walks forward in order.

FIDELITY: THE SAME MATH, LITERALLY THE SAME FUNCTION
-----------------------------------------------------
The new month is not recomputed by a second implementation. ``run()`` calls
:func:`harness.phase0q.decision_v3.run_decision_series_v3` — the function the rebuild
script calls — over ``[CHAIN_START, target_month]``, and publishes its LAST row. The
series is path-dependent (the latch threads ``previous_snapshot_id`` and the last
published quadrant month to month; the v2 filter reads 48 trailing observations), so
there is no way to compute month N without replaying the prefix. That replay is
~2 min and happens on every publishing run; it is the price of not having a second
engine.

``tests/test_open_macro_v03_chain.py::test_golden_replay_reproduces_the_certified_chain``
replays the pack with ``end=2026-06-30`` and compares all 148 months against a fixture
carved from the live table: the port reproduces the certified series exactly (the
stored NUMERICs carry the float8->NUMERIC 15-digit rendering of the same doubles,
because the 2026-07-17 rebuild predates the house ``_exact_numeric`` chokepoint).

INPUTS: CERTIFIED PREFIX + LIVE TAIL
-------------------------------------
The rebuild computed the series from the CERTIFIED INPUT PACK _003 only, whose window
stops at ``available_at`` 2026-06-25. A producer must see what came after, so the
inputs are assembled as:

  macro vintages = pack rows  UNION  live rows with ``available_at > pack max``
  SPY sessions   = pack rows  UNION  live rows with ``date > pack max``

Both boundaries are DERIVED from the pack, never hardcoded. The consequence is
deliberate: for every decision date at or before the pack's window the PIT read sees
EXACTLY the certified pack, so a late ingestion backfill of an OLD vintage can never
silently rewrite the inputs of certified history. (Audited 2026-08-07: the live
vintage store equals the pack row-for-row per series below the boundary — 8/8 series
— so today the two definitions coincide. The pack is the guarantee that they keep
coinciding.)

ENDPOINT DEPENDENCE — MEASURED, REPORTED, GATED
------------------------------------------------
The fused v3 model's auxiliary market sensor standardizes each month against the
month-end grid the replay walks (``decision_v3`` builds it ONCE over the union of all
decision dates' 48-month walk-backs). Extending the series by one month adds 46 grid
dates BEFORE the old end, which changes that standardizer's sample and therefore
moves earlier months slightly. Measured 2026-08-07, pack-only, end 2026-07-31 vs
2026-06-30: 45 of 592 numeric cells move (max 4.7e-3, on ``candidate_confidence``),
ONE categorical cell moves (2023-11-30 ``candidate_quadrant`` contraction->recovery),
and NOTHING that any consumer reads moves.

This is a property of the certified construction, not of this worker — the same shift
happens whenever the owner reruns the rebuild at a later end date. This worker does
not "fix" it (that would change the math). It does three things instead:

  * it never writes an existing month, so no stored cell can move;
  * :func:`prefix_gate` HARD-FAILS if any stored month's CONSUMABLE projection
    (``quadrant``, ``status``, ``transition_pending``) changes under the extended
    replay — those are what ``open_macro_v04``, the Light backend and the certified
    backtest read, and a change there would mean the new month does not sit on the
    certified chain;
  * it REPORTS ``candidate_quadrant`` and numeric drift in the run stats, so the
    endpoint effect stays visible instead of becoming folklore.

READINESS: NATIVE FREQUENCY, NOT CALENDAR MONTHS
-------------------------------------------------
See :func:`readiness`. A month is published only once every input arm has PROVEN it
published past the decision cutoff. Anything short of that is a no-op with the missing
arms named — never a forward-fill.

FAIL-LOUD ORDER (zero side effects before every gate)
------------------------------------------------------
 1. certified pack integrity (sha256 of both canonical files) — no connection opened;
 2. connect, pin ``search_path`` to public, advisory lock 900_220;
 3. READ-ONLY catalog verification of the chain table (this worker never applies DDL);
 4. read the chain and verify it IS the certified chain (contiguous month-ends from
    ``chain_start``, one ``basis``, one ``pack_sha256`` and it is this pack's);
 5. pick the target month; a month still in progress is a no-op;
 6. per-arm readiness; an unsettled arm is a no-op NAMING the arm;
 7. assemble inputs, replay the series;
 8. prefix gate (above);
 9. ONE insert, then re-read and compare.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from src.db import LOCK_OPEN_MACRO_V03_CHAIN, advisory_lock, connect, resolve_dsn

ROOT = Path(__file__).resolve().parents[2]
PACK = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_003"
MACRO_JSON = PACK / "data" / "canonical" / "macro_observation_vintage.json"
EOD_JSON = PACK / "data" / "canonical" / "eod_prices.json"

CHAIN_TABLE = "open_macro_v03_decision_chain"
VINTAGE_TABLE = "macro_observation_vintage"
EOD_TABLE = "eod_prices"

# The chain's genesis, from the rebuild script. It is also asserted against every
# stored row's ``chain_start``, so a table built from a different genesis is refused.
CHAIN_START = _dt.date(2014, 3, 1)
BASIS = "certified_chain"

# sha256 of the pack's canonical inputs as CHECKED OUT. ``.gitattributes`` pins
# ``fixtures/p1_packs/**/*.json`` to LF, so the raw-byte digest is stable on every
# platform. Pinning the FILE digests (not the pack's own table_hashes.json) means a
# tampered pack cannot vouch for itself.
CANONICAL_SHA256 = {
    "data/canonical/macro_observation_vintage.json":
        "53c367771e79f3671379d0c7bdc780a288113b9e90621c6f15d162d2bf9f87c4",
    "data/canonical/eod_prices.json":
        "2ddd58be82164a156e125a26535cf3361c7ea8efa7adba317497142c1a1f2407",
}

# The 8 seed arms are read from the model registry, never re-listed here: a series
# added to (or dropped from) the basket must move readiness with it.
def arm_series_ids() -> list[str]:
    from src.macro_sources import SEED_SOURCES
    return sorted({spec.series_id for spec in SEED_SOURCES})


def market_ticker() -> str:
    from src.quadrant_market_observation import MARKET_GROWTH_TICKER
    return MARKET_GROWTH_TICKER


class DecisionChainError(RuntimeError):
    """A gate refused. Raised BEFORE any side effect except a read."""


# --------------------------------------------------------------------------- #
# Gate 1 — certified pack integrity (zero side effects)
# --------------------------------------------------------------------------- #
def verify_pack() -> dict[str, Any]:
    """sha256 the pack's canonical inputs and return its identity.

    Runs before a connection is opened: a byte of the certified prefix missing must
    abort the run, not be shrugged off. Returns the ``input_pack_sha256`` the row's
    ``pack_sha256`` column is stamped with — the same value the 148 stored rows carry,
    which is what makes "this row extends THAT chain" checkable in SQL.
    """
    for relative, expected in CANONICAL_SHA256.items():
        path = PACK / relative
        if not path.is_file():
            raise DecisionChainError(f"certified pack input missing: {path}")
        got = hashlib.sha256(path.read_bytes()).hexdigest()
        if got != expected:
            raise DecisionChainError(
                f"certified pack input {relative} sha256 {got} != pinned {expected}")
    manifest = json.loads((PACK / "manifest.json").read_text(encoding="utf-8"))
    pack_sha = manifest["input_pack_sha256"]
    if len(pack_sha) != 64:
        raise DecisionChainError(f"input_pack_sha256 is not a sha256: {pack_sha!r}")
    return {"input_pack_sha256": pack_sha,
            "input_pack_id": manifest["input_pack_id"]}


def load_pack_inputs() -> tuple[list[dict], list[dict], _dt.datetime, _dt.date]:
    """The pack's canonical rows plus the two boundaries the live delta starts after.

    Both boundaries are OBSERVED from the pack's own content:
      * ``macro_boundary`` = max ``available_at`` over the vintage rows;
      * ``eod_boundary``   = max ``date`` over the market ticker's sessions.
    Hardcoding them would rot the day the pack is re-certified.
    """
    macro = json.loads(MACRO_JSON.read_text(encoding="utf-8"))
    eod = json.loads(EOD_JSON.read_text(encoding="utf-8"))
    if not macro or not eod:
        raise DecisionChainError("certified pack canonical inputs are empty")
    macro_boundary = max(_parse_available_at(r["available_at"]) for r in macro)
    ticker = market_ticker()
    sessions = [r["date"] for r in eod if r.get("ticker") == ticker]
    if not sessions:
        raise DecisionChainError(f"certified pack carries no {ticker} sessions")
    eod_boundary = _dt.date.fromisoformat(max(sessions)[:10])
    return macro, eod, macro_boundary, eod_boundary


def _parse_available_at(raw: str) -> _dt.datetime:
    value = _dt.datetime.fromisoformat(raw)
    if value.tzinfo is None:
        value = value.replace(tzinfo=_dt.timezone.utc)
    return value


# --------------------------------------------------------------------------- #
# Gate 2 — session hygiene
# --------------------------------------------------------------------------- #
def pin_search_path(conn) -> None:
    """Force ``search_path`` to public BEFORE any table access.

    Every table is referenced bare; a non-public role default would resolve them
    against a look-alike mirror and this worker would stamp certified-chain
    provenance on it. Transcribed from ``open_macro_v04.pin_search_path``.
    """
    with conn.cursor() as cur:
        cur.execute("SET search_path TO public")
        cur.execute("SHOW search_path")
        current = (cur.fetchone()[0] or "").replace(" ", "")
    conn.commit()
    if current != "public":
        raise DecisionChainError(
            f"search_path is {current!r} after pinning to public; refusing to run")


# --------------------------------------------------------------------------- #
# Gate 3 — READ-ONLY catalog verification
# --------------------------------------------------------------------------- #
# Captured from the live PostgreSQL 18 catalog on 2026-08-07. This worker NEVER
# applies DDL to the chain: the table is the contract open_macro_v04 and the Light
# backend read, and a producer that could reshape it is a producer that could break
# them.  col -> (data_type, character_maximum_length | None, is_nullable)
EXPECTED_COLUMNS: dict[str, tuple[str, int | None, str]] = {
    "as_of": ("date", None, "NO"),
    "quadrant": ("text", None, "YES"),
    "candidate_quadrant": ("text", None, "YES"),
    "status": ("text", None, "NO"),
    "candidate_confidence": ("numeric", None, "YES"),
    "growth_score": ("numeric", None, "YES"),
    "inflation_score": ("numeric", None, "YES"),
    "coverage_quality": ("numeric", None, "YES"),
    "transition_pending": ("boolean", None, "NO"),
    "basis": ("text", None, "NO"),
    "pack_sha256": ("character", 64, "NO"),
    "chain_start": ("date", None, "NO"),
    "code_commit": ("character", 40, "NO"),
    "loaded_at": ("timestamp with time zone", None, "NO"),
}

COLUMNS_SQL = (
    "SELECT column_name, data_type, character_maximum_length, is_nullable "
    "FROM information_schema.columns "
    "WHERE table_schema = 'public' AND table_name = %(table)s")


def verify_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(COLUMNS_SQL, {"table": CHAIN_TABLE})
        observed = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}
    if not observed:
        raise DecisionChainError(
            f"{CHAIN_TABLE} not found in schema public — this worker EXTENDS the "
            "certified chain, it does not create it")
    missing = sorted(set(EXPECTED_COLUMNS) - set(observed))
    if missing:
        raise DecisionChainError(f"{CHAIN_TABLE} is missing columns {missing}")
    extra = sorted(set(observed) - set(EXPECTED_COLUMNS))
    if extra:
        # An unknown column this worker cannot fill would be written as its default
        # (or NULL), quietly producing a row that means something else than the 148.
        raise DecisionChainError(
            f"{CHAIN_TABLE} carries unknown columns {extra}; extend this worker "
            "after inspecting their meaning instead of letting them default")
    for column, expected in EXPECTED_COLUMNS.items():
        if observed[column] != expected:
            raise DecisionChainError(
                f"{CHAIN_TABLE}.{column} is {observed[column]}, expected {expected}")


# --------------------------------------------------------------------------- #
# Gate 4 — the chain as it stands
# --------------------------------------------------------------------------- #
READ_CHAIN_SQL = (
    "SELECT as_of, quadrant, candidate_quadrant, status, candidate_confidence, "
    "growth_score, inflation_score, coverage_quality, transition_pending, "
    "basis, pack_sha256, chain_start "
    f"FROM {CHAIN_TABLE} ORDER BY as_of")

NUMERIC_COLUMNS = ("candidate_confidence", "growth_score", "inflation_score",
                   "coverage_quality")
# What every consumer of this table actually reads. ``open_macro_v04`` selects
# (as_of, quadrant, status, candidate_confidence); the Light backend's three call
# sites select (as_of, quadrant, candidate_confidence) filtered on status='valid'.
# ``transition_pending`` joins them here because it is the latch's own memory: if it
# moved, the chain the new month threads off is not the stored one.
CONSUMABLE_COLUMNS = ("quadrant", "status", "transition_pending")


def read_chain(conn) -> dict[_dt.date, dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(READ_CHAIN_SQL)
        rows = cur.fetchall()
    out: dict[_dt.date, dict[str, Any]] = {}
    for r in rows:
        out[r[0]] = {"quadrant": r[1], "candidate_quadrant": r[2], "status": r[3],
                     "candidate_confidence": r[4], "growth_score": r[5],
                     "inflation_score": r[6], "coverage_quality": r[7],
                     "transition_pending": r[8], "basis": r[9],
                     "pack_sha256": (r[10] or "").strip(), "chain_start": r[11]}
    return out


def verify_is_the_certified_chain(stored: dict[_dt.date, dict[str, Any]],
                                  pack_sha256: str) -> None:
    """Refuse to extend anything that is not the chain this worker was built for."""
    from harness.phase0q.decision import month_end_decision_dates
    if not stored:
        raise DecisionChainError(
            f"{CHAIN_TABLE} is EMPTY. This worker extends a certified chain; "
            "creating one is scripts/rebuild_decision_chain_v3.py under owner order")
    latest = max(stored)
    expected = month_end_decision_dates(CHAIN_START, latest)
    if sorted(stored) != expected:
        got, want = set(stored), set(expected)
        raise DecisionChainError(
            f"{CHAIN_TABLE} is not a contiguous month-end series from {CHAIN_START} "
            f"to {latest}: missing={sorted(want - got)[:5]} "
            f"unexpected={sorted(got - want)[:5]}")
    bases = {row["basis"] for row in stored.values()}
    if bases != {BASIS}:
        raise DecisionChainError(f"{CHAIN_TABLE} carries basis {sorted(bases)}, "
                                 f"expected {{{BASIS!r}}}")
    starts = {row["chain_start"] for row in stored.values()}
    if starts != {CHAIN_START}:
        raise DecisionChainError(
            f"{CHAIN_TABLE} carries chain_start {sorted(starts)}, expected "
            f"{CHAIN_START} — a different genesis is a different series")
    packs = {row["pack_sha256"] for row in stored.values()}
    if packs != {pack_sha256}:
        raise DecisionChainError(
            f"{CHAIN_TABLE} was built from pack(s) {sorted(packs)}, this worker "
            f"carries {pack_sha256} — the certified prefix must be the same pack")


# --------------------------------------------------------------------------- #
# Gate 5 — which month is next
# --------------------------------------------------------------------------- #
def last_complete_month_end(today: _dt.date) -> _dt.date:
    """The last month-end strictly before ``today``'s month.

    A month in progress has no decision: the decision date IS the month-end, and the
    PIT read is ``available_at <= that month-end``. Same rule as
    ``open_macro_v04.last_complete_month_end``.
    """
    first_of_this_month = today.replace(day=1)
    return first_of_this_month - _dt.timedelta(days=1)


def next_month_end(after: _dt.date) -> _dt.date:
    """The month-end of the month following ``after``'s month."""
    from harness.phase0q.decision import _month_end
    year, month = after.year, after.month + 1
    if month > 12:
        year, month = year + 1, 1
    return _month_end(year, month)


def decision_time(as_of: _dt.date) -> _dt.datetime:
    from harness.phase0q.decision import _decision_time
    return _decision_time(as_of)


# --------------------------------------------------------------------------- #
# Gate 6 — readiness, on each arm's NATIVE frequency
# --------------------------------------------------------------------------- #
ARM_FRESHNESS_SQL = (
    f"SELECT series_id, max(available_at) FROM {VINTAGE_TABLE} "
    "WHERE series_id = ANY(%(ids)s) GROUP BY series_id")
MARKET_FRESHNESS_SQL = (
    f"SELECT max(date) FROM {EOD_TABLE} WHERE ticker = %(ticker)s")


def readiness(conn, target: _dt.date) -> dict[str, Any]:
    """Is every input arm SETTLED at the ``target`` month-end decision cutoff?

    Derived from the construction, not from a calendar. The decision reads
    ``available_at <= decision_time(target)`` (``harness.phase0q.pit``), and this
    table is append-only, so a month may be frozen ONLY when nothing can still arrive
    below that cutoff. The proof that an arm's publication stream has moved past the
    cutoff is that the arm has a vintage STRICTLY AFTER it: ``max(available_at) >
    decision_time(target)``. Counting the arm's own native periods is what makes this
    correct for a basket that mixes release lags — ACOGNO's latest PIT observation at
    a month-end is routinely two periods old and always was, so "the month's own
    observation exists" would have been wrong for the whole certified history, while
    "the arm printed again after the cutoff" is right for all of them.

    The market sensor is settled the same way: a SPY session dated after the cutoff
    (``>``, not ``>=``, so a month-end that falls on a weekend still settles).

    Consequence, stated so nobody rediscovers it as a bug: MICH prints at the END of
    its own month, i.e. AT the cutoff, so its next print — and therefore the whole
    month's settlement — lands around the last Friday of the FOLLOWING month. This is
    stricter than the owner's manual rebuild, which froze 2026-06-30 on 2026-07-17
    while MICH had not yet printed past it. For an unattended producer writing rows
    it can never revise, waiting is the cheaper error.
    """
    cutoff = decision_time(target)
    ids = arm_series_ids()
    with conn.cursor() as cur:
        cur.execute(ARM_FRESHNESS_SQL, {"ids": ids})
        latest = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(MARKET_FRESHNESS_SQL, {"ticker": market_ticker()})
        market_latest = cur.fetchone()[0]

    arms: list[dict[str, Any]] = []
    for series_id in ids:
        seen = latest.get(series_id)
        arms.append({
            "arm": series_id,
            "last_available_at": seen.isoformat() if seen else None,
            "settled": bool(seen is not None and seen > cutoff),
        })
    arms.append({
        "arm": f"{market_ticker()}:{EOD_TABLE}",
        "last_available_at": market_latest.isoformat() if market_latest else None,
        "settled": bool(market_latest is not None and market_latest > target),
    })
    pending = [a["arm"] for a in arms if not a["settled"]]
    return {"cutoff": cutoff.isoformat(), "arms": arms, "pending": pending,
            "ready": not pending}


# --------------------------------------------------------------------------- #
# Gate 7 — inputs and the replay
# --------------------------------------------------------------------------- #
MACRO_DELTA_SQL = (
    "SELECT series_id, observation_period, available_at, vintage_date, "
    f"revision_number, value FROM {VINTAGE_TABLE} "
    "WHERE series_id = ANY(%(ids)s) AND available_at > %(boundary)s "
    "ORDER BY series_id, observation_period, available_at")
EOD_DELTA_SQL = (
    f"SELECT date, adj_close FROM {EOD_TABLE} "
    "WHERE ticker = %(ticker)s AND date > %(boundary)s AND adj_close IS NOT NULL "
    "ORDER BY date")


def read_macro_delta(conn, boundary: _dt.datetime) -> list[dict[str, Any]]:
    """Live vintages published after the pack's window, in the pack's own row shape.

    ``harness.phase0q.pit`` parses ``available_at`` / ``observation_period`` /
    ``vintage_date`` as ISO STRINGS, so the delta is normalized to strings here rather
    than teaching the PIT reader a second input dialect. ``revision_number`` and
    ``vintage_date`` matter: they are the deterministic tie-break for two vintages of
    one period sharing an ``available_at``.
    """
    with conn.cursor() as cur:
        cur.execute(MACRO_DELTA_SQL, {"ids": arm_series_ids(), "boundary": boundary})
        rows = cur.fetchall()
    out = []
    for series_id, period, available_at, vintage_date, revision, value in rows:
        if value is None:
            raise DecisionChainError(
                f"{VINTAGE_TABLE} row {series_id} {period} @ {available_at} has a "
                "NULL value; refusing to guess what was published")
        out.append({
            "series_id": series_id,
            "observation_period": period.isoformat(),
            "available_at": available_at.astimezone(_dt.timezone.utc).isoformat(),
            "vintage_date": vintage_date.isoformat() if vintage_date else None,
            "revision_number": int(revision) if revision is not None else None,
            "value": float(value),
        })
    return out


def read_eod_delta(conn, boundary: _dt.date) -> list[dict[str, Any]]:
    """Live market sessions after the pack's last one, in the pack's row shape.

    Only the model's own ticker is read (``quadrant_market_observation`` filters to
    it), and only sessions AFTER the pack's last one: a pack-covered session must come
    from the pack, so the certified prefix cannot move if the price mirror is ever
    re-adjusted. (Audited 2026-08-07: live ``adj_close`` matches the pack byte-for-byte
    on sampled sessions from 1998 to 2026 — the mirror is not re-adjusted today. The
    boundary is what keeps that from mattering.)
    """
    with conn.cursor() as cur:
        cur.execute(EOD_DELTA_SQL, {"ticker": market_ticker(), "boundary": boundary})
        rows = cur.fetchall()
    return [{"ticker": market_ticker(), "date": date.isoformat(),
             "adjusted_close": float(adj_close)} for date, adj_close in rows]


def compute_series(macro_rows: list[dict], eod_rows: list[dict], target: _dt.date):
    """The certified engine, called — not reimplemented."""
    from harness.phase0q.decision_v3 import run_decision_series_v3
    return run_decision_series_v3(macro_rows, eod_rows, CHAIN_START, target)


# --------------------------------------------------------------------------- #
# Gate 8 — the prefix gate
# --------------------------------------------------------------------------- #
# Postgres stores a float8 parameter in a NUMERIC column through DBL_DIG = 15
# significant digits. The 148 stored rows were written by the 2026-07-17 rebuild,
# which passed raw floats and therefore carries that rendering; rows this worker
# writes go through ``_exact_numeric`` and carry all 17. Comparing a recomputed double
# against a stored row must therefore allow the 15-digit rendering — a statement about
# the storage of the OLD rows, never a tolerance on the new one.
FLOAT8_TO_NUMERIC_DIGITS = 15


def _stored_matches_double(stored: decimal.Decimal | None, computed: float | None) -> bool:
    if stored is None or computed is None:
        return stored is None and computed is None
    if stored == decimal.Decimal(repr(float(computed))):
        return True
    return stored == decimal.Decimal("%.*g" % (FLOAT8_TO_NUMERIC_DIGITS, computed))


def prefix_gate(series, stored: dict[_dt.date, dict[str, Any]]) -> dict[str, Any]:
    """The replayed prefix must still carry the certified chain's consumable answer.

    HARD FAILURE on any stored month whose ``quadrant`` / ``status`` /
    ``transition_pending`` changed: those are what every consumer reads and what the
    latch threads, so a change there means the month this run is about to append does
    not sit on the chain that is in the table. Nothing is written in that case — the
    owner decides whether the chain gets re-certified.

    ``candidate_quadrant`` and the four NUMERICs are REPORTED, not gated: they move by
    the model's own endpoint dependence (module docstring), they are recomputation
    artifacts on rows this worker never writes, and no consumer reads
    ``candidate_quadrant`` at all.
    """
    replayed = {row.as_of: row for row in series}
    absent = sorted(set(stored) - set(replayed))
    if absent:
        raise DecisionChainError(
            f"the replay does not cover stored months {absent[:5]} — the engine and "
            "the table disagree about the series' span")

    consumable_drift: list[dict[str, Any]] = []
    candidate_drift: list[dict[str, Any]] = []
    numeric_drift: list[dict[str, Any]] = []
    for as_of, want in stored.items():
        got = replayed[as_of]
        for column in CONSUMABLE_COLUMNS:
            if want[column] != getattr(got, column):
                consumable_drift.append({"as_of": as_of.isoformat(), "column": column,
                                         "stored": want[column],
                                         "replayed": getattr(got, column)})
        if want["candidate_quadrant"] != got.candidate_quadrant:
            candidate_drift.append({"as_of": as_of.isoformat(),
                                    "stored": want["candidate_quadrant"],
                                    "replayed": got.candidate_quadrant})
        for column in NUMERIC_COLUMNS:
            if not _stored_matches_double(want[column], getattr(got, column)):
                numeric_drift.append({
                    "as_of": as_of.isoformat(), "column": column,
                    "delta": abs(float(want[column]) - float(getattr(got, column)))
                    if want[column] is not None and getattr(got, column) is not None
                    else None})
    if consumable_drift:
        raise DecisionChainError(
            "prefix gate: the extended replay changes the CONSUMABLE projection of "
            f"{len({d['as_of'] for d in consumable_drift})} already-published "
            f"month(s) — nothing was written. First diffs: {consumable_drift[:5]}")
    return {
        "months_verified": len(stored),
        "candidate_quadrant_drift": candidate_drift,
        "numeric_drift_cells": len(numeric_drift),
        "numeric_drift_max": max((d["delta"] for d in numeric_drift
                                  if d["delta"] is not None), default=0.0),
        "numeric_drift_months": sorted({d["as_of"] for d in numeric_drift}),
    }


# --------------------------------------------------------------------------- #
# Publication — the ONLY write in this module
# --------------------------------------------------------------------------- #
ROW_COLUMNS = ("as_of", "quadrant", "candidate_quadrant", "status",
               "candidate_confidence", "growth_score", "inflation_score",
               "coverage_quality", "transition_pending", "basis", "pack_sha256",
               "chain_start", "code_commit", "loaded_at")

# APPEND-ONLY. ``ON CONFLICT (as_of) DO NOTHING`` makes a re-run over a published
# month a no-op in the DATABASE, so idempotence does not depend on this worker having
# read the table first. There is deliberately no UPDATE branch: an existing month is
# certified history and only the owner-ordered manual script may revise it.
INSERT_SQL = (
    f"INSERT INTO {CHAIN_TABLE} (" + ", ".join(ROW_COLUMNS) + ") VALUES ("
    + ", ".join(f"%({c})s" for c in ROW_COLUMNS) + ") ON CONFLICT (as_of) DO NOTHING")


def _exact_numeric(value: Any) -> Any:
    """Python float -> ``decimal.Decimal(repr(value))`` for EXACT NUMERIC persistence.

    Postgres casts a float8 parameter to NUMERIC through DBL_DIG = 15 significant
    digits, so a raw Python float is silently truncated on the way in. ``repr`` of a
    float is the SHORTEST decimal string that round-trips, so ``float(Decimal(repr(x)))
    == x`` exactly. Transcribed from ``open_macro_v03`` / ``open_macro_v04`` /
    ``fund_peer_groups``: this is the house write chokepoint, not a variant of it.
    """
    if isinstance(value, float):
        return decimal.Decimal(repr(value))
    return value


def _exact_numeric_params(params: dict[str, Any]) -> dict[str, Any]:
    """A COPY of ``params`` with EVERY float converted — by TYPE, not by column name,
    so a numeric column added to ``ROW_COLUMNS`` tomorrow is covered without anyone
    remembering to list it. The copy matters: ``post_write_verify`` compares the
    re-read row against the dict the caller still holds, which keeps the raw floats."""
    return {key: _exact_numeric(value) for key, value in params.items()}


def build_row(decision, pack_sha256: str, commit: str,
              now: _dt.datetime) -> dict[str, Any]:
    """Project one ``DecisionRow`` onto the chain's 14 columns.

    ``pack_sha256`` stays the CERTIFIED pack's: it says which prefix this month
    extends, which is exactly what it says on the 148 rows before it. ``code_commit``
    is this worker's revision, so worker-produced months are distinguishable from the
    2026-07-17 rebuild's in one SQL query and nobody has to guess.
    """
    if decision.status == "valid" and decision.quadrant is None:
        raise DecisionChainError(
            f"{decision.as_of}: status 'valid' with a NULL quadrant violates the "
            "table's own CHECK — refusing to publish")
    if decision.status != "valid" and decision.quadrant is not None:
        raise DecisionChainError(
            f"{decision.as_of}: status {decision.status!r} with a non-NULL quadrant "
            "violates the table's own CHECK — refusing to publish")
    return {
        "as_of": decision.as_of,
        "quadrant": decision.quadrant,
        "candidate_quadrant": decision.candidate_quadrant,
        "status": decision.status,
        "candidate_confidence": decision.candidate_confidence,
        "growth_score": decision.growth_score,
        "inflation_score": decision.inflation_score,
        "coverage_quality": decision.coverage_quality,
        "transition_pending": bool(decision.transition_pending),
        "basis": BASIS,
        "pack_sha256": pack_sha256,
        "chain_start": CHAIN_START,
        "code_commit": commit,
        "loaded_at": now,
    }


def publish(conn, row: dict[str, Any]) -> int:
    """Insert the one month. Returns 1 when it landed, 0 when it was already there."""
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, _exact_numeric_params(row))
        inserted = cur.rowcount
    conn.commit()
    return int(inserted)


READBACK_SQL = ("SELECT " + ", ".join(ROW_COLUMNS[:-1])
                + f" FROM {CHAIN_TABLE} WHERE as_of = %(as_of)s")


def post_write_verify(conn, row: dict[str, Any]) -> None:
    """Re-read the published month and compare it to what was computed.

    ``loaded_at`` is excluded: it is stamped by this run and carries no model content.
    Every other column must come back equal — the NUMERICs EXACTLY, because
    ``_exact_numeric`` was used, so the 15-digit escape hatch that the old rows need
    must NOT apply to a row this worker just wrote.
    """
    with conn.cursor() as cur:
        cur.execute(READBACK_SQL, {"as_of": row["as_of"]})
        read = cur.fetchone()
    if read is None:
        raise DecisionChainError(f"published month {row['as_of']} is not readable back")
    for column, value in zip(ROW_COLUMNS[:-1], read):
        want = row[column]
        if isinstance(want, float):
            if value is None or decimal.Decimal(repr(want)) != value:
                raise DecisionChainError(
                    f"post-write verify: {column} read back {value!r}, wrote {want!r}")
        elif isinstance(want, str) and isinstance(value, str):
            if want != value.strip():   # bpchar columns come back space padded
                raise DecisionChainError(
                    f"post-write verify: {column} read back {value!r}, wrote {want!r}")
        elif want != value:
            raise DecisionChainError(
                f"post-write verify: {column} read back {value!r}, wrote {want!r}")


def code_commit() -> str:
    sha = os.environ.get("RAILWAY_GIT_COMMIT_SHA")
    if not sha:
        sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                             capture_output=True, text=True,
                             check=True).stdout.strip()
    sha = sha.strip()
    if len(sha) != 40:
        raise DecisionChainError(
            f"code_commit {sha!r} is not a 40-char sha; the column is char(40)")
    return sha


# --------------------------------------------------------------------------- #
# run()
# --------------------------------------------------------------------------- #
def run(dsn: str, *, today: _dt.date | None = None) -> dict[str, Any]:
    pack = verify_pack()
    today = today or _dt.datetime.now(_dt.timezone.utc).date()
    stats: dict[str, Any] = {"input_pack_id": pack["input_pack_id"],
                             "published": 0}

    with connect(resolve_dsn(dsn)) as conn, \
            advisory_lock(conn, LOCK_OPEN_MACRO_V03_CHAIN) as acquired:
        if not acquired:
            # Another run of THIS worker holds the lock; its replay will publish the
            # same month. Not an error and not an abort: the next cron sees the result.
            print("open_macro_v03_chain: advisory lock held by a concurrent run; "
                  "nothing to do", flush=True)
            return {**stats, "skipped": "advisory_lock_held"}

        pin_search_path(conn)
        verify_schema(conn)
        stored = read_chain(conn)
        verify_is_the_certified_chain(stored, pack["input_pack_sha256"])
        latest = max(stored)
        target = next_month_end(latest)
        horizon = last_complete_month_end(today)
        stats.update({"chain_rows": len(stored), "chain_latest": latest.isoformat(),
                      "target": target.isoformat(),
                      "last_complete_month_end": horizon.isoformat()})

        if target > horizon:
            print(f"open_macro_v03_chain: {target} is not a complete month yet "
                  f"(last complete month-end is {horizon}); nothing to do", flush=True)
            return {**stats, "reason": "month_in_progress"}

        gate = readiness(conn, target)
        stats["readiness"] = gate
        if not gate["ready"]:
            # FAIL-LOUD-AS-NO-OP: the month is NOT published and every arm that has
            # not printed past the cutoff is named. Publishing here would freeze a row
            # computed from an incomplete information set that this table can never
            # revise; forward-filling the missing arm would be the same error, quietly.
            for arm in gate["arms"]:
                if not arm["settled"]:
                    print(f"open_macro_v03_chain: arm {arm['arm']} has not published "
                          f"past {gate['cutoff']} (last: {arm['last_available_at']})",
                          flush=True)
            print(f"open_macro_v03_chain: {target} inputs not settled "
                  f"({len(gate['pending'])} arm(s) pending); nothing to do", flush=True)
            return {**stats, "reason": "inputs_not_settled"}

        macro_rows, eod_rows, macro_boundary, eod_boundary = load_pack_inputs()
        macro_delta = read_macro_delta(conn, macro_boundary)
        eod_delta = read_eod_delta(conn, eod_boundary)
        stats.update({"pack_macro_rows": len(macro_rows),
                      "pack_eod_rows": len(eod_rows),
                      "live_macro_delta_rows": len(macro_delta),
                      "live_eod_delta_rows": len(eod_delta),
                      "macro_boundary": macro_boundary.isoformat(),
                      "eod_boundary": eod_boundary.isoformat()})

        series = compute_series(macro_rows + macro_delta, eod_rows + eod_delta, target)
        if not series or series[-1].as_of != target:
            raise DecisionChainError(
                f"the replay's last month is "
                f"{series[-1].as_of if series else None}, expected {target}")
        stats["prefix"] = prefix_gate(series, stored)

        row = build_row(series[-1], pack["input_pack_sha256"], code_commit(),
                        _dt.datetime.now(_dt.timezone.utc))
        inserted = publish(conn, row)
        if inserted:
            post_write_verify(conn, row)
        stats.update({
            "published": inserted,
            "row": {"as_of": row["as_of"].isoformat(), "quadrant": row["quadrant"],
                    "candidate_quadrant": row["candidate_quadrant"],
                    "status": row["status"],
                    "candidate_confidence": row["candidate_confidence"],
                    "growth_score": row["growth_score"],
                    "inflation_score": row["inflation_score"],
                    "coverage_quality": row["coverage_quality"],
                    "transition_pending": row["transition_pending"]},
        })
        if not inserted:
            print(f"open_macro_v03_chain: {target} was already present; no-op",
                  flush=True)
    return stats


def main(argv: list[str] | None = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="open_macro_v03_chain")
    ap.add_argument("--dsn", default=None)
    ap.add_argument("--today", default=None,
                    help="pin the run date (YYYY-MM-DD) for a rehearsal")
    args = ap.parse_args(argv)
    today = _dt.date.fromisoformat(args.today) if args.today else None
    print(json.dumps(run(resolve_dsn(args.dsn), today=today), default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
