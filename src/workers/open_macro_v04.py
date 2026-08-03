"""open_macro v4.0-rev (M-COMP4) monthly regime worker — DARK RUN.

WHAT THIS PUBLISHES
-------------------
One row per MONTH-END in ``open_macro_v04_decisions`` + ``open_macro_v04_allocations``:
the three-layer state (L1 fiscal router, L3 credit guard, L2 book) and the book it
composes. These are SIBLING tables — ``open_macro_v03_decisions`` / ``_allocations``
are keyed by business day, written every day by the live certified worker, and are
NOT touched here. Dual-writing a monthly v4 row into them would collide on the
shared ``as_of`` primary key for the whole dark run.

THE ENGINE IS THE REPLAY'S ENGINE, LITERALLY
--------------------------------------------
``run()`` reads the four macro series, the decision chain and the price frame out of
the database, then calls :func:`harness.phase0q.v4_replay.build_ledger` — the SAME
function the fixture replay calls. There is no second implementation to drift. The
parity test in ``tests/test_open_macro_v04_worker.py`` feeds the pinned fixtures
through the worker's own call and compares the result to ``golden_ledger.csv`` byte
for byte, so "the worker computes what the signed ledger says" is a measurement, not
a claim.

CADENCE: MONTHLY DECISION, DAILY IDEMPOTENT RUN
-----------------------------------------------
The cron is daily; the product is monthly. Every run republishes every computable
month-end from ``PUBLISH_START`` to the last COMPLETE month-end, upserting. That is
deliberate: a month whose inputs were revised gets the revision, a month that was
never published gets published, and a run that dies halfway is repaired by the next
one. Nothing depends on a run having happened on a particular day.

``decision_basis`` records which kind of row it is:

  ``live``              this ``as_of`` was the last complete month-end at the moment
                        of the run AND the row did not exist before — a decision the
                        system actually lived through.
  ``bootstrap_replay``  a retroactive month reconstructed from the CURRENT vintage of
                        the inputs. Honest by name: the mirror is current-vintage, not
                        per-observation PIT, so a retroactive row is a replay of what
                        the engine WOULD have said, not a record of what it did say.

It is NEVER DEMOTED: the upsert's UPDATE branch does not touch the column at all, so
a month published live stays live no matter how many times it is recomputed. That is
a database-level guarantee, not a Python one.

FAIL-LOUD ORDER (zero side effects before every gate)
-----------------------------------------------------
 1. FORMULATION FREEZE — recompute the sha256 (CRLF->LF) of the four formula modules
    against the freeze artifact's pins, and recompute the canonical
    ``formulation_sha256`` over its own {books, formulation, freshness, gates} block.
    A mismatch raises with ZERO side effects: not one connection is opened.
 2. connect + pin ``search_path`` to public + advisory lock 900_218.
 3. READ-ONLY catalog verification of the two v04 tables. ``run()`` never applies
    schema — the operator does (``schemas/open_macro_v04_*.sql``).
 4. read the inputs, each series by EXPLICIT FRED id. A required series that is
    absent or empty raises NAMING THE SERIES.
 5. ``input_digest_sha256`` over the canonical serialization of every input.
 6. the ledger, through the shared replay path.
 7. publish decisions + allocations for every computable month, in ONE transaction.
 8. live freshness: the last month-end must be computable, and the per-arm coverage
    at the RUN DATE is reported. An uncomputable ``deficit_gdp`` at the latest
    month-end is a HARD FAILURE naming the input — staleness is a loud failure here,
    never a silent short publish.
 9. post-write verification: the published count, and a full re-read of the latest
    row compared field by field against what was computed.

WHY THE LIVE DIGEST WILL NEVER EQUAL THE FIXTURE SNAPSHOT DIGEST
-----------------------------------------------------------------
``macro_data.value`` is ``NUMERIC(24,6)``; the pinned fixtures carry full float64
precision. Same recipe, different bytes, therefore different digest — by
construction, not by drift. The digest exists to pin THIS run's inputs so a later
disagreement can be attributed; it is not a claim of equality with the snapshot.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import hashlib
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from harness.direct_activation import credit_guard as _guard
from harness.phase0q import book_router as _books
from harness.phase0q import v4_replay as _replay
from src import fiscal_state as _fiscal
from src.db import LOCK_OPEN_MACRO_V04, advisory_lock, connect

ROOT = Path(__file__).resolve().parents[2]
FREEZE_PATH = (ROOT / "artifacts" / "quant" / "open_macro_v4_formulation_freeze_001"
               / "formulation_freeze.json")

# The four modules the freeze describes. The trust base lives HERE, in the worker,
# not in the (unpinned) builder: otherwise an edit to the builder alongside the
# artifact could redefine the closure the gate compares against and smuggle a
# truncated pin set past it.
FORMULA_MODULES: tuple[str, ...] = (
    "src/fiscal_state.py",
    "harness/direct_activation/credit_guard.py",
    "harness/phase0q/book_router.py",
    "harness/phase0q/v4_replay.py",
)
FORMULATION_SHA_SCOPE: tuple[str, ...] = ("books", "formulation", "freshness", "gates")

# The engine reads these four by EXPLICIT FRED id. `GDP` is the NOMINAL LEVEL
# series, never A191RL1Q225SBEA (real growth): substituting one for the other
# silently changes the denominator of deficit_gdp.
REQUIRED_SERIES: tuple[str, ...] = (
    _fiscal.MTS_SERIES_ID,      # MTSDS133FMS — L1 numerator
    _fiscal.GDP_SERIES_ID,      # GDP         — L1 denominator
    _guard.SLOOS_SERIES_ID,     # SUBLPDCILSLGNQ — L3 arm A
    _guard.M2_SERIES_ID,        # M2SL        — L3 arm B
)
# The pre-chain quadrant proxy. OPTIONAL: it is a replay-only historical
# reconstruction, never a live decision path, so its absence degrades the pre-2014
# months to `proxy_missing` rather than failing the run.
PROXY_SERIES: tuple[str, ...] = (_replay.CFNAI_SERIES_ID, _replay.CPI_SERIES_ID)

# The order the combined input digest is built in — the canonical snapshot order.
# Not alphabetical and not arbitrary: the digest does not reproduce under any other.
CANONICAL_INPUT_ORDER: tuple[str, ...] = (
    _fiscal.GDP_SERIES_ID, _fiscal.MTS_SERIES_ID, _guard.SLOOS_SERIES_ID,
    _guard.M2_SERIES_ID, _replay.CFNAI_SERIES_ID, _replay.CPI_SERIES_ID,
    "chain", "prices")

# The seven instruments the book is priced on. HYG is in the engine's universe at a
# structural zero and is deliberately NOT priced or published (see the allocations
# DDL): pricing a sleeve that no emittable book can hold would invent a dependency.
BOOK_TICKERS: tuple[str, ...] = _replay.FRAME_TICKERS

# The published weight columns, in DDL order. Same set, same reason.
WEIGHT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("w_spy", "SPY"), ("w_tlt", "TLT"), ("w_tip", "TIP"), ("w_gld", "GLD"),
    ("w_dbc", "DBC"), ("w_shy", "SHY"), ("w_lqd", "LQD"))

CHAIN_TABLE = "open_macro_v03_decision_chain"
DECISIONS_TABLE = "open_macro_v04_decisions"
ALLOCATIONS_TABLE = "open_macro_v04_allocations"

# The window the signed configuration was measured on opens here. Earlier months are
# computed (the state machine needs the run-up) but never published.
PUBLISH_START = _dt.date(2006, 12, 31)

BASIS_LIVE = "live"
BASIS_BOOTSTRAP = "bootstrap_replay"

AS_OF_ENV = "OPEN_MACRO_V04_AS_OF"


class OpenMacroV04Error(RuntimeError):
    """A v4 runtime gate did not hold; the run must fail loud."""


# --------------------------------------------------------------------------- #
# Gate 1 — the frozen formulation
# --------------------------------------------------------------------------- #
def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise ValueError(f"duplicate JSON key {key!r}")
        payload[key] = value
    return payload


def _reject_non_finite_constant(constant: str) -> None:
    raise ValueError(f"non-finite JSON constant {constant!r}")


def _reject_non_finite_float(value: str) -> float:
    parsed = float(value)
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        raise ValueError(f"non-finite JSON number {value!r}")
    return parsed


def _load_json(path: Path) -> Any:
    """STRICT loader for the freeze artifact: duplicate keys and NaN/Infinity are
    rejected, so a doctored artifact cannot smuggle a second ``pins`` past the gate."""
    return json.loads(path.read_text(encoding="utf-8"),
                      object_pairs_hook=_reject_duplicate_keys,
                      parse_constant=_reject_non_finite_constant,
                      parse_float=_reject_non_finite_float)


def _sha256_norm(path: Path) -> str:
    """sha256 of file bytes with CRLF->LF normalization (git-checkout agnostic)."""
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def _canonical_block_sha256(block: dict) -> str:
    return hashlib.sha256(
        json.dumps(block, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def check_universe_coverage() -> None:
    """The published weight columns must BE the router's universe minus HYG.

    Part of Gate 1 rather than a comment: if the router ever gains an instrument, the
    v04 allocations table needs a column for it AND its weight-sum constraint needs
    re-deriving. Discovering that from a NOT NULL violation — or worse, from a book
    that quietly sums to 1 because the missing sleeve happened to be zero — is too
    late. HYG is the ONE permitted omission and it is named here, not inferred."""
    published = {t for _, t in WEIGHT_COLUMNS}
    universe = set(_books.UNIVERSE)
    unexpected = sorted(published - universe)
    omitted = sorted(universe - published)
    if unexpected:
        raise OpenMacroV04Error(
            f"the v04 allocations schema publishes {unexpected}, which the book "
            "router's universe does not contain")
    if omitted != ["HYG"]:
        raise OpenMacroV04Error(
            f"the book router's universe carries {omitted} that the v04 allocations "
            "schema does not publish; only HYG may be omitted (it is structurally "
            "zero). Add the column and re-derive the weight-sum constraint")


def verify_formulation_freeze(freeze: dict[str, Any] | None = None,
                              root: Path = ROOT) -> str:
    """Raise unless the freeze artifact describes THIS tree, and return its
    ``formulation_sha256``.

    Three independent things are checked, and each one alone is insufficient:

    * the pinned module SET is exactly ``FORMULA_MODULES`` — a truncated artifact
      that simply omits ``book_router.py`` must not pass by iterating only the keys
      it happens to contain;
    * every pinned sha256 recomputes over the tree — the formula on disk is the
      formula that was frozen;
    * ``formulation_sha256`` recomputes over the artifact's OWN
      {books, formulation, freshness, gates} block — an edit to a threshold's prose
      or to the books table without re-deriving the digest is caught even when the
      modules are untouched.
    """
    check_universe_coverage()
    freeze = _load_json(FREEZE_PATH) if freeze is None else freeze
    pins = freeze.get("pins")
    if not isinstance(pins, dict):
        raise OpenMacroV04Error(
            f"formulation freeze {FREEZE_PATH.name} has no pins block")
    modules = pins.get("modules")
    if not isinstance(modules, dict):
        raise OpenMacroV04Error("formulation freeze pins.modules missing")
    if set(modules) != set(FORMULA_MODULES):
        missing = sorted(set(FORMULA_MODULES) - set(modules))
        extra = sorted(set(modules) - set(FORMULA_MODULES))
        raise OpenMacroV04Error(
            "formulation freeze pins a different module set than the v4 formula "
            f"closure (missing={missing}, unexpected={extra})")
    for relative in sorted(modules):
        actual = _sha256_norm(root / relative)
        if actual != modules[relative]:
            raise OpenMacroV04Error(
                f"formula module pin mismatch for {relative}: {actual} != pinned "
                f"{modules[relative]} (the engine on disk is not the frozen one)")
    missing_blocks = [b for b in FORMULATION_SHA_SCOPE if b not in freeze]
    if missing_blocks:
        raise OpenMacroV04Error(
            f"formulation freeze is missing the blocks {missing_blocks} the "
            "canonical digest is taken over")
    recomputed = _canonical_block_sha256({b: freeze[b] for b in FORMULATION_SHA_SCOPE})
    recorded = pins.get("formulation_sha256")
    if recomputed != recorded:
        raise OpenMacroV04Error(
            f"formulation_sha256 {recorded!r} != recomputed {recomputed!r} (the "
            "frozen formulation block was altered without re-deriving its digest)")
    return recomputed


# --------------------------------------------------------------------------- #
# Gate 2 — session hygiene
# --------------------------------------------------------------------------- #
def pin_search_path(conn) -> None:
    """Force ``search_path`` to public BEFORE any table access.

    Every table is referenced bare, so a non-public DSN/role default would resolve
    them against that schema — the worker would read a look-alike mirror and stamp
    production provenance on it. SETTING the path (not merely reading it) overrides
    any startup default; the read-back re-asserts it landed."""
    with conn.cursor() as cur:
        cur.execute("SET search_path TO public")
        cur.execute("SHOW search_path")
        current = (cur.fetchone()[0] or "").replace(" ", "")
    conn.commit()
    if current != "public":
        raise OpenMacroV04Error(
            f"search_path is {current!r} after pinning to public; refusing to run "
            "against a non-public schema")


# --------------------------------------------------------------------------- #
# Gate 3 — READ-ONLY catalog verification
# --------------------------------------------------------------------------- #
# MAINTENANCE: mirrors schemas/open_macro_v04_*.sql EXACTLY. Captured from a real
# PostgreSQL 18 catalog after applying the committed DDL (the same major version
# production runs), so this is an observation, not a transcription.
#   col -> (data_type, character_maximum_length | None, is_nullable, column_default | None)
EXPECTED_COLUMNS: dict[str, dict[str, tuple]] = {
    DECISIONS_TABLE: {
        "as_of": ("date", None, "NO", None),
        "fiscal_state": ("text", None, "NO", None),
        "fiscal_boundary": ("boolean", None, "NO", None),
        "fiscal_state_age_m": ("integer", None, "NO", None),
        "deficit_gdp": ("numeric", None, "NO", None),
        "guard_level": ("text", None, "NO", None),
        "guard_coverage": ("text", None, "NO", None),
        "arm_a": ("boolean", None, "NO", None),
        "arm_b": ("boolean", None, "NO", None),
        "severe_run_age": ("integer", None, "NO", None),
        "severe_degraded": ("boolean", None, "NO", None),
        "stress_confirmed": ("boolean", None, "NO", None),
        "quadrant": ("text", None, "YES", None),
        "quadrant_source": ("text", None, "NO", None),
        "carry_age": ("integer", None, "NO", None),
        "quadrant_confidence": ("numeric", None, "YES", None),
        "decision_validity": ("text", None, "NO", None),
        "decision_basis": ("text", None, "NO", None),
        "input_digest_sha256": ("character", 64, "NO", None),
        "formulation_sha256": ("character", 64, "NO", None),
        "code_commit": ("character", 40, "NO", None),
        "run_id": ("text", None, "NO", None),
        "publish_state": ("text", None, "NO", "'published'::text"),
        "valid_status": ("text", None, "NO", "'valid'::text"),
        "valid_until": ("timestamp with time zone", None, "NO", None),
        "invalidated_at": ("timestamp with time zone", None, "YES", None),
        "invalidated_reason": ("text", None, "YES", None),
        "created_at": ("timestamp with time zone", None, "NO", "now()"),
        "updated_at": ("timestamp with time zone", None, "NO", "now()"),
    },
    ALLOCATIONS_TABLE: {
        "as_of": ("date", None, "NO", None),
        "book_id": ("text", None, "NO", None),
        "w_spy": ("numeric", None, "NO", None),
        "w_tlt": ("numeric", None, "NO", None),
        "w_tip": ("numeric", None, "NO", None),
        "w_gld": ("numeric", None, "NO", None),
        "w_dbc": ("numeric", None, "NO", None),
        "w_shy": ("numeric", None, "NO", None),
        "w_lqd": ("numeric", None, "NO", None),
        "priced_at": ("date", None, "NO", None),
        "input_digest_sha256": ("character", 64, "NO", None),
        "formulation_sha256": ("character", 64, "NO", None),
        "code_commit": ("character", 40, "NO", None),
        "run_id": ("text", None, "NO", None),
        "publish_state": ("text", None, "NO", "'published'::text"),
        "valid_status": ("text", None, "NO", "'valid'::text"),
        "valid_until": ("timestamp with time zone", None, "NO", None),
        "invalidated_at": ("timestamp with time zone", None, "YES", None),
        "invalidated_reason": ("text", None, "YES", None),
        "created_at": ("timestamp with time zone", None, "NO", "now()"),
        "updated_at": ("timestamp with time zone", None, "NO", "now()"),
    },
}

_CATALOG_COLUMNS_SQL = (
    "SELECT table_name, column_name, data_type, character_maximum_length, "
    "is_nullable, column_default FROM information_schema.columns "
    "WHERE table_schema = 'public' AND table_name = ANY(%(tables)s) "
    "ORDER BY table_name, ordinal_position")


def verify_schema(conn) -> dict[str, Any]:
    """Verify the live catalog against the committed DDL (READ-ONLY, SELECT only).

    Scoped to ``public``, so a look-alike scratch schema cannot be certified in place
    of the objects the worker writes. The full column signature is compared — type,
    length, nullability, default — because CHAR(40) where CHAR(64) was declared, or a
    dropped DEFAULT, is exactly the drift a bare name check waves through.
    ``run()`` never creates or alters anything: schema lifecycle is the operator's
    (``schemas/open_macro_v04_*.sql``), so an absent or drifted catalog fails here
    with zero writes."""
    tables = sorted(EXPECTED_COLUMNS)
    with conn.cursor() as cur:
        cur.execute(_CATALOG_COLUMNS_SQL, {"tables": tables})
        rows = cur.fetchall()

    actual: dict[str, dict[str, tuple]] = {t: {} for t in tables}
    for table, column, data_type, char_len, nullable, default in rows:
        actual.setdefault(table, {})[column] = (data_type, char_len, nullable, default)

    problems: list[str] = []
    for table, expected in EXPECTED_COLUMNS.items():
        columns = actual.get(table) or {}
        if not columns:
            problems.append(f"{table}: table missing from the catalog (apply "
                            f"schemas/{table}.sql first)")
            continue
        missing = sorted(set(expected) - set(columns))
        extra = sorted(set(columns) - set(expected))
        if missing or extra:
            problems.append(f"{table}: column set diverges from the committed DDL "
                            f"(missing={missing}, unexpected={extra})")
        for col in sorted(set(expected) & set(columns)):
            if columns[col] != expected[col]:
                problems.append(
                    f"{table}.{col}: signature {columns[col]} != expected "
                    f"{expected[col]} (type, length, nullable, default)")
    if problems:
        raise OpenMacroV04Error(
            "schema catalog verification failed: " + "; ".join(problems))
    return {t: dict(actual[t]) for t in tables}


# --------------------------------------------------------------------------- #
# Gate 4 — the inputs
# --------------------------------------------------------------------------- #
MACRO_SERIES_SQL = (
    "SELECT obs_date, value FROM macro_data "
    "WHERE series_id = %(series_id)s AND obs_date <= %(horizon)s "
    "ORDER BY obs_date")

PRICES_SQL = (
    "SELECT ticker, date, adj_close FROM eod_prices "
    "WHERE ticker = ANY(%(tickers)s) AND date <= %(horizon)s "
    "ORDER BY ticker, date")

CHAIN_SQL = (
    f"SELECT as_of, quadrant, status, candidate_confidence FROM {CHAIN_TABLE} "
    "WHERE as_of <= %(horizon)s ORDER BY as_of")


def last_complete_month_end(today: _dt.date) -> _dt.date:
    """The last month-end strictly before ``today``'s month.

    A month in progress has no decision: every sensor is PIT-lagged to the CLOSE of
    a month-end, and there is no close yet."""
    first_of_month = today.replace(day=1)
    return first_of_month - _dt.timedelta(days=1)


def resolve_as_of(as_of_arg: str | None = None, *,
                  today: _dt.date | None = None) -> _dt.date:
    """The month-end this run publishes through.

    An explicit override (argument or ``OPEN_MACRO_V04_AS_OF``) must itself be a
    month-end and may not be in the future — the worker never stamps a decision for a
    month that has not closed."""
    if today is None:
        from zoneinfo import ZoneInfo
        today = _dt.datetime.now(ZoneInfo("America/New_York")).date()
    latest = last_complete_month_end(today)
    override = as_of_arg or os.environ.get(AS_OF_ENV)
    if not override:
        return latest
    resolved = _dt.date.fromisoformat(override)
    if resolved != last_complete_month_end(resolved + _dt.timedelta(days=1)):
        raise OpenMacroV04Error(
            f"as_of override {resolved.isoformat()} is not a month-end; the v4 "
            "engine is monthly and a mid-month decision does not exist")
    if resolved > latest:
        raise OpenMacroV04Error(
            f"as_of override {resolved.isoformat()} is beyond the last complete "
            f"month-end {latest.isoformat()}; a month in progress has no close to "
            "decide at")
    return resolved


def read_macro_series(conn, series_id: str, horizon: _dt.date,
                      *, required: bool) -> pd.Series:
    """One ``macro_data`` series by EXPLICIT id, as float64 indexed by obs_date.

    A required series that is absent or empty raises NAMING IT: "the engine has no
    fiscal reading" is a diagnosis an operator can act on; a KeyError three frames
    deep is not."""
    with conn.cursor() as cur:
        cur.execute(MACRO_SERIES_SQL, {"series_id": series_id, "horizon": horizon})
        rows = cur.fetchall()
    if not rows:
        if required:
            raise OpenMacroV04Error(
                f"macro series {series_id!r} is absent or empty in macro_data "
                f"through {horizon.isoformat()}; the v4 engine cannot route without "
                "it (run the macro_ingestion worker and re-check)")
        return _replay.empty_series(series_id)
    return pd.Series(
        [float(value) for _, value in rows],
        index=pd.DatetimeIndex([pd.Timestamp(obs) for obs, _ in rows]),
        dtype="float64", name=series_id)


def read_price_frame(conn, horizon: _dt.date) -> tuple[pd.DataFrame, list[tuple]]:
    """Daily adjusted closes for the seven book instruments, and the raw rows.

    Returns the frame RESTRICTED to dates where EVERY instrument has a price — the
    replay's rule, and load-bearing rather than tidy: the month-end resample takes the
    last such session, so a frame that kept SPY-only dates would give a different SPY
    month-end in any month where another instrument did not trade on SPY's last
    session, and the ledger would stop reproducing. No forward fill, no imputation.

    The raw rows are returned alongside for the input digest, which pins what was
    READ, not what survived the restriction."""
    with conn.cursor() as cur:
        cur.execute(PRICES_SQL, {"tickers": list(BOOK_TICKERS), "horizon": horizon})
        rows = cur.fetchall()
    if not rows:
        raise OpenMacroV04Error(
            f"eod_prices has no rows for {list(BOOK_TICKERS)} through "
            f"{horizon.isoformat()}; the guard's price confirmation and the "
            "allocation's priced_at both need them")
    frame = pd.DataFrame(
        {"ticker": [r[0] for r in rows],
         "date": pd.to_datetime([r[1] for r in rows]),
         "adj_close": [float(r[2]) for r in rows]})
    daily = frame.pivot(index="date", columns="ticker", values="adj_close")
    missing = [t for t in BOOK_TICKERS if t not in daily.columns]
    if missing:
        raise OpenMacroV04Error(
            f"eod_prices carries no history at all for {missing}; the v4 book cannot "
            "be priced on a session where an instrument is absent (add them to the "
            "eod_prices_warmer priority head)")
    complete = daily[list(BOOK_TICKERS)].dropna(how="any")
    if complete.empty:
        raise OpenMacroV04Error(
            "no session prices all seven book instruments at once; refusing to price "
            "a book at inconsistent dates")
    return complete, rows


def read_chain(conn, horizon: _dt.date) -> tuple[pd.DataFrame, dict, list[tuple]]:
    """The v03 decision chain: the quadrant frame the engine reads, the per-month
    ``candidate_confidence`` passthrough, and the raw rows for the digest.

    An empty SQL field is a NULL and must read back as ``None``, never as ``''``:
    the empty string satisfies ``isinstance(q, str)`` and would silently latch a
    quadrant the chain refused to emit.

    The chain may legitimately be EMPTY (a database where it was never
    materialized). That is not an error: every month then reads the pre-chain proxy,
    exactly as the replay does before 2014-03."""
    with conn.cursor() as cur:
        cur.execute(CHAIN_SQL, {"horizon": horizon})
        rows = cur.fetchall()
    index = pd.DatetimeIndex([pd.Timestamp(r[0]) for r in rows])
    frame = pd.DataFrame(
        {"chain_quadrant": [r[1] if r[1] else None for r in rows],
         "chain_status": [r[2] if r[2] else None for r in rows]},
        index=index)
    confidence = {pd.Timestamp(r[0]): (None if r[3] is None else float(r[3]))
                  for r in rows}
    return frame, confidence, rows


# --------------------------------------------------------------------------- #
# Gate 5 — the input digest
# --------------------------------------------------------------------------- #
def _digest(lines: list[str]) -> str:
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def input_digest(series: dict[str, pd.Series], chain_rows: list[tuple],
                 price_rows: list[tuple]) -> tuple[str, dict[str, str]]:
    """The canonical input digest and its eight parts.

    The recipe is the fixture manifest's, verbatim — ``obs_date|value`` at ``%.17g``
    per macro series, ``as_of|quadrant|status`` for the chain (a SQL NULL renders as
    the literal ``None``), ``ticker|date|adj_close`` at ``%.17g`` for prices — each
    part hashed over its ``\\n``-joined lines, then the eight part digests joined in
    ``CANONICAL_INPUT_ORDER`` and hashed again. ``%.17g`` round-trips float64
    exactly, so the digest is a function of the values and not of a formatter.

    ``candidate_confidence`` is deliberately OUTSIDE the digest: it is a passthrough
    label carried onto the published row, not an input the engine computes with, and
    including it would make the digest claim more coverage than it has."""
    parts: dict[str, str] = {}
    for series_id in CANONICAL_INPUT_ORDER[:-2]:
        s = series.get(series_id)
        lines = ([] if s is None else
                 [f"{d:%Y-%m-%d}|{float(v):.17g}" for d, v in s.items()])
        parts[series_id] = _digest(lines)
    parts["chain"] = _digest([
        f"{r[0]:%Y-%m-%d}|{r[1] or 'None'}|{r[2] or 'None'}" for r in chain_rows])
    parts["prices"] = _digest([
        f"{r[0]}|{r[1]:%Y-%m-%d}|{float(r[2]):.17g}" for r in price_rows])
    combined = _digest([parts[name] for name in CANONICAL_INPUT_ORDER])
    return combined, parts


# --------------------------------------------------------------------------- #
# Gate 6 — the ledger, and the tokens derived from it
# --------------------------------------------------------------------------- #
def build_worker_ledger(series: dict[str, pd.Series], chain: pd.DataFrame,
                        prices: pd.DataFrame, as_of: _dt.date) -> pd.DataFrame:
    """The ledger through the SHARED replay path, over 1959-01..``as_of``.

    The index starts at the replay's own ``INDEX_START`` and not at the publish
    window: the state machine's run-up IS part of the answer (``fiscal_state_age_m``,
    the 54-month SLOOS z window, ``severe_run_age``, the carry counter), so a shorter
    index would produce different values for the very first published months.

    Truncating the index at ``as_of`` is safe because every transform is causal —
    trailing rolling windows, backward shifts, forward state walks. The parity test
    asserts exactly this: the ledger built to 2026-05 equals the golden window
    computed inside the full 1959..2026-07 index, byte for byte."""
    index = pd.date_range(_replay.INDEX_START, pd.Timestamp(as_of), freq="ME")
    monthly = _replay.month_end_prices(prices)
    return _replay.build_ledger(series, chain, monthly[_guard.STRESS_TICKER], index,
                                extended=True)


def fiscal_boundary(deficit_gdp: float,
                    thresholds: _fiscal.FiscalThresholds = _fiscal.SIGNED_THRESHOLDS
                    ) -> bool:
    """TRUE inside the CLOSED hysteresis band ``[exit, enter]``.

    Recomputed here rather than read off the ledger because the ledger's published
    column list does not carry it — and it is a PURE function of ``deficit_gdp``,
    which the ledger does carry, evaluated against the same frozen thresholds
    ``fiscal_state.route`` uses. The worker's test asserts the two agree month for
    month over the pinned panel, so this is a restatement, not a second opinion.

    It matters because inside the band the state was CARRIED, not chosen: a book
    taken there must be auditable as such."""
    return bool(thresholds.exit <= deficit_gdp <= thresholds.enter)


def decision_validity(guard_coverage: str, fiscal_state: str,
                      quadrant_source: str) -> str:
    """The consumption token the Light reads, derived deterministically.

    The order is the order of what the reader must NOT do. A blind guard first:
    nothing downstream may treat the month as observed, whatever the quadrant says.
    Then dominance: the book is the fiscal baseline and the quadrant did not choose
    it, so calling it 'fresh' would credit a diagnostic with an allocation it never
    made. Only inside contained does the chain's own freshness decide.

    The v04 decisions DDL restates this as a CHECK, so a hand-written or drifted
    token is refused by the database and not merely by this function."""
    if guard_coverage == _guard.COVERAGE_BLIND:
        return "guard_blind"
    if fiscal_state == _fiscal.DOMINANCE:
        return "dominance_baseline"
    if quadrant_source == "chain_fresh":
        return "fresh"
    if quadrant_source == "chain_carry":
        return "carried"
    return "no_signal"


def quadrant_confidence_in_force(ledger: pd.DataFrame,
                                 confidence: dict) -> dict[pd.Timestamp, float | None]:
    """The chain's ``candidate_confidence`` for the reading IN FORCE each month.

    A carried quadrant carries its seed's confidence — that is the whole point of a
    carry, and dropping the confidence would make a carried month look like a fresh
    reading of unknown quality. Months with no chain reading (proxy, no_signal) get
    ``None``: the proxy has no confidence to report and inventing one would be the
    exact fabrication the token exists to prevent."""
    out: dict[pd.Timestamp, float | None] = {}
    last: float | None = None
    for date in ledger.index:
        source = ledger.at[date, "quadrant_source"]
        if source == "chain_fresh":
            last = confidence.get(date)
            out[date] = last
        elif source == "chain_carry":
            out[date] = last
        else:
            out[date] = None
    return out


# --------------------------------------------------------------------------- #
# Gate 7 — publish
# --------------------------------------------------------------------------- #
def valid_until(as_of: _dt.date) -> _dt.datetime:
    """The NEXT month-end after ``as_of``, at 14:00 UTC.

    That is the moment this row's successor becomes computable, so exactly one row is
    ever 'current' and a superseded row says so by itself. The 14:00 offset gives the
    daily cron a window on the first of the month to publish the successor before the
    incumbent lapses — the same horizon convention the v03 pair uses."""
    nxt = (pd.Timestamp(as_of) + pd.offsets.MonthEnd(1)).date()
    return _dt.datetime(nxt.year, nxt.month, nxt.day, 14, 0, 0,
                        tzinfo=_dt.timezone.utc)


_DECISION_COLUMNS = (
    "as_of", "fiscal_state", "fiscal_boundary", "fiscal_state_age_m", "deficit_gdp",
    "guard_level", "guard_coverage", "arm_a", "arm_b", "severe_run_age",
    "severe_degraded", "stress_confirmed", "quadrant", "quadrant_source", "carry_age",
    "quadrant_confidence", "decision_validity", "decision_basis",
    "input_digest_sha256", "formulation_sha256", "code_commit", "run_id",
    "valid_until")

# decision_basis is ABSENT from the UPDATE list ON PURPOSE: a month published live
# can never be demoted to a replay by a later recomputation, and that is enforced by
# the SQL rather than by remembering to compute it right.
_DECISION_UPDATE_COLUMNS = tuple(
    c for c in _DECISION_COLUMNS if c not in ("as_of", "decision_basis"))

_ALLOCATION_COLUMNS = (
    "as_of", "book_id", *[c for c, _ in WEIGHT_COLUMNS], "priced_at",
    "input_digest_sha256", "formulation_sha256", "code_commit", "run_id",
    "valid_until")
_ALLOCATION_UPDATE_COLUMNS = tuple(c for c in _ALLOCATION_COLUMNS if c != "as_of")


def _upsert_sql(table: str, columns: tuple[str, ...],
                update_columns: tuple[str, ...]) -> str:
    """An upsert that NEVER resurrects an invalidated row.

    The ``WHERE ... valid_status <> 'invalidated'`` tail is the v03 idiom: a row an
    operator killed stays killed, and the caller sees rowcount 0 rather than a silent
    revival."""
    names = ", ".join(columns)
    placeholders = ", ".join(f"%({c})s" for c in columns)
    assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
    return (
        f"INSERT INTO {table} ({names}, publish_state, valid_status) "
        f"VALUES ({placeholders}, 'published', 'valid') "
        f"ON CONFLICT (as_of) DO UPDATE SET {assignments}, "
        " publish_state = 'published', valid_status = 'valid', "
        " invalidated_at = NULL, invalidated_reason = NULL, updated_at = now() "
        f"WHERE {table}.valid_status <> 'invalidated'")


DECISION_UPSERT_SQL = _upsert_sql(DECISIONS_TABLE, _DECISION_COLUMNS,
                                  _DECISION_UPDATE_COLUMNS)
ALLOCATION_UPSERT_SQL = _upsert_sql(ALLOCATIONS_TABLE, _ALLOCATION_COLUMNS,
                                    _ALLOCATION_UPDATE_COLUMNS)

EXISTING_BASIS_SQL = (
    f"SELECT as_of, decision_basis FROM {DECISIONS_TABLE} WHERE as_of >= %(start)s")


def _exact_numeric(value: Any) -> Any:
    """Python float -> ``decimal.Decimal(repr(value))`` for EXACT NUMERIC persistence.

    Postgres casts a float8 parameter to NUMERIC through 15 significant digits, so a
    raw Python float silently truncates the stored value (measured in production
    2026-07-06 on the v03 pair: a recomputed 0.8121545618518331 against a stored
    0.812154561851833). ``repr`` is the SHORTEST round-tripping decimal string, so
    ``float(Decimal(repr(x))) == x`` exactly. Non-floats pass through untouched."""
    if isinstance(value, float):
        return decimal.Decimal(repr(value))
    return value


def _exact_numeric_params(params: dict[str, Any]) -> dict[str, Any]:
    return {key: _exact_numeric(value) for key, value in params.items()}


def publish(conn, decision_rows: list[dict[str, Any]],
            allocation_rows: list[dict[str, Any]],
            latest_as_of: _dt.date) -> dict[str, int]:
    """Upsert every month's decision + allocation in ONE transaction.

    An invalidated row is SKIPPED, not resurrected, and the skip is counted and
    reported. It is only FATAL for ``latest_as_of``: refusing to publish the current
    decision is a real outage, while a single killed historical month must not wedge
    the monthly publisher forever."""
    skipped: set = set()
    with conn.cursor() as cur:
        for row in decision_rows:
            cur.execute(DECISION_UPSERT_SQL, _exact_numeric_params(row))
            if cur.rowcount == 0:
                skipped.add(row["as_of"])
                if row["as_of"] == latest_as_of:
                    raise OpenMacroV04Error(
                        f"decision upsert for the CURRENT month {latest_as_of} did "
                        "not apply: the row is invalidated. A re-run cannot resurrect "
                        "it; resolve the invalidation explicitly")
        for row in allocation_rows:
            if row["as_of"] in skipped:
                continue    # the FK parent was skipped; the pair stays consistent
            cur.execute(ALLOCATION_UPSERT_SQL, _exact_numeric_params(row))
            if cur.rowcount == 0 and row["as_of"] == latest_as_of:
                raise OpenMacroV04Error(
                    f"allocation upsert for the CURRENT month {latest_as_of} did not "
                    "apply: the row is invalidated")
    conn.commit()
    return {"n_applied": len(decision_rows) - len(skipped),
            "n_skipped_invalidated": len(skipped),
            "skipped_as_of": sorted(d.isoformat() for d in skipped)}


# --------------------------------------------------------------------------- #
# Gate 9 — post-write verification
# --------------------------------------------------------------------------- #
COUNT_SQL = ("SELECT count(*) FROM {table} "
             "WHERE as_of >= %(start)s AND valid_status = 'valid'")
READBACK_DECISION_SQL = (
    "SELECT " + ", ".join(_DECISION_COLUMNS) + ", publish_state, valid_status "
    f"FROM {DECISIONS_TABLE} WHERE as_of = %(as_of)s")
READBACK_ALLOCATION_SQL = (
    "SELECT " + ", ".join(_ALLOCATION_COLUMNS) + ", publish_state, valid_status "
    f"FROM {ALLOCATIONS_TABLE} WHERE as_of = %(as_of)s")


def _equal(read: Any, computed: Any) -> bool:
    """Compare a re-read value to the computed one across the driver's type shifts.

    NUMERIC comes back as Decimal and DATE as date; a float compares exactly because
    the write went through ``_exact_numeric`` — an inexact match here is a real
    corruption, not a rounding artefact, so the tolerance is ZERO."""
    if computed is None or read is None:
        return computed is None and read is None
    if isinstance(computed, float):
        return float(read) == computed
    if isinstance(computed, _dt.datetime):
        return read == computed
    return read == computed


def post_write_verify(conn, expected_count: int, latest_as_of: _dt.date,
                      decision_row: dict[str, Any],
                      allocation_row: dict[str, Any]) -> dict[str, int]:
    """Re-read in a fresh transaction: the counts, and the latest row FIELD BY FIELD.

    Counting alone would pass a run that wrote the right number of wrong rows; the
    latest row is the one the Light will consume, so every published column of it is
    compared against what was computed."""
    with conn.cursor() as cur:
        counts: dict[str, int] = {}
        for table in (DECISIONS_TABLE, ALLOCATIONS_TABLE):
            cur.execute(COUNT_SQL.format(table=table), {"start": PUBLISH_START})
            counts[table] = int(cur.fetchone()[0])
        cur.execute(READBACK_DECISION_SQL, {"as_of": latest_as_of})
        decision_back = cur.fetchone()
        cur.execute(READBACK_ALLOCATION_SQL, {"as_of": latest_as_of})
        allocation_back = cur.fetchone()
    conn.commit()

    problems: list[str] = []
    for table, count in counts.items():
        if count != expected_count:
            problems.append(f"{table}: {count} valid rows from {PUBLISH_START} != "
                            f"{expected_count} published")
    for label, back, row, columns in (
            ("decision", decision_back, decision_row, _DECISION_COLUMNS),
            ("allocation", allocation_back, allocation_row, _ALLOCATION_COLUMNS)):
        if back is None:
            problems.append(f"{label} row for {latest_as_of} absent after publish")
            continue
        if (back[-2], back[-1]) != ("published", "valid"):
            problems.append(f"{label} row is {back[-2]}/{back[-1]}, not published/valid")
        for i, column in enumerate(columns):
            if column == "decision_basis":
                continue    # deliberately never rewritten; see _DECISION_UPDATE_COLUMNS
            if not _equal(back[i], row[column]):
                problems.append(
                    f"{label}.{column} reread {back[i]!r} != computed {row[column]!r}")
    if problems:
        raise OpenMacroV04Error(
            "post-write verification failed: " + "; ".join(problems))
    return counts


# --------------------------------------------------------------------------- #
# Provenance
# --------------------------------------------------------------------------- #
def code_commit() -> str:
    sha = os.environ.get("RAILWAY_GIT_COMMIT_SHA")
    if sha:
        return sha.strip()
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                          capture_output=True, text=True, check=True).stdout.strip()


def ledger_csv(ledger: pd.DataFrame) -> str:
    """The ledger in the golden's exact encoding — the operator's diff tool.

    Delegates to the replay's serializer, so a live run's ledger and the pinned
    fixture are comparable with ``diff`` rather than by eye."""
    return _replay.ledger_to_csv(ledger)


def publishable_index(ledger: pd.DataFrame, latest: _dt.date) -> pd.DatetimeIndex:
    """The month-ends inside the publish window, computable or not."""
    return ledger.loc[pd.Timestamp(PUBLISH_START):pd.Timestamp(latest)].index


# --------------------------------------------------------------------------- #
# run()
# --------------------------------------------------------------------------- #
def run(dsn: str, *, as_of: str | None = None,
        today: _dt.date | None = None) -> dict[str, Any]:
    """Publish every computable month-end of the v4 engine. See the module docstring
    for the fail-loud gate ordering; no side effect precedes any gate."""
    t0 = time.monotonic()
    if today is None:
        from zoneinfo import ZoneInfo
        today = _dt.datetime.now(ZoneInfo("America/New_York")).date()

    # Gate 1 — the frozen formulation (NO DB).
    formulation_sha256 = verify_formulation_freeze()

    # Gate 2 — connect + search_path + advisory lock.
    conn = connect(dsn)
    try:
        pin_search_path(conn)
        with advisory_lock(conn, LOCK_OPEN_MACRO_V04) as got:
            if not got:
                return {"status": "lock_busy"}

            # Gate 3 — READ-ONLY catalog verification.
            verify_schema(conn)

            latest = resolve_as_of(as_of, today=today)

            # Gate 4 — inputs, each series by explicit id.
            series = {sid: read_macro_series(conn, sid, latest, required=True)
                      for sid in REQUIRED_SERIES}
            series.update({sid: read_macro_series(conn, sid, latest, required=False)
                           for sid in PROXY_SERIES})
            prices, price_rows = read_price_frame(conn, latest)
            chain, chain_confidence, chain_rows = read_chain(conn, latest)

            # Gate 5 — the input digest.
            digest, digest_parts = input_digest(series, chain_rows, price_rows)

            # Gate 6 — the ledger, through the shared replay path.
            ledger = build_worker_ledger(series, chain, prices, latest)
            confidence = quadrant_confidence_in_force(ledger, chain_confidence)

            # Gate 8 (evaluated BEFORE any write, so a stale input publishes nothing
            # rather than publishing a short table and then raising).
            latest_ts = pd.Timestamp(latest)
            if latest_ts not in ledger.index or ledger.at[latest_ts, "fiscal_state"] is None:
                raise OpenMacroV04Error(
                    f"deficit_gdp is not computable at the latest month-end {latest}: "
                    f"the rolling 12-month {_fiscal.MTS_SERIES_ID} sum or the PIT "
                    f"{_fiscal.GDP_SERIES_ID} reading is missing there. The fiscal "
                    "router abstains, so there is no state and no book — refusing to "
                    "publish anything rather than publishing a table that silently "
                    "stops one month short")
            # Coverage AT THE RUN DATE, alongside the published month-end verdict.
            # They answer different questions and both are needed: the ROW carries the
            # PIT verdict at its own month-end (that is what the decision was made
            # under, and changing it would break reproduction), while this one is the
            # operational reading — "is the mirror publishing today?" — which is what
            # an operator has to act on. Reporting only the row's would hide an arm
            # that went dark after the month closed.
            run_moment = pd.Timestamp(today)
            live_freshness = {
                "evaluated_at": today.isoformat(),
                "arm_a": _guard.arm_freshness(
                    series[_guard.SLOOS_SERIES_ID].index, run_moment,
                    _guard.SLOOS_CLOCK).as_dict(),
                "arm_b": _guard.arm_freshness(
                    series[_guard.M2_SERIES_ID].index, run_moment,
                    _guard.M2_CLOCK).as_dict(),
            }

            commit = code_commit()
            run_id = f"open_macro_v04-{latest.isoformat()}-{uuid.uuid4().hex[:8]}"

            # Which months already exist (and with which basis)? A month that exists
            # can never be relabelled 'live' — it was not lived through THIS run.
            with conn.cursor() as cur:
                cur.execute(EXISTING_BASIS_SQL, {"start": PUBLISH_START})
                existing = {r[0] for r in cur.fetchall()}

            decision_rows, allocation_rows = build_rows(
                ledger, confidence, latest, existing,
                digest=digest, formulation_sha256=formulation_sha256,
                commit=commit, run_id=run_id, prices=prices)

            # Gate 7 — publish (atomic).
            published = publish(conn, decision_rows, allocation_rows, latest)

            # Gate 9 — post-write verification.
            post_write_verify(conn, published["n_applied"], latest,
                              decision_rows[-1], allocation_rows[-1])

            last = decision_rows[-1]
            return {
                "status": "published",
                "n_published": published["n_applied"],
                "n_bootstrap": sum(1 for r in decision_rows
                                   if r["decision_basis"] == BASIS_BOOTSTRAP),
                "n_live": sum(1 for r in decision_rows
                              if r["decision_basis"] == BASIS_LIVE),
                "n_skipped_invalidated": published["n_skipped_invalidated"],
                "skipped_as_of": published["skipped_as_of"],
                "n_months_without_state": int(
                    len(publishable_index(ledger, latest)) - len(decision_rows)),
                "latest_as_of": latest.isoformat(),
                "fiscal_state": last["fiscal_state"],
                "guard_level": last["guard_level"],
                "guard_coverage": last["guard_coverage"],
                "decision_validity": last["decision_validity"],
                "decision_basis": last["decision_basis"],
                "book_id": allocation_rows[-1]["book_id"],
                "input_digest": digest,
                "input_digest_parts": digest_parts,
                "formulation_sha": formulation_sha256,
                "live_freshness": live_freshness,
                "run_id": run_id,
                "wall_ms": int((time.monotonic() - t0) * 1000),
            }
    finally:
        conn.close()


def build_rows(ledger: pd.DataFrame, confidence: dict, latest: _dt.date,
               existing: set, *, digest: str, formulation_sha256: str,
               commit: str, run_id: str,
               prices: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """Turn the ledger into the two row lists, month by month.

    A month with NO fiscal state is skipped, not faked: the router abstained, there
    is no book, and ``fiscal_state`` is NOT NULL for exactly that reason. In
    production the macro mirror is a rolling window rather than the replay's full
    history, so the early months of the publish window can legitimately have no
    reading — the count of skipped months is reported in the stats so the absence is
    visible rather than silent."""
    decisions: list[dict] = []
    allocations: list[dict] = []
    session_dates = prices.index
    for date in publishable_index(ledger, latest):
        state = ledger.at[date, "fiscal_state"]
        if not isinstance(state, str):
            continue
        quadrant = ledger.at[date, "quadrant"]
        quadrant = quadrant if isinstance(quadrant, str) else None
        source = str(ledger.at[date, "quadrant_source"])
        coverage = str(ledger.at[date, "guard_coverage"])
        as_of_date = date.date()
        # 'live' is the last complete month-end AND a row that did not exist before.
        # A month already on the table was not lived through THIS run, whatever it
        # says; the upsert never rewrites the column, so an existing 'live' stands.
        basis = (BASIS_LIVE if (as_of_date == latest and as_of_date not in existing)
                 else BASIS_BOOTSTRAP)
        decisions.append({
            "as_of": as_of_date,
            "fiscal_state": state,
            "fiscal_boundary": fiscal_boundary(float(ledger.at[date, "deficit_gdp"])),
            "fiscal_state_age_m": int(ledger.at[date, "fiscal_state_age_m"]),
            "deficit_gdp": float(ledger.at[date, "deficit_gdp"]),
            "guard_level": str(ledger.at[date, "guard_level"]),
            "guard_coverage": coverage,
            "arm_a": bool(ledger.at[date, "arm_a"]),
            "arm_b": bool(ledger.at[date, "arm_b"]),
            "severe_run_age": int(ledger.at[date, "severe_run_age"]),
            "severe_degraded": bool(ledger.at[date, "severe_degraded"]),
            "stress_confirmed": bool(ledger.at[date, "stress_confirmed"]),
            "quadrant": quadrant,
            "quadrant_source": source,
            "carry_age": int(ledger.at[date, "carry_age"]),
            "quadrant_confidence": confidence.get(date),
            "decision_validity": decision_validity(coverage, state, source),
            "decision_basis": basis,
            "input_digest_sha256": digest,
            "formulation_sha256": formulation_sha256,
            "code_commit": commit,
            "run_id": run_id,
            "valid_until": valid_until(as_of_date),
        })
        priced = session_dates[session_dates <= date]
        if len(priced) == 0:
            raise OpenMacroV04Error(
                f"no session prices all seven book instruments at or before "
                f"{as_of_date}; the allocation has no priced_at to stand on")
        allocations.append({
            "as_of": as_of_date,
            "book_id": str(ledger.at[date, "book_id"]),
            **{column: float(ledger.at[date, ticker])
               for column, ticker in WEIGHT_COLUMNS},
            "priced_at": priced[-1].date(),
            "input_digest_sha256": digest,
            "formulation_sha256": formulation_sha256,
            "code_commit": commit,
            "run_id": run_id,
            "valid_until": valid_until(as_of_date),
        })
    if not decisions:
        raise OpenMacroV04Error(
            f"no month between {PUBLISH_START} and {latest} has a fiscal state; the "
            f"{_fiscal.MTS_SERIES_ID}/{_fiscal.GDP_SERIES_ID} history in macro_data "
            "is too short for the rolling 12-month deficit")
    # HYG is structurally zero and has no column; assert it rather than assume it.
    for date in publishable_index(ledger, latest):
        hyg = ledger.at[date, "HYG"]
        if isinstance(hyg, float) and hyg == hyg and hyg != 0.0:
            raise OpenMacroV04Error(
                f"the book at {date:%Y-%m-%d} carries HYG {hyg!r}; the v04 "
                "allocations schema has no w_hyg column because no emittable book "
                "can. Add the column and re-derive the weight-sum constraint before "
                "publishing a book that holds it")
    return decisions, allocations


def main(argv: list[str] | None = None) -> int:
    import argparse
    from src.db import resolve_dsn
    parser = argparse.ArgumentParser(prog="python -m src.workers.open_macro_v04")
    parser.add_argument("--as-of", dest="as_of", default=None,
                        help="month-end to publish through (default: the last "
                             "complete month-end)")
    args = parser.parse_args(argv)
    print(json.dumps(run(resolve_dsn(), as_of=args.as_of), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["run", "main", "OpenMacroV04Error", "verify_formulation_freeze",
           "check_universe_coverage",
           "verify_schema", "build_worker_ledger", "build_rows", "decision_validity",
           "fiscal_boundary", "input_digest", "ledger_csv", "publish",
           "post_write_verify", "quadrant_confidence_in_force", "resolve_as_of",
           "last_complete_month_end", "valid_until", "EXPECTED_COLUMNS",
           "BOOK_TICKERS", "FORMULA_MODULES"]
