"""open_macro_v03 runtime worker — Stage B direct activation (regime decision + allocation).

Daily job that reconstitutes the latched global decision chain over the certified
pack v2 prefix PLUS a pinned live delta, computes today's consumable regime decision
and its ``compressed_50`` allocation (the product), and publishes BOTH rows to
``open_macro_v03_decisions`` / ``open_macro_v03_allocations`` atomically. When inputs
breach the staleness SLO it refuses to publish and writes a durable row to the
``open_macro_v03_staleness_blocks`` ledger instead — a POSITIVE, replayable record.

Fail-loud ordering (zero side effects before every gate):

  1. B2 feature flag (``open_macro_v03_runtime_activation``) — absent/!=true ⇒ exit
     ``flag_off`` WITHOUT connecting to the DB.
  2. Governance envelope — every activation gate must hold ⇒ else ``governance_blocked``
     WITHOUT connecting. (The committed envelope ships FULLY BLOCKED; the flips land
     only in the Stage B PR's final review, so a real run today returns governance_blocked.)
  3. Module pins — sha256 (CRLF→LF normalized) of each pinned pure module must match
     ⇒ else raise, BEFORE any DB access.
  4. connect + a dedicated advisory lock (at most one run per business day).
  5. ensure_schema (the three committed DDL files).
  6. resolve as_of (env override, else the current America/New_York business day).
  7. read inputs: pack v2 prefix (hash-compared to the pack pin — a pre-cut backfill
     can never silently shift the input basis) + pinned live delta, composed fail-loud.
  8. staleness gate — BREACH ⇒ ledger row + exit ``staleness_block`` (no output rows).
  9. latched decision chain + consumable position (fresh iff decided on as_of).
 10. compressed_50 allocation with the risk-cap / defensive-floor gates.
 11. PUBLISH decision + allocation in ONE transaction (re-run never resurrects an
     invalidated row).
 12. post-write verify (mismatch ⇒ stamp both rows invalidated + raise).

The ``invalidate`` CLI (``python -m src.workers.open_macro_v03 invalidate ...``) is the
manual kill switch; it is NEVER called by ``run()`` and never touches the ledger.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

from src.db import (
    LOCK_OPEN_MACRO_V03,
    advisory_lock,
    connect,
    resolve_dsn,
)
from src.quadrant_staleness import add_business_days
from harness.direct_activation.live_validation import (
    CHAIN_START,
    PACK,
    PACK_SHA256_PIN,
    _VINTAGE_KEY,
    compose_rows,
    consumable_today,
    staleness_report,
)
from harness.phase0q import decision as decision_mod
from harness.phase0q import sleeve as sleeve_mod
from scripts.p1_export.export_p1_sources import (
    EOD_MIN_DATE,
    EOD_PRICES_SQL,
    MACRO_VINTAGE_SQL,
    SLEEVE_TICKERS,
    _as_of_end_utc,
    _canonical_json,
    format_eod_price_rows,
    format_macro_vintage_rows,
    seed_series_ids,
)

ROOT = Path(__file__).resolve().parents[2]
STAGE_B_DIR = ROOT / "artifacts" / "a5" / "open_macro_v03_direct_activation_stage_b_001"
ENVELOPE_PATH = STAGE_B_DIR / "activation_envelope.json"
PINS_PATH = STAGE_B_DIR / "module_pins.json"

_SCHEMAS = (
    "schemas/open_macro_v03_decisions.sql",
    "schemas/open_macro_v03_allocations.sql",
    "schemas/open_macro_v03_staleness_blocks.sql",
)

ALLOWED_TABLES = frozenset({
    "open_macro_v03_decisions",
    "open_macro_v03_allocations",
    "open_macro_v03_staleness_blocks",
})

# The certified pack v2 cut date: the prefix boundary between the pinned committed
# pack and the live delta the worker reads.
PACK_CUT = _dt.date(2026, 6, 30)

RUNTIME_FLAG_ENV = "open_macro_v03_runtime_activation"
AS_OF_ENV = "OPEN_MACRO_V03_AS_OF"

CANDIDATE_ID = "open_macro_v03_compressed_50"
BOOK = "compressed_50"
JUDGMENT_REF = "open_macro_v03_dark_launch_readiness_001:go_candidate"
THRESHOLD_REF = "open_macro_v03_threshold_signoff_001"

DB_WRITE_MODE = "open_macro_v03_new_tables_only"


class OpenMacroV03Error(RuntimeError):
    """A Stage B runtime gate did not hold; the run must fail loud."""


# --------------------------------------------------------------------------- #
# Delta SQL (the pack prefix is read with the export SQL unchanged, so its hash
# reproduces the certified export; the delta reads > cut and <= as_of).
# --------------------------------------------------------------------------- #
DELTA_MACRO_SQL = (
    "SELECT series_id, observation_period, vintage_date, value, available_at,\n"
    "       revision_number, source, source_spec_version\n"
    "FROM macro_observation_vintage\n"
    "WHERE series_id = ANY(%(series_ids)s)\n"
    "  AND available_at > %(cut_end)s\n"
    "  AND available_at <= %(as_of_end)s\n"
    "ORDER BY series_id, observation_period, vintage_date"
)

DELTA_EOD_SQL = (
    "SELECT ticker, date, close, adj_close AS adjusted_close, volume\n"
    "FROM eod_prices\n"
    "WHERE ticker = ANY(%(tickers)s)\n"
    "  AND date > %(cut)s\n"
    "  AND date <= %(as_of)s\n"
    "ORDER BY ticker, date"
)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_norm(path: Path) -> str:
    """sha256 of file bytes with CRLF→LF normalization (git-checkout agnostic)."""
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def _canonical_sha256(rows: list[dict]) -> str:
    """sha256 of the canonical (export-format) serialization of a row list."""
    return hashlib.sha256(_canonical_json(rows).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Gate 2 — governance envelope
# --------------------------------------------------------------------------- #
def check_governance(envelope: dict[str, Any]) -> str | None:
    """Return ``None`` when EVERY activation gate holds, else a short reason string.

    Booleans are checked with strict identity (``is True``) so a string ``"true"``
    can never spoof a flip. All gates must be satisfied together."""
    def _true(key: str) -> bool:
        return envelope.get(key) is True

    if not _true("runtime_activation"):
        return "runtime_activation!=true"
    if not _true("activation_allowed"):
        return "activation_allowed!=true"
    if not _true("allow_db_write"):
        return "allow_db_write!=true"
    if not _true("db_write_official"):
        return "db_write_official!=true"
    if envelope.get("db_write_mode") != DB_WRITE_MODE:
        return f"db_write_mode!={DB_WRITE_MODE}"
    if not _true("allocator_publish"):
        return "allocator_publish!=true"
    if not _true("allow_allocator_publish"):
        return "allow_allocator_publish!=true"
    if not _true("official_result"):
        return "official_result!=true"
    if envelope.get("A5") != "active":
        return "A5!=active"
    allowed = envelope.get("allowed_tables")
    if not isinstance(allowed, list) or set(allowed) != set(ALLOWED_TABLES):
        return "allowed_tables!=the three sanctioned tables"
    environment = envelope.get("environment")
    if not isinstance(environment, dict) or not environment.get("railway_service_name"):
        return "environment.railway_service_name missing"
    return None


# --------------------------------------------------------------------------- #
# Gate 3 — module pins
# --------------------------------------------------------------------------- #
def verify_module_pins(pins: dict[str, Any], root: Path = ROOT) -> None:
    """Raise unless the sha256 (CRLF→LF) of EACH pinned module matches its pin."""
    modules = pins["modules"]
    for rel, expected in sorted(modules.items()):
        actual = _sha256_norm(root / rel)
        if actual != expected:
            raise OpenMacroV03Error(
                f"module pin mismatch for {rel}: {actual} != pinned {expected}")


# --------------------------------------------------------------------------- #
# Gate 6 — as_of resolution
# --------------------------------------------------------------------------- #
def resolve_as_of(as_of_arg: str | None = None, *,
                  today: _dt.date | None = None) -> _dt.date | None:
    """Explicit override (arg or env) is trusted; otherwise the current
    America/New_York calendar day, or ``None`` on a weekend (non-business day)."""
    if as_of_arg:
        return _dt.date.fromisoformat(as_of_arg)
    env = os.environ.get(AS_OF_ENV)
    if env:
        return _dt.date.fromisoformat(env)
    if today is None:
        from zoneinfo import ZoneInfo
        today = _dt.datetime.now(ZoneInfo("America/New_York")).date()
    if today.weekday() >= 5:  # Sat/Sun
        return None
    return today


# --------------------------------------------------------------------------- #
# Gate 7 — inputs
# --------------------------------------------------------------------------- #
def prefix_pins() -> dict[str, str]:
    """Per-table pins of the certified pack v2 prefix (pack SOURCE.json p1_export)."""
    source = _load_json(PACK / "SOURCE.json")
    return {t["table"]: t["sha256"] for t in source["p1_export"]["tables"]}


def read_prefix(conn) -> tuple[list[dict], list[dict]]:
    """The pre-cut live prefix read with the EXACT export SQL/params, so its canonical
    serialization reproduces the certified export byte-for-byte."""
    cut_end = _as_of_end_utc(PACK_CUT)
    with conn.cursor() as cur:
        cur.execute(MACRO_VINTAGE_SQL,
                    {"series_ids": list(seed_series_ids()), "as_of_end": cut_end})
        macro_rows = format_macro_vintage_rows(cur.fetchall())
    with conn.cursor() as cur:
        cur.execute(EOD_PRICES_SQL,
                    {"tickers": list(SLEEVE_TICKERS), "min_date": EOD_MIN_DATE,
                     "as_of": PACK_CUT.isoformat()})
        eod_rows = format_eod_price_rows(cur.fetchall())
    return macro_rows, eod_rows


def verify_prefix_hashes(macro_rows: list[dict], eod_rows: list[dict],
                         pins: dict[str, str]) -> None:
    """Fail loud unless the live pre-cut prefix matches the pack pin exactly."""
    macro_sha = _canonical_sha256(macro_rows)
    eod_sha = _canonical_sha256(eod_rows)
    if macro_sha != pins.get("macro_observation_vintage"):
        raise OpenMacroV03Error(
            f"pre-cut vintage prefix hash {macro_sha} != pack pin "
            f"{pins.get('macro_observation_vintage')} (a pre-cut backfill/correction "
            "would silently shift the certified input basis)")
    if eod_sha != pins.get("eod_prices"):
        raise OpenMacroV03Error(
            f"pre-cut price prefix hash {eod_sha} != pack pin {pins.get('eod_prices')}")


def read_delta(conn, as_of: _dt.date) -> tuple[list[dict], list[dict]]:
    """The live delta since the pack cut (> cut, <= as_of), same format helpers.

    Window enforcement is layered: the SQL itself bounds the window
    (``available_at > cut_end AND available_at <= as_of_end`` / ``date > cut AND
    date <= as_of``), and the row-level gate below re-asserts it on the FORMATTED
    rows — the worker-side analog of Stage A's ``vintage_delta_window_gate`` — so a
    driver/typing regression or a future SQL edit can never merge a pre-cut vintage
    (which would silently restate certified pack PIT history past the prefix-hash
    check) or a post-as-of row."""
    cut_end = _as_of_end_utc(PACK_CUT)
    as_of_end = _as_of_end_utc(as_of)
    with conn.cursor() as cur:
        cur.execute(DELTA_MACRO_SQL,
                    {"series_ids": list(seed_series_ids()), "cut_end": cut_end,
                     "as_of_end": as_of_end})
        macro_delta = format_macro_vintage_rows(cur.fetchall())
    with conn.cursor() as cur:
        cur.execute(DELTA_EOD_SQL,
                    {"tickers": list(SLEEVE_TICKERS), "cut": PACK_CUT.isoformat(),
                     "as_of": as_of.isoformat()})
        eod_delta = format_eod_price_rows(cur.fetchall())

    # row-level delta window gate (defense in depth over the SQL bounds)
    lower_exclusive = _dt.datetime.fromisoformat(cut_end)
    upper_inclusive = _dt.datetime.fromisoformat(as_of_end)
    for row in macro_delta:
        parsed = _dt.datetime.fromisoformat(row["available_at"])
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_dt.timezone.utc)
        if not (lower_exclusive < parsed <= upper_inclusive):
            raise OpenMacroV03Error(
                f"vintage delta window: {row.get('series_id')} available_at "
                f"{row['available_at']} is outside (PACK_CUT {PACK_CUT}, as_of {as_of}]; "
                "a pre-cut/post-as-of delta vintage cannot silently restate pack history")
    for row in eod_delta:
        if not (PACK_CUT.isoformat() < row["date"] <= as_of.isoformat()):
            raise OpenMacroV03Error(
                f"price delta window: {row.get('ticker')} date {row['date']} is outside "
                f"(PACK_CUT {PACK_CUT}, as_of {as_of}]")
    return macro_delta, eod_delta


def compose_inputs(conn, as_of: _dt.date) -> tuple[list[dict], list[dict]]:
    """Pack v2 prefix (disk) + hash-verified live prefix + composed live delta."""
    pack_manifest = _load_json(PACK / "manifest.json")
    if pack_manifest["input_pack_sha256"] != PACK_SHA256_PIN:
        raise OpenMacroV03Error("pack v2 sha diverged from the signed pin")

    pack_vintages = _load_json(PACK / "data" / "canonical" / "macro_observation_vintage.json")
    pack_prices = _load_json(PACK / "data" / "canonical" / "eod_prices.json")

    prefix_v, prefix_p = read_prefix(conn)
    verify_prefix_hashes(prefix_v, prefix_p, prefix_pins())

    delta_v, delta_p = read_delta(conn, as_of)
    vintage_rows = compose_rows(pack_vintages, delta_v, _VINTAGE_KEY, what="vintages")
    price_rows = compose_rows(pack_prices, delta_p, ("ticker", "date"), what="prices")
    return vintage_rows, price_rows


# --------------------------------------------------------------------------- #
# Gate 10 — allocation
# --------------------------------------------------------------------------- #
def build_allocation(quadrant: str, price_rows: list[dict]) -> dict[str, Any]:
    """Today's consumable compressed_50 target with the risk-cap/defensive-floor gates."""
    prices = sleeve_mod.PriceFrame(price_rows)
    if not prices.dates:
        raise OpenMacroV03Error("no priced sessions for the sleeve")
    priced_at = max(prices.dates)
    available: list[str] = []
    for ticker in sleeve_mod.SLEEVE_TICKERS:
        price = prices.price(ticker, priced_at)
        if not (price is not None and price == price and price > 0):
            raise OpenMacroV03Error(
                f"price gate: {ticker} has no usable price at {priced_at}")
        available.append(ticker)
    weights = sleeve_mod.target_weights(
        quadrant, sleeve_mod.SleeveParams(candidate_id=CANDIDATE_ID),
        available, compressed=True)
    total = sum(weights.values())
    if abs(total - 1.0) >= 1e-9:
        raise OpenMacroV03Error(f"weights do not sum to 1: {total}")
    risk = weights.get("SPY", 0.0) + weights.get("DBC", 0.0)
    defensive = weights.get("TLT", 0.0) + weights.get("SHY", 0.0) + weights.get("TIP", 0.0)
    if risk > sleeve_mod.RISK_CAP_BASELINE + 1e-9:
        raise OpenMacroV03Error(f"risk cap breached: {risk}")
    if defensive < sleeve_mod.DEFENSIVE_FLOOR_BASELINE - 1e-9:
        raise OpenMacroV03Error(f"defensive floor breached: {defensive}")
    return {
        "weights": {t: float(weights.get(t, 0.0)) for t in sleeve_mod.SLEEVE_TICKERS},
        "risk_assets_weight": float(risk),
        "defensive_assets_weight": float(defensive),
        "priced_at": priced_at,
    }


# --------------------------------------------------------------------------- #
# Gate 11 — valid_until + publish
# --------------------------------------------------------------------------- #
def valid_until(as_of: _dt.date) -> _dt.datetime:
    """Next business day at 14:00 UTC (the reader's freshness horizon)."""
    base = _dt.datetime(as_of.year, as_of.month, as_of.day, tzinfo=_dt.timezone.utc)
    nb = add_business_days(base, 1)
    return nb.replace(hour=14, minute=0, second=0, microsecond=0)


_DECISION_UPSERT_SQL = (
    "INSERT INTO open_macro_v03_decisions "
    "(as_of, quadrant, decision_validity, carry_seed_as_of, candidate_confidence, "
    " coverage_quality, growth_score, inflation_score, input_vintage_sha256, "
    " input_prices_sha256, pack_v2_sha256, module_pins_sha256, judgment_ref, "
    " threshold_ref, code_commit, run_id, publish_state, valid_status, valid_until) "
    "VALUES (%(as_of)s, %(quadrant)s, %(decision_validity)s, %(carry_seed_as_of)s, "
    " %(candidate_confidence)s, %(coverage_quality)s, %(growth_score)s, "
    " %(inflation_score)s, %(input_vintage_sha256)s, %(input_prices_sha256)s, "
    " %(pack_v2_sha256)s, %(module_pins_sha256)s, %(judgment_ref)s, %(threshold_ref)s, "
    " %(code_commit)s, %(run_id)s, 'published', 'valid', %(valid_until)s) "
    "ON CONFLICT (as_of) DO UPDATE SET "
    " quadrant = EXCLUDED.quadrant, decision_validity = EXCLUDED.decision_validity, "
    " carry_seed_as_of = EXCLUDED.carry_seed_as_of, "
    " candidate_confidence = EXCLUDED.candidate_confidence, "
    " coverage_quality = EXCLUDED.coverage_quality, growth_score = EXCLUDED.growth_score, "
    " inflation_score = EXCLUDED.inflation_score, "
    " input_vintage_sha256 = EXCLUDED.input_vintage_sha256, "
    " input_prices_sha256 = EXCLUDED.input_prices_sha256, "
    " pack_v2_sha256 = EXCLUDED.pack_v2_sha256, "
    " module_pins_sha256 = EXCLUDED.module_pins_sha256, "
    " judgment_ref = EXCLUDED.judgment_ref, threshold_ref = EXCLUDED.threshold_ref, "
    " code_commit = EXCLUDED.code_commit, run_id = EXCLUDED.run_id, "
    " publish_state = 'published', valid_status = 'valid', "
    " valid_until = EXCLUDED.valid_until, invalidated_at = NULL, "
    " invalidated_reason = NULL, updated_at = now() "
    "WHERE open_macro_v03_decisions.valid_status <> 'invalidated'"
)

_ALLOCATION_UPSERT_SQL = (
    "INSERT INTO open_macro_v03_allocations "
    "(as_of, book, w_spy, w_tlt, w_tip, w_gld, w_dbc, w_shy, risk_assets_weight, "
    " defensive_assets_weight, risk_cap, defensive_floor, priced_at, "
    " input_prices_sha256, pack_v2_sha256, module_pins_sha256, code_commit, run_id, "
    " publish_state, valid_status, valid_until) "
    "VALUES (%(as_of)s, %(book)s, %(w_spy)s, %(w_tlt)s, %(w_tip)s, %(w_gld)s, "
    " %(w_dbc)s, %(w_shy)s, %(risk_assets_weight)s, %(defensive_assets_weight)s, "
    " %(risk_cap)s, %(defensive_floor)s, %(priced_at)s, %(input_prices_sha256)s, "
    " %(pack_v2_sha256)s, %(module_pins_sha256)s, %(code_commit)s, %(run_id)s, "
    " 'published', 'valid', %(valid_until)s) "
    "ON CONFLICT (as_of) DO UPDATE SET "
    " book = EXCLUDED.book, w_spy = EXCLUDED.w_spy, w_tlt = EXCLUDED.w_tlt, "
    " w_tip = EXCLUDED.w_tip, w_gld = EXCLUDED.w_gld, w_dbc = EXCLUDED.w_dbc, "
    " w_shy = EXCLUDED.w_shy, risk_assets_weight = EXCLUDED.risk_assets_weight, "
    " defensive_assets_weight = EXCLUDED.defensive_assets_weight, "
    " risk_cap = EXCLUDED.risk_cap, defensive_floor = EXCLUDED.defensive_floor, "
    " priced_at = EXCLUDED.priced_at, input_prices_sha256 = EXCLUDED.input_prices_sha256, "
    " pack_v2_sha256 = EXCLUDED.pack_v2_sha256, "
    " module_pins_sha256 = EXCLUDED.module_pins_sha256, "
    " code_commit = EXCLUDED.code_commit, run_id = EXCLUDED.run_id, "
    " publish_state = 'published', valid_status = 'valid', "
    " valid_until = EXCLUDED.valid_until, invalidated_at = NULL, "
    " invalidated_reason = NULL, updated_at = now() "
    "WHERE open_macro_v03_allocations.valid_status <> 'invalidated'"
)

_STALENESS_UPSERT_SQL = (
    "INSERT INTO open_macro_v03_staleness_blocks "
    "(as_of, reason, stale_detail, input_vintage_sha256, input_prices_sha256, "
    " pack_v2_sha256, module_pins_sha256, code_commit, run_id) "
    "VALUES (%(as_of)s, %(reason)s, %(stale_detail)s::jsonb, %(input_vintage_sha256)s, "
    " %(input_prices_sha256)s, %(pack_v2_sha256)s, %(module_pins_sha256)s, "
    " %(code_commit)s, %(run_id)s) "
    "ON CONFLICT (as_of) DO UPDATE SET "
    " reason = EXCLUDED.reason, stale_detail = EXCLUDED.stale_detail, "
    " input_vintage_sha256 = EXCLUDED.input_vintage_sha256, "
    " input_prices_sha256 = EXCLUDED.input_prices_sha256, "
    " pack_v2_sha256 = EXCLUDED.pack_v2_sha256, "
    " module_pins_sha256 = EXCLUDED.module_pins_sha256, "
    " code_commit = EXCLUDED.code_commit, run_id = EXCLUDED.run_id"
)


def publish(conn, decision_row: dict[str, Any], allocation_row: dict[str, Any]) -> None:
    """Upsert decision + allocation in ONE transaction. A re-run NEVER resurrects an
    invalidated row: the ON CONFLICT WHERE clause skips it (rowcount 0) ⇒ raise."""
    with conn.cursor() as cur:
        cur.execute(_DECISION_UPSERT_SQL, decision_row)
        if cur.rowcount == 0:
            raise OpenMacroV03Error(
                f"decision upsert for {decision_row['as_of']} did not apply: the row is "
                "invalidated; a re-run cannot resurrect an invalidated decision")
        cur.execute(_ALLOCATION_UPSERT_SQL, allocation_row)
        if cur.rowcount == 0:
            raise OpenMacroV03Error(
                f"allocation upsert for {allocation_row['as_of']} did not apply: the row "
                "is invalidated; a re-run cannot resurrect an invalidated allocation")
    conn.commit()


def record_staleness_block(conn, block_row: dict[str, Any]) -> None:
    """Write the durable staleness-block ledger row in its own transaction."""
    with conn.cursor() as cur:
        cur.execute(_STALENESS_UPSERT_SQL, block_row)
    conn.commit()


# --------------------------------------------------------------------------- #
# Gate 12 — post-write verification
# --------------------------------------------------------------------------- #
def _invalidate_both(conn, as_of: _dt.date, reason: str) -> None:
    with conn.cursor() as cur:
        for table in ("open_macro_v03_decisions", "open_macro_v03_allocations"):
            cur.execute(
                f"UPDATE {table} SET valid_status='invalidated', invalidated_at=now(), "
                "invalidated_reason=%s, valid_until=now(), updated_at=now() "
                "WHERE as_of=%s",
                (reason, as_of))
    conn.commit()


def post_write_verify(conn, as_of: _dt.date, weights: dict[str, float]) -> None:
    """Re-read both rows in a fresh transaction: present, published, valid, and the
    persisted weights equal the computed ones. Any mismatch ⇒ stamp both invalidated."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT quadrant, publish_state, valid_status "
            "FROM open_macro_v03_decisions WHERE as_of=%s", (as_of,))
        d = cur.fetchone()
        cur.execute(
            "SELECT w_spy, w_tlt, w_tip, w_gld, w_dbc, w_shy, publish_state, valid_status "
            "FROM open_macro_v03_allocations WHERE as_of=%s", (as_of,))
        a = cur.fetchone()
    conn.commit()
    problems: list[str] = []
    if d is None:
        problems.append("decision row absent after publish")
    elif not (d[1] == "published" and d[2] == "valid"):
        problems.append(f"decision row not published/valid: {d[1]}/{d[2]}")
    if a is None:
        problems.append("allocation row absent after publish")
    else:
        if not (a[6] == "published" and a[7] == "valid"):
            problems.append(f"allocation row not published/valid: {a[6]}/{a[7]}")
        for col, ticker in zip(range(6), ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY")):
            if abs(float(a[col]) - weights[ticker]) > 1e-9:
                problems.append(f"{ticker} weight reread {a[col]} != computed {weights[ticker]}")
    if problems:
        _invalidate_both(conn, as_of, "post_write_verify: " + "; ".join(problems))
        raise OpenMacroV03Error("post-write verification failed: " + "; ".join(problems))


# --------------------------------------------------------------------------- #
# Provenance
# --------------------------------------------------------------------------- #
def code_commit() -> str:
    sha = os.environ.get("RAILWAY_GIT_COMMIT_SHA")
    if sha:
        return sha.strip()
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                          capture_output=True, text=True, check=True).stdout.strip()


def ensure_schema(conn) -> None:
    """Apply the three committed DDL files (idempotent)."""
    with conn.cursor() as cur:
        for rel in _SCHEMAS:
            cur.execute((ROOT / rel).read_text(encoding="utf-8"))
    conn.commit()


# --------------------------------------------------------------------------- #
# run()
# --------------------------------------------------------------------------- #
def run(dsn: str, *, as_of: str | None = None) -> dict[str, Any]:
    """Daily runtime job. Returns a stats dict; see the module docstring for the
    fail-loud gate ordering. No side effects precede any gate."""
    t0 = time.monotonic()

    # Gate 1 — feature flag (no DB).
    if os.environ.get(RUNTIME_FLAG_ENV) != "true":
        return {"status": "flag_off"}

    # Gate 2 — governance envelope (no DB).
    envelope = _load_json(ENVELOPE_PATH)
    reason = check_governance(envelope)
    if reason is not None:
        return {"status": "governance_blocked", "reason": reason}

    # Gate 3 — module pins (no DB).
    pins = _load_json(PINS_PATH)
    verify_module_pins(pins, ROOT)
    module_pins_sha256 = pins["module_pins_sha256"]

    # Gate 4 — connect + advisory lock.
    conn = connect(dsn)
    try:
        with advisory_lock(conn, LOCK_OPEN_MACRO_V03) as got:
            if not got:
                return {"status": "lock_busy"}

            # Gate 5 — schema.
            ensure_schema(conn)

            # Gate 6 — as_of.
            as_of_date = resolve_as_of(as_of)
            if as_of_date is None:
                return {"status": "non_business_day"}

            # Gate 7 — inputs.
            vintage_rows, price_rows = compose_inputs(conn, as_of_date)
            input_vintage_sha256 = _canonical_sha256(vintage_rows)
            input_prices_sha256 = _canonical_sha256(price_rows)

            commit = code_commit()
            run_id = f"open_macro_v03-{as_of_date.isoformat()}-{uuid.uuid4().hex[:8]}"

            # Gate 8 — staleness.
            report = staleness_report(vintage_rows, price_rows, as_of_date)
            if report["breaches"]:
                stale_detail = {
                    "breaches": report["breaches"],
                    "series": report["series"],
                    "prices": report["prices"],
                    "criteria": report["criteria"],
                }
                record_staleness_block(conn, {
                    "as_of": as_of_date,
                    "reason": "staleness SLO breach: " + "; ".join(
                        b.get("series_id") or b.get("ticker") or "?"
                        for b in report["breaches"]),
                    "stale_detail": json.dumps(stale_detail, sort_keys=True),
                    "input_vintage_sha256": input_vintage_sha256,
                    "input_prices_sha256": input_prices_sha256,
                    "pack_v2_sha256": PACK_SHA256_PIN,
                    "module_pins_sha256": module_pins_sha256,
                    "code_commit": commit,
                    "run_id": run_id,
                })
                # post-write check: the ledger row is present.
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM open_macro_v03_staleness_blocks WHERE as_of=%s",
                        (as_of_date,))
                    if cur.fetchone() is None:
                        raise OpenMacroV03Error(
                            "staleness-block ledger row absent after write")
                conn.commit()
                return {"status": "staleness_block", "as_of": as_of_date.isoformat(),
                        "reason": report["breaches"], "run_id": run_id,
                        "wall_ms": int((time.monotonic() - t0) * 1000)}

            # Gate 9 — decision chain + consumable position.
            chain = decision_mod.run_decision_series(vintage_rows, CHAIN_START, as_of_date)
            last, validity, seed_as_of = consumable_today(chain, as_of_date)

            # Gate 10 — allocation.
            allocation = build_allocation(last.quadrant, price_rows)
            weights = allocation["weights"]

            vu = valid_until(as_of_date)
            decision_row = {
                "as_of": as_of_date,
                "quadrant": last.quadrant,
                "decision_validity": validity,
                "carry_seed_as_of": seed_as_of,
                "candidate_confidence": last.candidate_confidence,
                "coverage_quality": last.coverage_quality,
                "growth_score": last.growth_score,
                "inflation_score": last.inflation_score,
                "input_vintage_sha256": input_vintage_sha256,
                "input_prices_sha256": input_prices_sha256,
                "pack_v2_sha256": PACK_SHA256_PIN,
                "module_pins_sha256": module_pins_sha256,
                "judgment_ref": JUDGMENT_REF,
                "threshold_ref": THRESHOLD_REF,
                "code_commit": commit,
                "run_id": run_id,
                "valid_until": vu,
            }
            allocation_row = {
                "as_of": as_of_date,
                "book": BOOK,
                "w_spy": weights["SPY"], "w_tlt": weights["TLT"], "w_tip": weights["TIP"],
                "w_gld": weights["GLD"], "w_dbc": weights["DBC"], "w_shy": weights["SHY"],
                "risk_assets_weight": allocation["risk_assets_weight"],
                "defensive_assets_weight": allocation["defensive_assets_weight"],
                "risk_cap": sleeve_mod.RISK_CAP_BASELINE,
                "defensive_floor": sleeve_mod.DEFENSIVE_FLOOR_BASELINE,
                "priced_at": allocation["priced_at"],
                "input_prices_sha256": input_prices_sha256,
                "pack_v2_sha256": PACK_SHA256_PIN,
                "module_pins_sha256": module_pins_sha256,
                "code_commit": commit,
                "run_id": run_id,
                "valid_until": vu,
            }

            # Gate 11 — publish (atomic).
            publish(conn, decision_row, allocation_row)

            # Gate 12 — post-write verify.
            post_write_verify(conn, as_of_date, weights)

            return {
                "status": "published",
                "as_of": as_of_date.isoformat(),
                "quadrant": last.quadrant,
                "decision_validity": validity,
                "weights": weights,
                "run_id": run_id,
                "wall_ms": int((time.monotonic() - t0) * 1000),
            }
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# invalidate CLI (manual kill switch — NEVER called by run(); never the ledger)
# --------------------------------------------------------------------------- #
_INVALIDATE_SQL = (
    "UPDATE {table} SET valid_status='invalidated', invalidated_at=now(), "
    "invalidated_reason=%(reason)s, valid_until=now(), updated_at=now() "
    "WHERE as_of BETWEEN %(from)s AND %(to)s"
)


def invalidate(dsn: str, *, as_of: str, to: str | None = None,
               reason: str) -> dict[str, Any]:
    """Stamp the decision AND allocation rows in [as_of, to] invalidated. The
    staleness-block ledger is NEVER touched."""
    to = to or as_of
    conn = connect(dsn)
    try:
        with advisory_lock(conn, LOCK_OPEN_MACRO_V03) as got:
            if not got:
                return {"status": "lock_busy"}
            params = {"reason": reason, "from": as_of, "to": to}
            with conn.cursor() as cur:
                cur.execute(_INVALIDATE_SQL.format(table="open_macro_v03_decisions"), params)
                decisions = cur.rowcount
                cur.execute(_INVALIDATE_SQL.format(table="open_macro_v03_allocations"), params)
                allocations = cur.rowcount
            conn.commit()
            return {"status": "invalidated", "as_of_from": as_of, "as_of_to": to,
                    "reason": reason, "decisions_invalidated": decisions,
                    "allocations_invalidated": allocations}
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(prog="python -m src.workers.open_macro_v03")
    sub = parser.add_subparsers(dest="command")
    inv = sub.add_parser("invalidate", help="manual kill switch: invalidate rows in a range")
    inv.add_argument("--as-of", dest="as_of", required=True, help="range start (YYYY-MM-DD)")
    inv.add_argument("--to", dest="to", default=None, help="range end (default: --as-of)")
    inv.add_argument("--reason", dest="reason", required=True, help="invalidation reason")
    args = parser.parse_args(argv)
    if args.command == "invalidate":
        stats = invalidate(resolve_dsn(), as_of=args.as_of, to=args.to, reason=args.reason)
        print(json.dumps(stats, default=str))
        return 0
    parser.error("no command (use: invalidate)")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
