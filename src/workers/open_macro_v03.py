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
  4. connect + pin search_path=public (before any DDL/table access, so a non-public
     DSN/role default cannot divert reads/writes) + a dedicated advisory lock (at
     most one run per business day).
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
    PRICE_MAX_AGE_BUSINESS_DAYS,
    _VINTAGE_KEY,
    compose_rows,
    consumable_today,
    staleness_report,
)
from harness.direct_activation.build_stage_b_artifacts import (
    PINNED_MODULES,
    _canonical_block_sha256,
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

# The inherited Phase 4 approval matrix roles (the set pinned by
# tests/test_dark_launch_readiness.py / tests/test_controlled_activation_proposal.py).
APPROVAL_ROLES = (
    "technical_owner",
    "quant_owner",
    "risk_owner",
    "operations_owner",
    "product_portfolio_owner",
    "final_approver",
)

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

# The identity fields the Stage B builder stamps on the envelope. check_governance
# rejects any envelope whose identity does not match EXACTLY before it trusts a single
# activation boolean — a wrong/stale artifact at ENVELOPE_PATH with all booleans filled
# must never let the worker publish official rows under an unratified governance record.
ENVELOPE_IDENTITY: dict[str, Any] = {
    "artifact_type": "open_macro_v03_stage_b_activation_envelope",
    "schema_version": 1,
    "stage": "B",
    "stage_b_id": "open_macro_v03_direct_activation_stage_b_001",
    "direct_activation_id": "open_macro_v03_direct_activation_001",
}

# The ONE approved Railway service the worker may publish official rows from. The
# envelope must name exactly this service AND (when the runtime sets RAILWAY_SERVICE_NAME)
# match it, so an artifact naming a staging/local service — or copied into another
# service that happens to carry the prod DSN + feature flag — cannot pass governance.
APPROVED_RAILWAY_SERVICE = "open-macro-v03-worker"


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
    """STRICT loader for every governance/pin/pack file the worker consumes:
    duplicate keys and NaN/Infinity are rejected (the guard-test semantics), so a
    doctored envelope can never smuggle a second key past the gate."""
    return json.loads(path.read_text(encoding="utf-8"),
                      object_pairs_hook=_reject_duplicate_keys,
                      parse_constant=_reject_non_finite_constant,
                      parse_float=_reject_non_finite_float)


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
    can never spoof a flip. All gates must be satisfied together. The envelope's
    identity (artifact_type / schema_version / stage / stage_b_id / direct_activation_id)
    is validated FIRST: a wrong or stale artifact at this path is rejected before any
    activation boolean is trusted."""
    def _true(key: str) -> bool:
        return envelope.get(key) is True

    for key, want in ENVELOPE_IDENTITY.items():
        if envelope.get(key) != want:
            return f"envelope identity {key}!={want!r} (got {envelope.get(key)!r})"

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
    # Stage B publishes to the new tables ONLY; the Stage C freeze and the production
    # endpoint stay blocked. A final JSON that flips these before the ratified cutover
    # must be rejected here, before any DB access.
    if envelope.get("freeze_ready") is not False:
        return "freeze_ready!=false (Stage B never sets the Stage C freeze)"
    if envelope.get("production_endpoint_activation") != "none":
        return "production_endpoint_activation!='none' (Stage B does not expose the endpoint)"
    if envelope.get("A5") != "active":
        return "A5!=active"
    allowed = envelope.get("allowed_tables")
    if not isinstance(allowed, list) or set(allowed) != set(ALLOWED_TABLES):
        return "allowed_tables!=the three sanctioned tables"
    environment = envelope.get("environment")
    if not isinstance(environment, dict):
        return "environment missing"
    service = environment.get("railway_service_name")
    if service != APPROVED_RAILWAY_SERVICE:
        return f"environment.railway_service_name!={APPROVED_RAILWAY_SERVICE!r} (got {service!r})"
    runtime_service = os.environ.get("RAILWAY_SERVICE_NAME")
    if runtime_service and service != runtime_service:
        return (f"environment.railway_service_name {service!r} != runtime Railway service "
                f"{runtime_service!r} (artifact copied into the wrong service)")
    # inherited Phase 4 approval matrix: EXACTLY the six pinned role ids, each with a
    # named (non-empty) holder, and the strict-bool completeness flag. An absent or
    # stale approval matrix blocks the flip.
    matrix = envelope.get("approval_matrix")
    if not isinstance(matrix, dict) or set(matrix) != set(APPROVAL_ROLES):
        return "approval_matrix missing or roles != the six pinned ids"
    for role in APPROVAL_ROLES:
        entry = matrix.get(role)
        if not isinstance(entry, dict):
            return f"approval_matrix.{role} missing"
        holder = entry.get("owner")
        if not (isinstance(holder, str) and holder.strip()):
            return f"approval_matrix.{role}.owner missing or empty"
        # a named owner is not a sign-off: each role must be explicitly APPROVED with
        # evidence and a timestamp, else a partially-filled matrix could pass the gate.
        if entry.get("approval_status") != "approved":
            return f"approval_matrix.{role}.approval_status!=approved"
        evidence = entry.get("approval_evidence")
        if not (isinstance(evidence, str) and evidence.strip()):
            return f"approval_matrix.{role}.approval_evidence missing"
        timestamp = entry.get("timestamp")
        if not (isinstance(timestamp, str) and timestamp.strip()):
            return f"approval_matrix.{role}.timestamp missing"
    if envelope.get("approval_matrix_complete") is not True:
        return "approval_matrix_complete!=true"
    return None


# --------------------------------------------------------------------------- #
# Gate 3 — module pins
# --------------------------------------------------------------------------- #
def verify_module_pins(pins: dict[str, Any], root: Path = ROOT) -> None:
    """Raise unless the pin manifest is COMPLETE and intact: the pinned module set is
    exactly the committed closure (``PINNED_MODULES``), each module's sha256 (CRLF→LF)
    matches the tree, AND ``module_pins_sha256`` recomputes over the {modules, pack}
    block. A truncated or doctored ``module_pins.json`` — one that omits a formula/
    sleeve module or carries an empty ``modules`` object — cannot pass by iterating only
    the keys it happens to contain and then stamp an unchecked ``module_pins_sha256``."""
    modules = pins["modules"]
    if set(modules) != set(PINNED_MODULES):
        missing = sorted(set(PINNED_MODULES) - set(modules))
        extra = sorted(set(modules) - set(PINNED_MODULES))
        raise OpenMacroV03Error(
            f"module pin set diverges from the committed manifest "
            f"(missing={missing}, unexpected={extra})")
    for rel in sorted(modules):
        actual = _sha256_norm(root / rel)
        if actual != modules[rel]:
            raise OpenMacroV03Error(
                f"module pin mismatch for {rel}: {actual} != pinned {modules[rel]}")
    # The pack block must be the CERTIFIED pack metadata, not merely internally
    # consistent: a stale/altered pack block with a matching recomputed hash would still
    # stamp a wrong module_pins_sha256 (for a different pin bundle) into the official
    # rows, corrupting the provenance the monitor / Stage C evidence relies on.
    pack_manifest = _load_json(PACK / "manifest.json")
    expected_pack = {
        "input_pack_id": pack_manifest["input_pack_id"],
        "input_pack_sha256": pack_manifest["input_pack_sha256"],
        "canonical_snapshot_sha256": pack_manifest["canonical_snapshot_sha256"],
    }
    if expected_pack["input_pack_sha256"] != PACK_SHA256_PIN:
        raise OpenMacroV03Error(
            "certified pack manifest input_pack_sha256 diverged from the signed pin")
    if pins.get("pack") != expected_pack:
        raise OpenMacroV03Error(
            f"module_pins pack block {pins.get('pack')} != certified pack "
            f"{expected_pack} (a stale/altered pack block cannot be stamped as provenance)")
    recomputed = _canonical_block_sha256({"modules": modules, "pack": pins["pack"]})
    if recomputed != pins.get("module_pins_sha256"):
        raise OpenMacroV03Error(
            f"module_pins_sha256 {pins.get('module_pins_sha256')!r} != recomputed "
            f"{recomputed!r} (the pin block was altered)")


# --------------------------------------------------------------------------- #
# Gate 3b — pack v2 REAL bytes (the declarative manifest is never the only source)
# --------------------------------------------------------------------------- #
def verify_pack_bytes(pack: Path | None = None) -> None:
    """Verify the BYTES the run will consume from the committed pack, before any DB:

    * sha256 (CRLF→LF normalized) of ``data/canonical/macro_observation_vintage.json``
      and ``data/canonical/eod_prices.json`` against the ``SOURCE.json`` p1_export
      per-table pins (the certified export byte hashes); and
    * the aggregate ``input_pack_sha256`` RECOMPUTED over the pack tree with the
      builder's own algorithm (``src.input_packs.manifest.compute_input_pack_sha256``,
      the same function ``harness/p1_pack/verifier.py::verify_pack`` uses) against the
      signed ``PACK_SHA256_PIN`` constant.

    A single mutated byte in a canonical data file fails here, loudly."""
    pack = pack if pack is not None else PACK
    source = _load_json(pack / "SOURCE.json")
    pins = {t["table"]: t["sha256"] for t in source["p1_export"]["tables"]}
    for table, filename in (("macro_observation_vintage", "macro_observation_vintage.json"),
                            ("eod_prices", "eod_prices.json")):
        actual = _sha256_norm(pack / "data" / "canonical" / filename)
        if actual != pins.get(table):
            raise OpenMacroV03Error(
                f"pack byte verification failed: {filename} sha256 {actual} != "
                f"SOURCE.json pin {pins.get(table)} (the committed pack bytes are not "
                "the certified export)")
    manifest = _load_json(pack / "manifest.json")
    if manifest.get("input_pack_sha256") != PACK_SHA256_PIN:
        raise OpenMacroV03Error(
            "pack byte verification failed: manifest input_pack_sha256 "
            f"{manifest.get('input_pack_sha256')} != signed pin {PACK_SHA256_PIN}")
    from src.input_packs.manifest import compute_input_pack_sha256
    recomputed = compute_input_pack_sha256(pack, manifest)
    if recomputed != PACK_SHA256_PIN:
        raise OpenMacroV03Error(
            f"pack byte verification failed: recomputed aggregate {recomputed} != "
            f"signed pin {PACK_SHA256_PIN} (a pack file diverges from the certified "
            "tree; the declarative manifest is not trusted alone)")


# --------------------------------------------------------------------------- #
# Gate 6 — as_of resolution
# --------------------------------------------------------------------------- #
def resolve_as_of(as_of_arg: str | None = None, *,
                  today: _dt.date | None = None) -> _dt.date | None:
    """Explicit override (arg or env) is trusted for any past-or-current date;
    otherwise the current America/New_York calendar day, or ``None`` on a weekend
    (non-business day).

    A FUTURE override (> the current America/New_York day) is REJECTED loud: the
    worker only publishes for a past-or-current business day and must never stamp an
    official future-dated decision/allocation (a future output row is itself illegal
    by the monitor's ``future_as_of_write`` guard). There is no separate non-
    publishing backfill path — ``run()`` is the only publish path — so the future
    gate cannot block a legitimate replay.

    Calendar policy (DELIBERATE, owner-ratified): business days are Mon–Fri with NO
    market-holiday calendar. On a US market holiday the worker RUNS and publishes a
    ``carried`` decision (renewing ``valid_until`` — no reader blackout), and the
    3-business-day price staleness gate absorbs the holiday price gap. This is
    consistent with the decision chain and with Stage A, whose live validation ran
    on 2026-07-03 (a market holiday) and published carried."""
    if today is None:
        from zoneinfo import ZoneInfo
        today = _dt.datetime.now(ZoneInfo("America/New_York")).date()
    override = as_of_arg or os.environ.get(AS_OF_ENV)
    if override:
        resolved = _dt.date.fromisoformat(override)
        if resolved > today:
            raise OpenMacroV03Error(
                f"as_of override {resolved.isoformat()} is in the future (> current "
                f"America/New_York day {today.isoformat()}); the worker never stamps "
                "an official future-dated decision (illegal by the monitor's "
                "future_as_of_write guard)")
        return resolved
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
def build_allocation(quadrant: str, price_rows: list[dict],
                     as_of: _dt.date) -> dict[str, Any]:
    """Today's consumable compressed_50 target with the risk-cap/defensive-floor gates."""
    prices = sleeve_mod.PriceFrame(price_rows)
    if not prices.dates:
        raise OpenMacroV03Error("no priced sessions for the sleeve")

    def _usable(p: float | None) -> bool:
        return p is not None and p == p and p > 0

    # Price the WHOLE sleeve at one coherent snapshot: the latest date at which EVERY
    # ticker has a usable price. The global max date can be a partial-ingest day one
    # ticker lags (still fresh per-ticker, so the staleness gate passed) — pricing
    # against it would raise here, AFTER the ledger gate, producing neither an output
    # nor a staleness-block row (a silent missing_output for the monitor).
    priced_at = next(
        (d for d in sorted(prices.dates, reverse=True)
         if all(_usable(prices.price(t, d)) for t in sleeve_mod.SLEEVE_TICKERS)),
        None)
    if priced_at is None:
        raise OpenMacroV03Error(
            "no session prices the full sleeve at a single date (split-date partial "
            "ingest); refuse rather than price tickers at inconsistent dates")
    # …and that common date must still satisfy the SAME price-age SLO the staleness gate
    # enforces. staleness_report only checks price DATES, so a recent-but-unusable print
    # (NaN/zero/negative) can pass it while the freshest sleeve-wide USABLE date is stale;
    # publishing a stale priced_at then would be wrong — bound it here.
    business_age = len([1 for i in range(1, (as_of - priced_at).days + 1)
                        if (priced_at + _dt.timedelta(days=i)).weekday() < 5])
    if business_age > PRICE_MAX_AGE_BUSINESS_DAYS:
        raise OpenMacroV03Error(
            f"sleeve-wide usable priced_at {priced_at} is {business_age} business days "
            f"old (> {PRICE_MAX_AGE_BUSINESS_DAYS}); a recent-but-unusable print must not "
            "publish a stale allocation")
    available: list[str] = list(sleeve_mod.SLEEVE_TICKERS)
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
    """Next business day at 14:00 UTC (the reader's freshness horizon).

    Business days are DELIBERATELY Mon–Fri without a market-holiday calendar (see
    ``resolve_as_of``): on a holiday the worker still runs and publishes carried,
    renewing this horizon, so the reader never sees a holiday blackout."""
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

# The ledger is IMMUTABLE: a re-run of a still-stale day preserves the FIRST
# recorded block (ON CONFLICT DO NOTHING); rowcount 0 = block already recorded.
_STALENESS_INSERT_SQL = (
    "INSERT INTO open_macro_v03_staleness_blocks "
    "(as_of, reason, stale_detail, input_vintage_sha256, input_prices_sha256, "
    " pack_v2_sha256, module_pins_sha256, code_commit, run_id) "
    "VALUES (%(as_of)s, %(reason)s, %(stale_detail)s::jsonb, %(input_vintage_sha256)s, "
    " %(input_prices_sha256)s, %(pack_v2_sha256)s, %(module_pins_sha256)s, "
    " %(code_commit)s, %(run_id)s) "
    "ON CONFLICT (as_of) DO NOTHING"
)


def publish(conn, decision_row: dict[str, Any], allocation_row: dict[str, Any]) -> None:
    """Upsert decision + allocation in ONE transaction. A re-run NEVER resurrects an
    invalidated row: the ON CONFLICT WHERE clause skips it (rowcount 0) ⇒ raise.

    Ledger×output mutual exclusion: inside the SAME transaction, a staleness-block
    ledger row for the as_of forbids publishing — the day was durably blocked, and
    publishing over a recorded block requires EXPLICIT operator resolution (remove
    or supersede the block through the sanctioned operational path), never a silent
    later run."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM open_macro_v03_staleness_blocks WHERE as_of = %(as_of)s",
            decision_row)
        if cur.fetchone() is not None:
            raise OpenMacroV03Error(
                f"publish refused for {decision_row['as_of']}: a staleness-block "
                "ledger row exists for this day; publishing after a recorded block "
                "requires explicit operator resolution, not a re-run")
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


def record_staleness_block(conn, block_row: dict[str, Any]) -> bool:
    """Write the durable staleness-block ledger row in its own transaction.

    Output×ledger mutual exclusion: inside the SAME transaction, ANY output row for
    the as_of forbids recording a block — a block must never land on top of already
    published output. The ledger is immutable (ON CONFLICT DO NOTHING): returns True
    when this run inserted the row, False when the day's block was already recorded
    (the first record is preserved verbatim)."""
    with conn.cursor() as cur:
        for table in ("open_macro_v03_decisions", "open_macro_v03_allocations"):
            cur.execute(f"SELECT 1 FROM {table} WHERE as_of = %(as_of)s", block_row)
            if cur.fetchone() is not None:
                raise OpenMacroV03Error(
                    f"staleness-block refused for {block_row['as_of']}: an output row "
                    f"already exists in {table}; a block is never recorded on top of "
                    "published output")
        cur.execute(_STALENESS_INSERT_SQL, block_row)
        inserted = cur.rowcount == 1
    conn.commit()
    return inserted


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


def pin_search_path(conn) -> None:
    """Force the session ``search_path`` to ``public`` BEFORE any DDL or table access.

    Every input/output table is referenced bare (and some read SQL is imported from
    the shared P1 exporter), so a non-public DSN/role default — e.g.
    ``options=-csearch_path=scratch`` — would resolve them against that schema. A
    scratch schema cloned up to ``PACK_CUT`` still satisfies the prefix-hash gate,
    after which the worker would compose scratch deltas while stamping official
    production provenance. SETTING the path (not merely verifying it) neutralizes
    that: the session ``SET`` overrides any startup/role default, and the read-back
    re-asserts it landed (fail loud otherwise)."""
    with conn.cursor() as cur:
        cur.execute("SET search_path TO public")
        cur.execute("SHOW search_path")
        current = (cur.fetchone()[0] or "").replace(" ", "")
    conn.commit()
    if current != "public":
        raise OpenMacroV03Error(
            f"search_path is {current!r} after pinning to public; refusing to run "
            "against a non-public schema (input and output tables must resolve to "
            "public)")


def ensure_schema(conn) -> None:
    """Apply the three committed DDL files (idempotent)."""
    with conn.cursor() as cur:
        for rel in _SCHEMAS:
            cur.execute((ROOT / rel).read_text(encoding="utf-8"))
    conn.commit()


# --------------------------------------------------------------------------- #
# Catalog verification (B1b evidence base)
# --------------------------------------------------------------------------- #
# MAINTENANCE: this dict mirrors schemas/open_macro_v03_*.sql EXACTLY, verified against
# the certified prod catalog (Investintell-Prod). Any DDL change MUST be reflected here
# (a stage-B guard test cross-checks every expected column/named-constraint against the
# committed DDL text). verify_schema compares the FULL column signature and every
# constraint definition, so drift a bare name+type gate would miss (CHAR(40) vs CHAR(64),
# a nullable/default change, a relaxed CHECK, a missing inline CHECK) fails loud.
#   columns:     col  -> (data_type, character_maximum_length | None, is_nullable, column_default | None)
#   constraints: name -> (contype, pg_get_constraintdef)   # exact prod catalog strings
EXPECTED_SCHEMA: dict[str, dict[str, dict[str, tuple]]] = {
    "open_macro_v03_decisions": {
        "columns": {
            "as_of": ("date", None, "NO", None),
            "quadrant": ("text", None, "NO", None),
            "decision_validity": ("text", None, "NO", None),
            "carry_seed_as_of": ("date", None, "NO", None),
            "candidate_confidence": ("numeric", None, "YES", None),
            "coverage_quality": ("numeric", None, "NO", None),
            "growth_score": ("numeric", None, "YES", None),
            "inflation_score": ("numeric", None, "YES", None),
            "input_vintage_sha256": ("character", 64, "NO", None),
            "input_prices_sha256": ("character", 64, "NO", None),
            "pack_v2_sha256": ("character", 64, "NO", None),
            "module_pins_sha256": ("character", 64, "NO", None),
            "judgment_ref": ("text", None, "NO", None),
            "threshold_ref": ("text", None, "NO", None),
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
        "constraints": {
            "open_macro_v03_decisions_pkey": ("p", "PRIMARY KEY (as_of)"),
            "open_macro_v03_decisions_check": ("c", "CHECK ((carry_seed_as_of <= as_of))"),
            "open_macro_v03_decisions_quadrant_check": ("c",
                "CHECK ((quadrant = ANY (ARRAY['recovery'::text, 'expansion'::text, "
                "'slowdown'::text, 'contraction'::text])))"),
            "open_macro_v03_decisions_decision_validity_check": ("c",
                "CHECK ((decision_validity = ANY (ARRAY['fresh'::text, 'carried'::text])))"),
            "open_macro_v03_decisions_candidate_confidence_check": ("c",
                "CHECK (((candidate_confidence IS NULL) OR ((candidate_confidence >= "
                "(0)::numeric) AND (candidate_confidence <= (1)::numeric))))"),
            "open_macro_v03_decisions_publish_state_check": ("c",
                "CHECK ((publish_state = ANY (ARRAY['publishing'::text, 'published'::text])))"),
            "open_macro_v03_decisions_valid_status_check": ("c",
                "CHECK ((valid_status = ANY (ARRAY['valid'::text, 'invalidated'::text])))"),
            "open_macro_v03_decisions_invalidation_consistent": ("c",
                "CHECK (((valid_status = 'invalidated'::text) = (invalidated_at IS NOT NULL)))"),
            "open_macro_v03_decisions_validity_seed": ("c",
                "CHECK ((((decision_validity = 'fresh'::text) AND (carry_seed_as_of = as_of)) "
                "OR ((decision_validity = 'carried'::text) AND (carry_seed_as_of < as_of))))"),
        },
    },
    "open_macro_v03_allocations": {
        "columns": {
            "as_of": ("date", None, "NO", None),
            "book": ("text", None, "NO", "'compressed_50'::text"),
            "w_spy": ("numeric", None, "NO", None),
            "w_tlt": ("numeric", None, "NO", None),
            "w_tip": ("numeric", None, "NO", None),
            "w_gld": ("numeric", None, "NO", None),
            "w_dbc": ("numeric", None, "NO", None),
            "w_shy": ("numeric", None, "NO", None),
            "risk_assets_weight": ("numeric", None, "NO", None),
            "defensive_assets_weight": ("numeric", None, "NO", None),
            "risk_cap": ("numeric", None, "NO", "0.65"),
            "defensive_floor": ("numeric", None, "NO", "0.20"),
            "priced_at": ("date", None, "NO", None),
            "input_prices_sha256": ("character", 64, "NO", None),
            "pack_v2_sha256": ("character", 64, "NO", None),
            "module_pins_sha256": ("character", 64, "NO", None),
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
        "constraints": {
            "open_macro_v03_allocations_pkey": ("p", "PRIMARY KEY (as_of)"),
            "open_macro_v03_allocations_as_of_fkey": ("f",
                "FOREIGN KEY (as_of) REFERENCES open_macro_v03_decisions(as_of)"),
            "open_macro_v03_allocations_book_check": ("c",
                "CHECK ((book = 'compressed_50'::text))"),
            "open_macro_v03_allocations_w_spy_check": ("c",
                "CHECK (((w_spy >= (0)::numeric) AND (w_spy <= (1)::numeric)))"),
            "open_macro_v03_allocations_w_tlt_check": ("c",
                "CHECK (((w_tlt >= (0)::numeric) AND (w_tlt <= (1)::numeric)))"),
            "open_macro_v03_allocations_w_tip_check": ("c",
                "CHECK (((w_tip >= (0)::numeric) AND (w_tip <= (1)::numeric)))"),
            "open_macro_v03_allocations_w_gld_check": ("c",
                "CHECK (((w_gld >= (0)::numeric) AND (w_gld <= (1)::numeric)))"),
            "open_macro_v03_allocations_w_dbc_check": ("c",
                "CHECK (((w_dbc >= (0)::numeric) AND (w_dbc <= (1)::numeric)))"),
            "open_macro_v03_allocations_w_shy_check": ("c",
                "CHECK (((w_shy >= (0)::numeric) AND (w_shy <= (1)::numeric)))"),
            "open_macro_v03_allocations_publish_state_check": ("c",
                "CHECK ((publish_state = ANY (ARRAY['publishing'::text, 'published'::text])))"),
            "open_macro_v03_allocations_valid_status_check": ("c",
                "CHECK ((valid_status = ANY (ARRAY['valid'::text, 'invalidated'::text])))"),
            "open_macro_v03_allocations_invalidation_consistent": ("c",
                "CHECK (((valid_status = 'invalidated'::text) = (invalidated_at IS NOT NULL)))"),
            "open_macro_v03_allocations_weights_sum": ("c",
                "CHECK ((abs(((((((w_spy + w_tlt) + w_tip) + w_gld) + w_dbc) + w_shy) "
                "- (1)::numeric)) < 0.000000001))"),
            "open_macro_v03_allocations_risk_cap": ("c",
                "CHECK ((risk_assets_weight <= (risk_cap + 0.000000001)))"),
            "open_macro_v03_allocations_defensive_floor": ("c",
                "CHECK ((defensive_assets_weight >= (defensive_floor - 0.000000001)))"),
        },
    },
    "open_macro_v03_staleness_blocks": {
        "columns": {
            "as_of": ("date", None, "NO", None),
            "reason": ("text", None, "NO", None),
            "stale_detail": ("jsonb", None, "NO", None),
            "input_vintage_sha256": ("character", 64, "NO", None),
            "input_prices_sha256": ("character", 64, "NO", None),
            "pack_v2_sha256": ("character", 64, "NO", None),
            "module_pins_sha256": ("character", 64, "NO", None),
            "code_commit": ("character", 40, "NO", None),
            "run_id": ("text", None, "NO", None),
            "created_at": ("timestamp with time zone", None, "NO", "now()"),
        },
        "constraints": {
            "open_macro_v03_staleness_blocks_pkey": ("p", "PRIMARY KEY (as_of)"),
        },
    },
}

_CATALOG_COLUMNS_SQL = (
    "SELECT table_name, column_name, data_type, character_maximum_length, "
    "is_nullable, column_default FROM information_schema.columns "
    "WHERE table_schema = 'public' AND table_name = ANY(%(tables)s) "
    "ORDER BY table_name, ordinal_position")
_CATALOG_CONSTRAINTS_SQL = (
    "SELECT conrelid::regclass::text AS table_name, conname, contype::text, "
    "pg_get_constraintdef(oid) AS condef "
    "FROM pg_constraint "
    "WHERE connamespace = 'public'::regnamespace "
    "AND contype IN ('c', 'p', 'f') "  # NOT NULL ('n') is verified via column is_nullable
    "AND conrelid::regclass::text = ANY(%(tables)s)")


def verify_schema(conn) -> dict[str, Any]:
    """Verify the live catalog against the committed DDL expectations (read-only).

    Scoped to the ``public`` schema (both queries), so a look-alike scratch schema
    with same-named ``open_macro_v03_*`` tables can never be certified in place of the
    objects the worker actually reads and writes. The FULL column signature must match
    (data_type + character_maximum_length + is_nullable + column_default — so CHAR(40)
    vs CHAR(64), a nullability flip, or a missing/altered DEFAULT is caught, not just
    the base type), no column may be missing or extra, and EVERY expected constraint
    (PK/FK, the named custom CHECKs, AND the inline auto-named CHECKs like quadrant /
    publish_state / the weight ranges) must be present with the right contype AND the
    exact ``pg_get_constraintdef`` (a relaxed same-named CHECK is caught, since
    ``CREATE TABLE IF NOT EXISTS`` never repairs drift). Any divergence raises. Returns
    the verified ``{table: {columns, constraints}}`` view — the evidence base for the
    B1b ``schema_migration_record``. This is the SAME function the read-only monitor
    imports (it issues SELECTs only)."""
    tables = sorted(EXPECTED_SCHEMA)
    with conn.cursor() as cur:
        cur.execute(_CATALOG_COLUMNS_SQL, {"tables": tables})
        column_rows = cur.fetchall()
        cur.execute(_CATALOG_CONSTRAINTS_SQL, {"tables": tables})
        constraint_rows = cur.fetchall()

    actual_columns: dict[str, dict[str, tuple]] = {t: {} for t in tables}
    for table_name, column_name, data_type, char_max_len, is_nullable, column_default in column_rows:
        actual_columns.setdefault(table_name, {})[column_name] = (
            data_type, char_max_len, is_nullable, column_default)
    actual_constraints: dict[str, dict[str, tuple]] = {t: {} for t in tables}
    for table_name, conname, contype, condef in constraint_rows:
        actual_constraints.setdefault(table_name, {})[conname] = (contype, condef)

    problems: list[str] = []
    verified: dict[str, Any] = {}
    for table, expected in EXPECTED_SCHEMA.items():
        columns = actual_columns.get(table) or {}
        if not columns:
            problems.append(f"{table}: table missing from the catalog")
            continue
        exp_cols = expected["columns"]
        missing = sorted(set(exp_cols) - set(columns))
        extra = sorted(set(columns) - set(exp_cols))
        if missing or extra:
            problems.append(
                f"{table}: column set diverges from the committed DDL "
                f"(missing={missing}, unexpected={extra})")
        for col in sorted(set(exp_cols) & set(columns)):
            if columns[col] != exp_cols[col]:
                problems.append(
                    f"{table}.{col}: signature {columns[col]} != expected {exp_cols[col]} "
                    "(data_type, char_max_len, is_nullable, column_default)")
        constraints = actual_constraints.get(table) or {}
        for conname, exp in expected["constraints"].items():
            if constraints.get(conname) != exp:
                problems.append(
                    f"{table}: constraint {conname} {constraints.get(conname)} != "
                    f"expected {exp} (contype, definition)")
        # an EXTRA CHECK/FK from a manual migration would let a valid publish() fail
        # against a constraint the DDL never declared; CREATE TABLE IF NOT EXISTS never
        # removes it, so reject unexpected constraints too (mirror the column-set check).
        extra_cons = sorted(set(constraints) - set(expected["constraints"]))
        if extra_cons:
            problems.append(
                f"{table}: unexpected constraints not in the committed DDL: {extra_cons}")
        verified[table] = {"columns": dict(columns),
                           "constraints": dict(constraints)}
    if problems:
        raise OpenMacroV03Error(
            "schema catalog verification failed: " + "; ".join(problems))
    return verified


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

    # Gate 3b — pack v2 REAL bytes (no DB): per-file SOURCE pins + recomputed
    # aggregate; the declarative manifest is never the only source of truth.
    verify_pack_bytes()

    # Gate 4 — connect + pin search_path (public, before any DDL/table access) +
    # advisory lock.
    conn = connect(dsn)
    try:
        pin_search_path(conn)
        with advisory_lock(conn, LOCK_OPEN_MACRO_V03) as got:
            if not got:
                return {"status": "lock_busy"}

            # Gate 5 — schema (apply, then VERIFY the live catalog against the
            # committed DDL expectations — the B1b evidence base).
            ensure_schema(conn)
            verify_schema(conn)

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
                inserted = record_staleness_block(conn, {
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
                # post-write check: the ledger row is present. A re-run of a
                # still-stale day is clean (the immutable first record stands).
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM open_macro_v03_staleness_blocks WHERE as_of=%s",
                        (as_of_date,))
                    if cur.fetchone() is None:
                        raise OpenMacroV03Error(
                            "staleness-block ledger row absent after write")
                conn.commit()
                return {"status": "staleness_block", "as_of": as_of_date.isoformat(),
                        "ledger": "inserted" if inserted else "already_recorded",
                        "reason": report["breaches"], "run_id": run_id,
                        "wall_ms": int((time.monotonic() - t0) * 1000)}

            # Gate 9 — decision chain + consumable position.
            chain = decision_mod.run_decision_series(vintage_rows, CHAIN_START, as_of_date)
            last, validity, seed_as_of = consumable_today(chain, as_of_date)

            # Gate 10 — allocation.
            allocation = build_allocation(last.quadrant, price_rows, as_of_date)
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
        pin_search_path(conn)  # the kill switch must resolve the SAME public rows run() writes
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
