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
  2b. WRITER runtime identity — the workload must present the ONE approved writer
     identity (``WORKER_SERVICE_IDENTITY`` / ``CLOUD_RUN_JOB`` / ``K_SERVICE`` /
     ``RAILWAY_SERVICE_NAME``, first non-empty wins) ⇒ else ``wrong_service`` WITHOUT
     connecting. Platform-neutral and fail-closed: an absent identity is never trusted.
  3. Module pins — sha256 (CRLF→LF normalized) of each pinned pure module must match
     ⇒ else raise, BEFORE any DB access.
  4. connect + pin search_path=public (before any DDL/table access, so a non-public
     DSN/role default cannot divert reads/writes) + a dedicated advisory lock (at
     most one run per business day).
  5. READ-ONLY catalog verification (verify_schema) against the committed base DDL +
     carry_decay_v1 migration expectations — run() never applies schema; an absent or
     unmigrated catalog fails loud with zero writes (schema lifecycle belongs to the
     orchestrator's controlled apply step).
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

The ``resolve-staleness`` CLI (``python -m src.workers.open_macro_v03 resolve-staleness
--as-of ... --resolved-by ... --reason ...``) is the SANCTIONED recovery path for a day
the staleness gate blocked. It never edits or deletes the immutable block: it APPENDS a
'resolved' event to ``open_macro_v03_staleness_resolutions`` — and only after the worker
itself re-read the inputs for that day under the same advisory lock and found the
freshness report breach-free, storing that report as the event's proof. A later normal
run for the day then publishes and appends the matching 'superseded' event. Before this
command existed the block message demanded an "explicit operator resolution" the system
did not implement, and the only way out was an ad-hoc SQL session on the database host.
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
from harness.direct_activation import carry_decay
from harness.phase0q import decision_v3 as decision_mod
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
    "schemas/open_macro_v03_staleness_resolutions.sql",
)

# The three PRODUCT tables the ratified activation envelope authorizes (its
# `allowed_tables` list is compared against exactly this set — never widen it here
# without a re-ratified envelope, or every run fails governance).
ALLOWED_TABLES = frozenset({
    "open_macro_v03_decisions",
    "open_macro_v03_allocations",
    "open_macro_v03_staleness_blocks",
})

# The append-only clearance ledger for staleness blocks. It is NOT a product table and
# NOT part of the envelope's allowed_tables: it holds no decision, no allocation and no
# regime data — only the positive record of how a recorded block was cleared. It lives
# in the same `open_macro_v03_new_tables_only` write mode the envelope pins, and it is
# written on exactly two paths: the operator `resolve-staleness` command and the
# 'superseded' event the writer appends when a fresh run finally publishes the day.
RESOLUTIONS_TABLE = "open_macro_v03_staleness_resolutions"
RESOLUTION_STATES = ("resolved", "superseded")

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
# carry_decay_v1 (phase0q_005, ratified 2026-07-11): the degraded book an expired
# carry publishes — the mandate-tilted centroid of the compressed_50 family, the
# same token carry_decay.evaluate emits and the migration DDL admits.
CENTER_BOOK = "center_50"
JUDGMENT_REF = "open_macro_v03_confidence_v3_evidence_001:go"
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

# The ONE approved WRITER IDENTITY the worker may publish official rows from. This is
# a LOGICAL name, not a platform hostname: the envelope must name exactly this identity
# AND the runtime must present it, so an artifact naming a staging/local service — or
# copied into another workload that happens to carry the prod DSN + feature flag —
# cannot pass governance. The identity is deliberately platform-neutral: Railway,
# Cloud Run jobs and Cloud Run services all resolve to the SAME logical writer.
APPROVED_WRITER_IDENTITY = "open-macro-v03-worker"
# Historical alias (the Railway-era spelling of the same constant): kept so the Stage B
# artifacts and their guard tests, which reference APPROVED_RAILWAY_SERVICE by name,
# keep pointing at the one approved identity.
APPROVED_RAILWAY_SERVICE = APPROVED_WRITER_IDENTITY

# Where the runtime writer identity is read from, in precedence order. The FIRST env
# var set (non-empty) wins and must equal APPROVED_WRITER_IDENTITY — a platform whose
# native name differs (a Cloud Run job is `dl-open-macro-v03`) declares the logical
# identity explicitly through WORKER_SERVICE_IDENTITY.
#   WORKER_SERVICE_IDENTITY  explicit, platform-neutral — the preferred wiring anywhere
#   CLOUD_RUN_JOB            Cloud Run jobs runtime contract (job name)
#   K_SERVICE                Cloud Run services runtime contract (service name)
#   RAILWAY_SERVICE_NAME     legacy Railway — accepted for the transition window only
WRITER_IDENTITY_ENV_VARS = (
    "WORKER_SERVICE_IDENTITY",
    "CLOUD_RUN_JOB",
    "K_SERVICE",
    "RAILWAY_SERVICE_NAME",
)

# Commit-provenance env vars, in precedence order — the repo-wide convention (see
# src/workers/bond_metrics.py, bond_price_observations.py, daily_publication_chain.py).
# A container image carries no .git, so the git fallback alone is not a runtime path.
REVISION_ENV_VARS = ("CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA")


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
    # The envelope declares the LOGICAL writer identity. `writer_identity` is the
    # platform-neutral key; `railway_service_name` is the historical spelling of the
    # same field and stays accepted so the ratified Stage B artifact keeps validating
    # byte-for-byte. Either way the value must be the ONE approved identity.
    service = environment.get("writer_identity") or environment.get("railway_service_name")
    if service != APPROVED_WRITER_IDENTITY:
        return (f"environment.writer_identity!={APPROVED_WRITER_IDENTITY!r} "
                f"(got {service!r})")
    # NB: the RUNTIME writer identity (os.environ, see WRITER_IDENTITY_ENV_VARS) is a
    # WRITER-only gate — see check_writer_runtime(), called by run(). It is deliberately
    # NOT part of this predicate, so the read-only monitor (a separate workload) can
    # share the SAME governance check and still arm after activation.
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


def resolve_writer_identity() -> tuple[str | None, str | None]:
    """The runtime writer identity and the env var it came from, or ``(None, None)``.

    The first NON-EMPTY var of :data:`WRITER_IDENTITY_ENV_VARS` wins, so a workload can
    always state its logical identity explicitly (``WORKER_SERVICE_IDENTITY``) even when
    the platform's own name differs, and the platform-native vars stay as a fallback."""
    for name in WRITER_IDENTITY_ENV_VARS:
        value = (os.environ.get(name) or "").strip()
        if value:
            return value, name
    return None, None


def check_writer_runtime() -> str | None:
    """WRITER-only gate: official rows may be published only FROM the approved writer
    identity. An absent or mismatched runtime identity (a local or misconfigured runner
    carrying the prod DSN + feature flag) blocks — absent identity is not trusted; the
    gate is FAIL-CLOSED and runs before any DB access. The read-only MONITOR is a
    separate workload and does NOT call this; it shares ``check_governance`` only, so it
    still arms after activation.

    Platform-neutral by construction: the identity is the logical writer name, resolved
    from an explicit env var first and from the Cloud Run / Railway runtime contracts
    after it, so moving the workload between platforms never moves the gate."""
    identity, source = resolve_writer_identity()
    if identity is None:
        return ("runtime writer identity absent (none of "
                f"{', '.join(WRITER_IDENTITY_ENV_VARS)} is set); official writes only "
                f"from {APPROVED_WRITER_IDENTITY!r} (absent identity is not trusted)")
    if identity != APPROVED_WRITER_IDENTITY:
        return (f"runtime writer identity {identity!r} (from {source}) != approved "
                f"{APPROVED_WRITER_IDENTITY!r} (official writes only from the approved "
                "writer; set WORKER_SERVICE_IDENTITY on the sanctioned workload)")
    return None


# --------------------------------------------------------------------------- #
# Gate 3 — module pins
# --------------------------------------------------------------------------- #
# The trust base of the pin gate lives HERE, in the runtime worker itself, NOT in the
# (unpinned) build_stage_b_artifacts.py generator — otherwise a change to that helper
# alongside the manifest could redefine the closure/hash the gate compares against and
# smuggle a truncated or re-canonicalized bundle. The builder is checked against THIS
# list (a stage-B guard test), so the two cannot drift.
EXPECTED_PINNED_MODULES = (
    "src/quadrant_score.py",
    "src/macro_transforms.py",
    "src/macro_sources.py",
    "src/quadrant_confidence.py",
    "src/quadrant_hysteresis.py",
    "src/quadrant_assemble.py",
    "src/quadrant_snapshot.py",
    "src/quadrant_staleness.py",
    # macro_quadrant_us_v3 (owner-approved switch 2026-07-16): the fused decision
    # path's own closure — confidence_v2.0 policy, v2 assembler, the market
    # observation builder, and the v3 chain engine itself.
    "src/quadrant_confidence_v2.py",
    "src/quadrant_assemble_v2.py",
    "src/quadrant_market_observation.py",
    "harness/direct_activation/live_validation.py",
    "harness/phase0q/decision.py",
    "harness/phase0q/decision_v3.py",
    "harness/phase0q/pit.py",
    "harness/phase0q/sleeve.py",
    "scripts/p1_export/export_p1_sources.py",
    "src/input_packs/manifest.py",
    "src/input_packs/hashing.py",
    "src/input_packs/p0_contract.py",
    # The certified-pack registry loader: it now RESOLVES which pack (and which
    # digest) the worker consumes, so it belongs in the same trust closure. The
    # registry DATA file is deliberately not pinned as a module — that is what
    # makes a promotion possible — but the resolved identity is pinned in the
    # `pack` block below and re-verified against the pack manifest and the
    # recomputed tree, so a promotion still has to restamp the pin bundle.
    "src/input_packs/registry.py",
)


def _pins_block_sha256(block: dict) -> str:
    """Canonical sha256 over the {modules, pack} pin block (same routine the builder
    uses, kept HERE so the gate owns its own hash algorithm)."""
    return hashlib.sha256(
        json.dumps(block, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def verify_module_pins(pins: dict[str, Any], root: Path = ROOT) -> None:
    """Raise unless the pin manifest is COMPLETE and intact: the pinned module set is
    exactly the worker-owned closure (``EXPECTED_PINNED_MODULES``), each module's sha256
    (CRLF→LF) matches the tree, the pack block is the certified pack, AND
    ``module_pins_sha256`` recomputes over the {modules, pack} block. A truncated or
    doctored ``module_pins.json`` — one that omits a formula/sleeve module or carries an
    empty ``modules`` object — cannot pass by iterating only the keys it contains and
    then stamp an unchecked ``module_pins_sha256``."""
    modules = pins["modules"]
    if set(modules) != set(EXPECTED_PINNED_MODULES):
        missing = sorted(set(EXPECTED_PINNED_MODULES) - set(modules))
        extra = sorted(set(modules) - set(EXPECTED_PINNED_MODULES))
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
    recomputed = _pins_block_sha256({"modules": modules, "pack": pins["pack"]})
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
    """Explicit override (arg or env) is trusted for any past-or-current BUSINESS day;
    a weekend override returns ``None`` (non-business day, exactly like the auto path),
    otherwise the current America/New_York calendar day, or ``None`` on a weekend.

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
        if resolved < PACK_CUT:
            raise OpenMacroV03Error(
                f"as_of override {resolved.isoformat()} is before the pack cut "
                f"{PACK_CUT.isoformat()}; compose_inputs always loads the certified pack "
                "THROUGH the cut plus the live delta, so a pre-cut replay would be "
                "evaluated with pack data from the future of that as_of")
        if resolved.weekday() >= 5:  # Sat/Sun override: non-business day, never publish
            return None
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
                     as_of: _dt.date, *,
                     degraded_to_center: bool = False) -> dict[str, Any]:
    """Today's consumable allocation with the risk-cap/defensive-floor gates.

    ``degraded_to_center`` (carry_decay_v1, phase0q_005 ratified 2026-07-11): when the
    consumable carry has EXPIRED (calendar age > MAX_CARRY_MONTHS) the published book
    is the mandate-tilted CENTER book (``center_50`` — the centroid of the four
    compressed_50 books through the same constraint machinery), not the stale seed
    quadrant's compressed_50 target. All pricing/constraint gates are identical."""
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
    if degraded_to_center:
        weights = carry_decay.center_book_50(
            sleeve_mod.SleeveParams(candidate_id=CANDIDATE_ID), available)
        book = CENTER_BOOK
    else:
        weights = sleeve_mod.target_weights(
            quadrant, sleeve_mod.SleeveParams(candidate_id=CANDIDATE_ID),
            available, compressed=True)
        book = BOOK
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
        "book": book,
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
    "(as_of, quadrant, decision_validity, carry_seed_as_of, carry_age_months, "
    " carry_expired, candidate_confidence, "
    " coverage_quality, growth_score, inflation_score, input_vintage_sha256, "
    " input_prices_sha256, pack_v2_sha256, module_pins_sha256, judgment_ref, "
    " threshold_ref, code_commit, run_id, publish_state, valid_status, valid_until) "
    "VALUES (%(as_of)s, %(quadrant)s, %(decision_validity)s, %(carry_seed_as_of)s, "
    " %(carry_age_months)s, %(carry_expired)s, "
    " %(candidate_confidence)s, %(coverage_quality)s, %(growth_score)s, "
    " %(inflation_score)s, %(input_vintage_sha256)s, %(input_prices_sha256)s, "
    " %(pack_v2_sha256)s, %(module_pins_sha256)s, %(judgment_ref)s, %(threshold_ref)s, "
    " %(code_commit)s, %(run_id)s, 'published', 'valid', %(valid_until)s) "
    "ON CONFLICT (as_of) DO UPDATE SET "
    " quadrant = EXCLUDED.quadrant, decision_validity = EXCLUDED.decision_validity, "
    " carry_seed_as_of = EXCLUDED.carry_seed_as_of, "
    " carry_age_months = EXCLUDED.carry_age_months, "
    " carry_expired = EXCLUDED.carry_expired, "
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
    " carry_age_months, carry_seed_as_of, carry_expired, "
    " input_prices_sha256, pack_v2_sha256, module_pins_sha256, code_commit, run_id, "
    " publish_state, valid_status, valid_until) "
    "VALUES (%(as_of)s, %(book)s, %(w_spy)s, %(w_tlt)s, %(w_tip)s, %(w_gld)s, "
    " %(w_dbc)s, %(w_shy)s, %(risk_assets_weight)s, %(defensive_assets_weight)s, "
    " %(risk_cap)s, %(defensive_floor)s, %(priced_at)s, "
    " %(carry_age_months)s, %(carry_seed_as_of)s, %(carry_expired)s, "
    " %(input_prices_sha256)s, "
    " %(pack_v2_sha256)s, %(module_pins_sha256)s, %(code_commit)s, %(run_id)s, "
    " 'published', 'valid', %(valid_until)s) "
    "ON CONFLICT (as_of) DO UPDATE SET "
    " book = EXCLUDED.book, w_spy = EXCLUDED.w_spy, w_tlt = EXCLUDED.w_tlt, "
    " w_tip = EXCLUDED.w_tip, w_gld = EXCLUDED.w_gld, w_dbc = EXCLUDED.w_dbc, "
    " w_shy = EXCLUDED.w_shy, risk_assets_weight = EXCLUDED.risk_assets_weight, "
    " defensive_assets_weight = EXCLUDED.defensive_assets_weight, "
    " risk_cap = EXCLUDED.risk_cap, defensive_floor = EXCLUDED.defensive_floor, "
    " priced_at = EXCLUDED.priced_at, "
    " carry_age_months = EXCLUDED.carry_age_months, "
    " carry_seed_as_of = EXCLUDED.carry_seed_as_of, "
    " carry_expired = EXCLUDED.carry_expired, "
    " input_prices_sha256 = EXCLUDED.input_prices_sha256, "
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

# --------------------------------------------------------------------------- #
# Staleness-block RESOLUTION ledger (append-only; schemas/
# open_macro_v03_staleness_resolutions.sql). The block ledger stays IMMUTABLE: a
# clearance is a NEW row here, never an edit or a delete over there.
# --------------------------------------------------------------------------- #
_BLOCK_DETAIL_SQL = (
    "SELECT run_id, input_vintage_sha256, input_prices_sha256 "
    "FROM open_macro_v03_staleness_blocks WHERE as_of = %(as_of)s"
)

# The newest 'resolved' event for a day. Only an operator writes this, and only after
# the worker itself recomputed the freshness report and found NO breach.
_ACTIVE_RESOLUTION_SQL = (
    f"SELECT resolution_id, resolved_by, reason, created_at FROM {RESOLUTIONS_TABLE} "
    "WHERE as_of = %(as_of)s AND resolution_state = 'resolved' "
    "ORDER BY created_at DESC LIMIT 1"
)

_SUPERSEDED_EXISTS_SQL = (
    f"SELECT 1 FROM {RESOLUTIONS_TABLE} "
    "WHERE as_of = %(as_of)s AND resolution_state = 'superseded' LIMIT 1"
)

_RESOLUTION_INSERT_SQL = (
    f"INSERT INTO {RESOLUTIONS_TABLE} "
    "(resolution_id, as_of, resolution_state, resolved_by, reason, freshness_proof, "
    " block_run_id, block_input_vintage_sha256, block_input_prices_sha256, "
    " input_vintage_sha256, input_prices_sha256, pack_v2_sha256, module_pins_sha256, "
    " code_commit, run_id) "
    "VALUES (%(resolution_id)s, %(as_of)s, %(resolution_state)s, %(resolved_by)s, "
    " %(reason)s, %(freshness_proof)s::jsonb, %(block_run_id)s, "
    " %(block_input_vintage_sha256)s, %(block_input_prices_sha256)s, "
    " %(input_vintage_sha256)s, %(input_prices_sha256)s, %(pack_v2_sha256)s, "
    " %(module_pins_sha256)s, %(code_commit)s, %(run_id)s)"
)


def freshness_proof(report: dict[str, Any], as_of: _dt.date) -> dict[str, Any]:
    """The PROOF OF RE-FRESHNESS stored on a resolution event.

    It is the staleness report itself: per-series ``last_available_at`` / ``age_days``
    against the ``bound_days`` that applied, the price ages, the criteria in force, and
    the (necessarily empty, for a resolution) breach list. A reader can therefore verify
    from the ledger alone that the sources really satisfied the thresholds at clearance
    time — the resolution asserts nothing the recomputation did not show."""
    return {
        "verified_as_of": as_of.isoformat(),
        "verified_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "criteria": report["criteria"],
        "series": report["series"],
        "prices": report["prices"],
        "breaches": report["breaches"],
    }


def _exact_numeric(value: Any) -> Any:
    """Python float → ``decimal.Decimal(repr(value))`` for EXACT NUMERIC persistence.

    Postgres casts a float8 parameter to NUMERIC through 15 significant digits, so
    passing a raw Python float silently truncates the stored value (measured in
    production on 2026-07-06: candidate_confidence recomputed 0.8121545618518331 vs
    stored 0.812154561851833 — a Stage C verifier abort). ``repr`` of a float is the
    SHORTEST decimal string that round-trips (17 significant digits when needed), so
    ``float(Decimal(repr(x))) == x`` exactly and the NUMERIC column stores the
    precise float64 the worker computed. ``None`` and non-float values pass through
    unchanged (dates, strings, ints, Decimals)."""
    if isinstance(value, float):
        return decimal.Decimal(repr(value))
    return value


def _exact_numeric_params(params: dict[str, Any]) -> dict[str, Any]:
    """A copy of ``params`` with EVERY float converted via :func:`_exact_numeric` —
    the single write-fidelity chokepoint for the publish/ledger DB parameters."""
    return {key: _exact_numeric(value) for key, value in params.items()}


def publish(conn, decision_row: dict[str, Any], allocation_row: dict[str, Any],
            *, proof: dict[str, Any] | None = None) -> None:
    """Upsert decision + allocation in ONE transaction. A re-run NEVER resurrects an
    invalidated row: the ON CONFLICT WHERE clause skips it (rowcount 0) ⇒ raise.

    Ledger×output mutual exclusion: inside the SAME transaction, a staleness-block
    ledger row for the as_of forbids publishing UNLESS the day carries a 'resolved'
    event in the append-only resolution ledger — the sanctioned operator path
    (``resolve-staleness``), which only writes after the worker itself recomputed the
    freshness report and found no breach. A block is therefore never silently
    outlived by a later run, and it is never edited or deleted either: when the
    publication lands, this function APPENDS the 'superseded' event (once per day,
    the first publication) so the ledger reads block → resolution → output.

    Write fidelity: every float parameter is converted to ``Decimal(repr(x))``
    before reaching the driver (see :func:`_exact_numeric`), so the NUMERIC columns
    store the exact float64 values the worker computed — the float8→numeric
    15-digit truncation can never diverge the published row from the recomputable
    output the Stage C verifier asserts against."""
    decision_row = _exact_numeric_params(decision_row)
    allocation_row = _exact_numeric_params(allocation_row)
    as_of = decision_row["as_of"]
    with conn.cursor() as cur:
        cur.execute(_BLOCK_DETAIL_SQL, {"as_of": as_of})
        block = cur.fetchone()
        if block is not None:
            cur.execute(_ACTIVE_RESOLUTION_SQL, {"as_of": as_of})
            resolution = cur.fetchone()
            if resolution is None:
                raise OpenMacroV03Error(
                    f"publish refused for {as_of}: a staleness-block ledger row exists "
                    "for this day and carries no resolution; publishing after a "
                    "recorded block requires explicit operator resolution — run "
                    "`python -m src.workers.open_macro_v03 resolve-staleness --as-of "
                    f"{as_of} --resolved-by <operator> --reason <why>`, which re-reads "
                    "the inputs and only records the clearance if they are fresh")
            if proof is None:
                raise OpenMacroV03Error(
                    f"publish refused for {as_of}: the day carries a resolved staleness "
                    "block, so the publication must supersede it with the freshness "
                    "report of THIS run; publish(..., proof=...) was not supplied")
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
        if block is not None:
            # Close the loop IN THE SAME TRANSACTION: the resolved block is superseded
            # by real output. Append-only and once per day — a later idempotent re-run
            # of an already-published day adds no second event.
            cur.execute(_SUPERSEDED_EXISTS_SQL, {"as_of": as_of})
            if cur.fetchone() is None:
                block_run_id, block_vintage_sha, block_prices_sha = block
                cur.execute(_RESOLUTION_INSERT_SQL, {
                    "resolution_id": str(uuid.uuid4()),
                    "as_of": as_of,
                    "resolution_state": "superseded",
                    "resolved_by": APPROVED_WRITER_IDENTITY,
                    "reason": (f"published run {decision_row['run_id']} superseded the "
                               "staleness block for this day"),
                    "freshness_proof": json.dumps(proof, sort_keys=True, default=str),
                    "block_run_id": block_run_id,
                    "block_input_vintage_sha256": block_vintage_sha,
                    "block_input_prices_sha256": block_prices_sha,
                    "input_vintage_sha256": decision_row["input_vintage_sha256"],
                    "input_prices_sha256": decision_row["input_prices_sha256"],
                    "pack_v2_sha256": decision_row["pack_v2_sha256"],
                    "module_pins_sha256": decision_row["module_pins_sha256"],
                    "code_commit": decision_row["code_commit"],
                    "run_id": decision_row["run_id"],
                })
    conn.commit()


def record_staleness_block(conn, block_row: dict[str, Any]) -> bool:
    """Write the durable staleness-block ledger row in its own transaction.

    Output×ledger mutual exclusion: inside the SAME transaction, ANY output row for
    the as_of forbids recording a block — a block must never land on top of already
    published output. The ledger is immutable (ON CONFLICT DO NOTHING): returns True
    when this run inserted the row, False when the day's block was already recorded
    (the first record is preserved verbatim). Float parameters (if any ever appear
    in the ledger row) go through the same :func:`_exact_numeric` chokepoint as the
    publish path."""
    block_row = _exact_numeric_params(block_row)
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
def _is_commit_sha(value: str) -> bool:
    return len(value) == 40 and all(c in "0123456789abcdef" for c in value.lower())


def code_commit() -> str:
    """The 40-hex commit sha stamped on every published row and ledger event.

    Platform-neutral: the repo-wide revision env vars (:data:`REVISION_ENV_VARS`) are
    read in order and the git fallback is last — a container image ships without a
    ``.git`` directory, so on Cloud Run the revision MUST come from the env. The value
    is validated: ``code_commit`` is a ``CHAR(40)`` provenance column and a short string
    would be silently blank-padded into a lie about which code produced the row."""
    for name in REVISION_ENV_VARS:
        value = (os.environ.get(name) or "").strip()
        if value:
            if not _is_commit_sha(value):
                raise OpenMacroV03Error(
                    f"{name}={value!r} is not a 40-hex commit sha; the code_commit "
                    "provenance column is CHAR(40) and would store a padded half-truth")
            return value.lower()
    try:
        sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                             capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise OpenMacroV03Error(
            "code_commit unresolved: no revision env var "
            f"({', '.join(REVISION_ENV_VARS)}) is set and git is unavailable here "
            "(a container image has no .git) — set CODE_REVISION on the workload") from exc
    if not _is_commit_sha(sha):
        raise OpenMacroV03Error(f"git rev-parse HEAD returned {sha!r}, not a 40-hex sha")
    return sha.lower()


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
    """Apply the committed BASE DDL files (idempotent). OPS TOOLING ONLY —
    deliberately NOT called by ``run()``: applying schema before verification would
    hand a fresh/partial database old-shaped tables (the base files lack the
    carry_decay_v1 migration) and commit a partial catalog behind the fail-loud
    abort. The orchestrator owns schema lifecycle: base files + the additive
    ``schemas/open_macro_v03_carry_decay_v1_migration.sql`` in a controlled step;
    ``run()``'s Gate 5 is a strictly READ-ONLY ``verify_schema``."""
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
            # carry_decay_v1 (phase0q_005, ratified 2026-07-11): nullable provenance
            # columns added by schemas/open_macro_v03_carry_decay_v1_migration.sql
            # (old rows keep NULLs; the base CREATE TABLE files are byte-pinned and
            # unchanged — the migration is applied by the orchestrator).
            "carry_age_months": ("integer", None, "YES", None),
            "carry_expired": ("boolean", None, "YES", None),
        },
        "constraints": {
            "open_macro_v03_decisions_pkey": ("p", "PRIMARY KEY (as_of)"),
            "open_macro_v03_decisions_check": ("c", "CHECK ((carry_seed_as_of <= as_of))"),
            "open_macro_v03_decisions_quadrant_check": ("c",
                "CHECK ((quadrant = ANY (ARRAY['recovery'::text, 'expansion'::text, "
                "'slowdown'::text, 'contraction'::text])))"),
            # carry_decay_v1: vocabulary widened to admit 'carried_expired'.
            "open_macro_v03_decisions_decision_validity_check": ("c",
                "CHECK ((decision_validity = ANY (ARRAY['fresh'::text, 'carried'::text, "
                "'carried_expired'::text])))"),
            "open_macro_v03_decisions_candidate_confidence_check": ("c",
                "CHECK (((candidate_confidence IS NULL) OR ((candidate_confidence >= "
                "(0)::numeric) AND (candidate_confidence <= (1)::numeric))))"),
            "open_macro_v03_decisions_publish_state_check": ("c",
                "CHECK ((publish_state = ANY (ARRAY['publishing'::text, 'published'::text])))"),
            "open_macro_v03_decisions_valid_status_check": ("c",
                "CHECK ((valid_status = ANY (ARRAY['valid'::text, 'invalidated'::text])))"),
            "open_macro_v03_decisions_invalidation_consistent": ("c",
                "CHECK (((valid_status = 'invalidated'::text) = (invalidated_at IS NOT NULL)))"),
            # carry_decay_v1: carried_expired requires an older seed, like carried.
            "open_macro_v03_decisions_validity_seed": ("c",
                "CHECK ((((decision_validity = 'fresh'::text) AND (carry_seed_as_of = as_of)) "
                "OR ((decision_validity = 'carried'::text) AND (carry_seed_as_of < as_of)) "
                "OR ((decision_validity = 'carried_expired'::text) AND (carry_seed_as_of < as_of))))"),
            # carry_decay_v1: the validity token and the provenance flag never disagree.
            "open_macro_v03_decisions_carry_expired_consistent": ("c",
                "CHECK (((decision_validity = 'carried_expired'::text) = (carry_expired IS TRUE)))"),
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
            # carry_decay_v1 (phase0q_005, ratified 2026-07-11): nullable provenance
            # columns (see the decisions table note; allocations also gain a NULLABLE
            # carry_seed_as_of so a center_50 row is self-describing without a join).
            "carry_age_months": ("integer", None, "YES", None),
            "carry_seed_as_of": ("date", None, "YES", None),
            "carry_expired": ("boolean", None, "YES", None),
        },
        "constraints": {
            "open_macro_v03_allocations_pkey": ("p", "PRIMARY KEY (as_of)"),
            "open_macro_v03_allocations_as_of_fkey": ("f",
                "FOREIGN KEY (as_of) REFERENCES open_macro_v03_decisions(as_of)"),
            # carry_decay_v1: vocabulary widened to admit the degraded CENTER book.
            "open_macro_v03_allocations_book_check": ("c",
                "CHECK ((book = ANY (ARRAY['compressed_50'::text, 'center_50'::text])))"),
            # carry_decay_v1: the degraded book and the provenance flag never disagree.
            "open_macro_v03_allocations_center_book_consistent": ("c",
                "CHECK (((book = 'center_50'::text) = (carry_expired IS TRUE)))"),
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
    # Append-only clearance ledger (schemas/open_macro_v03_staleness_resolutions.sql).
    # Verified like every other table the worker touches: an absent or drifted ledger
    # fails loud BEFORE any write, so the recovery path can never be half-installed.
    RESOLUTIONS_TABLE: {
        "columns": {
            "resolution_id": ("uuid", None, "NO", None),
            "as_of": ("date", None, "NO", None),
            "resolution_state": ("text", None, "NO", None),
            "resolved_by": ("text", None, "NO", None),
            "reason": ("text", None, "NO", None),
            "freshness_proof": ("jsonb", None, "NO", None),
            "block_run_id": ("text", None, "NO", None),
            "block_input_vintage_sha256": ("character", 64, "NO", None),
            "block_input_prices_sha256": ("character", 64, "NO", None),
            "input_vintage_sha256": ("character", 64, "NO", None),
            "input_prices_sha256": ("character", 64, "NO", None),
            "pack_v2_sha256": ("character", 64, "NO", None),
            "module_pins_sha256": ("character", 64, "NO", None),
            "code_commit": ("character", 40, "NO", None),
            "run_id": ("text", None, "NO", None),
            "created_at": ("timestamp with time zone", None, "NO", "now()"),
        },
        "constraints": {
            "open_macro_v03_staleness_resolutions_pkey": ("p", "PRIMARY KEY (resolution_id)"),
            "open_macro_v03_staleness_resolutions_as_of_fkey": ("f",
                "FOREIGN KEY (as_of) REFERENCES open_macro_v03_staleness_blocks(as_of)"),
            "open_macro_v03_staleness_resolutions_state_check": ("c",
                "CHECK ((resolution_state = ANY (ARRAY['resolved'::text, "
                "'superseded'::text])))"),
            "open_macro_v03_staleness_resolutions_proof_object": ("c",
                "CHECK ((jsonb_typeof(freshness_proof) = 'object'::text))"),
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

    # Gate 2b — WRITER runtime identity (no DB): official rows only from the approved
    # Railway service (the monitor, a separate service, does not enforce this).
    reason = check_writer_runtime()
    if reason is not None:
        return {"status": "wrong_service", "reason": reason}

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

            # Gate 5 — READ-ONLY catalog verification against the committed DDL
            # expectations (the B1b evidence base). run() deliberately does NOT call
            # ensure_schema: executing the base CREATE TABLE files before verifying
            # would give a missing/partially-initialized database old-shaped tables
            # (the base files lack the carry_decay_v1 migration, so the result could
            # never verify anyway) and leave a committed partial catalog behind the
            # abort. Schema lifecycle — base DDL AND the additive migration
            # (schemas/open_macro_v03_carry_decay_v1_migration.sql) — belongs to the
            # ORCHESTRATOR's controlled apply step, exactly like the migration
            # itself. An absent or unmigrated catalog therefore fails loud HERE with
            # zero mutating statements and zero schema/data commits.
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
                        # the recovery path is NAMED in the result, so an operator
                        # reading a blocked run never has to invent one (the
                        # 2026-07-17 incident was cleared by hand over SSH).
                        "resolution_path": (
                            "python -m src.workers.open_macro_v03 resolve-staleness "
                            f"--as-of {as_of_date.isoformat()} --resolved-by <operator> "
                            "--reason <what refreshed>"),
                        "wall_ms": int((time.monotonic() - t0) * 1000)}

            # Gate 9 — decision chain + consumable position. The GO candidate
            # macro_quadrant_us_v3 (docs/calibration/
            # open_macro_v03_confidence_v3_evidence_001.md — every ratified gate GO,
            # owner-approved model switch 2026-07-16): the market-fused v3 chain
            # consumes the SAME composed macro vintages plus the composed eod
            # prices (the market growth sensor reads PIT off the same price
            # surface the staleness gate and the allocator already verify).
            chain = decision_mod.run_decision_series_v3(
                vintage_rows, price_rows, CHAIN_START, as_of_date)
            last, validity, seed_as_of = consumable_today(chain, as_of_date)

            # carry_decay_v1 (phase0q_005, RATIFIED 2026-07-11; CARRY_DECAY_V1_ACTIVE
            # ships True): calendar-month carry age of the consumable seed. A carry
            # older than MAX_CARRY_MONTHS degrades the PUBLISHED position to the
            # mandate-tilted CENTER book with decision_validity 'carried_expired' and
            # honest provenance columns on BOTH rows. The seed quadrant stays on the
            # decision row as the reference (the quadrant CHECK vocabulary is
            # unchanged); the allocation book says 'center_50'. The additive migration
            # (schemas/open_macro_v03_carry_decay_v1_migration.sql) must have been
            # applied by the orchestrator — verify_schema (Gate 5) and the INSERTs
            # fail loud against an unmigrated catalog, never writing a disguised row.
            carry = carry_decay.carry_provenance(chain, as_of_date)
            degraded = bool(carry_decay.CARRY_DECAY_V1_ACTIVE and carry["carry_expired"])
            if degraded:
                validity = "carried_expired"

            # Gate 10 — allocation (CENTER book when the carry expired).
            allocation = build_allocation(last.quadrant, price_rows, as_of_date,
                                          degraded_to_center=degraded)
            weights = allocation["weights"]

            vu = valid_until(as_of_date)
            decision_row = {
                "as_of": as_of_date,
                "quadrant": last.quadrant,
                "decision_validity": validity,
                "carry_seed_as_of": seed_as_of,
                # published carry provenance: carry_expired mirrors the ENFORCED
                # state (== degraded), so the carried_expired<->carry_expired
                # consistency CHECK always holds on the row.
                "carry_age_months": carry["carry_age_months"],
                "carry_expired": degraded,
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
                "book": allocation["book"],
                "w_spy": weights["SPY"], "w_tlt": weights["TLT"], "w_tip": weights["TIP"],
                "w_gld": weights["GLD"], "w_dbc": weights["DBC"], "w_shy": weights["SHY"],
                "risk_assets_weight": allocation["risk_assets_weight"],
                "defensive_assets_weight": allocation["defensive_assets_weight"],
                "risk_cap": sleeve_mod.RISK_CAP_BASELINE,
                "defensive_floor": sleeve_mod.DEFENSIVE_FLOOR_BASELINE,
                "priced_at": allocation["priced_at"],
                "carry_age_months": carry["carry_age_months"],
                "carry_seed_as_of": seed_as_of,
                "carry_expired": degraded,
                "input_prices_sha256": input_prices_sha256,
                "pack_v2_sha256": PACK_SHA256_PIN,
                "module_pins_sha256": module_pins_sha256,
                "code_commit": commit,
                "run_id": run_id,
                "valid_until": vu,
            }

            # Gate 11 — publish (atomic). The freshness report of THIS run travels
            # with the write: if the day carries a resolved staleness block, publish()
            # appends the 'superseded' event with this proof attached.
            publish(conn, decision_row, allocation_row,
                    proof=freshness_proof(report, as_of_date))

            # Gate 12 — post-write verify.
            post_write_verify(conn, as_of_date, weights)

            return {
                "status": "published",
                "as_of": as_of_date.isoformat(),
                "quadrant": last.quadrant,
                "decision_validity": validity,
                "book": allocation["book"],
                "weights": weights,
                "run_id": run_id,
                "wall_ms": int((time.monotonic() - t0) * 1000),
                # carry_decay_v1 (phase0q_005, ratified 2026-07-11): ENFORCED carry
                # provenance — what was actually published, mirrored on the DB rows.
                "carry_provenance": {
                    "carry_policy": carry["carry_policy"],
                    "carry_seed_as_of": carry["carry_seed_as_of"].isoformat(),
                    "carry_age_months": carry["carry_age_months"],
                    "carry_expired": degraded,
                    "degraded_to_center": degraded,
                    "max_carry_months": carry["max_carry_months"],
                    "carry_decay_active": carry_decay.CARRY_DECAY_V1_ACTIVE,
                    "note": ("carry_decay_v1 ACTIVE: an expired carry publishes the "
                             "mandate-tilted center_50 book with decision_validity "
                             "carried_expired and provenance columns on both rows"),
                },
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


# --------------------------------------------------------------------------- #
# resolve-staleness CLI (the SANCTIONED recovery path for a blocked day)
# --------------------------------------------------------------------------- #
def resolve_staleness_block(dsn: str, *, as_of: str, resolved_by: str,
                            reason: str) -> dict[str, Any]:
    """Clear a recorded staleness block for ``as_of`` by APPENDING a 'resolved' event.

    This is the operator path the block message has always demanded and the codebase
    never implemented — the 2026-07-17 incident was cleared with an ad-hoc SQL session
    over SSH. It is executable from anywhere the worker image runs (a Cloud Run job
    execution with overridden args, a Railway one-off, a local run against the DSN);
    no shell on the database host is involved.

    The clearance is NOT a rubber stamp. The same gates as ``run()`` apply, in the same
    fail-closed order and with the same zero-side-effect-before-a-gate property (flag,
    governance envelope, WRITER runtime identity — all before any DB access; module
    pins and pack bytes before the inputs are trusted), and then, under the SAME
    advisory lock the daily run uses, the worker RE-READS the inputs for that day and
    RECOMPUTES the staleness report. The event is written only when the report is
    breach-free, and it carries that report as ``freshness_proof`` — the per-source
    timestamps and ages against the thresholds in force at clearance time.

    Nothing is mutated: the original block row stays verbatim in its immutable ledger,
    and a day already carrying a 'resolved' event returns ``already_resolved`` instead
    of stacking a second clearance. The corresponding publication is a separate,
    ordinary run (``OPEN_MACRO_V03_AS_OF=<as_of>``), which appends the 'superseded'
    event when it lands."""
    # Gate 1 — feature flag (no DB).
    if os.environ.get(RUNTIME_FLAG_ENV) != "true":
        return {"status": "flag_off"}

    # Gate 2 — governance envelope (no DB).
    envelope = _load_json(ENVELOPE_PATH)
    blocked = check_governance(envelope)
    if blocked is not None:
        return {"status": "governance_blocked", "reason": blocked}

    # Gate 2b — WRITER runtime identity (no DB). A clearance is a write on the
    # official ledger surface: only the approved writer may emit one.
    blocked = check_writer_runtime()
    if blocked is not None:
        return {"status": "wrong_service", "reason": blocked}

    if not (resolved_by or "").strip():
        raise OpenMacroV03Error("resolve-staleness requires a named --resolved-by "
                                "(an anonymous clearance is not a resolution)")
    if not (reason or "").strip():
        raise OpenMacroV03Error("resolve-staleness requires a --reason")

    # Gate 3 / 3b — module pins + pack bytes (no DB): the recomputation that produces
    # the proof must run on the pinned formulas and the certified pack bytes.
    pins = _load_json(PINS_PATH)
    verify_module_pins(pins, ROOT)
    module_pins_sha256 = pins["module_pins_sha256"]
    verify_pack_bytes()

    # Gate 6 — as_of (explicit here; the same future/pre-cut/business-day rules).
    as_of_date = resolve_as_of(as_of)
    if as_of_date is None:
        return {"status": "non_business_day", "as_of": as_of}

    commit = code_commit()
    run_id = f"open_macro_v03_resolve-{as_of_date.isoformat()}-{uuid.uuid4().hex[:8]}"

    conn = connect(dsn)
    try:
        pin_search_path(conn)
        with advisory_lock(conn, LOCK_OPEN_MACRO_V03) as got:
            if not got:
                return {"status": "lock_busy"}

            # Gate 5 — READ-ONLY catalog verification (includes the resolution ledger).
            verify_schema(conn)

            with conn.cursor() as cur:
                cur.execute(_BLOCK_DETAIL_SQL, {"as_of": as_of_date})
                block = cur.fetchone()
                if block is None:
                    conn.commit()
                    return {"status": "no_block", "as_of": as_of_date.isoformat(),
                            "detail": "no staleness-block ledger row for this day; "
                                      "there is nothing to resolve"}
                cur.execute(_ACTIVE_RESOLUTION_SQL, {"as_of": as_of_date})
                existing = cur.fetchone()
            conn.commit()
            if existing is not None:
                return {"status": "already_resolved", "as_of": as_of_date.isoformat(),
                        "resolution_id": str(existing[0]), "resolved_by": existing[1],
                        "detail": "the ledger is append-only; a second clearance for "
                                  "the same day is not recorded"}

            # PROOF — re-read the inputs and recompute freshness for that day.
            vintage_rows, price_rows = compose_inputs(conn, as_of_date)
            report = staleness_report(vintage_rows, price_rows, as_of_date)
            proof = freshness_proof(report, as_of_date)
            if report["breaches"]:
                return {"status": "still_stale", "as_of": as_of_date.isoformat(),
                        "reason": report["breaches"], "run_id": run_id,
                        "detail": "the inputs still breach the staleness SLO; no "
                                  "resolution recorded (re-run after the sources "
                                  "refresh)"}

            block_run_id, block_vintage_sha, block_prices_sha = block
            resolution_id = str(uuid.uuid4())
            with conn.cursor() as cur:
                cur.execute(_RESOLUTION_INSERT_SQL, {
                    "resolution_id": resolution_id,
                    "as_of": as_of_date,
                    "resolution_state": "resolved",
                    "resolved_by": resolved_by.strip(),
                    "reason": reason.strip(),
                    "freshness_proof": json.dumps(proof, sort_keys=True, default=str),
                    "block_run_id": block_run_id,
                    "block_input_vintage_sha256": block_vintage_sha,
                    "block_input_prices_sha256": block_prices_sha,
                    "input_vintage_sha256": _canonical_sha256(vintage_rows),
                    "input_prices_sha256": _canonical_sha256(price_rows),
                    "pack_v2_sha256": PACK_SHA256_PIN,
                    "module_pins_sha256": module_pins_sha256,
                    "code_commit": commit,
                    "run_id": run_id,
                })
            conn.commit()
            return {"status": "resolved", "as_of": as_of_date.isoformat(),
                    "resolution_id": resolution_id, "resolved_by": resolved_by.strip(),
                    "reason": reason.strip(), "run_id": run_id,
                    "block_run_id": block_run_id,
                    "freshness_proof": proof,
                    "next_step": ("publish the day with a normal run: "
                                  f"OPEN_MACRO_V03_AS_OF={as_of_date.isoformat()}")}
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
    res = sub.add_parser(
        "resolve-staleness",
        help="operator path: append a resolution event for a blocked day (proof required)")
    res.add_argument("--as-of", dest="as_of", required=True, help="blocked day (YYYY-MM-DD)")
    res.add_argument("--resolved-by", dest="resolved_by", required=True,
                     help="named operator emitting the resolution")
    res.add_argument("--reason", dest="reason", required=True,
                     help="why the day is resolvable (what refreshed)")
    args = parser.parse_args(argv)
    if args.command == "invalidate":
        stats = invalidate(resolve_dsn(), as_of=args.as_of, to=args.to, reason=args.reason)
        print(json.dumps(stats, default=str))
        return 0
    if args.command == "resolve-staleness":
        stats = resolve_staleness_block(resolve_dsn(), as_of=args.as_of,
                                        resolved_by=args.resolved_by, reason=args.reason)
        print(json.dumps(stats, default=str))
        return 0 if stats.get("status") in ("resolved", "already_resolved") else 1
    parser.error("no command (use: invalidate | resolve-staleness)")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
