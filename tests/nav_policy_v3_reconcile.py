"""Test-only first-release reconciliation, Round7: retained E23 -> live Round7.

NOT a classifier, a publication shortcut or an authorization token. It is
the operational gate of the FIRST Round7 publication only: an authorized
custodian proves that the SEC exclusions of the newly generated live policy
(``E_live``) differ from the historical, exactly replayed exclusions (``E23``,
retained v2 bundle judged by v3 at the ORIGINAL v2 decision instant) only by
funds that aged into ``sec.stale`` inside the approved window (``Delta``) and
funds that were re-listed fresh since the v2 decision (``R``); every other
decision, reason, frequency and flag is unchanged. Production scripts never
import this module; the operator never receives its report.

Inputs (read-only, every file pinned by an externally supplied SHA-256 from
the chain of custody, never self-approved): the retained v2 bundle (policy,
source snapshot, capture, dossier) with the v2 audit config its dossier
embeds, and the live Round7 bundle (policy, source snapshot, capture, strict
dossier, canary manifest, previous v1 policy) with the repository audit
config (its bytes must equal the operator's pinned repository file). The
expected-counts document is the fixed replay fixture (no override). No DSN,
network, tolerance, timestamp override or manual fund list exists.

Core (``reconcile``), all in memory:

1. the existing replay (``tests.nav_policy_v3_replay``) must PASS exactly; its
   in-memory reconstruction provides E23 by identifier with its reasons;
2. the live bundle is validated with the operator's own receipt validators
   (Round7 contract/config/code/capture/manifest, strict all-PASS) and
   re-audited offline with the independent auditor (``audit``): the
   reconstructed dossier and manifest must be byte-identical to the pinned
   ones; the live verdict is recomputed at the live ``generated_at``;
3. ``tau_v2 <= tau_live <= captured_at_live`` (aware UTC instants from the
   documents, never the tool clock);
4. ``R = E23 - E_live``, ``Delta = E_live - E23``, ``I = E23 & E_live``;
   ``E_live == (E23 - R) | Delta`` and every ``I`` member keeps its reason;
5. every retained identifier (ACTIVE, Delta and R included) keeps its complete
   normalized pre-SEC source/cardinality/claim/ownership state. Every
   ``Delta`` member was ACTIVE (SEC-matched) at ``tau_v2``, is ``sec.stale`` at
   ``tau_live``, and has exact SEC related-row identity continuity: every
   historical related row remains, no new related row appears and every
   timestamp is nondecreasing. Only then, its last retained listing ``t_last``
   must satisfy ``tau_v2 - 7d < t_last <= tau_live - 7d``;
6. every ``R`` member is ACTIVE at ``tau_live`` through a fresh, complete,
   consistent row newer than ``tau_v2``, with an unchanged registry key;
7. universe, pre-SEC candidates ``C``, ``P`` and INACTIVE are unchanged and no
   other decision moved; counts are conserved (ACTIVE/UNKNOWN/INACTIVE,
   stale/missing/integrity, ``C``, the live A8 exclusions, the daily subset)
   and A7 still passes.

Output: a CLOSED, sanitized document (static codes, booleans, counts, UTC
instants and SHA-256 digests only; no identifier, ticker, class, series,
path, source row or exception text), validated before anything is printed or
written; written once through the private POSIX custody writer (0600,
atomic, never overwritten). Exit 0 PASS, 3 STOP (mismatch, sanitized report
written), 2 blocked (invalid input/provenance/output: a two-key envelope
``{"status": "blocked", "code": <static code>}``, nothing written).

Run from the repository root under POSIX (e.g. the audit runner container)::

    python -m tests.nav_policy_v3_reconcile \\
        --v2-policy-file V/policy-v2.json --v2-policy-sha256 H \\
        --v2-source-snapshot-file V/source-v2.json --v2-source-snapshot-sha256 H \\
        --v2-capture-file V/capture.json --v2-capture-sha256 H \\
        --v2-audit-file V/audit.json --v2-audit-sha256 H \\
        --live-policy-file L/policy-v3.json --live-policy-sha256 H \\
        --live-source-snapshot-file L/source-v3.json \\
        --live-source-snapshot-sha256 H \\
        --live-capture-file L/capture-v3-round7.json --live-capture-sha256 H \\
        --live-audit-file L/audit-v3-round7.json --live-audit-sha256 H \\
        --live-canary-file L/canary-v3-round7.json --live-canary-sha256 H \\
        --live-previous-policy-file L/policy-v1.json \\
        --live-previous-policy-sha256 H \\
        --v2-audit-config configs/nav_identity_audit_v2.json \\
        --audit-config configs/nav_identity_audit_v3.json \\
        --custody-root R --output R/reconcile-round7.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

from scripts import fund_nav_readiness_schema as operator
from scripts import generate_fund_nav_policy_v1 as generator
from scripts import verify_fund_nav_identity_v2 as verifier
from tests import nav_policy_v3_replay as replay

ROOT = replay.ROOT
KIND = "nav-policy-v3-first-release-reconciliation-round7"
# The historical replay fixture, fixed (no override in the live reconciliation).
EXPECTED_PATH = replay.DEFAULT_EXPECTED
CODE_FILES = (
    *replay.CODE_FILES,
    "scripts/fund_nav_readiness_schema.py",
    "tests/nav_policy_v3_reconcile.py",
)
V2_KEYS = ("policy", "source_snapshot", "capture", "audit")
LIVE_KEYS = (
    "policy",
    "source_snapshot",
    "capture",
    "audit",
    "canary",
    "previous_policy",
)
INPUT_KEYS = (
    "v2_policy",
    "v2_source_snapshot",
    "v2_capture",
    "v2_audit",
    "v2_audit_config",
    "live_policy",
    "live_source_snapshot",
    "live_capture",
    "live_audit",
    "live_canary",
    "live_previous_policy",
    "audit_config",
    "expected_counts",
)
TIME_KEYS = (
    "tau_v2",
    "tau_live",
    "captured_at_live",
    "aging_lower_exclusive",
    "aging_upper_inclusive",
)
CHECK_NAMES = (
    "historical_replay_exact",
    "live_round7_strict_bound",
    "universe_and_pre_sec_unchanged",
    "intersection_reasons_unchanged",
    "additions_stale_only",
    "additions_aging_window",
    "removals_fresh_relisted",
    "only_sec_movements",
    "counts_conserved",
    "a7_minimums_preserved",
)
COUNT_KEYS = (
    "e23",
    "e_live",
    "retained",
    "added_stale",
    "recovered",
    "recovered_stale",
    "recovered_missing",
    "invalid_additions",
    "invalid_removals",
    "reason_changes",
    "non_sec_changes",
    "c_size",
    "bound",
    "stale",
    "missing",
    "integrity",
    "active",
    "unknown",
    "inactive",
    "structural_daily",
    "active_daily",
    "added_daily",
    "recovered_daily",
)
PUBLIC_KEYS = (
    "kind",
    "status",
    "audit_contract_version",
    "audit_contract_sha256",
    "inputs",
    "code_sha256",
    "times",
    "checks",
    "counts",
)
STALE, MISSING = verifier.SEC_STALE, verifier.SEC_MISSING
MAX_AGE = verifier.SEC_MAX_AGE
EVIDENCE_FIELDS = (
    "fund_status",
    "valuation_frequency",
    "identity_verified",
    "currency_verified",
    "return_basis_verified",
)
# Blocked codes: lowercase static tokens, at most two ``prefix:`` segments.
_STATIC_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}(?::[a-z][a-z0-9_]{0,63}){0,2}")
BLOCK_PREFIXES = ("auditor", "operator", "generator", "replay_config_invalid")
OWN_CODES = frozenset(
    {
        "reconcile_bundle_invalid",
        "reconcile_input_invalid",
        "reconcile_input_unreadable",
        "reconcile_time_invalid",
        "reconcile_time_order_invalid",
        "reconcile_output_not_sanitized",
        "reconcile_code_not_allowlisted",
        "reconcile_cli_invalid",
        "audit_config_pin_unreadable",
        "audit_config_not_repository_pin",
        "live_policy_not_canonical",
        "live_audit_not_canonical",
        "live_canary_not_canonical",
        "live_previous_policy_mismatch",
        "live_source_snapshot_mismatch",
        "live_sec_source_invalid",
    }
)


class ReconcileBlocked(Exception):
    """Static code: an input, pin, provenance or output problem (exit 2)."""


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ReconcileBlocked(code)


def _static(prefix: str | None, exc: BaseException) -> str:
    """A delegated exception as an allowlisted static code (never its text)."""
    text = str(exc)
    code = text if _STATIC_CODE.fullmatch(text) else "invalid"
    return code if prefix is None else f"{prefix}:{code}"


def blocked_code(code: str) -> str:
    """Only allowlisted static codes leave the tool."""
    head = code.split(":", 1)[0]
    if (
        isinstance(code, str)
        and _STATIC_CODE.fullmatch(code)
        and (code in OWN_CODES or head in BLOCK_PREFIXES or ":" not in code)
        and replay._public_problem({"code": code}) is None
    ):
        return code
    return "reconcile_code_not_allowlisted"


def _aware(text: object, code: str) -> dt.datetime:
    if not isinstance(text, str):
        raise ReconcileBlocked(code)
    try:
        value = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ReconcileBlocked(code) from exc
    if value.tzinfo is None or value.utcoffset() is None:
        raise ReconcileBlocked(code)
    return value.astimezone(dt.timezone.utc)


def _canonical(raw: bytes, code: str, not_canonical: str) -> dict:
    try:
        document = operator._strict_object(raw, code)
    except ValueError as exc:
        raise ReconcileBlocked(_static("operator", exc)) from exc
    _require(raw == operator._document_bytes(document), not_canonical)
    return document


# ── live Round7 bundle: operator validators + independent offline re-audit ──
def _verify_live(bundle: dict[str, bytes], config_raw: bytes) -> dict:
    try:
        pinned = operator.AUDIT_CONFIG.read_bytes()
    except OSError as exc:
        raise ReconcileBlocked("audit_config_pin_unreadable") from exc
    _require(config_raw == pinned, "audit_config_not_repository_pin")
    policy_raw = bundle["policy"]
    try:
        policy, _raw = operator._policy(policy_raw)
    except (ValueError, KeyError, TypeError) as exc:
        raise ReconcileBlocked(_static("operator", exc)) from exc
    _require(
        policy_raw == operator._document_bytes(policy), "live_policy_not_canonical"
    )
    policy_sha = _sha(policy_raw)
    dossier = _canonical(
        bundle["audit"], "audit_dossier_invalid", "live_audit_not_canonical"
    )
    capture = _canonical(
        bundle["capture"], "audit_capture_mismatch", "audit_capture_mismatch"
    )
    manifest = _canonical(
        bundle["canary"], "canary_manifest_invalid", "live_canary_not_canonical"
    )
    try:
        operator._validate_dossier(dossier, policy, policy_sha)
        members = operator._validate_capture(
            capture, bundle["capture"], dossier, policy
        )
        operator._validate_manifest(
            manifest,
            dossier,
            _sha(bundle["audit"]),
            _sha(bundle["capture"]),
            policy,
            policy_sha,
            members,
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise ReconcileBlocked(_static("operator", exc)) from exc
    inputs = dossier["inputs"]
    _require(
        inputs["previous_policy_sha256"] == _sha(bundle["previous_policy"]),
        "live_previous_policy_mismatch",
    )
    _require(
        inputs["source_snapshot_file_sha256"] == _sha(bundle["source_snapshot"]),
        "live_source_snapshot_mismatch",
    )
    try:
        config = verifier._load_config(config_raw)
        report = verifier.audit(
            policy_raw,
            snapshot_raw=bundle["source_snapshot"],
            capture_raw=bundle["capture"],
            previous_raw=bundle["previous_policy"],
            config_raw=config_raw,
        )
        strict = verifier.verdict_of(report, strict=True)
        report["result"] = "pass" if strict else "fail"
        report["strict"] = True
        report["canary_requested"] = True
        content = verifier.report_bytes(report)
        manifest_equal = False
        if strict:
            canary = verifier.build_canary(
                report, policy_raw, config, audit_report_sha256=_sha(content)
            )
            manifest_equal = (
                json.dumps(canary, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode("ascii") == bundle["canary"]
        _snapshot, sources = verifier._load_snapshot(bundle["source_snapshot"])
    except verifier.AuditBlocked as exc:
        raise ReconcileBlocked(_static("auditor", exc)) from exc
    tau = _aware(policy["generation"].get("generated_at"), "reconcile_time_invalid")
    captured = _aware(capture.get("captured_at"), "reconcile_time_invalid")
    judge = verifier.SecJudge(sources["sec"])
    _require(judge.source_problem(tau) is None, "live_sec_source_invalid")
    catalog = verifier.Catalog(sources)
    verdict = verifier.classify(catalog, judge, tau)
    return {
        "tau": tau,
        "captured": captured,
        "policy": policy,
        "dossier": dossier,
        "report": report,
        "strict": strict,
        "dossier_equal": content == bundle["audit"],
        "manifest_equal": manifest_equal,
        "sec_rows_equal": sorted(map(replay._compact, sources["sec"]))
        == sorted(map(replay._compact, capture["sec"]["rows"])),
        "judge": judge,
        "catalog": catalog,
        "verdict": verdict,
    }


# ── per-identifier helpers (identifiers never leave memory) ─────────────────
def _sec_key(catalog, uid: str):
    rows = catalog.rows["reg"].get(uid, [])
    if len(rows) != 1:
        return None
    reg = rows[0]
    return (
        verifier._upper(reg["ticker"]),
        verifier._upper(reg["sec_series_id"]),
        verifier._upper(reg["sec_class_id"]),
    )


def _normalized_rows(catalog, uid: str, source: str) -> tuple:
    """Closed normalized rows for one pre-SEC source family (private only)."""
    fields = {
        "iu": (
            ("instrument_type", verifier._clean),
            ("ticker", verifier._upper),
            ("isin", verifier._upper),
            ("currency", verifier._clean),
            ("is_active", lambda value: value),
        ),
        "fv": (
            ("series_id", verifier._upper),
            ("ticker", verifier._upper),
            ("isin", verifier._upper),
            ("cusip", verifier._upper),
            ("currency", verifier._clean),
            ("fund_type", verifier._clean),
        ),
        "reg": (
            ("sec_series_id", verifier._upper),
            ("sec_class_id", verifier._upper),
            ("ticker", verifier._upper),
            ("isin", verifier._upper),
            ("cusip_9", verifier._upper),
            ("figi", verifier._upper),
            ("resolution_status", verifier._clean),
            ("conflict_state", replay._compact),
        ),
    }[source]
    projected = [
        tuple(normalize(row[name]) for name, normalize in fields)
        for row in catalog.rows[source].get(uid, [])
    ]
    return tuple(sorted(projected, key=replay._compact))


def _owner_state(catalog, uid: str) -> tuple:
    """Exact global claim ownership relevant to ``uid`` (private only)."""
    state = []
    for kind, claims in catalog.owner.items():
        for claim, owners in claims.items():
            if uid in owners:
                state.append((kind, claim, tuple(sorted(owners))))
    return tuple(sorted(state, key=replay._compact))


def _pre_sec_identity(catalog, uid: str) -> tuple:
    """Canonical complete pre-SEC identity input for one retained UUID.

    This includes source presence/cardinality, every normalized source field
    used by the pre-SEC gates, exact claim ownership, and the independently
    derived pre-SEC failures. It deliberately excludes SEC listings and their
    timestamps. The projection never leaves process memory.
    """
    return (
        "pre-sec-identity-v1",
        uid in catalog.universe(),
        catalog.inactive(uid),
        _normalized_rows(catalog, uid, "iu"),
        _normalized_rows(catalog, uid, "fv"),
        _normalized_rows(catalog, uid, "reg"),
        _owner_state(catalog, uid),
        tuple(sorted(catalog.failures(uid))),
    )


def _related(judge, key) -> set:
    """Positions related to a fund by the SAME rule as the SEC judge."""
    ticker, _series, declared = key
    positions = set(judge.by_ticker.get(ticker, set()))
    if declared is not None:
        positions |= judge.by_class.get(declared, set())
    return positions


def _related_rows(judge, key) -> dict | None:
    """Canonical SEC row identity -> timestamp for rows related by the judge.

    The source relation has ``class_id`` as its key. A duplicate canonical row
    identity is therefore not continuity evidence and returns ``None``.
    """
    rows = {}
    for position in _related(judge, key):
        identity = judge.keys[position]
        if identity in rows:
            return None
        rows[identity] = judge.instants[position]
    return rows


def _fresh(judge, position: int, at: dt.datetime) -> bool:
    return dt.timedelta(0) <= at - judge.instants[position] <= MAX_AGE


def _sec_excluded(verdict: dict) -> dict:
    return {
        uid: entry["first"]
        for uid, entry in verdict.items()
        if entry["status"] == "UNKNOWN" and entry["first"] in verifier.SEC_CODES
    }


def _candidates(verdict: dict) -> set:
    return {
        uid
        for uid, entry in verdict.items()
        if entry["status"] == "ACTIVE"
        or (entry["status"] == "UNKNOWN" and entry["first"] in verifier.SEC_CODES)
    }


def _structural_daily(verdict: dict, catalog) -> set:
    return {
        uid
        for uid, entry in verdict.items()
        if entry["status"] != "INACTIVE"
        and not catalog.failures(uid, claims=False)
        and catalog.fund_type(uid) in verifier.DAILY_TYPES
    }


def _status_set(verdict: dict, status: str) -> set:
    return {uid for uid, entry in verdict.items() if entry["status"] == status}


def _daily_active(verdict: dict) -> set:
    return {
        uid
        for uid, entry in verdict.items()
        if entry["status"] == "ACTIVE" and entry["daily"]
    }


def _addition(uid: str, past: dict, live: dict, times: dict) -> tuple[bool, bool]:
    """``(stale_only, aging_window)`` for one ``Delta`` member."""
    before, after = past["verdict"].get(uid), live["verdict"].get(uid)
    key_v2, key_live = _sec_key(past["catalog"], uid), _sec_key(live["catalog"], uid)
    stale_only = (
        before is not None
        and before["status"] == "ACTIVE"
        and after is not None
        and after["status"] == "UNKNOWN"
        and after["first"] == STALE
        and key_v2 is not None
        and key_v2 == key_live
    )
    if not stale_only:
        return False, False
    v2_judge, live_judge = past["judge"], live["judge"]
    code, matched = v2_judge.judge(*key_v2, times["tau_v2"])
    related_v2 = _related_rows(v2_judge, key_v2)
    related_live = _related_rows(live_judge, key_live)
    if code is not None or matched is None or not related_v2 or not related_live:
        return True, False
    matched_rows = {
        identity: instant
        for identity, instant in related_v2.items()
        if dt.timedelta(0) <= times["tau_v2"] - instant <= MAX_AGE
    }
    continuity = (
        related_v2.keys() == related_live.keys()
        and all(related_live[row] >= instant for row, instant in related_v2.items())
        and all(
            row in related_live and related_live[row] >= instant
            for row, instant in matched_rows.items()
        )
    )
    if not continuity or len(matched_rows) != 1:
        return True, False
    # Only retained historical identities may provide the aging timestamp.
    t_last = max(related_live[row] for row in related_v2)
    window = (
        times["aging_lower_exclusive"] < t_last <= times["aging_upper_inclusive"]
        # The classifier itself must say stale (7 days exactly stays fresh).
        and all(
            times["tau_live"] - instant > MAX_AGE for instant in related_live.values()
        )
    )
    return True, window


def _removal(uid: str, past: dict, live: dict, times: dict) -> bool:
    """One ``R`` member: re-listed fresh since tau_v2 under the same identity."""
    before, after = past["verdict"].get(uid), live["verdict"].get(uid)
    key_v2, key_live = _sec_key(past["catalog"], uid), _sec_key(live["catalog"], uid)
    if (
        before is None
        or before["status"] != "UNKNOWN"
        or before["first"] not in verifier.SEC_CODES
        or after is None
        or after["status"] != "ACTIVE"
        or key_v2 is None
        or key_v2 != key_live
    ):
        return False
    code, instant = live["judge"].judge(*key_live, times["tau_live"])
    return code is None and instant is not None and instant > times["tau_v2"]


def _non_sec_changes(past: dict, live: dict, moved: set, retained: set) -> int:
    """Complete pre-SEC state plus non-moved decisions/frequencies/flags."""
    past_rows = {row["instrument_id"]: row for row in past["evidence"]}
    live_rows = {
        row["instrument_id"]: row for row in live["policy"]["instrument_evidence"]
    }
    changed = 0
    for uid in (
        set(past["verdict"]) | set(live["verdict"]) | set(past_rows) | set(live_rows)
    ):
        if _pre_sec_identity(past["catalog"], uid) != _pre_sec_identity(
            live["catalog"], uid
        ):
            changed += 1
            continue
        if uid in moved:
            continue
        before, after = past["verdict"].get(uid), live["verdict"].get(uid)
        row_before, row_after = past_rows.get(uid), live_rows.get(uid)
        if before is None or after is None or row_before is None or row_after is None:
            changed += 1
            continue
        fields = (
            ("status", "daily") if uid in retained else ("status", "first", "daily")
        )
        if any(before[field] != after[field] for field in fields) or any(
            row_before.get(field) != row_after.get(field) for field in EVIDENCE_FIELDS
        ):
            changed += 1
    return changed


# ── closed public schema ─────────────────────────────────────────────────────
def validate_public(document: object) -> None:
    """The persisted/printed report: fixed key sets (the persisted form is
    canonical, sorted JSON), typed values, no identifier anywhere."""
    try:
        ok = (
            isinstance(document, dict)
            and set(document) == set(PUBLIC_KEYS)
            and document["kind"] == KIND
            and document["status"] in ("pass", "stop")
            and document["audit_contract_version"] == verifier.AUDIT_CONTRACT_VERSION
            and document["audit_contract_sha256"] == verifier.AUDIT_CONTRACT_SHA256
            and set(document["inputs"]) == set(INPUT_KEYS)
            and all(replay._hex64(value) for value in document["inputs"].values())
            and set(document["code_sha256"]) == set(CODE_FILES)
            and all(replay._hex64(value) for value in document["code_sha256"].values())
            and set(document["times"]) == set(TIME_KEYS)
            and all(
                isinstance(value, str) and verifier.UTC_TEXT.fullmatch(value)
                for value in document["times"].values()
            )
            and set(document["checks"]) == set(CHECK_NAMES)
            and all(type(value) is bool for value in document["checks"].values())
            and set(document["counts"]) == set(COUNT_KEYS)
            and all(
                type(value) is int and value >= 0
                for value in document["counts"].values()
            )
            and (document["status"] == "pass") == all(document["checks"].values())
        )
    except (KeyError, TypeError, AttributeError):
        ok = False
    if not ok or replay._public_problem(document) is not None:
        raise ReconcileBlocked("reconcile_output_not_sanitized")


def validate_blocked(document: object) -> None:
    """Closed blocked envelope: static code only, no path or input value."""
    ok = (
        isinstance(document, dict)
        and set(document) == {"status", "code"}
        and document.get("status") == "blocked"
        and isinstance(document.get("code"), str)
        and document["code"] == blocked_code(document["code"])
    )
    if not ok or replay._public_problem(document) is not None:
        raise ReconcileBlocked("reconcile_output_not_sanitized")


def validate_output(document: object) -> None:
    if isinstance(document, dict) and document.get("status") == "blocked":
        validate_blocked(document)
    else:
        validate_public(document)


def _emit(document: dict) -> None:
    """One canonical public-output path for PASS, STOP and blocked."""
    validate_output(document)
    print(generator.canonical_json(document).decode("ascii"), end="")


def _code_hashes() -> dict:
    return {name: _sha((ROOT / name).read_bytes()) for name in CODE_FILES}


def reconcile(
    *,
    v2_bundle: dict[str, bytes],
    live_bundle: dict[str, bytes],
    v2_config_raw: bytes,
    config_raw: bytes,
    expected_raw: bytes,
) -> tuple[bool, dict]:
    """Pure core: ``(passed, public_report)``; per-ID state stays in memory."""
    _require(
        isinstance(v2_bundle, dict)
        and set(v2_bundle) == set(V2_KEYS)
        and isinstance(live_bundle, dict)
        and set(live_bundle) == set(LIVE_KEYS)
        and all(
            isinstance(value, bytes)
            for value in (
                *v2_bundle.values(),
                *live_bundle.values(),
                v2_config_raw,
                config_raw,
                expected_raw,
            )
        ),
        "reconcile_bundle_invalid",
    )
    # 1) historical replay: exact PASS; E23 by identifier from its state.
    try:
        replayed, _summary, past = replay.replay_state(
            v2_bundle["policy"],
            v2_bundle["source_snapshot"],
            v2_bundle["capture"],
            v2_bundle["audit"],
            v2_config_raw,
            config_raw,
            expected_raw,
        )
    except replay.ReplayBlocked as exc:
        raise ReconcileBlocked(_static(None, exc)) from exc
    except verifier.AuditBlocked as exc:
        raise ReconcileBlocked(_static("auditor", exc)) from exc
    # 2) live Round7 bundle.
    live = _verify_live(live_bundle, config_raw)
    # 3) instants from the documents only (never the tool clock).
    tau_v2 = past["tau"].astimezone(dt.timezone.utc)
    tau_live, captured = live["tau"], live["captured"]
    _require(tau_v2 <= tau_live <= captured, "reconcile_time_order_invalid")
    times = {
        "tau_v2": tau_v2,
        "tau_live": tau_live,
        "captured_at_live": captured,
        "aging_lower_exclusive": tau_v2 - MAX_AGE,
        "aging_upper_inclusive": tau_live - MAX_AGE,
    }
    # 4) set algebra by identifier.
    e23 = dict(past["excluded"])
    e_live = _sec_excluded(live["verdict"])
    removed = set(e23) - set(e_live)
    added = set(e_live) - set(e23)
    retained = set(e23) & set(e_live)
    algebra = set(e_live) == (set(e23) - removed) | added
    reason_changes = sum(1 for uid in retained if e23[uid] != e_live[uid])
    # 5) additions, 6) removals.
    additions = {uid: _addition(uid, past, live, times) for uid in added}
    invalid_additions = sum(1 for ok in additions.values() if not all(ok))
    valid_removals = {uid: _removal(uid, past, live, times) for uid in removed}
    invalid_removals = sum(1 for ok in valid_removals.values() if not ok)
    # 7) universe, pre-SEC candidates, P, INACTIVE and every other decision.
    past_verdict, live_verdict = past["verdict"], live["verdict"]
    moved = added | removed
    non_sec_changes = _non_sec_changes(past, live, moved, retained)
    past_candidates, live_candidates = (
        _candidates(past_verdict),
        _candidates(live_verdict),
    )
    past_p = _structural_daily(past_verdict, past["catalog"])
    live_p = _structural_daily(live_verdict, live["catalog"])
    past_daily, live_daily = _daily_active(past_verdict), _daily_active(live_verdict)
    exclusions = verifier._sec_exclusions(live_verdict)
    dossier = live["dossier"]
    recorded = dossier["details"]["A8"]["exclusions"]
    policy_counts = live["policy"]["generation"]["counts"]
    expected = past["expected"]
    live_status = {
        row["instrument_id"]: row["fund_status"]
        for row in live["policy"]["instrument_evidence"]
    }
    past_active = len(_status_set(past_verdict, "ACTIVE"))
    past_unknown = len(_status_set(past_verdict, "UNKNOWN"))
    past_inactive = _status_set(past_verdict, "INACTIVE")
    active = len(_status_set(live_verdict, "ACTIVE"))
    unknown = len(_status_set(live_verdict, "UNKNOWN"))
    inactive = _status_set(live_verdict, "INACTIVE")
    e23_reasons = Counter(e23.values())
    recovered_stale = sum(1 for uid in removed if e23[uid] == STALE)
    recovered_missing = sum(1 for uid in removed if e23[uid] == MISSING)
    added_daily = sum(1 for uid in added if uid in past_daily)
    recovered_daily = sum(1 for uid in removed if uid in live_daily)
    report = live["report"]
    checks = {
        "historical_replay_exact": replayed is True,
        "live_round7_strict_bound": live["strict"]
        and live["dossier_equal"]
        and live["manifest_equal"]
        and live["sec_rows_equal"]
        and dossier["inputs"]["audit_contract_version"]
        == verifier.AUDIT_CONTRACT_VERSION
        and dossier["differences"]["sec"]["excluded_at_generation"] == e_live
        and live_status
        == {uid: entry["status"] for uid, entry in live_verdict.items()},
        "universe_and_pre_sec_unchanged": set(past_verdict) == set(live_verdict)
        and past_candidates == live_candidates
        and past_p == live_p
        and past_inactive == inactive
        and non_sec_changes == 0,
        "intersection_reasons_unchanged": algebra and reason_changes == 0,
        "additions_stale_only": all(stale for stale, _window in additions.values()),
        "additions_aging_window": all(window for _stale, window in additions.values()),
        "removals_fresh_relisted": invalid_removals == 0,
        "only_sec_movements": non_sec_changes == 0
        and all(
            past_verdict.get(uid, {}).get("status") == "ACTIVE"
            and live_verdict.get(uid, {}).get("status") == "UNKNOWN"
            for uid in added
        )
        and all(
            past_verdict.get(uid, {}).get("status") == "UNKNOWN"
            and live_verdict.get(uid, {}).get("status") == "ACTIVE"
            for uid in removed
        ),
        "counts_conserved": active == past_active - len(added) + len(removed)
        and unknown == past_unknown + len(added) - len(removed)
        and len(inactive) == len(past_inactive)
        and exclusions["stale"] == e23_reasons[STALE] + len(added) - recovered_stale
        and exclusions["missing"] == e23_reasons[MISSING] - recovered_missing
        and exclusions["integrity"] == 0
        and exclusions["c_size"] == len(past_candidates) == len(live_candidates)
        and exclusions["c_size"]
        == active
        + exclusions["stale"]
        + exclusions["missing"]
        + exclusions["integrity"]
        and recorded == exclusions
        and policy_counts["fund_status"].get("ACTIVE", 0) == active
        and policy_counts["fund_status"].get("UNKNOWN", 0) == unknown
        and policy_counts["fund_status"].get("INACTIVE", 0) == len(inactive)
        and policy_counts["active_daily"] == len(live_daily)
        and len(live_daily) == len(past_daily) - added_daily + recovered_daily
        and policy_counts["structural_pre_claims_daily"] == len(live_p) == len(past_p)
        and expected["excluded"] == len(e23)
        and expected["sec_first_failure"] == dict(sorted(e23_reasons.items()))
        and expected["v3"]
        == {
            "active": past_active,
            "unknown": past_unknown,
            "inactive": len(past_inactive),
            "structural_pre_claims_daily": len(past_p),
        },
        "a7_minimums_preserved": past["a7_status"] == "PASS"
        and report["gates"]["A7"]["status"] == "PASS",
    }
    passed = all(checks.values())
    document = {
        "kind": KIND,
        "status": "pass" if passed else "stop",
        "audit_contract_version": verifier.AUDIT_CONTRACT_VERSION,
        "audit_contract_sha256": verifier.AUDIT_CONTRACT_SHA256,
        "inputs": {
            "v2_policy": _sha(v2_bundle["policy"]),
            "v2_source_snapshot": _sha(v2_bundle["source_snapshot"]),
            "v2_capture": _sha(v2_bundle["capture"]),
            "v2_audit": _sha(v2_bundle["audit"]),
            "v2_audit_config": _sha(v2_config_raw),
            "live_policy": _sha(live_bundle["policy"]),
            "live_source_snapshot": _sha(live_bundle["source_snapshot"]),
            "live_capture": _sha(live_bundle["capture"]),
            "live_audit": _sha(live_bundle["audit"]),
            "live_canary": _sha(live_bundle["canary"]),
            "live_previous_policy": _sha(live_bundle["previous_policy"]),
            "audit_config": _sha(config_raw),
            "expected_counts": _sha(expected_raw),
        },
        "code_sha256": _code_hashes(),
        "times": {key: verifier._utc_text(times[key]) for key in TIME_KEYS},
        "checks": {name: bool(checks[name]) for name in CHECK_NAMES},
        "counts": {
            "e23": len(e23),
            "e_live": len(e_live),
            "retained": len(retained),
            "added_stale": sum(1 for uid in added if e_live[uid] == STALE),
            "recovered": len(removed),
            "recovered_stale": recovered_stale,
            "recovered_missing": recovered_missing,
            "invalid_additions": invalid_additions,
            "invalid_removals": invalid_removals,
            "reason_changes": reason_changes,
            "non_sec_changes": non_sec_changes,
            "c_size": exclusions["c_size"],
            "bound": exclusions["bound"],
            "stale": exclusions["stale"],
            "missing": exclusions["missing"],
            "integrity": exclusions["integrity"],
            "active": active,
            "unknown": unknown,
            "inactive": len(inactive),
            "structural_daily": len(live_p),
            "active_daily": len(live_daily),
            "added_daily": added_daily,
            "recovered_daily": recovered_daily,
        },
    }
    validate_public(document)
    return passed, document


def _print_blocked(code: str) -> None:
    _emit({"status": "blocked", "code": blocked_code(code)})


class _SilentParser(argparse.ArgumentParser):
    """Argument parser that never writes usage, values or exception text."""

    def error(self, message):
        raise ReconcileBlocked("reconcile_cli_invalid")

    def exit(self, status=0, message=None):
        raise ReconcileBlocked("reconcile_cli_invalid")


def _parse_args(argv: list[str] | None):
    names = [f"v2-{name.replace('_', '-')}" for name in V2_KEYS] + [
        f"live-{name.replace('_', '-')}" for name in LIVE_KEYS
    ]
    options = tuple(
        [item for name in names for item in (f"--{name}-file", f"--{name}-sha256")]
        + ["--v2-audit-config", "--audit-config", "--custody-root", "--output"]
    )
    tokens = list(sys.argv[1:] if argv is None else argv)
    if len(tokens) != len(options) * 2 or any(
        not isinstance(item, str) for item in tokens
    ):
        raise ReconcileBlocked("reconcile_cli_invalid")
    seen = set()
    for index in range(0, len(tokens), 2):
        option = tokens[index]
        if option not in options or option in seen or tokens[index + 1] in options:
            raise ReconcileBlocked("reconcile_cli_invalid")
        seen.add(option)
    if seen != set(options):
        raise ReconcileBlocked("reconcile_cli_invalid")

    parser = _SilentParser(add_help=False, allow_abbrev=False)
    for name in names:
        parser.add_argument(f"--{name}-file", type=Path, required=True)
        parser.add_argument(f"--{name}-sha256", required=True)
    parser.add_argument("--v2-audit-config", type=Path, required=True)
    parser.add_argument("--audit-config", type=Path, required=True)
    parser.add_argument("--custody-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(tokens), names


def main(argv: list[str] | None = None) -> int:
    try:
        args, names = _parse_args(argv)
        pinned = {}
        for name in names:
            attribute = name.replace("-", "_")
            pinned[attribute] = replay._read_pinned(
                getattr(args, f"{attribute}_file"),
                getattr(args, f"{attribute}_sha256"),
                attribute,
            )
        try:
            v2_config_raw = args.v2_audit_config.read_bytes()
            config_raw = args.audit_config.read_bytes()
            expected_raw = EXPECTED_PATH.read_bytes()
        except OSError as exc:
            raise ReconcileBlocked("reconcile_input_unreadable") from exc
        passed, document = reconcile(
            v2_bundle={key: pinned[f"v2_{key}"] for key in V2_KEYS},
            live_bundle={key: pinned[f"live_{key}"] for key in LIVE_KEYS},
            v2_config_raw=v2_config_raw,
            config_raw=config_raw,
            expected_raw=expected_raw,
        )
        content = generator.canonical_json(document)
        generator.write_artifact(
            args.output,
            content,
            force=False,
            build=True,
            custody_root=args.custody_root,
        )
    except ReconcileBlocked as exc:
        _print_blocked(str(exc))
        return 2
    except replay.ReplayBlocked as exc:
        _print_blocked(_static(None, exc))
        return 2
    except verifier.AuditBlocked as exc:
        _print_blocked(_static("auditor", exc))
        return 2
    except generator.PolicyGenerationError as exc:
        _print_blocked(_static("generator", exc))
        return 2
    except Exception:  # noqa: BLE001 - never a traceback, payload or message
        _print_blocked("reconcile_input_invalid")
        return 2
    _emit(document)
    return 0 if passed else 3


if __name__ == "__main__":
    sys.exit(main())
