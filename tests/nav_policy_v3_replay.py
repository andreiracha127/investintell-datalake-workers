"""Test-only offline replay: retained v2 bundle -> v3 SEC delta (never production).

This is NOT a reclassifier or a publication shortcut. It reads a retained,
hash-pinned v2 bundle (policy, source snapshot, audit capture and dossier) that
only an authorized custodian holds, plus the v2 audit config whose SHA-256 the
v2 dossier embeds. It validates the provenance of every retained input, then
rebuilds the v3 inputs IN MEMORY from the three catalog sources of the v2
snapshot plus the SEC rows of the v2 capture and judges them at the ORIGINAL
v2 decision instant tau (the v2 policy's ``generated_at``) with BOTH the v3
generator classifier and the independent v3 auditor classifier. It never
touches a database or provider, never modifies or rewrites a v2 file and never
emits a policy.

Privacy: every per-identifier comparison stays in memory. The only persisted
output (written through the POSIX custody writer: 0600, atomic, never
overwritten) and stdout carry a CLOSED schema of static codes, booleans,
counts, timestamps and SHA-256 digests; no identifier, ticker, series, class,
map keyed by identifier or source row is emitted on success, on a mismatch
(exit 3) or on a block (exit 2). The schema is re-validated before anything is
written or printed.

Provenance (block, exit 2, static code): the v2 policy digests and ACTIVE set
digests; snapshot <-> policy; the capture contract exactly as the v2 auditor
wrote it (kind, contract version, canonical bytes, source query, source
counts and digest, no drift); the cohort (query and parameter hashes derived
from the v2 audit config, rows shape/order/count/digest/multiplicity); the SEC
block (source contract, relation, timestamp column, query, lineage query and
query contract pins, rows digest and lineage); every value mirrored in the v2
dossier; and the mapping to the replay (v3) config: its builder cohort query,
parameters and Light revision must equal the retained v2 ones (so A7 is
judged on the cohort that selection produced) and the v3 SEC query must be the
retained one. Historical pins are validated, never recomputed or overwritten
with v3 canonicalization.

Replay contract (all must hold, otherwise STOP, exit 3):

* generator v3 and auditor v3 agree PER IDENTIFIER on status, valuation
  frequency, every verification flag and the exact first failure (generator
  first failures are re-derived through the generator's own decision
  functions and must reproduce its evidence and reason histogram), on the
  INACTIVE reasons and on the ACTIVE-daily set (and the generator's digests);
* the excluded set E (v2 ACTIVE -> v3 UNKNOWN) is exactly the v2 private A8
  differences (``per_active_outcome`` non-matches plus ``stale_matched``) with
  the mapped reasons, every E reason is ``sec.*``, nothing else moved and the
  evidence universe is unchanged;
* the ACTIVE-daily loss is exactly the daily part of E;
* every non-SEC first-failure count, P (structural daily) and INACTIVE equal
  v2; ACTIVE/UNKNOWN move by exactly |E|;
* the aggregates (including the ACTIVE-daily delta) equal the expected-counts
  document (counts only, no identifiers);
* A7 (Stage-1 quotas + margin) passes on the retained, provenance-checked cohort.

Run from the repository root under POSIX (e.g. the audit runner container):

    python -m tests.nav_policy_v3_replay --v2-policy-file R/policy-v2.json \
        --v2-policy-sha256 H --v2-source-snapshot-file R/source-v2.json \
        --v2-source-snapshot-sha256 H --v2-capture-file R/capture.json \
        --v2-capture-sha256 H --v2-audit-file R/audit.json --v2-audit-sha256 H \
        [--v2-audit-config configs/nav_identity_audit_v2.json] \
        --audit-config configs/nav_identity_audit_v3.json \
        --custody-root R3 --output R3/replay-v3.json

``--v2-audit-config`` defaults to the repository's historical v2 config; its
SHA-256 must equal the one embedded in the pinned v2 dossier.

Exit codes: 0 replay reproduced; 3 STOP (a mismatch; sanitized evidence still
written); 2 blocked input/output (nothing classified or nothing written).
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

from scripts import generate_fund_nav_policy_v1 as generator
from scripts import nav_identity_audit_contract as contract
from scripts import verify_fund_nav_identity_v2 as verifier
from src.workers import _nav_policy as nav_policy

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_KIND = "nav-policy-v3-offline-replay-evidence-v2"
DEFAULT_EXPECTED = ROOT / "tests" / "fixtures" / "nav_policy_v3_replay_expected.json"
DEFAULT_V2_AUDIT_CONFIG = ROOT / "configs" / "nav_identity_audit_v2.json"
V2_GENERATOR = "fund-nav-policy-generator-v2"
V2_QUERY_VERSION = "nav-current-catalog-snapshot-v2"
V2_SNAPSHOT_KIND = "nav_policy_source_snapshot_v2"
V2_SOURCE_QUERY_SHA256 = (
    "39ca1c5d3f09d989abb8c7fdfd652ef3832683d99486ad006610a37fb0cb97f7"
)
V2_CAPTURE_KIND = "nav-identity-audit-capture-v2-round2"
V2_AUDIT_VERSION = "nav-identity-audit-v2"
V2_AUDIT_CONFIG_VERSION = "nav-identity-audit-config-v2"
# Historical v2 audit contracts that wrote the ``...-capture-v2-round2`` kind
# (frozen literals of the retired leaf; never recomputed).
V2_AUDIT_CONTRACTS = {
    "nav-identity-audit-contract-v2-round3": (
        "1ef74c426526f5308223921c8297516c9fa4ac4034737fad59cad668f73b0001"
    ),
    "nav-identity-audit-contract-v2-round4": (
        "64c75b3db696132e435523e0f3d072309fc78c72069c2d8942bf7ef89bfd68db"
    ),
    "nav-identity-audit-contract-v2-round5": (
        "0d237212e1e95a6e1e0494e071170d174481c29662a27b7bf9e7ac0bca3ec2c7"
    ),
}
CATALOG = ("funds", "identity", "instruments")
CAPTURE_KEYS = frozenset(
    {
        "kind",
        "audit_contract_version",
        "captured_at",
        "source_query_sha256",
        "sources",
        "source_row_counts",
        "source_snapshot_sha256",
        "cohort",
        "sec",
    }
)
COHORT_KEYS = frozenset(
    {
        "query_sha256",
        "parameters_sha256",
        "state",
        "code",
        "rows",
        "row_count",
        "rows_sha256",
    }
)
SEC_BLOCK_KEYS = frozenset(
    {
        "source_contract",
        "relation",
        "timestamp_column",
        "query_sha256",
        "lineage_query_sha256",
        "query_contract_sha256",
        "state",
        "rows",
        "row_count",
        "rows_sha256",
        "lineage",
    }
)
SEC_ROW_KEYS = frozenset({"class_id", "series_id", "ticker", "synced_at"})
# The SEC source the retained capture must have read (the v3 generator reads
# the same relation with the same query: checked below).
SEC_PINS = {
    "source_contract": contract.SEC_SOURCE_CONTRACT,
    "relation": contract.SEC_RELATION,
    "timestamp_column": contract.SEC_TIMESTAMP_COLUMN,
    "query_sha256": hashlib.sha256(contract.SEC_SQL.encode("utf-8")).hexdigest(),
    "lineage_query_sha256": hashlib.sha256(
        contract.SEC_LINEAGE_SQL.encode("utf-8")
    ).hexdigest(),
    "query_contract_sha256": contract.SEC_QUERY_CONTRACT_SHA256,
}
ROW_CEILING = 100_000
# v2 A8 outcome -> the v3 first failure the replay must reproduce.
V2_OUTCOME_TO_V3 = {
    "missing": "sec.missing",
    "partial": "sec.incomplete",
    "contradiction": "sec.contradiction",
    "ambiguous": "sec.ambiguous",
    "poisoned_active_mapping": "sec.poisoned_mapping",
}
CODE_FILES = (
    "scripts/generate_fund_nav_policy_v1.py",
    "scripts/verify_fund_nav_identity_v2.py",
    "scripts/nav_identity_audit_contract.py",
    "src/workers/_nav_policy.py",
    "tests/nav_policy_v3_replay.py",
)
_LOWER_UUID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z"
)

# ── closed public schema (everything persisted or printed) ───────────────────
CHECK_NAMES = (
    "generator_source_valid",
    "auditor_source_valid",
    "generator_reconstruction_consistent",
    "generator_digests_match_evidence",
    "generator_equals_auditor",
    "active_daily_set_equal",
    "universe_unchanged",
    "only_active_to_unknown_moved",
    "every_exclusion_is_sec",
    "excluded_equals_v2_private_differences",
    "daily_loss_is_excluded_daily",
    "non_sec_first_failures_unchanged",
    "structural_daily_unchanged",
    "inactive_unchanged",
    "active_unknown_move_by_excluded",
    "aggregates_equal_expected",
    "a7_stage1_minimums_preserved",
)
MISMATCH_FIELDS = (
    "universe",
    "status",
    "valuation_frequency",
    "verification_flags",
    "first_failure",
)
STATUS_KEYS = (
    "active",
    "unknown",
    "inactive",
    "structural_pre_claims_daily",
    "active_daily",
)
EXPECTED_KEYS = frozenset(
    {"excluded", "sec_first_failure", "active_daily_delta", "v2", "v3"}
)
EXPECTED_STATUS_KEYS = frozenset(
    {"active", "unknown", "inactive", "structural_pre_claims_daily"}
)
PUBLIC_KEYS = frozenset(
    {
        "kind",
        "generator_version",
        "audit_contract_version",
        "audit_contract_sha256",
        "inputs",
        "status",
        "codes",
        "checks",
        "aggregates",
        "digests",
        "provenance",
    }
)
INPUT_KEYS = frozenset(
    {
        "v2_policy_sha256",
        "v2_source_snapshot_sha256",
        "v2_capture_sha256",
        "v2_audit_sha256",
        "v2_audit_config_sha256",
        "audit_config_sha256",
        "expected_counts_sha256",
        "code_sha256",
    }
)
AGGREGATE_KEYS = frozenset(
    {
        "tau",
        "excluded",
        "excluded_daily",
        "excluded_by_reason",
        "sec_first_failure",
        "semantic_mismatches",
        "v2",
        "v3",
        "active_daily_delta",
        "a7",
    }
)
DIGEST_KEYS = frozenset(
    {
        "excluded_set_sha256",
        "v2_active_daily_set_sha256",
        "v3_active_set_sha256",
        "v3_active_daily_set_sha256",
    }
)
PROVENANCE_KEYS = frozenset(
    {
        "v2_audit_contract_version",
        "cohort_query_sha256",
        "cohort_parameters_sha256",
        "cohort_rows",
        "cohort_distinct",
        "cohort_rows_sha256",
        "sec_rows",
        "sec_rows_sha256",
        "light_revision",
    }
)
_ANY_UUID = re.compile(
    r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}"
)
# A standalone SEC class/series token (never matches inside a lowercase digest).
_SEC_IDENTIFIER = re.compile(r"(?<![0-9A-Za-z])[CS][0-9]{9}(?![0-9A-Za-z])")
# Static first-failure codes (closed sets of both classifiers) plus "None".
_REASON_CODES = frozenset(
    {*nav_policy.IDENTITY_FAILURE_CODES, *verifier.GATE_CODES, "None"}
)
_FORBIDDEN_KEYS = frozenset(
    {
        "instrument_id",
        "class_id",
        "series_id",
        "ticker",
        "synced_at",
        "rows",
        "private",
        "excluded_ids",
    }
)


class ReplayBlocked(Exception):
    """Static code: an input, pin or output problem (exit 2)."""


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _compact(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _doc_bytes(value) -> bytes:
    """The v2 auditor's canonical document bytes (compact JSON + newline)."""
    return (_compact(value) + "\n").encode("ascii")


def _hex64(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(c in "0123456789abcdef" for c in value)
    )


def _no_duplicate_keys(pairs):
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate_key")
    return dict(pairs)


def _strict_json(raw: bytes, code: str) -> dict:
    try:
        document = json.loads(raw, object_pairs_hook=_no_duplicate_keys)
    except (UnicodeDecodeError, ValueError) as exc:
        raise ReplayBlocked(f"{code}_not_json") from exc
    if not isinstance(document, dict):
        raise ReplayBlocked(f"{code}_not_object")
    return document


def _read_pinned(path: Path, pin: str, code: str) -> bytes:
    if not _hex64(pin):
        raise ReplayBlocked(f"{code}_pin_invalid")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ReplayBlocked(f"{code}_unreadable") from exc
    if _sha(raw) != pin:
        raise ReplayBlocked(f"{code}_sha256_mismatch")
    _strict_json(raw, code)
    return raw


def _instant(text: object, code: str) -> dt.datetime:
    if not isinstance(text, str):
        raise ReplayBlocked(code)
    try:
        value = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ReplayBlocked(code) from exc
    if value.tzinfo is None or value.utcoffset() is None:
        raise ReplayBlocked(code)
    return value


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ReplayBlocked(code)


def _catalog_digest(sources: dict) -> str:
    return _sha(
        _doc_bytes({name: sorted(sources[name], key=_compact) for name in CATALOG})
    )


def _evidence_sets(evidence: list) -> tuple[set, set]:
    active = {r["instrument_id"] for r in evidence if r["fund_status"] == "ACTIVE"}
    daily = {
        r["instrument_id"]
        for r in evidence
        if r["fund_status"] == "ACTIVE" and r["valuation_frequency"] == "daily"
    }
    return active, daily


# ── provenance of the retained v2 bundle ─────────────────────────────────────
def _verify_policy(policy_raw: bytes, policy: dict) -> dt.datetime:
    generation = policy.get("generation")
    evidence = policy.get("instrument_evidence")
    _require(
        policy.get("generator_version") == V2_GENERATOR
        and isinstance(generation, dict)
        and generation.get("generator_version") == V2_GENERATOR
        and generation.get("source_query_version") == V2_QUERY_VERSION
        and generation.get("source_query_sha256") == V2_SOURCE_QUERY_SHA256
        and isinstance(evidence, list)
        and all(
            isinstance(row, dict)
            and isinstance(row.get("instrument_id"), str)
            and _LOWER_UUID.match(row["instrument_id"])
            for row in evidence
        )
        and len({row["instrument_id"] for row in evidence}) == len(evidence),
        "v2_policy_contract_invalid",
    )
    active, daily = _evidence_sets(evidence)
    _require(
        generation.get("policy_hash") == nav_policy.policy_content_digest(policy)
        and generation.get("instrument_evidence_digest")
        == nav_policy.instrument_evidence_digest(evidence)
        and generation.get("generation_sha256")
        == nav_policy.generation_metadata_digest(generation)
        and generation.get("active_set_sha256") == nav_policy.uuid_set_digest(active)
        and generation.get("active_daily_set_sha256")
        == nav_policy.uuid_set_digest(daily),
        "v2_policy_digest_invalid",
    )
    return _instant(generation.get("generated_at"), "v2_policy_generated_at_invalid")


def _verify_snapshot(snapshot: dict, policy_raw: bytes, policy: dict) -> str:
    sources = snapshot.get("sources")
    _require(
        snapshot.get("kind") == V2_SNAPSHOT_KIND
        and snapshot.get("source_query_version") == V2_QUERY_VERSION
        and snapshot.get("source_query_sha256") == V2_SOURCE_QUERY_SHA256
        and isinstance(sources, dict)
        and set(sources) == set(CATALOG)
        and all(isinstance(sources[name], list) for name in CATALOG)
        and snapshot.get("row_counts") == {name: len(sources[name]) for name in CATALOG}
        and snapshot.get("policy_artifact_sha256") == _sha(policy_raw),
        "v2_snapshot_contract_invalid",
    )
    digest = _catalog_digest(sources)
    _require(
        digest
        == snapshot.get("source_snapshot_sha256")
        == policy["generation"].get("source_snapshot_sha256"),
        "v2_snapshot_link_invalid",
    )
    return digest


def _verify_v2_config(config: dict, contract_version: str) -> dict:
    """The v2 audit config the dossier embeds (hash checked by the caller)."""
    builder = config.get("builder")
    sec = config.get("sec")
    _require(
        config.get("audit_config_version") == V2_AUDIT_CONFIG_VERSION
        and config.get("audit_contract_version") == contract_version
        and isinstance(builder, dict)
        and isinstance(builder.get("cohort_query"), str)
        and isinstance(builder.get("cohort_parameters", {}), dict)
        and isinstance(builder.get("light_revision"), str)
        and re.fullmatch(r"[0-9a-f]{40}", builder["light_revision"]) is not None,
        "v2_audit_config_invalid",
    )
    # The v2 SEC block was optional; when present it must pin the same source
    # (the capture's own SEC pins are always checked against the contract).
    _require(
        sec is None
        or (
            isinstance(sec, dict)
            and sec.get("source_contract") == SEC_PINS["source_contract"]
            and sec.get("relation") == SEC_PINS["relation"]
            and sec.get("timestamp_column") == SEC_PINS["timestamp_column"]
            and sec.get("query_contract_sha256") == SEC_PINS["query_contract_sha256"]
            and type(sec.get("max_synced_age_days")) is int
            and sec["max_synced_age_days"] == contract.SEC_MAX_SYNCED_AGE_DAYS
        ),
        "v2_audit_config_sec_invalid",
    )
    return builder


def _cohort_pins(builder: dict) -> tuple[str, str]:
    """The v2 auditor's cohort query/parameter hashes for one builder config."""
    return (
        _sha(builder["cohort_query"].encode("utf-8")),
        _sha(_compact(builder.get("cohort_parameters") or {}).encode("utf-8")),
    )


def _verify_cohort(cohort: object, builder: dict) -> dict:
    """Retained cohort exactly as the v2 auditor canonicalized it."""
    _require(
        isinstance(cohort, dict)
        and set(cohort) == COHORT_KEYS
        and cohort["state"] == "captured"
        and cohort["code"] is None
        and isinstance(cohort["rows"], list),
        "v2_capture_cohort_invalid",
    )
    query_sha, parameters_sha = _cohort_pins(builder)
    _require(
        cohort["query_sha256"] == query_sha
        and cohort["parameters_sha256"] == parameters_sha,
        "v2_capture_cohort_pin_mismatch",
    )
    rows = cohort["rows"]
    _require(
        len(rows) <= ROW_CEILING
        and all(
            isinstance(row, dict)
            and set(row) == {"instrument_id", "strategy_label"}
            and isinstance(row["instrument_id"], str)
            and _LOWER_UUID.match(row["instrument_id"])
            and (
                row["strategy_label"] is None or isinstance(row["strategy_label"], str)
            )
            for row in rows
        )
        and rows == sorted(rows, key=_compact),
        "v2_capture_cohort_rows_invalid",
    )
    _require(
        type(cohort["row_count"]) is int
        and cohort["row_count"] == len(rows)
        and cohort["rows_sha256"] == _sha(_doc_bytes(rows)),
        "v2_capture_cohort_digest_invalid",
    )
    return cohort


def _verify_sec_block(sec: object) -> dict:
    """Retained SEC block: source pins, exact row shape, digest and lineage."""
    _require(
        isinstance(sec, dict)
        and set(sec) == SEC_BLOCK_KEYS
        and sec["state"] == "captured",
        "v2_capture_sec_invalid",
    )
    _require(
        all(sec[key] == value for key, value in SEC_PINS.items()),
        "v2_capture_sec_pin_invalid",
    )
    rows, lineage = sec["rows"], sec["lineage"]
    _require(
        isinstance(rows, list)
        and len(rows) <= ROW_CEILING
        and all(
            isinstance(row, dict)
            and set(row) == SEC_ROW_KEYS
            and all(
                row[key] is None or isinstance(row[key], str)
                for key in ("class_id", "series_id", "ticker")
            )
            for row in rows
        )
        and rows == sorted(rows, key=_compact),
        "v2_capture_sec_row_invalid",
    )
    _require(
        type(sec["row_count"]) is int
        and sec["row_count"] == len(rows)
        and sec["rows_sha256"] == _sha(_doc_bytes(rows)),
        "v2_capture_sec_digest_invalid",
    )
    instants = [_instant(r["synced_at"], "v2_capture_sec_time_invalid") for r in rows]
    _require(
        isinstance(lineage, dict)
        and set(lineage) == {"row_count", "min_synced_at", "max_synced_at"}
        and type(lineage["row_count"]) is int
        and lineage["row_count"] == len(rows),
        "v2_capture_sec_lineage_invalid",
    )
    if rows:
        _require(
            _instant(lineage["min_synced_at"], "v2_capture_sec_lineage_invalid")
            == min(instants)
            and _instant(lineage["max_synced_at"], "v2_capture_sec_lineage_invalid")
            == max(instants),
            "v2_capture_sec_lineage_invalid",
        )
    else:
        _require(
            lineage["min_synced_at"] is None and lineage["max_synced_at"] is None,
            "v2_capture_sec_lineage_invalid",
        )
    return sec


def _verify_capture(
    capture_raw: bytes, capture: dict, digest: str, contract_version: str, builder
) -> None:
    _require(
        set(capture) == CAPTURE_KEYS
        and capture["kind"] == V2_CAPTURE_KIND
        and capture["audit_contract_version"] == contract_version
        and capture["source_query_sha256"] == V2_SOURCE_QUERY_SHA256,
        "v2_capture_contract_invalid",
    )
    # The v2 auditor only ever accepted its own canonical bytes.
    _require(capture_raw == _doc_bytes(capture), "v2_capture_not_canonical")
    sources = capture["sources"]
    _require(
        isinstance(sources, dict)
        and set(sources) == set(CATALOG)
        and all(isinstance(sources[name], list) for name in CATALOG)
        and capture["source_row_counts"]
        == {name: len(sources[name]) for name in CATALOG},
        "v2_capture_sources_invalid",
    )
    # The v2 capture must be of exactly the v2 generation snapshot (no drift).
    _require(
        _catalog_digest(sources) == capture["source_snapshot_sha256"] == digest,
        "v2_capture_drifted",
    )
    _verify_cohort(capture["cohort"], builder)
    _verify_sec_block(capture["sec"])


def _verify_dossier(
    audit: dict,
    *,
    policy_raw: bytes,
    snapshot_raw: bytes,
    capture_raw: bytes,
    capture: dict,
    v2_config_raw: bytes,
    digest: str,
    builder: dict,
) -> None:
    """Every capture/config value mirrored in the v2 dossier must match."""
    inputs = audit.get("inputs")
    differences = audit.get("differences")
    details = audit.get("details")
    _require(
        audit.get("audit_version") == V2_AUDIT_VERSION
        and isinstance(inputs, dict)
        and isinstance(details, dict)
        and isinstance(differences, dict)
        and isinstance(differences.get("sec"), dict)
        and isinstance(differences["sec"].get("per_active_outcome"), dict)
        and isinstance(differences["sec"].get("stale_matched"), list),
        "v2_audit_contract_invalid",
    )
    _require(
        V2_AUDIT_CONTRACTS.get(inputs.get("audit_contract_version"))
        == inputs.get("audit_contract_sha256"),
        "v2_audit_contract_invalid",
    )
    _require(
        inputs.get("policy_artifact_sha256") == _sha(policy_raw)
        and inputs.get("source_snapshot_file_sha256") == _sha(snapshot_raw)
        and inputs.get("source_snapshot_sha256") == digest
        and inputs.get("live_source_snapshot_sha256") == digest
        and inputs.get("source_query_sha256") == V2_SOURCE_QUERY_SHA256
        and inputs.get("sec_source_contract") == SEC_PINS["source_contract"]
        and inputs.get("sec_query_contract_sha256")
        == SEC_PINS["query_contract_sha256"],
        "v2_audit_link_invalid",
    )
    _require(
        inputs.get("audit_config_sha256") == _sha(v2_config_raw),
        "v2_audit_config_mismatch",
    )
    mirror = inputs.get("capture")
    cohort, sec = capture["cohort"], capture["sec"]
    _require(
        isinstance(mirror, dict)
        and mirror.get("capture_bundle_sha256") == _sha(capture_raw)
        and mirror.get("captured_at") == capture["captured_at"]
        and mirror.get("source_snapshot_sha256") == capture["source_snapshot_sha256"]
        and mirror.get("source_row_counts") == capture["source_row_counts"]
        and mirror.get("cohort")
        == {
            key: cohort[key]
            for key in (
                "state",
                "code",
                "row_count",
                "rows_sha256",
                "query_sha256",
                "parameters_sha256",
            )
        }
        and mirror.get("sec")
        == {
            "code": None,
            **{
                key: sec[key]
                for key in (
                    "state",
                    "source_contract",
                    "relation",
                    "timestamp_column",
                    "query_sha256",
                    "lineage_query_sha256",
                    "query_contract_sha256",
                    "row_count",
                    "rows_sha256",
                    "lineage",
                )
            },
        },
        "v2_audit_capture_mismatch",
    )
    a7 = details.get("A7")
    uids = [row["instrument_id"] for row in cohort["rows"]]
    _require(
        isinstance(a7, dict)
        and a7.get("light_revision") == builder["light_revision"]
        and a7.get("cohort_query_sha256") == cohort["query_sha256"]
        and a7.get("cohort_parameters_sha256") == cohort["parameters_sha256"]
        and a7.get("cohort_rows_sha256") == cohort["rows_sha256"]
        and a7.get("cohort_rows") == len(uids)
        and a7.get("cohort_distinct") == len(set(uids))
        and a7.get("duplicate_uuids")
        == sum(1 for count in Counter(uids).values() if count > 1),
        "v2_audit_cohort_mismatch",
    )


def _verify_replay_config(config: dict, builder_v2: dict, capture: dict) -> dict:
    """Explicit v2 -> v3 mapping: the v3 builder selects the retained cohort."""
    builder = config.get("builder")
    _require(isinstance(builder, dict), "replay_config_builder_mismatch")
    _require(
        builder.get("cohort_query") == builder_v2["cohort_query"]
        and _compact(builder.get("cohort_parameters") or {})
        == _compact(builder_v2.get("cohort_parameters") or {})
        and builder.get("light_revision") == builder_v2["light_revision"]
        and _cohort_pins(builder)
        == (capture["cohort"]["query_sha256"], capture["cohort"]["parameters_sha256"]),
        "replay_config_builder_mismatch",
    )
    sec = config.get("sec")
    _require(
        isinstance(sec, dict)
        and sec.get("relation") == capture["sec"]["relation"]
        and sec.get("timestamp_column") == capture["sec"]["timestamp_column"]
        and sec.get("query_contract_sha256") == capture["sec"]["query_contract_sha256"]
        # The v3 generator reads the SEC source with the retained query.
        and _sha(nav_policy.SEC_QUERY.encode("utf-8")) == capture["sec"]["query_sha256"]
        and _sha(nav_policy.SEC_LINEAGE_QUERY.encode("utf-8"))
        == capture["sec"]["lineage_query_sha256"],
        "replay_config_sec_mismatch",
    )
    return builder


def verify_v2_bundle(
    policy_raw: bytes,
    snapshot_raw: bytes,
    capture_raw: bytes,
    audit_raw: bytes,
    v2_config_raw: bytes,
) -> dict:
    """Validate every retained input and link; returns the parsed documents."""
    policy = _strict_json(policy_raw, "v2_policy")
    snapshot = _strict_json(snapshot_raw, "v2_source_snapshot")
    capture = _strict_json(capture_raw, "v2_capture")
    audit = _strict_json(audit_raw, "v2_audit")
    v2_config = _strict_json(v2_config_raw, "v2_audit_config")
    tau = _verify_policy(policy_raw, policy)
    digest = _verify_snapshot(snapshot, policy_raw, policy)
    inputs = audit.get("inputs") if isinstance(audit.get("inputs"), dict) else {}
    contract_version = inputs.get("audit_contract_version")
    _require(contract_version in V2_AUDIT_CONTRACTS, "v2_audit_contract_invalid")
    builder = _verify_v2_config(v2_config, contract_version)
    _verify_capture(capture_raw, capture, digest, contract_version, builder)
    captured_at = _instant(capture["captured_at"], "v2_capture_time_invalid")
    _require(captured_at >= tau, "v2_capture_precedes_generation")
    _verify_dossier(
        audit,
        policy_raw=policy_raw,
        snapshot_raw=snapshot_raw,
        capture_raw=capture_raw,
        capture=capture,
        v2_config_raw=v2_config_raw,
        digest=digest,
        builder=builder,
    )
    return {
        "tau": tau,
        "policy": policy,
        "snapshot": snapshot,
        "capture": capture,
        "audit": audit,
        "builder": builder,
        "contract_version": contract_version,
    }


def expected_exclusions(audit: dict) -> dict[str, str]:
    """E from the v2 private A8 differences, mapped to the v3 reasons."""
    private = audit["differences"]["sec"]
    expected: dict[str, str] = {}
    for uid, outcome in private["per_active_outcome"].items():
        _require(outcome in V2_OUTCOME_TO_V3, "v2_audit_outcome_unknown")
        expected[uid] = V2_OUTCOME_TO_V3[outcome]
    for uid in private["stale_matched"]:
        _require(uid not in expected, "v2_audit_differences_overlap")
        expected[uid] = "sec.stale"
    return expected


def _sec_rows(capture: dict) -> list[dict]:
    """Retained SEC rows with aware instants (identifiers verbatim)."""
    return [
        dict(row, synced_at=_instant(row["synced_at"], "v2_capture_sec_time_invalid"))
        for row in capture["sec"]["rows"]
    ]


# ── generator per-identifier first failures (its own decision functions) ─────
def _generator_first_failures(catalog_rows: dict, sec_rows: list, tau) -> dict:
    """Per-UUID first failure through the generator's own gate functions.

    ``classify_catalog`` publishes only the reason histogram; this re-derives
    the per-UUID reason with the same functions it calls, and the caller
    requires the re-derivation to reproduce its evidence and histogram.
    """
    index = generator._CatalogIndex(
        generator.canonical_source_rows(catalog_rows["instruments"], "instruments"),
        generator.canonical_source_rows(catalog_rows["funds"], "funds"),
        generator.canonical_source_rows(catalog_rows["identity"], "identity"),
    )
    sec_index = generator._SecIndex(
        generator.canonical_source_rows(sec_rows, "sec"), tau
    )
    result: dict[str, tuple[str, str | None]] = {}
    for uid in index.universe():
        if generator._is_inactive(index, uid):
            result[uid] = ("INACTIVE", None)
            continue
        failure = generator._first_failure(index, uid)
        if failure is None:
            failure = generator._sec_first_failure(index, sec_index, uid)
        result[uid] = ("UNKNOWN" if failure is not None else "ACTIVE", failure)
    return result


def _semantic_mismatches(evidence: list, first: dict, verdict: dict) -> dict:
    """Per-field mismatch COUNTS (identifiers never leave this function)."""
    rows = {row["instrument_id"]: row for row in evidence}
    mismatches = dict.fromkeys(MISMATCH_FIELDS, 0)
    mismatches["universe"] = len(set(rows) ^ set(verdict))
    for uid in set(rows) & set(verdict):
        row, entry = rows[uid], verdict[uid]
        active = entry["status"] == "ACTIVE"
        daily = active and entry["daily"]
        if row["fund_status"] != entry["status"]:
            mismatches["status"] += 1
        if row["valuation_frequency"] != ("daily" if daily else "unknown"):
            mismatches["valuation_frequency"] += 1
        if (
            row["identity_verified"] is not active
            or row["currency_verified"] is not active
            or row["return_basis_verified"] is not daily
        ):
            mismatches["verification_flags"] += 1
        if first.get(uid, ("", "absent"))[1] != entry["first"]:
            mismatches["first_failure"] += 1
    return mismatches


def _public_problem(document: object, path: str = "") -> str | None:
    """None when a value tree is shareable (no identifier-like content)."""
    if isinstance(document, dict):
        for key, value in document.items():
            if (
                not isinstance(key, str)
                or key in _FORBIDDEN_KEYS
                or _ANY_UUID.search(key)
                or _SEC_IDENTIFIER.search(key)
            ):
                return f"{path}/<key>"
            problem = _public_problem(value, f"{path}/{key}")
            if problem is not None:
                return problem
        return None
    if isinstance(document, str):
        if _ANY_UUID.search(document) or _SEC_IDENTIFIER.search(document):
            return path
        return None
    if document is None or isinstance(document, (bool, int)):
        return None
    return path  # lists, floats and anything else are outside the schema


def validate_public(document: dict) -> None:
    """Closed schema of the persisted/printed evidence; raises when violated.

    Applies to pass AND stop documents alike: fixed key sets, booleans for the
    checks, static reason codes as the only histogram keys, SHA-256 digests,
    counts, and no identifier-like string or source-row key anywhere.
    """
    try:
        checks = document["checks"]
        aggregates = document["aggregates"]
        ok = (
            set(document) == PUBLIC_KEYS
            and set(document["inputs"]) == INPUT_KEYS
            and set(document["inputs"]["code_sha256"]) == set(CODE_FILES)
            and all(
                _hex64(value)
                for key, value in document["inputs"].items()
                if key != "code_sha256"
            )
            and all(_hex64(v) for v in document["inputs"]["code_sha256"].values())
            and set(checks) == set(CHECK_NAMES)
            and all(type(value) is bool for value in checks.values())
            and set(aggregates) == AGGREGATE_KEYS
            and set(aggregates["semantic_mismatches"]) == set(MISMATCH_FIELDS)
            and set(aggregates["v2"]) == set(STATUS_KEYS)
            and set(aggregates["v3"]) == set(STATUS_KEYS)
            and set(aggregates["a7"]) == {"status", "sleeves"}
            and all(
                set(item) == {"required", "available_daily_active"}
                for item in aggregates["a7"]["sleeves"].values()
            )
            and set(aggregates["excluded_by_reason"]) <= _REASON_CODES
            and set(aggregates["sec_first_failure"]) <= _REASON_CODES
            and set(document["digests"]) == DIGEST_KEYS
            and all(_hex64(value) for value in document["digests"].values())
            and set(document["provenance"]) == PROVENANCE_KEYS
            and set(document["codes"]) == {"generator", "auditor"}
            and document["status"] in ("pass", "stop")
        )
    except (KeyError, TypeError, AttributeError):
        ok = False
    if not ok or _public_problem(document) is not None:
        raise ReplayBlocked("replay_output_not_sanitized")


def replay(
    policy_raw: bytes,
    snapshot_raw: bytes,
    capture_raw: bytes,
    audit_raw: bytes,
    v2_config_raw: bytes,
    config_raw: bytes,
    expected_raw: bytes,
) -> tuple[bool, dict]:
    """Pure replay: ``(passed, public_summary)``; per-ID data stays in memory."""
    passed, summary, _state = replay_state(
        policy_raw,
        snapshot_raw,
        capture_raw,
        audit_raw,
        v2_config_raw,
        config_raw,
        expected_raw,
    )
    return passed, summary


def replay_state(
    policy_raw: bytes,
    snapshot_raw: bytes,
    capture_raw: bytes,
    audit_raw: bytes,
    v2_config_raw: bytes,
    config_raw: bytes,
    expected_raw: bytes,
) -> tuple[bool, dict, dict]:
    """Test-only seam: the replay plus its IN-MEMORY per-identifier state.

    Same inputs, checks and public summary as ``replay``. The third value
    (never serialized, printed or persisted) exposes the reconstruction at the
    original tau for the Round7 first-release reconciliation sibling: the
    excluded map E (identifier -> v3 reason), the auditor verdict/catalog/SEC
    judge, the generator evidence and the expected-counts document.
    """
    bundle = verify_v2_bundle(
        policy_raw, snapshot_raw, capture_raw, audit_raw, v2_config_raw
    )
    tau, policy, snapshot = bundle["tau"], bundle["policy"], bundle["snapshot"]
    capture, audit = bundle["capture"], bundle["audit"]
    expected_counts = _strict_json(expected_raw, "expected_counts")
    _require(
        set(expected_counts) == EXPECTED_KEYS
        and set(expected_counts["v2"]) == EXPECTED_STATUS_KEYS
        and set(expected_counts["v3"]) == EXPECTED_STATUS_KEYS,
        "expected_counts_invalid",
    )
    try:
        config = verifier._load_config(config_raw)
    except verifier.AuditBlocked as exc:
        raise ReplayBlocked(f"replay_config_invalid:{exc}") from exc
    builder = _verify_replay_config(config, bundle["builder"], capture)
    sec_rows = _sec_rows(capture)
    catalog_rows = {name: snapshot["sources"][name] for name in CATALOG}

    # 1) v3 generator classifier (source-level SEC defects are a STOP code).
    generation_error = None
    evidence: list = []
    counts: dict = {}
    digests: dict = {}
    first: dict = {}
    try:
        evidence, counts, digests = generator.classify_catalog(
            catalog_rows["instruments"],
            catalog_rows["funds"],
            catalog_rows["identity"],
            sec_rows,
            tau,
        )
        first = _generator_first_failures(catalog_rows, sec_rows, tau)
    except generator.PolicyGenerationError as exc:
        generation_error, evidence, counts, digests, first = str(exc), [], {}, {}, {}

    # 2) independent v3 auditor classifier over the same in-memory inputs.
    canonical_sec = [
        dict(row, synced_at=verifier._utc_text(row["synced_at"])) for row in sec_rows
    ]
    typed = {
        name: verifier._source_rows(catalog_rows[name], name, from_database=False)
        for name in CATALOG
    }
    typed["sec"] = verifier._sec_source_rows(canonical_sec, from_database=False)
    judge = verifier.SecJudge(typed["sec"])
    audit_error = judge.source_problem(tau)
    catalog = verifier.Catalog(typed)
    verdict = {} if audit_error else verifier.classify(catalog, judge, tau)

    # 3) comparisons, per identifier, in memory only.
    v2_evidence = policy["instrument_evidence"]
    v2_status = {row["instrument_id"]: row["fund_status"] for row in v2_evidence}
    v2_active, v2_daily = _evidence_sets(v2_evidence)
    v3_status = {row["instrument_id"]: row["fund_status"] for row in evidence}
    v3_active, v3_daily = _evidence_sets(evidence)
    auditor_daily = {
        u for u, e in verdict.items() if e["status"] == "ACTIVE" and e["daily"]
    }
    mismatches = _semantic_mismatches(evidence, first, verdict)
    reconstructed_first = Counter(f for _s, f in first.values() if f is not None)
    auditor_first = Counter(
        e["first"] for e in verdict.values() if e["first"] is not None
    )
    auditor_inactive = sum(1 for e in verdict.values() if e["status"] == "INACTIVE")
    moved = {
        uid: status for uid, status in v3_status.items() if v2_status.get(uid) != status
    }
    excluded = {
        uid: verdict[uid]["first"] if uid in verdict else None
        for uid, status in moved.items()
        if v2_status.get(uid) == "ACTIVE" and status == "UNKNOWN"
    }
    expected_e = expected_exclusions(audit)
    v2_counts = policy["generation"]["counts"]
    v2_first = v2_counts.get("identity_first_failure", {})
    v3_first = counts.get("identity_first_failure", {})
    sec_first = {k: v for k, v in v3_first.items() if k.startswith("sec.")}
    non_sec_first = {k: v for k, v in v3_first.items() if not k.startswith("sec.")}
    v2_fund = v2_counts.get("fund_status", {})
    v3_fund = counts.get("fund_status", {})
    a7_gate, a7_detail, _usable = verifier._a7(
        capture["cohort"], builder, verdict, catalog
    )

    def status_counts(fund, source_counts, daily):
        return {
            "active": fund.get("ACTIVE", 0),
            "unknown": fund.get("UNKNOWN", 0),
            "inactive": fund.get("INACTIVE", 0),
            "structural_pre_claims_daily": source_counts.get(
                "structural_pre_claims_daily"
            ),
            "active_daily": len(daily),
        }

    aggregates = {
        "tau": tau.isoformat(),
        "excluded": len(excluded),
        "excluded_daily": len(set(excluded) & v2_daily),
        "excluded_by_reason": dict(
            sorted(Counter(str(code) for code in excluded.values()).items())
        ),
        "sec_first_failure": dict(sorted(sec_first.items())),
        "semantic_mismatches": mismatches,
        "v2": status_counts(v2_fund, v2_counts, v2_daily),
        "v3": status_counts(v3_fund, counts, v3_daily),
        "active_daily_delta": len(v2_daily) - len(v3_daily),
        "a7": {
            "status": a7_gate["status"],
            "sleeves": {
                name: {k: item[k] for k in ("required", "available_daily_active")}
                for name, item in (a7_detail.get("sleeves") or {}).items()
            },
        },
    }
    expected_view = {
        "excluded": aggregates["excluded"],
        "sec_first_failure": aggregates["sec_first_failure"],
        "active_daily_delta": aggregates["active_daily_delta"],
        **{
            version: {
                key: aggregates[version][key] for key in sorted(EXPECTED_STATUS_KEYS)
            }
            for version in ("v2", "v3")
        },
    }
    generated = generation_error is None and audit_error is None
    checks = {
        "generator_source_valid": generation_error is None,
        "auditor_source_valid": audit_error is None,
        "generator_reconstruction_consistent": generated
        and {uid: status for uid, (status, _f) in first.items()} == v3_status
        and dict(reconstructed_first) == v3_first,
        "generator_digests_match_evidence": generated
        and digests.get("active_set_sha256") == nav_policy.uuid_set_digest(v3_active)
        and digests.get("active_daily_set_sha256")
        == nav_policy.uuid_set_digest(v3_daily)
        and counts.get("active_daily") == len(v3_daily),
        "generator_equals_auditor": generated
        and all(value == 0 for value in mismatches.values())
        and dict(auditor_first) == v3_first
        and counts.get("inactive_reason", {})
        == ({"inactive_without_funds_v": auditor_inactive} if auditor_inactive else {}),
        "active_daily_set_equal": generated and v3_daily == auditor_daily,
        "universe_unchanged": set(v2_status) == set(v3_status),
        "only_active_to_unknown_moved": set(moved) == set(excluded),
        "every_exclusion_is_sec": all(
            isinstance(code, str) and code.startswith("sec.")
            for code in excluded.values()
        ),
        "excluded_equals_v2_private_differences": excluded == expected_e,
        "daily_loss_is_excluded_daily": v3_daily <= v2_daily
        and v2_daily - v3_daily == set(excluded) & v2_daily,
        "non_sec_first_failures_unchanged": non_sec_first == v2_first,
        "structural_daily_unchanged": counts.get("structural_pre_claims_daily")
        == v2_counts.get("structural_pre_claims_daily"),
        "inactive_unchanged": v3_fund.get("INACTIVE") == v2_fund.get("INACTIVE"),
        "active_unknown_move_by_excluded": v3_fund.get("ACTIVE", 0)
        == v2_fund.get("ACTIVE", 0) - len(excluded)
        and v3_fund.get("UNKNOWN", 0) == v2_fund.get("UNKNOWN", 0) + len(excluded),
        "aggregates_equal_expected": expected_view == expected_counts,
        "a7_stage1_minimums_preserved": a7_gate["status"] == "PASS",
    }
    passed = all(checks.values())
    cohort, sec = capture["cohort"], capture["sec"]
    summary = {
        "status": "pass" if passed else "stop",
        "codes": {"generator": generation_error, "auditor": audit_error},
        "checks": checks,
        "aggregates": aggregates,
        "digests": {
            "excluded_set_sha256": nav_policy.uuid_set_digest(excluded),
            "v2_active_daily_set_sha256": nav_policy.uuid_set_digest(v2_daily),
            "v3_active_set_sha256": nav_policy.uuid_set_digest(v3_active),
            "v3_active_daily_set_sha256": nav_policy.uuid_set_digest(v3_daily),
        },
        "provenance": {
            "v2_audit_contract_version": bundle["contract_version"],
            "cohort_query_sha256": cohort["query_sha256"],
            "cohort_parameters_sha256": cohort["parameters_sha256"],
            "cohort_rows": cohort["row_count"],
            "cohort_distinct": len({row["instrument_id"] for row in cohort["rows"]}),
            "cohort_rows_sha256": cohort["rows_sha256"],
            "sec_rows": sec["row_count"],
            "sec_rows_sha256": sec["rows_sha256"],
            "light_revision": builder["light_revision"],
        },
    }
    state = {
        "tau": tau,
        "excluded": excluded,
        "verdict": verdict,
        "catalog": catalog,
        "judge": judge,
        "evidence": evidence,
        "counts": counts,
        "expected": expected_counts,
        "a7_status": a7_gate["status"],
    }
    return passed, summary, state


def _code_hashes() -> dict:
    return {name: _sha((ROOT / name).read_bytes()) for name in CODE_FILES}


def _blocked(code: str, reason: str | None = None) -> None:
    document = {"status": "blocked", "code": code}
    if reason is not None:
        document["reason"] = reason
    if _public_problem(document) is not None:
        document = {"status": "blocked", "code": "replay_code_not_sanitized"}
    print(json.dumps(document, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    for name in ("v2-policy", "v2-source-snapshot", "v2-capture", "v2-audit"):
        parser.add_argument(f"--{name}-file", type=Path, required=True)
        parser.add_argument(f"--{name}-sha256", required=True)
    parser.add_argument("--v2-audit-config", type=Path, default=DEFAULT_V2_AUDIT_CONFIG)
    parser.add_argument("--audit-config", type=Path, required=True)
    parser.add_argument("--expected", type=Path, default=DEFAULT_EXPECTED)
    parser.add_argument("--custody-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        pinned = {
            name: _read_pinned(
                getattr(args, f"{name}_file"), getattr(args, f"{name}_sha256"), name
            )
            for name in ("v2_policy", "v2_source_snapshot", "v2_capture", "v2_audit")
        }
        try:
            v2_config_raw = args.v2_audit_config.read_bytes()
            config_raw = args.audit_config.read_bytes()
            expected_raw = args.expected.read_bytes()
        except OSError as exc:
            raise ReplayBlocked("replay_input_unreadable") from exc
        passed, summary = replay(
            pinned["v2_policy"],
            pinned["v2_source_snapshot"],
            pinned["v2_capture"],
            pinned["v2_audit"],
            v2_config_raw,
            config_raw,
            expected_raw,
        )
        evidence = {
            "kind": EVIDENCE_KIND,
            "generator_version": generator.GENERATOR_VERSION,
            "audit_contract_version": verifier.AUDIT_CONTRACT_VERSION,
            "audit_contract_sha256": verifier.AUDIT_CONTRACT_SHA256,
            "inputs": {
                **{f"{name}_sha256": _sha(raw) for name, raw in pinned.items()},
                "v2_audit_config_sha256": _sha(v2_config_raw),
                "audit_config_sha256": _sha(config_raw),
                "expected_counts_sha256": _sha(expected_raw),
                "code_sha256": _code_hashes(),
            },
            **summary,
        }
        validate_public(evidence)
        content = generator.canonical_json(evidence)
        generator.write_artifact(
            args.output,
            content,
            force=False,
            build=True,
            custody_root=args.custody_root,
        )
    except ReplayBlocked as exc:
        _blocked(str(exc))
        return 2
    except verifier.AuditBlocked as exc:
        _blocked(f"auditor:{exc}")
        return 2
    except generator.PolicyGenerationError as exc:
        _blocked(str(exc))
        return 2
    except Exception as exc:  # noqa: BLE001 - never a traceback or payload
        _blocked("replay_input_invalid", type(exc).__name__)
        return 2
    print(json.dumps({**evidence, "evidence_sha256": _sha(content)}, sort_keys=True))
    return 0 if passed else 3


if __name__ == "__main__":
    sys.exit(main())
