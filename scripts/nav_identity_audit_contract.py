"""Declarative NAV identity audit contract, Round2 (shared leaf).

Pure data. This module imports nothing: not the generator/classifier, the
independent auditor, the operator, a database driver or the custody writer.
The auditor (``scripts.verify_fund_nav_identity_v2``) and the operator
(``scripts.fund_nav_readiness_schema``) both import it; the operator never
imports the auditor or the classifier to validate a publication receipt.

``AUDIT_CONTRACT`` is the canonical declaration of the governed audit
semantics (A4 ceiling population, A8 source/queries/freshness, required
checks per gate, dossier/manifest/receipt shapes, Stage-1 margin, canary rule
and selection digest). ``AUDIT_CONTRACT_SHA256`` is its frozen digest:
SHA-256 of ``json.dumps(AUDIT_CONTRACT, sort_keys=True, separators=(",", ":"),
ensure_ascii=True)`` encoded UTF-8. The auditor recomputes it at import and
refuses to run on a mismatch; any semantic change requires a new
``AUDIT_CONTRACT_VERSION`` and a new literal, never a silent edit.

``SEC_QUERY_CONTRACT_SHA256`` is SHA-256 of ``SEC_SQL + "\\n" + SEC_LINEAGE_SQL``.
"""

from __future__ import annotations

AUDIT_VERSION = "nav-identity-audit-v2"
AUDIT_CONFIG_VERSION = "nav-identity-audit-config-v2"
AUDIT_CONTRACT_VERSION = "nav-identity-audit-contract-v2-round4"
CAPTURE_KIND = "nav-identity-audit-capture-v2-round2"
CANARY_KIND = "nav-policy-v2-canary-manifest-round2"
PLAN_VERSION = "nav-schema-plan-v4"

# Catalog query digest of the three policy sources (generator contract; the
# auditor recomputes it from its own independent SQL literals).
CATALOG_SOURCE_QUERY_SHA256 = (
    "39ca1c5d3f09d989abb8c7fdfd652ef3832683d99486ad006610a37fb0cb97f7"
)

ROW_CEILING = 100_000
FETCH_BATCH = 5000
CANARY_MAX = 20
STAGE1_MARGIN_TEXT = "1/10"
DEFAULT_STRUCTURAL_DAILY_CEILING = 5103
STRUCTURAL_DAILY_CEILING_KEY = "structural_daily_ceiling"
RETIRED_CEILING_KEYS = ("active_ceiling",)

# ── A8: the only SEC correspondence source (updated_at is the freshness) ────
SEC_SOURCE_CONTRACT = "sec-company-tickers-mf-class-map-v1"
SEC_RELATION = "public.sec_company_tickers_mf"
SEC_TIMESTAMP_COLUMN = "updated_at"
SEC_CLASS_PATTERN = "^C[0-9]{9}$"
SEC_SERIES_PATTERN = "^S[0-9]{9}$"
SEC_MAX_SYNCED_AGE_DAYS = 7
SEC_SQL = (
    "SELECT class_id,series_id,ticker,updated_at AS synced_at "
    "FROM public.sec_company_tickers_mf "
    "ORDER BY class_id,series_id,ticker,updated_at LIMIT 100001"
)
SEC_LINEAGE_SQL = (
    "SELECT count(*) AS row_count,min(updated_at) AS min_synced_at,"
    "max(updated_at) AS max_synced_at FROM public.sec_company_tickers_mf"
)
SEC_QUERY_CONTRACT_SHA256 = (
    "60cd2425e504b346ee43967f341fe563e20392be6c4d2b5d70b310b6f243c870"
)
SEC_ROW_KEYS = ("class_id", "series_id", "synced_at", "ticker")
SEC_LINEAGE_KEYS = ("max_synced_at", "min_synced_at", "row_count")
SEC_CAPTURED_KEYS = (
    "lineage",
    "lineage_query_sha256",
    "query_contract_sha256",
    "query_sha256",
    "relation",
    "row_count",
    "rows",
    "rows_sha256",
    "source_contract",
    "state",
    "timestamp_column",
)
A8_OUTCOMES = (
    "ambiguous",
    "contradiction",
    "matched",
    "missing",
    "partial",
    "poisoned_active_mapping",
)
A8_DETAIL_KEYS = (
    "active_poisoned_count",
    "duplicate_source_rows",
    "excluded_poisoned_count",
    "freshness",
    "invalid_source_rows",
    "lineage",
    "outcomes",
    "relation",
    "sec_lineage_query_sha256",
    "sec_query_contract_sha256",
    "sec_query_sha256",
    "sec_row_count",
    "sec_rows_sha256",
    "source_contract",
    "timestamp_column",
)
A8_FRESHNESS_KEYS = (
    "decision_at",
    "lineage_max_synced_at",
    "max_matched_synced_at",
    "max_synced_age_days",
    "min_matched_synced_at",
    "valid_until",
)
A4_DETAIL_KEYS = (
    "accepted_structural_delta",
    "active_daily_count",
    "baseline_count",
    "ceiling_population",
    "claim_first_failure",
    "pre_claim_first_failure",
    "structural_baseline",
    "structural_daily_ceiling",
    "structural_daily_count",
    "structural_delta",
)

# ── dossier: exact gates and the exact non-empty check set of each gate ─────
GATE_NAMES = ("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8")
GATE_CHECKS = {
    "A1": (
        "calendar_identical_to_previous",
        "calendar_package",
        "canonical_bytes",
        "evidence_digest",
        "evidence_reference",
        "evidence_shape",
        "generation_digest",
        "generator_version",
        "hash_distinct_from_previous",
        "policy_hash",
        "provider_contract",
        "publication_state_approved",
        "query_sha256",
        "query_version",
        "snapshot_link",
        "source_reference_present",
        "source_snapshot_sha256",
        "timezone_new_york",
        "valid_through_equals_last_deadline",
    ),
    "A2": (
        "evidence_equals_universe",
        "first_failure_equals_independent",
        "first_failure_sum_equals_unknown",
        "generation_counts_exact",
        "generation_counts_types",
        "inactive_delta_explained",
        "one_row_per_uuid",
        "partition_sum",
        "previous_inactive_pin",
        "reported_status_counts",
    ),
    "A3": ("no_unexplained_loss",),
    "A4": (
        "active_daily_subset_of_structural_daily",
        "baseline_first_failure_closure",
        "structural_baseline_accepted",
        "structural_daily_subset_of_baseline",
        "structural_daily_within_ceiling",
        "structural_reconciled_by_claim_failures",
    ),
    "A5": (
        "active_daily_digest",
        "active_daily_set_equal",
        "active_digest",
        "active_set_equal",
        "generation_precedes_capture",
        "isin_presence_equal",
        "lifecycle_equal",
        "no_source_drift",
        "reason_counts_equal",
    ),
    "A6": (
        "active_passes_every_gate",
        "flags_consistent",
        "no_projection_divergence",
    ),
    "A7": (
        "cohort_nonempty",
        "cohort_unique",
        "every_exclusion_has_reason",
        "sleeve_quota_margin",
    ),
    "A8": (
        "active_nonempty",
        "all_active_matched",
        "fresh_within_max_age",
        "source_lineage_verified",
        "synced_not_in_future",
        "zero_active_duplicates",
        "zero_active_poisoned",
        "zero_contradictions",
    ),
}
DOSSIER_KEYS = (
    "audit_version",
    "canary_requested",
    "counts",
    "details",
    "differences",
    "gates",
    "inputs",
    "result",
    "strict",
)
DOSSIER_INPUT_KEYS = (
    "audit_config_sha256",
    "audit_contract_sha256",
    "audit_contract_version",
    "capture",
    "live_source_snapshot_sha256",
    "policy_artifact_sha256",
    "policy_generation_sha256",
    "policy_hash",
    "policy_id",
    "policy_version",
    "previous_policy_identity",
    "previous_policy_sha256",
    "sec_query_contract_sha256",
    "sec_source_contract",
    "source_query_sha256",
    "source_snapshot_file_sha256",
    "source_snapshot_sha256",
    "verifier_source_sha256",
)
DOSSIER_DETAIL_KEYS = (
    "A2",
    "A3",
    "A4",
    "A5",
    "A6",
    "A7",
    "A8",
    "canary_selection_sha256",
)

# ── canary: selection object (hashed into the dossier) and manifest ─────────
CANARY_TYPES = ("etf", "mutual_fund")
ISIN_CLASSES = ("both", "iu_only", "registry_only", "neither")
CANARY_STRATA = tuple(f"{c}|{t}" for c in ISIN_CLASSES for t in CANARY_TYPES)
CANARY_SELECTION_KEYS = ("allowlist", "cohort_size", "size", "strata")
CANARY_MANIFEST_KEYS = (
    "active_daily_set_sha256",
    "allowlist",
    "audit_config_sha256",
    "audit_contract_sha256",
    "audit_contract_version",
    "audit_report_sha256",
    "capture_bundle_sha256",
    "cohort_query_sha256",
    "cohort_size",
    "eligible_cohort_sha256",
    "eligible_cohort_size",
    "kind",
    "light_revision",
    "max_size",
    "policy_artifact_sha256",
    "policy_hash",
    "policy_id",
    "policy_version",
    "salt",
    "sec_query_contract_sha256",
    "sec_source_contract",
    "selection_sha256",
    "size",
    "strata",
    "verifier_source_sha256",
)

# ── operator plan-v4 receipt (hashes/aggregates only) ────────────────────────
AUDIT_RECEIPT_KEYS = (
    "audit_config_sha256",
    "audit_contract_sha256",
    "audit_contract_version",
    "audit_dossier_sha256",
    "canary_manifest_sha256",
    "canary_selection_sha256",
    "capture_bundle_sha256",
    "captured_at",
    "decision_at",
    "min_matched_synced_at",
    "previous_policy_identity",
    "previous_policy_sha256",
    "sec_query_contract_sha256",
    "sec_rows_sha256",
    "sec_source_contract",
    "sec_valid_until",
    "source_snapshot_file_sha256",
    "source_snapshot_sha256",
    "verifier_source_sha256",
)
# Time keys of the receipt (aware ISO-8601 instants).
AUDIT_RECEIPT_TIME_KEYS = (
    "captured_at",
    "decision_at",
    "min_matched_synced_at",
    "sec_valid_until",
)
# Private append-only ledger binding each governed publication to its plan.
PUBLICATION_RECEIPT_RELATION = "nav_policy_publication_receipts"

AUDIT_CONTRACT = {
    "a4": {
        "baseline": "v1 structural daily without ISIN (drift baseline B)",
        "ceiling_config_key": STRUCTURAL_DAILY_CEILING_KEY,
        "ceiling_default": DEFAULT_STRUCTURAL_DAILY_CEILING,
        "ceiling_population": "structural_pre_claims_daily",
        "closure": "D<=P<=B; B-P first failure in cardinality|registry|ticker|series; "
        "P-D first failure in isin|cusip|figi; |B|=|D|+|B-P|+|P-D|",
        "detail_keys": list(A4_DETAIL_KEYS),
        "retired_config_keys": list(RETIRED_CEILING_KEYS),
    },
    "a8": {
        "class_pattern": SEC_CLASS_PATTERN,
        "detail_keys": list(A8_DETAIL_KEYS),
        "freshness": "every matched row: 0 <= decision_at - synced_at <= "
        "max_synced_age_days; any captured row after decision_at fails; "
        "valid_until = min_matched_synced_at + max_synced_age_days",
        "freshness_keys": list(A8_FRESHNESS_KEYS),
        "lineage_query": SEC_LINEAGE_SQL,
        "max_synced_age_days": SEC_MAX_SYNCED_AGE_DAYS,
        "outcomes": list(A8_OUTCOMES),
        "query": SEC_SQL,
        "query_contract_sha256": SEC_QUERY_CONTRACT_SHA256,
        "relation": SEC_RELATION,
        "rule": "every related row is evaluated before a match: a related row with "
        "a malformed populated class or series identifier is poison, a populated "
        "identifier differing from the ACTIVE is a contradiction, and poison, "
        "contradictions, ambiguity, duplicates, stale or future timestamps FAIL; "
        "an incomplete related row (empty series or ticker) is partial and "
        "NOT_EVALUATED even beside a valid companion; missing is NOT_EVALUATED; "
        "failures dominate gaps; unrelated poison is counted, never fatal; "
        "exactly one valid complete related row and nothing else is a match; "
        "no fallback source",
        "series_pattern": SEC_SERIES_PATTERN,
        "source_contract": SEC_SOURCE_CONTRACT,
        "timestamp_column": SEC_TIMESTAMP_COLUMN,
    },
    "audit_config_version": AUDIT_CONFIG_VERSION,
    "audit_contract_version": AUDIT_CONTRACT_VERSION,
    "audit_receipt_keys": list(AUDIT_RECEIPT_KEYS),
    "audit_receipt_time": {
        "keys": list(AUDIT_RECEIPT_TIME_KEYS),
        "rule": "captured_at == decision_at; min_matched_synced_at <= decision_at "
        "<= sec_valid_until == min_matched_synced_at + max_synced_age_days; the "
        "operator requires captured_at <= db_now <= policy.valid_through and "
        "db_now <= sec_valid_until at check and again under the NAV writer locks, "
        "replay included",
    },
    "audit_version": AUDIT_VERSION,
    "canary": {
        "kind": CANARY_KIND,
        "manifest_keys": list(CANARY_MANIFEST_KEYS),
        "max": CANARY_MAX,
        "rule": "strata (isin class x fund type) then Stage-1 sleeves then salted "
        "sha256(salt+uuid) order; only after a strict all-PASS audit",
        "selection_hash": "sha256(json(selection, sort_keys, compact, ascii) + LF)",
        "selection_keys": list(CANARY_SELECTION_KEYS),
        "strata": list(CANARY_STRATA),
    },
    "capture_kind": CAPTURE_KIND,
    "catalog_source_query_sha256": CATALOG_SOURCE_QUERY_SHA256,
    "cohort_wrapper": "SELECT * FROM (<cohort_query>) AS nav_identity_audit_cohort "
    f"LIMIT {ROW_CEILING + 1}",
    "dossier": {
        "detail_keys": list(DOSSIER_DETAIL_KEYS),
        "input_keys": list(DOSSIER_INPUT_KEYS),
        "keys": list(DOSSIER_KEYS),
    },
    "fetch_batch": FETCH_BATCH,
    "gate_checks": {gate: list(checks) for gate, checks in GATE_CHECKS.items()},
    "plan_version": PLAN_VERSION,
    "publication_receipt": {
        "relation": PUBLICATION_RECEIPT_RELATION,
        "rule": "append-only, written in the publication transaction; binds the "
        "plan-v4 digest, policy bytes/document/hash, the normalized receipt digest "
        "and the dossier/manifest/capture hashes, plus server-stamped event fields: "
        "the exact current pointer published_at instant and the server SHA-256 of "
        "the whole lifecycle partition (policy_id, policy_version), every row with "
        "evidence_id and recorded_at, at most one receipt per pointer event; a "
        "current pointer already at the target is accepted only as an exact replay: "
        "one snapshot where such a receipt of the same identity (and the same plan "
        "digest when the original plan is supplied) still matches the current "
        "pointer instant and the current partition digest; any append to the "
        "partition (document or other instrument, retroactive or future) or any "
        "pointer re-stamp (rollback, re-publication, no-op update) invalidates it; "
        "maintenance-only operations write neither and stay outside",
    },
    "row_ceiling": ROW_CEILING,
    "stage1_margin": f"max(1, ceil({STAGE1_MARGIN_TEXT} * quota)) for quota > 0",
}
AUDIT_CONTRACT_SHA256 = (
    "64c75b3db696132e435523e0f3d072309fc78c72069c2d8942bf7ef89bfd68db"
)
