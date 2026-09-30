"""The shared audit contract leaf: pure data, frozen digest, no coupling."""

from __future__ import annotations

import ast
import copy
import hashlib
import json
from pathlib import Path

import pytest

from scripts import fund_nav_readiness_schema as operator
from scripts import nav_identity_audit_contract as contract
from scripts import verify_fund_nav_identity_v2 as verifier

ROOT = Path(contract.__file__).parents[1]


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add(node.module or "")
    return names


def test_leaf_imports_nothing_but_future():
    assert _imports(Path(contract.__file__)) == {"__future__"}


def test_frozen_contract_digest_matches_declaration():
    digest = hashlib.sha256(
        json.dumps(
            contract.AUDIT_CONTRACT,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()
    assert digest == contract.AUDIT_CONTRACT_SHA256
    assert verifier._contract_consistent() is True
    sec = hashlib.sha256(
        (contract.SEC_SQL + "\n" + contract.SEC_LINEAGE_SQL).encode("utf-8")
    ).hexdigest()
    assert sec == contract.SEC_QUERY_CONTRACT_SHA256


def test_operator_never_imports_auditor_or_classifier():
    imported = _imports(Path(operator.__file__))
    assert "scripts.verify_fund_nav_identity_v2" not in imported
    assert "scripts.generate_fund_nav_policy_v1" not in imported
    assert "scripts.nav_identity_audit_contract" in imported


def test_contract_constants_agree_across_auditor_operator_and_generator():
    from src.workers import _nav_policy as policy

    assert (
        contract.CATALOG_SOURCE_QUERY_SHA256
        == policy.SOURCE_QUERY_SHA256
        == verifier.QUERY_SHA256
    )
    # Own SQL literals, byte-identical projections (generator vs leaf).
    assert policy.SEC_QUERY == contract.SEC_SQL
    assert policy.SEC_LINEAGE_QUERY == contract.SEC_LINEAGE_SQL
    assert policy.SEC_RELATION == contract.SEC_RELATION
    assert policy.SEC_CLASS_PATTERN == contract.SEC_CLASS_PATTERN
    assert policy.SEC_SERIES_PATTERN == contract.SEC_SERIES_PATTERN
    assert policy.SEC_MAX_SYNCED_AGE.days == contract.SEC_MAX_SYNCED_AGE_DAYS
    assert policy.SEC_FAILURE_CODES == contract.SEC_FAILURE_CODES
    assert policy.SEC_CONFLICT_CODES == contract.SEC_CONFLICT_CODES
    # The generator's (unchanged) gap/conflict partition covers the Round7
    # integrity/stale/missing partition exactly; the leaf no longer declares
    # a gap family.
    assert set(policy.SEC_GAP_CODES) == {
        "sec.incomplete",
        contract.SEC_STALE_CODE,
        contract.SEC_MISSING_CODE,
    }
    assert set(contract.SEC_INTEGRITY_CODES) == set(policy.SEC_CONFLICT_CODES) | {
        "sec.incomplete"
    }
    assert policy.GENERATOR_VERSION == contract.GENERATOR_VERSION
    assert policy.CURRENT_CATALOG_QUERY_VERSION == contract.CATALOG_QUERY_VERSION
    assert policy.IDENTITY_CONTRACT_VERSION == contract.IDENTITY_CONTRACT_VERSION
    assert sorted(policy.RETIRED_GENERATOR_VERSIONS) == list(
        contract.RETIRED_GENERATOR_VERSIONS
    )
    assert sorted(policy.RETIRED_CATALOG_QUERY_VERSIONS) == list(
        contract.RETIRED_CATALOG_QUERY_VERSIONS
    )
    assert operator.PLAN_VERSION == contract.PLAN_VERSION == "nav-schema-plan-v4"
    assert set(contract.GATE_CHECKS) == set(contract.GATE_NAMES)
    assert all(contract.GATE_CHECKS[g] for g in contract.GATE_NAMES)
    assert contract.AUDIT_CONTRACT["gate_checks"] == {
        g: list(c) for g, c in contract.GATE_CHECKS.items()
    }


def test_contract_mismatch_blocks_auditor_import_guard(monkeypatch):
    monkeypatch.setattr(verifier, "AUDIT_CONTRACT_SHA256", "0" * 64)
    assert verifier._contract_consistent() is False


def test_sec_source_is_only_company_tickers_mf():
    assert contract.SEC_RELATION == "public.sec_company_tickers_mf"
    assert contract.SEC_TIMESTAMP_COLUMN == "updated_at"
    for text in (contract.SEC_SQL, contract.SEC_LINEAGE_SQL):
        assert "public.sec_company_tickers_mf" in text
        assert "fetched_at" not in text


ROUND6_CONTRACT_SHA256 = (
    "24a2c2fb989ef832f778a69d887d870c6e990573e21cd7564403862eb461b1ff"
)


def test_round7_contract_is_frozen_and_round3_to_round6_are_retired():
    """Round7 A8 rule: new audit literal; Round3-6 pins never match."""
    assert contract.AUDIT_CONTRACT_VERSION == "nav-identity-audit-contract-v3-round7"
    assert contract.AUDIT_CONTRACT_SHA256 == (
        "9941902de05c4006882bfb51b2816a357c91f2405e2a9f24c2369d3149ced244"
    )
    assert contract.CATALOG_SOURCE_QUERY_SHA256 == (
        "47cd5565cad722bd2ca7377f5206ea39d9460861394b7d09f87c113d5b39b2b5"
    )
    for retired in (
        "1ef74c426526f5308223921c8297516c9fa4ac4034737fad59cad668f73b0001",  # round3
        "64c75b3db696132e435523e0f3d072309fc78c72069c2d8942bf7ef89bfd68db",  # round4
        "0d237212e1e95a6e1e0494e071170d174481c29662a27b7bf9e7ac0bca3ec2c7",  # round5
        ROUND6_CONTRACT_SHA256,
    ):
        assert contract.AUDIT_CONTRACT_SHA256 != retired
    # The v2 three-source digest is retired with the v2 generator.
    assert contract.CATALOG_SOURCE_QUERY_SHA256 != (
        "39ca1c5d3f09d989abb8c7fdfd652ef3832683d99486ad006610a37fb0cb97f7"
    )
    assert contract.AUDIT_VERSION == "nav-identity-audit-v3"
    assert contract.AUDIT_CONFIG_VERSION == "nav-identity-audit-config-v3"
    assert contract.GENERATOR_VERSION == "fund-nav-policy-generator-v3"
    assert contract.SOURCE_SNAPSHOT_KIND == "nav-current-catalog-source-snapshot-v3"
    assert "fund-nav-policy-generator-v2" in contract.RETIRED_GENERATOR_VERSIONS
    assert "nav-current-catalog-snapshot-v2" in contract.RETIRED_CATALOG_QUERY_VERSIONS
    rule = contract.AUDIT_CONTRACT["publication_receipt"]["rule"]
    for phrase in (
        # Round4 replay semantics, preserved.
        "pointer published_at",
        "whole lifecycle partition",
        "evidence_id and recorded_at",
        "one receipt per pointer event",
        "same plan digest",
        "maintenance-only",
        # Round5 B-forte publication.
        "latest receipt of that version (by commit xid)",
        "no receipt and an empty partition",
        "extras never adopted",
        "baseline count plus the rows this publication inserted",
        "target_partition_diverged",
        "publication_plan_consumed",
        "rollback-only",
    ):
        assert phrase in rule
    # plan-v4 shape unchanged: no new plan field, same version literal and kinds.
    assert contract.PLAN_VERSION == operator.PLAN_VERSION == "nav-schema-plan-v4"
    assert contract.CAPTURE_KIND == "nav-identity-audit-capture-v3-round7"
    # Manifest shape unchanged: the v3-round7 contract/hash it carries binds it.
    assert contract.CANARY_KIND == "nav-policy-v2-canary-manifest-round2"
    assert "evidence_partition_digest" not in contract.AUDIT_RECEIPT_KEYS
    assert "pointer_published_at" not in contract.AUDIT_RECEIPT_KEYS
    for code in ("target_partition_diverged", "publication_plan_consumed"):
        assert operator._SAFE_CODES.fullmatch(code)


def test_round7_sec_exclusion_rule_is_declared_exactly():
    """rho = 1/10: integrity 0, stale and missing bounded separately by B."""
    assert contract.SEC_INTEGRITY_CODES == (
        "sec.declared_class_invalid",
        "sec.poisoned_mapping",
        "sec.contradiction",
        "sec.ambiguous",
        "sec.incomplete",
    )
    assert (contract.SEC_STALE_CODE, contract.SEC_MISSING_CODE) == (
        "sec.stale",
        "sec.missing",
    )
    # A partition of the SEC failure codes: integrity | {stale} | {missing}.
    partition = [*contract.SEC_INTEGRITY_CODES, "sec.stale", "sec.missing"]
    assert sorted(partition) == sorted(contract.SEC_FAILURE_CODES)
    assert len(partition) == len(set(partition))
    assert contract.SEC_INTEGRITY_CEILING == 0
    assert contract.SEC_CONFLICT_CEILING == 0
    assert (
        contract.SEC_EXCLUSION_FRACTION_NUMERATOR,
        contract.SEC_EXCLUSION_FRACTION_DENOMINATOR,
    ) == (1, 10)
    for value in (
        contract.SEC_EXCLUSION_FRACTION_NUMERATOR,
        contract.SEC_EXCLUSION_FRACTION_DENOMINATOR,
        contract.SEC_INTEGRITY_CEILING,
        contract.SEC_CONFLICT_CEILING,
    ):
        assert type(value) is int
    assert contract.SEC_RETIRED_CONFIG_KEYS == ("gap_ceiling",)
    for retired in ("SEC_GAP_CEILING", "SEC_GAP_CODES", "SEC_GAP_CEILING_KEY"):
        assert not hasattr(contract, retired)
    a8 = contract.AUDIT_CONTRACT["a8"]
    assert a8["exclusion_fraction"] == {"numerator": 1, "denominator": 10}
    assert a8["integrity_ceiling"] == 0 and a8["conflict_ceiling"] == 0
    assert "gap_ceiling" not in a8 and "gap_ceiling_key" not in a8
    assert a8["exclusion_keys"] == [
        "bound",
        "by_code",
        "c_size",
        "integrity",
        "missing",
        "stale",
    ]
    assert "gap_ceiling" not in a8["detail_keys"]
    assert "conflict_ceiling" not in a8["detail_keys"]
    assert "gap_codes" not in contract.AUDIT_CONTRACT["sec_classification"]
    assert contract.GATE_CHECKS["A8"] == (
        "active_nonempty",
        "all_active_matched",
        "sec_integrity_zero",
        "sec_missing_within_bound",
        "sec_stale_within_bound",
        "source_fresh_within_max_age",
        "source_lineage_verified",
        "synced_not_in_future",
    )
    rule = a8["exclusion_rule"]
    for phrase in ("(N * numerator) // denominator", "no minimum", "separately"):
        assert phrase in " ".join(rule.values())
    assert "structural_pre_claims - structural_claim_failures" in rule["conservation"]


def _digest(document) -> str:
    return hashlib.sha256(
        json.dumps(
            document, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    ).hexdigest()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda c: c["a8"].update(exclusion_fraction={"numerator": 1, "denominator": 9}),
        lambda c: c["a8"].update(
            exclusion_fraction={"numerator": 2, "denominator": 20}
        ),
        lambda c: c["a8"].update(
            exclusion_fraction={"numerator": True, "denominator": 10}
        ),
        lambda c: c["a8"].update(
            exclusion_fraction={"numerator": 1.0, "denominator": 10}
        ),
        lambda c: c["a8"].update(integrity_ceiling=1),
        lambda c: c["a8"].update(integrity_ceiling=False),
        lambda c: c["a8"].update(conflict_ceiling=1),
        lambda c: c["a8"].update(gap_ceiling=23),
        lambda c: c["a8"].update(retired_sec_config_keys=[]),
        lambda c: c["a8"]["exclusion_keys"].remove("stale"),
        lambda c: c["sec_classification"].update(
            integrity_codes=c["sec_classification"]["conflict_codes"]
        ),
        lambda c: c["sec_classification"].update(stale_code="sec.missing"),
        lambda c: c["sec_classification"].update(
            gap_codes=["sec.incomplete", "sec.stale", "sec.missing"]
        ),
    ],
    ids=[
        "fraction_1_9",
        "fraction_equivalent_2_20",
        "fraction_bool",
        "fraction_float",
        "integrity_1",
        "integrity_bool",
        "conflict_1",
        "gap_ceiling_back",
        "retired_keys_dropped",
        "exclusion_keys_shrunk",
        "integrity_without_incomplete",
        "stale_code_swapped",
        "gap_codes_back",
    ],
)
def test_realigned_digest_does_not_hide_a_semantic_change(monkeypatch, mutate):
    """A literal edited AND re-hashed still fails the semantic confrontation."""
    mutated = copy.deepcopy(contract.AUDIT_CONTRACT)
    mutate(mutated)
    monkeypatch.setattr(verifier, "AUDIT_CONTRACT", mutated)
    monkeypatch.setattr(verifier, "AUDIT_CONTRACT_SHA256", _digest(mutated))
    assert verifier._contract_consistent() is False


def test_auditor_gate_checks_must_be_the_round7_checks(monkeypatch):
    checks = dict(contract.GATE_CHECKS)
    checks["A8"] = tuple(
        name for name in checks["A8"] if name != "sec_missing_within_bound"
    ) + ("sec_gap_within_ceiling",)
    monkeypatch.setattr(verifier, "GATE_CHECKS", checks)
    assert verifier._contract_consistent() is False


def test_round7_leaves_ddl_and_catalog_unchanged():
    """Round5-7 are runtime-only; the Round4 schema artifacts are pinned."""
    ddl = (ROOT / "schemas" / "fund_nav_readiness_v1.sql").read_bytes()
    catalog = (ROOT / "schemas" / "fund_nav_readiness_v1.catalog.json").read_bytes()
    assert hashlib.sha256(ddl).hexdigest() == (
        "687b019cfd546aa69e7a6024e3fd3cb615b3e84622f24ec3b7b6ff2e5a5c9cf4"
    )
    assert hashlib.sha256(catalog).hexdigest() == (
        "c49223f7806b731e50eb8c8ae81a320c389fc1b557eee9ecacce92592e36ffe7"
    )


def test_repository_config_is_pinned_to_the_leaf():
    config = json.loads((ROOT / "configs" / "nav_identity_audit_v3.json").read_bytes())
    assert config["audit_contract_version"] == contract.AUDIT_CONTRACT_VERSION
    assert config["audit_config_version"] == contract.AUDIT_CONFIG_VERSION
    assert "gap_ceiling" not in config["sec"]
    assert config["sec"]["exclusion_fraction"] == {
        "numerator": contract.SEC_EXCLUSION_FRACTION_NUMERATOR,
        "denominator": contract.SEC_EXCLUSION_FRACTION_DENOMINATOR,
    }
    assert config["sec"]["conflict_ceiling"] == contract.SEC_CONFLICT_CEILING
    assert operator.AUDIT_CONFIG == ROOT / "configs" / "nav_identity_audit_v3.json"
    # Independent operator validation of the same repository bytes.
    operator._validate_audit_config(operator.AUDIT_CONFIG.read_bytes())
    # The v2 config is kept byte-for-byte as the historical blocked-audit record.
    historical = json.loads(
        (ROOT / "configs" / "nav_identity_audit_v2.json").read_bytes()
    )
    assert historical["audit_contract_version"] == (
        "nav-identity-audit-contract-v2-round5"
    )
    assert config["sec"]["relation"] == contract.SEC_RELATION
    assert config["sec"]["query_contract_sha256"] == contract.SEC_QUERY_CONTRACT_SHA256
    assert (
        config["structural_daily_ceiling"] == contract.DEFAULT_STRUCTURAL_DAILY_CEILING
    )
    assert "active_ceiling" not in config
