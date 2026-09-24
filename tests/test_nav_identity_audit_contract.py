"""The shared audit contract leaf: pure data, frozen digest, no coupling."""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

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
    from src.workers._nav_policy import SOURCE_QUERY_SHA256

    assert (
        contract.CATALOG_SOURCE_QUERY_SHA256
        == SOURCE_QUERY_SHA256
        == verifier.QUERY_SHA256
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


def test_repository_config_is_pinned_to_the_leaf():
    config = json.loads((ROOT / "configs" / "nav_identity_audit_v2.json").read_bytes())
    assert config["audit_contract_version"] == contract.AUDIT_CONTRACT_VERSION
    assert config["sec"]["relation"] == contract.SEC_RELATION
    assert config["sec"]["query_contract_sha256"] == contract.SEC_QUERY_CONTRACT_SHA256
    assert (
        config["structural_daily_ceiling"] == contract.DEFAULT_STRUCTURAL_DAILY_CEILING
    )
    assert "active_ceiling" not in config
