"""Guard tests for the open_macro_v03 Stage B direct-activation package.

Pins the committed governance envelope FULLY BLOCKED, verifies the module pins match
the tree, proves the decision-chain import closure is pinned (no formula module pulls
in an unpinned src module), and pins the lock/registration/DDL surface. These guards
are the promotion-gate path: the flips land only in the Stage B PR's final review.
"""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
STAGE_B = ROOT / "artifacts" / "a5" / "open_macro_v03_direct_activation_stage_b_001"
ENVELOPE = STAGE_B / "activation_envelope.json"
PINS = STAGE_B / "module_pins.json"

# The decision-chain pure modules whose src-import closure must be fully pinned.
DECISION_CHAIN_MODULES = (
    "src/quadrant_score.py",
    "src/macro_transforms.py",
    "src/macro_sources.py",
    "src/quadrant_confidence.py",
    "src/quadrant_hysteresis.py",
    "src/quadrant_assemble.py",
    "src/quadrant_snapshot.py",
    "src/quadrant_staleness.py",
    "harness/phase0q/decision.py",
    "harness/phase0q/sleeve.py",
)
PINNED_SRC_MODULES = {
    "src.quadrant_score", "src.macro_transforms", "src.macro_sources",
    "src.quadrant_confidence", "src.quadrant_hysteresis", "src.quadrant_assemble",
    "src.quadrant_snapshot", "src.quadrant_staleness",
}
# Infra allowlist: quadrant_assemble re-exports LOCK_REGIME_QUADRANT from src.db, an
# infrastructure (locks/connection) module that carries no decision-formula value and
# is transitively guarded by the prefix-hash pin, not by the formula closure.
SRC_INFRA_ALLOWLIST = {"src.db"}

APPROVAL_ROLES = {
    "technical_owner", "quant_owner", "risk_owner", "operations_owner",
    "product_portfolio_owner", "final_approver",
}

FORBIDDEN_TRUE_FIELDS = {
    "runtime_activation", "activation_allowed", "allow_db_write", "db_write_official",
    "allocator_publish", "allow_allocator_publish", "official_result", "freeze_ready",
    "approval_matrix_complete", "feature_flag_default", "approved",
}


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise ValueError(f"duplicate JSON key {key!r}")
        seen[key] = value
    return seen


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"),
                      object_pairs_hook=_reject_duplicate_keys)


def _is_truthy_flag(value: Any) -> bool:
    return value is True or (isinstance(value, str) and value.strip().lower() == "true")


def _walk(node: Any):
    if isinstance(node, dict):
        for key, value in node.items():
            yield key, value
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _sha256_norm(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


# --------------------------------------------------------------------------- #
# Artifacts exist
# --------------------------------------------------------------------------- #
def test_stage_b_artifacts_exist():
    assert ENVELOPE.is_file()
    assert PINS.is_file()


# --------------------------------------------------------------------------- #
# module_pins.json matches the tree
# --------------------------------------------------------------------------- #
def test_module_pins_match_recomputed_tree_hashes():
    pins = _load_json(PINS)
    modules = pins["modules"]
    # every pinned module hash equals the recomputed CRLF→LF sha256
    for rel, expected in modules.items():
        assert _sha256_norm(ROOT / rel) == expected, rel
    # the 11-module closure + export helper set is pinned
    assert set(modules) == set(DECISION_CHAIN_MODULES) | {
        "scripts/p1_export/export_p1_sources.py"}
    # module_pins_sha256 is the canonical hash over the {modules, pack} block
    block = {"modules": modules, "pack": pins["pack"]}
    recomputed = hashlib.sha256(
        json.dumps(block, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert pins["module_pins_sha256"] == recomputed


def test_module_pins_pack_matches_certified_pack():
    pins = _load_json(PINS)
    manifest = json.loads(
        (ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"
         / "manifest.json").read_text(encoding="utf-8"))
    assert pins["pack"]["input_pack_sha256"] == manifest["input_pack_sha256"]
    assert pins["pack"]["canonical_snapshot_sha256"] == manifest["canonical_snapshot_sha256"]


# --------------------------------------------------------------------------- #
# Import closure (AST): no formula module imports an unpinned src module
# --------------------------------------------------------------------------- #
def _imported_src_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "src" or alias.name.startswith("src."):
                    found.add(".".join(alias.name.split(".")[:2]))
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            if node.module == "src":
                for alias in node.names:
                    found.add(f"src.{alias.name}")
            elif node.module.startswith("src."):
                found.add(".".join(node.module.split(".")[:2]))
    return found


def test_decision_chain_import_closure_is_pinned():
    allowed = PINNED_SRC_MODULES | SRC_INFRA_ALLOWLIST
    for rel in DECISION_CHAIN_MODULES:
        imported = _imported_src_modules(ROOT / rel)
        stray = imported - allowed
        assert stray == set(), f"{rel} imports unpinned src modules: {stray}"


def test_decision_and_sleeve_import_only_pinned_src_modules():
    # the two harness formula modules may NOT lean on the infra allowlist
    for rel in ("harness/phase0q/decision.py", "harness/phase0q/sleeve.py"):
        imported = _imported_src_modules(ROOT / rel)
        assert imported <= PINNED_SRC_MODULES, f"{rel}: {imported - PINNED_SRC_MODULES}"


# --------------------------------------------------------------------------- #
# Committed envelope is FULLY BLOCKED
# --------------------------------------------------------------------------- #
def test_committed_envelope_is_fully_blocked():
    env = _load_json(ENVELOPE)
    assert env["A5"] == "blocked"
    assert env["runtime_activation"] is False
    assert env["activation_allowed"] is False
    assert env["allow_db_write"] is False
    assert env["db_write_official"] is False
    assert env["db_write_mode"] == "none"
    assert env["allocator_publish"] is False
    assert env["allow_allocator_publish"] is False
    assert env["official_result"] is False
    assert env["production_endpoint_activation"] == "none"
    assert env["freeze_ready"] is False
    assert env["allowed_tables"] == []
    assert env["approval_matrix_complete"] is False
    assert env["environment"] is None


def test_committed_envelope_approval_matrix_all_pending():
    env = _load_json(ENVELOPE)
    matrix = env["approval_matrix"]
    assert set(matrix) == APPROVAL_ROLES
    for role, entry in matrix.items():
        assert entry["approval_status"] == "pending", role
        assert entry["blocking"] is True, role
        assert entry["owner"] is None, role


def test_committed_envelope_has_no_truthy_activation_flags():
    env = _load_json(ENVELOPE)
    for key, value in _walk(env):
        if key in FORBIDDEN_TRUE_FIELDS:
            assert not _is_truthy_flag(value), f"{key} is truthy in the blocked envelope"
        if key in {"A5", "a5_status"}:
            assert value == "blocked", f"{key}={value!r}"


def test_envelope_loader_rejects_duplicate_keys():
    with pytest.raises(ValueError, match="duplicate JSON key 'runtime_activation'"):
        json.loads('{"runtime_activation": false, "runtime_activation": true}',
                   object_pairs_hook=_reject_duplicate_keys)


def test_string_truthy_semantics_detected():
    # a JSON string "true" must be treated as truthy (defeats a string-spoof flip)
    assert _is_truthy_flag("true") is True
    assert _is_truthy_flag("True") is True
    assert _is_truthy_flag(False) is False
    assert _is_truthy_flag("false") is False


# --------------------------------------------------------------------------- #
# Lock id / registration / DDL surface
# --------------------------------------------------------------------------- #
def test_lock_id_is_unique_in_registry():
    import src.db as db
    assert db.LOCK_OPEN_MACRO_V03 == 900_215
    others = {name: val for name, val in vars(db).items()
              if name.startswith("LOCK_") and name != "LOCK_OPEN_MACRO_V03"}
    assert db.LOCK_OPEN_MACRO_V03 not in others.values(), \
        f"900_215 collides with {[n for n, v in others.items() if v == 900_215]}"


def test_run_worker_help_lists_open_macro_v03():
    text = (ROOT / "src" / "run_worker.py").read_text(encoding="utf-8")
    assert "open_macro_v03" in text


def test_ddl_files_exist_and_carry_key_constraints():
    dec = (ROOT / "schemas" / "open_macro_v03_decisions.sql").read_text(encoding="utf-8")
    alloc = (ROOT / "schemas" / "open_macro_v03_allocations.sql").read_text(encoding="utf-8")
    stale = (ROOT / "schemas" / "open_macro_v03_staleness_blocks.sql").read_text(encoding="utf-8")

    # decisions: PK, quadrant/validity CHECKs, invalidation + validity/seed CHECKs, index
    assert "CREATE TABLE IF NOT EXISTS open_macro_v03_decisions" in dec
    assert "as_of                DATE          PRIMARY KEY" in dec
    assert "quadrant IN ('recovery', 'expansion', 'slowdown', 'contraction')" in dec
    assert "decision_validity IN ('fresh', 'carried')" in dec
    assert "(valid_status = 'invalidated') = (invalidated_at IS NOT NULL)" in dec
    assert "carry_seed_as_of = as_of" in dec and "carry_seed_as_of < as_of" in dec
    assert "idx_open_macro_v03_decisions_valid" in dec
    assert "create_hypertable" not in dec  # flat table, not a hypertable

    # allocations: FK, six weights, sum/cap/floor CHECKs
    assert "REFERENCES open_macro_v03_decisions (as_of)" in alloc
    for col in ("w_spy", "w_tlt", "w_tip", "w_gld", "w_dbc", "w_shy"):
        assert col in alloc
    assert "abs(w_spy + w_tlt + w_tip + w_gld + w_dbc + w_shy - 1) < 1e-9" in alloc
    assert "risk_assets_weight <= risk_cap + 1e-9" in alloc
    assert "defensive_assets_weight >= defensive_floor - 1e-9" in alloc
    assert "create_hypertable" not in alloc

    # staleness ledger: PK, jsonb detail, provenance hashes, NO lifecycle columns
    assert "CREATE TABLE IF NOT EXISTS open_macro_v03_staleness_blocks" in stale
    assert "as_of                DATE        PRIMARY KEY" in stale
    assert "stale_detail         JSONB       NOT NULL" in stale
    assert "valid_status" not in stale and "valid_until" not in stale
    assert "create_hypertable" not in stale
