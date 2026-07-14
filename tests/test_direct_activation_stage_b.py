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
    "harness/direct_activation/live_validation.py",
    "harness/phase0q/decision.py",
    "harness/phase0q/pit.py",
    "harness/phase0q/sleeve.py",
)
# pack-verification + input-canonicalization helpers the guarded runtime path executes
# (compute_input_pack_sha256: manifest -> hashing; p0_contract via the export helper).
PACK_HELPER_MODULES = (
    "src/input_packs/manifest.py",
    "src/input_packs/hashing.py",
    "src/input_packs/p0_contract.py",
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
    assert (STAGE_B / "activation_record.json").is_file()
    assert (STAGE_B / "deploy_record.json").is_file()
    assert (STAGE_B / "backend_flag_inert_record.json").is_file()
    assert (STAGE_B / "schema_migration_record.json").is_file()


# --------------------------------------------------------------------------- #
# module_pins.json matches the tree
# --------------------------------------------------------------------------- #
def test_module_pins_match_recomputed_tree_hashes():
    pins = _load_json(PINS)
    modules = pins["modules"]
    # every pinned module hash equals the recomputed CRLF→LF sha256
    for rel, expected in modules.items():
        assert _sha256_norm(ROOT / rel) == expected, rel
    # the decision-chain closure + export helper + pack-verification helpers are pinned
    assert set(modules) == (set(DECISION_CHAIN_MODULES) | set(PACK_HELPER_MODULES)
                            | {"scripts/p1_export/export_p1_sources.py"})
    # module_pins_sha256 is the canonical hash over the {modules, pack} block
    block = {"modules": modules, "pack": pins["pack"]}
    recomputed = hashlib.sha256(
        json.dumps(block, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert pins["module_pins_sha256"] == recomputed


def test_worker_owns_the_pin_policy_and_matches_the_builder():
    # the runtime pin trust base lives in the worker (EXPECTED_PINNED_MODULES +
    # _pins_block_sha256), NOT the unpinned builder; the builder (generator) and the
    # committed manifest must match it, so the gate cannot redefine its own trust base.
    import src.workers.open_macro_v03 as w
    from harness.direct_activation import build_stage_b_artifacts as b
    assert tuple(w.EXPECTED_PINNED_MODULES) == tuple(b.PINNED_MODULES)
    assert set(w.EXPECTED_PINNED_MODULES) == set(_load_json(PINS)["modules"])
    block = {"modules": {"a": "1"}, "pack": {"x": "y"}}
    assert w._pins_block_sha256(block) == b._canonical_block_sha256(block)


def test_module_pins_pack_matches_certified_pack():
    from harness.direct_activation import build_stage_b_artifacts as builder

    pins = _load_json(PINS)
    certified_pack = (
        ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_003"
    )
    manifest = json.loads((certified_pack / "manifest.json").read_text(encoding="utf-8"))
    assert builder.PACK == certified_pack
    assert pins["pack"] == {
        "input_pack_id": manifest["input_pack_id"],
        "input_pack_sha256": manifest["input_pack_sha256"],
        "canonical_snapshot_sha256": manifest["canonical_snapshot_sha256"],
    }
    assert builder.build_module_pins() == pins


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


def _imported_harness_modules(path: Path) -> set[str]:
    """harness/**.py modules imported by ``path`` (relative or absolute), resolved to
    repo-relative .py paths that actually exist."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    pkg = path.relative_to(ROOT).with_suffix("").parts  # e.g. harness/direct_activation/live_validation
    found: set[str] = set()

    def _add(dotted: str) -> None:
        rel = dotted.replace(".", "/") + ".py"
        if (ROOT / rel).is_file():
            found.add(rel)

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module and (
                    node.module == "harness" or node.module.startswith("harness.")):
                _add(node.module)                               # from harness.x import y (y is a name)
                for a in node.names:
                    _add(f"{node.module}.{a.name}")             # ...or y is a submodule
            elif node.level >= 1:
                base = pkg[:len(pkg) - node.level]              # resolve the relative anchor
                if base and base[0] == "harness":
                    prefix = ".".join(base) + ("." + node.module if node.module else "")
                    _add(prefix)
                    for a in node.names:
                        _add(f"{prefix}.{a.name}")
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name == "harness" or a.name.startswith("harness."):
                    _add(a.name)
    return found


def test_harness_import_closure_is_pinned():
    """A pinned harness module importing another harness module (e.g. decision.py ->
    pit.py, the PIT vintage selector; live_validation.py -> decision/sleeve) must have
    that dependency pinned too. This guards the exact gap that let pit.py drive the
    official decision while escaping verify_module_pins, generalized to every pinned
    harness module."""
    pinned = set(_load_json(PINS)["modules"])
    for rel in sorted(m for m in pinned if m.startswith("harness/")):
        for dep in _imported_harness_modules(ROOT / rel):
            assert dep in pinned, f"{rel} imports unpinned {dep}"


# --------------------------------------------------------------------------- #
# Committed envelope is FULLY BLOCKED
# --------------------------------------------------------------------------- #
ACTIVE_APPROVAL_EVIDENCE = (
    "Approved by Andrei Rachadel (holder of all six owner roles) per "
    "plan_go_decision_record.json (owner decisions 1-5, plan GO, 2026-07-03) and "
    "immediate_activation_decision_record.json (FULL IMMEDIATE ACTIVATION); this "
    "sign-off is ratified by the merge of this activation PR."
)

FINAL_APPROVER_ACT = (
    "I, Andrei Rachadel, as final_approver, activate open_macro_v03 as the ONLY "
    "model with official output from activation, ratified by the merge of this "
    "activation PR"
)


def _expected_active_envelope() -> dict:
    """Deterministic regeneration of the ACTIVE envelope: the committed builder's
    BLOCKED base + EXACTLY the documented B4 flips (the promotion-gate transform).
    Byte-equality against the committed artifact proves no other field moved and
    the serialization discipline (sort_keys / indent=1 / LF) held."""
    from harness.direct_activation.build_stage_b_artifacts import (
        build_activation_envelope)
    envelope = build_activation_envelope()
    envelope.update({
        "A5": "active",
        "runtime_activation": True,
        "activation_allowed": True,
        "allow_db_write": True,
        "db_write_official": True,
        "db_write_mode": "open_macro_v03_new_tables_only",
        "allocator_publish": True,
        "allow_allocator_publish": True,
        "official_result": True,
        "production_endpoint_activation": "none",
        "freeze_ready": False,
        "allowed_tables": sorted([
            "open_macro_v03_decisions", "open_macro_v03_allocations",
            "open_macro_v03_staleness_blocks"]),
        "approval_matrix": {
            role: {"owner": "Andrei Rachadel", "approval_status": "approved",
                   "approval_evidence": ACTIVE_APPROVAL_EVIDENCE,
                   "timestamp": "2026-07-06", "blocking": True}
            for role in sorted(APPROVAL_ROLES)},
        "approval_matrix_complete": True,
        "environment": {
            "railway_service_name": "open-macro-v03-worker",
            "note": ("the ONE approved production Railway service "
                     "(APPROVED_RAILWAY_SERVICE); the deployed service rename "
                     "open-macro-v03 -> open-macro-v03-worker is pending on the owner "
                     "dashboard and the infrastructure converges to this pinned name"),
        },
        "note": ("Stage B envelope ACTIVE: the B4 governance flip ratified by the merge "
                 "of this activation PR. freeze_ready stays false and "
                 "production_endpoint_activation stays 'none' by design (B3 split: the "
                 "post-merge backend_cutover_record only attests the ratified condition)."),
    })
    return envelope


def test_committed_envelope_is_active_exact():
    """B4 flip: the committed envelope is the ACTIVE state, field by field, and is
    byte-identical to the deterministic regeneration from the builder's blocked base
    + the documented flips. The worker's own gate must accept it."""
    env = _load_json(ENVELOPE)
    assert env == _expected_active_envelope()
    committed_bytes = ENVELOPE.read_bytes().replace(b"\r\n", b"\n")
    regenerated = (json.dumps(_expected_active_envelope(), sort_keys=True, indent=1,
                              ensure_ascii=False) + "\n").encode("utf-8")
    assert committed_bytes == regenerated
    import src.workers.open_macro_v03 as w
    assert w.check_governance(env) is None


def test_committed_envelope_approval_matrix_complete_per_role():
    env = _load_json(ENVELOPE)
    matrix = env["approval_matrix"]
    assert set(matrix) == APPROVAL_ROLES
    for role, entry in matrix.items():
        assert entry["owner"] == "Andrei Rachadel", role
        assert entry["approval_status"] == "approved", role
        assert entry["approval_evidence"] == ACTIVE_APPROVAL_EVIDENCE, role
        assert "plan_go_decision_record" in entry["approval_evidence"]
        assert "immediate_activation_decision_record" in entry["approval_evidence"]
        assert "ratified by the merge of this activation PR" in entry["approval_evidence"]
        assert entry["timestamp"] == "2026-07-06", role
        assert entry["blocking"] is True, role
    assert env["approval_matrix_complete"] is True


def test_committed_envelope_keeps_the_b4_blocked_fields_blocked():
    """B3/B4 by design: the Stage C freeze and the production endpoint do NOT flip
    in the activation PR - the post-merge cutover record only attests the ratified
    condition. String-truthy discipline still applies to the blocked fields."""
    env = _load_json(ENVELOPE)
    assert env["freeze_ready"] is False
    assert not _is_truthy_flag(env["freeze_ready"])
    assert env["production_endpoint_activation"] == "none"
    assert env["allowed_tables"] == sorted([
        "open_macro_v03_decisions", "open_macro_v03_allocations",
        "open_macro_v03_staleness_blocks"])
    assert env["environment"]["railway_service_name"] == "open-macro-v03-worker"
    for key, value in _walk(env):
        if key in {"feature_flag_default", "approved", "production_endpoint_activated",
                   "freeze_ready"}:
            assert not _is_truthy_flag(value), f"{key} truthy in the active envelope"


def test_activation_record_pins_the_human_act_and_evidence():
    """The B4 human act: verbatim final_approver act, mirrored complete matrix, the
    A4 advance (this flip, never Stage C), recomputable evidence refs (CRLF->LF),
    the CONDITIONAL ratified endpoint (both values signed; current stays none) and
    the inherited immutability restatement."""
    record = _load_json(STAGE_B / "activation_record.json")
    assert record["artifact_type"] == "stage_b_activation_record"
    assert record["schema_version"] == 1
    assert record["stage"] == "B"
    assert record["stage_b_id"] == "open_macro_v03_direct_activation_stage_b_001"
    assert record["direct_activation_id"] == "open_macro_v03_direct_activation_001"
    assert record["A4"] == "production_active_official"
    assert record["final_approver_act"] == FINAL_APPROVER_ACT
    assert record["approved_on"] == "2026-07-06"

    env = _load_json(ENVELOPE)
    assert record["approval_matrix"] == env["approval_matrix"]
    assert record["approval_matrix_complete"] is True

    refs = record["evidence_refs_sha256_crlf_normalized"]
    assert set(refs["stage_a"]) == {
        "live_validation_record.json", "reproducibility_record.json",
        "slo_conformance_record.json", "slo_threshold_amendment_record.json"}
    assert set(refs["stage_b"]) == {
        "schema_migration_record.json", "deploy_record.json",
        "backend_flag_inert_record.json", "module_pins.json"}
    assert set(refs["dark_launch_readiness"]) == {"dark_launch_manifest.json"}
    # The record is a BYTE-FROZEN pin of the evidence AS RATIFIED at the activation
    # merge (PR #35, 86cf287). Stage A evidence is legitimately RE-MEASURED at each
    # later PR tree (the R3/R5/R8/stage-C re-pins), so the refs are verified against
    # the git blobs at the ratification point, never the mutable working tree — the
    # historical record can neither drift nor be forced to chase re-measurements.
    ratification_merge = "86cf28782ad0e92d5b46d7c6372e757f4c0f4c6f"
    rel_dirs = {
        "stage_a": "artifacts/a5/open_macro_v03_direct_activation_stage_a_001",
        "stage_b": "artifacts/a5/open_macro_v03_direct_activation_stage_b_001",
        "dark_launch_readiness": "artifacts/a5/open_macro_v03_dark_launch_001",
    }
    import subprocess
    for group, entries in refs.items():
        for name, pinned in entries.items():
            blob = subprocess.run(
                ["git", "cat-file", "blob",
                 f"{ratification_merge}:{rel_dirs[group]}/{name}"],
                cwd=ROOT, capture_output=True, check=True).stdout
            actual = hashlib.sha256(blob.replace(b"\r\n", b"\n")).hexdigest()
            assert actual == pinned, f"{group}/{name} diverges from the ratified bytes"

    assert record["production_endpoint_activation"] == "none"
    cond = record["production_endpoint_activation_ratified"]
    assert cond["state"] == "conditional"
    assert cond["value_until_condition"] == "none"
    assert cond["ratified_target_route"] == "/macro/open-macro-v03/allocation"
    assert "backend_cutover_record.json" in cond["condition"]
    assert cond["both_values_signed_here"] is True

    assert record["immutability_constraint"] == {
        "formula_changes": "none", "input_pack_changes": "none",
        "calibration_pack_changes": "none", "contract_v1_changes": "none"}


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
    assert "open_macro_v03_monitor" in text


def test_monitor_worker_is_read_only_by_construction():
    """The B5 monitor must carry NO write path: no DML/DDL keyword anywhere in its
    source (SQL keywords are uppercase by repo convention, so an uppercase scan over
    the module is a faithful read-only proof), and it must not import the main
    worker's write helpers."""
    import re
    text = (ROOT / "src" / "workers" / "open_macro_v03_monitor.py").read_text(
        encoding="utf-8")
    forbidden = re.findall(
        r"\b(INSERT|UPDATE|DELETE|UPSERT|TRUNCATE|ALTER|DROP|CREATE|COPY|GRANT|MERGE)\b",
        text)
    assert forbidden == [], f"monitor contains write-capable keywords: {forbidden}"
    imports = re.findall(r"from src\.workers\.open_macro_v03 import \(([^)]*)\)", text, re.S)
    assert imports, "monitor must import its helpers from the main worker"
    for write_helper in ("publish", "record_staleness_block", "invalidate",
                         "ensure_schema", "_invalidate_both"):
        assert write_helper not in imports[0], f"monitor imports write helper {write_helper}"


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


def test_schema_migration_record_pins_the_applied_and_verified_catalog():
    """B1b evidence: the production DDL application + independent catalog
    verification record exists, loads strictly, pins the exact committed DDL bytes
    (CRLF->LF), covers exactly the three sanctioned tables with verdict match,
    documents the idempotent upsert semantics, and flips nothing."""
    path = STAGE_B / "schema_migration_record.json"
    assert path.is_file()
    record = _load_json(path)  # strict: duplicate keys rejected

    assert record["artifact_type"] == "stage_b_schema_migration_record"
    assert record["schema_version"] == 1
    assert record["stage"] == "B"
    assert record["direct_activation_id"] == "open_macro_v03_direct_activation_001"

    # applied: the DDL byte pins match the committed .sql files
    ddl_files = record["applied"]["ddl_files"]
    assert set(ddl_files) == {
        "schemas/open_macro_v03_decisions.sql",
        "schemas/open_macro_v03_allocations.sql",
        "schemas/open_macro_v03_staleness_blocks.sql",
    }
    for rel, pinned in ddl_files.items():
        assert _sha256_norm(ROOT / rel) == pinned, rel
    assert "t83f4np6x4" in record["applied"]["method"]
    assert "CREATE TABLE IF NOT EXISTS" in record["applied"]["method"]

    # verification: exactly the three sanctioned tables, every verdict "match"
    import src.workers.open_macro_v03 as w
    per_table = record["verification"]["per_table"]
    assert set(per_table) == set(w.ALLOWED_TABLES)
    for table, entry in per_table.items():
        assert entry["verdict"] == "match", table
        assert entry["columns_verified"] is True, table
        assert entry["row_count"] == 0, table
        assert entry["constraints_verified"], table
        assert "tsdbadmin" in entry["grants"], table
    # the auto-name caveat for the inline carry_seed<=as_of CHECK is recorded
    assert any("open_macro_v03_decisions_check" in note
               for note in record["verification"]["notes"])
    assert ("open_macro_v03_decisions_check (c)" in
            per_table["open_macro_v03_decisions"]["constraints_verified"])

    # idempotent upsert semantics documented
    assert "ON CONFLICT (as_of) DO UPDATE" in record["idempotent_upsert_semantics"]
    assert "DO NOTHING" in record["idempotent_upsert_semantics"]
    assert "tests/test_open_macro_v03_worker.py" in record["idempotent_upsert_semantics"]

    # governance: the record flips nothing (walk + string-truthy)
    gov = record["governance"]
    assert gov["A5"] == "blocked"
    assert gov["db_write_mode"] == "none"
    assert gov["production_endpoint_activation"] == "none"
    for key, value in _walk(record):
        if key in FORBIDDEN_TRUE_FIELDS:
            assert not _is_truthy_flag(value), f"{key} truthy in the migration record"


def test_expected_schema_dict_stays_in_sync_with_the_committed_ddl():
    """The worker's EXPECTED_SCHEMA (verify_schema expectations, the B1b evidence
    base) must mirror the committed DDL: the expected tables are exactly the three
    sanctioned ones, and every expected column name and every expected NAMED
    constraint appears in that table's committed DDL text — the base .sql PLUS the
    additive carry_decay_v1 migration (the byte-pinned base files are never edited;
    schema evolution lands as a separate additive migration file)."""
    import src.workers.open_macro_v03 as w

    migration = (ROOT / "schemas" / "open_macro_v03_carry_decay_v1_migration.sql"
                 ).read_text(encoding="utf-8")
    assert set(w.EXPECTED_SCHEMA) == set(w.ALLOWED_TABLES)
    auto_named = {"open_macro_v03_decisions_pkey", "open_macro_v03_allocations_pkey",
                  "open_macro_v03_staleness_blocks_pkey",
                  "open_macro_v03_allocations_as_of_fkey"}
    for table, expected in w.EXPECTED_SCHEMA.items():
        ddl = (ROOT / "schemas" / f"{table}.sql").read_text(encoding="utf-8") + migration
        for column in expected["columns"]:
            assert column in ddl, f"{table}: expected column {column} not in the DDL"
        for conname in expected["constraints"]:
            if conname in auto_named or conname.endswith("_check"):
                continue  # PK/FK + inline auto-named CHECKs are not written by name in the DDL
            assert conname in ddl, f"{table}: expected constraint {conname} not in the DDL"


def test_carry_decay_migration_ddl_is_additive_and_carries_key_tokens():
    """carry_decay_v1 schema evolution (phase0q_005, ratified 2026-07-11): the
    migration is a SEPARATE additive file — the three byte-pinned base DDL files stay
    untouched (their schema_migration_record pins hold) — that widens the
    decision_validity / book CHECKs with the new greppable tokens and adds nullable
    carry-provenance columns. Strictly additive: no destructive statement, existing
    rows stay valid. Applied by the orchestrator in a controlled step (NOT by
    ensure_schema)."""
    path = ROOT / "schemas" / "open_macro_v03_carry_decay_v1_migration.sql"
    assert path.is_file()
    text = path.read_text(encoding="utf-8")

    # the new vocabulary tokens (the Light repo greps for these).
    assert "'carried_expired'" in text
    assert "'center_50'" in text
    # widened CHECKs keep the old vocabulary too (old-shaped rows remain valid).
    assert "'fresh'" in text and "'carried'" in text
    assert "'compressed_50'" in text

    # nullable provenance columns, idempotent adds.
    assert "ADD COLUMN IF NOT EXISTS carry_age_months" in text
    assert "ADD COLUMN IF NOT EXISTS carry_expired" in text
    assert "ADD COLUMN IF NOT EXISTS carry_seed_as_of" in text  # allocations only
    assert "NOT NULL" not in text  # every added column is nullable (additive)

    # strictly additive: no destructive statement anywhere (DROP CONSTRAINT is the
    # sanctioned idempotent widen-recreate pair and is explicitly allowed).
    import re
    assert not re.search(r"\b(DROP\s+TABLE|DROP\s+COLUMN|TRUNCATE|DELETE|UPDATE\s+SET)\b",
                         text, re.IGNORECASE)
    # every DROP is a DROP CONSTRAINT IF EXISTS immediately re-added.
    drops = re.findall(r"DROP\s+CONSTRAINT\s+IF\s+EXISTS\s+(\w+)", text)
    adds = re.findall(r"ADD\s+CONSTRAINT\s+(\w+)", text)
    assert set(drops) <= set(adds), "a dropped constraint must be re-added (widen, not remove)"

    # the consistency constraints that bind the new vocabulary to the provenance flag.
    assert "open_macro_v03_decisions_carry_expired_consistent" in text
    assert "open_macro_v03_allocations_center_book_consistent" in text

    # governance: applied by the orchestrator, never silently by the worker.
    import src.workers.open_macro_v03 as w
    assert "schemas/open_macro_v03_carry_decay_v1_migration.sql" not in w._SCHEMAS
