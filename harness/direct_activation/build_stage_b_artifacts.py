"""Generate the Stage B governance artifacts (module pins + BLOCKED envelope).

Usage (from the repo root):

    python -m harness.direct_activation.build_stage_b_artifacts

Writes two committed artifacts under
``artifacts/a5/open_macro_v03_direct_activation_stage_b_001/``:

* ``module_pins.json`` — the sha256 (CRLF→LF normalized) of EACH consumed pure
  module of the decision chain + sleeve + the P1 export format helpers, plus the
  pack v3 pins and a ``module_pins_sha256`` over the canonicalized pin block. The
  runtime worker verifies these before any DB access, so a Stage B PR cannot
  silently alter scoring/confidence/hysteresis/source-weights/latch/sleeve behaviour.
* ``activation_envelope.json`` — the governance envelope INITIALLY FULLY BLOCKED.
  Every activation flip is false/none/blocked and every approval-matrix role is
  pending; the flips happen ONLY in the Stage B PR's final review. So the worker,
  on any real run today, returns ``governance_blocked`` — correct by construction.

Serialization: ``sort_keys`` + ``indent=1`` + a trailing LF newline.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STAGE_B = ROOT / "artifacts" / "a5" / "open_macro_v03_direct_activation_stage_b_001"
PACK = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_003"

# The FULL transitive closure of the decision-chain pure modules + the harness sleeve
# + the P1 export format helpers. Every module the runtime worker consumes to compute
# and read the certified candidate. NB: harness/phase0q/pit.py is the PIT vintage
# selector (``PitIndex``) that decision.py imports and run_decision_series builds at
# runtime — pinned here so a change to PIT selection cannot alter the official
# decision without tripping verify_module_pins. NB: harness/direct_activation/
# live_validation.py is pinned too — the worker imports compose_rows / consumable_today /
# staleness_report / _VINTAGE_KEY from it, so a change to input composition, carry
# selection, or the staleness block/publish decision cannot escape the pin gate.
PINNED_MODULES = (
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
    "scripts/p1_export/export_p1_sources.py",
    # pack-verification + input-canonicalization helpers the GUARDED runtime path runs:
    # compute_input_pack_sha256 (manifest -> hashing) for the pack aggregate check, and
    # p0_contract.normalize_* (via the export helper) for input hashing. Pinned so those
    # semantics cannot change without a new module_pins_sha256.
    "src/input_packs/manifest.py",
    "src/input_packs/hashing.py",
    "src/input_packs/p0_contract.py",
)

APPROVAL_ROLES = (
    "technical_owner",
    "quant_owner",
    "risk_owner",
    "operations_owner",
    "product_portfolio_owner",
    "final_approver",
)


def _sha256_norm(path: Path) -> str:
    """sha256 of file bytes with CRLF→LF normalization (git-checkout agnostic)."""
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def _canonical_block_sha256(block: dict) -> str:
    """sha256 over the canonicalized pin block (stable across writers)."""
    return hashlib.sha256(
        json.dumps(block, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def build_module_pins() -> dict:
    manifest = json.loads((PACK / "manifest.json").read_text(encoding="utf-8"))
    block = {
        "modules": {rel: _sha256_norm(ROOT / rel) for rel in PINNED_MODULES},
        "pack": {
            "input_pack_id": manifest["input_pack_id"],
            "input_pack_sha256": manifest["input_pack_sha256"],
            "canonical_snapshot_sha256": manifest["canonical_snapshot_sha256"],
        },
    }
    return {
        "artifact_type": "open_macro_v03_stage_b_module_pins",
        "schema_version": 1,
        "stage": "B",
        "stage_b_id": "open_macro_v03_direct_activation_stage_b_001",
        "modules": block["modules"],
        "pack": block["pack"],
        "module_pins_sha256": _canonical_block_sha256(block),
    }


def build_activation_envelope() -> dict:
    """The Stage B envelope, committed FULLY BLOCKED. The flips land only in the PR's
    final review; the guard tests cover this blocked state and the worker gate covers
    both states."""
    return {
        "artifact_type": "open_macro_v03_stage_b_activation_envelope",
        "schema_version": 1,
        "stage": "B",
        "stage_b_id": "open_macro_v03_direct_activation_stage_b_001",
        "direct_activation_id": "open_macro_v03_direct_activation_001",
        "A5": "blocked",
        "runtime_activation": False,
        "activation_allowed": False,
        "allow_db_write": False,
        "db_write_official": False,
        "db_write_mode": "none",
        "allocator_publish": False,
        "allow_allocator_publish": False,
        "official_result": False,
        "production_endpoint_activation": "none",
        "freeze_ready": False,
        "allowed_tables": [],
        "approval_matrix": {
            role: {
                "owner": None,
                "approval_status": "pending",
                "approval_evidence": None,
                "timestamp": None,
                "blocking": True,
            }
            for role in APPROVAL_ROLES
        },
        "approval_matrix_complete": False,
        "environment": None,
        "note": ("Stage B envelope committed FULLY BLOCKED. Every activation flip and "
                 "the six approval-matrix roles flip ONLY in the Stage B PR's final "
                 "review; a real worker run today returns governance_blocked."),
    }


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8", newline="\n")


def main() -> int:
    _write(STAGE_B / "module_pins.json", build_module_pins())
    _write(STAGE_B / "activation_envelope.json", build_activation_envelope())
    print(f"wrote module_pins.json ({len(PINNED_MODULES)} modules) + "
          "activation_envelope.json (FULLY BLOCKED) under "
          f"{STAGE_B.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
