"""Emit (does NOT run) the ordered upload plan for a built phase0q cloud bundle.

Given a bundle directory produced by :mod:`harness.phase0q_cloud.bundle`, this
module reads ``object_store_manifest.json`` and emits, PURELY BY CONSTRUCTION:

  * ``upload_plan.json`` — the ordered list of ``lean cloud object-store set
    <key> <path>`` commands (every object_files entry, then the MANIFEST LAST),
    plus post-upload verification commands (``lean cloud object-store ls`` +
    per-object ``get`` sha checks).
  * ``upload_plan.sh`` — the same, as a human-readable review script.

ZERO ``lean`` invocations. ZERO network calls. ZERO uploads. This only reads the
bundle manifest and writes two plan files describing what the orchestrator will run
separately after review. It never executes any command in the plan.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .bundle import canonical_json_bytes, file_sha256


def _read_manifest(bundle_dir: Path) -> dict[str, Any]:
    manifest_path = bundle_dir / "object_store_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"no object_store_manifest.json in {bundle_dir}; run "
            "`python -m harness.phase0q_cloud.bundle <harness_commit>` first"
        )
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _ordered_object_commands(bundle_dir: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """One command per object_files entry, in a deterministic sorted order.

    The manifest itself is NOT in object_files; it is appended LAST by the caller.
    Each command carries the expected content_sha256 so the plan is self-verifying.
    """
    object_files = manifest.get("object_files") or {}
    commands: list[dict[str, Any]] = []
    for rel in sorted(object_files):
        item = object_files[rel]
        local_path = bundle_dir / item["relative_path"]
        # Re-derive sha from the on-disk bundle so a plan can never point at a drifted
        # object without the reviewer seeing the mismatch (pure read; no upload).
        actual_sha = file_sha256(local_path) if local_path.is_file() else None
        commands.append({
            "order": len(commands) + 1,
            "relative_path": item["relative_path"],
            "object_store_key": item["object_store_key"],
            "local_path": str(local_path),
            "expected_content_sha256": item["content_sha256"],
            "on_disk_content_sha256": actual_sha,
            "drift": actual_sha != item["content_sha256"],
            "argv": ["lean", "cloud", "object-store", "set",
                     item["object_store_key"], str(local_path)],
        })
    return commands


def build_upload_plan(bundle_dir: str | Path) -> dict[str, Any]:
    """Construct the ordered upload plan dict (no execution)."""
    bundle_dir = Path(bundle_dir)
    manifest = _read_manifest(bundle_dir)
    prefix = manifest["object_store_prefix_immutable"]
    manifest_key = manifest["object_store_manifest_key"]
    manifest_path = bundle_dir / "object_store_manifest.json"

    object_commands = _ordered_object_commands(bundle_dir, manifest)
    manifest_command = {
        "order": len(object_commands) + 1,
        "relative_path": "object_store_manifest.json",
        "object_store_key": manifest_key,
        "local_path": str(manifest_path),
        "expected_content_sha256": file_sha256(manifest_path),
        "is_manifest": True,
        "note": "MANIFEST — uploaded LAST, after every object above.",
        "argv": ["lean", "cloud", "object-store", "set", manifest_key, str(manifest_path)],
    }

    verification = [
        {
            "purpose": "list the immutable prefix and confirm every object landed",
            "argv": ["lean", "cloud", "object-store", "ls", prefix],
        },
        {
            "purpose": "fetch the manifest back and diff against the local copy",
            "argv": ["lean", "cloud", "object-store", "get", manifest_key],
        },
    ]

    any_drift = any(c.get("drift") for c in object_commands)
    return {
        "artifact_type": "phase0q_cloud_upload_plan",
        "schema_version": 1,
        "status": "prepared_pending_upload",
        "executed": False,
        "network_calls": 0,
        "lean_invocations": 0,
        "bundle_dir": str(bundle_dir),
        "object_store_prefix_immutable": prefix,
        "object_store_manifest_key": manifest_key,
        "qc_project_id": manifest.get("qc_project_id"),
        "manifest_uploaded_last": True,
        "any_on_disk_drift": any_drift,
        "ordered_upload_commands": [*object_commands, manifest_command],
        "post_upload_verification_commands": verification,
        "governance": manifest.get("governance", {}),
    }


def render_shell_script(plan: dict[str, Any]) -> str:
    """Human-readable review script. NOT auto-run; the orchestrator runs it manually."""
    lines: list[str] = []
    lines.append("#!/bin/sh")
    lines.append("# phase0q cloud-leg upload plan — REVIEW BEFORE RUNNING.")
    lines.append("# Emitted by harness.phase0q_cloud.upload_plan; nothing here was executed.")
    lines.append("# The manifest is set LAST so a partial upload never advertises a")
    lines.append("# complete bundle. `set -e` aborts on the first failure.")
    lines.append("set -eu")
    lines.append("")
    lines.append(f"# immutable prefix: {plan['object_store_prefix_immutable']}")
    lines.append(f"# qc project id:    {plan.get('qc_project_id')}")
    lines.append("")
    lines.append("# --- objects (manifest excluded; uploaded last) ---")
    for cmd in plan["ordered_upload_commands"]:
        if cmd.get("is_manifest"):
            lines.append("")
            lines.append("# --- manifest LAST ---")
        lines.append(f"# sha256 {cmd['expected_content_sha256']}")
        lines.append(_quote_argv(cmd["argv"]))
    lines.append("")
    lines.append("# --- post-upload verification ---")
    for cmd in plan["post_upload_verification_commands"]:
        lines.append(f"# {cmd['purpose']}")
        lines.append(_quote_argv(cmd["argv"]))
    lines.append("")
    return "\n".join(lines) + "\n"


def _quote_argv(argv: list[str]) -> str:
    def q(token: str) -> str:
        return f'"{token}"' if (" " in token or "\\" in token) else token
    return " ".join(q(t) for t in argv)


def emit(bundle_dir: str | Path) -> dict[str, Any]:
    """Build the plan and write ``upload_plan.json`` + ``upload_plan.sh`` into the bundle."""
    bundle_dir = Path(bundle_dir)
    plan = build_upload_plan(bundle_dir)

    json_path = bundle_dir / "upload_plan.json"
    with json_path.open("wb") as handle:
        handle.write(canonical_json_bytes(plan))

    sh_path = bundle_dir / "upload_plan.sh"
    sh_path.write_bytes(render_shell_script(plan).encode("utf-8"))

    return {
        "status": plan["status"],
        "executed": False,
        "upload_plan_json": str(json_path),
        "upload_plan_sh": str(sh_path),
        "object_command_count": len(plan["ordered_upload_commands"]) - 1,
        "manifest_uploaded_last": True,
        "any_on_disk_drift": plan["any_on_disk_drift"],
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m harness.phase0q_cloud.upload_plan",
        description="Emit the ordered phase0q cloud upload plan (no lean, no network, no upload).",
    )
    parser.add_argument("--bundle-dir", required=True, help="A built bundle directory.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    summary = emit(args.bundle_dir)
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
