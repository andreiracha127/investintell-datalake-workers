"""Pins for artifacts/a5/open_macro_v03_direct_activation_001 (plan decision records).

The direct-activation plan root holds exactly the two owner decision records of the
plan PR (shadow elimination + plan GO) and stays immutable: byte pins over the
committed blobs, a closed directory listing, the recursive governance walk (with
duplicate-key rejection and string-truthy semantics), and the forbidden-marker sweep.
Stage A/B/C evidence lives in stage-scoped roots, never here.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

# Frozen pre-activation evidence replay. Kept live and replayable, but off the
# per-PR critical path: it validates artifacts from a governance phase that has
# already completed, so it cannot fail for a reason a new commit caused. Runs in
# .github/workflows/preactivation-evidence.yml.
pytestmark = pytest.mark.preactivation

ROOT = Path(__file__).resolve().parents[1]
PLAN_ROOT = ROOT / "artifacts" / "a5" / "open_macro_v03_direct_activation_001"

EXPECTED_FILE_SHA256 = {
    "shadow_elimination_decision_record.json":
        "8208c4c1bb496f0d18b4814c5df4f41fec893a8f3b35e1a327e0009adc0ea5f5",
    "plan_go_decision_record.json":
        "abd2ca2ba4ed7d925b2b718a700c9100605a73427bdbc8d5828b59214a02209e",
    "immediate_activation_decision_record.json":
        "c87a957d49e2acfbfc3781a516d0bd6d35de7a39fc6f06b4e74bf75d348e33d8",
}


def _reject_duplicate_keys(pairs):
    seen = {}
    for key, value in pairs:
        if key in seen:
            raise ValueError(f"duplicate JSON key {key!r}")
        seen[key] = value
    return seen


def _json(name: str) -> dict[str, Any]:
    payload = json.loads((PLAN_ROOT / name).read_text(encoding="utf-8"),
                         object_pairs_hook=_reject_duplicate_keys)
    assert isinstance(payload, dict)
    return payload


def test_plan_root_is_closed_and_byte_pinned() -> None:
    committed = sorted(
        str(p.relative_to(PLAN_ROOT)).replace("\\", "/")
        for p in PLAN_ROOT.rglob("*") if p.is_file())
    assert committed == sorted(EXPECTED_FILE_SHA256)
    for name, expected in EXPECTED_FILE_SHA256.items():
        blob = subprocess.run(
            ["git", "cat-file", "blob", f":artifacts/a5/open_macro_v03_direct_activation_001/{name}"],
            cwd=ROOT, capture_output=True, check=True)
        assert hashlib.sha256(blob.stdout).hexdigest() == expected, name


def test_decision_records_carry_the_owner_acts() -> None:
    shadow = _json("shadow_elimination_decision_record.json")
    assert shadow["decided_by"] == "Andrei Rachadel"
    assert "eliminando essa fase Shadow" in shadow["decision_verbatim"]
    assert "Phases 2-5" in shadow["supersedes"]

    go = _json("plan_go_decision_record.json")
    assert go["decided_by"] == "Andrei Rachadel"
    assert "produto em si" in go["decisions_verbatim"]
    assert "MANDATORY" in go["decisions"]["allocator"]
    assert "DECOMMISSIONED AT ACTIVATION" in go["decisions"]["old_model"]
    # the two-PR flip rule: Stage B flips activation flags; Stage C flips
    # official_result + the A4 exit — never anywhere else
    assert "Stage B" in go["governance"]["note"]
    assert "Stage C" in go["governance"]["note"]

    immediate = _json("immediate_activation_decision_record.json")
    assert immediate["decided_by"] == "Andrei Rachadel"
    assert "ligado imediatamente" in immediate["decision_verbatim"]
    assert "FULL IMMEDIATE ACTIVATION" in immediate["decision"]
    assert "kill switch" in immediate["observation_window_effect"]


FORBIDDEN_TRUE_KEYS = ("runtime_activation", "activation_allowed", "allocator_publish",
                       "official_result", "freeze_ready", "approved",
                       "feature_flag_default", "allow_db_write")


def _walk(node):
    if isinstance(node, dict):
        for key, value in node.items():
            yield key, value
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _is_truthy_flag(value: Any) -> bool:
    return value is True or (isinstance(value, str) and value.strip().lower() == "true")


def test_plan_records_keep_activation_blocked() -> None:
    for name in EXPECTED_FILE_SHA256:
        payload = _json(name)
        gov = payload["governance"]
        for key, value in _walk(gov):
            if key in FORBIDDEN_TRUE_KEYS:
                assert not _is_truthy_flag(value), f"{name}: {key} truthy"
        assert gov["A5"] == "blocked"
        assert gov["db_write_mode"] == "none"
