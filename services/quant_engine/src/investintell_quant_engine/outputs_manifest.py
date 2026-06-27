"""Output manifest builder for quant-engine artifacts.

Produces a closed manifest of every file written to an output directory so the
backend can validate and ingest results without ad-hoc filesystem inspection,
and so determinism can be proven bit-a-bit.

Two views are supported:

- ``canonical=False`` (raw): ``sha256`` is the raw byte digest of each file.
  This is the provenance/audit view recorded in the job envelope.
- ``canonical=True``: for JSON artifacts, volatile fields (timestamps,
  operational ids, host environment, paths) are stripped before hashing, so two
  semantically-identical runs (e.g. ``jobs=1`` vs ``jobs=4``, host vs container)
  yield identical digests. Non-JSON artifacts always use the raw byte digest.

The volatility policy is declared explicitly in ``VOLATILE_FIELDS`` rather than
inferred, matching the report's requirement to control nondeterminism instead of
hiding it.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

# Nondeterministic noise: fields whose value cannot be reproduced (wall-clock
# timestamps, operational ids, host environment, host-specific paths, provenance
# of the checkout). Stripped from the canonical view.
VOLATILE_FIELDS: frozenset[str] = frozenset(
    {
        "created_at",
        "started_at",
        "finished_at",
        "execution_id",
        "job_id",
        "artifact_prefix",
        "expected_metrics_path",
        "environment",
        "worker_commit",
        "git_dirty",
        "python_executable",
    }
)

# Deterministic operational knobs that are echoed into the envelope but are not
# part of the semantic result. ``jobs`` is precisely the parallelism level the
# determinism matrix varies; it is reproducible but must not count as a semantic
# difference (the whole point is proving the result is invariant to it).
OPERATIONAL_FIELDS: frozenset[str] = frozenset({"jobs", "requested_jobs"})

# The canonical (semantic) view strips both: anything that is not part of the
# computed result is removed before hashing.
NON_SEMANTIC_FIELDS: frozenset[str] = VOLATILE_FIELDS | OPERATIONAL_FIELDS


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _strip_volatile(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_volatile(val)
            for key, val in value.items()
            if key not in NON_SEMANTIC_FIELDS
        }
    if isinstance(value, list):
        return [_strip_volatile(item) for item in value]
    return value


def _canonical_json_bytes(payload: Any) -> bytes:
    stripped = _strip_volatile(payload)
    return json.dumps(
        stripped, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _artifact_digest(path: Path, *, canonical: bool) -> str:
    raw = path.read_bytes()
    if canonical and path.suffix == ".json":
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return _sha256_bytes(raw)
        return _sha256_bytes(_canonical_json_bytes(payload))
    return _sha256_bytes(raw)


def build_outputs_manifest(
    output_dir: str | Path,
    *,
    status: str = "succeeded",
    canonical: bool = False,
    exclude: Iterable[str | Path] = (),
) -> dict[str, Any]:
    """Walk ``output_dir`` and build a closed manifest of every file.

    Paths are relative to ``output_dir`` and POSIX-normalized for cross-platform
    stability. Artifacts are sorted by path so the manifest itself is canonical.

    ``exclude`` lists files to skip (resolved to absolute paths). The manifest's
    own target must be excluded when it lives inside ``output_dir``; otherwise a
    stale copy from a previous run would be folded in and break repeatability.
    """
    root = Path(output_dir)
    excluded = {Path(p).resolve() for p in exclude}
    artifacts: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.resolve() in excluded:
            continue
        rel = path.relative_to(root).as_posix()
        entry: dict[str, Any] = {
            "path": rel,
            "sha256": _artifact_digest(path, canonical=canonical),
            "bytes": path.stat().st_size,
        }
        if canonical and path.suffix == ".json":
            entry["raw_sha256"] = _sha256_bytes(path.read_bytes())
        artifacts.append(entry)
    artifacts.sort(key=lambda a: a["path"])
    return {
        "schema_version": 1,
        "artifact_type": "outputs_manifest",
        "canonical": canonical,
        "status": status,
        "artifacts": artifacts,
    }
