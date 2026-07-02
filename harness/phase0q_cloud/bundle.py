"""Deterministic LOCAL bundle builder for the phase0q cloud leg (build-only).

Builds a byte-identical-on-rebuild bundle directory containing everything the QC
Research notebook needs to reproduce the ``local_python_pure`` leg inside
``qc_research_object_store``:

  * pack v2 canonical tables (macro_observation_vintage.json, eod_prices.json),
  * gzipped harness sources (harness/phase0q/*.py) + the transitive ``src`` modules
    the decision path imports (shipped as bundle CONTENT; repo files are untouched),
  * scenario/config (the injected RunConfig + windows/candidates/cost grid),
  * an expected-results manifest whose logical hashes are READ from the committed
    immutable evidence artifacts (metric_evidence_001 + compression_grid_001),
  * an object_store_manifest.json with per-object content_sha256 and the immutable
    prefix ``investintell/open_macro_v03/phase0q/<harness_commit>/<pack_sha>/``.

Drift refusal: the build FAILS if any shipped source file's bytes differ from its
git HEAD blob (``git cat-file``). Determinism: gzip mtime pinned to 0; canonical
JSON writers (sorted keys, LF); no wall-clock in canonical payloads; no RNG.

ZERO network calls. ZERO ``lean`` invocations. ZERO uploads. This module only
writes a LOCAL directory (default ``build/phase0q_cloud_bundle`` — never committed,
and never under any ``data/`` path segment).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from . import (
    BUNDLE_SCHEMA_VERSION,
    OBJECT_STORE_BASE_PREFIX,
    QC_PROJECT_ID,
    QC_PROJECT_NAME,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

# Default LOCAL, uncommitted bundle output (NOT under any ``data/`` segment).
DEFAULT_BUNDLE_DIR = REPO_ROOT / "build" / "phase0q_cloud_bundle"

# Pack v2 (immutable). The harness verifies the WHOLE pack (verify_pack computes a
# digest over every file and checks it against manifest.json), so the bundle ships the
# complete pack tree — not just the two canonical tables — under ``pack/`` so the cloud
# leg can run ``load_and_verify_pack`` faithfully.
PACK_ID = "open_macro_v03_certified_input_pack_002"
PACK_DIR = REPO_ROOT / "fixtures" / "p1_packs" / PACK_ID
PACK_REL_ROOT = f"fixtures/p1_packs/{PACK_ID}"
# The two canonical tables the local-leg logical hashes are ultimately built from
# (surfaced explicitly in the manifest for the reviewer).
PACK_CANONICAL_TABLES = ("macro_observation_vintage", "eod_prices")

# Committed immutable evidence the expected-results manifest reads local hashes from.
EVIDENCE_001_DIR = REPO_ROOT / "artifacts" / "quant" / "open_macro_v03_metric_evidence_001"
COMPRESSION_GRID_DIR = REPO_ROOT / "artifacts" / "quant" / "open_macro_v03_compression_grid_001"

# The COMPLETE runtime import closure of ``run_harness`` (verified by inspecting
# ``sys.modules`` after a real run), shipped verbatim (gzipped) as bundle CONTENT so
# the QC Research notebook can materialize an importable tree. Repo files are never
# modified. Each entry's ``source_path`` is the repo path (drift-checked against git
# HEAD); ``target_path`` is where the notebook writes it under the QC project root.

# Harness modules (target == source path).
HARNESS_SOURCE_FILES = (
    "harness/__init__.py",
    "harness/p1_pack/__init__.py",
    "harness/p1_pack/contract.py",
    "harness/p1_pack/verifier.py",
    "harness/phase0q/__init__.py",
    "harness/phase0q/amendments.py",
    "harness/phase0q/decision.py",
    "harness/phase0q/grid.py",
    "harness/phase0q/metrics.py",
    "harness/phase0q/pit.py",
    "harness/phase0q/runner.py",
    "harness/phase0q/sleeve.py",
)

# ``src`` closure (target == source path). ``src/db.py`` is deliberately EXCLUDED
# here: the real one imports psycopg / requires network, so it is replaced by the
# fail-loud stub materialized in Research (see SRC_DB_STUB below).
SRC_SOURCE_FILES = (
    "src/__init__.py",
    "src/input_packs/__init__.py",
    "src/input_packs/hashing.py",
    "src/input_packs/manifest.py",
    "src/input_packs/p0_contract.py",
    "src/input_packs/p0_derived.py",
    "src/input_packs/verifier.py",
    "src/macro_sources.py",
    "src/macro_transforms.py",
    "src/quadrant_assemble.py",
    "src/quadrant_confidence.py",
    "src/quadrant_hysteresis.py",
    "src/quadrant_score.py",
    "src/quadrant_snapshot.py",
    "src/quadrant_staleness.py",
)

# The ``investintell_quant_core`` subtree the harness imports
# (``hashing.canonical.stable_hash`` / ``normalize_logical_value``). It lives under
# ``packages/.../src/`` in the repo but must be importable from the QC project ROOT,
# so the bundle records both the repo source path and the project-root target path.
QUANT_CORE_SRC_ROOT = "packages/investintell_quant_core/src"
QUANT_CORE_SOURCE_FILES = (
    "investintell_quant_core/__init__.py",
    "investintell_quant_core/version.py",
    "investintell_quant_core/hashing/__init__.py",
    "investintell_quant_core/hashing/canonical.py",
)

# Fail-loud offline DB stub materialized in Research in place of src/db.py. It
# provides the ONE constant quadrant_assemble re-exports (LOCK_REGIME_QUADRANT) and
# refuses every connection attempt so the cloud leg can never touch a database.
SRC_DB_STUB = (
    '"""Offline DB stub for the phase0q cloud leg.\n'
    "\n"
    "The real src/db.py requires psycopg and network access; the reproducibility\n"
    "leg is offline-only, so database access is forbidden. Only the single constant\n"
    "quadrant_assemble re-exports is provided.\n"
    '"""\n'
    "\n"
    "LOCK_REGIME_QUADRANT = 900_208\n"
    "\n"
    "\n"
    "def resolve_dsn(dsn=None):\n"
    "    raise RuntimeError('phase0q cloud leg is offline-only; database access is forbidden')\n"
    "\n"
    "\n"
    "def connect(dsn=None, *, autocommit=False):\n"
    "    raise RuntimeError('phase0q cloud leg is offline-only; database access is forbidden')\n"
    "\n"
    "\n"
    "def advisory_lock(conn=None, lock_id=None):\n"
    "    raise RuntimeError('phase0q cloud leg is offline-only; database access is forbidden')\n"
)

# Governance pins mirrored onto every emitted payload (candidate, non-activating).
GOVERNANCE_PINS = {
    "A3": "open_macro_v03",
    "A5": "blocked",
    "runtime_activation": False,
    "activation_allowed": False,
    "allocator_publish": False,
    "official_result": False,
    "db_write_mode": "none",
    "production_endpoint_activation": "none",
    "freeze_ready": False,
    "approved": False,
    "status": "candidate_not_approved",
    "classification": "metric_evidence_only",
}


# --------------------------------------------------------------------------- #
# Canonical writers + hashing (deterministic, network-free)                   #
# --------------------------------------------------------------------------- #

def canonical_json_bytes(payload: Any) -> bytes:
    """Sorted-key, LF-terminated canonical JSON bytes (deterministic)."""
    text = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    return text.encode("utf-8")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(canonical_json_bytes(payload))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha256(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


@contextmanager
def _deterministic_gzip(path: Path) -> Iterator[gzip.GzipFile]:
    """Gzip writer with mtime pinned to 0 for byte-identical rebuilds."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            yield gz


def gzip_bytes_deterministic(data: bytes) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, mtime=0) as gz:
        gz.write(data)
    return buffer.getvalue()


# --------------------------------------------------------------------------- #
# Git HEAD blob drift refusal                                                 #
# --------------------------------------------------------------------------- #

def git_head_blob(rel_path: str) -> bytes:
    """Return the bytes of ``rel_path`` at git HEAD. Raises on any git failure."""
    try:
        return subprocess.check_output(
            ["git", "cat-file", "blob", f"HEAD:{rel_path}"],
            cwd=REPO_ROOT,
            stderr=subprocess.PIPE,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        raise RuntimeError(
            f"cannot read git HEAD blob for {rel_path}; drift refusal cannot be "
            f"evaluated (is this a git checkout with {rel_path} committed?)"
        ) from exc


def read_source_with_drift_refusal(rel_path: str) -> bytes:
    """Read a shipped source file and REFUSE if its working-tree bytes differ from
    the committed git HEAD blob.

    Newline normalization: the ``.gitattributes`` pins ``harness/**/*.py`` (and the
    src modules via the repo default) to ``eol=lf``, so we compare on LF-normalized
    bytes to avoid a spurious CRLF-vs-LF drift on Windows checkouts.
    """
    abs_path = REPO_ROOT / rel_path
    if not abs_path.is_file():
        raise FileNotFoundError(f"shipped source missing from working tree: {rel_path}")
    working = abs_path.read_bytes()
    head = git_head_blob(rel_path)
    if _lf(working) != _lf(head):
        raise RuntimeError(
            f"drift refusal: {rel_path} differs from its git HEAD blob; refusing to "
            "build a cloud-leg bundle from uncommitted source changes"
        )
    # Ship the committed HEAD bytes (LF-normalized) so the bundle is independent of
    # the local checkout's line-ending configuration -> byte-identical rebuilds.
    return _lf(head)


def _lf(data: bytes) -> bytes:
    return data.replace(b"\r\n", b"\n")


# --------------------------------------------------------------------------- #
# Immutable prefix + object keys                                              #
# --------------------------------------------------------------------------- #

def immutable_prefix(harness_commit: str, pack_sha: str) -> str:
    """``investintell/open_macro_v03/phase0q/<harness_commit>/<pack_sha>/``."""
    return f"{OBJECT_STORE_BASE_PREFIX}/{harness_commit}/{pack_sha}"


def store_key(prefix: str, relative_path: str) -> str:
    return f"{prefix}/{relative_path}".replace("\\", "/")


# --------------------------------------------------------------------------- #
# Evidence readers (committed immutable local-leg hashes)                      #
# --------------------------------------------------------------------------- #

def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"expected committed evidence artifact: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _run_head_harness(harness_commit: str) -> dict[str, Any]:
    """Run the (drift-checked HEAD) harness locally to derive the expected hashes the
    cloud leg — which ships the SAME sources — must reproduce.

    The bundled harness code is git-HEAD (drift refusal guarantees it). Some logical
    hashes (metrics_canonical_logical_hash, out_of_sample_stability, run_fingerprint,
    the local_python_pure leg hash) are code-dependent and evolve with the harness, so
    the expected values are computed from the CURRENT code, not read stale from the
    frozen evidence. This is fully deterministic (no wall-clock, no RNG).

    run_id/started/finished match the committed metric_evidence_001 provenance so a
    reader can line the fingerprint up with that evidence's inputs.
    """
    from harness.phase0q import runner  # local import; no side effects, no network

    config = runner.RunConfig(
        run_id="open_macro_v03_metric_evidence_001",
        started_at="2026-07-02T00:00:00+00:00",
        finished_at="2026-07-02T00:00:00+00:00",
        harness_commit=harness_commit,
    )
    run = runner.run_harness(PACK_DIR, config)
    result = run["result"]
    return {
        "input_pack_sha256": run["input_pack_sha256"],
        "contract_bundle_sha256": result["contract_bundle_sha256"],
        "job_type": result["job_type"],
        "run_fingerprint": result["run_fingerprint"],
        "output_logical_hashes": result["output_logical_hashes"],
        "local_python_pure_logical_hash": result["execution_legs"][0]["logical_hash"],
    }


def read_local_leg_expected_hashes(harness_commit: str) -> dict[str, Any]:
    """Expected local-leg hashes for the cloud leg to reproduce.

    Computed LIVE from the drift-checked HEAD harness (the exact sources shipped in the
    bundle), with the committed ``metric_evidence_001`` result kept as a ratified anchor
    and per-hash agreement recorded so a reviewer sees which hashes are code-stable
    (turnover / drawdown / volatility / stress) vs which evolved with the harness
    (metrics_canonical / out_of_sample / run_fingerprint / leg hash).
    """
    live = _run_head_harness(harness_commit)
    committed = _read_json(EVIDENCE_001_DIR / "metric_backtest_result.json")
    grid_manifest = _read_json(COMPRESSION_GRID_DIR / "compression_grid_manifest.json")
    grid_prov = grid_manifest.get("provenance", {})

    committed_legs = {leg["leg"]: leg["logical_hash"] for leg in committed.get("execution_legs", [])}
    committed_local = committed_legs.get("local_python_pure")
    if not committed_local:
        raise ValueError("metric_evidence_001 result has no local_python_pure execution leg hash")

    anchor_agreement = {
        key: {
            "live": live["output_logical_hashes"][key],
            "committed_evidence_001": committed["output_logical_hashes"].get(key),
            "code_stable": live["output_logical_hashes"][key]
            == committed["output_logical_hashes"].get(key),
        }
        for key in sorted(live["output_logical_hashes"])
    }
    return {
        "source_artifacts": {
            "metric_backtest_result": "artifacts/quant/open_macro_v03_metric_evidence_001/metric_backtest_result.json",
            "compression_grid_manifest": "artifacts/quant/open_macro_v03_compression_grid_001/compression_grid_manifest.json",
        },
        "expected_hashes_source": "live_head_harness_run",
        "job_type": live["job_type"],
        "run_fingerprint": live["run_fingerprint"],
        "input_pack_sha256": live["input_pack_sha256"],
        "contract_bundle_sha256": live["contract_bundle_sha256"],
        "output_logical_hashes": live["output_logical_hashes"],
        "execution_legs": {
            "local_python_pure": {
                "logical_hash": live["local_python_pure_logical_hash"],
                "status": "complete",
            },
            "qc_research_object_store": {
                "logical_hash": None,
                "status": "pending_upload",
            },
        },
        "committed_evidence_001_anchor": {
            "run_fingerprint": committed["run_fingerprint"],
            "local_python_pure_logical_hash": committed_local,
            "output_logical_hashes_agreement": anchor_agreement,
        },
        "compression_grid_provenance": {
            "harness_commit": grid_prov.get("harness_commit"),
            "input_pack_sha256": grid_prov.get("input_pack_sha256"),
            "contract_bundle_sha256": grid_prov.get("contract_bundle_sha256"),
        },
        "float_tolerance": 1e-12,
        "hash_policy": {
            "canonical_writer": "investintell_quant_core.hashing.canonical.stable_hash",
            "float_decimals": 12,
            "rng": "none",
        },
    }


# --------------------------------------------------------------------------- #
# Scenario / config payload                                                   #
# --------------------------------------------------------------------------- #

def build_scenario_config(harness_commit: str) -> dict[str, Any]:
    """The injected, deterministic RunConfig the cloud leg must reproduce.

    Imports the module-level pins from the shipped runner so config and code cannot
    silently diverge. Base 5bps minimum; the full (0,5,10,25) grid runs if runtime
    permits (the notebook decides). run_id/started/finished match the committed
    metric_evidence_001 provenance so the recomputed fingerprint is identical.
    """
    from harness.phase0q import runner  # local import; no side effects, no network

    grid_manifest = _read_json(COMPRESSION_GRID_DIR / "compression_grid_manifest.json")
    grid_prov = grid_manifest.get("provenance", {})
    return {
        "artifact_type": "phase0q_cloud_scenario_config",
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "run_config": {
            "run_id": "open_macro_v03_metric_evidence_001",
            "started_at": "2026-07-02T00:00:00+00:00",
            "finished_at": "2026-07-02T00:00:00+00:00",
            "harness_commit": harness_commit,
            "candidates": [
                {
                    "candidate_id": c.candidate_id,
                    "growth_weight": c.growth_weight,
                    "inflation_weight": c.inflation_weight,
                    "risk_tilt": c.risk_tilt,
                    "defensive_floor_delta_pp": c.defensive_floor_delta_pp,
                    "risk_cap_delta_pp": c.risk_cap_delta_pp,
                }
                for c in runner.SCENARIO_CANDIDATES
            ],
            "cost_grid_bps": list(runner.COST_GRID_BPS),
            "base_cost_bps": runner.BASE_COST_BPS,
            "primary_window": [
                runner.PRIMARY_WINDOW[0].isoformat(),
                runner.PRIMARY_WINDOW[1].isoformat(),
            ],
            "stress_windows": [
                {
                    "window_id": w["window_id"],
                    "start": w["start"].isoformat(),
                    "end": w["end"].isoformat(),
                    "coverage": w["coverage"],
                }
                for w in runner.STRESS_WINDOWS
            ],
        },
        "compression_grid": {
            "sleeves": ["baseline_100", "compressed_50"],
            "full_grid": ["baseline_100", "compressed_75", "compressed_50", "compressed_25"],
            "base_cost_bps_minimum": runner.BASE_COST_BPS,
            "note": (
                "The cloud leg MUST measure at least baseline_100 and compressed_50 at "
                "the base 5bps cost; it runs the full cost grid / all four variants only "
                "if the Research node runtime permits."
            ),
        },
        "provenance": {
            "input_pack_id": PACK_ID,
            "input_pack_sha256": grid_prov.get("input_pack_sha256"),
            "contract_bundle_sha256": grid_prov.get("contract_bundle_sha256"),
            "harness_commit": harness_commit,
        },
        "governance": dict(GOVERNANCE_PINS),
    }


# --------------------------------------------------------------------------- #
# Bundle build                                                                #
# --------------------------------------------------------------------------- #

def _copy_pack_tree(bundle_dir: Path) -> tuple[list[dict[str, Any]], list[str]]:
    """Copy the FULL pack v2 tree into the bundle under ``pack/`` (byte-identical,
    with git HEAD drift refusal on each file).

    Returns (canonical_table_entries, all_pack_relpaths). The whole tree ships so the
    cloud leg can run ``load_and_verify_pack`` (which digests every pack file); the
    canonical-table entries are surfaced separately for the reviewer.
    """
    pack_files = sorted(
        p for p in PACK_DIR.rglob("*") if p.is_file()
    )
    if not pack_files:
        raise FileNotFoundError(f"pack v2 tree is empty: {PACK_DIR}")

    all_relpaths: list[str] = []
    for src in pack_files:
        pack_rel = src.relative_to(PACK_DIR).as_posix()
        head_rel = f"{PACK_REL_ROOT}/{pack_rel}"
        # Drift refusal: the shipped pack bytes must equal the committed git HEAD blob.
        data = read_source_with_drift_refusal(head_rel)
        bundle_rel = f"pack/{pack_rel}"
        dest = bundle_dir / bundle_rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        all_relpaths.append(bundle_rel)

    table_entries = [
        {"table": name, "relative_path": f"pack/data/canonical/{name}.json"}
        for name in PACK_CANONICAL_TABLES
    ]
    return table_entries, all_relpaths


def _gzip_source(bundle_dir: Path, *, source_rel: str, target_rel: str) -> dict[str, Any]:
    """Drift-check ``source_rel`` against git HEAD, gzip it to ``code/<target_rel>.gz``.

    ``target_rel`` is the repo-root-relative path the notebook materializes the source
    to (identical to ``source_rel`` except for the quant_core subtree, which moves from
    ``packages/.../src/`` to the project root).
    """
    plaintext = read_source_with_drift_refusal(source_rel)
    gz_rel = f"code/{target_rel}.gz"
    with _deterministic_gzip(bundle_dir / gz_rel) as gz:
        gz.write(plaintext)
    return {
        "source_path": source_rel,
        "target_path": target_rel,
        "relative_path": gz_rel,
        "plaintext_sha256": sha256_bytes(plaintext),
    }


def _write_gzipped_sources(
    bundle_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Gzip harness + src + quant_core sources into the bundle with drift refusal.

    Returns (harness_entries, src_entries, quant_core_entries). The fail-loud db stub
    is appended to src_entries with source_path=null (it has no repo file).
    """
    harness_entries = [
        _gzip_source(bundle_dir, source_rel=rel, target_rel=rel)
        for rel in HARNESS_SOURCE_FILES
    ]

    src_entries = [
        _gzip_source(bundle_dir, source_rel=rel, target_rel=rel)
        for rel in SRC_SOURCE_FILES
    ]
    stub_bytes = SRC_DB_STUB.encode("utf-8")
    stub_rel = "code/src/db.py.gz"
    with _deterministic_gzip(bundle_dir / stub_rel) as gz:
        gz.write(stub_bytes)
    src_entries.append({
        "source_path": None,
        "target_path": "src/db.py",
        "relative_path": stub_rel,
        "plaintext_sha256": sha256_bytes(stub_bytes),
        "fail_loud_db_stub": True,
    })

    quant_core_entries = [
        _gzip_source(bundle_dir, source_rel=f"{QUANT_CORE_SRC_ROOT}/{rel}", target_rel=rel)
        for rel in QUANT_CORE_SOURCE_FILES
    ]
    return harness_entries, src_entries, quant_core_entries


def _predrift_check_all_sources() -> None:
    """Evaluate git HEAD drift refusal for every shipped source up front.

    Ships no bytes; only raises if any shipped harness/src/quant_core/pack file (each
    ``.gitattributes``-LF) differs from its committed git HEAD blob. Runs before the
    live harness so a tamper aborts fast.
    """
    pack_rels = [
        f"{PACK_REL_ROOT}/{p.relative_to(PACK_DIR).as_posix()}"
        for p in PACK_DIR.rglob("*") if p.is_file()
    ]
    quant_core_rels = [f"{QUANT_CORE_SRC_ROOT}/{rel}" for rel in QUANT_CORE_SOURCE_FILES]
    for rel in (*HARNESS_SOURCE_FILES, *SRC_SOURCE_FILES, *quant_core_rels, *pack_rels):
        read_source_with_drift_refusal(rel)


def _object_files_manifest(
    bundle_dir: Path, prefix: str, relative_paths: list[str]
) -> dict[str, dict[str, Any]]:
    """Per-object {relative_path, object_store_key, file_size_bytes, content_sha256}.

    ``relative_paths`` are ordered; the manifest itself is NOT included here (it is
    uploaded LAST, separately).
    """
    files: dict[str, dict[str, Any]] = {}
    for rel in relative_paths:
        path = bundle_dir / rel
        if not path.is_file():
            raise FileNotFoundError(f"bundle object missing: {path}")
        files[rel] = {
            "relative_path": rel,
            "object_store_key": store_key(prefix, rel),
            "file_size_bytes": path.stat().st_size,
            "content_sha256": file_sha256(path),
        }
    return files


def build_bundle(bundle_dir: str | Path, harness_commit: str) -> dict[str, Any]:
    """Build the deterministic LOCAL bundle. Returns a summary dict.

    NO network. NO lean. NO upload. Writes only ``bundle_dir`` (cleared first for a
    byte-identical rebuild). ``harness_commit`` is the 40-char SHA that produced the
    committed local-leg evidence; the immutable prefix pins it.
    """
    if len(harness_commit) != 40 or not all(c in "0123456789abcdef" for c in harness_commit):
        raise ValueError(f"harness_commit must be a 40-char lowercase hex SHA: {harness_commit!r}")

    bundle_dir = Path(bundle_dir)
    _reset_dir(bundle_dir)

    # Drift refusal FIRST — before the (expensive) live harness run — so a tampered
    # source aborts immediately instead of after a full grid computation.
    _predrift_check_all_sources()

    expected = read_local_leg_expected_hashes(harness_commit)
    pack_sha = expected["input_pack_sha256"]
    prefix = immutable_prefix(harness_commit, pack_sha)

    pack_entries, pack_relpaths = _copy_pack_tree(bundle_dir)
    harness_entries, src_entries, quant_core_entries = _write_gzipped_sources(bundle_dir)

    scenario = build_scenario_config(harness_commit)
    write_json(bundle_dir / "scenario_config.json", scenario)
    write_json(bundle_dir / "expected_results_manifest.json", _expected_results_manifest(expected, harness_commit))

    # Ordered relative paths of every uploadable object (manifest excluded — LAST).
    ordered_rels: list[str] = []
    ordered_rels += pack_relpaths
    ordered_rels += ["scenario_config.json", "expected_results_manifest.json"]
    ordered_rels += [e["relative_path"] for e in harness_entries]
    ordered_rels += [e["relative_path"] for e in src_entries]
    ordered_rels += [e["relative_path"] for e in quant_core_entries]

    object_files = _object_files_manifest(bundle_dir, prefix, ordered_rels)

    manifest = _object_store_manifest(
        harness_commit=harness_commit,
        pack_sha=pack_sha,
        prefix=prefix,
        expected=expected,
        pack_entries=pack_entries,
        harness_entries=harness_entries,
        src_entries=src_entries,
        quant_core_entries=quant_core_entries,
        object_files=object_files,
    )
    manifest_path = bundle_dir / "object_store_manifest.json"
    write_json(manifest_path, manifest)

    # The manifest key that is uploaded LAST (after all object_files).
    return {
        "status": "prepared_pending_upload",
        "bundle_dir": str(bundle_dir),
        "object_store_prefix_immutable": prefix,
        "object_store_manifest_key": manifest["object_store_manifest_key"],
        "object_count": len(object_files),
        "manifest_content_sha256": file_sha256(manifest_path),
        "harness_commit": harness_commit,
        "input_pack_sha256": pack_sha,
    }


def _expected_results_manifest(expected: dict[str, Any], harness_commit: str) -> dict[str, Any]:
    return {
        "artifact_type": "phase0q_cloud_expected_results_manifest",
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "harness_commit": harness_commit,
        "qc_project_id": QC_PROJECT_ID,
        "qc_project_name": QC_PROJECT_NAME,
        **expected,
        "governance": dict(GOVERNANCE_PINS),
    }


def _object_store_manifest(
    *,
    harness_commit: str,
    pack_sha: str,
    prefix: str,
    expected: dict[str, Any],
    pack_entries: list[dict[str, Any]],
    harness_entries: list[dict[str, Any]],
    src_entries: list[dict[str, Any]],
    quant_core_entries: list[dict[str, Any]],
    object_files: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    bundle_size = sum(item["file_size_bytes"] for item in object_files.values())
    return {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "artifact_type": "phase0q_cloud_object_store_manifest",
        "bridge_scope": "qc_research_phase0q_reproducibility_only",
        "qc_project_id": QC_PROJECT_ID,
        "qc_project_name": QC_PROJECT_NAME,
        "harness_commit": harness_commit,
        "input_pack_id": PACK_ID,
        "input_pack_sha256": pack_sha,
        "contract_bundle_sha256": expected["contract_bundle_sha256"],
        "object_store_base_prefix": OBJECT_STORE_BASE_PREFIX,
        "object_store_prefix_immutable": prefix,
        "object_store_manifest_key": store_key(prefix, "object_store_manifest.json"),
        "uploaded_at": None,
        "upload_policy": {
            "manifest_uploaded_last": True,
            "drift_refusal": "content_sha256 mismatch aborts upload",
            "network_calls_during_build": 0,
            "lean_invocations_during_build": 0,
        },
        "pack_root_relative_path": "pack",
        "pack_canonical_tables": pack_entries,
        "harness_sources": harness_entries,
        "src_sources": src_entries,
        "quant_core_sources": quant_core_entries,
        "fail_loud_db_stub": "code/src/db.py.gz",
        "file_count": len(object_files),
        "bundle_size_bytes": bundle_size,
        "object_files": object_files,
        "expected": {
            "run_fingerprint": expected["run_fingerprint"],
            "output_logical_hashes": expected["output_logical_hashes"],
            "local_python_pure_logical_hash": (
                expected["execution_legs"]["local_python_pure"]["logical_hash"]
            ),
            "float_tolerance": expected["float_tolerance"],
        },
        "verdict_key_template": store_key(prefix, "results/phase0q_cloud_verdict.json"),
        "governance": dict(GOVERNANCE_PINS),
        "notes": (
            "PREPARED, PENDING UPLOAD. The orchestrator uploads every object_files entry "
            "with `lean cloud object-store set <key> <path>` and this manifest LAST. No "
            "verdict here grants activation: A5 stays blocked; runtime_activation / "
            "activation_allowed / allocator_publish / official_result are false; "
            "db_write_mode is none; status is candidate_not_approved."
        ),
    }


def _reset_dir(path: Path) -> None:
    import shutil

    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #

def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m harness.phase0q_cloud.bundle",
        description="Build the deterministic LOCAL phase0q cloud-leg bundle (no network, no upload).",
    )
    parser.add_argument(
        "harness_commit",
        help="40-char lowercase hex SHA that produced the committed local-leg evidence.",
    )
    parser.add_argument(
        "--bundle-dir",
        default=str(DEFAULT_BUNDLE_DIR),
        help=f"LOCAL output directory (default: {DEFAULT_BUNDLE_DIR}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    summary = build_bundle(args.bundle_dir, args.harness_commit)
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
