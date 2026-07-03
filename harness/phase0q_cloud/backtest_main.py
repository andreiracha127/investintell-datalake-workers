# ruff: noqa: F403,F405,E402
"""Backtest-shaped algorithm for the phase0q ``qc_research_object_store`` leg.

QC Research notebooks cannot be triggered headlessly. QC cloud BACKTESTS can (via the
``compile/create`` + ``backtests/create`` + ``backtests/read`` API). This module adapts
the exact compute of ``phase0q_cloud_leg.ipynb`` into a :class:`QCAlgorithm` so the same
reproducibility check runs as a backtest that a one-command driver can trigger and poll.

The algorithm:

  1. In ``Initialize`` sets a minimal two-day window + one cheap equity subscription (LEAN
     requires at least a data feed for a backtest to run), then runs the ENTIRE compute in
     ``OnEndOfAlgorithm`` (nothing is traded — determinism, no RNG, no wall-clock).
  2. Reads every bundle object via ``self.ObjectStore`` with per-object sha256 verification
     against the manifest (drift refusal), materializes the gzipped harness/src sources +
     the fail-loud ``src/db.py`` stub to a temp dir, imports and re-runs the decision chain
     plus the ``baseline_100`` / ``compressed_50`` sleeves at the base 5bps minimum.
  3. Recomputes the canonical logical hashes, compares them to
     ``expected_results_manifest.json``, and builds the SAME verdict JSON the notebook
     builds (:func:`build_verdict` — shared with the notebook's cell-10 logic).
  4. Saves the FULL verdict via ``self.ObjectStore.Save`` under
     ``<prefix>/results/phase0q_cloud_verdict.json``, emits the verdict ESSENTIALS via
     ``self.set_runtime_statistic`` (returned by ``backtests/read`` — the headless
     retrieval path), and ALSO logs the verdict in ~200-char base64 chunks with
     ``VERDICT_JSON_BEGIN`` / ``VERDICT_JSON_CHUNK i/n`` / ``VERDICT_JSON_END`` markers
     as a human-readable web-UI fallback (not the retrieval path).

**Governance (non-negotiable):** A5 stays **blocked**; ``runtime_activation``,
``activation_allowed``, ``allocator_publish``, ``official_result`` are all **false**;
``db_write_mode`` is ``none``; ``status`` is ``candidate_not_approved``. A matching verdict
is reproducibility evidence only and grants no activation or approval. No FRED / History /
external macro access. This file is the REPO copy for provenance; the driver uploads its
content verbatim as the cloud project's ``main.py``.
"""

from __future__ import annotations

import gzip
import hashlib
import importlib
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------- #
# Log-chunking markers (shared with the driver's reassembler).                 #
# ---------------------------------------------------------------------------- #
VERDICT_BEGIN_MARKER = "VERDICT_JSON_BEGIN"
VERDICT_END_MARKER = "VERDICT_JSON_END"
VERDICT_CHUNK_PREFIX = "VERDICT_JSON_CHUNK"
# QC log lines are truncated ~200 chars; keep the payload comfortably under that after the
# "VERDICT_JSON_CHUNK i/n " marker prefix.
VERDICT_CHUNK_SIZE = 160

# The env/config key the driver may set to point the algorithm at the manifest.
MANIFEST_KEY_PARAMETER = "PHASE0Q_CLOUD_MANIFEST_KEY"

# Injected by run_cloud_backtest.inject_manifest_key before upload; empty in
# the repo copy. API-created backtests have no project parameters and the
# key-file fallback is not part of the uploaded bundle, so the baked-in key
# is the reliable path.
MANIFEST_KEY_INJECTED = ""


def resolve_manifest_key_default(object_store) -> str:
    """Injected constant first; the committed key file as fallback."""
    if MANIFEST_KEY_INJECTED:
        return MANIFEST_KEY_INJECTED
    try:
        return object_store.read("phase0q_cloud/object_store_manifest_key.txt").strip()
    except Exception:
        return ""

# The sleeves the cloud leg MUST measure at the base 5bps minimum (mirrors the notebook).
REQUIRED_SLEEVES = ("baseline_100", "compressed_50")

# The expected-manifest keys the verdict compares against (mirrors the notebook + bundle).
EXPECTED_MANIFEST_KEYS = (
    "output_logical_hashes",
    "run_fingerprint",
    "execution_legs",
)


# ---------------------------------------------------------------------------- #
# Pure-python compute (imported by tests; QC-agnostic).                        #
# ---------------------------------------------------------------------------- #

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encode_verdict_log_chunks(verdict_bytes: bytes, chunk_size: int = VERDICT_CHUNK_SIZE):
    """Encode verdict JSON bytes into ordered log lines the driver reassembles.

    Returns a list of log lines: a BEGIN marker, then ``VERDICT_JSON_CHUNK i/n <payload>``
    lines, then an END marker. The payload is BASE64 of the verdict bytes, sliced — base64
    contains no whitespace/newlines, so QC's log-line handling (and any strip/splitlines on
    the way back) can never corrupt a chunk. Each line stays < ~200 chars so QC's log-line
    limit never truncates one.
    """
    import base64

    text = base64.b64encode(verdict_bytes).decode("ascii")
    slices = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)] or [""]
    total = len(slices)
    lines = [f"{VERDICT_BEGIN_MARKER} total={total} sha256={sha256_hex(verdict_bytes)}"]
    for idx, chunk in enumerate(slices, start=1):
        lines.append(f"{VERDICT_CHUNK_PREFIX} {idx}/{total} {chunk}")
    lines.append(VERDICT_END_MARKER)
    return lines


def decode_verdict_log_chunks(log_lines) -> bytes:
    """Reassemble verdict JSON bytes from backtest log lines (inverse of the encoder).

    Scans for the BEGIN marker, collects ``VERDICT_JSON_CHUNK i/n <payload>`` lines in order
    up to the END marker, concatenates the payloads, and verifies the reassembled sha256
    against the BEGIN marker's declared sha. Raises ValueError on any inconsistency.
    """
    begin_idx = None
    declared_total = None
    declared_sha = None
    for i, line in enumerate(log_lines):
        stripped = line.strip()
        if stripped.startswith(VERDICT_BEGIN_MARKER):
            begin_idx = i
            for token in stripped.split():
                if token.startswith("total="):
                    declared_total = int(token[len("total="):])
                elif token.startswith("sha256="):
                    declared_sha = token[len("sha256="):]
    if begin_idx is None:
        raise ValueError("no VERDICT_JSON_BEGIN marker found in log lines")

    chunks: dict[int, str] = {}
    saw_end = False
    for line in log_lines[begin_idx + 1:]:
        stripped = line.strip()
        if stripped.startswith(VERDICT_END_MARKER):
            saw_end = True
            break
        if not stripped.startswith(VERDICT_CHUNK_PREFIX):
            continue
        # format: "VERDICT_JSON_CHUNK i/n <base64-payload>"; base64 payloads carry no
        # whitespace, so splitting the marker + index token off the front is lossless.
        rest = stripped[len(VERDICT_CHUNK_PREFIX):].lstrip()
        head, sep, payload = rest.partition(" ")
        idx_str = head.split("/", 1)[0]
        chunks[int(idx_str)] = payload.strip() if sep else ""
    if not saw_end:
        raise ValueError("no VERDICT_JSON_END marker found after BEGIN")
    if declared_total is not None and len(chunks) != declared_total:
        raise ValueError(
            f"chunk count mismatch: got {len(chunks)} expected {declared_total}")

    import base64
    import binascii

    ordered = [chunks[i] for i in sorted(chunks)]
    try:
        verdict_bytes = base64.b64decode("".join(ordered).encode("ascii"), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"reassembled verdict is not valid base64: {exc}") from exc
    if declared_sha is not None and sha256_hex(verdict_bytes) != declared_sha:
        raise ValueError("reassembled verdict sha256 does not match the declared sha256")
    return verdict_bytes


def verify_object(rel_path: str, raw_bytes: bytes, expected_sha: str) -> None:
    """Drift refusal: raise if the object's bytes do not match the manifest sha256."""
    actual = sha256_hex(raw_bytes)
    if actual != expected_sha:
        raise RuntimeError(
            f"drift refusal: object {rel_path} sha {actual} != manifest {expected_sha}")


def materialize_sources(project_root: Path, manifest: dict, object_bytes: dict) -> None:
    """Decompress every ``code/*.gz`` object to its repo-relative path under ``project_root``.

    Mirrors the notebook's ``_materialize_gz``: strip the ``code/`` prefix and ``.gz`` suffix
    to recover each source's repo-relative target, write LF text, and ensure ``__init__.py``
    exists for every parent package. The fail-loud ``src/db.py`` stub ships as
    ``code/src/db.py.gz`` and is materialized here like any other source.
    """
    for rel_path in sorted(object_bytes):
        if not rel_path.startswith("code/") or not rel_path.endswith(".gz"):
            continue
        target_rel = rel_path[len("code/"):-len(".gz")]
        text = gzip.decompress(object_bytes[rel_path]).decode("utf-8")
        target = project_root / target_rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8", newline="\n")
        parent = target.parent
        while parent != project_root and parent.name:
            init = parent / "__init__.py"
            if not init.exists():
                init.write_text("", encoding="utf-8")
            parent = parent.parent


def materialize_pack(pack_root: Path, manifest: dict, object_bytes: dict) -> None:
    """Reconstruct the FULL pack v2 tree under ``pack_root`` from the ``pack/`` objects."""
    for rel_path in sorted(object_bytes):
        if not rel_path.startswith("pack/"):
            continue
        dest = pack_root / rel_path[len("pack/"):]
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(object_bytes[rel_path])


def assert_fail_loud_db_stub(project_root: Path) -> None:
    """Import the materialized ``src.db`` stub and assert it refuses connections."""
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    for mod in ("src", "src.db"):
        if mod in sys.modules:
            del sys.modules[mod]
    import src.db as _db  # noqa: E402
    if getattr(_db, "LOCK_REGIME_QUADRANT", None) != 900_208:
        raise AssertionError("db stub missing LOCK_REGIME_QUADRANT=900208")
    try:
        _db.connect()
    except RuntimeError as exc:
        if "offline-only" not in str(exc):
            raise AssertionError(f"db stub refused with unexpected message: {exc}") from exc
    else:
        raise AssertionError("db stub must refuse connect()")


def run_compute(project_root: Path, pack_root: Path, scenario: dict) -> dict:
    """Re-run the local-leg computation and return {run, sleeve_hashes} (notebook cell-8).

    Reloads the materialized harness modules, reconstructs the injected ``RunConfig`` from
    ``scenario['run_config']``, runs ``run_harness`` over the verified pack, then measures
    ``baseline_100`` / ``compressed_50`` at the base 5bps minimum via ``measure_grid_results``.
    Deterministic (no RNG, no wall-clock beyond the injected run_id/started/finished).
    """
    import datetime as dt

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    for mod in ("harness.phase0q.runner", "harness.phase0q.grid", "harness.phase0q.sleeve",
                "harness.phase0q.decision", "harness.phase0q.metrics", "harness.phase0q.pit"):
        if mod in sys.modules:
            importlib.reload(sys.modules[mod])

    from harness.phase0q import runner, sleeve, grid  # noqa: E402

    rc = scenario["run_config"]
    eod_rows = json.loads(
        (pack_root / "data" / "canonical" / "eod_prices.json").read_text(encoding="utf-8"))
    prices = sleeve.PriceFrame(eod_rows)
    candidates = tuple(
        sleeve.SleeveParams(
            c["candidate_id"], c["growth_weight"], c["inflation_weight"],
            c["risk_tilt"], c["defensive_floor_delta_pp"], c["risk_cap_delta_pp"])
        for c in rc["candidates"]
    )
    primary_window = (dt.date.fromisoformat(rc["primary_window"][0]),
                      dt.date.fromisoformat(rc["primary_window"][1]))
    stress_windows = tuple(
        {"window_id": w["window_id"], "start": dt.date.fromisoformat(w["start"]),
         "end": dt.date.fromisoformat(w["end"]), "coverage": w["coverage"]}
        for w in rc["stress_windows"]
    )
    config = runner.RunConfig(
        run_id=rc["run_id"], started_at=rc["started_at"], finished_at=rc["finished_at"],
        harness_commit=rc["harness_commit"], candidates=candidates,
        cost_grid=tuple(rc["cost_grid_bps"]), primary_window=primary_window,
        stress_windows=stress_windows,
    )

    run = runner.run_harness(pack_root, config)
    base_decisions = run["decisions"]
    grid_results = grid.measure_grid_results(
        prices, base_decisions,
        sleeve.SleeveParams("baseline_current", 0.5, 0.5, 0.0, 0.0, 0.0))
    sleeve_hashes = {
        vid: runner.stable_hash(runner.canonicalize(grid_results[vid]))
        for vid in REQUIRED_SLEEVES
    }
    return {"run": run, "sleeve_hashes": sleeve_hashes}


def build_verdict(manifest: dict, expected: dict, run: dict, sleeve_hashes: dict,
                  *, bundle_source: str = "quantbook_object_store") -> dict:
    """Build the verdict JSON — byte-for-byte the structure the notebook's cell-10 emits.

    Compares the recomputed contract-v2 hashes against ``expected_results_manifest.json``
    (exact logical-hash match; 1e-12 float tolerance carried in the payload). Returns the
    verdict dict (the caller canonicalizes + saves + logs it).
    """
    exp_hashes = expected["output_logical_hashes"]
    got_hashes = run["result"]["output_logical_hashes"]
    per_hash = {
        k: {"expected": exp_hashes[k], "actual": got_hashes.get(k),
            "match": exp_hashes[k] == got_hashes.get(k)}
        for k in sorted(exp_hashes)
    }
    exp_fp = expected["run_fingerprint"]
    got_fp = run["result"]["run_fingerprint"]
    exp_local = expected["execution_legs"]["local_python_pure"]["logical_hash"]
    got_cloud = run["result"]["execution_legs"][0]["logical_hash"]

    mismatches = [k for k, v in per_hash.items() if not v["match"]]
    all_match = not mismatches and exp_fp == got_fp and exp_local == got_cloud

    return {
        "artifact_type": "phase0q_cloud_leg_verdict",
        "schema_version": 1,
        "execution_backend": (
            "quantconnect_cloud_backtest" if bundle_source == "quantbook_object_store"
            else "local_backtest_fallback"),
        "bundle_source": bundle_source,
        "object_store_prefix_immutable": manifest["object_store_prefix_immutable"],
        "harness_commit": manifest["harness_commit"],
        "input_pack_sha256": manifest["input_pack_sha256"],
        "contract_bundle_sha256": manifest["contract_bundle_sha256"],
        "run_fingerprint": got_fp,
        "output_logical_hashes": got_hashes,
        "execution_legs": {
            "qc_research_object_store": {"logical_hash": got_cloud, "status": "complete"},
        },
        "sleeve_logical_hashes": sleeve_hashes,
        "comparison": {
            "output_logical_hashes": per_hash,
            "run_fingerprint": {"expected": exp_fp, "actual": got_fp,
                                "match": exp_fp == got_fp},
            "execution_leg_logical_hash": {
                "expected_local_python_pure": exp_local,
                "actual_qc_research_object_store": got_cloud,
                "match": exp_local == got_cloud},
            "float_tolerance": 1e-12,
            "mismatch_count": (len(mismatches) + (0 if exp_fp == got_fp else 1)
                               + (0 if exp_local == got_cloud else 1)),
            "all_hashes_match": all_match,
        },
        "external_macro_access": False,
        "reproduced": all_match,
        "verdict": "reproduced" if all_match else "not_reproduced",
        "governance": manifest["governance"],
    }


def canonical_verdict_bytes(verdict: dict) -> bytes:
    """Sorted-key, indented, LF-terminated canonical JSON (matches the notebook writer)."""
    return (json.dumps(verdict, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def build_runtime_statistics(verdict: dict, verdict_bytes: bytes) -> dict:
    """The verdict ESSENTIALS emitted via ``self.set_runtime_statistic`` (all strings).

    ``backtests/read`` returns ``runtimeStatistics`` — the supported headless retrieval
    channel (``backtests/read/log`` is NOT a supported endpoint). The cloud leg hash is the
    stable hash over ALL output logical hashes, so a single equality against the local
    expected leg hash proves the full per-hash set; the sha256 pins the FULL verdict JSON
    saved to the Object Store results key.
    """
    return {
        "phase0q_verdict": "reproduced" if verdict["reproduced"] else "mismatch",
        "phase0q_cloud_leg_hash": str(
            verdict["execution_legs"]["qc_research_object_store"]["logical_hash"]),
        "phase0q_expected_leg_hash": str(
            verdict["comparison"]["execution_leg_logical_hash"]["expected_local_python_pure"]),
        "phase0q_mismatch_count": str(verdict["comparison"]["mismatch_count"]),
        "phase0q_verdict_sha256": sha256_hex(verdict_bytes),
    }


def assert_governance(verdict: dict) -> None:
    """Fail loud if any governance pin drifted (mirrors the notebook's final asserts)."""
    gov = verdict["governance"]
    if gov.get("A5") != "blocked":
        raise AssertionError("A5 must stay blocked")
    if gov.get("runtime_activation") is not False:
        raise AssertionError("runtime_activation must be false")
    if gov.get("official_result") is not False:
        raise AssertionError("official_result must be false")
    if verdict["external_macro_access"] is not False:
        raise AssertionError("external_macro_access must be false")


# ---------------------------------------------------------------------------- #
# Object Store adapter (QCAlgorithm.ObjectStore <-> pure compute).             #
# ---------------------------------------------------------------------------- #

def _object_store_read_bytes(object_store, key: str) -> bytes:
    """Read raw bytes for ``key`` from a QCAlgorithm ObjectStore (case-tolerant API)."""
    if hasattr(object_store, "ReadBytes"):
        return bytes(object_store.ReadBytes(key))
    if hasattr(object_store, "read_bytes"):
        return bytes(object_store.read_bytes(key))
    # Fall back to a file-path read.
    if hasattr(object_store, "GetFilePath"):
        return Path(object_store.GetFilePath(key)).read_bytes()
    if hasattr(object_store, "get_file_path"):
        return Path(object_store.get_file_path(key)).read_bytes()
    raise RuntimeError("ObjectStore exposes no ReadBytes/GetFilePath method")


def _object_store_save_bytes(object_store, key: str, data: bytes) -> None:
    # QC's SaveBytes returns a bool: a False (quota/permission/transient storage
    # failure) must fail loudly BEFORE the results key is advertised anywhere.
    if hasattr(object_store, "SaveBytes"):
        result = object_store.SaveBytes(key, list(data))
    elif hasattr(object_store, "save_bytes"):
        result = object_store.save_bytes(key, data)
    else:
        raise RuntimeError("ObjectStore exposes no SaveBytes method")
    if result is False:
        raise RuntimeError(f"ObjectStore refused the save of {key}; verdict not advertised")


def load_bundle_from_object_store(object_store, manifest: dict) -> dict:
    """Read + verify every ``object_files`` entry from the ObjectStore (drift refusal).

    Returns ``{relative_path: raw_bytes}``. Each object's sha256 must match the manifest's
    ``content_sha256`` or the run aborts (mirrors the notebook's cell-4 resolve loop).
    """
    object_bytes: dict[str, bytes] = {}
    for rel_path, item in sorted(manifest["object_files"].items()):
        raw = _object_store_read_bytes(object_store, item["object_store_key"])
        verify_object(rel_path, raw, item["content_sha256"])
        object_bytes[rel_path] = raw
    return object_bytes


def execute_reproducibility_check(object_store, project_root: Path, manifest_key: str) -> dict:
    """The full compute, ObjectStore-driven, returning the verdict dict.

    Reads the manifest, verifies + materializes every object, asserts the fail-loud db stub,
    re-runs the compute, builds + governance-checks the verdict. QC-agnostic beyond the
    ``object_store`` argument (a QCAlgorithm ObjectStore or any object exposing ReadBytes).
    """
    manifest = json.loads(_object_store_read_bytes(object_store, manifest_key).decode("utf-8"))
    _assert_manifest_governance(manifest)

    object_bytes = load_bundle_from_object_store(object_store, manifest)
    materialize_sources(project_root, manifest, object_bytes)
    pack_root = project_root / "_pack_root"
    materialize_pack(pack_root, manifest, object_bytes)
    assert_fail_loud_db_stub(project_root)

    scenario = json.loads(object_bytes_lookup(object_bytes, manifest, "scenario_config.json"))
    expected = json.loads(
        object_bytes_lookup(object_bytes, manifest, "expected_results_manifest.json"))

    computed = run_compute(project_root, pack_root, scenario)
    verdict = build_verdict(manifest, expected, computed["run"], computed["sleeve_hashes"])
    assert_governance(verdict)
    return verdict


def object_bytes_lookup(object_bytes: dict, manifest: dict, relative_name: str) -> str:
    """Return the utf-8 text of a top-level bundle object by its relative name."""
    if relative_name in object_bytes:
        return object_bytes[relative_name].decode("utf-8")
    raise KeyError(f"bundle object not found: {relative_name}")


# The exact governance pins the pulled object-store manifest MUST carry. A regenerated
# or overwritten manifest with ANY drifted pin is refused BEFORE any compute or verdict
# emission — a matching verdict must never be built from a tampered root of trust.
MANIFEST_GOVERNANCE_PINS = {
    "A5": "blocked",
    "runtime_activation": False,
    "activation_allowed": False,
    "allocator_publish": False,
    "official_result": False,
    "db_write_mode": "none",
    "approved": False,
    "status": "candidate_not_approved",
}

_UNSET = object()


def assert_manifest_governance_pins(manifest: dict) -> None:
    """Refuse the pulled manifest unless EVERY governance pin matches exactly.

    Checks (identity for booleans, equality for strings): ``A5 == "blocked"``;
    ``runtime_activation`` / ``activation_allowed`` / ``allocator_publish`` /
    ``official_result`` all ``False``; ``db_write_mode == "none"``; ``approved is
    False``; ``status == "candidate_not_approved"``. A missing pin is drift too.
    Raises AssertionError naming the drifted pin — no verdict is emitted.
    """
    if manifest.get("bridge_scope") != "qc_research_phase0q_reproducibility_only":
        raise AssertionError(
            "governance drift refusal: unexpected bridge_scope "
            f"{manifest.get('bridge_scope')!r}; refusing to run")
    gov = manifest.get("governance")
    if not isinstance(gov, dict):
        raise AssertionError("governance drift refusal: manifest has no governance object")
    for pin, required in MANIFEST_GOVERNANCE_PINS.items():
        actual = gov.get(pin, _UNSET)
        ok = (actual is required) if isinstance(required, bool) else (actual == required)
        if not ok:
            shown = "<missing>" if actual is _UNSET else repr(actual)
            raise AssertionError(
                f"governance drift refusal: manifest pin {pin} must be {required!r} "
                f"but is {shown}; a regenerated/overwritten manifest is never silently "
                "accepted — no verdict emitted")


def _assert_manifest_governance(manifest: dict) -> None:
    assert_manifest_governance_pins(manifest)


# ---------------------------------------------------------------------------- #
# QCAlgorithm entry point (only active in the QC cloud runtime).              #
# ---------------------------------------------------------------------------- #

try:  # pragma: no cover - QC cloud runtime only
    from AlgorithmImports import *  # noqa: F401,F403

    class OpenMacroV03Phase0QCloudBacktest(QCAlgorithm):  # noqa: F405
        """Runs the phase0q reproducibility check as a headless-triggerable backtest."""

        def initialize(self):
            # Minimal two-day window + one cheap subscription so LEAN starts a data feed.
            self.set_start_date(2026, 6, 29)
            self.set_end_date(2026, 6, 30)
            self.set_cash(100000)
            self.add_equity("SPY")
            self._manifest_key = self.get_parameter(MANIFEST_KEY_PARAMETER) or self._manifest_key_default()

        def _manifest_key_default(self) -> str:
            # Injected constant first; the committed key file as fallback.
            return resolve_manifest_key_default(self.object_store)

        def on_data(self, data):  # noqa: D401 - no trading; determinism.
            pass

        def on_end_of_algorithm(self):
            import tempfile

            if not self._manifest_key:
                raise RuntimeError(
                    f"set the {MANIFEST_KEY_PARAMETER} parameter to the immutable manifest key")

            project_root = Path(tempfile.mkdtemp(prefix="phase0q_cloud_"))
            verdict = execute_reproducibility_check(
                self.object_store, project_root, self._manifest_key)
            verdict_bytes = canonical_verdict_bytes(verdict)

            # (a) save the FULL verdict back to the Object Store under the immutable prefix.
            manifest = json.loads(
                _object_store_read_bytes(self.object_store, self._manifest_key).decode("utf-8"))
            verdict_key = manifest.get("verdict_key_template")
            if verdict_key:
                try:
                    _object_store_save_bytes(self.object_store, verdict_key, verdict_bytes)
                except Exception as exc:  # pragma: no cover
                    self.log(f"warning: could not save verdict to object store: {exc}")

            # (b) emit the verdict ESSENTIALS as runtime statistics — the headless
            # retrieval path (they come back on backtests/read).
            for name, value in build_runtime_statistics(verdict, verdict_bytes).items():
                self.set_runtime_statistic(name, value)

            # (c) log the verdict in chunks as a human-readable fallback (web UI);
            # NOT the retrieval path.
            for line in encode_verdict_log_chunks(verdict_bytes):
                self.log(line)

            self.log(
                f"phase0q_cloud verdict={verdict['verdict']} "
                f"mismatches={verdict['comparison']['mismatch_count']}")

except ImportError:  # pragma: no cover - repo/test context (no AlgorithmImports)
    pass
