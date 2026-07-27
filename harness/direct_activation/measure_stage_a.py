"""Stage A (A2/A3) runner: measured reproducibility + SLO conformance of the signed
candidate's live-validation compute, under the Phase 1 measurement machinery.

Usage (from the repo root):

    python -m harness.direct_activation.measure_stage_a [--runs-per-leg 8]
    python -m harness.direct_activation.measure_stage_a --runs-per-leg 2 --skip-container --dry-run

Plan A2: "the signed candidate computes today's consumable position on the snapshot,
N=8 host + N=8 container under the Phase 1 measurement machinery (enforced job-identity
determinism, clean-tree/worker-commit/image-id/tree-hash provenance)". Plan A3:
reproducibility host==container (``mismatch_count=0``); SLO conformance vs the Phase 1
signed thresholds (``artifacts/a5/open_macro_v03_dark_launch_001/monitoring_thresholds_record.json``)
— breach = STOP and investigate, NEVER recalibrate.

This reuses the Phase 1 clean-tree / worker-commit / tree-hash / image-id provenance
mechanism from ``harness.dark_launch.measure_observability`` and the hardened container
profile from ``scripts/repeatability_matrix.py`` (same profile, same image). Stage A
binds that mechanism to its explicit decision-compute manifest rather than Phase 1's
broader quant-engine trees. Unlike the Phase 1 job (the heavy
``a3_qc_parity`` quant-engine compute), the Stage A compute is the pure
``live_validation.compute()`` over committed pack v2 + the pinned delta snapshot, so the
container leg mounts the REPO read-only (harness/, src/, fixtures/, the pinned snapshot)
and forces PYTHONPATH at ``/repo`` — the host and container legs import the IDENTICAL
source tree (the clean worktree), and the image supplies only the interpreter + deps.

Governance: measurement only. Stage A changes nothing; A5 stays blocked. On ANY
reproducibility mismatch or a memory/error/retry SLO breach the runner writes NOTHING
and exits non-zero. LATENCY is the one SLO with a signed-amendment path (orchestrator
decision — workload identity, not a regression): with ``--amend-latency``, a latency
breach vs the Phase 1 threshold triggers the two-phase flow — the reproducibility
record is written first, then ``build_stage_a_amendment`` derives the amended
threshold with the SAME Phase 1 Task 4 rule (``ceil(1.5 x measured p95)``, worst leg
of the host+container matrix) pinned to THAT round's reproducibility sha256, and the
conformance record marks latency ``pass_amended`` referencing the amendment's sha256.
Without ``--amend-latency`` a latency breach STOPs exactly as before.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from harness.dark_launch import measure_observability as mo
from harness.direct_activation import build_stage_a_amendment as amendment_builder
from harness.direct_activation.compute_manifest import STAGE_A_COMPUTE_PATHS
from harness.direct_activation.measure_stage_a_child import SENTINEL, canonical_hash

ROOT = Path(__file__).resolve().parents[2]
STAGE_A = ROOT / "artifacts" / "a5" / "open_macro_v03_direct_activation_stage_a_001"
DARK = ROOT / "artifacts" / "a5" / "open_macro_v03_dark_launch_001"
THRESHOLDS = DARK / "monitoring_thresholds_record.json"
LIVE_VALIDATION_RECORD = STAGE_A / "live_validation_record.json"
AMENDMENT_RECORD = STAGE_A / "slo_threshold_amendment_record.json"
DIRECT_ACTIVATION_ID = "open_macro_v03_direct_activation_001"
CHILD_MODULE = "harness.direct_activation.measure_stage_a_child"
CHILD_SCRIPT_IN_REPO = "/repo/harness/direct_activation/measure_stage_a_child.py"
CONTAINER_PYTHONPATH = ("/repo:/repo/packages/investintell_quant_core/src"
                        ":/repo/services/quant_engine/src")


def _image_id_for(image: str) -> str:
    """Immutable content id of the ACTUAL image the container leg will execute.

    ``mo._image_id()`` inspects the hard-coded Phase 1 default image; when the runner is
    invoked with ``--image <other>`` the container leg runs THAT image, so the provenance
    must certify the same image being executed, not the default."""
    result = subprocess.run(["docker", "image", "inspect", image, "--format", "{{.Id}}"],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"engine image {image} not present; build it first "
                           f"(docker build -f docker/quant-engine/Dockerfile -t {image} .)")
    return result.stdout.strip()


def _sha256_file(path: Path) -> str:
    # CRLF-normalized so the pin is checkout-independent (autocrlf=true checkouts
    # materialize committed LF artifacts as CRLF on Windows; the canonical bytes
    # are the LF git blob, which is what CI on Linux reads).
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def _parse_child_stdout(stdout: str, *, ctx: str) -> dict[str, Any]:
    """The child emits exactly one sentinel-prefixed JSON line; take the last one so a
    stray import banner cannot corrupt the parse."""
    line = None
    for candidate in stdout.splitlines():
        if candidate.startswith(SENTINEL):
            line = candidate
    if line is None:
        raise RuntimeError(f"{ctx}: child produced no {SENTINEL!r} line; stdout={stdout!r}")
    return json.loads(line[len(SENTINEL):].strip())


def _run_host(index: int, worker_commit: str) -> dict[str, Any]:
    sep = ";" if sys.platform == "win32" else ":"
    env = dict(os.environ)
    env["PYTHONPATH"] = sep.join([
        str(ROOT),
        str(ROOT / "packages" / "investintell_quant_core" / "src"),
        str(ROOT / "services" / "quant_engine" / "src"),
    ])
    env["OPEN_MACRO_WORKER_COMMIT"] = worker_commit
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    proc = subprocess.run([sys.executable, "-m", CHILD_MODULE],
                          cwd=ROOT, env=env, capture_output=True, text=True, check=True)
    return _parse_child_stdout(proc.stdout, ctx=f"host r{index}")


def _hardened_profile(rm, repo: Path) -> list[str]:
    """Phase 1 hardened container profile (scripts/repeatability_matrix.py
    ::_container_docker_base), but mounting the repo read-only instead of the combo
    input + writable output — the Stage A child reads the worktree and prints metrics to
    stdout, so no writable mount is required beyond the tmpfs /tmp."""
    src = repo.resolve()
    return [
        "--network", "none",
        "--read-only",
        "--user", rm.CONTAINER_UID,
        "--security-opt", "no-new-privileges",
        "--cap-drop", "ALL",
        "--pids-limit", "256",
        "--memory", "4g",
        "--cpus", "2",
        "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=256m",
        "--mount", f"type=bind,src={src},dst=/repo,readonly",
    ]


def _run_container(rm, index: int, worker_commit: str, image: str,
                   repo: Path) -> dict[str, Any]:
    cmd = [
        "docker", "run", "--rm",
        *_hardened_profile(rm, repo),
        "-e", f"OPEN_MACRO_WORKER_COMMIT={worker_commit}",
        "-e", f"PYTHONPATH={CONTAINER_PYTHONPATH}",
        "-e", "PYTHONDONTWRITEBYTECODE=1",
        "--entrypoint", "python", image,
        CHILD_SCRIPT_IN_REPO,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return _parse_child_stdout(proc.stdout, ctx=f"container r{index}")


def measure(runs_per_leg: int, *, skip_container: bool, image: str,
            repo: Path, worker_commit_override: str | None = None) -> dict[str, Any]:
    rm = mo._load_repeatability_module()
    # Phase 1 provenance mechanism, scoped to the explicit Stage A compute manifest:
    # HEAD ONLY from clean decision/measurement files.
    # ``worker_commit_override`` (the sibling repeatability_matrix.py --worker-commit
    # escape hatch) BYPASSES the clean-tree gate for a smoke run on a known-good
    # checkout; the default (real) path enforces the gate and records clean_tree=True.
    if worker_commit_override is not None:
        worker_commit = worker_commit_override
        clean_tree = False
    else:
        worker_commit = mo._worker_commit(STAGE_A_COMPUTE_PATHS)
        clean_tree = True
    tree_hashes = mo._compute_tree_hashes(worker_commit, STAGE_A_COMPUTE_PATHS)
    image_id = None if skip_container else _image_id_for(image)

    legs: dict[str, list[dict[str, Any]]] = {}
    for leg in ("host", "container"):
        if leg == "container" and skip_container:
            continue
        samples: list[dict[str, Any]] = []
        for index in range(runs_per_leg):
            if leg == "host":
                metrics = _run_host(index, worker_commit)
            else:
                metrics = _run_container(rm, index, worker_commit, image, repo)
            if metrics.get("exit_code") != 0:
                raise RuntimeError(f"{leg} run {index}: nonzero child exit "
                                   f"{metrics.get('exit_code')}")
            metrics["index"] = index
            samples.append(metrics)
            print(f"{leg} run {index}: {metrics['wall_ms']:.1f} ms, "
                  f"{metrics['memory_peak_bytes'] / 1e6:.0f} MB peak, "
                  f"hash {metrics['logical_output_hash'][:12]}")
        legs[leg] = samples

    return {
        "worker_commit": worker_commit,
        "tree_hashes": tree_hashes,
        "image_id": image_id,
        "clean_tree": clean_tree,
        "runs_per_leg": runs_per_leg,
        "legs": legs,
    }


def _governance_block() -> dict[str, Any]:
    """Mirror the live_validation_record governance verbatim (single source of truth)."""
    record = json.loads(LIVE_VALIDATION_RECORD.read_text(encoding="utf-8"))
    return record["governance"]


def build_reproducibility_record(raw: dict[str, Any], *, modal_hash: str,
                                 mismatch_count: int) -> dict[str, Any]:
    legs_out: dict[str, Any] = {}
    for leg, samples in raw["legs"].items():
        runs = [{
            "index": s["index"],
            "logical_output_hash": s["logical_output_hash"],
            "wall_ms": round(s["wall_ms"], 3),
            "memory_peak_bytes": s["memory_peak_bytes"],
            "exit_code": s["exit_code"],
            "platform": s["platform"],
        } for s in sorted(samples, key=lambda s: s["index"])]
        leg_hashes = {s["logical_output_hash"] for s in samples}
        legs_out[leg] = {
            "runs": runs,
            "logical_output_hash": leg_hashes.pop() if len(leg_hashes) == 1 else None,
        }
    runs_total = sum(len(s) for s in raw["legs"].values())
    return {
        "artifact_type": "direct_activation_stage_a_reproducibility_record",
        "schema_version": 1,
        "stage": "A",
        "direct_activation_id": DIRECT_ACTIVATION_ID,
        "runs_per_leg": raw["runs_per_leg"],
        "runs_total": runs_total,
        "legs": legs_out,
        "logical_output_hash": modal_hash,
        "mismatch_count": mismatch_count,
        "verdict": "reproduced",
        "job_identity": {
            "worker_commit": raw["worker_commit"],
            "tree_hashes": raw["tree_hashes"],
            "image_id": raw["image_id"],
            "clean_tree": raw["clean_tree"],
        },
        "live_validation_record_sha256": _sha256_file(LIVE_VALIDATION_RECORD),
        "governance": _governance_block(),
    }


def _p95(values: list[float]) -> float:
    return mo._p95(values)


def measured_metrics(raw: dict[str, Any]) -> dict[str, Any]:
    """Round-level metrics with the Phase 1 formulas: the SLO latency figure is the
    WORST LEG's p95 (max over per-leg p95s, exactly measure_observability.build_records
    -> "worst leg of the host+container matrix"), never the pooled p95 — pooling 16
    samples would dilute a slow leg behind a fast one. peak = max over all runs
    (identical to the worst leg's max). mismatch_count stays pooled 16/16 (identity is
    one comparison across the whole round; only the latency STATISTIC is per-leg)."""
    per_leg_p95 = {leg: round(_p95([s["wall_ms"] for s in samples]), 3)
                   for leg, samples in raw["legs"].items()}
    peaks = [s["memory_peak_bytes"] for leg in raw["legs"].values() for s in leg]
    exits = [s["exit_code"] for leg in raw["legs"].values() for s in leg]
    runs_total = len(exits)
    return {
        "runs_total": runs_total,
        "latency_p95_ms": max(per_leg_p95.values()),
        "latency_p95_ms_per_leg": per_leg_p95,
        "memory_peak_bytes": max(peaks),
        "error_rate": sum(1 for e in exits if e != 0) / runs_total,
        "retry_rate": 0.0,
    }


def load_phase1_thresholds() -> dict[str, Any]:
    thresholds_doc = json.loads(THRESHOLDS.read_text(encoding="utf-8"))
    return {slo["id"]: slo["threshold"] for slo in thresholds_doc["slos"]}


def committed_latency_threshold() -> int | None:
    """The signed latency threshold for THIS workload, if one has been ratified.

    Phase 1's latency_slo (37826 ms) was derived from the a3_qc_parity job — a
    different workload, as the amendment record on disk states in its own rationale.
    Stage A reconstitutes the 148-month latched decision chain and measures ~110 s
    per run, so judging it against Phase 1 makes an ordinary round fail every time.

    Reading the ratified amendment closes a contradiction that made the gate
    meaningless in both directions: a plain run always breached, and --amend-latency
    always passed because it re-derived the threshold from the very round being
    judged. With the committed threshold as the baseline, a run either fits the
    ratified envelope or it does not, and --amend-latency goes back to being what it
    is documented as — a deliberate decision for a genuine workload change.
    """
    if not AMENDMENT_RECORD.is_file():
        return None
    doc = json.loads(AMENDMENT_RECORD.read_text(encoding="utf-8"))
    amended = doc.get("amended_slo") or {}
    if amended.get("id") != "latency_slo":
        return None
    threshold = amended.get("threshold")
    return int(threshold) if isinstance(threshold, (int, float)) else None


def _rel_or_abs(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def build_slo_conformance_record(measured: dict[str, Any], thr: dict[str, Any], *,
                                 amendment_threshold: int | None = None,
                                 amendment_sha256: str | None = None,
                                 amendment_path: Path | None = None) -> dict[str, Any]:
    """Conformance vs the Phase 1 signed thresholds; latency may additionally be
    judged against a signed amendment derived from THIS round (``pass_amended``).
    memory/error/retry are ALWAYS Phase 1 — no amendment path exists for them."""

    def _conf(slo_id: str, value: float) -> dict[str, Any]:
        threshold = thr[slo_id]
        return {"measured": value, "threshold": threshold,
                "status": "pass" if value <= threshold else "fail"}

    latency_p95 = measured["latency_p95_ms"]
    if latency_p95 <= thr["latency_slo"]:
        latency_conf: dict[str, Any] = _conf("latency_slo", latency_p95)
    else:
        # amendment path: only reachable when the runner built the amendment from
        # THIS round (egg-and-chicken resolved by the two-phase flow in main()).
        if amendment_threshold is None or amendment_sha256 is None:
            latency_conf = _conf("latency_slo", latency_p95)  # plain fail
        else:
            status = "pass_amended" if latency_p95 <= amendment_threshold else "fail"
            latency_conf = {
                "status": status,
                "measured_p95_ms": latency_p95,
                "phase1_threshold": thr["latency_slo"],
                "amended_threshold": amendment_threshold,
                "amendment_sha256": amendment_sha256,
            }

    conformance = {
        "latency_slo": latency_conf,
        "memory_slo": _conf("memory_slo", measured["memory_peak_bytes"]),
        "error_rate_slo": _conf("error_rate_slo", measured["error_rate"]),
        "retry_rate_slo": _conf("retry_rate_slo", measured["retry_rate"]),
    }

    thresholds_source = [{"path": THRESHOLDS.relative_to(ROOT).as_posix(),
                          "sha256": _sha256_file(THRESHOLDS)}]
    if amendment_sha256 is not None and amendment_path is not None:
        thresholds_source.append({"path": _rel_or_abs(amendment_path),
                                  "sha256": amendment_sha256})

    statuses = {k: v["status"] for k, v in conformance.items()}
    if any(s not in ("pass", "pass_amended") for s in statuses.values()):
        # the runner must never serialize a failing conformance record; it STOPs instead
        raise RuntimeError(f"refusing to build a conformance record with failing "
                           f"SLOs: {statuses}")

    return {
        "artifact_type": "direct_activation_stage_a_slo_conformance_record",
        "schema_version": 1,
        "stage": "A",
        "direct_activation_id": DIRECT_ACTIVATION_ID,
        "runs_total": measured["runs_total"],
        "thresholds_source": thresholds_source,
        "measured": {
            "latency_p95_ms": latency_p95,
            "latency_p95_ms_per_leg": measured["latency_p95_ms_per_leg"],
            "latency_p95_note": "worst leg of the host+container matrix (max of "
                                "per-leg p95s), the Phase 1 Task 4 semantics",
            "memory_peak_bytes": measured["memory_peak_bytes"],
            "error_rate": measured["error_rate"],
            "retry_rate": measured["retry_rate"],
            "retry_rate_note": "no retry path exists in the Stage A executor; every run "
                               "is a first attempt (a nonzero child exit aborts the round)",
        },
        "conformance": conformance,
        "verdict": "conform",
        "governance": _governance_block(),
    }


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8", newline="\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-per-leg", type=int, default=8)
    parser.add_argument("--skip-container", action="store_true",
                        help="host-only smoke; the plan A3 host==container gate needs both")
    parser.add_argument("--image", default=mo.IMAGE)
    parser.add_argument("--work-root", default=str(ROOT / "_tmp_stage_a_round"),
                        help="scratch dir for the raw results dump")
    parser.add_argument("--out-dir", default=None,
                        help="where to write the two records (default: committed Stage A dir)")
    parser.add_argument("--dry-run", action="store_true",
                        help="write records under --work-root instead of the committed dir")
    parser.add_argument("--worker-commit", default=None,
                        help="smoke escape hatch: pin this commit and BYPASS the "
                             "clean-tree gate (the real run omits it and enforces the gate)")
    parser.add_argument("--amend-latency", action="store_true",
                        help="on a latency breach vs Phase 1, derive a signed amendment "
                             "from THIS round (ceil(1.5 x measured p95), same Task 4 "
                             "rule) instead of STOPping; memory/error/retry stay "
                             "fail-loud vs Phase 1 with no amendment path")
    args = parser.parse_args(argv)

    work_root = Path(args.work_root)
    if args.out_dir is not None:
        out_dir = Path(args.out_dir)
    elif args.dry_run:
        out_dir = work_root / "records"
    else:
        out_dir = STAGE_A

    # Guard the COMMITTED evidence path: only a full, official N=8 host + N=8 container
    # round may write into the committed Stage A dir. A host-only (``--skip-container``)
    # or under-sampled (``--runs-per-leg != 8``) round still produces mismatch_count=0/
    # verdict "reproduced" records; writing those to the committed dir would publish
    # official-looking evidence that never ran the A3 host==container gate or the
    # required sample size. Such smoke runs must go to ``--dry-run``/``--out-dir``.
    writes_committed = out_dir.resolve() == STAGE_A.resolve()
    if writes_committed and args.skip_container:
        parser.error("--skip-container cannot write to the committed Stage A dir: the "
                     "A3 host==container gate requires BOTH legs. Use --dry-run or "
                     "--out-dir for a host-only smoke.")
    if writes_committed and args.runs_per_leg != 8:
        parser.error(f"--runs-per-leg={args.runs_per_leg} cannot write to the committed "
                     "Stage A dir: the official round is N=8 host + N=8 container. Use "
                     "--dry-run or --out-dir for a non-default run count.")
    if writes_committed and args.worker_commit is not None:
        parser.error("--worker-commit cannot write to the committed Stage A dir: it is "
                     "the smoke escape hatch that BYPASSES the clean-tree gate (records "
                     "clean_tree=False) and stamps tree_hashes for the override commit "
                     "rather than the code that actually ran, so a dirty/local smoke "
                     "could overwrite the official records with the wrong provenance. "
                     "Use --dry-run or --out-dir for a pinned-commit smoke.")
    if writes_committed and args.image != mo.IMAGE:
        parser.error(
            f"--image={args.image!r} cannot write to the committed Stage A dir: official "
            f"Stage A reuses the Phase 1 machinery and MUST measure latency/memory on the "
            f"Phase 1 image {mo.IMAGE!r} so the SLO evidence binds to the activation "
            f"runtime; measuring on another image would stamp an alternate image_id but "
            f"unbound SLO numbers. Use --dry-run or --out-dir for an alternate-image smoke.")

    raw = measure(args.runs_per_leg, skip_container=args.skip_container,
                  image=args.image, repo=ROOT,
                  worker_commit_override=args.worker_commit)
    work_root.mkdir(parents=True, exist_ok=True)
    _write(work_root / "raw_results.json", raw)

    all_hashes = [s["logical_output_hash"] for leg in raw["legs"].values() for s in leg]
    modal_hash = Counter(all_hashes).most_common(1)[0][0]
    mismatch_count = sum(1 for h in all_hashes if h != modal_hash)

    # Reproducibility gate: 16/16 identical, host==container (one pooled comparison).
    if mismatch_count != 0:
        divergent = sorted({h[:12] for h in all_hashes})
        print(f"REPRODUCIBILITY BREACH: mismatch_count={mismatch_count} over "
              f"{len(all_hashes)} runs; distinct hashes {divergent}", file=sys.stderr)
        print("STOP and investigate; NO records written.", file=sys.stderr)
        return 1

    # Bind the measured round to the CITED live-validation record: the child hashes a
    # fresh compute() result, but build_reproducibility_record stamps
    # live_validation_record_sha256 from whatever is on disk. If the snapshot/inputs
    # changed and the committed record was not regenerated first, a reproducible round
    # (mismatch_count=0) could still bind a stale record whose canonical hash does not
    # equal the measured modal hash. Recompute the record's canonical hash (same
    # function the child uses) and STOP unless it equals the modal hash.
    cited_hash = canonical_hash(
        json.loads(LIVE_VALIDATION_RECORD.read_text(encoding="utf-8")))
    if cited_hash != modal_hash:
        print(f"CITED-RECORD MISMATCH: measured modal hash {modal_hash[:12]} != "
              f"canonical hash {cited_hash[:12]} of {LIVE_VALIDATION_RECORD.name}; "
              "regenerate the live-validation record at HEAD before measuring.",
              file=sys.stderr)
        print("STOP and investigate; NO records written.", file=sys.stderr)
        return 1

    measured = measured_metrics(raw)
    thr = load_phase1_thresholds()

    # Hard gates first — memory/error/retry are ALWAYS judged vs Phase 1, breach =
    # STOP with NO records written, no amendment path exists for them.
    hard_breaches = {}
    for slo_id, key in (("memory_slo", "memory_peak_bytes"),
                        ("error_rate_slo", "error_rate"),
                        ("retry_rate_slo", "retry_rate")):
        if measured[key] > thr[slo_id]:
            hard_breaches[slo_id] = {"measured": measured[key],
                                     "threshold": thr[slo_id]}
    if hard_breaches:
        print(f"SLO BREACH vs Phase 1 signed thresholds: {hard_breaches}",
              file=sys.stderr)
        print("STOP and investigate; NEVER recalibrate; NO records written.",
              file=sys.stderr)
        return 1

    ratified = committed_latency_threshold()
    effective_latency_threshold = ratified if ratified is not None else thr["latency_slo"]
    latency_breach = measured["latency_p95_ms"] > effective_latency_threshold
    if latency_breach and not args.amend_latency:
        source = ("the ratified Stage A amendment" if ratified is not None
                  else "Phase 1 signed thresholds")
        print(f"SLO BREACH vs {source}: latency_slo measured "
              f"{measured['latency_p95_ms']} ms > {effective_latency_threshold} ms",
              file=sys.stderr)
        print("STOP and investigate; NEVER recalibrate; NO records written "
              "(pass --amend-latency only under the orchestrator's signed-amendment "
              "decision).", file=sys.stderr)
        return 1

    # Two-phase flow: the reproducibility record is written FIRST so the amendment
    # can pin the sha256 of the very round that derived it (egg-and-chicken resolved).
    repro_record = build_reproducibility_record(raw, modal_hash=modal_hash,
                                                mismatch_count=mismatch_count)
    repro_path = out_dir / "reproducibility_record.json"
    _write(repro_path, repro_record)

    amendment_threshold = None
    amendment_sha256 = None
    amendment_path = None
    if latency_breach:
        amendment = amendment_builder.build(
            measured["latency_p95_ms"], measured["runs_total"],
            _sha256_file(repro_path))
        amendment_path = out_dir / "slo_threshold_amendment_record.json"
        _write(amendment_path, amendment)
        amendment_sha256 = _sha256_file(amendment_path)
        amendment_threshold = amendment["amended_slo"]["threshold"]
        print(f"latency amendment: phase1 {thr['latency_slo']} ms breached; amended "
              f"threshold {amendment_threshold} ms = ceil(1.5 x p95 "
              f"{measured['latency_p95_ms']} ms) pinned to round "
              f"{amendment['measured_round']['reproducibility_record_sha256'][:12]}")
    else:
        # No latency breach this round: drop any amendment left by a PRIOR breaching
        # round in this output dir, so a valid no-amendment round never publishes (or
        # fails CI against) a stale slo_threshold_amendment_record.json.
        stale_amendment = out_dir / "slo_threshold_amendment_record.json"
        if stale_amendment.exists():
            stale_amendment.unlink()
            print(f"removed stale amendment {stale_amendment.name} (no latency breach "
                  "this round)")

    slo_record = build_slo_conformance_record(
        measured, thr, amendment_threshold=amendment_threshold,
        amendment_sha256=amendment_sha256, amendment_path=amendment_path)
    _write(out_dir / "slo_conformance_record.json", slo_record)

    lat = slo_record["conformance"]["latency_slo"]
    lat_note = (f"latency_p95={measured['latency_p95_ms']} ms "
                f"({lat['status']} vs "
                f"{lat.get('amended_threshold', lat.get('threshold'))} ms)")
    print(f"reproduced: {len(all_hashes)}/{len(all_hashes)} identical "
          f"(hash {modal_hash[:12]}), mismatch_count=0")
    print(f"conform: {lat_note}, memory_peak={measured['memory_peak_bytes']} B "
          f"(<= {thr['memory_slo']})")
    print(f"records written -> {out_dir} (measurement only; A5 blocked)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
