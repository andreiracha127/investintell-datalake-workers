"""ONE-COMMAND driver: trigger the phase0q cloud BACKTEST headlessly + fetch the verdict.

QC Research notebooks cannot be triggered headlessly (and the QC web UI would not run
ours). QC cloud BACKTESTS **can** be triggered via the API, so this driver adapts the
Phase 0Q cloud leg to a backtest and runs the whole loop from the user's terminal:

    python -m harness.phase0q_cloud.run_cloud_backtest \\
        --expected-manifest build/phase0q_cloud_bundle/expected_results_manifest.json

Steps:
  (a) push ``backtest_main.py`` content to project 33679769 ``main.py`` via ``files/update``;
  (b) ``compile/create`` + poll ``compile/read`` until ``BuildSuccess``;
  (c) ``backtests/create`` (name ``phase0q_cloud_leg_<timestamp>``);
  (d) poll ``backtests/read`` until completed;
  (e) read the verdict ESSENTIALS from the backtest's ``runtimeStatistics`` (the algorithm
      emits them via ``set_runtime_statistic``; ``backtests/read`` is the supported
      retrieval channel — there is no backtest log read endpoint in the lean api client)
      and validate them: leg-hash equality vs the local expected manifest, integer
      mismatch count, verdict value, internal consistency — failing loudly (echoing the
      backtest's error/stacktrace) if the keys are absent;
  (f) reconstruct and write ``phase0q_cloud_verdict.json`` locally (essentials + the
      per-hash table derived from the local expected manifest — the cloud leg hash is the
      stable hash over ALL output logical hashes, so a single equality proves the full
      per-hash set — plus a note pointing at the FULL verdict's Object Store key and its
      sha256);
  (g) validate hashes with :mod:`fetch_results` semantics and write the COMPLETED report to
      ``build/consolidated_reproducibility_report.completed.json`` (a NEW output path — this
      driver never completes the COMMITTED artifact; the orchestrator handles the artifact /
      PR step after reviewing) + print a summary table.

This module makes the ONLY outbound calls in the workflow and the USER runs it; nothing in
the build/test path invokes it. Robustness: every API response is checked for
``success: false`` with the errors echoed; polls time out with clear messages; the api-token
is NEVER printed. Governance is never flipped: A5 stays blocked; runtime_activation /
activation_allowed / allocator_publish / official_result stay false.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

from . import QC_PROJECT_ID
from .fetch_results import build_consolidated_report, compare_leg_hashes

API_BASE_URL = "https://www.quantconnect.com/api/v2/"
CREDENTIALS_PATH = Path.home() / ".lean" / "credentials"

DEFAULT_BACKTEST_MAIN = Path(__file__).with_name("backtest_main.py")
DEFAULT_VERDICT_OUT = Path("phase0q_cloud_verdict.json")
DEFAULT_REPORT_OUT = Path("build") / "consolidated_reproducibility_report.completed.json"

CLOUD_LEG_MANIFEST_PATH = (
    Path(__file__).resolve().parents[2]
    / "artifacts" / "quant" / "open_macro_v03_cloud_leg_001" / "cloud_leg_manifest.json"
)
_MANIFEST_KEY_SENTINEL = 'MANIFEST_KEY_INJECTED = ""'


def default_manifest_key() -> str:
    """The immutable manifest key pinned by the committed cloud-leg manifest."""
    manifest = json.loads(CLOUD_LEG_MANIFEST_PATH.read_text(encoding="utf-8"))
    return manifest["object_store_manifest_key"]


def inject_manifest_key(source: str, key: str) -> str:
    """Bake the manifest key into the uploaded main.py: API-created backtests
    have no project parameters and the key-file fallback is not part of the
    uploaded bundle, so the baked-in constant is the reliable path."""
    if _MANIFEST_KEY_SENTINEL not in source:
        raise ValueError("backtest main source lacks the MANIFEST_KEY_INJECTED sentinel")
    return source.replace(_MANIFEST_KEY_SENTINEL, f'MANIFEST_KEY_INJECTED = "{key}"', 1)


_MANIFEST_SHA_SENTINEL = 'MANIFEST_SHA256_INJECTED = ""'


def default_manifest_sha256(expected_manifest_path: Path) -> str:
    """The expected sha256 of the manifest OBJECT: hash of the local bundle's
    ``object_store_manifest.json`` (sibling of the expected manifest — the bundle is
    byte-identical-on-rebuild, so the local file pins the uploaded bytes)."""
    manifest_path = Path(expected_manifest_path).parent / "object_store_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"cannot pin the manifest bytes: {manifest_path} not found next to the "
            "expected manifest (pass --manifest-sha256 explicitly); expected sibling "
            "object_store_manifest.json")
    return hashlib.sha256(manifest_path.read_bytes()).hexdigest()


def inject_manifest_sha(source: str, sha: str) -> str:
    """Bake the expected manifest sha256 into the uploaded main.py so the algorithm
    verifies the pulled manifest BYTES before trusting them as the root of trust for
    the per-object sha checks (an overwritten manifest is refused before json.loads)."""
    if _MANIFEST_SHA_SENTINEL not in source:
        raise ValueError("backtest main source lacks the MANIFEST_SHA256_INJECTED sentinel")
    return source.replace(_MANIFEST_SHA_SENTINEL, f'MANIFEST_SHA256_INJECTED = "{sha}"', 1)


COMPILE_POLL_SECONDS = 5.0
COMPILE_TIMEOUT_SECONDS = 300.0
BACKTEST_POLL_SECONDS = 10.0
BACKTEST_TIMEOUT_SECONDS = 1800.0


# ---------------------------------------------------------------------------- #
# Credentials + per-request auth (matches lean CLI's APIClient._request).      #
# ---------------------------------------------------------------------------- #

def read_credentials(path: Path = CREDENTIALS_PATH) -> tuple[str, str]:
    """Read (user_id, api_token) from ``~/.lean/credentials``. The token is never logged."""
    if not path.is_file():
        raise FileNotFoundError(
            f"no QC credentials at {path}; run `lean login` first (user-id + api-token)")
    data = json.loads(path.read_text(encoding="utf-8"))
    user_id = data.get("user-id")
    api_token = data.get("api-token")
    if not user_id or not api_token:
        raise ValueError(f"{path} must contain 'user-id' and 'api-token'")
    return str(user_id), str(api_token)


def build_auth_headers(user_id: str, api_token: str, *, now: float | None = None) -> dict[str, str]:
    """Build the per-request QC auth headers.

    ``ts = str(int(now))``; ``hash = sha256(f"{token}:{ts}")``;
    ``Authorization: Basic base64(f"{uid}:{hash}")``; plus the ``Timestamp`` header.
    """
    timestamp = str(int(time.time() if now is None else now))
    password = hashlib.sha256(f"{api_token}:{timestamp}".encode("utf-8")).hexdigest()
    token = base64.b64encode(f"{user_id}:{password}".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {token}",
        "Timestamp": timestamp,
        "User-Agent": "phase0q-cloud-backtest-runner",
    }


# ---------------------------------------------------------------------------- #
# HTTP (urllib; mockable in tests — no network in the build/test path).        #
# ---------------------------------------------------------------------------- #

class QCApiError(RuntimeError):
    """Raised when a QC API response reports ``success: false`` or an HTTP error."""


def _build_request(endpoint: str, payload: dict[str, Any], headers: dict[str, str]):
    """Construct a JSON POST ``urllib.request.Request`` (kept separate for test injection)."""
    url = API_BASE_URL + endpoint
    body = json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    for key, value in headers.items():
        req.add_header(key, value)
    return req


def post(endpoint: str, payload: dict[str, Any], creds: tuple[str, str], *,
         opener=urllib_request.urlopen) -> dict[str, Any]:
    """Make an authenticated JSON POST and return the parsed response.

    Raises :class:`QCApiError` on transport failure or a ``success: false`` body (with the
    ``errors`` / ``messages`` echoed). The api-token never appears in any raised message.
    """
    user_id, api_token = creds
    headers = build_auth_headers(user_id, api_token)
    req = _build_request(endpoint, payload, headers)
    try:
        with opener(req) as handle:
            raw = handle.read()
    except HTTPError as exc:  # pragma: no cover - network-only
        detail = exc.read().decode("utf-8", "replace") if hasattr(exc, "read") else ""
        raise QCApiError(f"{endpoint} HTTP {exc.code}: {detail}") from exc
    except URLError as exc:  # pragma: no cover - network-only
        raise QCApiError(f"{endpoint} transport error: {exc.reason}") from exc

    data = json.loads(raw.decode("utf-8"))
    if not data.get("success", False):
        errors = data.get("errors") or data.get("messages") or [data.get("Message", "unknown error")]
        raise QCApiError(f"{endpoint} failed: " + "; ".join(str(e) for e in errors))
    return data


# ---------------------------------------------------------------------------- #
# Workflow steps                                                               #
# ---------------------------------------------------------------------------- #

def push_main(creds, project_id: int, main_source: str, *, opener=urllib_request.urlopen) -> None:
    """(a) Upload ``backtest_main.py`` content as the project's ``main.py``."""
    post("files/update", {"projectId": project_id, "name": "main.py", "content": main_source},
         creds, opener=opener)


def compile_project(creds, project_id: int, *, opener=urllib_request.urlopen,
                    poll_seconds: float = COMPILE_POLL_SECONDS,
                    timeout_seconds: float = COMPILE_TIMEOUT_SECONDS,
                    sleep=time.sleep) -> str:
    """(b) ``compile/create`` then poll ``compile/read`` until ``BuildSuccess``.

    Returns the compileId. Raises on ``BuildError`` (echoing the build logs) or timeout.
    """
    created = post("compile/create", {"projectId": project_id}, creds, opener=opener)
    compile_id = created["compileId"]
    deadline = time.monotonic() + timeout_seconds
    while True:
        read = post("compile/read", {"projectId": project_id, "compileId": compile_id},
                    creds, opener=opener)
        state = read.get("state")
        if state == "BuildSuccess":
            return compile_id
        if state == "BuildError":
            logs = "\n".join(read.get("logs", []))
            raise QCApiError(f"compile BuildError for compileId {compile_id}:\n{logs}")
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"compile did not finish within {timeout_seconds:.0f}s (last state={state})")
        sleep(poll_seconds)


def create_backtest(creds, project_id: int, compile_id: str, name: str,
                    *, opener=urllib_request.urlopen) -> str:
    """(c) ``backtests/create`` and return the backtestId."""
    created = post("backtests/create",
                   {"projectId": project_id, "compileId": compile_id, "backtestName": name},
                   creds, opener=opener)
    return created["backtest"]["backtestId"]


def poll_backtest(creds, project_id: int, backtest_id: str, *, opener=urllib_request.urlopen,
                  poll_seconds: float = BACKTEST_POLL_SECONDS,
                  timeout_seconds: float = BACKTEST_TIMEOUT_SECONDS,
                  sleep=time.sleep) -> dict[str, Any]:
    """(d) Poll ``backtests/read`` until completed. Returns the backtest object.

    Raises on a backtest error/stacktrace (echoed) or on a poll timeout.
    """
    deadline = time.monotonic() + timeout_seconds
    while True:
        read = post("backtests/read", {"projectId": project_id, "backtestId": backtest_id},
                    creds, opener=opener)
        backtest = read["backtest"]
        if backtest.get("error") or backtest.get("stacktrace"):
            detail = backtest.get("stacktrace") or backtest.get("error")
            raise QCApiError(f"backtest {backtest_id} errored:\n{detail}")
        if backtest.get("completed"):
            return backtest
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"backtest {backtest_id} did not complete within {timeout_seconds:.0f}s "
                f"(progress={backtest.get('progress')})")
        sleep(poll_seconds)


# ---------------------------------------------------------------------------- #
# Verdict retrieval via runtimeStatistics (the supported headless channel).    #
# ---------------------------------------------------------------------------- #

RUNTIME_STAT_KEYS = (
    "phase0q_verdict",
    "phase0q_cloud_leg_hash",
    "phase0q_expected_leg_hash",
    "phase0q_mismatch_count",
    "phase0q_verdict_sha256",
)


def extract_runtime_statistics(backtest: dict[str, Any]) -> dict[str, str]:
    """(e) Pull the verdict essentials out of the completed backtest's runtimeStatistics.

    Fails loudly — echoing the backtest's ``error`` / ``stacktrace`` fields — if any
    essential key is absent (e.g. the algorithm crashed before emitting them).
    """
    stats = backtest.get("runtimeStatistics") or {}
    missing = [k for k in RUNTIME_STAT_KEYS if k not in stats]
    if missing:
        detail = backtest.get("stacktrace") or backtest.get("error") or "<none>"
        raise QCApiError(
            "backtest completed without the verdict runtime statistics "
            f"(missing: {', '.join(missing)}; present: {sorted(stats)}). The algorithm "
            f"likely crashed before emitting them. error/stacktrace: {detail}")
    return {k: str(stats[k]) for k in RUNTIME_STAT_KEYS}


def validate_runtime_essentials(stats: dict[str, str], expected: dict[str, Any]) -> bool:
    """Validate the essentials against the LOCAL expected manifest. Returns leg_match.

    Refuses (QCApiError): a non-integer mismatch count; an unknown verdict value; an
    expected-leg-hash pin that differs from OUR manifest (the cloud ran against a
    different bundle); internally inconsistent essentials (verdict says reproduced but
    the hashes differ / count non-zero, or vice versa).
    """
    verdict_value = stats["phase0q_verdict"]
    if verdict_value not in ("reproduced", "mismatch"):
        raise QCApiError(f"unknown verdict runtime statistic value: {verdict_value!r}")
    try:
        mismatch_count = int(stats["phase0q_mismatch_count"])
    except ValueError as exc:
        raise QCApiError(
            f"phase0q_mismatch_count is not an integer: "
            f"{stats['phase0q_mismatch_count']!r}") from exc

    local_expected_leg = expected["execution_legs"]["local_python_pure"]["logical_hash"]
    if stats["phase0q_expected_leg_hash"] != local_expected_leg:
        raise QCApiError(
            "cloud expected leg hash does not match the local expected manifest "
            f"({stats['phase0q_expected_leg_hash']} != {local_expected_leg}); the cloud "
            "leg ran against a DIFFERENT bundle — refusing to reconcile")

    leg_match = stats["phase0q_cloud_leg_hash"] == local_expected_leg
    consistent = (verdict_value == "reproduced") == (leg_match and mismatch_count == 0)
    if not consistent:
        raise QCApiError(
            "inconsistent verdict essentials: "
            f"verdict={verdict_value!r} leg_match={leg_match} "
            f"mismatch_count={mismatch_count}")
    return leg_match


def verdict_key_from_manifest_key(manifest_key: str) -> str:
    """``<prefix>/object_store_manifest.json`` -> ``<prefix>/results/phase0q_cloud_verdict.json``."""
    suffix = "object_store_manifest.json"
    if not manifest_key.endswith(suffix):
        raise ValueError(f"unexpected manifest key shape: {manifest_key}")
    return manifest_key[: -len(suffix)] + "results/phase0q_cloud_verdict.json"


def reconstruct_verdict(stats: dict[str, str], expected: dict[str, Any],
                        verdict_key: str) -> dict[str, Any]:
    """(f) Rebuild a verdict document from the essentials + the local expected manifest.

    The cloud leg hash is the stable hash over ALL output logical hashes, so leg-hash
    equality proves the full per-hash set: on a match the per-hash table and fingerprint
    are derived from the local expected manifest; on a mismatch nothing is derived (the
    comparison then reports every hash as unconfirmed). The FULL verdict JSON lives in
    the Object Store at ``verdict_key`` with the pinned sha256.
    """
    leg_match = stats["phase0q_cloud_leg_hash"] == (
        expected["execution_legs"]["local_python_pure"]["logical_hash"])
    # Derivation requires BOTH the reproduced verdict AND leg-hash equality: when the
    # stats say mismatch (e.g. fingerprint/manifest-field drift while output hashes
    # coincide), nothing may be advertised as confirmed — the derived all-equal table
    # would misrepresent the mismatch. Detail then lives only in the full ObjectStore
    # verdict.
    derive = leg_match and stats["phase0q_verdict"] == "reproduced"
    return {
        "artifact_type": "phase0q_cloud_leg_verdict",
        "schema_version": 1,
        "reconstructed_from": "backtest_runtime_statistics",
        "execution_backend": "quantconnect_cloud_backtest",
        "derivation": "derived_from_expected_manifest" if derive else "withheld_unconfirmed",
        "run_fingerprint": expected["run_fingerprint"] if derive else None,
        "output_logical_hashes": (
            dict(expected["output_logical_hashes"]) if derive else {}),
        "execution_legs": {
            "qc_research_object_store": {
                "logical_hash": stats["phase0q_cloud_leg_hash"],
                "status": "complete",
            },
        },
        "runtime_statistics": dict(stats),
        "reproduced": stats["phase0q_verdict"] == "reproduced",
        "verdict": ("reproduced" if stats["phase0q_verdict"] == "reproduced"
                    else "not_reproduced"),
        "full_verdict_object_store_key": verdict_key,
        "full_verdict_sha256": stats["phase0q_verdict_sha256"],
        "notes": (
            "Reconstructed from the backtest runtimeStatistics essentials. The cloud leg "
            "hash is the stable hash over all output logical hashes, so a single equality "
            "proves the full per-hash set; the per-hash table above is derived from the "
            "local expected manifest on a leg-hash match. The FULL verdict JSON emitted "
            f"in the cloud is stored in the QC Object Store at {verdict_key} "
            f"(sha256 {stats['phase0q_verdict_sha256']})."
        ),
    }


# ---------------------------------------------------------------------------- #
# Validation (fetch_results semantics -> NEW completed-report path).           #
# ---------------------------------------------------------------------------- #

def validate_and_complete(verdict: dict[str, Any], expected_manifest_path: Path,
                          report_out: Path) -> dict[str, Any]:
    """(g) Validate the verdict vs the expected manifest and write the COMPLETED report.

    Uses the SAME comparison + report builder as :mod:`fetch_results` (exact logical-hash
    match; 1e-12 float tolerance), but writes to a NEW output path so the committed artifact
    is untouched — the orchestrator completes that separately after review.
    """
    expected = json.loads(expected_manifest_path.read_text(encoding="utf-8"))
    comparison = compare_leg_hashes(expected, verdict)
    report = build_consolidated_report(expected, verdict, comparison)
    report["notes"] = (
        report["notes"] + " Completed by run_cloud_backtest (NEW output path); the committed "
        "artifact is completed separately by the orchestrator after review. Execution model: "
        "the reproducibility leg was adapted from a QC Research notebook to a headless-"
        "triggerable QC cloud BACKTEST; the compute is identical."
    )
    report_out = Path(report_out)
    report_out.parent.mkdir(parents=True, exist_ok=True)
    report_out.write_text(
        json.dumps(report, sort_keys=True, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return {"report": report, "comparison": comparison, "report_out": str(report_out)}


def format_summary_table(verdict: dict[str, Any], comparison: dict[str, Any]) -> str:
    """A compact human-readable summary of the per-hash comparison."""
    lines = ["", "phase0q cloud backtest — reproducibility summary", "=" * 52]
    per_hash = comparison["output_logical_hashes"]
    for key in sorted(per_hash):
        mark = "OK " if per_hash[key]["match"] else "XX "
        lines.append(f"  {mark} {key}")
    fp = comparison["run_fingerprint"]
    leg = comparison["execution_leg_logical_hash"]
    lines.append(f"  {'OK ' if fp['match'] else 'XX '} run_fingerprint")
    lines.append(f"  {'OK ' if leg['match'] else 'XX '} execution_leg_logical_hash")
    lines.append("-" * 52)
    lines.append(f"  verdict: {verdict['verdict']}  mismatches: {comparison['mismatch_count']}")
    lines.append(f"  reproduced: {comparison['all_hashes_match']}")
    lines.append("=" * 52)
    return "\n".join(lines)


# ---------------------------------------------------------------------------- #
# Orchestration                                                                #
# ---------------------------------------------------------------------------- #

def run(args: argparse.Namespace, *, opener=urllib_request.urlopen,
        creds: tuple[str, str] | None = None) -> dict[str, Any]:
    """Run the full driver loop. Returns a summary dict (also printed by ``main``)."""
    creds = creds or read_credentials(Path(args.credentials))
    project_id = args.project_id
    main_source = Path(args.main).read_text(encoding="utf-8")
    manifest_key = getattr(args, "manifest_key", None) or default_manifest_key()
    manifest_sha = (getattr(args, "manifest_sha256", None)
                    or default_manifest_sha256(Path(args.expected_manifest)))
    main_source = inject_manifest_key(main_source, manifest_key)
    main_source = inject_manifest_sha(main_source, manifest_sha)

    print(f"[a] pushing {args.main} -> project {project_id} main.py")
    print(f"    manifest key baked in:    {manifest_key}")
    print(f"    manifest sha256 baked in: {manifest_sha}")
    push_main(creds, project_id, main_source, opener=opener)

    print("[b] compiling project ...")
    compile_id = compile_project(creds, project_id, opener=opener)
    print(f"    compileId={compile_id}")

    name = args.name or f"phase0q_cloud_leg_{int(time.time())}"
    print(f"[c] creating backtest '{name}' ...")
    backtest_id = create_backtest(creds, project_id, compile_id, name, opener=opener)
    print(f"    backtestId={backtest_id}")

    print("[d] polling backtest until completed ...")
    backtest = poll_backtest(creds, project_id, backtest_id, opener=opener)

    print("[e] reading verdict essentials from runtimeStatistics ...")
    stats = extract_runtime_statistics(backtest)
    expected = json.loads(Path(args.expected_manifest).read_text(encoding="utf-8"))
    validate_runtime_essentials(stats, expected)

    verdict_key = verdict_key_from_manifest_key(manifest_key)
    verdict = reconstruct_verdict(stats, expected, verdict_key)
    verdict_out = Path(args.verdict_out)
    verdict_out.parent.mkdir(parents=True, exist_ok=True)
    verdict_out.write_text(
        json.dumps(verdict, sort_keys=True, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"[f] wrote reconstructed verdict -> {verdict_out}")
    print(f"    full verdict in Object Store: {verdict_key}")
    print(f"    full verdict sha256:          {stats['phase0q_verdict_sha256']}")

    completed = validate_and_complete(verdict, Path(args.expected_manifest), Path(args.report_out))
    print(f"[g] wrote completed report -> {completed['report_out']}")
    print(format_summary_table(verdict, completed["comparison"]))

    return {
        "backtest_id": backtest_id,
        "compile_id": compile_id,
        "verdict": verdict["verdict"],
        "reproduced": completed["comparison"]["all_hashes_match"],
        "mismatch_count": completed["comparison"]["mismatch_count"],
        "verdict_out": str(verdict_out),
        "report_out": completed["report_out"],
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m harness.phase0q_cloud.run_cloud_backtest",
        description="Trigger the phase0q cloud BACKTEST headlessly and fetch the verdict.",
    )
    parser.add_argument("--expected-manifest", required=True,
                        help="Path to the bundle's expected_results_manifest.json.")
    parser.add_argument("--project-id", type=int, default=QC_PROJECT_ID,
                        help=f"QC project id (default: {QC_PROJECT_ID}).")
    parser.add_argument("--main", default=str(DEFAULT_BACKTEST_MAIN),
                        help="Path to the backtest algorithm source pushed as main.py.")
    parser.add_argument("--manifest-key", dest="manifest_key", default=None,
                        help="immutable object-store manifest key (default: from the committed cloud_leg_manifest.json)")
    parser.add_argument("--manifest-sha256", dest="manifest_sha256", default=None,
                        help="expected sha256 of the manifest object (default: hash of the "
                             "local bundle's object_store_manifest.json next to --expected-manifest)")
    parser.add_argument("--name", default=None,
                        help="Backtest name (default: phase0q_cloud_leg_<timestamp>).")
    parser.add_argument("--credentials", default=str(CREDENTIALS_PATH),
                        help="Path to ~/.lean/credentials (user-id + api-token).")
    parser.add_argument("--verdict-out", default=str(DEFAULT_VERDICT_OUT),
                        help="Where to write the reconstructed verdict JSON.")
    parser.add_argument("--report-out", default=str(DEFAULT_REPORT_OUT),
                        help="Where to write the COMPLETED consolidated report (NEW path).")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    summary = run(args)
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0 if summary["reproduced"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
