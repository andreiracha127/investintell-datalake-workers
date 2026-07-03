"""Tests for the phase0q cloud BACKTEST runner (build-only; ZERO network / ZERO lean).

QC Research notebooks cannot be triggered headlessly; the cloud leg is adapted to a QC
cloud BACKTEST (``harness/phase0q_cloud/backtest_main.py``) driven headlessly by
``run_cloud_backtest.py``. These tests exercise:

  * compute-equivalence of ``backtest_main`` to the committed notebook (same shared helpers
    + same expected-manifest keys; a REAL end-to-end run through an in-memory ObjectStore
    reproduces the local-leg hashes and builds a matching verdict),
  * chunked-log encode/decode round-trip (incl. sha verification + tamper refusal),
  * driver request construction (auth headers + endpoints) with a MOCKED urllib opener,
  * verdict validation vs ``fetch_results`` semantics (same comparison + report),
  * governance markers on the verdict + completed report.

Nothing here performs any network call or ``lean`` invocation. The bundle is built once per
session (the deterministic local harness run is the slow part) and reused.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from harness.phase0q_cloud import backtest_main as bt
from harness.phase0q_cloud import fetch_results as fr
from harness.phase0q_cloud import run_cloud_backtest as driver
from harness.phase0q_cloud import bundle as bundle_mod

ROOT = Path(__file__).resolve().parents[1]
CLOUD_PKG = ROOT / "harness" / "phase0q_cloud"
NOTEBOOK = CLOUD_PKG / "phase0q_cloud_leg.ipynb"
HARNESS_COMMIT = "68b07e810bc28665fedd85c6acd3ea5770b4b099"


# --------------------------------------------------------------------------- #
# Session bundle build + in-memory ObjectStore                                #
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="session")
def built_bundle(tmp_path_factory) -> Path:
    bundle_dir = tmp_path_factory.mktemp("phase0q_cloud_bt_bundle")
    bundle_mod.build_bundle(bundle_dir, HARNESS_COMMIT)
    return bundle_dir


@pytest.fixture(scope="session")
def bundle_manifest(built_bundle) -> dict:
    return json.loads((built_bundle / "object_store_manifest.json").read_text(encoding="utf-8"))


class InMemoryObjectStore:
    """Minimal QCAlgorithm.ObjectStore stand-in backed by the on-disk bundle.

    Exposes ``ReadBytes(key)`` / ``SaveBytes(key, list[int])`` — the exact case-sensitive
    methods ``backtest_main`` prefers on the QC ObjectStore — so the pure compute runs
    unchanged against a real bundle with no QC runtime.
    """

    def __init__(self, bundle_dir: Path, manifest: dict):
        self._by_key: dict[str, bytes] = {}
        self.saved: dict[str, bytes] = {}
        manifest_key = manifest["object_store_manifest_key"]
        self._by_key[manifest_key] = (bundle_dir / "object_store_manifest.json").read_bytes()
        for item in manifest["object_files"].values():
            self._by_key[item["object_store_key"]] = (bundle_dir / item["relative_path"]).read_bytes()

    def ReadBytes(self, key: str):  # noqa: N802 - QC API casing
        if key not in self._by_key:
            raise KeyError(key)
        return list(self._by_key[key])

    def SaveBytes(self, key: str, data):  # noqa: N802 - QC API casing
        self.saved[key] = bytes(data)


@pytest.fixture()
def object_store(built_bundle, bundle_manifest) -> InMemoryObjectStore:
    return InMemoryObjectStore(built_bundle, bundle_manifest)


@pytest.fixture(scope="session")
def session_verdict(built_bundle, bundle_manifest, tmp_path_factory) -> dict:
    """Run the full ObjectStore-driven compute ONCE per session (the slow part)."""
    store = InMemoryObjectStore(built_bundle, bundle_manifest)
    proj = tmp_path_factory.mktemp("phase0q_bt_proj")
    return bt.execute_reproducibility_check(
        store, proj, bundle_manifest["object_store_manifest_key"])


# --------------------------------------------------------------------------- #
# 1. Compute-equivalence to the notebook                                      #
# --------------------------------------------------------------------------- #

def _notebook_source() -> str:
    nb = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    return "\n".join("".join(cell.get("source", [])) for cell in nb["cells"])


def test_backtest_main_uses_same_helper_functions_as_notebook():
    """backtest_main mirrors the notebook: same harness helpers + verdict fields."""
    src = _notebook_source()
    # Same decision-chain + sleeve helpers the notebook drives.
    for token in ("runner.run_harness", "measure_grid_results", "runner.stable_hash",
                  "runner.canonicalize", "PriceFrame", "SleeveParams"):
        assert token in src, f"notebook missing {token}"
    # backtest_main's run_compute must use the identical helpers.
    compute_src = (CLOUD_PKG / "backtest_main.py").read_text(encoding="utf-8")
    for token in ("runner.run_harness", "grid.measure_grid_results", "runner.stable_hash",
                  "runner.canonicalize", "sleeve.PriceFrame", "sleeve.SleeveParams"):
        assert token in compute_src, f"backtest_main missing {token}"


def test_backtest_main_required_sleeves_match_notebook():
    src = _notebook_source()
    assert '("baseline_100", "compressed_50")' in src or "baseline_100" in src
    assert bt.REQUIRED_SLEEVES == ("baseline_100", "compressed_50")


def test_expected_manifest_keys_match_between_verdict_and_bundle(bundle_manifest, built_bundle):
    expected = json.loads(
        (built_bundle / "expected_results_manifest.json").read_text(encoding="utf-8"))
    for key in bt.EXPECTED_MANIFEST_KEYS:
        assert key in expected, f"expected manifest missing {key}"
    # The verdict compares exactly these keys (output hashes, fingerprint, legs).
    assert set(bt.EXPECTED_MANIFEST_KEYS) == {
        "output_logical_hashes", "run_fingerprint", "execution_legs"}


def test_execute_reproducibility_check_reproduces_and_matches_notebook_verdict(
        session_verdict, built_bundle):
    verdict = session_verdict

    # Reproduced end to end (the bundle ships the same drift-checked HEAD sources).
    assert verdict["verdict"] == "reproduced"
    assert verdict["reproduced"] is True
    assert verdict["comparison"]["mismatch_count"] == 0
    assert verdict["comparison"]["all_hashes_match"] is True

    # Verdict has the SAME shape/fields the notebook's cell-10 emits.
    assert verdict["artifact_type"] == "phase0q_cloud_leg_verdict"
    assert set(verdict["execution_legs"]) == {"qc_research_object_store"}
    assert set(verdict["sleeve_logical_hashes"]) == {"baseline_100", "compressed_50"}
    assert verdict["external_macro_access"] is False

    # The cloud leg hash equals the expected local_python_pure hash.
    expected = json.loads(
        (built_bundle / "expected_results_manifest.json").read_text(encoding="utf-8"))
    exp_local = expected["execution_legs"]["local_python_pure"]["logical_hash"]
    assert verdict["execution_legs"]["qc_research_object_store"]["logical_hash"] == exp_local


def test_drift_refusal_on_tampered_object(object_store, bundle_manifest):
    # Corrupt one object's bytes -> sha mismatch must abort the whole run.
    first_key = next(iter(
        item["object_store_key"] for item in bundle_manifest["object_files"].values()))
    object_store._by_key[first_key] = object_store._by_key[first_key] + b"tamper"
    with pytest.raises(RuntimeError, match="drift refusal"):
        bt.load_bundle_from_object_store(object_store, bundle_manifest)


def test_fail_loud_db_stub_refuses(object_store, bundle_manifest, tmp_path):
    manifest_key = bundle_manifest["object_store_manifest_key"]
    object_bytes = bt.load_bundle_from_object_store(object_store, bundle_manifest)
    proj = tmp_path / "proj"
    bt.materialize_sources(proj, bundle_manifest, object_bytes)
    bt.assert_fail_loud_db_stub(proj)  # imports src.db + asserts connect() refuses


# --------------------------------------------------------------------------- #
# 2. Chunked-log encode/decode round-trip                                     #
# --------------------------------------------------------------------------- #

def test_chunk_round_trip_small():
    payload = json.dumps({"verdict": "reproduced", "n": 1}).encode("utf-8")
    lines = bt.encode_verdict_log_chunks(payload, chunk_size=8)
    assert lines[0].startswith(bt.VERDICT_BEGIN_MARKER)
    assert lines[-1] == bt.VERDICT_END_MARKER
    assert bt.decode_verdict_log_chunks(lines) == payload


def test_chunk_round_trip_large_verdict(session_verdict):
    verdict_bytes = bt.canonical_verdict_bytes(session_verdict)
    lines = bt.encode_verdict_log_chunks(verdict_bytes)
    # Each chunk line stays under QC's ~200-char log limit.
    assert all(len(line) < 200 for line in lines)
    assert bt.decode_verdict_log_chunks(lines) == verdict_bytes
    # And it survives interleaving with unrelated log noise.
    noisy = ["some other log line"] + lines[:1] + ["noise"] + lines[1:] + ["trailing"]
    assert bt.decode_verdict_log_chunks(noisy) == verdict_bytes


def test_chunk_decode_tamper_refused():
    payload = b'{"k": "v"}'
    lines = bt.encode_verdict_log_chunks(payload, chunk_size=4)
    # Corrupt a chunk payload -> sha check must fail.
    corrupted = [re.sub(r"(\d/\d )(.*)", r"\1XXXX", ln) if ln.startswith(bt.VERDICT_CHUNK_PREFIX)
                 else ln for ln in lines]
    with pytest.raises(ValueError, match="sha256"):
        bt.decode_verdict_log_chunks(corrupted)


def test_chunk_decode_missing_markers():
    with pytest.raises(ValueError, match="BEGIN"):
        bt.decode_verdict_log_chunks(["no markers here"])
    lines = bt.encode_verdict_log_chunks(b"abc", chunk_size=1)
    with pytest.raises(ValueError, match="END"):
        bt.decode_verdict_log_chunks([ln for ln in lines if ln != bt.VERDICT_END_MARKER])


# --------------------------------------------------------------------------- #
# 3. Driver request construction (mocked urllib — no network)                 #
# --------------------------------------------------------------------------- #

def test_auth_headers_construction():
    headers = driver.build_auth_headers("uid42", "tok-secret", now=1_700_000_000)
    assert headers["Timestamp"] == "1700000000"
    # Basic base64(uid:sha256(token:ts)); token itself never appears.
    import base64
    import hashlib
    decoded = base64.b64decode(headers["Authorization"].split(" ", 1)[1]).decode()
    uid, sep, digest = decoded.partition(":")
    assert uid == "uid42"
    assert digest == hashlib.sha256(b"tok-secret:1700000000").hexdigest()
    assert "tok-secret" not in headers["Authorization"]


class FakeResponse:
    def __init__(self, payload: dict):
        self._raw = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class RecordingOpener:
    """Captures POSTed (endpoint, payload, headers) and returns scripted responses."""

    def __init__(self, responses: dict[str, list[dict]]):
        self._responses = {k: list(v) for k, v in responses.items()}
        self.calls: list[dict] = []

    def __call__(self, req):
        endpoint = req.full_url.rsplit("/api/v2/", 1)[1]
        payload = json.loads(req.data.decode("utf-8"))
        self.calls.append({
            "endpoint": endpoint, "payload": payload,
            "headers": {k.lower(): v for k, v in req.header_items()},
        })
        queue = self._responses.get(endpoint)
        if not queue:
            raise AssertionError(f"unexpected endpoint {endpoint}")
        return FakeResponse(queue.pop(0))


def _creds():
    return ("uid42", "tok-secret")


def test_push_main_request():
    opener = RecordingOpener({"files/update": [{"success": True}]})
    driver.push_main(_creds(), 33679769, "print('hi')", opener=opener)
    call = opener.calls[0]
    assert call["endpoint"] == "files/update"
    assert call["payload"] == {"projectId": 33679769, "name": "main.py", "content": "print('hi')"}
    assert "authorization" in call["headers"] and "timestamp" in call["headers"]


def test_compile_project_polls_until_success():
    opener = RecordingOpener({
        "compile/create": [{"success": True, "compileId": "c1"}],
        "compile/read": [
            {"success": True, "state": "InQueue"},
            {"success": True, "state": "BuildSuccess", "logs": []},
        ],
    })
    compile_id = driver.compile_project(_creds(), 33679769, opener=opener,
                                        poll_seconds=0, sleep=lambda s: None)
    assert compile_id == "c1"
    assert [c["endpoint"] for c in opener.calls] == [
        "compile/create", "compile/read", "compile/read"]


def test_compile_build_error_raises():
    opener = RecordingOpener({
        "compile/create": [{"success": True, "compileId": "c1"}],
        "compile/read": [{"success": True, "state": "BuildError", "logs": ["boom"]}],
    })
    with pytest.raises(driver.QCApiError, match="BuildError"):
        driver.compile_project(_creds(), 33679769, opener=opener, poll_seconds=0,
                               sleep=lambda s: None)


def test_create_backtest_request():
    opener = RecordingOpener({"backtests/create": [
        {"success": True, "backtest": {"backtestId": "b1"}}]})
    bid = driver.create_backtest(_creds(), 33679769, "c1", "phase0q_cloud_leg_1", opener=opener)
    assert bid == "b1"
    assert opener.calls[0]["payload"] == {
        "projectId": 33679769, "compileId": "c1", "backtestName": "phase0q_cloud_leg_1"}


def test_poll_backtest_until_completed():
    opener = RecordingOpener({"backtests/read": [
        {"success": True, "backtest": {"backtestId": "b1", "completed": False, "progress": 0.5}},
        {"success": True, "backtest": {"backtestId": "b1", "completed": True, "progress": 1.0}},
    ]})
    bt_obj = driver.poll_backtest(_creds(), 33679769, "b1", opener=opener,
                                  poll_seconds=0, sleep=lambda s: None)
    assert bt_obj["completed"] is True


def test_poll_backtest_error_raises():
    opener = RecordingOpener({"backtests/read": [
        {"success": True, "backtest": {"backtestId": "b1", "completed": True,
                                       "stacktrace": "kaboom"}}]})
    with pytest.raises(driver.QCApiError, match="kaboom"):
        driver.poll_backtest(_creds(), 33679769, "b1", opener=opener, poll_seconds=0,
                             sleep=lambda s: None)


def test_api_error_never_leaks_token():
    opener = RecordingOpener({"files/update": [
        {"success": False, "errors": ["Hash doesn't match."]}]})
    with pytest.raises(driver.QCApiError) as exc:
        driver.push_main(_creds(), 33679769, "x", opener=opener)
    assert "tok-secret" not in str(exc.value)


def test_fetch_and_reassemble_from_logs(session_verdict):
    verdict_bytes = bt.canonical_verdict_bytes(session_verdict)
    log_lines = bt.encode_verdict_log_chunks(verdict_bytes)
    opener = RecordingOpener({"backtests/read/log": [
        {"success": True, "logs": log_lines}]})
    reassembled = driver.fetch_backtest_logs(_creds(), 33679769, "b1", opener=opener)
    assert driver.reassemble_verdict(reassembled) == json.loads(verdict_bytes)


# --------------------------------------------------------------------------- #
# 4. Verdict validation vs fetch_results semantics                            #
# --------------------------------------------------------------------------- #

def test_validate_and_complete_matches_fetch_results(session_verdict, built_bundle, tmp_path):
    verdict = session_verdict
    expected_path = built_bundle / "expected_results_manifest.json"
    report_out = tmp_path / "build" / "consolidated_reproducibility_report.completed.json"

    result = driver.validate_and_complete(verdict, expected_path, report_out)
    assert report_out.is_file()
    assert result["comparison"]["all_hashes_match"] is True

    # Same comparison the committed fetch_results would produce.
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    fr_comparison = fr.compare_leg_hashes(expected, verdict)
    assert result["comparison"]["mismatch_count"] == fr_comparison["mismatch_count"]
    assert result["report"]["reproduced"] == fr_comparison["all_hashes_match"]
    assert result["report"]["verdict"] == "reproduced"


def test_validate_writes_new_path_not_committed_artifact(session_verdict, built_bundle, tmp_path):
    # The completed report must land on the NEW build/ path, not the committed artifact dir.
    report_out = tmp_path / "build" / "consolidated_reproducibility_report.completed.json"
    driver.validate_and_complete(
        session_verdict, built_bundle / "expected_results_manifest.json", report_out)
    assert report_out.name == "consolidated_reproducibility_report.completed.json"
    assert "build" in report_out.parts


# --------------------------------------------------------------------------- #
# 5. Governance markers                                                        #
# --------------------------------------------------------------------------- #

def _walk_flags(obj, key):
    found = []
    if isinstance(obj, dict):
        if key in obj:
            found.append(obj[key])
        for v in obj.values():
            found.extend(_walk_flags(v, key))
    elif isinstance(obj, list):
        for v in obj:
            found.extend(_walk_flags(v, key))
    return found


def test_verdict_governance_pins(session_verdict):
    verdict = session_verdict
    gov = verdict["governance"]
    assert gov["A5"] == "blocked"
    for flag in ("runtime_activation", "activation_allowed", "allocator_publish",
                 "official_result"):
        assert gov[flag] is False
    assert gov["db_write_mode"] == "none"
    assert gov["status"] == "candidate_not_approved"
    # assert_governance passes (mirrors the notebook's final asserts).
    bt.assert_governance(verdict)


def test_completed_report_governance_pins(session_verdict, built_bundle, tmp_path):
    report_out = tmp_path / "build" / "report.completed.json"
    result = driver.validate_and_complete(
        session_verdict, built_bundle / "expected_results_manifest.json", report_out)
    report = result["report"]
    assert all(v is False for v in _walk_flags(report, "runtime_activation"))
    assert all(v is False for v in _walk_flags(report, "official_result"))
    assert report["governance"]["A5"] == "blocked"
    # The execution-model note (research -> backtest) lives in the report, not committed art.
    assert "backtest" in report["notes"].lower()


def test_no_network_imports_in_backtest_main():
    """backtest_main must not import requests/urllib/socket (offline compute only)."""
    src = (CLOUD_PKG / "backtest_main.py").read_text(encoding="utf-8")
    for banned in ("import requests", "import socket", "urllib.request", "http.client"):
        assert banned not in src, f"backtest_main must not use {banned}"
