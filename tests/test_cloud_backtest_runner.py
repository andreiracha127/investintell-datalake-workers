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
HARNESS_COMMIT = "130b754bdbf06ab92f80075933e8b9784bba3a27"


# --------------------------------------------------------------------------- #
# Global-state isolation                                                       #
# --------------------------------------------------------------------------- #
_CLOSURE_ROOTS = ("src", "harness", "investintell_quant_core")
_PRISTINE_SYS_PATH: list | None = None
_PRISTINE_CLOSURE_MODULES: dict | None = None


def _in_shipped_closure(name: str) -> bool:
    return name in _CLOSURE_ROOTS or name.split(".", 1)[0] in _CLOSURE_ROOTS


@pytest.fixture(scope="session", autouse=True)
def _snapshot_shipped_closure():
    """Capture the PRISTINE shipped-closure state ONCE at session start.

    The snapshot MUST be taken before any session-scoped cloud fixture
    (``built_bundle`` / ``session_verdict``) runs ``execute_reproducibility_check`` /
    ``run_compute``, which purge ``src.*`` / ``harness.*`` / ``investintell_quant_core.*``
    from ``sys.modules`` and insert a materialized bundle root on ``sys.path`` — swapping
    in the fail-loud ``src/db.py`` STUB (only ``LOCK_REGIME_QUADRANT``). pytest sets up
    higher-scoped fixtures first, so a module-scoped snapshot would capture the ALREADY
    polluted state; this session-scoped autouse fixture snapshots the clean state first."""
    import sys
    global _PRISTINE_SYS_PATH, _PRISTINE_CLOSURE_MODULES
    _PRISTINE_SYS_PATH = list(sys.path)
    _PRISTINE_CLOSURE_MODULES = {n: m for n, m in sys.modules.items()
                                 if _in_shipped_closure(n)}
    yield


@pytest.fixture(scope="module", autouse=True)
def _restore_shipped_closure_after_purge():
    """Restore the PRISTINE closure (captured at session start) once THIS module's tests
    finish, so the cloud stub ``src.db`` cannot leak into later tests and break the
    lock-id registry guards with ``AttributeError: ... 'LOCK_REGIME_GATE'``. Restoring at
    MODULE teardown (not session teardown) keeps the leak from reaching tests that run
    after this module but before the session ends."""
    import sys
    yield
    sys.path[:] = _PRISTINE_SYS_PATH
    for name in [n for n in sys.modules
                 if _in_shipped_closure(n) and n not in _PRISTINE_CLOSURE_MODULES]:
        del sys.modules[name]
    sys.modules.update(_PRISTINE_CLOSURE_MODULES)


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


def _bundle_manifest_sha(built_bundle: Path) -> str:
    return bt.sha256_hex((built_bundle / "object_store_manifest.json").read_bytes())


@pytest.fixture(scope="session")
def session_verdict(built_bundle, bundle_manifest, tmp_path_factory) -> dict:
    """Run the full ObjectStore-driven compute ONCE per session (the slow part)."""
    store = InMemoryObjectStore(built_bundle, bundle_manifest)
    proj = tmp_path_factory.mktemp("phase0q_bt_proj")
    return bt.execute_reproducibility_check(
        store, proj, bundle_manifest["object_store_manifest_key"],
        expected_manifest_sha256=_bundle_manifest_sha(built_bundle))


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


# --------------------------------------------------------------------------- #
# 3b. Verdict retrieval via runtimeStatistics (backtests/read/log is NOT a     #
#     supported endpoint — the lean api client only uses read/create/delete)   #
# --------------------------------------------------------------------------- #

def test_build_runtime_statistics_essentials(session_verdict):
    verdict_bytes = bt.canonical_verdict_bytes(session_verdict)
    stats = bt.build_runtime_statistics(session_verdict, verdict_bytes)
    assert set(stats) == {
        "phase0q_verdict", "phase0q_cloud_leg_hash", "phase0q_expected_leg_hash",
        "phase0q_mismatch_count", "phase0q_verdict_sha256", "phase0q_fullverdict_saved"}
    assert stats["phase0q_fullverdict_saved"] == "true"
    assert stats["phase0q_verdict"] == "reproduced"
    assert stats["phase0q_cloud_leg_hash"] == stats["phase0q_expected_leg_hash"]
    assert stats["phase0q_mismatch_count"] == "0"
    assert stats["phase0q_verdict_sha256"] == bt.sha256_hex(verdict_bytes)
    # runtimeStatistics values are strings on the QC side.
    assert all(isinstance(v, str) for v in stats.values())


def test_build_runtime_statistics_mismatch_value(session_verdict):
    doctored = json.loads(json.dumps(session_verdict))
    doctored["reproduced"] = False
    doctored["verdict"] = "not_reproduced"
    doctored["comparison"]["mismatch_count"] = 3
    stats = bt.build_runtime_statistics(doctored, b"{}")
    assert stats["phase0q_verdict"] == "mismatch"
    assert stats["phase0q_mismatch_count"] == "3"


def _synthetic_expected():
    return {
        "output_logical_hashes": {"h_a": "aaa", "h_b": "bbb"},
        "run_fingerprint": "fp-1",
        "execution_legs": {"local_python_pure": {"logical_hash": "LEG"}},
    }


def _stats(verdict="reproduced", cloud="LEG", exp="LEG", count="0", sha="s" * 64,
           saved="true"):
    return {
        "phase0q_verdict": verdict,
        "phase0q_cloud_leg_hash": cloud,
        "phase0q_expected_leg_hash": exp,
        "phase0q_mismatch_count": count,
        "phase0q_verdict_sha256": sha,
        "phase0q_fullverdict_saved": saved,
    }


def test_validate_runtime_essentials_ok():
    assert driver.validate_runtime_essentials(_stats(), _synthetic_expected()) is True


def test_validate_runtime_essentials_missing_keys_fail_loud():
    backtest = {"error": "boom during OnEndOfAlgorithm", "runtimeStatistics": {"Equity": "1"}}
    with pytest.raises(driver.QCApiError, match="runtime statistics"):
        driver.extract_runtime_statistics(backtest)
    # The algorithm's error/stacktrace fields are echoed for diagnosis.
    with pytest.raises(driver.QCApiError, match="boom during OnEndOfAlgorithm"):
        driver.extract_runtime_statistics(backtest)


def test_validate_runtime_essentials_rejects_bad_values():
    with pytest.raises(driver.QCApiError, match="mismatch_count"):
        driver.validate_runtime_essentials(_stats(count="NaN"), _synthetic_expected())
    with pytest.raises(driver.QCApiError, match="verdict"):
        driver.validate_runtime_essentials(_stats(verdict="maybe"), _synthetic_expected())
    # Cloud ran against a DIFFERENT bundle: its expected-leg pin differs from ours.
    with pytest.raises(driver.QCApiError, match="expected leg hash"):
        driver.validate_runtime_essentials(_stats(exp="OTHER", cloud="OTHER"),
                                           _synthetic_expected())
    # Internally inconsistent essentials must be refused.
    with pytest.raises(driver.QCApiError, match="inconsistent"):
        driver.validate_runtime_essentials(_stats(verdict="reproduced", cloud="X", count="1"),
                                           _synthetic_expected())


def test_reconstruct_verdict_leg_match_derives_per_hash_table():
    expected = _synthetic_expected()
    key = "investintell/p/q/results/phase0q_cloud_verdict.json"
    verdict = driver.reconstruct_verdict(_stats(), expected, key)
    # A single leg-hash equality proves the full per-hash set -> derived table.
    assert verdict["output_logical_hashes"] == expected["output_logical_hashes"]
    assert verdict["run_fingerprint"] == expected["run_fingerprint"]
    assert verdict["execution_legs"]["qc_research_object_store"]["logical_hash"] == "LEG"
    assert verdict["full_verdict_object_store_key"] == key
    assert verdict["full_verdict_sha256"] == "s" * 64
    assert key in verdict["notes"]
    # fetch_results semantics accept the reconstruction as reproduced.
    comparison = fr.compare_leg_hashes(expected, verdict)
    assert comparison["all_hashes_match"] is True


def test_reconstruct_verdict_leg_mismatch_does_not_derive():
    expected = _synthetic_expected()
    stats = _stats(verdict="mismatch", cloud="DIFFERENT", count="1")
    verdict = driver.reconstruct_verdict(stats, expected, "k")
    assert verdict["output_logical_hashes"] == {}
    assert verdict["run_fingerprint"] is None
    comparison = fr.compare_leg_hashes(expected, verdict)
    assert comparison["all_hashes_match"] is False


def test_verdict_key_from_manifest_key():
    key = driver.verdict_key_from_manifest_key("a/b/c/object_store_manifest.json")
    assert key == "a/b/c/results/phase0q_cloud_verdict.json"


def test_run_full_loop_uses_runtime_statistics_and_never_calls_read_log(
        session_verdict, bundle_manifest, built_bundle, tmp_path):
    verdict_bytes = bt.canonical_verdict_bytes(session_verdict)
    stats = bt.build_runtime_statistics(session_verdict, verdict_bytes)
    opener = RecordingOpener({
        "files/update": [{"success": True}],
        "compile/create": [{"success": True, "compileId": "c1"}],
        "compile/read": [{"success": True, "state": "BuildSuccess", "logs": []}],
        "backtests/create": [{"success": True, "backtest": {"backtestId": "b1"}}],
        "backtests/read": [{"success": True, "backtest": {
            "backtestId": "b1", "completed": True, "runtimeStatistics": stats}}],
    })
    args = driver.parse_args([
        "--expected-manifest", str(built_bundle / "expected_results_manifest.json"),
        "--manifest-key", bundle_manifest["object_store_manifest_key"],
        "--verdict-out", str(tmp_path / "verdict.json"),
        "--report-out", str(tmp_path / "build" / "report.completed.json"),
        "--name", "phase0q_cloud_leg_test",
    ])
    summary = driver.run(args, opener=opener, creds=_creds())
    endpoints = [c["endpoint"] for c in opener.calls]
    assert "backtests/read/log" not in endpoints  # unsupported endpoint: NEVER called
    assert endpoints == ["files/update", "compile/create", "compile/read",
                         "backtests/create", "backtests/read"]
    assert summary["reproduced"] is True
    # The uploaded main.py carries BOTH baked-in pins: the manifest key AND the
    # expected sha256 of the manifest bytes at that key.
    pushed = opener.calls[0]["payload"]["content"]
    assert f'MANIFEST_KEY_INJECTED = "{bundle_manifest["object_store_manifest_key"]}"' in pushed
    expected_sha = bt.sha256_hex((built_bundle / "object_store_manifest.json").read_bytes())
    assert f'MANIFEST_SHA256_INJECTED = "{expected_sha}"' in pushed
    assert 'MANIFEST_SHA256_INJECTED = ""' not in pushed
    # The reconstructed verdict file points at the full verdict in the Object Store.
    written = json.loads((tmp_path / "verdict.json").read_text(encoding="utf-8"))
    assert written["full_verdict_sha256"] == bt.sha256_hex(verdict_bytes)
    assert written["full_verdict_object_store_key"].endswith(
        "results/phase0q_cloud_verdict.json")
    assert (tmp_path / "build" / "report.completed.json").is_file()


def test_driver_has_no_read_log_endpoint_reference():
    src = (CLOUD_PKG / "run_cloud_backtest.py").read_text(encoding="utf-8")
    assert "backtests/read/log" not in src


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


def _valid_manifest_gov():
    return {
        "bridge_scope": "qc_research_phase0q_reproducibility_only",
        "governance": {
            "A5": "blocked",
            "runtime_activation": False,
            "activation_allowed": False,
            "allocator_publish": False,
            "official_result": False,
            "db_write_mode": "none",
            "approved": False,
            "freeze_ready": False,
            "status": "candidate_not_approved",
        },
    }


def test_manifest_governance_pins_accept():
    bt.assert_manifest_governance_pins(_valid_manifest_gov())  # must not raise


@pytest.mark.parametrize("pin,bad", [
    ("A5", "unblocked"),
    ("runtime_activation", True),
    ("activation_allowed", True),
    ("allocator_publish", True),
    ("official_result", True),
    ("db_write_mode", "readwrite"),
    ("approved", True),
    ("freeze_ready", True),
    ("status", "approved"),
])
def test_manifest_governance_pins_reject_drift(pin, bad):
    manifest = _valid_manifest_gov()
    manifest["governance"][pin] = bad
    with pytest.raises(AssertionError, match=pin):
        bt.assert_manifest_governance_pins(manifest)


def test_manifest_governance_pins_reject_missing_pin():
    manifest = _valid_manifest_gov()
    del manifest["governance"]["approved"]
    with pytest.raises(AssertionError, match="approved"):
        bt.assert_manifest_governance_pins(manifest)
    manifest = _valid_manifest_gov()
    del manifest["governance"]["freeze_ready"]
    with pytest.raises(AssertionError, match="freeze_ready"):
        bt.assert_manifest_governance_pins(manifest)


def test_execute_refuses_drifted_manifest_before_any_verdict(
        built_bundle, bundle_manifest, tmp_path):
    # A regenerated/overwritten manifest with flipped pins must be refused up front —
    # even when its sha pin matches (i.e. the sha gate passed), the governance gate
    # is a second, independent refusal layer.
    store = InMemoryObjectStore(built_bundle, bundle_manifest)
    manifest_key = bundle_manifest["object_store_manifest_key"]
    drifted = json.loads(store._by_key[manifest_key].decode("utf-8"))
    drifted["governance"]["approved"] = True
    drifted_bytes = json.dumps(drifted).encode("utf-8")
    store._by_key[manifest_key] = drifted_bytes
    with pytest.raises(AssertionError, match="approved"):
        bt.execute_reproducibility_check(
            store, tmp_path / "proj", manifest_key,
            expected_manifest_sha256=bt.sha256_hex(drifted_bytes))


# --------------------------------------------------------------------------- #
# 6. Manifest BYTES pin (second injected sentinel)                             #
# --------------------------------------------------------------------------- #

def test_execute_requires_manifest_sha_pin(built_bundle, bundle_manifest, tmp_path):
    store = InMemoryObjectStore(built_bundle, bundle_manifest)
    manifest_key = bundle_manifest["object_store_manifest_key"]
    with pytest.raises(RuntimeError, match="pin"):
        bt.execute_reproducibility_check(store, tmp_path / "proj", manifest_key)
    with pytest.raises(RuntimeError, match="pin"):
        bt.execute_reproducibility_check(store, tmp_path / "proj", manifest_key,
                                         expected_manifest_sha256="")


def test_execute_refuses_manifest_sha_drift(built_bundle, bundle_manifest, tmp_path):
    # An overwritten manifest (same governance pins, different object table) must be
    # refused BEFORE it becomes the root of trust for the per-object sha checks.
    store = InMemoryObjectStore(built_bundle, bundle_manifest)
    manifest_key = bundle_manifest["object_store_manifest_key"]
    overwritten = json.loads(store._by_key[manifest_key].decode("utf-8"))
    overwritten["object_files"] = dict(list(overwritten["object_files"].items())[:1])
    store._by_key[manifest_key] = json.dumps(overwritten).encode("utf-8")
    with pytest.raises(RuntimeError, match="drift refusal"):
        bt.execute_reproducibility_check(
            store, tmp_path / "proj", manifest_key,
            expected_manifest_sha256=_bundle_manifest_sha(built_bundle))


def test_inject_manifest_sha_sentinel():
    from harness.phase0q_cloud import backtest_main as bm

    src = Path(bm.__file__).read_text(encoding="utf-8")
    sha = "a" * 64
    injected = driver.inject_manifest_sha(src, sha)
    assert f'MANIFEST_SHA256_INJECTED = "{sha}"' in injected
    assert 'MANIFEST_SHA256_INJECTED = ""' not in injected
    with pytest.raises(ValueError, match="sentinel"):
        driver.inject_manifest_sha(injected, sha)  # sentinel already consumed


def test_default_manifest_sha256_hashes_sibling_bundle_manifest(built_bundle):
    expected_path = built_bundle / "expected_results_manifest.json"
    assert driver.default_manifest_sha256(expected_path) == _bundle_manifest_sha(built_bundle)


def test_default_manifest_sha256_missing_sibling_raises(tmp_path):
    lone = tmp_path / "expected_results_manifest.json"
    lone.write_text("{}", encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="object_store_manifest.json"):
        driver.default_manifest_sha256(lone)


# --------------------------------------------------------------------------- #
# 7. Object Store save failures must propagate                                 #
# --------------------------------------------------------------------------- #

def test_save_full_verdict_propagates_refusal():
    class _RefusingStore:
        def save_bytes(self, key, data):
            return False

    manifest = {"verdict_key_template": "p/results/phase0q_cloud_verdict.json"}
    with pytest.raises(RuntimeError, match="refused"):
        bt.save_full_verdict(_RefusingStore(), manifest, b"data")


def test_save_full_verdict_returns_key_on_success():
    saved = {}

    class _AcceptingStore:
        def save_bytes(self, key, data):
            saved[key] = bytes(data)
            return True

    manifest = {"verdict_key_template": "p/results/phase0q_cloud_verdict.json"}
    key = bt.save_full_verdict(_AcceptingStore(), manifest, b"data")
    assert key == "p/results/phase0q_cloud_verdict.json"
    assert saved[key] == b"data"


def test_save_full_verdict_requires_key_template():
    class _Store:
        def save_bytes(self, key, data):
            return True

    with pytest.raises(RuntimeError, match="verdict_key_template"):
        bt.save_full_verdict(_Store(), {}, b"data")


def test_algorithm_does_not_swallow_save_failures():
    # The old handler logged a warning and continued — a refused save must FAIL the
    # run before any results key/sha is advertised.
    src = (CLOUD_PKG / "backtest_main.py").read_text(encoding="utf-8")
    assert "could not save verdict to object store" not in src


# --------------------------------------------------------------------------- #
# 8. Stale cached modules are purged before the materialized import            #
# --------------------------------------------------------------------------- #

def test_shipped_module_names_cover_closure(bundle_manifest):
    names = bt.shipped_module_names(bundle_manifest)
    for expected in ("harness", "harness.phase0q", "harness.phase0q.runner",
                     "harness.p1_pack.verifier", "src", "src.db",
                     "investintell_quant_core.hashing.canonical"):
        assert expected in names, f"missing {expected}"
    # The phase0q_cloud package itself is NOT shipped and must never be purged.
    assert not any(n.startswith("harness.phase0q_cloud") for n in names)


def test_purged_imports_use_materialized_sources(built_bundle, bundle_manifest, tmp_path):
    import sys

    import harness.phase0q.runner  # noqa: F401 - pre-import the REPO module

    store = InMemoryObjectStore(built_bundle, bundle_manifest)
    object_bytes = bt.load_bundle_from_object_store(store, bundle_manifest)
    proj = tmp_path / "proj"
    bt.materialize_sources(proj, bundle_manifest, object_bytes)
    # Modify the MATERIALIZED copy so we can tell which version executes.
    runner_path = proj / "harness" / "phase0q" / "runner.py"
    runner_path.write_text(
        runner_path.read_text(encoding="utf-8") + "\nCLOUD_COPY_MARKER = 'materialized'\n",
        encoding="utf-8")
    sys.path.insert(0, str(proj))
    try:
        bt.purge_shipped_modules(bundle_manifest)
        import harness.phase0q.runner as r2
        assert getattr(r2, "CLOUD_COPY_MARKER", None) == "materialized", (
            "the cached repo module shadowed the materialized cloud copy")
    finally:
        sys.path.remove(str(proj))
        bt.purge_shipped_modules(bundle_manifest)


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


def test_driver_injects_manifest_key_into_uploaded_main():
    """The uploaded main.py must carry the manifest key baked in: the key file
    fallback is NOT among the uploaded objects and API-created backtests have
    no project parameters set, so without injection the first cloud run fails."""
    from pathlib import Path

    from harness.phase0q_cloud import backtest_main as bm
    from harness.phase0q_cloud.run_cloud_backtest import inject_manifest_key

    src = Path(bm.__file__).read_text(encoding="utf-8")
    key = "investintell/x/y/object_store_manifest.json"
    injected = inject_manifest_key(src, key)
    assert f'MANIFEST_KEY_INJECTED = "{key}"' in injected
    assert 'MANIFEST_KEY_INJECTED = ""' not in injected


def test_default_manifest_key_reads_committed_cloud_leg_manifest():
    from harness.phase0q_cloud.run_cloud_backtest import default_manifest_key

    key = default_manifest_key()
    assert key.startswith("investintell/open_macro_v03/phase0q/")
    assert key.endswith("/object_store_manifest.json")


def test_resolve_manifest_key_default_prefers_injected(monkeypatch):
    from harness.phase0q_cloud import backtest_main as bm

    class _Store:
        def read(self, key):
            raise AssertionError("must not read the key file when injected")

    monkeypatch.setattr(bm, "MANIFEST_KEY_INJECTED", "some/prefix/object_store_manifest.json")
    assert bm.resolve_manifest_key_default(_Store()) == "some/prefix/object_store_manifest.json"


def test_reconstruct_verdict_mismatch_with_matching_leg_hash_derives_nothing():
    """If the stats say mismatch but the leg hashes coincide (e.g. a fingerprint or
    manifest-field mismatch while output hashes match), NOTHING may be derived as
    confirmed — a derived all-equal table would misrepresent the mismatch."""
    from harness.phase0q_cloud.run_cloud_backtest import reconstruct_verdict

    expected = {
        "run_fingerprint": "f" * 64,
        "output_logical_hashes": {"turnover": "a" * 64},
        "execution_legs": {"local_python_pure": {"logical_hash": "b" * 64}},
    }
    stats = {
        "phase0q_verdict": "mismatch",
        "phase0q_cloud_leg_hash": "b" * 64,  # coincide com o esperado
        "phase0q_expected_leg_hash": "b" * 64,
        "phase0q_mismatch_count": "1",
        "phase0q_verdict_sha256": "c" * 64,
    }
    verdict = reconstruct_verdict(stats, expected, "prefix/results/phase0q_cloud_verdict.json")
    assert verdict["reproduced"] is False
    assert verdict["output_logical_hashes"] == {}
    assert verdict["run_fingerprint"] is None
    assert verdict["derivation"] == "withheld_unconfirmed"


def test_object_store_save_bytes_raises_when_store_reports_failure():
    """QC's SaveBytes returns a bool; a False (quota/permission/transient failure)
    must raise instead of silently advertising the results key."""
    import pytest as _pytest

    from harness.phase0q_cloud.backtest_main import _object_store_save_bytes

    class _RefusingStore:
        def save_bytes(self, key, data):
            return False

    with _pytest.raises(RuntimeError, match="refused"):
        _object_store_save_bytes(_RefusingStore(), "k", b"data")

    class _AcceptingStore:
        def save_bytes(self, key, data):
            return True

    _object_store_save_bytes(_AcceptingStore(), "k", b"data")


def test_persist_full_verdict_best_effort_reports_refusal_without_raising():
    """QUOTA REALITY (first real cloud run): the org store was over quota and
    SaveBytes refused — failing the whole run destroyed a complete reproducibility
    proof. The save is best-effort: refusal reports saved=False (never advertised
    as stored), the essentials still prove the leg via runtime statistics."""
    from harness.phase0q_cloud.backtest_main import persist_full_verdict_best_effort

    class _RefusingStore:
        def save_bytes(self, key, data):
            return False

    manifest = {"verdict_key_template": "p/results/phase0q_cloud_verdict.json"}
    saved, key, reason = persist_full_verdict_best_effort(_RefusingStore(), manifest, b"{}")
    assert saved is False
    assert key is None
    assert "refused" in reason

    class _AcceptingStore:
        def save_bytes(self, key, data):
            return True

    saved, key, reason = persist_full_verdict_best_effort(_AcceptingStore(), manifest, b"{}")
    assert saved is True
    assert key == "p/results/phase0q_cloud_verdict.json"
    assert reason is None


def test_runtime_statistics_carry_fullverdict_saved_flag():
    from harness.phase0q_cloud.backtest_main import build_runtime_statistics

    verdict = {
        "reproduced": True,
        "execution_legs": {"qc_research_object_store": {"logical_hash": "a" * 64}},
        "comparison": {
            "execution_leg_logical_hash": {"expected_local_python_pure": "a" * 64},
            "mismatch_count": 0,
        },
    }
    stats_saved = build_runtime_statistics(verdict, b"{}", full_verdict_saved=True)
    stats_unsaved = build_runtime_statistics(verdict, b"{}", full_verdict_saved=False)
    assert stats_saved["phase0q_fullverdict_saved"] == "true"
    assert stats_unsaved["phase0q_fullverdict_saved"] == "false"


def test_reconstruct_verdict_unsaved_full_verdict_is_not_advertised():
    from harness.phase0q_cloud.run_cloud_backtest import reconstruct_verdict

    expected = {
        "run_fingerprint": "f" * 64,
        "output_logical_hashes": {"turnover": "a" * 64},
        "execution_legs": {"local_python_pure": {"logical_hash": "b" * 64}},
    }
    stats = {
        "phase0q_verdict": "reproduced",
        "phase0q_cloud_leg_hash": "b" * 64,
        "phase0q_expected_leg_hash": "b" * 64,
        "phase0q_mismatch_count": "0",
        "phase0q_verdict_sha256": "c" * 64,
        "phase0q_fullverdict_saved": "false",
    }
    verdict = reconstruct_verdict(stats, expected, "p/results/phase0q_cloud_verdict.json")
    assert verdict["reproduced"] is True
    assert verdict["full_verdict_object_store_key"] is None
    assert "not archived" in verdict["notes"]
    assert verdict["full_verdict_sha256"] == "c" * 64


def test_extract_runtime_statistics_propagates_fullverdict_saved_flag():
    """P1 regression: the extraction filter omitted the new flag, so the driver
    defaulted to true and would advertise an unstored key — the exact false
    advertising the flag exists to prevent. The flag is REQUIRED and the
    reconstruct default is fail-safe (false)."""
    from harness.phase0q_cloud.run_cloud_backtest import (
        RUNTIME_STAT_KEYS, extract_runtime_statistics, reconstruct_verdict,
    )

    assert "phase0q_fullverdict_saved" in RUNTIME_STAT_KEYS

    backtest = {"runtimeStatistics": {
        "phase0q_verdict": "reproduced",
        "phase0q_cloud_leg_hash": "b" * 64,
        "phase0q_expected_leg_hash": "b" * 64,
        "phase0q_mismatch_count": "0",
        "phase0q_verdict_sha256": "c" * 64,
        "phase0q_fullverdict_saved": "false",
    }}
    stats = extract_runtime_statistics(backtest)
    assert stats["phase0q_fullverdict_saved"] == "false"

    expected = {
        "run_fingerprint": "f" * 64,
        "output_logical_hashes": {"turnover": "a" * 64},
        "execution_legs": {"local_python_pure": {"logical_hash": "b" * 64}},
    }
    verdict = reconstruct_verdict(stats, expected, "p/results/x.json")
    assert verdict["full_verdict_object_store_key"] is None

    # fail-safe: an absent flag must never be treated as saved
    stats_absent = {k: v for k, v in stats.items() if k != "phase0q_fullverdict_saved"}
    verdict2 = reconstruct_verdict(stats_absent, expected, "p/results/x.json")
    assert verdict2["full_verdict_object_store_key"] is None


def test_persist_best_effort_treats_thrown_write_failures_as_unsaved():
    """Adapter-level exceptions (I/O, quota bridge errors) must degrade to
    saved=False like a returned False — never kill a complete proof."""
    from harness.phase0q_cloud.backtest_main import persist_full_verdict_best_effort

    class _ThrowingStore:
        def save_bytes(self, key, data):
            raise ValueError("bridge exploded")

    manifest = {"verdict_key_template": "p/results/v.json"}
    saved, key, reason = persist_full_verdict_best_effort(_ThrowingStore(), manifest, b"{}")
    assert saved is False and key is None and "bridge exploded" in reason


class _FakeClrException(BaseException):
    """Stand-in for System.Exception: NOT a subclass of builtins.Exception.

    On QC cloud ``from AlgorithmImports import *`` (which pulls in
    ``from System import *``) rebinds the module-global ``Exception`` to
    System.Exception, so a bare ``except Exception`` stops catching Python
    exceptions entirely. Proven by backtest 814c68ef: the uploaded main.py
    carried the broad catch byte-identical to the repo copy, yet the
    RuntimeError from a quota-refused save escaped and killed the run.
    """


def test_persist_best_effort_survives_clr_exception_shadowing(monkeypatch):
    """The broad catch must keep catching Python exceptions even when the
    module-global ``Exception`` is shadowed by System.Exception (QC cloud)."""
    from harness.phase0q_cloud import backtest_main

    monkeypatch.setattr(backtest_main, "Exception", _FakeClrException, raising=False)

    class _RefusingStore:
        def save_bytes(self, key, data):
            return False

    manifest = {"verdict_key_template": "p/results/v.json"}
    saved, key, reason = backtest_main.persist_full_verdict_best_effort(
        _RefusingStore(), manifest, b"{}")
    assert saved is False and key is None and "refused" in reason


def test_resolve_manifest_key_default_survives_clr_exception_shadowing(monkeypatch):
    """The key-file fallback's catch must survive the same cloud shadowing."""
    from harness.phase0q_cloud import backtest_main

    monkeypatch.setattr(backtest_main, "Exception", _FakeClrException, raising=False)

    class _MissingKeyStore:
        def read(self, key):
            raise KeyError(key)

    assert backtest_main.resolve_manifest_key_default(_MissingKeyStore()) == ""
