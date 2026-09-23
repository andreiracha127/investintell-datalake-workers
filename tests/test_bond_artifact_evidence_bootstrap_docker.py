"""Opt-in real-Linux Docker acceptance for the root-only /evidence startup bootstrap.

These tests exercise an approved candidate image against real kernel mounts,
credentials and capabilities. They require a Docker engine whose volumes live on a
Linux filesystem (Linux host or Docker Desktop's WSL2 VM, not Windows bind metadata):

    BOND_BOOTSTRAP_DOCKER_TEST=1 BOND_BOOTSTRAP_IMAGE=<candidate image id> \
        python -m pytest -q tests/test_bond_artifact_evidence_bootstrap_docker.py

Only disposable directories inside a per-run named volume are prepared and chowned;
no developer, home or production directory is mounted writable. Every container uses
``--network none`` and receives no database variables; only the tampered-wrapper build
check uses the candidate build's default network so its locked dependency layer is reused. The two opt-in variables are
read only by this harness; the production wrapper reads neither. Test harness code is
passed to disposable containers with ``python -c`` and is never copied into an image.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "docker" / "bond-implied-artifact-loader"
WRAPPER_PATH = "/app/docker/bond-implied-artifact-loader/bootstrap_evidence.py"
CONTRACT = ROOT / "contracts" / "bond_market_implied_rating_round002_artifact.json"
IMAGE = os.environ.get("BOND_BOOTSTRAP_IMAGE", "")
ENABLED = os.environ.get("BOND_BOOTSTRAP_DOCKER_TEST") == "1" and bool(IMAGE)
LOADER_ARGV = [
    "timeout", "--signal=TERM", "--kill-after=30s", "9000s", "python", "-m",
    "scripts.load_bond_market_implied_rating_artifact", "--artifact-root", "/artifact",
    "--evidence-dir", "/evidence", "--verify-only",
]
RAILWAY_ENTRYPOINT = ["--entrypoint", "/usr/local/bin/python"]
RAILWAY_PREFIX = ["-I", "-S", WRAPPER_PATH]
REAL_RUN_TIMEOUT = 1800

pytestmark = pytest.mark.skipif(
    not ENABLED,
    reason="opt-in: set BOND_BOOTSTRAP_DOCKER_TEST=1 and BOND_BOOTSTRAP_IMAGE",
)

# Imports the production wrapper inside a disposable container and runs its real
# privilege path; only the terminal execve is replaced by the selected test action.
HARNESS = r'''
import importlib.util, json, os, sys
spec = importlib.util.spec_from_file_location(
    "bootstrap_under_test", "/app/docker/bond-implied-artifact-loader/bootstrap_evidence.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
action = sys.argv[1]
loader_argv = sys.argv[2:]

def attempt(function, *args):
    try:
        function(*args)
    except OSError as exc:
        return exc.errno
    return 0

def create(path):
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as exc:
        return exc.errno
    os.close(fd)
    os.unlink(path)
    return 0

def read(path):
    with open(path, "rb") as handle:
        handle.read()

CHILD = """
import json, os
uids = {}
for name in os.listdir('/proc'):
    if name.isdigit():
        try:
            with open('/proc/' + name + '/status') as handle:
                for line in handle:
                    if line.startswith(('Uid:', 'Gid:')):
                        uids.setdefault(name, []).append(line.split()[1:])
        except OSError:
            pass
print(json.dumps({'processes': uids}, sort_keys=True), flush=True)
raise SystemExit(17)
"""

class ProbeOps(module.SystemOps):
    def execve(self, path, argv, env):
        if action == "probe":
            status = {}
            with open("/proc/self/status") as handle:
                for line in handle:
                    key, _, value = line.partition(":")
                    if key in ("Uid", "Gid", "Groups", "CapInh", "CapPrm", "CapEff",
                               "CapAmb", "CapBnd", "NoNewPrivs"):
                        status[key] = value.split()
            evidence = os.stat("/evidence")
            report = {
                "exec_path": path, "argv": argv, "env_path": env.get("PATH"),
                "env_has_database": any("DATABASE" in key or "DSN" in key for key in env),
                "resuid": list(os.getresuid()), "resgid": list(os.getresgid()),
                "groups": os.getgroups(), "status": status,
                "regain": {
                    "setresuid": attempt(os.setresuid, 0, 0, 0),
                    "setuid": attempt(os.setuid, 0),
                    "seteuid": attempt(os.seteuid, 0),
                    "setresgid": attempt(os.setresgid, 0, 0, 0),
                    "setgid": attempt(os.setgid, 0),
                    "setgroups": attempt(os.setgroups, [0]),
                },
                "writes": {
                    target: create(target) for target in (
                        "/app/probe-write", "/artifact/probe-write",
                        "/root-sentinel/probe-write", "/evidence/probe-write")
                },
                "sentinel_read": attempt(read, "/root-sentinel/secret"),
                "evidence": [evidence.st_uid, evidence.st_gid, evidence.st_mode & 0o7777],
            }
            print(json.dumps(report, sort_keys=True), flush=True)
            os._exit(0)
        if action == "exit17":
            test_argv = ["timeout", "--signal=TERM", "--kill-after=5s", "120s",
                         "python", "-I", "-S", "-c", CHILD]
        elif action == "timeout124":
            test_argv = ["timeout", "--signal=TERM", "--kill-after=5s", "2s",
                         "python", "-I", "-S", "-c", "import time; time.sleep(60)"]
        else:
            raise SystemExit(99)
        os.execve(path, test_argv, env)

raise SystemExit(module.run(loader_argv, ProbeOps()))
'''


def _docker(*args: str, timeout: int = 300, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, encoding="utf-8",
        timeout=timeout, input=stdin, check=False,
    )


def _checked(*args: str, timeout: int = 300, stdin: str | None = None) -> str:
    result = _docker(*args, timeout=timeout, stdin=stdin)
    assert result.returncode == 0, (args, result.stdout[-2000:], result.stderr[-2000:])
    return result.stdout


class Scratch:
    """One disposable named volume holding every per-case directory for this run."""

    def __init__(self) -> None:
        self.suffix = uuid.uuid4().hex[:12]
        self.volume = f"bond-bootstrap-test-{self.suffix}"
        _checked("volume", "create", "--label", "purpose=bond-bootstrap-test", self.volume)
        mountpoint = _checked(
            "volume", "inspect", "--format", "{{.Mountpoint}}", self.volume
        ).strip()
        assert mountpoint.startswith("/") and mountpoint.endswith(f"/{self.volume}/_data")
        self.mountpoint = mountpoint

    def shell(self, script: str, *extra: str) -> str:
        return _checked(
            "run", "--rm", "--network", "none", "--user", "0:0", "--entrypoint", "/bin/sh",
            "-v", f"{self.volume}:/work", *extra, IMAGE, "-ec", script,
        )

    def case(self, name: str, owner: str = "0:0", mode: str = "0755") -> str:
        assert re.fullmatch(r"[a-z0-9-]+", name)
        self.shell(f"mkdir /work/{name} && chown {owner} /work/{name} && chmod {mode} /work/{name}")
        return name

    def source(self, name: str) -> str:
        return f"{self.mountpoint}/{name}"

    def mount(self, name: str, target: str = "/evidence", readonly: bool = False) -> list[str]:
        spec = f"type=bind,src={self.source(name)},dst={target}"
        return ["--mount", spec + (",readonly" if readonly else "")]

    def facts(self, name: str) -> str:
        return self.shell(f"stat -c '%u:%g:%a:%F' /work/{name}").strip()

    def listing(self, name: str) -> list[str]:
        output = self.shell(f"find /work/{name} -mindepth 1 -printf '%P|%U:%G|%m|%y|%l\\n'")
        return sorted(line for line in output.splitlines() if line)

    def read(self, path: str) -> str:
        return self.shell(f"cat /work/{path}")

    def close(self) -> None:
        _docker("volume", "rm", "-f", self.volume)


@pytest.fixture(scope="module")
def scratch() -> Iterator[Scratch]:
    workspace = Scratch()
    try:
        yield workspace
    finally:
        workspace.close()


@pytest.fixture(scope="module")
def image_id() -> str:
    return _checked("image", "inspect", "--format", "{{.Id}}", IMAGE).strip()


def _markers(stderr: str) -> list[dict[str, Any]]:
    markers = []
    for line in stderr.splitlines():
        if line.startswith("{") and '"schema":"bond_artifact_bootstrap/1"' in line:
            markers.append(json.loads(line))
    return markers


def _default_run(*args: str, timeout: int = REAL_RUN_TIMEOUT) -> subprocess.CompletedProcess[str]:
    return _docker("run", "--rm", "--network", "none", *args, IMAGE, timeout=timeout)


def _railway_run(
    *args: str, argv: list[str] = LOADER_ARGV, timeout: int = REAL_RUN_TIMEOUT
) -> subprocess.CompletedProcess[str]:
    return _docker(
        "run", "--rm", "--network", "none", *RAILWAY_ENTRYPOINT, *args, IMAGE,
        *RAILWAY_PREFIX, *argv, timeout=timeout,
    )


def _harness(
    action: str, *args: str, timeout: int = 300
) -> subprocess.CompletedProcess[str]:
    return _docker(
        "run", "--rm", "--network", "none", *RAILWAY_ENTRYPOINT, *args, IMAGE,
        "-I", "-S", "-c", HARNESS, action, *LOADER_ARGV, timeout=timeout,
    )


def _handoff_marker(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    markers = _markers(result.stderr)
    assert [marker["phase"] for marker in markers] == ["privileges_dropped"], result.stderr
    marker = markers[0]
    assert marker["wrapper_sha256"] == hashlib.sha256(
        (RUNTIME / "bootstrap_evidence.py").read_bytes()
    ).hexdigest()
    assert marker["uid"] == [65532] * 3 and marker["gid"] == [65532] * 3
    assert marker["groups"] == []
    assert marker["no_new_privs"] == 1
    for key in ("inheritable", "permitted", "effective", "ambient"):
        assert marker["capabilities"][key] == "0" * 16
    assert marker["exec"] == "/usr/bin/timeout"
    return marker


def _assert_refused(result: subprocess.CompletedProcess[str], reason: str) -> None:
    assert result.returncode == 78, (result.returncode, result.stdout, result.stderr)
    markers = _markers(result.stderr)
    assert markers == [
        {"schema": "bond_artifact_bootstrap/1", "phase": "bootstrap_failure", "reason": reason}
    ], result.stderr
    assert result.stdout == ""
    assert "verified_offline" not in result.stderr


def _assert_verified_offline(
    result: subprocess.CompletedProcess[str], scratch: Scratch, case: str, image_id: str,
    *, prior_receipts: int = 0,
) -> dict[str, Any]:
    assert result.returncode == 0, (result.stdout[-2000:], result.stderr[-4000:])
    marker = _handoff_marker(result)
    output = json.loads(result.stdout.strip().splitlines()[-1])
    contract = json.loads(CONTRACT.read_bytes())
    assert output["outcome"] == "verified_offline"
    assert output["publication_id"] == contract["identity"]["publication_id"]
    assert output["schema_installed"] is False and output["stored"] is None
    (receipt_ref,) = output["receipts"]
    assert receipt_ref["phase"] == "verify-only"
    receipt_text = scratch.read(f"{case}/{receipt_ref['basename']}")
    receipt_bytes = receipt_text.encode("ascii")
    assert hashlib.sha256(receipt_bytes).hexdigest() == receipt_ref["sha256"]
    assert len(receipt_bytes) == receipt_ref["size_bytes"]
    receipt = json.loads(receipt_bytes)
    assert receipt["outcome"] == "verified_offline"
    assert receipt["publication_id"] == contract["identity"]["publication_id"]
    assert receipt["input_fingerprint"] == contract["identity"]["input_fingerprint"]
    assert receipt["contract_sha256"] == hashlib.sha256(CONTRACT.read_bytes()).hexdigest()
    assert receipt["artifact_sha256"] == contract["artifact"]["sha256"]
    assert receipt["row_count"] == contract["expected"]["row_count"]
    assert receipt["rows_digest"] == contract["expected"]["rows_digest"]
    keys = ("label", "relative_path", "size_bytes", "sha256")
    expected_files = [
        {key: pin[key] for key in keys}
        for pin in (contract["artifact"], *contract["manifests"], contract["identity"]["receipt"])
    ]
    for source in contract["producer"]["sources"]:
        source_bytes = (ROOT / source["relative_path"]).read_bytes()
        digest = hashlib.sha256(source_bytes).hexdigest()
        assert digest in source["accepted_runtime_sha256"]
        expected_files.append({
            "label": "source", "relative_path": source["relative_path"],
            "size_bytes": len(source_bytes), "sha256": digest,
        })

    def order(item: dict[str, Any]) -> tuple[str, str]:
        return item["label"], item["relative_path"]

    assert sorted(receipt["files"], key=order) == sorted(expected_files, key=order)
    assert receipt["release"] is None and receipt["parent"] is None
    assert scratch.facts(case) == "65532:65532:700:directory"
    entries = scratch.listing(case)
    receipts = [entry for entry in entries if "-verify-only-" in entry]
    assert len(receipts) == prior_receipts + 1
    assert f"{receipt_ref['basename']}|65532:65532|600|f|" in entries
    assert not [entry for entry in entries if entry.startswith(".bootstrap-probe-")]
    print("BOND_BOOTSTRAP_EVIDENCE " + json.dumps({
        "case": case, "image_id": image_id, "marker": marker,
        "receipt": receipt_ref, "artifact_sha256": receipt["artifact_sha256"],
        "rows_digest": receipt["rows_digest"], "contract_sha256": receipt["contract_sha256"],
        "elapsed_seconds": receipt["elapsed_seconds"],
    }, sort_keys=True))
    return marker


# -- 1/2: real frozen artifact through both invocation paths -------------------------------


def test_default_entrypoint_and_cmd_verify_real_artifact_on_root_owned_bind_mount(
    scratch: Scratch, image_id: str
) -> None:
    case = scratch.case("real-default")
    assert scratch.facts(case) == "0:0:755:directory"
    inspect = json.loads(_checked("image", "inspect", IMAGE))[0]["Config"]
    assert inspect["User"] == "0:0"
    assert inspect["Entrypoint"] == ["/usr/local/bin/python", *RAILWAY_PREFIX]
    assert inspect["Cmd"] == LOADER_ARGV
    assert not [item for item in inspect.get("Env") or [] if "DATABASE" in item]
    marker = _assert_verified_offline(
        _default_run(*scratch.mount(case)), scratch, case, image_id
    )
    assert marker["evidence"]["initial_uid"] == 0
    assert marker["evidence"]["initial_mode"] == "0755"
    assert marker["evidence"]["chown_applied"] is True


def test_railway_start_command_override_verifies_real_artifact(
    scratch: Scratch, image_id: str
) -> None:
    case = scratch.case("real-railway")
    marker = _assert_verified_offline(
        _railway_run(*scratch.mount(case)), scratch, case, image_id
    )
    assert marker["evidence"]["chmod_applied"] is True
    assert marker["evidence"]["chown_applied"] is True


# -- 3/6: actual credentials, capabilities and exec contract ------------------------------


def _probe_case(scratch: Scratch, name: str) -> list[str]:
    case = scratch.case(name)
    sentinel = scratch.case(f"{name}-sentinel", mode="0700")
    scratch.shell(
        f"printf secret > /work/{sentinel}/secret && chmod 0600 /work/{sentinel}/secret"
    )
    return [*scratch.mount(case), *scratch.mount(sentinel, "/root-sentinel")]


@pytest.mark.parametrize("groups", [[], ["0", "4", "4242"]])
def test_privilege_drop_is_complete_and_irreversible(
    scratch: Scratch, groups: list[str]
) -> None:
    name = "probe-groups" if groups else "probe"
    mounts = _probe_case(scratch, name)
    group_args = [arg for group in groups for arg in ("--group-add", group)]
    result = _harness("probe", *group_args, *mounts)
    assert result.returncode == 0, result.stderr
    _handoff_marker(result)
    report = json.loads(result.stdout.strip().splitlines()[-1])
    assert report["exec_path"] == "/usr/bin/timeout"
    assert report["argv"] == LOADER_ARGV
    assert report["env_path"] == "/usr/local/bin:/usr/bin:/bin"
    assert report["env_has_database"] is False
    assert report["resuid"] == [65532] * 3 and report["resgid"] == [65532] * 3
    assert report["groups"] == []
    status = report["status"]
    assert status["Uid"] == ["65532"] * 4 and status["Gid"] == ["65532"] * 4
    assert status["Groups"] == []
    assert status["NoNewPrivs"] == ["1"]
    for key in ("CapInh", "CapPrm", "CapEff", "CapAmb"):
        assert status[key] == ["0" * 16]
    assert set(report["regain"].values()) == {1}  # EPERM for every root regain attempt
    assert report["writes"] == {
        "/app/probe-write": 13, "/artifact/probe-write": 13,
        "/root-sentinel/probe-write": 13, "/evidence/probe-write": 0,
    }
    assert report["sentinel_read"] == 13
    assert report["evidence"] == [65532, 65532, 0o700]
    assert scratch.listing(name) == []
    assert scratch.listing(f"{name}-sentinel") == ["secret|0:0|600|f|"]


def test_supervisor_propagates_child_exit_code_with_no_privileged_process(
    scratch: Scratch,
) -> None:
    case = scratch.case("exec-exit")
    result = _harness("exit17", *scratch.mount(case))
    assert result.returncode == 17, result.stderr
    _handoff_marker(result)
    processes = json.loads(result.stdout.strip().splitlines()[-1])["processes"]
    assert "1" in processes and len(processes) >= 2
    for credentials in processes.values():
        assert credentials == [["65532"] * 4, ["65532"] * 4]


def test_supervisor_timeout_contract_is_unchanged_after_drop(scratch: Scratch) -> None:
    case = scratch.case("exec-timeout")
    result = _harness("timeout124", *scratch.mount(case))
    assert result.returncode == 124, result.stderr
    _handoff_marker(result)


# -- 4: no recursion, idempotent rerun ---------------------------------------------------


def test_only_the_mount_root_changes_and_second_startup_is_idempotent(
    scratch: Scratch, image_id: str
) -> None:
    case = scratch.case("children")
    outside = scratch.case("children-outside", mode="0755")
    scratch.shell(
        f"printf sentinel > /work/{outside}/sentinel && chmod 0644 /work/{outside}/sentinel"
        f" && printf child > /work/{case}/child.txt && chown 1234:1234 /work/{case}/child.txt"
        f" && chmod 0640 /work/{case}/child.txt && mkdir /work/{case}/subdir"
        f" && chmod 0750 /work/{case}/subdir && printf inner > /work/{case}/subdir/inner.txt"
        f" && chmod 0600 /work/{case}/subdir/inner.txt"
        f" && ln -s /outside/sentinel /work/{case}/link"
    )
    children_before = scratch.listing(case)
    outside_before = scratch.listing(outside)
    content_before = scratch.shell(
        f"sha256sum /work/{case}/child.txt /work/{case}/subdir/inner.txt /work/{outside}/sentinel"
    )
    outside_mount = scratch.mount(outside, "/outside")
    first = _harness("probe", *scratch.mount(case), *outside_mount,
                     *scratch.mount(scratch.case("children-sentinel", mode="0700"),
                                    "/root-sentinel"))
    assert first.returncode == 0, first.stderr
    assert _handoff_marker(first)["evidence"]["chown_applied"] is True
    assert scratch.facts(case) == "65532:65532:700:directory"
    assert scratch.listing(case) == children_before
    assert scratch.listing(outside) == outside_before
    assert children_before == sorted([
        "child.txt|1234:1234|640|f|", "link|0:0|777|l|/outside/sentinel",
        "subdir|0:0|750|d|", "subdir/inner.txt|0:0|600|f|",
    ])
    second = _default_run(*scratch.mount(case), *outside_mount)
    marker = _assert_verified_offline(second, scratch, case, image_id)
    assert marker["evidence"] == {
        "path": "/evidence", "mount_id": marker["evidence"]["mount_id"],
        "initial_uid": 65532, "initial_gid": 65532, "initial_mode": "0700",
        "uid": 65532, "gid": 65532, "mode": "0700",
        "chmod_applied": False, "chown_applied": False,
    }
    after = [entry for entry in scratch.listing(case) if "-verify-only-" not in entry]
    assert after == children_before
    assert scratch.listing(outside) == outside_before
    assert scratch.shell(
        f"sha256sum /work/{case}/child.txt /work/{case}/subdir/inner.txt /work/{outside}/sentinel"
    ) == content_before


# -- 5: negative matrix -------------------------------------------------------------------


def test_unmounted_image_directory_is_refused() -> None:
    _assert_refused(_default_run(timeout=300), "evidence_not_mount")


@pytest.fixture(scope="module")
def image_without_evidence_dir(scratch: Scratch) -> Iterator[str]:
    base = f"bond-bootstrap-test-base:{scratch.suffix}"
    derived = f"bond-bootstrap-test-noevidence:{scratch.suffix}"
    _checked("tag", IMAGE, base)
    try:
        _checked("build", "--network", "none", "-t", derived, "-",
                 stdin=f"FROM {base}\nRUN rmdir /evidence\n", timeout=600)
        yield derived
    finally:
        _docker("image", "rm", "-f", derived)
        _docker("image", "rm", base)


def test_regular_file_bind_mounted_at_evidence_is_refused(
    scratch: Scratch, image_without_evidence_dir: str
) -> None:
    scratch.shell("printf data > /work/regular-file && chmod 0644 /work/regular-file")
    before = scratch.shell("stat -c '%u:%g:%a:%F:%s' /work/regular-file")
    result = _docker(
        "run", "--rm", "--network", "none", "--mount",
        f"type=bind,src={scratch.source('regular-file')},dst=/evidence",
        image_without_evidence_dir,
    )
    _assert_refused(result, "evidence_not_directory")
    assert scratch.shell("stat -c '%u:%g:%a:%F:%s' /work/regular-file") == before
    assert scratch.read("regular-file") == "data"


def test_evidence_symlink_to_a_mounted_directory_is_refused(scratch: Scratch) -> None:
    case = scratch.case("symlink-target")
    command = (
        "rm -rf /evidence && ln -s /target /evidence && exec /usr/local/bin/python -I -S "
        + WRAPPER_PATH + " " + " ".join(LOADER_ARGV)
    )
    result = _docker(
        "run", "--rm", "--network", "none", "--entrypoint", "/bin/sh",
        *scratch.mount(case, "/target"), IMAGE, "-c", command,
    )
    _assert_refused(result, "evidence_not_directory")
    assert scratch.facts(case) == "0:0:755:directory"
    assert scratch.listing(case) == []


@pytest.mark.parametrize(
    ("name", "owner", "mode", "reason"),
    [
        ("foreign", "1000:1000", "0755", "evidence_owner_refused"),
        ("mixed-root-group", "65532:0", "0700", "evidence_owner_refused"),
        ("mixed-app-group", "0:65532", "0755", "evidence_owner_refused"),
        ("group-writable", "0:0", "0775", "evidence_mode_refused"),
        ("world-writable", "0:0", "0777", "evidence_mode_refused"),
        ("sticky", "0:0", "1755", "evidence_mode_refused"),
        ("setgid", "0:0", "2755", "evidence_mode_refused"),
        ("setuid", "0:0", "4755", "evidence_mode_refused"),
    ],
)
def test_unsafe_owner_or_mode_is_refused_unchanged(
    scratch: Scratch, name: str, owner: str, mode: str, reason: str
) -> None:
    case = scratch.case(name, owner, mode)
    before = scratch.facts(case)
    _assert_refused(_default_run(*scratch.mount(case), timeout=300), reason)
    assert scratch.facts(case) == before
    assert scratch.listing(case) == []


def test_read_only_mount_is_refused_unchanged(scratch: Scratch) -> None:
    case = scratch.case("read-only")
    result = _default_run(*scratch.mount(case, readonly=True), timeout=300)
    _assert_refused(result, "evidence_mount_read_only")
    assert scratch.facts(case) == "0:0:755:directory"


def test_unprivileged_startup_on_uninitialized_mount_is_refused(scratch: Scratch) -> None:
    case = scratch.case("unprivileged")
    result = _default_run("--user", "65532:65532", *scratch.mount(case), timeout=300)
    _assert_refused(result, "not_root")
    assert scratch.facts(case) == "0:0:755:directory"


@pytest.mark.parametrize("capability", ["CHOWN", "SETUID", "SETGID"])
def test_missing_capability_is_refused_before_mutation(
    scratch: Scratch, capability: str
) -> None:
    case = scratch.case(f"no-{capability.lower()}")
    result = _default_run("--cap-drop", capability, *scratch.mount(case), timeout=300)
    _assert_refused(result, "capability_missing")
    assert scratch.facts(case) == "0:0:755:directory"
    assert scratch.listing(case) == []


@pytest.mark.parametrize(
    ("entrypoint", "arguments", "reason"),
    [
        (None, [*LOADER_ARGV, "--apply"], "argv_refused"),
        (None, [*LOADER_ARGV[:-1], "--apply", "--verify-only"], "argv_refused"),
        (None, ["/bin/sh", "-c", " ".join(LOADER_ARGV)], "argv_refused"),
        (None, [*LOADER_ARGV[:3], "9001s", *LOADER_ARGV[4:]], "argv_refused"),
        (None, [*LOADER_ARGV[:10], "/tmp", "--verify-only"], "argv_refused"),
        (RAILWAY_ENTRYPOINT, [*RAILWAY_PREFIX, *LOADER_ARGV[:-1]], "argv_refused"),
        (RAILWAY_ENTRYPOINT, [WRAPPER_PATH, *LOADER_ARGV], "interpreter_refused"),
        (RAILWAY_ENTRYPOINT, ["-I", WRAPPER_PATH, *LOADER_ARGV], "interpreter_refused"),
    ],
)
def test_argv_or_interpreter_substitution_is_refused_unchanged(
    scratch: Scratch, entrypoint: list[str] | None, arguments: list[str], reason: str
) -> None:
    case = scratch.case(f"argv-{uuid.uuid4().hex[:8]}")
    result = _docker(
        "run", "--rm", "--network", "none", *(entrypoint or []), *scratch.mount(case),
        IMAGE, *arguments, timeout=300,
    )
    _assert_refused(result, reason)
    assert scratch.facts(case) == "0:0:755:directory"
    assert scratch.listing(case) == []


# -- 7: tampered wrapper cannot be built --------------------------------------------------


def _copy_sources(dockerfile: str) -> list[str]:
    joined = re.sub(r"\\\n", " ", dockerfile)
    sources: list[str] = []
    for line in joined.splitlines():
        tokens = line.split()
        if tokens[:1] == ["COPY"]:
            sources.extend(token for token in tokens[1:-1] if not token.startswith("--"))
    return sources


def test_tampered_wrapper_fails_the_literal_checksum_build_step(tmp_path: Path) -> None:
    dockerfile = (RUNTIME / "Dockerfile").read_text(encoding="utf-8")
    sources = _copy_sources(dockerfile)
    assert "docker/bond-implied-artifact-loader/bootstrap_evidence.py" in sources
    context = tmp_path / "context"
    for relative in sources:
        if relative == "artifact/":
            continue
        target = context / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
        assert b"\r\n" not in target.read_bytes()
    # BuildKit resolves every COPY source before executing; a placeholder artifact/ keeps
    # the context well-formed so the build can only stop at the wrapper checksum step.
    (context / "artifact").mkdir()
    (context / "artifact" / "placeholder").write_bytes(b"not the frozen artifact\n")
    wrapper = context / "docker" / "bond-implied-artifact-loader" / "bootstrap_evidence.py"
    wrapper.write_bytes(wrapper.read_bytes() + b"# tampered\n")
    # Same build options as the candidate build: BuildKit keys RUN layers by network mode,
    # so the hash-locked dependency layer is reused and the tampered image is never tagged.
    result = _docker(
        "build", "--progress", "plain",
        "-f", str(context / "docker" / "bond-implied-artifact-loader" / "Dockerfile"),
        str(context), timeout=1200,
    )
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "bootstrap_evidence.py: FAILED" in output, output[-3000:]
    assert "computed checksum did NOT match" in output
    assert not re.search(r"\[\s*\d+/\d+\] COPY artifact/", output)
