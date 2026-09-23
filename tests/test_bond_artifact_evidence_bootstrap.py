"""Unit and static tests for the root-only /evidence startup bootstrap.

Every operating-system boundary is replaced by an in-memory fake, so these tests never
change a host UID, mount or permission. Real-kernel behavior is covered by the opt-in
Linux Docker acceptance suite in ``test_bond_artifact_evidence_bootstrap_docker.py``.
"""
from __future__ import annotations

import ast
import errno
import hashlib
import importlib.util
import json
import re
import shlex
import stat
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import tomllib

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "docker" / "bond-implied-artifact-loader"
WRAPPER = RUNTIME / "bootstrap_evidence.py"
DIR_FD = 5
PROBE_FD = 6
EVIDENCE_MOUNT_ID = 812
FULL_CAPS = "000001ffffffffff"
ZERO_CAPS = "0000000000000000"
LOADER_ARGV = [
    "timeout", "--signal=TERM", "--kill-after=30s", "9000s", "python", "-m",
    "scripts.load_bond_market_implied_rating_artifact", "--artifact-root", "/artifact",
    "--evidence-dir", "/evidence", "--verify-only",
]
# Reviewed runtime hashes at 52dbe8892e8904001f4b5b4689d1fb84b938219e (pre-bootstrap).
OLD_DOCKERFILE_SHA256 = "a3bdee376e2f78c0deb4ebf07c617d0d3edbb5cd5746db45bcf0cac7885058fc"
OLD_RAILWAY_TOML_SHA256 = "6d6083e213b733e9f1e3649fdc661ec161210964c62ac533645358cb044fe040"


def _load_wrapper() -> ModuleType:
    previous = sys.dont_write_bytecode
    spec = importlib.util.spec_from_file_location("bond_artifact_bootstrap_evidence", WRAPPER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


boot = _load_wrapper()


class ExecCalled(BaseException):
    """Raised by the fake execve so the terminal handoff is observable."""


@dataclass
class FakeStat:
    st_mode: int
    st_uid: int
    st_gid: int
    st_dev: int = 41
    st_ino: int = 7
    st_nlink: int = 2


@dataclass
class FakeStatvfs:
    f_flag: int


def _mountinfo(*extra: str, evidence: str | None = None) -> str:
    lines = [
        "700 690 0:61 / / rw,relatime master:1 - overlay overlay rw,lowerdir=/l",
        "701 700 0:64 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw",
        "702 700 0:65 / /artifact ro,relatime - overlay overlay ro",
    ]
    if evidence is None:
        evidence = (
            f"{EVIDENCE_MOUNT_ID} 700 8:48 /var/lib/docker/volumes/v/_data/case /evidence "
            "rw,relatime - ext4 /dev/sdd rw,discard,errors=remount-ro"
        )
    if evidence:
        lines.append(evidence)
    lines.extend(extra)
    return "\n".join(lines) + "\n"


class FakeOps(boot.SystemOps):
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fail: dict[str, BaseException] = {}
        self.uid = [0, 0, 0]
        self.gid = [0, 0, 0]
        self.groups = [0, 10]
        self.platform_name = "linux"
        self.root = FakeStat(stat.S_IFDIR | 0o755, 0, 0, st_dev=1, st_ino=2)
        self.evidence = FakeStat(stat.S_IFDIR | 0o755, 0, 0)
        self.path_override: FakeStat | None = None
        self.path_missing = False
        self.mountinfo = _mountinfo()
        self.fdinfo = f"pos:\t0\nflags:\t02304000\nmnt_id:\t{EVIDENCE_MOUNT_ID}\nino:\t7\n"
        self.statvfs_flag = 0
        self.cap_eff = FULL_CAPS
        self.dropped_caps = {
            "CapInh": ZERO_CAPS, "CapPrm": ZERO_CAPS, "CapEff": ZERO_CAPS,
            "CapAmb": ZERO_CAPS, "CapBnd": FULL_CAPS,
        }
        self.dropped_uid_line: str | None = None
        self.no_new_privs = 0
        self.no_new_privs_readback: int | None = None
        self.chown_is_noop = False
        self.probe_files: dict[str, bytearray] = {}
        self.probe_short_write = False
        self.wrapper_bytes = b"reviewed wrapper bytes\n"
        self.env = {"PATH": "/tmp/evil:/usr/bin", "DATABASE_URL": "postgresql://u:secret@h/db"}
        self.stderr = bytearray()
        self.umask_value: int | None = None

    # -- recording helpers -------------------------------------------------
    def _record(self, name: str, *args: Any) -> None:
        self.calls.append((name, args))
        failure = self.fail.get(name)
        if failure is not None:
            raise failure

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]

    @property
    def dropped(self) -> bool:
        return self.uid[1] != 0

    def _status(self) -> str:
        if self.dropped:
            caps = self.dropped_caps
            uid_line = self.dropped_uid_line or "\t".join([str(self.uid[0])] * 4)
        else:
            caps = {
                "CapInh": ZERO_CAPS, "CapPrm": self.cap_eff, "CapEff": self.cap_eff,
                "CapAmb": ZERO_CAPS, "CapBnd": FULL_CAPS,
            }
            uid_line = "\t".join(["0"] * 4)
        gid_line = "\t".join([str(self.gid[0])] * 4)
        groups = "".join(f"{group} " for group in self.groups)
        return (
            "Name:\tpython\nUmask:\t0022\nState:\tR (running)\n"
            f"Uid:\t{uid_line}\nGid:\t{gid_line}\nGroups:\t{groups}\n"
            f"NoNewPrivs:\t{self.no_new_privs}\nSeccomp:\t2\n"
            f"CapInh:\t{caps['CapInh']}\nCapPrm:\t{caps['CapPrm']}\n"
            f"CapEff:\t{caps['CapEff']}\nCapBnd:\t{caps['CapBnd']}\n"
            f"CapAmb:\t{caps['CapAmb']}\n"
        )

    # -- SystemOps boundary ------------------------------------------------
    def platform(self) -> str:
        self._record("platform")
        return self.platform_name

    def getresuid(self) -> tuple[int, int, int]:
        self._record("getresuid")
        return tuple(self.uid)  # type: ignore[return-value]

    def getresgid(self) -> tuple[int, int, int]:
        self._record("getresgid")
        return tuple(self.gid)  # type: ignore[return-value]

    def getgroups(self) -> list[int]:
        self._record("getgroups")
        return list(self.groups)

    def lstat(self, path: str) -> Any:
        self._record("lstat", path)
        if path == "/":
            return self.root
        assert path == "/evidence"
        if self.path_missing:
            raise FileNotFoundError(errno.ENOENT, "missing")
        return self.path_override or self.evidence

    def open_directory(self, path: str) -> int:
        self._record("open_directory", path)
        assert path == "/evidence"
        if not stat.S_ISDIR((self.path_override or self.evidence).st_mode):
            raise OSError(errno.ELOOP, "not followed")
        return DIR_FD

    def fstat(self, fd: int) -> Any:
        self._record("fstat", fd)
        if fd == DIR_FD:
            return replace(self.evidence)
        assert fd == PROBE_FD
        return FakeStat(stat.S_IFREG | 0o600, self.uid[1], self.gid[1], st_nlink=1)

    def fstatvfs(self, fd: int) -> Any:
        self._record("fstatvfs", fd)
        assert fd == DIR_FD
        return FakeStatvfs(self.statvfs_flag)

    def close(self, fd: int) -> None:
        self._record("close", fd)

    def read_file(self, path: str, limit: int) -> bytes:
        self._record("read_file", path)
        if path == boot.WRAPPER_PATH:
            return self.wrapper_bytes
        if path == "/proc/self/status":
            return self._status().encode()
        if path == "/proc/self/mountinfo":
            return self.mountinfo.encode()[: limit + 1]
        if path == f"/proc/self/fdinfo/{DIR_FD}":
            if self.dropped:  # the kernel makes fdinfo root-owned after set-ID changes
                raise PermissionError(errno.EACCES, "denied")
            return self.fdinfo.encode()
        raise AssertionError(path)

    def fchmod(self, fd: int, mode: int) -> None:
        self._record("fchmod", fd, mode)
        assert fd == DIR_FD
        self.evidence.st_mode = stat.S_IFDIR | mode

    def fchown(self, fd: int, uid: int, gid: int) -> None:
        self._record("fchown", fd, uid, gid)
        assert fd == DIR_FD
        if not self.chown_is_noop:
            self.evidence.st_uid, self.evidence.st_gid = uid, gid

    def umask(self, mask: int) -> int:
        self._record("umask", mask)
        self.umask_value = mask
        return 0o022

    def prctl(self, option: int, arg2: int) -> int:
        self._record("prctl", option, arg2)
        if option == boot.PR_SET_NO_NEW_PRIVS:
            self.no_new_privs = 1
            return 0
        assert option == boot.PR_GET_NO_NEW_PRIVS
        if self.no_new_privs_readback is not None:
            return self.no_new_privs_readback
        return self.no_new_privs

    def setgroups(self, groups: list[int]) -> None:
        self._record("setgroups", tuple(groups))
        if self.dropped:
            raise PermissionError(errno.EPERM, "no")
        self.groups = list(groups)

    def setresgid(self, rgid: int, egid: int, sgid: int) -> None:
        self._record("setresgid", rgid, egid, sgid)
        if self.dropped:
            raise PermissionError(errno.EPERM, "no")
        self.gid = [rgid, egid, sgid]

    def setresuid(self, ruid: int, euid: int, suid: int) -> None:
        self._record("setresuid", ruid, euid, suid)
        if self.dropped:
            raise PermissionError(errno.EPERM, "no")
        self.uid = [ruid, euid, suid]

    def urandom(self, size: int) -> bytes:
        self._record("urandom", size)
        return bytes(range(size))

    def open_probe(self, name: str, dir_fd: int) -> int:
        self._record("open_probe", name, dir_fd)
        assert dir_fd == DIR_FD and self.dropped
        if name in self.probe_files:
            raise FileExistsError(errno.EEXIST, "exists")
        self.probe_files[name] = bytearray()
        return PROBE_FD

    def write(self, fd: int, data: bytes) -> int:
        self._record("write", fd)
        assert fd == PROBE_FD
        written = len(data) - 1 if self.probe_short_write else len(data)
        next(iter(self.probe_files.values())).extend(data[:written])
        return written

    def fsync(self, fd: int) -> None:
        self._record("fsync", fd)

    def unlink(self, name: str, dir_fd: int) -> None:
        self._record("unlink", name, dir_fd)
        assert dir_fd == DIR_FD
        del self.probe_files[name]

    def environ(self) -> dict[str, str]:
        self._record("environ")
        return dict(self.env)

    def write_stderr(self, data: bytes) -> None:
        self.calls.append(("write_stderr", ()))
        self.stderr.extend(data)

    def execve(self, path: str, argv: list[str], env: dict[str, str]) -> None:
        self._record("execve", path, tuple(argv), tuple(sorted(env.items())))
        raise ExecCalled()


def _markers(ops: FakeOps) -> list[dict[str, Any]]:
    return [json.loads(line) for line in ops.stderr.decode("ascii").splitlines()]


def _refused(ops: FakeOps, argv: list[str] | None = None) -> str:
    assert boot.run(list(LOADER_ARGV if argv is None else argv), ops) == 78
    markers = _markers(ops)
    assert len(markers) == 1
    assert markers[0]["schema"] == "bond_artifact_bootstrap/1"
    assert markers[0]["phase"] == "bootstrap_failure"
    assert set(markers[0]) == {"schema", "phase", "reason"}
    assert "execve" not in ops.names()
    assert "secret" not in ops.stderr.decode("ascii")
    return str(markers[0]["reason"])


MUTATIONS = {"fchmod", "fchown", "umask", "prctl", "setgroups", "setresgid", "setresuid",
             "open_probe", "unlink", "execve"}


def _no_mutation(ops: FakeOps) -> None:
    assert not MUTATIONS & set(ops.names())


def _exec(ops: FakeOps, argv: list[str] | None = None) -> None:
    with pytest.raises(ExecCalled):
        boot.run(list(LOADER_ARGV if argv is None else argv), ops)


# -- success, ordering and idempotence -------------------------------------------------


def test_root_owned_mount_is_prepared_dropped_probed_then_execs_unchanged_argv() -> None:
    ops = FakeOps()
    _exec(ops)
    names = ops.names()
    order = [
        "fchmod", "fchown", "umask", "prctl", "setgroups", "setresgid", "setresuid",
        "open_probe", "unlink", "execve",
    ]
    positions = [names.index(name) for name in order]
    assert positions == sorted(positions)
    assert ("fchmod", (DIR_FD, 0o700)) in ops.calls
    assert ("fchown", (DIR_FD, 65532, 65532)) in ops.calls
    assert [args for name, args in ops.calls if name == "prctl"] == [
        (boot.PR_SET_NO_NEW_PRIVS, 1), (boot.PR_GET_NO_NEW_PRIVS, 0),
    ]
    assert ("setgroups", ((),)) in ops.calls
    assert ("setresgid", (65532, 65532, 65532)) in ops.calls
    assert ("setresuid", (65532, 65532, 65532)) in ops.calls
    assert ops.umask_value == 0o077
    # The directory descriptor is closed before the handoff and fdinfo is only read as root.
    assert ops.calls.index(("close", (DIR_FD,))) < names.index("execve")
    assert ops.calls.index(("close", (PROBE_FD,))) < ops.calls.index(("close", (DIR_FD,)))
    fdinfo_reads = [
        index for index, (name, args) in enumerate(ops.calls)
        if name == "read_file" and args[0].startswith("/proc/self/fdinfo/")
    ]
    assert fdinfo_reads and max(fdinfo_reads) < names.index("setresuid")
    # Exactly one new probe name was created and removed through the pinned descriptor.
    created = [args[0] for name, args in ops.calls if name == "open_probe"]
    assert created == [".bootstrap-probe-" + bytes(range(16)).hex()]
    assert ("unlink", (created[0], DIR_FD)) in ops.calls
    assert ops.probe_files == {}
    assert ("fsync", (DIR_FD,)) in ops.calls
    execve = [args for name, args in ops.calls if name == "execve"]
    assert len(execve) == 1
    path, argv, env = execve[0]
    assert path == "/usr/bin/timeout"
    assert list(argv) == LOADER_ARGV
    env_map = dict(env)
    assert env_map["PATH"] == "/usr/local/bin:/usr/bin:/bin"
    assert env_map["DATABASE_URL"] == ops.env["DATABASE_URL"]
    (marker,) = _markers(ops)
    assert marker["phase"] == "privileges_dropped"
    assert marker["wrapper_sha256"] == hashlib.sha256(ops.wrapper_bytes).hexdigest()
    assert marker["loader_mode"] == "--verify-only"
    assert marker["uid"] == [65532] * 3 and marker["gid"] == [65532] * 3
    assert marker["groups"] == []
    assert marker["no_new_privs"] == 1
    assert {marker["capabilities"][key] for key in (
        "inheritable", "permitted", "effective", "ambient")} == {ZERO_CAPS}
    assert marker["evidence"] == {
        "path": "/evidence", "mount_id": EVIDENCE_MOUNT_ID, "initial_uid": 0,
        "initial_gid": 0, "initial_mode": "0755", "uid": 65532, "gid": 65532,
        "mode": "0700", "chmod_applied": True, "chown_applied": True,
    }
    assert "secret" not in ops.stderr.decode("ascii")
    assert "DATABASE_URL" not in ops.stderr.decode("ascii")


def test_already_owned_private_mount_is_idempotent_without_metadata_changes() -> None:
    ops = FakeOps()
    ops.evidence = FakeStat(stat.S_IFDIR | 0o700, 65532, 65532)
    ops.cap_eff = f"{(1 << 6) | (1 << 7):016x}"  # SETGID+SETUID only
    _exec(ops)
    assert "fchmod" not in ops.names() and "fchown" not in ops.names()
    (marker,) = _markers(ops)
    assert marker["evidence"]["chmod_applied"] is False
    assert marker["evidence"]["chown_applied"] is False


def test_owned_mount_with_broader_safe_mode_is_narrowed_without_chown() -> None:
    ops = FakeOps()
    ops.evidence = FakeStat(stat.S_IFDIR | 0o750, 65532, 65532)
    _exec(ops)
    assert ("fchmod", (DIR_FD, 0o700)) in ops.calls
    assert "fchown" not in ops.names()


@pytest.mark.parametrize("mode", LOADER_ARGV[-1:] + ["--dry-run", "--apply", "--verify-published"])
def test_each_supported_explicit_mode_is_passed_through(mode: str) -> None:
    ops = FakeOps()
    argv = [*LOADER_ARGV[:-1], mode]
    _exec(ops, argv)
    execve = [args for name, args in ops.calls if name == "execve"]
    assert list(execve[0][1]) == argv


# -- invocation and argv ---------------------------------------------------------------


@pytest.mark.parametrize(
    "argv",
    [
        [],
        LOADER_ARGV[:-1],
        [*LOADER_ARGV, "--verify-only"],
        [*LOADER_ARGV[:-1], "--apply", "--verify-only"],
        [*LOADER_ARGV[:-1], "--force"],
        [*LOADER_ARGV[:-1], "--verify-only --apply"],
        [*LOADER_ARGV[:3], "9001s", *LOADER_ARGV[4:]],
        [*LOADER_ARGV[:10], "/tmp", "--verify-only"],
        [*LOADER_ARGV[:8], "/tmp/artifact", *LOADER_ARGV[9:]],
        ["/bin/sh", "-c", " ".join(LOADER_ARGV)],
        [" ".join(LOADER_ARGV)],
        ["/usr/bin/timeout", *LOADER_ARGV[1:]],
        [*LOADER_ARGV[:4], "/usr/local/bin/python", *LOADER_ARGV[5:]],
        [*LOADER_ARGV[:6], "scripts.other", *LOADER_ARGV[7:]],
    ],
)
def test_argv_substitution_is_refused_before_any_system_call(argv: list[str]) -> None:
    ops = FakeOps()
    assert _refused(ops, argv) == "argv_refused"
    assert set(ops.names()) == {"write_stderr"}


def test_isolated_invocation_requires_exact_interpreter_flags_and_wrapper() -> None:
    exact = [*boot.INTERPRETER_ARGV, *LOADER_ARGV]
    boot.require_isolated_invocation(exact, LOADER_ARGV, 1, 1)
    refused = [
        (["/usr/local/bin/python", "-S", boot.WRAPPER_PATH, *LOADER_ARGV], 1, 1),
        (["/usr/local/bin/python", "-I", boot.WRAPPER_PATH, *LOADER_ARGV], 1, 1),
        (["python", "-I", "-S", boot.WRAPPER_PATH, *LOADER_ARGV], 1, 1),
        (["/usr/local/bin/python", "-I", "-S", "/tmp/bootstrap_evidence.py", *LOADER_ARGV], 1, 1),
        ([*boot.INTERPRETER_ARGV, *LOADER_ARGV[:-1], "--apply"], 1, 1),
        (exact, 0, 1),
        (exact, 1, 0),
    ]
    for orig_argv, isolated, no_site in refused:
        with pytest.raises(boot.BootstrapError) as exc:
            boot.require_isolated_invocation(orig_argv, LOADER_ARGV, isolated, no_site)
        assert exc.value.reason == "interpreter_refused"


def test_main_refuses_a_non_isolated_host_invocation_before_system_access(
    capfd: pytest.CaptureFixture[str],
) -> None:
    assert boot.main(list(LOADER_ARGV)) == 78
    captured = capfd.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "schema": "bond_artifact_bootstrap/1", "phase": "bootstrap_failure",
        "reason": "interpreter_refused",
    }


def test_non_linux_platform_is_refused() -> None:
    ops = FakeOps()
    ops.platform_name = "win32"
    assert _refused(ops) == "platform_unsupported"
    _no_mutation(ops)


@pytest.mark.parametrize(
    ("uids", "gids"),
    [([65532] * 3, [65532] * 3), ([0, 0, 65532], [0, 0, 0]), ([0, 0, 0], [0, 65532, 0]),
     ([1000, 0, 0], [0, 0, 0])],
)
def test_privileged_bootstrap_requires_all_root_credential_slots(
    uids: list[int], gids: list[int]
) -> None:
    ops = FakeOps()
    ops.uid, ops.gid = uids, gids
    assert _refused(ops) == "not_root"
    assert "lstat" not in ops.names()
    _no_mutation(ops)


def test_unprivileged_writable_root_directory_is_refused() -> None:
    ops = FakeOps()
    ops.root = FakeStat(stat.S_IFDIR | 0o777, 0, 0, st_dev=1, st_ino=2)
    assert _refused(ops) == "root_directory_unsafe"
    _no_mutation(ops)


# -- path pinning and mount identity ------------------------------------------------------


def test_missing_evidence_path_is_refused_without_creating_it() -> None:
    ops = FakeOps()
    ops.path_missing = True
    assert _refused(ops) == "evidence_missing"
    assert "open_directory" not in ops.names()
    _no_mutation(ops)


@pytest.mark.parametrize("kind", [stat.S_IFLNK, stat.S_IFREG, stat.S_IFIFO])
def test_symlink_or_special_evidence_path_is_refused(kind: int) -> None:
    ops = FakeOps()
    ops.path_override = FakeStat(kind | 0o777, 0, 0, st_ino=99)
    assert _refused(ops) == "evidence_not_directory"
    assert "open_directory" not in ops.names()
    _no_mutation(ops)


def test_path_replaced_between_lstat_and_open_is_refused_and_descriptor_closed() -> None:
    ops = FakeOps()
    ops.path_override = FakeStat(stat.S_IFDIR | 0o755, 0, 0, st_ino=8)
    assert _refused(ops) == "evidence_identity_changed"
    assert ("close", (DIR_FD,)) in ops.calls
    _no_mutation(ops)


def test_open_failure_including_nofollow_rejection_is_refused() -> None:
    ops = FakeOps()
    ops.fail["open_directory"] = OSError(errno.ELOOP, "symlink")
    assert _refused(ops) == "evidence_open_failed"
    _no_mutation(ops)


def test_fdinfo_inode_disagreeing_with_descriptor_is_refused() -> None:
    ops = FakeOps()
    ops.fdinfo = f"pos:\t0\nmnt_id:\t{EVIDENCE_MOUNT_ID}\nino:\t8\n"
    assert _refused(ops) == "evidence_identity_changed"
    _no_mutation(ops)


def test_same_filesystem_directory_without_its_own_mount_is_refused() -> None:
    # The unmounted image directory can share st_dev with other paths; only mountinfo
    # proves that /evidence is itself a mount whose ID matches the pinned descriptor.
    ops = FakeOps()
    ops.evidence = FakeStat(stat.S_IFDIR | 0o755, 0, 0, st_dev=1, st_ino=7)
    ops.mountinfo = _mountinfo(evidence="")
    ops.fdinfo = "pos:\t0\nmnt_id:\t700\n"
    assert _refused(ops) == "evidence_not_mount"
    _no_mutation(ops)


def test_mount_id_mismatch_between_descriptor_and_path_is_refused() -> None:
    ops = FakeOps()
    ops.fdinfo = "pos:\t0\nmnt_id:\t700\n"
    assert _refused(ops) == "evidence_mount_mismatch"
    _no_mutation(ops)


def test_stacked_evidence_mounts_are_ambiguous() -> None:
    ops = FakeOps()
    ops.mountinfo = _mountinfo(
        "999 812 8:48 /other /evidence rw,relatime - ext4 /dev/sdd rw"
    )
    assert _refused(ops) == "evidence_mount_ambiguous"
    _no_mutation(ops)


def test_octal_escaped_mount_point_is_not_confused_with_evidence() -> None:
    mounts = boot.parse_mountinfo(
        "1 0 0:1 / / rw - overlay overlay rw\n"
        "2 1 0:2 / /evidence\\040copy rw - ext4 /dev/x rw\n"
    )
    assert [entry[1] for entry in mounts] == ["/", "/evidence copy"]


@pytest.mark.parametrize(
    "evidence",
    [
        f"{EVIDENCE_MOUNT_ID} 700 8:48 / /evidence ro,relatime - ext4 /dev/sdd ro",
        f"{EVIDENCE_MOUNT_ID} 700 8:48 / /evidence ro,relatime - ext4 /dev/sdd rw",
        f"{EVIDENCE_MOUNT_ID} 700 8:48 / /evidence rw,relatime - ext4 /dev/sdd ro,errors=x",
    ],
)
def test_read_only_mount_or_superblock_is_refused(evidence: str) -> None:
    ops = FakeOps()
    ops.mountinfo = _mountinfo(evidence=evidence)
    assert _refused(ops) == "evidence_mount_read_only"
    _no_mutation(ops)


def test_read_only_statvfs_is_refused() -> None:
    ops = FakeOps()
    ops.statvfs_flag = boot.ST_RDONLY
    assert _refused(ops) == "evidence_mount_read_only"
    _no_mutation(ops)


@pytest.mark.parametrize(
    "mountinfo",
    [
        "",
        "700 690 0:61 / / rw - overlay overlay rw",  # no trailing newline
        "700 690 0:61 / / rw overlay overlay rw\n",  # no separator
        "x 690 0:61 / / rw - overlay overlay rw\n",
        "700 690 061 / / rw - overlay overlay rw\n",
        "700 690 0:61 / relative rw - overlay overlay rw\n",
        "700 690 0:61 / / rw - overlay overlay\n",
        "700 690 0:61 / / rw - overlay overlay rw extra\n",
        "700 690 0:61 / / rw - overlay overlay rw\n700 690 0:61 / /x rw - o o rw\n",
        "700 690 0:61 / / rw - overlay overlay rw\n" * 30_000,  # exceeds the 1 MiB bound
    ],
    ids=[
        "empty", "no-newline", "no-separator", "bad-id", "bad-device", "relative-point",
        "short-super", "extra-field", "duplicate-id", "oversized",
    ],
)
def test_malformed_or_oversized_mountinfo_is_refused(mountinfo: str) -> None:
    ops = FakeOps()
    ops.mountinfo = mountinfo
    assert _refused(ops) == "proc_evidence_invalid"
    _no_mutation(ops)


@pytest.mark.parametrize(
    "fdinfo",
    ["pos:\t0\nflags:\t0\n", "mnt_id:\tabc\n", "mnt_id:\t1\nmnt_id:\t1\n", "mnt_id:\t812"],
)
def test_malformed_fdinfo_is_refused(fdinfo: str) -> None:
    ops = FakeOps()
    ops.fdinfo = fdinfo
    assert _refused(ops) == "proc_evidence_invalid"
    _no_mutation(ops)


def test_wrapper_hash_read_failure_is_refused() -> None:
    ops = FakeOps()
    ops.fail["read_file"] = PermissionError(errno.EACCES, "denied")
    assert _refused(ops) == "wrapper_hash_failed"
    assert "lstat" not in ops.names()
    _no_mutation(ops)


@pytest.mark.parametrize(
    "path", ["/proc/self/mountinfo", f"/proc/self/fdinfo/{DIR_FD}", "/proc/self/status"]
)
def test_proc_read_failure_is_refused(path: str) -> None:
    class ProcReadFails(FakeOps):
        def read_file(self, requested: str, limit: int) -> bytes:
            if requested == path:
                self._record("read_file", requested)
                raise PermissionError(errno.EACCES, "denied")
            return super().read_file(requested, limit)

    ops = ProcReadFails()
    assert _refused(ops) == "proc_evidence_invalid"
    _no_mutation(ops)


def test_status_parser_requires_every_credential_and_capability_field() -> None:
    ops = FakeOps()
    text = ops._status()
    boot.parse_status(text)
    for field in ("Uid", "Gid", "Groups", "NoNewPrivs", "CapInh", "CapAmb", "CapBnd"):
        broken = "".join(
            line + "\n" for line in text.splitlines() if not line.startswith(field + ":")
        )
        with pytest.raises(boot.BootstrapError) as exc:
            boot.parse_status(broken)
        assert exc.value.reason == "proc_evidence_invalid"
    for bad in (
        text.replace("CapEff:\t" + FULL_CAPS, "CapEff:\tzz"),
        text.replace("NoNewPrivs:\t0", "NoNewPrivs:\t2"),
        text.replace("Uid:\t0\t0\t0\t0", "Uid:\t0\t0\t0"),
        text + "CapInh:\t" + ZERO_CAPS + "\n",
    ):
        with pytest.raises(boot.BootstrapError):
            boot.parse_status(bad)


# -- ownership, mode and capability policy ---------------------------------------------


@pytest.mark.parametrize(
    ("uid", "gid"), [(1000, 1000), (0, 65532), (65532, 0), (65533, 65532), (0, 1)]
)
def test_foreign_or_mixed_owner_is_refused_without_repair(uid: int, gid: int) -> None:
    ops = FakeOps()
    ops.evidence = FakeStat(stat.S_IFDIR | 0o700, uid, gid)
    assert _refused(ops) == "evidence_owner_refused"
    assert not MUTATIONS & set(ops.names())
    assert (ops.evidence.st_uid, ops.evidence.st_gid) == (uid, gid)


@pytest.mark.parametrize("mode", [0o775, 0o757, 0o777, 0o1755, 0o2755, 0o4755, 0o1700])
def test_group_world_writable_or_special_mode_is_refused(mode: int) -> None:
    ops = FakeOps()
    ops.evidence = FakeStat(stat.S_IFDIR | mode, 0, 0)
    assert _refused(ops) == "evidence_mode_refused"
    _no_mutation(ops)
    assert stat.S_IMODE(ops.evidence.st_mode) == mode


@pytest.mark.parametrize(
    ("owner", "mode", "missing"),
    [
        ((0, 0), 0o755, 0),  # CAP_CHOWN
        ((0, 0), 0o755, 6),  # CAP_SETGID
        ((0, 0), 0o755, 7),  # CAP_SETUID
        ((65532, 65532), 0o755, 3),  # CAP_FOWNER for a foreign-owned chmod
        ((65532, 65532), 0o700, 7),
    ],
)
def test_missing_capability_is_refused_before_any_mutation(
    owner: tuple[int, int], mode: int, missing: int
) -> None:
    ops = FakeOps()
    ops.evidence = FakeStat(stat.S_IFDIR | mode, *owner)
    ops.cap_eff = f"{int(FULL_CAPS, 16) & ~(1 << missing):016x}"
    assert _refused(ops) == "capability_missing"
    assert not MUTATIONS & set(ops.names())


@pytest.mark.parametrize("failing", ["fchmod", "fchown"])
def test_metadata_change_failure_stops_before_privilege_drop(failing: str) -> None:
    ops = FakeOps()
    ops.fail[failing] = PermissionError(errno.EPERM, "root squashed")
    assert _refused(ops) == "evidence_prepare_failed"
    assert not {"umask", "prctl", "setgroups", "setresgid", "setresuid"} & set(ops.names())


def test_silently_ignored_chown_fails_postcondition() -> None:
    ops = FakeOps()
    ops.chown_is_noop = True
    assert _refused(ops) == "evidence_postcondition_failed"
    assert "setresuid" not in ops.names()


# -- irreversible privilege drop ----------------------------------------------------------


def test_no_new_privs_failure_is_fatal_before_identity_changes() -> None:
    ops = FakeOps()
    ops.fail["prctl"] = OSError(errno.EINVAL, "unsupported")
    assert _refused(ops) == "no_new_privs_failed"
    assert not {"setgroups", "setresgid", "setresuid"} & set(ops.names())


def test_no_new_privs_readback_must_be_one() -> None:
    ops = FakeOps()
    ops.no_new_privs_readback = 0
    assert _refused(ops) == "no_new_privs_failed"
    assert "setgroups" not in ops.names()


@pytest.mark.parametrize(
    ("failing", "absent"),
    [
        ("setgroups", {"setresgid", "setresuid"}),
        ("setresgid", {"setresuid"}),
        ("setresuid", set()),
    ],
)
def test_identity_change_failure_is_fatal_and_never_falls_back(
    failing: str, absent: set[str]
) -> None:
    ops = FakeOps()
    ops.fail[failing] = PermissionError(errno.EPERM, "denied")
    assert _refused(ops) == "privilege_drop_failed"
    assert not absent & set(ops.names())
    assert "open_probe" not in ops.names()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda ops: ops.dropped_caps.update(CapInh="0000000000000400"),
        lambda ops: ops.dropped_caps.update(CapPrm="0000000000000001"),
        lambda ops: ops.dropped_caps.update(CapEff="0000000000000080"),
        lambda ops: ops.dropped_caps.update(CapAmb="0000000000000001"),
        lambda ops: setattr(ops, "dropped_uid_line", "65532\t65532\t65532\t0"),
    ],
)
def test_wrong_post_drop_kernel_state_is_fatal(mutate: Any) -> None:
    ops = FakeOps()
    mutate(ops)
    assert _refused(ops) == "privilege_verification_failed"
    assert "open_probe" not in ops.names()


def test_retained_supplementary_group_is_fatal() -> None:
    class KeepsGroups(FakeOps):
        def setgroups(self, groups: list[int]) -> None:
            self._record("setgroups", tuple(groups))

    ops = KeepsGroups()
    assert _refused(ops) == "privilege_verification_failed"
    assert "open_probe" not in ops.names()


def test_saved_uid_left_as_root_is_fatal() -> None:
    class KeepsSavedRoot(FakeOps):
        def setresuid(self, ruid: int, euid: int, suid: int) -> None:
            self._record("setresuid", ruid, euid, suid)
            self.uid = [ruid, euid, 0]

    ops = KeepsSavedRoot()
    assert _refused(ops) == "privilege_verification_failed"


# -- unprivileged probe --------------------------------------------------------------------


def test_probe_name_collision_never_touches_existing_file() -> None:
    ops = FakeOps()
    existing = ".bootstrap-probe-" + bytes(range(16)).hex()
    ops.probe_files[existing] = bytearray(b"operator data")
    assert _refused(ops) == "probe_failed"
    assert "unlink" not in ops.names()
    assert ops.probe_files == {existing: bytearray(b"operator data")}


@pytest.mark.parametrize("failing", ["write", "fsync", "fstat", "close"])
def test_probe_write_sync_stat_or_close_failure_removes_only_the_new_file(failing: str) -> None:
    class ProbeFileFails(FakeOps):
        def _record(self, name: str, *args: Any) -> None:
            super()._record(name, *args)
            if name == failing and args[:1] == (PROBE_FD,):
                raise OSError(errno.ENOSPC, "full")

    ops = ProbeFileFails()
    assert _refused(ops) == "probe_failed"
    assert ops.probe_files == {}
    assert "unlink" in ops.names()
    assert ("fsync", (DIR_FD,)) in ops.calls


def test_short_probe_write_is_fatal_after_cleanup() -> None:
    ops = FakeOps()
    ops.probe_short_write = True
    assert _refused(ops) == "probe_failed"
    assert ops.probe_files == {}


def test_probe_cleanup_failure_is_fatal() -> None:
    ops = FakeOps()
    ops.fail["unlink"] = OSError(errno.EIO, "io")
    assert _refused(ops) == "probe_cleanup_failed"


def test_probe_directory_sync_failure_is_fatal() -> None:
    class DirectorySyncFails(FakeOps):
        def fsync(self, fd: int) -> None:
            self._record("fsync", fd)
            if fd == DIR_FD:
                raise OSError(errno.EIO, "io")

    ops = DirectorySyncFails()
    assert _refused(ops) == "probe_cleanup_failed"


def test_probe_open_failure_is_fatal() -> None:
    ops = FakeOps()
    ops.fail["open_probe"] = PermissionError(errno.EACCES, "denied")
    assert _refused(ops) == "probe_failed"


# -- handoff -------------------------------------------------------------------------------


def test_exec_failure_after_marker_reports_bootstrap_failure() -> None:
    ops = FakeOps()
    ops.fail["execve"] = OSError(errno.ENOENT, "missing")
    assert boot.run(list(LOADER_ARGV), ops) == 78
    phases = [marker["phase"] for marker in _markers(ops)]
    assert phases == ["privileges_dropped", "bootstrap_failure"]
    assert _markers(ops)[-1]["reason"] == "exec_failed"


def test_unexpected_exception_is_sanitized_as_internal_error() -> None:
    ops = FakeOps()
    ops.fail["getgroups"] = RuntimeError("postgresql://u:secret@h/db")
    assert _refused(ops) == "internal_error"


# -- static image and release-context checks ------------------------------------------


def _dockerfile() -> str:
    return (RUNTIME / "Dockerfile").read_text(encoding="utf-8")


def _cmd() -> list[str]:
    lines = [line for line in _dockerfile().splitlines() if line.startswith("CMD ")]
    assert len(lines) == 1
    return json.loads(lines[0][4:])


def test_wrapper_is_lf_stdlib_only_and_reads_no_environment_selection() -> None:
    raw = WRAPPER.read_bytes()
    assert b"\r" not in raw
    tree = ast.parse(raw)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert imported == {"sys", "ctypes", "hashlib", "json", "os", "re", "stat"}
    text = raw.decode("utf-8")
    for forbidden in ("getenv", "environ.get", "environ[", "subprocess", "shell=",
                      "import scripts", "from src", "RAILWAY_RUN_UID"):
        assert forbidden not in text
    assert text.index("sys.dont_write_bytecode = True") < text.index("import ctypes")


def test_dockerfile_pins_wrapper_copy_digest_root_start_and_unchanged_cmd() -> None:
    text = _dockerfile()
    digest = hashlib.sha256(WRAPPER.read_bytes()).hexdigest()
    copy = re.search(
        r"^COPY docker/bond-implied-artifact-loader/bootstrap_evidence\.py \\\n"
        r"\s+/app/docker/bond-implied-artifact-loader/\n",
        text, re.MULTILINE,
    )
    assert copy is not None
    literal = re.findall(r"\b([0-9a-f]{64})  /app/docker/bond-implied-artifact-loader/"
                         r"bootstrap_evidence\.py", text)
    assert literal == [digest]
    assert text.index("bootstrap_evidence.py \\") < text.index(digest)
    assert "sha256sum -c" in text
    users = [line for line in text.splitlines() if line.startswith("USER ")]
    assert users == ["USER 0:0"]
    entrypoints = [line for line in text.splitlines() if line.startswith("ENTRYPOINT")]
    assert len(entrypoints) == 1
    assert json.loads(entrypoints[0][len("ENTRYPOINT "):]) == list(boot.INTERPRETER_ARGV)
    assert _cmd() == LOADER_ARGV
    assert text.index("\nUSER 0:0\n") < text.index("\nENTRYPOINT [") < text.index("\nCMD [")
    after_user = text[text.index("\nUSER 0:0\n"):]
    assert "\nRUN " not in after_user and "\nCOPY " not in after_user
    assert "FROM python:3.13-slim@sha256:2b7445fb71ca9cb15e9aab053fe8cb3162796f8e1d92ada12a49c766a811bc1e" in text
    assert "requirements.lock" in text and "--require-hashes" in text


def test_railway_start_command_invokes_the_same_wrapper_and_cmd() -> None:
    document = tomllib.loads((RUNTIME / "railway.toml").read_text(encoding="utf-8"))
    command = document["deploy"]["startCommand"]
    tokens = shlex.split(command)
    assert command == " ".join(tokens)
    assert tuple(tokens[:4]) == boot.INTERPRETER_ARGV
    assert tokens[4:] == _cmd() == LOADER_ARGV
    assert document["deploy"]["restartPolicyType"] == "never"


def test_release_context_binds_new_runtime_and_refuses_pre_bootstrap_hashes(
    tmp_path: Path,
) -> None:
    from src.bonds import implied_rating_artifact_loader as loader

    contract = loader.load_frozen_contract()

    def write(dockerfile: str, railway: str, name: str) -> Path:
        evidence = tmp_path / name
        evidence.mkdir()
        document = {
            "schema_version": "bond_market_implied_rating_artifact_release/1",
            "target": "production",
            "loader_commit": "1" * 40,
            "loader_tree": "2" * 40,
            "review_changeset_sha256": "3" * 64,
            "review_report_sha256": "4" * 64,
            "context_archive_sha256": "5" * 64,
            "inventory_sha256": "6" * 64,
            "exclusion_evidence_sha256": "7" * 64,
            "runtime_base_image": contract.runtime.base_image,
            "requirements_lock_sha256": hashlib.sha256(
                (RUNTIME / "requirements.lock").read_bytes()
            ).hexdigest(),
            "dockerfile_sha256": dockerfile,
            "railway_toml_sha256": railway,
            "source_sha256": {
                relative: hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
                for relative in loader._RELEASE_SOURCE_PATHS
            },
        }
        (evidence / "release-context.json").write_text(json.dumps(document), encoding="utf-8")
        return evidence

    fresh_dockerfile = hashlib.sha256((RUNTIME / "Dockerfile").read_bytes()).hexdigest()
    fresh_railway = hashlib.sha256((RUNTIME / "railway.toml").read_bytes()).hexdigest()
    assert fresh_dockerfile != OLD_DOCKERFILE_SHA256
    assert fresh_railway != OLD_RAILWAY_TOML_SHA256
    release = loader._load_release_evidence(write(fresh_dockerfile, fresh_railway, "new"), contract)
    assert release.dockerfile_sha256 == fresh_dockerfile
    assert release.railway_toml_sha256 == fresh_railway
    # The wrapper is bound transitively by the Dockerfile's literal digest, not by a new
    # release-context field or source-inventory entry.
    assert "docker/bond-implied-artifact-loader/bootstrap_evidence.py" not in (
        loader._RELEASE_SOURCE_PATHS
    )
    for dockerfile, railway, field in (
        (OLD_DOCKERFILE_SHA256, fresh_railway, "release_context.dockerfile_sha256"),
        (fresh_dockerfile, OLD_RAILWAY_TOML_SHA256, "release_context.railway_toml_sha256"),
    ):
        with pytest.raises(loader.ArtifactLoaderError) as exc:
            loader._load_release_evidence(write(dockerfile, railway, field), contract)
        assert exc.value.code == loader.ErrorCode.RECEIPT_FAILURE.value
        assert exc.value.details == {"field": field}
