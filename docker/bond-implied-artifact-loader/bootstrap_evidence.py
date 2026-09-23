"""Bounded root-only startup bootstrap for the frozen artifact loader's /evidence volume.

The image starts this file as root through exactly::

    /usr/local/bin/python -I -S /app/docker/bond-implied-artifact-loader/bootstrap_evidence.py \
        timeout --signal=TERM --kill-after=30s 9000s python -m \
        scripts.load_bond_market_implied_rating_artifact --artifact-root /artifact \
        --evidence-dir /evidence <one explicit loader mode>

Railway replaces the Docker ENTRYPOINT with its custom start command, so both the image
ENTRYPOINT and the reviewed start command name this wrapper explicitly.

While root, the wrapper only pins and validates the mounted ``/evidence`` directory,
changes that single mount root to 65532:65532 mode 0700, and irreversibly drops every
UID/GID slot, supplementary group and capability behind ``no_new_privs``. It then proves
that UID 65532 can create and remove a receipt-shaped file and replaces itself with the
unchanged ``timeout`` argv. It imports only the standard library, never the loader,
never touches the database, reads no path/UID/executable from the environment and has
no repair, test or fallback mode. Every startup failure exits 78 before the loader runs.

The loader child is an ordinary ``python -m`` process, so its import path must not be
steerable by the deployment environment or the writable evidence volume. Before the
exec, every inherited ``PYTHON*`` variable and every dynamic-loader ``LD_*`` variable is
removed, only the reviewed ``PYTHONPATH=/app``, ``PYTHONNOUSERSITE=1``,
``PYTHONDONTWRITEBYTECODE=1`` and ``PYTHONUNBUFFERED=1`` are set, PATH is pinned, and the
working directory becomes the image-owned, non-writable ``/app``. Other variables (for
example the database settings) pass through unchanged and their values are never logged.
The root interpreter's own launch cannot be sanitized from inside it; it relies on the
trusted image and deployment environment together with ``-I -S``.
"""
import sys

# ``-I`` ignores PYTHONDONTWRITEBYTECODE; root must not write bytecode caches.
sys.dont_write_bytecode = True

import ctypes  # noqa: E402
import hashlib  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import re  # noqa: E402
import stat  # noqa: E402

MARKER_SCHEMA = "bond_artifact_bootstrap/1"
EXIT_BOOTSTRAP_FAILURE = 78

EVIDENCE_PATH = "/evidence"
TARGET_UID = 65532
TARGET_GID = 65532
TARGET_MODE = 0o700
PYTHON_EXECUTABLE = "/usr/local/bin/python"
WRAPPER_PATH = "/app/docker/bond-implied-artifact-loader/bootstrap_evidence.py"
TIMEOUT_EXECUTABLE = "/usr/bin/timeout"
TRUSTED_PATH = "/usr/local/bin:/usr/bin:/bin"
APP_DIRECTORY = "/app"
# The only interpreter settings the loader child receives; inherited values are dropped.
CHILD_PYTHON_ENVIRONMENT = (
    ("PYTHONPATH", APP_DIRECTORY),
    ("PYTHONNOUSERSITE", "1"),
    ("PYTHONDONTWRITEBYTECODE", "1"),
    ("PYTHONUNBUFFERED", "1"),
)
# Interpreter startup (PYTHONPATH/HOME/USERBASE/STARTUP/INSPECT/WARNINGS and any future
# PYTHON* setting) and dynamic-loader injection (LD_PRELOAD/LD_LIBRARY_PATH/LD_AUDIT...).
STRIPPED_ENVIRONMENT_PREFIXES = ("PYTHON", "LD_")

INTERPRETER_ARGV = (PYTHON_EXECUTABLE, "-I", "-S", WRAPPER_PATH)
LOADER_PREFIX = (
    "timeout", "--signal=TERM", "--kill-after=30s", "9000s",
    "python", "-m", "scripts.load_bond_market_implied_rating_artifact",
    "--artifact-root", "/artifact", "--evidence-dir", EVIDENCE_PATH,
)
LOADER_MODES = ("--verify-only", "--dry-run", "--apply", "--verify-published")

MAX_MOUNTINFO_BYTES = 1 << 20
MAX_PROC_FILE_BYTES = 1 << 16
MAX_WRAPPER_BYTES = 1 << 20

PR_SET_NO_NEW_PRIVS = 38
PR_GET_NO_NEW_PRIVS = 39
ST_RDONLY = 1  # Linux statvfs f_flag bit; not exported by ``os`` off Linux.
CAP_CHOWN = 0
CAP_FOWNER = 3
CAP_SETGID = 6
CAP_SETUID = 7

UNSAFE_MODE_BITS = stat.S_ISUID | stat.S_ISGID | stat.S_ISVTX | stat.S_IWGRP | stat.S_IWOTH
ADMITTED_OWNERS = ((0, 0), (TARGET_UID, TARGET_GID))
PROBE_PREFIX = ".bootstrap-probe-"
PROBE_BYTES = b"bond_artifact_bootstrap/1 probe\n"

_CAP_FIELDS = ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb")
_STATUS_FIELDS = ("Uid", "Gid", "Groups", "NoNewPrivs", *_CAP_FIELDS)
_CAP_RE = re.compile(r"[0-9a-f]{16}")
_DECIMAL_RE = re.compile(r"(?:0|[1-9][0-9]*)")
_DEVICE_RE = re.compile(r"[0-9]+:[0-9]+")
_OCTAL_ESCAPE_RE = re.compile(r"\\([0-7]{3})")


class BootstrapError(Exception):
    """Startup refusal carrying only a stable, sanitized reason code."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class SystemOps:
    """The real operating-system boundary; there is no alternative implementation."""

    def platform(self) -> str:
        return sys.platform

    def getresuid(self) -> tuple[int, int, int]:
        return os.getresuid()

    def getresgid(self) -> tuple[int, int, int]:
        return os.getresgid()

    def getgroups(self) -> list[int]:
        return os.getgroups()

    def lstat(self, path: str) -> os.stat_result:
        return os.lstat(path)

    def open_directory(self, path: str) -> int:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        return os.open(path, flags)

    def fstat(self, fd: int) -> os.stat_result:
        return os.fstat(fd)

    def fstatvfs(self, fd: int) -> object:  # os.statvfs_result exists only on POSIX
        return os.fstatvfs(fd)

    def close(self, fd: int) -> None:
        os.close(fd)

    def read_file(self, path: str, limit: int) -> bytes:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            chunks: list[bytes] = []
            total = 0
            while total <= limit:
                chunk = os.read(fd, min(65536, limit + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
            return b"".join(chunks)
        finally:
            os.close(fd)

    def fchmod(self, fd: int, mode: int) -> None:
        os.fchmod(fd, mode)

    def fchown(self, fd: int, uid: int, gid: int) -> None:
        os.fchown(fd, uid, gid)

    def umask(self, mask: int) -> int:
        return os.umask(mask)

    def prctl(self, option: int, arg2: int) -> int:
        libc = ctypes.CDLL(None, use_errno=True)
        function = libc.prctl
        function.restype = ctypes.c_int
        function.argtypes = (
            ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong,
        )
        result = function(option, arg2, 0, 0, 0)
        if result < 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        return int(result)

    def setgroups(self, groups: list[int]) -> None:
        os.setgroups(groups)

    def setresgid(self, rgid: int, egid: int, sgid: int) -> None:
        os.setresgid(rgid, egid, sgid)

    def setresuid(self, ruid: int, euid: int, suid: int) -> None:
        os.setresuid(ruid, euid, suid)

    def urandom(self, size: int) -> bytes:
        return os.urandom(size)

    def open_probe(self, name: str, dir_fd: int) -> int:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
        return os.open(name, flags, 0o600, dir_fd=dir_fd)

    def write(self, fd: int, data: bytes) -> int:
        return os.write(fd, data)

    def fsync(self, fd: int) -> None:
        os.fsync(fd)

    def unlink(self, name: str, dir_fd: int) -> None:
        os.unlink(name, dir_fd=dir_fd)

    def fchdir(self, fd: int) -> None:
        os.fchdir(fd)

    def environ(self) -> dict[str, str]:
        return dict(os.environ)

    def write_stderr(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            written = os.write(2, view)
            view = view[written:]

    def execve(self, path: str, argv: list[str], env: dict[str, str]) -> None:
        os.execve(path, argv, env)


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise BootstrapError(reason)


def _close_quietly(ops: SystemOps, fd: int) -> None:
    try:
        ops.close(fd)
    except OSError:
        pass


def validate_argv(argv: list[str]) -> str:
    """Admit only the reviewed supervisor argv with one explicit loader mode."""
    _require(isinstance(argv, list), "argv_refused")
    _require(all(type(token) is str for token in argv), "argv_refused")
    _require(len(argv) == len(LOADER_PREFIX) + 1, "argv_refused")
    _require(tuple(argv[: len(LOADER_PREFIX)]) == LOADER_PREFIX, "argv_refused")
    _require(argv[-1] in LOADER_MODES, "argv_refused")
    return argv[-1]


def require_isolated_invocation(
    orig_argv: list[str], argv: list[str], isolated: int, no_site: int
) -> None:
    """Pin the exact interpreter, flags and wrapper path used for the root phase."""
    _require(isolated == 1 and no_site == 1, "interpreter_refused")
    _require(type(orig_argv) is list, "interpreter_refused")
    _require(tuple(orig_argv[: len(INTERPRETER_ARGV)]) == INTERPRETER_ARGV, "interpreter_refused")
    _require(orig_argv[len(INTERPRETER_ARGV):] == argv, "interpreter_refused")


def _decode(raw: bytes) -> str:
    return raw.decode("utf-8", "surrogateescape")


def _read_bounded(ops: SystemOps, path: str, limit: int) -> str:
    try:
        raw = ops.read_file(path, limit)
    except OSError as exc:
        raise BootstrapError("proc_evidence_invalid") from exc
    _require(0 < len(raw) <= limit and raw.endswith(b"\n"), "proc_evidence_invalid")
    return _decode(raw)


def parse_status(text: str) -> dict[str, object]:
    """Extract credential, group, capability and no_new_privs facts from /proc status."""
    fields: dict[str, str] = {}
    for line in text.split("\n")[:-1]:
        key, separator, value = line.partition(":")
        if key in _STATUS_FIELDS:
            _require(separator == ":" and key not in fields, "proc_evidence_invalid")
            fields[key] = value.strip()
    _require(set(fields) == set(_STATUS_FIELDS), "proc_evidence_invalid")
    parsed: dict[str, object] = {}
    for key in ("Uid", "Gid"):
        values = fields[key].split()
        _require(len(values) == 4, "proc_evidence_invalid")
        _require(all(_DECIMAL_RE.fullmatch(value) for value in values), "proc_evidence_invalid")
        parsed[key] = tuple(int(value) for value in values)
    groups = fields["Groups"].split()
    _require(all(_DECIMAL_RE.fullmatch(value) for value in groups), "proc_evidence_invalid")
    parsed["Groups"] = tuple(int(value) for value in groups)
    _require(fields["NoNewPrivs"] in {"0", "1"}, "proc_evidence_invalid")
    parsed["NoNewPrivs"] = int(fields["NoNewPrivs"])
    for key in _CAP_FIELDS:
        _require(_CAP_RE.fullmatch(fields[key]) is not None, "proc_evidence_invalid")
        parsed[key] = fields[key]
    return parsed


def parse_fdinfo(text: str) -> tuple[int, int | None]:
    """Return (mnt_id, ino-or-None) for one descriptor's /proc fdinfo."""
    values: dict[str, str] = {}
    for line in text.split("\n")[:-1]:
        key, separator, value = line.partition(":")
        if key in {"mnt_id", "ino"}:
            _require(separator == ":" and key not in values, "proc_evidence_invalid")
            values[key] = value.strip()
    _require("mnt_id" in values, "proc_evidence_invalid")
    _require(_DECIMAL_RE.fullmatch(values["mnt_id"]) is not None, "proc_evidence_invalid")
    inode = None
    if "ino" in values:
        _require(_DECIMAL_RE.fullmatch(values["ino"]) is not None, "proc_evidence_invalid")
        inode = int(values["ino"])
    return int(values["mnt_id"]), inode


def _unescape(value: str) -> str:
    return _OCTAL_ESCAPE_RE.sub(lambda match: chr(int(match.group(1), 8)), value)


def parse_mountinfo(text: str) -> list[tuple[int, str, str, str]]:
    """Strictly parse /proc/self/mountinfo into (mount_id, mount_point, opts, super_opts)."""
    lines = text.split("\n")
    _require(len(lines) >= 2 and lines[-1] == "", "proc_evidence_invalid")
    mounts: list[tuple[int, str, str, str]] = []
    seen: set[int] = set()
    for line in lines[:-1]:
        fields = line.split(" ")
        _require(len(fields) >= 10 and "-" in fields[6:], "proc_evidence_invalid")
        separator = fields.index("-", 6)
        _require(len(fields) == separator + 4, "proc_evidence_invalid")
        mount_id, parent_id, device, _root, mount_point, options = fields[:6]
        _require(_DECIMAL_RE.fullmatch(mount_id) is not None, "proc_evidence_invalid")
        _require(_DECIMAL_RE.fullmatch(parent_id) is not None, "proc_evidence_invalid")
        _require(_DEVICE_RE.fullmatch(device) is not None, "proc_evidence_invalid")
        _require(mount_point.startswith("/"), "proc_evidence_invalid")
        _require(options != "" and fields[separator + 3] != "", "proc_evidence_invalid")
        identifier = int(mount_id)
        _require(identifier not in seen, "proc_evidence_invalid")
        seen.add(identifier)
        mounts.append((identifier, _unescape(mount_point), options, fields[separator + 3]))
    return mounts


def _capabilities(value: str) -> int:
    return int(value, 16)


def _evidence_mount(ops: SystemOps, mount_id: int) -> None:
    """Require exactly one writable /evidence mount whose ID is the pinned descriptor's."""
    mounts = parse_mountinfo(
        _read_bounded(ops, "/proc/self/mountinfo", MAX_MOUNTINFO_BYTES)
    )
    matches = [entry for entry in mounts if entry[1] == EVIDENCE_PATH]
    _require(bool(matches), "evidence_not_mount")
    _require(len(matches) == 1, "evidence_mount_ambiguous")
    identifier, _point, options, super_options = matches[0]
    _require(identifier == mount_id, "evidence_mount_mismatch")
    _require(options.split(",")[0] == "rw", "evidence_mount_read_only")
    _require("rw" in super_options.split(","), "evidence_mount_read_only")


def _descriptor_mount_id(ops: SystemOps, fd: int, inode: int) -> int:
    mount_id, fd_inode = parse_fdinfo(
        _read_bounded(ops, f"/proc/self/fdinfo/{fd}", MAX_PROC_FILE_BYTES)
    )
    _require(fd_inode is None or fd_inode == inode, "evidence_identity_changed")
    return mount_id


def _same_object(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _path_identity(ops: SystemOps, fd: int) -> os.stat_result:
    """Re-prove that the fixed path still names the pinned directory descriptor."""
    try:
        path_stat = ops.lstat(EVIDENCE_PATH)
        fd_stat = ops.fstat(fd)
    except OSError as exc:
        raise BootstrapError("evidence_identity_changed") from exc
    _require(stat.S_ISDIR(path_stat.st_mode), "evidence_identity_changed")
    _require(stat.S_ISDIR(fd_stat.st_mode), "evidence_identity_changed")
    _require(_same_object(path_stat, fd_stat), "evidence_identity_changed")
    return fd_stat


def _writable_filesystem(ops: SystemOps, fd: int) -> None:
    try:
        flags = ops.fstatvfs(fd).f_flag
    except OSError as exc:
        raise BootstrapError("evidence_mount_read_only") from exc
    _require(not flags & ST_RDONLY, "evidence_mount_read_only")


def _read_status(ops: SystemOps) -> dict[str, object]:
    return parse_status(_read_bounded(ops, "/proc/self/status", MAX_PROC_FILE_BYTES))


def _pin_evidence(ops: SystemOps) -> tuple[int, os.stat_result, int]:
    try:
        path_stat = ops.lstat(EVIDENCE_PATH)
    except FileNotFoundError as exc:
        raise BootstrapError("evidence_missing") from exc
    except OSError as exc:
        raise BootstrapError("evidence_open_failed") from exc
    _require(stat.S_ISDIR(path_stat.st_mode), "evidence_not_directory")
    try:
        fd = ops.open_directory(EVIDENCE_PATH)
    except OSError as exc:
        raise BootstrapError("evidence_open_failed") from exc
    try:
        try:
            fd_stat = ops.fstat(fd)
        except OSError as exc:
            raise BootstrapError("evidence_identity_changed") from exc
        _require(stat.S_ISDIR(fd_stat.st_mode), "evidence_identity_changed")
        _require(_same_object(path_stat, fd_stat), "evidence_identity_changed")
        mount_id = _descriptor_mount_id(ops, fd, fd_stat.st_ino)
        _evidence_mount(ops, mount_id)
        _writable_filesystem(ops, fd)
    except BaseException:
        _close_quietly(ops, fd)
        raise
    return fd, fd_stat, mount_id


def _admit_owner(fd_stat: os.stat_result) -> None:
    _require((fd_stat.st_uid, fd_stat.st_gid) in ADMITTED_OWNERS, "evidence_owner_refused")
    _require(not stat.S_IMODE(fd_stat.st_mode) & UNSAFE_MODE_BITS, "evidence_mode_refused")


def _require_capabilities(status: dict[str, object], fd_stat: os.stat_result) -> None:
    effective = _capabilities(str(status["CapEff"]))
    required = [CAP_SETGID, CAP_SETUID]
    if fd_stat.st_uid == 0:
        required.append(CAP_CHOWN)
    elif stat.S_IMODE(fd_stat.st_mode) != TARGET_MODE:
        required.append(CAP_FOWNER)
    _require(all(effective >> bit & 1 for bit in required), "capability_missing")


def _prepare_root(ops: SystemOps, fd: int, fd_stat: os.stat_result) -> tuple[bool, bool]:
    """Change only the pinned mount root: mode 0700 first, then 65532 ownership."""
    chmod_applied = stat.S_IMODE(fd_stat.st_mode) != TARGET_MODE
    chown_applied = fd_stat.st_uid == 0
    try:
        if chmod_applied:
            ops.fchmod(fd, TARGET_MODE)
        if chown_applied:
            ops.fchown(fd, TARGET_UID, TARGET_GID)
    except OSError as exc:
        raise BootstrapError("evidence_prepare_failed") from exc
    return chmod_applied, chown_applied


def _verify_prepared(ops: SystemOps, fd: int, pinned: os.stat_result, mount_id: int) -> None:
    current = _path_identity(ops, fd)
    _require(_same_object(current, pinned), "evidence_identity_changed")
    _require(
        (current.st_uid, current.st_gid) == (TARGET_UID, TARGET_GID)
        and stat.S_IMODE(current.st_mode) == TARGET_MODE,
        "evidence_postcondition_failed",
    )
    _evidence_mount(ops, mount_id)
    _writable_filesystem(ops, fd)


def _drop_privileges(ops: SystemOps) -> None:
    ops.umask(0o077)
    try:
        ops.prctl(PR_SET_NO_NEW_PRIVS, 1)
        enabled = ops.prctl(PR_GET_NO_NEW_PRIVS, 0)
    except OSError as exc:
        raise BootstrapError("no_new_privs_failed") from exc
    _require(enabled == 1, "no_new_privs_failed")
    try:
        ops.setgroups([])
        ops.setresgid(TARGET_GID, TARGET_GID, TARGET_GID)
        ops.setresuid(TARGET_UID, TARGET_UID, TARGET_UID)
    except OSError as exc:
        raise BootstrapError("privilege_drop_failed") from exc


def _verify_dropped(ops: SystemOps) -> dict[str, object]:
    target_uids = (TARGET_UID,) * 3
    target_gids = (TARGET_GID,) * 3
    try:
        uids = tuple(ops.getresuid())
        gids = tuple(ops.getresgid())
        groups = list(ops.getgroups())
    except OSError as exc:
        raise BootstrapError("privilege_verification_failed") from exc
    _require(uids == target_uids and gids == target_gids, "privilege_verification_failed")
    _require(groups == [], "privilege_verification_failed")
    status = _read_status(ops)
    _require(status["Uid"] == (TARGET_UID,) * 4, "privilege_verification_failed")
    _require(status["Gid"] == (TARGET_GID,) * 4, "privilege_verification_failed")
    _require(status["Groups"] == (), "privilege_verification_failed")
    _require(status["NoNewPrivs"] == 1, "privilege_verification_failed")
    for key in ("CapInh", "CapPrm", "CapEff", "CapAmb"):
        _require(_capabilities(str(status[key])) == 0, "privilege_verification_failed")
    return {
        "uid": list(uids),
        "gid": list(gids),
        "groups": groups,
        "capabilities": {
            "inheritable": status["CapInh"],
            "permitted": status["CapPrm"],
            "effective": status["CapEff"],
            "ambient": status["CapAmb"],
            "bounding": status["CapBnd"],
        },
        "no_new_privs": status["NoNewPrivs"],
    }


def _probe(ops: SystemOps, fd: int) -> None:
    """As UID 65532, create, sync and remove one new receipt-shaped file."""
    name = PROBE_PREFIX + ops.urandom(16).hex()
    try:
        probe_fd = ops.open_probe(name, fd)
    except OSError as exc:
        raise BootstrapError("probe_failed") from exc
    failure = None
    try:
        written = ops.write(probe_fd, PROBE_BYTES)
        ops.fsync(probe_fd)
        probe_stat = ops.fstat(probe_fd)
        if written != len(PROBE_BYTES) or not (
            stat.S_ISREG(probe_stat.st_mode)
            and (probe_stat.st_uid, probe_stat.st_gid) == (TARGET_UID, TARGET_GID)
            and stat.S_IMODE(probe_stat.st_mode) == 0o600
            and probe_stat.st_nlink == 1
        ):
            failure = "probe_failed"
    except OSError:
        failure = "probe_failed"
    try:
        ops.close(probe_fd)
    except OSError:
        failure = failure or "probe_failed"
    try:
        ops.unlink(name, fd)
        ops.fsync(fd)
    except OSError as exc:
        raise BootstrapError("probe_cleanup_failed") from exc
    if failure is not None:
        raise BootstrapError(failure)


def loader_environment(environment: dict[str, str]) -> dict[str, str]:
    """Return the child environment: no inherited PYTHON*/LD_* keys, pinned values only."""
    child = {
        key: value for key, value in environment.items()
        if not key.startswith(STRIPPED_ENVIRONMENT_PREFIXES)
    }
    child.update(CHILD_PYTHON_ENVIRONMENT)
    child["PATH"] = TRUSTED_PATH
    return child


def _pin_working_directory(ops: SystemOps) -> None:
    """Make the image-owned, non-writable /app the child's working directory."""
    try:
        path_stat = ops.lstat(APP_DIRECTORY)
    except OSError as exc:
        raise BootstrapError("working_directory_unsafe") from exc
    _require(stat.S_ISDIR(path_stat.st_mode), "working_directory_unsafe")
    try:
        fd = ops.open_directory(APP_DIRECTORY)
    except OSError as exc:
        raise BootstrapError("working_directory_unsafe") from exc
    try:
        try:
            fd_stat = ops.fstat(fd)
        except OSError as exc:
            raise BootstrapError("working_directory_unsafe") from exc
        _require(
            stat.S_ISDIR(fd_stat.st_mode)
            and _same_object(path_stat, fd_stat)
            and fd_stat.st_uid == 0
            and not stat.S_IMODE(fd_stat.st_mode) & (stat.S_IWGRP | stat.S_IWOTH),
            "working_directory_unsafe",
        )
        try:
            ops.fchdir(fd)
            current = ops.lstat(APP_DIRECTORY)
        except OSError as exc:
            raise BootstrapError("working_directory_failed") from exc
        _require(_same_object(current, fd_stat), "working_directory_unsafe")
    finally:
        _close_quietly(ops, fd)


def _emit(ops: SystemOps, document: dict[str, object]) -> None:
    payload = json.dumps(
        {"schema": MARKER_SCHEMA, **document},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
    )
    ops.write_stderr(payload.encode("ascii") + b"\n")


def _wrapper_sha256(ops: SystemOps) -> str:
    try:
        raw = ops.read_file(WRAPPER_PATH, MAX_WRAPPER_BYTES)
    except OSError as exc:
        raise BootstrapError("wrapper_hash_failed") from exc
    _require(0 < len(raw) <= MAX_WRAPPER_BYTES, "wrapper_hash_failed")
    return hashlib.sha256(raw).hexdigest()


def bootstrap(argv: list[str], ops: SystemOps) -> None:
    """Validate, prepare /evidence, drop privileges, probe, then exec the supervisor."""
    mode = validate_argv(argv)
    _require(ops.platform() == "linux", "platform_unsupported")
    _require(
        tuple(ops.getresuid()) == (0, 0, 0) and tuple(ops.getresgid()) == (0, 0, 0),
        "not_root",
    )
    wrapper_sha256 = _wrapper_sha256(ops)
    try:
        root = ops.lstat("/")
    except OSError as exc:
        raise BootstrapError("root_directory_unsafe") from exc
    _require(
        stat.S_ISDIR(root.st_mode) and root.st_uid == 0
        and not stat.S_IMODE(root.st_mode) & (stat.S_IWGRP | stat.S_IWOTH),
        "root_directory_unsafe",
    )
    fd, pinned, mount_id = _pin_evidence(ops)
    try:
        _admit_owner(pinned)
        _require_capabilities(_read_status(ops), pinned)
        chmod_applied, chown_applied = _prepare_root(ops, fd, pinned)
        _verify_prepared(ops, fd, pinned, mount_id)
        _require(
            _descriptor_mount_id(ops, fd, pinned.st_ino) == mount_id,
            "evidence_mount_mismatch",
        )
        _drop_privileges(ops)
        credentials = _verify_dropped(ops)
        # fdinfo becomes root-owned once the set-ID change clears "dumpable"; a descriptor's
        # mount never changes, so the path/mount association is rechecked through mountinfo.
        _verify_prepared(ops, fd, pinned, mount_id)
        _probe(ops, fd)
    except BaseException:
        _close_quietly(ops, fd)
        raise
    try:
        ops.close(fd)
    except OSError as exc:
        raise BootstrapError("evidence_close_failed") from exc
    _pin_working_directory(ops)
    inherited = ops.environ()
    environment = loader_environment(inherited)
    removed = sum(1 for key in inherited if key.startswith(STRIPPED_ENVIRONMENT_PREFIXES))
    _emit(ops, {
        "phase": "privileges_dropped",
        "wrapper_sha256": wrapper_sha256,
        "loader_mode": mode,
        "exec": TIMEOUT_EXECUTABLE,
        "evidence": {
            "path": EVIDENCE_PATH,
            "mount_id": mount_id,
            "initial_uid": pinned.st_uid,
            "initial_gid": pinned.st_gid,
            "initial_mode": format(stat.S_IMODE(pinned.st_mode), "04o"),
            "uid": TARGET_UID,
            "gid": TARGET_GID,
            "mode": format(TARGET_MODE, "04o"),
            "chmod_applied": chmod_applied,
            "chown_applied": chown_applied,
        },
        "child": {
            "cwd": APP_DIRECTORY,
            "path": TRUSTED_PATH,
            "python_environment": dict(CHILD_PYTHON_ENVIRONMENT),
            "stripped_environment_keys": removed,
        },
        **credentials,
    })
    try:
        ops.execve(TIMEOUT_EXECUTABLE, list(argv), environment)
    except OSError as exc:
        raise BootstrapError("exec_failed") from exc
    raise BootstrapError("exec_failed")


def run(argv: list[str], ops: SystemOps) -> int:
    """Run the bootstrap; any refusal emits one sanitized marker and returns 78."""
    try:
        bootstrap(argv, ops)
    except BootstrapError as exc:
        reason = exc.reason
    except Exception:
        reason = "internal_error"
    try:
        _emit(ops, {"phase": "bootstrap_failure", "reason": reason})
    except Exception:
        pass
    return EXIT_BOOTSTRAP_FAILURE


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    ops = SystemOps()
    try:
        require_isolated_invocation(
            list(getattr(sys, "orig_argv", [])), arguments,
            sys.flags.isolated, sys.flags.no_site,
        )
    except BootstrapError as exc:
        try:
            _emit(ops, {"phase": "bootstrap_failure", "reason": exc.reason})
        except Exception:
            pass
        return EXIT_BOOTSTRAP_FAILURE
    return run(arguments, ops)


if __name__ == "__main__":
    raise SystemExit(main())
