from __future__ import annotations

import ctypes
from io import BytesIO
import os
from pathlib import Path
import stat
from types import SimpleNamespace

import pytest

from src.bond_pilot.contracts import PilotError
from src.bond_pilot import _secure_local_fs as secure_fs


class _ClosingBytesIO(BytesIO):
    def __init__(self, value: bytes, close_handle) -> None:
        super().__init__(value)
        self._close_handle = close_handle

    def close(self) -> None:
        if not self.closed:
            self._close_handle()
        super().close()


class _WindowsApiFake:
    def __init__(self, *, drive_type: int = secure_fs.DRIVE_FIXED, root_path: str = r"\Device\HarddiskVolume7") -> None:
        self.drive = drive_type
        self.root_path = root_path
        self.protocol = 0
        self.records: list[tuple[object, ...]] = []
        self.closed: list[str] = []
        self.closed_fds: list[int] = []
        self.transferred: dict[int, str] = {}
        self.next_fd = 100
        self.infos: dict[str, secure_fs._WindowsFileInfo] = {}
        self.payloads: dict[str, bytes] = {}

    def get_drive_type(self, root: str) -> int:
        self.records.append(("drive", root))
        return self.drive

    def open_root(self, root: str, *, desired_access: int, share_access: int, flags: int) -> str:
        self.records.append(("root", root, desired_access, share_access, flags))
        self.infos["root"] = secure_fs._WindowsFileInfo(7, secure_fs.FILE_ATTRIBUTE_DIRECTORY, 0, (7, 1, stat.S_IFDIR, 0, 10, 11))
        return "root"

    def final_nt_path(self, handle: str) -> str:
        return self.root_path

    def remote_protocol(self, handle: str) -> int:
        return self.protocol

    def query_info(self, handle: str) -> secure_fs._WindowsFileInfo:
        return self.infos[handle]

    def nt_create(self, parent: str, name: str, *, desired_access: int, share_access: int, disposition: int, options: int, attributes: int) -> str:
        handle = f"{parent}/{name}"
        self.records.append(("nt_create", parent, name, desired_access, share_access, disposition, options, attributes))
        directory = bool(options & secure_fs.FILE_DIRECTORY_FILE)
        mode = stat.S_IFDIR if directory else stat.S_IFREG
        attrs = secure_fs.FILE_ATTRIBUTE_DIRECTORY if directory else secure_fs.FILE_ATTRIBUTE_NORMAL
        self.infos.setdefault(handle, secure_fs._WindowsFileInfo(7, attrs, 0, (7, len(self.infos) + 1, mode, len(self.payloads.get(name, b"payload")), 20, 21)))
        return handle

    def transfer_to_fd(self, handle: str, mode: str) -> int:
        fd = self.next_fd
        self.next_fd += 1
        self.transferred[fd] = handle
        self.records.append(("transfer", handle, mode, fd))
        return fd

    def fdopen(self, fd: int, mode: str):
        handle = self.transferred[fd]
        self.records.append(("fdopen", fd, mode))
        return _ClosingBytesIO(self.payloads.get(handle.rsplit("/", 1)[-1], b"payload"), lambda: self.close_fd(fd))

    def close_fd(self, fd: int) -> None:
        if fd in self.closed_fds:
            raise AssertionError(f"double fd close: {fd}")
        self.closed_fds.append(fd)

    def enumerate(self, handle: str) -> tuple[str, ...]:
        self.records.append(("enumerate", handle))
        return (".", "..", "child.json")

    def publish(self, file_handle: str, directory_handle: str, final_name: str, *, replace: bool) -> None:
        self.records.append(("publish", file_handle, directory_handle, final_name, replace))

    def flush(self, handle: str) -> None:
        self.records.append(("flush", handle))

    def close(self, handle: str) -> None:
        if handle in self.closed:
            raise AssertionError(f"double close: {handle}")
        self.closed.append(handle)


def test_windows_remote_protocol_reads_protocol_field_not_structure_size() -> None:
    def get_info(_handle, _info_class, info, _size) -> bool:
        result = ctypes.cast(info, ctypes.POINTER(secure_fs._FILE_REMOTE_PROTOCOL_INFO_STRUCT)).contents
        result.StructureVersion = 2
        result.StructureSize = ctypes.sizeof(secure_fs._FILE_REMOTE_PROTOCOL_INFO_STRUCT)
        result.Protocol = 0x00020000
        return True

    api = object.__new__(secure_fs._WindowsApi)
    api.kernel32 = SimpleNamespace(GetFileInformationByHandleEx=get_info)

    assert api.remote_protocol(1) == 0x00020000


def test_windows_native_structures_match_header_layout() -> None:
    remote = secure_fs._FILE_REMOTE_PROTOCOL_INFO_STRUCT
    standard = secure_fs._FILE_STANDARD_INFO_STRUCT

    assert remote.StructureVersion.offset == 0
    assert remote.StructureSize.offset == 2
    assert remote.Protocol.offset == 4
    assert ctypes.sizeof(remote) == 180
    assert standard.DeletePending.offset == 20
    assert standard.Directory.offset == 21
    assert ctypes.sizeof(standard) == 24


def test_windows_publish_one_character_uses_required_native_rename_length() -> None:
    observed: dict[str, object] = {}

    def set_information(_file, _iosb, buffer, length, info_class) -> int:
        observed["buffer"] = bytes(buffer)
        observed["length"] = length
        observed["info_class"] = info_class
        return 0

    api = object.__new__(secure_fs._WindowsApi)
    api.ntdll = SimpleNamespace(NtSetInformationFile=set_information)
    api.publish(101, 202, "x", replace=False)

    header = secure_fs._FILE_RENAME_INFORMATION_STRUCT.from_buffer_copy(observed["buffer"])
    assert observed["length"] == ctypes.sizeof(secure_fs._FILE_RENAME_INFORMATION_STRUCT) + 2
    assert observed["info_class"] == secure_fs._FILE_RENAME_INFORMATION
    assert header.ReplaceIfExists == 0
    assert header.RootDirectory == 202
    assert header.FileNameLength == 2


def test_windows_publish_translates_rename_buffer_construction_error(monkeypatch: pytest.MonkeyPatch) -> None:
    api = object.__new__(secure_fs._WindowsApi)
    api.ntdll = SimpleNamespace()
    monkeypatch.setattr(secure_fs.ctypes, "create_string_buffer", lambda _size: (_ for _ in ()).throw(ValueError("bad buffer")))

    with pytest.raises(OSError, match="rename information"):
        api.publish(101, 202, "x", replace=False)


def test_windows_traversal_uses_exact_relative_no_follow_flags_and_close_ownership() -> None:
    api = _WindowsApiFake()
    capability = secure_fs._WindowsBackend(api).open_file(Path(r"C:\safe\input.json"), error_code="unsafe")

    assert capability.read_all(max_bytes=1024) == b"payload"
    capability.close()

    root = next(record for record in api.records if record[0] == "root")
    assert root[2] & secure_fs.SYNCHRONIZE
    assert root[3] == secure_fs.FILE_SHARE_READ | secure_fs.FILE_SHARE_WRITE
    assert root[4] == secure_fs.FILE_FLAG_BACKUP_SEMANTICS | secure_fs.FILE_FLAG_OPEN_REPARSE_POINT
    creates = [record for record in api.records if record[0] == "nt_create"]
    ancestor, final = creates
    assert ancestor[3] & secure_fs.SYNCHRONIZE
    assert ancestor[4] == secure_fs.FILE_SHARE_READ | secure_fs.FILE_SHARE_WRITE
    assert ancestor[6] == secure_fs.FILE_DIRECTORY_FILE | secure_fs.FILE_SYNCHRONOUS_IO_NONALERT | secure_fs.FILE_OPEN_REPARSE_POINT
    assert ancestor[7] == secure_fs.OBJ_CASE_INSENSITIVE | secure_fs.OBJ_DONT_REPARSE
    assert final[3] & secure_fs.SYNCHRONIZE
    assert final[4] == secure_fs.FILE_SHARE_READ
    assert final[6] == secure_fs.FILE_NON_DIRECTORY_FILE | secure_fs.FILE_SYNCHRONOUS_IO_NONALERT | secure_fs.FILE_OPEN_REPARSE_POINT
    assert api.closed == ["root/safe", "root"]
    assert api.closed_fds == [100]


def test_secure_file_exposes_seekable_file_object_adapter_for_archive_readers(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"payload")
    capability = secure_fs.secure_open_file(source, error_code="unsafe")
    assert capability.seekable() is True
    assert capability.read(1) == b"p"
    assert capability.tell() == 1
    assert capability.seek(0) == 0
    assert capability.fileno() >= 0
    capability.close()
    assert capability.closed is True
    with pytest.raises(ValueError):
        capability.tell()


def test_windows_open_osfhandle_failure_closes_native_handle_once() -> None:
    api = _WindowsApiFake()

    def transfer(handle: str, mode: str) -> int:
        api.records.append(("transfer", handle, mode))
        raise OSError("transfer failed")

    api.transfer_to_fd = transfer

    with pytest.raises(PilotError, match="unsafe"):
        secure_fs._WindowsBackend(api).open_file(Path(r"C:\safe\input.json"), error_code="unsafe")

    assert api.closed == ["root/safe/input.json", "root/safe", "root"]
    assert api.closed_fds == []
    assert ("transfer", "root/safe/input.json", "rb") in api.records


def test_windows_fdopen_failure_closes_only_transferred_crt_fd() -> None:
    api = _WindowsApiFake()
    api.fdopen = lambda *_args: (_ for _ in ()).throw(OSError("fdopen failed"))

    with pytest.raises(PilotError, match="unsafe"):
        secure_fs._WindowsBackend(api).open_file(Path(r"C:\safe\input.json"), error_code="unsafe")

    assert api.closed == ["root/safe", "root"]
    assert api.closed_fds == [100]


@pytest.mark.parametrize(
    ("name", "operation"),
    [
        ("input.json", lambda directory: directory.open_file("input.json", error_code="unsafe")),
        (".pending", lambda directory: directory.create_file(".pending", error_code="unsafe")),
    ],
)
def test_windows_relative_fdopen_failure_never_closes_transferred_native_handle(name: str, operation) -> None:
    api = _WindowsApiFake()
    directory = secure_fs._WindowsBackend(api).open_dir(Path(r"C:\safe"), error_code="unsafe")
    api.fdopen = lambda *_args: (_ for _ in ()).throw(OSError("fdopen failed"))
    try:
        with pytest.raises(PilotError, match="unsafe"):
            operation(directory)
        assert f"root/safe/{name}" not in api.closed
        assert api.closed_fds == [100]
    finally:
        directory.close()
    assert api.closed == ["root/safe", "root"]


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (lambda api: setattr(api, "drive", secure_fs.DRIVE_REMOTE), "unsafe"),
        (lambda api: setattr(api, "root_path", r"\Device\Mup\server\share"), "unsafe"),
        (lambda api: setattr(api, "protocol", 0x00020000), "unsafe"),
    ],
)
def test_windows_rejects_mapped_or_remote_roots(mutate, expected: str) -> None:
    api = _WindowsApiFake()
    mutate(api)
    with pytest.raises(PilotError, match=expected):
        secure_fs._WindowsBackend(api).open_file(Path(r"C:\safe\input.json"), error_code="unsafe")
    assert len(api.closed) == len(set(api.closed))


def test_windows_rejects_reparse_root_before_relative_traversal() -> None:
    api = _WindowsApiFake()
    original = api.open_root

    def open_root(*args, **kwargs):
        handle = original(*args, **kwargs)
        info = api.infos[handle]
        api.infos[handle] = secure_fs._WindowsFileInfo(
            info.volume_serial,
            info.attributes | secure_fs.FILE_ATTRIBUTE_REPARSE_POINT,
            1,
            info.identity,
        )
        return handle

    api.open_root = open_root
    with pytest.raises(PilotError, match="unsafe"):
        secure_fs._WindowsBackend(api).open_file(Path(r"C:\safe\input.json"), error_code="unsafe")

    assert api.closed == ["root"]
    assert not [record for record in api.records if record[0] == "nt_create"]


@pytest.mark.parametrize("failure", ["volume", "reparse"])
def test_windows_rejects_component_volume_mismatch_or_reparse_and_closes_once(failure: str) -> None:
    api = _WindowsApiFake()
    original = api.nt_create

    def nt_create(*args, **kwargs):
        handle = original(*args, **kwargs)
        if handle.endswith("/safe"):
            info = api.infos[handle]
            api.infos[handle] = secure_fs._WindowsFileInfo(
                9 if failure == "volume" else info.volume_serial,
                info.attributes | (secure_fs.FILE_ATTRIBUTE_REPARSE_POINT if failure == "reparse" else 0),
                1 if failure == "reparse" else 0,
                info.identity,
            )
        return handle

    api.nt_create = nt_create
    with pytest.raises(PilotError, match="unsafe"):
        secure_fs._WindowsBackend(api).open_file(Path(r"C:\safe\input.json"), error_code="unsafe")
    assert len(api.closed) == len(set(api.closed))
    assert set(api.closed) == {"root/safe", "root"}


def test_windows_directory_enumeration_and_publish_stay_relative_and_no_replace() -> None:
    api = _WindowsApiFake()
    directory = secure_fs._WindowsBackend(api).open_dir(Path(r"C:\safe"), error_code="unsafe")
    assert directory.enumerate() == ("child.json",)
    created = directory.create_file(".temp", error_code="unsafe")
    created.write(b"new")
    directory.publish_no_replace(created, "final.json", error_code="unsafe")
    assert ("publish", "root/safe/.temp", "root/safe", "final.json", False) in api.records
    created.close()
    directory.close()
    assert len(api.closed) == len(set(api.closed))


@pytest.mark.parametrize(
    "action",
    [
        lambda directory, _created: directory.enumerate(),
        lambda directory, _created: directory.open_file("child.json", error_code="unsafe"),
        lambda directory, _created: directory.create_file("child.json", error_code="unsafe"),
        lambda directory, _created: directory.create_dir("child", error_code="unsafe"),
        lambda directory, created: directory.publish_no_replace(created, "child.json", error_code="unsafe"),
    ],
)
def test_closed_directory_never_reuses_stale_handle_or_calls_backend(action) -> None:
    api = _WindowsApiFake()
    directory = secure_fs._WindowsBackend(api).open_dir(Path(r"C:\safe"), error_code="unsafe")
    created = directory.create_file(".pending", error_code="unsafe")
    directory.close()
    api.infos["root/safe"] = secure_fs._WindowsFileInfo(7, secure_fs.FILE_ATTRIBUTE_DIRECTORY, 0, (7, 999, stat.S_IFDIR, 0, 20, 21))
    records_before = list(api.records)
    try:
        with pytest.raises(ValueError, match="closed capability"):
            action(directory, created)
        assert api.records == records_before
    finally:
        created.close()


class _PosixApiFake:
    def __init__(self, *, mismatch: str | None = None) -> None:
        self.mismatch = mismatch
        self.next_fd = 10
        self.records: list[tuple[object, ...]] = []
        self.closed: list[int] = []
        self.names: dict[int, str] = {}

    def open(self, name, flags, mode=0o777, *, dir_fd=None):
        fd = self.next_fd
        self.next_fd += 1
        self.names[fd] = str(name)
        self.records.append(("open", name, flags, mode, dir_fd, fd))
        return fd

    def fstat(self, fd: int):
        name = self.names[fd]
        is_dir = name == "/" or name == "safe"
        dev = 2 if self.mismatch == name else 1
        return SimpleNamespace(st_dev=dev, st_ino=fd, st_mode=stat.S_IFDIR if is_dir else stat.S_IFREG, st_size=7, st_mtime_ns=20, st_ctime_ns=21)

    def fdopen(self, fd: int, mode: str, closefd: bool = True):
        return _ClosingBytesIO(b"payload", lambda: self.close(fd))

    def close(self, fd: int) -> None:
        if fd in self.closed:
            raise AssertionError(f"double close: {fd}")
        self.closed.append(fd)

    def listdir(self, fd: int):
        return [".", "..", "child.json"]

    def mkdir(self, name: str, mode: int = 0o777, *, dir_fd: int):
        self.records.append(("mkdir", name, mode, dir_fd))

    def proc_fd_source(self, source_fd: int) -> str:
        self.records.append(("proc_fd_source", source_fd))
        return f"/proc/self/fd/{source_fd}"

    def linkat(self, source_dir_fd: int, source: str, dst_dir_fd: int, target: str, flags: int):
        self.records.append(("linkat", source_dir_fd, source, dst_dir_fd, target, flags))

    def fsync(self, fd: int):
        self.records.append(("fsync", fd))


def test_posix_traversal_pins_root_device_uses_dir_fd_and_cleans_up() -> None:
    api = _PosixApiFake()
    capability = secure_fs._PosixBackend(api).open_file(Path("/safe/input.json"), error_code="unsafe")
    assert capability.read_all(max_bytes=1024) == b"payload"
    capability.close()
    opens = [record for record in api.records if record[0] == "open"]
    assert opens[0][1] == "/"
    assert opens[1][4] == opens[0][5]
    assert opens[1][2] & secure_fs.O_NOFOLLOW and opens[1][2] & secure_fs.O_DIRECTORY and opens[1][2] & secure_fs.O_CLOEXEC
    assert opens[2][4] == opens[1][5]
    assert opens[2][2] & secure_fs.O_NOFOLLOW and opens[2][2] & secure_fs.O_NONBLOCK and opens[2][2] & secure_fs.O_CLOEXEC
    assert api.closed == [12, 11, 10]


@pytest.mark.parametrize("mismatch", ["safe", "input.json"])
def test_posix_rejects_ancestor_or_final_device_mismatch_and_closes_all(mismatch: str) -> None:
    api = _PosixApiFake(mismatch=mismatch)
    with pytest.raises(PilotError, match="unsafe"):
        secure_fs._PosixBackend(api).open_file(Path("/safe/input.json"), error_code="unsafe")
    assert len(api.closed) == len(set(api.closed))
    assert api.closed[-1] == 10


def test_posix_publish_uses_anonymous_retained_fd_and_ignores_name_injection() -> None:
    api = _PosixApiFake()
    directory = secure_fs._PosixBackend(api).open_dir(Path("/safe"), error_code="unsafe")
    created = directory.create_file(".temp", error_code="unsafe")
    api.names[created.native_handle] = "attacker-replacement"
    directory.publish_no_replace(created, "final.json", error_code="unsafe")
    parent_fd = directory.native_handle
    opens = [record for record in api.records if record[0] == "open"]
    assert created.name is None
    assert opens[-1][1] == "."
    assert opens[-1][2] & secure_fs.O_TMPFILE
    assert not opens[-1][2] & os.O_EXCL
    assert ("proc_fd_source", created.native_handle) in api.records
    assert ("linkat", secure_fs.AT_FDCWD, f"/proc/self/fd/{created.native_handle}", parent_fd, "final.json", secure_fs.AT_SYMLINK_FOLLOW) in api.records
    assert not [record for record in api.records if record[0] in {"link", "unlink"}]
    assert ("fsync", parent_fd) in api.records
    created.close()
    directory.close()


def test_posix_publish_fails_closed_when_verified_proc_descriptor_is_unavailable() -> None:
    api = _PosixApiFake()
    directory = secure_fs._PosixBackend(api).open_dir(Path("/safe"), error_code="unsafe")
    created = directory.create_file(".temp", error_code="unsafe")
    api.proc_fd_source = lambda _fd: (_ for _ in ()).throw(OSError("proc unavailable"))
    directory._backend._proc_fd_source = api.proc_fd_source
    try:
        with pytest.raises(PilotError, match="unsafe"):
            directory.publish_no_replace(created, "final.json", error_code="unsafe")
        assert not [record for record in api.records if record[0] == "linkat"]
    finally:
        created.close()
        directory.close()


def test_posix_create_dir_closes_new_handle_when_validation_fails() -> None:
    api = _PosixApiFake()
    directory = secure_fs._PosixBackend(api).open_dir(Path("/safe"), error_code="unsafe")
    try:
        with pytest.raises(PilotError, match="unsafe"):
            directory.create_dir("child", error_code="unsafe")
        assert api.closed == [12]
    finally:
        directory.close()


def test_windows_lexical_validation_rejects_ambiguous_paths() -> None:
    invalid = [
        r"\\server\share\x", r"\\?\C:\x", "file:///C:/x", "C:/x", "C:\\x\0bad",
        r"C:\safe\..\x", r"C:\safe\.\x", r"C:\safe\x:ads", "C:\\safe\\trail. ", "relative\\x",
    ]
    for value in invalid:
        with pytest.raises(PilotError, match="unsafe"):
            secure_fs.lexical_local_path(value, error_code="unsafe", platform="win32")


@pytest.mark.skipif(os.name != "nt", reason="real Windows handle-sharing proof")
def test_real_windows_capability_blocks_ancestor_rename_and_keeps_original_bytes(tmp_path: Path) -> None:
    ancestor = tmp_path / "pinned"
    ancestor.mkdir()
    source = ancestor / "control.bin"
    source.write_bytes(b"original")
    moved = tmp_path / "moved"

    capability = secure_fs.secure_open_file(source, error_code="unsafe")
    try:
        with pytest.raises(OSError):
            ancestor.rename(moved)
        assert capability.read_all(max_bytes=32) == b"original"
    finally:
        capability.close()


@pytest.mark.skipif(os.name != "nt", reason="real Windows native enumeration and publish proof")
def test_real_windows_directory_enumeration_and_publish_never_replace(tmp_path: Path) -> None:
    (tmp_path / "listed.json").write_bytes(b"listed")
    directory = secure_fs.secure_open_dir(tmp_path, error_code="unsafe")
    try:
        assert "listed.json" in directory.enumerate()
        created = directory.create_file(".pending", error_code="unsafe")
        try:
            created.write(b"new")
            directory.publish_no_replace(created, "x", error_code="unsafe")
        finally:
            created.close()
        assert (tmp_path / "x").read_bytes() == b"new"
        assert not (tmp_path / ".pending").exists()

        collision = directory.create_file(".collision", error_code="unsafe")
        try:
            collision.write(b"collision")
            (tmp_path / "final.json").write_bytes(b"old")
            with pytest.raises(PilotError, match="unsafe"):
                directory.publish_no_replace(collision, "final.json", error_code="unsafe")
            assert (tmp_path / "final.json").read_bytes() == b"old"
        finally:
            collision.close()
        assert (tmp_path / ".collision").read_bytes() == b"collision"
    finally:
        directory.close()


@pytest.mark.skipif(os.name == "nt" or not secure_fs._HAS_O_TMPFILE, reason="real POSIX O_TMPFILE proof")
def test_real_posix_publish_uses_anonymous_fd_and_never_uses_requested_temp_name(tmp_path: Path) -> None:
    directory = secure_fs.secure_open_dir(tmp_path, error_code="unsafe")
    try:
        try:
            created = directory.create_file("attacker-name", error_code="unsafe")
        except PilotError:
            pytest.skip("O_TMPFILE unavailable on active filesystem")
        try:
            created.write(b"original")
            (tmp_path / "attacker-name").write_bytes(b"attacker")
            directory.publish_no_replace(created, "final.json", error_code="unsafe")
        finally:
            created.close()
        assert (tmp_path / "final.json").read_bytes() == b"original"
        assert (tmp_path / "attacker-name").read_bytes() == b"attacker"
    finally:
        directory.close()


@pytest.mark.skipif(os.name == "nt", reason="real POSIX-only no-follow and FIFO proof")
def test_real_posix_rejects_symlink_and_fifo(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"ok")
    link = tmp_path / "link"
    link.symlink_to(source)
    with pytest.raises(PilotError, match="unsafe"):
        secure_fs.secure_open_file(link, error_code="unsafe")
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(PilotError, match="unsafe"):
        secure_fs.secure_open_file(fifo, error_code="unsafe")
