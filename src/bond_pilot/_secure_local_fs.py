"""Handle-anchored local filesystem capabilities for private pilot inputs and outputs."""

from __future__ import annotations

from dataclasses import dataclass
import ctypes
from ctypes import wintypes
import os
from pathlib import Path
import re
import stat
from typing import BinaryIO, Callable, Iterator, Protocol
from urllib.parse import urlsplit

from .contracts import PilotError


DRIVE_UNKNOWN = 0
DRIVE_NO_ROOT_DIR = 1
DRIVE_REMOVABLE = 2
DRIVE_FIXED = 3
DRIVE_REMOTE = 4
DRIVE_CDROM = 5
DRIVE_RAMDISK = 6

FILE_READ_DATA = 0x0001
FILE_LIST_DIRECTORY = 0x0001
FILE_WRITE_DATA = 0x0002
FILE_TRAVERSE = 0x0020
FILE_READ_ATTRIBUTES = 0x0080
DELETE = 0x00010000
SYNCHRONIZE = 0x00100000
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
FILE_SHARE_DELETE = 0x00000004
FILE_OPEN = 1
FILE_CREATE = 2
FILE_DIRECTORY_FILE = 0x00000001
FILE_SYNCHRONOUS_IO_NONALERT = 0x00000020
FILE_NON_DIRECTORY_FILE = 0x00000040
FILE_OPEN_REPARSE_POINT = 0x00200000
FILE_ATTRIBUTE_DIRECTORY = 0x00000010
FILE_ATTRIBUTE_NORMAL = 0x00000080
FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
OBJ_CASE_INSENSITIVE = 0x00000040
OBJ_DONT_REPARSE = 0x00001000

_OPEN_EXISTING = 3
_FILE_NAMES_INFORMATION = 12
_FILE_RENAME_INFORMATION = 10
_FILE_ATTRIBUTE_TAG_INFO = 9
_FILE_REMOTE_PROTOCOL_INFO = 13
_FILE_ID_INFO = 18
_STATUS_NO_MORE_FILES = 0x80000006
_REMOTE_DRIVES = frozenset({DRIVE_UNKNOWN, DRIVE_NO_ROOT_DIR, DRIVE_REMOTE})
_LOCAL_NT_PREFIXES = (r"\device\harddiskvolume", r"\device\cdrom", r"\device\ramdisk")
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:\\")
O_DIRECTORY = getattr(os, "O_DIRECTORY", 0x00010000)
O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0x00020000)
O_CLOEXEC = getattr(os, "O_CLOEXEC", 0x00080000)
O_NONBLOCK = getattr(os, "O_NONBLOCK", 0x00000004)
O_TMPFILE = getattr(os, "O_TMPFILE", 0o20200000)
_HAS_O_TMPFILE = hasattr(os, "O_TMPFILE")
AT_FDCWD = -100
AT_SYMLINK_FOLLOW = 0x400


def _error(code: str, cause: BaseException | None = None) -> PilotError:
    error = PilotError(code)
    if cause is not None:
        error.__cause__ = cause
    return error


def _proc_fd_source(source_fd: int) -> str:
    source = f"/proc/self/fd/{source_fd}"
    try:
        source_status = os.stat(source)
        handle_status = os.fstat(source_fd)
    except OSError as exc:
        raise OSError("verified proc descriptor source unavailable") from exc
    if (source_status.st_dev, source_status.st_ino) != (handle_status.st_dev, handle_status.st_ino):
        raise OSError("proc descriptor source identity mismatch")
    return source


def _linkat(source_dir_fd: int, source: str, dst_dir_fd: int, target: str, flags: int) -> None:
    if os.name == "nt":
        raise OSError("linkat unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        linkat = libc.linkat
    except AttributeError as exc:
        raise OSError("linkat unavailable") from exc
    linkat.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_int]
    linkat.restype = ctypes.c_int
    if linkat(source_dir_fd, os.fsencode(source), dst_dir_fd, os.fsencode(target), flags) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _single_name(name: str, error_code: str) -> str:
    if not isinstance(name, str) or not name or "\0" in name or name in {".", ".."} or any(character in name for character in ("/", "\\", ":")):
        raise PilotError(error_code)
    if name[-1] in {".", " "}:
        raise PilotError(error_code)
    return name


def lexical_local_path(value: str | Path, *, error_code: str, platform: str | None = None) -> Path:
    """Validate syntax only and return an absolute lexical local path."""
    locator = str(value)
    active_platform = platform or ("win32" if os.name == "nt" else "posix")
    if "\0" in locator:
        raise PilotError(error_code)
    if active_platform == "win32":
        if not _WINDOWS_ABSOLUTE.match(locator) or "/" in locator or locator.startswith("\\"):
            raise PilotError(error_code)
        components = locator[3:].split("\\") if len(locator) > 3 else []
        if any(not component or component in {".", ".."} or ":" in component or component[-1] in {".", " "} for component in components):
            raise PilotError(error_code)
        return Path(locator)
    normalized = locator.strip()
    if normalized.startswith(("\\", "//")) or urlsplit(normalized).scheme:
        raise PilotError(error_code)
    absolute = locator if os.path.isabs(locator) else os.path.abspath(locator)
    components = absolute.split(os.sep)[1:]
    if any(component in {".", ".."} for component in components):
        raise PilotError(error_code)
    return Path(absolute)


@dataclass(frozen=True)
class _WindowsFileInfo:
    volume_serial: int
    attributes: int
    reparse_tag: int
    identity: tuple[object, ...]


class _FileApi(Protocol):
    def close(self, handle: object) -> None: ...


class SecureFile:
    """A regular file plus every retained ancestor capability needed for its lifetime."""

    def __init__(
        self,
        path: Path,
        stream: BinaryIO,
        native_handle: object,
        identity: tuple[object, ...],
        identity_reader: Callable[[object], tuple[object, ...]],
        parent_handles: list[tuple[_FileApi, object]],
        error_code: str,
    ) -> None:
        self.path = path
        self._stream = stream
        self.native_handle = native_handle
        self._identity = identity
        self._identity_reader = identity_reader
        self._parents = parent_handles
        self._error_code = error_code
        self._closed = False

    def __enter__(self) -> SecureFile:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed capability")
        return self._stream.read(size)

    def iter_chunks(self, chunk_size: int, *, max_bytes: int) -> Iterator[bytes]:
        if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0 or max_bytes < 0:
            raise PilotError(self._error_code)
        total = 0
        while chunk := self.read(min(chunk_size, max_bytes - total + 1)):
            total += len(chunk)
            if total > max_bytes:
                raise PilotError(self._error_code)
            yield chunk
        self.verify_unchanged(expected_size=total)

    def read_all(self, *, max_bytes: int, too_large_code: str | None = None) -> bytes:
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 0:
            raise PilotError(self._error_code)
        expected_size = int(self._identity[3])
        if expected_size > max_bytes:
            raise PilotError(too_large_code or self._error_code)
        raw = self.read(max_bytes + 1)
        if len(raw) > max_bytes:
            raise PilotError(too_large_code or self._error_code)
        self.verify_unchanged(expected_size=len(raw))
        return raw

    def verify_unchanged(self, *, expected_size: int) -> None:
        try:
            after = self._identity_reader(self.native_handle)
        except OSError as exc:
            raise _error(self._error_code, exc)
        if after != self._identity or expected_size != int(self._identity[3]):
            raise PilotError(self._error_code)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._stream.close()
        finally:
            for api, handle in reversed(self._parents):
                api.close(handle)
            self._parents.clear()


class CreatedFile:
    def __init__(self, name: str | None, stream: BinaryIO, native_handle: object, close_parents: list[tuple[_FileApi, object]] | None = None) -> None:
        self.name = name
        self._stream = stream
        self.native_handle = native_handle
        self._parents = close_parents or []
        self._closed = False

    def write(self, value: bytes) -> int:
        return self._stream.write(value)

    def flush(self) -> None:
        self._stream.flush()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._stream.close()
        finally:
            for api, handle in reversed(self._parents):
                api.close(handle)
            self._parents.clear()

    def __enter__(self) -> CreatedFile:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class SecureDirectory:
    def __init__(self, path: Path, backend: object, native_handle: object, handles: list[object], root_device: int, error_code: str) -> None:
        self.path = path
        self._backend = backend
        self.native_handle = native_handle
        self._handles = handles
        self.root_device = root_device
        self.error_code = error_code
        self._closed = False

    def __enter__(self) -> SecureDirectory:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _require_open(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed capability")

    def enumerate(self) -> tuple[str, ...]:
        self._require_open()
        return self._backend.enumerate(self)

    def open_file(self, name: str, *, error_code: str | None = None) -> SecureFile:
        self._require_open()
        return self._backend.open_relative_file(self, _single_name(name, error_code or self.error_code), error_code=error_code or self.error_code)

    def create_file(self, name: str, *, error_code: str | None = None) -> CreatedFile:
        self._require_open()
        return self._backend.create_file(self, _single_name(name, error_code or self.error_code), error_code=error_code or self.error_code)

    def create_dir(self, name: str, *, error_code: str | None = None) -> SecureDirectory:
        self._require_open()
        return self._backend.create_dir(self, _single_name(name, error_code or self.error_code), error_code=error_code or self.error_code)

    def publish_no_replace(self, created: CreatedFile, final_name: str, *, error_code: str | None = None) -> None:
        self._require_open()
        self._backend.publish_no_replace(self, created, _single_name(final_name, error_code or self.error_code), error_code=error_code or self.error_code)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for handle in reversed(self._handles):
            self._backend.api.close(handle)
        self._handles.clear()


class _PosixBackend:
    def __init__(self, api: object = os) -> None:
        self.api = api
        self._proc_fd_source = getattr(api, "proc_fd_source", _proc_fd_source)
        self._linkat = getattr(api, "linkat", _linkat)

    @staticmethod
    def _identity(status: os.stat_result) -> tuple[object, ...]:
        return (status.st_dev, status.st_ino, stat.S_IFMT(status.st_mode), status.st_size, status.st_mtime_ns, status.st_ctime_ns)

    def _status(self, handle: int, *, root_device: int, directory: bool, error_code: str) -> os.stat_result:
        status = self.api.fstat(handle)
        expected = stat.S_ISDIR(status.st_mode) if directory else stat.S_ISREG(status.st_mode)
        if status.st_dev != root_device or not expected:
            raise PilotError(error_code)
        return status

    def _root(self, error_code: str) -> tuple[int, int]:
        flags = os.O_RDONLY | O_DIRECTORY | O_CLOEXEC
        handle = self.api.open("/", flags)
        try:
            status = self.api.fstat(handle)
            if not stat.S_ISDIR(status.st_mode):
                raise PilotError(error_code)
            return handle, status.st_dev
        except Exception:
            self.api.close(handle)
            raise

    def _open_directory_chain(self, path: Path, error_code: str) -> tuple[list[int], int]:
        root, root_device = self._root(error_code)
        handles = [root]
        try:
            for component in path.parts[1:]:
                handle = self.api.open(
                    component,
                    os.O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC,
                    dir_fd=handles[-1],
                )
                handles.append(handle)
                self._status(handle, root_device=root_device, directory=True, error_code=error_code)
            return handles, root_device
        except Exception as exc:
            for handle in reversed(handles):
                self.api.close(handle)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def open_file(self, path: Path, *, error_code: str) -> SecureFile:
        handles, root_device = self._open_directory_chain(path.parent, error_code)
        final: int | None = None
        try:
            final = self.api.open(path.name, os.O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK, dir_fd=handles[-1])
            status = self._status(final, root_device=root_device, directory=False, error_code=error_code)
            native_handle = final
            stream = self.api.fdopen(final, "rb", closefd=True)
            final = None
            return SecureFile(path, stream, native_handle, self._identity(status), lambda handle: self._identity(self.api.fstat(handle)), [(self.api, handle) for handle in handles], error_code)
        except Exception as exc:
            if final is not None:
                self.api.close(final)
            for handle in reversed(handles):
                self.api.close(handle)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def open_dir(self, path: Path, *, error_code: str) -> SecureDirectory:
        handles, root_device = self._open_directory_chain(path, error_code)
        return SecureDirectory(path, self, handles[-1], handles, root_device, error_code)

    def enumerate(self, directory: SecureDirectory) -> tuple[str, ...]:
        try:
            return tuple(name for name in self.api.listdir(directory.native_handle) if name not in {".", ".."})
        except OSError as exc:
            raise _error(directory.error_code, exc)

    def open_relative_file(self, directory: SecureDirectory, name: str, *, error_code: str) -> SecureFile:
        final: int | None = None
        try:
            final = self.api.open(name, os.O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK, dir_fd=directory.native_handle)
            status = self._status(final, root_device=directory.root_device, directory=False, error_code=error_code)
            native_handle = final
            stream = self.api.fdopen(final, "rb", closefd=True)
            final = None
            return SecureFile(directory.path / name, stream, native_handle, self._identity(status), lambda handle: self._identity(self.api.fstat(handle)), [], error_code)
        except Exception as exc:
            if final is not None:
                self.api.close(final)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def create_file(self, directory: SecureDirectory, name: str, *, error_code: str) -> CreatedFile:
        handle: int | None = None
        try:
            if self.api is os and not _HAS_O_TMPFILE:
                raise PilotError(error_code)
            handle = self.api.open(".", os.O_RDWR | O_TMPFILE | O_CLOEXEC, 0o600, dir_fd=directory.native_handle)
            status = self._status(handle, root_device=directory.root_device, directory=False, error_code=error_code)
            del status
            native_handle = handle
            stream = self.api.fdopen(handle, "w+b", closefd=True)
            handle = None
            return CreatedFile(None, stream, native_handle)
        except Exception as exc:
            if handle is not None:
                self.api.close(handle)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def create_dir(self, directory: SecureDirectory, name: str, *, error_code: str) -> SecureDirectory:
        handle: int | None = None
        try:
            self.api.mkdir(name, 0o700, dir_fd=directory.native_handle)
            handle = self.api.open(name, os.O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC, dir_fd=directory.native_handle)
            self._status(handle, root_device=directory.root_device, directory=True, error_code=error_code)
            result = SecureDirectory(directory.path / name, self, handle, [handle], directory.root_device, error_code)
            handle = None
            return result
        except Exception as exc:
            if handle is not None:
                self.api.close(handle)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def publish_no_replace(self, directory: SecureDirectory, created: CreatedFile, final_name: str, *, error_code: str) -> None:
        try:
            created.flush()
            self.api.fsync(created.native_handle)
            source = self._proc_fd_source(created.native_handle)
            self._linkat(AT_FDCWD, source, directory.native_handle, final_name, AT_SYMLINK_FOLLOW)
            self.api.fsync(directory.native_handle)
        except OSError as exc:
            raise _error(error_code, exc)


class _IO_STATUS_UNION(ctypes.Union):
    _fields_ = [("Status", wintypes.LONG), ("Pointer", ctypes.c_void_p)]


class _IO_STATUS_BLOCK(ctypes.Structure):
    _anonymous_ = ("u",)
    _fields_ = [("u", _IO_STATUS_UNION), ("Information", ctypes.c_size_t)]


class _UNICODE_STRING(ctypes.Structure):
    _fields_ = [("Length", wintypes.USHORT), ("MaximumLength", wintypes.USHORT), ("Buffer", wintypes.LPWSTR)]


class _OBJECT_ATTRIBUTES(ctypes.Structure):
    _fields_ = [
        ("Length", wintypes.ULONG),
        ("RootDirectory", wintypes.HANDLE),
        ("ObjectName", ctypes.POINTER(_UNICODE_STRING)),
        ("Attributes", wintypes.ULONG),
        ("SecurityDescriptor", ctypes.c_void_p),
        ("SecurityQualityOfService", ctypes.c_void_p),
    ]


class _FILE_ID_128(ctypes.Structure):
    _fields_ = [("Identifier", ctypes.c_ubyte * 16)]


class _FILE_ID_INFO_STRUCT(ctypes.Structure):
    _fields_ = [("VolumeSerialNumber", ctypes.c_ulonglong), ("FileId", _FILE_ID_128)]


class _FILE_ATTRIBUTE_TAG_INFO_STRUCT(ctypes.Structure):
    _fields_ = [("FileAttributes", wintypes.DWORD), ("ReparseTag", wintypes.DWORD)]


class _FILE_REMOTE_PROTOCOL_INFO_STRUCT(ctypes.Structure):
    _fields_ = [
        ("StructureVersion", wintypes.USHORT),
        ("StructureSize", wintypes.USHORT),
        ("Protocol", wintypes.DWORD),
        ("ProtocolMajorVersion", wintypes.USHORT),
        ("ProtocolMinorVersion", wintypes.USHORT),
        ("ProtocolRevision", wintypes.USHORT),
        ("Reserved", wintypes.USHORT),
        ("Flags", wintypes.DWORD),
        ("GenericReserved", wintypes.DWORD * 8),
        ("ProtocolSpecificReserved", wintypes.DWORD * 16),
        ("ProtocolSpecific", wintypes.DWORD * 16),
    ]


class _FILE_BASIC_INFO_STRUCT(ctypes.Structure):
    _fields_ = [
        ("CreationTime", ctypes.c_longlong),
        ("LastAccessTime", ctypes.c_longlong),
        ("LastWriteTime", ctypes.c_longlong),
        ("ChangeTime", ctypes.c_longlong),
        ("FileAttributes", wintypes.DWORD),
    ]


class _FILE_STANDARD_INFO_STRUCT(ctypes.Structure):
    _fields_ = [
        ("AllocationSize", ctypes.c_longlong),
        ("EndOfFile", ctypes.c_longlong),
        ("NumberOfLinks", wintypes.DWORD),
        ("DeletePending", ctypes.c_ubyte),
        ("Directory", ctypes.c_ubyte),
    ]


class _FILE_RENAME_INFORMATION_STRUCT(ctypes.Structure):
    _fields_ = [("ReplaceIfExists", wintypes.BOOLEAN), ("RootDirectory", wintypes.HANDLE), ("FileNameLength", wintypes.ULONG), ("FileName", wintypes.WCHAR * 1)]


def _validate_windows_abi() -> None:
    pointer = ctypes.sizeof(ctypes.c_void_p)
    expected_root = 8 if pointer == 8 else 4
    expected_name = 20 if pointer == 8 else 12
    expected_iosb = 16 if pointer == 8 else 8
    if (
        _FILE_RENAME_INFORMATION_STRUCT.RootDirectory.offset != expected_root
        or _FILE_RENAME_INFORMATION_STRUCT.FileName.offset != expected_name
        or ctypes.sizeof(_IO_STATUS_BLOCK) != expected_iosb
        or _FILE_REMOTE_PROTOCOL_INFO_STRUCT.StructureVersion.offset != 0
        or _FILE_REMOTE_PROTOCOL_INFO_STRUCT.StructureSize.offset != 2
        or _FILE_REMOTE_PROTOCOL_INFO_STRUCT.Protocol.offset != 4
        or ctypes.sizeof(_FILE_REMOTE_PROTOCOL_INFO_STRUCT) != 180
        or _FILE_STANDARD_INFO_STRUCT.DeletePending.offset != 20
        or _FILE_STANDARD_INFO_STRUCT.Directory.offset != 21
        or ctypes.sizeof(_FILE_STANDARD_INFO_STRUCT) != 24
    ):
        raise RuntimeError("unsupported Windows native ABI layout")


class _WindowsApi:
    def __init__(self) -> None:
        if os.name != "nt":
            raise RuntimeError("Windows native API unavailable")
        _validate_windows_abi()
        self.kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self.ntdll = ctypes.WinDLL("ntdll")
        self._bind()

    def _bind(self) -> None:
        self.kernel32.GetDriveTypeW.argtypes = [wintypes.LPCWSTR]
        self.kernel32.GetDriveTypeW.restype = wintypes.UINT
        self.kernel32.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        self.kernel32.CreateFileW.restype = wintypes.HANDLE
        self.kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        self.kernel32.CloseHandle.restype = wintypes.BOOL
        self.kernel32.GetFinalPathNameByHandleW.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD]
        self.kernel32.GetFinalPathNameByHandleW.restype = wintypes.DWORD
        self.kernel32.GetFileInformationByHandleEx.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        self.kernel32.GetFileInformationByHandleEx.restype = wintypes.BOOL
        self.kernel32.FlushFileBuffers.argtypes = [wintypes.HANDLE]
        self.kernel32.FlushFileBuffers.restype = wintypes.BOOL
        self.ntdll.NtCreateFile.argtypes = [ctypes.POINTER(wintypes.HANDLE), wintypes.DWORD, ctypes.POINTER(_OBJECT_ATTRIBUTES), ctypes.POINTER(_IO_STATUS_BLOCK), ctypes.c_void_p, wintypes.ULONG, wintypes.ULONG, wintypes.ULONG, wintypes.ULONG, ctypes.c_void_p, wintypes.ULONG]
        self.ntdll.NtCreateFile.restype = wintypes.LONG
        self.ntdll.NtQueryDirectoryFile.argtypes = [wintypes.HANDLE, wintypes.HANDLE, ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(_IO_STATUS_BLOCK), ctypes.c_void_p, wintypes.ULONG, ctypes.c_int, wintypes.BOOLEAN, ctypes.c_void_p, wintypes.BOOLEAN]
        self.ntdll.NtQueryDirectoryFile.restype = wintypes.LONG
        self.ntdll.NtSetInformationFile.argtypes = [wintypes.HANDLE, ctypes.POINTER(_IO_STATUS_BLOCK), ctypes.c_void_p, wintypes.ULONG, ctypes.c_int]
        self.ntdll.NtSetInformationFile.restype = wintypes.LONG

    @staticmethod
    def _raise_last_error() -> None:
        raise ctypes.WinError(ctypes.get_last_error())

    @staticmethod
    def _raise_status(status: int) -> None:
        raise OSError(f"NTSTATUS 0x{status & 0xFFFFFFFF:08x}")

    def get_drive_type(self, root: str) -> int:
        return int(self.kernel32.GetDriveTypeW(root))

    def open_root(self, root: str, *, desired_access: int, share_access: int, flags: int) -> int:
        handle = self.kernel32.CreateFileW(root, desired_access, share_access, None, _OPEN_EXISTING, flags, None)
        if handle == wintypes.HANDLE(-1).value:
            self._raise_last_error()
        return int(handle)

    def final_nt_path(self, handle: int) -> str:
        buffer = ctypes.create_unicode_buffer(32768)
        length = self.kernel32.GetFinalPathNameByHandleW(handle, buffer, len(buffer), 0x2)
        if not length or length >= len(buffer):
            self._raise_last_error()
        return buffer.value

    def remote_protocol(self, handle: int) -> int:
        info = _FILE_REMOTE_PROTOCOL_INFO_STRUCT()
        if not self.kernel32.GetFileInformationByHandleEx(handle, _FILE_REMOTE_PROTOCOL_INFO, ctypes.byref(info), ctypes.sizeof(info)):
            error = ctypes.get_last_error()
            if error == 87:
                return 0
            raise ctypes.WinError(error)
        return int(info.Protocol)

    def _query(self, handle: int, info_class: int, structure: ctypes.Structure) -> None:
        if not self.kernel32.GetFileInformationByHandleEx(handle, info_class, ctypes.byref(structure), ctypes.sizeof(structure)):
            self._raise_last_error()

    def query_info(self, handle: int) -> _WindowsFileInfo:
        file_id = _FILE_ID_INFO_STRUCT()
        tag = _FILE_ATTRIBUTE_TAG_INFO_STRUCT()
        basic = _FILE_BASIC_INFO_STRUCT()
        standard = _FILE_STANDARD_INFO_STRUCT()
        self._query(handle, _FILE_ID_INFO, file_id)
        self._query(handle, _FILE_ATTRIBUTE_TAG_INFO, tag)
        self._query(handle, 0, basic)
        self._query(handle, 1, standard)
        kind = stat.S_IFDIR if standard.Directory else stat.S_IFREG
        identity = (int(file_id.VolumeSerialNumber), bytes(file_id.FileId.Identifier), kind | int(basic.FileAttributes), int(standard.EndOfFile), int(basic.LastWriteTime), int(basic.ChangeTime))
        return _WindowsFileInfo(int(file_id.VolumeSerialNumber), int(tag.FileAttributes), int(tag.ReparseTag), identity)

    def nt_create(self, parent: int, name: str, *, desired_access: int, share_access: int, disposition: int, options: int, attributes: int) -> int:
        name_buffer = ctypes.create_unicode_buffer(name)
        object_name = _UNICODE_STRING(len(name.encode("utf-16-le")), ctypes.sizeof(name_buffer), ctypes.cast(name_buffer, wintypes.LPWSTR))
        object_attributes = _OBJECT_ATTRIBUTES(ctypes.sizeof(_OBJECT_ATTRIBUTES), parent, ctypes.pointer(object_name), attributes, None, None)
        result = wintypes.HANDLE()
        iosb = _IO_STATUS_BLOCK()
        status = int(self.ntdll.NtCreateFile(ctypes.byref(result), desired_access, ctypes.byref(object_attributes), ctypes.byref(iosb), None, FILE_ATTRIBUTE_NORMAL, share_access, disposition, options, None, 0))
        if status < 0:
            self._raise_status(status)
        return int(result.value)

    def transfer_to_fd(self, handle: int, mode: str) -> int:
        import msvcrt

        flags = os.O_BINARY | (os.O_RDONLY if mode == "rb" else os.O_RDWR)
        return msvcrt.open_osfhandle(handle, flags)

    def enumerate(self, handle: int) -> tuple[str, ...]:
        names: list[str] = []
        restart = True
        while True:
            buffer = (ctypes.c_ubyte * 65536)()
            iosb = _IO_STATUS_BLOCK()
            status = int(self.ntdll.NtQueryDirectoryFile(handle, None, None, None, ctypes.byref(iosb), buffer, ctypes.sizeof(buffer), _FILE_NAMES_INFORMATION, True, None, restart))
            restart = False
            unsigned = status & 0xFFFFFFFF
            if unsigned == _STATUS_NO_MORE_FILES or (status >= 0 and iosb.Information == 0):
                break
            if status < 0:
                self._raise_status(status)
            if iosb.Information < 12:
                raise OSError("invalid directory information")
            name_length = int.from_bytes(bytes(buffer[8:12]), "little")
            if name_length % 2 or 12 + name_length > iosb.Information:
                raise OSError("invalid directory entry")
            name = bytes(buffer[12 : 12 + name_length]).decode("utf-16-le")
            if name not in {".", ".."}:
                names.append(name)
        return tuple(names)

    def flush(self, handle: int) -> None:
        if not self.kernel32.FlushFileBuffers(handle):
            self._raise_last_error()

    def publish(self, file_handle: int, directory_handle: int, final_name: str, *, replace: bool) -> None:
        try:
            encoded = final_name.encode("utf-16-le")
            length = ctypes.sizeof(_FILE_RENAME_INFORMATION_STRUCT) + len(encoded)
            buffer = ctypes.create_string_buffer(length)
            header = _FILE_RENAME_INFORMATION_STRUCT.from_buffer(buffer)
            header.ReplaceIfExists = bool(replace)
            header.RootDirectory = directory_handle
            header.FileNameLength = len(encoded)
            ctypes.memmove(ctypes.addressof(buffer) + _FILE_RENAME_INFORMATION_STRUCT.FileName.offset, encoded, len(encoded))
        except (TypeError, ValueError, OverflowError) as exc:
            raise OSError("invalid rename information") from exc
        iosb = _IO_STATUS_BLOCK()
        status = int(self.ntdll.NtSetInformationFile(file_handle, ctypes.byref(iosb), buffer, length, _FILE_RENAME_INFORMATION))
        if status < 0:
            self._raise_status(status)

    def close(self, handle: int) -> None:
        if not self.kernel32.CloseHandle(handle):
            self._raise_last_error()


class _WindowsBackend:
    def __init__(self, api: object | None = None) -> None:
        self.api = api or _WindowsApi()

    @staticmethod
    def _root_text(path: Path) -> str:
        return f"{str(path)[0].upper()}:\\"

    def _fdopen(self, fd: int, mode: str) -> BinaryIO:
        opener = getattr(self.api, "fdopen", None)
        return opener(fd, mode) if opener is not None else os.fdopen(fd, mode, closefd=True)

    def _close_fd(self, fd: int) -> None:
        closer = getattr(self.api, "close_fd", None)
        if closer is None:
            os.close(fd)
        else:
            closer(fd)

    def _check_info(self, handle: object, *, root_volume: int, directory: bool, error_code: str) -> _WindowsFileInfo:
        info = self.api.query_info(handle)
        expected = bool(info.attributes & FILE_ATTRIBUTE_DIRECTORY) if directory else not bool(info.attributes & FILE_ATTRIBUTE_DIRECTORY)
        if info.volume_serial != root_volume or info.attributes & FILE_ATTRIBUTE_REPARSE_POINT or info.reparse_tag or not expected:
            raise PilotError(error_code)
        return info

    def _root(self, path: Path, error_code: str) -> tuple[object, int]:
        root = self._root_text(path)
        if self.api.get_drive_type(root) in _REMOTE_DRIVES:
            raise PilotError(error_code)
        handle = self.api.open_root(
            root,
            desired_access=FILE_LIST_DIRECTORY | FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            share_access=FILE_SHARE_READ | FILE_SHARE_WRITE,
            flags=FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
        )
        try:
            nt_path = self.api.final_nt_path(handle).lower()
            if not nt_path.startswith(_LOCAL_NT_PREFIXES) or self.api.remote_protocol(handle):
                raise PilotError(error_code)
            info = self.api.query_info(handle)
            if info.attributes & FILE_ATTRIBUTE_REPARSE_POINT or info.reparse_tag or not info.attributes & FILE_ATTRIBUTE_DIRECTORY:
                raise PilotError(error_code)
            return handle, info.volume_serial
        except Exception as exc:
            self.api.close(handle)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def _open_dirs(self, path: Path, error_code: str) -> tuple[list[object], int]:
        root, volume = self._root(path, error_code)
        handles = [root]
        try:
            for component in str(path)[3:].split("\\") if len(str(path)) > 3 else ():
                handle = self.api.nt_create(
                    handles[-1], component,
                    desired_access=FILE_LIST_DIRECTORY | FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
                    share_access=FILE_SHARE_READ | FILE_SHARE_WRITE,
                    disposition=FILE_OPEN,
                    options=FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT,
                    attributes=OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
                )
                handles.append(handle)
                self._check_info(handle, root_volume=volume, directory=True, error_code=error_code)
            return handles, volume
        except Exception as exc:
            for handle in reversed(handles):
                self.api.close(handle)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def open_file(self, path: Path, *, error_code: str) -> SecureFile:
        handles, volume = self._open_dirs(path.parent, error_code)
        final: object | None = None
        try:
            final = self.api.nt_create(
                handles[-1], path.name,
                desired_access=FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
                share_access=FILE_SHARE_READ,
                disposition=FILE_OPEN,
                options=FILE_NON_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT,
                attributes=OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
            )
            info = self._check_info(final, root_volume=volume, directory=False, error_code=error_code)
            native_handle = final
            fd = self.api.transfer_to_fd(final, "rb")
            final = None
            try:
                stream = self._fdopen(fd, "rb")
            except Exception:
                self._close_fd(fd)
                raise
            return SecureFile(path, stream, native_handle, info.identity, lambda handle: self.api.query_info(handle).identity, [(self.api, handle) for handle in handles], error_code)
        except Exception as exc:
            if final is not None:
                self.api.close(final)
            for handle in reversed(handles):
                self.api.close(handle)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def open_dir(self, path: Path, *, error_code: str) -> SecureDirectory:
        handles, volume = self._open_dirs(path, error_code)
        return SecureDirectory(path, self, handles[-1], handles, volume, error_code)

    def enumerate(self, directory: SecureDirectory) -> tuple[str, ...]:
        try:
            return tuple(name for name in self.api.enumerate(directory.native_handle) if name not in {".", ".."})
        except OSError as exc:
            raise _error(directory.error_code, exc)

    def open_relative_file(self, directory: SecureDirectory, name: str, *, error_code: str) -> SecureFile:
        final: object | None = None
        try:
            final = self.api.nt_create(directory.native_handle, name, desired_access=FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE, share_access=FILE_SHARE_READ, disposition=FILE_OPEN, options=FILE_NON_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT, attributes=OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE)
            info = self._check_info(final, root_volume=directory.root_device, directory=False, error_code=error_code)
            native_handle = final
            fd = self.api.transfer_to_fd(final, "rb")
            final = None
            try:
                stream = self._fdopen(fd, "rb")
            except Exception:
                self._close_fd(fd)
                raise
            return SecureFile(directory.path / name, stream, native_handle, info.identity, lambda handle: self.api.query_info(handle).identity, [], error_code)
        except Exception as exc:
            if final is not None:
                self.api.close(final)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def create_file(self, directory: SecureDirectory, name: str, *, error_code: str) -> CreatedFile:
        handle: object | None = None
        try:
            handle = self.api.nt_create(directory.native_handle, name, desired_access=FILE_READ_DATA | FILE_WRITE_DATA | FILE_READ_ATTRIBUTES | DELETE | SYNCHRONIZE, share_access=0, disposition=FILE_CREATE, options=FILE_NON_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT, attributes=OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE)
            self._check_info(handle, root_volume=directory.root_device, directory=False, error_code=error_code)
            native_handle = handle
            fd = self.api.transfer_to_fd(handle, "w+b")
            handle = None
            try:
                stream = self._fdopen(fd, "w+b")
            except Exception:
                self._close_fd(fd)
                raise
            return CreatedFile(name, stream, native_handle)
        except Exception as exc:
            if handle is not None:
                self.api.close(handle)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def create_dir(self, directory: SecureDirectory, name: str, *, error_code: str) -> SecureDirectory:
        handle: object | None = None
        try:
            handle = self.api.nt_create(directory.native_handle, name, desired_access=FILE_LIST_DIRECTORY | FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE, share_access=FILE_SHARE_READ | FILE_SHARE_WRITE, disposition=FILE_CREATE, options=FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT, attributes=OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE)
            self._check_info(handle, root_volume=directory.root_device, directory=True, error_code=error_code)
            return SecureDirectory(directory.path / name, self, handle, [handle], directory.root_device, error_code)
        except Exception as exc:
            if handle is not None:
                self.api.close(handle)
            if isinstance(exc, PilotError):
                raise
            raise _error(error_code, exc)

    def publish_no_replace(self, directory: SecureDirectory, created: CreatedFile, final_name: str, *, error_code: str) -> None:
        try:
            created.flush()
            self.api.flush(created.native_handle)
            self.api.publish(created.native_handle, directory.native_handle, final_name, replace=False)
        except OSError as exc:
            raise _error(error_code, exc)


def _backend() -> _WindowsBackend | _PosixBackend:
    return _WindowsBackend() if os.name == "nt" else _PosixBackend()


def secure_open_file(value: str | Path, *, error_code: str) -> SecureFile:
    path = lexical_local_path(value, error_code=error_code)
    return _backend().open_file(path, error_code=error_code)


def secure_read_file(value: str | Path, *, max_bytes: int, error_code: str, too_large_code: str | None = None) -> tuple[Path, bytes]:
    path = lexical_local_path(value, error_code=error_code)
    with _backend().open_file(path, error_code=error_code) as capability:
        return path, capability.read_all(max_bytes=max_bytes, too_large_code=too_large_code)


def secure_open_dir(value: str | Path, *, error_code: str) -> SecureDirectory:
    path = lexical_local_path(value, error_code=error_code)
    return _backend().open_dir(path, error_code=error_code)
