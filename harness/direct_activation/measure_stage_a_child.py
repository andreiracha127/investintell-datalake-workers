"""In-process measurement wrapper for one Stage A live-validation compute (stdlib only).

Usage: python -m harness.direct_activation.measure_stage_a_child

Runs ``harness.direct_activation.live_validation.compute()`` in THIS process with the
worker_commit pinned from ``OPEN_MACRO_WORKER_COMMIT`` (the container has no git, and
even on the host the runner injects the SAME commit into every leg so the record is
byte-identical across the whole round). It then canonicalizes the record, hashes it,
and prints a single sentinel-prefixed JSON line to stdout:

    MEASURE_STAGE_A_JSON {"logical_output_hash": ..., "wall_ms": ..., ...}

Measuring inside the child (rather than polling from a parent) reads the OS peak
memory of the very process that computed. ``_peak_bytes`` is duplicated from
``harness/dark_launch/measure_child.py`` (credit: Phase 1 measurement machinery) so
this child depends only on the stdlib and stays runnable in the hardened, read-only
container where importing the dark_launch package is unnecessary.

The record's LOGICAL output is hashed with ``provenance.worker_commit`` EXCLUDED:
that field is environment provenance (the commit the measurement ran on), not logical
output, and it varies across commits. Re-running the official path from the evidence
commit injects THAT commit as ``worker_commit``; if it were part of the hash, the
canonical fingerprint would diverge from the committed record and trip the cited-record
gate even though the decision/allocation are byte-identical. Stripping it makes the
fingerprint reproducible from ANY commit while ``compute()`` stays a pure function of
the committed pack v2 + the pinned delta snapshot.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time

SENTINEL = "MEASURE_STAGE_A_JSON"


def _peak_bytes() -> int:
    """OS peak memory of THIS process (credit: harness/dark_launch/measure_child.py)."""
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.windll.kernel32
        # without an explicit restype the -1 pseudo-handle truncates on 64-bit
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        # modern Windows exports it from kernel32 (K32 prefix); psapi is the legacy home
        get_info = getattr(kernel32, "K32GetProcessMemoryInfo", None)
        if get_info is None:
            get_info = ctypes.windll.psapi.GetProcessMemoryInfo
        get_info.argtypes = [wintypes.HANDLE,
                             ctypes.POINTER(PROCESS_MEMORY_COUNTERS), wintypes.DWORD]
        get_info.restype = wintypes.BOOL
        handle = kernel32.GetCurrentProcess()
        if not get_info(handle, ctypes.byref(counters), counters.cb):
            raise OSError("GetProcessMemoryInfo failed")
        return int(counters.PeakWorkingSetSize)
    import resource

    self_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    children_peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    peak = int(max(self_peak, children_peak))
    # ru_maxrss unit is platform-specific: KiB on Linux, BYTES on macOS/BSD.
    return peak if sys.platform == "darwin" else peak * 1024


def canonical_hash(record: dict) -> str:
    """sha256 of the canonical JSON of the record's LOGICAL output.

    ``provenance.worker_commit`` is EXCLUDED so the fingerprint is reproducible from any
    commit (see module docstring). Everything else — including the rest of provenance
    (e.g. ``modules``) — is part of the logical fingerprint. The input record is left
    untouched; the stripping happens on a shallow copy of ``provenance``."""
    logical = dict(record)
    provenance = logical.get("provenance")
    if isinstance(provenance, dict):
        logical["provenance"] = {k: v for k, v in provenance.items()
                                 if k != "worker_commit"}
    canonical = json.dumps(logical, sort_keys=True, ensure_ascii=False,
                           separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def main() -> int:
    override = os.environ.get("OPEN_MACRO_WORKER_COMMIT")
    # import AFTER the timer would understate cold-import cost, but the round measures
    # steady-state compute; import is outside the timed region on purpose, matching the
    # Phase 1 child which times only cli_main(...).
    from harness.direct_activation import live_validation

    started = time.perf_counter()
    record = live_validation.compute(worker_commit_override=override)
    wall_ms = (time.perf_counter() - started) * 1000.0

    out = {
        "logical_output_hash": canonical_hash(record),
        "wall_ms": wall_ms,
        "memory_peak_bytes": _peak_bytes(),
        "exit_code": 0,
        "platform": sys.platform,
    }
    sys.stdout.write(f"{SENTINEL} {json.dumps(out)}\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
