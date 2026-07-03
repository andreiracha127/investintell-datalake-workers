"""In-process measurement wrapper for one quant-engine job (stdlib only).

Usage: python measure_child.py <metrics_out.json> <cli args...>

Runs ``investintell_quant_engine.cli.main(cli args)`` in THIS process and writes
``{exit_code, wall_ms, memory_peak_bytes, platform}`` to the metrics path. Measuring
inside the child avoids polling races: the peak is read from the OS accounting of the
very process that did the work (Windows: GetProcessMemoryInfo.PeakWorkingSetSize;
Linux/container: ru_maxrss, max of SELF and CHILDREN).

Limitations recorded honestly: with --jobs 1 the engine computes in-process, so the
self peak is the job peak; if a future caller measures a multi-process run, the Linux
branch still sees children via RUSAGE_CHILDREN while the Windows branch does not.
"""

from __future__ import annotations

import json
import sys
import time


def _peak_bytes() -> int:
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


def main() -> int:
    metrics_out = sys.argv[1]
    cli_args = sys.argv[2:]
    from investintell_quant_engine.cli import main as cli_main

    started = time.perf_counter()
    exit_code = cli_main(cli_args)
    wall_ms = (time.perf_counter() - started) * 1000.0
    metrics = {
        "exit_code": int(exit_code or 0),
        "wall_ms": wall_ms,
        "memory_peak_bytes": _peak_bytes(),
        "platform": sys.platform,
    }
    with open(metrics_out, "w", encoding="utf-8") as handle:
        json.dump(metrics, handle)
    return int(exit_code or 0)


if __name__ == "__main__":
    raise SystemExit(main())
