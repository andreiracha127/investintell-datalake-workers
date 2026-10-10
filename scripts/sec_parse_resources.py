"""Conservative, machine-wide parsing budget, including container constraints."""
from __future__ import annotations

import math
import os
from pathlib import Path


def _read_limit(path: str) -> int | None:
    try:
        value = Path(path).read_text().strip()
        return None if value == "max" else int(value)
    except (OSError, ValueError):
        return None


def parse_resource_budget(worker_memory_mb: int | None = None) -> dict:
    if worker_memory_mb is None:
        worker_memory_mb = int(os.environ.get("SEC_PARSE_WORKER_MEMORY_MB", "600"))
    if worker_memory_mb <= 0:
        raise ValueError("worker_memory_mb must be positive")
    cpus = os.cpu_count() or 1
    ceiling = max(1, cpus - 4)
    try:
        ceiling = min(ceiling, len(os.sched_getaffinity(0)))
    except (AttributeError, OSError):
        pass
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        if quota != "max":
            ceiling = min(ceiling, max(1, math.floor(int(quota) / int(period))))
    except (OSError, ValueError, ZeroDivisionError):
        quota = _read_limit("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        period = _read_limit("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        if quota and quota > 0 and period:
            ceiling = min(ceiling, max(1, quota // period))
    available = None
    memory_capacity = None
    reserve = None
    try:
        import psutil
        memory = psutil.virtual_memory()
        available, memory_capacity = memory.available, memory.total
        try:
            ceiling = min(ceiling, len(psutil.Process().cpu_affinity()))
        except (AttributeError, OSError, psutil.Error):
            pass
    except ImportError:
        if os.name == "nt":
            import ctypes
            kernel = ctypes.windll.kernel32
            kernel.GetCurrentProcess.restype = ctypes.c_void_p
            kernel.GetProcessAffinityMask.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t)]
            process_mask, system_mask = ctypes.c_size_t(), ctypes.c_size_t()
            if kernel.GetProcessAffinityMask(kernel.GetCurrentProcess(), ctypes.byref(process_mask), ctypes.byref(system_mask)):
                ceiling = min(ceiling, max(1, process_mask.value.bit_count()))
            class MemoryStatus(ctypes.Structure):
                _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong)] + [
                    (name, ctypes.c_ulonglong) for name in
                    ("total", "available", "page_total", "page_available", "virtual_total", "virtual_available", "extended")]
            status = MemoryStatus()
            status.length = ctypes.sizeof(status)
            if kernel.GlobalMemoryStatusEx(ctypes.byref(status)):
                available = status.available
                memory_capacity = status.total
        else:
            try:
                for line in Path("/proc/meminfo").read_text().splitlines():
                    if line.startswith("MemAvailable:"):
                        available = int(line.split()[1]) * 1024
                    elif line.startswith("MemTotal:"):
                        memory_capacity = int(line.split()[1]) * 1024
            except (OSError, ValueError):
                pass
    for limit_path, usage_path in (("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"),
                                   ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "/sys/fs/cgroup/memory/memory.usage_in_bytes")):
        limit, usage = _read_limit(limit_path), _read_limit(usage_path)
        if limit is not None and usage is not None:
            remaining = max(0, limit - usage)
            available = min(available, remaining) if available is not None else remaining
            memory_capacity = min(memory_capacity, limit) if memory_capacity is not None else limit
    if available is not None:
        configured_reserve = os.environ.get("SEC_PARSE_MEMORY_RESERVE_MB")
        if configured_reserve is not None:
            reserve = int(configured_reserve) * 1024**2
            if reserve < 0:
                raise ValueError("SEC_PARSE_MEMORY_RESERVE_MB must be nonnegative")
        else:
            reserve = available // 4
            if memory_capacity is not None and memory_capacity >= 4 * 1024**3:
                reserve = max(2 * 1024**3, reserve)
        ceiling = min(ceiling, max(0, (available - reserve) // (worker_memory_mb * 1024**2)))
    else:
        ceiling = min(ceiling, 2)
    return {"logical_cpus": cpus, "available_memory_bytes": available,
            "memory_capacity_bytes": memory_capacity, "memory_reserve_bytes": reserve,
            "worker_memory_mb": worker_memory_mb, "max_workers": int(ceiling)}


def resolve_parse_workers(requested: int | None = None, *, total_budget: int | None = None,
                          shard_count: int = 1, shard_index: int = 0,
                          worker_memory_mb: int | None = None) -> int:
    if (requested is not None and requested < 1) or (total_budget is not None and total_budget < 1):
        raise ValueError("parse worker budget must be positive")
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("invalid parse shard allocation")
    total = parse_resource_budget(worker_memory_mb)["max_workers"]
    if total < 1:
        if requested == 1 and shard_count == 1:
            return 1  # one worker parses inline in this process; no pool memory is reserved
        raise MemoryError("Insufficient available memory for one parse worker while preserving the memory reserve")
    total = min(total, requested if requested is not None else total,
                total_budget if total_budget is not None else total)
    quotient, remainder = divmod(total, shard_count)
    return quotient + (shard_index < remainder)
