"""Runtime build manifest: WHAT STACK a bond publication was computed on.

The implied-rating product is replayed bit-for-bit (``rows_digest`` must
reproduce), and its log-derived columns go through ``np.log`` -- which NumPy
dispatches to a CPU-specific SIMD kernel at import time. Round 003 reproduced
the reference digest on Python 3.13.12 / numpy 2.5.1 / pandas 3.0.3 /
scipy 1.18.0 / pyarrow 25.0.0 and failed historical bit-reproduction only by
last-bit cells in those columns, so the interpreter, the numeric stack, the
CPU/SIMD dispatch and the BLAS configuration are EVIDENCE that every plan, run
and determinism receipt must carry.

Discipline:

  * PURE and allowlisted. Every value comes from a named, reviewed source:
    ``sys``/``platform``, package metadata, NumPy's own runtime introspection,
    ``/proc/cpuinfo`` (model + flags), the five thread-count variables, the
    shipped lock + Dockerfile and their recorded sha256 sidecars, and the code
    revision ladder. NEVER an environment dump, never a DSN, never argv.
  * OUT of identity. The manifest is attached to results and receipts only.
    It is not an input of ``publication_id``, ``build_fingerprint``,
    ``snapshot_fingerprint`` or ``rows_digest`` (tests pin this): a different
    stack reproducing the same rows is the same publication, and a stack that
    does not reproduce them is caught by the digest, not by the manifest.
  * Explicit absence. A package that is not installed, a file that is not
    shipped, an introspection NumPy does not offer on this build: all recorded
    as ``None``/``False`` with a reason, never silently omitted.
"""
from __future__ import annotations

import hashlib
import os
import platform
import subprocess
import sys
from collections.abc import Mapping
from importlib import metadata
from pathlib import Path
from typing import Any

MANIFEST_SCHEMA = "bond_build_manifest/1"

ROOT = Path(__file__).resolve().parents[2]
LOCK_DIR = Path("docker") / "bond-live-daily"
LOCK_FILE = "requirements.lock"
DOCKERFILE = "Dockerfile"

#: Distributions whose versions the manifest records (explicitly ``None`` when absent).
TRACKED_PACKAGES: tuple[str, ...] = (
    "numpy", "pandas", "scipy", "pyarrow", "psycopg", "psycopg-binary", "statsmodels",
)
#: The ONLY environment variables the manifest reads (thread/dispatch knobs).
ALLOWED_ENV_VARS: tuple[str, ...] = (
    "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "BLIS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS", "NPY_DISABLE_CPU_FEATURES", "OPENBLAS_CORETYPE",
)
#: Same ladder as src/workers/bond_market_implied_rating.py (one fleet, one ladder).
REVISION_ENV_VARS: tuple[str, ...] = (
    "CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA",
)
CPUINFO_PATH = Path("/proc/cpuinfo")
_MAX_CPUINFO_BYTES = 1 << 20


def _package_version(name: str) -> str | None:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def _python_section() -> dict[str, Any]:
    return {
        "version": platform.python_version(),
        "version_info": list(sys.version_info[:5]),
        "implementation": sys.implementation.name,
        "compiler": platform.python_compiler(),
        "build": list(platform.python_build()),
        "executable": sys.executable,
        "flags": {
            "isolated": bool(sys.flags.isolated),
            "no_site": bool(sys.flags.no_site),
            "safe_path": bool(getattr(sys.flags, "safe_path", False)),
        },
    }


def _platform_section() -> dict[str, Any]:
    libc = platform.libc_ver()
    return {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor() or None,
        "libc": {"name": libc[0] or None, "version": libc[1] or None},
        "cpu_count": os.cpu_count(),
    }


def _read_cpuinfo(path: Path = CPUINFO_PATH) -> str | None:
    try:
        with path.open("rb") as handle:
            raw = handle.read(_MAX_CPUINFO_BYTES)
    except OSError:
        return None
    return raw.decode("utf-8", "replace")


def _cpu_section(cpuinfo: str | None) -> dict[str, Any]:
    if cpuinfo is None:
        return {"source": None, "model": None, "flags": None, "logical_processors": None}
    model: str | None = None
    flags: list[str] | None = None
    processors = 0
    for line in cpuinfo.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            continue
        key = key.strip()
        value = value.strip()
        if key == "processor":
            processors += 1
        elif key == "model name" and model is None:
            model = value
        elif key in {"flags", "Features"} and flags is None:
            flags = sorted(set(value.split()))
    return {
        "source": CPUINFO_PATH.as_posix(),
        "model": model,
        "flags": flags,
        "logical_processors": processors or None,
    }


def _numpy_section() -> dict[str, Any]:
    try:
        import numpy as np
    except ImportError:
        return {"present": False}
    section: dict[str, Any] = {"present": True, "version": np.__version__}
    try:
        from numpy._core import _multiarray_umath as umath  # type: ignore[attr-defined]
    except ImportError:  # pragma: no cover - older layouts
        try:
            from numpy.core import _multiarray_umath as umath  # type: ignore[no-redef]
        except ImportError:
            umath = None
    if umath is None:
        section["cpu_features"] = None
        section["cpu_baseline"] = None
        section["cpu_dispatch"] = None
    else:
        features = getattr(umath, "__cpu_features__", None)
        section["cpu_features"] = (
            None if features is None
            else {name: bool(enabled) for name, enabled in sorted(features.items())}
        )
        section["cpu_baseline"] = list(getattr(umath, "__cpu_baseline__", None) or []) or None
        section["cpu_dispatch"] = list(getattr(umath, "__cpu_dispatch__", None) or []) or None
    try:
        config = np.show_config(mode="dicts")  # numpy >= 1.25
    except TypeError:  # pragma: no cover - numpy < 1.25 prints instead
        config = None
    except (AttributeError, KeyError, ValueError, RuntimeError):  # pragma: no cover
        config = None  # introspection must never break a run
    section["blas"] = _blas_from_config(config)
    return section


def _blas_from_config(config: Any) -> dict[str, Any] | None:
    if not isinstance(config, Mapping):
        return None
    deps = config.get("Build Dependencies")
    if not isinstance(deps, Mapping):
        return None
    out: dict[str, Any] = {}
    for key in ("blas", "lapack"):
        entry = deps.get(key)
        if isinstance(entry, Mapping):
            out[key] = {
                "name": entry.get("name"),
                "version": entry.get("version"),
                "found": entry.get("found"),
                "detection_method": entry.get("detection method"),
                "openblas_configuration": entry.get("openblas configuration"),
            }
        else:
            out[key] = None
    simd = config.get("SIMD Extensions")
    out["simd_extensions"] = (
        {k: list(v) if isinstance(v, (list, tuple)) else v for k, v in simd.items()}
        if isinstance(simd, Mapping) else None
    )
    return out


def _env_section(environ: Mapping[str, str]) -> dict[str, str | None]:
    return {name: environ.get(name) for name in ALLOWED_ENV_VARS}


def _relative(path: Path, base: Path) -> str:
    return (path.relative_to(base) if path.is_relative_to(base) else path).as_posix()


def _file_section(path: Path, *, base: Path) -> dict[str, Any]:
    sidecar = path.with_name(path.name + ".sha256")
    if not path.is_file():
        return {
            "path": _relative(path, base),
            "present": False, "sha256": None, "recorded_sha256": None,
            "sha256_matches_recorded": None,
        }
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    recorded: str | None = None
    if sidecar.is_file():
        try:
            recorded = sidecar.read_text(encoding="ascii").split()[0]
        except (OSError, IndexError, UnicodeDecodeError):
            recorded = None
    return {
        "path": _relative(path, base),
        "present": True,
        "sha256": digest,
        "recorded_sha256": recorded,
        "sha256_matches_recorded": None if recorded is None else digest == recorded,
    }


def resolve_source_revision(environ: Mapping[str, str], *, cwd: Path | None = None) -> dict[str, Any]:
    """The code-revision ladder: deploy stamps first, then git, else explicit None."""
    for name in REVISION_ENV_VARS:
        value = environ.get(name)
        if value:
            return {"revision": value.strip(), "source": name}
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5,
            check=False, cwd=str(cwd or ROOT),
        )
        stamped = out.stdout.strip()
        if out.returncode == 0 and stamped:
            return {"revision": stamped, "source": "git"}
    except (OSError, subprocess.SubprocessError):
        pass
    return {"revision": None, "source": None}


_READ_CPUINFO = object()  # sentinel: read /proc/cpuinfo unless the caller injects text


def collect_build_manifest(
    *, root: Path | None = None, environ: Mapping[str, str] | None = None,
    cpuinfo: Any = _READ_CPUINFO,
) -> dict[str, Any]:
    """Collect the allowlisted runtime stack evidence. JSON-serializable; no secrets.

    ``root`` is the checkout/image root holding ``docker/bond-live-daily``
    (defaults to this repository); ``environ`` and ``cpuinfo`` exist for tests
    and are read ONLY through the allowlists above.
    """
    base = ROOT if root is None else Path(root)
    env = os.environ if environ is None else environ
    info = _read_cpuinfo() if cpuinfo is _READ_CPUINFO else cpuinfo
    lock_dir = base / LOCK_DIR
    return {
        "schema": MANIFEST_SCHEMA,
        "python": _python_section(),
        "packages": {name: _package_version(name) for name in TRACKED_PACKAGES},
        "platform": _platform_section(),
        "cpu": _cpu_section(info),
        "numpy_runtime": _numpy_section(),
        "thread_env": _env_section(env),
        "lock": _file_section(lock_dir / LOCK_FILE, base=base),
        "dockerfile": _file_section(lock_dir / DOCKERFILE, base=base),
        "source_revision": resolve_source_revision(env, cwd=base),
    }


def manifest_summary(manifest: Mapping[str, Any]) -> str:
    """One log line: interpreter, numeric stack, lock digest prefix, baseline SIMD."""
    packages = manifest.get("packages", {})
    lock = manifest.get("lock", {})
    numpy_runtime = manifest.get("numpy_runtime", {})
    lock_digest = lock.get("sha256")
    return (
        f"python={manifest.get('python', {}).get('version')} "
        f"numpy={packages.get('numpy')} pandas={packages.get('pandas')} "
        f"scipy={packages.get('scipy')} pyarrow={packages.get('pyarrow')} "
        f"psycopg={packages.get('psycopg')} statsmodels={packages.get('statsmodels')} "
        f"lock={'absent' if lock_digest is None else lock_digest[:16]} "
        f"simd_baseline={','.join(numpy_runtime.get('cpu_baseline') or []) or 'n/a'} "
        f"machine={manifest.get('platform', {}).get('machine')}"
    )
