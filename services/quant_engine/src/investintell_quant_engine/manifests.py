"""Engine manifest builders."""

from __future__ import annotations

import datetime as dt
from typing import Any

from investintell_quant_core import __version__ as quant_core_version

from .environment import collect_environment
from .version import __version__ as quant_engine_version


def engine_manifest(*, job_type: str, jobs: int, offline: bool) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact_type": "engine_manifest",
        "created_at": dt.datetime.now(dt.UTC).isoformat(),
        "job_type": job_type,
        "jobs": jobs,
        "offline": offline,
        "runtime_activation": False,
        "quant_core_version": quant_core_version,
        "quant_engine_version": quant_engine_version,
        "environment": collect_environment(),
    }

