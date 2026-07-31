"""Preflight checks for quant-engine jobs.

``validate_runtime_disabled`` used to be the only answer available: it refused
to run unless ``runtime_activation`` was ``False``, ``freeze_ready`` was
``False`` and ``a5_status`` was ``"blocked"``. Combined with a result schema
whose activation fields were ``const``-pinned to the blocked values, the engine
could not express — let alone produce — an activated result. The gate could not
pass by design, and passing it would have required a whole new contract.

The decision now lives where it belongs: in the execution envelope the caller
declares (the same place ``src/workers/open_macro_v03.py::check_governance``
answers it, against a ratified approval matrix). The schema says what shapes are
expressible (see ``contracts/quant-engine/v3``); preflight checks that what the
job REPORTED is consistent with the mode it was RUN IN.

This is still fail-closed in both directions:

* a job that asked for ``offline_evidence`` and reported any activation field on
  fails — exactly the old guarantee;
* a job that asked for ``activated`` and reported a blocked A5 fails too.

The inconsistency is the error, not the state.
"""

from __future__ import annotations

from typing import Any

#: The mode a job declares it is running in. ``offline_evidence`` is the default
#: everywhere, so an unstated mode is the conservative one.
OFFLINE_EVIDENCE = "offline_evidence"
ACTIVATED = "activated"
RUNTIME_MODES = (OFFLINE_EVIDENCE, ACTIVATED)

#: What each mode requires of the fields a report may carry. A field absent from
#: the report is not checked; a field present must match.
_MODE_EXPECTATIONS: dict[str, dict[str, Any]] = {
    OFFLINE_EVIDENCE: {
        "runtime_activation": False,
        "freeze_ready": False,
        "a5_status": "blocked",
        "official_result": False,
        "allocator_publish": False,
        "db_write": "none",
        "production_endpoint_activation": "none",
    },
    ACTIVATED: {
        "runtime_activation": True,
        "a5_status": "active",
        "official_result": True,
        "db_write": "publication",
    },
}


class RuntimeModeError(ValueError):
    """A report contradicts the runtime mode its job declared."""


def validate_offline_request(*, offline: bool, jobs: int) -> None:
    if offline is not True:
        raise ValueError("quant-engine jobs must declare offline=true")
    if jobs < 1:
        raise ValueError("jobs must be >= 1")


def validate_runtime_mode(report: dict[str, Any], *, mode: str = OFFLINE_EVIDENCE) -> None:
    """Raise unless ``report``'s activation fields are consistent with ``mode``."""
    if mode not in RUNTIME_MODES:
        raise RuntimeModeError(
            f"unknown runtime mode {mode!r} (expected one of {', '.join(RUNTIME_MODES)})"
        )
    expectations = _MODE_EXPECTATIONS[mode]
    mismatches = [
        f"{field}={report[field]!r} (mode {mode!r} requires {expected!r})"
        for field, expected in expectations.items()
        if field in report and report[field] != expected
    ]
    if mismatches:
        raise RuntimeModeError("runtime mode inconsistency: " + "; ".join(sorted(mismatches)))


def validate_runtime_disabled(report: dict[str, Any]) -> None:
    """Back-compatible alias: assert the report is offline-evidence shaped.

    Kept so existing callers and their tests keep working; new code should call
    ``validate_runtime_mode`` and say which mode it ran in.
    """
    validate_runtime_mode(report, mode=OFFLINE_EVIDENCE)
