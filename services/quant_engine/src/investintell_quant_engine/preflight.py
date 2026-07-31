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

#: The value each mode requires of every governance field a report may carry. A
#: field PRESENT must match. A field ABSENT is governed by ``_MODE_REQUIRED``
#: below — silence is never treated as agreement.
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
        "freeze_ready": True,
        "a5_status": "active",
        "official_result": True,
        "allocator_publish": True,
        "db_write": "publication",
        "production_endpoint_activation": "live",
    },
}

#: Fields that MUST be present, in every report shape, for the mode to be
#: assertable at all. These two are the universal governance assertion: every
#: result variant in the contract declares them. An absent one is an
#: inconsistency, not a pass — ``{}`` can never validate.
_MODE_REQUIRED: dict[str, tuple[str, ...]] = {
    OFFLINE_EVIDENCE: ("runtime_activation", "a5_status"),
    ACTIVATED: ("runtime_activation", "a5_status"),
}

#: The publication flags. They exist only on the shapes that can publish, so they
#: are not universally required — but they travel together: a report carrying any
#: of them must carry all of them, or it is a half-activated result that no
#: reader can interpret.
_PUBLICATION_FIELDS: tuple[str, ...] = (
    "official_result",
    "allocator_publish",
    "db_write",
    "production_endpoint_activation",
)

#: Values a mode forbids outright, whatever the rest of the report says. The v3
#: contract widened ``classification`` so an activated run is expressible; that
#: must not let an OFFLINE run label itself productive.
_MODE_FORBIDDEN: dict[str, dict[str, frozenset[Any]]] = {
    OFFLINE_EVIDENCE: {"classification": frozenset({"productive_result"})},
    ACTIVATED: {"classification": frozenset({"metric_evidence_only"})},
}


class RuntimeModeError(ValueError):
    """A report contradicts the runtime mode its job declared."""


def validate_offline_request(*, offline: bool, jobs: int) -> None:
    if offline is not True:
        raise ValueError("quant-engine jobs must declare offline=true")
    if jobs < 1:
        raise ValueError("jobs must be >= 1")


def validate_runtime_mode(
    report: dict[str, Any],
    *,
    mode: str = OFFLINE_EVIDENCE,
    required: tuple[str, ...] | None = None,
) -> None:
    """Raise unless ``report``'s governance fields are consistent with ``mode``.

    Fail-closed on three distinct ways a report can fail to make its claim:

    * a required governance field is MISSING — a partial or empty report cannot
      assert anything, so it is rejected rather than skipped;
    * a present field CONTRADICTS the mode;
    * the publication flags are PARTIALLY present — a half-activated result.
    """
    if mode not in RUNTIME_MODES:
        raise RuntimeModeError(
            f"unknown runtime mode {mode!r} (expected one of {', '.join(RUNTIME_MODES)})"
        )
    expectations = _MODE_EXPECTATIONS[mode]
    problems: list[str] = []

    for field in required if required is not None else _MODE_REQUIRED[mode]:
        if field not in report:
            problems.append(
                f"{field} is missing (mode {mode!r} requires {expectations[field]!r}; "
                "an absent governance field is not an assertion)"
            )

    present_publication = [f for f in _PUBLICATION_FIELDS if f in report]
    if present_publication and len(present_publication) != len(_PUBLICATION_FIELDS):
        missing = sorted(set(_PUBLICATION_FIELDS) - set(present_publication))
        problems.append(
            f"publication flags are partially declared (missing: {', '.join(missing)}); "
            "a half-activated result cannot be interpreted"
        )

    problems.extend(
        f"{field}={report[field]!r} (mode {mode!r} requires {expected!r})"
        for field, expected in expectations.items()
        if field in report and report[field] != expected
    )

    problems.extend(
        f"{field}={report[field]!r} is forbidden in mode {mode!r}"
        for field, forbidden in _MODE_FORBIDDEN.get(mode, {}).items()
        if field in report and report[field] in forbidden
    )

    if problems:
        raise RuntimeModeError("runtime mode inconsistency: " + "; ".join(sorted(problems)))


#: What ``validate_runtime_disabled`` has always demanded be PRESENT and off.
_LEGACY_OFFLINE_REQUIRED = ("runtime_activation", "freeze_ready", "a5_status")


def validate_runtime_disabled(report: dict[str, Any]) -> None:
    """Back-compatible guard: assert the report is offline-evidence shaped.

    Preserves the exact legacy contract — ``runtime_activation``, ``freeze_ready``
    and ``a5_status`` must each be PRESENT and off. The old implementation used
    ``report.get(...) is not False``, so a missing field raised; that must keep
    raising, because this is the guard the quant-engine runners rely on.

    New code should call ``validate_runtime_mode`` and say which mode it ran in.
    """
    validate_runtime_mode(report, mode=OFFLINE_EVIDENCE, required=_LEGACY_OFFLINE_REQUIRED)


# --------------------------------------------------------------------------- #
# Request <-> result pairing
# --------------------------------------------------------------------------- #
#: Sleeve governance statuses an activated run may consume. A run that publishes
#: live outputs must not be built on a sleeve nobody approved.
APPROVED_SLEEVE_STATUSES = frozenset({"approved", "activated"})
CANDIDATE_SLEEVE_STATUS = "candidate_not_approved"


def request_runtime_mode(request: dict[str, Any]) -> str:
    """The mode a request declares. Absent means the conservative one."""
    mode = request.get("runtime_mode", OFFLINE_EVIDENCE)
    if mode not in RUNTIME_MODES:
        raise RuntimeModeError(
            f"unknown runtime mode {mode!r} (expected one of {', '.join(RUNTIME_MODES)})"
        )
    return str(mode)


def validate_sleeve_governance(request: dict[str, Any], *, mode: str | None = None) -> None:
    """The sleeve a run consumes must match the envelope the run declares.

    The v3 request pinned ``sleeve.status`` to ``candidate_not_approved`` with a
    ``const``, so a productive run that publishes live outputs could only be
    expressed by declaring the sleeve UNAPPROVED — the contract forced the
    publication to disown its own input governance. The status is now coupled to
    the runtime mode, in both directions:

    * an activated run must consume an approved (or activated) sleeve;
    * an offline evidence run must consume the unapproved candidate, so evidence
      cannot quietly borrow production's approval.
    """
    mode = mode if mode is not None else request_runtime_mode(request)
    sleeve = request.get("sleeve")
    if not isinstance(sleeve, dict) or "status" not in sleeve:
        raise RuntimeModeError(
            "sleeve governance inconsistency: the request declares no sleeve status; "
            "an absent governance field is not an assertion"
        )
    status = sleeve["status"]
    if mode == ACTIVATED and status not in APPROVED_SLEEVE_STATUSES:
        raise RuntimeModeError(
            f"sleeve governance inconsistency: mode 'activated' publishes live outputs "
            f"but the sleeve status is {status!r} (expected one of "
            f"{', '.join(sorted(APPROVED_SLEEVE_STATUSES))})"
        )
    if mode == OFFLINE_EVIDENCE and status != CANDIDATE_SLEEVE_STATUS:
        raise RuntimeModeError(
            f"sleeve governance inconsistency: mode 'offline_evidence' must consume the "
            f"unapproved candidate sleeve, got {status!r}"
        )


def validate_request_result_pair(request: dict[str, Any], result: dict[str, Any]) -> None:
    """Bind a job REQUEST and its RESULT to one runtime envelope.

    Either half alone can be self-consistent while the pair is nonsense: an
    offline request paired with a productive result, or an activated request
    whose result reports nothing activated. This is the seam where that is
    caught, and it is fail-closed on silence like everything else here.
    """
    mode = request_runtime_mode(request)
    if "job_type" in request and "job_type" in result and request["job_type"] != result["job_type"]:
        raise RuntimeModeError(
            f"request/result mismatch: job_type {request['job_type']!r} != {result['job_type']!r}"
        )
    if "sleeve" in request:
        validate_sleeve_governance(request, mode=mode)
    validate_runtime_mode(result, mode=mode)
