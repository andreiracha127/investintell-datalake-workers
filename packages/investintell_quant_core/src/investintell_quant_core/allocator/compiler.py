"""Pure deterministic compiler for Plan C category allocation problems."""

from __future__ import annotations

import hashlib
import json

import numpy as np

from .contracts import (
    AllocatorPolicy,
    AllocatorUniverse,
    BlockBudget,
    CompileResult,
    CompiledProblem,
    LinearConstraint,
    Tolerances,
)
from .errors import AllocatorErrorCode, AllocatorIssue


def _issue(
    code: AllocatorErrorCode,
    message: str,
    **context: object,
) -> CompileResult:
    return CompileResult(
        problem=None,
        issues=(AllocatorIssue(code=code, message=message, context=dict(context)),),
    )


def _signature(
    *,
    category_ids: tuple[str, ...],
    instrument_ids: tuple[str, ...],
    M: np.ndarray,
    return_dates: tuple[str, ...],
    linear: tuple[LinearConstraint, ...],
    mapping_version: str,
) -> str:
    constraints = [
        {
            "label": constraint.label,
            "lo": constraint.lo,
            "hi": constraint.hi,
            "coef": np.round(constraint.coef, 12).tolist(),
        }
        for constraint in linear
    ]
    payload = {
        "category_ids": category_ids,
        "instrument_ids": instrument_ids,
        "M": np.round(M, 12).tolist(),
        "return_dates": return_dates,
        "constraints": constraints,
        "mapping_version": mapping_version,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def compile_problem(
    universe: AllocatorUniverse,
    policy: AllocatorPolicy,
    *,
    tolerances: Tolerances = Tolerances(),
) -> CompileResult:
    """Compile ``S``, ``M_t`` and every linear policy row from materialized input."""
    instruments = universe.instruments
    if not instruments:
        return _issue(
            AllocatorErrorCode.POLICY_INFEASIBLE,
            "compiled allocator universe is empty",
        )
    if universe.decision_mode not in {"category_equal_weight", "instrument"}:
        return _issue(
            AllocatorErrorCode.POLICY_INFEASIBLE,
            f"unsupported allocator decision mode {universe.decision_mode!r}",
        )
    if len(set(universe.sleeve_ids)) != len(universe.sleeve_ids):
        return _issue(
            AllocatorErrorCode.POLICY_INFEASIBLE,
            "allocator sleeve order contains duplicates",
        )
    if len(set(item.instrument_id for item in instruments)) != len(instruments):
        return _issue(
            AllocatorErrorCode.POLICY_INFEASIBLE,
            "allocator instrument ids must be unique",
        )

    expected_rows = len(universe.return_dates)
    for item in instruments:
        if item.returns.shape != (expected_rows,) or not np.isfinite(item.returns).all():
            return _issue(
                AllocatorErrorCode.POLICY_INFEASIBLE,
                f"active instrument {item.instrument_id} has missing or non-finite return history",
                instrument_id=item.instrument_id,
            )

    band_by_sleeve = {band.sleeve_id: band for band in policy.sleeve_bands}
    if set(band_by_sleeve) != set(universe.sleeve_ids):
        return _issue(
            AllocatorErrorCode.POLICY_INFEASIBLE,
            "policy sleeve bands do not match the canonical sleeve order",
        )

    category_ids: list[str] = []
    category_sleeve: dict[str, str] = {}
    category_members: dict[str, list[int]] = {}
    for instrument_index, item in enumerate(instruments):
        decision_id = (
            f"{item.category_id}::{item.instrument_id}"
            if universe.decision_mode == "instrument"
            else item.category_id
        )
        known_sleeve = category_sleeve.get(decision_id)
        if known_sleeve is not None and known_sleeve != item.sleeve_id:
            return _issue(
                AllocatorErrorCode.POLICY_INFEASIBLE,
                f"decision atom {decision_id!r} maps to multiple sleeves",
                category_id=item.category_id,
            )
        if decision_id not in category_members:
            category_ids.append(decision_id)
            category_members[decision_id] = []
            category_sleeve[decision_id] = item.sleeve_id
        category_members[decision_id].append(instrument_index)

    instrument_ids = tuple(item.instrument_id for item in instruments)
    instrument_labels = tuple(item.label for item in instruments)
    category_ids_tuple = tuple(category_ids)
    category_sleeve_ids = tuple(category_sleeve[item] for item in category_ids)
    sleeve_index = {sleeve_id: i for i, sleeve_id in enumerate(universe.sleeve_ids)}

    unknown_sleeves = sorted(set(category_sleeve_ids) - set(sleeve_index))
    if unknown_sleeves:
        return _issue(
            AllocatorErrorCode.POLICY_INFEASIBLE,
            f"categories reference unknown sleeves: {', '.join(unknown_sleeves)}",
        )

    M = np.zeros((len(instruments), len(category_ids)), dtype=np.float64)
    for category_index, category_id in enumerate(category_ids):
        members = category_members[category_id]
        share = 1.0 / len(members)
        M[members, category_index] = share

    S = np.zeros((len(universe.sleeve_ids), len(category_ids)), dtype=np.float64)
    for category_index, sleeve_id in enumerate(category_sleeve_ids):
        S[sleeve_index[sleeve_id], category_index] = 1.0

    daily_returns = np.column_stack([item.returns for item in instruments])
    category_returns = daily_returns @ M

    blocks: list[BlockBudget] = []
    for sleeve_id in universe.sleeve_ids:
        band = band_by_sleeve[sleeve_id]
        indices = tuple(
            i for i, category_sleeve_id in enumerate(category_sleeve_ids)
            if category_sleeve_id == sleeve_id
        )
        if indices:
            blocks.append(
                BlockBudget(
                    indices=indices,
                    lo=band.lo,
                    hi=band.hi,
                    label=f"sleeve:{sleeve_id}",
                )
            )
        elif band.lo > 1e-12:
            code = (
                AllocatorErrorCode.MISSING_REQUIRED_SLEEVES
                if policy.strict_missing_sleeves
                else AllocatorErrorCode.POLICY_INFEASIBLE
            )
            return _issue(
                code,
                f"sleeve {sleeve_id!r} has floor {band.lo} but no active implementation",
                sleeve_id=sleeve_id,
            )

    linear: list[LinearConstraint] = []
    if policy.instrument_cap is not None:
        for i, instrument_id in enumerate(instrument_ids):
            linear.append(
                LinearConstraint(
                    coef=M[i, :],
                    lo=None,
                    hi=policy.instrument_cap,
                    label=f"instrument_cap:{instrument_id}",
                )
            )
    if policy.instrument_floor is not None and policy.instrument_floor > 0:
        for i, instrument_id in enumerate(instrument_ids):
            linear.append(
                LinearConstraint(
                    coef=M[i, :],
                    lo=policy.instrument_floor,
                    hi=None,
                    label=f"instrument_floor:{instrument_id}",
                )
            )

    if policy.risk_assets_cap is not None:
        linear.append(
            LinearConstraint(
                coef=np.array(
                    [
                        1.0 if sleeve in policy.risk_asset_sleeves else 0.0
                        for sleeve in category_sleeve_ids
                    ]
                ),
                lo=None,
                hi=policy.risk_assets_cap,
                label="risk_assets_cap",
            )
        )
    if policy.defensive_floor is not None:
        linear.append(
            LinearConstraint(
                coef=np.array(
                    [
                        1.0 if sleeve in policy.defensive_sleeves else 0.0
                        for sleeve in category_sleeve_ids
                    ]
                ),
                lo=policy.defensive_floor,
                hi=None,
                label="defensive_floor",
            )
        )

    for constraint in policy.instrument_linear_constraints:
        if constraint.coef.shape != (len(instruments),):
            return _issue(
                AllocatorErrorCode.POLICY_INFEASIBLE,
                f"instrument-space constraint {constraint.label!r} has unexpected shape",
                constraint_label=constraint.label,
            )
        linear.append(
            LinearConstraint(
                coef=constraint.coef @ M,
                lo=constraint.lo,
                hi=constraint.hi,
                label=constraint.label,
            )
        )

    linear_tuple = tuple(linear)
    signature = _signature(
        category_ids=category_ids_tuple,
        instrument_ids=instrument_ids,
        M=M,
        return_dates=universe.return_dates,
        linear=linear_tuple,
        mapping_version=universe.mapping_version,
    )
    return CompileResult(
        problem=CompiledProblem(
            category_ids=category_ids_tuple,
            category_sleeve_ids=category_sleeve_ids,
            sleeve_ids=universe.sleeve_ids,
            instrument_ids=instrument_ids,
            instrument_labels=instrument_labels,
            S=S,
            M=M,
            daily_returns=daily_returns,
            category_returns=category_returns,
            return_dates=universe.return_dates,
            blocks=tuple(blocks),
            linear_constraints=linear_tuple,
            cvar_alpha=policy.cvar_alpha,
            cvar_limit=policy.cvar_limit,
            min_weight=policy.instrument_floor,
            tolerances=tolerances,
            as_of=universe.return_dates[-1] if universe.return_dates else "",
            mapping_version=universe.mapping_version,
            signature=signature,
        )
    )
