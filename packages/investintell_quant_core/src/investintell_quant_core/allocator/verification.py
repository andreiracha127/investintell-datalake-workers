"""Pure projection and post-solve verification for Plan C books."""

from __future__ import annotations

import numpy as np

from .contracts import CompiledProblem, VerificationResult
from .errors import AllocatorErrorCode, AllocatorIssue


def project_book(problem: CompiledProblem, x: object) -> np.ndarray:
    x_array = np.asarray(x, dtype=float).ravel()
    if x_array.shape != (len(problem.category_ids),):
        raise ValueError(
            f"category solution has shape {x_array.shape}, expected "
            f"({len(problem.category_ids)},)"
        )
    return problem.M @ x_array


def realized_cvar(weights: object, scenarios: object, alpha: float) -> float:
    weight_array = np.asarray(weights, dtype=float).ravel()
    scenario_array = np.asarray(scenarios, dtype=float)
    rows = scenario_array.shape[0]
    if rows == 0:
        raise ValueError("scenarios must have at least one row")
    if not 0 < alpha < 1:
        raise ValueError(f"alpha must be in (0, 1), got {alpha}")
    losses = -scenario_array @ weight_array
    k = max(int(np.ceil(np.round((1.0 - alpha) * rows, 8))), 1)
    threshold = float(np.partition(losses, -k)[-k])
    excess = np.maximum(losses - threshold, 0.0)
    return float(threshold + excess.sum() / ((1.0 - alpha) * rows))


def verify_solution(
    problem: CompiledProblem,
    x: object,
    y: object,
) -> VerificationResult:
    x_array = np.asarray(x, dtype=float).ravel()
    y_array = np.asarray(y, dtype=float).ravel()
    issues: list[AllocatorIssue] = []
    expected_x_shape = (len(problem.category_ids),)
    expected_y_shape = (len(problem.instrument_ids),)
    if x_array.shape != expected_x_shape:
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.CONSTRAINT_VIOLATION,
                message=f"category solution has shape {x_array.shape}, expected {expected_x_shape}",
                constraint_label="x_shape",
            )
        )
    if y_array.shape != expected_y_shape:
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.CONSTRAINT_VIOLATION,
                message=f"final book has shape {y_array.shape}, expected {expected_y_shape}",
                constraint_label="y_shape",
            )
        )
    if issues:
        return VerificationResult(x=x_array, y=y_array, issues=tuple(issues))
    if not np.isfinite(x_array).all() or not np.isfinite(y_array).all():
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.CONSTRAINT_VIOLATION,
                message="solution contains non-finite weights",
                constraint_label="finiteness",
            )
        )
        return VerificationResult(x=x_array, y=y_array, issues=tuple(issues))

    tolerance = problem.tolerances
    projected = problem.M @ x_array
    projection_error = float(np.max(np.abs(y_array - projected), initial=0.0))
    if projection_error > tolerance.constraint:
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.CONSTRAINT_VIOLATION,
                message=f"final book does not equal M_t x (max error {projection_error})",
                constraint_label="y=M_t x",
                observed=projection_error,
                upper_bound=tolerance.constraint,
            )
        )
    total = float(y_array.sum())
    if abs(total - 1.0) > tolerance.sum:
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.CONSTRAINT_VIOLATION,
                message=f"final weights sum {total}",
                constraint_label="sum",
                observed=total,
                lower_bound=1.0,
                upper_bound=1.0,
            )
        )
    if (y_array < -tolerance.weight).any() or (x_array < -tolerance.weight).any():
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.CONSTRAINT_VIOLATION,
                message="negative weight",
                constraint_label="non_negative",
            )
        )

    for constraint in problem.linear_constraints:
        value = float(constraint.coef @ x_array)
        if constraint.hi is not None and value > constraint.hi + tolerance.constraint:
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.CONSTRAINT_VIOLATION,
                    message=f"{constraint.label}={value} > {constraint.hi}",
                    constraint_label=constraint.label,
                    observed=value,
                    upper_bound=constraint.hi,
                )
            )
        if constraint.lo is not None and value < constraint.lo - tolerance.constraint:
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.CONSTRAINT_VIOLATION,
                    message=f"{constraint.label}={value} < {constraint.lo}",
                    constraint_label=constraint.label,
                    observed=value,
                    lower_bound=constraint.lo,
                )
            )

    sleeve_vector = problem.S @ x_array
    sleeve_weights = {
        sleeve_id: float(sleeve_vector[i])
        for i, sleeve_id in enumerate(problem.sleeve_ids)
    }
    for block in problem.blocks:
        value = float(x_array[list(block.indices)].sum())
        if value < block.lo - tolerance.constraint or value > block.hi + tolerance.constraint:
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.CONSTRAINT_VIOLATION,
                    message=f"{block.label}={value} outside [{block.lo}, {block.hi}]",
                    constraint_label=block.label,
                    observed=value,
                    lower_bound=block.lo,
                    upper_bound=block.hi,
                )
            )

    cvar = realized_cvar(y_array, problem.daily_returns, problem.cvar_alpha)
    if cvar > problem.cvar_limit + tolerance.cvar:
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.CONSTRAINT_VIOLATION,
                message=f"CVaR {cvar} exceeds {problem.cvar_limit}",
                constraint_label="cvar_limit",
                observed=cvar,
                upper_bound=problem.cvar_limit,
            )
        )
    return VerificationResult(
        x=x_array,
        y=np.clip(y_array, 0.0, None),
        sleeve_weights=sleeve_weights,
        realized_cvar=cvar,
        issues=tuple(issues),
    )
