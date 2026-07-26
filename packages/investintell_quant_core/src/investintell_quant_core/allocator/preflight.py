"""Solver-free structural preflight for compiled allocator problems."""

from __future__ import annotations

import numpy as np

from .contracts import CompiledProblem, PreflightResult
from .errors import AllocatorErrorCode, AllocatorIssue


def structural_preflight(problem: CompiledProblem) -> PreflightResult:
    issues: list[AllocatorIssue] = []
    n_categories = len(problem.category_ids)
    n_instruments = len(problem.instrument_ids)
    n_sleeves = len(problem.sleeve_ids)
    n_rows = len(problem.return_dates)

    expected_shapes = {
        "S": (n_sleeves, n_categories),
        "M": (n_instruments, n_categories),
        "daily_returns": (n_rows, n_instruments),
        "category_returns": (n_rows, n_categories),
    }
    arrays = {
        "S": problem.S,
        "M": problem.M,
        "daily_returns": problem.daily_returns,
        "category_returns": problem.category_returns,
    }
    for label, expected in expected_shapes.items():
        value = arrays[label]
        if value.shape != expected:
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.POLICY_INFEASIBLE,
                    message=f"{label} has shape {value.shape}, expected {expected}",
                    constraint_label=label,
                )
            )
        elif not np.isfinite(value).all():
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.POLICY_INFEASIBLE,
                    message=f"{label} contains non-finite values",
                    constraint_label=label,
                )
            )
    if issues:
        return PreflightResult(tuple(issues))

    tolerance = problem.tolerances.constraint
    if (problem.S < -tolerance).any() or not np.allclose(
        problem.S.sum(axis=0), 1.0, atol=tolerance, rtol=0.0
    ):
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.POLICY_INFEASIBLE,
                message="S must map every category to exactly one sleeve",
                constraint_label="S",
            )
        )
    if (problem.M < -tolerance).any() or not np.allclose(
        problem.M.sum(axis=0), 1.0, atol=tolerance, rtol=0.0
    ):
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.POLICY_INFEASIBLE,
                message="M_t must map every category to a fully invested instrument book",
                constraint_label="M_t",
            )
        )
    if not np.allclose(
        problem.category_returns,
        problem.daily_returns @ problem.M,
        atol=tolerance,
        rtol=0.0,
    ):
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.POLICY_INFEASIBLE,
                message="category returns do not equal daily_returns @ M_t",
                constraint_label="category_returns",
            )
        )

    block_indices: set[int] = set()
    for block in problem.blocks:
        if not block.indices or len(set(block.indices)) != len(block.indices):
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.POLICY_INFEASIBLE,
                    message=f"{block.label} has empty or duplicate category indices",
                    constraint_label=block.label,
                )
            )
            continue
        if min(block.indices) < 0 or max(block.indices) >= n_categories:
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.POLICY_INFEASIBLE,
                    message=f"{block.label} contains an out-of-range category index",
                    constraint_label=block.label,
                )
            )
        if not 0 <= block.lo <= block.hi <= 1:
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.POLICY_INFEASIBLE,
                    message=f"{block.label} has invalid bounds [{block.lo}, {block.hi}]",
                    constraint_label=block.label,
                    lower_bound=block.lo,
                    upper_bound=block.hi,
                )
            )
        overlap = block_indices.intersection(block.indices)
        if overlap:
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.POLICY_INFEASIBLE,
                    message=f"category indices occur in multiple sleeve blocks: {sorted(overlap)}",
                    constraint_label=block.label,
                )
            )
        block_indices.update(block.indices)

    if block_indices != set(range(n_categories)):
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.POLICY_INFEASIBLE,
                message="sleeve blocks do not cover every category exactly once",
                constraint_label="sleeve_blocks",
            )
        )
    if sum(block.lo for block in problem.blocks) > 1 + tolerance:
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.POLICY_INFEASIBLE,
                message="sum of sleeve floors exceeds 1",
                constraint_label="sleeve_blocks",
            )
        )
    if sum(block.hi for block in problem.blocks) < 1 - tolerance:
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.POLICY_INFEASIBLE,
                message="sum of sleeve caps is below 1",
                constraint_label="sleeve_blocks",
            )
        )

    for constraint in problem.linear_constraints:
        if constraint.coef.shape != (n_categories,) or not np.isfinite(
            constraint.coef
        ).all():
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.POLICY_INFEASIBLE,
                    message=f"{constraint.label} has invalid coefficients",
                    constraint_label=constraint.label,
                )
            )
        if (
            constraint.lo is not None
            and constraint.hi is not None
            and constraint.lo > constraint.hi
        ):
            issues.append(
                AllocatorIssue(
                    code=AllocatorErrorCode.POLICY_INFEASIBLE,
                    message=f"{constraint.label} lower bound exceeds its upper bound",
                    constraint_label=constraint.label,
                    lower_bound=constraint.lo,
                    upper_bound=constraint.hi,
                )
            )

    if not 0 < problem.cvar_alpha < 1:
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.POLICY_INFEASIBLE,
                message=f"CVaR alpha must be in (0, 1), got {problem.cvar_alpha}",
                constraint_label="cvar_alpha",
            )
        )
    if not np.isfinite(problem.cvar_limit):
        issues.append(
            AllocatorIssue(
                code=AllocatorErrorCode.POLICY_INFEASIBLE,
                message="CVaR limit must be finite",
                constraint_label="cvar_limit",
            )
        )
    return PreflightResult(tuple(issues))
