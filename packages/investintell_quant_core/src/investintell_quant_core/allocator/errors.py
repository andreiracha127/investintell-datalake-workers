"""Structured allocator failures shared by pure core clients."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class AllocatorErrorCode(StrEnum):
    POLICY_INFEASIBLE = "POLICY_INFEASIBLE"
    MISSING_REQUIRED_SLEEVES = "MISSING_REQUIRED_SLEEVES"
    SOLVER_FAILED = "SOLVER_FAILED"
    CONSTRAINT_VIOLATION = "CONSTRAINT_VIOLATION"


@dataclass(frozen=True)
class AllocatorIssue:
    code: AllocatorErrorCode
    message: str
    constraint_label: str | None = None
    observed: float | None = None
    lower_bound: float | None = None
    upper_bound: float | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


class AllocatorCoreError(ValueError):
    """Exception adapter for callers that cannot consume result objects."""

    def __init__(self, issue: AllocatorIssue) -> None:
        self.issue = issue
        super().__init__(str(issue))
