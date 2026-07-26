"""Pure Plan C allocator contracts, compiler, preflight, and verification."""

from .compiler import compile_problem
from .contracts import (
    AllocatorPolicy,
    AllocatorUniverse,
    BlockBudget,
    CompileResult,
    CompiledProblem,
    DecisionMode,
    Instrument,
    LinearConstraint,
    PreflightResult,
    SleeveBand,
    SolveResult,
    Tolerances,
    VerificationResult,
)
from .errors import AllocatorCoreError, AllocatorErrorCode, AllocatorIssue
from .preflight import structural_preflight
from .verification import project_book, realized_cvar, verify_solution

__all__ = [
    "AllocatorCoreError",
    "AllocatorErrorCode",
    "AllocatorIssue",
    "AllocatorPolicy",
    "AllocatorUniverse",
    "BlockBudget",
    "CompileResult",
    "CompiledProblem",
    "DecisionMode",
    "Instrument",
    "LinearConstraint",
    "PreflightResult",
    "SleeveBand",
    "SolveResult",
    "Tolerances",
    "VerificationResult",
    "compile_problem",
    "project_book",
    "realized_cvar",
    "structural_preflight",
    "verify_solution",
]
