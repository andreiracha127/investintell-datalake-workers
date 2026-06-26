"""Preflight checks for offline quant-engine jobs."""

from __future__ import annotations

from typing import Any


def validate_offline_request(*, offline: bool, jobs: int) -> None:
    if offline is not True:
        raise ValueError("quant-engine jobs must declare offline=true")
    if jobs < 1:
        raise ValueError("jobs must be >= 1")


def validate_runtime_disabled(report: dict[str, Any]) -> None:
    if report.get("runtime_activation") is not False:
        raise ValueError("runtime_activation must remain false")
    if report.get("freeze_ready") is not False:
        raise ValueError("freeze_ready must remain false")
    if report.get("a5_status") != "blocked":
        raise ValueError("A5 must remain blocked")

