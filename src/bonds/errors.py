"""Stable, inspectable domain errors for the pure bonds library."""

from __future__ import annotations

from typing import Any, Mapping


class BondError(ValueError):
    """A domain error whose stable ``code`` is safe for callers to inspect."""

    def __init__(self, code: str, details: Mapping[str, Any] | None = None) -> None:
        self.code = code
        self.details = dict(details or {})
        super().__init__(f"{code}: {self.details}" if self.details else code)
