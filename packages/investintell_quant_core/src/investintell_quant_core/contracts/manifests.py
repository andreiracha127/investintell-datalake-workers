"""Manifest contract validation."""

from __future__ import annotations

from typing import Any


def validate_feature_manifest_contract(manifest: dict[str, Any]) -> None:
    if manifest.get("parameter_independent") is not True:
        raise ValueError("feature_manifest must be parameter_independent=true")
    if manifest.get("counterfactual_runtime_allowed") is not False:
        raise ValueError("feature_manifest must forbid counterfactual runtime use")
    roles = manifest.get("selection_roles") or {}
    if roles.get("latest") != "pit_runtime_candidate":
        raise ValueError("feature_manifest.latest must be pit_runtime_candidate")
    if roles.get("first_release") != "revised_vintage_counterfactual":
        raise ValueError("feature_manifest.first_release must be revised_vintage_counterfactual")

