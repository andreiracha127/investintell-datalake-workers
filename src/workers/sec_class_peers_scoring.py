"""DB-free, artifact-only governed SEC class peer/pillar evidence (v2)."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import stat
import tempfile
from typing import Any, Mapping

from src.workers import nport_v2_lookthrough as w2a
from src.workers import sec_class_factors as factors

ArtifactValidationError = factors.ArtifactValidationError
MATRIX_SCHEMA = "sec_class_policy_matrix/v2"
INPUT_SCHEMA = "sec_class_peer_scoring_input/v2"
RUN_SCHEMA = "sec_class_peer_scoring_run/v2"
EVIDENCE_SCHEMA = "sec_class_peer_evidence_manifest/v1"
PILLARS = (
    "data_quality",
    "investment",
    "operational_quality",
    "cost_efficiency",
    "disclosure_consistency",
)
_DIMENSIONS = ("duration", "structure", "region", "vehicle")
_QUALITY = {"certified", "degraded", "stale", "opaque"}
_STATES = {"available", "unavailable", "not_applicable"}
DIAGNOSTIC_IDS = (
    "provider_affiliation",
    "provider_change",
    "disclosure_volume",
    "nav_correction",
    "event_existence_text",
)
QUALITY_PENALTY = {
    "certified": 1.0,
    "degraded": 0.8,
    "stale": 0.5,
    "opaque": 0.0,
    "unavailable": 0.0,
}
_SOURCES = {
    "factor",
    "regulatory_numeric",
    "operational_numeric",
    "fee_numeric",
    "disclosure_numeric",
}
_DIAGNOSTIC_SOURCE_CLASSES = {"provider_diagnostic", "event_diagnostic", "nav_diagnostic", "disclosure_diagnostic"}
APPROVED_POLICY_MATRIX = (
    ("Cash Equivalent", "cash", "BIL", "cash", "BIL"),
    ("Government Money Market", "cash", "BIL", "cash", "BIL"),
    ("Large Blend", "equity", "IVV", "equity", "IVV"),
    ("Large Growth", "equity", "QQQ", "equity", "IVV"),
    ("Large Value", "equity", "VOOV", "equity", "IVV"),
    ("Mid Blend", "equity", "SCHM", "equity", "IVV"),
    ("Mid Growth", "equity", "IWP", "equity", "IVV"),
    ("Mid Value", "equity", "IWS", "equity", "IVV"),
    ("Small Blend", "equity", "IWM", "equity", "IVV"),
    ("Small Growth", "equity", "IWO", "equity", "IVV"),
    ("Small Value", "equity", "IWN", "equity", "IVV"),
    ("Emerging Markets Equity", "equity", "IEMG", "equity", "IVV"),
    ("European Equity", "equity", "FEZ", "equity", "IVV"),
    ("Global Equity", "equity", "VT", "equity", "IVV"),
    ("Asian Equity", "equity", "AAXJ", "equity", "IVV"),
    ("International Equity", "equity", "IEFA", "equity", "IVV"),
    ("Technology", "equity", "XLK", "thematic", "XLK"),
    ("Energy Equity", "equity", "XLE", "thematic", "XLK"),
    ("Health Care Equity", "equity", "XLV", "thematic", "XLK"),
    ("Financials Equity", "equity", "XLF", "thematic", "XLK"),
    ("Industrials Equity", "equity", "XLI", "thematic", "XLK"),
    ("Infrastructure Equity", "equity", "IFRA", "thematic", "XLK"),
    ("Materials Equity", "equity", "XLB", "thematic", "XLK"),
    ("Natural Resources Equity", "equity", "GUNR", "thematic", "XLK"),
    ("Communication Services Equity", "equity", "XLC", "thematic", "XLK"),
    ("Consumer Discretionary Equity", "equity", "XLY", "thematic", "XLK"),
    ("Consumer Staples Equity", "equity", "XLP", "thematic", "XLK"),
    ("Utilities Equity", "equity", "XLU", "thematic", "XLK"),
    ("Investment Grade Bond", "fixed_income", "LQD", "fixed_income", "GOVT"),
    ("Government Bond", "fixed_income", "GOVT", "fixed_income", "GOVT"),
    ("High Yield Bond", "fixed_income", "HYG", "fixed_income", "GOVT"),
    ("Inflation-Linked Bond", "fixed_income", "TIP", "fixed_income", "GOVT"),
    ("Short-Term Bond", "fixed_income", "SHY", "fixed_income", "GOVT"),
    ("Intermediate-Term Bond", "fixed_income", "BND", "fixed_income", "GOVT"),
    ("Long-Term Bond", "fixed_income", "TLT", "fixed_income", "GOVT"),
    ("Municipal Bond", "fixed_income", "MUB", "fixed_income", "GOVT"),
    ("Mortgage-Backed Securities", "fixed_income", "MBB", "fixed_income", "GOVT"),
    ("Structured Credit", "fixed_income", "PAAA", "fixed_income", "GOVT"),
    ("Asset-Backed Securities", "fixed_income", "DEED", "fixed_income", "GOVT"),
    ("Real Estate", "alternatives", "VNQ", "alternatives", "QAI"),
    ("Commodities", "alternatives", "GCC", "alternatives", "QAI"),
    ("Alternative", "alternatives", "QAI", "alternatives", "QAI"),
    ("Multi-Asset", "multi_asset", "AOR", "alternatives", "QAI"),
    ("Precious Metals", "alternatives", "RING", "alternatives", "QAI"),
    ("Long/Short Equity", "equity", "FTLS", "long_short", "FTLS"),
)
_MATRIX = {x[0]: x[1:] for x in APPROVED_POLICY_MATRIX}


def _definitions(broad: str, pillar: str) -> tuple[dict[str, Any], ...]:
    """Closed v1 registry; definitions are data, never caller-selected strings."""
    if broad == "fixed_income" and pillar == "investment":
        return (
            {
                "id": "fixed_income.investment.rates.v1", "weight": 0.65,
                "direction": "higher", "source_kind": "factor",
                "methodology_id": "sec-class-factor-ols-hac", "methodology_version": "v1",
                "minimum_coverage_pct": 70.0, "minimum_effective_weight": 0.70,
                "factor": "rates",
            },
            {
                "id": "fixed_income.investment.credit_spread.v1", "weight": 0.25,
                "direction": "higher", "source_kind": "factor",
                "methodology_id": "sec-class-factor-ols-hac", "methodology_version": "v1",
                "minimum_coverage_pct": 70.0, "minimum_effective_weight": 0.70,
                "factor": "credit_spread",
            },
            {
                "id": "fixed_income.investment.duration_spread.v1", "weight": 0.10,
                "direction": "lower", "source_kind": "regulatory_numeric",
                "methodology_id": "sec-class-investment-duration-spread", "methodology_version": "v1",
                "minimum_coverage_pct": 70.0, "minimum_effective_weight": 0.70,
                "factor": None,
            },
        )
    source = {
        "data_quality": "disclosure_numeric",
        "operational_quality": "operational_numeric",
        "cost_efficiency": "fee_numeric",
        "disclosure_consistency": "disclosure_numeric",
    }.get(pillar, "factor")
    direction = "lower" if pillar == "cost_efficiency" else "higher"
    weights = (0.75, 0.25) if broad == "fixed_income" else (0.65, 0.35)
    names = (
        ("rates", "credit_spread")
        if broad == "fixed_income" and pillar == "investment"
        else ("latent_factor_1", "latent_factor_2")
        if broad == "equity" and pillar == "investment"
        else ("trend", "carry")
        if broad == "alternatives" and pillar == "investment"
        else ("rates", "credit_spread")
        if broad == "multi_asset" and pillar == "investment"
        else ("not_applicable", "not_applicable_aux")
        if broad == "cash" and pillar == "investment"
        else ("freshness", "completeness")
        if pillar == "data_quality"
        else ("primary", "secondary")
    )
    return tuple(
        {
            "id": f"{broad}.{pillar}.{name}.v1",
            "weight": weight,
            "direction": direction,
            "source_kind": "factor"
            if pillar == "investment" and broad != "cash"
            else source,
            "methodology_id": "sec-class-factor-ols-hac"
            if pillar == "investment"
            else f"sec-class-{pillar}-{name}",
            "methodology_version": "v1",
            "minimum_coverage_pct": 70.0,
            "minimum_effective_weight": 0.70,
            "factor": name if pillar == "investment" and broad != "cash" else None,
        }
        for name, weight in zip(names, weights)
    )


COMPONENT_POLICY = {
    b: {p: _definitions(b, p) for p in PILLARS}
    for b in ("cash", "equity", "fixed_income", "alternatives", "multi_asset")
}


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode()


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _sha(value: Any, where: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value.lower())
    ):
        raise ArtifactValidationError(f"{where} must be SHA-256")
    return value


def _git(value: Any, where: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(c not in "0123456789abcdef" for c in value.lower())
    ):
        raise ArtifactValidationError(f"{where} must be Git SHA")
    return value


def _finite(value: Any) -> bool:
    return (
        isinstance(value, (float, int))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _hash(value: Mapping[str, Any], where: str) -> None:
    body = dict(value)
    claimed = body.pop("content_hash", None)
    if claimed != canonical_sha256(body):
        raise ArtifactValidationError(f"{where} self-hash mismatch")


def validate_evidence_manifest(value: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Authenticate the separately supplied typed evidence registry."""
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "state", "entries", "content_hash"}
        or value["schema_version"] != EVIDENCE_SCHEMA or value["state"] != "complete"
    ):
        raise ArtifactValidationError("evidence manifest schema mismatch")
    _hash(value, "evidence manifest")
    fields = {"source_ref_id", "source_class", "artifact_sha256", "measure", "methodology_id", "methodology_version", "diagnostic_only"}
    if not isinstance(value["entries"], list) or not value["entries"]:
        raise ArtifactValidationError("evidence manifest entries required")
    parsed = {}
    for entry in value["entries"]:
        if (
            not isinstance(entry, dict) or set(entry) != fields
            or not all(isinstance(entry[key], str) and entry[key] for key in fields - {"diagnostic_only"})
            or type(entry["diagnostic_only"]) is not bool
        ):
            raise ArtifactValidationError("evidence manifest entry invalid")
        _sha(entry["artifact_sha256"], "evidence artifact")
        if entry["source_ref_id"] in parsed:
            raise ArtifactValidationError("evidence manifest duplicate source ref")
        parsed[entry["source_ref_id"]] = entry
    if list(parsed) != sorted(parsed):
        raise ArtifactValidationError("evidence manifest entries must be sorted")
    return parsed


def validate_policy_matrix(
    value: Mapping[str, Any],
) -> dict[str, tuple[str, str, str, str]]:
    if (
        not isinstance(value, dict)
        or set(value)
        != {"schema_version", "policy_id", "policy_version", "entries", "content_hash"}
        or value["schema_version"] != MATRIX_SCHEMA
    ):
        raise ArtifactValidationError("matrix schema mismatch")
    _hash(value, "matrix")
    if not all(
        isinstance(value[k], str) and value[k] for k in ("policy_id", "policy_version")
    ) or not isinstance(value["entries"], list):
        raise ArtifactValidationError("matrix identity invalid")
    parsed = {}
    fields = {
        "classification_label",
        "broad_class",
        "approved_proxy",
        "optimizer_sleeve",
        "benchmark_family",
    }
    for row in value["entries"]:
        if (
            not isinstance(row, dict)
            or set(row) != fields
            or not all(isinstance(row[k], str) and row[k] for k in fields)
        ):
            raise ArtifactValidationError("matrix row schema mismatch")
        if row["classification_label"] in parsed:
            raise ArtifactValidationError("duplicate label")
        parsed[row["classification_label"]] = tuple(
            row[k]
            for k in (
                "broad_class",
                "approved_proxy",
                "optimizer_sleeve",
                "benchmark_family",
            )
        )
    if len(parsed) != 45 or parsed != _MATRIX:
        raise ArtifactValidationError("matrix must contain exact approved 45 labels")
    return parsed


def _component(value: Any, definition: Mapping[str, Any], evidence: Mapping[str, Any]) -> dict[str, Any]:
    fields = {
        "component_id",
        "state",
        "value",
        "coverage_pct",
        "quality",
        "source_kind",
        "source_refs",
        "methodology_id",
        "methodology_version",
        "direction",
        "diagnostic_only",
        "factor_path",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ArtifactValidationError("component envelope schema mismatch")
    if (
        value["component_id"] != definition["id"]
        or value["direction"] != definition["direction"]
        or value["source_kind"] != definition["source_kind"]
        or value["methodology_id"] != definition["methodology_id"]
        or value["methodology_version"] != definition["methodology_version"]
    ):
        raise ArtifactValidationError("component policy binding mismatch")
    if (
        value["state"] not in _STATES
        or value["quality"] not in _QUALITY
        or type(value["diagnostic_only"]) is not bool
        or not isinstance(value["source_refs"], list)
        or not value["source_refs"]
        or not all(isinstance(x, str) and x for x in value["source_refs"])
        or not all(
            isinstance(value[k], str) and value[k]
            for k in ("methodology_id", "methodology_version")
        )
        or not _finite(value["coverage_pct"])
        or not 0 <= value["coverage_pct"] <= 100
    ):
        raise ArtifactValidationError("component envelope invalid")
    if definition["source_kind"] == "factor":
        for source_ref in value["source_refs"]:
            _sha(source_ref, "factor source ref")
    else:
        if len(value["source_refs"]) != 1 or value["source_refs"][0] not in evidence:
            raise ArtifactValidationError("nonfactor evidence reference mismatch")
        item = evidence[value["source_refs"][0]]
        if (
            item["diagnostic_only"] or item["source_class"] != definition["source_kind"]
            or item["measure"] != definition["id"]
            or item["methodology_id"] != definition["methodology_id"]
            or item["methodology_version"] != definition["methodology_version"]
        ):
            raise ArtifactValidationError("nonfactor evidence manifest binding mismatch")
    if definition["factor"] is None:
        if value["factor_path"] is not None:
            raise ArtifactValidationError("non-factor component path invalid")
    elif not isinstance(value["factor_path"], dict) or set(value["factor_path"]) != (
        {"sleeve_id", "factor"} if definition.get("sleeve_id") is not None else {"factor"}
    ) or value["factor_path"].get("factor") != definition["factor"] or not all(
        isinstance(item, str) and item for item in value["factor_path"].values()
    ):
        raise ArtifactValidationError("factor component path invalid")
    elif definition.get("sleeve_id") is not None and value["factor_path"].get("sleeve_id") != definition["sleeve_id"]:
        raise ArtifactValidationError("factor sleeve path invalid")
    if value["state"] == "available" and not _finite(value["value"]):
        raise ArtifactValidationError("available component requires finite value")
    if value["state"] != "available" and value["value"] is not None:
        raise ArtifactValidationError("nonavailable component cannot carry value")
    return value


def _diagnostics(value: Any, evidence: Mapping[str, Any]) -> list[dict[str, Any]]:
    fields = {
        "diagnostic_id", "diagnostic_only", "value", "methodology_id",
        "methodology_version", "source_refs",
    }
    if not isinstance(value, list) or len(value) != len(DIAGNOSTIC_IDS):
        raise ArtifactValidationError("diagnostic list closure mismatch")
    ids = []
    for row in value:
        if (
            not isinstance(row, dict) or set(row) != fields
            or row.get("diagnostic_only") is not True or row.get("value") is not None
            or row.get("diagnostic_id") not in DIAGNOSTIC_IDS
            or row.get("methodology_id") != f"sec-class-diagnostic-{row.get('diagnostic_id')}"
            or row.get("methodology_version") != "v1"
            or not isinstance(row.get("source_refs"), list) or not row["source_refs"]
        ):
            raise ArtifactValidationError("diagnostic schema mismatch")
        if len(row["source_refs"]) != 1 or row["source_refs"][0] not in evidence:
            raise ArtifactValidationError("diagnostic evidence reference mismatch")
        item = evidence[row["source_refs"][0]]
        if (
            not item["diagnostic_only"] or item["measure"] != row["diagnostic_id"]
            or item["methodology_id"] != row["methodology_id"]
            or item["methodology_version"] != row["methodology_version"]
            or item["source_class"] not in _DIAGNOSTIC_SOURCE_CLASSES
        ):
            raise ArtifactValidationError("diagnostic evidence manifest binding mismatch")
        ids.append(row["diagnostic_id"])
    if tuple(ids) != DIAGNOSTIC_IDS:
        raise ArtifactValidationError("diagnostic identity/order mismatch")
    return value


def _input(
    value: Mapping[str, Any],
    matrix: Mapping[str, tuple[str, str, str, str]],
    policy: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> list[dict[str, Any]]:
    fields = {
        "schema_version",
        "state",
        "snapshot",
        "methodology",
        "rows",
        "content_hash",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value["schema_version"] != INPUT_SCHEMA
        or value["state"] != "complete"
    ):
        raise ArtifactValidationError("input schema mismatch")
    _hash(value, "input")
    # Validation must never sort or rewrite the authenticated caller payload.
    value = copy.deepcopy(value)
    snapshot = value["snapshot"]
    if (
        not isinstance(snapshot, dict)
        or set(snapshot) != {"snapshot_id", "policy_id", "policy_version"}
        or not all(isinstance(x, str) and x for x in snapshot.values())
    ):
        raise ArtifactValidationError("snapshot schema mismatch")
    if (
        snapshot["policy_id"] != policy["policy_id"]
        or snapshot["policy_version"] != policy["policy_version"]
    ):
        raise ArtifactValidationError("matrix policy != snapshot")
    if (
        not isinstance(value["methodology"], dict)
        or set(value["methodology"]) != {"id", "version"}
        or not all(isinstance(x, str) and x for x in value["methodology"].values())
    ):
        raise ArtifactValidationError("methodology schema mismatch")
    row_fields = {
        "instrument_id",
        "series_id",
        "classification_label",
        "policy_id",
        "policy_version",
        "snapshot_id",
        "currency",
        "benchmark_family",
        "dimensions",
        "components",
        "diagnostics",
    }
    if not isinstance(value["rows"], list) or not value["rows"]:
        raise ArtifactValidationError("rows required")
    out = []
    ids = []
    for row in value["rows"]:
        if (
            not isinstance(row, dict)
            or set(row) != row_fields
            or not all(
                isinstance(row[k], str) and row[k]
                for k in row_fields - {"dimensions", "components", "diagnostics"}
            )
        ):
            raise ArtifactValidationError("row schema mismatch")
        ids.append(row["instrument_id"])
        if any(
            row[k] != snapshot[k]
            for k in ("policy_id", "policy_version", "snapshot_id")
        ):
            raise ArtifactValidationError("row snapshot mismatch")
        mapped = matrix.get(row["classification_label"])
        if mapped is None or row["benchmark_family"] != mapped[3]:
            raise ArtifactValidationError("matrix label mismatch")
        if not isinstance(row["dimensions"], dict) or set(row["dimensions"]) - set(
            _DIMENSIONS
        ):
            raise ArtifactValidationError("forbidden cohort dimension")
        if not all(
            isinstance(k, str) and isinstance(v, str) and v
            for k, v in row["dimensions"].items()
        ):
            raise ArtifactValidationError("dimension invalid")
        broad = mapped[0]
        expected = COMPONENT_POLICY[broad]
        if not isinstance(row["components"], dict) or set(row["components"]) != set(
            PILLARS
        ):
            raise ArtifactValidationError("five components required")
        for pillar in PILLARS:
            submitted = row["components"][pillar]
            if pillar == "investment":
                if not isinstance(submitted, list) or not submitted:
                    raise ArtifactValidationError("investment components required")
                continue
            definitions = expected[pillar]
            if not isinstance(submitted, list) or len(submitted) != len(definitions):
                raise ArtifactValidationError("component registry closure mismatch")
            if [
                item.get("component_id") if isinstance(item, dict) else None
                for item in submitted
            ] != [item["id"] for item in definitions]:
                raise ArtifactValidationError(
                    "component registry identity/order mismatch"
                )
            by_id = {
                item.get("component_id"): item
                for item in submitted
                if isinstance(item, dict)
            }
            if set(by_id) != {item["id"] for item in definitions}:
                raise ArtifactValidationError("component registry identity mismatch")
            row["components"][pillar] = [
                _component(by_id[item["id"]], item, evidence) for item in definitions
            ]
        row["diagnostics"] = _diagnostics(row["diagnostics"], evidence)
        out.append(row)
    if ids != sorted(ids) or len(ids) != len(set(ids)):
        raise ArtifactValidationError("instrument ids must be strictly ordered unique")
    return out


def _factor_definition(prefix: str, factor: str, weight: float, *, sleeve_id: str | None = None) -> dict[str, Any]:
    return {
        "id": f"{prefix}.{factor}.v1", "weight": weight, "direction": "higher",
        "source_kind": "factor", "methodology_id": "sec-class-factor-ols-hac",
        "methodology_version": "v1", "minimum_coverage_pct": 70.0,
        "minimum_effective_weight": 0.70, "factor": factor,
        "sleeve_id": sleeve_id,
    }


def _trusted_investment_definitions(raw: Mapping[str, Any], series: Mapping[str, Any], broad: str) -> tuple[dict[str, Any], ...]:
    if broad == "cash":
        return ({**_factor_definition("cash.investment", "not_applicable", 1.0), "factor": None},)
    if broad == "fixed_income":
        return COMPONENT_POLICY[broad]["investment"]
    if broad == "equity":
        factors_out = [metric["factor"] for metric in series["metrics"]]
        if factors_out != [f"latent_factor_{index}" for index in range(1, len(factors_out) + 1)]:
            raise ArtifactValidationError("equity latent factor contract mismatch")
        return tuple(_factor_definition("equity.investment", factor, 1 / len(factors_out)) for factor in factors_out)
    if broad == "alternatives":
        subtype = raw.get("alternative_subtype")
        expected: tuple[str, ...] | None = {
            "managed_futures": ("trend", "carry", "fx"),
            "macro": ("rates", "fx"),
            "event_driven": ("equity_beta", "credit_spread"),
            "private_credit": ("credit_spread", "rates"),
        }.get(subtype if isinstance(subtype, str) else "")
        if expected is None:
            return (_factor_definition("alternatives.investment", "insufficient", 1.0) | {"factor": None},)
        if [metric["factor"] for metric in series["metrics"]] != list(expected):
            raise ArtifactValidationError("alternative subtype metric contract mismatch")
        return tuple(_factor_definition("alternatives.investment", factor, 1 / len(expected)) for factor in expected)
    if broad != "multi_asset":
        raise ArtifactValidationError("unsupported investment class")
    out = []
    raw_sleeves = {item["sleeve_id"]: item for item in raw["sleeves"]}
    for sleeve in series["sleeves"]:
        source = raw_sleeves.get(sleeve["sleeve_id"])
        if source is None:
            raise ArtifactValidationError("multi sleeve source mismatch")
        analytic = source["analytics"]
        sleeve_broad = analytic["broad_class"]
        sleeve_weight = sleeve["portfolio_coverage_pct"] / 100
        if sleeve_broad == "cash_mmf":
            out.append(_factor_definition(f"multi_asset.investment.{sleeve['sleeve_id']}", "not_applicable", sleeve_weight, sleeve_id=sleeve["sleeve_id"]) | {"factor": None})
            continue
        sleeve_definitions = _trusted_investment_definitions(analytic, sleeve, "alternatives" if sleeve_broad == "alternatives" else sleeve_broad)
        numeric = [item for item in sleeve_definitions if item["factor"] is not None]
        if not numeric:
            out.append(_factor_definition(f"multi_asset.investment.{sleeve['sleeve_id']}", "insufficient", sleeve_weight, sleeve_id=sleeve["sleeve_id"]) | {"factor": None})
            continue
        for item in numeric:
            out.append(_factor_definition(f"multi_asset.investment.{sleeve['sleeve_id']}", item["factor"], sleeve_weight * item["weight"], sleeve_id=sleeve["sleeve_id"]))
    return tuple(out)


def _cohort(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        row["classification_label"],
        row["currency"],
        row["benchmark_family"],
        *(row["dimensions"].get(x) for x in _DIMENSIONS),
    )


def _status(n: int) -> str:
    return "insufficient" if n < 10 else "degraded" if n < 30 else "certified"


def _rank(rows: list[tuple[str, float]], direction: str) -> dict[str, float]:
    order = sorted(
        rows, key=lambda x: ((-x[1] if direction == "higher" else x[1]), x[0])
    )
    result = {}
    i = 0
    while i < len(order):
        j = i + 1
        while j < len(order) and order[j][1] == order[i][1]:
            j += 1
        percentile = (
            100.0 * (len(order) - (i + 1 + j) / 2) / (len(order) - 1)
            if len(order) > 1
            else 100.0
        )
        for sid, _ in order[i:j]:
            result[sid] = percentile
        i = j
    return result


def _authenticated(
    phase4_manifest: dict[str, Any],
    w2a_bundle: Path | str,
    factor_input: dict[str, Any],
    w2b1_result: dict[str, Any],
    factor_runner_sha: str,
) -> dict[str, Any]:
    _git(factor_runner_sha, "factor runner SHA")
    return factors.validate_authenticated_artifact(
        w2b1_result,
        phase4_manifest=phase4_manifest,
        w2a_bundle=w2a_bundle,
        factor_input=factor_input,
        runner_sha=factor_runner_sha,
    )


def _metric_for_component(
    factor_series: Mapping[str, Any], definition: Mapping[str, Any], component: Mapping[str, Any]
) -> Mapping[str, Any] | None:
    """Resolve the only W2B1 metric a governed investment envelope may claim."""
    broad = factor_series["broad_class"]
    if broad == "cash_mmf":
        if (
            component["state"] != "not_applicable" or component["value"] is not None
            or component["coverage_pct"] != 0 or component["factor_path"] is not None
        ):
            raise ArtifactValidationError("investment factor lineage mismatch")
        return None
    if broad == "alternatives" and factor_series["quality_status"] == "insufficient":
        if (
            component["state"] != "unavailable" or component["value"] is not None
            or component["coverage_pct"] != 0
        ):
            raise ArtifactValidationError("investment factor lineage mismatch")
        return None
    if broad == "multi_asset":
        path = component["factor_path"]
        if not isinstance(path, dict) or set(path) != {"sleeve_id", "factor"}:
            raise ArtifactValidationError("investment factor lineage mismatch")
        sleeve = next(
            (row for row in factor_series["sleeves"] if row["sleeve_id"] == path["sleeve_id"]),
            None,
        )
        if sleeve is None or sleeve["method"] not in {"ols_hac", "latent_factor_model", "instrumented_pca"}:
            raise ArtifactValidationError("investment factor lineage mismatch")
        metrics = {metric["factor"]: metric for metric in sleeve["metrics"]}
        return metrics.get(definition["factor"])
    return {metric["factor"]: metric for metric in factor_series["metrics"]}.get(definition["factor"])


def _resolve_investment(
    factor_series: Mapping[str, Any], broad: str, row: Mapping[str, Any], definitions: tuple[dict[str, Any], ...], evidence: Mapping[str, Any]
) -> None:
    expected_broad = "cash_mmf" if broad == "cash" else broad
    if factor_series["broad_class"] != expected_broad:
        raise ArtifactValidationError("W2B1 broad class mismatch")
    if broad == "cash":
        if len(row["components"]["investment"]) != len(definitions):
            raise ArtifactValidationError("investment component registry mismatch")
        for component in row["components"]["investment"]:
            if (
                component["state"] != "not_applicable" or component["value"] is not None
                or component["coverage_pct"] != 0 or component["factor_path"] is not None
            ):
                raise ArtifactValidationError("investment factor lineage mismatch")
        return
    submitted = row["components"]["investment"]
    if len(submitted) != len(definitions) or [item.get("component_id") for item in submitted if isinstance(item, dict)] != [item["id"] for item in definitions]:
        raise ArtifactValidationError("investment component registry mismatch")
    row["components"]["investment"] = [_component(component, definition, evidence) for definition, component in zip(definitions, submitted)]
    for definition, component in zip(definitions, row["components"]["investment"]):
        if definition["factor"] is None:
            if definition["source_kind"] != "factor":
                continue
            state = "not_applicable" if "not_applicable" in definition["id"] else "unavailable"
            if component["state"] != state or component["value"] is not None or component["coverage_pct"] != 0:
                raise ArtifactValidationError("investment factor lineage mismatch")
            continue
        if definition["factor"] is None:
            continue
        metric = _metric_for_component(factor_series, definition, component)
        if metric is None:
            if (
                component["state"] != "unavailable" or component["value"] is not None
                or component["coverage_pct"] != 0
            ):
                raise ArtifactValidationError("investment factor lineage mismatch")
            continue
        metric_is_eligible = metric["quality_status"] in {"certified", "degraded"}
        if not metric_is_eligible:
            if component["state"] != "unavailable" or component["value"] is not None:
                raise ArtifactValidationError("investment factor lineage mismatch")
            continue
        if (
            component["state"] != "available"
            or component["value"] != metric["value"]
            or component["coverage_pct"] != metric["coverage_pct"]
            or component["methodology_id"] != metric["methodology_id"]
            or component["methodology_version"] != metric["methodology_version"]
            or component["source_refs"] != metric["source_refs"]
            or component["quality"] != metric["quality_status"]
        ):
            raise ArtifactValidationError("investment factor lineage mismatch")


def build_authenticated_result(
    scoring_input: dict[str, Any],
    policy_matrix: dict[str, Any],
    *,
    evidence_manifest: dict[str, Any] | None = None,
    phase4_manifest: dict[str, Any],
    w2a_bundle: Path | str,
    factor_input: dict[str, Any],
    w2b1_result: dict[str, Any],
    factor_runner_sha: str,
    runner_sha: str,
) -> dict[str, Any]:
    """Build only after independently authenticating W2B1's full lineage."""
    _git(runner_sha, "W2B2 runner SHA")
    if evidence_manifest is None:
        raise ArtifactValidationError("evidence manifest is required")
    evidence = validate_evidence_manifest(evidence_manifest)
    matrix = validate_policy_matrix(policy_matrix)
    rows = _input(scoring_input, matrix, policy_matrix, evidence)
    w2b1 = _authenticated(
        phase4_manifest, w2a_bundle, factor_input, w2b1_result, factor_runner_sha
    )
    known = {x["series_id"]: x for x in w2b1["series"]}
    reps: dict[tuple[tuple[Any, ...], str], dict[str, Any]] = {}
    for row in rows:
        if row["series_id"] not in known:
            raise ArtifactValidationError("investment factor lineage mismatch")
        factor_series = known[row["series_id"]]
        raw = next((item for item in factor_input["series"] if item["series_id"] == row["series_id"]), None)
        if (
            raw is None or raw["classification_label"] != row["classification_label"]
            or raw["policy_version"] != f"{row['policy_id']}/{row['policy_version']}"
            or factor_series["classification_label"] != row["classification_label"]
        ):
            raise ArtifactValidationError("trusted factor classification/policy mismatch")
        if raw.get("benchmark_family") is not None and (
            raw["benchmark_family"] != row["benchmark_family"]
            or raw.get("benchmark_method") not in {"relative", "absolute"}
        ):
            raise ArtifactValidationError("trusted factor benchmark mismatch")
        definitions = _trusted_investment_definitions(raw, factor_series, matrix[row["classification_label"]][0])
        _resolve_investment(factor_series, matrix[row["classification_label"]][0], row, definitions, evidence)
        row["_investment_definitions"] = definitions
        key = (_cohort(row), row["series_id"])
        old = reps.get(key)
        if old is not None and old["components"] != row["components"]:
            raise ArtifactValidationError(
                "duplicate series has inconsistent economic evidence"
            )
        reps.setdefault(key, row)
    ranks: dict[tuple[Any, ...], dict[str, dict[str, Any]]] = {}
    for cohort in {_cohort(x) for x in rows}:
        representatives = {sid: row for (c, sid), row in reps.items() if c == cohort}
        ranks[cohort] = {}
        for pillar in PILLARS:
            ranks[cohort][pillar] = {}
            first = representatives[next(iter(representatives))]
            definitions = first["_investment_definitions"] if pillar == "investment" else COMPONENT_POLICY[matrix[first["classification_label"]][0]][pillar]
            if pillar == "investment" and any(row["_investment_definitions"] != definitions for row in representatives.values()):
                raise ArtifactValidationError("cohort investment component signature mismatch")
            for index, definition in enumerate(definitions):
                components = [
                    row["components"][pillar][index] for row in representatives.values()
                ]
                directions = {x["direction"] for x in components}
                if directions != {definition["direction"]}:
                    raise ArtifactValidationError("inconsistent component direction")
                available = [
                    (sid, row["components"][pillar][index]["value"])
                    for sid, row in representatives.items()
                    if row["components"][pillar][index]["state"] == "available"
                    and row["components"][pillar][index]["quality"]
                    in {"certified", "degraded"}
                    and not row["components"][pillar][index]["diagnostic_only"]
                ]
                ranks[cohort][pillar][definition["id"]] = {
                    "n_total": len(representatives),
                    "n_valid": len(available),
                    "status": _status(len(available)),
                    "rank": _rank(available, definition["direction"])
                    if len(available) >= 10
                    else {},
                }
    scores = []
    for row in rows:
        broad = matrix[row["classification_label"]][0]
        cohort = _cohort(row)
        pillar_out = {}
        for pillar in PILLARS:
            definitions = row["_investment_definitions"] if pillar == "investment" else COMPONENT_POLICY[broad][pillar]
            entries = []
            effective = 0.0
            weighted = 0.0
            statuses = []
            for definition, component in zip(definitions, row["components"][pillar]):
                meta = ranks[cohort][pillar][definition["id"]]
                usable = (
                    component["state"] == "available"
                    and component["quality"] in {"certified", "degraded"}
                    and component["coverage_pct"] >= definition["minimum_coverage_pct"]
                    and meta["status"] != "insufficient"
                    and not component["diagnostic_only"]
                )
                percentile = meta["rank"].get(row["series_id"]) if usable else None
                if usable:
                    effective += definition["weight"]
                    weighted += definition["weight"] * percentile
                statuses.append(meta["status"])
                entries.append(
                    {
                        "component_id": definition["id"],
                        "qualified_value": component["value"],
                        "direction": component["direction"],
                        "methodology_id": component["methodology_id"],
                        "methodology_version": component["methodology_version"],
                        "source_kind": component["source_kind"],
                        "source_refs": component["source_refs"],
                        "diagnostic_flag": component["diagnostic_only"],
                        "factor_path": component["factor_path"],
                        "original_weight": definition["weight"],
                        "effective_weight": None,
                        "percentile": percentile,
                        "n_total": meta["n_total"],
                        "n_valid": meta["n_valid"],
                        "coverage_pct": component["coverage_pct"],
                        "quality": component["quality"],
                        "state": component["state"],
                        "status": meta["status"],
                    }
                )
            applicable = any(
                x["state"] != "not_applicable" for x in row["components"][pillar]
            )
            sufficient = effective >= definitions[0]["minimum_effective_weight"]
            score = weighted / effective if sufficient else None
            dq_health = None
            if pillar == "data_quality" and applicable:
                dq_health = sum(
                    entry["original_weight"]
                    * entry["coverage_pct"] / 100
                    * (QUALITY_PENALTY[entry["quality"]]
                       if entry["state"] == "available" and not entry["diagnostic_flag"]
                       else QUALITY_PENALTY["unavailable"])
                    for entry in entries
                )
                # DQ is a health measure: stale/opaque/unavailable evidence is
                # never neutral and opaque evidence contributes exactly zero.
                score = (score * dq_health) if sufficient else (100.0 * dq_health)
            for entry in entries:
                entry["effective_weight"] = (
                    entry["original_weight"] / effective
                    if sufficient and entry["percentile"] is not None
                    else 0.0
                )
            status = (
                "not_applicable"
                if not applicable
                else (
                    "insufficient"
                    if not sufficient
                    else (
                        "degraded"
                        if "degraded" in statuses
                        or any(x["quality"] == "degraded" for x in entries)
                        else "certified"
                        if all(x == "certified" for x in statuses)
                        else "degraded"
                    )
                )
            )
            pillar_out[pillar] = {
                "status": status,
                "score": score,
                "percentile": score,
                "n_total": entries[0]["n_total"]
                if "n_total" in entries[0]
                else ranks[cohort][pillar][definitions[0]["id"]]["n_total"],
                "n_valid": {x["component_id"]: x["n_valid"] for x in entries},
                "coverage_pct": sum(
                    x["original_weight"] * x["coverage_pct"] for x in entries
                ),
                "components": entries,
                "effective_weight": effective,
                "quality_health": dq_health,
                "uncertainty": "individual_or_cohort_degraded"
                if status == "degraded"
                else None,
            }
        scores.append(
            {
                "instrument_id": row["instrument_id"],
                "series_id": row["series_id"],
                "representative_instrument_id": reps[(cohort, row["series_id"])][
                    "instrument_id"
                ],
                "classification_label": row["classification_label"],
                "broad_class": broad,
                "cohort": {
                    "strategy": row["classification_label"],
                    "currency": row["currency"],
                    "benchmark_family": row["benchmark_family"],
                    "dimensions": row["dimensions"],
                },
                "pillars": pillar_out,
                "diagnostics": row["diagnostics"],
                "summary_score": None,
            }
        )
    binding = {
        "phase4_manifest_hash": w2a.canonical_sha256(phase4_manifest),
        "w2a_run_hash": factors._w2a(phase4_manifest, Path(w2a_bundle))["content_hash"],
        "factor_input_hash": factors.canonical_sha256(factor_input),
        "w2b1_run_hash": w2b1["content_hash"],
        "factor_runner_sha": factor_runner_sha,
        "evidence_manifest_hash": canonical_sha256(evidence_manifest),
        "scoring_input_hash": canonical_sha256(scoring_input),
        "matrix_hash": canonical_sha256(policy_matrix),
        "policy_hash": canonical_sha256(
            {
                "id": policy_matrix["policy_id"],
                "version": policy_matrix["policy_version"],
            }
        ),
        "methodology_hash": canonical_sha256(scoring_input["methodology"]),
        "runner_sha": runner_sha,
    }
    result = {
        "schema_version": RUN_SCHEMA,
        "state": "complete",
        "binding": binding,
        "scores": scores,
    }
    result["content_hash"] = canonical_sha256(result)
    return result


def validate_authenticated_result(
    result: dict[str, Any],
    scoring_input: dict[str, Any],
    policy_matrix: dict[str, Any],
    *,
    evidence_manifest: dict[str, Any] | None = None,
    phase4_manifest: dict[str, Any],
    w2a_bundle: Path | str,
    factor_input: dict[str, Any],
    w2b1_result: dict[str, Any],
    factor_runner_sha: str,
    runner_sha: str,
) -> dict[str, Any]:
    expected = build_authenticated_result(
        scoring_input,
        policy_matrix,
        evidence_manifest=evidence_manifest,
        phase4_manifest=phase4_manifest,
        w2a_bundle=w2a_bundle,
        factor_input=factor_input,
        w2b1_result=w2b1_result,
        factor_runner_sha=factor_runner_sha,
        runner_sha=runner_sha,
    )
    if result != expected:
        raise ArtifactValidationError("W2B2 artifact does not match governed inputs")
    return result


def _no_reparse(path: Path) -> None:
    try:
        item = os.lstat(path)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(item.st_mode) or bool(
        getattr(item, "st_file_attributes", 0) & 0x400
    ):
        raise ArtifactValidationError("symlink/reparse output refused")


def _external(path: Path) -> Path:
    probe = path
    while probe != probe.parent:
        _no_reparse(probe)
        probe = probe.parent
    resolved = path.resolve()
    for root in (
        Path(__file__).resolve().parents[2],
        Path("E:/Edgard/nport"),
        Path("E:/Edgard/ncen"),
        Path("E:/Edgard/RR1"),
        Path("E:/Edgard/13-F"),
    ):
        try:
            resolved.relative_to(root.resolve())
            raise ArtifactValidationError("output must be external to Git/source roots")
        except ValueError:
            pass
    return resolved


def _read(path: Path) -> dict[str, Any]:
    _no_reparse(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ArtifactValidationError("JSON artifact unreadable") from error
    if not isinstance(value, dict):
        raise ArtifactValidationError("JSON artifact must be object")
    return value


def _fsync_directory(path: Path) -> None:
    """Durably persist directory entries where the platform supports it."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_canonical(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(canonical_json(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        if temporary.exists():
            temporary.unlink()
        raise


def run_artifact(
    scoring_input: dict[str, Any],
    policy_matrix: dict[str, Any],
    output_dir: Path | str,
    *,
    evidence_manifest: dict[str, Any] | None = None,
    phase4_manifest: dict[str, Any],
    w2a_bundle: Path | str,
    factor_input: dict[str, Any],
    w2b1_result: dict[str, Any],
    factor_runner_sha: str,
    runner_sha: str,
) -> dict[str, Any]:
    result = build_authenticated_result(
        scoring_input,
        policy_matrix,
        evidence_manifest=evidence_manifest,
        phase4_manifest=phase4_manifest,
        w2a_bundle=w2a_bundle,
        factor_input=factor_input,
        w2b1_result=w2b1_result,
        factor_runner_sha=factor_runner_sha,
        runner_sha=runner_sha,
    )
    destination = _external(Path(output_dir))
    if destination.exists():
        child = destination / "peer_scoring_run.json"
        _no_reparse(destination)
        _no_reparse(child)
        if (
            not destination.is_dir()
            or {x.name for x in destination.iterdir()} != {"peer_scoring_run.json"}
            or not child.is_file()
            or child.read_bytes() != canonical_json(result)
            or _read(child) != result
        ):
            raise ArtifactValidationError(
                "partial foreign mixed or noncanonical output"
            )
        return validate_authenticated_result(
            _read(child),
            scoring_input,
            policy_matrix,
            evidence_manifest=evidence_manifest,
            phase4_manifest=phase4_manifest,
            w2a_bundle=w2a_bundle,
            factor_input=factor_input,
            w2b1_result=w2b1_result,
            factor_runner_sha=factor_runner_sha,
            runner_sha=runner_sha,
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.stage-", dir=destination.parent)
    )
    try:
        child = staging / "peer_scoring_run.json"
        _write_canonical(child, result)
        _no_reparse(staging)
        _no_reparse(child)
        if (
            {item.name for item in staging.iterdir()} != {"peer_scoring_run.json"}
            or not child.is_file()
            or child.read_bytes() != canonical_json(result)
        ):
            raise ArtifactValidationError("staged output is partial or noncanonical")
        staged = _read(child)
        validate_authenticated_result(
            staged,
            scoring_input,
            policy_matrix,
            evidence_manifest=evidence_manifest,
            phase4_manifest=phase4_manifest,
            w2a_bundle=w2a_bundle,
            factor_input=factor_input,
            w2b1_result=w2b1_result,
            factor_runner_sha=factor_runner_sha,
            runner_sha=runner_sha,
        )
        _fsync_directory(staging)
        os.replace(staging, destination)
        _fsync_directory(destination.parent)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return result


def run_shadow_unconfigured(*, authorization: dict[str, Any], **kwargs: Any) -> None:
    result = build_authenticated_result(**kwargs)
    expected = {
        "stage": "phase6_shadow",
        "command": "shadow-db-write",
        **result["binding"],
        "output_content_hash": result["content_hash"],
        "target": "isolated-shadow",
        "role": "shadow_writer",
    }
    if (
        not isinstance(authorization, dict)
        or set(authorization) != set(expected)
        or authorization != expected
        or any(
            "pointer" in k or "current" in k or "provider" in k for k in authorization
        )
    ):
        raise ArtifactValidationError("shadow authorization binding mismatch")
    raise ArtifactValidationError("shadow_writer_unconfigured")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run governed artifact-only SEC class peer scoring"
    )
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--artifact-only", action="store_true")
    modes.add_argument("--shadow-db-write", action="store_true")
    for name in (
        "phase4-manifest",
        "w2a-bundle",
        "factor-input",
        "w2b1-run",
        "scoring-input",
        "policy-matrix",
        "evidence-manifest",
    ):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--authorization-record", type=Path)
    parser.add_argument("--factor-runner-sha", required=True)
    parser.add_argument("--runner-sha", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        values = {
            "scoring_input": _read(args.scoring_input),
            "policy_matrix": _read(args.policy_matrix),
            "evidence_manifest": _read(args.evidence_manifest),
            "phase4_manifest": _read(args.phase4_manifest),
            "w2a_bundle": args.w2a_bundle,
            "factor_input": _read(args.factor_input),
            "w2b1_result": _read(args.w2b1_run),
            "factor_runner_sha": args.factor_runner_sha,
            "runner_sha": args.runner_sha,
        }
        if args.artifact_only:
            if args.output_dir is None:
                raise ArtifactValidationError(
                    "artifact-only mode requires --output-dir"
                )
            print(
                canonical_json(
                    run_artifact(output_dir=args.output_dir, **values)
                ).decode()
            )
            return 0
        if args.authorization_record is None:
            raise ArtifactValidationError("shadow mode requires --authorization-record")
        run_shadow_unconfigured(
            authorization=_read(args.authorization_record), **values
        )
    except ArtifactValidationError as error:
        print(str(error))
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
