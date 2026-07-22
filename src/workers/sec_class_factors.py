"""Offline, DB-free governed SEC class-specific factor evidence runner."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import stat
import tempfile
from typing import Any

import numpy as np

from src.workers import nport_v2_lookthrough as w2a

ArtifactValidationError = w2a.ArtifactValidationError
_INPUT = "sec_class_factor_input/v2"
_RUN = "sec_class_factor_run/v2"
_FI = ("rates", "credit_spread", "breakeven_inflation", "fx", "equity_beta")
_ALT = {
    "managed_futures": ("trend", "carry", "fx"),
    "macro": ("rates", "fx"),
    "event_driven": ("equity_beta", "credit_spread"),
    "private_credit": ("credit_spread", "rates"),
}
_MIN_DOF = 10
_CONDITION_MAX = 1e10
_QUALITY = {"certified", "degraded", "insufficient", "not_applicable"}
_BROAD_CLASSES = {"fixed_income", "equity", "alternatives", "cash_mmf", "multi_asset"}
_LATENT_ENGINES = {"instrumented_pca", "latent_factor_model"}
_METRIC_FLAGS = {
    "stale_or_smoothed_nav_excluded",
    "factor_date_coverage",
    "stability_subwindow_unavailable",
    "stability_unstable",
}
_MODEL_FLAGS = _METRIC_FLAGS | {
    "insufficient_sample",
    "singular_or_condition_failed",
    "rank_failed",
    "numerical_failure",
    "invalid_hac_variance",
}
_METRIC = {
    "factor",
    "value",
    "unit",
    "measurement_type",
    "methodology_id",
    "methodology_version",
    "as_of",
    "source_period_start",
    "source_period_end",
    "computed_at",
    "n_observations",
    "coverage_pct",
    "quality_status",
    "quality_flags",
    "source_refs",
    "confidence_interval",
    "benchmark_id",
    "benchmark_method",
    "dof",
    "critical_value",
    "critical_value_rule",
    "hac_standard_error",
    "t_stat",
    "r_squared",
    "diagnostics",
}


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False
    ).encode()


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ArtifactValidationError(
            "explicit JSON artifact could not be read"
        ) from error
    if not isinstance(value, dict):
        raise ArtifactValidationError("explicit JSON artifact must be object")
    return value


def _hash_ok(value: dict[str, Any]) -> bool:
    copy = dict(value)
    claim = copy.pop("content_hash", None)
    return isinstance(claim, str) and claim == canonical_sha256(copy)


def _sha(value: Any, where: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value.lower())
    ):
        raise ArtifactValidationError(f"{where} must be SHA-256")
    return value


def _git(value: Any) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(char not in "0123456789abcdef" for char in value.lower())
    ):
        raise ArtifactValidationError(
            "runner SHA must be exact 40-character Git identity"
        )
    return value


def _day(value: Any, where: str) -> date:
    if not isinstance(value, str):
        raise ArtifactValidationError(f"{where} must be ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ArtifactValidationError(f"{where} must be ISO date") from error


def _computed(value: Any) -> tuple[str, date]:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ArtifactValidationError("computed_at must be governed UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ArtifactValidationError("computed_at must be governed UTC") from error
    if parsed.tzinfo != timezone.utc:
        raise ArtifactValidationError("computed_at must be governed UTC")
    return value, parsed.date()


def _ordered(values: list[date], where: str) -> None:
    if len(set(values)) != len(values):
        raise ArtifactValidationError(f"{where} dates must be unique")
    if any(b <= a for a, b in zip(values, values[1:])):
        raise ArtifactValidationError(f"{where} dates must be ordered")
    if any((b - a).days > 7 for a, b in zip(values, values[1:])):
        raise ArtifactValidationError(f"{where} date gap exceeds governed maximum")


def _returns(
    value: Any, as_of: date, where: str
) -> list[tuple[date, float, bool, bool]]:
    if not isinstance(value, list) or not value:
        raise ArtifactValidationError(f"{where} returns required")
    out = []
    for row in value:
        if not isinstance(row, dict) or set(row) != {
            "date",
            "value",
            "stale",
            "smoothed",
        }:
            raise ArtifactValidationError(f"{where} return schema mismatch")
        d, x = _day(row["date"], where), row["value"]
        if d > as_of:
            raise ArtifactValidationError(f"{where} future look-ahead")
        if (
            not isinstance(x, (int, float))
            or isinstance(x, bool)
            or not math.isfinite(x)
        ):
            raise ArtifactValidationError(f"{where} return must be finite")
        if type(row["stale"]) is not bool or type(row["smoothed"]) is not bool:
            raise ArtifactValidationError(f"{where} stale/smoothed must be boolean")
        out.append((d, float(x), row["stale"], row["smoothed"]))
    _ordered([row[0] for row in out], where)
    return out


def _factors(
    value: Any, names: tuple[str, ...], as_of: date, where: str
) -> dict[str, dict[date, float]]:
    if not isinstance(value, dict) or tuple(value) != names:
        raise ArtifactValidationError(f"{where} factor names mismatch")
    answer = {}
    for name, rows in value.items():
        if not isinstance(rows, list) or not rows:
            raise ArtifactValidationError(f"{where} factor rows required")
        parsed = []
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"date", "value"}:
                raise ArtifactValidationError(f"{where} factor schema mismatch")
            d, x = _day(row["date"], where), row["value"]
            if d > as_of:
                raise ArtifactValidationError(f"{where} future look-ahead")
            if (
                not isinstance(x, (int, float))
                or isinstance(x, bool)
                or not math.isfinite(x)
            ):
                raise ArtifactValidationError(f"{where} factor must be finite")
            parsed.append((d, float(x)))
        _ordered([row[0] for row in parsed], where)
        answer[name] = dict(parsed)
    return answer


def _hashes(value: Any, where: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"nav", "factors"}:
        raise ArtifactValidationError(f"{where} source hashes schema mismatch")
    return {k: _sha(v, f"{where} {k}") for k, v in value.items()}


def _common(raw: dict[str, Any], expected: set[str], identity: str) -> None:
    if set(raw) != expected:
        raise ArtifactValidationError("class-specific series schema mismatch")
    for key in ("series_id", "classification_label", "broad_class", "policy_version"):
        if not isinstance(raw.get(key), str) or not raw[key]:
            raise ArtifactValidationError(f"series {key} required")
    if raw["policy_version"] != identity:
        raise ArtifactValidationError("series policy binding mismatch")


def _benchmark(raw: dict[str, Any]) -> None:
    for key in ("benchmark_id", "benchmark_family", "benchmark_method"):
        if not isinstance(raw.get(key), str) or not raw[key]:
            raise ArtifactValidationError("benchmark binding required")


def _payload(raw: dict[str, Any], as_of: date, identity: str) -> dict[str, Any]:
    kind = raw.get("broad_class")
    names: tuple[str, ...]
    base = {
        "series_id",
        "classification_label",
        "broad_class",
        "policy_version",
        "benchmark_id",
        "benchmark_family",
        "benchmark_method",
        "nav_returns",
        "factors",
        "source_hashes",
    }
    if kind == "fixed_income":
        names = _FI
        _common(raw, base, identity)
    elif kind == "alternatives":
        subtype = raw.get("alternative_subtype")
        if subtype == "generic":
            _common(
                raw,
                {
                    "series_id",
                    "classification_label",
                    "broad_class",
                    "policy_version",
                    "source_hashes",
                    "alternative_subtype",
                },
                identity,
            )
            _hashes(raw["source_hashes"], "generic alternative")
            return {"generic": True, "raw": raw}
        if subtype not in _ALT:
            raise ArtifactValidationError("insufficient_strategy")
        names = _ALT[subtype]
        _common(raw, base | {"alternative_subtype"}, identity)
    else:
        raise ArtifactValidationError("unsupported analytics class")
    _benchmark(raw)
    return {
        "raw": raw,
        "kind": kind,
        "returns": _returns(raw["nav_returns"], as_of, kind),
        "factors": _factors(raw["factors"], names, as_of, kind),
        "hashes": _hashes(raw["source_hashes"], kind),
    }


def _equity(raw: dict[str, Any], as_of: date, identity: str) -> dict[str, Any]:
    expected = {
        "series_id",
        "classification_label",
        "broad_class",
        "policy_version",
        "benchmark_id",
        "benchmark_family",
        "benchmark_method",
        "nav_returns",
        "latent_factors",
        "latent_model_evidence",
        "source_hashes",
    }
    _common(raw, expected, identity)
    _benchmark(raw)
    hashes = _hashes(raw["source_hashes"], "equity")
    ev = raw["latent_model_evidence"]
    keys = {
        "model_id",
        "model_version",
        "engine",
        "production_fit",
        "converged",
        "oos_r2",
        "sample_start",
        "sample_end",
        "feature_set_hash",
        "model_artifact_hash",
        "stability_status",
    }
    if not isinstance(ev, dict) or set(ev) != keys:
        raise ArtifactValidationError("latent evidence schema mismatch")
    if (
        not isinstance(ev["model_id"], str)
        or not ev["model_id"]
        or not isinstance(ev["model_version"], str)
        or not ev["model_version"]
        or ev["engine"] not in {"instrumented_pca", "latent_factor_model"}
        or ev["production_fit"] is not True
        or ev["converged"] is not True
        or ev["stability_status"] != "stable"
        or not isinstance(ev["oos_r2"], (int, float))
        or isinstance(ev["oos_r2"], bool)
        or not math.isfinite(ev["oos_r2"])
        or ev["oos_r2"] < 0
    ):
        raise ArtifactValidationError("latent evidence invalid")
    _sha(ev["feature_set_hash"], "latent feature hash")
    _sha(ev["model_artifact_hash"], "latent model artifact")
    if ev["model_artifact_hash"] != hashes["factors"]:
        raise ArtifactValidationError("latent model artifact binding mismatch")
    if not isinstance(raw["latent_factors"], dict):
        raise ArtifactValidationError("latent factors required")
    names = tuple(raw["latent_factors"])
    expected_names = tuple(f"latent_factor_{i}" for i in range(1, len(names) + 1))
    if not names or names != expected_names:
        raise ArtifactValidationError("latent factors must be contiguous")
    result = {
        "raw": raw,
        "kind": "equity",
        "returns": _returns(raw["nav_returns"], as_of, "equity"),
        "factors": _factors(raw["latent_factors"], names, as_of, "latent"),
        "hashes": hashes,
        "engine": ev["engine"],
        "evidence": ev,
    }
    aligned = [
        row[0]
        for row in result["returns"]
        if not row[2]
        and not row[3]
        and all(row[0] in mapping for mapping in result["factors"].values())
    ]
    if (
        not aligned
        or _day(ev["sample_start"], "latent sample") != aligned[0]
        or _day(ev["sample_end"], "latent sample") != aligned[-1]
    ):
        raise ArtifactValidationError("latent sample binding mismatch")
    return result


def _t_beta(a: float, b: float, x: float) -> float:
    qab = a + b
    qap = a + 1
    qam = a - 1
    c = 1.0
    d = 1 - qab * x / qap
    if abs(d) < 1e-30:
        d = 1e-30
    d = 1 / d
    h = d
    for m in range(1, 201):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1 + aa * d
        d = max(d, 1e-30)
        c = 1 + aa / c
        c = max(c, 1e-30)
        d = 1 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1 + aa * d
        d = max(d, 1e-30)
        c = 1 + aa / c
        c = max(c, 1e-30)
        d = 1 / d
        delta = d * c
        h *= delta
        if abs(delta - 1) < 3e-14:
            break
    return h


def _ibeta(x: float, a: float, b: float) -> float:
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    front = math.exp(
        math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        + a * math.log(x)
        + b * math.log1p(-x)
    )
    return (
        front * _t_beta(a, b, x) / a
        if x < (a + 1) / (a + b + 2)
        else 1 - front * _t_beta(b, a, 1 - x) / b
    )


def student_t_critical_95(dof: int) -> float:
    if dof < _MIN_DOF:
        raise ArtifactValidationError("residual dof below governed minimum")
    lo, hi = 0.0, 20.0
    for _ in range(100):
        mid = (lo + hi) / 2
        probability = 1 - 0.5 * _ibeta(dof / (dof + mid * mid), dof / 2, 0.5)
        if probability < 0.975:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def _fit(payload: dict[str, Any], computed_at: str) -> dict[str, Any]:
    if payload.get("generic"):
        return {
            "quality_status": "insufficient",
            "quality_flags": ["insufficient_strategy"],
            "coverage_pct": 0.0,
            "metrics": [],
            "intercept": None,
        }
    rows = [row for row in payload["returns"] if not row[2] and not row[3]]
    eligible = len(rows)
    names = tuple(payload["factors"])
    aligned = [
        row
        for row in rows
        if all(row[0] in mapping for mapping in payload["factors"].values())
    ]
    total = len(payload["returns"])
    coverage = 100 * len(aligned) / total if total else 0.0
    flags = []
    if len(rows) != len(payload["returns"]):
        flags.append("stale_or_smoothed_nav_excluded")
    if len(aligned) != eligible:
        flags.append("factor_date_coverage")
    if not payload.get("generic") and len(aligned) < len(names) + 1 + _MIN_DOF:
        return {
            "quality_status": "insufficient",
            "quality_flags": flags + ["insufficient_sample"],
            "coverage_pct": coverage,
            "metrics": [],
            "intercept": None,
        }
    y = np.array([row[1] for row in aligned])
    x = np.array(
        [
            [mapping[row[0]] for mapping in payload["factors"].values()]
            for row in aligned
        ]
    )
    design = np.column_stack((np.ones(len(y)), x))
    n, p = design.shape
    dof = n - p
    condition = float(np.linalg.cond(design))
    if (
        dof < _MIN_DOF
        or not math.isfinite(condition)
        or condition > _CONDITION_MAX
        or np.linalg.matrix_rank(design) != p
    ):
        return {
            "quality_status": "insufficient",
            "quality_flags": flags + ["singular_or_condition_failed"],
            "coverage_pct": coverage,
            "metrics": [],
            "intercept": None,
        }
    coefficients, _, rank, _ = np.linalg.lstsq(design, y, rcond=None)
    if rank != p:
        return {
            "quality_status": "insufficient",
            "quality_flags": flags + ["rank_failed"],
            "coverage_pct": coverage,
            "metrics": [],
            "intercept": None,
        }
    residual = y - design @ coefficients
    score = design * residual[:, None]
    lag = int(math.floor(4 * (n / 100) ** (2 / 9)))
    meat = score.T @ score
    for offset in range(1, lag + 1):
        weight = 1 - offset / (lag + 1)
        meat += weight * (
            score[offset:].T @ score[:-offset] + score[:-offset].T @ score[offset:]
        )
    try:
        # SVD pseudoinverse is aligned with lstsq and avoids normal-equation inversion.
        pseudo_inverse = np.linalg.pinv(design, rcond=1 / _CONDITION_MAX)
        # (X'X)^-1 = X+ (X+)'; this keeps the calculation in the same
        # SVD basis as lstsq rather than explicitly inverting X'X.
        bread = pseudo_inverse @ pseudo_inverse.T
        covariance = (n / dof) * bread @ meat @ bread
    except np.linalg.LinAlgError:
        return {
            "quality_status": "insufficient",
            "quality_flags": flags + ["numerical_failure"],
            "coverage_pct": coverage,
            "metrics": [],
            "intercept": None,
        }
    variances = np.diag(covariance)
    if (
        not np.all(np.isfinite(variances))
        or np.any(variances <= 0)
        or not np.all(np.isfinite(coefficients))
    ):
        return {
            "quality_status": "insufficient",
            "quality_flags": flags + ["invalid_hac_variance"],
            "coverage_pct": coverage,
            "metrics": [],
            "intercept": None,
        }
    midpoint = n // 2
    first_design, second_design = design[:midpoint], design[midpoint:]
    first_y, second_y = y[:midpoint], y[midpoint:]
    stability_status = "stable"
    max_beta_delta: float | None = None
    first_bounds = (aligned[0][0].isoformat(), aligned[midpoint - 1][0].isoformat())
    second_bounds = (aligned[midpoint][0].isoformat(), aligned[-1][0].isoformat())
    try:
        first_condition = float(np.linalg.cond(first_design))
        second_condition = float(np.linalg.cond(second_design))
        if (
            len(first_y) - p < _MIN_DOF
            or len(second_y) - p < _MIN_DOF
            or np.linalg.matrix_rank(first_design) != p
            or np.linalg.matrix_rank(second_design) != p
            or not math.isfinite(first_condition)
            or not math.isfinite(second_condition)
            or first_condition > _CONDITION_MAX
            or second_condition > _CONDITION_MAX
        ):
            stability_status = "unavailable"
            flags.append("stability_subwindow_unavailable")
        else:
            c1 = np.linalg.lstsq(first_design, first_y, rcond=None)[0]
            c2 = np.linalg.lstsq(second_design, second_y, rcond=None)[0]
            max_beta_delta = float(np.max(np.abs(c1[1:] - c2[1:])))
            if max_beta_delta > 0.75:
                stability_status = "unstable"
                flags.append("stability_unstable")
    except np.linalg.LinAlgError:
        stability_status = "unavailable"
        flags.append("stability_subwindow_unavailable")
    quality = "degraded" if flags else "certified"
    critical = student_t_critical_95(dof)
    r2 = (
        1 - float(np.sum(residual**2) / np.sum((y - y.mean()) ** 2))
        if not np.allclose(y, y[0])
        else 0.0
    )
    refs = sorted(payload["hashes"].values())

    def metric(name: str, value: float, index: int) -> dict[str, Any]:
        se = math.sqrt(float(variances[index]))
        return {
            "factor": name,
            "value": value,
            "unit": "return_sensitivity",
            "measurement_type": "estimated",
            "methodology_id": "sec-class-factor-ols-hac",
            "methodology_version": "v1",
            "as_of": aligned[-1][0].isoformat(),
            "source_period_start": aligned[0][0].isoformat(),
            "source_period_end": aligned[-1][0].isoformat(),
            "computed_at": computed_at,
            "n_observations": n,
            "coverage_pct": coverage,
            "quality_status": quality,
            "quality_flags": list(flags),
            "source_refs": refs,
            "confidence_interval": {
                "level": 0.95,
                "lower": value - critical * se,
                "upper": value + critical * se,
            },
            "benchmark_id": payload["raw"]["benchmark_id"],
            "benchmark_method": payload["raw"]["benchmark_method"],
            "dof": dof,
            "critical_value": critical,
            "critical_value_rule": "student_t_incomplete_beta_bisection_v1",
            "hac_standard_error": se,
            "t_stat": value / se,
            "r_squared": r2,
            "diagnostics": {
                "hac_lag": lag,
                "hac_finite_sample_correction": "n_over_n_minus_p",
                "condition_number": condition,
                "condition_number_max": _CONDITION_MAX,
                "stability_rule": "split_half_min_dof_rank_condition_beta_delta_v1",
                "stability_status": stability_status,
                "stability_first_start": first_bounds[0],
                "stability_first_end": first_bounds[1],
                "stability_second_start": second_bounds[0],
                "stability_second_end": second_bounds[1],
                "stability_max_beta_delta": max_beta_delta,
            },
        }

    return {
        "quality_status": quality,
        "quality_flags": flags,
        "coverage_pct": coverage,
        "metrics": [
            metric(name, float(coefficients[i + 1]), i + 1)
            for i, name in enumerate(names)
        ],
        "intercept": metric("intercept", float(coefficients[0]), 0),
    }


def _series(
    raw: dict[str, Any], as_of: date, computed_at: str, identity: str
) -> dict[str, Any]:
    kind = raw.get("broad_class")
    if kind == "cash_mmf":
        _common(
            raw,
            {
                "series_id",
                "classification_label",
                "broad_class",
                "policy_version",
                "source_hashes",
            },
            identity,
        )
        _hashes(raw["source_hashes"], "cash")
        return {
            "series_id": raw["series_id"],
            "classification_label": raw["classification_label"],
            "broad_class": kind,
            "quality_status": "not_applicable",
            "quality_flags": ["cash_mmf_v1_not_applicable"],
            "method": "not_applicable",
            "coverage_pct": 0.0,
            "metrics": [],
            "intercept": None,
        }
    if kind == "equity":
        payload = _equity(raw, as_of, identity)
        method = payload["engine"]
    elif kind in {"fixed_income", "alternatives"}:
        payload = _payload(raw, as_of, identity)
        method = "ols_hac"
    elif kind == "multi_asset":
        _common(
            raw,
            {
                "series_id",
                "classification_label",
                "broad_class",
                "policy_version",
                "source_hashes",
                "sleeves",
            },
            identity,
        )
        _hashes(raw["source_hashes"], "multi")
        sleeves = raw["sleeves"]
        if not isinstance(sleeves, list) or not sleeves:
            raise ArtifactValidationError("multi sleeves required")
        out = []
        ids = set()
        total = 0.0
        for sleeve in sleeves:
            if (
                not isinstance(sleeve, dict)
                or set(sleeve) != {"sleeve_id", "portfolio_coverage_pct", "analytics"}
                or not isinstance(sleeve["sleeve_id"], str)
                or not sleeve["sleeve_id"]
                or sleeve["sleeve_id"] in ids
                or not isinstance(sleeve["portfolio_coverage_pct"], (int, float))
                or not 0 < sleeve["portfolio_coverage_pct"] <= 100
                or not isinstance(sleeve["analytics"], dict)
            ):
                raise ArtifactValidationError("multi sleeve schema mismatch")
            ids.add(sleeve["sleeve_id"])
            total += float(sleeve["portfolio_coverage_pct"])
            analytic = _series(sleeve["analytics"], as_of, computed_at, identity)
            out.append(
                {
                    "sleeve_id": sleeve["sleeve_id"],
                    "portfolio_coverage_pct": float(sleeve["portfolio_coverage_pct"]),
                    "time_series_coverage_pct": analytic["coverage_pct"],
                    "quality_status": analytic["quality_status"],
                    "quality_flags": analytic["quality_flags"],
                    "method": analytic["method"],
                    "metrics": analytic["metrics"],
                    "intercept": analytic["intercept"],
                }
            )
        if not math.isclose(total, 100.0, abs_tol=1e-9):
            raise ArtifactValidationError("multi sleeve coverage must total 100")
        overall = (
            sum(
                row["portfolio_coverage_pct"] * row["time_series_coverage_pct"]
                for row in out
            )
            / 100
        )
        status = (
            "insufficient"
            if any(row["quality_status"] == "insufficient" for row in out)
            else (
                "degraded"
                if any(row["quality_status"] == "degraded" for row in out)
                else "certified"
            )
        )
        return {
            "series_id": raw["series_id"],
            "classification_label": raw["classification_label"],
            "broad_class": kind,
            "quality_status": status,
            "quality_flags": [],
            "method": "sleeve_specific_ols_hac",
            "coverage_pct": overall,
            "metrics": [],
            "intercept": None,
            "sleeves": out,
        }
    else:
        raise ArtifactValidationError("unsupported broad class")
    result = _fit(payload, computed_at)
    return {
        "series_id": raw["series_id"],
        "classification_label": raw["classification_label"],
        "broad_class": kind,
        "method": method,
        **result,
    }


def _assert_no_reparse(path: Path) -> None:
    try:
        item = os.lstat(path)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(item.st_mode) or bool(
        getattr(item, "st_file_attributes", 0) & 0x400
    ):
        raise ArtifactValidationError("output symlink or reparse point refuses")


def _external(path: Path) -> Path:
    probe = path
    while probe != probe.parent:
        _assert_no_reparse(probe)
        probe = probe.parent
    resolved = path.resolve()
    if resolved.parent == resolved:
        raise ArtifactValidationError("output cannot be filesystem root")
    for root in (
        Path(__file__).resolve().parents[2],
        Path("E:/Edgard/nport"),
        Path("E:/Edgard/ncen"),
        Path("E:/Edgard/RR1"),
        Path("E:/Edgard/13-F"),
    ):
        protected = root.resolve()
        try:
            resolved.relative_to(protected)
        except ValueError:
            try:
                protected.relative_to(resolved)
            except ValueError:
                continue
        raise ArtifactValidationError(
            "output directory must be outside Git and source roots"
        )
    return resolved


def _finite(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _validate_metric(row: Any) -> None:
    if not isinstance(row, dict) or set(row) != _METRIC:
        raise ArtifactValidationError("metric schema or numeric invalid")
    if (
        not all(
            isinstance(row[key], str) and row[key]
            for key in (
                "factor",
                "unit",
                "methodology_id",
                "methodology_version",
                "benchmark_id",
                "benchmark_method",
            )
        )
        or row["measurement_type"] != "estimated"
        or row["unit"] != "return_sensitivity"
        or row["methodology_id"] != "sec-class-factor-ols-hac"
        or row["methodology_version"] != "v1"
        or row["quality_status"] not in _QUALITY
        or not isinstance(row["quality_flags"], list)
        or not all(isinstance(flag, str) and flag for flag in row["quality_flags"])
        or not isinstance(row["source_refs"], list)
        or not _finite(row["coverage_pct"])
        or not 0 <= row["coverage_pct"] <= 100
        or not all(
            _finite(row[key])
            for key in (
                "value",
                "critical_value",
                "hac_standard_error",
                "t_stat",
                "r_squared",
            )
        )
        or row["critical_value"] <= 0
        or row["hac_standard_error"] <= 0
        or not -1e-12 <= row["r_squared"] <= 1 + 1e-12
        or not isinstance(row["n_observations"], int)
        or isinstance(row["n_observations"], bool)
        or not isinstance(row["dof"], int)
        or isinstance(row["dof"], bool)
        or row["n_observations"] <= 0
        or row["dof"] < _MIN_DOF
        or row["critical_value_rule"] != "student_t_incomplete_beta_bisection_v1"
    ):
        raise ArtifactValidationError("metric schema or numeric invalid")
    for source_ref in row["source_refs"]:
        _sha(source_ref, "metric source ref")
    start = _day(row["source_period_start"], "metric source period")
    end = _day(row["source_period_end"], "metric source period")
    as_of = _day(row["as_of"], "metric as_of")
    _computed(row["computed_at"])
    if start > end or end != as_of:
        raise ArtifactValidationError("metric date/count invalid")
    ci = row["confidence_interval"]
    if (
        not isinstance(ci, dict)
        or set(ci) != {"level", "lower", "upper"}
        or not all(_finite(ci[key]) for key in ci)
        or ci["level"] != 0.95
        or ci["lower"] > ci["upper"]
    ):
        raise ArtifactValidationError("metric confidence interval invalid")
    tolerance = 1e-10 * max(1.0, abs(row["value"]), abs(row["hac_standard_error"]))
    if (
        not math.isclose(
            row["t_stat"],
            row["value"] / row["hac_standard_error"],
            rel_tol=0.0,
            abs_tol=tolerance,
        )
        or not math.isclose(
            ci["lower"],
            row["value"] - row["critical_value"] * row["hac_standard_error"],
            rel_tol=0.0,
            abs_tol=tolerance,
        )
        or not math.isclose(
            ci["upper"],
            row["value"] + row["critical_value"] * row["hac_standard_error"],
            rel_tol=0.0,
            abs_tol=tolerance,
        )
    ):
        raise ArtifactValidationError("metric statistical identity invalid")
    if not math.isclose(
        row["critical_value"],
        student_t_critical_95(row["dof"]),
        rel_tol=0.0,
        abs_tol=1e-10,
    ):
        raise ArtifactValidationError("metric critical value invalid")
    diagnostics = row["diagnostics"]
    diagnostic_keys = {
        "hac_lag",
        "hac_finite_sample_correction",
        "condition_number",
        "condition_number_max",
        "stability_rule",
        "stability_status",
        "stability_first_start",
        "stability_first_end",
        "stability_second_start",
        "stability_second_end",
        "stability_max_beta_delta",
    }
    if (
        not isinstance(diagnostics, dict)
        or set(diagnostics) != diagnostic_keys
        or diagnostics["hac_finite_sample_correction"] != "n_over_n_minus_p"
        or diagnostics["stability_rule"]
        != "split_half_min_dof_rank_condition_beta_delta_v1"
        or diagnostics["stability_status"] not in {"stable", "unstable", "unavailable"}
        or not isinstance(diagnostics["hac_lag"], int)
        or isinstance(diagnostics["hac_lag"], bool)
        or not _finite(diagnostics["condition_number"])
        or not _finite(diagnostics["condition_number_max"])
        or diagnostics["condition_number_max"] != _CONDITION_MAX
    ):
        raise ArtifactValidationError("metric diagnostics invalid")
    expected_lag = int(math.floor(4 * (row["n_observations"] / 100) ** (2 / 9)))
    if (
        diagnostics["hac_lag"] != expected_lag
        or diagnostics["condition_number"] <= 0
        or diagnostics["condition_number"] > diagnostics["condition_number_max"]
    ):
        raise ArtifactValidationError("metric diagnostics semantic invalid")
    first_start = _day(diagnostics["stability_first_start"], "stability date")
    first_end = _day(diagnostics["stability_first_end"], "stability date")
    second_start = _day(diagnostics["stability_second_start"], "stability date")
    second_end = _day(diagnostics["stability_second_end"], "stability date")
    if (
        first_start > first_end
        or first_end >= second_start
        or second_start > second_end
    ):
        raise ArtifactValidationError("metric stability window invalid")
    delta = diagnostics["stability_max_beta_delta"]
    if (delta is None) != (diagnostics["stability_status"] == "unavailable") or (
        delta is not None and (not _finite(delta) or delta < 0)
    ):
        raise ArtifactValidationError("metric stability delta invalid")
    if (
        first_start != start
        or second_end != end
        or (
            delta is not None
            and diagnostics["stability_status"] == "stable"
            and delta > 0.75
        )
        or (
            delta is not None
            and diagnostics["stability_status"] == "unstable"
            and delta <= 0.75
        )
    ):
        raise ArtifactValidationError("metric stability semantic invalid")
    flags = row["quality_flags"]
    if (
        len(set(flags)) != len(flags)
        or any(flag not in _METRIC_FLAGS for flag in flags)
        or (
            diagnostics["stability_status"] == "stable"
            and any(flag.startswith("stability_") for flag in flags)
        )
        or (
            diagnostics["stability_status"] == "unstable"
            and "stability_unstable" not in flags
        )
        or (
            diagnostics["stability_status"] == "unavailable"
            and "stability_subwindow_unavailable" not in flags
        )
        or (row["quality_status"] == "certified" and flags)
        or (row["quality_status"] != "certified" and not flags)
        or row["quality_status"] not in {"certified", "degraded"}
    ):
        raise ArtifactValidationError("metric quality semantic invalid")


def _validate_model(
    metrics: list[Any],
    intercept: Any,
    quality_status: Any,
    quality_flags: Any,
    coverage_pct: Any,
    *,
    expected_factors: tuple[str, ...] | None = None,
) -> None:
    if quality_status == "insufficient":
        if (
            metrics
            or intercept is not None
            or not isinstance(quality_flags, list)
            or not quality_flags
            or len(set(quality_flags)) != len(quality_flags)
            or any(flag not in _MODEL_FLAGS for flag in quality_flags)
        ):
            raise ArtifactValidationError("insufficient model cannot publish estimates")
        return
    if (
        quality_status not in {"certified", "degraded"}
        or not metrics
        or intercept is None
    ):
        raise ArtifactValidationError("estimated model publication invalid")
    for row in [*metrics, intercept]:
        _validate_metric(row)
    factors = [row["factor"] for row in metrics]
    if (
        intercept["factor"] != "intercept"
        or "intercept" in factors
        or len(set(factors)) != len(factors)
        or (expected_factors is not None and tuple(factors) != expected_factors)
    ):
        raise ArtifactValidationError("model factor semantics invalid")
    reference = metrics[0]
    shared = (
        "n_observations",
        "dof",
        "coverage_pct",
        "quality_status",
        "quality_flags",
        "source_refs",
        "as_of",
        "source_period_start",
        "source_period_end",
        "computed_at",
        "diagnostics",
    )
    for row in [*metrics, intercept]:
        if (
            any(row[key] != reference[key] for key in shared)
            or row["quality_status"] != quality_status
            or row["quality_flags"] != quality_flags
            or not math.isclose(row["coverage_pct"], coverage_pct, abs_tol=1e-12)
            or row["dof"] != row["n_observations"] - len(metrics) - 1
        ):
            raise ArtifactValidationError("model cross-field contract invalid")


def _validate_generated_result(result: dict[str, Any]) -> None:
    keys = {
        "schema_version",
        "state",
        "computed_at_policy",
        "binding",
        "series",
        "content_hash",
    }
    binding = {
        "phase4_manifest_hash",
        "w2a_run_hash",
        "factor_input_hash",
        "classification_policy_hash",
        "methodology_hash",
        "runner_sha",
    }
    if (
        not isinstance(result, dict)
        or set(result) != keys
        or result.get("schema_version") != _RUN
        or result.get("state") != "complete"
        or result.get("computed_at_policy")
        != "governed_input_timestamp_in_content_identity"
        or not _hash_ok(result)
        or not isinstance(result.get("binding"), dict)
        or set(result["binding"]) != binding
        or any(not isinstance(v, str) or not v for v in result["binding"].values())
        or not isinstance(result.get("series"), list)
        or not result["series"]
    ):
        raise ArtifactValidationError("result top-level schema invalid")
    for binding_name, binding_value in result["binding"].items():
        if binding_name == "runner_sha":
            _git(binding_value)
        else:
            _sha(binding_value, f"result binding {binding_name}")
    series_ids = []
    for series in result["series"]:
        base_keys = {
            "series_id",
            "classification_label",
            "broad_class",
            "quality_status",
            "quality_flags",
            "method",
            "coverage_pct",
            "metrics",
            "intercept",
        }
        series_keys = set(series) if isinstance(series, dict) else set()
        if series_keys != base_keys and series_keys != base_keys | {"sleeves"}:
            raise ArtifactValidationError("result series exact schema invalid")
        if (
            series.get("quality_status") not in _QUALITY
            or not isinstance(series.get("series_id"), str)
            or not series["series_id"]
            or not isinstance(series.get("classification_label"), str)
            or not series["classification_label"]
            or not isinstance(series.get("broad_class"), str)
            or not series["broad_class"]
            or series["broad_class"] not in _BROAD_CLASSES
            or not isinstance(series.get("method"), str)
            or not series["method"]
            or not isinstance(series.get("quality_flags"), list)
            or not all(
                isinstance(flag, str) and flag for flag in series["quality_flags"]
            )
            or not _finite(series.get("coverage_pct"))
            or not 0 <= series["coverage_pct"] <= 100
            or not isinstance(series.get("metrics"), list)
        ):
            raise ArtifactValidationError("result series schema invalid")
        series_ids.append(series["series_id"])
        if (series["broad_class"] == "multi_asset") != ("sleeves" in series):
            raise ArtifactValidationError("result class/sleeve schema mismatch")
        kind = series["broad_class"]
        if kind == "cash_mmf":
            if (
                series["method"] != "not_applicable"
                or series["quality_status"] != "not_applicable"
                or series["quality_flags"] != ["cash_mmf_v1_not_applicable"]
                or series["coverage_pct"] != 0.0
                or series["metrics"]
                or series["intercept"] is not None
            ):
                raise ArtifactValidationError("cash publication contract invalid")
        elif kind == "fixed_income":
            if series["method"] != "ols_hac":
                raise ArtifactValidationError("fixed income method invalid")
            _validate_model(
                series["metrics"],
                series["intercept"],
                series["quality_status"],
                series["quality_flags"],
                series["coverage_pct"],
                expected_factors=_FI
                if series["quality_status"] != "insufficient"
                else None,
            )
        elif kind == "equity":
            if series["method"] not in _LATENT_ENGINES:
                raise ArtifactValidationError("equity method invalid")
            expected = tuple(
                f"latent_factor_{index}"
                for index in range(1, len(series["metrics"]) + 1)
            )
            _validate_model(
                series["metrics"],
                series["intercept"],
                series["quality_status"],
                series["quality_flags"],
                series["coverage_pct"],
                expected_factors=expected
                if series["quality_status"] != "insufficient"
                else None,
            )
        elif kind == "alternatives":
            if series["method"] != "ols_hac":
                raise ArtifactValidationError("alternative method invalid")
            if "insufficient_strategy" in series["quality_flags"]:
                if (
                    series["quality_status"] != "insufficient"
                    or series["quality_flags"] != ["insufficient_strategy"]
                    or series["coverage_pct"] != 0.0
                    or series["metrics"]
                    or series["intercept"] is not None
                ):
                    raise ArtifactValidationError(
                        "generic alternative publication invalid"
                    )
            else:
                _validate_model(
                    series["metrics"],
                    series["intercept"],
                    series["quality_status"],
                    series["quality_flags"],
                    series["coverage_pct"],
                )
                if (
                    series["quality_status"] != "insufficient"
                    and tuple(row["factor"] for row in series["metrics"])
                    not in _ALT.values()
                ):
                    raise ArtifactValidationError("alternative factor family invalid")
        else:
            if (
                series["method"] != "sleeve_specific_ols_hac"
                or series["quality_flags"]
                or series["metrics"]
                or series["intercept"] is not None
            ):
                raise ArtifactValidationError("multi publication contract invalid")
            if not isinstance(series["sleeves"], list):
                raise ArtifactValidationError("result sleeves schema invalid")
            sleeve_ids = set()
            sleeve_weight = 0.0
            sleeve_coverage = 0.0
            for sleeve in series["sleeves"]:
                if (
                    not isinstance(sleeve, dict)
                    or set(sleeve)
                    != {
                        "sleeve_id",
                        "portfolio_coverage_pct",
                        "time_series_coverage_pct",
                        "quality_status",
                        "quality_flags",
                        "method",
                        "metrics",
                        "intercept",
                    }
                    or not _finite(sleeve["portfolio_coverage_pct"])
                    or not _finite(sleeve["time_series_coverage_pct"])
                    or not 0 < sleeve["portfolio_coverage_pct"] <= 100
                    or not 0 <= sleeve["time_series_coverage_pct"] <= 100
                    or sleeve["quality_status"] not in _QUALITY
                    or not isinstance(sleeve["sleeve_id"], str)
                    or not sleeve["sleeve_id"]
                    or not isinstance(sleeve["quality_flags"], list)
                    or not all(
                        isinstance(flag, str) and flag
                        for flag in sleeve["quality_flags"]
                    )
                    or not isinstance(sleeve["method"], str)
                    or not sleeve["method"]
                    or not isinstance(sleeve["metrics"], list)
                ):
                    raise ArtifactValidationError("result sleeve schema invalid")
                if sleeve["sleeve_id"] in sleeve_ids:
                    raise ArtifactValidationError("result sleeve identity duplicated")
                sleeve_ids.add(sleeve["sleeve_id"])
                sleeve_weight += sleeve["portfolio_coverage_pct"]
                sleeve_coverage += (
                    sleeve["portfolio_coverage_pct"]
                    * sleeve["time_series_coverage_pct"]
                    / 100
                )
                if sleeve["method"] == "not_applicable":
                    if (
                        sleeve["quality_status"] != "not_applicable"
                        or sleeve["quality_flags"] != ["cash_mmf_v1_not_applicable"]
                        or sleeve["time_series_coverage_pct"] != 0.0
                        or sleeve["metrics"]
                        or sleeve["intercept"] is not None
                    ):
                        raise ArtifactValidationError("cash sleeve contract invalid")
                elif sleeve["method"] in {"ols_hac", *_LATENT_ENGINES}:
                    if "insufficient_strategy" in sleeve["quality_flags"]:
                        if (
                            sleeve["method"] != "ols_hac"
                            or sleeve["quality_status"] != "insufficient"
                            or sleeve["quality_flags"] != ["insufficient_strategy"]
                            or sleeve["time_series_coverage_pct"] != 0.0
                            or sleeve["metrics"]
                            or sleeve["intercept"] is not None
                        ):
                            raise ArtifactValidationError(
                                "generic alternative sleeve contract invalid"
                            )
                    else:
                        _validate_model(
                            sleeve["metrics"],
                            sleeve["intercept"],
                            sleeve["quality_status"],
                            sleeve["quality_flags"],
                            sleeve["time_series_coverage_pct"],
                        )
                else:
                    raise ArtifactValidationError("sleeve method invalid")
            if (
                not sleeve_ids
                or not math.isclose(sleeve_weight, 100.0, abs_tol=1e-9)
                or not math.isclose(
                    series["coverage_pct"], sleeve_coverage, abs_tol=1e-9
                )
            ):
                raise ArtifactValidationError("result sleeve aggregate invalid")
            expected_status = (
                "insufficient"
                if any(
                    sleeve["quality_status"] == "insufficient"
                    for sleeve in series["sleeves"]
                )
                else (
                    "degraded"
                    if any(
                        sleeve["quality_status"] == "degraded"
                        for sleeve in series["sleeves"]
                    )
                    else "certified"
                )
            )
            if series["quality_status"] != expected_status:
                raise ArtifactValidationError("result multi status invalid")
    if series_ids != sorted(series_ids) or len(set(series_ids)) != len(series_ids):
        raise ArtifactValidationError(
            "result series identities must be ordered and unique"
        )


def _w2a(phase4_manifest: dict[str, Any], bundle: Path) -> dict[str, Any]:
    if not isinstance(phase4_manifest, dict) or set(phase4_manifest) != {
        "schema_version",
        "state",
        "packages",
        "w1_producer_sha",
        "inventory_hash",
        "v2_holdings_source",
    }:
        raise ArtifactValidationError("Phase 4 manifest schema mismatch")
    packages = phase4_manifest.get("packages")
    if not isinstance(packages, list) or any(
        not isinstance(package, dict)
        or set(package) != {"package_id", "form", "state"}
        or not isinstance(package["package_id"], str)
        or not package["package_id"]
        or not isinstance(package["form"], str)
        or not isinstance(package["state"], str)
        for package in packages
    ):
        raise ArtifactValidationError("Phase 4 package schema mismatch")
    phase = w2a.validate_phase4_manifest(phase4_manifest)
    root = bundle.resolve()
    aggregate = _read(root / "aggregate_manifest.json")
    if set(aggregate) != {
        "schema_version",
        "state",
        "phase4_manifest_hash",
        "v2_publication_id",
        "w1_producer_sha",
        "runner_sha",
        "input_artifact_hash",
        "anchors",
        "content_hash",
    } or not _hash_ok(aggregate):
        raise ArtifactValidationError("W2A aggregate invalid")
    if (
        aggregate["phase4_manifest_hash"] != w2a.canonical_sha256(phase4_manifest)
        or aggregate["v2_publication_id"] != phase["v2_publication_id"]
        or aggregate["w1_producer_sha"] != phase["w1_producer_sha"]
    ):
        raise ArtifactValidationError("W2A lineage mismatch")
    anchors = aggregate.get("anchors")
    if not isinstance(anchors, list) or len(anchors) != 8:
        raise ArtifactValidationError("W2A anchors invalid")
    dates = [
        _day(row.get("report_date"), "W2A anchor")
        if isinstance(row, dict)
        else (_ for _ in ()).throw(ArtifactValidationError("W2A anchor schema"))
        for row in anchors
    ]
    try:
        expected = w2a.derive_governed_anchors(dates)
    except ArtifactValidationError as error:
        raise ArtifactValidationError(f"W2A cohort invalid: {error}") from error
    if [(row.get("anchor_id"), row.get("report_date")) for row in anchors] != [
        (row["anchor_id"], row["report_date"]) for row in expected
    ]:
        raise ArtifactValidationError("W2A derived cohort mismatch")
    try:
        w2a._read_prior_complete_anchor_outputs(root, expected)
    except ArtifactValidationError as error:
        raise ArtifactValidationError(
            f"W2A semantic bundle mismatch: {error}"
        ) from error
    return aggregate


def w2a_run_hash(bundle: Path | str) -> str:
    item = _read(Path(bundle) / "aggregate_manifest.json")
    if not _hash_ok(item):
        raise ArtifactValidationError("W2A aggregate hash mismatch")
    return str(item["content_hash"])


def _input(value: dict[str, Any]) -> tuple[dict[str, Any], str, date]:
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "snapshot_id",
            "computed_at",
            "classification_policy",
            "methodology",
            "series",
        }
        or value.get("schema_version") != _INPUT
        or not isinstance(value.get("snapshot_id"), str)
        or not value["snapshot_id"]
    ):
        raise ArtifactValidationError("factor input schema mismatch")
    computed, as_of = _computed(value["computed_at"])
    policy = value["classification_policy"]
    if (
        not isinstance(policy, dict)
        or set(policy) != {"id", "version", "rules_sha256"}
        or not isinstance(policy.get("id"), str)
        or not policy["id"]
        or not isinstance(policy.get("version"), str)
        or not policy["version"]
    ):
        raise ArtifactValidationError("classification policy schema mismatch")
    _sha(policy["rules_sha256"], "classification rules")
    if value["methodology"] != {"id": "sec-class-factor-ols-hac", "version": "v1"}:
        raise ArtifactValidationError("methodology v1 required")
    if not isinstance(value["series"], list) or not value["series"]:
        raise ArtifactValidationError("series required")
    if any(
        not isinstance(series, dict)
        or not isinstance(series.get("series_id"), str)
        or not series["series_id"]
        for series in value["series"]
    ):
        raise ArtifactValidationError("series_id is required")
    series_ids = [series["series_id"] for series in value["series"]]
    if series_ids != sorted(series_ids) or len(set(series_ids)) != len(series_ids):
        raise ArtifactValidationError(
            "series_id must be strictly increasing and unique"
        )
    return value, computed, as_of


def build_artifact_result(
    phase4_manifest: dict[str, Any],
    w2a_bundle: Path | str,
    factor_input: dict[str, Any],
    *,
    runner_sha: str,
) -> dict[str, Any]:
    _git(runner_sha)
    source, computed, as_of = _input(factor_input)
    aggregate = _w2a(phase4_manifest, Path(w2a_bundle))
    identity = f"{source['classification_policy']['id']}/{source['classification_policy']['version']}"
    series = [_series(raw, as_of, computed, identity) for raw in source["series"]]
    if len({row["series_id"] for row in series}) != len(series):
        raise ArtifactValidationError("duplicate series identity")
    binding = {
        "phase4_manifest_hash": w2a.canonical_sha256(phase4_manifest),
        "w2a_run_hash": aggregate["content_hash"],
        "factor_input_hash": canonical_sha256(factor_input),
        "classification_policy_hash": canonical_sha256(source["classification_policy"]),
        "methodology_hash": canonical_sha256(source["methodology"]),
        "runner_sha": runner_sha,
    }
    result = {
        "schema_version": _RUN,
        "state": "complete",
        "computed_at_policy": "governed_input_timestamp_in_content_identity",
        "binding": binding,
        "series": series,
    }
    result["content_hash"] = canonical_sha256(result)
    _validate_generated_result(result)
    return result


def validate_result(
    result: dict[str, Any],
    *,
    phase4_manifest: dict[str, Any] | None = None,
    w2a_bundle: Path | str | None = None,
    factor_input: dict[str, Any] | None = None,
    runner_sha: str | None = None,
) -> None:
    """Authenticate a published artifact by deterministically rebuilding it."""
    if (
        phase4_manifest is None
        or w2a_bundle is None
        or factor_input is None
        or runner_sha is None
    ):
        raise ArtifactValidationError("governed context is required for validation")
    expected = build_artifact_result(
        phase4_manifest,
        w2a_bundle,
        factor_input,
        runner_sha=runner_sha,
    )
    if result != expected:
        raise ArtifactValidationError("artifact does not match governed inputs")


def validate_authenticated_artifact(
    result: dict[str, Any],
    *,
    phase4_manifest: dict[str, Any],
    w2a_bundle: Path | str,
    factor_input: dict[str, Any],
    runner_sha: str,
) -> dict[str, Any]:
    """Consumer-facing authenticated W2B1 artifact validation."""
    validate_result(
        result,
        phase4_manifest=phase4_manifest,
        w2a_bundle=w2a_bundle,
        factor_input=factor_input,
        runner_sha=runner_sha,
    )
    return result


def _write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(canonical_json(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def _verify(path: Path, candidate: dict[str, Any]) -> dict[str, Any]:
    _assert_no_reparse(path)
    child = path / "factor_run.json"
    _assert_no_reparse(child)
    if not path.is_dir() or {item.name for item in path.iterdir()} != {
        "factor_run.json"
    }:
        raise ArtifactValidationError("partial, foreign, or mixed output")
    result = _read(child)
    _validate_generated_result(result)
    if result != candidate:
        raise ArtifactValidationError("output identity mismatch")
    return result


def run_artifact(
    phase4_manifest: dict[str, Any],
    w2a_bundle: Path | str,
    factor_input: dict[str, Any],
    output_dir: Path | str,
    *,
    runner_sha: str,
) -> dict[str, Any]:
    result = build_artifact_result(
        phase4_manifest, w2a_bundle, factor_input, runner_sha=runner_sha
    )
    destination = _external(Path(output_dir))
    if destination.exists():
        return _verify(destination, result)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.stage-", dir=destination.parent)
    )
    try:
        _write(staging / "factor_run.json", result)
        _verify(staging, result)
        os.replace(staging, destination)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return result


def run_shadow_unconfigured(
    phase4_manifest: dict[str, Any],
    w2a_bundle: Path | str,
    factor_input: dict[str, Any],
    authorization: dict[str, Any],
    *,
    runner_sha: str,
) -> None:
    result = build_artifact_result(
        phase4_manifest, w2a_bundle, factor_input, runner_sha=runner_sha
    )
    expected = {
        "stage": "phase6_shadow",
        "command": "shadow-db-write",
        "runner_sha": runner_sha,
        "phase4_manifest_hash": result["binding"]["phase4_manifest_hash"],
        "w2a_run_hash": result["binding"]["w2a_run_hash"],
        "factor_input_hash": result["binding"]["factor_input_hash"],
        "classification_policy_hash": result["binding"]["classification_policy_hash"],
        "methodology_hash": result["binding"]["methodology_hash"],
        "output_content_hash": result["content_hash"],
        "target": "isolated-shadow",
        "role": "shadow_writer",
    }
    if not isinstance(authorization, dict) or set(authorization) != set(expected):
        raise ArtifactValidationError("shadow authorization schema mismatch")
    if any(authorization[key] != value for key, value in expected.items()):
        raise ArtifactValidationError("shadow authorization binding mismatch")
    raise ArtifactValidationError("shadow_writer_unconfigured")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run governed artifact-only SEC class-specific factor evidence"
    )
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--artifact-only", action="store_true")
    modes.add_argument("--shadow-db-write", action="store_true")
    parser.add_argument("--phase4-manifest", type=Path, required=True)
    parser.add_argument("--w2a-bundle", type=Path, required=True)
    parser.add_argument("--factor-input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--authorization-record", type=Path)
    parser.add_argument("--runner-sha", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        phase, payload = _read(args.phase4_manifest), _read(args.factor_input)
        if args.artifact_only:
            if args.output_dir is None:
                raise ArtifactValidationError(
                    "artifact-only mode requires --output-dir"
                )
            print(
                canonical_json(
                    run_artifact(
                        phase,
                        args.w2a_bundle,
                        payload,
                        args.output_dir,
                        runner_sha=args.runner_sha,
                    )
                ).decode()
            )
            return 0
        if args.authorization_record is None:
            raise ArtifactValidationError("shadow mode requires --authorization-record")
        run_shadow_unconfigured(
            phase,
            args.w2a_bundle,
            payload,
            _read(args.authorization_record),
            runner_sha=args.runner_sha,
        )
    except ArtifactValidationError as error:
        print(str(error))
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
