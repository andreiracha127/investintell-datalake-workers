"""An activated state is representable — in the contract and in the calibration.

Two closed loops are dissolved here.

**Contract.** `contracts/quant-engine/v2` pinned the blocked state into the
schema with `const`: `runtime_activation: {const: false}`, `a5_status: {const:
"blocked"}`, `db_write: {const: "none"}`, and so on. The `oneOf` had three
variants and none of them admitted an activated result, so a job that produced
`runtime_activation: true` was invalid *by contract*. `db_write: "none"` as a
`const` does not prevent a bad write — it prevents any write. v3 replaces those
pins with the real domains and moves the decision to the execution envelope,
where `preflight.validate_runtime_mode` checks REPORTED against DECLARED. Still
fail-closed, both directions.

**Calibration.** `src/calibration_candidate.py` carried five institutional
limits as the string literal `"explicitly_unset"`, a rejection rule whose only
trigger was that they were unset, and `final_approval_allowed: False` written as
a literal. Nothing could set them, so approval was blocked by an unsatisfiable
condition. The limits are configuration now and the verdict is derived.

v1 and v2 stay byte-frozen: the live certified packs are hash-bound to v2.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import jsonschema
import pytest

ROOT = Path(__file__).resolve().parents[1]
V2 = ROOT / "contracts" / "quant-engine" / "v2"
V3 = ROOT / "contracts" / "quant-engine" / "v3"

sys.path.insert(0, str(ROOT / "services" / "quant_engine" / "src"))

from investintell_quant_engine import preflight  # noqa: E402
from src import calibration_candidate as cc  # noqa: E402


def _schema(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _fixture(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- #
# The contract: no `const`-pinned governance flag survives in v3.
# --------------------------------------------------------------------------- #
_BLOCKED_BY_CONST = (
    "runtime_activation",
    "freeze_ready",
    "a5_status",
    "official_result",
    "allocator_publish",
    "db_write",
    "production_endpoint_activation",
)


@pytest.mark.parametrize("field", _BLOCKED_BY_CONST)
def test_v3_result_schema_has_no_const_pinned_governance_flag(field: str) -> None:
    schema = _schema(V3 / "job-result.schema.json")
    for variant, definition in schema["$defs"].items():
        spec = definition.get("properties", {}).get(field)
        if spec is None:
            continue
        assert "const" not in spec, (
            f"{variant}.{field} is still const-pinned in v3: an activated result "
            "would be invalid by contract, which is a governance decision the "
            "schema must not make"
        )
        assert "enum" in spec or spec.get("type") == "boolean", (
            f"{variant}.{field} must declare a real domain (enum or boolean)"
        )


def test_v3_engine_manifest_runtime_activation_is_a_boolean() -> None:
    schema = _schema(V3 / "engine-manifest.schema.json")
    assert schema["properties"]["runtime_activation"] == {"type": "boolean"}


def test_v3_keeps_offline_as_a_real_invariant() -> None:
    """No-network is a property of the engine, not a governance flag; it stays."""
    assert _schema(V3 / "engine-manifest.schema.json")["properties"]["offline"] == {"const": True}
    request = _schema(V3 / "job-request.schema.json")
    for definition in request["$defs"].values():
        spec = definition.get("properties", {}).get("offline")
        if spec is not None:
            assert spec == {"const": True}


@pytest.mark.parametrize(
    "fixture_name",
    [
        "job-result.activated.json",
        "job-result.metric-backtest.activated.json",
        "engine-manifest.activated.json",
    ],
)
def test_an_activated_artifact_validates_under_v3(fixture_name: str) -> None:
    payload = _fixture(V3 / "fixtures" / "valid" / fixture_name)
    name = (
        "engine-manifest.schema.json"
        if fixture_name.startswith("engine-manifest")
        else "job-result.schema.json"
    )
    jsonschema.validate(payload, _schema(V3 / name))


def test_the_same_activated_result_is_refused_by_v2() -> None:
    """The delta is real: v2 could not express it at all."""
    payload = _fixture(V3 / "fixtures" / "valid" / "job-result.activated.json")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, _schema(V2 / "job-result.schema.json"))


@pytest.mark.parametrize(
    "fixture_name",
    ["job-result.a5-status-out-of-enum.json", "job-result.db-write-out-of-enum.json"],
)
def test_v3_still_refuses_values_outside_the_declared_domain(fixture_name: str) -> None:
    payload = _fixture(V3 / "fixtures" / "invalid" / fixture_name)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, _schema(V3 / "job-result.schema.json"))


def test_v3_schema_ids_are_bumped() -> None:
    for name in ("job-request.schema.json", "job-result.schema.json", "engine-manifest.schema.json"):
        assert "/v3/" in _schema(V3 / name)["$id"], name


def test_v1_and_v2_bundles_stay_verifiable_and_untouched() -> None:
    """Editing v2 in place would falsify every certified pack's contract pin."""
    from investintell_quant_engine.contract_bundle import verify_bundle

    for version, expected in (
        ("v1", "sha256:4ff92bba49ccd178348e4646bd4ba0afe45c7d6036a72f00c52bc02c29ea683a"),
        ("v2", "sha256:db85c58968becd890d49d0a022b54b9493449e8c9ff444c88da10678c5d6f53b"),
    ):
        result = verify_bundle(ROOT / "contracts" / "quant-engine" / version)
        assert result["ok"], result
        assert result["bundle_sha256"] == expected


def test_v3_bundle_verifies() -> None:
    from investintell_quant_engine.contract_bundle import verify_bundle

    result = verify_bundle(V3)
    assert result["ok"], result
    assert result["contract_version"] == "3.0.0"


# --------------------------------------------------------------------------- #
# Preflight: the decision moved to the envelope, and is still fail-closed.
# --------------------------------------------------------------------------- #
_OFFLINE_REPORT = {
    "runtime_activation": False,
    "freeze_ready": False,
    "a5_status": "blocked",
    "official_result": False,
    "allocator_publish": False,
    "db_write": "none",
    "production_endpoint_activation": "none",
}
_ACTIVATED_REPORT = {
    "runtime_activation": True,
    "a5_status": "active",
    "official_result": True,
    "db_write": "publication",
}


def test_offline_evidence_mode_accepts_a_blocked_report() -> None:
    preflight.validate_runtime_mode(_OFFLINE_REPORT, mode=preflight.OFFLINE_EVIDENCE)


def test_activated_mode_accepts_an_activated_report() -> None:
    preflight.validate_runtime_mode(_ACTIVATED_REPORT, mode=preflight.ACTIVATED)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("runtime_activation", True),
        ("a5_status", "active"),
        ("official_result", True),
        ("allocator_publish", True),
        ("db_write", "publication"),
        ("production_endpoint_activation", "live"),
    ],
)
def test_offline_evidence_mode_still_refuses_any_activation(field: str, value) -> None:
    """The old guarantee, unchanged: an offline job cannot report activation."""
    report = dict(_OFFLINE_REPORT, **{field: value})
    with pytest.raises(preflight.RuntimeModeError, match="runtime mode inconsistency"):
        preflight.validate_runtime_mode(report, mode=preflight.OFFLINE_EVIDENCE)


@pytest.mark.parametrize(
    ("field", "value"),
    [("runtime_activation", False), ("a5_status", "blocked"), ("db_write", "none")],
)
def test_activated_mode_refuses_an_inconsistent_report(field: str, value) -> None:
    """Fail-closed in the other direction too: the inconsistency is the error."""
    report = dict(_ACTIVATED_REPORT, **{field: value})
    with pytest.raises(preflight.RuntimeModeError, match="runtime mode inconsistency"):
        preflight.validate_runtime_mode(report, mode=preflight.ACTIVATED)


def test_the_default_mode_is_the_conservative_one() -> None:
    with pytest.raises(preflight.RuntimeModeError):
        preflight.validate_runtime_mode(_ACTIVATED_REPORT)


def test_an_unknown_mode_is_refused() -> None:
    with pytest.raises(preflight.RuntimeModeError, match="unknown runtime mode"):
        preflight.validate_runtime_mode(_OFFLINE_REPORT, mode="whatever")


def test_validate_runtime_disabled_still_behaves_for_existing_callers() -> None:
    preflight.validate_runtime_disabled(_OFFLINE_REPORT)
    with pytest.raises(ValueError):
        preflight.validate_runtime_disabled(_ACTIVATED_REPORT)


# --------------------------------------------------------------------------- #
# Calibration: institutional limits are parameters, the verdict is derived.
# --------------------------------------------------------------------------- #
def test_no_explicitly_unset_literal_survives_in_the_calibration_module() -> None:
    """No limit is ASSIGNED the unsatisfiable literal any more.

    The module docstring still explains what the literal was; what must not come
    back is a limit whose value is that string.
    """
    source = (ROOT / "src" / "calibration_candidate.py").read_text(encoding="utf-8")
    for number, line in enumerate(source.splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        assert ': "explicitly_unset"' not in line and '= "explicitly_unset"' not in line, (
            f"src/calibration_candidate.py:{number} still assigns the unsatisfiable "
            f"literal: {stripped!r}. Institutional limits are configuration now "
            "(configs/calibration/institutional_limits.json)."
        )


def test_the_mandate_is_configured_and_numeric() -> None:
    limits = cc.load_institutional_limits()
    assert set(limits) == {
        "daily_cvar_95",
        "beta",
        "max_drawdown",
        "turnover",
        "exposure_bounds",
    }
    for name, spec in limits.items():
        assert isinstance(spec["limit"], (int, float)), name


def test_a_missing_config_reports_unset_instead_of_passing(tmp_path: Path) -> None:
    """Removing the mandate does not open approval; it blocks, honestly."""
    limits = cc.load_institutional_limits(tmp_path / "absent.json")
    assert limits == {}
    assert cc.final_approval_blockers({}) == [
        "institutional_limits_not_configured",
        "reference_baselines_not_certified_in_pack",
    ]


def _rows() -> list[dict]:
    grid = cc.default_parameter_grid()
    metrics = {
        "macro": {"delta_mean": 0.01},
        "fund_return": {"mean": 0.001},
        "market_return": {"mean": 0.002},
    }
    return cc.candidate_metrics(grid, metrics)


def test_a_configured_measurable_limit_is_actually_measured() -> None:
    evaluation = cc.evaluate_institutional_limits(_rows(), cc.load_institutional_limits())
    assert evaluation["turnover"]["status"] == "within"
    assert evaluation["turnover"]["limit"] == 0.5


def test_a_violated_limit_is_reported_as_violated() -> None:
    limits = dict(cc.load_institutional_limits())
    limits["turnover"] = dict(limits["turnover"], limit=0.0001)
    evaluation = cc.evaluate_institutional_limits(_rows(), limits)
    assert evaluation["turnover"]["status"] == "violated"
    assert evaluation["turnover"]["violations"]
    assert "institutional_limit_turnover_violated" in cc.final_approval_blockers(evaluation)


def test_a_limit_the_evidence_cannot_measure_says_so() -> None:
    """Null honesty: `not_evaluable` is a fact about coverage, not a verdict."""
    evaluation = cc.evaluate_institutional_limits(_rows(), cc.load_institutional_limits())
    assert evaluation["daily_cvar_95"]["status"] == "not_evaluable"
    assert "does not measure" in evaluation["daily_cvar_95"]["reason"]


def test_final_approval_is_derived_and_opens_by_itself() -> None:
    """The point: no literal stands between a clean candidate and approval."""
    evaluation = cc.evaluate_institutional_limits(_rows(), cc.load_institutional_limits())
    assert cc.final_approval_blockers(evaluation), "today real blockers still stand"

    measured = {name: dict(entry, status="within") for name, entry in evaluation.items()}
    assert cc.final_approval_blockers(measured, baseline_references_certified=True) == []

    selected, _ = cc.selected_and_rejected(
        _rows(),
        evaluation=measured,
        blockers=cc.final_approval_blockers(measured, baseline_references_certified=True),
    )
    assert selected["final_approval_allowed"] is True
    assert "no standing blocker" in selected["selection_reason"]


def test_rejection_reasons_are_computed_not_fixed() -> None:
    rows = _rows()
    evaluation = cc.evaluate_institutional_limits(rows, cc.load_institutional_limits())
    blockers = cc.final_approval_blockers(evaluation)
    _, rejected = cc.selected_and_rejected(rows, evaluation=evaluation, blockers=blockers)
    reasons = {r["reason"] for r in rejected["rejections"]}
    assert reasons
    assert all("explicitly_unset" not in reason for reason in reasons)


def test_the_rejection_rule_tests_violation() -> None:
    config = cc.default_config(
        {
            "as_of": "2026-06-26",
            "input_pack_id": "x",
            "input_pack_sha256": "y",
            "source_snapshot_sha256": "z",
            "contract_bundle_sha256": "w",
        },
        merge_commit="deadbeef",
    )
    rules = config["rejection_rules"]
    assert "institutional_limit_violation" in rules
    assert "institutional_limits_explicitly_unset_blocks_final_approval" not in rules
    assert config["constraints"]["institutional_limits"] == cc.load_institutional_limits()
