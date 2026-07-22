"""Synthetic-only, DB-free contracts for authenticated W2B2 evidence."""

from __future__ import annotations

import copy
import subprocess
import sys
from pathlib import Path

import pytest

from src.workers import sec_class_factors as factors
from src.workers import sec_class_peers_scoring as peers
from tests.test_sec_class_factors import (
    alternative_series,
    bundle,
    equity_series,
    fi_series,
    input_for as factor_input,
    phase4,
)


def rehash(value: dict) -> dict:
    value["content_hash"] = peers.canonical_sha256(
        {k: v for k, v in value.items() if k != "content_hash"}
    )
    return value


def matrix() -> dict:
    rows = [
        {
            "classification_label": a,
            "broad_class": b,
            "approved_proxy": c,
            "optimizer_sleeve": d,
            "benchmark_family": e,
        }
        for a, b, c, d, e in peers.APPROVED_POLICY_MATRIX
    ]
    return rehash(
        {
            "schema_version": peers.MATRIX_SCHEMA,
            "policy_id": "policy",
            "policy_version": "v1",
            "entries": rows,
        }
    )


def evidence() -> dict:
    entries = []
    for broad in peers.COMPONENT_POLICY.values():
        for definitions in broad.values():
            for definition in definitions:
                if definition["source_kind"] != "factor":
                    entries.append({
                        "source_ref_id": definition["id"],
                        "source_class": definition["source_kind"],
                        "artifact_sha256": "a" * 64,
                        "measure": definition["id"],
                        "methodology_id": definition["methodology_id"],
                        "methodology_version": definition["methodology_version"],
                        "diagnostic_only": False,
                    })
    for diagnostic_id in peers.DIAGNOSTIC_IDS:
        entries.append({
            "source_ref_id": f"diagnostic:{diagnostic_id}",
            "source_class": "event_diagnostic",
            "artifact_sha256": "b" * 64,
            "measure": diagnostic_id,
            "methodology_id": f"sec-class-diagnostic-{diagnostic_id}",
            "methodology_version": "v1",
            "diagnostic_only": True,
        })
    entries.sort(key=lambda entry: entry["source_ref_id"])
    return rehash({"schema_version": peers.EVIDENCE_SCHEMA, "state": "complete", "entries": entries})


@pytest.fixture(autouse=True)
def inject_evidence_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("build_authenticated_result", "validate_authenticated_result"):
        original = getattr(peers, name)
        monkeypatch.setattr(
            peers, name,
            lambda *args, _original=original, **kwargs: _original(
                *args, evidence_manifest=kwargs.pop("evidence_manifest", None) or evidence(), **kwargs
            ),
        )


def component(
    broad: str,
    pillar: str,
    value: float | None,
    component_index: int = 0,
    *,
    state: str = "available",
    coverage: float = 100,
    diagnostic: bool = False,
) -> dict:
    definition = peers.COMPONENT_POLICY[broad][pillar][component_index]
    return {
        "component_id": definition["id"],
        "state": state,
        "value": value,
        "coverage_pct": coverage,
        "quality": "certified",
        "source_kind": definition["source_kind"],
        "source_refs": [definition["id"] if definition["source_kind"] != "factor" else "a" * 64],
        "methodology_id": definition["methodology_id"],
        "methodology_version": definition["methodology_version"],
        "direction": definition["direction"],
        "diagnostic_only": diagnostic,
        "factor_path": ({"factor": definition["factor"]} if definition["factor"] else None),
    }


def diagnostics() -> list[dict]:
    return [
        {
            "diagnostic_id": diagnostic_id,
            "diagnostic_only": True,
            "value": None,
            "methodology_id": f"sec-class-diagnostic-{diagnostic_id}",
            "methodology_version": "v1",
            "source_refs": [f"diagnostic:{diagnostic_id}"],
        }
        for diagnostic_id in peers.DIAGNOSTIC_IDS
    ]


def scoring_input(count: int) -> dict:
    rows = []
    for index in range(count):
        components = {
            pillar: [
                component("fixed_income", pillar, float(index), component_index)
                for component_index in range(
                    len(peers.COMPONENT_POLICY["fixed_income"][pillar])
                )
            ]
            for pillar in peers.PILLARS
        }
        rows.append(
            {
                "instrument_id": f"I{index:02}",
                "series_id": f"S{index:02}",
                "classification_label": "Investment Grade Bond",
                "policy_id": "policy",
                "policy_version": "v1",
                "snapshot_id": "snapshot",
                "currency": "USD",
                "benchmark_family": "GOVT",
                "dimensions": {},
                "components": components,
                "diagnostics": diagnostics(),
            }
        )
    return rehash(
        {
            "schema_version": peers.INPUT_SCHEMA,
            "state": "complete",
            "snapshot": {
                "snapshot_id": "snapshot",
                "policy_id": "policy",
                "policy_version": "v1",
            },
            "methodology": {"id": "peer-v1", "version": "v1"},
            "rows": rows,
        }
    )


def governed(
    tmp_path: Path, count: int = 10
) -> tuple[dict, dict, dict, Path, dict, dict]:
    source = factor_input(
        [
            fi_series(series_id=f"S{index:02}")
            | {"classification_label": "Investment Grade Bond", "benchmark_family": "GOVT"}
            for index in range(count)
        ]
    )
    manifest = phase4()
    root = bundle(tmp_path)
    result = factors.build_artifact_result(manifest, root, source, runner_sha="f" * 40)
    score = scoring_input(count)
    metrics = {
        row["series_id"]: {metric["factor"]: metric for metric in row["metrics"]}
        for row in result["series"]
    }
    for row in score["rows"]:
        for component_index, definition in enumerate(
            peers.COMPONENT_POLICY["fixed_income"]["investment"]
        ):
            if definition["factor"] is None:
                continue
            metric = metrics[row["series_id"]][definition["factor"]]
            row["components"]["investment"][component_index].update(
                value=metric["value"],
                coverage_pct=metric["coverage_pct"],
                quality=metric["quality_status"],
                source_refs=metric["source_refs"],
            )
    return rehash(score), matrix(), manifest, root, source, result


def build(tmp_path: Path, count: int = 10) -> tuple[dict, tuple]:
    source, policy, manifest, root, finput, factor = governed(tmp_path, count)
    result = peers.build_authenticated_result(
        source,
        policy,
        phase4_manifest=manifest,
        w2a_bundle=root,
        factor_input=finput,
        w2b1_result=factor,
        factor_runner_sha="f" * 40,
        runner_sha="e" * 40,
    )
    return result, (source, policy, manifest, root, finput, factor)


def test_public_builder_requires_explicit_authenticated_context() -> None:
    with pytest.raises(TypeError):
        peers.build_authenticated_result(
            scoring_input(10), matrix(), runner_sha="e" * 40
        )


@pytest.mark.parametrize(
    "count,status",
    [(9, "insufficient"), (10, "degraded"), (29, "degraded"), (30, "certified")],
)
def test_thresholds_publish_no_neutral_score(
    tmp_path: Path, count: int, status: str
) -> None:
    result, _ = build(tmp_path, count)
    row = result["scores"][0]["pillars"]["investment"]
    assert row["status"] == status
    assert row["score"] is None if count == 9 else row["score"] is not None


def test_ties_directions_missing_and_series_representative_rules(
    tmp_path: Path,
) -> None:
    source, policy, manifest, root, finput, factor = governed(tmp_path, 11)
    source["rows"][1]["series_id"] = "S00"
    source["rows"][1]["components"] = copy.deepcopy(source["rows"][0]["components"])
    source = rehash(source)
    result = peers.build_authenticated_result(
        source,
        policy,
        phase4_manifest=manifest,
        w2a_bundle=root,
        factor_input=finput,
        w2b1_result=factor,
        factor_runner_sha="f" * 40,
        runner_sha="e" * 40,
    )
    by_id = {x["instrument_id"]: x for x in result["scores"]}
    assert (
        by_id["I00"]["pillars"]["investment"]["percentile"]
        == by_id["I01"]["pillars"]["investment"]["percentile"]
    )
    assert (
        by_id["I10"]["pillars"]["cost_efficiency"]["percentile"]
        < by_id["I00"]["pillars"]["cost_efficiency"]["percentile"]
    )
    assert by_id["I01"]["representative_instrument_id"] == "I00"


def test_duplicate_series_inconsistent_direction_matrix_snapshot_and_dimensions_fail(
    tmp_path: Path,
) -> None:
    source, policy, manifest, root, finput, factor = governed(tmp_path, 10)
    source["rows"][1]["series_id"] = "S00"
    rehash(source)
    with pytest.raises(peers.ArtifactValidationError, match="inconsistent economic"):
        peers.build_authenticated_result(
            source,
            policy,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=finput,
            w2b1_result=factor,
            factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
        )
    source = scoring_input(10)
    source["snapshot"]["policy_id"] = "foreign"
    rehash(source)
    with pytest.raises(peers.ArtifactValidationError, match="matrix policy"):
        peers.build_authenticated_result(
            source,
            policy,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=finput,
            w2b1_result=factor,
            factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
        )
    source = scoring_input(10)
    source["rows"][0]["dimensions"] = {"rating": "AAA"}
    rehash(source)
    with pytest.raises(peers.ArtifactValidationError, match="forbidden"):
        peers.build_authenticated_result(
            source,
            policy,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=finput,
            w2b1_result=factor,
            factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
        )


def test_matrix_closure_and_component_governance(tmp_path: Path) -> None:
    value = matrix()
    value["entries"].pop()
    rehash(value)
    with pytest.raises(peers.ArtifactValidationError):
        peers.validate_policy_matrix(value)
    value = matrix()
    value["entries"].append(copy.deepcopy(value["entries"][0]))
    rehash(value)
    with pytest.raises(peers.ArtifactValidationError):
        peers.validate_policy_matrix(value)
    source, policy, manifest, root, finput, factor = governed(tmp_path, 10)
    source["rows"][0]["components"]["investment"][0]["methodology_id"] = "arbitrary"
    rehash(source)
    # Versioned methodology is allowed in the envelope, but cannot bypass source/component binding.
    source["rows"][0]["components"]["investment"][0]["source_kind"] = (
        "regulatory_numeric"
    )
    rehash(source)
    with pytest.raises(peers.ArtifactValidationError, match="component policy"):
        peers.build_authenticated_result(
            source,
            policy,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=finput,
            w2b1_result=factor,
            factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
        )


def test_data_quality_penalty_diagnostic_events_and_authenticated_tamper(
    tmp_path: Path,
) -> None:
    result, context = build(tmp_path, 10)
    source, policy, manifest, root, finput, factor = context
    changed = copy.deepcopy(source)
    changed["rows"][0]["components"]["data_quality"] = [
        component(
            "fixed_income",
            "data_quality",
            None,
            component_index,
            state="unavailable",
            coverage=0,
        )
        for component_index in range(2)
    ]
    changed["rows"][0]["components"]["operational_quality"] = [
        component(
            "fixed_income", "operational_quality", 1, component_index, diagnostic=True
        )
        for component_index in range(2)
    ]
    rehash(changed)
    value = peers.build_authenticated_result(
        changed,
        policy,
        phase4_manifest=manifest,
        w2a_bundle=root,
        factor_input=finput,
        w2b1_result=factor,
        factor_runner_sha="f" * 40,
        runner_sha="e" * 40,
    )
    assert value["scores"][0]["pillars"]["data_quality"]["score"] == 0
    assert value["scores"][0]["pillars"]["operational_quality"]["score"] is None
    result["scores"][0]["pillars"]["investment"]["score"] = 77
    with pytest.raises(peers.ArtifactValidationError):
        peers.validate_authenticated_result(
            result,
            source,
            policy,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=finput,
            w2b1_result=factor,
            factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
        )


def test_factor_tamper_atomic_noop_and_shadow_refusals(tmp_path: Path) -> None:
    result, context = build(tmp_path, 10)
    source, policy, manifest, root, finput, factor = context
    factor["binding"]["runner_sha"] = "0" * 40
    with pytest.raises(factors.ArtifactValidationError):
        peers.build_authenticated_result(
            source,
            policy,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=finput,
            w2b1_result=factor,
            factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
        )
    source, policy, manifest, root, finput, factor = governed(tmp_path / "good", 10)
    output = tmp_path / "external" / "run"
    first = peers.run_artifact(
        source,
        policy,
        output,
        phase4_manifest=manifest,
        w2a_bundle=root,
        factor_input=finput,
        w2b1_result=factor,
        factor_runner_sha="f" * 40,
        runner_sha="e" * 40,
    )
    assert (
        peers.run_artifact(
            source,
            policy,
            output,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=finput,
            w2b1_result=factor,
            factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
        )
        == first
    )
    (output / "foreign").write_text("x")
    with pytest.raises(peers.ArtifactValidationError):
        peers.run_artifact(
            source,
            policy,
            output,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=finput,
            w2b1_result=factor,
            factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
        )
    auth = {
        "stage": "phase6_shadow",
        "command": "shadow-db-write",
        **first["binding"],
        "output_content_hash": first["content_hash"],
        "target": "isolated-shadow",
        "role": "shadow_writer",
        "pointer": "bad",
    }
    with pytest.raises(peers.ArtifactValidationError):
        peers.run_shadow_unconfigured(
            scoring_input=source,
            policy_matrix=policy,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=finput,
            w2b1_result=factor,
            factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
            authorization=auth,
        )


def test_fresh_import_is_db_free_and_cli_refuses_missing_mode() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import src.workers.sec_class_peers_scoring; assert 'src.db' not in sys.modules; assert 'psycopg' not in sys.modules",
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    cli = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.workers.sec_class_peers_scoring",
            "--artifact-only",
        ],
        capture_output=True,
        text=True,
    )
    assert cli.returncode == 2


def test_registry_is_multi_component_and_class_specific() -> None:
    equity = peers.COMPONENT_POLICY["equity"]["investment"]
    fixed_income = peers.COMPONENT_POLICY["fixed_income"]["investment"]
    assert len(equity) >= 2
    assert sum(item["weight"] for item in equity) == 1
    assert equity != fixed_income


def test_authenticated_input_is_not_mutated_or_reordered(tmp_path: Path) -> None:
    source, policy, manifest, root, finput, factor = governed(tmp_path, 10)
    before = copy.deepcopy(source)
    peers.build_authenticated_result(
        source,
        policy,
        phase4_manifest=manifest,
        w2a_bundle=root,
        factor_input=finput,
        w2b1_result=factor,
        factor_runner_sha="f" * 40,
        runner_sha="e" * 40,
    )
    assert source == before
    source["rows"][0]["components"]["investment"].reverse()
    rehash(source)
    with pytest.raises(peers.ArtifactValidationError, match="registry"):
        peers.build_authenticated_result(
            source,
            policy,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=finput,
            w2b1_result=factor,
            factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
        )


def test_investment_reweights_when_minimum_survives_and_is_insufficient_otherwise(
    tmp_path: Path,
) -> None:
    source, policy, manifest, root, finput, factor = governed(tmp_path, 10)
    for row in source["rows"]:
        row["components"]["investment"][2] = component(
            "fixed_income", "investment", None, 2, state="unavailable", coverage=0
        )
    rehash(source)
    partial = peers.build_authenticated_result(
        source,
        policy,
        phase4_manifest=manifest,
        w2a_bundle=root,
        factor_input=finput,
        w2b1_result=factor,
        factor_runner_sha="f" * 40,
        runner_sha="e" * 40,
    )
    investment = partial["scores"][0]["pillars"]["investment"]
    assert investment["score"] is not None and investment["effective_weight"] == 0.9
    for row in source["rows"]:
        row["components"]["investment"][1] = component(
            "fixed_income", "investment", None, 1, state="unavailable", coverage=0
        )
    rehash(source)
    with pytest.raises(peers.ArtifactValidationError, match="investment factor"):
        peers.build_authenticated_result(source, policy, phase4_manifest=manifest, w2a_bundle=root, factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40, runner_sha="e" * 40)


def _all_class_context(tmp_path: Path, *, generic: bool = False) -> tuple[dict, dict, dict, Path, dict, dict]:
    cash = {
        "series_id": "CASH",
        "classification_label": "Cash Equivalent",
        "broad_class": "cash_mmf",
        "policy_version": "policy/v1",
        "source_hashes": {"nav": "9" * 64, "factors": "a" * 64},
    }
    multi = {
        "series_id": "MULTI",
        "classification_label": "Multi-Asset",
        "broad_class": "multi_asset",
        "policy_version": "policy/v1",
        "source_hashes": {"nav": "b" * 64, "factors": "c" * 64},
        "sleeves": [{"sleeve_id": "fi", "portfolio_coverage_pct": 100.0, "analytics": fi_series(series_id="MULTI-FI")}],
    }
    factor_source = factor_input(sorted([
        equity_series() | {"series_id": "EQUITY", "classification_label": "Large Blend", "benchmark_family": "IVV"},
        fi_series(series_id="FI") | {"classification_label": "Investment Grade Bond", "benchmark_family": "GOVT"},
        alternative_series(generic=generic) | ({"series_id": "ALT", "classification_label": "Alternative"} if generic else {"series_id": "ALT", "classification_label": "Alternative", "benchmark_family": "QAI"}),
        multi,
        cash,
    ], key=lambda row: row["series_id"]))
    manifest, root = phase4(), bundle(tmp_path)
    factor_result = factors.build_artifact_result(manifest, root, factor_source, runner_sha="f" * 40)
    labels = {
        "EQUITY": ("Large Blend", "equity"),
        "FI": ("Investment Grade Bond", "fixed_income"),
        "ALT": ("Alternative", "alternatives"),
        "MULTI": ("Multi-Asset", "multi_asset"),
        "CASH": ("Cash Equivalent", "cash"),
    }
    by_series = {row["series_id"]: row for row in factor_result["series"]}
    rows = []
    for instrument_id, (label, broad) in labels.items():
        components = {
            pillar: [component(broad, pillar, 1.0, index) for index in range(len(peers.COMPONENT_POLICY[broad][pillar]))]
            for pillar in peers.PILLARS
        }
        series = by_series[instrument_id]
        if broad == "alternatives":
            definitions = peers._trusted_investment_definitions(
                next(item for item in factor_source["series"] if item["series_id"] == instrument_id), series, broad
            )
            components["investment"] = [
                {
                    "component_id": definition["id"], "state": "unavailable" if definition["factor"] is None else "available",
                    "value": None, "coverage_pct": 0, "quality": "opaque", "source_kind": "factor",
                    "source_refs": ["a" * 64], "methodology_id": definition["methodology_id"],
                    "methodology_version": "v1", "direction": "higher", "diagnostic_only": False,
                    "factor_path": None if definition["factor"] is None else {"factor": definition["factor"]},
                }
                for definition in definitions
            ]
        if broad == "cash":
            components["investment"] = components["investment"][:1]
            for value in components["investment"]:
                value.update(state="not_applicable", value=None, coverage_pct=0, quality="opaque")
        elif broad == "multi_asset":
            sleeve = series["sleeves"][0]
            metrics = {metric["factor"]: metric for metric in sleeve["metrics"]}
            definitions = peers._trusted_investment_definitions(
                next(item for item in factor_source["series"] if item["series_id"] == instrument_id), series, broad
            )
            components["investment"] = []
            for definition in definitions:
                metric = metrics[definition["factor"]]
                components["investment"].append({
                    "component_id": definition["id"], "state": "available", "value": metric["value"],
                    "coverage_pct": metric["coverage_pct"], "quality": metric["quality_status"], "source_kind": "factor",
                    "source_refs": metric["source_refs"], "methodology_id": definition["methodology_id"], "methodology_version": "v1",
                    "direction": "higher", "diagnostic_only": False, "factor_path": {"sleeve_id": "fi", "factor": definition["factor"]},
                })
        elif generic and broad == "alternatives":
            for value in components["investment"]:
                value.update(state="unavailable", value=None, coverage_pct=0, quality="opaque")
        else:
            metrics = {metric["factor"]: metric for metric in series["metrics"]}
            for index, definition in enumerate(peers.COMPONENT_POLICY[broad]["investment"]):
                if definition["factor"] is None:
                    continue
                metric = metrics[definition["factor"]]
                components["investment"][index].update(
                    value=metric["value"], coverage_pct=metric["coverage_pct"], quality=metric["quality_status"], source_refs=metric["source_refs"],
                    factor_path={"factor": definition["factor"]},
                )
        if broad == "alternatives" and not generic:
            metrics = {metric["factor"]: metric for metric in series["metrics"]}
            for item in components["investment"]:
                metric = metrics[item["factor_path"]["factor"]]
                item.update(value=metric["value"], coverage_pct=metric["coverage_pct"], quality=metric["quality_status"], source_refs=metric["source_refs"])
        rows.append({
            "instrument_id": instrument_id,
            "series_id": instrument_id,
            "classification_label": label,
            "policy_id": "policy", "policy_version": "v1", "snapshot_id": "snapshot",
            "currency": "USD", "benchmark_family": peers._MATRIX[label][3], "dimensions": {},
            "components": components, "diagnostics": diagnostics(),
        })
    source = rehash({
        "schema_version": peers.INPUT_SCHEMA, "state": "complete",
        "snapshot": {"snapshot_id": "snapshot", "policy_id": "policy", "policy_version": "v1"},
        "methodology": {"id": "peer-v1", "version": "v1"}, "rows": sorted(rows, key=lambda row: row["instrument_id"]),
    })
    return source, matrix(), manifest, root, factor_source, factor_result


@pytest.mark.parametrize("generic", [False, True])
def test_w2b1_investment_resolution_is_exact_for_every_broad_class(tmp_path: Path, generic: bool) -> None:
    source, policy, manifest, root, finput, factor = _all_class_context(tmp_path, generic=generic)
    value = peers.build_authenticated_result(source, policy, phase4_manifest=manifest, w2a_bundle=root, factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40, runner_sha="e" * 40)
    assert {row["broad_class"] for row in value["scores"]} == {"equity", "fixed_income", "alternatives", "multi_asset", "cash"}
    for instrument_id in ("EQUITY", "FI", "ALT", "MULTI", "CASH"):
        broken = copy.deepcopy(source)
        target = next(row for row in broken["rows"] if row["instrument_id"] == instrument_id)
        target["components"]["investment"][0]["state"] = "available" if instrument_id in {"CASH", "ALT"} and generic else "unavailable"
        target["components"]["investment"][0]["value"] = 1.0 if target["components"]["investment"][0]["state"] == "available" else None
        rehash(broken)
        with pytest.raises(peers.ArtifactValidationError, match="investment factor"):
            peers.build_authenticated_result(broken, policy, phase4_manifest=manifest, w2a_bundle=root, factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40, runner_sha="e" * 40)


def test_closed_diagnostics_and_full_component_lineage_are_evidence_only(tmp_path: Path) -> None:
    result, context = build(tmp_path, 10)
    source, policy, manifest, root, finput, factor = context
    component_lineage = result["scores"][0]["pillars"]["investment"]["components"][0]
    assert {"qualified_value", "direction", "methodology_id", "methodology_version", "source_kind", "source_refs", "diagnostic_flag", "factor_path", "n_total", "n_valid", "original_weight", "effective_weight", "quality", "state", "coverage_pct", "status", "percentile"} <= set(component_lineage)
    assert result["scores"][0]["diagnostics"] == source["rows"][0]["diagnostics"]
    changed = copy.deepcopy(source)
    changed["rows"][0]["diagnostics"][0]["source_refs"] = ["c" * 64]
    rehash(changed)
    with pytest.raises(peers.ArtifactValidationError, match="diagnostic evidence"):
        peers.build_authenticated_result(changed, policy, phase4_manifest=manifest, w2a_bundle=root, factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40, runner_sha="e" * 40)
    changed["rows"][0]["diagnostics"][0]["diagnostic_id"] = "rr1"
    rehash(changed)
    with pytest.raises(peers.ArtifactValidationError, match="diagnostic"):
        peers.build_authenticated_result(changed, policy, phase4_manifest=manifest, w2a_bundle=root, factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40, runner_sha="e" * 40)


def test_data_quality_penalties_order_and_rehashed_lineage_tamper_fail(tmp_path: Path) -> None:
    source, policy, manifest, root, finput, factor = governed(tmp_path, 10)
    scores = {}
    for quality in ("certified", "degraded", "stale", "opaque"):
        candidate = copy.deepcopy(source)
        for row in candidate["rows"]:
            for item in row["components"]["data_quality"]:
                item["quality"] = quality
        rehash(candidate)
        value = peers.build_authenticated_result(candidate, policy, phase4_manifest=manifest, w2a_bundle=root, factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40, runner_sha="e" * 40)
        scores[quality] = value["scores"][-1]["pillars"]["data_quality"]["score"]
    assert scores["certified"] > scores["degraded"] > scores["stale"] > scores["opaque"] == 0
    result, context = build(tmp_path / "rehash", 10)
    result["scores"][0]["pillars"]["investment"]["components"][0]["source_refs"] = ["0" * 64]
    rehash(result)
    with pytest.raises(peers.ArtifactValidationError):
        peers.validate_authenticated_result(result, *context[:2], phase4_manifest=context[2], w2a_bundle=context[3], factor_input=context[4], w2b1_result=context[5], factor_runner_sha="f" * 40, runner_sha="e" * 40)


def test_typed_evidence_manifest_and_exact_factor_label_policy_benchmark_binding(tmp_path: Path) -> None:
    source, policy, phase4_manifest, root, finput, factor = governed(tmp_path, 10)
    value = peers.build_authenticated_result(
        source, policy, phase4_manifest=phase4_manifest, w2a_bundle=root,
        factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40,
        runner_sha="e" * 40,
    )
    assert value["binding"]["evidence_manifest_hash"] == peers.canonical_sha256(evidence())
    bad_evidence = evidence()
    next(entry for entry in bad_evidence["entries"] if entry["source_ref_id"] == "fixed_income.data_quality.freshness.v1")["diagnostic_only"] = True
    rehash(bad_evidence)
    with pytest.raises(peers.ArtifactValidationError, match="manifest binding"):
        peers._component(component("fixed_income", "data_quality", 1), peers.COMPONENT_POLICY["fixed_income"]["data_quality"][0], peers.validate_evidence_manifest(bad_evidence))
    relabeled = copy.deepcopy(source)
    relabeled["rows"][0]["classification_label"] = "High Yield Bond"
    relabeled["rows"][0]["benchmark_family"] = "GOVT"
    rehash(relabeled)
    with pytest.raises(peers.ArtifactValidationError, match="classification/policy"):
        peers.build_authenticated_result(
            relabeled, policy, phase4_manifest=phase4_manifest, w2a_bundle=root,
            factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40,
            runner_sha="e" * 40,
        )


def test_factor_path_shape_is_trusted_definition_specific(tmp_path: Path) -> None:
    source, policy, manifest, root, finput, factor = governed(tmp_path, 10)
    source["rows"][0]["components"]["investment"][0]["factor_path"] = {"sleeve_id": "invented", "factor": "rates"}
    rehash(source)
    with pytest.raises(peers.ArtifactValidationError, match="path"):
        peers.build_authenticated_result(source, policy, phase4_manifest=manifest, w2a_bundle=root, factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40, runner_sha="e" * 40)
    source, policy, manifest, root, finput, factor = _all_class_context(tmp_path / "classes")
    for instrument_id in ("EQUITY", "ALT"):
        candidate = copy.deepcopy(source)
        target = next(row for row in candidate["rows"] if row["instrument_id"] == instrument_id)
        target["components"]["investment"][0]["factor_path"]["sleeve_id"] = "invented"
        rehash(candidate)
        with pytest.raises(peers.ArtifactValidationError, match="path"):
            peers.build_authenticated_result(candidate, policy, phase4_manifest=manifest, w2a_bundle=root, factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40, runner_sha="e" * 40)
    assert peers.build_authenticated_result(source, policy, phase4_manifest=manifest, w2a_bundle=root, factor_input=finput, w2b1_result=factor, factor_runner_sha="f" * 40, runner_sha="e" * 40)
