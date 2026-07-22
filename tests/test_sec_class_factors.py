"""Offline fixture contracts for governed SEC class-specific factor evidence."""

from __future__ import annotations

from datetime import date, timedelta
import json
import math
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

from src.workers import nport_v2_lookthrough as w2a
from src.workers import sec_class_factors as factors


ENVELOPE = {
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

def phase4() -> dict:
    packages = [
        {"package_id": f"{form}-{index}", "form": form, "state": "successful"}
        for form, amount in (("nport", 26), ("ncen", 17), ("rr1", 39))
        for index in range(amount)
    ]
    return {
        "schema_version": "phase4_completion_manifest/v1",
        "state": "complete",
        "packages": packages,
        "w1_producer_sha": "a" * 40,
        "inventory_hash": "b" * 64,
        "v2_holdings_source": {
            "relation": "sec_nport_holdings_v2",
            "publication_id": "fixture-v2",
        },
    }


def bundle(tmp_path: Path) -> Path:
    output = tmp_path / "w2a"
    dates = [
        "2024-06-30",
        "2024-09-30",
        "2024-12-31",
        "2025-03-31",
        "2025-06-30",
        "2025-09-30",
        "2025-12-31",
        "2026-03-31",
    ]
    source = {
        "schema_version": "nport_v2_input_artifact/v1",
        "source": {"relation": "sec_nport_holdings_v2", "publication_id": "fixture-v2"},
        "source_declarations": [],
        "root_series_ids": ["ROOT"],
        "fund_map": {"cusip": {}, "isin": {}},
        "sector_map": {},
        "equity_sidecars": [],
        "holdings": [
            {"series_id": "ROOT", "report_date": day, "holdings": []} for day in dates
        ],
    }
    w2a.run_artifact(
        phase4(), source, output, runner_sha="c" * 40, require_all_eight=True
    )
    return output


def days(count: int) -> list[str]:
    return [
        (date(2026, 1, 1) + timedelta(days=index)).isoformat() for index in range(count)
    ]


def returns(values: list[float], stale: set[int] | None = None) -> list[dict]:
    stale = stale or set()
    return [
        {
            "date": days(len(values))[index],
            "value": value,
            "stale": index in stale,
            "smoothed": False,
        }
        for index, value in enumerate(values)
    ]


def factor_rows(values: list[float]) -> list[dict]:
    return [
        {"date": days(len(values))[index], "value": value}
        for index, value in enumerate(values)
    ]


def fi_series(
    *, series_id: str = "FI", beta: float = 1.5, count: int = 36, drop_rates: int = 0
) -> dict:
    values = {
        "rates": [math.sin(index) / 100 for index in range(count)],
        "credit_spread": [math.cos(index * 0.7) / 100 for index in range(count)],
        "breakeven_inflation": [math.sin(index * 0.37) / 100 for index in range(count)],
        "fx": [math.cos(index * 0.23) / 100 for index in range(count)],
        "equity_beta": [math.sin(index * 0.53) / 100 for index in range(count)],
    }
    nav = [
        0.0005
        + beta * values["rates"][index]
        - 0.7 * values["credit_spread"][index]
        + (index % 3 - 1) / 10000
        for index in range(count)
    ]
    factors_by_name = {name: factor_rows(rows) for name, rows in values.items()}
    if drop_rates:
        factors_by_name["rates"] = factors_by_name["rates"][:-drop_rates]
    return {
        "series_id": series_id,
        "classification_label": "bond",
        "broad_class": "fixed_income",
        "policy_version": "policy/v1",
        "benchmark_id": "fi-bm",
        "benchmark_family": "fi",
        "benchmark_method": "relative",
        "nav_returns": returns(nav),
        "factors": factors_by_name,
        "source_hashes": {"nav": "1" * 64, "factors": "2" * 64},
    }


def equity_series() -> dict:
    count = 36
    latent = {
        f"latent_factor_{index}": factor_rows(
            [math.sin(day * index * 0.31) / 100 for day in range(count)]
        )
        for index in (1, 2)
    }
    nav = [
        0.001
        + 1.2 * latent["latent_factor_1"][index]["value"]
        - 0.4 * latent["latent_factor_2"][index]["value"]
        + (index % 2) / 10000
        for index in range(count)
    ]
    return {
        "series_id": "EQ",
        "classification_label": "equity",
        "broad_class": "equity",
        "policy_version": "policy/v1",
        "benchmark_id": "eq-bm",
        "benchmark_family": "equity",
        "benchmark_method": "relative",
        "nav_returns": returns(nav),
        "latent_factors": latent,
        "latent_model_evidence": {
            "model_id": "model",
            "model_version": "v1",
            "engine": "latent_factor_model",
            "production_fit": True,
            "converged": True,
            "oos_r2": 0.2,
            "sample_start": days(count)[0],
            "sample_end": days(count)[-1],
            "feature_set_hash": "3" * 64,
            "model_artifact_hash": "4" * 64,
            "stability_status": "stable",
        },
        "source_hashes": {"nav": "5" * 64, "factors": "4" * 64},
    }


def alternative_series(*, generic: bool = False) -> dict:
    common = {
        "series_id": "ALT",
        "classification_label": "alternative",
        "broad_class": "alternatives",
        "policy_version": "policy/v1",
        "source_hashes": {"nav": "6" * 64, "factors": "7" * 64},
        "alternative_subtype": "generic" if generic else "managed_futures",
    }
    if generic:
        return common
    values = {
        "trend": [math.sin(index * 0.4) / 100 for index in range(36)],
        "carry": [math.cos(index * 0.3) / 100 for index in range(36)],
        "fx": [math.sin(index * 0.2) / 100 for index in range(36)],
    }
    return {
        **common,
        "benchmark_id": "alt-bm",
        "benchmark_family": "alt",
        "benchmark_method": "relative",
        "nav_returns": returns(
            [values["trend"][index] + values["carry"][index] for index in range(36)],
            {0, 1},
        ),
        "factors": {name: factor_rows(rows) for name, rows in values.items()},
    }


def input_for(series: list[dict]) -> dict:
    return {
        "schema_version": "sec_class_factor_input/v2",
        "snapshot_id": "snapshot",
        "computed_at": "2026-06-30T00:00:00Z",
        "classification_policy": {
            "id": "policy",
            "version": "v1",
            "rules_sha256": "8" * 64,
        },
        "methodology": {"id": "sec-class-factor-ols-hac", "version": "v1"},
        "series": series,
    }


def test_fi_exact_envelope_hac_student_t_and_actual_coverage(tmp_path: Path) -> None:
    result = factors.run_artifact(
        phase4(),
        bundle(tmp_path),
        input_for([fi_series(drop_rates=2)]),
        tmp_path / "out",
        runner_sha="f" * 40,
    )
    metric = next(
        row for row in result["series"][0]["metrics"] if row["factor"] == "rates"
    )
    assert set(metric) == ENVELOPE
    assert metric["value"] == pytest.approx(1.5, abs=0.03)
    assert metric["coverage_pct"] == pytest.approx(100 * 34 / 36)
    assert metric["dof"] == 28
    assert metric["measurement_type"] == "estimated"
    assert metric["quality_status"] in {"certified", "degraded"}
    # This fixture is well conditioned, so it is the portable numerical oracle for
    # the HAC covariance and confidence interval calculation.
    assert metric["diagnostics"]["condition_number"] == pytest.approx(
        177.3394899451424, rel=1e-6
    )
    assert metric["hac_standard_error"] == pytest.approx(
        0.001324411727884817, rel=1e-6
    )
    assert metric["confidence_interval"] == pytest.approx(
        {
            "level": 0.95,
            "lower": 1.4975097622793052,
            "upper": 1.5029356311634585,
        },
        rel=1e-6,
    )
    assert factors.student_t_critical_95(10) == pytest.approx(2.228138852, abs=1e-7)
    assert factors.student_t_critical_95(30) == pytest.approx(2.042272456, abs=1e-7)


def test_equity_evidence_is_exact_bound_and_latent_only(tmp_path: Path) -> None:
    equity = equity_series()
    result = factors.run_artifact(
        phase4(),
        bundle(tmp_path),
        input_for([equity]),
        tmp_path / "equity",
        runner_sha="f" * 40,
    )
    assert [row["factor"] for row in result["series"][0]["metrics"]] == [
        "latent_factor_1",
        "latent_factor_2",
    ]
    assert "ipca" not in json.dumps(result).lower()
    for key, bad in (
        ("model_id", None),
        ("oos_r2", True),
        ("oos_r2", float("nan")),
        ("sample_start", "2026-02-01"),
        ("model_artifact_hash", "0" * 64),
    ):
        broken = equity_series()
        broken["latent_model_evidence"][key] = bad
        with pytest.raises(factors.ArtifactValidationError, match="latent"):
            factors.run_artifact(
                phase4(),
                bundle(tmp_path / str(key).replace("_", "")),
                input_for([broken]),
                tmp_path / f"bad-{key}",
                runner_sha="f" * 40,
            )


def test_generic_alternative_validates_small_schema_before_insufficient(
    tmp_path: Path,
) -> None:
    generic = alternative_series(generic=True)
    result = factors.run_artifact(
        phase4(),
        bundle(tmp_path),
        input_for([generic]),
        tmp_path / "generic",
        runner_sha="f" * 40,
    )
    assert result["series"][0]["quality_flags"] == ["insufficient_strategy"]
    generic["source_hashes"]["nav"] = "bad"
    with pytest.raises(factors.ArtifactValidationError, match="SHA"):
        factors.run_artifact(
            phase4(),
            bundle(tmp_path / "hash"),
            input_for([generic]),
            tmp_path / "hash",
            runner_sha="f" * 40,
        )
    generic = alternative_series(generic=True)
    generic["factors"] = {}
    with pytest.raises(factors.ArtifactValidationError, match="schema"):
        factors.run_artifact(
            phase4(),
            bundle(tmp_path / "extra"),
            input_for([generic]),
            tmp_path / "extra",
            runner_sha="f" * 40,
        )


def test_multi_asset_routes_every_class_and_weighted_evidence_coverage(
    tmp_path: Path,
) -> None:
    cash = {
        "series_id": "CASH",
        "classification_label": "cash",
        "broad_class": "cash_mmf",
        "policy_version": "policy/v1",
        "source_hashes": {"nav": "9" * 64, "factors": "a" * 64},
    }
    analytics = [
        fi_series(series_id="fi", drop_rates=2),
        equity_series(),
        alternative_series(),
        cash,
    ]
    sleeves = [
        {
            "sleeve_id": item["series_id"],
            "portfolio_coverage_pct": 25.0,
            "analytics": item,
        }
        for item in analytics
    ]
    multi = {
        "series_id": "MA",
        "classification_label": "multi",
        "broad_class": "multi_asset",
        "policy_version": "policy/v1",
        "source_hashes": {"nav": "b" * 64, "factors": "c" * 64},
        "sleeves": sleeves,
    }
    result = factors.run_artifact(
        phase4(),
        bundle(tmp_path),
        input_for([multi]),
        tmp_path / "multi",
        runner_sha="f" * 40,
    )
    assert {row["sleeve_id"] for row in result["series"][0]["sleeves"]} == {
        "fi",
        "EQ",
        "ALT",
        "CASH",
    }
    assert result["series"][0]["coverage_pct"] == pytest.approx(
        (100 * 34 / 36 + 100 + 100 * 34 / 36 + 0) / 4
    )
    assert (
        next(
            row for row in result["series"][0]["sleeves"] if row["sleeve_id"] == "CASH"
        )["quality_status"]
        == "not_applicable"
    )


@pytest.mark.parametrize("bad", ["alternative", "equity", "cash"])
def test_multi_asset_rejects_malformed_class_specific_sleeve(
    tmp_path: Path, bad: str
) -> None:
    analytics = fi_series()
    if bad == "alternative":
        analytics = alternative_series()
        analytics.pop("factors")
    elif bad == "equity":
        analytics = equity_series()
        analytics["latent_model_evidence"].pop("model_id")
    else:
        analytics = {**fi_series(), "broad_class": "cash_mmf"}
    multi = {
        "series_id": "MA",
        "classification_label": "multi",
        "broad_class": "multi_asset",
        "policy_version": "policy/v1",
        "source_hashes": {"nav": "b" * 64, "factors": "c" * 64},
        "sleeves": [
            {
                "sleeve_id": "one",
                "portfolio_coverage_pct": 100.0,
                "analytics": analytics,
            }
        ],
    }
    with pytest.raises(factors.ArtifactValidationError):
        factors.run_artifact(
            phase4(),
            bundle(tmp_path),
            input_for([multi]),
            tmp_path / "bad",
            runner_sha="f" * 40,
        )


def test_w2a_cohort_is_derived_not_self_oracled(tmp_path: Path) -> None:
    root = bundle(tmp_path)
    aggregate_path = root / "aggregate_manifest.json"
    aggregate = json.loads(aggregate_path.read_text())
    entry = aggregate["anchors"][-1]
    old = root / "anchors" / f"{entry['anchor_id']}.json"
    anchor = json.loads(old.read_text())
    entry["anchor_id"], entry["report_date"] = "2026Q2-2026-06-30", "2026-06-30"
    anchor["anchor"] = {
        "anchor_id": entry["anchor_id"],
        "report_date": entry["report_date"],
    }
    anchor["content_hash"] = w2a.canonical_sha256(
        {key: value for key, value in anchor.items() if key != "content_hash"}
    )
    old.unlink()
    (root / "anchors" / f"{entry['anchor_id']}.json").write_text(
        json.dumps(anchor), encoding="utf-8"
    )
    entry["content_hash"] = anchor["content_hash"]
    aggregate["content_hash"] = w2a.canonical_sha256(
        {key: value for key, value in aggregate.items() if key != "content_hash"}
    )
    aggregate_path.write_text(json.dumps(aggregate), encoding="utf-8")
    with pytest.raises(factors.ArtifactValidationError, match="cohort|anchor"):
        factors.run_artifact(
            phase4(),
            root,
            input_for([fi_series()]),
            tmp_path / "bad",
            runner_sha="f" * 40,
        )


def test_deep_result_validator_rejects_nested_self_rehashed_mutations(
    tmp_path: Path,
) -> None:
    result = factors.build_artifact_result(
        phase4(), bundle(tmp_path), input_for([fi_series()]), runner_sha="f" * 40
    )
    for mutation in (
        lambda value: value["series"][0].update(quality_status="complete"),
        lambda value: value["series"][0]["metrics"][0].update(coverage_pct=101.0),
        lambda value: value["series"][0]["metrics"][0]["confidence_interval"].update(
            lower=2.0, upper=1.0
        ),
    ):
        bad = json.loads(json.dumps(result))
        mutation(bad)
        bad["content_hash"] = factors.canonical_sha256(
            {key: value for key, value in bad.items() if key != "content_hash"}
        )
        with pytest.raises(factors.ArtifactValidationError):
            factors.validate_result(bad)


@pytest.mark.parametrize("kind", ["quality", "coverage", "metric_key", "binding"])
def test_result_validator_exact_nested_contract(tmp_path: Path, kind: str) -> None:
    value = factors.build_artifact_result(
        phase4(), bundle(tmp_path), input_for([fi_series()]), runner_sha="f" * 40
    )
    if kind == "quality":
        value["series"][0]["quality_status"] = "complete"
    elif kind == "coverage":
        value["series"][0]["metrics"][0]["coverage_pct"] = -1.0
    elif kind == "metric_key":
        value["series"][0]["metrics"][0].pop("benchmark_method")
    else:
        value["binding"]["unknown"] = "x"
    value["content_hash"] = factors.canonical_sha256(
        {key: item for key, item in value.items() if key != "content_hash"}
    )
    with pytest.raises(factors.ArtifactValidationError):
        factors.validate_result(value)


def test_reparse_helper_output_child_symlink_shadow_cli_and_db_free(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = factors.run_artifact(
        phase4(),
        bundle(tmp_path),
        input_for([fi_series()]),
        tmp_path / "out",
        runner_sha="f" * 40,
    )
    child = tmp_path / "out" / "factor_run.json"
    target = tmp_path / "target.json"
    target.write_text(child.read_text(), encoding="utf-8")
    child.unlink()
    try:
        os.symlink(target, child)
    except OSError:

        class FakeStat:
            st_mode = stat.S_IFREG
            st_file_attributes = 0x400

        monkeypatch.setattr(factors.os, "lstat", lambda _path: FakeStat())
        with pytest.raises(factors.ArtifactValidationError, match="reparse"):
            factors._assert_no_reparse(tmp_path / "fake")
    else:
        with pytest.raises(factors.ArtifactValidationError, match="reparse"):
            factors.run_artifact(
                phase4(),
                bundle(tmp_path / "again"),
                input_for([fi_series()]),
                tmp_path / "out",
                runner_sha="f" * 40,
            )
    preview = factors.build_artifact_result(
        phase4(),
        bundle(tmp_path / "shadow"),
        input_for([fi_series()]),
        runner_sha="f" * 40,
    )
    auth = {
        "stage": "phase6_shadow",
        "command": "shadow-db-write",
        "runner_sha": "f" * 40,
        "phase4_manifest_hash": preview["binding"]["phase4_manifest_hash"],
        "w2a_run_hash": preview["binding"]["w2a_run_hash"],
        "factor_input_hash": preview["binding"]["factor_input_hash"],
        "classification_policy_hash": preview["binding"]["classification_policy_hash"],
        "methodology_hash": preview["binding"]["methodology_hash"],
        "output_content_hash": preview["content_hash"],
        "target": "isolated-shadow",
        "role": "shadow_writer",
    }
    with pytest.raises(
        factors.ArtifactValidationError, match="shadow_writer_unconfigured"
    ):
        factors.run_shadow_unconfigured(
            phase4(),
            bundle(tmp_path / "shadow2"),
            input_for([fi_series()]),
            auth,
            runner_sha="f" * 40,
        )
    assert (
        subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; import src.workers.sec_class_factors; assert 'src.db' not in sys.modules; assert 'psycopg' not in sys.modules",
            ],
            check=False,
        ).returncode
        == 0
    )
    with pytest.raises(SystemExit):
        factors.build_parser().parse_args([])
    with pytest.raises(SystemExit):
        factors.build_parser().parse_args(["--artifact-only", "--shadow-db-write"])
    assert result["state"] == "complete"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda manifest: manifest["packages"][0].update(unexpected="x"),
        lambda manifest: manifest["packages"].__setitem__(
            0, {"package_id": 1, "form": "nport", "state": "successful"}
        ),
    ],
)
def test_phase4_nested_package_schema_fails_closed(tmp_path: Path, mutation) -> None:
    manifest = phase4()
    mutation(manifest)
    with pytest.raises(factors.ArtifactValidationError, match="package"):
        factors.build_artifact_result(
            manifest, bundle(tmp_path), input_for([fi_series()]), runner_sha="f" * 40
        )


def test_full_nav_window_coverage_and_equity_usable_boundaries(tmp_path: Path) -> None:
    alternative = factors.run_artifact(
        phase4(),
        bundle(tmp_path),
        input_for([alternative_series()]),
        tmp_path / "alt-coverage",
        runner_sha="f" * 40,
    )
    assert alternative["series"][0]["coverage_pct"] == pytest.approx(100 * 34 / 36)
    equity = equity_series()
    equity["nav_returns"][0]["stale"] = True
    equity["nav_returns"][-1]["smoothed"] = True
    equity["latent_model_evidence"]["sample_start"] = days(36)[1]
    equity["latent_model_evidence"]["sample_end"] = days(36)[-2]
    result = factors.run_artifact(
        phase4(),
        bundle(tmp_path / "equity"),
        input_for([equity]),
        tmp_path / "equity-coverage",
        runner_sha="f" * 40,
    )
    metric = result["series"][0]["metrics"][0]
    assert metric["coverage_pct"] == pytest.approx(100 * 34 / 36)
    assert metric["source_period_start"] == days(36)[1]
    assert metric["source_period_end"] == days(36)[-2]


def test_stability_diagnostics_and_unsafe_subwindow_are_governed(
    tmp_path: Path,
) -> None:
    value = factors.run_artifact(
        phase4(),
        bundle(tmp_path),
        input_for([fi_series()]),
        tmp_path / "stable",
        runner_sha="f" * 40,
    )
    diagnostics = value["series"][0]["metrics"][0]["diagnostics"]
    assert set(diagnostics) >= {
        "stability_rule",
        "stability_status",
        "stability_first_start",
        "stability_first_end",
        "stability_second_start",
        "stability_second_end",
        "stability_max_beta_delta",
    }
    broken = fi_series()
    for name in broken["factors"]:
        broken["factors"][name] = (
            broken["factors"][name][:8] + broken["factors"][name][8:]
        )
    for index, row in enumerate(broken["factors"]["credit_spread"][18:], start=18):
        row["value"] = broken["factors"]["rates"][index]["value"]
    result = factors.run_artifact(
        phase4(),
        bundle(tmp_path / "unstable"),
        input_for([broken]),
        tmp_path / "unstable-output",
        runner_sha="f" * 40,
    )
    assert result["series"][0]["quality_status"] in {"degraded", "insufficient"}


def test_hac_svd_reference_is_stable_at_near_limit_condition() -> None:
    """Regression fixture: cond(X) is approximately 1.9e7, below the hard gate."""
    start = date(2025, 1, 1)
    dates = [start + timedelta(days=index) for index in range(40)]
    epsilon = 2.39e-6
    x1 = [float(index) for index in range(40)]
    x2 = [value + epsilon * ((-1) ** index) for index, value in enumerate(x1)]
    returns = [
        0.35 + 1.2 * left - 0.4 * right + 0.03 * math.sin(index * 0.7)
        for index, (left, right) in enumerate(zip(x1, x2, strict=True))
    ]
    fitted = factors._fit(
        {
            "returns": [
                (day, value, False, False)
                for day, value in zip(dates, returns, strict=True)
            ],
            "factors": {
                "x1": dict(zip(dates, x1, strict=True)),
                "x2": dict(zip(dates, x2, strict=True)),
            },
            "raw": {"benchmark_id": "fixture", "benchmark_method": "fixture"},
            "hashes": {"nav": "1" * 64, "factors": "2" * 64},
        },
        "2026-07-19T00:00:00Z",
    )
    x1_metric = fitted["metrics"][0]
    x2_metric = fitted["metrics"][1]
    assert x1_metric["diagnostics"]["condition_number"] == pytest.approx(
        1.898745e7, rel=1e-5
    )
    assert x1_metric["diagnostics"]["condition_number"] < factors._CONDITION_MAX
    # x1 and x2 are intentionally almost collinear. Their individual SVD/HAC
    # covariance entries vary across legitimate BLAS/LAPACK reduction orders; the
    # governed result is their common exposure plus symmetric, non-significant uncertainty.
    assert x1_metric["value"] == pytest.approx(158.62340522206406, rel=1e-8)
    assert x2_metric["value"] == pytest.approx(-157.82341722136064, rel=1e-8)
    assert x1_metric["value"] + x2_metric["value"] == pytest.approx(0.8, abs=2e-4)
    assert x1_metric["hac_standard_error"] / x2_metric["hac_standard_error"] == pytest.approx(
        1.0, rel=0.1
    )
    for metric, expected_sign in ((x1_metric, 1), (x2_metric, -1)):
        assert math.isfinite(metric["value"])
        assert math.isfinite(metric["hac_standard_error"])
        assert metric["hac_standard_error"] > 2 * abs(metric["value"])
        assert metric["value"] * expected_sign > 0
        interval = metric["confidence_interval"]
        assert interval["lower"] < 0 < interval["upper"]
        assert interval["upper"] - interval["lower"] == pytest.approx(
            2 * metric["critical_value"] * metric["hac_standard_error"]
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda result: result["series"][0].update(unknown="x"),
        lambda result: result["series"][0]["metrics"][0].update(unit=None),
        lambda result: result["series"][0]["metrics"][0].update(methodology_id=""),
        lambda result: result["series"][0]["metrics"][0].update(n_observations=-1),
        lambda result: result["series"][0]["metrics"][0].update(critical_value_rule=""),
        lambda result: result["series"][0]["metrics"][0].update(diagnostics="bad"),
        lambda result: result["series"][0]["metrics"][0]["confidence_interval"].update(
            level=0.1
        ),
    ],
)
def test_deep_validator_rejects_reviewer_mutations(tmp_path: Path, mutation) -> None:
    result = factors.build_artifact_result(
        phase4(), bundle(tmp_path), input_for([fi_series()]), runner_sha="f" * 40
    )
    mutation(result)
    result["content_hash"] = factors.canonical_sha256(
        {key: value for key, value in result.items() if key != "content_hash"}
    )
    with pytest.raises(factors.ArtifactValidationError):
        factors.validate_result(result)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda result: result["series"][0].pop("sleeves"),
        lambda result: result["series"][0]["sleeves"][0].update(unknown="x"),
        lambda result: result["series"][0]["sleeves"][0].update(sleeve_id=""),
        lambda result: result["series"][0]["sleeves"][0].update(metrics="bad"),
        lambda result: result["series"][0]["sleeves"][0].update(
            time_series_coverage_pct=101.0
        ),
    ],
)
def test_deep_validator_rejects_multi_sleeve_mutations(
    tmp_path: Path, mutation
) -> None:
    single = fi_series()
    multi = {
        "series_id": "MA",
        "classification_label": "multi",
        "broad_class": "multi_asset",
        "policy_version": "policy/v1",
        "source_hashes": {"nav": "b" * 64, "factors": "c" * 64},
        "sleeves": [
            {
                "sleeve_id": "fi",
                "portfolio_coverage_pct": 100.0,
                "analytics": single,
            }
        ],
    }
    result = factors.build_artifact_result(
        phase4(), bundle(tmp_path), input_for([multi]), runner_sha="f" * 40
    )
    mutation(result)
    result["content_hash"] = factors.canonical_sha256(
        {key: value for key, value in result.items() if key != "content_hash"}
    )
    with pytest.raises(factors.ArtifactValidationError):
        factors.validate_result(result)


def test_series_id_order_and_duplicate_are_rejected(tmp_path: Path) -> None:
    unsorted = input_for([fi_series(series_id="Z"), fi_series(series_id="A")])
    with pytest.raises(factors.ArtifactValidationError, match="series_id"):
        factors.build_artifact_result(
            phase4(), bundle(tmp_path), unsorted, runner_sha="f" * 40
        )
    duplicate = input_for([fi_series(series_id="A"), fi_series(series_id="A")])
    with pytest.raises(factors.ArtifactValidationError, match="series_id"):
        factors.build_artifact_result(
            phase4(), bundle(tmp_path / "duplicate"), duplicate, runner_sha="f" * 40
        )


def _rehash_result(result: dict) -> None:
    result["content_hash"] = factors.canonical_sha256(
        {key: value for key, value in result.items() if key != "content_hash"}
    )


@pytest.mark.parametrize(
    "kind",
    [
        "critical",
        "unit",
        "methodology_id",
        "methodology_version",
        "hac_lag",
        "condition",
        "stable_delta",
        "fixed_income_not_applicable",
    ],
)
def test_semantic_self_rehashes_are_rejected(tmp_path: Path, kind: str) -> None:
    result = factors.build_artifact_result(
        phase4(), bundle(tmp_path), input_for([fi_series()]), runner_sha="f" * 40
    )
    series = result["series"][0]
    metric = series["metrics"][0]
    if kind == "critical":
        metric["critical_value"] = 9.0
        metric["confidence_interval"] = {
            "level": 0.95,
            "lower": metric["value"] - 9 * metric["hac_standard_error"],
            "upper": metric["value"] + 9 * metric["hac_standard_error"],
        }
    elif kind == "unit":
        metric["unit"] = "bananas"
    elif kind == "methodology_id":
        metric["methodology_id"] = "foreign"
    elif kind == "methodology_version":
        metric["methodology_version"] = "v9"
    elif kind == "hac_lag":
        metric["diagnostics"]["hac_lag"] = 999
    elif kind == "condition":
        metric["diagnostics"]["condition_number"] = 1e11
    elif kind == "stable_delta":
        metric["diagnostics"]["stability_status"] = "stable"
        metric["diagnostics"]["stability_max_beta_delta"] = 999.0
    else:
        series.update(
            quality_status="not_applicable",
            quality_flags=["cash_mmf_v1_not_applicable"],
            method="not_applicable",
            coverage_pct=0.0,
            metrics=[],
            intercept=None,
        )
    _rehash_result(result)
    with pytest.raises(factors.ArtifactValidationError):
        factors.validate_result(result)


@pytest.mark.parametrize("kind", ["empty", "duplicate", "reordered"])
def test_result_series_identity_semantics_are_rejected(
    tmp_path: Path, kind: str
) -> None:
    result = factors.build_artifact_result(
        phase4(),
        bundle(tmp_path),
        input_for([fi_series(series_id="A"), fi_series(series_id="B")]),
        runner_sha="f" * 40,
    )
    if kind == "empty":
        result["series"] = []
    elif kind == "duplicate":
        result["series"][1]["series_id"] = "A"
    else:
        result["series"].reverse()
    _rehash_result(result)
    with pytest.raises(factors.ArtifactValidationError):
        factors.validate_result(result)


def test_class_method_status_and_applicability_contracts(tmp_path: Path) -> None:
    cash = {
        "series_id": "CASH",
        "classification_label": "cash",
        "broad_class": "cash_mmf",
        "policy_version": "policy/v1",
        "source_hashes": {"nav": "9" * 64, "factors": "a" * 64},
    }
    multi = {
        "series_id": "MULTI",
        "classification_label": "multi",
        "broad_class": "multi_asset",
        "policy_version": "policy/v1",
        "source_hashes": {"nav": "b" * 64, "factors": "c" * 64},
        "sleeves": [
            {
                "sleeve_id": "fi",
                "portfolio_coverage_pct": 100.0,
                "analytics": fi_series(series_id="SLEEVE"),
            }
        ],
    }
    generic = alternative_series(generic=True)
    generic["series_id"] = "GALT"
    result = factors.build_artifact_result(
        phase4(),
        bundle(tmp_path),
        input_for(
            [
                alternative_series(),
                cash,
                equity_series(),
                fi_series(),
                generic,
                multi,
            ]
        ),
        runner_sha="f" * 40,
    )
    by_class = {
        row["broad_class"]: row
        for row in result["series"]
        if row["broad_class"] != "alternatives"
    }
    alternatives = [
        row for row in result["series"] if row["broad_class"] == "alternatives"
    ]
    assert by_class["fixed_income"]["method"] == "ols_hac"
    assert by_class["equity"]["method"] == "latent_factor_model"
    assert by_class["cash_mmf"] == {
        "series_id": "CASH",
        "classification_label": "cash",
        "broad_class": "cash_mmf",
        "quality_status": "not_applicable",
        "quality_flags": ["cash_mmf_v1_not_applicable"],
        "method": "not_applicable",
        "coverage_pct": 0.0,
        "metrics": [],
        "intercept": None,
    }
    assert by_class["multi_asset"]["method"] == "sleeve_specific_ols_hac"
    assert by_class["multi_asset"]["metrics"] == []
    assert by_class["multi_asset"]["intercept"] is None
    assert by_class["multi_asset"]["sleeves"]
    assert {
        row["quality_flags"][0] for row in alternatives if row["quality_flags"]
    } >= {"insufficient_strategy"}


def _governed_result(
    tmp_path: Path, series: list[dict]
) -> tuple[dict, dict, Path, dict]:
    manifest = phase4()
    root = bundle(tmp_path)
    source = input_for(series)
    result = factors.build_artifact_result(manifest, root, source, runner_sha="f" * 40)
    return result, manifest, root, source


def _validate_governed(result: dict, manifest: dict, root: Path, source: dict) -> None:
    factors.validate_result(
        result,
        phase4_manifest=manifest,
        w2a_bundle=root,
        factor_input=source,
        runner_sha="f" * 40,
    )


@pytest.mark.parametrize(
    "kind",
    [
        "series_flags",
        "source_refs",
        "benchmark",
        "windows",
        "computed_policy",
        "r_squared",
    ],
)
def test_authenticated_validator_rejects_coherent_fi_forgeries(
    tmp_path: Path, kind: str
) -> None:
    result, manifest, root, source = _governed_result(
        tmp_path, [fi_series(drop_rates=2)]
    )
    series = result["series"][0]
    rows = [*series["metrics"], series["intercept"]]
    if kind == "series_flags":
        assert series["quality_status"] == "degraded"
        series["quality_flags"] = []
    elif kind == "source_refs":
        for row in rows:
            row["source_refs"] = ["d" * 64, "e" * 64]
    elif kind == "benchmark":
        for row in rows:
            row["benchmark_id"] = "forged"
            row["benchmark_method"] = "absolute"
    elif kind == "windows":
        for row in rows:
            row.update(
                as_of="2025-02-05",
                source_period_start="2025-01-01",
                source_period_end="2025-02-05",
            )
            row["diagnostics"].update(
                stability_first_start="2025-01-01",
                stability_first_end="2025-01-18",
                stability_second_start="2025-01-19",
                stability_second_end="2025-02-05",
            )
    elif kind == "computed_policy":
        result["computed_at_policy"] = "foreign"
    else:
        for row in rows:
            row["r_squared"] = 2.0
    _rehash_result(result)
    with pytest.raises(factors.ArtifactValidationError):
        _validate_governed(result, manifest, root, source)


@pytest.mark.parametrize("kind", ["sleeve_flags", "generic_flags", "equity_engine"])
def test_authenticated_validator_rejects_class_specific_forgeries(
    tmp_path: Path, kind: str
) -> None:
    if kind == "sleeve_flags":
        multi = {
            "series_id": "MULTI",
            "classification_label": "multi",
            "broad_class": "multi_asset",
            "policy_version": "policy/v1",
            "source_hashes": {"nav": "b" * 64, "factors": "c" * 64},
            "sleeves": [
                {
                    "sleeve_id": "fi",
                    "portfolio_coverage_pct": 100.0,
                    "analytics": fi_series(series_id="SLEEVE", drop_rates=2),
                }
            ],
        }
        result, manifest, root, source = _governed_result(tmp_path, [multi])
        result["series"][0]["sleeves"][0]["quality_flags"] = []
    elif kind == "generic_flags":
        result, manifest, root, source = _governed_result(
            tmp_path, [alternative_series(generic=True)]
        )
        result["series"][0]["quality_flags"] = ["bananas"]
    else:
        result, manifest, root, source = _governed_result(tmp_path, [equity_series()])
        result["series"][0]["method"] = "instrumented_pca"
    _rehash_result(result)
    with pytest.raises(factors.ArtifactValidationError):
        _validate_governed(result, manifest, root, source)


def test_context_free_result_validation_is_rejected(tmp_path: Path) -> None:
    result, _, _, _ = _governed_result(tmp_path, [fi_series()])
    with pytest.raises(factors.ArtifactValidationError, match="governed context"):
        factors.validate_result(result)


@pytest.mark.parametrize(
    "series",
    [
        pytest.param([fi_series()], id="fixed_income"),
        pytest.param([equity_series()], id="equity"),
        pytest.param([alternative_series(generic=True)], id="generic_alternative"),
        pytest.param(
            [
                {
                    "series_id": "CASH",
                    "classification_label": "cash",
                    "broad_class": "cash_mmf",
                    "policy_version": "policy/v1",
                    "source_hashes": {"nav": "9" * 64, "factors": "a" * 64},
                }
            ],
            id="cash",
        ),
    ],
)
def test_authenticated_consumer_accepts_class_artifacts(
    tmp_path: Path, series: list[dict]
) -> None:
    result, manifest, root, source = _governed_result(tmp_path, series)
    _validate_governed(result, manifest, root, source)
    assert (
        factors.validate_authenticated_artifact(
            result,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=source,
            runner_sha="f" * 40,
        )
        == result
    )


def test_authenticated_consumer_accepts_multiasset_artifact(tmp_path: Path) -> None:
    multi = {
        "series_id": "MULTI",
        "classification_label": "multi",
        "broad_class": "multi_asset",
        "policy_version": "policy/v1",
        "source_hashes": {"nav": "b" * 64, "factors": "c" * 64},
        "sleeves": [
            {
                "sleeve_id": "fi",
                "portfolio_coverage_pct": 100.0,
                "analytics": fi_series(series_id="SLEEVE"),
            }
        ],
    }
    result, manifest, root, source = _governed_result(tmp_path, [multi])
    assert (
        factors.validate_authenticated_artifact(
            result,
            phase4_manifest=manifest,
            w2a_bundle=root,
            factor_input=source,
            runner_sha="f" * 40,
        )
        == result
    )
