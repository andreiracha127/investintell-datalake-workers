"""Artifact-only contracts for the governed N-PORT V2 look-through runner."""

from __future__ import annotations

import copy
from datetime import date, timedelta
from importlib import import_module
import json
from pathlib import Path
import subprocess
import sys

import pytest

from src.workers import nport_v2_lookthrough as runner


def phase4_manifest() -> dict:
    packages: list[dict[str, str]] = []
    for form, total in (("nport", 26), ("ncen", 17), ("rr1", 39)):
        packages.extend(
            {
                "package_id": f"{form}-{number}",
                "form": form,
                "state": "successful",
            }
            for number in range(total)
        )
    return {
        "schema_version": "phase4_completion_manifest/v1",
        "state": "complete",
        "packages": packages,
        "w1_producer_sha": "a" * 40,
        "inventory_hash": "b" * 64,
        "v2_holdings_source": {
            "relation": "sec_nport_holdings_v2",
            "publication_id": "v2-published-001",
        },
    }


def test_phase4_manifest_requires_exact_complete_inventory_and_v2_identity() -> None:
    validated = runner.validate_phase4_manifest(phase4_manifest())

    assert validated["form_counts"] == {"nport": 26, "ncen": 17, "rr1": 39}
    assert validated["v2_publication_id"] == "v2-published-001"


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda value: value.update(state="running"), "terminal complete"),
        (lambda value: value["packages"].pop(), "exactly 82"),
        (
            lambda value: value["packages"].__setitem__(0, value["packages"][1]),
            "duplicate package identity",
        ),
        (
            lambda value: value["v2_holdings_source"].update(relation="sec_nport_holdings"),
            "sec_nport_holdings_v2",
        ),
        (lambda value: value.update(inventory_hash="not-a-hash"), "inventory hash"),
        (lambda value: value.update(inventory_hash="b" * 65), "inventory hash"),
    ],
)
def test_phase4_manifest_fails_closed_for_forged_or_incomplete_inputs(mutation, match) -> None:
    manifest = copy.deepcopy(phase4_manifest())
    mutation(manifest)

    with pytest.raises(runner.ArtifactValidationError, match=match):
        runner.validate_phase4_manifest(manifest)


def test_governed_anchors_use_latest_date_per_quarter_in_oldest_first_order() -> None:
    dates = [
        date(2024, 1, 31), date(2024, 3, 31), date(2024, 6, 30),
        date(2024, 9, 30), date(2024, 12, 31), date(2025, 3, 31),
        date(2025, 6, 30), date(2025, 9, 29), date(2025, 9, 30),
        date(2025, 12, 31), date(2026, 3, 31),
    ]

    anchors = runner.derive_governed_anchors(dates)

    assert [anchor["report_date"] for anchor in anchors] == [
        "2024-06-30", "2024-09-30", "2024-12-31", "2025-03-31",
        "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31",
    ]
    assert [anchor["anchor_id"] for anchor in anchors] == [
        "2024Q2-2024-06-30", "2024Q3-2024-09-30", "2024Q4-2024-12-31",
        "2025Q1-2025-03-31", "2025Q2-2025-06-30", "2025Q3-2025-09-30",
        "2025Q4-2025-12-31", "2026Q1-2026-03-31",
    ]


@pytest.mark.parametrize(
    ("dates", "match"),
    [
        ([date(2025, 3, 31)] * 7, "exactly eight"),
        (
            [date(2024, 3, 31), date(2024, 6, 30), date(2024, 9, 30), date(2024, 12, 31),
             date(2025, 3, 31), date(2025, 6, 30), date(2025, 9, 30), date(2026, 6, 30)],
            "chain gap",
        ),
    ],
)
def test_governed_anchors_fail_closed_when_eight_consecutive_quarters_are_unavailable(dates, match) -> None:
    with pytest.raises(runner.ArtifactValidationError, match=match):
        runner.derive_governed_anchors(dates)


ANCHOR_DATES = [
    "2024-06-30", "2024-09-30", "2024-12-31", "2025-03-31",
    "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31",
]


def v2_input(*, publication_id: str = "v2-published-001") -> dict:
    return {
        "schema_version": "nport_v2_input_artifact/v1",
        "source": {
            "relation": "sec_nport_holdings_v2",
            "publication_id": publication_id,
        },
        "source_declarations": [],
        "root_series_ids": ["ROOT"],
        "fund_map": {"cusip": {}, "isin": {}},
        "sector_map": {"037833": "Information Technology"},
        "equity_sidecars": [],
        "holdings": [
            {
                "series_id": "ROOT",
                "report_date": report_date,
                "holdings": [{
                    "cusip": "037833100",
                    "isin": "US0378331005",
                    "issuer_name": "Apple Inc",
                    "asset_class": "EC",
                    "sector": "Technology",
                    "currency": "USD",
                    "pct_of_nav": 100.0,
                }],
            }
            for report_date in ANCHOR_DATES
        ],
    }


def remove_root_anchor_keep_anchor_date(value: dict) -> None:
    for record in value["holdings"]:
        if record["series_id"] == "ROOT":
            record["series_id"] = "OTHER"


def test_artifact_run_writes_deterministic_v2_bound_anchor_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "lookthrough-output"

    first = runner.run_artifact(
        phase4_manifest(), v2_input(), output_dir, runner_sha="c" * 40,
        require_all_eight=True,
    )
    first_bytes = (output_dir / "aggregate_manifest.json").read_bytes()
    second = runner.run_artifact(
        phase4_manifest(), v2_input(), output_dir, runner_sha="c" * 40,
        require_all_eight=True,
    )

    assert first == second
    assert first_bytes == (output_dir / "aggregate_manifest.json").read_bytes()
    assert first["state"] == "complete"
    assert first["phase4_manifest_hash"] == runner.canonical_sha256(phase4_manifest())
    assert first["v2_publication_id"] == "v2-published-001"
    assert len(first["anchors"]) == 8
    anchor = json.loads((output_dir / "anchors" / "2024Q2-2024-06-30.json").read_text())
    assert anchor["binding"]["input_artifact_hash"] == runner.canonical_sha256(v2_input())
    assert anchor["series"][0]["summary"]["direct_pct"] == pytest.approx(100.0)
    assert anchor["series"][0]["output_row_count"] == 5


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda value: value["source"].update(publication_id="other-publication"), "publication"),
        (lambda value: value["source"].update(relation="sec_nport_holdings"), "sec_nport_holdings_v2"),
        (remove_root_anchor_keep_anchor_date, "missing root series"),
    ],
)
def test_artifact_run_rejects_wrong_v2_source_or_missing_anchor_series(tmp_path, mutation, match) -> None:
    artifact = v2_input()
    mutation(artifact)

    with pytest.raises(runner.ArtifactValidationError, match=match):
        runner.run_artifact(
            phase4_manifest(), artifact, tmp_path / "out", runner_sha="c" * 40,
            require_all_eight=True,
        )


def test_artifact_run_refuses_output_within_git_or_source_root() -> None:
    with pytest.raises(runner.ArtifactValidationError, match="outside Git and source roots"):
        runner.run_artifact(
            phase4_manifest(), v2_input(), Path.cwd() / "artifacts" / "forbidden-run",
            runner_sha="c" * 40,
            require_all_eight=True,
        )


def test_cli_requires_exactly_one_execution_mode() -> None:
    with pytest.raises(SystemExit) as neither:
        runner.build_parser().parse_args([])
    with pytest.raises(SystemExit) as both:
        runner.build_parser().parse_args(["--artifact-only", "--shadow-db-write"])

    assert neither.value.code == 2
    assert both.value.code == 2


def test_shadow_mode_validates_bound_authorization_then_refuses_without_writer() -> None:
    manifest = phase4_manifest()
    authorization = {
        "stage": "phase4b_shadow",
        "runner_sha": "c" * 40,
        "phase4_manifest_hash": runner.canonical_sha256(manifest),
        "v2_publication_id": "v2-published-001",
        "command": "shadow-db-write",
        "target": "isolated-shadow",
        "role": "shadow_writer",
        "table_allowlist": list(runner.SHADOW_TABLE_ALLOWLIST),
    }

    with pytest.raises(runner.ArtifactValidationError, match="shadow_writer_unconfigured"):
        runner.run_shadow_unconfigured(manifest, v2_input(), authorization, runner_sha="c" * 40)

    authorization["table_allowlist"] = ["unapproved_table"]
    with pytest.raises(runner.ArtifactValidationError, match="table allowlist"):
        runner.run_shadow_unconfigured(manifest, v2_input(), authorization, runner_sha="c" * 40)


def test_artifact_only_module_has_no_direct_database_or_sql_surface() -> None:
    source = Path(runner.__file__).read_text(encoding="utf-8")

    assert "src.db" not in source
    assert "psycopg" not in source
    assert "execute(" not in source


def test_prior_complete_run_adds_deterministic_evidence_only_parity_counts(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    runner.run_artifact(
        phase4_manifest(), v2_input(), baseline, runner_sha="c" * 40,
        require_all_eight=True,
    )

    result = runner.run_artifact(
        phase4_manifest(), v2_input(), tmp_path / "candidate", runner_sha="c" * 40,
        require_all_eight=True, prior_complete_run=baseline,
    )

    parity = result["parity"]
    assert parity["evidence_only"] is True
    assert parity["direct"] == {
        "matched_count": 40, "changed_count": 0, "added_count": 0,
        "removed_count": 0, "value_delta": 0.0, "max_abs_delta": 0.0,
    }
    for metric in ("indirect", "signed", "gross", "net", "residual", "staleness", "country", "currency", "issuer", "asset_class", "sector"):
        assert metric in parity


def test_fresh_artifact_only_import_does_not_load_database_worker_modules() -> None:
    probe = (
        "import sys; import src.workers.nport_v2_lookthrough; "
        "assert 'src.db' not in sys.modules; assert 'psycopg' not in sys.modules"
    )

    completed = subprocess.run([sys.executable, "-c", probe], check=False, capture_output=True, text=True)

    assert completed.returncode == 0, completed.stderr


def test_supported_schema_versions_are_exact() -> None:
    manifest = phase4_manifest()
    manifest["schema_version"] = "phase4_completion_manifest/v2"
    with pytest.raises(runner.ArtifactValidationError, match="schema version"):
        runner.validate_phase4_manifest(manifest)

    artifact = v2_input()
    artifact["schema_version"] = "nport_v2_input_artifact/v2"
    with pytest.raises(runner.ArtifactValidationError, match="schema version"):
        runner.run_artifact(phase4_manifest(), artifact, Path("C:/tmp/out"), runner_sha="c" * 40, require_all_eight=True)


def test_local_pure_engine_matches_legacy_for_signed_derivative_stale_and_sidecar_semantics() -> None:
    legacy = import_module("src.workers.nport_lookthrough")

    root_day, child_day = date(2025, 3, 31), date(2024, 12, 31)
    holdings = {
        "ROOT": (root_day, [
            {"cusip": "037833100", "isin": "US0378331005", "issuer_name": "Apple", "asset_class": "EC", "sector": "CORP", "currency": "USD", "pct_of_nav": 50.0},
            {"cusip": "111111111", "isin": "US1111111111", "issuer_name": "Wrapper", "asset_class": "EC", "sector": "CORP", "currency": "USD", "pct_of_nav": 20.0},
            {"cusip": "222222222", "asset_class": "DE", "sector": "CORP", "currency": "USD", "pct_of_nav": -10.0},
            {"cusip": "LE:529900ABCDEF", "asset_class": "DBT", "sector": "CORP", "currency": "USD", "pct_of_nav": 20.0},
        ]),
        "CHILD": (child_day, [{"cusip": "594918104", "isin": "US5949181045", "issuer_name": "Microsoft", "asset_class": "EC", "sector": "CORP", "currency": "USD", "pct_of_nav": 100.0}]),
    }
    fund_map = {"cusip": {"111111111": "CHILD"}, "isin": {}}
    sidecars = {
        ("ROOT", root_day): runner.EquityInputs(70.0, 70.0, {"US": 70.0}, {"037833100": 50.0, "111111111": 20.0}, {"037833100": 50.0, "111111111": 20.0}),
    }
    def getter(series_id):
        return holdings.get(series_id)

    def equity(series_id, report_date):
        return sidecars.get((series_id, report_date))
    expected = legacy.expand_series("ROOT", getter, fund_map, sector_map={"037833": "Information Technology", "594918": "Information Technology"}, get_equity_inputs=equity)
    actual = runner.expand_series("ROOT", getter, fund_map, sector_map={"037833": "Information Technology", "594918": "Information Technology"}, get_equity_inputs=equity)

    assert actual == expected
    assert actual[1]["oldest_report_date"] == child_day
    assert actual[1]["derivatives_net_pct"] == pytest.approx(-10.0)
    assert actual[1]["nondecomposable_fund_pct"] == pytest.approx(0.0)


def test_roots_require_one_latest_eligible_report_per_anchor_quarter(tmp_path: Path) -> None:
    artifact = v2_input()
    records = artifact["holdings"]
    artifact["holdings"] = [
        {**copy.deepcopy(record), "report_date": (date.fromisoformat(record["report_date"]) - timedelta(days=1)).isoformat()}
        for record in records
    ] + [
        {**copy.deepcopy(record), "series_id": "GLOBAL"}
        for record in records
    ]

    result = runner.run_artifact(phase4_manifest(), artifact, tmp_path / "temporal", runner_sha="c" * 40, require_all_eight=True)

    assert [json.loads((tmp_path / "temporal" / "anchors" / f"{entry['anchor_id']}.json").read_text())["series"][0]["report_date"] for entry in result["anchors"]] == [
        (date.fromisoformat(value) - timedelta(days=1)).isoformat() for value in ANCHOR_DATES
    ]
    artifact["holdings"] = [record for record in artifact["holdings"] if record["report_date"] != "2025-06-29" or record["series_id"] != "ROOT"]
    with pytest.raises(runner.ArtifactValidationError, match="missing root series"):
        runner.run_artifact(phase4_manifest(), artifact, tmp_path / "missing-quarter", runner_sha="c" * 40, require_all_eight=True)


@pytest.mark.parametrize(
    ("target", "key"),
    [
        ("v2_holdings_source", "legacy_relation"),
        ("v2_holdings_source", "current_pointer"),
        ("source", "provider"),
        ("source", "legacy_relation"),
    ],
)
def test_nested_source_schema_rejects_contamination(target, key) -> None:
    manifest = phase4_manifest()
    artifact = v2_input()
    if target == "v2_holdings_source":
        manifest[target][key] = "sec_nport_holdings"
        with pytest.raises(runner.ArtifactValidationError, match="source schema"):
            runner.validate_phase4_manifest(manifest)
    else:
        artifact[target][key] = "sec_nport_holdings"
        with pytest.raises(runner.ArtifactValidationError, match="source schema"):
            runner._validate_v2_input(artifact, "v2-published-001")


def test_prior_bundle_rejects_self_rehashed_row_count_mismatch(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    runner.run_artifact(phase4_manifest(), v2_input(), baseline, runner_sha="c" * 40, require_all_eight=True)
    anchor_path = baseline / "anchors" / "2024Q2-2024-06-30.json"
    anchor = json.loads(anchor_path.read_text())
    anchor["series"][0]["output_row_count"] += 1
    anchor["content_hash"] = runner.canonical_sha256({key: value for key, value in anchor.items() if key != "content_hash"})
    anchor_path.write_text(json.dumps(anchor), encoding="utf-8")
    aggregate_path = baseline / "aggregate_manifest.json"
    aggregate = json.loads(aggregate_path.read_text())
    aggregate["anchors"][0]["content_hash"] = anchor["content_hash"]
    aggregate["anchors"][0]["output_row_count"] += 1
    aggregate["content_hash"] = runner.canonical_sha256({key: value for key, value in aggregate.items() if key != "content_hash"})
    aggregate_path.write_text(json.dumps(aggregate), encoding="utf-8")

    with pytest.raises(runner.ArtifactValidationError, match="row count"):
        runner.run_artifact(phase4_manifest(), v2_input(), tmp_path / "rejected", runner_sha="c" * 40, require_all_eight=True, prior_complete_run=baseline)


def test_value_parity_detects_weight_change_and_rejects_forged_prior_bundle(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    runner.run_artifact(phase4_manifest(), v2_input(), baseline, runner_sha="c" * 40, require_all_eight=True)
    changed = v2_input()
    changed["holdings"][0]["holdings"][0]["pct_of_nav"] = 50.0
    result = runner.run_artifact(phase4_manifest(), changed, tmp_path / "changed", runner_sha="c" * 40, require_all_eight=True, prior_complete_run=baseline)
    assert result["parity"]["direct"]["changed_count"] > 0
    assert result["parity"]["direct"]["max_abs_delta"] == pytest.approx(50.0)

    anchor_path = baseline / "anchors" / "2024Q2-2024-06-30.json"
    forged = json.loads(anchor_path.read_text())
    forged["series"][0]["summary"]["direct_pct"] = 0.0
    anchor_path.write_text(json.dumps(forged), encoding="utf-8")
    with pytest.raises(runner.ArtifactValidationError, match="hash"):
        runner.run_artifact(phase4_manifest(), changed, tmp_path / "rejected", runner_sha="c" * 40, require_all_eight=True, prior_complete_run=baseline)


def test_output_root_and_shadow_unknown_fields_fail_closed() -> None:
    for forbidden in (Path("E:/"), Path("E:/Edgard/nport/results"), Path.cwd()):
        with pytest.raises(runner.ArtifactValidationError):
            runner._require_external_output_dir(forbidden)
    auth = {
        "stage": "phase4b_shadow", "runner_sha": "c" * 40,
        "phase4_manifest_hash": runner.canonical_sha256(phase4_manifest()),
        "v2_publication_id": "v2-published-001", "command": "shadow-db-write",
        "target": "isolated-shadow", "role": "shadow_writer",
        "table_allowlist": list(runner.SHADOW_TABLE_ALLOWLIST), "current_pointer": "forbidden",
    }
    with pytest.raises(runner.ArtifactValidationError, match="schema"):
        runner.run_shadow_unconfigured(phase4_manifest(), v2_input(), auth, runner_sha="c" * 40)
