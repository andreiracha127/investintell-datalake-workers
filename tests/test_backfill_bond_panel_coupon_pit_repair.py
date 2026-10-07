"""The coupon-PIT repair emitter: pinned artifact, deterministic child, verbatim copies, gated finalize."""
from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import backfill_bond_panel_coupon_pit_repair as repair
from scripts import backfill_bond_panel_history as backfill
from scripts import build_bond_panel_coupon_pit_returns as builder
from src.bonds.panel_resolvers import coupon_from_price_ytm

HEAD = "aab1db6a-306f-5011-b505-48cae95263a5"
RETURNS_SCHEMA = pa.schema([
    ("publication_id", pa.string()), ("month", pa.date32()), ("cusip_id", pa.string()),
    ("total_return", pa.float64()), ("price_return", pa.float64()), ("carry_return", pa.float64()),
    ("exit_basis", pa.string()), ("exit_reason", pa.string()), ("suspect", pa.bool_()), ("payload", pa.string()),
    ("distribution_rule", pa.string()), ("reference_cusip9", pa.string()), ("distribution_decision_id", pa.string()),
])


def _v2_dir(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    """A tiny v2 set whose stored carry is the full-history median over months <= 2025-03."""
    directory = tmp_path / "unit_repair_v2"
    directory.mkdir(parents=True)
    months = pd.date_range("2020-01-01", periods=60, freq="MS")
    panel = pd.DataFrame({
        "month": list(months.date) * 2,
        "cusip_id": ["AAA000001"] * 60 + ["BBB000002"] * 60,
        "price": [100.0] * 30 + [70.0] * 30 + [100.0] * 60,
        "ytm": [0.05] * 30 + [0.18] * 30 + [0.05] * 60,
        "maturity_years": [8.0] * 120,
        "coupon_pct": [None] * 120,
    })
    panel["month"] = pd.to_datetime(panel["month"])
    inversion = coupon_from_price_ytm(panel["price"], panel["ytm"], panel["maturity_years"])
    coupon = inversion.groupby(panel["cusip_id"]).median()
    rows = []
    for cusip, group in panel.groupby("cusip_id"):
        group = group.reset_index(drop=True)
        for index in range(1, len(group)):
            previous, current = group.loc[index - 1], group.loc[index]
            price_return = (current["price"] - previous["price"]) / previous["price"]
            carry = coupon[cusip] / 12 / previous["price"]
            rows.append({"publication_id": HEAD, "month": current["month"].date(), "cusip_id": cusip, "total_return": price_return + carry, "price_return": price_return, "carry_return": carry, "exit_basis": "observed", "exit_reason": None, "suspect": abs(price_return + carry) > 0.5, "payload": json.dumps({"source_lineage": {"frozen": True}}), "distribution_rule": "rule_144a", "reference_cusip9": cusip, "distribution_decision_id": None})
    rows.append({"publication_id": HEAD, "month": pd.Timestamp("2026-07-01").date(), "cusip_id": "AAA000001", "total_return": 0.02, "price_return": 0.01, "carry_return": 0.01, "exit_basis": "observed", "exit_reason": None, "suspect": False, "payload": "{}", "distribution_rule": "rule_144a", "reference_cusip9": "AAA000001", "distribution_decision_id": None})
    returns = pd.DataFrame(rows)
    panel_out = panel.copy()
    panel_out["month"] = panel_out["month"].dt.date
    pq.write_table(pa.Table.from_pandas(panel_out, preserve_index=False), directory / "bond_panel_live.parquet")
    pq.write_table(pa.Table.from_pandas(returns, schema=RETURNS_SCHEMA, preserve_index=False), directory / "bond_monthly_returns.parquet")
    hashes = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest() for name in builder.INPUT_FILES}
    (directory / "manifest.json").write_text(json.dumps({"artifact_sha256": hashes, "source_pointer": backfill.UNIT_REPAIR_FROM_HEAD_PUBLICATION_ID}), encoding="utf-8")
    return directory, hashes


def _artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, terms: bool = True) -> tuple[Path, dict[str, object]]:
    """Build a real v3 artifact with the builder and derive the authorization pins from its manifest."""
    v2, hashes = _v2_dir(tmp_path)
    monkeypatch.setattr(builder, "EXPECTED_SHA256_UNIT_REPAIR_V2", {**backfill.EXPECTED_SHA256_UNIT_REPAIR_V2, **hashes})
    monkeypatch.setattr(repair, "EXPECTED_SHA256_UNIT_REPAIR_V2", {**backfill.EXPECTED_SHA256_UNIT_REPAIR_V2, **hashes})
    terms_path = None
    if terms:
        terms_path = tmp_path / "bond_reference_terms_coupons.csv"
        terms_path.write_text("cusip9,coupon_rate,coupon_type\nAAA000001,6.5,Fixed\n", encoding="utf-8")
    out = tmp_path / "coupon_pit_v3"
    manifest = builder.build(v2, out, terms_path)
    pins = {
        "artifact_sha256": {name: hashlib.sha256((out / name).read_bytes()).hexdigest() for name in repair.COUPON_PIT_ARTIFACT_FILES},
        "terms_export_sha256": (manifest["inputs"]["terms_export"] or {}).get("sha256"),
        "per_year_digest": manifest["per_year_digest"],
        "counts": {key: manifest["counts"][key] for key in repair.COUPON_PIT_PINNED_COUNT_KEYS},
    }
    return out, pins


def test_artifacts_refuse_unpinned_preview_and_drifted_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out, pins = _artifact(tmp_path, monkeypatch)
    monkeypatch.setattr(repair, "COUPON_PIT_EXPECTED_ARTIFACT", None)
    with pytest.raises(backfill.ArtifactPinError, match="coupon_pit_artifact_unpinned"):
        repair.CouponPitArtifacts.open(out)
    opened = repair.CouponPitArtifacts.open(out, expected=pins)
    assert opened.sha256 == pins["artifact_sha256"]
    with pytest.raises(backfill.ArtifactPinError, match="artifact_sha256_mismatch:bond_monthly_returns.parquet"):
        repair.CouponPitArtifacts.open(out, expected={**pins, "artifact_sha256": {**pins["artifact_sha256"], builder.OUTPUT_RETURNS: "0" * 64}})
    with pytest.raises(backfill.PlanError, match="coupon_pit_terms_export_sha256_mismatch"):
        repair.CouponPitArtifacts.open(out, expected={**pins, "terms_export_sha256": "0" * 64})
    with pytest.raises(backfill.PlanError, match="coupon_pit_counts_mismatch"):
        repair.CouponPitArtifacts.open(out, expected={**pins, "counts": {**pins["counts"], "repriced_rows": 1}})
    preview, preview_pins = _artifact(tmp_path / "preview", monkeypatch, terms=False)
    with pytest.raises(backfill.PlanError, match="coupon_pit_preview_artifact_refused"):
        repair.CouponPitArtifacts.open(preview, expected=preview_pins)


def test_plan_is_deterministic_for_the_bound_head_and_pinned_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out, pins = _artifact(tmp_path, monkeypatch)
    artifacts = repair.CouponPitArtifacts.open(out, expected=pins)
    plan = repair.build_coupon_pit_plan(artifacts, from_head_publication_id=HEAD)
    again = repair.build_coupon_pit_plan(artifacts, from_head_publication_id=HEAD)
    assert plan == again
    assert plan.publication_id == str(uuid.uuid5(uuid.NAMESPACE_URL, f"{backfill.PRODUCT}:coupon-pit-repair:{plan.input_fingerprint}"))
    other_head = repair.build_coupon_pit_plan(artifacts, from_head_publication_id=str(uuid.uuid4()))
    assert other_head.publication_id != plan.publication_id
    evidence = plan.evidence()
    assert evidence["contract"] == "returns_coupon_pit_repair_v1"
    assert evidence["code_revision"] == "t3_returns_coupon_pit_repair_v1"
    assert evidence["config_hash"] == backfill.UNIT_REPAIR_CONFIG_HASH
    assert evidence["unit_repair_child_publication_id"] == backfill.UNIT_REPAIR_EXPECTED_PUBLICATION_ID
    assert evidence["counts"]["dropped_rows_no_pit_basis"] if "dropped_rows_no_pit_basis" in evidence["counts"] else True
    assert evidence["artifact_sha256"] == pins["artifact_sha256"]
    with pytest.raises(backfill.PlanError, match="coupon_pit_from_head_not_a_uuid"):
        repair.build_coupon_pit_plan(artifacts, from_head_publication_id="not-a-uuid")


def test_prepare_sql_verifies_the_live_head_and_declares_counts_without_moving_the_pointer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out, pins = _artifact(tmp_path, monkeypatch)
    artifacts = repair.CouponPitArtifacts.open(out, expected=pins)
    plan = repair.build_coupon_pit_plan(artifacts, from_head_publication_id=HEAD)
    sql = repair.render_coupon_pit_prepare_sql(plan, artifacts)
    assert "coupon pit prepare requires the expected head pointer" in sql
    assert "head.last_closed_month > '2026-06-01'::date" in sql
    assert backfill.UNIT_REPAIR_EXPECTED_PUBLICATION_ID in sql and backfill.UNIT_REPAIR_ROOT_BASE_PUBLICATION_ID in sql
    assert f"v_returns_before <> {plan.counts['rows_at_or_before_cutoff']}" in sql
    assert "INSERT INTO bond_panel_publications" in sql and "'prepared'" in sql
    assert "bond_panel_app_pointer SET" not in sql and "UPDATE bond_panel_app_pointer" not in sql
    assert plan.input_fingerprint in sql and plan.publication_id in sql and HEAD in sql
    assert "'coupon_pit_repair'" in sql and "t3_returns_coupon_pit_repair_v1" in sql


def test_copy_sql_is_verbatim_and_scopes_returns_to_months_after_the_cutoff(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out, pins = _artifact(tmp_path, monkeypatch)
    artifacts = repair.CouponPitArtifacts.open(out, expected=pins)
    plan = repair.build_coupon_pit_plan(artifacts, from_head_publication_id=HEAD)
    snapshot = repair.render_coupon_pit_copy_sql(plan, artifacts, "snapshot")
    assert "FROM bond_panel_current_snapshot_v1 source" in snapshot
    assert "coupon pit verbatim conflict:snapshot" in snapshot
    assert "dollar_volume * 1000000" not in snapshot and "unit_repair" not in snapshot
    assert "INSERT INTO bond_panel_publications" not in snapshot
    assert "ROW(candidate.month, candidate.cusip_id" in snapshot
    returns = repair.render_coupon_pit_copy_sql(plan, artifacts, "returns")
    assert "source.month > '2026-06-01'::date" in returns
    assert f"candidate.returns_rows - {plan.counts['rows_at_or_before_cutoff']}" in returns
    with pytest.raises(ValueError, match="unknown_surface"):
        repair.render_coupon_pit_copy_sql(plan, artifacts, "other")  # type: ignore[arg-type]


def test_batch_sql_loads_marked_artifact_rows_and_is_replay_safe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out, pins = _artifact(tmp_path, monkeypatch)
    artifacts = repair.CouponPitArtifacts.open(out, expected=pins)
    plan = repair.build_coupon_pit_plan(artifacts, from_head_publication_id=HEAD)
    first = repair.render_coupon_pit_batch_sql(plan, artifacts, start_after=0, limit=5)
    assert "COPY _backfill_stage" in first and first.count("\n") > 10
    assert "coupon pit batch requires the prepared coupon-pit child" in first
    assert "WHERE EXISTS (SELECT 1 FROM bond_panel_publications p WHERE p.publication_id='" + plan.publication_id + "'::uuid AND p.publication_status='prepared')" in first
    assert "'surface','returns_coupon_pit'" in first and "'done',false" in first
    assert "coupon_pit_repair" in first and "frozen" in first  # artifact payload kept, marker added
    assert pins["artifact_sha256"][builder.OUTPUT_RETURNS] in first
    total = plan.counts["rows_at_or_before_cutoff"]
    last = repair.render_coupon_pit_batch_sql(plan, artifacts, start_after=total - 1, limit=5)
    assert "'done',true" in last and f"'committed_through',{total}" in last
    # Repriced carry for AAA at 2021-01 (contractual 6.5 over the previous price 100): 6.5/12/100.
    assert "0.005416666666666667" in first or "0.0054166666666666" in first
    with pytest.raises(backfill.CursorError):
        repair.render_coupon_pit_batch_sql(plan, artifacts, start_after=-1, limit=5)


def test_finalize_sql_gates_keys_price_identity_carry_and_then_cas_and_refreshes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out, pins = _artifact(tmp_path, monkeypatch)
    artifacts = repair.CouponPitArtifacts.open(out, expected=pins)
    plan = repair.build_coupon_pit_plan(artifacts, from_head_publication_id=HEAD)
    sql = repair.render_coupon_pit_finalize_sql(plan, artifacts)
    for needle in (
        "LOCK TABLE bond_panel_snapshot, bond_panel_rv_signal, bond_panel_returns, bond_panel_rating_pit IN SHARE MODE",
        "candidate.price_return IS DISTINCT FROM source.price_return",
        "candidate.exit_basis IS DISTINCT FROM source.exit_basis",
        "abs(candidate.total_return - (candidate.price_return + candidate.carry_return)) > 1e-12",
        "candidate.suspect IS DISTINCT FROM (abs(candidate.total_return) > 0.5)",
        "candidate.payload @> jsonb_build_object('coupon_pit_repair'",
        "coupon pit artifact/DB per-year carry mismatch",
        "coupon pit source ancestry does not reach the unit-repair child and the frozen root",
        "coupon pit pointer compare-and-swap lost",
        "REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_returns_v1_mat;",
        "REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_snapshot_v1_mat;",
        "SET LOCAL lock_timeout = '5s'",
    ):
        assert needle in sql, needle
    assert sql.index("coupon pit artifact/DB per-year carry mismatch") < sql.index("SET publication_status = 'validated'") < sql.index("UPDATE bond_panel_app_pointer") < sql.index("COMMIT;")
    assert sql.index("COMMIT;") < sql.index("REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rv_signal_v1_mat;")
    expected = json.loads(repair._expected_per_year_json(plan))
    assert {item["year"] for item in expected} == {2020, 2021, 2022, 2023, 2024}
    assert all(item["sum_carry_after"] != item["sum_carry_before"] for item in expected if item["year"] >= 2020)


def test_cli_refuses_unpinned_artifacts_and_emits_when_pinned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    out, pins = _artifact(tmp_path, monkeypatch)
    monkeypatch.setattr(repair, "COUPON_PIT_EXPECTED_ARTIFACT", None)
    assert repair.main(["--plan", "--from-head", HEAD, "--artifact-dir", str(out)]) == 2
    assert "coupon_pit_artifact_unpinned" in capsys.readouterr().err
    monkeypatch.setattr(repair, "COUPON_PIT_EXPECTED_ARTIFACT", pins)
    assert repair.main(["--plan", "--from-head", HEAD, "--artifact-dir", str(out)]) == 0
    evidence = json.loads(capsys.readouterr().out)
    assert evidence["from_head_publication_id"] == HEAD and evidence["artifact_sha256"] == pins["artifact_sha256"]
    assert repair.main(["--emit-prepare", "--from-head", HEAD, "--artifact-dir", str(out)]) == 0
    assert "$coupon_pit_prepare$" in capsys.readouterr().out
    assert repair.main(["--emit-copy", "rating_pit", "--from-head", HEAD, "--artifact-dir", str(out)]) == 0
    assert "bond_panel_current_rating_pit_v1 source" in capsys.readouterr().out
    assert repair.main(["--emit-batch", "--limit", "3", "--from-head", HEAD, "--artifact-dir", str(out)]) == 0
    assert "COPY _backfill_stage" in capsys.readouterr().out
    assert repair.main(["--emit-finalize", "--from-head", HEAD, "--artifact-dir", str(out)]) == 0
    assert "$coupon_pit_finalize$" in capsys.readouterr().out
    assert repair.main(["--plan", "--from-head", "nope", "--artifact-dir", str(out)]) == 2
    assert "coupon_pit_from_head_not_a_uuid" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        repair.main(["--emit-batch", "--from-head", HEAD, "--artifact-dir", str(out)])


def test_authorization_constants_are_frozen_until_the_artifact_exists() -> None:
    assert repair.COUPON_PIT_EXPECTED_ARTIFACT is None
    assert repair.COUPON_PIT_CONTRACT == "returns_coupon_pit_repair_v1"
    assert repair.COUPON_PIT_CODE_REVISION == "t3_returns_coupon_pit_repair_v1"
    assert repair.COUPON_PIT_CUTOFF == "2026-06-01"
    assert repair.COUPON_PIT_AFFECTED_SURFACES == ("returns",)
    assert repair.COUPON_PIT_DEFAULT_ARTIFACT_DIRECTORY == backfill.UNIT_REPAIR_DEFAULT_ARTIFACT_DIRECTORY / "coupon_pit_v3"
