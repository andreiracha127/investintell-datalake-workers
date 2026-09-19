"""Contracts for the offline-only T3 historical panel publication emitter."""
from __future__ import annotations

import decimal
import hashlib
import json
import re
import uuid
from dataclasses import replace
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from scripts import backfill_bond_panel_history as backfill

UNIT_REPAIR_HEAD = backfill.UNIT_REPAIR_FROM_HEAD_PUBLICATION_ID


def _unit_repair_dir(tmp_path: Path, *, osbap_factor: float = 1.0) -> tuple[Path, dict[str, str], dict[str, int], dict[str, object]]:
    """Synthetic four-parquet v2 artifact set plus a provenance manifest."""
    directory = tmp_path / "unit_repair_v2"
    directory.mkdir(parents=True)
    rows_by_file: dict[str, list[dict[str, object]]] = {
        "bond_panel_live.parquet": [
            {"month": "2025-03-01", "cusip_id": "AAA000001", "price_source": "osbap", "dollar_volume": 5_000_000.0 * osbap_factor},
            {"month": "2025-04-01", "cusip_id": "BBB000002", "price_source": "trace_local", "dollar_volume": 2_000_000.0},
            {"month": "2026-06-01", "cusip_id": "AAA000001", "price_source": "osbap", "dollar_volume": 6_000_000.0 * osbap_factor},
        ],
        "universe_snapshots_live.parquet": [
            {"month": "2025-03-01", "cusip_id": "AAA000001", "price_source": "osbap", "dollar_volume": 5_000_000.0 * osbap_factor},
            {"month": "2025-04-01", "cusip_id": "BBB000002", "price_source": "trace_local", "dollar_volume": 2_000_000.0},
        ],
        "bond_monthly_returns.parquet": [
            {"month": "2025-04-01", "cusip_id": "AAA000001"},
            {"month": "2026-06-01", "cusip_id": "AAA000001"},
        ],
        "bond_ratings_pit.parquet": [
            {"month": "2025-03-01", "cusip_id": "AAA000001"},
            {"month": "2026-06-01", "cusip_id": "AAA000001"},
        ],
    }
    for name, rows in rows_by_file.items():
        _write(directory / name, rows)
    hashes = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest() for name in rows_by_file}
    counts = {"snapshot": 3, "rv_signal": 2, "returns": 2, "rating_pit": 2}
    provenance: dict[str, object] = {
        "source_pointer": UNIT_REPAIR_HEAD,
        "export_manifest_sha256": hashlib.sha256(b"t0.2v-export-manifest").hexdigest(),
        "export_datetime_utc": "2026-09-18T21:32:07Z",
        "export_files": {surface: hashlib.sha256(surface.encode()).hexdigest() for surface in counts},
    }
    manifest = {
        "manifest_version": "bond_panel_unit_repair_v2_manifest_v1",
        "unit_repair_contract": backfill.UNIT_REPAIR_CONTRACT,
        "unit": "usd",
        "predicate": "price_source == 'osbap'",
        "scale": 1000000.0,
        "source_pointer": UNIT_REPAIR_HEAD,
        "input_state": {"unit_repair_applied": True, "note": "synthetic test fixture"},
        "artifact_sha256": hashes,
        "artifacts": {name: {"rows": len(rows)} for name, rows in rows_by_file.items()},
        "export": {
            "manifest_sha256": provenance["export_manifest_sha256"],
            "export_datetime_utc": provenance["export_datetime_utc"],
            "files": [
                {"surface": surface, "sha256": digest}
                for surface, digest in provenance["export_files"].items()
            ],
        },
    }
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return directory, hashes, counts, provenance


def _open_unit_repair(
    directory: Path,
    hashes: dict[str, str],
    counts: dict[str, int],
    provenance: dict[str, object],
) -> backfill.UnitRepairArtifacts:
    return backfill.UnitRepairArtifacts.open(
        directory,
        expected_hashes=hashes,
        expected_counts=counts,
        expected_provenance=provenance,
    )


def _unit_repair_plan(tmp_path: Path) -> backfill.UnitRepairPlan:
    directory, hashes, counts, provenance = _unit_repair_dir(tmp_path)
    artifacts = _open_unit_repair(directory, hashes, counts, provenance)
    return backfill.build_unit_repair_plan(artifacts, from_head_publication_id=UNIT_REPAIR_HEAD)


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), path)


def _authorized_frozen_repair_plan() -> backfill.BackfillPlan:
    return backfill.BackfillPlan(
        publication_id=backfill.REPAIR_EXPECTED_PUBLICATION_ID,
        input_fingerprint=backfill.REPAIR_EXPECTED_INPUT_FINGERPRINT,
        cutoff=backfill.DEFAULT_CUTOFF,
        first_month=backfill.REPAIR_EXPECTED_FIRST_MONTH,
        last_closed_month=backfill.DEFAULT_CUTOFF,
        returns_last_month=backfill.DEFAULT_CUTOFF,
        counts=dict(backfill.REPAIR_EXPECTED_COUNTS),
        source_sha256=dict(backfill.EXPECTED_SHA256),
        panel_without_rating_pit=backfill.REPAIR_EXPECTED_PANEL_WITHOUT_RATING_PIT,
        base_repair=backfill._authorized_repair_base_evidence(),
        returns_first_month=backfill.REPAIR_EXPECTED_RETURNS_FIRST_MONTH,
    )


def _artifact_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "bond_panel_monthly"
    directory.mkdir(parents=True)
    history_months = [str(month.date()) for month in pd.date_range("2025-03-01", "2026-06-01", freq="MS")]
    _write(directory / "bond_panel_live.parquet", [
        {"cusip_id": "AAA000001", "month": "2025-03-01", "pr": 100.0, "ytm": .05, "mod_dur": 5.0, "bond_maturity": 4.0, "credit_spread": .012, "trade_count": 8.0, "dollar_volume": 9.0, "traded_days": 5, "prc_bid": 99.0, "prc_ask": 101.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 4.0, "db_type": 1.0, "price_source": "frozen"},
        {"cusip_id": "BBB000002", "month": "2025-04-01", "pr": 100.0, "ytm": .04, "mod_dur": 4.0, "bond_maturity": 5.0, "credit_spread": .011, "trade_count": 2.0, "dollar_volume": 2.0, "traded_days": 2, "prc_bid": None, "prc_ask": None, "rel_bid_ask_bps": None, "quoted_days": 0, "amt_outstanding_k": 200000, "ff17num": 5.0, "db_type": 1.0, "price_source": "frozen"},
        {"cusip_id": "DDD000004", "month": "2025-04-01", "pr": 101.0, "ytm": .04, "mod_dur": 4.0, "bond_maturity": 5.0, "credit_spread": .011, "trade_count": 6.0, "dollar_volume": 2.0, "traded_days": 5, "prc_bid": 100.0, "prc_ask": 102.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 5.0, "db_type": 1.0, "price_source": "frozen"},
        {"cusip_id": "AAA000001", "month": "2026-06-01", "pr": 102.0, "ytm": .05, "mod_dur": 5.0, "bond_maturity": 3.0, "credit_spread": .012, "trade_count": 8.0, "dollar_volume": 9.0, "traded_days": 5, "prc_bid": 101.0, "prc_ask": 103.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 4.0, "db_type": 1.0, "price_source": "frozen"},
        {"cusip_id": "CCC000003", "month": "2026-07-01", "pr": 101.0, "ytm": .05, "mod_dur": 3.0, "bond_maturity": 3.0, "credit_spread": .010, "trade_count": 3.0, "dollar_volume": 3.0, "traded_days": 5, "prc_bid": 100.0, "prc_ask": 102.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 6.0, "db_type": 1.0, "price_source": "future"},
        *[
            {"cusip_id": "AAA000001", "month": month, "pr": 100.0, "ytm": .05, "mod_dur": 5.0, "bond_maturity": 4.0, "credit_spread": .012, "trade_count": 8.0, "dollar_volume": 9.0, "traded_days": 5, "prc_bid": 99.0, "prc_ask": 101.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 4.0, "db_type": 1.0, "price_source": "frozen"}
            for month in history_months if month not in {"2025-03-01", "2026-06-01"}
        ],
    ])
    _write(directory / "universe_snapshots_live.parquet", [
        {"cusip_id": "AAA000001", "month": "2025-03-01", "pr": 100.0, "ytm": .05, "mod_dur": 5.0, "bond_maturity": 4.0, "credit_spread": .012, "trade_count": 8.0, "dollar_volume": 9.0, "traded_days": 5, "prc_bid": 99.0, "prc_ask": 101.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 4.0, "db_type": 1.0, "price_source": "frozen", "spread_final": .013, "rating_bucket": "A", "ever_held_window": True},
        {"cusip_id": "DDD000004", "month": "2025-04-01", "pr": 101.0, "ytm": .04, "mod_dur": 4.0, "bond_maturity": 5.0, "credit_spread": .011, "trade_count": 6.0, "dollar_volume": 2.0, "traded_days": 5, "prc_bid": 100.0, "prc_ask": 102.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 5.0, "db_type": 1.0, "price_source": "frozen", "spread_final": .014, "rating_bucket": "BBB", "ever_held_window": True},
        {"cusip_id": "AAA000001", "month": "2026-06-01", "pr": 102.0, "ytm": .05, "mod_dur": 5.0, "bond_maturity": 3.0, "credit_spread": .012, "trade_count": 8.0, "dollar_volume": 9.0, "traded_days": 5, "prc_bid": 101.0, "prc_ask": 103.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 4.0, "db_type": 1.0, "price_source": "frozen", "spread_final": .013, "rating_bucket": "A", "ever_held_window": True},
    ])
    _write(directory / "rv_signal_live.parquet", [
        {"cusip_id": "AAA000001", "month": "2025-03-01", "spread_bps": 130.0, "fitted_bps": 120.0, "residual_bps": 10.0, "rv_signal": 1.0},
        {"cusip_id": "DDD000004", "month": "2025-04-01", "spread_bps": 140.0, "fitted_bps": 130.0, "residual_bps": 10.0, "rv_signal": 1.0},
        {"cusip_id": "AAA000001", "month": "2026-06-01", "spread_bps": 130.0, "fitted_bps": 120.0, "residual_bps": 10.0, "rv_signal": 1.0},
    ])
    _write(directory / "bond_monthly_returns.parquet", [
        {"cusip_id": "AAA000001", "month": month, "total_return": .01, "price_return": .009, "carry_return": .001, "suspect": False}
        for month in history_months
    ])
    _write(directory / "bond_ratings_pit.parquet", [{"cusip_id": "AAA000001", "month": "2025-03-01", "rating_bucket": "A"}])
    return directory


def _hashes(directory: Path) -> dict[str, str]:
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in directory.glob("*.parquet")}


def test_artifact_pin_refuses_any_mismatched_source_before_plan(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    hashes = _hashes(directory)
    hashes["rv_signal_live.parquet"] = "0" * 64

    with pytest.raises(backfill.ArtifactPinError, match="artifact_sha256_mismatch:rv_signal_live.parquet"):
        backfill.ArtifactSet.open(directory, expected_hashes=hashes)


def test_plan_is_deterministic_and_excludes_open_and_future_months(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))

    plan = backfill.build_plan(artifacts, cutoff="2026-06-01")

    assert plan.counts == {"snapshot": 18, "rv_signal": 3, "returns": 16, "rating_pit": 18}
    assert plan.first_month == "2025-03-01"
    assert plan.last_closed_month == "2026-06-01"
    assert plan.returns_last_month == "2026-06-01"
    assert plan.config_hash == "0c0d78a866bc1090"
    returns = backfill.rows_for_surface(artifacts, plan, "returns", start_after=0, limit=10)
    assert returns.rows[-1]["payload"]["historical_return_coverage_through"] == plan.cutoff


def test_snapshot_retains_every_candidate_and_rating_uses_no_agency(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))
    plan = backfill.build_plan(artifacts, cutoff="2026-06-01")

    snapshots = backfill.rows_for_surface(artifacts, plan, "snapshot", start_after=0, limit=100)
    ratings = backfill.rows_for_surface(artifacts, plan, "rating_pit", start_after=0, limit=100)
    snapshot_rows = {(row["cusip_id"], row["month"]): row for row in snapshots.rows}
    rating_rows = {(row["cusip_id"], row["month"]): row for row in ratings.rows}

    aaa = snapshot_rows[("AAA000001", "2025-03-01")]
    bbb = snapshot_rows[("BBB000002", "2025-04-01")]
    ddd = snapshot_rows[("DDD000004", "2025-04-01")]
    assert aaa["issuer_id"] is None
    assert aaa["issuer_identity_state"] == "historical_identity_absent"
    assert bbb["eligibility_state"] == "excluded"
    assert bbb["eligibility_reason"] == "too_small"
    assert rating_rows[("BBB000002", "2025-04-01")]["rating_bucket"] == "NR"
    assert rating_rows[("BBB000002", "2025-04-01")]["rating_state"] == "historical_missing"
    assert rating_rows[("DDD000004", "2025-04-01")]["rating_bucket"] == "NR"
    assert rating_rows[("DDD000004", "2025-04-01")]["rating_state"] == "historical_missing"
    assert ddd["rating_bucket"] == "NR"
    assert ddd["rating_state"] == "historical_missing"
    assert "agency" not in rating_rows[("AAA000001", "2025-03-01")]


def test_plan_refuses_returns_not_present_in_snapshot_panel(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    returns_path = directory / "bond_monthly_returns.parquet"
    _write(returns_path, [
        {"cusip_id": "AAA000001", "month": "2025-03-01", "total_return": .01, "price_return": .009, "carry_return": .001, "suspect": False},
        {"cusip_id": "ZZZ000009", "month": "2026-06-01", "total_return": .01, "price_return": .009, "carry_return": .001, "suspect": False},
    ])
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))

    with pytest.raises(backfill.PlanError, match="returns_not_subset_of_panel"):
        backfill.build_plan(artifacts, cutoff="2026-06-01")


def test_plan_refuses_returns_that_do_not_reach_the_requested_cutoff(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    returns_path = directory / "bond_monthly_returns.parquet"
    _write(returns_path, [{"cusip_id": "AAA000001", "month": "2025-03-01", "total_return": .01, "price_return": .009, "carry_return": .001, "suspect": False}])
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))

    with pytest.raises(backfill.PlanError, match="returns_history_must_reach_cutoff"):
        backfill.build_plan(artifacts, cutoff="2026-06-01")


def test_plan_refuses_a_gap_in_historical_returns_before_cutoff(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    returns_path = directory / "bond_monthly_returns.parquet"
    returns = pq.read_table(returns_path)
    pq.write_table(returns.filter(pc.not_equal(returns["month"], "2025-10-01")), returns_path)
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))

    with pytest.raises(backfill.PlanError, match="returns_history_must_be_contiguous_through_cutoff"):
        backfill.build_plan(artifacts, cutoff="2026-06-01")


def test_cursor_bounds_and_psql_protocol_keep_pointer_until_finalize(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))
    plan = backfill.build_plan(artifacts, cutoff="2026-06-01")
    with pytest.raises(backfill.CursorError, match="start_after_exceeds_surface"):
        backfill.rows_for_surface(artifacts, plan, "snapshot", start_after=plan.counts["snapshot"] + 1, limit=1)

    prepare = backfill.render_prepare_sql(plan)
    batch = backfill.render_batch_sql(artifacts, plan, "snapshot", start_after=0, limit=1)
    finalize = backfill.render_finalize_sql(plan)

    assert "SET LOCAL ROLE worker_writer;" in prepare
    assert "bond_panel_app_pointer" not in prepare
    assert "\\set ON_ERROR_STOP on" in batch
    assert "COPY _backfill_stage" in batch
    assert "ON CONFLICT" in batch
    assert "immutable evidence conflict" in batch
    assert "selected" in batch and "committed_through" in batch and "done" in batch
    assert "inserted" in batch and "existing" in batch
    assert "publication_status='prepared'" in batch
    assert "UPDATE bond_panel_publications" in finalize
    assert "INSERT INTO bond_panel_app_pointer" in finalize
    assert "generate_series" in finalize
    assert "LEFT JOIN bond_panel_returns" in finalize
    assert "returns history is not contiguous through closed-month cutoff" in finalize


def test_plan_refuses_duplicate_and_missing_frozen_keys(tmp_path: Path) -> None:
    duplicate_directory = _artifact_dir(tmp_path / "duplicate")
    panel_path = duplicate_directory / "bond_panel_live.parquet"
    panel = pq.read_table(panel_path)
    pq.write_table(pa.concat_tables([panel, panel.slice(0, 1)]), panel_path)
    duplicate = backfill.ArtifactSet.open(duplicate_directory, expected_hashes=_hashes(duplicate_directory))
    with pytest.raises(backfill.PlanError, match="duplicate_month_cusip:panel"):
        backfill.build_plan(duplicate)

    missing_directory = _artifact_dir(tmp_path / "missing")
    missing_panel_path = missing_directory / "bond_panel_live.parquet"
    missing_panel = pq.read_table(missing_panel_path)
    pq.write_table(missing_panel.filter(pc.not_equal(missing_panel["cusip_id"], "AAA000001")), missing_panel_path)
    missing = backfill.ArtifactSet.open(missing_directory, expected_hashes=_hashes(missing_directory))
    with pytest.raises(backfill.PlanError, match="included_universe_missing_panel"):
        backfill.build_plan(missing)


def test_plan_refuses_rating_values_the_destination_schema_cannot_store(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    ratings_path = directory / "bond_ratings_pit.parquet"
    _write(ratings_path, [{"cusip_id": "AAA000001", "month": "2025-03-01", "rating_bucket": "C"}])
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))

    with pytest.raises(backfill.PlanError, match="rating_pit_value_invalid"):
        backfill.build_plan(artifacts)


def test_present_but_null_rating_is_typed_missing_not_historical_pit(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    ratings_path = directory / "bond_ratings_pit.parquet"
    _write(ratings_path, [{"cusip_id": "AAA000001", "month": "2025-03-01", "rating_bucket": None}])
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))
    plan = backfill.build_plan(artifacts)

    ratings = backfill.rows_for_surface(artifacts, plan, "rating_pit", start_after=0, limit=1)

    assert ratings.rows[0]["rating_bucket"] == "NR"
    assert ratings.rows[0]["rating_state"] == "historical_missing"
    assert ratings.rows[0]["rating_reason"] == "historical_rating_absent"
    assert ratings.rows[0]["rating_as_of_month"] is None


def test_plan_refuses_an_empty_historical_rating_source(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    ratings_path = directory / "bond_ratings_pit.parquet"
    # Preserve a typed, schema-valid empty source rather than failing at column discovery.
    import pyarrow as pa

    pq.write_table(
        pa.table({
            "cusip_id": pa.array([], type=pa.string()),
            "month": pa.array([], type=pa.string()),
            "rating_bucket": pa.array([], type=pa.string()),
        }),
        ratings_path,
    )
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))

    with pytest.raises(backfill.PlanError, match="source_empty:rating_pit"):
        backfill.build_plan(artifacts)


def test_emit_schema_installs_then_transfers_worker_ownership(capsys: pytest.CaptureFixture[str]) -> None:
    assert backfill.main(["--emit-schema"]) == 0
    emitted = capsys.readouterr().out
    assert emitted.startswith("\\set ON_ERROR_STOP on\nBEGIN;\n")
    assert "SET LOCAL ROLE worker_writer" not in emitted
    assert "CREATE TABLE IF NOT EXISTS bond_panel_publications" in emitted
    assert "ALTER TABLE bond_panel_publications OWNER TO worker_writer" in emitted
    assert "COMMIT;" in emitted


def test_repair_plan_reconstructs_missing_observed_tail_with_deterministic_lineage(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    # The frozen returns artifact is deliberately incomplete, as is the legacy
    # publication being replaced.  Snapshot prices remain frozen through June.
    _write(directory / "bond_monthly_returns.parquet", [
        # A return needs a predecessor price, so the first return month is one
        # month after the first snapshot month in the real frozen artifacts.
        {"cusip_id": "AAA000001", "month": "2025-03-01", "total_return": .01, "price_return": .009, "carry_return": .001, "suspect": False},
    ])
    panel_path = directory / "bond_panel_live.parquet"
    panel = pq.read_table(panel_path)
    extra = pa.Table.from_pylist([
        {"cusip_id": "AAA000001", "month": "2025-02-01", "pr": 100.0, "ytm": .05, "mod_dur": 5.0, "bond_maturity": 4.0, "credit_spread": .012, "trade_count": 8.0, "dollar_volume": 9.0, "traded_days": 5, "prc_bid": 99.0, "prc_ask": 101.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 4.0, "db_type": 1.0, "price_source": "frozen"},
        {"cusip_id": "BBB000002", "month": "2025-03-01", "pr": 99.0, "ytm": .06, "mod_dur": 4.0, "bond_maturity": 5.0, "credit_spread": .011, "trade_count": 6.0, "dollar_volume": 2.0, "traded_days": 5, "prc_bid": 98.0, "prc_ask": 100.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 5.0, "db_type": 1.0, "price_source": "frozen"},
        *[
            {"cusip_id": "BBB000002", "month": str(month.date()), "pr": 100.0, "ytm": .06, "mod_dur": 4.0, "bond_maturity": 5.0, "credit_spread": .011, "trade_count": 6.0, "dollar_volume": 2.0, "traded_days": 5, "prc_bid": 99.0, "prc_ask": 101.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 5.0, "db_type": 1.0, "price_source": "frozen"}
            for month in pd.date_range("2025-05-01", "2026-06-01", freq="MS")
        ],
        *[
            {"cusip_id": "CCC000003", "month": str(month.date()), "pr": 100.0, "ytm": .07, "mod_dur": 4.0, "bond_maturity": 5.0, "credit_spread": .011, "trade_count": 6.0, "dollar_volume": 2.0, "traded_days": 5, "prc_bid": 99.0, "prc_ask": 101.0, "rel_bid_ask_bps": 200.0, "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 5.0, "db_type": 1.0, "price_source": "frozen"}
            for month in pd.date_range("2025-04-01", "2026-06-01", freq="MS")
        ],
    ])
    pq.write_table(pa.concat_tables([panel, extra]), panel_path)
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))

    plan = backfill.build_repair_plan(
        artifacts, from_publication_id=backfill.LEGACY_REPAIR_FROM_PUBLICATION_ID,
    )
    rows = backfill.rows_for_surface(artifacts, plan, "returns", start_after=0, limit=100)
    tail = {(row["cusip_id"], row["month"]): row for row in rows.rows}

    assert plan.input_fingerprint != plan.evidence()["base_repair"]["from_artifact_fingerprint"]
    assert plan.counts["returns"] == 45
    assert plan.evidence()["returns_first_month"] == "2025-03-01"
    assert rows.total == 44
    assert all(row["month"] >= "2025-04-01" for row in rows.rows)
    repair = plan.evidence()["base_repair"]
    assert {key: repair[key] for key in (
        "contract", "from_publication_id", "from_config_hash", "from_input_fingerprint",
        "first_month", "last_closed_month", "reconstruction", "tail_rows", "tail_months",
    )} == {
        "contract": "legacy_parentless_return_coverage_repair_v1",
        "from_publication_id": backfill.LEGACY_REPAIR_FROM_PUBLICATION_ID,
        "from_config_hash": backfill.CONFIG_HASH,
        "from_input_fingerprint": backfill.LEGACY_REPAIR_FROM_INPUT_FINGERPRINT,
        "first_month": "2025-02-01",
        "last_closed_month": "2026-06-01",
        "reconstruction": "median_coupon_from_historical_carry_then_price_ytm_fallback",
        "tail_rows": 44,
        "tail_months": 15,
    }
    assert repair["tail_month_counts"] == [2, *([3] * 14)]
    assert len(repair["tail_digest"]) == 64
    assert tail[("AAA000001", "2025-04-01")]["price_return"] == 0.0
    assert tail[("AAA000001", "2025-04-01")]["carry_return"] == pytest.approx(.001)
    assert tail[("AAA000001", "2025-04-01")]["exit_basis"] == "observed"
    assert tail[("AAA000001", "2025-04-01")]["exit_reason"] is None
    assert tail[("BBB000002", "2025-04-01")]["carry_return"] > 0
    assert tail[("CCC000003", "2025-05-01")]["carry_return"] > 0


def test_repair_sql_copies_only_old_publication_facts_and_cas_points_exact_source(tmp_path: Path, monkeypatch) -> None:
    directory = _artifact_dir(tmp_path)
    panel_path = directory / "bond_panel_live.parquet"
    panel = pq.read_table(panel_path)
    pq.write_table(pa.concat_tables([panel, pa.Table.from_pylist([{
        "cusip_id": "AAA000001", "month": "2025-02-01", "pr": 100.0, "ytm": .05,
        "mod_dur": 5.0, "bond_maturity": 4.0, "credit_spread": .012,
        "trade_count": 8.0, "dollar_volume": 9.0, "traded_days": 5,
        "prc_bid": 99.0, "prc_ask": 101.0, "rel_bid_ask_bps": 200.0,
        "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 4.0,
        "db_type": 1.0, "price_source": "frozen",
    }])]), panel_path)
    _write(directory / "bond_monthly_returns.parquet", [
        {"cusip_id": "AAA000001", "month": "2025-03-01", "total_return": .01, "price_return": .009, "carry_return": .001, "suspect": False},
    ])
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))
    plan = backfill.build_repair_plan(artifacts, from_publication_id=backfill.LEGACY_REPAIR_FROM_PUBLICATION_ID)

    copies = {
        surface: backfill.render_repair_copy_sql(plan, surface)
        for surface in backfill.SURFACES
    }
    copied = copies["snapshot"]
    tail = backfill.render_batch_sql(artifacts, plan, "returns", start_after=0, limit=1)
    original_tail_rows = backfill._repair_return_tail_rows
    tail_calls: list[tuple[int, int | None]] = []

    def bounded_tail_rows(*args, start_after: int = 0, limit: int | None = None, **kwargs):
        tail_calls.append((start_after, limit))
        return original_tail_rows(*args, start_after=start_after, limit=limit, **kwargs)

    monkeypatch.setattr(backfill, "_repair_return_tail_rows", bounded_tail_rows)
    resumed_tail = backfill.render_batch_sql(artifacts, plan, "returns", start_after=1, limit=1)
    finalize = backfill.render_finalize_sql(plan)

    assert f"WHERE publication_id={backfill._sql_string(backfill.LEGACY_REPAIR_FROM_PUBLICATION_ID)}::uuid" in copied
    assert "INSERT INTO bond_panel_snapshot" in copied
    assert "bond_panel_live.parquet" not in copied
    assert "candidate.publication_status IN ('prepared','validated')" in copied
    assert "pointer.publication_id=candidate.publication_id" in copied
    assert (
        f"WHERE source.publication_id={backfill._sql_string(backfill.LEGACY_REPAIR_FROM_PUBLICATION_ID)}::uuid\n"
        "  AND EXISTS (\n      SELECT 1 FROM bond_panel_publications candidate"
    ) in copied
    for surface, copy_sql in copies.items():
        assert f"repair copy immutable evidence conflict:{surface}" in copy_sql
        for column in (
            "distribution_rule",
            "reference_cusip9",
            "distribution_decision_id",
            *backfill._COLUMNS[surface][1:],
        ):
            assert f"candidate.{column}" in copy_sql
            assert f"source.{column}" in copy_sql
    assert "COPY _backfill_stage" in tail
    assert "repair tail requires exact copied historical returns before tail" in tail
    assert "publication_status='prepared'" in tail
    assert "publication_status IN ('prepared','validated')" not in tail
    assert "bond_panel_repair_tail_batch_attestation" in resumed_tail
    assert "repair tail requires contiguous attested prefix" in resumed_tail
    assert "CREATE TEMP TABLE _repair_tail_prefix" not in resumed_tail
    assert tail_calls == [(1, 1)]
    assert "repair tail final count mismatch" in resumed_tail
    assert "immutable evidence conflict with existing target row" in resumed_tail
    assert "WHERE publication_id=" in finalize
    assert "AND publication_id=" in finalize
    assert "base_repair" in finalize
    assert backfill._sql_string(plan.returns_first_month) in finalize
    assert f"publication_id={backfill._sql_string(plan.publication_id)}::uuid" in finalize
    assert "bond_panel_repair_tail_batch_attestation" in finalize
    assert "repair terminal tail attestation missing or non-identical" in finalize
    assert f"committed_through={plan.base_repair['tail_rows']}" in finalize


def test_frozen_repair_plan_pins_authorized_identity_and_tail_facts_before_emission() -> None:
    plan = _authorized_frozen_repair_plan()

    assert "INSERT INTO bond_panel_publications" in backfill.render_prepare_sql(plan)
    assert "INSERT INTO bond_panel_returns" in backfill.render_repair_copy_sql(plan, "returns")
    assert "validated_and_pointed" in backfill.render_finalize_sql(plan)

    for drifted in (
        replace(plan, publication_id="00000000-0000-0000-0000-000000000000"),
        replace(plan, input_fingerprint="0" * 64),
        replace(plan, counts={**plan.counts, "returns": plan.counts["returns"] - 1}),
        replace(plan, base_repair={**plan.base_repair, "tail_digest": "0" * 64}),
        replace(plan, base_repair={**plan.base_repair, "tail_month_counts": [0] * 15}),
    ):
        with pytest.raises(backfill.PlanError, match="repair_plan_not_authorized"):
            backfill.render_prepare_sql(drifted)


def test_repair_returns_copy_replays_historical_facts_after_tail_without_counting_tail(tmp_path: Path) -> None:
    copy_sql = backfill.render_repair_copy_sql(_authorized_frozen_repair_plan(), "returns")

    assert "source.month <= (SELECT max(legacy.month)" in copy_sql
    assert "candidate.month <= (SELECT max(legacy.month)" in copy_sql
    assert "repair historical copy count mismatch:returns" in copy_sql
    assert "repair copy count mismatch:returns" not in copy_sql


def test_repair_terminal_tail_replay_is_validated_only_when_pointer_is_exact(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    panel_path = directory / "bond_panel_live.parquet"
    panel = pq.read_table(panel_path)
    pq.write_table(pa.concat_tables([panel, pa.Table.from_pylist([{
        "cusip_id": "AAA000001", "month": "2025-02-01", "pr": 100.0, "ytm": .05,
        "mod_dur": 5.0, "bond_maturity": 4.0, "credit_spread": .012,
        "trade_count": 8.0, "dollar_volume": 9.0, "traded_days": 5,
        "prc_bid": 99.0, "prc_ask": 101.0, "rel_bid_ask_bps": 200.0,
        "quoted_days": 5, "amt_outstanding_k": 300000, "ff17num": 4.0,
        "db_type": 1.0, "price_source": "frozen",
    }])]), panel_path)
    _write(directory / "bond_monthly_returns.parquet", [
        {"cusip_id": "AAA000001", "month": "2025-03-01", "total_return": .01, "price_return": .009, "carry_return": .001, "suspect": False},
    ])
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))
    plan = backfill.build_repair_plan(artifacts, from_publication_id=backfill.LEGACY_REPAIR_FROM_PUBLICATION_ID)

    terminal = backfill.render_batch_sql(
        artifacts, plan, "returns", start_after=plan.base_repair["tail_rows"] - 1, limit=1,
    )
    nonterminal = backfill.render_batch_sql(artifacts, plan, "returns", start_after=0, limit=1)

    assert "repair terminal tail replay requires validated pointed candidate" in terminal
    assert f"pointer.publication_id={backfill._sql_string(plan.publication_id)}::uuid" in terminal
    assert "candidate.publication_status='prepared'" in terminal
    assert "repair terminal tail replay requires validated pointed candidate" not in nonterminal


def test_normal_mode_still_refuses_a_missing_return_tail(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    _write(directory / "bond_monthly_returns.parquet", [
        {"cusip_id": "AAA000001", "month": "2025-03-01", "total_return": .01, "price_return": .009, "carry_return": .001, "suspect": False},
    ])
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))

    with pytest.raises(backfill.PlanError, match="returns_history_must_reach_cutoff"):
        backfill.build_plan(artifacts)


def test_unit_repair_plan_is_deterministic_and_pins_artifact_identity(tmp_path: Path) -> None:
    directory, hashes, counts, provenance = _unit_repair_dir(tmp_path)
    plan = backfill.build_unit_repair_plan(
        _open_unit_repair(directory, hashes, counts, provenance),
        from_head_publication_id=UNIT_REPAIR_HEAD,
    )
    again = backfill.build_unit_repair_plan(
        _open_unit_repair(directory, hashes, counts, provenance),
        from_head_publication_id=UNIT_REPAIR_HEAD,
    )

    assert plan == again
    assert plan.input_fingerprint == again.input_fingerprint
    assert plan.publication_id == str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"{backfill.PRODUCT}:unit-repair:{plan.input_fingerprint}")
    )
    assert plan.counts == counts
    assert plan.source_sha256 == hashes

    evidence = plan.evidence()
    assert evidence["contract"] == backfill.UNIT_REPAIR_CONTRACT
    assert evidence["code_revision"] == backfill.UNIT_REPAIR_CODE_REVISION
    assert evidence["from_head_publication_id"] == UNIT_REPAIR_HEAD
    assert evidence["root_base_publication_id"] == backfill.UNIT_REPAIR_ROOT_BASE_PUBLICATION_ID
    assert evidence["config_hash"] == backfill.UNIT_REPAIR_CONFIG_HASH
    assert (evidence["first_month"], evidence["last_closed_month"], evidence["open_month"]) == (
        "2002-07-01", "2026-08-01", "2026-09-01",
    )
    assert evidence["scale"] == 1000000
    assert evidence["predicate"] == "price_source = 'osbap'"
    assert evidence["affected_surfaces"] == ["snapshot", "rv_signal"]
    assert evidence["export_provenance"] == provenance

    snapshot_years = {item["year"]: item for item in plan.per_year if item["surface"] == "snapshot"}
    assert snapshot_years[2025]["rows"] == 2
    assert snapshot_years[2025]["nulls"] == 0
    assert snapshot_years[2025]["sum_dollar_volume"] == "7000000.000000"
    assert snapshot_years[2026]["sum_dollar_volume"] == "6000000.000000"
    rv_years = {item["year"]: item for item in plan.per_year if item["surface"] == "rv_signal"}
    assert rv_years[2025]["sum_dollar_volume"] == "7000000.000000"
    return_years = {item["year"]: item for item in plan.per_year if item["surface"] == "returns"}
    assert return_years[2025]["rows"] == 1
    assert return_years[2025]["sum_dollar_volume"] is None


def test_unit_repair_artifacts_refuse_the_wrong_head_and_broken_manifests(tmp_path: Path) -> None:
    directory, hashes, counts, provenance = _unit_repair_dir(tmp_path)
    artifacts = _open_unit_repair(directory, hashes, counts, provenance)

    with pytest.raises(backfill.PlanError, match="unit_repair_from_head_not_authorized"):
        backfill.build_unit_repair_plan(
            artifacts, from_head_publication_id="00000000-0000-0000-0000-000000000000",
        )

    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    del manifest["artifact_sha256"]
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(backfill.ArtifactPinError, match="unit_repair_manifest_artifact_sha256_absent"):
        _open_unit_repair(directory, hashes, counts, provenance)

    five_key = {**hashes, "rv_signal_live.parquet": "0" * 64}
    manifest["artifact_sha256"] = five_key
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(backfill.ArtifactPinError, match="unit_repair_manifest_artifact_sha256_key_mismatch"):
        _open_unit_repair(directory, hashes, counts, provenance)
    with pytest.raises(backfill.ArtifactPinError, match="unit_repair_artifact_map_not_four_surface"):
        _open_unit_repair(directory, five_key, counts, provenance)

    manifest["artifact_sha256"] = hashes
    manifest["input_state"] = {"unit_repair_applied": False}
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(backfill.PlanError, match="unit_repair_artifacts_not_repaired"):
        _open_unit_repair(directory, hashes, counts, provenance)

    manifest["input_state"] = {"unit_repair_applied": True}
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    drifted_hashes = {**hashes, "bond_panel_live.parquet": "0" * 64}
    with pytest.raises(backfill.ArtifactPinError, match="artifact_sha256_mismatch:bond_panel_live.parquet"):
        _open_unit_repair(directory, drifted_hashes, counts, provenance)
    with pytest.raises(backfill.ArtifactPinError, match="unit_repair_rows_mismatch:bond_panel_live.parquet"):
        _open_unit_repair(directory, hashes, {**counts, "snapshot": 4}, provenance)


def test_unit_repair_plan_refuses_unrepaired_and_double_scaled_inputs(tmp_path: Path) -> None:
    for factor in (1e-6, 1e7):
        directory, hashes, counts, provenance = _unit_repair_dir(tmp_path / f"factor_{factor}", osbap_factor=factor)
        with pytest.raises(backfill.PlanError, match="unit_repair_artifact_scale_out_of_band"):
            backfill.build_unit_repair_plan(
                _open_unit_repair(directory, hashes, counts, provenance),
                from_head_publication_id=UNIT_REPAIR_HEAD,
            )


def test_unit_repair_renderers_refuse_a_drifted_plan(tmp_path: Path) -> None:
    plan = _unit_repair_plan(tmp_path)

    for drifted in (
        replace(plan, counts={**plan.counts, "returns": plan.counts["returns"] - 1}),
        replace(plan, source_sha256={**plan.source_sha256, "bond_panel_live.parquet": "0" * 64}),
        replace(plan, per_year=plan.per_year[:-1]),
        replace(plan, export_provenance={**plan.export_provenance, "export_manifest_sha256": "0" * 64}),
        replace(plan, publication_id="00000000-0000-0000-0000-000000000000"),
        replace(plan, predicate="price_source = 'trace_local'"),
    ):
        with pytest.raises(backfill.PlanError, match="unit_repair_plan_not_authorized"):
            backfill.render_unit_repair_prepare_sql(drifted)
        with pytest.raises(backfill.PlanError, match="unit_repair_plan_not_authorized"):
            backfill.render_unit_repair_copy_sql(drifted, "snapshot")
        with pytest.raises(backfill.PlanError, match="unit_repair_plan_not_authorized"):
            backfill.render_unit_repair_finalize_sql(drifted)


def test_unit_repair_prepare_sql_pins_head_root_and_contract(tmp_path: Path) -> None:
    plan = _unit_repair_plan(tmp_path)
    sql = backfill.render_unit_repair_prepare_sql(plan)

    assert f"publication_id = '{UNIT_REPAIR_HEAD}'::uuid" in sql
    assert "unit repair prepare requires the expected head pointer" in sql
    assert "unit repair prepare requires the pinned head window" in sql
    assert backfill.UNIT_REPAIR_CONFIG_HASH in sql
    assert "unit repair prepare requires the frozen root provenance" in sql
    assert backfill.UNIT_REPAIR_ROOT_BASE_PUBLICATION_ID in sql
    assert backfill.REPAIR_CODE_REVISION in sql
    assert backfill.EXPECTED_SHA256["bond_panel_live.parquet"] in sql
    assert "unit repair child already validated for this head" in sql
    assert "unit repair prepare requires the pinned snapshot count" in sql
    assert "INSERT INTO bond_panel_publications" in sql
    assert "'prepared'" in sql
    assert f"'{plan.input_fingerprint}'" in sql
    assert "head.source_lineage || jsonb_build_object('source_sha256'" in sql
    assert "'unit_repair'" in sql
    assert "artifact_sha256_v2" in sql
    assert "export_provenance" in sql
    assert "'2002-07-01'::date" in sql and "'2026-08-01'::date" in sql and "'2026-09-01'::date" in sql
    assert f"{backfill.UNIT_REPAIR_SCALE}" in sql


def test_unit_repair_copy_sql_scales_only_volume_surfaces(tmp_path: Path) -> None:
    plan = _unit_repair_plan(tmp_path)
    copies = {surface: backfill.render_unit_repair_copy_sql(plan, surface) for surface in backfill.SURFACES}

    for surface in ("snapshot", "rv_signal"):
        sql = copies[surface]
        assert "CASE WHEN source.price_source = 'osbap' THEN source.dollar_volume * 1000000 ELSE source.dollar_volume END" in sql
        assert f"unit repair per-year osbap sum mismatch:{surface}" in sql
        assert f"unit repair null volume mismatch:{surface}" in sql
        assert f"unit repair marker conflict:{surface}" in sql
        assert "source.source_lineage || jsonb_build_object('unit_repair', " in sql
        assert "source.payload || jsonb_build_object('unit_repair', " in sql
        assert "|| '{" not in sql

    for surface in ("returns", "rating_pit"):
        sql = copies[surface]
        assert "* 1000000" not in sql
        assert "price_source = 'osbap'" not in sql
        assert "source_lineage || " not in sql
        assert "source.payload || " not in sql
        assert f"unit repair copy count mismatch:{surface}" in sql

    for surface, sql in copies.items():
        assert "SET LOCAL ROLE worker_writer;" in sql
        assert "unit repair copy requires the expected head pointer" in sql
        assert "unit repair copy requires the prepared unit-repair child" in sql
        assert "COALESCE(source.distribution_rule, 'rule_144a')" in sql
        assert "COALESCE(source.reference_cusip9, source.cusip_id)" in sql
        assert f"LOCK TABLE {backfill._TABLES[surface]} IN SHARE ROW EXCLUSIVE MODE;" in sql
        assert "ON CONFLICT (publication_id, month, cusip_id) DO NOTHING" in sql
        assert "candidate.publication_status = 'prepared'" in sql
        assert f"unit repair verbatim conflict:{surface}" in sql
        assert f"FROM bond_panel_current_{surface}_v1 source" in sql
        assert f"'{plan.publication_id}'::uuid" in sql


def test_unit_repair_finalize_sql_gates_then_cas_and_refreshes(tmp_path: Path) -> None:
    plan = _unit_repair_plan(tmp_path)
    sql = backfill.render_unit_repair_finalize_sql(plan)

    for surface in backfill.SURFACES:
        assert f"unit repair final count mismatch:{surface}" in sql
    assert "unit repair returns must start one month after the first snapshot month" in sql
    assert "unit repair returns history is not contiguous through the closed-month cutoff" in sql
    assert "unit repair rv_signal coverage mismatch" in sql
    assert "unit repair returns coverage mismatch" in sql
    assert "unit repair rating coverage mismatch" in sql
    assert "unit repair identity coverage invalid" in sql
    assert "unit repair cross-surface identity mismatch:rv_signal" in sql
    assert "unit repair cross-surface identity mismatch:returns" in sql
    assert "unit repair cross-surface identity mismatch:rating_pit" in sql
    assert "unit repair identity bootstrap missing" in sql
    assert "unit repair artifact/DB aggregate mismatch" in sql
    assert "jsonb_to_recordset(" in sql
    snapshot_sums = {item["year"]: item["sum_dollar_volume"] for item in plan.per_year if item["surface"] == "snapshot"}
    assert snapshot_sums[2025] in sql
    assert '"surface":"snapshot"' in sql
    assert "UPDATE bond_panel_publications" in sql
    assert "publication_status = 'validated'" in sql
    assert "publication_status = 'prepared'" in sql
    assert f"WHERE product = 'bond_panel_v1' AND publication_id = '{UNIT_REPAIR_HEAD}'::uuid" in sql
    assert "unit repair pointer compare-and-swap lost" in sql
    assert "COMMIT;" in sql
    assert "dollar_volume * 1000000" not in sql
    refresh_order = [
        sql.index("REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rv_signal_v1_mat;"),
        sql.index("REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_returns_v1_mat;"),
        sql.index("REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rating_pit_v1_mat;"),
        sql.index("REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_snapshot_v1_mat;"),
    ]
    assert refresh_order == sorted(refresh_order)


def test_unit_repair_cli_plans_and_emits_without_touching_a_database(
    tmp_path: Path, monkeypatch, capsys: pytest.CaptureFixture[str],
) -> None:
    directory, hashes, counts, provenance = _unit_repair_dir(tmp_path)
    monkeypatch.setattr(backfill, "EXPECTED_SHA256_UNIT_REPAIR_V2", hashes)
    monkeypatch.setattr(backfill, "UNIT_REPAIR_EXPECTED_COUNTS", counts)
    monkeypatch.setattr(backfill, "EXPECTED_EXPORT_PROVENANCE", provenance)

    assert backfill.main(["--plan", "--unit-repair-from-head", UNIT_REPAIR_HEAD, "--artifact-dir", str(directory)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["counts"] == counts
    assert payload["from_head_publication_id"] == UNIT_REPAIR_HEAD
    assert payload["publication_id"].count("-") == 4

    assert backfill.main(["--emit-unit-repair-copy", "snapshot", "--unit-repair-from-head", UNIT_REPAIR_HEAD, "--artifact-dir", str(directory)]) == 0
    emitted = capsys.readouterr().out
    assert "CASE WHEN source.price_source = 'osbap' THEN source.dollar_volume * 1000000 ELSE source.dollar_volume END" in emitted
    assert "INSERT INTO bond_panel_publications" not in emitted

    assert backfill.main(["--emit-prepare", "--unit-repair-from-head", UNIT_REPAIR_HEAD, "--artifact-dir", str(directory)]) == 0
    prepare = capsys.readouterr().out
    assert "INSERT INTO bond_panel_publications" in prepare
    assert "$unit_repair_prepare$" in prepare

    assert backfill.main(["--emit-finalize", "--unit-repair-from-head", UNIT_REPAIR_HEAD, "--artifact-dir", str(directory)]) == 0
    finalize = capsys.readouterr().out
    assert "unit repair pointer compare-and-swap lost" in finalize
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_snapshot_v1_mat;" in finalize

    assert backfill.main(["--plan", "--unit-repair-from-head", "00000000-0000-0000-0000-000000000000", "--artifact-dir", str(directory)]) == 2
    assert "unit_repair_from_head_not_authorized" in capsys.readouterr().err

    assert backfill.main(["--emit-unit-repair-copy", "returns", "--artifact-dir", str(directory)]) == 2
    assert "unit_repair_from_head_required" in capsys.readouterr().err

    assert backfill.main(["--emit-batch", "snapshot", "--limit", "1", "--unit-repair-from-head", UNIT_REPAIR_HEAD, "--artifact-dir", str(directory)]) == 2
    assert "unit_repair_does_not_accept_emit_batch" in capsys.readouterr().err


def _split_sql_list(value: str) -> list[str]:
    """Split a SQL expression list on top-level commas (parens and strings aware)."""
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    quoted = False
    index = 0
    while index < len(value):
        char = value[index]
        if quoted:
            current.append(char)
            if char == "'":
                if index + 1 < len(value) and value[index + 1] == "'":
                    current.append(value[index + 1])
                    index += 2
                    continue
                quoted = False
        elif char == "'":
            quoted = True
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        index += 1
    parts.append("".join(current).strip())
    return parts


def _copy_projection_shapes(sql: str) -> dict[str, str]:
    """Map each INSERT target column to its SELECT expression text, in order."""
    match = re.search(r"INSERT INTO \S+ \(([^)]*)\)\s*\nSELECT (.+)\nFROM ", sql)
    assert match, "copy insert statement not found"
    columns = [column.strip() for column in match.group(1).split(",")]
    expressions = _split_sql_list(match.group(2))
    assert len(columns) == len(expressions)
    return dict(zip(columns, expressions))


def _verbatim_predicate(sql: str) -> tuple[str, list[str]]:
    match = re.search(
        r"ROW\((candidate\.[^)]*)\) IS DISTINCT FROM ROW\((source\.[^)]*)\)", sql,
    )
    assert match, "verbatim row gate not found"
    candidate_columns = re.findall(r"candidate\.(\w+)", match.group(1))
    source_columns = re.findall(r"source\.(\w+)", match.group(2))
    assert candidate_columns == source_columns
    predicate = f"ROW({match.group(1)}) IS DISTINCT FROM ROW({match.group(2)})"
    return predicate, candidate_columns


def test_unit_repair_verbatim_gate_excludes_publication_id(tmp_path: Path) -> None:
    """The row gate must compare exactly the non-key, non-by-design columns.

    ``publication_id`` is in the INSERT target but is C's identity, never H's,
    so it must be excluded from the comparison.  The join keys ``month`` and
    ``cusip_id`` stay in the gate (equal by construction, so the duplicate check
    is harmless), and the union of the remaining compared columns with the
    excluded data columns (identity fill and, on volume surfaces, the scaled
    volume plus the lineage/payload marker) is exactly the INSERT target minus
    the keys and the publication identity.
    """
    plan = _unit_repair_plan(tmp_path)
    for surface in backfill.SURFACES:
        sql = backfill.render_unit_repair_copy_sql(plan, surface)
        _predicate, compared = _verbatim_predicate(sql)
        insert_columns = set(backfill._COPY_COLUMNS[surface])
        excluded_by_design = {"publication_id", *backfill._UNIT_REPAIR_IDENTITY_COLUMNS}
        if surface in backfill.UNIT_REPAIR_AFFECTED_SURFACES:
            excluded_by_design |= {"dollar_volume", *backfill._UNIT_REPAIR_MARKER_COLUMNS}

        assert "publication_id" not in compared
        assert {"month", "cusip_id"} <= set(compared)
        compared_data = set(compared) - {"month", "cusip_id"}
        assert compared_data == (insert_columns - {"month", "cusip_id"}) - excluded_by_design
        assert compared_data | (excluded_by_design - {"publication_id"}) == (
            insert_columns - {"month", "cusip_id", "publication_id"}
        )


def test_unit_repair_verbatim_gate_semantics_on_publication_only_differences(tmp_path: Path) -> None:
    """DuckDB replay of the emitted predicate: H-vs-C identity must not trip it."""
    plan = _unit_repair_plan(tmp_path)
    for surface in backfill.SURFACES:
        sql = backfill.render_unit_repair_copy_sql(plan, surface)
        predicate, compared = _verbatim_predicate(sql)
        con = duckdb.connect()
        try:
            columns = list(backfill._COPY_COLUMNS[surface])
            column_defs = ", ".join(f'"{column}" VARCHAR' for column in columns)
            con.execute(f"CREATE TABLE candidate ({column_defs})")
            con.execute(f"CREATE TABLE source ({column_defs})")
            child_row = {column: f"C-{column}" for column in columns}
            view_row = dict(child_row)
            # Differences that exist by design between C and the current view.
            view_row["publication_id"] = "71b672c8-239c-55bc-bccb-ed39960c0fd2"
            view_row["distribution_rule"] = None
            view_row["reference_cusip9"] = None
            view_row["distribution_decision_id"] = None
            if surface in backfill.UNIT_REPAIR_AFFECTED_SURFACES:
                view_row["dollar_volume"] = "1.0"
                view_row["source_lineage"] = "{}"
                view_row["payload"] = "{}"
            placeholders = ", ".join("?" for _ in columns)
            column_list = ", ".join(f'"{column}"' for column in columns)
            con.execute(
                f"INSERT INTO candidate ({column_list}) VALUES ({placeholders})",
                [child_row[column] for column in columns],
            )
            con.execute(
                f"INSERT INTO source ({column_list}) VALUES ({placeholders})",
                [view_row[column] for column in columns],
            )
            gate = (
                "SELECT count(*) FROM candidate JOIN source USING (month, cusip_id) "
                f"WHERE {predicate}"
            )
            assert con.execute(gate).fetchone()[0] == 0
            # A genuine data difference on a compared column must still trip it.
            probe = next(column for column in compared if column not in {"month", "cusip_id"})
            con.execute(f'UPDATE candidate SET "{probe}" = ?', ["tampered"])
            assert con.execute(gate).fetchone()[0] == 1
        finally:
            con.close()

def test_unit_repair_copy_marker_is_nested_under_unit_repair_key(tmp_path: Path) -> None:
    """Row markers must nest under ``unit_repair`` exactly like the prepare shape.

    The copy gate and the prepare both assert ``@> jsonb_build_object('unit_repair',
    <marker>)``; a flat append would never satisfy them.
    """
    plan = _unit_repair_plan(tmp_path)
    marker = f"jsonb_build_object('unit_repair', {backfill._sql_json(backfill._unit_repair_marker(plan))}::jsonb)"
    for surface in backfill.UNIT_REPAIR_AFFECTED_SURFACES:
        sql = backfill.render_unit_repair_copy_sql(plan, surface)
        shapes = _copy_projection_shapes(sql)
        for column in ("source_lineage", "payload"):
            assert shapes[column] == f"source.{column} || {marker}"
        assert "|| '{" not in sql
    for surface in ("returns", "rating_pit"):
        sql = backfill.render_unit_repair_copy_sql(plan, surface)
        shapes = _copy_projection_shapes(sql)
        for column in ("source_lineage", "payload"):
            if column in shapes:
                assert shapes[column] == f"source.{column}"


def test_unit_repair_copy_marker_ast_shape(tmp_path: Path) -> None:
    """Parse-level check: the marker projection is ``col || jsonb_build_object('unit_repair', ...)``."""
    sqlglot = pytest.importorskip("sqlglot")
    plan = _unit_repair_plan(tmp_path)
    for surface in backfill.UNIT_REPAIR_AFFECTED_SURFACES:
        sql = backfill.render_unit_repair_copy_sql(plan, surface)
        insert_sql = sql[sql.index("INSERT INTO"): sql.index("DO NOTHING;") + len("DO NOTHING;")]
        statement = sqlglot.parse_one(insert_sql, read="postgres")
        assert isinstance(statement, sqlglot.exp.Insert)
        select = statement.expression
        assert isinstance(select, sqlglot.exp.Select)
        shapes = dict(zip(_copy_projection_shapes(sql), select.expressions))
        for column in ("source_lineage", "payload"):
            projection = shapes[column]
            assert isinstance(projection, sqlglot.exp.DPipe)
            assert isinstance(projection.this, sqlglot.exp.Column)
            assert projection.this.name == column
            builder = projection.expression
            assert isinstance(builder, sqlglot.exp.Anonymous)
            assert builder.name.lower() == "jsonb_build_object"
            key, value = builder.expressions
            assert isinstance(key, sqlglot.exp.Literal) and key.this == "unit_repair"
            literal = value.this if isinstance(value, sqlglot.exp.Cast) else value
            assert json.loads(literal.this) == backfill._unit_repair_marker(plan)


# --- §14 gate-equivalence harness (DuckDB) -----------------------------------
# The §14 amendment replaces set operations and all-history scans with small
# monthly summaries plus month-bounded, publication-pinned anti-joins.  These
# tests execute OLD and NEW gate shapes side by side on adversarial fixtures and
# require identical verdicts.  No database server is required (DuckDB).

_UNIT_REPAIR_FIXTURE_MONTHS = ("2002-08-01", "2002-09-01", "2002-10-01", "2002-11-01")
_UNIT_REPAIR_FIXTURE_FIRST = "2002-08-01"
_UNIT_REPAIR_FIXTURE_LAST = "2002-11-01"
_UNIT_REPAIR_FIXTURE_CHILD = "65156481-8cb4-52b5-8676-cf77edc5644f"
_UNIT_REPAIR_FIXTURE_CUTOFF = "2026-06-01"
_UNIT_REPAIR_FIXTURE_MONTH_COUNT = 4

_IDENTITY_FAIL = (
    "{alias}.distribution_rule IS NULL"
    " OR {alias}.reference_cusip9 IS NULL OR trim({alias}.reference_cusip9) = ''"
    " OR ({alias}.distribution_rule = 'rule_144a' AND"
    " ({alias}.cusip_id <> {alias}.reference_cusip9 OR {alias}.distribution_decision_id IS NOT NULL))"
    " OR ({alias}.distribution_rule = 'reg_s' AND nullif({alias}.distribution_decision_id, '') IS NULL)"
)
_BOOTSTRAP_OK = (
    "{alias}.distribution_rule = 'rule_144a' AND {alias}.cusip_id = {alias}.reference_cusip9"
    " AND {alias}.distribution_decision_id IS NULL"
)


def _identity(cusip: str) -> dict[str, object]:
    return {"distribution_rule": "rule_144a", "reference_cusip9": cusip, "distribution_decision_id": None}


def _baseline_fixture() -> dict[str, list[dict[str, object]]]:
    rows: dict[str, list[dict[str, object]]] = {"snapshot": [], "rv_signal": [], "returns": [], "rating_pit": []}
    for month in _UNIT_REPAIR_FIXTURE_MONTHS:
        for cusip, volume in (("AAA000001", "10.5"), ("BBB000002", "20.25")):
            rows["snapshot"].append({"month": month, "cusip_id": cusip, "eligibility_state": "included", "dollar_volume": volume, **_identity(cusip)})
            rows["rv_signal"].append({"month": month, "cusip_id": cusip, "dollar_volume": volume, **_identity(cusip)})
            rows["returns"].append({"month": month, "cusip_id": cusip, **_identity(cusip)})
            rows["rating_pit"].append({"month": month, "cusip_id": cusip, **_identity(cusip)})
    return rows


def _duckdb_fixture(rows: dict[str, list[dict[str, object]]]):
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE snapshot (publication_id VARCHAR, month DATE, cusip_id VARCHAR, eligibility_state VARCHAR,"
        " distribution_rule VARCHAR, reference_cusip9 VARCHAR, distribution_decision_id VARCHAR, dollar_volume DECIMAL(38,6))"
    )
    con.execute(
        "CREATE TABLE rv_signal (publication_id VARCHAR, month DATE, cusip_id VARCHAR,"
        " distribution_rule VARCHAR, reference_cusip9 VARCHAR, distribution_decision_id VARCHAR, dollar_volume DECIMAL(38,6))"
    )
    for table in ("returns", "rating_pit"):
        con.execute(
            f"CREATE TABLE {table} (publication_id VARCHAR, month DATE, cusip_id VARCHAR,"
            " distribution_rule VARCHAR, reference_cusip9 VARCHAR, distribution_decision_id VARCHAR)"
        )
    columns = {
        "snapshot": ("publication_id", "month", "cusip_id", "eligibility_state", "distribution_rule", "reference_cusip9", "distribution_decision_id", "dollar_volume"),
        "rv_signal": ("publication_id", "month", "cusip_id", "distribution_rule", "reference_cusip9", "distribution_decision_id", "dollar_volume"),
        "returns": ("publication_id", "month", "cusip_id", "distribution_rule", "reference_cusip9", "distribution_decision_id"),
        "rating_pit": ("publication_id", "month", "cusip_id", "distribution_rule", "reference_cusip9", "distribution_decision_id"),
    }
    for surface, table_rows in rows.items():
        names = columns[surface]
        placeholders = ", ".join("?" for _ in names)
        payload = []
        for row in table_rows:
            values = []
            for name in names:
                value = row.get(name)
                if name == "publication_id" and value is None:
                    value = _UNIT_REPAIR_FIXTURE_CHILD
                values.append(value)
            payload.append(tuple(values))
        if not payload:
            continue
        con.executemany(f"INSERT INTO {surface} ({', '.join(names)}) VALUES ({placeholders})", payload)
    return con


def _counts_verdicts(con, expected_counts: dict[str, int]) -> tuple[bool, bool]:
    child = _UNIT_REPAIR_FIXTURE_CHILD
    old_fail = False
    for surface, expected in expected_counts.items():
        actual = con.execute(f"SELECT count(*) FROM {surface} WHERE publication_id = ?", [child]).fetchone()[0]
        if actual != expected:
            old_fail = True
    new_fail = False
    for surface, expected in expected_counts.items():
        actual = con.execute(
            f"SELECT coalesce(sum(monthly.rows), 0) FROM (SELECT count(*) AS rows FROM {surface}"
            " WHERE publication_id = ? GROUP BY month) monthly",
            [child],
        ).fetchone()[0]
        if actual != expected:
            new_fail = True
    return old_fail, new_fail


def _bootstrap_verdicts(con) -> tuple[bool, bool]:
    child = _UNIT_REPAIR_FIXTURE_CHILD
    old_missing = con.execute(
        "SELECT count(*) FROM snapshot f WHERE f.publication_id = ? AND ("
        + _BOOTSTRAP_OK.format(alias="f")
        + ")",
        [child],
    ).fetchone()[0] == 0
    new_rows = con.execute(
        "SELECT bool_or(" + _BOOTSTRAP_OK.format(alias="f") + ") AS bootstrap FROM snapshot f"
        " WHERE f.publication_id = ? GROUP BY f.month",
        [child],
    ).fetchall()
    new_missing = not any(row[0] is True for row in new_rows)
    return old_missing, new_missing


def _coverage_verdicts(con) -> tuple[bool, bool]:
    child = _UNIT_REPAIR_FIXTURE_CHILD
    directions = (
        ("rv_signal", "SELECT month, cusip_id FROM rv_signal WHERE publication_id = ?", "SELECT month, cusip_id FROM snapshot WHERE publication_id = ? AND eligibility_state = 'included'"),
        ("returns", "SELECT month, cusip_id FROM returns WHERE publication_id = ?", "SELECT month, cusip_id FROM snapshot WHERE publication_id = ?"),
        ("snapshot", "SELECT month, cusip_id FROM snapshot WHERE publication_id = ?", "SELECT month, cusip_id FROM rating_pit WHERE publication_id = ?"),
        ("rating_pit", "SELECT month, cusip_id FROM rating_pit WHERE publication_id = ?", "SELECT month, cusip_id FROM snapshot WHERE publication_id = ?"),
    )
    old_fail = False
    for _surface, forward_sql, probe_sql in directions:
        row = con.execute(f"SELECT 1 FROM ({forward_sql} EXCEPT {probe_sql}) differences LIMIT 1", [child, child]).fetchone()
        if row:
            old_fail = True
            break
    months = [row[0] for row in con.execute(
        "SELECT DISTINCT month FROM (SELECT month FROM snapshot WHERE publication_id = ? "
        "UNION SELECT month FROM rv_signal WHERE publication_id = ? "
        "UNION SELECT month FROM returns WHERE publication_id = ? "
        "UNION SELECT month FROM rating_pit WHERE publication_id = ?) months ORDER BY month",
        [child] * 4,
    ).fetchall()]
    new_fail = False
    for month in months:
        for forward_table, probe_table, probe_extra in (
            ("rv_signal", "snapshot", " AND s.eligibility_state = 'included'"),
            ("returns", "snapshot", ""),
            ("snapshot", "rating_pit", ""),
            ("rating_pit", "snapshot", ""),
        ):
            row = con.execute(
                f"SELECT 1 FROM {forward_table} f WHERE f.publication_id = ? AND f.month = ?"
                f" AND NOT EXISTS (SELECT 1 FROM {probe_table} s WHERE s.publication_id = ? AND s.month = ?"
                f" AND s.month = f.month AND s.cusip_id = f.cusip_id{probe_extra}) LIMIT 1",
                [child, month, child, month],
            ).fetchone()
            if row:
                new_fail = True
                break
        if new_fail:
            break
    return old_fail, new_fail


def _continuity_verdicts(con) -> tuple[bool, bool]:
    child = _UNIT_REPAIR_FIXTURE_CHILD
    old_min = con.execute("SELECT min(month) FROM returns WHERE publication_id = ?", [child]).fetchone()[0]
    old_fail = str(old_min) != _UNIT_REPAIR_FIXTURE_FIRST
    if not old_fail:
        gaps = con.execute(
            "SELECT count(*) FROM generate_series(CAST(? AS DATE), CAST(? AS DATE), INTERVAL '1 month') expected(month)"
            " LEFT JOIN (SELECT DISTINCT month FROM returns WHERE publication_id = ?) actual ON actual.month = CAST(expected.month AS DATE)"
            " WHERE actual.month IS NULL",
            [_UNIT_REPAIR_FIXTURE_FIRST, _UNIT_REPAIR_FIXTURE_LAST, child],
        ).fetchone()[0]
        old_fail = gaps > 0
    new_min = con.execute("SELECT min(month) FROM returns WHERE publication_id = ?", [child]).fetchone()[0]
    new_fail = str(new_min) != _UNIT_REPAIR_FIXTURE_FIRST
    if not new_fail:
        months = con.execute(
            "SELECT count(*) FROM (SELECT DISTINCT month FROM returns WHERE publication_id = ?) returns_months"
            " WHERE month BETWEEN CAST(? AS DATE) AND CAST(? AS DATE) AND month = date_trunc('month', month)::date",
            [child, _UNIT_REPAIR_FIXTURE_FIRST, _UNIT_REPAIR_FIXTURE_LAST],
        ).fetchone()[0]
        new_fail = months != _UNIT_REPAIR_FIXTURE_MONTH_COUNT
    return old_fail, new_fail


def _identity_verdicts(con) -> tuple[bool, bool]:
    child = _UNIT_REPAIR_FIXTURE_CHILD
    old_fail = False
    for table in ("snapshot", "rv_signal", "returns", "rating_pit"):
        row = con.execute(
            f"SELECT 1 FROM {table} f WHERE f.publication_id = ? AND ({_IDENTITY_FAIL.format(alias='f')}) LIMIT 1",
            [child],
        ).fetchone()
        if row:
            old_fail = True
            break
    stats = con.execute(
        "SELECT f.surface, bool_or(" + _IDENTITY_FAIL.format(alias="f") + ") AS bad FROM ("
        " SELECT 'snapshot' AS surface, publication_id, month, cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM snapshot"
        " UNION ALL SELECT 'rv_signal', publication_id, month, cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM rv_signal"
        " UNION ALL SELECT 'returns', publication_id, month, cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM returns"
        " UNION ALL SELECT 'rating_pit', publication_id, month, cusip_id, distribution_rule, reference_cusip9, distribution_decision_id FROM rating_pit"
        ") f WHERE f.publication_id = ? GROUP BY f.surface",
        [child],
    ).fetchall()
    new_fail = any(row[1] is True for row in stats)
    return old_fail, new_fail


def _cross_identity_verdicts(con) -> tuple[bool, bool]:
    child = _UNIT_REPAIR_FIXTURE_CHILD
    comparison = (
        "ROW(f.distribution_rule, f.reference_cusip9, f.distribution_decision_id)"
        " IS DISTINCT FROM ROW(s.distribution_rule, s.reference_cusip9, s.distribution_decision_id)"
    )
    old_fail = False
    for surface in ("rv_signal", "returns", "rating_pit"):
        row = con.execute(
            f"SELECT 1 FROM {surface} f JOIN snapshot s ON s.month = f.month AND s.cusip_id = f.cusip_id"
            f" AND s.publication_id = ? WHERE f.publication_id = ? AND {comparison} LIMIT 1",
            [child, child],
        ).fetchone()
        if row:
            old_fail = True
            break
    months = [row[0] for row in con.execute(
        "SELECT DISTINCT month FROM (SELECT month FROM snapshot WHERE publication_id = ? "
        "UNION SELECT month FROM rv_signal WHERE publication_id = ? "
        "UNION SELECT month FROM returns WHERE publication_id = ? "
        "UNION SELECT month FROM rating_pit WHERE publication_id = ?) months ORDER BY month",
        [child] * 4,
    ).fetchall()]
    new_fail = False
    for month in months:
        for surface in ("rv_signal", "returns", "rating_pit"):
            row = con.execute(
                f"SELECT 1 FROM {surface} f JOIN snapshot s ON s.month = f.month AND s.cusip_id = f.cusip_id"
                f" AND s.publication_id = ? AND s.month = ? WHERE f.publication_id = ? AND f.month = ?"
                f" AND {comparison} LIMIT 1",
                [child, month, child, month],
            ).fetchone()
            if row:
                new_fail = True
                break
        if new_fail:
            break
    return old_fail, new_fail


def _annual_expected(con) -> dict[tuple[str, int], dict[str, object]]:
    """Expected annual map derived from the fixture (the artifact-side input)."""
    child = _UNIT_REPAIR_FIXTURE_CHILD
    result: dict[tuple[str, int], dict[str, object]] = {}
    for surface in ("snapshot", "rv_signal"):
        for row in con.execute(
            f"SELECT extract(year FROM month)::int, count(*),"
            " count(*) FILTER (WHERE dollar_volume IS NULL), CAST(sum(dollar_volume) AS DECIMAL(38,6))"
            f" FROM {surface} WHERE publication_id = ? AND month <= CAST(? AS DATE) GROUP BY 1",
            [child, _UNIT_REPAIR_FIXTURE_CUTOFF],
        ).fetchall():
            result[(surface, int(row[0]))] = {"rows": row[1], "nulls": row[2], "sum_dollar_volume": row[3]}
    for surface in ("returns", "rating_pit"):
        for row in con.execute(
            f"SELECT extract(year FROM month)::int, count(*) FROM {surface}"
            " WHERE publication_id = ? AND month <= CAST(? AS DATE) GROUP BY 1",
            [child, _UNIT_REPAIR_FIXTURE_CUTOFF],
        ).fetchall():
            result[(surface, int(row[0]))] = {"rows": row[1], "nulls": None, "sum_dollar_volume": None}
    return result


def _old_annual_actual(con) -> list[tuple]:
    """The pre-amendment direct per-year grouping (no monthly intermediate)."""
    child = _UNIT_REPAIR_FIXTURE_CHILD
    cutoff = _UNIT_REPAIR_FIXTURE_CUTOFF
    rows: list[tuple] = []
    for surface in ("snapshot", "rv_signal"):
        rows.extend(con.execute(
            f"SELECT '{surface}', extract(year FROM month)::int, count(*),"
            " count(*) FILTER (WHERE dollar_volume IS NULL), CAST(sum(dollar_volume) AS DECIMAL(38,6))"
            f" FROM {surface} WHERE publication_id = ? AND month <= CAST(? AS DATE) GROUP BY 2",
            [child, cutoff],
        ).fetchall())
    for surface in ("returns", "rating_pit"):
        rows.extend(con.execute(
            f"SELECT '{surface}', extract(year FROM month)::int, count(*), CAST(NULL AS BIGINT), CAST(NULL AS DECIMAL(38,6))"
            f" FROM {surface} WHERE publication_id = ? AND month <= CAST(? AS DATE) GROUP BY 2",
            [child, cutoff],
        ).fetchall())
    return rows


def _annual_rollup_verdict(con, expected: dict[tuple[str, int], dict[str, object]], *, rounded: bool = False) -> tuple[bool, bool]:
    """Return (old_fail, new_fail) for the annual artifact gate."""
    child = _UNIT_REPAIR_FIXTURE_CHILD
    actual = _old_annual_actual(con)
    monthly_expression = "CAST(round(volume_sum) AS DECIMAL(38,6))" if rounded else "volume_sum"
    rollup = con.execute(
        "SELECT surface, extract(year FROM month)::int AS year, sum(rows) AS rows, sum(nulls) AS nulls,"
        f" CAST(sum({monthly_expression}) AS DECIMAL(38,6)) AS sum_dollar_volume"
        " FROM (SELECT 'snapshot' AS surface, month, count(*) AS rows,"
        " count(*) FILTER (WHERE dollar_volume IS NULL) AS nulls, sum(dollar_volume) AS volume_sum FROM snapshot WHERE publication_id = ? GROUP BY month"
        " UNION ALL SELECT 'rv_signal', month, count(*), count(*) FILTER (WHERE dollar_volume IS NULL), sum(dollar_volume) FROM rv_signal WHERE publication_id = ? GROUP BY month"
        " UNION ALL SELECT 'returns', month, count(*), CAST(NULL AS BIGINT), CAST(NULL AS DECIMAL(38,6)) FROM returns WHERE publication_id = ? GROUP BY month"
        " UNION ALL SELECT 'rating_pit', month, count(*), CAST(NULL AS BIGINT), CAST(NULL AS DECIMAL(38,6)) FROM rating_pit WHERE publication_id = ? GROUP BY month)"
        " WHERE month <= CAST(? AS DATE) GROUP BY surface, extract(year FROM month)::int",
        [child, child, child, child, _UNIT_REPAIR_FIXTURE_CUTOFF],
    ).fetchall()

    def mismatch(rows) -> bool:
        actual_map = {(row[0], int(row[1])): {"rows": row[2], "nulls": row[3], "sum_dollar_volume": row[4]} for row in rows}
        for key in set(expected) | set(actual_map):
            want, got = expected.get(key), actual_map.get(key)
            if want is None or got is None:
                return True
            if want["rows"] != got["rows"] or want["nulls"] != got["nulls"]:
                return True
            if (want["sum_dollar_volume"] is None) != (got["sum_dollar_volume"] is None):
                return True
            if want["sum_dollar_volume"] is not None and abs(want["sum_dollar_volume"] - got["sum_dollar_volume"]) > 1:
                return True
        return False

    return mismatch(actual), mismatch(rollup)


def test_unit_repair_finalize_has_no_except_set_operations(tmp_path: Path) -> None:
    sql = backfill.render_unit_repair_finalize_sql(_unit_repair_plan(tmp_path))
    assert re.search(r"\bEXCEPT\b(?!ION)", sql) is None
    assert "RETURNING" not in sql


def test_unit_repair_finalize_antijoins_pin_child_and_month_both_sides(tmp_path: Path) -> None:
    plan = _unit_repair_plan(tmp_path)
    sql = backfill.render_unit_repair_finalize_sql(plan)
    child = plan.publication_id
    start = sql.index("FOR v_month IN SELECT DISTINCT month FROM pg_temp.unit_repair_month_stats ORDER BY month LOOP")
    section = sql[start: sql.index("END LOOP;", start)]
    assert f"f.publication_id = '{child}'::uuid" in section
    assert section.count(f"f.publication_id = '{child}'::uuid") == 7
    assert section.count(f"s.publication_id = '{child}'::uuid") == 7
    assert section.count("AND f.month = v_month") == 7
    assert section.count("s.month = v_month") == 7
    assert "s.eligibility_state = 'included'" in section
    # both rating directions are explicit
    assert "SELECT 1 FROM bond_panel_snapshot f" in section and "SELECT 1 FROM bond_panel_rating_pit s" in section
    assert "SELECT 1 FROM bond_panel_rating_pit f" in section and "SELECT 1 FROM bond_panel_snapshot s" in section
    assert section.count("unit repair rating coverage mismatch") == 2
    for message in (
        "unit repair rv_signal coverage mismatch",
        "unit repair returns coverage mismatch",
        "unit repair cross-surface identity mismatch:rv_signal",
        "unit repair cross-surface identity mismatch:returns",
        "unit repair cross-surface identity mismatch:rating_pit",
    ):
        assert message in section
    assert "LIMIT 1" in section


def test_unit_repair_finalize_monthly_summaries_reused_once_per_surface(tmp_path: Path) -> None:
    plan = _unit_repair_plan(tmp_path)
    sql = backfill.render_unit_repair_finalize_sql(plan)
    child = plan.publication_id
    assert "CREATE TEMP TABLE unit_repair_month_stats (" in sql
    assert "PRIMARY KEY (surface, month)" in sql
    assert sql.count("INSERT INTO pg_temp.unit_repair_month_stats (surface, month, rows, nulls, volume_sum, bad_identity, bootstrap)") == 4
    for surface in backfill.SURFACES:
        assert f"SELECT '{surface}', f.month, count(*)," in sql
    assert sql.count(f"WHERE f.publication_id = '{child}'::uuid\nGROUP BY f.month;") == 4
    # Counts come from the small summary, never from raw-table rescans.
    for surface in backfill.SURFACES:
        assert f" FROM pg_temp.unit_repair_month_stats WHERE surface = '{surface}';" in sql
        assert f"(SELECT count(*) FROM {backfill._TABLES[surface]} WHERE publication_id = '{child}'::uuid) <> {plan.counts[surface]}" not in sql
    assert "generate_series(" not in sql
    assert "date_trunc('month', month)::date" in sql
    assert "IF v_months <> 289 THEN" in sql
    assert "IF v_returns_min IS DISTINCT FROM '2002-08-01'::date THEN" in sql
    for surface in backfill.SURFACES:
        assert f"WHERE surface = '{surface}' AND bad_identity IS TRUE LIMIT 1" in sql
    assert "WHERE surface = 'snapshot' AND bootstrap IS TRUE LIMIT 1" in sql


def test_unit_repair_finalize_annual_gate_shape_and_rounding(tmp_path: Path) -> None:
    plan = _unit_repair_plan(tmp_path)
    sql = backfill.render_unit_repair_finalize_sql(plan)
    assert backfill._unit_repair_expected_aggregates_json(plan) in sql
    assert sql.count("CREATE TEMP TABLE unit_repair_expected_year (") == 1
    assert sql.count("CREATE TEMP TABLE unit_repair_actual_year (") == 1
    assert "jsonb_to_recordset(" in sql and "AS expected(surface text, \"year\" int, rows bigint, \"nulls\" bigint, sum_dollar_volume numeric)" in sql
    assert "CAST(sum(volume_sum) AS numeric(38,6))" in sql
    assert "sum(volume_sum::numeric(38,6))" not in sql
    assert "round(volume_sum" not in sql
    assert "coalesce(sum(volume_sum)" not in sql
    assert "FULL JOIN pg_temp.unit_repair_actual_year a USING (surface, year)" in sql
    assert "WHERE e.rows IS DISTINCT FROM a.rows" in sql
    assert "OR e.nulls IS DISTINCT FROM a.nulls" in sql
    assert "OR (e.sum_dollar_volume IS NULL) <> (a.sum_dollar_volume IS NULL)" in sql
    assert "OR (e.sum_dollar_volume IS NOT NULL AND abs(e.sum_dollar_volume - a.sum_dollar_volume) > 1)" in sql
    assert f"WHERE month <= '{backfill.UNIT_REPAIR_ARTIFACT_CUTOFF}'::date" in sql


def test_unit_repair_finalize_single_timed_transaction_and_locks(tmp_path: Path) -> None:
    plan = _unit_repair_plan(tmp_path)
    sql = backfill.render_unit_repair_finalize_sql(plan)
    child = plan.publication_id
    head = backfill.UNIT_REPAIR_FROM_HEAD_PUBLICATION_ID
    ordered = (
        "BEGIN;",
        "SET LOCAL ROLE worker_writer;",
        f"SET LOCAL lock_timeout = '{backfill.UNIT_REPAIR_FINALIZE_LOCK_TIMEOUT}';",
        f"SET LOCAL statement_timeout = '{backfill.UNIT_REPAIR_FINALIZE_STATEMENT_TIMEOUT}';",
        "LOCK TABLE bond_panel_snapshot, bond_panel_rv_signal, bond_panel_returns, bond_panel_rating_pit IN SHARE MODE;",
        "DO $unit_repair_finalize$",
        "PERFORM 1 FROM bond_panel_publications WHERE publication_id = '" + child + "'::uuid FOR UPDATE;",
        "PERFORM 1 FROM bond_panel_app_pointer WHERE product = 'bond_panel_v1' FOR UPDATE;",
        f"publication_id IN ('{head}'::uuid, '{child}'::uuid)",
        "CREATE TEMP TABLE unit_repair_month_stats (",
        "unit repair artifact/DB aggregate mismatch",
        "UPDATE bond_panel_publications",
        "GET DIAGNOSTICS v_status_rows = ROW_COUNT;",
        "UPDATE bond_panel_app_pointer",
        "GET DIAGNOSTICS v_cas_rows = ROW_COUNT;",
        "$unit_repair_finalize$;",
        "COMMIT;",
        "unit_repair_finalize_phase=refresh_start",
        f"SET statement_timeout = '{backfill.UNIT_REPAIR_FINALIZE_REFRESH_STATEMENT_TIMEOUT}';",
        "REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rv_signal_v1_mat;",
        "REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_returns_v1_mat;",
        "REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rating_pit_v1_mat;",
        "REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_snapshot_v1_mat;",
        "RESET statement_timeout;",
        "RESET ROLE;",
        "unit_repair_finalize_phase=refresh_done",
    )
    positions = [sql.index(fragment) for fragment in ordered]
    assert positions == sorted(positions)
    assert sql.count("DO $") == 1
    assert "NOT IN (" not in sql
    assert "unit repair finalize requires the expected head pointer or this child" in sql
    assert "unit repair finalize cannot point at an unvalidated child" in sql
    assert "unit repair finalize requires one prepared-to-validated transition or an identical validated child" in sql
    assert "unit repair pointer compare-and-swap lost" in sql
    assert f"WHERE product = 'bond_panel_v1' AND publication_id = '{head}'::uuid" in sql
    assert "clock_timestamp()" in sql
    assert "now()" in sql  # validated_at/changed_at semantics unchanged


def test_unit_repair_finalize_gate_equivalence_on_adversarial_fixtures() -> None:
    def drop_returns_month(rows, month):
        rows["returns"] = [row for row in rows["returns"] if row["month"] != month]

    cases = []

    def case(name, mutate=None, expect=None):
        cases.append((name, mutate, expect or {}))

    case("valid")
    case("missing_middle_returns_month", lambda rows: drop_returns_month(rows, "2002-10-01"), {"continuity": True, "counts": True})
    case("missing_last_returns_month", lambda rows: drop_returns_month(rows, "2002-11-01"), {"continuity": True, "counts": True})
    case(
        "earlier_return_date",
        lambda rows: rows["returns"].append({"month": "2002-07-01", "cusip_id": "AAA000001", **_identity("AAA000001")}),
        {"continuity": True, "counts": True},
    )
    case(
        "midmonth_date_masks_gap",
        lambda rows: drop_returns_month(rows, "2002-10-01") or rows["returns"].append(
            {"month": "2002-10-15", "cusip_id": "AAA000001", **_identity("AAA000001")}
        ),
        {"continuity": True},
    )
    case(
        "extra_later_date",
        lambda rows: rows["returns"].append({"month": "2002-11-15", "cusip_id": "AAA000001", **_identity("AAA000001")}),
        {"continuity": False},
    )
    case("empty_returns_surface", lambda rows: rows.__setitem__("returns", []), {"counts": True, "continuity": True})
    case(
        "orphan_rv_key",
        lambda rows: rows["rv_signal"].append({"month": "2002-11-01", "cusip_id": "ZZZ999999", "dollar_volume": "1.0", **_identity("ZZZ999999")}),
        {"coverage": True},
    )
    case(
        "rv_matches_excluded_snapshot",
        lambda rows: [row.update(eligibility_state="excluded") for row in rows["snapshot"] if row["cusip_id"] == "BBB000002"],
        {"coverage": True},
    )
    case(
        "rating_missing_key",
        lambda rows: rows.__setitem__("rating_pit", [row for row in rows["rating_pit"] if not (row["month"] == "2002-09-01" and row["cusip_id"] == "AAA000001")]),
        {"coverage": True, "counts": True},
    )
    case(
        "rating_extra_key",
        lambda rows: rows["rating_pit"].append({"month": "2002-09-01", "cusip_id": "ZZZ999999", **_identity("ZZZ999999")}),
        {"coverage": True, "counts": True},
    )
    case(
        "identity_invalid_rule_144a_reference",
        lambda rows: [row.update(reference_cusip9="XXX999999") for row in rows["snapshot"] if row["month"] == "2002-09-01" and row["cusip_id"] == "AAA000001"],
        {"identity": True, "cross": True},
    )
    case(
        "identity_invalid_reg_s_without_decision",
        lambda rows: [row.update(distribution_rule="reg_s", distribution_decision_id=None) for row in rows["snapshot"] if row["month"] == "2002-10-01" and row["cusip_id"] == "AAA000001"],
        {"identity": True, "cross": True},
    )
    case(
        "identity_invalid_null_reference",
        lambda rows: [row.update(reference_cusip9=None) for row in rows["returns"] if row["month"] == "2002-09-01" and row["cusip_id"] == "AAA000001"],
        {"identity": True},
    )
    case(
        "identity_invalid_blank_reference",
        lambda rows: [row.update(reference_cusip9="   ") for row in rows["rating_pit"] if row["month"] == "2002-09-01" and row["cusip_id"] == "AAA000001"],
        {"identity": True},
    )
    case(
        "cross_surface_null_vs_value_identity",
        lambda rows: [row.update(distribution_decision_id="decision-1") for row in rows["rv_signal"] if row["month"] == "2002-10-01"],
        {"cross": True},
    )
    case(
        "bootstrap_absent",
        lambda rows: [
            row.update(distribution_rule="reg_s", distribution_decision_id="decision-1", reference_cusip9=row["cusip_id"])
            for row in rows["snapshot"]
        ],
        {"bootstrap": True},
    )

    for name, mutate, expect in cases:
        rows = _baseline_fixture()
        baseline_counts = {surface: len(surface_rows) for surface, surface_rows in rows.items()}
        if mutate:
            mutate(rows)
        con = _duckdb_fixture(rows)
        try:
            coverage_old, coverage_new = _coverage_verdicts(con)
            continuity_old, continuity_new = _continuity_verdicts(con)
            identity_old, identity_new = _identity_verdicts(con)
            cross_old, cross_new = _cross_identity_verdicts(con)
            counts_old, counts_new = _counts_verdicts(con, baseline_counts)
            bootstrap_old, bootstrap_new = _bootstrap_verdicts(con)
            expected_map = _annual_expected(con)
            annual_old, annual_new = _annual_rollup_verdict(con, expected_map)
            verdicts_old = {
                "coverage": coverage_old, "continuity": continuity_old, "identity": identity_old,
                "cross": cross_old, "counts": counts_old, "bootstrap": bootstrap_old,
            }
            verdicts_new = {
                "coverage": coverage_new, "continuity": continuity_new, "identity": identity_new,
                "cross": cross_new, "counts": counts_new, "bootstrap": bootstrap_new,
            }
            assert verdicts_old == verdicts_new, f"{name}: old={verdicts_old} new={verdicts_new}"
            assert annual_old == annual_new, f"{name}: annual old={annual_old} new={annual_new}"
            for gate, expected_fail in expect.items():
                observed = verdicts_new.get(gate, annual_new if gate == "annual" else None)
                assert observed is expected_fail, f"{name}: gate {gate} expected {expected_fail} got {observed}"
        finally:
            con.close()


def test_unit_repair_finalize_annual_gate_drift_and_tolerance() -> None:
    con = _duckdb_fixture(_baseline_fixture())
    try:
        expected = _annual_expected(con)
        old_fail, new_fail = _annual_rollup_verdict(con, expected)
        assert (old_fail, new_fail) == (False, False)

        missing_year = dict(expected)
        missing_year[("snapshot", 2003)] = {"rows": 2, "nulls": 0, "sum_dollar_volume": decimal.Decimal("21.0")}
        assert _annual_rollup_verdict(con, missing_year) == (True, True)

        extra_year = dict(expected)
        for key in [key for key in extra_year if key[1] == 2002]:
            del extra_year[key]
        assert _annual_rollup_verdict(con, extra_year) == (True, True)

        row_drift = {key: dict(value) for key, value in expected.items()}
        snapshot_2002 = row_drift[("snapshot", 2002)]
        snapshot_2002["rows"] = snapshot_2002["rows"] + 1
        assert _annual_rollup_verdict(con, row_drift) == (True, True)

        null_drift = {key: dict(value) for key, value in expected.items()}
        null_drift[("snapshot", 2002)]["nulls"] = 1
        assert _annual_rollup_verdict(con, null_drift) == (True, True)

        exactly_one = {key: dict(value) for key, value in expected.items()}
        exactly_one[("snapshot", 2002)]["sum_dollar_volume"] += decimal.Decimal(1)
        assert _annual_rollup_verdict(con, exactly_one) == (False, False)

        over_one = {key: dict(value) for key, value in expected.items()}
        over_one[("snapshot", 2002)]["sum_dollar_volume"] += decimal.Decimal("1.01")
        assert _annual_rollup_verdict(con, over_one) == (True, True)

        all_null = _baseline_fixture()
        for surface in ("snapshot", "rv_signal"):
            for row in all_null[surface]:
                row["dollar_volume"] = None
        null_con = _duckdb_fixture(all_null)
        try:
            null_expected = _annual_expected(null_con)
            assert all(value["sum_dollar_volume"] is None for value in null_expected.values())
            assert _annual_rollup_verdict(null_con, null_expected) == (False, False)
        finally:
            null_con.close()
    finally:
        con.close()


def test_unit_repair_finalize_annual_rollup_keeps_unrounded_monthly_sums() -> None:
    rows: dict[str, list[dict[str, object]]] = {"snapshot": [], "rv_signal": [], "returns": [], "rating_pit": []}
    for month in _UNIT_REPAIR_FIXTURE_MONTHS:
        rows["snapshot"].append({"month": month, "cusip_id": "AAA000001", "eligibility_state": "included", "dollar_volume": "0.6", **_identity("AAA000001")})
        rows["rv_signal"].append({"month": month, "cusip_id": "AAA000001", "dollar_volume": "0.6", **_identity("AAA000001")})
        rows["returns"].append({"month": month, "cusip_id": "AAA000001", **_identity("AAA000001")})
        rows["rating_pit"].append({"month": month, "cusip_id": "AAA000001", **_identity("AAA000001")})
    con = _duckdb_fixture(rows)
    try:
        expected = _annual_expected(con)
        old_fail, new_fail = _annual_rollup_verdict(con, expected)
        assert (old_fail, new_fail) == (False, False)
        rounded_old, rounded_new = _annual_rollup_verdict(con, expected, rounded=True)
        assert rounded_old is False  # the direct annual cast is unaffected by the flag
        assert rounded_new is True  # rounding the monthly partials is observable
        exact_sum = expected[("snapshot", 2002)]["sum_dollar_volume"]
        rounded_sum = con.execute(
            "SELECT CAST(sum(round(volume_sum)) AS DECIMAL(38,6)) FROM ("
            "SELECT month, sum(dollar_volume) AS volume_sum FROM snapshot WHERE publication_id = ? GROUP BY month) monthly",
            [_UNIT_REPAIR_FIXTURE_CHILD],
        ).fetchone()[0]
        assert abs(rounded_sum - exact_sum) > 1
    finally:
        con.close()
