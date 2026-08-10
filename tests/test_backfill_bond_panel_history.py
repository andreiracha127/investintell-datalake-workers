"""Contracts for the offline-only T3 historical panel publication emitter."""
from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pandas as pd
import pytest

from scripts import backfill_bond_panel_history as backfill


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), path)


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


def test_repair_sql_copies_only_old_publication_facts_and_cas_points_exact_source(tmp_path: Path) -> None:
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
    finalize = backfill.render_finalize_sql(plan)

    assert f"WHERE publication_id={backfill._sql_string(backfill.LEGACY_REPAIR_FROM_PUBLICATION_ID)}::uuid" in copied
    assert "INSERT INTO bond_panel_snapshot" in copied
    assert "bond_panel_live.parquet" not in copied
    for surface, copy_sql in copies.items():
        assert f"repair copy immutable evidence conflict:{surface}" in copy_sql
        for column in backfill._COLUMNS[surface][1:]:
            assert f"candidate.{column}" in copy_sql
            assert f"source.{column}" in copy_sql
    assert "COPY _backfill_stage" in tail
    assert "repair tail requires exact copied historical returns before tail" in tail
    assert "WHERE publication_id=" in finalize
    assert "AND publication_id=" in finalize
    assert "base_repair" in finalize
    assert backfill._sql_string(plan.returns_first_month) in finalize
    assert f"publication_id={backfill._sql_string(plan.publication_id)}::uuid" in finalize


def test_normal_mode_still_refuses_a_missing_return_tail(tmp_path: Path) -> None:
    directory = _artifact_dir(tmp_path)
    _write(directory / "bond_monthly_returns.parquet", [
        {"cusip_id": "AAA000001", "month": "2025-03-01", "total_return": .01, "price_return": .009, "carry_return": .001, "suspect": False},
    ])
    artifacts = backfill.ArtifactSet.open(directory, expected_hashes=_hashes(directory))

    with pytest.raises(backfill.PlanError, match="returns_history_must_reach_cutoff"):
        backfill.build_plan(artifacts)
