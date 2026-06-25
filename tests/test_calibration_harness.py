from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path

import pytest

from src import calibration_harness as ch

UTC = dt.timezone.utc


def _row(
    series_id: str,
    period: dt.date,
    vintage: dt.date,
    value: float,
    *,
    revision: int = 0,
) -> ch.VintageRow:
    return ch.VintageRow(
        series_id=series_id,
        observation_period=period,
        vintage_date=vintage,
        value=value,
        available_at=dt.datetime.combine(vintage, dt.time(0, 0), tzinfo=UTC),
        revision_number=revision,
        source_spec_version="test",
    )


def test_select_rows_as_of_ignores_future_revision() -> None:
    period = dt.date(2024, 1, 1)
    rows = [
        _row("PAYEMS", period, dt.date(2024, 2, 1), 100.0, revision=0),
        _row("PAYEMS", period, dt.date(2024, 3, 1), 900.0, revision=1),
    ]
    grouped = ch.group_rows(rows)
    selected = ch.select_rows_as_of(
        grouped,
        dt.datetime(2024, 2, 15, 12, tzinfo=UTC),
    )
    assert selected["PAYEMS"][period].value == 100.0
    assert selected["PAYEMS"][period].revision_number == 0


def test_select_rows_as_of_can_select_first_release_for_stability_replay() -> None:
    period = dt.date(2024, 1, 1)
    rows = [
        _row("INDPRO", period, dt.date(2024, 2, 1), 100.0, revision=0),
        _row("INDPRO", period, dt.date(2024, 3, 1), 101.0, revision=1),
    ]
    grouped = ch.group_rows(rows)
    cut = dt.datetime(2024, 3, 15, 12, tzinfo=UTC)
    latest = ch.select_rows_as_of(grouped, cut, mode="latest")
    first = ch.select_rows_as_of(grouped, cut, mode="first")
    assert latest["INDPRO"][period].value == 101.0
    assert first["INDPRO"][period].value == 100.0


def test_synthetic_future_revision_probe_passes() -> None:
    rows = [_row("ACOGNO", dt.date(2024, 1, 1), dt.date(2024, 2, 1), 10.0)]
    grouped = ch.group_rows(rows)
    qa = ch.compute_pit_qa(
        rows,
        grouped,
        calendar=[dt.date(2024, 2, 15), dt.date(2024, 2, 16)],
        repeat_rows=list(rows),
    )
    assert qa["unique_key_duplicate_count"] == 0
    assert qa["idempotent_repeat_read"] is True
    assert qa["synthetic_future_revision_no_effect"] is True


def test_v01_confidence_formula_combines_u_and_margins() -> None:
    g = ch.axis_margin(0.35, ch.GROWTH_ENTER, ch.AXIS_EXIT)
    i = ch.axis_margin(0.40, ch.INFLATION_ENTER, ch.AXIS_EXIT)
    u = 0.65
    confidence = 0.60 * u + 0.40 * math.sqrt(g * i)
    assert g == 1.0
    assert i == 1.0
    assert abs(confidence - 0.79) < 1e-12


def test_replay_carries_rows_when_inputs_do_not_change(monkeypatch) -> None:
    def fake_score_all_series(selected, cut):
        return {
            cfg.series_id: ch.SeriesScore(
                score=1.0,
                observation_period=dt.date(2020, 1, 1),
                vintage_date=dt.date(2020, 2, 1),
                available_at=dt.datetime(2020, 2, 1, tzinfo=UTC),
                revision_number=5,
                freshness=1.0,
                vintage_quality=1.0,
            )
            for cfg in ch.BASELINE_SERIES
        }

    monkeypatch.setattr(ch, "score_all_series", fake_score_all_series)
    rows = [
        _row(cfg.series_id, dt.date(2020, 1, 1), dt.date(2020, 2, 1), 100.0)
        for cfg in ch.BASELINE_SERIES
    ]
    replay = ch.replay_macro(
        rows,
        [dt.date(2020, 2, 3), dt.date(2020, 2, 4)],
    )
    assert replay[0]["reevaluated"] is True
    assert replay[1]["reevaluated"] is False
    assert replay[1]["inputs_changed"] is False
    assert replay[1]["published_quadrant"] == replay[0]["published_quadrant"]


def test_compare_macro_market_conditions_on_both_valid_dates() -> None:
    macro = [
        {
            "date": "2024-01-02",
            "status": "valid",
            "published_quadrant": "expansion",
            "candidate_quadrant": "expansion",
            "candidate_confidence": 0.80,
            "growth_sign": 1,
            "inflation_sign": 1,
        },
        {
            "date": "2024-01-03",
            "status": "valid",
            "published_quadrant": "expansion",
            "candidate_quadrant": "expansion",
            "candidate_confidence": 0.80,
            "growth_sign": 1,
            "inflation_sign": 1,
        },
    ]
    market = [
        {
            "date": "2024-01-02",
            "status": "valid",
            "quadrant": "expansion",
            "candidate_quadrant": "expansion",
            "growth_sign": 1,
            "inflation_sign": 1,
        },
        {
            "date": "2024-01-03",
            "status": "abstain",
            "quadrant": None,
            "candidate_quadrant": "expansion",
            "growth_sign": None,
            "inflation_sign": None,
        },
    ]
    rows, metrics = ch.compare_macro_market(macro, market, source="snapshot")
    assert len(rows) == 2
    assert metrics["both_valid_dates"] == 1
    assert metrics["exact_quadrant_agreement_rate"] == 1.0
    assert metrics["macro_valid_market_abstain_rate"] == 0.5


def test_parse_args_defaults_market_source_to_db_cagg(tmp_path) -> None:
    cfg = ch.parse_args([
        "--start-date",
        "2024-01-02",
        "--end-date",
        "2024-01-03",
        "--output-dir",
        str(tmp_path),
        "--data-snapshot-id",
        "snap",
        "--backend-commit",
        "backend",
        "--worker-commit",
        "worker",
    ])
    assert cfg.market_source == "db_cagg"
    assert cfg.input_cache_dir == Path(ch.DEFAULT_INPUT_CACHE_DIR)


def test_parse_args_can_disable_input_cache(tmp_path) -> None:
    cfg = ch.parse_args([
        "--start-date",
        "2024-01-02",
        "--end-date",
        "2024-01-03",
        "--output-dir",
        str(tmp_path),
        "--data-snapshot-id",
        "snap",
        "--backend-commit",
        "backend",
        "--worker-commit",
        "worker",
        "--no-input-cache",
    ])
    assert cfg.input_cache_dir is None


def test_parse_args_supports_offline_cache_key(tmp_path) -> None:
    cfg = ch.parse_args([
        "--start-date",
        "2024-01-02",
        "--end-date",
        "2024-01-03",
        "--output-dir",
        str(tmp_path),
        "--data-snapshot-id",
        "snap",
        "--backend-commit",
        "backend",
        "--worker-commit",
        "worker",
        "--offline",
        "--input-cache-key",
        "abc123",
    ])
    assert cfg.offline is True
    assert cfg.input_cache_key == "abc123"


def test_input_cache_final_key_depends_on_source_hashes(tmp_path) -> None:
    cfg = ch.HarnessConfig(
        start_date=dt.date(2024, 1, 2),
        end_date=dt.date(2024, 1, 3),
        output_dir=tmp_path / "out",
        data_snapshot_id="snap",
        backend_commit="backend",
        worker_commit="worker",
        input_cache_dir=tmp_path / "cache",
    )
    cut = ch.decision_time(cfg.end_date)
    first = ch.input_cache_key(cfg, cut, {"macro_vintages": "a", "market_levels": "b"})
    second = ch.input_cache_key(cfg, cut, {"macro_vintages": "a", "market_levels": "c"})
    assert first != second


def test_load_market_levels_from_cagg_nav_daily_resolves_ief_nav() -> None:
    class FakeCursor:
        sql = ""
        params = ()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            self.sql = sql
            self.params = params

        def fetchall(self):
            return [
                ("SPY", dt.date(2024, 1, 2), 470.0),
                ("IEF", dt.date(2024, 1, 2), 94.5),
                ("TIP", dt.date(2024, 1, 2), 107.2),
            ]

    class FakeConn:
        def __init__(self):
            self.fake_cursor = FakeCursor()

        def cursor(self):
            return self.fake_cursor

    conn = FakeConn()
    levels = ch.load_market_levels_from_cagg_nav_daily(
        conn,
        dt.date(2024, 1, 2),
        dt.date(2024, 1, 3),
    )

    assert "cagg_nav_daily" in conn.fake_cursor.sql
    assert "instruments_universe" in conn.fake_cursor.sql
    assert conn.fake_cursor.params[0] == ["SPY", "IEF", "TIP"]
    assert levels["IEF"][dt.date(2024, 1, 2)] == 94.5


def test_input_cache_hit_reuses_local_parquets_without_db(tmp_path, monkeypatch) -> None:
    calls = {"vintage": 0, "market": 0}
    vintage_rows = [
        _row("PAYEMS", dt.date(2024, 1, 1), dt.date(2024, 2, 1), 100.0)
    ]
    market_levels = {
        "SPY": {dt.date(2024, 1, 2): 470.0},
        "IEF": {dt.date(2024, 1, 2): 94.5},
        "TIP": {dt.date(2024, 1, 2): 107.2},
    }

    def fake_read_vintage_rows(conn, *, max_available_at):
        calls["vintage"] += 1
        return vintage_rows

    def fake_load_market_levels(conn, start_date, end_date):
        calls["market"] += 1
        return market_levels

    monkeypatch.setattr(ch, "read_vintage_rows", fake_read_vintage_rows)
    monkeypatch.setattr(
        ch,
        "load_market_levels_from_cagg_nav_daily",
        fake_load_market_levels,
    )
    cfg = ch.HarnessConfig(
        start_date=dt.date(2024, 1, 2),
        end_date=dt.date(2024, 1, 3),
        output_dir=tmp_path / "out",
        data_snapshot_id="snap",
        backend_commit="backend",
        worker_commit="worker",
        input_cache_dir=tmp_path / "cache",
    )
    max_available_at = ch.decision_time(cfg.end_date)

    first = ch.load_or_create_harness_inputs(object(), cfg, max_available_at=max_available_at)
    second = ch.load_or_create_harness_inputs(None, cfg, max_available_at=max_available_at)

    assert first.cache_metadata["cache_hit"] is False
    assert second.cache_metadata["cache_hit"] is True
    assert calls == {"vintage": 1, "market": 1}
    assert second.vintage_rows == vintage_rows
    assert second.market_levels == market_levels


def test_pit_selection_and_feature_panels_have_expected_grain() -> None:
    calendar = [dt.date(2024, 2, 15)]
    rows = [
        _row(cfg.series_id, dt.date(2024, 1, 1), dt.date(2024, 2, 1), 100.0)
        for cfg in ch.BASELINE_SERIES
    ]
    grouped = ch.group_rows(rows)

    pit = ch.build_pit_selection_panel(grouped, calendar)
    macro = ch.build_macro_feature_primitives(grouped, calendar)

    assert len(pit) == len(ch.BASELINE_SERIES)
    assert len(macro) == len(ch.BASELINE_SERIES) * 2
    assert {row["selection_mode"] for row in macro} == {"latest", "first_release"}
    assert all(row["coverage_flag"] for row in pit)


def test_v02_candidate_universe_excludes_market_derived_series() -> None:
    ids = {spec.series_id for spec in ch.V02_CHALLENGER_SERIES_SPECS}

    assert {"ICSA", "BUSAPPWNSAUS", "DRTSCILM", "DRSDCILM", "UMCSENT"} <= ids
    assert "GACDFSA066MSFRBPHI" in ids
    assert "NOCDFSA066MSFRBPHI" in ids
    assert "GACDISA066MSFRBNY" in ids
    assert "NOCDISA066MSFRBNY" in ids
    assert ids.isdisjoint(ch.V02_EXCLUDED_MARKET_DERIVED_SERIES)


def test_read_env_file_value_parses_quoted_fred_api_key(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join([
            "# local secrets",
            "DATABASE_URL=postgres://example",
            'FRED_API_KEY="abc123"',
        ]),
        encoding="utf-8",
    )

    assert ch.read_env_file_value(env_file, "FRED_API_KEY") == "abc123"


def test_merge_vintage_rows_deduplicates_and_sorts() -> None:
    older = _row("B", dt.date(2024, 1, 1), dt.date(2024, 2, 1), 2.0)
    first = _row("A", dt.date(2024, 1, 1), dt.date(2024, 2, 1), 1.0)
    replacement = _row("A", dt.date(2024, 1, 1), dt.date(2024, 2, 1), 1.5)

    rows = ch.merge_vintage_rows([older, first, replacement])

    assert [row.series_id for row in rows] == ["A", "B"]
    assert rows[0].value == 1.5


def test_v02_qualification_marks_missing_candidate_vintages(tmp_path) -> None:
    manifest_path = tmp_path / "feature_manifest.json"
    ch.write_json(
        manifest_path,
        {"macro_feature_primitives": {"logical_hash": ch.PARENT_V01_L2_HASH}},
    )
    vintage_cache = tmp_path / "macro_vintages.parquet"
    ch.write_vintage_cache(
        vintage_cache,
        [
            _row("ACOGNO", dt.date(2024, 1, 1), dt.date(2024, 2, 1), 100.0),
            _row("ACOGNO", dt.date(2024, 1, 1), dt.date(2024, 3, 1), 101.0, revision=1),
        ],
    )

    result = ch.run_v02_qualification(ch.V02QualificationConfig(
        v01_feature_manifest=manifest_path,
        vintage_cache=vintage_cache,
        output_dir=tmp_path / "out",
        start_date=dt.date(2024, 2, 15),
        end_date=dt.date(2024, 2, 15),
        offline=True,
        worker_commit="test",
    ))

    assert result["status"] == "blocked_data_qualification"
    audit = ch.read_parquet_records(tmp_path / "out" / "v02_series_audit.parquet")
    by_id = {row["series_id"]: row for row in audit}
    assert by_id["ACOGNO"]["eligibility_status"] == "baseline_v01_preserved"
    assert by_id["ICSA"]["eligibility_status"] == "missing_vintages"
    assert result["candidate_eligibility_status_counts"] == {"missing_vintages": 9}

    feature_manifest = ch.read_json_dict(
        tmp_path / "out" / "feature_manifest_v02_union.json"
    )
    assert feature_manifest["parent_v01_l2_hash"] == ch.PARENT_V01_L2_HASH
    assert feature_manifest["ready_for_grid"] is False
    assert "NFCI" in feature_manifest["market_derived_series_excluded"]


def test_load_l2_macro_from_feature_manifest_honors_file_name(tmp_path) -> None:
    rows = [{"business_date": "2024-01-02", "selection_mode": "latest", "series_id": "X"}]
    ch.write_parquet(tmp_path / "macro_feature_primitives_v02_union.parquet", rows)
    l2_hash = ch.logical_records_hash(rows)
    ch.write_json(
        tmp_path / "feature_manifest_v02_union.json",
        {
            "schema_version": ch.L2_SCHEMA_VERSION,
            "parameter_independent": True,
            "counterfactual_runtime_allowed": False,
            "selection_roles": {
                "latest": "pit_runtime_candidate",
                "first_release": "revised_vintage_counterfactual",
            },
            "macro_feature_primitives": {
                "logical_hash": l2_hash,
                "row_count": 1,
                "file_name": "macro_feature_primitives_v02_union.parquet",
            },
        },
    )

    _, l2_path, actual_hash, records = ch.load_l2_macro_from_feature_manifest(
        tmp_path / "feature_manifest_v02_union.json"
    )

    assert l2_path.name == "macro_feature_primitives_v02_union.parquet"
    assert actual_hash == l2_hash
    assert records == rows


def test_publication_status_rejects_low_axis_coverage_even_with_confidence() -> None:
    axis = {
        "growth": {
            "score": 1.0,
            "coverage": 1.0,
            "freshness": 1.0,
            "family_count": 2,
            "has_anchor": True,
            "dispersion": 0.0,
        },
        "inflation": {
            "score": 1.0,
            "coverage": 0.764706,
            "freshness": 1.0,
            "family_count": 3,
            "has_anchor": True,
            "dispersion": 0.0,
        },
    }
    status, reasons = ch.resolve_candidate_status(
        axis=axis,
        g_state=ch.AxisState(internal_sign=1, effective_sign=1, reason=None),
        i_state=ch.AxisState(internal_sign=1, effective_sign=1, reason=None),
        u_t=0.82,
        candidate_confidence=0.83,
    )
    assert status == "abstain"
    assert reasons == ["inflation_coverage_insufficient"]


def test_build_market_metrics_reports_row_price_source_and_convention() -> None:
    metrics = ch.build_market_metrics([
        {
            "status": "abstain",
            "status_reason_primary": "market_growth_deadband",
            "status_reasons_all": "market_growth_deadband,confidence_below_min",
            "candidate_confidence": 0.2,
            "candidate_quadrant": "expansion",
            "growth_score": 0.1,
            "inflation_score": 0.2,
            "price_source": "cagg_nav_daily",
            "price_convention": "uses cagg_nav_daily NAV on as_of when present",
        }
    ])

    assert metrics["market_price_sources"] == ["cagg_nav_daily"]
    assert metrics["market_price_convention"] == (
        "uses cagg_nav_daily NAV on as_of when present"
    )
    assert metrics["market_confidence_formula"] == "sqrt(growth_margin * inflation_margin)"


def test_market_confidence_scale_strong_and_neutral_cases() -> None:
    days = ch.business_days(dt.date(2024, 1, 2), dt.date(2024, 8, 30))
    levels = {"SPY": {}, "IEF": {}, "TIP": {}}
    for idx, day in enumerate(days):
        strong_ratio = 1.0 + 0.004 * idx
        levels["SPY"][day] = 100.0 * strong_ratio
        levels["IEF"][day] = 100.0
        levels["TIP"][day] = 100.0 * strong_ratio

    rows = ch.build_market_replay_from_levels(
        levels,
        days[0],
        days[-1],
        model_version="test",
        price_source="synthetic",
        price_convention="synthetic",
    )
    last = rows[-1]

    assert last["candidate_confidence"] == 1.0
    assert last["confidence_formula"] == "sqrt(growth_margin * inflation_margin)"
    assert last["confidence_growth_margin_component"] == 1.0
    assert last["confidence_inflation_margin_component"] == 1.0

    flat_levels = {
        ticker: {day: 100.0 for day in days}
        for ticker in ("SPY", "IEF", "TIP")
    }
    flat_rows = ch.build_market_replay_from_levels(
        flat_levels,
        days[0],
        days[-1],
        model_version="test",
        price_source="synthetic",
        price_convention="synthetic",
    )
    assert flat_rows[-1]["candidate_confidence"] == 0.0
    assert flat_rows[-1]["status"] == "abstain"


def test_market_grid_uses_primitives_and_carries_market_closed_days(tmp_path) -> None:
    rows = [
        {
            "business_date": "2024-01-02",
            "price_source": "test",
            "lookback_days": 126,
            "trading_session_indicator": True,
            "spy_available": True,
            "tip_available": True,
            "ief_available": True,
            "breakeven_available": True,
            "growth_126d_return": 0.10,
            "inflation_126d_return": 0.10,
        },
        {
            "business_date": "2024-01-03",
            "price_source": "test",
            "lookback_days": 126,
            "trading_session_indicator": False,
            "spy_available": False,
            "tip_available": False,
            "ief_available": False,
            "breakeven_available": False,
            "growth_126d_return": None,
            "inflation_126d_return": None,
        },
    ]
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    ch.write_parquet(feature_dir / "market_feature_primitives.parquet", rows)
    ch.write_json(
        feature_dir / "feature_manifest.json",
        {
            "schema_version": ch.L2_SCHEMA_VERSION,
            "parameter_independent": True,
            "business_date_calendar_hash": "cal",
            "market_feature_primitives": {
                "row_count": len(rows),
                "logical_hash": ch.logical_records_hash(rows),
                "grain": "business_date",
            },
        },
    )

    result = ch.run_market_grid(ch.MarketGridConfig(
        feature_manifest=feature_dir / "feature_manifest.json",
        macro_feature_manifest=None,
        a31_catalog=None,
        a32_grid_dir=None,
        output_dir=tmp_path / "market_grid",
        offline=True,
        worker_commit="worker",
    ))
    replay = ch.read_parquet_records(
        tmp_path / "market_grid" / "market_replay_selected.parquet"
    )
    summary = ch.read_parquet_records(
        tmp_path / "market_grid" / "market_grid_summary.parquet"
    )

    assert result["selected_has_valid_all_years"] is True
    assert replay[0]["status"] == "valid"
    assert replay[1]["status"] == "valid"
    assert replay[1]["carried_on_market_closed_day"] is True
    assert summary[0]["market_closed_days"] == 1
    assert summary[0]["spy_missing_data_days"] == 0


def test_a3_scope_outcome_prefers_macro_v03_when_freeze_gates_fail() -> None:
    outcome = ch.a3_scope_outcome(
        {
            "candidate_revision_change_rate": 0.1962,
            "consumable_state_coverage": 0.3809,
        },
        {"freeze_blockers": ["candidate_revision_change_rate_above_10pct_freeze_gate"]},
        {"selected_has_valid_all_years": True},
        {"both_valid_dates": 100},
    )

    assert outcome == "open_macro_v03"


def test_l3_l4_reference_parity_matches_replay(monkeypatch) -> None:
    def fake_score_all_series(selected, cut):
        return {
            cfg.series_id: ch.SeriesScore(
                score=1.0,
                observation_period=dt.date(2024, 1, 1),
                vintage_date=dt.date(2024, 2, 1),
                available_at=dt.datetime(2024, 2, 1, tzinfo=UTC),
                revision_number=5,
                freshness=1.0,
                vintage_quality=1.0,
            )
            for cfg in ch.BASELINE_SERIES
        }

    monkeypatch.setattr(ch, "score_all_series", fake_score_all_series)
    monkeypatch.setattr(ch, "reference_series_score", lambda cfg, series: 1.0)
    monkeypatch.setattr(ch, "series_freshness", lambda cut, available_at, **kwargs: 1.0)
    monkeypatch.setattr(ch, "vintage_quality", lambda revision_number: 1.0)
    rows = [
        _row(cfg.series_id, dt.date(2024, 1, 1), dt.date(2024, 2, 1), 100.0)
        for cfg in ch.BASELINE_SERIES
    ]
    calendar = [dt.date(2024, 2, 2), dt.date(2024, 2, 5)]
    reference = ch.replay_macro(rows, calendar, selection_mode="latest")
    primitives = ch.build_macro_feature_primitives(ch.group_rows(rows), calendar)
    l2_hash = ch.logical_records_hash(primitives)
    l3_rows, _, _ = ch.build_l3_score_panel(
        primitives,
        ch.reference_a31_config(),
        l2_macro_logical_hash=l2_hash,
        expected_l2_macro_logical_hash=l2_hash,
    )
    l4_rows, _ = ch.run_l4_state_machine(
        l3_rows,
        ch.reference_a32_config(),
        selection_mode="latest",
    )
    parity = ch.compare_l4_reference_parity(reference, l4_rows)
    assert parity["passed"] is True


def _l2_row(
    business_date: str,
    series_id: str,
    axis: str,
    family: str,
    score: float,
    *,
    transform_class: str = "rate_level",
    raw_value: float = 100.0,
    revision: int = 0,
) -> dict[str, object]:
    return {
        "business_date": business_date,
        "selection_mode": "latest",
        "selection_role": "pit_runtime_candidate",
        "counterfactual_only": False,
        "series_id": series_id,
        "axis_id": axis,
        "family_id": family,
        "transform_class": transform_class,
        "observation_period": "2024-01-01",
        "vintage_date": "2024-02-01",
        "available_at": "2024-02-01T00:00:00+00:00",
        "raw_value": raw_value,
        "revision_number": revision,
        "freshness": 1.0,
        "vintage_quality": 1.0,
        "coverage": 1.0,
        "reference_series_score": score,
        "reference_transform_reason": None,
    }


def test_l3_information_hash_ignores_unselected_union_series() -> None:
    rows = []
    for day, icsa_value in [("2024-02-02", 200.0), ("2024-02-05", 250.0)]:
        for cfg in ch.BASELINE_SERIES:
            rows.append(_l2_row(day, cfg.series_id, cfg.axis, cfg.family, 1.0))
        rows.append(
            _l2_row(
                day,
                "ICSA",
                "growth",
                "claims_labor",
                1.0,
                transform_class="claims_log4w",
                raw_value=icsa_value,
                revision=1,
            )
        )
    l2_hash = ch.logical_records_hash(rows)
    l3_rows, _, _ = ch.build_l3_score_panel(
        rows,
        ch.reference_a31_config(),
        l2_macro_logical_hash=l2_hash,
        expected_l2_macro_logical_hash=l2_hash,
    )

    assert l3_rows[0]["information_set_hash"] == l3_rows[1]["information_set_hash"]


def test_series_transform_overrides_apply_v02a_sign_conventions() -> None:
    claims_row = {
        "series_id": "ICSA",
        "transform_class": "claims_log4w",
        "z_claims_log_ma4": 2.0,
        "z_claims_delta_13w_log_ma4": 1.0,
    }
    diffusion_row = {
        "series_id": "GACDFSA066MSFRBPHI",
        "transform_class": "diffusion_zero_centered",
        "z_diffusion_zero_centered": 0.75,
    }
    cfg = ch.A31Config(
        **{
            **ch.asdict(ch.reference_a31_config()),
            "series_transform_overrides": {
                "ICSA": "claims_log4w_delta13",
                "GACDFSA066MSFRBPHI": "diffusion_zero_centered",
            },
        }
    )

    assert ch.series_score_from_l2_row(claims_row, cfg) == pytest.approx(-1.7)
    assert ch.series_score_from_l2_row(diffusion_row, cfg) == pytest.approx(0.75)


def test_a32_hash_is_canonical_and_evaluation_hash_is_contextual() -> None:
    a32 = ch.reference_a32_config()
    canonical = ch.a32_config_hash(a32)

    assert ch.a32_config_hash(a32) == canonical
    assert ch.evaluation_hash("a31-left", canonical) != ch.evaluation_hash(
        "a31-right",
        canonical,
    )


def test_quarterly_survey_level_v1_applies_sloos_direction_and_acceleration() -> None:
    standards_row = {
        "series_id": "DRTSCILM",
        "transform_class": "quarterly_survey_level_v1",
        "quarterly_level_z": 2.0,
        "quarterly_delta_1q_z": 1.0,
        "direction": -1,
    }
    demand_row = {
        "series_id": "DRSDCILM",
        "transform_class": "quarterly_survey_level_v1",
        "quarterly_level_z": 2.0,
        "quarterly_delta_1q_z": 1.0,
        "direction": 1,
    }
    ref = ch.reference_a31_config()
    level_only = ch.A31Config(
        **{
            **ch.asdict(ref),
            "series_transform_overrides": {
                "DRTSCILM": "quarterly_survey_level_v1",
                "DRSDCILM": "quarterly_survey_level_v1",
            },
        }
    )
    accel = ch.A31Config(
        **{
            **ch.asdict(level_only),
            "transformation_weights": {
                **level_only.transformation_weights,
                "quarterly_survey_level_v1": {"level": 0.80, "delta_1q": 0.20},
            },
        }
    )

    assert ch.series_score_from_l2_row(standards_row, level_only) == pytest.approx(-2.0)
    assert ch.series_score_from_l2_row(demand_row, level_only) == pytest.approx(2.0)
    assert ch.series_score_from_l2_row(standards_row, accel) == pytest.approx(-1.8)


def test_l3_rejects_parent_hash_mismatch() -> None:
    with pytest.raises(ValueError, match="parent hash mismatch"):
        ch.build_l3_score_panel(
            [],
            ch.reference_a31_config(),
            l2_macro_logical_hash="actual",
            expected_l2_macro_logical_hash="expected",
        )


def test_macro_metrics_separate_candidate_valid_and_latched_counts() -> None:
    rows = [
        {
            "status": "valid",
            "candidate_quadrant": "expansion",
            "published_quadrant": "expansion",
            "candidate_confidence": 0.8,
            "u": 0.8,
            "coverage_quality": 1.0,
            "growth_margin": 1.0,
            "inflation_margin": 1.0,
            "status_reasons_all": "",
        },
        {
            "status": "abstain",
            "candidate_quadrant": "slowdown",
            "published_quadrant": "expansion",
            "candidate_confidence": 0.5,
            "u": 0.8,
            "coverage_quality": 1.0,
            "growth_margin": 0.0,
            "inflation_margin": 1.0,
            "status_reason_primary": "confidence_below_min",
            "status_reasons_all": "confidence_below_min,growth_deadband",
        },
    ]
    metrics = ch.build_macro_metrics(rows)
    assert metrics["candidate_quadrant_counts_all_days"] == {"expansion": 1, "slowdown": 1}
    assert metrics["valid_published_quadrant_counts"] == {"expansion": 1}
    assert metrics["latched_quadrant_counts_including_abstain"] == {"expansion": 2}
    assert metrics["abstention_reason_any_counts"]["confidence_below_threshold"] == 1
    assert metrics["abstention_reason_any_counts"]["axis_neutral"] == 1


def test_classify_baseline_run_marks_failed_when_valid_rate_low() -> None:
    out = ch.classify_baseline_run(
        valid_rate=0.24,
        abstain_rate=0.76,
        revision_change_rate=0.32,
    )
    assert out == "diagnostic_baseline_failed"


def test_collect_environment_metadata_hashes_dependencies() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    metadata = ch.collect_environment_metadata(repo_root)
    assert metadata["python_version"]
    assert metadata["requirements_txt_sha256"]
    assert metadata["pip_freeze_sha256"]
    assert "git_dirty" in metadata


def test_harness_primary_artifacts_are_deterministic(tmp_path, monkeypatch) -> None:
    replay = [
        {
            "date": "2024-01-02",
            "decision_time": "2024-01-02T23:59:59+00:00",
            "selection_mode": "latest",
            "inputs_changed": True,
            "reevaluated": True,
            "source_vintage_hash": "h",
            "status": "valid",
            "status_reasons": "",
            "status_reasons_all": "",
            "status_reason_primary": None,
            "candidate_quadrant": "expansion",
            "instant_quadrant": "expansion",
            "published_quadrant": "expansion",
            "candidate_confidence": 0.8,
            "confidence": 0.8,
            "u": 0.8,
            "C": 1.0,
            "F": 1.0,
            "A": 1.0,
            "V": 1.0,
            "coverage_quality": 1.0,
            "freshness_quality": 1.0,
            "concordance_quality": 1.0,
            "vintage_quality": 1.0,
            "growth_score": 1.0,
            "inflation_score": 1.0,
            "growth_margin": 1.0,
            "inflation_margin": 1.0,
            "m_growth": 1.0,
            "m_inflation": 1.0,
            "growth_sign": 1,
            "inflation_sign": 1,
            "growth_internal_sign": 1,
            "inflation_internal_sign": 1,
            "growth_axis_state": "positive",
            "inflation_axis_state": "positive",
            "coverage_ok": True,
            "freshness_ok": True,
            "critical_family_ok": True,
            "u_ok": True,
            "confidence_ok": True,
            "dispersion_ok": True,
            "growth_family_count": 2,
            "inflation_family_count": 3,
            "growth_dispersion": 0.0,
            "inflation_dispersion": 0.0,
            "model_version": ch.MACRO_MODEL_VERSION,
            "macro_config": ch.MACRO_CONFIG_ID,
            "confidence_model_version": ch.CONFIDENCE_MODEL_VERSION,
        }
    ]
    pit_qa = {
        "row_count": 0,
        "unique_key_duplicate_count": 0,
        "available_before_vintage_count": 0,
        "data_hash": "data",
        "repeat_read_hash": "data",
        "idempotent_repeat_read": True,
        "selected_future_observation_count": 0,
        "synthetic_future_revision_no_effect": True,
        "spot_checks": [],
        "coverage_by_series": {},
    }
    fixed_env = {
        "python_version": "3.test",
        "python_executable": "python",
        "pyarrow_version": "test",
        "pandas_version": "test",
        "git_head": "abc",
        "git_dirty": False,
        "requirements_txt_sha256": "req",
        "pip_freeze_sha256": "freeze",
    }

    monkeypatch.setattr(ch, "read_vintage_rows", lambda conn, max_available_at: [])
    monkeypatch.setattr(ch, "compute_pit_qa", lambda rows, grouped, calendar, repeat_rows: pit_qa)
    monkeypatch.setattr(ch, "replay_macro", lambda rows, calendar, selection_mode="latest": replay)
    monkeypatch.setattr(ch, "build_revision_attribution", lambda latest, first: ([], [], []))
    monkeypatch.setattr(ch, "collect_environment_metadata", lambda repo_root: fixed_env)

    hashes = []
    for name in ("a", "b"):
        cfg = ch.HarnessConfig(
            start_date=dt.date(2024, 1, 2),
            end_date=dt.date(2024, 1, 2),
            output_dir=tmp_path / name,
            data_snapshot_id="snap",
            backend_commit="backend",
            worker_commit="worker",
            market_source="none",
        )
        ch.run_harness(None, cfg)
        hashes.append(
            {
                path.name: ch.hash_file(path)
                for path in sorted((tmp_path / name).glob("*"))
                if path.name not in {"parameter_manifest.yaml", "artifact_hashes.json"}
            }
        )

    assert hashes[0] == hashes[1]


def _write_grid_feature_inputs(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    monkeypatch.setattr(ch, "reference_series_score", lambda cfg, series: 1.0)
    monkeypatch.setattr(ch, "series_freshness", lambda cut, available_at, **kwargs: 1.0)
    monkeypatch.setattr(ch, "vintage_quality", lambda revision_number: 1.0)
    calendar = [dt.date(2024, 2, 2), dt.date(2024, 2, 5)]
    rows = [
        _row(cfg.series_id, dt.date(2024, 1, 1), dt.date(2024, 2, 1), 100.0)
        for cfg in ch.BASELINE_SERIES
    ]
    primitives = ch.build_macro_feature_primitives(ch.group_rows(rows), calendar)
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    ch.write_parquet(feature_dir / "macro_feature_primitives.parquet", primitives)
    l2_hash = ch.logical_records_hash(primitives)
    ch.write_json(
        feature_dir / "feature_manifest.json",
        {
            "schema_version": ch.L2_SCHEMA_VERSION,
            "parameter_independent": True,
            "counterfactual_runtime_allowed": False,
            "business_date_calendar_hash": ch.business_calendar_hash(calendar),
            "series_family_mapping_hash": ch.series_family_mapping_hash(),
            "selection_roles": {
                "latest": "pit_runtime_candidate",
                "first_release": "revised_vintage_counterfactual",
            },
            "macro_feature_primitives": {
                "row_count": len(primitives),
                "logical_hash": l2_hash,
                "grain": "business_date x selection_mode x series_id",
            },
        },
    )
    catalog = tmp_path / "catalog.yaml"
    catalog.write_text(json.dumps({
        "configs": [
            {"name": "A31-REF"},
            {"name": "A31-ROBUST-MEDIAN", "aggregation_method": "median"},
        ],
    }), encoding="utf-8")
    return feature_dir / "feature_manifest.json", catalog


def test_a31_grid_only_writes_artifacts_and_resumes(tmp_path, monkeypatch) -> None:
    manifest, catalog = _write_grid_feature_inputs(tmp_path, monkeypatch)
    out = tmp_path / "grid"

    first = ch.run_a31_grid(ch.A31GridConfig(
        feature_manifest=manifest,
        config_catalog=catalog,
        output_dir=out,
        jobs=1,
        resume=False,
        offline=True,
        worker_commit="worker",
    ))
    second = ch.run_a31_grid(ch.A31GridConfig(
        feature_manifest=manifest,
        config_catalog=catalog,
        output_dir=out,
        jobs=1,
        resume=True,
        offline=True,
        worker_commit="worker",
    ))

    assert first["completed_count"] == 2
    assert second["resume_skipped_configs"] == 2
    grid_manifest = json.loads((out / "grid_manifest.json").read_text(encoding="utf-8"))
    assert grid_manifest["grid_only"] is True
    assert grid_manifest["skipped_stages"] == ["DB", "Tiingo", "L0", "L1", "L2"]
    assert (out / "results").exists()
    assert ch.read_parquet_records(out / "a31_pareto.parquet")
    for result_dir in (out / "results").iterdir():
        assert (result_dir / "l3_score_panel.parquet").exists()
        assert (result_dir / "l4_replay_a32_ref.parquet").exists()
        result_manifest = json.loads(
            (result_dir / "result_manifest.json").read_text(encoding="utf-8")
        )
        assert result_manifest["counterfactual_only_flags"]["counterfactual_runtime_allowed"] is False


def test_a31_grid_parallel_matches_serial_metrics(tmp_path, monkeypatch) -> None:
    manifest, catalog = _write_grid_feature_inputs(tmp_path, monkeypatch)
    serial = tmp_path / "serial"
    parallel = tmp_path / "parallel"

    ch.run_a31_grid(ch.A31GridConfig(
        feature_manifest=manifest,
        config_catalog=catalog,
        output_dir=serial,
        jobs=1,
        offline=True,
        worker_commit="worker",
    ))
    ch.run_a31_grid(ch.A31GridConfig(
        feature_manifest=manifest,
        config_catalog=catalog,
        output_dir=parallel,
        jobs=2,
        offline=True,
        worker_commit="worker",
    ))

    serial_metrics = ch.read_parquet_records(serial / "a31_grid_metrics.parquet")
    parallel_metrics = ch.read_parquet_records(parallel / "a31_grid_metrics.parquet")
    assert ch.logical_records_hash(serial_metrics) == ch.logical_records_hash(parallel_metrics)
    serial_summary = ch.read_parquet_records(serial / "a31_grid_summary.parquet")
    parallel_summary = ch.read_parquet_records(parallel / "a31_grid_summary.parquet")
    assert [row["a31_config_hash"] for row in serial_summary] == [
        row["a31_config_hash"] for row in parallel_summary
    ]
    for serial_row, parallel_row in zip(serial_summary, parallel_summary):
        assert serial_row["result_classification"] == parallel_row["result_classification"]


def test_a31_progression_payload_allows_conditional_pass() -> None:
    payload = ch.a31_progression_payload({
        "candidate_revision_change_rate": 0.2220,
        "growth_sign_revision_change_days": 1028,
        "valid_rate": ch.A31_V01_VALID_RATE + 0.001,
    })

    assert payload["decision_policy_version"] == ch.A31_PROGRESSION_POLICY_VERSION
    assert payload["progression_level"] == "conditional_pass"
    assert payload["a31_provisional_status"] == "a31_provisional_candidate"
    assert payload["risk_flag"] == "elevated_vintage_instability"
    assert payload["absolute_target_20pct_met"] is False


def test_a31_progression_decision_uses_g2_control_for_fold_deltas() -> None:
    control = {
        "a31_config_hash": "control",
        "a31_config_name": "G2-CREDIT6040-15",
        "progression_level": "conditional_pass",
        "candidate_revision_change_rate": 0.2220,
        "growth_sign_revision_change_days": 1028,
        "a31_provisional_status": "a31_provisional_candidate",
        "risk_flag": "elevated_vintage_instability",
        "relative_revision_improvement_vs_v01": 0.119,
        "valid_rate": 0.22,
        "absolute_target_20pct_met": False,
    }
    challenger = {
        **control,
        "a31_config_hash": "challenger",
        "a31_config_name": "G2-CREDIT6040-10-SURVEY05",
        "candidate_revision_change_rate": 0.2210,
    }
    metrics = [
        {
            "a31_config_hash": "control",
            "fold": "2014_2017",
            "candidate_revision_change_rate": 0.25,
        },
        {
            "a31_config_hash": "challenger",
            "fold": "2014_2017",
            "candidate_revision_change_rate": 0.24,
        },
    ]

    decision = ch.build_a31_progression_decision_manifest(
        [control, challenger],
        metrics,
        config_catalog_path=Path("catalog.yaml"),
    )

    assert decision["new_decision"] == "advance_to_g2_limited"
    assert decision["reason"]["fold_revision_deltas_vs_control"]["2014_2017"] == pytest.approx(-0.01)
    assert decision["a4_status"] == ch.A4_PROVISIONAL_STATUS


def test_a32_grid_configs_match_limited_surface() -> None:
    configs = ch.a32_grid_configs()

    assert len(configs) == 24
    assert {cfg.min_confidence for cfg in configs} == {0.60, 0.65, 0.70}
    assert {cfg.growth_enter for cfg in configs} == {0.30, 0.35}
    assert {cfg.inflation_enter for cfg in configs} == {0.35, 0.40}
    assert {cfg.growth_exit for cfg in configs} == {0.10, 0.15}
    assert {cfg.inflation_exit for cfg in configs} == {0.10, 0.15}
    assert {cfg.u_floor for cfg in configs} == {0.65}
    assert {cfg.growth_score_scale for cfg in configs} == {1.0}
    assert {cfg.inflation_score_scale for cfg in configs} == {1.0}
    assert {cfg.dispersion_limit for cfg in configs} == {1.25}
    assert all(cfg.growth_exit < cfg.growth_enter for cfg in configs)
    assert all(cfg.inflation_exit < cfg.inflation_enter for cfg in configs)


def _a32_summary_row(
    role_suffix: str,
    growth_enter: float,
    inflation_enter: float,
    axis_exit: float,
    min_confidence: float,
) -> dict[str, object]:
    return {
        "a31_config_name": "G2-CREDIT6040-15-SURVEY05",
        "a31_config_hash": "a31",
        "a32_config_name": f"A32-{role_suffix}",
        "a32_config_hash": role_suffix,
        "evaluation_hash": f"eval-{role_suffix}",
        "growth_enter": growth_enter,
        "inflation_enter": inflation_enter,
        "axis_exit": axis_exit,
        "min_confidence": min_confidence,
        "u_floor": 0.65,
        "growth_score_scale": 1.0,
        "inflation_score_scale": 1.0,
        "dispersion_limit": 1.25,
    }


def _a32_metric_row(a32_hash: str) -> dict[str, object]:
    return {
        "fold": "full",
        "a31_config_hash": "a31",
        "a32_config_hash": a32_hash,
        "candidate_revision_change_rate": 0.1962,
        "growth_raw_sign_change_days": 940,
        "inflation_raw_sign_change_days": 320,
        "growth_sign_revision_change_days": 978,
        "growth_axis_state_change_days": 978,
        "inflation_sign_revision_change_days": 410,
        "inflation_axis_state_change_days": 415,
        "candidate_quadrant_change_days": 632,
        "status_revision_change_days": 42,
        "status_revision_change_rate": 0.013,
        "published_revision_change_days": 17,
        "published_revision_change_rate": 0.005,
        "latched_revision_change_days": 17,
        "latched_revision_change_rate": 0.005,
        "transition_timing_displacement": json.dumps({
            "p10": 1,
            "median": 3,
            "p90": 8,
        }),
        "candidate_flips_per_year": 8.7,
        "published_flips_per_year": 1.0,
        "candidate_duration_distribution": json.dumps({
            "p10": 4,
            "median": 22,
            "p90": 80,
        }),
        "published_duration_distribution": json.dumps({
            "p10": 12,
            "median": 75,
            "p90": 210,
        }),
        "valid_rate": 0.3247,
        "abstain_rate": 0.6753,
        "consumable_state_coverage": 0.3809,
        "stale_days_over_5bd": 1482,
        "longest_stale_run": 356,
        "days_since_last_valid_distribution": json.dumps({
            "min": 0,
            "p10": 0,
            "median": 2,
            "p90": 18,
            "max": 356,
        }),
        "consumed_state_age_distribution": json.dumps({
            "min": 0,
            "p10": 0,
            "median": 1,
            "p90": 4,
            "max": 5,
        }),
        "first_input_ready_date": "2014-02-19",
        "first_latched_date": "2016-02-08",
        "first_operational_date": "2016-02-08",
        "post_initialization_start_date": "2016-02-08",
        "quadrant_occupancy": json.dumps({"expansion": 10}),
        "reason_counts": json.dumps({"confidence_below_threshold": 5}),
    }


def test_a32_freeze_readiness_pareto_separates_metric_taxonomy() -> None:
    specs = [
        ("cur", 0.35, 0.35, 0.10, 0.60),
        ("conf", 0.35, 0.35, 0.10, 0.65),
        ("infl", 0.35, 0.40, 0.10, 0.60),
        ("exit", 0.35, 0.35, 0.15, 0.60),
        ("growth", 0.30, 0.35, 0.10, 0.60),
    ]
    summary = [_a32_summary_row(*spec) for spec in specs]
    metrics = [_a32_metric_row(str(row["a32_config_hash"])) for row in summary]

    pareto = ch.a32_freeze_readiness_pareto(summary, metrics)

    assert [row["pareto_role"] for row in pareto] == [
        "current_stability_preserving",
        "neighbor_confidence_0_65",
        "neighbor_inflation_enter_0_40",
        "neighbor_exit_0_15",
        "neighbor_growth_enter_0_30_high_coverage",
    ]
    assert pareto[0]["raw_growth_sign_revision_changes"] == 940
    assert pareto[0]["axis_effective_sign_revision_changes_growth"] == 978
    assert pareto[0]["axis_state_label_revision_changes_growth"] == 978
    assert pareto[0]["raw_inflation_sign_revision_changes"] == 320
    assert pareto[0]["state_age_since_last_valid_p50"] == 2
    assert pareto[0]["consumed_state_age_p50"] == 1
    assert pareto[0]["consumed_state_age_p90"] == 4
    assert pareto[0]["first_operational_date"] == "2016-02-08"


def test_freeze_blockers_keep_progression_separate_from_freeze() -> None:
    blockers = ch.freeze_blockers({
        "candidate_revision_change_rate": 0.1962,
        "valid_rate": 0.3247,
        "consumable_state_coverage": 0.3809,
        "raw_inflation_sign_revision_changes": 320,
        "consumed_state_age_p50": 1,
    })

    assert "candidate_revision_change_rate_above_10pct_freeze_gate" in blockers
    assert "valid_rate_below_original_freeze_band" in blockers
    assert "market_implied_valid_vs_valid_comparison_not_operational" in blockers
    assert "raw_inflation_revision_metric_not_available" not in blockers


def test_a32_freeze_readiness_pareto_by_fold_materializes_fold_rows() -> None:
    summary = [_a32_summary_row("cur", 0.35, 0.35, 0.10, 0.60)]
    full = _a32_metric_row("cur")
    post = {**full, "fold": "post_initialization", "valid_rate": 0.55}
    folds = [full, post]

    pareto = ch.a32_freeze_readiness_pareto(
        summary + [
            _a32_summary_row("conf", 0.35, 0.35, 0.10, 0.65),
            _a32_summary_row("infl", 0.35, 0.40, 0.10, 0.60),
            _a32_summary_row("exit", 0.35, 0.35, 0.15, 0.60),
            _a32_summary_row("growth", 0.30, 0.35, 0.10, 0.60),
        ],
        folds + [
            _a32_metric_row("conf"),
            _a32_metric_row("infl"),
            _a32_metric_row("exit"),
            _a32_metric_row("growth"),
        ],
    )
    by_fold = ch.a32_freeze_readiness_pareto_by_fold(pareto[:1], folds)

    assert [row["fold"] for row in by_fold] == ["full", "post_initialization"]
    assert by_fold[1]["valid_rate"] == 0.55
    assert by_fold[1]["consumed_state_age_p90"] == 4


def test_a31_catalog_compiles_multiple_family_shifts_from_immutable_base() -> None:
    cfg, metadata = ch.a31_config_from_catalog_entry({
        "name": "A31-MULTI-SHIFT",
        "family_weight_shifts": [
            {
                "axis": "inflation",
                "source": "consumer_prices",
                "recipients": ["producer_prices", "wages", "expectations"],
                "delta": 0.05,
            },
            {
                "axis": "inflation",
                "source": "wages",
                "recipients": ["consumer_prices", "producer_prices", "expectations"],
                "delta": 0.05,
            },
        ],
    })
    ref = ch.reference_a31_config()
    weights = cfg.family_weights["inflation"]

    assert abs(sum(weights.values()) - 1.0) < 1e-12
    assert weights["consumer_prices"] == pytest.approx(
        ref.family_weights["inflation"]["consumer_prices"] - 0.05 + (0.05 / 3.0)
    )
    assert weights["wages"] == pytest.approx(
        ref.family_weights["inflation"]["wages"] - 0.05 + (0.05 / 3.0)
    )
    assert metadata["family_weight_shifts"][0]["recipients"] == [
        "expectations",
        "producer_prices",
        "wages",
    ]
    assert metadata["resolved_family_weights"] == ch.normalize_logical_value(cfg.family_weights)


def test_a31_transformation_weights_use_l2_component_z() -> None:
    ref = ch.reference_a31_config()
    cfg = ch.A31Config(
        **{
            **ch.asdict(ref),
            "name": "A31-STABLE",
            "transformation_weights": {
                **ref.transformation_weights,
                "quantity_index": {
                    "acceleration_3m": 0.30,
                    "acceleration_6m": 0.40,
                    "change_12m": 0.30,
                },
            },
        }
    )
    row = {
        "transform_class": "quantity_index",
        "reference_series_score": 0.10,
        "z_acceleration_3m": 1.0,
        "z_acceleration_6m": 0.0,
        "z_change_12m": -1.0,
    }

    assert ch.series_score_from_l2_row(row, ref) == 0.10
    assert ch.series_score_from_l2_row(row, cfg) == pytest.approx(0.0)
