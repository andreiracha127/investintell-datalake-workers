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
    monkeypatch.setattr(ch, "series_freshness", lambda cut, available_at: 1.0)
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
    monkeypatch.setattr(ch, "series_freshness", lambda cut, available_at: 1.0)
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
    catalog.write_text(
        "\n".join([
            "configs:",
            "  - name: A31-REF",
            "  - name: A31-ROBUST-MEDIAN",
            "    aggregation_method: median",
        ]),
        encoding="utf-8",
    )
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
