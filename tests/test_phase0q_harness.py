"""TDD suite for the Phase 0Q metric parity harness (PR-C).

Covers, in order:
  * PIT golden parity (adapter == src.macro_pit semantics),
  * decision-engine goldens (hysteresis / latch / coverage vs quadrant_macro),
  * sleeve unit tests (constraints, drift band, cost application, renormalization),
  * metric formula unit tests (hand-computed mini-fixtures),
  * determinism (two runs byte-identical),
  * schema validation of the emitted contract result,
  * gate-report integrity (measured == per-cell; no go unless envelope satisfied).

Network-free, DB-free. The frozen decision modules are imported for reference /
parity only and never modified.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from harness.phase0q import decision, metrics, pit, runner, sleeve

ROOT = Path(__file__).resolve().parents[1]
PACK_DIR = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"
EVIDENCE_DIR = ROOT / "artifacts" / "quant" / "open_macro_v03_metric_evidence_001"


# --------------------------------------------------------------------------- #
# PIT golden parity                                                           #
# --------------------------------------------------------------------------- #

def _rows(*triples):
    """Build pack-v2 vintage rows from (series, period, avail, value[, vintage])."""
    out = []
    for series, period, avail, value, *rest in triples:
        vintage = rest[0] if rest else avail[:10]
        out.append({
            "series_id": series,
            "observation_period": period,
            "vintage_date": vintage,
            "value": value,
            "available_at": avail,
            "revision_number": 0,
            "source": "alfred",
            "source_spec_version": "macro_quadrant_us_v1.0",
        })
    return out


class _Cur:
    """Reference cursor that applies the real _PIT_SQL semantics in Python."""

    def __init__(self, rows):
        self._rows = rows
        self._result = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        series_ids, cutoff = params
        wanted = set(series_ids)
        best = {}
        for r in self._rows:
            if r["series_id"] not in wanted:
                continue
            avail = dt.datetime.fromisoformat(r["available_at"])
            if avail.tzinfo is None:
                avail = avail.replace(tzinfo=dt.timezone.utc)
            if avail > cutoff:
                continue
            key = (r["series_id"], r["observation_period"])
            if key not in best or avail >= best[key][0]:
                best[key] = (avail, r)
        self._result = [
            (r["series_id"], dt.date.fromisoformat(r["observation_period"][:10]), r["value"])
            for _, (_, r) in sorted(best.items())
        ]

    def fetchall(self):
        return self._result


class _Conn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cur(self._rows)


def test_pit_matches_reference_semantics_latest_vintage():
    """Golden parity: adapter picks the value known at decision_time per period."""
    rows = _rows(
        ("PAYEMS", "2010-03-01", "2010-04-02T00:00:00+00:00", 129871.0),
        ("PAYEMS", "2010-04-01", "2010-05-07T00:00:00+00:00", 130161.0),
    )
    out = pit.latest_vintage_as_of(
        rows, ["PAYEMS"], dt.datetime(2010, 6, 1, tzinfo=dt.timezone.utc))
    assert out == {"PAYEMS": {dt.date(2010, 3, 1): 129871.0, dt.date(2010, 4, 1): 130161.0}}


def test_pit_excludes_vintages_after_decision_time():
    """available_at strictly after decision_time is invisible (no look-ahead)."""
    rows = _rows(
        ("CPILFESL", "2020-01-01", "2020-02-15T00:00:00+00:00", 100.0),
        ("CPILFESL", "2020-01-01", "2020-07-15T00:00:00+00:00", 101.0),  # revised later
    )
    out = pit.latest_vintage_as_of(
        rows, ["CPILFESL"], dt.datetime(2020, 3, 1, tzinfo=dt.timezone.utc))
    assert out == {"CPILFESL": {dt.date(2020, 1, 1): 100.0}}  # sees only the first vintage


def test_pit_picks_latest_available_at_for_same_period():
    """Two vintages of one period before the cutoff -> the later available_at wins."""
    rows = _rows(
        ("INDPRO", "2015-06-01", "2015-07-15T00:00:00+00:00", 100.0),
        ("INDPRO", "2015-06-01", "2015-08-15T00:00:00+00:00", 102.0),
    )
    out = pit.latest_vintage_as_of(
        rows, ["INDPRO"], dt.datetime(2016, 1, 1, tzinfo=dt.timezone.utc))
    assert out == {"INDPRO": {dt.date(2015, 6, 1): 102.0}}


def test_pit_parity_against_reference_cursor_on_mixed_basket():
    """The adapter agrees row-for-row with the real src.macro_pit query semantics."""
    from src import macro_pit as ref

    rows = _rows(
        ("INDPRO", "2015-06-01", "2015-07-15T00:00:00+00:00", 100.0),
        ("INDPRO", "2015-06-01", "2015-08-15T00:00:00+00:00", 102.0),
        ("INDPRO", "2015-07-01", "2015-08-15T00:00:00+00:00", 103.0),
        ("PAYEMS", "2015-06-01", "2015-07-03T00:00:00+00:00", 141000.0),
        ("PAYEMS", "2015-06-01", "2099-01-01T00:00:00+00:00", 999.0),  # future -> excluded
    )
    cutoff = dt.datetime(2016, 1, 1, tzinfo=dt.timezone.utc)
    adapter_out = pit.latest_vintage_as_of(rows, ["INDPRO", "PAYEMS"], cutoff)
    reference_out = ref.latest_vintage_as_of(_Conn(rows), ["INDPRO", "PAYEMS"], cutoff)
    assert adapter_out == reference_out


def test_pit_index_matches_scan_over_real_pack():
    """The bisect-based PitIndex is byte-identical to the O(rows) scan (same
    DISTINCT-ON winners) at several decision times across the pack history."""
    rows = _load_pack_table("macro_observation_vintage")
    index = pit.PitIndex(rows)
    series_ids = ["INDPRO", "PCEC96", "PAYEMS", "ACOGNO",
                  "CPILFESL", "PPIFIS", "AHETPI", "MICH"]
    for t in (dt.datetime(2014, 3, 31, tzinfo=dt.timezone.utc),
              dt.datetime(2018, 6, 30, tzinfo=dt.timezone.utc),
              dt.datetime(2026, 6, 30, tzinfo=dt.timezone.utc)):
        assert index.latest_vintage_as_of(series_ids, t) == \
            pit.latest_vintage_as_of(rows, series_ids, t)


def test_pit_over_real_pack_v2_ppifis_first_vintage():
    """Over the committed pack, PPIFIS is unseen before its first vintage 2014-02-19
    and visible at/after it (the constraining full-basket series)."""
    rows = _load_pack_table("macro_observation_vintage")
    before = pit.latest_vintage_as_of(
        rows, ["PPIFIS"], dt.datetime(2014, 2, 18, tzinfo=dt.timezone.utc))
    after = pit.latest_vintage_as_of(
        rows, ["PPIFIS"], dt.datetime(2014, 3, 1, tzinfo=dt.timezone.utc))
    assert before == {"PPIFIS": {}}
    assert after["PPIFIS"], "PPIFIS must be visible from its first vintage"


def _load_pack_table(name: str):
    path = PACK_DIR / "data" / "canonical" / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- #
# Decision engine goldens                                                     #
# --------------------------------------------------------------------------- #

def test_month_end_decision_dates_are_last_calendar_day():
    dates = decision.month_end_decision_dates(dt.date(2020, 1, 15), dt.date(2020, 4, 10))
    assert dates == [dt.date(2020, 1, 31), dt.date(2020, 2, 29), dt.date(2020, 3, 31)]


def _synthetic_basket_rows(monthly_values, start_period=dt.date(2010, 1, 1),
                           n_months=180):
    """Build a full 8-series vintage store where every series shares one monthly
    level path. Each observation period is published (available_at) on the first of
    the following month, so a decision on month-end sees all periods <= that month.
    """
    from src.macro_sources import SEED_SOURCES
    rows = []
    period = start_period
    for k in range(n_months):
        value = monthly_values(k)
        avail = _first_of_next_month(period)
        for spec in SEED_SOURCES:
            rows.append({
                "series_id": spec.series_id,
                "observation_period": period.isoformat(),
                "vintage_date": avail.isoformat(),
                "value": value,
                "available_at": f"{avail.isoformat()}T00:00:00+00:00",
                "revision_number": 0,
                "source": "alfred",
                "source_spec_version": "macro_quadrant_us_v1.0",
            })
        period = _first_of_next_month(period)
    return rows


def _first_of_next_month(d: dt.date) -> dt.date:
    if d.month == 12:
        return dt.date(d.year + 1, 1, 1)
    return dt.date(d.year, d.month + 1, 1)


def test_decision_latches_chain_and_reaches_valid_quadrant():
    """A basket with a varying-then-accelerating path eventually confirms a valid
    quadrant, and the per-axis internal_sign latches forward (owner decision C)."""
    import math
    # A pure geometric path has a CONSTANT impulse -> zero MAD -> None score
    # (degenerate). Add a sinusoidal wobble on top of a rising trend so the impulse
    # history has non-zero robust scale, then a strong late acceleration to confirm.
    def path(k):
        trend = 100.0 * (1.004 ** k)
        wobble = 1.0 + 0.03 * math.sin(k / 5.0)
        accel = 1.0 + 0.02 * max(0, k - 120)
        return trend * wobble * accel
    rows = _synthetic_basket_rows(path)
    series = decision.run_decision_series(rows, dt.date(2013, 1, 1), dt.date(2024, 12, 31))
    assert series, "expected monthly decisions"
    # latch: once an internal_sign is set it is carried (never silently reset to None
    # by a mere deadband) — check monotonic presence after first non-null.
    first_latched = next((r for r in series if r.growth_internal_sign is not None), None)
    assert first_latched is not None
    idx = series.index(first_latched)
    assert all(r.growth_internal_sign is not None for r in series[idx:])
    # at least one valid consumable quadrant is produced.
    assert any(r.has_valid_quadrant() for r in series)


def test_decision_reduced_coverage_yields_unavailable_when_score_missing():
    """With only one growth series present (others absent), growth coverage falls
    below the 0.80 gate -> unavailable, quadrant None, confidence None (parity with
    quadrant_macro build_snapshot coverage handling)."""
    from src.macro_sources import SEED_SOURCES
    rows = []
    period = dt.date(2010, 1, 1)
    for k in range(180):
        avail = _first_of_next_month(period)
        # Only INDPRO present -> growth coverage 0.25, inflation coverage 0.0.
        for spec in SEED_SOURCES:
            if spec.series_id != "INDPRO":
                continue
            rows.append({
                "series_id": spec.series_id,
                "observation_period": period.isoformat(),
                "vintage_date": avail.isoformat(),
                "value": 100.0 * (1.01 ** k),
                "available_at": f"{avail.isoformat()}T00:00:00+00:00",
                "revision_number": 0, "source": "alfred",
                "source_spec_version": "macro_quadrant_us_v1.0",
            })
        period = _first_of_next_month(period)
    series = decision.run_decision_series(rows, dt.date(2013, 1, 1), dt.date(2015, 12, 31))
    assert series
    assert all(r.status in {"unavailable", "low_confidence"} for r in series)
    assert all(r.quadrant is None for r in series)


def test_month_end_count_over_primary_window():
    """The primary full-basket window spans exactly 148 monthly decision dates."""
    dates = decision.month_end_decision_dates(dt.date(2014, 3, 1), dt.date(2026, 6, 30))
    assert len(dates) == 148  # 2014-03 .. 2026-06 inclusive month-ends


def test_decision_over_real_pack_full_basket_has_full_coverage_and_decisions():
    """Golden over the committed pack (representative slice): full-basket coverage is
    1.0 on every scheduled decision, every decision is valid or low_confidence (never
    unavailable/invalid), valid quadrants come from the four-quadrant label set, and
    the latched internal_sign is always present once the basket is scored."""
    rows = _load_pack_table("macro_observation_vintage")
    series = decision.run_decision_series(rows, dt.date(2018, 1, 1), dt.date(2020, 12, 31))
    assert len(series) == 36
    assert all(r.coverage_quality == 1.0 for r in series)
    assert all(r.status in {"valid", "low_confidence"} for r in series)
    # latch monotonicity: once an axis internal_sign is set it never reverts to None.
    for axis_attr in ("growth_internal_sign", "inflation_internal_sign"):
        signs = [getattr(r, axis_attr) for r in series]
        first = next((i for i, s in enumerate(signs) if s is not None), None)
        assert first is not None
        assert all(s is not None for s in signs[first:])
    valid_quadrants = {r.quadrant for r in series if r.has_valid_quadrant()}
    assert valid_quadrants
    assert valid_quadrants <= {"recovery", "expansion", "slowdown", "contraction"}


# --------------------------------------------------------------------------- #
# Sleeve unit tests                                                           #
# --------------------------------------------------------------------------- #

def _params(**over):
    return sleeve.SleeveParams(candidate_id=over.pop("candidate_id", "baseline_current"), **over)


def test_sleeve_target_weights_sum_to_one_and_respect_constraints():
    for quadrant in ("recovery", "expansion", "slowdown", "contraction"):
        w = sleeve.target_weights(quadrant, _params(), sleeve.SLEEVE_TICKERS)
        assert abs(sum(w.values()) - 1.0) < 1e-12
        risk = sum(w.get(t, 0.0) for t in sleeve.RISK_ASSETS)
        defensive = sum(w.get(t, 0.0) for t in sleeve.DEFENSIVE_ASSETS)
        assert risk <= sleeve.RISK_CAP_BASELINE + 1e-9
        assert defensive >= sleeve.DEFENSIVE_FLOOR_BASELINE - 1e-9


def test_sleeve_risk_tilt_moves_spy_vs_shy():
    base = sleeve.target_weights("expansion", _params(risk_tilt=0.0), sleeve.SLEEVE_TICKERS)
    up = sleeve.target_weights("expansion", _params(risk_tilt=0.01), sleeve.SLEEVE_TICKERS)
    # +1pp tilt raises SPY share and lowers SHY share (before renormalization the
    # deltas are exact; after renormalization the ordering is preserved).
    assert up["SPY"] > base["SPY"]
    assert up["SHY"] < base["SHY"]


def test_sleeve_pre_inception_renormalizes_when_dbc_absent():
    """Before DBC inception (2006-02-06) DBC is dropped from a quadrant that would
    hold it (expansion holds DBC 0.15), and the remaining weights renormalize."""
    available = [t for t in sleeve.SLEEVE_TICKERS if t != "DBC"]
    w = sleeve.target_weights("expansion", _params(), available)
    assert "DBC" not in w
    assert abs(sum(w.values()) - 1.0) < 1e-12


def test_sleeve_risk_cap_delta_tightens_cap():
    """A negative risk_cap_delta_pp lowers the enforced risk cap; a quadrant whose
    baseline risk share exceeds the tightened cap is scaled down to it."""
    # recovery holds SPY 0.60 (+ DBC 0.0) = 0.60 risk; tighten cap to 0.50.
    w = sleeve.target_weights("recovery", _params(risk_cap_delta_pp=-15.0), sleeve.SLEEVE_TICKERS)
    risk = sum(w.get(t, 0.0) for t in sleeve.RISK_ASSETS)
    assert risk <= 0.50 + 1e-9


def _flat_prices(tickers, start, days, rate_by_ticker):
    """Synthetic eod rows: each ticker compounds at a fixed daily rate from 100."""
    rows = []
    for t in tickers:
        p = 100.0
        d = start
        for _ in range(days):
            rows.append({"ticker": t, "date": d.isoformat(), "close": p,
                         "adjusted_close": p, "volume": 1000})
            p *= (1.0 + rate_by_ticker[t])
            d = d + dt.timedelta(days=1)
    return rows


class _FakeDecision:
    def __init__(self, as_of, quadrant, status="valid"):
        self.as_of = as_of
        self.quadrant = quadrant
        self.status = status

    def has_valid_quadrant(self):
        return self.status == "valid" and self.quadrant is not None


def test_sleeve_charges_one_way_cost_on_rebalance():
    """A single rebalance from empty to target charges cost = bps * one_way_turnover
    (one_way = 0.5*sum|dw| = 0.5*1.0 = 0.5 when going flat->fully-invested)."""
    start = dt.date(2020, 1, 1)
    prices = sleeve.PriceFrame(_flat_prices(
        sleeve.SLEEVE_TICKERS, start, 40, {t: 0.0 for t in sleeve.SLEEVE_TICKERS}))
    decisions = [_FakeDecision(dt.date(2020, 1, 31), "expansion")]
    # zero-return prices, so NAV only changes by the trade cost.
    res_free = sleeve.simulate(prices, decisions, _params(), start=start,
                               end=dt.date(2020, 2, 9), cost_bps=0)
    res_cost = sleeve.simulate(prices, decisions, _params(), start=start,
                               end=dt.date(2020, 2, 9), cost_bps=25)
    assert res_free.nav[-1] == pytest.approx(1.0, abs=1e-12)
    # one_way turnover 0.5 -> cost 25bps*0.5 = 12.5bps => NAV ~ 1 - 0.00125.
    assert res_cost.one_way_turnover_by_date[list(res_cost.one_way_turnover_by_date)[0]] \
        == pytest.approx(0.5, abs=1e-12)
    assert res_cost.nav[-1] == pytest.approx(1.0 * (1 - 0.0025 * 0.5), abs=1e-9)


def test_sleeve_no_trade_within_drift_band():
    """Two consecutive month-ends with the SAME quadrant and flat prices produce
    exactly one trade (the initial one); the second is inside the 5pp drift band."""
    start = dt.date(2020, 1, 1)
    prices = sleeve.PriceFrame(_flat_prices(
        sleeve.SLEEVE_TICKERS, start, 90, {t: 0.0 for t in sleeve.SLEEVE_TICKERS}))
    decisions = [_FakeDecision(dt.date(2020, 1, 31), "expansion"),
                 _FakeDecision(dt.date(2020, 2, 29), "expansion")]
    res = sleeve.simulate(prices, decisions, _params(), start=start,
                          end=dt.date(2020, 3, 20), cost_bps=5)
    assert len(res.rebalance_dates) == 1


def test_sleeve_trades_on_quadrant_change():
    start = dt.date(2020, 1, 1)
    prices = sleeve.PriceFrame(_flat_prices(
        sleeve.SLEEVE_TICKERS, start, 90, {t: 0.0 for t in sleeve.SLEEVE_TICKERS}))
    decisions = [_FakeDecision(dt.date(2020, 1, 31), "expansion"),
                 _FakeDecision(dt.date(2020, 2, 29), "contraction")]
    res = sleeve.simulate(prices, decisions, _params(), start=start,
                          end=dt.date(2020, 3, 20), cost_bps=5)
    assert len(res.rebalance_dates) == 2


# --------------------------------------------------------------------------- #
# Metric formula unit tests (hand-computed mini-fixtures)                     #
# --------------------------------------------------------------------------- #

def test_max_drawdown_window_local_peak():
    # NAV 100 -> 120 -> 90 -> 110: peak 120, trough 90 -> MDD = 30/120 = 0.25.
    assert metrics.max_drawdown([100, 120, 90, 110]) == pytest.approx(0.25)
    # monotone up -> zero drawdown.
    assert metrics.max_drawdown([1, 2, 3, 4]) == 0.0


def test_annualized_volatility_matches_hand_computation():
    import math
    # NAV with alternating +10% / -10% log-ish steps.
    nav = [100.0, 110.0, 99.0, 108.9]
    rets = [math.log(110/100), math.log(99/110), math.log(108.9/99)]
    expected = statistics_stdev(rets) * math.sqrt(252)
    assert metrics.annualized_volatility(nav) == pytest.approx(expected, rel=1e-12)


def statistics_stdev(xs):
    import statistics
    return statistics.stdev(xs)


def test_worst_5d_return_picks_worst_rolling_window():
    # A sharp 5-day drop from 100 to 80 = -0.20 is the worst 5-day return.
    nav = [100, 101, 102, 103, 104, 80, 82, 84]
    # rolling 5-day endpoints: index5/index0 = 80/100 - 1 = -0.20 is the worst.
    assert metrics.worst_5d_return(nav) == pytest.approx(-0.20)


def test_turnover_annualized_trailing_252_and_average():
    # Two rebalances of 0.5 one-way turnover each within a 10-day window.
    dates = [dt.date(2020, 1, d) for d in range(1, 11)]
    turnover = {dates[0]: 0.5, dates[5]: 0.5}
    out = metrics.one_way_turnover_annualized(dates, turnover)
    assert out["total_one_way"] == pytest.approx(1.0)
    # both rebalances fall inside a trailing 252-day window -> max trailing = 1.0.
    assert out["max_trailing_252"] == pytest.approx(1.0)
    # window average annualized = total * 252 / n_days = 1.0 * 252 / 10 = 25.2.
    assert out["window_average_annualized"] == pytest.approx(25.2)


def test_decision_coverage_fraction():
    scheduled = [dt.date(2020, 1, 31), dt.date(2020, 2, 29), dt.date(2020, 3, 31)]
    valid = {dt.date(2020, 1, 31), dt.date(2020, 3, 31)}
    assert metrics.decision_coverage(scheduled, valid) == pytest.approx(2 / 3)
    assert metrics.decision_coverage([], set()) == 0.0


def test_stability_max_dev_from_median():
    folds = [{"mdd": 0.10}, {"mdd": 0.14}, {"mdd": 0.30}]
    out = metrics.stability_from_folds(folds)
    # median 0.14; max abs dev = |0.30 - 0.14| = 0.16.
    assert out["mdd_max_dev_from_median"] == pytest.approx(0.16)


# --------------------------------------------------------------------------- #
# Runner: determinism, schema, gate integrity                                #
# --------------------------------------------------------------------------- #

def _fast_config():
    """A small, fast, fully-deterministic run config for CI: a 2-candidate x
    2-cost grid over a short full-basket primary window and one stress window."""
    return runner.RunConfig(
        run_id="phase0q-harness-test-0000",
        started_at="2026-07-02T00:00:00+00:00",
        finished_at="2026-07-02T00:00:01+00:00",
        harness_commit="0" * 40,
        candidates=(runner.SCENARIO_CANDIDATES[0], runner.SCENARIO_CANDIDATES[3]),
        cost_grid=(0, 5),
        primary_window=(dt.date(2019, 6, 1), dt.date(2020, 12, 31)),
        stress_windows=(
            {"window_id": "COVID_2020", "start": dt.date(2020, 2, 15),
             "end": dt.date(2020, 4, 30), "coverage": "full_basket"},
        ),
    )


@pytest.fixture(scope="module")
def fast_run():
    return runner.run_harness(PACK_DIR, _fast_config())


def test_runner_result_validates_against_v2_contract_schema(fast_run):
    import jsonschema
    schema = json.loads(
        (ROOT / "contracts" / "quant-engine" / "v2" / "job-result.schema.json")
        .read_text(encoding="utf-8"))
    # canonicalize the result (12-decimal floats etc.) then validate.
    result = runner.canonicalize(fast_run["result"])
    jsonschema.validate(result, schema)
    assert result["classification"] == "metric_evidence_only"
    assert result["a5_status"] == "blocked"
    assert result["runtime_activation"] is False
    assert result["official_result"] is False
    assert result["db_write"] == "none"


def test_runner_is_deterministic_byte_identical(fast_run):
    run2 = runner.run_harness(PACK_DIR, _fast_config())
    assert runner.canonical_json(fast_run["result"]) == runner.canonical_json(run2["result"])
    assert runner.canonical_json(fast_run["gate_report"]) == \
        runner.canonical_json(run2["gate_report"])
    for c1, c2 in zip(fast_run["cells"], run2["cells"]):
        assert runner.canonical_json(c1) == runner.canonical_json(c2)


def test_runner_refuses_on_pack_mismatch(tmp_path):
    # copy the pack but corrupt the manifest sha -> verify_pack fails -> refuse.
    import shutil
    dest = tmp_path / "pack"
    shutil.copytree(PACK_DIR, dest)
    manifest = json.loads((dest / "manifest.json").read_text(encoding="utf-8"))
    manifest["input_pack_sha256"] = "0" * 64
    (dest / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RuntimeError, match="pack verification failed"):
        runner.run_harness(dest, _fast_config())


def test_gate_report_integrity_measured_matches_cells(fast_run):
    """Every gate's measured value in the report is exactly the per-cell measured
    value, and no gate is marked go at base cost unless the measured value satisfies
    the envelope for every candidate cell at that cost."""
    report = fast_run["gate_report"]
    cells_by_key = {(c["candidate_id"], c["cost_bps"]): c for c in fast_run["cells"]}
    base = str(runner.BASE_COST_BPS)
    per_gate = report["per_cost_level"][base]["per_gate"]

    for cid, cell in [(k[0], v) for k, v in cells_by_key.items() if k[1] == runner.BASE_COST_BPS]:
        judged = runner.judge_gates_for_cell(cell)
        assert per_gate["turnover"]["by_candidate"][cid]["measured"] == \
            cell["primary_window"]["annualized_turnover"]
        assert per_gate["drawdown"]["by_candidate"][cid]["measured"] == \
            cell["primary_window"]["max_drawdown"]
        assert per_gate["volatility"]["by_candidate"][cid]["measured"] == \
            cell["primary_window"]["annualized_volatility"]
        # go flag consistency with the envelope.
        assert judged["turnover"]["go"] == (
            cell["primary_window"]["annualized_turnover"]
            <= runner.BASE_ENVELOPE["max_one_way_turnover_annualized"])

    # overall base go requires ALL candidate cells at base cost to pass the gate.
    for gate in ("turnover", "drawdown", "volatility", "stress_windows", "out_of_sample"):
        overall_go = report["gates_overall_base_cost"][gate]["go_no_go"] == "go"
        all_pass = all(
            runner.judge_gates_for_cell(cells_by_key[(cid, runner.BASE_COST_BPS)])[gate]["go"]
            for cid in {c["candidate_id"] for c in fast_run["cells"]})
        assert overall_go == all_pass


def test_gate_report_governance_pins_never_activate(fast_run):
    report = fast_run["gate_report"]
    assert report["approved"] is False
    assert report["status"] == "measured_pending_cloud_leg"
    assert report["governance"]["A5"] == "blocked"
    assert report["governance"]["runtime_activation"] is False
    assert report["governance"]["freeze_ready"] is False
    assert report["execution_legs"] == {
        "local_python_pure": "complete", "qc_research_object_store": "pending"}
