"""Pure tests for the Wave-1 metric engine runner (activation Wave 1, Task 3).

The runner is DB-free glue: published security terms + the eligible latest clean
price (% of par) -> validated engine calls -> typed per-metric rows. Every
degenerate outcome is a TYPED status — never a NaN, never a fabricated value,
never a silent zero (program Constraint 4).

Wave-1 metric set is EXACTLY (security_ytm, security_ytw, current_yield, wal);
OAS / z-spread / duration are deliberately absent (Global Constraints 1-2).
"""
from __future__ import annotations

import math
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from src.bonds import cashflows, pricing
from src.bonds.metrics_engine_runner import (
    PRICE_CONSUMING_METRICS,
    STATUS_AVAILABLE,
    STATUS_ENGINE_TYPED_ERROR,
    STATUS_GATE_NOT_PASSED,
    STATUS_NO_ELIGIBLE_PRICE,
    STATUS_TERMS_INSUFFICIENT,
    WAVE1_METRICS,
    EligiblePrice,
    SecurityTermsInput,
    compute_security_metrics,
)

SEC = UUID("11111111-1111-5111-8111-111111111111")
AS_OF = date(2025, 1, 1)
ALL_PASS = {m: True for m in WAVE1_METRICS}


def fabozzi_terms(**overrides: object) -> SecurityTermsInput:
    """Fabozzi-style fixture bond: 10% semiannual, 5y to maturity at settlement.

    Coupon rate is the PUBLISHED convention: percent of par per annum (the
    serving surface renders ``coupon_rate::text || '%'``).
    """
    base: dict[str, object] = {
        "security_id": SEC,
        "coupon_type": "fixed",
        "coupon_rate": Decimal("10.0"),
        "maturity_date": date(2030, 1, 1),
        "day_count": "30/360 US",
        "coupon_schedule": [
            {"date": "2025-07-01", "rate": 10.0},
            {"date": "2026-01-01", "rate": 10.0},
        ],
        "call_schedule": None,
    }
    base.update(overrides)
    return SecurityTermsInput(**base)  # type: ignore[arg-type]


def price(value: object, observation_date: date = AS_OF) -> EligiblePrice:
    return EligiblePrice(price=value, observation_date=observation_date)


def by_metric(rows):
    out = {r.metric_id: r for r in rows}
    assert len(out) == len(rows)  # exactly one row per metric
    return out


# --------------------------------------------------------------------------- #
# Wave-1 metric set (Global Constraints 1-2: never OAS/z-spread/duration)
# --------------------------------------------------------------------------- #

def test_wave1_metric_set_is_exactly_the_four_owner_qualified_metrics() -> None:
    assert WAVE1_METRICS == ("security_ytm", "security_ytw", "current_yield", "wal")
    assert PRICE_CONSUMING_METRICS == frozenset(
        {"security_ytm", "security_ytw", "current_yield"}
    )
    forbidden = ("oas", "zspread", "z_spread", "duration", "carry", "rolldown")
    for metric in WAVE1_METRICS:
        for token in forbidden:
            assert token not in metric


def test_runner_module_is_db_free() -> None:
    source = (
        Path(__file__).resolve().parents[2]
        / "src" / "bonds" / "metrics_engine_runner.py"
    ).read_text(encoding="utf-8")
    assert "psycopg" not in source
    assert "import os" not in source  # no env reads: fully deterministic glue


# --------------------------------------------------------------------------- #
# Happy path: Fabozzi fixture bond -> YTM ~ 11% (authoritative published sample)
# --------------------------------------------------------------------------- #

def test_happy_path_ytm_matches_the_fabozzi_published_sample() -> None:
    rows = by_metric(
        compute_security_metrics(fabozzi_terms(), price(Decimal("96.23")),
                                 as_of=AS_OF, gate_passed=ALL_PASS)
    )
    ytm = rows["security_ytm"]
    assert ytm.status == STATUS_AVAILABLE
    assert ytm.engine_error_code is None
    assert ytm.value == pytest.approx(0.11, abs=1e-3)
    assert ytm.as_of == AS_OF
    assert ytm.security_id == SEC


def test_happy_path_full_row_set_ytw_current_yield_and_wal() -> None:
    rows = by_metric(
        compute_security_metrics(fabozzi_terms(), price(Decimal("96.23")),
                                 as_of=AS_OF, gate_passed=ALL_PASS)
    )
    assert set(rows) == set(WAVE1_METRICS)
    # No calls -> YTW == YTM (maturity is the worst scenario).
    assert rows["security_ytw"].status == STATUS_AVAILABLE
    assert rows["security_ytw"].value == pytest.approx(rows["security_ytm"].value)
    # Current yield = annual coupon / clean price (fractions per annum).
    assert rows["current_yield"].value == pytest.approx(10.0 / 96.23, rel=1e-9)
    # Bullet WAL = ACT/365F years from as_of to maturity.
    expected_wal = (date(2030, 1, 1) - AS_OF).days / 365.0
    assert rows["wal"].value == pytest.approx(expected_wal, rel=1e-9)
    for row in rows.values():
        assert row.status == STATUS_AVAILABLE
        assert row.value is not None and math.isfinite(row.value)


def test_callable_bond_ytw_is_the_engine_worst_scenario() -> None:
    terms = fabozzi_terms(
        call_schedule=[{"call_date": "2027-01-01", "call_price": 100.0}]
    )
    rows = by_metric(
        compute_security_metrics(terms, price(Decimal("105.0")),
                                 as_of=AS_OF, gate_passed=ALL_PASS)
    )
    engine_terms = cashflows.BondTerms(
        issue_date=date(2025, 1, 1), maturity_date=date(2030, 1, 1),
        coupon_rate=0.10, frequency=cashflows.Frequency.SEMIANNUAL,
        day_count=cashflows.DayCount.THIRTY_360_US,
        call_schedule=(cashflows.CallOption(date(2027, 1, 1), 100.0),),
    )
    schedule = cashflows.generate_schedule(engine_terms)
    expected = pricing.yield_to_worst(schedule, AS_OF, 105.0)
    assert rows["security_ytw"].value == pytest.approx(expected.ytw, rel=1e-12)
    assert rows["security_ytm"].value == pytest.approx(expected.to_maturity, rel=1e-12)
    # Premium callable: worst is the call, strictly below yield-to-maturity.
    assert rows["security_ytw"].value < rows["security_ytm"].value


def test_zero_coupon_bond_prices_and_has_zero_current_yield() -> None:
    terms = fabozzi_terms(
        coupon_type="zero", coupon_rate=None, day_count="ACT/365F",
        coupon_schedule=None,
    )
    rows = by_metric(
        compute_security_metrics(terms, price(Decimal("78.12")),
                                 as_of=AS_OF, gate_passed=ALL_PASS)
    )
    engine_terms = cashflows.BondTerms(
        issue_date=AS_OF, maturity_date=date(2030, 1, 1), coupon_rate=0.0,
        frequency=cashflows.Frequency.SEMIANNUAL,
        day_count=cashflows.DayCount.ACT_365F,
    )
    schedule = cashflows.generate_schedule(engine_terms)
    expected = pricing.yield_to_maturity(schedule, AS_OF, 78.12)
    assert rows["security_ytm"].value == pytest.approx(expected, rel=1e-12)
    # A zero-coupon bond's current yield is genuinely 0 (no coupon income).
    assert rows["current_yield"].status == STATUS_AVAILABLE
    assert rows["current_yield"].value == 0.0
    assert rows["wal"].value == pytest.approx((date(2030, 1, 1) - AS_OF).days / 365.0)


# --------------------------------------------------------------------------- #
# Gate honesty (fail-closed): a metric that does not pass carries NULL value
# --------------------------------------------------------------------------- #

def test_gated_metric_rows_carry_gate_not_passed_and_null_value() -> None:
    gate = dict(ALL_PASS)
    gate["security_ytw"] = False
    rows = by_metric(
        compute_security_metrics(fabozzi_terms(), price(Decimal("96.23")),
                                 as_of=AS_OF, gate_passed=gate)
    )
    assert rows["security_ytw"].status == STATUS_GATE_NOT_PASSED
    assert rows["security_ytw"].value is None
    assert rows["security_ytw"].engine_error_code is None
    assert rows["security_ytm"].status == STATUS_AVAILABLE


def test_gate_missing_from_the_map_fails_closed() -> None:
    rows = by_metric(
        compute_security_metrics(fabozzi_terms(), price(Decimal("96.23")),
                                 as_of=AS_OF, gate_passed={})
    )
    for metric in WAVE1_METRICS:
        assert rows[metric].status == STATUS_GATE_NOT_PASSED
        assert rows[metric].value is None


def test_gate_precedes_every_other_status() -> None:
    # Gated AND priceless AND terms-insufficient -> the gate wins (honest order).
    terms = fabozzi_terms(coupon_type="floating")
    rows = by_metric(
        compute_security_metrics(terms, None, as_of=AS_OF,
                                 gate_passed={m: False for m in WAVE1_METRICS})
    )
    for metric in WAVE1_METRICS:
        assert rows[metric].status == STATUS_GATE_NOT_PASSED


# --------------------------------------------------------------------------- #
# No eligible price (price-consuming metrics only; WAL is schedule-only)
# --------------------------------------------------------------------------- #

def test_missing_price_yields_no_eligible_price_but_wal_still_computes() -> None:
    rows = by_metric(
        compute_security_metrics(fabozzi_terms(), None,
                                 as_of=AS_OF, gate_passed=ALL_PASS)
    )
    for metric in PRICE_CONSUMING_METRICS:
        assert rows[metric].status == STATUS_NO_ELIGIBLE_PRICE
        assert rows[metric].value is None
        assert rows[metric].engine_error_code is None
    assert rows["wal"].status == STATUS_AVAILABLE
    assert rows["wal"].value == pytest.approx((date(2030, 1, 1) - AS_OF).days / 365.0)


# --------------------------------------------------------------------------- #
# Terms insufficiency (typed reason codes; never a guessed default)
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"coupon_type": "floating"}, "coupon_type_unsupported"),
        ({"coupon_type": None}, "coupon_type_missing"),
        ({"maturity_date": None}, "maturity_date_missing"),
        ({"maturity_date": "not-a-date"}, "maturity_date_invalid"),
        ({"coupon_rate": None}, "coupon_rate_missing"),
        ({"coupon_rate": "ten"}, "coupon_rate_invalid"),
        ({"day_count": None}, "day_count_missing"),
        ({"day_count": "ACT/252"}, "day_count_unsupported"),
        ({"coupon_schedule": None}, "coupon_schedule_missing"),
        ({"coupon_schedule": []}, "coupon_schedule_missing"),
        ({"coupon_schedule": "not-a-list"}, "coupon_schedule_invalid"),
        (
            {"coupon_schedule": [{"date": "2026-01-01"}]},
            "coupon_frequency_underivable",
        ),
        (
            {
                "coupon_schedule": [
                    {"date": "2025-08-01"},
                    {"date": "2026-01-01"},
                ]
            },
            "coupon_frequency_underivable",  # 5-month gap: not a supported frequency
        ),
        (
            {"call_schedule": [{"call_date": "2027-01-01"}]},  # price missing
            "call_schedule_invalid",
        ),
        (
            {"coupon_type": "zero", "coupon_schedule": None},
            "coupon_type_conflicts_rate",  # zero with a reported non-zero coupon
        ),
    ],
)
def test_insufficient_terms_are_typed_per_field(overrides, code) -> None:
    rows = by_metric(
        compute_security_metrics(fabozzi_terms(**overrides),
                                 price(Decimal("96.23")),
                                 as_of=AS_OF, gate_passed=ALL_PASS)
    )
    for metric in WAVE1_METRICS:
        assert rows[metric].status == STATUS_TERMS_INSUFFICIENT, metric
        assert rows[metric].engine_error_code == code
        assert rows[metric].value is None


def test_price_status_precedes_terms_for_price_consuming_metrics() -> None:
    # Insufficient terms AND no price: the price gate is reported for the price
    # metrics (their input pipeline stops earlier); WAL reports the terms gap.
    rows = by_metric(
        compute_security_metrics(fabozzi_terms(coupon_type="floating"), None,
                                 as_of=AS_OF, gate_passed=ALL_PASS)
    )
    for metric in PRICE_CONSUMING_METRICS:
        assert rows[metric].status == STATUS_NO_ELIGIBLE_PRICE
    assert rows["wal"].status == STATUS_TERMS_INSUFFICIENT
    assert rows["wal"].engine_error_code == "coupon_type_unsupported"


# --------------------------------------------------------------------------- #
# Engine typed errors surface as engine_typed_error + code (never NaN)
# --------------------------------------------------------------------------- #

def test_front_stub_is_a_typed_engine_error() -> None:
    # Reported coupon dates sit OFF the maturity-anchored grid: the engine's own
    # front-stub refusal surfaces as a typed row, never a fabricated schedule.
    terms = fabozzi_terms(
        coupon_schedule=[{"date": "2025-08-15"}, {"date": "2026-02-15"}]
    )
    rows = by_metric(
        compute_security_metrics(terms, price(Decimal("96.23")),
                                 as_of=AS_OF, gate_passed=ALL_PASS)
    )
    for metric in WAVE1_METRICS:
        assert rows[metric].status == STATUS_ENGINE_TYPED_ERROR, metric
        assert rows[metric].engine_error_code == "front_stub_unsupported"
        assert rows[metric].value is None


def test_degenerate_price_is_a_typed_engine_error_and_wal_survives() -> None:
    rows = by_metric(
        compute_security_metrics(fabozzi_terms(), price(Decimal("0")),
                                 as_of=AS_OF, gate_passed=ALL_PASS)
    )
    for metric in PRICE_CONSUMING_METRICS:
        assert rows[metric].status == STATUS_ENGINE_TYPED_ERROR
        assert rows[metric].engine_error_code == "non_positive_price"
        assert rows[metric].value is None
    assert rows["wal"].status == STATUS_AVAILABLE


def test_non_numeric_price_is_a_typed_engine_error() -> None:
    rows = by_metric(
        compute_security_metrics(fabozzi_terms(), price("ninety-six"),
                                 as_of=AS_OF, gate_passed=ALL_PASS)
    )
    for metric in PRICE_CONSUMING_METRICS:
        assert rows[metric].status == STATUS_ENGINE_TYPED_ERROR
        assert rows[metric].engine_error_code == "invalid_price_input"


def test_matured_bond_is_a_typed_engine_error_for_wal() -> None:
    terms = fabozzi_terms(
        maturity_date=date(2024, 1, 1),
        coupon_schedule=[{"date": "2023-07-01"}, {"date": "2024-01-01"}],
    )
    rows = by_metric(
        compute_security_metrics(terms, None, as_of=AS_OF, gate_passed=ALL_PASS)
    )
    assert rows["wal"].status == STATUS_ENGINE_TYPED_ERROR
    assert rows["wal"].engine_error_code == "settlement_after_maturity"


# --------------------------------------------------------------------------- #
# Structural honesty + determinism
# --------------------------------------------------------------------------- #

def test_value_present_iff_available_and_always_finite() -> None:
    cases = [
        (fabozzi_terms(), price(Decimal("96.23")), ALL_PASS),
        (fabozzi_terms(), None, ALL_PASS),
        (fabozzi_terms(coupon_type="floating"), price(Decimal("96.23")), ALL_PASS),
        (fabozzi_terms(), price(Decimal("96.23")), {m: False for m in WAVE1_METRICS}),
        (fabozzi_terms(), price(Decimal("0")), ALL_PASS),
    ]
    for terms, quote, gate in cases:
        for row in compute_security_metrics(terms, quote, as_of=AS_OF, gate_passed=gate):
            if row.status == STATUS_AVAILABLE:
                assert row.value is not None and math.isfinite(row.value)
            else:
                assert row.value is None
            code_bearing = row.status in (
                STATUS_ENGINE_TYPED_ERROR, STATUS_TERMS_INSUFFICIENT
            )
            assert (row.engine_error_code is not None) == code_bearing


def test_same_inputs_produce_identical_rows() -> None:
    first = compute_security_metrics(fabozzi_terms(), price(Decimal("96.23")),
                                     as_of=AS_OF, gate_passed=ALL_PASS)
    second = compute_security_metrics(fabozzi_terms(), price(Decimal("96.23")),
                                      as_of=AS_OF, gate_passed=ALL_PASS)
    assert first == second
    assert [r.metric_id for r in first] == list(WAVE1_METRICS)
