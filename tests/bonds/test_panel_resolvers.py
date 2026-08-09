from datetime import date
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from src.bonds.panel_resolvers import (
    analytical_mod_dur,
    analytical_ytm,
    apply_typed_exits,
    bond_coupons,
    build_universe_snapshot,
    build_db_monthly_panel,
    compute_spread,
    eligibility,
    fuse_live_panel,
    monthly_treasury_curve,
    ratings_static_mapping,
    fit_month,
    monthly_returns,
)


def test_spread_is_ytm_minus_interpolated_treasury_never_oas() -> None:
    daily = pd.DataFrame({"DGS5": [4.0], "DGS10": [4.4]}, index=pd.to_datetime(["2024-01-02"]))
    curve = monthly_treasury_curve(daily)
    panel = pd.DataFrame({"month": [pd.Timestamp("2024-01-01")], "ytm": [0.054], "bond_maturity": [10.0]})
    result = compute_spread(panel, curve)
    assert result.name == "spread_final"
    assert result.iloc[0] == pytest.approx(0.010)


def test_eligibility_has_a_typed_reason_without_inferring_distribution_rule() -> None:
    rows = pd.DataFrame([
        {"cusip_id": "OK", "ytm": .05, "mod_dur": 5., "pr": 100., "amt_outstanding_k": 300_000,
         "bond_maturity": 2., "traded_days": 5, "issuer_id": "issuer", "ff17num": 10, "db_type": 1, "currency": "USD", "asset_class": "corporate"},
        {"cusip_id": "SECTOR", "ytm": .05, "mod_dur": 5., "pr": 100., "amt_outstanding_k": 300_000,
         "bond_maturity": 2., "traded_days": 5, "issuer_id": "issuer", "ff17num": np.nan, "db_type": 1, "currency": "USD", "asset_class": "corporate"},
        {"cusip_id": "DBTYPE", "ytm": .05, "mod_dur": 5., "pr": 100., "amt_outstanding_k": 300_000,
         "bond_maturity": 2., "traded_days": 5, "issuer_id": "issuer", "ff17num": 10, "db_type": np.nan, "currency": "USD", "asset_class": "corporate"},
        {"cusip_id": "144A", "ytm": .05, "mod_dur": 5., "pr": 100., "amt_outstanding_k": 300_000,
         "bond_maturity": 2., "traded_days": 5, "issuer_id": "issuer", "ff17num": 10, "db_type": 3, "currency": "USD", "asset_class": "corporate"},
    ])
    assert eligibility(rows).tolist() == ["eligible", "missing_sector", "eligible", "eligible"]
    snapshot = build_universe_snapshot(rows)
    assert snapshot[["cusip_id", "eligibility_state", "eligibility_reason"]].to_dict("records") == [
        {"cusip_id": "OK", "eligibility_state": "included", "eligibility_reason": "eligible"},
        {"cusip_id": "SECTOR", "eligibility_state": "excluded", "eligibility_reason": "missing_sector"},
        {"cusip_id": "DBTYPE", "eligibility_state": "included", "eligibility_reason": "eligible"},
        {"cusip_id": "144A", "eligibility_state": "included", "eligibility_reason": "eligible"},
    ]


def test_eligibility_distinguishes_absent_currency_and_asset_class() -> None:
    base = {
        "ytm": .05, "mod_dur": 5., "pr": 100., "amt_outstanding_k": 300_000,
        "bond_maturity": 2., "traded_days": 5, "issuer_id": "issuer", "ff17num": 10, "db_type": 1,
    }
    rows = pd.DataFrame([
        {**base, "currency": None, "asset_class": "corporate"},
        {**base, "currency": "USD", "asset_class": "missing"},
        {**base, "currency": "EUR", "asset_class": "corporate"},
        {**base, "currency": "USD", "asset_class": "government"},
    ])

    assert eligibility(rows).tolist() == [
        "missing_currency", "missing_asset_class", "non_usd", "noncorporate"
    ]


def test_static_rating_mapping_is_neutral_and_marks_absent_or_stale() -> None:
    mapping = pd.DataFrame({"cusip_id": ["AAA"], "rating_bucket": ["BBB"],
                            "rating_as_of_month": pd.to_datetime(["2023-01-01"]), "rating_state": ["static_carry_forward"], "rating_reason": ["frozen_pack_static"]})
    targets = pd.DataFrame({"cusip_id": ["AAA"], "month": pd.to_datetime(["2024-02-01"])})
    pit = ratings_static_mapping(mapping, pd.concat([targets, pd.DataFrame({"cusip_id": ["NONE"], "month": pd.to_datetime(["2024-02-01"])})]))
    assert set(pit.columns).isdisjoint({"agency"})
    assert pit.set_index("cusip_id")["rating_bucket"].to_dict() == {"AAA": "BBB", "NONE": "NR"}
    assert pit.set_index("cusip_id").loc["NONE", "rating_reason"] == "static_rating_absent"


def test_static_rating_mapping_refuses_a_rating_dated_after_the_target_month() -> None:
    mapping = pd.DataFrame({
        "cusip_id": ["FUTURE", "CURRENT"],
        "rating_bucket": ["A", "BBB"],
        "rating_as_of_month": pd.to_datetime(["2024-02-01", "2024-01-01"]),
        "rating_state": ["static_carry_forward", "static_current"],
    })
    targets = pd.DataFrame({
        "cusip_id": ["FUTURE", "CURRENT"],
        "month": pd.to_datetime(["2024-01-01", "2024-01-01"]),
    })

    rows = ratings_static_mapping(mapping, targets).set_index("cusip_id")

    assert rows.loc["FUTURE", "rating_bucket"] == "NR"
    assert rows.loc["FUTURE", "rating_state"] == "static_missing"
    assert rows.loc["FUTURE", "rating_reason"] == "static_rating_future"
    assert pd.isna(rows.loc["FUTURE", "rating_as_of_month"])
    assert pd.isna(rows.loc["FUTURE", "rating_staleness_months"])
    assert rows.loc["CURRENT", "rating_state"] == "static_current"
    assert rows.loc["CURRENT", "rating_staleness_months"] == 0


def test_analytical_inversions_keep_fraction_yield_and_year_duration_units() -> None:
    y = analytical_ytm(pd.Series([100.0]), pd.Series([5.0]), pd.Series([5.0]))
    d = analytical_mod_dur(y, pd.Series([5.0]), pd.Series([10.0]))
    assert y.iloc[0] == pytest.approx(.05, abs=1e-6)
    assert d.iloc[0] == pytest.approx(7.79, abs=.05)


def test_live_fuse_declares_analytical_ytm_duration_and_structural_basis() -> None:
    base = pd.DataFrame({"cusip_id": ["AAA"], "month": pd.to_datetime(["2025-03-01"]), "pr": [100.], "ytm": [.05],
                         "mod_dur": [5.], "bond_maturity": [7.], "amt_outstanding_k": [500_000.], "ff17num": [10], "db_type": [1]})
    tail = pd.DataFrame({"cusip_id": ["AAA"], "month": pd.to_datetime(["2025-04-01"]), "pr": [100.], "ytm": [np.nan], "coupon_pct": [5.], "maturity_date": pd.to_datetime(["2030-04-01"])})
    fused = fuse_live_panel(base, pd.DataFrame(), tail)
    row = fused.iloc[-1]
    assert row["price_source"] == "finnhub"
    assert row["ytm_basis"] == "analytical"
    assert row["mod_dur_source"] == "analytical"
    assert row["structural_basis"] == "carry_forward"
    assert row["bond_maturity"] == pytest.approx(5.0, abs=.01)


def test_typed_exits_are_matured_distressed_or_last_price_flat() -> None:
    attrs = pd.DataFrame({"cusip_id": ["MATURED", "DISTRESSED", "UNKNOWN"],
                          "bond_maturity": [1., 5., 5.], "pr": [80., 60., 100.],
                          "rating_bucket": ["A", "A", "A"], "ytm": [.06, .08, .05]})
    realized, exited, reasons = apply_typed_exits(np.array([np.nan, np.nan, np.nan]), attrs, np.array([True, True, True]), return_reasons=True)
    matured_carry = bond_coupons(attrs.iloc[[0]]).iloc[0] / 12
    assert reasons.tolist() == ["matured", "distressed", "unexplained"]
    assert realized.tolist() == pytest.approx([(100. + matured_carry - 80.) / 80., (40. - 60.) / 60., 0.])
    assert exited.tolist() == [True, True, True]


def test_spread_model_clusters_by_resolved_issuer_not_cusip6(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = pd.DataFrame({
        "cusip_id": [f"CUS{i:06d}" for i in range(300)], "issuer_id": [f"issuer-{i % 3}" for i in range(300)],
        "month": pd.Timestamp("2024-01-01"), "spread_final": np.linspace(.01, .03, 300), "bond_maturity": np.linspace(2., 8., 300),
        "amt_outstanding_k": [Decimal(300_000 + index * 1_000) for index in range(300)],
        "dollar_volume": [Decimal(1 + index) for index in range(300)],
        "db_type": 1, "rating_bucket": "A", "ff17num": 10,
    })
    observed: dict[str, object] = {}

    class FakeFit:
        rsquared = .5
        def predict(self, x: pd.DataFrame) -> np.ndarray:
            return np.zeros(len(x))

    class FakeModel:
        def fit(self, **kwargs: object) -> FakeFit:
            observed.update(kwargs)
            return FakeFit()

    def fake_ols(_endog, exog, **_kwargs):
        observed["exog_columns"] = list(exog.columns)
        return FakeModel()

    monkeypatch.setattr("src.bonds.panel_resolvers.sm.OLS", fake_ols)
    fit_month(rows)
    assert list(observed["cov_kwds"]["groups"]) == rows["issuer_id"].tolist()
    assert "is_144a" not in observed["exog_columns"]


def test_spread_model_rejects_future_rows_when_an_asof_is_declared() -> None:
    rows = pd.DataFrame({
        "cusip_id": [f"CUS{i:06d}" for i in range(300)], "issuer_id": ["issuer"] * 300,
        "month": pd.Timestamp("2024-02-01"), "spread_final": .02, "bond_maturity": 5.,
        "amt_outstanding_k": 500_000., "dollar_volume": 1., "db_type": 1, "rating_bucket": "A", "ff17num": 10,
    })
    with pytest.raises(ValueError, match="future"):
        fit_month(rows, as_of=pd.Timestamp("2024-01-31"))


def test_monthly_returns_persists_typed_terminal_exit_rows() -> None:
    panel = pd.DataFrame({"cusip_id": ["AAA"], "month": pd.to_datetime(["2024-01-01"]), "pr": [60.], "ytm": [.08], "bond_maturity": [5.]})
    exits = pd.DataFrame({"cusip_id": ["AAA"], "month": pd.to_datetime(["2024-02-01"]), "pr": [60.], "ytm": [.08], "bond_maturity": [5.], "rating_bucket": ["D"]})
    output = monthly_returns(panel, terminal_exits=exits)
    row = output.iloc[0]
    assert row["exit_basis"] == "distressed"
    assert row["exit_reason"] == "distressed"
    assert row["total_return"] == pytest.approx((40. - 60.) / 60.)


def test_monthly_returns_prefers_contractual_coupon_over_implied_coupon() -> None:
    panel = pd.DataFrame({
        "cusip_id": ["AAA", "AAA"],
        "month": pd.to_datetime(["2024-01-01", "2024-02-01"]),
        "pr": [80., 80.],
        "ytm": [.12, .12],
        "bond_maturity": [5., 5.],
        "coupon_pct": [6., 6.],
    })

    row = monthly_returns(panel).iloc[0]

    assert row["price_return"] == 0
    assert row["carry_return"] == pytest.approx((6. / 12) / 80.)
    assert row["total_return"] == pytest.approx((6. / 12) / 80.)


def test_matured_exit_prefers_contractual_coupon_when_present() -> None:
    attrs = pd.DataFrame({
        "cusip_id": ["AAA"],
        "bond_maturity": [1.],
        "pr": [80.],
        "ytm": [.12],
        "coupon_pct": [6.],
        "rating_bucket": ["BBB"],
    })

    realized, _exited = apply_typed_exits(np.array([np.nan]), attrs, np.array([True]))

    assert realized[0] == pytest.approx((100. + 6. / 12 - 80.) / 80.)


def test_db_shaped_month_builder_uses_observed_then_analytical_terms_and_one_spread() -> None:
    daily = pd.DataFrame({"cusip9": ["AAA", "AAA"], "day": pd.to_datetime(["2024-01-05", "2024-01-20"]), "price": [100., 100.], "ytm": [np.nan, np.nan], "volume": [np.nan, np.nan]})
    terms = pd.DataFrame({"cusip9": ["AAA"], "coupon_rate": [5.], "maturity_date": pd.to_datetime(["2029-01-20"]), "amount_outstanding_mm": [500], "amount_outstanding_vendor": [np.nan], "amount_outstanding_k": [500_000.]})
    curve = pd.DataFrame({"day": pd.to_datetime(["2024-01-02", "2024-01-02", "2024-01-02"]), "tenor": ["1y", "5y", "10y"], "yield_pct": [4., 4., 4.]})
    sector = pd.DataFrame({"cusip9": ["AAA"], "issuer_id": ["issuer"], "ff17num": [10], "db_type": [1], "db_type_reason": ["pit_present"]})
    liquidity = pd.DataFrame({"cusip9": ["AAA"], "month": [date(2024, 1, 1)], "traded_days": [5], "quoted_days": [2], "rel_bid_ask_bps": [50.], "dollar_volume": [300.], "quote_state": ["quoted"], "reason_code": [None]})
    rows = build_db_monthly_panel(daily, terms, curve, sector, liquidity, pd.DataFrame(), months=[pd.Timestamp("2024-01-01")])
    row = rows.iloc[0]
    assert row["ytm_basis"] == "analytical"
    assert row["mod_dur_source"] == "analytical"
    assert row["spread_definition"] == "ytm_minus_interpolated_dgs"
    assert row["rating_bucket"] == "NR"
    assert row["dollar_volume"] == pytest.approx(300.)
    assert row["amt_outstanding_k"] == pytest.approx(500_000.)


def test_db_month_builder_does_not_treat_unproven_vendor_amount_as_millions() -> None:
    terms = pd.DataFrame({
        "cusip9": ["AAA"],
        "coupon_rate": [5.],
        "maturity_date": pd.to_datetime(["2029-01-20"]),
        "amount_outstanding_mm": [525.],
        "amount_outstanding_vendor": [525.],
    })
    sector = pd.DataFrame({
        "cusip9": ["AAA"], "issuer_id": ["issuer"], "ff17num": [10],
        "db_type": [1], "currency": ["USD"], "asset_class": ["corporate"],
    })

    rows = build_db_monthly_panel(
        pd.DataFrame(), terms, pd.DataFrame(), sector, pd.DataFrame(), pd.DataFrame(),
        months=[pd.Timestamp("2024-01-01")],
    )

    assert pd.isna(rows.loc[0, "amt_outstanding_k"])


def test_db_month_builder_requires_explicit_normalized_amount_even_without_vendor_copy() -> None:
    terms = pd.DataFrame({
        "cusip9": ["AAA"],
        "coupon_rate": [5.],
        "maturity_date": pd.to_datetime(["2029-01-20"]),
        "amount_outstanding_mm": [525.],
        "amount_outstanding_vendor": [np.nan],
    })
    sector = pd.DataFrame({
        "cusip9": ["AAA"], "issuer_id": ["issuer"], "ff17num": [10],
        "db_type": [1], "currency": ["USD"], "asset_class": ["corporate"],
    })

    rows = build_db_monthly_panel(
        pd.DataFrame(), terms, pd.DataFrame(), sector, pd.DataFrame(), pd.DataFrame(),
        months=[pd.Timestamp("2024-01-01")],
    )

    assert pd.isna(rows.loc[0, "amt_outstanding_k"])


def test_db_month_builder_uses_month_specific_identity_and_db_type_rows() -> None:
    months = pd.to_datetime(["2024-01-01", "2024-02-01"])
    sector = pd.DataFrame({
        "cusip9": ["AAA", "AAA"],
        "month": months,
        "issuer_id": ["issuer-old", "issuer-new"],
        "ff17num": [10, 20],
        "db_type": [1, 3],
        "currency": ["USD", "USD"],
        "asset_class": ["corporate", "corporate"],
    })
    terms = pd.DataFrame({
        "cusip9": ["AAA"],
        "coupon_rate": [5.],
        "maturity_date": pd.to_datetime(["2029-01-20"]),
        "amount_outstanding_mm": [500.],
        "amount_outstanding_vendor": [np.nan],
        "amount_outstanding_k": [500_000.],
    })

    rows = build_db_monthly_panel(
        pd.DataFrame(), terms, pd.DataFrame(),
        sector, pd.DataFrame(), pd.DataFrame(), months=list(months),
    )

    assert rows[["month", "issuer_id", "db_type"]].to_dict("records") == [
        {"month": months[0], "issuer_id": "issuer-old", "db_type": 1},
        {"month": months[1], "issuer_id": "issuer-new", "db_type": 3},
    ]


def test_db_month_builder_keeps_unobserved_candidates_for_typed_exclusion() -> None:
    daily = pd.DataFrame({
        "cusip9": ["OBSERVED"],
        "day": pd.to_datetime(["2024-01-05"]),
        "price": [100.],
        "ytm": [.05],
        "volume": [1.],
    })
    terms = pd.DataFrame({
        "cusip9": ["OBSERVED", "MISSING"],
        "coupon_rate": [5., 5.],
        "maturity_date": pd.to_datetime(["2029-01-20", "2029-01-20"]),
        "amount_outstanding_mm": [500, 500],
        "amount_outstanding_k": [500_000., 500_000.],
    })
    curve = pd.DataFrame({
        "day": pd.to_datetime(["2024-01-02", "2024-01-02"]),
        "tenor": ["1y", "10y"],
        "yield_pct": [4., 4.],
    })
    sector = pd.DataFrame({
        "cusip9": ["OBSERVED", "MISSING"],
        "issuer_id": ["issuer-1", "issuer-2"],
        "ff17num": [10, 10],
        "db_type": [1, 1],
        "db_type_reason": ["pit_present", "pit_present"],
        "currency": ["USD", "USD"],
        "asset_class": ["corporate", "corporate"],
    })

    rows = build_db_monthly_panel(
        daily,
        terms,
        curve,
        sector,
        pd.DataFrame(),
        pd.DataFrame(),
        months=[pd.Timestamp("2024-01-01")],
    )
    snapshot = build_universe_snapshot(rows).set_index("cusip_id")

    assert set(snapshot.index) == {"OBSERVED", "MISSING"}
    assert snapshot.loc["MISSING", "eligibility_state"] == "excluded"
    assert snapshot.loc["MISSING", "eligibility_reason"] == "missing_traded_days"
