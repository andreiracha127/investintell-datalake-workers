"""Tests for the market-implied rating product (pure machine + worker + writer).

Three layers, all DB-free:

  * the frozen policy: the declared numbers, the canonical digest and the
    helpers that must never silently change;
  * the state machine over synthetic panel grids: witness, score, market-level
    chain, hysteresis, carry-forward, spells, source exits, D candidate /
    confirmation / cure, and the point-in-time invariant (a month's
    ``implied_bucket`` never changes when the series is truncated after it);
  * the writer/worker: the in-memory mirror of the shared publication ledger
    (idempotent replay, compare-and-set pointer) and ``run``'s typed states.

The DDL CHECKs are also asserted in Python over the synthetic output, so a
state that PostgreSQL would reject cannot ship unnoticed. The database-level
materializer test lives in ``tests/test_bond_market_implied_rating_materializer_db.py``
(skipped without ``SEC_TEST_DATABASE_URL``).
"""
from __future__ import annotations

import logging
import math
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.bonds import implied_rating as ir
from src.bonds.errors import BondError
from src.bonds.implied_rating_materializer import (
    InMemoryPublicationStore,
    ImpliedRatingPublication,
    build_fingerprint,
    publication_id_for,
)
from src.workers import bond_market_implied_rating as worker

MONTHS = pd.date_range("2025-01-01", periods=8, freq="MS")
MATURITY_FAR = date(2035, 1, 1)


def bond_rows(
    cusip: str,
    spreads: list[float],
    *,
    prices: list[float] | None = None,
    months: pd.DatetimeIndex | None = None,
    mod_dur: float = 5.0,
    maturity: date = MATURITY_FAR,
    trade_count: int = 10,
    dollar_volume: float = 1_000_000.0,
) -> list[dict[str, object]]:
    index = months if months is not None else MONTHS
    values = prices if prices is not None else [95.0] * len(spreads)
    return [
        {
            "cusip_id": cusip,
            "month": month,
            "price": price,
            "spread_final_bps": spread,
            "mod_dur": mod_dur,
            "trade_count": trade_count,
            "dollar_volume": dollar_volume,
            "maturity_date": maturity,
        }
        for month, spread, price in zip(index, spreads, values, strict=True)
    ]


def market_fillers(
    count: int = 2, spread: float = 300.0, *, months: pd.DatetimeIndex | None = None
) -> list[dict[str, object]]:
    """Flat bonds that keep the median intersection delta at zero.

    The market-level chain is the MEDIAN of the witness-set delta; two flat
    bonds outvote one moving bond, so a test CUSIP's neutralized score collapses
    to ``log(spread)`` and bucket expectations can be read off the policy cuts.
    """
    index = months if months is not None else MONTHS
    return [
        row
        for position in range(count)
        for row in bond_rows(f"F{position}", [spread] * len(index), months=index)
    ]


def build(frame: pd.DataFrame, *, months: pd.DatetimeIndex | None = None) -> pd.DataFrame:
    index = months if months is not None else MONTHS
    return ir.build_publication_rows(frame, last_closed_month=index[-1])


def slice_of(rows: pd.DataFrame, cusip: str) -> pd.DataFrame:
    return rows.loc[rows["cusip_id"].eq(cusip)].reset_index(drop=True)


def assert_ddl_invariants(rows: pd.DataFrame) -> None:
    for record in rows.to_dict(orient="records"):
        assert record["implied_bucket"] in ir.BUCKET_ORDER
        assert record["censoring"] in ir.CENSORING_KINDS
        assert int(record["spell_id"]) >= 1
        assert int(record["carry_months"]) >= 0
        assert (record["implied_bucket"] == "D") == bool(record["d_confirmed"])
        if record["d_confirmed"]:
            assert record["d_event_month"] is not None
        else:
            assert record["d_event_month"] is None
        if record["implied_bucket"] in ir.RATED_BUCKETS:
            assert bool(record["witnessed"]) == (int(record["carry_months"]) == 0)
        assert pd.isna(record["spread_norm_log"]) == (not bool(record["witnessed"]))
        assert pd.isna(record["neutralized_score"]) == (not bool(record["witnessed"]))


# --------------------------------------------------------------------------- #
# The frozen policy
# --------------------------------------------------------------------------- #
def test_policy_matches_the_declared_round() -> None:
    assert ir.POLICY_VERSION == "bond_market_implied_rating_policy_v1"
    assert ir.POLICY["cuts_bps"] == ["60", "85", "125", "220", "380", "700"]
    assert ir.POLICY["spread"]["winsor_bps"] == ["5", "5000"]
    assert ir.POLICY["spread"]["duration_slope_b"] == "0.2"
    assert ir.POLICY["market_level"]["beta"] == "0.6"
    assert ir.POLICY["market_level"]["anchor"]["window_months"] == "36"
    assert ir.POLICY["hysteresis"] == {"delta_log": "0.10", "confirm_obs": "2", "h_months": "3"}
    assert ir.POLICY["carry_forward_k"] == "3"
    assert ir.POLICY["default"] == {
        "p_d": "50", "s_d_bps": "2000", "p_hard": "35",
        "hard_price_confirmation": "standalone_immediate",
        "h_d": "3", "p_cure": "80", "n_cure": "3",
    }
    assert ir.POLICY["calibration"]["lambda_floor"] == "20"
    assert ir.POLICY["buckets"] == list(ir.BUCKET_ORDER)
    assert list(ir.RATED_BUCKETS) == ["AAA", "AA", "A", "BBB", "BB", "B", "CCC"]


def test_policy_digest_is_canonical_and_stable() -> None:
    import hashlib
    import json

    recomputed = hashlib.sha256(
        json.dumps(
            {"policy_version": ir.POLICY_VERSION, "policy": dict(ir.POLICY)},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    assert ir.POLICY_DIGEST == recomputed
    assert len(ir.POLICY_DIGEST) == 64
    assert set(ir.POLICY_DIGEST) <= set("0123456789abcdef")
    # The declaration freezes the digest BEFORE the round runs: the document and
    # the code must not drift apart, and the pre-owner-decision digest must be
    # gone (it pinned hard-price confirmations to an active episode).
    declaration = (
        Path(__file__).resolve().parents[1]
        / "docs" / "calibration" / "bond_market_implied_rating_round_declaration.md"
    ).read_text(encoding="utf-8")
    assert ir.POLICY_DIGEST in declaration
    # The pre-owner-decision digest survives only as a tombstone, never as the
    # declared identity.
    superseded = "a23fd5115cd66abf54986b5189724d1f9497c3f8594c7ecf35855c5ca256983b"
    assert declaration.count(superseded) == 1
    assert "pre-decision evidence" in declaration


def test_bucket_cuts_are_the_logs_of_the_declared_bps_cuts() -> None:
    assert ir.bucket_cuts_log() == tuple(
        math.log(float(cut)) for cut in ir.POLICY["cuts_bps"]
    )
    assert ir.bucket_for_score(math.log(60) - 1e-9) == "AAA"
    assert ir.bucket_for_score(math.log(60)) == "AA"
    assert ir.bucket_for_score(math.log(700)) == "CCC"
    with pytest.raises(ValueError):
        ir.bucket_for_score(float("nan"))


# --------------------------------------------------------------------------- #
# Witness, score, market level
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "overrides",
    (
        {"trade_count": 2},
        {"dollar_volume": 249_999.0},
        {"price": 0.99},
        {"price": 200.01},
        {"price": None},
        {"mod_dur": 0.49},
        {"spread_final_bps": None},
    ),
)
def test_witness_mask_requires_every_declared_condition(overrides) -> None:
    row = bond_rows("W1", [150.0], months=MONTHS[:1])[0]
    row.update(overrides)
    frame = pd.DataFrame([row])
    assert not bool(ir.witness_mask(frame).iloc[0])
    assert pd.isna(ir.normalized_spread(frame).iloc[0])


def test_witness_mask_admits_the_declared_baseline() -> None:
    frame = pd.DataFrame(bond_rows("W1", [150.0], months=MONTHS[:1]))
    assert bool(ir.witness_mask(frame).iloc[0])


def test_normalized_spread_winsorizes_and_adjusts_for_duration() -> None:
    frame = pd.DataFrame([
        bond_rows("A", [3.0], months=MONTHS[:1])[0],
        bond_rows("B", [9000.0], months=MONTHS[:1])[0],
        bond_rows("C", [200.0], months=MONTHS[:1], mod_dur=2.5)[0],
    ])
    scores = ir.normalized_spread(frame)
    assert scores.iloc[0] == pytest.approx(math.log(5.0))
    assert scores.iloc[1] == pytest.approx(math.log(5000.0))
    assert scores.iloc[2] == pytest.approx(math.log(200.0) + 0.2 * math.log(2.0))


def test_market_level_chains_intersection_medians() -> None:
    months = MONTHS[:3]
    frame = pd.DataFrame(
        bond_rows("A", [100.0, 200.0, 200.0], months=months)
        + bond_rows("B", [100.0, 100.0, 100.0], months=months)
        + bond_rows("C", [100.0, None, None], months=months)
    )
    witnessed = ir.witness_mask(frame)
    scores = ir.normalized_spread(frame)
    spread_norm = frame.loc[witnessed, ["cusip_id", "month"]].assign(
        spread_norm_log=scores[witnessed].to_numpy()
    )
    level = ir.market_level(spread_norm)
    first = math.log(200.0) - math.log(100.0)
    # Month 2: A moves (log 2), B is flat -> median of {log2, 0} = log2/2.
    # Month 3: C is gone; A and B are both flat -> 0.
    assert level.iloc[0] == pytest.approx(0.0)
    assert level.iloc[1] == pytest.approx(first / 2.0)
    assert level.iloc[2] == pytest.approx(first / 2.0)


def test_market_level_keeps_the_previous_level_when_nothing_is_shared() -> None:
    months = MONTHS[:2]
    frame = pd.DataFrame(
        bond_rows("A", [100.0], months=months[:1])
        + bond_rows("B", [500.0], months=months[1:])
    )
    witnessed = ir.witness_mask(frame)
    scores = ir.normalized_spread(frame)
    spread_norm = frame.loc[witnessed, ["cusip_id", "month"]].assign(
        spread_norm_log=scores[witnessed].to_numpy()
    )
    level = ir.market_level(spread_norm)
    assert level.iloc[1] == pytest.approx(0.0)


def test_market_anchor_is_the_window_median_and_rejects_an_empty_window() -> None:
    level = pd.Series(
        [1.0, 2.0, 3.0],
        index=pd.to_datetime(["2026-06-01", "2026-07-01", "2026-08-01"]),
    )
    assert ir.market_anchor(level) == pytest.approx(2.0)
    with pytest.raises(ValueError):
        ir.market_anchor(level, window_end_month="2030-01-01")


def test_policy_anchor_pin_is_checked_against_the_closed_history(monkeypatch) -> None:
    level = pd.Series(
        [1.0, 2.0], index=pd.to_datetime(["2026-07-01", "2026-08-01"])
    )
    monkeypatch.setitem(
        ir.POLICY["market_level"]["anchor"], "l_anchor", "1.5"
    )
    assert ir.market_anchor(level) == pytest.approx(1.5)
    monkeypatch.setitem(
        ir.POLICY["market_level"]["anchor"], "l_anchor", "9.5"
    )
    with pytest.raises(ValueError, match="new calibration round"):
        ir.market_anchor(level)


def test_spread_norm_log_is_s_and_neutralized_score_is_the_market_adjustment() -> None:
    """Both layers are published; only x drives the state machine.

    Two bonds rise 20 % a month while the test bond stays flat, so the median
    intersection delta is positive: L climbs away from its window median and
    x = s - beta*(L - anchor) differs from s on every observed month.
    """
    months = MONTHS[:8]
    ratios = [1.2 ** position for position in range(len(months))]
    frame = pd.DataFrame(
        bond_rows("R1", [200.0 * ratio for ratio in ratios], months=months)
        + bond_rows("R2", [250.0 * ratio for ratio in ratios], months=months)
        + bond_rows("X", [300.0] * len(months), months=months)
    )
    rows = slice_of(build(frame, months=months), "X")
    anchor = ir.market_anchor_for_snapshot(frame, last_closed_month=months[-1])
    source = frame.loc[frame["cusip_id"].eq("X")].sort_values("month")
    expected_s = ir.normalized_spread(frame)
    assert rows["spread_norm_log"].to_numpy() == pytest.approx(
        expected_s.loc[source.index].to_numpy()
    )
    recomputed_x = rows["spread_norm_log"] - 0.6 * (rows["market_level_l"] - anchor)
    assert rows["neutralized_score"].to_numpy() == pytest.approx(recomputed_x.to_numpy())
    assert (rows["spread_norm_log"] != rows["neutralized_score"]).all()
    assert rows["market_level_l"].nunique() > 1
    first = rows.iloc[0]
    # L(month 0) == 0 < anchor, so the neutralized score is ABOVE s, and the
    # first bucket is read off x (B), not off s (BB).
    assert first["neutralized_score"] > first["spread_norm_log"]
    assert ir.bucket_for_score(first["spread_norm_log"]) == "BB"
    assert first["implied_bucket"] == "B"
    assert first["implied_bucket"] == ir.bucket_for_score(first["neutralized_score"])


# --------------------------------------------------------------------------- #
# Buckets, hysteresis, carry-forward, spells
# --------------------------------------------------------------------------- #
def test_first_witnessed_month_takes_the_raw_bucket() -> None:
    months = MONTHS[:3]
    frame = pd.DataFrame(
        market_fillers() + bond_rows("X", [300.0] * 3, months=months)
    )
    rows = slice_of(build(frame, months=months), "X")
    assert rows["implied_bucket"].tolist() == ["BB", "BB", "BB"]


def test_hysteresis_needs_the_delta_or_a_second_observation() -> None:
    from collections import deque

    cut_220 = math.log(220.0)
    # A small crossing does not move on the first observation...
    assert ir._hysteresis_bucket(
        bucket="BBB", score=cut_220 + 0.02, history=deque(), month_index=5, h_months=3
    ) == "BBB"
    # ...but the second observation inside the window does.
    history = deque([(4, "BB")])
    assert ir._hysteresis_bucket(
        bucket="BBB", score=cut_220 + 0.02, history=history, month_index=5, h_months=3
    ) == "BB"
    # A crossing with the declared margin moves immediately.
    assert ir._hysteresis_bucket(
        bucket="BBB", score=cut_220 + 0.10, history=deque(), month_index=5, h_months=3
    ) == "BB"
    # Outside the window the old observation no longer confirms.
    history = deque([(0, "BB")])
    assert ir._hysteresis_bucket(
        bucket="BBB", score=cut_220 + 0.02, history=history, month_index=5, h_months=3
    ) == "BBB"
    # An observation on the far side of the target DOES confirm the crossing
    # (CCC is beyond BB), but one on the near side does not.
    history = deque([(4, "CCC")])
    assert ir._hysteresis_bucket(
        bucket="BBB", score=cut_220 + 0.02, history=history, month_index=5, h_months=3
    ) == "BB"
    history = deque([(4, "AAA")])
    assert ir._hysteresis_bucket(
        bucket="BBB", score=cut_220 + 0.02, history=history, month_index=5, h_months=3
    ) == "BBB"


def test_hysteresis_damps_a_single_small_move_end_to_end() -> None:
    months = MONTHS[:5]
    frame = pd.DataFrame(bond_rows("X", [200.0, 230.0, 230.0, 230.0, 230.0], months=months))
    rows = slice_of(build(frame, months=months), "X")
    # The neutralized score of month 2 lands just below BB's margin, so the
    # first move is withheld; the repeated observation in month 3 confirms it.
    assert rows["implied_bucket"].tolist() == ["BBB", "BBB", "BB", "BB", "BB"]


def test_carry_forward_keeps_the_bucket_then_withdraws() -> None:
    months = pd.date_range("2025-01-01", periods=7, freq="MS")
    frame = pd.DataFrame(bond_rows("X", [300.0] * 3, months=months[:3]))
    rows = slice_of(build(frame, months=months), "X")
    buckets = rows["implied_bucket"].tolist()
    assert buckets[:3] == ["BB", "BB", "BB"]
    assert buckets[3:6] == ["BB", "BB", "BB"]
    assert buckets[6] == "WITHDRAWN"
    carried = rows.iloc[3:6]
    assert carried["witnessed"].tolist() == [False, False, False]
    assert carried["carry_months"].tolist() == [1, 2, 3]
    assert rows.iloc[6]["carry_months"] == 4
    assert rows.iloc[6]["censoring"] == "withdrawal_absorbing"
    assert rows["spell_id"].nunique() == 1
    assert_ddl_invariants(rows)


def test_a_new_witness_after_withdrawal_opens_a_new_spell() -> None:
    months = pd.date_range("2025-01-01", periods=9, freq="MS")
    fillers = [
        row
        for index in range(2)
        for row in bond_rows(f"F{index}", [250.0] * len(months), months=months)
    ]
    frame = pd.DataFrame(
        fillers
        + bond_rows("X", [300.0, 300.0], months=months[:2])
        + bond_rows("X", [200.0], months=months[6:7])
    )
    rows = slice_of(build(frame, months=months), "X")
    assert rows["implied_bucket"].tolist() == [
        "BB", "BB", "BB", "BB", "BB", "WITHDRAWN", "BBB", "BBB", "BBB",
    ]
    assert rows["censoring"].iloc[5] == "withdrawal_absorbing"
    resumed = rows.iloc[6]
    assert resumed["witnessed"] and resumed["spell_id"] == 2


def test_never_witnessed_cusip_is_not_rated() -> None:
    months = MONTHS[:3]
    frame = pd.DataFrame(
        market_fillers()
        + bond_rows("X", [300.0] * 3, months=months, trade_count=1)
    )
    rows = slice_of(build(frame, months=months), "X")
    assert rows["implied_bucket"].tolist() == ["NOT_RATED"] * 3
    assert not rows["witnessed"].any()
    assert_ddl_invariants(rows)


def test_a_grid_with_no_witness_at_all_cannot_be_anchored() -> None:
    months = MONTHS[:3]
    frame = pd.DataFrame(bond_rows("X", [300.0] * 3, months=months, trade_count=1))
    with pytest.raises(ValueError, match="no market-level observation"):
        build(frame, months=months)


def test_source_exit_at_par_closes_the_spell_without_carry_rows() -> None:
    months = pd.date_range("2025-01-01", periods=6, freq="MS")
    frame = pd.DataFrame(
        bond_rows("X", [300.0, 300.0], months=months[:2], prices=[95.0, 98.5])
    )
    rows = slice_of(build(frame, months=months), "X")
    assert len(rows) == 2
    assert rows["censoring"].tolist() == ["none", "source_exit"]
    assert rows["implied_bucket"].tolist() == ["BB", "BB"]


def test_source_exit_by_maturity() -> None:
    months = pd.date_range("2025-01-01", periods=4, freq="MS")
    frame = pd.DataFrame(
        bond_rows("X", [300.0, 300.0], months=months[:2], maturity=date(2025, 3, 1))
    )
    rows = slice_of(build(frame, months=months), "X")
    assert rows["censoring"].iloc[-1] == "source_exit"
    assert len(rows) == 2


# --------------------------------------------------------------------------- #
# Defaults: candidate, confirmation, dating, absorption, cure
# --------------------------------------------------------------------------- #
def test_default_confirms_on_the_second_observation_and_keeps_pit_bucket() -> None:
    months = MONTHS[:6]
    frame = pd.DataFrame(market_fillers(months=months) + bond_rows(
        "X", [300.0, 300.0, 2500.0, 2600.0, 2600.0, 2600.0],
        months=months, prices=[95.0, 95.0, 45.0, 44.0, 44.0, 46.0],
    ))
    rows = slice_of(build(frame, months=months), "X")
    # The pre-confirmation months keep the market bucket (the spread jump moved
    # it to CCC); only the confirmation month becomes D.
    assert rows["implied_bucket"].tolist() == ["BB", "BB", "CCC", "D", "D", "D"]
    candidate = rows.iloc[2]
    assert bool(candidate["d_candidate"]) and not bool(candidate["d_confirmed"])
    assert candidate["d_event_month"] is None
    confirmed = rows.iloc[3]
    assert bool(confirmed["d_confirmed"])
    assert confirmed["d_event_month"] == date(2025, 3, 1)
    # The event is dated at the CANDIDATE month and recovery is its price.
    assert confirmed["recovery_observed"] == pytest.approx(45.0)
    assert rows["spell_id"].nunique() == 1
    assert_ddl_invariants(rows)


def test_hard_price_confirms_in_the_candidate_month() -> None:
    months = MONTHS[:4]
    frame = pd.DataFrame(market_fillers(months=months) + bond_rows(
        "X", [300.0, 2400.0, 2500.0, 2500.0], months=months,
        prices=[95.0, 33.0, 33.0, 34.0],
    ))
    rows = slice_of(build(frame, months=months), "X")
    assert rows["implied_bucket"].tolist() == ["BB", "D", "D", "D"]
    assert rows.iloc[1]["d_event_month"] == date(2025, 2, 1)
    assert rows.iloc[1]["recovery_observed"] == pytest.approx(33.0)


def test_candidate_that_lapses_never_becomes_a_default() -> None:
    months = MONTHS[:6]
    frame = pd.DataFrame(market_fillers(months=months) + bond_rows(
        "X", [300.0, 300.0, 2500.0, 1500.0, 400.0, 300.0], months=months,
        prices=[95.0, 95.0, 45.0, 60.0, 70.0, 80.0],
    ))
    rows = slice_of(build(frame, months=months), "X")
    assert "D" not in rows["implied_bucket"].tolist()
    assert not rows["d_confirmed"].any()
    assert bool(rows.iloc[2]["d_candidate"])
    assert rows["d_event_month"].isna().all()


def test_standalone_hard_price_confirms_without_a_candidate() -> None:
    """Owner decision (2026-09-18): price <= 35 is a standalone immediate D.

    No dual-distress month, no candidate, spread far below 2000 -- the hard
    price alone confirms in its own month and dates the event there.
    """
    months = MONTHS[:4]
    frame = pd.DataFrame(market_fillers(months=months) + bond_rows(
        "X", [300.0, 400.0, 450.0, 500.0], months=months,
        prices=[95.0, 30.0, 35.0, 34.0],
    ))
    rows = slice_of(build(frame, months=months), "X")
    assert not rows["d_candidate"].any(), "the dual condition never fired"
    assert rows["implied_bucket"].tolist() == ["BB", "D", "D", "D"]
    assert rows["d_confirmed"].tolist() == [False, True, True, True]
    assert rows["d_event_month"].iloc[1] == date(2025, 2, 1)
    # Absorbing: the later hard prints do not re-date the episode.
    assert rows["d_event_month"].iloc[3] == date(2025, 2, 1)
    assert rows["recovery_observed"].iloc[1] == pytest.approx(30.0)
    assert_ddl_invariants(rows)


def test_standalone_hard_price_requires_the_price_cut() -> None:
    months = MONTHS[:4]
    frame = pd.DataFrame(market_fillers(months=months) + bond_rows(
        "X", [300.0, 400.0, 450.0, 500.0], months=months,
        prices=[95.0, 36.0, 36.0, 36.0],
    ))
    rows = slice_of(build(frame, months=months), "X")
    assert not rows["d_confirmed"].any()
    assert not rows["d_candidate"].any()


def test_standalone_hard_confirmation_is_point_in_time() -> None:
    months = MONTHS[:7]
    frame = pd.DataFrame(bond_rows(
        "X", [300.0, 350.0, 400.0, 2500.0, 2600.0, 2600.0, 2600.0],
        months=months,
        prices=[95.0, 30.0, 40.0, 44.0, 44.0, 46.0, 48.0],
    ))
    anchor = ir.market_anchor_for_snapshot(frame, last_closed_month=months[-1])
    full = ir.build_publication_rows(frame, last_closed_month=months[-1], l_anchor=anchor)
    truncated = ir.build_publication_rows(frame, last_closed_month=months[3], l_anchor=anchor)
    for column in ("implied_bucket", "spell_id", "d_confirmed", "d_event_month"):
        pd.testing.assert_series_equal(
            full.loc[full["month"].le(months[3]), column].reset_index(drop=True),
            truncated[column].reset_index(drop=True),
            check_names=False,
            check_dtype=False,
        )
    assert (
        full.loc[full["d_confirmed"], "d_event_month"] == date(2025, 2, 1)
    ).all()
    assert_ddl_invariants(truncated)


def test_standalone_hard_default_is_absorbing_and_cures() -> None:
    months = MONTHS[:7]
    frame = pd.DataFrame(market_fillers(months=months) + bond_rows(
        "X", [300.0] * 7, months=months,
        prices=[95.0, 30.0, 30.0, 82.0, 84.0, 86.0, 88.0],
    ))
    rows = slice_of(build(frame, months=months), "X")
    assert rows["implied_bucket"].tolist() == ["BB", "D", "D", "D", "D", "BB", "BB"]
    assert rows["d_confirmed"].tolist() == [False, True, True, True, True, False, False]
    # The cure completes after three consecutive prices >= 80 (months 3-5) and
    # opens a NEW spell with the raw bucket.
    assert rows["spell_id"].iloc[5] == rows["spell_id"].iloc[0] + 1
    assert rows["d_event_month"].iloc[4] == date(2025, 2, 1)
    assert_ddl_invariants(rows)


def test_default_is_absorbing_and_a_cure_opens_a_new_spell() -> None:
    months = MONTHS[:8]
    frame = pd.DataFrame(market_fillers(months=months) + bond_rows(
        "X", [300.0, 2500.0, 2600.0, 2600.0, 2600.0, 2600.0, 2600.0, 2600.0],
        months=months, prices=[95.0, 45.0, 44.0, 82.0, 84.0, 86.0, 88.0, 90.0],
    ))
    rows = slice_of(build(frame, months=months), "X")
    # Candidate month 1 (CCC by its own spread jump, pre-confirmation), D from
    # month 2, and the cure completes after three consecutive prices >= 80
    # (months 3, 4, 5), which opens a new spell at the raw bucket.
    assert rows["implied_bucket"].tolist() == [
        "BB", "CCC", "D", "D", "D", "CCC", "CCC", "CCC",
    ]
    assert rows["spell_id"].iloc[5] == rows["spell_id"].iloc[0] + 1
    assert not bool(rows["d_confirmed"].iloc[5])
    assert rows["d_event_month"].iloc[2] == date(2025, 2, 1)
    assert_ddl_invariants(rows)


def test_default_rows_carry_then_withdraw_with_default_absorbing() -> None:
    months = pd.date_range("2025-01-01", periods=7, freq="MS")
    frame = pd.DataFrame(market_fillers(months=months) + bond_rows(
        "X", [2400.0, 2500.0], months=months[:2], prices=[45.0, 44.0],
    ))
    rows = slice_of(build(frame, months=months), "X")
    assert rows["implied_bucket"].iloc[1] == "D"
    assert rows["implied_bucket"].iloc[2:5].tolist() == ["D", "D", "D"]
    assert rows["carry_months"].iloc[2:5].tolist() == [1, 2, 3]
    assert rows["implied_bucket"].iloc[5] == "WITHDRAWN"
    assert rows["censoring"].iloc[5] == "default_absorbing"
    # The DDL ties d_confirmed to the D bucket: the withdrawal row cannot
    # carry the event flags.
    assert not bool(rows["d_confirmed"].iloc[5])
    assert rows["d_event_month"].iloc[5] is None
    assert_ddl_invariants(rows)


def test_default_event_dates_at_the_candidate_with_the_prior_month_as_origin() -> None:
    months = MONTHS[:5]
    frame = pd.DataFrame(market_fillers(months=months) + bond_rows(
        "X", [200.0, 200.0, 2500.0, 2600.0, 2600.0], months=months,
        prices=[95.0, 95.0, 45.0, 44.0, 46.0],
    ))
    rows = slice_of(build(frame, months=months), "X")
    # BBB -> the candidate month is already CCC; the exposure origin bucket is
    # the PRIOR month's implied bucket, which is what the app must count.
    assert rows["implied_bucket"].tolist() == ["BBB", "BBB", "CCC", "D", "D"]
    confirmed = rows.loc[rows["d_confirmed"]].iloc[0]
    event_month = pd.Timestamp(confirmed["d_event_month"])
    prior_row = rows.loc[rows["month"].eq(event_month - pd.DateOffset(months=1))]
    assert len(prior_row) == 1
    assert prior_row["implied_bucket"].iloc[0] == "BBB"


# --------------------------------------------------------------------------- #
# Point-in-time strictness and determinism
# --------------------------------------------------------------------------- #
def test_implied_bucket_never_changes_when_the_series_is_truncated() -> None:
    months = pd.date_range("2025-01-01", periods=8, freq="MS")
    frame = pd.DataFrame(bond_rows(
        "X", [300.0, 300.0, 2500.0, 2600.0, 2600.0, 2600.0, 2600.0, 2600.0],
        months=months, prices=[95.0, 95.0, 45.0, 44.0, 44.0, 46.0, 47.0, 48.0],
    ))
    anchor = ir.market_anchor_for_snapshot(frame, last_closed_month=months[-1])
    full = ir.build_publication_rows(frame, last_closed_month=months[-1], l_anchor=anchor)
    for cut in range(2, len(months)):
        truncated = ir.build_publication_rows(
            frame, last_closed_month=months[cut], l_anchor=anchor
        )
        left = full.loc[full["month"].le(months[cut])].reset_index(drop=True)
        right = truncated.reset_index(drop=True)
        assert len(left) == len(right)
        for column in ("implied_bucket", "witnessed", "carry_months", "spell_id",
                       "censoring", "spread_norm_log", "market_level_l"):
            pd.testing.assert_series_equal(
                left[column], right[column], check_names=False, check_dtype=False
            )
        for column in ("d_candidate",):
            pd.testing.assert_series_equal(
                left[column], right[column], check_names=False, check_dtype=False
            )
        # Only event annotations may differ, and only on the rows whose event
        # is not yet visible in the truncated series.
        differing = {
            column for column in left.columns
            if not left[column].equals(right[column])
        }
        assert differing <= {"d_confirmed", "d_event_month", "recovery_observed"}


def test_build_is_deterministic() -> None:
    frame = pd.DataFrame(
        bond_rows("A", [100.0] * 4 + [2500.0, 2600.0], months=MONTHS[:6])
        + bond_rows("B", [400.0] * 6, months=MONTHS[:6])
    )
    first = build(frame)
    second = build(frame)
    pd.testing.assert_frame_equal(first, second)
    assert ir.rows_digest(first) == ir.rows_digest(second)
    assert ir.snapshot_fingerprint(frame) == ir.snapshot_fingerprint(frame.copy())


def test_snapshot_fingerprint_reacts_to_consumed_values() -> None:
    frame = pd.DataFrame(bond_rows("A", [100.0, 120.0], months=MONTHS[:2]))
    changed = frame.copy()
    changed.loc[1, "spread_final_bps"] = 120.5
    assert ir.snapshot_fingerprint(frame) != ir.snapshot_fingerprint(changed)


def test_grid_gaps_become_carried_adjacent_months() -> None:
    months = MONTHS[:7]
    frame = pd.DataFrame(
        market_fillers(months=months)
        + bond_rows("X", [300.0], months=months[:1])
        + bond_rows("X", [300.0], months=months[2:3])
    )
    rows = slice_of(build(frame, months=months), "X")
    # The lane keeps every calendar month: the gap is carried (so an
    # adjacent-pair exposure walk sees the bond), and observation stopping after
    # month 3 closes the spell with the k+1 withdrawal.
    assert rows["month"].tolist() == list(months)
    assert rows["implied_bucket"].tolist() == [
        "BB", "BB", "BB", "BB", "BB", "BB", "WITHDRAWN",
    ]
    middle = rows.iloc[1]
    assert not bool(middle["witnessed"])
    assert middle["carry_months"] == 1
    assert rows.iloc[6]["carry_months"] == 4
    assert rows.iloc[6]["censoring"] == "withdrawal_absorbing"


def test_rows_do_not_exceed_the_last_closed_month() -> None:
    months = pd.date_range("2025-01-01", periods=4, freq="MS")
    frame = pd.DataFrame(bond_rows("X", [300.0], months=months[-1:]))
    rows = build(frame, months=months)
    assert rows["month"].max() == months[-1]


# --------------------------------------------------------------------------- #
# The exposure contract the app's estimator reads
# --------------------------------------------------------------------------- #
def test_carry_forward_rows_are_adjacent_so_they_count_as_exposure() -> None:
    months = MONTHS[:5]
    frame = pd.DataFrame(
        market_fillers(months=months) + bond_rows("X", [300.0] * 2, months=months[:2])
    )
    rows = slice_of(build(frame, months=months), "X")
    for position in range(1, len(rows)):
        current = rows.iloc[position]
        previous = rows.iloc[position - 1]
        assert current["month"] == previous["month"] + pd.DateOffset(months=1)
    carried = rows.iloc[2]
    assert carried["implied_bucket"] in ir.RATED_BUCKETS
    assert not bool(carried["witnessed"]) and carried["carry_months"] <= 3


def test_pending_candidate_is_published_for_the_apps_censoring() -> None:
    months = MONTHS[:4]
    frame = pd.DataFrame(market_fillers(months=months) + bond_rows(
        "X", [300.0, 300.0, 300.0, 2600.0], months=months,
        prices=[95.0, 95.0, 95.0, 44.0],
    ))
    rows = slice_of(build(frame, months=months), "X")
    # The candidate at the LAST month has no room to confirm inside the window;
    # it stays a candidate (never a default) and the app censors it as pending.
    last = rows.iloc[-1]
    assert bool(last["d_candidate"]) and not bool(last["d_confirmed"])
    assert last["month"] == months[-1]


def test_live_spell_at_the_window_end_has_no_terminal_censoring() -> None:
    months = MONTHS[:3]
    frame = pd.DataFrame(
        market_fillers(months=months) + bond_rows("X", [300.0] * 3, months=months)
    )
    rows = slice_of(build(frame, months=months), "X")
    last = rows.iloc[-1]
    assert last["month"] == months[-1]
    assert last["censoring"] == "none"
    assert last["implied_bucket"] in ir.RATED_BUCKETS
    # No successor month is published past the window: this is the app's
    # `censored_window_end`, never a wrong terminal classification.
    assert rows["censoring"].tolist() == ["none"] * 3


def test_terminal_censoring_kinds_are_distinct() -> None:
    months = pd.date_range("2025-01-01", periods=7, freq="MS")
    fillers = market_fillers(months=months)
    withdrawal = pd.DataFrame(fillers + bond_rows("W", [250.0] * 2, months=months[:2]))
    source_exit = pd.DataFrame(
        fillers + bond_rows("S", [250.0] * 2, months=months[:2], prices=[95.0, 99.0])
    )
    defaulted = pd.DataFrame(fillers + bond_rows(
        "D", [2400.0, 2500.0], months=months[:2], prices=[45.0, 44.0],
    ))
    withdrawal_rows = slice_of(build(withdrawal, months=months), "W")
    source_rows = slice_of(build(source_exit, months=months), "S")
    default_rows = slice_of(build(defaulted, months=months), "D")
    assert withdrawal_rows.iloc[-1]["censoring"] == "withdrawal_absorbing"
    assert withdrawal_rows.iloc[-1]["implied_bucket"] == "WITHDRAWN"
    assert source_rows.iloc[-1]["censoring"] == "source_exit"
    assert source_rows.iloc[-1]["implied_bucket"] in ir.RATED_BUCKETS
    assert default_rows.iloc[-1]["censoring"] == "default_absorbing"
    assert default_rows.iloc[-1]["implied_bucket"] == "WITHDRAWN"
    assert_ddl_invariants(withdrawal_rows)
    assert_ddl_invariants(source_rows)
    assert_ddl_invariants(default_rows)


# --------------------------------------------------------------------------- #
# The writer (in-memory mirror of the shared ledger)
# --------------------------------------------------------------------------- #
def _publication(frame: pd.DataFrame, **overrides) -> ImpliedRatingPublication:
    rows = build(frame)
    anchor = ir.market_anchor_for_snapshot(frame, last_closed_month=MONTHS[-1])
    fingerprint = ir.snapshot_fingerprint(frame)
    confirmed, candidates = ir.default_counts(rows)
    payload = {
        "publication_id": publication_id_for(ir.POLICY_DIGEST, "rev1", fingerprint),
        "panel_publication_id": "00000000-0000-0000-0000-00000000000a",
        "policy_version": ir.POLICY_VERSION,
        "policy_digest": ir.POLICY_DIGEST,
        "code_revision": "rev1",
        "panel_last_closed_month": MONTHS[-1].date(),
        "first_month": rows["month"].min().date(),
        "last_month": MONTHS[-1].date(),
        "input_fingerprint": fingerprint,
        "l_anchor": anchor,
        "rows_digest": ir.rows_digest(rows),
        "d_confirmed_count": confirmed,
        "d_candidate_count": candidates,
        "row_count": len(rows),
    }
    payload.update(overrides)
    return ImpliedRatingPublication(**payload)


def test_writer_lifecycle_is_idempotent_and_pointer_is_compare_and_set() -> None:
    frame = pd.DataFrame(bond_rows("A", [300.0] * 4, months=MONTHS[:4]))
    publication = _publication(frame)
    payload = [(None,) * len(worker.STAGE_COLUMNS)] * publication.row_count
    store = InMemoryPublicationStore()
    first = store.materialize(publication, payload, expected_pointer=None)
    assert first.lifecycle == "validated" and not first.reused
    rerun = store.materialize(publication, payload, expected_pointer=publication.publication_id)
    assert rerun.reused and store.pointer == publication.publication_id
    with pytest.raises(BondError) as error:
        store.materialize(publication, payload, expected_pointer="somebody-else")
    assert error.value.code == "pointer_moved"


def test_row_tuples_null_the_pandas_missing_values() -> None:
    from src.bonds.implied_rating_materializer import (
        FRAME_COLUMNS,
        ROW_COLUMNS,
        publication_row_tuples,
    )

    months = MONTHS[:3]
    frame = pd.DataFrame(
        market_fillers(months=months)
        + bond_rows("X", [300.0] * 3, months=months, trade_count=1)
    )
    rows = build(frame, months=months)
    fingerprint = ir.snapshot_fingerprint(frame)
    publication = ImpliedRatingPublication(
        publication_id=publication_id_for(ir.POLICY_DIGEST, "rev1", fingerprint),
        panel_publication_id="00000000-0000-0000-0000-00000000000a",
        policy_version=ir.POLICY_VERSION,
        policy_digest=ir.POLICY_DIGEST,
        code_revision="rev1",
        panel_last_closed_month=months[-1].date(),
        first_month=months[0].date(),
        last_month=months[-1].date(),
        input_fingerprint=fingerprint,
        l_anchor=0.0,
        rows_digest=ir.rows_digest(rows),
        d_confirmed_count=0,
        d_candidate_count=0,
        row_count=len(rows),
    )
    payload = publication_row_tuples(publication, rows)
    assert len(payload) == len(rows)
    assert all(len(values) == len(ROW_COLUMNS) for values in payload)
    by_column = {
        column: [values[index] for values in payload]
        for index, column in enumerate(FRAME_COLUMNS, start=1)
    }
    unwitnessed = by_column["implied_bucket"].index("NOT_RATED")
    assert by_column["spread_norm_log"][unwitnessed] is None
    assert by_column["neutralized_score"][unwitnessed] is None
    assert by_column["d_event_month"][unwitnessed] is None
    assert by_column["recovery_observed"][unwitnessed] is None
    assert all(isinstance(value, bool) for value in by_column["witnessed"])
    assert all(
        isinstance(value, int) and not isinstance(value, bool)
        for value in by_column["carry_months"]
    )
    assert all(values[0] == publication.publication_id for values in payload)


def test_publication_identity_binds_policy_revision_and_inputs() -> None:
    base = publication_id_for("a" * 64, "rev1", "b" * 64)
    assert base == publication_id_for("a" * 64, "rev1", "b" * 64)
    assert base != publication_id_for("c" * 64, "rev1", "b" * 64)
    assert base != publication_id_for("a" * 64, "rev2", "b" * 64)
    assert base != publication_id_for("a" * 64, "rev1", "d" * 64)
    assert build_fingerprint("a" * 64, "rev1", "b" * 64) != base


# --------------------------------------------------------------------------- #
# Worker states
# --------------------------------------------------------------------------- #
class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _patch_worker(monkeypatch, *, panel=None, pointer=None, current=None, snapshot=None,
                  relations=None, anchor=None) -> dict[str, object]:
    captured: dict[str, object] = {}
    monkeypatch.setattr(worker, "connect", lambda _dsn: _FakeConnection())
    monkeypatch.setattr(worker, "install_schema", lambda _conn: None)
    monkeypatch.setattr(worker, "_relation_exists", lambda _conn, name: (relations or {}).get(name, True))
    monkeypatch.setattr(worker, "_current_panel", lambda _conn: panel)
    monkeypatch.setattr(worker, "_current_pointer", lambda _conn: pointer)
    monkeypatch.setattr(worker, "_already_current", lambda _conn, **kwargs: current)
    monkeypatch.setattr(worker, "_read_snapshot", lambda _conn, **kwargs: snapshot)
    monkeypatch.setattr(worker, "current_pinned_anchor", lambda _conn: anchor)
    monkeypatch.setenv("CODE_REVISION", "test-rev")
    monkeypatch.delenv("BOND_IMPLIED_RATING_FORCE_REPUBLISH", raising=False)

    def fake_materialize(conn, publication, rows, *, expected_pointer):
        captured["publication"] = publication
        captured["expected_pointer"] = expected_pointer
        return type("Result", (), {
            "publication_id": publication.publication_id,
            "row_count": publication.row_count,
            "reused": False,
        })()

    monkeypatch.setattr(worker, "materialize", fake_materialize)
    return captured


def test_worker_refuses_an_unresolvable_code_revision(monkeypatch) -> None:
    monkeypatch.setattr(worker, "_code_revision", lambda: "unknown")
    result = worker.run("postgresql://example")
    assert result["state"] == "gate_failed"
    assert result["aborted"] is True
    assert result["input_reasons"] == ["code_revision_absent"]


def test_worker_reports_missing_panel_relations(monkeypatch) -> None:
    _patch_worker(monkeypatch, relations={"bond_panel_publications": False})
    result = worker.run("postgresql://example")
    assert result["state"] == "gate_failed"
    assert "relation_absent:bond_panel_publications" in result["input_reasons"]


def test_worker_reports_the_missing_snapshot_matview(monkeypatch) -> None:
    _patch_worker(monkeypatch, relations={worker.SNAPSHOT_MATVIEW: False})
    result = worker.run("postgresql://example")
    assert result["input_reasons"] == [f"relation_absent:{worker.SNAPSHOT_MATVIEW}"]


def test_worker_reports_no_panel_parent(monkeypatch) -> None:
    _patch_worker(monkeypatch, panel=None)
    result = worker.run("postgresql://example")
    assert result["input_reasons"] == ["panel_no_parent"]


def test_worker_short_circuits_when_already_current(monkeypatch) -> None:
    captured = _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[-1].date(), "open_month": None},
        pointer="pub-current",
        current="pub-current",
    )
    result = worker.run("postgresql://example")
    assert result["state"] == "current"
    assert result["reason"] == "implied_rating_already_current"
    assert "publication" not in captured


def test_worker_force_republish_bypasses_the_short_circuit(monkeypatch) -> None:
    frame = pd.DataFrame(bond_rows("A", [300.0] * 4, months=MONTHS[:4]))
    captured = _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[-1].date(), "open_month": None},
        pointer="pub-current",
        current="pub-current",
        snapshot=frame,
        anchor=None,
    )
    monkeypatch.setenv("BOND_IMPLIED_RATING_FORCE_REPUBLISH", "1")
    result = worker.run("postgresql://example")
    assert result["state"] in {"published", "published_no_defaults"}
    assert captured["expected_pointer"] == "pub-current"


def test_worker_publishes_and_reports_the_identity(monkeypatch) -> None:
    frame = pd.DataFrame(bond_rows("A", [300.0] * 4, months=MONTHS[:4]))
    captured = _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[-1].date(), "open_month": None},
        pointer=None,
        current=None,
        snapshot=frame,
    )
    result = worker.run("postgresql://example")
    assert result["state"] == "published_no_defaults"
    assert result["aborted"] is False
    assert result["panel_publication_id"] == "panel-1"
    assert result["policy_digest"] == ir.POLICY_DIGEST
    assert result["row_count"] > 0
    publication = captured["publication"]
    assert publication.input_fingerprint == ir.snapshot_fingerprint(frame)
    assert publication.l_anchor == pytest.approx(
        ir.market_anchor_for_snapshot(frame, last_closed_month=MONTHS[-1])
    )


def test_worker_refuses_an_anchor_drift(monkeypatch, caplog) -> None:
    frame = pd.DataFrame(bond_rows("A", [300.0] * 4, months=MONTHS[:4]))
    _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[-1].date(), "open_month": None},
        pointer=None,
        current=None,
        snapshot=frame,
        anchor=123.456,
    )
    with caplog.at_level(logging.WARNING, logger="src.workers.bond_market_implied_rating"):
        result = worker.run("postgresql://example")
    assert result["state"] == "gate_failed"
    assert result["input_reasons"] == ["anchor_drift"]
    # Pre-Phase-3 the daily stage is verdict-neutral: the warning is the alert.
    assert any(
        record.levelno == logging.WARNING and "anchor_drift" in record.getMessage()
        for record in caplog.records
    )


def test_worker_warns_when_the_publication_fails(monkeypatch, caplog) -> None:
    frame = pd.DataFrame(bond_rows("A", [300.0] * 4, months=MONTHS[:4]))
    _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[-1].date(), "open_month": None},
        pointer=None,
        current=None,
        snapshot=frame,
    )

    def boom(_conn, _publication, _rows, *, expected_pointer):
        raise BondError("build_pin_mismatch", {"publication_id": "x"})

    monkeypatch.setattr(worker, "materialize", boom)
    with caplog.at_level(logging.WARNING, logger="src.workers.bond_market_implied_rating"):
        result = worker.run("postgresql://example")
    assert result["reason"] == "implied_rating_publish_failed"
    assert result["input_reasons"] == ["build_pin_mismatch"]
    assert any(
        record.levelno == logging.WARNING and "publish_failed" in record.getMessage()
        for record in caplog.records
    )


def test_worker_refuses_a_snapshot_without_a_market_level_observation(monkeypatch) -> None:
    """No witnessed spread in the frozen window -> typed refusal, no anchor."""
    frame = pd.DataFrame(
        bond_rows("A", [100.0] * 3, months=MONTHS[:3], trade_count=1)
    )
    _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[2].date(), "open_month": None},
        pointer=None,
        current=None,
        snapshot=frame,
    )
    result = worker.run("postgresql://example")
    assert result["state"] == "gate_failed"
    assert result["input_reasons"] == ["no_market_level_observation"]


def test_worker_types_a_pointer_move_as_a_gate_failure(monkeypatch) -> None:
    frame = pd.DataFrame(bond_rows("A", [300.0] * 4, months=MONTHS[:4]))
    _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[-1].date(), "open_month": None},
        pointer="pub-old",
        current=None,
        snapshot=frame,
    )

    def boom(_conn, _publication, _rows, *, expected_pointer):
        raise BondError("pointer_moved", {"expected": expected_pointer})

    monkeypatch.setattr(worker, "materialize", boom)
    result = worker.run("postgresql://example")
    assert result["state"] == "gate_failed"
    assert result["input_reasons"] == ["pointer_moved"]


def test_worker_reports_an_empty_snapshot(monkeypatch) -> None:
    _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[-1].date(), "open_month": None},
        pointer=None,
        current=None,
        snapshot=pd.DataFrame(columns=list(worker.STAGE_COLUMNS)),
    )
    result = worker.run("postgresql://example")
    assert result["input_reasons"] == ["snapshot_empty"]


def test_worker_reports_a_snapshot_window_that_stops_early(monkeypatch) -> None:
    frame = pd.DataFrame(bond_rows("A", [300.0] * 3, months=MONTHS[:3]))
    _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[-1].date(), "open_month": None},
        pointer=None,
        current=None,
        snapshot=frame,
    )
    result = worker.run("postgresql://example")
    assert result["input_reasons"] == ["snapshot_window_incomplete"]


# --------------------------------------------------------------------------- #
# Dry-run planner (the backfill's default mode)
# --------------------------------------------------------------------------- #
def test_the_planner_reports_the_identity_without_writing(monkeypatch) -> None:
    frame = pd.DataFrame(bond_rows("A", [300.0] * 4, months=MONTHS[:4]))
    _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[-1].date(), "open_month": None},
        pointer=None,
        current=None,
        snapshot=frame,
        anchor=None,
    )
    monkeypatch.setattr(worker, "materialize", lambda *args, **kwargs: pytest.fail("plan wrote"))
    result = worker.plan("postgresql://example")
    assert result["state"] == "planned"
    assert result["aborted"] is False
    assert result["anchor_drift"] is False
    assert result["row_count"] > 0
    assert result["policy_digest"] == ir.POLICY_DIGEST
    assert result["l_anchor"] == pytest.approx(
        ir.market_anchor_for_snapshot(frame, last_closed_month=MONTHS[-1])
    )


def test_the_planner_surfaces_a_drifted_anchor(monkeypatch, caplog) -> None:
    frame = pd.DataFrame(bond_rows("A", [300.0] * 4, months=MONTHS[:4]))
    _patch_worker(
        monkeypatch,
        panel={"publication_id": "panel-1", "first_month": MONTHS[0].date(),
               "last_closed_month": MONTHS[-1].date(), "open_month": None},
        pointer="pub-current",
        current=None,
        snapshot=frame,
        anchor=123.456,
    )
    monkeypatch.setattr(worker, "materialize", lambda *args, **kwargs: pytest.fail("plan wrote"))
    with caplog.at_level(logging.WARNING, logger="src.workers.bond_market_implied_rating"):
        result = worker.plan("postgresql://example")
    assert result["state"] == "anchor_drift"
    assert result["anchor_drift"] is True
    assert result["pinned_l_anchor"] == 123.456
    assert result["pinned_l_anchor"] != result["l_anchor"]
    assert any(
        record.levelno == logging.WARNING and "anchor drift" in record.getMessage()
        for record in caplog.records
    )


def test_a_ledger_that_does_not_exist_yet_reads_as_absent(monkeypatch) -> None:
    """The product must be installable on a database whose ledger never ran."""
    monkeypatch.setattr(worker, "_relation_exists", lambda _conn, _name: False)
    assert worker._current_pointer(object()) is None
