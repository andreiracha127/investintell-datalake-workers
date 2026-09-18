"""Market-implied bond rating: the frozen policy and the point-in-time state machine.

This module is PURE (pandas/numpy only, no database, no repo imports). It turns
the served bond-panel snapshot grid into the monthly state series the
``bond_market_implied_rating_v1`` product publishes:

    witnessed month -> normalized spread score -> chained market level L_t
    -> bucket thresholds with hysteresis -> carry-forward -> spells
    -> D candidate / confirmation -> cure

The policy below is the DECLARED round policy of
``docs/calibration/bond_market_implied_rating_round_declaration.md``. Every
number is a Decimal-exact string, ``POLICY_DIGEST`` is the canonical sha256 over
``POLICY_VERSION`` + ``POLICY``, and the digest is part of the publication
identity: changing any value here mints a different publication and requires a
new calibration round, never a silent retune.

Point-in-time discipline (plan D2). ``implied_bucket`` of a month may NOT change
when the series is truncated after that month: every transition below is causal.
The only columns legitimately revised once a later month confirms a default are
``d_confirmed`` / ``d_event_month`` (and the D rows they belong to) -- the event
is dated at the CANDIDATE month, and the confirmation month is when the market
can first see it.

Local readings of the declared policy, recorded because the plan fixes the
numbers but leaves these mechanisms open:

  * ``witness_mask`` also requires the score inputs (``spread_final_bps``,
    ``mod_dur``): the published row CHECK is ``(spread_norm_log IS NULL) =
    (NOT witnessed)``, so a "witnessed" month without a computable score cannot
    be published as such. It is carried instead.
  * Hysteresis: a move to the raw target bucket is confirmed when the score is
    past the TARGET bucket's own boundary by ``delta_log``, or when the target
    was observed in a second witnessed month within ``h_months`` of the current
    one (carried months are not observations). Otherwise the bucket does not
    move.
  * A candidate episode stays active for ``h_d`` months; the hard price trigger
    (``price <= p_hard``) only ACCELERATES a confirmation inside an active
    episode. A deep price without the dual distress condition never creates a
    default on its own -- the conservative side of the declared false-D gate.
  * ``d_candidate`` is the dual distress condition (``price <= p_d`` and
    ``spread >= s_d_bps``) at a witnessed month, regardless of the spell state.
    It is a point-in-time observation flag: the months between candidate and
    confirmation keep it, and the app's pending censoring
    (``d_candidate AND NOT d_confirmed``) reads exactly that.
  * The timeline is calendar-dense per CUSIP: months between two observed grid
    months, and up to ``k`` months after the last one, are published as
    unwitnessed carry rows while the spell is alive, so the app's adjacent-pair
    exposure walk can tell "carried" from "gone". Month ``k + 1`` without a
    witness closes the spell with a WITHDRAWN row. A later witnessed month opens
    a NEW spell (new ``spell_id``), it never revives the withdrawn one.
  * ``L_anchor`` is the median of the chained ``L`` over the policy's declared
    calibration window, whose END month is frozen in the policy: a wall-clock
    window would move the anchor and rewrite point-in-time buckets.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import deque
from datetime import date
from types import MappingProxyType
from typing import Any

import numpy as np
import pandas as pd

POLICY_VERSION = "bond_market_implied_rating_policy_v1"
PRODUCT = "bond_market_implied_rating_v1"

#: The last closed bond-panel month at the round declaration. Frozen in the
#: policy on purpose (see the module docstring): the calibration window is
#: ``[window_end_month - 35, window_end_month]`` and every build must resolve
#: the SAME anchor from the same closed history.
CALIBRATION_WINDOW_END_MONTH = "2026-08-01"
CALIBRATION_WINDOW_MONTHS = "36"

BUCKET_ORDER: tuple[str, ...] = (
    "AAA", "AA", "A", "BBB", "BB", "B", "CCC", "D", "WITHDRAWN", "NOT_RATED",
)
RATED_BUCKETS: tuple[str, ...] = ("AAA", "AA", "A", "BBB", "BB", "B", "CCC")
CENSORING_KINDS: tuple[str, ...] = (
    "none", "source_exit", "withdrawal_absorbing", "default_absorbing",
)

#: The frozen declared policy. Decimal-exact strings, lists for order -- this
#: mapping IS the round: its sha256 is POLICY_DIGEST.
POLICY: MappingProxyType = MappingProxyType({
    "witness": {
        "n_min": "3",
        "v_min_usd": "250000",
        "price_min": "1",
        "price_max": "200",
        "d_min": "0.5",
    },
    "spread": {
        "winsor_bps": ["5", "5000"],
        "d_ref": "5",
        "duration_slope_b": "0.2",
    },
    "market_level": {
        "chain": "median_of_intersection_deltas",
        "beta": "0.6",
        "anchor": {
            "kind": "calibration_window_median_of_l",
            "window_months": CALIBRATION_WINDOW_MONTHS,
            "window_end_month": CALIBRATION_WINDOW_END_MONTH,
            # The frozen VALUE (median of L over the window). It stays NULL
            # until the calibration round resolves it; while NULL the worker
            # derives it from the closed history and the publication pins it.
            # Setting it is a POLICY change: a new digest, a new round.
            "l_anchor": None,
        },
    },
    "cuts_bps": ["60", "85", "125", "220", "380", "700"],
    "hysteresis": {"delta_log": "0.10", "confirm_obs": "2", "h_months": "3"},
    "carry_forward_k": "3",
    "source_exit": {"months_to_maturity_max": "1", "price_par": "97"},
    "default": {
        "p_d": "50",
        "s_d_bps": "2000",
        "p_hard": "35",
        "h_d": "3",
        "p_cure": "80",
        "n_cure": "3",
    },
    "calibration": {
        "t_c": "36",
        "holdout": "24",
        "lambda_floor": "20",
        "m_max": "0.05",
        "phi_max": "0.25",
    },
    "buckets": list(BUCKET_ORDER),
})

PUBLICATION_COLUMNS: tuple[str, ...] = (
    "month",
    "cusip_id",
    "implied_bucket",
    "spread_norm_log",
    "market_level_l",
    "witnessed",
    "carry_months",
    "spell_id",
    "d_candidate",
    "d_confirmed",
    "d_event_month",
    "recovery_observed",
    "censoring",
    "policy_version",
    "policy_digest",
)

POLICY_DIGEST: str = hashlib.sha256(
    json.dumps(
        {"policy_version": POLICY_VERSION, "policy": dict(POLICY)},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
).hexdigest()


def _policy_float(*path: str) -> float:
    node: Any = POLICY
    for key in path:
        node = node[key]
    return float(node)


# --------------------------------------------------------------------------- #
# Policy accessors
# --------------------------------------------------------------------------- #
def bucket_cuts_log() -> tuple[float, ...]:
    """The log of the six declared bps cuts, in AAA..CCC order of use."""
    return tuple(math.log(float(cut)) for cut in POLICY["cuts_bps"])


def bucket_for_score(score: float) -> str:
    """The raw (hysteresis-free) bucket of a normalized log score."""
    if not math.isfinite(score):
        raise ValueError("a raw bucket requires a finite score")
    for index, cut in enumerate(bucket_cuts_log()):
        if score < cut:
            return RATED_BUCKETS[index]
    return RATED_BUCKETS[-1]


def _bucket_rank(bucket: str) -> int:
    return RATED_BUCKETS.index(bucket)


# --------------------------------------------------------------------------- #
# Witness, score, market level
# --------------------------------------------------------------------------- #
def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        raise KeyError(f"implied rating policy requires column {column!r}")
    return pd.to_numeric(frame[column], errors="coerce")


def witness_mask(snapshot: pd.DataFrame) -> pd.Series:
    """The declared monthly witness mask, index-aligned to ``snapshot``.

    ``n_min`` traded prints, ``v_min_usd`` dollar volume, a clean price inside
    ``[price_min, price_max]`` and a modified duration of at least ``d_min``.
    Missing score inputs (``spread_final_bps``, ``mod_dur``) make the month
    unwitnessed rather than unscorable -- see the module docstring.
    """
    price = _numeric(snapshot, "price")
    spread_bps = _numeric(snapshot, "spread_final_bps")
    mod_dur = _numeric(snapshot, "mod_dur")
    trade_count = _numeric(snapshot, "trade_count")
    dollar_volume = _numeric(snapshot, "dollar_volume")
    mask = (
        trade_count.ge(_policy_float("witness", "n_min"))
        & dollar_volume.ge(_policy_float("witness", "v_min_usd"))
        & price.between(
            _policy_float("witness", "price_min"),
            _policy_float("witness", "price_max"),
            inclusive="both",
        )
        & mod_dur.ge(_policy_float("witness", "d_min"))
        & spread_bps.notna()
        & mod_dur.notna()
    )
    return mask.fillna(False).astype(bool)


def normalized_spread(snapshot: pd.DataFrame) -> pd.Series:
    """Winsorized, duration-adjusted log spread; NaN where unwitnessed.

    ``s = log(winsor(spread_bps)) - b * (log(mod_dur) - log(d_ref))``
    """
    spread_bps = _numeric(snapshot, "spread_final_bps")
    mod_dur = _numeric(snapshot, "mod_dur")
    low, high = (float(value) for value in POLICY["spread"]["winsor_bps"])
    slope = _policy_float("spread", "duration_slope_b")
    d_ref = _policy_float("spread", "d_ref")
    clipped = spread_bps.clip(lower=low, upper=high)
    duration = mod_dur.where(mod_dur.gt(0))
    score = np.log(clipped.to_numpy(dtype="float64")) - slope * (
        np.log(duration.to_numpy(dtype="float64")) - math.log(d_ref)
    )
    series = pd.Series(score, index=snapshot.index, dtype="float64")
    return series.where(witness_mask(snapshot))


def market_level(spread_norm: pd.DataFrame) -> pd.Series:
    """Chained market level ``L_t`` from the median of intersection deltas.

    ``L_t = L_{t-1} + median_{i in W_t & W_{t-1}} (s_i,t - s_i,t-1)``

    ``spread_norm`` carries the witnessed rows (``cusip_id``, ``month``,
    ``spread_norm_log``). Months with an empty intersection keep the previous
    level (delta 0): the chain never invents a move it did not observe. The
    first month is 0.0 by construction; a single-month input returns a
    single-point series.
    """
    frame = spread_norm.loc[
        spread_norm["spread_norm_log"].notna(), ["cusip_id", "month", "spread_norm_log"]
    ].copy()
    frame["month"] = pd.to_datetime(frame["month"]).dt.normalize()
    frame["spread_norm_log"] = pd.to_numeric(frame["spread_norm_log"], errors="coerce")
    months = pd.DatetimeIndex(sorted(frame["month"].unique()))
    level = pd.Series(0.0, index=months, dtype="float64")
    if len(months) <= 1:
        return level
    delta = pd.Series(0.0, index=months, dtype="float64")
    for position in range(1, len(months)):
        left = frame.loc[
            frame["month"].eq(months[position - 1]), ["cusip_id", "spread_norm_log"]
        ]
        right = frame.loc[
            frame["month"].eq(months[position]), ["cusip_id", "spread_norm_log"]
        ]
        common = left.merge(right, on="cusip_id", how="inner", suffixes=("_prev", "_curr"))
        if common.empty:
            continue
        delta.iloc[position] = float(
            (common["spread_norm_log_curr"] - common["spread_norm_log_prev"]).median()
        )
    return (level + delta.cumsum()).astype("float64")


def market_anchor(
    level: pd.Series,
    *,
    window_end_month: str | None = None,
    window_months: int | None = None,
) -> float:
    """Median of ``L`` over the frozen calibration window.

    The window is ``[window_end - (window_months - 1), window_end]`` inclusive,
    over the months the level series actually carries. A window with no
    observation raises: an unanchored chain would silently move every bucket.
    """
    anchor_policy = POLICY["market_level"]["anchor"]
    end = pd.Timestamp(window_end_month or anchor_policy["window_end_month"]).normalize()
    count = int(window_months or int(anchor_policy["window_months"]))
    start = pd.Timestamp(end - pd.DateOffset(months=count - 1)).normalize()
    window = level.loc[(level.index >= start) & (level.index <= end)]
    if window.empty:
        raise ValueError(
            f"calibration window {start.date().isoformat()}..{end.date().isoformat()} "
            "has no market-level observation"
        )
    resolved = float(window.median())
    pinned = anchor_policy.get("l_anchor")
    if pinned is not None and float(pinned) != resolved:
        # The policy froze a value; the closed history no longer reproduces it
        # (a forced panel republish rewrote window months). Publishing under the
        # drifted anchor would silently rewrite every historical bucket.
        raise ValueError(
            f"policy l_anchor {float(pinned)!r} is not reproduced by the closed "
            f"history ({resolved!r}); a new calibration round is required"
        )
    return resolved


# --------------------------------------------------------------------------- #
# Default events: candidate / confirmation
# --------------------------------------------------------------------------- #
def default_events(observations: pd.DataFrame) -> pd.DataFrame:
    """Distress episodes of ONE cusip timeline, in candidate-month order.

    ``observations`` is that CUSIP's calendar-dense timeline (``month``,
    ``witnessed``, ``price``, ``spread_final_bps``), sorted by month. A month
    is a candidate when ``price <= p_d`` and ``spread >= s_d_bps``. The episode
    stays active for ``h_d`` months; it confirms at the first witnessed month
    inside that window that repeats the dual condition or prints
    ``price <= p_hard``. Outside the window it expires, and a later condition
    month opens a new candidate.

    Returns one row per episode: ``candidate_month``, ``confirmation_month``
    (``NaT`` when never confirmed) and ``immediate`` (confirmation in the
    candidate month itself).
    """
    p_d = _policy_float("default", "p_d")
    s_d = _policy_float("default", "s_d_bps")
    p_hard = _policy_float("default", "p_hard")
    h_d = int(POLICY["default"]["h_d"])

    months = pd.to_datetime(observations["month"]).dt.normalize().tolist()
    witnessed = observations["witnessed"].fillna(False).to_numpy(dtype=bool)
    price = pd.to_numeric(observations["price"], errors="coerce").to_numpy(dtype="float64")
    spread = pd.to_numeric(
        observations["spread_final_bps"], errors="coerce"
    ).to_numpy(dtype="float64")

    episodes: list[dict[str, Any]] = []
    candidate: int | None = None
    for index, month in enumerate(months):
        if not witnessed[index]:
            continue
        condition = bool(price[index] <= p_d and spread[index] >= s_d)
        hard = bool(price[index] <= p_hard)
        if condition:
            if candidate is None or index - candidate > h_d:
                candidate = index
            if candidate == index:
                if hard:
                    episodes.append({
                        "candidate_month": month,
                        "confirmation_month": month,
                        "immediate": True,
                    })
                    candidate = None
            elif index - candidate <= h_d:
                episodes.append({
                    "candidate_month": months[candidate],
                    "confirmation_month": month,
                    "immediate": False,
                })
                candidate = None
            else:  # pragma: no cover - guarded by the reset above
                candidate = index
        elif hard and candidate is not None and index - candidate <= h_d:
            episodes.append({
                "candidate_month": months[candidate],
                "confirmation_month": month,
                "immediate": False,
            })
            candidate = None
        elif candidate is not None and index - candidate > h_d:
            candidate = None
    if candidate is not None:
        episodes.append({
            "candidate_month": months[candidate],
            "confirmation_month": pd.NaT,
            "immediate": False,
        })
    return pd.DataFrame(
        episodes, columns=["candidate_month", "confirmation_month", "immediate"]
    )


# --------------------------------------------------------------------------- #
# The state machine: buckets, hysteresis, carry-forward, spells, D, cure
# --------------------------------------------------------------------------- #
def _add_months(value: pd.Timestamp, count: int) -> pd.Timestamp:
    return (pd.Timestamp(value) + pd.DateOffset(months=count)).normalize()


def _months_between(start: pd.Timestamp, end: pd.Timestamp) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month)


def _calendar_timeline(group: pd.DataFrame, *, end: pd.Timestamp) -> pd.DataFrame:
    """A calendar-dense month timeline, with the carry tail the policy needs.

    Months between observed grid months stay in the timeline as unwitnessed
    months (carried while a spell is alive), and the timeline runs ``k + 1``
    months past the last observed month so a spell that simply stops being
    observed still closes with a WITHDRAWN row -- never past ``last_closed``.
    """
    k = int(POLICY["carry_forward_k"])
    first = pd.Timestamp(group["month"].min()).normalize()
    last = pd.Timestamp(group["month"].max()).normalize()
    tail = min(_add_months(last, k + 1), end)
    timeline = pd.DataFrame({"month": pd.date_range(start=first, end=tail, freq="MS")})
    observed = group.copy()
    observed["in_grid"] = True
    timeline = timeline.merge(observed, on="month", how="left")
    timeline["in_grid"] = timeline["in_grid"].fillna(False).astype(bool)
    timeline["witnessed"] = timeline["witnessed"].fillna(False).astype(bool)
    return timeline


def _hysteresis_bucket(
    *,
    bucket: str,
    score: float,
    history: deque[tuple[int, str]],
    month_index: int,
    h_months: int,
) -> str:
    """The declared hysteresis: a move needs a delta-past-boundary OR two obs."""
    target = bucket_for_score(score)
    if target == bucket:
        return bucket
    delta = _policy_float("hysteresis", "delta_log")
    confirm_obs = int(POLICY["hysteresis"]["confirm_obs"])
    cuts = bucket_cuts_log()
    direction = 1 if _bucket_rank(target) > _bucket_rank(bucket) else -1
    if direction > 0:
        beyond_margin = score >= cuts[_bucket_rank(target) - 1] + delta
    else:
        beyond_margin = score < cuts[_bucket_rank(target)] - delta
    if beyond_margin:
        return target
    observations = [entry for entry in history if month_index - entry[0] <= h_months]
    observations.append((month_index, target))
    target_rank = _bucket_rank(target)
    if direction > 0:
        on_target_side = sum(
            1 for _entry_index, observed in observations
            if _bucket_rank(observed) >= target_rank
        )
    else:
        on_target_side = sum(
            1 for _entry_index, observed in observations
            if _bucket_rank(observed) <= target_rank
        )
    if on_target_side >= confirm_obs:
        return target
    return bucket


class _SpellState:
    """Mutable state of one CUSIP's live spell (reset when a spell ends)."""

    __slots__ = (
        "bucket", "carry", "confirmed", "cure_streak", "event_month",
        "event_price", "history", "spell_id", "spell_start",
    )

    def __init__(self) -> None:
        self.reset(spell_id=0, spell_start=-1)

    def reset(self, *, spell_id: int, spell_start: int) -> None:
        self.spell_id = spell_id
        self.spell_start = spell_start
        self.bucket: str | None = None
        self.carry = 0
        self.confirmed = False
        self.cure_streak = 0
        self.event_month: pd.Timestamp | None = None
        self.event_price: float | None = None
        self.history: deque[tuple[int, str]] = deque()


def _state_rows_for_cusip(timeline: pd.DataFrame, *, cusip: str) -> list[dict[str, Any]]:
    """Run one CUSIP's calendar-dense timeline through the declared machine.

    ``timeline`` is sorted by month and carries ``in_grid``, ``witnessed``,
    ``spread_norm_log``, ``price``, ``score``, ``market_level_l`` and
    ``maturity_date``.
    """
    in_grid = timeline["in_grid"].to_numpy(dtype=bool)
    witnessed = timeline["witnessed"].to_numpy(dtype=bool)
    price = pd.to_numeric(timeline["price"], errors="coerce").to_numpy(dtype="float64")
    spread = pd.to_numeric(
        timeline["spread_final_bps"], errors="coerce"
    ).to_numpy(dtype="float64")
    score = pd.to_numeric(timeline["score"], errors="coerce").to_numpy(dtype="float64")
    level = pd.to_numeric(
        timeline["market_level_l"], errors="coerce"
    ).to_numpy(dtype="float64")
    maturity = pd.to_datetime(timeline["maturity_date"]).dt.normalize().to_numpy()
    months = pd.to_datetime(timeline["month"]).dt.normalize().tolist()
    month_index = {month: index for index, month in enumerate(months)}

    p_d = _policy_float("default", "p_d")
    s_d = _policy_float("default", "s_d_bps")
    p_cure = _policy_float("default", "p_cure")
    n_cure = int(POLICY["default"]["n_cure"])
    exit_months = int(POLICY["source_exit"]["months_to_maturity_max"])
    price_par = _policy_float("source_exit", "price_par")
    k = int(POLICY["carry_forward_k"])
    h_months = int(POLICY["hysteresis"]["h_months"])

    episodes = default_events(timeline.reset_index(drop=True))
    confirmations = {
        month_index[episode.confirmation_month]: month_index[episode.candidate_month]
        for episode in episodes.itertuples(index=False)
        if not pd.isna(episode.confirmation_month)
    }

    rows: list[dict[str, Any]] = []
    state = _SpellState()
    spell_seq = 0
    last_witnessed_row: int | None = None
    last_witnessed_index: int | None = None

    def level_at(index: int) -> float | None:
        return float(level[index]) if math.isfinite(level[index]) else None

    def source_exit_after(index: int) -> bool:
        """Did the LAST witnessed month look like the end of the source?

        ``SOURCE_EXIT`` is a terminal classification of a disappearance, not a
        per-month state: a bond that keeps printing 98s is still observed. When
        observation stops, the last evidence decides -- near maturity or at par
        means the bond matured/was called, and the spell closes right there with
        ``censoring='source_exit'`` instead of carrying to WITHDRAWN.
        """
        if bool(price[index] >= price_par):
            return True
        if maturity[index] is None or pd.isna(maturity[index]):
            return False
        return _months_between(months[index], pd.Timestamp(maturity[index])) <= exit_months

    for index, month in enumerate(months):
        base: dict[str, Any] = {
            "month": month,
            "cusip_id": cusip,
            "market_level_l": level_at(index),
        }
        # The dual distress condition is a point-in-time observation flag: it is
        # published on every witnessed month that satisfies it (plan D2: the
        # app reads `d_candidate AND NOT d_confirmed` for pending censoring).
        condition_now = bool(
            witnessed[index] and price[index] <= p_d and spread[index] >= s_d
        )
        if witnessed[index]:
            if not state.confirmed:
                confirmation = confirmations.get(index)
                if confirmation is not None and confirmation >= state.spell_start:
                    if state.spell_id == 0:
                        spell_seq += 1
                        state.reset(spell_id=spell_seq, spell_start=index)
                    state.confirmed = True
                    state.cure_streak = 0
                    state.event_month = months[confirmation]
                    state.event_price = (
                        float(price[confirmation])
                        if math.isfinite(price[confirmation])
                        else None
                    )
                    rows.append({
                        **base,
                        "implied_bucket": "D",
                        "spread_norm_log": float(score[index]),
                        "witnessed": True,
                        "carry_months": 0,
                        "spell_id": state.spell_id,
                        "d_candidate": condition_now,
                        "d_confirmed": True,
                        "d_event_month": state.event_month.date(),
                        "recovery_observed": state.event_price,
                        "censoring": "none",
                    })
                else:
                    if state.spell_id == 0:
                        spell_seq += 1
                        state.reset(spell_id=spell_seq, spell_start=index)
                        state.bucket = bucket_for_score(score[index])
                    else:
                        state.bucket = _hysteresis_bucket(
                            bucket=state.bucket or bucket_for_score(score[index]),
                            score=float(score[index]),
                            history=state.history,
                            month_index=index,
                            h_months=h_months,
                        )
                    rows.append({
                        **base,
                        "implied_bucket": state.bucket,
                        "spread_norm_log": float(score[index]),
                        "witnessed": True,
                        "carry_months": 0,
                        "spell_id": state.spell_id,
                        "d_candidate": condition_now,
                        "d_confirmed": False,
                        "d_event_month": None,
                        "recovery_observed": None,
                        "censoring": "none",
                    })
                    state.history.append((index, bucket_for_score(score[index])))
                    while state.history and index - state.history[0][0] > h_months:
                        state.history.popleft()
                state.carry = 0
            else:
                state.cure_streak = state.cure_streak + 1 if price[index] >= p_cure else 0
                if state.cure_streak >= n_cure:
                    # Cure complete: a NEW spell starts at this month.
                    spell_seq += 1
                    state.reset(spell_id=spell_seq, spell_start=index)
                    state.bucket = bucket_for_score(score[index])
                    state.history.append((index, state.bucket))
                    rows.append({
                        **base,
                        "implied_bucket": state.bucket,
                        "spread_norm_log": float(score[index]),
                        "witnessed": True,
                        "carry_months": 0,
                        "spell_id": state.spell_id,
                        "d_candidate": condition_now,
                        "d_confirmed": False,
                        "d_event_month": None,
                        "recovery_observed": None,
                        "censoring": "none",
                    })
                else:
                    rows.append({
                        **base,
                        "implied_bucket": "D",
                        "spread_norm_log": float(score[index]),
                        "witnessed": True,
                        "carry_months": 0,
                        "spell_id": state.spell_id,
                        "d_candidate": condition_now,
                        "d_confirmed": True,
                        "d_event_month": state.event_month.date(),
                        "recovery_observed": state.event_price,
                        "censoring": "none",
                    })
                state.carry = 0
            last_witnessed_row = len(rows) - 1
            last_witnessed_index = index
            continue

        # ---- unwitnessed month -------------------------------------------------
        if state.spell_id == 0 or state.bucket is None:
            if in_grid[index]:
                rows.append({
                    **base,
                    "implied_bucket": "NOT_RATED",
                    "spread_norm_log": None,
                    "witnessed": False,
                    "carry_months": 0,
                    "spell_id": spell_seq + 1,
                    "d_candidate": False,
                    "d_confirmed": False,
                    "d_event_month": None,
                    "recovery_observed": None,
                    "censoring": "none",
                })
            continue
        if state.carry == 0 and last_witnessed_index is not None:
            # First month without observation after a witnessed one: the
            # terminal classification of this disappearance is decided here.
            if source_exit_after(last_witnessed_index):
                rows[last_witnessed_row]["censoring"] = "source_exit"
                state.reset(spell_id=0, spell_start=index + 1)
                continue
        state.carry += 1
        if state.confirmed:
            if state.carry > k:
                rows.append(_carry_row(
                    month=month, cusip=cusip, bucket="WITHDRAWN",
                    spell_id=state.spell_id, carry=state.carry,
                    level=level_at(index), censoring="default_absorbing",
                ))
                state.reset(spell_id=0, spell_start=index + 1)
                continue
            rows.append(_carry_row(
                month=month, cusip=cusip, bucket="D", spell_id=state.spell_id,
                carry=state.carry, level=level_at(index), d_confirmed=True,
                d_event_month=state.event_month.date() if state.event_month else None,
                recovery=state.event_price,
            ))
            continue
        if state.carry > k:
            rows.append(_carry_row(
                month=month, cusip=cusip, bucket="WITHDRAWN", spell_id=state.spell_id,
                carry=state.carry, level=level_at(index),
                censoring="withdrawal_absorbing",
            ))
            state.reset(spell_id=0, spell_start=index + 1)
            continue
        rows.append(_carry_row(
            month=month, cusip=cusip, bucket=state.bucket, spell_id=state.spell_id,
            carry=state.carry, level=level_at(index),
        ))
    return rows


def _carry_row(
    *,
    month: pd.Timestamp,
    cusip: str,
    bucket: str,
    spell_id: int,
    carry: int,
    level: float | None,
    censoring: str = "none",
    d_confirmed: bool = False,
    d_event_month: Any = None,
    recovery: float | None = None,
) -> dict[str, Any]:
    return {
        "month": month,
        "cusip_id": cusip,
        "implied_bucket": bucket,
        "spread_norm_log": None,
        "market_level_l": level,
        "witnessed": False,
        "carry_months": carry,
        "spell_id": spell_id,
        "d_candidate": False,
        "d_confirmed": d_confirmed,
        "d_event_month": d_event_month,
        "recovery_observed": recovery,
        "censoring": censoring,
    }


def _closed_frame(
    snapshot: pd.DataFrame, *, last_closed_month: date | pd.Timestamp | str
) -> pd.DataFrame:
    """Filter the grid to the closed months and attach witnessed/score columns."""
    required = (
        "cusip_id", "month", "price", "spread_final_bps", "mod_dur",
        "trade_count", "dollar_volume", "maturity_date",
    )
    missing = [column for column in required if column not in snapshot.columns]
    if missing:
        raise KeyError(f"implied rating build requires columns {missing}")
    frame = snapshot.loc[:, list(required)].copy()
    frame["month"] = pd.to_datetime(frame["month"]).dt.normalize()
    end = pd.Timestamp(last_closed_month).normalize()
    frame = frame.loc[frame["month"].le(end)].sort_values(
        ["cusip_id", "month"], kind="stable"
    ).reset_index(drop=True)
    if frame.duplicated(["cusip_id", "month"]).any():
        raise ValueError("snapshot grid must be unique on (cusip_id, month)")
    frame["witnessed"] = witness_mask(frame)
    frame["spread_norm_log"] = normalized_spread(frame)
    return frame


def market_anchor_for_snapshot(
    snapshot: pd.DataFrame,
    *,
    last_closed_month: date | pd.Timestamp | str,
    window_end_month: str | None = None,
    window_months: int | None = None,
) -> float:
    """Resolve the frozen calibration anchor from the closed snapshot grid.

    The worker resolves it ONCE per build, pins it on the publication and refuses
    to publish under a different anchor than the current one (a forced panel
    republish that rewrote window months would otherwise silently re-bucket every
    historical month).
    """
    frame = _closed_frame(snapshot, last_closed_month=last_closed_month)
    level = market_level(
        frame.loc[frame["witnessed"], ["cusip_id", "month", "spread_norm_log"]]
    )
    return market_anchor(
        level, window_end_month=window_end_month, window_months=window_months
    )


def build_publication_rows(
    snapshot: pd.DataFrame,
    *,
    last_closed_month: date | pd.Timestamp | str,
    l_anchor: float | None = None,
) -> pd.DataFrame:
    """The published cusip x month rows (months <= ``last_closed_month``).

    ``snapshot`` is the served panel snapshot grid with the columns the policy
    consumes: ``cusip_id, month, price, spread_final_bps, mod_dur, trade_count,
    dollar_volume, maturity_date``. All candidates are admitted -- panel
    eligibility is not a witness condition. ``l_anchor`` overrides the frozen
    calibration-window resolution for the calibration notebook (and for the
    worker, which resolves it first so it can pin and guard it); when omitted
    the policy resolves it from the closed history.
    """
    if snapshot.empty:
        return pd.DataFrame(columns=[*PUBLICATION_COLUMNS])
    frame = _closed_frame(snapshot, last_closed_month=last_closed_month)
    if frame.empty:
        return pd.DataFrame(columns=[*PUBLICATION_COLUMNS])
    end = pd.Timestamp(last_closed_month).normalize()
    level = market_level(
        frame.loc[frame["witnessed"], ["cusip_id", "month", "spread_norm_log"]]
    )
    resolved_anchor = market_anchor(level) if l_anchor is None else float(l_anchor)
    beta = _policy_float("market_level", "beta")

    outputs: list[dict[str, Any]] = []
    for cusip, group in frame.groupby("cusip_id", sort=True):
        timeline = _calendar_timeline(group, end=end)
        timeline["market_level_l"] = level.reindex(timeline["month"]).to_numpy()
        timeline["score"] = (
            timeline["spread_norm_log"].to_numpy(dtype="float64")
            - beta
            * (timeline["market_level_l"].to_numpy(dtype="float64") - resolved_anchor)
        )
        outputs.extend(_state_rows_for_cusip(timeline, cusip=str(cusip)))

    rows = pd.DataFrame(outputs)
    if rows.empty:
        return pd.DataFrame(columns=[*PUBLICATION_COLUMNS])
    rows["policy_version"] = POLICY_VERSION
    rows["policy_digest"] = POLICY_DIGEST
    rows = rows.loc[:, list(PUBLICATION_COLUMNS)]
    rows["month"] = pd.to_datetime(rows["month"]).dt.normalize()
    return rows.sort_values(["month", "cusip_id"], kind="stable").reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Digests
# --------------------------------------------------------------------------- #
def _canonical_cell(value: Any) -> str:
    if value is None or value is pd.NaT:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.normalize().date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (np.floating, float)):
        return repr(float(value))
    if isinstance(value, bool) or isinstance(value, np.bool_):
        return "true" if value else "false"
    if isinstance(value, (np.integer, int)):
        return str(int(value))
    return str(value)


def _canonical_rows(frame: pd.DataFrame, columns: tuple[str, ...]) -> list[list[str]]:
    ordered = frame.sort_values(list(columns[:2]), kind="stable")
    return [
        [_canonical_cell(row[column]) for column in columns]
        for _, row in ordered.iterrows()
    ]


def snapshot_fingerprint(snapshot: pd.DataFrame) -> str:
    """Canonical sha256 of the snapshot rows consumed by the build."""
    columns = (
        "month", "cusip_id", "price", "spread_final_bps", "mod_dur",
        "trade_count", "dollar_volume", "maturity_date",
    )
    missing = [column for column in columns if column not in snapshot.columns]
    if missing:
        raise KeyError(f"snapshot fingerprint requires columns {missing}")
    body = _canonical_rows(snapshot.loc[:, list(columns)], columns)
    return hashlib.sha256(
        json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()


def rows_digest(rows: pd.DataFrame) -> str:
    """Canonical sha256 of the published rows."""
    columns = PUBLICATION_COLUMNS
    missing = [column for column in columns if column not in rows.columns]
    if missing:
        raise KeyError(f"rows digest requires columns {missing}")
    body = _canonical_rows(rows.loc[:, list(columns)], columns)
    return hashlib.sha256(
        json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()


def default_counts(rows: pd.DataFrame) -> tuple[int, int]:
    """``(d_confirmed_count, d_candidate_count)`` -- row-level month counts."""
    if rows.empty:
        return 0, 0
    confirmed = int(rows["d_confirmed"].fillna(False).astype(bool).sum())
    candidates = int(rows["d_candidate"].fillna(False).astype(bool).sum())
    return confirmed, candidates
