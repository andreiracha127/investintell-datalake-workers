"""Pure, DB-free transforms behind the ``bond_live_daily`` worker.

Everything here is a total function over provider payloads: no connection, no
clock, no network. The worker module owns the I/O; this module owns the
arithmetic and the unit discipline, which is where a daily feed silently goes
wrong.

UNITS -- pinned here because three conventions meet in this file:

  * the provider quotes a bond price as PERCENT OF PAR, clean (98.39), and a
    bond yield in PERCENT per annum (4.058);
  * ``bond_observation_daily.ytm`` is a FRACTION (0.04058), matching
    ``bond_metric_v1.security_ytm``, so every yield is divided by 100 exactly
    once, at the boundary in :func:`candle_rows`;
  * ``bond_yield_curve_daily.yield_pct`` stays PERCENT, because a curve tenor is
    a quoted rate and the column name says so.

A value the provider does not send is ABSENT (``None``), never zero-filled: a
candle carries no volume, so ``volume`` is None rather than 0, and a day with no
reported yield gets a SOLVED yield with ``ytm_basis='computed'`` or, when the
terms cannot support one, no yield at all.
"""

from __future__ import annotations

import datetime as _dt
import math as _math
from dataclasses import dataclass
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

# --------------------------------------------------------------------------- #
# Source precedence. The hypertable's conflict rule replaces a row only with one
# of rank >= the incumbent, so the live feed (the lowest rank) can refresh its
# OWN rows on a re-run while never overwriting the richer bulk history that sits
# above it. These are the ranks the loaded history already carries.
# --------------------------------------------------------------------------- #
SOURCE_LIVE = "finnhub"
SOURCE_RANK_LIVE = 1

#: Basis token stored next to a yield: did the day's source publish it, or did
#: we solve it from reported terms?
BASIS_REPORTED = "reported"
BASIS_COMPUTED = "computed"

#: The price basis every source in this table reports (measured across the whole
#: loaded history 2026-08-07: 34.6M rows, all 'trade'/'clean'). Written
#: explicitly rather than inherited so a provider change shows up as a diff.
PRICE_TYPE = "trade"
ACCRUED = "clean"

#: Trade-tape side semantics, empirical (see schemas/bond_live_daily.sql).
BID_SIDE = 1
ASK_SIDE = 2

#: Bisection bracket for :func:`analytical_ytm`, as a fraction. Wide enough for
#: a deeply distressed bond and for the negative-yield tail; a price the bracket
#: cannot reach is REFUSED rather than clamped to an endpoint, which would
#: publish a fabricated yield as if it had been solved.
YTM_BRACKET = (-0.5, 5.0)
_YTM_ITERS = 80

_DAY_SECONDS = 86_400


# --------------------------------------------------------------------------- #
# Yield solving (used only when the provider reports no yield)
# --------------------------------------------------------------------------- #
def _bullet_dirty_price(
    ytm: float, coupon_pct: float, periods: float, period_frac: float
) -> float:
    """Dirty price of a semiannual bullet at ``ytm``, in points of par.

    Coupons fall ``period_frac``, ``period_frac + 1``, ... periods out, so the
    annuity is SHIFTED rather than assumed to start a full period away::

        sum_k c/(1+h)^(k+w) = c * (1-(1+h)^-n)/h * (1+h)^(1-w)
    """
    half_y = ytm / 2.0
    half_c = coupon_pct / 2.0
    # Removable singularity at a zero yield: the annuity tends to ``n``.
    safe = 1e-12 if abs(half_y) < 1e-12 else half_y
    annuity = (1.0 - (1.0 + safe) ** (-periods)) / safe
    pv_coupons = half_c * annuity * (1.0 + safe) ** (1.0 - period_frac)
    pv_principal = 100.0 * (1.0 + safe) ** (-(periods - 1.0 + period_frac))
    return pv_coupons + pv_principal


def analytical_ytm(
    clean_price: float | None, coupon_pct: float | None, years_to_maturity: float | None
) -> float | None:
    """Yield of a semiannual fixed-rate bullet, solved by bisection. FRACTION.

    Given a CLEAN price in points of par, the annual coupon in points, and the
    settlement-to-maturity year fraction, return the yield as a fraction
    (0.048 = 4.8%).

    Accrued interest is rebuilt from the position inside the current coupon
    period before solving. That correction is the whole reason this is not a
    two-line root-find: a yield solved against a CLEAN price is wrong by most of
    a coupon, and the error is largest exactly where it is least visible -- just
    before a coupon date, on a high-coupon bond.

    Returns ``None`` (never a clamped endpoint, never NaN) when the inputs
    cannot support a yield: absent price/coupon/maturity, a matured bond, a
    non-positive price, or a price the bracket cannot reach.
    """
    price = _finite(clean_price)
    coupon = _finite(coupon_pct)
    ttm = _finite(years_to_maturity)
    if price is None or coupon is None or ttm is None:
        return None
    if price <= 0 or coupon < 0 or ttm <= 0:
        return None

    # Whole half-periods remaining, and where inside the current one we sit.
    periods = max(float(_math.ceil(ttm * 2.0)), 1.0)
    period_frac = min(max(ttm * 2.0 - (periods - 1.0), 1e-6), 1.0)
    dirty = price + (coupon / 2.0) * (1.0 - period_frac)

    low, high = YTM_BRACKET
    # Price falls as yield rises, so the bracket is walked inverted.
    pv_low = _bullet_dirty_price(low, coupon, periods, period_frac)
    pv_high = _bullet_dirty_price(high, coupon, periods, period_frac)
    if not (pv_high <= dirty <= pv_low):
        return None

    lo, hi = low, high
    for _ in range(_YTM_ITERS):
        mid = (lo + hi) / 2.0
        if _bullet_dirty_price(mid, coupon, periods, period_frac) > dirty:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def _finite(value: Any) -> float | None:
    """Coerce to a finite float, or ``None``. Booleans are never numbers here."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


# --------------------------------------------------------------------------- #
# Candles -> observation rows
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ObservationRow:
    """One (cusip9, day) row destined for ``bond_observation_daily``."""

    cusip9: str
    day: _dt.date
    price: float | None
    ytm: float | None
    volume: float | None
    price_type: str
    accrued: str
    source: str
    source_rank: int
    ytm_basis: str | None

    def as_tuple(self) -> tuple[Any, ...]:
        """Column order shared verbatim with the worker's INSERT (one definition)."""
        return (
            self.cusip9, self.day, self.price, self.ytm, self.volume,
            self.price_type, self.accrued, self.source, self.source_rank, self.ytm_basis,
        )


#: Column protocol of :meth:`ObservationRow.as_tuple`.
OBSERVATION_COLUMNS = (
    "cusip9", "day", "price", "ytm", "volume",
    "price_type", "accrued", "source", "source_rank", "ytm_basis",
)


@dataclass(frozen=True)
class CandleFold:
    """What one candle payload folded into: the rows, and what was REFUSED.

    ``dropped_after_window`` counts candles whose day fell PAST the requested
    upper bound. It is carried out of the pure layer rather than dropped on the
    floor because a provider that starts emitting future-dated candles is a
    silent, compounding failure: both ``bond_metric_v1`` and the serving surface
    anchor their ``as_of`` on ``max(day)`` of ``bond_observation_daily``, so one
    such candle would date the whole product into the future and then make every
    legitimate later publication look like an as-of REGRESSION -- a jam only a
    manual delete in production clears. A counter in the stage report is what
    turns that from an outage into a line someone can read.

    Deliberately asymmetric: the LOWER bound is trimmed silently, because a
    provider returning more history than was asked for is expected and
    harmless. Only the upper side is anomaly, so only the upper side is counted.
    """

    rows: list[ObservationRow]
    dropped_after_window: int = 0


def day_from_timestamp(ts: Any) -> _dt.date | None:
    """UTC calendar day of a provider epoch-second stamp, or ``None``.

    The provider stamps each daily candle at 00:00 UTC of its own day, so the
    conversion is a floor to the UTC day -- deliberately NOT a local-time one,
    which would shift every candle by a day for half the world's operators.
    """
    seconds = _finite(ts)
    if seconds is None:
        return None
    try:
        return _dt.datetime.fromtimestamp(
            (int(seconds) // _DAY_SECONDS) * _DAY_SECONDS, tz=_dt.timezone.utc
        ).date()
    except (OverflowError, OSError, ValueError):
        return None


def candle_rows(
    cusip9: str,
    candles: Mapping[str, Any] | None,
    *,
    not_before: _dt.date | None = None,
    not_after: _dt.date | None = None,
    coupon_pct: float | None = None,
    maturity_date: _dt.date | None = None,
) -> CandleFold:
    """Fold one provider candle payload into observation rows.

    ``not_before`` and ``not_after`` are the INCLUSIVE bounds of the window the
    request actually asked for -- ``[from, to]``, the same pair
    :func:`fetch_window` handed the client. Both are enforced here, at the only
    place observation rows are minted, and for different reasons:

      * BELOW ``not_before``: a provider that returns more history than was
        asked for would widen a delta run into a re-load. Expected, harmless,
        trimmed silently.
      * ABOVE ``not_after``: a candle dated past the end of the requested window
        cannot be a real session -- the request could not have covered it. It is
        a provider bug, a bad stamp, or a timezone slip, and it is DANGEROUS
        rather than merely wrong: ``max(day)`` of ``bond_observation_daily`` is
        the as-of anchor of both downstream publications, so one such row dates
        the product into the future and turns every legitimate later publication
        into an as-of regression. Refused, and COUNTED (see :class:`CandleFold`)
        so a provider that starts emitting garbage shows up in the stage report
        instead of in a stuck chain three days later.

    ``day == not_after`` is kept: the window is inclusive at both ends, and the
    last day of the window is the whole point of a daily run. Passing ``None``
    for either bound leaves that side unbounded.

    The yield is taken from the payload when present (``ytm_basis='reported'``)
    and otherwise SOLVED from the reported terms against that day's own price
    (``ytm_basis='computed'``). A row whose yield can be neither read nor solved
    still lands -- with a price and a NULL yield -- because dropping it would
    silently punch a hole in the price series to protect a yield nobody asked
    for.

    A day with no usable price is dropped entirely: ``bond_observation_daily``
    is a price series, and a row with neither price nor yield carries nothing.
    """
    if not candles or str(candles.get("s") or "") != "ok":
        return CandleFold(rows=[])
    stamps = candles.get("t")
    if not isinstance(stamps, list):
        return CandleFold(rows=[])
    closes = _column(candles, "c", len(stamps))
    yields = _column(candles, "y", len(stamps))

    rows: list[ObservationRow] = []
    seen: set[_dt.date] = set()
    dropped_after = 0
    for index, stamp in enumerate(stamps):
        day = day_from_timestamp(stamp)
        if day is None or (not_before is not None and day < not_before):
            continue
        # Checked BEFORE the price and before the supersession bookkeeping: a
        # future-dated candle is refused whatever it carries, and must never be
        # able to evict a valid row for a day it happens to collide with.
        if not_after is not None and day > not_after:
            dropped_after += 1
            continue
        price = _finite(closes[index])
        if price is None or price <= 0:
            continue
        # A payload that repeats a day would otherwise raise ON CONFLICT ...
        # cannot affect row a second time inside one statement. Last write wins,
        # matching the "later element supersedes" reading of an ordered series.
        if day in seen:
            rows = [r for r in rows if r.day != day]
        seen.add(day)

        reported = _finite(yields[index])
        if reported is not None:
            ytm: float | None = reported / 100.0
            basis: str | None = BASIS_REPORTED
        else:
            ytm = analytical_ytm(
                price, coupon_pct, _years_to_maturity(day, maturity_date)
            )
            basis = BASIS_COMPUTED if ytm is not None else None
        rows.append(
            ObservationRow(
                cusip9=cusip9, day=day, price=price, ytm=ytm, volume=None,
                price_type=PRICE_TYPE, accrued=ACCRUED,
                source=SOURCE_LIVE, source_rank=SOURCE_RANK_LIVE, ytm_basis=basis,
            )
        )
    rows.sort(key=lambda r: r.day)
    return CandleFold(rows=rows, dropped_after_window=dropped_after)


def _column(payload: Mapping[str, Any], key: str, length: int) -> list[Any]:
    """A provider column padded to ``length`` -- an absent one is all-absent."""
    values = payload.get(key)
    if not isinstance(values, list):
        return [None] * length
    if len(values) < length:
        return list(values) + [None] * (length - len(values))
    return list(values[:length])


def _years_to_maturity(day: _dt.date, maturity_date: _dt.date | None) -> float | None:
    """Settlement-to-maturity in years (365.25), or ``None`` past maturity."""
    if maturity_date is None:
        return None
    days = (maturity_date - day).days
    return days / 365.25 if days > 0 else None


# --------------------------------------------------------------------------- #
# Yield curve
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CurvePoint:
    day: _dt.date
    tenor: str
    yield_pct: float

    def as_tuple(self) -> tuple[Any, ...]:
        return (self.day, self.tenor, self.yield_pct, SOURCE_LIVE)


CURVE_COLUMNS = ("day", "tenor", "yield_pct", "source")


def curve_points(
    tenor: str, payload: Mapping[str, Any] | None, *, not_before: _dt.date | None = None
) -> list[CurvePoint]:
    """Fold one tenor's provider series into curve points.

    ``not_before`` is an INCLUSIVE lower bound. The provider returns the whole
    history in one response, so the worker asks once and lets this trim it: a
    table that was never loaded backfills itself on the first run (the curve is
    ~9k points per tenor, not a bulk load), and a loaded one costs one
    comparison per point.
    """
    if not payload:
        return []
    data = payload.get("data")
    if not isinstance(data, list):
        return []
    points: dict[_dt.date, CurvePoint] = {}
    for item in data:
        if not isinstance(item, Mapping):
            continue
        day = _iso_date(item.get("d"))
        value = _finite(item.get("v"))
        if day is None or value is None:
            continue
        if not_before is not None and day < not_before:
            continue
        points[day] = CurvePoint(day=day, tenor=tenor, yield_pct=value)
    return sorted(points.values(), key=lambda p: p.day)


def _iso_date(value: Any) -> _dt.date | None:
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        try:
            return _dt.date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


# --------------------------------------------------------------------------- #
# Ticks -> one daily aggregate
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TickAggregate:
    cusip9: str
    day: _dt.date
    trade_count: int
    par_volume: float | None
    price_median: float | None
    bid_price_median: float | None
    ask_price_median: float | None
    bid_ask_bps: float | None
    yield_median: float | None

    def as_tuple(self) -> tuple[Any, ...]:
        return (
            self.cusip9, self.day, self.trade_count, self.par_volume,
            self.price_median, self.bid_price_median, self.ask_price_median,
            self.bid_ask_bps, self.yield_median, SOURCE_LIVE,
        )


TICK_COLUMNS = (
    "cusip9", "day", "trade_count", "par_volume", "price_median",
    "bid_price_median", "ask_price_median", "bid_ask_bps", "yield_median", "source",
)


def aggregate_ticks(
    cusip9: str, day: _dt.date, ticks: Mapping[str, Any] | None
) -> TickAggregate | None:
    """Collapse one bond-day of columnar ticks into a single aggregate row.

    Returns ``None`` for a day with no usable trade -- an untraded bond gets no
    row rather than a row of zeros, so ``count(*)`` over this table means "days
    this bond actually printed".

    ``bid_ask_bps`` is present ONLY when both sides traded. A one-sided day is a
    day whose spread the tape did not reveal, and a fabricated 0 there would
    read as a frictionless bond -- the single most expensive lie a transaction
    cost table can tell an optimizer.
    """
    if not ticks:
        return None
    stamps = ticks.get("t")
    if not isinstance(stamps, list) or not stamps:
        return None
    length = len(stamps)
    prices = _column(ticks, "p", length)
    volumes = _column(ticks, "v", length)
    sides = _column(ticks, "si", length)
    yields = _column(ticks, "y", length)

    all_prices: list[float] = []
    bid_prices: list[float] = []
    ask_prices: list[float] = []
    all_yields: list[float] = []
    volume_total = 0.0
    volume_seen = False
    trades = 0

    for index in range(length):
        price = _finite(prices[index])
        if price is None or price <= 0:
            continue
        trades += 1
        all_prices.append(price)
        side = _finite(sides[index])
        if side is not None:
            if int(side) == BID_SIDE:
                bid_prices.append(price)
            elif int(side) == ASK_SIDE:
                ask_prices.append(price)
        volume = _finite(volumes[index])
        if volume is not None:
            volume_total += volume
            volume_seen = True
        reported_yield = _finite(yields[index])
        if reported_yield is not None:
            all_yields.append(reported_yield)

    if not trades:
        return None

    bid_ask_bps: float | None = None
    bid_median = median(bid_prices) if bid_prices else None
    ask_median = median(ask_prices) if ask_prices else None
    if bid_median is not None and ask_median is not None:
        mid = (bid_median + ask_median) / 2.0
        if mid > 0:
            # Negative values are KEPT: crossing medians on a thin day are real.
            bid_ask_bps = (ask_median - bid_median) / mid * 10_000.0

    return TickAggregate(
        cusip9=cusip9,
        day=day,
        trade_count=trades,
        par_volume=volume_total if volume_seen else None,
        price_median=median(all_prices),
        bid_price_median=bid_median,
        ask_price_median=ask_median,
        bid_ask_bps=bid_ask_bps,
        yield_median=median(all_yields) if all_yields else None,
    )


# --------------------------------------------------------------------------- #
# Window arithmetic
# --------------------------------------------------------------------------- #
def fetch_window(
    watermark: _dt.date | None, today: _dt.date, *, cold_start_days: int = 30
) -> tuple[_dt.date, _dt.date]:
    """The ``[from, to]`` calendar window one delta run asks the provider for.

    ``to`` is ``today`` (the provider clips to what it has; asking past the tape
    costs nothing and means a run started before the tape lands still catches up
    tomorrow). ``from`` is the watermark day ITSELF, not the day after it: the
    watermark day is re-read every run so a source that later revises the
    session it first printed is picked up instead of frozen at that first print.
    Re-reading is free because the conflict rule replaces a same-source row.

    A cold table (no watermark) asks for ``cold_start_days`` -- deliberately a
    small window, not the full history: bulk history is a load, not a daily job,
    and a worker that silently pulls 24 years on a fresh table would exhaust the
    provider budget before anyone noticed the table was empty.

    CONTRACT, relied on by :func:`candle_rows`: ``to`` is ALWAYS ``today`` --
    never later, for any watermark, including one already ahead of today. That
    is what makes the ``not_after`` bound an execution-date ceiling rather than
    merely a request parameter, without this clock-free module ever reading a
    clock. It is pinned by test, so an edit that lets ``to`` run past the
    execution date fails rather than quietly re-opening the future-day hole.
    """
    if watermark is None:
        start = today - _dt.timedelta(days=cold_start_days)
    else:
        start = watermark  # inclusive re-read of the watermark day itself
    if start > today:
        start = today
    return start, today


def to_epoch(day: _dt.date) -> int:
    """UTC midnight of ``day`` as epoch seconds (the provider's window unit)."""
    return int(
        _dt.datetime(day.year, day.month, day.day, tzinfo=_dt.timezone.utc).timestamp()
    )


def previous_business_day(day: _dt.date) -> _dt.date:
    """The prior Mon-Fri calendar day.

    Deliberately weekday-only: this repo has no trading calendar, and inventing
    one here would make the tick lane wrong on exactly the days a holiday
    calendar is supposed to fix. A holiday simply yields a day with no trades,
    which :func:`aggregate_ticks` already reports as absence.
    """
    probe = day - _dt.timedelta(days=1)
    while probe.weekday() >= 5:
        probe -= _dt.timedelta(days=1)
    return probe


def rank_by_activity(rows: Iterable[Sequence[Any]], limit: int) -> list[str]:
    """The ``limit`` most-active CUSIP9s from ``(cusip9, activity)`` pairs.

    Ties break on the CUSIP so the tick cohort is deterministic across runs --
    an arbitrary tie-break would silently rotate which bonds have cost data.
    """
    scored = [
        (str(row[0]), _finite(row[1]) or 0.0)
        for row in rows
        if row and row[0]
    ]
    scored.sort(key=lambda item: (-item[1], item[0]))
    return [cusip for cusip, _ in scored[: max(0, limit)]]
