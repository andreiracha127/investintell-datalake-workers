"""Daily live feed for the bond product: load, aggregate, republish.

The owner's requirement in one line: *"there must be a worker that loads and
recomputes the data daily"*. This is that worker. It runs BEFORE the publication
chain, on its own Railway service, and it leaves the product fresh with no human
in the loop.

Five stages, each REPORTED separately so a partial day is visible rather than
laundered into a green run:

  1. ``candles``  -- delta of daily price/YTM candles for the curated universe
                     into the ``bond_observation_daily`` hypertable, from THIS
                     LANE's own watermark (never the table's: a higher-ranked
                     bulk row this worker cannot overwrite must not move the
                     window past days it never loaded). One or two days per bond.
  2. ``curve``    -- the treasury yield curve into ``bond_yield_curve_daily``:
                     the rate a bond's yield is measured against, and the input
                     a duration-targeted portfolio needs. Watermarked PER TENOR,
                     so a tenor the provider drops for a week recovers its own
                     gap instead of being trimmed at the front the others set.
  3. ``ticks``    -- the previous session's two-sided trade tape for the most
                     active bonds into ``bond_tick_daily``: what it COSTS to
                     trade, which no price series can say.
  4. ``matview``  -- ``REFRESH MATERIALIZED VIEW CONCURRENTLY
                     bond_curated_securities``.
  5. ``republish``-- re-run ``bond_metrics`` then ``bond_serving`` so the served
                     payloads carry the day just loaded.

WHY STAGE 5 EXISTS AT ALL (measured 2026-08-07, do not remove it):
``daily_chain`` keys a run by ``(chain, source_day, code_revision,
config_version)`` and returns a COMPLETED run's summary verbatim without
re-executing it. ``source_day`` is the max watermark over the security/price/
N-CEN/RR1 landing tables -- none of which move when this worker loads candles.
So on a day with no deploy and no filing, the 11:00 chain would find its own run
already complete and execute nothing, and every byte loaded here would sit
unserved. Invoking the two publication workers directly is safe by construction:
both are self-anchored, both take their own advisory locks, and both are
idempotent on their input fingerprint -- so the chain re-running them at 11:00
re-points to the same publication instead of rebuilding it.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
``calc_date`` pins "today" for the window arithmetic (replay/backfill).
``limit`` caps the universe swept in one run, for a budget-bounded catch-up;
the sweep is ordered as a ring (see ``sweep_priority_order``) so successive
capped runs advance instead of re-refreshing the same prefix.

THE EXIT CONTRACT, in one rule: **a run exits green only when it actually did
the day's work.** ``run_worker`` exits non-zero on the top-level ``aborted``
key, and every exit path below that did not load-and-republish the curated
universe sets it. That includes the paths that look like polite no-ops:

===========================  =====  ====================================
state                        green  why
===========================  =====  ====================================
``ok``                       yes    all five stages ran, publications recomputed
``locked``                   no     another holder; this run did NOTHING
``no_observation_table``     no     the target hypertable is absent; nothing loaded
``no_universe``              no     the curated universe is empty/absent
``no_api_key``               no     FINNHUB_API_KEY unset: no candles, no republication
``provider_rejected``        no     the key was rejected mid-sweep (401/403)
``aborted``                  no     the provider cut the candle sweep short
``curve_failed``             no     ALL 13 tenors failed: stage 2 did no work
``ticks_failed``             no     every tick call failed: stage 3 did no work
``matview_failed``           no     REFRESH failed (ownership); the cohort is stale
``republish_locked``         no     a publication worker's lock was held: nothing recomputed
``republish_no_op``          no     a publication worker reported a dark state: nothing published
``republish_failed``         no     a publication worker failed, raised, or returned an
                                    unrecognised state (contract drift is never a success)
``partial_sweep``            no     WORKER_LIMIT truncated the universe: the run covered
                                    its budget, NOT the day
===========================  =====  ====================================

A ``locked`` exit is a typed abort rather than an in-process retry, deliberately.
This service is a daily cron with ``restartPolicy=NEVER`` (deploy IS execution)
at 07:30 UTC and the publication chain runs at 11:00, so an overlap is anomalous
rather than routine; both publication builds take MINUTES, so a seconds-scale
backoff would only sleep and fail anyway, while waiting them out would hold this
worker's own advisory lock and its connection open for an unbounded time -- the
long-transaction trap this repo has already paid for. Failing loudly hands the
decision to an operator, who can restart the service once the holder is gone.
"""

from __future__ import annotations

import datetime as _dt
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg

from src.bonds import live_daily
from src.db import LOCK_BOND_LIVE_DAILY, advisory_lock, connect, resolve_dsn
from src.workers._finnhub import (
    MAX_CONSECUTIVE_FAILURES,
    FinnhubConfigError,
    FinnhubTransientError,
    client_from_env,
)

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "bond_live_daily.sql"

#: The hypertable this worker feeds. Owned (and created) by the serving
#: repository because the app range-scans it on the request path; an absent
#: relation here is a REPORTED no-op, never a silently-created plain table that
#: would lose the time partitioning and the continuous aggregate above it.
OBSERVATION_TABLE = "bond_observation_daily"
CURATED_MATVIEW = "bond_curated_securities"

#: Curve tenors, all verified live 2026-08-07. An unknown code raises a
#: non-retryable 4xx, so the list is pinned rather than guessed; a tenor the
#: provider later drops is reported as a failed unit, not a crashed run.
CURVE_TENORS = ("1m", "2m", "3m", "4m", "6m", "1y", "2y", "3y", "5y", "7y", "10y", "20y", "30y")

#: How many bonds get the (expensive, one-call-per-bond-day) tick treatment.
#: The tape is only informative where it is liquid, and the cost lane is a head
#: product by construction. Override with BOND_TICK_TOP_N.
DEFAULT_TICK_TOP_N = 500

#: Rows per commit. Long transactions hold back VACUUM for the WHOLE database
#: (a trap this repo has already paid for), so the sweep commits in slices.
COMMIT_EVERY = 500


def install_schema(conn: psycopg.Connection) -> None:
    """Apply this worker's own side-table DDL idempotently."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def _relation_exists(conn: psycopg.Connection, name: str) -> bool:
    return bool(conn.execute("SELECT to_regclass(%s)", (name,)).fetchone()[0])


def _int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


# --------------------------------------------------------------------------- #
# Universe
# --------------------------------------------------------------------------- #
# The sweep universe is the curated cohort INTERSECTED with the reference terms,
# because candles are addressed by ISIN and ``bond_reference_terms`` is where the
# CUSIP -> ISIN mapping lives (measured 2026-08-07: all 10,073 curated rows carry
# an ISIN, a coupon and a maturity). The coupon/maturity ride along so a day
# whose yield the provider does not report can still be SOLVED rather than lost.
_UNIVERSE_SQL = """
SELECT r.cusip9, r.isin, r.coupon_rate, r.maturity_date
FROM bond_reference_terms r
JOIN bond_curated_universe u ON u.cusip9 = r.cusip9
WHERE r.isin IS NOT NULL AND btrim(r.isin) <> ''
ORDER BY r.cusip9
"""

# Per-CUSIP watermark: the last day THIS LANE already loaded, read in ONE pass
# rather than per bond.
#
# The source filter is the whole correctness of the delta. The upsert only ever
# refreshes same-or-lower-ranked rows, so a table-wide max(day) is the watermark
# of a lane this worker cannot write: when a higher-ranked bulk row lands later
# than the live feed's own last day, the next window opens at that unrelated day
# and every live day in between is skipped PERMANENTLY -- a delta never looks
# back. (Measured 2026-08-07: 0 curated bonds are frozen that way today, because
# the live feed happens to hold the global max; one reload of TRACE tars into a
# month the live feed already covers is all it takes. The rule has to hold by
# construction, not by the accident of which backfill ran last.)
#
# It is also what preserves the cold start: on a table preloaded with bulk
# history only, an ABSENT live watermark is what puts the bond in
# ``fetch_window``'s intended 30-day window instead of a 500-day one (430 of the
# 10,206 curated bonds, measured 2026-08-07).
#
# The source is INLINED, not bound: a bind parameter cannot be proved to imply
# the partial index's predicate at plan time, and without that index this
# degrades to a sequential scan of every chunk. Measured on production
# (2026-08-07, 34.6M rows / 26 chunks): 45.5s filtered-without-index, 0.10s with
# it -- and 20.8s for the unfiltered query this replaces. The index is versioned
# in schemas/bond_live_daily.sql and its predicate must match this literal.
_WATERMARK_SQL = f"""
SELECT o.cusip9, max(o.day)
FROM {OBSERVATION_TABLE} o
JOIN bond_curated_universe u ON u.cusip9 = o.cusip9
WHERE o.source = '{live_daily.SOURCE_LIVE}'
GROUP BY o.cusip9
"""

# Per-TENOR curve watermark, for the same reason and with a sharper failure
# mode. A table-wide max(day) turns one tenor's outage into a permanent hole:
# while 30y is down the other twelve advance the table max, and the recovered
# 30y response -- which carries the provider's FULL history -- gets trimmed to
# days after that max, discarding exactly the days that were missed. Per tenor,
# the recovered tenor backfills its own gap on the next successful call, and a
# tenor never loaded still has no bound at all, so it cold-starts on its whole
# history instead of being filtered to empty by a bound its neighbours set.
# Served by bond_yield_curve_daily_tenor_day_idx (tenor, day).
_CURVE_WATERMARK_SQL = """
SELECT tenor, max(day)
FROM bond_yield_curve_daily
GROUP BY tenor
"""

# Sweep ring cursor: when this worker last ASKED about each bond. One row per
# curated bond, so this is a ~10k-row seq scan on a table nothing else touches.
# See schemas/bond_live_daily.sql for why the ring is keyed on the ATTEMPT and
# not on the watermark.
_ATTEMPT_SQL = """
SELECT cusip9, last_attempt_at
FROM bond_live_daily_sweep
"""

_SWEEP_STAMP_UPSERT = """
INSERT INTO bond_live_daily_sweep (cusip9, last_attempt_at)
VALUES (%s, now())
ON CONFLICT (cusip9) DO UPDATE SET last_attempt_at = now()
"""

# Idempotent upsert. A conflicting row is replaced only by one of rank >= the
# incumbent: the live feed (the lowest rank) refreshes its OWN rows on a re-run
# -- which is what makes re-reading the watermark day free -- while never
# overwriting the richer bulk history that outranks it.
_OBSERVATION_UPSERT = f"""
INSERT INTO {OBSERVATION_TABLE} ({", ".join(live_daily.OBSERVATION_COLUMNS)})
VALUES ({", ".join(["%s"] * len(live_daily.OBSERVATION_COLUMNS))})
ON CONFLICT (cusip9, day) DO UPDATE SET
    price = EXCLUDED.price, ytm = EXCLUDED.ytm, volume = EXCLUDED.volume,
    price_type = EXCLUDED.price_type, accrued = EXCLUDED.accrued,
    source = EXCLUDED.source, source_rank = EXCLUDED.source_rank,
    ytm_basis = EXCLUDED.ytm_basis
WHERE EXCLUDED.source_rank >= {OBSERVATION_TABLE}.source_rank
"""

_CURVE_UPSERT = f"""
INSERT INTO bond_yield_curve_daily ({", ".join(live_daily.CURVE_COLUMNS)})
VALUES ({", ".join(["%s"] * len(live_daily.CURVE_COLUMNS))})
ON CONFLICT (day, tenor) DO UPDATE SET
    yield_pct = EXCLUDED.yield_pct, source = EXCLUDED.source, loaded_at = now()
"""

_TICK_UPSERT = f"""
INSERT INTO bond_tick_daily ({", ".join(live_daily.TICK_COLUMNS)})
VALUES ({", ".join(["%s"] * len(live_daily.TICK_COLUMNS))})
ON CONFLICT (cusip9, day) DO UPDATE SET
    trade_count = EXCLUDED.trade_count, par_volume = EXCLUDED.par_volume,
    price_median = EXCLUDED.price_median,
    bid_price_median = EXCLUDED.bid_price_median,
    ask_price_median = EXCLUDED.ask_price_median,
    bid_ask_bps = EXCLUDED.bid_ask_bps, yield_median = EXCLUDED.yield_median,
    source = EXCLUDED.source, loaded_at = now()
"""

# Activity ranking for the tick cohort: par volume where the tape reported it,
# otherwise trade days. Bounded to a recent window so the cohort tracks what is
# liquid NOW, not what was liquid in 2008.
_ACTIVITY_SQL = f"""
SELECT o.cusip9, coalesce(sum(o.volume), count(*))
FROM {OBSERVATION_TABLE} o
JOIN bond_curated_universe u ON u.cusip9 = o.cusip9
WHERE o.day >= %(since)s
GROUP BY o.cusip9
"""


#: Sort placeholders for "never" -- only ever compared against real values that
#: are, by construction, later than both.
_NEVER_ATTEMPTED = _dt.datetime.min.replace(tzinfo=_dt.timezone.utc)
_NEVER_LOADED = _dt.date.min


def sweep_priority_order(
    rows: Sequence[Sequence[Any]],
    attempts: Mapping[str, _dt.datetime],
    watermarks: Mapping[str, _dt.date],
) -> list[tuple[Any, ...]]:
    """Order the universe as a RING: least recently attempted, most behind first.

    A capped run (``WORKER_LIMIT``, the documented budget-bounded catch-up) takes
    a PREFIX of this order, so the order is the whole of whether the catch-up
    converges. Sorted by CUSIP -- what this replaces -- the prefix is the same
    ~N bonds every run: the first N stay fresh and the rest are never loaded at
    all, while the run still republishes as though the day were covered.

    Two keys, in this order, and both are load-bearing:

    1. **last attempt, never-attempted first.** This is what makes it a ring.
       After a capped run the bonds it swept carry the newest stamp and sink to
       the back, so the next run gets the next slice and
       ``ceil(universe / limit)`` runs cover everything. Keyed on the ATTEMPT
       rather than on the loaded watermark on purpose: a bond the provider has
       no data for never gains a watermark, and a watermark-only order therefore
       hands the head of every capped run to the same permanently dataless
       cohort -- which starves the rest exactly like the CUSIP prefix once that
       cohort reaches ``limit``. Nothing the provider does can withhold an
       attempt.
    2. **watermark, never-loaded first.** The priority INSIDE a round: among
       bonds last attempted together, the one furthest behind goes first. This
       is also what puts a transient-failed bond (stamped, watermark unmoved)
       ahead of one that loaded cleanly (stamped, watermark today) when the ring
       comes back around -- retry-first falls out of the ordering rather than
       needing a rule of its own.

    The CUSIP is the final tie-break, so the order is total and a replay of the
    same state is byte-identical.
    """
    def key(row: Sequence[Any]) -> tuple[Any, ...]:
        cusip = str(row[0])
        attempt = attempts.get(cusip)
        watermark = watermarks.get(cusip)
        return (
            0 if attempt is None else 1, attempt or _NEVER_ATTEMPTED,
            0 if watermark is None else 1, watermark or _NEVER_LOADED,
            cusip,
        )

    return sorted((tuple(row) for row in rows), key=key)


def _universe(
    conn: psycopg.Connection, limit: int | None
) -> tuple[list[tuple[Any, ...]], int]:
    """``(rows to sweep, size of the whole curated universe)``.

    The total is returned separately because it is the only thing that can say
    whether a capped run covered the day or a slice of it, and a sliced list
    cannot say it about itself.

    The watermarks are read here for the ORDER and again in ``_load_candles``
    for each bond's WINDOW. That is deliberate rather than plumbed through: both
    reads are the same 0.10 s index-only scan (measured 2026-08-07 on 34.6M rows;
    see ``_WATERMARK_SQL``), they run inside one transaction so they cannot
    disagree, and passing the dict along would make the sweep's window depend on
    a caller getting it right.
    """
    if not (_relation_exists(conn, "bond_reference_terms")
            and _relation_exists(conn, "bond_curated_universe")):
        return [], 0
    rows = sweep_priority_order(
        conn.execute(_UNIVERSE_SQL).fetchall(), _attempts(conn), _watermarks(conn)
    )
    return (list(rows[:limit]) if limit else list(rows)), len(rows)


def _watermarks(conn: psycopg.Connection) -> dict[str, _dt.date]:
    """Last day THIS LANE loaded, per bond. A bond absent here cold-starts."""
    return {
        str(cusip): day
        for cusip, day in conn.execute(_WATERMARK_SQL).fetchall()
        if day is not None
    }


def _attempts(conn: psycopg.Connection) -> dict[str, _dt.datetime]:
    """When this worker last ASKED about each bond. Absent = never attempted."""
    return {
        str(cusip): stamp
        for cusip, stamp in conn.execute(_ATTEMPT_SQL).fetchall()
        if stamp is not None
    }


def _curve_watermarks(conn: psycopg.Connection) -> dict[str, _dt.date]:
    """Last day loaded per curve tenor. A tenor absent here cold-starts."""
    return {
        str(tenor): day
        for tenor, day in conn.execute(_CURVE_WATERMARK_SQL).fetchall()
        if day is not None
    }


# --------------------------------------------------------------------------- #
# Stage 1: candles
# --------------------------------------------------------------------------- #
def _load_candles(
    conn: psycopg.Connection, client: Any, universe: list[tuple[Any, ...]], today: _dt.date
) -> dict[str, Any]:
    watermarks = _watermarks(conn)
    swept = fetched = upserted = no_data = failed = 0
    consecutive = 0
    aborted = False
    pending = 0
    dropped_after_window = 0
    first_day: _dt.date | None = None
    last_day: _dt.date | None = None

    for cusip9, isin, coupon_rate, maturity_date in universe:
        swept += 1
        start, end = live_daily.fetch_window(watermarks.get(str(cusip9)), today)
        try:
            try:
                payload = client.daily_candles(
                    str(isin), live_daily.to_epoch(start), live_daily.to_epoch(end)
                )
                consecutive = 0
            except FinnhubTransientError:
                failed += 1
                consecutive += 1
                if consecutive >= MAX_CONSECUTIVE_FAILURES:
                    # The provider is down, not the data missing. Stop rather
                    # than spend the window proving it; the watermark resumes
                    # tomorrow.
                    aborted = True
                    break
                continue
            fold = live_daily.candle_rows(
                str(cusip9), payload, not_before=start, not_after=end,
                coupon_pct=_as_float(coupon_rate), maturity_date=maturity_date,
            )
            dropped_after_window += fold.dropped_after_window
            rows = fold.rows
            if not rows:
                no_data += 1
                continue
            fetched += 1
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(_OBSERVATION_UPSERT, row.as_tuple())
                    upserted += cur.rowcount
                    pending += 1
                    first_day = row.day if first_day is None or row.day < first_day else first_day
                    last_day = row.day if last_day is None or row.day > last_day else last_day
        finally:
            # Stamp the ATTEMPT, whatever came of it -- data, no data, or a
            # transient failure -- and stamp it AFTER the bond's own rows so the
            # sweep never advances its ring past a bond it did not finish. This
            # is the only progress record a capped run leaves, and it rides the
            # same commit cadence as the data: a run the provider cuts short
            # keeps the progress of the prefix it did sweep, so the next one
            # resumes rather than repeating it.
            with conn.cursor() as cur:
                cur.execute(_SWEEP_STAMP_UPSERT, (str(cusip9),))
            pending += 1
            # Checked in ``finally`` because a bond with no data is now a write
            # too: leaving the cadence on the data-bearing path would let 10k
            # stamps accumulate in ONE transaction, which is the long-transaction
            # /VACUUM trap this slicing exists to avoid.
            if pending >= COMMIT_EVERY:
                conn.commit()
                pending = 0
    conn.commit()
    return {
        "swept": swept, "with_data": fetched, "rows_upserted": upserted,
        "no_data": no_data, "transient_failures": failed,
        "first_day": first_day.isoformat() if first_day else None,
        "last_day": last_day.isoformat() if last_day else None,
        # A candle dated past the requested window is refused before it can
        # become a row, because both publications anchor their as_of on this
        # table's max(day): one bad future candle would move the product's
        # as_of forward and then reject every legitimate publication after it
        # as a regression. Counted rather than silent -- a provider that
        # starts emitting them must be visible -- but deliberately NOT a
        # reason to fail the run: the guard already held, and dropping the day
        # would turn a defended anomaly into an outage.
        "dropped_after_window": dropped_after_window,
        "aborted": aborted,
    }


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Stage 2: yield curve
# --------------------------------------------------------------------------- #
def _load_curve(conn: psycopg.Connection, client: Any) -> dict[str, Any]:
    watermarks = _curve_watermarks(conn)
    # A tenor behind the front of the curve is a hole this run may close. It is
    # REPORTED rather than inferred from row counts, because that is the only
    # signal that distinguishes "the provider dropped 30y for a week" from "30y
    # is fine" once the failure itself has scrolled out of the logs.
    front = max(watermarks.values(), default=None)
    lagging = sorted(
        tenor for tenor in CURVE_TENORS
        if front is not None and watermarks.get(tenor, front) < front
    )
    tenors_loaded = 0
    upserted = 0
    failed: list[str] = []
    latest: _dt.date | None = None
    for tenor in CURVE_TENORS:
        try:
            payload = client.yield_curve(tenor)
        except (FinnhubTransientError, FinnhubConfigError):
            # One tenor the provider dropped must not cost the other twelve.
            failed.append(tenor)
            continue
        # Each tenor is trimmed by ITS OWN last loaded day, so a tenor that
        # missed runs recovers the missed days from the full history the
        # endpoint returns, and a tenor never seen has no bound at all.
        points = live_daily.curve_points(
            tenor, payload, not_before=watermarks.get(tenor)
        )
        if not points:
            continue
        tenors_loaded += 1
        with conn.cursor() as cur:
            for point in points:
                cur.execute(_CURVE_UPSERT, point.as_tuple())
                upserted += cur.rowcount
                if latest is None or point.day > latest:
                    latest = point.day
        conn.commit()
    return {
        "tenors": tenors_loaded, "rows_upserted": upserted,
        "failed_tenors": failed,
        "lagging_tenors": lagging,
        "latest_day": latest.isoformat() if latest else None,
    }


# --------------------------------------------------------------------------- #
# Stage 3: ticks
# --------------------------------------------------------------------------- #
def _load_ticks(
    conn: psycopg.Connection, client: Any, universe: list[tuple[Any, ...]], today: _dt.date
) -> dict[str, Any]:
    top_n = _int_env("BOND_TICK_TOP_N", DEFAULT_TICK_TOP_N)
    day = live_daily.previous_business_day(today)
    activity = conn.execute(
        _ACTIVITY_SQL, {"since": today - _dt.timedelta(days=90)}
    ).fetchall()
    cohort = set(live_daily.rank_by_activity(activity, top_n))
    isin_by_cusip = {str(c): str(i) for c, i, _, _ in universe}

    swept = traded = upserted = failed = 0
    for cusip9 in sorted(cohort):
        isin = isin_by_cusip.get(cusip9)
        if not isin:
            continue
        swept += 1
        try:
            ticks = client.ticks(isin, day.isoformat())
        except FinnhubTransientError:
            failed += 1
            continue
        aggregate = live_daily.aggregate_ticks(cusip9, day, ticks)
        if aggregate is None:
            continue
        traded += 1
        with conn.cursor() as cur:
            cur.execute(_TICK_UPSERT, aggregate.as_tuple())
            upserted += cur.rowcount
        if traded % COMMIT_EVERY == 0:
            conn.commit()
    conn.commit()
    return {
        # ``cohort`` vs ``swept``: the cost lane is the top-N by activity, but it
        # can only ask about bonds the CANDLE sweep carried an ISIN for, so a
        # capped run silently shrinks it. Reporting both is what makes that
        # truncation readable instead of inferred.
        "day": day.isoformat(), "cohort": len(cohort), "swept": swept,
        "traded": traded, "rows_upserted": upserted, "transient_failures": failed,
    }


# --------------------------------------------------------------------------- #
# Stage 4: matview
# --------------------------------------------------------------------------- #
def _refresh_curated(dsn: str) -> dict[str, Any]:
    """REFRESH ... CONCURRENTLY needs autocommit (it cannot run in a transaction).

    Postgres requires OWNERSHIP to refresh a materialized view -- a GRANT is not
    enough -- so this is the one stage whose failure mode is a privilege, not
    data. It is caught rather than raised: the refresh runs between the load and
    the republication, and letting it abort would mean a permission problem
    silently costs the product the day's prices, which is the larger harm by
    far. The caller still folds the failure into the run's exit code, so it
    lands as a failed deploy instead of a line in a JSON blob nobody reads.
    """
    with connect(dsn, autocommit=True) as conn:
        if not _relation_exists(conn, CURATED_MATVIEW):
            return {"state": "absent", "matview": CURATED_MATVIEW}
        try:
            with conn.cursor() as cur:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {CURATED_MATVIEW}")
        except Exception as exc:
            return {"state": "failed", "matview": CURATED_MATVIEW,
                    "error": f"{type(exc).__name__}: {exc}"}
    return {"state": "refreshed", "matview": CURATED_MATVIEW}


# --------------------------------------------------------------------------- #
# Stage 5: republish
# --------------------------------------------------------------------------- #
#: The publication workers' OWN result vocabulary, read here exactly as
#: ``daily_chain.classify_worker_result`` reads it -- one vocabulary, two
#: consumers. ``current`` is the shared publication protocol's success state
#: (a self-promoted publication), which overrides the envelope's literal.
_PUBLISHED_STATES = frozenset({"ok", "ready", "current"})
#: Dark no-ops: the worker ran and had nothing to publish.
_DARK_STATES = frozenset({"no_source", "no_securities", "no_observations"})

#: verdict -> the run state it produces. Only ``recomputed`` is green.
_REPUBLISH_STATE = {
    "locked": "republish_locked",
    "no_op": "republish_no_op",
    "failed": "republish_failed",
}


def republish_verdict(result: Mapping[str, Any]) -> str:
    """Classify one publication worker's ``run()`` dict. Only ``recomputed`` passed.

    The three non-success verdicts are kept APART because they need different
    hands: ``locked`` is a collision an operator clears by re-running,
    ``no_op`` is a dark publication (the input the build needs is gone), and
    ``failed`` is a break. An unrecognised state joins ``failed`` rather than
    being tolerated: a sub-worker that grew a state this contract does not know
    is contract drift, and the chain's own rule -- never a silent success -- is
    the only safe reading of it.
    """
    state = str(result.get("state") or result.get("status") or "")
    if state in _PUBLISHED_STATES:
        return "recomputed"
    if state == "locked":
        return "locked"
    if state in _DARK_STATES:
        return "no_op"
    return "failed"


def _republish(dsn: str) -> dict[str, Any]:
    """Re-run the metric and serving publications over the day just loaded.

    Imported lazily and invoked through their own ``run(dsn)`` so each opens its
    own connection and takes its own advisory lock -- the chain's own idiom. A
    failure never rolls back the load (the rows are already durable), but it
    DOES fail the run: the owner asked for a worker that loads *and* recomputes,
    and a day whose recompute failed is a truncated day, not a green one.
    Nothing else will retry it before tomorrow -- the 11:00 chain's run_id does
    not change just because this worker failed -- so a silent green here would
    leave the product a day stale with no signal at all.

    That reasoning is INDIFFERENT to how the recompute failed to happen. Both
    workers return ``{"state": "locked"}`` when their advisory lock is already
    held, and a dark state when they have nothing to publish; in either case
    stage 5 did not run and the load sits unserved behind a green deploy. So the
    verdict here is the one thing that is true of a good run and of nothing
    else: **both publications reported that they published.**
    """
    from src.workers import bond_metrics, bond_serving

    out: dict[str, Any] = {}
    verdict = "recomputed"
    for name, worker in (("bond_metrics", bond_metrics), ("bond_serving", bond_serving)):
        try:
            result = worker.run(dsn)
        except Exception as exc:
            out[name] = {"state": "failed", "error": f"{type(exc).__name__}: {exc}"}
            verdict = "failed"
            break
        out[name] = result
        verdict = republish_verdict(result)
        if verdict != "recomputed":
            # bond_serving consumes the metric view at build time, so a metric
            # publication that did not happen makes the serving build meaningless
            # rather than merely unnecessary. Stop at the first one.
            break
    out["verdict"] = verdict
    return out


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def _halted(state: str, **detail: Any) -> dict[str, Any]:
    """An exit that did NOT do the day's work, in the one shape that says so.

    Every early return in ``run`` goes through here. The point is the invariant,
    not the helper: ``run_worker`` exits non-zero on ``aborted`` alone, so an
    early return that omits it paints a run green that loaded nothing at all --
    an absent secret, a hypertable that never got its DDL, a lock held by
    something else. Those are the failures worth waking up for and the easiest
    ones to launder into a polite JSON no-op.

    ``halted_by`` carries the same shape as the full run's, so an operator (or a
    log query) reads one key on every result rather than one key on the runs that
    got far enough to have a verdict.
    """
    return {"state": state, "aborted": True, "halted_by": [state], **detail}


def run(
    dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None
) -> dict[str, Any]:
    resolved = resolve_dsn(dsn)
    today = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()

    with connect(resolved) as conn, advisory_lock(conn, LOCK_BOND_LIVE_DAILY) as acquired:
        if not acquired:
            # Something else holds this worker's lock, so this run does nothing
            # at all. Typed abort rather than an in-process retry -- see the
            # module docstring for the argument.
            return _halted("locked", lock_id=LOCK_BOND_LIVE_DAILY)
        install_schema(conn)
        conn.commit()
        if not _relation_exists(conn, OBSERVATION_TABLE):
            # The serving repository owns this hypertable's DDL. Reporting the
            # absence beats creating a plain table that would silently lose the
            # partitioning and the continuous aggregate hanging off it. Reported
            # is not the same as tolerated: nothing was loaded, so it is red.
            conn.commit()
            return _halted("no_observation_table", table=OBSERVATION_TABLE)
        universe, universe_total = _universe(conn, limit)
        if not universe:
            conn.commit()
            return _halted("no_universe")
        try:
            client = client_from_env()
        except FinnhubConfigError as exc:
            # A configuration fault, not an empty day: it must look different --
            # and it must not be green. Without the secret there are no candles,
            # no curve, no ticks and no republication, which is a Railway service
            # doing nothing every morning behind a successful deploy.
            conn.commit()
            return _halted("no_api_key", detail=str(exc))

        try:
            candles = _load_candles(conn, client, universe, today)
            curve = _load_curve(conn, client)
            ticks = _load_ticks(conn, client, universe, today)
        except FinnhubConfigError as exc:
            # A key REVOKED mid-sweep (401/403 is non-transient by design, so it
            # is raised rather than counted). Whatever was loaded before it is
            # already committed; the run is typed and red instead of a traceback.
            conn.commit()
            return _halted("provider_rejected", detail=str(exc), provider=client.stats())

    matview = _refresh_curated(resolved)
    republish = _republish(resolved)

    # THE VERDICT. Every stage above has already RUN -- this is computed at the
    # end and never used to skip work -- and each clause below is a way the day
    # is not the day, which ``run_worker`` turns into a non-zero exit through the
    # top-level ``aborted`` key. In severity order, so ``state`` names the thing
    # an operator should look at first.
    verdict = str(republish.get("verdict") or "failed")
    swept = len(universe)
    reasons: list[tuple[bool, str]] = [
        # Stage 5 did not recompute: the load is durable but unserved, and
        # nothing retries it before tomorrow.
        (verdict != "recomputed", _REPUBLISH_STATE.get(verdict, "republish_failed")),
        # The curated cohort the publications read is stale.
        (matview.get("state") == "failed", "matview_failed"),
        # The provider cut the candle sweep short.
        (bool(candles.get("aborted")), "aborted"),
        # A stage that did NO work is not a stage that had nothing to do. All 13
        # tenors failing is the provider or the key, not a dropped tenor -- while
        # a handful of failures is the case the per-tenor watermarks self-heal,
        # so it stays green on purpose.
        (len(curve.get("failed_tenors") or ()) == len(CURVE_TENORS), "curve_failed"),
        (ticks["swept"] > 0 and ticks["transient_failures"] == ticks["swept"],
         "ticks_failed"),
        # A capped run covered its BUDGET, not the day. It still republishes --
        # the rows it loaded are real and every security carries its own
        # observation date, so the payload stays honest -- but the run must not
        # report the day as done while most of the universe was never asked
        # about. The ring ordering means the next runs finish it; this state is
        # what says it is not finished yet.
        (swept < universe_total, "partial_sweep"),
    ]
    triggered = [state for hit, state in reasons if hit]
    return {
        "state": triggered[0] if triggered else "ok",
        "aborted": bool(triggered),
        "halted_by": triggered,
        "as_of": today.isoformat(),
        "coverage": {
            "universe": universe_total,
            "swept": swept,
            "remaining": universe_total - swept,
            "complete": swept == universe_total,
            "limit": limit,
        },
        "candles": candles,
        "curve": curve,
        "ticks": ticks,
        "matview": matview,
        "republish": republish,
        "provider": client.stats(),
    }
