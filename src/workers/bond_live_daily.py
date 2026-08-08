"""Daily live feed for the bond product: load, aggregate, republish.

The owner's requirement in one line: *"there must be a worker that loads and
recomputes the data daily"*. This is that worker. It runs BEFORE the publication
chain, on its own Railway service, and it leaves the product fresh with no human
in the loop.

Five stages, each REPORTED separately so a partial day is visible rather than
laundered into a green run:

  1. ``candles``  -- delta of daily price/YTM candles for the curated universe
                     into the ``bond_observation_daily`` hypertable, from the
                     table's own watermark. Typically one or two days per bond.
  2. ``curve``    -- the treasury yield curve into ``bond_yield_curve_daily``:
                     the rate a bond's yield is measured against, and the input
                     a duration-targeted portfolio needs.
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
``limit`` caps the universe swept in one run, for a budget-bounded catch-up.
Exit code is the platform contract: a sweep that stopped early on provider
failures reports ``aborted`` and ``run_worker`` exits non-zero, so a truncated
day is never painted green.
"""

from __future__ import annotations

import datetime as _dt
import os
from pathlib import Path
from typing import Any

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

# Per-CUSIP watermark: the last day already loaded. Restricted to this worker's
# own source so a bond whose bulk history reaches further does not freeze the
# live lane behind it -- and read in ONE pass rather than per bond.
_WATERMARK_SQL = f"""
SELECT o.cusip9, max(o.day)
FROM {OBSERVATION_TABLE} o
JOIN bond_curated_universe u ON u.cusip9 = o.cusip9
GROUP BY o.cusip9
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


def _universe(conn: psycopg.Connection, limit: int | None) -> list[tuple[Any, ...]]:
    if not (_relation_exists(conn, "bond_reference_terms")
            and _relation_exists(conn, "bond_curated_universe")):
        return []
    rows = conn.execute(_UNIVERSE_SQL).fetchall()
    return list(rows[:limit]) if limit else list(rows)


def _watermarks(conn: psycopg.Connection) -> dict[str, _dt.date]:
    return {
        str(cusip): day
        for cusip, day in conn.execute(_WATERMARK_SQL).fetchall()
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
    first_day: _dt.date | None = None
    last_day: _dt.date | None = None

    for cusip9, isin, coupon_rate, maturity_date in universe:
        swept += 1
        start, end = live_daily.fetch_window(watermarks.get(str(cusip9)), today)
        try:
            payload = client.daily_candles(
                str(isin), live_daily.to_epoch(start), live_daily.to_epoch(end)
            )
            consecutive = 0
        except FinnhubTransientError:
            failed += 1
            consecutive += 1
            if consecutive >= MAX_CONSECUTIVE_FAILURES:
                # The provider is down, not the data missing. Stop rather than
                # spend the window proving it; the watermark resumes tomorrow.
                aborted = True
                break
            continue
        rows = live_daily.candle_rows(
            str(cusip9), payload, not_before=start,
            coupon_pct=_as_float(coupon_rate), maturity_date=maturity_date,
        )
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
        if pending >= COMMIT_EVERY:
            conn.commit()
            pending = 0
    conn.commit()
    return {
        "swept": swept, "with_data": fetched, "rows_upserted": upserted,
        "no_data": no_data, "transient_failures": failed,
        "first_day": first_day.isoformat() if first_day else None,
        "last_day": last_day.isoformat() if last_day else None,
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
    row = conn.execute("SELECT max(day) FROM bond_yield_curve_daily").fetchone()
    watermark: _dt.date | None = row[0] if row else None
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
        points = live_daily.curve_points(tenor, payload, not_before=watermark)
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
        "day": day.isoformat(), "swept": swept, "traded": traded,
        "rows_upserted": upserted, "transient_failures": failed,
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
    """
    from src.workers import bond_metrics, bond_serving

    out: dict[str, Any] = {}
    failed = False
    for name, worker in (("bond_metrics", bond_metrics), ("bond_serving", bond_serving)):
        try:
            result = worker.run(dsn)
        except Exception as exc:
            out[name] = {"state": "failed", "error": f"{type(exc).__name__}: {exc}"}
            failed = True
            break
        out[name] = result
        if str(result.get("state")) == "failed":
            failed = True
            break
    out["failed"] = failed
    return out


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def run(
    dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None
) -> dict[str, Any]:
    resolved = resolve_dsn(dsn)
    today = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()

    with connect(resolved) as conn, advisory_lock(conn, LOCK_BOND_LIVE_DAILY) as acquired:
        if not acquired:
            return {"state": "locked"}
        install_schema(conn)
        conn.commit()
        if not _relation_exists(conn, OBSERVATION_TABLE):
            # The serving repository owns this hypertable's DDL. Reporting the
            # absence beats creating a plain table that would silently lose the
            # partitioning and the continuous aggregate hanging off it.
            conn.commit()
            return {"state": "no_observation_table", "table": OBSERVATION_TABLE}
        universe = _universe(conn, limit)
        if not universe:
            conn.commit()
            return {"state": "no_universe"}
        try:
            client = client_from_env()
        except FinnhubConfigError as exc:
            # A configuration fault, not an empty day: it must look different.
            conn.commit()
            return {"state": "no_api_key", "detail": str(exc)}

        candles = _load_candles(conn, client, universe, today)
        curve = _load_curve(conn, client)
        ticks = _load_ticks(conn, client, universe, today)

    matview = _refresh_curated(resolved)
    republish = _republish(resolved)

    # Three ways a day is truncated, and NONE may be painted green: a sweep the
    # provider cut short, a matview left stale, and a republication that failed
    # (the load is durable but unserved). ``run_worker`` reads the top-level
    # ``aborted`` key and exits non-zero on it, so each shows up as a failed
    # deploy instead of a log line nobody reads. Every stage still RAN -- the
    # verdict is computed at the end, never used to skip work.
    republish_failed = bool(republish.pop("failed", False))
    matview_failed = matview.get("state") == "failed"
    aborted = bool(candles.get("aborted")) or republish_failed or matview_failed
    return {
        "state": ("republish_failed" if republish_failed
                  else "matview_failed" if matview_failed
                  else "aborted" if aborted else "ok"),
        "aborted": aborted,
        "as_of": today.isoformat(),
        "universe": len(universe),
        "candles": candles,
        "curve": curve,
        "ticks": ticks,
        "matview": matview,
        "republish": republish,
        "provider": client.stats(),
    }
