"""instrument_ingestion worker — Tiingo daily prices → nav_timeseries.

Standalone reimplementation of the monolith ``instrument_ingestion`` worker
(reference: ``app/jobs/workers/instrument_ingestion.py``), adapted from the
monolith's full-universe ~15y refetch to a **stale-only, AUM-prioritised
sweep**: only tickers whose newest nav_date is stale are fetched, from their
watermark, so a daily run moves a fraction of the data. The account's Tiingo
budget was verified empirically on 2026-06-12 (150 req in 2.9s, zero 429 —
Power tier, 10k req/h), so ``DEFAULT_TICKER_CAP`` admits the whole ~6.1k-ticker
universe in a single run; fetches run on ``FETCH_CONCURRENCY`` threads paced
at 10 req/s (a full sweep is ~6.1k requests ≈ 10 min, still under the hourly
budget), upserts stay on the main thread/connection. The cap remains as a
guard against unbounded universe growth.

Faithful where it matters:
  * universe = ``instruments_universe WHERE is_active AND ticker IS NOT NULL``;
    one Tiingo call per **unique** ticker, fanned out to every instrument
    sharing it (each row keeps its instrument's currency);
  * ``adjClose`` preferred (split/dividend-adjusted), fallback ``close``;
  * rows: nav rounded 6, log ``return_1d`` rounded 8, ``return_type='log'``,
    ``source='tiingo'`` — matching the 26.8M existing rows;
  * upsert ON CONFLICT (instrument_id, nav_date) DO UPDATE in statement
    chunks of 500 inside ONE transaction per instrument, together with its
    attempt and row evidence (no intermediate commits);
  * 30-consecutive-429 breaker aborts cleanly (resume next cycle from the
    watermarks). ~416 no-ticker instruments are skipped by design.

UCITS coverage: tickers Tiingo returns empty for (the ``.L/.PA/.MI/.SW``
European share classes — design §1D provider gap) fall through to the
``_fallback_nav`` chain: EODHD when ``EODHD_API_KEY`` is set, else Yahoo
(which fed the existing 622k ``source='yahoo'`` rows). Rows carry the actual
provider in ``source``; ``stats["fallback_loaded"]`` reports the split.

Contract:  run(dsn, *, calc_date=None, limit=None) -> {"fetched", "upserted", ...}
``limit`` overrides DEFAULT_TICKER_CAP. Env: TIINGO_API_KEY.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime as _dt
import math
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row

from src.db import LOCK_FUND_NAV_READINESS, LOCK_INSTRUMENT_INGESTION, advisory_lock, connect
from src.workers._nav_policy import (
    ADJUSTED_OVERLAP_ABS_TOL,
    ADJUSTED_OVERLAP_REL_TOL,
    CALENDAR_FIELDS,
    effective_calendar_tuple,
    level_evidence_digest,
)
from src.workers._nav_sanitize import REPAIRED_NAV_KINDS, sanitize_nav_series
from src.workers._tiingo import (
    DEFAULT_RATE_PER_S, NavFetchResult, NavObservation, TiingoBudgetExceeded, TiingoClient,
)

UPSERT_CHUNK = 500
DEFAULT_LOOKBACK_DAYS = 5475   # ~15y for first-time backfills
WATERMARK_OVERLAP_DAYS = 7     # re-fetch overlap to catch revisions
STALE_AFTER_DAYS = 2           # weekend-tolerant: refreshed daily data is fresh
DEFAULT_TICKER_CAP = 10_000    # full universe fits the verified 10k req/h budget
# Railway service: 24 vCPU / 24 GB. Fetches are I/O-bound; concurrency matches
# the cores and the bucket caps the burst. The old note — "a full sweep is ~6.1k
# requests, so even at 25 req/s the hourly total stays under 10k/h" — measured
# the sweep, not the rate: 6.1k requests at 25 req/s land in ~4 minutes, taking
# 61% of the fleet's hourly budget in one burst and leaving whatever runs next
# with 429s. The budget is per account, not per sweep.
FETCH_CONCURRENCY = 24         # parallel Tiingo fetches (upserts stay single-conn)
FETCH_RATE_PER_S = DEFAULT_RATE_PER_S


@dataclass(frozen=True)
class TickerPlan:
    """One Tiingo fetch: a ticker, its start date and target instruments."""

    ticker: str
    start_date: _dt.date
    instruments: tuple[tuple[Any, str], ...]  # (instrument_id, currency)
    max_aum: float | None


# ──────────────────────────────────────────────────────────────────────────────
# Pure planning + row building
# ──────────────────────────────────────────────────────────────────────────────
def select_stale_tickers(universe: list[dict[str, Any]],
                         watermarks: dict[str, _dt.date],
                         as_of: _dt.date, cap: int, *,
                         target_session: _dt.date | None = None) -> list[TickerPlan]:
    """Stale-only, AUM-prioritised fetch plan (one entry per unique ticker).

    A ticker is stale when it has no NAV history or its newest nav_date is
    older than STALE_AFTER_DAYS. Plans are ordered by AUM descending (NULLs
    last) and capped to bound the run within the Tiingo budget.
    """
    if target_session is not None and target_session != as_of:
        raise ValueError("target_session must match the requested as_of date")
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for inst in universe:
        ticker = (inst.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        by_ticker.setdefault(ticker, []).append(inst)

    plans: list[TickerPlan] = []
    threshold = as_of - _dt.timedelta(days=STALE_AFTER_DAYS)
    for ticker, instruments in by_ticker.items():
        wm = watermarks.get(ticker)
        if wm is not None and (wm >= target_session if target_session is not None
                               else wm >= threshold):
            continue  # fresh
        start = (wm - _dt.timedelta(days=WATERMARK_OVERLAP_DAYS) if wm is not None
                 else as_of - _dt.timedelta(days=DEFAULT_LOOKBACK_DAYS))
        aums = [i["aum_usd"] for i in instruments if i.get("aum_usd") is not None]
        plans.append(TickerPlan(
            ticker=ticker,
            start_date=start,
            instruments=tuple((i["instrument_id"], i.get("currency") or "USD")
                              for i in instruments),
            max_aum=max(aums) if aums else None,
        ))
    plans.sort(key=lambda p: (p.max_aum is None, -(p.max_aum or 0.0), p.ticker))
    return plans[:cap]


def build_rows(series: list[tuple[_dt.date, float | None]] | tuple[NavObservation, ...],
               instruments: list[tuple[Any, str]] | tuple[tuple[Any, str], ...],
               source: str = "tiingo", *,
               calendar: dict[_dt.date, tuple[str, str, str]] | None = None) -> list[dict[str, Any]]:
    """One ticker series → rows for every instrument sharing it (log returns).

    Runs ``sanitize_nav_series`` over the price series BEFORE computing
    ``return_1d`` so a transient near-zero glitch (Bug 2) never reaches
    nav_timeseries as an impossible log return. Dead / scale-step series are not
    repaired (the eligibility flag handles them); their values pass through.
    """
    observations = [o if isinstance(o, NavObservation) else NavObservation(o[0], o[1], "unknown")
                    for o in series]
    observations.sort(key=lambda o: o.date)
    if len({o.date for o in observations}) != len(observations):
        raise ValueError("duplicate NAV date in provider response")
    ordered = [o for o in observations if o.price is not None
               and math.isfinite(o.price) and o.price > 0]
    clean = sanitize_nav_series([(o.date, o.price) for o in ordered])
    rows: list[dict[str, Any]] = []
    prev: float | None = None
    prev_date: _dt.date | None = None
    prev_kind: str | None = None
    prev_repaired = False
    for idx, (obs, price) in enumerate(zip(ordered, clean.nav)):
        if price is None or price <= 0:
            continue
        compatible = prev is not None and prev_kind == obs.kind
        ret = round(math.log(price / prev), 8) if compatible and prev else None
        repair = (clean.repair_kinds[idx] if clean.repaired[idx] else
                  "not_repaired_dead_series" if clean.dead else
                  "not_repaired_scale_step" if clean.scale_step else "none")
        for instrument_id, currency in instruments:
            rows.append({
                "instrument_id": instrument_id,
                "nav_date": obs.date,
                "nav": round(price, 6),
                "return_1d": ret,
                "return_type": "log",
                "currency": currency,
                "source": source,
                "source_nav": round(obs.price, 6),
                "source_nav_kind": obs.kind,
                "nav_repair_kind": repair,
                "return_start_date": prev_date if compatible else None,
                "return_source_boundary": (prev_kind != obs.kind if prev_kind is not None else None),
                "return_uses_repaired_nav": (prev_repaired or clean.repaired[idx]) if compatible else None,
                "return_semantics": "observed_interval_log_ratio" if compatible else None,
                "return_verification_status": "unverified" if compatible else None,
                "calendar_id": calendar[obs.date][0] if calendar and obs.date in calendar else None,
                "calendar_version": calendar[obs.date][1] if calendar and obs.date in calendar else None,
                "calendar_source": calendar[obs.date][2] if calendar and obs.date in calendar else None,
            })
        prev = price
        prev_date = obs.date
        prev_kind = obs.kind
        prev_repaired = clean.repaired[idx]
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# DB I/O
# ──────────────────────────────────────────────────────────────────────────────
def _fetch_universe(conn) -> list[dict[str, Any]]:
    """Active instruments with a ticker, plus their attributes-resident AUM."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT instrument_id, ticker, currency,
                      NULLIF(attributes->>'aum_usd', '')::numeric AS aum_usd
               FROM instruments_universe
               WHERE is_active AND ticker IS NOT NULL AND ticker != ''""")
        return [{"instrument_id": r[0], "ticker": r[1], "currency": r[2],
                 "aum_usd": float(r[3]) if r[3] is not None else None}
                for r in cur.fetchall()]


def _fetch_watermarks(conn) -> dict[str, _dt.date]:
    """Newest nav_date per ticker (min across instruments sharing the ticker,
    so a brand-new share class forces a refetch deep enough to cover it)."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT upper(iu.ticker),
                      CASE WHEN count(*)=count(mx) THEN min(mx) END
               FROM instruments_universe iu
                LEFT JOIN (
                    SELECT instrument_id, max(nav_date) AS mx
                    FROM nav_timeseries GROUP BY instrument_id
                ) n USING (instrument_id)
                WHERE iu.ticker IS NOT NULL AND iu.ticker != ''
                GROUP BY upper(iu.ticker)""")
        return {r[0]: r[1] for r in cur.fetchall() if r[1] is not None}


def _published_calendar(conn, dates: list[_dt.date]) -> dict[_dt.date, tuple[str, str, str]]:
    """Only the current, published, unexpired policy may identify a due session.

    An unmapped date is *no new assertion*: ``upsert_nav_timeseries`` then keeps
    whatever tuple is already persisted instead of overwriting it with NULL.
    """
    if not dates:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """SELECT policy.calendar_id, policy.calendar_version, policy.calendar_source
               FROM nav_policy_current current_policy
               JOIN nav_policy_versions policy USING (policy_id, policy_version)
               WHERE current_policy.readiness_profile = 'current_daily_nav_v1'
                 AND policy.published_at IS NOT NULL
                 AND policy.valid_through >= clock_timestamp()"""
        )
        policies = cur.fetchall()
        if len(policies) != 1:
            return {}  # unpublished: no calendar assertion
        calendar_id, version, source = policies[0]
        cur.execute(
            """SELECT session_date, calendar_source FROM nav_valuation_schedules
               WHERE calendar_id = %s AND calendar_version = %s
                 AND session_date = ANY(%s)""",
            (calendar_id, version, dates),
        )
        return {date: (calendar_id, version, source) for date, observed_source in cur.fetchall()
                if observed_source == source}


def _instrument_last_nav(conn, instruments: tuple[tuple[Any, str], ...]) -> dict[Any, _dt.date]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT instrument_id, max(nav_date) FROM nav_timeseries
               WHERE instrument_id = ANY(%s) GROUP BY instrument_id""",
            ([iid for iid, _ in instruments],),
        )
        return dict(cur.fetchall())


SUCCESS_ATTEMPTS = ("success_new", "success_no_new")


def _insert_attempt_tx(conn, run_id: uuid.UUID, plan: TickerPlan, provider: str,
                       result: NavFetchResult, requested_end: _dt.date,
                       last_dates: dict[Any, _dt.date] | None = None, *,
                       reason_code: str | None = None) -> dict[Any, str]:
    """Insert the terminal attempt rows of one provider fetch; never commits.

    Attempts are append-only (one per run/instrument/provider): a retry after a
    persisted attempt needs a new run. The server assigns ``commit_xid`` and
    ``persisted_at``. Only a bounded status/code is stored, never payload, URL
    or exception text. A success without valid rows is recorded as ``empty``.
    ``reason_code`` (bounded ``[A-Z_]``) overrides the default code of a
    non-success status. Returns ``{instrument_id: status}``.
    """
    valid = [o for o in result.observations if o.price is not None
             and math.isfinite(o.price) and o.price > 0]
    latest = max((o.date for o in valid), default=None)
    count = len(valid)
    statuses: dict[Any, str] = {}
    with conn.cursor() as cur:
        for iid, _currency in plan.instruments:
            actual = result.status
            if actual == "success_new" and not valid:
                actual = "invalid_payload"
            if actual in SUCCESS_ATTEMPTS and not valid:
                actual = "empty"
            if actual == "success_new" and latest is not None and last_dates is not None:
                if last_dates.get(iid) is not None and latest <= last_dates[iid]:
                    actual = "success_no_new"
            cur.execute(
                """INSERT INTO nav_ingestion_attempts
                   (run_id, instrument_id, ticker, provider, requested_start,
                    requested_end, attempted_at, finished_at, status,
                    newest_observed_date, row_count, reason_code)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (run_id, iid, plan.ticker, provider, plan.start_date,
                 requested_end,
                 result.attempted_at, result.finished_at, actual, latest, count,
                 None if actual in SUCCESS_ATTEMPTS else (reason_code or actual.upper())),
            )
            statuses[iid] = actual
    return statuses


def _record_attempt(conn, run_id: uuid.UUID, plan: TickerPlan, provider: str,
                    result: NavFetchResult, requested_end: _dt.date,
                    last_dates: dict[Any, _dt.date] | None = None, *,
                    reason_code: str | None = None) -> None:
    """Attempt without NAV writes (failure/budget/not-due): its own transaction."""
    try:
        _insert_attempt_tx(conn, run_id, plan, provider, result, requested_end,
                           last_dates, reason_code=reason_code)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _numeric6(value: Any) -> Decimal:
    """The persisted NUMERIC(18,6) value of a writer float (row evidence)."""
    return Decimal(str(value)).quantize(Decimal("0.000001"))


def _insert_row_evidence_tx(conn, *, run_id: uuid.UUID, instrument_id: Any,
                            provider: str, rows: list[dict[str, Any]]) -> int:
    """Row evidence for every fetched observation of one instrument; never commits.

    Must run AFTER the instrument's NAV writes in the same transaction: each
    row carries the final head, and the deferred DB trigger checks at COMMIT
    that the persisted level projection equals the writer's expected digest
    (``nav-level-evidence-v1``), the head is final and the attempt shares the
    xid. Only observations the writer received are proven; neighbours that
    were merely read (predecessor/successor) never get evidence.
    """
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE((SELECT revision_id FROM fund_nav_data_heads "
            "WHERE instrument_id = %s), 0)",
            (instrument_id,),
        )
        head = cur.fetchone()[0]
        cur.executemany(
            """INSERT INTO nav_ingestion_row_evidence
               (run_id, instrument_id, provider, nav_date, level_digest, observed_nav,
                source_nav_kind, revision_head, commit_xid, recorded_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,pg_current_xact_id(),clock_timestamp())""",
            [
                (run_id, instrument_id, provider, row["nav_date"],
                 level_evidence_digest(row["nav_date"], row["nav"], row["source_nav"],
                                       row["source"], row["source_nav_kind"],
                                       row["currency"], row["nav_repair_kind"]),
                 _numeric6(row["source_nav"]), row["source_nav_kind"], head)
                for row in rows
            ],
        )
    return len(rows)


@dataclass(frozen=True)
class NavWriteResult:
    """Outcome of one ``_write_instrument_nav_tx`` call (inside the caller's txn)."""

    changed_level_rows: int
    changed_return_rows: int
    revision_count: int
    revision_head: dict
    detected_event_ids: tuple


def _write_instrument_nav_tx(conn, rows: list[dict[str, Any]], *,
                             run_id: uuid.UUID | None = None,
                             provider: str | None = None,
                             mode: str = "normal") -> NavWriteResult:
    """Write levels, persisted-neighbor returns and revisions; never commits.

    R2-A seam. Chunks are statement batches inside the caller's transaction:
    a provider-attributed write (``run_id`` + ``provider``) must commit
    together with the same-transaction successful attempt, which the deferred
    DB trigger verifies. Only rows whose level/provenance actually changed are
    written, so an identical re-fetch creates no revision. Reexpression of
    adjusted history opens an append-only DETECTED event (attributed writes
    only; an unattributed reexpression is refused).

    No row locks are taken on nav_timeseries: TimescaleDB rejects locking
    compressed tuples (0A000) and production history is compressed. Every
    governed writer (normal run, rebase, calendar maintenance) holds the
    INGESTION advisory lock, and the revision trigger locks the instrument's
    ``fund_nav_data_heads`` row, which serializes NAV writes.

    ``mode='rebase'`` (R2-B, attributed only) additionally recomputes the
    return of EVERY presented date from its persisted predecessor, so a full
    window reconciled from one snapshot ends with coherent returns; the
    ``IS DISTINCT FROM`` guard still makes an already coherent return a no-op
    (no DML, no revision). The default ``normal`` mode is unchanged.
    """
    if (run_id is None) != (provider is None):
        raise ValueError("NAV attribution requires both run_id and provider")
    if mode not in ("normal", "rebase"):
        raise ValueError("unknown NAV write mode")
    rebase = mode == "rebase"
    if rebase and run_id is None:
        raise ValueError("NAV rebase writes require provider attribution")
    levels = """
        INSERT INTO nav_timeseries
            (instrument_id, nav_date, nav, return_1d, return_type, currency, source,
             source_nav, source_nav_kind, nav_repair_kind,
             calendar_id, calendar_version, calendar_source)
        VALUES (%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (instrument_id, nav_date) DO UPDATE SET
            nav=EXCLUDED.nav, return_type=EXCLUDED.return_type,
            currency=EXCLUDED.currency, source=EXCLUDED.source,
            source_nav=EXCLUDED.source_nav, source_nav_kind=EXCLUDED.source_nav_kind,
            nav_repair_kind=EXCLUDED.nav_repair_kind,
            calendar_id=EXCLUDED.calendar_id,
            calendar_version=EXCLUDED.calendar_version,
            calendar_source=EXCLUDED.calendar_source
    """
    return_update = """
        UPDATE nav_timeseries SET return_1d=%s, return_start_date=%s,
            return_source_boundary=%s, return_uses_repaired_nav=%s,
            return_semantics=%s, return_verification_status=%s
        WHERE instrument_id=%s AND nav_date=%s
          AND (return_1d, return_start_date, return_source_boundary,
               return_uses_repaired_nav, return_semantics,
               return_verification_status)
              IS DISTINCT FROM (%s,%s,%s,%s,%s,%s)
    """
    source_fields = ("nav", "return_type", "currency", "source", "source_nav",
                     "source_nav_kind", "nav_repair_kind", "calendar_id",
                     "calendar_version", "calendar_source")
    grouped: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        effective_calendar_tuple(row, None)  # partial incoming tuples are invalid
        grouped.setdefault(row["instrument_id"], []).append(row)
    changed_levels = changed_returns = 0
    events: list[int] = []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT set_config('nav.ingestion_run_id', %s, true)",
                    (str(run_id) if run_id else "",))
        cur.execute("SELECT set_config('nav.ingestion_provider', %s, true)",
                    (provider or "",))
        # A pre-W1 nav_timeseries (no ledger) keeps the plain level/return writer.
        cur.execute("SELECT to_regclass('fund_nav_data_revisions') IS NOT NULL AS w1")
        has_ledger = cur.fetchone()["w1"]
        revision_floor = 0
        if has_ledger:
            cur.execute(
                "SELECT COALESCE(max(revision_id), 0) AS n FROM fund_nav_data_revisions"
            )
            revision_floor = cur.fetchone()["n"]
        for instrument_id, series in grouped.items():
            series.sort(key=lambda r: r["nav_date"])
            if len({r["nav_date"] for r in series}) != len(series):
                raise ValueError("duplicate NAV date for instrument")
            for i in range(0, len(series), UPSERT_CHUNK):
                chunk = series[i:i + UPSERT_CHUNK]
                cur.execute(
                    """SELECT nav_date, nav, return_type, currency, source,
                              source_nav, source_nav_kind, nav_repair_kind,
                              calendar_id, calendar_version, calendar_source
                       FROM nav_timeseries WHERE instrument_id=%s
                         AND nav_date BETWEEN %s AND %s
                       ORDER BY nav_date""",
                    (instrument_id, chunk[0]["nav_date"], chunk[-1]["nav_date"]),
                )
                old = {r["nav_date"]: r for r in cur.fetchall()}
                # The three calendar columns move together: a missing mapping keeps
                # the persisted tuple (no NULL overwrite, no spurious revision).
                chunk = [
                    {**row, **dict(zip(CALENDAR_FIELDS,
                                       effective_calendar_tuple(row, old.get(row["nav_date"]))
                                       or (None, None, None)))}
                    for row in chunk
                ]
                changed = {row["nav_date"] for row in chunk if row["nav_date"] not in old
                           or any((float(old[row["nav_date"]][field]) != float(row[field])
                                   if field in ("nav", "source_nav")
                                   and old[row["nav_date"]][field] is not None
                                   and row[field] is not None
                                   else old[row["nav_date"]][field] != row[field])
                                  for field in source_fields)}
                reexpressed = []
                for row in chunk:
                    prior = old.get(row["nav_date"])
                    if prior is None or row["source_nav_kind"] != "adjusted":
                        continue
                    prior_level = prior["source_nav"] if prior["source_nav"] is not None else prior["nav"]
                    if (prior_level is not None and row["source_nav"] is not None
                            and not math.isclose(float(prior_level), float(row["source_nav"]),
                                                 rel_tol=ADJUSTED_OVERLAP_REL_TOL,
                                                 abs_tol=ADJUSTED_OVERLAP_ABS_TOL)):
                        reexpressed.append(row["nav_date"])
                if reexpressed:
                    if run_id is None:
                        raise ValueError("NAV reexpression requires provider attribution")
                    cur.execute(
                        """INSERT INTO fund_nav_reexpression_events
                           (instrument_id, event_kind, first_changed_date,
                            last_changed_date, source_run_id, source_provider,
                            revision_head, recorded_at, reason_code)
                           VALUES (%s,'DETECTED',%s,%s,%s,%s,0,clock_timestamp(),
                                   'ADJUSTED_HISTORY_REEXPRESSION')
                           RETURNING event_id""",
                        (instrument_id, min(reexpressed), max(reexpressed), run_id,
                         provider),
                    )
                    events.append(cur.fetchone()["event_id"])
                writes = [r for r in chunk if r["nav_date"] in changed]
                cur.executemany(levels, [
                    (instrument_id, r["nav_date"], r["nav"], r["return_type"],
                     r["currency"], r["source"], r["source_nav"],
                     r["source_nav_kind"], r["nav_repair_kind"],
                     r["calendar_id"], r["calendar_version"], r["calendar_source"])
                    for r in writes
                ])
                changed_levels += len(writes)
                if changed or rebase:
                    cur.execute(
                        """SELECT nav_date, nav, source, source_nav_kind, nav_repair_kind
                           FROM nav_timeseries WHERE instrument_id=%s AND nav_date < %s
                           ORDER BY nav_date DESC LIMIT 1""",
                        (instrument_id, chunk[0]["nav_date"]),
                    )
                    predecessor = cur.fetchone()
                    cur.execute(
                        """SELECT nav_date FROM nav_timeseries
                           WHERE instrument_id=%s AND nav_date > %s
                           ORDER BY nav_date LIMIT 1""",
                        (instrument_id, chunk[-1]["nav_date"]),
                    )
                    successor = cur.fetchone()
                    end = successor["nav_date"] if successor else chunk[-1]["nav_date"]
                    cur.execute(
                        """SELECT nav_date, nav, source, source_nav_kind, nav_repair_kind
                           FROM nav_timeseries WHERE instrument_id=%s
                             AND nav_date BETWEEN %s AND %s
                           ORDER BY nav_date""",
                        (instrument_id, chunk[0]["nav_date"], end),
                    )
                    neighborhood = ([predecessor] if predecessor else []) + cur.fetchall()
                    affected = set(changed)
                    if rebase:
                        affected.update(row["nav_date"] for row in chunk)
                    for position, persisted in enumerate(neighborhood[:-1]):
                        if persisted["nav_date"] in changed:
                            affected.add(neighborhood[position + 1]["nav_date"])
                    for position, persisted in enumerate(neighborhood):
                        if persisted["nav_date"] not in affected:
                            continue
                        prev = neighborhood[position - 1] if position else None
                        boundary = (prev["source"] != persisted["source"] or
                                    prev["source_nav_kind"] != persisted["source_nav_kind"]
                                    if prev is not None else None)
                        compatible = (prev is not None and not boundary
                                      and persisted["source_nav_kind"] in ("adjusted", "raw", "unknown")
                                      and prev["nav_repair_kind"] is not None
                                      and persisted["nav_repair_kind"] is not None
                                      and prev["nav"] is not None and persisted["nav"] is not None
                                      and min(float(prev["nav"]), float(persisted["nav"])) > 0)
                        ret = (round(math.log(float(persisted["nav"]) / float(prev["nav"])), 8)
                               if compatible else None)
                        values = (
                            ret, prev["nav_date"] if compatible else None,
                            boundary, (prev["nav_repair_kind"] in REPAIRED_NAV_KINDS
                                       or persisted["nav_repair_kind"] in REPAIRED_NAV_KINDS)
                            if compatible else None,
                            "observed_interval_log_ratio" if compatible else None,
                            "unverified" if compatible else None,
                        )
                        cur.execute(return_update,
                                    (*values, instrument_id, persisted["nav_date"], *values))
                        changed_returns += cur.rowcount
        revision_count, heads = 0, {}
        if has_ledger:
            cur.execute(
                """SELECT count(*) AS n FROM fund_nav_data_revisions
                   WHERE revision_id > %s AND instrument_id = ANY(%s)""",
                (revision_floor, list(grouped)),
            )
            revision_count = cur.fetchone()["n"]
            cur.execute(
                "SELECT instrument_id, revision_id FROM fund_nav_data_heads "
                "WHERE instrument_id = ANY(%s)",
                (list(grouped),),
            )
            heads = {row["instrument_id"]: row["revision_id"] for row in cur.fetchall()}
    return NavWriteResult(changed_levels, changed_returns, revision_count, heads,
                          tuple(events))


def upsert_nav_timeseries(conn, rows: list[dict[str, Any]], *,
                          run_id: uuid.UUID | None = None,
                          provider: str | None = None) -> int:
    """Legacy wrapper: one ``_write_instrument_nav_tx`` then COMMIT.

    Provider-attributed callers must have inserted the successful attempt in
    the same open transaction; otherwise the deferred attribution trigger
    rejects the COMMIT and everything rolls back. Returns the number of rows
    presented (unchanged rows are not rewritten).
    """
    try:
        _write_instrument_nav_tx(conn, rows, run_id=run_id, provider=provider)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return len(rows)


# ──────────────────────────────────────────────────────────────────────────────
# Run lifecycle: orphan recovery, per-instrument persistence, truthful finalization
# ──────────────────────────────────────────────────────────────────────────────
def _recover_orphan_runs(conn, operation: str) -> int:
    """Mark earlier ``running`` runs of ``operation`` aborted (``INTERRUPTED``).

    Only valid under that operation's exclusive lock (normal: the INGESTION
    session lock; rebase: the rebase operator mutex), so no live run of the
    same operation can be hit. Committed per-instrument evidence of the
    interrupted run stays valid: parent status is never an economic gate.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE nav_ingestion_runs SET status = 'aborted', reason_code = 'INTERRUPTED'
                   WHERE status = 'running' AND operation = %s""",
                (operation,),
            )
            recovered = cur.rowcount
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return recovered


def _finalize_run(conn, run_id: uuid.UUID, status: str, reason_code: str | None) -> None:
    """Terminal transition in its own transaction (never leaves ``running``)."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        conn.rollback()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE nav_ingestion_runs SET status = %s, reason_code = %s
                   WHERE run_id = %s AND status = 'running'""",
                (status, reason_code, run_id),
            )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _within_window(result: NavFetchResult, start: _dt.date,
                   end: _dt.date) -> tuple[NavFetchResult, int]:
    """Observations outside the requested window are never attributed.

    The DB refuses a revision/row evidence outside the attempt window, so an
    over-delivering provider cannot write beyond what was requested; the count
    of dropped observations is reported, not silently absorbed.
    """
    kept = tuple(o for o in result.observations if start <= o.date <= end)
    dropped = len(result.observations) - len(kept)
    if not dropped:
        return result, 0
    return dataclasses.replace(result, observations=kept), dropped


def _persist_instrument(conn, run_id: uuid.UUID, plan: TickerPlan,
                        instrument: tuple[Any, str], as_of: _dt.date,
                        attempts: list[tuple[str, NavFetchResult]],
                        source: str | None, rows: list[dict[str, Any]],
                        last_dates: dict[Any, _dt.date]) -> NavWriteResult | None:
    """One instrument, one transaction: attempts + levels/returns/revisions +
    row evidence commit together or not at all (N1-b). An instrument without
    usable data commits only its terminal attempts (no NAV DML)."""
    single = dataclasses.replace(plan, instruments=(instrument,))
    written: NavWriteResult | None = None
    try:
        for provider, attempt in attempts:
            _insert_attempt_tx(conn, run_id, single, provider, attempt, as_of,
                               last_dates=last_dates)
        if source is not None and rows:
            written = _write_instrument_nav_tx(conn, rows, run_id=run_id, provider=source)
            _insert_row_evidence_tx(conn, run_id=run_id, instrument_id=instrument[0],
                                    provider=source, rows=rows)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return written


def _record_failure_best_effort(conn, run_id: uuid.UUID, plan: TickerPlan,
                                provider: str, status: str, reason_code: str,
                                as_of: _dt.date, *, provider_outcome: bool = True) -> None:
    """Failure attempt without NAV in its own transaction, never masking the
    original error: a secondary failure is only noted on it by the caller.

    A local failure (``provider_outcome=False``, e.g. persistence) carries no
    provider instant, so it never outranks a real provider attempt.
    """
    now = _dt.datetime.now(_dt.timezone.utc) if provider_outcome else None
    _record_attempt(conn, run_id, plan, provider,
                    NavFetchResult(status, attempted_at=now, finished_at=now), as_of,
                    reason_code=reason_code)


def _rebase_required_count(conn) -> int:
    """Instruments with an active hold relevant to the current due grid.

    Detections wholly before the grid are ledger-only (never block readiness,
    never resolved by a grid rebase) and are not counted. Without a usable
    published policy every active hold counts. Read-only; no fetch is started.
    """
    from src.workers._nav_policy import PolicyUnavailable, resolve_policy_and_grid

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT clock_timestamp()")
            now = cur.fetchone()[0]
        try:
            floor = resolve_policy_and_grid(conn, now)[1][0]
        except PolicyUnavailable:
            floor = _dt.date.min
        with conn.cursor() as cur:
            cur.execute(
                """SELECT count(DISTINCT instrument_id) FROM fund_nav_reexpression_holds
                   WHERE last_changed_date >= %s""",
                (floor,),
            )
            count = cur.fetchone()[0]
    finally:
        conn.rollback()
    return int(count)


# ──────────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None,
        target_session: _dt.date | None = None) -> dict:
    """Refresh nav_timeseries for the stalest/biggest tickers from Tiingo.

    Persistence is serialized per instrument (attempt + NAV + row evidence in
    one transaction); a failing instrument rolls back whole and the run is
    finalized ``failed`` before the error propagates; a Tiingo budget abort
    cancels pending fetches and finalizes ``aborted``. Earlier orphaned normal
    runs are recovered first under the exclusive INGESTION lock. Reexpression
    only opens a hold (``rebase_required_count``); no automatic full fetch.
    """
    as_of = _dt.date.fromisoformat(calc_date) if calc_date else _dt.date.today()
    if target_session is not None and target_session != as_of:
        raise ValueError("target_session must match calc_date")
    cap = limit if limit is not None else DEFAULT_TICKER_CAP
    fetched = upserted = 0
    committed = outside_window = reexpression_detected = 0
    empty_tickers: list[str] = []
    aborted = None
    run_id = uuid.uuid4()
    processed: set[str] = set()
    plans: list[TickerPlan] = []
    done = 0
    fallback_loaded: dict[str, int] = {}

    with connect(dsn) as conn:
        with (advisory_lock(conn, LOCK_INSTRUMENT_INGESTION) as got,
              advisory_lock(conn, LOCK_FUND_NAV_READINESS) as readiness_got):
            if not got or not readiness_got:
                return {"fetched": 0, "upserted": 0, "skipped": "lock_busy"}
            conn.commit()
            recovered = _recover_orphan_runs(conn, "normal")

            universe = _fetch_universe(conn)
            watermarks = _fetch_watermarks(conn)
            all_plans = select_stale_tickers(
                universe, watermarks, as_of, len(universe),
                target_session=target_session,
            )
            plans = all_plans[:cap]
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO nav_ingestion_runs
                           (run_id, started_at, requested_end, status, operation)
                           VALUES (%s, clock_timestamp(), %s, 'running', 'normal')""",
                        (run_id, as_of),
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

            try:
                planned_tickers = {p.ticker for p in all_plans}
                # Attempts without NAV (over cap / not due): one own transaction.
                try:
                    for p in all_plans[cap:]:
                        _insert_attempt_tx(conn, run_id, p, "tiingo",
                                           NavFetchResult("not_attempted_budget"), as_of)
                    for inst in universe:
                        ticker = (inst.get("ticker") or "").strip().upper()
                        if ticker and ticker not in planned_tickers:
                            skipped = TickerPlan(
                                ticker, as_of,
                                ((inst["instrument_id"], inst.get("currency") or "USD"),),
                                None,
                            )
                            _insert_attempt_tx(conn, run_id, skipped, "tiingo",
                                               NavFetchResult("not_due"), as_of)
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise

                from src.workers._fallback_nav import FallbackNav
                from src.workers._tiingo import TokenBucket
                with TiingoClient(bucket=TokenBucket(max_tokens=20,
                                                     refill_rate=FETCH_RATE_PER_S)) as tiingo, \
                        FallbackNav() as fallback:

                    def fetch_one(p: TickerPlan) -> tuple[NavFetchResult, str | None,
                                                          list[tuple[str, NavFetchResult]]]:
                        primary = tiingo.fetch_daily_observations(p.ticker, p.start_date, as_of)
                        attempts = [("tiingo", primary)]
                        if primary.status == "success_new":
                            return primary, "tiingo", attempts
                        result, provider, tried = fallback.fetch_observations(
                            p.ticker, p.start_date, as_of)
                        return result, provider, attempts + tried

                    # Fetches fan out across threads (httpx.Client is thread-safe);
                    # persistence stays serialized on this one connection.
                    pool = concurrent.futures.ThreadPoolExecutor(FETCH_CONCURRENCY)
                    try:
                        futures = {pool.submit(fetch_one, p): p for p in plans}
                        for fut in concurrent.futures.as_completed(futures):
                            plan = futures[fut]
                            try:
                                result, source, attempts = fut.result()
                            except TiingoBudgetExceeded:
                                aborted = "tiingo_budget"
                                pool.shutdown(wait=False, cancel_futures=True)
                                processed.add(plan.ticker)
                                _record_failure_best_effort(
                                    conn, run_id, plan, "tiingo", "rate_limited",
                                    "TIINGO_BUDGET", as_of)
                                break
                            except Exception:
                                # Nothing about an arbitrary exception is safe to persist.
                                processed.add(plan.ticker)
                                _record_attempt(conn, run_id, plan, "tiingo",
                                                NavFetchResult("transient_error"), as_of)
                                raise
                            processed.add(plan.ticker)
                            result, dropped = _within_window(result, plan.start_date, as_of)
                            attempts = [(provider, _within_window(attempt, plan.start_date,
                                                                  as_of)[0])
                                        for provider, attempt in attempts]
                            outside_window += dropped
                            last_dates = _instrument_last_nav(conn, plan.instruments)
                            by_instrument: dict[Any, list[dict[str, Any]]] = {}
                            usable = source is not None and bool(result.observations)
                            if not usable:
                                empty_tickers.append(plan.ticker)  # gap em todos os provedores
                            else:
                                fetched += len(result.observations)
                                if source != "tiingo":
                                    fallback_loaded[source] = fallback_loaded.get(source, 0) + 1
                                calendar = _published_calendar(
                                    conn, [o.date for o in result.observations])
                                for row in build_rows(result.observations, plan.instruments,
                                                      source, calendar=calendar):
                                    by_instrument.setdefault(row["instrument_id"], []).append(row)
                            conn.rollback()  # reads done; each instrument is its own txn
                            for instrument in plan.instruments:
                                rows = by_instrument.get(instrument[0], [])
                                try:
                                    written = _persist_instrument(
                                        conn, run_id, plan, instrument, as_of, attempts,
                                        source if usable else None, rows, last_dates)
                                except Exception as exc:
                                    single = dataclasses.replace(plan, instruments=(instrument,))
                                    try:
                                        _record_failure_best_effort(
                                            conn, run_id, single, source or "tiingo",
                                            "transient_error", "PERSISTENCE_FAILED", as_of,
                                            provider_outcome=False)
                                    except Exception as secondary:
                                        exc.add_note("failure attempt not recorded: "
                                                     + type(secondary).__name__)
                                    raise
                                committed += 1
                                if written is not None:
                                    upserted += len(rows)
                                    reexpression_detected += bool(written.detected_event_ids)
                            if usable:
                                done += 1
                    except BaseException:
                        pool.shutdown(wait=False, cancel_futures=True)
                        raise
                    finally:
                        pool.shutdown(wait=True)
                if aborted:
                    # Budget stop: plans never persisted are explicitly not attempted.
                    try:
                        for p in plans:
                            if p.ticker not in processed:
                                _insert_attempt_tx(conn, run_id, p, "tiingo",
                                                   NavFetchResult("not_attempted_budget"),
                                                   as_of, reason_code="TIINGO_BUDGET_ABORT")
                        conn.commit()
                    except BaseException:
                        conn.rollback()
                        raise
            except BaseException as exc:
                reason = ("INTERRUPTED" if isinstance(exc, KeyboardInterrupt)
                          else "UNEXPECTED_ERROR")
                try:
                    _finalize_run(conn, run_id, "failed", reason)
                except Exception as secondary:
                    exc.add_note("NAV ingestion run finalization failed: "
                                 + type(secondary).__name__)
                raise
            _finalize_run(conn, run_id, "aborted" if aborted else "completed",
                          "TIINGO_BUDGET" if aborted else None)
            rebase_required = _rebase_required_count(conn)

    stats: dict[str, Any] = {
        "fetched": fetched, "upserted": upserted,
        "tickers_planned": len(plans), "tickers_loaded": done,
        "tickers_empty": len(empty_tickers), "as_of": as_of.isoformat(),
        "ingestion_run_id": str(run_id),
        "instruments_committed": committed,
        "not_attempted": sum(p.ticker not in processed for p in plans),
        "orphan_runs_recovered": recovered,
        "observations_outside_window": outside_window,
        "reexpression_detected": reexpression_detected,
        "rebase_required_count": rebase_required,
        "run_status": "aborted" if aborted else "completed",
    }
    if fallback_loaded:
        stats["fallback_loaded"] = fallback_loaded
    if aborted:
        stats["aborted"] = aborted
    return stats
