"""A non-cohort must never be published as a peer cohort.

``peer_strategy_label`` is what the Light serves as "these are your peers", so a
label that means *we could not classify this fund* — ``'Unclassified'`` or blank
— cannot be allowed to become a peer group: it would present the funds nobody
could label as each other's comparables. ``_PEER_PERCENTILES_SQL`` guards that in
its ``labels`` CTE, and these tests hold the guard to its behaviour rather than
to its text: they run the REAL constant against a real PostgreSQL over a seeded
fixture and read the rows back.

Why the guard sits AFTER the ``DISTINCT ON`` and not inside it: filtering inside
would drop the ``'Unclassified'`` row from the candidate set and let the fund's
*previous* proposal win instead, republishing a superseded label as governed.
``test_unclassified_winner_does_not_resurrect_previous_label`` pins exactly that.

Requires the CI PostgreSQL service (``workers-new-surfaces-postgres``); every
test builds its own throwaway schema and rolls the whole transaction back.
"""
from __future__ import annotations

import datetime as _dt
from uuid import uuid4

import psycopg
import pytest

from src.workers import risk_metrics as rm

DSN = "host=127.0.0.1 port=65431 dbname=postgres user=postgres"

CALC_DATE = _dt.date(2026, 8, 4)
COHORT = "Large Blend"
# The cohort must clear MIN_PEER_COHORT_SIZE, otherwise every percentile
# degrades to 50.0 and the assertions could not tell a real ranking from the
# small-cohort guard.
COHORT_N = 12


def _seed(cur) -> None:
    """The two relations the peer SQL reads, and nothing else it does not touch."""
    schema = f"peer_guard_{uuid4().hex}"
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute(
        """
        CREATE TABLE strategy_reclassification_stage (
            stage_id bigserial PRIMARY KEY,
            source_table text NOT NULL,
            source_pk text NOT NULL,
            proposed_strategy_label text,
            classification_source text,
            classified_at timestamptz NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE fund_risk_metrics (
            instrument_id uuid NOT NULL,
            calc_date date NOT NULL,
            organization_id uuid,
            sharpe_1y numeric(10,6),
            sortino_1y numeric(10,6),
            return_1y numeric(10,6),
            max_drawdown_1y numeric(10,6),
            peer_strategy_label varchar,
            peer_sharpe_pctl numeric(6,2),
            peer_sortino_pctl numeric(6,2),
            peer_return_pctl numeric(6,2),
            peer_drawdown_pctl numeric(6,2),
            peer_count integer,
            peer_overall_quartile smallint,
            peer_band_low numeric(10,6),
            peer_band_mid numeric(10,6),
            peer_band_high numeric(10,6),
            PRIMARY KEY (instrument_id, calc_date)
        )
        """
    )


def _add_fund(cur, sharpe: float) -> str:
    iid = str(uuid4())
    cur.execute(
        """
        INSERT INTO fund_risk_metrics
            (instrument_id, calc_date, organization_id,
             sharpe_1y, sortino_1y, return_1y, max_drawdown_1y)
        VALUES (%s, %s, NULL, %s, %s, %s, %s)
        """,
        (iid, CALC_DATE, sharpe, sharpe * 1.2, sharpe * 0.1, -0.10 - sharpe / 100),
    )
    return iid


def _propose(cur, iid: str, label: str | None, *, at: str, source: str = "auto") -> None:
    cur.execute(
        """
        INSERT INTO strategy_reclassification_stage
            (source_table, source_pk, proposed_strategy_label,
             classification_source, classified_at)
        VALUES ('instruments_universe', %s, %s, %s, %s)
        """,
        (iid, label, source, at),
    )


def _peer_row(cur, iid: str) -> dict:
    cur.execute(
        """
        SELECT peer_strategy_label, peer_sharpe_pctl, peer_sortino_pctl,
               peer_return_pctl, peer_drawdown_pctl, peer_count,
               peer_overall_quartile, peer_band_mid
        FROM fund_risk_metrics
        WHERE instrument_id = %s AND calc_date = %s
        """,
        (iid, CALC_DATE),
    )
    return dict(zip([d.name for d in cur.description], cur.fetchone()))


def _cohort(cur) -> list[str]:
    """Funds seeded into the healthy cohort, best sharpe first."""
    return [_add_fund(cur, 0.10 * (i + 1)) for i in range(COHORT_N)]


@pytest.fixture
def conn():
    with psycopg.connect(DSN) as c:
        with c.cursor() as cur:
            _seed(cur)
        yield c
        c.rollback()


def test_governed_label_is_published_with_percentiles(conn):
    """Control: a normal label still produces a cohort, count and ranking."""
    with conn.cursor() as cur:
        funds = _cohort(cur)
        for iid in funds:
            _propose(cur, iid, COHORT, at="2026-07-01")
        rm._update_peer_percentiles(conn, CALC_DATE)

        for iid in funds:
            row = _peer_row(cur, iid)
            assert row["peer_strategy_label"] == COHORT
            assert row["peer_count"] == COHORT_N
            assert row["peer_band_mid"] is not None
        # Highest sharpe ranks top of its cohort — a real ranking, not the
        # MIN_PEER_COHORT_SIZE fallback of 50.0.
        assert float(_peer_row(cur, funds[-1])["peer_sharpe_pctl"]) == 100.0
        assert _peer_row(cur, funds[-1])["peer_overall_quartile"] == 1


@pytest.mark.parametrize("label", ["Unclassified", "", "   "])
def test_non_cohort_label_publishes_no_peer_group(conn, label):
    """'Unclassified' and blank leave every peer_* column NULL."""
    with conn.cursor() as cur:
        for iid in _cohort(cur):
            _propose(cur, iid, COHORT, at="2026-07-01")
        orphan = _add_fund(cur, 0.55)
        _propose(cur, orphan, label, at="2026-07-01")

        rm._update_peer_percentiles(conn, CALC_DATE)

        row = _peer_row(cur, orphan)
        assert row["peer_strategy_label"] is None, (
            f"{label!r} was published as a peer cohort"
        )
        assert all(value is None for value in row.values()), (
            f"{label!r} left peer analytics behind: {row}"
        )


def test_excluded_fund_is_not_counted_in_a_real_cohort(conn):
    """The guard runs before ``counts``, so peer_count excludes the orphans."""
    with conn.cursor() as cur:
        funds = _cohort(cur)
        for iid in funds:
            _propose(cur, iid, COHORT, at="2026-07-01")
        for label in ("Unclassified", ""):
            _propose(cur, _add_fund(cur, 0.99), label, at="2026-07-01")

        rm._update_peer_percentiles(conn, CALC_DATE)

        assert _peer_row(cur, funds[0])["peer_count"] == COHORT_N


def test_unclassified_winner_does_not_resurrect_previous_label(conn):
    """A fund reclassified INTO 'Unclassified' loses its cohort, keeps no ghost.

    This is the reason the guard is applied after the ``DISTINCT ON`` winner is
    chosen. Filtering inside the ``DISTINCT ON`` would make the superseded
    'Large Blend' proposal win and republish it as if it were governed.
    """
    with conn.cursor() as cur:
        for iid in _cohort(cur):
            _propose(cur, iid, COHORT, at="2026-07-01")
        demoted = _add_fund(cur, 0.55)
        _propose(cur, demoted, COHORT, at="2026-06-01")
        _propose(cur, demoted, "Unclassified", at="2026-07-15")

        rm._update_peer_percentiles(conn, CALC_DATE)

        assert _peer_row(cur, demoted)["peer_strategy_label"] is None


def test_manual_override_cannot_force_a_non_cohort(conn):
    """manual_override outranks the automatic label — and is guarded all the same."""
    with conn.cursor() as cur:
        for iid in _cohort(cur):
            _propose(cur, iid, COHORT, at="2026-07-01")
        overridden = _add_fund(cur, 0.55)
        _propose(cur, overridden, COHORT, at="2026-07-20")
        _propose(
            cur, overridden, "Unclassified", at="2026-06-01", source="manual_override"
        )

        rm._update_peer_percentiles(conn, CALC_DATE)

        assert _peer_row(cur, overridden)["peer_strategy_label"] is None
