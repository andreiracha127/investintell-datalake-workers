"""Public-only serving materializer for the sibling product ``bond_serving_v1``.

Reads the current bond snapshots -- ``sec_current_bond_security_v1`` (+aliases,
Task 3), the ``bond_price_latest_v1`` / ``bond_price_fund_asof_v1`` price lanes
(Task 4) and the N-PORT ``sec_nport_holdings_v2_current`` reverse-lookup source --
and projects ONLY public columns into ``bond_serving_facts`` across the four bond
serving surfaces under one atomically promoted derived publication:

  * catalog       -- one row per security: search-ready identity + summary terms
                     + data state + computed summary values (Wave 1):
                     latest_price_pct (% of par, sole ELIGIBLE latest observation)
                     and security_ytm / security_ytw (decimal fractions from the
                     promoted ``sec_current_bond_metric_v1`` view); plus the
                     reported issuer classification issuer_country /
                     issuer_sector, resolved to the security grain by reported
                     consensus (see ``_ISSUER_CLASSIFICATION_SQL``).
  * detail        -- one row per security: full terms incl. call/put schedule,
                     144A, PIT aliases, NEUTRAL identity ambiguity evidence, and
                     the computed metrics current_yield / security_ytm /
                     security_ytw (fractions) + wal (years) from the promoted
                     current metric view.
  * observations  -- price/trade observations WITH a mandatory ``lane`` discriminator
                     (``latest`` informative + ``fund_asof`` point-in-time, no
                     look-ahead) + freshness/ambiguity states.
  * fund_exposure -- N-PORT point-in-time reverse lookup by security, pre-aggregated
                     at fund (series) grain with a multiplication HARD-FAIL guard.

Every payload is passed through ``bond_serving_scrub`` (a recursive key-strip that
also neutralises ``cik:``/``row:`` VALUES) and each projection lists only neutral,
source-free public keys with neutral product dates (``as_of``/``observation_date``)
-- so no source/vendor literal, raw row id, source lineage, filing key or internal
identity key ever reaches the serving surface (plan Global Constraints 3 & 4).

Null-honesty (Wave 1, plan Global Constraint 4): every computed key is ALWAYS
present in its payload; a security without an 'available' metric row (or with no
row at all), or without exactly one eligible latest observation, serves the key as
JSON null -- never a synthetic 0. Values come ONLY from promoted current relations
(``sec_current_bond_metric_v1``, ``bond_price_latest_v1``), never from landed or
unpromoted builds.
"""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

import psycopg
from psycopg.types.json import Json

from src.bonds import serving_contract as contract

ROOT = Path(__file__).resolve().parents[2]

_NAMESPACE = UUID("b0d5e70a-0000-5000-a000-736572766e67")

_SCHEMA_FILES = (
    "sec_derived_publications.sql",
    "bond_serving_v1.sql",
)

_COLUMNS = (
    "publication_id, surface, security_id, lane, fund_key, fact_key, state, "
    "reason_code, identity_state, ambiguity_state, as_of, observation_date, "
    "coverage_pct, payload"
)

# Each surface's source_view (contract) plus every relation the projection reads.
# A surface is 'present' iff ALL its required relations exist; a missing surface
# fails the coverage gate closed (all declared surfaces or nothing).
_SURFACE_REQUIRED_RELATIONS: dict[str, tuple[str, ...]] = {
    # catalog also reads the alias view (searchable aliases_cusip9/isin arrays) and,
    # since Wave 1, the promoted latest price lane (latest_price_pct) + the promoted
    # current metric view (security_ytm/security_ytw). detail reads the metric view
    # for current_yield/security_ytm/security_ytw/wal. A build without them would
    # silently serve payloads missing contract keys -> the gate fails closed.
    "catalog": (
        "sec_current_bond_security_v1", "sec_current_bond_security_alias_v1",
        "bond_price_latest_v1", "sec_current_bond_metric_v1",
        # issuer_country / issuer_sector are resolved from the N-PORT reported
        # classification; without this relation the catalog would serve payloads
        # missing two contract keys -> the coverage gate fails closed.
        "sec_nport_holdings_v2_current",
    ),
    "detail": (
        "sec_current_bond_security_v1", "sec_current_bond_security_alias_v1",
        "sec_current_bond_metric_v1",
    ),
    "observations": ("bond_price_latest_v1",),
    "fund_exposure": (
        "sec_current_bond_security_v1",
        "sec_current_bond_security_alias_v1",
        "sec_nport_holdings_v2_current",
    ),
}


class BondServingSurfaceCoverageError(RuntimeError):
    """Raised when a serving build would promote a PARTIAL surface set.

    The serving publication is one complete, atomically promoted surface set:
    every contract-declared surface whose source relation is missing -- or which
    projects ZERO rows -- would silently drop that surface from the served
    publication. Failing closed here prevents a partial promotion;
    ``materialize(..., allow_missing_surfaces=True)`` opts out for tests that
    deliberately exercise a subset of surfaces.
    """


class BondFundExposureMultiplicationError(RuntimeError):
    """Raised when the N-PORT reverse lookup would multiply the fund grain.

    Fund exposure pre-aggregates holding lots to (security, series) grain. If a
    single source holding lot mapped to more than one security (an identity
    ambiguity), or a single (security, series, lot, report) collapsed to more than
    one distinct value, aggregating would DOUBLE-COUNT ownership. The build fails
    closed rather than emit an inflated exposure (Increment 1 Constraint 4 heritage
    -- 'row multiplication detected').
    """


# ---------------------------------------------------------------------------
# catalog + detail (grain: security) over sec_current_bond_security_v1, joined
# with the promoted current metric view + latest price lane (Wave 1).
# ---------------------------------------------------------------------------

# The CLOSED Wave-1 metric vocabulary this projection may serve (owner
# authorization 2026-07-23; matches the bond_metric_v1 CHECK constraint).
_WAVE1_SERVED_METRICS = ("security_ytm", "security_ytw", "current_yield", "wal")


def _metric_value_sql(metric_id: str) -> str:
    """Scalar subquery serving one Wave-1 metric value from the PROMOTED current view.

    'available' is the ONLY value-bearing status: any other status -- or no row
    at all -- projects an honest JSON null under the contract key, never a
    synthetic 0 (plan Global Constraint 4). The status filter is a serving-side
    guard in its own right (mutation-locked by poisoned fixture rows), not a free
    ride on the upstream bond_metric_v1 CHECK.
    """
    if metric_id not in _WAVE1_SERVED_METRICS:  # closed vocabulary, fail loud
        raise ValueError(f"metric {metric_id!r} is not a served Wave-1 metric")
    return (
        "(SELECT m.value FROM sec_current_bond_metric_v1 m"
        " WHERE m.security_id = s.security_id"
        f" AND m.metric_id = '{metric_id}' AND m.status = 'available')"
    )


# The sole ELIGIBLE latest observation's price (% of par) or an honest NULL.
# Mirrors bond_price_is_eligible (bond_price_eligibility_v1.sql) column-wise;
# identity is resolved by lane construction (only resolved observations publish).
# Parity with the canonical predicate is regex-locked by
# test_latest_price_inline_eligibility_matches_the_canonical_predicate.
# A duplicate cohort has NO unambiguous latest price -> NULL, never an arbitrary
# winner; at most one row can match (unique cohort), so the subquery is scalar.
_LATEST_PRICE_PCT_SQL = """(
    SELECT p.price FROM bond_price_latest_v1 p
    WHERE p.security_id = s.security_id
      AND p.price_type IN ('trade', 'evaluated')
      AND p.accrued_treatment IN ('clean', 'dirty')
      AND p.daily_key_state = 'unique_in_matching_cohort'
      AND p.price_state = 'present')"""

# ---------------------------------------------------------------------------
# Reported issuer classification (issuer_country / issuer_sector), resolved from
# the N-PORT holding grain to the SECURITY grain once per build.
#
# The classification is a REPORTED attribute of the holding, not of the security:
# many funds hold the same bond and each one reports its own issuer country and
# issuer category, so a security can carry several reported values (measured
# 2026-07-30 on the live cohort: 1,108 securities disagree on sector, 769 on
# country). Resolution is by REPORTED CONSENSUS -- the value the most holdings
# report wins -- with an alphabetical tiebreak so the same cohort always builds
# the same publication. A security no holding classifies gets an honest JSON null,
# never a guess (31 of 10,794 on the live cohort).
#
# The cohort mirrors ``_prepare_fund_exposure_source`` (current DBT holdings) and
# the alias join mirrors ``_FUND_EXPOSURE_MATCHES``: two single-kind equality
# joins UNIONed, never one OR'd join (the OR form cannot use the alias indexes).
# Aliases are NOT point-in-time filtered here: the classification describes the
# security itself, so every alias it was ever known by is a valid route to it.
# ---------------------------------------------------------------------------
_ISSUER_CLASSIFICATION_SQL = """
CREATE TEMP TABLE _bond_issuer_classification ON COMMIT DROP AS
WITH hold AS MATERIALIZED (
    SELECT cusip, isin,
           nullif(btrim(issuer_category), '') AS sector,
           nullif(btrim(source_typed_projection->>'INVESTMENT_COUNTRY'), '') AS country
    FROM sec_nport_holdings_v2_current
    WHERE report_date <= %(as_of)s
      AND upper(btrim(coalesce(source_typed_projection->>'ASSET_CAT', ''))) = 'DBT'
), matched AS MATERIALIZED (
    SELECT al.security_id, h.sector, h.country
    FROM sec_current_bond_security_alias_v1 al
    JOIN hold h ON h.cusip = al.alias_value
    WHERE al.alias_kind = 'cusip9'
    UNION ALL
    SELECT al.security_id, h.sector, h.country
    FROM sec_current_bond_security_alias_v1 al
    JOIN hold h ON h.isin = al.alias_value
    WHERE al.alias_kind = 'isin'
), sector_consensus AS (
    SELECT security_id, sector,
           row_number() OVER (PARTITION BY security_id
                              ORDER BY count(*) DESC, sector ASC) AS rn
    FROM matched WHERE sector IS NOT NULL GROUP BY security_id, sector
), country_consensus AS (
    SELECT security_id, country,
           row_number() OVER (PARTITION BY security_id
                              ORDER BY count(*) DESC, country ASC) AS rn
    FROM matched WHERE country IS NOT NULL GROUP BY security_id, country
)
-- LEFT JOIN, never a correlated subquery per security: the consensus CTEs carry
-- no index, so a per-row lookup degrades to one scan of them PER security and
-- the build stops finishing (measured on the live cohort 2026-07-30). Joining
-- the rn=1 slices hashes them once. LEFT so an unclassified security still gets
-- its row, with both keys NULL.
SELECT sec.security_id, c.country AS issuer_country, s.sector AS issuer_sector
FROM sec_current_bond_security_v1 sec
LEFT JOIN country_consensus c ON c.security_id = sec.security_id AND c.rn = 1
LEFT JOIN sector_consensus s ON s.security_id = sec.security_id AND s.rn = 1
"""

# The resolved classification for one security, or an honest NULL. At most one
# row per security (the temp table is built at security grain), so it is scalar.
_ISSUER_COUNTRY_SQL = """(
    SELECT ic.issuer_country FROM _bond_issuer_classification ic
    WHERE ic.security_id = s.security_id)"""

_ISSUER_SECTOR_SQL = """(
    SELECT ic.issuer_sector FROM _bond_issuer_classification ic
    WHERE ic.security_id = s.security_id)"""

_CATALOG_SQL = f"""
INSERT INTO bond_serving_facts ({_COLUMNS})
SELECT %(pub)s, 'catalog', s.security_id, '', '', '',
       CASE WHEN s.identity_state = 'ambiguous' THEN 'degraded' ELSE 'available' END,
       CASE WHEN s.identity_state = 'ambiguous' THEN 'identity_ambiguous' ELSE NULL END,
       s.identity_state, s.identity_state, %(as_of)s, NULL,
       CASE WHEN s.identity_state = 'ambiguous' THEN 50 ELSE 100 END,
       bond_serving_scrub(jsonb_build_object(
           'display', concat_ws(' ', s.issuer_name,
               CASE WHEN s.coupon_rate IS NOT NULL THEN s.coupon_rate::text || '%%' END,
               s.maturity_date::text),
           'issuer_name', s.issuer_name,
           'currency', s.currency,
           'coupon_type', s.coupon_type,
           'coupon_rate', s.coupon_rate,
           'maturity_date', s.maturity_date,
           'is_144a', s.is_144a,
           -- public normalized identifiers for CUSIP9/ISIN search (only VALID aliases
           -- reach the alias view; rejected/placeholder identifiers never do).
           'aliases_cusip9', (SELECT COALESCE(jsonb_agg(v ORDER BY v), '[]'::jsonb)
               FROM (SELECT DISTINCT a.alias_value AS v FROM sec_current_bond_security_alias_v1 a
                     WHERE a.security_id = s.security_id AND a.alias_kind = 'cusip9') c),
           'aliases_isin', (SELECT COALESCE(jsonb_agg(v ORDER BY v), '[]'::jsonb)
               FROM (SELECT DISTINCT a.alias_value AS v FROM sec_current_bond_security_alias_v1 a
                     WHERE a.security_id = s.security_id AND a.alias_kind = 'isin') c),
           -- Wave 1 computed summary values (null-honest; promoted sources only).
           'latest_price_pct', {_LATEST_PRICE_PCT_SQL},
           'security_ytm', {_metric_value_sql("security_ytm")},
           'security_ytw', {_metric_value_sql("security_ytw")},
           -- Reported issuer classification, resolved to the security grain by
           -- reported consensus (null-honest: unclassified serves JSON null).
           'issuer_country', {_ISSUER_COUNTRY_SQL},
           'issuer_sector', {_ISSUER_SECTOR_SQL},
           'identity_state', s.identity_state))
FROM sec_current_bond_security_v1 s
ON CONFLICT DO NOTHING
"""

_DETAIL_SQL = f"""
INSERT INTO bond_serving_facts ({_COLUMNS})
SELECT %(pub)s, 'detail', s.security_id, '', '', '',
       CASE WHEN s.identity_state = 'ambiguous' THEN 'degraded' ELSE 'available' END,
       CASE WHEN s.identity_state = 'ambiguous' THEN 'identity_ambiguous' ELSE NULL END,
       s.identity_state, s.identity_state, %(as_of)s, NULL,
       CASE WHEN s.identity_state = 'ambiguous' THEN 50 ELSE 100 END,
       bond_serving_scrub(jsonb_build_object(
           'issuer_name', s.issuer_name,
           'currency', s.currency,
           'coupon_type', s.coupon_type,
           'coupon_rate', s.coupon_rate,
           'coupon_schedule', s.terms -> 'coupon_schedule',
           'maturity_date', s.maturity_date,
           'seniority', s.seniority,
           'secured', s.secured,
           'call_schedule', s.terms -> 'call_schedule',
           'put_schedule', s.terms -> 'put_schedule',
           'is_144a', s.is_144a,
           'day_count', s.day_count,
           'settlement_convention', s.settlement_convention,
           -- Wave 1 computed metrics (null-honest; promoted current metric view
           -- only). Coupon above stays a reported TERM, never a yield.
           'current_yield', {_metric_value_sql("current_yield")},
           'security_ytm', {_metric_value_sql("security_ytm")},
           'security_ytw', {_metric_value_sql("security_ytw")},
           'wal', {_metric_value_sql("wal")},
           'identity_state', s.identity_state,
           -- ambiguous identity surfaces the conflicting VALUES only (neutral
           -- evidence), never the internal contributing_observation_ids lineage.
           'identity_evidence', CASE WHEN s.identity_state = 'ambiguous' THEN jsonb_build_object(
                   'distinct_cusip9', s.identity_evidence -> 'distinct_cusip9',
                   'distinct_isin', s.identity_evidence -> 'distinct_isin',
                   'distinct_issuer_name', s.identity_evidence -> 'distinct_issuer_name',
                   'conflicts', s.identity_evidence -> 'conflicts') ELSE NULL END,
           'aliases', (
               SELECT COALESCE(jsonb_agg(jsonb_build_object(
                       'alias_kind', a.alias_kind, 'alias_value', a.alias_value,
                       'valid_from', a.valid_from, 'valid_to', a.valid_to)
                   ORDER BY a.alias_kind, a.valid_from), '[]'::jsonb)
               FROM sec_current_bond_security_alias_v1 a WHERE a.security_id = s.security_id)))
FROM sec_current_bond_security_v1 s
ON CONFLICT DO NOTHING
"""

# ---------------------------------------------------------------------------
# observations (grain: security_observation) over the two PIT lanes. Each lane
# hardcodes its literal (Task 4 structural isolation); the serving projects the
# lane into both the fact column and the payload (Global Constraint 3). Duplicate
# cohort -> observation_ambiguous (both rows retained); fund_asof age>=31d -> stale.
# ---------------------------------------------------------------------------
_OBSERVATIONS_SQL = f"""
INSERT INTO bond_serving_facts ({_COLUMNS})
SELECT %(pub)s, 'observations', o.security_id, o.lane, '',
       concat_ws('|', o.observation_date::text, o.lane, o.rn::text),
       o.state, o.reason_code, NULL, o.ambiguity_state, %(as_of)s, o.observation_date, 100,
       bond_serving_scrub(jsonb_build_object(
           'lane', o.lane,
           'observation_date', o.observation_date,
           'price', o.price,
           'price_type', o.price_type,
           'accrued_treatment', o.accrued_treatment,
           'price_state', o.price_state,
           'ytm', o.ytm,
           'is_144a', o.is_144a,
           'daily_key_state', o.daily_key_state,
           'observation_age_days', o.observation_age_days,
           'is_stale', o.is_stale))
FROM (
    SELECT lane, security_id, observation_date, source_row_number, price, price_type,
           accrued_treatment, price_state, ytm, is_144a, daily_key_state,
           -- latest is INFORMATIVE only: with no fund as_of anchor there is nothing to
           -- measure staleness against, so is_stale/observation_age_days are an HONEST
           -- NULL (absence), never a fabricated ``false``.
           NULL::integer AS observation_age_days, NULL::boolean AS is_stale,
           row_number() OVER (PARTITION BY security_id, observation_date ORDER BY source_row_number) AS rn,
           CASE WHEN daily_key_state = 'duplicate_in_matching_cohort'
                THEN 'degraded' ELSE 'available' END AS state,
           CASE WHEN daily_key_state = 'duplicate_in_matching_cohort'
                THEN 'observation_ambiguous' ELSE NULL END AS reason_code,
           CASE WHEN daily_key_state = 'duplicate_in_matching_cohort'
                THEN 'ambiguous' ELSE 'resolved' END AS ambiguity_state
    FROM bond_price_latest_v1
    UNION ALL
    SELECT lane, security_id, observation_date, source_row_number, price, price_type,
           accrued_treatment, price_state, ytm, is_144a, daily_key_state,
           observation_age_days, is_stale,
           row_number() OVER (PARTITION BY security_id, observation_date ORDER BY source_row_number) AS rn,
           CASE WHEN daily_key_state = 'duplicate_in_matching_cohort' THEN 'degraded'
                WHEN is_stale THEN 'degraded' ELSE 'available' END AS state,
           CASE WHEN daily_key_state = 'duplicate_in_matching_cohort' THEN 'observation_ambiguous'
                WHEN is_stale THEN 'observation_stale' ELSE NULL END AS reason_code,
           CASE WHEN daily_key_state = 'duplicate_in_matching_cohort'
                THEN 'ambiguous' ELSE 'resolved' END AS ambiguity_state
    FROM bond_price_fund_asof_v1(%(as_of)s)
) o
ON CONFLICT DO NOTHING
"""

# ---------------------------------------------------------------------------
# fund_exposure (grain: security_fund). PIT reverse lookup: security -> PIT-valid
# alias -> N-PORT holding (report_date <= as_of). Bridge class fan-out is collapsed
# by DISTINCT to the holding lot before aggregating at (security, series) grain.
# ---------------------------------------------------------------------------
_FUND_EXPOSURE_MATCHES = """
matched AS (
    SELECT sec.security_id, h.series_id, h.accession_number, h.holding_id,
           h.report_date, h.signed_market_value, h.signed_pct_of_nav
    FROM sec_current_bond_security_v1 sec
    JOIN sec_current_bond_security_alias_v1 al
      ON al.security_id = sec.security_id
     AND al.valid_from <= %(as_of)s
     AND (al.valid_to IS NULL OR al.valid_to > %(as_of)s)
    JOIN _bond_fund_holdings h
      ON al.alias_kind = 'cusip9' AND h.cusip = al.alias_value
    UNION ALL
    SELECT sec.security_id, h.series_id, h.accession_number, h.holding_id,
           h.report_date, h.signed_market_value, h.signed_pct_of_nav
    FROM sec_current_bond_security_v1 sec
    JOIN sec_current_bond_security_alias_v1 al
      ON al.security_id = sec.security_id
     AND al.valid_from <= %(as_of)s
     AND (al.valid_to IS NULL OR al.valid_to > %(as_of)s)
    JOIN _bond_fund_holdings h
      ON al.alias_kind = 'isin' AND h.isin = al.alias_value
)
"""

# Guard A: active PIT aliases must be unique across security identities. The
# security master withholds every cross-identity collision before publication.
_FUND_EXPOSURE_GUARD_IDENTITY = """
SELECT 1
FROM sec_current_bond_security_alias_v1
WHERE valid_from <= %(as_of)s
  AND (valid_to IS NULL OR valid_to > %(as_of)s)
GROUP BY alias_kind, alias_value
HAVING count(DISTINCT security_id) > 1
LIMIT 1
"""

# Guard B: after collapsing the bridge class fan-out to the lot, a single
# (security, series, lot, report) must not carry more than one distinct value.
_FUND_EXPOSURE_GUARD_ROWS = """
WITH lots AS (
    SELECT DISTINCT series_id, accession_number, holding_id, report_date,
           signed_market_value, signed_pct_of_nav
    FROM _bond_fund_holdings
)
SELECT 1
FROM lots
GROUP BY series_id, accession_number, holding_id, report_date
HAVING count(*) > 1
LIMIT 1
"""

_FUND_EXPOSURE_SQL = f"""WITH {_FUND_EXPOSURE_MATCHES},
lots AS (
    SELECT DISTINCT security_id, series_id, accession_number, holding_id,
           report_date, signed_market_value AS mv, signed_pct_of_nav AS pct
    FROM matched
), latest AS (
    SELECT security_id, series_id, max(report_date) AS report_date
    FROM lots GROUP BY security_id, series_id
), agg AS (
    SELECT l.security_id, l.series_id, l.report_date,
           sum(l.mv) AS holding_market_value,
           sum(l.pct) AS holding_pct_of_nav,
           count(*) AS position_lot_count
    FROM lots l
    JOIN latest lt ON lt.security_id = l.security_id AND lt.series_id = l.series_id
                  AND lt.report_date = l.report_date
    GROUP BY l.security_id, l.series_id, l.report_date
)
INSERT INTO bond_serving_facts ({_COLUMNS})
SELECT %(pub)s, 'fund_exposure', a.security_id, '', a.series_id, a.report_date::text,
       'available', NULL, NULL, 'resolved', %(as_of)s, NULL, 100,
       bond_serving_scrub(jsonb_build_object(
           'as_of', %(as_of)s,
           'series_id', a.series_id,
           'holding_market_value', a.holding_market_value,
           'holding_pct_of_nav', a.holding_pct_of_nav,
           'position_lot_count', a.position_lot_count,
           'report_date', a.report_date))
FROM agg a
ON CONFLICT DO NOTHING
"""

_SURFACE_SQL: dict[str, str] = {
    "catalog": _CATALOG_SQL,
    "detail": _DETAIL_SQL,
    "observations": _OBSERVATIONS_SQL,
    "fund_exposure": _FUND_EXPOSURE_SQL,
}

# Surfaces allowed to project zero rows without failing the coverage gate.
# fund_exposure (owner decision 2026-07-26): the reverse N-PORT look-up is not a
# product we want, so an empty projection is the intended state, not a regression.
# The surface is still declared by the contract — dropping it means a coordinated
# contract-digest change plus retiring /v2/bonds/{id}/fund-exposure and its UI block.
# Until that happens this exemption is what keeps the gate honest for the other three.
_MAY_BE_EMPTY = frozenset({"fund_exposure"})


def install_schema(conn: psycopg.Connection) -> None:
    """Apply the serving DDL idempotently (bond source relations must pre-exist)."""
    with conn.cursor() as cur:
        for name in _SCHEMA_FILES:
            cur.execute((ROOT / "schemas" / name).read_text(encoding="utf-8"))


def publication_id_for(as_of: date, code_revision: str) -> UUID:
    return uuid5(_NAMESPACE, f"{contract.SERVING_PRODUCT}|{as_of.isoformat()}|{code_revision}")


def _relation_exists(conn: psycopg.Connection, name: str) -> bool:
    return conn.execute("SELECT to_regclass(%s) IS NOT NULL", (name,)).fetchone()[0]


def _function_exists(conn: psycopg.Connection, signature: str) -> bool:
    return conn.execute("SELECT to_regprocedure(%s) IS NOT NULL", (signature,)).fetchone()[0]


def _surface_present(conn: psycopg.Connection, surface: str) -> bool:
    if not all(_relation_exists(conn, rel) for rel in _SURFACE_REQUIRED_RELATIONS[surface]):
        return False
    # observations reads BOTH lanes: the latest view AND the point-in-time function.
    # to_regclass never resolves a function, so the fund_asof lane is gated here.
    if surface == "observations":
        return _function_exists(conn, "bond_price_fund_asof_v1(date)")
    return True


def _resolve_anchor(
    conn: psycopg.Connection, run_id: UUID | None, package_id: UUID | None,
) -> tuple[UUID, UUID]:
    if run_id is not None and package_id is not None:
        return run_id, package_id
    row = conn.execute(
        """
        SELECT r.run_id, p.package_id
        FROM sec_validated_raw_runs r
        JOIN sec_ingestion_runs ir ON ir.run_id=r.run_id AND ir.raw_validated_at IS NOT NULL
        JOIN sec_source_packages p ON p.run_id=r.run_id
        ORDER BY ir.raw_validated_at DESC, p.package_id
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        raise RuntimeError("bond serving publication requires a validated source run/package anchor")
    return row[0], row[1]


def _guard_fund_exposure(conn: psycopg.Connection, params: dict[str, Any]) -> None:
    """Hard-fail before writing fund_exposure if the reverse lookup would multiply."""
    if conn.execute(_FUND_EXPOSURE_GUARD_IDENTITY, params).fetchone() is not None:
        raise BondFundExposureMultiplicationError(
            "fund_exposure holding->security row multiplication detected "
            "(one holding lot maps to more than one security)"
        )
    if conn.execute(_FUND_EXPOSURE_GUARD_ROWS, params).fetchone() is not None:
        raise BondFundExposureMultiplicationError(
            "fund_exposure row multiplication detected "
            "(one holding lot collapsed to more than one distinct value)"
        )


def _prepare_issuer_classification_source(
    conn: psycopg.Connection, params: dict[str, Any],
) -> None:
    """Resolve the reported issuer classification to security grain once per build."""
    conn.execute("DROP TABLE IF EXISTS _bond_issuer_classification")
    conn.execute(_ISSUER_CLASSIFICATION_SQL, params)
    conn.execute(
        "CREATE INDEX _bond_issuer_classification_security_idx "
        "ON _bond_issuer_classification(security_id)"
    )
    conn.execute("ANALYZE _bond_issuer_classification")


def _prepare_fund_exposure_source(
    conn: psycopg.Connection, params: dict[str, Any],
) -> None:
    """Materialize and index the current DBT holding cohort once per build."""
    conn.execute("DROP TABLE IF EXISTS _bond_fund_holdings")
    conn.execute(
        """
        CREATE TEMP TABLE _bond_fund_holdings ON COMMIT DROP AS
        SELECT series_id, accession_number, holding_id, report_date,
               signed_market_value, signed_pct_of_nav, cusip, isin
        FROM sec_nport_holdings_v2_current
        WHERE report_date <= %(as_of)s
          AND upper(btrim(coalesce(source_typed_projection->>'ASSET_CAT',''))) = 'DBT'
        """,
        params,
    )
    conn.execute(
        "CREATE INDEX _bond_fund_holdings_cusip_idx "
        "ON _bond_fund_holdings(cusip) WHERE cusip IS NOT NULL"
    )
    conn.execute(
        "CREATE INDEX _bond_fund_holdings_isin_idx "
        "ON _bond_fund_holdings(isin) WHERE isin IS NOT NULL"
    )
    conn.execute("ANALYZE _bond_fund_holdings")


def materialize(
    conn: psycopg.Connection,
    *,
    as_of: date,
    code_revision: str,
    source_run_id: UUID | None = None,
    source_package_id: UUID | None = None,
    allow_missing_surfaces: bool = False,
) -> dict[str, Any]:
    """Prepare -> project every present surface -> validate -> current, atomically.

    Fails closed (``BondServingSurfaceCoverageError``) and promotes NOTHING when any
    contract-declared surface's required source relations are missing, so a partial
    surface set can never be silently promoted. Pass ``allow_missing_surfaces=True``
    only for tests that deliberately seed a subset of the surfaces.
    """
    publication_id = publication_id_for(as_of, code_revision)
    product = contract.SERVING_PRODUCT

    existing = conn.execute(
        "SELECT lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
        (publication_id,),
    ).fetchone()

    surfaces_written: list[str] = []
    empty: list[str] = []
    if existing is None:
        anchor_run, anchor_package = _resolve_anchor(conn, source_run_id, source_package_id)
        version = conn.execute(
            "SELECT COALESCE(max(publication_version),0)+1 "
            "FROM sec_derived_publications WHERE product=%s",
            (product,),
        ).fetchone()[0]
        fingerprint = hashlib.sha256(
            f"{product}|{as_of.isoformat()}|{anchor_run}".encode()
        ).hexdigest()
        conn.execute(
            "INSERT INTO sec_derived_publications"
            "(publication_id,product,publication_version,source_run_id,source_package_id,build_fingerprint)"
            " VALUES(%s,%s,%s,%s,%s,%s)",
            (publication_id, product, version, anchor_run, anchor_package, fingerprint),
        )
        params = {"pub": publication_id, "as_of": as_of}
        consumed: dict[str, str] = {}
        for surface in contract.SURFACES:
            name = surface["surface"]
            if not _surface_present(conn, name):
                continue
            if name == "catalog":
                _prepare_issuer_classification_source(conn, params)
            if name == "fund_exposure":
                _prepare_fund_exposure_source(conn, params)
                _guard_fund_exposure(conn, params)
            projected = conn.execute(_SURFACE_SQL[name], params).rowcount
            surfaces_written.append(name)
            if projected <= 0 and name not in _MAY_BE_EMPTY:
                empty.append(name)
            pub_row = conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
                (surface["source_product"],),
            ).fetchone()
            if pub_row is not None:
                consumed[name] = str(pub_row[0])
        missing = [s for s in contract.surface_names() if s not in set(surfaces_written)]
        if (missing or empty) and not allow_missing_surfaces:
            # Fail closed BEFORE the build pin, validation and current-pointer flip:
            # nothing is validated and nothing is promoted for a partial surface set.
            # Cardinality counts as coverage: a surface whose relations all EXIST but
            # which projected ZERO rows is just as partial as an absent one (2026-07-24:
            # fund_exposure published, validated and promoted with 0 rows because the
            # gate only asked whether the relations existed).
            raise BondServingSurfaceCoverageError(
                "bond serving build would promote a partial surface set; missing surface "
                f"source relations {missing}; surfaces projecting zero rows {empty}"
            )
        conn.execute(
            "INSERT INTO bond_serving_builds"
            "(publication_id,as_of_date,input_fingerprint,consumed_source_publications)"
            " VALUES(%s,%s,%s,%s)",
            (publication_id, as_of, fingerprint, Json(consumed)),
        )
        conn.execute("SELECT sec_validate_derived_publication(%s)", (publication_id,))

    current = conn.execute(
        "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s", (product,)
    ).fetchone()
    if current is None or current[0] != publication_id:
        conn.execute("SELECT sec_set_current_derived_publication(%s,%s)", (product, publication_id))

    row_count = conn.execute(
        "SELECT count(*) FROM bond_serving_facts WHERE publication_id=%s",
        (publication_id,),
    ).fetchone()[0]
    return {
        "product": product,
        "publication_id": str(publication_id),
        "surfaces_written": surfaces_written,
        "rows": row_count,
        "state": "current",
    }
