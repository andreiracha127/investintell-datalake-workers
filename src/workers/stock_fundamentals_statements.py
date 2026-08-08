"""Incrementally materialize the app-compatible stock fundamentals statements.

The app's ``stock_fundamentals_statements_mv`` remains intact.  This worker
builds its additive companion table one affected CIK at a time, using the MV's
catalog definition as the single semantic source of truth.  It therefore avoids
the routine whole-MV refresh while never maintaining a second copy of the large
quarterly/annual assembly query.
"""

from __future__ import annotations

import datetime as dt
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.db import LOCK_STOCK_FUNDAMENTALS_STATEMENTS, advisory_lock, connect


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "stock_fundamentals_statements_incremental.sql"
TARGET = "stock_fundamentals_statements_incremental"
SOURCE_MV = "stock_fundamentals_statements_mv"
MIN_YEAR = 1900
MAX_YEAR_AHEAD = 1

# Must mirror the concepts that feed the app MV.  Tracking only semantic inputs
# keeps unrelated SEC facts from causing a needless statement recomputation.
CONCEPTS = (
    "RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet",
    "CostOfGoodsAndServicesSold", "CostOfRevenue", "CostOfGoodsSold", "GrossProfit",
    "ResearchAndDevelopmentExpense", "SellingGeneralAndAdministrativeExpense", "OperatingIncomeLoss",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    "IncomeTaxExpenseBenefit", "NetIncomeLoss", "EarningsPerShareDiluted",
    "WeightedAverageNumberOfDilutedSharesOutstanding", "DepreciationDepletionAndAmortization",
    "DepreciationAmortizationAndAccretionNet", "Assets", "Liabilities", "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    "CashAndCashEquivalentsAtCarryingValue", "LongTermDebtCurrent", "DebtCurrent",
    "LongTermDebtNoncurrent", "LongTermDebt", "AssetsCurrent", "LiabilitiesCurrent",
    "NetCashProvidedByUsedInOperatingActivities", "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsOfDividendsCommonStock", "PaymentsOfDividends", "CommonStockDividendsPerShareDeclared",
)
COLUMNS = (
    "ticker", "cik", "freq", "period_end", "fy", "fp", "filed", "revenue", "cost_of_revenue",
    "gross_profit", "rnd_expense", "sga_expense", "operating_income", "pretax_income", "income_tax",
    "net_income", "eps_diluted", "shares_diluted", "d_and_a", "assets", "liabilities", "equity",
    "cash", "st_debt", "lt_debt", "current_assets", "current_liabilities", "ocf", "capex", "fcf",
    "dividends_paid", "dps",
)


@dataclass(frozen=True)
class SourceFact:
    identity: str
    fingerprint: str
    cik: int
    accession: str | None
    period_start: dt.date | None
    period_end: dt.date | None


@dataclass(frozen=True)
class Watermark:
    fingerprint: str
    cik: int


@dataclass(frozen=True)
class UniverseConstituent:
    """The subset of the served universe that determines statement output."""

    ticker: str
    fingerprint: str
    cik: int


@dataclass(frozen=True)
class UniverseWatermark:
    fingerprint: str
    cik: int


@dataclass(frozen=True)
class ChangePlan:
    upserts: tuple[SourceFact, ...]
    deletes: tuple[str, ...]
    affected_ciks: tuple[int, ...]
    universe_upserts: tuple[UniverseConstituent, ...] = ()
    universe_deletes: tuple[str, ...] = ()


_FACTS_SQL = """
SELECT
    concat_ws(E'\\x1f', f.cik::text, coalesce(f.accn, ''), f.taxonomy, f.concept,
              f.unit, coalesce(f.period_start::text, ''), f.period_end::text, f.filed::text) AS fact_identity,
    md5(concat_ws(E'\\x1f', f.cik::text, coalesce(f.accn, ''), f.taxonomy, f.concept,
                  f.unit, coalesce(f.period_start::text, ''), f.period_end::text, f.filed::text,
                  coalesce(f.val::text, ''), coalesce(f.fy::text, ''), coalesce(f.fp, ''))) AS fact_fingerprint,
    f.cik, f.accn, f.period_start, f.period_end
FROM sec_xbrl_facts f
JOIN (SELECT DISTINCT cik FROM universe_constituents WHERE cik IS NOT NULL) u ON u.cik = f.cik
WHERE f.taxonomy = 'us-gaap'
  AND f.unit IN ('USD', 'USD/shares', 'shares')
  AND f.concept = ANY(%s)
  -- Match the app MV's retained history.  Keep absurd future dates in the
  -- scan so the quarantine observes them instead of silently losing evidence.
  AND (f.period_end >= CURRENT_DATE - INTERVAL '12 years'
       OR f.period_end >= DATE '2100-01-01')
"""

_UNIVERSE_SQL = """
SELECT u.ticker, u.cik,
       md5(concat_ws(E'\\x1f', u.ticker, u.cik::text)) AS universe_fingerprint
FROM (
    SELECT DISTINCT upper(ticker) AS ticker, cik
    FROM universe_constituents
    WHERE cik IS NOT NULL
) AS u
"""


def install_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def load_source_facts(conn: Any) -> list[SourceFact]:
    with conn.cursor() as cur:
        cur.execute(_FACTS_SQL, (list(CONCEPTS),))
        rows = cur.fetchall()
    return [
        SourceFact(
            identity=row[0], fingerprint=row[1], cik=int(row[2]), accession=row[3],
            period_start=row[4], period_end=row[5],
        )
        for row in rows
    ]


def load_watermarks(conn: Any) -> dict[str, Watermark]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT fact_identity, fact_fingerprint, cik "
            "FROM stock_fundamentals_statement_fact_watermarks"
        )
        return {row[0]: Watermark(fingerprint=row[1], cik=int(row[2])) for row in cur.fetchall()}


def universe_identity(ticker: str, cik: int) -> str:
    """Return the app-MV semantic identity for one universe membership."""
    return f"{ticker}\x1f{cik}"


def load_universe_constituents(conn: Any) -> list[UniverseConstituent]:
    """Load the normalized ticker/CIK memberships consumed by the source MV."""
    with conn.cursor() as cur:
        cur.execute(_UNIVERSE_SQL)
        rows = cur.fetchall()
    return [
        UniverseConstituent(ticker=str(row[0]), fingerprint=str(row[2]), cik=int(row[1]))
        for row in rows
    ]


def load_universe_watermarks(conn: Any) -> dict[str, UniverseWatermark]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, cik, universe_fingerprint "
            "FROM stock_fundamentals_statement_universe_watermarks"
        )
        return {
            universe_identity(str(row[0]), int(row[1])): UniverseWatermark(
                fingerprint=str(row[2]), cik=int(row[1])
            )
            for row in cur.fetchall()
        }


def is_valid_fact_date(fact: SourceFact, *, today: dt.date | None = None) -> bool:
    """Reject impossible, reversed, or implausibly future reporting periods."""
    today = today or dt.date.today()
    if fact.period_end is None or not (MIN_YEAR <= fact.period_end.year <= today.year + MAX_YEAR_AHEAD):
        return False
    return fact.period_start is None or (
        MIN_YEAR <= fact.period_start.year <= fact.period_end.year and fact.period_start <= fact.period_end
    )


def plan_changes(
    facts: list[SourceFact], watermarks: dict[str, Watermark], *,
    universe: list[UniverseConstituent] | None = None,
    universe_watermarks: dict[str, UniverseWatermark] | None = None,
    rebuild: bool = False,
) -> ChangePlan:
    current = {fact.identity: fact for fact in facts}
    upserts = tuple(
        fact for identity, fact in current.items()
        if rebuild or identity not in watermarks or watermarks[identity].fingerprint != fact.fingerprint
    )
    deletes = tuple(sorted(identity for identity in watermarks if identity not in current))
    affected = {fact.cik for fact in upserts}
    affected.update(watermarks[identity].cik for identity in deletes)
    if rebuild:
        affected.update(fact.cik for fact in facts)
        affected.update(mark.cik for mark in watermarks.values())

    current_universe = {
        universe_identity(member.ticker, member.cik): member
        for member in (universe or [])
    }
    previous_universe = universe_watermarks or {}
    universe_upserts = tuple(
        member for identity, member in current_universe.items()
        if rebuild or identity not in previous_universe
        or previous_universe[identity].fingerprint != member.fingerprint
    )
    universe_deletes = tuple(sorted(identity for identity in previous_universe if identity not in current_universe))
    affected.update(member.cik for member in universe_upserts)
    affected.update(previous_universe[identity].cik for identity in universe_deletes)
    if rebuild:
        affected.update(member.cik for member in current_universe.values())
        affected.update(mark.cik for mark in previous_universe.values())
    return ChangePlan(
        upserts=upserts,
        deletes=deletes,
        affected_ciks=tuple(sorted(affected)),
        universe_upserts=universe_upserts,
        universe_deletes=universe_deletes,
    )


def quarantine_invalid_facts(conn: Any, facts: list[SourceFact]) -> None:
    if not facts:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO stock_fundamentals_statement_fact_quarantine "
            "(fact_identity,cik,accession,period_start,period_end,reason_code) "
            "VALUES (%s,%s,%s,%s,%s,'invalid_reporting_period') "
            "ON CONFLICT (fact_identity) DO UPDATE SET observed_at=now(), reason_code=EXCLUDED.reason_code",
            [(f.identity, f.cik, f.accession, f.period_start, f.period_end) for f in facts],
        )


def _source_definition(conn: Any) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_get_viewdef(%s::regclass, true)", (SOURCE_MV,))
        row = cur.fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"{SOURCE_MV} definition is unavailable; run the explicit MV installation first")
    return str(row[0]).rstrip().rstrip(";")


def scope_definition_to_changed_universe(definition: str) -> str:
    """Bind the source CTE to a tiny changed-CIK universe before it reads facts."""
    relation_pattern = r"\b(FROM|JOIN)\s+(?:public\.)?universe_constituents\b"
    if len(re.findall(relation_pattern, definition, flags=re.IGNORECASE)) != 1:
        raise RuntimeError("unexpected universe relation in stock fundamentals MV definition")
    scoped = re.sub(
        relation_pattern,
        r"\1 stock_fundamentals_statements_scope_universe",
        definition,
        count=1,
        flags=re.IGNORECASE,
    )
    return re.sub(
        r"\buniverse_constituents\.",
        "stock_fundamentals_statements_scope_universe.",
        scoped,
        flags=re.IGNORECASE,
    )


def target_is_empty(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"SELECT NOT EXISTS (SELECT 1 FROM {TARGET} LIMIT 1)")
        row = cur.fetchone()
    return bool(row and row[0])


def bootstrap_from_source_mv(conn: Any) -> int:
    """Seed an empty target from the already validated semantic source MV."""
    columns = ", ".join(COLUMNS)
    updates = ", ".join(
        f"{column} = EXCLUDED.{column}"
        for column in COLUMNS
        if column not in {"ticker", "freq", "period_end"}
    )
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {TARGET} ({columns}) "
            f"SELECT {columns} FROM {SOURCE_MV} "
            "WHERE period_end >= DATE '1900-01-01' "
            "AND period_end <= CURRENT_DATE + INTERVAL '1 year' "
            "ON CONFLICT (ticker, freq, period_end) DO UPDATE SET " + updates
        )
        return max(cur.rowcount, 0)


def recompute_scoped(conn: Any, ciks: tuple[int, ...]) -> tuple[int, int]:
    """Replace only affected CIK output inside the additive compatible table."""
    if not ciks:
        return (0, 0)
    definition = scope_definition_to_changed_universe(_source_definition(conn))
    columns = ", ".join(COLUMNS)
    updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in COLUMNS if column not in {"ticker", "freq", "period_end"})
    with conn.cursor() as cur:
        # The MV definition's first CTE reads this name.  A scoped temporary
        # relation makes the CIK restriction reach its multi-use ``facts`` CTE
        # instead of filtering only after the full XBRL assembly is complete.
        cur.execute(
            "CREATE TEMP TABLE stock_fundamentals_statements_scope_universe ON COMMIT DROP AS "
            "SELECT * FROM universe_constituents WHERE cik = ANY(%s)",
            (list(ciks),),
        )
        cur.execute(
            "CREATE INDEX stock_fundamentals_statements_scope_universe_cik_idx "
            "ON stock_fundamentals_statements_scope_universe (cik)"
        )
        cur.execute(
            "CREATE TEMP TABLE stock_fundamentals_statements_scope ON COMMIT DROP AS "
            f"SELECT {columns} FROM ({definition}) AS source "
            "WHERE cik = ANY(%s) "
            "AND period_end >= DATE '1900-01-01' "
            "AND period_end <= CURRENT_DATE + INTERVAL '1 year'",
            (list(ciks),),
        )
        cur.execute(f"DELETE FROM {TARGET} WHERE cik = ANY(%s)", (list(ciks),))
        deleted = max(cur.rowcount, 0)
        cur.execute(
            f"INSERT INTO {TARGET} ({columns}) SELECT {columns} FROM stock_fundamentals_statements_scope "
            "ON CONFLICT (ticker, freq, period_end) DO UPDATE SET " + updates
        )
        upserted = max(cur.rowcount, 0)
    return deleted, upserted


def apply_watermark_changes(conn: Any, plan: ChangePlan) -> None:
    with conn.cursor() as cur:
        if plan.deletes:
            cur.execute(
                "DELETE FROM stock_fundamentals_statement_fact_watermarks WHERE fact_identity = ANY(%s)",
                (list(plan.deletes),),
            )
        if plan.upserts:
            cur.executemany(
                "INSERT INTO stock_fundamentals_statement_fact_watermarks "
                "(fact_identity,fact_fingerprint,cik) VALUES (%s,%s,%s) "
                "ON CONFLICT (fact_identity) DO UPDATE SET fact_fingerprint=EXCLUDED.fact_fingerprint, "
                "cik=EXCLUDED.cik, processed_at=now()",
                [(fact.identity, fact.fingerprint, fact.cik) for fact in plan.upserts],
            )
        if plan.universe_deletes:
            cur.execute(
                "DELETE FROM stock_fundamentals_statement_universe_watermarks "
                "WHERE concat_ws(E'\\x1f', ticker, cik::text) = ANY(%s)",
                (list(plan.universe_deletes),),
            )
        if plan.universe_upserts:
            cur.executemany(
                "INSERT INTO stock_fundamentals_statement_universe_watermarks "
                "(ticker,cik,universe_fingerprint) VALUES (%s,%s,%s) "
                "ON CONFLICT (ticker,cik) DO UPDATE SET "
                "universe_fingerprint=EXCLUDED.universe_fingerprint, processed_at=now()",
                [(member.ticker, member.cik, member.fingerprint) for member in plan.universe_upserts],
            )


def record_run(conn: Any, *, rebuild: bool, changed_facts: int, changed_universe_constituents: int,
               affected_ciks: int,
               rows_deleted: int, rows_upserted: int, quarantined_facts: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO stock_fundamentals_statement_runs "
            "(run_id,mode,changed_facts,changed_universe_constituents,affected_ciks,rows_deleted,"
            "rows_upserted,quarantined_facts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (uuid.uuid4(), "rebuild" if rebuild else "incremental", changed_facts,
             changed_universe_constituents, affected_ciks, rows_deleted, rows_upserted, quarantined_facts),
        )


def run(dsn: str, *, rebuild: bool = False) -> dict[str, int | str | bool]:
    """Apply a transactional changed-fact delta, or an explicit one-shot rebuild."""
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_STOCK_FUNDAMENTALS_STATEMENTS) as acquired:
            if not acquired:
                return {
                    "affected_ciks": 0, "changed_facts": 0, "rows_deleted": 0,
                    "rows_upserted": 0, "skipped": "lock_busy",
                }
            try:
                install_schema(conn)
                source = load_source_facts(conn)
                invalid = [fact for fact in source if not is_valid_fact_date(fact)]
                valid = [fact for fact in source if is_valid_fact_date(fact)]
                watermarks = load_watermarks(conn)
                universe = load_universe_constituents(conn)
                universe_watermarks = load_universe_watermarks(conn)
                empty_target = target_is_empty(conn)
                plan = plan_changes(
                    valid,
                    watermarks,
                    universe=universe,
                    universe_watermarks=universe_watermarks,
                    rebuild=rebuild,
                )
                changed_facts = len(plan.upserts) + len(plan.deletes)
                changed_universe_constituents = len(plan.universe_upserts) + len(plan.universe_deletes)
                quarantine_invalid_facts(conn, invalid)
                if not plan.affected_ciks:
                    if invalid:
                        record_run(
                            conn, rebuild=rebuild, changed_facts=0, changed_universe_constituents=0,
                            affected_ciks=0,
                            rows_deleted=0, rows_upserted=0, quarantined_facts=len(invalid),
                        )
                        conn.commit()
                    return {
                        "affected_ciks": 0, "changed_facts": 0, "rows_deleted": 0,
                        "rows_upserted": 0, "quarantined_facts": len(invalid),
                        "changed_universe_constituents": 0, "skipped": "no_changes",
                    }
                if empty_target and not watermarks and not universe_watermarks and not rebuild:
                    rows_deleted, rows_upserted = 0, bootstrap_from_source_mv(conn)
                else:
                    rows_deleted, rows_upserted = recompute_scoped(conn, plan.affected_ciks)
                # Hold the session advisory lock through both commits.  First make
                # the target relation durable; only then advance the replay-safe
                # input state.  A second-step failure therefore repeats work but
                # can never skip an unmaterialized fact or universe membership.
                conn.commit()
                apply_watermark_changes(conn, plan)
                record_run(
                    conn, rebuild=rebuild, changed_facts=changed_facts,
                    changed_universe_constituents=changed_universe_constituents,
                    affected_ciks=len(plan.affected_ciks), rows_deleted=rows_deleted,
                    rows_upserted=rows_upserted, quarantined_facts=len(invalid),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    return {
        "affected_ciks": len(plan.affected_ciks), "changed_facts": changed_facts,
        "rows_deleted": rows_deleted, "rows_upserted": rows_upserted,
        "quarantined_facts": len(invalid),
        "changed_universe_constituents": changed_universe_constituents,
        "rebuild": rebuild,
    }
