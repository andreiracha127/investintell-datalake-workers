"""Registered materializer for the mixed_quant_v1 point-in-time publication.

This turns the artifact-only pure computations into a production writer: it
resolves stable instrument identities, folds alias history, records observed
returns and income, and wraps the pure V2 look-through engine
(``nport_v2_lookthrough.expand_series``) to derive exposures. Everything lands in
one inactive publication under an advisory lock, stage by stage with
checkpoints, so the build is reproducible and restartable. Promotion to the
active pointer is a separate, atomic step (``publication.promote``).

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
"""

from __future__ import annotations

from collections import defaultdict
import calendar
from dataclasses import replace
from datetime import date
import os
import subprocess
from typing import Any, Callable
import uuid

from src.db import LOCK_MIXED_QUANT_PUBLICATION, advisory_lock, connect, resolve_dsn
from src.quant_data import publication as pub
from src.quant_data.contracts import (
    NAMED_BOND_FACTORS,
    IdentityObservation,
    ResolvedInstrument,
    resolve_identities,
    validate_bond_factor_row,
    validate_class_factor_row,
)
from src.workers.nport_v2_lookthrough import SYNTHETIC_PREFIXES, _exposure_rows, expand_series

PRODUCT = pub.PRODUCT
_OBSERVATION_TABLES = (
    "mixed_quant_identity_observation",
    "mixed_quant_return_observation",
    "mixed_quant_income_observation",
    "mixed_quant_holding_observation",
    "mixed_quant_class_factor_observation",
    "mixed_quant_bond_factor_observation",
)


class MixedQuantSourceError(RuntimeError):
    """A source surface is populated but resolved to nothing downstream.

    Guards the failure mode that produced a funds-only ``mixed_quant_v1``
    publication in production for weeks: the single-name staging joined
    ``sec_cusip_ticker_map.cusip`` (a 9-character CUSIP) against
    ``left(h.cusip, 6)``, matched 0 of 1,582,121 eligible holdings, recorded
    ``equity_cusip_identities: 0`` and reported success. A publication with no
    single names is not a mixed universe — it must fail, not ship.
    """


# Holding rows the single-name cohort is drawn from. Used only to tell "this
# deployment genuinely has no equity holdings" (fine) from "the resolution
# stopped matching" (a defect), so the probe runs solely on the zero path.
_ELIGIBLE_EQUITY_HOLDING_SQL = """
    SELECT EXISTS (
        SELECT 1 FROM sec_nport_holdings_v2_current h
        WHERE h.cusip ~ '^[A-Z0-9]{9}$'
          AND upper(btrim(coalesce(h.source_typed_projection->>'ASSET_CAT',''))) IN ('EC','EP')
    )
"""

# The guard asserts STATE, not the delta of this run. Both identity inserts are
# ``ON CONFLICT DO NOTHING``, so a rerun for an as_of already staged returns
# rowcount 0 — this worker is idempotent and restartable by design, and a guard
# reading rowcount would fail every legitimate resume.
_STAGED_EQUITY_IDENTITY_SQL = """
    SELECT EXISTS (
        SELECT 1 FROM mixed_quant_identity_observation
        WHERE as_of = %s AND instrument_type = 'equity'
    )
"""


def _populate_native_observations(conn: Any, as_of: date) -> dict[str, int]:
    """Populate the activation cohort from existing PIT fund/market/N-PORT data.

    This is intentionally direct and append-only. It carries three years of
    observed returns, fund identities, qualified US equity identities, and the
    latest pinned N-PORT holdings. Missing class factors, bond factors, and cash
    events remain absent instead of being inferred.
    """
    required = (
        "funds_v",
        "nav_timeseries",
        "sec_cusip_ticker_map",
        "sec_nport_holdings_v2_current",
        "stock_daily_returns",
    )
    if not all(
        conn.execute("SELECT to_regclass(%s) IS NOT NULL", (name,)).fetchone()[0]
        for name in required
    ):
        return {}
    counts: dict[str, int] = {}
    result = conn.execute(
        """
        INSERT INTO mixed_quant_identity_observation(
            observation_id,as_of,instrument_type,currency,issuer_id,security_id,
            alias_type,alias_value,deterministic_key,observed_at,valid_from,valid_to,source_lineage
        )
        SELECT md5('fund-ticker|'||%s::text||'|'||instrument_id::text)::uuid,
               %s,'fund',currency,NULL,series_id,'ticker',ticker,
               'fund:'||instrument_id::text,now(),coalesce(inception_date,%s),NULL,
               jsonb_build_object('source_surface','funds_v','instrument_id',instrument_id,'series_id',series_id)
        FROM funds_v
        WHERE nullif(btrim(series_id),'') IS NOT NULL
          AND nullif(btrim(ticker),'') IS NOT NULL
          AND currency ~ '^[A-Z]{3}$'
        ON CONFLICT (observation_id) DO NOTHING
        """,
        (as_of, as_of, as_of),
    )
    counts["fund_identities"] = max(result.rowcount, 0)

    result = conn.execute(
        """
        INSERT INTO mixed_quant_identity_observation(
            observation_id,as_of,instrument_type,currency,issuer_id,security_id,
            alias_type,alias_value,deterministic_key,observed_at,valid_from,valid_to,source_lineage
        )
        SELECT md5('equity-cusip|'||%s::text||'|'||cusip)::uuid,%s,'equity','USD',
               issuer_cik,cusip,'cusip',cusip,'cusip:'||cusip,
               coalesce(last_verified_at,now()),%s,NULL,
               jsonb_build_object('source_surface','sec_cusip_ticker_map','resolved_via',resolved_via)
        FROM (
            SELECT DISTINCT ON (h.cusip)
                   h.cusip,m.issuer_cik,m.resolved_via,m.last_verified_at
            FROM sec_nport_holdings_v2_current h
            JOIN sec_cusip_ticker_map m ON m.cusip=h.cusip
            WHERE h.cusip ~ '^[A-Z0-9]{9}$' AND m.is_tradeable
              AND m.security_type NOT IN ('ETP','Open-End Fund','Closed-End Fund')
              AND upper(btrim(coalesce(h.source_typed_projection->>'ASSET_CAT',''))) IN ('EC','EP')
            ORDER BY h.cusip,m.last_verified_at DESC NULLS LAST
        ) equities
        ON CONFLICT (observation_id) DO NOTHING
        """,
        (as_of, as_of, as_of),
    )
    counts["equity_cusip_identities"] = max(result.rowcount, 0)
    result = conn.execute(
        """
        INSERT INTO mixed_quant_identity_observation(
            observation_id,as_of,instrument_type,currency,issuer_id,security_id,
            alias_type,alias_value,deterministic_key,observed_at,valid_from,valid_to,source_lineage
        )
        SELECT md5('equity-ticker|'||%s::text||'|'||cusip||'|'||ticker)::uuid,
               %s,'equity','USD',issuer_cik,cusip,'ticker',ticker,'cusip:'||cusip,
               coalesce(last_verified_at,now()),%s,NULL,
               jsonb_build_object('source_surface','sec_cusip_ticker_map','resolved_via',resolved_via)
        FROM (
            SELECT DISTINCT ON (h.cusip,m.ticker)
                   h.cusip,m.ticker,m.issuer_cik,m.resolved_via,m.last_verified_at
            FROM sec_nport_holdings_v2_current h
            JOIN sec_cusip_ticker_map m ON m.cusip=h.cusip
            WHERE h.cusip ~ '^[A-Z0-9]{9}$' AND m.is_tradeable
              AND nullif(btrim(m.ticker),'') IS NOT NULL
              AND m.security_type NOT IN ('ETP','Open-End Fund','Closed-End Fund')
              AND upper(btrim(coalesce(h.source_typed_projection->>'ASSET_CAT',''))) IN ('EC','EP')
            ORDER BY h.cusip,m.ticker,m.last_verified_at DESC NULLS LAST
        ) equities
        ON CONFLICT (observation_id) DO NOTHING
        """,
        (as_of, as_of, as_of),
    )
    counts["equity_ticker_identities"] = max(result.rowcount, 0)

    if not conn.execute(_STAGED_EQUITY_IDENTITY_SQL, (as_of,)).fetchone()[0]:
        if conn.execute(_ELIGIBLE_EQUITY_HOLDING_SQL).fetchone()[0]:
            raise MixedQuantSourceError(
                "no single-name identity resolved from eligible equity holdings — "
                "sec_nport_holdings_v2_current has EC/EP rows with 9-character "
                "CUSIPs that sec_cusip_ticker_map did not match. Publishing now "
                "would yield a funds-only 'mixed' universe."
            )

    cutoff_year = as_of.year - 3
    cutoff = as_of.replace(
        year=cutoff_year,
        day=min(as_of.day, calendar.monthrange(cutoff_year, as_of.month)[1]),
    )
    result = conn.execute(
        """
        INSERT INTO mixed_quant_return_observation(
            observation_id,as_of,alias_type,alias_value,period_end,frequency,
            total_return,observed_at,source_lineage
        )
        SELECT md5('fund-return|'||%s::text||'|'||n.instrument_id::text||'|'||n.nav_date::text)::uuid,
               %s,'ticker',f.ticker,n.nav_date,'daily',
               CASE WHEN n.return_type='log' THEN exp(n.return_1d)-1 ELSE n.return_1d END,
               n.nav_date::timestamp AT TIME ZONE 'UTC',
               jsonb_build_object('source_surface','nav_timeseries','instrument_id',n.instrument_id,
                                  'return_type',n.return_type,'source',n.source)
        FROM nav_timeseries n JOIN funds_v f ON f.instrument_id=n.instrument_id
        WHERE n.return_1d IS NOT NULL AND n.nav_date BETWEEN %s AND %s
          AND nullif(btrim(f.ticker),'') IS NOT NULL
        ON CONFLICT (observation_id) DO NOTHING
        """,
        (as_of, as_of, cutoff, as_of),
    )
    counts["fund_returns"] = max(result.rowcount, 0)
    result = conn.execute(
        """
        WITH eligible AS (
            SELECT DISTINCT m.ticker
            FROM sec_nport_holdings_v2_current h
            JOIN sec_cusip_ticker_map m ON m.cusip=h.cusip
            WHERE h.cusip ~ '^[A-Z0-9]{9}$' AND m.is_tradeable
              AND m.security_type NOT IN ('ETP','Open-End Fund','Closed-End Fund')
              AND upper(btrim(coalesce(h.source_typed_projection->>'ASSET_CAT',''))) IN ('EC','EP')
        )
        INSERT INTO mixed_quant_return_observation(
            observation_id,as_of,alias_type,alias_value,period_end,frequency,
            total_return,observed_at,source_lineage
        )
        SELECT md5('equity-return|'||%s::text||'|'||r.ticker||'|'||r.date::text)::uuid,
               %s,'ticker',r.ticker,r.date,'daily',r.return_1d,
               r.date::timestamp AT TIME ZONE 'UTC',
               jsonb_build_object('source_surface','stock_daily_returns','ticker',r.ticker)
        FROM stock_daily_returns r JOIN eligible e USING(ticker)
        WHERE r.return_1d IS NOT NULL AND r.date BETWEEN %s AND %s
        ON CONFLICT (observation_id) DO NOTHING
        """,
        (as_of, as_of, cutoff, as_of),
    )
    counts["equity_returns"] = max(result.rowcount, 0)

    if conn.execute("SELECT to_regclass('sec_nport_holdings_v2') IS NOT NULL").fetchone()[0]:
        result = conn.execute(
            """
            WITH source AS (
                SELECT h.*,h.publication_id AS nport_publication_id
                FROM sec_nport_holdings_v2_current h
                WHERE nullif(btrim(h.source_series_id),'') IS NOT NULL
                  AND h.report_date<=%s
            ), latest AS (
                SELECT source_series_id,max(report_date) AS report_date
                FROM source GROUP BY source_series_id
            ), grouped AS (
                SELECT s.nport_publication_id,s.source_series_id,s.report_date,
                       jsonb_agg(jsonb_build_object(
                           'cusip',s.cusip,'isin',s.isin,'issuer_name',s.issuer_name,
                           'asset_class',s.source_typed_projection->>'ASSET_CAT',
                           'sector',s.issuer_category,
                           'currency',s.source_typed_projection->>'CURRENCY_CODE',
                           'investment_country',s.source_typed_projection->>'INVESTMENT_COUNTRY',
                           'pct_of_nav',s.signed_pct_of_nav,'payoff_profile',s.payoff_profile
                       ) ORDER BY s.holding_id) AS holdings,
                       min(s.source_run_id::text)::uuid AS source_run_id
                FROM source s JOIN latest l USING(source_series_id,report_date)
                GROUP BY s.nport_publication_id,s.source_series_id,s.report_date
            )
            INSERT INTO mixed_quant_holding_observation(
                observation_id,as_of,series_id,report_date,holdings,source_lineage
            )
            SELECT md5('holdings|'||nport_publication_id::text||'|'||source_series_id||'|'||report_date::text)::uuid,
                   %s,source_series_id,report_date,holdings,
                   jsonb_build_object('source_surface','sec_nport_holdings_v2',
                                      'publication_id',nport_publication_id,
                                      'source_run_id',source_run_id)
            FROM grouped
            ON CONFLICT (observation_id) DO NOTHING
            """,
            (as_of, as_of),
        )
        counts["holding_snapshots"] = max(result.rowcount, 0)
    return counts


def _git_revision() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


def _resolve_as_of(conn: Any, calc_date: str | None) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    latest: date | None = None
    for table in _OBSERVATION_TABLES:
        row = conn.execute(f"SELECT max(as_of) FROM {table}").fetchone()
        if row and row[0] is not None:
            latest = row[0] if latest is None else max(latest, row[0])
    return latest


def _watermarks(conn: Any, as_of: date) -> dict[str, Any]:
    marks: dict[str, Any] = {}
    for table in _OBSERVATION_TABLES:
        marks[table] = conn.execute(
            f"SELECT count(*) FROM {table} WHERE as_of=%s", (as_of,)
        ).fetchone()[0]
    return marks


def _effective_deterministic_key(
    instrument_type: str, source_lineage: dict[str, Any], stored_key: str | None,
) -> str | None:
    """Keep fund share-class identities distinct even in legacy staged rows."""
    if (
        instrument_type == "fund"
        and source_lineage.get("source_surface") == "funds_v"
        and source_lineage.get("instrument_id")
    ):
        return f"fund:{source_lineage['instrument_id']}"
    return stored_key


def _load_identity_observations(conn: Any, as_of: date) -> list[IdentityObservation]:
    rows = conn.execute(
        "SELECT observation_id, instrument_type, currency, alias_type, alias_value, "
        "valid_from, valid_to, observed_at, source_lineage, issuer_id, security_id, deterministic_key "
        "FROM mixed_quant_identity_observation WHERE as_of=%s "
        "ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    return [
        IdentityObservation(
            observation_id=r[0], instrument_type=r[1], currency=r[2], alias_type=r[3],
            alias_value=r[4], valid_from=r[5], valid_to=r[6], observed_at=r[7],
            source_lineage=r[8], issuer_id=r[9], security_id=r[10],
            deterministic_key=_effective_deterministic_key(r[1], r[8], r[11]),
        )
        for r in rows
    ]


def _alias_index(resolved: list[ResolvedInstrument]) -> dict[tuple[str, str], list[uuid.UUID]]:
    index: dict[tuple[str, str], set[uuid.UUID]] = defaultdict(set)
    for inst in resolved:
        for alias in inst.aliases:
            index[(alias.alias_type, alias.alias_value)].add(inst.instrument_id)
    return {key: sorted(ids, key=str) for key, ids in index.items()}


def _unique_instrument(index: dict[tuple[str, str], list[uuid.UUID]], alias_type: str, alias_value: str) -> uuid.UUID | None:
    ids = index.get((alias_type, alias_value))
    # A collision (alias resolving to >1 unresolved instrument) is intentionally
    # left unattached rather than guessed at.
    return ids[0] if ids and len(ids) == 1 else None


def _with_public_fund_id(instrument: ResolvedInstrument) -> ResolvedInstrument:
    """Funds use the public app UUID accepted by the builder request contract."""
    if instrument.instrument_type != "fund":
        return instrument
    source_ids = {
        str(alias.source_lineage["instrument_id"])
        for alias in instrument.aliases
        if alias.source_lineage.get("source_surface") == "funds_v"
        and alias.source_lineage.get("instrument_id")
    }
    if len(source_ids) != 1:
        return instrument
    try:
        public_id = uuid.UUID(next(iter(source_ids)))
    except ValueError:
        return instrument
    return replace(instrument, instrument_id=public_id)


def _stage_identities(conn: Any, publication_id: uuid.UUID, as_of: date) -> list[ResolvedInstrument]:
    resolved = [
        _with_public_fund_id(instrument)
        for instrument in resolve_identities(_load_identity_observations(conn, as_of))
    ]
    if pub.get_checkpoint(conn, publication_id, "identities") is None:
        for inst in resolved:
            pub.write_instrument(conn, publication_id, inst)
        pub.record_checkpoint(conn, publication_id, "identities", {"instruments": len(resolved)})
    return resolved


def _stage_returns(conn: Any, publication_id: uuid.UUID, as_of: date, index: dict[tuple[str, str], list[uuid.UUID]]) -> None:
    if pub.get_checkpoint(conn, publication_id, "returns") is not None:
        return
    seen = conn.execute(
        "SELECT count(*) FROM mixed_quant_return_observation WHERE as_of=%s",
        (as_of,),
    ).fetchone()[0]
    result = conn.execute(
        """
        WITH unique_alias AS (
            SELECT alias_type,alias_value,min(instrument_id::text)::uuid AS instrument_id
            FROM quant_instrument_alias_v1
            WHERE publication_id=%s
            GROUP BY alias_type,alias_value
            HAVING count(DISTINCT instrument_id)=1
        )
        INSERT INTO quant_return_v1(
            publication_id,instrument_id,period_end,frequency,total_return,observed_at,source_lineage
        )
        SELECT %s,u.instrument_id,o.period_end,o.frequency,o.total_return,o.observed_at,o.source_lineage
        FROM mixed_quant_return_observation o
        JOIN unique_alias u USING(alias_type,alias_value)
        WHERE o.as_of=%s
        ON CONFLICT (publication_id,instrument_id,period_end,frequency) DO UPDATE SET
            total_return=EXCLUDED.total_return,
            observed_at=EXCLUDED.observed_at,
            source_lineage=EXCLUDED.source_lineage
        """,
        (publication_id, publication_id, as_of),
    )
    pub.record_checkpoint(
        conn, publication_id, "returns", {"written": max(result.rowcount, 0), "seen": seen}
    )


def _holdings_reader(conn: Any, as_of: date) -> tuple[Callable[[str], tuple[date, list[dict[str, Any]]] | None], list[str]]:
    rows = conn.execute(
        "SELECT series_id, report_date, holdings FROM mixed_quant_holding_observation WHERE as_of=%s",
        (as_of,),
    ).fetchall()
    table: dict[str, tuple[date, list[dict[str, Any]]]] = {r[0]: (r[1], list(r[2])) for r in rows}

    def get_holdings(series_id: str) -> tuple[date, list[dict[str, Any]]] | None:
        return table.get(series_id)

    return get_holdings, [r[0] for r in rows]


def _fund_ids_by_series(
    resolved: list[ResolvedInstrument],
) -> dict[str, list[uuid.UUID]]:
    mapping: dict[str, set[uuid.UUID]] = defaultdict(set)
    for instrument in resolved:
        if instrument.instrument_type != "fund":
            continue
        for alias in instrument.aliases:
            series_id = alias.source_lineage.get("series_id")
            if series_id:
                mapping[str(series_id)].add(instrument.instrument_id)
    return {
        series_id: sorted(instrument_ids, key=str)
        for series_id, instrument_ids in mapping.items()
    }


def _stage_exposures(conn: Any, publication_id: uuid.UUID, as_of: date, resolved: list[ResolvedInstrument]) -> None:
    if pub.get_checkpoint(conn, publication_id, "exposures") is not None:
        return
    fund_ids_by_series = _fund_ids_by_series(resolved)
    get_holdings, series_ids = _holdings_reader(conn, as_of)
    written = 0
    for series_id in series_ids:
        instrument_ids = fund_ids_by_series.get(series_id, [])
        if not instrument_ids:
            continue
        # No fund-of-funds edges are wired for the point-in-time snapshot yet;
        # expand_series still needs the alias buckets present to look children up.
        exposures, summary = expand_series(series_id, get_holdings, fund_map={"cusip": {}, "isin": {}})
        for instrument_id in instrument_ids:
            for row in _exposure_rows(exposures):
                factor = f"{row['dimension']}:{row['key']}"
                pub.write_exposure(
                    conn, publication_id, instrument_id,
                    factor=factor,
                    value=float(row["direct_pct"]) + float(row["indirect_pct"]),
                    method="nport_v2_lookthrough",
                    coverage={
                        "measurement_type": "observed",
                        "direct_pct": row["direct_pct"],
                        "indirect_pct": row["indirect_pct"],
                        "label": row["label"],
                        "report_date": summary["report_date"].isoformat(),
                    },
                    source_lineage={"engine": "expand_series", "series_id": series_id, "as_of": as_of.isoformat()},
                )
                written += 1
    pub.record_checkpoint(conn, publication_id, "exposures", {"written": written})


def _link_targets(holding: dict[str, Any]) -> list[tuple[str, str]]:
    """Alias lookups a direct holding may resolve to (skips synthetic ids)."""
    out: list[tuple[str, str]] = []
    cusip = (holding.get("cusip") or "").strip()
    isin = (holding.get("isin") or "").strip()
    if cusip and not cusip.upper().startswith(SYNTHETIC_PREFIXES):
        out.append(("cusip", cusip))
    if isin:
        out.append(("isin", isin))
    return out


def _stage_linkage(
    conn: Any,
    publication_id: uuid.UUID,
    as_of: date,
    resolved: list[ResolvedInstrument],
    index: dict[tuple[str, str], list[uuid.UUID]],
) -> None:
    """Link each fund's direct holdings to the security identities they resolve to.

    Only unambiguous resolutions (a holding alias resolving to exactly one
    instrument other than the fund itself) are linked; collisions and unresolved
    holdings are intentionally left unlinked rather than guessed at.
    """
    if pub.get_checkpoint(conn, publication_id, "linkage") is not None:
        return
    present = {inst.instrument_id for inst in resolved}
    fund_ids_by_series = _fund_ids_by_series(resolved)
    get_holdings, series_ids = _holdings_reader(conn, as_of)
    written = 0
    for series_id in series_ids:
        fund_ids = fund_ids_by_series.get(series_id, [])
        if not fund_ids:
            continue
        record = get_holdings(series_id)
        if record is None:
            continue
        report_date, holdings = record
        # Aggregate direct weight per resolved security (a security may appear
        # under several holding lines).
        links: dict[uuid.UUID, dict[str, Any]] = {}
        for holding in holdings:
            if holding.get("pct_of_nav") is None:
                continue
            for alias_type, alias_value in _link_targets(holding):
                security_id = _unique_instrument(index, alias_type, alias_value)
                if security_id is None or security_id not in present:
                    continue
                entry = links.setdefault(
                    security_id,
                    {"alias_type": alias_type, "alias_value": alias_value, "weight_pct": 0.0},
                )
                entry["weight_pct"] += float(holding["pct_of_nav"])
                break  # one resolution per holding line
        for fund_id in fund_ids:
            for security_id, entry in links.items():
                if security_id == fund_id:
                    continue
                pub.write_holding_link(
                    conn, publication_id, fund_id, security_id,
                    alias_type=entry["alias_type"], alias_value=entry["alias_value"],
                    weight_pct=entry["weight_pct"],
                    coverage={"resolution": "direct_security", "report_date": report_date.isoformat()},
                    source_lineage={"engine": "alias_resolution", "series_id": series_id, "as_of": as_of.isoformat()},
                )
                written += 1
    pub.record_checkpoint(conn, publication_id, "linkage", {"written": written})


def _stage_class_factors(
    conn: Any,
    publication_id: uuid.UUID,
    as_of: date,
    index: dict[tuple[str, str], list[uuid.UUID]],
) -> None:
    """Publish governed return-estimated class-factor exposures with evidence."""
    if pub.get_checkpoint(conn, publication_id, "class_factors") is not None:
        return
    rows = conn.execute(
        "SELECT alias_type, alias_value, factor, value, method, measurement_type, "
        "       quality_status, quality_flags, evidence, source_lineage "
        "FROM mixed_quant_class_factor_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    written = 0
    for (alias_type, alias_value, factor, value, method, measurement_type,
         quality_status, quality_flags, evidence, lineage) in rows:
        instrument_id = _unique_instrument(index, alias_type, alias_value)
        if instrument_id is None:
            continue
        clean = validate_class_factor_row({
            "factor": factor, "value": value, "method": method,
            "measurement_type": measurement_type, "quality_status": quality_status,
            "quality_flags": quality_flags, "evidence": evidence, "source_lineage": lineage,
        })
        pub.write_exposure(
            conn, publication_id, instrument_id,
            factor=f"class_factor:{clean['factor']}",
            value=clean["value"],
            method=clean["method"],
            coverage={
                "measurement_type": clean["measurement_type"],
                "quality_status": clean["quality_status"],
                "quality_flags": clean["quality_flags"],
                "evidence": clean["evidence"],
            },
            source_lineage=clean["source_lineage"],
        )
        written += 1
    pub.record_checkpoint(conn, publication_id, "class_factors", {"written": written, "seen": len(rows)})


def _stage_bond_factors(
    conn: Any,
    publication_id: uuid.UUID,
    as_of: date,
    resolved: list[ResolvedInstrument],
    index: dict[tuple[str, str], list[uuid.UUID]],
) -> None:
    """Publish named bond factors ONLY where observed; declare the rest absent.

    Every bond instrument gets an explicit coverage map over the five named
    factors: 'observed' where a value was published, 'absent' otherwise. Absent
    factors carry no exposure row (never a fabricated value).
    """
    if pub.get_checkpoint(conn, publication_id, "bond_factors") is not None:
        return
    bonds = {inst.instrument_id for inst in resolved if inst.instrument_type == "bond"}
    rows = conn.execute(
        "SELECT alias_type, alias_value, factor, value, method, source_lineage "
        "FROM mixed_quant_bond_factor_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    observed: dict[uuid.UUID, set[str]] = {}
    written = 0
    for alias_type, alias_value, factor, value, method, lineage in rows:
        instrument_id = _unique_instrument(index, alias_type, alias_value)
        if instrument_id is None or instrument_id not in bonds:
            continue
        clean = validate_bond_factor_row({
            "factor": factor, "value": value, "method": method, "source_lineage": lineage,
        })
        pub.write_exposure(
            conn, publication_id, instrument_id,
            factor=f"bond_factor:{clean['factor']}",
            value=clean["value"],
            method=clean["method"],
            coverage={"measurement_type": "observed", "named_factor": clean["factor"]},
            source_lineage=clean["source_lineage"],
        )
        observed.setdefault(instrument_id, set()).add(clean["factor"])
        written += 1
    # Declare coverage (observed/absent) for every bond instrument.
    for bond_id in bonds:
        seen = observed.get(bond_id, set())
        pub.merge_instrument_coverage(
            conn, publication_id, bond_id,
            {"bond_factor_coverage": {
                name: ("observed" if name in seen else "absent") for name in NAMED_BOND_FACTORS
            }},
        )
    pub.record_checkpoint(conn, publication_id, "bond_factors", {"written": written, "seen": len(rows)})


def _stage_income(conn: Any, publication_id: uuid.UUID, as_of: date, index: dict[tuple[str, str], list[uuid.UUID]]) -> None:
    if pub.get_checkpoint(conn, publication_id, "income") is not None:
        return
    rows = conn.execute(
        "SELECT alias_type, alias_value, event_date, cash_amount, currency, event_type, source_lineage "
        "FROM mixed_quant_income_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    written = 0
    for alias_type, alias_value, event_date, cash_amount, currency, event_type, lineage in rows:
        instrument_id = _unique_instrument(index, alias_type, alias_value)
        if instrument_id is None:
            continue
        pub.write_income(conn, publication_id, instrument_id, {
            "event_date": event_date, "cash_amount": cash_amount, "currency": currency,
            "event_type": event_type, "source_lineage": lineage,
        })
        written += 1
    pub.record_checkpoint(conn, publication_id, "income", {"written": written, "seen": len(rows)})


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    """Build one inactive mixed_quant_v1 publication for ``calc_date`` (or the
    latest observed as_of). Idempotent and restartable; does not promote."""
    product = PRODUCT
    code_revision = os.getenv("CODE_REVISION") or _git_revision() or "dev"
    config_version = os.getenv("MIXED_QUANT_CONFIG_VERSION", "v1")

    with connect(resolve_dsn(dsn)) as conn:
        pub.install_schema(conn)
        requested_as_of = date.fromisoformat(calc_date) if calc_date else date.today()
        if os.getenv("MIXED_QUANT_REUSE_NATIVE_OBSERVATIONS", "").lower() == "true":
            populated = {"reused_existing_observations": 1}
        else:
            populated = _populate_native_observations(conn, requested_as_of)
        conn.commit()
        with advisory_lock(conn, LOCK_MIXED_QUANT_PUBLICATION) as got:
            if not got:
                return {"status": "locked", "product": product}
            as_of = _resolve_as_of(conn, calc_date)
            if as_of is None:
                return {"status": "no_observations", "product": product}
            publication_id = pub.open_publication(
                conn, product=product, as_of=as_of, code_revision=code_revision,
                config_version=config_version, input_watermarks=_watermarks(conn, as_of),
            )
            if os.getenv("MIXED_QUANT_REBUILD_HOLDINGS_STAGES", "").lower() == "true":
                conn.execute(
                    "DELETE FROM quant_publication_checkpoint_v1 "
                    "WHERE publication_id=%s AND stage IN ('exposures','linkage')",
                    (publication_id,),
                )
                conn.execute(
                    "DELETE FROM quant_exposure_v1 "
                    "WHERE publication_id=%s AND method='nport_v2_lookthrough'",
                    (publication_id,),
                )
                conn.execute(
                    "DELETE FROM quant_holding_link_v1 WHERE publication_id=%s",
                    (publication_id,),
                )
            conn.commit()

            resolved = _stage_identities(conn, publication_id, as_of)
            conn.commit()
            index = _alias_index(resolved)
            _stage_returns(conn, publication_id, as_of, index)
            conn.commit()
            _stage_exposures(conn, publication_id, as_of, resolved)
            conn.commit()
            _stage_linkage(conn, publication_id, as_of, resolved, index)
            conn.commit()
            _stage_class_factors(conn, publication_id, as_of, index)
            conn.commit()
            _stage_bond_factors(conn, publication_id, as_of, resolved, index)
            conn.commit()
            _stage_income(conn, publication_id, as_of, index)
            conn.commit()

            counts = pub.count_rows(conn, publication_id)
            pub.mark_ready(conn, publication_id, counts)
            conn.commit()

    return {
        "status": "ready", "product": product, "as_of": as_of.isoformat(),
        "publication_id": str(publication_id), "code_revision": code_revision,
        "config_version": config_version, "populated": populated, **counts,
    }
