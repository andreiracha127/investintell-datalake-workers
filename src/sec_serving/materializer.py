"""Public-only serving materializer for ``sec_regulatory_serving_v1``.

Reads the current N-CEN/RR1 snapshot views (``sec_current_*``) and projects ONLY
public columns into ``sec_regulatory_serving_facts`` under one atomically promoted
derived publication.  Internal provenance, ``source_run_id``, ``registrant_cik``,
raw row ids, ``source_table`` and narrative hashes never reach the serving surface:
every family payload is passed through ``sec_serving_scrub`` (a recursive key-strip)
and each projection lists only the family's public columns.

State mapping (documented in ``contract.FAMILIES``):
  * RR1 families already carry the 4-state ``status`` -> passed through.
  * N-CEN families carry a 3-state ``*_state`` -> mapped to the 4-state serving
    vocabulary, with ``degraded`` computed where a forward-note requires it
    (etf net-flow leg coercion; expense all-null legs).
  * The crosswalk family emits ONLY approved + high-confidence mappings, and its
    public evidence is ``canonical_concept``/``crosswalk_version``/``confidence`` --
    never the internal custom tag (born empty -> zero rows).
"""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

import psycopg
from psycopg.types.json import Json

from src.sec_serving import contract

ROOT = Path(__file__).resolve().parents[2]

_NAMESPACE = UUID("9e2b7a54-1c3d-5e6f-8a90-1b2c3d4e5f60")


class ServingFamilyCoverageError(RuntimeError):
    """Raised when a serving build would promote a PARTIAL family surface.

    The serving publication is one complete, atomically promoted surface: every
    contract-declared family whose source snapshot is missing would silently drop
    that family from the served publication. Failing closed here prevents a partial
    promotion; ``materialize(..., allow_missing_families=True)`` opts out for tests
    that deliberately exercise a subset of families.
    """

_SCHEMA_FILES = (
    "sec_derived_publications.sql",
    "sec_regulatory_serving.sql",
)

_COLUMNS = (
    "publication_id, family, series_id, class_id, fund_id, fact_key, grain_origin, "
    "state, reason_code, snapshot_reason_code, coverage_pct, source_date, "
    "accession_number, document_id, filing_date, effective_date, payload"
)

# Reason code derived deterministically from the serving state.
_REASON = (
    "CASE srv_state WHEN 'available' THEN NULL "
    "WHEN 'degraded' THEN 'coverage_below_certified_threshold' "
    "WHEN 'not_applicable' THEN 'asset_family_not_applicable' "
    "ELSE 'source_filing_unavailable' END"
)
_COVERAGE = (
    "CASE srv_state WHEN 'available' THEN 100 WHEN 'degraded' THEN 50 ELSE NULL END"
)
# 3-state N-CEN family state -> 4-state serving state. ``degraded`` is a complete
# ``WHEN <cond> THEN 'degraded' `` clause (or empty) evaluated BEFORE 'available'.
_NCEN_STATE = (
    "CASE {degraded}WHEN {state_col}='available' THEN 'available' "
    "WHEN {state_col}='not_applicable' THEN 'not_applicable' ELSE 'unavailable' END"
)


def _ncen_fund_sql(
    family: str, view: str, state_col: str, reason_col: str,
    payload_build: str, *, degraded: str = "",
) -> str:
    srv_state = _NCEN_STATE.format(state_col=state_col, degraded=degraded)
    return f"""
    INSERT INTO sec_regulatory_serving_facts ({_COLUMNS})
    SELECT %(pub)s, '{family}', COALESCE(series_id,''), '', fund_id, '', 'fund',
           srv_state, {_REASON}, {reason_col}, {_COVERAGE}, measured_at,
           accession_number, '', NULL, effective_date,
           CASE WHEN srv_state IN ('available','degraded')
                THEN sec_serving_scrub({payload_build}) ELSE NULL END
    FROM (
        SELECT s.*, ({srv_state}) AS srv_state
        FROM {view} s
    ) s
    ON CONFLICT DO NOTHING
    """


# Natural/numeric crosswalk-version ordering (v2 < v10): rank by the digit run, with
# a lexical tiebreak, so the HIGHEST approved version wins. Mirrors the SQL resolver
# ``rr1_crosswalk_resolve`` (schemas/rr1_custom_tag_crosswalk.sql).
_CROSSWALK_VERSION_ORDER = (
    "NULLIF(regexp_replace(x.crosswalk_version, '[^0-9]', '', 'g'), '')::numeric DESC NULLS LAST, "
    "x.crosswalk_version DESC"
)

# forward-notes 12 & 15: the confidence-gated crosswalk evidence for a fact resolved
# from a custom (non-canonical) tag. Public evidence is canonical_concept +
# crosswalk_version + confidence ONLY -- NEVER the internal custom/original tag name.
# Approved + confidence>=threshold, highest crosswalk_version. Joins on the fact's
# preserved (original_tag, original_version) sidecar for the SAME canonical_concept.
_CROSSWALK_EVIDENCE_JOIN = f"""
    LEFT JOIN LATERAL (
        SELECT jsonb_build_object(
                   'canonical_concept', x.canonical_concept,
                   'crosswalk_version', x.crosswalk_version,
                   'confidence', x.confidence) AS evidence
        FROM rr1_custom_tag_crosswalk x
        WHERE x.custom_tag = s.original_tag
          AND x.custom_version = s.original_version
          AND x.canonical_concept = s.canonical_concept
          AND x.review_status = 'approved'
          AND x.confidence >= %(min_conf)s
        ORDER BY {_CROSSWALK_VERSION_ORDER}
        LIMIT 1
    ) cw ON true
"""


def _rr1_fact_sql(
    family: str, view: str, payload_build: str, fact_key: str, *,
    accession: str = "accession_number", filing: str = "filed_date",
    source_date: str = "data_date", grain_origin: str = "class",
    class_id: str = "COALESCE(class_id,'')", series_id: str = "COALESCE(series_id,'')",
    document: str = "COALESCE(document_id,'')",
    crosswalk_evidence: bool = False,
) -> str:
    # RR1 status is already the 4-state serving vocabulary -> pass through.
    if crosswalk_evidence:
        payload_expr = (
            f"({payload_build} || CASE WHEN cw.evidence IS NOT NULL "
            "THEN jsonb_build_object('crosswalk_evidence', cw.evidence) "
            "ELSE '{}'::jsonb END)"
        )
        join_clause = _CROSSWALK_EVIDENCE_JOIN
    else:
        payload_expr = payload_build
        join_clause = ""
    return f"""
    INSERT INTO sec_regulatory_serving_facts ({_COLUMNS})
    SELECT %(pub)s, '{family}', {series_id}, {class_id}, '', {fact_key}, '{grain_origin}',
           srv_state, {_REASON}, reason_code, {_COVERAGE}, {source_date},
           {accession}, {document}, {filing}, effective_date,
           CASE WHEN srv_state IN ('available','degraded')
                THEN sec_serving_scrub({payload_expr}) ELSE NULL END
    FROM (SELECT s.*, status AS srv_state FROM {view} s) s{join_clause}
    ON CONFLICT DO NOTHING
    """


def _family_sql() -> dict[str, str]:
    """Per-family public projection SQL (publication_id bound as %(pub)s)."""
    sql: dict[str, str] = {}

    sql["ncen_structure"] = _ncen_fund_sql(
        "ncen_structure", "sec_current_ncen_structure_profiles",
        "structure_state", "structure_reason_code",
        "jsonb_build_object('structure_flags', s.structure_flags, "
        "'regulatory_reliance', s.regulatory_reliance, "
        "'report_period_lt_12month', s.report_period_lt_12month, "
        "'reliance_state', s.reliance_state, 'reliance_reason_code', s.reliance_reason_code)",
    )
    sql["ncen_provider_network"] = _ncen_fund_sql(
        "ncen_provider_network", "sec_current_ncen_provider_network_profiles",
        "provider_network_state", "provider_network_reason_code", "s.provider_network",
    )
    sql["ncen_liquidity_backstop"] = _ncen_fund_sql(
        "ncen_liquidity_backstop", "sec_current_ncen_liquidity_backstop_profiles",
        "liquidity_backstop_state", "liquidity_backstop_reason_code", "s.liquidity_backstop",
    )
    # forward-note 6: surface the frozen IS_COLLATERAL_LIQUIDATED contract defect
    # as a reduced-quality flag; never silently repaired.
    sql["ncen_securities_lending"] = _ncen_fund_sql(
        "ncen_securities_lending", "sec_current_ncen_securities_lending_profiles",
        "securities_lending_state", "securities_lending_reason_code",
        "(s.securities_lending || jsonb_build_object('quality_flags', "
        "jsonb_build_array('collateral_liquidated_field_contract_defect')))",
    )
    sql["ncen_closed_end"] = _ncen_fund_sql(
        "ncen_closed_end", "sec_current_ncen_closed_end_profiles",
        "closed_end_state", "closed_end_reason_code", "s.closed_end",
    )
    # forward-note 8: the snapshot state cannot tell an empty fund from a reported
    # one -> degrade when every expense leg is NULL.
    expense_degraded = (
        "WHEN expense_brokerage_state='available' "
        "AND s.expense_brokerage#>>'{expenses,management_fee}' IS NULL "
        "AND s.expense_brokerage#>>'{expenses,net_operating_expenses}' IS NULL THEN 'degraded' "
    )
    sql["ncen_expense_brokerage"] = _ncen_fund_sql(
        "ncen_expense_brokerage", "sec_current_ncen_expense_brokerage_profiles",
        "expense_brokerage_state", "expense_brokerage_reason_code", "s.expense_brokerage",
        degraded=expense_degraded,
    )
    # forward-note 4: a net flow computed from two PRESENT legs is legitimate and is
    # served as-is; degrade ONLY when the snapshot flagged an incomplete leg (>=1 AP
    # row carried a single leg), and in that case never serve the untrustworthy net.
    etf_degraded = (
        "WHEN etf_primary_market_state='available' "
        "AND (s.etf_primary_market#>>'{derived,leg_incomplete}')::boolean THEN 'degraded' "
    )
    sql["ncen_etf_primary_market"] = _ncen_fund_sql(
        "ncen_etf_primary_market", "sec_current_ncen_etf_primary_market_profiles",
        "etf_primary_market_state", "etf_primary_market_reason_code",
        "(CASE WHEN (s.etf_primary_market#>>'{derived,leg_incomplete}')::boolean "
        "THEN s.etf_primary_market #- '{derived,net_primary_market_flow}' "
        "ELSE s.etf_primary_market END)",
        degraded=etf_degraded,
    )

    # forward-note 2: operational events are registrant grain -> fan out to every
    # fund of the same accession via the structure roster; label grain_origin.
    oe_state = _NCEN_STATE.format(state_col="o.operational_event_state", degraded="")
    sql["ncen_operational_event"] = f"""
    INSERT INTO sec_regulatory_serving_facts ({_COLUMNS})
    SELECT %(pub)s, 'ncen_operational_event', COALESCE(roster.series_id,''), '',
           roster.fund_id, '', 'registrant', srv_state, {_REASON},
           oe.operational_event_reason_code, {_COVERAGE}, oe.measured_at,
           oe.accession_number, '', NULL, oe.effective_date,
           CASE WHEN srv_state IN ('available','degraded')
                THEN sec_serving_scrub(oe.operational_events) ELSE NULL END
    FROM (SELECT o.*, ({oe_state}) AS srv_state
          FROM sec_current_ncen_operational_event_profiles o) oe
    JOIN (SELECT DISTINCT accession_number, fund_id, series_id
          FROM sec_current_ncen_structure_profiles) roster
      ON roster.accession_number = oe.accession_number
    ON CONFLICT DO NOTHING
    """

    # ---- RR1 fact families (4-state status pass-through) --------------------
    sql["rr1_fee"] = _rr1_fact_sql(
        "rr1_fee", "sec_current_rr1_fee_profiles",
        "jsonb_build_object('canonical_concept', s.canonical_concept, "
        "'value_numeric', s.value_numeric, 'declared_unit', 'fraction')",
        "concat_ws('|', s.canonical_concept, s.measure_id, s.document_id, s.dimensions, "
        "s.occurrence, s.data_date::text)",
        crosswalk_evidence=True,
    )
    sql["rr1_shareholder_cost"] = _rr1_fact_sql(
        "rr1_shareholder_cost", "sec_current_rr1_shareholder_cost_profiles",
        "jsonb_build_object('canonical_concept', s.canonical_concept, 'cost_group', s.cost_group, "
        "'value_numeric', s.value_numeric, 'declared_unit', s.declared_unit)",
        "concat_ws('|', s.canonical_concept, s.measure_id, s.document_id, s.dimensions, "
        "s.occurrence, s.data_date::text)",
    )
    # forward-note 10: reconciliation divergence is a quality flag, never adjusted.
    sql["rr1_waiver"] = _rr1_fact_sql(
        "rr1_waiver", "sec_current_rr1_waiver_profiles",
        "jsonb_build_object('waiver_over_assets', s.waiver_over_assets, "
        "'gross_expense_over_assets', s.gross_expense_over_assets, "
        "'net_expense_over_assets', s.net_expense_over_assets, 'declared_unit', s.declared_unit, "
        "'termination_date', s.termination_date, 'term_days', s.term_days, "
        "'remaining_days', s.remaining_days, 'gross_minus_waiver', s.gross_minus_waiver, "
        "'net_reconstruction_gap', s.net_reconstruction_gap, "
        "'reconciliation_status', s.reconciliation_status, "
        "'reconciliation_tolerance', s.reconciliation_tolerance, "
        "'cliff_horizon_days', s.cliff_horizon_days, 'cliff_flag', s.cliff_flag, "
        "'termination_reason_code', s.termination_reason_code)",
        "concat_ws('|', s.measure_id, s.document_id, s.dimensions, s.occurrence, s.data_date::text)",
    )
    # forward-note 13: number + consistency flag only; never the narrative text.
    sql["rr1_turnover"] = _rr1_fact_sql(
        "rr1_turnover", "sec_current_rr1_turnover_profiles",
        "jsonb_build_object('turnover_rate', s.turnover_rate, 'declared_unit', s.declared_unit, "
        "'turnover_numeric_present', s.turnover_numeric_present, "
        "'turnover_text_present', s.turnover_text_present, "
        "'narrative_consistency', s.narrative_consistency)",
        "concat_ws('|', s.measure_id, s.document_id, s.dimensions, s.occurrence, s.data_date::text)",
    )
    # forward-notes 11 & 16: treatment carries the load/tax signal; no fabricated bool.
    sql["rr1_reported_performance"] = _rr1_fact_sql(
        "rr1_reported_performance", "sec_current_rr1_reported_performance_profiles",
        "jsonb_build_object('canonical_concept', s.canonical_concept, 'value_kind', s.value_kind, "
        "'value_numeric', s.value_numeric, 'value_date', s.value_date, "
        "'value_label', s.value_label, 'declared_unit', s.declared_unit, 'treatment', s.treatment)",
        "concat_ws('|', s.canonical_concept, s.measure_id, s.document_id, s.dimensions, "
        "s.occurrence, s.data_date::text)",
        crosswalk_evidence=True,
    )
    # forward-note 9: use post-rename names; numeric_class_count is never the class count.
    sql["rr1_class_cost_dispersion"] = f"""
    INSERT INTO sec_regulatory_serving_facts ({_COLUMNS})
    SELECT %(pub)s, 'rr1_class_cost_dispersion', COALESCE(series_id,''), '', '', '', 'series',
           srv_state, {_REASON}, reason_code, {_COVERAGE}, data_date,
           accession_number, '', filed_date, effective_date,
           CASE WHEN srv_state IN ('available','degraded') THEN sec_serving_scrub(
               jsonb_build_object('numeric_class_count', s.numeric_class_count,
                   'class_total', s.class_total, 'net_min', s.net_min, 'net_max', s.net_max,
                   'net_spread', s.net_spread, 'net_min_class_id', s.net_min_class_id,
                   'net_max_class_id', s.net_max_class_id,
                   'per_class_evidence', s.per_class_evidence)) ELSE NULL END
    FROM (SELECT s.*, status AS srv_state FROM sec_current_rr1_class_cost_dispersion s) s
    ON CONFLICT DO NOTHING
    """
    sql["rr1_benchmark"] = f"""
    INSERT INTO sec_regulatory_serving_facts ({_COLUMNS})
    SELECT %(pub)s, 'rr1_benchmark', COALESCE(series_id,''), COALESCE(class_id,''), '', '', 'class',
           srv_state, {_REASON}, reason_code, {_COVERAGE}, latest_effective_date,
           latest_accession_number, '', latest_filed_date, latest_effective_date,
           CASE WHEN srv_state IN ('available','degraded') THEN sec_serving_scrub(
               jsonb_build_object('primary_benchmark', s.primary_benchmark,
                   'benchmark_consistency', s.benchmark_consistency,
                   'declared_benchmark_count', s.declared_benchmark_count,
                   'observation_count', s.observation_count, 'context_count', s.context_count,
                   'document_count', s.document_count, 'period_count', s.period_count,
                   'per_benchmark_evidence', s.per_benchmark_evidence)) ELSE NULL END
    FROM (SELECT s.*, status AS srv_state FROM sec_current_rr1_benchmark_profiles s) s
    ON CONFLICT DO NOTHING
    """

    # forward-notes 12 & 15: only approved + confidence>=threshold; public evidence
    # is canonical_concept + crosswalk_version + confidence (never the custom tag).
    sql["rr1_custom_tag_crosswalk"] = f"""
    INSERT INTO sec_regulatory_serving_facts ({_COLUMNS})
    SELECT %(pub)s, 'rr1_custom_tag_crosswalk', '', '', '',
           concat_ws('|', canonical_concept, crosswalk_version), 'crosswalk',
           'available', NULL, review_status, 100, NULL, '', '', NULL, NULL,
           sec_serving_scrub(jsonb_build_object('canonical_concept', canonical_concept,
               'crosswalk_version', crosswalk_version, 'confidence', confidence))
    FROM rr1_custom_tag_crosswalk
    WHERE review_status='approved' AND confidence >= %(min_conf)s
    ON CONFLICT DO NOTHING
    """
    return sql


_FAMILY_SQL = _family_sql()


def install_schema(conn: psycopg.Connection) -> None:
    """Apply the serving DDL idempotently (family ``sec_current_*`` must pre-exist)."""
    with conn.cursor() as cur:
        for name in _SCHEMA_FILES:
            cur.execute((ROOT / "schemas" / name).read_text(encoding="utf-8"))


def publication_id_for(as_of: date, code_revision: str) -> UUID:
    return uuid5(_NAMESPACE, f"{contract.SERVING_PRODUCT}|{as_of.isoformat()}|{code_revision}")


def _relation_exists(conn: psycopg.Connection, name: str) -> bool:
    return conn.execute("SELECT to_regclass(%s) IS NOT NULL", (name,)).fetchone()[0]


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
        raise RuntimeError("serving publication requires a validated source run/package anchor")
    return row[0], row[1]


def materialize(
    conn: psycopg.Connection,
    *,
    as_of: date,
    code_revision: str,
    source_run_id: UUID | None = None,
    source_package_id: UUID | None = None,
    allow_missing_families: bool = False,
) -> dict[str, Any]:
    """Prepare -> project every present family -> validate -> current, atomically.

    Fails closed (``ServingFamilyCoverageError``) and promotes NOTHING when any
    contract-declared family's ``source_view`` is missing, so a partial surface can
    never be silently promoted. Pass ``allow_missing_families=True`` only for tests
    that deliberately seed a subset of the family snapshots.
    """
    publication_id = publication_id_for(as_of, code_revision)
    product = contract.SERVING_PRODUCT

    existing = conn.execute(
        "SELECT lifecycle_state FROM sec_derived_publications WHERE publication_id=%s",
        (publication_id,),
    ).fetchone()

    families_written: list[str] = []
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
        consumed: dict[str, str] = {}
        for family in contract.FAMILIES:
            view = family["source_view"]
            if not _relation_exists(conn, view):
                continue
            conn.execute(
                _FAMILY_SQL[family["family"]],
                {"pub": publication_id, "min_conf": contract.CROSSWALK_MIN_CONFIDENCE},
            )
            families_written.append(family["family"])
            pub_row = conn.execute(
                "SELECT publication_id FROM sec_derived_current_pointers WHERE product=%s",
                (family["source_product"],),
            ).fetchone()
            if pub_row is not None:
                consumed[family["family"]] = str(pub_row[0])
        missing_families = [f for f in contract.family_names() if f not in set(families_written)]
        if missing_families and not allow_missing_families:
            # Fail closed BEFORE the build pin, validation and current-pointer flip:
            # nothing is validated and nothing is promoted for a partial surface.
            raise ServingFamilyCoverageError(
                "serving build would promote a partial surface; missing family source "
                f"views {missing_families}"
            )
        conn.execute(
            "INSERT INTO sec_regulatory_serving_builds"
            "(publication_id,as_of_date,input_fingerprint,consumed_family_publications)"
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
        "SELECT count(*) FROM sec_regulatory_serving_facts WHERE publication_id=%s",
        (publication_id,),
    ).fetchone()[0]
    return {
        "product": product,
        "publication_id": str(publication_id),
        "families_written": families_written,
        "rows": row_count,
        "state": "current",
    }
