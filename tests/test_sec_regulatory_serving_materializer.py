"""Focused tests for the public ``sec_regulatory_serving_v1`` materializer.

Uses synthetic ``sec_current_*`` snapshot stand-ins so the serving projection is
exercised independently of the family build machinery. Proves:
  * every present family projects into the public serving surface;
  * the 3-state N-CEN -> 4-state serving mapping incl. computed ``degraded``
    (etf net-flow leg coercion, expense all-null legs);
  * RR1 4-state ``status`` passes through;
  * the registrant->fund fan-out (forward-note 2);
  * the crosswalk confidence gate (forward-note 12);
  * DATA leak absence (forward-notes 1 & 14): no internal filenames, raw_row_id,
    source_run_id, hashes, vendor names, or ``cik:`` / ``row:`` identifiers.

DSN-agnostic (Global Constraint 9): reads ``SEC_TEST_DATABASE_URL``.
"""
from __future__ import annotations

import json
import os
from datetime import date
from uuid import uuid4

import psycopg
import pytest

from src.sec_serving import contract, materializer

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

# Internal-identifier sentinels embedded in every synthetic payload/provenance.
SENT_RAW = "RAWROWLEAK123"
SENT_SRC = "SRCRUNLEAK456"
SENT_MD5 = "d41d8cd98f00b204e9800998ecf8427e"
SENT_CIK = "cik:LEAKCIK789"
SENT_ROW = "row:ROWKEYLEAK000"
SENT_FILE = "SUBMISSION_INTERNAL.tsv"
SENT_VENDOR = "InternalVendorNameX"
FORBIDDEN = [
    SENT_RAW, SENT_SRC, SENT_MD5, "LEAKCIK789", "ROWKEYLEAK000", SENT_FILE, SENT_VENDOR,
    "raw_row_id", "source_run_id", "ingestion_run_id", "text_block_md5", "provenance",
]

# A nested internal blob attached to every family payload; the scrub must remove
# every blocklisted KEY and neutralise the ``row:`` / ``cik:`` entity VALUES that
# survive under a public (non-blocklisted) key.  ``issuer_ref`` carries a ``cik:``
# value under a public key: its KEY is not blocked, so only the value rule can
# neutralise it (item 1a).
LEAK_BLOB = {
    "evidence": {
        "raw_row_id": SENT_RAW,
        "source_run_id": SENT_SRC,
        "text_block_md5": SENT_MD5,
        "registrant_cik": SENT_CIK,
        "fund_raw_row_id": SENT_RAW,
        "nested": {"submission_raw_row_id": SENT_RAW, "entity_key": SENT_ROW,
                   "issuer_ref": SENT_CIK},
    }
}
# A full internal provenance column (must never be projected at all).
PROV = json.dumps({
    "source_run_id": SENT_SRC, "raw_row_id": SENT_RAW, "text_block_md5": SENT_MD5,
    "fund_source_table": SENT_FILE, "vendor": SENT_VENDOR, "registrant_cik": SENT_CIK,
})


def _blob(**public: object) -> str:
    return json.dumps({**public, **LEAK_BLOB})


def _connect() -> psycopg.Connection:
    return psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"])


def _setup(cur) -> tuple[str, object, object]:
    schema = f"sec_serving_{uuid4().hex}"
    run_id, package_id = uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    materializer.install_schema(cur.connection)
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
    _synthetic_snapshots(cur)
    return schema, run_id, package_id


def _synthetic_snapshots(cur) -> None:
    """Create ``sec_current_*`` stand-ins with public columns + embedded leaks."""
    # --- N-CEN fund families (single state + jsonb payload) -----------------
    def ncen_fund(view: str, state_col: str, reason_col: str, payload_col: str, extra_cols: str = ""):
        cur.execute(f"""
            CREATE TABLE {view}(
                series_id text, fund_id text, accession_number text,
                measured_at date, effective_date date,
                {state_col} text, {reason_col} text, {payload_col} jsonb,
                provenance jsonb, coverage jsonb {extra_cols})
        """)

    ncen_fund("sec_current_ncen_structure_profiles", "structure_state", "structure_reason_code",
              "structure_flags",
              ", regulatory_reliance jsonb, report_period_lt_12month text, "
              "reliance_state text, reliance_reason_code text")
    cur.execute(
        "INSERT INTO sec_current_ncen_structure_profiles VALUES "
        "('S1','F1','ACC-1','2025-12-31','2025-12-31','available',NULL,%s,%s,'{}',"
        "%s,'true','available',NULL)",
        (_blob(is_etf="true"), PROV, json.dumps({"reliance": "6c-11"})),
    )
    ncen_fund("sec_current_ncen_provider_network_profiles", "provider_network_state",
              "provider_network_reason_code", "provider_network")
    cur.execute(
        "INSERT INTO sec_current_ncen_provider_network_profiles VALUES "
        "('S1','F1','ACC-1','2025-12-31','2025-12-31','available',NULL,%s,%s,'{}')",
        (_blob(adviser={"affiliated": "true"}), PROV),
    )
    ncen_fund("sec_current_ncen_liquidity_backstop_profiles", "liquidity_backstop_state",
              "liquidity_backstop_reason_code", "liquidity_backstop")
    cur.execute(
        "INSERT INTO sec_current_ncen_liquidity_backstop_profiles VALUES "
        "('S1','F1','ACC-1','2025-12-31','2025-12-31','available',NULL,%s,%s,'{}')",
        (_blob(line_of_credit={"loc_state": "available", "interfund_state": "not_applicable"}), PROV),
    )
    ncen_fund("sec_current_ncen_securities_lending_profiles", "securities_lending_state",
              "securities_lending_reason_code", "securities_lending")
    cur.execute(
        "INSERT INTO sec_current_ncen_securities_lending_profiles VALUES "
        "('S1','F1','ACC-1','2025-12-31','2025-12-31','available',NULL,%s,%s,'{}')",
        (_blob(authorized="true"), PROV),
    )
    ncen_fund("sec_current_ncen_closed_end_profiles", "closed_end_state",
              "closed_end_reason_code", "closed_end")
    cur.execute(
        "INSERT INTO sec_current_ncen_closed_end_profiles VALUES "
        "('S1','F1','ACC-1','2025-12-31','2025-12-31','not_applicable','fund_is_not_closed_end',"
        "NULL,%s,'{}')",
        (PROV,),
    )
    ncen_fund("sec_current_ncen_expense_brokerage_profiles", "expense_brokerage_state",
              "expense_brokerage_reason_code", "expense_brokerage")
    # available BUT all expense legs NULL -> forward-note 8 -> degraded.
    cur.execute(
        "INSERT INTO sec_current_ncen_expense_brokerage_profiles VALUES "
        "('S1','F1','ACC-1','2025-12-31','2025-12-31','available',NULL,%s,%s,'{}')",
        (_blob(expenses={"management_fee": None, "net_operating_expenses": None}), PROV),
    )
    ncen_fund("sec_current_ncen_etf_primary_market_profiles", "etf_primary_market_state",
              "etf_primary_market_reason_code", "etf_primary_market")
    # both legs present (leg_incomplete false) -> forward-note 4 -> serve the net as-is.
    etf_payload = {
        "authorized_participants": [{"entity_key": SENT_ROW, "purchase_value": 10}],
        "derived": {"net_primary_market_flow": 4000000, "leg_incomplete": False,
                    "authorized_participant_count": 1},
        **LEAK_BLOB,
    }
    cur.execute(
        "INSERT INTO sec_current_ncen_etf_primary_market_profiles VALUES "
        "('S1','F1','ACC-1','2025-12-31','2025-12-31','available',NULL,%s,%s,'{}')",
        (json.dumps(etf_payload), PROV),
    )
    # operational events: registrant grain, fanned out to funds of ACC-1.
    cur.execute("""
        CREATE TABLE sec_current_ncen_operational_event_profiles(
            registrant_cik text, accession_number text, measured_at date, effective_date date,
            operational_event_state text, operational_event_reason_code text,
            operational_events jsonb, provenance jsonb, coverage jsonb)
    """)
    oe_payload = {"detail_evidence": [{"raw_row_id": SENT_RAW, "evidence": {"cco_change": "true"}}], **LEAK_BLOB}
    cur.execute(
        "INSERT INTO sec_current_ncen_operational_event_profiles VALUES "
        "(%s,'ACC-1','2025-12-31','2025-12-31','available',NULL,%s,%s,'{}')",
        (SENT_CIK, json.dumps(oe_payload), PROV),
    )

    # --- RR1 fact families (4-state status) ---------------------------------
    def rr1_fact(view: str, value_cols: str):
        cur.execute(f"""
            CREATE TABLE {view}(
                series_id text, class_id text, accession_number text,
                data_date date, effective_date date, filed_date date,
                document_id text, measure_id text, dimensions text, occurrence text,
                status text, reason_code text, provenance jsonb, coverage jsonb {value_cols})
        """)

    # original_tag/original_version mirror the real fee view (rr taxonomy tag + version);
    # these RR-namespaced sidecars never match a custom-tag crosswalk row (Constraint 5).
    rr1_fact("sec_current_rr1_fee_profiles",
             ", canonical_concept text, value_numeric numeric, original_tag text, original_version text")
    cur.execute(
        "INSERT INTO sec_current_rr1_fee_profiles VALUES "
        "('S1','C1','ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
        "'available',NULL,%s,'{}','net_expense',0.0075,'ExpensesOverAssets','rr/2023')",
        (PROV,),
    )
    cur.execute(
        "INSERT INTO sec_current_rr1_fee_profiles VALUES "
        "('S1','C2','ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
        "'unavailable','source_filing_unavailable',%s,'{}','net_expense',NULL,NULL,NULL)",
        (PROV,),
    )
    rr1_fact("sec_current_rr1_shareholder_cost_profiles",
             ", canonical_concept text, cost_group text, value_numeric numeric, declared_unit text")
    cur.execute(
        "INSERT INTO sec_current_rr1_shareholder_cost_profiles VALUES "
        "('S1','C1','ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
        "'available',NULL,%s,'{}','redemption_fee','shareholder_fee',0.02,'fraction')",
        (PROV,),
    )
    rr1_fact("sec_current_rr1_waiver_profiles",
             ", waiver_over_assets numeric, gross_expense_over_assets numeric, "
             "net_expense_over_assets numeric, declared_unit text, termination_date date, "
             "term_days integer, remaining_days integer, gross_minus_waiver numeric, "
             "net_reconstruction_gap numeric, reconciliation_status text, "
             "reconciliation_tolerance numeric, cliff_horizon_days integer, cliff_flag boolean, "
             "termination_reason_code text")
    cur.execute(
        "INSERT INTO sec_current_rr1_waiver_profiles VALUES "
        "('S1','C1','ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
        "'degraded','coverage_below_certified_threshold',%s,'{}',"
        "0.001,0.009,0.008,'fraction','2026-06-30',180,120,0.008,0.0,'divergent',0.0001,30,true,NULL)",
        (PROV,),
    )
    rr1_fact("sec_current_rr1_turnover_profiles",
             ", turnover_rate numeric, declared_unit text, turnover_numeric_present boolean, "
             "turnover_text_present boolean, narrative_consistency text")
    cur.execute(
        "INSERT INTO sec_current_rr1_turnover_profiles VALUES "
        "('S1','C1','ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
        "'available',NULL,%s,'{}',0.45,'fraction',true,true,'corroborated')",
        (PROV,),
    )
    rr1_fact("sec_current_rr1_reported_performance_profiles",
             ", canonical_concept text, value_kind text, value_numeric numeric, value_date date, "
             "value_label text, declared_unit text, treatment text, "
             "original_tag text, original_version text")
    cur.execute(
        "INSERT INTO sec_current_rr1_reported_performance_profiles VALUES "
        "('S1','C1','ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
        "'available',NULL,%s,'{}','avg_annual_return','numeric',0.123,NULL,NULL,'pure',"
        "'after_tax_distributions_and_sales','AvgAnnlRtrPct','rr/2023')",
        (PROV,),
    )
    # dispersion: series grain, 3-state.
    cur.execute("""
        CREATE TABLE sec_current_rr1_class_cost_dispersion(
            series_id text, accession_number text, data_date date, effective_date date,
            filed_date date, numeric_class_count integer, class_total integer,
            net_min numeric, net_max numeric, net_spread numeric,
            net_min_class_id text, net_max_class_id text, status text, reason_code text,
            per_class_evidence jsonb, provenance jsonb, coverage jsonb)
    """)
    cur.execute(
        "INSERT INTO sec_current_rr1_class_cost_dispersion VALUES "
        "('S1','ACC-9','2025-12-31','2025-12-31','2026-02-01',3,4,0.005,0.012,0.007,"
        "'C1','C3','available',NULL,%s,%s,'{}')",
        (json.dumps([{"class_id": "C1", "net": 0.005, **LEAK_BLOB}]), PROV),
    )
    # benchmark: series/class grain, latest_* evidence.
    cur.execute("""
        CREATE TABLE sec_current_rr1_benchmark_profiles(
            series_id text, class_id text, latest_accession_number text,
            latest_effective_date date, latest_filed_date date, status text, reason_code text,
            primary_benchmark text, benchmark_consistency text, declared_benchmark_count integer,
            observation_count integer, context_count integer, document_count integer,
            period_count integer, per_benchmark_evidence jsonb, provenance jsonb, coverage jsonb)
    """)
    cur.execute(
        "INSERT INTO sec_current_rr1_benchmark_profiles VALUES "
        "('S1','C1','ACC-9','2025-12-31','2026-02-01','available',NULL,'SP500 TR','consistent',"
        "1,4,1,1,1,%s,%s,'{}')",
        (json.dumps([{"benchmark": "SP500 TR", **LEAK_BLOB}]), PROV),
    )
    # crosswalk: one approved+high-confidence (emitted), one proposed (excluded),
    # one approved but low-confidence (excluded).
    cur.execute("""
        CREATE TABLE rr1_custom_tag_crosswalk(
            custom_tag text, custom_version text, crosswalk_version text,
            canonical_concept text, confidence numeric, method text, review_status text,
            rationale text, created_at timestamptz DEFAULT now())
    """)
    cur.execute(
        "INSERT INTO rr1_custom_tag_crosswalk(custom_tag,custom_version,crosswalk_version,"
        "canonical_concept,confidence,method,review_status) VALUES "
        "('CustomTagLeakX','v1','cw1','management_fee',0.95,'labels','approved'),"
        "('CustomTagLeakY','v1','cw1','net_expense',0.50,'labels','approved'),"
        "('CustomTagLeakZ','v1','cw1','other_expense',0.99,'labels','proposed')",
    )


def _materialize(cur) -> dict:
    return materializer.materialize(cur.connection, as_of=date(2025, 12, 31), code_revision="test")


def test_materialize_projects_every_present_family_and_promotes_current() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        result = _materialize(cur)
        assert result["state"] == "current"
        # all 16 families' sources exist -> all written.
        assert set(result["families_written"]) == set(contract.family_names())
        pointer = cur.execute(
            "SELECT publication_id FROM sec_derived_current_pointers "
            "WHERE product='sec_regulatory_serving_v1'"
        ).fetchone()
        assert pointer is not None
        validated = cur.execute(
            "SELECT lifecycle_state FROM sec_derived_publications "
            "WHERE product='sec_regulatory_serving_v1'"
        ).fetchone()[0]
        assert validated == "validated"
        # publication_version is pinnable by the app.
        version = cur.execute(
            "SELECT publication_version FROM sec_derived_publications "
            "WHERE product='sec_regulatory_serving_v1'"
        ).fetchone()[0]
        assert version == 1
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_state_mapping_and_forward_notes() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        _materialize(cur)

        def one(family: str) -> dict:
            row = cur.execute(
                "SELECT state, reason_code, snapshot_reason_code, payload::text "
                "FROM sec_current_regulatory_serving_facts WHERE family=%s LIMIT 1",
                (family,),
            ).fetchone()
            return {"state": row[0], "reason": row[1], "snap": row[2], "payload": row[3]}

        # forward-note 4: both legs present (leg_incomplete false) -> stays available and
        # the legitimate net flow is SERVED as-is (never dropped, never coerced).
        etf = one("ncen_etf_primary_market")
        assert etf["state"] == "available"
        assert '"net_primary_market_flow": 4000000' in etf["payload"]
        # forward-note 8: expense all-null legs -> degraded.  Task 1c: a QUALITATIVE
        # N-CEN degrade carries ``disclosure_quality_degraded`` (never the coverage code).
        expense = one("ncen_expense_brokerage")
        assert expense["state"] == "degraded"
        assert expense["reason"] == "disclosure_quality_degraded"
        # forward-note 7: closed-end not_applicable preserves the typed snapshot reason.
        ce = one("ncen_closed_end")
        assert ce["state"] == "not_applicable"
        assert ce["reason"] == "asset_family_not_applicable"
        assert ce["snap"] == "fund_is_not_closed_end"
        assert ce["payload"] is None
        # RR1 status passes through (available + degraded + unavailable all present).
        fee_states = {
            r[0] for r in cur.execute(
                "SELECT state FROM sec_current_regulatory_serving_facts WHERE family='rr1_fee'"
            ).fetchall()
        }
        assert fee_states == {"available", "unavailable"}
        # Task 1c: an RR1 status pass-through degrade is a QUANTITATIVE coverage shortfall.
        waiver = one("rr1_waiver")
        assert waiver["state"] == "degraded"
        assert waiver["reason"] == "coverage_below_certified_threshold"
        # forward-note 5: liquidity keeps available with sub-block states in payload.
        liq = one("ncen_liquidity_backstop")
        assert liq["state"] == "available" and "loc_state" in liq["payload"]
        # forward-note 6: securities-lending carries the contract-defect quality flag.
        assert "collateral_liquidated_field_contract_defect" in one("ncen_securities_lending")["payload"]
        # forward-note 10 / Constraint 5: fee fraction declares its unit.
        assert '"declared_unit": "fraction"' in one("rr1_fee")["payload"]
        # forward-notes 11 & 16: treatment carries the load/tax signal.
        assert "after_tax_distributions_and_sales" in one("rr1_reported_performance")["payload"]
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_operational_events_fan_out_to_registrant_funds() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        # add a second fund under the same accession to the roster.
        cur.execute(
            "INSERT INTO sec_current_ncen_structure_profiles VALUES "
            "('S2','F2','ACC-1','2025-12-31','2025-12-31','available',NULL,%s,%s,'{}',"
            "%s,'true','available',NULL)",
            (_blob(is_etf="false"), PROV, json.dumps({})),
        )
        _materialize(cur)
        rows = cur.execute(
            "SELECT fund_id, grain_origin FROM sec_current_regulatory_serving_facts "
            "WHERE family='ncen_operational_event' ORDER BY fund_id"
        ).fetchall()
        assert [r[0] for r in rows] == ["F1", "F2"]
        assert {r[1] for r in rows} == {"registrant"}
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_crosswalk_confidence_gate() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        _materialize(cur)
        rows = cur.execute(
            "SELECT payload::text FROM sec_current_regulatory_serving_facts "
            "WHERE family='rr1_custom_tag_crosswalk'"
        ).fetchall()
        # only the approved + confidence>=0.80 mapping is emitted; tag never exposed.
        assert len(rows) == 1
        assert "management_fee" in rows[0][0]
        assert "CustomTagLeak" not in rows[0][0]
        assert "net_expense" not in rows[0][0]  # approved-but-low-confidence excluded
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_serving_data_contains_no_internal_identifiers() -> None:
    """forward-notes 1 & 14: prove the serving DATA leaks nothing internal."""
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        _materialize(cur)
        dump = cur.execute(
            "SELECT string_agg(concat_ws('|', family, series_id, class_id, fund_id, fact_key, "
            "grain_origin, state, COALESCE(reason_code,''), COALESCE(snapshot_reason_code,''), "
            "COALESCE(accession_number,''), COALESCE(document_id,''), COALESCE(payload::text,'')), "
            "chr(10)) FROM sec_regulatory_serving_facts"
        ).fetchone()[0]
        assert dump  # non-empty
        for token in FORBIDDEN:
            assert token not in dump, f"leaked internal token {token!r}"
        # item 1a: a ``cik:`` value under a PUBLIC key (issuer_ref) is neutralised by
        # the scrubber's value rule (not merely stripped by a blocked key), so no raw
        # ``cik:`` identifier survives anywhere in the served data.
        assert "cik:" not in dump
        # the neutralised entity-key fallbacks (row:/cik:) surface the honest sentinel.
        assert "unavailable" in dump
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_etf_leg_incomplete_degrades_and_drops_net() -> None:
    """forward-note 4: a snapshot with an incomplete (one-legged) AP degrades and the
    untrustworthy net is never served -- even though a net value is present upstream."""
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        payload = {
            "authorized_participants": [],
            "derived": {"net_primary_market_flow": 700, "leg_incomplete": True,
                        "authorized_participant_count": 2},
        }
        cur.execute(
            "INSERT INTO sec_current_ncen_etf_primary_market_profiles VALUES "
            "('S9','F9','ACC-1','2025-12-31','2025-12-31','available',NULL,%s,%s,'{}')",
            (json.dumps(payload), PROV),
        )
        _materialize(cur)
        row = cur.execute(
            "SELECT state, reason_code, payload::text FROM sec_current_regulatory_serving_facts "
            "WHERE family='ncen_etf_primary_market' AND fund_id='F9'"
        ).fetchone()
        assert row[0] == "degraded"
        # Task 1c: leg_incomplete is a QUALITATIVE degrade, not a coverage shortfall.
        assert row[1] == "disclosure_quality_degraded"
        assert "net_primary_market_flow" not in row[2]
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_fee_crosswalk_evidence_is_confidence_gated_and_tag_free() -> None:
    """forward-notes 12 & 15: a fee fact resolved from an APPROVED high-confidence custom
    mapping surfaces crosswalk evidence (concept + version + confidence, never the tag);
    approved-but-low-confidence and proposed mappings surface nothing."""
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        # class -> (custom original tag, canonical concept) matched against the setup's
        # synthetic crosswalk: X=approved 0.95, Y=approved 0.50, Z=proposed 0.99.
        for class_id, tag, concept in (
            ("CWH", "CustomTagLeakX", "management_fee"),
            ("CWL", "CustomTagLeakY", "net_expense"),
            ("CWP", "CustomTagLeakZ", "other_expense"),
        ):
            cur.execute(
                "INSERT INTO sec_current_rr1_fee_profiles VALUES "
                "('S1',%s,'ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
                "'available',NULL,%s,'{}',%s,0.005,%s,'v1')",
                (class_id, PROV, concept, tag),
            )
        _materialize(cur)

        def payload_for(class_id: str) -> str:
            row = cur.execute(
                "SELECT payload::text FROM sec_current_regulatory_serving_facts "
                "WHERE family='rr1_fee' AND class_id=%s",
                (class_id,),
            ).fetchone()
            return row[0] if row else ""

        high = payload_for("CWH")
        assert '"crosswalk_evidence"' in high
        assert '"crosswalk_version": "cw1"' in high
        assert '"canonical_concept": "management_fee"' in high
        assert "CustomTagLeak" not in high  # the internal tag name never leaks
        # gated out -> no crosswalk_evidence key at all.
        assert "crosswalk_evidence" not in payload_for("CWL")
        assert "crosswalk_evidence" not in payload_for("CWP")
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_partial_family_surface_fails_and_promotes_nothing() -> None:
    """A missing family source view must fail closed: nothing validated, nothing
    promoted (the current pointer is never set)."""
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        cur.execute("DROP TABLE sec_current_ncen_closed_end_profiles")
        with pytest.raises(materializer.ServingFamilyCoverageError):
            _materialize(cur)
        pointer = cur.execute(
            "SELECT publication_id FROM sec_derived_current_pointers "
            "WHERE product='sec_regulatory_serving_v1'"
        ).fetchone()
        assert pointer is None
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_allow_missing_families_opts_out_of_the_coverage_gate() -> None:
    """The explicit opt-out lets a deliberately partial setup materialize + promote."""
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        cur.execute("DROP TABLE sec_current_ncen_closed_end_profiles")
        result = materializer.materialize(
            cur.connection, as_of=date(2025, 12, 31), code_revision="test",
            allow_missing_families=True,
        )
        assert result["state"] == "current"
        assert "ncen_closed_end" not in result["families_written"]
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_materialize_is_idempotent() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        first = _materialize(cur)
        second = _materialize(cur)
        assert first["publication_id"] == second["publication_id"]
        count = cur.execute(
            "SELECT count(*) FROM sec_derived_publications WHERE product='sec_regulatory_serving_v1'"
        ).fetchone()[0]
        assert count == 1
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()
