-- Immutable N-CEN expense / brokerage profiles.  Grain is one row per
-- (publication, source run, accession, FUND_ID).  Families cover fund expenses
-- (management fee, net operating expenses, expense-limit / waiver / recoupment
-- arrangement flags), brokerage (aggregate commissions with per-broker detail,
-- aggregate principal transactions with per-dealer detail, broker research), and
-- a CROSS-REFERENCE to the registrant-grain accounting / audit / control
-- disclosures (qualified opinion, material weakness, internal-control report,
-- valuation-method change, accounting-principle change, NAV-error correction).
-- Those disclosures are authoritatively materialized once, at registrant grain,
-- by ncen_operational_event_v1; here they are referenced by fund/accession and
-- NOT re-materialized -- the cross-reference names that product and grain so the
-- fund-grain regulatory-complexity indicator can count them.  Broker and
-- principal-dealer children are PRE-AGGREGATED at their own grain (Global
-- Constraint 4) with a fan-out guard.  The regulatory-complexity indicator is
-- counts and flags only -- a disclosure is never treated as evidence of
-- misconduct, and no absent leg is coerced to a synthetic zero.

CREATE TABLE IF NOT EXISTS ncen_expense_brokerage_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    source_run_id uuid NOT NULL,
    accession_number text NOT NULL,
    effective_date date NOT NULL,
    form text NOT NULL,
    is_amendment integer NOT NULL CHECK (is_amendment IN (0, 1)),
    registrant_cik text NOT NULL,
    fund_id text NOT NULL,
    series_id text,
    methodology_version text NOT NULL DEFAULT 'ncen_expense_brokerage_v1',
    measured_at date NOT NULL,
    expense_brokerage_state text NOT NULL CHECK (expense_brokerage_state IN ('available','unavailable','not_applicable')),
    expense_brokerage_reason_code text,
    expense_brokerage jsonb,
    provenance jsonb NOT NULL,
    coverage jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (publication_id, source_run_id, accession_number, fund_id),
    CHECK (methodology_version = 'ncen_expense_brokerage_v1'),
    CHECK (jsonb_typeof(provenance) = 'object'),
    CHECK (jsonb_typeof(coverage) = 'object'),
    CHECK ((expense_brokerage_state = 'available') = (expense_brokerage IS NOT NULL AND expense_brokerage_reason_code IS NULL))
);

CREATE TABLE IF NOT EXISTS ncen_expense_brokerage_builds (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    as_of_date date NOT NULL,
    effective_input_count integer NOT NULL CHECK (effective_input_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION ncen_expense_brokerage_build_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN expense/brokerage build identity is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_expense_brokerage_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN expense/brokerage build identity requires a prepared expense/brokerage publication';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_expense_brokerage_build_guard ON ncen_expense_brokerage_builds;
CREATE TRIGGER ncen_expense_brokerage_build_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_expense_brokerage_builds
FOR EACH ROW EXECUTE FUNCTION ncen_expense_brokerage_build_guard();

CREATE OR REPLACE FUNCTION ncen_expense_brokerage_write_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
        pinned_as_of date;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'N-CEN expense/brokerage profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = NEW.publication_id AND product = 'ncen_expense_brokerage_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN expense/brokerage profile requires a prepared expense/brokerage publication';
    END IF;
    SELECT as_of_date INTO pinned_as_of FROM ncen_expense_brokerage_builds WHERE publication_id = NEW.publication_id;
    IF pinned_as_of IS DISTINCT FROM NEW.measured_at THEN
        RAISE EXCEPTION 'N-CEN expense/brokerage profile requires matching pinned build metadata';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS ncen_expense_brokerage_write_guard ON ncen_expense_brokerage_profiles;
CREATE TRIGGER ncen_expense_brokerage_write_guard
BEFORE INSERT OR UPDATE OR DELETE ON ncen_expense_brokerage_profiles
FOR EACH ROW EXECUTE FUNCTION ncen_expense_brokerage_write_guard();

CREATE OR REPLACE FUNCTION build_ncen_expense_brokerage_profiles(
    target_publication_id uuid,
    as_of_date date
) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    parent_state text;
    computed_fingerprint char(64);
    selected_count integer;
    pinned_fingerprint char(64);
    pinned_as_of date;
    inserted_count integer;
BEGIN
    IF as_of_date IS NULL THEN
        RAISE EXCEPTION 'N-CEN expense/brokerage build requires an as_of_date';
    END IF;

    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id = target_publication_id AND product = 'ncen_expense_brokerage_v1'
    FOR UPDATE;
    IF parent_state <> 'prepared' THEN
        RAISE EXCEPTION 'N-CEN expense/brokerage build requires a prepared expense/brokerage publication';
    END IF;

    SELECT count(*)::integer,
           (md5(COALESCE(string_agg(
                ingestion_run_id::text || ':' || accession_number || ':' || effective_date::text || ':' || form,
                '|' ORDER BY ingestion_run_id, accession_number, effective_date, form
            ), '')) || md5('ncen_expense_brokerage_v1:' || as_of_date::text))::char(64)
      INTO selected_count, computed_fingerprint
    FROM ncen_effective_filings
    WHERE effective_date <= as_of_date;

    PERFORM ncen_assert_effective_fund_identity(as_of_date);

    -- Fan-out guard (Increment 2 Task 1b): the parent CTE LEFT JOINs REGISTRANT 1:1
    -- to fold registrant-level audit disclosures onto each fund row.  If a filing ever
    -- exposed more than one REGISTRANT surface, that join would multiply the fund grain
    -- and the fund-grain PK would keep an ARBITRARY registrant row -- a silent wrong
    -- choice.  Assert at most one REGISTRANT per effective filing (mirrors the
    -- ncen_operational_event_profiles template) so it fails closed instead.
    IF EXISTS (
        SELECT 1
        FROM ncen_effective_filings e
        JOIN ncen_raw_v2_rows r ON r.ingestion_run_id=e.ingestion_run_id
                               AND r.source_table='REGISTRANT.tsv'
                               AND r.parse_status='typed'
                               AND r.accession_number=e.accession_number
        WHERE e.effective_date <= as_of_date
        GROUP BY e.ingestion_run_id, e.accession_number
        HAVING count(*) <> 1
    ) THEN
        RAISE EXCEPTION 'ambiguous N-CEN registrant identity for effective filing';
    END IF;

    INSERT INTO ncen_expense_brokerage_builds
        (publication_id, input_fingerprint, as_of_date, effective_input_count)
    VALUES (target_publication_id, computed_fingerprint, as_of_date, selected_count)
    ON CONFLICT (publication_id) DO NOTHING;

    SELECT b.input_fingerprint, b.as_of_date INTO pinned_fingerprint, pinned_as_of
    FROM ncen_expense_brokerage_builds b WHERE b.publication_id = target_publication_id FOR UPDATE;
    IF pinned_as_of IS DISTINCT FROM as_of_date THEN
        RAISE EXCEPTION 'N-CEN expense/brokerage publication is already pinned to as_of_date %', pinned_as_of;
    END IF;
    IF pinned_fingerprint IS DISTINCT FROM computed_fingerprint THEN
        RAISE EXCEPTION 'N-CEN expense/brokerage publication is already pinned to effective-input fingerprint %', pinned_fingerprint;
    END IF;

    DROP TABLE IF EXISTS _ncen_eb_sf;
    DROP TABLE IF EXISTS _ncen_eb_children;
    DROP TABLE IF EXISTS _ncen_eb_child_agg;

    CREATE TEMP TABLE _ncen_eb_sf ON COMMIT DROP AS
    SELECT e.ingestion_run_id AS source_run_id, e.accession_number, e.effective_date, e.form, e.is_amendment,
           e.registrant_cik, e.raw_row_id AS submission_raw_row_id,
           f.fund_id, f.raw_row_id AS fund_raw_row_id, f.typed_projection AS fund_evidence,
           NULLIF(btrim(f.typed_projection->>'SERIES_ID'),'') AS series_id,
           NULLIF(btrim(f.typed_projection->>'MANAGEMENT_FEE'),'')::numeric AS management_fee,
           NULLIF(btrim(f.typed_projection->>'NET_OPERATING_EXPENSES'),'')::numeric AS net_operating_expenses,
           NULLIF(btrim(f.typed_projection->>'MONTHLY_AVG_NET_ASSETS'),'')::numeric AS monthly_avg_net_assets,
           NULLIF(btrim(f.typed_projection->>'DAILY_AVG_NET_ASSETS'),'')::numeric AS daily_avg_net_assets,
           NULLIF(btrim(f.typed_projection->>'AGG_COMMISSION'),'')::numeric AS aggregate_commission,
           NULLIF(btrim(f.typed_projection->>'AGG_PRINCIPAL'),'')::numeric AS aggregate_principal,
           reg.typed_projection AS reg_evidence, reg.raw_row_id AS registrant_raw_row_id,
           sub.typed_projection AS sub_evidence,
           jsonb_build_object(
               'has_expense_limit', ncen_tristate_flag(f.typed_projection->>'HAS_EXP_LIMIT'),
               'has_expense_reduced_waived', ncen_tristate_flag(f.typed_projection->>'HAS_EXP_REDUCED_WAIVED'),
               'has_expense_subject_recoupment', ncen_tristate_flag(f.typed_projection->>'HAS_EXP_SUBJ_RECOUP'),
               'has_expense_recouped', ncen_tristate_flag(f.typed_projection->>'HAS_EXP_RECOUPED')
           ) AS expense_arrangements,
           jsonb_build_object(
               'qualified_audit_opinion', ncen_tristate_flag(reg.typed_projection->>'IS_ACCT_OPINION_QUALIFIED'),
               'material_weakness', ncen_tristate_flag(reg.typed_projection->>'IS_MATERIAL_WEAKNESS_NOTED'),
               'internal_control_report', ncen_tristate_flag(sub.typed_projection->>'IS_IPA_REPORT_INTERNAL_CONTROL'),
               'valuation_method_change', ncen_tristate_flag(reg.typed_projection->>'IS_VALUE_METHOD_CHANGED'),
               'accounting_principle_change', ncen_tristate_flag(reg.typed_projection->>'IS_ACCT_PRINCIPLE_CHANGED'),
               'nav_error_correction', ncen_tristate_flag(reg.typed_projection->>'IS_NAV_ERROR_CORRECTED')
           ) AS audit_disclosures
    FROM ncen_effective_filings e
    JOIN ncen_raw_v2_rows f ON f.ingestion_run_id=e.ingestion_run_id
                           AND f.source_table='FUND_REPORTED_INFO.tsv'
                           AND f.parse_status='typed'
                           AND f.accession_number=e.accession_number
    JOIN ncen_raw_v2_rows sub ON sub.raw_row_id=e.raw_row_id
    LEFT JOIN ncen_raw_v2_rows reg ON reg.ingestion_run_id=e.ingestion_run_id
                                  AND reg.source_table='REGISTRANT.tsv'
                                  AND reg.parse_status='typed'
                                  AND reg.accession_number=e.accession_number
    WHERE e.effective_date <= as_of_date;

    -- Broker and principal-dealer children pre-aggregated at their own grain
    -- (each raw child row is one execution counterparty).  Every candidate
    -- carries the raw row it came from so a fan-out can be detected.
    CREATE TEMP TABLE _ncen_eb_children ON COMMIT DROP AS
    WITH candidates AS (
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'broker' AS relation,
               ncen_provider_identity(r.typed_projection->>'BROKER_LEI', r.typed_projection->>'CRD_NUM', NULL,
                                      r.typed_projection->>'BROKER_NAME') AS identity,
               lower(btrim(r.typed_projection->>'BROKER_NAME')) AS display_name,
               NULLIF(btrim(r.typed_projection->>'GROSS_COMMISSION'),'')::numeric AS amount,
               r.source_table, r.raw_row_id
        FROM _ncen_eb_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='BROKER.tsv' AND r.parse_status='typed'
        UNION ALL
        SELECT sf.source_run_id, sf.accession_number, sf.fund_id, 'principal_dealer',
               ncen_provider_identity(r.typed_projection->>'PRINCIPAL_LEI', r.typed_projection->>'CRD_NUM', NULL,
                                      r.typed_projection->>'PRINCIPAL_NAME'),
               lower(btrim(r.typed_projection->>'PRINCIPAL_NAME')),
               NULLIF(btrim(r.typed_projection->>'PRINCIPAL_TOTAL_PURCHASE_SALE'),'')::numeric,
               r.source_table, r.raw_row_id
        FROM _ncen_eb_sf sf JOIN ncen_raw_v2_rows r
          ON r.ingestion_run_id=sf.source_run_id AND r.fund_id=sf.fund_id
         AND r.source_table='PRINCIPAL_TRANSACTION.tsv' AND r.parse_status='typed'
    )
    SELECT source_run_id, accession_number, fund_id, relation,
           identity->>'identifier_kind' AS identifier_kind, identity->>'identifier_value' AS identifier_value,
           display_name, amount, source_table, raw_row_id
    FROM candidates;

    -- Hard failure if pre-aggregation ever multiplied the fund grain.
    IF EXISTS (
        SELECT 1 FROM _ncen_eb_children
        GROUP BY source_run_id, accession_number, fund_id, source_table, raw_row_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'N-CEN expense/brokerage row multiplication detected';
    END IF;

    CREATE TEMP TABLE _ncen_eb_child_agg ON COMMIT DROP AS
    SELECT source_run_id, accession_number, fund_id,
           COALESCE(jsonb_agg(jsonb_build_object(
               'identifier_kind', identifier_kind, 'identifier_value', identifier_value,
               'display_name', display_name, 'gross_commission', amount)
               ORDER BY raw_row_id) FILTER (WHERE relation='broker'), '[]'::jsonb) AS brokers,
           COALESCE(jsonb_agg(jsonb_build_object(
               'identifier_kind', identifier_kind, 'identifier_value', identifier_value,
               'display_name', display_name, 'total_purchase_sale', amount)
               ORDER BY raw_row_id) FILTER (WHERE relation='principal_dealer'), '[]'::jsonb) AS principal_dealers,
           count(*) FILTER (WHERE relation='broker') AS broker_count,
           count(*) FILTER (WHERE relation='principal_dealer') AS principal_dealer_count
    FROM _ncen_eb_children
    GROUP BY source_run_id, accession_number, fund_id;

    INSERT INTO ncen_expense_brokerage_profiles(
        publication_id,source_run_id,accession_number,effective_date,form,is_amendment,registrant_cik,fund_id,series_id,
        measured_at,expense_brokerage_state,expense_brokerage_reason_code,expense_brokerage,provenance,coverage
    )
    SELECT target_publication_id, sf.source_run_id, sf.accession_number, sf.effective_date, sf.form, sf.is_amendment,
           sf.registrant_cik, sf.fund_id, sf.series_id, as_of_date,
           'available', NULL,
           jsonb_build_object(
               'expenses', jsonb_build_object(
                   'management_fee', sf.management_fee,
                   'net_operating_expenses', sf.net_operating_expenses,
                   'monthly_avg_net_assets', sf.monthly_avg_net_assets,
                   'daily_avg_net_assets', sf.daily_avg_net_assets,
                   'expense_arrangements', sf.expense_arrangements),
               'brokerage', jsonb_build_object(
                   'aggregate_commission', sf.aggregate_commission,
                   'aggregate_principal', sf.aggregate_principal,
                   'paid_broker_research', ncen_tristate_flag(sf.fund_evidence->>'DID_PAY_BROKER_RESEARCH'),
                   'brokers', COALESCE(ca.brokers, '[]'::jsonb),
                   'principal_dealers', COALESCE(ca.principal_dealers, '[]'::jsonb)),
               'audit_control_cross_reference', jsonb_build_object(
                   'operational_event_grain', 'registrant',
                   'operational_event_product', 'ncen_operational_event_v1',
                   'registrant_cik', sf.registrant_cik,
                   'accession_number', sf.accession_number,
                   'disclosures', sf.audit_disclosures),
               'derived', jsonb_build_object(
                   'regulatory_complexity', jsonb_build_object(
                       'broker_count', COALESCE(ca.broker_count, 0),
                       'principal_dealer_count', COALESCE(ca.principal_dealer_count, 0),
                       'expense_arrangement_flag_count',
                           (SELECT count(*) FROM jsonb_each_text(sf.expense_arrangements) WHERE value='true'),
                       'audit_control_disclosure_count',
                           (SELECT count(*) FROM jsonb_each_text(sf.audit_disclosures) WHERE value='true'),
                       'total_disclosure_signals',
                           (SELECT count(*) FROM jsonb_each_text(sf.expense_arrangements) WHERE value='true')
                           + (SELECT count(*) FROM jsonb_each_text(sf.audit_disclosures) WHERE value='true'),
                       'has_audit_control_disclosure',
                           (SELECT count(*) FROM jsonb_each_text(sf.audit_disclosures) WHERE value='true') > 0))
           ),
           jsonb_build_object('effective_selection_view','ncen_effective_filings','registrant_cik',sf.registrant_cik,
                              'submission_raw_row_id',sf.submission_raw_row_id,'fund_raw_row_id',sf.fund_raw_row_id,
                              'registrant_raw_row_id',sf.registrant_raw_row_id,
                              'source_run_id',sf.source_run_id,'accession_number',sf.accession_number,
                              'fund_source_table','FUND_REPORTED_INFO.tsv'),
           jsonb_build_object('broker_count', COALESCE(ca.broker_count, 0),
                              'principal_dealer_count', COALESCE(ca.principal_dealer_count, 0),
                              'registrant_evidence_present', sf.reg_evidence IS NOT NULL)
    FROM _ncen_eb_sf sf
    LEFT JOIN _ncen_eb_child_agg ca USING (source_run_id, accession_number, fund_id)
    ON CONFLICT (publication_id,source_run_id,accession_number,fund_id) DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END $$;

CREATE OR REPLACE VIEW sec_current_ncen_expense_brokerage_profiles AS
SELECT p.*
FROM sec_derived_current_pointers c
JOIN ncen_expense_brokerage_profiles p ON p.publication_id=c.publication_id
WHERE c.product='ncen_expense_brokerage_v1';
