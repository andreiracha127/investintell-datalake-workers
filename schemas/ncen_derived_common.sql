-- Shared helpers for the N-CEN derived-profile snapshots (structure/reliance,
-- provider network, operational events).  These consume only the amendment-aware
-- effective selection and typed V2 rows; no current view reaches into raw rows.

-- Tri-state lexical flag.  The frozen SEC code dictionary authorizes literal `Y`
-- (positive) and literal `N` (negative); every other lexical value -- blank,
-- absent, or an unexpected code -- is `not_reported`.  Empty is NEVER coerced to
-- the negative.
CREATE OR REPLACE FUNCTION ncen_tristate_flag(lexical_value text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN lexical_value = 'Y' THEN 'true'
        WHEN lexical_value = 'N' THEN 'false'
        ELSE 'not_reported'
    END
$$;

-- Stable provider identity.  Identifier precedence is LEI, then CRD, then PCAOB,
-- with the reported name as the last-resort fallback.  The chosen kind is
-- surfaced so downstream consumers can weigh a name-only match differently from
-- a registry match.  Returns NULL when no evidence at all is present.
CREATE OR REPLACE FUNCTION ncen_provider_identity(lei text, crd text, pcaob text, name text)
RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN NULLIF(btrim(lei), '') IS NOT NULL
            THEN jsonb_build_object('identifier_kind','lei','identifier_value',btrim(lei))
        WHEN NULLIF(btrim(crd), '') IS NOT NULL
            THEN jsonb_build_object('identifier_kind','crd','identifier_value',btrim(crd))
        WHEN NULLIF(btrim(pcaob), '') IS NOT NULL
            THEN jsonb_build_object('identifier_kind','pcaob','identifier_value',btrim(pcaob))
        WHEN NULLIF(btrim(name), '') IS NOT NULL
            THEN jsonb_build_object('identifier_kind','name','identifier_value',lower(btrim(name)))
        ELSE NULL
    END
$$;

-- Every effective filing must expose exactly one typed official FUND_ID parent
-- for each profile source row.  There are no CIK-derived identifiers.  Raises on
-- a missing or ambiguous fund identity so a fund-grain build fails closed.
CREATE OR REPLACE FUNCTION ncen_assert_effective_fund_identity(as_of_date date)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM ncen_effective_filings e
        LEFT JOIN ncen_raw_v2_rows f
          ON f.ingestion_run_id = e.ingestion_run_id
         AND f.source_table = 'FUND_REPORTED_INFO.tsv'
         AND f.parse_status = 'typed'
         AND f.accession_number = e.accession_number
        WHERE e.effective_date <= as_of_date
        GROUP BY e.ingestion_run_id, e.accession_number
        HAVING count(f.raw_row_id) = 0
            OR count(f.raw_row_id) FILTER (WHERE NULLIF(btrim(f.fund_id), '') IS NULL) > 0
    ) THEN
        RAISE EXCEPTION 'missing N-CEN fund identity for effective filing';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM ncen_effective_filings e
        JOIN ncen_raw_v2_rows f
          ON f.ingestion_run_id = e.ingestion_run_id
         AND f.source_table = 'FUND_REPORTED_INFO.tsv'
         AND f.parse_status = 'typed'
         AND f.accession_number = e.accession_number
        WHERE e.effective_date <= as_of_date
        GROUP BY e.ingestion_run_id, e.accession_number, f.fund_id
        HAVING count(*) <> 1
    ) THEN
        RAISE EXCEPTION 'ambiguous N-CEN fund identity for effective filing';
    END IF;
END $$;
