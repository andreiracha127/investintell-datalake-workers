-- Additive price-eligibility predicate over bond_price_observation (Increment 3,
-- Task 5).  This file DOES NOT alter the bond_price_observation_v1 product; it only
-- READS the immutable observation inputs and adds a pure predicate + a classifying
-- view.  It creates NO table, NO publication, and NO current pointer.
--
-- An observation is ELIGIBLE for downstream valuation when ALL hold:
--   * price_type is in the DECLARED eligible set ('trade','evaluated')
--   * accrued_treatment is KNOWN ('clean' or 'dirty'; never 'not_reported')
--   * identity is RESOLVED (identity_state='resolved' => a real security_id)
--   * the (security,date) key is NON-AMBIGUOUS (daily_key_state='unique_in_matching_cohort')
--   * the price is actually PRESENT (price_state='present')
-- Any other observation is NOT eligible — an honest exclusion, never fabricated.
-- The eligibility_reason mirrors the first failing condition, in a fixed order, so
-- the SQL predicate and the Python mirror (src/bonds/eligibility.py) agree exactly.
--
-- Idempotent (CREATE OR REPLACE) so the worker's install step may re-apply it.

CREATE OR REPLACE FUNCTION bond_price_is_eligible(
    p_price_type text,
    p_accrued_treatment text,
    p_identity_state text,
    p_daily_key_state text,
    p_price_state text
) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
    SELECT p_price_type IN ('trade', 'evaluated')
       AND p_accrued_treatment IN ('clean', 'dirty')
       AND p_identity_state = 'resolved'
       AND p_daily_key_state = 'unique_in_matching_cohort'
       AND p_price_state = 'present';
$$;

-- First failing condition wins (fixed order; mirrors src/bonds/eligibility.py).
CREATE OR REPLACE FUNCTION bond_price_eligibility_reason(
    p_price_type text,
    p_accrued_treatment text,
    p_identity_state text,
    p_daily_key_state text,
    p_price_state text
) RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN p_price_type NOT IN ('trade', 'evaluated') THEN 'price_type_not_eligible'
        WHEN p_accrued_treatment NOT IN ('clean', 'dirty') THEN 'accrued_treatment_unknown'
        WHEN p_identity_state <> 'resolved' THEN 'identity_unresolved'
        WHEN p_daily_key_state <> 'unique_in_matching_cohort' THEN 'identity_ambiguous'
        WHEN p_price_state <> 'present' THEN 'price_absent'
        ELSE NULL
    END;
$$;

-- Classifying view: EVERY immutable observation with an is_eligible flag and, when
-- ineligible, the typed reason.  Reads bond_price_observation only.
CREATE OR REPLACE VIEW bond_price_eligibility_v1 AS
SELECT
    o.observation_id,
    o.as_of,
    o.observation_date,
    o.security_id,
    o.price_type,
    o.accrued_treatment,
    o.identity_state,
    o.daily_key_state,
    o.price_state,
    bond_price_is_eligible(o.price_type, o.accrued_treatment, o.identity_state,
                           o.daily_key_state, o.price_state) AS is_eligible,
    bond_price_eligibility_reason(o.price_type, o.accrued_treatment, o.identity_state,
                                  o.daily_key_state, o.price_state) AS eligibility_reason
FROM bond_price_observation o;
