-- Shared helpers for the RR1 derived-profile snapshots (fee-waterfall /
-- shareholder-cost profiles, waiver durability, share-class cost dispersion).
-- These consume only the amendment-aware effective selection (rr1_effective_facts);
-- no current view built on top of them ever reaches back into raw RR1 rows.

-- RR1 fee facts are ``uom=pure`` fractions and expense examples are currency
-- amounts.  This helper recognises the exact lexical shape the parser preserves
-- so a genuine reported value becomes numeric and anything else stays NULL --
-- never a synthetic zero.  Scaling (e.g. fraction x100 for display) is a
-- display-side concern; the fraction is stored verbatim (Global Constraint 5).
CREATE OR REPLACE FUNCTION rr1_numeric_value(raw_value text)
RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN raw_value ~ '^[-+]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$'
            THEN raw_value::numeric
        ELSE NULL
    END
$$;

-- NULL-safe difference for two-leg derived metrics (gross - waiver, termination
-- - effective).  A missing leg yields a derived NULL; the legs stay independently
-- nullable in the payload so a gap is never masked by a fabricated value.
CREATE OR REPLACE FUNCTION rr1_safe_diff(minuend numeric, subtrahend numeric)
RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN minuend IS NULL OR subtrahend IS NULL THEN NULL
        ELSE minuend - subtrahend
    END
$$;

-- NULL-safe day span between two dates (e.g. termination minus effective).
-- Returns NULL whenever either endpoint is absent; a real same-day span is 0.
CREATE OR REPLACE FUNCTION rr1_safe_day_span(from_date date, to_date date)
RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN from_date IS NULL OR to_date IS NULL THEN NULL
        ELSE (to_date - from_date)
    END
$$;

-- NULL-safe ratio/spread for derived dispersion intensities.  Returns NULL --
-- never a synthetic zero -- whenever a leg is missing or the denominator is zero.
CREATE OR REPLACE FUNCTION rr1_safe_ratio(numerator numeric, denominator numeric)
RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN numerator IS NULL OR denominator IS NULL OR denominator = 0 THEN NULL
        ELSE round(numerator / denominator, 6)
    END
$$;

-- Mechanical narrative corroboration: does a decimal/percentage rendering of the
-- reported numeric fraction appear as a substring of a narrative text block?  It
-- returns NULL when either leg is missing or the value is non-numeric; TRUE/FALSE
-- otherwise.  This is an OBSERVATION, never a judgement of correctness -- a FALSE
-- means only that the number was not found verbatim in the prose, not that the two
-- disclosures disagree.  The narrative text itself is never persisted by callers.
CREATE OR REPLACE FUNCTION rr1_number_in_text(raw_value text, text_value text)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN raw_value IS NULL OR text_value IS NULL OR rr1_numeric_value(raw_value) IS NULL THEN NULL
        ELSE EXISTS (
            SELECT 1 FROM unnest(ARRAY[
                btrim(raw_value),
                rr1_numeric_value(raw_value)::text,
                (rr1_numeric_value(raw_value) * 100)::text,
                trunc(rr1_numeric_value(raw_value) * 100)::text
            ]) AS c(candidate)
            WHERE c.candidate <> '' AND position(c.candidate IN text_value) > 0
        )
    END
$$;

-- Classify the RR "Performance Measure" axis member (the num/txt ``measure``
-- column) into the tax/load treatment it denotes.  Per the frozen source-table
-- contract the axis distinguishes Before Taxes (empty), After Taxes on
-- Distributions, After Taxes on Distributions and Sales, or a pre-tax broadly
-- available market index.  Anything unrecognised stays ``unclassified`` -- an
-- honest tri-state, never a guessed treatment.  The raw member is preserved
-- verbatim by the caller; this is only a derived label.
CREATE OR REPLACE FUNCTION rr1_performance_measure_treatment(measure_id text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN NULLIF(btrim(measure_id), '') IS NULL THEN 'before_taxes'
        WHEN measure_id ILIKE '%aftertaxesondistributionsandsale%' THEN 'after_tax_distributions_and_sales'
        WHEN measure_id ILIKE '%aftertaxesondistributions%' THEN 'after_tax_distributions'
        WHEN measure_id ILIKE '%broadbasedindex%' OR measure_id ILIKE '%broadmarket%'
             OR measure_id ILIKE '%broadbasedsecuritiesindex%' THEN 'broad_market_index'
        ELSE 'unclassified'
    END
$$;

-- Strict ISO (YYYY-MM-DD) date parse for date-typed text facts (e.g. a fee-waiver
-- termination date carried in txt.tsv).  Anything else -- absent, malformed, or a
-- non-ISO lexical -- yields NULL so the caller can flag it honestly rather than
-- fabricate a date.  The raw parser already quarantines malformed selection dates
-- upstream; this guards the value leg of a text date fact.
CREATE OR REPLACE FUNCTION rr1_safe_iso_date(raw_value text)
RETURNS date LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE parsed date;
BEGIN
    IF raw_value IS NULL OR raw_value !~ '^\d{4}-\d{2}-\d{2}$' THEN
        RETURN NULL;
    END IF;
    BEGIN
        parsed := raw_value::date;
    EXCEPTION WHEN SQLSTATE '22007' OR SQLSTATE '22008' THEN
        RETURN NULL;
    END;
    RETURN parsed;
END $$;
