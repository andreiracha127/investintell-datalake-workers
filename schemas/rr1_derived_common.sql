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
