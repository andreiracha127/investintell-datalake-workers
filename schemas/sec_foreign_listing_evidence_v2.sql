-- v2 of SEC foreign listing evidence (on top of sec_foreign_listing_evidence.sql).
--
-- A parser correction restates our reading; it does not change the public record.
-- This is the project's restatement rule, as in W1 and W1b.
-- sec_foreign_listing_evidence.retired_reason records why a version was retired:
-- * 'source': a republished document carries other content or loses a fact.
--   The version stays visible before retired_on. NULL, on rows retired before
--   v2, means the same. The replacement is known no earlier than reconciliation.
-- * 'parser_correction': the same source bytes read by another parser version.
--   The old reading was never true, so it is visible at no date. The loader
--   dates the new reading as the reading it replaces: from source_available_on
--   for a document first loaded with its accession, from republication for
--   republished content. A reading that replaces none is known from the filing's
--   public date. New source documents retain the first-loaded protection.
-- Facts and latest-document metadata already record parser_version; no further
-- parser-version column is needed.
--
-- Governed, owner-applied migration (worker_writer or postgres, psql with
-- ON_ERROR_STOP), one transaction, idempotent over the v1 production schema.
-- The nullable column without a default and CHECK NOT VALID do not rewrite or
-- scan existing rows; their reason remains NULL. New rows are checked. The
-- resolver is replaced in place. No evidence or source row is written.
-- Rollback: schemas/sec_foreign_listing_evidence_v2.rollback.sql.
BEGIN;
SET LOCAL lock_timeout = '5s';
-- Serialize DDL with apply before either checks the installed resolver.
SELECT pg_advisory_xact_lock(79311, 173);

ALTER TABLE public.sec_foreign_listing_evidence
    ADD COLUMN IF NOT EXISTS retired_reason text;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                   WHERE conrelid = to_regclass('public.sec_foreign_listing_evidence')
                     AND conname = 'sec_foreign_listing_evidence_retired_reason_check') THEN
        ALTER TABLE public.sec_foreign_listing_evidence
            ADD CONSTRAINT sec_foreign_listing_evidence_retired_reason_check
            CHECK (retired_reason IS NULL
                   OR (retired_reason IN ('source', 'parser_correction')
                       AND retired_on IS NOT NULL)) NOT VALID;
    END IF;
END $$;
COMMENT ON COLUMN public.sec_foreign_listing_evidence.retired_reason IS
    'source (or NULL): the public record changed, visible before retired_on; '
    'parser_correction: our reading was wrong, visible at no date';

-- Exactly one evidence-only result for the line at D. Availability still gates
-- knowledge and effective dates still gate entitlement. The restatement rule
-- removes parser-corrected old readings before every resolver decision.
CREATE OR REPLACE FUNCTION public.sec_foreign_listing_at(
    p_cik bigint, p_symbol text, p_as_of date
)
RETURNS TABLE (
    status text,
    listed_type text,
    ratio_numerator numeric,
    ratio_denominator numeric,
    listing_status text,
    ratio_status text,
    evidence_ids bigint[]
)
LANGUAGE sql STABLE PARALLEL SAFE
SET search_path = pg_catalog, public
AS $fn$
-- All downstream controls, authorities, availability ordering and future-date
-- bounds derive from issuer_observed. A parser-corrected old reading cannot
-- participate at any date, including in the public-by-registration tests.
WITH issuer_observed AS MATERIALIZED (
    SELECT e.*,
           CASE WHEN e.source_kind = 'f6' THEN 'f6'
                WHEN e.source_kind = 'ratio_change_6k' THEN 'change'
                ELSE 'cover' END AS source_stream
    FROM public.sec_foreign_listing_evidence e
    WHERE e.cik = p_cik
      AND (e.evidence_kind <> 'ads_ratio' OR e.ordinary_candidate)
      AND e.available_on <= p_as_of
      AND (e.retired_on IS NULL
           OR (e.retired_on > p_as_of
               AND e.retired_reason IS DISTINCT FROM 'parser_correction'))
), issuer_known AS MATERIALIZED (
    SELECT e.* FROM issuer_observed e
    WHERE NOT (
        e.source_stream = 'change' AND e.ratio_change_program_key IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM issuer_observed c
            WHERE c.source_stream = 'change'
              AND c.ratio_change_correction_kind = 'correcting_and_replacing'
              AND c.ratio_change_correction_text IS NOT NULL
              AND c.ratio_change_program_key = e.ratio_change_program_key
              AND c.effective_from = e.effective_from
              AND c.symbol_key IS NOT DISTINCT FROM e.symbol_key
              AND c.underlying_class IS NOT DISTINCT FROM e.underlying_class
              AND c.ordinary_candidate = e.ordinary_candidate
              AND c.adsh <> e.adsh AND c.filed > e.filed
        )
    )
), issuer_visible AS MATERIALIZED (
    SELECT e.* FROM issuer_known e WHERE e.effective_from <= p_as_of
), issuer_cover AS MATERIALIZED (
    SELECT v.* FROM issuer_visible v
    WHERE v.evidence_kind = 'listed_type'
      AND v.effective_from = (
          SELECT max(t.effective_from) FROM issuer_visible t
          WHERE t.evidence_kind = 'listed_type')
      AND (v.effective_to IS NULL OR v.effective_to > p_as_of)
), issuer_binding AS (
    SELECT CASE WHEN count(DISTINCT c.symbol_key) FILTER (WHERE c.listed_type = 'ads' AND c.ordinary_candidate) = 1
                     AND NOT bool_or(c.listed_type = 'unknown' AND c.ordinary_candidate)
                     AND NOT bool_or(c.listed_type = 'ads' AND c.symbol IS NULL AND c.ordinary_candidate)
                THEN min(c.symbol_key) FILTER (WHERE c.listed_type = 'ads' AND c.ordinary_candidate) END AS symbol_key,
           CASE WHEN count(DISTINCT c.underlying_class) FILTER (WHERE c.listed_type = 'ads' AND c.ordinary_candidate) = 1
                THEN min(c.underlying_class) FILTER (WHERE c.listed_type = 'ads' AND c.ordinary_candidate) END AS underlying_class
    FROM issuer_cover c
), bound_known AS MATERIALIZED (
    SELECT v.* FROM issuer_known v CROSS JOIN issuer_binding b
    WHERE v.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
       OR (v.symbol IS NULL AND v.ordinary_candidate AND v.source_kind IN ('f6', 'ratio_change_6k')
           AND b.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
           AND (v.underlying_class IS NULL OR b.underlying_class IS NULL
                OR v.underlying_class = b.underlying_class))
), ratio_families AS MATERIALIZED (
    -- Every row has already proved its binding to the queried line. Literal
    -- issuer-only and explicit spellings therefore share one source family.
    -- Separate 6-K event dates remain independent within one accession.
    SELECT source_kind, adsh, filed, underlying_class, ratio_change_program_key,
           CASE WHEN source_kind = 'ratio_change_6k' THEN effective_from END AS event_from,
           ratio_numerator / gcd(ratio_numerator, ratio_denominator) AS num,
           ratio_denominator / gcd(ratio_numerator, ratio_denominator) AS den,
           bool_or(coalesce(operative_date_conflict, false)) AS date_conflict,
           bool_or(coalesce(ratio_effectiveness_pending, false)) AS pending
    FROM bound_known
    WHERE evidence_kind = 'ads_ratio'
    GROUP BY source_kind, adsh, filed, underlying_class, ratio_change_program_key,
             CASE WHEN source_kind = 'ratio_change_6k' THEN effective_from END,
             ratio_numerator / gcd(ratio_numerator, ratio_denominator),
             ratio_denominator / gcd(ratio_numerator, ratio_denominator)
), bound_facts AS MATERIALIZED (
    SELECT v.*, coalesce(f.date_conflict, false) AS family_date_conflict,
           coalesce(f.pending, false) AS family_pending,
           f.num AS family_num, f.den AS family_den
    FROM bound_known v CROSS JOIN LATERAL (
        SELECT bool_or(f.date_conflict) AS date_conflict, bool_or(f.pending) AS pending,
               min(f.num) AS num, min(f.den) AS den
        FROM ratio_families f
        WHERE f.source_kind = v.source_kind AND f.adsh = v.adsh AND f.filed = v.filed
          AND f.underlying_class IS NOT DISTINCT FROM v.underlying_class
          AND f.event_from IS NOT DISTINCT FROM (
              CASE WHEN v.source_kind = 'ratio_change_6k' THEN v.effective_from END)
          AND f.num * v.ratio_denominator = v.ratio_numerator * f.den
          AND (f.ratio_change_program_key IS NOT DISTINCT FROM v.ratio_change_program_key
               OR ((f.ratio_change_program_key IS NULL OR v.ratio_change_program_key IS NULL)
                   AND NOT EXISTS (
                       SELECT 1 FROM ratio_families p
                       WHERE p.source_kind = v.source_kind AND p.adsh = v.adsh AND p.filed = v.filed
                         AND p.underlying_class IS NOT DISTINCT FROM v.underlying_class
                         AND p.event_from IS NOT DISTINCT FROM (
                             CASE WHEN v.source_kind = 'ratio_change_6k' THEN v.effective_from END)
                         AND p.num * v.ratio_denominator = v.ratio_numerator * p.den
                       HAVING count(DISTINCT p.ratio_change_program_key) > 1)))
    ) f
), controls AS MATERIALIZED (
    SELECT v.* FROM bound_facts v
    WHERE v.evidence_kind = 'ads_ratio'
      AND (v.operative_date_conflict IS TRUE
           OR (v.source_kind = 'ratio_change_6k' AND v.ratio_effectiveness_pending IS TRUE))
      AND NOT EXISTS (
          SELECT 1 FROM issuer_cover t
          WHERE t.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
            AND t.underlying_class <> v.underlying_class)
), authorities AS MATERIALIZED (
    SELECT v.* FROM bound_facts v
    WHERE v.evidence_kind = 'ads_ratio' AND v.effective_date_explicit
      AND NOT v.family_date_conflict AND NOT v.family_pending
      AND (v.source_kind = 'f6'
           OR (v.source_kind = 'ratio_change_6k'
               AND v.ratio_effectiveness_confirmed IS TRUE
               AND v.ratio_effectiveness_confirmation_text IS NOT NULL))
), settling_options AS MATERIALIZED (
    SELECT m.id AS control_id, a.*
    FROM controls m JOIN authorities a
      ON a.adsh <> m.adsh AND a.filed > m.filed AND a.available_on > m.available_on
     AND a.underlying_class IS NOT DISTINCT FROM m.underlying_class
     AND (a.effective_from >= m.effective_from
          OR (m.operative_date_conflict IS TRUE
              AND a.ratio_numerator * m.ratio_denominator = m.ratio_numerator * a.ratio_denominator
              AND (a.source_kind = 'f6'
                   OR 'ratio_effective' = ANY(a.ratio_effectiveness_confirmed_conditions))))
     -- Approval of one prerequisite cannot discharge a different outstanding
     -- condition. Explicit completion establishes the entitlement itself;
     -- unknown approval identities require that stronger source proof.
     AND (NOT (m.source_kind = 'ratio_change_6k' AND m.ratio_effectiveness_pending IS TRUE)
          OR (a.ratio_effectiveness_confirmed IS TRUE
              AND a.ratio_effectiveness_confirmation_text IS NOT NULL
              AND ('ratio_effective' = ANY(a.ratio_effectiveness_confirmed_conditions)
                   OR (cardinality(m.ratio_effectiveness_conditions) > 0
                       AND NOT ('other_approval' = ANY(m.ratio_effectiveness_conditions))
                       AND NOT ('unknown_condition' = ANY(m.ratio_effectiveness_conditions))
                       AND a.ratio_effectiveness_confirmed_conditions @> m.ratio_effectiveness_conditions))))
     -- A different number must belong to an actual later event, not a
     -- contradictory proposed entitlement for this same event date.
     AND (a.ratio_numerator * m.ratio_denominator = m.ratio_numerator * a.ratio_denominator
          OR (a.effective_from <= p_as_of
              AND a.effective_from > coalesce(
                  m.operative_date_candidates[cardinality(m.operative_date_candidates)], m.effective_from)))
     -- A future exact-ratio authority can establish a conflicted contract's
     -- clock. A conditional notice already pins its proposed event date;
     -- an unrelated future event cannot satisfy that outstanding condition.
     AND (a.effective_from <= p_as_of
          OR (a.ratio_numerator * m.ratio_denominator = m.ratio_numerator * a.ratio_denominator
              AND (m.operative_date_conflict IS TRUE OR a.effective_from = m.effective_from)))
     AND (
         (a.ratio_change_program_key IS NOT NULL AND m.ratio_change_program_key IS NOT NULL
          AND a.ratio_change_program_key = m.ratio_change_program_key)
         OR ((a.ratio_change_program_key IS NULL OR m.ratio_change_program_key IS NULL)
             AND NOT EXISTS (
                 SELECT 1 FROM bound_facts p
                 WHERE p.evidence_kind = 'ads_ratio'
                   AND p.underlying_class IS NOT DISTINCT FROM m.underlying_class
                   AND p.effective_from >= least(m.effective_from, a.effective_from)
                   AND p.effective_from <= greatest(a.effective_from, coalesce(
                       m.operative_date_candidates[cardinality(m.operative_date_candidates)], m.effective_from))
                   AND p.ratio_change_program_key IS NOT NULL
                 HAVING count(DISTINCT p.ratio_change_program_key) > 1))
     )
), settling_clock_options AS MATERIALIZED (
    -- A later notice of a future event cannot roll an already-established
    -- operative entitlement back into the former regime. Apply this across
    -- both authority streams, before their independent latest selection.
    -- Expiry remains after latest selection; an ended operative assertion
    -- cannot resurrect either an older authority or its former ratio.
    SELECT a.* FROM settling_options a
    WHERE a.effective_from <= p_as_of
       OR NOT EXISTS (
           SELECT 1 FROM settling_options s
           WHERE s.control_id = a.control_id AND s.effective_from <= p_as_of)
), current_settling_options AS MATERIALIZED (
    SELECT a.* FROM settling_clock_options a
    WHERE CASE WHEN a.source_stream = 'change' THEN a.effective_from ELSE a.filed + 1 END = (
        SELECT max(CASE WHEN s.source_stream = 'change' THEN s.effective_from ELSE s.filed + 1 END)
        FROM settling_clock_options s
        WHERE s.control_id = a.control_id AND s.source_stream = a.source_stream)
), current_settling_assertions AS MATERIALIZED (
    SELECT a.* FROM current_settling_options a
    WHERE a.source_stream = 'change'
       OR NOT EXISTS (
           SELECT 1 FROM current_settling_options c
           WHERE c.control_id = a.control_id AND c.source_stream = 'change')
       OR a.effective_from >= (
           SELECT max(c.effective_from) FROM current_settling_options c
           WHERE c.control_id = a.control_id AND c.source_stream = 'change')
       OR a.filed + 1 >= (
           SELECT max(c.effective_from) FROM current_settling_options c
           WHERE c.control_id = a.control_id AND c.source_stream = 'change')
       OR EXISTS (
           SELECT 1 FROM current_settling_options c
           WHERE c.control_id = a.control_id AND c.source_stream = 'change'
             AND a.ratio_numerator * c.ratio_denominator = c.ratio_numerator * a.ratio_denominator)
), control_settlement AS MATERIALIZED (
    SELECT m.*, s.settlement_count, s.settled_from, s.settled_num, s.settled_den,
           coalesce(s.settlement_ids, ARRAY[]::bigint[]) AS settlement_ids
    FROM controls m CROSS JOIN LATERAL (
        -- Multiple later sources that independently repeat the same ratio
        -- do not reopen an already established clock merely because one
        -- registration starts that same ratio again at a later date.
        SELECT count(DISTINCT (a.family_num, a.family_den)) AS settlement_count,
               min(a.effective_from) AS settled_from,
               min(a.family_num) AS settled_num, min(a.family_den) AS settled_den,
               array_agg(DISTINCT a.id) AS settlement_ids
        FROM current_settling_assertions a
        WHERE a.control_id = m.id AND (a.effective_to IS NULL OR a.effective_to > p_as_of)
    ) s
), active_controls AS MATERIALIZED (
    SELECT c.* FROM control_settlement c
    WHERE c.effective_from <= p_as_of
      AND (c.settlement_count <> 1 OR c.settled_from > p_as_of)
), settled_registrations AS MATERIALIZED (
    SELECT c.adsh, c.filed, c.underlying_class, c.family_num, c.family_den,
           min(c.settled_from) AS operative_from
    FROM control_settlement c
    WHERE c.source_kind = 'f6' AND c.operative_date_conflict IS TRUE
    GROUP BY c.adsh, c.filed, c.underlying_class, c.family_num, c.family_den
    HAVING bool_and(c.settlement_count = 1
                    AND c.settled_num * c.ratio_denominator = c.ratio_numerator * c.settled_den)
       AND count(DISTINCT c.settled_from) = 1
), operative_facts AS MATERIALIZED (
    SELECT v.*, CASE WHEN v.family_date_conflict THEN s.operative_from
                    ELSE v.effective_from END AS operative_from
    FROM bound_facts v LEFT JOIN settled_registrations s
      ON s.adsh = v.adsh AND s.filed = v.filed
     AND s.underlying_class IS NOT DISTINCT FROM v.underlying_class
     AND s.family_num * v.ratio_denominator = v.ratio_numerator * s.family_den
    WHERE NOT (v.source_kind = 'ratio_change_6k' AND (v.family_pending OR v.family_date_conflict))
      AND (NOT v.family_date_conflict OR (v.source_kind = 'f6' AND s.operative_from IS NOT NULL))
), announced_changes AS MATERIALIZED (
    SELECT c.* FROM bound_facts c CROSS JOIN issuer_binding b
    WHERE c.evidence_kind = 'ads_ratio' AND c.source_kind = 'ratio_change_6k'
      AND c.ordinary_candidate AND NOT c.family_pending AND NOT c.family_date_conflict
      AND NOT EXISTS (
          SELECT 1 FROM issuer_cover t
          WHERE t.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
            AND t.underlying_class <> c.underlying_class)
      AND (c.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
           OR (c.symbol IS NULL
               AND b.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
               AND (c.underlying_class IS NULL OR b.underlying_class IS NULL
                    OR c.underlying_class = b.underlying_class)))
      -- A generic later plan cannot bypass an outstanding conditional event.
      AND NOT EXISTS (
          SELECT 1 FROM control_settlement m
          WHERE m.source_kind = 'ratio_change_6k' AND m.ratio_effectiveness_pending IS TRUE
            AND c.effective_from >= m.effective_from
            AND m.underlying_class IS NOT DISTINCT FROM c.underlying_class
            AND m.ratio_numerator * c.ratio_denominator = c.ratio_numerator * m.ratio_denominator
            AND (m.ratio_change_program_key IS NULL OR c.ratio_change_program_key IS NULL
                 OR m.ratio_change_program_key = c.ratio_change_program_key)
            AND (m.settlement_count <> 1 OR m.settled_from <> c.effective_from
                 OR m.settled_num * c.ratio_denominator <> c.ratio_numerator * m.settled_den))
), future_date_bounds AS MATERIALIZED (
    SELECT c.*, false AS conservative_only FROM announced_changes c
    UNION ALL
    SELECT c.*, true AS conservative_only FROM bound_facts c
    WHERE c.evidence_kind = 'ads_ratio' AND c.source_kind = 'ratio_change_6k'
      AND c.ordinary_candidate AND c.effective_date_explicit AND c.family_pending
      AND NOT EXISTS (
          SELECT 1 FROM issuer_cover t
          WHERE t.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
            AND t.underlying_class <> c.underlying_class)
), visible AS MATERIALIZED (
    SELECT v.* FROM operative_facts v CROSS JOIN issuer_binding b
    WHERE v.operative_from <= p_as_of AND NOT (
        v.source_kind = 'f6'
        AND v.family_pending
        AND NOT EXISTS (
            SELECT 1 FROM announced_changes c
            WHERE c.effective_from <= p_as_of
              -- Normally an announcement date must be on/after filing+1.
              -- A later affirmative completion may instead confirm an event
              -- dated on the F-6 filing day; it becomes usable only once that
              -- completion is public. An older matching ratio cannot activate
              -- a distinct pending registration.
              AND (c.effective_from >= v.filed + 1
                   OR (c.effective_from = v.filed AND c.filed > v.filed
                       AND c.available_on > v.available_on
                       AND c.ratio_effectiveness_confirmed IS TRUE
                       AND c.ratio_effectiveness_confirmation_text IS NOT NULL
                       AND 'ratio_effective' = ANY(c.ratio_effectiveness_confirmed_conditions)))
              AND c.underlying_class IS NOT DISTINCT FROM v.underlying_class
              AND c.ratio_numerator * v.ratio_denominator
                  = v.ratio_numerator * c.ratio_denominator
              AND (
                  (v.ratio_change_program_key IS NOT NULL AND c.ratio_change_program_key IS NOT NULL
                   AND v.ratio_change_program_key = c.ratio_change_program_key)
                  OR ((v.ratio_change_program_key IS NULL OR c.ratio_change_program_key IS NULL)
                      AND NOT EXISTS (
                          SELECT 1 FROM bound_facts p
                          WHERE p.evidence_kind = 'ads_ratio'
                            AND p.underlying_class IS NOT DISTINCT FROM v.underlying_class
                            AND p.effective_from >= CASE WHEN c.effective_from = v.filed
                                                         THEN v.filed ELSE v.filed + 1 END
                            AND p.effective_from <= c.effective_from
                            AND p.ratio_change_program_key IS NOT NULL
                          HAVING count(DISTINCT p.ratio_change_program_key) > 1
                             OR (v.ratio_change_program_key IS NOT NULL
                                 AND coalesce(bool_or(p.ratio_change_program_key <> v.ratio_change_program_key), false))
                             OR (c.ratio_change_program_key IS NOT NULL
                                 AND coalesce(bool_or(p.ratio_change_program_key <> c.ratio_change_program_key), false))))
        )
    )) AND NOT (
        v.source_kind = 'f6' AND NOT v.effective_date_explicit
        AND v.effective_from = v.filed + 1 AND v.ordinary_candidate
        AND (v.underlying_class IS NULL OR b.underlying_class IS NULL
             OR v.underlying_class = b.underlying_class)
        AND NOT EXISTS (
            SELECT 1 FROM issuer_cover t
            WHERE t.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
              AND t.underlying_class <> v.underlying_class)
        AND EXISTS (
             SELECT 1 FROM future_date_bounds c
             WHERE c.source_available_on <= v.source_available_on
               AND c.available_on <= v.source_available_on
               AND c.effective_from > v.source_available_on
               AND c.ratio_numerator * v.ratio_denominator
                   = v.ratio_numerator * c.ratio_denominator
               AND (v.underlying_class IS NULL OR c.underlying_class IS NULL
                    OR v.underlying_class = c.underlying_class)
             HAVING (count(DISTINCT c.effective_from) = 1 OR bool_or(c.conservative_only))
                AND min(c.effective_from) > p_as_of)
    )
), types AS (
    SELECT c.* FROM issuer_cover c
    WHERE c.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
), type_state AS (
    SELECT CASE WHEN count(DISTINCT t.listed_type) > 1
                     OR count(DISTINCT t.underlying_class) > 1 THEN 'ambiguous'
                WHEN min(t.listed_type) IN ('ads', 'ordinary_direct') THEN 'resolved'
                ELSE 'none' END AS state,
           CASE WHEN count(DISTINCT t.listed_type) = 1
                THEN min(t.listed_type) END AS kind,
           CASE WHEN count(DISTINCT t.underlying_class) = 1
                THEN min(t.underlying_class) END AS underlying_class
    FROM types t
), changes AS (
    SELECT v.* FROM visible v
    WHERE v.evidence_kind = 'ads_ratio' AND v.source_stream = 'change'
      AND v.operative_from = (
          SELECT max(c.operative_from) FROM visible c
          WHERE c.evidence_kind = 'ads_ratio' AND c.source_stream = 'change')
), latest_ratios AS (
    SELECT v.* FROM visible v
    WHERE v.evidence_kind = 'ads_ratio'
      AND CASE WHEN v.source_stream = 'change' THEN v.operative_from
               ELSE v.filed + 1 END = (
          SELECT max(CASE WHEN s.source_stream = 'change' THEN s.operative_from
                          ELSE s.filed + 1 END) FROM visible s
          WHERE s.evidence_kind = 'ads_ratio' AND s.source_stream = v.source_stream)
), ratio_candidates AS (
    SELECT v.* FROM latest_ratios v
    WHERE (
          NOT EXISTS (SELECT 1 FROM changes)
          OR v.operative_from >= (SELECT max(c.operative_from) FROM changes c)
          OR (v.source_stream <> 'change'
              AND v.filed + 1 >= (SELECT max(c.operative_from) FROM changes c))
          OR EXISTS (
              SELECT 1 FROM changes c
              WHERE v.ratio_numerator * c.ratio_denominator
                  = c.ratio_numerator * v.ratio_denominator)
      )
), ratios AS (
    SELECT r.*,
           r.ratio_numerator / gcd(r.ratio_numerator, r.ratio_denominator) AS num,
           r.ratio_denominator / gcd(r.ratio_numerator, r.ratio_denominator) AS den
    FROM ratio_candidates r
    WHERE r.effective_to IS NULL OR r.effective_to > p_as_of
), ratio_state AS (
    SELECT CASE WHEN EXISTS (SELECT 1 FROM active_controls) THEN 'ambiguous'
                WHEN count(DISTINCT (r.num, r.den)) > 1
                     OR count(DISTINCT r.underlying_class) > 1
                     OR bool_or(r.underlying_class <> (
                         SELECT t.underlying_class FROM type_state t)) THEN 'ambiguous'
                WHEN bool_or(r.source_stream = 'f6')
                     AND (bool_or(r.source_stream = 'cover')
                          OR bool_or(r.source_stream = 'change')) THEN 'resolved'
                ELSE 'none' END AS state,
           min(r.num) AS num, min(r.den) AS den
    FROM ratios r
), result AS (
    SELECT t.state AS listing_state, t.kind,
           CASE WHEN t.state = 'resolved' AND t.kind = 'ordinary_direct'
                THEN 'resolved' ELSE r.state END AS ratio_state,
           CASE WHEN t.state = 'resolved' AND t.kind = 'ordinary_direct'
                THEN 1::numeric WHEN r.state = 'resolved' THEN r.num END AS num,
           CASE WHEN t.state = 'resolved' AND t.kind = 'ordinary_direct'
                THEN 1::numeric WHEN r.state = 'resolved' THEN r.den END AS den
    FROM type_state t CROSS JOIN ratio_state r
)
SELECT CASE WHEN x.listing_state = 'ambiguous' OR x.ratio_state = 'ambiguous'
                 THEN 'ambiguous'
            WHEN x.listing_state = 'resolved' AND x.ratio_state = 'resolved'
                 THEN 'resolved'
            ELSE 'none' END,
       x.kind,
       CASE WHEN x.listing_state = 'resolved' THEN x.num END,
       CASE WHEN x.listing_state = 'resolved' THEN x.den END,
       x.listing_state, x.ratio_state,
       ARRAY(SELECT DISTINCT q.id FROM (
           SELECT t.id FROM types t
           UNION ALL
           SELECT r.id FROM ratios r
           WHERE x.kind IS DISTINCT FROM 'ordinary_direct'
           UNION ALL
           SELECT c.id FROM active_controls c
           WHERE x.kind IS DISTINCT FROM 'ordinary_direct'
           UNION ALL
           SELECT c.id FROM control_settlement c
           WHERE c.settlement_count = 1 AND c.effective_from <= p_as_of
             AND x.kind IS DISTINCT FROM 'ordinary_direct'
           UNION ALL
           SELECT unnest(c.settlement_ids) FROM control_settlement c
           WHERE c.effective_from <= p_as_of AND x.kind IS DISTINCT FROM 'ordinary_direct'
       ) q ORDER BY q.id)
FROM result x
$fn$;

COMMENT ON TABLE public.sec_foreign_listing_evidence IS
    'Bitemporal SEC annual-report/F-6/6-K evidence only; ordinary shares per ADS are exact rational numbers. Does not admit or size W1 lines. A parser correction''s old reading is visible at no date.';
COMMENT ON TABLE public.sec_foreign_listing_sources IS
    'Latest document reconciliation metadata, including zero-fact parses; first-loaded date protects genuinely new source documents. Same-byte parser additions are known from the filing''s public date; replacements inherit the replaced reading''s availability.';
COMMENT ON FUNCTION public.sec_foreign_listing_at(bigint, text, date) IS
    'PIT foreign line evidence: resolved|ambiguous|none, with separate listing/ratio statuses and underlying evidence IDs. Direct ordinary lines return 1/1. A parser correction''s old reading is visible at no date.';

-- Match W1 ownership. Explicitly undo default reader write privileges.
REVOKE ALL ON TABLE public.sec_foreign_listing_evidence,
    public.sec_foreign_listing_sources FROM PUBLIC;
REVOKE ALL ON SEQUENCE public.sec_foreign_listing_evidence_id_seq FROM PUBLIC;
REVOKE ALL ON FUNCTION public.sec_foreign_listing_at(bigint, text, date) FROM PUBLIC;
DO $$
DECLARE
    relation_name text;
    reader text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'worker_writer') THEN
        ALTER TABLE public.sec_foreign_listing_evidence OWNER TO worker_writer;
        ALTER TABLE public.sec_foreign_listing_sources OWNER TO worker_writer;
        ALTER FUNCTION public.sec_foreign_listing_at(bigint, text, date) OWNER TO worker_writer;
    END IF;
    FOREACH reader IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = reader) THEN
            FOREACH relation_name IN ARRAY ARRAY[
                'sec_foreign_listing_evidence', 'sec_foreign_listing_sources'
            ] LOOP
                EXECUTE format('REVOKE ALL ON TABLE public.%I FROM %I', relation_name, reader);
                EXECUTE format('GRANT SELECT ON TABLE public.%I TO %I', relation_name, reader);
            END LOOP;
            EXECUTE format('REVOKE ALL ON SEQUENCE public.sec_foreign_listing_evidence_id_seq FROM %I', reader);
            EXECUTE format('GRANT EXECUTE ON FUNCTION public.sec_foreign_listing_at(bigint, text, date) TO %I', reader);
        END IF;
    END LOOP;
END $$;
COMMIT;
