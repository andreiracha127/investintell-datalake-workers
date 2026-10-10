-- B2 phase 1 foreign equity sizing: existing W1/W1c evidence only.
-- Apply after sec_ticker_cik_history_v1..v3 and W1c base + v2.
-- No publication registry, action ledger, currency evidence, or new source data.
-- Rollback: schemas/sec_foreign_equity_sizing_v1.rollback.sql.
BEGIN;
SET LOCAL lock_timeout = '5s';
-- The loader rechecks the resolver under this same reconciliation lock.
SELECT pg_catalog.pg_advisory_xact_lock(79311, 173);

DO $dependencies$
BEGIN
    IF pg_catalog.to_regclass('public.sec_ticker_cik_observations') IS NULL
       OR pg_catalog.to_regclass('public.sec_cover_share_counts') IS NULL
        OR COALESCE((SELECT pg_catalog.md5(p.prosrc) <> '64663a568b4e2b749069faae232105b9' FROM pg_catalog.pg_proc p WHERE p.oid = pg_catalog.to_regprocedure('public.sec_observations_at(date,boolean)')), true)
        OR COALESCE((SELECT pg_catalog.md5(p.prosrc) <> '331ad6a0e746bb240d1f0230c1a2d3ae' FROM pg_catalog.pg_proc p WHERE p.oid = pg_catalog.to_regprocedure('public.sec_share_counts_at(date,boolean)')), true)
        OR COALESCE((SELECT pg_catalog.md5(p.prosrc) <> '5268c2b949cbf33c22e6db36130fe458' FROM pg_catalog.pg_proc p WHERE p.oid = pg_catalog.to_regprocedure('public.sec_class_label(text,text)')), true)
        OR COALESCE((SELECT pg_catalog.md5(p.prosrc) <> '3d942d2115af41d460afb4db7381085c' FROM pg_catalog.pg_proc p WHERE p.oid = pg_catalog.to_regprocedure('public.sec_label_norm(text)')), true)
       OR NOT COALESCE((SELECT 'effective_on' = ANY(p.proargnames)
                        FROM pg_catalog.pg_proc p WHERE p.oid = pg_catalog.to_regprocedure(
                            'public.sec_issuer_end_events(bigint,date,boolean)')), false) THEN
        RAISE EXCEPTION 'B2 sizing requires the exact W1 v1-v3 evidence/helper APIs';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a
                   WHERE a.attrelid = pg_catalog.to_regclass('public.sec_foreign_listing_evidence')
                     AND a.attname = 'retired_reason' AND NOT a.attisdropped)
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c
                      WHERE c.conrelid = pg_catalog.to_regclass('public.sec_foreign_listing_evidence')
                        AND c.conname = 'sec_foreign_listing_evidence_retired_reason_check'
                        AND c.contype = 'c')
       OR NOT (COALESCE((SELECT pg_catalog.md5(p.prosrc) = '60f5d1bf86a645a41fb7e23328c7ab8a'
                        FROM pg_catalog.pg_proc p WHERE p.oid = pg_catalog.to_regprocedure(
                            'public.sec_foreign_listing_at(bigint,text,date)')), false)
               OR (COALESCE((SELECT pg_catalog.md5(p.prosrc) = 'fe9e9f1e13d882d5215f22f13e02d7f5'
                             AND p.proconfig IS NULL AND NOT p.prosecdef
                             FROM pg_catalog.pg_proc p WHERE p.oid = pg_catalog.to_regprocedure(
                                 'public.sec_foreign_listing_at(bigint,text,date)')), false)
                   AND COALESCE((SELECT pg_catalog.md5(p.prosrc) = 'e1c0a72d43fb036c80cdb153f4150bd5'
                                 AND p.proconfig IS NULL AND NOT p.prosecdef
                                 FROM pg_catalog.pg_proc p WHERE p.oid = pg_catalog.to_regprocedure(
                                     'public.sec_foreign_listing_context_at(bigint,text,date,date)')), false)
                   AND COALESCE((SELECT pg_catalog.md5(p.prosrc) = '0d53b7999be2799647a422997a04bcdc'
                                 AND p.proconfig IS NULL AND NOT p.prosecdef
                                 FROM pg_catalog.pg_proc p WHERE p.oid = pg_catalog.to_regprocedure(
                                     'public.sec_foreign_listing_election_at(bigint,text,date,date)')), false))) THEN
        RAISE EXCEPTION 'B2 sizing requires W1c base + exact v2 (or exact installed sizing v1 election/context/legacy composition)';
    END IF;
END
$dependencies$;

-- The v2 resolver election is authoritative here. Economic eligibility uses
-- p_effective_on; source visibility/retirement uses p_known_on. Metadata comes
-- from the elected type and ratio facts, never from the audit evidence_ids.
CREATE OR REPLACE FUNCTION public.sec_foreign_listing_election_at(
    p_cik bigint, p_symbol text, p_effective_on date, p_known_on date
)
RETURNS TABLE (
    status text,
    listed_type text,
    ratio_numerator numeric,
    ratio_denominator numeric,
    listing_status text,
    ratio_status text,
    evidence_ids bigint[],
    listing_class text,
    ratio_class text,
    program_key text,
    ratio_effective_from date,
    ratio_effective_to date,
    program_ambiguous boolean
)
LANGUAGE sql STABLE PARALLEL SAFE
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
      AND e.available_on <= p_known_on
      AND (e.retired_on IS NULL
           OR (e.retired_on > p_known_on
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
    SELECT e.* FROM issuer_known e WHERE e.effective_from <= p_effective_on
), issuer_cover AS MATERIALIZED (
    SELECT v.* FROM issuer_visible v
    WHERE v.evidence_kind = 'listed_type'
      AND v.effective_from = (
          SELECT pg_catalog.max(t.effective_from) FROM issuer_visible t
          WHERE t.evidence_kind = 'listed_type')
      AND (v.effective_to IS NULL OR v.effective_to > p_effective_on)
), issuer_binding AS (
    SELECT CASE WHEN pg_catalog.count(DISTINCT c.symbol_key) FILTER (WHERE c.listed_type = 'ads' AND c.ordinary_candidate) = 1
                     AND NOT pg_catalog.bool_or(c.listed_type = 'unknown' AND c.ordinary_candidate)
                     AND NOT pg_catalog.bool_or(c.listed_type = 'ads' AND c.symbol IS NULL AND c.ordinary_candidate)
                THEN pg_catalog.min(c.symbol_key) FILTER (WHERE c.listed_type = 'ads' AND c.ordinary_candidate) END AS symbol_key,
           CASE WHEN pg_catalog.count(DISTINCT c.underlying_class) FILTER (WHERE c.listed_type = 'ads' AND c.ordinary_candidate) = 1
                THEN pg_catalog.min(c.underlying_class) FILTER (WHERE c.listed_type = 'ads' AND c.ordinary_candidate) END AS underlying_class
    FROM issuer_cover c
), bound_known AS MATERIALIZED (
    SELECT v.* FROM issuer_known v CROSS JOIN issuer_binding b
    WHERE v.symbol_key = pg_catalog.regexp_replace(pg_catalog.upper(p_symbol), '[^A-Z0-9]', '', 'g')
       OR (v.symbol IS NULL AND v.ordinary_candidate AND v.source_kind IN ('f6', 'ratio_change_6k')
           AND b.symbol_key = pg_catalog.regexp_replace(pg_catalog.upper(p_symbol), '[^A-Z0-9]', '', 'g')
           AND (v.underlying_class IS NULL OR b.underlying_class IS NULL
                OR v.underlying_class = b.underlying_class))
), ratio_families AS MATERIALIZED (
    -- Every row has already proved its binding to the queried line. Literal
    -- issuer-only and explicit spellings therefore share one source family.
    -- Separate 6-K event dates remain independent within one accession.
    SELECT source_kind, adsh, filed, underlying_class, ratio_change_program_key,
           CASE WHEN source_kind = 'ratio_change_6k' THEN effective_from END AS event_from,
           ratio_numerator / pg_catalog.gcd(ratio_numerator, ratio_denominator) AS num,
           ratio_denominator / pg_catalog.gcd(ratio_numerator, ratio_denominator) AS den,
           pg_catalog.bool_or(coalesce(operative_date_conflict, false)) AS date_conflict,
           pg_catalog.bool_or(coalesce(ratio_effectiveness_pending, false)) AS pending
    FROM bound_known
    WHERE evidence_kind = 'ads_ratio'
    GROUP BY source_kind, adsh, filed, underlying_class, ratio_change_program_key,
             CASE WHEN source_kind = 'ratio_change_6k' THEN effective_from END,
             ratio_numerator / pg_catalog.gcd(ratio_numerator, ratio_denominator),
             ratio_denominator / pg_catalog.gcd(ratio_numerator, ratio_denominator)
), bound_facts AS MATERIALIZED (
    SELECT v.*, coalesce(f.date_conflict, false) AS family_date_conflict,
           coalesce(f.pending, false) AS family_pending,
           f.num AS family_num, f.den AS family_den
    FROM bound_known v CROSS JOIN LATERAL (
        SELECT pg_catalog.bool_or(f.date_conflict) AS date_conflict, pg_catalog.bool_or(f.pending) AS pending,
               pg_catalog.min(f.num) AS num, pg_catalog.min(f.den) AS den
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
                       HAVING pg_catalog.count(DISTINCT p.ratio_change_program_key) > 1)))
    ) f
), controls AS MATERIALIZED (
    SELECT v.* FROM bound_facts v
    WHERE v.evidence_kind = 'ads_ratio'
      AND (v.operative_date_conflict IS TRUE
           OR (v.source_kind = 'ratio_change_6k' AND v.ratio_effectiveness_pending IS TRUE))
      AND NOT EXISTS (
          SELECT 1 FROM issuer_cover t
          WHERE t.symbol_key = pg_catalog.regexp_replace(pg_catalog.upper(p_symbol), '[^A-Z0-9]', '', 'g')
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
                   OR (pg_catalog.cardinality(m.ratio_effectiveness_conditions) > 0
                       AND NOT ('other_approval' = ANY(m.ratio_effectiveness_conditions))
                       AND NOT ('unknown_condition' = ANY(m.ratio_effectiveness_conditions))
                       AND a.ratio_effectiveness_confirmed_conditions @> m.ratio_effectiveness_conditions))))
     -- A different number must belong to an actual later event, not a
     -- contradictory proposed entitlement for this same event date.
     AND (a.ratio_numerator * m.ratio_denominator = m.ratio_numerator * a.ratio_denominator
          OR (a.effective_from <= p_effective_on
              AND a.effective_from > coalesce(
                  m.operative_date_candidates[pg_catalog.cardinality(m.operative_date_candidates)], m.effective_from)))
     -- A future exact-ratio authority can establish a conflicted contract's
     -- clock. A conditional notice already pins its proposed event date;
     -- an unrelated future event cannot satisfy that outstanding condition.
     AND (a.effective_from <= p_effective_on
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
                       m.operative_date_candidates[pg_catalog.cardinality(m.operative_date_candidates)], m.effective_from))
                   AND p.ratio_change_program_key IS NOT NULL
                 HAVING pg_catalog.count(DISTINCT p.ratio_change_program_key) > 1))
     )
), settling_clock_options AS MATERIALIZED (
    -- A later notice of a future event cannot roll an already-established
    -- operative entitlement back into the former regime. Apply this across
    -- both authority streams, before their independent latest selection.
    -- Expiry remains after latest selection; an ended operative assertion
    -- cannot resurrect either an older authority or its former ratio.
    SELECT a.* FROM settling_options a
    WHERE a.effective_from <= p_effective_on
       OR NOT EXISTS (
           SELECT 1 FROM settling_options s
           WHERE s.control_id = a.control_id AND s.effective_from <= p_effective_on)
), current_settling_options AS MATERIALIZED (
    SELECT a.* FROM settling_clock_options a
    WHERE CASE WHEN a.source_stream = 'change' THEN a.effective_from ELSE a.filed + 1 END = (
        SELECT pg_catalog.max(CASE WHEN s.source_stream = 'change' THEN s.effective_from ELSE s.filed + 1 END)
        FROM settling_clock_options s
        WHERE s.control_id = a.control_id AND s.source_stream = a.source_stream)
), current_settling_assertions AS MATERIALIZED (
    SELECT a.* FROM current_settling_options a
    WHERE a.source_stream = 'change'
       OR NOT EXISTS (
           SELECT 1 FROM current_settling_options c
           WHERE c.control_id = a.control_id AND c.source_stream = 'change')
       OR a.effective_from >= (
           SELECT pg_catalog.max(c.effective_from) FROM current_settling_options c
           WHERE c.control_id = a.control_id AND c.source_stream = 'change')
       OR a.filed + 1 >= (
           SELECT pg_catalog.max(c.effective_from) FROM current_settling_options c
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
        SELECT pg_catalog.count(DISTINCT (a.family_num, a.family_den)) AS settlement_count,
               pg_catalog.min(a.effective_from) AS settled_from,
               pg_catalog.min(a.family_num) AS settled_num, pg_catalog.min(a.family_den) AS settled_den,
               pg_catalog.array_agg(DISTINCT a.id) AS settlement_ids
        FROM current_settling_assertions a
        WHERE a.control_id = m.id AND (a.effective_to IS NULL OR a.effective_to > p_effective_on)
    ) s
), active_controls AS MATERIALIZED (
    SELECT c.* FROM control_settlement c
    WHERE c.effective_from <= p_effective_on
      AND (c.settlement_count <> 1 OR c.settled_from > p_effective_on)
), settled_registrations AS MATERIALIZED (
    SELECT c.adsh, c.filed, c.underlying_class, c.family_num, c.family_den,
           pg_catalog.min(c.settled_from) AS operative_from
    FROM control_settlement c
    WHERE c.source_kind = 'f6' AND c.operative_date_conflict IS TRUE
    GROUP BY c.adsh, c.filed, c.underlying_class, c.family_num, c.family_den
    HAVING pg_catalog.bool_and(c.settlement_count = 1
                    AND c.settled_num * c.ratio_denominator = c.ratio_numerator * c.settled_den)
       AND pg_catalog.count(DISTINCT c.settled_from) = 1
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
          WHERE t.symbol_key = pg_catalog.regexp_replace(pg_catalog.upper(p_symbol), '[^A-Z0-9]', '', 'g')
            AND t.underlying_class <> c.underlying_class)
      AND (c.symbol_key = pg_catalog.regexp_replace(pg_catalog.upper(p_symbol), '[^A-Z0-9]', '', 'g')
           OR (c.symbol IS NULL
               AND b.symbol_key = pg_catalog.regexp_replace(pg_catalog.upper(p_symbol), '[^A-Z0-9]', '', 'g')
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
          WHERE t.symbol_key = pg_catalog.regexp_replace(pg_catalog.upper(p_symbol), '[^A-Z0-9]', '', 'g')
            AND t.underlying_class <> c.underlying_class)
), visible AS MATERIALIZED (
    SELECT v.* FROM operative_facts v CROSS JOIN issuer_binding b
    WHERE v.operative_from <= p_effective_on AND NOT (
        v.source_kind = 'f6'
        AND v.family_pending
        AND NOT EXISTS (
            SELECT 1 FROM announced_changes c
            WHERE c.effective_from <= p_effective_on
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
                          HAVING pg_catalog.count(DISTINCT p.ratio_change_program_key) > 1
                             OR (v.ratio_change_program_key IS NOT NULL
                                 AND coalesce(pg_catalog.bool_or(p.ratio_change_program_key <> v.ratio_change_program_key), false))
                             OR (c.ratio_change_program_key IS NOT NULL
                                 AND coalesce(pg_catalog.bool_or(p.ratio_change_program_key <> c.ratio_change_program_key), false))))
        )
    )) AND NOT (
        v.source_kind = 'f6' AND NOT v.effective_date_explicit
        AND v.effective_from = v.filed + 1 AND v.ordinary_candidate
        AND (v.underlying_class IS NULL OR b.underlying_class IS NULL
             OR v.underlying_class = b.underlying_class)
        AND NOT EXISTS (
            SELECT 1 FROM issuer_cover t
            WHERE t.symbol_key = pg_catalog.regexp_replace(pg_catalog.upper(p_symbol), '[^A-Z0-9]', '', 'g')
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
             HAVING (pg_catalog.count(DISTINCT c.effective_from) = 1 OR pg_catalog.bool_or(c.conservative_only))
                AND pg_catalog.min(c.effective_from) > p_effective_on)
    )
), types AS (
    SELECT c.* FROM issuer_cover c
    WHERE c.symbol_key = pg_catalog.regexp_replace(pg_catalog.upper(p_symbol), '[^A-Z0-9]', '', 'g')
), type_state AS (
    SELECT CASE WHEN pg_catalog.count(DISTINCT t.listed_type) > 1
                     OR pg_catalog.count(DISTINCT t.underlying_class) > 1 THEN 'ambiguous'
                WHEN pg_catalog.min(t.listed_type) IN ('ads', 'ordinary_direct') THEN 'resolved'
                ELSE 'none' END AS state,
           CASE WHEN pg_catalog.count(DISTINCT t.listed_type) = 1
                THEN pg_catalog.min(t.listed_type) END AS kind,
           CASE WHEN pg_catalog.count(DISTINCT t.underlying_class) = 1
                THEN pg_catalog.min(t.underlying_class) END AS underlying_class,
           pg_catalog.max(t.effective_from) AS effective_from,
           pg_catalog.min(t.effective_to) AS effective_to
    FROM types t
), changes AS (
    SELECT v.* FROM visible v
    WHERE v.evidence_kind = 'ads_ratio' AND v.source_stream = 'change'
      AND v.operative_from = (
          SELECT pg_catalog.max(c.operative_from) FROM visible c
          WHERE c.evidence_kind = 'ads_ratio' AND c.source_stream = 'change')
), latest_ratios AS (
    SELECT v.* FROM visible v
    WHERE v.evidence_kind = 'ads_ratio'
      AND CASE WHEN v.source_stream = 'change' THEN v.operative_from
               ELSE v.filed + 1 END = (
          SELECT pg_catalog.max(CASE WHEN s.source_stream = 'change' THEN s.operative_from
                          ELSE s.filed + 1 END) FROM visible s
          WHERE s.evidence_kind = 'ads_ratio' AND s.source_stream = v.source_stream)
), ratio_candidates AS (
    SELECT v.* FROM latest_ratios v
    WHERE (
          NOT EXISTS (SELECT 1 FROM changes)
          OR v.operative_from >= (SELECT pg_catalog.max(c.operative_from) FROM changes c)
          OR (v.source_stream <> 'change'
              AND v.filed + 1 >= (SELECT pg_catalog.max(c.operative_from) FROM changes c))
          OR EXISTS (
              SELECT 1 FROM changes c
              WHERE v.ratio_numerator * c.ratio_denominator
                  = c.ratio_numerator * v.ratio_denominator)
      )
), ratios AS (
    SELECT r.*,
           r.ratio_numerator / pg_catalog.gcd(r.ratio_numerator, r.ratio_denominator) AS num,
           r.ratio_denominator / pg_catalog.gcd(r.ratio_numerator, r.ratio_denominator) AS den
    FROM ratio_candidates r
    WHERE r.effective_to IS NULL OR r.effective_to > p_effective_on
), ratio_state AS (
    SELECT CASE WHEN EXISTS (SELECT 1 FROM active_controls) THEN 'ambiguous'
                WHEN pg_catalog.count(DISTINCT (r.num, r.den)) > 1
                     OR pg_catalog.count(DISTINCT r.underlying_class) > 1
                     OR pg_catalog.bool_or(r.underlying_class <> (
                         SELECT t.underlying_class FROM type_state t)) THEN 'ambiguous'
                WHEN pg_catalog.bool_or(r.source_stream = 'f6')
                     AND (pg_catalog.bool_or(r.source_stream = 'cover')
                          OR pg_catalog.bool_or(r.source_stream = 'change')) THEN 'resolved'
                ELSE 'none' END AS state,
           pg_catalog.min(r.num) AS num, pg_catalog.min(r.den) AS den,
           CASE WHEN pg_catalog.count(DISTINCT r.underlying_class) = 1
                THEN pg_catalog.min(r.underlying_class) END AS underlying_class,
           CASE WHEN pg_catalog.count(DISTINCT r.ratio_change_program_key) = 1
                THEN pg_catalog.min(r.ratio_change_program_key) END AS program_key,
           pg_catalog.max(r.operative_from) AS effective_from,
           pg_catalog.min(r.effective_to) AS effective_to,
           pg_catalog.count(DISTINCT r.ratio_change_program_key) > 1 AS program_ambiguous
    FROM ratios r
), result AS (
    SELECT t.state AS listing_state, t.kind,
           CASE WHEN t.state = 'resolved' AND t.kind = 'ordinary_direct'
                THEN 'resolved' ELSE r.state END AS ratio_state,
           CASE WHEN t.state = 'resolved' AND t.kind = 'ordinary_direct'
                THEN 1::numeric WHEN r.state = 'resolved' THEN r.num END AS num,
           CASE WHEN t.state = 'resolved' AND t.kind = 'ordinary_direct'
                THEN 1::numeric WHEN r.state = 'resolved' THEN r.den END AS den,
           t.underlying_class AS listing_class,
           CASE WHEN t.state = 'resolved' AND t.kind = 'ordinary_direct'
                THEN t.underlying_class ELSE r.underlying_class END AS ratio_class,
           CASE WHEN t.kind IS DISTINCT FROM 'ordinary_direct'
                THEN r.program_key END AS program_key,
           CASE WHEN t.state = 'resolved' AND t.kind = 'ordinary_direct'
                THEN t.effective_from ELSE r.effective_from END AS ratio_effective_from,
           CASE WHEN t.state = 'resolved' AND t.kind = 'ordinary_direct'
                THEN t.effective_to ELSE r.effective_to END AS ratio_effective_to,
           CASE WHEN t.state = 'resolved' AND t.kind = 'ordinary_direct'
                THEN false ELSE r.program_ambiguous END AS program_ambiguous
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
           WHERE c.settlement_count = 1 AND c.effective_from <= p_effective_on
             AND x.kind IS DISTINCT FROM 'ordinary_direct'
           UNION ALL
           SELECT pg_catalog.unnest(c.settlement_ids) FROM control_settlement c
           WHERE c.effective_from <= p_effective_on AND x.kind IS DISTINCT FROM 'ordinary_direct'
       ) q ORDER BY q.id),
       x.listing_class, x.ratio_class, x.program_key,
       x.ratio_effective_from, x.ratio_effective_to, x.program_ambiguous
FROM result x
$fn$;

-- Required public 12-column context ABI; internal diagnostics stay internal.
CREATE OR REPLACE FUNCTION public.sec_foreign_listing_context_at(
    p_cik bigint, p_symbol text, p_effective_on date, p_known_on date
)
RETURNS TABLE (
    status text,
    listed_type text,
    ratio_numerator numeric,
    ratio_denominator numeric,
    listing_status text,
    ratio_status text,
    evidence_ids bigint[],
    listing_class text,
    ratio_class text,
    program_key text,
    ratio_effective_from date,
    ratio_effective_to date
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT r.status, r.listed_type, r.ratio_numerator, r.ratio_denominator,
       r.listing_status, r.ratio_status, r.evidence_ids,
       r.listing_class, r.ratio_class, r.program_key,
       r.ratio_effective_from, r.ratio_effective_to
FROM public.sec_foreign_listing_election_at(p_cik, p_symbol, p_effective_on, p_known_on) r
$fn$;

-- Legacy ABI: one projection, with no second resolver or function SET.
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
AS $fn$
SELECT r.status, r.listed_type, r.ratio_numerator, r.ratio_denominator,
       r.listing_status, r.ratio_status, r.evidence_ids
FROM public.sec_foreign_listing_context_at(p_cik, p_symbol, p_as_of, p_as_of) r
$fn$;

COMMENT ON FUNCTION public.sec_foreign_listing_election_at(bigint, text, date, date) IS
    'Single internal W1c v2 election with economic/knowledge dates, elected class/program/interval and independent competing-program diagnostic. Audit IDs also include controls. No function SET; SQL-inlinable.';
COMMENT ON FUNCTION public.sec_foreign_listing_context_at(bigint, text, date, date) IS
    'Required 12-column W1c context ABI projected from the single internal election. Competing programs remain a sizing diagnostic without changing v2 legacy answers. No function SET; SQL-inlinable.';
COMMENT ON FUNCTION public.sec_foreign_listing_at(bigint, text, date) IS
    'Seven-column W1c v2 ABI projected from sec_foreign_listing_context_at at D,D; unchanged v2 evidence answers.';

-- Shared W1 election. Eligibility is selected before the common election;
-- legacy policies remain byte-for-byte equivalent in their matching predicates.
CREATE OR REPLACE FUNCTION public.sec_foreign_class_key(p_class text)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
SELECT public.sec_first_label(regexp_replace(p_class, '_|:', ' ', 'g'))
$fn$;

CREATE OR REPLACE FUNCTION public.sec_cover_share_election_at(
    p_cik bigint, p_ticker text, p_class_key text, p_line_members text[],
    p_as_of date, p_selection text, p_underlying_label text
)
RETURNS TABLE (
    source_status text, shares numeric, shares_as_of date, adsh text,
    basis text, legacy_refusal text, count_class_key text, count_label text,
    share_unit text, sole_class_proven boolean, exchange_name text,
    identity_ambiguous boolean, evidence jsonb
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH candidates AS NOT MATERIALIZED (
    SELECT c.adsh, c.stated_on, c.available_on, c.accepted, c.shares,
           c.class_key, c.form,
           CASE WHEN p_selection = 'sizing' THEN c.fact_hash END AS fact_hash,
           CASE WHEN p_selection = 'sizing' THEN c.source_package END AS source_package,
           CASE WHEN c.class_key = '' THEN 'sole_class_total' ELSE 'class' END AS basis,
           CASE WHEN p_selection = 'class' AND p_class_key = ''
                          AND regexp_replace(c.form, '/A$', '') IN ('20-F', '40-F', '6-K', '20-FR')
                     THEN 'foreign_issuer_listing_unverified'
                WHEN p_selection = 'ticker'
                     AND regexp_replace(c.form, '/A$', '') IN ('20-F', '40-F', '6-K', '20-FR')
                     AND NOT (c.class_key <> '' AND l.depositary)
                     THEN 'foreign_issuer_listing_unverified'
                WHEN p_selection = 'sizing' AND c.class_key <> ''
                     AND c.class_key ~* '(deposit[ao]ry|\mads|\madrs?([0-9]|member|;|$))'
                     THEN 'ordinary_class_shares_unavailable'
           END AS policy_refusal
    FROM public.sec_share_counts_at(p_as_of, false) c
    LEFT JOIN LATERAL (
        SELECT count(*) > 0 AS bound, bool_or(o.security_kind = 'depositary'
                       AND c.class_key ~* '(deposit[ao]ry|\mads|\madrs?([0-9]|member|;|$))') AS depositary
        FROM public.sec_observations_at(p_as_of, false) o
        WHERE o.adsh = c.adsh AND o.cik = c.cik AND o.available_on <= p_as_of
          AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
          AND CASE WHEN p_selection = 'ticker' THEN
                       CASE WHEN c.class_key <> '' THEN o.class_key = c.class_key
                            ELSE o.filing_equity_classes = 1
                                 AND o.security_kind IN ('equity', 'unknown') END
                   WHEN p_selection = 'sizing' THEN
                       o.class_key = ANY(p_line_members)
                   ELSE false END
    ) l ON true
    WHERE c.cik = p_cik AND c.stated_on <= p_as_of
      AND (p_selection = 'class' OR l.bound)
      AND CASE WHEN p_selection = 'class' THEN c.class_key = p_class_key
               WHEN p_selection = 'ticker' THEN true
               WHEN p_selection = 'sizing' THEN
                   c.class_key = ''
                   OR EXISTS (
                       SELECT 1 FROM public.sec_observations_at(p_as_of, false) o
                       WHERE o.adsh = c.adsh AND o.cik = c.cik
                         AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
                         AND o.class_key = ANY(p_line_members) AND o.class_key = c.class_key)
                   OR public.sec_class_label(NULL, c.class_key) = p_underlying_label
                   OR EXISTS (
                       SELECT 1 FROM public.sec_observations_at(p_as_of, false) o
                       WHERE o.adsh = c.adsh AND o.cik = c.cik AND o.class_key = c.class_key
                         AND public.sec_class_label(o.security_title, o.class_key) = p_underlying_label)
                   -- Preserve a contradictory newest filing as a candidate:
                   -- a missing bound class there cannot rescue an older one.
                   OR (p_underlying_label IS NOT NULL AND NOT EXISTS (
                       SELECT 1 FROM public.sec_share_counts_at(p_as_of, false) bound_count
                       WHERE bound_count.cik = c.cik AND bound_count.adsh = c.adsh
                         AND bound_count.stated_on <= p_as_of AND bound_count.class_key <> ''
                         AND (public.sec_class_label(NULL, bound_count.class_key) = p_underlying_label
                              OR EXISTS (
                                  SELECT 1 FROM public.sec_observations_at(p_as_of, false) named
                                  WHERE named.cik = c.cik AND named.adsh = c.adsh
                                    AND named.class_key = bound_count.class_key
                                    AND public.sec_class_label(named.security_title, named.class_key) = p_underlying_label))))
                   OR (p_underlying_label IS NULL AND NOT EXISTS (
                       SELECT 1 FROM public.sec_share_counts_at(p_as_of, false) own_count
                       WHERE own_count.cik = c.cik AND own_count.adsh = c.adsh
                         AND own_count.class_key = ANY(p_line_members)))
               ELSE false END
), chosen AS (
    SELECT k.* FROM candidates k
    ORDER BY k.stated_on DESC, k.available_on DESC, k.accepted DESC NULLS LAST,
             k.adsh COLLATE "C" DESC, k.basis COLLATE "C",
             CASE WHEN p_selection <> 'class' THEN k.policy_refusal IS NOT NULL END
    LIMIT 1
), elected AS NOT MATERIALIZED (
    SELECT k.* FROM candidates k, chosen h
    WHERE k.adsh = h.adsh AND k.stated_on = h.stated_on AND k.basis = h.basis
      AND (p_selection = 'class' OR k.policy_refusal IS NOT DISTINCT FROM h.policy_refusal)
), count_values AS (
    SELECT DISTINCT k.shares FROM elected k
), count_result AS (
    -- Return aggregate Vars so the inlined legacy status/share projections
    -- reuse one count, rather than duplicating scalar count InitPlans.
    SELECT count(*) AS value_count, min(k.shares) AS sole_value FROM count_values k
)
SELECT CASE WHEN h.adsh IS NULL THEN 'missing'
            WHEN v.value_count > 1 THEN 'ambiguous'
            ELSE 'resolved' END,
       CASE WHEN v.value_count = 1 THEN v.sole_value END,
       h.stated_on, h.adsh, h.basis, h.policy_refusal, h.class_key,
       NULL::text, NULL::text, false, NULL::text,
       CASE WHEN p_selection = 'sizing'
            THEN (SELECT count(DISTINCT e.class_key) FROM elected e) > 1 ELSE false END,
       CASE WHEN p_selection = 'sizing' THEN jsonb_build_object(
           'chosen_fact_hash', h.fact_hash,
           'count_fact_hashes', COALESCE((SELECT jsonb_agg(DISTINCT e.fact_hash ORDER BY e.fact_hash) FROM elected e), '[]'::jsonb),
           'count_source_packages', COALESCE((SELECT jsonb_agg(DISTINCT e.source_package ORDER BY e.source_package) FROM elected e), '[]'::jsonb),
           'count_class_keys', COALESCE((SELECT jsonb_agg(DISTINCT e.class_key ORDER BY e.class_key) FROM elected e), '[]'::jsonb))
            ELSE NULL::jsonb END
FROM count_result v LEFT JOIN chosen h ON true
$fn$;

-- Only the foreign sizing API asks for units, labels, filing scope, exchange and
-- audit JSON. Keep that work outside the shared count election so the existing
-- W1 class/ticker APIs do not plan or execute it on domestic equity requests.
CREATE OR REPLACE FUNCTION public.sec_cover_sizing_share_detail_at(
    p_cik bigint, p_ticker text, p_line_members text[], p_as_of date,
    p_underlying_label text
)
RETURNS TABLE (
    source_status text, shares numeric, shares_as_of date, adsh text,
    basis text, legacy_refusal text, count_class_key text, count_label text,
    share_unit text, sole_class_proven boolean, exchange_name text,
    identity_ambiguous boolean, evidence jsonb
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH election AS (
    SELECT e.* FROM public.sec_cover_share_election_at(
        p_cik, p_ticker, NULL, p_line_members, p_as_of, 'sizing', p_underlying_label
    ) e
), elected AS (
    SELECT c.*, labels.count_label, labels.label_ambiguous,
           labels.ordinary_unit, labels.preferred_unit
    FROM election e
    CROSS JOIN LATERAL public.sec_share_counts_at(p_as_of, false) c
    CROSS JOIN LATERAL (
        SELECT CASE WHEN count(DISTINCT x.label) = 1 THEN min(x.label COLLATE "C") END AS count_label,
               count(DISTINCT x.label) > 1 AS label_ambiguous,
               c.class_key ~* '(common|ordinary)'
                   OR COALESCE(bool_or(x.ordinary_unit), false) AS ordinary_unit,
               c.class_key ~* '(preferred|preference)'
                   OR COALESCE(bool_or(x.preferred_unit), false) AS preferred_unit
        FROM (
            SELECT public.sec_class_label(NULL, c.class_key) AS label,
                   false AS ordinary_unit, false AS preferred_unit
            UNION ALL
            SELECT public.sec_class_label(o.security_title, o.class_key),
                   o.security_kind = 'equity' AND o.security_title ~* '(common|ordinary)',
                   o.security_kind = 'preferred' OR o.security_title ~* '(preferred|preference)'
            FROM public.sec_observations_at(p_as_of, false) o
            WHERE o.adsh = c.adsh AND o.cik = c.cik AND o.class_key = c.class_key
        ) x
    ) labels
    WHERE c.cik = p_cik AND c.adsh = e.adsh AND c.stated_on = e.shares_as_of
      AND e.evidence -> 'count_fact_hashes' ? c.fact_hash
), census AS (
    SELECT COALESCE(bool_and(o.filing_equity_classes = 1 AND o.filing_complete), false) AS one_class,
           CASE WHEN count(DISTINCT public.sec_class_label(o.security_title, o.class_key)) = 1
                THEN min(public.sec_class_label(o.security_title, o.class_key) COLLATE "C") END AS sole_label
    FROM public.sec_observations_at(p_as_of, false) o, election e
    WHERE o.adsh = e.adsh AND o.cik = p_cik
), count_census AS (
    SELECT count(DISTINCT c.class_key) AS count_classes
    FROM public.sec_share_counts_at(p_as_of, false) c, election e
    WHERE c.adsh = e.adsh AND c.cik = p_cik
), chosen_detail AS (
    SELECT c.* FROM elected c, election e
    WHERE c.fact_hash = e.evidence ->> 'chosen_fact_hash'
    LIMIT 1
), detail AS (
    SELECT e.*, c.form,
           COALESCE(c.count_label,
                    CASE WHEN e.count_class_key = '' THEN s.sole_label END) AS label,
           e.count_class_key <> ''
               AND e.count_class_key ~* '(deposit[ao]ry|\mads|\madrs?([0-9]|member|;|$))' AS ads_unit,
           c.preferred_unit, c.ordinary_unit,
           s.one_class AND k.count_classes = 1
               AND regexp_replace(c.form, '/A$', '') = '20-F' AS sole_proven,
           e.identity_ambiguous
               OR COALESCE((SELECT bool_or(x.label_ambiguous) FROM elected x), false)
               OR (SELECT count(DISTINCT x.count_label) FROM elected x) > 1 AS different_classes,
           s.one_class, k.count_classes
    FROM election e LEFT JOIN chosen_detail c ON true
    CROSS JOIN census s CROSS JOIN count_census k
), exchange_fact AS (
    SELECT CASE WHEN count(DISTINCT o.exchange) = 1 THEN min(o.exchange COLLATE "C") END AS exchange_name
    FROM public.sec_observations_at(p_as_of, false) o, election e
    WHERE o.cik = p_cik AND o.adsh = e.adsh
      AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
      AND o.class_key = ANY(p_line_members)
)
SELECT d.source_status, d.shares, d.shares_as_of, d.adsh, d.basis,
       d.legacy_refusal, d.count_class_key, d.label,
       CASE WHEN d.ads_unit THEN 'ads' WHEN d.preferred_unit THEN 'preferred'
            WHEN d.ordinary_unit OR (d.count_class_key = '' AND d.sole_proven) THEN 'ordinary'
            WHEN d.adsh IS NOT NULL THEN 'unknown' END,
       COALESCE(d.sole_proven, false), x.exchange_name, COALESCE(d.different_classes, false),
       d.evidence || jsonb_build_object(
           'count_labels', COALESCE((SELECT jsonb_agg(DISTINCT e.count_label ORDER BY e.count_label)
                                    FILTER (WHERE e.count_label IS NOT NULL) FROM elected e), '[]'::jsonb),
           'count_labels_ambiguous', COALESCE(d.different_classes, false),
           'filing_equity_classes_one', d.one_class,
           'filing_count_classes', d.count_classes,
           'count_form', d.form,
           'efm_per_class_mandatory', regexp_replace(d.form, '/A$', '') = '20-F')
FROM detail d CROSS JOIN exchange_fact x
$fn$;

-- Both legacy functions use the same selected-source election and keep their
-- original foreign-policy / age / ambiguity precedence and return signatures.
CREATE OR REPLACE FUNCTION public.sec_cover_class_shares_at(
    p_cik bigint, p_class_key text, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (status text, shares numeric, shares_as_of date, adsh text, refusal text)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT CASE WHEN e.source_status = 'missing' THEN 'missing'
            WHEN e.legacy_refusal IS NOT NULL THEN 'refused'
            WHEN e.shares_as_of < p_as_of - p_max_age_days THEN 'stale'
            WHEN e.source_status = 'ambiguous' THEN 'ambiguous'
            ELSE 'resolved' END,
       CASE WHEN e.legacy_refusal IS NULL THEN e.shares END,
       e.shares_as_of, e.adsh, e.legacy_refusal
FROM public.sec_cover_share_election_at(p_cik, NULL, p_class_key, NULL, p_as_of, 'class', NULL) e
$fn$;

CREATE OR REPLACE FUNCTION public.sec_cover_ticker_shares_at(
    p_ticker text, p_cik bigint, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (status text, shares numeric, shares_as_of date, adsh text, basis text, refusal text)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT CASE WHEN e.source_status = 'missing' THEN 'missing'
            WHEN e.legacy_refusal IS NOT NULL THEN 'refused'
            WHEN e.shares_as_of < p_as_of - p_max_age_days THEN 'stale'
            WHEN e.source_status = 'ambiguous' THEN 'ambiguous'
            ELSE 'resolved' END,
       CASE WHEN e.legacy_refusal IS NULL THEN e.shares END,
       e.shares_as_of, e.adsh, e.basis, e.legacy_refusal
FROM public.sec_cover_share_election_at(p_cik, p_ticker, NULL, NULL, p_as_of, 'ticker', NULL) e
$fn$;

CREATE OR REPLACE FUNCTION public.sec_cover_ticker_size_basis_at(
    p_ticker text, p_cik bigint, p_line_members text[], p_as_of date,
    p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    status text, refusal text, ordinary_shares numeric, shares_as_of date,
    adsh text, count_class_key text, canonical_underlying_class_id text,
    share_unit text, basis text, class_binding text, listed_type text,
    ratio_numerator numeric, ratio_denominator numeric,
    count_ratio_numerator numeric, count_ratio_denominator numeric,
    listing_status text, ratio_status text, program_key text,
    exchange_name text, evidence jsonb
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH listing AS (
    SELECT l.*, public.sec_foreign_class_key(l.ratio_class) AS ratio_label,
           public.sec_foreign_class_key(l.listing_class) AS listing_label
    FROM public.sec_foreign_listing_election_at(p_cik, p_ticker, p_as_of, p_as_of) l
), selected AS (
    SELECT l.*, e.*,
           e.basis = 'class'
               OR e.basis = 'sole_class_total' AND e.sole_class_proven AS class_proof,
           CASE WHEN e.identity_ambiguous THEN 'ambiguous'
                WHEN e.count_label IS NOT NULL
                     AND ((l.ratio_label IS NOT NULL AND l.ratio_label <> e.count_label)
                          OR (l.listing_label IS NOT NULL AND l.listing_label <> e.count_label))
                     THEN 'mismatch'
                WHEN e.count_label IS NOT NULL
                     AND (CASE WHEN l.listed_type = 'ads' THEN l.ratio_label ELSE l.listing_label END) = e.count_label
                     THEN 'explicit'
                WHEN e.sole_class_proven THEN 'sole_ordinary_class_proven'
                ELSE 'ambiguous' END AS binding
    FROM listing l CROSS JOIN LATERAL public.sec_cover_sizing_share_detail_at(
        p_cik, p_ticker, p_line_members, p_as_of,
        CASE WHEN l.listed_type = 'ads' THEN l.ratio_label ELSE l.listing_label END
    ) e
), decided AS (
    SELECT s.*, CASE
      WHEN s.source_status = 'missing' THEN 'class_shares_unavailable'
      WHEN s.source_status IS DISTINCT FROM 'resolved' THEN 'ambiguous'
      WHEN s.shares IS NULL OR s.shares <= 0 OR s.shares::text IN ('NaN', 'Infinity', '-Infinity')
        THEN 'nonpositive_share_count'
      WHEN s.class_proof IS DISTINCT FROM true THEN 'share_total_class_scope_unverified'
      WHEN s.share_unit = 'ads' THEN 'ordinary_class_shares_unavailable'
      WHEN s.share_unit IS DISTINCT FROM 'ordinary' THEN 'share_count_unit_unverified'
      WHEN s.listing_status = 'ambiguous' OR s.ratio_status = 'ambiguous' THEN 'foreign_listing_ambiguous'
      WHEN s.listing_status IS DISTINCT FROM 'resolved' THEN 'foreign_issuer_listing_unverified'
      WHEN s.listed_type = 'ads' AND s.ratio_status IS DISTINCT FROM 'resolved' THEN 'depositary_ratio_unsourced'
      WHEN s.ratio_status IS DISTINCT FROM 'resolved' OR s.ratio_numerator IS NULL OR s.ratio_denominator IS NULL
           OR s.ratio_numerator <= 0 OR s.ratio_denominator <= 0
           OR s.ratio_numerator::text IN ('NaN', 'Infinity', '-Infinity')
           OR s.ratio_denominator::text IN ('NaN', 'Infinity', '-Infinity') THEN 'depositary_ratio_invalid'
      WHEN s.binding = 'mismatch' THEN 'foreign_listing_class_mismatch'
      WHEN s.program_ambiguous IS DISTINCT FROM false THEN 'foreign_listing_class_ambiguous'
      WHEN s.binding NOT IN ('explicit', 'sole_ordinary_class_proven') THEN 'foreign_listing_class_ambiguous'
      WHEN p_as_of IS NULL OR p_max_age_days IS NULL OR p_max_age_days < 0
           OR s.shares_as_of < p_as_of - p_max_age_days THEN 'stale'
      END AS refusal_code
    FROM selected s
), historical AS (
    SELECT d.*,
           CASE WHEN c.program_ambiguous IS NOT DISTINCT FROM false THEN c.ratio_numerator END AS count_numerator,
           CASE WHEN c.program_ambiguous IS NOT DISTINCT FROM false THEN c.ratio_denominator END AS count_denominator,
           c.ratio_numerator AS raw_count_numerator, c.ratio_denominator AS raw_count_denominator,
           c.status AS count_listing_contract_status, c.ratio_status AS count_ratio_status,
           c.evidence_ids AS count_evidence_ids, c.ratio_class AS count_ratio_class,
           c.program_key AS count_program_key, c.program_ambiguous AS count_program_ambiguous
    FROM decided d CROSS JOIN LATERAL public.sec_foreign_listing_election_at(
        p_cik, p_ticker, d.shares_as_of, p_as_of
    ) c
), exchange_filing AS (
    SELECT o.adsh, o.source_available_on
    FROM public.sec_observations_at(p_as_of, false) o
    WHERE o.cik = p_cik
      AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
      AND o.class_key = ANY(p_line_members)
    ORDER BY o.source_available_on DESC, o.available_on DESC,
             o.accepted DESC NULLS LAST, o.adsh COLLATE "C" DESC
    LIMIT 1
), exchange_fact AS (
    SELECT CASE WHEN count(DISTINCT o.exchange) = 1 AND bool_and(o.exchange IS NOT NULL)
                THEN min(o.exchange COLLATE "C") END AS exchange_name,
           count(DISTINCT o.exchange) > 1 AS exchange_ambiguous,
           (SELECT x.adsh FROM exchange_filing x) AS exchange_adsh,
           (SELECT x.source_available_on FROM exchange_filing x) AS exchange_available_on
    FROM public.sec_observations_at(p_as_of, false) o CROSS JOIN exchange_filing x
    WHERE o.cik = p_cik AND o.adsh = x.adsh
      AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
      AND o.class_key = ANY(p_line_members)
)
SELECT CASE WHEN h.refusal_code IS NULL THEN 'resolved'
            WHEN h.refusal_code = 'stale' THEN 'stale'
            WHEN h.refusal_code IN ('ambiguous', 'foreign_listing_ambiguous') THEN 'ambiguous'
            WHEN h.refusal_code = 'class_shares_unavailable' THEN 'missing'
            ELSE 'refused' END,
       CASE h.refusal_code
         WHEN 'class_shares_unavailable' THEN format('class_shares_unavailable: %s has no public ordinary cover share count by %s', p_ticker, p_as_of)
         WHEN 'ambiguous' THEN format('ambiguous: %s has conflicting share counts from %s', p_ticker, h.adsh)
         WHEN 'nonpositive_share_count' THEN format('nonpositive_share_count: %s count from %s is not positive and finite', p_ticker, h.adsh)
         WHEN 'share_total_class_scope_unverified' THEN format('share_total_class_scope_unverified: %s total from %s is not proven to cover only class %s', p_ticker, h.adsh, COALESCE(h.ratio_label, h.listing_label, 'unknown'))
         WHEN 'ordinary_class_shares_unavailable' THEN format('ordinary_class_shares_unavailable: %s has no verified outstanding ordinary count for class %s by %s', p_ticker, COALESCE(h.ratio_label, 'unknown'), p_as_of)
         WHEN 'share_count_unit_unverified' THEN format('share_count_unit_unverified: %s count from %s has no verified ordinary unit basis', p_ticker, h.adsh)
         WHEN 'foreign_listing_ambiguous' THEN format('foreign_listing_ambiguous: %s listing or ADS ratio is ambiguous at %s (listing %s, ratio %s)', p_ticker, p_as_of, h.listing_status, h.ratio_status)
         WHEN 'foreign_issuer_listing_unverified' THEN format('foreign_issuer_listing_unverified: %s has no foreign listing evidence public by %s', p_ticker, p_as_of)
         WHEN 'depositary_ratio_unsourced' THEN format('depositary_ratio_unsourced: %s is a depositary line of CIK %s and its ADS-to-share ratio is not sourced (%s)', p_ticker, p_cik, p_as_of)
         WHEN 'depositary_ratio_invalid' THEN format('depositary_ratio_invalid: %s has no valid ordinary-shares-per-ADS ratio at %s', p_ticker, p_as_of)
         WHEN 'foreign_listing_class_mismatch' THEN format('foreign_listing_class_mismatch: %s ratio is for class %s but its count from %s is class %s', p_ticker, COALESCE(h.ratio_label, h.listing_label), h.adsh, h.count_label)
         WHEN 'foreign_listing_class_ambiguous' THEN format('foreign_listing_class_ambiguous: %s has no unique ordinary class for its ratio and count at %s', p_ticker, p_as_of)
         WHEN 'stale' THEN format('stale: latest public class share count for %s as of %s is older than %s days', p_ticker, h.shares_as_of, p_max_age_days)
       END,
       CASE WHEN h.refusal_code IS NULL THEN h.shares END,
       h.shares_as_of, h.adsh, h.count_class_key,
       CASE WHEN h.binding IN ('explicit', 'sole_ordinary_class_proven')
            THEN COALESCE(h.count_label, h.ratio_label, h.listing_label, 'ordinary') END,
       h.share_unit, h.basis,
       CASE WHEN h.binding IN ('explicit', 'sole_ordinary_class_proven') THEN h.binding END,
       h.listed_type,
       CASE WHEN h.refusal_code IS NULL THEN h.ratio_numerator END,
       CASE WHEN h.refusal_code IS NULL THEN h.ratio_denominator END,
       CASE WHEN h.refusal_code IS NULL THEN h.count_numerator END,
       CASE WHEN h.refusal_code IS NULL THEN h.count_denominator END,
       h.listing_status, h.ratio_status, h.program_key, x.exchange_name,
       h.evidence || jsonb_build_object(
           'count_found', h.adsh IS NOT NULL,
           'count_non_stale', COALESCE(h.shares_as_of >= p_as_of - p_max_age_days, false),
           'class_proof', COALESCE(h.class_proof, false),
           'class_binding_valid', h.binding IN ('explicit', 'sole_ordinary_class_proven')
               AND h.program_ambiguous IS NOT DISTINCT FROM false,
           'class_binding_status', h.binding,
           'program_ambiguous', h.program_ambiguous,
           'exchange_ambiguous', x.exchange_ambiguous,
           'exchange_adsh', x.exchange_adsh, 'exchange_available_on', x.exchange_available_on,
           'listing_class', h.listing_class, 'ratio_class', h.ratio_class,
           'listing_ratio_numerator', h.ratio_numerator, 'listing_ratio_denominator', h.ratio_denominator,
           'ratio_effective_from', h.ratio_effective_from, 'ratio_effective_to', h.ratio_effective_to,
           'listing_evidence_ids', h.evidence_ids,
           'count_ratio_status', h.count_ratio_status,
           'count_listing_contract_status', h.count_listing_contract_status,
           'count_ratio_class', h.count_ratio_class, 'count_program_key', h.count_program_key,
           'count_program_ambiguous', h.count_program_ambiguous,
           'count_ratio_numerator', h.raw_count_numerator, 'count_ratio_denominator', h.raw_count_denominator,
           'count_listing_evidence_ids', h.count_evidence_ids,
           'phase', 1)
FROM historical h CROSS JOIN exchange_fact x
$fn$;


REVOKE ALL ON FUNCTION public.sec_foreign_listing_election_at(bigint, text, date, date),
    public.sec_foreign_listing_context_at(bigint, text, date, date),
    public.sec_foreign_listing_at(bigint, text, date),
    public.sec_foreign_class_key(text),
    public.sec_cover_share_election_at(bigint,text,text,text[],date,text,text),
    public.sec_cover_sizing_share_detail_at(bigint,text,text[],date,text),
    public.sec_cover_class_shares_at(bigint,text,date,integer),
    public.sec_cover_ticker_shares_at(text,bigint,date,integer),
    public.sec_cover_ticker_size_basis_at(text,bigint,text[],date,integer) FROM PUBLIC;
DO $privileges$
DECLARE
    reader text;
    function_signature text;
BEGIN
    FOREACH function_signature IN ARRAY ARRAY[
        'sec_foreign_listing_election_at(bigint,text,date,date)',
        'sec_foreign_listing_context_at(bigint,text,date,date)',
        'sec_foreign_listing_at(bigint,text,date)',
        'sec_foreign_class_key(text)',
        'sec_cover_share_election_at(bigint,text,text,text[],date,text,text)',
        'sec_cover_sizing_share_detail_at(bigint,text,text[],date,text)',
        'sec_cover_class_shares_at(bigint,text,date,integer)',
        'sec_cover_ticker_shares_at(text,bigint,date,integer)',
        'sec_cover_ticker_size_basis_at(text,bigint,text[],date,integer)'
    ] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'worker_writer') THEN
            EXECUTE pg_catalog.format('ALTER FUNCTION public.%s OWNER TO worker_writer', function_signature);
        END IF;
        FOREACH reader IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
            IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = reader) THEN
                EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION public.%s TO %I', function_signature, reader);
            END IF;
        END LOOP;
    END LOOP;
END
$privileges$;
COMMIT;
