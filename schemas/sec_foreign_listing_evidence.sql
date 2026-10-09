-- Evidence-only foreign listed security types and ordinary shares per ADS.
-- Owner-applied, additive migration; no W1 admission or sizing is changed.
-- Rollback: schemas/sec_foreign_listing_evidence.rollback.sql.
--
-- Two clocks follow W1: source_available_on is filed + 1, bounded below by any
-- later observed publication date of a replacement filing document. available_on is
-- source_available_on for the first source import and at least reconciliation
-- day for corrections. Retired versions remain queryable before retired_on.
-- Effective intervals are [effective_from, effective_to). Cover types begin
-- at filed + 1; a dated ratio-change announcement begins on its stated date,
-- and can only affect a query once its own availability date has arrived.
BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS public.sec_foreign_listing_sources (
    -- Stable document key; must not include parser version or content hash.
    source_package text PRIMARY KEY,
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    source_url text NOT NULL,
    source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    parser_version text NOT NULL,
    first_loaded_on date NOT NULL,
    last_loaded_on date NOT NULL,
    evidence_count integer NOT NULL CHECK (evidence_count >= 0),
    CHECK (last_loaded_on >= first_loaded_on)
);

CREATE TABLE IF NOT EXISTS public.sec_foreign_listing_evidence (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fact_hash text NOT NULL CHECK (fact_hash ~ '^[0-9a-f]{32}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    symbol text CHECK (symbol ~ '^[A-Z0-9]+(-[A-Z0-9]+)*$'),
    -- Match W1's separator-free aliases without picking a preferred spelling.
    symbol_key text GENERATED ALWAYS AS (replace(symbol, '-', '')) STORED,
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    form text NOT NULL,
    filed date NOT NULL,
    -- Some replacement accessions retain the original legal filing date in
    -- EDGAR indexes but have a later observed publication date. Never expose
    -- their replacement document before that later public date.
    publication_floor_on date,
    source_url text NOT NULL,
    source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    source_kind text NOT NULL CHECK (source_kind IN (
        'cover_12b', 'cover_footnote', 'item_12d', 'securities_description',
        'listing_description', 'f6', 'ratio_change_6k'
    )),
    evidence_kind text NOT NULL CHECK (evidence_kind IN ('listed_type', 'ads_ratio')),
    listed_type text CHECK (listed_type IN ('ads', 'ordinary_direct', 'unknown')),
    -- Explicit underlying class token only (for example class_a / class_b).
    -- NULL means the statement supplies no such token, not a generic class.
    underlying_class text,
    -- False only when the source explicitly identifies debt, rights, warrants,
    -- preferred equity or ADSs over preferred equity. Those cover rows remain
    -- auditable but do not compete with an ordinary-share ADS program.
    ordinary_candidate boolean NOT NULL DEFAULT true,
    -- Exact rational ordinary shares per ADS: 1 ADS = numerator / denominator
    -- ordinary shares. No floating-point arithmetic or decimal rounding.
    ratio_numerator numeric,
    ratio_denominator numeric,
    effective_from date NOT NULL,
    -- True only when the document itself states the extracted effective date;
    -- false when effective_from is the filing + 1 observation fallback.
    effective_date_explicit boolean NOT NULL DEFAULT false,
    -- A correction links one expressly replaced ratio-change event, never
    -- arbitrary announcements by filing order. Program identity is the exact
    -- source-labelled old CUSIP; the literal correction headline is retained.
    ratio_change_program_key text,
    ratio_change_correction_kind text,
    ratio_change_correction_text text,
    -- A contract expressly awaiting a Depositary-announced ratio date is not
    -- operative merely because the amended registration was filed.
    ratio_effectiveness_pending boolean DEFAULT false,
    ratio_effectiveness_pending_text text,
    ratio_effectiveness_conditions text[],
    -- Conflicting operative dates remain visible controls. Their numeric
    -- entitlement is not operative until a later authoritative source settles
    -- its clock; effective_from is the earliest disputed date.
    operative_date_conflict boolean DEFAULT false,
    operative_date_candidates date[],
    operative_date_conflict_text text,
    -- A completion, definitive depositary notice or actual approval result,
    -- rather than an unfulfilled conditional plan. Preserve the literal proof.
    ratio_effectiveness_confirmed boolean DEFAULT false,
    ratio_effectiveness_confirmation_text text,
    ratio_effectiveness_confirmed_conditions text[],
    effective_to date,
    evidence_text text NOT NULL CHECK (length(btrim(evidence_text)) > 0),
    evidence_location text NOT NULL CHECK (length(btrim(evidence_location)) > 0),
    parser_version text NOT NULL,
    source_available_on date GENERATED ALWAYS AS (greatest(filed + 1, publication_floor_on)) STORED,
    available_on date NOT NULL,
    retired_on date,
    loaded_on date NOT NULL,
    source_package text NOT NULL,
    CHECK (available_on >= greatest(filed + 1, publication_floor_on)),
    -- A filing loaded and corrected on its filing day may retire a version
    -- before filed + 1. Such a version was never visible; retain its real
    -- reconciliation date rather than postponing retirement into the future.
    CHECK (effective_to IS NULL OR effective_to > effective_from),
    CHECK (listed_type IS DISTINCT FROM 'ordinary_direct' OR ordinary_candidate),
    CHECK (
        (evidence_kind = 'listed_type' AND listed_type IS NOT NULL
         AND ratio_numerator IS NULL AND ratio_denominator IS NULL
         AND source_kind IN ('cover_12b', 'cover_footnote', 'listing_description')
         AND effective_from = filed + 1)
        OR
        (evidence_kind = 'ads_ratio' AND listed_type IS NULL
         AND source_kind <> 'listing_description'
         AND ratio_numerator IS NOT NULL AND ratio_denominator IS NOT NULL
         AND ratio_numerator > 0 AND ratio_numerator < 'Infinity'::numeric
         AND ratio_denominator > 0 AND ratio_denominator < 'Infinity'::numeric
         AND ratio_numerator = trunc(ratio_numerator)
         AND ratio_denominator = trunc(ratio_denominator))
    )
);

-- Additive and replayable for existing evidence-only installations.
ALTER TABLE public.sec_foreign_listing_evidence
    ADD COLUMN IF NOT EXISTS ratio_change_program_key text,
    ADD COLUMN IF NOT EXISTS ratio_change_correction_kind text,
    ADD COLUMN IF NOT EXISTS ratio_change_correction_text text,
    ADD COLUMN IF NOT EXISTS ratio_effectiveness_pending boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS ratio_effectiveness_pending_text text,
    ADD COLUMN IF NOT EXISTS ratio_effectiveness_conditions text[],
    ADD COLUMN IF NOT EXISTS operative_date_conflict boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS operative_date_candidates date[],
    ADD COLUMN IF NOT EXISTS operative_date_conflict_text text,
    ADD COLUMN IF NOT EXISTS ratio_effectiveness_confirmed boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS ratio_effectiveness_confirmation_text text,
    ADD COLUMN IF NOT EXISTS ratio_effectiveness_confirmed_conditions text[];
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.sec_foreign_listing_evidence'::regclass
          AND conname = 'sec_foreign_listing_ratio_program_ck'
    ) THEN
        ALTER TABLE public.sec_foreign_listing_evidence
            ADD CONSTRAINT sec_foreign_listing_ratio_program_ck CHECK (
                ratio_change_program_key IS NULL
                OR (ratio_change_program_key ~ '^old_cusip:[A-Z0-9]{9}$'
                    AND evidence_kind = 'ads_ratio' AND source_kind = 'ratio_change_6k'
                    AND form IN ('6-K', '6-K/A'))
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.sec_foreign_listing_evidence'::regclass
          AND conname = 'sec_foreign_listing_ratio_correction_ck'
    ) THEN
        ALTER TABLE public.sec_foreign_listing_evidence
            ADD CONSTRAINT sec_foreign_listing_ratio_correction_ck CHECK (
                (ratio_change_correction_kind IS NULL AND ratio_change_correction_text IS NULL)
                OR (ratio_change_correction_kind IS NOT NULL
                    AND ratio_change_correction_kind = 'correcting_and_replacing'
                    AND ratio_change_program_key IS NOT NULL
                    AND ratio_change_correction_text IS NOT NULL
                    AND length(btrim(ratio_change_correction_text)) > 0)
            );
    END IF;
    -- Replace the previous F-6-only check on replay: conditional 6-K notices
    -- use the same pending-effectiveness representation.
    ALTER TABLE public.sec_foreign_listing_evidence
        DROP CONSTRAINT IF EXISTS sec_foreign_listing_ratio_pending_ck;
    ALTER TABLE public.sec_foreign_listing_evidence
        ADD CONSTRAINT sec_foreign_listing_ratio_pending_ck CHECK (
            (NOT coalesce(ratio_effectiveness_pending, false)
                AND ratio_effectiveness_pending_text IS NULL
                AND ratio_effectiveness_conditions IS NULL)
            OR (ratio_effectiveness_pending IS TRUE
                AND ratio_effectiveness_pending_text IS NOT NULL
                AND length(btrim(ratio_effectiveness_pending_text)) > 0
                AND evidence_kind = 'ads_ratio'
                AND source_kind IN ('f6', 'ratio_change_6k')
                AND (source_kind = 'f6' OR (
                    effective_date_explicit
                    AND ratio_effectiveness_conditions IS NOT NULL
                    AND cardinality(ratio_effectiveness_conditions) > 0))
                AND (ratio_effectiveness_conditions IS NULL OR (
                    array_ndims(ratio_effectiveness_conditions) = 1
                    AND cardinality(ratio_effectiveness_conditions) > 0
                    AND array_position(ratio_effectiveness_conditions, NULL) IS NULL
                    AND ratio_effectiveness_conditions <@ ARRAY[
                        'shareholder_approval', 'consolidation', 'regulatory_approval',
                        'depositary_notice', 'other_approval', 'unknown_condition']::text[])))
        );
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.sec_foreign_listing_evidence'::regclass
          AND conname = 'sec_foreign_listing_operative_date_conflict_ck'
    ) THEN
        ALTER TABLE public.sec_foreign_listing_evidence
            ADD CONSTRAINT sec_foreign_listing_operative_date_conflict_ck CHECK (
                (NOT coalesce(operative_date_conflict, false)
                    AND operative_date_candidates IS NULL
                    AND operative_date_conflict_text IS NULL)
                OR (operative_date_conflict IS TRUE
                    AND evidence_kind = 'ads_ratio'
                    AND source_kind <> 'listing_description'
                    AND operative_date_candidates IS NOT NULL
                    AND array_ndims(operative_date_candidates) = 1
                    AND array_lower(operative_date_candidates, 1) = 1
                    AND cardinality(operative_date_candidates) >= 2
                    AND array_position(operative_date_candidates, NULL) IS NULL
                    AND operative_date_candidates[1] = effective_from
                    AND operative_date_candidates[1]
                        < operative_date_candidates[cardinality(operative_date_candidates)]
                    AND effective_date_explicit
                    AND operative_date_conflict_text IS NOT NULL
                    AND length(btrim(operative_date_conflict_text)) > 0)
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.sec_foreign_listing_evidence'::regclass
          AND conname = 'sec_foreign_listing_ratio_confirmation_ck'
    ) THEN
        ALTER TABLE public.sec_foreign_listing_evidence
            ADD CONSTRAINT sec_foreign_listing_ratio_confirmation_ck CHECK (
                (NOT coalesce(ratio_effectiveness_confirmed, false)
                    AND ratio_effectiveness_confirmation_text IS NULL
                    AND ratio_effectiveness_confirmed_conditions IS NULL)
                OR (ratio_effectiveness_confirmed IS TRUE
                    AND evidence_kind = 'ads_ratio'
                    AND source_kind IN ('f6', 'ratio_change_6k')
                    AND effective_date_explicit
                    AND NOT coalesce(ratio_effectiveness_pending, false)
                    AND NOT coalesce(operative_date_conflict, false)
                    AND ratio_effectiveness_confirmation_text IS NOT NULL
                    AND length(btrim(ratio_effectiveness_confirmation_text)) > 0
                    AND ratio_effectiveness_confirmed_conditions IS NOT NULL
                    AND array_ndims(ratio_effectiveness_confirmed_conditions) = 1
                    AND cardinality(ratio_effectiveness_confirmed_conditions) > 0
                    AND array_position(ratio_effectiveness_confirmed_conditions, NULL) IS NULL
                    AND ratio_effectiveness_confirmed_conditions <@ ARRAY[
                        'shareholder_approval', 'consolidation', 'regulatory_approval',
                        'depositary_notice', 'other_approval', 'ratio_effective']::text[])
            );
    END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS sec_foreign_listing_evidence_current_idx
    ON public.sec_foreign_listing_evidence (fact_hash) WHERE retired_on IS NULL;
CREATE INDEX IF NOT EXISTS sec_foreign_listing_evidence_line_idx
    ON public.sec_foreign_listing_evidence (cik, symbol_key, available_on, effective_from);
CREATE INDEX IF NOT EXISTS sec_foreign_listing_evidence_source_idx
    ON public.sec_foreign_listing_evidence (source_package, fact_hash);

-- Exactly one result, including for absent evidence. A direct ordinary listing
-- has the identity ratio 1/1; it does not need a depositary registration.
-- Cover statements lacking a symbol remain unbound (the loader may bind them
-- only through the same filing's W1 symbol evidence). F-6/6-K statements lacking
-- a literal trading symbol remain issuer-scoped.
-- They bind only to the unique ADS symbol in the latest issuer cover snapshot
-- visible at D, with no unknown competing lines. Multiple ADS programs require
-- explicit symbol evidence. No present-day symbol assignment is backfilled.
-- Explicit underlying classes scope issuer-only registrations. Class A and
-- class B registrations with equal numeric ratios are still distinct programs;
-- they cannot collapse into one answer when the cover cannot identify a class.
-- Ratio eligibility is restricted to ordinary deposited units before any
-- binding, correction, announcement, pending-contract or latest-filing logic.
-- This includes explicitly symbol-bound ratios: a preferred or CPO programme
-- cannot displace an ordinary registration or activate its future entitlement.
-- Nonordinary listed-type statements remain auditable, and an ended latest
-- ordinary assertion does not resurrect an earlier ordinary registration.
--
-- A cover is a dated statement about its listed line. Use the latest effective
-- cover date, retaining all ties (never a chosen accession).
-- A tightly symbol-bound Item 9 listing description is separate type-only
-- evidence on that same filing date. It never overrides a contradictory cover:
-- ordinary_direct versus ads in the same filing remains ambiguous.
-- Ratios are compared
-- across independent F-6, annual-report corroboration and 6-K streams. The
-- corroboration stream contains cover footnotes, Item 12.D and the annual
-- report's attached description of registered Section 12(b) securities. The
-- attachment contributes ratios only, explicitly identified by source kind;
-- it never determines a listed security's type. Unbound attachment ratios
-- cannot infer a ticker through the issuer-only F-6/6-K binding rule.
-- F-6 and annual-report streams contribute all assertions in their latest
-- filing, after their effective date has arrived. A later annual filing may
-- state an earlier ratio-change date; that is still the newer assertion.
-- Filing assertion order uses the legal filed date, not a replacement's later
-- publication floor: replacing an old report does not turn it into a new report.
-- The 6-K stream contributes the latest effective event, retaining every tie.
-- An explicit correcting-and-replacing announcement ends only the earlier
-- claim for the exact same old CUSIP, event date, symbol and underlying class,
-- after the correction becomes public. A later legal filing is required;
-- republication dates cannot promote an older claim. Unrelated and same-date
-- conflicting assertions remain independent. This replacement is applied to
-- known future announcements as well, before F-6 deferral and selection.
-- Repeated annual statements therefore
-- do not make a properly evidenced subsequent change ambiguous forever.
-- An ended latest assertion does not resurrect an older assertion in its stream.
--
-- A visible, effective 6-K change ends earlier ratios. An earlier registration
-- or cover matching the announced new ratio can still corroborate it (F-6 often
-- precedes the change); earlier *different* ratios describe the ended regime.
-- Differing assertions effective or filed on/after the change, or conflicting
-- announcements of the same change, are retained and return ambiguous. Future announcements neither
-- end earlier ratios nor leak into earlier answers.
-- An effective dated 6-K change can corroborate its matching F-6 registration
-- before the next annual cover is filed. Initial ratios still require annual
-- corroboration. A later contradicting annual assertion remains ambiguous.
-- A fallback-dated F-6 for an already-announced future change is deferred until
-- that change: the 6-K must already have been public by the F-6 publication,
-- identify the same program/class and exact ratio, and supply one unique future
-- date. This does not rewrite the source dates, move older registrations, or
-- override an F-6's own explicit date. Deferral precedes latest-filing selection.
-- An expressly pending F-6 entitlement requires a matching public dated change
-- on/after the registration, for the same bound program and exact underlying
-- class, before becoming operative. Unlike an ordinary filing-date fallback,
-- this explicit condition can be satisfied by later-announced date knowledge,
-- prospectively from its availability and effective date only. Pending contracts
-- are excluded before latest-filing selection, preserving the last operative
-- registration. Historical or unrelated matching ratios cannot activate them.
-- A literal pending condition in the operative contract governs matching ratio
-- facts throughout that same issuer/accession/class/symbol registration family;
-- its fee table or counsel opinion cannot bypass the contract's effective clause.
-- Contradictory operative dates and conditional 6-K changes remain source
-- controls, rather than disappearing or becoming filing-date entitlements.
-- Once their earliest stated date and public availability have arrived, an
-- unresolved control makes the ratio ambiguous, including against older
-- agreeing registration and annual streams. Conditional changes never activate
-- a pending contract. A later public definitive dated registration or confirmed
-- 6-K can settle only the same bound line, exact class and compatible program.
-- Legal assertion order and publication order must both advance; an old report
-- republished later cannot settle the uncertainty. Independent current settling
-- assertions retain ties and must agree on both operative date and exact ratio.
-- Conditional changes additionally require affirmative confirmation of every
-- named prerequisite, or explicit completion of the ratio itself. A merely
-- dated registration cannot prove shareholder, regulatory or depositary approval;
-- unnamed conditions require actual completion, not another generic approval.
-- A public conditional announcement still supplies a conservative future bound
-- for its matching fallback-dated registration. This deferral uses the same
-- public-by-registration binding, class and exact-ratio fences, and never makes
-- the conditional announcement an operative event or an activating notice.
-- A definitive future notice can settle uncertainty prospectively, while its
-- number still waits for that operative date. An exact-ratio confirmation can
-- establish the clock of a date-conflicted registration's existing numeric
-- entitlement; both source IDs remain visible. A later actual different-ratio
-- event may end an obsolete control without reusing its former proposed number.
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
WITH issuer_observed AS MATERIALIZED (
    SELECT e.*,
           CASE WHEN e.source_kind = 'f6' THEN 'f6'
                WHEN e.source_kind = 'ratio_change_6k' THEN 'change'
                ELSE 'cover' END AS source_stream
    FROM public.sec_foreign_listing_evidence e
    WHERE e.cik = p_cik
      AND (e.evidence_kind <> 'ads_ratio' OR e.ordinary_candidate)
      AND e.available_on <= p_as_of
      AND (e.retired_on IS NULL OR e.retired_on > p_as_of)
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
    'Bitemporal SEC annual-report/F-6/6-K evidence only; ordinary shares per ADS are exact rational numbers. Does not admit or size W1 lines.';
COMMENT ON TABLE public.sec_foreign_listing_sources IS
    'Latest document reconciliation metadata, including zero-fact parses; first-loaded date prevents later parser additions from being backdated.';
COMMENT ON FUNCTION public.sec_foreign_listing_at(bigint, text, date) IS
    'PIT foreign line evidence: resolved|ambiguous|none, with separate listing/ratio statuses and underlying evidence IDs. Direct ordinary lines return 1/1.';

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
