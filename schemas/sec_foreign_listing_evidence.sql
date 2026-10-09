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
        'f6', 'ratio_change_6k'
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
         AND source_kind IN ('cover_12b', 'cover_footnote')
         AND effective_from = filed + 1)
        OR
        (evidence_kind = 'ads_ratio' AND listed_type IS NULL
         AND ratio_numerator IS NOT NULL AND ratio_denominator IS NOT NULL
         AND ratio_numerator > 0 AND ratio_numerator < 'Infinity'::numeric
         AND ratio_denominator > 0 AND ratio_denominator < 'Infinity'::numeric
         AND ratio_numerator = trunc(ratio_numerator)
         AND ratio_denominator = trunc(ratio_denominator))
    )
);

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
--
-- A cover is a dated statement about its listed line. Use the latest effective
-- cover date, retaining all ties (never a chosen accession). Ratios are compared
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
WITH issuer_visible AS MATERIALIZED (
    SELECT e.*,
           CASE WHEN e.source_kind = 'f6' THEN 'f6'
                WHEN e.source_kind = 'ratio_change_6k' THEN 'change'
                ELSE 'cover' END AS source_stream
    FROM public.sec_foreign_listing_evidence e
    WHERE e.cik = p_cik
      AND e.available_on <= p_as_of
      AND (e.retired_on IS NULL OR e.retired_on > p_as_of)
      AND e.effective_from <= p_as_of
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
), visible AS MATERIALIZED (
    SELECT v.* FROM issuer_visible v CROSS JOIN issuer_binding b
    WHERE v.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
       OR (v.symbol IS NULL AND v.ordinary_candidate AND v.source_kind IN ('f6', 'ratio_change_6k')
           AND b.symbol_key = regexp_replace(upper(p_symbol), '[^A-Z0-9]', '', 'g')
           AND (v.underlying_class IS NULL OR b.underlying_class IS NULL
                OR v.underlying_class = b.underlying_class))
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
      AND v.effective_from = (
          SELECT max(c.effective_from) FROM visible c
          WHERE c.evidence_kind = 'ads_ratio' AND c.source_stream = 'change')
), latest_ratios AS (
    SELECT v.* FROM visible v
    WHERE v.evidence_kind = 'ads_ratio'
      AND CASE WHEN v.source_stream = 'change' THEN v.effective_from
               ELSE v.filed + 1 END = (
          SELECT max(CASE WHEN s.source_stream = 'change' THEN s.effective_from
                          ELSE s.filed + 1 END) FROM visible s
          WHERE s.evidence_kind = 'ads_ratio' AND s.source_stream = v.source_stream)
), ratio_candidates AS (
    SELECT v.* FROM latest_ratios v
    WHERE (
          NOT EXISTS (SELECT 1 FROM changes)
          OR v.effective_from >= (SELECT max(c.effective_from) FROM changes c)
          OR (v.source_stream <> 'change'
              AND v.filed + 1 >= (SELECT max(c.effective_from) FROM changes c))
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
    SELECT CASE WHEN count(DISTINCT (r.num, r.den)) > 1
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
