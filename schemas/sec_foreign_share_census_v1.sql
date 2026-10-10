-- Positive 20-F/40-F cover-statement share-class census; evidence only.
-- Additive, owner-applied migration. Existing W1 and W1c evidence is untouched.
-- Rollback: schemas/sec_foreign_share_census_v1.rollback.sql.
--
-- W1c v2 restatement semantics apply: source retirement preserves prior
-- knowledge before retired_on; parser_correction removes the wrong reading
-- from every historical answer. The loader assigns replacement availability
-- and maintains zero-census source metadata under this same advisory lock.
BEGIN;
SET LOCAL lock_timeout = '5s';
SELECT pg_catalog.pg_advisory_xact_lock(79311, 173);

CREATE TABLE IF NOT EXISTS public.sec_foreign_share_census_sources (
    -- Stable document key, excluding the parser version and source hash.
    source_package text PRIMARY KEY,
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    source_url text NOT NULL CHECK (pg_catalog.length(pg_catalog.btrim(source_url)) > 0),
    source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    parser_version text NOT NULL CHECK (pg_catalog.length(pg_catalog.btrim(parser_version)) > 0),
    first_loaded_on date NOT NULL,
    last_loaded_on date NOT NULL,
    census_count integer NOT NULL CHECK (census_count >= 0),
    CHECK (last_loaded_on >= first_loaded_on)
);

CREATE TABLE IF NOT EXISTS public.sec_foreign_share_census (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fact_hash text NOT NULL CHECK (fact_hash ~ '^[0-9a-f]{32}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    form text NOT NULL CHECK (form IN ('20-F', '20-F/A', '40-F', '40-F/A')),
    filed date NOT NULL,
    accepted timestamp with time zone,
    period_end date,
    -- A recognized but incompletely read statement may have no known date.
    shares_as_of date,
    date_explicit boolean NOT NULL DEFAULT false,
    -- Exact source names, normalized class keys, kind and count per class.
    -- Nil counts are zero; class names are never invented by the resolver.
    classes jsonb NOT NULL CHECK (pg_catalog.jsonb_typeof(classes) = 'array'),
    stated_total numeric,
    computed_total numeric,
    complete boolean NOT NULL,
    conflicting boolean NOT NULL,
    status text GENERATED ALWAYS AS (
        CASE WHEN conflicting THEN 'conflicting'
             WHEN complete THEN 'complete' ELSE 'incomplete' END
    ) STORED,
    reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    cross_checks jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (pg_catalog.jsonb_typeof(cross_checks) = 'object'),
    publication_floor_on date,
    source_url text NOT NULL CHECK (pg_catalog.length(pg_catalog.btrim(source_url)) > 0),
    source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    evidence_text text NOT NULL CHECK (pg_catalog.length(pg_catalog.btrim(evidence_text)) > 0),
    evidence_location text NOT NULL CHECK (pg_catalog.length(pg_catalog.btrim(evidence_location)) > 0),
    parser_version text NOT NULL CHECK (pg_catalog.length(pg_catalog.btrim(parser_version)) > 0),
    source_available_on date GENERATED ALWAYS AS (
        greatest(filed + 1, publication_floor_on)
    ) STORED,
    available_on date NOT NULL,
    retired_on date,
    retired_reason text,
    loaded_on date NOT NULL,
    source_package text NOT NULL CHECK (pg_catalog.length(pg_catalog.btrim(source_package)) > 0),
    CHECK (available_on >= greatest(filed + 1, publication_floor_on)),
    CHECK (retired_reason IS NULL
           OR (retired_reason IN ('source', 'parser_correction')
               AND retired_on IS NOT NULL)),
    CHECK (stated_total IS NULL
           OR (stated_total >= 0 AND stated_total < 'Infinity'::numeric
               AND stated_total = pg_catalog.trunc(stated_total))),
    CHECK (computed_total IS NULL
           OR (computed_total >= 0 AND computed_total < 'Infinity'::numeric
               AND computed_total = pg_catalog.trunc(computed_total))),
    CHECK (NOT date_explicit OR shares_as_of IS NOT NULL),
    CHECK (NOT complete
           OR (shares_as_of IS NOT NULL
               AND CASE WHEN pg_catalog.jsonb_typeof(classes) = 'array'
                        THEN pg_catalog.jsonb_array_length(classes) > 0
                        ELSE false END
               AND computed_total IS NOT NULL
               AND (stated_total IS NULL OR stated_total = computed_total)))
);

CREATE INDEX IF NOT EXISTS sec_foreign_share_census_issuer_idx
    ON public.sec_foreign_share_census (cik, shares_as_of DESC, filed DESC, adsh DESC);
CREATE INDEX IF NOT EXISTS sec_foreign_share_census_source_idx
    ON public.sec_foreign_share_census (source_package, fact_hash);

-- Select the latest public census before inspecting its status. A newer
-- incomplete or conflicting census is an audit result and must not cause a
-- consumer to fall back to an older usable count. Unknown statement dates
-- compete on period end (or filing date); they remain explicitly incomplete.
-- No function-level SET or security definer: this single SQL statement can
-- inline into the future sizing consumer and uses only qualified relations.
CREATE OR REPLACE FUNCTION public.sec_foreign_share_census_at(
    p_cik bigint, p_as_of date
)
RETURNS SETOF public.sec_foreign_share_census
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT c.*
FROM public.sec_foreign_share_census AS c
WHERE c.cik = p_cik
  AND c.available_on <= p_as_of
  AND (c.shares_as_of IS NULL OR c.shares_as_of <= p_as_of)
  AND (c.retired_on IS NULL
       OR (c.retired_on > p_as_of
           AND c.retired_reason IS DISTINCT FROM 'parser_correction'))
ORDER BY coalesce(c.shares_as_of, c.period_end, c.filed) DESC,
         c.filed DESC, c.accepted DESC NULLS LAST, c.adsh DESC,
         c.available_on DESC, c.id DESC
LIMIT 1
$fn$;

COMMENT ON TABLE public.sec_foreign_share_census IS
    'Bitemporal positive annual-report cover-statement census, including unlisted classes; incomplete or conflicting evidence cannot prove sizing scope.';
COMMENT ON TABLE public.sec_foreign_share_census_sources IS
    'Latest source reconciliation, including zero-census parses; immutable first-loaded date protects availability of later parser additions.';
COMMENT ON COLUMN public.sec_foreign_share_census.classes IS
    'Source class names, normalized class/series/ordinary/common keys, kinds and exact share counts; no inferred class.';
COMMENT ON COLUMN public.sec_foreign_share_census.complete IS
    'Every named class and numeric residue parsed, date known, and any stated total equals the class sum; conflict is an independent refusal.';
COMMENT ON COLUMN public.sec_foreign_share_census.cross_checks IS
    'Same-accession W1 issuer-total and class-count comparisons; mismatches mark conflicting and never correct source counts.';
COMMENT ON COLUMN public.sec_foreign_share_census.retired_reason IS
    'source (or NULL): public record changed, visible before retired_on; parser_correction: wrong reading, visible at no date';
COMMENT ON FUNCTION public.sec_foreign_share_census_at(bigint, date) IS
    'Latest census public by D, ordered by statement date then filing order; returns its full classes, provenance and refusal status, or no row.';

-- Match W1c owner and remove default privileges, including reader writes.
REVOKE ALL ON TABLE public.sec_foreign_share_census,
    public.sec_foreign_share_census_sources FROM PUBLIC;
REVOKE ALL ON SEQUENCE public.sec_foreign_share_census_id_seq FROM PUBLIC;
REVOKE ALL ON FUNCTION public.sec_foreign_share_census_at(bigint, date) FROM PUBLIC;
DO $grants$
DECLARE
    reader text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'worker_writer') THEN
        ALTER TABLE public.sec_foreign_share_census OWNER TO worker_writer;
        ALTER TABLE public.sec_foreign_share_census_sources OWNER TO worker_writer;
        ALTER FUNCTION public.sec_foreign_share_census_at(bigint, date) OWNER TO worker_writer;
    END IF;
    FOREACH reader IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = reader) THEN
            EXECUTE pg_catalog.format('REVOKE ALL ON TABLE public.sec_foreign_share_census, public.sec_foreign_share_census_sources FROM %I', reader);
            EXECUTE pg_catalog.format('GRANT SELECT ON TABLE public.sec_foreign_share_census, public.sec_foreign_share_census_sources TO %I', reader);
            EXECUTE pg_catalog.format('REVOKE ALL ON SEQUENCE public.sec_foreign_share_census_id_seq FROM %I', reader);
            EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION public.sec_foreign_share_census_at(bigint, date) FROM %I', reader);
            EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION public.sec_foreign_share_census_at(bigint, date) TO %I', reader);
        END IF;
    END LOOP;
END
$grants$;
COMMIT;
