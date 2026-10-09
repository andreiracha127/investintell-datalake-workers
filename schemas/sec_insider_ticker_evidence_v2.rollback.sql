-- Rollback of schemas/sec_insider_ticker_evidence_v2.sql: the v1 definition of
-- sec_insider_ticker_issuer_at, copied from sec_insider_ticker_evidence.sql, in
-- one transaction. Every row is kept, and so are the retired_reason column, its
-- values and its CHECK, which v1 ignores; applying v2 again restores the rule.
-- With v1's predicate a version retired as a parser correction is visible again
-- before its retirement date. The resolver still counts each accession's latest
-- visible version, which is the corrected reading (the v2 loader gives it the
-- replaced reading's availability and a later load date), so answers change only
-- where a parser correction withdrew a reading without a replacement.
-- The v4 loader refuses a rolled-back database. Apply as the role that applied
-- v2, with psql -v ON_ERROR_STOP=1.
BEGIN;
SET LOCAL lock_timeout = '5s';

-- One result for the ticker at D. Filings occupy [D - 365, D), independently
-- of knowledge-time availability. Count accessions, never package copies or
-- symbol spellings. All CIKs participate in conflict/dominance counts; a
-- one-filing minority therefore cannot disappear through admission filtering.
-- The winner independently needs >= 2 filings on >= 2 distinct filing dates.
-- Unanimous eligible evidence or >= 3x the runner-up resolves; any other CIK
-- conflict is ambiguous. There is no latest-filing or plain-majority rule.
-- Counts describe the top candidate even for none/ambiguous; cik is NULL then.
CREATE OR REPLACE FUNCTION sec_insider_ticker_issuer_at(p_ticker text, p_d date)
RETURNS TABLE (
    cik bigint,
    status text,
    filing_count bigint,
    distinct_dates bigint,
    candidate_count bigint,
    total_filings bigint,
    runner_up_filings bigint,
    window_start date,
    window_end date
)
LANGUAGE sql STABLE PARALLEL SAFE
SET jit = off
AS $fn$
WITH wanted AS (
    SELECT regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g') AS ticker_key
), key_versions AS MATERIALIZED (
    -- SQL functions plan their date parameters generically. Resolve the GIN
    -- key first so that an underestimated date range cannot scan every filing
    -- in the year again for each ticker lookup.
    SELECT f.accession, f.filed, f.available_on, f.retired_on
    FROM sec_insider_filings f, wanted w
    WHERE f.ticker_keys @> ARRAY[w.ticker_key]
), matching_accessions AS MATERIALIZED (
    SELECT DISTINCT f.accession
    FROM key_versions f
    WHERE f.filed >= p_d - 365 AND f.filed < p_d
      AND f.available_on <= p_d
      AND (f.retired_on IS NULL OR f.retired_on > p_d)
), visible AS (
    -- Select the latest visible content version of an accession, then check its
    -- symbol again: an older package cannot retain a superseded symbol. This
    -- ordering selects data versions, never competing issuers' latest filings.
    SELECT f.*
    FROM matching_accessions a
    CROSS JOIN LATERAL (
        SELECT v.* FROM sec_insider_filings v
        WHERE v.accession = a.accession
          AND v.available_on <= p_d
          AND (v.retired_on IS NULL OR v.retired_on > p_d)
        ORDER BY v.available_on DESC, v.loaded_on DESC, v.id DESC
        LIMIT 1
    ) f
    CROSS JOIN wanted w
    WHERE f.ticker_keys @> ARRAY[w.ticker_key]
      AND f.filed >= p_d - 365 AND f.filed < p_d
), candidates AS (
    SELECT v.cik, count(*) AS filings, count(DISTINCT v.filed) AS dates
    FROM visible v
    GROUP BY v.cik
), ranked AS (
    SELECT c.*, row_number() OVER (ORDER BY c.filings DESC, c.cik) AS rank
    FROM candidates c
), counts AS (
    SELECT count(*) AS candidate_count, COALESCE(sum(c.filings), 0)::bigint AS total_filings
    FROM candidates c
), decision AS (
    SELECT top.cik, COALESCE(top.filings, 0)::bigint AS filing_count,
           COALESCE(top.dates, 0)::bigint AS distinct_dates,
           n.candidate_count, n.total_filings,
           COALESCE(runner.filings, 0)::bigint AS runner_up_filings,
           top.filings >= 2 AND top.dates >= 2
             AND (n.candidate_count = 1 OR top.filings >= 3 * runner.filings) AS resolves
    FROM counts n
    LEFT JOIN ranked top ON top.rank = 1
    LEFT JOIN ranked runner ON runner.rank = 2
)
SELECT CASE WHEN d.resolves THEN d.cik END,
       CASE WHEN d.resolves THEN 'resolved'
            WHEN d.candidate_count > 1 THEN 'ambiguous' ELSE 'none' END,
       d.filing_count, d.distinct_dates, d.candidate_count, d.total_filings,
       d.runner_up_filings, p_d - 365, p_d
FROM decision d
$fn$;

COMMENT ON FUNCTION sec_insider_ticker_issuer_at(text, date) IS
    'Pure-SEC ticker/CIK evidence in [D-365,D): 2 filings on 2 dates, unanimous or 3x dominance; resolved|ambiguous|none. Counts include unadmitted minorities.';

REVOKE ALL ON FUNCTION sec_insider_ticker_issuer_at(text, date) FROM PUBLIC;
DO $$
DECLARE
    reader text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'worker_writer') THEN
        ALTER FUNCTION sec_insider_ticker_issuer_at(text, date) OWNER TO worker_writer;
    END IF;
    FOREACH reader IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = reader) THEN
            EXECUTE format('GRANT EXECUTE ON FUNCTION sec_insider_ticker_issuer_at(text, date) TO %I', reader);
        END IF;
    END LOOP;
END $$;

COMMIT;
