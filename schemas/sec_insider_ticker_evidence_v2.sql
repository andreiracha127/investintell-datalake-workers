-- v2 of the SEC insider ticker evidence (on top of sec_insider_ticker_evidence.sql).
--
-- A parser correction restates our reading; it does not change the public record.
-- This is the project's restatement rule, as in sec_ticker_cik_history_v2.sql.
-- sec_insider_filings.retired_reason records why a version was retired:
-- * 'source': a republished package carries other content or no longer carries
--   the filing. The version stays visible before retired_on. NULL, on rows
--   retired before v2, means the same.
-- * 'parser_correction': the same package bytes read by another parser version.
--   The old reading was never true, so it is visible at no date. The loader
--   dates the new reading as the reading it replaces: from the filing's public
--   date for a filing first loaded with its package.
-- Package carriers (sec_insider_package_facts) need no reason: they decide when a
-- filing version retires, never what a query at D sees.
--
-- Governed, owner-applied migration (worker_writer or postgres, psql with
-- ON_ERROR_STOP), one transaction, idempotent. It adds one nullable column without
-- a default (a catalog change: no rewrite, no scan; ACCESS EXCLUSIVE on
-- sec_insider_filings for the transaction's milliseconds) and its CHECK NOT VALID
-- (every existing row is NULL there; new rows are checked), and replaces
-- sec_insider_ticker_issuer_at in place. No row is written.
-- Rollback: schemas/sec_insider_ticker_evidence_v2.rollback.sql.
BEGIN;
SET LOCAL lock_timeout = '5s';

ALTER TABLE sec_insider_filings ADD COLUMN IF NOT EXISTS retired_reason text;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                   WHERE conrelid = to_regclass('sec_insider_filings')
                     AND conname = 'sec_insider_filings_retired_reason_check') THEN
        ALTER TABLE sec_insider_filings
            ADD CONSTRAINT sec_insider_filings_retired_reason_check
            CHECK (retired_reason IS NULL
                   OR (retired_reason IN ('source', 'parser_correction')
                       AND retired_on IS NOT NULL)) NOT VALID;
    END IF;
END $$;
COMMENT ON COLUMN sec_insider_filings.retired_reason IS
    'source (or NULL): the public record changed, visible before retired_on; '
    'parser_correction: our reading was wrong, visible at no date';

-- One result for the ticker at D. Filings occupy [D - 365, D), independently
-- of knowledge-time availability. Count accessions, never package copies or
-- symbol spellings. All CIKs participate in conflict/dominance counts; a
-- one-filing minority therefore cannot disappear through admission filtering.
-- The winner independently needs >= 2 filings on >= 2 distinct filing dates.
-- Unanimous eligible evidence or >= 3x the runner-up resolves; any other CIK
-- conflict is ambiguous. There is no latest-filing or plain-majority rule.
-- Counts describe the top candidate even for none/ambiguous; cik is NULL then.
-- A version is visible at D when available_on <= D and it is not retired at D
-- by a change of the public record; a version retired as a parser correction is
-- visible at no date.
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
    SELECT f.accession, f.filed, f.available_on, f.retired_on, f.retired_reason
    FROM sec_insider_filings f, wanted w
    WHERE f.ticker_keys @> ARRAY[w.ticker_key]
), matching_accessions AS MATERIALIZED (
    SELECT DISTINCT f.accession
    FROM key_versions f
    WHERE f.filed >= p_d - 365 AND f.filed < p_d
      AND f.available_on <= p_d
      AND (f.retired_on IS NULL
           OR (f.retired_on > p_d
               AND f.retired_reason IS DISTINCT FROM 'parser_correction'))
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
          AND (v.retired_on IS NULL
               OR (v.retired_on > p_d
                   AND v.retired_reason IS DISTINCT FROM 'parser_correction'))
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
    'Pure-SEC ticker/CIK evidence in [D-365,D): 2 filings on 2 dates, unanimous or 3x dominance; resolved|ambiguous|none. Counts include unadmitted minorities. A parser correction''s old reading is visible at no date.';

-- CREATE OR REPLACE keeps the function's owner and privileges; restate them as
-- sec_insider_ticker_evidence.sql grants them.
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
            EXECUTE format('GRANT SELECT ON TABLE sec_insider_filings TO %I', reader);
            EXECUTE format('GRANT EXECUTE ON FUNCTION sec_insider_ticker_issuer_at(text, date) TO %I', reader);
        END IF;
    END LOOP;
END $$;

COMMIT;
