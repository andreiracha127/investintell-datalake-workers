-- Ticker -> issuer evidence from SEC Section 16 Forms 3/4/5 and amendments.
-- Sources: DERA quarterly Insider Transactions SUBMISSION.tsv (2006 onwards)
-- and sec-api.io original ownership XML archives (May 2003 through 2005).
-- This source has no security-class dimension and does not consult an outside
-- ticker/CIK prior. Consumers wire it only into cover-page 'missing' states.
--
-- Bitemporal conventions follow sec_ticker_cik_history_v1.sql. Initial facts
-- are available at the source's EDGAR acceptance date (America/New_York), or
-- filed + 1 when the source has only a date. A correction to an accession seen
-- before is available no earlier than reconciliation. Facts and package
-- memberships are retired, never deleted; unchanged facts retain availability.
-- The loader reconciles a package atomically and retires a fact only once no
-- current package carries it. Package metadata records its current validators.
--
-- Governed, owner-applied, additive and idempotent migration. The loader never
-- applies it implicitly. Rollback: sec_insider_ticker_evidence.rollback.sql.
BEGIN;
SET LOCAL lock_timeout = '5s';

-- Match W1's separator-free keys while preserving each normalised spelling.
CREATE OR REPLACE FUNCTION sec_insider_symbol_keys(p_symbols text[])
RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
SELECT ARRAY(
    SELECT DISTINCT replace(s.symbol, '-', '')
    FROM unnest(p_symbols) AS s(symbol)
    ORDER BY 1
)
$fn$;

CREATE TABLE IF NOT EXISTS sec_insider_filings (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- SHA-256 of filing content, excluding package/version provenance.
    fact_hash text NOT NULL CHECK (fact_hash ~ '^[0-9a-f]{64}$'),
    accession text NOT NULL CHECK (accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    raw_symbol text NOT NULL,
    -- Empty arrays preserve raw filings whose symbols are unusable.
    normalized_symbols text[] NOT NULL,
    ticker_keys text[] GENERATED ALWAYS AS (
        sec_insider_symbol_keys(normalized_symbols)
    ) STORED,
    form text NOT NULL CHECK (form IN ('3', '4', '5', '3/A', '4/A', '5/A')),
    filed date NOT NULL,
    accepted timestamptz,
    source_available_on date GENERATED ALWAYS AS (
        COALESCE((accepted AT TIME ZONE 'America/New_York')::date, filed + 1)
    ) STORED,
    available_on date NOT NULL,
    retired_on date,
    loaded_on date NOT NULL,
    source text NOT NULL CHECK (source <> ''),
    source_package text NOT NULL CHECK (source_package <> ''),
    source_version text NOT NULL CHECK (source_version <> ''),
    CHECK (array_position(normalized_symbols, NULL) IS NULL),
    CHECK (available_on >= COALESCE(
        (accepted AT TIME ZONE 'America/New_York')::date, filed + 1)),
    CHECK (retired_on IS NULL OR retired_on >= available_on)
);

CREATE UNIQUE INDEX IF NOT EXISTS sec_insider_filings_current_idx
    ON sec_insider_filings (fact_hash) WHERE retired_on IS NULL;
CREATE INDEX IF NOT EXISTS sec_insider_filings_keys_idx
    ON sec_insider_filings USING gin (ticker_keys);
CREATE INDEX IF NOT EXISTS sec_insider_filings_accession_idx
    ON sec_insider_filings (accession, available_on DESC, loaded_on DESC, id DESC);
CREATE INDEX IF NOT EXISTS sec_insider_filings_filed_idx
    ON sec_insider_filings (filed);

CREATE TABLE IF NOT EXISTS sec_insider_packages (
    source_package text PRIMARY KEY,
    source text NOT NULL CHECK (source <> ''),
    source_version text NOT NULL CHECK (source_version <> ''),
    package_sha256 text NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    package_bytes bigint NOT NULL CHECK (package_bytes > 0),
    parser_version text NOT NULL CHECK (parser_version <> ''),
    filings integer NOT NULL CHECK (filings >= 0),
    rejected jsonb NOT NULL DEFAULT '{}'::jsonb,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    remote_etag text,
    remote_last_modified text
);

-- Every accession ever carried by a package: even a removed and later restored
-- accession is recognised as a correction rather than backdated initial data.
CREATE TABLE IF NOT EXISTS sec_insider_package_members (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_package text NOT NULL,
    accession text NOT NULL CHECK (accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    loaded_on date NOT NULL,
    retired_on date,
    CHECK (retired_on IS NULL OR retired_on >= loaded_on)
);

CREATE UNIQUE INDEX IF NOT EXISTS sec_insider_package_members_current_idx
    ON sec_insider_package_members (source_package, accession)
    WHERE retired_on IS NULL;
CREATE INDEX IF NOT EXISTS sec_insider_package_members_accession_idx
    ON sec_insider_package_members (accession);

-- Content versions carried by a package. The same fact can have several
-- carriers; withdrawing one package does not withdraw another package's fact.
CREATE TABLE IF NOT EXISTS sec_insider_package_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_package text NOT NULL,
    fact_hash text NOT NULL CHECK (fact_hash ~ '^[0-9a-f]{64}$'),
    loaded_on date NOT NULL,
    retired_on date,
    CHECK (retired_on IS NULL OR retired_on >= loaded_on)
);

CREATE UNIQUE INDEX IF NOT EXISTS sec_insider_package_facts_current_idx
    ON sec_insider_package_facts (source_package, fact_hash)
    WHERE retired_on IS NULL;
CREATE INDEX IF NOT EXISTS sec_insider_package_facts_hash_idx
    ON sec_insider_package_facts (fact_hash) WHERE retired_on IS NULL;

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

COMMENT ON TABLE sec_insider_filings IS
    'Bitemporal SEC Section 16 issuer CIK and trading-symbol statements; no class mapping or outside CIK prior.';
COMMENT ON TABLE sec_insider_packages IS
    'Current source package validators and parser version; filing and carrier history are preserved separately.';
COMMENT ON FUNCTION sec_insider_ticker_issuer_at(text, date) IS
    'Pure-SEC ticker/CIK evidence in [D-365,D): 2 filings on 2 dates, unanimous or 3x dominance; resolved|ambiguous|none. Counts include unadmitted minorities.';

-- Match W1's owner/read-only grants even if database default privileges grant
-- app_runtime writes. Workers apply the schema and load only as its owner.
REVOKE ALL ON TABLE sec_insider_filings, sec_insider_packages,
    sec_insider_package_members, sec_insider_package_facts FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_insider_symbol_keys(text[]),
    sec_insider_ticker_issuer_at(text, date) FROM PUBLIC;
DO $$
DECLARE
    relations constant text[] := ARRAY[
        'sec_insider_filings', 'sec_insider_packages',
        'sec_insider_package_members', 'sec_insider_package_facts'];
    routines constant text[] := ARRAY[
        'sec_insider_symbol_keys(text[])',
        'sec_insider_ticker_issuer_at(text, date)'];
    item text;
    reader text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'worker_writer') THEN
        FOREACH item IN ARRAY relations LOOP
            EXECUTE format('ALTER TABLE %I OWNER TO worker_writer', item);
        END LOOP;
        FOREACH item IN ARRAY routines LOOP
            EXECUTE format('ALTER FUNCTION %s OWNER TO worker_writer', item);
        END LOOP;
    END IF;
    FOREACH reader IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = reader) THEN
            FOREACH item IN ARRAY relations LOOP
                EXECUTE format('REVOKE ALL ON TABLE %I FROM %I', item, reader);
                EXECUTE format('GRANT SELECT ON TABLE %I TO %I', item, reader);
            END LOOP;
            FOREACH item IN ARRAY routines LOOP
                EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO %I', item, reader);
            END LOOP;
        END IF;
    END LOOP;
END $$;

COMMIT;
