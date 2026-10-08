-- Effective-dated ticker -> issuer (CIK) history from SEC XBRL cover pages.
--
-- Source: SEC DERA "Financial Statement and Notes" data sets
-- (https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets),
-- loaded by scripts/load_sec_ticker_cik_history.py. Every inline-XBRL cover page
-- tags dei:TradingSymbol (with dei:Security12bTitle and dei:SecurityExchangeName,
-- one set per security class via dimensions); the 2019 cover-page rule made it
-- mandatory on 10-K/10-Q/8-K/20-F/40-F, and many filers tagged it voluntarily
-- since 2009. Each fact is a dated statement by the registrant: "as of this
-- filing, CIK X's security trades as T". One row per (accession, ticker).
--
-- Governed, owner-applied migration (postgres or worker_writer, psql with
-- ON_ERROR_STOP). Additive and idempotent; workers never apply it implicitly.
-- Rollback: schemas/sec_ticker_cik_history_v1.rollback.sql.
BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS sec_ticker_cik_observations (
    -- EDGAR accession number of the filing that carries the cover page.
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    -- Uppercase symbol in the eod_prices / universe_constituents style:
    -- class and preferred separators normalized to '-' (BRK.B -> BRK-B,
    -- "USB PrA" -> USB-PA). ticker_raw keeps the filer's spelling.
    ticker text NOT NULL CHECK (ticker ~ '^[A-Z0-9]+(-[A-Z0-9]+)*$'),
    -- Separator-free match key: filers also write class shares without a
    -- separator (Brown-Forman tags "BFB" for BF-B), so the resolver matches on
    -- this key. A key collision between issuers surfaces as 'ambiguous'.
    ticker_key text GENERATED ALWAYS AS (replace(ticker, '-', '')) STORED,
    ticker_raw text NOT NULL,
    cik bigint NOT NULL CHECK (cik > 0),
    security_title text,
    exchange text,
    form text NOT NULL,
    period date,
    filed date NOT NULL,
    -- EDGAR acceptance datetime as published by DERA: America/New_York wall
    -- clock, no zone. NULL when the package does not carry it.
    accepted timestamp,
    -- First date the observation is public: the acceptance date when known,
    -- else the day after the filing date (a date without time is conservative).
    available_on date GENERATED ALWAYS AS (COALESCE(accepted::date, filed + 1)) STORED,
    source_package text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (adsh, ticker)
);

CREATE INDEX IF NOT EXISTS sec_ticker_cik_observations_key_idx
    ON sec_ticker_cik_observations (ticker_key, available_on DESC, cik);
CREATE INDEX IF NOT EXISTS sec_ticker_cik_observations_cik_idx
    ON sec_ticker_cik_observations (cik, available_on DESC);

-- One row per loaded DERA package: provenance and the loader's counts.
CREATE TABLE IF NOT EXISTS sec_ticker_cik_packages (
    source_package text PRIMARY KEY,
    package_sha256 text NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    package_bytes bigint NOT NULL CHECK (package_bytes > 0),
    submissions integer NOT NULL CHECK (submissions >= 0),
    symbol_facts integer NOT NULL CHECK (symbol_facts >= 0),
    observations integer NOT NULL CHECK (observations >= 0),
    rejected jsonb NOT NULL DEFAULT '{}'::jsonb,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

-- issuer_at(ticker, D): the CIK a ticker belonged to at date D, from what was
-- public at D.
--
-- * Candidates are the observations whose ticker_key equals the key of
--   p_ticker and whose available_on <= D.
-- * none of them           -> status 'missing'.
-- * the latest one is older than p_max_age_days (available_on < D - max_age)
--                          -> status 'stale' (cik NULL; active_ciks holds the
--                             stale issuer for diagnostics).
-- * otherwise, within the window [D - max_age, D]: the winner is the CIK of the
--   most recent observation. Every other CIK with an in-window observation must
--   have its latest in-window observation strictly before the winner's earliest
--   in-window observation -- a clean reassignment (old issuer stops reporting
--   the symbol, then the new one starts). Otherwise both claimed the symbol
--   concurrently (including a tie on the latest date) -> status 'ambiguous'
--   (cik NULL; active_ciks lists the winner and every contender).
-- * else status 'resolved', cik = winner, active_ciks = {winner}.
-- observed_on/adsh identify the deciding observation (latest in window, or the
-- latest overall when stale). Exactly one row is returned for any input.
-- SECURITY INVOKER, no SET clause: the planner can inline it into a LATERAL
-- join, and it reads the observation table the caller's search_path resolves.
CREATE OR REPLACE FUNCTION sec_ticker_issuer_at(
    p_ticker text, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    status text,
    cik bigint,
    observed_on date,
    adsh text,
    active_ciks bigint[]
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH observed AS (
    SELECT o.cik, o.available_on, o.adsh
    FROM sec_ticker_cik_observations o
    WHERE o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
      AND o.available_on <= p_as_of
), windowed AS (
    SELECT w.cik, w.available_on, w.adsh
    FROM observed w
    WHERE w.available_on >= p_as_of - p_max_age_days
), per_cik AS (
    SELECT w.cik, min(w.available_on) AS first_on, max(w.available_on) AS last_on
    FROM windowed w
    GROUP BY w.cik
), latest AS (
    SELECT w.cik, w.available_on, w.adsh
    FROM windowed w
    ORDER BY w.available_on DESC, w.cik, w.adsh DESC
    LIMIT 1
), contenders AS (
    SELECT p.cik
    FROM per_cik p
    JOIN per_cik winner ON winner.cik = (SELECT l.cik FROM latest l)
    WHERE p.cik <> winner.cik AND p.last_on >= winner.first_on
), last_seen AS (
    SELECT o.cik, o.available_on, o.adsh
    FROM observed o
    ORDER BY o.available_on DESC, o.cik, o.adsh DESC
    LIMIT 1
)
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM observed) THEN 'missing'
        WHEN NOT EXISTS (SELECT 1 FROM windowed) THEN 'stale'
        WHEN EXISTS (SELECT 1 FROM contenders) THEN 'ambiguous'
        ELSE 'resolved'
    END AS status,
    CASE WHEN NOT EXISTS (SELECT 1 FROM contenders)
         THEN (SELECT l.cik FROM latest l) END AS cik,
    COALESCE((SELECT l.available_on FROM latest l),
             (SELECT s.available_on FROM last_seen s)) AS observed_on,
    COALESCE((SELECT l.adsh FROM latest l), (SELECT s.adsh FROM last_seen s)) AS adsh,
    CASE
        WHEN EXISTS (SELECT 1 FROM latest)
            THEN (SELECT l.cik FROM latest l)
                 || ARRAY(SELECT c.cik FROM contenders c ORDER BY c.cik)
        ELSE ARRAY(SELECT s.cik FROM last_seen s)
    END AS active_ciks
$fn$;

COMMENT ON TABLE sec_ticker_cik_observations IS
    'SEC XBRL cover-page dei:TradingSymbol observations: CIK X traded as ticker T '
    'as of filing adsh. Loaded from DERA Financial Statement and Notes data sets.';
COMMENT ON FUNCTION sec_ticker_issuer_at(text, date, integer) IS
    'issuer_at(ticker, D): status resolved|missing|stale|ambiguous; see '
    'schemas/sec_ticker_cik_history_v1.sql for the semantics.';

-- Ownership and grants. The loader writes as the owner; readers get SELECT and
-- EXECUTE only (default privileges would otherwise hand app_runtime writes).
REVOKE ALL ON TABLE sec_ticker_cik_observations, sec_ticker_cik_packages FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_ticker_issuer_at(text, date, integer) FROM PUBLIC;
DO $$
DECLARE
    reader text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'worker_writer') THEN
        ALTER TABLE sec_ticker_cik_observations OWNER TO worker_writer;
        ALTER TABLE sec_ticker_cik_packages OWNER TO worker_writer;
        ALTER FUNCTION sec_ticker_issuer_at(text, date, integer) OWNER TO worker_writer;
    END IF;
    FOREACH reader IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = reader) THEN
            EXECUTE format(
                'REVOKE ALL ON TABLE sec_ticker_cik_observations, '
                'sec_ticker_cik_packages FROM %I', reader);
            EXECUTE format(
                'GRANT SELECT ON TABLE sec_ticker_cik_observations, '
                'sec_ticker_cik_packages TO %I', reader);
            EXECUTE format(
                'GRANT EXECUTE ON FUNCTION sec_ticker_issuer_at(text, date, integer) TO %I',
                reader);
        END IF;
    END LOOP;
END $$;

COMMIT;
