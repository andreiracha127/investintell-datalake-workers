-- Effective-dated ticker -> issuer (CIK, class) history from SEC XBRL cover pages.
--
-- Sources (public; loaded by scripts/load_sec_ticker_cik_history.py):
-- * SEC DERA "Financial Statement and Notes" data sets
--   (https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets):
--   every inline-XBRL cover page tags dei:TradingSymbol, dei:Security12bTitle and
--   dei:SecurityExchangeName, one set per security class (StatementClassOfStockAxis
--   member), and dei:EntityCommonStockSharesOutstanding (per class or in total).
--   Mandatory cover tagging phased in for periods ending on/after 2019-06-15
--   (large accelerated), 2020-06-15 (accelerated) and 2021-06-15 (others); many
--   filers tagged dei:TradingSymbol voluntarily since 2009.
-- * EDGAR full-text indexes (https://www.sec.gov/Archives/edgar/full-index/):
--   Forms 15-12B, 15-12G, 15-15D, 25 and 25-NSE (deregistration / delisting).
--
-- A security LINE is (cik, class_key): class_key is the context's dimension
-- segments without the listing-exchange and legal-entity axes
-- ('ClassOfStock=CommonClassA;'), '' when the fact has no dimensions. Each row
-- of sec_ticker_cik_observations is one dated statement by the registrant:
-- "as of filing adsh, line (cik, class_key) trades as ticker". Rows are never
-- rewritten from later knowledge (sub.prevrpt is not applied retroactively);
-- a row is usable from available_on, its knowledge date.
--
-- Governed, owner-applied migration (postgres or worker_writer, psql with
-- ON_ERROR_STOP). Additive and idempotent; workers never apply it implicitly.
-- Rollback: schemas/sec_ticker_cik_history_v1.rollback.sql.
BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS sec_ticker_cik_observations (
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    -- DERA dimension hash of the TradingSymbol context and its segments.
    dimh text NOT NULL,
    segments text NOT NULL,
    class_key text NOT NULL,
    -- Uppercase symbol in the eod_prices / universe_constituents style:
    -- class and preferred separators normalized to '-' (BRK.B -> BRK-B,
    -- "USB PrA" -> USB-PA). ticker_raw keeps the filer's spelling.
    ticker text NOT NULL CHECK (ticker ~ '^[A-Z0-9]+(-[A-Z0-9]+)*$'),
    -- Separator-free match key: filers also write class shares without a
    -- separator (Brown-Forman tags "BFB" for BF-B), so resolution matches on
    -- this key; a key collision between issuers surfaces as 'ambiguous'.
    ticker_key text GENERATED ALWAYS AS (replace(ticker, '-', '')) STORED,
    ticker_raw text NOT NULL,
    security_title text,
    exchange text,
    -- From the title, else the symbol/segments: what the line is.
    security_kind text NOT NULL CHECK (security_kind IN (
        'equity', 'depositary', 'preferred', 'debt', 'warrant', 'unit', 'right'
    )),
    -- The fact's ddate (DERA rounds it to a month end; informational only).
    ddate date,
    form text NOT NULL,
    period date,
    filed date NOT NULL,
    -- EDGAR acceptance datetime as published by DERA: America/New_York wall
    -- clock, no zone. NULL when the package does not carry it.
    accepted timestamp,
    -- Knowledge date: the acceptance date when known, else the day after the
    -- filing date (a date without a time is conservative).
    available_on date GENERATED ALWAYS AS (COALESCE(accepted::date, filed + 1)) STORED,
    source_package text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (adsh, dimh, ticker)
);

CREATE INDEX IF NOT EXISTS sec_ticker_cik_observations_key_idx
    ON sec_ticker_cik_observations (ticker_key, available_on DESC);
CREATE INDEX IF NOT EXISTS sec_ticker_cik_observations_line_idx
    ON sec_ticker_cik_observations (cik, class_key, available_on DESC, adsh DESC);
CREATE INDEX IF NOT EXISTS sec_ticker_cik_observations_filing_idx
    ON sec_ticker_cik_observations (cik, available_on DESC, adsh DESC);

-- Cover-page dei:EntityCommonStockSharesOutstanding, per class or in total.
-- Distinct values reported for the same (filing, class, date) are all kept.
CREATE TABLE IF NOT EXISTS sec_cover_share_counts (
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    dimh text NOT NULL,
    segments text NOT NULL,
    class_key text NOT NULL,
    -- The date the count is stated as of (cover date, usually shortly before filing).
    ddate date NOT NULL,
    shares numeric NOT NULL CHECK (shares >= 0),
    form text NOT NULL,
    filed date NOT NULL,
    accepted timestamp,
    available_on date GENERATED ALWAYS AS (COALESCE(accepted::date, filed + 1)) STORED,
    source_package text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (adsh, dimh, ddate, shares)
);

CREATE INDEX IF NOT EXISTS sec_cover_share_counts_line_idx
    ON sec_cover_share_counts (cik, class_key, ddate DESC, available_on DESC);

-- Deregistration / delisting filings from the EDGAR form indexes (date only).
CREATE TABLE IF NOT EXISTS sec_registration_events (
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    form text NOT NULL CHECK (form IN ('15-12B', '15-12G', '15-15D', '25', '25-NSE')),
    filed date NOT NULL,
    available_on date GENERATED ALWAYS AS (filed + 1) STORED,
    source_package text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (adsh, cik)
);

CREATE INDEX IF NOT EXISTS sec_registration_events_cik_idx
    ON sec_registration_events (cik, available_on DESC);

-- One row per loaded source package: provenance and the loader's counts.
CREATE TABLE IF NOT EXISTS sec_ticker_cik_packages (
    source_package text PRIMARY KEY,
    package_sha256 text NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    package_bytes bigint NOT NULL CHECK (package_bytes > 0),
    submissions integer NOT NULL CHECK (submissions >= 0),
    symbol_facts integer NOT NULL CHECK (symbol_facts >= 0),
    observations integer NOT NULL CHECK (observations >= 0),
    share_counts integer NOT NULL CHECK (share_counts >= 0),
    events integer NOT NULL CHECK (events >= 0),
    rejected jsonb NOT NULL DEFAULT '{}'::jsonb,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

-- A line's state at D (internal helper; one row per line that ever showed the
-- key by D). A line is the interval holder of the symbol from the knowledge date
-- of the first filing showing it until the knowledge date of the first later
-- filing of the same line showing another symbol, or of a deregistration that
-- applies to it; computed at D from what was public at D:
-- * statement  = the line's latest filing available on or before D;
-- * 'ended'    = that statement shows another symbol, or a later 15-12G/15-15D
--                (whole registrant) or 15-12B/25/25-NSE (when the statement's
--                filing listed a single symbol, so the class is unambiguous)
--                was public by D;
-- * 'stale'    = an open interval whose statement is older than p_max_age_days;
-- * 'active'   = otherwise.
CREATE OR REPLACE FUNCTION sec_ticker_lines_at(
    p_ticker text, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    cik bigint,
    class_key text,
    state text,
    security_kind text,
    statement_on date,
    statement_adsh text,
    confirmed_on date,
    shows_symbol boolean
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH lines AS (
    SELECT o.cik, o.class_key, max(o.available_on) AS confirmed_on
    FROM sec_ticker_cik_observations o
    WHERE o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
      AND o.available_on <= p_as_of
    GROUP BY o.cik, o.class_key
)
SELECT l.cik, l.class_key,
       CASE
           WHEN NOT st.shows_symbol THEN 'ended'
           WHEN EXISTS (
               SELECT 1 FROM sec_registration_events e
               WHERE e.cik = l.cik
                 AND e.available_on > st.statement_on AND e.available_on <= p_as_of
                 AND (e.form IN ('15-12G', '15-15D') OR (
                     SELECT count(DISTINCT f.ticker_key) FROM sec_ticker_cik_observations f
                     WHERE f.adsh = st.statement_adsh AND f.cik = l.cik) = 1)
           ) THEN 'ended'
           WHEN st.statement_on < p_as_of - p_max_age_days THEN 'stale'
           ELSE 'active'
       END AS state,
       st.security_kind, st.statement_on, st.statement_adsh, l.confirmed_on, st.shows_symbol
FROM lines l
CROSS JOIN LATERAL (
    SELECT s.available_on AS statement_on, s.adsh AS statement_adsh,
           bool_or(s.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g'))
               AS shows_symbol,
           min(s.security_kind) FILTER (
               WHERE s.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
           ) AS security_kind
    FROM sec_ticker_cik_observations s
    WHERE (s.cik, s.class_key, s.available_on, s.adsh) = (
        SELECT x.cik, x.class_key, x.available_on, x.adsh
        FROM sec_ticker_cik_observations x
        WHERE x.cik = l.cik AND x.class_key = l.class_key AND x.available_on <= p_as_of
        ORDER BY x.available_on DESC, x.adsh DESC
        LIMIT 1
    )
    GROUP BY s.available_on, s.adsh
) st
$fn$;

-- issuer_at(ticker, D): the (CIK, class) the ticker belonged to at D, from what
-- was public at D (see sec_ticker_lines_at for the interval rule).
-- * 'resolved'  : exactly one CIK holds an active interval; class_key is its
--                 active line with the latest statement.
-- * 'ambiguous' : two or more CIKs hold overlapping active intervals at D
--                 (cik NULL; active_ciks lists them).
-- * 'stale'     : no active interval, but an open one lacks a confirmation
--                 within p_max_age_days (cik NULL; active_ciks = its holders).
-- * 'ended'     : every interval that showed the ticker ended by D (the holder
--                 moved to another symbol or deregistered).
-- * 'missing'   : no filing public by D showed the ticker.
-- observed_on/adsh: the deciding line's last confirmation of the ticker and its
-- latest statement. Exactly one row for any input. SECURITY INVOKER with no SET
-- clause so the planner can inline it; it reads the tables the caller's
-- search_path resolves.
CREATE OR REPLACE FUNCTION sec_ticker_issuer_at(
    p_ticker text, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    status text,
    cik bigint,
    class_key text,
    security_kind text,
    observed_on date,
    adsh text,
    active_ciks bigint[]
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH lines AS (
    SELECT * FROM sec_ticker_lines_at(p_ticker, p_as_of, p_max_age_days)
), ranked AS (
    SELECT l.*,
           CASE l.state WHEN 'active' THEN 0 WHEN 'stale' THEN 1 ELSE 2 END AS rank_state
    FROM lines l
), decided AS (
    SELECT r.* FROM ranked r
    ORDER BY r.rank_state, r.statement_on DESC, r.confirmed_on DESC, r.cik, r.class_key
    LIMIT 1
), holders AS (
    SELECT r.cik, max(r.confirmed_on) AS confirmed_on
    FROM ranked r
    WHERE r.rank_state = (SELECT d.rank_state FROM decided d) AND r.rank_state < 2
    GROUP BY r.cik
)
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM decided) THEN 'missing'
        WHEN (SELECT d.rank_state FROM decided d) = 2 THEN 'ended'
        WHEN (SELECT d.rank_state FROM decided d) = 1 THEN 'stale'
        WHEN (SELECT count(*) FROM holders) > 1 THEN 'ambiguous'
        ELSE 'resolved'
    END AS status,
    CASE WHEN (SELECT d.rank_state FROM decided d) = 0
              AND (SELECT count(*) FROM holders) = 1
         THEN (SELECT d.cik FROM decided d) END AS cik,
    CASE WHEN (SELECT d.rank_state FROM decided d) = 0
              AND (SELECT count(*) FROM holders) = 1
         THEN (SELECT d.class_key FROM decided d) END AS class_key,
    (SELECT d.security_kind FROM decided d) AS security_kind,
    (SELECT d.confirmed_on FROM decided d) AS observed_on,
    (SELECT d.statement_adsh FROM decided d) AS adsh,
    COALESCE(ARRAY(SELECT h.cik FROM holders h ORDER BY h.confirmed_on DESC, h.cik),
             '{}'::bigint[]) AS active_ciks
$fn$;

-- What a known line (cik, class_key) traded as at D. Used to follow a rename:
-- the current symbol's line, asked at an earlier D, shows the symbol it had
-- then. An issuer whose latest cover filing by D lists exactly one equity or
-- depositary line is followed through that line (single-class filers rename
-- their class member, or drop the dimension, between filings); otherwise the
-- line's own latest statement by D decides.
-- * status 'resolved' | 'stale' | 'ended' | 'missing' | 'ambiguous_class'.
-- * equity_lines: listed equity/depositary lines in the issuer's latest cover
--   filing by D (used to decide whether an issuer total can size one class).
CREATE OR REPLACE FUNCTION sec_issuer_line_at(
    p_cik bigint, p_class_key text, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    status text,
    class_key text,
    tickers text[],
    security_kind text,
    statement_on date,
    adsh text,
    equity_lines integer
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH latest_filing AS (
    SELECT x.adsh, x.available_on
    FROM sec_ticker_cik_observations x
    WHERE x.cik = p_cik AND x.available_on <= p_as_of
    ORDER BY x.available_on DESC, x.adsh DESC
    LIMIT 1
), filing_lines AS (
    SELECT f.class_key, min(f.security_kind) AS security_kind
    FROM sec_ticker_cik_observations f
    WHERE f.cik = p_cik AND f.adsh = (SELECT lf.adsh FROM latest_filing lf)
    GROUP BY f.class_key
), equity AS (
    SELECT fl.class_key FROM filing_lines fl
    WHERE fl.security_kind IN ('equity', 'depositary')
), own_statement AS (
    SELECT x.class_key, x.available_on, x.adsh
    FROM sec_ticker_cik_observations x
    WHERE x.cik = p_cik AND x.class_key = p_class_key AND x.available_on <= p_as_of
    ORDER BY x.available_on DESC, x.adsh DESC
    LIMIT 1
), chosen AS (
    -- A single-class issuer's one equity line is the same security whatever its
    -- member was called; a multi-class issuer is followed only by its own member.
    SELECT e.class_key, lf.available_on, lf.adsh
    FROM equity e, latest_filing lf
    WHERE (SELECT count(*) FROM equity) = 1
    UNION ALL
    SELECT o.class_key, o.available_on, o.adsh FROM own_statement o
    WHERE (SELECT count(*) FROM equity) <> 1
), statement AS (
    SELECT c.class_key, c.available_on, c.adsh,
           ARRAY(SELECT s.ticker FROM sec_ticker_cik_observations s
                 WHERE s.adsh = c.adsh AND s.cik = p_cik AND s.class_key = c.class_key
                 ORDER BY s.ticker) AS tickers,
           (SELECT min(s.security_kind) FROM sec_ticker_cik_observations s
            WHERE s.adsh = c.adsh AND s.cik = p_cik AND s.class_key = c.class_key)
               AS security_kind,
           EXISTS (
               SELECT 1 FROM sec_registration_events e
               WHERE e.cik = p_cik
                 AND e.available_on > c.available_on AND e.available_on <= p_as_of
                 AND (e.form IN ('15-12G', '15-15D') OR (
                     SELECT count(DISTINCT f.ticker_key) FROM sec_ticker_cik_observations f
                     WHERE f.adsh = c.adsh AND f.cik = p_cik) = 1)
           ) AS deregistered
    FROM chosen c
)
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM statement) THEN
            CASE WHEN (SELECT count(*) FROM equity) > 1 THEN 'ambiguous_class' ELSE 'missing' END
        WHEN (SELECT s.deregistered FROM statement s) THEN 'ended'
        WHEN (SELECT s.available_on FROM statement s) < p_as_of - p_max_age_days THEN 'stale'
        ELSE 'resolved'
    END AS status,
    (SELECT s.class_key FROM statement s) AS class_key,
    COALESCE((SELECT s.tickers FROM statement s), '{}'::text[]) AS tickers,
    (SELECT s.security_kind FROM statement s) AS security_kind,
    (SELECT s.available_on FROM statement s) AS statement_on,
    (SELECT s.adsh FROM statement s) AS adsh,
    (SELECT count(*)::integer FROM equity) AS equity_lines
$fn$;

-- The class's own cover-page share count public at D: the latest stated date
-- (ddate <= D, filing available by D), then the latest filing. A date older
-- than p_max_age_days -> 'stale'; distinct values at that (date, filing) ->
-- 'ambiguous'; none -> 'missing'. class_key '' is the issuer total.
CREATE OR REPLACE FUNCTION sec_cover_class_shares_at(
    p_cik bigint, p_class_key text, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    status text,
    shares numeric,
    shares_as_of date,
    adsh text
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH chosen AS (
    SELECT c.ddate, c.available_on, c.adsh
    FROM sec_cover_share_counts c
    WHERE c.cik = p_cik AND c.class_key = p_class_key
      AND c.available_on <= p_as_of AND c.ddate <= p_as_of
    ORDER BY c.ddate DESC, c.available_on DESC, c.adsh DESC
    LIMIT 1
), counts AS (
    SELECT DISTINCT c.shares
    FROM sec_cover_share_counts c, chosen h
    WHERE c.cik = p_cik AND c.class_key = p_class_key
      AND c.ddate = h.ddate AND c.available_on = h.available_on
)
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM chosen) THEN 'missing'
        WHEN (SELECT h.ddate FROM chosen h) < p_as_of - p_max_age_days THEN 'stale'
        WHEN (SELECT count(*) FROM counts) > 1 THEN 'ambiguous'
        ELSE 'resolved'
    END AS status,
    CASE WHEN (SELECT count(*) FROM counts) = 1 THEN (SELECT k.shares FROM counts k) END
        AS shares,
    (SELECT h.ddate FROM chosen h) AS shares_as_of,
    (SELECT h.adsh FROM chosen h) AS adsh
$fn$;

-- Diagnostics: every interval as known today (valid_to = knowledge date of the
-- first later statement of the line showing another symbol; deregistrations
-- are applied by the functions above, not here). Not used for decisions.
CREATE OR REPLACE VIEW sec_ticker_intervals AS
WITH statements AS (
    SELECT o.cik, o.class_key, o.available_on, o.adsh,
           array_agg(DISTINCT o.ticker ORDER BY o.ticker) AS tickers
    FROM sec_ticker_cik_observations o
    GROUP BY o.cik, o.class_key, o.available_on, o.adsh
), changes AS (
    SELECT s.*,
           lag(s.tickers) OVER w IS DISTINCT FROM s.tickers AS starts
    FROM statements s
    WINDOW w AS (PARTITION BY s.cik, s.class_key ORDER BY s.available_on, s.adsh)
), numbered AS (
    SELECT c.*, sum(c.starts::integer) OVER (
        PARTITION BY c.cik, c.class_key ORDER BY c.available_on, c.adsh
    ) AS run
    FROM changes c
), runs AS (
    SELECT n.cik, n.class_key, n.run, min(n.tickers) AS tickers,
           min(n.available_on) AS valid_from, max(n.available_on) AS last_confirmed_on,
           count(*) AS statements
    FROM numbered n
    GROUP BY n.cik, n.class_key, n.run
)
SELECT r.cik, r.class_key, t.ticker, r.valid_from, r.last_confirmed_on,
       lead(r.valid_from) OVER (PARTITION BY r.cik, r.class_key ORDER BY r.run) AS valid_to,
       r.statements
FROM runs r
CROSS JOIN LATERAL unnest(r.tickers) AS t(ticker);

COMMENT ON TABLE sec_ticker_cik_observations IS
    'SEC XBRL cover-page dei:TradingSymbol statements: line (cik, class_key) traded as '
    'ticker as of filing adsh. Loaded from DERA Financial Statement and Notes data sets.';
COMMENT ON FUNCTION sec_ticker_issuer_at(text, date, integer) IS
    'issuer_at(ticker, D): resolved|ambiguous|stale|ended|missing; see '
    'schemas/sec_ticker_cik_history_v1.sql.';

-- Ownership and grants. The loader writes as the owner; readers get SELECT and
-- EXECUTE only (default privileges would otherwise hand app_runtime writes).
REVOKE ALL ON TABLE sec_ticker_cik_observations, sec_cover_share_counts,
    sec_registration_events, sec_ticker_cik_packages, sec_ticker_intervals FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_ticker_lines_at(text, date, integer),
    sec_ticker_issuer_at(text, date, integer),
    sec_issuer_line_at(bigint, text, date, integer),
    sec_cover_class_shares_at(bigint, text, date, integer) FROM PUBLIC;
DO $$
DECLARE
    reader text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'worker_writer') THEN
        ALTER TABLE sec_ticker_cik_observations OWNER TO worker_writer;
        ALTER TABLE sec_cover_share_counts OWNER TO worker_writer;
        ALTER TABLE sec_registration_events OWNER TO worker_writer;
        ALTER TABLE sec_ticker_cik_packages OWNER TO worker_writer;
        ALTER VIEW sec_ticker_intervals OWNER TO worker_writer;
        ALTER FUNCTION sec_ticker_lines_at(text, date, integer) OWNER TO worker_writer;
        ALTER FUNCTION sec_ticker_issuer_at(text, date, integer) OWNER TO worker_writer;
        ALTER FUNCTION sec_issuer_line_at(bigint, text, date, integer) OWNER TO worker_writer;
        ALTER FUNCTION sec_cover_class_shares_at(bigint, text, date, integer)
            OWNER TO worker_writer;
    END IF;
    FOREACH reader IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = reader) THEN
            EXECUTE format(
                'REVOKE ALL ON TABLE sec_ticker_cik_observations, sec_cover_share_counts, '
                'sec_registration_events, sec_ticker_cik_packages, sec_ticker_intervals '
                'FROM %I', reader);
            EXECUTE format(
                'GRANT SELECT ON TABLE sec_ticker_cik_observations, sec_cover_share_counts, '
                'sec_registration_events, sec_ticker_cik_packages, sec_ticker_intervals '
                'TO %I', reader);
            EXECUTE format(
                'GRANT EXECUTE ON FUNCTION sec_ticker_lines_at(text, date, integer), '
                'sec_ticker_issuer_at(text, date, integer), '
                'sec_issuer_line_at(bigint, text, date, integer), '
                'sec_cover_class_shares_at(bigint, text, date, integer) TO %I', reader);
        END IF;
    END LOOP;
END $$;

COMMIT;
