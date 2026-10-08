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
--   Forms 15-12B, 15-12G, 15-15D, 25, 25-NSE and their /A amendments.
--
-- A CLASS is (cik, class_key): the context's dimension segments without the
-- listing-exchange axis, and without the legal-entity axis when that axis names a
-- registrant ('ClassOfStock=CommonClassA;'); '' when the fact has no dimensions.
-- Each row of sec_ticker_cik_observations is one dated statement by the
-- registrant: "as of filing adsh, this class trades as ticker". Each filing also
-- carries, on its rows, how many equity classes it shows in total
-- (filing_equity_classes: listed and unlisted, from symbols, share counts and
-- titled classes) and whether it reports a cover share count (filing_complete:
-- a 10-K/10-Q/20-F cover lists every class; an 8-K cover need not). Both are
-- computed from that filing alone.
--
-- BITEMPORAL. Facts are never deleted or overwritten. Every fact row has
-- * source_available_on: when the filing was public (EDGAR acceptance date, else
--   the filing date + 1);
-- * available_on: when this system could know the row: source_available_on for a
--   fact first loaded with its accession; for a correction (a fact a later
--   package version adds to an accession already loaded) the later of
--   source_available_on and the reconciliation date;
-- * retired_on: the reconciliation date on which no loaded package carries the
--   fact any more (NULL: current).
-- Point-in-time functions see a row at D iff available_on <= D and (retired_on is
-- NULL or retired_on > D). Lineage (sec_ticker_price_span) uses today's truth:
-- current rows at their source_available_on. History starts at the first load:
-- corrections folded into the packages before then are invisible.
--
-- Governed, owner-applied migration (postgres or worker_writer, psql with
-- ON_ERROR_STOP). Additive and idempotent; workers never apply it implicitly.
-- Rollback: schemas/sec_ticker_cik_history_v1.rollback.sql.
BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS sec_ticker_cik_observations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- md5 of every content column: one version of one fact.
    fact_hash text NOT NULL CHECK (fact_hash ~ '^[0-9a-f]{32}$'),
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
    -- From the title, else the symbol/segments: what the class is.
    security_kind text NOT NULL CHECK (security_kind IN (
        'equity', 'depositary', 'preferred', 'debt', 'warrant', 'unit', 'right'
    )),
    -- Equity classes the filing shows in total, and whether it reports a count.
    filing_equity_classes integer NOT NULL CHECK (filing_equity_classes >= 0),
    filing_complete boolean NOT NULL,
    -- The fact's ddate as DERA publishes it (rounded to a month end; informational).
    ddate date,
    form text NOT NULL,
    period date,
    filed date NOT NULL,
    -- EDGAR acceptance datetime as published by DERA: America/New_York wall
    -- clock, no zone. NULL when the package does not carry it.
    accepted timestamp,
    source_available_on date GENERATED ALWAYS AS (COALESCE(accepted::date, filed + 1)) STORED,
    available_on date NOT NULL,
    retired_on date,
    loaded_on date NOT NULL,
    source_package text NOT NULL,
    CHECK (available_on >= COALESCE(accepted::date, filed + 1))
);

CREATE UNIQUE INDEX IF NOT EXISTS sec_ticker_cik_observations_current_idx
    ON sec_ticker_cik_observations (fact_hash) WHERE retired_on IS NULL;
CREATE INDEX IF NOT EXISTS sec_ticker_cik_observations_key_idx
    ON sec_ticker_cik_observations (ticker_key, available_on DESC);
CREATE INDEX IF NOT EXISTS sec_ticker_cik_observations_filing_idx
    ON sec_ticker_cik_observations (cik, available_on DESC, adsh DESC);
CREATE INDEX IF NOT EXISTS sec_ticker_cik_observations_class_idx
    ON sec_ticker_cik_observations (cik, class_key, available_on DESC);
CREATE INDEX IF NOT EXISTS sec_ticker_cik_observations_adsh_idx
    ON sec_ticker_cik_observations (adsh, cik);

-- Cover-page dei:EntityCommonStockSharesOutstanding, per class or in total.
CREATE TABLE IF NOT EXISTS sec_cover_share_counts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fact_hash text NOT NULL CHECK (fact_hash ~ '^[0-9a-f]{32}$'),
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    dimh text NOT NULL,
    segments text NOT NULL,
    class_key text NOT NULL,
    -- The date the cover states the count as of: DERA's rounded ddate minus its
    -- datp (signed days from the stated date to the month end), checked against
    -- the exact XBRL contexts of the same accessions.
    stated_on date NOT NULL,
    ddate_rounded date NOT NULL,
    shares numeric NOT NULL CHECK (shares >= 0),
    form text NOT NULL,
    filed date NOT NULL,
    accepted timestamp,
    source_available_on date GENERATED ALWAYS AS (COALESCE(accepted::date, filed + 1)) STORED,
    available_on date NOT NULL,
    retired_on date,
    loaded_on date NOT NULL,
    source_package text NOT NULL,
    CHECK (available_on >= COALESCE(accepted::date, filed + 1))
);

CREATE UNIQUE INDEX IF NOT EXISTS sec_cover_share_counts_current_idx
    ON sec_cover_share_counts (fact_hash) WHERE retired_on IS NULL;
CREATE INDEX IF NOT EXISTS sec_cover_share_counts_line_idx
    ON sec_cover_share_counts (cik, class_key, stated_on DESC, available_on DESC);
CREATE INDEX IF NOT EXISTS sec_cover_share_counts_adsh_idx
    ON sec_cover_share_counts (adsh, cik);

-- Deregistration / delisting filings from the EDGAR form indexes (date only).
-- An amendment (form || '/A') supersedes exactly one original: the latest
-- original of its form for the same CIK filed on or before it (ties broken by
-- accession), from the amendment's own knowledge date. The index cannot say
-- whether it corrects or withdraws the original (Minim's 25-NSE/A of
-- 2025-04-09 withdrew a 2024 delisting), so from then on that original no
-- longer ends a hold.
CREATE TABLE IF NOT EXISTS sec_registration_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fact_hash text NOT NULL CHECK (fact_hash ~ '^[0-9a-f]{32}$'),
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    form text NOT NULL CHECK (form IN (
        '15-12B', '15-12G', '15-15D', '25', '25-NSE',
        '15-12B/A', '15-12G/A', '15-15D/A', '25/A', '25-NSE/A'
    )),
    filed date NOT NULL,
    source_available_on date GENERATED ALWAYS AS (filed + 1) STORED,
    available_on date NOT NULL,
    retired_on date,
    loaded_on date NOT NULL,
    source_package text NOT NULL,
    CHECK (available_on >= filed + 1)
);

CREATE UNIQUE INDEX IF NOT EXISTS sec_registration_events_current_idx
    ON sec_registration_events (fact_hash) WHERE retired_on IS NULL;
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

-- The (accession, CIK) pairs each package or index contains: an accession seen
-- before makes a newly carried fact of it a correction.
CREATE TABLE IF NOT EXISTS sec_ticker_cik_package_members (
    source_package text NOT NULL,
    adsh text NOT NULL,
    cik bigint NOT NULL,
    PRIMARY KEY (source_package, adsh, cik)
);

CREATE INDEX IF NOT EXISTS sec_ticker_cik_package_members_adsh_idx
    ON sec_ticker_cik_package_members (adsh, cik);

-- Which fact versions each package carries: a fact is retired only when no
-- loaded package carries it any more.
CREATE TABLE IF NOT EXISTS sec_ticker_cik_package_facts (
    source_package text NOT NULL,
    fact_table text NOT NULL CHECK (fact_table IN ('observation', 'share_count', 'event')),
    fact_hash text NOT NULL,
    PRIMARY KEY (source_package, fact_table, fact_hash)
);

CREATE INDEX IF NOT EXISTS sec_ticker_cik_package_facts_hash_idx
    ON sec_ticker_cik_package_facts (fact_table, fact_hash);

-- Visible rows. p_current = false: what was known at D (available_on <= D, not
-- yet retired at D). p_current = true: today's truth up to D (current rows whose
-- filing was public by D). Inlined into every caller.
CREATE OR REPLACE FUNCTION sec_observations_at(p_as_of date, p_current boolean)
RETURNS SETOF sec_ticker_cik_observations
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT o.* FROM sec_ticker_cik_observations o
WHERE CASE WHEN p_current
           THEN o.retired_on IS NULL AND o.source_available_on <= p_as_of
           ELSE o.available_on <= p_as_of AND (o.retired_on IS NULL OR o.retired_on > p_as_of)
      END
$fn$;

CREATE OR REPLACE FUNCTION sec_share_counts_at(p_as_of date, p_current boolean)
RETURNS SETOF sec_cover_share_counts
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT c.* FROM sec_cover_share_counts c
WHERE CASE WHEN p_current
           THEN c.retired_on IS NULL AND c.source_available_on <= p_as_of
           ELSE c.available_on <= p_as_of AND (c.retired_on IS NULL OR c.retired_on > p_as_of)
      END
$fn$;

-- The deregistration/delisting events of a CIK that end holds at p_as_of:
-- visible originals not superseded by a visible amendment of their form.
CREATE OR REPLACE FUNCTION sec_registration_end_events(
    p_cik bigint, p_as_of date, p_current boolean DEFAULT false
)
RETURNS TABLE (available_on date, form text, adsh text)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH visible AS (
    SELECT e.adsh, e.form, e.filed,
           CASE WHEN p_current THEN e.source_available_on ELSE e.available_on END AS known_on
    FROM sec_registration_events e
    WHERE e.cik = p_cik
      AND CASE WHEN p_current
               THEN e.retired_on IS NULL AND e.source_available_on <= p_as_of
               ELSE e.available_on <= p_as_of
                    AND (e.retired_on IS NULL OR e.retired_on > p_as_of)
          END
), superseded AS (
    SELECT (
        SELECT o.adsh FROM visible o
        WHERE o.form = left(a.form, -2) AND o.filed <= a.filed
        ORDER BY o.filed DESC, o.adsh DESC
        LIMIT 1
    ) AS adsh
    FROM visible a
    WHERE a.form LIKE '%/A'
)
SELECT v.known_on, v.form, v.adsh
FROM visible v
WHERE v.form NOT LIKE '%/A'
  AND v.adsh NOT IN (SELECT s.adsh FROM superseded s WHERE s.adsh IS NOT NULL)
$fn$;

-- Each CIK's hold of a ticker at D (internal helper; one row per CIK that showed
-- the ticker by D). Equity/depositary rows decide whenever one showed the
-- ticker (filers also tag their common symbol on notes lines). A hold is
-- followed through:
--   (a) every filing that tags a class that showed the ticker, and
--   (b) when the latest complete filing showing the ticker listed one equity
--       class: every later complete filing (the issuer's sole security, however
--       its member is called, until a complete filing says otherwise).
-- The statement is the latest such filing; the hold has ended when the statement
-- does not show the ticker in those classes ('other_symbol'), or when a
-- 15-12G/15-15D, or a 15-12B/25/25-NSE after a statement filing listing a
-- single symbol, ends it (not superseded by an amendment); an open hold whose
-- statement is older than p_max_age_days is stale. Statements and events are
-- ordered by knowledge date, acceptance time, then accession.
CREATE OR REPLACE FUNCTION sec_ticker_holds(
    p_ticker text, p_as_of date, p_max_age_days integer DEFAULT 400,
    p_current boolean DEFAULT false
)
RETURNS TABLE (
    cik bigint,
    state text,
    class_key text,
    security_kind text,
    statement_on date,
    statement_adsh text,
    confirmed_on date,
    end_reason text
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH shown AS (
    SELECT o.cik, o.class_key, o.security_kind, o.adsh, o.accepted,
           CASE WHEN p_current THEN o.source_available_on ELSE o.available_on END AS known_on,
           o.filing_equity_classes, o.filing_complete
    FROM sec_observations_at(p_as_of, p_current) o
    WHERE o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
), relevant AS (
    SELECT s.* FROM shown s
    WHERE s.security_kind IN ('equity', 'depositary')
       OR NOT EXISTS (SELECT 1 FROM shown e WHERE e.security_kind IN ('equity', 'depositary'))
), per_cik AS (
    SELECT r.cik,
           max(r.known_on) AS confirmed_on,
           (array_agg(r.class_key ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh DESC))[1] AS class_key,
           min(r.security_kind) AS security_kind,
           array_agg(DISTINCT r.class_key) AS classes,
           COALESCE((array_agg(r.filing_equity_classes = 1 ORDER BY r.known_on DESC,
                               r.accepted DESC NULLS LAST, r.adsh DESC)
                     FILTER (WHERE r.filing_complete))[1], false) AS sole
    FROM relevant r
    GROUP BY r.cik
), statement AS (
    SELECT p.*, st.adsh AS statement_adsh, st.known_on AS statement_on
    FROM per_cik p
    CROSS JOIN LATERAL (
        SELECT f.adsh, CASE WHEN p_current THEN f.source_available_on ELSE f.available_on END
                   AS known_on
        FROM sec_observations_at(p_as_of, p_current) f
        WHERE f.cik = p.cik
          AND (f.class_key = ANY(p.classes)
               OR (p.sole AND f.filing_complete
                   AND f.security_kind IN ('equity', 'depositary')))
        ORDER BY known_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
        LIMIT 1
    ) st
), judged AS (
    SELECT s.*,
           EXISTS (
               SELECT 1 FROM sec_observations_at(p_as_of, p_current) g
               WHERE g.adsh = s.statement_adsh AND g.cik = s.cik
                 AND g.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
                 AND (g.class_key = ANY(s.classes)
                      OR (s.sole AND g.filing_complete
                          AND g.security_kind IN ('equity', 'depositary')))
           ) AS shows,
           (
               SELECT e.form FROM sec_registration_end_events(s.cik, p_as_of, p_current) e
               WHERE e.available_on > s.statement_on
                 AND (e.form IN ('15-12G', '15-15D') OR (
                     SELECT count(DISTINCT x.ticker_key)
                     FROM sec_observations_at(p_as_of, p_current) x
                     WHERE x.adsh = s.statement_adsh AND x.cik = s.cik) = 1)
               ORDER BY e.available_on, e.adsh
               LIMIT 1
           ) AS end_form
    FROM statement s
)
SELECT j.cik,
       CASE
           WHEN NOT j.shows OR j.end_form IS NOT NULL THEN 'ended'
           WHEN j.statement_on < p_as_of - p_max_age_days THEN 'stale'
           ELSE 'active'
       END AS state,
       j.class_key, j.security_kind, j.statement_on, j.statement_adsh, j.confirmed_on,
       CASE WHEN NOT j.shows THEN 'other_symbol' ELSE j.end_form END AS end_reason
FROM judged j
$fn$;

-- issuer_at(ticker, D): the (CIK, class) the ticker belonged to at D, from what
-- was known at D (see sec_ticker_holds).
-- * 'resolved'  : exactly one CIK holds the ticker actively; class_key is the
--                 class of its latest statement showing the ticker.
-- * 'ambiguous' : two or more CIKs hold it actively at D (cik NULL; active_ciks).
-- * 'stale'     : no active hold, but an open one lacks a confirmation within
--                 p_max_age_days (cik NULL; active_ciks = its holders).
-- * 'ended'     : every hold that showed the ticker ended by D.
-- * 'missing'   : nothing known at D showed the ticker.
-- observed_on/adsh: the deciding hold's last confirmation and its statement.
-- Exactly one row for any input. SECURITY INVOKER with no SET clause so the
-- planner can inline it; it reads the tables the caller's search_path resolves.
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
WITH ranked AS (
    SELECT h.*,
           CASE h.state WHEN 'active' THEN 0 WHEN 'stale' THEN 1 ELSE 2 END AS rank_state
    FROM sec_ticker_holds(p_ticker, p_as_of, p_max_age_days, false) h
), decided AS (
    SELECT r.* FROM ranked r
    ORDER BY r.rank_state, r.statement_on DESC, r.confirmed_on DESC, r.cik
    LIMIT 1
), holders AS (
    SELECT r.cik, r.confirmed_on
    FROM ranked r
    WHERE r.rank_state = (SELECT d.rank_state FROM decided d) AND r.rank_state < 2
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

-- What a class (cik, class_key) traded as at D, from what was known at D. Used
-- to follow a rename backwards: the caller's class, asked at an earlier D, shows
-- the symbol it had then. The class is followed through the filings that tag it
-- and, when its latest complete filing listed one equity class (or the class is
-- not yet known at D), through later complete filings that list one equity
-- class. status: 'resolved' | 'stale' | 'ended' | 'missing' | 'ambiguous_class'
-- (the class is not known at D and the issuer then listed several equity
-- classes). equity_lines: equity classes in the issuer's latest complete filing
-- by D (used to decide whether an issuer total can size one class).
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
WITH own AS (
    SELECT o.available_on, o.accepted, o.adsh, o.filing_equity_classes, o.filing_complete
    FROM sec_observations_at(p_as_of, false) o
    WHERE o.cik = p_cik AND o.class_key = p_class_key
), follows_sole AS (
    SELECT CASE
               WHEN NOT EXISTS (SELECT 1 FROM own) THEN true
               ELSE COALESCE((array_agg(o.filing_equity_classes = 1 ORDER BY o.available_on DESC,
                                        o.accepted DESC NULLS LAST, o.adsh DESC)
                              FILTER (WHERE o.filing_complete))[1], false)
           END AS sole
    FROM own o
), latest_complete AS (
    SELECT f.adsh, f.filing_equity_classes
    FROM sec_observations_at(p_as_of, false) f
    WHERE f.cik = p_cik AND f.filing_complete
    ORDER BY f.available_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
    LIMIT 1
), chosen AS (
    SELECT f.available_on, f.adsh
    FROM sec_observations_at(p_as_of, false) f
    WHERE f.cik = p_cik
      AND (f.class_key = p_class_key
           OR ((SELECT s.sole FROM follows_sole s) AND f.filing_complete
               AND f.filing_equity_classes = 1
               AND f.security_kind IN ('equity', 'depositary')))
    ORDER BY f.available_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
    LIMIT 1
), statement AS (
    SELECT c.available_on, c.adsh,
           ARRAY(SELECT DISTINCT s.ticker FROM sec_observations_at(p_as_of, false) s
                 WHERE s.adsh = c.adsh AND s.cik = p_cik
                   AND (s.class_key = p_class_key
                        OR (s.filing_complete AND s.filing_equity_classes = 1
                            AND s.security_kind IN ('equity', 'depositary')))
                 ORDER BY s.ticker) AS tickers,
           (SELECT (array_agg(s.class_key ORDER BY s.class_key = p_class_key DESC,
                              s.class_key))[1]
            FROM sec_observations_at(p_as_of, false) s
            WHERE s.adsh = c.adsh AND s.cik = p_cik
              AND (s.class_key = p_class_key
                   OR (s.filing_complete AND s.filing_equity_classes = 1
                       AND s.security_kind IN ('equity', 'depositary')))) AS class_key,
           (SELECT min(s.security_kind)
            FROM sec_observations_at(p_as_of, false) s
            WHERE s.adsh = c.adsh AND s.cik = p_cik
              AND (s.class_key = p_class_key
                   OR (s.filing_complete AND s.filing_equity_classes = 1
                       AND s.security_kind IN ('equity', 'depositary')))) AS security_kind,
           EXISTS (
               SELECT 1 FROM sec_registration_end_events(p_cik, p_as_of, false) e
               WHERE e.available_on > c.available_on
                 AND (e.form IN ('15-12G', '15-15D') OR (
                     SELECT count(DISTINCT x.ticker_key)
                     FROM sec_observations_at(p_as_of, false) x
                     WHERE x.adsh = c.adsh AND x.cik = p_cik) = 1)
           ) AS deregistered
    FROM chosen c
)
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM statement) THEN
            CASE WHEN COALESCE((SELECT l.filing_equity_classes FROM latest_complete l), 0) > 1
                 THEN 'ambiguous_class' ELSE 'missing' END
        WHEN (SELECT s.deregistered FROM statement s) THEN 'ended'
        WHEN (SELECT s.available_on FROM statement s) < p_as_of - p_max_age_days THEN 'stale'
        ELSE 'resolved'
    END AS status,
    (SELECT s.class_key FROM statement s) AS class_key,
    COALESCE((SELECT s.tickers FROM statement s), '{}'::text[]) AS tickers,
    (SELECT s.security_kind FROM statement s) AS security_kind,
    (SELECT s.available_on FROM statement s) AS statement_on,
    (SELECT s.adsh FROM statement s) AS adsh,
    COALESCE((SELECT l.filing_equity_classes FROM latest_complete l), 0) AS equity_lines
$fn$;

-- The class's own cover-page share count known at D: the latest stated date
-- (stated_on <= D), then the latest filing (knowledge date, acceptance time,
-- accession). A date older than p_max_age_days -> 'stale'; distinct values
-- within that one filing -> 'ambiguous'; none -> 'missing'. class_key '' is the
-- issuer total.
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
    SELECT c.stated_on, c.available_on, c.adsh
    FROM sec_share_counts_at(p_as_of, false) c
    WHERE c.cik = p_cik AND c.class_key = p_class_key AND c.stated_on <= p_as_of
    ORDER BY c.stated_on DESC, c.available_on DESC, c.accepted DESC NULLS LAST, c.adsh DESC
    LIMIT 1
), counts AS (
    SELECT DISTINCT c.shares
    FROM sec_share_counts_at(p_as_of, false) c, chosen h
    WHERE c.cik = p_cik AND c.class_key = p_class_key
      AND c.adsh = h.adsh AND c.stated_on = h.stated_on
)
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM chosen) THEN 'missing'
        WHEN (SELECT h.stated_on FROM chosen h) < p_as_of - p_max_age_days THEN 'stale'
        WHEN (SELECT count(*) FROM counts) > 1 THEN 'ambiguous'
        ELSE 'resolved'
    END AS status,
    CASE WHEN (SELECT count(*) FROM counts) = 1 THEN (SELECT k.shares FROM counts k) END
        AS shares,
    (SELECT h.stated_on FROM chosen h) AS shares_as_of,
    (SELECT h.adsh FROM chosen h) AS adsh
$fn$;

-- The cover share count of the class that trades as p_ticker, known at D, joined
-- inside each filing:
-- * 'class': a count on a (dimensioned) class tagged with the ticker in the same
--   filing; member names change between filings (Berkshire's 10-Q counts
--   'CommonClassB' while its 8-K covers tag BRK.B on 'ClassBCommonStock'), so the
--   filing, not the member, ties a count to a symbol;
-- * 'sole_class_total': the filing's undimensioned total, only when the filing
--   shows exactly one equity class in total (listed or not) and that class is an
--   equity (never a depositary) line showing the ticker.
-- Latest stated date first, then the latest filing; a class count wins over a
-- total in one filing. status resolved | stale | ambiguous | missing.
CREATE OR REPLACE FUNCTION sec_cover_ticker_shares_at(
    p_ticker text, p_cik bigint, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    status text,
    shares numeric,
    shares_as_of date,
    adsh text,
    basis text
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH candidates AS (
    SELECT c.adsh, c.stated_on, c.available_on, c.accepted, c.shares,
           CASE WHEN c.class_key = '' THEN 'sole_class_total' ELSE 'class' END AS basis
    FROM sec_share_counts_at(p_as_of, false) c
    WHERE c.cik = p_cik AND c.stated_on <= p_as_of
      AND CASE
              WHEN c.class_key <> '' THEN EXISTS (
                  SELECT 1 FROM sec_observations_at(p_as_of, false) o
                  WHERE o.adsh = c.adsh AND o.cik = c.cik AND o.class_key = c.class_key
                    AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g'))
              ELSE EXISTS (
                  SELECT 1 FROM sec_observations_at(p_as_of, false) o
                  WHERE o.adsh = c.adsh AND o.cik = c.cik
                    AND o.filing_equity_classes = 1 AND o.security_kind = 'equity'
                    AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g'))
          END
), chosen AS (
    SELECT k.* FROM candidates k
    ORDER BY k.stated_on DESC, k.available_on DESC, k.accepted DESC NULLS LAST, k.adsh DESC,
             k.basis
    LIMIT 1
), counts AS (
    SELECT DISTINCT k.shares
    FROM candidates k, chosen h
    WHERE k.adsh = h.adsh AND k.stated_on = h.stated_on AND k.basis = h.basis
)
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM chosen) THEN 'missing'
        WHEN (SELECT h.stated_on FROM chosen h) < p_as_of - p_max_age_days THEN 'stale'
        WHEN (SELECT count(*) FROM counts) > 1 THEN 'ambiguous'
        ELSE 'resolved'
    END AS status,
    CASE WHEN (SELECT count(*) FROM counts) = 1 THEN (SELECT k.shares FROM counts k) END
        AS shares,
    (SELECT h.stated_on FROM chosen h) AS shares_as_of,
    (SELECT h.adsh FROM chosen h) AS adsh,
    (SELECT h.basis FROM chosen h) AS basis
$fn$;

-- Price lineage: which stored price rows of a ticker belong to an issuer.
-- This is a data-lineage question about a vendor series stitched under today's
-- symbol, so it uses today's truth (current rows at their filing's public date;
-- issuer resolution at D stays point-in-time through sec_ticker_issuer_at).
-- The holds are those of sec_ticker_holds, evaluated with today's truth at every
-- date a relevant filing or event of a holder became public; one row per RUN of
-- p_cik (when p_class_key is given, only if that class ever showed the ticker):
-- * valid_from        : the date the run starts;
-- * valid_to          : the date its end is evidenced (end_reason 'other_symbol'
--                       or the deregistration form); NULL = open;
-- * last_confirmed_on : the run's latest statement showing the ticker;
-- * prior_holder_end  : the latest end (valid_to, else last confirmation) of a
--                       run of ANOTHER CIK that started before this run;
-- * next_holder_start : the earliest start of another CIK's run on or after
--                       this run's start; NULL if none;
-- * line_key          : the run's class (kept for callers that order by it).
-- Consumers admit rows in [valid_from, valid_to), rows in
-- (prior_holder_end, valid_from) only after a continuity check, and refuse rows
-- at or before prior_holder_end or at or after next_holder_start.
CREATE OR REPLACE FUNCTION sec_ticker_price_span(
    p_ticker text, p_cik bigint, p_class_key text DEFAULT NULL
)
RETURNS TABLE (
    class_key text,
    valid_from date,
    valid_to date,
    end_reason text,
    last_confirmed_on date,
    prior_holder_end date,
    next_holder_start date,
    line_key text
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH holders AS (
    SELECT DISTINCT o.cik
    FROM sec_observations_at('infinity'::date, true) o
    WHERE o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
), bounds AS (
    SELECT DISTINCT b.on_date
    FROM (
        SELECT o.source_available_on AS on_date
        FROM sec_observations_at('infinity'::date, true) o
        WHERE o.cik IN (SELECT h.cik FROM holders h)
        UNION
        SELECT e.available_on
        FROM holders h
        CROSS JOIN LATERAL sec_registration_end_events(h.cik, 'infinity'::date, true) e
    ) b
), states AS (
    SELECT b.on_date, s.cik, s.state, s.class_key, s.confirmed_on, s.end_reason
    FROM bounds b
    CROSS JOIN LATERAL sec_ticker_holds(p_ticker, b.on_date, 2147483647, true) s
), marked AS (
    SELECT st.*,
           st.state = 'active' AS holding,
           sum(CASE WHEN st.state = 'active' AND NOT COALESCE(st.prev_active, false)
                    THEN 1 ELSE 0 END)
               OVER (PARTITION BY st.cik ORDER BY st.on_date) AS run_no
    FROM (
        SELECT s.*, lag(s.state = 'active') OVER (PARTITION BY s.cik ORDER BY s.on_date)
                   AS prev_active
        FROM states s
    ) st
), runs AS (
    SELECT m.cik, m.run_no,
           min(m.on_date) FILTER (WHERE m.holding) AS valid_from,
           max(m.confirmed_on) FILTER (WHERE m.holding) AS last_confirmed_on,
           (array_agg(m.class_key ORDER BY m.on_date DESC) FILTER (WHERE m.holding))[1]
               AS class_key
    FROM marked m
    WHERE m.run_no > 0
    GROUP BY m.cik, m.run_no
), ends AS (
    SELECT DISTINCT ON (m.cik, m.run_no) m.cik, m.run_no, m.on_date AS valid_to, m.end_reason
    FROM marked m
    WHERE m.run_no > 0 AND NOT m.holding
    ORDER BY m.cik, m.run_no, m.on_date
), spans AS (
    SELECT r.cik, r.class_key, r.valid_from, e.valid_to, e.end_reason, r.last_confirmed_on
    FROM runs r
    LEFT JOIN ends e ON e.cik = r.cik AND e.run_no = r.run_no
)
SELECT a.class_key, a.valid_from, a.valid_to, a.end_reason, a.last_confirmed_on,
       (SELECT max(COALESCE(o.valid_to, o.last_confirmed_on)) FROM spans o
        WHERE o.cik <> a.cik AND o.valid_from < a.valid_from) AS prior_holder_end,
       (SELECT min(o.valid_from) FROM spans o
        WHERE o.cik <> a.cik AND o.valid_from >= a.valid_from) AS next_holder_start,
       a.class_key AS line_key
FROM spans a
WHERE a.cik = p_cik
  AND (p_class_key IS NULL OR EXISTS (
      SELECT 1 FROM sec_observations_at('infinity'::date, true) o
      WHERE o.cik = p_cik AND o.class_key = p_class_key
        AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')))
ORDER BY a.valid_from
$fn$;

-- Diagnostics: every class interval as known today (current rows). A symbol
-- superseded on the day it appeared is in force on no date and is not listed.
-- Deregistrations are applied by the functions above, not here.
CREATE OR REPLACE VIEW sec_ticker_intervals AS
WITH statements AS (
    SELECT o.cik, o.class_key, o.source_available_on AS on_date,
           max(o.accepted) AS accepted, o.adsh,
           array_agg(DISTINCT o.ticker ORDER BY o.ticker) AS tickers
    FROM sec_ticker_cik_observations o
    WHERE o.retired_on IS NULL
    GROUP BY o.cik, o.class_key, o.source_available_on, o.adsh
), changes AS (
    SELECT s.*,
           lag(s.tickers) OVER w IS DISTINCT FROM s.tickers AS starts
    FROM statements s
    WINDOW w AS (PARTITION BY s.cik, s.class_key
                 ORDER BY s.on_date, s.accepted NULLS FIRST, s.adsh)
), numbered AS (
    SELECT c.*, sum(c.starts::integer) OVER (
        PARTITION BY c.cik, c.class_key ORDER BY c.on_date, c.accepted NULLS FIRST, c.adsh
    ) AS run
    FROM changes c
), runs AS (
    SELECT n.cik, n.class_key, n.run, min(n.tickers) AS tickers,
           min(n.on_date) AS valid_from, max(n.on_date) AS last_confirmed_on,
           count(*) AS statements
    FROM numbered n
    GROUP BY n.cik, n.class_key, n.run
), bounded AS (
    -- The next run's start, taken before the symbols are expanded.
    SELECT r.*, lead(r.valid_from) OVER (
        PARTITION BY r.cik, r.class_key ORDER BY r.run
    ) AS valid_to
    FROM runs r
)
SELECT b.cik, b.class_key, t.ticker, b.valid_from, b.last_confirmed_on, b.valid_to,
       b.statements
FROM bounded b
CROSS JOIN LATERAL unnest(b.tickers) AS t(ticker)
WHERE b.valid_to IS NULL OR b.valid_to > b.valid_from;

COMMENT ON TABLE sec_ticker_cik_observations IS
    'SEC XBRL cover-page dei:TradingSymbol statements (bitemporal): class (cik, class_key) '
    'traded as ticker as of filing adsh. Loaded from DERA Financial Statement and Notes.';
COMMENT ON FUNCTION sec_ticker_issuer_at(text, date, integer) IS
    'issuer_at(ticker, D): resolved|ambiguous|stale|ended|missing; see '
    'schemas/sec_ticker_cik_history_v1.sql.';

-- Ownership and grants. The loader writes as the owner; readers get SELECT and
-- EXECUTE only (default privileges would otherwise hand app_runtime writes).
REVOKE ALL ON TABLE sec_ticker_cik_observations, sec_cover_share_counts,
    sec_registration_events, sec_ticker_cik_packages, sec_ticker_cik_package_members,
    sec_ticker_cik_package_facts, sec_ticker_intervals FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_observations_at(date, boolean),
    sec_share_counts_at(date, boolean),
    sec_registration_end_events(bigint, date, boolean),
    sec_ticker_holds(text, date, integer, boolean),
    sec_ticker_issuer_at(text, date, integer),
    sec_issuer_line_at(bigint, text, date, integer),
    sec_cover_class_shares_at(bigint, text, date, integer),
    sec_cover_ticker_shares_at(text, bigint, date, integer),
    sec_ticker_price_span(text, bigint, text) FROM PUBLIC;
DO $$
DECLARE
    relations constant text[] := ARRAY[
        'sec_ticker_cik_observations', 'sec_cover_share_counts', 'sec_registration_events',
        'sec_ticker_cik_packages', 'sec_ticker_cik_package_members',
        'sec_ticker_cik_package_facts', 'sec_ticker_intervals'];
    routines constant text[] := ARRAY[
        'sec_observations_at(date, boolean)',
        'sec_share_counts_at(date, boolean)',
        'sec_registration_end_events(bigint, date, boolean)',
        'sec_ticker_holds(text, date, integer, boolean)',
        'sec_ticker_issuer_at(text, date, integer)',
        'sec_issuer_line_at(bigint, text, date, integer)',
        'sec_cover_class_shares_at(bigint, text, date, integer)',
        'sec_cover_ticker_shares_at(text, bigint, date, integer)',
        'sec_ticker_price_span(text, bigint, text)'];
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
