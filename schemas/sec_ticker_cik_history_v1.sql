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
-- A security CLASS is (cik, class_key): class_key is the context's dimension
-- segments without the listing-exchange axis, and without the legal-entity axis
-- when that axis names a registrant ('ClassOfStock=CommonClassA;'); '' when the
-- fact has no dimensions. A LINE is (cik, line_key): the class, except that the
-- one equity/depositary class of a filing that lists exactly one is the issuer's
-- sole equity line '*', whatever its member is called in that filing (single-class
-- filers add, drop and rename the class dimension between filings). Each row of
-- sec_ticker_cik_observations is one dated statement by the registrant: "as of
-- filing adsh, this class trades as ticker". Rows are never rewritten from later
-- knowledge (sub.prevrpt is not applied retroactively); a row is usable from
-- available_on, its knowledge date.
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
    -- '*' for the sole equity/depositary class of its filing, else class_key.
    line_key text NOT NULL,
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
    -- The fact's ddate as DERA publishes it (rounded to a month end; informational).
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
    ON sec_ticker_cik_observations (cik, line_key, available_on DESC, adsh DESC);
CREATE INDEX IF NOT EXISTS sec_ticker_cik_observations_class_idx
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
    -- The date the cover states the count as of: DERA's rounded ddate minus its
    -- datp (days from the stated date to the month end), checked against the
    -- exact XBRL contexts across the 2010-2024 vintages.
    stated_on date NOT NULL,
    ddate_rounded date NOT NULL,
    shares numeric NOT NULL CHECK (shares >= 0),
    form text NOT NULL,
    filed date NOT NULL,
    accepted timestamp,
    available_on date GENERATED ALWAYS AS (COALESCE(accepted::date, filed + 1)) STORED,
    source_package text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (adsh, dimh, stated_on, shares)
);

CREATE INDEX IF NOT EXISTS sec_cover_share_counts_line_idx
    ON sec_cover_share_counts (cik, class_key, stated_on DESC, available_on DESC);

-- Deregistration / delisting filings from the EDGAR form indexes (date only).
-- An amendment (form || '/A') supersedes the latest earlier original of its
-- form for the same CIK from its own knowledge date: the index cannot say
-- whether it corrects or withdraws the original (Minim's 25-NSE/A of 2025-04-09
-- withdrew a 2024 delisting), so from then on the original no longer ends a hold.
CREATE TABLE IF NOT EXISTS sec_registration_events (
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    form text NOT NULL CHECK (form IN (
        '15-12B', '15-12G', '15-15D', '25', '25-NSE',
        '15-12B/A', '15-12G/A', '15-15D/A', '25/A', '25-NSE/A'
    )),
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

-- Which (accession, CIK) pairs each package contains: a package reload removes
-- the evidence of members it no longer contains unless another package does.
CREATE TABLE IF NOT EXISTS sec_ticker_cik_package_members (
    source_package text NOT NULL,
    adsh text NOT NULL,
    cik bigint NOT NULL,
    PRIMARY KEY (source_package, adsh, cik)
);

CREATE INDEX IF NOT EXISTS sec_ticker_cik_package_members_adsh_idx
    ON sec_ticker_cik_package_members (adsh, cik);

-- The deregistration/delisting events of a CIK that still end holds when known
-- at p_as_of (NULL: everything known today): originals not superseded by an
-- amendment of their own form public by then.
CREATE OR REPLACE FUNCTION sec_registration_end_events(p_cik bigint, p_as_of date)
RETURNS TABLE (available_on date, form text, adsh text)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT e.available_on, e.form, e.adsh
FROM sec_registration_events e
WHERE e.cik = p_cik
  AND e.form IN ('15-12B', '15-12G', '15-15D', '25', '25-NSE')
  AND (p_as_of IS NULL OR e.available_on <= p_as_of)
  AND NOT EXISTS (
      SELECT 1
      FROM sec_registration_events a
      WHERE a.cik = e.cik AND a.form = e.form || '/A'
        AND a.filed >= e.filed
        AND (p_as_of IS NULL OR a.available_on <= p_as_of)
        AND NOT EXISTS (
            SELECT 1 FROM sec_registration_events n
            WHERE n.cik = e.cik AND n.form = e.form
              AND n.filed > e.filed AND n.filed <= a.filed
        )
  )
$fn$;

-- A line's state at D (internal helper; one row per line that ever showed the
-- key by D). A line holds the symbol from the knowledge date of the first filing
-- showing it until the knowledge date of the first later filing of the same line
-- showing another symbol, or of a deregistration that applies to it; computed at
-- D from what was public at D:
-- * statement  = the line's latest filing available on or before D;
-- * 'ended'    = that statement shows another symbol, or a later 15-12G/15-15D
--                (whole registrant) or 15-12B/25/25-NSE (when the statement's
--                filing listed a single symbol, so the class is unambiguous),
--                not superseded by an amendment, was public by D;
-- * 'stale'    = an open interval whose statement is older than p_max_age_days;
-- * 'active'   = otherwise.
-- class_key is the line's class in its latest observation of the ticker.
CREATE OR REPLACE FUNCTION sec_ticker_lines_at(
    p_ticker text, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    cik bigint,
    line_key text,
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
    -- The line's kind for this ticker over every statement showing it by D
    -- (an equity kind wins: one untitled statement must not demote it).
    SELECT o.cik, o.line_key, max(o.available_on) AS confirmed_on,
           (array_agg(o.class_key ORDER BY o.available_on DESC, o.adsh DESC))[1]
               AS class_key,
           COALESCE(min(o.security_kind) FILTER (
                        WHERE o.security_kind IN ('equity', 'depositary')),
                    min(o.security_kind)) AS line_kind
    FROM sec_ticker_cik_observations o
    WHERE o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
      AND o.available_on <= p_as_of
    GROUP BY o.cik, o.line_key
)
SELECT l.cik, l.line_key, l.class_key,
       CASE
           WHEN NOT st.shows_symbol THEN 'ended'
           WHEN EXISTS (
               SELECT 1 FROM sec_registration_end_events(l.cik, p_as_of) e
               WHERE e.available_on > st.statement_on
                 AND (e.form IN ('15-12G', '15-15D') OR (
                     SELECT count(DISTINCT f.ticker_key) FROM sec_ticker_cik_observations f
                     WHERE f.adsh = st.statement_adsh AND f.cik = l.cik) = 1)
           ) THEN 'ended'
           WHEN st.statement_on < p_as_of - p_max_age_days THEN 'stale'
           ELSE 'active'
       END AS state,
       l.line_kind AS security_kind, st.statement_on, st.statement_adsh, l.confirmed_on,
       st.shows_symbol
FROM lines l
CROSS JOIN LATERAL (
    SELECT s.available_on AS statement_on, s.adsh AS statement_adsh,
           bool_or(s.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g'))
               AS shows_symbol
    FROM sec_ticker_cik_observations s
    WHERE (s.cik, s.line_key, s.available_on, s.adsh) = (
        SELECT x.cik, x.line_key, x.available_on, x.adsh
        FROM sec_ticker_cik_observations x
        WHERE x.cik = l.cik AND x.line_key = l.line_key AND x.available_on <= p_as_of
        ORDER BY x.available_on DESC, x.adsh DESC
        LIMIT 1
    )
    GROUP BY s.available_on, s.adsh
) st
$fn$;

-- issuer_at(ticker, D): the (CIK, class) the ticker belonged to at D, from what
-- was public at D (see sec_ticker_lines_at for the interval rule).
-- Equity/depositary lines decide whenever one showed the ticker; other kinds
-- (notes, preferred...) only for a ticker no equity line ever showed.
-- * 'resolved'  : exactly one CIK holds an active interval; class_key is the
--                 class of its active line with the latest statement.
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
           CASE l.state WHEN 'active' THEN 0 WHEN 'stale' THEN 1 ELSE 2 END AS rank_state,
           -- Equity lines decide when there are any: filers also tag their
           -- common symbol on notes lines, which must not pick the class or
           -- make another issuer's notes line a rival holder.
           CASE WHEN l.security_kind IN ('equity', 'depositary') THEN 0 ELSE 1 END
               AS rank_kind
    FROM lines l
), decided AS (
    SELECT r.* FROM ranked r
    ORDER BY r.rank_kind, r.rank_state, r.statement_on DESC, r.confirmed_on DESC, r.cik,
             r.line_key
    LIMIT 1
), holders AS (
    SELECT r.cik, max(r.confirmed_on) AS confirmed_on
    FROM ranked r
    WHERE r.rank_state = (SELECT d.rank_state FROM decided d)
      AND r.rank_kind = (SELECT d.rank_kind FROM decided d) AND r.rank_state < 2
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

-- What a known class (cik, class_key) traded as at D. Used to follow a rename:
-- the current symbol's class, asked at an earlier D, shows the symbol its line
-- had then. The line is the class's line today (its latest observation: the
-- sole equity line '*' when today's filings list one equity class); the
-- statement is the latest filing by D of that line or of the class itself.
-- * status 'resolved' | 'stale' | 'ended' | 'missing' | 'ambiguous_class'
--   (no statement by D while the issuer then listed several equity classes).
-- * equity_lines: listed equity/depositary classes in the issuer's latest cover
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
WITH target AS (
    SELECT COALESCE((
        SELECT x.line_key FROM sec_ticker_cik_observations x
        WHERE x.cik = p_cik AND x.class_key = p_class_key
        ORDER BY x.available_on DESC, x.adsh DESC
        LIMIT 1
    ), p_class_key) AS line_key
), latest_filing AS (
    SELECT x.adsh, x.available_on
    FROM sec_ticker_cik_observations x
    WHERE x.cik = p_cik AND x.available_on <= p_as_of
    ORDER BY x.available_on DESC, x.adsh DESC
    LIMIT 1
), equity AS (
    SELECT DISTINCT f.class_key
    FROM sec_ticker_cik_observations f
    WHERE f.cik = p_cik AND f.adsh = (SELECT lf.adsh FROM latest_filing lf)
      AND f.security_kind IN ('equity', 'depositary')
), chosen AS (
    SELECT x.available_on, x.adsh
    FROM sec_ticker_cik_observations x
    WHERE x.cik = p_cik AND x.available_on <= p_as_of
      AND (x.line_key = (SELECT t.line_key FROM target t) OR x.class_key = p_class_key)
    ORDER BY x.available_on DESC, x.adsh DESC
    LIMIT 1
), statement AS (
    SELECT c.available_on, c.adsh,
           ARRAY(SELECT DISTINCT s.ticker FROM sec_ticker_cik_observations s
                 WHERE s.adsh = c.adsh AND s.cik = p_cik
                   AND (s.line_key = (SELECT t.line_key FROM target t)
                        OR s.class_key = p_class_key)
                 ORDER BY s.ticker) AS tickers,
           (SELECT (array_agg(s.class_key ORDER BY s.class_key))[1]
            FROM sec_ticker_cik_observations s
            WHERE s.adsh = c.adsh AND s.cik = p_cik
              AND (s.line_key = (SELECT t.line_key FROM target t)
                   OR s.class_key = p_class_key)) AS class_key,
           (SELECT COALESCE(min(s.security_kind) FILTER (
                                WHERE s.security_kind IN ('equity', 'depositary')),
                            min(s.security_kind))
            FROM sec_ticker_cik_observations s
            WHERE s.adsh = c.adsh AND s.cik = p_cik
              AND (s.line_key = (SELECT t.line_key FROM target t)
                   OR s.class_key = p_class_key)) AS security_kind,
           EXISTS (
               SELECT 1 FROM sec_registration_end_events(p_cik, p_as_of) e
               WHERE e.available_on > c.available_on
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
-- (stated_on <= D, filing available by D), then the latest filing (acceptance,
-- then accession). A date older than p_max_age_days -> 'stale'; distinct values
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
    FROM sec_cover_share_counts c
    WHERE c.cik = p_cik AND c.class_key = p_class_key
      AND c.available_on <= p_as_of AND c.stated_on <= p_as_of
    ORDER BY c.stated_on DESC, c.available_on DESC, c.accepted DESC NULLS LAST, c.adsh DESC
    LIMIT 1
), counts AS (
    SELECT DISTINCT c.shares
    FROM sec_cover_share_counts c, chosen h
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

-- The cover share count of the class that trades as p_ticker, joined inside each
-- filing: a count whose class is tagged with the ticker in the same filing, or
-- the filing's total when its one equity class (not a depositary line) shows
-- the ticker. Member names change between filings (Berkshire's 10-Q counts
-- 'CommonClassB' while its 8-K covers tag BRK.B on 'ClassBCommonStock'), so the
-- filing, not the member, ties a count to a symbol. Latest stated date first,
-- then the latest filing; a class count wins over a total in one filing.
-- status resolved | stale | ambiguous | missing; basis 'class' | 'sole_class_total'.
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
    FROM sec_cover_share_counts c
    WHERE c.cik = p_cik AND c.available_on <= p_as_of AND c.stated_on <= p_as_of
      AND (
          EXISTS (
              SELECT 1 FROM sec_ticker_cik_observations o
              WHERE o.adsh = c.adsh AND o.cik = c.cik AND o.class_key = c.class_key
                AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g'))
          OR (c.class_key = '' AND EXISTS (
              SELECT 1 FROM sec_ticker_cik_observations o
              WHERE o.adsh = c.adsh AND o.cik = c.cik AND o.line_key = '*'
                AND o.security_kind = 'equity'
                AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')))
      )
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

-- Price lineage: which stored price rows of a ticker belong to an issuer's line.
-- This is a data-lineage question about a vendor series stitched under today's
-- symbol, so it uses everything known today (issuer resolution at D stays
-- point-in-time through sec_ticker_issuer_at). One row per RUN: a maximal
-- stretch in which a line of p_cik (only lines of class p_class_key when given)
-- holds the ticker key, by the same rules as the point functions.
-- * valid_from        : knowledge date of the run's first statement;
-- * valid_to          : knowledge date of the first evidence of its end -- a later
--                       statement of the line showing another symbol
--                       (end_reason 'other_symbol'), or a 15-12G/15-15D, or a
--                       15-12B/25/25-NSE when the run's last statement before it
--                       listed a single symbol (end_reason = the form), unless an
--                       amendment superseded it; NULL = open;
-- * last_confirmed_on : the run's latest statement showing the ticker;
-- * prior_holder_end  : the latest end evidence (valid_to, else last
--                       confirmation) of any run of ANOTHER CIK holding the key
--                       that started before this run; NULL if none;
-- * next_holder_start : the earliest start of a run of another CIK holding the
--                       key that starts on or after this run's start; NULL if none.
-- Lines of the same CIK showing the same symbol are one security relabelled, so
-- other holders are other CIKs. Only equity/depositary lines count when any
-- showed the key. Consumers admit rows in [valid_from, valid_to), rows in
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
WITH shown AS (
    SELECT o.cik, o.line_key,
           bool_or(o.security_kind IN ('equity', 'depositary')) AS is_equity,
           bool_or(o.class_key = p_class_key) AS has_class
    FROM sec_ticker_cik_observations o
    WHERE o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
    GROUP BY o.cik, o.line_key
), holder_lines AS (
    -- As in sec_ticker_issuer_at: equity/depositary lines only, unless no
    -- equity line ever showed the ticker.
    SELECT s.cik, s.line_key, s.has_class FROM shown s
    WHERE s.is_equity OR NOT EXISTS (SELECT 1 FROM shown e WHERE e.is_equity)
), statements AS (
    SELECT s.cik, s.line_key, s.available_on, s.adsh,
           bool_or(s.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g'))
               AS shows,
           (array_agg(s.class_key ORDER BY s.class_key) FILTER (
               WHERE s.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
           ))[1] AS class_key,
           (SELECT count(DISTINCT f.ticker_key) FROM sec_ticker_cik_observations f
            WHERE f.adsh = s.adsh AND f.cik = s.cik) AS filing_symbols
    FROM sec_ticker_cik_observations s
    JOIN holder_lines h ON h.cik = s.cik AND h.line_key = s.line_key
    GROUP BY s.cik, s.line_key, s.available_on, s.adsh
), applied_events AS (
    -- A deregistration ends a hold when the line's latest statement before it
    -- (strictly earlier: a same-day statement prevails) shows the ticker.
    SELECT h.cik, h.line_key, e.available_on, e.form
    FROM holder_lines h
    CROSS JOIN LATERAL sec_registration_end_events(h.cik, NULL) e
    CROSS JOIN LATERAL (
        SELECT st.available_on, st.shows, st.filing_symbols
        FROM statements st
        WHERE st.cik = h.cik AND st.line_key = h.line_key
          AND st.available_on <= e.available_on
        ORDER BY st.available_on DESC, st.adsh DESC
        LIMIT 1
    ) prev
    WHERE prev.available_on < e.available_on AND prev.shows
      AND (e.form IN ('15-12G', '15-15D') OR prev.filing_symbols = 1)
), timeline AS (
    SELECT st.cik, st.line_key, st.available_on, 0 AS ord, st.adsh, st.shows AS holding,
           CASE WHEN NOT st.shows THEN 'other_symbol' END AS reason, st.class_key
    FROM statements st
    UNION ALL
    SELECT ae.cik, ae.line_key, ae.available_on, 1, '', false, ae.form, NULL
    FROM applied_events ae
), marked AS (
    SELECT t.*,
           sum(CASE WHEN t.holding AND NOT COALESCE(t.prev_holding, false) THEN 1 ELSE 0 END)
               OVER (PARTITION BY t.cik, t.line_key
                     ORDER BY t.available_on, t.ord, t.adsh) AS run_no
    FROM (
        SELECT tl.*, lag(tl.holding) OVER (
            PARTITION BY tl.cik, tl.line_key ORDER BY tl.available_on, tl.ord, tl.adsh
        ) AS prev_holding
        FROM timeline tl
    ) t
), runs AS (
    SELECT m.cik, m.line_key, m.run_no,
           min(m.available_on) FILTER (WHERE m.holding) AS valid_from,
           max(m.available_on) FILTER (WHERE m.holding) AS last_confirmed_on,
           (array_agg(m.class_key ORDER BY m.available_on DESC, m.adsh DESC)
               FILTER (WHERE m.holding))[1] AS class_key
    FROM marked m
    WHERE m.run_no > 0
    GROUP BY m.cik, m.line_key, m.run_no
), ends AS (
    SELECT DISTINCT ON (m.cik, m.line_key, m.run_no)
           m.cik, m.line_key, m.run_no, m.available_on AS valid_to, m.reason
    FROM marked m
    WHERE m.run_no > 0 AND NOT m.holding
    ORDER BY m.cik, m.line_key, m.run_no, m.available_on, m.ord, m.adsh
), spans AS (
    SELECT r.cik, r.line_key, r.class_key, r.valid_from, e.valid_to,
           e.reason AS end_reason, r.last_confirmed_on
    FROM runs r
    LEFT JOIN ends e ON e.cik = r.cik AND e.line_key = r.line_key AND e.run_no = r.run_no
)
SELECT a.class_key, a.valid_from, a.valid_to, a.end_reason, a.last_confirmed_on,
       (SELECT max(COALESCE(o.valid_to, o.last_confirmed_on)) FROM spans o
        WHERE o.cik <> a.cik AND o.valid_from < a.valid_from) AS prior_holder_end,
       (SELECT min(o.valid_from) FROM spans o
        WHERE o.cik <> a.cik AND o.valid_from >= a.valid_from) AS next_holder_start,
       a.line_key
FROM spans a
WHERE a.cik = p_cik
  AND (p_class_key IS NULL OR EXISTS (
      SELECT 1 FROM holder_lines h
      WHERE h.cik = a.cik AND h.line_key = a.line_key AND h.has_class))
ORDER BY a.valid_from, a.line_key
$fn$;

-- Diagnostics: every interval as known today (valid_to = knowledge date of the
-- first later statement of the line showing another symbol; deregistrations
-- are applied by the functions above, not here). Not used for decisions.
CREATE OR REPLACE VIEW sec_ticker_intervals AS
WITH statements AS (
    SELECT o.cik, o.line_key, o.available_on, o.adsh,
           array_agg(DISTINCT o.ticker ORDER BY o.ticker) AS tickers
    FROM sec_ticker_cik_observations o
    GROUP BY o.cik, o.line_key, o.available_on, o.adsh
), changes AS (
    SELECT s.*,
           lag(s.tickers) OVER w IS DISTINCT FROM s.tickers AS starts
    FROM statements s
    WINDOW w AS (PARTITION BY s.cik, s.line_key ORDER BY s.available_on, s.adsh)
), numbered AS (
    SELECT c.*, sum(c.starts::integer) OVER (
        PARTITION BY c.cik, c.line_key ORDER BY c.available_on, c.adsh
    ) AS run
    FROM changes c
), runs AS (
    SELECT n.cik, n.line_key, n.run, min(n.tickers) AS tickers,
           min(n.available_on) AS valid_from, max(n.available_on) AS last_confirmed_on,
           count(*) AS statements
    FROM numbered n
    GROUP BY n.cik, n.line_key, n.run
), bounded AS (
    -- The next run's start, taken before the symbols are expanded: one run with
    -- several symbols must not end at its own start.
    SELECT r.*, lead(r.valid_from) OVER (
        PARTITION BY r.cik, r.line_key ORDER BY r.run
    ) AS valid_to
    FROM runs r
)
SELECT b.cik, b.line_key, t.ticker, b.valid_from, b.last_confirmed_on, b.valid_to,
       b.statements
FROM bounded b
CROSS JOIN LATERAL unnest(b.tickers) AS t(ticker);

COMMENT ON TABLE sec_ticker_cik_observations IS
    'SEC XBRL cover-page dei:TradingSymbol statements: class (cik, class_key) traded as '
    'ticker as of filing adsh. Loaded from DERA Financial Statement and Notes data sets.';
COMMENT ON FUNCTION sec_ticker_issuer_at(text, date, integer) IS
    'issuer_at(ticker, D): resolved|ambiguous|stale|ended|missing; see '
    'schemas/sec_ticker_cik_history_v1.sql.';

-- Ownership and grants. The loader writes as the owner; readers get SELECT and
-- EXECUTE only (default privileges would otherwise hand app_runtime writes).
REVOKE ALL ON TABLE sec_ticker_cik_observations, sec_cover_share_counts,
    sec_registration_events, sec_ticker_cik_packages, sec_ticker_cik_package_members,
    sec_ticker_intervals FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_registration_end_events(bigint, date),
    sec_ticker_lines_at(text, date, integer),
    sec_ticker_issuer_at(text, date, integer),
    sec_issuer_line_at(bigint, text, date, integer),
    sec_cover_class_shares_at(bigint, text, date, integer),
    sec_cover_ticker_shares_at(text, bigint, date, integer),
    sec_ticker_price_span(text, bigint, text) FROM PUBLIC;
DO $$
DECLARE
    reader text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'worker_writer') THEN
        ALTER TABLE sec_ticker_cik_observations OWNER TO worker_writer;
        ALTER TABLE sec_cover_share_counts OWNER TO worker_writer;
        ALTER TABLE sec_registration_events OWNER TO worker_writer;
        ALTER TABLE sec_ticker_cik_packages OWNER TO worker_writer;
        ALTER TABLE sec_ticker_cik_package_members OWNER TO worker_writer;
        ALTER VIEW sec_ticker_intervals OWNER TO worker_writer;
        ALTER FUNCTION sec_registration_end_events(bigint, date) OWNER TO worker_writer;
        ALTER FUNCTION sec_ticker_lines_at(text, date, integer) OWNER TO worker_writer;
        ALTER FUNCTION sec_ticker_issuer_at(text, date, integer) OWNER TO worker_writer;
        ALTER FUNCTION sec_issuer_line_at(bigint, text, date, integer) OWNER TO worker_writer;
        ALTER FUNCTION sec_cover_class_shares_at(bigint, text, date, integer)
            OWNER TO worker_writer;
        ALTER FUNCTION sec_cover_ticker_shares_at(text, bigint, date, integer)
            OWNER TO worker_writer;
        ALTER FUNCTION sec_ticker_price_span(text, bigint, text) OWNER TO worker_writer;
    END IF;
    FOREACH reader IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = reader) THEN
            EXECUTE format(
                'REVOKE ALL ON TABLE sec_ticker_cik_observations, sec_cover_share_counts, '
                'sec_registration_events, sec_ticker_cik_packages, '
                'sec_ticker_cik_package_members, sec_ticker_intervals FROM %I', reader);
            EXECUTE format(
                'GRANT SELECT ON TABLE sec_ticker_cik_observations, sec_cover_share_counts, '
                'sec_registration_events, sec_ticker_cik_packages, '
                'sec_ticker_cik_package_members, sec_ticker_intervals TO %I', reader);
            EXECUTE format(
                'GRANT EXECUTE ON FUNCTION sec_registration_end_events(bigint, date), '
                'sec_ticker_lines_at(text, date, integer), '
                'sec_ticker_issuer_at(text, date, integer), '
                'sec_issuer_line_at(bigint, text, date, integer), '
                'sec_cover_class_shares_at(bigint, text, date, integer), '
                'sec_cover_ticker_shares_at(text, bigint, date, integer), '
                'sec_ticker_price_span(text, bigint, text) TO %I', reader);
        END IF;
    END LOOP;
END $$;

COMMIT;
