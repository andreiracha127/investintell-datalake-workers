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
--   Forms 15-12B, 15-12G, 15-15D, 25, 25-NSE (ends), 8-A12B, 8-A12G, 10-12B,
--   10-12G (registrations) and their /A amendments; for the ends of CIKs with
--   cover data, the filing itself (https://www.sec.gov/Archives/edgar/data/),
--   read for the class, rule provision and exchange it states.
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
    -- From the title, else the symbol/segments: what the class is. 'unknown': a
    -- line on a foreign private issuer's form (20-F, 40-F, 6-K, 20-FR) that no
    -- title, segment or symbol suffix identifies (TSM's 2018 20-F tags TSM, its
    -- ADS, untitled and undimensioned): a listed line, never taken for equity.
    security_kind text NOT NULL CHECK (security_kind IN (
        'equity', 'depositary', 'preferred', 'debt', 'warrant', 'unit', 'right', 'unknown'
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

-- Registration filings from the EDGAR form indexes (dates only):
-- * ends: Forms 15-12B, 15-12G, 15-15D (termination of registration or of the
--   duty to report), 25 and 25-NSE (removal from listing), and their /A;
-- * starts: Forms 8-A12B, 8-A12G, 10-12B, 10-12G (registration of a class), and
--   their /A. A start near a delisting is a transfer; a start after an end is a
--   relisting.
-- For the ends of CIKs with cover data, the loader reads the filing itself
-- (parser_version names the parser) and stores what it says:
-- * class_description: the class the form concerns (Form 25-NSE XML
--   descriptionClassSecurity; the block above "(Description of class of
--   securities)" on Form 25 and "(Title of each class of securities covered by
--   this Form)" on Form 15);
-- * class_kind: 'equity' (common/ordinary shares, depositary shares of them,
--   partnership units), 'other' (notes, preferred, warrants, rights plans,
--   units, employee-plan interests) or 'unknown' (the filing states no class,
--   or could not be parsed); class_count: the share classes it names (Class A
--   and Class B -> 2; at least 1);
-- * provision / extinguished: the Rule 12d2-2 provision of a 25-NSE; (a)(1)-(4)
--   means the class was redeemed, retired, substituted in a merger or its rights
--   extinguished;
-- * venue / venue_kind: the exchange of a Form 25/25-NSE; 'secondary' for
--   exchanges where issuers keep second listings (Chicago, Boston, Philadelphia,
--   National, NYSE Arca/Pacific);
-- * amendment_effect of an /A: 'cancels' when it says the delisting will not
--   happen or is withdrawn (Minim's 25-NSE/A of 2025-04-09), else 'restates'.
-- class_kind and amendment_effect are NULL when the filing was not read. A
-- parser change re-derives these columns as a correction (new fact version).
CREATE TABLE IF NOT EXISTS sec_registration_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fact_hash text NOT NULL CHECK (fact_hash ~ '^[0-9a-f]{32}$'),
    adsh text NOT NULL CHECK (adsh ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    cik bigint NOT NULL CHECK (cik > 0),
    form text NOT NULL CHECK (form IN (
        '15-12B', '15-12G', '15-15D', '25', '25-NSE',
        '15-12B/A', '15-12G/A', '15-15D/A', '25/A', '25-NSE/A',
        '8-A12B', '8-A12G', '10-12B', '10-12G',
        '8-A12B/A', '8-A12G/A', '10-12B/A', '10-12G/A'
    )),
    filed date NOT NULL,
    class_description text,
    class_kind text CHECK (class_kind IN ('equity', 'other', 'unknown')),
    class_count integer CHECK (class_count >= 1),
    provision text,
    extinguished boolean,
    venue text,
    venue_kind text CHECK (venue_kind IN ('primary', 'secondary', 'unknown')),
    amendment_effect text CHECK (amendment_effect IN ('cancels', 'restates')),
    parser_version text,
    source_available_on date GENERATED ALWAYS AS (filed + 1) STORED,
    available_on date NOT NULL,
    retired_on date,
    loaded_on date NOT NULL,
    source_package text NOT NULL,
    CHECK (available_on >= filed + 1),
    CHECK ((class_kind IS NULL) = (parser_version IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS sec_registration_events_current_idx
    ON sec_registration_events (fact_hash) WHERE retired_on IS NULL;
CREATE INDEX IF NOT EXISTS sec_registration_events_cik_idx
    ON sec_registration_events (cik, available_on DESC);

-- One row per loaded source package: provenance and the loader's counts. DERA
-- consolidates monthly packages into a quarterly one after about a year
-- (YYYYqN covers the filings of months 3N-2..3N, as the YYYY_MM packages did);
-- once the quarterly is loaded, its months' packages are superseded_by it from
-- superseded_on, and their facts retire unless a current package carries them.
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
    loaded_at timestamptz NOT NULL DEFAULT now(),
    -- The SEC's validators for the loaded version (HEAD ETag / Last-Modified),
    -- when it sends them; the worker compares them, else the SHA-256 above.
    remote_etag text,
    remote_last_modified text,
    superseded_by text,
    superseded_on date,
    CHECK ((superseded_by IS NULL) = (superseded_on IS NULL))
);

-- The (fact family, accession, CIK) triples each package or index contains,
-- over time (never deleted): an accession any package ever contained for a fact
-- family makes a newly carried fact of that family a correction, knowable from
-- the reconciliation date, including a fact that was retired and is carried
-- again. A DERA package records its submissions for observations and share
-- counts; an index records its rows for events.
CREATE TABLE IF NOT EXISTS sec_ticker_cik_package_members (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_package text NOT NULL,
    fact_table text NOT NULL CHECK (fact_table IN ('observation', 'share_count', 'event')),
    adsh text NOT NULL,
    cik bigint NOT NULL,
    loaded_on date NOT NULL,
    retired_on date
);

CREATE UNIQUE INDEX IF NOT EXISTS sec_ticker_cik_package_members_current_idx
    ON sec_ticker_cik_package_members (source_package, fact_table, adsh, cik)
    WHERE retired_on IS NULL;
CREATE INDEX IF NOT EXISTS sec_ticker_cik_package_members_adsh_idx
    ON sec_ticker_cik_package_members (adsh, fact_table);

-- Which fact versions each package carries, over time (never deleted): a fact
-- is retired only when no current package carries it any more.
CREATE TABLE IF NOT EXISTS sec_ticker_cik_package_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_package text NOT NULL,
    fact_table text NOT NULL CHECK (fact_table IN ('observation', 'share_count', 'event')),
    fact_hash text NOT NULL,
    loaded_on date NOT NULL,
    retired_on date
);

CREATE UNIQUE INDEX IF NOT EXISTS sec_ticker_cik_package_facts_current_idx
    ON sec_ticker_cik_package_facts (source_package, fact_table, fact_hash)
    WHERE retired_on IS NULL;
CREATE INDEX IF NOT EXISTS sec_ticker_cik_package_facts_hash_idx
    ON sec_ticker_cik_package_facts (fact_table, fact_hash) WHERE retired_on IS NULL;

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

-- The end filings (15-12B/15-12G/15-15D/25/25-NSE) of a CIK visible at D, as
-- amended by D. An amendment applies to exactly one original: the latest
-- original of its form for the CIK filed on or before it (then by accession),
-- from the amendment's own knowledge date. A cancelling amendment removes the
-- original; a restating one replaces its class attributes (the original's are
-- returned as original_*, and restated_on is the amendment's knowledge date); an
-- amendment that was not read leaves the original in force. p_current (lineage:
-- today's truth) applies every current amendment, whenever it was filed.
CREATE OR REPLACE FUNCTION sec_registration_end_events(
    p_cik bigint, p_as_of date, p_current boolean DEFAULT false
)
RETURNS TABLE (
    available_on date,
    filed date,
    form text,
    adsh text,
    class_kind text,
    class_count integer,
    extinguished boolean,
    venue_kind text,
    restated_on date,
    original_class_kind text,
    original_class_count integer,
    original_extinguished boolean,
    original_venue_kind text
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH visible AS (
    SELECT e.adsh, e.form, e.filed, e.class_kind, e.class_count, e.extinguished,
           e.venue_kind, e.amendment_effect,
           CASE WHEN p_current THEN e.source_available_on ELSE e.available_on END AS known_on
    FROM sec_registration_events e
    WHERE e.cik = p_cik
      AND e.form IN ('15-12B', '15-12G', '15-15D', '25', '25-NSE',
                     '15-12B/A', '15-12G/A', '15-15D/A', '25/A', '25-NSE/A')
      AND CASE WHEN p_current
               THEN e.retired_on IS NULL
                    AND (e.source_available_on <= p_as_of OR e.form LIKE '%/A')
               ELSE e.available_on <= p_as_of
                    AND (e.retired_on IS NULL OR e.retired_on > p_as_of)
          END
), amended AS (
    SELECT a.*, (
        SELECT o.adsh FROM visible o
        WHERE o.form = left(a.form, -2) AND o.filed <= a.filed
        ORDER BY o.filed DESC, o.adsh DESC
        LIMIT 1
    ) AS original
    FROM visible a
    WHERE a.form LIKE '%/A'
), latest_amendment AS (
    SELECT DISTINCT ON (m.original) m.*
    FROM amended m
    WHERE m.original IS NOT NULL AND m.amendment_effect IS NOT NULL
    ORDER BY m.original, m.filed DESC, m.adsh DESC
)
SELECT v.known_on, v.filed, v.form, v.adsh,
       CASE WHEN l.amendment_effect = 'restates' THEN l.class_kind ELSE v.class_kind END,
       CASE WHEN l.amendment_effect = 'restates' THEN l.class_count ELSE v.class_count END,
       CASE WHEN l.amendment_effect = 'restates' THEN l.extinguished ELSE v.extinguished END,
       CASE WHEN l.amendment_effect = 'restates' THEN l.venue_kind ELSE v.venue_kind END,
       CASE WHEN l.amendment_effect = 'restates' THEN l.known_on END,
       v.class_kind, v.class_count, v.extinguished, v.venue_kind
FROM visible v
LEFT JOIN latest_amendment l ON l.original = v.adsh
WHERE v.form NOT LIKE '%/A'
  AND l.amendment_effect IS DISTINCT FROM 'cancels'
$fn$;

-- The registration filings (8-A12B/8-A12G/10-12B/10-12G originals) of a CIK
-- visible at D.
CREATE OR REPLACE FUNCTION sec_registration_starts(
    p_cik bigint, p_as_of date, p_current boolean DEFAULT false
)
RETURNS TABLE (available_on date, filed date, form text, adsh text)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT CASE WHEN p_current THEN e.source_available_on ELSE e.available_on END,
       e.filed, e.form, e.adsh
FROM sec_registration_events e
WHERE e.cik = p_cik
  AND e.form IN ('8-A12B', '8-A12G', '10-12B', '10-12G')
  AND CASE WHEN p_current
           THEN e.retired_on IS NULL AND e.source_available_on <= p_as_of
           ELSE e.available_on <= p_as_of AND (e.retired_on IS NULL OR e.retired_on > p_as_of)
      END
$fn$;

-- The end filings of a CIK that end its equity lines at D, and whether each is
-- DEFINITIVE. Judged against the CIK's latest filing with an equity line before
-- the event (prior symbols: its equity symbols when the end names an equity
-- class, else all its symbols) and its latest complete filing (prior classes).
-- An end APPLIES unless
-- * the filing concerns another class (class_kind 'other'), or
-- * it is a 25/25-NSE/15-12B (12(b) removal) that may concern a class other than
--   the listed symbol: the issuer then showed several symbols and the filing
--   names fewer classes than it showed symbols, or the exchange is a
--   secondary one, or a registration filed from 30 days before to 10 days after
--   it makes it a transfer (PepsiCo's NYSE -> Nasdaq move: Form 25 and 8-A12B the
--   same day), unless the class was extinguished.
-- An applying end of an equity class (class_kind 'equity', naming at least as
-- many classes as the issuer has) is DEFINITIVE when the 25-NSE says the class
-- was extinguished (12d2-2(a)), or when an applying delisting (25/25-NSE, not
-- secondary, not a transfer) naming every listed class (as many classes as the
-- issuer showed equity symbols) and an applying termination (15-12B/15-12G/
-- 15-15D) naming every equity class are both on file within 120 days of each
-- other, unless the shareholder base continued:
-- the first cover share count after the end is within 0.8-1.25 times the last
-- one before it (a holding-company reorganization or REIT conversion that keeps
-- the CIK: United Fire 2012, Ulta and SBA 2017; American Greetings reported 100
-- shares after its merger). Other applying ends (including class_kind 'unknown'
-- or unread) end the lines until a later statement shows the symbol again (an
-- exchange delisting to OTC, a stale 12(g) registration).
-- A restated end that applies only as restated is public from the amendment's
-- knowledge date (available_on); one that no longer applies as restated is
-- withdrawn from it. p_current (lineage): ends filed by D, judged with every
-- current filing (a transfer registration, a paired Form 15 or an amendment filed
-- after D still counts), each at its original's date.
CREATE OR REPLACE FUNCTION sec_issuer_end_events(
    p_cik bigint, p_as_of date, p_current boolean DEFAULT false
)
RETURNS TABLE (available_on date, filed date, form text, adsh text, definitive boolean)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH horizon AS (
    SELECT CASE WHEN p_current THEN 'infinity'::date ELSE p_as_of END AS on_date
), filings AS (
    SELECT o.adsh,
           max(CASE WHEN p_current THEN o.source_available_on ELSE o.available_on END)
               AS known_on,
           max(o.accepted) AS accepted,
           count(DISTINCT o.ticker_key) AS symbols,
           count(DISTINCT o.ticker_key) FILTER (
               WHERE o.security_kind IN ('equity', 'depositary', 'unknown')) AS equity_symbols,
           max(o.filing_equity_classes) AS classes,
           bool_or(o.filing_complete) AS complete
    FROM horizon h
    CROSS JOIN LATERAL sec_observations_at(h.on_date, p_current) o
    WHERE o.cik = p_cik
    GROUP BY o.adsh
    HAVING bool_or(o.security_kind IN ('equity', 'depositary', 'unknown'))
), totals AS (
    -- each filing's cover share count: its issuer total, else its class counts
    SELECT c.adsh,
           max(CASE WHEN p_current THEN c.source_available_on ELSE c.available_on END)
               AS known_on,
           max(c.accepted) AS accepted,
           COALESCE(max(c.shares) FILTER (WHERE c.class_key = ''),
                    sum(c.shares) FILTER (WHERE c.class_key <> '')) AS total
    FROM horizon h
    CROSS JOIN LATERAL sec_share_counts_at(h.on_date, p_current) c
    WHERE c.cik = p_cik
    GROUP BY c.adsh
), events AS (
    SELECT e.*,
           EXISTS (
               SELECT 1 FROM horizon h
               CROSS JOIN LATERAL sec_registration_starts(p_cik, h.on_date, p_current) r
               WHERE r.filed BETWEEN e.filed - 30 AND e.filed + 10
           ) AS registered_nearby
    FROM horizon h
    CROSS JOIN LATERAL sec_registration_end_events(p_cik, h.on_date, p_current) e
), versions AS (
    -- each end as it reads now ('effective') and, when restated, as filed
    SELECT e.adsh, e.form, e.filed, e.available_on, e.restated_on, e.registered_nearby,
           true AS effective, e.class_kind, e.class_count, e.extinguished, e.venue_kind
    FROM events e
    UNION ALL
    SELECT e.adsh, e.form, e.filed, e.available_on, e.restated_on, e.registered_nearby,
           false, e.original_class_kind, e.original_class_count, e.original_extinguished,
           e.original_venue_kind
    FROM events e
    WHERE e.restated_on IS NOT NULL
), judged AS (
    SELECT v.*,
           (SELECT CASE WHEN v.class_kind = 'equity' THEN f.equity_symbols ELSE f.symbols END
            FROM filings f WHERE f.known_on < v.available_on
            ORDER BY f.known_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
            LIMIT 1) AS prior_symbols,
           GREATEST(COALESCE((
               SELECT f.classes FROM filings f WHERE f.complete AND f.known_on < v.available_on
               ORDER BY f.known_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
               LIMIT 1), 1), 1) AS prior_classes,
           v.registered_nearby AND NOT COALESCE(v.extinguished, false) AS transfer
    FROM versions v
), applies AS (
    SELECT j.*,
           COALESCE(j.class_kind, 'unknown') <> 'other'
           AND (j.form IN ('15-12G', '15-15D')
                OR (NOT j.transfer AND j.venue_kind IS DISTINCT FROM 'secondary'
                    AND (j.prior_symbols = 1
                         OR (j.class_kind = 'equity' AND j.class_count >= j.prior_symbols))))
               AS applying,
           j.class_kind = 'equity' AND j.class_count >= j.prior_classes AS whole_equity,
           j.form IN ('25', '25-NSE')
               AND j.class_kind = 'equity'
               AND j.venue_kind IS DISTINCT FROM 'secondary'
               AND NOT j.transfer AS equity_delisting
    FROM judged j
), current_ends AS (
    SELECT a.*,
           CASE WHEN a.restated_on IS NOT NULL AND NOT p_current
                     AND NOT COALESCE((SELECT o.applying FROM applies o
                                       WHERE o.adsh = a.adsh AND NOT o.effective), false)
                THEN a.restated_on
                ELSE a.available_on
           END AS effective_on
    FROM applies a
    WHERE a.effective AND a.applying
)
SELECT a.effective_on, a.filed, a.form, a.adsh,
       a.whole_equity AND NOT COALESCE((
           SELECT after_end.total BETWEEN 0.8 * before_end.total AND 1.25 * before_end.total
           FROM (SELECT t.total FROM totals t WHERE t.known_on < a.available_on
                 ORDER BY t.known_on DESC, t.accepted DESC NULLS LAST, t.adsh DESC
                 LIMIT 1) before_end,
                (SELECT t.total FROM totals t WHERE t.known_on >= a.available_on
                 ORDER BY t.known_on, t.accepted NULLS FIRST, t.adsh
                 LIMIT 1) after_end), false)
       AND (
           (a.form = '25-NSE' AND COALESCE(a.extinguished, false))
           OR (a.form IN ('15-12B', '15-12G', '15-15D') AND EXISTS (
               SELECT 1 FROM applies d
               WHERE d.effective AND d.applying AND d.equity_delisting
                 AND d.class_count >= d.prior_symbols
                 AND d.filed BETWEEN a.filed - 120 AND a.filed + 120))
           OR (a.equity_delisting AND a.class_count >= a.prior_symbols AND EXISTS (
               SELECT 1 FROM applies t
               WHERE t.effective AND t.applying AND t.form IN ('15-12B', '15-12G', '15-15D')
                 AND t.class_kind = 'equity' AND t.class_count >= t.prior_classes
                 AND t.filed BETWEEN a.filed - 120 AND a.filed + 120))
       ) AS definitive
FROM current_ends a
WHERE a.available_on <= p_as_of
$fn$;

-- Each CIK's hold of a ticker at D (internal helper; one row per CIK that showed
-- the ticker by D). Listed rows (equity, depositary or unknown) decide whenever
-- one showed the ticker (filers also tag their common symbol on notes lines). A
-- hold is
-- followed through its candidate statements:
--   (a) every filing that tags a class that showed the ticker,
--   (b) when the latest complete filing showing the ticker listed one equity
--       class: every complete filing (the issuer's sole security, however its
--       member is called, until a complete filing says otherwise), and
--   (c) every complete filing that shows one equity class in total (classes
--       that merged into one: the old symbols are gone unless it shows them).
-- Ends come from sec_issuer_end_events. A candidate filed after an end does not
-- count when
--   * the end is DEFINITIVE and the ticker was shown before it (American
--     Greetings tagged AM on 10-Qs for three years after its 2013 merger
--     delisting and Form 15), or
--   * another CIK first showed the ticker from 30 days before the end to this
--     CIK's first statement after it (the symbol moved: Google Inc's 10-Q of
--     2015-10-29 still tagged GOOG after its Form 15s, the day Alphabet's first
--     10-Q did),
-- unless its row for the ticker carries a 12(b) title or a registration
-- (8-A12B/8-A12G/10-12B/10-12G) filed after the end was public by then. A
-- symbol the CIK first showed after a definitive end is a new line (Swift's SWFT
-- ended in the merger; the same CIK traded as KNX).
-- The statement is the latest counting candidate. The hold has ended when the
-- latest applying end is newer than the statement (end_reason: its form) or the
-- statement does not show the ticker ('other_symbol'); an open hold whose
-- statement is older than p_max_age_days is stale. first_on / confirmed_on: the
-- first and last knowledge dates of rows showing the ticker; class_key and
-- security_kind come from the latest of those rows. Ordering within a day is by
-- acceptance time, then accession.
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
    statement_accepted timestamp,
    statement_adsh text,
    first_on date,
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
    WHERE s.security_kind IN ('equity', 'depositary', 'unknown')
       OR NOT EXISTS (SELECT 1 FROM shown e
                      WHERE e.security_kind IN ('equity', 'depositary', 'unknown'))
), per_cik AS (
    SELECT r.cik,
           min(r.known_on) AS first_on,
           max(r.known_on) AS confirmed_on,
           (array_agg(r.class_key ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh DESC, r.class_key))[1] AS class_key,
           (array_agg(r.security_kind ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh DESC, r.class_key))[1] AS security_kind,
           array_agg(DISTINCT r.class_key) AS classes,
           COALESCE((array_agg(r.filing_equity_classes = 1 ORDER BY r.known_on DESC,
                               r.accepted DESC NULLS LAST, r.adsh DESC)
                     FILTER (WHERE r.filing_complete))[1], false) AS sole
    FROM relevant r
    GROUP BY r.cik
), candidates AS (
    SELECT p.cik, f.adsh,
           max(CASE WHEN p_current THEN f.source_available_on ELSE f.available_on END)
               AS known_on,
           max(f.accepted) AS accepted,
           bool_or(f.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g'))
               AS shows,
           bool_or(f.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
                   AND f.security_title IS NOT NULL) AS titled
    FROM per_cik p
    JOIN sec_observations_at(p_as_of, p_current) f
      ON f.cik = p.cik
     AND (f.class_key = ANY(p.classes)
          OR (f.filing_complete AND f.security_kind IN ('equity', 'depositary', 'unknown')
              AND (p.sole OR f.filing_equity_classes = 1)))
    GROUP BY p.cik, f.adsh
), ends AS (
    SELECT p.cik, e.available_on, e.filed, e.form, e.adsh, e.definitive,
           (SELECT min(c.known_on) FROM candidates c
            WHERE c.cik = p.cik AND c.shows AND c.known_on >= e.available_on) AS first_post_on
    FROM per_cik p
    CROSS JOIN LATERAL sec_issuer_end_events(p.cik, p_as_of, p_current) e
), last_end AS (
    SELECT DISTINCT ON (e.cik) e.*
    FROM ends e
    ORDER BY e.cik, e.available_on DESC, e.adsh DESC
), statement AS (
    SELECT DISTINCT ON (c.cik) c.*
    FROM candidates c
    JOIN per_cik p ON p.cik = c.cik
    WHERE c.titled OR NOT EXISTS (
        SELECT 1 FROM ends e
        WHERE e.cik = c.cik AND e.available_on <= c.known_on
          AND ((e.definitive AND p.first_on < e.available_on)
               OR EXISTS (
                   SELECT 1 FROM per_cik o
                   WHERE o.cik <> c.cik
                     AND o.first_on BETWEEN e.available_on - 30 AND e.first_post_on))
          AND NOT EXISTS (
              SELECT 1 FROM sec_registration_starts(c.cik, p_as_of, p_current) r
              WHERE r.filed > e.filed AND r.available_on <= c.known_on))
    ORDER BY c.cik, c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh DESC
)
SELECT p.cik,
       CASE
           WHEN l.available_on > s.known_on THEN 'ended'
           WHEN NOT s.shows THEN 'ended'
           WHEN p_as_of - s.known_on > p_max_age_days THEN 'stale'
           ELSE 'active'
       END AS state,
       p.class_key, p.security_kind, s.known_on AS statement_on,
       s.accepted AS statement_accepted, s.adsh AS statement_adsh,
       p.first_on, p.confirmed_on,
       CASE
           WHEN l.available_on > s.known_on THEN l.form
           WHEN NOT s.shows THEN 'other_symbol'
       END AS end_reason
FROM per_cik p
JOIN statement s ON s.cik = p.cik
LEFT JOIN last_end l ON l.cik = p.cik
$fn$;

-- issuer_at(ticker, D): the (CIK, class) the ticker belonged to at D, from what
-- was known at D (see sec_ticker_holds).
-- * 'resolved'  : exactly one CIK holds the ticker actively; class_key is the
--                 class of its latest row showing the ticker. An active hold whose
--                 claims lie strictly inside another active holder's (that holder
--                 showed the ticker before the first and after the last of them:
--                 a misfiled report, or another issuer typing the symbol) does not
--                 count against it.
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
WITH holds AS (
    SELECT h.*,
           CASE h.state WHEN 'active' THEN 0 WHEN 'stale' THEN 1 ELSE 2 END AS rank_state
    FROM sec_ticker_holds(p_ticker, p_as_of, p_max_age_days, false) h
), ranked AS (
    SELECT h.* FROM holds h
    WHERE NOT (h.rank_state = 0 AND EXISTS (
        SELECT 1 FROM holds y
        WHERE y.rank_state = 0 AND y.cik <> h.cik
          AND y.first_on < h.first_on AND y.confirmed_on > h.confirmed_on))
), decided AS (
    SELECT r.* FROM ranked r
    ORDER BY r.rank_state, r.statement_on DESC, r.statement_accepted DESC NULLS LAST,
             r.statement_adsh DESC, r.cik
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
-- class. Ends as in sec_ticker_holds: a filing after a definitive end counts
-- only with a 12(b) title, a registration after the end, or symbols the CIK
-- first showed after the end. class_key and security_kind come from one row of
-- the statement (the caller's class first). status: 'resolved' | 'stale' | 'ended' |
-- 'missing' | 'ambiguous_class' (the class is not known at D and the issuer then
-- listed several equity classes). equity_lines: equity classes in the issuer's
-- latest complete filing by D.
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
               ELSE COALESCE((
                   SELECT o.filing_equity_classes = 1 FROM own o
                   WHERE o.filing_complete
                   ORDER BY o.available_on DESC, o.accepted DESC NULLS LAST, o.adsh DESC
                   LIMIT 1), false)
           END AS sole
), latest_complete AS (
    SELECT f.adsh, f.filing_equity_classes
    FROM sec_observations_at(p_as_of, false) f
    WHERE f.cik = p_cik AND f.filing_complete
    ORDER BY f.available_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
    LIMIT 1
), rows AS (
    SELECT f.adsh, f.available_on, f.accepted, f.ticker, f.ticker_key, f.class_key,
           f.security_kind, f.security_title
    FROM sec_observations_at(p_as_of, false) f
    WHERE f.cik = p_cik
      AND (f.class_key = p_class_key
           OR ((SELECT s.sole FROM follows_sole s) AND f.filing_complete
               AND f.filing_equity_classes = 1
               AND f.security_kind IN ('equity', 'depositary', 'unknown')))
), ends AS (
    SELECT e.* FROM sec_issuer_end_events(p_cik, p_as_of, false) e
), last_end AS (
    SELECT e.* FROM ends e ORDER BY e.available_on DESC, e.adsh DESC LIMIT 1
), candidates AS (
    SELECT r.adsh, max(r.available_on) AS known_on, max(r.accepted) AS accepted,
           bool_or(r.security_title IS NOT NULL) AS titled,
           array_agg(DISTINCT r.ticker_key) AS keys
    FROM rows r
    GROUP BY r.adsh
), chosen AS (
    SELECT c.* FROM candidates c
    WHERE c.titled OR NOT EXISTS (
        SELECT 1 FROM ends d
        WHERE d.definitive AND d.available_on <= c.known_on
          AND EXISTS (
              SELECT 1 FROM sec_observations_at(p_as_of, false) x
              WHERE x.cik = p_cik AND x.ticker_key = ANY(c.keys)
                AND x.available_on < d.available_on)
          AND NOT EXISTS (
              SELECT 1 FROM sec_registration_starts(p_cik, p_as_of, false) r
              WHERE r.filed > d.filed AND r.available_on <= c.known_on))
    ORDER BY c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh DESC
    LIMIT 1
), statement AS (
    SELECT c.known_on, c.adsh,
           ARRAY(SELECT DISTINCT r.ticker FROM rows r WHERE r.adsh = c.adsh ORDER BY r.ticker)
               AS tickers,
           first_row.class_key, first_row.security_kind,
           EXISTS (SELECT 1 FROM last_end l WHERE l.available_on > c.known_on) AS ended
    FROM chosen c
    CROSS JOIN LATERAL (
        SELECT r.class_key, r.security_kind FROM rows r WHERE r.adsh = c.adsh
        ORDER BY r.class_key = p_class_key DESC, r.class_key, r.ticker
        LIMIT 1
    ) first_row
)
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM statement) THEN
            CASE WHEN COALESCE((SELECT l.filing_equity_classes FROM latest_complete l), 0) > 1
                 THEN 'ambiguous_class' ELSE 'missing' END
        WHEN (SELECT s.ended FROM statement s) THEN 'ended'
        WHEN (SELECT s.known_on FROM statement s) < p_as_of - p_max_age_days THEN 'stale'
        ELSE 'resolved'
    END AS status,
    (SELECT s.class_key FROM statement s) AS class_key,
    COALESCE((SELECT s.tickers FROM statement s), '{}'::text[]) AS tickers,
    (SELECT s.security_kind FROM statement s) AS security_kind,
    (SELECT s.known_on FROM statement s) AS statement_on,
    (SELECT s.adsh FROM statement s) AS adsh,
    COALESCE((SELECT l.filing_equity_classes FROM latest_complete l), 0) AS equity_lines
$fn$;

-- The class's own cover-page share count known at D: the latest stated date
-- (stated_on <= D), then the latest filing (knowledge date, acceptance time,
-- accession). A date older than p_max_age_days -> 'stale'; distinct values
-- within that one filing -> 'ambiguous'; none -> 'missing'. class_key '' is the
-- issuer total. An issuer total from a foreign private issuer's filing (20-F,
-- 40-F, 6-K, 20-FR and their /A) -> 'refused', refusal
-- 'foreign_issuer_listing_unverified' (see sec_cover_ticker_shares_at), with
-- shares_as_of and adsh kept for audit.
CREATE OR REPLACE FUNCTION sec_cover_class_shares_at(
    p_cik bigint, p_class_key text, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    status text,
    shares numeric,
    shares_as_of date,
    adsh text,
    refusal text
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH chosen AS (
    SELECT c.stated_on, c.available_on, c.adsh,
           CASE WHEN p_class_key = ''
                     AND regexp_replace(c.form, '/A$', '') IN ('20-F', '40-F', '6-K', '20-FR')
                THEN 'foreign_issuer_listing_unverified' END AS refusal
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
        WHEN (SELECT h.refusal FROM chosen h) IS NOT NULL THEN 'refused'
        WHEN (SELECT h.stated_on FROM chosen h) < p_as_of - p_max_age_days THEN 'stale'
        WHEN (SELECT count(*) FROM counts) > 1 THEN 'ambiguous'
        ELSE 'resolved'
    END AS status,
    CASE WHEN (SELECT h.refusal FROM chosen h) IS NULL AND (SELECT count(*) FROM counts) = 1
         THEN (SELECT k.shares FROM counts k) END AS shares,
    (SELECT h.stated_on FROM chosen h) AS shares_as_of,
    (SELECT h.adsh FROM chosen h) AS adsh,
    (SELECT h.refusal FROM chosen h) AS refusal
$fn$;

-- The cover share count of the class that trades as p_ticker, known at D, joined
-- inside each filing:
-- * 'class': a count on a (dimensioned) class tagged with the ticker in the same
--   filing; member names change between filings (Berkshire's 10-Q counts
--   'CommonClassB' while its 8-K covers tag BRK.B on 'ClassBCommonStock'), so the
--   filing, not the member, ties a count to a symbol;
-- * 'sole_class_total': the filing's undimensioned total, only when the filing
--   shows exactly one equity class in total (listed or not) and that class is an
--   equity (never a depositary) line showing the ticker, or an 'unknown' one.
-- A foreign private issuer's filing (20-F, 40-F, 6-K, 20-FR and their /A) counts
-- its underlying shares, and its cover may tag the ADS symbol on that class: TSM's
-- 20-Fs tag TSM "Common Shares" beside 25.9 billion common shares (five per ADS);
-- FUTU's 2022 20-F tags FUTU on its Class A ordinary shares (eight per ADS) and
-- America Movil's 2021 20-F tags AMOV on its A shares (twenty per ADS), and its
-- 2023 20-F titles AMX's B share member as its ADS. So from such a filing a
-- total, and a class count whose member is not an explicit depositary member,
-- are 'refused' (refusal 'foreign_issuer_listing_unverified':
-- shares NULL; shares_as_of, adsh and basis kept for audit) until the cover page
-- evidences what is listed.
-- Latest stated date first, then the latest filing; a class count wins over a
-- total in one filing, an admissible count over a refused one. status resolved |
-- stale | ambiguous | missing | refused.
CREATE OR REPLACE FUNCTION sec_cover_ticker_shares_at(
    p_ticker text, p_cik bigint, p_as_of date, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    status text,
    shares numeric,
    shares_as_of date,
    adsh text,
    basis text,
    refusal text
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH candidates AS (
    SELECT c.adsh, c.stated_on, c.available_on, c.accepted, c.shares, l.basis,
           CASE WHEN regexp_replace(c.form, '/A$', '') IN ('20-F', '40-F', '6-K', '20-FR')
                     AND NOT (l.basis = 'class' AND l.depositary)
                THEN 'foreign_issuer_listing_unverified' END AS refusal
    FROM sec_share_counts_at(p_as_of, false) c
    CROSS JOIN LATERAL (
        SELECT CASE WHEN c.class_key = '' THEN 'sole_class_total' ELSE 'class' END AS basis,
               -- an explicit depositary member, not a depositary title on the
               -- underlying class (AMX's 2023 20-F titles its B shares' member
               -- "American Depositary Shares, each representing 20 B Shares")
               bool_or(o.security_kind = 'depositary'
                       AND c.class_key ~* '(deposit[ao]ry|\mads|\madrs?([0-9]|member|;|$))')
                   AS depositary
        FROM sec_observations_at(p_as_of, false) o
        WHERE o.adsh = c.adsh AND o.cik = c.cik AND o.available_on <= p_as_of
          AND o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
          AND CASE WHEN c.class_key <> '' THEN o.class_key = c.class_key
                   ELSE o.filing_equity_classes = 1
                        AND o.security_kind IN ('equity', 'unknown')
              END
        HAVING count(*) > 0
    ) l
    WHERE c.cik = p_cik AND c.stated_on <= p_as_of
), chosen AS (
    SELECT k.* FROM candidates k
    ORDER BY k.stated_on DESC, k.available_on DESC, k.accepted DESC NULLS LAST, k.adsh DESC,
             k.basis, k.refusal IS NOT NULL
    LIMIT 1
), counts AS (
    SELECT DISTINCT k.shares
    FROM candidates k, chosen h
    WHERE k.adsh = h.adsh AND k.stated_on = h.stated_on AND k.basis = h.basis
      AND k.refusal IS NOT DISTINCT FROM h.refusal
)
SELECT
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM chosen) THEN 'missing'
        WHEN (SELECT h.refusal FROM chosen h) IS NOT NULL THEN 'refused'
        WHEN (SELECT h.stated_on FROM chosen h) < p_as_of - p_max_age_days THEN 'stale'
        WHEN (SELECT count(*) FROM counts) > 1 THEN 'ambiguous'
        ELSE 'resolved'
    END AS status,
    CASE WHEN (SELECT h.refusal FROM chosen h) IS NULL AND (SELECT count(*) FROM counts) = 1
         THEN (SELECT k.shares FROM counts k) END AS shares,
    (SELECT h.stated_on FROM chosen h) AS shares_as_of,
    (SELECT h.adsh FROM chosen h) AS adsh,
    (SELECT h.basis FROM chosen h) AS basis,
    (SELECT h.refusal FROM chosen h) AS refusal
$fn$;

-- The LINES of a CIK as known today: one row per class it ever stated, with the
-- line (one security) the class belongs to. Listed classes (equity, depositary
-- or unknown) are linked by evidence edges:
-- * they showed the same symbol and no filing shows both (Berkshire's 10-Qs tag
--   BRK.B on CommonClassB, its 8-Ks on ClassBCommonStock; Google's class A and
--   class C both showed GOOG, but side by side from April 2014, so no edge), or
-- * they are the one equity class of consecutive complete filings (a
--   single-class filer renaming its member or dropping the dimension).
-- Edges are applied in order of the date the link is first evidenced, then by
-- their evidence count (filings, descending), then by class keys; an edge whose
-- two lines have classes that appear side by side in any filing is dropped, so
-- no line ever holds two classes that coexist (A-B then B-C, with A and C in one
-- filing, gives the lines {A, B} and {C}). line_key is the line's first-stated
-- class (then the lowest class_key). Other classes are their own line.
CREATE OR REPLACE FUNCTION sec_issuer_lines(p_cik bigint)
RETURNS TABLE (class_key text, line_key text)
LANGUAGE plpgsql STABLE PARALLEL SAFE
-- Planned once per call; JIT compilation would cost more than the query.
SET jit = off
AS $fn$
#variable_conflict use_column
DECLARE
    keys text[];
    firsts date[];
    equity boolean[];
    comp integer[];
    pairs text[];
    edge record;
    ia integer;
    ib integer;
    ca integer;
    cb integer;
    i integer;
    j integer;
    clash boolean;
BEGIN
    SELECT array_agg(c.class_key ORDER BY c.class_key),
           array_agg(c.first_on ORDER BY c.class_key),
           array_agg(c.equity ORDER BY c.class_key)
      INTO keys, firsts, equity
    FROM (
        SELECT o.class_key, min(o.source_available_on) AS first_on,
               bool_or(o.security_kind IN ('equity', 'depositary', 'unknown')) AS equity
        FROM sec_observations_at('infinity'::date, true) o
        WHERE o.cik = p_cik
        GROUP BY o.class_key
    ) c;
    IF keys IS NULL THEN
        RETURN;
    END IF;
    comp := ARRAY(SELECT generate_series(1, cardinality(keys)));
    -- classes that appear side by side in one filing, as 'a' || chr(31) || 'b'
    SELECT COALESCE(array_agg(DISTINCT x.class_key || chr(31) || y.class_key), '{}')
      INTO pairs
    FROM sec_observations_at('infinity'::date, true) x
    JOIN sec_observations_at('infinity'::date, true) y
      ON y.adsh = x.adsh AND y.cik = x.cik AND y.class_key <> x.class_key
    WHERE x.cik = p_cik
      AND x.security_kind IN ('equity', 'depositary', 'unknown')
      AND y.security_kind IN ('equity', 'depositary', 'unknown');
    FOR edge IN
        WITH equity_rows AS (
            SELECT o.adsh, o.class_key, o.ticker_key, o.source_available_on AS on_date,
                   o.accepted, o.filing_equity_classes, o.filing_complete
            FROM sec_observations_at('infinity'::date, true) o
            WHERE o.cik = p_cik AND o.security_kind IN ('equity', 'depositary', 'unknown')
        ), shown AS (
            SELECT e.class_key, e.ticker_key, min(e.on_date) AS first_on,
                   count(DISTINCT e.adsh) AS filings
            FROM equity_rows e
            GROUP BY e.class_key, e.ticker_key
        ), shared AS (
            SELECT x.class_key AS a, y.class_key AS b,
                   min(GREATEST(x.first_on, y.first_on)) AS on_date,
                   sum(x.filings + y.filings) AS evidence
            FROM shown x
            JOIN shown y ON y.ticker_key = x.ticker_key AND y.class_key > x.class_key
            GROUP BY x.class_key, y.class_key
        ), sole AS (
            SELECT f.key, f.on_date,
                   lag(f.key) OVER (ORDER BY f.on_date, f.accepted NULLS FIRST, f.adsh)
                       AS prev_key
            FROM (
                SELECT e.adsh, min(e.on_date) AS on_date, max(e.accepted) AS accepted,
                       CASE WHEN count(DISTINCT e.class_key) = 1
                                 AND bool_and(e.filing_equity_classes = 1)
                            THEN min(e.class_key) END AS key
                FROM equity_rows e
                WHERE e.filing_complete
                GROUP BY e.adsh
            ) f
        ), relabels AS (
            SELECT LEAST(s.prev_key, s.key) AS a, GREATEST(s.prev_key, s.key) AS b,
                   min(s.on_date) AS on_date, count(*) AS evidence
            FROM sole s
            WHERE s.key IS NOT NULL AND s.prev_key IS NOT NULL AND s.prev_key <> s.key
            GROUP BY LEAST(s.prev_key, s.key), GREATEST(s.prev_key, s.key)
        )
        SELECT u.a, u.b, min(u.on_date) AS on_date, sum(u.evidence) AS evidence
        FROM (SELECT * FROM shared UNION ALL SELECT * FROM relabels) u
        GROUP BY u.a, u.b
        ORDER BY min(u.on_date), sum(u.evidence) DESC, u.a, u.b
    LOOP
        ia := array_position(keys, edge.a);
        ib := array_position(keys, edge.b);
        ca := comp[ia];
        cb := comp[ib];
        CONTINUE WHEN ca = cb OR (edge.a || chr(31) || edge.b) = ANY(pairs);
        clash := false;
        FOR i IN 1 .. cardinality(keys) LOOP
            CONTINUE WHEN comp[i] <> ca;
            FOR j IN 1 .. cardinality(keys) LOOP
                IF comp[j] = cb AND (keys[i] || chr(31) || keys[j]) = ANY(pairs) THEN
                    clash := true;
                    EXIT;
                END IF;
            END LOOP;
            EXIT WHEN clash;
        END LOOP;
        CONTINUE WHEN clash;
        FOR i IN 1 .. cardinality(keys) LOOP
            IF comp[i] = cb THEN
                comp[i] := ca;
            END IF;
        END LOOP;
    END LOOP;
    RETURN QUERY
    SELECT k.key,
           CASE WHEN equity[k.n::integer] THEN (
               SELECT keys[m.n] FROM generate_subscripts(keys, 1) AS m(n)
               WHERE comp[m.n] = comp[k.n::integer] AND equity[m.n]
               ORDER BY firsts[m.n], keys[m.n]
               LIMIT 1)
           ELSE k.key END
    FROM unnest(keys) WITH ORDINALITY AS k(key, n);
END
$fn$;

-- Lineage engine (today's truth: current rows at their filing's public date,
-- every current amendment, ends judged with everything known today). Each
-- (CIK, line) that showed the ticker is followed through its candidate filings
-- (filings tagging a class of the line, and every complete filing with one
-- equity class in total) with the end, definitive-end and successor rules of
-- sec_ticker_holds, evaluated once per change date (a filing, an end, or a
-- statement reaching p_max_age_days). One row per RUN in which the line held the
-- ticker: [valid_from, valid_to), valid_to NULL while open; end_reason
-- 'other_symbol', the end form, or 'stale' (no confirmation within
-- p_max_age_days; NULL p_max_age_days never goes stale); first/last_confirmed_on:
-- the run's first and last statements showing the ticker.
CREATE OR REPLACE FUNCTION sec_ticker_line_runs(p_ticker text, p_max_age_days integer DEFAULT NULL)
RETURNS TABLE (
    cik bigint,
    line_key text,
    class_key text,
    valid_from date,
    valid_to date,
    end_reason text,
    first_confirmed_on date,
    last_confirmed_on date
)
LANGUAGE sql STABLE PARALLEL SAFE
-- Planned once per call; JIT compilation would cost more than the query.
SET jit = off
AS $fn$
WITH key AS (
    SELECT regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g') AS k
), shown AS (
    SELECT o.cik, o.class_key, o.security_kind, o.source_available_on AS known_on
    FROM sec_observations_at('infinity'::date, true) o, key
    WHERE o.ticker_key = key.k
), relevant AS (
    SELECT s.* FROM shown s
    WHERE s.security_kind IN ('equity', 'depositary', 'unknown')
       OR NOT EXISTS (SELECT 1 FROM shown e
                      WHERE e.security_kind IN ('equity', 'depositary', 'unknown'))
), holder_ciks AS (
    SELECT DISTINCT r.cik FROM relevant r
), lines AS MATERIALIZED (
    SELECT h.cik, l.class_key, l.line_key
    FROM holder_ciks h CROSS JOIN LATERAL sec_issuer_lines(h.cik) l
), held AS (
    SELECT r.cik, l.line_key, min(r.known_on) AS first_on
    FROM relevant r JOIN lines l ON l.cik = r.cik AND l.class_key = r.class_key
    GROUP BY r.cik, l.line_key
), cik_first AS (
    SELECT r.cik, min(r.known_on) AS first_on FROM relevant r GROUP BY r.cik
), candidates AS MATERIALIZED (
    SELECT h.cik, h.line_key, o.adsh,
           max(o.source_available_on) AS known_on,
           max(o.accepted) AS accepted,
           bool_or(l.line_key = h.line_key AND o.ticker_key = key.k) AS shows,
           bool_or(l.line_key = h.line_key AND o.ticker_key = key.k
                   AND o.security_title IS NOT NULL) AS titled,
           (array_agg(o.class_key ORDER BY o.class_key)
               FILTER (WHERE l.line_key = h.line_key AND o.ticker_key = key.k))[1] AS class_key
    FROM held h
    CROSS JOIN key
    JOIN sec_observations_at('infinity'::date, true) o ON o.cik = h.cik
    LEFT JOIN lines l ON l.cik = o.cik AND l.class_key = o.class_key
    WHERE l.line_key = h.line_key
       OR (o.filing_complete AND o.filing_equity_classes = 1
           AND o.security_kind IN ('equity', 'depositary', 'unknown'))
    GROUP BY h.cik, h.line_key, o.adsh
), ends AS MATERIALIZED (
    SELECT h.cik, e.available_on, e.filed, e.form, e.adsh, e.definitive
    FROM holder_ciks h
    CROSS JOIN LATERAL sec_issuer_end_events(h.cik, 'infinity'::date, true) e
), starts AS MATERIALIZED (
    SELECT h.cik, r.filed, r.available_on
    FROM holder_ciks h
    CROSS JOIN LATERAL sec_registration_starts(h.cik, 'infinity'::date, true) r
), blocking AS (
    -- ends after which the line's later candidates do not count (definitive end
    -- of a ticker shown before it, or the ticker moved to another CIK)
    SELECT h.cik, h.line_key, e.available_on, e.filed
    FROM held h
    JOIN ends e ON e.cik = h.cik
    WHERE (e.definitive AND h.first_on < e.available_on)
       OR EXISTS (
           SELECT 1 FROM cik_first o
           WHERE o.cik <> h.cik
             AND o.first_on BETWEEN e.available_on - 30 AND (
                 SELECT min(c.known_on) FROM candidates c
                 WHERE c.cik = h.cik AND c.line_key = h.line_key AND c.shows
                   AND c.known_on >= e.available_on))
), counted AS MATERIALIZED (
    SELECT c.* FROM candidates c
    WHERE c.titled OR NOT EXISTS (
        SELECT 1 FROM blocking b
        WHERE b.cik = c.cik AND b.line_key = c.line_key AND b.available_on <= c.known_on
          AND NOT EXISTS (
              SELECT 1 FROM starts r
              WHERE r.cik = c.cik AND r.filed > b.filed AND r.available_on <= c.known_on))
), bounds AS (
    SELECT DISTINCT h.cik, h.line_key, x.on_date
    FROM held h
    CROSS JOIN LATERAL (
        SELECT c.known_on AS on_date FROM counted c
        WHERE c.cik = h.cik AND c.line_key = h.line_key
        UNION
        SELECT c.known_on + p_max_age_days + 1 FROM counted c
        WHERE c.cik = h.cik AND c.line_key = h.line_key AND c.shows
          AND p_max_age_days IS NOT NULL
        UNION
        SELECT e.available_on FROM ends e WHERE e.cik = h.cik
    ) x
), states AS (
    SELECT b.cik, b.line_key, b.on_date, s.known_on AS statement_on, s.shows, s.class_key,
           le.available_on AS end_on, le.form AS end_form,
           (SELECT max(c.known_on) FROM counted c
            WHERE c.cik = b.cik AND c.line_key = b.line_key AND c.shows
              AND c.known_on <= b.on_date) AS confirmed_on
    FROM bounds b
    LEFT JOIN LATERAL (
        SELECT c.known_on, c.shows, c.class_key FROM counted c
        WHERE c.cik = b.cik AND c.line_key = b.line_key AND c.known_on <= b.on_date
        ORDER BY c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh DESC
        LIMIT 1
    ) s ON true
    LEFT JOIN LATERAL (
        SELECT e.available_on, e.form FROM ends e
        WHERE e.cik = b.cik AND e.available_on <= b.on_date
        ORDER BY e.available_on DESC, e.adsh DESC
        LIMIT 1
    ) le ON true
), judged AS (
    SELECT st.*,
           st.statement_on IS NOT NULL AND st.shows
           AND NOT COALESCE(st.end_on > st.statement_on, false)
           AND (p_max_age_days IS NULL OR st.on_date - st.statement_on <= p_max_age_days)
               AS holding,
           CASE
               WHEN st.end_on > st.statement_on THEN st.end_form
               WHEN NOT st.shows THEN 'other_symbol'
               WHEN p_max_age_days IS NOT NULL AND st.on_date - st.statement_on > p_max_age_days
                   THEN 'stale'
           END AS reason
    FROM states st
), marked AS (
    SELECT j.*,
           sum(CASE WHEN j.holding AND NOT COALESCE(j.prev_holding, false) THEN 1 ELSE 0 END)
               OVER (PARTITION BY j.cik, j.line_key ORDER BY j.on_date) AS run_no
    FROM (
        SELECT j.*, lag(j.holding) OVER (PARTITION BY j.cik, j.line_key ORDER BY j.on_date)
                   AS prev_holding
        FROM judged j
    ) j
), runs AS (
    SELECT m.cik, m.line_key, m.run_no,
           min(m.on_date) FILTER (WHERE m.holding) AS valid_from,
           min(m.statement_on) FILTER (WHERE m.holding) AS first_confirmed_on,
           max(m.confirmed_on) FILTER (WHERE m.holding) AS last_confirmed_on,
           (array_agg(m.class_key ORDER BY m.on_date DESC) FILTER (WHERE m.holding))[1]
               AS class_key
    FROM marked m
    WHERE m.run_no > 0
    GROUP BY m.cik, m.line_key, m.run_no
), run_ends AS (
    SELECT DISTINCT ON (m.cik, m.line_key, m.run_no) m.cik, m.line_key, m.run_no,
           m.on_date AS valid_to, m.reason
    FROM marked m
    WHERE m.run_no > 0 AND NOT m.holding
    ORDER BY m.cik, m.line_key, m.run_no, m.on_date
)
SELECT r.cik, r.line_key, r.class_key, r.valid_from, e.valid_to, e.reason,
       r.first_confirmed_on, r.last_confirmed_on
FROM runs r
LEFT JOIN run_ends e ON e.cik = r.cik AND e.line_key = r.line_key AND e.run_no = r.run_no
$fn$;

-- Lineage engine: when a line (p_cik, p_line_key) of an issuer was evidenced
-- alive, under any symbol (Meta's line is alive through its FB years). The line
-- is followed through its candidate filings (filings tagging one of its
-- classes, and every complete filing with one equity class in total) with the
-- end rules of sec_ticker_holds: after a definitive end a filing counts only with
-- a 12(b) title, a registration after the end, or symbols the CIK first showed
-- after the end. Alive at a date: its latest counting filing has a row of the
-- line, no applying end is newer, and (p_max_age_days not NULL) it is at most
-- p_max_age_days old. One row per run, [valid_from, valid_to); end_reason the
-- end form, 'merged' (a one-class filing of another line), or 'stale'.
CREATE OR REPLACE FUNCTION sec_line_alive_runs(
    p_cik bigint, p_line_key text, p_max_age_days integer DEFAULT 400
)
RETURNS TABLE (
    valid_from date,
    valid_to date,
    end_reason text,
    first_confirmed_on date,
    last_confirmed_on date,
    symbols text[]
)
LANGUAGE sql STABLE PARALLEL SAFE
-- Planned once per call; JIT compilation would cost more than the query.
SET jit = off
AS $fn$
WITH lines AS MATERIALIZED (
    SELECT l.class_key, l.line_key FROM sec_issuer_lines(p_cik) l
), candidates AS MATERIALIZED (
    SELECT o.adsh,
           max(o.source_available_on) AS known_on,
           max(o.accepted) AS accepted,
           bool_or(l.line_key = p_line_key) AS has_line,
           bool_or(l.line_key = p_line_key AND o.security_title IS NOT NULL) AS titled,
           array_agg(DISTINCT o.ticker_key) FILTER (WHERE l.line_key = p_line_key) AS keys,
           array_agg(DISTINCT o.ticker ORDER BY o.ticker)
               FILTER (WHERE l.line_key = p_line_key) AS tickers
    FROM sec_observations_at('infinity'::date, true) o
    LEFT JOIN lines l ON l.class_key = o.class_key
    WHERE o.cik = p_cik
      AND (l.line_key = p_line_key
           OR (o.filing_complete AND o.filing_equity_classes = 1
               AND o.security_kind IN ('equity', 'depositary', 'unknown')))
    GROUP BY o.adsh
), ends AS MATERIALIZED (
    SELECT e.* FROM sec_issuer_end_events(p_cik, 'infinity'::date, true) e
), counted AS MATERIALIZED (
    SELECT c.* FROM candidates c
    WHERE c.titled OR NOT EXISTS (
        SELECT 1 FROM ends d
        WHERE d.definitive AND d.available_on <= c.known_on
          AND EXISTS (
              SELECT 1 FROM candidates x
              WHERE x.has_line AND x.known_on < d.available_on AND x.keys && c.keys)
          AND NOT EXISTS (
              SELECT 1 FROM sec_registration_starts(p_cik, 'infinity'::date, true) r
              WHERE r.filed > d.filed AND r.available_on <= c.known_on))
), bounds AS (
    SELECT c.known_on AS on_date FROM counted c
    UNION
    SELECT c.known_on + p_max_age_days + 1 FROM counted c
    WHERE c.has_line AND p_max_age_days IS NOT NULL
    UNION
    SELECT e.available_on FROM ends e
), states AS (
    SELECT b.on_date, s.known_on AS statement_on, s.has_line, s.tickers,
           le.available_on AS end_on, le.form AS end_form
    FROM bounds b
    LEFT JOIN LATERAL (
        SELECT c.known_on, c.has_line, c.tickers FROM counted c
        WHERE c.known_on <= b.on_date
        ORDER BY c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh DESC
        LIMIT 1
    ) s ON true
    LEFT JOIN LATERAL (
        SELECT e.available_on, e.form FROM ends e WHERE e.available_on <= b.on_date
        ORDER BY e.available_on DESC, e.adsh DESC
        LIMIT 1
    ) le ON true
), judged AS (
    SELECT st.*,
           st.statement_on IS NOT NULL AND st.has_line
           AND NOT COALESCE(st.end_on > st.statement_on, false)
           AND (p_max_age_days IS NULL OR st.on_date - st.statement_on <= p_max_age_days)
               AS alive,
           CASE
               WHEN st.end_on > st.statement_on THEN st.end_form
               WHEN NOT st.has_line THEN 'merged'
               WHEN p_max_age_days IS NOT NULL AND st.on_date - st.statement_on > p_max_age_days
                   THEN 'stale'
           END AS reason
    FROM states st
), marked AS (
    SELECT j.*,
           sum(CASE WHEN j.alive AND NOT COALESCE(j.prev_alive, false) THEN 1 ELSE 0 END)
               OVER (ORDER BY j.on_date) AS run_no
    FROM (SELECT j.*, lag(j.alive) OVER (ORDER BY j.on_date) AS prev_alive FROM judged j) j
), runs AS (
    SELECT m.run_no,
           min(m.on_date) FILTER (WHERE m.alive) AS valid_from,
           min(m.statement_on) FILTER (WHERE m.alive) AS first_confirmed_on,
           max(m.statement_on) FILTER (WHERE m.alive) AS last_confirmed_on
    FROM marked m
    WHERE m.run_no > 0
    GROUP BY m.run_no
), run_ends AS (
    SELECT DISTINCT ON (m.run_no) m.run_no, m.on_date AS valid_to, m.reason
    FROM marked m
    WHERE m.run_no > 0 AND NOT m.alive
    ORDER BY m.run_no, m.on_date
)
SELECT r.valid_from, e.valid_to, e.reason, r.first_confirmed_on, r.last_confirmed_on,
       ARRAY(SELECT DISTINCT t FROM marked m, unnest(m.tickers) t
             WHERE m.run_no = r.run_no AND m.alive ORDER BY t) AS symbols
FROM runs r
LEFT JOIN run_ends e ON e.run_no = r.run_no
ORDER BY r.valid_from
$fn$;

-- Price lineage evidence for consumers that admit a stored price row of ticker T
-- at date t for issuer p_cik, line L (the line of p_class_key: pass the class of
-- today's latest equity row showing T) only if L is evidenced alive at t under
-- any symbol and no other holder of T's key is evidenced holding it at t. Uses
-- lineage semantics (today's truth). Rows:
-- * evidence 'alive'        : the runs of sec_line_alive_runs(p_cik, L, 400);
-- * evidence 'other_holder' : the runs of sec_ticker_line_runs(T, 400) of every
--   other holder of T's key: another CIK, or another line of p_cik (Google's
--   class A held GOOG until April 2014, then class C). A run of another CIK whose
--   claims lie strictly inside a run of a different CIK (a misfiled 10-Q under a
--   shell CIK, another issuer typing the symbol) is not evidence of holding.
-- [valid_from, valid_to) with valid_to exclusive: an open run whose last
-- statement is older than 400 days ends at its stale cutoff (statement + 401).
-- source names the evidence ('sec_cover': cover-page statements and end filings;
-- other sources, such as insider filings, add rows of their own).
CREATE OR REPLACE FUNCTION sec_line_price_evidence(
    p_ticker text, p_cik bigint, p_class_key text
)
RETURNS TABLE (
    evidence text,
    holder_cik bigint,
    line_key text,
    valid_from date,
    valid_to date,
    end_reason text,
    first_confirmed_on date,
    last_confirmed_on date,
    symbols text[],
    source text
)
LANGUAGE sql STABLE PARALLEL SAFE
-- Planned once per call; JIT compilation would cost more than the query.
SET jit = off
AS $fn$
WITH wanted AS (
    SELECT COALESCE((SELECT l.line_key FROM sec_issuer_lines(p_cik) l
                     WHERE l.class_key = p_class_key), p_class_key) AS line_key
), held AS MATERIALIZED (
    SELECT r.* FROM sec_ticker_line_runs(p_ticker, 400) r
)
SELECT 'alive', p_cik, w.line_key, a.valid_from, a.valid_to, a.end_reason,
       a.first_confirmed_on, a.last_confirmed_on, a.symbols, 'sec_cover'
FROM wanted w
CROSS JOIN LATERAL sec_line_alive_runs(p_cik, w.line_key, 400) a
UNION ALL
SELECT 'other_holder', o.cik, o.line_key, o.valid_from, o.valid_to, o.end_reason,
       o.first_confirmed_on, o.last_confirmed_on,
       ARRAY[regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')], 'sec_cover'
FROM held o, wanted w
WHERE (o.cik, o.line_key) <> (p_cik, w.line_key)
  AND NOT (o.cik <> p_cik AND EXISTS (
      SELECT 1 FROM held y
      WHERE y.cik <> o.cik AND y.first_confirmed_on < o.first_confirmed_on
        AND y.last_confirmed_on > o.last_confirmed_on))
ORDER BY 1, 4, 2
$fn$;

-- Price lineage: which stored price rows of a ticker belong to an issuer's line,
-- as runs with their neighbouring holders (sec_ticker_line_runs, no staleness).
-- One row per run of a line of p_cik holding the ticker; with p_class_key, only
-- the line that class belongs to (pass the class of today's latest equity row
-- showing the ticker):
-- * class_key         : the run's latest class showing the ticker;
-- * valid_from        : the date the run starts;
-- * valid_to          : the date its end is evidenced (end_reason 'other_symbol'
--                       or the end form); NULL = open;
-- * last_confirmed_on : the run's latest statement showing the ticker;
-- * prior_holder_end  : NULL when no other holder's run started before this run;
--                       this run's valid_from when such a run has no end
--                       evidence (valid_to NULL: open; its last confirmation is
--                       not an end, so nothing before this run is admitted);
--                       else the latest valid_to of those runs;
-- * next_holder_start : the earliest start of another holder's run on or after
--                       this run's start; NULL if none;
-- * line_key          : the line (stable across member renames).
-- Other holders are the lines of other CIKs and the OTHER LINES OF p_cik under
-- the same ticker. Each date is admitted for at most one holder: an earlier open
-- run yields at the next run's valid_from. A run of another CIK whose claims lie
-- strictly inside a run of a different CIK bounds nothing.
-- Consumers admit rows in [valid_from, valid_to), rows in
-- (prior_holder_end, valid_from) only after a continuity check, and refuse rows
-- at or before prior_holder_end or at or after next_holder_start.
-- sec_line_price_evidence is the evidence-only alternative.
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
-- Planned once per call; JIT compilation would cost more than the query.
SET jit = off
AS $fn$
WITH spans AS MATERIALIZED (
    SELECT r.* FROM sec_ticker_line_runs(p_ticker, NULL) r
), others AS (
    SELECT o.* FROM spans o
    WHERE NOT (o.cik <> p_cik AND EXISTS (
        SELECT 1 FROM spans y
        WHERE y.cik <> o.cik AND y.first_confirmed_on < o.first_confirmed_on
          AND y.last_confirmed_on > o.last_confirmed_on))
), wanted AS (
    SELECT CASE WHEN p_class_key IS NULL THEN NULL ELSE COALESCE((
               SELECT l.line_key FROM sec_issuer_lines(p_cik) l
               WHERE l.class_key = p_class_key), p_class_key) END AS line_key
)
SELECT a.class_key, a.valid_from, a.valid_to, a.end_reason, a.last_confirmed_on,
       (SELECT CASE WHEN bool_or(o.valid_to IS NULL) THEN a.valid_from
                    ELSE max(o.valid_to) END
        FROM others o
        WHERE (o.cik, o.line_key) <> (a.cik, a.line_key)
          AND o.valid_from < a.valid_from) AS prior_holder_end,
       (SELECT min(o.valid_from) FROM others o
        WHERE (o.cik, o.line_key) <> (a.cik, a.line_key)
          AND o.valid_from >= a.valid_from) AS next_holder_start,
       a.line_key
FROM spans a, wanted w
WHERE a.cik = p_cik
  AND (w.line_key IS NULL OR a.line_key = w.line_key)
ORDER BY a.valid_from, a.line_key
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
    sec_registration_starts(bigint, date, boolean),
    sec_issuer_end_events(bigint, date, boolean),
    sec_ticker_holds(text, date, integer, boolean),
    sec_ticker_issuer_at(text, date, integer),
    sec_issuer_line_at(bigint, text, date, integer),
    sec_cover_class_shares_at(bigint, text, date, integer),
    sec_cover_ticker_shares_at(text, bigint, date, integer),
    sec_issuer_lines(bigint),
    sec_ticker_line_runs(text, integer),
    sec_line_alive_runs(bigint, text, integer),
    sec_line_price_evidence(text, bigint, text),
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
        'sec_registration_starts(bigint, date, boolean)',
        'sec_issuer_end_events(bigint, date, boolean)',
        'sec_ticker_holds(text, date, integer, boolean)',
        'sec_ticker_issuer_at(text, date, integer)',
        'sec_issuer_line_at(bigint, text, date, integer)',
        'sec_cover_class_shares_at(bigint, text, date, integer)',
        'sec_cover_ticker_shares_at(text, bigint, date, integer)',
        'sec_issuer_lines(bigint)',
        'sec_ticker_line_runs(text, integer)',
        'sec_line_alive_runs(bigint, text, integer)',
        'sec_line_price_evidence(text, bigint, text)',
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
