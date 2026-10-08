-- Rollback of schemas/sec_ticker_cik_history_v2.sql: the v1 definitions of the
-- ten functions v2 changes, copied from schemas/sec_ticker_cik_history_v1.sql, in
-- one transaction. Every table and row is kept, and so are the columns v2 added
-- (parser_version, retired_reason). One v2 rule stays: the four functions that
-- gate point-in-time rows (sec_observations_at, sec_share_counts_at,
-- sec_registration_end_events, sec_registration_starts) keep hiding a version
-- retired as a parser correction. A v2 re-derivation dated the corrected reading
-- from the filing; with v1's predicate the old reading would be visible beside it
-- until the re-derivation date. Where no parser correction happened (no row has
-- retired_reason 'parser_correction') these functions answer exactly as v1's.
-- Rows a v2 loader wrote, such as the class of a Form 8-A, are valid v1 rows. Apply as the role that applied v2, with psql -v ON_ERROR_STOP=1. To
-- remove the whole schema afterwards, apply schemas/sec_ticker_cik_history_v1.rollback.sql.
BEGIN;
SET LOCAL lock_timeout = '5s';

DROP FUNCTION IF EXISTS sec_issuer_end_events(bigint, date, boolean);
DROP FUNCTION IF EXISTS sec_registration_end_events(bigint, date, boolean);

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
                AND o.retired_reason IS DISTINCT FROM 'parser_correction'
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
                AND c.retired_reason IS DISTINCT FROM 'parser_correction'
      END
$fn$;

-- The end filings (15-12B/15-12G/15-15D, 15F-12B/15F-12G/15F-15D, 25/25-NSE) of
-- a CIK visible at D, as
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
      AND e.form IN ('15-12B', '15-12G', '15-15D', '15F-12B', '15F-12G', '15F-15D',
                     '25', '25-NSE', '15-12B/A', '15-12G/A', '15-15D/A', '15F-12B/A',
                     '15F-12G/A', '15F-15D/A', '25/A', '25-NSE/A')
      AND CASE WHEN p_current
               THEN e.retired_on IS NULL
                    AND (e.source_available_on <= p_as_of OR e.form LIKE '%/A')
               ELSE e.available_on <= p_as_of
                    AND (e.retired_on IS NULL OR e.retired_on > p_as_of)
                    AND e.retired_reason IS DISTINCT FROM 'parser_correction'
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
                AND e.retired_reason IS DISTINCT FROM 'parser_correction'
      END
$fn$;

-- The end filings of a CIK that end its equity lines at D, and whether each is
-- DEFINITIVE. Judged against the CIK's latest filing with an equity line filed
-- before the event (prior symbols: its equity symbols when the end names an
-- equity class, else all its symbols) and its latest complete filing filed before
-- it (prior classes), among the filings visible at D: filing dates place them,
-- knowledge dates only gate what is visible (an end re-derived years later is
-- still judged against the covers before its filing).
-- A Form 15F (a foreign private issuer's termination under Rule 12h-6) counts as
-- the Form 15 it stands for: 15F-12B as 15-12B, 15F-12G as 15-12G, 15F-15D as
-- 15-15D. An end APPLIES unless
-- * the filing concerns another class (class_kind 'other'), or
-- * it may concern a class other than the listed symbol: the issuer then showed
--   several symbols and the filing names fewer classes than it showed symbols
--   (a 15-12G or 15-15D for one class of a multi-class issuer, too), or
-- * it is a 25/25-NSE/15-12B (12(b) removal) on a secondary exchange, or one
--   that a registration filed from 30 days before to 10 days after makes a
--   transfer (PepsiCo's NYSE -> Nasdaq move: Form 25 and 8-A12B the same day),
--   unless the class was extinguished.
-- An applying end of an equity class (class_kind 'equity', naming at least as
-- many classes as the issuer has) is DEFINITIVE when the 25-NSE says the class
-- was extinguished (12d2-2(a)), or when an applying delisting (25/25-NSE, not
-- secondary, not a transfer) naming every listed class (as many classes as the
-- issuer showed equity symbols) and an applying termination (15-12B/15-12G/
-- 15-15D or 15F) naming every equity class are both on file within 120 days of
-- each other, unless the shareholder base continued:
-- the first cover share count filed after the end is within 0.8-1.25 times the
-- last one filed before it (a holding-company reorganization or REIT conversion
-- that keeps the CIK: United Fire 2012, Ulta and SBA 2017; American Greetings
-- reported 100 shares after its merger). Other applying ends (including class_kind 'unknown'
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
    -- the covers visible at D, placed by when they were filed (source_on)
    SELECT o.adsh,
           max(o.source_available_on) AS source_on,
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
           max(c.source_available_on) AS source_on,
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
            FROM filings f WHERE f.source_on < v.filed + 1
            ORDER BY f.source_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
            LIMIT 1) AS prior_symbols,
           GREATEST(COALESCE((
               SELECT f.classes FROM filings f WHERE f.complete AND f.source_on < v.filed + 1
               ORDER BY f.source_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
               LIMIT 1), 1), 1) AS prior_classes,
           v.registered_nearby AND NOT COALESCE(v.extinguished, false) AS transfer
    FROM versions v
), applies AS (
    SELECT j.*,
           COALESCE(j.class_kind, 'unknown') <> 'other'
           AND (j.prior_symbols = 1
                OR (j.class_kind = 'equity' AND j.class_count >= j.prior_symbols))
           AND (replace(j.form, '15F-', '15-') IN ('15-12G', '15-15D')
                OR (NOT j.transfer AND j.venue_kind IS DISTINCT FROM 'secondary'))
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
           FROM (SELECT t.total FROM totals t WHERE t.source_on < a.filed + 1
                 ORDER BY t.source_on DESC, t.accepted DESC NULLS LAST, t.adsh DESC
                 LIMIT 1) before_end,
                (SELECT t.total FROM totals t WHERE t.source_on >= a.filed + 1
                 ORDER BY t.source_on, t.accepted NULLS FIRST, t.adsh
                 LIMIT 1) after_end), false)
       AND (
           (a.form = '25-NSE' AND COALESCE(a.extinguished, false))
           OR (replace(a.form, '15F-', '15-') IN ('15-12B', '15-12G', '15-15D') AND EXISTS (
               SELECT 1 FROM applies d
               WHERE d.effective AND d.applying AND d.equity_delisting
                 AND d.class_count >= d.prior_symbols
                 AND d.filed BETWEEN a.filed - 120 AND a.filed + 120))
           OR (a.equity_delisting AND a.class_count >= a.prior_symbols AND EXISTS (
               SELECT 1 FROM applies t
               WHERE t.effective AND t.applying
                 AND replace(t.form, '15F-', '15-') IN ('15-12B', '15-12G', '15-15D')
                 AND t.class_kind = 'equity' AND t.class_count >= t.prior_classes
                 AND t.filed BETWEEN a.filed - 120 AND a.filed + 120))
       ) AS definitive
FROM current_ends a
WHERE a.available_on <= p_as_of
$fn$;

-- Each CIK's hold of a ticker at D (internal helper; one row per CIK that showed
-- the ticker by D). Listed rows (equity, depositary or unknown) decide whenever
-- one showed the ticker (filers also tag their common symbol on notes lines). A
-- hold is followed through its candidate statements:
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

-- Ownership and grants of every routine, as v1 sets them (the two functions
-- created again above start with default privileges).
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
        FOREACH item IN ARRAY routines LOOP
            EXECUTE format('ALTER FUNCTION %s OWNER TO worker_writer', item);
        END LOOP;
    END IF;
    FOREACH reader IN ARRAY ARRAY['app_runtime', 'app_analytics_ro', 'mcp_ro'] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = reader) THEN
            FOREACH item IN ARRAY routines LOOP
                EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO %I', item, reader);
            END LOOP;
        END IF;
    END LOOP;
END $$;

COMMIT;
