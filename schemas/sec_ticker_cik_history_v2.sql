-- SEC ticker -> issuer (CIK, class) history, v2: the follow-up of
-- schemas/sec_ticker_cik_history_v1.sql (applied first; this file changes the
-- functions and adds audit columns, writing no row).
-- docs/runbooks/sec-ticker-cik-history.md documents both.
--
-- * End events carry an EFFECTIVE date apart from their knowledge date.
--   sec_issuer_end_events returns effective_on = the end filing's date + 1 (or,
--   point-in-time, the restating amendment's date + 1 when the end applies only
--   as restated) and keeps available_on, the date the end as it applies became
--   known, as the visibility gate. Holds, lines and runs order ends against
--   statements by effective_on: an end the public record adds years later (a
--   rebuilt index that moves it to this CIK) is visible from that correction but
--   takes effect at its filing, so a cover filed after the filing still reopens
--   a hold that is not definitively ended.
-- * sec_registration_end_events adds restated_filed (the restating amendment's
--   filing date), class_description and original_class_description.
-- * A registration (8-A12B, 8-A12G, 10-12B, 10-12G, and a successor's 8-K12B or
--   8-K12G3, now registration events too) corroborates a transfer, or
--   relists after a definitive end, only when it registers an equity class or a
--   class not read (sec_registration_starts): the 8-A of notes or preferred filed
--   near a delisting of the common stock is not a transfer (Statera, 2023).
-- * A 15-12G or 15-15D (or 15F-12G, 15F-15D), which may terminate an unlisted
--   class, ends the listed lines only when it names as many equity classes as
--   the issuer's latest complete filing showed, listed or not: a 15-12G for the
--   unlisted class B does not close the listed class A.
-- * A post-end share count proves the shareholder base continued only when it is
--   stated on or after the end, not merely filed after it.
-- * An end that names some of an issuer's listed classes by letter ("Class B
--   common stock") ends those classes' holds and lines only (class_keys); one
--   naming only classes the issuer does not list ends none.
-- * A successor's registration of the CIK's class (8-K12B, 8-K12G3 under the same
--   CIK) near an end makes it no end, and after a definitive end relists the class.
-- * An end of an equity class ends the listed lines, and a preferred, warrant,
--   unit, right or notes line of the same CIK only when it names that instrument
--   (sec_issuer_end_events.class_kind, named_kinds).
-- * After a definitive end only a registration of the class (or a symbol first
--   shown after it) starts a new run: a later cover, even with a 12(b) title, no
--   longer reopens it.
-- * An undimensioned class shown only on one-class covers is not linked to a
--   dimensioned class shown only beside another listed class merely because it
--   later carries the same symbol (Google's GOOG before and after its 2014 class
--   C); in lineage, such a sole-class line is followed through the issuer's later
--   complete filings, so the recapitalization ends its run.
-- * A non-listed row (debt, preferred...) showing a ticker is evidence only while
--   no listed row showing it was known at or before it, per holder and over time,
--   not across the ticker's whole history: an earlier holder that tagged its
--   ticker only on non-listed rows keeps its run when a later issuer reuses it.
--
-- * A parser correction is a restatement of our reading, not a change of the
--   public record. The fact tables record why a version was retired
--   (retired_reason): 'source' (the SEC republished a package, an index dropped or
--   reassigned a row, a monthly package was superseded; NULL on rows retired
--   before v2 means the same) keeps the retired version visible before its
--   retirement date; 'parser_correction' (the same filing read again by another
--   parser version) makes it visible at no date, and the loader dates the new
--   reading from the filing's own public date. parser_version records the parser
--   that read each fact version (NULL: read before v2) and each package version.
--
-- Governed, owner-applied migration (postgres or worker_writer, psql with
-- ON_ERROR_STOP), one transaction, idempotent. It adds nullable columns without
-- a default (a catalog change: no rewrite, no scan; ACCESS EXCLUSIVE on the four
-- tables for the transaction's milliseconds) and their CHECKs NOT VALID (every
-- existing row is NULL there; new rows are checked), and replaces the CHECK on
-- sec_registration_events.form with one that also admits 8-K12B and 8-K12G3 (NOT
-- VALID: every existing row is in the narrower list). Two functions change their
-- result columns (sec_registration_end_events, sec_issuer_end_events) and are
-- dropped and created again; the others are replaced in place. No row is
-- written.
-- Rollback: schemas/sec_ticker_cik_history_v2.rollback.sql (restores the v1
-- functions; keeps every row and the new columns, which v1 ignores).
BEGIN;
SET LOCAL lock_timeout = '5s';

ALTER TABLE sec_ticker_cik_observations
    ADD COLUMN IF NOT EXISTS parser_version text,
    ADD COLUMN IF NOT EXISTS retired_reason text;
ALTER TABLE sec_cover_share_counts
    ADD COLUMN IF NOT EXISTS parser_version text,
    ADD COLUMN IF NOT EXISTS retired_reason text;
ALTER TABLE sec_registration_events
    ADD COLUMN IF NOT EXISTS retired_reason text;
ALTER TABLE sec_ticker_cik_packages
    ADD COLUMN IF NOT EXISTS parser_version text;
-- Successor registrations (8-K12B, 8-K12G3 and their /A) are registration
-- events too. The CHECK v1 put on form is replaced by one that admits them (NOT
-- VALID: every existing row satisfies the narrower v1 list).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                   WHERE conrelid = to_regclass('sec_registration_events')
                     AND conname = 'sec_registration_events_form_v2_check') THEN
        ALTER TABLE sec_registration_events
            DROP CONSTRAINT IF EXISTS sec_registration_events_form_check;
        ALTER TABLE sec_registration_events
            ADD CONSTRAINT sec_registration_events_form_v2_check CHECK (form IN (
                '15-12B', '15-12G', '15-15D', '15F-12B', '15F-12G', '15F-15D', '25', '25-NSE',
                '15-12B/A', '15-12G/A', '15-15D/A', '15F-12B/A', '15F-12G/A', '15F-15D/A',
                '25/A', '25-NSE/A',
                '8-A12B', '8-A12G', '10-12B', '10-12G',
                '8-A12B/A', '8-A12G/A', '10-12B/A', '10-12G/A',
                '8-K12B', '8-K12G3', '8-K12B/A', '8-K12G3/A'
            )) NOT VALID;
    END IF;
END $$;
DO $$
DECLARE
    item text;
BEGIN
    FOREACH item IN ARRAY ARRAY['sec_ticker_cik_observations', 'sec_cover_share_counts',
                                'sec_registration_events'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                       WHERE conrelid = to_regclass(item)
                         AND conname = item || '_retired_reason_check') THEN
            EXECUTE format(
                'ALTER TABLE %I ADD CONSTRAINT %I CHECK (retired_reason IS NULL '
                'OR (retired_reason IN (''source'', ''parser_correction'') '
                'AND retired_on IS NOT NULL)) NOT VALID',
                item, item || '_retired_reason_check');
        END IF;
    END LOOP;
END $$;
COMMENT ON COLUMN sec_ticker_cik_observations.retired_reason IS
    'source (or NULL): the public record changed, visible before retired_on; '
    'parser_correction: our reading was wrong, visible at no date';

-- Visible rows. p_current = false: what was known at D (available_on <= D, not
-- yet retired at D by a change of the public record; a version a parser
-- correction retired is visible at no date). p_current = true: today's truth up
-- to D (current rows whose filing was public by D). Inlined into every caller.
CREATE OR REPLACE FUNCTION sec_observations_at(p_as_of date, p_current boolean)
RETURNS SETOF sec_ticker_cik_observations
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT o.* FROM sec_ticker_cik_observations o
WHERE CASE WHEN p_current
           THEN o.retired_on IS NULL AND o.source_available_on <= p_as_of
           ELSE o.available_on <= p_as_of
                AND (o.retired_on IS NULL
                     OR (o.retired_on > p_as_of
                         AND o.retired_reason IS DISTINCT FROM 'parser_correction'))
      END
$fn$;

CREATE OR REPLACE FUNCTION sec_share_counts_at(p_as_of date, p_current boolean)
RETURNS SETOF sec_cover_share_counts
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT c.* FROM sec_cover_share_counts c
WHERE CASE WHEN p_current
           THEN c.retired_on IS NULL AND c.source_available_on <= p_as_of
           ELSE c.available_on <= p_as_of
                AND (c.retired_on IS NULL
                     OR (c.retired_on > p_as_of
                         AND c.retired_reason IS DISTINCT FROM 'parser_correction'))
      END
$fn$;

DROP FUNCTION IF EXISTS sec_issuer_end_events(bigint, date, boolean);
DROP FUNCTION IF EXISTS sec_registration_end_events(bigint, date, boolean);

-- The end filings (15-12B/15-12G/15-15D, 15F-12B/15F-12G/15F-15D, 25/25-NSE) of
-- a CIK visible at D, as amended by D. An amendment applies to exactly one
-- original: the latest original of its form for the CIK filed on or before it
-- (then by accession), from the amendment's own knowledge date. A cancelling
-- amendment removes the original; a restating one replaces its class attributes
-- (the original's are returned as original_*, restated_on is the amendment's
-- knowledge date and restated_filed its filing date; class_description is the
-- class the end names as it reads now); an amendment that was not
-- read leaves the original in force. p_current (lineage: today's truth) applies
-- every current amendment, whenever it was filed.
CREATE FUNCTION sec_registration_end_events(
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
    original_venue_kind text,
    restated_filed date,
    class_description text,
    original_class_description text
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH visible AS (
    SELECT e.adsh, e.form, e.filed, e.class_kind, e.class_count, e.extinguished,
           e.venue_kind, e.amendment_effect, e.class_description,
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
                    AND (e.retired_on IS NULL
                         OR (e.retired_on > p_as_of
                             AND e.retired_reason IS DISTINCT FROM 'parser_correction'))
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
       v.class_kind, v.class_count, v.extinguished, v.venue_kind,
       CASE WHEN l.amendment_effect = 'restates' THEN l.filed END,
       CASE WHEN l.amendment_effect = 'restates' THEN l.class_description
            ELSE v.class_description END,
       v.class_description
FROM visible v
LEFT JOIN latest_amendment l ON l.original = v.adsh
WHERE v.form NOT LIKE '%/A'
  AND l.amendment_effect IS DISTINCT FROM 'cancels'
$fn$;

-- The registration filings (8-A12B/8-A12G/10-12B/10-12G originals, and a
-- successor's 8-K12B/8-K12G3) of a CIK visible at D that register an equity
-- class, or a class not read: a Form 8-A of notes, preferred or warrants is no
-- transfer of a delisted common stock and no relisting of it (Statera's 8-A12G of
-- its Series B Preferred Stock, filed the day Nasdaq delisted its common,
-- 2023-02-01). Forms 8-A of CIKs with cover data are read (class_kind); a Form 10
-- or 8-K12B is not.
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
  AND e.form IN ('8-A12B', '8-A12G', '10-12B', '10-12G', '8-K12B', '8-K12G3')
  AND e.class_kind IS DISTINCT FROM 'other'
  AND CASE WHEN p_current
           THEN e.retired_on IS NULL AND e.source_available_on <= p_as_of
           ELSE e.available_on <= p_as_of
                AND (e.retired_on IS NULL
                     OR (e.retired_on > p_as_of
                         AND e.retired_reason IS DISTINCT FROM 'parser_correction'))
      END
$fn$;

-- The end filings of a CIK that end its equity lines at D, and whether each is
-- DEFINITIVE. Judged against the CIK's latest filing with an equity line filed
-- before the event (prior symbols: its equity symbols when the end names an
-- equity class, else all its symbols) and its latest complete filing filed before
-- it (prior classes: its equity classes, listed or not), among the filings
-- visible at D: filing dates place them, knowledge dates only gate what is
-- visible (an end re-derived years later is still judged against the covers
-- before its filing).
-- A Form 15F (a foreign private issuer's termination under Rule 12h-6) counts as
-- the Form 15 it stands for: 15F-12B as 15-12B, 15F-12G as 15-12G, 15F-15D as
-- 15-15D.
-- An end of an equity class that names its classes by letter ("Class B common
-- stock", "Class A and B", "Series A ... Common Stock"), when every listed class
-- of the prior cover has a letter (its 12(b) title, else its member:
-- CommonClassB, ClassBCommonStock, CapitalClassC), applies to the listed classes
-- it names only (class_keys; NULL when it names all of them) and to none when it
-- names none of them (a 15-12G of the unlisted class B). Such an end of some of
-- the classes is definitive only when its 25-NSE says they were extinguished.
-- Any other end APPLIES to every line of the CIK unless
-- * the filing concerns another class (class_kind 'other'), or
-- * it may concern a class other than the listed symbol: the issuer then showed
--   several symbols and the filing names fewer classes than it showed symbols
--   (a 15-12G or 15-15D for one class of a multi-class issuer, too), or
-- * it is a 15-12G or 15-15D naming an equity class, which may be a class that is
--   not listed, and names fewer equity classes than the issuer's latest complete
--   filing showed, listed or not (listed class A, unlisted class B, a 15-12G for
--   one class: B's), or
-- * a successor registered the CIK's class (8-K12B or 8-K12G3 under the same CIK,
--   filed from 30 days before to 10 days after it: a holding-company
--   reorganization that keeps the CIK, such as KKR's of 2022, whose share count
--   rose 45%), or
-- * it is a 25/25-NSE/15-12B (12(b) removal) on a secondary exchange, or one
--   that a registration of an equity class (or of a class not read) filed from 30
--   days before to 10 days after makes a transfer (PepsiCo's NYSE -> Nasdaq move:
--   Form 25 and 8-A12B of its common stock the same day), unless the class was
--   extinguished.
-- An applying end of an equity class (class_kind 'equity', naming at least as
-- many classes as the issuer has) is DEFINITIVE when the 25-NSE says the class
-- was extinguished (12d2-2(a)), or when an applying delisting (25/25-NSE, not
-- secondary, not a transfer) naming every listed class (as many classes as the
-- issuer showed equity symbols) and an applying termination (15-12B/15-12G/
-- 15-15D or 15F) naming every equity class are both on file within 120 days of
-- each other, unless the shareholder base continued: the first cover share
-- count filed after the end and stated on or after it is within 0.8-1.25 times
-- the last one filed before it (a holding-company reorganization or REIT
-- conversion that keeps the CIK: United Fire 2012, Ulta and SBA 2017; American
-- Greetings reported 100 shares after its merger; a 10-Q filed after the end
-- that states a count from before it proves nothing). Other applying ends
-- (including class_kind 'unknown' or unread) end the lines until a later
-- statement shows the symbol again (an exchange delisting to OTC, a stale 12(g)
-- registration).
-- class_kind: what the end names as it applies; named_kinds: the other
-- instruments its description also names (warrant, unit, right, preferred,
-- debt). An end of an equity class ends the listed lines, and a preferred,
-- warrant, unit, right or notes line only when it names that instrument: the
-- 25-NSE of a common stock taken private does not end its preferred or notes,
-- which may stay listed (Triton's and Brookfield Property's preferreds did), while
-- a SPAC's "Units; Class A common stock; Warrants" ends all three.
-- effective_on: the date the end takes effect, its filing date + 1; a restated
-- end that applies only as restated takes effect point-in-time from the
-- amendment's filing date + 1. available_on: the date the end, as it applies,
-- became known (the amendment's knowledge date for one that applies only as
-- restated); it is the visibility gate (available_on <= D), never the effect.
-- A restated end that no longer applies as restated is withdrawn from the
-- amendment's knowledge date. p_current (lineage): ends filed by D, judged with
-- every current filing (a transfer registration, a paired Form 15 or an
-- amendment filed after D still counts), each at its original's date.
CREATE FUNCTION sec_issuer_end_events(
    p_cik bigint, p_as_of date, p_current boolean DEFAULT false
)
RETURNS TABLE (
    available_on date,
    filed date,
    form text,
    adsh text,
    definitive boolean,
    effective_on date,
    class_keys text[],
    class_kind text,
    named_kinds text[]
)
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
), listed_classes AS (
    -- each cover's listed classes and the class each names: the identifier of
    -- its 12(b) title ("Class B common stock", "Class B-2 Common Stock"), else of
    -- its member (CommonClassB, ClassBCommonStock, CapitalClassC, ClassB2...)
    SELECT o.adsh, o.class_key,
           max(lower(replace(COALESCE(
               substring(o.security_title
                         from '(?i)\m(?:class|series)\s+([a-z0-9]{1,2}(?:-[a-z0-9]{1,2})?)\M'),
               substring(o.class_key from '(?:Class|Series)([A-Z][0-9]?|[0-9]{1,2})(?![a-z])')),
               '-', ''))) AS label
    FROM horizon h
    CROSS JOIN LATERAL sec_observations_at(h.on_date, p_current) o
    WHERE o.cik = p_cik AND o.security_kind IN ('equity', 'depositary', 'unknown')
    GROUP BY o.adsh, o.class_key
), totals AS (
    -- each filing's cover share count (its issuer total, else its class counts)
    -- and the date the cover states it as of
    SELECT c.adsh,
           max(c.source_available_on) AS source_on,
           max(c.accepted) AS accepted,
           max(c.stated_on) AS stated_on,
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
           ) AS registered_nearby,
           -- a successor registered this CIK's class (8-K12B/8-K12G3 under the
           -- same CIK): its line continues under the successor
           EXISTS (
               SELECT 1 FROM horizon h
               CROSS JOIN LATERAL sec_registration_starts(p_cik, h.on_date, p_current) r
               WHERE r.form IN ('8-K12B', '8-K12G3')
                 AND r.filed BETWEEN e.filed - 30 AND e.filed + 10
           ) AS succeeded
    FROM horizon h
    CROSS JOIN LATERAL sec_registration_end_events(p_cik, h.on_date, p_current) e
), versions AS (
    -- each end as it reads now ('effective') and, when restated, as filed
    SELECT e.adsh, e.form, e.filed, e.available_on, e.restated_on, e.restated_filed,
           e.registered_nearby, e.succeeded, true AS effective, e.class_kind, e.class_count,
           e.extinguished, e.venue_kind, e.class_description
    FROM events e
    UNION ALL
    SELECT e.adsh, e.form, e.filed, e.available_on, e.restated_on, e.restated_filed,
           e.registered_nearby, e.succeeded, false, e.original_class_kind,
           e.original_class_count, e.original_extinguished, e.original_venue_kind,
           e.original_class_description
    FROM events e
    WHERE e.restated_on IS NOT NULL
), judged AS (
    SELECT v.*,
           CASE WHEN v.class_kind = 'equity' THEN pf.equity_symbols ELSE pf.symbols END
               AS prior_symbols,
           GREATEST(COALESCE((
               SELECT f.classes FROM filings f WHERE f.complete AND f.source_on < v.filed + 1
               ORDER BY f.source_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
               LIMIT 1), 1), 1) AS prior_classes,
           v.registered_nearby AND NOT COALESCE(v.extinguished, false) AS transfer,
           -- the classes the end names by letter ("Class B common stock", "Class
           -- A and B", "Series A ... Common Stock"), and the prior cover's listed
           -- classes that are those classes
           v.class_kind = 'equity' AND cardinality(named.labels) > 0
               AND pc.listed > 0 AND pc.labelled AS scoped,
           pc.listed,
           COALESCE(ARRAY(SELECT c.class_key FROM listed_classes c
                          WHERE c.adsh = pf.adsh AND c.label = ANY(named.labels)
                          ORDER BY c.class_key), '{}'::text[]) AS matched
    FROM versions v
    LEFT JOIN LATERAL (
        SELECT f.* FROM filings f WHERE f.source_on < v.filed + 1
        ORDER BY f.source_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
        LIMIT 1
    ) pf ON true
    CROSS JOIN LATERAL (
        SELECT ARRAY(
            SELECT DISTINCT lower(replace(l.label, '-', ''))
            FROM regexp_matches(
                COALESCE(v.class_description, ''),
                '(?i)\mclass(?:es)?\s+([a-z0-9]{1,2}(?:-[a-z0-9]{1,2})?'
                '(?:\s*(?:,|/|&|\mand\M|\mor\M)\s*(?:class\s+)?[a-z0-9]{1,2}(?:-[a-z0-9]{1,2})?)*)\M'
                '|\mseries\s+([a-z0-9]{1,2})\s+(?:(?:non-?)?voting\s+)?(?:common|ordinary|capital)\M',
                'g') AS m(groups)
            CROSS JOIN LATERAL regexp_split_to_table(
                COALESCE(m.groups[1], m.groups[2]),
                '(?i)\s*(?:,|/|&|\mand\M|\mor\M|\mclass\M)\s*') AS l(label)
            WHERE l.label <> '') AS labels
    ) named
    CROSS JOIN LATERAL (
        SELECT count(*) AS listed, bool_and(c.label IS NOT NULL) AS labelled
        FROM listed_classes c WHERE c.adsh = pf.adsh
    ) pc
), applies AS (
    SELECT j.*,
           COALESCE(j.class_kind, 'unknown') <> 'other'
           AND NOT j.succeeded
           AND CASE WHEN j.scoped THEN cardinality(j.matched) > 0
                    ELSE (j.prior_symbols = 1
                          OR (j.class_kind = 'equity' AND j.class_count >= j.prior_symbols))
                         AND NOT COALESCE(j.class_kind = 'equity'
                                          AND replace(j.form, '15F-', '15-') IN ('15-12G', '15-15D')
                                          AND j.class_count < j.prior_classes, false)
               END
           AND (replace(j.form, '15F-', '15-') IN ('15-12G', '15-15D')
                OR (NOT j.transfer AND j.venue_kind IS DISTINCT FROM 'secondary'))
               AS applying,
           CASE WHEN j.scoped AND cardinality(j.matched) < j.listed THEN j.matched END
               AS class_keys,
           j.class_kind = 'equity' AND j.class_count >= j.prior_classes AS whole_equity,
           j.form IN ('25', '25-NSE')
               AND j.class_kind = 'equity'
               AND j.venue_kind IS DISTINCT FROM 'secondary'
               AND NOT j.transfer AS equity_delisting
    FROM judged j
), current_ends AS (
    SELECT a.*,
           a.restated_on IS NOT NULL AND NOT p_current
               AND NOT COALESCE((SELECT o.applying FROM applies o
                                 WHERE o.adsh = a.adsh AND NOT o.effective), false)
               AS restated_only
    FROM applies a
    WHERE a.effective AND a.applying
)
SELECT CASE WHEN a.restated_only THEN a.restated_on ELSE a.available_on END,
       a.filed, a.form, a.adsh,
       CASE
           -- an end of some of the listed classes: definitive for them only when
           -- the 25-NSE says they were extinguished
           WHEN a.class_keys IS NOT NULL
               THEN a.form = '25-NSE' AND COALESCE(a.extinguished, false)
           ELSE a.whole_equity AND NOT COALESCE((
               SELECT after_end.total BETWEEN 0.8 * before_end.total AND 1.25 * before_end.total
               FROM (SELECT t.total FROM totals t WHERE t.source_on < a.filed + 1
                     ORDER BY t.source_on DESC, t.accepted DESC NULLS LAST, t.adsh DESC
                     LIMIT 1) before_end,
                    (SELECT t.total FROM totals t
                     WHERE t.source_on >= a.filed + 1 AND t.stated_on >= a.filed
                     ORDER BY t.source_on, t.accepted NULLS FIRST, t.adsh
                     LIMIT 1) after_end), false)
           AND (
               (a.form = '25-NSE' AND COALESCE(a.extinguished, false))
               OR (replace(a.form, '15F-', '15-') IN ('15-12B', '15-12G', '15-15D') AND EXISTS (
                   SELECT 1 FROM applies d
                   WHERE d.effective AND d.applying AND d.equity_delisting
                     AND d.class_keys IS NULL AND d.class_count >= d.prior_symbols
                     AND d.filed BETWEEN a.filed - 120 AND a.filed + 120))
               OR (a.equity_delisting AND a.class_count >= a.prior_symbols AND EXISTS (
                   SELECT 1 FROM applies t
                   WHERE t.effective AND t.applying AND t.class_keys IS NULL
                     AND replace(t.form, '15F-', '15-') IN ('15-12B', '15-12G', '15-15D')
                     AND t.class_kind = 'equity' AND t.class_count >= t.prior_classes
                     AND t.filed BETWEEN a.filed - 120 AND a.filed + 120))
           )
       END AS definitive,
       CASE WHEN a.restated_only THEN a.restated_filed + 1 ELSE a.filed + 1 END,
       a.class_keys, a.class_kind,
       ARRAY(SELECT n.kind
             FROM (VALUES ('warrant', '\mwarrants?\M'), ('unit', '\munits?\M'),
                          ('right', '\mrights?\M'), ('preferred', 'preferred|preference'),
                          ('debt', '\mnotes?\M|debentures?|\mbonds?\M')) n(kind, pattern)
             WHERE a.class_description ~* n.pattern
             ORDER BY 1)
FROM current_ends a
WHERE a.available_on <= p_as_of
$fn$;

-- Each CIK's hold of a ticker at D (internal helper; one row per CIK that showed
-- the ticker by D). Listed rows (equity, depositary or unknown) decide: a
-- non-listed row showing the ticker (filers also tag their common symbol on
-- notes lines) counts only while no listed row showing it was known at or before
-- it, so a ticker only ever shown on preferred or notes lines resolves through
-- them, and an earlier holder that tagged it only on such lines keeps them when a
-- later issuer lists it. A hold is followed through its candidate statements:
--   (a) every filing that tags a class that showed the ticker,
--   (b) when the latest complete filing showing the ticker listed one equity
--       class: every complete filing (the issuer's sole security, however its
--       member is called, until a complete filing says otherwise), and
--   (c) every complete filing that shows one equity class in total (classes
--       that merged into one: the old symbols are gone unless it shows them).
-- Ends come from sec_issuer_end_events, placed at their effective date (an end
-- known only later still takes effect at its filing); an end that names some of
-- the CIK's classes (class_keys) applies only to a hold of those classes. A
-- candidate filed after an end does not count when
--   * the end is DEFINITIVE and the ticker was shown before it (American
--     Greetings tagged AM on 10-Qs for three years after its 2013 merger
--     delisting and Form 15), or
--   * another CIK first showed the ticker from 30 days before the end to this
--     CIK's first statement after it (the symbol moved: Google Inc's 10-Q of
--     2015-10-29 still tagged GOOG after its Form 15s, the day Alphabet's first
--     10-Q did),
-- unless a registration of the class (8-A12B/8-A12G/10-12B/10-12G) filed after
-- the end was public by then: a later cover, 12(b) title or not, never reopens
-- it by itself. A symbol the CIK first showed after a definitive end is a new
-- line (Swift's SWFT ended in the merger; the same CIK traded as KNX).
-- The statement is the latest counting candidate. The hold has ended when the
-- latest applying end takes effect after the statement (end_reason: its form) or
-- the statement does not show the ticker ('other_symbol'); an open hold whose
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
                      WHERE e.security_kind IN ('equity', 'depositary', 'unknown')
                        AND e.known_on <= s.known_on)
), per_cik AS (
    SELECT r.cik,
           min(r.known_on) AS first_on,
           max(r.known_on) AS confirmed_on,
           (array_agg(r.class_key ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh DESC, r.class_key))[1] AS class_key,
           (array_agg(r.security_kind ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh DESC, r.class_key))[1] AS security_kind,
           array_agg(DISTINCT r.class_key) AS classes,
           bool_or(r.security_kind IN ('equity', 'depositary', 'unknown')) AS listed,
           array_agg(DISTINCT r.security_kind) AS kinds,
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
               AS shows
    FROM per_cik p
    JOIN sec_observations_at(p_as_of, p_current) f
      ON f.cik = p.cik
     AND (f.class_key = ANY(p.classes)
          OR (f.filing_complete AND f.security_kind IN ('equity', 'depositary', 'unknown')
              AND (p.sole OR f.filing_equity_classes = 1)))
    GROUP BY p.cik, f.adsh
), ends AS (
    -- the ends of the classes that showed the ticker (an end naming other
    -- classes of the CIK does not apply to this hold)
    SELECT p.cik, e.effective_on, e.filed, e.form, e.adsh, e.definitive,
           (SELECT min(c.known_on) FROM candidates c
            WHERE c.cik = p.cik AND c.shows AND c.known_on >= e.effective_on) AS first_post_on
    FROM per_cik p
    CROSS JOIN LATERAL sec_issuer_end_events(p.cik, p_as_of, p_current) e
    WHERE (e.class_keys IS NULL OR e.class_keys && p.classes)
      AND (e.class_kind IS DISTINCT FROM 'equity' OR p.listed OR e.named_kinds && p.kinds)
), last_end AS (
    SELECT DISTINCT ON (e.cik) e.*
    FROM ends e
    ORDER BY e.cik, e.effective_on DESC, e.adsh DESC
), statement AS (
    SELECT DISTINCT ON (c.cik) c.*
    FROM candidates c
    JOIN per_cik p ON p.cik = c.cik
    WHERE NOT EXISTS (
        SELECT 1 FROM ends e
        WHERE e.cik = c.cik AND e.effective_on <= c.known_on
          AND ((e.definitive AND p.first_on < e.effective_on)
               OR EXISTS (
                   SELECT 1 FROM per_cik o
                   WHERE o.cik <> c.cik
                     AND o.first_on BETWEEN e.effective_on - 30 AND e.first_post_on))
          AND NOT EXISTS (
              SELECT 1 FROM sec_registration_starts(c.cik, p_as_of, p_current) r
              WHERE r.filed > e.filed AND r.available_on <= c.known_on))
    ORDER BY c.cik, c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh DESC
)
SELECT p.cik,
       CASE
           WHEN l.effective_on > s.known_on THEN 'ended'
           WHEN NOT s.shows THEN 'ended'
           WHEN p_as_of - s.known_on > p_max_age_days THEN 'stale'
           ELSE 'active'
       END AS state,
       p.class_key, p.security_kind, s.known_on AS statement_on,
       s.accepted AS statement_accepted, s.adsh AS statement_adsh,
       p.first_on, p.confirmed_on,
       CASE
           WHEN l.effective_on > s.known_on THEN l.form
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
-- class. Ends as in sec_ticker_holds, at their effective date and only those of
-- the class (or of every class): a filing after a definitive end counts only
-- with a registration after the end, or symbols the CIK first showed after it. class_key and security_kind
-- come from one row of the statement (the caller's class first). status:
-- 'resolved' | 'stale' | 'ended' | 'missing' | 'ambiguous_class' (the class is
-- not known at D and the issuer then listed several equity classes).
-- equity_lines: equity classes in the issuer's latest complete filing by D.
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
    WHERE (e.class_keys IS NULL OR p_class_key = ANY(e.class_keys)
           OR e.class_keys && ARRAY(SELECT DISTINCT r.class_key FROM rows r))
      AND (e.class_kind IS DISTINCT FROM 'equity'
           OR EXISTS (SELECT 1 FROM rows r
                      WHERE r.security_kind IN ('equity', 'depositary', 'unknown')
                         OR r.security_kind = ANY(e.named_kinds)))
), last_end AS (
    SELECT e.* FROM ends e ORDER BY e.effective_on DESC, e.adsh DESC LIMIT 1
), candidates AS (
    SELECT r.adsh, max(r.available_on) AS known_on, max(r.accepted) AS accepted,
           array_agg(DISTINCT r.ticker_key) AS keys
    FROM rows r
    GROUP BY r.adsh
), chosen AS (
    SELECT c.* FROM candidates c
    WHERE NOT EXISTS (
        SELECT 1 FROM ends d
        WHERE d.definitive AND d.effective_on <= c.known_on
          AND EXISTS (
              SELECT 1 FROM sec_observations_at(p_as_of, false) x
              WHERE x.cik = p_cik AND x.ticker_key = ANY(c.keys)
                AND x.available_on < d.effective_on)
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
           EXISTS (SELECT 1 FROM last_end l WHERE l.effective_on > c.known_on) AS ended
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
-- An undimensioned class shown only on one-class covers and a dimensioned class
-- shown only beside another listed class are not linked by a symbol they share:
-- the symbol moved in a recapitalization (Google's GOOG, its sole class until
-- 2014, then its class C beside class A's GOOGL), so it does not say which of the
-- listed classes continues the old one; only a relabel edge would. A class listed
-- alone beside unlisted ones (a filer that starts dimensioning its one listed
-- class) stays linked.
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
        ), filing_lines AS (
            SELECT e.adsh, count(DISTINCT e.class_key) AS listed
            FROM equity_rows e
            GROUP BY e.adsh
        ), structure AS (
            SELECT e.class_key, bool_and(e.filing_equity_classes = 1) AS sole_only,
                   bool_and(f.listed > 1) AS beside_listed
            FROM equity_rows e
            JOIN filing_lines f ON f.adsh = e.adsh
            GROUP BY e.class_key
        ), shared AS (
            SELECT x.class_key AS a, y.class_key AS b,
                   min(GREATEST(x.first_on, y.first_on)) AS on_date,
                   sum(x.filings + y.filings) AS evidence
            FROM shown x
            JOIN shown y ON y.ticker_key = x.ticker_key AND y.class_key > x.class_key
            JOIN structure sx ON sx.class_key = x.class_key
            JOIN structure sy ON sy.class_key = y.class_key
            WHERE NOT (x.class_key = '' AND sx.sole_only AND sy.beside_listed)
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
-- equity class in total) with the end (per line: an end naming some classes
-- ends only their lines), definitive-end, successor and listed-row rules of
-- sec_ticker_holds, evaluated once per change date (a filing, an end,
-- or a statement reaching p_max_age_days). One row per RUN in which the line held
-- the ticker: [valid_from, valid_to), valid_to NULL while open; end_reason
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
                      WHERE e.security_kind IN ('equity', 'depositary', 'unknown')
                        AND e.known_on <= s.known_on)
), holder_ciks AS (
    SELECT DISTINCT r.cik FROM relevant r
), lines AS MATERIALIZED (
    SELECT h.cik, l.class_key, l.line_key
    FROM holder_ciks h CROSS JOIN LATERAL sec_issuer_lines(h.cik) l
), held AS (
    SELECT r.cik, l.line_key, min(r.known_on) AS first_on,
           bool_or(r.security_kind IN ('equity', 'depositary', 'unknown')) AS listed,
           array_agg(DISTINCT r.security_kind) AS kinds
    FROM relevant r JOIN lines l ON l.cik = r.cik AND l.class_key = r.class_key
    GROUP BY r.cik, l.line_key
), cik_first AS (
    SELECT r.cik, min(r.known_on) AS first_on FROM relevant r GROUP BY r.cik
), sole_lines AS (
    -- a line whose complete filings all listed one equity class is the issuer's
    -- sole security: every later complete filing of the CIK is one of its
    -- candidates (rule (b) of sec_ticker_holds), so a recapitalization that lists
    -- the ticker on another class ends its run
    SELECT h.cik, h.line_key, max(o.source_available_on) AS sole_until
    FROM held h
    JOIN lines l ON l.cik = h.cik AND l.line_key = h.line_key
    JOIN sec_observations_at('infinity'::date, true) o
      ON o.cik = l.cik AND o.class_key = l.class_key
    WHERE o.filing_complete AND o.security_kind IN ('equity', 'depositary', 'unknown')
    GROUP BY h.cik, h.line_key
    HAVING bool_and(o.filing_equity_classes = 1)
), candidates AS MATERIALIZED (
    SELECT h.cik, h.line_key, o.adsh,
           max(o.source_available_on) AS known_on,
           max(o.accepted) AS accepted,
           bool_or(l.line_key = h.line_key AND o.ticker_key = key.k) AS shows,
           (array_agg(o.class_key ORDER BY o.class_key)
               FILTER (WHERE l.line_key = h.line_key AND o.ticker_key = key.k))[1] AS class_key
    FROM held h
    CROSS JOIN key
    JOIN sec_observations_at('infinity'::date, true) o ON o.cik = h.cik
    LEFT JOIN lines l ON l.cik = o.cik AND l.class_key = o.class_key
    WHERE l.line_key = h.line_key
       OR (o.filing_complete AND o.filing_equity_classes = 1
           AND o.security_kind IN ('equity', 'depositary', 'unknown'))
       OR (o.filing_complete AND o.security_kind IN ('equity', 'depositary', 'unknown')
           AND EXISTS (SELECT 1 FROM sole_lines s
                       WHERE s.cik = h.cik AND s.line_key = h.line_key
                         AND o.source_available_on > s.sole_until))
    GROUP BY h.cik, h.line_key, o.adsh
), ends AS MATERIALIZED (
    SELECT h.cik, e.effective_on, e.filed, e.form, e.adsh, e.definitive, e.class_keys,
           e.class_kind, e.named_kinds
    FROM holder_ciks h
    CROSS JOIN LATERAL sec_issuer_end_events(h.cik, 'infinity'::date, true) e
), starts AS MATERIALIZED (
    SELECT h.cik, r.filed, r.available_on
    FROM holder_ciks h
    CROSS JOIN LATERAL sec_registration_starts(h.cik, 'infinity'::date, true) r
), line_ends AS MATERIALIZED (
    -- the ends of each line: those of every class of the CIK, and those naming
    -- one of the line's classes
    SELECT h.cik, h.line_key, e.effective_on, e.filed, e.form, e.adsh, e.definitive
    FROM held h
    JOIN ends e ON e.cik = h.cik
    WHERE (e.class_keys IS NULL OR EXISTS (
               SELECT 1 FROM lines l
               WHERE l.cik = h.cik AND l.line_key = h.line_key
                 AND l.class_key = ANY(e.class_keys)))
      AND (e.class_kind IS DISTINCT FROM 'equity' OR h.listed OR e.named_kinds && h.kinds)
), blocking AS (
    -- ends after which the line's later candidates do not count (definitive end
    -- of a ticker shown before it, or the ticker moved to another CIK)
    SELECT h.cik, h.line_key, e.effective_on, e.filed
    FROM held h
    JOIN line_ends e ON e.cik = h.cik AND e.line_key = h.line_key
    WHERE (e.definitive AND h.first_on < e.effective_on)
       OR EXISTS (
           SELECT 1 FROM cik_first o
           WHERE o.cik <> h.cik
             AND o.first_on BETWEEN e.effective_on - 30 AND (
                 SELECT min(c.known_on) FROM candidates c
                 WHERE c.cik = h.cik AND c.line_key = h.line_key AND c.shows
                   AND c.known_on >= e.effective_on))
), counted AS MATERIALIZED (
    SELECT c.* FROM candidates c
    WHERE NOT EXISTS (
        SELECT 1 FROM blocking b
        WHERE b.cik = c.cik AND b.line_key = c.line_key AND b.effective_on <= c.known_on
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
        SELECT e.effective_on FROM line_ends e
        WHERE e.cik = h.cik AND e.line_key = h.line_key
    ) x
), states AS (
    SELECT b.cik, b.line_key, b.on_date, s.known_on AS statement_on, s.shows, s.class_key,
           le.effective_on AS end_on, le.form AS end_form,
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
        SELECT e.effective_on, e.form FROM line_ends e
        WHERE e.cik = b.cik AND e.line_key = b.line_key AND e.effective_on <= b.on_date
        ORDER BY e.effective_on DESC, e.adsh DESC
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
-- end rules of sec_ticker_holds (ends at their effective date, those of the
-- line's classes or of every class): after a definitive end a filing counts only
-- with a registration after the end, or symbols the CIK first showed after it. Alive at a date: its
-- latest counting filing has a row of the line, no applying end takes effect
-- after it, and (p_max_age_days not NULL) it is at most p_max_age_days old. One
-- row per run, [valid_from, valid_to); end_reason the end form, 'merged' (a
-- one-class filing of another line), or 'stale'.
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
), sole AS (
    -- the line is the issuer's sole security until its last one-class complete
    -- filing when all its complete filings listed one class (see sec_ticker_line_runs)
    SELECT max(o.source_available_on) AS sole_until
    FROM sec_observations_at('infinity'::date, true) o
    JOIN lines l ON l.class_key = o.class_key
    WHERE o.cik = p_cik AND l.line_key = p_line_key AND o.filing_complete
      AND o.security_kind IN ('equity', 'depositary', 'unknown')
    HAVING bool_and(o.filing_equity_classes = 1)
), candidates AS MATERIALIZED (
    SELECT o.adsh,
           max(o.source_available_on) AS known_on,
           max(o.accepted) AS accepted,
           bool_or(l.line_key = p_line_key) AS has_line,
           array_agg(DISTINCT o.ticker_key) FILTER (WHERE l.line_key = p_line_key) AS keys,
           array_agg(DISTINCT o.ticker ORDER BY o.ticker)
               FILTER (WHERE l.line_key = p_line_key) AS tickers
    FROM sec_observations_at('infinity'::date, true) o
    LEFT JOIN lines l ON l.class_key = o.class_key
    WHERE o.cik = p_cik
      AND (l.line_key = p_line_key
           OR (o.filing_complete AND o.filing_equity_classes = 1
               AND o.security_kind IN ('equity', 'depositary', 'unknown'))
           OR (o.filing_complete AND o.security_kind IN ('equity', 'depositary', 'unknown')
               AND o.source_available_on > (SELECT s.sole_until FROM sole s)))
    GROUP BY o.adsh
), ends AS MATERIALIZED (
    SELECT e.* FROM sec_issuer_end_events(p_cik, 'infinity'::date, true) e
    WHERE (e.class_keys IS NULL OR EXISTS (
               SELECT 1 FROM lines l
               WHERE l.line_key = p_line_key AND l.class_key = ANY(e.class_keys)))
      AND (e.class_kind IS DISTINCT FROM 'equity' OR EXISTS (
               SELECT 1 FROM sec_observations_at('infinity'::date, true) o
               JOIN lines l ON l.class_key = o.class_key
               WHERE o.cik = p_cik AND l.line_key = p_line_key
                 AND (o.security_kind IN ('equity', 'depositary', 'unknown')
                      OR o.security_kind = ANY(e.named_kinds))))
), counted AS MATERIALIZED (
    SELECT c.* FROM candidates c
    WHERE NOT EXISTS (
        SELECT 1 FROM ends d
        WHERE d.definitive AND d.effective_on <= c.known_on
          AND EXISTS (
              SELECT 1 FROM candidates x
              WHERE x.has_line AND x.known_on < d.effective_on AND x.keys && c.keys)
          AND NOT EXISTS (
              SELECT 1 FROM sec_registration_starts(p_cik, 'infinity'::date, true) r
              WHERE r.filed > d.filed AND r.available_on <= c.known_on))
), bounds AS (
    SELECT c.known_on AS on_date FROM counted c
    UNION
    SELECT c.known_on + p_max_age_days + 1 FROM counted c
    WHERE c.has_line AND p_max_age_days IS NOT NULL
    UNION
    SELECT e.effective_on FROM ends e
), states AS (
    SELECT b.on_date, s.known_on AS statement_on, s.has_line, s.tickers,
           le.effective_on AS end_on, le.form AS end_form
    FROM bounds b
    LEFT JOIN LATERAL (
        SELECT c.known_on, c.has_line, c.tickers FROM counted c
        WHERE c.known_on <= b.on_date
        ORDER BY c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh DESC
        LIMIT 1
    ) s ON true
    LEFT JOIN LATERAL (
        SELECT e.effective_on, e.form FROM ends e WHERE e.effective_on <= b.on_date
        ORDER BY e.effective_on DESC, e.adsh DESC
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

COMMENT ON FUNCTION sec_issuer_end_events(bigint, date, boolean) IS
    'End events of a CIK at D: effective_on (takes effect), available_on (known); '
    'schema v2, see schemas/sec_ticker_cik_history_v2.sql.';

-- Ownership and grants of every routine (idempotent; the two functions created
-- again above start with default privileges).
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
