-- Restore the exact v2 routines (including inherited v1 routines).
-- No rows or schema columns are changed; parser-correction visibility remains v2.
BEGIN;
SET LOCAL lock_timeout = '5s';

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

CREATE OR REPLACE FUNCTION sec_label_text(p_text text)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
SELECT regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
       regexp_replace(regexp_replace(
           COALESCE(p_text, ''),
           '[A-Za-z]+=', ' ', 'g'),                                  -- dimension names
           '([A-Za-z])160(?=[A-Za-z0-9])', '\1 ', 'g'),              -- &#160;
           '(Class|Series|CLASS|SERIES)([a-z])(?![a-z])', '\1 \2', 'g'),  -- Classa
           '([a-z])([A-Z0-9])', '\1 \2', 'g'),                       -- camelCase, Series1
           '([A-Z])([A-Z][a-z])', '\1 \2', 'g'),                     -- IICommon
           '([0-9])([A-Z][a-z])', '\1 \2', 'g'),                     -- 2Common
           '((?:class|series)(?:es)?\s+[A-Z]{1,2})((?:com|ord|pref|vot|non|shar|stoc|unit|rede|subj|par)[a-z]*)',
           '\1 \2', 'gi')                                            -- Acommon
$fn$;

CREATE OR REPLACE FUNCTION sec_label_norm(p_id text)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
SELECT CASE
           WHEN p_id IS NULL THEN NULL
           -- two letters: capitals or a Roman numeral, never a word ("of", "OF")
           WHEN p_id ~ '^[A-Za-z]{2}$'
                AND (upper(p_id) IN ('OF', 'TO', 'IN', 'ON', 'AS', 'AN', 'OR', 'BY', 'NO', 'IS',
                                     'IT', 'AT', 'BE', 'DO', 'IF', 'SO', 'UP', 'WE', 'US', 'PA')
                     OR (p_id !~ '^[A-Z]{2}$' AND lower(p_id) !~ '^(ii|iv|vi|ix|xi|xv|xx)$'))
               THEN NULL
           -- a Roman numeral (I to XXXIX) is the number it writes: Class II is
           -- Class 2, so an end naming one form closes a class shown in the other
           WHEN lower(p_id) ~ '^x{0,3}(ix|iv|v?i{0,3})$' THEN
               (10 * (length(p_id) - length(ltrim(lower(p_id), 'x')))
                + CASE ltrim(lower(p_id), 'x')
                      WHEN 'ix' THEN 9
                      WHEN 'iv' THEN 4
                      ELSE (CASE WHEN ltrim(lower(p_id), 'x') LIKE 'v%' THEN 5 ELSE 0 END)
                           + length(replace(ltrim(lower(p_id), 'x'), 'v', ''))
                  END)::text
           ELSE lower(replace(p_id, '-', ''))
       END
$fn$;

CREATE OR REPLACE FUNCTION sec_label_id_re()
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
SELECT '(?:(?=[ivx]{2})x{0,3}(?:ix|iv|v?i{0,3})|[a-z]-?[0-9]{1,2}|[0-9]{1,4}(?:-?[a-z0-9]{1,2})?'
       || '|[a-z]{1,2})(?![a-z0-9])'
$fn$;

CREATE OR REPLACE FUNCTION sec_first_label(p_text text)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
SELECT sec_label_norm(x.m[1])
FROM regexp_matches(sec_label_text(p_text),
                    '(?:^|[^a-z])(?:class|series)\s+(' || sec_label_id_re() || ')',
                    'gi') WITH ORDINALITY AS x(m, n)
WHERE sec_label_norm(x.m[1]) IS NOT NULL
ORDER BY x.n
LIMIT 1
$fn$;

CREATE OR REPLACE FUNCTION sec_class_label(p_title text, p_class_key text)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
SELECT COALESCE(sec_first_label(p_title), sec_first_label(p_class_key))
$fn$;

CREATE OR REPLACE FUNCTION sec_named_classes(p_description text)
RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
WITH t AS (SELECT sec_label_text(p_description) AS text),
ids AS (
    SELECT m[1] AS kw, m[2] AS ids
    FROM t, regexp_matches(
        t.text,
        '(?:^|[^a-z])(class(?:es)?|series)\s+((?:' || sec_label_id_re() || ')'
        || '(?:\s*(?:,|/|&|\mand\M|\mor\M)\s*(?:(?:class|series)\s+)?(?:' || sec_label_id_re()
        || '))*)'
        || '((?:\s+(?!preferred|preference|notes?\M|debentures?|warrants?|rights?|units?\M|class|series)'
        || '[a-z][a-z0-9.,''-]*){0,5}?\s+(?:common|ordinary|capital)\M)?',
        'gi') AS m
    WHERE lower(m[1]) <> 'series' OR m[3] IS NOT NULL
)
SELECT ARRAY(
    SELECT DISTINCT sec_label_norm(l.id)
    FROM ids,
         regexp_split_to_table(ids.ids, '(?i)\s*(?:,|/|&|\mand\M|\mor\M|\mclass\M|\mseries\M)\s*')
             AS l(id)
    WHERE l.id <> '' AND sec_label_norm(l.id) IS NOT NULL
    ORDER BY 1)
$fn$;

CREATE OR REPLACE FUNCTION sec_end_role(
    p_class_keys text[], p_tentative_keys text[], p_class_kind text, p_named_kinds text[],
    p_class_key text, p_kind text
)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
SELECT CASE
           WHEN p_kind IN ('equity', 'depositary', 'unknown') THEN
               CASE
                   WHEN p_class_keys IS NULL THEN 'identified'
                   WHEN p_class_key = ANY(p_class_keys) THEN
                       CASE WHEN p_class_key = ANY(p_tentative_keys) THEN 'tentative'
                            ELSE 'identified' END
               END
           WHEN p_kind = ANY(p_named_kinds) THEN 'identified'
           WHEN p_class_kind IS DISTINCT FROM 'equity' THEN
               CASE WHEN cardinality(p_tentative_keys) > 0 THEN 'tentative'
                    ELSE 'identified' END
       END
$fn$;

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

CREATE OR REPLACE FUNCTION sec_registration_starts(
    p_cik bigint, p_as_of date, p_current boolean DEFAULT false
)
RETURNS TABLE (available_on date, filed date, form text, adsh text, classes text[])
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT CASE WHEN p_current THEN e.source_available_on ELSE e.available_on END,
       e.filed, e.form, e.adsh, sec_named_classes(e.class_description)
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

CREATE OR REPLACE FUNCTION sec_issuer_end_events(
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
    named_kinds text[],
    tentative_keys text[],
    issuer_symbols integer
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
), cover_rows AS (
    -- each cover's rows, and the class each names (sec_class_label: its 12(b)
    -- title's identifier, else its member's)
    SELECT o.adsh, o.class_key, o.ticker_key,
           o.security_kind IN ('equity', 'depositary', 'unknown') AS listed,
           sec_class_label(o.security_title, o.class_key) AS label
    FROM horizon h
    CROSS JOIN LATERAL sec_observations_at(h.on_date, p_current) o
    WHERE o.cik = p_cik
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
    SELECT e.*
    FROM horizon h
    CROSS JOIN LATERAL sec_registration_end_events(p_cik, h.on_date, p_current) e
), starts AS MATERIALIZED (
    -- the registrations visible at D (a successor's 8-K12B/8-K12G3 among them)
    SELECT g.* FROM horizon h
    CROSS JOIN LATERAL sec_registration_starts(p_cik, h.on_date, p_current) g
), versions AS (
    -- each end as it reads now ('effective') and, when restated, as filed
    SELECT e.adsh, e.form, e.filed, e.available_on, e.restated_on, e.restated_filed,
           true AS effective, e.class_kind, e.class_count,
           e.extinguished, e.venue_kind, e.class_description
    FROM events e
    UNION ALL
    SELECT e.adsh, e.form, e.filed, e.available_on, e.restated_on, e.restated_filed,
           false, e.original_class_kind,
           e.original_class_count, e.original_extinguished, e.original_venue_kind,
           e.original_class_description
    FROM events e
    WHERE e.restated_on IS NOT NULL
), judging AS (
    -- the complete cover each end is judged against: the latest one (a
    -- 10-K/10-Q-type cover lists every class) filed before it
    SELECT v.adsh, v.effective, pc.adsh AS cover_adsh, pc.source_on AS cover_on,
           pc.classes AS cover_classes
    FROM versions v
    LEFT JOIN LATERAL (
        SELECT f.* FROM filings f WHERE f.complete AND f.source_on < v.filed + 1
        ORDER BY f.source_on DESC, f.accepted DESC NULLS LAST, f.adsh DESC
        LIMIT 1
    ) pc ON true
), evidence AS (
    -- the issuer's classes when it filed the end: that cover's and those of every
    -- cover filed after it and before the end (an incomplete cover adds the
    -- classes it shows and drops none); without a complete cover, every cover's
    SELECT v.adsh, v.effective, r.class_key, r.ticker_key, r.listed, r.label
    FROM versions v
    JOIN judging j ON j.adsh = v.adsh AND j.effective = v.effective
    JOIN filings f ON f.source_on < v.filed + 1
                  AND (j.cover_adsh IS NULL OR f.adsh = j.cover_adsh OR f.source_on > j.cover_on)
    JOIN cover_rows r ON r.adsh = f.adsh
), classes AS (
    SELECT e.adsh, e.effective, e.class_key, max(e.label) AS label,
           count(DISTINCT e.label) AS labels
    FROM evidence e
    WHERE e.listed
    GROUP BY e.adsh, e.effective, e.class_key
), label_symbols AS (
    -- a label identifies a class only when one symbol carries it (tracking
    -- stocks of several groups each have a Series A)
    SELECT e.adsh, e.effective, e.label, count(DISTINCT e.ticker_key) AS symbols
    FROM evidence e
    WHERE e.listed AND e.label IS NOT NULL
    GROUP BY e.adsh, e.effective, e.label
), judged AS (
    SELECT v.*,
           sec_named_classes(v.class_description) AS named,
           COALESCE((SELECT CASE WHEN v.class_kind = 'equity'
                                 THEN count(DISTINCT e.ticker_key) FILTER (WHERE e.listed)
                                 ELSE count(DISTINCT e.ticker_key) END
                     FROM evidence e WHERE e.adsh = v.adsh AND e.effective = v.effective), 0)
               AS prior_symbols,
           GREATEST(COALESCE(j.cover_classes, 1),
                    (SELECT count(*) FROM classes c
                     WHERE c.adsh = v.adsh AND c.effective = v.effective), 1) AS prior_classes,
           -- a Form 15-12G or 15-15D (or 15F) ends a registration, which may be of an
           -- unlisted class; the other forms end listed classes
           replace(v.form, '15F-', '15-') IN ('15-12G', '15-15D') AS registration_end
    FROM versions v
    JOIN judging j ON j.adsh = v.adsh AND j.effective = v.effective
), roles AS (
    -- what an end says of each listed class (the admission rule: any ambiguity
    -- refuses). 'identified': it names the class by a label one symbol carries;
    -- or it names no class and counts all of them; or the issuer lists one
    -- symbol (and, for a 15-12G/15-15D, counts one class). 'excluded': it names
    -- other classes and this one's label is known. Else 'tentative': it may
    -- concern the class but does not say so.
    SELECT j.adsh, j.effective, c.class_key, c.label,
           CASE
               WHEN j.class_kind = 'equity' AND cardinality(j.named) > 0 THEN
                   CASE
                       WHEN c.label IS NOT NULL AND c.labels = 1 AND c.label = ANY(j.named)
                            AND ls.symbols = 1 THEN 'identified'
                       WHEN c.label IS NOT NULL AND c.labels = 1
                            AND NOT c.label = ANY(j.named) THEN 'excluded'
                       WHEN c.label IS NULL AND j.prior_symbols = 1
                            AND (NOT j.registration_end OR j.prior_classes = 1)
                           THEN 'identified'
                       ELSE 'tentative'
                   END
               WHEN j.class_kind = 'equity' THEN
                   CASE WHEN j.class_count >= j.prior_symbols
                             AND (NOT j.registration_end OR j.class_count >= j.prior_classes)
                        THEN 'identified' ELSE 'tentative' END
               ELSE
                   CASE WHEN j.prior_symbols <= 1
                             AND (NOT j.registration_end OR j.prior_classes = 1)
                        THEN 'identified' ELSE 'tentative' END
           END AS role
    FROM judged j
    JOIN classes c ON c.adsh = j.adsh AND c.effective = j.effective
    LEFT JOIN label_symbols ls
      ON ls.adsh = c.adsh AND ls.effective = c.effective AND ls.label = c.label
), closed AS (
    -- the classes an end closes: those it concerns that no registration carries
    -- on. A registration from 30 days before to 10 days after the end carries on
    -- the classes it names, or, naming none, the issuer's one symbol: a
    -- successor's registration of the CIK's class (8-K12B, 8-K12G3 under the same
    -- CIK) whatever the end, a transfer registration across a delisting unless the
    -- class was extinguished (a Form 15-12G or 15-15D ends the registration
    -- whatever is registered).
    SELECT r.*
    FROM roles r
    JOIN judged j ON j.adsh = r.adsh AND j.effective = r.effective
    WHERE r.role IN ('identified', 'tentative')
      AND NOT EXISTS (
          SELECT 1 FROM starts g
          WHERE g.filed BETWEEN j.filed - 30 AND j.filed + 10
            AND (g.form IN ('8-K12B', '8-K12G3')
                 OR NOT (j.registration_end OR COALESCE(j.extinguished, false)))
            AND ((r.label IS NOT NULL AND r.label = ANY(g.classes))
                 OR (cardinality(g.classes) = 0 AND j.prior_symbols = 1)))
), applies AS (
    SELECT j.*,
           COALESCE(j.class_kind, 'unknown') <> 'other'
           AND EXISTS (SELECT 1 FROM closed c WHERE c.adsh = j.adsh AND c.effective = j.effective)
           AND (j.registration_end OR j.venue_kind IS DISTINCT FROM 'secondary') AS applying,
           -- NULL: every listed class, each identified and closed
           CASE WHEN NOT EXISTS (
                         SELECT 1 FROM roles r
                         WHERE r.adsh = j.adsh AND r.effective = j.effective
                           AND NOT EXISTS (
                               SELECT 1 FROM closed c
                               WHERE c.adsh = r.adsh AND c.effective = r.effective
                                 AND c.class_key = r.class_key AND c.role = 'identified'))
                THEN NULL
                ELSE ARRAY(SELECT c.class_key FROM closed c
                           WHERE c.adsh = j.adsh AND c.effective = j.effective
                           ORDER BY c.class_key)
           END AS class_keys,
           ARRAY(SELECT c.class_key FROM closed c
                 WHERE c.adsh = j.adsh AND c.effective = j.effective AND c.role = 'tentative'
                 ORDER BY c.class_key) AS tentative_keys,
           j.class_kind = 'equity' AND j.class_count >= j.prior_classes AS whole_equity,
           j.form IN ('25', '25-NSE')
               AND j.class_kind = 'equity'
               AND j.venue_kind IS DISTINCT FROM 'secondary' AS equity_delisting
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
           -- an end that identifies no class is never definitive
           WHEN a.class_keys IS NOT NULL AND a.class_keys <@ a.tentative_keys THEN false
           -- an end of some of the listed classes: definitive for those it
           -- identifies only when the 25-NSE says they were extinguished
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
             -- rights attached to the class ("Common Stock and associated Preferred
             -- Stock Purchase Rights") name no instrument of their own
             WHERE regexp_replace(
                       a.class_description,
                       '(?i)(?:\m(?:associated|attached)\s+)?(?:\w+\s+){0,4}purchase\s+rights?\M',
                       ' ', 'g') ~* n.pattern
             ORDER BY 1),
       a.tentative_keys, a.prior_symbols::integer
FROM current_ends a
WHERE a.available_on <= p_as_of
$fn$;

CREATE OR REPLACE FUNCTION sec_ticker_listed_holds_at(
    p_ticker text, p_as_of date, p_max_age_days integer, p_current boolean
)
RETURNS TABLE (
    on_date date,
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
    SELECT o.cik, o.class_key, o.security_kind, o.adsh, o.accepted, o.security_title,
           CASE WHEN p_current THEN o.source_available_on ELSE o.available_on END AS known_on,
           o.filing_equity_classes, o.filing_complete
    FROM sec_observations_at(p_as_of, p_current) o
    WHERE o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
), relevant AS (
    -- pass 1: the listed rows alone (equity, depositary, unknown)
    SELECT s.* FROM shown s
    WHERE s.security_kind IN ('equity', 'depositary', 'unknown')
), per_cik AS (
    SELECT r.cik,
           min(r.known_on) AS first_on,
           max(r.known_on) AS confirmed_on,
           (array_agg(r.class_key ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh DESC, r.class_key))[1] AS class_key,
           (array_agg(r.security_kind ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh DESC, r.class_key))[1] AS security_kind,
           array_agg(DISTINCT r.class_key) AS classes,
           array_remove(array_agg(DISTINCT sec_class_label(r.security_title, r.class_key)),
                        NULL) AS labels,
           bool_or(r.security_kind IN ('equity', 'depositary', 'unknown')) AS listed,
           array_agg(DISTINCT r.security_kind) AS kinds,
           COALESCE((array_agg(r.filing_equity_classes = 1 ORDER BY r.known_on DESC,
                               r.accepted DESC NULLS LAST, r.adsh DESC)
                     FILTER (WHERE r.filing_complete))[1], false) AS sole
    FROM relevant r
    GROUP BY r.cik
), candidate_rows AS MATERIALIZED (
    SELECT p.cik, f.adsh, f.class_key, f.security_kind, f.accepted, f.filing_complete,
           f.filing_equity_classes, f.security_title,
           CASE WHEN p_current THEN f.source_available_on ELSE f.available_on END AS known_on,
           f.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g') AS shows
    FROM per_cik p
    JOIN sec_observations_at(p_as_of, p_current) f
      ON f.cik = p.cik
     AND (f.class_key = ANY(p.classes)
          OR (f.filing_complete AND f.security_kind IN ('equity', 'depositary', 'unknown')
              AND (p.sole OR f.filing_equity_classes = 1)))
), candidates AS (
    -- labels: the classes of the rows by which the candidate states the hold (its
    -- rows showing the ticker, else its rows): only a registration naming one of
    -- them, never another class the ticker once showed on, reopens it
    SELECT r.cik, r.adsh, max(r.known_on) AS known_on, max(r.accepted) AS accepted,
           bool_or(r.shows) AS shows,
           CASE WHEN bool_or(r.shows)
                THEN array_remove(array_agg(DISTINCT sec_class_label(r.security_title, r.class_key))
                                  FILTER (WHERE r.shows), NULL)
                ELSE array_remove(array_agg(DISTINCT sec_class_label(r.security_title, r.class_key)),
                                  NULL)
           END AS labels
    FROM candidate_rows r
    GROUP BY r.cik, r.adsh
), stated AS (
    -- a filing that does not show the ticker states the hold (ending it as
    -- 'other_symbol') when it is complete (a 10-K/10-Q-type cover lists every
    -- class: the ticker is no longer listed), or when it shows every class that
    -- showed the ticker in the latest filing before it that did: an 8-K showing
    -- one class under another symbol leaves the ticker to the others
    SELECT c.* FROM candidates c
    WHERE c.shows
       OR EXISTS (SELECT 1 FROM candidate_rows z
                  WHERE z.cik = c.cik AND z.adsh = c.adsh AND z.filing_complete)
       OR NOT EXISTS (
        SELECT 1 FROM candidate_rows t
        WHERE t.cik = c.cik AND t.shows
          AND t.adsh = (SELECT x.adsh FROM candidates x
                        WHERE x.cik = c.cik AND x.shows AND x.known_on <= c.known_on
                          AND x.adsh <> c.adsh
                        ORDER BY x.known_on DESC, x.accepted DESC NULLS LAST, x.adsh DESC
                        LIMIT 1)
          AND NOT EXISTS (SELECT 1 FROM candidate_rows y
                          WHERE y.cik = c.cik AND y.adsh = c.adsh AND y.class_key = t.class_key))
), ends AS MATERIALIZED (
    SELECT p.cik, e.effective_on, e.filed, e.form, e.adsh, e.definitive, e.class_keys,
           e.tentative_keys, e.class_kind, e.named_kinds, e.issuer_symbols,
           (SELECT min(c.known_on) FROM candidates c
            WHERE c.cik = p.cik AND c.shows AND c.known_on >= e.effective_on) AS first_post_on
    FROM per_cik p
    CROSS JOIN LATERAL sec_issuer_end_events(p.cik, p_as_of, p_current) e
), closes AS MATERIALIZED (
    -- each end against each statement, per class: whether it closes every row by
    -- which the statement states the hold (its rows showing the ticker, else its
    -- rows), and whether it identifies them all (else it closes them tentatively)
    SELECT e.cik, e.adsh AS end_adsh, c.adsh,
           bool_and(sec_end_role(e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds,
                                 r.class_key, r.security_kind) IS NOT NULL) AS closed,
           bool_and(sec_end_role(e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds,
                                 r.class_key, r.security_kind) = 'identified') AS identified
    FROM ends e
    JOIN candidates c ON c.cik = e.cik
    JOIN candidate_rows r ON r.cik = c.cik AND r.adsh = c.adsh AND (r.shows OR NOT c.shows)
    GROUP BY e.cik, e.adsh, c.adsh
), dates AS (
    -- the dates a non-listed row showed the ticker
    SELECT DISTINCT s.known_on AS on_date FROM shown s
    WHERE s.security_kind NOT IN ('equity', 'depositary', 'unknown')
), statement AS (
    -- at each date, the latest statement known by then that no end blocks
    SELECT DISTINCT ON (d.on_date, c.cik) d.on_date, c.*
    FROM dates d
    JOIN stated c ON c.known_on <= d.on_date
    JOIN per_cik p ON p.cik = c.cik
    WHERE NOT EXISTS (
        SELECT 1 FROM ends e
        JOIN closes k ON k.cik = e.cik AND k.end_adsh = e.adsh AND k.adsh = c.adsh
        WHERE e.cik = c.cik AND e.effective_on <= c.known_on AND k.closed
          AND ((e.definitive AND k.identified AND p.first_on < e.effective_on)
               OR EXISTS (
                   SELECT 1 FROM per_cik o
                   WHERE o.cik <> c.cik
                     AND o.first_on BETWEEN e.effective_on - 30 AND e.first_post_on))
          AND NOT EXISTS (
              -- a registration after the end that identifies the class relists it:
              -- one naming its class, or naming none when the issuer listed one
              -- symbol
              SELECT 1 FROM sec_registration_starts(c.cik, p_as_of, p_current) r
              WHERE r.filed > e.filed AND r.available_on <= c.known_on
                AND (r.classes && c.labels
                     OR (cardinality(r.classes) = 0 AND e.issuer_symbols = 1))))
    ORDER BY d.on_date, c.cik, c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh DESC
), last_end AS (
    -- the latest end, effective by the date, after the statement that closes every
    -- class it shows the ticker on (an end of another class leaves the hold)
    SELECT DISTINCT ON (s.on_date, e.cik) s.on_date, e.*
    FROM ends e
    JOIN statement s ON s.cik = e.cik
    JOIN closes k ON k.cik = e.cik AND k.end_adsh = e.adsh AND k.adsh = s.adsh
    WHERE e.effective_on > s.known_on AND e.effective_on <= s.on_date AND k.closed
    ORDER BY s.on_date, e.cik, e.effective_on DESC, k.identified DESC, e.adsh DESC
)
SELECT s.on_date, p.cik,
       CASE
           WHEN l.effective_on > s.known_on THEN 'ended'
           WHEN NOT s.shows THEN 'ended'
           WHEN s.on_date - s.known_on > p_max_age_days THEN 'stale'
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
LEFT JOIN last_end l ON l.cik = p.cik AND l.on_date = s.on_date
$fn$;

CREATE OR REPLACE FUNCTION sec_ticker_holds_at(
    p_ticker text, p_as_of date, p_on date[], p_max_age_days integer, p_current boolean
)
RETURNS TABLE (
    on_date date,
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
    SELECT o.cik, o.class_key, o.security_kind, o.adsh, o.accepted, o.security_title,
           CASE WHEN p_current THEN o.source_available_on ELSE o.available_on END AS known_on,
           o.filing_equity_classes, o.filing_complete
    FROM sec_observations_at(p_as_of, p_current) o
    WHERE o.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
), listed_active AS MATERIALIZED (
    -- pass 1: the same engine on the listed rows alone (sec_ticker_listed_holds_at),
    -- evaluated at each date a non-listed row showed the ticker
    SELECT DISTINCT h.on_date
    FROM sec_ticker_listed_holds_at(p_ticker, p_as_of, 400, p_current) h
    WHERE h.state = 'active'
), relevant AS (
    -- pass 2: a non-listed row showing the ticker is a competing holder unless a
    -- listed hold of it, as pass 1 computed it with this engine's full lifecycle,
    -- was active at that row's date (the admission rule)
    SELECT s.* FROM shown s
    WHERE s.security_kind IN ('equity', 'depositary', 'unknown')
       OR NOT EXISTS (SELECT 1 FROM listed_active a WHERE a.on_date = s.known_on)
), per_cik AS (
    SELECT r.cik,
           min(r.known_on) AS first_on,
           max(r.known_on) AS confirmed_on,
           (array_agg(r.class_key ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh DESC, r.class_key))[1] AS class_key,
           (array_agg(r.security_kind ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh DESC, r.class_key))[1] AS security_kind,
           array_agg(DISTINCT r.class_key) AS classes,
           array_remove(array_agg(DISTINCT sec_class_label(r.security_title, r.class_key)),
                        NULL) AS labels,
           bool_or(r.security_kind IN ('equity', 'depositary', 'unknown')) AS listed,
           array_agg(DISTINCT r.security_kind) AS kinds,
           COALESCE((array_agg(r.filing_equity_classes = 1 ORDER BY r.known_on DESC,
                               r.accepted DESC NULLS LAST, r.adsh DESC)
                     FILTER (WHERE r.filing_complete))[1], false) AS sole
    FROM relevant r
    GROUP BY r.cik
), candidate_rows AS MATERIALIZED (
    SELECT p.cik, f.adsh, f.class_key, f.security_kind, f.accepted, f.filing_complete,
           f.filing_equity_classes, f.security_title,
           CASE WHEN p_current THEN f.source_available_on ELSE f.available_on END AS known_on,
           f.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g') AS shows
    FROM per_cik p
    JOIN sec_observations_at(p_as_of, p_current) f
      ON f.cik = p.cik
     AND (f.class_key = ANY(p.classes)
          OR (f.filing_complete AND f.security_kind IN ('equity', 'depositary', 'unknown')
              AND (p.sole OR f.filing_equity_classes = 1)))
), candidates AS (
    -- labels: the classes of the rows by which the candidate states the hold (its
    -- rows showing the ticker, else its rows): only a registration naming one of
    -- them, never another class the ticker once showed on, reopens it
    SELECT r.cik, r.adsh, max(r.known_on) AS known_on, max(r.accepted) AS accepted,
           bool_or(r.shows) AS shows,
           CASE WHEN bool_or(r.shows)
                THEN array_remove(array_agg(DISTINCT sec_class_label(r.security_title, r.class_key))
                                  FILTER (WHERE r.shows), NULL)
                ELSE array_remove(array_agg(DISTINCT sec_class_label(r.security_title, r.class_key)),
                                  NULL)
           END AS labels
    FROM candidate_rows r
    GROUP BY r.cik, r.adsh
), stated AS (
    -- a filing that does not show the ticker states the hold (ending it as
    -- 'other_symbol') when it is complete (a 10-K/10-Q-type cover lists every
    -- class: the ticker is no longer listed), or when it shows every class that
    -- showed the ticker in the latest filing before it that did: an 8-K showing
    -- one class under another symbol leaves the ticker to the others
    SELECT c.* FROM candidates c
    WHERE c.shows
       OR EXISTS (SELECT 1 FROM candidate_rows z
                  WHERE z.cik = c.cik AND z.adsh = c.adsh AND z.filing_complete)
       OR NOT EXISTS (
        SELECT 1 FROM candidate_rows t
        WHERE t.cik = c.cik AND t.shows
          AND t.adsh = (SELECT x.adsh FROM candidates x
                        WHERE x.cik = c.cik AND x.shows AND x.known_on <= c.known_on
                          AND x.adsh <> c.adsh
                        ORDER BY x.known_on DESC, x.accepted DESC NULLS LAST, x.adsh DESC
                        LIMIT 1)
          AND NOT EXISTS (SELECT 1 FROM candidate_rows y
                          WHERE y.cik = c.cik AND y.adsh = c.adsh AND y.class_key = t.class_key))
), ends AS MATERIALIZED (
    SELECT p.cik, e.effective_on, e.filed, e.form, e.adsh, e.definitive, e.class_keys,
           e.tentative_keys, e.class_kind, e.named_kinds, e.issuer_symbols,
           (SELECT min(c.known_on) FROM candidates c
            WHERE c.cik = p.cik AND c.shows AND c.known_on >= e.effective_on) AS first_post_on
    FROM per_cik p
    CROSS JOIN LATERAL sec_issuer_end_events(p.cik, p_as_of, p_current) e
), closes AS MATERIALIZED (
    -- each end against each statement, per class: whether it closes every row by
    -- which the statement states the hold (its rows showing the ticker, else its
    -- rows), and whether it identifies them all (else it closes them tentatively)
    SELECT e.cik, e.adsh AS end_adsh, c.adsh,
           bool_and(sec_end_role(e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds,
                                 r.class_key, r.security_kind) IS NOT NULL) AS closed,
           bool_and(sec_end_role(e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds,
                                 r.class_key, r.security_kind) = 'identified') AS identified
    FROM ends e
    JOIN candidates c ON c.cik = e.cik
    JOIN candidate_rows r ON r.cik = c.cik AND r.adsh = c.adsh AND (r.shows OR NOT c.shows)
    GROUP BY e.cik, e.adsh, c.adsh
), dates AS (
    SELECT DISTINCT d.on_date FROM unnest(p_on) d(on_date)
), statement AS (
    -- at each date, the latest statement known by then that no end blocks
    SELECT DISTINCT ON (d.on_date, c.cik) d.on_date, c.*
    FROM dates d
    JOIN stated c ON c.known_on <= d.on_date
    JOIN per_cik p ON p.cik = c.cik
    WHERE NOT EXISTS (
        SELECT 1 FROM ends e
        JOIN closes k ON k.cik = e.cik AND k.end_adsh = e.adsh AND k.adsh = c.adsh
        WHERE e.cik = c.cik AND e.effective_on <= c.known_on AND k.closed
          AND ((e.definitive AND k.identified AND p.first_on < e.effective_on)
               OR EXISTS (
                   SELECT 1 FROM per_cik o
                   WHERE o.cik <> c.cik
                     AND o.first_on BETWEEN e.effective_on - 30 AND e.first_post_on))
          AND NOT EXISTS (
              -- a registration after the end that identifies the class relists it:
              -- one naming its class, or naming none when the issuer listed one
              -- symbol
              SELECT 1 FROM sec_registration_starts(c.cik, p_as_of, p_current) r
              WHERE r.filed > e.filed AND r.available_on <= c.known_on
                AND (r.classes && c.labels
                     OR (cardinality(r.classes) = 0 AND e.issuer_symbols = 1))))
    ORDER BY d.on_date, c.cik, c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh DESC
), last_end AS (
    -- the latest end, effective by the date, after the statement that closes every
    -- class it shows the ticker on (an end of another class leaves the hold)
    SELECT DISTINCT ON (s.on_date, e.cik) s.on_date, e.*
    FROM ends e
    JOIN statement s ON s.cik = e.cik
    JOIN closes k ON k.cik = e.cik AND k.end_adsh = e.adsh AND k.adsh = s.adsh
    WHERE e.effective_on > s.known_on AND e.effective_on <= s.on_date AND k.closed
    ORDER BY s.on_date, e.cik, e.effective_on DESC, k.identified DESC, e.adsh DESC
)
SELECT s.on_date, p.cik,
       CASE
           WHEN l.effective_on > s.known_on THEN 'ended'
           WHEN NOT s.shows THEN 'ended'
           WHEN s.on_date - s.known_on > p_max_age_days THEN 'stale'
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
LEFT JOIN last_end l ON l.cik = p.cik AND l.on_date = s.on_date
$fn$;

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
SELECT h.cik, h.state, h.class_key, h.security_kind, h.statement_on, h.statement_accepted,
       h.statement_adsh, h.first_on, h.confirmed_on, h.end_reason
FROM sec_ticker_holds_at(p_ticker, p_as_of, ARRAY[p_as_of], p_max_age_days, p_current) h
$fn$;

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
    SELECT o.available_on, o.accepted, o.adsh, o.filing_equity_classes, o.filing_complete,
           o.security_kind
    FROM sec_observations_at(p_as_of, false) o
    WHERE o.cik = p_cik AND o.class_key = p_class_key
), follows_sole AS (
    -- a listed class that its latest complete filing showed as the issuer's only
    -- equity class is followed through the issuer's one-class filings (a
    -- preferred or notes line is not the issuer's equity)
    SELECT CASE
               WHEN NOT EXISTS (SELECT 1 FROM own) THEN true
               ELSE COALESCE((
                   SELECT o.filing_equity_classes = 1
                          AND o.security_kind IN ('equity', 'depositary', 'unknown')
                   FROM own o
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
    -- the ends that close the line's classes ('identified' when one of them is
    -- named, else 'tentative')
    SELECT e.*, x.role
    FROM sec_issuer_end_events(p_cik, p_as_of, false) e
    CROSS JOIN LATERAL (
        SELECT CASE WHEN bool_or(y.role = 'identified') THEN 'identified'
                    WHEN bool_or(y.role = 'tentative') THEN 'tentative' END AS role
        FROM (SELECT sec_end_role(e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds,
                                  r.class_key, r.security_kind) AS role
              FROM (SELECT DISTINCT r.class_key, r.security_kind FROM rows r) r) y
    ) x
    WHERE x.role IS NOT NULL
), last_end AS (
    SELECT e.* FROM ends e ORDER BY e.effective_on DESC, e.role = 'identified' DESC, e.adsh DESC LIMIT 1
), candidates AS (
    -- labels: the classes of the candidate's own rows (only a registration naming
    -- one of them reopens the line)
    SELECT r.adsh, max(r.available_on) AS known_on, max(r.accepted) AS accepted,
           array_agg(DISTINCT r.ticker_key) AS keys,
           COALESCE(array_remove(array_agg(DISTINCT sec_class_label(r.security_title, r.class_key)),
                                 NULL), '{}') AS labels
    FROM rows r
    GROUP BY r.adsh
), chosen AS (
    SELECT c.* FROM candidates c
    WHERE NOT EXISTS (
        SELECT 1 FROM ends d
        WHERE d.definitive AND d.role = 'identified' AND d.effective_on <= c.known_on
          AND EXISTS (
              SELECT 1 FROM sec_observations_at(p_as_of, false) x
              WHERE x.cik = p_cik AND x.ticker_key = ANY(c.keys)
                AND x.available_on < d.effective_on)
          AND NOT EXISTS (
              SELECT 1 FROM sec_registration_starts(p_cik, p_as_of, false) r
              WHERE r.filed > d.filed AND r.available_on <= c.known_on
                AND (r.classes && c.labels
                     OR (cardinality(r.classes) = 0 AND d.issuer_symbols = 1))))
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
            -- the class structure comes from complete covers only: an 8-K (whose
            -- members vary: CCL's, DUK's and NI's 8-Ks name their common stock
            -- unlike their 10-Qs) says neither that a class is the only one nor
            -- that it is listed beside another
            SELECT e.class_key,
                   bool_and(e.filing_equity_classes = 1) FILTER (WHERE e.filing_complete)
                       AS sole_only,
                   bool_or(f.listed > 1) FILTER (WHERE e.filing_complete) AS beside_listed
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
            WHERE NOT (x.class_key = '' AND COALESCE(sx.sole_only, true)
                       AND COALESCE(sy.beside_listed, false))
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

CREATE OR REPLACE FUNCTION sec_ticker_line_runs_from(
    p_ticker text, p_max_age_days integer, p_listed_only boolean
)
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
-- Planned once per call, never inlined (it calls itself for its first pass); JIT
-- compilation would cost more than the query.
SET jit = off
AS $fn$
WITH key AS (
    SELECT regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g') AS k
), shown AS (
    SELECT o.cik, o.class_key, o.security_kind, o.adsh, o.source_available_on AS known_on,
           o.filing_complete, o.filing_equity_classes
    FROM sec_observations_at('infinity'::date, true) o, key
    WHERE o.ticker_key = key.k
), listed_runs AS MATERIALIZED (
    -- pass 1: this engine on the listed rows alone (equity, depositary, unknown),
    -- each run going stale 400 days after its last statement; scanned only by
    -- pass 2, so a listed-only call never calls itself again
    SELECT r.valid_from, r.valid_to
    FROM sec_ticker_line_runs_from(p_ticker, 400, true) r
), relevant AS (
    -- pass 2: a non-listed row showing the ticker is a competing holder unless a
    -- listed run of it, as pass 1 computed it with this engine's full lifecycle,
    -- was alive at that row's date (the admission rule)
    SELECT s.* FROM shown s
    WHERE s.security_kind IN ('equity', 'depositary', 'unknown')
       OR CASE WHEN p_listed_only THEN false
               ELSE NOT EXISTS (
                   SELECT 1 FROM listed_runs r
                   WHERE r.valid_from <= s.known_on
                     AND (r.valid_to IS NULL OR s.known_on < r.valid_to))
          END
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
               FILTER (WHERE l.line_key = h.line_key AND o.ticker_key = key.k))[1] AS class_key,
           -- the classes of the candidate's own rows of the line: only a
           -- registration naming one of them reopens the line
           COALESCE(array_remove(array_agg(DISTINCT sec_class_label(o.security_title, o.class_key))
                                 FILTER (WHERE l.line_key = h.line_key), NULL), '{}') AS labels
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
           e.tentative_keys, e.class_kind, e.named_kinds, e.issuer_symbols
    FROM holder_ciks h
    CROSS JOIN LATERAL sec_issuer_end_events(h.cik, 'infinity'::date, true) e
), starts AS MATERIALIZED (
    SELECT h.cik, r.filed, r.available_on, r.classes
    FROM holder_ciks h
    CROSS JOIN LATERAL sec_registration_starts(h.cik, 'infinity'::date, true) r
), line_ends AS MATERIALIZED (
    -- the ends that close a class of the line ('identified' when one is named,
    -- else 'tentative': closed until the line's next statement)
    SELECT h.cik, h.line_key, e.effective_on, e.filed, e.form, e.adsh, e.definitive, x.role,
           e.issuer_symbols
    FROM held h
    JOIN ends e ON e.cik = h.cik
    CROSS JOIN LATERAL (
        SELECT CASE WHEN bool_or(y.role = 'identified') THEN 'identified'
                    WHEN bool_or(y.role = 'tentative') THEN 'tentative' END AS role
        FROM (SELECT sec_end_role(e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds,
                                  l.class_key, k.kind) AS role
              FROM lines l CROSS JOIN unnest(h.kinds) k(kind)
              WHERE l.cik = h.cik AND l.line_key = h.line_key) y
    ) x
    WHERE x.role IS NOT NULL
), blocking AS (
    -- ends after which the line's later candidates do not count (definitive end
    -- of a ticker shown before it, or the ticker moved to another CIK)
    SELECT h.cik, h.line_key, e.effective_on, e.filed, e.issuer_symbols
    FROM held h
    JOIN line_ends e ON e.cik = h.cik AND e.line_key = h.line_key
    WHERE (e.definitive AND e.role = 'identified' AND h.first_on < e.effective_on)
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
              WHERE r.cik = c.cik AND r.filed > b.filed AND r.available_on <= c.known_on
                AND (r.classes && c.labels
                     OR (cardinality(r.classes) = 0 AND b.issuer_symbols = 1))))
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
        ORDER BY e.effective_on DESC, e.role = 'identified' DESC, e.adsh DESC
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
SET jit = off
AS $fn$
SELECT * FROM sec_ticker_line_runs_from(p_ticker, p_max_age_days, false)
$fn$;

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
               FILTER (WHERE l.line_key = p_line_key) AS tickers,
           -- the classes of the candidate's own rows of the line: only a
           -- registration naming one of them reopens the line
           COALESCE(array_remove(array_agg(DISTINCT sec_class_label(o.security_title, o.class_key))
                                 FILTER (WHERE l.line_key = p_line_key), NULL), '{}') AS labels
    FROM sec_observations_at('infinity'::date, true) o
    LEFT JOIN lines l ON l.class_key = o.class_key
    WHERE o.cik = p_cik
      AND (l.line_key = p_line_key
           OR (o.filing_complete AND o.filing_equity_classes = 1
               AND o.security_kind IN ('equity', 'depositary', 'unknown'))
           OR (o.filing_complete AND o.security_kind IN ('equity', 'depositary', 'unknown')
               AND o.source_available_on > (SELECT s.sole_until FROM sole s)))
    GROUP BY o.adsh
), line_rows AS MATERIALIZED (
    SELECT DISTINCT o.class_key, o.security_kind
    FROM sec_observations_at('infinity'::date, true) o
    JOIN lines l ON l.class_key = o.class_key
    WHERE o.cik = p_cik AND l.line_key = p_line_key
), ends AS MATERIALIZED (
    -- the ends that close a class of the line ('identified' when one is named,
    -- else 'tentative')
    SELECT e.*, x.role
    FROM sec_issuer_end_events(p_cik, 'infinity'::date, true) e
    CROSS JOIN LATERAL (
        SELECT CASE WHEN bool_or(y.role = 'identified') THEN 'identified'
                    WHEN bool_or(y.role = 'tentative') THEN 'tentative' END AS role
        FROM (SELECT sec_end_role(e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds,
                                  r.class_key, r.security_kind) AS role
              FROM line_rows r) y
    ) x
    WHERE x.role IS NOT NULL
), counted AS MATERIALIZED (
    SELECT c.* FROM candidates c
    WHERE NOT EXISTS (
        SELECT 1 FROM ends d
        WHERE d.definitive AND d.role = 'identified' AND d.effective_on <= c.known_on
          AND EXISTS (
              SELECT 1 FROM candidates x
              WHERE x.has_line AND x.known_on < d.effective_on AND x.keys && c.keys)
          AND NOT EXISTS (
              SELECT 1 FROM sec_registration_starts(p_cik, 'infinity'::date, true) r
              WHERE r.filed > d.filed AND r.available_on <= c.known_on
                AND (r.classes && c.labels
                     OR (cardinality(r.classes) = 0 AND d.issuer_symbols = 1))))
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
        ORDER BY e.effective_on DESC, e.role = 'identified' DESC, e.adsh DESC
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

REVOKE ALL ON FUNCTION sec_label_text(text), sec_label_norm(text), sec_label_id_re(), sec_first_label(text),
    sec_class_label(text, text), sec_named_classes(text),
    sec_end_role(text[], text[], text, text[], text, text),
    sec_observations_at(date, boolean),
    sec_share_counts_at(date, boolean),
    sec_registration_end_events(bigint, date, boolean),
    sec_registration_starts(bigint, date, boolean),
    sec_issuer_end_events(bigint, date, boolean),
    sec_ticker_holds(text, date, integer, boolean),
    sec_ticker_holds_at(text, date, date[], integer, boolean),
    sec_ticker_listed_holds_at(text, date, integer, boolean),
    sec_ticker_issuer_at(text, date, integer),
    sec_issuer_line_at(bigint, text, date, integer),
    sec_cover_class_shares_at(bigint, text, date, integer),
    sec_cover_ticker_shares_at(text, bigint, date, integer),
    sec_issuer_lines(bigint),
    sec_ticker_line_runs(text, integer),
    sec_ticker_line_runs_from(text, integer, boolean),
    sec_line_alive_runs(bigint, text, integer),
    sec_line_price_evidence(text, bigint, text),
    sec_ticker_price_span(text, bigint, text) FROM PUBLIC;
DO $$
DECLARE
    routines constant text[] := ARRAY[
        'sec_label_text(text)',
        'sec_label_norm(text)',
        'sec_label_id_re()',
        'sec_first_label(text)',
        'sec_class_label(text, text)',
        'sec_named_classes(text)',
        'sec_end_role(text[], text[], text, text[], text, text)',
        'sec_observations_at(date, boolean)',
        'sec_share_counts_at(date, boolean)',
        'sec_registration_end_events(bigint, date, boolean)',
        'sec_registration_starts(bigint, date, boolean)',
        'sec_issuer_end_events(bigint, date, boolean)',
        'sec_ticker_holds(text, date, integer, boolean)',
        'sec_ticker_holds_at(text, date, date[], integer, boolean)',
        'sec_ticker_listed_holds_at(text, date, integer, boolean)',
        'sec_ticker_issuer_at(text, date, integer)',
        'sec_issuer_line_at(bigint, text, date, integer)',
        'sec_cover_class_shares_at(bigint, text, date, integer)',
        'sec_cover_ticker_shares_at(text, bigint, date, integer)',
        'sec_issuer_lines(bigint)',
        'sec_ticker_line_runs(text, integer)',
        'sec_ticker_line_runs_from(text, integer, boolean)',
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

DROP FUNCTION IF EXISTS sec_end_role(text[], text[], text, text[], text, text, text, text, jsonb);
DROP FUNCTION IF EXISTS sec_issuer_end_scopes(bigint, date, boolean);
DROP FUNCTION IF EXISTS sec_registration_identifies(bigint, date, boolean, date, date, text, text);
DROP FUNCTION IF EXISTS sec_instrument_label(text, text, text);
DROP FUNCTION IF EXISTS sec_instrument_scopes(text);
DROP FUNCTION IF EXISTS sec_issuer_lines_at(bigint, date, boolean, date);
DROP FUNCTION IF EXISTS sec_named_kinds(text);
DROP FUNCTION IF EXISTS sec_class_label_history(bigint, date, boolean, jsonb);
COMMIT;
