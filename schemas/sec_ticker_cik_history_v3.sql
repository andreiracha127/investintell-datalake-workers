-- SEC ticker -> (CIK, class) contract v3; apply after v1 and v2.
-- Catalog-only, idempotent function/view replacement: no tables or rows changed.
-- All statements are positioned at source_available_on; available_on gates visibility.
-- Labels include their Class/Series namespace. Amendment-added end scopes take
-- effect at their own filing; retained scopes keep the original effect date.
-- See docs/runbooks/sec-ticker-cik-history.md for the contract and measured proof.
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
SELECT lower(x.m[1]) || ':' || sec_label_norm(x.m[2])
FROM regexp_matches(sec_label_text(p_text),
                    '(?:^|[^a-z])(class|series)\s+(' || sec_label_id_re() || ')',
                    'gi') WITH ORDINALITY AS x(m, n)
WHERE sec_label_norm(x.m[2]) IS NOT NULL
ORDER BY x.n
LIMIT 1
$fn$;

CREATE OR REPLACE FUNCTION sec_class_label(p_title text, p_class_key text)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
SELECT COALESCE(sec_first_label(p_title), sec_first_label(p_class_key))
$fn$;

-- Batch fallback labels for listed rows without an explicit current title.
-- Requests: [{"class_key": "raw member key", "bound_on": "YYYY-MM-DD"}, ...].
-- Call once per CIK with DISTINCT requests, after narrowing to needed rows.
-- A caller's own explicit title ALWAYS takes precedence over this fallback.
CREATE OR REPLACE FUNCTION sec_class_label_history(
    p_cik bigint, p_as_of date, p_current boolean, p_requests jsonb
)
RETURNS TABLE (class_key text, bound_on date, label text)
LANGUAGE plpgsql STABLE PARALLEL SAFE
AS $fn$
DECLARE
    requested record;
    donor record;
    bounds date[];
    position integer;
    bound_count integer;
    explicit_label text;
    explicit_labels text[];
BEGIN
    -- One ordered donor scan per requested raw key. The PL/pgSQL loop parses
    -- only donors needed to answer a bound, and each donor at most once. In
    -- particular, this does not eagerly parse every historical cover title.
    FOR requested IN
        SELECT q.class_key,
               array_agg(DISTINCT q.bound_on ORDER BY q.bound_on DESC) AS bounds
        FROM jsonb_to_recordset(COALESCE(p_requests, '[]'::jsonb))
             AS q(class_key text, bound_on date)
        WHERE q.class_key IS NOT NULL AND q.bound_on IS NOT NULL
        GROUP BY q.class_key
        ORDER BY q.class_key COLLATE "C"
    LOOP
        bounds := requested.bounds;
        bound_count := cardinality(bounds);
        position := 1;
        class_key := requested.class_key;
        FOR donor IN
            SELECT o.source_available_on AS source_on, o.adsh,
                   array_agg(DISTINCT o.security_title COLLATE "C" ORDER BY o.security_title COLLATE "C") AS titles
            FROM sec_observations_at(p_as_of, p_current) o
            WHERE o.cik = p_cik AND o.class_key = requested.class_key
              AND o.security_kind IN ('equity', 'depositary', 'unknown')
              AND o.security_title IS NOT NULL
              AND o.source_available_on <= bounds[1]
            GROUP BY o.source_available_on, o.adsh
            ORDER BY o.source_available_on DESC, max(o.accepted) DESC NULLS LAST,
                     o.adsh COLLATE "C" DESC
        LOOP
            IF donor.source_on > bounds[position] THEN
                CONTINUE;
            END IF;
            SELECT array_remove(array_agg(DISTINCT sec_first_label(t.title) COLLATE "C" ORDER BY sec_first_label(t.title) COLLATE "C"), NULL)
            INTO explicit_labels FROM unnest(donor.titles) AS t(title);
            IF cardinality(explicit_labels) = 0 THEN
                CONTINUE;
            END IF;
            -- A same-key filing naming two distinct labels is ambiguous.
            -- Preserve NULL instead of choosing a title or member namespace.
            explicit_label := CASE WHEN cardinality(explicit_labels) = 1
                                   THEN explicit_labels[1] END;
            -- Reuse one authoritative donor for every still-pending bound
            -- at or after its source date, without another database query.
            WHILE position <= bound_count AND donor.source_on <= bounds[position] LOOP
                bound_on := bounds[position];
                label := explicit_label;
                RETURN NEXT;
                position := position + 1;
            END LOOP;
            EXIT WHEN position > bound_count;
        END LOOP;
        -- Only a lack of visible, sufficiently old explicit listed titles
        -- permits member fallback. Class and Series remain separate labels.
        label := sec_first_label(requested.class_key);
        WHILE position <= bound_count LOOP
            bound_on := bounds[position];
            RETURN NEXT;
            position := position + 1;
        END LOOP;
    END LOOP;
END
$fn$;

CREATE OR REPLACE FUNCTION sec_named_classes(p_description text)
RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
WITH grammar AS (
    SELECT '(?:^|[^a-z])(class(?:es)?|series)\s+((?:' || sec_label_id_re() || ')'
           || '(?:\s*(?:,|/|&|\mand\M|\mor\M)\s*(?:' || sec_label_id_re() || '))*)' AS pattern
), clauses AS (
    SELECT c.text FROM regexp_split_to_table(sec_label_text(p_description), '[;\n]') c(text)
), expanded AS (
    SELECT x.m[1] AS kw, x.m[2] AS ids,
           (regexp_match(substring(c.text FROM regexp_instr(c.text, g.pattern, 1, x.n::integer, 1, 'i')),
                '(?i)\m(common|ordinary|capital|preferred|preference|notes?|debentures?|warrants?|rights?|units?)\M'))[1]
               AS instrument
    FROM clauses c CROSS JOIN grammar g
    CROSS JOIN LATERAL regexp_matches(c.text, g.pattern, 'gi') WITH ORDINALITY x(m,n)
)
SELECT ARRAY(
    SELECT DISTINCT ((CASE WHEN lower(e.kw) = 'series' THEN 'series:' ELSE 'class:' END)
                     || sec_label_norm(l.id)) COLLATE "C"
    FROM expanded e,
         regexp_split_to_table(e.ids, '(?i)\s*(?:,|/|&|\mand\M|\mor\M)\s*') l(id)
    WHERE l.id <> '' AND sec_label_norm(l.id) IS NOT NULL
      AND (lower(e.kw) <> 'series' OR lower(e.instrument) IN ('common', 'ordinary', 'capital'))
    ORDER BY 1)
$fn$;

CREATE OR REPLACE FUNCTION sec_named_kinds(p_description text)
RETURNS text[] LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
WITH cleaned AS (
    SELECT regexp_replace(regexp_replace(COALESCE(p_description, ''),
        '(?i)\m(?:associated|attached|and|together with)\s+(?:\w+\s+){0,4}purchase\s+rights?\M', ' ', 'g'),
        '(?i)\m(?:associated|attached|and|together with)\s+(?:associated\s+)?rights?\M[^;,\n]{0,100}?\mto\s+purchase\M[^;,\n]*',
        ' ', 'g') AS attached_cleaned
), instruments AS (
    SELECT regexp_replace(regexp_replace(attached_cleaned,
        '(?i)(?:\w+\s+){0,6}purchase\s+rights?\M', 'rights', 'g'),
        '(?i)(\mrights?\M)[^;,\n]{0,100}?\mto\s+purchase\M[^;,\n]*', '\1', 'g') AS description
    FROM cleaned
), own_instruments AS (
    SELECT regexp_replace(description, '(?i)(\mwarrants?\M[^;\n]{0,100}?)\m(?:to\s+(?:purchase|acquire)|exercisable\s+for)\M[^;,\n]*', '\1', 'g') AS description FROM instruments
)
SELECT ARRAY(SELECT n.kind
    FROM own_instruments c, (VALUES ('warrant', '\mwarrants?\M'), ('unit', '\munits?\M'),
        ('right', '\mrights?\M'), ('preferred', 'preferred|preference'),
        ('debt', '\mnotes?\M|debentures?|\mbonds?\M')) n(kind, pattern)
    WHERE c.description ~* n.pattern ORDER BY n.kind COLLATE "C")
$fn$;

-- Instrument scopes keep labels and explicit trading symbols attached to
-- their own kind. Labels naming purchased common/preferred shares do not name
-- the warrant/right itself. Dependent rights are excluded by sec_named_kinds.
CREATE OR REPLACE FUNCTION sec_instrument_scopes(p_description text)
RETURNS TABLE (kind text, labels text[], symbols text[])
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
WITH detached AS (
    -- Remove dependent rights before stripping standalone purchase targets;
    -- otherwise removing "to purchase" would turn an attachment into a right.
    SELECT regexp_replace(regexp_replace(sec_label_text(p_description),
        '(?i)\m(?:associated|attached|and|together with)\s+(?:\w+\s+){0,4}purchase\s+rights?\M', ' ', 'g'),
        '(?i)\m(?:associated|attached|and|together with)\s+(?:associated\s+)?rights?\M[^;,\n]{0,100}?\mto\s+purchase\M[^;,\n]*',
        ' ', 'g') AS text
), own_text AS (
    SELECT regexp_replace(regexp_replace(text,
        '(?i)(\m(?:warrants?|rights?)\M[^;\n]{0,100}?)\m(?:to\s+(?:purchase|acquire)|exercisable\s+for)\M[^;,\n]*', '\1', 'g'),
        '(?i)(\m(?:symbol|ticker))\s*\(s\)', '\1s', 'g') AS text
    FROM detached
),
clauses AS MATERIALIZED (
    SELECT c.text FROM own_text own CROSS JOIN LATERAL regexp_split_to_table(own.text,
        '(?i)[;\n]|(?:,|\mand\M)\s*(?=(?:warrants?|units?|rights?|preferred|preference|notes?|debentures?|bonds?)\M)') c(text)
), grammar AS (
    SELECT '(?:^|[^a-z])(class(?:es)?|series)\s+((?:' || sec_label_id_re() || ')'
        || '(?:\s*(?:,|/|&|\mand\M|\mor\M)\s*(?:' || sec_label_id_re() || '))*)' AS label_re,
        '(?i)\m(?:(?:trading\s+symbols?|tickers?|symbols?)\M\s*(?:is\s+|are\s+)?[:=("'' ]*|(?:nyse(?:\s+(?:american|arca))?|nasdaq|amex)\s*[:(]\s*)([a-z0-9][a-z0-9.-]{0,31})(?=\s*(?:$|[,;:()"'']))' AS symbol_re,
        '(?i)\m(common|ordinary|capital|preferred|preference|warrants?|units?|rights?|notes?|debentures?|bonds?)\M' AS kind_re
), tokens AS (
    SELECT c.text, 'label'::text AS type, x.m[1] AS namespace, x.m[2] AS value,
           regexp_instr(c.text, g.label_re, 1, x.n::integer, 0, 'i') AS begins,
           regexp_instr(c.text, g.label_re, 1, x.n::integer, 1, 'i') AS ends
    FROM clauses c CROSS JOIN grammar g
    CROSS JOIN LATERAL regexp_matches(c.text, g.label_re, 'gi') WITH ORDINALITY x(m,n)
    UNION ALL
    SELECT c.text, 'symbol', NULL, x.m[1],
           regexp_instr(c.text, g.symbol_re, 1, x.n::integer, 0),
           regexp_instr(c.text, g.symbol_re, 1, x.n::integer, 1)
    FROM clauses c CROSS JOIN grammar g
    CROSS JOIN LATERAL regexp_matches(c.text, g.symbol_re, 'g') WITH ORDINALITY x(m,n)
), attached AS (
    SELECT t.*, CASE WHEN t.type = 'label' THEN COALESCE(after_kind.value, before_kind.value)
                    ELSE COALESCE(before_kind.value, after_kind.value) END AS kind_word
    FROM tokens t CROSS JOIN grammar g
    LEFT JOIN LATERAL (SELECT lower((regexp_match(substring(t.text FROM t.ends), g.kind_re))[1]) AS value) after_kind ON true
    LEFT JOIN LATERAL (
        SELECT lower(x.m[1]) AS value
        FROM regexp_matches(left(t.text, t.begins - 1), g.kind_re, 'g') WITH ORDINALITY x(m,n)
        ORDER BY x.n DESC LIMIT 1
    ) before_kind ON true
), scoped AS (
    SELECT a.*, CASE WHEN a.kind_word IN ('common','ordinary','capital') THEN 'equity'
                    WHEN a.kind_word IN ('preferred','preference') THEN 'preferred'
                    WHEN a.kind_word IN ('note','notes','debenture','debentures','bond','bonds') THEN 'debt'
                    ELSE rtrim(a.kind_word, 's') END AS instrument_kind
    FROM attached a
), kinds AS (
    SELECT DISTINCT c.text, k.kind FROM clauses c CROSS JOIN LATERAL unnest(sec_named_kinds(c.text)) k(kind)
)
SELECT k.kind,
       ARRAY(SELECT DISTINCT ((CASE WHEN lower(t.namespace) = 'series' THEN 'series:' ELSE 'class:' END)
                    || sec_label_norm(id.value)) COLLATE "C"
             FROM scoped t CROSS JOIN LATERAL regexp_split_to_table(t.value,
                 '(?i)\s*(?:,|/|&|\mand\M|\mor\M)\s*') id(value)
             WHERE t.text = k.text AND t.type = 'label' AND t.instrument_kind = k.kind AND sec_label_norm(id.value) IS NOT NULL
             ORDER BY 1),
       ARRAY(SELECT DISTINCT regexp_replace(upper(t.value), '[^A-Z0-9]', '', 'g') COLLATE "C"
             FROM scoped t WHERE t.text = k.text AND t.type = 'symbol' AND t.instrument_kind = k.kind
               AND (lower(t.value) NOT IN ('of','the','is','are','for','on','and','or','in','to','an')
                    OR substring(t.text FROM t.begins FOR t.ends - t.begins) ~ '[:=("'']') ORDER BY 1)
FROM kinds k
$fn$;

CREATE OR REPLACE FUNCTION sec_instrument_label(p_title text, p_class_key text, p_kind text)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
WITH title AS MATERIALIZED (
         SELECT s.kind, array_remove(array_agg(DISTINCT l.label COLLATE "C" ORDER BY l.label COLLATE "C"), NULL) AS labels
         FROM sec_instrument_scopes(p_title) s LEFT JOIN LATERAL unnest(s.labels) l(label) ON true
         GROUP BY s.kind
     ), member AS MATERIALIZED (
         SELECT s.kind, array_remove(array_agg(DISTINCT l.label COLLATE "C" ORDER BY l.label COLLATE "C"), NULL) AS labels
         FROM sec_instrument_scopes(p_class_key) s LEFT JOIN LATERAL unnest(s.labels) l(label) ON true
         GROUP BY s.kind
     ),
     generic_labels AS (
         -- Supplying a harmless equity suffix reuses the namespace/enumeration
         -- grammar for kind-free labels. The guards below reject actual equity
         -- wording and purchase targets before these labels can be used.
         SELECT ARRAY(SELECT DISTINCT label COLLATE "C"
                      FROM regexp_split_to_table(COALESCE(p_title, ''), '[;\n]') clause
                      CROSS JOIN LATERAL unnest(sec_named_classes(clause || ' common stock')) label
                      ORDER BY 1) AS title_labels,
                ARRAY(SELECT DISTINCT label COLLATE "C"
                      FROM regexp_split_to_table(COALESCE(p_class_key, ''), '[;\n]') clause
                      CROSS JOIN LATERAL unnest(sec_named_classes(clause || ' common stock')) label
                      ORDER BY 1) AS member_labels
     ), plain AS (
         SELECT g.*, CASE WHEN cardinality(g.member_labels) = 1 THEN g.member_labels[1] END AS label,
                sec_label_text(p_class_key) AS member_text, sec_label_text(p_title) AS title_text
         FROM generic_labels g
     ), target AS (
         SELECT COALESCE((regexp_match(p.title_text,
             '(?i)\m(?:to\s+(?:purchase|acquire)|exercisable\s+for)\M(.*)'))[1], '') AS text
         FROM plain p
     )
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM title t WHERE t.kind = p_kind AND cardinality(t.labels) > 0)
        THEN (SELECT CASE WHEN cardinality(t.labels) = 1 THEN t.labels[1] END
              FROM title t WHERE t.kind = p_kind)
    WHEN cardinality(p.title_labels) > 0 AND NOT EXISTS (SELECT 1 FROM title)
      AND p.title_text !~* '\m(common|ordinary|capital)\M'
      AND p.title_text !~* '\m(?:to\s+(?:purchase|acquire)|exercisable\s+for|purchase\s+rights?)\M'
        THEN CASE WHEN cardinality(p.title_labels) = 1 THEN p.title_labels[1] END
    WHEN NOT EXISTS (SELECT 1 FROM title t WHERE t.kind = p_kind)
      AND (EXISTS (SELECT 1 FROM title) OR p.title_text ~* '\m(common|ordinary|capital)\M')
        THEN NULL
    WHEN EXISTS (SELECT 1 FROM member m WHERE m.kind = p_kind AND cardinality(m.labels) > 0)
        THEN (SELECT CASE WHEN cardinality(m.labels) = 1 THEN m.labels[1] END
              FROM member m WHERE m.kind = p_kind)
    -- A plain Class/Series member is own-class evidence when the row supplies
    -- its kind. Explicit other-kind words and purchase targets are not.
    WHEN NOT EXISTS (SELECT 1 FROM member m WHERE m.kind <> p_kind)
      AND p.member_text !~* '\m(common|ordinary|capital)\M'
      AND p.member_text !~* '\m(?:to\s+(?:purchase|acquire)|exercisable\s+for|purchase\s+rights?)\M'
      AND NOT COALESCE(p.label = sec_first_label(target.text), false)
      AND NOT COALESCE(p.label = ANY(sec_named_classes(target.text)), false)
      AND NOT EXISTS (SELECT 1 FROM sec_instrument_scopes(target.text) t WHERE p.label = ANY(t.labels))
      AND NOT (p.title_text ~* '\mpurchase\s+rights?\M'
               AND COALESCE(p.label = sec_first_label(p.title_text), false))
        THEN p.label
END
FROM plain p CROSS JOIN target
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
               CASE WHEN p_class_kind = 'other' THEN NULL ELSE
               CASE
                   WHEN p_class_keys IS NULL THEN 'identified'
                   WHEN p_class_key = ANY(p_class_keys) THEN
                       CASE WHEN p_class_key = ANY(p_tentative_keys) THEN 'tentative'
                            ELSE 'identified' END
               END
               END
           WHEN p_class_kind = 'other' AND p_kind = ANY(p_named_kinds) THEN
               CASE WHEN p_class_key = ANY(p_class_keys) THEN
                   CASE WHEN p_class_key = ANY(p_tentative_keys) THEN 'tentative' ELSE 'identified' END END
           WHEN p_class_kind IS NULL OR p_class_kind = 'unknown' THEN
               CASE WHEN cardinality(p_tentative_keys) > 0 THEN 'tentative'
                    ELSE 'identified' END
       END
$fn$;

CREATE OR REPLACE FUNCTION sec_end_role(
    p_class_keys text[], p_tentative_keys text[], p_class_kind text, p_named_kinds text[],
    p_class_key text, p_kind text, p_label text, p_ticker_key text, p_instrument_scope jsonb
)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
SELECT CASE WHEN p_kind IN ('equity','depositary','unknown') OR p_class_kind IS DISTINCT FROM 'other'
    THEN sec_end_role(p_class_keys,p_tentative_keys,p_class_kind,p_named_kinds,p_class_key,p_kind)
    WHEN p_kind = ANY(p_named_kinds) THEN (
        SELECT CASE WHEN bool_or(scope.role = 'identified') THEN 'identified'
                    WHEN bool_or(scope.role = 'tentative') THEN 'tentative' END
        FROM jsonb_to_recordset(COALESCE(p_instrument_scope, '[]'::jsonb))
             scope(class_key text,label text,ticker_key text,mode text,named_label text,role text)
        WHERE scope.class_key = p_class_key
          AND (scope.mode <> 'symbol' OR scope.ticker_key = p_ticker_key)
          AND (scope.mode = 'unmatched_label' OR p_label IS NULL
               OR scope.named_label IS NULL OR p_label = scope.named_label)
          AND (p_label IS NULL OR scope.label IS NULL OR p_label = scope.label)) END
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
        ORDER BY o.filed DESC, o.adsh COLLATE "C" DESC
        LIMIT 1
    ) AS original
    FROM visible a
    WHERE a.form LIKE '%/A'
), latest_amendment AS (
    SELECT DISTINCT ON (m.original COLLATE "C") m.*
    FROM amended m
    WHERE m.original IS NOT NULL AND m.amendment_effect IS NOT NULL
    ORDER BY m.original COLLATE "C", m.filed DESC, m.adsh COLLATE "C" DESC
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
WITH visible AS MATERIALIZED (
    SELECT e.*, CASE WHEN p_current THEN e.source_available_on ELSE e.available_on END AS known_on
    FROM sec_registration_events e
    WHERE e.cik = p_cik
      AND replace(e.form, '/A', '') IN ('8-A12B', '8-A12G', '10-12B', '10-12G', '8-K12B', '8-K12G3')
      AND CASE WHEN p_current
               THEN e.retired_on IS NULL AND e.source_available_on <= p_as_of
               ELSE e.available_on <= p_as_of
                    AND (e.retired_on IS NULL OR (e.retired_on > p_as_of
                         AND e.retired_reason IS DISTINCT FROM 'parser_correction')) END
), amendments AS MATERIALIZED (
    SELECT a.*, (SELECT o.adsh FROM visible o
                  WHERE o.form = left(a.form, -2) AND o.filed <= a.filed
                  ORDER BY o.filed DESC, o.adsh COLLATE "C" DESC LIMIT 1) AS original
    FROM visible a WHERE a.form LIKE '%/A'
), versions AS MATERIALIZED (
    SELECT v.adsh AS original, v.adsh, v.form, v.filed, v.known_on,
           sec_named_classes(v.class_description) AS classes,
           v.class_kind IS DISTINCT FROM 'other' AS eligible
    FROM visible v WHERE v.form NOT LIKE '%/A'
    UNION ALL
    SELECT COALESCE(a.original, a.adsh), a.adsh, left(a.form, -2), a.filed, a.known_on,
           sec_named_classes(a.class_description),
           a.class_kind = 'equity' AND a.amendment_effect = 'restates'
    FROM amendments a WHERE a.amendment_effect IN ('restates', 'cancels')
), ordered AS MATERIALIZED (
    SELECT v.*, row_number() OVER (PARTITION BY v.original
                                   ORDER BY v.filed, v.adsh COLLATE "C") AS sequence
    FROM versions v
), latest AS (
    SELECT DISTINCT ON (v.original COLLATE "C") v.* FROM ordered v
    ORDER BY v.original COLLATE "C", v.sequence DESC
), members AS (
    SELECT l.original, k AS member FROM latest l
    CROSS JOIN LATERAL unnest(CASE WHEN cardinality(l.classes) = 0 THEN ARRAY[''] ELSE l.classes END) k
    WHERE l.eligible
), history AS MATERIALIZED (
    SELECT m.member, v.*,
           COALESCE(v.eligible, false) AND CASE WHEN m.member = '' THEN cardinality(v.classes) = 0
                                               ELSE m.member = ANY(v.classes) END AS present
    FROM members m JOIN ordered v ON v.original = m.original
), surviving AS (
    SELECT h.*
    FROM history h
    WHERE h.present AND h.sequence > COALESCE((
        SELECT max(z.sequence) FROM history z WHERE z.original = h.original AND z.member = h.member
          AND NOT z.present), 0)
)
-- Keep every positive registration in the latest uninterrupted class scope.
-- In particular an equity amendment can register that same class again after
-- an intervening definitive end, even if its older original named it already.
-- Removal/cancellation discards older scopes; unread amendments never relist.
SELECT h.known_on, h.filed, h.form, h.adsh,
       COALESCE(array_agg(h.member COLLATE "C" ORDER BY h.member COLLATE "C")
                FILTER (WHERE h.member <> ''), '{}')
FROM surviving h GROUP BY h.original, h.known_on, h.filed, h.form, h.adsh
$fn$;

CREATE OR REPLACE FUNCTION sec_issuer_lines_at(
    p_cik bigint, p_as_of date, p_current boolean, p_source_through date
)
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
    SELECT array_agg(c.class_key ORDER BY c.class_key COLLATE "C"),
           array_agg(c.first_on ORDER BY c.class_key COLLATE "C"),
           array_agg(c.equity ORDER BY c.class_key COLLATE "C")
      INTO keys, firsts, equity
    FROM (
        SELECT o.class_key, min(o.source_available_on) AS first_on,
               bool_or(o.security_kind IN ('equity', 'depositary', 'unknown')) AS equity
        FROM sec_observations_at(p_as_of, p_current) o
        WHERE o.cik = p_cik AND o.source_available_on <= p_source_through
        GROUP BY o.class_key
    ) c;
    IF keys IS NULL THEN
        RETURN;
    END IF;
    comp := ARRAY(SELECT generate_series(1, cardinality(keys)));
    -- classes that appear side by side in one filing, as 'a' || chr(31) || 'b'
    SELECT COALESCE(array_agg(DISTINCT (x.class_key || chr(31) || y.class_key) COLLATE "C" ORDER BY (x.class_key || chr(31) || y.class_key) COLLATE "C"), '{}')
      INTO pairs
    FROM sec_observations_at(p_as_of, p_current) x
    JOIN sec_observations_at(p_as_of, p_current) y
      ON y.adsh = x.adsh AND y.cik = x.cik AND y.class_key <> x.class_key
    WHERE x.cik = p_cik AND x.source_available_on <= p_source_through
      AND y.source_available_on <= p_source_through
      AND x.security_kind IN ('equity', 'depositary', 'unknown')
      AND y.security_kind IN ('equity', 'depositary', 'unknown');
    FOR edge IN
        WITH equity_rows AS (
            SELECT o.adsh, o.class_key, o.ticker_key, o.source_available_on AS on_date,
                   o.accepted, o.filing_equity_classes, o.filing_complete
            FROM sec_observations_at(p_as_of, p_current) o
            WHERE o.cik = p_cik AND o.source_available_on <= p_source_through AND o.security_kind IN ('equity', 'depositary', 'unknown')
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
            JOIN shown y ON y.ticker_key = x.ticker_key AND y.class_key COLLATE "C" > x.class_key COLLATE "C"
            JOIN structure sx ON sx.class_key = x.class_key
            JOIN structure sy ON sy.class_key = y.class_key
            WHERE NOT (x.class_key = '' AND COALESCE(sx.sole_only, true)
                       AND COALESCE(sy.beside_listed, false))
            GROUP BY x.class_key, y.class_key
        ), sole AS (
            SELECT f.key, f.on_date,
                   lag(f.key) OVER (ORDER BY f.on_date, f.accepted NULLS FIRST, f.adsh COLLATE "C")
                       AS prev_key
            FROM (
                SELECT e.adsh, min(e.on_date) AS on_date, max(e.accepted) AS accepted,
                       CASE WHEN count(DISTINCT e.class_key) = 1
                                 AND bool_and(e.filing_equity_classes = 1)
                            THEN min(e.class_key COLLATE "C") END AS key
                FROM equity_rows e
                WHERE e.filing_complete
                GROUP BY e.adsh
            ) f
        ), relabels AS (
            SELECT LEAST(s.prev_key COLLATE "C", s.key COLLATE "C") AS a, GREATEST(s.prev_key COLLATE "C", s.key COLLATE "C") AS b,
                   min(s.on_date) AS on_date, count(*) AS evidence
            FROM sole s
            WHERE s.key IS NOT NULL AND s.prev_key IS NOT NULL AND s.prev_key <> s.key
            GROUP BY LEAST(s.prev_key COLLATE "C", s.key COLLATE "C"), GREATEST(s.prev_key COLLATE "C", s.key COLLATE "C")
        )
        SELECT u.a, u.b, min(u.on_date) AS on_date, sum(u.evidence) AS evidence
        FROM (SELECT * FROM shared UNION ALL SELECT * FROM relabels) u
        GROUP BY u.a, u.b
        ORDER BY min(u.on_date), sum(u.evidence) DESC, u.a COLLATE "C", u.b COLLATE "C"
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
               ORDER BY firsts[m.n], keys[m.n] COLLATE "C"
               LIMIT 1)
           ELSE k.key END
    FROM unnest(keys) WITH ORDINALITY AS k(key, n);
END
$fn$;

-- Caller first checks that the registration names p_label (or names no class
-- and the end's singleton rule applies). Dates bound source chronology;
-- p_as_of/p_current independently control which fact versions are visible.
CREATE OR REPLACE FUNCTION sec_registration_identifies(
    p_cik bigint, p_as_of date, p_current boolean,
    p_registration_on date, p_candidate_on date,
    p_class_key text, p_label text
)
RETURNS boolean
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH bounds AS (
    SELECT 'registration'::text AS which, p_registration_on AS on_date
    UNION ALL
    SELECT 'candidate', p_candidate_on
), visible AS MATERIALIZED (
    SELECT o.adsh, o.class_key, o.security_title, o.source_available_on AS source_on,
           o.accepted, o.filing_complete
    FROM sec_observations_at(p_as_of, p_current) o
    WHERE o.cik = p_cik
      AND o.security_kind IN ('equity', 'depositary', 'unknown')
      AND o.source_available_on <= GREATEST(p_registration_on, p_candidate_on)
), filings AS MATERIALIZED (
    SELECT o.adsh, max(o.source_on) AS source_on, max(o.accepted) AS accepted,
           bool_or(o.filing_complete) AS complete
    FROM visible o GROUP BY o.adsh
), latest_complete AS (
    SELECT b.which, b.on_date, f.adsh, f.source_on, f.accepted
    FROM bounds b
    LEFT JOIN LATERAL (
        SELECT f.* FROM filings f WHERE f.complete AND f.source_on <= b.on_date
        ORDER BY f.source_on DESC, f.accepted DESC NULLS LAST, f.adsh COLLATE "C" DESC
        LIMIT 1
    ) f ON true
), cohort_rows AS MATERIALIZED (
    SELECT b.which, o.class_key, o.source_on, sec_first_label(o.security_title) AS explicit_label
    FROM latest_complete b
    JOIN filings f ON f.source_on <= b.on_date
      AND (b.adsh IS NULL OR f.adsh = b.adsh OR f.source_on > b.source_on
           OR (f.source_on = b.source_on AND
               (COALESCE(f.accepted, '-infinity'::timestamp), f.adsh COLLATE "C")
                 > (COALESCE(b.accepted, '-infinity'::timestamp), b.adsh COLLATE "C")))
    JOIN visible o ON o.adsh = f.adsh
), requests AS (
    SELECT DISTINCT r.class_key, r.source_on AS bound_on
    FROM cohort_rows r WHERE r.explicit_label IS NULL
), historical_labels AS MATERIALIZED (
    SELECT l.* FROM sec_class_label_history(p_cik, p_as_of, p_current,
        (SELECT jsonb_agg(jsonb_build_object('class_key', r.class_key, 'bound_on', r.bound_on)
                          ORDER BY r.class_key COLLATE "C", r.bound_on)
         FROM requests r)) l
), labelled AS MATERIALIZED (
    SELECT r.which, r.class_key, COALESCE(r.explicit_label, h.label) AS label
    FROM cohort_rows r LEFT JOIN historical_labels h
      ON h.class_key = r.class_key AND h.bound_on = r.source_on
), mappings AS MATERIALIZED (
    SELECT b.which, l.class_key, l.line_key
    FROM bounds b CROSS JOIN LATERAL
         sec_issuer_lines_at(p_cik, p_as_of, p_current, b.on_date) l
), target AS (
    SELECT m.line_key FROM mappings m
    WHERE m.which = 'candidate' AND m.class_key = p_class_key
), cohort AS MATERIALIZED (
    SELECT l.which, l.label, own.line_key, candidate.line_key AS candidate_line
    FROM labelled l
    JOIN mappings own ON own.which = l.which AND own.class_key = l.class_key
    LEFT JOIN mappings candidate
      ON candidate.which = 'candidate' AND candidate.class_key = l.class_key
), candidate_scope AS (
    SELECT count(DISTINCT c.line_key) AS lines,
           bool_or(c.line_key = t.line_key) AS includes_target
    FROM cohort c CROSS JOIN target t
    WHERE c.which = 'candidate' AND (p_label IS NULL OR c.label = p_label)
), registration_scope AS (
    SELECT count(DISTINCT c.line_key) AS lines,
           bool_and(COALESCE(c.candidate_line = t.line_key, false)) AS same_line
    FROM cohort c CROSS JOIN target t
    WHERE c.which = 'registration' AND (p_label IS NULL OR c.label = p_label)
)
SELECT COALESCE(p_registration_on <= p_candidate_on AND p_class_key IS NOT NULL
       AND c.lines = 1 AND c.includes_target
       AND ((r.lines = 1 AND r.same_line)
            -- A registration may precede the first cover. The later candidate
            -- must identify exactly one canonical line: the named label when
            -- supplied, or the whole cohort for the existing unnamed fallback.
            OR r.lines = 0), false)
FROM candidate_scope c CROSS JOIN registration_scope r
$fn$;
CREATE OR REPLACE FUNCTION sec_issuer_end_scopes(
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
    issuer_symbols integer,
    instrument_scope jsonb
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH horizon AS (
    SELECT CASE WHEN p_current THEN 'infinity'::date ELSE p_as_of END AS on_date
), all_filings AS MATERIALIZED (
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
), filings AS MATERIALIZED (
    SELECT f.* FROM all_filings f WHERE f.equity_symbols > 0
), cover_rows AS MATERIALIZED (
    -- Keep every cover row for counts; labels are parsed only after the
    -- per-end evidence join selects the relevant listed observations.
    SELECT o.adsh, o.class_key, o.ticker_key, o.security_kind,
           o.security_kind IN ('equity', 'depositary', 'unknown') AS listed,
           o.security_title
    FROM horizon h
    CROSS JOIN LATERAL sec_observations_at(h.on_date, p_current) o
    WHERE o.cik = p_cik
), totals AS MATERIALIZED (
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
), events AS MATERIALIZED (
    SELECT e.*
    FROM horizon h
    CROSS JOIN LATERAL sec_registration_end_events(p_cik, h.on_date, p_current) e
), starts AS MATERIALIZED (
    -- the registrations visible at D (a successor's 8-K12B/8-K12G3 among them)
    SELECT g.* FROM horizon h
    CROSS JOIN LATERAL sec_registration_starts(p_cik, h.on_date, p_current) g
), amendment_history AS MATERIALIZED (
    SELECT e.adsh AS original_adsh, a.*,
           CASE WHEN p_current THEN a.source_available_on ELSE a.available_on END AS known_on,
           row_number() OVER (PARTITION BY e.adsh ORDER BY a.filed DESC, a.adsh COLLATE "C" DESC) AS latest
    FROM events e JOIN sec_registration_events a
      ON a.cik = p_cik AND a.form = e.form || '/A'
     AND a.amendment_effect IN ('restates', 'cancels')
    WHERE CASE WHEN p_current THEN a.retired_on IS NULL
               ELSE a.available_on <= p_as_of
                    AND (a.retired_on IS NULL OR (a.retired_on > p_as_of
                         AND a.retired_reason IS DISTINCT FROM 'parser_correction')) END
      AND e.adsh = (
          SELECT o.adsh FROM sec_registration_events o
          WHERE o.cik = p_cik AND o.form = e.form AND o.filed <= a.filed
            AND CASE WHEN p_current THEN o.retired_on IS NULL
                     ELSE o.available_on <= p_as_of
                          AND (o.retired_on IS NULL OR (o.retired_on > p_as_of
                               AND o.retired_reason IS DISTINCT FROM 'parser_correction')) END
          ORDER BY o.filed DESC, o.adsh COLLATE "C" DESC LIMIT 1)
), versions AS MATERIALIZED (
    SELECT e.adsh, e.form, e.filed, e.available_on, e.restated_on, e.restated_filed,
           true AS effective, 'current'::text AS version_key,
           e.class_kind, e.class_count, e.extinguished, e.venue_kind, e.class_description
    FROM events e
    UNION ALL
    SELECT e.adsh, e.form, e.filed, e.available_on, NULL::date, NULL::date,
           false, 'original', e.original_class_kind, e.original_class_count,
           e.original_extinguished, e.original_venue_kind, e.original_class_description
    FROM events e WHERE e.restated_on IS NOT NULL
    UNION ALL
    SELECT e.adsh, e.form, e.filed, e.available_on, h.known_on, h.filed,
           false, h.adsh,
           CASE WHEN h.amendment_effect = 'cancels' THEN 'other' ELSE h.class_kind END,
           h.class_count, h.extinguished, h.venue_kind,
           CASE WHEN h.amendment_effect = 'cancels' THEN NULL ELSE h.class_description END
    FROM events e JOIN amendment_history h ON h.original_adsh = e.adsh
    WHERE h.latest > 1
), version_lines AS MATERIALIZED (
    SELECT v.adsh, v.version_key, l.class_key, l.line_key
    FROM versions v CROSS JOIN horizon h
    CROSS JOIN LATERAL sec_issuer_lines_at(p_cik, h.on_date, p_current,
        COALESCE(v.restated_filed, v.filed) + 1) l
), judging AS MATERIALIZED (
    -- the complete cover each end is judged against: the latest one (a
    -- 10-K/10-Q-type cover lists every class) filed before it
    SELECT v.adsh, v.effective, v.version_key, pc.adsh AS cover_adsh, pc.source_on AS cover_on,
           pc.classes AS cover_classes
    FROM versions v
    LEFT JOIN LATERAL (
        SELECT f.* FROM filings f WHERE f.complete AND f.source_on < COALESCE(v.restated_filed, v.filed) + 1
        ORDER BY f.source_on DESC, f.accepted DESC NULLS LAST, f.adsh COLLATE "C" DESC
        LIMIT 1
    ) pc ON true
), evidence_raw AS MATERIALIZED (
    -- the issuer's classes when it filed the end: that cover's and those of every
    -- cover filed after it and before the end (an incomplete cover adds the
    -- classes it shows and drops none); without a complete cover, every cover's
    SELECT v.adsh, v.effective, v.version_key, r.class_key, r.ticker_key, r.listed,
           f.source_on AS bound_on,
           CASE WHEN r.listed THEN sec_first_label(r.security_title) END AS explicit_label
    FROM versions v
    JOIN judging j ON j.adsh = v.adsh AND j.version_key = v.version_key
    JOIN filings f ON f.source_on < COALESCE(v.restated_filed, v.filed) + 1
                  AND (j.cover_adsh IS NULL OR f.adsh = j.cover_adsh OR f.source_on > j.cover_on)
    JOIN cover_rows r ON r.adsh = f.adsh
), label_requests AS MATERIALIZED (
    SELECT DISTINCT e.class_key, e.bound_on FROM evidence_raw e
    WHERE e.listed AND e.explicit_label IS NULL
), historical_labels AS MATERIALIZED (
    SELECT l.* FROM sec_class_label_history(
        p_cik, CASE WHEN p_current THEN 'infinity'::date ELSE p_as_of END, p_current,
        (SELECT jsonb_agg(jsonb_build_object('class_key', r.class_key, 'bound_on', r.bound_on)
                          ORDER BY r.class_key COLLATE "C", r.bound_on)
         FROM label_requests r)) l
), evidence AS MATERIALIZED (
    SELECT e.adsh, e.effective, e.version_key, e.class_key, e.ticker_key, e.listed,
           CASE WHEN e.listed THEN COALESCE(e.explicit_label, l.label) END AS label
    FROM evidence_raw e LEFT JOIN historical_labels l
      ON l.class_key = e.class_key AND l.bound_on = e.bound_on
), classes AS MATERIALIZED (
    SELECT e.adsh, e.effective, e.version_key, e.class_key, max(e.label COLLATE "C") AS label,
           count(DISTINCT e.label) AS labels,
           COALESCE(max(line.line_key COLLATE "C"), e.class_key) AS line_key
    FROM evidence e
    LEFT JOIN version_lines line ON line.adsh = e.adsh AND line.version_key = e.version_key AND line.class_key = e.class_key
    WHERE e.listed
    GROUP BY e.adsh, e.effective, e.version_key, e.class_key
), label_symbols AS MATERIALIZED (
    -- a label identifies a class only when one symbol carries it (tracking
    -- stocks of several groups each have a Series A)
    SELECT e.adsh, e.effective, e.version_key, e.label, count(DISTINCT COALESCE(line.line_key, e.class_key)) AS symbols
    FROM evidence e
    LEFT JOIN version_lines line ON line.adsh = e.adsh AND line.version_key = e.version_key AND line.class_key = e.class_key
    WHERE e.listed AND e.label IS NOT NULL
    GROUP BY e.adsh, e.effective, e.version_key, e.label
), version_instrument_clauses AS MATERIALIZED (
    SELECT v.adsh, v.version_key, scope.kind, scope.labels, scope.symbols
    FROM versions v CROSS JOIN LATERAL sec_instrument_scopes(v.class_description) scope
), version_instrument_scopes AS MATERIALIZED (
    -- Each declared label has its own history. Removing a sibling label from
    -- an amendment must not move a retained label's original effective date.
    SELECT DISTINCT c.adsh, c.version_key, c.kind, c.symbols, selected.label AS named_label,
           jsonb_build_array(c.kind, selected.label, c.symbols)::text AS selector_key,
           CASE WHEN selected.label IS NULL THEN '{}'::text[] ELSE ARRAY[selected.label] END AS labels
    FROM version_instrument_clauses c
    CROSS JOIN LATERAL unnest(CASE WHEN cardinality(c.labels) = 0 THEN ARRAY[NULL::text]
                                  ELSE c.labels END) selected(label)
), instrument_current_raw AS MATERIALIZED (
    SELECT DISTINCT v.adsh, v.version_key, r.security_kind, r.class_key, r.ticker_key,
           r.security_title,
           COALESCE(line.line_key, r.class_key) AS line_key, true AS current_cohort
    FROM versions v
    LEFT JOIN LATERAL (
        SELECT f.adsh, f.source_on FROM all_filings f
        WHERE f.complete AND f.source_on < COALESCE(v.restated_filed, v.filed) + 1
        ORDER BY f.source_on DESC, f.accepted DESC NULLS LAST, f.adsh COLLATE "C" DESC LIMIT 1
    ) complete ON true
    JOIN all_filings f ON f.source_on < COALESCE(v.restated_filed, v.filed) + 1
      AND (complete.adsh IS NULL OR f.adsh = complete.adsh OR f.source_on > complete.source_on)
    JOIN cover_rows r ON r.adsh = f.adsh AND NOT r.listed
    LEFT JOIN version_lines line ON line.adsh = v.adsh AND line.version_key = v.version_key AND line.class_key = r.class_key
), historical_instrument_kinds AS MATERIALIZED (
    SELECT DISTINCT scope.adsh, scope.version_key, scope.kind
    FROM version_instrument_scopes scope
    WHERE scope.named_label IS NOT NULL OR cardinality(scope.symbols) > 0
), instrument_historical_raw AS MATERIALIZED (
    -- An explicitly named old instrument may disappear from an unrelated
    -- complete cover before its registration ends. Retain its source-bounded
    -- identity for selector-specific fallback; never expose it to kind-only
    -- wildcard scope or overwrite a positive current selector match.
    SELECT DISTINCT v.adsh, v.version_key, r.security_kind, r.class_key, r.ticker_key,
           r.security_title, COALESCE(line.line_key, r.class_key) AS line_key, false AS current_cohort
    FROM versions v JOIN historical_instrument_kinds kind
      ON kind.adsh = v.adsh AND kind.version_key = v.version_key
    JOIN all_filings f ON f.source_on < COALESCE(v.restated_filed, v.filed) + 1
    JOIN cover_rows r ON r.adsh = f.adsh AND NOT r.listed AND r.security_kind = kind.kind
    LEFT JOIN version_lines line ON line.adsh = v.adsh AND line.version_key = v.version_key
                                AND line.class_key = r.class_key
), instrument_evidence_raw AS MATERIALIZED (
    SELECT * FROM instrument_current_raw
    UNION ALL
    SELECT * FROM instrument_historical_raw
), instrument_label_inputs AS MATERIALIZED (
    -- The new identity parser is immutable. Parse each distinct input once,
    -- not once per repeated cover/end-version join row before DISTINCT.
    SELECT DISTINCT r.security_title, r.class_key, r.security_kind
    FROM instrument_evidence_raw r
), instrument_labels AS MATERIALIZED (
    SELECT r.*, sec_instrument_label(r.security_title, r.class_key, r.security_kind) AS label
    FROM instrument_label_inputs r
), instrument_evidence AS MATERIALIZED (
    SELECT r.adsh, r.version_key, r.security_kind, r.class_key, r.ticker_key,
           label.label, r.line_key, bool_or(r.current_cohort) AS current_cohort
    FROM instrument_evidence_raw r JOIN instrument_labels label
      ON label.class_key = r.class_key AND label.security_kind = r.security_kind
     AND label.security_title IS NOT DISTINCT FROM r.security_title
    GROUP BY r.adsh, r.version_key, r.security_kind, r.class_key, r.ticker_key, label.label, r.line_key
), instrument_scope_presence AS MATERIALIZED (
    SELECT scope.adsh, scope.version_key, scope.selector_key,
           COALESCE(bool_or(
               ((scope.named_label IS NOT NULL AND r.label = scope.named_label)
                OR (cardinality(scope.symbols) > 0 AND r.ticker_key = ANY(scope.symbols)))
               AND (scope.named_label IS NULL OR r.label IS NULL OR r.label = scope.named_label)
               AND (cardinality(scope.symbols) = 0 OR r.ticker_key = ANY(scope.symbols))), false) AS has_current_match
    FROM version_instrument_scopes scope LEFT JOIN instrument_evidence r
      ON r.adsh = scope.adsh AND r.version_key = scope.version_key AND r.security_kind = scope.kind
     AND r.current_cohort
    GROUP BY scope.adsh, scope.version_key, scope.selector_key
), instrument_candidates AS MATERIALIZED (
    SELECT r.*, scope.selector_key, scope.labels AS named_labels, scope.symbols AS named_symbols, scope.named_label
    FROM version_instrument_scopes scope JOIN instrument_scope_presence present
      ON present.adsh = scope.adsh AND present.version_key = scope.version_key AND present.selector_key = scope.selector_key
    JOIN instrument_evidence r ON r.adsh = scope.adsh AND r.version_key = scope.version_key AND r.security_kind = scope.kind
    WHERE r.current_cohort OR (
        NOT present.has_current_match
        AND ((scope.named_label IS NOT NULL AND r.label = scope.named_label)
             OR (cardinality(scope.symbols) > 0 AND r.ticker_key = ANY(scope.symbols)))
        AND (scope.named_label IS NULL OR r.label IS NULL OR r.label = scope.named_label)
        AND (cardinality(scope.symbols) = 0 OR r.ticker_key = ANY(scope.symbols)))
), instrument_kind_counts AS MATERIALIZED (
    SELECT r.adsh, r.version_key, r.security_kind, r.selector_key,
           count(DISTINCT (r.line_key, COALESCE(r.label, 'symbol:' || r.ticker_key))) AS lines
    FROM instrument_candidates r GROUP BY r.adsh, r.version_key, r.security_kind, r.selector_key
), instrument_label_counts AS MATERIALIZED (
    SELECT r.adsh, r.version_key, r.security_kind, r.selector_key, r.label,
           count(DISTINCT (r.line_key, COALESCE(r.label, 'symbol:' || r.ticker_key))) AS lines
    FROM instrument_candidates r WHERE r.label IS NOT NULL
    GROUP BY r.adsh, r.version_key, r.security_kind, r.selector_key, r.label
), instrument_symbol_counts AS MATERIALIZED (
    SELECT r.adsh, r.version_key, r.security_kind, r.selector_key, r.ticker_key,
           count(DISTINCT (r.line_key, COALESCE(r.label, 'symbol:' || r.ticker_key))) AS lines
    FROM instrument_candidates r GROUP BY r.adsh, r.version_key, r.security_kind, r.selector_key, r.ticker_key
), instrument_roles AS MATERIALIZED (
    SELECT v.adsh, v.version_key, r.security_kind, r.class_key, r.label, r.ticker_key,
           jsonb_build_array(r.class_key, r.label,
               CASE WHEN selection.value = 'symbol' THEN r.ticker_key END,
               selection.value, r.named_label)::text AS member,
           CASE
               -- No candidate establishes the declared label's identity. Keep
               -- the ambiguity as a temporary kind-wide closure, never as an
               -- exclusion or an identified (potentially definitive) end.
               WHEN selection.value = 'unmatched_label' THEN 'tentative'
               WHEN cardinality(r.named_labels) > 0 AND r.label IS NOT NULL AND NOT r.label = ANY(r.named_labels)
                    THEN 'excluded'
               WHEN cardinality(r.named_symbols) > 0 AND NOT r.ticker_key = ANY(r.named_symbols) THEN 'excluded'
               WHEN cardinality(r.named_symbols) > 0 AND symbol_count.lines = 1 THEN 'identified'
               WHEN cardinality(r.named_labels) > 0 AND r.label = ANY(r.named_labels) AND label_count.lines = 1 THEN 'identified'
               WHEN cardinality(r.named_labels) = 0 AND cardinality(r.named_symbols) = 0 AND kind_count.lines = 1 THEN 'identified'
               ELSE 'tentative'
           END AS role
    FROM instrument_candidates r JOIN versions v ON v.adsh = r.adsh AND v.version_key = r.version_key
    JOIN instrument_kind_counts kind_count ON kind_count.adsh = r.adsh AND kind_count.version_key = r.version_key
      AND kind_count.security_kind = r.security_kind AND kind_count.selector_key = r.selector_key
    LEFT JOIN instrument_label_counts label_count ON label_count.adsh = r.adsh AND label_count.version_key = r.version_key
      AND label_count.security_kind = r.security_kind AND label_count.selector_key = r.selector_key AND label_count.label = r.label
    LEFT JOIN instrument_label_counts named_count ON named_count.adsh = r.adsh AND named_count.version_key = r.version_key
      AND named_count.security_kind = r.security_kind AND named_count.selector_key = r.selector_key AND named_count.label = r.named_label
    JOIN instrument_symbol_counts symbol_count ON symbol_count.adsh = r.adsh AND symbol_count.version_key = r.version_key
      AND symbol_count.security_kind = r.security_kind AND symbol_count.selector_key = r.selector_key AND symbol_count.ticker_key = r.ticker_key
    CROSS JOIN LATERAL (
        SELECT CASE WHEN r.named_label IS NOT NULL AND cardinality(r.named_symbols) = 0
                         AND COALESCE(named_count.lines, 0) = 0 THEN 'unmatched_label'
                    WHEN r.named_label IS NOT NULL AND r.label = r.named_label AND label_count.lines = 1
                    THEN 'label'
                    WHEN cardinality(r.named_symbols) > 0 THEN 'symbol'
                    WHEN r.named_label IS NOT NULL THEN 'label' ELSE 'kind' END AS value
    ) selection
), judged AS MATERIALIZED (
    SELECT v.*,
           sec_named_classes(v.class_description) AS named,
           sec_named_kinds(v.class_description) AS named_kinds,
           COALESCE((SELECT CASE WHEN v.class_kind = 'equity'
                                 THEN count(DISTINCT e.ticker_key) FILTER (WHERE e.listed)
                                 ELSE count(DISTINCT e.ticker_key) END
                     FROM evidence e WHERE e.adsh = v.adsh AND e.version_key = v.version_key), 0)
               AS prior_symbols,
           GREATEST(COALESCE(j.cover_classes, 1),
                    (SELECT count(DISTINCT c.line_key) FROM classes c
                     WHERE c.adsh = v.adsh AND c.version_key = v.version_key), 1) AS prior_classes,
           -- a Form 15-12G or 15-15D (or 15F) ends a registration, which may be of an
           -- unlisted class; the other forms end listed classes
           replace(v.form, '15F-', '15-') IN ('15-12G', '15-15D') AS registration_end
    FROM versions v
    JOIN judging j ON j.adsh = v.adsh AND j.version_key = v.version_key
), roles AS MATERIALIZED (
    -- what an end says of each listed class (the admission rule: any ambiguity
    -- refuses). 'identified': it names the class by a label one symbol carries;
    -- or it names no class and counts all of them; or the issuer lists one
    -- symbol (and, for a 15-12G/15-15D, counts one class). 'excluded': it names
    -- other classes and this one's label is known. Else 'tentative': it may
    -- concern the class but does not say so.
    SELECT j.adsh, j.effective, j.version_key, c.class_key, c.label,
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
    JOIN classes c ON c.adsh = j.adsh AND c.version_key = j.version_key
                  AND j.class_kind IS DISTINCT FROM 'other'
    LEFT JOIN label_symbols ls
      ON ls.adsh = c.adsh AND ls.version_key = c.version_key AND ls.label = c.label
), role_history AS MATERIALIZED (
    -- Include absent roles: a removal/cancellation must break continuity before
    -- a later amendment reintroduces the same class.
    SELECT v.adsh, v.version_key, m.class_key,
           CASE WHEN v.registration_end OR v.venue_kind IS DISTINCT FROM 'secondary' THEN r.role END AS role,
           COALESCE(v.restated_filed, v.filed) AS scope_filed,
           row_number() OVER (PARTITION BY v.adsh, m.class_key
               ORDER BY COALESCE(v.restated_filed, v.filed),
                        CASE v.version_key WHEN 'original' THEN 0 WHEN 'current' THEN 2 ELSE 1 END,
                        v.version_key COLLATE "C") AS sequence
    FROM judged v JOIN (SELECT DISTINCT adsh, class_key FROM roles) m ON m.adsh = v.adsh
    LEFT JOIN roles r ON r.adsh = v.adsh AND r.version_key = v.version_key AND r.class_key = m.class_key
), role_dates AS MATERIALIZED (
    SELECT h.*, (SELECT min(z.scope_filed) FROM role_history z
                 WHERE z.adsh = h.adsh AND z.class_key = h.class_key AND z.sequence <= h.sequence
                   AND z.role = h.role AND NOT EXISTS (
                       SELECT 1 FROM role_history gap WHERE gap.adsh = h.adsh AND gap.class_key = h.class_key
                         AND gap.sequence BETWEEN z.sequence AND h.sequence
                         AND gap.role IS DISTINCT FROM h.role)) AS first_filed
    FROM role_history h WHERE h.role IN ('identified', 'tentative')
), closed AS MATERIALIZED (
    -- the classes an end closes: those it concerns that no registration carries
    -- on. A registration from 30 days before to 10 days after the end carries on
    -- the classes it names, or, naming none, the issuer's one symbol: a
    -- successor's registration of the CIK's class (8-K12B, 8-K12G3 under the same
    -- CIK) whatever the end, a transfer registration across a delisting unless the
    -- class was extinguished (a Form 15-12G or 15-15D ends the registration
    -- whatever is registered).
    SELECT r.*
    FROM roles r
    JOIN role_dates scope ON scope.adsh = r.adsh AND scope.version_key = r.version_key AND scope.class_key = r.class_key
    JOIN judged j ON j.adsh = r.adsh AND j.version_key = r.version_key
    LEFT JOIN label_symbols unique_label ON unique_label.adsh = r.adsh AND unique_label.version_key = r.version_key AND unique_label.label = r.label
    WHERE r.role IN ('identified', 'tentative')
      AND NOT EXISTS (
          SELECT 1 FROM starts g
          WHERE g.filed BETWEEN scope.first_filed - 30 AND scope.first_filed + 10
            AND (g.form IN ('8-K12B', '8-K12G3')
                 OR NOT (j.registration_end OR COALESCE(j.extinguished, false)))
            AND CASE WHEN ((r.label IS NOT NULL AND r.label = ANY(g.classes) AND unique_label.symbols = 1)
                 OR (cardinality(g.classes) = 0 AND j.prior_symbols = 1)) THEN
                sec_registration_identifies(p_cik, CASE WHEN p_current THEN 'infinity'::date ELSE p_as_of END,
                    p_current, g.filed + 1, GREATEST(scope.first_filed + 1, g.filed + 1), r.class_key,
                    CASE WHEN cardinality(g.classes) = 0 THEN NULL ELSE r.label END)
                ELSE false END)
), applies AS MATERIALIZED (
    SELECT j.*,
           ((EXISTS (SELECT 1 FROM instrument_roles instrument WHERE instrument.adsh = j.adsh AND instrument.version_key = j.version_key AND instrument.role <> 'excluded'))
            OR (j.class_kind IS DISTINCT FROM 'other' AND EXISTS (
                SELECT 1 FROM closed c WHERE c.adsh = j.adsh AND c.version_key = j.version_key)))
           AND (j.registration_end OR j.venue_kind IS DISTINCT FROM 'secondary') AS applying,
           -- NULL: every listed class, each identified and closed
           CASE WHEN j.class_kind = 'other' THEN '{}'::text[]
                WHEN NOT EXISTS (
                         SELECT 1 FROM roles r
                         WHERE r.adsh = j.adsh AND r.version_key = j.version_key
                           AND NOT EXISTS (
                               SELECT 1 FROM closed c
                               WHERE c.adsh = r.adsh AND c.version_key = r.version_key
                                 AND c.class_key = r.class_key AND c.role = 'identified'))
                THEN NULL
                ELSE ARRAY(SELECT c.class_key FROM closed c
                           WHERE c.adsh = j.adsh AND c.version_key = j.version_key
                           ORDER BY c.class_key COLLATE "C")
           END AS class_keys,
           ARRAY(SELECT c.class_key FROM closed c
                 WHERE c.adsh = j.adsh AND c.version_key = j.version_key AND c.role = 'tentative'
                 ORDER BY c.class_key COLLATE "C") AS tentative_keys,
           j.class_kind = 'equity' AND j.class_count >= j.prior_classes AS whole_equity,
           j.form IN ('25', '25-NSE')
               AND j.class_kind = 'equity'
               AND j.venue_kind IS DISTINCT FROM 'secondary' AS equity_delisting
    FROM judged j
), scope_members AS MATERIALIZED (
    SELECT a.adsh, 'class'::text AS domain, c.class_key AS member, c.role
    FROM applies a JOIN closed c ON c.adsh = a.adsh AND c.version_key = a.version_key
    WHERE a.effective AND a.applying
    UNION ALL
    SELECT DISTINCT a.adsh, 'instrument:' || instrument.security_kind, instrument.member, instrument.role
    FROM applies a JOIN instrument_roles instrument ON instrument.adsh = a.adsh AND instrument.version_key = a.version_key
    WHERE a.effective AND a.applying AND instrument.role <> 'excluded'
), scope_history AS MATERIALIZED (
    SELECT m.*, v.version_key,
           COALESCE(v.restated_filed, v.filed) + 1 AS scope_on,
           COALESCE(v.restated_on, v.available_on) AS scope_available,
           row_number() OVER (PARTITION BY m.adsh, m.domain, m.member
               ORDER BY COALESCE(v.restated_filed, v.filed),
                        CASE v.version_key WHEN 'original' THEN 0 WHEN 'current' THEN 2 ELSE 1 END,
                        v.version_key COLLATE "C") AS sequence,
           v.applying AND CASE WHEN m.domain = 'class' THEN EXISTS (
               SELECT 1 FROM closed c WHERE c.adsh = v.adsh AND c.version_key = v.version_key
                 AND c.class_key = m.member AND c.role = m.role)
               ELSE EXISTS (SELECT 1 FROM instrument_roles instrument
                    WHERE instrument.adsh = v.adsh AND instrument.version_key = v.version_key
                      AND 'instrument:' || instrument.security_kind = m.domain
                      AND instrument.member = m.member AND instrument.role = m.role) END AS present
    FROM scope_members m JOIN applies v ON v.adsh = m.adsh
), surviving_scope AS (
    SELECT DISTINCT ON (h.adsh COLLATE "C", h.domain COLLATE "C", h.member COLLATE "C") h.*
    FROM scope_history h
    WHERE h.present AND h.sequence > COALESCE((
        SELECT max(z.sequence) FROM scope_history z
        WHERE z.adsh = h.adsh AND z.domain = h.domain AND z.member = h.member AND NOT z.present), 0)
    ORDER BY h.adsh COLLATE "C", h.domain COLLATE "C", h.member COLLATE "C", h.sequence
), dated_scopes AS (
    SELECT h.adsh, h.scope_on, h.scope_available, h.domain,
           ARRAY(SELECT DISTINCT CASE WHEN h.domain = 'class' THEN member ELSE member::jsonb ->> 0 END COLLATE "C"
                 FROM unnest(array_agg(h.member)) member ORDER BY 1) AS scope_keys,
           ARRAY(SELECT DISTINCT CASE WHEN h.domain = 'class' THEN member ELSE member::jsonb ->> 0 END COLLATE "C"
                 FROM unnest(array_agg(h.member) FILTER (WHERE h.role = 'tentative')) member ORDER BY 1) AS scope_tentative,
           CASE WHEN h.domain = 'class' THEN '{}'::text[] ELSE ARRAY[substring(h.domain FROM 12)] END AS scope_kinds,
           bool_or(h.role = 'identified') AS any_identified,
           CASE WHEN h.domain = 'class' THEN '[]'::jsonb ELSE
               jsonb_agg(jsonb_build_object('class_key', h.member::jsonb ->> 0,
                   'label', h.member::jsonb ->> 1, 'ticker_key', h.member::jsonb ->> 2,
                   'mode', h.member::jsonb ->> 3, 'named_label', h.member::jsonb ->> 4, 'role', h.role)
                   ORDER BY h.member COLLATE "C") FILTER (WHERE h.domain <> 'class') END AS instrument_scope
    FROM surviving_scope h GROUP BY h.adsh, h.scope_on, h.scope_available, h.domain
), current_ends AS (
    SELECT a.*,
           CASE WHEN d.domain = 'class' AND a.restated_on IS NULL THEN a.class_keys ELSE d.scope_keys END AS scope_keys,
           d.scope_tentative, d.scope_kinds, d.scope_on, d.scope_available, d.any_identified, d.instrument_scope,
           d.domain <> 'class' AS instrument_only
    FROM applies a JOIN dated_scopes d ON d.adsh = a.adsh
    WHERE a.effective AND a.applying
), registrations AS (
    SELECT a.*, ARRAY(
        SELECT c.class_key FROM classes c
        JOIN label_symbols ls ON ls.adsh = c.adsh AND ls.version_key = c.version_key
                              AND ls.label = c.label AND ls.symbols = 1
        WHERE NOT a.instrument_only AND c.adsh = a.adsh AND c.effective AND c.labels = 1
          AND (a.scope_keys IS NULL OR c.class_key = ANY(a.scope_keys))
          AND EXISTS (
              SELECT 1 FROM starts g JOIN sec_registration_events source
                ON source.cik = p_cik AND source.adsh = g.adsh
               AND source.class_kind = 'equity'
               AND CASE WHEN p_current THEN source.retired_on IS NULL
                        ELSE source.available_on <= p_as_of
                             AND (source.retired_on IS NULL OR (source.retired_on > p_as_of
                                  AND source.retired_reason IS DISTINCT FROM 'parser_correction')) END
              WHERE g.form IN ('8-A12B', '8-A12G')
                AND g.filed BETWEEN a.scope_on - 31 AND a.scope_on - 1
                AND g.available_on <= a.scope_on
                AND CASE WHEN c.label = ANY(g.classes) THEN
                    sec_registration_identifies(p_cik, CASE WHEN p_current THEN 'infinity'::date ELSE p_as_of END,
                        p_current, g.filed + 1, a.scope_on, c.class_key, c.label)
                    ELSE false END)
        ORDER BY c.class_key COLLATE "C") AS reissued_keys
    FROM current_ends a
)
SELECT a.scope_available,
       a.filed, a.form, a.adsh,
       CASE
           WHEN scope.reissued THEN false
           -- an end that identifies no class is never definitive
           WHEN NOT a.any_identified THEN false
           WHEN a.instrument_only THEN a.form = '25-NSE' AND COALESCE(a.extinguished, false)
           WHEN a.scope_keys IS NOT NULL AND a.scope_keys <@ a.scope_tentative THEN false
           -- an end of some of the listed classes: definitive for those it
           -- identifies only when the 25-NSE says they were extinguished
           WHEN a.class_keys IS NOT NULL
               THEN a.form = '25-NSE' AND COALESCE(a.extinguished, false)
           ELSE a.whole_equity AND NOT COALESCE((
               SELECT after_end.total BETWEEN 0.8 * before_end.total AND 1.25 * before_end.total
               FROM (SELECT t.total FROM totals t WHERE t.source_on < a.filed + 1
                     ORDER BY t.source_on DESC, t.accepted DESC NULLS LAST, t.adsh COLLATE "C" DESC
                     LIMIT 1) before_end,
                    (SELECT t.total FROM totals t
                     WHERE t.source_on >= a.filed + 1 AND t.stated_on >= a.filed
                     ORDER BY t.source_on, t.accepted NULLS FIRST, t.adsh COLLATE "C"
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
       a.scope_on,
       scope.class_keys, CASE WHEN a.instrument_only THEN 'other' ELSE a.class_kind END,
       a.scope_kinds,
       a.scope_tentative, a.prior_symbols::integer, a.instrument_scope
FROM registrations a
CROSS JOIN LATERAL (
    SELECT a.scope_keys AS class_keys, false AS reissued
    WHERE cardinality(a.reissued_keys) = 0
    UNION ALL
    SELECT ARRAY(SELECT c.class_key FROM classes c WHERE c.adsh = a.adsh AND c.effective
                  AND (a.scope_keys IS NULL OR c.class_key = ANY(a.scope_keys))
                  AND NOT c.class_key = ANY(a.reissued_keys) ORDER BY c.class_key COLLATE "C"), false
    WHERE cardinality(a.reissued_keys) > 0
    UNION ALL
    SELECT a.reissued_keys, true WHERE cardinality(a.reissued_keys) > 0
) scope
WHERE (scope.class_keys IS NULL OR cardinality(scope.class_keys) > 0
       OR cardinality(a.scope_kinds) > 0) AND a.scope_available <= p_as_of
$fn$;

CREATE OR REPLACE FUNCTION sec_issuer_end_events(
    p_cik bigint, p_as_of date, p_current boolean DEFAULT false
)
RETURNS TABLE (available_on date, filed date, form text, adsh text, definitive boolean,
    effective_on date, class_keys text[], class_kind text, named_kinds text[],
    tentative_keys text[], issuer_symbols integer)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
SELECT e.available_on, e.filed, e.form, e.adsh, e.definitive, e.effective_on,
       e.class_keys, e.class_kind, e.named_kinds, e.tentative_keys, e.issuer_symbols
FROM sec_issuer_end_scopes(p_cik, p_as_of, p_current) e
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
           o.source_available_on AS known_on,
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
                      r.adsh COLLATE "C" DESC, r.class_key COLLATE "C"))[1] AS class_key,
           (array_agg(r.security_kind ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh COLLATE "C" DESC, r.class_key COLLATE "C"))[1] AS security_kind,
           array_agg(DISTINCT (r.class_key) COLLATE "C" ORDER BY (r.class_key) COLLATE "C") AS classes,
           bool_or(r.security_kind IN ('equity', 'depositary', 'unknown')) AS listed,
           array_agg(DISTINCT (r.security_kind) COLLATE "C" ORDER BY (r.security_kind) COLLATE "C") AS kinds,
           COALESCE((array_agg(r.filing_equity_classes = 1 ORDER BY r.known_on DESC,
                               r.accepted DESC NULLS LAST, r.adsh COLLATE "C" DESC)
                     FILTER (WHERE r.filing_complete))[1], false) AS sole
    FROM relevant r
    GROUP BY r.cik
), candidate_rows_unlabelled AS MATERIALIZED (
    SELECT p.cik, f.adsh, f.class_key, f.security_kind, f.accepted, f.filing_complete,
           f.filing_equity_classes, f.security_title, f.ticker_key,
           f.source_available_on AS known_on,
           f.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g') AS shows
    FROM per_cik p
    JOIN sec_observations_at(p_as_of, p_current) f
      ON f.cik = p.cik
     AND (f.class_key = ANY(p.classes)
          OR (f.filing_complete AND f.security_kind IN ('equity', 'depositary', 'unknown')
              AND (p.sole OR f.filing_equity_classes = 1)))

    WHERE f.security_kind IN ('equity', 'depositary', 'unknown')), label_inputs AS MATERIALIZED (
    SELECT r.*, CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown')
                     THEN sec_first_label(r.security_title) END AS explicit_label
    FROM candidate_rows_unlabelled r
), label_requests AS (
    SELECT requested.cik,
           jsonb_agg(jsonb_build_object('class_key', requested.class_key, 'bound_on', requested.known_on)
                     ORDER BY requested.class_key COLLATE "C", requested.known_on) AS requests
    FROM (SELECT DISTINCT r.cik AS cik, r.class_key, r.known_on
          FROM label_inputs r
          WHERE r.security_kind IN ('equity', 'depositary', 'unknown') AND r.explicit_label IS NULL) requested
    GROUP BY requested.cik
), historical_labels AS MATERIALIZED (
    SELECT request.cik, label.* FROM label_requests request
    CROSS JOIN LATERAL sec_class_label_history(request.cik, p_as_of, p_current, request.requests) label
), candidate_rows AS MATERIALIZED (
    SELECT r.*, CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown')
                    THEN COALESCE(r.explicit_label, history.label)
                    ELSE sec_instrument_label(r.security_title, r.class_key, r.security_kind) END AS label
    FROM label_inputs r LEFT JOIN historical_labels history
      ON history.cik = r.cik AND history.class_key = r.class_key AND history.bound_on = r.known_on
), candidates AS (
    -- labels: the classes of the rows by which the candidate states the hold (its
    -- rows showing the ticker, else its rows): only a registration naming one of
    -- them, never another class the ticker once showed on, reopens it
    SELECT r.cik, r.adsh, max(r.known_on) AS known_on, max(r.accepted) AS accepted,
           bool_or(r.shows) AS shows,
           CASE WHEN bool_or(r.shows)
                THEN array_remove(array_agg(DISTINCT (r.label) COLLATE "C" ORDER BY (r.label) COLLATE "C")
                                  FILTER (WHERE r.shows), NULL)
                ELSE array_remove(array_agg(DISTINCT (r.label) COLLATE "C" ORDER BY (r.label) COLLATE "C"),
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
                        ORDER BY x.known_on DESC, x.accepted DESC NULLS LAST, x.adsh COLLATE "C" DESC
                        LIMIT 1)
          AND NOT EXISTS (SELECT 1 FROM candidate_rows y
                          WHERE y.cik = c.cik AND y.adsh = c.adsh AND y.class_key = t.class_key))
), ends AS MATERIALIZED (
    SELECT p.cik, e.effective_on, e.filed, e.form, e.adsh, e.definitive, e.class_keys,
           e.tentative_keys, e.class_kind, e.named_kinds, e.issuer_symbols, e.instrument_scope,
           (SELECT min(c.known_on) FROM candidates c
            WHERE c.cik = p.cik AND c.shows AND c.known_on >= e.effective_on) AS first_post_on
    FROM per_cik p
    CROSS JOIN LATERAL sec_issuer_end_scopes(p.cik, p_as_of, p_current) e
), starts AS MATERIALIZED (
    SELECT p.cik, r.* FROM per_cik p
    CROSS JOIN LATERAL sec_registration_starts(p.cik, p_as_of, p_current) r
), row_closures AS MATERIALIZED (
    -- Close each statement row independently. Different scopes (or distinct
    -- ends) may close A and B on different dates while both carry the ticker.
    -- A stale post-definitive row remains closed; a registration only rescues
    -- the class of that row, never another class sharing its ticker.
    SELECT r.cik, r.adsh, r.class_key, r.security_kind, r.ticker_key, d.effective_on, d.adsh AS end_adsh, role.value,
           d.effective_on <= c.known_on AS blocked
    FROM candidate_rows r JOIN candidates c ON c.cik = r.cik AND c.adsh = r.adsh
    JOIN ends d ON d.cik = r.cik
    CROSS JOIN LATERAL (SELECT sec_end_role(d.class_keys, d.tentative_keys,
        d.class_kind, d.named_kinds, r.class_key, r.security_kind, r.label, r.ticker_key, d.instrument_scope) AS value) role
    WHERE (r.shows OR NOT c.shows) AND role.value IS NOT NULL
      AND (d.effective_on > c.known_on OR (
          ((d.definitive AND role.value = 'identified' AND EXISTS (
              SELECT 1 FROM relevant prior WHERE prior.cik = r.cik AND prior.known_on < d.effective_on
                AND (prior.security_kind = r.security_kind
                     OR (prior.security_kind IN ('equity', 'depositary', 'unknown')
                         AND r.security_kind IN ('equity', 'depositary', 'unknown')))))
           OR EXISTS (SELECT 1 FROM per_cik other WHERE other.cik <> r.cik
                      AND other.first_on BETWEEN d.effective_on - 30 AND d.first_post_on))
          AND NOT EXISTS (
              SELECT 1 FROM starts registration
              WHERE registration.cik = r.cik AND registration.filed >= d.effective_on
                AND registration.available_on <= c.known_on
                AND r.security_kind IN ('equity', 'depositary', 'unknown')
                AND CASE WHEN (r.label = ANY(registration.classes)
                     OR (cardinality(registration.classes) = 0 AND d.issuer_symbols = 1)) THEN
                    sec_registration_identifies(r.cik, p_as_of, p_current, registration.filed + 1,
                        r.known_on, r.class_key,
                        CASE WHEN cardinality(registration.classes) = 0 THEN NULL ELSE r.label END)
                    ELSE false END)))
), closes AS MATERIALIZED (
    -- EVERY candidate row needs SOME closure by this bound. Requiring a
    -- single end row to close all rows fails after a per-class amendment split.
    SELECT e.cik, e.adsh AS end_adsh, e.effective_on, e.class_keys, c.adsh,
           bool_and(EXISTS (SELECT 1 FROM row_closures z
               WHERE z.cik = r.cik AND z.adsh = r.adsh AND z.class_key = r.class_key
                 AND z.security_kind = r.security_kind AND z.ticker_key = r.ticker_key AND z.effective_on <= e.effective_on)) AS closed,
           bool_and(EXISTS (SELECT 1 FROM row_closures z
               WHERE z.cik = r.cik AND z.adsh = r.adsh AND z.class_key = r.class_key
                 AND z.security_kind = r.security_kind AND z.ticker_key = r.ticker_key AND z.effective_on <= e.effective_on
                 AND z.value = 'identified')) AS identified,
           bool_and(EXISTS (SELECT 1 FROM row_closures z
               WHERE z.cik = r.cik AND z.adsh = r.adsh AND z.class_key = r.class_key
                 AND z.security_kind = r.security_kind AND z.ticker_key = r.ticker_key AND z.effective_on <= e.effective_on
                 AND z.blocked)) AS blocked
    FROM ends e JOIN candidates c ON c.cik = e.cik
    JOIN candidate_rows r ON r.cik = c.cik AND r.adsh = c.adsh AND (r.shows OR NOT c.shows)
    WHERE EXISTS (SELECT 1 FROM row_closures z WHERE z.cik = c.cik AND z.adsh = c.adsh
                  AND z.end_adsh = e.adsh AND z.effective_on = e.effective_on)
    GROUP BY e.cik, e.adsh, e.effective_on, e.class_keys, c.adsh
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
        JOIN closes k ON k.cik = e.cik AND k.end_adsh = e.adsh AND k.effective_on = e.effective_on AND k.class_keys IS NOT DISTINCT FROM e.class_keys AND k.adsh = c.adsh
        WHERE e.cik = c.cik AND e.effective_on <= c.known_on AND k.blocked)
    ORDER BY d.on_date, c.cik, c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh COLLATE "C" DESC
), last_end AS (
    -- the latest end, effective by the date, after the statement that closes every
    -- class it shows the ticker on (an end of another class leaves the hold)
    SELECT DISTINCT ON (s.on_date, e.cik) s.on_date, e.*
    FROM ends e
    JOIN statement s ON s.cik = e.cik
    JOIN closes k ON k.cik = e.cik AND k.end_adsh = e.adsh AND k.effective_on = e.effective_on AND k.class_keys IS NOT DISTINCT FROM e.class_keys AND k.adsh = s.adsh
    WHERE e.effective_on > s.known_on AND e.effective_on <= s.on_date AND k.closed
    ORDER BY s.on_date, e.cik, e.effective_on DESC, k.identified DESC, e.adsh COLLATE "C" DESC
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
           o.source_available_on AS known_on,
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
                      r.adsh COLLATE "C" DESC, r.class_key COLLATE "C"))[1] AS class_key,
           (array_agg(r.security_kind ORDER BY r.known_on DESC, r.accepted DESC NULLS LAST,
                      r.adsh COLLATE "C" DESC, r.class_key COLLATE "C"))[1] AS security_kind,
           array_agg(DISTINCT (r.class_key) COLLATE "C" ORDER BY (r.class_key) COLLATE "C") AS classes,
           bool_or(r.security_kind IN ('equity', 'depositary', 'unknown')) AS listed,
           array_agg(DISTINCT (r.security_kind) COLLATE "C" ORDER BY (r.security_kind) COLLATE "C") AS kinds,
           COALESCE((array_agg(r.filing_equity_classes = 1 ORDER BY r.known_on DESC,
                               r.accepted DESC NULLS LAST, r.adsh COLLATE "C" DESC)
                     FILTER (WHERE r.filing_complete))[1], false) AS sole
    FROM relevant r
    GROUP BY r.cik
), candidate_rows_unlabelled AS MATERIALIZED (
    SELECT p.cik, f.adsh, f.class_key, f.security_kind, f.accepted, f.filing_complete,
           f.filing_equity_classes, f.security_title, f.ticker_key,
           f.source_available_on AS known_on,
           f.ticker_key = regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g') AS shows
    FROM per_cik p
    JOIN sec_observations_at(p_as_of, p_current) f
      ON f.cik = p.cik
     AND (f.class_key = ANY(p.classes)
          OR (f.filing_complete AND f.security_kind IN ('equity', 'depositary', 'unknown')
              AND (p.sole OR f.filing_equity_classes = 1)))

    WHERE f.security_kind IN ('equity', 'depositary', 'unknown')
       OR EXISTS (SELECT 1 FROM relevant kept
                  WHERE kept.cik = f.cik AND kept.class_key = f.class_key
                    AND kept.security_kind = f.security_kind
                    AND (kept.adsh = f.adsh OR (
                         f.ticker_key <> regexp_replace(upper(p_ticker), '[^A-Z0-9]', '', 'g')
                         AND kept.known_on <= f.source_available_on)))), label_inputs AS MATERIALIZED (
    SELECT r.*, CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown')
                     THEN sec_first_label(r.security_title) END AS explicit_label
    FROM candidate_rows_unlabelled r
), label_requests AS (
    SELECT requested.cik,
           jsonb_agg(jsonb_build_object('class_key', requested.class_key, 'bound_on', requested.known_on)
                     ORDER BY requested.class_key COLLATE "C", requested.known_on) AS requests
    FROM (SELECT DISTINCT r.cik AS cik, r.class_key, r.known_on
          FROM label_inputs r
          WHERE r.security_kind IN ('equity', 'depositary', 'unknown') AND r.explicit_label IS NULL) requested
    GROUP BY requested.cik
), historical_labels AS MATERIALIZED (
    SELECT request.cik, label.* FROM label_requests request
    CROSS JOIN LATERAL sec_class_label_history(request.cik, p_as_of, p_current, request.requests) label
), candidate_rows AS MATERIALIZED (
    SELECT r.*, CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown')
                    THEN COALESCE(r.explicit_label, history.label)
                    ELSE sec_instrument_label(r.security_title, r.class_key, r.security_kind) END AS label
    FROM label_inputs r LEFT JOIN historical_labels history
      ON history.cik = r.cik AND history.class_key = r.class_key AND history.bound_on = r.known_on
), candidates AS (
    -- labels: the classes of the rows by which the candidate states the hold (its
    -- rows showing the ticker, else its rows): only a registration naming one of
    -- them, never another class the ticker once showed on, reopens it
    SELECT r.cik, r.adsh, max(r.known_on) AS known_on, max(r.accepted) AS accepted,
           bool_or(r.shows) AS shows,
           CASE WHEN bool_or(r.shows)
                THEN array_remove(array_agg(DISTINCT (r.label) COLLATE "C" ORDER BY (r.label) COLLATE "C")
                                  FILTER (WHERE r.shows), NULL)
                ELSE array_remove(array_agg(DISTINCT (r.label) COLLATE "C" ORDER BY (r.label) COLLATE "C"),
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
                        ORDER BY x.known_on DESC, x.accepted DESC NULLS LAST, x.adsh COLLATE "C" DESC
                        LIMIT 1)
          AND NOT EXISTS (SELECT 1 FROM candidate_rows y
                          WHERE y.cik = c.cik AND y.adsh = c.adsh AND y.class_key = t.class_key))
), ends AS MATERIALIZED (
    SELECT p.cik, e.effective_on, e.filed, e.form, e.adsh, e.definitive, e.class_keys,
           e.tentative_keys, e.class_kind, e.named_kinds, e.issuer_symbols, e.instrument_scope,
           (SELECT min(c.known_on) FROM candidates c
            WHERE c.cik = p.cik AND c.shows AND c.known_on >= e.effective_on) AS first_post_on
    FROM per_cik p
    CROSS JOIN LATERAL sec_issuer_end_scopes(p.cik, p_as_of, p_current) e
), starts AS MATERIALIZED (
    SELECT p.cik, r.* FROM per_cik p
    CROSS JOIN LATERAL sec_registration_starts(p.cik, p_as_of, p_current) r
), row_closures AS MATERIALIZED (
    -- Close each statement row independently. Different scopes (or distinct
    -- ends) may close A and B on different dates while both carry the ticker.
    -- A stale post-definitive row remains closed; a registration only rescues
    -- the class of that row, never another class sharing its ticker.
    SELECT r.cik, r.adsh, r.class_key, r.security_kind, r.ticker_key, d.effective_on, d.adsh AS end_adsh, role.value,
           d.effective_on <= c.known_on AS blocked
    FROM candidate_rows r JOIN candidates c ON c.cik = r.cik AND c.adsh = r.adsh
    JOIN ends d ON d.cik = r.cik
    CROSS JOIN LATERAL (SELECT sec_end_role(d.class_keys, d.tentative_keys,
        d.class_kind, d.named_kinds, r.class_key, r.security_kind, r.label, r.ticker_key, d.instrument_scope) AS value) role
    WHERE (r.shows OR NOT c.shows) AND role.value IS NOT NULL
      AND (d.effective_on > c.known_on OR (
          ((d.definitive AND role.value = 'identified' AND EXISTS (
              SELECT 1 FROM relevant prior WHERE prior.cik = r.cik AND prior.known_on < d.effective_on
                AND (prior.security_kind = r.security_kind
                     OR (prior.security_kind IN ('equity', 'depositary', 'unknown')
                         AND r.security_kind IN ('equity', 'depositary', 'unknown')))))
           OR EXISTS (SELECT 1 FROM per_cik other WHERE other.cik <> r.cik
                      AND other.first_on BETWEEN d.effective_on - 30 AND d.first_post_on))
          AND NOT EXISTS (
              SELECT 1 FROM starts registration
              WHERE registration.cik = r.cik AND registration.filed >= d.effective_on
                AND registration.available_on <= c.known_on
                AND r.security_kind IN ('equity', 'depositary', 'unknown')
                AND CASE WHEN (r.label = ANY(registration.classes)
                     OR (cardinality(registration.classes) = 0 AND d.issuer_symbols = 1)) THEN
                    sec_registration_identifies(r.cik, p_as_of, p_current, registration.filed + 1,
                        r.known_on, r.class_key,
                        CASE WHEN cardinality(registration.classes) = 0 THEN NULL ELSE r.label END)
                    ELSE false END)))
), closes AS MATERIALIZED (
    -- EVERY candidate row needs SOME closure by this bound. Requiring a
    -- single end row to close all rows fails after a per-class amendment split.
    SELECT e.cik, e.adsh AS end_adsh, e.effective_on, e.class_keys, c.adsh,
           bool_and(EXISTS (SELECT 1 FROM row_closures z
               WHERE z.cik = r.cik AND z.adsh = r.adsh AND z.class_key = r.class_key
                 AND z.security_kind = r.security_kind AND z.ticker_key = r.ticker_key AND z.effective_on <= e.effective_on)) AS closed,
           bool_and(EXISTS (SELECT 1 FROM row_closures z
               WHERE z.cik = r.cik AND z.adsh = r.adsh AND z.class_key = r.class_key
                 AND z.security_kind = r.security_kind AND z.ticker_key = r.ticker_key AND z.effective_on <= e.effective_on
                 AND z.value = 'identified')) AS identified,
           bool_and(EXISTS (SELECT 1 FROM row_closures z
               WHERE z.cik = r.cik AND z.adsh = r.adsh AND z.class_key = r.class_key
                 AND z.security_kind = r.security_kind AND z.ticker_key = r.ticker_key AND z.effective_on <= e.effective_on
                 AND z.blocked)) AS blocked
    FROM ends e JOIN candidates c ON c.cik = e.cik
    JOIN candidate_rows r ON r.cik = c.cik AND r.adsh = c.adsh AND (r.shows OR NOT c.shows)
    WHERE EXISTS (SELECT 1 FROM row_closures z WHERE z.cik = c.cik AND z.adsh = c.adsh
                  AND z.end_adsh = e.adsh AND z.effective_on = e.effective_on)
    GROUP BY e.cik, e.adsh, e.effective_on, e.class_keys, c.adsh
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
        JOIN closes k ON k.cik = e.cik AND k.end_adsh = e.adsh AND k.effective_on = e.effective_on AND k.class_keys IS NOT DISTINCT FROM e.class_keys AND k.adsh = c.adsh
        WHERE e.cik = c.cik AND e.effective_on <= c.known_on AND k.blocked)
    ORDER BY d.on_date, c.cik, c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh COLLATE "C" DESC
), last_end AS (
    -- the latest end, effective by the date, after the statement that closes every
    -- class it shows the ticker on (an end of another class leaves the hold)
    SELECT DISTINCT ON (s.on_date, e.cik) s.on_date, e.*
    FROM ends e
    JOIN statement s ON s.cik = e.cik
    JOIN closes k ON k.cik = e.cik AND k.end_adsh = e.adsh AND k.effective_on = e.effective_on AND k.class_keys IS NOT DISTINCT FROM e.class_keys AND k.adsh = s.adsh
    WHERE e.effective_on > s.known_on AND e.effective_on <= s.on_date AND k.closed
    ORDER BY s.on_date, e.cik, e.effective_on DESC, k.identified DESC, e.adsh COLLATE "C" DESC
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
    status text, class_key text, tickers text[], security_kind text,
    statement_on date, adsh text, equity_lines integer
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $fn$
WITH raw_own AS (
    SELECT o.source_available_on AS known_on, o.accepted, o.adsh,
           o.filing_equity_classes, o.filing_complete, o.security_kind, o.ticker_key
    FROM sec_observations_at(p_as_of, false) o
    WHERE o.cik = p_cik AND o.class_key = p_class_key
), nonlisted_tickers AS (
    SELECT DISTINCT r.ticker_key FROM raw_own r
    WHERE r.security_kind NOT IN ('equity', 'depositary', 'unknown')
), listed_active AS MATERIALIZED (
    -- A suppressed non-listed row cannot choose a statement or change the
    -- sole-class profile merely because its raw context matches the class.
    SELECT t.ticker_key, h.on_date
    FROM nonlisted_tickers t
    CROSS JOIN LATERAL sec_ticker_listed_holds_at(t.ticker_key, p_as_of, 400, false) h
    WHERE h.state = 'active'
), own AS (
    SELECT r.* FROM raw_own r
    WHERE r.security_kind IN ('equity', 'depositary', 'unknown')
       OR NOT EXISTS (SELECT 1 FROM listed_active a
                      WHERE a.ticker_key = r.ticker_key AND a.on_date = r.known_on)
), follows_sole AS (
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM own) THEN true ELSE COALESCE((
        SELECT o.filing_equity_classes = 1
               AND o.security_kind IN ('equity', 'depositary', 'unknown')
        FROM own o WHERE o.filing_complete
        ORDER BY o.known_on DESC, o.accepted DESC NULLS LAST, o.adsh COLLATE "C" DESC
        LIMIT 1), false) END AS sole
), latest_complete AS (
    SELECT f.adsh, f.filing_equity_classes
    FROM sec_observations_at(p_as_of, false) f
    WHERE f.cik = p_cik AND f.filing_complete
    ORDER BY f.source_available_on DESC, f.accepted DESC NULLS LAST, f.adsh COLLATE "C" DESC
    LIMIT 1
), raw_rows AS MATERIALIZED (
    SELECT f.adsh, f.source_available_on AS known_on, f.accepted,
           f.ticker, f.ticker_key, f.class_key, f.security_kind, f.security_title
    FROM sec_observations_at(p_as_of, false) f
    WHERE f.cik = p_cik
      AND (f.class_key = p_class_key
           OR ((SELECT s.sole FROM follows_sole s) AND f.filing_complete
               AND f.filing_equity_classes = 1
               AND f.security_kind IN ('equity', 'depositary', 'unknown')))
), rows_unlabelled AS MATERIALIZED (
    SELECT r.* FROM raw_rows r
    WHERE r.security_kind IN ('equity', 'depositary', 'unknown')
       OR NOT EXISTS (SELECT 1 FROM listed_active a
                      WHERE a.ticker_key = r.ticker_key AND a.on_date = r.known_on)
), label_inputs AS MATERIALIZED (
    SELECT r.*, CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown')
                     THEN sec_first_label(r.security_title) END AS explicit_label
    FROM rows_unlabelled r
), label_requests AS (
    SELECT requested.cik,
           jsonb_agg(jsonb_build_object('class_key', requested.class_key, 'bound_on', requested.known_on)
                     ORDER BY requested.class_key COLLATE "C", requested.known_on) AS requests
    FROM (SELECT DISTINCT p_cik AS cik, r.class_key, r.known_on
          FROM label_inputs r
          WHERE r.security_kind IN ('equity', 'depositary', 'unknown') AND r.explicit_label IS NULL) requested
    GROUP BY requested.cik
), historical_labels AS MATERIALIZED (
    SELECT request.cik, label.* FROM label_requests request
    CROSS JOIN LATERAL sec_class_label_history(request.cik, p_as_of, false, request.requests) label
), rows AS MATERIALIZED (
    SELECT r.*, CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown')
                    THEN COALESCE(r.explicit_label, history.label)
                    ELSE sec_instrument_label(r.security_title, r.class_key, r.security_kind) END AS label
    FROM label_inputs r LEFT JOIN historical_labels history
      ON history.cik = p_cik AND history.class_key = r.class_key AND history.bound_on = r.known_on
), aliases AS MATERIALIZED (
    -- These are the dated resolver's existing sole-class aliases, not future
    -- current-truth line links. Only the candidate's own kind is applied to them.
    SELECT DISTINCT r.class_key FROM rows r
    WHERE r.security_kind IN ('equity', 'depositary', 'unknown')
), candidates AS MATERIALIZED (
    SELECT r.adsh, max(r.known_on) AS known_on, max(r.accepted) AS accepted
    FROM rows r GROUP BY r.adsh
), ends AS MATERIALIZED (
    SELECT e.* FROM sec_issuer_end_scopes(p_cik, p_as_of, false) e
), starts AS MATERIALIZED (
    SELECT s.* FROM sec_registration_starts(p_cik, p_as_of, false) s
), row_closures AS MATERIALIZED (
    SELECT r.adsh, r.class_key, r.security_kind, r.ticker_key,
           e.effective_on, e.effective_on <= c.known_on AS blocked
    FROM rows r JOIN candidates c ON c.adsh = r.adsh CROSS JOIN ends e
    CROSS JOIN LATERAL (
        SELECT CASE WHEN bool_or(x.value = 'identified') THEN 'identified'
                    WHEN bool_or(x.value = 'tentative') THEN 'tentative' END AS value
        FROM (
            SELECT sec_end_role(e.class_keys, e.tentative_keys, e.class_kind,
                                e.named_kinds, k.class_key, r.security_kind, r.label, r.ticker_key, e.instrument_scope) AS value
            FROM (
                SELECT a.class_key FROM aliases a
                WHERE r.security_kind IN ('equity', 'depositary', 'unknown')
                UNION
                SELECT r.class_key
            ) k
        ) x
    ) role
    WHERE role.value IS NOT NULL AND (
        e.effective_on > c.known_on
        OR (e.definitive AND role.value = 'identified'
            AND EXISTS (SELECT 1 FROM sec_observations_at(p_as_of, false) prior
                        WHERE prior.cik = p_cik AND prior.ticker_key = r.ticker_key
                          AND prior.source_available_on < e.effective_on
                          AND (prior.security_kind = r.security_kind
                               OR (prior.security_kind IN ('equity', 'depositary', 'unknown')
                                   AND r.security_kind IN ('equity', 'depositary', 'unknown'))))
            AND NOT EXISTS (
                SELECT 1 FROM starts s
                WHERE s.filed >= e.effective_on AND s.available_on <= c.known_on
                  AND r.security_kind IN ('equity', 'depositary', 'unknown')
                  AND CASE WHEN (r.label = ANY(s.classes)
                       OR (cardinality(s.classes) = 0 AND e.issuer_symbols = 1)) THEN
                      sec_registration_identifies(p_cik, p_as_of, false, s.filed + 1, r.known_on,
                          r.class_key, CASE WHEN cardinality(s.classes) = 0 THEN NULL ELSE r.label END)
                      ELSE false END)))
), chosen AS (
    SELECT c.* FROM candidates c
    WHERE EXISTS (
        SELECT 1 FROM rows r WHERE r.adsh = c.adsh
          AND NOT EXISTS (SELECT 1 FROM row_closures z
              WHERE z.adsh = r.adsh AND z.class_key = r.class_key
                AND z.security_kind = r.security_kind AND z.ticker_key = r.ticker_key
                AND z.blocked))
    ORDER BY c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh COLLATE "C" DESC
    LIMIT 1
), judged_statement AS (
    SELECT c.*, NOT EXISTS (
        SELECT 1 FROM rows r WHERE r.adsh = c.adsh
          AND NOT EXISTS (SELECT 1 FROM row_closures z
              WHERE z.adsh = r.adsh AND z.class_key = r.class_key
                AND z.security_kind = r.security_kind AND z.ticker_key = r.ticker_key
                AND z.effective_on <= p_as_of)) AS ended
    FROM chosen c
), statement_rows AS MATERIALIZED (
    -- An alive statement reports only surviving rows. An ended statement keeps
    -- its last rows for audit, as the public resolver did before v3.
    SELECT r.* FROM rows r JOIN judged_statement c ON c.adsh = r.adsh
    WHERE c.ended OR NOT EXISTS (
        SELECT 1 FROM row_closures z WHERE z.adsh = r.adsh AND z.class_key = r.class_key
          AND z.security_kind = r.security_kind AND z.ticker_key = r.ticker_key
          AND z.effective_on <= p_as_of)
), statement AS (
    SELECT c.known_on, c.adsh,
           ARRAY(SELECT DISTINCT r.ticker COLLATE "C" FROM statement_rows r
                 ORDER BY r.ticker COLLATE "C") AS tickers,
           first_row.class_key, first_row.security_kind, c.ended
    FROM judged_statement c
    CROSS JOIN LATERAL (
        SELECT r.class_key, r.security_kind FROM statement_rows r
        ORDER BY r.class_key = p_class_key DESC, r.class_key COLLATE "C", r.ticker COLLATE "C"
        LIMIT 1
    ) first_row
)
SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM statement) THEN
                CASE WHEN COALESCE((SELECT l.filing_equity_classes FROM latest_complete l), 0) > 1
                     THEN 'ambiguous_class' ELSE 'missing' END
            WHEN (SELECT s.ended FROM statement s) THEN 'ended'
            WHEN (SELECT s.known_on FROM statement s) < p_as_of - p_max_age_days THEN 'stale'
            ELSE 'resolved' END,
       (SELECT s.class_key FROM statement s),
       COALESCE((SELECT s.tickers FROM statement s), '{}'::text[]),
       (SELECT s.security_kind FROM statement s),
       (SELECT s.known_on FROM statement s), (SELECT s.adsh FROM statement s),
       COALESCE((SELECT l.filing_equity_classes FROM latest_complete l), 0)
$fn$;

CREATE OR REPLACE FUNCTION sec_issuer_lines(p_cik bigint)
RETURNS TABLE (class_key text, line_key text)
LANGUAGE sql STABLE PARALLEL SAFE
SET jit = off
AS $fn$
SELECT l.class_key, l.line_key
FROM sec_issuer_lines_at(p_cik, 'infinity'::date, true, 'infinity'::date) l
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
           array_agg(DISTINCT (r.security_kind) COLLATE "C" ORDER BY (r.security_kind) COLLATE "C") AS kinds
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
), candidate_rows_unlabelled AS MATERIALIZED (
    SELECT h.cik, h.line_key, o.adsh, o.class_key, o.security_kind, o.security_title,
           o.ticker_key, o.source_available_on AS known_on, o.accepted,
           COALESCE(l.line_key = h.line_key, false) AS has_line,
           COALESCE(l.line_key = h.line_key, false) AND o.ticker_key = key.k AS shows
    FROM held h CROSS JOIN key
    JOIN sec_observations_at('infinity'::date, true) o ON o.cik = h.cik
    LEFT JOIN lines l ON l.cik = o.cik AND l.class_key = o.class_key
    WHERE (l.line_key = h.line_key
       OR (o.filing_complete AND o.filing_equity_classes = 1
           AND o.security_kind IN ('equity', 'depositary', 'unknown'))
       OR (o.filing_complete AND o.security_kind IN ('equity', 'depositary', 'unknown')
           AND EXISTS (SELECT 1 FROM sole_lines sole
                       WHERE sole.cik = h.cik AND sole.line_key = h.line_key
                         AND o.source_available_on > sole.sole_until)))
      AND (o.security_kind IN ('equity', 'depositary', 'unknown')
           OR (NOT p_listed_only AND EXISTS (
               SELECT 1 FROM relevant kept
               WHERE kept.cik = o.cik AND kept.class_key = o.class_key AND kept.security_kind = o.security_kind
                 AND (kept.adsh = o.adsh OR (o.ticker_key <> key.k AND kept.known_on <= o.source_available_on)))))
), label_inputs AS MATERIALIZED (
    SELECT r.*, CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown')
                     THEN sec_first_label(r.security_title) END AS explicit_label
    FROM candidate_rows_unlabelled r
), label_requests AS (
    SELECT requested.cik,
           jsonb_agg(jsonb_build_object('class_key', requested.class_key, 'bound_on', requested.known_on)
                     ORDER BY requested.class_key COLLATE "C", requested.known_on) AS requests
    FROM (SELECT DISTINCT r.cik AS cik, r.class_key, r.known_on
          FROM label_inputs r
          WHERE r.security_kind IN ('equity', 'depositary', 'unknown') AND r.explicit_label IS NULL) requested
    GROUP BY requested.cik
), historical_labels AS MATERIALIZED (
    SELECT request.cik, label.* FROM label_requests request
    CROSS JOIN LATERAL sec_class_label_history(request.cik, 'infinity'::date, true, request.requests) label
), candidate_rows AS MATERIALIZED (
    SELECT r.*, CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown')
                    THEN COALESCE(r.explicit_label, history.label)
                    ELSE sec_instrument_label(r.security_title, r.class_key, r.security_kind) END AS label
    FROM label_inputs r LEFT JOIN historical_labels history
      ON history.cik = r.cik AND history.class_key = r.class_key AND history.bound_on = r.known_on
), candidates AS MATERIALIZED (
    SELECT r.cik, r.line_key, r.adsh, max(r.known_on) AS known_on, max(r.accepted) AS accepted,
           bool_or(r.shows) AS shows,
           (array_agg(r.class_key ORDER BY r.class_key COLLATE "C") FILTER (WHERE r.shows))[1] AS class_key
    FROM candidate_rows r GROUP BY r.cik, r.line_key, r.adsh
), ends AS MATERIALIZED (
    SELECT h.cik, e.effective_on, e.filed, e.form, e.adsh, e.definitive, e.class_keys,
           e.tentative_keys, e.class_kind, e.named_kinds, e.issuer_symbols, e.instrument_scope
    FROM holder_ciks h
    CROSS JOIN LATERAL sec_issuer_end_scopes(h.cik, 'infinity'::date, true) e
), starts AS MATERIALIZED (
    SELECT h.cik, r.filed, r.available_on, r.classes
    FROM holder_ciks h
    CROSS JOIN LATERAL sec_registration_starts(h.cik, 'infinity'::date, true) r
), line_ends AS MATERIALIZED (
    -- Alias keys preserve class continuity; the kind is joined back to each
    -- actual candidate row below, never aggregated across the line's history.
    SELECT h.cik, h.line_key, k.security_kind, e.effective_on, e.form, e.adsh,
           e.definitive, e.issuer_symbols, e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds, e.instrument_scope, role.value AS role
    FROM held h JOIN ends e ON e.cik = h.cik
    JOIN (SELECT DISTINCT cik, line_key, security_kind FROM candidate_rows WHERE has_line) k
      ON k.cik = h.cik AND k.line_key = h.line_key
    CROSS JOIN LATERAL (
        SELECT CASE WHEN bool_or(r.value = 'identified') THEN 'identified'
                    WHEN bool_or(r.value = 'tentative') THEN 'tentative' END AS value
        FROM (SELECT sec_end_role(e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds,
                                 l.class_key, k.security_kind) AS value
              FROM lines l WHERE l.cik = h.cik AND l.line_key = h.line_key) r
    ) role WHERE role.value IS NOT NULL
), row_closures AS MATERIALIZED (
    SELECT r.cik, r.line_key, r.adsh, r.class_key, r.security_kind, r.ticker_key,
           d.effective_on, d.adsh AS end_adsh, d.form, actual.value AS role,
           d.effective_on <= r.known_on AS blocked
    FROM candidate_rows r JOIN candidates c
      ON c.cik = r.cik AND c.line_key = r.line_key AND c.adsh = r.adsh
    JOIN line_ends d ON d.cik = r.cik AND d.line_key = r.line_key AND d.security_kind = r.security_kind
    CROSS JOIN LATERAL (
        SELECT CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown') THEN d.role ELSE (
            SELECT CASE WHEN bool_or(role.value = 'identified') THEN 'identified'
                        WHEN bool_or(role.value = 'tentative') THEN 'tentative' END
            FROM lines l CROSS JOIN LATERAL (SELECT sec_end_role(d.class_keys, d.tentative_keys,
                 d.class_kind, d.named_kinds, l.class_key, r.security_kind, r.label, r.ticker_key,
                 d.instrument_scope) AS value) role
            WHERE l.cik = r.cik AND l.line_key = r.line_key) END AS value
    ) actual
    WHERE r.has_line AND (r.shows OR NOT c.shows) AND actual.value IS NOT NULL
      AND (d.effective_on > r.known_on OR (
          ((d.definitive AND actual.value = 'identified' AND EXISTS (
              SELECT 1 FROM candidate_rows prior
              WHERE prior.cik = r.cik AND prior.line_key = r.line_key AND prior.has_line
                AND prior.known_on < d.effective_on AND prior.ticker_key = r.ticker_key
                AND (prior.security_kind = r.security_kind
                     OR (prior.security_kind IN ('equity', 'depositary', 'unknown')
                         AND r.security_kind IN ('equity', 'depositary', 'unknown')))))
           OR EXISTS (
               SELECT 1 FROM cik_first other WHERE other.cik <> r.cik
                 AND other.first_on BETWEEN d.effective_on - 30 AND (
                     SELECT min(next.known_on) FROM candidates next
                     WHERE next.cik = r.cik AND next.line_key = r.line_key AND next.shows
                       AND next.known_on >= d.effective_on)))
          AND NOT EXISTS (
              SELECT 1 FROM starts registration
              WHERE registration.cik = r.cik AND registration.filed >= d.effective_on
                AND registration.available_on <= r.known_on
                AND r.security_kind IN ('equity', 'depositary', 'unknown')
                AND CASE WHEN (r.label = ANY(registration.classes)
                     OR (cardinality(registration.classes) = 0 AND d.issuer_symbols = 1)) THEN
                    sec_registration_identifies(r.cik, 'infinity'::date, true, registration.filed + 1,
                        r.known_on, r.class_key,
                        CASE WHEN cardinality(registration.classes) = 0 THEN NULL ELSE r.label END)
                    ELSE false END)))
), counted AS MATERIALIZED (
    SELECT c.* FROM candidates c
    WHERE NOT (EXISTS (SELECT 1 FROM candidate_rows r
                       WHERE r.cik = c.cik AND r.line_key = c.line_key AND r.adsh = c.adsh
                         AND r.has_line AND (r.shows OR NOT c.shows))
        AND NOT EXISTS (
            SELECT 1 FROM candidate_rows r
            WHERE r.cik = c.cik AND r.line_key = c.line_key AND r.adsh = c.adsh
              AND r.has_line AND (r.shows OR NOT c.shows)
              AND NOT EXISTS (SELECT 1 FROM row_closures z
                  WHERE z.cik = r.cik AND z.line_key = r.line_key AND z.adsh = r.adsh
                    AND z.class_key = r.class_key AND z.security_kind = r.security_kind
                    AND z.ticker_key = r.ticker_key AND z.blocked)))
), candidate_ends AS MATERIALIZED (
    -- Every represented row needs a closure by this bound. Kinds come from the
    -- candidate itself; an old/future preferred annotation cannot end common.
    SELECT DISTINCT bound.cik, bound.line_key, bound.adsh AS candidate_adsh,
           bound.effective_on, bound.end_adsh AS adsh, bound.form, bound.role
    FROM row_closures bound JOIN counted c
      ON c.cik = bound.cik AND c.line_key = bound.line_key AND c.adsh = bound.adsh
    WHERE NOT EXISTS (
        SELECT 1 FROM candidate_rows r
        WHERE r.cik = c.cik AND r.line_key = c.line_key AND r.adsh = c.adsh
          AND r.has_line AND (r.shows OR NOT c.shows)
          AND NOT EXISTS (SELECT 1 FROM row_closures z
              WHERE z.cik = r.cik AND z.line_key = r.line_key AND z.adsh = r.adsh
                AND z.class_key = r.class_key AND z.security_kind = r.security_kind
                AND z.ticker_key = r.ticker_key AND z.effective_on <= bound.effective_on))
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
        SELECT e.effective_on FROM candidate_ends e
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
        SELECT c.adsh, c.known_on, c.shows, c.class_key FROM counted c
        WHERE c.cik = b.cik AND c.line_key = b.line_key AND c.known_on <= b.on_date
        ORDER BY c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh COLLATE "C" DESC
        LIMIT 1
    ) s ON true
    LEFT JOIN LATERAL (
        SELECT e.effective_on, e.form FROM candidate_ends e
        WHERE e.cik = b.cik AND e.line_key = b.line_key AND e.candidate_adsh = s.adsh AND e.effective_on <= b.on_date
        ORDER BY e.effective_on DESC, e.role = 'identified' DESC, e.adsh COLLATE "C" DESC
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
    SELECT DISTINCT ON (m.cik, m.line_key COLLATE "C", m.run_no) m.cik, m.line_key, m.run_no,
           m.on_date AS valid_to, m.reason
    FROM marked m
    WHERE m.run_no > 0 AND NOT m.holding
    ORDER BY m.cik, m.line_key COLLATE "C", m.run_no, m.on_date
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
), raw_rows AS MATERIALIZED (
    SELECT p_cik AS cik, p_line_key AS line_key, o.adsh, o.class_key, o.security_kind,
           o.security_title, o.ticker_key, o.ticker, o.source_available_on AS known_on, o.accepted,
           COALESCE(l.line_key = p_line_key, false) AS has_line,
           COALESCE(l.line_key = p_line_key, false) AS shows
    FROM sec_observations_at('infinity'::date, true) o
    LEFT JOIN lines l ON l.class_key = o.class_key
    WHERE o.cik = p_cik
      AND (l.line_key = p_line_key
           OR (o.filing_complete AND o.filing_equity_classes = 1
               AND o.security_kind IN ('equity', 'depositary', 'unknown'))
           OR (o.filing_complete AND o.security_kind IN ('equity', 'depositary', 'unknown')
               AND o.source_available_on > (SELECT sole.sole_until FROM sole)))
), listed_runs AS MATERIALIZED (
    SELECT ticker.ticker_key, held.valid_from, held.valid_to
    FROM (SELECT DISTINCT ticker_key FROM raw_rows
          WHERE has_line AND security_kind NOT IN ('equity', 'depositary', 'unknown')) ticker
    CROSS JOIN LATERAL sec_ticker_line_runs_from(ticker.ticker_key, 400, true) held
), candidate_rows_unlabelled AS MATERIALIZED (
    SELECT r.* FROM raw_rows r
    WHERE r.security_kind IN ('equity', 'depositary', 'unknown')
       OR NOT EXISTS (SELECT 1 FROM listed_runs listed
                      WHERE listed.ticker_key = r.ticker_key AND listed.valid_from <= r.known_on
                        AND (listed.valid_to IS NULL OR r.known_on < listed.valid_to))
), label_inputs AS MATERIALIZED (
    SELECT r.*, CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown')
                     THEN sec_first_label(r.security_title) END AS explicit_label
    FROM candidate_rows_unlabelled r
), label_requests AS (
    SELECT requested.cik,
           jsonb_agg(jsonb_build_object('class_key', requested.class_key, 'bound_on', requested.known_on)
                     ORDER BY requested.class_key COLLATE "C", requested.known_on) AS requests
    FROM (SELECT DISTINCT r.cik AS cik, r.class_key, r.known_on
          FROM label_inputs r
          WHERE r.security_kind IN ('equity', 'depositary', 'unknown') AND r.explicit_label IS NULL) requested
    GROUP BY requested.cik
), historical_labels AS MATERIALIZED (
    SELECT request.cik, label.* FROM label_requests request
    CROSS JOIN LATERAL sec_class_label_history(request.cik, 'infinity'::date, true, request.requests) label
), candidate_rows AS MATERIALIZED (
    SELECT r.*, CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown')
                    THEN COALESCE(r.explicit_label, history.label)
                    ELSE sec_instrument_label(r.security_title, r.class_key, r.security_kind) END AS label
    FROM label_inputs r LEFT JOIN historical_labels history
      ON history.cik = r.cik AND history.class_key = r.class_key AND history.bound_on = r.known_on
), candidates AS MATERIALIZED (
    SELECT r.cik, r.line_key, r.adsh, max(r.known_on) AS known_on, max(r.accepted) AS accepted,
           bool_or(r.has_line) AS has_line, bool_or(r.shows) AS shows,
           array_agg(DISTINCT r.ticker COLLATE "C" ORDER BY r.ticker COLLATE "C")
               FILTER (WHERE r.has_line) AS tickers
    FROM candidate_rows r GROUP BY r.cik, r.line_key, r.adsh
), starts AS MATERIALIZED (
    SELECT p_cik AS cik, r.* FROM sec_registration_starts(p_cik, 'infinity'::date, true) r
), ends AS MATERIALIZED (
    SELECT e.* FROM sec_issuer_end_scopes(p_cik, 'infinity'::date, true) e
), line_ends AS MATERIALIZED (
    SELECT p_cik AS cik, p_line_key AS line_key, k.security_kind,
           e.effective_on, e.adsh, e.form, e.definitive, e.issuer_symbols, e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds, e.instrument_scope, role.value AS role
    FROM ends e
    CROSS JOIN (SELECT DISTINCT security_kind FROM candidate_rows WHERE has_line) k
    CROSS JOIN LATERAL (
        SELECT CASE WHEN bool_or(r.value = 'identified') THEN 'identified'
                    WHEN bool_or(r.value = 'tentative') THEN 'tentative' END AS value
        FROM (SELECT sec_end_role(e.class_keys, e.tentative_keys, e.class_kind, e.named_kinds,
                                 l.class_key, k.security_kind) AS value
              FROM lines l WHERE l.line_key = p_line_key) r
    ) role WHERE role.value IS NOT NULL
), row_closures AS MATERIALIZED (
    SELECT r.cik, r.line_key, r.adsh, r.class_key, r.security_kind, r.ticker_key,
           d.effective_on, d.adsh AS end_adsh, d.form, actual.value AS role,
           d.effective_on <= r.known_on AS blocked
    FROM candidate_rows r JOIN candidates c
      ON c.cik = r.cik AND c.line_key = r.line_key AND c.adsh = r.adsh
    JOIN line_ends d ON d.cik = r.cik AND d.line_key = r.line_key AND d.security_kind = r.security_kind
    CROSS JOIN LATERAL (
        SELECT CASE WHEN r.security_kind IN ('equity', 'depositary', 'unknown') THEN d.role ELSE (
            SELECT CASE WHEN bool_or(role.value = 'identified') THEN 'identified'
                        WHEN bool_or(role.value = 'tentative') THEN 'tentative' END
            FROM lines l CROSS JOIN LATERAL (SELECT sec_end_role(d.class_keys, d.tentative_keys,
                 d.class_kind, d.named_kinds, l.class_key, r.security_kind, r.label, r.ticker_key,
                 d.instrument_scope) AS value) role
            WHERE l.line_key = p_line_key) END AS value
    ) actual
    WHERE r.has_line AND (r.shows OR NOT c.shows) AND actual.value IS NOT NULL
      AND (d.effective_on > r.known_on OR (
          ((d.definitive AND actual.value = 'identified' AND EXISTS (
              SELECT 1 FROM candidate_rows prior
              WHERE prior.cik = r.cik AND prior.line_key = r.line_key AND prior.has_line
                AND prior.known_on < d.effective_on AND prior.ticker_key = r.ticker_key
                AND (prior.security_kind = r.security_kind
                     OR (prior.security_kind IN ('equity', 'depositary', 'unknown')
                         AND r.security_kind IN ('equity', 'depositary', 'unknown')))))
           )
          AND NOT EXISTS (
              SELECT 1 FROM starts registration
              WHERE registration.cik = r.cik AND registration.filed >= d.effective_on
                AND registration.available_on <= r.known_on
                AND r.security_kind IN ('equity', 'depositary', 'unknown')
                AND CASE WHEN (r.label = ANY(registration.classes)
                     OR (cardinality(registration.classes) = 0 AND d.issuer_symbols = 1)) THEN
                    sec_registration_identifies(r.cik, 'infinity'::date, true, registration.filed + 1,
                        r.known_on, r.class_key,
                        CASE WHEN cardinality(registration.classes) = 0 THEN NULL ELSE r.label END)
                    ELSE false END)))
), counted AS MATERIALIZED (
    SELECT c.* FROM candidates c
    WHERE NOT (EXISTS (SELECT 1 FROM candidate_rows r
                       WHERE r.cik = c.cik AND r.line_key = c.line_key AND r.adsh = c.adsh
                         AND r.has_line AND (r.shows OR NOT c.shows))
        AND NOT EXISTS (
            SELECT 1 FROM candidate_rows r
            WHERE r.cik = c.cik AND r.line_key = c.line_key AND r.adsh = c.adsh
              AND r.has_line AND (r.shows OR NOT c.shows)
              AND NOT EXISTS (SELECT 1 FROM row_closures z
                  WHERE z.cik = r.cik AND z.line_key = r.line_key AND z.adsh = r.adsh
                    AND z.class_key = r.class_key AND z.security_kind = r.security_kind
                    AND z.ticker_key = r.ticker_key AND z.blocked)))
), candidate_ends AS MATERIALIZED (
    -- Every represented row needs a closure by this bound. Kinds come from the
    -- candidate itself; an old/future preferred annotation cannot end common.
    SELECT DISTINCT bound.cik, bound.line_key, bound.adsh AS candidate_adsh,
           bound.effective_on, bound.end_adsh AS adsh, bound.form, bound.role
    FROM row_closures bound JOIN counted c
      ON c.cik = bound.cik AND c.line_key = bound.line_key AND c.adsh = bound.adsh
    WHERE NOT EXISTS (
        SELECT 1 FROM candidate_rows r
        WHERE r.cik = c.cik AND r.line_key = c.line_key AND r.adsh = c.adsh
          AND r.has_line AND (r.shows OR NOT c.shows)
          AND NOT EXISTS (SELECT 1 FROM row_closures z
              WHERE z.cik = r.cik AND z.line_key = r.line_key AND z.adsh = r.adsh
                AND z.class_key = r.class_key AND z.security_kind = r.security_kind
                AND z.ticker_key = r.ticker_key AND z.effective_on <= bound.effective_on))
), bounds AS (
    SELECT c.known_on AS on_date FROM counted c
    UNION
    SELECT c.known_on + p_max_age_days + 1 FROM counted c
    WHERE c.has_line AND p_max_age_days IS NOT NULL
    UNION
    SELECT e.effective_on FROM candidate_ends e
), states AS (
    SELECT b.on_date, s.known_on AS statement_on, s.has_line, s.tickers,
           le.effective_on AS end_on, le.form AS end_form
    FROM bounds b
    LEFT JOIN LATERAL (
        SELECT c.adsh, c.known_on, c.has_line, c.tickers FROM counted c
        WHERE c.known_on <= b.on_date
        ORDER BY c.known_on DESC, c.accepted DESC NULLS LAST, c.adsh COLLATE "C" DESC
        LIMIT 1
    ) s ON true
    LEFT JOIN LATERAL (
        SELECT e.effective_on, e.form FROM candidate_ends e WHERE e.candidate_adsh = s.adsh AND e.effective_on <= b.on_date
        ORDER BY e.effective_on DESC, e.role = 'identified' DESC, e.adsh COLLATE "C" DESC
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
       ARRAY(SELECT DISTINCT t COLLATE "C" FROM marked m, unnest(m.tickers) t
             WHERE m.run_no = r.run_no AND m.alive ORDER BY t COLLATE "C") AS symbols
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
             r.statement_adsh COLLATE "C" DESC, r.cik
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
    ORDER BY c.stated_on DESC, c.available_on DESC, c.accepted DESC NULLS LAST, c.adsh COLLATE "C" DESC
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
    ORDER BY k.stated_on DESC, k.available_on DESC, k.accepted DESC NULLS LAST, k.adsh COLLATE "C" DESC,
             k.basis COLLATE "C", k.refusal IS NOT NULL
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
SELECT 'alive'::text COLLATE "C", p_cik, w.line_key COLLATE "C", a.valid_from, a.valid_to, a.end_reason,
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
ORDER BY 1, 4, 2, 3, 5
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
ORDER BY a.valid_from, a.line_key COLLATE "C"
$fn$;

CREATE OR REPLACE VIEW sec_ticker_intervals AS
WITH statements AS (
    SELECT o.cik, o.class_key, o.source_available_on AS on_date,
           max(o.accepted) AS accepted, o.adsh,
           array_agg(DISTINCT o.ticker COLLATE "C" ORDER BY o.ticker COLLATE "C") AS tickers
    FROM sec_ticker_cik_observations o
    WHERE o.retired_on IS NULL
    GROUP BY o.cik, o.class_key, o.source_available_on, o.adsh
), changes AS (
    SELECT s.*,
           lag(s.tickers) OVER w IS DISTINCT FROM s.tickers AS starts
    FROM statements s
    WINDOW w AS (PARTITION BY s.cik, s.class_key
                 ORDER BY s.on_date, s.accepted NULLS FIRST, s.adsh COLLATE "C")
), numbered AS (
    SELECT c.*, sum(c.starts::integer) OVER (
        PARTITION BY c.cik, c.class_key ORDER BY c.on_date, c.accepted NULLS FIRST, c.adsh COLLATE "C"
    ) AS run
    FROM changes c
), runs AS (
    SELECT n.cik, n.class_key, n.run, min(n.tickers COLLATE "C") AS tickers,
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
SELECT b.cik, b.class_key, t.ticker COLLATE "default" AS ticker, b.valid_from, b.last_confirmed_on, b.valid_to,
       b.statements
FROM bounded b
CROSS JOIN LATERAL unnest(b.tickers) AS t(ticker)
WHERE b.valid_to IS NULL OR b.valid_to > b.valid_from;

REVOKE ALL ON FUNCTION sec_instrument_label(text, text, text),
    sec_issuer_lines_at(bigint, date, boolean, date),
    sec_instrument_scopes(text),
    sec_registration_identifies(bigint, date, boolean, date, date, text, text),
    sec_issuer_end_scopes(bigint, date, boolean),
    sec_end_role(text[], text[], text, text[], text, text, text, text, jsonb),
    sec_class_label_history(bigint, date, boolean, jsonb), sec_named_kinds(text), sec_label_text(text), sec_label_norm(text), sec_label_id_re(), sec_first_label(text),
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
        'sec_instrument_label(text, text, text)',
        'sec_issuer_lines_at(bigint, date, boolean, date)',
        'sec_instrument_scopes(text)',
        'sec_registration_identifies(bigint, date, boolean, date, date, text, text)',
        'sec_issuer_end_scopes(bigint, date, boolean)',
        'sec_end_role(text[], text[], text, text[], text, text, text, text, jsonb)',

        'sec_class_label_history(bigint, date, boolean, jsonb)',
        'sec_named_kinds(text)',
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

COMMIT;
