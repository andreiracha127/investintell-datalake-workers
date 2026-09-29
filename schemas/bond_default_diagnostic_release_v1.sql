-- bond_default_diagnostic_release_v1: separate, additive diagnostic publication layer for the
-- experimental coverage-only display of bond default events. It never touches the qualified
-- bond_credit_current_pointer: a diagnostic release binds one validated, PARTIAL / LIMITED
-- bond_credit publication to an allowlisted display projection (bond_default_events_display_v1)
-- and is elected through its own compare-and-set pointer.
--
-- This file is NOT part of contracts.SQL_PATHS and does not enter the four-file SQL digest; it has
-- its own LF-normalized SHA-256 pinned by src/bonds/default_events/diagnostic_publication.py.
-- Apply AFTER the four bond_credit files, as the trusted table owner (worker_writer), against
-- schema public only (`SET search_path TO public, pg_temp`; the file asserts it). The NOLOGIN
-- role bond_default_diagnostic_reader must be pre-created by an administrator, like the three
-- bond_credit_* group roles; this file creates no role.
--
-- Every function is SECURITY DEFINER with `SET search_path = public, pg_temp` captured
-- explicitly, no dynamic identifiers, no caller-controlled schema. EXECUTE is revoked from PUBLIC;
-- only bond_default_current_diagnostic_release() is granted to the runtime reader.
--
-- Failure contract (stable): SQLSTATE P0002 with message `bond_default_diagnostic:diagnostic_not_published`
-- when no pointer exists; SQLSTATE P0001 with message `bond_default_diagnostic:<reason>` otherwise
-- (bounded reason codes; DETAIL never carries SQL, paths or credentials).
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';
SET LOCAL search_path = public, pg_temp;

DO $$
BEGIN
    IF pg_catalog.current_schema() IS DISTINCT FROM 'public' THEN
        RAISE EXCEPTION 'bond_default_diagnostic_release_v1: installs only into schema public';
    END IF;
    IF pg_catalog.to_regclass('public.bond_credit_publications') IS NULL
       OR pg_catalog.to_regclass('public.bond_default_coverage_v1') IS NULL
       OR pg_catalog.to_regclass('public.bond_rating_history_public_v1') IS NULL
       OR pg_catalog.to_regclass('public.bond_default_event_v1') IS NULL THEN
        RAISE EXCEPTION 'bond_default_diagnostic_release_v1: apply the four bond_credit SQL files first (schema public)';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'bond_credit_writer')
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'bond_credit_auditor') THEN
        RAISE EXCEPTION 'bond_default_diagnostic_release_v1: bond_credit_writer / bond_credit_auditor are missing';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'bond_default_diagnostic_reader') THEN
        RAISE EXCEPTION 'bond_default_diagnostic_release_v1: pre-create NOLOGIN role bond_default_diagnostic_reader';
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Tables (all in public). Releases and revocations are immutable / append-only; the pointer is
-- writable only inside bond_default_promote_diagnostic (transaction-scoped token).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_default_diagnostic_releases (
    release_id uuid PRIMARY KEY,
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    product text NOT NULL CHECK (product = 'bond_default_events_diagnostic_v1'),
    target_month date NOT NULL CHECK (EXTRACT(day FROM target_month) = 1),
    knowledge_cutoff timestamptz NOT NULL,
    tier text NOT NULL CHECK (tier = 'experimental_partial'),
    display_projection_version text NOT NULL CHECK (display_projection_version = 'bond_default_events_display_v1'),
    publication_fingerprint text NOT NULL CHECK (publication_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    policy_digest text NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    contract_digest text NOT NULL CHECK (contract_digest ~ '^sha256:[0-9a-f]{64}$'),
    source_frontier_manifest_digest text NOT NULL CHECK (source_frontier_manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    projection_digest text NOT NULL CHECK (projection_digest ~ '^sha256:[0-9a-f]{64}$'),
    diagnostic_sql_digest text NOT NULL CHECK (diagnostic_sql_digest ~ '^sha256:[0-9a-f]{64}$'),
    display_projection jsonb NOT NULL CHECK (pg_catalog.jsonb_typeof(display_projection) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_by text NOT NULL DEFAULT session_user
);

CREATE TABLE IF NOT EXISTS bond_default_diagnostic_pointer (
    product text PRIMARY KEY CHECK (product = 'bond_default_events_diagnostic_v1'),
    release_id uuid NOT NULL REFERENCES bond_default_diagnostic_releases(release_id),
    promoted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    promoted_by text NOT NULL DEFAULT session_user
);

CREATE TABLE IF NOT EXISTS bond_default_diagnostic_revocations (
    release_id uuid PRIMARY KEY REFERENCES bond_default_diagnostic_releases(release_id),
    revoked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    reason_code text NOT NULL CHECK (reason_code ~ '^[a-z][a-z0-9_]{2,63}$'),
    revoked_by text NOT NULL DEFAULT session_user
);

-- ---------------------------------------------------------------------------
-- Guards
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_default_diag_raise(reason text, detail text DEFAULT NULL,
                                                   sqlstate_code text DEFAULT 'P0001')
RETURNS void LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
BEGIN
    RAISE EXCEPTION '%', 'bond_default_diagnostic:' || reason
        USING ERRCODE = sqlstate_code, DETAIL = COALESCE(pg_catalog.left(detail, 200), '');
END $$;

CREATE OR REPLACE FUNCTION bond_default_diag_append_only() RETURNS trigger
LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
BEGIN
    RAISE EXCEPTION '%: append-only, % refused', TG_TABLE_NAME, TG_OP;
END $$;

CREATE OR REPLACE FUNCTION bond_default_diag_pointer_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
BEGIN
    IF TG_OP = 'DELETE'
       OR pg_catalog.current_setting('bond_default_diagnostic.pointer_token', true) IS DISTINCT FROM
          (pg_catalog.pg_backend_pid()::text || ':' || pg_catalog.txid_current()::text) THEN
        RAISE EXCEPTION 'bond_default_diagnostic_pointer is managed only by bond_default_promote_diagnostic';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_default_diagnostic_releases_append_only ON bond_default_diagnostic_releases;
CREATE TRIGGER bond_default_diagnostic_releases_append_only BEFORE UPDATE OR DELETE ON bond_default_diagnostic_releases
FOR EACH ROW EXECUTE FUNCTION bond_default_diag_append_only();
DROP TRIGGER IF EXISTS bond_default_diagnostic_releases_no_truncate ON bond_default_diagnostic_releases;
CREATE TRIGGER bond_default_diagnostic_releases_no_truncate BEFORE TRUNCATE ON bond_default_diagnostic_releases
FOR EACH STATEMENT EXECUTE FUNCTION bond_default_diag_append_only();

DROP TRIGGER IF EXISTS bond_default_diagnostic_revocations_append_only ON bond_default_diagnostic_revocations;
CREATE TRIGGER bond_default_diagnostic_revocations_append_only BEFORE UPDATE OR DELETE ON bond_default_diagnostic_revocations
FOR EACH ROW EXECUTE FUNCTION bond_default_diag_append_only();
DROP TRIGGER IF EXISTS bond_default_diagnostic_revocations_no_truncate ON bond_default_diagnostic_revocations;
CREATE TRIGGER bond_default_diagnostic_revocations_no_truncate BEFORE TRUNCATE ON bond_default_diagnostic_revocations
FOR EACH STATEMENT EXECUTE FUNCTION bond_default_diag_append_only();

DROP TRIGGER IF EXISTS bond_default_diagnostic_pointer_guard ON bond_default_diagnostic_pointer;
CREATE TRIGGER bond_default_diagnostic_pointer_guard BEFORE INSERT OR UPDATE OR DELETE ON bond_default_diagnostic_pointer
FOR EACH ROW EXECUTE FUNCTION bond_default_diag_pointer_guard();
DROP TRIGGER IF EXISTS bond_default_diagnostic_pointer_no_truncate ON bond_default_diagnostic_pointer;
CREATE TRIGGER bond_default_diagnostic_pointer_no_truncate BEFORE TRUNCATE ON bond_default_diagnostic_pointer
FOR EACH STATEMENT EXECUTE FUNCTION bond_default_diag_append_only();

-- ---------------------------------------------------------------------------
-- Lexical helpers (never raise on hostile input)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_default_diag_try_jsonb(v text) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE SET search_path = public, pg_temp AS $$
BEGIN
    RETURN v::pg_catalog.jsonb;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END $$;

-- 'YYYY-MM-DD' -> date only when the text round-trips exactly.
CREATE OR REPLACE FUNCTION bond_default_diag_parse_date(v text) RETURNS date
LANGUAGE plpgsql IMMUTABLE SET search_path = public, pg_temp AS $$
DECLARE
    d date;
BEGIN
    IF v IS NULL OR v !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
        RETURN NULL;
    END IF;
    d := v::pg_catalog.date;
    IF pg_catalog.to_char(d, 'YYYY-MM-DD') <> v THEN
        RETURN NULL;
    END IF;
    RETURN d;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END $$;

-- 'YYYY-MM-DDTHH:MM:SS.ffffffZ' -> timestamptz only when the text round-trips exactly.
CREATE OR REPLACE FUNCTION bond_default_diag_parse_ts(v text) RETURNS timestamptz
LANGUAGE plpgsql STABLE SET search_path = public, pg_temp AS $$
DECLARE
    t timestamptz;
BEGIN
    IF v IS NULL OR v !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$' THEN
        RETURN NULL;
    END IF;
    t := (pg_catalog.substr(v, 1, 26) || '+00')::pg_catalog.timestamptz;
    IF bond_credit_ts_text(t) <> v THEN
        RETURN NULL;
    END IF;
    RETURN t;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION bond_default_diag_limitation_codes() RETURNS text[]
LANGUAGE sql IMMUTABLE SET search_path = public, pg_temp AS $$
    SELECT ARRAY['censoring_not_evaluated', 'coverage_only', 'inventory_not_ingested', 'no_adjudicated_events',
                 'not_for_expected_loss', 'not_for_recommendation', 'nport_consensus_c1_blocked',
                 'nport_q3_unavailable_at_observation', 'outcomes_unascertained', 'rating_rights_unverified',
                 'rating_source_unavailable', 'reference_unreviewed', 'unresolved_not_evaluated']::text[]
$$;

-- Limitations every v1 coverage-only display carries (nport_q3_unavailable_at_observation is conditional).
CREATE OR REPLACE FUNCTION bond_default_diag_base_limitations() RETURNS text[]
LANGUAGE sql IMMUTABLE SET search_path = public, pg_temp AS $$
    SELECT ARRAY['censoring_not_evaluated', 'coverage_only', 'inventory_not_ingested', 'no_adjudicated_events',
                 'not_for_expected_loss', 'not_for_recommendation', 'nport_consensus_c1_blocked',
                 'outcomes_unascertained', 'rating_rights_unverified', 'rating_source_unavailable',
                 'reference_unreviewed', 'unresolved_not_evaluated']::text[]
$$;

-- A JSON array of known limitation codes in strict C order without duplicates.
CREATE OR REPLACE FUNCTION bond_default_diag_codes_ok(a jsonb) RETURNS boolean
LANGUAGE plpgsql STABLE SET search_path = public, pg_temp AS $$
BEGIN
    IF a IS NULL OR pg_catalog.jsonb_typeof(a) <> 'array' THEN
        RETURN false;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(a) e
               WHERE pg_catalog.jsonb_typeof(e) <> 'string'
                  OR (e #>> '{}') <> ALL (bond_default_diag_limitation_codes())) THEN
        RETURN false;
    END IF;
    RETURN a = COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t.v) ORDER BY t.v COLLATE "C")
                         FROM (SELECT DISTINCT e #>> '{}' AS v FROM pg_catalog.jsonb_array_elements(a) e) t),
                        '[]'::pg_catalog.jsonb);
END $$;

CREATE OR REPLACE FUNCTION bond_default_diag_keys_are(obj jsonb, expected text[]) RETURNS boolean
LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
    SELECT CASE WHEN pg_catalog.jsonb_typeof(obj) = 'object'
                THEN COALESCE((SELECT pg_catalog.array_agg(k ORDER BY k COLLATE "C")
                               FROM pg_catalog.jsonb_object_keys(obj) k), ARRAY[]::text[]) = expected
                ELSE false END
$$;

-- ---------------------------------------------------------------------------
-- Frontier records embedded in coverage rationales (see diagnostic_publication.frontier_record)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_default_diag_record_problem(e jsonb) RETURNS text
LANGUAGE plpgsql STABLE SET search_path = public, pg_temp AS $$
DECLARE
    src text;
    kind text;
BEGIN
    IF NOT bond_default_diag_keys_are(e, ARRAY['basis', 'code', 'filing_count', 'frontier', 'inventory_kind',
                                               'manifest_sha256', 'observed_at', 'reason_codes', 'source',
                                               'source_key', 'state']) THEN
        RETURN 'record_keys';
    END IF;
    IF pg_catalog.jsonb_typeof(e -> 'source') <> 'string' OR pg_catalog.jsonb_typeof(e -> 'inventory_kind') <> 'string'
       OR pg_catalog.jsonb_typeof(e -> 'state') <> 'string' OR pg_catalog.jsonb_typeof(e -> 'basis') <> 'string'
       OR pg_catalog.jsonb_typeof(e -> 'code') <> 'string' OR pg_catalog.jsonb_typeof(e -> 'manifest_sha256') <> 'string'
       OR pg_catalog.jsonb_typeof(e -> 'source_key') <> 'string' OR pg_catalog.jsonb_typeof(e -> 'observed_at') <> 'string' THEN
        RETURN 'record_types';
    END IF;
    src := e ->> 'source';
    kind := e ->> 'inventory_kind';
    IF (src, kind) NOT IN (('sec_nport', 'dera_packages'), ('sec_ncen', 'dera_packages'), ('sec_ncen', 'form_index'),
                           ('sec_edgar', 'submissions'), ('sec_edgar', 'census'), ('agency_rocr', 'agency_history')) THEN
        RETURN 'record_source_kind';
    END IF;
    IF (e ->> 'state') NOT IN ('inventory_only', 'unavailable') THEN
        RETURN 'record_state';
    END IF;
    IF (e ->> 'basis') !~ '^[A-Za-z0-9 _.,:;()+=-]{1,200}$' OR (e ->> 'code') !~ '^[a-z0-9_]{1,64}$'
       OR (e ->> 'manifest_sha256') !~ '^[0-9a-f]{64}$' OR (e ->> 'source_key') !~ '^[A-Za-z0-9_.:-]{1,128}$' THEN
        RETURN 'record_text';
    END IF;
    IF bond_default_diag_parse_ts(e ->> 'observed_at') IS NULL THEN
        RETURN 'record_observed_at';
    END IF;
    IF NOT (pg_catalog.jsonb_typeof(e -> 'frontier') = 'null'
            OR (pg_catalog.jsonb_typeof(e -> 'frontier') = 'string'
                AND bond_default_diag_parse_date(e ->> 'frontier') IS NOT NULL)) THEN
        RETURN 'record_frontier';
    END IF;
    IF NOT (pg_catalog.jsonb_typeof(e -> 'filing_count') = 'null'
            OR (pg_catalog.jsonb_typeof(e -> 'filing_count') = 'number'
                AND CASE WHEN (e ->> 'filing_count') ~ '^(0|[1-9][0-9]{0,9})$'
                         THEN (e ->> 'filing_count')::pg_catalog.int8 <= 2147483647 ELSE false END)) THEN
        RETURN 'record_filing_count';
    END IF;
    IF NOT bond_default_diag_codes_ok(e -> 'reason_codes') THEN
        RETURN 'record_reason_codes';
    END IF;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION bond_default_diag_rationale_problem(r jsonb) RETURNS text
LANGUAGE plpgsql STABLE SET search_path = public, pg_temp AS $$
DECLARE
    problem text;
BEGIN
    IF r IS NULL OR pg_catalog.jsonb_typeof(r) <> 'object' THEN
        RETURN 'rationale_not_json_object';
    END IF;
    IF NOT bond_default_diag_keys_are(r, ARRAY['format', 'frontiers', 'reason_codes']) THEN
        RETURN 'rationale_keys';
    END IF;
    IF r -> 'format' IS DISTINCT FROM '"bond_default_coverage_rationale_v1"'::pg_catalog.jsonb THEN
        RETURN 'rationale_format';
    END IF;
    IF pg_catalog.jsonb_typeof(r -> 'frontiers') <> 'array' OR pg_catalog.jsonb_array_length(r -> 'frontiers') > 16 THEN
        RETURN 'rationale_frontiers';
    END IF;
    IF NOT bond_default_diag_codes_ok(r -> 'reason_codes') THEN
        RETURN 'rationale_reason_codes';
    END IF;
    SELECT bond_default_diag_record_problem(e) INTO problem
    FROM pg_catalog.jsonb_array_elements(r -> 'frontiers') e
    WHERE bond_default_diag_record_problem(e) IS NOT NULL LIMIT 1;
    RETURN problem;
END $$;

-- Sorted distinct frontier records of every coverage rationale of a publication (validated).
CREATE OR REPLACE FUNCTION bond_default_diag_frontier_records(target_publication_id uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path = public, pg_temp AS $$
DECLARE
    bad text;
    recs jsonb;
    n_records integer;
    n_pairs integer;
    k timestamptz;
BEGIN
    SELECT p.knowledge_cutoff INTO k FROM bond_credit_publications p WHERE p.publication_id = target_publication_id;
    SELECT c.period_label || '/' || c.source || '/' || c.event_type || ':'
           || bond_default_diag_rationale_problem(bond_default_diag_try_jsonb(c.rationale)) INTO bad
    FROM bond_default_coverage_v1 c
    WHERE c.publication_id = target_publication_id
      AND bond_default_diag_rationale_problem(bond_default_diag_try_jsonb(c.rationale)) IS NOT NULL
    ORDER BY c.period_label, c.source, c.event_type LIMIT 1;
    IF bad IS NOT NULL THEN
        PERFORM bond_default_diag_raise('coverage_invalid', bad);
    END IF;
    SELECT pg_catalog.jsonb_agg(x.rec ORDER BY x.rec ->> 'source' COLLATE "C", x.rec ->> 'inventory_kind' COLLATE "C"),
           pg_catalog.count(*),
           pg_catalog.count(DISTINCT (x.rec ->> 'source') || '/' || (x.rec ->> 'inventory_kind'))
    INTO recs, n_records, n_pairs
    FROM (SELECT DISTINCT e AS rec
          FROM bond_default_coverage_v1 c
          CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(bond_default_diag_try_jsonb(c.rationale) -> 'frontiers') e
          WHERE c.publication_id = target_publication_id) x;
    IF n_records = 0 THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'no_frontier_records');
    END IF;
    IF n_records <> n_pairs THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'frontier_records_inconsistent');
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(recs) r
               WHERE bond_default_diag_parse_ts(r ->> 'observed_at') > k) THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'frontier_observed_after_cutoff');
    END IF;
    RETURN recs;
END $$;

-- Digest bound to the embedded records (contracts-side: diagnostic_publication.frontier_manifest_digest).
CREATE OR REPLACE FUNCTION bond_default_diag_frontier_digest(records jsonb) RETURNS text
LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
    SELECT bond_credit_json_digest(pg_catalog.jsonb_build_object(
        'version', 'bond_default_frontier_manifest_v1', 'records', records))
$$;

-- ---------------------------------------------------------------------------
-- Projection derivation from the persisted, structurally validated frames
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_default_diag_derive(target_publication_id uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path = public, pg_temp AS $$
DECLARE
    pub bond_credit_publications%ROWTYPE;
    bad text;
    n_periods integer;
    n_cells integer;
    first_p date;
    last_p date;
    candidate bigint;
    at_target bigint;
    n_pit bigint;
    n_audit bigint;
    recs jsonb;
    cells jsonb;
    frontiers jsonb;
    limitations jsonb;
BEGIN
    SELECT * INTO pub FROM bond_credit_publications p WHERE p.publication_id = target_publication_id;
    IF NOT FOUND THEN
        PERFORM bond_default_diag_raise('unknown_publication');
    END IF;
    IF pub.sources_count <> 0 OR pub.events_count <> 0 OR pub.followups_count <> 0 OR pub.exits_count <> 0
       OR pub.family_contexts_count <> 0 OR pub.family_evidence_count <> 0 OR pub.proposal_evidence_count <> 0
       OR pub.exchange_relations_count <> 0 THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'frames_not_empty');
    END IF;
    IF pub.ratings_count::bigint <> 2::bigint * pub.panel_grid_count::bigint OR pub.coverage_count = 0 THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'frame_counts');
    END IF;
    IF EXISTS (SELECT 1 FROM bond_rating_history_public_v1 r
               WHERE r.publication_id = pub.publication_id
                 AND (r.state <> 'missing' OR r.bucket IS NOT NULL OR r.action_date IS NOT NULL
                      OR r.public_known_at IS NOT NULL OR r.coverage_frontier IS NOT NULL
                      OR r.action_input_digest IS NOT NULL OR r.default_overlay_episode_id IS NOT NULL
                      OR pg_catalog.cardinality(r.agency_source_ids) <> 0
                      OR pg_catalog.cardinality(r.binding_link_ids) <> 0)) THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'ratings_not_all_missing');
    END IF;

    SELECT c.period_label || '/' || c.source || '/' || c.event_type INTO bad
    FROM bond_default_coverage_v1 c
    WHERE c.publication_id = pub.publication_id
      AND NOT (c.period_label ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'
               AND (c.source, c.event_type) IN (('all', 'all'), ('sec_nport', 'default_state'),
                                                ('sec_edgar', 'bankruptcy'), ('sec_edgar', 'payment_default'),
                                                ('sec_edgar', 'distressed_exchange'),
                                                ('agency_rocr', 'agency_issue_default'))
               AND c.rating_stratum = 'unknown' AND c.exposure_cohort = 'all' AND c.state = 'unavailable'
               AND c.denominator_basis = 'panel_exposure' AND c.denominator_count = c.exposed_issue_months
               AND c.event_count = 0 AND c.unlinked_count = 0 AND c.date_uncertain_count = 0
               AND c.unknown_outcome_issue_months = c.exposed_issue_months
               AND c.lag_p50_days IS NULL AND c.lag_p90_days IS NULL AND c.lag_max_days IS NULL
               AND c.validation_receipt_digest IS NULL)
    ORDER BY c.period_label, c.source, c.event_type LIMIT 1;
    IF bad IS NOT NULL THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'cell_shape:' || bad);
    END IF;

    SELECT pg_catalog.count(*), pg_catalog.count(DISTINCT c.period_label),
           pg_catalog.min((c.period_label || '-01')::pg_catalog.date), pg_catalog.max((c.period_label || '-01')::pg_catalog.date)
    INTO n_cells, n_periods, first_p, last_p
    FROM bond_default_coverage_v1 c WHERE c.publication_id = pub.publication_id;
    IF n_cells <> 6 * n_periods THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'cells_per_period');
    END IF;
    IF last_p <> pub.target_month THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'last_period_not_target_month');
    END IF;
    IF (EXTRACT(year FROM last_p)::integer * 12 + EXTRACT(month FROM last_p)::integer)
       - (EXTRACT(year FROM first_p)::integer * 12 + EXTRACT(month FROM first_p)::integer) + 1 <> n_periods THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'periods_not_contiguous');
    END IF;

    SELECT pg_catalog.count(*) FILTER (WHERE r.view_kind = 'public_pit'),
           pg_catalog.count(*) FILTER (WHERE r.view_kind = 'effective_audit'),
           pg_catalog.count(*) FILTER (WHERE r.view_kind = 'public_pit' AND r.month = last_p)
    INTO n_pit, n_audit, at_target
    FROM bond_rating_history_public_v1 r WHERE r.publication_id = pub.publication_id;
    IF n_pit <> pub.panel_grid_count::bigint OR n_audit <> pub.panel_grid_count::bigint THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'ratings_not_two_per_key');
    END IF;
    IF EXISTS (SELECT 1 FROM bond_rating_history_public_v1 r
               WHERE r.publication_id = pub.publication_id
                 AND (r.month < (first_p - INTERVAL '1 month')::pg_catalog.date OR r.month > last_p)) THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'ratings_outside_window');
    END IF;
    SELECT c.period_label INTO bad
    FROM bond_default_coverage_v1 c
    LEFT JOIN (SELECT r.month, pg_catalog.count(*) AS n
               FROM bond_rating_history_public_v1 r
               WHERE r.publication_id = pub.publication_id AND r.view_kind = 'public_pit'
               GROUP BY r.month) s
      ON s.month = ((c.period_label || '-01')::pg_catalog.date - INTERVAL '1 month')::pg_catalog.date
    WHERE c.publication_id = pub.publication_id AND c.exposed_issue_months <> COALESCE(s.n, 0)
    ORDER BY c.period_label, c.source, c.event_type LIMIT 1;
    IF bad IS NOT NULL THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'exposure_not_panel_start_count:' || bad);
    END IF;
    SELECT COALESCE(pg_catalog.sum(c.exposed_issue_months), 0) INTO candidate
    FROM bond_default_coverage_v1 c
    WHERE c.publication_id = pub.publication_id AND c.source = 'all' AND c.event_type = 'all';
    IF candidate + at_target <> pub.panel_grid_count::bigint THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'grid_count_mismatch');
    END IF;

    recs := bond_default_diag_frontier_records(pub.publication_id);
    IF (SELECT pg_catalog.count(DISTINCT r ->> 'manifest_sha256') FROM pg_catalog.jsonb_array_elements(recs) r) <> 1 THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'manifest_sha256_not_single');
    END IF;
    -- A source cell embeds only its own source's frontier records; its source_frontier is NULL or one of
    -- their frontiers. The aggregate all/all cell never carries a source_frontier.
    SELECT c.period_label || '/' || c.source || '/' || c.event_type INTO bad
    FROM bond_default_coverage_v1 c
    WHERE c.publication_id = pub.publication_id
      AND ((c.source = 'all' AND c.source_frontier IS NOT NULL)
           OR (c.source <> 'all'
               AND (NOT EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(
                                    bond_default_diag_try_jsonb(c.rationale) -> 'frontiers') e)
                    OR EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(
                                   bond_default_diag_try_jsonb(c.rationale) -> 'frontiers') e
                               WHERE e ->> 'source' <> c.source)
                    OR (c.source_frontier IS NOT NULL
                        AND NOT EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(
                                            bond_default_diag_try_jsonb(c.rationale) -> 'frontiers') e
                                        WHERE bond_default_diag_parse_date(e ->> 'frontier') = c.source_frontier)))))
    ORDER BY c.period_label, c.source, c.event_type LIMIT 1;
    IF bad IS NOT NULL THEN
        PERFORM bond_default_diag_raise('coverage_invalid', 'cell_frontier:' || bad);
    END IF;
    SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
               'source', r -> 'source', 'inventory_kind', r -> 'inventory_kind', 'frontier', r -> 'frontier',
               'observed_at', r -> 'observed_at', 'state', r -> 'state', 'filing_count', r -> 'filing_count',
               'reason_codes', r -> 'reason_codes')
               ORDER BY r ->> 'source' COLLATE "C", r ->> 'inventory_kind' COLLATE "C")
    INTO frontiers FROM pg_catalog.jsonb_array_elements(recs) r;

    SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
               'period_label', c.period_label, 'source', c.source, 'event_type', c.event_type,
               'rating_stratum', c.rating_stratum, 'exposure_cohort', c.exposure_cohort, 'state', c.state,
               'denominator_basis', c.denominator_basis, 'denominator_count', c.denominator_count,
               'exposed_issue_months', c.exposed_issue_months, 'event_count', c.event_count,
               'unlinked_count', c.unlinked_count, 'date_uncertain_count', c.date_uncertain_count,
               'unknown_outcome_issue_months', c.unknown_outcome_issue_months,
               'source_frontier', pg_catalog.to_char(c.source_frontier, 'YYYY-MM-DD'),
               'reason_codes', bond_default_diag_try_jsonb(c.rationale) -> 'reason_codes')
               ORDER BY c.period_label COLLATE "C", c.source COLLATE "C", c.event_type COLLATE "C")
    INTO cells FROM bond_default_coverage_v1 c WHERE c.publication_id = pub.publication_id;

    SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t.v) ORDER BY t.v COLLATE "C"), '[]'::pg_catalog.jsonb)
    INTO limitations
    FROM (SELECT pg_catalog.unnest(bond_default_diag_base_limitations()) AS v
          UNION SELECT x #>> '{}' FROM pg_catalog.jsonb_array_elements(recs) r,
                                       pg_catalog.jsonb_array_elements(r -> 'reason_codes') x
          UNION SELECT x #>> '{}' FROM bond_default_coverage_v1 c,
                                       pg_catalog.jsonb_array_elements(bond_default_diag_try_jsonb(c.rationale) -> 'reason_codes') x
                WHERE c.publication_id = pub.publication_id) t;

    RETURN pg_catalog.jsonb_build_object(
        'schema_version', 'bond_default_events_display_v1',
        'product', 'bond_default_events_diagnostic_v1',
        'tier', 'experimental_partial',
        'display_mode', 'coverage_only',
        'quality_state', 'partial',
        'build_scope', 'limited',
        'recommendation_eligible', false,
        'source_frontiers', frontiers,
        'coverage', cells,
        'accepted_events', '[]'::pg_catalog.jsonb,
        'counts', pg_catalog.jsonb_build_object(
            'panel_grid_keys', pub.panel_grid_count,
            'candidate_issue_months', candidate,
            'accepted_events', 0,
            'unknown_outcome_issue_months', candidate,
            'unresolved_events', NULL::text,
            'censored_issue_months', NULL::text,
            'reason_codes', '["censoring_not_evaluated","unresolved_not_evaluated"]'::pg_catalog.jsonb),
        'limitations', limitations);
END $$;

-- Allowlist check of a supplied / stored projection (keys, constants, no paths, integer numbers).
-- Field-level equality with the derived projection is enforced separately (prepare / verify / promote).
CREATE OR REPLACE FUNCTION bond_default_diag_projection_problem(p jsonb) RETURNS text
LANGUAGE plpgsql STABLE SET search_path = public, pg_temp AS $$
BEGIN
    IF NOT bond_default_diag_keys_are(p, ARRAY['accepted_events', 'build_scope', 'counts', 'coverage', 'display_mode',
                                               'limitations', 'product', 'quality_state', 'recommendation_eligible',
                                               'schema_version', 'source_frontiers', 'tier']) THEN
        RETURN 'keys';
    END IF;
    IF p -> 'schema_version' IS DISTINCT FROM '"bond_default_events_display_v1"'::pg_catalog.jsonb
       OR p -> 'product' IS DISTINCT FROM '"bond_default_events_diagnostic_v1"'::pg_catalog.jsonb
       OR p -> 'tier' IS DISTINCT FROM '"experimental_partial"'::pg_catalog.jsonb
       OR p -> 'display_mode' IS DISTINCT FROM '"coverage_only"'::pg_catalog.jsonb
       OR p -> 'quality_state' IS DISTINCT FROM '"partial"'::pg_catalog.jsonb
       OR p -> 'build_scope' IS DISTINCT FROM '"limited"'::pg_catalog.jsonb
       OR p -> 'recommendation_eligible' IS DISTINCT FROM 'false'::pg_catalog.jsonb THEN
        RETURN 'constants';
    END IF;
    IF pg_catalog.jsonb_typeof(p -> 'accepted_events') <> 'array' OR pg_catalog.jsonb_array_length(p -> 'accepted_events') <> 0 THEN
        RETURN 'accepted_events';
    END IF;
    IF pg_catalog.jsonb_typeof(p -> 'source_frontiers') <> 'array' OR pg_catalog.jsonb_typeof(p -> 'coverage') <> 'array'
       OR pg_catalog.jsonb_array_length(p -> 'source_frontiers') > 16 OR pg_catalog.jsonb_array_length(p -> 'coverage') > 4000 THEN
        RETURN 'collections';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(p -> 'source_frontiers') e
               WHERE NOT bond_default_diag_keys_are(e, ARRAY['filing_count', 'frontier', 'inventory_kind', 'observed_at',
                                                            'reason_codes', 'source', 'state'])) THEN
        RETURN 'frontier_keys';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements(p -> 'coverage') e
               WHERE NOT bond_default_diag_keys_are(e, ARRAY['date_uncertain_count', 'denominator_basis', 'denominator_count',
                                                            'event_count', 'event_type', 'exposed_issue_months',
                                                            'exposure_cohort', 'period_label', 'rating_stratum',
                                                            'reason_codes', 'source', 'source_frontier', 'state',
                                                            'unknown_outcome_issue_months', 'unlinked_count'])) THEN
        RETURN 'coverage_keys';
    END IF;
    IF NOT bond_default_diag_keys_are(p -> 'counts', ARRAY['accepted_events', 'candidate_issue_months', 'censored_issue_months',
                                                          'panel_grid_keys', 'reason_codes', 'unknown_outcome_issue_months',
                                                          'unresolved_events']) THEN
        RETURN 'counts_keys';
    END IF;
    IF NOT bond_default_diag_codes_ok(p -> 'limitations') THEN
        RETURN 'limitations';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.jsonb_path_query(p, 'strict $.**') v
               WHERE (pg_catalog.jsonb_typeof(v) = 'string' AND (v #>> '{}') ~ '[/\\]')
                  OR (pg_catalog.jsonb_typeof(v) = 'number'
                      AND ((v #>> '{}') !~ '^(0|[1-9][0-9]{0,9})$'
                           OR COALESCE(CASE WHEN (v #>> '{}') ~ '^(0|[1-9][0-9]{0,9})$'
                                            THEN (v #>> '{}')::pg_catalog.int8 > 2147483647 END, true)))) THEN
        RETURN 'string_or_number';
    END IF;
    RETURN NULL;
END $$;

-- Deterministic release identity (excludes audit timestamps and created_by).
CREATE OR REPLACE FUNCTION bond_default_diag_identity(
    publication uuid, publication_fingerprint text, target_month date, knowledge_cutoff timestamptz,
    policy_digest text, contract_digest text, frontier_digest text, projection_digest text, sql_digest text
) RETURNS jsonb LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
    SELECT pg_catalog.jsonb_build_object(
        'namespace', 'https://investintell.local/contracts/bonds/bond_default_events_diagnostic_v1',
        'product', 'bond_default_events_diagnostic_v1',
        'publication_id', publication::text,
        'publication_fingerprint', publication_fingerprint,
        'display_projection_version', 'bond_default_events_display_v1',
        'tier', 'experimental_partial',
        'target_month', pg_catalog.to_char(target_month, 'YYYY-MM-DD'),
        'knowledge_cutoff', bond_credit_ts_text(knowledge_cutoff),
        'policy_digest', policy_digest,
        'contract_digest', contract_digest,
        'source_frontier_manifest_digest', frontier_digest,
        'projection_digest', projection_digest,
        'diagnostic_sql_digest', sql_digest)
$$;

-- Deterministic UUIDv5 under the contract namespace over the canonical identity digest
-- (bond_credit_uuid5 / bond_credit_identity_name are the pinned SQL mirrors of contracts.uuid5_of).
CREATE OR REPLACE FUNCTION bond_default_diag_release_id_for(identity jsonb) RETURNS uuid
LANGUAGE sql STABLE SET search_path = public, pg_temp AS $$
    SELECT bond_credit_uuid5('bb022049-6738-5033-b532-d19b7c29c541'::uuid,
        bond_credit_identity_name('bond_default_diagnostic_release', ARRAY[bond_credit_json_digest(identity)]))
$$;

-- Every guard shared by verify / promote / current reader. deep = re-derive the projection from the
-- persisted frames and recompute the frontier digest (writer paths); the runtime reader skips it.
CREATE OR REPLACE FUNCTION bond_default_diag_guard(target_release_id uuid, deep boolean)
RETURNS bond_default_diagnostic_releases
LANGUAGE plpgsql STABLE SET search_path = public, pg_temp AS $$
DECLARE
    rel bond_default_diagnostic_releases%ROWTYPE;
    pub bond_credit_publications%ROWTYPE;
    problem text;
    pins record;
BEGIN
    SELECT * INTO rel FROM bond_default_diagnostic_releases r WHERE r.release_id = target_release_id;
    IF NOT FOUND THEN
        PERFORM bond_default_diag_raise('unknown_release');
    END IF;
    IF EXISTS (SELECT 1 FROM bond_default_diagnostic_revocations v WHERE v.release_id = rel.release_id) THEN
        PERFORM bond_default_diag_raise('diagnostic_release_revoked');
    END IF;
    SELECT * INTO pub FROM bond_credit_publications p WHERE p.publication_id = rel.publication_id;
    IF NOT FOUND THEN
        PERFORM bond_default_diag_raise('diagnostic_invalid', 'publication_missing');
    END IF;
    IF bond_credit_is_revoked(pub.publication_id) THEN
        PERFORM bond_default_diag_raise('credit_publication_revoked');
    END IF;
    SELECT x.contract_version, x.policy_digest, x.contract_digest INTO pins FROM bond_credit_expected_pins() x;
    IF pub.contract_version IS DISTINCT FROM pins.contract_version OR pub.policy_digest IS DISTINCT FROM pins.policy_digest
       OR pub.contract_digest IS DISTINCT FROM pins.contract_digest THEN
        PERFORM bond_default_diag_raise('diagnostic_contract_unsupported', 'publication_pins');
    END IF;
    IF rel.display_projection_version <> 'bond_default_events_display_v1' OR rel.product <> 'bond_default_events_diagnostic_v1'
       OR rel.tier <> 'experimental_partial' THEN
        PERFORM bond_default_diag_raise('diagnostic_contract_unsupported', 'release_version');
    END IF;
    IF pub.lifecycle_state <> 'validated' THEN
        PERFORM bond_default_diag_raise('diagnostic_invalid', 'publication_not_validated');
    END IF;
    IF pub.quality_state <> 'partial' OR pub.build_scope <> 'limited' THEN
        PERFORM bond_default_diag_raise('diagnostic_invalid', 'publication_not_partial_limited');
    END IF;
    IF rel.target_month <> pub.target_month OR rel.knowledge_cutoff <> pub.knowledge_cutoff
       OR rel.publication_fingerprint <> pub.fingerprint_digest OR rel.policy_digest <> pub.policy_digest
       OR rel.contract_digest <> pub.contract_digest THEN
        PERFORM bond_default_diag_raise('diagnostic_invalid', 'release_publication_mismatch');
    END IF;
    IF bond_credit_json_digest(rel.display_projection) <> rel.projection_digest THEN
        PERFORM bond_default_diag_raise('diagnostic_invalid', 'projection_digest_mismatch');
    END IF;
    IF bond_default_diag_release_id_for(bond_default_diag_identity(
           rel.publication_id, rel.publication_fingerprint, rel.target_month, rel.knowledge_cutoff, rel.policy_digest,
           rel.contract_digest, rel.source_frontier_manifest_digest, rel.projection_digest,
           rel.diagnostic_sql_digest)) <> rel.release_id THEN
        PERFORM bond_default_diag_raise('diagnostic_invalid', 'release_id_mismatch');
    END IF;
    problem := bond_default_diag_projection_problem(rel.display_projection);
    IF problem IS NOT NULL THEN
        PERFORM bond_default_diag_raise('diagnostic_invalid', 'projection_' || problem);
    END IF;
    IF pg_catalog.jsonb_array_length(rel.display_projection -> 'coverage') <> pub.coverage_count OR pub.events_count <> 0
       OR pub.ratings_count::bigint <> 2::bigint * pub.panel_grid_count::bigint
       OR (rel.display_projection #>> '{counts,panel_grid_keys}')::bigint <> pub.panel_grid_count::bigint THEN
        PERFORM bond_default_diag_raise('diagnostic_invalid', 'projection_header_mismatch');
    END IF;
    IF deep THEN
        IF bond_default_diag_derive(pub.publication_id) IS DISTINCT FROM rel.display_projection THEN
            PERFORM bond_default_diag_raise('diagnostic_invalid', 'projection_not_derived_from_frames');
        END IF;
        IF bond_default_diag_frontier_digest(bond_default_diag_frontier_records(pub.publication_id))
           <> rel.source_frontier_manifest_digest THEN
            PERFORM bond_default_diag_raise('diagnostic_invalid', 'frontier_digest_mismatch');
        END IF;
    END IF;
    RETURN rel;
END $$;

-- ---------------------------------------------------------------------------
-- 1. prepare (writer only)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_default_prepare_diagnostic(
    target_publication_id uuid, projection jsonb, frontier_digest text, diagnostic_sql_digest text
) RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
    pub bond_credit_publications%ROWTYPE;
    pins record;
    problem text;
    projection_digest text;
    rid uuid;
    existing bond_default_diagnostic_releases%ROWTYPE;
BEGIN
    IF target_publication_id IS NULL OR projection IS NULL OR frontier_digest IS NULL OR diagnostic_sql_digest IS NULL THEN
        PERFORM bond_default_diag_raise('invalid_argument', 'null');
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('bond_default_events_diagnostic_v1|prepare|' || target_publication_id::text, 0));
    SELECT * INTO pub FROM bond_credit_publications p WHERE p.publication_id = target_publication_id;
    IF NOT FOUND THEN
        PERFORM bond_default_diag_raise('unknown_publication');
    END IF;
    IF pub.lifecycle_state <> 'validated' THEN
        PERFORM bond_default_diag_raise('publication_not_validated');
    END IF;
    IF bond_credit_is_revoked(pub.publication_id) THEN
        PERFORM bond_default_diag_raise('credit_publication_revoked');
    END IF;
    -- The same guarded reader the qualified path uses (shadow allowed: a diagnostic is never qualified).
    PERFORM bond_credit_read_publication(pub.publication_id, true);
    IF pub.quality_state <> 'partial' OR pub.build_scope <> 'limited' THEN
        PERFORM bond_default_diag_raise('publication_not_partial_limited');
    END IF;
    SELECT x.contract_version, x.policy_digest, x.contract_digest INTO pins FROM bond_credit_expected_pins() x;
    IF pub.contract_version IS DISTINCT FROM pins.contract_version OR pub.policy_digest IS DISTINCT FROM pins.policy_digest
       OR pub.contract_digest IS DISTINCT FROM pins.contract_digest THEN
        PERFORM bond_default_diag_raise('contract_pin_mismatch');
    END IF;
    IF frontier_digest !~ '^sha256:[0-9a-f]{64}$' OR diagnostic_sql_digest !~ '^sha256:[0-9a-f]{64}$' THEN
        PERFORM bond_default_diag_raise('invalid_argument', 'digest_format');
    END IF;
    problem := bond_default_diag_projection_problem(projection);
    IF problem IS NOT NULL THEN
        PERFORM bond_default_diag_raise('projection_invalid', problem);
    END IF;
    IF bond_default_diag_derive(pub.publication_id) IS DISTINCT FROM projection THEN
        PERFORM bond_default_diag_raise('projection_mismatch');
    END IF;
    IF bond_default_diag_frontier_digest(bond_default_diag_frontier_records(pub.publication_id)) <> frontier_digest THEN
        PERFORM bond_default_diag_raise('frontier_digest_mismatch');
    END IF;
    projection_digest := bond_credit_json_digest(projection);
    rid := bond_default_diag_release_id_for(bond_default_diag_identity(
        pub.publication_id, pub.fingerprint_digest, pub.target_month, pub.knowledge_cutoff, pub.policy_digest,
        pub.contract_digest, frontier_digest, projection_digest, diagnostic_sql_digest));
    SELECT * INTO existing FROM bond_default_diagnostic_releases r WHERE r.release_id = rid;
    IF FOUND THEN
        IF (existing.publication_id, existing.publication_fingerprint, existing.target_month, existing.knowledge_cutoff,
            existing.policy_digest, existing.contract_digest, existing.source_frontier_manifest_digest,
            existing.projection_digest, existing.diagnostic_sql_digest)
           IS DISTINCT FROM
           (pub.publication_id, pub.fingerprint_digest, pub.target_month, pub.knowledge_cutoff, pub.policy_digest,
            pub.contract_digest, frontier_digest, projection_digest, diagnostic_sql_digest)
           OR existing.display_projection IS DISTINCT FROM projection THEN
            PERFORM bond_default_diag_raise('release_collision');
        END IF;
        RETURN rid;
    END IF;
    INSERT INTO bond_default_diagnostic_releases (
        release_id, publication_id, product, target_month, knowledge_cutoff, tier, display_projection_version,
        publication_fingerprint, policy_digest, contract_digest, source_frontier_manifest_digest, projection_digest,
        diagnostic_sql_digest, display_projection, created_by)
    VALUES (rid, pub.publication_id, 'bond_default_events_diagnostic_v1', pub.target_month, pub.knowledge_cutoff,
            'experimental_partial', 'bond_default_events_display_v1', pub.fingerprint_digest, pub.policy_digest,
            pub.contract_digest, frontier_digest, projection_digest, diagnostic_sql_digest, projection,
            session_user::text);
    RETURN rid;
END $$;

-- ---------------------------------------------------------------------------
-- 2. promote (writer only): CAS under a transaction-scoped product lock taken BEFORE any lookup
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_default_promote_diagnostic(target_release_id uuid, expected_release_id uuid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
    current_id uuid;
    rel bond_default_diagnostic_releases%ROWTYPE;
    cur bond_default_diagnostic_releases%ROWTYPE;
BEGIN
    IF target_release_id IS NULL THEN
        PERFORM bond_default_diag_raise('invalid_argument', 'null_release');
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bond_default_events_diagnostic_v1|pointer', 0));
    SELECT d.release_id INTO current_id FROM bond_default_diagnostic_pointer d
    WHERE d.product = 'bond_default_events_diagnostic_v1' FOR UPDATE;
    IF current_id IS DISTINCT FROM expected_release_id THEN
        PERFORM bond_default_diag_raise('cas_mismatch', 'expected=' || COALESCE(expected_release_id::text, 'absent')
                                        || ' current=' || COALESCE(current_id::text, 'absent'));
    END IF;
    rel := bond_default_diag_guard(target_release_id, true);
    IF current_id = rel.release_id THEN
        RETURN;
    END IF;
    IF current_id IS NOT NULL THEN
        SELECT * INTO cur FROM bond_default_diagnostic_releases r WHERE r.release_id = current_id;
        IF rel.target_month < cur.target_month THEN
            PERFORM bond_default_diag_raise('t_regression', cur.target_month::text || ' -> ' || rel.target_month::text);
        END IF;
        IF rel.knowledge_cutoff < cur.knowledge_cutoff THEN
            PERFORM bond_default_diag_raise('k_regression', bond_credit_ts_text(cur.knowledge_cutoff) || ' -> '
                                            || bond_credit_ts_text(rel.knowledge_cutoff));
        END IF;
    END IF;
    PERFORM pg_catalog.set_config('bond_default_diagnostic.pointer_token',
        pg_catalog.pg_backend_pid()::text || ':' || pg_catalog.txid_current()::text, true);
    INSERT INTO bond_default_diagnostic_pointer (product, release_id, promoted_by)
    VALUES ('bond_default_events_diagnostic_v1', rel.release_id, session_user::text)
    ON CONFLICT (product) DO UPDATE SET release_id = EXCLUDED.release_id,
                                        promoted_at = pg_catalog.clock_timestamp(),
                                        promoted_by = EXCLUDED.promoted_by;
    PERFORM pg_catalog.set_config('bond_default_diagnostic.pointer_token', '', true);
END $$;

-- ---------------------------------------------------------------------------
-- 3. the ONLY runtime reader: complete allowlisted response of the elected release
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_default_current_diagnostic_release() RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
    ptr bond_default_diagnostic_pointer%ROWTYPE;
    rel bond_default_diagnostic_releases%ROWTYPE;
    pub bond_credit_publications%ROWTYPE;
BEGIN
    SELECT * INTO ptr FROM bond_default_diagnostic_pointer d WHERE d.product = 'bond_default_events_diagnostic_v1';
    IF NOT FOUND THEN
        PERFORM bond_default_diag_raise('diagnostic_not_published', NULL, 'P0002');
    END IF;
    rel := bond_default_diag_guard(ptr.release_id, false);
    SELECT * INTO pub FROM bond_credit_publications p WHERE p.publication_id = rel.publication_id;
    RETURN rel.display_projection || pg_catalog.jsonb_build_object(
        'release_id', rel.release_id,
        'publication_id', rel.publication_id,
        'panel_publication_id', pub.panel_publication_id,
        'target_month', pg_catalog.to_char(rel.target_month, 'YYYY-MM-DD'),
        'knowledge_cutoff', bond_credit_ts_text(rel.knowledge_cutoff),
        'built_at', bond_credit_ts_text(pub.prepared_at),
        'validated_at', bond_credit_ts_text(pub.validated_at),
        'promoted_at', bond_credit_ts_text(ptr.promoted_at));
END $$;

-- ---------------------------------------------------------------------------
-- 4. verify (writer / auditor) and revoke (writer)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_default_verify_diagnostic(target_release_id uuid) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
    rel bond_default_diagnostic_releases%ROWTYPE;
    pub bond_credit_publications%ROWTYPE;
    current_id uuid;
BEGIN
    IF target_release_id IS NULL THEN
        PERFORM bond_default_diag_raise('invalid_argument', 'null_release');
    END IF;
    rel := bond_default_diag_guard(target_release_id, true);
    SELECT * INTO pub FROM bond_credit_publications p WHERE p.publication_id = rel.publication_id;
    SELECT d.release_id INTO current_id FROM bond_default_diagnostic_pointer d
    WHERE d.product = 'bond_default_events_diagnostic_v1';
    RETURN pg_catalog.jsonb_build_object(
        'release_id', rel.release_id,
        'publication_id', rel.publication_id,
        'panel_publication_id', pub.panel_publication_id,
        'product', rel.product,
        'tier', rel.tier,
        'display_projection_version', rel.display_projection_version,
        'target_month', pg_catalog.to_char(rel.target_month, 'YYYY-MM-DD'),
        'knowledge_cutoff', bond_credit_ts_text(rel.knowledge_cutoff),
        'publication_fingerprint', rel.publication_fingerprint,
        'policy_digest', rel.policy_digest,
        'contract_digest', rel.contract_digest,
        'source_frontier_manifest_digest', rel.source_frontier_manifest_digest,
        'projection_digest', rel.projection_digest,
        'diagnostic_sql_digest', rel.diagnostic_sql_digest,
        'built_at', bond_credit_ts_text(pub.prepared_at),
        'validated_at', bond_credit_ts_text(pub.validated_at),
        'is_current', current_id IS NOT DISTINCT FROM rel.release_id,
        'counts', pg_catalog.jsonb_build_object(
            'coverage_cells', pg_catalog.jsonb_array_length(rel.display_projection -> 'coverage'),
            'source_frontiers', pg_catalog.jsonb_array_length(rel.display_projection -> 'source_frontiers'),
            'panel_grid_keys', rel.display_projection #> '{counts,panel_grid_keys}',
            'candidate_issue_months', rel.display_projection #> '{counts,candidate_issue_months}',
            'accepted_events', rel.display_projection #> '{counts,accepted_events}'),
        'projection', rel.display_projection);
END $$;

CREATE OR REPLACE FUNCTION bond_default_revoke_diagnostic(target_release_id uuid, reason_code text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
    existing text;
BEGIN
    IF target_release_id IS NULL OR reason_code IS NULL OR reason_code !~ '^[a-z][a-z0-9_]{2,63}$' THEN
        PERFORM bond_default_diag_raise('invalid_argument', 'revoke');
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bond_default_events_diagnostic_v1|revoke', 0));
    IF NOT EXISTS (SELECT 1 FROM bond_default_diagnostic_releases r WHERE r.release_id = target_release_id) THEN
        PERFORM bond_default_diag_raise('unknown_release');
    END IF;
    SELECT v.reason_code INTO existing FROM bond_default_diagnostic_revocations v WHERE v.release_id = target_release_id;
    IF FOUND THEN
        IF existing <> reason_code THEN
            PERFORM bond_default_diag_raise('revocation_conflict');
        END IF;
        RETURN;
    END IF;
    INSERT INTO bond_default_diagnostic_revocations (release_id, reason_code, revoked_by)
    VALUES (target_release_id, reason_code, session_user::text);
END $$;

-- ---------------------------------------------------------------------------
-- Privileges: PUBLIC loses everything created here; only the atomic reader is exposed to the runtime.
-- The production database defines ALTER DEFAULT PRIVILEGES that grant the API role (and a read-only
-- analytics role) DML / SELECT on every new public table. Such default grants must not survive on the
-- objects created here: sweep every explicit grantee other than the owner before the intended grants
-- (install-time DO block only; runtime functions build no dynamic SQL).
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT c.relname AS obj, pg_catalog.pg_get_userbyid(a.grantee) AS grantee
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(c.relacl, pg_catalog.acldefault('r', c.relowner))) a
        WHERE c.relname IN ('bond_default_diagnostic_releases', 'bond_default_diagnostic_pointer',
                            'bond_default_diagnostic_revocations')
          AND a.grantee <> 0 AND a.grantee <> c.relowner
        ORDER BY 1, 2
    LOOP
        EXECUTE pg_catalog.format('REVOKE ALL ON TABLE public.%I FROM %I', r.obj, r.grantee);
    END LOOP;
    FOR r IN
        SELECT p.proname AS obj, pg_catalog.pg_get_function_identity_arguments(p.oid) AS args,
               pg_catalog.pg_get_userbyid(a.grantee) AS grantee
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace AND n.nspname = 'public'
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))) a
        WHERE (p.proname LIKE 'bond\_default\_diag\_%'
               OR p.proname IN ('bond_default_prepare_diagnostic', 'bond_default_promote_diagnostic',
                                'bond_default_current_diagnostic_release', 'bond_default_verify_diagnostic',
                                'bond_default_revoke_diagnostic'))
          AND a.grantee <> 0 AND a.grantee <> p.proowner
        ORDER BY 1, 2, 3
    LOOP
        EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION public.%I(%s) FROM %I', r.obj, r.args, r.grantee);
    END LOOP;
END $$;

REVOKE ALL ON bond_default_diagnostic_releases, bond_default_diagnostic_pointer, bond_default_diagnostic_revocations
    FROM PUBLIC, bond_credit_reader, bond_credit_writer, bond_credit_auditor, bond_default_diagnostic_reader;
GRANT SELECT ON bond_default_diagnostic_releases, bond_default_diagnostic_pointer, bond_default_diagnostic_revocations
    TO bond_credit_auditor, bond_credit_writer;

REVOKE ALL ON FUNCTION
    bond_default_diag_raise(text, text, text), bond_default_diag_append_only(), bond_default_diag_pointer_guard(),
    bond_default_diag_try_jsonb(text), bond_default_diag_parse_date(text), bond_default_diag_parse_ts(text),
    bond_default_diag_limitation_codes(), bond_default_diag_base_limitations(), bond_default_diag_codes_ok(jsonb),
    bond_default_diag_keys_are(jsonb, text[]), bond_default_diag_record_problem(jsonb),
    bond_default_diag_rationale_problem(jsonb), bond_default_diag_frontier_records(uuid),
    bond_default_diag_frontier_digest(jsonb), bond_default_diag_derive(uuid), bond_default_diag_projection_problem(jsonb),
    bond_default_diag_identity(uuid, text, date, timestamptz, text, text, text, text, text),
    bond_default_diag_release_id_for(jsonb), bond_default_diag_guard(uuid, boolean),
    bond_default_prepare_diagnostic(uuid, jsonb, text, text), bond_default_promote_diagnostic(uuid, uuid),
    bond_default_current_diagnostic_release(), bond_default_verify_diagnostic(uuid),
    bond_default_revoke_diagnostic(uuid, text)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION
    bond_default_prepare_diagnostic(uuid, jsonb, text, text), bond_default_promote_diagnostic(uuid, uuid),
    bond_default_current_diagnostic_release(), bond_default_verify_diagnostic(uuid),
    bond_default_revoke_diagnostic(uuid, text)
    FROM bond_credit_reader, bond_credit_writer, bond_credit_auditor, bond_default_diagnostic_reader;
GRANT EXECUTE ON FUNCTION bond_default_prepare_diagnostic(uuid, jsonb, text, text),
    bond_default_promote_diagnostic(uuid, uuid), bond_default_revoke_diagnostic(uuid, text)
    TO bond_credit_writer;
GRANT EXECUTE ON FUNCTION bond_default_verify_diagnostic(uuid) TO bond_credit_writer, bond_credit_auditor;
GRANT EXECUTE ON FUNCTION bond_default_current_diagnostic_release() TO bond_default_diagnostic_reader;

COMMIT;
