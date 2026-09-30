-- bond_default_sources_v1: append-only input custody for public bond default evidence.
--
-- Contract: contracts/bonds/default_event_bundle_v2.schema.json (input frames
-- source_packages, observations, event_links, adjudications, ncen_filings) and
-- contracts/bonds/default_event_policy_v1.json. Additive only: no existing
-- table, function, allowlist or schema privilege is changed, and no global
-- CREATE revocation is performed.
--
-- Apply order: bond_default_sources_v1 -> bond_credit_publications_v1 ->
-- bond_default_events_v1 -> bond_rating_history_public_v1, each on an
-- autocommit session with `SET search_path TO <schema>, pg_temp`. Every function
-- captures exactly that path (`SET search_path FROM CURRENT`), so pg_temp is
-- searched last and application objects resolve in <schema> only.
--
-- Roles: bond_credit_reader / bond_credit_writer / bond_credit_auditor are NOLOGIN group
-- roles created here only when absent (never altered; memberships are never granted here).
-- The serving reader has no table privilege: it reads frames only through the guarded
-- SECURITY DEFINER bond_credit_read_* functions (bond_credit_publications_v1.sql). The
-- auditor has raw SELECT for audits; the writer appends.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

DO $$
BEGIN
    IF pg_catalog.current_schema() IS NULL
       OR pg_catalog.current_schema() IN ('pg_catalog', 'information_schema')
       OR pg_catalog.current_schema() LIKE 'pg\_temp%' THEN
        RAISE EXCEPTION 'bond_default_sources_v1: apply with SET search_path TO <schema>, pg_temp';
    END IF;
    -- W0 amendment 1 (bundle v2) stop gate: a v1 install is never upgraded in place. The v2
    -- files use CREATE ... IF NOT EXISTS, so installing over v1 tables would silently keep the
    -- v1 shapes; refuse instead (a disposable v1 schema is dropped and recreated).
    IF (pg_catalog.to_regclass('bond_default_adjudication') IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                        WHERE attrelid = pg_catalog.to_regclass('bond_default_adjudication')
                          AND attname = 'proposal_evidence_ids' AND NOT attisdropped))
       OR (pg_catalog.to_regclass('bond_credit_publications') IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                           WHERE attrelid = pg_catalog.to_regclass('bond_credit_publications')
                             AND attname = 'ncen_filing_inventory_digest' AND NOT attisdropped)) THEN
        RAISE EXCEPTION 'bond_default_sources_v1: installed bond_default_event_bundle_v1 schema %; in-place v1->v2 migration is not supported (W0 amendment 1 stop gate)',
            pg_catalog.current_schema();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'bond_credit_reader') THEN
        CREATE ROLE bond_credit_reader NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'bond_credit_writer') THEN
        CREATE ROLE bond_credit_writer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'bond_credit_auditor') THEN
        CREATE ROLE bond_credit_auditor NOLOGIN;
    END IF;
    EXECUTE pg_catalog.format(
        'GRANT USAGE ON SCHEMA %I TO bond_credit_reader, bond_credit_writer, bond_credit_auditor',
        pg_catalog.current_schema());
END $$;

-- ---------------------------------------------------------------------------
-- Shared pure helpers (used by CHECK constraints in all four files)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_credit_cusip9_valid(value text) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
DECLARE
    total integer := 0;
    v integer;
    ch text;
BEGIN
    IF value !~ '^[0-9A-Z*@#]{8}[0-9]$' THEN
        RETURN false;
    END IF;
    FOR i IN 1..8 LOOP
        ch := substr(value, i, 1);
        IF ch ~ '[0-9]' THEN
            v := ascii(ch) - 48;
        ELSIF ch ~ '[A-Z]' THEN
            v := ascii(ch) - 55;
        ELSIF ch = '*' THEN
            v := 36;
        ELSIF ch = '@' THEN
            v := 37;
        ELSE
            v := 38;
        END IF;
        IF i % 2 = 0 THEN
            v := v * 2;
        END IF;
        total := total + v / 10 + v % 10;
    END LOOP;
    RETURN ((10 - total % 10) % 10)::text = substr(value, 9, 1);
END $$;

CREATE OR REPLACE FUNCTION bond_credit_uuids_canonical(value uuid[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT pg_catalog.array_ndims(value) IS NULL
        OR (pg_catalog.array_ndims(value) = 1
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(value) u WHERE u IS NULL)
            AND value::text[] = ARRAY(SELECT s.t FROM (SELECT DISTINCT u::text COLLATE "C" AS t
                                                       FROM pg_catalog.unnest(value) u) s
                                      ORDER BY s.t))
$$;

CREATE OR REPLACE FUNCTION bond_credit_texts_canonical(value text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT pg_catalog.array_ndims(value) IS NULL
        OR (pg_catalog.array_ndims(value) = 1
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(value) u WHERE u IS NULL OR u !~ '\S')
            AND value = ARRAY(SELECT s.t FROM (SELECT DISTINCT u COLLATE "C" AS t
                                               FROM pg_catalog.unnest(value) u) s
                              ORDER BY s.t))
$$;

CREATE OR REPLACE FUNCTION bond_credit_family_role(family text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT CASE family
        WHEN 'sec_nport_dera' THEN 'nport_evidence'
        WHEN 'sec_nport_public_xml' THEN 'nport_evidence'
        WHEN 'sec_edgar_index' THEN 'edgar_evidence'
        WHEN 'sec_edgar_document' THEN 'edgar_evidence'
        WHEN 'issuer_public_document' THEN 'document_evidence'
        WHEN 'court_public_document' THEN 'document_evidence'
        WHEN 'agency_rocr_xbrl' THEN 'agency_evidence'
        WHEN 'link_batch' THEN 'link_inventory'
        WHEN 'adjudication_batch' THEN 'adjudication_inventory'
        WHEN 'sec_ncen_dera' THEN 'ncen_family_evidence'
        WHEN 'sec_ncen_public_xml' THEN 'ncen_family_evidence'
        WHEN 'sec_ncen_acceptance_header' THEN 'ncen_family_evidence'
    END
$$;

CREATE OR REPLACE FUNCTION bond_credit_append_only() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % refused', TG_TABLE_NAME, TG_OP;
END $$;

-- ---------------------------------------------------------------------------
-- Source packages
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_default_source_package (
    package_id uuid PRIMARY KEY,
    source_family text NOT NULL CHECK (source_family IN (
        'adjudication_batch', 'agency_rocr_xbrl', 'court_public_document',
        'issuer_public_document', 'link_batch', 'sec_edgar_document', 'sec_edgar_index',
        'sec_ncen_acceptance_header', 'sec_ncen_dera', 'sec_ncen_public_xml',
        'sec_nport_dera', 'sec_nport_public_xml')),
    external_id text NOT NULL CHECK (external_id ~ '\S'),
    content_sha256 char(64) NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    raw_sha256 char(64) NOT NULL CHECK (raw_sha256 ~ '^[0-9a-f]{64}$'),
    header_sha256 char(64) CHECK (header_sha256 ~ '^[0-9a-f]{64}$'),
    member_sha256s jsonb NOT NULL CHECK (jsonb_typeof(member_sha256s) = 'object'),
    official_url text CHECK (official_url ~ '\S'),
    accession_number text CHECK (accession_number ~ '\S'),
    rights_state text NOT NULL CHECK (rights_state IN (
        'approved', 'denied', 'internal_work_product', 'public_document_internal_use',
        'public_government_record', 'unverified')),
    rights_ref text CHECK (rights_ref ~ '\S'),
    parser_version text NOT NULL CHECK (parser_version ~ '\S'),
    schema_version text NOT NULL CHECK (schema_version ~ '\S'),
    retrieved_at timestamptz NOT NULL,
    first_verified_public_at timestamptz NOT NULL,
    public_time_basis text NOT NULL CHECK (public_time_basis IN (
        'archived_release_metadata', 'date_only_next_day_boundary', 'edgar_acceptance_datetime',
        'first_verified_retrieval', 'internal_record', 'rocr_file_creation')),
    public_time_evidence text NOT NULL CHECK (public_time_evidence ~ '\S'),
    source_coverage_start date,
    source_coverage_end date,
    public_coverage_start date,
    public_coverage_end date,
    effective_coverage_start date,
    effective_coverage_end date,
    raw_locator text NOT NULL CHECK (raw_locator ~ '\S' AND raw_locator !~ '^([/\\]|[A-Za-z]:)'
                                     AND raw_locator !~ '(^|[/\\])\.\.([/\\]|$)'),
    revision_of_package_id uuid REFERENCES bond_default_source_package(package_id),
    sec_run_id uuid,
    sec_package_id uuid,
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (source_family, external_id, content_sha256),
    CHECK (first_verified_public_at <= retrieved_at),
    CHECK (revision_of_package_id IS DISTINCT FROM package_id),
    CHECK ((source_coverage_start IS NULL) = (source_coverage_end IS NULL)
           AND (source_coverage_start IS NULL OR source_coverage_start <= source_coverage_end)),
    CHECK ((public_coverage_start IS NULL) = (public_coverage_end IS NULL)
           AND (public_coverage_start IS NULL OR public_coverage_start <= public_coverage_end)),
    CHECK ((effective_coverage_start IS NULL) = (effective_coverage_end IS NULL)
           AND (effective_coverage_start IS NULL OR effective_coverage_start <= effective_coverage_end)),
    -- Public source policy: rights gate per family; agency data only when approved.
    CHECK (
        (source_family IN ('sec_nport_dera', 'sec_nport_public_xml', 'sec_edgar_index',
                           'sec_edgar_document', 'sec_ncen_dera', 'sec_ncen_public_xml',
                           'sec_ncen_acceptance_header') AND rights_state = 'public_government_record')
        OR (source_family IN ('issuer_public_document', 'court_public_document')
            AND rights_state = 'public_document_internal_use')
        OR (source_family = 'agency_rocr_xbrl' AND rights_state = 'approved'
            AND rights_ref IS NOT NULL)
        OR (source_family IN ('link_batch', 'adjudication_batch')
            AND rights_state = 'internal_work_product')
    )
);
CREATE INDEX IF NOT EXISTS bond_default_source_package_accession_idx
    ON bond_default_source_package (accession_number) WHERE accession_number IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Observations (lexical values retained; interpretation never erases them)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_credit_observation (
    observation_id uuid PRIMARY KEY,
    package_id uuid NOT NULL REFERENCES bond_default_source_package(package_id),
    member_name text NOT NULL CHECK (member_name ~ '\S'),
    row_locator text NOT NULL CHECK (row_locator ~ '\S'),
    observation_kind text NOT NULL CHECK (observation_kind IN (
        'agency_action', 'court_document_passage', 'edgar_passage',
        'issuer_document_passage', 'nport_holding')),
    semantic_key text NOT NULL CHECK (semantic_key ~ '\S'),
    accession_number text CHECK (accession_number ~ '\S'),
    holding_id text CHECK (holding_id ~ '\S'),
    cusip_raw text,
    cusip9 char(9) CHECK (bond_credit_cusip9_valid(cusip9)),
    isin_raw text,
    security_id uuid,
    issuer_cik char(10) CHECK (issuer_cik ~ '^[0-9]{10}$'),
    registrant_cik char(10) CHECK (registrant_cik ~ '^[0-9]{10}$'),
    series_id text CHECK (series_id ~ '\S'),
    fund_family_id text CHECK (fund_family_id ~ '\S'),
    issuer_type_raw text,
    asset_category_raw text,
    report_date date,
    effective_date date,
    date_precision text NOT NULL CHECK (date_precision IN ('day', 'interval', 'month', 'unknown')),
    effective_lower_exclusive date,
    effective_upper_inclusive date,
    acceptance_raw text,
    acceptance_at timestamptz,
    public_available_at timestamptz NOT NULL,
    public_time_basis text NOT NULL CHECK (public_time_basis IN (
        'archived_release_metadata', 'date_only_next_day_boundary', 'edgar_acceptance_datetime',
        'first_verified_retrieval', 'internal_record', 'rocr_file_creation')),
    first_seen_at timestamptz NOT NULL,
    nport_is_default text,
    nport_arrears_or_deferral text,
    nport_paid_in_kind text,
    field_presence jsonb NOT NULL CHECK (jsonb_typeof(field_presence) = 'object'),
    agency_name text CHECK (agency_name ~ '\S'),
    agency_subject_kind text CHECK (agency_subject_kind IN ('instrument', 'issuer')),
    agency_rating_type text,
    agency_scale text,
    agency_currency text,
    agency_rating_symbol text,
    agency_action_classification text,
    agency_action_date date,
    agency_file_creation_at timestamptz,
    document_quote text,
    document_location text CHECK (document_location ~ '\S'),
    document_sha256 char(64) CHECK (document_sha256 ~ '^[0-9a-f]{64}$'),
    revision_kind text NOT NULL CHECK (revision_kind IN ('amendment', 'correction', 'original', 'retraction')),
    supersedes_observation_id uuid REFERENCES bond_credit_observation(observation_id),
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (package_id, member_name, row_locator),
    CHECK ((revision_kind = 'original') = (supersedes_observation_id IS NULL)),
    CHECK (supersedes_observation_id IS DISTINCT FROM observation_id),
    CHECK (date_precision <> 'day' OR effective_date IS NOT NULL),
    CHECK (CASE WHEN date_precision = 'interval'
                THEN effective_upper_inclusive IS NOT NULL
                     AND (effective_lower_exclusive IS NULL
                          OR effective_lower_exclusive < effective_upper_inclusive)
                ELSE effective_upper_inclusive IS NULL AND effective_lower_exclusive IS NULL END),
    CHECK (acceptance_at IS NULL OR acceptance_raw IS NOT NULL),
    CHECK (public_time_basis <> 'edgar_acceptance_datetime'
           OR (acceptance_at IS NOT NULL AND public_available_at = acceptance_at)),
    CHECK (CASE WHEN observation_kind = 'nport_holding'
                THEN accession_number IS NOT NULL AND holding_id IS NOT NULL
                     AND report_date IS NOT NULL
                     AND field_presence ?& ARRAY['nport_is_default', 'nport_arrears_or_deferral',
                                                 'nport_paid_in_kind']
                ELSE field_presence = '{}'::jsonb AND nport_is_default IS NULL
                     AND nport_arrears_or_deferral IS NULL AND nport_paid_in_kind IS NULL END),
    CHECK (CASE WHEN observation_kind = 'agency_action'
                THEN agency_name IS NOT NULL AND agency_subject_kind IS NOT NULL
                     AND agency_action_date IS NOT NULL
                ELSE num_nonnulls(agency_name, agency_subject_kind, agency_rating_type, agency_scale,
                                  agency_currency, agency_rating_symbol,
                                  agency_action_classification, agency_action_date,
                                  agency_file_creation_at) = 0 END),
    CHECK (observation_kind NOT LIKE '%\_passage'
           OR (document_sha256 IS NOT NULL AND document_location IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS bond_credit_observation_package_idx ON bond_credit_observation (package_id);
CREATE INDEX IF NOT EXISTS bond_credit_observation_accession_idx
    ON bond_credit_observation (accession_number) WHERE accession_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS bond_credit_observation_cusip_idx
    ON bond_credit_observation (cusip9, report_date) WHERE cusip9 IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Links (immutable revisions; quarantined links are never admitted)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_default_event_link (
    link_id uuid PRIMARY KEY,
    package_id uuid NOT NULL REFERENCES bond_default_source_package(package_id),
    observation_id uuid NOT NULL REFERENCES bond_credit_observation(observation_id),
    security_id uuid NOT NULL,
    cusip9 char(9) NOT NULL CHECK (bond_credit_cusip9_valid(cusip9)),
    obligor_id text NOT NULL CHECK (obligor_id ~ '\S'),
    affected_scope text NOT NULL CHECK (affected_scope IN (
        'exchange_new', 'exchange_old', 'issue', 'issuer_affected_obligation')),
    valid_from date NOT NULL,
    valid_to date,
    link_known_at timestamptz NOT NULL,
    identity_evidence_refs text[] NOT NULL CHECK (bond_credit_texts_canonical(identity_evidence_refs)),
    identity_evidence_digest text NOT NULL CHECK (identity_evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
    status text NOT NULL CHECK (status IN ('admitted', 'quarantined', 'rejected')),
    rationale text NOT NULL CHECK (rationale ~ '\S'),
    supersedes_link_id uuid REFERENCES bond_default_event_link(link_id),
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (valid_to IS NULL OR valid_to >= valid_from),
    CHECK (status <> 'admitted' OR cardinality(identity_evidence_refs) > 0),
    CHECK (supersedes_link_id IS DISTINCT FROM link_id)
);
CREATE INDEX IF NOT EXISTS bond_default_event_link_package_idx ON bond_default_event_link (package_id);
CREATE INDEX IF NOT EXISTS bond_default_event_link_observation_idx ON bond_default_event_link (observation_id);
CREATE INDEX IF NOT EXISTS bond_default_event_link_security_idx ON bond_default_event_link (security_id, cusip9);

-- ---------------------------------------------------------------------------
-- Adjudications (append-only; reviewer role limits admissible statuses)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_default_adjudication (
    adjudication_id uuid PRIMARY KEY,
    package_id uuid NOT NULL REFERENCES bond_default_source_package(package_id),
    subject_kind text NOT NULL CHECK (subject_kind IN (
        'candidate', 'corroboration', 'exchange_pairing', 'followup_continuity', 'issue_episode', 'issue_scope')),
    subject_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN (
        'accepted_event', 'accepted_evidence', 'accepted_state', 'candidate', 'disputed', 'nonqualifying',
        'retracted')),
    supersedes_adjudication_id uuid REFERENCES bond_default_adjudication(adjudication_id),
    policy_digest text NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    reviewer_id text NOT NULL CHECK (reviewer_id ~ '\S'),
    reviewer_role text NOT NULL CHECK (reviewer_role IN (
        'extraction_proposer', 'human_reviewer', 'policy_rule_engine')),
    adjudicated_at timestamptz NOT NULL,
    rationale text NOT NULL CHECK (rationale ~ '\S'),
    evidence_observation_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(evidence_observation_ids)),
    link_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(link_ids)),
    -- W0 amendment 1: persisted proposal edges and evidence-support validity window.
    proposal_evidence_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(proposal_evidence_ids)),
    support_valid_from date,
    support_valid_to date,
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (supersedes_adjudication_id IS DISTINCT FROM adjudication_id),
    -- Evidence-only acceptance: human reviewer, evidence subject, cited support (never admits an event).
    CHECK (status <> 'accepted_evidence'
           OR (subject_kind IN ('corroboration', 'exchange_pairing', 'followup_continuity', 'issue_scope')
               AND reviewer_role = 'human_reviewer' AND cardinality(evidence_observation_ids) > 0
               AND (subject_kind = 'followup_continuity' OR cardinality(link_ids) > 0)
               AND (subject_kind <> 'issue_scope' OR subject_id = ANY (link_ids)))),
    CHECK (cardinality(proposal_evidence_ids) = 0 OR subject_kind = 'issue_episode'),
    CHECK ((support_valid_from IS NOT NULL)
           = (subject_kind IN ('corroboration', 'exchange_pairing') AND status = 'accepted_evidence')),
    CHECK (support_valid_to IS NULL OR (support_valid_from IS NOT NULL AND support_valid_to > support_valid_from)),
    CHECK (CASE reviewer_role
                WHEN 'extraction_proposer' THEN status = 'candidate'
                WHEN 'policy_rule_engine' THEN status <> 'accepted_event'
                ELSE true END),
    CHECK (status NOT IN ('accepted_state', 'accepted_event')
           OR (subject_kind = 'issue_episode' AND cardinality(evidence_observation_ids) > 0
               AND cardinality(link_ids) > 0))
);
CREATE INDEX IF NOT EXISTS bond_default_adjudication_package_idx ON bond_default_adjudication (package_id);
CREATE INDEX IF NOT EXISTS bond_default_adjudication_subject_idx ON bond_default_adjudication (subject_id);
CREATE INDEX IF NOT EXISTS bond_default_adjudication_supersedes_idx
    ON bond_default_adjudication (supersedes_adjudication_id) WHERE supersedes_adjudication_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- N-CEN filing ledger (W0 amendment 1): one projection of one accession from one source
-- artifact. Fund-family provenance only, never credit evidence. Structured adviser and
-- underwriter records are bounded arrays of fixed-shape objects (raw values retained).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bond_credit_sha1(input bytea) RETURNS bytea
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
DECLARE
    message bytea := input || pg_catalog.decode('80', 'hex');
    padding integer;
    words bigint[] := pg_catalog.array_fill(0::bigint, ARRAY[80], ARRAY[0]);
    h0 bigint := 1732584193;
    h1 bigint := 4023233417;
    h2 bigint := 2562383102;
    h3 bigint := 271733878;
    h4 bigint := 3285377520;
    a bigint;
    b bigint;
    c bigint;
    d bigint;
    e bigint;
    f bigint;
    k bigint;
    temp bigint;
    block_index integer;
    i integer;
BEGIN
    padding := (56 - (pg_catalog.octet_length(message) % 64) + 64) % 64;
    message := message || pg_catalog.decode(pg_catalog.repeat('00', padding), 'hex')
               || pg_catalog.int8send(pg_catalog.octet_length(input)::bigint * 8);
    FOR block_index IN 0..(pg_catalog.octet_length(message) / 64 - 1) LOOP
        FOR i IN 0..15 LOOP
            words[i] := ((pg_catalog.get_byte(message, block_index * 64 + i * 4)::bigint << 24)
                      | (pg_catalog.get_byte(message, block_index * 64 + i * 4 + 1)::bigint << 16)
                      | (pg_catalog.get_byte(message, block_index * 64 + i * 4 + 2)::bigint << 8)
                      | pg_catalog.get_byte(message, block_index * 64 + i * 4 + 3)::bigint) & 4294967295;
        END LOOP;
        FOR i IN 16..79 LOOP
            temp := words[i - 3] # words[i - 8] # words[i - 14] # words[i - 16];
            words[i] := ((temp << 1) | (temp >> 31)) & 4294967295;
        END LOOP;
        a := h0;
        b := h1;
        c := h2;
        d := h3;
        e := h4;
        FOR i IN 0..79 LOOP
            IF i < 20 THEN
                f := (b & c) | ((~b) & d);
                k := 1518500249;
            ELSIF i < 40 THEN
                f := b # c # d;
                k := 1859775393;
            ELSIF i < 60 THEN
                f := (b & c) | (b & d) | (c & d);
                k := 2400959708;
            ELSE
                f := b # c # d;
                k := 3395469782;
            END IF;
            temp := (((a << 5) | (a >> 27)) + f + e + k + words[i]) & 4294967295;
            e := d;
            d := c;
            c := ((b << 30) | (b >> 2)) & 4294967295;
            b := a;
            a := temp;
        END LOOP;
        h0 := (h0 + a) & 4294967295;
        h1 := (h1 + b) & 4294967295;
        h2 := (h2 + c) & 4294967295;
        h3 := (h3 + d) & 4294967295;
        h4 := (h4 + e) & 4294967295;
    END LOOP;
    RETURN pg_catalog.substr(pg_catalog.int8send(h0), 5, 4)
        || pg_catalog.substr(pg_catalog.int8send(h1), 5, 4)
        || pg_catalog.substr(pg_catalog.int8send(h2), 5, 4)
        || pg_catalog.substr(pg_catalog.int8send(h3), 5, 4)
        || pg_catalog.substr(pg_catalog.int8send(h4), 5, 4);
END $$;

CREATE OR REPLACE FUNCTION bond_credit_uuid5(namespace_id uuid, name text) RETURNS uuid
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
DECLARE
    digest bytea := bond_credit_sha1(pg_catalog.uuid_send(namespace_id) || pg_catalog.convert_to(name, 'UTF8'));
    encoded text;
BEGIN
    digest := pg_catalog.set_byte(digest, 6, (pg_catalog.get_byte(digest, 6) & 15) | 80);
    digest := pg_catalog.set_byte(digest, 8, (pg_catalog.get_byte(digest, 8) & 63) | 128);
    encoded := pg_catalog.encode(pg_catalog.substr(digest, 1, 16), 'hex');
    RETURN (pg_catalog.substr(encoded, 1, 8) || '-' ||
            pg_catalog.substr(encoded, 9, 4) || '-' ||
            pg_catalog.substr(encoded, 13, 4) || '-' ||
            pg_catalog.substr(encoded, 17, 4) || '-' ||
            pg_catalog.substr(encoded, 21, 12))::uuid;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_identity_name(kind text, parts text[]) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT pg_catalog.to_json(pg_catalog.array_prepend(kind, parts))::text
$$;

CREATE OR REPLACE FUNCTION bond_credit_edgar_acceptance_to_utc(raw text) RETURNS timestamptz
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
DECLARE
    local_time timestamp;
    accepted timestamptz;
    hour_value integer;
    minute_value integer;
    second_value integer;
BEGIN
    IF raw !~ '^[0-9]{14}$' THEN
        RETURN NULL;
    END IF;
    hour_value := pg_catalog.substr(raw, 9, 2)::integer;
    minute_value := pg_catalog.substr(raw, 11, 2)::integer;
    second_value := pg_catalog.substr(raw, 13, 2)::integer;
    IF hour_value NOT BETWEEN 0 AND 23
       OR minute_value NOT BETWEEN 0 AND 59
       OR second_value NOT BETWEEN 0 AND 59 THEN
        RETURN NULL;
    END IF;
    BEGIN
        local_time := pg_catalog.make_timestamp(
            pg_catalog.substr(raw, 1, 4)::integer,
            pg_catalog.substr(raw, 5, 2)::integer,
            pg_catalog.substr(raw, 7, 2)::integer,
            hour_value,
            minute_value,
            second_value::double precision);
    EXCEPTION WHEN datetime_field_overflow THEN
        RETURN NULL;
    END;
    accepted := local_time AT TIME ZONE 'America/New_York';
    IF accepted AT TIME ZONE 'America/New_York' IS DISTINCT FROM local_time THEN
        RETURN NULL;
    END IF;
    RETURN accepted;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_ncen_date_boundary(day date) RETURNS timestamptz
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
DECLARE
    local_time timestamp := (day + 1)::timestamp;
    boundary timestamptz;
BEGIN
    boundary := local_time AT TIME ZONE 'America/New_York';
    IF boundary AT TIME ZONE 'America/New_York' IS DISTINCT FROM local_time THEN
        RETURN NULL;
    END IF;
    RETURN boundary;
END
$$;

CREATE OR REPLACE FUNCTION bond_credit_structured_canonical(value jsonb, fields text[], enum_field text,
                                                            enum_values text[], cap integer)
RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    -- Array of objects with exactly ``fields`` (text <= 1000 chars with a non-space, or null),
    -- sorted by the declared fields with null before text, unique, at most ``cap`` items.
    SELECT pg_catalog.jsonb_typeof(value) = 'array'
       AND pg_catalog.jsonb_array_length(value) <= cap
       AND NOT EXISTS (
           SELECT 1 FROM pg_catalog.jsonb_array_elements(value) e
           WHERE pg_catalog.jsonb_typeof(e) <> 'object'
              OR ARRAY(SELECT k FROM pg_catalog.jsonb_object_keys(e) k ORDER BY k COLLATE "C")
                 <> ARRAY(SELECT f FROM pg_catalog.unnest(fields) f ORDER BY f COLLATE "C")
              OR EXISTS (SELECT 1 FROM pg_catalog.unnest(fields) f
                         WHERE pg_catalog.jsonb_typeof(e -> f) NOT IN ('null', 'string')
                            OR (pg_catalog.jsonb_typeof(e -> f) = 'string'
                                AND ((e ->> f) !~ '\S' OR pg_catalog.length(e ->> f) > 1000)))
              OR (enum_field IS NOT NULL AND NOT ((e ->> enum_field) = ANY (enum_values))))
       AND value = COALESCE((SELECT pg_catalog.jsonb_agg(d.e ORDER BY d.k) FROM (
               SELECT DISTINCT e, ARRAY(SELECT CASE WHEN e ->> f IS NULL THEN '0' ELSE '1' || (e ->> f) END
                                        FROM pg_catalog.unnest(fields) WITH ORDINALITY AS u(f, n) ORDER BY n)
                                  COLLATE "C" AS k
               FROM pg_catalog.jsonb_array_elements(value) e) d), '[]'::jsonb)
$$;

CREATE TABLE IF NOT EXISTS bond_default_ncen_filing (
    filing_evidence_id uuid PRIMARY KEY,
    package_id uuid NOT NULL REFERENCES bond_default_source_package(package_id),
    accession_number text NOT NULL CHECK (accession_number ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    row_locator text NOT NULL CHECK (row_locator ~ '\S'),
    registrant_cik text CHECK (registrant_cik ~ '^[0-9]{10}$'),
    form_type text CHECK (form_type IN ('N-CEN', 'N-CEN/A')),
    report_period_end date,
    filing_date date,
    -- Validated against the publication inventory by bond_credit_validate (no FK: an omitted
    -- header package is a validation refusal, not an insert error).
    header_package_id uuid,
    acceptance_raw text CHECK (acceptance_raw ~ '\S'),
    acceptance_at timestamptz,
    public_available_at timestamptz NOT NULL,
    first_seen_at timestamptz NOT NULL,
    public_time_basis text NOT NULL CHECK (public_time_basis IN (
        'archived_release_metadata', 'date_only_next_day_boundary', 'edgar_acceptance_datetime',
        'first_verified_retrieval')),
    version_evidence_filing_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(version_evidence_filing_ids)),
    parse_status text NOT NULL CHECK (parse_status IN ('index_only', 'parsed', 'quarantined', 'retracted')),
    reasons text[] NOT NULL CHECK (bond_credit_texts_canonical(reasons)),
    family_answer text CHECK (family_answer IN ('N', 'Y')),
    family_name_raw text CHECK (family_name_raw ~ '\S' AND pg_catalog.length(family_name_raw) <= 1000),
    reported_series_ids text[] NOT NULL CHECK (bond_credit_texts_canonical(reported_series_ids)
                                               AND cardinality(reported_series_ids) <= 5000),
    adviser_records jsonb NOT NULL CHECK (bond_credit_structured_canonical(
        adviser_records, ARRAY['series_id', 'role', 'file_number_raw', 'crd_raw', 'lei_raw'], 'role',
        ARRAY['adviser', 'sub_adviser', 'terminated_adviser', 'terminated_sub_adviser'], 20000)),
    underwriter_records jsonb NOT NULL CHECK (bond_credit_structured_canonical(
        underwriter_records, ARRAY['file_number_raw', 'crd_raw', 'lei_raw'], NULL, NULL, 1000)),
    projection_digest text NOT NULL CHECK (projection_digest ~ '^sha256:[0-9a-f]{64}$'),
    supersedes_filing_evidence_id uuid REFERENCES bond_default_ncen_filing(filing_evidence_id),
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (package_id, accession_number, row_locator),
    CHECK (filing_evidence_id = bond_credit_uuid5(
        'bb022049-6738-5033-b532-d19b7c29c541'::uuid,
        bond_credit_identity_name('ncen_filing', ARRAY[package_id::text, accession_number, row_locator]))),
    CHECK (supersedes_filing_evidence_id IS DISTINCT FROM filing_evidence_id),
    CHECK (NOT (filing_evidence_id = ANY (version_evidence_filing_ids))),
    CHECK (acceptance_at IS NULL OR acceptance_raw IS NOT NULL),
    CHECK (acceptance_raw IS NULL OR header_package_id IS NOT NULL),
    CHECK (acceptance_raw IS NULL OR (
        bond_credit_edgar_acceptance_to_utc(acceptance_raw) IS NOT NULL
        AND acceptance_at IS NOT DISTINCT FROM bond_credit_edgar_acceptance_to_utc(acceptance_raw))),
    CHECK (public_time_basis <> 'edgar_acceptance_datetime'
           OR (acceptance_at IS NOT NULL AND public_available_at = acceptance_at)),
    CHECK (public_time_basis <> 'date_only_next_day_boundary'
           OR (filing_date IS NOT NULL AND bond_credit_ncen_date_boundary(filing_date) IS NOT NULL
               AND public_available_at = bond_credit_ncen_date_boundary(filing_date))),
    CHECK (public_time_basis <> 'first_verified_retrieval' OR public_available_at <= first_seen_at),
    CHECK (parse_status <> 'parsed' OR (registrant_cik IS NOT NULL AND form_type IS NOT NULL
                                        AND report_period_end IS NOT NULL AND filing_date IS NOT NULL
                                        AND acceptance_at IS NOT NULL AND cardinality(reasons) = 0)),
    CHECK (parse_status <> 'index_only' OR (registrant_cik IS NOT NULL AND family_answer IS NULL
                                            AND family_name_raw IS NULL AND cardinality(reported_series_ids) = 0
                                            AND adviser_records = '[]'::jsonb AND underwriter_records = '[]'::jsonb
                                            AND cardinality(version_evidence_filing_ids) = 0)),
    CHECK (parse_status IN ('parsed', 'index_only') OR cardinality(reasons) > 0),
    CHECK (parse_status <> 'retracted' OR supersedes_filing_evidence_id IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS bond_default_ncen_filing_package_idx ON bond_default_ncen_filing (package_id);
CREATE INDEX IF NOT EXISTS bond_default_ncen_filing_registrant_idx
    ON bond_default_ncen_filing (registrant_cik, report_period_end);

-- Cross-table insert rule: each row kind comes from the source family that may carry it.
CREATE OR REPLACE FUNCTION bond_credit_ledger_family_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    family text;
    allowed text[];
BEGIN
    SELECT p.source_family INTO family FROM bond_default_source_package p WHERE p.package_id = NEW.package_id;
    IF family IS NULL THEN
        RAISE EXCEPTION '%: package % does not exist', TG_TABLE_NAME, NEW.package_id;
    END IF;
    IF TG_TABLE_NAME = 'bond_credit_observation' THEN
        allowed := CASE NEW.observation_kind
            WHEN 'nport_holding' THEN ARRAY['sec_nport_dera', 'sec_nport_public_xml']
            WHEN 'edgar_passage' THEN ARRAY['sec_edgar_document', 'sec_edgar_index']
            WHEN 'issuer_document_passage' THEN ARRAY['issuer_public_document']
            WHEN 'court_document_passage' THEN ARRAY['court_public_document']
            WHEN 'agency_action' THEN ARRAY['agency_rocr_xbrl']
        END;
    ELSIF TG_TABLE_NAME = 'bond_default_event_link' THEN
        allowed := ARRAY['link_batch'];
    ELSIF TG_TABLE_NAME = 'bond_default_ncen_filing' THEN
        -- index_only rows come only from the EDGAR index; projections only from N-CEN artifacts.
        allowed := CASE WHEN NEW.parse_status = 'index_only' THEN ARRAY['sec_edgar_index']
                        ELSE ARRAY['sec_ncen_dera', 'sec_ncen_public_xml'] END;
    ELSE
        allowed := ARRAY['adjudication_batch'];
    END IF;
    IF NOT family = ANY (allowed) THEN
        RAISE EXCEPTION '%: source family % cannot carry this row', TG_TABLE_NAME, family;
    END IF;
    -- A package is sealed once any publication consumed it: its row set is then frozen.
    -- The package-scoped lock serializes this check with publication-source inserts.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('bond_credit_package|' || NEW.package_id::text, 0));
    IF pg_catalog.to_regclass('bond_credit_publication_sources') IS NOT NULL
       AND EXISTS (SELECT 1 FROM bond_credit_publication_sources s WHERE s.package_id = NEW.package_id) THEN
        RAISE EXCEPTION '%: package % is sealed by a publication', TG_TABLE_NAME, NEW.package_id;
    END IF;
    RETURN NEW;
END $$;

DO $$
DECLARE
    rel text;
BEGIN
    FOREACH rel IN ARRAY ARRAY['bond_default_source_package', 'bond_credit_observation',
                               'bond_default_event_link', 'bond_default_adjudication',
                               'bond_default_ncen_filing'] LOOP
        EXECUTE pg_catalog.format('DROP TRIGGER IF EXISTS %I ON %I', rel || '_append_only', rel);
        EXECUTE pg_catalog.format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION bond_credit_append_only()',
            rel || '_append_only', rel);
        EXECUTE pg_catalog.format('DROP TRIGGER IF EXISTS %I ON %I', rel || '_no_truncate', rel);
        EXECUTE pg_catalog.format(
            'CREATE TRIGGER %I BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION bond_credit_append_only()',
            rel || '_no_truncate', rel);
    END LOOP;
    FOREACH rel IN ARRAY ARRAY['bond_credit_observation', 'bond_default_event_link',
                               'bond_default_adjudication', 'bond_default_ncen_filing'] LOOP
        EXECUTE pg_catalog.format('DROP TRIGGER IF EXISTS %I ON %I', rel || '_family_guard', rel);
        EXECUTE pg_catalog.format(
            'CREATE TRIGGER %I BEFORE INSERT ON %I FOR EACH ROW EXECUTE FUNCTION bond_credit_ledger_family_guard()',
            rel || '_family_guard', rel);
    END LOOP;
END $$;

-- Least privilege: the writer appends, the auditor reads raw rows; nobody updates/deletes.
-- The serving reader has no table privilege (revoked explicitly for re-installs).
REVOKE ALL ON bond_default_source_package, bond_credit_observation, bond_default_event_link,
    bond_default_adjudication, bond_default_ncen_filing FROM PUBLIC, bond_credit_reader;
GRANT SELECT ON bond_default_source_package, bond_credit_observation, bond_default_event_link,
    bond_default_adjudication, bond_default_ncen_filing TO bond_credit_auditor;
GRANT SELECT, INSERT ON bond_default_source_package, bond_credit_observation,
    bond_default_event_link, bond_default_adjudication, bond_default_ncen_filing TO bond_credit_writer;
REVOKE ALL ON FUNCTION bond_credit_cusip9_valid(text), bond_credit_uuids_canonical(uuid[]),
    bond_credit_texts_canonical(text[]), bond_credit_family_role(text),
    bond_credit_sha1(bytea), bond_credit_uuid5(uuid, text), bond_credit_identity_name(text, text[]),
    bond_credit_edgar_acceptance_to_utc(text), bond_credit_ncen_date_boundary(date),
    bond_credit_append_only(), bond_credit_ledger_family_guard(),
    bond_credit_structured_canonical(jsonb, text[], text, text[], integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bond_credit_cusip9_valid(text), bond_credit_uuids_canonical(uuid[]),
    bond_credit_texts_canonical(text[]), bond_credit_family_role(text),
    bond_credit_sha1(bytea), bond_credit_uuid5(uuid, text), bond_credit_identity_name(text, text[]),
    bond_credit_edgar_acceptance_to_utc(text), bond_credit_ncen_date_boundary(date),
    bond_credit_structured_canonical(jsonb, text[], text, text[], integer)
    TO bond_credit_reader, bond_credit_writer;

COMMIT;
