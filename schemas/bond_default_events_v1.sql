-- bond_default_events_v1: publication-scoped default episodes, follow-up segments,
-- exit evidence and coverage cells (bundle frames events, followups,
-- exit_evidence, coverage). Rows are inserted only while the owning publication is
-- `prepared` and are never updated or deleted.
--
-- Apply after bond_credit_publications_v1 with `SET search_path TO <schema>, pg_temp`.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

DO $$
BEGIN
    IF pg_catalog.to_regclass('bond_credit_publications') IS NULL THEN
        RAISE EXCEPTION 'bond_default_events_v1: apply bond_credit_publications_v1 first (same schema)';
    END IF;
END $$;

-- Mirrors contracts.DefaultEpisode.derive_input_digest (v2; canonical compact JSON, sorted keys):
-- the three direct arrays, the proposal/relation edges and the typed dependency digest.
CREATE OR REPLACE FUNCTION bond_credit_event_input_digest(evidence_ids uuid[], links uuid[], adjudications uuid[],
                                                          proposals uuid[], relations uuid[], dependency text)
RETURNS text LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
    SELECT 'sha256:' || pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
        '{"adjudication_ids":' || pg_catalog.array_to_json(adjudications::text[])::text
        || ',"dependency_digest":' || pg_catalog.to_json(dependency)::text
        || ',"evidence_observation_ids":' || pg_catalog.array_to_json(evidence_ids::text[])::text
        || ',"exchange_relation_ids":' || pg_catalog.array_to_json(relations::text[])::text
        || ',"link_ids":' || pg_catalog.array_to_json(links::text[])::text
        || ',"proposal_evidence_ids":' || pg_catalog.array_to_json(proposals::text[])::text || '}', 'UTF8')), 'hex')
$$;

CREATE OR REPLACE FUNCTION bond_credit_timing_class(lower_exclusive date, upper_inclusive date)
RETURNS text LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
    SELECT CASE
        WHEN lower_exclusive IS NULL THEN 'prevalent'
        WHEN pg_catalog.date_trunc('month', lower_exclusive + 1) = pg_catalog.date_trunc('month', upper_inclusive)
            THEN 'incident'
        ELSE 'interval_uncertain' END
$$;

CREATE TABLE IF NOT EXISTS bond_default_event_v1 (
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    security_id uuid NOT NULL,
    episode_id uuid NOT NULL,
    cusip9 char(9) NOT NULL CHECK (bond_credit_cusip9_valid(cusip9)),
    obligor_id text NOT NULL CHECK (obligor_id ~ '\S'),
    issuer_episode_id uuid NOT NULL,
    primary_type text NOT NULL CHECK (primary_type IN (
        'agency_issue_default', 'bankruptcy', 'default_state', 'distressed_exchange', 'payment_default')),
    corroboration_flags text[] NOT NULL CHECK (
        bond_credit_texts_canonical(corroboration_flags)
        AND corroboration_flags <@ ARRAY['agency_issue_default', 'agency_rac_wd', 'bankruptcy',
            'court_document', 'distressed_exchange', 'edgar_document', 'issuer_agency_default_linked',
            'issuer_document', 'nport_consensus_state', 'payment_default']),
    admission_status text NOT NULL CHECK (admission_status IN ('accepted_event', 'accepted_state')),
    timing_class text NOT NULL CHECK (timing_class IN ('incident', 'interval_uncertain', 'prevalent')),
    onset_date date,
    onset_lower_exclusive date,
    onset_upper_inclusive date NOT NULL,
    -- Explicit provenance of a non-null lower bound (never invented; checked by bond_credit_validate).
    onset_lower_evidence_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(onset_lower_evidence_ids)),
    recognition_date date,
    evidence_known_at timestamptz NOT NULL,
    link_known_at timestamptz NOT NULL,
    evidence_observation_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(evidence_observation_ids)
                                                    AND cardinality(evidence_observation_ids) > 0),
    link_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(link_ids) AND cardinality(link_ids) > 0),
    adjudication_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(adjudication_ids)
                                            AND cardinality(adjudication_ids) > 0),
    resolution_date date,
    resolution_refs uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(resolution_refs)),
    resolution_known_at timestamptz,
    alias_spell_id uuid,
    proposal_evidence_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(proposal_evidence_ids)),
    exchange_relation_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(exchange_relation_ids)),
    dependency_digest text NOT NULL CHECK (dependency_digest ~ '^sha256:[0-9a-f]{64}$'),
    event_input_digest text NOT NULL CHECK (event_input_digest ~ '^sha256:[0-9a-f]{64}$'),
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, security_id, episode_id),
    CHECK (onset_lower_exclusive IS NULL OR onset_lower_exclusive < onset_upper_inclusive),
    CHECK (timing_class = bond_credit_timing_class(onset_lower_exclusive, onset_upper_inclusive)),
    CHECK (onset_date IS NULL OR (onset_lower_exclusive IS NOT NULL AND onset_upper_inclusive = onset_date
                                  AND onset_lower_exclusive = onset_date - 1)),
    CHECK ((onset_lower_exclusive IS NULL) = (cardinality(onset_lower_evidence_ids) = 0)),
    CHECK (onset_lower_evidence_ids <@ evidence_observation_ids),
    CHECK ((admission_status = 'accepted_state') = (primary_type = 'default_state')),
    CHECK (link_known_at <= evidence_known_at),
    CHECK ((resolution_date IS NULL) = (cardinality(resolution_refs) = 0)),
    CHECK ((resolution_date IS NULL) = (resolution_known_at IS NULL)),
    CHECK (resolution_date IS NULL OR resolution_date >= onset_upper_inclusive),
    -- A completed exchange (primary or flag) needs its persisted relation(s); spell iff relations.
    CHECK ((primary_type = 'distressed_exchange' OR 'distressed_exchange' = ANY (corroboration_flags))
           = (cardinality(exchange_relation_ids) > 0)),
    CHECK ((alias_spell_id IS NULL) = (cardinality(exchange_relation_ids) = 0)),
    CHECK (('nport_consensus_state' = ANY (corroboration_flags)) = (cardinality(proposal_evidence_ids) > 0)),
    CHECK (event_input_digest = bond_credit_event_input_digest(evidence_observation_ids, link_ids, adjudication_ids,
                                                               proposal_evidence_ids, exchange_relation_ids,
                                                               dependency_digest))
);
CREATE INDEX IF NOT EXISTS bond_default_event_v1_cusip_onset_idx
    ON bond_default_event_v1 (publication_id, cusip9, onset_upper_inclusive);
CREATE INDEX IF NOT EXISTS bond_default_event_v1_episode_idx
    ON bond_default_event_v1 (publication_id, episode_id);

CREATE TABLE IF NOT EXISTS bond_default_followup_v1 (
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    security_id uuid NOT NULL,
    spell_id uuid NOT NULL,
    segment_id uuid NOT NULL,
    cusip9 char(9) NOT NULL CHECK (bond_credit_cusip9_valid(cusip9)),
    interval_start_exclusive date NOT NULL,
    interval_end_inclusive date NOT NULL,
    status text NOT NULL CHECK (status IN ('nondefault_continuous', 'repaid', 'resolved', 'unknown')),
    completeness_basis text NOT NULL CHECK (completeness_basis IN (
        'continuous_document', 'issuer_trustee_confirmation', 'none', 'surveillance_receipt')),
    evidence_observation_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(evidence_observation_ids)),
    adjudication_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(adjudication_ids)),
    known_at timestamptz NOT NULL,
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, security_id, spell_id, segment_id),
    CHECK (interval_start_exclusive < interval_end_inclusive),
    CHECK ((status = 'unknown') = (completeness_basis = 'none')),
    CHECK (status = 'unknown' OR cardinality(evidence_observation_ids) > 0)
);
CREATE INDEX IF NOT EXISTS bond_default_followup_v1_cusip_idx
    ON bond_default_followup_v1 (publication_id, cusip9, interval_end_inclusive);

CREATE TABLE IF NOT EXISTS bond_default_exit_evidence_v1 (
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    security_id uuid NOT NULL,
    last_panel_month date NOT NULL CHECK (EXTRACT(day FROM last_panel_month) = 1),
    cusip9 char(9) NOT NULL CHECK (bond_credit_cusip9_valid(cusip9)),
    primary_reason text NOT NULL CHECK (primary_reason IN (
        'matured', 'maturity_floor', 'scope_change', 'source_handoff', 'unknown')),
    flags text[] NOT NULL CHECK (
        bond_credit_texts_canonical(flags)
        AND flags <@ ARRAY['distressed_candidate', 'matured', 'maturity_floor', 'observed_gap_reentry',
                           'scope_change', 'source_handoff', 'target_end_censored', 'unknown']),
    next_observed_month date CHECK (EXTRACT(day FROM next_observed_month) = 1),
    gap_months integer CHECK (gap_months >= 0),
    scheduled_maturity date,
    proven_repayment_date date,
    evidence_observation_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(evidence_observation_ids)),
    known_at timestamptz NOT NULL,
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, security_id, last_panel_month),
    CHECK (('observed_gap_reentry' = ANY (flags)) = (next_observed_month IS NOT NULL)),
    CHECK (next_observed_month IS NULL OR next_observed_month > last_panel_month),
    CHECK ((next_observed_month IS NULL AND gap_months IS NULL)
           OR (next_observed_month IS NOT NULL AND gap_months IS NOT NULL AND gap_months =
               ((EXTRACT(year FROM next_observed_month) - EXTRACT(year FROM last_panel_month)) * 12
                + EXTRACT(month FROM next_observed_month) - EXTRACT(month FROM last_panel_month) - 1)::integer)),
    CHECK (primary_reason <> 'matured' OR scheduled_maturity IS NOT NULL),
    CHECK (proven_repayment_date IS NULL OR cardinality(evidence_observation_ids) > 0)
);
CREATE INDEX IF NOT EXISTS bond_default_exit_evidence_v1_cusip_idx
    ON bond_default_exit_evidence_v1 (publication_id, cusip9, last_panel_month);

CREATE TABLE IF NOT EXISTS bond_default_coverage_v1 (
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    period_label text NOT NULL CHECK (period_label ~ '^(all|[0-9]{4}|[0-9]{4}-(0[1-9]|1[0-2]))$'),
    source text NOT NULL CHECK (source IN (
        'agency_rocr', 'all', 'independent_reference', 'sec_edgar', 'sec_nport', 'unknown')),
    event_type text NOT NULL CHECK (event_type IN (
        'agency_issue_default', 'all', 'bankruptcy', 'default_state', 'distressed_exchange',
        'payment_default', 'unknown')),
    rating_stratum text NOT NULL CHECK (rating_stratum IN ('HY', 'IG', 'all', 'unknown')),
    exposure_cohort text NOT NULL CHECK (exposure_cohort IN ('all', 'exited', 'gap', 'retained', 'unknown')),
    state text NOT NULL CHECK (state IN ('not_applicable', 'partial', 'qualified', 'unavailable')),
    denominator_basis text NOT NULL CHECK (denominator_basis IN (
        'independent_reference_enumeration', 'none', 'panel_exposure')),
    denominator_count integer CHECK (denominator_count >= 0),
    exposed_issue_months integer NOT NULL CHECK (exposed_issue_months >= 0),
    event_count integer NOT NULL CHECK (event_count >= 0),
    unlinked_count integer NOT NULL CHECK (unlinked_count >= 0),
    date_uncertain_count integer NOT NULL CHECK (date_uncertain_count >= 0),
    unknown_outcome_issue_months integer NOT NULL CHECK (unknown_outcome_issue_months >= 0),
    source_frontier date,
    lag_p50_days integer CHECK (lag_p50_days >= 0),
    lag_p90_days integer CHECK (lag_p90_days >= 0),
    lag_max_days integer CHECK (lag_max_days >= 0),
    rationale text NOT NULL CHECK (rationale ~ '\S'),
    validation_receipt_digest text CHECK (validation_receipt_digest ~ '^sha256:[0-9a-f]{64}$'),
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, period_label, source, event_type, rating_stratum, exposure_cohort),
    CHECK ((denominator_basis = 'none') = (denominator_count IS NULL)),
    CHECK (state <> 'qualified' OR (validation_receipt_digest IS NOT NULL AND denominator_basis <> 'none')),
    CHECK (num_nonnulls(lag_p50_days, lag_p90_days, lag_max_days) IN (0, 3)),
    CHECK (lag_p50_days IS NULL OR (lag_p50_days <= lag_p90_days AND lag_p90_days <= lag_max_days))
);

-- ---------------------------------------------------------------------------
-- W0 amendment 1 (bundle v2): persisted dependency closure, publication-scoped.
-- Identities (uuid5) are derived by the row contract; every relational rule, time and digest
-- is recomputed by bond_credit_validate from the persisted inventory.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bond_default_family_context_v2 (
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    context_id uuid NOT NULL,
    report_date date NOT NULL,
    knowledge_cutoff timestamptz NOT NULL,
    rule_version text NOT NULL CHECK (rule_version = 'bond_default_ncen_family_fe1ab_v3'),
    vote_observation_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(vote_observation_ids)),
    selection_filing_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(selection_filing_ids)),
    index_package_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(index_package_ids)),
    universe_digest text NOT NULL CHECK (universe_digest ~ '^sha256:[0-9a-f]{64}$'),
    membership_digest text NOT NULL CHECK (membership_digest ~ '^sha256:[0-9a-f]{64}$'),
    evidence_known_at timestamptz NOT NULL,
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, context_id)
);

CREATE TABLE IF NOT EXISTS bond_default_family_evidence_v2 (
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    family_evidence_id uuid NOT NULL,
    context_id uuid NOT NULL,
    registrant_cik text NOT NULL CHECK (registrant_cik ~ '^[0-9]{10}$'),
    voting_series_ids text[] NOT NULL CHECK (bond_credit_texts_canonical(voting_series_ids)),
    vote_observation_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(vote_observation_ids)
                                                AND cardinality(vote_observation_ids) > 0),
    selected_filing_id uuid,
    blocking_filing_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(blocking_filing_ids)),
    state text NOT NULL CHECK (state IN ('complete', 'incomplete')),
    reasons text[] NOT NULL CHECK (bond_credit_texts_canonical(reasons)),
    component_id text CHECK (component_id ~ '^ncenfam:[0-9a-f]{32}$'),
    valid_from date NOT NULL,
    valid_to date NOT NULL,
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, context_id, registrant_cik),
    UNIQUE (publication_id, family_evidence_id),
    CHECK (valid_to = valid_from + 1),
    CHECK ((state = 'complete') = (cardinality(reasons) = 0)),
    CHECK (state <> 'complete' OR (selected_filing_id IS NOT NULL AND component_id IS NOT NULL)),
    CHECK (state <> 'incomplete' OR component_id IS NULL)
);

CREATE TABLE IF NOT EXISTS bond_default_proposal_evidence_v2 (
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    proposal_evidence_id uuid NOT NULL,
    cusip9 char(9) NOT NULL CHECK (bond_credit_cusip9_valid(cusip9)),
    proposed_status text NOT NULL CHECK (proposed_status IN ('accepted_state', 'candidate')),
    basis text NOT NULL CHECK (basis ~ '\S'),
    onset_lower_exclusive date,
    onset_upper_inclusive date,
    onset_lower_evidence_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(onset_lower_evidence_ids)),
    onset_upper_evidence_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(onset_upper_evidence_ids)),
    evidence_observation_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(evidence_observation_ids)
                                                    AND cardinality(evidence_observation_ids) > 0),
    family_evidence_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(family_evidence_ids)),
    corroboration_adjudication_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(corroboration_adjudication_ids)),
    evidence_known_at timestamptz NOT NULL,
    policy_digest text NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, proposal_evidence_id),
    CHECK (proposed_status <> 'accepted_state'
           OR (onset_upper_inclusive IS NOT NULL AND cardinality(onset_upper_evidence_ids) > 0
               AND (onset_lower_exclusive IS NULL OR onset_lower_exclusive < onset_upper_inclusive))),
    CHECK (proposed_status = 'accepted_state'
           OR (onset_lower_exclusive IS NULL AND onset_upper_inclusive IS NULL
               AND cardinality(onset_upper_evidence_ids) = 0)),
    CHECK ((onset_lower_exclusive IS NULL) = (cardinality(onset_lower_evidence_ids) = 0)),
    CHECK ((onset_lower_evidence_ids || onset_upper_evidence_ids) <@ evidence_observation_ids)
);

-- Directional old -> new continuity; valid_to is EXCLUSIVE (EventLink.valid_to is inclusive).
CREATE TABLE IF NOT EXISTS bond_default_exchange_relation_v2 (
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    relation_id uuid NOT NULL,
    old_security_id uuid NOT NULL,
    old_cusip9 char(9) NOT NULL CHECK (bond_credit_cusip9_valid(old_cusip9)),
    new_security_id uuid NOT NULL,
    new_cusip9 char(9) NOT NULL CHECK (bond_credit_cusip9_valid(new_cusip9)),
    episode_id uuid NOT NULL,
    alias_spell_id uuid NOT NULL,
    old_link_id uuid NOT NULL,
    new_link_id uuid NOT NULL,
    exchange_document_observation_ids uuid[] NOT NULL CHECK (
        bond_credit_uuids_canonical(exchange_document_observation_ids)
        AND cardinality(exchange_document_observation_ids) > 0),
    pairing_adjudication_id uuid NOT NULL,
    exchange_effective_date date NOT NULL,
    valid_from date NOT NULL,
    valid_to date,
    evidence_known_at timestamptz NOT NULL,
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, old_security_id, new_security_id, episode_id),
    UNIQUE (publication_id, relation_id),
    CHECK (old_security_id <> new_security_id AND old_cusip9 <> new_cusip9),
    CHECK (valid_from = exchange_effective_date),
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

DO $$
DECLARE
    rel text;
BEGIN
    FOREACH rel IN ARRAY ARRAY['bond_default_event_v1', 'bond_default_followup_v1',
                               'bond_default_exit_evidence_v1', 'bond_default_coverage_v1',
                               'bond_default_family_context_v2', 'bond_default_family_evidence_v2',
                               'bond_default_proposal_evidence_v2', 'bond_default_exchange_relation_v2'] LOOP
        EXECUTE pg_catalog.format('DROP TRIGGER IF EXISTS %I ON %I', rel || '_insert_guard', rel);
        EXECUTE pg_catalog.format(
            'CREATE TRIGGER %I BEFORE INSERT ON %I FOR EACH ROW EXECUTE FUNCTION bond_credit_child_insert_guard()',
            rel || '_insert_guard', rel);
        EXECUTE pg_catalog.format('DROP TRIGGER IF EXISTS %I ON %I', rel || '_append_only', rel);
        EXECUTE pg_catalog.format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION bond_credit_append_only()',
            rel || '_append_only', rel);
        EXECUTE pg_catalog.format('DROP TRIGGER IF EXISTS %I ON %I', rel || '_no_truncate', rel);
        EXECUTE pg_catalog.format(
            'CREATE TRIGGER %I BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION bond_credit_append_only()',
            rel || '_no_truncate', rel);
    END LOOP;
END $$;

-- One publication-local transitive closure. UUIDs are typed by frame; ownership identifiers
-- (subject/context/episode) are not reverse edges. Revision heads are checked by validate first.
CREATE OR REPLACE FUNCTION bond_credit_dependency_closure(
    pub_id uuid, pkgs uuid[], stale_obs uuid[], stale_links uuid[], eff_ids uuid[], policy text,
    root_obs uuid[] DEFAULT '{}', root_links uuid[] DEFAULT '{}', root_adjs uuid[] DEFAULT '{}',
    root_props uuid[] DEFAULT '{}', root_relations uuid[] DEFAULT '{}')
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    k timestamptz;
    obs_ids uuid[] := COALESCE(root_obs, '{}');
    reached_link_ids uuid[] := COALESCE(root_links, '{}');
    adj_ids uuid[] := COALESCE(root_adjs, '{}');
    prop_ids uuid[] := COALESCE(root_props, '{}');
    relation_ids uuid[] := COALESCE(root_relations, '{}');
    member_ids uuid[] := '{}';
    context_ids uuid[] := '{}';
    filing_ids uuid[] := '{}';
    package_ids uuid[] := '{}';
    before_counts text;
    after_counts text;
    missing boolean := false;
    unresolved boolean := false;
    cycle_node text;
    known_at timestamptz;
    link_known_at timestamptz;
BEGIN
    SELECT knowledge_cutoff INTO k FROM bond_credit_publications WHERE publication_id = pub_id;
    LOOP
        before_counts := pg_catalog.concat_ws('|', cardinality(obs_ids), cardinality(reached_link_ids), cardinality(adj_ids),
            cardinality(prop_ids), cardinality(relation_ids), cardinality(member_ids), cardinality(context_ids),
            cardinality(filing_ids), cardinality(package_ids));

        missing := missing OR EXISTS (SELECT 1 FROM pg_catalog.unnest(relation_ids) x
            WHERE NOT EXISTS (SELECT 1 FROM bond_default_exchange_relation_v2 r
                              WHERE r.publication_id = pub_id AND r.relation_id = x));
        relation_ids := ARRAY(SELECT DISTINCT r.relation_id FROM bond_default_exchange_relation_v2 r
                              WHERE r.publication_id = pub_id AND r.relation_id = ANY (relation_ids) ORDER BY 1);
        obs_ids := obs_ids || ARRAY(SELECT x FROM bond_default_exchange_relation_v2 r
            CROSS JOIN LATERAL pg_catalog.unnest(r.exchange_document_observation_ids) x
            WHERE r.publication_id = pub_id AND r.relation_id = ANY (relation_ids));
        reached_link_ids := reached_link_ids || ARRAY(SELECT x FROM bond_default_exchange_relation_v2 r
            CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[r.old_link_id, r.new_link_id]) x
            WHERE r.publication_id = pub_id AND r.relation_id = ANY (relation_ids));
        adj_ids := adj_ids || ARRAY(SELECT r.pairing_adjudication_id FROM bond_default_exchange_relation_v2 r
                                   WHERE r.publication_id = pub_id AND r.relation_id = ANY (relation_ids));

        missing := missing OR EXISTS (SELECT 1 FROM pg_catalog.unnest(adj_ids) x
            WHERE NOT EXISTS (SELECT 1 FROM bond_default_adjudication a
                              WHERE a.package_id = ANY (pkgs) AND a.adjudication_id = x));
        adj_ids := ARRAY(SELECT DISTINCT a.adjudication_id FROM bond_default_adjudication a
                         WHERE a.package_id = ANY (pkgs) AND a.adjudication_id = ANY (adj_ids) ORDER BY 1);
        obs_ids := obs_ids || ARRAY(SELECT x FROM bond_default_adjudication a
            CROSS JOIN LATERAL pg_catalog.unnest(a.evidence_observation_ids) x WHERE a.adjudication_id = ANY (adj_ids));
        reached_link_ids := reached_link_ids || ARRAY(SELECT x FROM bond_default_adjudication a
            CROSS JOIN LATERAL pg_catalog.unnest(a.link_ids) x WHERE a.adjudication_id = ANY (adj_ids));
        prop_ids := prop_ids || ARRAY(SELECT x FROM bond_default_adjudication a
            CROSS JOIN LATERAL pg_catalog.unnest(a.proposal_evidence_ids) x WHERE a.adjudication_id = ANY (adj_ids));

        missing := missing OR EXISTS (SELECT 1 FROM pg_catalog.unnest(prop_ids) x
            WHERE NOT EXISTS (SELECT 1 FROM bond_default_proposal_evidence_v2 p
                              WHERE p.publication_id = pub_id AND p.proposal_evidence_id = x));
        prop_ids := ARRAY(SELECT DISTINCT p.proposal_evidence_id FROM bond_default_proposal_evidence_v2 p
                          WHERE p.publication_id = pub_id AND p.proposal_evidence_id = ANY (prop_ids) ORDER BY 1);
        obs_ids := obs_ids || ARRAY(SELECT x FROM bond_default_proposal_evidence_v2 p
            CROSS JOIN LATERAL pg_catalog.unnest(p.evidence_observation_ids) x
            WHERE p.publication_id = pub_id AND p.proposal_evidence_id = ANY (prop_ids));
        adj_ids := adj_ids || ARRAY(SELECT x FROM bond_default_proposal_evidence_v2 p
            CROSS JOIN LATERAL pg_catalog.unnest(p.corroboration_adjudication_ids) x
            WHERE p.publication_id = pub_id AND p.proposal_evidence_id = ANY (prop_ids));
        member_ids := member_ids || ARRAY(SELECT x FROM bond_default_proposal_evidence_v2 p
            CROSS JOIN LATERAL pg_catalog.unnest(p.family_evidence_ids) x
            WHERE p.publication_id = pub_id AND p.proposal_evidence_id = ANY (prop_ids));
        context_ids := context_ids || ARRAY(
            SELECT c.context_id FROM bond_default_proposal_evidence_v2 p
            JOIN bond_default_family_context_v2 c ON c.publication_id = p.publication_id
            WHERE p.publication_id = pub_id AND p.proposal_evidence_id = ANY (prop_ids)
              AND p.proposed_status = 'accepted_state' AND (
                  c.report_date = p.onset_upper_inclusive
                  OR c.report_date IS NOT DISTINCT FROM p.onset_lower_exclusive
                  OR EXISTS (SELECT 1 FROM bond_credit_observation o
                             WHERE o.package_id = ANY (pkgs) AND NOT (o.observation_id = ANY (stale_obs))
                               AND o.public_available_at <= k AND o.observation_kind = 'nport_holding'
                               AND o.cusip9 = p.cusip9 AND o.report_date = c.report_date
                               AND o.report_date < p.onset_upper_inclusive AND o.registrant_cik IS NOT NULL
                               AND o.field_presence ->> 'nport_is_default' = 'present'
                               AND o.nport_is_default IN ('Y', 'N')
                               AND (o.nport_is_default = 'Y' OR p.onset_lower_exclusive IS NULL
                                    OR o.report_date > p.onset_lower_exclusive))));

        missing := missing OR EXISTS (SELECT 1 FROM pg_catalog.unnest(member_ids) x
            WHERE NOT EXISTS (SELECT 1 FROM bond_default_family_evidence_v2 m
                              WHERE m.publication_id = pub_id AND m.family_evidence_id = x));
        context_ids := context_ids || ARRAY(SELECT m.context_id FROM bond_default_family_evidence_v2 m
                                            WHERE m.publication_id = pub_id AND m.family_evidence_id = ANY (member_ids));
        context_ids := ARRAY(SELECT DISTINCT c.context_id FROM bond_default_family_context_v2 c
                             WHERE c.publication_id = pub_id AND c.context_id = ANY (context_ids) ORDER BY 1);
        member_ids := member_ids || ARRAY(SELECT m.family_evidence_id FROM bond_default_family_evidence_v2 m
                                          WHERE m.publication_id = pub_id AND m.context_id = ANY (context_ids));
        obs_ids := obs_ids || ARRAY(SELECT x FROM bond_default_family_context_v2 c
            CROSS JOIN LATERAL pg_catalog.unnest(c.vote_observation_ids) x
            WHERE c.publication_id = pub_id AND c.context_id = ANY (context_ids));
        filing_ids := filing_ids || ARRAY(SELECT x FROM bond_default_family_context_v2 c
            CROSS JOIN LATERAL pg_catalog.unnest(c.selection_filing_ids) x
            WHERE c.publication_id = pub_id AND c.context_id = ANY (context_ids));
        package_ids := package_ids || ARRAY(SELECT x FROM bond_default_family_context_v2 c
            CROSS JOIN LATERAL pg_catalog.unnest(c.index_package_ids) x
            WHERE c.publication_id = pub_id AND c.context_id = ANY (context_ids));

        missing := missing OR EXISTS (SELECT 1 FROM pg_catalog.unnest(filing_ids) x
            WHERE NOT EXISTS (SELECT 1 FROM bond_default_ncen_filing f
                              WHERE f.package_id = ANY (pkgs) AND f.filing_evidence_id = x));
        filing_ids := ARRAY(SELECT DISTINCT f.filing_evidence_id FROM bond_default_ncen_filing f
                            WHERE f.package_id = ANY (pkgs) AND f.filing_evidence_id = ANY (filing_ids) ORDER BY 1);
        filing_ids := filing_ids || ARRAY(SELECT x FROM bond_default_ncen_filing f
            CROSS JOIN LATERAL pg_catalog.unnest(f.version_evidence_filing_ids) x
            WHERE f.filing_evidence_id = ANY (filing_ids));
        package_ids := package_ids || ARRAY(SELECT f.package_id FROM bond_default_ncen_filing f
                                            WHERE f.filing_evidence_id = ANY (filing_ids))
            || ARRAY(SELECT f.header_package_id FROM bond_default_ncen_filing f
                     WHERE f.filing_evidence_id = ANY (filing_ids) AND f.header_package_id IS NOT NULL);

        adj_ids := adj_ids || ARRAY(SELECT bond_credit_scope_head(pkgs, eff_ids, policy, l.link_id)
            FROM bond_default_event_link l WHERE l.link_id = ANY (reached_link_ids) AND l.package_id = ANY (pkgs)
              AND l.status = 'quarantined' AND bond_credit_scope_head(pkgs, eff_ids, policy, l.link_id) IS NOT NULL);
        missing := missing OR EXISTS (SELECT 1 FROM pg_catalog.unnest(reached_link_ids) x
            WHERE NOT EXISTS (SELECT 1 FROM bond_default_event_link l WHERE l.package_id = ANY (pkgs) AND l.link_id = x));
        unresolved := unresolved OR EXISTS (
            SELECT 1 FROM bond_default_event_link l
            WHERE l.package_id = ANY (pkgs) AND l.link_id = ANY (reached_link_ids)
              AND l.status <> 'admitted'
              AND (l.status <> 'quarantined'
                   OR bond_credit_scope_head(pkgs, eff_ids, policy, l.link_id) IS NULL));
        reached_link_ids := ARRAY(SELECT DISTINCT l.link_id FROM bond_default_event_link l
                                  WHERE l.package_id = ANY (pkgs) AND l.link_id = ANY (reached_link_ids) ORDER BY 1);
        obs_ids := obs_ids || ARRAY(SELECT l.observation_id FROM bond_default_event_link l
                                    WHERE l.link_id = ANY (reached_link_ids));
        package_ids := package_ids || ARRAY(SELECT l.package_id FROM bond_default_event_link l
                                            WHERE l.link_id = ANY (reached_link_ids));

        missing := missing OR EXISTS (SELECT 1 FROM pg_catalog.unnest(obs_ids) x
            WHERE NOT EXISTS (SELECT 1 FROM bond_credit_observation o WHERE o.package_id = ANY (pkgs) AND o.observation_id = x));
        obs_ids := ARRAY(SELECT DISTINCT o.observation_id FROM bond_credit_observation o
                         WHERE o.package_id = ANY (pkgs) AND o.observation_id = ANY (obs_ids) ORDER BY 1);
        package_ids := package_ids || ARRAY(SELECT o.package_id FROM bond_credit_observation o
                                            WHERE o.observation_id = ANY (obs_ids))
            || ARRAY(SELECT a.package_id FROM bond_default_adjudication a WHERE a.adjudication_id = ANY (adj_ids));

        missing := missing OR EXISTS (SELECT 1 FROM pg_catalog.unnest(package_ids) x
            WHERE NOT (x = ANY (pkgs)));
        package_ids := ARRAY(SELECT DISTINCT p.package_id FROM bond_default_source_package p
                             WHERE p.package_id = ANY (pkgs) AND p.package_id = ANY (package_ids) ORDER BY 1);
        package_ids := package_ids || ARRAY(SELECT p.revision_of_package_id FROM bond_default_source_package p
            WHERE p.package_id = ANY (package_ids) AND p.revision_of_package_id IS NOT NULL);

        SELECT COALESCE(ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(obs_ids) x ORDER BY 1), '{}'),
               COALESCE(ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(reached_link_ids) x ORDER BY 1), '{}'),
               COALESCE(ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(adj_ids) x ORDER BY 1), '{}'),
               COALESCE(ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(prop_ids) x ORDER BY 1), '{}'),
               COALESCE(ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(relation_ids) x ORDER BY 1), '{}'),
               COALESCE(ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(member_ids) x ORDER BY 1), '{}'),
               COALESCE(ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(context_ids) x ORDER BY 1), '{}'),
               COALESCE(ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(filing_ids) x ORDER BY 1), '{}'),
               COALESCE(ARRAY(SELECT DISTINCT x FROM pg_catalog.unnest(package_ids) x ORDER BY 1), '{}')
        INTO obs_ids, reached_link_ids, adj_ids, prop_ids, relation_ids, member_ids, context_ids, filing_ids, package_ids;
        after_counts := pg_catalog.concat_ws('|', cardinality(obs_ids), cardinality(reached_link_ids), cardinality(adj_ids),
            cardinality(prop_ids), cardinality(relation_ids), cardinality(member_ids), cardinality(context_ids),
            cardinality(filing_ids), cardinality(package_ids));
        EXIT WHEN after_counts = before_counts;
    END LOOP;

    IF missing THEN
        RAISE EXCEPTION 'bond_credit_validate:dependency_missing';
    END IF;
    IF unresolved THEN
        RAISE EXCEPTION 'bond_credit_validate:dependency_unresolved';
    END IF;
    WITH RECURSIVE edges(src, dst) AS (
        SELECT 'link:' || l.link_id::text,
               'adjudication:' || bond_credit_scope_head(pkgs, eff_ids, policy, l.link_id)::text
        FROM bond_default_event_link l
        WHERE l.link_id = ANY (reached_link_ids) AND l.status = 'quarantined'
          AND bond_credit_scope_head(pkgs, eff_ids, policy, l.link_id) IS NOT NULL
        UNION ALL
        SELECT 'adjudication:' || a.adjudication_id::text, 'link:' || edge.link_id::text
        FROM bond_default_adjudication a
        CROSS JOIN LATERAL pg_catalog.unnest(a.link_ids) AS edge(link_id)
        WHERE a.adjudication_id = ANY (adj_ids) AND edge.link_id = ANY (reached_link_ids)
          AND NOT (a.subject_kind = 'issue_scope' AND edge.link_id = a.subject_id)
    ), walk(node, path, cycle) AS (
        SELECT e.dst, ARRAY[e.src, e.dst]::text[], e.dst = e.src FROM edges e
        UNION ALL
        SELECT e.dst, w.path || e.dst, e.dst = ANY (w.path)
        FROM walk w JOIN edges e ON e.src = w.node WHERE NOT w.cycle
    )
    SELECT node INTO cycle_node FROM walk WHERE cycle ORDER BY node LIMIT 1;
    IF cycle_node IS NOT NULL THEN
        RAISE EXCEPTION 'bond_credit_validate:dependency_cycle:%', cycle_node;
    END IF;

    SELECT pg_catalog.max(t) INTO known_at FROM (
        SELECT o.public_available_at AS t FROM bond_credit_observation o WHERE o.observation_id = ANY (obs_ids)
        UNION ALL SELECT l.link_known_at FROM bond_default_event_link l WHERE l.link_id = ANY (reached_link_ids)
        UNION ALL SELECT c.evidence_known_at FROM bond_default_family_context_v2 c
                  WHERE c.publication_id = pub_id AND c.context_id = ANY (context_ids)) s;
    SELECT pg_catalog.max(bond_credit_link_time(pkgs, eff_ids, policy, x)) INTO link_known_at
    FROM pg_catalog.unnest(reached_link_ids) x;
    RETURN pg_catalog.jsonb_build_object(
        'observations', pg_catalog.to_jsonb(obs_ids), 'event_links', pg_catalog.to_jsonb(reached_link_ids),
        'adjudications', pg_catalog.to_jsonb(adj_ids), 'proposal_evidence', pg_catalog.to_jsonb(prop_ids),
        'exchange_relations', pg_catalog.to_jsonb(relation_ids), 'family_evidence', pg_catalog.to_jsonb(member_ids),
        'family_contexts', pg_catalog.to_jsonb(context_ids), 'ncen_filings', pg_catalog.to_jsonb(filing_ids),
        'source_packages', pg_catalog.to_jsonb(package_ids), 'known_at', pg_catalog.to_jsonb(known_at),
        'link_known_at', pg_catalog.to_jsonb(link_known_at), 'missing', pg_catalog.to_jsonb(missing));
END $$;

CREATE OR REPLACE FUNCTION bond_credit_proposal_known_at(pub_id uuid, pkgs uuid[], stale_obs uuid[],
                                                         stale_links uuid[], eff_ids uuid[], policy text,
                                                         prop_id uuid)
RETURNS timestamptz LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT (bond_credit_dependency_closure(pub_id, pkgs, stale_obs, stale_links, eff_ids, policy,
        '{}', '{}', '{}', ARRAY[prop_id], '{}') ->> 'known_at')::timestamptz
$$;

CREATE OR REPLACE FUNCTION bond_credit_relation_known_at(pkgs uuid[], stale_obs uuid[], stale_links uuid[],
                                                         eff_ids uuid[], policy text,
                                                         rel bond_default_exchange_relation_v2)
RETURNS timestamptz LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT (bond_credit_dependency_closure(rel.publication_id, pkgs, stale_obs, stale_links, eff_ids, policy,
        '{}', '{}', '{}', '{}', ARRAY[rel.relation_id]) ->> 'known_at')::timestamptz
$$;

CREATE OR REPLACE FUNCTION bond_credit_support_known_at(pub_id uuid, pkgs uuid[], stale_obs uuid[],
                                                        stale_links uuid[], eff_ids uuid[], policy text,
                                                        evidence uuid[], decisions uuid[])
RETURNS timestamptz LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT (bond_credit_dependency_closure(pub_id, pkgs, stale_obs, stale_links, eff_ids, policy,
        evidence, '{}', decisions, '{}', '{}') ->> 'known_at')::timestamptz
$$;

CREATE OR REPLACE FUNCTION bond_credit_event_closure(pkgs uuid[], stale_obs uuid[], stale_links uuid[],
                                                     eff_ids uuid[], policy text, ev bond_default_event_v1)
RETURNS jsonb LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT bond_credit_dependency_closure(ev.publication_id, pkgs, stale_obs, stale_links, eff_ids, policy,
        ev.evidence_observation_ids || ev.onset_lower_evidence_ids, ev.link_ids, ev.adjudication_ids,
        ev.proposal_evidence_ids, ev.exchange_relation_ids)
$$;

CREATE OR REPLACE FUNCTION bond_credit_event_link_known_at(pkgs uuid[], stale_obs uuid[], stale_links uuid[],
                                                           eff_ids uuid[], policy text, ev bond_default_event_v1)
RETURNS timestamptz LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT (bond_credit_event_closure(pkgs, stale_obs, stale_links, eff_ids, policy, ev) ->> 'link_known_at')::timestamptz
$$;

CREATE OR REPLACE FUNCTION bond_credit_event_evidence_known_at(pkgs uuid[], stale_obs uuid[], stale_links uuid[],
                                                               eff_ids uuid[], policy text, ev bond_default_event_v1)
RETURNS timestamptz LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT (bond_credit_event_closure(pkgs, stale_obs, stale_links, eff_ids, policy, ev) ->> 'known_at')::timestamptz
$$;

CREATE OR REPLACE FUNCTION bond_credit_event_dependency_digest(pkgs uuid[], stale_obs uuid[], stale_links uuid[],
                                                               eff_ids uuid[], policy text, ev bond_default_event_v1)
RETURNS text LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    closure jsonb := bond_credit_event_closure(pkgs, stale_obs, stale_links, eff_ids, policy, ev);
    ids uuid[];
    parts text[] := '{}';
    frame text;
    hashes text[];
BEGIN
    FOREACH frame IN ARRAY ARRAY['adjudications', 'event_links', 'exchange_relations', 'family_contexts',
                                 'family_evidence', 'ncen_filings', 'observations', 'proposal_evidence',
                                 'source_packages'] LOOP
        ids := ARRAY(SELECT x::uuid FROM pg_catalog.jsonb_array_elements_text(closure -> frame) x);
        hashes := CASE frame
            WHEN 'adjudications' THEN ARRAY(SELECT bond_credit_checked_row_sha256(frame, pg_catalog.to_jsonb(t))
                                            FROM bond_default_adjudication t WHERE t.adjudication_id = ANY (ids))
            WHEN 'event_links' THEN ARRAY(SELECT bond_credit_checked_row_sha256(frame, pg_catalog.to_jsonb(t))
                                          FROM bond_default_event_link t WHERE t.link_id = ANY (ids))
            WHEN 'exchange_relations' THEN ARRAY(SELECT bond_credit_checked_row_sha256(frame, pg_catalog.to_jsonb(t))
                                                 FROM bond_default_exchange_relation_v2 t
                                                 WHERE t.publication_id = ev.publication_id AND t.relation_id = ANY (ids))
            WHEN 'family_contexts' THEN ARRAY(SELECT bond_credit_checked_row_sha256(frame, pg_catalog.to_jsonb(t))
                                              FROM bond_default_family_context_v2 t
                                              WHERE t.publication_id = ev.publication_id AND t.context_id = ANY (ids))
            WHEN 'family_evidence' THEN ARRAY(SELECT bond_credit_checked_row_sha256(frame, pg_catalog.to_jsonb(t))
                                              FROM bond_default_family_evidence_v2 t
                                              WHERE t.publication_id = ev.publication_id AND t.family_evidence_id = ANY (ids))
            WHEN 'ncen_filings' THEN ARRAY(SELECT bond_credit_checked_row_sha256(frame, pg_catalog.to_jsonb(t))
                                           FROM bond_default_ncen_filing t WHERE t.filing_evidence_id = ANY (ids))
            WHEN 'observations' THEN ARRAY(SELECT bond_credit_checked_row_sha256(frame, pg_catalog.to_jsonb(t))
                                           FROM bond_credit_observation t WHERE t.observation_id = ANY (ids))
            WHEN 'proposal_evidence' THEN ARRAY(SELECT bond_credit_checked_row_sha256(frame, pg_catalog.to_jsonb(t))
                                                FROM bond_default_proposal_evidence_v2 t
                                                WHERE t.publication_id = ev.publication_id AND t.proposal_evidence_id = ANY (ids))
            ELSE ARRAY(SELECT bond_credit_checked_row_sha256(frame, pg_catalog.to_jsonb(t))
                       FROM bond_default_source_package t WHERE t.package_id = ANY (ids)) END;
        IF cardinality(hashes) > 0 THEN
            parts := parts || ('{"count":' || cardinality(hashes)::text || ',"digest":'
                               || pg_catalog.to_json(bond_credit_frame_digest(hashes))::text || ',"frame":'
                               || pg_catalog.to_json(frame)::text || '}');
        END IF;
    END LOOP;
    RETURN bond_credit_text_digest('[' || pg_catalog.array_to_string(parts, ',') || ']');
END $$;

-- Guarded output-frame readers (see bond_credit_read_publication in bond_credit_publications_v1.sql).
CREATE OR REPLACE FUNCTION bond_credit_read_events(target_publication_id uuid, allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_event_v1
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_event_v1 r WHERE r.publication_id = target_publication_id;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_followups(target_publication_id uuid, allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_followup_v1
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_followup_v1 r WHERE r.publication_id = target_publication_id;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_exit_evidence(target_publication_id uuid,
                                                          allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_exit_evidence_v1
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_exit_evidence_v1 r WHERE r.publication_id = target_publication_id;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_coverage(target_publication_id uuid, allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_coverage_v1
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_coverage_v1 r WHERE r.publication_id = target_publication_id;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_family_contexts(target_publication_id uuid,
                                                            allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_family_context_v2
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_family_context_v2 r WHERE r.publication_id = target_publication_id;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_family_evidence(target_publication_id uuid,
                                                            allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_family_evidence_v2
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_family_evidence_v2 r WHERE r.publication_id = target_publication_id;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_proposal_evidence(target_publication_id uuid,
                                                              allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_proposal_evidence_v2
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_proposal_evidence_v2 r WHERE r.publication_id = target_publication_id;
END $$;

CREATE OR REPLACE FUNCTION bond_credit_read_exchange_relations(target_publication_id uuid,
                                                               allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_default_exchange_relation_v2
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_default_exchange_relation_v2 r WHERE r.publication_id = target_publication_id;
END $$;

-- The serving reader reads only through the guarded bond_credit_read_* functions.
REVOKE ALL ON bond_default_event_v1, bond_default_followup_v1, bond_default_exit_evidence_v1,
    bond_default_coverage_v1, bond_default_family_context_v2, bond_default_family_evidence_v2,
    bond_default_proposal_evidence_v2, bond_default_exchange_relation_v2 FROM PUBLIC, bond_credit_reader;
GRANT SELECT ON bond_default_event_v1, bond_default_followup_v1, bond_default_exit_evidence_v1,
    bond_default_coverage_v1, bond_default_family_context_v2, bond_default_family_evidence_v2,
    bond_default_proposal_evidence_v2, bond_default_exchange_relation_v2 TO bond_credit_auditor;
GRANT SELECT, INSERT ON bond_default_event_v1, bond_default_followup_v1, bond_default_exit_evidence_v1,
    bond_default_coverage_v1, bond_default_family_context_v2, bond_default_family_evidence_v2,
    bond_default_proposal_evidence_v2, bond_default_exchange_relation_v2 TO bond_credit_writer;
REVOKE ALL ON FUNCTION bond_credit_event_input_digest(uuid[], uuid[], uuid[], uuid[], uuid[], text),
    bond_credit_timing_class(date, date), bond_credit_read_events(uuid, boolean),
    bond_credit_read_followups(uuid, boolean), bond_credit_read_exit_evidence(uuid, boolean),
    bond_credit_read_coverage(uuid, boolean), bond_credit_read_family_contexts(uuid, boolean),
    bond_credit_read_family_evidence(uuid, boolean), bond_credit_read_proposal_evidence(uuid, boolean),
    bond_credit_read_exchange_relations(uuid, boolean),
    bond_credit_dependency_closure(uuid, uuid[], uuid[], uuid[], uuid[], text, uuid[], uuid[], uuid[], uuid[], uuid[]),
    bond_credit_proposal_known_at(uuid, uuid[], uuid[], uuid[], uuid[], text, uuid),
    bond_credit_relation_known_at(uuid[], uuid[], uuid[], uuid[], text, bond_default_exchange_relation_v2),
    bond_credit_support_known_at(uuid, uuid[], uuid[], uuid[], uuid[], text, uuid[], uuid[]),
    bond_credit_event_closure(uuid[], uuid[], uuid[], uuid[], text, bond_default_event_v1),
    bond_credit_event_link_known_at(uuid[], uuid[], uuid[], uuid[], text, bond_default_event_v1),
    bond_credit_event_evidence_known_at(uuid[], uuid[], uuid[], uuid[], text, bond_default_event_v1),
    bond_credit_event_dependency_digest(uuid[], uuid[], uuid[], uuid[], text, bond_default_event_v1) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bond_credit_event_input_digest(uuid[], uuid[], uuid[], uuid[], uuid[], text),
    bond_credit_timing_class(date, date) TO bond_credit_reader, bond_credit_writer;
GRANT EXECUTE ON FUNCTION bond_credit_read_events(uuid, boolean), bond_credit_read_followups(uuid, boolean),
    bond_credit_read_exit_evidence(uuid, boolean), bond_credit_read_coverage(uuid, boolean),
    bond_credit_read_family_contexts(uuid, boolean), bond_credit_read_family_evidence(uuid, boolean),
    bond_credit_read_proposal_evidence(uuid, boolean), bond_credit_read_exchange_relations(uuid, boolean)
    TO bond_credit_reader;

COMMIT;
