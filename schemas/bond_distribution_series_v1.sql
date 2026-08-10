-- Governed Regulation S / Rule 144A distribution-series registry.
--
-- This is an additive, non-activating registry.  It is deliberately separate
-- from bond_security_v1: a Common Code is registry-local evidence, never a
-- global security alias.  Every table is insert-only so a later correction is
-- a new evidence/observation/snapshot/decision row rather than a rewrite.

CREATE TABLE IF NOT EXISTS bond_distribution_source_evidence (
    source_evidence_id   text PRIMARY KEY CHECK (source_evidence_id <> ''),
    sec_accession        text NOT NULL CHECK (sec_accession <> ''),
    form_type            text NOT NULL CHECK (form_type <> ''),
    document_type        text NOT NULL CHECK (document_type <> ''),
    filed_at             timestamptz,
    search_query_id      text,
    source_url           text NOT NULL CHECK (source_url <> ''),
    document_url         text,
    retrieved_at         timestamptz NOT NULL,
    raw_document_sha256  char(64) NOT NULL CHECK (raw_document_sha256 ~ '^[0-9a-f]{64}$'),
    parser_version       text NOT NULL CHECK (parser_version <> ''),
    created_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bond_distribution_parser_observation (
    parser_observation_id text PRIMARY KEY CHECK (parser_observation_id <> ''),
    source_evidence_id    text NOT NULL REFERENCES bond_distribution_source_evidence(source_evidence_id) ON DELETE RESTRICT,
    parser_version        text NOT NULL CHECK (parser_version <> ''),
    block_locator         text NOT NULL CHECK (block_locator <> ''),
    exact_source_label    text NOT NULL CHECK (exact_source_label <> ''),
    source_value          text NOT NULL CHECK (source_value <> ''),
    normalized_value      text,
    observation_state     text NOT NULL CHECK (observation_state IN ('candidate','validated','rejected')),
    created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bond_distribution_mapping_snapshot (
    snapshot_id       text PRIMARY KEY CHECK (snapshot_id <> ''),
    snapshot_status   text NOT NULL CHECK (snapshot_status IN ('draft','approved','revoked')),
    content_hash      char(64) NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    created_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (content_hash)
);

-- A snapshot remains an immutable draft while its decisions and identifiers are
-- inserted.  This separate immutable approval record closes that composition;
-- it repeats the draft's content hash so an approval cannot silently authorize
-- a different artifact.  The content hash is computed by the controlled loader path from canonical decisions/identifiers (no weak SQL pseudo-hash is used).
-- Production grants must restrict direct approval INSERT to that loader role/path.
CREATE TABLE IF NOT EXISTS bond_distribution_snapshot_approval (
    snapshot_id       text PRIMARY KEY REFERENCES bond_distribution_mapping_snapshot(snapshot_id) ON DELETE RESTRICT,
    content_hash      char(64) NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    approved_at       timestamptz NOT NULL DEFAULT now(),
    CHECK (content_hash <> '')
);

CREATE TABLE IF NOT EXISTS bond_distribution_pair_decision (
    decision_id             text PRIMARY KEY CHECK (decision_id <> ''),
    snapshot_id             text NOT NULL REFERENCES bond_distribution_mapping_snapshot(snapshot_id) ON DELETE RESTRICT,
    pair_key                text NOT NULL CHECK (pair_key <> ''),
    decision_state          text NOT NULL CHECK (decision_state IN ('candidate','approved','ambiguous','rejected','revoked')),
    source_observation_id   text REFERENCES bond_distribution_parser_observation(parser_observation_id) ON DELETE RESTRICT,
    valid_from              date NOT NULL,
    valid_to                date,
    created_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (snapshot_id, pair_key, decision_state, valid_from),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (decision_state <> 'approved' OR source_observation_id IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS bond_distribution_pair_identifier (
    identifier_id        text PRIMARY KEY CHECK (identifier_id <> ''),
    decision_id          text NOT NULL REFERENCES bond_distribution_pair_decision(decision_id) ON DELETE RESTRICT,
    source_observation_id text NOT NULL REFERENCES bond_distribution_parser_observation(parser_observation_id) ON DELETE RESTRICT,
    distribution_rule    text NOT NULL CHECK (distribution_rule IN ('reg_s','rule_144a')),
    identifier_kind      text NOT NULL CHECK (identifier_kind IN ('cusip9','isin','common_code')),
    identifier_value     text NOT NULL CHECK (identifier_value <> ''),
    identifier_tenure    text NOT NULL CHECK (identifier_tenure IN ('temporary','permanent','not_stated')),
    valid_from           date NOT NULL,
    valid_to             date,
    created_at           timestamptz NOT NULL DEFAULT now(),
    UNIQUE (decision_id, distribution_rule, identifier_kind, identifier_value, valid_from),
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE INDEX IF NOT EXISTS bond_distribution_pair_identifier_lookup_idx
ON bond_distribution_pair_identifier (decision_id, distribution_rule, identifier_kind, identifier_value, valid_from);

-- A decision's mapping is recorded through the explicit per-pair identifiers;
-- no db_type, is_144a, ISIN prefix, CUSIP shape, terms, or identifier absence is
-- a permissible substitute.  The trigger below ensures an approved
-- snapshot/reference CUSIP cannot map to multiple Reg S CUSIPs over overlapping
-- validity windows.
CREATE OR REPLACE FUNCTION bond_distribution_prevent_conflicting_approved_cusip_mapping()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM bond_distribution_pair_decision current_decision
        JOIN bond_distribution_mapping_snapshot current_snapshot
          ON current_snapshot.snapshot_id = current_decision.snapshot_id
        JOIN bond_distribution_pair_identifier current_ref
          ON current_ref.decision_id = current_decision.decision_id
         AND current_ref.distribution_rule = 'rule_144a'
         AND current_ref.identifier_kind = 'cusip9'
        JOIN bond_distribution_pair_identifier current_regs
          ON current_regs.decision_id = current_decision.decision_id
         AND current_regs.distribution_rule = 'reg_s'
         AND current_regs.identifier_kind = 'cusip9'
        JOIN bond_distribution_pair_decision other_decision
          ON other_decision.snapshot_id = current_decision.snapshot_id
         AND other_decision.decision_state = 'approved'
        JOIN bond_distribution_pair_identifier other_ref
          ON other_ref.decision_id = other_decision.decision_id
         AND other_ref.distribution_rule = 'rule_144a'
         AND other_ref.identifier_kind = 'cusip9'
         AND other_ref.identifier_value = current_ref.identifier_value
        JOIN bond_distribution_pair_identifier other_regs
          ON other_regs.decision_id = other_decision.decision_id
         AND other_regs.distribution_rule = 'reg_s'
         AND other_regs.identifier_kind = 'cusip9'
         AND other_regs.identifier_value <> current_regs.identifier_value
        WHERE current_decision.decision_id = NEW.decision_id
          AND current_decision.decision_state = 'approved'
          AND daterange(current_decision.valid_from, current_decision.valid_to, '[)')
              && daterange(other_decision.valid_from, other_decision.valid_to, '[)')
          AND daterange(current_ref.valid_from, current_ref.valid_to, '[)')
              && daterange(other_ref.valid_from, other_ref.valid_to, '[)')
          AND daterange(current_regs.valid_from, current_regs.valid_to, '[)')
              && daterange(other_regs.valid_from, other_regs.valid_to, '[)')
    ) THEN
        RAISE EXCEPTION 'approved snapshot/reference CUSIP cannot map to multiple Reg S CUSIPs';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_distribution_pair_identifier_mapping_guard ON bond_distribution_pair_identifier;
CREATE TRIGGER bond_distribution_pair_identifier_mapping_guard
AFTER INSERT ON bond_distribution_pair_identifier
FOR EACH ROW EXECUTE FUNCTION bond_distribution_prevent_conflicting_approved_cusip_mapping();

-- An identifier is not independent evidence.  It must reproduce exactly the
-- normalized value and explicit, anchored label taxonomy of its linked parser
-- observation.  CINS is intentionally represented as the registry's cusip9
-- kind; no CUSIP/ISIN shape inference is performed.
CREATE OR REPLACE FUNCTION bond_distribution_pair_identifier_observation_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    observed_value text;
    observed_label text;
    observed_state text;
    expected_rule text;
    expected_kind text;
BEGIN
    SELECT normalized_value, exact_source_label, observation_state
    INTO observed_value, observed_label, observed_state
    FROM bond_distribution_parser_observation
    WHERE parser_observation_id = NEW.source_observation_id;

    IF observed_value IS NULL OR observed_state <> 'validated' THEN
        RAISE EXCEPTION 'identifier source observation does not match value/kind/side taxonomy';
    END IF;
    IF observed_label ~* '^(rule[[:space:]]*144a|144a)[[:space:]]+(cusip|cins)$' THEN
        expected_rule := 'rule_144a'; expected_kind := 'cusip9';
    ELSIF observed_label ~* '^(rule[[:space:]]*144a|144a)[[:space:]]+isin$' THEN
        expected_rule := 'rule_144a'; expected_kind := 'isin';
    ELSIF observed_label ~* '^(rule[[:space:]]*144a|144a)[[:space:]]+common[[:space:]]+code$' THEN
        expected_rule := 'rule_144a'; expected_kind := 'common_code';
    ELSIF observed_label ~* '^(regulation[[:space:]]*s|reg[[:space:]]*s)[[:space:]]+(cusip|cins)$' THEN
        expected_rule := 'reg_s'; expected_kind := 'cusip9';
    ELSIF observed_label ~* '^(regulation[[:space:]]*s|reg[[:space:]]*s)[[:space:]]+isin$' THEN
        expected_rule := 'reg_s'; expected_kind := 'isin';
    ELSIF observed_label ~* '^(regulation[[:space:]]*s|reg[[:space:]]*s)[[:space:]]+common[[:space:]]+code$' THEN
        expected_rule := 'reg_s'; expected_kind := 'common_code';
    ELSE
        RAISE EXCEPTION 'identifier source observation does not match value/kind/side taxonomy';
    END IF;
    IF NEW.identifier_value <> observed_value
       OR NEW.distribution_rule <> expected_rule
       OR NEW.identifier_kind <> expected_kind THEN
        RAISE EXCEPTION 'identifier source observation does not match value/kind/side taxonomy';
    END IF;
    -- This is the exact executable subset of normalize_cusip9: the other
    -- canonical placeholders and synthetic prefixes already fail the form.
    IF (expected_kind = 'cusip9' AND (
            observed_value !~ '^[A-Z0-9]{9}$'
            OR observed_value IN ('000000000','XXXXXXXXX','NNNNNNNNN','999999999')
        ))
       OR (expected_kind = 'isin' AND observed_value !~ '^[A-Z0-9]{12}$')
       OR (expected_kind = 'common_code' AND observed_value !~ '^[0-9]{9}$') THEN
        RAISE EXCEPTION 'identifier source observation has invalid executable syntax';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_distribution_pair_identifier_observation_guard ON bond_distribution_pair_identifier;
CREATE TRIGGER bond_distribution_pair_identifier_observation_guard
BEFORE INSERT ON bond_distribution_pair_identifier
FOR EACH ROW EXECUTE FUNCTION bond_distribution_pair_identifier_observation_guard();

-- Snapshot composition is closed after approval: decisions and identifiers can
-- only be appended while no approval record exists for their draft snapshot.
CREATE OR REPLACE FUNCTION bond_distribution_snapshot_composition_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_snapshot_id text;
BEGIN
    IF TG_TABLE_NAME = 'bond_distribution_pair_decision' THEN
        target_snapshot_id := NEW.snapshot_id;
    ELSE
        SELECT snapshot_id INTO target_snapshot_id
        FROM bond_distribution_pair_decision WHERE decision_id = NEW.decision_id;
    END IF;
    PERFORM 1
    FROM bond_distribution_mapping_snapshot
    WHERE snapshot_id = target_snapshot_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'snapshot composition requires an existing snapshot';
    END IF;
    IF EXISTS (
        SELECT 1 FROM bond_distribution_snapshot_approval
        WHERE snapshot_id = target_snapshot_id
    ) THEN
        RAISE EXCEPTION 'snapshot composition is closed after approval';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_distribution_pair_decision_composition_guard ON bond_distribution_pair_decision;
CREATE TRIGGER bond_distribution_pair_decision_composition_guard
BEFORE INSERT ON bond_distribution_pair_decision
FOR EACH ROW EXECUTE FUNCTION bond_distribution_snapshot_composition_guard();

DROP TRIGGER IF EXISTS bond_distribution_pair_identifier_composition_guard ON bond_distribution_pair_identifier;
CREATE TRIGGER bond_distribution_pair_identifier_composition_guard
BEFORE INSERT ON bond_distribution_pair_identifier
FOR EACH ROW EXECUTE FUNCTION bond_distribution_snapshot_composition_guard();

CREATE OR REPLACE FUNCTION bond_distribution_snapshot_approval_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE snapshot_hash char(64);
        snapshot_state text;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'bond_distribution_snapshot_approval is immutable';
    END IF;
    SELECT content_hash, snapshot_status INTO snapshot_hash, snapshot_state
    FROM bond_distribution_mapping_snapshot WHERE snapshot_id = NEW.snapshot_id FOR UPDATE;
    IF snapshot_hash IS NULL OR snapshot_state <> 'draft' OR snapshot_hash <> NEW.content_hash THEN
        RAISE EXCEPTION 'snapshot approval requires matching draft snapshot content hash';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bond_distribution_snapshot_approval_guard ON bond_distribution_snapshot_approval;
CREATE TRIGGER bond_distribution_snapshot_approval_guard
BEFORE INSERT OR UPDATE OR DELETE ON bond_distribution_snapshot_approval
FOR EACH ROW EXECUTE FUNCTION bond_distribution_snapshot_approval_guard();

CREATE OR REPLACE FUNCTION bond_distribution_prevent_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is immutable', TG_TABLE_NAME;
END $$;

-- bond_distribution_source_evidence is immutable
DROP TRIGGER IF EXISTS bond_distribution_source_evidence_immutable ON bond_distribution_source_evidence;
CREATE TRIGGER bond_distribution_source_evidence_immutable
BEFORE UPDATE OR DELETE ON bond_distribution_source_evidence
FOR EACH ROW EXECUTE FUNCTION bond_distribution_prevent_mutation();

-- bond_distribution_parser_observation is immutable
DROP TRIGGER IF EXISTS bond_distribution_parser_observation_immutable ON bond_distribution_parser_observation;
CREATE TRIGGER bond_distribution_parser_observation_immutable
BEFORE UPDATE OR DELETE ON bond_distribution_parser_observation
FOR EACH ROW EXECUTE FUNCTION bond_distribution_prevent_mutation();

-- bond_distribution_mapping_snapshot is immutable
DROP TRIGGER IF EXISTS bond_distribution_mapping_snapshot_immutable ON bond_distribution_mapping_snapshot;
CREATE TRIGGER bond_distribution_mapping_snapshot_immutable
BEFORE UPDATE OR DELETE ON bond_distribution_mapping_snapshot
FOR EACH ROW EXECUTE FUNCTION bond_distribution_prevent_mutation();

-- bond_distribution_snapshot_approval is immutable
DROP TRIGGER IF EXISTS bond_distribution_snapshot_approval_immutable ON bond_distribution_snapshot_approval;
CREATE TRIGGER bond_distribution_snapshot_approval_immutable
BEFORE UPDATE OR DELETE ON bond_distribution_snapshot_approval
FOR EACH ROW EXECUTE FUNCTION bond_distribution_prevent_mutation();

-- bond_distribution_pair_decision is immutable
DROP TRIGGER IF EXISTS bond_distribution_pair_decision_immutable ON bond_distribution_pair_decision;
CREATE TRIGGER bond_distribution_pair_decision_immutable
BEFORE UPDATE OR DELETE ON bond_distribution_pair_decision
FOR EACH ROW EXECUTE FUNCTION bond_distribution_prevent_mutation();

-- bond_distribution_pair_identifier is immutable
DROP TRIGGER IF EXISTS bond_distribution_pair_identifier_immutable ON bond_distribution_pair_identifier;
CREATE TRIGGER bond_distribution_pair_identifier_immutable
BEFORE UPDATE OR DELETE ON bond_distribution_pair_identifier
FOR EACH ROW EXECUTE FUNCTION bond_distribution_prevent_mutation();

-- The loader creates and re-applies this registry as postgres or worker_writer.
-- Runtime mutations and trigger maintenance require ownership, so normalize it
-- to the runtime role rather than relying on the installing session's defaults.
ALTER TABLE bond_distribution_source_evidence OWNER TO worker_writer;
ALTER TABLE bond_distribution_parser_observation OWNER TO worker_writer;
ALTER TABLE bond_distribution_mapping_snapshot OWNER TO worker_writer;
ALTER TABLE bond_distribution_snapshot_approval OWNER TO worker_writer;
ALTER TABLE bond_distribution_pair_decision OWNER TO worker_writer;
ALTER TABLE bond_distribution_pair_identifier OWNER TO worker_writer;
ALTER FUNCTION bond_distribution_prevent_conflicting_approved_cusip_mapping() OWNER TO worker_writer;
ALTER FUNCTION bond_distribution_pair_identifier_observation_guard() OWNER TO worker_writer;
ALTER FUNCTION bond_distribution_snapshot_composition_guard() OWNER TO worker_writer;
ALTER FUNCTION bond_distribution_snapshot_approval_guard() OWNER TO worker_writer;
ALTER FUNCTION bond_distribution_prevent_mutation() OWNER TO worker_writer;

REVOKE ALL ON TABLE bond_distribution_source_evidence FROM PUBLIC;
REVOKE ALL ON TABLE bond_distribution_parser_observation FROM PUBLIC;
REVOKE ALL ON TABLE bond_distribution_mapping_snapshot FROM PUBLIC;
REVOKE ALL ON TABLE bond_distribution_snapshot_approval FROM PUBLIC;
REVOKE ALL ON TABLE bond_distribution_pair_decision FROM PUBLIC;
REVOKE ALL ON TABLE bond_distribution_pair_identifier FROM PUBLIC;
REVOKE ALL ON FUNCTION bond_distribution_prevent_conflicting_approved_cusip_mapping() FROM PUBLIC;
REVOKE ALL ON FUNCTION bond_distribution_pair_identifier_observation_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION bond_distribution_snapshot_composition_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION bond_distribution_snapshot_approval_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION bond_distribution_prevent_mutation() FROM PUBLIC;
