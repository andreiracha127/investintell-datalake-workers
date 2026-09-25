-- Round3 (c30ab29) publication-receipt ledger, verbatim from
-- schemas/fund_nav_readiness_v1.sql at c30ab2971d022806e91ca04219b4052bd44630a9
-- (plus its REVOKE). Test-only: rebuilds the exact Round3 shape on a Round4
-- schema to exercise the restricted Round3 -> Round4 upgrade. Applied with
-- search_path <schema>, pg_temp like the operator.
CREATE TABLE IF NOT EXISTS nav_policy_publication_receipts (
    receipt_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    readiness_profile text NOT NULL CHECK (readiness_profile = 'current_daily_nav_v1'),
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    policy_hash char(64) NOT NULL CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
    plan_version text NOT NULL CHECK (plan_version = 'nav-schema-plan-v4'),
    plan_sha256 char(64) NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    policy_artifact_sha256 char(64) NOT NULL
        CHECK (policy_artifact_sha256 ~ '^[0-9a-f]{64}$'),
    policy_document_digest char(64) NOT NULL
        CHECK (policy_document_digest ~ '^[0-9a-f]{64}$'),
    audit_receipt_sha256 char(64) NOT NULL CHECK (audit_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    audit_dossier_sha256 char(64) NOT NULL CHECK (audit_dossier_sha256 ~ '^[0-9a-f]{64}$'),
    canary_manifest_sha256 char(64) NOT NULL
        CHECK (canary_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    capture_bundle_sha256 char(64) NOT NULL
        CHECK (capture_bundle_sha256 ~ '^[0-9a-f]{64}$'),
    previous_policy_id text,
    previous_policy_version text,
    previous_policy_hash char(64) CHECK (previous_policy_hash ~ '^[0-9a-f]{64}$'),
    captured_at timestamptz NOT NULL,
    sec_valid_until timestamptz NOT NULL,
    published_at timestamptz NOT NULL,
    commit_xid xid8 NOT NULL,
    UNIQUE (plan_sha256),
    FOREIGN KEY (policy_id, policy_version) REFERENCES nav_policy_versions,
    CHECK ((previous_policy_id IS NULL) = (previous_policy_version IS NULL)
           AND (previous_policy_id IS NULL) = (previous_policy_hash IS NULL)),
    CHECK (captured_at <= published_at AND published_at <= sec_valid_until)
);
CREATE INDEX IF NOT EXISTS nav_policy_publication_receipts_policy_idx
    ON nav_policy_publication_receipts (policy_id, policy_version, published_at DESC);
CREATE OR REPLACE FUNCTION nav_policy_publication_receipt_guard_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    stamp timestamptz := clock_timestamp();
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'NAV policy publication receipts are append-only';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM nav_policy_current c
        JOIN nav_policy_versions p
          ON p.policy_id = c.policy_id AND p.policy_version = c.policy_version
        WHERE c.readiness_profile = NEW.readiness_profile
          AND c.policy_id = NEW.policy_id AND c.policy_version = NEW.policy_version
          AND p.policy_hash = NEW.policy_hash
          AND p.published_at IS NOT NULL AND p.valid_through >= stamp
    ) THEN
        RAISE EXCEPTION 'NAV publication receipt must describe the current published unexpired policy';
    END IF;
    NEW.published_at := stamp;
    NEW.commit_xid := pg_current_xact_id();
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS nav_policy_publication_receipt_guard ON nav_policy_publication_receipts;
CREATE TRIGGER nav_policy_publication_receipt_guard
BEFORE INSERT OR UPDATE OR DELETE ON nav_policy_publication_receipts
FOR EACH ROW EXECUTE FUNCTION nav_policy_publication_receipt_guard_v1();
REVOKE ALL ON FUNCTION nav_policy_publication_receipt_guard_v1() FROM PUBLIC;
