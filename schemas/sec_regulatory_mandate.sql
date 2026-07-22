-- Sidecar output: prepared by a bounded build, never served by scanning raw RR1 rows.
CREATE TABLE IF NOT EXISTS sec_regulatory_mandate_profiles (
    publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    series_id text NOT NULL,
    class_id text NOT NULL DEFAULT '',
    document_id text NOT NULL DEFAULT '',
    objective_text text,
    objective_state text NOT NULL CHECK (objective_state IN ('available','unavailable','not_applicable','degraded')),
    objective_reason_code text,
    objective_source_date date,
    objective_provenance jsonb,
    strategy_text text,
    strategy_state text NOT NULL CHECK (strategy_state IN ('available','unavailable','not_applicable','degraded')),
    strategy_reason_code text,
    strategy_source_date date,
    strategy_provenance jsonb,
    concentration_policy_text text,
    concentration_policy_state text NOT NULL CHECK (concentration_policy_state IN ('available','unavailable','not_applicable','degraded')),
    concentration_policy_reason_code text,
    concentration_policy_source_date date,
    concentration_policy_provenance jsonb,
    principal_risks_text text,
    principal_risks_state text NOT NULL CHECK (principal_risks_state IN ('available','unavailable','not_applicable','degraded')),
    principal_risks_reason_code text,
    principal_risks_source_date date,
    principal_risks_provenance jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(publication_id,series_id,class_id,document_id),
    CHECK ((objective_state IN ('available','degraded')) = (objective_text IS NOT NULL AND objective_source_date IS NOT NULL AND objective_provenance IS NOT NULL)),
    CHECK ((strategy_state IN ('available','degraded')) = (strategy_text IS NOT NULL AND strategy_source_date IS NOT NULL AND strategy_provenance IS NOT NULL)),
    CHECK ((concentration_policy_state IN ('available','degraded')) = (concentration_policy_text IS NOT NULL AND concentration_policy_source_date IS NOT NULL AND concentration_policy_provenance IS NOT NULL)),
    CHECK ((principal_risks_state IN ('available','degraded')) = (principal_risks_text IS NOT NULL AND principal_risks_source_date IS NOT NULL AND principal_risks_provenance IS NOT NULL)),
    CHECK ((objective_state='available') = (objective_reason_code IS NULL)),
    CHECK ((strategy_state='available') = (strategy_reason_code IS NULL)),
    CHECK ((concentration_policy_state='available') = (concentration_policy_reason_code IS NULL)),
    CHECK ((principal_risks_state='available') = (principal_risks_reason_code IS NULL))
);

CREATE OR REPLACE FUNCTION sec_regulatory_mandate_field_is_valid(
    field_state text, field_text text, reason_code text, source_date date, provenance jsonb
) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE field_state
        WHEN 'available' THEN NULLIF(btrim(field_text),'') IS NOT NULL
                              AND reason_code IS NULL
                              AND source_date IS NOT NULL AND provenance IS NOT NULL
        WHEN 'degraded' THEN NULLIF(btrim(field_text),'') IS NOT NULL
                             AND NULLIF(btrim(reason_code),'') IS NOT NULL
                             AND source_date IS NOT NULL AND provenance IS NOT NULL
        WHEN 'unavailable' THEN field_text IS NULL AND source_date IS NULL AND provenance IS NULL
                                AND NULLIF(btrim(reason_code),'') IS NOT NULL
        WHEN 'not_applicable' THEN field_text IS NULL AND source_date IS NULL AND provenance IS NULL
                                   AND NULLIF(btrim(reason_code),'') IS NOT NULL
        ELSE false
    END
$$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='sec_mandate_objective_payload_ck'
                   AND conrelid='sec_regulatory_mandate_profiles'::regclass) THEN
        ALTER TABLE sec_regulatory_mandate_profiles ADD CONSTRAINT sec_mandate_objective_payload_ck
        CHECK (sec_regulatory_mandate_field_is_valid(objective_state,objective_text,objective_reason_code,objective_source_date,objective_provenance));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='sec_mandate_strategy_payload_ck'
                   AND conrelid='sec_regulatory_mandate_profiles'::regclass) THEN
        ALTER TABLE sec_regulatory_mandate_profiles ADD CONSTRAINT sec_mandate_strategy_payload_ck
        CHECK (sec_regulatory_mandate_field_is_valid(strategy_state,strategy_text,strategy_reason_code,strategy_source_date,strategy_provenance));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='sec_mandate_concentration_payload_ck'
                   AND conrelid='sec_regulatory_mandate_profiles'::regclass) THEN
        ALTER TABLE sec_regulatory_mandate_profiles ADD CONSTRAINT sec_mandate_concentration_payload_ck
        CHECK (sec_regulatory_mandate_field_is_valid(concentration_policy_state,concentration_policy_text,concentration_policy_reason_code,concentration_policy_source_date,concentration_policy_provenance));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='sec_mandate_principal_risks_payload_ck'
                   AND conrelid='sec_regulatory_mandate_profiles'::regclass) THEN
        ALTER TABLE sec_regulatory_mandate_profiles ADD CONSTRAINT sec_mandate_principal_risks_payload_ck
        CHECK (sec_regulatory_mandate_field_is_valid(principal_risks_state,principal_risks_text,principal_risks_reason_code,principal_risks_source_date,principal_risks_provenance));
    END IF;
END $$;

CREATE OR REPLACE FUNCTION sec_regulatory_mandate_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_state text;
BEGIN
    IF TG_OP<>'INSERT' THEN
        RAISE EXCEPTION 'regulatory mandate profile is immutable';
    END IF;
    SELECT lifecycle_state INTO parent_state
    FROM sec_derived_publications
    WHERE publication_id=NEW.publication_id AND product='regulatory_mandate'
    FOR UPDATE;
    IF parent_state='prepared' THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'mandate profile requires a prepared mandate publication';
END $$;
DROP TRIGGER IF EXISTS sec_regulatory_mandate_guard ON sec_regulatory_mandate_profiles;
CREATE TRIGGER sec_regulatory_mandate_guard BEFORE INSERT OR UPDATE OR DELETE ON sec_regulatory_mandate_profiles
FOR EACH ROW EXECUTE FUNCTION sec_regulatory_mandate_guard();

CREATE OR REPLACE VIEW sec_current_regulatory_mandate_profiles AS
SELECT m.* FROM sec_derived_current_pointers c
JOIN sec_regulatory_mandate_profiles m ON m.publication_id=c.publication_id
WHERE c.product='regulatory_mandate';
