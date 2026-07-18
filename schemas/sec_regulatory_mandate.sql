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

CREATE OR REPLACE FUNCTION sec_regulatory_mandate_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' AND sec_derived_publication_is_validated(NEW.publication_id, 'regulatory_mandate') THEN
        RETURN NEW;
    END IF;
    IF TG_OP='INSERT' THEN RAISE EXCEPTION 'mandate profile requires a validated mandate publication'; END IF;
    RAISE EXCEPTION 'regulatory mandate profile is immutable';
END $$;
DROP TRIGGER IF EXISTS sec_regulatory_mandate_guard ON sec_regulatory_mandate_profiles;
CREATE TRIGGER sec_regulatory_mandate_guard BEFORE INSERT OR UPDATE OR DELETE ON sec_regulatory_mandate_profiles
FOR EACH ROW EXECUTE FUNCTION sec_regulatory_mandate_guard();

CREATE OR REPLACE VIEW sec_current_regulatory_mandate_profiles AS
SELECT m.* FROM sec_derived_current_pointers c
JOIN sec_regulatory_mandate_profiles m ON m.publication_id=c.publication_id
WHERE c.product='regulatory_mandate';
