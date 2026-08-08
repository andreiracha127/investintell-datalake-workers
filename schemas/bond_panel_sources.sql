-- Static, immutable FF17 issuer-sector attributes for the bond panel.
-- This is intentionally NOT the serving layer's issuer_sector taxonomy.
CREATE TABLE IF NOT EXISTS bond_issuer_sector (
    cusip9 text PRIMARY KEY CHECK (cusip9 ~ '^[0-9A-Z]{9}$'),
    ff17num smallint NOT NULL CHECK (ff17num BETWEEN 1 AND 17),
    source text NOT NULL CHECK (source IN ('osbap', 'sic_map')),
    disagreement_count integer NOT NULL DEFAULT 0 CHECK (disagreement_count >= 0),
    source_provenance jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    CHECK (jsonb_typeof(source_provenance) = 'object' AND source_provenance <> '{}'::jsonb),
    CHECK (
        (source = 'osbap' AND source_provenance ? 'artifact_sha256')
        OR (source = 'sic_map' AND source_provenance ? 'sic_code' AND disagreement_count = 0)
    )
);

CREATE OR REPLACE FUNCTION bond_issuer_sector_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'bond_issuer_sector is immutable';
END $$;

DROP TRIGGER IF EXISTS bond_issuer_sector_immutable ON bond_issuer_sector;
CREATE TRIGGER bond_issuer_sector_immutable
BEFORE UPDATE OR DELETE ON bond_issuer_sector
FOR EACH ROW EXECUTE FUNCTION bond_issuer_sector_immutable();

COMMENT ON TABLE bond_issuer_sector IS
    'Static canonical Fama-French 17 issuer sector. Source is OSBAP panel or exact-CUSIP9 SEC SIC map; absence means no sector.';

-- Ownership, not a grant, is required for worker-managed relations.  The block
-- is safe under the production postgres installer and under worker_writer; it
-- fails closed if either the target role or authority is absent.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worker_writer') THEN
        RAISE EXCEPTION 'worker_writer role is required for bond_issuer_sector ownership';
    END IF;
    IF current_user <> 'postgres'
       AND current_user <> 'worker_writer'
       AND NOT pg_has_role(current_user, 'worker_writer', 'MEMBER') THEN
        RAISE EXCEPTION 'bond_issuer_sector must be installed by postgres or worker_writer';
    END IF;
    ALTER TABLE bond_issuer_sector OWNER TO worker_writer;
    ALTER FUNCTION bond_issuer_sector_immutable() OWNER TO worker_writer;
END $$;

REVOKE ALL ON bond_issuer_sector FROM PUBLIC;
