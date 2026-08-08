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

-- One immutable, source-governed monthly liquidity observation per CUSIP/month.
-- The OSBAP/TRACE parquet is accepted only by the one-time backfill CLI; runtime
-- workers and serving paths read this relation, never an operator-local artifact.
CREATE TABLE IF NOT EXISTS bond_liquidity_monthly (
    cusip9 text NOT NULL CHECK (cusip9 ~ '^[0-9A-Z]{9}$'),
    month date NOT NULL CHECK (month = date_trunc('month', month)::date),
    rel_bid_ask_bps numeric,
    quoted_days integer NOT NULL CHECK (quoted_days >= 0),
    dollar_volume numeric,
    quote_state text NOT NULL CHECK (quote_state IN ('quoted', 'unquoted')),
    reason_code text NOT NULL CHECK (reason_code IN (
        'valid_quote_valid_dollar_volume',
        'valid_quote_missing_dollar_volume',
        'valid_quote_invalid_dollar_volume',
        'valid_quote_invalid_quoted_days_valid_dollar_volume',
        'valid_quote_invalid_quoted_days_missing_dollar_volume',
        'valid_quote_invalid_quoted_days_invalid_dollar_volume',
        'missing_rel_bid_ask_bps',
        'missing_rel_bid_ask_bps_missing_dollar_volume',
        'missing_rel_bid_ask_bps_invalid_dollar_volume',
        'crossed_rel_bid_ask_bps',
        'crossed_rel_bid_ask_bps_missing_dollar_volume',
        'crossed_rel_bid_ask_bps_invalid_dollar_volume',
        'invalid_rel_bid_ask_bps',
        'invalid_rel_bid_ask_bps_missing_dollar_volume',
        'invalid_rel_bid_ask_bps_invalid_dollar_volume'
    )),
    source text NOT NULL CHECK (source = 'osbap_trace_historical'),
    source_provenance jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cusip9, month, source),
    CHECK (jsonb_typeof(source_provenance) = 'object' AND source_provenance <> '{}'::jsonb),
    CHECK (source_provenance ? 'artifact_sha256' AND source_provenance ? 'source_columns' AND source_provenance ? 'row_identity'),
    CHECK ((quote_state = 'quoted' AND rel_bid_ask_bps IS NOT NULL) OR (quote_state = 'unquoted' AND rel_bid_ask_bps IS NULL AND quoted_days = 0)),
    CHECK (rel_bid_ask_bps IS NULL OR rel_bid_ask_bps >= 0),
    CHECK (dollar_volume IS NULL OR dollar_volume >= 0)
);

CREATE OR REPLACE FUNCTION bond_liquidity_monthly_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'bond_liquidity_monthly is immutable';
END $$;

DROP TRIGGER IF EXISTS bond_liquidity_monthly_immutable ON bond_liquidity_monthly;
CREATE TRIGGER bond_liquidity_monthly_immutable
BEFORE UPDATE OR DELETE ON bond_liquidity_monthly
FOR EACH ROW EXECUTE FUNCTION bond_liquidity_monthly_immutable();

COMMENT ON TABLE bond_liquidity_monthly IS
    'Immutable historical OSBAP/TRACE monthly liquidity evidence; unavailable values remain NULL with typed reason codes.';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worker_writer') THEN
        RAISE EXCEPTION 'worker_writer role is required for bond_liquidity_monthly ownership';
    END IF;
    IF current_user <> 'postgres'
       AND current_user <> 'worker_writer'
       AND NOT pg_has_role(current_user, 'worker_writer', 'MEMBER') THEN
        RAISE EXCEPTION 'bond_liquidity_monthly must be installed by postgres or worker_writer';
    END IF;
    ALTER TABLE bond_liquidity_monthly OWNER TO worker_writer;
    ALTER FUNCTION bond_liquidity_monthly_immutable() OWNER TO worker_writer;
END $$;

REVOKE ALL ON bond_liquidity_monthly FROM PUBLIC;
