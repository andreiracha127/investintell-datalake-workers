-- schemas/fund_institutional_reveal.sql
-- A3 — artefato JSONB do institutional-reveal por série (cruzamento N-PORT×13F +
-- rede). schema_version permite bump quando o shape muda. Upsert por
-- (series_id, as_of). Apply: psql "$DATABASE_URL" -f schemas/fund_institutional_reveal.sql
CREATE TABLE IF NOT EXISTS fund_institutional_reveal_artifacts (
    series_id        text    NOT NULL,
    as_of            date    NOT NULL,
    schema_version   int     NOT NULL DEFAULT 1,
    payload          jsonb   NOT NULL,
    organization_id  uuid,
    computed_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ux_fund_inst_reveal_pk
        UNIQUE NULLS NOT DISTINCT (series_id, as_of, organization_id)
);

CREATE INDEX IF NOT EXISTS fund_inst_reveal_series_idx
    ON fund_institutional_reveal_artifacts (series_id, as_of DESC);
