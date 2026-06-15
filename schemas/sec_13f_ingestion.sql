-- sec_13f_ingestion worker — SEC 13F holdings and period diffs.
-- Apply against the cloud with: psql "$DATABASE_URL" -f schemas/sec_13f_ingestion.sql

CREATE TABLE IF NOT EXISTS curated_institutions (
    cik             text        PRIMARY KEY,
    manager_name    text        NOT NULL,
    source          text        NOT NULL DEFAULT 'manual',
    is_active       boolean     NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sec_managers (
    cik             text        PRIMARY KEY,
    manager_name    text        NOT NULL,
    aum_total       numeric,
    source          text        NOT NULL DEFAULT 'sec_13f_seed',
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sec_13f_holdings (
    cik               text        NOT NULL,
    manager_name      text        NOT NULL,
    period            date        NOT NULL,
    report_date       date,
    cusip             text        NOT NULL,
    name              text,
    value_usd         numeric,
    shares            numeric,
    accession_number  text        NOT NULL,
    form_type         text,
    source_url        text,
    fetched_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT ux_sec_13f_holdings
        UNIQUE (cik, period, accession_number, cusip)
);

CREATE INDEX IF NOT EXISTS sec_13f_holdings_cusip_period_idx
    ON sec_13f_holdings USING btree (cusip, period DESC);

CREATE INDEX IF NOT EXISTS sec_13f_holdings_cik_period_idx
    ON sec_13f_holdings USING btree (cik, period DESC);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'sec_13f_holdings', 'period',
            chunk_time_interval => INTERVAL '3 months',
            if_not_exists => TRUE,
            migrate_data => TRUE
        );
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS sec_13f_diffs (
    cik                text        NOT NULL,
    manager_name       text        NOT NULL,
    cusip              text        NOT NULL,
    name               text,
    period             date        NOT NULL,
    previous_period    date,
    value_usd          numeric,
    previous_value_usd numeric,
    value_change_usd   numeric,
    shares             numeric,
    previous_shares    numeric,
    shares_change      numeric,
    updated_at         timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT ux_sec_13f_diffs
        UNIQUE (cik, cusip, period)
);

CREATE INDEX IF NOT EXISTS sec_13f_diffs_cusip_period_idx
    ON sec_13f_diffs USING btree (cusip, period DESC);
