-- form345_ingestion worker — SEC Form 3/4/5 structured data.
-- Apply against the cloud with: psql "$DATABASE_URL" -f schemas/form345_ingestion.sql

CREATE TABLE IF NOT EXISTS sec_insider_transactions (
    accession_number       text        NOT NULL,
    trans_sk               text        NOT NULL,
    issuer_cik             text        NOT NULL,
    issuer_name            text,
    reporting_owner_cik    text,
    reporting_owner_name   text,
    transaction_date       date        NOT NULL,
    transaction_code       text,
    shares                 numeric,
    value_usd              numeric,
    buy_sell               text        NOT NULL,
    source_period          text,
    fetched_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT ux_sec_insider_transactions
        UNIQUE (accession_number, trans_sk),
    CONSTRAINT ck_sec_insider_transactions_buy_sell
        CHECK (buy_sell IN ('buy', 'sell', 'other'))
);

CREATE INDEX IF NOT EXISTS sec_insider_transactions_issuer_date_idx
    ON sec_insider_transactions USING btree (issuer_cik, transaction_date DESC);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'sec_insider_transactions', 'transaction_date',
            chunk_time_interval => INTERVAL '3 months',
            if_not_exists => TRUE,
            migrate_data => TRUE
        );
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS sec_insider_sentiment (
    cik          text        NOT NULL,
    quarter      date        NOT NULL,
    buy_value    numeric     NOT NULL DEFAULT 0,
    sell_value   numeric     NOT NULL DEFAULT 0,
    buy_count    integer     NOT NULL DEFAULT 0,
    sell_count   integer     NOT NULL DEFAULT 0,
    net_value    numeric     NOT NULL DEFAULT 0,
    updated_at   timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT ux_sec_insider_sentiment
        UNIQUE (cik, quarter)
);

CREATE INDEX IF NOT EXISTS sec_insider_sentiment_cik_quarter_idx
    ON sec_insider_sentiment USING btree (cik, quarter DESC);
