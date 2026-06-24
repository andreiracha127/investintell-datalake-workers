-- N-PORT fund-level flow tables.
--
-- Source: DERA quarterly N-PORT bulk datasets, FUND_REPORTED_INFO.tsv joined
-- to SUBMISSION.tsv by ACCESSION_NUMBER.

CREATE TABLE IF NOT EXISTS sec_nport_fund_reported_info (
    accession_number text PRIMARY KEY,
    series_id text,
    series_name text,
    series_lei text,
    report_date date NOT NULL,
    filing_date date,
    total_assets numeric(20,2),
    total_liabilities numeric(20,2),
    net_assets numeric(20,2),
    sales_flow_mon1 numeric(20,2),
    reinvestment_flow_mon1 numeric(20,2),
    redemption_flow_mon1 numeric(20,2),
    sales_flow_mon2 numeric(20,2),
    reinvestment_flow_mon2 numeric(20,2),
    redemption_flow_mon2 numeric(20,2),
    sales_flow_mon3 numeric(20,2),
    reinvestment_flow_mon3 numeric(20,2),
    redemption_flow_mon3 numeric(20,2),
    source_quarter text NOT NULL,
    source_file text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sec_nport_fund_reported_info_series_idx
    ON sec_nport_fund_reported_info (series_id, report_date DESC);

CREATE TABLE IF NOT EXISTS sec_nport_fund_monthly_flows (
    accession_number text NOT NULL,
    series_id text NOT NULL,
    series_name text,
    report_date date NOT NULL,
    filing_date date,
    flow_month_end date NOT NULL,
    month_ordinal smallint NOT NULL CHECK (month_ordinal BETWEEN 1 AND 3),
    total_assets numeric(20,2),
    net_assets numeric(20,2),
    sales_flow numeric(20,2),
    reinvestment_flow numeric(20,2),
    redemption_flow numeric(20,2),
    gross_subscription_flow numeric(20,2),
    gross_redemption_flow numeric(20,2),
    net_flow numeric(20,2),
    net_flow_pct_assets numeric(18,10),
    source_quarter text NOT NULL,
    source_file text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT sec_nport_fund_monthly_flows_pk
        PRIMARY KEY (accession_number, month_ordinal)
);

CREATE INDEX IF NOT EXISTS sec_nport_fund_monthly_flows_series_idx
    ON sec_nport_fund_monthly_flows (series_id, flow_month_end DESC);

CREATE INDEX IF NOT EXISTS sec_nport_fund_monthly_flows_report_idx
    ON sec_nport_fund_monthly_flows (report_date DESC, filing_date DESC);
