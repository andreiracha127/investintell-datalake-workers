-- Indexed read model for the institutional-reveal worker.  The source
-- hypertable is compressed by report date, so probing it once per fund causes
-- every probe to revisit all compressed chunks.  Keeping only the latest rows
-- for each normalized CUSIP is sufficient to preserve the worker's semantics:
-- for any selected CUSIP set, max(per-CUSIP max(report_date)) is the same date
-- as max(report_date) over all raw matches, and only rows on that date survive.
CREATE MATERIALIZED VIEW IF NOT EXISTS fund_reveal_13f_holdings_mv AS
WITH latest_by_cusip AS (
    SELECT
        upper(cusip) AS cusip,
        max(report_date) AS report_date
    FROM sec_13f_holdings
    WHERE cusip IS NOT NULL
    GROUP BY upper(cusip)
)
SELECT
    h.cik,
    h.report_date,
    h.cusip AS source_cusip,
    latest.cusip,
    h.issuer_name AS name,
    h.market_value AS value_usd,
    h.shares
FROM sec_13f_holdings h
JOIN latest_by_cusip latest
  ON latest.cusip = upper(h.cusip)
 AND latest.report_date = h.report_date
WITH NO DATA;

-- Production sec_13f_holdings is keyed by (report_date, cik, cusip).  Keeping
-- the source spelling in the identity avoids collapsing case/whitespace
-- variants while the normalized CUSIP remains the lookup key.
CREATE UNIQUE INDEX IF NOT EXISTS fund_reveal_13f_holdings_mv_identity_uidx
    ON fund_reveal_13f_holdings_mv (report_date, cik, source_cusip);

CREATE INDEX IF NOT EXISTS fund_reveal_13f_holdings_mv_cusip_report_idx
    ON fund_reveal_13f_holdings_mv (cusip, report_date DESC)
    INCLUDE (cik, name, value_usd, shares);
