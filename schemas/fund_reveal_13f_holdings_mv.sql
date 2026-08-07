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
    coalesce((to_jsonb(h) ->> 'period')::date, h.report_date) AS source_period,
    h.accession_number,
    h.cusip AS source_cusip,
    latest.cusip,
    -- The versioned schema uses name/value_usd while the live legacy table
    -- still exposes issuer_name/market_value. Reading through the row JSON
    -- keeps one bootstrap definition compatible with both shapes.
    coalesce(to_jsonb(h) ->> 'name', to_jsonb(h) ->> 'issuer_name') AS name,
    coalesce(
        (to_jsonb(h) ->> 'value_usd')::numeric,
        (to_jsonb(h) ->> 'market_value')::numeric
    ) AS value_usd,
    h.shares
FROM sec_13f_holdings h
JOIN latest_by_cusip latest
  ON latest.cusip = upper(h.cusip)
 AND latest.report_date = h.report_date
WITH NO DATA;

-- Preserve the source filing identity for concurrent refreshes.  Keeping the
-- source spelling also avoids collapsing case/whitespace variants while the
-- normalized CUSIP remains the lookup key.
CREATE UNIQUE INDEX IF NOT EXISTS fund_reveal_13f_holdings_mv_identity_uidx
    ON fund_reveal_13f_holdings_mv (
        report_date, cik, source_period, accession_number, source_cusip
    );

CREATE INDEX IF NOT EXISTS fund_reveal_13f_holdings_mv_cusip_report_idx
    ON fund_reveal_13f_holdings_mv (cusip, report_date DESC)
    INCLUDE (cik, name, value_usd, shares);
