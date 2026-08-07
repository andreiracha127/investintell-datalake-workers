-- Read model for the institutional-reveal API.  This deliberately remains
-- separate from fund_top_holdings_mv, whose serving semantics are unchanged.
CREATE MATERIALIZED VIEW IF NOT EXISTS fund_reveal_holdings_mv AS
WITH latest_reports AS (
    -- Use the last fully published top-holdings snapshot as the report-date
    -- boundary instead of racing a multi-file load into the raw hypertable.
    -- Only the date boundary is reused; reveal weights/ranks are recomputed
    -- below from all raw rows with their own exact semantics.
    SELECT series_id, max(report_date) AS report_date
    FROM fund_top_holdings_mv
    GROUP BY series_id
), normalized_holdings AS (
    SELECT
        h.series_id,
        h.report_date,
        upper(btrim(h.cusip)) AS cusip,
        h.pct_of_nav
    FROM sec_nport_holdings h
    JOIN latest_reports latest
      ON latest.series_id = h.series_id
     AND latest.report_date = h.report_date
    WHERE nullif(btrim(h.cusip), '') IS NOT NULL
      -- Fallback N-PORT identity keys are never 13F CUSIPs. Exclude them
      -- before aggregation so an unknown synthetic weight cannot quarantine
      -- otherwise joinable holdings for the same series.
      AND upper(btrim(h.cusip)) !~ '^(IS:|LE:|H:|CIK:)'
), aggregated_holdings AS (
    SELECT
        series_id,
        report_date,
        cusip,
        -- SUM preserves an all-NULL pct_of_nav group as NULL: unknown is not zero.
        sum(pct_of_nav) / 100.0 AS weight,
        count(*) AS source_row_count,
        count(pct_of_nav) AS nonnull_weight_count,
        count(*) - count(pct_of_nav) AS null_weight_count
    FROM normalized_holdings
    GROUP BY series_id, report_date, cusip
), ranked_holdings AS (
    SELECT
        series_id,
        report_date,
        cusip,
        weight,
        source_row_count,
        nonnull_weight_count,
        null_weight_count,
        -- Calculated before top-100 truncation so an unknown group at rank 101+
        -- still quarantines the whole series. CUSIP-less and synthetic-key
        -- rows are excluded because they cannot participate in the N-PORT x
        -- 13F join.
        bool_or(weight IS NULL OR null_weight_count > 0) OVER (
            PARTITION BY series_id, report_date
        ) AS has_unknown_weight,
        row_number() OVER (
            PARTITION BY series_id, report_date
            ORDER BY weight DESC NULLS LAST, cusip ASC
        ) AS rank
    FROM aggregated_holdings
)
SELECT
    series_id,
    report_date,
    rank,
    cusip,
    weight,
    source_row_count,
    nonnull_weight_count,
    null_weight_count,
    has_unknown_weight
FROM ranked_holdings
WHERE rank <= 100
WITH NO DATA;

-- The unique key enables REFRESH ... CONCURRENTLY after the plain bootstrap
-- refresh populates this WITH NO DATA materialized view for the first time.
CREATE UNIQUE INDEX IF NOT EXISTS fund_reveal_holdings_mv_series_report_rank_uidx
    ON fund_reveal_holdings_mv (series_id, report_date, rank);
