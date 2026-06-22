-- schemas/stock_daily_returns.sql
-- Worker-owned base table: daily simple returns per stock ticker.
-- TimescaleDB forbids window functions (lag) in continuous aggregates, so the
-- per-day return cannot be a cagg over eod_prices; this is materialized by the
-- stock_daily_returns worker (mirrors nav_timeseries.return_1d for funds).
CREATE TABLE IF NOT EXISTS stock_daily_returns (
    ticker      text             NOT NULL,
    date        date             NOT NULL,
    return_1d   double precision,
    adj_close   double precision,
    PRIMARY KEY (ticker, date)
);

-- Cross-sectional ("all tickers on date X") + per-ticker scans both benefit.
CREATE INDEX IF NOT EXISTS stock_daily_returns_date_idx
    ON stock_daily_returns (date);
