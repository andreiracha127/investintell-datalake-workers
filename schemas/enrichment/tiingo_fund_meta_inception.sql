-- ============================================================================
-- PROPOSAL — APPLY MANUALLY, DO NOT WIRE INTO ANY WORKER OR MIGRATION RUNNER.
-- ============================================================================
-- Back-fill fund inception dates in the SEC catalog from Tiingo startDate.
--
-- The tiingo_fund_meta worker persists Tiingo's per-ticker startDate (the first
-- EOD bar Tiingo has for a listing). This script proposes using it to fill
-- ONLY the still-NULL inception_date on the two SEC catalog tables:
--     sec_registered_funds.inception_date
--     sec_etfs.inception_date
--
-- IMPORTANT SCOPE / SAFETY NOTES
--   * NULL-ONLY. Every UPDATE is guarded by `WHERE inception_date IS NULL`, so
--     an existing SEC-sourced value is never overwritten. The legacy allocation
--     repo's decision that fund *attributes* come from SEC sources stands —
--     this only *fills gaps* with a descriptive-provider startDate.
--   * SERIES-LEVEL. A fund series has many share classes (many tickers). We take
--     MIN(start_date) across every class ticker that maps to the series as the
--     series inception (earliest listing wins).
--   * The join goes THROUGH THE CLASS TICKER: sec_fund_classes.ticker carries
--     (cik, series_id); tiingo_fund_meta.ticker is stored upper-cased, so the
--     join uses upper(sec_fund_classes.ticker).
--   * Only ok rows with a real start_date participate (source_status='ok').
--   * VERIFY the target columns exist before running:
--         ALTER TABLE sec_registered_funds ADD COLUMN IF NOT EXISTS inception_date date;
--         ALTER TABLE sec_etfs             ADD COLUMN IF NOT EXISTS inception_date date;
--     (Left commented on purpose — confirm the intended column type first.)
--
-- Run inside an explicit transaction so you can review the preview counts and
-- ROLLBACK if the coverage looks wrong:
--     psql "$DATABASE_URL"        # then paste this file, or \i it, then COMMIT.
-- ============================================================================

BEGIN;

-- Series-level inception = earliest Tiingo startDate over all class tickers of
-- the series. Materialized once into a TEMP table so both UPDATEs (and the
-- optional previews) share it — a plain WITH clause would only scope to a single
-- statement. The temp table is dropped automatically at COMMIT/ROLLBACK.
--   sec_fund_classes (ticker, cik, series_id)  ->  tiingo_fund_meta (start_date)
CREATE TEMP TABLE series_inception ON COMMIT DROP AS
    SELECT
        fc.series_id,
        MIN(m.start_date) AS inception_date
    FROM sec_fund_classes fc
    JOIN tiingo_fund_meta m
      ON upper(fc.ticker) = m.ticker
    WHERE fc.series_id  IS NOT NULL
      AND fc.ticker     IS NOT NULL
      AND m.source_status = 'ok'
      AND m.start_date  IS NOT NULL
    GROUP BY fc.series_id;

-- ---- PREVIEW (read-only): how many series would resolve, and coverage. -------
-- Uncomment to inspect before the UPDATEs:
-- SELECT count(*) AS series_with_tiingo_inception FROM series_inception;
-- SELECT count(*) AS registered_funds_fillable
--   FROM sec_registered_funds rf
--   JOIN series_inception si ON si.series_id = rf.series_id
--  WHERE rf.inception_date IS NULL;
-- SELECT count(*) AS etfs_fillable
--   FROM sec_etfs e
--   JOIN series_inception si ON si.series_id = e.series_id
--  WHERE e.inception_date IS NULL;

-- ---- UPDATE 1: registered funds (N-CEN slice), NULL-only. --------------------
UPDATE sec_registered_funds rf
   SET inception_date = si.inception_date
  FROM series_inception si
 WHERE si.series_id = rf.series_id
   AND rf.inception_date IS NULL;

-- ---- UPDATE 2: ETFs, NULL-only, same series-level inception. -----------------
UPDATE sec_etfs e
   SET inception_date = si.inception_date
  FROM series_inception si
 WHERE si.series_id = e.series_id
   AND e.inception_date IS NULL;

-- Review the row counts reported by the two UPDATEs, then finish explicitly:
--   COMMIT;     -- to keep the back-fill
--   ROLLBACK;   -- to discard it
-- Left uncommitted on purpose so a paste never mutates the catalog by accident.
