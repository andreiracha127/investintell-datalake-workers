# Timescale audit refresh workers

These workers are staged code. This change does not create Railway services,
cron schedules, deploys, database objects, or production backfills.

## Activation order

1. Apply and verify the Light DDL in a maintenance window.
2. Backfill both 13F CAGGs in bounded, quarter-aligned windows and compare them
   with the existing MVs before changing any reader.
3. Create the dedicated `stock-fundamentals-statements` Railway service with
   `railway.stock-fundamentals-statements.toml`, `WORKER=stock_fundamentals_statements`,
   and the datalake `DATABASE_URL`. The committed config is deliberately parked
   at `0 0 29 2 *`, uses `restartPolicyType = "never"`, and contains no
   healthcheck. A deploy therefore only stages the image; it is not evidence of
   a database run.
4. With the service still parked, manually restart it once to bootstrap the
   empty companion table. A first incremental run sees no fact or universe
   watermarks and is equivalent to the bounded initial build. Do not add a
   recurring schedule or redirect any reader yet.
5. Compare the result with `stock_fundamentals_statements_mv`; only then choose
   and document the incremental cadence. The legacy `SOURCE_MV` remains the
   semantic definition and the app reader remains on that MV until a separate
   reader-cutover review.
6. Create one-shot Railway services with `restartPolicy=NEVER` only after their
   upstream publication service is identified:
   - `sec_13f_publication_chain`
   - `nport_v2_publication_chain`
   - `analytics_refresh_chain`
   - `matview_refresh`
7. Remove overlapping legacy schedules only after database watermarks and read
   models prove the new chain completed. Parking a Railway cron requires the
   impossible schedule `0 0 29 2 *`; an empty or null schedule is not disabled.

## Verification queries

Check incremental fundamentals activity, independent universe state, and
quarantine before reader cutover:

```sql
SELECT *
FROM stock_fundamentals_statement_runs
ORDER BY completed_at DESC
LIMIT 20;

SELECT ticker, cik, processed_at
FROM stock_fundamentals_statement_universe_watermarks
ORDER BY processed_at DESC
LIMIT 20;

SELECT reason_code, count(*), min(observed_at), max(observed_at)
FROM stock_fundamentals_statement_fact_quarantine
GROUP BY reason_code;
```

The initial run is eligible for parity review only when every query below returns
zero rows. Run it before changing a reader; it compares the whole relation, not
only a sample.

```sql
SELECT count(*) AS incremental_only
FROM (
  SELECT * FROM stock_fundamentals_statements_incremental
  EXCEPT ALL
  SELECT * FROM stock_fundamentals_statements_mv
) AS diff;

SELECT count(*) AS legacy_only
FROM (
  SELECT * FROM stock_fundamentals_statements_mv
  EXCEPT ALL
  SELECT * FROM stock_fundamentals_statements_incremental
) AS diff;
```

If either count is nonzero, keep the service parked and preserve
`stock_fundamentals_statements_mv` as the only reader. If the manual run fails
after its materialized-data commit but before its state commit, restart it while
the service is still parked: the lock remains held through both commits and the
replayed delta is idempotent. Never advance the schedule based on a green build
or container-start status alone.

Check 13F CAGG freshness beside the legacy MVs:

```sql
SELECT view_name, materialization_hypertable_name
FROM timescaledb_information.continuous_aggregates
WHERE view_name IN (
  'institution_13f_totals_history_cagg',
  'institution_13f_sector_history_cagg'
);
```

Every chain returns ordered stage evidence. A result with `aborted`, `skipped`,
or `published=false` is not a successful publication even if Railway reports
that its container started.
