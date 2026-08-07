# Timescale audit refresh workers

These workers are staged code. This change does not create Railway services,
cron schedules, deploys, database objects, or production backfills.

## Activation order

1. Apply and verify the Light DDL in a maintenance window.
2. Backfill both 13F CAGGs in bounded, quarter-aligned windows and compare them
   with the existing MVs before changing any reader.
3. Run `stock_fundamentals_statements` once with `rebuild=True` outside the
   scheduler, compare row counts and sampled rows with
   `stock_fundamentals_statements_mv`, then schedule only incremental mode.
4. Create one-shot Railway services with `restartPolicy=NEVER` only after their
   upstream publication service is identified:
   - `sec_13f_publication_chain`
   - `nport_v2_publication_chain`
   - `analytics_refresh_chain`
   - `matview_refresh`
5. Remove overlapping legacy schedules only after database watermarks and read
   models prove the new chain completed. Parking a Railway cron requires the
   impossible schedule `0 0 29 2 *`; an empty or null schedule is not disabled.

## Verification queries

Check incremental fundamentals activity and quarantine before reader cutover:

```sql
SELECT *
FROM stock_fundamentals_statement_runs
ORDER BY completed_at DESC
LIMIT 20;

SELECT reason_code, count(*), min(observed_at), max(observed_at)
FROM stock_fundamentals_statement_fact_quarantine
GROUP BY reason_code;
```

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
