# Fund reveal read models

`fund_reveal_holdings_mv` is the N-PORT read boundary for
`fund_institutional_reveal`. It is additive and does not replace or alter
`fund_top_holdings_mv`. `fund_reveal_13f_holdings_mv` is the indexed 13F read
boundary; it replaces thousands of probes across every compressed 13F chunk
with B-tree lookups.

## Bootstrap order

Keep the reveal consumer on its previous deployment until every step below has
passed.

1. Apply `schemas/fund_reveal_holdings_mv.sql` and
   `schemas/fund_reveal_13f_holdings_mv.sql` with `ON_ERROR_STOP=1`.
2. Verify both views exist and are unpopulated. Verify the N-PORT unique key
   `(series_id, report_date, rank)` and the 13F identity key
   `(report_date, cik, source_cusip)`.
3. Run one plain `REFRESH MATERIALIZED VIEW fund_reveal_holdings_mv` with an
   operator timeout of 30 minutes. The first refresh cannot use `CONCURRENTLY`.
4. Run one plain `REFRESH MATERIALIZED VIEW fund_reveal_13f_holdings_mv` with a
   bounded operator timeout. This scans the 13F hypertable a fixed number of
   times; it must not run once per fund.
5. Verify `relispopulated`, key uniqueness, N-PORT ranks 1..100, report-date
   parity, NULL-quality counts, and raw-vs-MV 13F query parity on representative
   CUSIP sets.
6. Only then deploy or execute `fund_institutional_reveal`. Later refreshes use
   `CONCURRENTLY` in the same order through `matview_refresh`.

The latest report boundary comes from the last completed
`fund_top_holdings_mv` snapshot. Reveal weights and ranks are still recomputed
from all raw holdings. This prevents the read model from observing a raw
multi-file load before the existing holdings snapshot has published it.
The DDL, bootstrap, refresh worker, and reveal consumer must all use the same
primary database DSN; the view intentionally has no cross-database dependency.

The 13F view retains every row at each upper-cased CUSIP's own latest report
date. For any fund CUSIP set, the maximum of those per-CUSIP dates is identical
to the maximum date in the original raw match; filtering rows to that date
therefore preserves the old result bag. Manager names remain a live lookup from
`sec_managers`, so that dimension does not wait for an MV refresh.
Upper-casing intentionally matches the legacy worker exactly; surrounding
whitespace is not trimmed because doing so could advance a fund to a period it
did not previously match.

## Rollback

1. Redeploy the previous reveal worker so no consumer requires either new view.
2. Remove both reveal views from the refresh registry and deploy that change.
3. After both readbacks, drop `fund_reveal_13f_holdings_mv`, then
   `fund_reveal_holdings_mv`. They contain only derived, reproducible data.

`CREATE MATERIALIZED VIEW IF NOT EXISTS` is only an initial-install guard. A
future definition change must use a versioned replacement migration and must
not rely on reapplying the bootstrap file.
