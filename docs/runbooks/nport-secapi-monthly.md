# N-PORT from sec-api.io monthly bulk: manual load and the monthly lane

`sec_nport_holdings` had full coverage only through report_date 2026-04-30 on
2026-10-06 (2026-05-31 held 3 series). Light's `fund-classification` job (180-day
report-age rule) has refused to publish since 2026-08-27. The DERA quarterly
packages arrive too late to fix that, so the table is now fed from sec-api.io's
monthly `form-nport` bulk dataset through the existing loader.

| piece | what it does |
|---|---|
| `tools/nport_secapi/download.py` | fetches monthly containers with the `sec_api` SDK; every requested month must be listed; re-fetches when the remote size or `updatedAt` changes |
| `tools/nport_secapi/convert.py` | containers to one loader CSV per report_date plus `manifest.json` |
| `tools/nport_secapi/validate.py` | offline value sanity: pct_of_nav sums, ISIN fill, USD totals |
| `tools/nport_dera/nport_parallel_load.py` | the only write path; now with `--dry-run` and `--new-series-only` |
| `src/workers/nport_secapi_monthly.py` | the monthly lane (no cron configured) |

## What the sec-api data is, and what it is not

* Containers are partitioned by **filing month**. N-PORT turns public about 60
  days after the period, so report_date R lands mostly in container R+2, with
  late filers in R+3. 2026-07-31 sits almost entirely in `2026-09`.
* The converter reproduces what production already holds. Over the
  2026-04/05/06 containers, per series `n_holdings`, `total_market_value`,
  `coverage_pct` and `n_synthetic` in `cagg_nport_series_profile` match for every
  series shared with the 2026-08-06 load (2,483 of 2026-02-28, 6,819 of
  2026-03-31, 4,051 of 2026-04-30).
* One filing per `(report_date, series_id)`: the newest NPORT-P or NPORT-P/A
  with holdings wins, and the whole series comes from that filing. The CSVs have
  no conflict-key duplicates.
* Key policy `dera` (the default) keys holdings the way every existing row was
  keyed. `strict` stops `999999999` and `N/A` from folding distinct holdings
  together. It keeps ~12% more rows on a quarter-end, but it lowers the ISIN fill
  below the 0.90 floor that the loader's verify and `nport_identifier_coverage`
  gate on. Leave it off until that floor is re-based. See `convert.py`.

## The overdue load (2026-10)

Seed: `E:\tmp-deploy\nport-q3-seed\` (one CSV per report_date, `manifest.json`),
built from `E:\tmp-deploy\sec-cache\nport-secapi\form-nport\2026\2026-06..10`.

```
python -m tools.nport_secapi.download --from 2026-06 --to 2026-10 \
    --out E:\tmp-deploy\sec-cache\nport-secapi --dotenv E:\investintell-light\backend\.env
python -m tools.nport_secapi.convert --out E:\tmp-deploy\nport-q3-seed \
    --min-report-date 2026-05-01 --partial-months 2026-10 <the five containers>
python -m tools.nport_secapi.validate E:\tmp-deploy\nport-q3-seed
```

`convert` refuses a `--out` that already holds CSVs or a `manifest.json`. To
replace them, pass `--overwrite`.

Load **one report_date per run**. `--only` matters: without it, every run COPYs
every CSV in the directory, even though the INSERT is scoped. Run the
`--dry-run` first. It opens no connection. `$DSN` is the read-write datalake DSN
(`market`, through `centerbeam.proxy.rlwy.net:36616` from outside Railway). Run
from this branch's worktree with an interpreter that has `psycopg` (`py -3.13` on
the operator box). Start with `2026-05-29`: at 58k rows it pays the full chunk
decompress/recompress, so it calibrates the timing before the 2M-row date.

```
for d in 2026-05-29 2026-05-31 2026-06-30 2026-07-31; do
  python -m tools.nport_dera.nport_parallel_load --seed-dir E:\tmp-deploy\nport-q3-seed \
      --only $d.csv --only-report-dates $d --dry-run
  python -m tools.nport_dera.nport_parallel_load --seed-dir E:\tmp-deploy\nport-q3-seed \
      --dsn "$DSN" --workers 4 --skip-matview --only $d.csv --only-report-dates $d
done
```

```sql
CALL refresh_continuous_aggregate('cagg_nport_series_profile', '2026-05-01', '2026-11-01');
```

The cagg is `materialized_only`. Until it is refreshed, the new dates are
invisible to it and to `nport_lookthrough`'s coverage copy. Its policy (job 1078,
every 6 h, `start_offset` NULL) would catch up on its own. The `CALL` makes it
immediate. Run it outside a transaction block.

Do not load `2026-08-31` yet. Its main month (`2026-10`) is still filling, and it
holds 1 series. The lane picks it up later.

* `2026-05-31` already holds 3 series (109 rows). The seed carries the same
  filings: identical `n_holdings`, market value and `coverage_pct` for all three.
  The plain load is therefore equivalent to `--new-series-only`.
* `2026-05-29` and `2026-05-31` both fall in compressed chunk
  `_hyper_13_6982_chunk` (2026-03-08..2026-06-06, ~3.2M rows). `prep()`
  decompresses it and `finalize()` recompresses it, once per run. To pay that
  once, load the two together: `--only 2026-05-29,2026-05-31 --only-report-dates
  2026-05-29,2026-05-31`. Each CSV is still its own transaction, which is what the
  one-date rule protects. `2026-06-30` and `2026-07-31` land in a chunk that does
  not exist yet.
* `--skip-matview` is required: `mv_nport_sector_attribution` does not exist in
  production. Without the flag, `finalize()` raises after the rows are committed
  and before `add_compression_policy`, which leaves the table with no
  compression policy. The matviews that read the table (`fund_top_holdings_mv`,
  `fund_style_drift_mv`, `fund_reveal_holdings_mv`) are refreshed by
  `matview_refresh`. `nport_holdings_snapshot_identity_v1` is refreshed out of
  band by its owner.
* The loader prints a rollback handle: `DELETE FROM sec_nport_holdings WHERE
  created_at = '<ts>'`.

Checks after the load:

```sql
SELECT report_date, count(*) rows, count(DISTINCT series_id) series,
       round(avg((isin IS NOT NULL AND isin <> '')::int), 4) isin_fill
FROM sec_nport_holdings WHERE report_date >= '2026-05-01' GROUP BY 1 ORDER BY 1;
-- expect rows/series exactly as in manifest.json (2026-05-31: the 3 existing series are in the seed)

SELECT report_day, count(*) FROM cagg_nport_series_profile
WHERE report_day >= '2026-05-01' GROUP BY 1 ORDER BY 1;   -- same series counts

SELECT max(report_date) FROM sec_nport_holdings;           -- 2026-07-31
```

Then run `nport_identifier_coverage` (inside `nport_lookthrough`) and check that
the new dates read `clean`. After that, `fund-classification` in Light can be
re-run.

## Re-running and idempotency

* The insert is `ON CONFLICT (report_date, series_id, cusip) DO NOTHING` against
  the primary key. Loading a date twice inserts 0 rows and creates no
  duplicates. It also **repairs nothing**: a changed row keeps its old values.
* A re-load after an amendment would add the amendment's new keys next to the
  original filing's rows. `--new-series-only` prevents that. It inserts a
  `(report_date, series_id)` only if the table has none of it, and it requires
  each series to sit in exactly one CSV, which the converter guarantees.
* `--delete-first --only-report-dates D` is the delete-then-reload repair.
* Each CSV is one transaction. If the process dies between `prep()` and
  `finalize()`, the compression policy stays removed. Re-add it with
  `add_compression_policy('sec_nport_holdings', INTERVAL '3 months')`.

## The monthly lane

`WORKER=nport_secapi_monthly`, `railway.nport-secapi-monthly.toml`. As of month
M, a run:

1. downloads containers M-3..M;
2. converts them, report_dates from M-5 to M-3;
3. for each date with series the table lacks, runs the loader `--dry-run`, then
   `--new-series-only`, one date per invocation, with the loader's ISIN verify;
4. refreshes `cagg_nport_series_profile` over the loaded dates, never across a date whose load failed.

Each report_date is revisited by three consecutive runs while its late filers
arrive. Series already loaded are never touched. Cost: with `compress_after 3
months`, the third revisit can find the date's chunk compressed again. A single
late series then means a full chunk decompress and recompress. When nothing new
arrived, the pre-check skips the date and the chunk is not touched. Dates older than M-5 are left
to an operator. The proposed schedule is `0 9 3 * *`. It is **not enabled**:
creating the service and its cron is an operator step, after the manual load
above has been verified. The lane builds with nixpacks from the whole repository,
because the fleet Dockerfile does not COPY `tools/`. It needs `SEC_API_IO_KEY`.
Without `NPORT_SECAPI_CACHE_DIR` it fetches ~0.4-0.5 GB per run.
