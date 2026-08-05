# N-PORT identifier coverage — what the warning means and how to repair it

`src/workers/nport_identifier_coverage.probe` runs at the end of
`nport_lookthrough` (Railway `nport-lookthrough`, cron `0 4 * * 0`). It reads the
last 150 days of `sec_nport_holdings` and reports, per `report_date`, how much of
it still carries an ISIN.

## The failure it watches for

`sec_nport_holdings` is loaded one DERA quarterly package at a time by
`tools/nport_dera/nport_bulk_parse.py` → `tools/nport_dera/nport_parallel_load.py`,
run by hand. Until 2026-08-05 those two scripts lived only on the operator's disk,
untracked and untested; that is the reason two bad quarters went unnoticed for one
and two years, and it is why they are now in this repository with a fixture-backed
suite (`tests/test_nport_dera_tooling.py`). The parse builds a `HOLDING_ID → ISIN`
map from `IDENTIFIERS.tsv` and uses it twice: to fill the `isin` column, and to
choose the synthetic key when a holding has no CUSIP — `IS:<isin>` first,
`LE:<lei>` only as a fallback.

If that map comes out incomplete, the load still succeeds and looks normal.
Every row arrives. `series_id`, `cik`, `sector`, `quantity`, `currency`,
`pct_of_nav`, `fair_value_level` are all populated exactly as usual. The only
differences are that `isin` is NULL and the synthetic key degraded from
`IS:<isin>` to `LE:<lei>` — and because `LE:` keys collide across every holding
of the same issuer inside a series, part of the package is additionally eaten by
`ON CONFLICT (report_date, series_id, cusip) DO NOTHING`.

Two packages have already landed this way (`2023q4` and `2025q1`), unnoticed for
one and two years respectively. The symptom surfaced only downstream, as funds
dropping out of an empirical peer universe.

## Reading the verdict

```
state                    clean | degraded | undecidable
degraded_report_dates    the report_dates below the floor
worst_isin_fill          lowest judged reading in the window
checked[]                per report_date: rows, isin_fill, identifiable_share
```

* **clean** — every judged `report_date` is at or above `floor` (0.90).
* **degraded** — a load lost its ISIN side. Act.
* **undecidable** — the window held no `report_date` with `min_rows` (1 000)
  rows. This is not a pass; it means the probe had nothing to judge.

`identifiable_share` (CUSIP-9 **or** ISIN **or** `IS:` key) is reported but not
gated: over 2019-09-30..2026-01-31 its worst clean reading is 0.9657 and its best
degraded one 0.9613, so no floor fits between them. `isin_fill` separates the
same two populations by 32.5 pp (worst clean 0.9445, best degraded 0.6198).

## Repair

1. **Identify the package, not the report_date.** The loss is scoped to the DERA
   package, so the same `report_date` can be half broken and half fine. Map the
   degraded `report_date`s to the quarter whose filings carry them (N-PORT is
   filed within 60 days of the period, so `report_date` 2024-12-31 lands in
   `2025q1`), and confirm by checking whether the clean part of that
   `report_date` belongs to the neighbouring package.

2. **Re-parse from the local package, scoped to the report_dates.** No SEC or
   `sec-api.io` request is needed: the DERA packages are on disk
   (`E:\Edgard\nport\<q>_nport`) and the parser reads them correctly today — the
   defect was in the artifact produced at load time, not in the parser as it
   stands. `--only-report-dates` keeps the CSV inside the repair's charter (a
   quarterly package also carries its neighbours' report_dates), and the parse
   refuses to hand over a CSV whose own ISIN fill is below the floor:

   ```
   python -m tools.nport_dera.nport_bulk_parse "E:\Edgard\nport\2023q4_nport" \
       -o out\2023q4.csv --only-report-dates 2023-09-29,2023-09-30,2023-10-31
   ```

   Run it once per package that contributes rows to the affected dates, not just
   the guilty one: the clean neighbours own real rows on the same `report_date`s
   and the `DELETE` in step 3 takes those out too.

3. **Delete before loading.** A plain re-run repairs nothing: the loader is
   `ON CONFLICT (report_date, series_id, cusip) DO NOTHING` and the bad rows
   already own the key. `--delete-first` does it, and refuses to run without an
   explicit `--only-report-dates` (which also bounds the INSERT, independently of
   the parser). `sec_nport_holdings` is a compressed hypertable — `prep()`
   decompresses only the chunks the target dates fall in and `finalize()`
   re-compresses exactly those and restores the policy:

   ```
   python -m tools.nport_dera.nport_parallel_load --seed-dir out --dsn "$DSN" \
       --workers 4 --skip-matview --delete-first \
       --only-report-dates 2023-09-29,2023-09-30,2023-10-31
   ```

   The loader verifies the ISIN fill of each target `report_date` after the load
   and exits non-zero below the floor. Do one `report_date` at a time on the
   production datalake: a single transaction over all eight holds `backend_xmin`
   long enough to stall the global `VACUUM`.

4. **Re-run the probe** (`nport_lookthrough`, or `probe()` against the datalake
   directly) and confirm the readings return to the 0.98–0.99 band.

## Deliberately not a gate

The probe does not raise, and `nport_lookthrough` does not fail on a degraded
verdict. The damage is to history already written; stopping the weekly run would
cost a week of look-through exposures without repairing a single row. The verdict
rides in the worker's stats so it is legible in the run log, and the WARNING
carries the report_dates and the reason a re-run alone is a no-op.
