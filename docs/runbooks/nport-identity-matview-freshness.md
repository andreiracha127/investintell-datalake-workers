# Runbook — freshness of `nport_holdings_snapshot_identity_v1`

## What this relation is

A MATERIALIZED VIEW in the datalake (`market`) over
`sec_nport_holdings_v2 JOIN sec_nport_instrument_class_bridge` restricted to
`resolution_state = 'resolved'` and to the bridge's validity window:

```sql
SELECT DISTINCT h.publication_id, b.series_id, h.source_series_id,
       h.report_date, h.accession_number, h.filing_date
FROM sec_nport_holdings_v2 h
JOIN sec_nport_instrument_class_bridge b
  ON b.publication_id=h.publication_id AND b.accession_number=h.accession_number
 AND b.holding_id=h.holding_id
WHERE b.resolution_state='resolved'
  AND h.report_date >= b.valid_from AND (b.valid_to IS NULL OR h.report_date <= b.valid_to);
```

It is owned by `postgres` and is **not created or refreshed by this repository**.
The app reads it through `sec_current_nport_holdings_snapshot_identity_v1`
(pointer-joined) in `fixed_income_analytics.py`, `fixed_income_holdings.py` and
`nport_class_aware.py`, and takes `MAX(report_date)` per series as the **anchor**
of the fixed-income dossier.

## The two failure modes

| | symptom | already detected? |
|---|---|---|
| pointer advances past the matview | no rows for the pinned publication → empty anchor → dossier degrades | yes, fail-closed at the app |
| matview behind **within** the pinned publication | anchor resolves an OLDER `report_date` than the same publication would serve | **no** — stale, but labelled with a current publication id |

`sec_nport_holdings_v2_write_guard` admits INSERTs only while the parent
publication is `prepared` and forbids UPDATE/DELETE, so a publication is frozen
once validated: a matview refreshed **after** validation is complete for that
publication forever. The residual window is a refresh that landed *during* the
prepared window and was never repeated.

## The assertion

`schemas/nport_holdings_snapshot_identity_freshness.sql` installs two functions:

```sql
SELECT nport_holdings_snapshot_identity_freshness();     -- structured verdict, never raises
SELECT nport_holdings_snapshot_identity_assert_fresh();  -- raises on stale / behind_pointer
```

Verdict states and how each one lands:

| state | meaning | `assert_fresh()` | the job |
|---|---|---|---|
| `fresh` | the matview carries the newest `report_date` of the pinned publication, and the newest `filing_date` at that date | returns | green |
| `stale` | proven divergence — the matview is behind | raises (`data_exception`) | red, `IdentityMatviewStale` |
| `behind_pointer` | the matview holds no rows for the pinned publication | raises (`data_exception`) | red, `IdentityMatviewStale` |
| `unreadable` | the matview is absent, or exists but was never refreshed | raises (`undefined_table`) | red, `IdentityMatviewUnreadable` |
| `unavailable` | no pointer, no holdings surface, or a pinned publication with no holdings — undecidable **upstream** | returns | green, with a WARNING |

`unreadable` is hard on purpose: this runbook tells you to call the assertion
right after a `REFRESH`, and a silent pass from a matview that does not exist
would tell you the opposite of the truth. `unavailable` stays soft because a
datalake between publications is not a broken read path — but it is never
reported as `fresh` either.

### Who installs the functions

**Install them once, as the role that owns them, and keep that role.**
`CREATE OR REPLACE FUNCTION` requires ownership: if an operator applies the file
as `postgres` and the worker later runs as `worker_writer`, the worker's install
fails with *must be owner of function*. The worker tolerates exactly that case —
it logs a WARNING and uses the already-installed functions — but only when they
are already there; if they are absent the privilege error is re-raised, because
then there is nothing to probe with. `probe(conn, install=False)` skips the
install entirely for a caller that knows the functions are managed elsewhere.

Whoever owns them must **re-apply `schemas/nport_holdings_snapshot_identity_freshness.sql`
when it changes** — the worker cannot do it for them.

Cost: four index-backed `max()` reads — **24 ms cold, 3 ms warm** measured on the
mirror against the current 625 508-row publication. `max(filing_date)` is taken
*at* the maximum `report_date`; unscoped it matches no index and cost 1.5 s of the
5.6 s an earlier draft spent per call. The exact, bridge-filtered maximum (~15 s)
is paid **only** when the cheap comparison already looks wrong, because the cheap
source-side max ignores the bridge filter and would otherwise cry wolf on a
publication whose newest filing is still resolving.

Verified end to end on the mirror on 2026-08-02:

```
$ python -m src.run nport_holdings_identity_freshness
{"worker": "nport_holdings_identity_freshness", "rows": 0, "state": "fresh",
 "reason": "matview_carries_the_publication_maxima",
 "publication_id": "4594cd24-ad7c-4f45-9d56-367963a18e37", "matview_rows": 13598,
 "matview_max_report_date": "2026-05-31", "source_max_report_date": "2026-05-31",
 "matview_max_filing_date": "2026-06-30", "source_max_filing_date": "2026-06-30",
 "confirmed_against_bridge": false}
```

## Where to run it

**Right after the out-of-band refresh.** Whatever runs

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY nport_holdings_snapshot_identity_v1;
```

should run `SELECT nport_holdings_snapshot_identity_assert_fresh();` in the same
session. The matview carries the UNIQUE index `CONCURRENTLY` needs
(`nport_holdings_snapshot_identity_v1_pk` on
`(publication_id, series_id, report_date, accession_number)`).

**As a job**, for the days nobody refreshes anything:

```
python -m src.run nport_holdings_identity_freshness
```

It installs the functions, prints the verdict as JSON, and **exits non-zero** on
`stale` / `behind_pointer` with a WARNING that names the publication and both
maxima. Point it at the datalake DSN; it writes nothing but the two functions.

## When it goes red

1. Read the verdict: `matview_max_report_date` vs `source_max_report_date` /
   `bridge_resolved_max_report_date` tell you how far behind it is, and
   `matview_rows` gives the matview's own cardinality for the pinned publication.
2. On `unreadable`, a refresh is not the fix — the relation is missing or was
   created `WITH NO DATA`. Re-create it from the definition above (as its owner),
   then refresh.
3. On `stale` / `behind_pointer`, refresh the matview (`CONCURRENTLY` is safe —
   unique index present).
4. Re-run the assertion. It must return `fresh`.
5. If it stays behind after a refresh, the pinned publication is still gaining
   rows — i.e. it is not actually validated — and the pointer is the problem, not
   the matview.

### What `fresh` does and does not prove

It proves the matview carries the newest `report_date` of the pinned publication
and the newest `filing_date` at that date — the coordinate the app's anchor is
computed from. It does **not** prove completeness. `sec_nport_holdings_v2` is
generated offline (`src/nport/local_v2_generator.py` writes
`sec_nport_holdings_v2.tsv.gz`) and loaded in accession order, not report_date
order, so a capture taken part-way through a load is a *prefix of accessions* that
can already contain the global maxima while missing much of the publication — and
this assertion would call it `fresh`. There is no cheap cardinality check to close
that gap: `sec_derived_publications` records no row count, and the current
publication holds 4 507 500 rows. `matview_rows` is reported as evidence for
anyone comparing two runs, not as proof.

## Open proposal (not implemented)

Move the refresh in-band, next to the datalake list in
`src/workers/matview_refresh.py` (`_DATALAKE_MVS`), and assert immediately after.
That removes the failure mode instead of only detecting it. It was **not** done
here because the refresh cost was never measured against production: answering a
single `max()` over the matview's defining join takes ~15 s on the mirror, so a
full `REFRESH CONCURRENTLY` is plausibly minutes and would land inside the daily
locked run. Measure it first.
