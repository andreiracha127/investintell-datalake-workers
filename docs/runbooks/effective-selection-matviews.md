# Effective-selection matviews — apply, prove, swap

The N-CEN and RR1 "effective" selections are plain views over a self-join of the
raw landing table plus a `dense_rank()` / `count(*) OVER` window with **no date
predicate**. Every read expands the whole history of `ncen_raw_v2_rows` /
`rr1_raw_v2_rows`.

The daily publication chain reads them only for `max(effective_date)`, and it does
it **twice per run** — `discover_source_days` picks the day, `build_watermarks`
records the run row's freshness — so `dl-bond-chain` pays **four full expansions a
day to learn two dates**. `rr1_derived_profiles` pays a fifth on its own cadence.

This change puts a cache in front of those reads. One rule governs it:

> **The view is the authority. The matview is only a cache.**

`src/sec_effective_matviews.py` reads a matview **only** while the source
signature recorded at its last refresh still equals the live signature of the
family's validated-run surface — `(count(*), max(raw_validated_at))` over
`sec_validated_raw_runs` for that family. Both effective views admit a raw row
only through a join to that relation, so a family whose validated-run surface has
not moved cannot have changed the views' content. Anything else — matview absent,
never populated, signature moved, state row missing, refresh forbidden — resolves
back to the view. A missed refresh costs the old scan; it can never produce a
stale watermark.

## What is materialized (and what is not)

| Relation | Cache | Why |
|---|---|---|
| `ncen_effective_filings` | `ncen_effective_filings_mv` — **full mirror** | One row per `(registrant_cik, effective_date)` winner. Small; a full mirror also keeps any future consumer off the raw expansion. |
| `rr1_effective_facts` | `rr1_effective_fact_calendar_mv` — **per-date roll-up** | The view carries the whole typed jsonb projection of every publishable fact. Mirroring it would duplicate the largest surviving landing table on a disk this program is shrinking. The only live read outside the fee builder's own session is `max(effective_date)`; the fee builder shadows the name with a transaction-local table it fills itself. |
| `nport_effective_filings`, `nport_current_holdings`, `nport_identifier_surface` | **none** | No live consumer in `src/` (the fourteen dossier profile products were cut in #65) and `nport_raw_rows` was truncated on 2026-07-24. Materializing them would add storage for nobody. |

## Apply (operator)

The DDL is a **migration**, deliberately not wired into any worker's
`install_schema`: in production the runtime role does not own the raw views —
2026-07-24 logged `ERROR: must be owner of view ncen_effective_filing_candidates`
from a worker-side `CREATE OR REPLACE` (see `src/db.py::_release_advisory_lock`).

```bash
psql "$DATALAKE_DSN" -v ON_ERROR_STOP=1 -f schemas/sec_effective_matviews.sql
```

The matviews are created `WITH NO DATA`, so applying the file is instant and
changes nothing: until the first refresh, `resolve_relation` keeps every caller on
the views. The trailing `DO` block hands ownership to `app_runtime` when that role
exists (`REFRESH` requires ownership).

First population — plain `REFRESH` (`CONCURRENTLY` is impossible on an unpopulated
matview; the module picks the right form automatically):

```bash
python -c "from src import sec_effective_matviews as m; \
           import os,json; print(json.dumps(m.refresh_stale(os.environ['DATALAKE_DSN']), default=str))"
```

## Prove equivalence (run BEFORE trusting the cache)

Every query below must return the value stated. Run them after the first refresh
and **before** the code that reads the cache is deployed.

```sql
-- 1. N-CEN mirror: identical in both directions. Expect (0, 0).
SELECT (SELECT count(*) FROM (SELECT * FROM ncen_effective_filings
        EXCEPT ALL SELECT * FROM ncen_effective_filings_mv) d) AS view_not_in_mv,
       (SELECT count(*) FROM (SELECT * FROM ncen_effective_filings_mv
        EXCEPT ALL SELECT * FROM ncen_effective_filings) d) AS mv_not_in_view;

-- 2. RR1 calendar: per-date counts identical in both directions. Expect (0, 0).
WITH live AS (
    SELECT effective_date,
           count(*)::bigint AS publishable_rows,
           count(DISTINCT accession_number)::bigint AS publishable_accessions
    FROM rr1_effective_facts GROUP BY effective_date
)
SELECT (SELECT count(*) FROM (TABLE live
        EXCEPT ALL SELECT * FROM rr1_effective_fact_calendar_mv) d) AS view_not_in_mv,
       (SELECT count(*) FROM (SELECT * FROM rr1_effective_fact_calendar_mv
        EXCEPT ALL TABLE live) d) AS mv_not_in_view;

-- 3. The reads the chain actually performs. Expect (t, t).
SELECT (SELECT max(effective_date) FROM ncen_effective_filings)
     = (SELECT max(effective_date) FROM ncen_effective_filings_mv) AS ncen_watermark_equal,
       (SELECT max(effective_date) FROM rr1_effective_facts)
     = (SELECT max(effective_date) FROM rr1_effective_fact_calendar_mv) AS rr1_watermark_equal;

-- 4. The unique keys the cache depends on are really unique. Expect (0, 0).
SELECT (SELECT count(*) FROM (SELECT raw_row_id FROM ncen_effective_filings
        GROUP BY 1 HAVING count(*) > 1) d) AS ncen_dup_raw_row_id,
       (SELECT count(*) FROM (SELECT effective_date FROM rr1_effective_fact_calendar_mv
        GROUP BY 1 HAVING count(*) > 1) d) AS rr1_dup_effective_date;

-- 5. Recorded state (evidence to keep with the swap).
SELECT * FROM sec_effective_matview_state ORDER BY matview;
```

Cost evidence, before and after (keep both plans):

```sql
EXPLAIN (ANALYZE, BUFFERS) SELECT max(effective_date) FROM ncen_effective_filings;
EXPLAIN (ANALYZE, BUFFERS) SELECT max(effective_date) FROM ncen_effective_filings_mv;
EXPLAIN (ANALYZE, BUFFERS) SELECT max(effective_date) FROM rr1_effective_facts;
EXPLAIN (ANALYZE, BUFFERS) SELECT max(effective_date) FROM rr1_effective_fact_calendar_mv;
```

## Swap

1. Apply the migration. Nothing changes: matviews are unpopulated, callers stay on
   the views.
2. Refresh once (`refresh_stale`). Still nothing changes for deployed code.
3. Run the equivalence block above. **Any non-zero difference stops the swap** —
   drop the matviews and the state table; no deployed code depends on them.
4. Deploy the code. `daily_publication_chain` and `rr1_derived_profiles` now route
   their `max(effective_date)` reads through `resolve_relation`, and the chain
   calls `refresh_stale` before discovering the source day. `matview_refresh`
   keeps the caches current on days the chain does not run.
5. Watch one chain run: the result payload carries `effective_matviews` with one
   entry per matview (`fresh` / `refreshed` / `absent` / `refresh_failed`).

## Rollback

Fully reversible at any point, with no code change:

```sql
DROP MATERIALIZED VIEW IF EXISTS ncen_effective_filings_mv;
DROP MATERIALIZED VIEW IF EXISTS rr1_effective_fact_calendar_mv;
DROP TABLE IF EXISTS sec_effective_matview_state;
```

`resolve_relation` then returns the views again and the system is back to the
pre-change cost. Reverting the code commit alone also works — it just leaves two
unused relations behind.

## Operating notes

- **A refresh is not free.** It is one full expansion of the selection. It runs
  only when the family's validated-run signature moved (i.e. an N-CEN/RR1 package
  was ingested and validated), which is a quarterly-ish event, not a daily one.
- **`force=True`** re-refreshes regardless of the signature. Use it after any
  manual mutation of the raw landing, and as the periodic re-proof: re-run the
  equivalence block after a forced refresh.
- **The fee builder is unchanged.** `rr1_derived_profiles._materialize_effective_cache`
  still fills its own transaction-local `rr1_effective_facts`; the calendar matview
  does not and cannot replace it (it has no fact rows). Making the fee build and
  its closure guard cheap when cold is a separate change.
