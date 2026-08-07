# `open_macro_v03_chain` — activation runbook

The producer of `open_macro_v03_decision_chain`. Until 2026-08 the only writer of that
table was the manual script `scripts/rebuild_decision_chain_v3.py` (owner order,
2026-07-17): a whole-table DELETE + INSERT with `CHAIN_END` pinned to `2026-06-30`.
This worker replaces the **pinned end**, not the script — history revision stays a
manual, owner-ordered operation.

Merging this branch does **not** create or run the service. Railway's `watchPatterns`
is empty for this project: publishing worker code is `serviceInstanceDeploy` with an
explicit `commitSha`, and running it is `deploymentRestart`.

---

## 1. What the worker does, in one paragraph

Every run it replays the certified `macro_quadrant_us_v3` series from `2014-03-01` to
the target month by calling `harness.phase0q.decision_v3.run_decision_series_v3` — the
same function the rebuild script calls — over the certified input pack `_003` **union**
the live vintages/sessions published after the pack's window. It then publishes the
**one** last month, if and only if (a) that month has closed, (b) every input arm has
printed past the month-end decision cutoff, and (c) the replayed prefix still carries
the same `quadrant` / `status` / `transition_pending` as every month already in the
table. Anything else is a no-op with the reason named, or a hard failure with nothing
written.

## 2. Create the service (house recipe: create bare → vars → cron → connect repo)

1. **Create the service with NO source.** In the `investintell-workers` project, add a
   service and leave the repo unconnected. Connecting first triggers a build against a
   service with no `WORKER`, which starts the fleet default and fails noisily.
2. **Set the variables** (service scope):

   | var | value |
   |---|---|
   | `WORKER` | `open_macro_v03_chain` |
   | `DATABASE_URL` | the datalake `market` DSN (same value the other workers carry; password URL-encoded) |
   | `DB_TLS_CA_PEM` / `DB_TLS_CERT_PEM` / `DB_TLS_KEY_PEM` | copy from a sibling worker service — the mTLS client pair (`CN=worker_writer`) |

   No worker-specific variables exist. `WORKER_LIMIT` and `WORKER_CALC_DATE` are
   deliberately unsupported: `run()` takes neither, and `src/run_worker.py` exits
   non-zero if either is set, rather than sweeping a different scope than the config
   claims.
3. **Set the cron: `45 7 * * *`.** Justification, in the day's order:
   `macro-ingestion` 05:00 → `eod-prices-warmer` 06:15 → **`open_macro_v03_chain`
   07:45** → `open-macro-v03-worker` 08:30 → `open-macro-v04-worker` 08:45. The chain
   must run **before** the v04 so the v04 of the same day reads the new month instead
   of carrying the previous one forward (`quadrant_source=chain_carry` →
   `decision_validity="carried"`, `open_macro_v04.py:623-666`). It must run **after**
   the two ingestions because they are its inputs. 07:45 leaves ~1h30 of slack if the
   macro ingestion runs late and ~45 min of headroom before the v03 job.
   **Never** set `cronSchedule` to `null` (Railway reads that as *run continuously*)
   nor to `""` (it poisons the whole staged patch). To disable, use an impossible date:
   `0 0 29 2 *`.
   Give the service ≥ 1 GB: the replay holds the pack's ~20 MB of vintages plus the
   PIT index in memory and takes ~2 minutes of CPU on a publishing run.
4. **Connect the repo** (branch `main`) and deploy the merge commit explicitly:
   `serviceInstanceDeploy` with the `commitSha`. A green Railway status means the
   container was built and started — nothing more.
5. **Run it**: `deploymentRestart`. A redeploy of a cron service does **not** execute
   the job.

## 3. What the first run will do

On any date before MICH's first print after the target month-end, the first run is a
**no-op that names the pending arms** and exits 0. That is the designed steady state,
not a failure. Expected log:

```
open_macro_v03_chain: arm MICH has not published past 2026-07-31T00:00:00+00:00 (last: 2026-06-26T00:00:00+00:00)
open_macro_v03_chain: 2026-07-31 inputs not settled (N arm(s) pending); nothing to do
```

## 4. Verify a run — ALWAYS in the table, never on the dashboard

```sql
-- 1. did a new month land, and who wrote it?
SELECT as_of, quadrant, candidate_quadrant, status, candidate_confidence,
       growth_score, inflation_score, coverage_quality, transition_pending,
       code_commit, loaded_at
FROM open_macro_v03_decision_chain
ORDER BY as_of DESC
LIMIT 3;

-- 2. the certified history must be UNTOUCHED: 148 rows carry the rebuild's commit,
--    and every worker-published month carries a different one.
SELECT code_commit, count(*), min(as_of), max(as_of), max(loaded_at)
FROM open_macro_v03_decision_chain
GROUP BY code_commit
ORDER BY min(as_of);
-- expected after the first publishing run:
--   9aadeaae02a533751407b43d89426852850c166b | 148 | 2014-03-31 | 2026-06-30
--   <the deployed commit>                    |   1 | 2026-07-31 | 2026-07-31

-- 3. the series must stay a contiguous month-end chain with no duplicates
SELECT count(*) AS rows,
       count(DISTINCT as_of) AS distinct_months,
       min(as_of), max(as_of),
       count(*) FILTER (WHERE as_of <> (date_trunc('month', as_of)
                                        + interval '1 month - 1 day')::date) AS not_month_end
FROM open_macro_v03_decision_chain;

-- 4. why a run published nothing: which arm has not printed past the cutoff?
SELECT series_id, max(available_at) AS last_print,
       max(available_at) > (SELECT (max(as_of) + 1)::timestamptz
                            FROM open_macro_v03_decision_chain) AS settled_for_next_month
FROM macro_observation_vintage
WHERE series_id IN ('ACOGNO','AHETPI','CPILFESL','INDPRO','MICH','PAYEMS','PCEC96','PPIFIS')
GROUP BY series_id
ORDER BY 2;
```

The run's own stats (printed as JSON by `src/run_worker.py`) carry `readiness.arms`,
the prefix-gate report and the published row.

## 5. Failure modes and what they mean

| symptom | meaning | action |
|---|---|---|
| `inputs_not_settled`, exit 0 | at least one arm has not printed past the cutoff | none — wait. Around the last Friday of the following month for MICH. |
| `month_in_progress`, exit 0 | the target month has not closed | none |
| `skipped: advisory_lock_held`, exit 0 | a concurrent run is replaying | none |
| `prefix gate: ... CONSUMABLE projection ...`, non-zero | the extended replay changes a published month's quadrant/status/latch | **stop.** Nothing was written. The new month does not sit on the certified chain; the owner decides whether the chain gets re-certified. |
| `certified pack input ... sha256 ... != pinned` | the image's certified prefix is not the certified prefix | stop; do not re-pin to make it pass |
| `... carries unknown columns [...]` | someone altered the chain table | stop; the table is the contract `open_macro_v04` and the Light read |
| `is EMPTY` | pointed at a database where the chain was never materialized | this worker extends a chain, it does not create one |

## 6. Known, measured, and deliberately not "fixed"

The fused v3 model's auxiliary market sensor standardizes against the month-end grid
the replay walks, and that grid is built once over the union of every decision date's
48-month walk-back. Extending the series by one month therefore adds 46 grid dates
*before* the old end and moves earlier months slightly.

Measured 2026-08-07 (pack-only, end `2026-07-31` vs `2026-06-30`): 45 of 592 numeric
cells move, at most 4.7e-3 and always on `candidate_confidence`; exactly one
categorical cell moves (`2023-11-30` `candidate_quadrant` contraction → recovery);
**nothing any consumer reads moves.** `open_macro_v04` selects
`(as_of, quadrant, status, candidate_confidence)`; the Light backend's three call sites
select `(as_of, quadrant, candidate_confidence)` filtered on `status='valid'`; nobody
reads `candidate_quadrant`.

This is a property of the certified construction — the same shift happens whenever the
owner reruns the rebuild at a later end date — so the worker does not change it. It
never writes an existing month, it hard-fails if the consumable projection of a
published month changes, and it reports the rest in its stats. Re-certifying the chain
against a later end is an owner decision, not a worker behaviour.
