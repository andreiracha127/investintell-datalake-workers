# Fund-classification inputs: freshness, retries, and last-good publication

This code has not changed production services, schedules, grants, data, or
deployments. Apply/configure steps below require the owner's deployment
authorization. This workers PR owns the governed cagg-request SQL migration.
The companion Light PR owns the classification publication guard, displayed
as-of, and incident root-cause note.

## Evidence and source boundaries

On 2026-10-06 the legacy `sec_nport_holdings` load, six-hour
`cagg_nport_series_profile` policy, and monthly equity-characteristics run were
out of order. A validated manual load eventually reached 2026-07-31, then the
owner-run cagg policy caught up, then a roughly 50-minute characteristics run
completed. The old look-through selected its parents from the cagg, committed
one parent at a time, and merely warned about identifier loss. Characteristics
caught layer exceptions as `partial`; the dispatcher painted those partial runs
green. These are separate legacy sources from V2 artifact-only look-through.

`nport_ingestion` ingests local raw/landing packages. It is not the recurring
writer of `sec_nport_holdings`; an empty package scope now fails explicitly.
The recurring legacy loader is workers PR
[#153](https://github.com/andreiracha127/investintell-datalake-workers/pull/153),
still **DO NOT MERGE**. This branch does not modify it.

## Explicit policy

| Stage | Evidence and threshold | Failure behavior |
| --- | --- | --- |
| N-PORT | Newest reporting month with at least 1,000 distinct series; anchor age at most 120 UTC calendar days. Distinct series across its three report months retain at least 90% of the preceding three-month cohort. | Blocks the fund chain; reports actual raw max separately. New series cannot compensate for missing previous series. |
| Cagg | At least 90% of the anchored source series have the exact same latest report day and holding count. | Requests the existing owner-run policy, then polls six times with two seconds between polls. An unfinished refresh is `blocked`, never proof of freshness. |
| Equity characteristics | At least 90% of the mapped, positive-value EC/EP source series have, for every instrument mapped to the series, a row at the exact source report date with `computed_at >=` that series' latest source `created_at`. One share class cannot cover a sibling. | Fund output is validated before commit. An unchanged fully matched cohort skips the heavy chain rebuild. |
| Identifier coverage | Existing 90% ISIN fill floor, judged per report date with at least 1,000 holdings over 150 days through the anchor. | `degraded` or `undecidable` blocks candidates before writes. |
| Look-through | At least 90% of the same source series match report date/count/source load time and have exposures for that same report. Computation age is at most seven days. Expanded child reports are at most 180 UTC calendar days old. | Older child holdings are left as an explicit unexpanded-fund residual. Shards write candidates; only a checked complete batch replaces serving output atomically. |
| Classification health | Nonempty `status='completed'` run that started at or after the most recent 08:00 UTC classifier slot (five-minute start grace; the slot counts once its one-hour completion window has elapsed, so the 09:00 check requires today's run) and completed after the latest raw-tail load; run as-of within 120 days and at least the broad anchor and actual raw max. Source and all derived cohort checks pass, and each derived global watermark reaches the actual loaded raw max. | Independent read-only monitor exits 1 even if the classifier did not execute or did not publish. A recent replay of an older as-of cannot renew health. |

The three-month union is necessary: the observed staggered May/June/July cohorts
were approximately 2,505 / 7,026 / 4,194 series. Comparing July alone with June
would reject valid quarterly filers. A lone August row cannot advance the broad
July anchor. Missing/empty baselines and empty eligible characteristics cohorts
are alarms. The previous source baseline comes from the small cagg; the raw
scan is limited to the recent tail needed for the oldest admissible quarter.
No freshness scan runs on a user-facing API route.

Upstream jobs may repair the last meaningful cohort while a sparse newer raw
report is being validated. Light publication and classification health remain
conservative about that raw max and its load time: the singleton cannot renew
the classifier's as-of or completion proof. Quarantine partial new raw loads
until the loader verifies their alignment, or complete the loader's repair.

The standalone monthly `characteristics` job retains its independent company
Layer 1 refresh because stock screeners consume it. Layer 1 can advance even
when N-PORT blocks the fund layer, and that partial fund-chain outcome is red.
For a dependency-chain build, company and fund candidates share the validated
transaction. Original calculations/history windows are preserved. A new source
can still require the measured heavy history rebuild; schedule it off peak and
watch shared DB IO. Daily retries validate unchanged output rather than
recomputing it. Look-through additionally rebuilds at least every seven days,
preserving the old weekly repair cadence for sector/security/ISIN maps and exact
sidecars whose changes have no reliable source watermark. For an authorized
immediate mapping repair, call `nport_lookthrough.run(..., force_rebuild=True)`
or set `NPORT_LOOKTHROUGH_FORCE_REBUILD=1` for one `src.run_worker` invocation,
then unset it; leaving it set forces every retry to do heavy work.

## Cagg privilege and completion

Timescale's refresh procedure commits internally, so calling it inside a
`SECURITY DEFINER` function is not a valid fix. The governed migration
[nport_series_profile_refresh_request_v1.sql](../../schemas/nport_series_profile_refresh_request_v1.sql)
installs `public.request_nport_series_profile_refresh() RETURNS integer`.
`worker_writer` can request an earlier `next_start` for the existing
postgres-owned policy (production job 1078); it cannot choose a relation, widen
the refresh range, change offsets/ownership, or run arbitrary privileged SQL.
The function validates the enabled owner/policy configuration, returns the job
ID, and throttles repeated requests for 15 minutes. Only the existing owner job
does the refresh. Apply the migration as the approved owner **before** deploying
these callers; do not grant broad refresh ownership to runtime.

The chain uses an autocommit connection, so the schedule request is committed
before polling. It measures cagg/source alignment after the request; returning
1078 is acceptance only. A long refresh ends this attempt as
`cagg_refresh_pending`, preserves serving output, and resumes on the next retry.
No queue, new privileged job, or synchronous refresh is introduced.

## Apply/deploy order for an authorized operator

1. Apply the cagg-request SQL migration and review its companion rollback file
   as **postgres**, the policy owner; inspect the enabled policy and function
   grants. Read the companion Light runbook and apply its governed classification
   migrations using its approved Alembic URL and classification-table owner.
   Apply `schemas/nport_lookthrough.sql` as **worker_writer**, which owns the
   existing serving tables, if the runtime's existing CREATE permission will
   not install its two new candidate tables. Creating candidates as postgres
   would require unnecessary additional grants. Keep existing owners unchanged.
2. Deploy this workers PR together with the companion Light guard/as-of PR
   under the repository's deployment process. A merge of workers main can kill
   running git-connected cron jobs; coordinate an idle window.
3. Add the dedicated input-chain service with
   `railway.nport-classification-inputs-chain.toml`,
   `WORKER=nport_classification_inputs_chain`, and the existing DB credential.
   Proposed retries are 04:00, 05:00, 06:00, and 07:00 UTC. Retain the standalone
   monthly characteristics service for independent company consumers. Replace
   the old standalone look-through cron with this ordered caller once checked.
4. Add the independent health service with
   `railway.fund-pipeline-health.toml`, `WORKER=fund_pipeline_health`, and the
   existing DB credential. Proposed cron is 09:00 UTC, after Light's 08:00 run.
   If Light's classifier cron moves, update `CLASSIFIER_START_UTC` in
   `src/workers/fund_pipeline_health.py` and this cron together. Configure the
   platform's failed-job alert destination; nonzero exits and JSON event lines
   are the alarm signal, not an external notification sent by these workers.
5. Verify read-only: broad source anchor/retention; exact cagg matches; exact
   characteristics matches; complete look-through summary/exposures; fresh
   nonempty completed classification. Inspect the JSON `fund_pipeline_alarm`
   event and its stage/cohort evidence for every failed or pending attempt.

The shared look-through/Light-reader lock remains **900204** intentionally;
Light's transaction reader guard prevents classification during publication.
The outer chain has **900366**, distinct from its children; the cagg request
uses **900367** (900363-900365 belong to the monthly N-PORT loader). Look-through
promotion also takes the loader's **900365** as a non-blocking transaction lock
held through its commit: a running load blocks publication
(`SOURCE_LOAD_IN_PROGRESS`), and a load starting meanwhile waits for the commit.
The look-through never waits for 900365, so its order 900366 -> 900204 -> 900365
cannot deadlock with the loader's 900363 -> 900365 -> 900364. A partial
`WORKER_LIMIT` cannot publish a full fund cohort.
A replay cutoff does not replace the UTC clock used to judge live freshness.

## PR #153 integration still required

At the read-only snapshot of 2026-10-07 03:46 UTC, PR #153 head `3c3afab`
remained open, **DO NOT MERGE**, and its checks were unstable. The loader now
validates the actual inserted rows before commit and rolls rejected rows back;
the other agent also addressed the converter/value review findings. The
remaining integration is the direct owner-only `CALL` in `refresh_cagg`, the
approved request/completion contract below, and the downstream trigger/retry.
Another agent owns that branch. Recheck its head, review threads, CI, and the
owner's release authorization before merging; this snapshot is not approval.

The central dispatcher in this PR already enforces the shared source gate after
future `nport_secapi_monthly` **successful or noop** runs. It reads in a separate
read-only transaction and turns stale/insufficient source evidence into a
structured blocked exit. This protects the lane when the two branches are
integrated without modifying PR #153 here. After its own loader/value/transaction
review is complete, integrate the same contract at its release boundary. Use
the fixed request function and commit the request before any poll; never
restore the direct `CALL`. A successful load can invoke this input-chain module
or rely on its scheduled retries. Do not run heavy downstream stages when the
loader failed or the broad source gate did not pass. Late/new series at an
unchanged max date are detected by per-series holding counts and source load
timestamps. Loader regression coverage must include a no-op against stale
inputs and a rejected load that cannot later be materialized by policy 1078.

## Interrupted-run cleanup and retry

Candidates are identified by `run_id`. A normal failure rolls back promotion and
cleans only that run's candidates. A killed process cannot publish them. Under
the producer mutex, later builds remove at most 5,000 orphan rows per candidate
table that are older than seven days; this avoids a large cleanup transaction.
For a large abandoned run, an authorized operator may repeat the same bounded
age cleanup until it drains. Do not delete serving output to make a retry pass.
Every retry checks the source signature and the live raw watermark (global
latest report date and newest load in the raw tail, never capped by the chain's
anchor) again before publication; a concurrent source change or newly loaded
month blocks that candidate and leaves the previous complete output.
