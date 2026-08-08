# Runbook — `bond_live_daily`

The daily worker that keeps the bond product fresh with no human in the loop:
it loads the day's prices, the yield curve and the trade tape, then republishes
what the app serves.

Railway service: **`bond-live-daily`** (project `investintell-db`, env
`production`), `WORKER=bond_live_daily`, `restartPolicy=NEVER`,
cron **`30 7 * * *` UTC**.

---

## 1. What it does, and why in that order

| # | Stage | Writes | Notes |
|---|-------|--------|-------|
| 1 | `candles` | `bond_observation_daily`, `bond_live_daily_sweep` | Per-CUSIP delta from that CUSIP's own watermark. ~10k calls at ~190/min ≈ 55 min. |
| 2 | `curve` | `bond_yield_curve_daily` | 13 tenors, one call each. Each response is the tenor's whole history, folded between its own watermark and `calc_date` — backfills itself on a cold table, never past the requested day (§3c). |
| 3 | `ticks` | `bond_tick_daily` | Previous session, the full curated universe by default. `BOND_TICK_TOP_N` is an explicit typed degradation only. Same consecutive-failure breaker as stage 1 — see §4b. |
| 4 | `matview` | — | `REFRESH MATERIALIZED VIEW CONCURRENTLY bond_curated_securities`. |
| 5 | `republish` | `bond_metric_v1`, `bond_serving_v1` | Invokes `bond_metrics.run()` then `bond_serving.run()`. |
| 6 | `panel` | `bond_panel_v1` | Reads only production DB relations and atomically publishes the closed-month plus open-month delta after stage 5. |

**Stage 5 is not optional.** `daily_chain` keys a run by `(chain, source_day,
code_revision, config_version)` and returns a *completed* run's summary verbatim
without re-executing it. `source_day` is the max watermark over the
security/price/N-CEN/RR1 landing tables — none of which move when candles land.
So on a day with no deploy and no filing, the 11:00 chain finds its own run
already complete and executes nothing. Without stage 5 every byte loaded here
would sit unserved.

Double invocation is safe by construction: both publication workers are
self-anchored, take their own advisory locks, and are idempotent on their input
fingerprint, so the chain re-running them at 11:00 re-points instead of
rebuilding.

## 2. Cron placement

`30 7 * * *` UTC. The sweep plus the republication runs ~80 min, finishing
around 09:00 — a two-hour buffer before the chain at `0 11 * * *`. The two
cannot corrupt each other (distinct advisory locks) but overlapping them would
mean paying for the same publication twice.

## 3. Environment

| Variable | Required | Meaning |
|----------|----------|---------|
| `DATABASE_URL` | yes | The datalake (project-private network). |
| `FINNHUB_API_KEY` | yes | Absent ⇒ input lanes report `no_api_key`; refresh, republish, and panel still run before the non-zero verdict. |
| `WORKER_LIMIT` | no | Caps the universe swept in one run (budget-bounded catch-up). **Every capped run is red** — see §3b. |
| `WORKER_CALC_DATE` | no | Pins "today" for the window arithmetic (replay). **A date past the execution date is refused, never clamped** (`calc_date_in_future`) — see §3c. |
| `BOND_TICK_TOP_N` | no | Optional emergency cap. Unset means full curated universe; any positive cap is reported as `bounded_tick_scope` and keeps the run red. |
| `CODE_REVISION` | no | **One-off pin only — never a permanent service variable.** See §3a. |

### 3a. The deploy sha is a requirement, not a nicety

**The service MUST deploy from the GitHub source.** The run republishes
`bond_metrics` and `bond_serving`, and a serving publication's identity is
`uuid5(product | as_of | revision)`. `materialize` treats an existing id as
already built and merely **re-points** it — so if the revision does not move when
the code moves, a code-only serving/materializer change re-serves the *previous*
payload while the daily run reports success. That defect is invisible for weeks.

The revision comes from this ladder (highest precedence first):

| Rung | Source | When it answers |
|------|--------|-----------------|
| 1 | `CODE_REVISION` | A deliberate pin — see below. |
| 2 | `GIT_SHA` / `SOURCE_COMMIT` | Other CI/CD injections; unset here. |
| 3 | `RAILWAY_GIT_COMMIT_SHA` | **The production rung.** Railway injects the deploy's commit into every deployment that originates from the GitHub source. |
| 4 | `git rev-parse --short HEAD` | A developer's checkout. A deployed image has no `.git`, so this is silent in production by construction. |
| — | *nothing resolved* | The worker **raises** `BondServingRevisionUnresolved`. |

Rung 3 is measured, not assumed. On 2026-08-08 the sibling `bond-chain` service
had none of `CODE_REVISION` / `GIT_SHA` / `SOURCE_COMMIT` set, yet its unattended
cron runs on 2026-08-06 and 2026-08-07 recorded the full 40-hex shas
`e36213c3…` and `cfa628e5…` in `bond_daily_chain_runs` — both real `main` merge
commits, each the one deployed at that moment. Note that the Railway variables
API does **not** list `RAILWAY_GIT_*` (it is per-deployment metadata), so its
absence from `railway variables` proves nothing either way.

Raising is deliberate. The old behaviour fell back to the string `"unknown"`,
which collapsed every build of one `as_of` onto a single publication id — a green
run serving stale payload. The raise can only fire on a service that is *both*
detached from the GitHub source *and* unpinned, so failing the first run is the
cheap outcome.

**Verify on the first deploy.** The run result carries `code_revision` as
`<revision>+<input digest>`. Confirm the left side is the 40-hex sha of the
deployed commit — not `unknown`, and not a stale pin.

#### `CODE_REVISION` is a one-off pin, and leaving it set is the trap

Set it only to force a replay onto a known publication, and **remove it as soon
as the republication is done**. A permanently-set `CODE_REVISION` shadows rung 3,
so the identity stops moving on deploy and a code-only change collides on the
same `publication_id` again — exactly the defect the ladder removes, reintroduced
by the fix for it. Production paid this on 2026-08-07: the variable was set for
one republication and then deliberately removed.

Consequence of a moving revision, so it is not mistaken for a bug: **the first
run after any deploy rebuilds once**, because the revision changed. Replays
between deploys with unchanged inputs still land on the same id and re-point
idempotently.

## 3b. `WORKER_LIMIT`: the sweep is a ring, and a capped run is red on purpose

Two properties, both load-bearing, and the second one surprises people.

**The cap slices a ring, not a prefix.** The sweep is ordered by
`(last attempt NULLS FIRST, watermark NULLS FIRST, cusip9)`. The attempt stamps
live in `bond_live_daily_sweep` — one row per curated bond, written for every
bond the sweep *reaches*, whether it returned data, returned nothing, or failed.
So the bonds a capped run swept carry the newest stamp, sink to the back, and
the next run takes the next slice: `ceil(universe / WORKER_LIMIT)` runs cover
everything, then it wraps. Sorted by CUSIP — what this replaces — the cap took
the same first N bonds every run and the rest of the universe was never loaded.

The stamp is keyed on the **attempt**, not on the loaded watermark, and that is
the whole reason the extra table exists. A bond the provider has no data for
never gains a watermark, so a "most behind first" order keyed on the watermark
alone hands the head of every capped run to the same permanently dataless
cohort — and once that cohort reaches `WORKER_LIMIT` the sweep stops advancing
at all. Nothing the provider does can withhold an attempt. The watermark is
still the tie-break *inside* a round, which is also what makes a
transient-failed bond (stamped, watermark unmoved) retry ahead of one that
loaded cleanly when the ring comes back around.

The table is progress state, not data: dropping it costs one re-swept round.

```sql
SELECT count(*) FILTER (WHERE last_attempt_at >= current_date) AS swept_today,
       count(*) AS ring_size
FROM bond_live_daily_sweep;
```

**A capped run reports `partial_sweep` and exits non-zero.** It covered its
budget, not the day. It still republishes — the rows it loaded are real, and
every security carries its own observation date, so the payload stays honest —
but the *run* must not claim a day in which most of the universe was never asked
about. Consequence to expect rather than "fix": **a service left with
`WORKER_LIMIT` set is red on every run until the ring closes the gap or the
variable is removed.** That red is the signal that the catch-up is not finished;
the run that finally covers the universe is the one that goes green. `coverage`
in the run's JSON (`universe`, `swept`, `remaining`, `complete`) is where the
progress is read.

## 3c. `WORKER_CALC_DATE`: a replay is bounded on BOTH sides, and the future is refused

Every lane this worker loads takes its ceiling from the requested date, not
from what the table happens to hold: the candle window (`fetch_window` →
`not_after`), the tick session (`previous_business_day(calc_date)`) and — since
2026-08-08 — the **activity ranking that chooses the tick cohort**
(`[calc_date − 90d, calc_date]`, inclusive at both ends) and the **yield-curve
fold** (`curve_points(..., not_after=calc_date)`).

The curve was, briefly, argued to be exempt: it is not a windowed *request* at
all — the worker asks once per tenor and the provider returns that tenor's whole
history, which the fold trims at each tenor's own watermark. The argument holds
for a normal run and fails for exactly the case `WORKER_CALC_DATE` exists to
serve. The response is the full history either way, so a replay of an old
session upserted every point *after* it as well: `bond_yield_curve_daily` would
advance past the day being replayed, carrying rates from sessions the replay is
not loading — and stage 2 would report `latest_day` as today on a run whose
prices stopped at the replay date. The fold is now bounded above like everything
else; on a normal run the bound binds nothing (`calc_date` *is* the execution
date and the provider has no rates past it). The trim is silent, unlike the
candle path's counted `dropped_after_window`: a curve point past a replay date
is a real session this run is simply not the one to load, not a provider
anomaly.

That bound has a second consequence, and §4d depends on it: on a replay of a day
a tenor is already past, `not_before > not_after` and the fold is empty **by
construction**. That is the one empty curve response this worker treats as
benign — reported as `skipped_tenors`, apart from the empties that mean the
provider stopped answering with anything usable. Stage 1 gets the same treatment
for the same reason, since `fetch_window` clamps a replay's window to the single
requested day.

The tick cohort is the other replay defect worth stating plainly. The cohort is the
top-N most active bonds, and against a database that already contains sessions
*after* the replay date an open-ended window ranked them on activity that had
not happened yet: the run would ask the provider for the replay day's tape of
bonds that only became liquid later, and skip the bonds that were actually
trading then. Every call succeeds, so nothing in the run's JSON says the cohort
was wrong. In normal operation the bound binds nothing (`calc_date` *is* the
execution date, and future-dated candles are already refused at the loader);
it exists for replay.

**A `calc_date` past the execution date is refused before a connection is
opened** — state `calc_date_in_future`, exit non-zero — rather than clamped to
today. Two reasons, and the first is a data defect:

* `fetch_window`'s `to` is always `calc_date`, and that same value is the
  `not_after` bound `candle_rows` enforces. With a future `calc_date`, any
  provider stamp in `(execution_date, calc_date]` is accepted as a real session.
  `max(day)` of `bond_observation_daily` anchors the as-of of *both*
  publications, so one such row dates the product into the future and turns
  every legitimate publication after it into an as-of regression — a jam only a
  manual delete in production clears.
* Clamping would silently rewrite an operator's explicit parameter: the run
  would replay a date nobody asked for while the logs named the one they did.

Fix the variable and re-run (`railway service restart`, not `redeploy`).

## 3d. One-time privilege prerequisite (already applied 2026-08-07)

Postgres requires **ownership** to refresh a materialized view — a `GRANT` is not
enough — and `bond_curated_securities` was owned by `postgres` while the worker
connects as `worker_writer`. Stage 4 would have failed on every run. Applied on
the production datalake, aligning it with the other bond relations
(`bond_metric_v1`, `bond_reference_terms`, `bond_serving_facts` were already
`worker_writer`-owned):

```sql
ALTER MATERIALIZED VIEW bond_curated_securities OWNER TO worker_writer;
```

Verified by refreshing it `SET ROLE worker_writer` (10,073 rows). If the matview
is ever rebuilt by an operator running as `postgres`, this has to be re-applied —
the run reports `matview_failed` and exits non-zero if it is not.

## 3e. One-shot T3 parity gate before the first Stage 6 run

Run `WORKER=bond_panel_parity` once before allowing the first live panel
publication. The worker opens a read-only transaction, pins base publication
`92740098-1571-559d-9fb3-119de8321754` and fingerprint
`5a7af9e1adaed315e9940293cf3e9e789ca6350993688d58ab3e759cee37a3cb`, and
rebuilds only `2025-01` and `2026-06` from database inputs. It never inserts
facts or moves the panel pointer.

Set the temporary worker variable, deploy the exact revision, and execute with
`railway service restart`. A successful build is not execution evidence. Read
the emitted worker JSON and require `state=parity_passed`, `aborted=false`, and
both monthly gate records. Then restore `WORKER=bond_live_daily` before the live
run. Any `parity_failed` result is a stop: do not execute Stage 6 and do not
change the predeclared thresholds to make the result pass.

The parity transaction must still see the frozen publication as current. Run it
before Stage 6, because a successful Stage 6 publication intentionally advances
the pointer and makes a later replay fail `current_publication_id_mismatch`.

## 4. Reading the result

**One rule: a run exits green only when it actually did the day's work.**
`run_worker` exits non-zero on the top-level `aborted` key, and every state
below except `ok` sets it. There is no "reported no-op" tier any more — a polite
JSON blob describing a run that loaded nothing is exactly how a broken service
stays invisible for a week.

| `state` | green? | what it means |
|---------|--------|---------------|
| `ok` | **yes** | all six stages ran; both serving publications and the panel delta reported a publication |
| `calc_date_in_future` | no | `WORKER_CALC_DATE` is past the execution date: refused before anything opens, never clamped (§3c) |
| `locked` | no | another holder had this worker's advisory lock; **this run did nothing** (§4a) |
| `no_observation_table` | no | `bond_observation_daily` is absent (the serving repo owns its DDL); nothing loaded |
| `no_universe` | no | the curated universe is empty or its tables are absent |
| `no_api_key` | no | `FINNHUB_API_KEY` unset: candles, curve, and ticks are typed red; downstream stages still run before the final verdict |
| `provider_rejected` | no | the key was rejected mid-sweep (401/403 is non-transient by design) |
| `aborted` | no | the provider cut the candle sweep short (`MAX_CONSECUTIVE_FAILURES`) |
| `candles_failed` | no | stage 1 loaded **nothing**: not one bond whose window re-opened on a day this lane had *already* loaded came back with a candle — empty payloads, failures, or both (§4d). A cold table and a replay of a day every bond is already past are excluded, so this is silent on both |
| `curve_failed` | no | stage 2 loaded **no tenor**: all 13 failed, *or* their payloads folded to nothing — a 200 with empty/renamed `data` used to read exactly like a healthy run (§4d). Green needs one loaded tenor; a handful of failures on a run that still loaded something stays green, which is the case the per-tenor watermarks heal by themselves. `failed_tenors` / `empty_tenors` / `skipped_tenors` in the JSON say which shape |
| `ticks_failed` | no | stage 3 did not cover its cohort: every tick call failed, **or** the consecutive-failure breaker cut the lane short mid-cohort (§4b). `ticks.aborted` in the JSON says which |
| `matview_failed` | no | stage 4 did not refresh, so the cohort the publications read is stale. Two shapes, one state — `matview.state` in the JSON says which: `failed` = the `REFRESH` was rejected (ownership — see *One-time privilege prerequisite*); `absent` = `bond_curated_securities` does not exist at all (schema/deploy). Only `refreshed` is green |
| `republish_locked` | no | a publication worker's own lock was held: stage 5 did not recompute |
| `republish_no_op` | no | a publication worker reported a dark state (`no_source`/`no_securities`/`no_observations`): nothing was published |
| `republish_failed` | no | a publication worker failed, raised, or returned a state this contract does not know (drift is never read as success) |
| `panel_failed` | no | stage 6 could not rebuild a non-empty DB-only delta or one of its required fact surfaces was empty; it never reads an operator-local artifact |
| `panel_publish_failed` | no | stage 6 encountered a database/materializer operational failure; its pointer was not advanced |
| `panel_gate_failed` | no | stage 6 refused a missing/incompatible current parent, required relation, or validation gate; it does not bootstrap a two-month history |
| `partial_sweep` | no | `WORKER_LIMIT` truncated the universe — the budget was covered, the day was not (§3b) |

`halted_by` lists every clause that fired, in severity order; `state` is the
first of them. `coverage` carries `universe / swept / remaining / complete`.

### 4a. Why a held lock aborts instead of retrying

Decided 2026-08-07. Both publication workers return `{"state": "locked"}` when
their advisory lock is already held, and so does this worker for its own. The
alternative was an in-process retry with backoff. It loses on three counts:

* **Overlap is anomalous, not routine.** This service is a daily cron at
  `30 7 * * *` with `restartPolicy=NEVER` — a deploy *is* the execution — and the
  publication chain runs at `0 11 * * *`. A ~2-hour buffer separates them, so a
  collision means something already went wrong (a previous run still going, a
  manual rebuild, an operator restart). Retrying papers over exactly the event
  worth seeing.
* **A seconds-scale backoff cannot win.** The metric build takes minutes and the
  serving build carries ~2M facts. A retry loop short enough to be safe would
  sleep and fail anyway.
* **Waiting is the expensive kind of wrong.** An unbounded wait holds *this*
  worker's advisory lock open for a time nothing bounds, on a run that has done
  no work and may still do none. (Note which half of that is the objection —
  see §4c: *holding* the lock while this run works is bounded by the work and
  costs an idle connection; *waiting* on somebody else's is not bounded at all.)

So it fails loudly and hands the decision to an operator: check who holds the
lock, then re-run the service (`railway service restart`, not `redeploy`). The
state name stays `locked` on purpose — `daily_chain.classify_worker_result`
reads that exact string and classifies it as transient/retryable.

### 4c. The daily lock is held through stage 6 — and costs nothing

The lock wraps **all six stages**, not just the three that
write on its own connection.

Released after stage 3, an overlapping manual restart could take it while this
run was still refreshing the matview and republishing. That second run would
commit a *prefix* of its own revised candles into `bond_observation_daily` while
this run's `bond_metrics` / `bond_serving` build was reading it, then abort on
the publication locks — and this run would exit **green** having served a mix of
two sweeps. Nothing downstream can detect that: the rows are individually valid
and every security carries its own observation date. The lock has to cover the
read, not just the write.

Holding it does **not** re-open the VACUUM trap of §6, and the distinction is
worth keeping straight because they look alike:

* `advisory_lock` takes a **session** advisory lock (`pg_try_advisory_lock`,
  `src/db.py`). A session lock pins no snapshot and holds back no xmin horizon.
* The run **commits** before stage 4, so the connection sits `idle`, not
  `idle in transaction`, for the minutes the two publication builds take. That
commit is load-bearing: adding a read on that connection between stage 3 and
stage 6 would silently turn it into a minutes-long transaction, which is the
  trap. The code says so at the call site.
* Stages 4 through 6 open their **own** connections (`_refresh_curated` in
  autocommit; each publication worker and the panel materializer use their
  own), so the held connection only carries the lock.

This is `daily_publication_chain`'s idiom, not a new one: it holds
`LOCK_DAILY_PUBLICATION_CHAIN` across these same two workers while each takes
its own lock underneath. Deadlock-free by construction — nothing else in the
fleet takes `LOCK_BOND_LIVE_DAILY` (900_353), so there is no cycle to close.

One consequence to expect: if that idle connection dies during stage 6, the
release raises and the run exits red *after* the work landed. That is the safe
direction (verify in the tables, per §4a) and it is the same exposure the chain
already carries.

**Verify in the tables, never on the dashboard** — and note that on this project
`railway redeploy` reports SUCCESS *without executing*; only
`railway service restart` runs the job.

```sql
SELECT max(day) FROM bond_observation_daily;                     -- last business day
SELECT max(day) FROM bond_yield_curve_daily;                     -- same
SELECT count(*) FROM bond_tick_daily WHERE day = <prev session>; -- > 0
SELECT max(observation_date) FROM bond_serving_facts_v
 WHERE surface = 'observations' AND lane = 'latest';             -- the header's date
SELECT max(computed_at), max(last_closed_month), max(open_month), count(*)
  FROM bond_panel_publications WHERE publication_status='validated';
SELECT p.publication_id, p.config_hash, p.snapshot_rows, p.rv_signal_rows,
       p.returns_rows, p.ratings_pit_rows
  FROM bond_panel_app_pointer a
  JOIN bond_panel_publications p USING (publication_id)
 WHERE a.product='bond_panel_v1';                                 -- exact current pin
```

One caveat when reading the FIRST morning: at 07:30 UTC the previous session's
tape has not necessarily landed at the provider. A `max(day)` of D-2 on the
first run is plausibly the provider's timing rather than a bug — the window
re-reads its own watermark day, so the next run picks it up. Judge it on the
second day, not the first.

### 4b. Why a provider outage stops the tick sweep too

Decided 2026-08-08. Stage 1 has always had a consecutive-failure breaker; stage 3
did not, and it is the more expensive lane to leave unbraked. By the time
`client.ticks()` raises, the client has already spent its whole retry ladder —
**126 s of backoff per exhausted logical request** (measured 2026-08-07, after
the trailing-sleep fix; it was 246 s before), plus up to 7 × 45 s of
connect/read timeout on top. Across the full ~10k universe that theoretical
cost is much larger, which is why the breaker is part of the full-universe
contract rather than an optional optimization. The first 25 exhausted calls
already establish the outage; no later call can change that diagnosis.

The normal provider budget was declared before activation: at the measured
~190 calls/minute, the ~10k candle lane is about 55 minutes and the full tick
lane adds roughly another 55 minutes, for about 110 minutes total before the
database-only stages. A positive `BOND_TICK_TOP_N` is therefore an emergency
degradation with its own reason code, not the steady-state schedule.

Stage 3 now uses the **same** constant and the same shape as stage 1:
`MAX_CONSECUTIVE_FAILURES` (25) consecutive exhaustions abort the lane, the
counter resets on any successful call, and what was loaded before the outage
stays committed. Worst case drops to **25 × 126 s ≈ 52 min**, the same bound the
candle sweep carries.

The abort is **red**, and it reuses `ticks_failed` rather than adding a state:

* one event (the provider stopped answering), one operator hand — there is
  nothing to fix here but the provider;
* a full outage trips `swept == transient_failures` anyway, so the state an
  operator already knows keeps its meaning; only the mid-cohort case is new;
* and it must not fold into `aborted`, which names the *candle* sweep. A run
  whose prices landed cleanly and whose tape was cut short would otherwise send
  the operator to the wrong stage.

Unlike stage 1 there is no watermark to resume from: tomorrow's run asks for
tomorrow's session, so the tape of every bond the outage cut off is gone for
good. That is why a partially covered cost lane is a failed run and not a
progress report — the day's `bond_tick_daily` coverage is what it is.

### 4d. An empty lane is a lane that did no work — and how to tell it from a quiet day

Decided 2026-08-08, for stages 1 and 2 together, because it is one event in two
places: **a provider that keeps answering `200` with nothing in it.** Entitlement
lost and surfaced as JSON, a shape change under `s`/`t`/`c` or under
`data`/`d`/`v`, an ISIN mapping that stopped resolving — none of them raise, none
of them fail, and none of them used to be visible. The candle sweep counted
`no_data` and left `aborted` false; the curve loop `continue`d, so `failed_tenors`
stayed empty and the "all 13 failed" check could never fire. Both lanes could
load **zero rows for the entire universe** and the run still refreshed,
republished and exited `ok` — serving yesterday's data as today's work.
Reproduced against the pre-fix worker: both scenarios return `state=ok`,
`halted_by=[]`.

Both clauses now assert the **success** state, the shape the round-6 review
settled on for `matview_failed`: stage 1 did its work iff at least one bond
returned a candle, stage 2 iff at least one tenor loaded.

**The hard part is not the check — it is the false positive.** A day on which
nothing trades is implausible across 10k bonds, but it is not impossible on a
*replay*, and neither lane may cry wolf at an operator running one. The
distinction each stage uses is a day the provider can be **proved** to owe:

* **Stage 1 — `resumed`, not `swept`.** A bond whose window re-opens on a day
  *this lane already loaded* must come back with that day: this feed loaded it
  out of this same endpoint, so an empty answer for it is a fault. Bonds without
  that promise are excluded, and both exclusions are systematic rather than
  hypothetical:
  * a **cold-start** bond gets `fetch_window`'s 30-day window and no evidence.
    409 of the 10,073 curated bonds have been attempted and have never once
    returned data (measured 2026-08-08; 9,779 do carry a live watermark), and the
    sweep ring sorts never-loaded bonds *first inside a round* — so a thin
    `WORKER_LIMIT` slice is *expected* to be all-dataless. Judged on `swept`,
    this clause would fire on every capped run and mean nothing.
  * a **replay** clamps every loaded bond's window to `[calc_date, calc_date]`
    (`fetch_window` refuses to look past `today`), and a Saturday legitimately
    has no candle for any bond. This repo owns no trading calendar on purpose
    (`previous_business_day`), so such a run proves nothing and is left alone.

  In production neither exclusion costs coverage: on a healthy table `resumed ≈
  9,779 ≈ swept`. `candles.resumed` in the JSON is the denominator; it is counted
  at the window, before the answer, so a bond that failed transiently still
  counts — which is how stage 1 finally gets the "every call failed" half stage 3
  always had (below `MAX_CONSECUTIVE_FAILURES` bonds an outage never trips the
  breaker).

* **Stage 2 — the response is a history, not a session.** One call returns the
  tenor's *whole* history, so it is never empty for a market reason: a weekend
  cannot empty it. The only bound that can is an **inverted fold** —
  `not_before > not_after`, i.e. a replay of a day this tenor is already past
  (§3c), which is the normal state of a replay against a healthy table. Those
  land in `skipped_tenors` and are benign; everything else empty lands in
  `empty_tenors` and counts against the stage.

  Consequence, stated so it is not reopened as a bug: the benign exemption must
  be **unanimous**. A replay in which twelve tenors were skipped and one *failed*
  is red, because nothing loaded and something broke. "A handful of failed tenors
  stays green" remains true of a run that loaded something — which is the case
  the per-tenor watermarks heal by themselves.

Reading it: `candles.resumed` / `candles.with_data` for stage 1;
`curve.failed_tenors` / `curve.empty_tenors` / `curve.skipped_tenors` for stage 2
— one state per stage, the JSON says which shape, exactly as `matview.state` and
`ticks.aborted` already do. An all-empty lane is the provider, not the database:
check the key's entitlements and one raw response before touching this worker.

### 4e. A refused write surfaces its own error

Decided 2026-08-08. The candle sweep stamped its progress into
`bond_live_daily_sweep` from a `finally`, which ran on *every* exit from a bond —
including the one where it could not possibly work. An insert PostgreSQL refuses
(a constraint, a column the served DDL never grew) leaves the transaction
**aborted**, so the stamp raised `InFailedSqlTransaction`; and an exception raised
in a `finally` *replaces* the one already unwinding. The operator got "current
transaction is aborted" and the actionable error was gone. This is the same
incident `src/db.py::_release_advisory_lock` documents one frame up (2026-07-24:
`must be owner of view …` lost behind `pg_advisory_unlock`) — the release was
made non-masking then; the loader below it was not.

The stamp is now called **explicitly on the three handled paths** (data, no data,
transient failure) rather than from a `finally`, so a refused write simply
propagates as itself. Verified against a real transaction
(`tests/test_bond_live_daily_worker.py`, `SEC_TEST_DATABASE_URL`): the pre-fix
worker raises `InFailedSqlTransaction` from the stamp; the current one raises the
`CheckViolation`.

The resumability the `finally` existed for is intact, because the two properties
never actually collided: **the only path that loses its stamp is the one where
the stamp could not have been written at all.** Every path that still has a usable
transaction stamps, on the same commit cadence, so a capped or provider-cut run
keeps the progress of the prefix it swept. The bond that broke is deliberately
*not* stamped — the ring must not advance past a bond whose rows never landed —
and everything committed before it survives the rollback.

### Retention reports quietly — read it every run

`_prune_superseded_facts` is wrapped in try/except (a retention failure must
never roll back a promotion), so a problem shows up as a field, not an alarm.
The mechanism itself is covered by executing tests in
`tests/test_bond_serving_worker.py` (the purge, the token, the batching, the
kept build row, the refusals, a schema replay); what the runbook checks is the
production effect:

```sql
-- 1. the run's own report. Healthy: retention.state='purged' with pruned_rows,
--    or the bare {pruned_publications:0, pruned_rows:0, kept:N} of a steady
--    state. NOT state='failed', and NOT state='blocked_by_write_guard' (that
--    one means the database is running DDL older than the purge routine --
--    re-apply schemas/bond_serving_v1.sql; see §6).

-- 2. the WORST failure available: the app still points at facts that exist
SELECT count(*) FROM bond_serving_facts f
  JOIN bond_serving_publications s ON s.worker_publication_id = f.publication_id
  JOIN bond_serving_app_current_pointer p ON p.app_publication_id = s.app_publication_id;
-- must be > 0 (2,021,178 per complete publication)

-- 3. steady state is about three publications, not seven and not one
SELECT count(DISTINCT publication_id) FROM bond_serving_facts;

-- 4. every publication keeps its build row, purged or not (the as_of guard reads it)
SELECT count(*) FROM sec_derived_publications p
  LEFT JOIN bond_serving_builds b USING (publication_id)
WHERE p.product='bond_serving_v1' AND b.publication_id IS NULL;  -- must be 0

-- 5. the run's `rebuilt_over` field. Normally []. A non-empty list means an input
--    reverted onto an identity retention had already emptied and the worker built
--    past it — expected, not an incident. In the CHAIN's stage detail the list is
--    dropped (`_result_detail` keeps scalars only, as it does for `retention`);
--    what survives there is `code_revision`, which then ends `+rebuild1`,
--    `+rebuild2`, … — that suffix is the same evidence. A chain that keeps GROWING
--    on consecutive runs IS the incident: retention purging what a rebuild has
--    just created. See §6.
```

## 5. The header-freshness mechanism (decided 2026-08-07)

**Problem.** `bond_price_latest_v1` projects the governed price publication,
whose landing table stops at **2025-03-31**. The detail header and the catalog's
price/yield columns were built from it, so the product stated a sixteen-month-old
price next to a chart drawn through yesterday.

**Chosen: the dense daily series becomes an optional input to the two
publication builds, and the serving `as_of` follows it.** Three parts:

1. `bond_metrics` reads `bond_observation_daily` **per field** — the freshest
   priced day for the price, the freshest yielded day for the yield, each
   carrying its own date, duration settling on the yield's. Folding them
   together would let a fresh price erase an older bond's yield *and* its
   duration.
2. `serving_materializer` resolves `_bond_latest_observation` once per build:
   the governed lane, with each security whose dense observation is *strictly*
   newer replaced by it. Both the inline `latest_price_pct` and the observations
   `latest` lane read that table. The `fund_asof` (point-in-time) lane is
   untouched.
3. `bond_metrics._resolve_as_of` and `bond_serving._resolve_as_of` take the
   **greatest** of their existing anchor and the series' last day.

Part 3 is what makes the other two do anything. Measured before the change:
`max(as_of)` on the governed landing table is 2025-03-31, so the
`observation_date <= as_of` no-look-ahead guard excluded every fresh row; and
the serving publication id is `uuid5(product | as_of | code_revision)` with
`as_of` frozen at the security master's 2026-07-23 and the revision moving
only on a deploy — so every run resolved to the same publication, which
`materialize` treats as already built and merely re-points.

**Rejected: salting the serving publication identity with an input
fingerprint.** It would mint a new publication daily while the payload still
*claimed* 2026-07-23. A fresh price stamped with a stale date is worse than a
stale price.

**Rejected: appending the day into `bond_price_observation`** (the governed
landing table) so the existing chain republishes it. `_PUBLISH_SQL` builds each
publication from ONE `as_of` cohort, so a daily landing shrinks
`bond_price_latest_v1` to that day's cohort. Re-landing each CUSIP's latest known
row daily to repair the coverage is worse still: a bond that does not trade gets
the same `(security_id, observation_date)` landed under successive cohorts, and
`bond_price_fund_asof_v1` recomputes duplicates across the whole landing table —
so every non-trading bond's observations would degrade to ambiguous.

**Measured on production, 2026-08-07 (rollback-only, SQL generated from the
shipping constants):**

| | before | after |
|---|---|---|
| securities with a served price | 11,964 | 21,935 |
| securities priced in 2026-08 | 0 | 9,149 |
| latest lane max observation_date | 2025-03-31 | 2026-08-06 |
| alias disagreements on the latest day | — | 0 |
| max eligible rows per security (inline subquery must stay scalar) | — | 1 |

Cost: the dense-lane resolution adds ~87 s to the serving build and ~4 min to the
metric build.

## 6. Retention (why it had to ship with this)

A complete serving publication is ~2.0M facts / ~1.2 GB. That was harmless while
a rebuild needed a deploy; now that `as_of` follows the daily series a rebuild
happens most days. `bond_serving.run()` therefore prunes, in batched commits, the
facts of every publication outside a three-part keep-set:

* the worker's own current pointer;
* whatever the **app's** current pin references — the pin advance can honestly
  fail and leave the app on an older publication, and deleting that one breaks
  the product silently;
* the immediately-prior worker publication — `daily_chain` compensation restores
  the *pre-run* pointer on a failed run.

### How the rows can go at all: the purge token

`bond_serving_facts` is immutable by construction — the write guard rejects every
non-INSERT, the `publication_id` FK is `ON DELETE RESTRICT`, and
`sec_derived_publication_delete_guard` refuses to delete a validated publication,
so deleting the parent is not a way around it either. Retention does **not**
relax that. It uses ONE sanctioned exception, in the protocol's own idiom
(`sec_derived_publication_tokens` / `sec_derived_pointer_tokens`):

| piece | where |
| --- | --- |
| `bond_serving_purge_tokens(publication_id, backend_pid)`, revoked from PUBLIC | `schemas/bond_serving_v1.sql` |
| write-guard DELETE branch: returns OLD only for `bond_serving_facts`, only when THIS backend holds the token | `bond_serving_write_guard()` |
| `bond_purge_serving_publication(uuid, batch int)`: takes the token, deletes one bounded batch, drops the token | `schemas/bond_serving_v1.sql` |
| `bond_serving_purged_publications`: written by that routine, in the same transaction as the batch — "this id lost facts" (see the purged-identity section below) | `schemas/bond_serving_v1.sql` |
| the call site: `_prune_superseded_facts` loops it, **committing per batch** | `src/workers/bond_serving.py` |

Consequences worth knowing before touching any of it:

* **The `bond_serving_builds` row is kept on purpose.**
  `sec_derived_publication_as_of` reads it and that feeds the current-pointer
  as_of regression guard. A purged publication stays in the ledger, with its
  build metadata, holding no facts. Deleting the build row would break the guard.
  Verified after the first production purge: all 9 publications still resolve an
  `as_of` (v1 → 2026-07-23, v4–v7 → 2025-03-31, …) while 7 of them hold 0 facts.
* **One transaction per batch, never one big DELETE.** A long transaction holds
  back VACUUM for the WHOLE database — see the trap measured below.
* **An UPDATE stays forbidden**, token or no token, and a token minted by another
  backend authorises nothing.
* `bond_purge_serving_publication` refuses the worker-current and the app-pinned
  publication itself, so a hand-run purge cannot empty the served surface either.
  The wider margin (the immediately-prior publication) is worker policy.
* The worker probes for the **capability** (`bond_serving_purge_tokens` +
  `to_regprocedure('bond_purge_serving_publication(uuid,integer)')`), never for
  the guard's absence — the guard survives the purge, that is the design. A
  database that predates the routine still gets the typed
  `retention.state=blocked_by_write_guard` report, now naming the DDL that fixes it.

### The hole retention opens: a purged identity that resolves again

Keeping the publication row (above) and deterministic identity (§5) are each
right on their own and together they let the product serve **nothing** under a
green run:

1. retention frees a superseded publication's facts and leaves its publication +
   build rows in the ledger;
2. identity is `uuid5(product | as_of | revision)` with the content digest in the
   revision, so the same inputs resolve to the same `publication_id` **forever** —
   the replay invariant, and it is wanted;
3. an input REVERSION (a corrected close re-read back to its earlier value, a
   metric publication restored, a `calc_date` replay) therefore reproduces a
   purged publication's id, and `materialize` reads an existing id as **already
   built**: it skips the projection, re-points, and `_advance_app_pin` pins it.

Measured 2026-08-08: **eight of ten** `bond_serving_v1` publications are
validated and hold zero facts (five freed by the first purge, three born empty
before the coverage gate existed), all eight still carrying their build row. The
as_of regression guard is only a partial net — all eight sit at as_of ≤
2026-07-23 against a pointer at 2026-08-07, so an OLDER-day replay dies loudly
inside `sec_set_current_derived_publication`; what it cannot see is the
**same-as_of** case, which is exactly what same-day content revisions manufacture
(production carries four publications on 2025-03-31 and three on 2026-07-23).

`_servable_revision` (`src/workers/bond_serving.py`) closes it by walking
`base`, `base+rebuild1`, `base+rebuild2` … and stopping at the first identity
that either does not exist (built) or exists **with its facts** (re-pointed,
exactly as before). Two things follow, and both are deliberate:

* the condition is **observable**, not remembered — "this publication holds no
  facts" answers for the eight production already emptied, with no backfill, and
  no state to keep in sync. Marking purged ids non-reusable was the alternative
  and is strictly worse: it cannot make the marked id servable either (validated
  rows are undeletable and the write guard only admits an INSERT under a
  `prepared` parent), so it needs this same walk anyway;
* the walk is deterministic in the DATABASE STATE, so **idempotence survives**: a
  replay over unchanged inputs steps past the same unservable candidates and
  re-points onto the same built one, no rebuild.

`bond_serving_purged_publications` covers the other half. The purge commits one
batch per transaction (a 2M-row DELETE would hold back VACUUM database-wide), so
a crash between batches leaves a **partial** publication — facts > 0, payload
incomplete — which emptiness alone would read as built. The routine records the
publication there in the same transaction as the batch it precedes, so the row
exists iff rows were actually lost.

Cost of the probe, measured on production 2026-08-08, and the reason it is a
`count(*)` and not the obvious `EXISTS`: `publication_id` has a handful of
distinct values, so the planner estimates ~2M rows for an equality and takes a
Seq Scan for `EXISTS (… LIMIT 1)` — which stops early only if a row matches, i.e.
never for the empty publication this path is about (**8.4 s** warm under a
generic plan, **40.2 s** cold). An aggregate cannot stop early, so the index-only
scan wins under both plan modes: 0.5 ms / 11.0 ms empty, 91.7 ms / 126.2 ms on
the 2,031,147-row live publication — and it is only asked when the identity
already exists, i.e. on a replay.

Exhaustion (`REBUILD_ATTEMPT_LIMIT`, 32) raises `BondServingRebuildExhausted` and
FAILS the run — never a dark state. It is a pathology guard, not the terminator:
a purged publication can never regain facts.

**Named residual.** A `WORKER_CALC_DATE` replay of an *older* day that resolves
onto a purged identity now BUILDS (~2M rows) and is then refused by the as_of
regression guard, which rolls the whole transaction back — where before the walk
it failed immediately on the same guard. The run is red either way and nothing is
promoted either way; the replay just costs a wasted build. That path is
deliberately closed by the guard (an old day must not become current), so it is
documented, not worked around: to replay an old day on purpose, use the guard's
own `allow_as_of_regression` escape hatch rather than expecting this worker to
short-circuit it.

### Disk: re-used, NOT returned (measured, production, 2026-08-07)

`bond_serving_facts` is a **plain table, not a hypertable** — there are no chunks
to drop. A DELETE only marks tuples dead; VACUUM then makes that space reusable
by this table and does not give it back to the OS (only `VACUUM FULL` would, and
it takes an ACCESS EXCLUSIVE lock on a table the app reads — do not).

First production purge, run by hand through the recipe below:

| | before | after purge | after VACUUM |
| --- | --- | --- | --- |
| rows in `bond_serving_facts` | 5,988,124 | 4,042,356 | 4,042,356 |
| publications holding facts | 7 | 2 (v8 + v9, the keep-set) | 2 |
| `n_dead_tup` | 0 | 1,945,768 | **0** |
| `pg_total_relation_size` | 3446 MB | 3446 MB | 3446 MB |
| volume (`/var/lib/postgresql`, 458 GB) | 220.8 GB free | 212.1 GB free | 208.1 GB free |

1,945,768 rows across 5 unreachable publications went in 50k-row batches — 40
batches carrying rows, 0.6–15.0 s each, 107.6 s of database time in total, and
not one transaction longer than a batch. `n_dead_tup` then went from 1,945,768 to
**0** once the horizon cleared (final pass 1.6 s, 1,363,958 dead item identifiers
removed from the indexes, 21,110 + 6,487 index pages now reusable; an autovacuum
pass took the rest) — and the table is still
3446 MB, to the megabyte. That is the whole shape of the result: ~1.1 GB inside
the table became free space the next publication is written INTO instead of
extending the file. The number that says retention is working is
`count(DISTINCT publication_id)`, not `pg_total_relation_size`.

Read the volume row correctly: it kept FALLING (220.8 → 212.1 → 208.1 GB free)
right through the purge, and none of that is the purge's to give back — a DELETE
only writes WAL, and VACUUM wrote 11 MB more. The window was shared with a
TimescaleDB compression policy and other writers, so the movement is theirs.
**There is no measurement in which purging returns disk to the OS.**

**Trap paid on this very run — budget for it.** The first
`VACUUM (ANALYZE, VERBOSE)` right after the purge reported
`1945768 are dead but not yet removable` and freed nothing. Nothing was wrong
with the purge: an unrelated hour-old transaction (`CREATE TEMP TABLE
stock_fundamentals_statements_scope ...`, pid 113314) was pinning the global xmin
horizon, and a dead tuple cannot be reclaimed while any snapshot older than its
deletion exists. It stayed pinned for ~20 further minutes; the moment it cleared,
one `VACUUM` took the whole 1,945,768 to zero in 1.6 s. Diagnose before
re-vacuuming rather than re-running the purge:

```sql
SELECT pid, usename, state, age(backend_xmin) AS xmin_age, now()-xact_start AS xact_age,
       left(regexp_replace(query,'\s+',' ','g'), 80) AS q
FROM pg_stat_activity WHERE backend_xmin IS NOT NULL ORDER BY 4 DESC LIMIT 5;
SELECT slot_name, active, age(xmin) FROM pg_replication_slots;  -- the other classic holder
```

Evidence it was the horizon and not the purge: `n_dead_tup` sat at exactly
1,945,768 — the purged row count, to the row — across one manual `VACUUM` and
several autovacuum passes while the blocking transaction stayed open, then went
to 0 in a single pass afterwards. Autovacuum retries on its own, so doing nothing
also works; the only requirement is not mistaking it for a failed purge. Watch:

```sql
SELECT n_dead_tup, last_autovacuum FROM pg_stat_user_tables
WHERE relname='bond_serving_facts';   -- drops to 0 once the pin clears
```

### Manual purge recipe

Only needed to catch up a database that accumulated publications before the
routine existed — the daily run does this by itself. Two things are load-bearing:

* **`SET ROLE worker_writer`**. Every bond object in production is owned by
  `worker_writer`, and the worker re-applies this DDL on EVERY run with
  `CREATE OR REPLACE`. Applying it as `postgres` leaves postgres-owned functions
  and bricks every future run.
* **Apply the schema and purge in the SAME session/script run.** A deployed image
  that still carries the old DDL will `CREATE OR REPLACE` the guard back to its
  no-DELETE body on its next run; re-apply the schema first if you resume later.

Build ONE script locally and pipe it in. The DDL has to be **inlined** — `\i`
resolves inside the container, which has no repo checkout:

```sh
{
  printf '\\set ON_ERROR_STOP on\nSET ROLE worker_writer;\n'
  # BEGIN/COMMIT around the DDL is not decoration: the file DROPs the write
  # guard before re-creating it, so a half-applied run leaves the table UNGUARDED.
  printf "SET lock_timeout = '15s';\nBEGIN;\n"
  tr -d '\r' < schemas/bond_serving_v1.sql
  printf 'COMMIT;\n'
  # One statement per batch -- psql autocommit gives one transaction per batch.
  # Repeat per stale publication until it returns 0. NEVER wrap the loop in a DO
  # block or a generate_series: that collapses every batch into ONE transaction,
  # which is the VACUUM trap this batching exists to avoid.
  for i in $(seq 1 40); do
    printf "SELECT bond_purge_serving_publication('%s'::uuid, 50000);\n" "$PUB"
  done
} > apply_and_purge.sql

railway ssh --project 35fa36a3-2641-42b2-b48b-540eac0597c6 --environment production \
  --service market-clean-serial -- psql -U postgres -d market -f - < apply_and_purge.sql
```

(Extra calls past exhaustion are harmless: they return 0.)

The stale set is the same arithmetic the worker uses — note it asks the 9-row
publications table which ids are stale and only THEN counts those through the
index. A bare `GROUP BY publication_id` over the facts table reads the whole
3.4 GB relation (4m28s measured) for the same answer:

```sql
WITH keep AS (
  SELECT publication_id FROM sec_derived_current_pointers WHERE product='bond_serving_v1'
  UNION SELECT s.worker_publication_id FROM bond_serving_app_current_pointer p
        JOIN bond_serving_publications s ON s.app_publication_id=p.app_publication_id
  UNION SELECT publication_id FROM (SELECT publication_id FROM sec_derived_publications
        WHERE product='bond_serving_v1' ORDER BY publication_version DESC LIMIT 2) r),
stale AS (
  SELECT publication_id FROM sec_derived_publications
  WHERE product='bond_serving_v1' AND publication_id NOT IN (SELECT publication_id FROM keep))
SELECT f.publication_id, count(*) FROM bond_serving_facts f
WHERE f.publication_id = ANY(ARRAY(SELECT publication_id FROM stale)) GROUP BY 1;
```

The sibling question — `bond_metric_v1` now mints one publication a day —
is a **known follow-up**, deliberately not bundled here. Measured 2026-08-07:
8 publications, 5.40M rows, 1807 MB, i.e. ~1.27M rows (211,406 securities x 6
metrics) and **~270 MB per day**. That is smaller than the serving facts in
BYTES (no jsonb payload) but comparable in ROWS — do not carry over the serving
keep-set by analogy. Its restore targets are different (the chain compensates
metric pointers too, and `bond_serving` consumes the metric view at build time,
so a pruned metric publication can strand a serving rebuild) and that reasoning
has to be done on its own terms.

## 7. Free property

On a day the series does not advance (a weekend), `as_of` does not move, both
publication identities replay, and the republication is a cheap re-point rather
than a needless 2M-row rewrite.
