# Runbook — `bond_market_implied_rating_v1` republication (DG-4 Phase 4)

The ordered procedure for republishing the market-implied rating product under
the fixed policy `4b752a3fc5d5222b398f1e2a073b7c428b068e77968f8fb79edd20957202a08a`
(anchor pin `-0.8864114120812487`, owner amendment 2026-09-30) on a pinned,
recorded production stack, with G1(a) proven on the exact production image
before anything is written.

**Every step in §2 touches production and requires explicit owner
authorization, step by step. Nothing in this runbook is authorized by being
written here.** The engineering gates it relies on are local commits on
`feat/live-daily-pinned-stack`; the owner reviews them in the PR.

Related: [`bond-live-daily.md`](bond-live-daily.md) (the daily service and
Stage 7), [`bond-el-local-preparation.md`](bond-el-local-preparation.md) (the
fixed-anchor policy change), the calibration round result 003 (owner
acceptance of the reference identifiers and of this Phase 4 plan).

---

## 1. What changed and why

| Gate | Where | What it gives the owner |
|------|-------|-------------------------|
| Pinned stack | `docker/bond-live-daily/{Dockerfile,requirements.in,requirements.lock}` + `railway.bond-live-daily.toml` | `bond-live-daily` builds a dedicated image: `python:3.13.12-slim` pinned by linux/amd64 manifest digest, every wheel sha256-locked (numpy 2.5.1, pandas 3.0.3, scipy 1.18.0, pyarrow 25.0.0, psycopg 3.3.3, the full closure of the root requirements incl. statsmodels), BLAS/OpenMP single-threaded, `pip check` and a full daily-closure import proven at build time. The other services keep Nixpacks and the root files. |
| Build manifest | `src/bonds/build_manifest.py`; `build_manifest` on every `plan()`/`run()` result and every determinism receipt | Interpreter, package versions, platform/libc, CPU model + flags, NumPy SIMD baseline/dispatch and BLAS config, the thread variables, the sha256 of the shipped lock/Dockerfile, the code revision. Evidence only: never part of any identity, fingerprint or digest. |
| Determinism check | `--determinism-check` in `scripts/backfill_bond_market_implied_rating.py`, `src/bonds/implied_rating_replay.py`; on the deployed image `WORKER=bond_market_implied_rating_check` (`src/workers/bond_market_implied_rating_check.py`, same replay code) | G1(a) without writing: one read-only snapshot read, an immutable sha256-pinned export, two sequential fresh interpreter processes running the same pure build, compared digests/counts/frames, a JSON receipt (returned in the worker's stats, i.e. in the service log). |
| Apply preconditions | `--expect-current-pointer / --expect-panel-publication / --expect-input-fingerprint / --expect-rows-digest`; on the deployed image the `BOND_IMPLIED_RATING_EXPECT_*` service variables | `--apply` and every FORCED republication are digest-bound: `rows_digest` + `input_fingerprint` expectations are REQUIRED (refused before any DDL without them); any set expectation that production does not match refuses (`precondition_failed`, no write). |
| Latest-month guard | `run()` refuses `implied_rating_latest_month_unwitnessed` when the last closed month has zero witnessed rows; `plan()`/receipts report `latest_month_witnessed_count` | A dark September cannot be republished by accident. Separate commit; the owner may drop it. |
| Read-only verification | `scripts/verify_bond_market_implied_rating_publication.py` | Recomputes `rows_digest` from the stored rows of one UUID with the producer's canonical digest and compares it with the build pin, counts, policy digest and pointer. |

Identity is unchanged: `publication_id = uuid5(product | policy_version |
policy_digest | code_revision | input_fingerprint)`. The code revision in
production is `RAILWAY_GIT_COMMIT_SHA` of the deployed commit, so the production
publication id will NOT equal the one minted by the local round-003 rebuild;
`rows_digest`, `input_fingerprint`, `policy_digest` and the anchor are what must
match.

## 2. Procedure (ordered; each step needs its own owner authorization)

### 2.0 Preconditions (no production action)

- The September-dark producer fix (other branch) is merged and deployed, or the
  owner has decided to drop the latest-month guard. Without it, step 2.3 refuses
  with `implied_rating_latest_month_unwitnessed`.
- This branch is merged to `main`; the deployed commit sha is known.
- Round-003 reference identifiers are at hand (final section of
  `bond_market_implied_rating_round_result_003.md`): the accepted
  `b8b9503d…` / `28f70b9b…` identifiers, the pin `-0.8864114120812487`, policy
  digest `4b752a3f…`. The receipt of step 2.2 is compared against them.
- Nobody else publishes this product: Stage 7 stays `BOND_IMPLIED_RATING_ENABLED`
  unset/false on `bond-live-daily` (default), and no permanent
  `WORKER=bond_market_implied_rating` service exists (§2.2/§2.3 set `WORKER`
  on `bond-live-daily` TEMPORARILY and revert it). Both the determinism check
  and the apply are run OUTSIDE the daily window (`30 7 * * *` UTC, ~2 h run;
  see `bond-live-daily.md` §2) so the panel pointer does not move under the
  read (`inputs_moved` / `panel_pointer_moved` refuse otherwise) and so the
  cron does not fire while the variables are overridden.
- The operator can change service variables and restart the service (Railway
  dashboard or CLI); there is no shell on the image (§2.2 explains why).

### 2.1 Deploy the dedicated config to `bond-live-daily` — OWNER AUTHORIZATION REQUIRED

1. In the Railway service settings of `bond-live-daily` (project
   `investintell-db`, env `production`, service `e673db8e-…`) set the
   config-as-code path to `railway.bond-live-daily.toml`. Do not change
   variables. The service must keep deploying from the GitHub source
   (`RAILWAY_GIT_COMMIT_SHA` is the code revision; the image carries no `.git`).
2. Deploy the merged commit. Expected in the build log, last step:
   `bond-live-daily image verified: python=3.13.12 numpy=2.5.1 pandas=3.0.3
   scipy=1.18.0 pyarrow=25.0.0 psycopg=3.3.3 lock_sha256=<16 hex>...`
3. Confirm the schedule after the switch is still `30 7 * * *` UTC and the
   restart policy is `never` (the file carries both; the dashboard shows the
   effective values).
4. Record the stack. The service is a cron container that exits between runs,
   so there is no shell to `railway ssh` into; the evidence comes from two
   places instead: the build log line of step 2 (`verify_image.py` ran at build
   time on the image) and the `build_manifest` the next run JSON carries
   (`parent_build_manifest` / `children[*].build_manifest` in the §2.2
   receipt, `build_manifest` in every daily Stage 7 result and in every
   `plan()`/`run()` result, refusals included). Its `lock.sha256` must equal
   `git show <deployed sha>:docker/bond-live-daily/requirements.lock | sha256sum`
   (on a Linux checkout; the file is `eol=lf` pinned). Keep the manifest JSON
   with the evidence: it is the CPU model/flags and NumPy SIMD baseline/dispatch
   of the production host, which the round-003 last-bit differences are read
   against.
5. The next scheduled daily run must be green with Stage 7 still `disabled`.
   Verify in the tables (`bond-live-daily.md` §4), never on the deploy status.

Rollback of this step: set the config path back to the root `railway.toml`
(Nixpacks) and redeploy. Nothing else depends on the image.

### 2.2 Determinism one-shot on the exact production image — OWNER AUTHORIZATION REQUIRED

**Why this shape.** `bond-live-daily` is a cron service: its container runs
`python -m src.run_worker` once and exits, so between runs there is nothing to
`railway ssh` into, and `railway run` executes on the operator's machine, not
on the image. The only way to execute code on the EXACT deployed image is the
existing entry point with a different `WORKER`. The determinism check is
therefore a worker module, `src/workers/bond_market_implied_rating_check.py`,
that calls the same `src.bonds.implied_rating_replay.determinism_check` the
CLI's `--determinism-check` calls (shared code, no second implementation) and
returns the FULL receipt inside the stats JSON `run_worker` prints — the
service log is the receipt, because the container disk is ephemeral. It never
writes: the session is `default_transaction_read_only=on` with bounded
timeouts, the children get no DSN, and the module contains no
`install_schema`/`materialize`.

Procedure, on the `bond-live-daily` service, OUTSIDE the `30 7 * * *` UTC window
(the daily run takes ~2 h; stay clear of it), each sub-step owner-authorized:

1. Note the current value of `WORKER` (expected `bond_live_daily`) and that
   `BOND_IMPLIED_RATING_FORCE_REPUBLISH` and every `BOND_IMPLIED_RATING_EXPECT_*`
   variable are UNSET.
2. Set the service variables (temporarily):

   ```text
   WORKER=bond_market_implied_rating_check
   BOND_IMPLIED_RATING_EXPECT_PANEL_PUBLICATION=<bond_panel_app_pointer publication_id (UUID)>
   BOND_IMPLIED_RATING_EXPECT_CURRENT_POINTER=<sec_derived_current_pointers publication_id (UUID); leave UNSET when absent>
   ```

   Both expectations are optional for the check; a malformed value refuses
   before connecting (`expected_<field>_malformed`). Do NOT set the force flag.
3. Trigger exactly one run: `railway service restart` (or the dashboard's
   restart) on `bond-live-daily`. **The effective behaviour must be confirmed in
   the logs**: the first JSON line must read `"worker": "bond_market_implied_rating_check"`
   and the deploy must still be the pinned image (`build_manifest.python.version`
   `3.13.12`, `lock.sha256` as in §2.1). If the log shows `"worker": "bond_live_daily"`,
   the variable change had not propagated to that run — do not interpret that
   run as the check.
4. Read the receipt from the log (the whole stats JSON; `receipt` is the full
   document, the top-level keys repeat its summary). Save it with the evidence.
   Exit code 0 with `"state": "deterministic"` is the only pass; `determinism_mismatch`
   / `determinism_refused` exit 1 with `mismatch_reasons` / `receipt.refusal`.
5. Restore `WORKER=bond_live_daily` and REMOVE the `BOND_IMPLIED_RATING_EXPECT_*`
   variables. Verify the variable set is back to its pre-step-1 state.

**The cron can fire during the override.** If 07:30 UTC arrives while
`WORKER=bond_market_implied_rating_check` is set, that day's daily run is
replaced by a (harmless, read-only) determinism check and the daily chain does
not run; a forced-apply override (§2.3) left in place would republish on every
cron. Keep the override window short, never overnight, and always revert.

Budget: the production snapshot is ~3M rows; the export is a pickle of that
frame (GBs of RAM in the parent, hundreds of MB on the container's ephemeral
disk, under a fresh `/tmp/bond-implied-rating-check-*` directory), each child
is one full-history build (tens of minutes). The replay defaults bound it
(`statement_timeout` 1800 s, child timeout 21600 s). The local CLI form,
`python -m scripts.backfill_bond_market_implied_rating --determinism-check
--work-dir ... --receipt ... --expect-*`, is the SAME check on a different
stack: useful for a dry rehearsal, not evidence about the production image.

Record from the receipt, next to the round-003 reference:

| Receipt field | Must be |
|---------------|---------|
| `policy_digest` | `4b752a3fc5d5222b398f1e2a073b7c428b068e77968f8fb79edd20957202a08a` |
| `l_anchor.repr` / `.hex` | `-0.8864114120812487` / its hex |
| `input_fingerprint`, `rows_digest`, `row_count`, `publication_id` | recorded; compared with the accepted reference |
| `latest_month_witnessed_count` | `> 0` (else step 2.3 will refuse) |
| `child_pids` | two distinct pids, neither the parent |
| `children[*].build_manifest` | python 3.13.12, numpy 2.5.1, lock sha256 == the image's; CPU model/flags and `numpy_runtime.cpu_baseline/cpu_dispatch` recorded |
| `export.sha256`, `export.roundtrip` | present; the roundtrip fingerprint equals `input_fingerprint` |

A `rows_digest` that reproduces across the two processes but differs from the
round-003 local reference is a STOP for the owner, not a retune: round 003
observed 637 last-bit differences in the log-derived columns between stacks
(`np.log` is SIMD-dispatched), and the two manifests (local vs production CPU
flags / NumPy dispatch) are the evidence to adjudicate it. `mismatch` between
the two children means the production image is not deterministic: stop.

### 2.3 Apply with expectations — OWNER AUTHORIZATION REQUIRED (writes + DDL replay)

Same service, same mechanism as §2.2 (the deployed image through
`src.run_worker`), outside the daily window, with the values taken from the
receipt the owner accepted in §2.2. A forced republication is DIGEST-BOUND: the
worker refuses — before connecting, before any DDL — unless BOTH
`BOND_IMPLIED_RATING_EXPECT_ROWS_DIGEST` and
`BOND_IMPLIED_RATING_EXPECT_INPUT_FINGERPRINT` are set (64 lowercase hex each;
`precondition_failed`, `input_reasons` `expected_rows_digest_required` /
`expected_input_fingerprint_required` otherwise). Each sub-step owner-authorized:

1. Confirm §2.2 passed on this image and that the variable set is back to
   normal (`WORKER=bond_live_daily`, no force flag, no `EXPECT_*`).
2. Set the service variables (temporarily):

   ```text
   WORKER=bond_market_implied_rating
   BOND_IMPLIED_RATING_FORCE_REPUBLISH=1
   BOND_IMPLIED_RATING_EXPECT_ROWS_DIGEST=<receipt rows_digest>
   BOND_IMPLIED_RATING_EXPECT_INPUT_FINGERPRINT=<receipt input_fingerprint>
   BOND_IMPLIED_RATING_EXPECT_PANEL_PUBLICATION=<receipt panel_publication_id>
   BOND_IMPLIED_RATING_EXPECT_CURRENT_POINTER=<receipt current_pointer; leave UNSET when the receipt says null>
   ```

3. Trigger exactly one run: `railway service restart` on `bond-live-daily`.
   **Confirm the effective behaviour in the logs**: the JSON line must read
   `"worker": "bond_market_implied_rating"` on the pinned image; a line with
   `"worker": "bond_live_daily"` means the variables had not propagated to that
   run.
4. Read the result JSON from the log (below). Exit 0 with `state` `published`
   (or `published_no_defaults`) is the only success; `precondition_failed`,
   `latest_month_unwitnessed`, `gate_failed` (incl. `pointer_moved`,
   `anchor_drift`) exit 1 and wrote nothing (step 3 of the order below is the
   one exception: the idempotent DDL replay is committed before the digest
   comparison).
5. REMOVE `BOND_IMPLIED_RATING_FORCE_REPUBLISH` and every
   `BOND_IMPLIED_RATING_EXPECT_*` variable, restore `WORKER=bond_live_daily`,
   and verify the variable set is back to its pre-step-2 state. A forced-apply
   override left in place would republish on every 07:30 UTC cron (the CAS and
   the digest expectations would refuse a second identical run, but the DDL
   replay and the lock on the ledger would still happen each time).

The cron can fire during the override (same caveat as §2.2): keep the window
short and revert immediately after reading the result.

What it does, in order (`src/workers/bond_market_implied_rating.run`):

0. (before any connection) parse the `BOND_IMPLIED_RATING_EXPECT_*`
   variables; a malformed value refuses (`expected_<field>_malformed`); a
   forced run without the rows digest and input fingerprint refuses
   (`expected_*_required`). Every result from here on, publication or refusal,
   carries the runtime `build_manifest`;
1. gates (panel relations, current validated panel, mirror provenance), then
   the `current_pointer` / `panel_publication_id` expectations — refuse with
   `precondition_failed` and no write on mismatch;
2. one snapshot read + fingerprint; the `input_fingerprint` expectation —
   still before any DDL;
3. **`install_schema`: replays `schemas/sec_derived_publications.sql` and
   `schemas/bond_market_implied_rating_v1.sql` (idempotent `CREATE … IF NOT
   EXISTS` / `CREATE OR REPLACE`, plus the narrow `app_runtime` revokes) and
   COMMITS it before the build — i.e. the apply replays and commits the
   idempotent DDL BEFORE the `rows_digest` comparison of step 4.** It briefly
   takes `AccessExclusiveLock` on the shared derived-publication ledger. The
   authorization for this step must explicitly cover that DDL replay; it is
   not optional in `run()`.
4. the full-history build (the same pure function the children ran), then the
   `rows_digest` expectation and the latest-month guard — both refuse before
   `materialize`;
5. `materialize`: ledger row (`sec_derived_publications`, anchored to the
   latest validated raw run/package), build pin
   (`bond_market_implied_rating_v1_builds`), rows, `sec_validate_derived_publication`,
   then the pointer CAS (`sec_set_current_derived_publication`; refuses if the
   pointer moved since step 1).

Expected JSON: `state` `published` (or `published_no_defaults`),
`reused_publication: false`, `rows_digest`/`input_fingerprint` equal to the
receipt, `policy_digest` `4b752a3f…`, `code_revision` = the deployed 40-hex
sha, `latest_month_witnessed_count > 0`, `build_manifest` present. Exit 0.
Never set `CODE_REVISION` as a permanent service variable
(`bond-live-daily.md` §3a).

The local CLI form (`python -m scripts.backfill_bond_market_implied_rating
--apply --expect-rows-digest … --expect-input-fingerprint … [--expect-panel-publication …
--expect-current-pointer …]`) enforces the same digest binding (refused before
the DSN is resolved without both values) but runs on the OPERATOR's stack, not
the production image, and so mints a different `code_revision`; it is not the
Phase 4 procedure.

### 2.4 Read-only verification by UUID — OWNER AUTHORIZATION REQUIRED (read-only)

```sh
python -m scripts.verify_bond_market_implied_rating_publication \
  --publication-id <publication_id from 2.3> \
  --expect-rows-digest <receipt rows_digest> \
  --expect-policy-digest 4b752a3fc5d5222b398f1e2a073b7c428b068e77968f8fb79edd20957202a08a \
  --expect-current
```

Reads the build pin, every stored row of that UUID (ordered `month, cusip_id`)
and the pointer on a read-only session, recomputes `rows_digest` with
`src.bonds.implied_rating.rows_digest` and refuses on any divergence
(`rows_digest_not_reproduced`, `row_count_mismatch`, `default_counts_mismatch`,
`not_validated`, `not_current_pointer`, …). Exit 0 and `"ok": true` is the pass.
This verifier is a read-only CLI, not a worker: it runs from an operator
machine with an authorized read-only DSN (the digest is a canonical hash of
the STORED rows, so it does not depend on the numeric stack it runs on).
Cross-check in SQL:

```sql
SELECT publication_id FROM sec_derived_current_pointers
 WHERE product = 'bond_market_implied_rating_v1';
SELECT publication_id, panel_publication_id, policy_digest, code_revision,
       panel_last_closed_month, row_count, rows_digest, l_anchor,
       d_confirmed_count, d_candidate_count, created_at
  FROM bond_market_implied_rating_v1_builds ORDER BY created_at DESC LIMIT 3;
```

**Light EL reader grant — already executed.** The Light EL reader
(`app_runtime`) needs table-level `SELECT` on `bond_market_implied_rating_v1_builds`
(it already reads the rows table `bond_market_implied_rating_v1`). The DDL
deliberately does not grant it ("the `app_runtime` SELECT grant is applied
operationally", `schemas/bond_market_implied_rating_v1.sql`); it only revokes
write privileges. The grant
`GRANT SELECT ON public.bond_market_implied_rating_v1_builds TO app_runtime`
was executed on **2026-10-01 17:30 -03:00**, owner-authorized, and read back
as `app_runtime` (the builds table was readable afterwards). Nothing remains
to grant; keep only the verification, which must return `true, true`:

```sql
SELECT has_table_privilege('app_runtime', 'bond_market_implied_rating_v1_builds', 'SELECT'),
       has_table_privilege('app_runtime', 'bond_market_implied_rating_v1', 'SELECT');
```

### 2.5 Light activation — OWNER AUTHORIZATION REQUIRED (Light environment)

In the Light environment, per the Light owner decisions document:

- `BOND_EL_RATING_POLICY_DIGEST=4b752a3fc5d5222b398f1e2a073b7c428b068e77968f8fb79edd20957202a08a`
  — the Light reader must bind to exactly this policy digest and refuse any
  other current publication;
- `USE_BOND_EL_HAZARD` — the consumer switch, set to the value the Light
  decisions prescribe, only after 2.4 passed (the grant is already in place,
  §2.4).

Verify on Light's own evidence path (its API/DB-first checks) that the current
publication it reads is the UUID from 2.3. This runbook does not operate Light.

## 3. Rollback notes

- **Image/config (2.1):** switch the config path back to the root `railway.toml`
  and redeploy; the daily service returns to Nixpacks. The published data is
  unaffected.
- **Publication (2.3):** rows and ledger rows are immutable (write and delete
  guards); a publication is never deleted, only unpointed. If the previous
  publication carries the same `panel_last_closed_month`, an authorized
  operator can re-point:
  `SELECT sec_set_current_derived_publication('bond_market_implied_rating_v1', '<previous uuid>');`
  The ledger refuses an as-of regression, so a previous publication of an
  OLDER month cannot be re-pointed — the rollback is then on the consumer side
  (below) until a corrected publication is built. `BOND_IMPLIED_RATING_ENABLED`
  stays off, so nothing republishes automatically; the next deploy changes the
  code revision and a later authorized digest-bound apply (§2.3) mints a new
  identity.
- **Variable override (2.2/2.3):** if a run was triggered with the wrong
  `WORKER` or the override was left in place, restore `WORKER=bond_live_daily`
  and remove the force/expectation variables first; the next 07:30 UTC cron
  then runs the daily chain again. A check run wrote nothing; an apply run is
  covered by the publication rollback above.
- **DDL replay (2.3 step 3):** idempotent and additive; there is nothing to roll
  back, which is exactly why the authorization must cover it beforehand.
- **Light (2.5):** unset `USE_BOND_EL_HAZARD` (or revert
  `BOND_EL_RATING_POLICY_DIGEST`) in the Light environment; Light's reader is
  fail-closed on a digest mismatch by design.
- **Grant (2.4, executed 2026-10-01):** `REVOKE SELECT ON TABLE bond_market_implied_rating_v1_builds FROM app_runtime;`
  if the activation is abandoned (owner-authorized; Light's reader then
  fails closed on the builds table).

## 4. Open risks the owner decides on

- The determinism receipt proves two fresh processes on the SAME host/image
  agree; it does not prove the production digest equals the local round-003
  digest (the 637 last-bit cells). The owner adjudicates with both manifests.
- The latest-month guard is fail-closed on a dark last month and is a separate
  commit the owner may drop; without it, a dark September is published.
- The determinism export is a sha256-pinned pickle of the exact in-memory
  frame (types preserved by construction, proven by the fingerprint roundtrip);
  it is written to the container's ephemeral disk and must be sized for ~3M rows.
- `pip install --require-hashes` pins wheels, not pip itself; the base image's
  pip performs the install. The image digest pin covers that pip.
- The image runs as root, as Nixpacks does today; dropping privileges is a
  separate decision.
