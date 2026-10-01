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
| Determinism check | `--determinism-check` in `scripts/backfill_bond_market_implied_rating.py`, `src/bonds/implied_rating_replay.py` | G1(a) without writing: one read-only snapshot read, an immutable sha256-pinned export, two sequential fresh interpreter processes running the same pure build, compared digests/counts/frames, a JSON receipt. |
| Apply preconditions | `--expect-current-pointer / --expect-panel-publication / --expect-input-fingerprint / --expect-rows-digest` | `--apply` refuses (`precondition_failed`, no write) unless production still matches what the receipt proved. |
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
  unset/false on `bond-live-daily` (default), and no manual
  `WORKER=bond_market_implied_rating` service exists. Both the determinism
  check and `--apply` are run OUTSIDE the daily window (`30 7 * * *` UTC, ~2 h
  run; see `bond-live-daily.md` §2) so the panel pointer does not move under
  the read (`inputs_moved` / `panel_pointer_moved` refuse otherwise).

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
4. Inside the deployed container (`railway ssh` on the service, or the
   equivalent shell on that image), record the stack:

   ```sh
   python docker/bond-live-daily/verify_image.py
   python -c "import json; from src.bonds.build_manifest import collect_build_manifest; print(json.dumps(collect_build_manifest(), indent=1))"
   sha256sum docker/bond-live-daily/requirements.lock
   ```

   The lock sha256 must equal `git show <deployed sha>:docker/bond-live-daily/requirements.lock | sha256sum`
   (on a Linux checkout; the file is `eol=lf` pinned). Keep the manifest JSON
   with the evidence: it is the CPU model/flags and NumPy SIMD baseline/dispatch
   of the production host, which the round-003 last-bit differences are read
   against.
5. The next scheduled daily run must be green with Stage 7 still `disabled`.
   Verify in the tables (`bond-live-daily.md` §4), never on the deploy status.

Rollback of this step: set the config path back to the root `railway.toml`
(Nixpacks) and redeploy. Nothing else depends on the image.

### 2.2 Determinism one-shot on the exact production image — OWNER AUTHORIZATION REQUIRED

Run on the deployed `bond-live-daily` image with the service's `DATABASE_URL`
(e.g. `railway ssh` into the service after the daily run has finished, or a
one-off shell on the same image). It never writes: the session is
`default_transaction_read_only=on` with bounded timeouts, the children get no
DSN, and the code path contains no `install_schema`/`materialize`.

```sh
python -m scripts.backfill_bond_market_implied_rating --determinism-check \
  --work-dir /tmp/implied-rating-replay \
  --receipt /tmp/implied-rating-replay/determinism_receipt.json \
  --expect-panel-publication <bond_panel_app_pointer publication_id> \
  --expect-current-pointer <sec_derived_current_pointers publication_id or omit when absent>
```

Reads the panel's current validated publication and the closed snapshot from
`bond_panel_current_snapshot_v1_mat` through the worker's own gates. Budget:
the production snapshot is ~3M rows; the export is a pickle of that frame (GBs
of RAM in the parent, hundreds of MB on disk), each child is one full-history
build (tens of minutes). `--statement-timeout-seconds` (default 1800) and
`--child-timeout-seconds` (default 21600) bound it.

Exit 0 and `"verdict": "deterministic"` is the only pass. Record from the
receipt, next to the round-003 reference:

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

Same shell as 2.2, outside the daily window, with the values taken from the
receipt the owner accepted:

```sh
python -m scripts.backfill_bond_market_implied_rating --apply \
  --expect-current-pointer <receipt current_pointer, or omit when the receipt says null> \
  --expect-panel-publication <receipt panel_publication_id> \
  --expect-input-fingerprint <receipt input_fingerprint> \
  --expect-rows-digest <receipt rows_digest>
```

What it does, in order (`src/workers/bond_market_implied_rating.run`):

1. gates (panel relations, current validated panel, mirror provenance), then
   the `current_pointer` / `panel_publication_id` expectations — refuse with
   `precondition_failed` and no write on mismatch;
2. one snapshot read + fingerprint; the `input_fingerprint` expectation —
   still before any DDL;
3. **`install_schema`: replays `schemas/sec_derived_publications.sql` and
   `schemas/bond_market_implied_rating_v1.sql` (idempotent `CREATE … IF NOT
   EXISTS` / `CREATE OR REPLACE`, plus the narrow `app_runtime` revokes) and
   COMMITS it before the build.** It briefly takes `AccessExclusiveLock` on the
   shared derived-publication ledger. The authorization for this step must
   explicitly cover that DDL replay; it is not optional in `run()`.
4. the full-history build (the same pure function the children ran), then the
   `rows_digest` expectation and the latest-month guard — both refuse before
   `materialize`;
5. `materialize`: ledger row (`sec_derived_publications`, anchored to the
   latest validated raw run/package), build pin
   (`bond_market_implied_rating_v1_builds`), rows, `sec_validate_derived_publication`,
   then the pointer CAS (`sec_set_current_derived_publication`; refuses if the
   pointer moved since step 1). `BOND_IMPLIED_RATING_FORCE_REPUBLISH=1` is set
   by the CLI for this one call only.

Expected JSON: `state` `published` (or `published_no_defaults`),
`reused_publication: false`, `rows_digest`/`input_fingerprint` equal to the
receipt, `policy_digest` `4b752a3f…`, `code_revision` = the deployed 40-hex
sha, `latest_month_witnessed_count > 0`, `build_manifest` present. Exit 0.
Never set `CODE_REVISION` as a permanent service variable
(`bond-live-daily.md` §3a).

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
Cross-check in SQL:

```sql
SELECT publication_id FROM sec_derived_current_pointers
 WHERE product = 'bond_market_implied_rating_v1';
SELECT publication_id, panel_publication_id, policy_digest, code_revision,
       panel_last_closed_month, row_count, rows_digest, l_anchor,
       d_confirmed_count, d_candidate_count, created_at
  FROM bond_market_implied_rating_v1_builds ORDER BY created_at DESC LIMIT 3;
```

**Light EL reader grant.** The Light EL reader (`app_runtime`) needs
table-level `SELECT` on `bond_market_implied_rating_v1_builds` (and on the rows
table / current view it reads). The DDL deliberately does not grant it ("the
`app_runtime` SELECT grant is applied operationally", `schemas/bond_market_implied_rating_v1.sql`);
it only revokes write privileges. Check, and grant as the table owner if missing:

```sql
SELECT has_table_privilege('app_runtime', 'bond_market_implied_rating_v1_builds', 'SELECT'),
       has_table_privilege('app_runtime', 'bond_market_implied_rating_v1', 'SELECT');
GRANT SELECT ON TABLE bond_market_implied_rating_v1_builds, bond_market_implied_rating_v1 TO app_runtime;
```

### 2.5 Light activation — OWNER AUTHORIZATION REQUIRED (Light environment)

In the Light environment, per the Light owner decisions document:

- `BOND_EL_RATING_POLICY_DIGEST=4b752a3fc5d5222b398f1e2a073b7c428b068e77968f8fb79edd20957202a08a`
  — the Light reader must bind to exactly this policy digest and refuse any
  other current publication;
- `USE_BOND_EL_HAZARD` — the consumer switch, set to the value the Light
  decisions prescribe, only after 2.4 passed and the grant is in place.

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
  code revision and a later authorized `--apply` mints a new identity.
- **DDL replay (2.3 step 3):** idempotent and additive; there is nothing to roll
  back, which is exactly why the authorization must cover it beforehand.
- **Light (2.5):** unset `USE_BOND_EL_HAZARD` (or revert
  `BOND_EL_RATING_POLICY_DIGEST`) in the Light environment; Light's reader is
  fail-closed on a digest mismatch by design.
- **Grant (2.4):** `REVOKE SELECT ON TABLE bond_market_implied_rating_v1_builds FROM app_runtime;`
  if the activation is abandoned.

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
