# Fixed-income publication closure — apply and verify

## What changed

An idempotent replay of an already-published fixed-income artifact re-proved
storage by running `count(*)` over each of the eight target relations — one of
them `nport_fixed_income_metric_coverage_v2`, **45.6M rows in production** — after
the manifest bytes, the lifecycle state and the current pointer had *already* been
verified.

`nport_fixed_income_publication_closures` records, per publication, the manifest
hash and the per-relation counts observed **while the publication was already
validated**. A replay re-proves storage by reading that one indexed row.

## Why that is equivalent, not weaker

The eight relations are frozen for a validated publication, by the database:

| write | what stops it |
|---|---|
| `UPDATE` / `DELETE` | `nport_fixed_income_*_write_guard`: `IF TG_OP <> 'INSERT' THEN RAISE` |
| `INSERT` | same guards: require the parent publication in `lifecycle_state = 'prepared'` |
| `TRUNCATE` | `nport_fixed_income_truncate_guard` (new; row triggers do not see TRUNCATE) |

A closure is itself immutable and can only be recorded for a validated
publication whose manifest hash matches the published one
(`nport_fixed_income_closure_write_guard`).

Fail-closed is preserved end to end: no closure ⇒ full recount (then record one,
so the cost is paid once); a closure that disagrees with the manifest ⇒
`PublicationConflictError`; lifecycle/pointer checks are untouched and still run
*before* the closure is consulted.

## Apply (operator)

`schemas/nport_fixed_income_features.sql` is idempotent (`CREATE TABLE IF NOT
EXISTS`, `CREATE OR REPLACE FUNCTION`, `DROP TRIGGER IF EXISTS` + `CREATE
TRIGGER`). Re-applying it is the migration:

```bash
psql "$DATALAKE_DSN" -v ON_ERROR_STOP=1 -f schemas/nport_fixed_income_features.sql
```

Existing publications get no closure from the migration — deliberately. The first
replay of each one recounts (exactly as today) and records the closure, so every
replay after that is O(1). Nothing needs backfilling.

## Verify

```sql
-- 1. The new objects exist. Expect one closure table, eight truncate guards, and
--    exactly one coverage-rollup backfill guard.
--
--    That last count is not cosmetic. The backfill guard is what makes the rollup
--    repair below possible at all: while it is missing, the per-row check it
--    replaced still refuses the repair's second row with the message
--    "N-PORT fixed-income coverage rollup is already published for this
--    publication" -- the SAME message a genuinely closed rollup produces. An
--    operator who runs the repair without re-applying this DDL therefore sees the
--    old bug and concludes the repair is impossible. Check the trigger BEFORE
--    concluding anything from that message.
SELECT to_regclass('nport_fixed_income_publication_closures') IS NOT NULL AS closure_table,
       (SELECT count(*) FROM pg_trigger
        WHERE tgname = 'nport_fixed_income_truncate_guard' AND NOT tgisinternal) AS truncate_guards,
       (SELECT count(*) FROM pg_trigger
        WHERE tgname = 'nport_fixed_income_coverage_rollup_backfill_guard'
          AND NOT tgisinternal) AS rollup_backfill_guard;  -- expect 1

-- 2. Every recorded closure still matches real storage. Expect zero rows.
SELECT c.publication_id, r.relation, (c.relation_counts->>r.relation)::bigint AS closed, r.actual
FROM nport_fixed_income_publication_closures c
CROSS JOIN LATERAL (
    SELECT 'nport_fixed_income_metric_coverage_v2' AS relation,
           (SELECT count(*) FROM nport_fixed_income_metric_coverage_v2 t
            WHERE t.publication_id = c.publication_id) AS actual
    UNION ALL SELECT 'nport_fixed_income_features',
           (SELECT count(*) FROM nport_fixed_income_features t
            WHERE t.publication_id = c.publication_id)
    UNION ALL SELECT 'nport_fixed_income_key_rate_sensitivities_v2',
           (SELECT count(*) FROM nport_fixed_income_key_rate_sensitivities_v2 t
            WHERE t.publication_id = c.publication_id)
    UNION ALL SELECT 'nport_fixed_income_credit_spread_sensitivities_v2',
           (SELECT count(*) FROM nport_fixed_income_credit_spread_sensitivities_v2 t
            WHERE t.publication_id = c.publication_id)
    UNION ALL SELECT 'nport_fixed_income_balance_sheet_primitives_v2',
           (SELECT count(*) FROM nport_fixed_income_balance_sheet_primitives_v2 t
            WHERE t.publication_id = c.publication_id)
    UNION ALL SELECT 'nport_fixed_income_debt_flag_features_v2',
           (SELECT count(*) FROM nport_fixed_income_debt_flag_features_v2 t
            WHERE t.publication_id = c.publication_id)
    UNION ALL SELECT 'nport_fixed_income_repo_lending_primitives_v2',
           (SELECT count(*) FROM nport_fixed_income_repo_lending_primitives_v2 t
            WHERE t.publication_id = c.publication_id)
    UNION ALL SELECT 'nport_fixed_income_repo_lending_reported_flags_v2',
           (SELECT count(*) FROM nport_fixed_income_repo_lending_reported_flags_v2 t
            WHERE t.publication_id = c.publication_id)
) r
WHERE (c.relation_counts->>r.relation)::bigint IS DISTINCT FROM r.actual;

-- 3. The closure agrees with the published manifest. Expect zero rows.
SELECT c.publication_id
FROM nport_fixed_income_publication_closures c
JOIN nport_fixed_income_publication_manifests m USING (publication_id)
WHERE c.manifest_sha256 <> m.manifest_sha256;
```

Query 2 is the full audit — the exact work the replay no longer does. Run it on
demand (restore, DR drill, quarterly audit), never per replay.

## Family completeness: what to do when the job stops publishing

Since 2026-08-02 a publication of `nport_fixed_income_features_v1` may only
become `current` if it did not empty a relation the publication being served has
rows in. Two things enforce it, and both can stop a scheduled run:

| symptom | who refused | meaning |
|---|---|---|
| job exits **0** with `{"state":"no_source","reason":"pinned_raw_evidence_pruned",…}` and a **WARNING** in the Cloud Run log | `src/workers/nport_fixed_income_serving.py` | the pinned N-PORT raw rows are gone, so the raw-derived relations would publish empty |
| job exits **non-zero** with `N-PORT fixed-income publication … regressed to zero rows` | `nport_fixed_income_assert_publication_complete()` | the build ran but a served relation came out empty for another reason |

**`pinned_raw_evidence_pruned` is a standing state, not a blip.** Production
prunes `nport_raw_rows` once a run is attested (`schemas/nport_raw.sql`), and the
immutability guard forbids re-inserting rows into an attested run — so the
in-database producer *cannot* rebuild the four raw-derived relations for a run
whose evidence is gone. The weekly `dl-nport-fixed-income` job will report this
every week, green, until a **new ingestion run** produces a new holdings
publication. Green is deliberate: the exit code drives the scheduler's retry
semantics, and there is nothing to retry. The WARNING line carries
`source_publication_id`, `source_run_id`, `as_of` and the pruned relations, so
the standstill is legible in the logs rather than silent.

Operator action while that state holds:

1. **Do nothing to the pointer.** The previously published, complete publication
   stays `current` and stays served — that is the whole point of the refusal.
2. To publish new content, use the **offline artifact route**
   (`scripts/materialize_nport_fixed_income_local.py`), which builds against a
   local PostgreSQL where the raw snapshot is resident and COPYs the attested
   payload into production. That route enforces the same completeness gate.
3. Or wait for the next ingestion run: a new validated
   `sec_nport_holdings_v2` publication moves the pinned `source_run_id` to a run
   whose raw rows are still resident, and the worker builds normally.

To publish anyway — only when the emptiness is a verified, deliberate fact:

- `publish_artifact(..., allow_relation_regression=True)`
- or `NPORT_FI_ALLOW_RELATION_REGRESSION=1` in the environment (both routes)

The override logs a `WARNING` from PostgreSQL naming the relations it let
through. It is a human assertion; never set it on a schedule.

### Prerequisite for the artifact route

`publish_artifact` calls `nport_fixed_income_assert_publication_complete()`, so
the target database must have `schemas/nport_fixed_income_features.sql` applied.
The `publish` subcommand of `scripts/materialize_nport_fixed_income_local.py`
applies it (idempotently) before publishing, mirroring what the in-database
worker does; a restore that bypasses the CLI must apply the DDL first or it will
fail with `42883 undefined_function`.

## SEC API fund-level recovery — recover without publishing

When the validated holdings publication is intact but its historical DERA raw
evidence has been pruned, `nport_fixed_income_secapi_recovery` can recover only
the fund-level reported facts and interest-rate-risk rows from SEC API. It is a
sidecar recovery: it does **not** write `nport_raw_rows`, does **not** mutate a
derived publication, and does **not** move a current pointer. A successful
recovery is evidence for a later, separately authorized source-selection and
publication step; it is never publication by implication.

The worker's expected accession set comes exclusively from
`nport_holdings_snapshot_identity_v1` for the explicit holdings publication.
It makes exactly one accession-scoped `FormNportApi` call per pending accession,
stores only compact fund/risk projections, and rejects any `invstOrSecs`
position payload from those projections.

### Operator canary

Apply the idempotent sidecar DDL as an explicit release migration. The recurring
worker only verifies these objects and fails if they are absent; it never takes
schema-changing locks during a recovery run:

```bash
psql "$DATALAKE_DSN" -v ON_ERROR_STOP=1 -f schemas/nport_fixed_income_secapi_sidecars_v1.sql
```

Create or update a dedicated Railway service to use
`railway.nport-fixed-income-secapi-recovery.toml`. It is a one-shot service:
`restartPolicyType = "never"`, no schedule, and no web endpoint. Set these
service-only variables, with a narrowly bounded canary:

```text
WORKER=nport_fixed_income_secapi_recovery
SEC_API_IO_KEY=<Railway secret; never print it>
NPORT_SECAPI_SOURCE_HOLDINGS_PUBLICATION_ID=<validated holdings publication UUID>
NPORT_SECAPI_SOURCE_RUN_ID=<its source run UUID>
NPORT_SECAPI_MAX_ACCESSIONS=1
NPORT_SECAPI_MAX_API_CALLS=1
NPORT_SECAPI_REQUEST_INTERVAL_SECONDS=0.1
```

`0.1` seconds caps this worker at 10 requests/second, below the currently
published 20 requests/second floor for paid Query API plans. Use a slower value
if the account contract is more restrictive; never omit the explicit pacing
value on Railway.

Deploying the service runs that one bounded batch. A green deployment only means
the job exited; inspect the returned JSON and the database. Never set an empty
or continuous cron for this worker.

### v2 fallback overlay for Form exact-zero terminals

Use this only after the v1 worker has recorded immutable `terminal_error`
accessions for the exact publication/run. Apply the explicit release migration
first; the worker is verify-only and never performs DDL:

```bash
psql "$DATALAKE_DSN" -v ON_ERROR_STOP=1 -f schemas/nport_fixed_income_secapi_fallback_v2.sql
```

Create a separate one-shot Railway service using
`railway.nport-fixed-income-secapi-fallback.toml` and set:

```text
WORKER=nport_fixed_income_secapi_fallback
SEC_API_IO_KEY=<Railway secret; never print it>
NPORT_SECAPI_SOURCE_HOLDINGS_PUBLICATION_ID=<validated holdings publication UUID>
NPORT_SECAPI_SOURCE_RUN_ID=<the same source run UUID>
NPORT_SECAPI_FALLBACK_MAX_ACCESSIONS=1
NPORT_SECAPI_FALLBACK_MAX_API_CALLS=3
NPORT_SECAPI_FALLBACK_REQUEST_INTERVAL_SECONDS=0.1
```

One fallback accession reserves exactly three paced calls: FormNportApi,
QueryApi, and RenderApi. It begins only when the remaining budget can pay for
all three. The stored overlay contains only their separate hashes, the canonical
SEC document URL, and compact fund/rate projections; raw XML and positions are
never stored. `partial`, `failed`, `conflict`, and `locked` exit non-zero. A
dry-run verifies schema and reports the immutable terminal candidate set without
constructing an API client or writing overlay rows.

Before any later serving authorization, require this exact gate to report
`"ready": true`; the fallback worker itself does not publish or move a pointer:

```sql
SELECT nport_fixed_income_secapi_fallback_scope_ready_v2(
  '<holdings-publication-uuid>'::uuid,
  '<source-run-uuid>'::uuid,
  'nport-secapi-fixed-income/v1',
  'nport-secapi-fixed-income/v2',
  'secapi-query-render/v1'
);
```

### v1 recovery completeness gate

After enough bounded runs have completed, the following must return
`"ready": true`, zero missing/non-success/unexpected counts, and matching
declared metric/rate-row counts. Do not run the fixed-income publisher or move
any pointer while it is false.

```sql
SELECT nport_fixed_income_secapi_scope_ready(
  '<holdings-publication-uuid>'::uuid,
  '<source-run-uuid>'::uuid,
  'nport-secapi-fixed-income/v1'
);
```

Treat `conflict`, accession mismatch, malformed payload, authentication failure,
or an unexpected readiness count as terminal until investigated. Retrying a
network/429/5xx failure is bounded inside an accession call; it never broadens
the query to a date, CIK, form, or pagination sweep.

### Controlled activation

Recovery alone changes no served data. Once the scope-readiness gate is true,
obtain explicit owner approval for the source-selection/build path. Compute the
exact `source_hash` reported by the serving worker's readiness probe and set it
as `NPORT_FI_SECAPI_APPROVED_SOURCE_HASH` only for the controlled serving run.
The worker compares the full hash, not a boolean; a new or changed recovery
scope is denied by default. Run the publisher's completeness checks and
publication procedure, verify the resulting manifest and pointer, then remove
that variable. Preserve the previous `nport_fixed_income_features_v1` pointer
until validation finishes. Recovery itself has no automatic hand-off to serving.

## Forcing the old behaviour

- `publish_artifact(..., verify_storage=True)`
- or `NPORT_FI_VERIFY_STORAGE=1` in the environment

Both skip the closure and run the eight counts, then re-record the closure.

## Rollback

```sql
DROP TABLE IF EXISTS nport_fixed_income_publication_closures;  -- cascades its trigger
```

With the table gone, `_recorded_closure` finds nothing and every replay recounts,
exactly as before. Reverting the code commit alone also works. The truncate guards
are safe to leave in place either way.
