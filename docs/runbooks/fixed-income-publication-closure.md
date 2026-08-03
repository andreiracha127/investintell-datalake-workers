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
