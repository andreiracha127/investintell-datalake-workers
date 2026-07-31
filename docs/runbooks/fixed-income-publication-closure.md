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
-- 1. The new objects exist. Expect one closure table and eight truncate guards.
SELECT to_regclass('nport_fixed_income_publication_closures') IS NOT NULL AS closure_table,
       (SELECT count(*) FROM pg_trigger
        WHERE tgname = 'nport_fixed_income_truncate_guard' AND NOT tgisinternal) AS truncate_guards;

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
