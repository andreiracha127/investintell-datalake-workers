# Reclaiming the per-position coverage table

**Status:** proposal for an operator to execute. Nothing here runs automatically,
and none of it was run against production while writing it.

## Why

`nport_fixed_income_metric_coverage_v2` was written at one row per holding per
metric key. Measured in production:

| fact | value |
|---|---|
| rows | 45.6M |
| table | 20 GB |
| primary key | 18 GB |
| dead tuples from the cancelled debut builds | 20.1M |
| rows per snapshot | ~173,716 |
| the six relations carrying actual values, combined | ~260 MB |

The large majority of those rows are absence markers
(`availability_state <> 'reported_numeric'`). The debut build was cancelled after
2h+ of CPU, dominated by writing them.

The builder now persists only rows that carry a value, and counts absence in
`nport_fixed_income_metric_coverage_snapshot_v1` (46 rows per snapshot:
`source_row_count`, `reported_row_count`, `missing_reason_counts` per metric).

## What reads the per-position grain

Nothing live. The app reads the rollup only —
`backend/app/repositories/fixed_income_analytics.py` selects from
`sec_current_nport_fixed_income_metric_coverage_snapshot_v1`. The base relation
is still declared by the frozen producer contract
(`nport-fixed-income-features/v2`, digest `sha256:797332a9…be563`), so the table
must keep existing: **do not `DROP` it.** Dropping it would break the contract
digest the app verifies at startup, and the relation remains genuinely useful —
it is the position-level provenance of every reported value.

What can go is its *content*: rows belonging to publications that were never
promoted.

## Step 1 — prove nothing promoted is at stake (must return zero rows)

```sql
SELECT p.publication_id, p.lifecycle_state,
       c.publication_id IS NOT NULL AS is_current,
       (SELECT count(*) FROM nport_fixed_income_metric_coverage_v2 m
         WHERE m.publication_id = p.publication_id) AS coverage_rows
FROM sec_derived_publications p
LEFT JOIN sec_derived_current_pointers c
       ON c.product = p.product AND c.publication_id = p.publication_id
WHERE p.product = 'nport_fixed_income_features_v1'
  AND p.lifecycle_state = 'validated';
```

If this returns rows, stop: a promoted publication exists and its coverage rows
are being served. Fall through to step 4 instead.

## Step 2 — reclaim (the fast path, only if step 1 was empty)

Everything in the table then belongs to publications the builds never promoted.
`TRUNCATE` is guarded on purpose — a publication relation cannot be truncated by
accident — so the guard has to be removed and put back deliberately, inside one
transaction:

```sql
BEGIN;
DROP TRIGGER nport_fixed_income_truncate_guard ON nport_fixed_income_metric_coverage_v2;
TRUNCATE nport_fixed_income_metric_coverage_v2;
CREATE TRIGGER nport_fixed_income_truncate_guard
BEFORE TRUNCATE ON nport_fixed_income_metric_coverage_v2
FOR EACH STATEMENT EXECUTE FUNCTION nport_fixed_income_truncate_guard();
COMMIT;
```

`TRUNCATE` returns the ~38 GB immediately (table + index), with no `VACUUM FULL`
and no long transaction — the failure mode that already cost a day on this
database.

## Step 3 — the abandoned publication rows (optional, and guarded)

The cancelled builds also left `prepared` publications with their build-identity
and manifest rows. **You almost certainly do not need to remove them.** They are
one row each, they cost no measurable disk, and they are the provenance record of
what was attempted. Leaving them costs nothing; the next run creates a new
publication id and ignores them.

If you do want them gone, note that a plain `DELETE` **fails**: both
`nport_fixed_income_feature_builds` and `nport_fixed_income_publication_manifests`
carry `BEFORE INSERT OR UPDATE OR DELETE` guards that raise on anything that is
not an `INSERT` ("feature build identity is immutable" / "publication manifest is
immutable"). That is deliberate — a promoted publication must not be able to lose
its identity or its manifest.

There is no sanctioned function for removing them, so the only correct procedure
is the same deliberate, transactional guard removal used for the truncate in step
2 — and it must be scoped to the publications you listed and confirmed:

```sql
-- 1. list them and confirm every id is 'prepared' and NOT current
SELECT p.publication_id, p.prepared_at, p.lifecycle_state,
       c.publication_id IS NOT NULL AS is_current
FROM sec_derived_publications p
LEFT JOIN sec_derived_current_pointers c
       ON c.product = p.product AND c.publication_id = p.publication_id
WHERE p.product = 'nport_fixed_income_features_v1'
  AND p.lifecycle_state = 'prepared'
ORDER BY p.prepared_at;
```

```sql
-- 2. remove, guards down only for the duration of the transaction
BEGIN;
ALTER TABLE nport_fixed_income_feature_builds DISABLE TRIGGER nport_fixed_income_feature_build_write_guard;
ALTER TABLE nport_fixed_income_publication_manifests DISABLE TRIGGER nport_fixed_income_manifest_write_guard;
ALTER TABLE nport_fixed_income_publication_closures DISABLE TRIGGER nport_fixed_income_closure_write_guard;

DELETE FROM nport_fixed_income_feature_builds        WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_publication_manifests WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_publication_closures  WHERE publication_id = ANY(:ids);

ALTER TABLE nport_fixed_income_feature_builds ENABLE TRIGGER nport_fixed_income_feature_build_write_guard;
ALTER TABLE nport_fixed_income_publication_manifests ENABLE TRIGGER nport_fixed_income_manifest_write_guard;
ALTER TABLE nport_fixed_income_publication_closures ENABLE TRIGGER nport_fixed_income_closure_write_guard;

-- The publication delete guard stays ON: it refuses anything validated or
-- current, which is the check that actually protects you here.
DELETE FROM sec_derived_publications
WHERE publication_id = ANY(:ids) AND lifecycle_state = 'prepared';
COMMIT;
```

`ALTER TABLE ... DISABLE TRIGGER` takes an ACCESS EXCLUSIVE lock and is
transactional, so an abort restores the guards. Do not leave the transaction
open while you go read something.

## Step 4 — if a promoted publication does exist

Delete only the superseded ones and let autovacuum reclaim. The row guard on
this table rejects `UPDATE`/`DELETE` too, so it needs the same transactional
guard removal as step 3:

```sql
BEGIN;
ALTER TABLE nport_fixed_income_metric_coverage_v2 DISABLE TRIGGER nport_fixed_income_v2_fact_write_guard;

DELETE FROM nport_fixed_income_metric_coverage_v2 m
WHERE m.publication_id NOT IN (
  SELECT publication_id FROM sec_derived_current_pointers
  WHERE product = 'nport_fixed_income_features_v1'
)
AND m.ctid = ANY (ARRAY(
  SELECT ctid FROM nport_fixed_income_metric_coverage_v2
  WHERE publication_id NOT IN (
    SELECT publication_id FROM sec_derived_current_pointers
    WHERE product = 'nport_fixed_income_features_v1'
  )
  LIMIT 200000
));

ALTER TABLE nport_fixed_income_metric_coverage_v2 ENABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
COMMIT;
-- repeat until it deletes 0
```

Batch it, as above, rather than one statement: a single 45M-row delete holds one
transaction open long enough to block the global `VACUUM`, which is the exact
incident pattern this database has already had. Disk comes back only after
vacuum, not at commit.

## Step 5 — republish

Run `dl-nport-fixed-income` (worker `nport_fixed_income_serving`). Expected
shape of the new build:

- coverage rows written: only the reported ones — order 10^4 per snapshot
  instead of 173,716, i.e. roughly the ratio of reported to evaluated positions;
- rollup rows: 46 per fund snapshot;
- the six value relations: unchanged, ~260 MB;
- runtime: minutes, since the dominant write disappears.

Verify with the worker's own JSON (`counts` now includes
`nport_fixed_income_metric_coverage_snapshot_v1`) and then:

```sql
SELECT count(*) FROM sec_current_nport_fixed_income_metric_coverage_snapshot_v1;
SELECT sum(source_row_count), sum(reported_row_count)
FROM sec_current_nport_fixed_income_metric_coverage_snapshot_v1;
```

The two sums are the honesty check: `source_row_count` is what the old absence
rows would have counted, `reported_row_count` is what is materialized.
