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

Every fact relation references `sec_derived_publications(publication_id)` with
`ON DELETE RESTRICT`, so the parent row cannot go until its children do — and a
cancelled build usually *did* write facts before it was killed. Delete children
first, in this order:

```sql
-- 2. remove, guards down only for the duration of the transaction
BEGIN;
ALTER TABLE nport_fixed_income_feature_builds DISABLE TRIGGER nport_fixed_income_feature_build_write_guard;
ALTER TABLE nport_fixed_income_publication_manifests DISABLE TRIGGER nport_fixed_income_manifest_write_guard;
ALTER TABLE nport_fixed_income_publication_closures DISABLE TRIGGER nport_fixed_income_closure_write_guard;
ALTER TABLE nport_fixed_income_features DISABLE TRIGGER nport_fixed_income_features_write_guard;

-- The nine fact relations, all guarded by nport_fixed_income_v2_fact_write_guard
-- except nport_fixed_income_features, which has its own.
ALTER TABLE nport_fixed_income_key_rate_sensitivities_v2       DISABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_credit_spread_sensitivities_v2  DISABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_balance_sheet_primitives_v2     DISABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_debt_flag_features_v2           DISABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_repo_lending_primitives_v2      DISABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_repo_lending_reported_flags_v2  DISABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_metric_coverage_v2              DISABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_metric_coverage_snapshot_v1     DISABLE TRIGGER nport_fixed_income_v2_fact_write_guard;

-- 2a. facts first (if step 2 already TRUNCATEd the coverage table, its DELETE
--     is a no-op; the others are small for an abandoned build). Batch by ctid
--     as in step 4 if any of them turns out to hold millions of rows.
DELETE FROM nport_fixed_income_metric_coverage_snapshot_v1     WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_metric_coverage_v2              WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_repo_lending_reported_flags_v2  WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_repo_lending_primitives_v2      WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_debt_flag_features_v2           WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_balance_sheet_primitives_v2     WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_credit_spread_sensitivities_v2  WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_key_rate_sensitivities_v2       WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_features                        WHERE publication_id = ANY(:ids);

-- 2b. then the identity/provenance rows
DELETE FROM nport_fixed_income_publication_closures  WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_publication_manifests WHERE publication_id = ANY(:ids);
DELETE FROM nport_fixed_income_feature_builds        WHERE publication_id = ANY(:ids);

ALTER TABLE nport_fixed_income_features ENABLE TRIGGER nport_fixed_income_features_write_guard;
ALTER TABLE nport_fixed_income_key_rate_sensitivities_v2       ENABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_credit_spread_sensitivities_v2  ENABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_balance_sheet_primitives_v2     ENABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_debt_flag_features_v2           ENABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_repo_lending_primitives_v2      ENABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_repo_lending_reported_flags_v2  ENABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_metric_coverage_v2              ENABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_metric_coverage_snapshot_v1     ENABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
ALTER TABLE nport_fixed_income_publication_closures  ENABLE TRIGGER nport_fixed_income_closure_write_guard;
ALTER TABLE nport_fixed_income_publication_manifests ENABLE TRIGGER nport_fixed_income_manifest_write_guard;
ALTER TABLE nport_fixed_income_feature_builds ENABLE TRIGGER nport_fixed_income_feature_build_write_guard;

-- 2c. finally the parent. The publication delete guard stays ON throughout: it
-- refuses anything validated or current, which is the check that actually
-- protects you here.
DELETE FROM sec_derived_publications
WHERE publication_id = ANY(:ids) AND lifecycle_state = 'prepared';
COMMIT;
```

If you would rather not touch guards at all, the equivalent proof is to select
only ids with no children and delete just those:

```sql
SELECT p.publication_id FROM sec_derived_publications p
WHERE p.product = 'nport_fixed_income_features_v1'
  AND p.lifecycle_state = 'prepared'
  AND NOT EXISTS (SELECT 1 FROM nport_fixed_income_features f WHERE f.publication_id = p.publication_id)
  AND NOT EXISTS (SELECT 1 FROM nport_fixed_income_metric_coverage_v2 m WHERE m.publication_id = p.publication_id)
  AND NOT EXISTS (SELECT 1 FROM nport_fixed_income_feature_builds b WHERE b.publication_id = p.publication_id)
  AND NOT EXISTS (SELECT 1 FROM nport_fixed_income_publication_manifests x WHERE x.publication_id = p.publication_id);
```

`ALTER TABLE ... DISABLE TRIGGER` takes an ACCESS EXCLUSIVE lock and is
transactional, so an abort restores the guards. Do not leave the transaction
open while you go read something.

## Step 4 — if a promoted publication does exist

Then `TRUNCATE` is off the table, and so is "delete everything that is not
current".

**A validated publication keeps its rows, even when it is no longer current.**
Its manifest is immutable and its storage closure attests an exact count per
relation; deleting its facts would leave both attesting rows that no longer
exist, so a deliberate rollback to that publication would promote a contract
relation with the data missing — and the closure would certify it. There is no
retention policy for superseded fixed-income publications yet; until there is
one (and it must retire the closure and manifest together with the rows), they
stay.

So step 4 removes facts only from publications that were **never validated** —
the same abandoned builds as step 3, just with a promoted publication also
present:

```sql
BEGIN;
ALTER TABLE nport_fixed_income_metric_coverage_v2 DISABLE TRIGGER nport_fixed_income_v2_fact_write_guard;

DELETE FROM nport_fixed_income_metric_coverage_v2 m
WHERE m.ctid = ANY (ARRAY(
  SELECT c.ctid
  FROM nport_fixed_income_metric_coverage_v2 c
  JOIN sec_derived_publications p ON p.publication_id = c.publication_id
  WHERE p.product = 'nport_fixed_income_features_v1'
    AND p.lifecycle_state = 'prepared'      -- never validated, never served
    AND p.validated_at IS NULL
  LIMIT 200000
));

ALTER TABLE nport_fixed_income_metric_coverage_v2 ENABLE TRIGGER nport_fixed_income_v2_fact_write_guard;
COMMIT;
-- repeat until it deletes 0
```

`lifecycle_state = 'prepared' AND validated_at IS NULL` is the predicate that
matters: `prepared` is a state a publication only ever leaves in one direction,
so a row selected by it was never promoted and no closure or manifest attests
it.

Batch it, as above, rather than one statement: a single 45M-row delete holds one
transaction open long enough to block the global `VACUUM`, which is the exact
incident pattern this database has already had. Disk comes back only after
vacuum, not at commit.

Then remove the emptied publications with step 3, which already deletes children
before the parent.

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
