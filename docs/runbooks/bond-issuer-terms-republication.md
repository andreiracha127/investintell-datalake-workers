# Runbook — republishing bonds with attributed issuers, reference terms and duration

One-off execution plan for the change that gives the bond product an issuer
column, the terms the filings never report, and two new metrics. After this
runs once, the normal `bond-chain` cron carries it: nothing here is a permanent
new step.

**Production is Railway project `investintell-db`** (`35fa36a3-2641-42b2-b48b-540eac0597c6`,
env `production`). The chain runs as service **`bond-chain`**
(`6fca05e1-d90b-4d34-afe8-bd62fe481b87`, `WORKER=daily_publication_chain`,
`railwayConfigFile=/railway.toml`, cron `0 11 * * *`, `restartPolicy=NEVER` —
**a deploy IS an execution**). The datalake is `market-clean-serial`, reachable
only at `market-clean-serial.railway.internal:5432`.

---

## 0. What this changes, in one paragraph

`bond_security_v1` used to null an issuer name whenever two funds spelled it
differently, and to mark the whole IDENTITY `ambiguous` for the same reason.
Measured 2026-08-07 on the 10,073 curated securities: 384 had an issuer, and
9,688 of the 9,689 gaps carried `identity_reason_code='conflicting_issuer_evidence'`.
The resolver now treats spelling variance as reporting noise (which it is) and
resolves the name by fail-closed consensus, so those securities gain a name
**and flip from `ambiguous` to `resolved`**.

> **This is the load-bearing side effect.** The app derives its serving state
> from `identity_state`: ~9.6k securities move from `degraded` (coverage 50,
> `reason_code='identity_ambiguous'`) to `available` (coverage 100). That is the
> intended outcome, but it is a semantics change — confirm it is wanted before
> step 3.

---

## 1. Pre-flight (read-only, no writes)

Baseline the numbers the run has to move. Run SQL through the datalake with an
LF-terminated file (`-c` fights three levels of quoting, and `\copy` breaks on
CRLF):

```sh
railway ssh --project 35fa36a3-2641-42b2-b48b-540eac0597c6 \
  --environment production --service market-clean-serial -- \
  psql -U postgres -d market -f - < baseline.sql
```

```sql
-- The canonical target set: bond_curated_universe resolved to security_ids
-- through the PUBLISHED aliases (10,206 cusip9 -> 10,073 securities; the 133
-- with no published alias are not in the master and are out of scope).
CREATE TEMP VIEW cur AS
SELECT DISTINCT a.security_id
FROM bond_curated_universe u
JOIN sec_current_bond_security_alias_v1 a
  ON a.alias_kind='cusip9' AND a.alias_value = upper(btrim(u.cusip9));

SELECT count(*) AS curated,
       count(*) FILTER (WHERE s.issuer_name IS NOT NULL) AS with_issuer,
       count(*) FILTER (WHERE s.identity_state='ambiguous') AS ambiguous,
       count(*) FILTER (WHERE s.seniority IS NOT NULL) AS with_seniority
FROM cur c JOIN sec_current_bond_security_v1 s USING (security_id);
-- Expected BEFORE: 10073 | 384 | 9689 | 0

SELECT product, publication_id FROM sec_derived_current_pointers
WHERE product IN ('bond_security_v1','bond_metric_v1','bond_serving_v1');
-- Record these three: they are the rollback targets and the proof of advance.
```

## 2. Load the reference terms (once; before the chain runs)

> **Steps 2 onwards write to production. Do not start them without an explicit
> GO** — the `\copy` creates and fills a table in the market database, and step 3
> advances three current pointers.

The reference bodies live on the operator workstation and the workers have no
access to that filesystem — hence a table, loaded by hand, that the build reads.

**2a. Create the table by hand — and hand it to the worker role.** The security
master's `install_schema` would also create it, but letting it do so means that
build reads an EMPTY table, enriches nothing, and the whole chain run is wasted:
the table is read once, at build time. Do not spend a chain run on that.

```sh
railway ssh --project 35fa36a3-2641-42b2-b48b-540eac0597c6 \
  --environment production --service market-clean-serial -- \
  psql -U postgres -d market -f - < schemas/bond_reference_terms.sql
```

Then, in the same session, **transfer ownership**:

```sql
ALTER TABLE bond_reference_terms OWNER TO worker_writer;
SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname='bond_reference_terms';
-- must print worker_writer, like every other bond product table
```

This is not cosmetic and it is not optional. The worker RE-APPLIES this schema
file on every build, and the file ends in `COMMENT ON TABLE`, which is owner-only
DDL. A table created by `postgres` therefore makes the worker's own
`install_schema` fail closed with `must be owner of table
bond_reference_terms` — measured 2026-08-07, it killed the `pit_update` stage in
0 seconds. The general rule: **any schema a worker applies must be owned by the
role the worker connects as.**

**2b. Flatten the profiles.** (Smoke-run 2026-08-07; these counts are what a
correct run reproduces.)

```sh
python scripts/load_bond_reference_terms.py \
  --profiles C:/Users/andre/Downloads/stage1_osbap_0k_volume_2025/finnhub/finnhub_cache/profiles \
  --out bond_reference_terms.csv --batch-label 2026-08-07
# rows=10073 skipped=0
#   seniority 9883 | callable 9858 | amount_outstanding_mm 10073
#   secured 982 | day_count 983
```

**2c. Load it.** A ~10k-row `\copy` is one small statement; the by-slice
discipline that protects VACUUM is for the multi-million-row backfills, not this.

```sh
railway ssh --project 35fa36a3-2641-42b2-b48b-540eac0597c6 \
  --environment production --service market-clean-serial -- \
  psql -U postgres -d market -c "\copy bond_reference_terms \
    (cusip9,isin,coupon_rate,coupon_type,maturity_date,issue_date,seniority,\
     secured,day_count,payment_frequency,callable,amount_outstanding_mm,batch_label) \
    FROM STDIN WITH (FORMAT csv, HEADER true)" < bond_reference_terms.csv
```

Confirm the load before moving on:

```sql
SELECT count(*) AS rows,
       count(seniority) AS seniority,
       count(callable) AS callable,
       count(amount_outstanding_mm) AS amount_mm,
       count(secured) AS secured,
       count(day_count) AS day_count
FROM bond_reference_terms;
-- Expected: 10073 | 9883 | 9858 | 10073 | 982 | 983
-- ``secured`` is low ON PURPOSE: the flattener emits it only where a debt-type
-- token states collateral in so many words. Seniority is not evidence about it.
```

## 3. Execute the chain (deploy = execution)

`bond-chain` has no `DAILY_CHAIN_STAGES` override, so it runs all eight stages;
the three that matter here are `pit_update` (bond_security_v1),
`materialize` (bond_metric_v1) and `refresh` (bond_serving_v1).

**Set `CODE_REVISION` explicitly first.** The security-master publication id is
`uuid5(product | as_of | code_revision)`: with an unchanged revision and an
unchanged `as_of` the id COLLIDES, the existing publication is already
`validated`, and `materialize` silently re-points instead of rebuilding — the
run goes green and nothing changes. This is the single most likely way for this
execution to appear to succeed while doing nothing.

```sh
railway variables --service bond-chain --environment production \
  --set CODE_REVISION=<merge commit sha>
```

**Then trigger an actual EXECUTION — and note that on this cron service, NO
deployment command is one.** Measured end-to-end on 2026-08-07, three commands,
three different outcomes, only one of which ran the worker:

| command | deployment | ran the chain? |
|---|---|---|
| `railway redeploy --from-source` | `buildOnly: true`, → SUCCESS | **no** |
| `railway redeploy` (plain) | `buildOnly: false`, → SUCCESS | **no** |
| `railway service restart` | reuses the built deployment | **YES** |

A deployment on a `restartPolicy=NEVER` cron service only makes an image
available; the instance sits at `CREATED` and the container is started by the
schedule. `railway service restart` is the `deploymentRestart` that starts it
now. So the two-step recipe is:

```sh
# 1. get the merged code into the service's image (builds; does NOT execute)
railway redeploy --service bond-chain --environment production --from-source --yes
# 2. actually run it (blocks for minutes while the container works)
railway service restart --service bond-chain --environment production
```

The arbiter is step 4, never the deployment status: all three commands above
report SUCCESS, and two of them did nothing. If neither step is convenient the
`0 11 * * *` cron fires on its own — but then the run is unattended, so still
verify in the tables afterwards.

Watch the runtime line the worker prints. Use the **CLI**: the GraphQL
`deploymentLogs` query often returns only "Starting Container".

```sh
railway logs --service bond-chain --environment production
```

Expect `pit_update` to take tens of minutes: the security master rebuilds the
whole published universe (~211k securities) and now also reads the reported LEI
set. **A green Railway status only means the container built and started.**
Every claim below is made against the TABLES.

## 4. Verify — in the tables, never on the dashboard

```sql
-- 4.1 All three pointers must have MOVED (compare with the step-1 record).
SELECT product, publication_id FROM sec_derived_current_pointers
WHERE product IN ('bond_security_v1','bond_metric_v1','bond_serving_v1');

-- 4.2 Issuer coverage and the identity flip.
CREATE TEMP VIEW cur AS
SELECT DISTINCT a.security_id
FROM bond_curated_universe u
JOIN sec_current_bond_security_alias_v1 a
  ON a.alias_kind='cusip9' AND a.alias_value = upper(btrim(u.cusip9));

SELECT count(*) AS curated,
       count(*) FILTER (WHERE s.issuer_name IS NOT NULL) AS with_issuer,
       count(*) FILTER (WHERE s.identity_state='ambiguous') AS ambiguous,
       count(*) FILTER (WHERE s.seniority IS NOT NULL) AS with_seniority,
       count(*) FILTER (WHERE s.terms ? 'callable') AS with_callable
FROM cur c JOIN sec_current_bond_security_v1 s USING (security_id);
-- Expected AFTER: with_issuer ~8,631 (measured 8,335 exact-CUSIP9 + 296 CUSIP6
-- fallback; small drift is legitimate, holdings move between measurement and
-- run). ambiguous ~1 (only the real cross-identity alias collision).
-- with_seniority ~9,883, with_callable ~9,858.

-- 4.3 The abstentions must be REASONED, never silent.
SELECT s.identity_evidence->'issuer_attribution'->>'abstain_reason' AS reason,
       count(*)
FROM cur c JOIN sec_current_bond_security_v1 s USING (security_id)
WHERE s.issuer_name IS NULL
GROUP BY 1 ORDER BY 2 DESC;
-- Expect only: no_consensus, multiple_lei, vehicle_name_at_cusip6,
-- no_named_source. A NULL reason here is a bug, not an abstention.

-- 4.4 The new metrics.
SELECT m.metric_id, m.status, count(*)
FROM cur c JOIN sec_current_bond_metric_v1 m USING (security_id)
WHERE m.metric_id IN ('security_effective_duration','latest_price_pct')
GROUP BY 1,2 ORDER BY 1,2;
-- Expected on the curated set, PROJECTED from the same predicates the worker
-- applies (measured 2026-08-07, before the reference load):
--   latest_price_pct            available 10,072 | no_eligible_price 1
--   security_effective_duration available 10,057 | no_eligible_price 1
--                               terms_insufficient 15 | engine_typed_error 0
-- The 15 are 8 securities with no published coupon and 7 whose coupon_type is
-- not 'Fixed' (4 'None', 3 'Variable') -- the closed form is a fixed-rate bullet
-- and refuses the rest by TYPE, never by guessing. After the reference fills the
-- 8 coupons, expect available 10,065 / terms_insufficient 7.
-- engine_typed_error is 0 here but is NOT dead code: across the full 68k-security
-- price lane, 549 observations sit at ytm >= 0.60 and 4 at <= -0.02, and those
-- would land as yield_out_of_domain BY DESIGN.

-- 4.5 Structural honesty: a value exists iff the row is available.
SELECT count(*) FROM sec_current_bond_metric_v1
WHERE (status='available') <> (value IS NOT NULL);   -- must be 0

-- 4.6 The serving payload actually carries the keys (spot-check before repin).
SELECT payload->>'issuer_name'                  AS issuer,
       payload->>'security_effective_duration'  AS eff_dur,
       payload->>'latest_price_pct'             AS price,
       payload->>'callable'                     AS callable,
       payload->>'amount_outstanding_mm'        AS amt_mm
FROM bond_serving_facts
WHERE publication_id = (SELECT publication_id FROM sec_derived_current_pointers
                        WHERE product='bond_serving_v1')
  AND surface='detail'
LIMIT 5;

SELECT count(*) FILTER (WHERE payload ? 'security_effective_duration') AS with_dur,
       count(*) AS catalog_rows
FROM bond_serving_facts
WHERE publication_id = (SELECT publication_id FROM sec_derived_current_pointers
                        WHERE product='bond_serving_v1')
  AND surface='catalog';
-- with_dur must equal catalog_rows: the key is ALWAYS present, null-honest.
```

## 5. Repin the app

The app reads `bond_serving_facts` **by exact `publication_id`** through its pin
row (`bond_serving_publications` / the `bond_serving_facts_v` view), never the
live current pointer. Until the pin moves, the app serves the OLD publication
and none of the above is visible. Repin to the `bond_serving_v1` publication id
from 4.1, then re-check the app surface.

## 6. Rollback

Every product self-promotes atomically inside its stage and the chain
compensates a mid-run terminal failure by itself. To undo a COMPLETED run:

```python
from src.bonds import daily_chain
daily_chain.rollback_pointer(conn, "bond_serving_v1")
daily_chain.rollback_pointer(conn, "bond_metric_v1")
daily_chain.rollback_pointer(conn, "bond_security_v1")
```

Then repin the app to the previous serving publication id recorded in step 1.
Nothing is deleted: the prior publications stay queryable.

---

## Cross-repo dependency (does NOT block this execution)

Two app-side unlocks are outside this repository and outside this run:

1. **The catalog duration filter** stays 422-gated until the app repo re-syncs
   `app.contracts.bond_serving_v1.SURFACE_DIGEST` to
   `sha256:cd14dcbe08339b31176f0f6c65b00d2f15e4b05fbf9e943fc0ca98a158329999` and
   mirrors the two `payload_keys` tuples. The app derives its answerable filter
   set FROM those keys, so the sync IS the unlock — no other app code changes.
2. **The `security_effective_duration` detail tile** stays
   `phase10_gate_not_passed` until the app's metric registry flips that metric's
   availability: the app deliberately IGNORES a payload value for a gated
   metric.

Publishing the keys ahead of both is harmless — the reader ignores payload keys
it does not map — so this execution does not have to wait for either.

## Named residuals

* **~1,442 curated securities keep no issuer name** (measured): ~1,413 whose
  reported names do not reach the 0.60 consensus at the exact CUSIP9 and are not
  rescued at CUSIP6, ~320 with conflicting reported LEIs at CUSIP9 and ~417 at
  CUSIP6. Each carries its `abstain_reason`. The 0.50-0.60 share band (1,133
  securities) is declared upside, deliberately not tuned after measuring.
* **133 of the 10,206 curated CUSIP9s have no published alias**, so they are not
  in the security master at all and no publication can name them.
* **5 curated securities have no NAMED holding anywhere**, at CUSIP9 or CUSIP6.
* **A mid-word truncation does not fold** into its complete sibling
  (`AMC ENTERTAINMENT H` vs `AMC ENTERTAINMENT HOLDINGS`): the prefix collapse
  requires a word boundary so that `ACME` can never absorb `ACMEX`. The measured
  coverage already reflects this.
* **`security_ytw` stays absent** even though callability is now known: the
  reference states THAT a bond is callable, not WHEN. A worst-case yield without
  call dates would be a guess.
* **OAS and z-spread stay absent** — no validated model publishes them.
* **The security-master build pin digests the enrichment inputs, but the
  publication ID does not.** A reference-table change with an unchanged
  `code_revision` and `as_of` therefore replays the existing publication instead
  of rebuilding. That is exactly why step 3 sets `CODE_REVISION` explicitly.
