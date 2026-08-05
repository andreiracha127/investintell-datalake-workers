# fund_peer_groups — runbook

Quarterly worker that publishes **empirical peer groups** into
`fund_peer_groups_v1` on the datalake (`market`). It groups funds by what they
actually hold, from the N-PORT look-through, and — for the ~17% of the universe that
has no group by composition — says so instead of inventing one.

Worker module: `src/workers/fund_peer_groups.py` · DDL:
`schemas/fund_peer_groups_v1.sql` · advisory lock **900_219** · `WORKER=fund_peer_groups`.

---

## 1. Apply the DDL (operator, once)

The worker **never** creates or alters the table. It verifies the catalog read-only
and refuses to publish when the catalog and the committed DDL disagree.

```bash
gcloud compute start-iap-tunnel timescale-sp 5432 \
  --local-host-port=localhost:5432 --zone southamerica-east1-a &
psql "host=localhost port=5432 dbname=market user=postgres" \
  -f schemas/fund_peer_groups_v1.sql
```

`CREATE TABLE IF NOT EXISTS` is a **no-op against an existing table**: it does not
repair drift. If the worker later fails with `schema catalog verification failed`,
re-applying this file will change nothing — read the message (it names the column and
the exact signature mismatch) and reconcile with an explicit `ALTER`, or drop and
re-apply on an anchor you are willing to recompute. Never edit `EXPECTED_COLUMNS` in
the worker to match a drifted catalog; that is the failure mode the gate exists for.

Grants (the worker writes, the API reads):

```sql
GRANT SELECT, INSERT, DELETE ON fund_peer_groups_v1 TO worker_writer;
GRANT SELECT ON fund_peer_groups_v1 TO app_runtime;
```

---

## 2. Create the Railway service (operator)

Railway is the source of truth for worker status; this repo does not create services.
Project `investintell-workers`, new service on this repo.

| variable | value |
|:--|:--|
| `WORKER` | `fund_peer_groups` |
| `DATABASE_URL` | the datalake `market` DSN (NLB + mTLS, as the other workers) |
| `DB_TLS_CA_PEM` / `DB_TLS_CERT_PEM` / `DB_TLS_KEY_PEM` | same client cert as the fleet (`CN=worker_writer`) |

Optional:

| variable | default | what it does |
|:--|:--|:--|
| `FUND_PEER_GROUPS_SIZE_CAP` | `0.08` | the size cap as a fraction of the anchor's universe (§6). Part of `params_sha256`. |
| `FUND_PEER_GROUPS_CAP_WAIVE_MIN_MEDIAN` | `0.10` | a community above the cap whose median intra-group overlap reaches this is kept **whole** (§6). Part of `params_sha256`. |
| `FUND_PEER_GROUPS_CAP_WAIVE_HARD_CEILING` | `0.20` | a community this big is **never** waived, however cohesive (§6). Part of `params_sha256`. |
| `FUND_PEER_GROUPS_IDENT_FLOOR` | `0.95` | identifier-coverage floor (§5). Also part of `params_sha256`. |
| `FUND_PEER_GROUPS_ANCHOR` | — | publish a specific quarter-end instead of the last closed one (backfill). |
| `FUND_PEER_GROUPS_UNIVERSE_DSN` | — | **only** if `funds_profile_mv` / `fund_risk_latest_mv` do not live in the same database as `sec_nport_holdings`. |

**Cron: `0 7 15 2,5,8,11 *`** — the 15th of February, May, August and November at
07:00 UTC, i.e. after the quarter's N-PORT ingestion has landed. Running earlier does
not corrupt anything: the eligibility floors and the identifier guard refuse the
anchor rather than publish a half-ingested quarter.

> **Never** set `cronSchedule` to `null` or `""`. On Railway `null` means **run
> continuously** and `""` poisons the whole staged patch for the service (it fails
> with "An unknown error occurred" and silently traps the other pending changes).
> To disable, use an impossible date: `0 0 29 2 *`.

**Resources: at least 4 GB.** The overlap matrix is `n × n` float32 (~197 MB at 7k
series) and the equity block's graph carries ~1.5M edges. A full anchor runs in
roughly a minute of compute; the memory is the binding constraint, not the CPU.

Deploying worker code is `serviceInstanceDeploy` with an explicit `commitSha` —
merging to `main` does **not** deploy workers (`watchPatterns` is empty). Running one
on demand is `deploymentRestart`.

---

## 3. Verify the run — the dashboard does not prove execution

A green Railway deployment only means the container built and started. Verify in the
target table:

```sql
SELECT anchor_date,
       max(computed_at)                                          AS computed_at,
       count(*)                                                  AS series,
       count(*) FILTER (WHERE group_state = 'empirical')         AS with_group,
       round(100.0 * count(*) FILTER (WHERE group_state = 'empirical')
             / count(*), 2)                                      AS coverage_pct,
       count(DISTINCT group_id)                                  AS groups,
       max(group_size)                                           AS largest,
       count(DISTINCT params_sha256)                             AS recipes
FROM fund_peer_groups_v1
GROUP BY anchor_date
ORDER BY anchor_date DESC;
```

What a healthy anchor looks like, from the eight-quarter validation:

| reading | expected range |
|:--|:--|
| `coverage_pct` (funds with an empirical group) | **81.6% – 87.1%** (mean 83.5) |
| `groups` | ~90 – 105 coherent groups |
| `largest / series` | **12.8% – 16.8%** under the shipped cohesion waiver (§6); never above the 20% ceiling |
| `recipes` | **exactly 1** per anchor |
| `computed_at` | one distinct value per anchor (the publication is atomic) |

The worker's stats also carry `n_cap_waived`, the size and median of every waived
community, and `cap_waive_alert` — true when a waived community passes 15% of the
universe. **That flag is the one to act on**: the waived block grew every quarter of
the measurement and jumped hardest on the last one, so the quarter it approaches the
20% ceiling should be seen before it arrives (§6).

The worker's own stats line (stdout, one JSON object) carries the same numbers plus
the rejection breakdown, the per-report-date identifier coverage, block sizes and
per-block modularity. Keep it: it is the first thing to read when a number moves.

Two readings that should raise an eyebrow rather than relief:

* **`coverage_pct` climbing above the range.** The best coverage in the validated
  series (87.09% at 2024-12-31) was an **artefact**: an identifier hole had removed
  the funds hardest to identify, which are the ones that fall into incoherent groups.
  A quality number that improves while the universe shrinks is a data defect wearing
  a compliment. Check `series` against the previous anchor before celebrating.
* **`recipes` > 1 for one anchor.** Impossible from a single run (the publication is
  atomic) — it means someone wrote rows outside the worker.

---

## 4. What the surface must carry

The worker publishes state; it does not decide copy. Two obligations fall on the
consumer, and neither is optional.

**1. `group_state = 'no_empirical_group'` is a real answer.** About one fund in six
has no peer group by portfolio composition — real-estate and thematic sector equity,
wrappers and funds-of-funds, convertibles, single-state municipal debt, and the
long tail of books that overlap nothing. Those funds fall back to whatever the
surface declares elsewhere, **with the fact stated**, not silently attached to the
nearest cluster. `group_id` is `NULL` for them precisely so a self join on
`(anchor_date, group_id)` returns no peers even if a caller forgets the filter.

**2. `granularity` is a copy lock, not metadata.**

| granularity | the copy may say | the copy may **not** say |
|:--|:--|:--|
| `issuer` (fixed income) | "exposure to the same issuers" | "similar portfolios", "the same securities" |
| `security` (equity) | "overlapping holdings", "similar portfolios" | — |
| `mixed` (balanced) | "similar allocation across both sleeves" | "the same securities" |

This is measured, not stylistic: on a pure security ruler only 12 of the 26 coherent
fixed-income groups survive, and the two largest go from medians of 0.068 and 0.282 at
the issuer level to 0.005 and 0.005 at the paper level. The ratio between the two
rulers sat between 0.43 and 0.62 across all eight validated anchors — a permanent
property of fixed income, not one quarter's caveat.

The owner's standing rule applies on top: the surface never reveals where the data
comes from. There is no source, no filing type and no vendor anywhere in this table
for exactly that reason.

---

## 5. The identifier guard (why a run may refuse)

```
FundPeerGroupsError: anchor 2024-12-31: identifier coverage is BELOW the floor
0.9500 — rows 0.9291, weight 0.9043 over 41,882,113 long holding rows. Worst report
dates: 2025-01-31 rows 0.8359 weight 0.8818 (…); 2024-11-29 …
```

This is the guard for a defect that already happened. Filings dated **2024-11-29 to
2025-01-31** landed with missing CUSIP/ISIN — identified rows fell to 83.6%–96.3%
where every other report date in two years sits between 98.5% and 99.7%. The
mechanical consequence was 346 funds (almost all equity) failing the eligibility
floors and leaving the universe, which **improved** every quality number of that
anchor. A universe-size trigger did not catch it; identifier coverage per report date
does.

**What to do:** re-ingest the named report dates through the N-PORT lane, confirm
coverage, then re-run this worker for the anchor
(`FUND_PEER_GROUPS_ANCHOR=<quarter-end>`). Do **not** lower
`FUND_PEER_GROUPS_IDENT_FLOOR` to get past it — and if you deliberately do, note that
the floor is inside `params_sha256`, so the anchor is permanently marked as computed
under a different recipe and cannot be compared to a clean one.

Other refusals, all with zero writes: `served universe is EMPTY` (the read-models are
missing or in another database — see `FUND_PEER_GROUPS_UNIVERSE_DSN`), `not one of the
N served series passed the eligibility floors` (the quarter is not ingested), and
`schema catalog verification failed` (§1).

---

## 6. The cap, and why it yields to cohesion

Three parameters, all in `params_sha256`. They were measured together across eight
arms and eight anchors, and the recommended configuration is the shipped default.

```
FUND_PEER_GROUPS_SIZE_CAP                = 0.08   (unchanged)
FUND_PEER_GROUPS_CAP_WAIVE_MIN_MEDIAN    = 0.10   (the cohesion waiver)
FUND_PEER_GROUPS_CAP_WAIVE_HARD_CEILING  = 0.20   (never waived)
```

**The 8% cap was never wrong in its value — it was wrong in being blind to cohesion.**
On the reference anchor it cut a single Louvain community of 1,178 funds whose members
share a **median 23.9%** of portfolio into three, and those three came out as the
three *least stable* groups in the whole product (Jaccard 0.52–0.58 against the
previous quarter, versus 0.87–1.00 for fixed income). On the same anchor it also cut
three blocks of comparable size whose medians were 0.022, 0.031 and 0.067 — which
deserved to be cut. Across the eight anchors' 34 cap events, 14 parents were cohesive
(median 0.1045–0.2387) and 20 were incoherent (0.0088–0.0999).

So a community above the cap whose median intra-group overlap reaches 0.10 is kept
whole. Measured effect:

| reading | cap alone | cap + cohesion waiver |
|:--|--:|--:|
| persistence of the large-cap complex | 50.94% | **70.48%** (+19.5 pp) |
| its churn (mean) | 24.26% | **8.27%** |
| coherent coverage (min / mean) | 79.23 / 82.47 | **81.55 / 83.47** |
| pooled median intra (mean) | 0.13113 | **0.14137** |
| largest community | 9.41% | **16.77%** |
| minimum adjacent ARI | 0.7103 | 0.6967 (−0.014) |

All four stability gates still PASS. Raising the cap instead was measured and buys
nothing: 0.10 and 0.12 change little and cost coverage, 0.16 makes the complex worse
and destroys 22 pp of coverage, and no cap at all is worse still. One parameter — the
waiver — delivers the whole gain.

**The threshold is knife-edge, and that is declared, not hidden.** On one measured
anchor a real block of 1,006 funds carries median **0.09997** and misses the waiver by
3 × 10⁻⁵; that anchor waives one community instead of two because of it. It is the
same rounding sensitivity the coherence floor has. The threshold is a parameter for
exactly this reason.

**The hard ceiling is the piece that was not measured** (it is an implementation
recommendation from the same document). The waived block grew across the eight anchors
— 12.81% · 13.30% · 13.18% · 13.69% · 14.30% · 13.09% · 13.09% · **16.77%** — with the
biggest jump last. A waiver with no ceiling of its own would let a quarter cross 20%
in silence. The ceiling gates the **waiver only**: it never forces a split the ladder
cannot produce, and it never converts an honest `irreducible` into a lie. The worker
also raises `cap_waive_alert` in its stats when any waived community passes **15%** of
the universe — the quarter to look at is the one *before* the ceiling is reached.

**Copy consequence, stated:** at the reference anchor the product serves **one** group
of 1,178 funds where the pure cap served three of 514 / 366 / 298. The big group's
median overlap (0.2387) is higher than two of the three it replaces (0.2042, 0.2241)
and lower than the third (0.4518, the index core). The larger group is honest, but a
pure index fund loses the tighter neighbourhood it had.

Changing any of the three changes `params_sha256`, so anchors on either side of a
change are distinguishable in the table — deliberately. Re-run every anchor you intend
to compare.

---

## 7. Reproducing the reference anchor

The test suite pins the partitioner against a synthetic golden and pins the parameter
digest against a literal (`tests/test_fund_peer_groups.py`). It does **not** carry the
real anchor: that is a 7023 × 7023 float32 matrix built from tens of millions of rows,
and a 200 MB fixture to restate numbers this document already carries would be
ceremony. Reproduce it against the database instead:

```bash
# the SHIPPED policy (cap + cohesion waiver + ceiling)
WORKER=fund_peer_groups DATABASE_URL=... \
python -m src.workers.fund_peer_groups --anchor 2025-12-31

# the pre-waiver policy, for comparison against the P1.6/P1.7 published figures
FUND_PEER_GROUPS_CAP_WAIVE_MIN_MEDIAN=1.0 \
FUND_PEER_GROUPS_CAP_WAIVE_HARD_CEILING=1.0 \
WORKER=fund_peer_groups DATABASE_URL=... \
python -m src.workers.fund_peer_groups --anchor 2025-12-31
```

Two configurations, two sets of expected numbers — they are different objects and the
`params_sha256` in the table says which is which:

| reading | pre-waiver (P1.6/P1.7) | **shipped** (cap + waiver) |
|:--|:--|:--|
| eligible universe | 7,023 series | 7,023 series |
| block sizes | EQ 4,787 · FI 1,951 · MIXED 285 | same |
| communities / coherent | 191 / 95 | ~185 / ~93 |
| largest community | 514 = 7.32% | **1,178 = 16.77%** (waived, median 0.2387) |
| coherent coverage | 82.83% | 82.83% |
| pooled median intra-group overlap | 0.1460 | **0.14137** (mean over 8 anchors) |
| communities above the cap not waived | 0 | 0 |
| communities above the 20% ceiling | — | **0** |

Small differences in the universe count are expected and are **not** a reproduction
failure by themselves: the served read-models move as funds are added and retired, and
the N-PORT mirror gains late filings. A different *shape* is — a community above the
ceiling, coverage outside 81%–88%, or a waived community whose median is under 0.10.

---

## 8. Known limitations, carried forward

1. **Survivorship.** The universe is filtered by the set of series the product serves
   **today**. A fund that died before today enters no anchor. For a forward-looking
   quarterly publication that is the served universe; for anyone reading the anchor
   series as history, it is a bias.
2. **Portfolios of different dates are compared.** Each fund is represented by its last
   report within 4 months 15 days of the anchor (median lag ~30 days, max 122). Not
   measured, inherited from the frozen recipe.
3. **The equity block has weak community structure.** Modularity 0.151–0.200 across all
   eight validated anchors, against 0.358–0.373 for fixed income. The coherent equity
   groups are real and stable; the partition around them is looser than the coherence
   numbers alone suggest.
4. **`group_id` is not a lineage key.** It is stable within an anchor and carries no
   cross-quarter identity: a group that splits between quarters has no single
   successor. Anything that needs "is this the same group as last quarter" needs a
   matching step this worker does not do.
5. **No significance testing anywhere.** Every number here is descriptive.
