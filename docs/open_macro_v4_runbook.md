# open_macro v4.0-rev — operator runbook (dark run)

The v4 engine (M-COMP4) publishes a **monthly** regime decision and its book into
`open_macro_v04_decisions` / `open_macro_v04_allocations`. This runbook takes it from
"nothing exists" to "the dark run is verifiably producing rows", and stops there.

**It stops there on purpose.** The formulation freeze was committed
`status: awaiting_ratification`, `approved: false`, and was **ratified by the
`quant_owner` on 2026-08-04** (see §7.1). Nothing in this document flips the Light
onto v4 by itself; step 7 is the gate that says who may, and §7.1 records that
they did.

---

## 0. What this does NOT touch

`open_macro_v03_decisions` / `open_macro_v03_allocations` / the v03 staleness ledger
are **untouched**. The v03 worker is live, certified and daily; v4 is monthly and
writes to sibling tables under a different advisory lock (`900_218` vs `900_215`).
They run side by side and neither serializes against the other. If you find yourself
about to stop the v03 worker "so v4 can run", stop and re-read this paragraph.

---

## 1. Apply the DDL (operator, once)

`run()` **never** creates or alters a table. It verifies the catalog read-only and
fails loud if the tables are absent, so the migration is a deliberate step and a
half-applied schema can never hide behind an abort.

The worker's DB role cannot `CREATE TABLE`. Use the **IAP tunnel as `postgres`**:

```bash
# terminal 1 — the tunnel (no public IP on the VM)
gcloud compute start-iap-tunnel timescale-sp 5432 \
  --local-host-port=localhost:5432 --zone southamerica-east1-a

# terminal 2 — apply in dependency order
psql "postgresql://postgres@localhost:5432/market" \
  -v ON_ERROR_STOP=1 \
  -f schemas/open_macro_v04_decisions.sql \
  -f schemas/open_macro_v04_allocations.sql \
  -f schemas/open_macro_v04_decision_input_captures.sql \
  -f schemas/open_macro_v04_pit_evidence.sql
```

The producer files are idempotent, so
re-running is safe — **but** `IF NOT EXISTS` is a no-op on an existing table and
repairs nothing. If a table already exists in a different shape, the worker's Gate 3
will say exactly which column diverges; fix it with an explicit `ALTER`, do not
re-run the file and assume.

Confirm:

```sql
SELECT table_name, count(*) AS columns
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (
      'open_macro_v04_decisions', 'open_macro_v04_allocations',
      'open_macro_v04_decision_input_captures'
  )
GROUP BY 1 ORDER BY 1;
-- expected: open_macro_v04_allocations | 21
--           open_macro_v04_decision_input_captures | 9
--           open_macro_v04_decisions   | 29
```

The capture DDL grants `worker_writer` append-only capture access. If a distinct
producer role is used, grant it write access to decisions/allocations and insert-only
capture access; the evidence service needs only SELECT on captures.

```sql
GRANT SELECT, INSERT, UPDATE ON open_macro_v04_decisions,  open_macro_v04_allocations
  TO app_runtime;   -- or the role the Railway worker connects as
```

Everything after this step uses the **NLB + mTLS** path (`CN=worker_writer`), the same
route the Railway workers take — not the tunnel:

```bash
psql "postgresql://worker_writer:<URL-ENCODED-PASSWORD>@<nlb-ip>:5432/market\
?sslmode=verify-full&sslrootcert=ca.crt&sslcert=client.crt&sslkey=client.key"
```

> The password in the secret contains characters that **must stay URL-encoded**
> (`/` → `%2F`, `@` → `%40`). `src/db.py` never re-encodes the userinfo section, so an
> unencoded password fails as an auth error and sends you hunting for the wrong bug.

---

## 2. Feed the engine, then check the four series

The v4 engine reads four FRED series by **explicit id**. They are already in
`RAW_INGEST_SERIES`, so `macro_ingestion` keeps them current — but it fetches a
`LOOKBACK_YEARS` window and **does not backfill the decades of history the replay
reads**. Run it first:

```bash
WORKER=macro_ingestion python -m src.run_worker
```

Then verify — and read the `months` column, not just the `last_obs`:

```sql
SELECT series_id, count(*) AS n, min(obs_date) AS first_obs, max(obs_date) AS last_obs
FROM macro_data
WHERE series_id IN ('MTSDS133FMS', 'GDP', 'M2SL', 'SUBLPDCILSLGNQ',
                    'CFNAI', 'CPIAUCSL')
GROUP BY 1 ORDER BY 1;
```

What each one is, and what breaks without it:

| series | layer | role | absent ⇒ |
|---|---|---|---|
| `MTSDS133FMS` | L1 | rolling-12m federal deficit, $ millions, negative in a deficit month | **run refuses**, naming it |
| `GDP` | L1 | **nominal LEVEL**, $ billions — *never* `A191RL1Q225SBEA` (real growth) | **run refuses**, naming it |
| `SUBLPDCILSLGNQ` | L3 arm A | SLOOS net % of banks tightening C&I standards — the bank **decision**, not a spread | **run refuses**, naming it |
| `M2SL` | L3 arm B | M2 money stock, YoY % | **run refuses**, naming it |
| `CFNAI`, `CPIAUCSL` | pre-chain proxy | replay-only quadrant reconstruction before 2014-03 | run continues; pre-2014 months read `proxy_missing` |

`MTSDS133FMS` needs at least **12 months** of history before `deficit_gdp` exists at
all, and `GDP` is quarterly with a 3-month PIT lag. A short mirror does not fail the
run — it publishes fewer months, and reports how many it skipped in
`n_months_without_state`. Check that number.

Also confirm the price warmer reaches all seven book instruments (**LQD included** —
the dominance book routes 0.155 into it through the B60-LQD barbell):

```sql
SELECT ticker, max(date) AS last_session
FROM eod_prices
WHERE ticker IN ('SPY','TLT','TIP','GLD','DBC','SHY','LQD')
GROUP BY 1 ORDER BY 1;
```

All seven must be current. The worker prices the book only on sessions where **every**
instrument has a usable close; one lagging ticker moves `priced_at` backwards for
every month, and a ticker with no history at all refuses the run by name.

---

## 3. Create the Railway service

`railway.toml` documents the contract but deliberately **does not create the service** —
that is operational. Create it in the Railway `investintell-workers` project:

- `WORKER=open_macro_v04`
- the shared `DATABASE_URL` + `DB_TLS_CA_PEM` / `DB_TLS_CERT_PEM` / `DB_TLS_KEY_PEM`
- cron: **daily, after the ingest chain** — `45 8 * * *` UTC is the slot
  (`macro_ingestion` 05:00 → `eod_prices_warmer` 06:15 → `open_macro_v03` 08:30 →
  **`open_macro_v04` 08:45**)

Ordering is not cosmetic. Running before `macro_ingestion` publishes a month against
yesterday's mirror; running before `eod_prices_warmer` moves `priced_at` back a day.
Neither fails — they just quietly publish something slightly different — which is why
the ordering is written down rather than left to whoever creates the service.

> **Never set `cronSchedule: null`** to disable a worker in this project. On Railway
> that means *run continuously*, not *off*. Use an unreachable schedule
> (`0 0 29 2 *`). An empty string `""` is worse: it poisons the whole staged patch and
> silently traps every other pending change with it.

Publishing worker code is `serviceInstanceDeploy` with an explicit `commitSha`
(`watchPatterns` is empty, so merging to `main` deploys nothing); *running* one is
`deploymentRestart`.

---

## 4. Verify the dark run

### 4.1 A green Railway status is not evidence

`SUCCESS` on the dashboard means the container **built and started**. It does not mean
the job ran, and `lock_busy` exits 0. Verify in the target table, every time:

```sql
SELECT count(*)                      AS n_months,
       min(as_of)                    AS first_month,
       max(as_of)                    AS latest_month,
       max(updated_at)               AS last_written,
       count(*) FILTER (WHERE decision_basis = 'live')             AS n_live,
       count(*) FILTER (WHERE decision_basis = 'bootstrap_replay') AS n_bootstrap,
       count(*) FILTER (WHERE valid_status = 'invalidated')        AS n_invalidated
FROM open_macro_v04_decisions;
```

`last_written` must be **today**. If it is not, the worker did not run, whatever the
dashboard says.

First successful run, with a full macro mirror: `n_months = 236`
(2006-12-31 … 2026-07-31), `n_live = 1`, `n_bootstrap = 235`. With production's rolling
mirror, `first_month` will be later and `n_months` smaller — compare it against the
`n_months_without_state` the run reported, and the two must account for each other.

### 4.2 The current decision

```sql
SELECT d.as_of, d.fiscal_state, d.fiscal_state_age_m, round(d.deficit_gdp, 3) AS deficit_gdp,
       d.guard_level, d.guard_coverage, d.arm_a, d.arm_b,
       d.quadrant, d.quadrant_source, d.decision_validity, d.decision_basis,
       a.book_id, a.w_spy, a.w_tlt, a.w_tip, a.w_gld, a.w_dbc, a.w_shy, a.w_lqd,
       a.priced_at, d.valid_until
FROM open_macro_v04_decisions d
JOIN open_macro_v04_allocations a USING (as_of)
WHERE d.valid_status = 'valid' AND d.valid_until > now()
ORDER BY d.as_of DESC;
```

**Exactly one row.** That is what `valid_until` (the next month-end at 14:00 UTC) is
for: the successor supersedes the incumbent the moment it becomes computable. Zero
rows means the worker has not published this month's decision — check `last_written`
above before anything else.

**Expected state today (2026-08, publishing 2026-07-31):**

| field | value |
|---|---|
| `fiscal_state` | `dominance` (`deficit_gdp` ≈ 5.56, above the 5.0 entry, age 46 months) |
| `guard_level` | `off` (neither arm fired) |
| `guard_coverage` | `full` |
| `decision_validity` | `dominance_baseline` |
| `book_id` | `expansion_c50+bb` |
| weights | SPY 0.39375 · TLT 0 · TIP 0 · GLD 0.10625 · DBC 0.1125 · SHY 0.2325 · LQD 0.155 |

If the state is `dominance` and the book is **not** `expansion_c50+bb`, do not
rationalise it — the guard fired, and §4.4 says what that means.

### 4.3 Cross-check against the signed ledger

The published rows are the golden ledger's rows. Spot-check the crosstab over the
measured window; it is pinned in `golden_meta.json` and reproduced by the suite:

```sql
SELECT fiscal_state, guard_level, count(*)
FROM open_macro_v04_decisions
WHERE as_of BETWEEN '2006-12-31' AND '2026-05-31'
GROUP BY 1, 2 ORDER BY 1, 2;
-- contained|off 58   contained|alert 25   contained|severe 23
-- dominance|off 86   dominance|alert 42   dominance|severe 0
```

`dominance|severe = 0` is A3, not a coincidence: SEVERE is structurally unreachable
under fiscal dominance, and the table's own CHECK constraint refuses such a row.

Provenance must be one run and one formulation:

```sql
SELECT formulation_sha256, code_commit, count(*), count(DISTINCT input_digest_sha256)
FROM open_macro_v04_decisions GROUP BY 1, 2;
-- formulation_sha256 = 0f154e614a12cea69a77a8d275204b81d1b32dffd1fb6269ef962d0244f84060
```

More than one `formulation_sha256` means rows were produced under different frozen
formulations and **cannot be compared to each other**. Find out which is which before
reading anything else in the table.

### 4.4 What each token means

**`fiscal_state`** — `dominance` above a 5.0% rolling-12m deficit/GDP, `contained`
below 4.0%. The closed band `[4.0, 5.0]` **holds** whatever state is in force and
raises `fiscal_boundary`: inside the band the state was *carried*, not chosen.

**`guard_level`**
- `off` — neither arm fired and the guard could see.
- `alert` — the book is blended halfway to the CENTER. Inside `contained` under
  amplitude 0 the L2 book **is** the CENTER, so the blend is the identity and the
  guard is *inert on the portfolio*. The token is published anyway, precisely because
  that is the month where a suppressed token would hide a real loss of coverage.
- `severe` — the defensive book wholesale. Requires `contained` with age ≥ 3 (A3).

**`guard_coverage`** — freshness in **native publication periods**, never in months.
`partial_a` = arm A (SLOOS, quarterly, lag 2) is the missing one; `partial_b` = arm B
(M2SL, monthly, lag 1) is. A stale arm contributes `False`, never its last reading.
`blind` = both stale: the guard compresses conservatively (ALERT) and says so.
A month-count rule cannot express this — same label age, opposite verdicts.

**`quadrant` / `quadrant_source`** — a **published diagnostic that allocates nothing**.
Contained amplitude is 0, so `compressed_0(q)` is the CENTER for every `q`. The
quadrant is recorded so you can watch the diagnostic move while the weights do not.
`chain_fresh` → the v03 chain read `valid` this month; `chain_carry` → the last valid
reading is being carried (≤ 3 months); `no_signal` → the carry lapsed; `proxy` /
`proxy_missing` → the replay-only pre-2014 reconstruction.

**`decision_validity`** — the token the Light will read, derived in this order:
`guard_blind` (coverage blind) → `dominance_baseline` (the book is the fiscal
baseline, the quadrant did not choose it) → `fresh` → `carried` → `no_signal`. The
DDL restates the derivation as a CHECK, so the database refuses a hand-written token.

**`decision_basis`** — `live` means this month was the last complete month-end at the
moment of the run *and* the row did not exist before. `bootstrap_replay` means a
retroactive month reconstructed from the **current vintage** of the inputs: what the
engine *would* have said, not a record of what it did say. It is never demoted; the
upsert's UPDATE branch does not touch the column.

**`severe_degraded`** — read §4.5. It is the one token whose comfortable half is a lie.

### 4.5 The step rule, verbatim from the freeze

Quoted exactly as the owner required it in
`artifacts/quant/open_macro_v4_formulation_freeze_001/formulation_freeze.json`
(`formulation.L3_guard.A8.normative_text`):

> Sob amplitude 0, ALERT em contained devolve o center: inerte. A leitura correta:
> após 3 meses não confirmados a guarda DESARMA A CARTEIRA, e só o preço
> (SPY ≤ −8% da máxima móvel de 12m) a re-arma. 'Degrada a ALERT (nunca off)' é
> verdadeiro do estado — a re-escalada segue vigiando — e falso da carteira.

It is here because "degrades to ALERT (never off)" is **true of the state and false of
the portfolio**, and a reader who takes only the comfortable half will believe the book
is defended when it is not.

```sql
SELECT as_of, severe_run_age, stress_confirmed, guard_level
FROM open_macro_v04_decisions
WHERE severe_degraded ORDER BY as_of;
-- 24 months over the measured window; all guard_level = 'alert', all contained.
-- 2015-09 is NOT among them: its SPY drawdown reached -8.48%, so A8 re-escalated it
-- to SEVERE. That is the designed behaviour, not a gap.
```

---

## 5. Failure modes and what they mean

| the run says | what happened | do |
|---|---|---|
| `formula module pin mismatch` | a formula module on disk differs from the freeze | do **not** regenerate the artifact to make it pass. Find out why the module changed. |
| `formulation_sha256 … != recomputed` | the freeze's own formula block was edited without re-deriving its digest | same. This is the case the module pins cannot see. |
| `macro series 'X' is absent or empty` | the mirror has no rows for X | run `macro_ingestion`, re-check §2 |
| `schema catalog verification failed` | the DDL is absent or drifted | §1. The message names the exact column. |
| `deficit_gdp is not computable at the latest month-end` | `MTSDS133FMS`/`GDP` do not reach the current month | **nothing was published.** Deliberate: a short publish that stops one month silently is worse than a loud stop. |
| `lock_busy` | another run holds `900_218` | normal on overlap; exits 0 |
| `decision upsert for the CURRENT month … invalidated` | someone killed this month's row | resolve the invalidation explicitly; a re-run will not resurrect it |
| `n_skipped_invalidated > 0` | historical rows are killed and stay killed | intentional — one kill-switch action must not wedge the publisher |
| `the book … carries HYG` | a router change gave HYG weight | the allocations table has no `w_hyg`. Add the column and re-derive the weight-sum constraint **before** publishing. |

---

## 6. Reproducing a dispute

The worker and the fixture replay run the **same function**
(`v4_replay.build_ledger`). To compare a live month against the signed ledger:

```bash
PYTHONPATH=. python -m harness.phase0q.v4_replay     # replays the pinned fixtures
PYTHONPATH=. python -m pytest tests/test_open_macro_v04_worker.py -q
```

The suite drives `run()` end to end over the pinned inputs and asserts the published
rows against `golden_ledger.csv` byte for byte. If it is green and production
disagrees, the difference is in the **inputs**, not the engine — compare
`input_digest_sha256` on the row against the parts the run reported.

> The live digest will never equal the pinned snapshot digest.
> `macro_data.value` is `NUMERIC(24,6)`; the fixtures carry full float64 precision.
> Same recipe, different bytes — by construction, not by drift.

---

## 7. The gate before the Light consumes v4

**The formulation freeze is `awaiting_ratification`, `approved: false`, and
self-ratification is prohibited.** The ratification decision belongs to the
`quant_owner`.

Until the owner ratifies
`artifacts/quant/open_macro_v4_formulation_freeze_001/formulation_freeze.json`:

- the dark run publishes and is inspected; that is all it does;
- **no** Light flag is flipped onto `open_macro_v04_*`;
- **no** v03 worker, table or consumer is retired, degraded, or repointed.

What ratification needs in front of it — the dark run's own evidence:

1. ≥ 1 month-end published with `decision_basis = 'live'` (not only bootstrap rows);
2. the §4.3 crosstab reproducing over the measured window;
3. a single `formulation_sha256` across the table;
4. `guard_coverage` observed at `full` for the live month, or a stated reason it is not;
5. the A4′ 2013 exception acknowledged as **dated** — it expires with v4.1 and must be
   re-measured, not renewed by default.

An engineering agent producing these rows is not a step toward approval. Bring the
evidence; the owner decides.

### 7.1 Ratification record

**Ratified 2026-08-04 by the `quant_owner`** — written statement, verbatim:
"RATIFICO O formulation_freeze_001". All five evidence items above were satisfied
by the dark run at the moment of ratification (236 months, `live = 1`, the §4.3
crosstab and single `formulation_sha256` reproducing, `guard_coverage = full` on
2026-07-31, A4′ acknowledged as dated). The artifact now carries
`status: ratified`, `approved: true` and a `ratification` block naming the act;
the formulation digest and module pins are byte-identical to the signed state.

What ratification authorizes — and what it does not:

- `USE_OPEN_MACRO_V04` may be flipped on the Light (`hub-api`), lighting the
  taxonomy compass and the divergence diagnostic together;
- v03 retirement and builder consumption of v4 books remain **separate, later
  increments** — nothing in this ratification retires, degrades or repoints a
  v03 worker, table or consumer.
