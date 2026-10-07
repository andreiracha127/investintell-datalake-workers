# Runbook — `bond_panel_v1` returns: coupon-PIT republication (audit A2-01)

The ordered procedure for republishing the historical returns surface of the
bond panel under the one coupon convention the resolver now implements
(contractual coupon first, point-in-time expanding median of the price/YTM
inversion otherwise), as a governed child publication that extends the current
head and keeps every other fact verbatim.

**Every step in §3 that touches production requires explicit owner
authorization, step by step. Nothing in this runbook is authorized by being
written here.** The engineering gates it relies on are the commits of the PR
that introduced it (`fix/bond-panel-coupon-pit`): the resolver fix, the
artifact builder, the terms export, and the emitter.

Related: the unit-repair child (T2.1) in `scripts/backfill_bond_panel_history.py`
(the transport this one mirrors), [`bond-market-implied-rating-republication.md`](bond-market-implied-rating-republication.md)
(the same "determinism before writing, digest-bound apply" discipline), the
round-2 applied-math audit report (area A, A2-01).

---

## 1. What changed and why

`src/bonds/panel_resolvers.bond_coupons` used the per-CUSIP **median over the
bond's full history** of the price/YTM coupon inversion whenever a row carried
no contractual `coupon_pct`. Month t's carry therefore used prices from months
after t, and a distressed tail moved the coupon of every earlier month.
Production measurement (read-only, 2026-10-07): every CUSIP-month through
2026-06 had `coupon_pct IS NULL`, i.e. the whole published history
(2002-08 → 2026-06, 2,801,208 returns rows) was priced off that fallback; from
2026-07 the live worker merges `bond_reference_terms.coupon_rate` into
`coupon_pct` and the fallback is inert where terms exist.

The resolver now carries the convention Light adopted for BOND-01
(`app.bond_optimizer.returns.bond_coupons`, branch
`fix/bond-optimizer-pinned-audit` @ `b61f019e`): the finite contractual
`coupon_pct` where the row carries one, else the expanding (months ≤ t) median
of the inversion per CUSIP in month order. The Stage 6 cron is invariant under
the change (its input is one anchor month plus the closed month, and the
contractual coupon is present from 2026-07), so **the deploy does not move any
live publication**; only the history needs republishing, and that is what this
runbook does.

### Stored basis of the history (what the republication replaces)

| Rows | Stored coupon basis | Reproduced by the builder |
|---|---|---|
| 2002-08 → 2025-03 (T3 base, `t3_historical_base_001`) | Light pre-BOND-01 full-history median over all OSBAP months ≤ 2025-03 per CUSIP | yes — `reconciliation` block of the artifact manifest |
| 2025-04 → 2026-06 (return-coverage repair tail, `t3_historical_base_return_coverage_repair_v1`) | the same coupon, backed out of the stored carry (`median(12·carry·prev_price)`), or the first inversion after 2025-03 for entrants | yes — same block |
| 2026-07 → head | live worker, contractual coupon from `bond_reference_terms` | untouched (copied verbatim) |

The builder refuses to write an artifact unless every observed row at or before
the cutoff reproduces BOTH its stored `price_return` (float32-noise tolerance:
the frozen pack stored prices as float32) and its stored carry under that basis.
That is the proof that the inputs are the ones the history was built from.

### What the republication changes

Only `carry_return`, `total_return` and `suspect` on observed rows with
`month <= 2026-06-01`. Keys, `price_return`, typed-exit rows, the distribution
identity columns, the payload (plus a `coupon_pit_repair` marker) and every
row after the cutoff are verbatim. The DDL's generic pointer rules admit the
child (same config, same window, parent = current head, dual-series identity);
no trigger branch is added.

## 2. Artifacts, identity, consumers

### Inputs (pinned, already on the operator machine)

The v2 artifact set `C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\unit_repair_v2`
(the read-only T0.2v export under pointer `71b672c8-…`, re-derived by the unit
repair), pinned by `EXPECTED_SHA256_UNIT_REPAIR_V2`:

| File | sha256 |
|---|---|
| `bond_panel_live.parquet` (3,448,307 rows) | `2bc85774f608aebc57e8e345e49113938549f8f89ae87dee509cdcae8758aba2` |
| `bond_monthly_returns.parquet` (2,810,912 rows) | `a2778b5c723f1d4c1c91e31319dfe2618589a30ce56c60a9d7d5008482f39007` |

The rows at or before the cutoff are invariant across later heads (a Stage 6
child adds only its own closed month; the history always resolves to the
unit-repair child `65156481-…`), which is why an export taken under `71b672c8`
is still the correct input today.

Plus the owner's read-only `bond_reference_terms` coupon export (§3.1) — the
only production read the republication needs. On 2026-10-07 the table held
10,206 rows with a coupon (9,971 `Fixed`, 42 `Variable`, 193 untyped;
`max(loaded_at)` 2026-08-08). The builder applies `coupon_rate` without a
`coupon_type` filter, exactly as `build_db_monthly_panel` does for the live
panel (one convention), and records the `coupon_type` distribution of the
CUSIPs it used in the manifest. **Whether `Variable` coupons should be excluded
from the contractual branch is an owner call** — the live worker does not
exclude them today; excluding them here only would make the history and the
live tail disagree.

### Outputs (the v3 artifact; `scripts/build_bond_panel_coupon_pit_returns.py`)

`<unit_repair_v2>/coupon_pit_v3/`:

- `bond_monthly_returns.parquet` — same 13 columns and row order as v2;
- `coupon_basis.parquet` — per repriced row: previous price, stored coupon,
  new coupon, basis (`contractual` | `pit`), carry before/after, delta in bp;
- `manifest.json` — `contract = returns_coupon_pit_repair_v1`, `mode`
  (`contractual_then_pit`; the emitter refuses `pit_only_preview`), input and
  output digests, the terms export digest and coverage, the resolver's own
  sha256, counts (`rows_at_or_before_cutoff`, `repriced_rows`,
  `contractual_rows`, `pit_rows`, `dropped_rows_no_pit_basis`, …),
  reconciliation maxima, the delta distribution and the per-year carry sums
  (`per_year`, `per_year_digest`) the finalize gate compares against.

### Identity (`scripts/backfill_bond_panel_coupon_pit_repair.py`)

`publication_id = uuid5(bond_panel_v1:coupon-pit-repair:<fingerprint>)` where
the fingerprint binds the contract, the head bound at plan time, the root base
and unit-repair child ids, the config hash `1863d3d5fa3a0edf`, the cutoff, the
three artifact digests, the terms export digest, the per-year carry digest,
the pinned counts and the resolver sha256. `code_revision =
t3_returns_coupon_pit_repair_v1`. The frozen authorization is
`COUPON_PIT_EXPECTED_ARTIFACT` (step 3.3); while it is `None` the emitter
refuses with `coupon_pit_artifact_unpinned`.

### Downstream consumers of the historical rows

| Consumer | Reads | After republication |
|---|---|---|
| Light `backend/app/repositories/bond_panel.py` (`_HISTORY_RETURNS_SQL`) → `services/bond_quality_v1.py` (`_signal_joined`, `factor_returns` → `bond_factor_returns_v1`, `factor_returns_digest`, the `snapshot_rv_returns_rating_watermark…` identity), `services/bond_backtest.py` (`expanding_rv_beta`, folds), `services/bond_recommendation_refresh.py` | `bond_panel_current_returns_v1_mat` | **re-run the bond recommendation refresh** (quality, factor returns, backtest); the pinned factor-returns digest and the quality publication identity move |
| Workers `bond_panel_current_returns_v1_mat` | refreshed by the finalize step and by the daily chain | nothing to re-run |
| Workers implied rating (`src/bonds/implied_rating.py`), EL anchor, serving (`serving_materializer`) | `spread_final_bps`, `mod_dur`, prices — never the carry | not affected; `contracts/bond_market_implied_rating_round002_*` pin `bond_panel_live.parquet` only |
| Workers `fund_factors` / `factor_model` | NAV returns, not bond returns | not affected |

## 3. Procedure (ordered; each production step needs its own owner authorization)

### 3.0 Preconditions (no production action)

- The PR is merged to `main` and the workers services are redeployed from it
  (merging workers `main` redeploys git-connected crons). The Stage 6 cron has
  run at least once on the new resolver with an unchanged publication
  (expected: the same-month short-circuit, or a child whose returns rows equal
  the previous run's).
- The operator can run `psql` against the private database the way the T3
  base and the unit repair were applied (`railway ssh` into the serial worker,
  `psql -X -q -v ON_ERROR_STOP=1 -f -` on stdin; see the T0.2v export manifest
  method notes), in a quiet panel-write window (no Stage 6 run, no Light
  refresh: outside 05:00–09:00 UTC).
- The v2 artifact directory above is present and its digests match (the
  builder refuses otherwise).

### 3.1 Owner export of the contractual coupons — READ-ONLY (owner runs it)

From `E:\tmp-deploy\api\backend` (the railway-linked checkout), outside
06:00–08:30 UTC:

```powershell
railway run --service risk-metrics -- uv run --no-project --with "psycopg[binary]" `
  python E:\investintell-datalake-workers-sep\scripts\export_bond_reference_terms_coupons.py `
  --out C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\terms_export_YYYYMMDD
```

The script rewrites the DSN to the public proxy, opens one
`default_transaction_read_only=on` session with a 30 s statement timeout,
backs off if a long active transaction exists, runs one LIMIT-bounded `SELECT`
(`cusip9, coupon_rate, coupon_type, maturity_date, batch_label, loaded_at`
where `coupon_rate IS NOT NULL`), refuses if the count differs from
`count(*)`, and writes `bond_reference_terms_coupons.csv` plus a `.sha256`
sidecar. Keep the printed `rows=… max_loaded_at=… sha256=…` line with the
evidence.

### 3.2 Build the v3 artifact — OFFLINE (no database)

From the merged `main` checkout, one process at a time (the build holds the
3.4M-row panel and the 2.8M-row returns in memory; several minutes):

```powershell
python scripts\build_bond_panel_coupon_pit_returns.py `
  --artifact-dir C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\unit_repair_v2 `
  --terms C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\terms_export_YYYYMMDD\bond_reference_terms_coupons.csv `
  --out C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\unit_repair_v2\coupon_pit_v3
```

Review `manifest.json` before anything else happens:

- `mode == contractual_then_pit`; `counts.dropped_rows_no_pit_basis == 0`
  (the emitter refuses otherwise: a non-zero count means some row has no
  finite inversion up to its month and no terms — decide per row, do not
  republish around it);
- `reconciliation.max_relative_diff` and
  `price_return_reproduction.max_relative_diff` at float32 noise (≈ 1e-7);
- `counts.cusips_with_contractual_coupon` vs `counts.cusips_in_scope`, and
  `inputs.terms_export.coupon_type_distribution_with_coupon` (the `Variable`
  question above);
- the delta distribution (`delta_bp`, `per_year`): the expectation from the
  production sample is sub-bp on ordinary names and tens to hundreds of
  bp/month on distressed names with a contractual coupon far above the
  price-implied one.

### 3.3 Pin the artifact — follow-up commit (owner review, quiet-window merge)

Fill `COUPON_PIT_EXPECTED_ARTIFACT` in
`scripts/backfill_bond_panel_coupon_pit_repair.py` from the manifest:

```python
COUPON_PIT_EXPECTED_ARTIFACT = {
    "artifact_sha256": {"bond_monthly_returns.parquet": "…", "coupon_basis.parquet": "…", "manifest.json": "…"},
    "terms_export_sha256": "…",   # inputs.terms_export.sha256
    "per_year_digest": "…",       # per_year_digest
    "counts": {"returns_rows_out": …, "rows_at_or_before_cutoff": …, "rows_after_cutoff": …,
               "scope_rows": …, "repriced_rows": …, "exit_rows_at_or_before_cutoff": …},
}
```

and flip `test_authorization_constants_are_frozen_until_the_artifact_exists`
to assert those values. Merge in the operator's quiet window (merging workers
`main` redeploys git-connected crons); nothing in the deploy reads the pins.

### 3.4 Plan — LOCAL, read-only

```powershell
python scripts\backfill_bond_panel_coupon_pit_repair.py --plan --from-head <current pointer publication_id>
```

`<current pointer>` is `SELECT publication_id FROM bond_panel_app_pointer WHERE
product = 'bond_panel_v1'` at the start of the quiet window. The evidence JSON
carries the deterministic child id; record it. If the pointer moves before
3.8 (a Stage 6 run), every emitted transaction refuses
(`… requires the expected head pointer`): re-plan from the new head, nothing
else changes.

### 3.5 Prepare — PRODUCTION WRITE, OWNER AUTHORIZATION REQUIRED

`--emit-prepare` | psql. Verifies live: pointer = head; head validated,
config `1863d3d5fa3a0edf`, window `2002-07-01 … > 2026-06-01`, open month =
closed + 1; ancestry reaches the unit-repair child and the frozen root; the
current view holds exactly `rows_at_or_before_cutoff` returns rows at or
before the cutoff. Inserts the `prepared` child with the live counts of the
three verbatim surfaces and `rows_at_or_before_cutoff + (view rows after the
cutoff)` returns rows. Idempotent; never moves the pointer.

### 3.6 Verbatim copies — PRODUCTION WRITES, OWNER AUTHORIZATION REQUIRED

`--emit-copy snapshot`, `--emit-copy rv_signal`, `--emit-copy rating_pit`,
`--emit-copy returns` (months after the cutoff only), each | psql. Each
transaction re-checks the pointer and the prepared child, inserts from the
current view with the dual-series identity fill, and gates: count equals the
declared count, every data column equals the view row.

### 3.7 Artifact batches — PRODUCTION WRITES, OWNER AUTHORIZATION REQUIRED

```powershell
python scripts\backfill_bond_panel_coupon_pit_repair.py --emit-batch --from-head <head> --start-after 0 --limit 100000 | psql …
```

then `--start-after 100000`, … until the returned evidence says
`"done": true` (`rows_at_or_before_cutoff` rows, ≈ 28 batches at 100,000).
Each batch is one COPY transaction, idempotent (`ON CONFLICT DO NOTHING` after
an immutable-evidence preflight), replay-safe after finalize, and bound to the
artifact sha256. The artifact payload is kept and a `coupon_pit_repair`
marker is added.

### 3.8 Finalize — PRODUCTION WRITE, QUIET WINDOW, OWNER AUTHORIZATION REQUIRED

`--emit-finalize` | psql. One timed transaction (`lock_timeout 5s`,
`statement_timeout 55min`) with the four fact tables in SHARE mode: counts
per surface equal the declaration; returns span `2002-08-01 … head closed
month` contiguously; dual-series identity and coverage gates (as the unit
repair); per month, the child's returns keys equal the head projection's,
`price_return`/`exit_basis`/`exit_reason` are identical per row, rows after
the cutoff are identical in full, repriced rows carry the marker and satisfy
`total_return = price_return + carry_return` (1e-12) and
`suspect = |total| > 0.5`; per year, rows and the carry sums before AND after
equal the artifact manifest (1e-6). Then `prepared → validated`, pointer CAS
head → child, and after COMMIT the four `*_mat` refreshes in the frozen order
(own 20 min timeout; cannot undo the CAS).

### 3.9 Verification — READ-ONLY

- `bond_panel_app_pointer` points at the child; the child is `validated`;
  `bond_panel_current_returns_v1_mat` count equals the child's `returns_rows`.
- Spot CUSIPs from the production sample: `29078EAA3` (carry ≈ unchanged,
  +0.01–0.04 bp/mo), `87952VAM8` (carry moves from the 1.398 % implied coupon
  to the 6.5 % contractual one: +40 … +150 bp/mo at prices 28–70),
  `00077TAB0` (no terms: PIT basis, unchanged within 0.01 bp).
- Run the Light bond recommendation refresh (§2) and confirm the new
  `factor_returns_digest`.

### 3.10 Rollback

The child is immutable and the head is untouched: `UPDATE bond_panel_app_pointer
SET publication_id = <head> WHERE product = 'bond_panel_v1' AND publication_id =
<child>` (the pointer trigger requires `<head>` to be the child's parent, which
it is), then refresh the four `*_mat` views. Light's refresh must be re-run
again afterwards.

## 4. Evidence

### 4.1 Production sample (read-only, 2026-10-07 13:50–14:40 UTC, `bond_panel_current_*_v1_mat`, 7 CUSIPs, 804 snapshot rows, 728 returns rows)

Stored carry reproduced exactly (max |12·carry·prev − stored coupon| = 0.0 on
all 722 fallback rows); `before` = stored carry; `after` = contractual coupon
where `bond_reference_terms` carries one (6 of 7), else PIT.

| CUSIP | months | fallback rows | min price | terms | stored coupon | PIT range | Δcarry bp/mo mean (min … max) | cum. total return before → after |
|---|---|---|---|---|---|---|---|---|
| 29078EAA3 (distressed) | 246 | 241 | 14.41 | 7.995 Fixed | 7.9939 | 7.9939 … 7.9949 | +0.01 (+0.01 … +0.04) | 163.68 % → 163.71 % |
| 87952VAM8 (distressed) | 86 | 81 | 28.28 | 6.500 Fixed | 1.3978 | 1.3978 … 6.5012 | **+84.17 (+40.05 … +150.33)** | 15.81 % → 83.99 % |
| 00033GAA3 | 34 | 29 | 83.66 | 8.375 Fixed | 8.3805 | 8.376 … 8.391 | −0.05 | 25.56 % → 25.55 % |
| 00033GAB1 | 34 | 29 | 77.38 | 8.750 Fixed | 8.7516 | 8.745 … 8.755 | −0.01 (−0.02 … −0.01) | 23.99 % → 23.98 % |
| 00037BAC6 | 171 | 163 | 81.95 | 4.375 Fixed | 4.3748 | 4.374 … 4.375 | 0.00 | 58.61 % → 58.61 % |
| 00037BAF9 | 104 | 99 | 93.65 | 3.800 Fixed | 3.8001 | 3.799 … 3.801 | −0.00 | 29.32 % → 29.32 % |
| 00077TAB0 | 129 | 80 | 69.31 | none → PIT | 7.1245 | 7.1243 … 7.1247 | −0.00 (|max| 0.00) | 136.58 % → 136.58 % |

Reading: the audit's windowed estimate for `29078EAA3` (+9.2 bp/mo) was an
artifact of starting the median at 2023-06; over the full history the PIT
median and the contractual coupon agree with the stored one to 1 bp of
coupon. The material move is `87952VAM8`: its stored implied coupon (1.4 %)
is the inversion of a deeply distressed price, while the contractual coupon
is 6.5 % — under the owner convention its carry rises by 40–150 bp/month
(PIT-only would already add +56.5 bp/mo on average). This is the known
limitation of `coupon/12/P` carry on names whose coupons may not be paid; the
convention is the owner's, and the basis is now declared per row in
`coupon_basis.parquet`.

### 4.2 Full-scale preview (PIT-only, offline, the pinned v2 production export)

See the PR body: `scripts/build_bond_panel_coupon_pit_returns.py --out …`
without `--terms` builds the `pit_only_preview` artifact from the same pinned
inputs (never an input to the republication); its manifest carries the
full-history delta distribution and the reconciliation maxima.
